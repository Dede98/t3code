import {
  OrchestrationActorKind,
  OrchestrationEvent,
  type ProviderInstanceId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  combinedInitialPlanningEventDigest,
  decodeCanonicalUtf8Bytes,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import type { AgentControlVerificationClaim } from "./model.ts";
import type { AgentControlVerificationTurnAcceptance } from "./Services/AgentControlVerificationHandoffStore.ts";
import {
  normalizeVerificationTerminalSource,
  type VerificationTerminalObservation,
  type VerificationTerminalSource,
} from "./terminalObservation.ts";

export class AgentControlVerificationOrchestrationHistoryError extends Schema.TaggedErrorClass<AgentControlVerificationOrchestrationHistoryError>()(
  "AgentControlVerificationOrchestrationHistoryError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["persistence", "corrupt-history", "terminal-conflict"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const isOrchestrationHistoryError = Schema.is(AgentControlVerificationOrchestrationHistoryError);
const decodeUnknownJson = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);

interface StoredOrchestrationEvent {
  readonly event: OrchestrationEvent;
  readonly streamVersion: number;
  readonly actorKind: typeof OrchestrationActorKind.Type;
  readonly envelopeJson: string;
}

const error = (
  operation: string,
  reason: AgentControlVerificationOrchestrationHistoryError["reason"],
  cause?: unknown,
) =>
  new AgentControlVerificationOrchestrationHistoryError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const decodeText = (value: unknown, operation: string) =>
  Effect.try({
    try: () => decodeCanonicalUtf8Bytes(value),
    catch: (cause) => error(operation, "corrupt-history", cause),
  });

const decodeNullableText = (value: unknown, operation: string) =>
  value === null ? Effect.succeed(null) : decodeText(value, operation);

const decodeJson = (value: unknown, operation: string) =>
  decodeText(value, `${operation}-bytes`).pipe(
    Effect.flatMap((source) =>
      decodeUnknownJson(source).pipe(
        Effect.map((decoded) => decoded as JsonValue),
        Effect.mapError((cause) => error(`${operation}-json`, "corrupt-history", cause)),
      ),
    ),
  );

const canonicalEnvelope = (entry: Omit<StoredOrchestrationEvent, "envelopeJson">) =>
  canonicalJson({
    actorKind: entry.actorKind,
    aggregateId: entry.event.aggregateId,
    aggregateKind: entry.event.aggregateKind,
    causationEventId: entry.event.causationEventId,
    commandId: entry.event.commandId,
    correlationId: entry.event.correlationId,
    eventId: entry.event.eventId,
    metadata: entry.event.metadata as JsonValue,
    occurredAt: entry.event.occurredAt,
    payload: entry.event.payload as JsonValue,
    sequence: entry.event.sequence,
    streamVersion: entry.streamVersion,
    type: entry.event.type,
  });

const sameSessionProjection = (
  projection: {
    readonly threadId: string;
    readonly status: string;
    readonly providerName: string | null;
    readonly providerInstanceId: string | null;
    readonly runtimeMode: string;
    readonly activeTurnId: string | null;
    readonly lastError: string | null;
    readonly updatedAt: string;
  },
  entry: StoredOrchestrationEvent,
) => {
  if (entry.event.type !== "thread.session-set") return false;
  const session = entry.event.payload.session;
  return (
    projection.threadId === session.threadId &&
    projection.status === session.status &&
    projection.providerName === session.providerName &&
    projection.providerInstanceId === (session.providerInstanceId ?? null) &&
    projection.runtimeMode === session.runtimeMode &&
    projection.activeTurnId === session.activeTurnId &&
    projection.lastError === session.lastError &&
    projection.updatedAt === session.updatedAt
  );
};

const terminalSessionStatus = (source: VerificationTerminalSource): "ready" | "error" =>
  source.runtimeEventType === "turn.aborted" || source.providerState === "failed"
    ? "error"
    : "ready";

export interface VerificationProviderStartHistoryEntry {
  readonly streamVersion: number;
  readonly occurredAt: string;
  readonly session: {
    readonly threadId: string;
    readonly status: string;
    readonly providerName: string | null;
    readonly providerInstanceId?: string | undefined;
    readonly runtimeMode: string;
    readonly activeTurnId: string | null;
  };
  readonly lifecycle?:
    | {
        readonly runtimeEventId: string;
        readonly runtimeEventType: "turn.started" | "turn.completed" | "turn.aborted";
        readonly providerInstanceId: string;
        readonly providerTurnId: string;
      }
    | undefined;
  readonly canonicalSessionJson: string;
}

export interface VerificationProviderTerminalHistoryEntry {
  readonly streamVersion: number;
  readonly source: VerificationTerminalSource;
  readonly payload: JsonValue;
  readonly metadata: JsonValue;
}

export const selectVerificationProviderStart = Effect.fn("selectVerificationProviderStart")(
  function* (
    entries: ReadonlyArray<VerificationProviderStartHistoryEntry>,
    identity: {
      readonly threadId: string;
      readonly providerInstanceId: string;
      readonly providerTurnId: string;
      readonly runtimeMode: string;
      readonly turnRequestStreamVersion: number;
    },
  ) {
    const matches = (entry: VerificationProviderStartHistoryEntry) => {
      const session = entry.session;
      const lifecycle = entry.lifecycle;
      if (
        session.threadId !== identity.threadId ||
        session.status !== "running" ||
        session.activeTurnId !== identity.providerTurnId ||
        (session.providerInstanceId !== undefined &&
          session.providerInstanceId !== identity.providerInstanceId) ||
        session.runtimeMode !== identity.runtimeMode ||
        session.providerName === null
      ) {
        return false;
      }
      return (
        lifecycle === undefined ||
        (lifecycle.runtimeEventType === "turn.started" &&
          lifecycle.providerInstanceId === identity.providerInstanceId &&
          lifecycle.providerTurnId === identity.providerTurnId)
      );
    };
    if (
      entries.some(
        (entry) => entry.streamVersion <= identity.turnRequestStreamVersion && matches(entry),
      )
    ) {
      return yield* error("provider-start-before-turn-request", "corrupt-history");
    }
    const candidates = entries
      .map((entry, index) => ({ entry, index }))
      .filter(
        ({ entry }) => entry.streamVersion > identity.turnRequestStreamVersion && matches(entry),
      );
    if (candidates.length === 0) return { _tag: "Waiting" } as const;
    const first = candidates[0]!;
    if (candidates.length > 1) {
      const lifecycle = first.entry.lifecycle;
      const sameAuthoritativeRuntimeStart =
        lifecycle?.runtimeEventType === "turn.started" &&
        candidates.every(({ entry }) => {
          const candidateLifecycle = entry.lifecycle;
          return (
            candidateLifecycle?.runtimeEventType === "turn.started" &&
            candidateLifecycle.runtimeEventId === lifecycle.runtimeEventId &&
            entry.occurredAt === first.entry.occurredAt &&
            entry.canonicalSessionJson === first.entry.canonicalSessionJson
          );
        });
      if (!sameAuthoritativeRuntimeStart) {
        return yield* error("provider-start-ambiguous", "corrupt-history");
      }
    }
    return { _tag: "Ready", index: candidates.at(-1)!.index } as const;
  },
);

export const selectVerificationProviderTerminal = Effect.fn("selectVerificationProviderTerminal")(
  function* (
    entries: ReadonlyArray<VerificationProviderTerminalHistoryEntry>,
    identity: {
      readonly providerDeliveryId: string;
      readonly threadId: ThreadId;
      readonly providerInstanceId: ProviderInstanceId;
      readonly providerTurnId: TurnId;
    },
  ) {
    if (entries.length === 0) return { _tag: "Waiting" } as const;
    const candidates: Array<{
      readonly entry: VerificationProviderTerminalHistoryEntry;
      readonly observation: VerificationTerminalObservation;
      readonly replayEvidenceJson: string;
    }> = [];
    for (const entry of entries) {
      const observation = yield* normalizeVerificationTerminalSource(entry.source, {
        providerDeliveryId: identity.providerDeliveryId,
        threadId: identity.threadId,
        providerInstanceId: identity.providerInstanceId,
        providerTurnId: identity.providerTurnId,
      }).pipe(
        Effect.mapError((cause) => error("normalize-provider-terminal", "corrupt-history", cause)),
      );
      candidates.push({
        entry,
        observation,
        replayEvidenceJson: canonicalJson({
          deliveryState: observation.deliveryState,
          expectedSessionStatus: terminalSessionStatus(entry.source),
          lastErrorCode: observation.lastErrorCode,
          metadata: entry.metadata,
          payload: entry.payload,
          providerInstanceId: entry.source.providerInstanceId,
          providerState: observation.providerState,
          providerTurnId: entry.source.providerTurnId,
          runtimeEventId: entry.source.runtimeEventId,
          runtimeEventType: entry.source.runtimeEventType,
          terminalAt: entry.source.terminalAt,
          threadId: entry.source.threadId,
        }),
      });
    }
    const first = candidates[0]!;
    if (candidates.some((candidate) => candidate.replayEvidenceJson !== first.replayEvidenceJson)) {
      return yield* error("provider-terminal-ambiguous", "terminal-conflict");
    }
    return {
      _tag: "Ready",
      index: 0,
      observation: first.observation,
    } as const;
  },
);

const loadVerificationTerminalFromOrchestrationHistoryInTransaction = Effect.fn(
  "loadVerificationTerminalFromOrchestrationHistoryInTransaction",
)(function* (
  sql: SqlClient.SqlClient,
  claim: AgentControlVerificationClaim,
  acceptance: AgentControlVerificationTurnAcceptance,
) {
  const providerTurnId = claim.delivery.providerTurnId;
  if (providerTurnId === null || claim.delivery.providerAcceptedAt === null) {
    return { _tag: "Waiting" } as const;
  }

  const rawRows = yield* sql<Record<string, unknown>>`
    SELECT sequence, stream_version AS "streamVersion",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      CAST(event_type AS BLOB) AS "typeBytes",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${claim.evidence.threadId}
    ORDER BY stream_version, sequence
  `.pipe(Effect.mapError((cause) => error("read-orchestration-history", "persistence", cause)));
  if (rawRows.length === 0) {
    return yield* error("orchestration-history-missing", "corrupt-history");
  }

  const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
  const decodeActorKind = Schema.decodeUnknownEffect(OrchestrationActorKind);
  const history: Array<StoredOrchestrationEvent> = [];
  let previousSequence = 0;
  for (const [index, row] of rawRows.entries()) {
    if (
      typeof row.sequence !== "number" ||
      !Number.isInteger(row.sequence) ||
      row.sequence <= previousSequence ||
      typeof row.streamVersion !== "number" ||
      !Number.isInteger(row.streamVersion) ||
      row.streamVersion !== index + 1
    ) {
      return yield* error("orchestration-history-order", "corrupt-history");
    }
    previousSequence = row.sequence;
    const [
      eventId,
      aggregateKind,
      aggregateId,
      type,
      occurredAt,
      commandId,
      causationEventId,
      correlationId,
      actorKindText,
    ] = yield* Effect.all([
      decodeText(row.eventIdBytes, "orchestration-event-id"),
      decodeText(row.aggregateKindBytes, "orchestration-aggregate-kind"),
      decodeText(row.aggregateIdBytes, "orchestration-aggregate-id"),
      decodeText(row.typeBytes, "orchestration-event-type"),
      decodeText(row.occurredAtBytes, "orchestration-occurred-at"),
      decodeNullableText(row.commandIdBytes, "orchestration-command-id"),
      decodeNullableText(row.causationEventIdBytes, "orchestration-causation-event-id"),
      decodeNullableText(row.correlationIdBytes, "orchestration-correlation-id"),
      decodeText(row.actorKindBytes, "orchestration-actor-kind"),
    ]);
    if (aggregateKind !== "thread" || aggregateId !== claim.evidence.threadId) {
      return yield* error("orchestration-stream-identity", "corrupt-history");
    }
    const actorKind = yield* decodeActorKind(actorKindText).pipe(
      Effect.mapError((cause) =>
        error("decode-orchestration-actor-kind", "corrupt-history", cause),
      ),
    );
    const { payload, metadata } = yield* Effect.all(
      {
        payload: decodeJson(row.payloadBytes, "orchestration-payload"),
        metadata: decodeJson(row.metadataBytes, "orchestration-metadata"),
      },
      { concurrency: "unbounded" },
    );
    const event = yield* decodeOrchestrationEvent({
      sequence: row.sequence,
      eventId,
      aggregateKind,
      aggregateId,
      type,
      occurredAt,
      commandId,
      causationEventId,
      correlationId,
      payload,
      metadata,
    }).pipe(
      Effect.mapError((cause) => error("decode-orchestration-event", "corrupt-history", cause)),
    );
    if (
      canonicalJson(event.payload as JsonValue) !== canonicalJson(payload) ||
      canonicalJson(event.metadata as JsonValue) !== canonicalJson(metadata)
    ) {
      return yield* error("orchestration-event-fields-stripped", "corrupt-history");
    }
    const entry = { event, streamVersion: row.streamVersion, actorKind };
    history.push({ ...entry, envelopeJson: canonicalEnvelope(entry) });
  }

  if (
    acceptance.handoffId !== claim.evidence.handoffId ||
    acceptance.handoffFingerprint !== claim.evidence.handoffFingerprint ||
    acceptance.threadId !== claim.evidence.threadId ||
    acceptance.turnRequestCommandId !== claim.evidence.turnRequestCommandId ||
    acceptance.messageId !== claim.evidence.messageId ||
    acceptance.messageEventId !== claim.evidence.messageEventId ||
    acceptance.turnRequestEventId !== claim.evidence.turnRequestEventId ||
    acceptance.eventEvidenceDigest !==
      combinedInitialPlanningEventDigest(
        acceptance.messageEventEnvelopeJson,
        acceptance.turnRequestEventEnvelopeJson,
      )
  ) {
    return yield* error("turn-acceptance-identity", "corrupt-history");
  }
  const message = history.filter((entry) => entry.event.eventId === acceptance.messageEventId);
  const turn = history.filter((entry) => entry.event.eventId === acceptance.turnRequestEventId);
  if (message.length !== 1 || turn.length !== 1) {
    return yield* error("turn-request-identity", "corrupt-history");
  }
  const messageEntry = message[0]!;
  const turnEntry = turn[0]!;
  if (
    messageEntry.event.type !== "thread.message-sent" ||
    turnEntry.event.type !== "thread.turn-start-requested" ||
    messageEntry.actorKind !== "client" ||
    turnEntry.actorKind !== "client" ||
    messageEntry.streamVersion !== 3 ||
    turnEntry.streamVersion !== 4 ||
    messageEntry.event.sequence !== acceptance.messageEventSequence ||
    turnEntry.event.sequence !== acceptance.turnRequestEventSequence ||
    messageEntry.envelopeJson !== acceptance.messageEventEnvelopeJson ||
    turnEntry.envelopeJson !== acceptance.turnRequestEventEnvelopeJson ||
    messageEntry.event.commandId !== claim.evidence.turnRequestCommandId ||
    turnEntry.event.commandId !== claim.evidence.turnRequestCommandId ||
    messageEntry.event.causationEventId !== null ||
    turnEntry.event.causationEventId !== claim.evidence.messageEventId ||
    messageEntry.event.correlationId !== claim.evidence.turnRequestCommandId ||
    turnEntry.event.correlationId !== claim.evidence.turnRequestCommandId ||
    messageEntry.event.payload.threadId !== claim.evidence.threadId ||
    messageEntry.event.payload.messageId !== claim.evidence.messageId ||
    messageEntry.event.payload.role !== "user" ||
    messageEntry.event.payload.text !== claim.evidence.promptText ||
    turnEntry.event.payload.threadId !== claim.evidence.threadId ||
    turnEntry.event.payload.messageId !== claim.evidence.messageId ||
    turnEntry.event.payload.runtimeMode !== claim.evidence.runtimeMode ||
    turnEntry.event.payload.interactionMode !== "default" ||
    canonicalJson(turnEntry.event.payload.modelSelection as JsonValue) !==
      claim.evidence.modelSelectionJson ||
    turnEntry.event.payload.sourceProposedPlan?.threadId !== claim.evidence.planningThreadId ||
    turnEntry.event.payload.sourceProposedPlan?.planId !== claim.evidence.planId
  ) {
    return yield* error("turn-request-history", "corrupt-history");
  }

  const createdEntries = history.filter((entry) => entry.event.type === "thread.created");
  const boundEntries = history.filter((entry) => entry.event.type === "thread.agent-control-bound");
  if (createdEntries.length !== 1 || boundEntries.length !== 1) {
    return yield* error("thread-materialization-history", "corrupt-history");
  }
  const createdEntry = createdEntries[0]!;
  const boundEntry = boundEntries[0]!;
  const created = createdEntry.event;
  const bound = boundEntry.event;
  if (
    createdEntry.streamVersion !== 1 ||
    boundEntry.streamVersion !== 2 ||
    created.type !== "thread.created" ||
    created.payload.threadId !== claim.evidence.threadId ||
    created.payload.projectId !== claim.evidence.projectId ||
    created.payload.branch !== claim.evidence.branch ||
    created.payload.worktreePath !== claim.evidence.worktreePath ||
    created.payload.runtimeMode !== claim.evidence.runtimeMode ||
    created.payload.interactionMode !== "default" ||
    canonicalJson(created.payload.modelSelection as JsonValue) !==
      claim.evidence.modelSelectionJson ||
    bound.type !== "thread.agent-control-bound" ||
    bound.payload.threadId !== claim.evidence.threadId ||
    bound.payload.binding.taskId !== claim.evidence.taskId ||
    bound.payload.binding.stageRunId !== claim.evidence.stageRunId ||
    bound.payload.binding.attemptId !== claim.evidence.attemptId ||
    bound.payload.binding.roleId !== "verifier" ||
    bound.payload.binding.controlState !== "controlled"
  ) {
    return yield* error("thread-materialization-binding", "corrupt-history");
  }

  const providerSessions = history.filter(
    (
      entry,
    ): entry is StoredOrchestrationEvent & {
      readonly event: Extract<OrchestrationEvent, { readonly type: "thread.session-set" }>;
    } => entry.event.type === "thread.session-set" && entry.actorKind === "provider",
  );
  const startSelection = yield* selectVerificationProviderStart(
    providerSessions.map((entry) => ({
      streamVersion: entry.streamVersion,
      occurredAt: entry.event.occurredAt,
      session: entry.event.payload.session,
      lifecycle: entry.event.metadata.providerRuntimeLifecycle,
      canonicalSessionJson: canonicalJson(entry.event.payload.session as JsonValue),
    })),
    {
      threadId: claim.evidence.threadId,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: TurnId.make(providerTurnId),
      runtimeMode: claim.evidence.runtimeMode,
      turnRequestStreamVersion: turnEntry.streamVersion,
    },
  );
  if (startSelection._tag === "Waiting") {
    const terminalBeforeStart = providerSessions.some((entry) => {
      const lifecycle = entry.event.metadata.providerRuntimeLifecycle;
      return (
        lifecycle !== undefined &&
        lifecycle.runtimeEventType !== "turn.started" &&
        lifecycle.providerInstanceId === claim.evidence.providerInstanceId &&
        lifecycle.providerTurnId === providerTurnId
      );
    });
    if (terminalBeforeStart) {
      return yield* error("provider-terminal-before-start", "terminal-conflict");
    }
    return { _tag: "Waiting" } as const;
  }
  const started = providerSessions[startSelection.index]!;
  const startedProviderName = started.event.payload.session.providerName;

  const matchingTerminalEntries: Array<{
    readonly entry: (typeof providerSessions)[number];
    readonly source: VerificationTerminalSource;
  }> = [];
  const foreignTerminalEntries: Array<(typeof providerSessions)[number]> = [];
  for (const entry of providerSessions) {
    const lifecycle = entry.event.metadata.providerRuntimeLifecycle;
    if (lifecycle === undefined || lifecycle.runtimeEventType === "turn.started") {
      continue;
    }
    if (entry.streamVersion <= started.streamVersion) {
      if (
        lifecycle.providerInstanceId === claim.evidence.providerInstanceId &&
        lifecycle.providerTurnId === providerTurnId
      ) {
        return yield* error("provider-terminal-before-start", "terminal-conflict");
      }
      continue;
    }
    if (
      lifecycle.providerInstanceId !== claim.evidence.providerInstanceId ||
      lifecycle.providerTurnId !== providerTurnId
    ) {
      foreignTerminalEntries.push(entry);
      continue;
    }
    const source: VerificationTerminalSource =
      lifecycle.runtimeEventType === "turn.completed"
        ? {
            runtimeEventId: lifecycle.runtimeEventId,
            runtimeEventType: lifecycle.runtimeEventType,
            threadId: entry.event.payload.threadId,
            providerInstanceId: lifecycle.providerInstanceId,
            providerTurnId: lifecycle.providerTurnId,
            providerState: lifecycle.providerState,
            terminalAt: entry.event.occurredAt,
          }
        : {
            runtimeEventId: lifecycle.runtimeEventId,
            runtimeEventType: lifecycle.runtimeEventType,
            threadId: entry.event.payload.threadId,
            providerInstanceId: lifecycle.providerInstanceId,
            providerTurnId: lifecycle.providerTurnId,
            terminalAt: entry.event.occurredAt,
          };
    const session = entry.event.payload.session;
    if (
      session.threadId !== claim.evidence.threadId ||
      session.providerInstanceId !== claim.evidence.providerInstanceId ||
      session.providerName !== startedProviderName ||
      session.runtimeMode !== claim.evidence.runtimeMode ||
      session.activeTurnId !== null ||
      session.updatedAt !== entry.event.occurredAt ||
      session.status !== terminalSessionStatus(source)
    ) {
      return yield* error("provider-terminal-session-divergent", "terminal-conflict");
    }
    matchingTerminalEntries.push({ entry, source });
  }
  const terminalSelection = yield* selectVerificationProviderTerminal(
    matchingTerminalEntries.map(({ entry, source }) => ({
      streamVersion: entry.streamVersion,
      source,
      payload: entry.event.payload as JsonValue,
      metadata: entry.event.metadata as JsonValue,
    })),
    {
      providerDeliveryId: claim.evidence.providerDeliveryId,
      threadId: claim.evidence.threadId,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: TurnId.make(providerTurnId),
    },
  );
  const terminal =
    terminalSelection._tag === "Ready"
      ? matchingTerminalEntries[terminalSelection.index]
      : undefined;
  if (
    foreignTerminalEntries.some(
      (entry) => terminal === undefined || entry.streamVersion < terminal.entry.streamVersion,
    )
  ) {
    return yield* error("terminal-identity-divergent", "corrupt-history");
  }

  const latestSession = providerSessions.at(-1);
  if (latestSession === undefined) return { _tag: "Waiting" } as const;
  const projectionRows = yield* sql<Record<string, unknown>>`
    SELECT CAST(thread_id AS BLOB) AS "threadIdBytes",
      CAST(status AS BLOB) AS "statusBytes",
      CASE WHEN provider_name IS NULL THEN NULL ELSE CAST(provider_name AS BLOB) END
        AS "providerNameBytes",
      CASE WHEN provider_instance_id IS NULL THEN NULL ELSE CAST(provider_instance_id AS BLOB) END
        AS "providerInstanceIdBytes",
      CAST(runtime_mode AS BLOB) AS "runtimeModeBytes",
      CASE WHEN active_turn_id IS NULL THEN NULL ELSE CAST(active_turn_id AS BLOB) END
        AS "activeTurnIdBytes",
      CASE WHEN last_error IS NULL THEN NULL ELSE CAST(last_error AS BLOB) END AS "lastErrorBytes",
      CAST(updated_at AS BLOB) AS "updatedAtBytes"
    FROM projection_thread_sessions WHERE thread_id = ${claim.evidence.threadId}
  `.pipe(Effect.mapError((cause) => error("read-session-projection", "persistence", cause)));
  if (projectionRows.length === 0) return { _tag: "Waiting" } as const;
  if (projectionRows.length !== 1) {
    return yield* error("session-projection-count", "corrupt-history");
  }
  const projectionRow = projectionRows[0]!;
  const [
    projectedThreadId,
    status,
    providerName,
    providerInstanceId,
    runtimeMode,
    activeTurnId,
    lastError,
    updatedAt,
  ] = yield* Effect.all([
    decodeText(projectionRow.threadIdBytes, "session-projection-thread-id"),
    decodeText(projectionRow.statusBytes, "session-projection-status"),
    decodeNullableText(projectionRow.providerNameBytes, "session-projection-provider-name"),
    decodeNullableText(
      projectionRow.providerInstanceIdBytes,
      "session-projection-provider-instance-id",
    ),
    decodeText(projectionRow.runtimeModeBytes, "session-projection-runtime-mode"),
    decodeNullableText(projectionRow.activeTurnIdBytes, "session-projection-active-turn-id"),
    decodeNullableText(projectionRow.lastErrorBytes, "session-projection-last-error"),
    decodeText(projectionRow.updatedAtBytes, "session-projection-updated-at"),
  ]);
  const projection = {
    threadId: projectedThreadId,
    status,
    providerName,
    providerInstanceId,
    runtimeMode,
    activeTurnId,
    lastError,
    updatedAt,
  };
  if (!sameSessionProjection(projection, latestSession)) {
    if (providerSessions.some((entry) => sameSessionProjection(projection, entry))) {
      return { _tag: "Waiting" } as const;
    }
    return yield* error("session-projection-divergent", "corrupt-history");
  }

  if (terminal === undefined) {
    const laterDivergentActiveSession = providerSessions.some((entry) => {
      if (entry.streamVersion <= started.streamVersion) return false;
      const session = entry.event.payload.session;
      return (
        session.activeTurnId !== null &&
        (session.providerInstanceId !== claim.evidence.providerInstanceId ||
          session.activeTurnId !== providerTurnId)
      );
    });
    if (laterDivergentActiveSession) {
      return yield* error("runtime-session-divergent", "corrupt-history");
    }
    return { _tag: "Waiting" } as const;
  }
  if (terminalSelection._tag !== "Ready") {
    return yield* error("provider-terminal-selection", "corrupt-history");
  }
  return {
    _tag: "Ready",
    startedStreamVersion: started.streamVersion,
    terminalStreamVersion: terminal.entry.streamVersion,
    observation: terminalSelection.observation,
  } as const;
});

export const loadVerificationTerminalFromOrchestrationHistory = Effect.fn(
  "loadVerificationTerminalFromOrchestrationHistory",
)(function* (
  sql: SqlClient.SqlClient,
  claim: AgentControlVerificationClaim,
  acceptance: AgentControlVerificationTurnAcceptance,
) {
  return yield* sql
    .withTransaction(
      loadVerificationTerminalFromOrchestrationHistoryInTransaction(sql, claim, acceptance),
    )
    .pipe(
      Effect.mapError((cause) =>
        isOrchestrationHistoryError(cause)
          ? cause
          : error("orchestration-history-transaction", "persistence", cause),
      ),
    );
});
