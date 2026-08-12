import {
  OrchestrationActorKind,
  OrchestrationEvent,
  type ProviderInstanceId,
  ThreadId,
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

type ThreadSessionSetEvent = Extract<OrchestrationEvent, { readonly type: "thread.session-set" }>;

type StoredSessionEvent = StoredOrchestrationEvent & {
  readonly event: ThreadSessionSetEvent;
};

type VerificationProviderLifecycleHistoryEntry =
  | {
      readonly _tag: "Start";
      readonly entry: StoredSessionEvent;
    }
  | {
      readonly _tag: "Terminal";
      readonly entry: StoredSessionEvent;
      readonly source: VerificationTerminalSource;
    };

const routingBytes = (value: string): Uint8Array => new TextEncoder().encode(value);

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

const uuidV4Pattern = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}";
const providerRuntimeSessionCommandPattern = new RegExp(
  `^provider:.+:thread-session-set:${uuidV4Pattern}$`,
  "iu",
);
const serverStoppedSessionCommandPattern = new RegExp(
  `^server:provider-session-set:${uuidV4Pattern}$`,
  "iu",
);

const isProductionTerminalSessionSuffix = (
  source: VerificationTerminalSource,
  previousSession: ThreadSessionSetEvent["payload"]["session"],
  entry: StoredSessionEvent,
  identity: {
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerName: string;
    readonly runtimeMode: string;
  },
): boolean => {
  const session = entry.event.payload.session;
  const commandId = entry.event.commandId;
  if (
    canonicalJson(entry.event.metadata as JsonValue) !== "{}" ||
    session.providerName !== identity.providerName ||
    session.providerInstanceId !== identity.providerInstanceId ||
    session.runtimeMode !== identity.runtimeMode ||
    session.activeTurnId !== null ||
    commandId === null
  ) {
    return false;
  }

  if (session.status === "ready") {
    const followsTechnicalTerminal =
      source.runtimeEventType === "turn.aborted" || source.providerState === "failed"
        ? previousSession.status === "error" || previousSession.status === "ready"
        : previousSession.status === "ready";
    return (
      followsTechnicalTerminal &&
      entry.actorKind === "provider" &&
      providerRuntimeSessionCommandPattern.test(commandId) &&
      session.lastError === null
    );
  }

  if (session.status === "stopped") {
    const hasProductionLineage =
      (entry.actorKind === "provider" && providerRuntimeSessionCommandPattern.test(commandId)) ||
      (entry.actorKind === "server" && serverStoppedSessionCommandPattern.test(commandId));
    return (
      hasProductionLineage &&
      (previousSession.status === "error" ||
        previousSession.status === "ready" ||
        previousSession.status === "stopped") &&
      session.lastError === previousSession.lastError
    );
  }

  return false;
};

export interface VerificationProviderStartHistoryEntry {
  readonly streamVersion: number;
  readonly actorKind: typeof OrchestrationActorKind.Type;
  readonly eventType: OrchestrationEvent["type"];
  readonly occurredAt: string;
  readonly envelopeLineage: {
    readonly eventId: string;
    readonly commandId: string | null;
    readonly causationEventId: string | null;
    readonly correlationId: string | null;
  };
  readonly payload: ThreadSessionSetEvent["payload"];
  readonly metadata: ThreadSessionSetEvent["metadata"];
  readonly session: ThreadSessionSetEvent["payload"]["session"];
  readonly lifecycle: ThreadSessionSetEvent["metadata"]["providerRuntimeLifecycle"];
}

export interface VerificationProviderTerminalHistoryEntry {
  readonly streamVersion: number;
  readonly envelopeLineage: {
    readonly eventId: string;
    readonly commandId: string | null;
    readonly causationEventId: string | null;
    readonly correlationId: string | null;
  };
  readonly source: VerificationTerminalSource;
  readonly payload: JsonValue;
  readonly metadata: JsonValue;
}

const canonicalProviderStartReplayEvidence = (
  entry: VerificationProviderStartHistoryEntry,
): string | undefined => {
  const lifecycle = entry.lifecycle;
  if (lifecycle?.runtimeEventType !== "turn.started") return undefined;
  return canonicalJson({
    actorKind: entry.actorKind,
    eventType: entry.eventType,
    expectedSessionStatus: "running",
    lineage: {
      causationEventId: null,
      correlationId: "self-correlated-command",
    },
    metadata: entry.metadata as JsonValue,
    occurredAt: entry.occurredAt,
    payload: entry.payload as JsonValue,
    providerInstanceId: lifecycle.providerInstanceId,
    providerTurnId: lifecycle.providerTurnId,
    runtimeEventId: lifecycle.runtimeEventId,
    runtimeEventType: lifecycle.runtimeEventType,
    runtimeMode: entry.session.runtimeMode,
    session: entry.session as JsonValue,
    threadId: entry.payload.threadId,
  });
};

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
        entry.actorKind !== "provider" ||
        entry.eventType !== "thread.session-set" ||
        entry.payload.threadId !== identity.threadId ||
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
    for (const entry of entries) {
      if (entry.lifecycle?.runtimeEventType !== "turn.started") continue;
      if (
        entry.actorKind !== "provider" ||
        entry.eventType !== "thread.session-set" ||
        entry.envelopeLineage.commandId === null ||
        !entry.envelopeLineage.commandId.startsWith("provider:") ||
        entry.envelopeLineage.causationEventId !== null ||
        entry.envelopeLineage.correlationId !== entry.envelopeLineage.commandId
      ) {
        return yield* error("provider-start-envelope-lineage", "corrupt-history");
      }
    }
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
      const replayEvidenceJson = canonicalProviderStartReplayEvidence(first.entry);
      const sameAuthoritativeRuntimeStart =
        replayEvidenceJson !== undefined &&
        candidates.every(
          ({ entry }) => canonicalProviderStartReplayEvidence(entry) === replayEvidenceJson,
        );
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
      if (
        entry.envelopeLineage.commandId === null ||
        entry.envelopeLineage.causationEventId !== null ||
        entry.envelopeLineage.correlationId !== entry.envelopeLineage.commandId
      ) {
        return yield* error("provider-terminal-envelope-lineage", "terminal-conflict");
      }
      const observation = yield* normalizeVerificationTerminalSource(entry.source, {
        providerDeliveryId: identity.providerDeliveryId,
        threadId: identity.threadId,
        providerInstanceId: identity.providerInstanceId,
        providerTurnId: identity.providerTurnId,
      }).pipe(Effect.mapError(() => error("normalize-provider-terminal", "terminal-conflict")));
      candidates.push({
        entry,
        observation,
        replayEvidenceJson: canonicalJson({
          deliveryState: observation.deliveryState,
          expectedSessionStatus: terminalSessionStatus(entry.source),
          lastErrorCode: observation.lastErrorCode,
          lineage: {
            causationEventId: null,
            correlationId: "self-correlated-command",
          },
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

  const aggregateKind = "thread";
  const aggregateKindBytes = routingBytes(aggregateKind);
  const threadIdBytes = routingBytes(claim.evidence.threadId);

  const rawRows = yield* sql<Record<string, unknown>>`
    SELECT sequence, stream_version AS "streamVersion",
      typeof(event_id) AS "eventIdStorageClass",
      CAST(event_id AS BLOB) AS "eventIdBytes",
      typeof(aggregate_kind) AS "aggregateKindStorageClass",
      CAST(aggregate_kind AS BLOB) AS "aggregateKindBytes",
      typeof(stream_id) AS "aggregateIdStorageClass",
      CAST(stream_id AS BLOB) AS "aggregateIdBytes",
      typeof(event_type) AS "typeStorageClass",
      CAST(event_type AS BLOB) AS "typeBytes",
      typeof(occurred_at) AS "occurredAtStorageClass",
      CAST(occurred_at AS BLOB) AS "occurredAtBytes",
      typeof(command_id) AS "commandIdStorageClass",
      CASE WHEN command_id IS NULL THEN NULL ELSE CAST(command_id AS BLOB) END AS "commandIdBytes",
      typeof(causation_event_id) AS "causationEventIdStorageClass",
      CASE WHEN causation_event_id IS NULL THEN NULL ELSE CAST(causation_event_id AS BLOB) END
        AS "causationEventIdBytes",
      typeof(correlation_id) AS "correlationIdStorageClass",
      CASE WHEN correlation_id IS NULL THEN NULL ELSE CAST(correlation_id AS BLOB) END
        AS "correlationIdBytes",
      typeof(actor_kind) AS "actorKindStorageClass",
      CAST(actor_kind AS BLOB) AS "actorKindBytes",
      typeof(payload_json) AS "payloadStorageClass",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM orchestration_events
    WHERE aggregate_kind IN (${aggregateKind}, ${aggregateKindBytes})
      AND stream_id IN (${claim.evidence.threadId}, ${threadIdBytes})
    ORDER BY stream_version, sequence
  `.pipe(Effect.mapError((cause) => error("read-orchestration-history", "persistence", cause)));
  if (rawRows.length === 0) {
    return yield* error("orchestration-history-missing", "corrupt-history");
  }

  const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
  const decodeActorKind = Schema.decodeUnknownEffect(OrchestrationActorKind);
  const decodeThreadId = Schema.decodeUnknownEffect(ThreadId);
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
    for (const [storageClass, operation] of [
      [row.eventIdStorageClass, "orchestration-event-id-storage-class"],
      [row.aggregateKindStorageClass, "orchestration-aggregate-kind-storage-class"],
      [row.aggregateIdStorageClass, "orchestration-aggregate-id-storage-class"],
      [row.typeStorageClass, "orchestration-event-type-storage-class"],
      [row.occurredAtStorageClass, "orchestration-occurred-at-storage-class"],
      [row.actorKindStorageClass, "orchestration-actor-kind-storage-class"],
      [row.payloadStorageClass, "orchestration-payload-storage-class"],
      [row.metadataStorageClass, "orchestration-metadata-storage-class"],
    ] as const) {
      if (storageClass !== "text") {
        return yield* error(operation, "corrupt-history");
      }
    }
    for (const [storageClass, operation] of [
      [row.commandIdStorageClass, "orchestration-command-id-storage-class"],
      [row.causationEventIdStorageClass, "orchestration-causation-event-id-storage-class"],
      [row.correlationIdStorageClass, "orchestration-correlation-id-storage-class"],
    ] as const) {
      if (storageClass !== "text" && storageClass !== "null") {
        return yield* error(operation, "corrupt-history");
      }
    }
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

  // Phase 1: lifecycle evidence is globally authoritative. Inspect the complete decoded
  // controlled stream before any position, actor, or event-family subset is selected.
  const turnRequestStreamVersion = 4;
  const providerLifecycleEntries: Array<VerificationProviderLifecycleHistoryEntry> = [];
  for (const entry of history) {
    const lifecycle = entry.event.metadata.providerRuntimeLifecycle;
    if (lifecycle === undefined) continue;
    if (entry.streamVersion <= turnRequestStreamVersion) {
      return yield* error("provider-lifecycle-before-turn-request", "terminal-conflict");
    }
    if (entry.event.type !== "thread.session-set" || entry.actorKind !== "provider") {
      return yield* error("provider-lifecycle-event-shape", "terminal-conflict");
    }
    const providerSessionEntry: StoredSessionEvent = { ...entry, event: entry.event };
    const commandId = entry.event.commandId;
    if (
      commandId === null ||
      !commandId.startsWith("provider:") ||
      entry.event.causationEventId !== null ||
      entry.event.correlationId !== commandId
    ) {
      return yield* error("provider-lifecycle-envelope-lineage", "terminal-conflict");
    }
    const session = entry.event.payload.session;
    if (
      entry.event.payload.threadId !== claim.evidence.threadId ||
      session.threadId !== claim.evidence.threadId ||
      lifecycle.providerInstanceId !== claim.evidence.providerInstanceId ||
      lifecycle.providerTurnId !== providerTurnId ||
      session.providerInstanceId !== lifecycle.providerInstanceId ||
      session.providerName === null ||
      session.runtimeMode !== claim.evidence.runtimeMode ||
      session.updatedAt !== entry.event.occurredAt
    ) {
      return yield* error("provider-lifecycle-session-identity", "terminal-conflict");
    }
    if (lifecycle.runtimeEventType === "turn.started") {
      if (session.status !== "running" || session.activeTurnId !== lifecycle.providerTurnId) {
        return yield* error("provider-lifecycle-start-session", "terminal-conflict");
      }
      providerLifecycleEntries.push({ _tag: "Start", entry: providerSessionEntry });
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
    if (session.status !== terminalSessionStatus(source) || session.activeTurnId !== null) {
      return yield* error("provider-lifecycle-terminal-session", "terminal-conflict");
    }
    providerLifecycleEntries.push({ _tag: "Terminal", entry: providerSessionEntry, source });
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

  // Phase 2: select authoritative provider start and terminal evidence only after the
  // complete stream has passed lifecycle validation.
  const allSessions = history.filter(
    (entry): entry is StoredSessionEvent => entry.event.type === "thread.session-set",
  );
  const providerSessions = allSessions.filter((entry) => entry.actorKind === "provider");
  const startSelection = yield* selectVerificationProviderStart(
    providerSessions.map((entry) => ({
      streamVersion: entry.streamVersion,
      actorKind: entry.actorKind,
      eventType: entry.event.type,
      occurredAt: entry.event.occurredAt,
      envelopeLineage: {
        eventId: entry.event.eventId,
        commandId: entry.event.commandId,
        causationEventId: entry.event.causationEventId,
        correlationId: entry.event.correlationId,
      },
      payload: entry.event.payload,
      metadata: entry.event.metadata,
      session: entry.event.payload.session,
      lifecycle: entry.event.metadata.providerRuntimeLifecycle,
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
    const terminalBeforeStart = providerLifecycleEntries.some((entry) => entry._tag === "Terminal");
    if (terminalBeforeStart) {
      return yield* error("provider-terminal-before-start", "terminal-conflict");
    }
    return { _tag: "Waiting" } as const;
  }
  const started = providerSessions[startSelection.index]!;
  const startedProviderName = started.event.payload.session.providerName;

  const terminalEntries: Array<{
    readonly entry: StoredSessionEvent;
    readonly source: VerificationTerminalSource;
  }> = [];
  for (const lifecycleEntry of providerLifecycleEntries) {
    if (lifecycleEntry._tag === "Start") continue;
    const { entry, source } = lifecycleEntry;
    if (entry.streamVersion <= started.streamVersion) {
      return yield* error("provider-terminal-before-start", "terminal-conflict");
    }
    const session = entry.event.payload.session;
    if (session.providerName !== startedProviderName) {
      return yield* error("provider-terminal-session-divergent", "terminal-conflict");
    }
    terminalEntries.push({ entry, source });
  }
  const terminalSelection = yield* selectVerificationProviderTerminal(
    terminalEntries.map(({ entry, source }) => ({
      streamVersion: entry.streamVersion,
      envelopeLineage: {
        eventId: entry.event.eventId,
        commandId: entry.event.commandId,
        causationEventId: entry.event.causationEventId,
        correlationId: entry.event.correlationId,
      },
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
    terminalSelection._tag === "Ready" ? terminalEntries[terminalSelection.index] : undefined;

  // Phase 3: session history and projection authority include every valid session event,
  // while the technical terminal remains anchored to provider lifecycle evidence.
  for (const entry of allSessions) {
    if (entry.streamVersion <= turnEntry.streamVersion) continue;
    const session = entry.event.payload.session;
    const commandId = entry.event.commandId;
    const actorCommandPrefix =
      entry.actorKind === "provider"
        ? "provider:"
        : entry.actorKind === "server"
          ? "server:"
          : null;
    if (
      entry.event.payload.threadId !== claim.evidence.threadId ||
      session.threadId !== claim.evidence.threadId ||
      session.runtimeMode !== claim.evidence.runtimeMode ||
      session.updatedAt !== entry.event.occurredAt
    ) {
      return yield* error("runtime-session-identity", "corrupt-history");
    }
    if (
      actorCommandPrefix === null ||
      commandId === null ||
      !commandId.startsWith(actorCommandPrefix) ||
      entry.event.causationEventId !== null ||
      entry.event.correlationId !== commandId
    ) {
      return yield* error("runtime-session-envelope-lineage", "corrupt-history");
    }
    if (entry.streamVersion <= started.streamVersion) continue;
    if (
      session.providerName !== startedProviderName ||
      (session.providerInstanceId !== undefined &&
        session.providerInstanceId !== claim.evidence.providerInstanceId) ||
      (session.activeTurnId !== null && session.activeTurnId !== providerTurnId)
    ) {
      return yield* error("runtime-session-divergent", "corrupt-history");
    }
  }

  if (terminal !== undefined) {
    const terminalSession = terminal.entry.event.payload.session;
    let latestSessionSnapshot = terminalSession;
    for (const entry of allSessions) {
      if (entry.streamVersion <= terminal.entry.streamVersion) {
        continue;
      }
      if (entry.event.metadata.providerRuntimeLifecycle !== undefined) {
        // Exact lifecycle replays were already compared as immutable technical evidence.
        // They also remain real session snapshots when they occur later in the stream.
        latestSessionSnapshot = entry.event.payload.session;
        continue;
      }
      // Provider lifecycle terminals are immutable technical evidence. A later lifecycle-free
      // session event is only the current provider/server snapshot and must never replace the
      // selected terminal observation, digest, timestamp, outcome, or provider turn identity.
      if (
        terminalSession.providerName === null ||
        !isProductionTerminalSessionSuffix(terminal.source, latestSessionSnapshot, entry, {
          providerInstanceId: claim.evidence.providerInstanceId,
          providerName: terminalSession.providerName,
          runtimeMode: terminalSession.runtimeMode,
        })
      ) {
        return yield* error("runtime-session-terminal-suffix", "corrupt-history");
      }
      latestSessionSnapshot = entry.event.payload.session;
    }
  }

  const latestSession = allSessions.at(-1);
  if (latestSession === undefined) return { _tag: "Waiting" } as const;
  const projectionRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(thread_id) AS "threadIdStorageClass",
      CAST(thread_id AS BLOB) AS "threadIdBytes",
      typeof(status) AS "statusStorageClass",
      CAST(status AS BLOB) AS "statusBytes",
      typeof(provider_name) AS "providerNameStorageClass",
      CASE WHEN provider_name IS NULL THEN NULL ELSE CAST(provider_name AS BLOB) END
        AS "providerNameBytes",
      typeof(provider_instance_id) AS "providerInstanceIdStorageClass",
      CASE WHEN provider_instance_id IS NULL THEN NULL ELSE CAST(provider_instance_id AS BLOB) END
        AS "providerInstanceIdBytes",
      typeof(runtime_mode) AS "runtimeModeStorageClass",
      CAST(runtime_mode AS BLOB) AS "runtimeModeBytes",
      typeof(active_turn_id) AS "activeTurnIdStorageClass",
      CASE WHEN active_turn_id IS NULL THEN NULL ELSE CAST(active_turn_id AS BLOB) END
        AS "activeTurnIdBytes",
      typeof(last_error) AS "lastErrorStorageClass",
      CASE WHEN last_error IS NULL THEN NULL ELSE CAST(last_error AS BLOB) END AS "lastErrorBytes",
      typeof(updated_at) AS "updatedAtStorageClass",
      CAST(updated_at AS BLOB) AS "updatedAtBytes"
    FROM projection_thread_sessions
    WHERE thread_id IN (${claim.evidence.threadId}, ${threadIdBytes})
  `.pipe(Effect.mapError((cause) => error("read-session-projection", "persistence", cause)));
  if (projectionRows.length === 0) return { _tag: "Waiting" } as const;
  if (projectionRows.length !== 1) {
    return yield* error("session-projection-count", "corrupt-history");
  }
  const projectionRow = projectionRows[0]!;
  for (const [storageClass, operation] of [
    [projectionRow.threadIdStorageClass, "session-projection-thread-id-storage-class"],
    [projectionRow.statusStorageClass, "session-projection-status-storage-class"],
    [projectionRow.runtimeModeStorageClass, "session-projection-runtime-mode-storage-class"],
    [projectionRow.updatedAtStorageClass, "session-projection-updated-at-storage-class"],
  ] as const) {
    if (storageClass !== "text") {
      return yield* error(operation, "corrupt-history");
    }
  }
  for (const [storageClass, operation] of [
    [projectionRow.providerNameStorageClass, "session-projection-provider-name-storage-class"],
    [
      projectionRow.providerInstanceIdStorageClass,
      "session-projection-provider-instance-id-storage-class",
    ],
    [projectionRow.activeTurnIdStorageClass, "session-projection-active-turn-id-storage-class"],
    [projectionRow.lastErrorStorageClass, "session-projection-last-error-storage-class"],
  ] as const) {
    if (storageClass !== "text" && storageClass !== "null") {
      return yield* error(operation, "corrupt-history");
    }
  }
  const [
    projectedThreadIdText,
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
  const projectedThreadId = yield* decodeThreadId(projectedThreadIdText).pipe(
    Effect.mapError((cause) =>
      error("decode-session-projection-thread-id", "corrupt-history", cause),
    ),
  );
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
  const matchingSessionPositions = allSessions.flatMap((entry, index) =>
    sameSessionProjection(projection, entry) ? [index] : [],
  );
  const latestMatchingSessionPosition = matchingSessionPositions.at(-1);
  if (latestMatchingSessionPosition === undefined) {
    return yield* error("session-projection-divergent", "corrupt-history");
  }
  if (latestMatchingSessionPosition !== allSessions.length - 1) {
    return { _tag: "Waiting" } as const;
  }

  if (terminal === undefined) {
    const laterDivergentActiveSession = allSessions.some((entry) => {
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
