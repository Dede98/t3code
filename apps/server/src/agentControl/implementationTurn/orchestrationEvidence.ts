import { OrchestrationEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  combinedInitialPlanningEventDigest,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  parseJsonStrict,
  sha256Utf8,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import type { AgentControlImplementationClaim } from "./model.ts";
import { decodeCanonicalOrLegacyOrchestrationMetadata } from "../../orchestration/providerRuntimeMessageCorrelation.ts";

const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEvent);

export type AgentControlImplementationOutcome = "succeeded" | "failed" | "cancelled";

export class AgentControlImplementationOrchestrationEvidenceError extends Schema.TaggedErrorClass<AgentControlImplementationOrchestrationEvidenceError>()(
  "AgentControlImplementationOrchestrationEvidenceError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["persistence", "corrupt-history", "ambiguous-terminal"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlImplementationStoredOrchestrationEvent {
  readonly event: OrchestrationEvent;
  readonly streamVersion: number;
  readonly actorKind: string;
  readonly envelopeJson: string;
}

export interface AgentControlImplementationOrchestrationEvidence {
  readonly history: ReadonlyArray<AgentControlImplementationStoredOrchestrationEvent>;
  readonly historyJson: string;
  readonly historyDigest: string;
  readonly started: AgentControlImplementationStoredOrchestrationEvent;
  readonly terminal: AgentControlImplementationStoredOrchestrationEvent | null;
  readonly outcome: AgentControlImplementationOutcome | null;
}

const error = (
  operation: string,
  reason: AgentControlImplementationOrchestrationEvidenceError["reason"],
  cause?: unknown,
) =>
  new AgentControlImplementationOrchestrationEvidenceError({
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

const decodeCanonicalJson = (value: unknown, operation: string) =>
  decodeText(value, `${operation}-bytes`).pipe(
    Effect.flatMap((source) =>
      Effect.try({
        try: () => {
          parseCanonicalJson(source);
          return source;
        },
        catch: (cause) => error(`${operation}-json`, "corrupt-history", cause),
      }),
    ),
  );

const decodeOrchestrationMetadata = (value: unknown, operation: string) =>
  decodeText(value, `${operation}-bytes`).pipe(
    Effect.flatMap((source) =>
      Effect.try({
        try: () => decodeCanonicalOrLegacyOrchestrationMetadata(source),
        catch: (cause) => error(`${operation}-json`, "corrupt-history", cause),
      }),
    ),
  );

const decodeStoredJson = (value: unknown, operation: string) =>
  decodeText(value, `${operation}-bytes`).pipe(
    Effect.flatMap((source) =>
      Effect.try({
        try: () => canonicalJson(parseJsonStrict(source)),
        catch: (cause) => error(`${operation}-json`, "corrupt-history", cause),
      }),
    ),
  );

const expectedOutcome = (
  state: AgentControlImplementationClaim["delivery"]["state"],
): AgentControlImplementationOutcome | null =>
  state === "completed"
    ? "succeeded"
    : state === "failed"
      ? "failed"
      : state === "interrupted"
        ? "cancelled"
        : null;

const terminalSessionStatus = (outcome: AgentControlImplementationOutcome): "ready" | "error" =>
  outcome === "failed" ? "error" : "ready";

const canonicalEnvelope = (input: {
  readonly event: OrchestrationEvent;
  readonly streamVersion: number;
  readonly actorKind: string;
}) =>
  canonicalJson({
    actorKind: input.actorKind,
    aggregateId: input.event.aggregateId,
    aggregateKind: input.event.aggregateKind,
    causationEventId: input.event.causationEventId,
    commandId: input.event.commandId,
    correlationId: input.event.correlationId,
    eventId: input.event.eventId,
    metadata: input.event.metadata as JsonValue,
    occurredAt: input.event.occurredAt,
    payload: input.event.payload as JsonValue,
    sequence: input.event.sequence,
    streamVersion: input.streamVersion,
    type: input.event.type,
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
  entry: AgentControlImplementationStoredOrchestrationEvent,
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

export const loadAgentControlImplementationOrchestrationEvidence = Effect.fn(
  "loadAgentControlImplementationOrchestrationEvidence",
)(function* (
  sql: SqlClient.SqlClient,
  claim: AgentControlImplementationClaim,
  options: { readonly requireTerminal: boolean },
) {
  const providerTurnId = claim.delivery.providerTurnId;
  const providerAcceptedAt = claim.delivery.providerAcceptedAt;
  if (providerTurnId === null || providerAcceptedAt === null) {
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
      typeof(payload_json) AS "payloadStorageClass",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${claim.evidence.threadId}
    ORDER BY stream_version, sequence
  `.pipe(Effect.mapError((cause) => error("read-orchestration-history", "persistence", cause)));
  if (rawRows.length === 0) return yield* error("orchestration-history-missing", "corrupt-history");

  const history: Array<AgentControlImplementationStoredOrchestrationEvent> = [];
  let previousSequence = 0;
  for (const [index, row] of rawRows.entries()) {
    const sequence = row.sequence;
    const streamVersion = row.streamVersion;
    if (
      typeof sequence !== "number" ||
      !Number.isInteger(sequence) ||
      sequence <= previousSequence ||
      typeof streamVersion !== "number" ||
      !Number.isInteger(streamVersion) ||
      streamVersion !== index + 1
    ) {
      return yield* error("orchestration-history-order", "corrupt-history");
    }
    previousSequence = sequence;
    if (row.payloadStorageClass !== "text" || row.metadataStorageClass !== "text") {
      return yield* error("orchestration-json-storage", "corrupt-history");
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
      actorKind,
      payloadJson,
      metadata,
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
      decodeStoredJson(row.payloadBytes, "orchestration-payload"),
      decodeOrchestrationMetadata(row.metadataBytes, "orchestration-metadata"),
    ]);
    if (aggregateKind !== "thread" || aggregateId !== claim.evidence.threadId) {
      return yield* error("orchestration-stream-identity", "corrupt-history");
    }
    const event = yield* decodeOrchestrationEvent({
      sequence,
      eventId,
      aggregateKind,
      aggregateId,
      type,
      occurredAt,
      commandId,
      causationEventId,
      correlationId,
      payload: parseCanonicalJson(payloadJson),
      metadata,
    }).pipe(
      Effect.mapError((cause) => error("decode-orchestration-event", "corrupt-history", cause)),
    );
    const entry = { event, streamVersion, actorKind, envelopeJson: "" };
    history.push({ ...entry, envelopeJson: canonicalEnvelope(entry) });
  }

  const acceptanceRows = yield* sql<Record<string, unknown>>`
    SELECT message_event_sequence AS "messageEventSequence",
      turn_request_event_sequence AS "turnRequestEventSequence",
      CAST(message_event_envelope_json AS BLOB) AS "messageEnvelopeBytes",
      CAST(turn_request_event_envelope_json AS BLOB) AS "turnEnvelopeBytes",
      CAST(event_evidence_digest AS BLOB) AS "eventEvidenceDigestBytes"
    FROM agent_control_implementation_turn_accepted
    WHERE handoff_id = ${claim.evidence.handoffId}
  `.pipe(Effect.mapError((cause) => error("read-turn-acceptance", "persistence", cause)));
  if (acceptanceRows.length !== 1) return yield* error("turn-acceptance-chain", "corrupt-history");
  const acceptance = acceptanceRows[0]!;
  if (
    typeof acceptance.messageEventSequence !== "number" ||
    typeof acceptance.turnRequestEventSequence !== "number"
  ) {
    return yield* error("turn-acceptance-storage", "corrupt-history");
  }
  const messageEnvelope = yield* decodeCanonicalJson(
    acceptance.messageEnvelopeBytes,
    "message-envelope",
  );
  const turnEnvelope = yield* decodeCanonicalJson(acceptance.turnEnvelopeBytes, "turn-envelope");
  const eventEvidenceDigest = yield* decodeText(
    acceptance.eventEvidenceDigestBytes,
    "turn-acceptance-digest",
  );
  if (eventEvidenceDigest !== combinedInitialPlanningEventDigest(messageEnvelope, turnEnvelope)) {
    return yield* error("turn-acceptance-digest", "corrupt-history");
  }

  const messageEvents = history.filter(
    (entry) => entry.event.eventId === claim.evidence.messageEventId,
  );
  const turnEvents = history.filter(
    (entry) => entry.event.eventId === claim.evidence.turnRequestEventId,
  );
  if (messageEvents.length !== 1 || turnEvents.length !== 1) {
    return yield* error("turn-request-identity", "corrupt-history");
  }
  const message = messageEvents[0]!;
  const turn = turnEvents[0]!;
  if (
    message.event.type !== "thread.message-sent" ||
    turn.event.type !== "thread.turn-start-requested" ||
    message.actorKind !== "client" ||
    turn.actorKind !== "client" ||
    message.streamVersion !== 3 ||
    turn.streamVersion !== 4 ||
    message.event.sequence !== acceptance.messageEventSequence ||
    turn.event.sequence !== acceptance.turnRequestEventSequence ||
    message.event.commandId !== claim.evidence.turnRequestCommandId ||
    turn.event.commandId !== claim.evidence.turnRequestCommandId ||
    message.event.causationEventId !== null ||
    turn.event.causationEventId !== claim.evidence.messageEventId ||
    message.event.correlationId !== claim.evidence.turnRequestCommandId ||
    turn.event.correlationId !== claim.evidence.turnRequestCommandId ||
    message.envelopeJson !== messageEnvelope ||
    turn.envelopeJson !== turnEnvelope ||
    message.event.payload.threadId !== claim.evidence.threadId ||
    message.event.payload.messageId !== claim.evidence.messageId ||
    message.event.payload.role !== "user" ||
    message.event.payload.text !== claim.evidence.promptText ||
    turn.event.payload.threadId !== claim.evidence.threadId ||
    turn.event.payload.messageId !== claim.evidence.messageId ||
    turn.event.payload.runtimeMode !== claim.evidence.runtimeMode ||
    turn.event.payload.interactionMode !== "default" ||
    canonicalJson(turn.event.payload.modelSelection as JsonValue) !==
      claim.evidence.modelSelectionJson ||
    turn.event.payload.sourceProposedPlan?.threadId !== claim.evidence.planningThreadId ||
    turn.event.payload.sourceProposedPlan?.planId !== claim.evidence.planId
  ) {
    return yield* error("turn-request-history", "corrupt-history");
  }

  const threadCreated = history.filter((entry) => entry.event.type === "thread.created");
  const threadBound = history.filter((entry) => entry.event.type === "thread.agent-control-bound");
  if (threadCreated.length !== 1 || threadBound.length !== 1) {
    return yield* error("thread-materialization-history", "corrupt-history");
  }
  const created = threadCreated[0]!.event;
  const bound = threadBound[0]!.event;
  if (
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
    bound.payload.binding.roleId !== "implementer" ||
    bound.payload.binding.controlState !== "controlled"
  ) {
    return yield* error("thread-materialization-binding", "corrupt-history");
  }

  const providerSessions = history.filter(
    (
      entry,
    ): entry is AgentControlImplementationStoredOrchestrationEvent & {
      readonly event: Extract<OrchestrationEvent, { readonly type: "thread.session-set" }>;
    } => entry.event.type === "thread.session-set" && entry.actorKind === "provider",
  );
  const starts = providerSessions.filter((entry) => {
    const session = entry.event.payload.session;
    return (
      entry.event.occurredAt === providerAcceptedAt &&
      session.updatedAt === providerAcceptedAt &&
      session.threadId === claim.evidence.threadId &&
      session.status === "running" &&
      session.activeTurnId === providerTurnId &&
      session.providerInstanceId === claim.evidence.providerInstanceId &&
      session.runtimeMode === claim.evidence.runtimeMode &&
      session.providerName !== null
    );
  });
  if (starts.length === 0) return { _tag: "Waiting" } as const;
  if (starts.length !== 1) return yield* error("provider-start-ambiguous", "corrupt-history");
  const started = starts[0]!;
  const providerName = started.event.payload.session.providerName;

  const outcome = expectedOutcome(claim.delivery.state);
  let terminal: AgentControlImplementationStoredOrchestrationEvent | null = null;
  if (outcome !== null) {
    if (claim.delivery.terminalAt === null) {
      return yield* error("delivery-terminal-time", "corrupt-history");
    }
    const terminalLike = providerSessions.filter((entry) => {
      const session = entry.event.payload.session;
      return (
        entry.event.sequence > started.event.sequence &&
        session.threadId === claim.evidence.threadId &&
        session.providerInstanceId === claim.evidence.providerInstanceId &&
        session.providerName === providerName &&
        session.runtimeMode === claim.evidence.runtimeMode &&
        session.activeTurnId === null &&
        (session.status === "ready" || session.status === "error")
      );
    });
    const candidates = terminalLike.filter(
      (entry) =>
        entry.event.occurredAt === claim.delivery.terminalAt &&
        entry.event.payload.session.updatedAt === claim.delivery.terminalAt &&
        entry.event.payload.session.status === terminalSessionStatus(outcome),
    );
    if (terminalLike.length > 0 && (terminalLike.length !== 1 || candidates.length !== 1)) {
      return yield* error("provider-terminal-conflict", "ambiguous-terminal");
    }
    terminal = candidates[0] ?? null;
    if (terminal === null && options.requireTerminal) return { _tag: "Waiting" } as const;
  } else if (options.requireTerminal) {
    return { _tag: "Waiting" } as const;
  }

  const chainEnd = terminal?.event.sequence ?? Number.POSITIVE_INFINITY;
  for (const entry of providerSessions) {
    if (entry.event.sequence < started.event.sequence || entry.event.sequence > chainEnd) continue;
    const session = entry.event.payload.session;
    if (
      session.threadId !== claim.evidence.threadId ||
      session.providerInstanceId !== claim.evidence.providerInstanceId ||
      session.providerName !== providerName ||
      session.runtimeMode !== claim.evidence.runtimeMode ||
      (session.activeTurnId !== null && session.activeTurnId !== providerTurnId)
    ) {
      return yield* error("provider-session-chain", "corrupt-history");
    }
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
  if (projectionRows.length !== 1)
    return yield* error("session-projection-count", "corrupt-history");
  const projectionRow = projectionRows[0]!;
  const [
    threadId,
    status,
    projectedProviderName,
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
    threadId,
    status,
    providerName: projectedProviderName,
    providerInstanceId,
    runtimeMode,
    activeTurnId,
    lastError,
    updatedAt,
  };
  if (!sameSessionProjection(projection, latestSession)) {
    if (projection.updatedAt < latestSession.event.occurredAt) return { _tag: "Waiting" } as const;
    return yield* error("session-projection-divergent", "corrupt-history");
  }
  if (terminal !== null && latestSession.event.eventId !== terminal.event.eventId) {
    return yield* error("provider-terminal-not-latest", "ambiguous-terminal");
  }

  const historyJson = canonicalJson(
    history.map((entry) => parseCanonicalJson(entry.envelopeJson)) as JsonValue,
  );
  return {
    _tag: "Ready",
    evidence: {
      history,
      historyJson,
      historyDigest: sha256Utf8(historyJson),
      started,
      terminal,
      outcome,
    } satisfies AgentControlImplementationOrchestrationEvidence,
  } as const;
});
