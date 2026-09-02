import {
  AgentControlTaskEvent,
  AgentControlTaskId,
  AgentControlTaskState,
  AgentControlWorktreeEvent,
  AgentControlWorktreeReservationId,
  EventId,
  IsoDateTime,
  PositiveInt,
  ProjectId,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import { projectAgentControlTaskEvent } from "../task/projector.ts";
import { decodeAgentControlTaskProjectionRow } from "../task/Layers/AgentControlTaskStateRepository.ts";
import { sameAgentControlWorktreeReservationState } from "../worktree/authoritative.ts";
import { projectAgentControlWorktreeEvent } from "../worktree/projector.ts";
import {
  AGENT_CONTROL_WORKTREE_STATE_SELECT,
  decodeAgentControlWorktreeProjectionRow,
} from "../worktree/Layers/AgentControlWorktreeStateRepository.ts";

export const AgentControlVerificationHistoricalAuthorityReason = Schema.Literals([
  "history-missing",
  "history-divergent",
  "history-undecodable",
  "projection-missing",
  "projection-divergent",
]);
export type AgentControlVerificationHistoricalAuthorityReason =
  typeof AgentControlVerificationHistoricalAuthorityReason.Type;

export class AgentControlVerificationHistoricalAuthorityError extends Schema.TaggedErrorClass<AgentControlVerificationHistoricalAuthorityError>()(
  "AgentControlVerificationHistoricalAuthorityError",
  {
    operation: Schema.String,
    reason: AgentControlVerificationHistoricalAuthorityReason,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const historyError = (
  operation: string,
  reason: AgentControlVerificationHistoricalAuthorityReason,
  cause?: unknown,
) =>
  new AgentControlVerificationHistoricalAuthorityError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const EventCoordinates = Schema.Struct({
  sequence: PositiveInt,
  eventId: EventId,
  aggregateKind: Schema.String,
  aggregateId: Schema.String,
  streamVersion: PositiveInt,
  type: Schema.String,
  occurredAt: IsoDateTime,
  commandId: Schema.String,
  causationEventId: Schema.NullOr(Schema.String),
  correlationId: Schema.String,
  authority: Schema.String,
  payloadStorageClass: Schema.Literal("text"),
  payloadBytes: Schema.Unknown,
  metadataStorageClass: Schema.Literal("text"),
  metadataBytes: Schema.Unknown,
});
const WorktreeCatalogRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  fenceToken: PositiveInt,
  createdAt: IsoDateTime,
  initialEventId: EventId,
  initialStreamVersion: Schema.Literal(1),
});
const WorktreeEnvelopeRow = Schema.Struct({
  eventId: EventId,
  reservationId: AgentControlWorktreeReservationId,
  streamVersion: PositiveInt,
  eventType: Schema.String,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  fenceToken: PositiveInt,
  createdAt: IsoDateTime,
});

const decodeEventCoordinates = Schema.decodeUnknownEffect(EventCoordinates);
const decodeTaskEvent = Schema.decodeUnknownEffect(AgentControlTaskEvent);
const decodeWorktreeEvent = Schema.decodeUnknownEffect(AgentControlWorktreeEvent);
const encodeTaskEvent = Schema.encodeUnknownEffect(AgentControlTaskEvent);
const encodeWorktreeEvent = Schema.encodeUnknownEffect(AgentControlWorktreeEvent);
const decodeWorktreeCatalog = Schema.decodeUnknownEffect(WorktreeCatalogRow);
const decodeWorktreeEnvelope = Schema.decodeUnknownEffect(WorktreeEnvelopeRow);

const decodeStoredEvent = Effect.fn("decodeStoredVerificationAuthorityEvent")(function* <A>(
  raw: Record<string, unknown>,
  decode: (input: unknown) => Effect.Effect<A, Schema.SchemaError>,
  encode: (input: A) => Effect.Effect<unknown, Schema.SchemaError>,
  operation: string,
) {
  const coordinates = yield* decodeEventCoordinates(raw).pipe(
    Effect.mapError((cause) => historyError(operation, "history-undecodable", cause)),
  );
  const payloadSource = yield* Effect.try({
    try: () => decodeCanonicalUtf8Bytes(coordinates.payloadBytes),
    catch: (cause) => historyError(`${operation}-payload-bytes`, "history-undecodable", cause),
  });
  const metadataSource = yield* Effect.try({
    try: () => decodeCanonicalUtf8Bytes(coordinates.metadataBytes),
    catch: (cause) => historyError(`${operation}-metadata-bytes`, "history-undecodable", cause),
  });
  const payload = yield* Effect.try({
    try: () => parseJsonStrict(payloadSource),
    catch: (cause) => historyError(`${operation}-payload-json`, "history-undecodable", cause),
  });
  const metadata = yield* Effect.try({
    try: () => parseJsonStrict(metadataSource),
    catch: (cause) => historyError(`${operation}-metadata-json`, "history-undecodable", cause),
  });
  const event = yield* decode({
    sequence: coordinates.sequence,
    eventId: coordinates.eventId,
    aggregateKind: coordinates.aggregateKind,
    aggregateId: coordinates.aggregateId,
    streamVersion: coordinates.streamVersion,
    type: coordinates.type,
    occurredAt: coordinates.occurredAt,
    commandId: coordinates.commandId,
    causationEventId: coordinates.causationEventId,
    correlationId: coordinates.correlationId,
    authority: coordinates.authority,
    payload,
    metadata,
  }).pipe(Effect.mapError((cause) => historyError(operation, "history-undecodable", cause)));
  const encoded = yield* encode(event).pipe(
    Effect.mapError((cause) => historyError(`${operation}-reencode`, "history-undecodable", cause)),
  );
  if (encoded === null || typeof encoded !== "object" || Array.isArray(encoded)) {
    return yield* historyError(`${operation}-reencode-shape`, "history-undecodable");
  }
  const encodedEvent = encoded as { readonly payload?: unknown; readonly metadata?: unknown };
  const acceptedSource = (source: string, typedValue: unknown): boolean =>
    source === JSON.stringify(typedValue) || source === canonicalJson(typedValue as JsonValue);
  if (!acceptedSource(payloadSource, encodedEvent.payload)) {
    return yield* historyError(`${operation}-payload-shape`, "history-divergent");
  }
  if (!acceptedSource(metadataSource, encodedEvent.metadata)) {
    return yield* historyError(`${operation}-metadata-shape`, "history-divergent");
  }
  return event;
});

export const decodeAgentControlVerificationStoredTaskEvent = (raw: Record<string, unknown>) =>
  decodeStoredEvent(raw, decodeTaskEvent, encodeTaskEvent, "task-history-event");

export const decodeAgentControlVerificationStoredWorktreeEvent = (raw: Record<string, unknown>) =>
  decodeStoredEvent(raw, decodeWorktreeEvent, encodeWorktreeEvent, "worktree-history-event");

const sameTaskState = (left: AgentControlTaskState, right: AgentControlTaskState) =>
  canonicalJson(left as unknown as JsonValue) === canonicalJson(right as unknown as JsonValue);

export const loadAgentControlVerificationTaskAuthorityInTransaction = Effect.fn(
  "loadAgentControlVerificationTaskAuthorityInTransaction",
)(function* (sql: SqlClient.SqlClient, taskId: AgentControlTaskId, targetRevision: number) {
  const rawEvents = yield* sql<Record<string, unknown>>`
    SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
      stream_id AS "aggregateId", stream_version AS "streamVersion",
      event_type AS type, occurred_at AS "occurredAt", command_id AS "commandId",
      causation_event_id AS "causationEventId", correlation_id AS "correlationId",
      actor_authority AS authority, typeof(payload_json) AS "payloadStorageClass",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.agent_control_events
    WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
      AND stream_version <= ${targetRevision}
    ORDER BY stream_version, sequence
  `;
  if (rawEvents.length === 0) return yield* historyError("task-history", "history-missing");
  if (rawEvents.length !== targetRevision) {
    return yield* historyError("task-history-count", "history-divergent");
  }
  const events = yield* Effect.forEach(rawEvents, (row) =>
    decodeAgentControlVerificationStoredTaskEvent(row),
  );
  let state: AgentControlTaskState | null = null;
  for (const [index, event] of events.entries()) {
    if (
      event.aggregateId !== taskId ||
      event.streamVersion !== index + 1 ||
      (index > 0 && event.sequence <= events[index - 1]!.sequence)
    ) {
      return yield* historyError("task-history-order", "history-divergent");
    }
    state = yield* projectAgentControlTaskEvent(state, event).pipe(
      Effect.mapError((cause) => historyError("task-history-project", "history-divergent", cause)),
    );
  }
  if (state === null || state.revision !== targetRevision) {
    return yield* historyError("task-history-state", "history-divergent");
  }
  const projectionRows = yield* sql<Record<string, unknown>>`
    SELECT typeof(state_json) AS "stateStorageClass",
      CAST(state_json AS BLOB) AS "stateBytes", task_id AS "taskId",
      project_id AS "projectId", revision, last_event_sequence AS sequence,
      repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
      issue_number AS "issueNumber", issue_url AS "issueUrl", status,
      source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
      github_intake_sequence AS "githubIntakeSequence",
      created_at AS "createdAt", updated_at AS "updatedAt"
    FROM main.agent_control_task_states WHERE task_id = ${taskId}
  `;
  if (projectionRows.length === 0) {
    return yield* historyError("task-projection", "projection-missing");
  }
  if (projectionRows.length !== 1) {
    return yield* historyError("task-projection-count", "projection-divergent");
  }
  if (projectionRows[0]!.stateStorageClass !== "text") {
    return yield* historyError("task-projection-storage", "projection-divergent");
  }
  const { stateStorageClass: _taskStorageClass, ...taskProjectionCoordinates } = projectionRows[0]!;
  const projectionSource = yield* Effect.try({
    try: () => decodeCanonicalUtf8Bytes(projectionRows[0]!.stateBytes),
    catch: (cause) => historyError("task-projection-bytes", "projection-divergent", cause),
  });
  const projection = yield* decodeAgentControlTaskProjectionRow(
    { ...taskProjectionCoordinates, state: projectionSource },
    "verification-task-projection",
  ).pipe(
    Effect.mapError((cause) =>
      historyError("task-projection-decode", "projection-divergent", cause),
    ),
  );
  if (
    projection.revision < targetRevision ||
    (projection.revision === targetRevision && !sameTaskState(projection, state))
  ) {
    return yield* historyError("task-projection-authority", "projection-divergent");
  }
  return { state, event: events.at(-1)!, events, projection };
});

export const loadAgentControlVerificationWorktreeAuthorityInTransaction = Effect.fn(
  "loadAgentControlVerificationWorktreeAuthorityInTransaction",
)(function* (
  sql: SqlClient.SqlClient,
  reservationId: AgentControlWorktreeReservationId,
  target?: {
    readonly eventId: EventId;
    readonly sequence: number;
    readonly streamVersion: number;
  },
) {
  const catalogRows = yield* sql<Record<string, unknown>>`
    SELECT reservation_id AS "reservationId", project_id AS "projectId",
      task_id AS "taskId", stage_run_id AS "stageRunId", attempt_id AS "attemptId",
      lease_id AS "leaseId", fence_token AS "fenceToken", created_at AS "createdAt",
      initial_event_id AS "initialEventId", initial_stream_version AS "initialStreamVersion"
    FROM main.agent_control_worktree_stream_catalog WHERE reservation_id = ${reservationId}
  `;
  const envelopeRows = yield* sql<Record<string, unknown>>`
    SELECT event_id AS "eventId", reservation_id AS "reservationId",
      stream_version AS "streamVersion", event_type AS "eventType",
      project_id AS "projectId", task_id AS "taskId", stage_run_id AS "stageRunId",
      attempt_id AS "attemptId", lease_id AS "leaseId", fence_token AS "fenceToken",
      created_at AS "createdAt"
    FROM main.agent_control_worktree_event_envelopes WHERE reservation_id = ${reservationId}
    ORDER BY stream_version, event_id
  `;
  const rawEvents = yield* sql<Record<string, unknown>>`
    SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
      stream_id AS "aggregateId", stream_version AS "streamVersion",
      event_type AS type, occurred_at AS "occurredAt", command_id AS "commandId",
      causation_event_id AS "causationEventId", correlation_id AS "correlationId",
      actor_authority AS authority, typeof(payload_json) AS "payloadStorageClass",
      CAST(payload_json AS BLOB) AS "payloadBytes",
      typeof(metadata_json) AS "metadataStorageClass",
      CAST(metadata_json AS BLOB) AS "metadataBytes"
    FROM main.agent_control_events
    WHERE aggregate_kind = 'worktree-reservation' AND stream_id = ${reservationId}
    ORDER BY stream_version, sequence
  `;
  if (catalogRows.length + envelopeRows.length + rawEvents.length === 0) {
    return yield* historyError("worktree-history", "history-missing");
  }
  if (
    catalogRows.length !== 1 ||
    envelopeRows.length === 0 ||
    envelopeRows.length !== rawEvents.length
  ) {
    return yield* historyError("worktree-history-relations", "history-divergent");
  }
  const catalog = yield* decodeWorktreeCatalog(catalogRows[0]).pipe(
    Effect.mapError((cause) => historyError("worktree-catalog", "history-undecodable", cause)),
  );
  const envelopes = yield* Effect.forEach(envelopeRows, (row) =>
    decodeWorktreeEnvelope(row).pipe(
      Effect.mapError((cause) => historyError("worktree-envelope", "history-undecodable", cause)),
    ),
  );
  const events = yield* Effect.forEach(rawEvents, (row) =>
    decodeAgentControlVerificationStoredWorktreeEvent(row),
  );
  const selectedIndex =
    target === undefined
      ? events.length - 1
      : events.findIndex(
          (event) =>
            event.eventId === target.eventId &&
            event.sequence === target.sequence &&
            event.streamVersion === target.streamVersion,
        );
  if (selectedIndex < 0) {
    return yield* historyError("worktree-history-target", "history-divergent");
  }
  let state: AgentControlWorktreeReservationState | null = null;
  let selectedState: AgentControlWorktreeReservationState | null = null;
  for (const [index, event] of events.entries()) {
    const envelope = envelopes[index]!;
    if (
      event.aggregateId !== reservationId ||
      event.streamVersion !== index + 1 ||
      (index > 0 && event.sequence <= events[index - 1]!.sequence) ||
      envelope.eventId !== event.eventId ||
      envelope.reservationId !== reservationId ||
      envelope.streamVersion !== event.streamVersion ||
      envelope.eventType !== event.type ||
      envelope.projectId !== catalog.projectId ||
      envelope.taskId !== catalog.taskId ||
      envelope.stageRunId !== catalog.stageRunId ||
      envelope.attemptId !== catalog.attemptId ||
      envelope.leaseId !== catalog.leaseId ||
      envelope.fenceToken !== catalog.fenceToken ||
      envelope.createdAt !== event.occurredAt
    ) {
      return yield* historyError("worktree-history-order", "history-divergent");
    }
    state = yield* projectAgentControlWorktreeEvent(state, event).pipe(
      Effect.mapError((cause) =>
        historyError("worktree-history-project", "history-divergent", cause),
      ),
    );
    if (index === selectedIndex) selectedState = state;
  }
  if (
    state === null ||
    catalog.reservationId !== reservationId ||
    catalog.initialEventId !== events[0]!.eventId ||
    catalog.initialStreamVersion !== 1 ||
    catalog.createdAt !== events[0]!.occurredAt ||
    catalog.projectId !== state.projectId ||
    catalog.taskId !== state.taskId ||
    catalog.stageRunId !== state.stageRunId ||
    catalog.attemptId !== state.attemptId ||
    catalog.leaseId !== state.leaseId ||
    catalog.fenceToken !== state.fenceToken
  ) {
    return yield* historyError("worktree-history-state", "history-divergent");
  }
  const projectionRows = yield* sql.unsafe<Record<string, unknown>>(
    `SELECT typeof(state_json) AS stateStorageClass,
       ${AGENT_CONTROL_WORKTREE_STATE_SELECT.replace("state_json AS state", "CAST(state_json AS BLOB) AS stateBytes")}
     FROM main.agent_control_worktree_reservation_states WHERE reservation_id = ?`,
    [reservationId],
  );
  if (projectionRows.length === 0) {
    return yield* historyError("worktree-projection", "projection-missing");
  }
  if (projectionRows.length !== 1) {
    return yield* historyError("worktree-projection-count", "projection-divergent");
  }
  if (projectionRows[0]!.stateStorageClass !== "text") {
    return yield* historyError("worktree-projection-storage", "projection-divergent");
  }
  const { stateStorageClass: _worktreeStorageClass, ...worktreeProjectionCoordinates } =
    projectionRows[0]!;
  const projectionSource = yield* Effect.try({
    try: () => decodeCanonicalUtf8Bytes(projectionRows[0]!.stateBytes),
    catch: (cause) => historyError("worktree-projection-bytes", "projection-divergent", cause),
  });
  const projection = yield* decodeAgentControlWorktreeProjectionRow(
    { ...worktreeProjectionCoordinates, state: projectionSource },
    "verification-worktree-projection",
  ).pipe(
    Effect.mapError((cause) =>
      historyError("worktree-projection-decode", "projection-divergent", cause),
    ),
  );
  if (!sameAgentControlWorktreeReservationState(state, projection)) {
    return yield* historyError("worktree-projection-authority", "projection-divergent");
  }
  if (selectedState === null) {
    return yield* historyError("worktree-history-target-state", "history-divergent");
  }
  return { state: selectedState, event: events[selectedIndex]!, events, projection };
});
