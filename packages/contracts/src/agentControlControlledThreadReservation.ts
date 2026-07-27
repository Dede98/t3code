/**
 * Schema-only contracts for a server-prepared Controlled Thread reservation.
 *
 * A reservation binds a future orchestration thread identity to one canonical
 * task/stage/lease/worktree snapshot. It is not execution authority and does
 * not create an orchestration thread, provider session, or turn.
 *
 * @module agentControlControlledThreadReservation
 */
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Option from "effect/Option";

import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { AgentControlStageKind } from "./agentControlStageRun.ts";

export const AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_RPC_METHODS = {
  get: "agentControlControlledThreadReservation.get",
  list: "agentControlControlledThreadReservation.list",
  prepareInitial: "agentControlControlledThreadReservation.prepareInitial",
} as const;

const ReservationBinding = {
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: Schema.Literal("planning"),
  stageOrdinal: Schema.Literal(1),
  attemptOrdinal: Schema.Literal(1),
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  worktreeReservationId: AgentControlWorktreeReservationId,
} as const;

const PreparedState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  ...ReservationBinding,
  status: Schema.Literal("prepared"),
  revision: Schema.Literal(1),
  sequence: PositiveInt,
  preparedAt: IsoDateTime,
});
const MaterializingBinding = {
  coordinatorCommandId: CommandId,
  coordinatorCommandFingerprint: TrimmedNonEmptyString,
  materializingTransitionCommandId: CommandId,
  materializationCommandId: CommandId,
  materializationCommandFingerprint: TrimmedNonEmptyString,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  materializingAt: IsoDateTime,
} as const;
const MaterializingState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  ...ReservationBinding,
  status: Schema.Literal("materializing"),
  revision: Schema.Literal(2),
  sequence: PositiveInt,
  preparedAt: IsoDateTime,
  ...MaterializingBinding,
});
const BoundBinding = {
  ...MaterializingBinding,
  boundTransitionCommandId: CommandId,
  orchestrationResultSequence: PositiveInt,
  materializedAt: IsoDateTime,
  boundAt: IsoDateTime,
} as const;
const BoundState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  ...ReservationBinding,
  status: Schema.Literal("bound"),
  revision: Schema.Literal(3),
  sequence: PositiveInt,
  preparedAt: IsoDateTime,
  ...BoundBinding,
});

export const AgentControlControlledThreadReservationState = Schema.Union([
  PreparedState,
  MaterializingState,
  BoundState,
]);
export type AgentControlControlledThreadReservationState =
  typeof AgentControlControlledThreadReservationState.Type;

/** The only reservation fields that may cross the transport boundary. */
export const AgentControlControlledThreadReservationView = Schema.Struct({
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  status: Schema.Literals(["prepared", "materializing", "bound"]),
  revision: PositiveInt,
  preparedAt: IsoDateTime,
});
export type AgentControlControlledThreadReservationView =
  typeof AgentControlControlledThreadReservationView.Type;

export const AgentControlControlledThreadReservationGetInput = Schema.Struct({
  projectId: ProjectId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
});
export type AgentControlControlledThreadReservationGetInput =
  typeof AgentControlControlledThreadReservationGetInput.Type;

export const AgentControlControlledThreadReservationListInput = Schema.Struct({
  projectId: ProjectId,
});
export type AgentControlControlledThreadReservationListInput =
  typeof AgentControlControlledThreadReservationListInput.Type;

export const AgentControlControlledThreadReservationListResult = Schema.Struct({
  projectId: ProjectId,
  reservations: Schema.Array(AgentControlControlledThreadReservationView),
  quarantinedCount: NonNegativeInt,
});
export type AgentControlControlledThreadReservationListResult =
  typeof AgentControlControlledThreadReservationListResult.Type;

/** The client supplies identity for neither the reservation nor the thread. */
export const AgentControlControlledThreadReservationPrepareInitialInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
});
export type AgentControlControlledThreadReservationPrepareInitialInput =
  typeof AgentControlControlledThreadReservationPrepareInitialInput.Type;

const PREPARE_INITIAL_TRANSPORT_KEYS = ["commandId", "projectId", "taskId"] as const;
const exactPrepareInitialTransportObject = Schema.makeFilter<unknown>(
  (input) => {
    if (
      typeof input !== "object" ||
      input === null ||
      (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null)
    ) {
      return new SchemaIssue.InvalidValue(Option.some(input), {
        message: "prepareInitial payload must be a plain object",
      });
    }
    const keys = Reflect.ownKeys(input);
    if (
      keys.length !== PREPARE_INITIAL_TRANSPORT_KEYS.length ||
      keys.some(
        (key) =>
          typeof key !== "string" ||
          !PREPARE_INITIAL_TRANSPORT_KEYS.includes(
            key as (typeof PREPARE_INITIAL_TRANSPORT_KEYS)[number],
          ),
      )
    ) {
      return new SchemaIssue.InvalidValue(Option.some(input), {
        message: "prepareInitial payload contains unknown fields",
      });
    }
    return true;
  },
  { identifier: "AgentControlControlledThreadReservationPrepareInitialTransportObject" },
);

/**
 * Transport-only decoder. The raw object's own keys are validated before the
 * Struct decoder can strip excess properties.
 */
export const AgentControlControlledThreadReservationPrepareInitialTransportInput =
  Schema.Unknown.check(exactPrepareInitialTransportObject).pipe(
    Schema.decodeTo(AgentControlControlledThreadReservationPrepareInitialInput),
  );

const CommandBinding = {
  commandId: CommandId,
  authority: Schema.Literals(["human", "controller", "system"]),
  ...ReservationBinding,
  stageKind: AgentControlStageKind,
  stageOrdinal: PositiveInt,
  attemptOrdinal: PositiveInt,
} as const;

/** Server-internal command assembled only from authoritative state. */
export const AgentControlControlledThreadReservationPrepareCommand = Schema.Struct({
  ...CommandBinding,
  type: Schema.Literal("agentControl.controlledThreadReservation.prepare"),
  expectedRevision: Schema.Literal(0),
});
export type AgentControlControlledThreadReservationPrepareCommand =
  typeof AgentControlControlledThreadReservationPrepareCommand.Type;

export const AgentControlControlledThreadReservationBeginMaterializationCommand = Schema.Struct({
  ...CommandBinding,
  type: Schema.Literal("agentControl.controlledThreadReservation.beginMaterialization"),
  expectedRevision: Schema.Literal(1),
  ...MaterializingBinding,
});
export type AgentControlControlledThreadReservationBeginMaterializationCommand =
  typeof AgentControlControlledThreadReservationBeginMaterializationCommand.Type;

export const AgentControlControlledThreadReservationBindMaterializationCommand = Schema.Struct({
  ...CommandBinding,
  type: Schema.Literal("agentControl.controlledThreadReservation.bindMaterialization"),
  expectedRevision: Schema.Literal(2),
  ...BoundBinding,
});
export type AgentControlControlledThreadReservationBindMaterializationCommand =
  typeof AgentControlControlledThreadReservationBindMaterializationCommand.Type;

/** Closed future transition contract. Release and invalidation remain unavailable. */
export const AgentControlControlledThreadReservationUnavailableCommand = Schema.Struct({
  ...CommandBinding,
  type: Schema.Literal("agentControl.controlledThreadReservation.transition"),
  expectedRevision: NonNegativeInt,
  targetStatus: Schema.Literals(["materializing", "bound", "released", "invalidated"]),
});
export type AgentControlControlledThreadReservationUnavailableCommand =
  typeof AgentControlControlledThreadReservationUnavailableCommand.Type;

export const AgentControlControlledThreadReservationCommand = Schema.Union([
  AgentControlControlledThreadReservationPrepareCommand,
  AgentControlControlledThreadReservationBeginMaterializationCommand,
  AgentControlControlledThreadReservationBindMaterializationCommand,
  AgentControlControlledThreadReservationUnavailableCommand,
]);
export type AgentControlControlledThreadReservationCommand =
  typeof AgentControlControlledThreadReservationCommand.Type;

export const AgentControlControlledThreadReservationCommandResult = Schema.Struct({
  reservation: AgentControlControlledThreadReservationView,
  resultSequence: PositiveInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlControlledThreadReservationCommandResult =
  typeof AgentControlControlledThreadReservationCommandResult.Type;

export const AgentControlControlledThreadReservationPreparedPayload = Schema.Struct({
  ...ReservationBinding,
  status: Schema.Literal("prepared"),
  preparedAt: IsoDateTime,
});
export type AgentControlControlledThreadReservationPreparedPayload =
  typeof AgentControlControlledThreadReservationPreparedPayload.Type;

export const AgentControlControlledThreadReservationMaterializingPayload = Schema.Struct({
  ...ReservationBinding,
  status: Schema.Literal("materializing"),
  preparedAt: IsoDateTime,
  ...MaterializingBinding,
});
export type AgentControlControlledThreadReservationMaterializingPayload =
  typeof AgentControlControlledThreadReservationMaterializingPayload.Type;

export const AgentControlControlledThreadReservationBoundPayload = Schema.Struct({
  ...ReservationBinding,
  status: Schema.Literal("bound"),
  preparedAt: IsoDateTime,
  ...BoundBinding,
});
export type AgentControlControlledThreadReservationBoundPayload =
  typeof AgentControlControlledThreadReservationBoundPayload.Type;

const EventEnvelope = {
  eventId: EventId,
  aggregateKind: Schema.Literal("controlled-thread-reservation"),
  aggregateId: AgentControlControlledThreadReservationId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

const PreparedEventDraft = Schema.Struct({
  ...EventEnvelope,
  type: Schema.Literal("agentControl.controlledThreadReservation.prepared"),
  payload: AgentControlControlledThreadReservationPreparedPayload,
});
const MaterializingEventDraft = Schema.Struct({
  ...EventEnvelope,
  type: Schema.Literal("agentControl.controlledThreadReservation.materializing"),
  payload: AgentControlControlledThreadReservationMaterializingPayload,
});
const BoundEventDraft = Schema.Struct({
  ...EventEnvelope,
  type: Schema.Literal("agentControl.controlledThreadReservation.bound"),
  payload: AgentControlControlledThreadReservationBoundPayload,
});
export const AgentControlControlledThreadReservationEventDraft = Schema.Union([
  PreparedEventDraft,
  MaterializingEventDraft,
  BoundEventDraft,
]);
export type AgentControlControlledThreadReservationEventDraft =
  typeof AgentControlControlledThreadReservationEventDraft.Type;

const PreparedEvent = Schema.Struct({
  ...PreparedEventDraft.fields,
  streamVersion: Schema.Literal(1),
  sequence: PositiveInt,
});
const MaterializingEvent = Schema.Struct({
  ...MaterializingEventDraft.fields,
  streamVersion: Schema.Literal(2),
  sequence: PositiveInt,
});
const BoundEvent = Schema.Struct({
  ...BoundEventDraft.fields,
  streamVersion: Schema.Literal(3),
  sequence: PositiveInt,
});
export const AgentControlControlledThreadReservationEvent = Schema.Union([
  PreparedEvent,
  MaterializingEvent,
  BoundEvent,
]);
export type AgentControlControlledThreadReservationEvent =
  typeof AgentControlControlledThreadReservationEvent.Type;

export const AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_REJECTED_COMMAND_CODES = [
  "validation",
  "project-unavailable",
  "project-mode-inactive",
  "task-missing",
  "task-not-candidate",
  "task-ineligible",
  "task-stage-inactive",
  "task-projection-corrupt",
  "source-snapshot-unavailable",
  "source-snapshot-stale",
  "source-watermark-stale",
  "stage-run-missing",
  "stage-run-not-prepared",
  "stage-run-projection-corrupt",
  "stage-run-history-ambiguous",
  "lease-missing",
  "lease-not-reserved",
  "lease-expired",
  "lease-foreign-runtime",
  "lease-projection-corrupt",
  "fence-token-mismatch",
  "worktree-missing",
  "worktree-not-ready",
  "worktree-projection-corrupt",
  "worktree-history-ambiguous",
  "controlled-thread-reservation-missing",
  "controlled-thread-reservation-identity-conflict",
  "controlled-thread-reservation-corrupt",
  "revision-conflict",
  "state-not-available",
  "command-identity-mismatch",
  "command-previously-rejected",
  "internal-persistence-error",
] as const;
export const AgentControlControlledThreadReservationRejectedCommandCode = Schema.Literals(
  AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_REJECTED_COMMAND_CODES,
);
export type AgentControlControlledThreadReservationRejectedCommandCode =
  typeof AgentControlControlledThreadReservationRejectedCommandCode.Type;

/** Closed wire error: no paths, fingerprints, holder IDs, fences, or causes. */
export class AgentControlControlledThreadReservationRpcError extends Schema.TaggedErrorClass<AgentControlControlledThreadReservationRpcError>()(
  "AgentControlControlledThreadReservationRpcError",
  {
    code: AgentControlControlledThreadReservationRejectedCommandCode,
    operation: Schema.Literals(["get", "list", "prepare-initial", "dispatch"]),
    projectId: ProjectId,
    taskId: Schema.NullOr(AgentControlTaskId),
    controlledThreadReservationId: Schema.NullOr(AgentControlControlledThreadReservationId),
  },
) {}
