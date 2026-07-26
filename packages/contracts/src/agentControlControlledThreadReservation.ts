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

import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
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

export const AgentControlControlledThreadReservationState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  ...ReservationBinding,
  status: Schema.Literal("prepared"),
  revision: Schema.Literal(1),
  sequence: PositiveInt,
  preparedAt: IsoDateTime,
});
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
  status: Schema.Literal("prepared"),
  revision: Schema.Literal(1),
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

const CommandBase = {
  commandId: CommandId,
  authority: Schema.Literal("controller"),
  ...ReservationBinding,
  expectedRevision: Schema.Literal(0),
} as const;

/** Server-internal command assembled only from authoritative state. */
export const AgentControlControlledThreadReservationPrepareCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.controlledThreadReservation.prepare"),
});
export type AgentControlControlledThreadReservationPrepareCommand =
  typeof AgentControlControlledThreadReservationPrepareCommand.Type;

/** Closed future transition contract. This slice rejects every invocation. */
export const AgentControlControlledThreadReservationUnavailableCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.controlledThreadReservation.transition"),
  targetStatus: Schema.Literals(["materializing", "bound", "released", "invalidated"]),
});
export type AgentControlControlledThreadReservationUnavailableCommand =
  typeof AgentControlControlledThreadReservationUnavailableCommand.Type;

export const AgentControlControlledThreadReservationCommand = Schema.Union([
  AgentControlControlledThreadReservationPrepareCommand,
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

const EventBase = {
  eventId: EventId,
  type: Schema.Literal("agentControl.controlledThreadReservation.prepared"),
  aggregateKind: Schema.Literal("controlled-thread-reservation"),
  aggregateId: AgentControlControlledThreadReservationId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  payload: AgentControlControlledThreadReservationPreparedPayload,
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

export const AgentControlControlledThreadReservationEventDraft = Schema.Struct(EventBase);
export type AgentControlControlledThreadReservationEventDraft =
  typeof AgentControlControlledThreadReservationEventDraft.Type;

export const AgentControlControlledThreadReservationEvent = Schema.Struct({
  ...EventBase,
  streamVersion: Schema.Literal(1),
  sequence: PositiveInt,
});
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
