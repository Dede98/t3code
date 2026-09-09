import {
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlControlledThreadReservationId,
  AgentControlWorktreeReservationId,
  AgentControlTaskId,
  CommandId,
  NonNegativeInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class AgentControlPersistenceSqlError extends Schema.TaggedError<AgentControlPersistenceSqlError>()(
  "AgentControlPersistenceSqlError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class AgentControlPersistenceDecodeError extends Schema.TaggedError<AgentControlPersistenceDecodeError>()(
  "AgentControlPersistenceDecodeError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class AgentControlStreamVersionConflictError extends Schema.TaggedError<AgentControlStreamVersionConflictError>()(
  "AgentControlStreamVersionConflictError",
  {
    projectId: ProjectId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlTaskStreamVersionConflictError extends Schema.TaggedError<AgentControlTaskStreamVersionConflictError>()(
  "AgentControlTaskStreamVersionConflictError",
  {
    taskId: AgentControlTaskId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlStageRunStreamVersionConflictError extends Schema.TaggedError<AgentControlStageRunStreamVersionConflictError>()(
  "AgentControlStageRunStreamVersionConflictError",
  {
    stageRunId: AgentControlStageRunId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlStageRunLeaseStreamVersionConflictError extends Schema.TaggedError<AgentControlStageRunLeaseStreamVersionConflictError>()(
  "AgentControlStageRunLeaseStreamVersionConflictError",
  {
    leaseId: AgentControlStageRunLeaseId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlWorktreeStreamVersionConflictError extends Schema.TaggedError<AgentControlWorktreeStreamVersionConflictError>()(
  "AgentControlWorktreeStreamVersionConflictError",
  {
    reservationId: AgentControlWorktreeReservationId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlControlledThreadReservationStreamVersionConflictError extends Schema.TaggedError<AgentControlControlledThreadReservationStreamVersionConflictError>()(
  "AgentControlControlledThreadReservationStreamVersionConflictError",
  {
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlGithubSchedulerConflictError extends Schema.TaggedError<AgentControlGithubSchedulerConflictError>()(
  "AgentControlGithubSchedulerConflictError",
  {
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: NonNegativeInt,
  },
) {}

export class AgentControlTaskReconcileConflictError extends Schema.TaggedError<AgentControlTaskReconcileConflictError>()(
  "AgentControlTaskReconcileConflictError",
  {
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: NonNegativeInt,
  },
) {}

export class AgentControlProjectUnavailableError extends Schema.TaggedError<AgentControlProjectUnavailableError>()(
  "AgentControlProjectUnavailableError",
  {
    projectId: ProjectId,
    reason: Schema.Literals(["missing", "deleted"]),
  },
) {}

export class AgentControlReceiptConflictError extends Schema.TaggedError<AgentControlReceiptConflictError>()(
  "AgentControlReceiptConflictError",
  {
    commandId: CommandId,
  },
) {}

export type AgentControlEventStoreError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError
  | AgentControlStreamVersionConflictError;

export type AgentControlTaskEventStoreError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError
  | AgentControlTaskStreamVersionConflictError;

export type AgentControlStageRunEventStoreError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError
  | AgentControlStageRunStreamVersionConflictError;

export type AgentControlStageRunLeaseEventStoreError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError
  | AgentControlStageRunLeaseStreamVersionConflictError;

export type AgentControlControlledThreadReservationEventStoreError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError
  | AgentControlControlledThreadReservationStreamVersionConflictError;

export type AgentControlRepositoryError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError;
