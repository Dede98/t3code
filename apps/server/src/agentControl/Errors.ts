import {
  AgentControlStageRunId,
  AgentControlTaskId,
  CommandId,
  NonNegativeInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export class AgentControlPersistenceSqlError extends Schema.TaggedErrorClass<AgentControlPersistenceSqlError>()(
  "AgentControlPersistenceSqlError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class AgentControlPersistenceDecodeError extends Schema.TaggedErrorClass<AgentControlPersistenceDecodeError>()(
  "AgentControlPersistenceDecodeError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export class AgentControlStreamVersionConflictError extends Schema.TaggedErrorClass<AgentControlStreamVersionConflictError>()(
  "AgentControlStreamVersionConflictError",
  {
    projectId: ProjectId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlTaskStreamVersionConflictError extends Schema.TaggedErrorClass<AgentControlTaskStreamVersionConflictError>()(
  "AgentControlTaskStreamVersionConflictError",
  {
    taskId: AgentControlTaskId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlStageRunStreamVersionConflictError extends Schema.TaggedErrorClass<AgentControlStageRunStreamVersionConflictError>()(
  "AgentControlStageRunStreamVersionConflictError",
  {
    stageRunId: AgentControlStageRunId,
    expectedVersion: NonNegativeInt,
    actualVersion: NonNegativeInt,
  },
) {}

export class AgentControlGithubSchedulerConflictError extends Schema.TaggedErrorClass<AgentControlGithubSchedulerConflictError>()(
  "AgentControlGithubSchedulerConflictError",
  {
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: NonNegativeInt,
  },
) {}

export class AgentControlTaskReconcileConflictError extends Schema.TaggedErrorClass<AgentControlTaskReconcileConflictError>()(
  "AgentControlTaskReconcileConflictError",
  {
    projectId: ProjectId,
    expectedRevision: NonNegativeInt,
    actualRevision: NonNegativeInt,
  },
) {}

export class AgentControlProjectUnavailableError extends Schema.TaggedErrorClass<AgentControlProjectUnavailableError>()(
  "AgentControlProjectUnavailableError",
  {
    projectId: ProjectId,
    reason: Schema.Literals(["missing", "deleted"]),
  },
) {}

export class AgentControlReceiptConflictError extends Schema.TaggedErrorClass<AgentControlReceiptConflictError>()(
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

export type AgentControlRepositoryError =
  | AgentControlPersistenceSqlError
  | AgentControlPersistenceDecodeError;
