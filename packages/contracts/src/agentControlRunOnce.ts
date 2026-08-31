/** Schema-only server-internal contracts for durable Run-Once execution. */
import * as Schema from "effect/Schema";

import {
  AgentControlControlledThreadReservationId,
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
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const AgentControlRunOnceId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlRunOnceId"),
);
export type AgentControlRunOnceId = typeof AgentControlRunOnceId.Type;

export const AGENT_CONTROL_RUN_ONCE_STEPS = [
  "activation-admitted",
  "task-selected",
  "no-eligible-task",
  "stage-prepared",
  "lease-reserved",
  "worktree-ready",
  "thread-activated",
  "task-terminal-observed",
  "mode-reset",
  "mode-reset-superseded",
  "completed",
] as const;
export const AgentControlRunOnceStep = Schema.Literals(AGENT_CONTROL_RUN_ONCE_STEPS);
export type AgentControlRunOnceStep = typeof AgentControlRunOnceStep.Type;

export const AgentControlRunOnceStatus = Schema.Literals([
  "active",
  "completed",
  "no-eligible-task",
]);
export type AgentControlRunOnceStatus = typeof AgentControlRunOnceStatus.Type;

export const AgentControlRunOnceActivation = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  runId: AgentControlRunOnceId,
  projectId: ProjectId,
  activationEventId: EventId,
  activationEventSequence: PositiveInt,
  activationEventStreamVersion: PositiveInt,
  activationCommandId: CommandId,
  githubIntakeSequence: PositiveInt,
  githubEventId: EventId,
  githubEventSequence: PositiveInt,
  githubEventStreamVersion: PositiveInt,
  reconcileRevision: PositiveInt,
  sourceFingerprint: TrimmedNonEmptyString,
  activatedAt: IsoDateTime,
});
export type AgentControlRunOnceActivation = typeof AgentControlRunOnceActivation.Type;

export const AgentControlRunOnceState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  runId: AgentControlRunOnceId,
  projectId: ProjectId,
  status: AgentControlRunOnceStatus,
  nextOrdinal: PositiveInt,
  lastStep: AgentControlRunOnceStep,
  taskId: Schema.NullOr(AgentControlTaskId),
  stageRunId: Schema.NullOr(AgentControlStageRunId),
  leaseId: Schema.NullOr(AgentControlStageRunLeaseId),
  worktreeReservationId: Schema.NullOr(AgentControlWorktreeReservationId),
  controlledThreadReservationId: Schema.NullOr(AgentControlControlledThreadReservationId),
  terminalTaskEventId: Schema.NullOr(EventId),
  activationProjectRevision: PositiveInt,
  resetProjectRevision: Schema.NullOr(PositiveInt),
  updatedAt: IsoDateTime,
});
export type AgentControlRunOnceState = typeof AgentControlRunOnceState.Type;

export const AgentControlRunOncePublication = Schema.Struct({
  publicationId: TrimmedNonEmptyString,
  runId: AgentControlRunOnceId,
  ordinal: PositiveInt,
  step: AgentControlRunOnceStep,
  published: Schema.Boolean,
  attemptCount: NonNegativeInt,
});
export type AgentControlRunOncePublication = typeof AgentControlRunOncePublication.Type;
