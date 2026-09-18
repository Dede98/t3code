import { AgentControlEpicQueue } from "./agentControlEpicQueue.ts";
import { AgentControlRunOnceCheckView } from "./agentControlVerificationView.ts";
export { AgentControlRunOnceCheckView } from "./agentControlVerificationView.ts";
import { AgentControlEpicRuntimeView } from "./agentControlEpicRuntime.ts";
/** Durable Run-Once execution and project-scoped client read models. */
import * as Schema from "effect/Schema";
import { AgentControlStageRunState } from "./agentControlStageRun.ts";
import { AgentControlTaskSummary } from "./agentControlTask.ts";
import { AgentControlProjectState } from "./agentControlRuntime.ts";
import { ResourceAdmissionWait } from "./resourceAdmission.ts";

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
  ThreadId,
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

const AgentControlRunOnceActivationFields = {
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
} as const;

export const AgentControlRunOnceActivation = Schema.Union([
  Schema.Struct({
    ...AgentControlRunOnceActivationFields,
    originMode: Schema.Literal("observe"),
    armedDispatchId: Schema.Null,
    armedClaimId: Schema.Null,
    armedMarkerId: Schema.Null,
  }),
  Schema.Struct({
    ...AgentControlRunOnceActivationFields,
    originMode: Schema.Literal("armed"),
    armedDispatchId: TrimmedNonEmptyString,
    armedClaimId: TrimmedNonEmptyString,
    armedMarkerId: TrimmedNonEmptyString,
  }),
]);
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

export const AGENT_CONTROL_RUN_ONCE_RPC_METHODS = {
  getSnapshot: "agentControlRunOnce.getSnapshot",
  subscribe: "agentControlRunOnce.subscribe",
} as const;

export const AgentControlRunOnceSnapshotInput = Schema.Struct({
  projectId: ProjectId,
  runId: Schema.optionalKey(AgentControlRunOnceId),
});
export type AgentControlRunOnceSnapshotInput = typeof AgentControlRunOnceSnapshotInput.Type;

export const AgentControlRunOnceVerificationView = Schema.Struct({
  providerDeliveryId: Schema.String,
  verdict: Schema.NullOr(Schema.Literals(["passed", "failed"])),
  errorCode: Schema.NullOr(Schema.String),
  evaluatedAt: Schema.NullOr(IsoDateTime),
  checks: Schema.Array(AgentControlRunOnceCheckView),
});
export const AgentControlRunOnceStageView = Schema.Struct({
  ...AgentControlStageRunState.fields,
  providerInstanceId: Schema.optionalKey(Schema.NullOr(Schema.String)),
  model: Schema.optionalKey(Schema.NullOr(Schema.String)),
  displayStage: Schema.Literals(["planning", "implementation", "verification", "repair"]),
  threadId: Schema.NullOr(ThreadId),
  worktreePath: Schema.NullOr(Schema.String),
  branch: Schema.NullOr(Schema.String),
  errorCode: Schema.NullOr(Schema.String),
  /** Present while the stage has not started and is queued by host admission. */
  admissionWait: Schema.optionalKey(ResourceAdmissionWait),
  verification: Schema.NullOr(AgentControlRunOnceVerificationView),
});
export type AgentControlRunOnceStageView = typeof AgentControlRunOnceStageView.Type;
export const AgentControlRunOnceView = Schema.Struct({
  originMode: Schema.optionalKey(Schema.NullOr(Schema.Literals(["observe", "armed"]))),
  errorCode: Schema.NullOr(Schema.String),
  state: AgentControlRunOnceState,
  task: Schema.NullOr(AgentControlTaskSummary),
  stages: Schema.Array(AgentControlRunOnceStageView),
});
export type AgentControlRunOnceView = typeof AgentControlRunOnceView.Type;
export const AgentControlRunOnceSnapshot = Schema.Struct({
  /** Authoritative execution targets; `epic` remains a compatibility view for older servers. */
  epics: Schema.optionalKey(Schema.Array(AgentControlEpicRuntimeView)),
  epic: Schema.optionalKey(Schema.NullOr(AgentControlEpicRuntimeView)),
  epicQueue: Schema.optionalKey(Schema.NullOr(AgentControlEpicQueue)),
  epicHistory: Schema.optionalKey(Schema.Array(AgentControlEpicRuntimeView)),
  /** Absent on older servers; clients must not interpret unknown authority as off. */
  armed: Schema.optionalKey(Schema.Struct({ enabled: Schema.Boolean })),
  blockers: Schema.Array(Schema.String),
  projectId: ProjectId,
  projectState: AgentControlProjectState,
  tasks: Schema.Array(AgentControlTaskSummary),
  nextTaskId: Schema.NullOr(AgentControlTaskId),
  runs: Schema.Array(AgentControlRunOnceView),
});
export type AgentControlRunOnceSnapshot = typeof AgentControlRunOnceSnapshot.Type;
