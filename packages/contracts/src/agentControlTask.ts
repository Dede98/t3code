/**
 * Schema-only contracts for durable Agent Control task intake.
 *
 * GitHub title/body data is always untrusted external data. It is retained as
 * source evidence and is never interpreted as controller authority or a
 * transition instruction.
 *
 * @module agentControlTask
 */
import * as Schema from "effect/Schema";

import {
  AgentControlTaskId,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const AGENT_CONTROL_TASK_RPC_METHODS = {
  getTask: "agentControlTask.getTask",
  listTasks: "agentControlTask.listTasks",
  reconcileOnce: "agentControlTask.reconcileOnce",
  getReactorStatus: "agentControlTask.getReactorStatus",
} as const;

export const AGENT_CONTROL_TASK_STATUSES = [
  "candidate",
  "needs-attention",
  "cancelled",
  "queued",
  "running",
  "waiting",
  "succeeded",
  "failed",
] as const;
export const AgentControlTaskStatus = Schema.Literals(AGENT_CONTROL_TASK_STATUSES);
export type AgentControlTaskStatus = typeof AgentControlTaskStatus.Type;

export const AGENT_CONTROL_TASK_EXECUTION_STATUSES = [
  "queued",
  "running",
  "waiting",
  "succeeded",
  "failed",
] as const;
export const AgentControlTaskExecutionStatus = Schema.Literals(
  AGENT_CONTROL_TASK_EXECUTION_STATUSES,
);
export type AgentControlTaskExecutionStatus = typeof AgentControlTaskExecutionStatus.Type;

export const AgentControlTaskSourceGate = Schema.Literals([
  "eligible",
  "not-ready",
  "paused",
  "closed",
  "timeline-invalid",
  "identity-invalid",
  "source-missing",
]);
export type AgentControlTaskSourceGate = typeof AgentControlTaskSourceGate.Type;

export const AgentControlTaskPipelineStage = Schema.Literals(["intake", "verification"]);
export type AgentControlTaskPipelineStage = typeof AgentControlTaskPipelineStage.Type;

export const AgentControlTaskSourceIdentity = Schema.Struct({
  projectId: ProjectId,
  repositoryNodeId: TrimmedNonEmptyString,
  issueNodeId: TrimmedNonEmptyString,
  issueNumber: PositiveInt,
  issueUrl: TrimmedNonEmptyString,
});
export type AgentControlTaskSourceIdentity = typeof AgentControlTaskSourceIdentity.Type;

export const AgentControlTaskSourceSnapshot = Schema.Struct({
  repositoryNodeId: TrimmedNonEmptyString,
  issueNodeId: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "closed"]),
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  contentTrust: Schema.Literal("untrusted-external"),
  updatedAt: IsoDateTime,
  timelineComplete: Schema.Boolean,
  ready: Schema.Boolean,
  paused: Schema.Boolean,
  eligible: Schema.Boolean,
  eligibilityReason: Schema.Literals([
    "eligible",
    "closed",
    "ready-inactive",
    "paused",
    "timeline-invalid",
  ]),
});
export type AgentControlTaskSourceSnapshot = typeof AgentControlTaskSourceSnapshot.Type;

/**
 * Stable identity of one complete GitHub projection snapshot. Issue rows are
 * deliberately not copied into commands; the engine revalidates this token
 * against the GitHub state and projected issue count in its SQLite transaction.
 */
export const AgentControlTaskSourcePrecondition = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  githubIntakeSequence: PositiveInt,
  githubProjectionRevision: PositiveInt,
  githubConfigRevision: PositiveInt,
  repositoryNodeId: TrimmedNonEmptyString,
  pollStatus: Schema.Literal("success"),
  expectedIssueCount: NonNegativeInt,
});
export type AgentControlTaskSourcePrecondition = typeof AgentControlTaskSourcePrecondition.Type;

export const AgentControlTaskState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  status: AgentControlTaskStatus,
  sourceGate: AgentControlTaskSourceGate,
  stage: AgentControlTaskPipelineStage,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  revision: PositiveInt,
  sequence: PositiveInt,
});
export type AgentControlTaskState = typeof AgentControlTaskState.Type;

/** List-safe projection: issue bodies are deliberately omitted. */
export const AgentControlTaskSummary = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  status: AgentControlTaskStatus,
  sourceGate: AgentControlTaskSourceGate,
  stage: AgentControlTaskPipelineStage,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  title: Schema.String,
  contentTrust: Schema.Literal("untrusted-external"),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  revision: PositiveInt,
  sequence: PositiveInt,
});
export type AgentControlTaskSummary = typeof AgentControlTaskSummary.Type;

export const AgentControlTaskGetInput = Schema.Struct({
  projectId: ProjectId,
  taskId: AgentControlTaskId,
});
export type AgentControlTaskGetInput = typeof AgentControlTaskGetInput.Type;

export const AgentControlTaskListInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlTaskListInput = typeof AgentControlTaskListInput.Type;

export const AgentControlTaskListResult = Schema.Struct({
  projectId: ProjectId,
  tasks: Schema.Array(AgentControlTaskSummary),
  quarantinedCount: NonNegativeInt,
});
export type AgentControlTaskListResult = typeof AgentControlTaskListResult.Type;

export const AgentControlTaskReconcileOnceInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlTaskReconcileOnceInput = typeof AgentControlTaskReconcileOnceInput.Type;

export const AgentControlTaskReconcileOnceResult = Schema.Struct({
  projectId: ProjectId,
  githubIntakeSequence: PositiveInt,
  observedCount: NonNegativeInt,
  createdCount: NonNegativeInt,
  updatedCount: NonNegativeInt,
  needsAttentionCount: NonNegativeInt,
  unchangedCount: NonNegativeInt,
});
export type AgentControlTaskReconcileOnceResult = typeof AgentControlTaskReconcileOnceResult.Type;

export const AgentControlTaskReactorActivity = Schema.Literals([
  "inactive",
  "waiting-source",
  "reconciling",
  "recovering",
  "suspended",
]);
export type AgentControlTaskReactorActivity = typeof AgentControlTaskReactorActivity.Type;

export const AgentControlTaskReactorHealth = Schema.Literals(["healthy", "recovering", "degraded"]);
export type AgentControlTaskReactorHealth = typeof AgentControlTaskReactorHealth.Type;

export const AgentControlTaskReactorWorkerState = Schema.Literals([
  "stopped",
  "queued",
  "running",
  "backoff",
]);
export type AgentControlTaskReactorWorkerState = typeof AgentControlTaskReactorWorkerState.Type;

export const AGENT_CONTROL_TASK_REACTOR_ERROR_CODES = [
  "project-unavailable",
  "mode-inactive",
  "source-snapshot-unavailable",
  "source-snapshot-stale",
  "project-mode-inactive",
  "revision-conflict",
  "task-projection-corrupt",
  "source-identity-conflict",
  "internal-persistence-error",
  "subscription-unavailable",
  "enumeration-failed",
] as const;
export const AgentControlTaskReactorErrorCode = Schema.Literals(
  AGENT_CONTROL_TASK_REACTOR_ERROR_CODES,
);
export type AgentControlTaskReactorErrorCode = typeof AgentControlTaskReactorErrorCode.Type;

/** Transport-safe reactor status. It intentionally excludes source and process data. */
export const AgentControlTaskReactorStatus = Schema.Struct({
  projectId: ProjectId,
  activity: AgentControlTaskReactorActivity,
  health: AgentControlTaskReactorHealth,
  workerState: AgentControlTaskReactorWorkerState,
  subscriptionHealth: AgentControlTaskReactorHealth,
  globalHealth: AgentControlTaskReactorHealth,
  currentSourceSequence: Schema.NullOr(PositiveInt),
  targetSequence: Schema.NullOr(PositiveInt),
  lastCompletedSequence: Schema.NullOr(NonNegativeInt),
  sequenceCurrent: Schema.Boolean,
  retryAttempt: NonNegativeInt,
  nextAttemptAt: Schema.NullOr(IsoDateTime),
  lastErrorCode: Schema.NullOr(AgentControlTaskReactorErrorCode),
});
export type AgentControlTaskReactorStatus = typeof AgentControlTaskReactorStatus.Type;

const CommandBase = {
  commandId: CommandId,
  taskId: AgentControlTaskId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
} as const;
const SourceCommandBase = {
  ...CommandBase,
  sourcePrecondition: AgentControlTaskSourcePrecondition,
} as const;

export const AgentControlTaskCreateFromGithubIssueCommand = Schema.Struct({
  ...SourceCommandBase,
  type: Schema.Literal("agentControl.task.createFromGithubIssue"),
  source: AgentControlTaskSourceIdentity,
  sourceGate: AgentControlTaskSourceGate,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
});
export type AgentControlTaskCreateFromGithubIssueCommand =
  typeof AgentControlTaskCreateFromGithubIssueCommand.Type;

export const AgentControlTaskSourceGateRefreshCommand = Schema.Struct({
  ...SourceCommandBase,
  type: Schema.Literal("agentControl.task.sourceGate.refresh"),
  source: AgentControlTaskSourceIdentity,
  sourceGate: AgentControlTaskSourceGate,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
});
export type AgentControlTaskSourceGateRefreshCommand =
  typeof AgentControlTaskSourceGateRefreshCommand.Type;

export const AgentControlTaskMarkNeedsAttentionCommand = Schema.Struct({
  ...SourceCommandBase,
  type: Schema.Literal("agentControl.task.markNeedsAttention"),
  source: AgentControlTaskSourceIdentity,
  sourceGate: Schema.Literals(["identity-invalid", "source-missing"]),
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
});
export type AgentControlTaskMarkNeedsAttentionCommand =
  typeof AgentControlTaskMarkNeedsAttentionCommand.Type;

export const AgentControlTaskRecoverSourceMissingCommand = Schema.Struct({
  ...SourceCommandBase,
  type: Schema.Literal("agentControl.task.recoverSourceMissing"),
  source: AgentControlTaskSourceIdentity,
  sourceGate: Schema.Literal("eligible"),
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
});
export type AgentControlTaskRecoverSourceMissingCommand =
  typeof AgentControlTaskRecoverSourceMissingCommand.Type;

/** Reserved execution transition contract; this slice rejects every use. */
export const AgentControlTaskSetStatusCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.task.status.set"),
  status: AgentControlTaskExecutionStatus,
});
export type AgentControlTaskSetStatusCommand = typeof AgentControlTaskSetStatusCommand.Type;

export const AgentControlTaskCommand = Schema.Union([
  AgentControlTaskCreateFromGithubIssueCommand,
  AgentControlTaskSourceGateRefreshCommand,
  AgentControlTaskMarkNeedsAttentionCommand,
  AgentControlTaskRecoverSourceMissingCommand,
  AgentControlTaskSetStatusCommand,
]);
export type AgentControlTaskCommand = typeof AgentControlTaskCommand.Type;

export const AgentControlTaskCommandResult = Schema.Struct({
  state: AgentControlTaskState,
  resultSequence: PositiveInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlTaskCommandResult = typeof AgentControlTaskCommandResult.Type;

const EventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("task"),
  aggregateId: AgentControlTaskId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

const SystemEventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("task"),
  aggregateId: AgentControlTaskId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: EventId,
  correlationId: CommandId,
  authority: Schema.Literal("system"),
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

export const AgentControlTaskCreatedPayload = Schema.Struct({
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  status: Schema.Literal("candidate"),
  sourceGate: AgentControlTaskSourceGate,
  stage: Schema.Literal("intake"),
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
  createdAt: IsoDateTime,
});
export type AgentControlTaskCreatedPayload = typeof AgentControlTaskCreatedPayload.Type;

export const AgentControlTaskSourceGateChangedPayload = Schema.Struct({
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  previousSourceGate: AgentControlTaskSourceGate,
  sourceGate: AgentControlTaskSourceGate,
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
  changedAt: IsoDateTime,
});
export type AgentControlTaskSourceGateChangedPayload =
  typeof AgentControlTaskSourceGateChangedPayload.Type;

export const AgentControlTaskNeedsAttentionMarkedPayload = Schema.Struct({
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  previousStatus: AgentControlTaskStatus,
  previousSourceGate: AgentControlTaskSourceGate,
  sourceGate: Schema.Literals(["identity-invalid", "source-missing"]),
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
  markedAt: IsoDateTime,
});
export type AgentControlTaskNeedsAttentionMarkedPayload =
  typeof AgentControlTaskNeedsAttentionMarkedPayload.Type;

export const AgentControlTaskSourceMissingRecoveredPayload = Schema.Struct({
  taskId: AgentControlTaskId,
  source: AgentControlTaskSourceIdentity,
  previousStatus: Schema.Literal("needs-attention"),
  previousSourceGate: Schema.Literal("source-missing"),
  status: Schema.Literal("candidate"),
  sourceGate: Schema.Literal("eligible"),
  sourceUpdatedAt: IsoDateTime,
  githubIntakeSequence: PositiveInt,
  sourceSnapshot: AgentControlTaskSourceSnapshot,
  recoveredAt: IsoDateTime,
});
export type AgentControlTaskSourceMissingRecoveredPayload =
  typeof AgentControlTaskSourceMissingRecoveredPayload.Type;

const VerificationEvaluationIdentity = {
  evaluationAuthority: Schema.Literal("accepted-evaluation"),
  evaluationId: TrimmedNonEmptyString,
  evaluationEvidenceId: TrimmedNonEmptyString,
  evaluationReceiptId: TrimmedNonEmptyString,
  evaluationMarkerId: TrimmedNonEmptyString,
} as const;
const VerificationNoEvaluation = Schema.Struct({
  evaluationAuthority: Schema.Literal("not-applicable"),
  evaluationId: Schema.Null,
  evaluationEvidenceId: Schema.Null,
  evaluationReceiptId: Schema.Null,
  evaluationMarkerId: Schema.Null,
  evaluationDisposition: Schema.Null,
  verificationVerdict: Schema.Null,
  invalidOutputCode: Schema.Null,
});
const VerificationPassedEvaluation = Schema.Struct({
  ...VerificationEvaluationIdentity,
  evaluationDisposition: Schema.Literal("evaluated"),
  verificationVerdict: Schema.Literal("passed"),
  invalidOutputCode: Schema.Null,
});
const VerificationFailedEvaluation = Schema.Struct({
  ...VerificationEvaluationIdentity,
  evaluationDisposition: Schema.Literal("evaluated"),
  verificationVerdict: Schema.Literal("failed"),
  invalidOutputCode: Schema.Null,
});
const VerificationInvalidOutputEvaluation = Schema.Struct({
  ...VerificationEvaluationIdentity,
  evaluationDisposition: Schema.Literal("invalid-output"),
  verificationVerdict: Schema.Null,
  invalidOutputCode: Schema.Literals([
    "missing-final-message",
    "output-too-large",
    "invalid-utf8",
    "malformed-json",
    "unsupported-schema-version",
    "schema-violation",
  ]),
});

const VerificationTaskFinalizationSource = {
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  verificationTaskRevision: PositiveInt,
  previousTaskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  taskSourceEventId: EventId,
  taskSourceEventSequence: PositiveInt,
  taskSourceEventStreamVersion: PositiveInt,
  handoffId: TrimmedNonEmptyString,
  handoffFingerprint: TrimmedNonEmptyString,
  verificationFinalizationEvidenceId: TrimmedNonEmptyString,
  verificationFinalizationReceiptId: TrimmedNonEmptyString,
  verificationFinalizationMarkerId: TrimmedNonEmptyString,
  verificationFinalizationCommandId: CommandId,
  verificationFinalizationFingerprint: TrimmedNonEmptyString,
  verificationFinalizationMarkerFingerprint: TrimmedNonEmptyString,
  terminalStageRunId: TrimmedNonEmptyString,
  terminalStageEventId: EventId,
  terminalStageEventSequence: PositiveInt,
  terminalStageEventStreamVersion: PositiveInt,
  releasedLeaseId: TrimmedNonEmptyString,
  releasedLeaseEventId: EventId,
  releasedLeaseEventSequence: PositiveInt,
  releasedLeaseEventStreamVersion: PositiveInt,
  terminalRuntimeEventId: EventId,
  taskFinalizationEvidenceId: TrimmedNonEmptyString,
  finalizedAt: IsoDateTime,
} as const;
const VerificationTaskPreviousStatus = Schema.Literals([
  "candidate",
  "needs-attention",
  "queued",
  "running",
  "waiting",
]);

export const AgentControlTaskFinalizedAfterVerificationPayload = Schema.Union([
  Schema.Struct({
    ...VerificationTaskFinalizationSource,
    deliveryTerminalState: Schema.Literal("completed"),
    verificationOutcome: Schema.Literal("succeeded"),
    terminalCause: Schema.Literal("verification-passed"),
    previousStatus: VerificationTaskPreviousStatus,
    status: Schema.Literal("succeeded"),
    stage: Schema.Literal("verification"),
    evaluation: VerificationPassedEvaluation,
  }),
  Schema.Struct({
    ...VerificationTaskFinalizationSource,
    deliveryTerminalState: Schema.Literal("completed"),
    verificationOutcome: Schema.Literal("failed"),
    terminalCause: Schema.Literal("verification-failed"),
    previousStatus: VerificationTaskPreviousStatus,
    status: Schema.Literal("failed"),
    stage: Schema.Literal("verification"),
    evaluation: VerificationFailedEvaluation,
  }),
  Schema.Struct({
    ...VerificationTaskFinalizationSource,
    deliveryTerminalState: Schema.Literal("completed"),
    verificationOutcome: Schema.Literal("failed"),
    terminalCause: Schema.Literal("verification-invalid-output"),
    previousStatus: VerificationTaskPreviousStatus,
    status: Schema.Literal("failed"),
    stage: Schema.Literal("verification"),
    evaluation: VerificationInvalidOutputEvaluation,
  }),
  Schema.Struct({
    ...VerificationTaskFinalizationSource,
    deliveryTerminalState: Schema.Literal("failed"),
    verificationOutcome: Schema.Literal("failed"),
    terminalCause: Schema.Literal("provider-delivery-failed"),
    previousStatus: VerificationTaskPreviousStatus,
    status: Schema.Literal("failed"),
    stage: Schema.Literal("verification"),
    evaluation: VerificationNoEvaluation,
  }),
  Schema.Struct({
    ...VerificationTaskFinalizationSource,
    deliveryTerminalState: Schema.Literal("interrupted"),
    verificationOutcome: Schema.Literal("cancelled"),
    terminalCause: Schema.Literal("provider-delivery-interrupted"),
    previousStatus: VerificationTaskPreviousStatus,
    status: Schema.Literal("cancelled"),
    stage: Schema.Literal("verification"),
    evaluation: VerificationNoEvaluation,
  }),
]).annotate({ parseOptions: { onExcessProperty: "error" } });
export type AgentControlTaskFinalizedAfterVerificationPayload =
  typeof AgentControlTaskFinalizedAfterVerificationPayload.Type;

const createdFields = {
  ...EventBase,
  type: Schema.Literal("agentControl.task.created"),
  payload: AgentControlTaskCreatedPayload,
} as const;
const sourceGateChangedFields = {
  ...EventBase,
  type: Schema.Literal("agentControl.task.sourceGate.changed"),
  payload: AgentControlTaskSourceGateChangedPayload,
} as const;
const needsAttentionFields = {
  ...EventBase,
  type: Schema.Literal("agentControl.task.needsAttentionMarked"),
  payload: AgentControlTaskNeedsAttentionMarkedPayload,
} as const;
const sourceMissingRecoveredFields = {
  ...EventBase,
  type: Schema.Literal("agentControl.task.sourceMissingRecovered"),
  payload: AgentControlTaskSourceMissingRecoveredPayload,
} as const;
const finalizedAfterVerificationFields = {
  ...SystemEventBase,
  type: Schema.Literal("agentControl.task.finalizedAfterVerification"),
  payload: AgentControlTaskFinalizedAfterVerificationPayload,
} as const;

export const AgentControlTaskEventDraft = Schema.Union([
  Schema.Struct(createdFields),
  Schema.Struct(sourceGateChangedFields),
  Schema.Struct(needsAttentionFields),
  Schema.Struct(sourceMissingRecoveredFields),
  Schema.Struct(finalizedAfterVerificationFields),
]);
export type AgentControlTaskEventDraft = typeof AgentControlTaskEventDraft.Type;

export const AgentControlTaskEvent = Schema.Union([
  Schema.Struct({ ...createdFields, streamVersion: PositiveInt, sequence: PositiveInt }),
  Schema.Struct({
    ...sourceGateChangedFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...needsAttentionFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...sourceMissingRecoveredFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...finalizedAfterVerificationFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
]);
export type AgentControlTaskEvent = typeof AgentControlTaskEvent.Type;

export const AGENT_CONTROL_TASK_REJECTED_COMMAND_CODES = [
  "validation",
  "project-missing",
  "project-deleted",
  "task-missing",
  "revision-conflict",
  "source-identity-conflict",
  "source-state-conflict",
  "source-snapshot-stale",
  "project-mode-inactive",
  "task-projection-corrupt",
  "state-not-available",
  "command-identity-mismatch",
  "command-previously-rejected",
  "source-snapshot-unavailable",
  "internal-persistence-error",
] as const;
export const AgentControlTaskRejectedCommandCode = Schema.Literals(
  AGENT_CONTROL_TASK_REJECTED_COMMAND_CODES,
);
export type AgentControlTaskRejectedCommandCode = typeof AgentControlTaskRejectedCommandCode.Type;

/** Closed wire error: it cannot carry source content, commands, paths, or exceptions. */
export class AgentControlTaskRpcError extends Schema.TaggedErrorClass<AgentControlTaskRpcError>()(
  "AgentControlTaskRpcError",
  {
    code: AgentControlTaskRejectedCommandCode,
    operation: Schema.Literals([
      "get-task",
      "list-tasks",
      "reconcile-once",
      "get-reactor-status",
      "dispatch",
    ]),
    projectId: ProjectId,
    taskId: Schema.NullOr(AgentControlTaskId),
  },
) {}

export type AgentControlTaskCommandError = AgentControlTaskRpcError;
