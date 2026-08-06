/**
 * Schema-only contracts for durable Agent Control stage-run preparation.
 *
 * The client selects neither stage identity nor execution authority. Preparing
 * a stage run records intent only; it does not queue, claim, or execute a task.
 *
 * @module agentControlStageRun
 */
import * as Schema from "effect/Schema";

import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const AGENT_CONTROL_STAGE_RUN_RPC_METHODS = {
  getStageRun: "agentControlStageRun.getStageRun",
  listStageRuns: "agentControlStageRun.listStageRuns",
  prepareInitial: "agentControlStageRun.prepareInitial",
} as const;

export const AGENT_CONTROL_STAGE_KINDS = [
  "classification",
  "design-pre-review",
  "planning",
  "implementation",
  "verification",
  "general-review",
  "gpt-review",
  "repair",
  "pr",
  "attestation",
  "merge",
] as const;
export const AgentControlStageKind = Schema.Literals(AGENT_CONTROL_STAGE_KINDS);
export type AgentControlStageKind = typeof AgentControlStageKind.Type;

export const AGENT_CONTROL_STAGE_RUN_STATUSES = [
  "prepared",
  "queued",
  "running",
  "waiting",
  "succeeded",
  "failed",
  "cancelled",
] as const;
export const AgentControlStageRunStatus = Schema.Literals(AGENT_CONTROL_STAGE_RUN_STATUSES);
export type AgentControlStageRunStatus = typeof AgentControlStageRunStatus.Type;

export const AgentControlStageRunState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: AgentControlStageKind,
  stageOrdinal: PositiveInt,
  attemptOrdinal: PositiveInt,
  status: AgentControlStageRunStatus,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  revision: PositiveInt,
  sequence: PositiveInt,
});
export type AgentControlStageRunState = typeof AgentControlStageRunState.Type;

/** List-safe by construction: no source content or local runtime data is stored. */
export const AgentControlStageRunSummary = AgentControlStageRunState;
export type AgentControlStageRunSummary = typeof AgentControlStageRunSummary.Type;

export const AgentControlStageRunGetInput = Schema.Struct({
  projectId: ProjectId,
  taskId: AgentControlTaskId,
});
export type AgentControlStageRunGetInput = typeof AgentControlStageRunGetInput.Type;

export const AgentControlStageRunListInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlStageRunListInput = typeof AgentControlStageRunListInput.Type;

export const AgentControlStageRunListResult = Schema.Struct({
  projectId: ProjectId,
  stageRuns: Schema.Array(AgentControlStageRunSummary),
  quarantinedCount: NonNegativeInt,
});
export type AgentControlStageRunListResult = typeof AgentControlStageRunListResult.Type;

export const AgentControlStageRunPrepareInitialInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
});
export type AgentControlStageRunPrepareInitialInput =
  typeof AgentControlStageRunPrepareInitialInput.Type;

const CommandBase = {
  commandId: CommandId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: AgentControlStageKind,
  stageOrdinal: PositiveInt,
  attemptOrdinal: PositiveInt,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  expectedRevision: NonNegativeInt,
} as const;

/** Server-internal command assembled only from canonical task/source state. */
export const AgentControlStageRunPrepareCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRun.prepare"),
});
export type AgentControlStageRunPrepareCommand = typeof AgentControlStageRunPrepareCommand.Type;

/** Reserved transition contract; this foundation rejects every use. */
export const AgentControlStageRunSetStatusCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRun.status.set"),
  status: Schema.Literals(["queued", "running", "waiting", "succeeded", "failed", "cancelled"]),
});
export type AgentControlStageRunSetStatusCommand = typeof AgentControlStageRunSetStatusCommand.Type;

export const AgentControlStageRunCommand = Schema.Union([
  AgentControlStageRunPrepareCommand,
  AgentControlStageRunSetStatusCommand,
]);
export type AgentControlStageRunCommand = typeof AgentControlStageRunCommand.Type;

export const AgentControlStageRunCommandResult = Schema.Struct({
  state: AgentControlStageRunState,
  resultSequence: PositiveInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlStageRunCommandResult = typeof AgentControlStageRunCommandResult.Type;

export const AgentControlStageRunPreparedPayload = Schema.Struct({
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: AgentControlStageKind,
  stageOrdinal: PositiveInt,
  attemptOrdinal: PositiveInt,
  status: Schema.Literal("prepared"),
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  preparedAt: IsoDateTime,
});
export type AgentControlStageRunPreparedPayload = typeof AgentControlStageRunPreparedPayload.Type;

const StageRunIdentityPayload = {
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: Schema.Literal("planning"),
  stageKind: Schema.Literal("planning"),
  stageOrdinal: Schema.Literal(1),
  attemptOrdinal: Schema.Literal(1),
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
} as const;

const InitialPlanningLifecyclePayload = {
  ...StageRunIdentityPayload,
  handoffId: TrimmedNonEmptyString,
  handoffFingerprint: TrimmedNonEmptyString,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  providerDeliveryId: TrimmedNonEmptyString,
  providerInstanceId: ProviderInstanceId,
  providerTurnId: TrimmedNonEmptyString,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: TrimmedNonEmptyString,
  leaseId: AgentControlStageRunLeaseId,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
} as const;

export const AgentControlStageRunPlanningStartedPayload = Schema.Struct({
  ...InitialPlanningLifecyclePayload,
  status: Schema.Literal("running"),
  startedAt: IsoDateTime,
});
export type AgentControlStageRunPlanningStartedPayload =
  typeof AgentControlStageRunPlanningStartedPayload.Type;

const ImplementationStageRunIdentityPayload = {
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: Schema.Literal("implementer"),
  stageKind: Schema.Literal("implementation"),
  stageOrdinal: Schema.Literal(2),
  attemptOrdinal: Schema.Literal(1),
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
} as const;

export const AgentControlStageRunImplementationStartedPayload = Schema.Struct({
  ...ImplementationStageRunIdentityPayload,
  status: Schema.Literal("running"),
  admissionEvidenceId: TrimmedNonEmptyString,
  admissionReceiptId: TrimmedNonEmptyString,
  admissionMarkerId: TrimmedNonEmptyString,
  materializationEvidenceId: TrimmedNonEmptyString,
  materializationReceiptId: TrimmedNonEmptyString,
  materializationMarkerId: TrimmedNonEmptyString,
  handoffId: TrimmedNonEmptyString,
  handoffFingerprint: TrimmedNonEmptyString,
  providerDeliveryId: TrimmedNonEmptyString,
  deliveryRevision: PositiveInt,
  claimGeneration: PositiveInt,
  attemptCount: PositiveInt,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: TrimmedNonEmptyString,
  proposedPlanDigest: TrimmedNonEmptyString,
  providerInstanceId: ProviderInstanceId,
  providerTurnId: TrimmedNonEmptyString,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: TrimmedNonEmptyString,
  leaseId: AgentControlStageRunLeaseId,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  startedAt: IsoDateTime,
});
export type AgentControlStageRunImplementationStartedPayload =
  typeof AgentControlStageRunImplementationStartedPayload.Type;

const VerificationStageRunIdentityPayload = {
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: Schema.Literal("verifier"),
  stageKind: Schema.Literal("verification"),
  stageOrdinal: Schema.Literal(3),
  attemptOrdinal: Schema.Literal(1),
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
} as const;

export const AgentControlStageRunVerificationStartedPayload = Schema.Struct({
  ...VerificationStageRunIdentityPayload,
  status: Schema.Literal("running"),
  admissionEvidenceId: TrimmedNonEmptyString,
  admissionReceiptId: TrimmedNonEmptyString,
  admissionMarkerId: TrimmedNonEmptyString,
  materializationEvidenceId: TrimmedNonEmptyString,
  materializationReceiptId: TrimmedNonEmptyString,
  materializationMarkerId: TrimmedNonEmptyString,
  handoffId: TrimmedNonEmptyString,
  handoffFingerprint: TrimmedNonEmptyString,
  providerDeliveryId: TrimmedNonEmptyString,
  deliveryRevision: PositiveInt,
  claimGeneration: PositiveInt,
  attemptCount: PositiveInt,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: TrimmedNonEmptyString,
  proposedPlanDigest: TrimmedNonEmptyString,
  providerInstanceId: ProviderInstanceId,
  providerTurnId: TrimmedNonEmptyString,
  runtimeMode: Schema.Literal("approval-required"),
  modelSelectionFingerprint: TrimmedNonEmptyString,
  leaseId: AgentControlStageRunLeaseId,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  startedAt: IsoDateTime,
});
export type AgentControlStageRunVerificationStartedPayload =
  typeof AgentControlStageRunVerificationStartedPayload.Type;

const PlanningFinalizedPayload = {
  ...InitialPlanningLifecyclePayload,
  resultEvidenceId: TrimmedNonEmptyString,
  finalizedAt: IsoDateTime,
} as const;

const ImplementationFinalizedPayload = {
  ...ImplementationStageRunIdentityPayload,
  admissionEvidenceId: TrimmedNonEmptyString,
  admissionReceiptId: TrimmedNonEmptyString,
  admissionMarkerId: TrimmedNonEmptyString,
  materializationEvidenceId: TrimmedNonEmptyString,
  materializationReceiptId: TrimmedNonEmptyString,
  materializationMarkerId: TrimmedNonEmptyString,
  startEvidenceId: TrimmedNonEmptyString,
  startReceiptId: TrimmedNonEmptyString,
  startMarkerId: TrimmedNonEmptyString,
  handoffId: TrimmedNonEmptyString,
  handoffFingerprint: TrimmedNonEmptyString,
  providerDeliveryId: TrimmedNonEmptyString,
  deliveryRevision: PositiveInt,
  claimGeneration: PositiveInt,
  attemptCount: PositiveInt,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: TrimmedNonEmptyString,
  proposedPlanDigest: TrimmedNonEmptyString,
  repositoryDisplay: TrimmedNonEmptyString,
  sourceRevision: TrimmedNonEmptyString,
  taskSourceEventId: EventId,
  taskSourceEventSequence: PositiveInt,
  taskSourceEventStreamVersion: PositiveInt,
  worktreeReservationId: TrimmedNonEmptyString,
  worktreeEventId: EventId,
  worktreeEventSequence: PositiveInt,
  worktreeEventStreamVersion: PositiveInt,
  worktreeOwnershipFingerprint: TrimmedNonEmptyString,
  turnRequestCommandId: CommandId,
  messageId: TrimmedNonEmptyString,
  messageEventId: EventId,
  turnRequestEventId: EventId,
  providerInstanceId: ProviderInstanceId,
  providerTurnId: TrimmedNonEmptyString,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: TrimmedNonEmptyString,
  leaseId: AgentControlStageRunLeaseId,
  leaseHolderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  providerStartedEventId: EventId,
  providerStartedSequence: PositiveInt,
  providerStartedStreamVersion: PositiveInt,
  providerTerminalEventId: EventId,
  providerTerminalSequence: PositiveInt,
  providerTerminalStreamVersion: PositiveInt,
  orchestrationHistoryDigest: TrimmedNonEmptyString,
  orchestrationHistoryEventCount: PositiveInt,
  resultEvidenceId: TrimmedNonEmptyString,
  finalizedAt: IsoDateTime,
} as const;

export const AgentControlStageRunPlanningSucceededPayload = Schema.Struct({
  ...PlanningFinalizedPayload,
  status: Schema.Literal("succeeded"),
});
export type AgentControlStageRunPlanningSucceededPayload =
  typeof AgentControlStageRunPlanningSucceededPayload.Type;

export const AgentControlStageRunPlanningFailedPayload = Schema.Struct({
  ...PlanningFinalizedPayload,
  status: Schema.Literal("failed"),
});
export type AgentControlStageRunPlanningFailedPayload =
  typeof AgentControlStageRunPlanningFailedPayload.Type;

export const AgentControlStageRunPlanningCancelledPayload = Schema.Struct({
  ...PlanningFinalizedPayload,
  status: Schema.Literal("cancelled"),
});
export type AgentControlStageRunPlanningCancelledPayload =
  typeof AgentControlStageRunPlanningCancelledPayload.Type;

export const AgentControlStageRunImplementationSucceededPayload = Schema.Struct({
  ...ImplementationFinalizedPayload,
  deliveryTerminalState: Schema.Literal("completed"),
  status: Schema.Literal("succeeded"),
});
export type AgentControlStageRunImplementationSucceededPayload =
  typeof AgentControlStageRunImplementationSucceededPayload.Type;

export const AgentControlStageRunImplementationFailedPayload = Schema.Struct({
  ...ImplementationFinalizedPayload,
  deliveryTerminalState: Schema.Literal("failed"),
  status: Schema.Literal("failed"),
});
export type AgentControlStageRunImplementationFailedPayload =
  typeof AgentControlStageRunImplementationFailedPayload.Type;

export const AgentControlStageRunImplementationCancelledPayload = Schema.Struct({
  ...ImplementationFinalizedPayload,
  deliveryTerminalState: Schema.Literal("interrupted"),
  status: Schema.Literal("cancelled"),
});
export type AgentControlStageRunImplementationCancelledPayload =
  typeof AgentControlStageRunImplementationCancelledPayload.Type;

export const AgentControlStageRunLifecyclePayload = Schema.Union([
  AgentControlStageRunPreparedPayload,
  AgentControlStageRunPlanningStartedPayload,
  AgentControlStageRunImplementationStartedPayload,
  AgentControlStageRunVerificationStartedPayload,
  AgentControlStageRunPlanningSucceededPayload,
  AgentControlStageRunPlanningFailedPayload,
  AgentControlStageRunPlanningCancelledPayload,
  AgentControlStageRunImplementationSucceededPayload,
  AgentControlStageRunImplementationFailedPayload,
  AgentControlStageRunImplementationCancelledPayload,
]);
export type AgentControlStageRunLifecyclePayload = typeof AgentControlStageRunLifecyclePayload.Type;

const EventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("stage-run"),
  aggregateId: AgentControlStageRunId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

const PreparedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.prepared"),
  authority: Schema.Literal("controller"),
  payload: AgentControlStageRunPreparedPayload,
});
const PlanningStartedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.planningStarted"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunPlanningStartedPayload,
});
const ImplementationStartedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.implementationStarted"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunImplementationStartedPayload,
});
const VerificationStartedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.verificationStarted"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunVerificationStartedPayload,
});
const PlanningSucceededEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.planningSucceeded"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunPlanningSucceededPayload,
});
const PlanningFailedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.planningFailed"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunPlanningFailedPayload,
});
const PlanningCancelledEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.planningCancelled"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunPlanningCancelledPayload,
});
const ImplementationSucceededEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.implementationSucceeded"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunImplementationSucceededPayload,
});
const ImplementationFailedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.implementationFailed"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunImplementationFailedPayload,
});
const ImplementationCancelledEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRun.implementationCancelled"),
  authority: Schema.Literal("system"),
  payload: AgentControlStageRunImplementationCancelledPayload,
});

export const AgentControlStageRunEventDraft = Schema.Union([
  PreparedEventDraft,
  PlanningStartedEventDraft,
  ImplementationStartedEventDraft,
  VerificationStartedEventDraft,
  PlanningSucceededEventDraft,
  PlanningFailedEventDraft,
  PlanningCancelledEventDraft,
  ImplementationSucceededEventDraft,
  ImplementationFailedEventDraft,
  ImplementationCancelledEventDraft,
]);
export type AgentControlStageRunEventDraft = typeof AgentControlStageRunEventDraft.Type;

export const AgentControlStageRunEvent = Schema.Union([
  Schema.Struct({
    ...PreparedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...PlanningStartedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...ImplementationStartedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...VerificationStartedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...PlanningSucceededEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...PlanningFailedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...PlanningCancelledEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...ImplementationSucceededEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...ImplementationFailedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...ImplementationCancelledEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
]);
export type AgentControlStageRunEvent = typeof AgentControlStageRunEvent.Type;

export const AGENT_CONTROL_STAGE_RUN_REJECTED_COMMAND_CODES = [
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
  "stage-run-identity-conflict",
  "stage-run-projection-corrupt",
  "revision-conflict",
  "state-not-available",
  "command-identity-mismatch",
  "command-previously-rejected",
  "internal-persistence-error",
] as const;
export const AgentControlStageRunRejectedCommandCode = Schema.Literals(
  AGENT_CONTROL_STAGE_RUN_REJECTED_COMMAND_CODES,
);
export type AgentControlStageRunRejectedCommandCode =
  typeof AgentControlStageRunRejectedCommandCode.Type;

/** Closed wire error: it cannot carry source content, paths, commands, or causes. */
export class AgentControlStageRunRpcError extends Schema.TaggedErrorClass<AgentControlStageRunRpcError>()(
  "AgentControlStageRunRpcError",
  {
    code: AgentControlStageRunRejectedCommandCode,
    operation: Schema.Literals(["get-stage-run", "list-stage-runs", "prepare-initial", "dispatch"]),
    projectId: ProjectId,
    taskId: Schema.NullOr(AgentControlTaskId),
  },
) {}

export type AgentControlStageRunCommandError = AgentControlStageRunRpcError;
