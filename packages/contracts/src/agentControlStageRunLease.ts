/**
 * Schema-only contracts for durable stage-run writer reservations.
 *
 * A lease reserves future writer authority only. It does not authorize or
 * prove worktree creation, process execution, provider access, or termination.
 *
 * @module agentControlStageRunLease
 */
import * as Schema from "effect/Schema";

import {
  AgentControlAttemptId,
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
} from "./baseSchemas.ts";

export const AGENT_CONTROL_STAGE_RUN_LEASE_RPC_METHODS = {
  getLease: "agentControlStageRunLease.getLease",
  listLeases: "agentControlStageRunLease.listLeases",
} as const;

export const AgentControlStageRunLeaseStatus = Schema.Literals(["reserved", "released"]);
export type AgentControlStageRunLeaseStatus = typeof AgentControlStageRunLeaseStatus.Type;

const LeaseStateBase = {
  schemaVersion: Schema.Literal(1),
  leaseId: AgentControlStageRunLeaseId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  acquiredAt: IsoDateTime,
  renewedAt: IsoDateTime,
  expiresAt: IsoDateTime,
  revision: PositiveInt,
  sequence: PositiveInt,
} as const;
export const AgentControlStageRunLeaseState = Schema.Union([
  Schema.Struct({
    ...LeaseStateBase,
    status: Schema.Literal("reserved"),
    releasedAt: Schema.Null,
  }),
  Schema.Struct({
    ...LeaseStateBase,
    status: Schema.Literal("released"),
    releasedAt: IsoDateTime,
  }),
]);
export type AgentControlStageRunLeaseState = typeof AgentControlStageRunLeaseState.Type;

export const AgentControlStageRunLeaseOwnership = Schema.Literals([
  "current-runtime",
  "foreign-runtime",
  "none",
]);
export type AgentControlStageRunLeaseOwnership = typeof AgentControlStageRunLeaseOwnership.Type;

export const AgentControlStageRunLeaseHealth = Schema.Literals([
  "healthy",
  "expiring",
  "expired",
  "recovery-required",
]);
export type AgentControlStageRunLeaseHealth = typeof AgentControlStageRunLeaseHealth.Type;

/** Wire-safe inspection view. The persistent holder identity is never exposed. */
export const AgentControlStageRunLeaseView = Schema.Struct({
  leaseId: AgentControlStageRunLeaseId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  fenceToken: PositiveInt,
  status: AgentControlStageRunLeaseStatus,
  ownership: AgentControlStageRunLeaseOwnership,
  health: AgentControlStageRunLeaseHealth,
  acquiredAt: IsoDateTime,
  renewedAt: IsoDateTime,
  expiresAt: IsoDateTime,
  releasedAt: Schema.NullOr(IsoDateTime),
  revision: PositiveInt,
});
export type AgentControlStageRunLeaseView = typeof AgentControlStageRunLeaseView.Type;

export const AgentControlStageRunLeaseGetInput = Schema.Struct({
  projectId: ProjectId,
  taskId: AgentControlTaskId,
});
export type AgentControlStageRunLeaseGetInput = typeof AgentControlStageRunLeaseGetInput.Type;

export const AgentControlStageRunLeaseListInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlStageRunLeaseListInput = typeof AgentControlStageRunLeaseListInput.Type;

export const AgentControlStageRunLeaseListResult = Schema.Struct({
  projectId: ProjectId,
  leases: Schema.Array(AgentControlStageRunLeaseView),
  quarantinedCount: NonNegativeInt,
});
export type AgentControlStageRunLeaseListResult = typeof AgentControlStageRunLeaseListResult.Type;

export const AgentControlStageRunLeaseCommandAuthority = Schema.Literals(["controller", "system"]);
export type AgentControlStageRunLeaseCommandAuthority =
  typeof AgentControlStageRunLeaseCommandAuthority.Type;

const CommandBase = {
  commandId: CommandId,
  authority: AgentControlStageRunLeaseCommandAuthority,
  leaseId: AgentControlStageRunLeaseId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  expectedRevision: NonNegativeInt,
} as const;

export const AgentControlStageRunLeaseReserveCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRunLease.reserve"),
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  leaseDurationMs: PositiveInt,
});
export type AgentControlStageRunLeaseReserveCommand =
  typeof AgentControlStageRunLeaseReserveCommand.Type;

export const AgentControlStageRunLeaseRenewCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRunLease.renew"),
  leaseDurationMs: PositiveInt,
});
export type AgentControlStageRunLeaseRenewCommand =
  typeof AgentControlStageRunLeaseRenewCommand.Type;

/**
 * Releases only a reservation for which execution has not started. This is
 * never termination proof and must not be reused as a general force-release.
 */
export const AgentControlStageRunLeaseReleaseBeforeExecutionCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRunLease.releaseBeforeExecution"),
});
export type AgentControlStageRunLeaseReleaseBeforeExecutionCommand =
  typeof AgentControlStageRunLeaseReleaseBeforeExecutionCommand.Type;

/** Reserved future transitions. The foundation rejects every use. */
export const AgentControlStageRunLeaseUnavailableCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.stageRunLease.transition"),
  targetStatus: Schema.Literals(["running", "draining", "terminated", "takeover", "revoked"]),
});
export type AgentControlStageRunLeaseUnavailableCommand =
  typeof AgentControlStageRunLeaseUnavailableCommand.Type;

export const AgentControlStageRunLeaseCommand = Schema.Union([
  AgentControlStageRunLeaseReserveCommand,
  AgentControlStageRunLeaseRenewCommand,
  AgentControlStageRunLeaseReleaseBeforeExecutionCommand,
  AgentControlStageRunLeaseUnavailableCommand,
]);
export type AgentControlStageRunLeaseCommand = typeof AgentControlStageRunLeaseCommand.Type;

const EventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("stage-run-lease"),
  aggregateId: AgentControlStageRunLeaseId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: AgentControlStageRunLeaseCommandAuthority,
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

export const AgentControlStageRunLeaseReservedPayload = Schema.Struct({
  leaseId: AgentControlStageRunLeaseId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: Schema.String,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  acquiredAt: IsoDateTime,
  renewedAt: IsoDateTime,
  expiresAt: IsoDateTime,
});
export type AgentControlStageRunLeaseReservedPayload =
  typeof AgentControlStageRunLeaseReservedPayload.Type;

export const AgentControlStageRunLeaseRenewedPayload = Schema.Struct({
  leaseId: AgentControlStageRunLeaseId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  renewedAt: IsoDateTime,
  expiresAt: IsoDateTime,
});
export type AgentControlStageRunLeaseRenewedPayload =
  typeof AgentControlStageRunLeaseRenewedPayload.Type;

export const AgentControlStageRunLeaseReleasedPayload = Schema.Struct({
  leaseId: AgentControlStageRunLeaseId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  holderId: AgentControlStageRunLeaseHolderId,
  fenceToken: PositiveInt,
  releasedAt: IsoDateTime,
});
export type AgentControlStageRunLeaseReleasedPayload =
  typeof AgentControlStageRunLeaseReleasedPayload.Type;

const ReservedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRunLease.reserved"),
  payload: AgentControlStageRunLeaseReservedPayload,
});
const RenewedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRunLease.renewed"),
  payload: AgentControlStageRunLeaseRenewedPayload,
});
const ReleasedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.stageRunLease.releasedBeforeExecution"),
  payload: AgentControlStageRunLeaseReleasedPayload,
});

export const AgentControlStageRunLeaseEventDraft = Schema.Union([
  ReservedEventDraft,
  RenewedEventDraft,
  ReleasedEventDraft,
]);
export type AgentControlStageRunLeaseEventDraft = typeof AgentControlStageRunLeaseEventDraft.Type;

export const AgentControlStageRunLeaseEvent = Schema.Union([
  Schema.Struct({
    ...ReservedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...RenewedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...ReleasedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
]);
export type AgentControlStageRunLeaseEvent = typeof AgentControlStageRunLeaseEvent.Type;

export const AgentControlStageRunLeaseCommandResult = Schema.Struct({
  state: AgentControlStageRunLeaseState,
  resultSequence: PositiveInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlStageRunLeaseCommandResult =
  typeof AgentControlStageRunLeaseCommandResult.Type;

export const AGENT_CONTROL_STAGE_RUN_LEASE_REJECTED_COMMAND_CODES = [
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
  "lease-already-reserved",
  "lease-projection-corrupt",
  "holder-mismatch",
  "fence-token-mismatch",
  "revision-conflict",
  "state-not-available",
  "command-identity-mismatch",
  "command-previously-rejected",
  "internal-persistence-error",
] as const;
export const AgentControlStageRunLeaseRejectedCommandCode = Schema.Literals(
  AGENT_CONTROL_STAGE_RUN_LEASE_REJECTED_COMMAND_CODES,
);
export type AgentControlStageRunLeaseRejectedCommandCode =
  typeof AgentControlStageRunLeaseRejectedCommandCode.Type;

/** Closed wire error: no holder, process, path, source content, or SQL cause. */
export class AgentControlStageRunLeaseRpcError extends Schema.TaggedErrorClass<AgentControlStageRunLeaseRpcError>()(
  "AgentControlStageRunLeaseRpcError",
  {
    code: AgentControlStageRunLeaseRejectedCommandCode,
    operation: Schema.Literals(["get-lease", "list-leases", "dispatch"]),
    projectId: ProjectId,
    taskId: Schema.NullOr(AgentControlTaskId),
  },
) {}
