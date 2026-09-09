/**
 * Schema-only contracts for Agent Control worktree reservations.
 *
 * Local paths and runtime holder identity are persistent server state only and
 * are deliberately absent from every wire view.
 *
 * @module agentControlWorktree
 */
import * as Schema from "effect/Schema";

import {
  AgentControlAttemptId,
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

export const AGENT_CONTROL_WORKTREE_RPC_METHODS = {
  getReservation: "agentControlWorktree.getReservation",
  listReservations: "agentControlWorktree.listReservations",
} as const;

export const AgentControlWorktreeReservationStatus = Schema.Literals([
  "reserved",
  "materializing",
  "ready",
  "needs-attention",
]);
export type AgentControlWorktreeReservationStatus =
  typeof AgentControlWorktreeReservationStatus.Type;

export const AGENT_CONTROL_WORKTREE_ATTENTION_CODES = [
  "path-occupied",
  "branch-commit-mismatch",
  "branch-in-other-worktree",
  "worktree-registration-mismatch",
  "worktree-registration-ambiguous",
  "worktree-branch-mismatch",
  "worktree-head-mismatch",
  "repository-identity-mismatch",
  "ownership-unproven",
  "ownership-mismatch",
  "worktree-dirty",
  "worktree-sequencer-state",
] as const;
export const AgentControlWorktreeAttentionCode = Schema.Literals(
  AGENT_CONTROL_WORKTREE_ATTENTION_CODES,
);
export type AgentControlWorktreeAttentionCode = typeof AgentControlWorktreeAttentionCode.Type;

export const AgentControlWorktreeRepositoryIdentity = Schema.Struct({
  repositoryNodeId: TrimmedNonEmptyString,
  nameWithOwner: TrimmedNonEmptyString,
  canonicalKey: TrimmedNonEmptyString,
  remoteName: TrimmedNonEmptyString,
  remoteUrl: TrimmedNonEmptyString,
  defaultRemoteRef: TrimmedNonEmptyString,
  commonDirDevice: NonNegativeInt,
  commonDirInode: NonNegativeInt,
});
export type AgentControlWorktreeRepositoryIdentity =
  typeof AgentControlWorktreeRepositoryIdentity.Type;

export const AgentControlWorktreeMaterializationPhase = Schema.Literals([
  "reserved",
  "materializing",
  "git-created",
  "ownership-marked",
]);
export type AgentControlWorktreeMaterializationPhase =
  typeof AgentControlWorktreeMaterializationPhase.Type;

const ReservationStateBase = {
  schemaVersion: Schema.Literal(1),
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  repository: AgentControlWorktreeRepositoryIdentity,
  repositoryWorkspace: TrimmedNonEmptyString,
  repositoryCommonDir: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  baseCommitSha: TrimmedNonEmptyString,
  branchName: TrimmedNonEmptyString,
  internalWorktreePath: TrimmedNonEmptyString,
  targetGenerationId: TrimmedNonEmptyString,
  worktreeRootDevice: NonNegativeInt,
  worktreeRootInode: NonNegativeInt,
  worktreeParentDevice: NonNegativeInt,
  worktreeParentInode: NonNegativeInt,
  materializationPhase: AgentControlWorktreeMaterializationPhase,
  gitCreatedDevice: Schema.NullOr(NonNegativeInt),
  gitCreatedInode: Schema.NullOr(NonNegativeInt),
  gitCreatedGitDir: Schema.NullOr(TrimmedNonEmptyString),
  markedOwnershipFingerprint: Schema.NullOr(TrimmedNonEmptyString),
  headCommitSha: Schema.NullOr(TrimmedNonEmptyString),
  ownershipFingerprint: Schema.NullOr(TrimmedNonEmptyString),
  verifiedAt: Schema.NullOr(IsoDateTime),
  reservedAt: IsoDateTime,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  revision: PositiveInt,
  sequence: PositiveInt,
} as const;

export const AgentControlWorktreeReservationState = Schema.Union([
  Schema.Struct({
    ...ReservationStateBase,
    status: Schema.Literals(["reserved", "materializing", "ready"]),
    attentionCode: Schema.Null,
  }),
  Schema.Struct({
    ...ReservationStateBase,
    status: Schema.Literal("needs-attention"),
    attentionCode: AgentControlWorktreeAttentionCode,
  }),
]);
export type AgentControlWorktreeReservationState = typeof AgentControlWorktreeReservationState.Type;

/**
 * Wire-safe projection. Absolute local paths, ownership metadata, and holder
 * identity never cross RPC.
 *
 * `ready` means that the reservation was completely verified at `verifiedAt`.
 * It is not an execution permit: every consumer must use the server-side
 * `useReadyWorktree` guard immediately before touching the worktree.
 */
export const AgentControlWorktreeReservationView = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  repositoryNodeId: TrimmedNonEmptyString,
  branchName: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  baseCommitSha: TrimmedNonEmptyString,
  headCommitSha: Schema.NullOr(TrimmedNonEmptyString),
  status: AgentControlWorktreeReservationStatus,
  attentionCode: Schema.NullOr(AgentControlWorktreeAttentionCode),
  verifiedAt: Schema.NullOr(IsoDateTime),
  revision: PositiveInt,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type AgentControlWorktreeReservationView = typeof AgentControlWorktreeReservationView.Type;

export const AgentControlWorktreeGetInput = Schema.Struct({
  projectId: ProjectId,
  reservationId: AgentControlWorktreeReservationId,
});
export type AgentControlWorktreeGetInput = typeof AgentControlWorktreeGetInput.Type;

export const AgentControlWorktreeListInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlWorktreeListInput = typeof AgentControlWorktreeListInput.Type;

export const AgentControlWorktreeListResult = Schema.Struct({
  projectId: ProjectId,
  reservations: Schema.Array(AgentControlWorktreeReservationView),
  quarantinedCount: NonNegativeInt,
});
export type AgentControlWorktreeListResult = typeof AgentControlWorktreeListResult.Type;

const CommandBase = {
  commandId: CommandId,
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  expectedRevision: NonNegativeInt,
} as const;

export const AgentControlWorktreeTargetClaimCloseEvidence = Schema.Struct({
  pendingToken: TrimmedNonEmptyString,
  claimAttemptId: TrimmedNonEmptyString,
  expectedRevision: PositiveInt,
  resultingRevision: PositiveInt,
  targetGeneration: TrimmedNonEmptyString,
  compositeCommandId: CommandId,
  compositeOperation: Schema.Literals(["reserve-and-materialize", "reconcile"]),
  compositeFingerprint: TrimmedNonEmptyString,
  reservationId: AgentControlWorktreeReservationId,
  phase: Schema.Literals(["materialized", "retained-attention"]),
});
export type AgentControlWorktreeTargetClaimCloseEvidence =
  typeof AgentControlWorktreeTargetClaimCloseEvidence.Type;

export const AgentControlWorktreeReserveCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.worktree.reserve"),
  repository: AgentControlWorktreeRepositoryIdentity,
  repositoryWorkspace: TrimmedNonEmptyString,
  repositoryCommonDir: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  baseCommitSha: TrimmedNonEmptyString,
  branchName: TrimmedNonEmptyString,
  internalWorktreePath: TrimmedNonEmptyString,
  targetGenerationId: TrimmedNonEmptyString,
  worktreeRootDevice: NonNegativeInt,
  worktreeRootInode: NonNegativeInt,
  worktreeParentDevice: NonNegativeInt,
  worktreeParentInode: NonNegativeInt,
});
export type AgentControlWorktreeReserveCommand = typeof AgentControlWorktreeReserveCommand.Type;

export const AgentControlWorktreeStartMaterializationCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.worktree.materialization.start"),
});
export type AgentControlWorktreeStartMaterializationCommand =
  typeof AgentControlWorktreeStartMaterializationCommand.Type;

export const AgentControlWorktreeMarkReadyCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.worktree.ready"),
  headCommitSha: TrimmedNonEmptyString,
  ownershipFingerprint: TrimmedNonEmptyString,
  gitCreatedDevice: NonNegativeInt,
  gitCreatedInode: NonNegativeInt,
  gitCreatedGitDir: TrimmedNonEmptyString,
  markedOwnershipFingerprint: TrimmedNonEmptyString,
  verifiedAt: IsoDateTime,
  targetClaimCloseEvidence: AgentControlWorktreeTargetClaimCloseEvidence,
});
export type AgentControlWorktreeMarkReadyCommand = typeof AgentControlWorktreeMarkReadyCommand.Type;

export const AgentControlWorktreeNeedsAttentionCommand = Schema.Struct({
  ...CommandBase,
  type: Schema.Literal("agentControl.worktree.needsAttention"),
  attentionCode: AgentControlWorktreeAttentionCode,
  materializationPhase: AgentControlWorktreeMaterializationPhase,
  gitCreatedDevice: Schema.NullOr(NonNegativeInt),
  gitCreatedInode: Schema.NullOr(NonNegativeInt),
  gitCreatedGitDir: Schema.NullOr(TrimmedNonEmptyString),
  markedOwnershipFingerprint: Schema.NullOr(TrimmedNonEmptyString),
  targetClaimCloseEvidence: Schema.NullOr(AgentControlWorktreeTargetClaimCloseEvidence),
});
export type AgentControlWorktreeNeedsAttentionCommand =
  typeof AgentControlWorktreeNeedsAttentionCommand.Type;

export const AgentControlWorktreeCommand = Schema.Union([
  AgentControlWorktreeReserveCommand,
  AgentControlWorktreeStartMaterializationCommand,
  AgentControlWorktreeMarkReadyCommand,
  AgentControlWorktreeNeedsAttentionCommand,
]);
export type AgentControlWorktreeCommand = typeof AgentControlWorktreeCommand.Type;

const EventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("worktree-reservation"),
  aggregateId: AgentControlWorktreeReservationId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  authority: Schema.Literal("controller"),
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

export const AgentControlWorktreeReservedPayload = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: PositiveInt,
  githubIntakeSequence: PositiveInt,
  sourceIdentityFingerprint: TrimmedNonEmptyString,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  repository: AgentControlWorktreeRepositoryIdentity,
  repositoryWorkspace: TrimmedNonEmptyString,
  repositoryCommonDir: TrimmedNonEmptyString,
  baseRef: TrimmedNonEmptyString,
  baseCommitSha: TrimmedNonEmptyString,
  branchName: TrimmedNonEmptyString,
  internalWorktreePath: TrimmedNonEmptyString,
  targetGenerationId: TrimmedNonEmptyString,
  worktreeRootDevice: NonNegativeInt,
  worktreeRootInode: NonNegativeInt,
  worktreeParentDevice: NonNegativeInt,
  worktreeParentInode: NonNegativeInt,
  reservedAt: IsoDateTime,
});

export const AgentControlWorktreeTransitionPayload = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  transitionedAt: IsoDateTime,
});

const ReservedEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.worktree.reserved"),
  payload: AgentControlWorktreeReservedPayload,
});
const MaterializingEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.worktree.materializationStarted"),
  payload: AgentControlWorktreeTransitionPayload,
});
const ReadyEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.worktree.ready"),
  payload: Schema.Struct({
    ...AgentControlWorktreeTransitionPayload.fields,
    headCommitSha: TrimmedNonEmptyString,
    ownershipFingerprint: TrimmedNonEmptyString,
    gitCreatedDevice: NonNegativeInt,
    gitCreatedInode: NonNegativeInt,
    gitCreatedGitDir: TrimmedNonEmptyString,
    markedOwnershipFingerprint: TrimmedNonEmptyString,
    verifiedAt: IsoDateTime,
    targetClaimCloseEvidence: AgentControlWorktreeTargetClaimCloseEvidence,
  }),
});
const NeedsAttentionEventDraft = Schema.Struct({
  ...EventBase,
  type: Schema.Literal("agentControl.worktree.needsAttention"),
  payload: Schema.Struct({
    ...AgentControlWorktreeTransitionPayload.fields,
    attentionCode: AgentControlWorktreeAttentionCode,
    materializationPhase: AgentControlWorktreeMaterializationPhase,
    gitCreatedDevice: Schema.NullOr(NonNegativeInt),
    gitCreatedInode: Schema.NullOr(NonNegativeInt),
    gitCreatedGitDir: Schema.NullOr(TrimmedNonEmptyString),
    markedOwnershipFingerprint: Schema.NullOr(TrimmedNonEmptyString),
    targetClaimCloseEvidence: Schema.NullOr(AgentControlWorktreeTargetClaimCloseEvidence),
  }),
});

export const AgentControlWorktreeEventDraft = Schema.Union([
  ReservedEventDraft,
  MaterializingEventDraft,
  ReadyEventDraft,
  NeedsAttentionEventDraft,
]);
export type AgentControlWorktreeEventDraft = typeof AgentControlWorktreeEventDraft.Type;

export const AgentControlWorktreeEvent = Schema.Union([
  Schema.Struct({
    ...ReservedEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...MaterializingEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({ ...ReadyEventDraft.fields, streamVersion: PositiveInt, sequence: PositiveInt }),
  Schema.Struct({
    ...NeedsAttentionEventDraft.fields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
]);
export type AgentControlWorktreeEvent = typeof AgentControlWorktreeEvent.Type;

export const AgentControlWorktreeCommandResult = Schema.Struct({
  state: AgentControlWorktreeReservationState,
  resultSequence: PositiveInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlWorktreeCommandResult = typeof AgentControlWorktreeCommandResult.Type;

export const AGENT_CONTROL_WORKTREE_REJECTED_COMMAND_CODES = [
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
  "lease-recovery-required",
  "lease-projection-corrupt",
  "fence-token-mismatch",
  "reservation-missing",
  "reservation-conflict",
  "reservation-projection-corrupt",
  "revision-conflict",
  "state-not-available",
  "command-identity-mismatch",
  "command-previously-rejected",
  "repository-unavailable",
  "repository-identity-mismatch",
  "default-remote-ref-unavailable",
  "branch-name-invalid",
  "worktree-path-invalid",
  "repository-lock-unavailable",
  "internal-persistence-error",
] as const;
export const AgentControlWorktreeRejectedCommandCode = Schema.Literals(
  AGENT_CONTROL_WORKTREE_REJECTED_COMMAND_CODES,
);
export type AgentControlWorktreeRejectedCommandCode =
  typeof AgentControlWorktreeRejectedCommandCode.Type;

export class AgentControlWorktreeRpcError extends Schema.TaggedError<AgentControlWorktreeRpcError>()(
  "AgentControlWorktreeRpcError",
  {
    code: AgentControlWorktreeRejectedCommandCode,
    operation: Schema.Literals([
      "get-reservation",
      "list-reservations",
      "reserve",
      "materialize",
      "reconcile",
    ]),
    projectId: ProjectId,
    taskId: Schema.NullOr(AgentControlTaskId),
    reservationId: Schema.NullOr(AgentControlWorktreeReservationId),
  },
) {}
