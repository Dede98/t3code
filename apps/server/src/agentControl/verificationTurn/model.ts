import type {
  AgentControlControlledThreadReservationId,
  CommandId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";

export interface AgentControlVerificationHandoffEvidence {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly materializationEvidenceId: string;
  readonly materializationReceiptId: string;
  readonly materializationMarkerId: string;
  readonly admissionEvidenceId: string;
  readonly admissionReceiptId: string;
  readonly admissionMarkerId: string;
  readonly projectId: ProjectId;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly taskSourceEventId: string;
  readonly taskSourceEventSequence: number;
  readonly taskSourceEventStreamVersion: number;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly worktreeRevision: number;
  readonly worktreeEventId: string;
  readonly worktreeEventSequence: number;
  readonly worktreeEventStreamVersion: number;
  readonly worktreeOwnershipFingerprint: string;
  readonly worktreeVerifiedAt: string;
  readonly worktreePath: string;
  readonly branch: string;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly planningThreadId: ThreadId;
  readonly planId: string;
  readonly proposedPlanDigest: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly runtimeMode: "approval-required";
  readonly modelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly templateVersion: string;
  readonly promptText: string;
  readonly promptDigest: string;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: MessageId;
  readonly messageEventId: string;
  readonly turnRequestEventId: string;
  readonly messageEventTemplateJson: string;
  readonly turnRequestEventTemplateJson: string;
  readonly eventTemplateDigest: string;
  readonly providerDeliveryId: string;
  readonly createdAt: string;
}

export type AgentControlVerificationDeliveryState =
  | "pending"
  | "turn-accepted"
  | "claimed"
  | "delivery-attempted"
  | "provider-started"
  | "retry-wait"
  | "ambiguous";

export interface AgentControlVerificationDelivery {
  readonly providerDeliveryId: string;
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly admissionMarkerId: string;
  readonly materializationEvidenceId: string;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly providerInstanceId: ProviderInstanceId;
  readonly runtimeMode: "approval-required";
  readonly modelSelectionFingerprint: string;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: MessageId;
  readonly planningThreadId: ThreadId;
  readonly planId: string;
  readonly state: AgentControlVerificationDeliveryState;
  readonly revision: number;
  readonly claimOwnerId: string | null;
  readonly claimGeneration: number;
  readonly claimExpiresAt: string | null;
  readonly attemptCount: number;
  readonly nextAttemptAt: string | null;
  readonly providerTurnId: string | null;
  readonly providerAcceptedAt: string | null;
  readonly providerSessionCreatedAt: string | null;
  readonly providerResumeCursorJson: string | null;
  readonly terminalAt: string | null;
  readonly lastErrorCode: string | null;
  readonly interruptRequested: boolean;
  readonly updatedAt: string;
}

export interface AgentControlVerificationClaim {
  readonly evidence: AgentControlVerificationHandoffEvidence;
  readonly delivery: AgentControlVerificationDelivery;
}
