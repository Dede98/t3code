import type {
  AgentControlControlledThreadReservationId,
  CommandId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";

export interface AgentControlInitialPlanningHandoffEvidence {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly coordinatorCommandId: CommandId;
  readonly coordinatorCommandFingerprint: string;
  readonly materializationCommandId: CommandId;
  readonly materializationCommandFingerprint: string;
  readonly projectId: ProjectId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly roleId: "planning";
  readonly stageKind: "planning";
  readonly stageOrdinal: 1;
  readonly attemptOrdinal: 1;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly worktreePath: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly runtimeMode: "approval-required" | "full-access";
  readonly modelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly planningRole: "planner";
  readonly templateVersion: string;
  readonly promptText: string;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: MessageId;
  readonly messageEventId: string;
  readonly turnRequestEventId: string;
  readonly messageEventTemplateJson: string;
  readonly turnRequestEventTemplateJson: string;
  readonly eventTemplateDigest: string;
  readonly providerDeliveryId: string;
  readonly createdAt: string;
  readonly planningDeadlineAt: string;
}

export type AgentControlInitialPlanningDeliveryState =
  | "pending"
  | "turn-accepted"
  | "claimed"
  | "delivery-attempted"
  | "provider-started"
  | "interrupt-requested"
  | "retry-wait"
  | "ambiguous"
  | "completed"
  | "failed"
  | "interrupted";

export interface AgentControlInitialPlanningDelivery {
  readonly providerDeliveryId: string;
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: MessageId;
  readonly providerInstanceId: ProviderInstanceId;
  readonly state: AgentControlInitialPlanningDeliveryState;
  readonly revision: number;
  readonly claimOwnerId: string | null;
  readonly claimGeneration: number;
  readonly claimExpiresAt: string | null;
  readonly attemptCount: number;
  readonly nextAttemptAt: string | null;
  readonly planningDeadlineAt: string;
  readonly providerTurnId: string | null;
  readonly providerAcceptedAt: string | null;
  readonly providerSessionCreatedAt: string | null;
  readonly providerResumeCursorJson: string | null;
  readonly terminalAt: string | null;
  readonly lastErrorCode: string | null;
  readonly interruptRequested: boolean;
  readonly updatedAt: string;
}

export interface AgentControlInitialPlanningClaim {
  readonly evidence: AgentControlInitialPlanningHandoffEvidence;
  readonly delivery: AgentControlInitialPlanningDelivery;
}
