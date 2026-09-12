import type { ModelSelection, ProjectId, ThreadId } from "@t3tools/contracts";

import {
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
} from "../initialPlanning/eventEvidence.ts";
import { verificationMessagePayload, verificationTurnRequestPayload } from "./eventEvidence.ts";
import {
  deriveVerificationHandoffId,
  deriveVerificationMessageEventId,
  deriveVerificationMessageId,
  deriveVerificationProviderDeliveryId,
  deriveVerificationTurnRequestCommandId,
  deriveVerificationTurnRequestEventId,
  fingerprintVerificationHandoff,
} from "./identity.ts";
import type { AgentControlVerificationHandoffEvidence } from "./model.ts";
import {
  AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT,
  AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT_V2,
  AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION_V2,
  isStructuredAgentControlVerificationPromptVersion,
  type AgentControlVerificationPromptTemplateVersion,
  AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION,
  AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION_V1,
  buildAgentControlVerificationPrompt,
} from "./prompt.ts";
import {
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
} from "./verificationResult.ts";

export interface AgentControlVerificationHandoffAuthority {
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
  readonly controlledThreadReservationId: AgentControlVerificationHandoffEvidence["controlledThreadReservationId"];
  readonly threadId: ThreadId;
  readonly planningThreadId: ThreadId;
  readonly planId: string;
  readonly proposedPlanJson: string;
  readonly proposedPlanDigest: string;
  readonly implementationHandoffJson: string;
  readonly implementationHandoffDigest: string;
  readonly implementationProviderDeliveryJson: string;
  readonly implementationProviderDeliveryDigest: string;
  readonly implementationResultJson: string;
  readonly implementationResultDigest: string;
  readonly repairReportJson?: string;
  readonly verificationAdmissionJson: string;
  readonly verificationAdmissionDigest: string;
  readonly verificationIdentityJson: string;
  readonly verificationIdentityDigest: string;
  readonly repositoryDisplay: string;
  readonly sourceRevision: string;
  readonly taskTitle: string;
  readonly taskBody: string | null;
  readonly providerInstanceId: AgentControlVerificationHandoffEvidence["providerInstanceId"];
  readonly runtimeMode: AgentControlVerificationHandoffEvidence["runtimeMode"];
  readonly modelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly createdAt: string;
}

export const buildExpectedAgentControlVerificationHandoff = (
  authority: AgentControlVerificationHandoffAuthority,
  templateVersion: AgentControlVerificationPromptTemplateVersion = AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION,
): AgentControlVerificationHandoffEvidence => {
  const handoffId = deriveVerificationHandoffId(authority.materializationEvidenceId);
  const turnRequestCommandId = deriveVerificationTurnRequestCommandId(handoffId);
  const messageId = deriveVerificationMessageId(handoffId);
  const messageEventId = deriveVerificationMessageEventId(turnRequestCommandId);
  const turnRequestEventId = deriveVerificationTurnRequestEventId(turnRequestCommandId);
  const providerDeliveryId = deriveVerificationProviderDeliveryId(handoffId);
  const prompt = buildAgentControlVerificationPrompt(
    {
      repositoryDisplay: authority.repositoryDisplay,
      taskId: authority.taskId,
      taskTitle: authority.taskTitle,
      taskBody: authority.taskBody,
      sourceRevision: authority.sourceRevision,
      planningThreadId: authority.planningThreadId,
      planId: authority.planId,
      proposedPlanJson: authority.proposedPlanJson,
      proposedPlanDigest: authority.proposedPlanDigest,
      implementationHandoffJson: authority.implementationHandoffJson,
      implementationHandoffDigest: authority.implementationHandoffDigest,
      implementationProviderDeliveryJson: authority.implementationProviderDeliveryJson,
      implementationProviderDeliveryDigest: authority.implementationProviderDeliveryDigest,
      implementationResultJson: authority.implementationResultJson,
      implementationResultDigest: authority.implementationResultDigest,
      ...(authority.repairReportJson === undefined
        ? {}
        : { repairReportJson: authority.repairReportJson }),
      verificationAdmissionJson: authority.verificationAdmissionJson,
      verificationAdmissionDigest: authority.verificationAdmissionDigest,
      verificationIdentityJson: authority.verificationIdentityJson,
      verificationIdentityDigest: authority.verificationIdentityDigest,
    },
    templateVersion,
  );
  const messageEventTemplateJson = canonicalInitialPlanningEventTemplate({
    streamVersion: 3,
    eventId: messageEventId,
    aggregateKind: "thread",
    aggregateId: authority.threadId,
    type: "thread.message-sent",
    occurredAt: authority.createdAt,
    commandId: turnRequestCommandId,
    causationEventId: null,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: verificationMessagePayload({
      threadId: authority.threadId,
      messageId,
      promptText: prompt.promptText,
      createdAt: authority.createdAt,
    }),
    metadata: {},
  });
  const turnRequestEventTemplateJson = canonicalInitialPlanningEventTemplate({
    streamVersion: 4,
    eventId: turnRequestEventId,
    aggregateKind: "thread",
    aggregateId: authority.threadId,
    type: "thread.turn-start-requested",
    occurredAt: authority.createdAt,
    commandId: turnRequestCommandId,
    causationEventId: messageEventId,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: verificationTurnRequestPayload({
      threadId: authority.threadId,
      messageId,
      modelSelection: authority.modelSelection,
      runtimeMode: authority.runtimeMode,
      sourceProposedPlan: {
        threadId: authority.planningThreadId,
        planId: authority.planId,
      },
      createdAt: authority.createdAt,
    }),
    metadata: {},
  });
  const base = {
    handoffId,
    handoffFingerprint: "",
    materializationEvidenceId: authority.materializationEvidenceId,
    materializationReceiptId: authority.materializationReceiptId,
    materializationMarkerId: authority.materializationMarkerId,
    admissionEvidenceId: authority.admissionEvidenceId,
    admissionReceiptId: authority.admissionReceiptId,
    admissionMarkerId: authority.admissionMarkerId,
    projectId: authority.projectId,
    taskId: authority.taskId,
    taskRevision: authority.taskRevision,
    githubIntakeSequence: authority.githubIntakeSequence,
    sourceIdentityFingerprint: authority.sourceIdentityFingerprint,
    taskSourceEventId: authority.taskSourceEventId,
    taskSourceEventSequence: authority.taskSourceEventSequence,
    taskSourceEventStreamVersion: authority.taskSourceEventStreamVersion,
    stageRunId: authority.stageRunId,
    attemptId: authority.attemptId,
    leaseId: authority.leaseId,
    leaseHolderId: authority.leaseHolderId,
    fenceToken: authority.fenceToken,
    worktreeReservationId: authority.worktreeReservationId,
    worktreeRevision: authority.worktreeRevision,
    worktreeEventId: authority.worktreeEventId,
    worktreeEventSequence: authority.worktreeEventSequence,
    worktreeEventStreamVersion: authority.worktreeEventStreamVersion,
    worktreeOwnershipFingerprint: authority.worktreeOwnershipFingerprint,
    worktreeVerifiedAt: authority.worktreeVerifiedAt,
    worktreePath: authority.worktreePath,
    branch: authority.branch,
    controlledThreadReservationId: authority.controlledThreadReservationId,
    threadId: authority.threadId,
    planningThreadId: authority.planningThreadId,
    planId: authority.planId,
    proposedPlanDigest: authority.proposedPlanDigest,
    providerInstanceId: authority.providerInstanceId,
    runtimeMode: authority.runtimeMode,
    modelSelection: authority.modelSelection,
    modelSelectionJson: authority.modelSelectionJson,
    modelSelectionFingerprint: authority.modelSelectionFingerprint,
    templateVersion,
    promptContractFingerprint:
      templateVersion === AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION
        ? AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT
        : templateVersion === AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION_V2
          ? AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT_V2
          : null,
    promptText: prompt.promptText,
    promptDigest: prompt.promptDigest,
    resultSchemaVersion: isStructuredAgentControlVerificationPromptVersion(templateVersion)
      ? AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION
      : null,
    resultSchemaFingerprint: isStructuredAgentControlVerificationPromptVersion(templateVersion)
      ? AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT
      : null,
    turnRequestCommandId,
    messageId,
    messageEventId,
    turnRequestEventId,
    messageEventTemplateJson,
    turnRequestEventTemplateJson,
    eventTemplateDigest: combinedInitialPlanningEventDigest(
      messageEventTemplateJson,
      turnRequestEventTemplateJson,
    ),
    providerDeliveryId,
    createdAt: authority.createdAt,
  } satisfies AgentControlVerificationHandoffEvidence;
  return {
    ...base,
    handoffFingerprint: fingerprintVerificationHandoff(base),
  };
};

const comparedFields = [
  "handoffId",
  "handoffFingerprint",
  "materializationEvidenceId",
  "materializationReceiptId",
  "materializationMarkerId",
  "admissionEvidenceId",
  "admissionReceiptId",
  "admissionMarkerId",
  "projectId",
  "taskId",
  "taskRevision",
  "githubIntakeSequence",
  "sourceIdentityFingerprint",
  "taskSourceEventId",
  "taskSourceEventSequence",
  "taskSourceEventStreamVersion",
  "stageRunId",
  "attemptId",
  "leaseId",
  "leaseHolderId",
  "fenceToken",
  "worktreeReservationId",
  "worktreeRevision",
  "worktreeEventId",
  "worktreeEventSequence",
  "worktreeEventStreamVersion",
  "worktreeOwnershipFingerprint",
  "worktreeVerifiedAt",
  "worktreePath",
  "branch",
  "controlledThreadReservationId",
  "threadId",
  "planningThreadId",
  "planId",
  "proposedPlanDigest",
  "providerInstanceId",
  "runtimeMode",
  "modelSelectionJson",
  "modelSelectionFingerprint",
  "templateVersion",
  "promptContractFingerprint",
  "promptText",
  "promptDigest",
  "resultSchemaVersion",
  "resultSchemaFingerprint",
  "turnRequestCommandId",
  "messageId",
  "messageEventId",
  "turnRequestEventId",
  "messageEventTemplateJson",
  "turnRequestEventTemplateJson",
  "eventTemplateDigest",
  "providerDeliveryId",
  "createdAt",
] as const satisfies ReadonlyArray<keyof AgentControlVerificationHandoffEvidence>;

export const verificationHandoffAuthorityMismatch = (
  authority: AgentControlVerificationHandoffAuthority,
  evidence: AgentControlVerificationHandoffEvidence,
): string | null => {
  if (
    evidence.templateVersion !== AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION_V1 &&
    !isStructuredAgentControlVerificationPromptVersion(evidence.templateVersion)
  )
    return "templateVersion";
  const expected = buildExpectedAgentControlVerificationHandoff(
    authority,
    evidence.templateVersion,
  );
  for (const field of comparedFields) {
    if (evidence[field] !== expected[field]) return field;
  }
  return null;
};
