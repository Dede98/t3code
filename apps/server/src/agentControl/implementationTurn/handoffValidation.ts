import type { ModelSelection, ProjectId, ThreadId } from "@t3tools/contracts";

import {
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
} from "../initialPlanning/eventEvidence.ts";
import { implementationMessagePayload, implementationTurnRequestPayload } from "./eventEvidence.ts";
import {
  deriveImplementationHandoffId,
  deriveImplementationMessageEventId,
  deriveImplementationMessageId,
  deriveImplementationProviderDeliveryId,
  deriveImplementationTurnRequestCommandId,
  deriveImplementationTurnRequestEventId,
  fingerprintImplementationHandoff,
} from "./identity.ts";
import type { AgentControlImplementationHandoffEvidence } from "./model.ts";
import {
  AGENT_CONTROL_IMPLEMENTATION_PROMPT_TEMPLATE_VERSION,
  buildAgentControlImplementationPrompt,
} from "./prompt.ts";

export interface AgentControlImplementationHandoffAuthority {
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
  readonly controlledThreadReservationId: AgentControlImplementationHandoffEvidence["controlledThreadReservationId"];
  readonly threadId: ThreadId;
  readonly planningThreadId: ThreadId;
  readonly planId: string;
  readonly proposedPlanJson: string;
  readonly proposedPlanDigest: string;
  readonly repositoryDisplay: string;
  readonly sourceRevision: string;
  readonly taskTitle: string;
  readonly taskBody: string | null;
  readonly providerInstanceId: AgentControlImplementationHandoffEvidence["providerInstanceId"];
  readonly runtimeMode: AgentControlImplementationHandoffEvidence["runtimeMode"];
  readonly modelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly createdAt: string;
}

export const buildExpectedAgentControlImplementationHandoff = (
  authority: AgentControlImplementationHandoffAuthority,
): AgentControlImplementationHandoffEvidence => {
  const handoffId = deriveImplementationHandoffId(authority.materializationEvidenceId);
  const turnRequestCommandId = deriveImplementationTurnRequestCommandId(handoffId);
  const messageId = deriveImplementationMessageId(handoffId);
  const messageEventId = deriveImplementationMessageEventId(turnRequestCommandId);
  const turnRequestEventId = deriveImplementationTurnRequestEventId(turnRequestCommandId);
  const providerDeliveryId = deriveImplementationProviderDeliveryId(handoffId);
  const prompt = buildAgentControlImplementationPrompt({
    repositoryDisplay: authority.repositoryDisplay,
    taskId: authority.taskId,
    taskTitle: authority.taskTitle,
    taskBody: authority.taskBody,
    sourceRevision: authority.sourceRevision,
    planningThreadId: authority.planningThreadId,
    planId: authority.planId,
    proposedPlanJson: authority.proposedPlanJson,
    proposedPlanDigest: authority.proposedPlanDigest,
  });
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
    payload: implementationMessagePayload({
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
    payload: implementationTurnRequestPayload({
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
    templateVersion: AGENT_CONTROL_IMPLEMENTATION_PROMPT_TEMPLATE_VERSION,
    promptText: prompt.promptText,
    promptDigest: prompt.promptDigest,
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
  } satisfies AgentControlImplementationHandoffEvidence;
  return {
    ...base,
    handoffFingerprint: fingerprintImplementationHandoff(base),
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
  "promptText",
  "promptDigest",
  "turnRequestCommandId",
  "messageId",
  "messageEventId",
  "turnRequestEventId",
  "messageEventTemplateJson",
  "turnRequestEventTemplateJson",
  "eventTemplateDigest",
  "providerDeliveryId",
  "createdAt",
] as const satisfies ReadonlyArray<keyof AgentControlImplementationHandoffEvidence>;

export const implementationHandoffAuthorityMismatch = (
  authority: AgentControlImplementationHandoffAuthority,
  evidence: AgentControlImplementationHandoffEvidence,
): string | null => {
  const expected = buildExpectedAgentControlImplementationHandoff(authority);
  for (const field of comparedFields) {
    if (evidence[field] !== expected[field]) return field;
  }
  return null;
};
