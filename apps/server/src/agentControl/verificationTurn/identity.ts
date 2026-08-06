import { CommandId, EventId, MessageId } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

import type { AgentControlVerificationHandoffEvidence } from "./model.ts";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const fingerprintVerificationTurn = (domain: string, parts: ReadonlyArray<string>): string =>
  NodeCrypto.createHash("sha256")
    .update(frame([`agent-control-verification-turn-${domain}-v1`, ...parts]), "utf8")
    .digest("hex");

const identity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${fingerprintVerificationTurn(domain, parts)}`;

export const deriveVerificationMaterializationEvidenceId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("verification-materialization-evidence", "materialization-evidence", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveVerificationMaterializationReceiptId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("verification-materialization-receipt", "materialization-receipt", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveVerificationMaterializationMarkerId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("verification-materialization-marker", "materialization-marker", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveVerificationHandoffId = (materializationEvidenceId: string) =>
  identity("verification-handoff", "handoff", [materializationEvidenceId]);

export const deriveVerificationTurnRequestCommandId = (handoffId: string) =>
  CommandId.make(identity("verification-turn", "turn-request-command", [handoffId]));

export const deriveVerificationMessageId = (handoffId: string) =>
  MessageId.make(identity("verification-message", "message", [handoffId]));

export const deriveVerificationMessageEventId = (turnRequestCommandId: CommandId) =>
  EventId.make(identity("verification-message-event", "message-event", [turnRequestCommandId]));

export const deriveVerificationTurnRequestEventId = (turnRequestCommandId: CommandId) =>
  EventId.make(identity("verification-turn-event", "turn-request-event", [turnRequestCommandId]));

export const deriveVerificationProviderDeliveryId = (handoffId: string) =>
  identity("verification-delivery", "provider-delivery", [handoffId]);

export const fingerprintVerificationHandoff = (input: AgentControlVerificationHandoffEvidence) =>
  fingerprintVerificationTurn("handoff-fingerprint", [
    input.handoffId,
    input.materializationEvidenceId,
    input.materializationReceiptId,
    input.materializationMarkerId,
    input.admissionEvidenceId,
    input.admissionReceiptId,
    input.admissionMarkerId,
    input.projectId,
    input.taskId,
    String(input.taskRevision),
    String(input.githubIntakeSequence),
    input.sourceIdentityFingerprint,
    input.taskSourceEventId,
    String(input.taskSourceEventSequence),
    String(input.taskSourceEventStreamVersion),
    input.stageRunId,
    input.attemptId,
    input.leaseId,
    input.leaseHolderId,
    String(input.fenceToken),
    input.worktreeReservationId,
    String(input.worktreeRevision),
    input.worktreeEventId,
    String(input.worktreeEventSequence),
    String(input.worktreeEventStreamVersion),
    input.worktreeOwnershipFingerprint,
    input.worktreeVerifiedAt,
    input.worktreePath,
    input.branch,
    input.controlledThreadReservationId,
    input.threadId,
    input.planningThreadId,
    input.planId,
    input.proposedPlanDigest,
    input.providerInstanceId,
    input.runtimeMode,
    input.modelSelectionJson,
    input.modelSelectionFingerprint,
    input.templateVersion,
    input.promptText,
    input.promptDigest,
    input.turnRequestCommandId,
    input.messageId,
    input.messageEventId,
    input.turnRequestEventId,
    input.messageEventTemplateJson,
    input.turnRequestEventTemplateJson,
    input.eventTemplateDigest,
    input.providerDeliveryId,
    input.createdAt,
  ]);

export const deriveVerificationStageStartCommandId = (
  providerDeliveryId: string,
  providerTurnId: string,
) =>
  CommandId.make(
    identity("verification-stage-start", "start-command", [providerDeliveryId, providerTurnId]),
  );

export const deriveVerificationStageStartEventId = (startCommandId: CommandId) =>
  EventId.make(identity("verification-stage-start-event", "start-event", [startCommandId]));

export const deriveVerificationStageStartEvidenceId = (startCommandId: CommandId) =>
  identity("verification-stage-start-evidence", "start-evidence", [startCommandId]);

export const deriveVerificationStageStartReceiptId = (startCommandId: CommandId) =>
  identity("verification-stage-start-receipt", "start-receipt", [startCommandId]);

export const deriveVerificationStageStartMarkerId = (startCommandId: CommandId) =>
  identity("verification-stage-start-marker", "start-marker", [startCommandId]);
