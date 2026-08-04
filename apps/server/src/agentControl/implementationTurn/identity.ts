import { CommandId, EventId, MessageId } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

import type { AgentControlImplementationHandoffEvidence } from "./model.ts";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const fingerprintImplementationTurn = (
  domain: string,
  parts: ReadonlyArray<string>,
): string =>
  NodeCrypto.createHash("sha256")
    .update(frame([`agent-control-implementation-turn-${domain}-v1`, ...parts]), "utf8")
    .digest("hex");

const identity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${fingerprintImplementationTurn(domain, parts)}`;

export const deriveImplementationMaterializationEvidenceId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("implementation-materialization-evidence", "materialization-evidence", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveImplementationMaterializationReceiptId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("implementation-materialization-receipt", "materialization-receipt", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveImplementationMaterializationMarkerId = (
  admissionEvidenceId: string,
  controlledThreadReservationId: string,
) =>
  identity("implementation-materialization-marker", "materialization-marker", [
    admissionEvidenceId,
    controlledThreadReservationId,
  ]);

export const deriveImplementationHandoffId = (materializationEvidenceId: string) =>
  identity("implementation-handoff", "handoff", [materializationEvidenceId]);

export const deriveImplementationTurnRequestCommandId = (handoffId: string) =>
  CommandId.make(identity("implementation-turn", "turn-request-command", [handoffId]));

export const deriveImplementationMessageId = (handoffId: string) =>
  MessageId.make(identity("implementation-message", "message", [handoffId]));

export const deriveImplementationMessageEventId = (turnRequestCommandId: CommandId) =>
  EventId.make(identity("implementation-message-event", "message-event", [turnRequestCommandId]));

export const deriveImplementationTurnRequestEventId = (turnRequestCommandId: CommandId) =>
  EventId.make(identity("implementation-turn-event", "turn-request-event", [turnRequestCommandId]));

export const deriveImplementationProviderDeliveryId = (handoffId: string) =>
  identity("implementation-delivery", "provider-delivery", [handoffId]);

export const fingerprintImplementationHandoff = (
  input: AgentControlImplementationHandoffEvidence,
) =>
  fingerprintImplementationTurn("handoff-fingerprint", [
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
    input.stageRunId,
    input.attemptId,
    input.leaseId,
    input.leaseHolderId,
    String(input.fenceToken),
    input.worktreeReservationId,
    String(input.worktreeRevision),
    String(input.worktreeEventSequence),
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

export const deriveImplementationStageStartCommandId = (
  providerDeliveryId: string,
  providerTurnId: string,
) =>
  CommandId.make(
    identity("implementation-stage-start", "start-command", [providerDeliveryId, providerTurnId]),
  );

export const deriveImplementationStageStartEventId = (startCommandId: CommandId) =>
  EventId.make(identity("implementation-stage-start-event", "start-event", [startCommandId]));

export const deriveImplementationStageStartEvidenceId = (startCommandId: CommandId) =>
  identity("implementation-stage-start-evidence", "start-evidence", [startCommandId]);

export const deriveImplementationStageStartReceiptId = (startCommandId: CommandId) =>
  identity("implementation-stage-start-receipt", "start-receipt", [startCommandId]);

export const deriveImplementationStageStartMarkerId = (startCommandId: CommandId) =>
  identity("implementation-stage-start-marker", "start-marker", [startCommandId]);
