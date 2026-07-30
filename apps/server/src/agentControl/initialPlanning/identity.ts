import {
  CommandId,
  EventId,
  MessageId,
  type AgentControlControlledThreadReservationId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import { sha256AgentControlIdentity } from "../controlledThreadReservation/identity.ts";

export const deriveAgentControlInitialPlanningHandoffId = (
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
) =>
  Effect.sync(
    () =>
      `initial-planning-handoff-${sha256AgentControlIdentity([
        "agent-control-initial-planning-handoff-v1",
        controlledThreadReservationId,
        threadId,
      ])}`,
  );

export const deriveAgentControlInitialPlanningTurnRequestCommandId = (handoffId: string) =>
  Effect.sync(() =>
    CommandId.make(
      `initial-planning-turn-${sha256AgentControlIdentity([
        "agent-control-initial-planning-turn-request-v1",
        handoffId,
      ])}`,
    ),
  );

export const deriveAgentControlInitialPlanningMessageId = (handoffId: string) =>
  Effect.sync(() =>
    MessageId.make(
      `initial-planning-message-${sha256AgentControlIdentity([
        "agent-control-initial-planning-message-v1",
        handoffId,
      ])}`,
    ),
  );

export const deriveAgentControlInitialPlanningMessageEventId = (turnRequestCommandId: CommandId) =>
  Effect.sync(() =>
    EventId.make(
      `initial-planning-message-event-${sha256AgentControlIdentity([
        "agent-control-initial-planning-message-event-v1",
        turnRequestCommandId,
      ])}`,
    ),
  );

export const deriveAgentControlInitialPlanningTurnRequestEventId = (
  turnRequestCommandId: CommandId,
) =>
  Effect.sync(() =>
    EventId.make(
      `initial-planning-turn-event-${sha256AgentControlIdentity([
        "agent-control-initial-planning-turn-event-v1",
        turnRequestCommandId,
      ])}`,
    ),
  );

export const deriveAgentControlInitialPlanningProviderDeliveryId = (handoffId: string) =>
  Effect.sync(
    () =>
      `initial-planning-delivery-${sha256AgentControlIdentity([
        "agent-control-initial-planning-provider-delivery-v1",
        handoffId,
      ])}`,
  );

export interface AgentControlInitialPlanningFingerprintInput {
  readonly handoffId: string;
  readonly coordinatorCommandId: string;
  readonly coordinatorCommandFingerprint: string;
  readonly materializationCommandId: string;
  readonly materializationCommandFingerprint: string;
  readonly projectId: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly roleId: string;
  readonly stageKind: string;
  readonly stageOrdinal: number;
  readonly attemptOrdinal: number;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly worktreeReservationId: string;
  readonly worktreePath: string;
  readonly planningRole: "planner";
  readonly providerInstanceId: string;
  readonly runtimeMode: string;
  readonly modelSelectionJson: string;
  readonly templateVersion: string;
  readonly promptText: string;
  readonly turnRequestCommandId: string;
  readonly messageId: string;
  readonly messageEventId: string;
  readonly turnRequestEventId: string;
  readonly providerDeliveryId: string;
}

export const fingerprintAgentControlInitialPlanningHandoff = (
  input: AgentControlInitialPlanningFingerprintInput,
) =>
  sha256AgentControlIdentity([
    "agent-control-initial-planning-handoff-fingerprint-v1",
    input.handoffId,
    input.coordinatorCommandId,
    input.coordinatorCommandFingerprint,
    input.materializationCommandId,
    input.materializationCommandFingerprint,
    input.projectId,
    input.controlledThreadReservationId,
    input.threadId,
    input.taskId,
    String(input.taskRevision),
    String(input.githubIntakeSequence),
    input.sourceIdentityFingerprint,
    input.stageRunId,
    input.attemptId,
    input.roleId,
    input.stageKind,
    String(input.stageOrdinal),
    String(input.attemptOrdinal),
    input.leaseId,
    input.leaseHolderId,
    String(input.fenceToken),
    input.worktreeReservationId,
    input.worktreePath,
    input.planningRole,
    input.providerInstanceId,
    input.runtimeMode,
    input.modelSelectionJson,
    input.templateVersion,
    input.promptText,
    input.turnRequestCommandId,
    input.messageId,
    input.messageEventId,
    input.turnRequestEventId,
    input.providerDeliveryId,
  ]);
