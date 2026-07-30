import {
  AgentControlControlledThreadReservationId,
  CommandId,
  ThreadId,
  type AgentControlAttemptId,
  type AgentControlRoleId,
  type AgentControlStageKind,
  type AgentControlStageRunId,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";

export const lengthFrameAgentControlIdentity = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const sha256AgentControlIdentity = (parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256")
    .update(lengthFrameAgentControlIdentity(parts), "utf8")
    .digest("hex");

export interface AgentControlControlledThreadStableIdentity {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly roleId: AgentControlRoleId;
  readonly stageKind: AgentControlStageKind;
  readonly stageOrdinal: number;
  readonly attemptOrdinal: number;
}

const stableParts = (input: AgentControlControlledThreadStableIdentity) => [
  input.projectId,
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
];

export const deriveAgentControlControlledThreadReservationId = (
  input: AgentControlControlledThreadStableIdentity,
) =>
  Effect.sync(() =>
    AgentControlControlledThreadReservationId.make(
      `controlled-thread-reservation-${sha256AgentControlIdentity([
        "agent-control-controlled-thread-reservation-v1",
        ...stableParts(input),
      ])}`,
    ),
  );

export const deriveAgentControlReservedThreadId = (
  input: AgentControlControlledThreadStableIdentity,
) =>
  Effect.sync(() =>
    ThreadId.make(
      `t3-auto-reserved-thread-${sha256AgentControlIdentity([
        "agent-control-controlled-thread-v1",
        ...stableParts(input),
      ])}`,
    ),
  );

export const deriveRejectedAgentControlControlledThreadReservationId = (input: {
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
}) =>
  Effect.sync(() =>
    AgentControlControlledThreadReservationId.make(
      `controlled-thread-reservation-rejected-${sha256AgentControlIdentity([
        "agent-control-controlled-thread-reservation-rejected-v1",
        input.projectId,
        input.taskId,
      ])}`,
    ),
  );

export const deriveAgentControlControlledThreadActivationCommandId = (
  prepareCommandId: CommandId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
) =>
  Effect.sync(() =>
    CommandId.make(
      `controlled-thread-activation-${sha256AgentControlIdentity([
        "agent-control-controlled-thread-activation-v1",
        prepareCommandId,
        controlledThreadReservationId,
      ])}`,
    ),
  );

const deriveMaterializationCommandId = (
  domain: string,
  coordinatorCommandId: CommandId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
) =>
  Effect.sync(() =>
    CommandId.make(
      `controlled-thread-materialization-${sha256AgentControlIdentity([
        domain,
        coordinatorCommandId,
        controlledThreadReservationId,
      ])}`,
    ),
  );

export const deriveAgentControlMaterializingTransitionCommandId = (
  coordinatorCommandId: CommandId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
) =>
  deriveMaterializationCommandId(
    "agent-control-controlled-thread-materializing-transition-v1",
    coordinatorCommandId,
    controlledThreadReservationId,
  );

export const deriveAgentControlThreadMaterializationCommandId = (
  coordinatorCommandId: CommandId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
) =>
  deriveMaterializationCommandId(
    "agent-control-controlled-thread-orchestration-materialization-v1",
    coordinatorCommandId,
    controlledThreadReservationId,
  );

export const deriveAgentControlBoundTransitionCommandId = (
  coordinatorCommandId: CommandId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
) =>
  deriveMaterializationCommandId(
    "agent-control-controlled-thread-bound-transition-v1",
    coordinatorCommandId,
    controlledThreadReservationId,
  );
