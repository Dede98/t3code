import { ProjectId, type AgentControlTaskState } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const AgentControlTaskConsumerGuardReason = Schema.Literals([
  "project-unavailable",
  "mode-inactive",
  "source-snapshot-unavailable",
  "watermark-missing",
  "watermark-not-completed",
  "watermark-sequence-mismatch",
  "task-sequence-mismatch",
  "task-projection-corrupt",
  "internal-persistence-error",
]);
export type AgentControlTaskConsumerGuardReason = typeof AgentControlTaskConsumerGuardReason.Type;

export class AgentControlTaskConsumerGuardError extends Schema.TaggedErrorClass<AgentControlTaskConsumerGuardError>()(
  "AgentControlTaskConsumerGuardError",
  {
    projectId: ProjectId,
    reason: AgentControlTaskConsumerGuardReason,
  },
) {}

export interface AgentControlTaskProjectGate {
  readonly projectId: ProjectId;
  readonly activation: "inactive" | "waiting-source" | "observe";
  readonly currentSourceSequence: number | null;
  readonly targetSequence: number | null;
  readonly lastCompletedSequence: number | null;
  readonly sequenceCurrent: boolean;
  readonly sourceFingerprint: string | null;
  readonly reason: AgentControlTaskConsumerGuardReason | null;
}

export interface AgentControlTaskConsumerGuardShape {
  /**
   * Reads every canonical input needed by the intake and future execution
   * boundary. No caller-provided mode, source sequence, or watermark is trusted.
   */
  readonly inspectProject: (
    projectId: ProjectId,
  ) => Effect.Effect<AgentControlTaskProjectGate, AgentControlTaskConsumerGuardError>;
  /**
   * Mandatory future-consumer gate. Passing a task additionally proves that the
   * task itself was projected from the current completed GitHub sequence.
   */
  readonly ensureCurrent: (
    projectId: ProjectId,
    task?: AgentControlTaskState,
  ) => Effect.Effect<AgentControlTaskProjectGate, AgentControlTaskConsumerGuardError>;
}

export class AgentControlTaskConsumerGuard extends Context.Service<
  AgentControlTaskConsumerGuard,
  AgentControlTaskConsumerGuardShape
>()("t3/agentControl/task/Services/AgentControlTaskConsumerGuard") {}
