import { type AgentControlTaskId, type AgentControlTaskState, ProjectId } from "@t3tools/contracts";
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
  "task-missing",
  "task-project-mismatch",
  "task-status-inactive",
  "task-source-ineligible",
  "task-stage-inactive",
  "task-sequence-mismatch",
  "task-source-mismatch",
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
  readonly watermarkCompleted: boolean;
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
   * Transaction-bound future-consumer gate. The task is loaded canonically by
   * id, and `use` runs before the same SQLite transaction is committed. A
   * caller must perform its claim/write in `use`; the returned value is not a
   * reusable authorization token.
   */
  readonly useTaskConsumable: <A, E, R>(
    projectId: ProjectId,
    taskId: AgentControlTaskId,
    use: (task: AgentControlTaskState, gate: AgentControlTaskProjectGate) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, AgentControlTaskConsumerGuardError | E, R>;
}

export class AgentControlTaskConsumerGuard extends Context.Service<
  AgentControlTaskConsumerGuard,
  AgentControlTaskConsumerGuardShape
>()("t3/agentControl/task/Services/AgentControlTaskConsumerGuard") {}
