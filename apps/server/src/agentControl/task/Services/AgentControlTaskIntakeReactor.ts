import type {
  AgentControlTaskReactorStatus,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

export class AgentControlTaskIntakeStartupError extends Schema.TaggedErrorClass<AgentControlTaskIntakeStartupError>()(
  "AgentControlTaskIntakeStartupError",
  {
    reason: Schema.Literals([
      "subscription-activation-failed",
      "enumeration-failed",
      "queue-barrier-failed",
    ]),
  },
) {}

export interface AgentControlTaskIntakeReactorShape {
  readonly start: () => Effect.Effect<void, AgentControlTaskIntakeStartupError, Scope.Scope>;
  readonly getStatus: (
    input: AgentControlTaskReconcileOnceInput,
  ) => Effect.Effect<AgentControlTaskReactorStatus, AgentControlTaskRpcError>;
  /** Volatile wakeups only; Armed reconstructs authority from SQLite after subscribing. */
  readonly subscribeCompletions?: Effect.Effect<Stream.Stream<ProjectId>, never, Scope.Scope>;
}

export class AgentControlTaskIntakeReactor extends Context.Service<
  AgentControlTaskIntakeReactor,
  AgentControlTaskIntakeReactorShape
>()("t3/agentControl/task/Services/AgentControlTaskIntakeReactor") {}
