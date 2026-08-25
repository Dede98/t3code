import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlVerificationEvaluationError extends Schema.TaggedErrorClass<AgentControlVerificationEvaluationError>()(
  "AgentControlVerificationEvaluationError",
  {
    handoffId: Schema.optional(Schema.String),
    operation: Schema.String,
    reason: Schema.Literals([
      "authority-conflict",
      "history-corrupt",
      "evaluation-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type AgentControlVerificationEvaluatorResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Evaluated"; readonly evaluationId: string }
  | { readonly _tag: "Replayed"; readonly evaluationId: string };

export interface AgentControlVerificationEvaluatorShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlVerificationEvaluatorResult,
    AgentControlVerificationEvaluationError
  >;
  readonly recover: Effect.Effect<void, AgentControlVerificationEvaluationError>;
  readonly prepare: (activation: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void, AgentControlVerificationEvaluationError>;
}

export class AgentControlVerificationEvaluator extends Context.Service<
  AgentControlVerificationEvaluator,
  AgentControlVerificationEvaluatorShape
>()("t3/agentControl/verificationTurn/Services/AgentControlVerificationEvaluator") {}
