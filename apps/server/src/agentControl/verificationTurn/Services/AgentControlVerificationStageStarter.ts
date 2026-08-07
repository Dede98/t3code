import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlVerificationStageStarterError extends Schema.TaggedErrorClass<AgentControlVerificationStageStarterError>()(
  "AgentControlVerificationStageStarterError",
  {
    handoffId: Schema.String,
    operation: Schema.String,
    reason: Schema.Literals([
      "waiting",
      "identity-mismatch",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "revision-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type AgentControlVerificationStageStarterResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Started"; readonly stageEventSequence: number }
  | { readonly _tag: "Replayed"; readonly stageEventSequence: number };

export interface AgentControlVerificationStageStarterShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlVerificationStageStarterResult,
    AgentControlVerificationStageStarterError
  >;
  readonly recover: Effect.Effect<void, AgentControlVerificationStageStarterError>;
  /** Prepare one attempt-owned worker and subscriptions, parked behind activation. */
  readonly prepare: (activation: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
}

export const AgentControlVerificationStageStarter =
  Context.Reference<AgentControlVerificationStageStarterShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationStageStarter",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
        recover: Effect.void,
        prepare: () => Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
      }),
    },
  );
