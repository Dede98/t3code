import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlImplementationStageStarterError extends Schema.TaggedErrorClass<AgentControlImplementationStageStarterError>()(
  "AgentControlImplementationStageStarterError",
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

export type AgentControlImplementationStageStarterResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Started"; readonly stageEventSequence: number }
  | { readonly _tag: "Replayed"; readonly stageEventSequence: number };

export interface AgentControlImplementationStageStarterShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlImplementationStageStarterResult,
    AgentControlImplementationStageStarterError
  >;
  readonly recover: Effect.Effect<void, AgentControlImplementationStageStarterError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
}

export const AgentControlImplementationStageStarter =
  Context.Reference<AgentControlImplementationStageStarterShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationStageStarter",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
      }),
    },
  );
