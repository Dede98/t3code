import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlImplementationStageStarterHooksShape {
  readonly afterProviderEvidence: (handoffId: string) => Effect.Effect<void>;
  readonly afterStageProjection: (handoffId: string) => Effect.Effect<void>;
  readonly beforeFinalMarker: (handoffId: string) => Effect.Effect<void>;
  readonly afterOuterCommit: (handoffId: string) => Effect.Effect<void>;
  readonly afterPublication: (handoffId: string) => Effect.Effect<void>;
}

export const AgentControlImplementationStageStarterHooks =
  Context.Reference<AgentControlImplementationStageStarterHooksShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationStageStarterHooks",
    {
      defaultValue: () => ({
        afterProviderEvidence: () => Effect.void,
        afterStageProjection: () => Effect.void,
        beforeFinalMarker: () => Effect.void,
        afterOuterCommit: () => Effect.void,
        afterPublication: () => Effect.void,
      }),
    },
  );
