import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlImplementationStageFinalizerHooksShape {
  readonly afterAuthoritativeEvidence: (handoffId: string) => Effect.Effect<void>;
  readonly beforeAppend: (handoffId: string) => Effect.Effect<void>;
  readonly beforeFinalMarker: (handoffId: string) => Effect.Effect<void>;
  readonly afterOuterCommit: (handoffId: string) => Effect.Effect<void>;
  readonly afterPublication: (handoffId: string) => Effect.Effect<void>;
}

export const AgentControlImplementationStageFinalizerHooks =
  Context.Reference<AgentControlImplementationStageFinalizerHooksShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationStageFinalizerHooks",
    {
      defaultValue: () => ({
        afterAuthoritativeEvidence: () => Effect.void,
        beforeAppend: () => Effect.void,
        beforeFinalMarker: () => Effect.void,
        afterOuterCommit: () => Effect.void,
        afterPublication: () => Effect.void,
      }),
    },
  );
