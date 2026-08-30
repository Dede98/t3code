import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlTaskVerificationFinalizerHooksShape {
  readonly beforeTransaction: (handoffId: string) => Effect.Effect<void>;
  readonly afterAuthoritativeRead: (handoffId: string) => Effect.Effect<void>;
  readonly afterTaskProjection: (handoffId: string) => Effect.Effect<void>;
  readonly afterEvidence: (handoffId: string) => Effect.Effect<void>;
  readonly afterReceipt: (handoffId: string) => Effect.Effect<void>;
  readonly beforeMarker: (handoffId: string) => Effect.Effect<void>;
  readonly afterCommit: (handoffId: string) => Effect.Effect<void>;
  readonly afterPublication: (handoffId: string) => Effect.Effect<void>;
  readonly recoveryPageSize?: number;
}

export const AgentControlTaskVerificationFinalizerHooks =
  Context.Reference<AgentControlTaskVerificationFinalizerHooksShape>(
    "t3/agentControl/task/Services/AgentControlTaskVerificationFinalizerHooks",
    {
      defaultValue: () => ({
        beforeTransaction: () => Effect.void,
        afterAuthoritativeRead: () => Effect.void,
        afterTaskProjection: () => Effect.void,
        afterEvidence: () => Effect.void,
        afterReceipt: () => Effect.void,
        beforeMarker: () => Effect.void,
        afterCommit: () => Effect.void,
        afterPublication: () => Effect.void,
      }),
    },
  );
