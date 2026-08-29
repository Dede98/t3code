import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlVerificationStageFinalizerHooksShape {
  readonly beforeTransaction: (handoffId: string) => Effect.Effect<void>;
  readonly afterAuthoritativeRead: (handoffId: string) => Effect.Effect<void>;
  readonly afterStageProjection: (handoffId: string) => Effect.Effect<void>;
  readonly afterLeaseProjection: (handoffId: string) => Effect.Effect<void>;
  readonly afterEvidence: (handoffId: string) => Effect.Effect<void>;
  readonly afterReceipt: (handoffId: string) => Effect.Effect<void>;
  readonly beforeMarker: (handoffId: string) => Effect.Effect<void>;
  readonly afterCommit: (handoffId: string) => Effect.Effect<void>;
  readonly afterPublication: (handoffId: string) => Effect.Effect<void>;
  readonly recoveryPageSize?: number;
}

export const AgentControlVerificationStageFinalizerHooks =
  Context.Reference<AgentControlVerificationStageFinalizerHooksShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationStageFinalizerHooks",
    {
      defaultValue: () => ({
        beforeTransaction: () => Effect.void,
        afterAuthoritativeRead: () => Effect.void,
        afterStageProjection: () => Effect.void,
        afterLeaseProjection: () => Effect.void,
        afterEvidence: () => Effect.void,
        afterReceipt: () => Effect.void,
        beforeMarker: () => Effect.void,
        afterCommit: () => Effect.void,
        afterPublication: () => Effect.void,
      }),
    },
  );
