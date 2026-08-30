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
  /** Test seam immediately before the durable publication claim CAS. */
  readonly beforePublicationClaim?: (handoffId: string) => Effect.Effect<void>;
  /** Test seam after the claim commit and immediately before its fence revalidation CAS. */
  readonly beforePublicationFenceValidation?: (handoffId: string) => Effect.Effect<void>;
  /** Test seam after PubSub observed the event and before the completion CAS. */
  readonly afterPublicationBeforeCompletion?: (handoffId: string) => Effect.Effect<void>;
  /** Test seam between a failed transient publication attempt and its one bounded retry. */
  readonly afterPublicationAttemptFailure?: (
    handoffId: string,
    operation: string,
  ) => Effect.Effect<void>;
  readonly afterPublication: (handoffId: string) => Effect.Effect<void>;
  /** Stable owner seam for independent-connection publication-fence tests. */
  readonly publicationOwnerId?: string;
  readonly publicationLeaseDurationMillis?: number;
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
        beforePublicationClaim: () => Effect.void,
        beforePublicationFenceValidation: () => Effect.void,
        afterPublicationBeforeCompletion: () => Effect.void,
        afterPublicationAttemptFailure: () => Effect.void,
        afterPublication: () => Effect.void,
      }),
    },
  );
