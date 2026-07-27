import type { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlThreadMaterializationTransactionObservation {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly projectExists: boolean;
  readonly threadExists: boolean;
  readonly receiptExists: boolean;
  readonly intentExists: boolean;
  readonly createdEventSequence: number | null;
  readonly bindingEventSequence: number | null;
}

export interface AgentControlThreadMaterializationTransactionHooksShape {
  readonly afterAuthoritativeRead: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly beforeFirstEventAppend: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly afterFirstEventAppend: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly afterSecondEventAppend: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly afterProjection: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly afterReceiptInsert: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly afterIntentInsert: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
  readonly beforeTransactionComplete: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
}

const noop = (_observation: AgentControlThreadMaterializationTransactionObservation) => Effect.void;

/**
 * Production-bound coordination seam used only to prove the real
 * read/append/commit ordering. The production default is a cached no-op.
 */
export const AgentControlThreadMaterializationTransactionHooks =
  Context.Reference<AgentControlThreadMaterializationTransactionHooksShape>(
    "t3/orchestration/Services/AgentControlThreadMaterializationTransactionHooks",
    {
      defaultValue: () => ({
        afterAuthoritativeRead: noop,
        beforeFirstEventAppend: noop,
        afterFirstEventAppend: noop,
        afterSecondEventAppend: noop,
        afterProjection: noop,
        afterReceiptInsert: noop,
        afterIntentInsert: noop,
        beforeTransactionComplete: noop,
      }),
    },
  );
