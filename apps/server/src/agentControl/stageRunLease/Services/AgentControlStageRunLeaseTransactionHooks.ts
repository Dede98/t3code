import type { AgentControlStageRunLeaseId, CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlStageRunLeaseTransactionObservation {
  readonly commandId: CommandId;
  readonly leaseId: AgentControlStageRunLeaseId;
  readonly streamVersion: number;
  readonly projectionRevision: number | null;
  readonly fenceToken: number | null;
}

export interface AgentControlStageRunLeaseTransactionHooksShape {
  readonly afterAuthoritativeRead: (
    observation: AgentControlStageRunLeaseTransactionObservation,
  ) => Effect.Effect<void>;
  readonly beforeAppend: (
    observation: AgentControlStageRunLeaseTransactionObservation,
  ) => Effect.Effect<void>;
  readonly beforeTransactionComplete: (
    observation: AgentControlStageRunLeaseTransactionObservation,
  ) => Effect.Effect<void>;
}

const noop = (_observation: AgentControlStageRunLeaseTransactionObservation) => Effect.void;

/**
 * Server-internal coordination seam for deterministic lease transaction tests.
 * The cached production default is strictly effectful no-op behavior.
 */
export const AgentControlStageRunLeaseTransactionHooks =
  Context.Reference<AgentControlStageRunLeaseTransactionHooksShape>(
    "t3/agentControl/stageRunLease/Services/AgentControlStageRunLeaseTransactionHooks",
    {
      defaultValue: () => ({
        afterAuthoritativeRead: noop,
        beforeAppend: noop,
        beforeTransactionComplete: noop,
      }),
    },
  );
