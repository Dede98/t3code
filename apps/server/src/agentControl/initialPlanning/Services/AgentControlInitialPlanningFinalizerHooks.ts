import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlInitialPlanningFinalizerObservation {
  readonly handoffId: string;
  readonly stageRunId: string;
  readonly leaseId: string;
  readonly deliveryRevision: number;
  readonly stageRevision: number;
  readonly leaseRevision: number;
}

export interface AgentControlInitialPlanningFinalizerHooksShape {
  readonly afterAuthoritativeRead: (
    observation: AgentControlInitialPlanningFinalizerObservation,
  ) => Effect.Effect<void>;
  readonly beforeTransactionComplete: (
    observation: AgentControlInitialPlanningFinalizerObservation,
  ) => Effect.Effect<void>;
  readonly afterNativeCommit: (
    observation: AgentControlInitialPlanningFinalizerObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlInitialPlanningFinalizerObservation,
  ) => Effect.Effect<void>;
}

const noop = () => Effect.void;

export const AgentControlInitialPlanningFinalizerHooks =
  Context.Reference<AgentControlInitialPlanningFinalizerHooksShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningFinalizerHooks",
    {
      defaultValue: () => ({
        afterAuthoritativeRead: noop,
        beforeTransactionComplete: noop,
        afterNativeCommit: noop,
        afterPublication: noop,
      }),
    },
  );
