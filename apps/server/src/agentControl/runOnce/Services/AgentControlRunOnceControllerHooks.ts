import type { AgentControlRunOnceId, AgentControlRunOnceStep, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlRunOnceObservation {
  readonly projectId: ProjectId;
  readonly runId: AgentControlRunOnceId | null;
  readonly ordinal: number | null;
  readonly step: AgentControlRunOnceStep | null;
}

export interface AgentControlRunOnceControllerHooksShape {
  readonly afterSubscriptionsBeforeRecovery: Effect.Effect<void>;
  readonly afterActivationAuthority: (
    observation: AgentControlRunOnceObservation,
  ) => Effect.Effect<void>;
  readonly afterStepCommitted: (observation: AgentControlRunOnceObservation) => Effect.Effect<void>;
  readonly beforePublication: (observation: AgentControlRunOnceObservation) => Effect.Effect<void>;
  readonly afterPublication: (observation: AgentControlRunOnceObservation) => Effect.Effect<void>;
}

const noop = () => Effect.void;

export const AgentControlRunOnceControllerHooks =
  Context.Reference<AgentControlRunOnceControllerHooksShape>(
    "t3/agentControl/runOnce/Services/AgentControlRunOnceControllerHooks",
    {
      defaultValue: () => ({
        afterSubscriptionsBeforeRecovery: Effect.void,
        afterActivationAuthority: noop,
        afterStepCommitted: noop,
        beforePublication: noop,
        afterPublication: noop,
      }),
    },
  );
