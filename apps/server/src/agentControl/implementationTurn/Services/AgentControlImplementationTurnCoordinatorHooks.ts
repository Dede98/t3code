import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface AgentControlImplementationTurnObservation {
  readonly handoffId: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
}

export interface AgentControlImplementationTurnCoordinatorHooksShape {
  readonly afterAdmissionReplay: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterMaterializingProjection: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterOrchestrationMaterialization: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterBoundProjection: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterHandoffAccepted: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly beforeMaterializationMarker: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterOuterCommit: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlImplementationTurnObservation,
  ) => Effect.Effect<void>;
}

export class AgentControlImplementationTurnCoordinatorHooks extends Context.Service<
  AgentControlImplementationTurnCoordinatorHooks,
  AgentControlImplementationTurnCoordinatorHooksShape
>()("t3/agentControl/implementationTurn/Services/AgentControlImplementationTurnCoordinatorHooks") {}

const noop: AgentControlImplementationTurnCoordinatorHooksShape = {
  afterAdmissionReplay: () => Effect.void,
  afterMaterializingProjection: () => Effect.void,
  afterOrchestrationMaterialization: () => Effect.void,
  afterBoundProjection: () => Effect.void,
  afterHandoffAccepted: () => Effect.void,
  beforeMaterializationMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};

export const AgentControlImplementationTurnCoordinatorHooksNoop = Layer.succeed(
  AgentControlImplementationTurnCoordinatorHooks,
  AgentControlImplementationTurnCoordinatorHooks.of(noop),
);
