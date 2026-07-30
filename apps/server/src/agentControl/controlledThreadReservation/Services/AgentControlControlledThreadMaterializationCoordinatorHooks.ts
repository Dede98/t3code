import type {
  AgentControlControlledThreadReservationId,
  CommandId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface AgentControlControlledThreadMaterializationCoordinatorObservation {
  readonly coordinatorCommandId: CommandId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId | null;
}

export interface AgentControlControlledThreadMaterializationCoordinatorHooksShape {
  readonly afterReceiptFirst: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterAuthoritativeResolution: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly beforeTransactionAdmission: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterMaterializingProjection: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterOrchestrationMaterialization: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterBoundProjection: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterCoordinatorEvidence: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterInitialPlanningIntent: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterInitialPlanningReceipt: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterInitialPlanningAccepted: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterInitialPlanningDelivery: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly beforeAcceptedMarker: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterOuterCommit: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly beforeReservationFinalization: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterReservationFinalization: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly beforeOrchestrationFinalization: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterOrchestrationFinalization: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
  readonly afterPublication: (
    observation: AgentControlControlledThreadMaterializationCoordinatorObservation,
  ) => Effect.Effect<void>;
}

export class AgentControlControlledThreadMaterializationCoordinatorHooks extends Context.Service<
  AgentControlControlledThreadMaterializationCoordinatorHooks,
  AgentControlControlledThreadMaterializationCoordinatorHooksShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinatorHooks",
) {}

const noop = () => Effect.void;

export const AgentControlControlledThreadMaterializationCoordinatorHooksNoop = Layer.succeed(
  AgentControlControlledThreadMaterializationCoordinatorHooks,
  AgentControlControlledThreadMaterializationCoordinatorHooks.of({
    afterReceiptFirst: noop,
    afterAuthoritativeResolution: noop,
    beforeTransactionAdmission: noop,
    afterMaterializingProjection: noop,
    afterOrchestrationMaterialization: noop,
    afterBoundProjection: noop,
    afterCoordinatorEvidence: noop,
    afterInitialPlanningIntent: noop,
    afterInitialPlanningReceipt: noop,
    afterInitialPlanningAccepted: noop,
    afterInitialPlanningDelivery: noop,
    beforeAcceptedMarker: noop,
    afterOuterCommit: noop,
    beforeReservationFinalization: noop,
    afterReservationFinalization: noop,
    beforeOrchestrationFinalization: noop,
    afterOrchestrationFinalization: noop,
    afterPublication: noop,
  }),
);
