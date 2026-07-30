import type {
  AgentControlControlledThreadReservationId,
  AgentControlTaskId,
  CommandId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface AgentControlControlledThreadActivationObservation {
  readonly prepareCommandId: CommandId;
  readonly activationCommandId: CommandId;
  readonly projectId: ProjectId;
  readonly taskId: AgentControlTaskId;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
}

export interface AgentControlControlledThreadActivationHooksShape {
  readonly afterPrepareAcceptedBeforeMaterialize: (
    observation: AgentControlControlledThreadActivationObservation,
  ) => Effect.Effect<void>;
  readonly afterMaterializationAcceptedBeforeReturn: (
    observation: AgentControlControlledThreadActivationObservation,
  ) => Effect.Effect<void>;
}

export class AgentControlControlledThreadActivationHooks extends Context.Service<
  AgentControlControlledThreadActivationHooks,
  AgentControlControlledThreadActivationHooksShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadActivationHooks",
) {}

const noop = () => Effect.void;

export const AgentControlControlledThreadActivationHooksNoop = Layer.succeed(
  AgentControlControlledThreadActivationHooks,
  AgentControlControlledThreadActivationHooks.of({
    afterPrepareAcceptedBeforeMaterialize: noop,
    afterMaterializationAcceptedBeforeReturn: noop,
  }),
);
