import type { AgentControlWorktreeReservationId, CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlWorktreeControllerHooksShape {
  readonly afterCompositeClaim?: (commandId: CommandId) => Effect.Effect<void>;
  readonly afterReadyInspection: (
    reservationId: AgentControlWorktreeReservationId,
  ) => Effect.Effect<void>;
  readonly beforeCompositeAccept?: (commandId: CommandId) => Effect.Effect<void>;
}

export const AgentControlWorktreeControllerHooks =
  Context.Reference<AgentControlWorktreeControllerHooksShape>(
    "t3/agentControl/worktree/Services/AgentControlWorktreeControllerHooks",
    {
      defaultValue: () => ({
        afterCompositeClaim: () => Effect.void,
        afterReadyInspection: () => Effect.void,
        beforeCompositeAccept: () => Effect.void,
      }),
    },
  );
