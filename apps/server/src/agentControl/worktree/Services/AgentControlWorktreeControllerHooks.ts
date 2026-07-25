import type { AgentControlWorktreeReservationId, CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export type AgentControlWorktreeLifecycleCheckpoint =
  | "after-preflight"
  | "after-reserved"
  | "after-target-acquired"
  | "before-target-cleanup"
  | "after-target-remove-before-claim-delete"
  | "after-target-cleanup"
  | "after-materializing"
  | "after-git-call"
  | "after-git-created"
  | "after-marker-publish"
  | "after-ownership-marked"
  | "before-ready";

export interface AgentControlWorktreeControllerHooksShape {
  readonly afterCompositeClaim?: (commandId: CommandId) => Effect.Effect<void>;
  readonly afterLifecycleCheckpoint?: (
    checkpoint: AgentControlWorktreeLifecycleCheckpoint,
    commandId: CommandId,
    reservationId: AgentControlWorktreeReservationId | null,
  ) => Effect.Effect<void>;
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
        afterLifecycleCheckpoint: () => Effect.void,
        afterReadyInspection: () => Effect.void,
        beforeCompositeAccept: () => Effect.void,
      }),
    },
  );
