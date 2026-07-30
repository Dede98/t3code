import type { CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface ProviderCommandReactorHooksShape {
  readonly beforeInitialPlanningOwnershipRead: (commandId: CommandId) => Effect.Effect<void>;
  readonly afterInitialPlanningOwnershipRead: (
    commandId: CommandId,
    owned: boolean,
  ) => Effect.Effect<void>;
}

export const ProviderCommandReactorHooks = Context.Reference<ProviderCommandReactorHooksShape>(
  "@t3tools/server/ProviderCommandReactorHooks",
  {
    defaultValue: () => ({
      beforeInitialPlanningOwnershipRead: () => Effect.void,
      afterInitialPlanningOwnershipRead: () => Effect.void,
    }),
  },
);
