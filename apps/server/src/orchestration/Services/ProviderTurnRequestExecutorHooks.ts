import type { CommandId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

export interface ProviderTurnRequestExecutorHooksShape {
  /** Test-only deterministic command-id boundary; live code uses Crypto. */
  readonly makeServerCommandId?: (tag: string) => Effect.Effect<CommandId>;
}

export const ProviderTurnRequestExecutorHooks =
  Context.Reference<ProviderTurnRequestExecutorHooksShape>(
    "t3/orchestration/Services/ProviderTurnRequestExecutorHooks",
    { defaultValue: () => ({}) },
  );
