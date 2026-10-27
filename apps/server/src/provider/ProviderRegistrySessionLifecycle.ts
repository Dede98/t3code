import type { ProviderInstanceId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

interface SessionLifecycle {
  readonly canRebuild: (ids: ReadonlySet<ProviderInstanceId>) => Effect.Effect<boolean>;
  readonly closeInstances: (ids: ReadonlySet<ProviderInstanceId>) => Effect.Effect<void>;
}

/** The V2 manager registers its lifecycle after registry construction, avoiding a layer cycle. */
export class ProviderRegistrySessionLifecycle extends Context.Service<
  ProviderRegistrySessionLifecycle,
  SessionLifecycle & {
    readonly register: (hooks: SessionLifecycle) => Effect.Effect<void>;
  }
>()("t3/provider/ProviderRegistrySessionLifecycle") {
  static readonly layer = Layer.sync(ProviderRegistrySessionLifecycle, () => {
    let hooks: SessionLifecycle | undefined;
    return {
      register: Effect.fn("ProviderRegistrySessionLifecycle.register")(function (next) {
        return Effect.sync(() => {
          hooks = next;
        });
      }),
      canRebuild: Effect.fn("ProviderRegistrySessionLifecycle.canRebuild")(function (ids) {
        return Effect.suspend(() => hooks?.canRebuild(ids) ?? Effect.succeed(true));
      }),
      closeInstances: Effect.fn("ProviderRegistrySessionLifecycle.closeInstances")(function (ids) {
        return Effect.suspend(() => hooks?.closeInstances(ids) ?? Effect.void);
      }),
    };
  });
}
