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
>()("t3/provider/Services/ProviderRegistrySessionLifecycle") {
  static readonly layer = Layer.sync(ProviderRegistrySessionLifecycle, () => {
    let hooks: SessionLifecycle | undefined;
    return {
      register: (next) =>
        Effect.sync(() => {
          hooks = next;
        }),
      canRebuild: (ids) => Effect.suspend(() => hooks?.canRebuild(ids) ?? Effect.succeed(true)),
      closeInstances: (ids) => Effect.suspend(() => hooks?.closeInstances(ids) ?? Effect.void),
    };
  });
}
