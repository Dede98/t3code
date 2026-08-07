/**
 * OrchestrationReactor - Composite orchestration reactor service interface.
 *
 * Coordinates startup of orchestration runtime reactors that translate domain
 * events into downstream side effects.
 *
 * @module OrchestrationReactor
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type { ReactorStartupActivation } from "../../reactorStartupActivation.ts";

export class OrchestrationReactorStartupError extends Schema.TaggedErrorClass<OrchestrationReactorStartupError>()(
  "OrchestrationReactorStartupError",
  {
    reason: Schema.Literals([
      "already-started-different-scope",
      "commit-before-start",
      "lifecycle-closed",
    ]),
  },
) {}

/**
 * OrchestrationReactorShape - Service API for orchestration reactor lifecycle.
 */
export interface OrchestrationReactorShape {
  /**
   * Start orchestration-side reactors for provider/runtime/checkpoint flows.
   *
   * The returned effect must be run in a scope so all worker fibers can be
   * finalized on shutdown.
   */
  readonly start: (
    activation?: ReactorStartupActivation,
  ) => Effect.Effect<void, OrchestrationReactorStartupError, Scope.Scope>;

  /**
   * Commit a fully prepared server startup and release provider publication.
   * This is idempotent in the same lifecycle and must run only after every
   * server reactor participating in readiness has started successfully.
   */
  readonly commit: () => Effect.Effect<void, OrchestrationReactorStartupError, Scope.Scope>;
}

/**
 * OrchestrationReactor - Service tag for orchestration reactor coordination.
 */
export class OrchestrationReactor extends Context.Service<
  OrchestrationReactor,
  OrchestrationReactorShape
>()("t3/orchestration/Services/OrchestrationReactor") {}
