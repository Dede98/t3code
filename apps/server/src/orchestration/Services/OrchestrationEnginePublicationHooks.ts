import type { EventId, OrchestrationEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

/**
 * Opaque identity allocated by the layer wiring an OrchestrationEngine.
 * Tests use object identity to bind publications to the concrete engine that
 * crossed the production PubSub boundary; production uses the no-op default.
 */
export interface OrchestrationEnginePublicationSource {
  readonly name: string;
  readonly identity: object;
}

export interface OrchestrationEnginePublicationHooksShape {
  readonly source: OrchestrationEnginePublicationSource;
  /** Test-only deterministic event-id boundary; live code uses Crypto. */
  readonly nextEventId?: () => Effect.Effect<EventId>;
  readonly onPublish: (input: {
    readonly source: OrchestrationEnginePublicationSource;
    readonly event: OrchestrationEvent;
  }) => Effect.Effect<void>;
}

const productionSource: OrchestrationEnginePublicationSource = {
  name: "production",
  identity: {},
};

export const OrchestrationEnginePublicationHooks =
  Context.Reference<OrchestrationEnginePublicationHooksShape>(
    "t3/orchestration/Services/OrchestrationEnginePublicationHooks",
    {
      defaultValue: () => ({
        source: productionSource,
        onPublish: () => Effect.void,
      }),
    },
  );
