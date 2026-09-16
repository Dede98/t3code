import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as HostResources from "../resourceTelemetry/HostResources.ts";
import type { ResourcePressureSample } from "./model.ts";

export class ResourcePressure extends Context.Service<
  ResourcePressure,
  {
    readonly sample: Effect.Effect<ResourcePressureSample>;
    /** Resolves when a later pressure sample may differ; no polling contract is implied. */
    readonly awaitChange: (afterSampledAtMs: number) => Effect.Effect<void>;
  }
>()("t3/resourceAdmission/ResourcePressure") {}

export const make = Effect.fn("resourceAdmission.resourcePressure.make")(function* () {
  const hostResources = yield* HostResources.HostResources;
  return ResourcePressure.of({
    sample: hostResources.read.pipe(
      Effect.map((snapshot): ResourcePressureSample => ({
        sampledAtMs: snapshot.sampledAt,
        telemetry: "available",
        cpuUtilization: snapshot.cpuUtilization,
        availableMemoryBytes: snapshot.availableMemoryBytes,
        // HostResources intentionally makes no GPU-capacity claim.
        gpu: { status: "unavailable" },
      })),
      Effect.catchDefect(() =>
        Effect.succeed({
          sampledAtMs: 0,
          telemetry: "unavailable",
          cpuUtilization: null,
          availableMemoryBytes: null,
          gpu: { status: "unavailable" },
        } as const),
      ),
    ),
    // HostResources is demand-driven and cached, so a low-frequency wake-up
    // re-samples cheap OS counters without introducing process-tree scans.
    awaitChange: () => Effect.sleep("5 seconds"),
  });
});

export const layer = Layer.effect(ResourcePressure, make());
