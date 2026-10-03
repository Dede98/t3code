import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt } from "./baseSchemas.ts";
import { ExecutionEnvironmentDescriptor } from "./environment.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ServerProviderUsageWindow } from "./providerUsageLimits.ts";
import { HostResourcesSnapshot } from "./resourceTelemetry.ts";
import { ServerProvider } from "./server.ts";

const ThreadCounts = Schema.Struct({
  idle: NonNegativeInt,
  preparing: NonNegativeInt,
  queued: NonNegativeInt,
  starting: NonNegativeInt,
  running: NonNegativeInt,
  waiting: NonNegativeInt,
  completed: NonNegativeInt,
  failed: NonNegativeInt,
  cancelled: NonNegativeInt,
  interrupted: NonNegativeInt,
  rolled_back: NonNegativeInt,
});

const WorkloadCounts = Schema.Struct({
  /** Non-archived T3 threads, including delegated children, by current activity status. */
  threads: ThreadCounts,
  /** Provider-owned child threads are separate, never added to threads. */
  nativeSubagents: ThreadCounts,
  /** A subset of threads with pending background work after their root run ended. */
  waitingForBackgroundThreads: NonNegativeInt,
});

/** Local observations, not reservations or a guarantee that a launch will succeed. */
export const EnvironmentCapacityReport = Schema.Struct({
  observedAt: IsoDateTime,
  environment: Schema.Struct({
    environmentId: ExecutionEnvironmentDescriptor.fields.environmentId,
    label: ExecutionEnvironmentDescriptor.fields.label,
    platform: ExecutionEnvironmentDescriptor.fields.platform,
  }),
  host: Schema.Struct({
    ...HostResourcesSnapshot.fields,
    ageMs: NonNegativeInt,
    stale: Schema.Boolean,
  }),
  workload: Schema.Struct({
    ...WorkloadCounts.fields,
    snapshotSequence: NonNegativeInt,
    byProviderInstance: Schema.Array(
      Schema.Struct({ providerInstanceId: ProviderInstanceId, ...WorkloadCounts.fields }),
    ),
  }),
  providers: Schema.Array(
    Schema.Struct({
      providerInstanceId: ProviderInstanceId,
      driver: ServerProvider.fields.driver,
      displayName: Schema.NullOr(Schema.String),
      enabled: Schema.Boolean,
      installed: Schema.Boolean,
      status: ServerProvider.fields.status,
      authStatus: ServerProvider.fields.auth.fields.status,
      availability: Schema.Literals(["available", "unavailable"]),
      checkedAt: IsoDateTime,
      ageMs: NonNegativeInt,
      stale: Schema.Boolean,
      /** Same non-null value means shared quota. Null means account identity is unknown. */
      quotaGroupId: Schema.NullOr(Schema.String),
      /** Null means no quota snapshot has been reported, never unlimited capacity. */
      usageLimits: Schema.NullOr(
        Schema.Struct({
          checkedAt: IsoDateTime,
          ageMs: NonNegativeInt,
          /** Snapshot age only; sparse provider events can leave individual windows older. */
          stale: Schema.Boolean,
          unavailableReason: Schema.NullOr(Schema.Literals(["unsupported", "probeFailed"])),
          windows: Schema.Array(
            Schema.Struct({
              ...ServerProviderUsageWindow.fields,
              remainingPercent: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 100 })),
              /** A reset time has passed; the cached percentage needs a fresh observation. */
              resetPassed: Schema.Boolean,
            }),
          ),
        }),
      ),
    }),
  ),
});
export type EnvironmentCapacityReport = typeof EnvironmentCapacityReport.Type;
