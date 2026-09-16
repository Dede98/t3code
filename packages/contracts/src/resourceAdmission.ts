import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";

import { IsoDateTime, NonNegativeInt, PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";

export const DEFAULT_RESOURCE_ADMISSION_PROVIDER_MAX_CONCURRENT = 4;
export const DEFAULT_RESOURCE_ADMISSION_INTERACTIVE_RESERVE = 1;
export const DEFAULT_RESOURCE_ADMISSION_BACKGROUND_AGING_SECONDS = 30;
export const DEFAULT_RESOURCE_ADMISSION_BACKGROUND_GRANT_INTERVAL = 4;
export const DEFAULT_RESOURCE_ADMISSION_LOCAL_CHECK_MAX_CONCURRENT = 1;
export const DEFAULT_RESOURCE_ADMISSION_CPU_PAUSE_THRESHOLD = 0.85;
export const DEFAULT_RESOURCE_ADMISSION_CPU_RESUME_THRESHOLD = 0.7;
export const DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_PAUSE_BYTES = 1.5 * 1024 ** 3;
export const DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_RESUME_BYTES = 2 * 1024 ** 3;
export const DEFAULT_RESOURCE_ADMISSION_GPU_MAX_CONCURRENT = 0;
export const DEFAULT_RESOURCE_ADMISSION_MISSING_TELEMETRY_POLICY = "defer-background" as const;

export const ResourceAdmissionMissingTelemetryPolicy = Schema.Literals([
  "defer-background",
  "allow",
]);
export type ResourceAdmissionMissingTelemetryPolicy =
  typeof ResourceAdmissionMissingTelemetryPolicy.Type;

const Capacity = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 64 }));
const Reserve = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 64 }));
const AgingSeconds = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3_600 }));
const GrantInterval = Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }));
const PressureThreshold = Schema.Finite.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

/** Host-owned admission policy. Measurement thresholds pause starts; they are not OS quotas. */
export const ResourceAdmissionSettings = Schema.Struct({
  providerMaxConcurrent: Capacity.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_PROVIDER_MAX_CONCURRENT)),
  ),
  interactiveReserve: Reserve.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_INTERACTIVE_RESERVE)),
  ),
  backgroundAgingSeconds: AgingSeconds.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_BACKGROUND_AGING_SECONDS)),
  ),
  backgroundGrantInterval: GrantInterval.pipe(
    Schema.withDecodingDefault(
      Effect.succeed(DEFAULT_RESOURCE_ADMISSION_BACKGROUND_GRANT_INTERVAL),
    ),
  ),
  localCheckMaxConcurrent: Capacity.pipe(
    Schema.withDecodingDefault(
      Effect.succeed(DEFAULT_RESOURCE_ADMISSION_LOCAL_CHECK_MAX_CONCURRENT),
    ),
  ),
  cpuPauseThreshold: PressureThreshold.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_CPU_PAUSE_THRESHOLD)),
  ),
  cpuResumeThreshold: PressureThreshold.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_CPU_RESUME_THRESHOLD)),
  ),
  availableMemoryPauseBytes: PositiveInt.pipe(
    Schema.withDecodingDefault(
      Effect.succeed(DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_PAUSE_BYTES),
    ),
  ),
  availableMemoryResumeBytes: PositiveInt.pipe(
    Schema.withDecodingDefault(
      Effect.succeed(DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_RESUME_BYTES),
    ),
  ),
  gpuMaxConcurrent: NonNegativeInt.check(Schema.isLessThanOrEqualTo(64)).pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_GPU_MAX_CONCURRENT)),
  ),
  missingTelemetryPolicy: ResourceAdmissionMissingTelemetryPolicy.pipe(
    Schema.withDecodingDefault(Effect.succeed(DEFAULT_RESOURCE_ADMISSION_MISSING_TELEMETRY_POLICY)),
  ),
  /** Instances mapped to the same value share one provider-account budget. */
  providerAccountScopes: Schema.Record(ProviderInstanceId, TrimmedNonEmptyString).pipe(
    Schema.withDecodingDefault(Effect.succeed({})),
  ),
}).check(
  Schema.makeFilter(
    (settings) =>
      settings.interactiveReserve <= settings.providerMaxConcurrent ||
      new SchemaIssue.InvalidValue({
        message: "interactiveReserve must not exceed providerMaxConcurrent",
      }),
    { identifier: "ResourceAdmissionInteractiveReserve" },
  ),
  Schema.makeFilter(
    (settings) =>
      settings.cpuResumeThreshold < settings.cpuPauseThreshold ||
      new SchemaIssue.InvalidValue({
        message: "cpuResumeThreshold must be lower than cpuPauseThreshold",
      }),
    { identifier: "ResourceAdmissionCpuHysteresis" },
  ),
  Schema.makeFilter(
    (settings) =>
      settings.availableMemoryResumeBytes > settings.availableMemoryPauseBytes ||
      new SchemaIssue.InvalidValue({
        message: "availableMemoryResumeBytes must exceed availableMemoryPauseBytes",
      }),
    { identifier: "ResourceAdmissionMemoryHysteresis" },
  ),
);
export type ResourceAdmissionSettings = typeof ResourceAdmissionSettings.Type;

export const ResourceAdmissionSettingsPatch = Schema.Struct({
  providerMaxConcurrent: Schema.optionalKey(Capacity),
  interactiveReserve: Schema.optionalKey(Reserve),
  backgroundAgingSeconds: Schema.optionalKey(AgingSeconds),
  backgroundGrantInterval: Schema.optionalKey(GrantInterval),
  localCheckMaxConcurrent: Schema.optionalKey(Capacity),
  cpuPauseThreshold: Schema.optionalKey(PressureThreshold),
  cpuResumeThreshold: Schema.optionalKey(PressureThreshold),
  availableMemoryPauseBytes: Schema.optionalKey(PositiveInt),
  availableMemoryResumeBytes: Schema.optionalKey(PositiveInt),
  gpuMaxConcurrent: Schema.optionalKey(NonNegativeInt.check(Schema.isLessThanOrEqualTo(64))),
  missingTelemetryPolicy: Schema.optionalKey(ResourceAdmissionMissingTelemetryPolicy),
  providerAccountScopes: Schema.optionalKey(
    Schema.Record(ProviderInstanceId, TrimmedNonEmptyString),
  ),
});
export type ResourceAdmissionSettingsPatch = typeof ResourceAdmissionSettingsPatch.Type;

export const RESOURCE_ADMISSION_WAIT_REASONS = [
  "provider-limit",
  "local-capacity",
  "cpu-pressure",
  "ram-pressure",
  "gpu-pressure",
  "interactive-priority",
  "telemetry-unavailable",
  "unsupported-requirement",
] as const;

export const ResourceAdmissionWaitReason = Schema.Literals(RESOURCE_ADMISSION_WAIT_REASONS);
export type ResourceAdmissionWaitReason = typeof ResourceAdmissionWaitReason.Type;

/**
 * A host-authored explanation for work that has not started. `detail` may
 * report observed values but must not imply an enforced CPU/RAM/GPU quota.
 */
export const ResourceAdmissionWait = Schema.Struct({
  reason: ResourceAdmissionWaitReason,
  hostId: Schema.optionalKey(TrimmedNonEmptyString),
  observedAt: Schema.optionalKey(IsoDateTime),
  detail: Schema.optionalKey(TrimmedNonEmptyString),
});
export type ResourceAdmissionWait = typeof ResourceAdmissionWait.Type;
