import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_PAUSE_BYTES,
  DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_RESUME_BYTES,
  DEFAULT_RESOURCE_ADMISSION_BACKGROUND_AGING_SECONDS,
  DEFAULT_RESOURCE_ADMISSION_BACKGROUND_GRANT_INTERVAL,
  DEFAULT_RESOURCE_ADMISSION_CPU_PAUSE_THRESHOLD,
  DEFAULT_RESOURCE_ADMISSION_CPU_RESUME_THRESHOLD,
  DEFAULT_RESOURCE_ADMISSION_GPU_MAX_CONCURRENT,
  DEFAULT_RESOURCE_ADMISSION_INTERACTIVE_RESERVE,
  DEFAULT_RESOURCE_ADMISSION_LOCAL_CHECK_MAX_CONCURRENT,
  DEFAULT_RESOURCE_ADMISSION_MISSING_TELEMETRY_POLICY,
  DEFAULT_RESOURCE_ADMISSION_PROVIDER_MAX_CONCURRENT,
  RESOURCE_ADMISSION_WAIT_REASONS,
  ResourceAdmissionSettings,
  ResourceAdmissionSettingsPatch,
  ResourceAdmissionWait,
} from "./resourceAdmission.ts";

const decodeSettings = Schema.decodeUnknownSync(ResourceAdmissionSettings);
const decodePatch = Schema.decodeUnknownSync(ResourceAdmissionSettingsPatch);
const decodeWait = Schema.decodeUnknownSync(ResourceAdmissionWait);

describe("resource admission contracts", () => {
  it("uses conservative host defaults with hysteresis and no implicit GPU capacity", () => {
    expect(decodeSettings({})).toEqual({
      providerMaxConcurrent: DEFAULT_RESOURCE_ADMISSION_PROVIDER_MAX_CONCURRENT,
      interactiveReserve: DEFAULT_RESOURCE_ADMISSION_INTERACTIVE_RESERVE,
      backgroundAgingSeconds: DEFAULT_RESOURCE_ADMISSION_BACKGROUND_AGING_SECONDS,
      backgroundGrantInterval: DEFAULT_RESOURCE_ADMISSION_BACKGROUND_GRANT_INTERVAL,
      localCheckMaxConcurrent: DEFAULT_RESOURCE_ADMISSION_LOCAL_CHECK_MAX_CONCURRENT,
      cpuPauseThreshold: DEFAULT_RESOURCE_ADMISSION_CPU_PAUSE_THRESHOLD,
      cpuResumeThreshold: DEFAULT_RESOURCE_ADMISSION_CPU_RESUME_THRESHOLD,
      availableMemoryPauseBytes: DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_PAUSE_BYTES,
      availableMemoryResumeBytes: DEFAULT_RESOURCE_ADMISSION_AVAILABLE_MEMORY_RESUME_BYTES,
      gpuMaxConcurrent: DEFAULT_RESOURCE_ADMISSION_GPU_MAX_CONCURRENT,
      missingTelemetryPolicy: DEFAULT_RESOURCE_ADMISSION_MISSING_TELEMETRY_POLICY,
      providerAccountScopes: {},
    });
  });

  it("accepts partial settings patches and provider account grouping", () => {
    expect(
      decodePatch({
        providerMaxConcurrent: 6,
        localCheckMaxConcurrent: 2,
        missingTelemetryPolicy: "allow",
        providerAccountScopes: { "codex-work": "openai-team-a" },
      }),
    ).toEqual({
      providerMaxConcurrent: 6,
      localCheckMaxConcurrent: 2,
      missingTelemetryPolicy: "allow",
      providerAccountScopes: { "codex-work": "openai-team-a" },
    });
  });

  it("rejects settings that remove the configured hysteresis or interactive capacity", () => {
    expect(() => decodeSettings({ providerMaxConcurrent: 2, interactiveReserve: 3 })).toThrow();
    expect(() => decodeSettings({ cpuPauseThreshold: 0.7, cpuResumeThreshold: 0.7 })).toThrow();
    expect(() =>
      decodeSettings({
        availableMemoryPauseBytes: 2 * 1024 ** 3,
        availableMemoryResumeBytes: 1.5 * 1024 ** 3,
      }),
    ).toThrow();
  });

  it("decodes every closed wait reason with host-authored context", () => {
    for (const reason of RESOURCE_ADMISSION_WAIT_REASONS) {
      expect(
        decodeWait({
          reason,
          hostId: "builder-01",
          observedAt: "2026-09-16T10:00:00.000Z",
          detail: "Observed on the server host.",
        }),
      ).toEqual({
        reason,
        hostId: "builder-01",
        observedAt: "2026-09-16T10:00:00.000Z",
        detail: "Observed on the server host.",
      });
    }
    expect(() => decodeWait({ reason: "browser-busy" })).toThrow();
  });
});
