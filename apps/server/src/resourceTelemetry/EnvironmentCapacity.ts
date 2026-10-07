import {
  isProviderNativeSubagentThread,
  type EnvironmentCapacityReport,
  type ProviderInstanceId,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as HostResources from "./HostResources.ts";

export class EnvironmentCapacityError extends Schema.TaggedError<EnvironmentCapacityError>()(
  "EnvironmentCapacityError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Could not read environment capacity.";
  }
}

export class EnvironmentCapacity extends Context.Service<
  EnvironmentCapacity,
  { readonly read: Effect.Effect<EnvironmentCapacityReport, EnvironmentCapacityError> }
>()("t3/resourceTelemetry/EnvironmentCapacity") {}

function emptyCounts() {
  return {
    idle: 0,
    preparing: 0,
    queued: 0,
    starting: 0,
    running: 0,
    waiting: 0,
    completed: 0,
    failed: 0,
    cancelled: 0,
    interrupted: 0,
    rolled_back: 0,
  };
}

function emptyWorkload() {
  return { threads: emptyCounts(), nativeSubagents: emptyCounts(), waitingForBackgroundThreads: 0 };
}

function freshness(sampledAt: number, now: number, maxAgeMs: number) {
  const ageMs = Math.max(0, now - sampledAt);
  return { ageMs, stale: sampledAt > now || ageMs > maxAgeMs };
}

const encodeQuotaIdentity = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

const quotaGroupId = Effect.fn("EnvironmentCapacity.quotaGroupId")(function* (
  provider: ServerProvider,
) {
  const email = provider.auth.email?.trim().toLowerCase();
  const identity = email
    ? [provider.driver, "email", email]
    : provider.usageLimits?.credentialFingerprint
      ? [provider.driver, "credential", provider.usageLimits.credentialFingerprint]
      : null;
  if (identity === null) return null;
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto
    .digest("SHA-256", new TextEncoder().encode(encodeQuotaIdentity(identity)))
    .pipe(Effect.mapError((cause) => new EnvironmentCapacityError({ cause })));
  return Hex.encode(digest);
});

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const environment = yield* ServerEnvironment.ServerEnvironment;
  const resources = yield* HostResources.HostResources;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const orchestrator = yield* Orchestrator.OrchestratorV2;

  const read = Effect.gen(function* () {
    const host = yield* resources.read;
    const descriptor = yield* environment.getDescriptor;
    const providers = yield* registry.getProviders;
    // Reads compact shell projections, never transcripts or provider processes.
    const snapshot = yield* orchestrator
      .getShellSnapshot({ location: "active" })
      .pipe(Effect.mapError((cause) => new EnvironmentCapacityError({ cause })));
    const now = yield* DateTime.now;
    const nowMs = DateTime.toEpochMillis(now);
    const totals = emptyWorkload();
    const byInstance = new Map<ProviderInstanceId, ReturnType<typeof emptyWorkload>>();
    for (const provider of providers) byInstance.set(provider.instanceId, emptyWorkload());
    for (const thread of snapshot.threads) {
      if (thread.deletedAt !== null || thread.archivedAt !== null) continue;
      let counts = byInstance.get(thread.providerInstanceId);
      if (!counts) {
        counts = emptyWorkload();
        byInstance.set(thread.providerInstanceId, counts);
      }
      const kind = isProviderNativeSubagentThread(thread) ? "nativeSubagents" : "threads";
      // A queued follow-up must not hide the run that is still consuming capacity.
      const status = thread.activityRunStatus ?? thread.status;
      counts[kind][status]++;
      totals[kind][status]++;
      if (kind === "threads" && (thread.pendingBackgroundTasks?.length ?? 0) > 0) {
        counts.waitingForBackgroundThreads++;
        totals.waitingForBackgroundThreads++;
      }
    }
    return {
      observedAt: DateTime.formatIso(now),
      environment: {
        environmentId: descriptor.environmentId,
        label: descriptor.label,
        platform: descriptor.platform,
      },
      host: { ...host, ...freshness(host.sampledAt, nowMs, 15_000) },
      workload: {
        ...totals,
        snapshotSequence: snapshot.snapshotSequence,
        byProviderInstance: [...byInstance]
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([providerInstanceId, counts]) => ({ providerInstanceId, ...counts })),
      },
      providers: yield* Effect.forEach(providers, (provider) =>
        Effect.gen(function* () {
          return {
            providerInstanceId: provider.instanceId,
            driver: provider.driver,
            displayName: provider.displayName ?? null,
            enabled: provider.enabled,
            installed: provider.installed,
            status: provider.status,
            authStatus: provider.auth.status,
            availability: provider.availability ?? "available",
            checkedAt: provider.checkedAt,
            ...freshness(Date.parse(provider.checkedAt), nowMs, 5 * 60_000),
            quotaGroupId: yield* quotaGroupId(provider),
            usageLimits: provider.usageLimits
              ? {
                  checkedAt: provider.usageLimits.checkedAt,
                  ...freshness(Date.parse(provider.usageLimits.checkedAt), nowMs, 5 * 60_000),
                  unavailableReason: provider.usageLimits.unavailable?.reason ?? null,
                  windows: provider.usageLimits.windows.map((window) => ({
                    id: window.id,
                    kind: window.kind,
                    label: window.label,
                    usedPercent: window.usedPercent,
                    remainingPercent: 100 - window.usedPercent,
                    ...(window.resetsAt === undefined ? {} : { resetsAt: window.resetsAt }),
                    ...(window.windowDurationMins === undefined
                      ? {}
                      : { windowDurationMins: window.windowDurationMins }),
                    resetPassed:
                      window.resetsAt !== undefined && Date.parse(window.resetsAt) <= nowMs,
                  })),
                }
              : null,
          };
        }),
      ),
    } satisfies EnvironmentCapacityReport;
  }).pipe(Effect.provideService(Crypto.Crypto, crypto));
  return EnvironmentCapacity.of({ read });
});

export const layer = Layer.effect(EnvironmentCapacity, make);
