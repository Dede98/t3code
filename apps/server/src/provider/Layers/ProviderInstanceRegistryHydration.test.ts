import {
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  type ServerSettings as ServerSettingsType,
} from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";
import * as Equal from "effect/Equal";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import {
  type DesiredProviderRegistrySettings,
  deriveProviderInstanceConfigMap,
  isProviderSessionBusyForRegistryRebuild,
  runProviderRegistryReconcileWorker,
  waitForProviderSessionsToSettle,
} from "./ProviderInstanceRegistryHydration.ts";
import type { ProviderInstance } from "../ProviderDriver.ts";
import type { ProviderInstanceRegistryShape } from "../Services/ProviderInstanceRegistry.ts";

const decodeServerSettings = Schema.decodeSync(ServerSettings);

const deriveWithGate = (enabled: boolean) =>
  deriveProviderInstanceConfigMap(
    decodeServerSettings({
      claudeCrossAccountContinuationEnabled: enabled,
      providers: {
        claudeAgent: {
          configDirPath: "~/.claude-default",
        },
      },
      providerInstances: {
        claude_work: {
          driver: "claudeAgent",
          config: {
            configDirPath: "~/.claude-work",
            crossAccountContinuationEnabled: !enabled,
          },
        },
        claude_personal: {
          driver: "claudeAgent",
          config: {
            configDirPath: "~/.claude-personal",
          },
        },
        codex_work: {
          driver: "codex",
          config: {
            homePath: "~/.codex-work",
          },
        },
      } as unknown as ServerSettingsType["providerInstances"],
    }),
  );

const readInjectedGate = (config: unknown): unknown =>
  (config as { readonly crossAccountContinuationEnabled?: unknown })
    .crossAccountContinuationEnabled;

describe("deriveProviderInstanceConfigMap Claude continuation gate", () => {
  it("delays registry rebuilds only while provider sessions are starting or running", () => {
    expect(isProviderSessionBusyForRegistryRebuild("connecting")).toBe(true);
    expect(isProviderSessionBusyForRegistryRebuild("running")).toBe(true);
    expect(isProviderSessionBusyForRegistryRebuild("ready")).toBe(false);
    expect(isProviderSessionBusyForRegistryRebuild("closed")).toBe(false);
    expect(isProviderSessionBusyForRegistryRebuild("error")).toBe(false);
  });

  it("injects the global value into legacy and every explicit Claude instance", () => {
    const disabled = deriveWithGate(false);
    const enabled = deriveWithGate(true);
    const claudeIds = [
      ProviderInstanceId.make("claudeAgent"),
      ProviderInstanceId.make("claude_work"),
      ProviderInstanceId.make("claude_personal"),
    ];

    for (const instanceId of claudeIds) {
      expect(readInjectedGate(disabled[instanceId]?.config)).toBe(false);
      expect(readInjectedGate(enabled[instanceId]?.config)).toBe(true);
    }
  });

  it("changes Claude entry equality without rebuilding unrelated drivers", () => {
    const disabled = deriveWithGate(false);
    const enabled = deriveWithGate(true);

    expect(
      Equal.equals(
        disabled[ProviderInstanceId.make("claude_work")],
        enabled[ProviderInstanceId.make("claude_work")],
      ),
    ).toBe(false);
    expect(
      Equal.equals(
        disabled[ProviderInstanceId.make("claude_personal")],
        enabled[ProviderInstanceId.make("claude_personal")],
      ),
    ).toBe(false);
    expect(
      Equal.equals(
        disabled[ProviderInstanceId.make("codex_work")],
        enabled[ProviderInstanceId.make("codex_work")],
      ),
    ).toBe(true);
  });
});

const registryWithProviderSessionRead = (
  listSessions: ProviderInstance["adapter"]["listSessions"],
  driverKind: ProviderDriverKind = ProviderDriverKind.make("claudeAgent"),
): Pick<ProviderInstanceRegistryShape, "listInstances"> => ({
  listInstances: Effect.succeed([
    {
      driverKind,
      adapter: { listSessions },
    } as ProviderInstance,
  ]),
});

effectIt.effect("bounds a hung provider session status read", () =>
  Effect.gen(function* () {
    const registry = registryWithProviderSessionRead(() => Effect.never);
    const fiber = yield* waitForProviderSessionsToSettle(registry, {
      pollIntervalMs: 10,
      timeoutMs: 1_000,
    }).pipe(Effect.forkChild);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("1 second");

    expect(yield* Fiber.join(fiber)).toBe(false);
  }),
);

effectIt.effect("bounds repeatedly failing provider session status reads", () =>
  Effect.gen(function* () {
    const registry = registryWithProviderSessionRead(() =>
      Effect.die("simulated listSessions failure"),
    );
    const fiber = yield* waitForProviderSessionsToSettle(registry, {
      pollIntervalMs: 10,
      timeoutMs: 1_000,
    }).pipe(Effect.forkChild);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("1 second");

    expect(yield* Fiber.join(fiber)).toBe(false);
  }),
);

effectIt.effect("treats a running non-Claude provider session as busy", () =>
  Effect.gen(function* () {
    let activeTurn = true;
    const registry = registryWithProviderSessionRead(
      () =>
        activeTurn
          ? Effect.succeed([
              {
                provider: ProviderDriverKind.make("codex"),
                status: "running" as const,
                runtimeMode: "full-access" as const,
                threadId: "thread-active-codex-config-change" as never,
                createdAt: "2026-07-17T00:00:00.000Z",
                updatedAt: "2026-07-17T00:00:00.000Z",
              },
            ])
          : Effect.succeed([]),
      ProviderDriverKind.make("codex"),
    );
    const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
      settings: decodeServerSettings({ providers: { codex: { binaryPath: "/tmp/codex-next" } } }),
      version: 1,
    });
    let reconciles = 0;
    yield* runProviderRegistryReconcileWorker({
      desired,
      initialAppliedVersion: 0,
      registry,
      mutator: {
        reconcile: () =>
          Effect.sync(() => {
            reconciles += 1;
          }),
      },
      rebuildBarrier: { withRebuild: (effect) => effect },
      pollIntervalMs: 10,
      settleTimeoutMs: 50,
      exclusiveCheckTimeoutMs: 10,
    }).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("100 millis");
    expect(reconciles).toBe(0);

    activeTurn = false;
    yield* TestClock.adjust("100 millis");
    yield* Effect.yieldNow;
    expect(reconciles).toBe(1);
  }),
);

effectIt.effect("retries a defective provider instance listing", () =>
  Effect.gen(function* () {
    let listAttempts = 0;
    const registry: Pick<ProviderInstanceRegistryShape, "listInstances"> = {
      listInstances: Effect.suspend(() => {
        listAttempts += 1;
        return listAttempts === 1
          ? Effect.die("simulated registry listing defect")
          : Effect.succeed([]);
      }),
    };
    const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
      settings: decodeServerSettings({}),
      version: 1,
    });
    let reconciles = 0;
    yield* runProviderRegistryReconcileWorker({
      desired,
      initialAppliedVersion: 0,
      registry,
      mutator: {
        reconcile: () =>
          Effect.sync(() => {
            reconciles += 1;
          }),
      },
      rebuildBarrier: { withRebuild: (effect) => effect },
      pollIntervalMs: 10,
      settleTimeoutMs: 100,
      exclusiveCheckTimeoutMs: 10,
    }).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("100 millis");
    yield* Effect.yieldNow;

    expect(listAttempts).toBeGreaterThanOrEqual(2);
    expect(reconciles).toBe(1);
  }),
);

effectIt.effect("retries reconciliation after a defect and applies the same desired version", () =>
  Effect.gen(function* () {
    const registry: Pick<ProviderInstanceRegistryShape, "listInstances"> = {
      listInstances: Effect.succeed([]),
    };
    const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
      settings: decodeServerSettings({ providers: { codex: { binaryPath: "/tmp/retry" } } }),
      version: 1,
    });
    let reconcileAttempts = 0;
    let successfulReconciles = 0;
    yield* runProviderRegistryReconcileWorker({
      desired,
      initialAppliedVersion: 0,
      registry,
      mutator: {
        reconcile: () =>
          Effect.suspend(() => {
            reconcileAttempts += 1;
            if (reconcileAttempts === 1) {
              return Effect.die("simulated reconcile defect");
            }
            successfulReconciles += 1;
            return Effect.void;
          }),
      },
      rebuildBarrier: { withRebuild: (effect) => effect },
      pollIntervalMs: 10,
      settleTimeoutMs: 100,
      exclusiveCheckTimeoutMs: 10,
    }).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("100 millis");
    yield* Effect.yieldNow;

    expect(reconcileAttempts).toBe(2);
    expect(successfulReconciles).toBe(1);
  }),
);

effectIt.effect("defers and coalesces normal provider config changes during an active turn", () =>
  Effect.gen(function* () {
    let activeTurn = true;
    const registry = registryWithProviderSessionRead(() =>
      activeTurn
        ? Effect.succeed([
            {
              provider: ProviderDriverKind.make("claudeAgent"),
              status: "running" as const,
              runtimeMode: "full-access" as const,
              threadId: "thread-active-config-change" as never,
              createdAt: "2026-07-17T00:00:00.000Z",
              updatedAt: "2026-07-17T00:00:00.000Z",
            },
          ])
        : Effect.succeed([]),
    );
    const first = decodeServerSettings({
      providers: { codex: { binaryPath: "/tmp/codex-first" } },
    });
    const latest = decodeServerSettings({
      providers: { codex: { binaryPath: "/tmp/codex-latest" } },
    });
    const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
      settings: first,
      version: 1,
    });
    const reconciled: Array<ReturnType<typeof deriveProviderInstanceConfigMap>> = [];
    yield* runProviderRegistryReconcileWorker({
      desired,
      initialAppliedVersion: 0,
      registry,
      mutator: {
        reconcile: (config) =>
          Effect.sync(() => {
            reconciled.push(config);
          }),
      },
      rebuildBarrier: { withRebuild: (effect) => effect },
      pollIntervalMs: 10,
      settleTimeoutMs: 100,
      exclusiveCheckTimeoutMs: 10,
    }).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* TestClock.adjust("200 millis");
    expect(reconciled).toHaveLength(0);

    yield* Ref.set(desired, { settings: latest, version: 2 });
    activeTurn = false;
    yield* TestClock.adjust("200 millis");
    yield* Effect.yieldNow;

    expect(reconciled).toHaveLength(1);
    expect(
      (
        reconciled[0]?.[ProviderInstanceId.make("codex")]?.config as
          | { binaryPath?: string }
          | undefined
      )?.binaryPath,
    ).toBe("/tmp/codex-latest");
  }),
);
