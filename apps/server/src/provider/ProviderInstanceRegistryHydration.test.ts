import * as Deferred from "effect/Deferred";
import {
  ProviderInstanceId,
  ServerSettings,
  type ServerSettings as ServerSettingsType,
} from "@t3tools/contracts";
import { it as effectIt } from "@effect/vitest";
import { describe, expect, it } from "vite-plus/test";
import * as Equal from "effect/Equal";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import {
  type DesiredProviderRegistrySettings,
  deriveProviderInstanceConfigMap,
  providerInstanceIdsRequiringSettle,
  runProviderRegistryReconcileWorker,
} from "./ProviderInstanceRegistryHydration.ts";

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

effectIt.effect(
  "coalesces changes and closes only affected idle V2 instances under the rebuild barrier",
  () =>
    Effect.gen(function* () {
      const initial = decodeServerSettings({ providers: { codex: { binaryPath: "/old" } } });
      const first = decodeServerSettings({ providers: { codex: { binaryPath: "/intermediate" } } });
      const latest = decodeServerSettings({ providers: { codex: { binaryPath: "/latest" } } });
      const desired = yield* Ref.make<DesiredProviderRegistrySettings>({
        settings: first,
        version: 1,
      });
      const checked = yield* Deferred.make<void>();
      const settled = yield* Deferred.make<void>();
      const applied = yield* Deferred.make<void>();
      const closeIds: string[][] = [];
      let exclusive = false;
      yield* runProviderRegistryReconcileWorker({
        desired,
        initialAppliedVersion: 0,
        initialAppliedConfigMap: deriveProviderInstanceConfigMap(initial),
        lifecycle: {
          register: () => Effect.void,
          canRebuild: (ids) =>
            Effect.gen(function* () {
              expect([...ids]).toEqual([ProviderInstanceId.make("codex")]);
              yield* Deferred.succeed(checked, undefined);
              yield* Deferred.await(settled);
              return true;
            }),
          closeInstances: (ids) =>
            Effect.sync(() => {
              expect(exclusive).toBe(true);
              closeIds.push([...ids]);
            }),
        },
        rebuildBarrier: {
          withRebuild: (effect) =>
            Effect.sync(() => {
              exclusive = true;
            }).pipe(
              Effect.andThen(effect),
              Effect.ensuring(
                Effect.sync(() => {
                  exclusive = false;
                }),
              ),
            ),
        },
        mutator: {
          reconcile: (map) =>
            Effect.gen(function* () {
              expect(exclusive).toBe(true);
              expect(map).toEqual(deriveProviderInstanceConfigMap(latest));
              yield* Deferred.succeed(applied, undefined);
            }),
        },
      }).pipe(Effect.forkScoped);
      yield* Deferred.await(checked);
      expect(closeIds).toEqual([]);
      yield* Ref.set(desired, { settings: latest, version: 2 });
      yield* Deferred.succeed(settled, undefined);
      yield* Deferred.await(applied);
      expect(closeIds).toEqual([["codex"]]);
    }),
);

it("rebuilds only instances selected by changed external MCP configuration without embedding secrets", () => {
  const settings = (token: string) =>
    decodeServerSettings({
      externalMcpServers: {
        remote: {
          enabled: true,
          url: "https://remote.example/mcp",
          providerInstances: ["codex"],
          headers: [{ name: "Authorization", value: token, sensitive: true }],
        },
      },
    });
  const before = deriveProviderInstanceConfigMap(settings("before-secret"));
  const after = deriveProviderInstanceConfigMap(settings("after-secret"));
  expect([...providerInstanceIdsRequiringSettle(before, after)]).toEqual(["codex"]);
  expect(JSON.stringify(after)).not.toContain("after-secret");
});
