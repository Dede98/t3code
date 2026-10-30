import * as Deferred from "effect/Deferred";
import {
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it as effectIt } from "@effect/vitest";
import * as Layer from "effect/Layer";
import { describe, expect, it } from "vite-plus/test";
import * as Equal from "effect/Equal";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import {
  DEV_REPLAY_INSTANCE_ID,
  seedDevReplayInstance,
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
      providerInstances: {
        claude_work: {
          driver: "claudeAgent",
          config: {
            configDirPath: "~/.claude-work",
            crossAccountContinuationEnabled: !enabled,
          },
        },
        claude_empty: { driver: "claudeAgent" },
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
      } as unknown as (typeof ServerSettings.Encoded)["providerInstances"],
    }),
  );

const readInjectedGate = (config: unknown): unknown =>
  (config as { readonly crossAccountContinuationEnabled?: unknown })
    .crossAccountContinuationEnabled;

describe("deriveProviderInstanceConfigMap Claude continuation gate", () => {
  it("injects the global value into implicit and every explicit Claude instance", () => {
    const disabled = deriveWithGate(false);
    const enabled = deriveWithGate(true);
    const claudeIds = [
      ProviderInstanceId.make("claudeAgent"),
      ProviderInstanceId.make("claude_work"),
      ProviderInstanceId.make("claude_personal"),
      ProviderInstanceId.make("claude_empty"),
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
      const initial = decodeServerSettings({
        providerInstances: { codex: { driver: "codex", config: { binaryPath: "/old" } } },
      });
      const first = decodeServerSettings({
        providerInstances: { codex: { driver: "codex", config: { binaryPath: "/intermediate" } } },
      });
      const latest = decodeServerSettings({
        providerInstances: { codex: { driver: "codex", config: { binaryPath: "/latest" } } },
      });
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

it.each([false, true])(
  "rebuilds only MCP-selected instances without embedding secrets (explicit empty config: %s)",
  (explicit) => {
    const settings = (token: string) =>
      decodeServerSettings({
        ...(explicit
          ? { providerInstances: { [ProviderInstanceId.make("codex")]: { driver: "codex" } } }
          : {}),
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
  },
);
import * as ServerConfig from "../config.ts";
import { ServerSettingsService } from "../serverSettings.ts";

const projectId = ProjectId.make("project-a");
const instanceId = ProviderInstanceId.make("codex_work");

function settingsWith(overrides: Partial<ServerSettings>): ServerSettings {
  return { ...DEFAULT_SERVER_SETTINGS, ...overrides };
}

describe("deriveProviderInstanceConfigMap", () => {
  it("turns a machine-disabled instance on when a project enables it", () => {
    const settings = settingsWith({
      providerInstances: {
        [instanceId]: { driver: ProviderDriverKind.make("codex"), enabled: false },
      },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: true } } },
      },
    });

    const merged = deriveProviderInstanceConfigMap(settings);

    expect(merged[instanceId]).toMatchObject({
      driver: ProviderDriverKind.make("codex"),
      enabled: true,
    });
  });

  it("clears a nested config.enabled:false flag when a project enables the instance", () => {
    const settings = settingsWith({
      providerInstances: {
        [instanceId]: {
          driver: ProviderDriverKind.make("codex"),
          config: { enabled: false },
        },
      },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: true } } },
      },
    });

    const merged = deriveProviderInstanceConfigMap(settings);

    expect(merged[instanceId]).toMatchObject({
      driver: ProviderDriverKind.make("codex"),
      enabled: true,
      config: { enabled: true },
    });
  });

  it("does not revive an instance a project override references but settings no longer configure", () => {
    const settings = settingsWith({
      providerInstances: {},
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: true } } },
      },
    });

    const merged = deriveProviderInstanceConfigMap(settings);

    expect(merged[instanceId]).toBeUndefined();
  });

  it("ignores a project override that disables an instance, leaving the machine's config in charge", () => {
    const settings = settingsWith({
      providerInstances: {
        [instanceId]: { driver: ProviderDriverKind.make("codex"), enabled: true },
      },
      projectSettingsOverrides: {
        [projectId]: { providerInstances: { [instanceId]: { enabled: false } } },
      },
    });

    const merged = deriveProviderInstanceConfigMap(settings);

    expect(merged[instanceId]).toMatchObject(settings.providerInstances[instanceId]!);
  });
});

describe("seedDevReplayInstance", () => {
  const seed = (devUrl: URL | undefined) =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.ServerConfig;
      yield* seedDevReplayInstance().pipe(
        Effect.provideService(ServerConfig.ServerConfig, { ...config, devUrl }),
      );
      const settings = yield* ServerSettingsService;
      return (yield* settings.getSettings).providerInstances[DEV_REPLAY_INSTANCE_ID];
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          ServerConfig.layerTest(process.cwd(), { prefix: "t3-dev-replay-seed-" }),
          ServerSettingsService.layerTest({
            providerInstances: {
              [DEV_REPLAY_INSTANCE_ID]: {
                driver: ProviderDriverKind.make("acpRegistry"),
                displayName: "My replay",
                enabled: false,
                config: { source: "local", commandPath: "/old/node" },
              },
            },
          }),
        ).pipe(Layer.provideMerge(NodeServices.layer)),
      ),
    );

  effectIt.effect("leaves settings alone outside dev", () =>
    seed(undefined).pipe(
      Effect.map((instance) =>
        expect(instance?.config).toEqual({ source: "local", commandPath: "/old/node" }),
      ),
    ),
  );

  effectIt.effect("points a dev server at the replay agent and keeps user edits", () =>
    seed(new URL("http://localhost:5733")).pipe(
      Effect.map((instance) => {
        expect(instance?.displayName).toBe("My replay");
        expect(instance?.enabled).toBe(false);
        expect(instance?.config).toMatchObject({
          source: "local",
          commandArgs: [expect.stringMatching(/scripts[/\\]dev-replay-agent\.ts$/)],
        });
      }),
    ),
  );
});
