import { ProviderRegistrySessionLifecycle } from "./ProviderRegistrySessionLifecycle.ts";
import { vi } from "vite-plus/test";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as CodexInstallation from "./CodexInstallation.ts";
import * as CodexManagedTokenBroker from "./CodexManagedTokenBroker.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, it, assert, expect } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import {
  EnvironmentId,
  ClaudeSettings,
  CodexSettings,
  DEFAULT_SERVER_SETTINGS,
  PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS,
  ProviderDriverKind,
  ProviderInstanceId,
  ServerSettings,
  ServerSettingsError,
  ServerProvider,
  type ServerProviderSlashCommand,
  type ServerSettings as ContractServerSettings,
} from "@t3tools/contracts";
import { HttpClient, HttpClientResponse } from "effect/http";
import { ChildProcessSpawner } from "effect/process";
import { deepMerge } from "@t3tools/shared/Struct";
import { createModelCapabilities } from "@t3tools/shared/model";
import { applyServerSettingsPatch } from "@t3tools/shared/serverSettings";

import { checkCodexProviderStatus, type CodexAppServerProviderSnapshot } from "./CodexProvider.ts";
import { checkClaudeProviderStatus } from "./ClaudeProvider.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as AntigravityInstallation from "./AntigravityInstallation.ts";
import * as ModelManifest from "./ModelManifest.ts";
import { applyProviderCompatibility } from "./providerCompatibility.ts";
import * as ResetCreditCoordinator from "./resetCreditCoordinator.ts";
import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as OpenCodeRuntime from "@t3tools/provider-opencode/server/OpenCodeRuntime";
import * as OpenCodeServerLedger from "@t3tools/provider-opencode/server/OpenCodeServerLedger";
import * as ClaudeSessionStore from "./ClaudeSessionStore.ts";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderInstanceRegistryHydration from "./ProviderInstanceRegistryHydration.ts";
import { makeProviderRegistryRebuildBarrier } from "./ProviderRegistryRebuildBarrier.ts";
import * as ProviderRegistryRebuildBarrier from "./ProviderRegistryRebuildBarrier.ts";
import * as ServerConfig from "../config.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettingsModule from "../serverSettings.ts";
import {
  readProviderStatusCache,
  resolveProviderStatusCachePath,
  writeProviderStatusCache,
} from "./providerStatusCache.ts";
import { COMPACT_SLASH_COMMAND } from "@t3tools/provider-core/server/snapshotProbe";
import type {
  ProviderInstance,
  ProviderWorkspaceSnapshot,
} from "@t3tools/provider-core/server/driver";
import * as ProviderInstanceRegistry from "./ProviderInstanceRegistry.ts";
import * as ProviderRegistry from "./ProviderRegistry.ts";
import { mergeProviderInstanceEnvironment } from "@t3tools/provider-core/server/instanceEnvironment";
import { makeManualOnlyProviderMaintenanceCapabilities } from "@t3tools/provider-core/server/maintenanceResolver";
vi.mock("@anthropic-ai/claude-agent-sdk", { spy: true });

const decodeServerProvider = Schema.decodeSync(ServerProvider);
const encodeServerProvider = Schema.encodeSync(ServerProvider);
const decodeServerSettings = Schema.decodeSync(ServerSettings);
const encodeServerSettings = Schema.encodeSync(ServerSettings);
const encodedDefaultServerSettings = encodeServerSettings(DEFAULT_SERVER_SETTINGS);

const disabledDefaultSlots = (
  ...drivers: ReadonlyArray<string>
): ContractServerSettings["providerInstances"] =>
  Object.fromEntries(
    drivers.map((driver) => [
      ProviderInstanceId.make(driver),
      { driver: ProviderDriverKind.make(driver), enabled: false },
    ]),
  );

const defaultClaudeSettings: ClaudeSettings = Schema.decodeSync(ClaudeSettings)({});
const defaultCodexSettings: CodexSettings = Schema.decodeSync(CodexSettings)({});

process.env.T3CODE_CURSOR_ENABLED = "1";

const encoder = new TextEncoder();
const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const withBundledCompatibility = (snapshot: ServerProvider) =>
  applyProviderCompatibility(
    snapshot,
    undefined,
    ModelManifest.BUNDLED_MODEL_MANIFEST.compatibility,
  );

const layerTestHttpClient = Layer.succeed(
  HttpClient.HttpClient,
  HttpClient.make((request) =>
    Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ version: "0.0.0" }))),
  ),
).pipe(Layer.provideMerge(ModelManifest.layerTest));
const ClaudeSessionStoreTestLive = ClaudeSessionStore.layer.pipe(
  Layer.provide(SqlitePersistence.layerMemory),
);
const ProviderInstanceRegistryHydrationTestLive = ProviderInstanceRegistryHydration.layer.pipe(
  Layer.provideMerge(ProviderRegistrySessionLifecycle.layer),
  Layer.provide(ProviderRegistryRebuildBarrier.layer),
);
const layerRegistry = ProviderRegistry.layer.pipe(
  Layer.provideMerge(ProviderRegistryRebuildBarrier.layer),
);

const layerBackgroundPolicyAlwaysRun = Layer.mock(BackgroundPolicy.BackgroundPolicy)({
  reportClientActivity: () => Effect.void,
  removeRpcClient: () => Effect.void,
  reportHostPowerState: () => Effect.void,
  snapshot: Effect.succeed({
    hostPower: {
      source: "unknown",
      idle: "unknown",
      idleSeconds: null,
      locked: "unknown",
      suspended: false,
      onBattery: "unknown",
      lowPowerMode: "unknown",
      thermalState: "unknown",
      stale: true,
      updatedAt: TEST_EPOCH,
    },
    leases: [],
    activeForegroundLeaseCount: 0,
    activeScopeKeys: [],
    shouldRunOpportunisticWork: true,
    updatedAt: TEST_EPOCH,
  }),
  streamChanges: Stream.empty,
  hasDemand: () => Effect.succeed(true),
  shouldRunScopeWork: () => Effect.succeed(true),
  shouldRunOpportunisticWork: Effect.succeed(true),
});

function selectDescriptor(
  id: string,
  label: string,
  options: ReadonlyArray<{ id: string; label: string; isDefault?: boolean }>,
) {
  return {
    id,
    label,
    type: "select" as const,
    options: [...options],
    ...(options.find((option) => option.isDefault)?.id
      ? { currentValue: options.find((option) => option.isDefault)?.id }
      : {}),
  };
}

function booleanDescriptor(id: string, label: string) {
  return {
    id,
    label,
    type: "boolean" as const,
  };
}

function makeSnapshotTestInstance(input: {
  readonly provider: ServerProvider;
  readonly getSnapshot?: Effect.Effect<ServerProvider>;
  readonly streamChanges?: Stream.Stream<ServerProvider>;
}): ProviderInstance {
  return {
    instanceId: input.provider.instanceId,
    driverKind: input.provider.driver,
    continuationIdentity: {
      driverKind: input.provider.driver,
      continuationKey: `${input.provider.driver}:instance:${input.provider.instanceId}`,
    },
    displayName: undefined,
    enabled: true,
    snapshot: {
      resolveMaintenance: () =>
        Effect.succeed(
          makeManualOnlyProviderMaintenanceCapabilities({
            provider: input.provider.driver,
            packageName: null,
          }),
        ),
      getSnapshot: input.getSnapshot ?? Effect.succeed(input.provider),
      refresh: Effect.succeed(input.provider),
      streamChanges: input.streamChanges ?? Stream.empty,
      applyUsageLimits: () => Effect.void,
    },
    orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
    textGeneration: {} as ProviderInstance["textGeneration"],
  };
}

type TestClaudeCapabilities = {
  readonly email: string | undefined;
  readonly subscriptionType: string | undefined;
  readonly tokenSource: string | undefined;
  readonly apiKeySource: string | undefined;
  readonly apiProvider: string | undefined;
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
};

function claudeCapabilities(overrides: Partial<TestClaudeCapabilities> = {}) {
  return () =>
    Effect.succeed({
      email: undefined,
      subscriptionType: undefined,
      tokenSource: undefined,
      apiKeySource: undefined,
      apiProvider: undefined,
      slashCommands: [],
      ...overrides,
    });
}

function mockHandle(result: { stdout: string; stderr: string; code: number }) {
  return ChildProcessSpawner.makeHandle({
    pid: ChildProcessSpawner.ProcessId(1),
    exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(result.code)),
    isRunning: Effect.succeed(false),
    kill: () => Effect.void,
    unref: Effect.succeed(Effect.void),
    stdin: Sink.drain,
    stdout: Stream.make(encoder.encode(result.stdout)),
    stderr: Stream.make(encoder.encode(result.stderr)),
    all: Stream.empty,
    getInputFd: () => Sink.drain,
    getOutputFd: () => Stream.empty,
  });
}

function layerMockSpawner(
  handler: (args: ReadonlyArray<string>) => {
    stdout: string;
    stderr: string;
    code: number;
  },
) {
  return Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make((command) => {
      const cmd = command as unknown as { args: ReadonlyArray<string> };
      return Effect.succeed(mockHandle(handler(cmd.args)));
    }),
  );
}

const codexModelCapabilities = createModelCapabilities({
  optionDescriptors: [
    selectDescriptor("reasoningEffort", "Reasoning", [
      { id: "high", label: "High", isDefault: true },
      { id: "low", label: "Low" },
    ]),
    booleanDescriptor("fastMode", "Fast Mode"),
  ],
}) satisfies NonNullable<ServerProvider["models"][number]["capabilities"]>;

function makeCodexProbeSnapshot(
  input: Partial<CodexAppServerProviderSnapshot> = {},
): CodexAppServerProviderSnapshot {
  return {
    version: "1.0.0",
    account: {
      account: {
        type: "chatgpt",
        email: "test@example.com",
        planType: "pro",
      },
      requiresOpenaiAuth: false,
    },
    models: [
      {
        slug: "gpt-live-codex",
        name: "GPT Live Codex",
        isCustom: false,
        capabilities: codexModelCapabilities,
      },
    ],
    skills: [],
    ...input,
  };
}

function makeMutableServerSettingsService(
  initial: ContractServerSettings = DEFAULT_SERVER_SETTINGS,
) {
  return Effect.gen(function* () {
    const settingsRef = yield* Ref.make(initial);
    const changes = yield* PubSub.unbounded<ContractServerSettings>();

    return {
      start: Effect.void,
      ready: Effect.void,
      getSettings: Ref.get(settingsRef),
      updateSettings: (patch) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(settingsRef);
          const next = applyServerSettingsPatch(current, patch);
          encodeServerSettings(next);
          yield* Ref.set(settingsRef, next);
          yield* PubSub.publish(changes, next);
          return next;
        }),
      updateProviderInstance: (mutation, patch = {}) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(settingsRef);
          const next = ServerSettingsModule.applyProviderInstanceMutation(
            applyServerSettingsPatch(current, patch),
            mutation,
          );
          encodeServerSettings(next);
          yield* Ref.set(settingsRef, next);
          yield* PubSub.publish(changes, next);
          return next;
        }),
      withSettingsSnapshot: (use) => Ref.get(settingsRef).pipe(Effect.flatMap(use)),
      get streamChanges() {
        return Stream.fromPubSub(changes);
      },
      get subscribeChanges() {
        return PubSub.subscribe(changes).pipe(
          Effect.map((subscription) => Stream.fromSubscription(subscription)),
        );
      },
    } satisfies ServerSettingsModule.ServerSettingsService["Service"];
  });
}

const awaitPersistedProvider = (
  registry: ProviderRegistry.ProviderRegistry["Service"],
  checkedAt: string,
) =>
  registry.streamChanges.pipe(
    Stream.filter((providers) => providers.some((provider) => provider.checkedAt === checkedAt)),
    Stream.take(1),
    Stream.runDrain,
    Effect.forkScoped,
  );

const layerTestNodeServices = Layer.mergeAll(
  ClaudeSessionStoreTestLive,
  ProviderLatestVersions.layer,
  NodeServices.layer,
  Layer.mock(CodexManagedTokenBroker.CodexManagedTokenBroker)({
    endpoint: Effect.succeed("unused-private-pipe"),
  }),
  Layer.mock(CodexInstallation.CodexInstallation)({
    managedDirectory: "unused-managed-installation",
  }),
  Layer.mock(ServerSecretStore.ServerSecretStore)({}),
  Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("00000000-0000-4000-8000-000000000001")),
  }),
);

it.layer(
  Layer.mergeAll(layerTestNodeServices, ServerSettingsModule.layerTest(), layerTestHttpClient),
)("ProviderRegistry", (it) => {
  describe("checkCodexProviderStatus", () => {
    it.effect("uses the app-server account and model list for provider status", () =>
      Effect.gen(function* () {
        const status = yield* checkCodexProviderStatus(defaultCodexSettings, () =>
          Effect.succeed(
            makeCodexProbeSnapshot({
              skills: [
                {
                  name: "github:gh-fix-ci",
                  path: "/Users/test/.codex/skills/gh-fix-ci/SKILL.md",
                  enabled: true,
                  displayName: "CI Debug",
                  shortDescription: "Debug failing GitHub Actions checks",
                },
              ],
            }),
          ),
        );
        assert.strictEqual(status.status, "ready");
        assert.strictEqual(status.installed, true);
        assert.strictEqual(status.version, "1.0.0");
        assert.strictEqual(status.auth.status, "authenticated");
        assert.strictEqual(status.auth.type, "chatgpt");
        assert.strictEqual(status.auth.label, "ChatGPT Pro 200 Subscription");
        assert.strictEqual(status.auth.email, "test@example.com");
        assert.deepStrictEqual(status.setup, { canAuthenticate: false, canInstall: false });
        assert.deepStrictEqual(status.models, [
          {
            slug: "gpt-live-codex",
            name: "GPT Live Codex",
            isCustom: false,
            capabilities: codexModelCapabilities,
          },
        ]);
        assert.deepStrictEqual(status.skills, [
          {
            name: "github:gh-fix-ci",
            path: "/Users/test/.codex/skills/gh-fix-ci/SKILL.md",
            enabled: true,
            displayName: "CI Debug",
            shortDescription: "Debug failing GitHub Actions checks",
          },
        ]);
        assert.deepStrictEqual(status.slashCommands.slice(1), [
          {
            name: "goal",
            description: "Set a goal Codex keeps working toward until it is done",
            input: { hint: "Objective, or pause, resume, clear" },
          },
          {
            name: "feedback",
            description: "Send this thread and Codex logs to OpenAI",
            input: { hint: "Describe the issue (optional)" },
          },
        ]);
      }),
    );

    it.effect("returns unauthenticated when app-server requires OpenAI auth", () =>
      Effect.gen(function* () {
        const status = yield* checkCodexProviderStatus(defaultCodexSettings, () =>
          Effect.succeed(
            makeCodexProbeSnapshot({
              account: {
                account: null,
                requiresOpenaiAuth: true,
              },
            }),
          ),
        );

        assert.strictEqual(status.status, "error");
        assert.strictEqual(status.auth.status, "unauthenticated");
        assert.strictEqual(
          status.message,
          "Codex CLI is not authenticated. Run `codex login` and try again.",
        );
        assert.deepStrictEqual(status.usageLimits?.unavailable, {
          reason: "probeFailed",
          message: "Codex CLI is not authenticated. Run `codex login` and try again.",
        });
      }),
    );

    it.effect("returns ready with unknown auth when app-server does not require OpenAI auth", () =>
      Effect.gen(function* () {
        const status = yield* checkCodexProviderStatus(defaultCodexSettings, () =>
          Effect.succeed(
            makeCodexProbeSnapshot({
              account: {
                account: null,
                requiresOpenaiAuth: false,
              },
            }),
          ),
        );

        assert.strictEqual(status.status, "ready");
        assert.strictEqual(status.auth.status, "unknown");
      }),
    );
  });

  describe("ProviderRegistry.layer", () => {
    it("stores workspace skills, commands and options without changing machine metadata", () => {
      const provider = {
        instanceId: ProviderInstanceId.make("codex"),
        driver: ProviderDriverKind.make("codex"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-03-25T00:00:00.000Z",
        version: "1.0.0",
        models: [],
        slashCommands: [{ name: "global" }],
        skills: [{ name: "global", path: "/global/SKILL.md", enabled: true }],
      } satisfies ServerProvider;
      const scopedSnapshot = {
        ...provider,
        checkedAt: "2026-03-25T00:01:00.000Z",
        slashCommands: [{ name: "project" }],
        skills: [{ name: "project", path: "/project/SKILL.md", enabled: true }],
        optionDescriptors: [
          {
            id: "agent",
            label: "Agent",
            type: "select",
            options: [{ id: "readonly", label: "Readonly" }],
          },
        ],
      } satisfies ProviderWorkspaceSnapshot;

      const result = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        provider,
        "/project",
        scopedSnapshot,
      );

      assert.deepStrictEqual(result.slashCommands, provider.slashCommands);
      assert.deepStrictEqual(result.skills, provider.skills);
      assert.strictEqual(result.models, provider.models);
      const decoded = decodeServerProvider(encodeServerProvider(result));
      assert.deepStrictEqual(decoded.workspaceSnapshots, [
        {
          cwd: "/project",
          checkedAt: scopedSnapshot.checkedAt,
          slashCommands: scopedSnapshot.slashCommands,
          skills: scopedSnapshot.skills,
          optionDescriptors: scopedSnapshot.optionDescriptors,
        },
      ]);

      const pendingSnapshot = {
        ...scopedSnapshot,
        slashCommands: [COMPACT_SLASH_COMMAND],
        slashCommandsPending: true,
      } satisfies ProviderWorkspaceSnapshot;
      const partial = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        result,
        "/project",
        pendingSnapshot,
      );
      assert.deepStrictEqual(partial.workspaceSnapshots?.[0]?.slashCommands, [{ name: "project" }]);
      assert.strictEqual(partial.workspaceSnapshots?.[0]?.slashCommandsPending, true);
      const otherProject = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        partial,
        "/other-project",
        pendingSnapshot,
      );
      assert.deepStrictEqual(otherProject.workspaceSnapshots?.[1]?.slashCommands, [
        COMPACT_SLASH_COMMAND,
      ]);
      const recovered = ProviderRegistry.upsertProviderWorkspaceSnapshot(partial, "/project", {
        ...scopedSnapshot,
        slashCommands: [COMPACT_SLASH_COMMAND, { name: "replacement" }],
      });
      assert.deepStrictEqual(recovered.workspaceSnapshots?.[0]?.slashCommands, [
        COMPACT_SLASH_COMMAND,
        { name: "replacement" },
      ]);
      assert.strictEqual(recovered.workspaceSnapshots?.[0]?.slashCommandsPending, undefined);

      const { optionDescriptors, ...failedSnapshot } = scopedSnapshot;
      const firstFailure = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        provider,
        "/project",
        failedSnapshot,
      );
      assert.strictEqual(
        Object.hasOwn(firstFailure.workspaceSnapshots![0]!, "optionDescriptors"),
        false,
      );
      const failedOtherProject = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        recovered,
        "/other-project",
        failedSnapshot,
      );
      assert.strictEqual(
        Object.hasOwn(failedOtherProject.workspaceSnapshots![1]!, "optionDescriptors"),
        false,
      );

      for (const clearedDescriptors of [[], [{ ...optionDescriptors[0]!, options: [] }]]) {
        const cleared = ProviderRegistry.upsertProviderWorkspaceSnapshot(recovered, "/project", {
          ...scopedSnapshot,
          optionDescriptors: clearedDescriptors,
        });
        assert.deepStrictEqual(
          cleared.workspaceSnapshots?.[0]?.optionDescriptors,
          clearedDescriptors,
        );
      }

      const failedRefresh = ProviderRegistry.upsertProviderWorkspaceSnapshot(
        failedOtherProject,
        "/project",
        failedSnapshot,
      );
      assert.deepStrictEqual(
        failedRefresh.workspaceSnapshots?.[1]?.optionDescriptors,
        optionDescriptors,
      );
    });

    it("preserves previously discovered provider models when a refresh returns none", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("cursor"),
        driver: ProviderDriverKind.make("cursor"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-04-14T00:00:00.000Z",
        version: "2026.04.09-f2b0fcd",
        models: [
          {
            slug: "claude-opus-4-6",
            name: "Opus 4.6",
            isCustom: false,
            capabilities: createModelCapabilities({
              optionDescriptors: [
                selectDescriptor("reasoning", "Reasoning", [
                  { id: "high", label: "High", isDefault: true },
                ]),
                booleanDescriptor("fastMode", "Fast Mode"),
                booleanDescriptor("thinking", "Thinking"),
              ],
            }),
          },
        ],
        slashCommands: [{ name: "review", description: "Review changes" }],
        skills: [
          {
            name: "typescript",
            description: "TypeScript help",
            path: "/skills/typescript/SKILL.md",
            enabled: true,
          },
        ],
      } as const satisfies ServerProvider;
      const refreshedProvider = {
        ...previousProvider,
        checkedAt: "2026-04-14T00:01:00.000Z",
        models: [],
        slashCommands: [],
        skills: [],
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).models,
        [...previousProvider.models],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).slashCommands,
        [],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).skills,
        [],
      );
    });

    it("does not bring back models the installed CLI is too old to run", () => {
      const pendingProvider = {
        instanceId: ProviderInstanceId.make("claudeAgent"),
        driver: ProviderDriverKind.make("claudeAgent"),
        status: "warning",
        enabled: true,
        installed: false,
        auth: { status: "unknown" },
        checkedAt: "2026-04-14T00:00:00.000Z",
        version: null,
        models: [
          { slug: "claude-old", name: "Old", isCustom: false, capabilities: null },
          { slug: "claude-next", name: "Next", isCustom: false, capabilities: null },
        ],
        slashCommands: [],
        skills: [],
      } as const satisfies ServerProvider;
      const probedProvider = {
        ...pendingProvider,
        status: "ready",
        installed: true,
        auth: { status: "authenticated" },
        version: "1.0.0",
        models: [pendingProvider.models[0]],
        updateRequiredModels: [{ slug: "claude-next", name: "Next", minVersion: "1.1.0" }],
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(pendingProvider, probedProvider).models,
        [...probedProvider.models],
      );
    });

    it("retains ACP Registry models while discovery has not completed", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("acpRegistry_codex"),
        driver: ProviderDriverKind.make("acpRegistry"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-08-13T00:00:00.000Z",
        version: "1.2.0",
        models: [{ slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null }],
        slashCommands: [],
        skills: [],
      } as const satisfies ServerProvider;
      const checkingProvider = {
        ...previousProvider,
        checkedAt: "2026-08-13T00:01:00.000Z",
        auth: { status: "unknown" },
        message: "Checking ACP authentication, models, and commands in the background...",
        models: [{ slug: "default", name: "Default", isCustom: false, capabilities: null }],
      } satisfies ServerProvider;
      const confirmedCheckingProvider = {
        ...checkingProvider,
        auth: { status: "authenticated" },
      } satisfies ServerProvider;
      const failedProbeProvider = {
        ...checkingProvider,
        status: "warning",
        message: "ACP discovery failed.",
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, checkingProvider).models,
        [
          { slug: "default", name: "Default", isCustom: false, capabilities: null },
          { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
        ],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, confirmedCheckingProvider).models,
        [
          { slug: "default", name: "Default", isCustom: false, capabilities: null },
          { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
        ],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, failedProbeProvider).models,
        [
          { slug: "default", name: "Default", isCustom: false, capabilities: null },
          { slug: "gpt-5.6-sol", name: "GPT-5.6-Sol", isCustom: false, capabilities: null },
        ],
      );
    });

    it("drops Grok models a completed probe no longer advertises", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("grok"),
        driver: ProviderDriverKind.make("grok"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-09-30T00:00:00.000Z",
        version: "1.0.44",
        models: [
          { slug: "grok-4.7", name: "Grok 4.7", isCustom: false, capabilities: null },
          { slug: "codexhub-removed", name: "Removed", isCustom: false, capabilities: null },
        ],
        slashCommands: [],
        skills: [],
      } as const satisfies ServerProvider;
      const refreshedProvider = {
        ...previousProvider,
        checkedAt: "2026-09-30T00:01:00.000Z",
        models: [previousProvider.models[0]],
      } satisfies ServerProvider;
      const acpFailedProvider = {
        ...refreshedProvider,
        status: "warning",
        message: "Grok CLI is installed but ACP initialize failed.",
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).models,
        [previousProvider.models[0]],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, acpFailedProvider).models,
        [...previousProvider.models],
      );
    });

    it("retains stale OpenCode models when a refresh fails", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("opencode"),
        driver: ProviderDriverKind.make("opencode"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-07-17T00:00:00.000Z",
        version: "1.0.0",
        models: [
          {
            slug: "github/gpt-5",
            name: "GPT-5",
            subProvider: "GitHub",
            isCustom: false,
            capabilities: null,
          },
        ],
        slashCommands: [{ name: "review", description: "Review changes" }],
        skills: [
          {
            name: "typescript",
            description: "TypeScript help",
            path: "/skills/typescript/SKILL.md",
            enabled: true,
          },
        ],
      } as const satisfies ServerProvider;
      const refreshedProvider = {
        ...previousProvider,
        status: "error",
        auth: { status: "unknown" },
        checkedAt: "2026-07-17T00:01:00.000Z",
        models: [],
        message: "Failed to refresh OpenCode models.",
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).models,
        [...previousProvider.models],
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).slashCommands,
        previousProvider.slashCommands,
      );
      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).skills,
        previousProvider.skills,
      );
    });

    describe("Pi model inventories", () => {
      const defaultModel = {
        slug: "default",
        name: "Pi default",
        isCustom: false,
        capabilities: null,
      } as const;
      const currentModel = {
        slug: "openai/gpt-6.1-sol",
        name: "GPT-6.1-Sol",
        isCustom: false,
        capabilities: null,
      } as const;
      const removedModel = {
        slug: "openrouter/google/gemini-2.5-pro",
        name: "Google: Gemini 2.5 Pro",
        isCustom: false,
        capabilities: null,
      } as const;
      const customModel = {
        slug: "custom-model",
        name: "Custom model",
        isCustom: true,
        capabilities: null,
      } as const;
      const cachedProvider = {
        instanceId: ProviderInstanceId.make("pi-personal"),
        driver: ProviderDriverKind.make("pi"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated", type: "pi" },
        checkedAt: "2026-10-03T00:00:00.000Z",
        version: "1.0.0",
        models: [
          defaultModel,
          currentModel,
          removedModel,
          { ...customModel, slug: "removed-custom" },
        ],
        slashCommands: [],
        skills: [],
      } satisfies ServerProvider;
      const refreshedProvider = {
        ...cachedProvider,
        checkedAt: "2026-10-03T00:01:00.000Z",
        models: [defaultModel, currentModel, customModel],
      } satisfies ServerProvider;
      const pendingProvider = {
        ...cachedProvider,
        status: "warning",
        version: null,
        auth: { status: "unknown" },
        models: [defaultModel, customModel],
      } satisfies ServerProvider;
      const failedProvider = {
        ...pendingProvider,
        checkedAt: "2026-10-03T00:02:00.000Z",
        status: "ready",
        version: "1.0.0",
        message: "Pi is available, but T3 Code could not refresh its models and commands.",
      } satisfies ServerProvider;

      it("retains Pi inventories during incomplete probes without restoring removed custom models", () => {
        const incompleteProviders = [
          pendingProvider,
          { ...pendingProvider, installed: false },
          failedProvider,
          { ...failedProvider, status: "error" },
        ] satisfies ReadonlyArray<ServerProvider>;

        for (const provider of incompleteProviders) {
          assert.deepStrictEqual(
            ProviderRegistry.mergeProviderSnapshot(cachedProvider, provider).models,
            [defaultModel, customModel, currentModel, removedModel],
          );
        }
      });

      it("does not restore disconnected Pi models after a later discovery failure", () => {
        const afterRemoval = ProviderRegistry.mergeProviderSnapshot(
          cachedProvider,
          refreshedProvider,
        );
        assert.deepStrictEqual(
          ProviderRegistry.mergeProviderSnapshot(afterRemoval, failedProvider).models,
          [defaultModel, customModel, currentModel],
        );
      });

      it.effect(
        "persists Pi inventory removals across failed refreshes and registry restarts",
        () =>
          Effect.gen(function* () {
            const config = yield* ServerConfig.ServerConfig;
            const filePath = yield* resolveProviderStatusCachePath({
              cacheDir: config.providerStatusCacheDir,
              instanceId: cachedProvider.instanceId,
            });
            yield* writeProviderStatusCache({ filePath, provider: cachedProvider });
            const nextProvider = yield* Ref.make<ServerProvider>(refreshedProvider);
            const instance = {
              instanceId: cachedProvider.instanceId,
              driverKind: cachedProvider.driver,
              continuationIdentity: {
                driverKind: cachedProvider.driver,
                continuationKey: "pi:instance:pi-personal",
              },
              displayName: undefined,
              enabled: true,
              snapshot: {
                resolveMaintenance: () =>
                  Effect.succeed(
                    makeManualOnlyProviderMaintenanceCapabilities({
                      provider: cachedProvider.driver,
                      packageName: null,
                    }),
                  ),
                getSnapshot: Effect.succeed(pendingProvider),
                refresh: Ref.get(nextProvider),
                streamChanges: Stream.empty,
                applyUsageLimits: () => Effect.void,
              },
              orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
              textGeneration: {} as ProviderInstance["textGeneration"],
            } satisfies ProviderInstance;
            const layerInstanceRegistry = Layer.succeed(
              ProviderInstanceRegistry.ProviderInstanceRegistry,
              {
                getInstance: (id) =>
                  Effect.succeed(id === instance.instanceId ? instance : undefined),
                listInstances: Effect.succeed([instance]),
                listUnavailable: Effect.succeed([]),
                streamChanges: Stream.empty,
                subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
              },
            );
            const retainedModels = [defaultModel, customModel, currentModel];

            for (const restarted of [false, true]) {
              yield* Effect.gen(function* () {
                const registry = yield* ProviderRegistry.ProviderRegistry;
                assert.deepStrictEqual(
                  (yield* registry.getProviders)[0]?.models,
                  restarted
                    ? retainedModels
                    : [defaultModel, customModel, currentModel, removedModel],
                );

                yield* registry.refreshInstance(instance.instanceId);
                assert.deepStrictEqual(
                  (yield* readProviderStatusCache(filePath))?.models,
                  restarted ? retainedModels : refreshedProvider.models,
                );

                yield* Ref.set(nextProvider, failedProvider);
                const afterFailure = yield* registry.refreshInstance(instance.instanceId);
                assert.deepStrictEqual(afterFailure[0]?.models, retainedModels);
                assert.deepStrictEqual(
                  (yield* readProviderStatusCache(filePath))?.models,
                  retainedModels,
                );
              }).pipe(
                Effect.provide(layerRegistry.pipe(Layer.provide(layerInstanceRegistry))),
                Effect.scoped,
              );
            }
          }).pipe(
            Effect.provide(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-pi-model-cache-",
              }).pipe(Layer.provideMerge(NodeServices.layer)),
            ),
          ),
      );
    });

    describe("Codex model inventories", () => {
      const cachedProvider = {
        instanceId: ProviderInstanceId.make("codex-personal"),
        driver: ProviderDriverKind.make("codex"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-09-04T19:00:00.000Z",
        version: "0.153.3",
        models: ["vega-alpha", "joule-alpha", "kindle-alpha", "ultima-alpha", "solstice-alpha"].map(
          (slug) => ({ slug, name: slug, isCustom: false, capabilities: null }),
        ),
        slashCommands: [],
        skills: [],
      } satisfies ServerProvider;
      const customModel = {
        slug: "custom-model",
        name: "Custom model",
        isCustom: true,
        capabilities: null,
      } as const;
      const refreshedProvider = {
        ...cachedProvider,
        checkedAt: "2026-09-04T19:01:00.000Z",
        models: [
          { slug: "gpt-6-astra", name: "GPT 6 Astra", isCustom: false, capabilities: null },
          cachedProvider.models[0]!,
          customModel,
        ],
      } satisfies ServerProvider;
      const pendingProvider = {
        ...cachedProvider,
        status: "warning",
        installed: false,
        auth: { status: "unknown" },
        models: [customModel],
      } satisfies ServerProvider;
      const failedProvider = {
        ...pendingProvider,
        checkedAt: "2026-09-04T19:02:00.000Z",
        status: "error",
        installed: true,
      } satisfies ServerProvider;

      it.effect("persists removals across failed refreshes and registry restarts", () =>
        Effect.gen(function* () {
          const config = yield* ServerConfig.ServerConfig;
          const filePath = yield* resolveProviderStatusCachePath({
            cacheDir: config.providerStatusCacheDir,
            instanceId: cachedProvider.instanceId,
          });
          yield* writeProviderStatusCache({ filePath, provider: cachedProvider });
          const nextProvider = yield* Ref.make<ServerProvider>(refreshedProvider);
          const instance = {
            instanceId: cachedProvider.instanceId,
            driverKind: cachedProvider.driver,
            continuationIdentity: {
              driverKind: cachedProvider.driver,
              continuationKey: "codex:instance:codex-personal",
            },
            displayName: undefined,
            enabled: true,
            snapshot: {
              resolveMaintenance: () =>
                Effect.succeed(
                  makeManualOnlyProviderMaintenanceCapabilities({
                    provider: cachedProvider.driver,
                    packageName: null,
                  }),
                ),
              getSnapshot: Effect.succeed(pendingProvider),
              refresh: Ref.get(nextProvider),
              streamChanges: Stream.empty,
              applyUsageLimits: () => Effect.void,
            },
            orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
            textGeneration: {} as ProviderInstance["textGeneration"],
          } satisfies ProviderInstance;
          const instanceRegistryLayer = Layer.succeed(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            {
              getInstance: (id) =>
                Effect.succeed(id === instance.instanceId ? instance : undefined),
              listInstances: Effect.succeed([instance]),
              listUnavailable: Effect.succeed([]),
              streamChanges: Stream.empty,
              subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
            },
          );
          const retainedModels = [
            customModel,
            ...refreshedProvider.models.filter((model) => !model.isCustom),
          ];

          for (const restarted of [false, true]) {
            yield* Effect.gen(function* () {
              const registry = yield* ProviderRegistry.ProviderRegistry;
              const expectedModels = restarted
                ? retainedModels
                : [customModel, ...cachedProvider.models];
              assert.deepStrictEqual((yield* registry.getProviders)[0]?.models, expectedModels);

              yield* registry.refreshInstance(instance.instanceId);
              assert.deepStrictEqual(
                (yield* readProviderStatusCache(filePath))?.models,
                restarted ? retainedModels : refreshedProvider.models,
              );

              yield* Ref.set(nextProvider, failedProvider);
              const afterFailure = yield* registry.refreshInstance(instance.instanceId);
              assert.deepStrictEqual(afterFailure[0]?.models, retainedModels);
              assert.deepStrictEqual(
                (yield* readProviderStatusCache(filePath))?.models,
                retainedModels,
              );
            }).pipe(
              Effect.provide(layerRegistry.pipe(Layer.provide(instanceRegistryLayer))),
              Effect.scoped,
            );
          }
        }).pipe(
          Effect.provide(
            ServerConfig.layerTest(process.cwd(), {
              prefix: "t3-codex-retired-model-cache-",
            }).pipe(Layer.provideMerge(NodeServices.layer)),
          ),
        ),
      );
    });

    describe("Antigravity model inventories", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("antigravity-personal"),
        driver: ProviderDriverKind.make("antigravity"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-09-02T00:00:00.000Z",
        version: "0.1.3",
        models: [
          {
            slug: "gemini-3.1-pro-high",
            name: "Gemini 3.1 Pro High",
            isCustom: false,
            capabilities: null,
          },
          {
            slug: "gemini-3-flash",
            name: "Gemini 3 Flash",
            isCustom: false,
            capabilities: null,
          },
        ],
        slashCommands: [],
        skills: [],
      } as const satisfies ServerProvider;

      it("clears models after sign-out, disable, uninstall, or an empty successful refresh", () => {
        const emptyProvider = {
          ...previousProvider,
          checkedAt: "2026-09-02T00:01:00.000Z",
          models: [],
        } satisfies ServerProvider;
        const clearedProviders = [
          { ...emptyProvider, status: "warning", auth: { status: "unauthenticated" } },
          { ...emptyProvider, status: "error", auth: { status: "unauthenticated" } },
          { ...emptyProvider, status: "disabled", enabled: false },
          { ...emptyProvider, status: "error", enabled: false },
          { ...emptyProvider, status: "error", installed: false, auth: { status: "unknown" } },
          emptyProvider,
        ] satisfies ReadonlyArray<ServerProvider>;

        for (const provider of clearedProviders) {
          const afterRemoval = ProviderRegistry.mergeProviderSnapshot(previousProvider, provider);
          assert.deepStrictEqual(afterRemoval.models, []);

          const afterFailure = ProviderRegistry.mergeProviderSnapshot(afterRemoval, {
            ...emptyProvider,
            status: "error",
            auth: { status: "unknown" },
          });
          assert.deepStrictEqual(afterFailure.models, []);
        }
      });
    });

    describe("Antigravity saved account", () => {
      const signedIn = {
        instanceId: ProviderInstanceId.make("antigravity-personal"),
        driver: ProviderDriverKind.make("antigravity"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated", type: "oauth-personal", label: "Google account" },
        checkedAt: "2026-09-05T00:00:00.000Z",
        version: "agy_acp_server_1.1.1",
        models: [
          {
            slug: "gemini-3.7-flash-high",
            name: "Gemini 3.7 Flash",
            isCustom: false,
            capabilities: null,
          },
        ],
        slashCommands: [{ name: "plan" }],
        skills: [],
      } as const satisfies ServerProvider;
      const uncheckedMessage =
        "Antigravity is installed. Google account access is not checked yet.";
      const restartProbe = {
        ...signedIn,
        status: "warning",
        auth: { status: "unknown" },
        checkedAt: "2026-09-05T00:01:00.000Z",
        message: uncheckedMessage,
        models: [],
      } as const satisfies ServerProvider;

      it("keeps the saved Google account through restart health checks", () => {
        const merged = ProviderRegistry.mergeProviderSnapshot(signedIn, restartProbe);
        const { message: _uncheckedMessage, ...probeWithoutMessage } = restartProbe;
        assert.deepStrictEqual(merged, {
          ...probeWithoutMessage,
          status: "ready",
          auth: signedIn.auth,
          models: signedIn.models,
        });
        assert.equal("message" in merged, false);
        assert.deepStrictEqual(
          ProviderRegistry.mergeProviderSnapshot(merged, restartProbe),
          merged,
        );
      });

      it("does not invent an account after sign-out, disable, uninstall, or for other providers", () => {
        const untouched = [
          { ...restartProbe, auth: { status: "unauthenticated" } },
          { ...restartProbe, status: "disabled", enabled: false },
          { ...restartProbe, status: "error", installed: false },
          { ...restartProbe, driver: ProviderDriverKind.make("codex") },
          { ...restartProbe, auth: { status: "unknown", type: "gemini-api-key" } },
        ] satisfies ReadonlyArray<ServerProvider>;
        for (const next of untouched) {
          const merged = ProviderRegistry.mergeProviderSnapshot(signedIn, next);
          assert.deepStrictEqual(merged.auth, next.auth);
          assert.equal(merged.status, next.status);
          assert.equal(merged.message, next.message);
        }
        assert.deepStrictEqual(
          ProviderRegistry.mergeProviderSnapshot(
            { ...signedIn, auth: { status: "unknown" } },
            restartProbe,
          ).auth,
          { status: "unknown" },
        );
        assert.equal(
          ProviderRegistry.mergeProviderSnapshot(
            { ...signedIn, driver: ProviderDriverKind.make("codex") },
            restartProbe,
          ).auth.status,
          "unknown",
        );
        assert.deepStrictEqual(
          ProviderRegistry.mergeProviderSnapshot(signedIn, {
            ...restartProbe,
            auth: { status: "unknown", type: "oauth-personal" },
          }).auth,
          signedIn.auth,
        );
      });
    });

    it("fills missing capabilities from the previous provider snapshot", () => {
      const previousProvider = {
        instanceId: ProviderInstanceId.make("cursor"),
        driver: ProviderDriverKind.make("cursor"),
        status: "ready",
        enabled: true,
        installed: true,
        auth: { status: "authenticated" },
        checkedAt: "2026-04-14T00:00:00.000Z",
        version: "2026.04.09-f2b0fcd",
        models: [
          {
            slug: "claude-opus-4-6",
            name: "Opus 4.6",
            isCustom: false,
            capabilities: createModelCapabilities({
              optionDescriptors: [
                selectDescriptor("reasoning", "Reasoning", [
                  { id: "high", label: "High", isDefault: true },
                ]),
                booleanDescriptor("fastMode", "Fast Mode"),
                booleanDescriptor("thinking", "Thinking"),
              ],
            }),
          },
        ],
        slashCommands: [],
        skills: [],
      } as const satisfies ServerProvider;
      const refreshedProvider = {
        ...previousProvider,
        checkedAt: "2026-04-14T00:01:00.000Z",
        models: [
          {
            slug: "claude-opus-4-6",
            name: "Opus 4.6",
            isCustom: false,
            capabilities: createModelCapabilities({
              optionDescriptors: [],
            }),
          },
        ],
      } satisfies ServerProvider;

      assert.deepStrictEqual(
        ProviderRegistry.mergeProviderSnapshot(previousProvider, refreshedProvider).models,
        [...previousProvider.models],
      );
    });

    it.effect("refreshes cached provider snapshots before allowing a registry rebuild", () =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("codex_guarded_refresh");
        const driverKind = ProviderDriverKind.make("codex");
        const initialProvider = {
          instanceId,
          driver: driverKind,
          status: "ready",
          enabled: true,
          installed: true,
          auth: { status: "authenticated" },
          checkedAt: "2026-07-17T00:00:00.000Z",
          version: "1.0.0",
          models: [],
          slashCommands: [],
          skills: [],
        } as const satisfies ServerProvider;
        const refreshedProvider = {
          ...initialProvider,
          checkedAt: "2026-07-17T00:01:00.000Z",
        } satisfies ServerProvider;
        const cached = yield* Ref.make(true);
        const refreshStarted = yield* Deferred.make<void>();
        const releaseRefresh = yield* Deferred.make<void>();
        const rebuildStarted = yield* Deferred.make<void>();
        const rebuildBarrier = yield* makeProviderRegistryRebuildBarrier;
        const instance = {
          instanceId,
          driverKind,
          continuationIdentity: {
            driverKind,
            continuationKey: "codex:instance:guarded-refresh",
          },
          displayName: undefined,
          enabled: true,
          invalidateCaches: Ref.set(cached, false),
          snapshot: {
            resolveMaintenance: () =>
              Effect.succeed(
                makeManualOnlyProviderMaintenanceCapabilities({
                  provider: driverKind,
                  packageName: null,
                }),
              ),
            getSnapshot: Effect.succeed(initialProvider),
            refresh: Deferred.succeed(refreshStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseRefresh)),
              Effect.andThen(Ref.get(cached)),
              Effect.map((cached) => (cached ? initialProvider : refreshedProvider)),
            ),
            streamChanges: Stream.empty,
            applyUsageLimits: () => Effect.void,
          },
          orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
          textGeneration: {} as ProviderInstance["textGeneration"],
        } satisfies ProviderInstance;
        const instanceRegistryLayer = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            getInstance: (candidateId) =>
              Effect.succeed(candidateId === instanceId ? instance : undefined),
            listInstances: Effect.succeed([instance]),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.empty,
            subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          ProviderRegistry.layer.pipe(
            Layer.provideMerge(instanceRegistryLayer),
            Layer.provideMerge(
              Layer.succeed(
                ProviderRegistryRebuildBarrier.ProviderRegistryRebuildBarrier,
                rebuildBarrier,
              ),
            ),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-guarded-refresh-",
              }),
            ),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const refresh = yield* registry.refreshInstance(instanceId).pipe(Effect.forkScoped);
          yield* Deferred.await(refreshStarted);
          const rebuild = yield* rebuildBarrier
            .withRebuild(Deferred.succeed(rebuildStarted, undefined))
            .pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          assert.isTrue(Option.isNone(yield* Deferred.poll(rebuildStarted)));

          yield* Deferred.succeed(releaseRefresh, undefined);
          const providers = yield* Fiber.join(refresh);
          assert.strictEqual(providers[0]?.checkedAt, refreshedProvider.checkedAt);
          yield* Fiber.join(rebuild);
          assert.isTrue(Option.isSome(yield* Deferred.poll(rebuildStarted)));
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect("discards a late snapshot event from a rebuilt instance", () =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("codex_stale_stream");
        const driver = ProviderDriverKind.make("codex");
        const provider = (checkedAt: string) =>
          ({
            instanceId,
            driver,
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt,
            version: "1.0.0",
            models: [],
            slashCommands: [],
            skills: [],
          }) as const satisfies ServerProvider;
        const oldEvents = yield* PubSub.unbounded<ServerProvider>();
        const oldInstance = makeSnapshotTestInstance({
          provider: provider("2026-07-17T00:00:00.000Z"),
          streamChanges: Stream.fromPubSub(oldEvents),
        });
        const newProvider = provider("2026-07-17T00:01:00.000Z");
        const newInstance = makeSnapshotTestInstance({ provider: newProvider });
        const instances = yield* Ref.make<ReadonlyArray<ProviderInstance>>([oldInstance]);
        const registryChanges = yield* PubSub.unbounded<void>();
        const rebuildBarrier = yield* makeProviderRegistryRebuildBarrier;
        const instanceRegistryLayer = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            getInstance: (candidateId) =>
              Ref.get(instances).pipe(
                Effect.map((current) =>
                  current.find((instance) => instance.instanceId === candidateId),
                ),
              ),
            listInstances: Ref.get(instances),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.fromPubSub(registryChanges),
            subscribeChanges: PubSub.subscribe(registryChanges),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          ProviderRegistry.layer.pipe(
            Layer.provideMerge(instanceRegistryLayer),
            Layer.provideMerge(
              Layer.succeed(
                ProviderRegistryRebuildBarrier.ProviderRegistryRebuildBarrier,
                rebuildBarrier,
              ),
            ),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-stale-stream-",
              }),
            ),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const rebuilt = yield* awaitPersistedProvider(registry, newProvider.checkedAt);
          yield* rebuildBarrier.withRebuild(
            Ref.set(instances, [newInstance]).pipe(
              Effect.andThen(PubSub.publish(registryChanges, undefined)),
            ),
          );
          yield* Fiber.join(rebuilt);
          assert.strictEqual((yield* registry.getProviders)[0]?.checkedAt, newProvider.checkedAt);

          yield* PubSub.publish(oldEvents, provider("2026-07-17T00:02:00.000Z"));
          yield* Effect.yieldNow;
          assert.strictEqual((yield* registry.getProviders)[0]?.checkedAt, newProvider.checkedAt);
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect("holds the reader barrier across a finite current snapshot sync", () =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("codex_guarded_current");
        const driver = ProviderDriverKind.make("codex");
        const provider = {
          instanceId,
          driver,
          status: "ready",
          enabled: true,
          installed: true,
          auth: { status: "authenticated" },
          checkedAt: "2026-07-17T00:00:00.000Z",
          version: "1.0.0",
          models: [],
          slashCommands: [],
          skills: [],
        } as const satisfies ServerProvider;
        const currentReadStarted = yield* Deferred.make<void>();
        const releaseCurrentRead = yield* Deferred.make<void>();
        const writerStarted = yield* Deferred.make<void>();
        const registryChanges = yield* PubSub.unbounded<void>();
        const instances = yield* Ref.make<ReadonlyArray<ProviderInstance>>([]);
        const instance = makeSnapshotTestInstance({
          provider,
          getSnapshot: Deferred.succeed(currentReadStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCurrentRead)),
            Effect.as(provider),
          ),
        });
        const rebuildBarrier = yield* makeProviderRegistryRebuildBarrier;
        const instanceRegistryLayer = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            // @effect-diagnostics-next-line effectSucceedWithVoid:off - This optional result requires undefined, not the wider void type.
            getInstance: () => Effect.succeed(undefined),
            listInstances: Ref.get(instances),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.fromPubSub(registryChanges),
            subscribeChanges: PubSub.subscribe(registryChanges),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          ProviderRegistry.layer.pipe(
            Layer.provideMerge(instanceRegistryLayer),
            Layer.provideMerge(
              Layer.succeed(
                ProviderRegistryRebuildBarrier.ProviderRegistryRebuildBarrier,
                rebuildBarrier,
              ),
            ),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-guarded-current-",
              }),
            ),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          yield* rebuildBarrier.withRebuild(
            Ref.set(instances, [instance]).pipe(
              Effect.andThen(PubSub.publish(registryChanges, undefined)),
            ),
          );
          yield* Deferred.await(currentReadStarted);
          const writer = yield* rebuildBarrier
            .withRebuild(Deferred.succeed(writerStarted, undefined))
            .pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          assert.isTrue(Option.isNone(yield* Deferred.poll(writerStarted)));

          yield* Deferred.succeed(releaseCurrentRead, undefined);
          yield* Fiber.join(writer);
          assert.isTrue(Option.isSome(yield* Deferred.poll(writerStarted)));
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect(
      "a second request for a cwd being scanned gets its result, even if the first leaves",
      () =>
        Effect.gen(function* () {
          const driver = ProviderDriverKind.make("codex");
          const instanceId = ProviderInstanceId.make("codex");
          const provider = {
            instanceId,
            driver,
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt: "2026-06-10T00:00:00.000Z",
            version: "1.0.0",
            models: [],
            slashCommands: [],
            skills: [],
          } as const satisfies ServerProvider;
          const scanStarted = yield* Deferred.make<void>();
          const releaseScan = yield* Deferred.make<void>();
          const scans = yield* Ref.make(0);
          const instance: ProviderInstance = {
            instanceId,
            driverKind: driver,
            continuationIdentity: { driverKind: driver, continuationKey: "codex:instance:codex" },
            displayName: undefined,
            enabled: true,
            snapshot: {
              resolveMaintenance: () =>
                Effect.succeed(
                  makeManualOnlyProviderMaintenanceCapabilities({
                    provider: driver,
                    packageName: null,
                  }),
                ),
              getSnapshot: Effect.succeed(provider),
              refresh: Effect.succeed(provider),
              streamChanges: Stream.empty,
              applyUsageLimits: () => Effect.void,
            },
            snapshotForCwd: () =>
              Ref.update(scans, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(scanStarted, undefined)),
                Effect.andThen(Deferred.await(releaseScan)),
                Effect.as(provider),
              ),
            orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
            textGeneration: {} as ProviderInstance["textGeneration"],
          };
          const layerInstanceRegistry = Layer.succeed(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            {
              getInstance: (requestedId) =>
                Effect.succeed(requestedId === instanceId ? instance : undefined),
              listInstances: Effect.succeed([instance]),
              listUnavailable: Effect.succeed([]),
              streamChanges: Stream.empty,
              subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
            },
          );
          const scope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
          const runtimeServices = yield* Layer.build(
            layerRegistry.pipe(
              Layer.provideMerge(layerInstanceRegistry),
              Layer.provideMerge(
                ServerConfig.layerTest(process.cwd(), {
                  prefix: "t3-provider-registry-shared-workspace-scan-",
                }),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
          ).pipe(Scope.provide(scope));

          yield* Effect.gen(function* () {
            const registry = yield* ProviderRegistry.ProviderRegistry;
            const refresh = registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
            const first = yield* Effect.forkChild(refresh);
            yield* Deferred.await(scanStarted);
            const second = yield* Effect.forkChild(refresh, { startImmediately: true });
            yield* Fiber.interrupt(first);
            yield* Deferred.succeed(releaseScan, undefined);
            assert.deepStrictEqual(
              (yield* Fiber.join(second))[0]?.workspaceSnapshots?.map((snapshot) => snapshot.cwd),
              ["/workspace"],
            );
            assert.strictEqual(yield* Ref.get(scans), 1);
          }).pipe(Effect.provide(runtimeServices));
        }),
    );

    it.effect("publishes workspace skills while machine health is in error", () =>
      Effect.gen(function* () {
        const driver = ProviderDriverKind.make("codex");
        const instanceId = ProviderInstanceId.make("codex");
        const healthyProvider = {
          instanceId,
          driver,
          status: "ready",
          enabled: true,
          installed: true,
          auth: { status: "authenticated" },
          checkedAt: "2026-06-10T00:00:00.000Z",
          version: "1.0.0",
          models: [],
          slashCommands: [COMPACT_SLASH_COMMAND],
          skills: [],
        } as const satisfies ServerProvider;
        const unhealthyProvider = {
          ...healthyProvider,
          status: "error",
          auth: { status: "unknown" },
          message: "Codex app-server provider probe failed: unauthorized (401).",
          slashCommands: [],
        } as const satisfies ServerProvider;
        const firstSkills = [{ name: "first", path: "/workspace/first/SKILL.md", enabled: true }];
        const laterSkills = [{ name: "later", path: "/workspace/later/SKILL.md", enabled: true }];
        const scanResult = yield* Ref.make<ProviderWorkspaceSnapshot>({
          ...healthyProvider,
          skills: firstSkills,
        });
        const instance: ProviderInstance = {
          instanceId,
          driverKind: driver,
          continuationIdentity: { driverKind: driver, continuationKey: "codex:instance:codex" },
          displayName: undefined,
          enabled: true,
          snapshot: {
            resolveMaintenance: () =>
              Effect.succeed(
                makeManualOnlyProviderMaintenanceCapabilities({
                  provider: driver,
                  packageName: null,
                }),
              ),
            getSnapshot: Effect.succeed(unhealthyProvider),
            refresh: Effect.succeed(unhealthyProvider),
            streamChanges: Stream.empty,
            applyUsageLimits: () => Effect.void,
          },
          snapshotForCwd: () => Ref.get(scanResult),
          orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
          textGeneration: {} as ProviderInstance["textGeneration"],
        };
        const layerInstanceRegistry = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            getInstance: (requestedId) =>
              Effect.succeed(requestedId === instanceId ? instance : undefined),
            listInstances: Effect.succeed([instance]),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.empty,
            subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          layerRegistry.pipe(
            Layer.provideMerge(layerInstanceRegistry),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-unhealthy-workspace-",
              }),
            ),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const workspaceOf = Effect.map(
            registry.getProviders,
            (providers) => providers[0]?.workspaceSnapshots?.[0],
          );
          yield* registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
          assert.deepStrictEqual((yield* workspaceOf)?.slashCommands, [COMPACT_SLASH_COMMAND]);

          yield* TestClock.adjust(PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS);
          yield* Ref.set(scanResult, { ...unhealthyProvider, skills: laterSkills });
          yield* registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
          const unhealthy = yield* workspaceOf;
          assert.deepStrictEqual(unhealthy?.skills, laterSkills);
          assert.deepStrictEqual(unhealthy?.slashCommands, [COMPACT_SLASH_COMMAND]);
          assert.strictEqual(unhealthy?.slashCommandsPending, true);
          const provider = (yield* registry.getProviders)[0];
          assert.strictEqual(provider?.status, "error");
          assert.strictEqual(provider?.message, unhealthyProvider.message);

          yield* Ref.set(scanResult, { ...unhealthyProvider, skills: [] });
          yield* registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
          assert.deepStrictEqual((yield* workspaceOf)?.skills, []);

          const discoveredCommand = { name: "review", description: "Review changes" };
          yield* Ref.set(scanResult, {
            ...unhealthyProvider,
            slashCommands: [discoveredCommand],
            slashCommandsPending: false,
          });
          yield* registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
          const discovered = yield* workspaceOf;
          assert.deepStrictEqual(discovered?.slashCommands, [discoveredCommand]);
          assert.strictEqual(discovered?.slashCommandsPending, undefined);

          yield* TestClock.adjust(PROVIDER_WORKSPACE_SNAPSHOT_TTL_MS);
          yield* Ref.set(scanResult, { ...healthyProvider, skills: laterSkills });
          yield* registry.refreshWorkspaceSnapshot({ instanceId, cwd: "/workspace" });
          const recovered = yield* workspaceOf;
          assert.deepStrictEqual(recovered?.skills, laterSkills);
          assert.deepStrictEqual(recovered?.slashCommands, [COMPACT_SLASH_COMMAND]);
          assert.strictEqual(recovered?.slashCommandsPending, undefined);
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect.each([false, true])(
      "shares an untargeted refresh and guards rebuilds when callers cancel: %s",
      (cancelCallers) =>
        Effect.gen(function* () {
          const driver = ProviderDriverKind.make("codex");
          const instanceId = ProviderInstanceId.make("codex");
          const provider = {
            instanceId,
            driver,
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt: "2026-06-10T00:00:00.000Z",
            version: "1.0.0",
            models: [],
            slashCommands: [],
            skills: [],
          } as const satisfies ServerProvider;
          const refreshCalls = yield* Ref.make(0);
          const probeStarted = yield* Deferred.make<void>();
          const releaseProbe = yield* Deferred.make<void>();
          const instances = [
            {
              instanceId,
              driverKind: driver,
              continuationIdentity: {
                driverKind: driver,
                continuationKey: "codex:instance:codex",
              },
              displayName: undefined,
              enabled: true,
              snapshot: {
                resolveMaintenance: () =>
                  Effect.succeed(
                    makeManualOnlyProviderMaintenanceCapabilities({
                      provider: driver,
                      packageName: null,
                    }),
                  ),
                getSnapshot: Effect.succeed(provider),
                refresh: Effect.gen(function* () {
                  yield* Ref.update(refreshCalls, (count) => count + 1);
                  yield* Deferred.succeed(probeStarted, undefined);
                  yield* Deferred.await(releaseProbe);
                  return provider;
                }),
                streamChanges: Stream.empty,
                applyUsageLimits: () => Effect.void,
              },
              orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
              textGeneration: {} as ProviderInstance["textGeneration"],
            },
          ] satisfies ReadonlyArray<ProviderInstance>;
          const instanceRegistryLayer = Layer.succeed(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            {
              getInstance: (requestedId) =>
                Effect.succeed(instances.find((instance) => instance.instanceId === requestedId)),
              listInstances: Effect.succeed(instances),
              listUnavailable: Effect.succeed([]),
              streamChanges: Stream.empty,
              subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
            },
          );
          const scope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
          const runtimeServices = yield* Layer.build(
            ProviderRegistry.layer.pipe(
              Layer.provideMerge(instanceRegistryLayer),
              Layer.provideMerge(ProviderRegistryRebuildBarrier.layer),
              Layer.provideMerge(
                ServerConfig.layerTest(process.cwd(), {
                  prefix: "t3-provider-registry-shared-refresh-",
                }),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
          ).pipe(Scope.provide(scope));

          yield* Effect.gen(function* () {
            const registry = yield* ProviderRegistry.ProviderRegistry;
            const first = yield* registry.refresh().pipe(Effect.forkChild);
            yield* Deferred.await(probeStarted);
            const second = yield* registry.refresh().pipe(Effect.forkChild);
            yield* Effect.yieldNow;
            assert.strictEqual(yield* Ref.get(refreshCalls), 1);
            const barrier = yield* ProviderRegistryRebuildBarrier.ProviderRegistryRebuildBarrier;
            const rebuilt = yield* Deferred.make<void>();
            const rebuilding = yield* barrier
              .withRebuild(Deferred.succeed(rebuilt, undefined))
              .pipe(Effect.forkChild);
            if (cancelCallers) {
              yield* Fiber.interrupt(first);
              yield* Fiber.interrupt(second);
            }
            yield* Effect.yieldNow;
            assert.isFalse(yield* Deferred.isDone(rebuilt));
            // Even after both callers leave, a later caller joins the same pass.
            const final = yield* registry.refresh().pipe(Effect.forkChild);
            yield* Effect.yieldNow;
            assert.strictEqual(yield* Ref.get(refreshCalls), 1);
            yield* Deferred.succeed(releaseProbe, undefined);
            expect(yield* Fiber.join(final)).toEqual([expect.objectContaining(provider)]);
            if (!cancelCallers) {
              expect(yield* Fiber.join(first)).toEqual([expect.objectContaining(provider)]);
              expect(yield* Fiber.join(second)).toEqual([expect.objectContaining(provider)]);
            }
            yield* Fiber.join(rebuilding);
            assert.strictEqual(yield* Ref.get(refreshCalls), 1);

            yield* registry.refresh();
            assert.strictEqual(yield* Ref.get(refreshCalls), 2);
          }).pipe(Effect.provide(runtimeServices));
        }),
    );

    it.effect("persists the merged snapshot when a live update has empty models", () =>
      Effect.gen(function* () {
        const cursorDriver = ProviderDriverKind.make("cursor");
        const cursorInstanceId = ProviderInstanceId.make("cursor");
        const initialProvider = {
          instanceId: cursorInstanceId,
          driver: cursorDriver,
          status: "ready",
          enabled: true,
          installed: true,
          auth: { status: "authenticated" },
          checkedAt: "2026-04-14T00:00:00.000Z",
          version: "2026.04.09-f2b0fcd",
          models: [
            {
              slug: "claude-opus-4-6",
              name: "Opus 4.6",
              isCustom: false,
              capabilities: createModelCapabilities({
                optionDescriptors: [
                  selectDescriptor("reasoning", "Reasoning", [
                    { id: "high", label: "High", isDefault: true },
                  ]),
                ],
              }),
            },
          ],
          slashCommands: [],
          skills: [],
        } as const satisfies ServerProvider;
        const refreshedProvider = {
          ...initialProvider,
          checkedAt: "2026-04-14T00:01:00.000Z",
          models: [],
        } satisfies ServerProvider;
        const changes = yield* PubSub.unbounded<ServerProvider>();
        const instance = {
          instanceId: cursorInstanceId,
          driverKind: cursorDriver,
          continuationIdentity: {
            driverKind: cursorDriver,
            continuationKey: "cursor:instance:cursor",
          },
          displayName: undefined,
          enabled: true,
          snapshot: {
            resolveMaintenance: () =>
              Effect.succeed(
                makeManualOnlyProviderMaintenanceCapabilities({
                  provider: cursorDriver,
                  packageName: null,
                }),
              ),
            getSnapshot: Effect.succeed(initialProvider),
            refresh: Effect.succeed(refreshedProvider),
            streamChanges: Stream.fromPubSub(changes),
            applyUsageLimits: () => Effect.void,
          },
          orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
          textGeneration: {} as ProviderInstance["textGeneration"],
        } satisfies ProviderInstance;
        const instanceRegistryLayer = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            getInstance: (instanceId) =>
              Effect.succeed(instanceId === cursorInstanceId ? instance : undefined),
            listInstances: Effect.succeed([instance]),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.empty,
            subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
              PubSub.subscribe(pubsub),
            ),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          layerRegistry.pipe(
            Layer.provideMerge(instanceRegistryLayer),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-merged-persist-",
              }),
            ),
            Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const config = yield* ServerConfig.ServerConfig;
          const filePath = yield* resolveProviderStatusCachePath({
            cacheDir: config.providerStatusCacheDir,
            instanceId: cursorInstanceId,
          });

          assert.deepStrictEqual((yield* registry.getProviders)[0]?.models, [
            ...initialProvider.models,
          ]);
          const persisted = yield* awaitPersistedProvider(registry, refreshedProvider.checkedAt);
          yield* PubSub.publish(changes, refreshedProvider);
          yield* Fiber.join(persisted);
          const cachedProvider = yield* readProviderStatusCache(filePath);

          assert.deepStrictEqual(
            cachedProvider,
            withBundledCompatibility({
              ...refreshedProvider,
              models: [...initialProvider.models],
            }),
          );
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect(
      "persists authoritative OpenCode removals without resurrecting them on a failed live refresh",
      () =>
        Effect.gen(function* () {
          const openCodeDriver = ProviderDriverKind.make("opencode");
          const openCodeInstanceId = ProviderInstanceId.make("opencode");
          const initialProvider = {
            instanceId: openCodeInstanceId,
            driver: openCodeDriver,
            status: "ready",
            enabled: true,
            installed: true,
            auth: { status: "authenticated" },
            checkedAt: "2026-07-17T00:00:00.000Z",
            version: "1.0.0",
            models: [
              {
                slug: "github/gpt-5",
                name: "GPT-5",
                subProvider: "GitHub",
                isCustom: false,
                capabilities: null,
              },
              {
                slug: "removed-plugin/model",
                name: "Removed Plugin Model",
                subProvider: "Removed Plugin",
                isCustom: false,
                capabilities: null,
              },
            ],
            slashCommands: [],
            skills: [],
          } as const satisfies ServerProvider;
          const authoritativeProvider = {
            ...initialProvider,
            checkedAt: "2026-07-17T00:01:00.000Z",
            models: [initialProvider.models[0]!],
          } satisfies ServerProvider;
          const failedProvider = {
            ...authoritativeProvider,
            status: "error",
            auth: { status: "unknown" },
            checkedAt: "2026-07-17T00:02:00.000Z",
            models: [],
            message: "Failed to refresh OpenCode models.",
          } satisfies ServerProvider;
          const changes = yield* PubSub.unbounded<ServerProvider>();
          const instance = {
            instanceId: openCodeInstanceId,
            driverKind: openCodeDriver,
            continuationIdentity: {
              driverKind: openCodeDriver,
              continuationKey: "opencode:instance:opencode",
            },
            displayName: undefined,
            enabled: true,
            snapshot: {
              resolveMaintenance: () =>
                Effect.succeed(
                  makeManualOnlyProviderMaintenanceCapabilities({
                    provider: openCodeDriver,
                    packageName: null,
                  }),
                ),
              getSnapshot: Effect.succeed(initialProvider),
              refresh: Effect.succeed(authoritativeProvider),
              streamChanges: Stream.fromPubSub(changes),
              applyUsageLimits: () => Effect.void,
            },
            orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
            textGeneration: {} as ProviderInstance["textGeneration"],
          } satisfies ProviderInstance;
          const instanceRegistryLayer = Layer.succeed(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            {
              getInstance: (instanceId) =>
                Effect.succeed(instanceId === openCodeInstanceId ? instance : undefined),
              listInstances: Effect.succeed([instance]),
              listUnavailable: Effect.succeed([]),
              streamChanges: Stream.empty,
              subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
                PubSub.subscribe(pubsub),
              ),
            },
          );
          const scope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
          const runtimeServices = yield* Layer.build(
            layerRegistry.pipe(
              Layer.provideMerge(instanceRegistryLayer),
              Layer.provideMerge(
                ServerConfig.layerTest(process.cwd(), {
                  prefix: "t3-provider-registry-opencode-authoritative-persist-",
                }),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
          ).pipe(Scope.provide(scope));

          yield* Effect.gen(function* () {
            const registry = yield* ProviderRegistry.ProviderRegistry;
            const config = yield* ServerConfig.ServerConfig;
            const filePath = yield* resolveProviderStatusCachePath({
              cacheDir: config.providerStatusCacheDir,
              instanceId: openCodeInstanceId,
            });

            const authoritativePersisted = yield* awaitPersistedProvider(
              registry,
              authoritativeProvider.checkedAt,
            );
            yield* PubSub.publish(changes, authoritativeProvider);
            yield* Fiber.join(authoritativePersisted);
            let cachedProvider = yield* readProviderStatusCache(filePath);

            assert.deepStrictEqual(cachedProvider?.models, [authoritativeProvider.models[0]!]);

            const failedPersisted = yield* awaitPersistedProvider(
              registry,
              failedProvider.checkedAt,
            );
            yield* PubSub.publish(changes, failedProvider);
            yield* Fiber.join(failedPersisted);
            cachedProvider = yield* readProviderStatusCache(filePath);

            assert.deepStrictEqual(cachedProvider?.models, [authoritativeProvider.models[0]!]);
            assert.deepStrictEqual((yield* registry.getProviders)[0]?.models, [
              authoritativeProvider.models[0]!,
            ]);
          }).pipe(Effect.provide(runtimeServices));
        }),
    );

    it.effect("returns the cached provider list when a manual refresh fails", () =>
      Effect.gen(function* () {
        const codexDriver = ProviderDriverKind.make("codex");
        const codexInstanceId = ProviderInstanceId.make("codex");
        const cachedProvider = {
          instanceId: codexInstanceId,
          driver: codexDriver,
          status: "ready",
          enabled: true,
          installed: true,
          auth: { status: "authenticated" },
          checkedAt: "2026-04-29T10:00:00.000Z",
          version: "1.0.0",
          models: [],
          slashCommands: [],
          skills: [],
        } as const satisfies ServerProvider;
        const instance = {
          instanceId: codexInstanceId,
          driverKind: codexDriver,
          continuationIdentity: {
            driverKind: codexDriver,
            continuationKey: "codex:instance:codex",
          },
          displayName: undefined,
          enabled: true,
          snapshot: {
            resolveMaintenance: () =>
              Effect.succeed(
                makeManualOnlyProviderMaintenanceCapabilities({
                  provider: codexDriver,
                  packageName: null,
                }),
              ),
            getSnapshot: Effect.succeed(cachedProvider),
            refresh: Effect.die(new Error("simulated refresh failure")),
            streamChanges: Stream.empty,
            applyUsageLimits: () => Effect.void,
          },
          orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
          textGeneration: {} as ProviderInstance["textGeneration"],
        } satisfies ProviderInstance;
        const instanceRegistryLayer = Layer.succeed(
          ProviderInstanceRegistry.ProviderInstanceRegistry,
          {
            getInstance: (instanceId) =>
              Effect.succeed(instanceId === codexInstanceId ? instance : undefined),
            listInstances: Effect.succeed([instance]),
            listUnavailable: Effect.succeed([]),
            streamChanges: Stream.empty,
            subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
              PubSub.subscribe(pubsub),
            ),
          },
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const runtimeServices = yield* Layer.build(
          layerRegistry.pipe(
            Layer.provideMerge(instanceRegistryLayer),
            Layer.provideMerge(
              ServerConfig.layerTest(process.cwd(), {
                prefix: "t3-provider-registry-refresh-failure-",
              }),
            ),
            Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
            Layer.provideMerge(NodeServices.layer),
          ),
        ).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;

          assert.deepStrictEqual(yield* registry.getProviders, [
            withBundledCompatibility(cachedProvider),
          ]);
          assert.deepStrictEqual(yield* registry.refresh(codexDriver), [
            withBundledCompatibility(cachedProvider),
          ]);
          assert.deepStrictEqual(yield* registry.refreshInstance(codexInstanceId), [
            withBundledCompatibility(cachedProvider),
          ]);
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect("re-probes when settings change the codex binaryPath", () =>
      Effect.gen(function* () {
        const firstMissing = `t3code_codex_first_`;
        const secondMissing = `t3code_codex_second_`;
        const spawnedCommands: Array<string> = [];
        const secondProbeStarted = yield* Deferred.make<void>();
        const releaseSecondProbe = yield* Deferred.make<void>();
        const serverSettings = yield* makeMutableServerSettingsService(
          decodeServerSettings(
            deepMerge(encodedDefaultServerSettings, {
              providerInstances: {
                ...disabledDefaultSlots("claudeAgent", "cursor", "grok", "opencode"),
                [ProviderInstanceId.make("codex")]: {
                  driver: ProviderDriverKind.make("codex"),
                  enabled: true,
                  config: { binaryPath: firstMissing },
                },
              },
            }),
          ),
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const layerProviderRegistry = layerRegistry.pipe(
          Layer.provideMerge(
            ProviderInstanceRegistryHydrationTestLive.pipe(
              Layer.provide(ClaudeSessionStoreTestLive),
              Layer.provideMerge(ProviderLatestVersions.layer),
              Layer.provideMerge(McpProviderSessions.layer),
            ),
          ),
          Layer.provideMerge(AntigravityInstallation.AntigravityInstallation.layer),
          Layer.provideMerge(
            Layer.succeed(ServerSettingsModule.ServerSettingsService, serverSettings),
          ),
          Layer.provideMerge(ServerSecretStore.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), {
              prefix: "t3-provider-registry-",
            }),
          ),
          Layer.provideMerge(layerTestHttpClient),
          Layer.provideMerge(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
          Layer.provideMerge(ProviderLatestVersions.layer),
          Layer.provideMerge(McpProviderSessions.layer),
          Layer.provideMerge(ModelManifest.layerTest),
          Layer.provideMerge(ResetCreditCoordinator.layerTest),
          Layer.provideMerge(
            OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ),
          Layer.updateService(ChildProcessSpawner.ChildProcessSpawner, (spawner) =>
            ChildProcessSpawner.make((command) => {
              if (command._tag !== "StandardCommand") return spawner.spawn(command);
              spawnedCommands.push(command.command);
              const beforeSpawn =
                command.command === secondMissing
                  ? Deferred.succeed(secondProbeStarted, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseSecondProbe)),
                    )
                  : Effect.void;
              return beforeSpawn.pipe(Effect.andThen(spawner.spawn(command)));
            }),
          ),
          Layer.provideMerge(NodeServices.layer),
          Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
        );
        const runtimeServices = yield* Layer.build(layerProviderRegistry).pipe(
          Scope.provide(scope),
        );

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const codexSnapshots = registry.streamChanges.pipe(
            Stream.map((providers) =>
              providers.find((provider) => provider.instanceId === "codex"),
            ),
            Stream.filter((provider): provider is ServerProvider => provider !== undefined),
          );
          const firstError = yield* Stream.toPull(
            codexSnapshots.pipe(Stream.filter((provider) => provider.status === "error")),
          );
          const currentCodex = (yield* registry.getProviders).find(
            (provider) => provider.instanceId === "codex",
          );
          const initialCodex =
            currentCodex?.status === "error" ? currentCodex : (yield* firstError)[0];
          assert.strictEqual(initialCodex?.status, "error");
          assert.strictEqual(initialCodex?.installed, false);
          assert.deepStrictEqual(spawnedCommands, [firstMissing]);

          const pendingRebuild = yield* Stream.toPull(
            codexSnapshots.pipe(
              Stream.filter((provider) => provider.status === "warning" && !provider.installed),
            ),
          );
          yield* serverSettings.updateSettings({
            providerInstances: {
              ...disabledDefaultSlots("claudeAgent", "cursor", "grok", "opencode"),
              [ProviderInstanceId.make("codex")]: {
                driver: ProviderDriverKind.make("codex"),
                enabled: true,
                config: { binaryPath: secondMissing },
              },
            },
          });
          // Hydration acquires `subscribeChanges` during layer construction.
          // Let its watcher publish the desired version, then deterministically
          // wake the coalescing reconcile worker.
          yield* Effect.yieldNow;
          yield* TestClock.adjust("100 millis");
          yield* Effect.yieldNow;

          yield* Deferred.await(secondProbeStarted);
          yield* pendingRebuild;
          const rebuiltError = yield* Stream.toPull(
            codexSnapshots.pipe(Stream.filter((provider) => provider.status === "error")),
          );
          yield* Deferred.succeed(releaseSecondProbe, undefined);
          const [reprobedCodex] = yield* rebuiltError;
          assert.deepStrictEqual(spawnedCommands, [firstMissing, secondMissing]);
          assert.strictEqual(reprobedCodex?.status, "error");
          assert.strictEqual(reprobedCodex?.installed, false);
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect("keeps Claude live and adds continuation sync when the global gate is enabled", () =>
      Effect.gen(function* () {
        const serverSettings = yield* makeMutableServerSettingsService(
          decodeServerSettings(
            deepMerge(encodedDefaultServerSettings, {
              claudeCrossAccountContinuationEnabled: false,
              providerInstances: disabledDefaultSlots(
                "codex",
                "claudeAgent",
                "cursor",
                "grok",
                "opencode",
              ),
            }),
          ),
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const hydrationLayer = ProviderInstanceRegistryHydrationTestLive.pipe(
          Layer.provide(ClaudeSessionStoreTestLive),
          Layer.provideMerge(ProviderLatestVersions.layer),
          Layer.provideMerge(McpProviderSessions.layer),
          Layer.provideMerge(AntigravityInstallation.AntigravityInstallation.layer),
          Layer.provideMerge(
            Layer.succeed(ServerSettingsModule.ServerSettingsService, serverSettings),
          ),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), {
              prefix: "t3-provider-registry-claude-continuation-",
            }),
          ),
          Layer.provideMerge(layerTestHttpClient),
          Layer.provideMerge(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
          Layer.provideMerge(ModelManifest.layerTest),
          Layer.provideMerge(ResetCreditCoordinator.layerTest),
          Layer.provideMerge(
            OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ),
          Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
          Layer.provideMerge(NodeServices.layer),
        );
        const runtimeServices = yield* Layer.build(hydrationLayer).pipe(Scope.provide(scope));

        yield* Effect.gen(function* () {
          const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
          const claudeInstanceId = ProviderInstanceId.make("claudeAgent");
          const before = yield* registry.getInstance(claudeInstanceId);
          assert.notStrictEqual(before, undefined);
          const disabledSync = yield* before!.orchestrationAdapter.syncContinuation!({
            nativeThreadId: "disabled-sync",
            cwd: "/workspace",
          }).pipe(Effect.flip);
          assert.equal(disabledSync.code, "feature-disabled");
          const changes = yield* registry.subscribeChanges;

          yield* Effect.yieldNow;
          yield* serverSettings.updateSettings({
            claudeCrossAccountContinuationEnabled: true,
          });
          yield* TestClock.adjust("100 millis");
          yield* PubSub.take(changes);
          const after = yield* registry.getInstance(claudeInstanceId);
          assert.notStrictEqual(after, before);
          assert.strictEqual(typeof after?.orchestrationAdapter.syncContinuation, "function");
          assert.strictEqual(
            (yield* registry.listUnavailable).some(
              (provider) => provider.instanceId === claudeInstanceId,
            ),
            false,
          );
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    it.effect("includes unavailable instance snapshots in getProviders", () =>
      Effect.gen(function* () {
        const serverSettings = yield* makeMutableServerSettingsService(
          decodeServerSettings(
            deepMerge(encodedDefaultServerSettings, {
              providerInstances: {
                ...disabledDefaultSlots("codex", "claudeAgent", "cursor", "grok", "opencode"),
                ghost_main: {
                  driver: "ghostDriver",
                  displayName: "A fork-only driver we don't ship",
                  enabled: false,
                  config: { arbitrary: "payload" },
                },
              } as unknown as ContractServerSettings["providerInstances"],
            }),
          ),
        );
        const scope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const layerProviderRegistry = layerRegistry.pipe(
          Layer.provideMerge(
            ProviderInstanceRegistryHydrationTestLive.pipe(
              Layer.provide(ClaudeSessionStoreTestLive),
              Layer.provideMerge(ProviderLatestVersions.layer),
              Layer.provideMerge(McpProviderSessions.layer),
            ),
          ),
          Layer.provideMerge(AntigravityInstallation.AntigravityInstallation.layer),
          Layer.provideMerge(
            Layer.succeed(ServerSettingsModule.ServerSettingsService, serverSettings),
          ),
          Layer.provideMerge(ServerSecretStore.layer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), {
              prefix: "t3-provider-registry-",
            }),
          ),
          Layer.provideMerge(layerTestHttpClient),
          Layer.provideMerge(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
          Layer.provideMerge(ProviderLatestVersions.layer),
          Layer.provideMerge(McpProviderSessions.layer),
          Layer.provideMerge(ModelManifest.layerTest),
          Layer.provideMerge(ResetCreditCoordinator.layerTest),
          Layer.provideMerge(
            OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
          ),
          Layer.provideMerge(NodeServices.layer),
          Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
        );
        const runtimeServices = yield* Layer.build(layerProviderRegistry).pipe(
          Scope.provide(scope),
        );

        yield* Effect.gen(function* () {
          const registry = yield* ProviderRegistry.ProviderRegistry;
          const providers = yield* registry.getProviders;
          const ghost = providers.find((provider) => provider.instanceId === "ghost_main");

          assert.notStrictEqual(ghost, undefined);
          assert.strictEqual(ghost?.driver, "ghostDriver");
          assert.strictEqual(ghost?.availability, "unavailable");
          assert.strictEqual(Object.hasOwn(ghost ?? {}, "setup"), false);
          assert.match(ghost?.unavailableReason ?? "", /ghostDriver/);
        }).pipe(Effect.provide(runtimeServices));
      }),
    );

    const startupSettings = (instanceId?: string) =>
      decodeServerSettings({
        ...encodedDefaultServerSettings,
        providerInstances: {
          ...disabledDefaultSlots(
            "codex",
            "claudeAgent",
            "cursor",
            "grok",
            "opencode",
            "antigravity",
            "pi",
            "muse",
          ),
          ...(instanceId ? { [instanceId]: { driver: "ghostDriver" } } : {}),
        },
      });

    const hydrateDuringSettingsChange = Effect.fnUntraced(function* (input: {
      seed: ContractServerSettings;
      saved: ContractServerSettings;
      current: ContractServerSettings;
      failCurrentRead?: boolean;
    }) {
      const changes = yield* PubSub.unbounded<ContractServerSettings | undefined>();
      const consume = yield* Deferred.make<void>();
      const drained: Array<Deferred.Deferred<void>> = [];
      let readSeed = true;
      let failCurrentRead = false;
      const settings = yield* makeMutableServerSettingsService(input.current);
      const serverSettings = {
        ...settings,
        getSettings: Effect.suspend(() => {
          if (!readSeed) {
            readSeed = true;
            return PubSub.publish(changes, input.saved).pipe(Effect.as(input.seed));
          }
          return failCurrentRead
            ? Effect.fail(
                new ServerSettingsError({
                  settingsPath: "<memory>",
                  operation: "read-secret",
                  cause: new Error("secret materialization failed"),
                }),
              )
            : settings.getSettings;
        }),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.gen(function* () {
          const subscription = yield* PubSub.subscribe(changes);
          const done = yield* Deferred.make<void>();
          drained.push(done);
          // The terminal marker acknowledges preceding reconciles, including no-op/failed reads.
          return Stream.fromEffectRepeat(
            Deferred.await(consume).pipe(Effect.andThen(PubSub.take(subscription))),
          ).pipe(
            Stream.takeWhile((next): next is ContractServerSettings => next !== undefined),
            Stream.ensuring(Deferred.succeed(done, undefined)),
          );
        }),
      } satisfies ServerSettingsModule.ServerSettingsService["Service"];
      const dependencies = yield* Layer.build(
        Layer.mergeAll(
          AntigravityInstallation.AntigravityInstallation.layer,
          ServerSecretStore.layer,
          OpenCodeRuntime.layer.pipe(Layer.provide(OpenCodeServerLedger.layerTest)),
        ).pipe(
          Layer.provideMerge(
            Layer.succeed(ServerSettingsModule.ServerSettingsService, serverSettings),
          ),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-provider-registry-" }),
          ),
          Layer.provideMerge(layerTestHttpClient),
          Layer.provideMerge(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
          Layer.provideMerge(ProviderLatestVersions.layer),
          Layer.provideMerge(McpProviderSessions.layer),
          Layer.provideMerge(ResetCreditCoordinator.layerTest),
          Layer.provideMerge(layerBackgroundPolicyAlwaysRun),
        ),
      );
      readSeed = false;
      const services = yield* Layer.build(ProviderInstanceRegistryHydrationTestLive).pipe(
        Effect.provide(dependencies),
      );
      failCurrentRead = input.failCurrentRead ?? false;
      yield* PubSub.publish(changes, undefined);
      yield* Deferred.succeed(consume, undefined);
      yield* Effect.forEach(drained, Deferred.await);
      yield* TestClock.adjust("100 millis");
      const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry.pipe(
        Effect.provide(services),
      );
      return (yield* registry.listUnavailable).map(
        (provider): string | undefined => provider.instanceId,
      );
    });

    it.effect("applies a settings change saved while the provider registry starts", () =>
      Effect.gen(function* () {
        const saved = startupSettings("ghost_late");
        const unavailable = yield* hydrateDuringSettingsChange({
          seed: startupSettings(),
          saved,
          current: saved,
        });
        assert.include(unavailable, "ghost_late");
      }),
    );

    it.effect("does not apply a queued settings change older than the registry seed", () =>
      Effect.gen(function* () {
        const seed = startupSettings("ghost_current");
        const unavailable = yield* hydrateDuringSettingsChange({
          seed,
          saved: startupSettings("ghost_stale"),
          current: seed,
        });
        assert.include(unavailable, "ghost_current");
        assert.notInclude(unavailable, "ghost_stale");
      }),
    );

    it.effect(
      "retains the newer registry seed when a queued stale change cannot read current settings",
      () =>
        Effect.gen(function* () {
          const seed = startupSettings("ghost_current");
          const unavailable = yield* hydrateDuringSettingsChange({
            seed,
            saved: startupSettings("ghost_stale"),
            current: seed,
            failCurrentRead: true,
          });
          assert.include(unavailable, "ghost_current");
          assert.notInclude(unavailable, "ghost_stale");
        }),
    );
  });

  describe("checkClaudeProviderStatus", () => {
    it.effect("reports a logged-out CLI as unauthenticated", () =>
      Effect.gen(function* () {
        const status = yield* checkClaudeProviderStatus(
          defaultClaudeSettings,
          claudeCapabilities({
            tokenSource: "none",
            apiKeySource: "none",
            apiProvider: "firstParty",
          }),
          { ANTHROPIC_BASE_URL: "" },
        );
        assert.strictEqual(status.status, "error");
        assert.strictEqual(status.auth.status, "unauthenticated");
      }).pipe(
        Effect.provide(
          layerMockSpawner((args) => {
            const joined = args.join(" ");
            if (joined === "--version") return { stdout: "1.0.0\n", stderr: "", code: 0 };
            throw new Error(`Unexpected args: ${joined}`);
          }),
        ),
      ),
    );

    it.effect.each([
      { source: "inherited HTTPS endpoint", baseUrl: "https://proxy.example/api", overrides: [] },
      {
        source: "instance HTTP endpoint overriding Anthropic",
        baseUrl: "https://api.anthropic.com",
        overrides: [
          { name: "ANTHROPIC_BASE_URL", value: "http://localhost:8080", sensitive: false },
        ],
      },
    ])("keeps Claude ready with an empty first-party account: $source", ({ baseUrl, overrides }) =>
      Effect.gen(function* () {
        const environment = yield* mergeProviderInstanceEnvironment(overrides, {
          ANTHROPIC_BASE_URL: baseUrl,
        });
        const status = yield* checkClaudeProviderStatus(
          defaultClaudeSettings,
          claudeCapabilities({
            tokenSource: "none",
            apiKeySource: "none",
            apiProvider: "firstParty",
          }),
          environment,
        );
        assert.strictEqual(status.status, "ready");
        assert.strictEqual(status.auth.status, "authenticated");
        assert.strictEqual(status.auth.type, "customEndpoint");
        assert.strictEqual(status.auth.label, "Custom API endpoint");
      }).pipe(
        Effect.provide(
          layerMockSpawner((args) => {
            const joined = args.join(" ");
            if (joined === "--version") return { stdout: "1.0.0\n", stderr: "", code: 0 };
            throw new Error(`Unexpected args: ${joined}`);
          }),
        ),
      ),
    );

    it.effect.each([
      "https://api.anthropic.com",
      "https://api.anthropic.com:443",
      "https://api.anthropic.com:8443",
      "openrouter.ai/api",
      "localhost:8080",
      "ftp://proxy.example",
    ])("keeps Claude logged out when the instance endpoint is %s", (baseUrl) =>
      Effect.gen(function* () {
        const environment = yield* mergeProviderInstanceEnvironment(
          [{ name: "ANTHROPIC_BASE_URL", value: baseUrl, sensitive: false }],
          { ANTHROPIC_BASE_URL: "https://proxy.example/api" },
        );
        const status = yield* checkClaudeProviderStatus(
          defaultClaudeSettings,
          claudeCapabilities({
            tokenSource: "none",
            apiKeySource: "none",
            apiProvider: "firstParty",
          }),
          environment,
        );
        assert.strictEqual(status.status, "error");
        assert.strictEqual(status.auth.status, "unauthenticated");
        assert.strictEqual(status.auth.type, undefined);
      }).pipe(
        Effect.provide(
          layerMockSpawner((args) => {
            const joined = args.join(" ");
            if (joined === "--version") return { stdout: "1.0.0\n", stderr: "", code: 0 };
            throw new Error(`Unexpected args: ${joined}`);
          }),
        ),
      ),
    );

    it.effect("keeps an API key install authenticated when it reports no token source", () =>
      Effect.gen(function* () {
        const status = yield* checkClaudeProviderStatus(
          defaultClaudeSettings,
          claudeCapabilities({
            tokenSource: "none",
            apiKeySource: "ANTHROPIC_API_KEY",
            apiProvider: "firstParty",
          }),
        );
        assert.strictEqual(status.status, "ready");
        assert.strictEqual(status.auth.status, "authenticated");
      }).pipe(
        Effect.provide(
          layerMockSpawner((args) => {
            const joined = args.join(" ");
            if (joined === "--version") return { stdout: "1.0.0\n", stderr: "", code: 0 };
            throw new Error(`Unexpected args: ${joined}`);
          }),
        ),
      ),
    );

    it.effect("keeps a CLI that says nothing about its account authenticated", () =>
      Effect.gen(function* () {
        const status = yield* checkClaudeProviderStatus(
          defaultClaudeSettings,
          claudeCapabilities(),
        );
        assert.strictEqual(status.status, "ready");
        assert.strictEqual(status.auth.status, "authenticated");
      }).pipe(
        Effect.provide(
          layerMockSpawner((args) => {
            const joined = args.join(" ");
            if (joined === "--version") return { stdout: "1.0.0\n", stderr: "", code: 0 };
            throw new Error(`Unexpected args: ${joined}`);
          }),
        ),
      ),
    );
  });
});
