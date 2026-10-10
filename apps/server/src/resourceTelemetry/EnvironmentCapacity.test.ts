import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  EnvironmentCapacityReport,
  EnvironmentId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2ThreadShell,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";
import { McpSchema, McpServer } from "effect/ai";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ThreadCommandExecutor from "../orchestration-v2/ThreadCommandExecutor.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Settings from "../serverSettings.ts";
import * as EnvironmentCapacity from "./EnvironmentCapacity.ts";
import * as HostResources from "./HostResources.ts";

const now = Date.parse("2026-10-03T12:00:00.000Z");
const iso = (offsetMs = 0) => DateTime.formatIso(DateTime.makeUnsafe(now + offsetMs));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeReport = Schema.decodeUnknownEffect(EnvironmentCapacityReport);
const environmentId = EnvironmentId.make("capacity-environment");
const instanceId = ProviderInstanceId.make("codex");
const callerId = ThreadId.make("capacity-caller");
const descriptor = {
  environmentId,
  label: "Test machine",
  platform: { os: "linux" as const, arch: "x64" as const },
  serverVersion: "test",
  capabilities: { repositoryIdentity: false },
};

function thread(overrides: Partial<OrchestrationV2ThreadShell> = {}): OrchestrationV2ThreadShell {
  return {
    id: callerId,
    projectId: ProjectId.make("private-project"),
    title: "private-title",
    providerInstanceId: instanceId,
    modelSelection: { instanceId, model: "test-model" },
    runtimeMode: "approval-required",
    interactionMode: "plan",
    createdBy: "user",
    creationSource: "web",
    branch: null,
    worktreePath: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: callerId },
    forkedFrom: null,
    activeProviderThreadId: null,
    latestRunId: null,
    activeRunId: null,
    activityRunStatus: null,
    status: "idle",
    pendingRuntimeRequest: null,
    latestVisibleMessage: null,
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    pendingBackgroundTasks: [],
    providerInstanceHistory: [],
    itemCount: 0,
    visibleItemCount: 0,
    createdAt: DateTime.makeUnsafe(now),
    updatedAt: DateTime.makeUnsafe(now),
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    deletedAt: null,
    ...overrides,
  };
}

function provider(overrides: Partial<ServerProvider> = {}): ServerProvider {
  return {
    instanceId,
    driver: ProviderDriverKind.make("codex"),
    enabled: true,
    installed: true,
    version: "test",
    status: "ready",
    auth: { status: "authenticated", email: "private@example.test" },
    checkedAt: iso(),
    models: [],
    slashCommands: [],
    skills: [],
    usageLimits: {
      checkedAt: iso(),
      credentialFingerprint: "private-fingerprint",
      resetCredits: { availableCount: 1, nextCreditId: "private-credit-id" },
      windows: [{ id: "weekly", label: "Weekly", kind: "weekly", usedPercent: 30.5 }],
    },
    ...overrides,
  };
}

function dependencies(
  options: {
    threads?: readonly OrchestrationV2ThreadShell[];
    providers?: readonly ServerProvider[];
    sampledAt?: number;
    cpuUtilization?: number | null;
    failWorkload?: boolean;
  } = {},
) {
  return Layer.mergeAll(
    NodeCrypto.layer,
    Layer.succeed(ServerEnvironment.ServerEnvironment, {
      getEnvironmentId: Effect.succeed(environmentId),
      getDescriptor: Effect.succeed(descriptor),
    }),
    Layer.succeed(HostResources.HostResources, {
      read: Effect.succeed({
        sampledAt: options.sampledAt ?? now,
        cpuUtilization: options.cpuUtilization === undefined ? 0.25 : options.cpuUtilization,
        cpuCount: 8,
        availableMemoryBytes: 8_000_000_000,
        totalMemoryBytes: 16_000_000_000,
      }),
    }),
    Layer.mock(ProviderRegistry.ProviderRegistry)({
      getProviders: Effect.succeed(options.providers ?? [provider()]),
      // A refresh is deliberately not implemented: capacity must only read snapshots.
    }),
    Layer.mock(Orchestrator.OrchestratorV2)({
      getShellSnapshot: (input) => {
        expect(input).toEqual({ location: "active" });
        return options.failWorkload
          ? Effect.fail(
              new Orchestrator.OrchestratorProjectionError({
                threadId: callerId,
                cause: "private-db-path",
              }),
            )
          : Effect.succeed({
              schemaVersion: 1,
              snapshotSequence: 42,
              threads: options.threads ?? [thread()],
              archivedThreads: [thread({ archivedAt: DateTime.makeUnsafe(now) })],
            });
      },
    }),
  );
}

const read = Effect.gen(function* () {
  yield* TestClock.setTime(now);
  const capacity = yield* EnvironmentCapacity.EnvironmentCapacity;
  const result = yield* capacity.read;
  return yield* decodeReport(result);
});

it.effect(
  "reports zero workload for an empty environment without treating missing quotas as unlimited",
  () =>
    Effect.gen(function* () {
      const report = yield* read;
      expect(Object.values(report.workload.threads).every((count) => count === 0)).toBe(true);
      expect(Object.values(report.workload.nativeSubagents).every((count) => count === 0)).toBe(
        true,
      );
      expect(report.workload.byProviderInstance).toEqual([]);
      expect(report.providers).toEqual([]);
    }).pipe(
      Effect.provide(
        EnvironmentCapacity.layer.pipe(Layer.provide(dependencies({ threads: [], providers: [] }))),
      ),
    ),
);

it.effect(
  "reports resources and cached quotas without exposing provider credentials or conversations",
  () =>
    Effect.gen(function* () {
      const report = yield* read;
      expect(report.environment).toEqual({
        environmentId,
        label: descriptor.label,
        platform: descriptor.platform,
      });
      expect(report.host).toMatchObject({
        cpuUtilization: 0.25,
        cpuCount: 8,
        ageMs: 0,
        stale: false,
      });
      expect(report.providers[0]?.usageLimits?.windows[0]).toMatchObject({
        usedPercent: 30.5,
        remainingPercent: 69.5,
      });
      expect(report.workload.snapshotSequence).toBe(42);
      expect(encodeJson(report)).not.toContain("private-");
      expect(encodeJson(report)).not.toContain("private@example.test");
    }).pipe(Effect.provide(EnvironmentCapacity.layer.pipe(Layer.provide(dependencies())))),
);

it.effect(
  "counts activity across projects, separating native children and excluding archived/deleted threads",
  () =>
    Effect.gen(function* () {
      const report = yield* read;
      expect(report.workload.threads).toMatchObject({
        running: 2,
        waiting: 1,
        starting: 1,
        preparing: 1,
        queued: 1,
        completed: 1,
      });
      expect(report.workload.nativeSubagents.running).toBe(1);
      expect(report.workload.waitingForBackgroundThreads).toBe(1);
      expect(
        report.workload.byProviderInstance.find((row) => row.providerInstanceId === "removed")
          ?.threads.running,
      ).toBe(1);
      expect(report.providers.map((row) => row.providerInstanceId)).toEqual([instanceId]);
    }).pipe(
      Effect.provide(
        EnvironmentCapacity.layer.pipe(
          Layer.provide(
            dependencies({
              threads: [
                thread({ status: "queued", activityRunStatus: "running" }),
                thread({ projectId: ProjectId.make("other-project"), status: "waiting" }),
                thread({ status: "starting" }),
                thread({ status: "preparing" }),
                thread({ status: "queued" }),
                thread({
                  providerInstanceId: ProviderInstanceId.make("removed"),
                  status: "running",
                  creationSource: "mcp",
                  lineage: {
                    rootThreadId: callerId,
                    parentThreadId: callerId,
                    relationshipToParent: "subagent",
                  },
                }),
                thread({
                  status: "running",
                  creationSource: "provider",
                  lineage: {
                    rootThreadId: callerId,
                    parentThreadId: callerId,
                    relationshipToParent: "subagent",
                  },
                }),
                thread({
                  status: "completed",
                  pendingBackgroundTasks: [{ kind: "subagent", taskId: "child" }],
                }),
                thread({ status: "running", archivedAt: DateTime.makeUnsafe(now) }),
                thread({ status: "running", deletedAt: DateTime.makeUnsafe(now) }),
              ],
            }),
          ),
        ),
      ),
    ),
);

it.effect("keeps shared allowance identity separate from launch instance identity", () =>
  Effect.gen(function* () {
    const report = yield* read;
    const groups = report.providers.map((row) => row.quotaGroupId);
    expect(groups[0]).toBe(groups[1]);
    expect(groups[0]).not.toBe(groups[2]);
    expect(groups[3]).toBe(groups[4]);
    expect(groups[5]).toBeNull();
    expect(report.workload.byProviderInstance).toHaveLength(6);
  }).pipe(
    Effect.provide(
      EnvironmentCapacity.layer.pipe(
        Layer.provide(
          dependencies({
            providers: [
              provider(),
              provider({
                instanceId: ProviderInstanceId.make("same"),
                auth: { status: "authenticated", email: "PRIVATE@example.test" },
              }),
              provider({
                instanceId: ProviderInstanceId.make("claude"),
                driver: ProviderDriverKind.make("claudeAgent"),
              }),
              provider({
                instanceId: ProviderInstanceId.make("fingerprint-a"),
                auth: { status: "authenticated" },
              }),
              provider({
                instanceId: ProviderInstanceId.make("fingerprint-b"),
                auth: { status: "authenticated" },
              }),
              provider({
                instanceId: ProviderInstanceId.make("unknown"),
                auth: { status: "unknown" },
                usageLimits: undefined,
              }),
            ],
          }),
        ),
      ),
    ),
  ),
);

it.effect(
  "reports stale snapshots, unknown CPU, and passed resets without inventing refreshed quota",
  () =>
    Effect.gen(function* () {
      const report = yield* read;
      expect(report.host).toMatchObject({ cpuUtilization: null, ageMs: 16_000, stale: true });
      expect(report.providers[0]).toMatchObject({ ageMs: 600_000, stale: true });
      expect(report.providers[0]?.usageLimits).toMatchObject({ ageMs: 600_000, stale: true });
      expect(report.providers[0]?.usageLimits?.windows[0]).toMatchObject({
        remainingPercent: 0,
        resetPassed: true,
      });
    }).pipe(
      Effect.provide(
        EnvironmentCapacity.layer.pipe(
          Layer.provide(
            dependencies({
              sampledAt: now - 16_000,
              cpuUtilization: null,
              providers: [
                provider({
                  checkedAt: iso(-600_000),
                  usageLimits: {
                    checkedAt: iso(-600_000),
                    windows: [
                      {
                        id: "five_hour",
                        kind: "session",
                        label: "Session",
                        usedPercent: 100,
                        resetsAt: iso(-1),
                        windowDurationMins: 300,
                      },
                    ],
                  },
                }),
              ],
            }),
          ),
        ),
      ),
    ),
);

it.effect(
  "keeps disabled, unsupported, failed, missing, and future observations distinguishable",
  () =>
    Effect.gen(function* () {
      const report = yield* read;
      expect(report.host).toMatchObject({ stale: true, ageMs: 0 });
      expect(report.providers[0]).toMatchObject({
        enabled: false,
        status: "disabled",
        usageLimits: null,
      });
      expect(report.providers[1]?.usageLimits?.unavailableReason).toBe("unsupported");
      expect(report.providers[2]?.usageLimits?.unavailableReason).toBe("probeFailed");
      expect(report.providers[3]?.usageLimits).toMatchObject({ stale: true, ageMs: 0 });
    }).pipe(
      Effect.provide(
        EnvironmentCapacity.layer.pipe(
          Layer.provide(
            dependencies({
              sampledAt: now + 1,
              providers: [
                provider({ enabled: false, status: "disabled", usageLimits: undefined }),
                provider({
                  instanceId: ProviderInstanceId.make("api-key"),
                  usageLimits: {
                    checkedAt: iso(),
                    windows: [],
                    unavailable: { reason: "unsupported" },
                  },
                }),
                provider({
                  instanceId: ProviderInstanceId.make("failed"),
                  usageLimits: {
                    checkedAt: iso(),
                    windows: [],
                    unavailable: { reason: "probeFailed", message: "private-error" },
                  },
                }),
                provider({
                  instanceId: ProviderInstanceId.make("future"),
                  usageLimits: { checkedAt: iso(1), windows: [] },
                }),
              ],
            }),
          ),
        ),
      ),
    ),
);

const scope: McpInvocationContext.McpInvocationScope = {
  environmentId,
  thread: {
    threadId: callerId,
    providerInstanceId: instanceId,
    providerSessionId: "test-session",
  },
  client: undefined,
  requestNamespace: "capacity-test",
  issuedAt: 0,
  capabilities: new Set(["orchestration"]),
};
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo: { name: "capacity-test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "capacity-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

function mcpLayer(options: { failWorkload?: boolean; deleted?: boolean } = {}) {
  return McpHttpServer.layerEnvironmentToolkit.pipe(
    Layer.provide(EnvironmentCapacity.layer),
    Layer.provideMerge(McpServer.McpServer.layer),
    Layer.provide(
      Layer.mergeAll(
        dependencies(options),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          getThreadShell: () => Effect.succeed(options.deleted ? null : thread()),
        }),
        Layer.mock(Settings.ServerSettingsService)({}),
        Layer.mock(ThreadCommandExecutor.ThreadCommandExecutor)({}),
      ),
    ),
  );
}

const decodeFailure = Schema.decodeUnknownSync(Schema.fromJsonString(OrchestratorMcpFailure));

const readFailure = (result: {
  readonly isError?: boolean | undefined;
  readonly content: ReadonlyArray<{ readonly type: string; readonly text?: string | undefined }>;
}) => {
  expect(result.isError).toBe(true);
  const text = result.content.find((part) => part.type === "text")?.text;
  return decodeFailure(text);
};

const call = (invocation = scope) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now);
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name: "t3_capacity_read", arguments: {} })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  });

it.effect("serves capacity through the production MCP registration for a read-only caller", () =>
  Effect.gen(function* () {
    const result = yield* call();
    expect(result.isError).not.toBe(true);
    const report = yield* decodeReport(result.structuredContent);
    expect(report.workload.threads.idle).toBe(1);
    expect(report.providers[0]?.usageLimits?.windows[0]?.remainingPercent).toBe(69.5);
  }).pipe(Effect.provide(mcpLayer())),
);

it.effect("rejects an MCP credential without orchestration access", () =>
  Effect.gen(function* () {
    const result = yield* call({ ...scope, capabilities: new Set() });
    expect(readFailure(result)).toMatchObject({ code: "capability_denied" });
  }).pipe(Effect.provide(mcpLayer())),
);

it.effect("rejects credentials belonging to another environment", () =>
  Effect.gen(function* () {
    const result = yield* call({ ...scope, environmentId: EnvironmentId.make("other") });
    expect(readFailure(result)).toMatchObject({ code: "capability_denied" });
  }).pipe(Effect.provide(mcpLayer())),
);

it.effect("rejects a deleted caller", () =>
  Effect.gen(function* () {
    const result = yield* call();
    expect(readFailure(result)).toMatchObject({ code: "thread_not_found" });
  }).pipe(Effect.provide(mcpLayer({ deleted: true }))),
);

it.effect("returns a public error instead of zero workload when projections fail", () =>
  Effect.gen(function* () {
    const result = yield* call();
    expect(readFailure(result)).toMatchObject({ code: "orchestration_error" });
    expect(encodeJson(result)).not.toContain("private-db-path");
    expect(readFailure(result)).not.toHaveProperty("workload");
  }).pipe(Effect.provide(mcpLayer({ failWorkload: true }))),
);
