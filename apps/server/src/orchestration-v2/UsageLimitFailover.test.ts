import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadShellSnapshot,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadContinuationSyncError,
  RunId,
  type ServerProvider,
  type ServerProviderUsageWindow,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ProviderThreadContinuationSync from "../provider/ProviderThreadContinuationSync.ts";
import * as ServerSettings from "../serverSettings.ts";
import type * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as UsageLimitFailover from "./UsageLimitFailover.ts";

const NOW = Date.parse("2026-10-28T12:00:00.000Z");
const MINUTE = 60_000;
const iso = (offsetMs: number) => DateTime.formatIso(DateTime.makeUnsafe(NOW + offsetMs));
const claude = ProviderDriverKind.make("claudeAgent");
const threadId = ThreadId.make("thread-1");
const lineage = { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId };
const run = (messageId: string, ageMs: number) => ({
  userMessageId: MessageId.make(messageId),
  requestedAt: DateTime.makeUnsafe(NOW - ageMs),
});
const failoverRun = (ageMs: number) => run(`limit-failover:${threadId}:run-${ageMs}`, ageMs);

it.each([
  { name: "a reset 30 minutes away waits", resetIn: 30 * MINUTE, expected: "wait" },
  { name: "a later reset looks for an account", resetIn: 30 * MINUTE + 1_000, expected: "look" },
  {
    name: "delegated tasks stay on their account",
    resetIn: 2 * 60 * MINUTE,
    thread: {
      lineage,
      delegatedFrom: { environmentId: EnvironmentId.make("e"), threadId, title: "t" },
    },
    expected: "stay",
  },
  {
    name: "provider subagents stay",
    resetIn: 2 * 60 * MINUTE,
    thread: { lineage: { ...lineage, relationshipToParent: "subagent" as const } },
    expected: "stay",
  },
  {
    name: "snoozed threads stay asleep",
    resetIn: 2 * 60 * MINUTE,
    thread: { lineage, snoozedUntil: DateTime.makeUnsafe(NOW + 60 * MINUTE) },
    expected: "stay",
  },
  {
    name: "two switches within the hour stop further switching",
    resetIn: 2 * 60 * MINUTE,
    runs: [failoverRun(10 * MINUTE), failoverRun(50 * MINUTE)],
    expected: "stay",
  },
  {
    name: "a close reset still waits after two switches",
    resetIn: 10 * MINUTE,
    runs: [failoverRun(10 * MINUTE), failoverRun(50 * MINUTE)],
    expected: "wait",
  },
  {
    name: "switches older than an hour no longer count",
    resetIn: 2 * 60 * MINUTE,
    runs: [failoverRun(10 * MINUTE), failoverRun(61 * MINUTE)],
    expected: "look",
  },
  {
    name: "ordinary turns do not count as switches",
    resetIn: 2 * 60 * MINUTE,
    runs: [run("message-1", 10 * MINUTE), run("message-2", 20 * MINUTE)],
    expected: "look",
  },
  {
    name: "stops older than 15 minutes keep the user's own recovery",
    resetIn: 2 * 60 * MINUTE,
    stoppedAgo: 16 * MINUTE,
    expected: "stay",
  },
  {
    name: "an old stop whose reset passed is not resumed",
    resetIn: -2 * 24 * 60 * MINUTE,
    stoppedAgo: 3 * 24 * 60 * MINUTE,
    expected: "stay",
  },
])("$name", ({ resetIn, stoppedAgo, thread, runs, expected }) => {
  assert.equal(
    UsageLimitFailover.limitFailoverGate({
      nowMs: NOW,
      stoppedAtMs: NOW - (stoppedAgo ?? MINUTE),
      resetAt: iso(resetIn),
      thread: thread ?? { lineage },
      runs: runs ?? [],
    }),
    expected,
  );
});

const window = (
  id: string,
  kind: ServerProviderUsageWindow["kind"],
  usedPercent: number,
  resetsAt?: string,
): ServerProviderUsageWindow => ({
  id,
  kind,
  label: id,
  usedPercent,
  ...(resetsAt === undefined ? {} : { resetsAt }),
});
const roomy = [window("five_hour", "session", 10), window("seven_day", "weekly", 10)];

const opus = (efforts: ReadonlyArray<string>) => ({
  slug: "claude-opus-5",
  name: "Claude Opus 5",
  isCustom: false,
  capabilities: {
    optionDescriptors: [
      {
        id: "effort",
        label: "Effort",
        type: "select" as const,
        options: efforts.map((effort) => ({ id: effort, label: effort })),
      },
    ],
  },
});

const account = (
  id: string,
  windows: ReadonlyArray<ServerProviderUsageWindow>,
  overrides: Partial<ServerProvider> = {},
): ServerProvider => ({
  instanceId: ProviderInstanceId.make(id),
  driver: claude,
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated", email: `${id}@example.com` },
  checkedAt: iso(0),
  models: [opus(["high", "max"])],
  slashCommands: [],
  skills: [],
  usageLimits: { checkedAt: iso(-MINUTE), windows },
  ...overrides,
});

const selection = (instanceId: string) => ({
  instanceId: ProviderInstanceId.make(instanceId),
  model: "claude-opus-5",
  options: [{ id: "effort", value: "max" }],
});

it("ranks accounts with allowance left for the same model and effort", () => {
  const source = account("source", [window("five_hour", "session", 100, iso(4 * 60 * MINUTE))]);
  const providers = [
    source,
    account("same-login", roomy, {
      auth: { status: "authenticated", email: "SOURCE@example.com" },
    }),
    account("codex", roomy, { driver: ProviderDriverKind.make("codex") }),
    account("no-history", roomy),
    account("signed-out", roomy, { auth: { status: "unauthenticated" } }),
    account("disabled", roomy, { enabled: false }),
    account("no-opus", roomy, { models: [] }),
    account("no-max-effort", roomy, { models: [opus(["high"])] }),
    account("stale", [], { usageLimits: { checkedAt: iso(-11 * MINUTE), windows: roomy } }),
    account("probe-failed", [], {
      usageLimits: {
        checkedAt: iso(-MINUTE),
        windows: roomy,
        unavailable: { reason: "probeFailed" },
      },
    }),
    account("unknown-limits", []),
    account("opus-week-spent", [...roomy, window("seven_day_opus", "weekly", 95)]),
    account("nearly-spent", [
      window("five_hour", "session", 92),
      window("seven_day", "weekly", 10),
    ]),
    account("sonnet-week-spent", [
      window("five_hour", "session", 50),
      window("seven_day", "weekly", 40),
      window("seven_day_sonnet", "weekly", 100),
    ]),
    account("reset-passed", [
      window("five_hour", "session", 100, iso(-MINUTE)),
      window("seven_day", "weekly", 30),
    ]),
    account("spend-cap", [
      window("five_hour", "session", 20),
      window("monthly_spend", "monthly", 100),
    ]),
    account("tie", [window("five_hour", "session", 50)]),
  ];

  const targets = UsageLimitFailover.rankLimitFailoverTargets({
    nowMs: NOW,
    source,
    modelSelection: selection("source"),
    providers,
    eligibleInstanceIds: new Set(
      providers
        .map((provider) => provider.instanceId)
        .filter((instanceId) => instanceId !== "no-history"),
    ),
    runningThreads: new Map(),
  });

  assert.deepEqual(
    targets.map((target) => [target.provider.instanceId, target.remainingPercent]),
    [
      ["spend-cap", 80],
      ["reset-passed", 70],
      ["sonnet-week-spent", 50],
      ["tie", 50],
    ],
  );
  assert.deepEqual(targets[0]?.modelSelection, selection("spend-cap"));
});

it("spreads threads by the allowance each would share with the threads already running", () => {
  const source = account("source", [window("five_hour", "session", 100, iso(4 * 60 * MINUTE))]);
  const used = (id: string, usedPercent: number) =>
    account(id, [window("five_hour", "session", usedPercent)]);
  const providers = [
    source,
    used("busy-roomy", 20),
    used("idle-half", 60),
    used("one-running", 40),
    used("same-share-less-left", 75),
    used("same-share-more-left", 50),
    used("crowded", 50),
  ];
  const running = (id: string, count: number) => [ProviderInstanceId.make(id), count] as const;

  const targets = UsageLimitFailover.rankLimitFailoverTargets({
    nowMs: NOW,
    source,
    modelSelection: selection("source"),
    providers,
    eligibleInstanceIds: new Set(providers.map((provider) => provider.instanceId)),
    runningThreads: new Map([
      running("busy-roomy", 3),
      running("one-running", 1),
      running("same-share-more-left", 1),
      running("crowded", 9),
    ]),
  });

  assert.deepEqual(
    targets.map((target) => [target.provider.instanceId, target.sharePercent]),
    [
      ["idle-half", 40],
      ["one-running", 30],
      ["same-share-more-left", 25],
      ["same-share-less-left", 25],
      ["busy-roomy", 20],
      // The 10% floor applies to the allowance, so a crowded account still qualifies, last.
      ["crowded", 5],
    ],
  );
});

it("counts threads whose turn is preparing, starting or running", () => {
  const thread = (
    instanceId: string,
    activityRunStatus: OrchestrationV2ThreadShell["activityRunStatus"],
  ) => ({ providerInstanceId: ProviderInstanceId.make(instanceId), activityRunStatus });

  assert.deepEqual(
    [
      ...UsageLimitFailover.runningThreadsByInstance([
        thread("a", "running"),
        thread("a", "starting"),
        thread("a", "preparing"),
        thread("a", "waiting"),
        thread("a", null),
        thread("b", "running"),
      ]),
    ],
    [
      ["a", 3],
      ["b", 1],
    ],
  );
});

const runId = RunId.make("run-1");
const RESET_AT = iso(4 * 60 * MINUTE);
const candidate: ProjectionStore.ProjectionLimitRecoveryCandidate = {
  id: threadId,
  status: "failed",
  lastErrorClass: "usage_limit",
  latestRunId: runId,
  usageLimitResetAt: RESET_AT,
  archivedAt: null,
  settledOverride: null,
  pendingRuntimeRequest: null,
  latestRunCompletedAt: DateTime.makeUnsafe(NOW - MINUTE),
  updatedAt: DateTime.makeUnsafe(NOW - MINUTE),
  limitRecovery: null,
  snoozedUntil: null,
};

const threadProjection = (stoppedMeanwhile: boolean) =>
  ({
    thread: {
      id: threadId,
      projectId: ProjectId.make("project-1"),
      lineage,
      limitRecovery: stoppedMeanwhile ? { runId, resetAt: RESET_AT, autoResume: false } : null,
    },
    runs: [
      {
        id: runId,
        providerInstanceId: ProviderInstanceId.make("claude-a"),
        modelSelection: selection("claude-a"),
        ...run("message-1", 5 * MINUTE),
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

it.each([
  {
    name: "an account just under 10% with a far reset",
    windows: [window("five_hour", "session", 91, iso(2 * 60 * MINUTE))],
    expected: true,
  },
  {
    name: "an account with exactly 10% left",
    windows: [window("five_hour", "session", 90, iso(2 * 60 * MINUTE))],
    expected: false,
  },
  {
    name: "a spent account resetting in exactly 30 minutes",
    windows: [window("five_hour", "session", 100, iso(30 * MINUTE))],
    expected: false,
  },
  {
    name: "a spent account resetting just after 30 minutes",
    windows: [window("five_hour", "session", 100, iso(30 * MINUTE + 1_000))],
    expected: true,
  },
  {
    name: "an account whose weekly limit is spent while the session has room",
    windows: [
      window("five_hour", "session", 20),
      window("seven_day", "weekly", 100, iso(3 * 24 * 60 * MINUTE)),
    ],
    expected: true,
  },
  { name: "an account with unknown limits", windows: [], expected: false },
  {
    name: "a spent account without a reset time",
    windows: [window("primary", "session", 100)],
    expected: true,
  },
  {
    name: "an account that only spent another model's weekly limit",
    windows: [
      window("five_hour", "session", 20),
      window("seven_day_sonnet", "weekly", 100, iso(3 * 24 * 60 * MINUTE)),
    ],
    expected: false,
  },
])("new threads avoid $name: $expected", ({ windows, expected }) => {
  assert.equal(
    UsageLimitFailover.limitBlocksLaunch(account("source", windows), selection("source"), NOW),
    expected,
  );
});

function failoverLayer(options: {
  readonly failoverEnabled?: boolean;
  readonly targetDisabledForProject?: boolean;
  readonly syncFails?: boolean;
  readonly stoppedDuringSync?: boolean;
  readonly transition?: "restart_and_resume" | "create_with_handoff";
  readonly limitsAgeMs?: number;
  /** Adds claude-c with less allowance than claude-b, which runs three threads. */
  readonly busyPeer?: boolean;
  readonly overviewFails?: boolean;
}) {
  const steps: Array<string> = [];
  const dispatched: Array<OrchestrationV2ServerCommand> = [];
  const probes = { count: 0 };
  const limitsCheckedAt = iso(-(options.limitsAgeMs ?? MINUTE));
  const providers = [
    ["claude-a", [window("five_hour", "session", 100, RESET_AT)]] as const,
    ["claude-b", roomy] as const,
    ...(options.busyPeer === true
      ? [["claude-c", [window("five_hour", "session", 40)]] as const]
      : []),
  ].map(([id, windows]) =>
    account(id, windows, { usageLimits: { checkedAt: limitsCheckedAt, windows } }),
  );
  const busyThreads = Array.from({ length: 3 }, () => ({
    providerInstanceId: ProviderInstanceId.make("claude-b"),
    activityRunStatus: "running",
  }));
  let projectionReads = 0;
  const layer = UsageLimitFailover.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getThreadProjection: () =>
            Effect.sync(() =>
              threadProjection(options.stoppedDuringSync === true && projectionReads++ > 0),
            ),
        }),
        Layer.mock(ProviderRegistry.ProviderRegistry)({
          getProviders: Effect.succeed(providers),
          refreshInstance: () =>
            Effect.sync(() => {
              probes.count += 1;
              return providers;
            }),
        }),
        Layer.mock(ProviderSwitchService.ProviderSwitchServiceV2)({
          plan: () =>
            Effect.succeed({
              instanceChanged: true,
              releaseProviderSessionIds: [],
              transition: { type: options.transition ?? "restart_and_resume" },
            }),
        }),
        Layer.mock(ProviderThreadContinuationSync.ProviderThreadContinuationSync)({
          sync: (input) =>
            Effect.suspend(() => {
              steps.push("sync");
              return options.syncFails === true
                ? Effect.fail(
                    new ProviderThreadContinuationSyncError({ code: "sync-failed", detail: "x" }),
                  )
                : Effect.succeed({
                    threadId: input.threadId,
                    providerInstanceId: ProviderInstanceId.make("claude-a"),
                    state: "imported" as const,
                  });
            }),
        }),
        Layer.mock(ThreadManagement.ThreadManagementService)({
          dispatch: (command) =>
            Effect.sync(() => {
              steps.push("dispatch");
              dispatched.push(command);
              return {} as unknown as Orchestrator.OrchestratorV2DispatchResult;
            }),
          getShellSnapshot: () =>
            options.overviewFails === true
              ? Effect.die("thread overview unavailable")
              : Effect.succeed({
                  threads: busyThreads,
                } as unknown as OrchestrationV2ThreadShellSnapshot),
        }),
        ServerSettings.layerTest({
          failoverLimitedThreads: options.failoverEnabled === true,
          providerInstances: {
            [ProviderInstanceId.make("claude-a")]: { driver: claude, enabled: true },
            [ProviderInstanceId.make("claude-b")]: { driver: claude, enabled: true },
            [ProviderInstanceId.make("claude-c")]: { driver: claude, enabled: true },
          },
          ...(options.targetDisabledForProject === true
            ? {
                projectSettingsOverrides: {
                  [ProjectId.make("project-1")]: {
                    providerInstances: {
                      [ProviderInstanceId.make("claude-b")]: { enabled: false },
                    },
                  },
                },
              }
            : {}),
        }),
      ),
    ),
  );
  return { layer, steps, dispatched, probes };
}

it.effect.each([
  {
    name: "syncs Claude history, then continues on the other account",
    expected: "switched",
    expectedSteps: ["sync", "dispatch"],
  },
  {
    name: "stays when the history cannot be synced",
    syncFails: true,
    expected: "stay",
    expectedSteps: ["sync"],
  },
  {
    name: "lets a Stop during the sync win",
    stoppedDuringSync: true,
    expected: "stay",
    expectedSteps: ["sync"],
  },
  {
    name: "never switches into a summary handoff",
    transition: "create_with_handoff" as const,
    expected: "stay",
    expectedSteps: [],
  },
  {
    name: "spreads to a quieter account over a busier one with more allowance",
    busyPeer: true,
    expected: "switched",
    expectedSteps: ["sync", "dispatch"],
    expectedTarget: "claude-c",
  },
  {
    name: "ranks by allowance alone when the thread overview fails",
    busyPeer: true,
    overviewFails: true,
    expected: "switched",
    expectedSteps: ["sync", "dispatch"],
  },
])("attempt $name", ({ expected, expectedSteps, expectedTarget, ...options }) =>
  Effect.gen(function* () {
    const { layer, steps, dispatched } = failoverLayer(options);
    yield* TestClock.setTime(NOW);
    const outcome = yield* Effect.gen(function* () {
      const failover = yield* UsageLimitFailover.UsageLimitFailover;
      return yield* failover.attempt(candidate);
    }).pipe(Effect.provide(layer));

    assert.equal(outcome, expected);
    assert.deepEqual(steps, expectedSteps);
    if (expected === "switched") {
      const command = dispatched[0];
      assert.equal(command?.type, "message.dispatch");
      if (command?.type !== "message.dispatch") return;
      assert.equal(command.commandId, `limit-failover:${threadId}:${runId}`);
      assert.equal(command.manualContinuationOfRunId, runId);
      assert.deepEqual(command.modelSelection, selection(expectedTarget ?? "claude-b"));
    } else {
      assert.deepEqual(dispatched, []);
    }
  }),
);

// Limits five minutes old are still usable but due for a probe.
it.effect.each([
  {
    name: "keeps the stored account without probing while the switch is off",
    failoverEnabled: false,
    expected: "claude-a",
    expectedProbes: 0,
  },
  {
    name: "starts on an account with allowance when the stored one is spent",
    failoverEnabled: true,
    expected: "claude-b",
    expectedProbes: 2,
  },
  {
    name: "never starts on an account the project turned off",
    failoverEnabled: true,
    targetDisabledForProject: true,
    expected: "claude-a",
    expectedProbes: 1,
  },
  {
    name: "probes each account at most once a minute",
    failoverEnabled: true,
    launches: 3,
    expected: "claude-b",
    expectedProbes: 2,
  },
])("launchSelection $name", ({ expected, expectedProbes, launches, ...options }) =>
  Effect.gen(function* () {
    const { layer, probes } = failoverLayer({ ...options, limitsAgeMs: 5 * MINUTE });
    yield* TestClock.setTime(NOW);
    const chosen = yield* Effect.gen(function* () {
      const failover = yield* UsageLimitFailover.UsageLimitFailover;
      const launch = failover.launchSelection({
        projectId: ProjectId.make("project-1"),
        modelSelection: selection("claude-a"),
      });
      let latest = yield* launch;
      for (let index = 1; index < (launches ?? 1); index += 1) latest = yield* launch;
      return latest;
    }).pipe(Effect.provide(layer));

    assert.deepEqual(chosen, selection(expected));
    assert.equal(probes.count, expectedProbes);
  }),
);
