import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2ThreadProjection,
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

function failoverLayer(options: {
  readonly syncFails?: boolean;
  readonly stoppedDuringSync?: boolean;
  readonly transition?: "restart_and_resume" | "create_with_handoff";
}) {
  const steps: Array<string> = [];
  const dispatched: Array<OrchestrationV2ServerCommand> = [];
  const providers = [
    account("claude-a", [window("five_hour", "session", 100, RESET_AT)]),
    account("claude-b", roomy),
  ];
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
          refreshInstance: () => Effect.succeed(providers),
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
        }),
        ServerSettings.layerTest({
          providerInstances: {
            [ProviderInstanceId.make("claude-a")]: { driver: claude, enabled: true },
            [ProviderInstanceId.make("claude-b")]: { driver: claude, enabled: true },
          },
        }),
      ),
    ),
  );
  return { layer, steps, dispatched };
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
])("attempt $name", ({ expected, expectedSteps, ...options }) =>
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
      assert.deepEqual(command.modelSelection, selection("claude-b"));
    } else {
      assert.deepEqual(dispatched, []);
    }
  }),
);
