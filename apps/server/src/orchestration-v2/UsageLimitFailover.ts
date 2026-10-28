import {
  CommandId,
  isProviderAvailable,
  MessageId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2Run,
  type ProviderInstanceId,
  resolveProjectProviderInstanceEnabled,
  type ServerProvider,
  type ServerProviderUsageLimits,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ProviderThreadContinuationSync from "../provider/ProviderThreadContinuationSync.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderSwitchService from "./ProviderSwitchService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

const WAIT_THRESHOLD_MS = 30 * 60_000;
// Older stops were left before the switch was on, or before a restart; they keep the user's choice.
const FRESH_STOP_MS = 15 * 60_000;
const MIN_REMAINING_PERCENT = 10;
// Claude caches its usage probe for five minutes, so even a refresh can return data that old.
const MAX_LIMITS_AGE_MS = 10 * 60_000;
const REFRESH_LIMITS_AFTER_MS = 60_000;
const MAX_FAILOVERS_PER_HOUR = 2;
const FAILOVER_ID_PREFIX = "limit-failover:";
const SCOPED_WEEKLY_PREFIX = "seven_day_";

type LimitFailoverOutcome = "wait" | "switched" | "stay";

const isSnoozed = (thread: Pick<OrchestrationV2AppThread, "snoozedUntil">, nowMs: number) =>
  thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs;

export function limitFailoverGate(input: {
  readonly nowMs: number;
  readonly stoppedAtMs: number;
  readonly resetAt: string;
  readonly thread: Pick<OrchestrationV2AppThread, "lineage" | "delegatedFrom" | "snoozedUntil">;
  readonly runs: ReadonlyArray<Pick<OrchestrationV2Run, "userMessageId" | "requestedAt">>;
}): "wait" | "look" | "stay" {
  const resetMs = Date.parse(input.resetAt);
  if (
    !Number.isFinite(resetMs) ||
    input.nowMs - input.stoppedAtMs > FRESH_STOP_MS ||
    input.thread.delegatedFrom !== undefined ||
    input.thread.lineage.relationshipToParent === "subagent" ||
    isSnoozed(input.thread, input.nowMs)
  ) {
    return "stay";
  }
  if (resetMs - input.nowMs <= WAIT_THRESHOLD_MS) return "wait";
  const recentFailovers = input.runs.filter(
    (run) =>
      run.userMessageId.startsWith(FAILOVER_ID_PREFIX) &&
      input.nowMs - DateTime.toEpochMillis(run.requestedAt) < 60 * 60_000,
  ).length;
  return recentFailovers >= MAX_FAILOVERS_PER_HOUR ? "stay" : "look";
}

interface LimitFailoverTarget {
  readonly provider: ServerProvider;
  readonly modelSelection: ModelSelection;
  readonly remainingPercent: number;
}

/** Same-driver accounts that can continue the thread, the one with the most allowance left first. */
export function rankLimitFailoverTargets(input: {
  readonly nowMs: number;
  readonly source: ServerProvider;
  readonly modelSelection: ModelSelection;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly eligibleInstanceIds: ReadonlySet<ProviderInstanceId>;
}): ReadonlyArray<LimitFailoverTarget> {
  const sourceAccount = accountKey(input.source);
  const targets: Array<LimitFailoverTarget> = [];
  for (const provider of input.providers) {
    if (
      provider.instanceId === input.source.instanceId ||
      provider.driver !== input.source.driver ||
      !input.eligibleInstanceIds.has(provider.instanceId) ||
      !isUsable(provider) ||
      (sourceAccount !== null && accountKey(provider) === sourceAccount)
    ) {
      continue;
    }
    const target = targetModel(provider, input.modelSelection);
    if (target === null) continue;
    const remainingPercent = remainingAllowance(provider.usageLimits, target.text, input.nowMs);
    if (remainingPercent === null || remainingPercent < MIN_REMAINING_PERCENT) continue;
    targets.push({ provider, modelSelection: target.selection, remainingPercent });
  }
  return targets.toSorted((left, right) => right.remainingPercent - left.remainingPercent);
}

const isUsable = (provider: ServerProvider) =>
  provider.enabled &&
  provider.installed &&
  isProviderAvailable(provider) &&
  provider.auth.status !== "unauthenticated" &&
  provider.status !== "error" &&
  provider.status !== "disabled";

function accountKey(provider: ServerProvider): string | null {
  const email = provider.auth.email?.trim().toLowerCase();
  if (email) return `email:${email}`;
  const fingerprint = provider.usageLimits?.credentialFingerprint;
  return fingerprint ? `credential:${fingerprint}` : null;
}

function targetModel(provider: ServerProvider, selection: ModelSelection) {
  const model = provider.models.find(
    (candidate) =>
      candidate.slug === selection.model || candidate.aliases?.includes(selection.model),
  );
  if (model === undefined) return null;
  const descriptors = model.capabilities?.optionDescriptors;
  const optionsSupported = (selection.options ?? []).every((option) => {
    if (descriptors === undefined) return true;
    const descriptor = descriptors.find((candidate) => candidate.id === option.id);
    if (descriptor === undefined) return false;
    return descriptor.type === "boolean"
      ? typeof option.value === "boolean"
      : descriptor.options.some((choice) => choice.id === option.value);
  });
  return optionsSupported
    ? {
        selection: { ...selection, instanceId: provider.instanceId },
        text: `${model.slug} ${model.name}`.toLowerCase(),
      }
    : null;
}

/** The tightest allowance the model draws from, or null when it is unknown or stale. */
function remainingAllowance(
  limits: ServerProviderUsageLimits | undefined,
  modelText: string,
  nowMs: number,
): number | null {
  if (
    limits === undefined ||
    limits.unavailable !== undefined ||
    !(nowMs - Date.parse(limits.checkedAt) <= MAX_LIMITS_AGE_MS)
  ) {
    return null;
  }
  const windows = limits.windows.filter((window) => windowApplies(window, modelText));
  if (windows.length === 0) return null;
  return Math.min(
    ...windows.map((window) =>
      window.resetsAt !== undefined && Date.parse(window.resetsAt) <= nowMs
        ? 100
        : 100 - window.usedPercent,
    ),
  );
}

function windowApplies(window: ServerProviderUsageWindow, modelText: string): boolean {
  // A monthly window without a reset is a spend budget for paid overage, not the plan allowance.
  if (window.kind === "monthly" && window.resetsAt === undefined) return false;
  // Claude names per-model weekly windows seven_day_<model>; they bind only that model.
  if (!window.id.startsWith(SCOPED_WEEKLY_PREFIX)) return true;
  return window.id
    .slice(SCOPED_WEEKLY_PREFIX.length)
    .split("_")
    .every((token) => modelText.includes(token));
}

function formatWait(ms: number): string {
  const minutes = Math.ceil(ms / 60_000);
  const days = Math.floor(minutes / 1_440);
  const hours = Math.floor((minutes % 1_440) / 60);
  if (days > 0) return `${days} d ${hours} h`;
  if (hours > 0) return `${hours} h ${minutes % 60} min`;
  return `${minutes} min`;
}

export interface UsageLimitFailoverShape {
  /** Waits for a reset that is close, or moves the thread to another account of its provider. */
  readonly attempt: (
    thread: ProjectionStore.ProjectionLimitRecoveryCandidate,
  ) => Effect.Effect<LimitFailoverOutcome>;
}

export class UsageLimitFailover extends Context.Service<
  UsageLimitFailover,
  UsageLimitFailoverShape
>()("t3/orchestration-v2/UsageLimitFailover") {}

const make = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const switches = yield* ProviderSwitchService.ProviderSwitchServiceV2;
  const continuation = yield* ProviderThreadContinuationSync.ProviderThreadContinuationSync;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const settings = yield* ServerSettings.ServerSettingsService;

  const attempt = Effect.fn("UsageLimitFailover.attempt")(
    function* (candidate: ProjectionStore.ProjectionLimitRecoveryCandidate) {
      const runId = candidate.latestRunId;
      const resetAt = candidate.usageLimitResetAt;
      if (!runId || !resetAt) return "stay" as const;
      const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
      const projection = yield* projections.getThreadProjection(candidate.id);
      const gate = limitFailoverGate({
        nowMs,
        stoppedAtMs: DateTime.toEpochMillis(candidate.latestRunCompletedAt ?? candidate.updatedAt),
        resetAt,
        thread: projection.thread,
        runs: projection.runs,
      });
      if (gate !== "look") return gate;
      const run = projection.runs.find((entry) => entry.id === runId);
      let providers = yield* registry.getProviders;
      const source = providers.find((provider) => provider.instanceId === run?.providerInstanceId);
      if (run === undefined || source === undefined) return "stay" as const;

      const preferences = yield* settings.getSettings;
      const eligibleInstanceIds = new Set<ProviderInstanceId>();
      for (const peer of providers) {
        if (
          peer.driver !== source.driver ||
          peer.instanceId === source.instanceId ||
          !isUsable(peer) ||
          targetModel(peer, run.modelSelection) === null ||
          !resolveProjectProviderInstanceEnabled(
            preferences,
            projection.thread.projectId,
            peer.instanceId,
          )
        ) {
          continue;
        }
        const plan = yield* Effect.option(
          switches.plan({
            projection,
            targetModelSelection: { ...run.modelSelection, instanceId: peer.instanceId },
          }),
        );
        // Only a native resume carries the whole conversation; a handoff summary would not.
        if (Option.isNone(plan) || plan.value.transition.type !== "restart_and_resume") continue;
        eligibleInstanceIds.add(peer.instanceId);
        const checkedAt = peer.usageLimits?.checkedAt;
        if (checkedAt === undefined || nowMs - Date.parse(checkedAt) > REFRESH_LIMITS_AFTER_MS) {
          providers = yield* registry.refreshInstance(peer.instanceId);
        }
      }
      const [target] = rankLimitFailoverTargets({
        nowMs,
        source,
        modelSelection: run.modelSelection,
        providers,
        eligibleInstanceIds,
      });
      if (target === undefined) {
        yield* Effect.logInfo("orchestration-v2.limit-failover.no-target", {
          threadId: candidate.id,
          sourceInstanceId: source.instanceId,
          candidateInstanceIds: [...eligibleInstanceIds],
        });
        return "stay" as const;
      }

      if (source.driver === "claudeAgent") {
        yield* continuation.sync({ threadId: candidate.id });
      }
      // A Stop, Resume at reset, or snooze chosen while the history synced wins over the switch.
      const latest = yield* projections.getThreadProjection(candidate.id);
      if (latest.thread.limitRecovery?.runId === runId || isSnoozed(latest.thread, nowMs)) {
        return "stay" as const;
      }

      const sourceName = source.displayName ?? source.instanceId;
      const targetName = target.provider.displayName ?? target.provider.instanceId;
      const identity = `${FAILOVER_ID_PREFIX}${candidate.id}:${runId}`;
      yield* threads.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(identity),
        messageId: MessageId.make(identity),
        threadId: candidate.id,
        manualContinuationOfRunId: runId,
        modelSelection: target.modelSelection,
        text: `Continue where you left off. (${sourceName} reached its usage limit, which resets in ${formatWait(Date.parse(resetAt) - nowMs)}, so T3 Code moved this thread to ${targetName}.)`,
        attachments: [],
        dispatchMode: { type: "start_immediately" },
        createdBy: "user",
        creationSource: "server",
      });
      yield* Effect.logInfo("orchestration-v2.limit-failover.switched", {
        threadId: candidate.id,
        sourceInstanceId: source.instanceId,
        targetInstanceId: target.provider.instanceId,
        remainingPercent: target.remainingPercent,
      });
      return "switched" as const;
    },
    (effect, candidate) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.limit-failover.failed", {
            threadId: candidate.id,
            cause,
          }).pipe(Effect.as("stay" as const)),
        ),
      ),
  );

  return UsageLimitFailover.of({ attempt });
});

export const layer = Layer.effect(UsageLimitFailover, make);
