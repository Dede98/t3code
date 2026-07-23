import type {
  AgentControlGithubEvent,
  AgentControlGithubIntakeState,
  AgentControlGithubPollErrorCode,
  AgentControlGithubReactorReasonCode,
  ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { AgentControlGithubSchedulerState } from "./Services/AgentControlGithubSchedulerState.ts";

export const AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD = 5;
export const AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS = 15 * 60 * 1_000;
export const AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS = 15 * 60 * 1_000;

const RETRYABLE_CODES = new Set<AgentControlGithubPollErrorCode>([
  "github-unavailable",
  "github-authentication",
  "github-timeout",
  "github-command-failed",
  "github-decode-failed",
]);

const SUSPENDING_CODES = new Set<AgentControlGithubPollErrorCode>([
  "repository-identity-changed",
  "issue-repository-changed",
  "timeline-incomplete",
  "pagination-overflow",
]);

const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));

export function githubObserveBackoffMs(
  pollIntervalSeconds: number,
  consecutiveFailures: number,
  code: AgentControlGithubPollErrorCode,
): number {
  if (code === "github-authentication") return AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS;
  const base = Math.max(1, pollIntervalSeconds) * 1_000;
  const exponent = Math.max(0, Math.min(30, consecutiveFailures - 1));
  return Math.min(AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS, base * 2 ** exponent);
}

export const githubObserveConfigFingerprint = (config: {
  readonly repository: {
    readonly repositoryNodeId: string;
    readonly nameWithOwner: string;
  };
  readonly settings: {
    readonly trackerKind: "github";
    readonly readyLabel: string;
    readonly pausedLabel: string;
    readonly trustedLogins: ReadonlyArray<string>;
    readonly pollIntervalSeconds: number;
  };
}) =>
  JSON.stringify([
    config.repository.repositoryNodeId,
    config.repository.nameWithOwner.toLocaleLowerCase("en-US"),
    config.settings.trackerKind,
    config.settings.readyLabel,
    config.settings.pausedLabel,
    [...config.settings.trustedLogins],
    config.settings.pollIntervalSeconds,
  ]);

const failureReason = (
  code: AgentControlGithubPollErrorCode,
): AgentControlGithubReactorReasonCode =>
  code === "internal-persistence-error" ? "internal-coordination-error" : code;

export interface GithubObserveSchedulerFold {
  readonly state: AgentControlGithubSchedulerState | null;
  readonly lastGeneration: number;
}

export interface GithubObserveSchedulerTransitionContext {
  readonly now: number;
  readonly jitterMillis: (
    projectId: ProjectId,
    generation: number,
    attempt: number,
    pollIntervalSeconds: number,
  ) => number;
}

/**
 * Pure domain transition for committed github-intake events. Persistence
 * revisions are stamped by the repository caller after this transition.
 */
export const reduceGithubObserveScheduler = (
  fold: GithubObserveSchedulerFold,
  event: AgentControlGithubEvent,
  context: GithubObserveSchedulerTransitionContext,
): GithubObserveSchedulerFold => {
  if (fold.state !== null && event.sequence <= fold.state.lastGithubEventSequence) {
    return fold;
  }

  switch (event.type) {
    case "agentControl.github.config.set": {
      const generation = fold.lastGeneration + 1;
      const interval = event.payload.settings.pollIntervalSeconds;
      return {
        lastGeneration: generation,
        state: {
          schemaVersion: 1,
          projectId: event.aggregateId,
          schedulerRevision: fold.state?.schedulerRevision ?? 1,
          generation,
          configFingerprint: githubObserveConfigFingerprint(event.payload),
          pollIntervalSeconds: interval,
          lastGithubEventSequence: event.sequence,
          activity: "active",
          circuitState: "closed",
          consecutiveFailures: 0,
          lastAttemptAt: null,
          nextAttemptAt: iso(
            context.now + context.jitterMillis(event.aggregateId, generation, 0, interval),
          ),
          cooldownUntil: null,
          reasonCode: null,
          updatedAt: iso(context.now),
        },
      };
    }
    case "agentControl.github.config.cleared":
      return { state: null, lastGeneration: fold.lastGeneration };
    case "agentControl.github.poll.succeeded": {
      const state = fold.state;
      if (state === null) return fold;
      const completedAt = Date.parse(event.payload.completedAt);
      const nextAttemptAt =
        completedAt +
        state.pollIntervalSeconds * 1_000 +
        context.jitterMillis(state.projectId, state.generation, 0, state.pollIntervalSeconds);
      return {
        lastGeneration: fold.lastGeneration,
        state: {
          ...state,
          lastGithubEventSequence: event.sequence,
          activity: "active",
          circuitState: "closed",
          consecutiveFailures: 0,
          lastAttemptAt: event.payload.attemptedAt,
          nextAttemptAt: iso(nextAttemptAt),
          cooldownUntil: null,
          reasonCode: null,
          updatedAt: event.payload.completedAt,
        },
      };
    }
    case "agentControl.github.poll.failed": {
      const state = fold.state;
      if (state === null) return fold;
      const base = {
        ...state,
        lastGithubEventSequence: event.sequence,
        lastAttemptAt: event.payload.attemptedAt,
        updatedAt: event.payload.completedAt,
      };

      // A committed success or a new configuration generation is the only
      // transition allowed to clear a hard suspension.
      if (state.activity === "suspended") {
        return {
          lastGeneration: fold.lastGeneration,
          state: base,
        };
      }

      if (SUSPENDING_CODES.has(event.payload.errorCode)) {
        return {
          lastGeneration: fold.lastGeneration,
          state: {
            ...base,
            activity: "suspended",
            circuitState: "open",
            nextAttemptAt: null,
            cooldownUntil: null,
            reasonCode: failureReason(event.payload.errorCode),
          },
        };
      }

      const completedAt = Date.parse(event.payload.completedAt);
      if (!RETRYABLE_CODES.has(event.payload.errorCode)) {
        if (state.circuitState === "open") {
          return { lastGeneration: fold.lastGeneration, state: base };
        }
        const halfOpen = state.circuitState === "half-open";
        const delay = halfOpen
          ? AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS
          : state.pollIntervalSeconds * 1_000 +
            context.jitterMillis(
              state.projectId,
              state.generation,
              state.consecutiveFailures,
              state.pollIntervalSeconds,
            );
        const nextAttemptAt = completedAt + delay;
        return {
          lastGeneration: fold.lastGeneration,
          state: {
            ...base,
            activity: "active",
            circuitState: halfOpen ? "open" : "closed",
            nextAttemptAt: iso(nextAttemptAt),
            cooldownUntil: halfOpen ? iso(nextAttemptAt) : null,
            reasonCode: failureReason(event.payload.errorCode),
          },
        };
      }

      const failures = state.consecutiveFailures + 1;
      const open =
        failures >= AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD ||
        state.circuitState === "half-open";
      const delay = open
        ? AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS
        : githubObserveBackoffMs(state.pollIntervalSeconds, failures, event.payload.errorCode);
      const nextAttemptAt = completedAt + delay;
      return {
        lastGeneration: fold.lastGeneration,
        state: {
          ...base,
          activity: "active",
          circuitState: open ? "open" : "closed",
          consecutiveFailures: failures,
          nextAttemptAt: iso(nextAttemptAt),
          cooldownUntil: open ? iso(nextAttemptAt) : null,
          reasonCode: failureReason(event.payload.errorCode),
        },
      };
    }
  }
};

export const githubObserveProjectionMatches = (
  state: AgentControlGithubSchedulerState,
  projection: NonNullable<AgentControlGithubIntakeState["config"]>,
) =>
  state.configFingerprint === githubObserveConfigFingerprint(projection) &&
  state.pollIntervalSeconds === projection.settings.pollIntervalSeconds;
