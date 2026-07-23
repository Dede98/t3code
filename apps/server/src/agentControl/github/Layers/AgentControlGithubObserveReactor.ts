import {
  AgentControlGithubProjectInput,
  type AgentControlGithubEvent,
  type AgentControlGithubPollErrorCode,
  type AgentControlGithubReactorReasonCode,
  AgentControlGithubRpcError,
  CommandId,
  type OrchestrationEvent,
  type ProjectId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlGithubIntake } from "../Services/AgentControlGithubIntake.ts";
import {
  AgentControlGithubObserveReactor,
  type AgentControlGithubObserveReactorShape,
} from "../Services/AgentControlGithubObserveReactor.ts";
import {
  type AgentControlGithubSchedulerState,
  AgentControlGithubSchedulerStateRepository,
} from "../Services/AgentControlGithubSchedulerState.ts";
import { AgentControlGithubStateRepository } from "../Services/AgentControlGithubStateRepository.ts";

export const AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD = 5;
export const AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS = Duration.toMillis(Duration.minutes(15));
export const AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS = Duration.toMillis(Duration.minutes(15));

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
const isGithubRpcError = Schema.is(AgentControlGithubRpcError);
const decodeProjectInput = Schema.decodeUnknownEffect(AgentControlGithubProjectInput);

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

export interface AgentControlGithubObserveReactorOptions {
  readonly jitterMillis?: (input: {
    readonly projectId: ProjectId;
    readonly generation: number;
    readonly attempt: number;
    readonly pollIntervalSeconds: number;
  }) => number;
}

interface ActiveWorker {
  readonly generation: number;
  readonly scope: Scope.Closeable;
  phase: "timer" | "poll";
}

type ReactorMessage =
  | { readonly _tag: "ProjectChanged"; readonly projectId: ProjectId }
  | { readonly _tag: "RecoverGithubOutcome"; readonly projectId: ProjectId }
  | { readonly _tag: "GithubEvent"; readonly event: AgentControlGithubEvent }
  | { readonly _tag: "ProjectDeleted"; readonly projectId: ProjectId }
  | { readonly _tag: "TimerDue"; readonly projectId: ProjectId; readonly generation: number }
  | {
      readonly _tag: "PollCoordination";
      readonly projectId: ProjectId;
      readonly generation: number;
      readonly code: AgentControlGithubReactorReasonCode;
    };

const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
const millis = (value: string | null) => (value === null ? null : Date.parse(value));

const defaultJitterMillis: NonNullable<AgentControlGithubObserveReactorOptions["jitterMillis"]> = ({
  projectId,
  generation,
  attempt,
  pollIntervalSeconds,
}) => {
  let hash = 2_166_136_261;
  const key = `${projectId}:${generation}:${attempt}`;
  for (let index = 0; index < key.length; index += 1) {
    hash ^= key.charCodeAt(index);
    hash = Math.imul(hash, 16_777_619);
  }
  const bound = Math.max(1, Math.min(5_000, Math.floor(pollIntervalSeconds * 100)));
  return (hash >>> 0) % bound;
};

const configFingerprint = (state: {
  readonly config: NonNullable<
    import("@t3tools/contracts").AgentControlGithubIntakeState["config"]
  >;
}) =>
  JSON.stringify([
    state.config.repository.repositoryNodeId,
    state.config.repository.nameWithOwner.toLocaleLowerCase("en-US"),
    state.config.settings.trackerKind,
    state.config.settings.readyLabel,
    state.config.settings.pausedLabel,
    [...state.config.settings.trustedLogins],
    state.config.settings.pollIntervalSeconds,
  ]);

const rpcError = (
  projectId: ProjectId,
  code: AgentControlGithubRpcError["code"] = "internal-persistence-error",
) =>
  new AgentControlGithubRpcError({
    projectId,
    code,
    operation: "get-reactor-status",
  });

const makeReasonCode = (
  code: AgentControlGithubRpcError["code"],
): AgentControlGithubReactorReasonCode => {
  switch (code) {
    case "poll-in-progress":
    case "revision-conflict":
      return code;
    case "project-missing":
    case "project-deleted":
      return "project-unavailable";
    case "tracker-not-configured":
      return "tracker-not-configured";
    default:
      return "internal-coordination-error";
  }
};

const pollFailureReason = (
  code: AgentControlGithubPollErrorCode,
): AgentControlGithubReactorReasonCode =>
  code === "internal-persistence-error" ? "internal-coordination-error" : code;

export const make = Effect.fn("AgentControlGithubObserveReactor.make")(function* (
  options: AgentControlGithubObserveReactorOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const projectController = yield* AgentControlEngine;
  const githubIntake = yield* AgentControlGithubIntake;
  const githubStates = yield* AgentControlGithubStateRepository;
  const schedulerStates = yield* AgentControlGithubSchedulerStateRepository;
  const projectStates = yield* AgentControlProjectStateRepository;
  const availability = yield* AgentControlProjectAvailability;
  const orchestration = yield* OrchestrationEngineService;
  const messages = yield* Queue.unbounded<ReactorMessage>();
  const started = yield* Ref.make(false);
  const workers = new Map<string, ActiveWorker>();
  const jitterMillis = options.jitterMillis ?? defaultJitterMillis;

  const jitter = (
    projectId: ProjectId,
    generation: number,
    attempt: number,
    pollIntervalSeconds: number,
  ) =>
    Math.max(
      0,
      Math.min(
        5_000,
        Math.floor(jitterMillis({ projectId, generation, attempt, pollIntervalSeconds })),
      ),
    );

  const stopWorker = Effect.fn("AgentControlGithubObserveReactor.stopWorker")(function* (
    projectId: ProjectId,
  ) {
    const worker = workers.get(projectId);
    if (worker === undefined) return;
    workers.delete(projectId);
    yield* Scope.close(worker.scope, Exit.void).pipe(Effect.ignore);
  });

  const stopAllWorkers = Effect.fn("AgentControlGithubObserveReactor.stopAllWorkers")(function* () {
    const active = [...workers.entries()];
    workers.clear();
    yield* Effect.forEach(active, ([, worker]) => Scope.close(worker.scope, Exit.void), {
      concurrency: "unbounded",
      discard: true,
    }).pipe(Effect.ignore);
  });

  const installTimer = Effect.fn("AgentControlGithubObserveReactor.installTimer")(function* (
    state: AgentControlGithubSchedulerState,
  ) {
    yield* stopWorker(state.projectId);
    if (state.activity !== "active" || state.nextAttemptAt === null) return;
    const now = yield* Clock.currentTimeMillis;
    const dueAt = millis(state.nextAttemptAt);
    if (dueAt === null || !Number.isFinite(dueAt)) return;
    const workerScope = yield* Scope.make("sequential");
    const worker: ActiveWorker = {
      generation: state.generation,
      scope: workerScope,
      phase: "timer",
    };
    workers.set(state.projectId, worker);
    yield* Effect.sleep(Duration.millis(Math.max(0, dueAt - now))).pipe(
      Effect.andThen(
        Queue.offer(messages, {
          _tag: "TimerDue",
          projectId: state.projectId,
          generation: state.generation,
        }),
      ),
      Effect.forkIn(workerScope),
    );
  });

  const saveAndSchedule = Effect.fn("AgentControlGithubObserveReactor.saveAndSchedule")(function* (
    state: AgentControlGithubSchedulerState,
  ) {
    yield* schedulerStates.save(state);
    yield* installTimer(state);
  });

  const cleanupProject = Effect.fn("AgentControlGithubObserveReactor.cleanupProject")(function* (
    projectId: ProjectId,
  ) {
    yield* stopWorker(projectId);
    yield* schedulerStates.delete(projectId);
  });

  const loadObservedProject = Effect.fn("AgentControlGithubObserveReactor.loadObservedProject")(
    function* (projectId: ProjectId) {
      const available = yield* Effect.result(availability.ensureAvailable(projectId));
      if (available._tag === "Failure") return Option.none();
      const project = yield* projectStates.get(projectId);
      if (Option.isNone(project) || project.value.mode !== "observe") return Option.none();
      const github = yield* githubStates.get(projectId);
      if (Option.isNone(github) || github.value.config === null) return Option.none();
      return Option.some({
        project: project.value,
        github: github.value,
        fingerprint: configFingerprint({ config: github.value.config }),
        intervalSeconds: github.value.config.settings.pollIntervalSeconds,
      });
    },
  );

  const freshState = (
    projectId: ProjectId,
    fingerprint: string,
    intervalSeconds: number,
    generation: number,
    lastGithubEventSequence: number,
    now: number,
  ): AgentControlGithubSchedulerState => ({
    schemaVersion: 1,
    projectId,
    generation,
    configFingerprint: fingerprint,
    pollIntervalSeconds: intervalSeconds,
    lastGithubEventSequence,
    activity: "active",
    circuitState: "closed",
    consecutiveFailures: 0,
    lastAttemptAt: null,
    nextAttemptAt: iso(now + jitter(projectId, generation, 0, intervalSeconds)),
    cooldownUntil: null,
    reasonCode: null,
    updatedAt: iso(now),
  });

  const reconcileProject = Effect.fn("AgentControlGithubObserveReactor.reconcileProject")(
    function* (projectId: ProjectId) {
      const observed = yield* loadObservedProject(projectId);
      if (Option.isNone(observed)) {
        yield* cleanupProject(projectId);
        return;
      }
      const now = yield* Clock.currentTimeMillis;
      const existing = yield* schedulerStates.get(projectId);
      if (Option.isNone(existing)) {
        yield* saveAndSchedule(
          freshState(
            projectId,
            observed.value.fingerprint,
            observed.value.intervalSeconds,
            1,
            observed.value.github.sequence,
            now,
          ),
        );
        return;
      }
      const state = existing.value;
      if (
        state.configFingerprint !== observed.value.fingerprint ||
        state.pollIntervalSeconds !== observed.value.intervalSeconds
      ) {
        yield* saveAndSchedule(
          freshState(
            projectId,
            observed.value.fingerprint,
            observed.value.intervalSeconds,
            state.generation + 1,
            observed.value.github.sequence,
            now,
          ),
        );
        return;
      }
      if (observed.value.github.sequence > state.lastGithubEventSequence) {
        yield* Queue.offer(messages, {
          _tag: "RecoverGithubOutcome",
          projectId,
        });
        return;
      }
      if (state.activity === "suspended") {
        yield* stopWorker(projectId);
        return;
      }
      if (state.circuitState === "half-open") {
        const cooldownUntil = now + AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS;
        yield* saveAndSchedule({
          ...state,
          circuitState: "open",
          nextAttemptAt: iso(cooldownUntil),
          cooldownUntil: iso(cooldownUntil),
          updatedAt: iso(now),
        });
        return;
      }
      if (state.nextAttemptAt === null) {
        yield* saveAndSchedule({
          ...state,
          nextAttemptAt: iso(
            now +
              state.pollIntervalSeconds * 1_000 +
              jitter(
                projectId,
                state.generation,
                state.consecutiveFailures,
                state.pollIntervalSeconds,
              ),
          ),
          reasonCode: "internal-coordination-error",
          updatedAt: iso(now),
        });
        return;
      }
      const worker = workers.get(projectId);
      if (worker?.generation === state.generation) return;
      yield* installTimer(state);
    },
  );

  const resetAfterSuccess = Effect.fn("AgentControlGithubObserveReactor.resetAfterSuccess")(
    function* (outcome: {
      readonly projectId: ProjectId;
      readonly sequence: number;
      readonly attemptedAt: string;
      readonly completedAt: string;
    }) {
      const projectId = outcome.projectId;
      const observed = yield* loadObservedProject(projectId);
      if (Option.isNone(observed)) {
        yield* cleanupProject(projectId);
        return;
      }
      const now = Date.parse(outcome.completedAt);
      const existing = yield* schedulerStates.get(projectId);
      if (Option.isSome(existing) && existing.value.lastGithubEventSequence >= outcome.sequence) {
        return;
      }
      const generation =
        Option.isSome(existing) && existing.value.configFingerprint === observed.value.fingerprint
          ? existing.value.generation
          : Option.isSome(existing)
            ? existing.value.generation + 1
            : 1;
      const nextAttempt =
        now +
        observed.value.intervalSeconds * 1_000 +
        jitter(projectId, generation, 0, observed.value.intervalSeconds);
      yield* saveAndSchedule({
        schemaVersion: 1,
        projectId,
        generation,
        configFingerprint: observed.value.fingerprint,
        pollIntervalSeconds: observed.value.intervalSeconds,
        lastGithubEventSequence: outcome.sequence,
        activity: "active",
        circuitState: "closed",
        consecutiveFailures: 0,
        lastAttemptAt: outcome.attemptedAt,
        nextAttemptAt: iso(nextAttempt),
        cooldownUntil: null,
        reasonCode: null,
        updatedAt: outcome.completedAt,
      });
    },
  );

  const applyPollFailure = Effect.fn("AgentControlGithubObserveReactor.applyPollFailure")(
    function* (outcome: {
      readonly projectId: ProjectId;
      readonly sequence: number;
      readonly attemptedAt: string;
      readonly completedAt: string;
      readonly errorCode: AgentControlGithubPollErrorCode;
    }) {
      const projectId = outcome.projectId;
      const observed = yield* loadObservedProject(projectId);
      if (Option.isNone(observed)) {
        yield* cleanupProject(projectId);
        return;
      }
      const existing = yield* schedulerStates.get(projectId);
      const existingState = Option.getOrUndefined(existing);
      if (
        existingState !== undefined &&
        existingState.lastGithubEventSequence >= outcome.sequence
      ) {
        return;
      }
      const configChanged =
        existingState !== undefined &&
        existingState.configFingerprint !== observed.value.fingerprint;
      const generation =
        existingState === undefined
          ? 1
          : configChanged
            ? existingState.generation + 1
            : existingState.generation;
      const previousFailures = configChanged ? 0 : (existingState?.consecutiveFailures ?? 0);
      const completedAt = Date.parse(outcome.completedAt);
      if (SUSPENDING_CODES.has(outcome.errorCode)) {
        yield* saveAndSchedule({
          schemaVersion: 1,
          projectId,
          generation,
          configFingerprint: observed.value.fingerprint,
          pollIntervalSeconds: observed.value.intervalSeconds,
          lastGithubEventSequence: outcome.sequence,
          activity: "suspended",
          circuitState: "open",
          consecutiveFailures: previousFailures,
          lastAttemptAt: outcome.attemptedAt,
          nextAttemptAt: null,
          cooldownUntil: null,
          reasonCode: pollFailureReason(outcome.errorCode),
          updatedAt: outcome.completedAt,
        });
        return;
      }
      if (!RETRYABLE_CODES.has(outcome.errorCode)) {
        if (
          !configChanged &&
          existingState !== undefined &&
          (existingState.activity === "suspended" || existingState.circuitState === "open")
        ) {
          yield* saveAndSchedule({
            ...existingState,
            lastGithubEventSequence: outcome.sequence,
            lastAttemptAt: outcome.attemptedAt,
            updatedAt: outcome.completedAt,
          });
          return;
        }
        const halfOpen = !configChanged && existingState?.circuitState === "half-open";
        const delay = halfOpen
          ? AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS
          : observed.value.intervalSeconds * 1_000 +
            jitter(projectId, generation, previousFailures, observed.value.intervalSeconds);
        const nextAttempt = completedAt + delay;
        yield* saveAndSchedule({
          schemaVersion: 1,
          projectId,
          generation,
          configFingerprint: observed.value.fingerprint,
          pollIntervalSeconds: observed.value.intervalSeconds,
          lastGithubEventSequence: outcome.sequence,
          activity: "active",
          circuitState: halfOpen ? "open" : "closed",
          consecutiveFailures: previousFailures,
          lastAttemptAt: outcome.attemptedAt,
          nextAttemptAt: iso(nextAttempt),
          cooldownUntil: halfOpen ? iso(nextAttempt) : null,
          reasonCode: pollFailureReason(outcome.errorCode),
          updatedAt: outcome.completedAt,
        });
        return;
      }
      const failures = previousFailures + 1;
      const open =
        failures >= AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD ||
        existingState?.circuitState === "half-open";
      const delay = open
        ? AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS
        : githubObserveBackoffMs(observed.value.intervalSeconds, failures, outcome.errorCode);
      const nextAttempt = completedAt + delay;
      yield* saveAndSchedule({
        schemaVersion: 1,
        projectId,
        generation,
        configFingerprint: observed.value.fingerprint,
        pollIntervalSeconds: observed.value.intervalSeconds,
        lastGithubEventSequence: outcome.sequence,
        activity: "active",
        circuitState: open ? "open" : "closed",
        consecutiveFailures: failures,
        lastAttemptAt: outcome.attemptedAt,
        nextAttemptAt: iso(nextAttempt),
        cooldownUntil: open ? iso(nextAttempt) : null,
        reasonCode: pollFailureReason(outcome.errorCode),
        updatedAt: outcome.completedAt,
      });
    },
  );

  const recoverCommittedGithubOutcome = Effect.fn(
    "AgentControlGithubObserveReactor.recoverCommittedGithubOutcome",
  )(function* (projectId: ProjectId) {
    const observed = yield* loadObservedProject(projectId);
    if (Option.isNone(observed)) {
      yield* cleanupProject(projectId);
      return;
    }
    const existing = yield* schedulerStates.get(projectId);
    if (
      Option.isNone(existing) ||
      observed.value.github.sequence <= existing.value.lastGithubEventSequence
    ) {
      yield* reconcileProject(projectId);
      return;
    }
    const pollStatus = observed.value.github.pollStatus;
    if (pollStatus.status === "success") {
      yield* resetAfterSuccess({
        projectId,
        sequence: observed.value.github.sequence,
        attemptedAt: pollStatus.attemptedAt,
        completedAt: pollStatus.completedAt,
      });
      return;
    }
    if (pollStatus.status === "needs-attention") {
      yield* applyPollFailure({
        projectId,
        sequence: observed.value.github.sequence,
        attemptedAt: pollStatus.attemptedAt,
        completedAt: pollStatus.completedAt,
        errorCode: pollStatus.errorCode,
      });
      return;
    }
    yield* schedulerStates.save({
      ...existing.value,
      lastGithubEventSequence: observed.value.github.sequence,
    });
    yield* reconcileProject(projectId);
  });

  const processGithubEvent = Effect.fn("AgentControlGithubObserveReactor.processGithubEvent")(
    function* (event: AgentControlGithubEvent) {
      switch (event.type) {
        case "agentControl.github.config.set":
          yield* reconcileProject(event.aggregateId);
          return;
        case "agentControl.github.config.cleared":
          yield* cleanupProject(event.aggregateId);
          return;
        case "agentControl.github.poll.succeeded":
          yield* resetAfterSuccess({
            projectId: event.aggregateId,
            sequence: event.sequence,
            attemptedAt: event.payload.attemptedAt,
            completedAt: event.payload.completedAt,
          });
          return;
        case "agentControl.github.poll.failed":
          yield* applyPollFailure({
            projectId: event.aggregateId,
            sequence: event.sequence,
            attemptedAt: event.payload.attemptedAt,
            completedAt: event.payload.completedAt,
            errorCode: event.payload.errorCode,
          });
          return;
      }
    },
  );

  const processTimerDue = Effect.fn("AgentControlGithubObserveReactor.processTimerDue")(function* (
    message: Extract<ReactorMessage, { _tag: "TimerDue" }>,
  ) {
    const worker = workers.get(message.projectId);
    if (
      worker === undefined ||
      worker.generation !== message.generation ||
      worker.phase !== "timer"
    ) {
      return;
    }
    const observed = yield* loadObservedProject(message.projectId);
    if (Option.isNone(observed)) {
      yield* cleanupProject(message.projectId);
      return;
    }
    const persisted = yield* schedulerStates.get(message.projectId);
    if (
      Option.isNone(persisted) ||
      persisted.value.generation !== message.generation ||
      persisted.value.activity !== "active"
    ) {
      yield* stopWorker(message.projectId);
      return;
    }
    const state = persisted.value;
    const now = yield* Clock.currentTimeMillis;
    const nextAttempt = millis(state.nextAttemptAt);
    if (nextAttempt !== null && nextAttempt > now) {
      yield* installTimer(state);
      return;
    }
    const halfOpen = state.circuitState === "open";
    const attemptState: AgentControlGithubSchedulerState = {
      ...state,
      circuitState: halfOpen ? "half-open" : state.circuitState,
      lastAttemptAt: iso(now),
      nextAttemptAt: null,
      updatedAt: iso(now),
    };
    yield* schedulerStates.save(attemptState);
    worker.phase = "poll";

    const attempt = Effect.gen(function* () {
      // The intake revision is deliberately loaded immediately before each
      // real attempt; every attempt receives a fresh server-owned command id.
      const intakeState = yield* githubIntake.getObserveState({
        projectId: message.projectId,
      });
      const commandId = CommandId.make(`server:github-observe:${yield* crypto.randomUUIDv4}`);
      yield* githubIntake.pollOnce({
        commandId,
        projectId: message.projectId,
        expectedRevision: intakeState.revision,
      });
    }).pipe(
      Effect.catch((error) =>
        Queue.offer(messages, {
          _tag: "PollCoordination",
          projectId: message.projectId,
          generation: message.generation,
          code: isGithubRpcError(error)
            ? makeReasonCode(error.code)
            : "internal-coordination-error",
        }),
      ),
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
        return Queue.offer(messages, {
          _tag: "PollCoordination",
          projectId: message.projectId,
          generation: message.generation,
          code: "internal-coordination-error",
        });
      }),
    );
    yield* Effect.forkIn(worker.scope)(attempt);
  });

  const processPollCoordination = Effect.fn(
    "AgentControlGithubObserveReactor.processPollCoordination",
  )(function* (message: Extract<ReactorMessage, { _tag: "PollCoordination" }>) {
    if (message.code === "project-unavailable" || message.code === "tracker-not-configured") {
      yield* cleanupProject(message.projectId);
      return;
    }
    const observed = yield* loadObservedProject(message.projectId);
    if (Option.isNone(observed)) {
      yield* cleanupProject(message.projectId);
      return;
    }
    const persisted = yield* schedulerStates.get(message.projectId);
    if (
      Option.isNone(persisted) ||
      persisted.value.generation !== message.generation ||
      persisted.value.nextAttemptAt !== null
    ) {
      return;
    }
    const now = yield* Clock.currentTimeMillis;
    const state = persisted.value;
    const next =
      now +
      state.pollIntervalSeconds * 1_000 +
      jitter(
        message.projectId,
        state.generation,
        state.consecutiveFailures,
        state.pollIntervalSeconds,
      );
    yield* saveAndSchedule({
      ...state,
      circuitState: state.circuitState === "half-open" ? "open" : state.circuitState,
      nextAttemptAt:
        state.circuitState === "half-open"
          ? iso(now + AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS)
          : iso(next),
      cooldownUntil:
        state.circuitState === "half-open"
          ? iso(now + AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS)
          : state.cooldownUntil,
      reasonCode: message.code,
      updatedAt: iso(now),
    });
  });

  const processMessage = Effect.fn("AgentControlGithubObserveReactor.processMessage")(function* (
    message: ReactorMessage,
  ) {
    switch (message._tag) {
      case "ProjectChanged":
        yield* reconcileProject(message.projectId);
        return;
      case "RecoverGithubOutcome":
        yield* recoverCommittedGithubOutcome(message.projectId);
        return;
      case "GithubEvent":
        yield* processGithubEvent(message.event);
        return;
      case "ProjectDeleted":
        yield* cleanupProject(message.projectId);
        return;
      case "TimerDue":
        yield* processTimerDue(message);
        return;
      case "PollCoordination":
        yield* processPollCoordination(message);
        return;
    }
  });

  const processMessageSafely = (message: ReactorMessage) =>
    processMessage(message).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
        const projectId =
          message._tag === "GithubEvent" ? message.event.aggregateId : message.projectId;
        return stopWorker(projectId).pipe(
          Effect.andThen(
            Effect.logWarning("Agent Control GitHub Observe reconcile failed", {
              projectId,
              status: "skipped",
              reasonCode: "internal-coordination-error",
            }),
          ),
        );
      }),
    );

  const launchSubscription = <A>(
    stream: Stream.Stream<A>,
    toMessage: (event: A) => ReactorMessage | null,
  ) =>
    Stream.runForEach(stream, (event) => {
      const message = toMessage(event);
      return message === null ? Effect.void : Queue.offer(messages, message);
    }).pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
        return Effect.logWarning("Agent Control reactor event subscription stopped", {
          status: "inactive",
          reasonCode: "internal-coordination-error",
        });
      }),
      Effect.forkScoped,
    );

  const start: AgentControlGithubObserveReactorShape["start"] = Effect.fn(
    "AgentControlGithubObserveReactor.start",
  )(function* () {
    if (yield* Ref.getAndSet(started, true)) return;
    yield* Effect.addFinalizer(() => stopAllWorkers());
    yield* Queue.take(messages).pipe(
      Effect.flatMap(processMessageSafely),
      Effect.forever,
      Effect.forkScoped,
    );

    // Attach all hot subscriptions before reading persisted projections.
    yield* launchSubscription(projectController.streamDomainEvents, (event) => ({
      _tag: "ProjectChanged",
      projectId: event.aggregateId,
    }));
    yield* launchSubscription(githubIntake.streamDomainEvents, (event) => ({
      _tag: "GithubEvent",
      event,
    }));
    yield* launchSubscription(orchestration.streamDomainEvents, (event: OrchestrationEvent) =>
      event.type === "project.deleted"
        ? { _tag: "ProjectDeleted", projectId: event.payload.projectId }
        : null,
    );
    yield* Effect.yieldNow;

    const enumeration = yield* Effect.result(projectStates.listPersisted);
    if (enumeration._tag === "Failure") {
      yield* Effect.logWarning("Agent Control GitHub Observe startup recovery skipped", {
        status: "inactive",
        reasonCode: "internal-coordination-error",
      });
      return;
    }
    for (const entry of enumeration.success) {
      if (entry._tag === "Corrupt") {
        if (entry.projectId !== null) {
          yield* processMessageSafely({
            _tag: "ProjectDeleted",
            projectId: entry.projectId,
          });
        }
        yield* Effect.logWarning("Agent Control project projection quarantined", {
          projectId: entry.projectId,
          status: "inactive",
          reasonCode: "internal-coordination-error",
        });
        continue;
      }
      yield* processMessageSafely({
        _tag: "ProjectChanged",
        projectId: entry.state.projectId,
      });
    }
    // Give any event queued during the scan a chance to reconcile before
    // command readiness is opened.
    yield* Effect.yieldNow;
  });

  const getStatus: AgentControlGithubObserveReactorShape["getStatus"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeProjectInput(rawInput).pipe(
        Effect.mapError(() =>
          rpcError(String(rawInput.projectId ?? "invalid") as ProjectId, "validation"),
        ),
      );
      yield* availability
        .ensureAvailable(input.projectId)
        .pipe(
          Effect.mapError((error) =>
            rpcError(
              input.projectId,
              error._tag === "AgentControlProjectUnavailableError"
                ? error.reason === "deleted"
                  ? "project-deleted"
                  : "project-missing"
                : "internal-persistence-error",
            ),
          ),
        );
      const persisted = yield* schedulerStates
        .get(input.projectId)
        .pipe(Effect.mapError(() => rpcError(input.projectId)));
      if (Option.isNone(persisted)) {
        return {
          projectId: input.projectId,
          activity: "inactive" as const,
          circuitState: "closed" as const,
          consecutiveFailures: 0,
          lastAttemptAt: null,
          nextAttemptAt: null,
          reasonCode: null,
        };
      }
      const state = persisted.value;
      return {
        projectId: state.projectId,
        activity: state.activity,
        circuitState: state.circuitState,
        consecutiveFailures: state.consecutiveFailures,
        lastAttemptAt: state.lastAttemptAt,
        nextAttemptAt: state.nextAttemptAt,
        reasonCode: state.reasonCode,
      };
    });

  return AgentControlGithubObserveReactor.of({ start, getStatus });
});

export const layer = Layer.effect(AgentControlGithubObserveReactor, make());
