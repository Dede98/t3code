import {
  AgentControlGithubProjectInput,
  type AgentControlGithubEvent,
  type AgentControlGithubReactorHealth,
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
import * as Deferred from "effect/Deferred";
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
import {
  AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS,
  AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD,
  AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS,
  githubObserveBackoffMs,
  githubObserveConfigFingerprint,
  githubObserveProjectionMatches,
  reduceGithubObserveScheduler,
  type GithubObserveSchedulerFold,
} from "../githubObserveScheduler.ts";
import { AgentControlGithubEventStore } from "../Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubIntake } from "../Services/AgentControlGithubIntake.ts";
import {
  AgentControlGithubObserveReactor,
  AgentControlGithubObserveStartupError,
  type AgentControlGithubObserveReactorShape,
} from "../Services/AgentControlGithubObserveReactor.ts";
import {
  type AgentControlGithubSchedulerState,
  AgentControlGithubSchedulerStateRepository,
} from "../Services/AgentControlGithubSchedulerState.ts";
import { AgentControlGithubStateRepository } from "../Services/AgentControlGithubStateRepository.ts";

export {
  AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS,
  AGENT_CONTROL_GITHUB_CIRCUIT_FAILURE_THRESHOLD,
  AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS,
  githubObserveBackoffMs,
};

const isGithubRpcError = Schema.is(AgentControlGithubRpcError);
const decodeProjectInput = Schema.decodeUnknownEffect(AgentControlGithubProjectInput);

const iso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
const millis = (value: string | null) => (value === null ? null : Date.parse(value));

const STARTUP_MAX_ATTEMPTS = 3;
const STARTUP_RETRY_BASE_MS = 100;
const RECOVERY_RETRY_BASE_MS = 1_000;
const RECOVERY_RETRY_MAX_MS = 60_000;
const SUBSCRIPTION_RETRY_BASE_MS = 250;
const SUBSCRIPTION_RETRY_MAX_MS = 30_000;
const SUBSCRIPTION_STARTUP_TIMEOUT_MS = 10_000;
const WATCHDOG_INTERVAL_MS = 5 * 60_000;
const REPLAY_PAGE_SIZE = 100;

export interface AgentControlGithubObserveReactorOptions {
  readonly jitterMillis?: (input: {
    readonly projectId: ProjectId;
    readonly generation: number;
    readonly attempt: number;
    readonly pollIntervalSeconds: number;
  }) => number;
  readonly startupMaxAttempts?: number;
  readonly startupRetryBaseMs?: number;
  readonly recoveryRetryBaseMs?: number;
  readonly recoveryRetryMaxMs?: number;
  readonly subscriptionRetryBaseMs?: number;
  readonly subscriptionRetryMaxMs?: number;
  readonly subscriptionStartupTimeoutMs?: number;
  readonly watchdogIntervalMs?: number;
  readonly replayPageSize?: number;
  /** Deterministic synchronization seams for focused reactor tests. */
  readonly testHooks?: {
    readonly acquireRuntimeResource?: Effect.Effect<void, never, Scope.Scope>;
    readonly lifecycleEvent?: (event: ReactorLifecycleTestEvent) => Effect.Effect<void>;
  };
}

type ReactorLifecycleTestEvent =
  | { readonly _tag: "project-reconciled"; readonly projectId: ProjectId }
  | { readonly _tag: "closing"; readonly attemptId: number }
  | { readonly _tag: "waiting-for-closing"; readonly attemptId: number }
  | { readonly _tag: "shutdown-completed"; readonly attemptId: number };

interface SchedulerToken {
  readonly generation: number;
  readonly schedulerRevision: number;
}

interface ActiveWorker extends SchedulerToken {
  readonly scope: Scope.Closeable;
  phase: "timer" | "poll";
}

type SubscriptionName = "project-controller" | "github-intake" | "project-delete";

interface RuntimeAttempt {
  readonly id: number;
  readonly scope: Scope.Closeable;
  readonly completion: Deferred.Deferred<void, AgentControlGithubObserveStartupError>;
  readonly shutdownRequested: Deferred.Deferred<void>;
  readonly shutdownCompletion: Deferred.Deferred<void>;
  closed: boolean;
}

type ReactorMessage =
  | { readonly _tag: "ReconcileProject"; readonly projectId: ProjectId }
  | { readonly _tag: "ProjectDeleted"; readonly projectId: ProjectId }
  | { readonly _tag: "QuarantineProject"; readonly projectId: ProjectId | null }
  | {
      readonly _tag: "TimerDue";
      readonly projectId: ProjectId;
      readonly generation: number;
      readonly schedulerRevision: number;
    }
  | {
      readonly _tag: "PollCoordination";
      readonly projectId: ProjectId;
      readonly generation: number;
      readonly schedulerRevision: number;
      readonly code: AgentControlGithubReactorReasonCode;
    }
  | { readonly _tag: "FullReconcile"; readonly attemptId: number }
  | {
      readonly _tag: "FullReconcileBarrier";
      readonly attemptId: number;
      readonly epoch: number;
    }
  | { readonly _tag: "Barrier" };

interface ReactorEnvelope {
  readonly message: ReactorMessage;
  readonly acknowledgement?: Deferred.Deferred<void, AgentControlGithubObserveStartupError>;
}

type ReactorLifecycle =
  | { readonly _tag: "idle" }
  | {
      readonly _tag: "starting";
      readonly attemptId: number;
      readonly completion: Deferred.Deferred<void, AgentControlGithubObserveStartupError>;
    }
  | {
      readonly _tag: "started";
      readonly attemptId: number;
      readonly scope: Scope.Closeable;
    }
  | {
      readonly _tag: "closing";
      readonly attemptId: number;
      readonly shutdownCompletion: Deferred.Deferred<void>;
    };

type StartDecision =
  | {
      readonly _tag: "launch";
      readonly attemptId: number;
      readonly completion: Deferred.Deferred<void, AgentControlGithubObserveStartupError>;
    }
  | {
      readonly _tag: "wait";
      readonly completion: Deferred.Deferred<void, AgentControlGithubObserveStartupError>;
    }
  | {
      readonly _tag: "wait-for-closing";
      readonly attemptId: number;
      readonly shutdownCompletion: Deferred.Deferred<void>;
    }
  | { readonly _tag: "done" };

class AgentControlGithubObserveRecoveryError extends Schema.TaggedError<AgentControlGithubObserveRecoveryError>()(
  "AgentControlGithubObserveRecoveryError",
  {
    projectId: Schema.NullOr(Schema.String),
    reason: Schema.Literals(["projection-unavailable", "history-incomplete"]),
  },
) {}

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

const boundedInt = (value: number | undefined, fallback: number, minimum = 1) =>
  Math.max(minimum, Math.floor(value ?? fallback));

const tokenCompare = (left: SchedulerToken, right: SchedulerToken) =>
  left.generation === right.generation
    ? left.schedulerRevision - right.schedulerRevision
    : left.generation - right.generation;

const projectIdOf = (message: ReactorMessage): ProjectId | null => {
  switch (message._tag) {
    case "ReconcileProject":
    case "ProjectDeleted":
    case "TimerDue":
    case "PollCoordination":
      return message.projectId;
    case "QuarantineProject":
      return message.projectId;
    case "FullReconcile":
    case "FullReconcileBarrier":
    case "Barrier":
      return null;
  }
};

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

const startupError = (reason: AgentControlGithubObserveStartupError["reason"]) =>
  new AgentControlGithubObserveStartupError({ reason });

export const make = Effect.fn("AgentControlGithubObserveReactor.make")(function* (
  options: AgentControlGithubObserveReactorOptions = {},
) {
  const crypto = yield* Crypto.Crypto;
  const projectController = yield* AgentControlEngine;
  const githubIntake = yield* AgentControlGithubIntake;
  const githubEvents = yield* AgentControlGithubEventStore;
  const githubStates = yield* AgentControlGithubStateRepository;
  const schedulerStates = yield* AgentControlGithubSchedulerStateRepository;
  const projectStates = yield* AgentControlProjectStateRepository;
  const availability = yield* AgentControlProjectAvailability;
  const orchestration = yield* OrchestrationEngineService;
  const messages = yield* Queue.unbounded<ReactorEnvelope>();
  const lifecycle = yield* Ref.make<ReactorLifecycle>({ _tag: "idle" });

  const workers = new Map<string, ActiveWorker>();
  const recoveryAttempts = new Map<string, number>();
  const recoveryScheduled = new Set<string>();
  const recoveringProjects = new Set<string>();
  const reconcilingProjects = new Set<string>();
  const queuedReconciles = new Set<string>();
  const subscriptionHealth = new Map<SubscriptionName, AgentControlGithubReactorHealth>([
    ["project-controller", "recovering"],
    ["github-intake", "recovering"],
    ["project-delete", "recovering"],
  ]);
  let activeRuntime: RuntimeAttempt | null = null;
  let nextAttemptId = 0;
  let startupPreviouslyFailed = false;
  let fullReconcileRequestedEpoch = 0;
  let fullReconcileCompletedEpoch = 0;
  let fullReconcileQueued = false;
  let fullReconcileRunning = false;
  let fullReconcileRunningEpoch: number | null = null;
  let globalRecoveryAttempt = 0;
  let globalRecoveryScheduled = false;
  let globalRecoveryToken = 0;

  const jitterMillis = options.jitterMillis ?? defaultJitterMillis;
  const startupMaxAttempts = boundedInt(options.startupMaxAttempts, STARTUP_MAX_ATTEMPTS);
  const startupRetryBaseMs = boundedInt(options.startupRetryBaseMs, STARTUP_RETRY_BASE_MS);
  const recoveryRetryBaseMs = boundedInt(options.recoveryRetryBaseMs, RECOVERY_RETRY_BASE_MS);
  const recoveryRetryMaxMs = boundedInt(options.recoveryRetryMaxMs, RECOVERY_RETRY_MAX_MS);
  const subscriptionRetryBaseMs = boundedInt(
    options.subscriptionRetryBaseMs,
    SUBSCRIPTION_RETRY_BASE_MS,
  );
  const subscriptionRetryMaxMs = boundedInt(
    options.subscriptionRetryMaxMs,
    SUBSCRIPTION_RETRY_MAX_MS,
  );
  const subscriptionStartupTimeoutMs = boundedInt(
    options.subscriptionStartupTimeoutMs,
    SUBSCRIPTION_STARTUP_TIMEOUT_MS,
  );
  const watchdogIntervalMs = boundedInt(options.watchdogIntervalMs, WATCHDOG_INTERVAL_MS);
  const replayPageSize = Math.min(1_000, boundedInt(options.replayPageSize, REPLAY_PAGE_SIZE));

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

  const stopWorkerUpTo = Effect.fn("AgentControlGithubObserveReactor.stopWorkerUpTo")(function* (
    projectId: ProjectId,
    token?: SchedulerToken,
  ) {
    const worker = workers.get(projectId);
    if (worker === undefined) return;
    if (token !== undefined && tokenCompare(worker, token) > 0) return;
    workers.delete(projectId);
    yield* Scope.close(worker.scope, Exit.void).pipe(Effect.ignore);
  });

  const stopAllWorkers = Effect.fn("AgentControlGithubObserveReactor.stopAllWorkers")(function* () {
    const active = [...workers.values()];
    workers.clear();
    let closeCause: Cause.Cause<never> | null = null;
    for (const worker of active) {
      const closeExit = yield* Effect.exit(Scope.close(worker.scope, Exit.void));
      if (Exit.isFailure(closeExit)) {
        closeCause =
          closeCause === null ? closeExit.cause : Cause.combine(closeCause, closeExit.cause);
      }
    }
    if (closeCause !== null) return yield* Effect.failCause(closeCause);
  });

  const installTimer = Effect.fn("AgentControlGithubObserveReactor.installTimer")(function* (
    state: AgentControlGithubSchedulerState,
  ) {
    const candidate: SchedulerToken = state;
    const existing = workers.get(state.projectId);
    if (existing !== undefined) {
      const comparison = tokenCompare(existing, candidate);
      if (comparison > 0) return;
      if (comparison === 0 && state.activity === "active" && state.nextAttemptAt !== null) {
        return;
      }
      yield* stopWorkerUpTo(state.projectId, candidate);
    }
    if (state.activity !== "active" || state.nextAttemptAt === null) return;

    const dueAt = millis(state.nextAttemptAt);
    if (dueAt === null || !Number.isFinite(dueAt)) return;
    const runtime = activeRuntime;
    if (runtime === null || runtime.closed) return;
    const now = yield* Clock.currentTimeMillis;
    const workerScope = yield* Scope.fork(runtime.scope, "sequential");
    if (runtime.closed || activeRuntime !== runtime) {
      yield* Scope.close(workerScope, Exit.void).pipe(Effect.ignore);
      return;
    }
    const worker: ActiveWorker = {
      generation: state.generation,
      schedulerRevision: state.schedulerRevision,
      scope: workerScope,
      phase: "timer",
    };
    workers.set(state.projectId, worker);
    yield* Effect.sleep(Duration.millis(Math.max(0, dueAt - now))).pipe(
      Effect.andThen(
        Queue.offer(messages, {
          message: {
            _tag: "TimerDue",
            projectId: state.projectId,
            generation: state.generation,
            schedulerRevision: state.schedulerRevision,
          },
        }),
      ),
      Effect.forkIn(workerScope),
    );
  });

  const saveConfirmed = Effect.fn("AgentControlGithubObserveReactor.saveConfirmed")(function* (
    rawState: AgentControlGithubSchedulerState,
    expectedRevision: number,
    schedule: boolean,
  ) {
    const next: AgentControlGithubSchedulerState = {
      ...rawState,
      schedulerRevision: expectedRevision + 1,
    };
    const confirmed = yield* schedulerStates.save(next, expectedRevision);
    if (schedule) yield* installTimer(confirmed);
    return confirmed;
  });

  const cleanupProject = Effect.fn("AgentControlGithubObserveReactor.cleanupProject")(function* (
    projectId: ProjectId,
  ) {
    // Definitive cleanup is stop-first. Persistence recovery must never leave a
    // poll running after deletion, quarantine, config clear, or Observe exit.
    yield* stopWorkerUpTo(projectId);
    const existing = yield* schedulerStates.get(projectId);
    if (Option.isNone(existing)) return;
    yield* schedulerStates.delete(projectId, existing.value.schedulerRevision);
  });

  const loadObservedProject = Effect.fn("AgentControlGithubObserveReactor.loadObservedProject")(
    function* (projectId: ProjectId) {
      const unavailable = yield* availability.ensureAvailable(projectId).pipe(
        Effect.as(false),
        Effect.catchTag("AgentControlProjectUnavailableError", () => Effect.succeed(true)),
      );
      if (unavailable) return Option.none();

      const project = yield* projectStates.get(projectId);
      // Before the first mode change, an available project has the same implicit
      // manual state as AgentControlEngine.getProjectState; configuration alone
      // must not start polling or persist a controller mode.
      if (Option.isNone(project)) return Option.none();
      if (project.value.mode !== "observe" && project.value.mode !== "armed") {
        return Option.none();
      }

      const github = yield* githubStates.get(projectId);
      if (Option.isNone(github) || github.value.config === null) return Option.none();
      return Option.some({
        project: project.value,
        github: github.value,
        config: github.value.config,
        fingerprint: githubObserveConfigFingerprint(github.value.config),
        intervalSeconds: github.value.config.settings.pollIntervalSeconds,
      });
    },
  );

  const replayProject = Effect.fn("AgentControlGithubObserveReactor.replayProject")(function* (
    projectId: ProjectId,
  ) {
    const observed = yield* loadObservedProject(projectId);
    if (Option.isNone(observed)) {
      yield* cleanupProject(projectId);
      return;
    }

    const existing = yield* schedulerStates.get(projectId);
    let fold: GithubObserveSchedulerFold = Option.match(existing, {
      onNone: () => ({ state: null, lastGeneration: 0 }),
      onSome: (state) => ({ state, lastGeneration: state.generation }),
    });
    let cursor = fold.state?.lastGithubEventSequence ?? 0;
    let appliedAny = false;

    while (true) {
      const page = yield* githubEvents.readProjectAfterSequence(projectId, cursor, replayPageSize);
      if (page.length === 0) break;

      for (const event of page) {
        if (event.aggregateId !== projectId || event.sequence <= cursor) {
          return yield* new AgentControlGithubObserveRecoveryError({
            projectId,
            reason: "history-incomplete",
          });
        }
        const now = yield* Clock.currentTimeMillis;
        const previous = fold.state;
        const transitioned = reduceGithubObserveScheduler(fold, event, {
          now,
          jitterMillis: jitter,
        });
        const next = transitioned.state;

        if (previous !== next) {
          if (next === null) {
            if (previous !== null) {
              yield* stopWorkerUpTo(projectId);
              yield* schedulerStates.delete(projectId, previous.schedulerRevision);
            }
          } else {
            const expectedRevision = previous?.schedulerRevision ?? 0;
            const confirmed = yield* saveConfirmed(next, expectedRevision, false);
            fold = { state: confirmed, lastGeneration: transitioned.lastGeneration };
          }
        }
        if (next === null) {
          fold = transitioned;
        }
        cursor = event.sequence;
        appliedAny = true;
      }
      if (page.length < replayPageSize) break;
    }

    const state = fold.state;
    if (state === null) {
      return yield* new AgentControlGithubObserveRecoveryError({
        projectId,
        reason: "history-incomplete",
      });
    }
    if (!githubObserveProjectionMatches(state, observed.value.config)) {
      return yield* new AgentControlGithubObserveRecoveryError({
        projectId,
        reason: "history-incomplete",
      });
    }

    const now = yield* Clock.currentTimeMillis;
    if (state.activity === "suspended") {
      yield* stopWorkerUpTo(projectId, state);
      return;
    }
    if (!appliedAny && state.circuitState === "half-open") {
      const cooldownUntil = now + AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS;
      yield* saveConfirmed(
        {
          ...state,
          circuitState: "open",
          nextAttemptAt: iso(cooldownUntil),
          cooldownUntil: iso(cooldownUntil),
          reasonCode: "internal-coordination-error",
          updatedAt: iso(now),
        },
        state.schedulerRevision,
        true,
      );
      return;
    }
    if (state.nextAttemptAt === null) {
      yield* saveConfirmed(
        {
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
        },
        state.schedulerRevision,
        true,
      );
      return;
    }
    yield* installTimer(state);
  });

  const enqueueReconcile = Effect.fn("AgentControlGithubObserveReactor.enqueueReconcile")(
    function* (projectId: ProjectId) {
      if (queuedReconciles.has(projectId)) return;
      queuedReconciles.add(projectId);
      yield* Queue.offer(messages, {
        message: { _tag: "ReconcileProject", projectId },
      });
    },
  );

  const enqueuePendingFullReconcile = Effect.fn(
    "AgentControlGithubObserveReactor.enqueuePendingFullReconcile",
  )(function* () {
    const runtime = activeRuntime;
    if (
      runtime === null ||
      runtime.closed ||
      fullReconcileRequestedEpoch <= fullReconcileCompletedEpoch ||
      fullReconcileQueued ||
      fullReconcileRunning
    ) {
      return;
    }
    fullReconcileQueued = true;
    yield* Queue.offer(messages, {
      message: { _tag: "FullReconcile", attemptId: runtime.id },
    });
  });

  const enqueueFullReconcile = Effect.fn("AgentControlGithubObserveReactor.enqueueFullReconcile")(
    function* () {
      const runtime = activeRuntime;
      if (runtime === null || runtime.closed) return;
      fullReconcileRequestedEpoch += 1;
      yield* enqueuePendingFullReconcile();
    },
  );

  const scheduleProjectRecovery = Effect.fn(
    "AgentControlGithubObserveReactor.scheduleProjectRecovery",
  )(function* (projectId: ProjectId) {
    recoveringProjects.add(projectId);
    if (recoveryScheduled.has(projectId)) return;
    const runtime = activeRuntime;
    if (runtime === null || runtime.closed) return;
    const attempt = (recoveryAttempts.get(projectId) ?? 0) + 1;
    recoveryAttempts.set(projectId, attempt);
    recoveryScheduled.add(projectId);
    const delay = Math.min(
      recoveryRetryMaxMs,
      recoveryRetryBaseMs * 2 ** Math.min(20, attempt - 1),
    );
    yield* Effect.sleep(Duration.millis(delay)).pipe(
      Effect.andThen(
        Effect.suspend(() => {
          if (
            !recoveryScheduled.has(projectId) ||
            !recoveringProjects.has(projectId) ||
            recoveryAttempts.get(projectId) !== attempt
          ) {
            return Effect.void;
          }
          recoveryScheduled.delete(projectId);
          return enqueueReconcile(projectId);
        }),
      ),
      Effect.forkIn(runtime.scope),
    );
  });

  const scheduleGlobalRecovery = Effect.fn(
    "AgentControlGithubObserveReactor.scheduleGlobalRecovery",
  )(function* () {
    globalRecoveryAttempt += 1;
    if (globalRecoveryScheduled) return;
    const runtime = activeRuntime;
    if (runtime === null || runtime.closed) return;
    globalRecoveryToken += 1;
    const token = globalRecoveryToken;
    globalRecoveryScheduled = true;
    const delay = Math.min(
      recoveryRetryMaxMs,
      recoveryRetryBaseMs * 2 ** Math.min(20, globalRecoveryAttempt - 1),
    );
    yield* Effect.sleep(Duration.millis(delay)).pipe(
      Effect.andThen(
        Effect.suspend(() => {
          if (
            !globalRecoveryScheduled ||
            globalRecoveryToken !== token ||
            runtime.closed ||
            activeRuntime !== runtime
          ) {
            return Effect.void;
          }
          globalRecoveryScheduled = false;
          return enqueuePendingFullReconcile();
        }),
      ),
      Effect.forkIn(runtime.scope),
    );
  });

  const enumerateForWatchdog = Effect.fn("AgentControlGithubObserveReactor.enumerateForWatchdog")(
    function* () {
      const entries = yield* projectStates.listPersisted;
      for (const entry of entries) {
        if (entry._tag === "Corrupt") {
          yield* Queue.offer(messages, {
            message: { _tag: "QuarantineProject", projectId: entry.projectId },
          });
        } else {
          yield* enqueueReconcile(entry.state.projectId);
        }
      }
    },
  );

  const processTimerDue = Effect.fn("AgentControlGithubObserveReactor.processTimerDue")(function* (
    message: Extract<ReactorMessage, { _tag: "TimerDue" }>,
  ) {
    const worker = workers.get(message.projectId);
    if (worker === undefined || worker.phase !== "timer" || tokenCompare(worker, message) !== 0) {
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
      tokenCompare(persisted.value, message) !== 0 ||
      persisted.value.activity !== "active"
    ) {
      yield* stopWorkerUpTo(message.projectId, message);
      return;
    }

    const state = persisted.value;
    const now = yield* Clock.currentTimeMillis;
    const nextAttempt = millis(state.nextAttemptAt);
    if (nextAttempt !== null && nextAttempt > now) {
      yield* stopWorkerUpTo(message.projectId, message);
      yield* installTimer(state);
      return;
    }

    const attemptState = yield* saveConfirmed(
      {
        ...state,
        circuitState: state.circuitState === "open" ? "half-open" : state.circuitState,
        lastAttemptAt: iso(now),
        nextAttemptAt: null,
        updatedAt: iso(now),
      },
      state.schedulerRevision,
      false,
    );
    const pollWorker: ActiveWorker = {
      generation: attemptState.generation,
      schedulerRevision: attemptState.schedulerRevision,
      scope: worker.scope,
      phase: "poll",
    };
    workers.set(message.projectId, pollWorker);

    const attempt = Effect.gen(function* () {
      const intakeState = yield* githubIntake.getObserveState({
        projectId: message.projectId,
      });
      const commandId = CommandId.make(`server:github-observe:${yield* crypto.randomUUIDv4}`);
      yield* githubIntake.pollOnce({
        commandId,
        projectId: message.projectId,
        expectedRevision: intakeState.revision,
      });
      yield* enqueueReconcile(message.projectId);
    }).pipe(
      Effect.catch((error) =>
        Queue.offer(messages, {
          message: {
            _tag: "PollCoordination",
            projectId: message.projectId,
            generation: attemptState.generation,
            schedulerRevision: attemptState.schedulerRevision,
            code: isGithubRpcError(error)
              ? makeReasonCode(error.code)
              : "internal-coordination-error",
          },
        }),
      ),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.interrupt
          : Queue.offer(messages, {
              message: {
                _tag: "PollCoordination",
                projectId: message.projectId,
                generation: attemptState.generation,
                schedulerRevision: attemptState.schedulerRevision,
                code: "internal-coordination-error",
              },
            }),
      ),
    );
    yield* Effect.forkIn(pollWorker.scope)(attempt);
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
      tokenCompare(persisted.value, message) !== 0 ||
      persisted.value.nextAttemptAt !== null
    ) {
      return;
    }

    const now = yield* Clock.currentTimeMillis;
    const state = persisted.value;
    const halfOpen = state.circuitState === "half-open";
    const dueAt = halfOpen
      ? now + AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS
      : now +
        state.pollIntervalSeconds * 1_000 +
        jitter(
          message.projectId,
          state.generation,
          state.consecutiveFailures,
          state.pollIntervalSeconds,
        );
    yield* saveConfirmed(
      {
        ...state,
        circuitState: halfOpen ? "open" : state.circuitState,
        nextAttemptAt: iso(dueAt),
        cooldownUntil: halfOpen ? iso(dueAt) : state.cooldownUntil,
        reasonCode: message.code,
        updatedAt: iso(now),
      },
      state.schedulerRevision,
      true,
    );
  });

  const processMessage = Effect.fn("AgentControlGithubObserveReactor.processMessage")(function* (
    message: ReactorMessage,
  ) {
    switch (message._tag) {
      case "ReconcileProject":
        yield* replayProject(message.projectId);
        return;
      case "ProjectDeleted":
        yield* cleanupProject(message.projectId);
        return;
      case "QuarantineProject":
        if (message.projectId !== null) yield* cleanupProject(message.projectId);
        yield* Effect.logWarning("Agent Control project projection quarantined", {
          projectId: message.projectId,
          status: "inactive",
          reasonCode: "internal-coordination-error",
        });
        return;
      case "TimerDue":
        yield* processTimerDue(message);
        return;
      case "PollCoordination":
        yield* processPollCoordination(message);
        return;
      case "FullReconcile": {
        const passEpoch = fullReconcileRunningEpoch;
        if (passEpoch === null) return;
        yield* enumerateForWatchdog();
        if (
          activeRuntime === null ||
          activeRuntime.closed ||
          activeRuntime.id !== message.attemptId
        ) {
          return;
        }
        yield* Queue.offer(messages, {
          message: {
            _tag: "FullReconcileBarrier",
            attemptId: message.attemptId,
            epoch: passEpoch,
          },
        });
        return;
      }
      case "FullReconcileBarrier":
        return;
      case "Barrier":
        return;
    }
  });

  const processEnvelope = Effect.fn("AgentControlGithubObserveReactor.processEnvelope")(function* (
    envelope: ReactorEnvelope,
  ) {
    const message = envelope.message;
    if (
      (message._tag === "FullReconcile" || message._tag === "FullReconcileBarrier") &&
      (activeRuntime === null || activeRuntime.closed || message.attemptId !== activeRuntime.id)
    ) {
      if (envelope.acknowledgement !== undefined) {
        yield* Deferred.succeed(envelope.acknowledgement, undefined).pipe(Effect.ignore);
      }
      return;
    }
    const projectId = projectIdOf(message);
    if (message._tag === "ReconcileProject") queuedReconciles.delete(message.projectId);
    if (message._tag === "FullReconcile") {
      fullReconcileQueued = false;
      fullReconcileRunning = true;
      // This is the pass scan cutoff. Requests arriving after it stay open
      // until a later barrier completes a follow-up pass.
      fullReconcileRunningEpoch = fullReconcileRequestedEpoch;
    }
    if (projectId !== null) reconcilingProjects.add(projectId);

    const exit = yield* Effect.exit(processMessage(message)).pipe(
      Effect.ensuring(
        projectId === null
          ? Effect.void
          : Effect.sync(() => {
              reconcilingProjects.delete(projectId);
            }),
      ),
    );
    if (Exit.isSuccess(exit)) {
      if (projectId !== null) {
        recoveryAttempts.delete(projectId);
        recoveryScheduled.delete(projectId);
        recoveringProjects.delete(projectId);
        yield* emitLifecycleTestEvent({ _tag: "project-reconciled", projectId });
      }
      if (
        message._tag === "FullReconcileBarrier" &&
        fullReconcileRunning &&
        fullReconcileRunningEpoch === message.epoch
      ) {
        fullReconcileCompletedEpoch = Math.max(fullReconcileCompletedEpoch, message.epoch);
        fullReconcileRunning = false;
        fullReconcileRunningEpoch = null;
        if (fullReconcileRequestedEpoch > fullReconcileCompletedEpoch) {
          yield* enqueuePendingFullReconcile();
        } else {
          globalRecoveryScheduled = false;
          globalRecoveryToken += 1;
          globalRecoveryAttempt = 0;
        }
      }
      if (envelope.acknowledgement !== undefined) {
        yield* Deferred.succeed(envelope.acknowledgement, undefined).pipe(Effect.ignore);
      }
      return;
    }
    if (Cause.hasInterruptsOnly(exit.cause)) return yield* Effect.interrupt;

    if (projectId === null) {
      if (message._tag === "FullReconcile") {
        fullReconcileRunning = false;
        fullReconcileRunningEpoch = null;
      }
      yield* scheduleGlobalRecovery();
    } else {
      if (message._tag === "TimerDue" || message._tag === "PollCoordination") {
        yield* stopWorkerUpTo(projectId, message);
      }
      yield* scheduleProjectRecovery(projectId);
    }
    yield* Effect.logWarning("Agent Control GitHub Observe recovery failed", {
      projectId,
      status: "recovering",
      reasonCode: "internal-coordination-error",
    });
    if (envelope.acknowledgement !== undefined) {
      yield* Deferred.fail(
        envelope.acknowledgement,
        startupError(message._tag === "FullReconcile" ? "enumeration-failed" : "reconcile-failed"),
      ).pipe(Effect.ignore);
    }
  });

  const launchConsumer = Effect.fn("AgentControlGithubObserveReactor.launchConsumer")(function* (
    scope: Scope.Closeable,
  ) {
    yield* Queue.take(messages).pipe(
      Effect.flatMap(processEnvelope),
      Effect.forever,
      Effect.forkIn(scope),
    );
  });

  const launchSubscriptionSupervisor = Effect.fn(
    "AgentControlGithubObserveReactor.launchSubscriptionSupervisor",
  )(function* <A>(
    scope: Scope.Closeable,
    name: SubscriptionName,
    subscribe: Effect.Effect<Stream.Stream<A>, never, Scope.Scope>,
    toMessage: (event: A) => ReactorMessage | null,
    initialActivation: Deferred.Deferred<void, AgentControlGithubObserveStartupError>,
  ) {
    let attempts = 0;
    let activated = false;

    const supervise: Effect.Effect<void> = Effect.suspend(() =>
      Effect.gen(function* () {
        const recovering = attempts > 0;
        const consumeExit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* subscribe;
              const pull = yield* Stream.toPull(stream);
              subscriptionHealth.set(name, "healthy");
              if (!activated) {
                activated = true;
                yield* Deferred.succeed(initialActivation, undefined).pipe(Effect.ignore);
              } else if (recovering) {
                yield* enqueueFullReconcile();
              }
              return yield* pull.pipe(
                Effect.flatMap((events) =>
                  Effect.forEach(
                    events,
                    (event) => {
                      const message = toMessage(event);
                      if (message === null) return Effect.void;
                      if (message._tag === "ReconcileProject") {
                        return enqueueReconcile(message.projectId);
                      }
                      return Queue.offer(messages, { message });
                    },
                    { concurrency: 1, discard: true },
                  ),
                ),
                Effect.forever,
              );
            }),
          ),
        );
        if (Exit.isFailure(consumeExit) && Cause.hasInterruptsOnly(consumeExit.cause)) {
          return yield* Effect.interrupt;
        }

        attempts += 1;
        subscriptionHealth.set(name, attempts >= 3 ? "degraded" : "recovering");
        if (!activated && attempts >= startupMaxAttempts) {
          yield* Deferred.fail(
            initialActivation,
            startupError("subscription-activation-failed"),
          ).pipe(Effect.ignore);
        }
        yield* Effect.logWarning("Agent Control reactor subscription restarting", {
          subscription: name,
          status: subscriptionHealth.get(name),
          reasonCode: "internal-coordination-error",
        });
        yield* Effect.sleep(
          Duration.millis(
            Math.min(
              subscriptionRetryMaxMs,
              subscriptionRetryBaseMs * 2 ** Math.min(20, attempts - 1),
            ),
          ),
        );
        return yield* supervise;
      }),
    );

    yield* supervise.pipe(
      Effect.onExit((exit) =>
        activated ? Effect.void : Deferred.done(initialActivation, exit).pipe(Effect.ignore),
      ),
      Effect.forkIn(scope),
    );
  });

  const initialRecoveryAttempt = Effect.fn(
    "AgentControlGithubObserveReactor.initialRecoveryAttempt",
  )(function* () {
    const entries = yield* projectStates.listPersisted.pipe(
      Effect.mapError(() => startupError("enumeration-failed")),
    );
    const acknowledgements: Array<Deferred.Deferred<void, AgentControlGithubObserveStartupError>> =
      [];
    for (const entry of entries) {
      const acknowledgement = yield* Deferred.make<void, AgentControlGithubObserveStartupError>();
      acknowledgements.push(acknowledgement);
      yield* Queue.offer(messages, {
        message:
          entry._tag === "Corrupt"
            ? { _tag: "QuarantineProject", projectId: entry.projectId }
            : { _tag: "ReconcileProject", projectId: entry.state.projectId },
        acknowledgement,
      });
    }
    const barrier = yield* Deferred.make<void, AgentControlGithubObserveStartupError>();
    yield* Queue.offer(messages, {
      message: { _tag: "Barrier" },
      acknowledgement: barrier,
    });

    const results = yield* Effect.forEach(
      acknowledgements,
      (acknowledgement) => Effect.exit(Deferred.await(acknowledgement)),
      { concurrency: "unbounded" },
    );
    yield* Deferred.await(barrier);
    if (results.some(Exit.isFailure)) {
      return yield* startupError("reconcile-failed");
    }
  });

  const runInitialRecovery = Effect.fn("AgentControlGithubObserveReactor.runInitialRecovery")(
    function* () {
      let lastError = startupError("reconcile-failed");
      for (let attempt = 1; attempt <= startupMaxAttempts; attempt += 1) {
        const result = yield* Effect.exit(
          initialRecoveryAttempt().pipe(
            Effect.catchCause((cause) =>
              Option.match(Cause.findErrorOption(cause), {
                onNone: () => Effect.fail(startupError("reconcile-failed")),
                onSome: Effect.fail,
              }),
            ),
          ),
        );
        if (Exit.isSuccess(result)) return;
        lastError = Option.getOrElse(Cause.findErrorOption(result.cause), () => lastError);
        if (attempt < startupMaxAttempts) {
          yield* Effect.sleep(Duration.millis(startupRetryBaseMs * 2 ** Math.min(20, attempt - 1)));
        }
      }
      return yield* lastError;
    },
  );

  const launchWatchdog = Effect.fn("AgentControlGithubObserveReactor.launchWatchdog")(function* (
    scope: Scope.Closeable,
  ) {
    yield* Effect.sleep(Duration.millis(watchdogIntervalMs)).pipe(
      Effect.andThen(enqueueFullReconcile()),
      Effect.forever,
      Effect.forkIn(scope),
    );
  });

  const emitLifecycleTestEvent = (event: ReactorLifecycleTestEvent) =>
    options.testHooks?.lifecycleEvent?.(event) ?? Effect.void;

  const shutdownRuntime = Effect.fn("AgentControlGithubObserveReactor.shutdownRuntime")(function* (
    attempt: RuntimeAttempt,
    completionExit: Exit.Exit<void, AgentControlGithubObserveStartupError>,
  ) {
    if (attempt.closed) return;
    attempt.closed = true;

    let cleanupCause: Cause.Cause<never> | null = null;
    const cleanup = Effect.fn("AgentControlGithubObserveReactor.shutdownRuntime.cleanup")(
      function* <A>(operation: Effect.Effect<A, never, never>) {
        const operationExit = yield* Effect.exit(operation);
        if (Exit.isFailure(operationExit)) {
          cleanupCause =
            cleanupCause === null
              ? operationExit.cause
              : Cause.combine(cleanupCause, operationExit.cause);
        }
        return operationExit;
      },
    );

    const ownsLifecycle = yield* Ref.modify(lifecycle, (current) =>
      current._tag !== "idle" && current._tag !== "closing" && current.attemptId === attempt.id
        ? [
            true,
            {
              _tag: "closing",
              attemptId: attempt.id,
              shutdownCompletion: attempt.shutdownCompletion,
            } satisfies ReactorLifecycle,
          ]
        : [false, current],
    );
    if (ownsLifecycle) {
      yield* cleanup(emitLifecycleTestEvent({ _tag: "closing", attemptId: attempt.id }));
    }
    yield* cleanup(Deferred.succeed(attempt.shutdownRequested, undefined));
    yield* cleanup(Scope.close(attempt.scope, completionExit));

    if (ownsLifecycle && activeRuntime === attempt) {
      yield* cleanup(stopAllWorkers());
      yield* cleanup(
        Effect.sync(() => {
          recoveryAttempts.clear();
          recoveryScheduled.clear();
          recoveringProjects.clear();
          reconcilingProjects.clear();
          queuedReconciles.clear();
          fullReconcileRequestedEpoch = 0;
          fullReconcileCompletedEpoch = 0;
          fullReconcileQueued = false;
          fullReconcileRunning = false;
          fullReconcileRunningEpoch = null;
          globalRecoveryAttempt = 0;
          globalRecoveryScheduled = false;
          globalRecoveryToken += 1;
          subscriptionHealth.set("project-controller", "recovering");
          subscriptionHealth.set("github-intake", "recovering");
          subscriptionHealth.set("project-delete", "recovering");
        }),
      );

      const pendingCountExit = yield* cleanup(Queue.size(messages));
      if (Exit.isSuccess(pendingCountExit) && pendingCountExit.value > 0) {
        const pendingExit = yield* cleanup(Queue.takeN(messages, pendingCountExit.value));
        if (Exit.isSuccess(pendingExit)) {
          for (const { acknowledgement } of pendingExit.value) {
            if (acknowledgement !== undefined) {
              yield* cleanup(Deferred.interrupt(acknowledgement));
            }
          }
        }
      }

      yield* cleanup(
        Effect.sync(() => {
          if (activeRuntime === attempt) activeRuntime = null;
        }),
      );
    }
    yield* cleanup(Deferred.done(attempt.completion, completionExit));
    if (ownsLifecycle) {
      yield* cleanup(
        Ref.update(lifecycle, (current) =>
          current._tag === "closing" &&
          current.attemptId === attempt.id &&
          current.shutdownCompletion === attempt.shutdownCompletion
            ? ({ _tag: "idle" } as const)
            : current,
        ),
      );
    }
    yield* cleanup(Deferred.succeed(attempt.shutdownCompletion, undefined));
    yield* cleanup(emitLifecycleTestEvent({ _tag: "shutdown-completed", attemptId: attempt.id }));
    const finalCleanupCause = cleanupCause;
    if (finalCleanupCause !== null) return yield* Effect.failCause<never>(finalCleanupCause);
  });

  const awaitSubscriptionsHealthy: Effect.Effect<void> = Effect.suspend(() =>
    [...subscriptionHealth.values()].every((health) => health === "healthy")
      ? Effect.void
      : Effect.sleep(Duration.millis(1)).pipe(Effect.andThen(awaitSubscriptionsHealthy)),
  );

  const start: AgentControlGithubObserveReactorShape["start"] = Effect.fn(
    "AgentControlGithubObserveReactor.start",
  )(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const completion = yield* Deferred.make<void, AgentControlGithubObserveStartupError>();
        const attemptId = yield* Effect.sync(() => {
          nextAttemptId += 1;
          return nextAttemptId;
        });
        const decision = yield* Ref.modify<ReactorLifecycle, StartDecision>(
          lifecycle,
          (current) => {
            switch (current._tag) {
              case "idle":
                return [
                  { _tag: "launch", attemptId, completion },
                  {
                    _tag: "starting",
                    attemptId,
                    completion,
                  } satisfies ReactorLifecycle,
                ] as const;
              case "starting":
                return [{ _tag: "wait", completion: current.completion }, current] as const;
              case "started":
                return [{ _tag: "done" }, current] as const;
              case "closing":
                return [
                  {
                    _tag: "wait-for-closing",
                    attemptId: current.attemptId,
                    shutdownCompletion: current.shutdownCompletion,
                  },
                  current,
                ] as const;
            }
          },
        );
        if (decision._tag === "done") return;
        if (decision._tag === "wait") {
          yield* restore(Deferred.await(decision.completion));
          return;
        }
        if (decision._tag === "wait-for-closing") {
          yield* emitLifecycleTestEvent({
            _tag: "waiting-for-closing",
            attemptId: decision.attemptId,
          });
          yield* restore(Deferred.await(decision.shutdownCompletion));
          return yield* start();
        }

        const runtimeScope = yield* Scope.make("sequential");
        const shutdownRequested = yield* Deferred.make<void>();
        const shutdownCompletion = yield* Deferred.make<void>();
        const attempt: RuntimeAttempt = {
          id: decision.attemptId,
          scope: runtimeScope,
          completion: decision.completion,
          shutdownRequested,
          shutdownCompletion,
          closed: false,
        };
        activeRuntime = attempt;
        yield* Effect.addFinalizer(() =>
          shutdownRuntime(attempt, Exit.failCause(Cause.interrupt())),
        );

        const startup = Effect.gen(function* () {
          if (options.testHooks?.acquireRuntimeResource !== undefined) {
            yield* options.testHooks.acquireRuntimeResource.pipe(Scope.provide(runtimeScope));
          }
          const controllerSubscribe = projectController.subscribeDomainEvents;
          const githubSubscribe = githubIntake.subscribeDomainEvents;
          const orchestrationSubscribe = orchestration.subscribeDomainEvents;
          if (
            controllerSubscribe === undefined ||
            githubSubscribe === undefined ||
            orchestrationSubscribe === undefined
          ) {
            return yield* startupError("subscription-activation-failed");
          }

          yield* launchConsumer(runtimeScope);
          const controllerActivation = yield* Deferred.make<
            void,
            AgentControlGithubObserveStartupError
          >();
          const githubActivation = yield* Deferred.make<
            void,
            AgentControlGithubObserveStartupError
          >();
          const orchestrationActivation = yield* Deferred.make<
            void,
            AgentControlGithubObserveStartupError
          >();

          yield* launchSubscriptionSupervisor(
            runtimeScope,
            "project-controller",
            controllerSubscribe,
            (event) => ({ _tag: "ReconcileProject", projectId: event.aggregateId }),
            controllerActivation,
          );
          yield* launchSubscriptionSupervisor(
            runtimeScope,
            "github-intake",
            githubSubscribe,
            (event: AgentControlGithubEvent) => ({
              _tag: "ReconcileProject",
              projectId: event.aggregateId,
            }),
            githubActivation,
          );
          yield* launchSubscriptionSupervisor(
            runtimeScope,
            "project-delete",
            orchestrationSubscribe,
            (event: OrchestrationEvent) =>
              event.type === "project.deleted"
                ? { _tag: "ProjectDeleted", projectId: event.payload.projectId }
                : null,
            orchestrationActivation,
          );

          yield* Effect.all(
            [
              Deferred.await(controllerActivation),
              Deferred.await(githubActivation),
              Deferred.await(orchestrationActivation),
            ],
            { concurrency: "unbounded", discard: true },
          );
          yield* runInitialRecovery();
          yield* awaitSubscriptionsHealthy.pipe(
            Effect.timeoutOrElse({
              duration: Duration.millis(subscriptionStartupTimeoutMs),
              orElse: () => Effect.fail(startupError("subscription-activation-failed")),
            }),
          );
        });
        const startupExit = yield* Effect.exit(
          restore(
            Effect.raceFirst(
              startup,
              Deferred.await(shutdownRequested).pipe(Effect.andThen(Effect.interrupt)),
            ),
          ),
        );
        if (Exit.isFailure(startupExit)) {
          startupPreviouslyFailed = !Cause.hasInterruptsOnly(startupExit.cause);
          const shutdownExit = yield* Effect.exit(shutdownRuntime(attempt, startupExit));
          return yield* Effect.failCause(
            Exit.isFailure(shutdownExit)
              ? Cause.combine(startupExit.cause, shutdownExit.cause)
              : startupExit.cause,
          );
        }

        startupPreviouslyFailed = false;
        yield* launchWatchdog(runtimeScope);
        const transitioned = yield* Ref.modify(lifecycle, (current) =>
          current._tag === "starting" && current.attemptId === attempt.id && !attempt.closed
            ? [
                true,
                {
                  _tag: "started",
                  attemptId: attempt.id,
                  scope: runtimeScope,
                } satisfies ReactorLifecycle,
              ]
            : [false, current],
        );
        if (!transitioned) {
          yield* restore(Deferred.await(attempt.completion));
          return;
        }
        const completed = yield* Deferred.done(attempt.completion, startupExit);
        if (!completed) yield* restore(Deferred.await(attempt.completion));
        return;
      }),
    ),
  );

  const aggregateSubscriptionHealth = (): AgentControlGithubReactorHealth => {
    const states = [...subscriptionHealth.values()];
    if (states.some((state) => state === "degraded")) return "degraded";
    if (states.some((state) => state === "recovering")) return "recovering";
    return "healthy";
  };

  const aggregateRuntimeHealth = (
    lifecycleState: ReactorLifecycle,
    subscriptions: AgentControlGithubReactorHealth,
  ): AgentControlGithubReactorHealth => {
    if (subscriptions === "degraded") return "degraded";
    if (lifecycleState._tag !== "started") {
      return lifecycleState._tag === "idle" && startupPreviouslyFailed ? "degraded" : "recovering";
    }
    if (globalRecoveryAttempt >= 3) return "degraded";
    if (
      subscriptions === "recovering" ||
      globalRecoveryScheduled ||
      fullReconcileRequestedEpoch > fullReconcileCompletedEpoch ||
      fullReconcileQueued ||
      fullReconcileRunning
    ) {
      return "recovering";
    }
    return "healthy";
  };

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
      const worker = workers.get(input.projectId);
      const subscriptions = aggregateSubscriptionHealth();
      const lifecycleState = yield* Ref.get(lifecycle);
      const runtimeHealth = aggregateRuntimeHealth(lifecycleState, subscriptions);
      const isRecovering =
        recoveringProjects.has(input.projectId) ||
        recoveryScheduled.has(input.projectId) ||
        reconcilingProjects.has(input.projectId);

      if (Option.isNone(persisted)) {
        const workerStatus =
          worker?.phase === "timer"
            ? ("scheduled" as const)
            : worker?.phase === "poll"
              ? ("polling" as const)
              : ("stopped" as const);
        const health: AgentControlGithubReactorHealth =
          worker !== undefined
            ? "degraded"
            : isRecovering && runtimeHealth === "healthy"
              ? "recovering"
              : runtimeHealth;
        return {
          projectId: input.projectId,
          activity: "inactive" as const,
          health,
          workerStatus,
          subscriptionHealth: subscriptions,
          circuitState: "closed" as const,
          consecutiveFailures: 0,
          lastAttemptAt: null,
          nextAttemptAt: null,
          reasonCode:
            health === "degraded" && worker !== undefined
              ? ("internal-coordination-error" as const)
              : null,
        };
      }

      const state = persisted.value;
      const matchingWorker =
        worker !== undefined && tokenCompare(worker, state) === 0 ? worker : undefined;
      const workerStatus =
        matchingWorker?.phase === "timer"
          ? ("scheduled" as const)
          : matchingWorker?.phase === "poll"
            ? ("polling" as const)
            : state.activity === "active"
              ? ("missing" as const)
              : ("stopped" as const);
      let health: AgentControlGithubReactorHealth = runtimeHealth;
      if (isRecovering && health !== "degraded") {
        health = "recovering";
      } else if (state.activity === "active" && matchingWorker === undefined) {
        health = isRecovering ? "recovering" : "degraded";
      }

      return {
        projectId: state.projectId,
        activity: state.activity,
        health,
        workerStatus,
        subscriptionHealth: subscriptions,
        circuitState: state.circuitState,
        consecutiveFailures: state.consecutiveFailures,
        lastAttemptAt: state.lastAttemptAt,
        nextAttemptAt: state.nextAttemptAt,
        reasonCode:
          health === "degraded" &&
          state.activity === "active" &&
          matchingWorker === undefined &&
          state.reasonCode === null
            ? "internal-coordination-error"
            : state.reasonCode,
      };
    });

  return AgentControlGithubObserveReactor.of({ start, getStatus });
});

export const layer = Layer.effect(AgentControlGithubObserveReactor, make());
