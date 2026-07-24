import {
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskRpcError,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  type AgentControlTaskReactorErrorCode,
  type AgentControlTaskReactorHealth,
  type OrchestrationEvent,
  type ProjectId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlGithubIntake } from "../../github/Services/AgentControlGithubIntake.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskProjectGate,
} from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskIntake } from "../Services/AgentControlTaskIntake.ts";
import {
  AgentControlTaskIntakeReactor,
  AgentControlTaskIntakeStartupError,
  type AgentControlTaskIntakeReactorShape,
} from "../Services/AgentControlTaskIntakeReactor.ts";

const decodeInput = Schema.decodeUnknownEffect(AgentControlTaskReconcileOnceInput);
const isTaskRpcError = Schema.is(AgentControlTaskRpcError);

const SUBSCRIPTION_NAMES = ["project-controller", "github-intake", "project-delete"] as const;
type SubscriptionName = (typeof SUBSCRIPTION_NAMES)[number];

const RETRY_LIMIT = 5;
const RETRY_BASE_MS = 1_000;
const RETRY_MAX_MS = 30_000;
const SUBSCRIPTION_RETRY_BASE_MS = 250;
const SUBSCRIPTION_RETRY_MAX_MS = 30_000;
const SUBSCRIPTION_STARTUP_ATTEMPTS = 3;
const SUBSCRIPTION_STARTUP_TIMEOUT_MS = 10_000;
const WATCHDOG_INTERVAL_MS = 60_000;

export interface AgentControlTaskIntakeReactorOptions {
  readonly retryBaseMs?: number;
  readonly retryMaxMs?: number;
  readonly retryLimit?: number;
  readonly subscriptionRetryBaseMs?: number;
  readonly subscriptionRetryMaxMs?: number;
  readonly subscriptionStartupAttempts?: number;
  readonly subscriptionStartupTimeoutMs?: number;
  readonly watchdogIntervalMs?: number;
}

interface SubscriptionGenerationState {
  readonly generation: number;
  readonly phase: "acquiring" | "running" | "terminated";
}

interface SubscriptionStartupState {
  readonly startupOpen: boolean;
  readonly generations: Partial<Record<SubscriptionName, SubscriptionGenerationState>>;
}

interface SubscriptionStartupTracker {
  readonly state: Ref.Ref<SubscriptionStartupState>;
  readonly signal: Queue.Queue<void>;
  readonly failure: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
}

interface ProjectWaiter {
  readonly epoch: number;
  readonly acknowledgement: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
}

interface ProjectRuntime {
  readonly projectId: ProjectId;
  readonly generation: number;
  requestedEpoch: number;
  completedEpoch: number;
  running: boolean;
  workerState: "stopped" | "queued" | "running" | "backoff";
  activity: "inactive" | "waiting-source" | "reconciling" | "recovering" | "suspended";
  health: AgentControlTaskReactorHealth;
  sourceFingerprint: string | null;
  suspendedFingerprint: string | null;
  suspensionReason: AgentControlTaskReactorErrorCode | null;
  retryFingerprint: string | null;
  retryToken: number;
  retryAttempt: number;
  nextAttemptAt: string | null;
  lastErrorCode: AgentControlTaskReactorErrorCode | null;
  workerFiber: Fiber.Fiber<void, never> | null;
  retryFiber: Fiber.Fiber<void, never> | null;
  waiters: Array<ProjectWaiter>;
}

interface RuntimeAttempt {
  readonly id: number;
  readonly scope: Scope.Closeable;
  readonly queue: Queue.Queue<ReactorEnvelope>;
  readonly completion: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
  readonly shutdownRequested: Deferred.Deferred<void>;
  readonly subscriptionStartupSignal: Queue.Queue<void>;
  closed: boolean;
}

type ReactorMessage =
  | { readonly _tag: "RequestProject"; readonly projectId: ProjectId }
  | { readonly _tag: "ProjectDeleted"; readonly projectId: ProjectId }
  | {
      readonly _tag: "RetryDue";
      readonly projectId: ProjectId;
      readonly generation: number;
      readonly fingerprint: string;
      readonly token: number;
    }
  | { readonly _tag: "FullReconcile"; readonly attemptId: number; readonly epoch: number }
  | {
      readonly _tag: "FullReconcileBarrier";
      readonly attemptId: number;
      readonly epoch: number;
    }
  | { readonly _tag: "Barrier" };

interface ReactorEnvelope {
  readonly message: ReactorMessage;
  readonly acknowledgement?: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
}

type ReactorLifecycle =
  | { readonly _tag: "idle" }
  | {
      readonly _tag: "starting";
      readonly attemptId: number;
      readonly completion: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
    }
  | { readonly _tag: "started"; readonly attemptId: number };

type StartDecision =
  | {
      readonly _tag: "launch";
      readonly attemptId: number;
      readonly completion: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
    }
  | {
      readonly _tag: "wait";
      readonly completion: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>;
    }
  | { readonly _tag: "done" };

const startupError = (reason: AgentControlTaskIntakeStartupError["reason"]) =>
  new AgentControlTaskIntakeStartupError({ reason });

const rpcError = (
  projectId: ProjectId,
  code: AgentControlTaskRpcError["code"],
): AgentControlTaskRpcError =>
  new AgentControlTaskRpcError({
    code,
    operation: "get-reactor-status",
    projectId,
    taskId: null,
  });

const positiveInt = (value: number | undefined, fallback: number) =>
  Math.max(1, Math.floor(value ?? fallback));

const safeCode = (code: string): AgentControlTaskReactorErrorCode => {
  switch (code) {
    case "source-snapshot-unavailable":
    case "source-snapshot-stale":
    case "project-mode-inactive":
    case "revision-conflict":
    case "task-projection-corrupt":
    case "source-identity-conflict":
    case "internal-persistence-error":
      return code;
    default:
      return "internal-persistence-error";
  }
};

export const make = Effect.fn("AgentControlTaskIntakeReactor.make")(function* (
  options: AgentControlTaskIntakeReactorOptions = {},
) {
  const projectController = yield* AgentControlEngine;
  const githubIntake = yield* AgentControlGithubIntake;
  const orchestration = yield* OrchestrationEngineService;
  const projects = yield* AgentControlProjectStateRepository;
  const availability = yield* AgentControlProjectAvailability;
  const intake = yield* AgentControlTaskIntake;
  const guard = yield* AgentControlTaskConsumerGuard;
  const lifecycle = yield* Ref.make<ReactorLifecycle>({ _tag: "idle" });

  const retryLimit = positiveInt(options.retryLimit, RETRY_LIMIT);
  const retryBaseMs = positiveInt(options.retryBaseMs, RETRY_BASE_MS);
  const retryMaxMs = positiveInt(options.retryMaxMs, RETRY_MAX_MS);
  const subscriptionRetryBaseMs = positiveInt(
    options.subscriptionRetryBaseMs,
    SUBSCRIPTION_RETRY_BASE_MS,
  );
  const subscriptionRetryMaxMs = positiveInt(
    options.subscriptionRetryMaxMs,
    SUBSCRIPTION_RETRY_MAX_MS,
  );
  const subscriptionStartupAttempts = positiveInt(
    options.subscriptionStartupAttempts,
    SUBSCRIPTION_STARTUP_ATTEMPTS,
  );
  const subscriptionStartupTimeoutMs = positiveInt(
    options.subscriptionStartupTimeoutMs,
    SUBSCRIPTION_STARTUP_TIMEOUT_MS,
  );
  const watchdogIntervalMs = positiveInt(options.watchdogIntervalMs, WATCHDOG_INTERVAL_MS);

  const runtimes = new Map<string, ProjectRuntime>();
  const subscriptionHealth = new Map<SubscriptionName, AgentControlTaskReactorHealth>(
    SUBSCRIPTION_NAMES.map((name) => [name, "recovering"]),
  );
  let activeRuntime: RuntimeAttempt | null = null;
  let nextAttemptId = 0;
  let nextGeneration = 0;
  let nextSubscriptionGeneration = 0;
  let startupPreviouslyFailed = false;
  let fullRequestedEpoch = 0;
  let fullCompletedEpoch = 0;
  let fullQueued = false;
  let fullRunning = false;
  let globalRecoveryAttempt = 0;
  let globalRetryFiber: Fiber.Fiber<void, never> | null = null;
  let globalLastError: AgentControlTaskReactorErrorCode | null = null;

  const currentRuntime = () => {
    const runtime = activeRuntime;
    return runtime === null || runtime.closed ? null : runtime;
  };

  const resolveWaiters = Effect.fn("AgentControlTaskIntakeReactor.resolveWaiters")(function* (
    state: ProjectRuntime,
  ) {
    const ready = state.waiters.filter((waiter) => waiter.epoch <= state.completedEpoch);
    state.waiters = state.waiters.filter((waiter) => waiter.epoch > state.completedEpoch);
    yield* Effect.forEach(ready, (waiter) => Deferred.succeed(waiter.acknowledgement, undefined), {
      concurrency: "unbounded",
      discard: true,
    });
  });

  const interruptFiber = (fiber: Fiber.Fiber<void, never> | null) =>
    fiber === null ? Effect.void : Fiber.interrupt(fiber).pipe(Effect.asVoid);

  const removeProject = Effect.fn("AgentControlTaskIntakeReactor.removeProject")(function* (
    projectId: ProjectId,
    expectedGeneration?: number,
    interruptWorker = true,
  ) {
    const state = runtimes.get(projectId);
    if (
      state === undefined ||
      (expectedGeneration !== undefined && state.generation !== expectedGeneration)
    ) {
      return;
    }
    runtimes.delete(projectId);
    yield* interruptFiber(state.retryFiber);
    if (interruptWorker && state.workerFiber !== null && state.running) {
      yield* interruptFiber(state.workerFiber);
    }
    yield* Effect.forEach(
      state.waiters,
      (waiter) => Deferred.succeed(waiter.acknowledgement, undefined),
      { concurrency: "unbounded", discard: true },
    );
  });

  const getOrCreateProject = (projectId: ProjectId) => {
    const existing = runtimes.get(projectId);
    if (existing !== undefined) return existing;
    nextGeneration += 1;
    const created: ProjectRuntime = {
      projectId,
      generation: nextGeneration,
      requestedEpoch: 0,
      completedEpoch: 0,
      running: false,
      workerState: "stopped",
      activity: "inactive",
      health: "healthy",
      sourceFingerprint: null,
      suspendedFingerprint: null,
      suspensionReason: null,
      retryFingerprint: null,
      retryToken: 0,
      retryAttempt: 0,
      nextAttemptAt: null,
      lastErrorCode: null,
      workerFiber: null,
      retryFiber: null,
      waiters: [],
    };
    runtimes.set(projectId, created);
    return created;
  };

  const enqueue = (
    message: ReactorMessage,
    acknowledgement?: ReactorEnvelope["acknowledgement"],
  ) => {
    const runtime = currentRuntime();
    return runtime === null
      ? Effect.void
      : Queue.offer(
          runtime.queue,
          acknowledgement === undefined ? { message } : { message, acknowledgement },
        ).pipe(Effect.asVoid);
  };

  const enqueueFullReconcile = Effect.fn("AgentControlTaskIntakeReactor.enqueueFullReconcile")(
    function* () {
      const runtime = currentRuntime();
      if (runtime === null) return;
      fullRequestedEpoch += 1;
      if (fullQueued || fullRunning) return;
      fullQueued = true;
      yield* enqueue({
        _tag: "FullReconcile",
        attemptId: runtime.id,
        epoch: fullRequestedEpoch,
      });
    },
  );

  const scheduleGlobalRetry = Effect.fn("AgentControlTaskIntakeReactor.scheduleGlobalRetry")(
    function* () {
      const runtime = currentRuntime();
      if (runtime === null || globalRetryFiber !== null) return;
      globalRecoveryAttempt += 1;
      const delay = Math.min(
        retryMaxMs,
        retryBaseMs * 2 ** Math.min(20, globalRecoveryAttempt - 1),
      );
      globalRetryFiber = yield* Effect.sleep(Duration.millis(delay)).pipe(
        Effect.andThen(
          Effect.sync(() => {
            globalRetryFiber = null;
          }),
        ),
        Effect.andThen(enqueueFullReconcile()),
        Effect.forkIn(runtime.scope),
      );
    },
  );

  const scheduleRetry = Effect.fn("AgentControlTaskIntakeReactor.scheduleRetry")(function* (
    state: ProjectRuntime,
    fingerprintValue: string,
    errorCode: AgentControlTaskReactorErrorCode,
  ): Effect.fn.Return<"scheduled" | "suspended"> {
    const runtime = currentRuntime();
    if (runtime === null || runtimes.get(state.projectId) !== state) return "suspended";
    if (state.retryFingerprint !== fingerprintValue) {
      state.retryFingerprint = fingerprintValue;
      state.retryAttempt = 0;
    }
    if (state.retryAttempt >= retryLimit) {
      state.activity = "suspended";
      state.health = "degraded";
      state.workerState = "stopped";
      state.suspendedFingerprint = fingerprintValue;
      state.suspensionReason = errorCode;
      state.lastErrorCode = errorCode;
      state.nextAttemptAt = null;
      return "suspended";
    }

    state.retryAttempt += 1;
    const delay = Math.min(retryMaxMs, retryBaseMs * 2 ** Math.min(20, state.retryAttempt - 1));
    const dueAt = (yield* Clock.currentTimeMillis) + delay;
    state.activity = "recovering";
    state.health = "recovering";
    state.workerState = "backoff";
    state.lastErrorCode = errorCode;
    state.nextAttemptAt = DateTime.formatIso(DateTime.makeUnsafe(dueAt));
    yield* interruptFiber(state.retryFiber);
    state.retryToken += 1;
    const token = state.retryToken;
    state.retryFiber = yield* Effect.sleep(Duration.millis(delay)).pipe(
      Effect.andThen(
        enqueue({
          _tag: "RetryDue",
          projectId: state.projectId,
          generation: state.generation,
          fingerprint: fingerprintValue,
          token,
        }),
      ),
      Effect.forkIn(runtime.scope),
    );
    return "scheduled";
  });

  const resetForFingerprint = (state: ProjectRuntime, gate: AgentControlTaskProjectGate) => {
    if (gate.sourceFingerprint === null || gate.sourceFingerprint === state.sourceFingerprint)
      return;
    state.sourceFingerprint = gate.sourceFingerprint;
    state.retryFingerprint = gate.sourceFingerprint;
    state.retryAttempt = 0;
    state.nextAttemptAt = null;
    if (
      state.suspensionReason !== "task-projection-corrupt" &&
      state.suspendedFingerprint !== gate.sourceFingerprint
    ) {
      state.suspendedFingerprint = null;
      state.suspensionReason = null;
      state.lastErrorCode = null;
    }
  };

  const finishPass = Effect.fn("AgentControlTaskIntakeReactor.finishPass")(function* (
    state: ProjectRuntime,
    epoch: number,
  ) {
    if (runtimes.get(state.projectId) !== state) return;
    state.completedEpoch = Math.max(state.completedEpoch, epoch);
    yield* resolveWaiters(state);
  });

  const runProjectWorker = Effect.fn("AgentControlTaskIntakeReactor.runProjectWorker")(function* (
    state: ProjectRuntime,
  ) {
    while (runtimes.get(state.projectId) === state && state.completedEpoch < state.requestedEpoch) {
      const passEpoch = state.requestedEpoch;
      state.workerState = "running";
      state.activity = "reconciling";
      state.health = "recovering";

      const inspected = yield* Effect.result(guard.inspectProject(state.projectId));
      if (inspected._tag === "Failure") {
        const fingerprintValue = state.sourceFingerprint ?? "persistence-unavailable";
        const retry = yield* scheduleRetry(state, fingerprintValue, "internal-persistence-error");
        if (retry === "suspended") yield* finishPass(state, passEpoch);
        return;
      }
      const gate = inspected.success;
      resetForFingerprint(state, gate);

      if (gate.activation === "inactive") {
        yield* finishPass(state, passEpoch);
        yield* removeProject(state.projectId, state.generation, false);
        return;
      }
      if (gate.activation === "waiting-source") {
        state.activity = "waiting-source";
        state.health = "healthy";
        state.workerState = "stopped";
        state.lastErrorCode = "source-snapshot-unavailable";
        state.nextAttemptAt = null;
        yield* finishPass(state, passEpoch);
        return;
      }
      const fingerprintValue = gate.sourceFingerprint ?? "source-unavailable";
      if (
        state.suspensionReason === "task-projection-corrupt" &&
        gate.reason === "task-projection-corrupt"
      ) {
        state.activity = "suspended";
        state.health = "degraded";
        state.workerState = "stopped";
        yield* finishPass(state, passEpoch);
        return;
      }
      if (
        state.suspensionReason === "task-projection-corrupt" &&
        gate.reason !== "task-projection-corrupt"
      ) {
        state.suspendedFingerprint = null;
        state.suspensionReason = null;
        state.lastErrorCode = null;
        state.retryAttempt = 0;
      }
      if (
        state.suspensionReason !== null &&
        state.suspendedFingerprint === fingerprintValue &&
        !gate.sequenceCurrent
      ) {
        state.activity = "suspended";
        state.health = "degraded";
        state.workerState = "stopped";
        yield* finishPass(state, passEpoch);
        return;
      }
      if (gate.sequenceCurrent) {
        state.activity = "inactive";
        state.health = "healthy";
        state.workerState = "stopped";
        state.suspendedFingerprint = null;
        state.suspensionReason = null;
        state.retryAttempt = 0;
        state.nextAttemptAt = null;
        state.lastErrorCode = null;
        yield* finishPass(state, passEpoch);
        continue;
      }

      const reconciled = yield* Effect.result(
        intake.reconcileObservedProject({ projectId: state.projectId }),
      );
      if (runtimes.get(state.projectId) !== state) return;
      if (reconciled._tag === "Failure") {
        const code = isTaskRpcError(reconciled.failure)
          ? safeCode(reconciled.failure.code)
          : "internal-persistence-error";
        if (
          isTaskRpcError(reconciled.failure) &&
          (reconciled.failure.code === "project-missing" ||
            reconciled.failure.code === "project-deleted")
        ) {
          yield* finishPass(state, passEpoch);
          yield* removeProject(state.projectId, state.generation, false);
          return;
        }
        if (code === "source-snapshot-unavailable") {
          state.activity = "waiting-source";
          state.health = "healthy";
          state.workerState = "stopped";
          state.lastErrorCode = code;
          state.nextAttemptAt = null;
        } else if (code === "task-projection-corrupt" || code === "source-identity-conflict") {
          state.activity = "suspended";
          state.health = "degraded";
          state.workerState = "stopped";
          state.suspendedFingerprint = fingerprintValue;
          state.suspensionReason = code;
          state.lastErrorCode = code;
          state.nextAttemptAt = null;
        } else {
          const retry = yield* scheduleRetry(state, fingerprintValue, code);
          if (retry === "scheduled") return;
        }
        yield* finishPass(state, passEpoch);
        return;
      }

      const post = yield* Effect.result(guard.inspectProject(state.projectId));
      if (post._tag === "Failure") {
        const retry = yield* scheduleRetry(state, fingerprintValue, "internal-persistence-error");
        if (retry === "suspended") yield* finishPass(state, passEpoch);
        return;
      }
      resetForFingerprint(state, post.success);
      if (post.success.activation === "inactive") {
        yield* finishPass(state, passEpoch);
        yield* removeProject(state.projectId, state.generation, false);
        return;
      }
      if (post.success.activation === "waiting-source") {
        state.activity = "waiting-source";
        state.health = "healthy";
        state.workerState = "stopped";
        state.lastErrorCode = "source-snapshot-unavailable";
        yield* finishPass(state, passEpoch);
        return;
      }
      if (!post.success.sequenceCurrent) {
        yield* finishPass(state, passEpoch);
        yield* enqueue({ _tag: "RequestProject", projectId: state.projectId });
        return;
      }

      state.activity = "inactive";
      state.health = "healthy";
      state.workerState = "stopped";
      state.retryAttempt = 0;
      state.nextAttemptAt = null;
      state.lastErrorCode = null;
      state.suspendedFingerprint = null;
      state.suspensionReason = null;
      yield* finishPass(state, passEpoch);
    }
  });

  const launchProjectWorker = Effect.fn("AgentControlTaskIntakeReactor.launchProjectWorker")(
    function* (state: ProjectRuntime) {
      const runtime = currentRuntime();
      if (runtime === null || state.running || runtimes.get(state.projectId) !== state) return;
      state.running = true;
      state.workerState = "queued";
      state.workerFiber = yield* runProjectWorker(state).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.gen(function* () {
                const retry = yield* scheduleRetry(
                  state,
                  state.sourceFingerprint ?? "worker-defect",
                  "internal-persistence-error",
                );
                if (retry === "suspended") {
                  yield* finishPass(state, state.requestedEpoch);
                }
              }),
        ),
        Effect.ensuring(
          Effect.gen(function* () {
            let relaunch = false;
            if (runtimes.get(state.projectId) === state) {
              state.running = false;
              state.workerFiber = null;
              if (state.workerState === "running") state.workerState = "stopped";
              relaunch =
                state.completedEpoch < state.requestedEpoch &&
                state.workerState !== "backoff" &&
                state.activity !== "suspended";
            }
            if (relaunch) {
              yield* enqueue({ _tag: "RequestProject", projectId: state.projectId });
            }
          }),
        ),
        Effect.forkIn(runtime.scope),
      );
    },
  );

  const requestProject = Effect.fn("AgentControlTaskIntakeReactor.requestProject")(function* (
    projectId: ProjectId,
    acknowledgement?: Deferred.Deferred<void, AgentControlTaskIntakeStartupError>,
  ) {
    const state = getOrCreateProject(projectId);
    state.requestedEpoch += 1;
    if (acknowledgement !== undefined) {
      state.waiters.push({ epoch: state.requestedEpoch, acknowledgement });
    }
    if (state.activity === "suspended") {
      const inspected = yield* Effect.exit(guard.inspectProject(projectId));
      if (Exit.isFailure(inspected) || runtimes.get(projectId) !== state) {
        yield* finishPass(state, state.requestedEpoch);
        return;
      }
      const gate = inspected.value;
      if (gate.activation === "inactive") {
        yield* removeProject(projectId, state.generation);
        return;
      }
      if (gate.activation === "waiting-source") {
        state.activity = "waiting-source";
        state.health = "healthy";
        state.workerState = "stopped";
        state.lastErrorCode = "source-snapshot-unavailable";
        yield* finishPass(state, state.requestedEpoch);
        return;
      }
      resetForFingerprint(state, gate);
      if (
        state.suspensionReason === "task-projection-corrupt" &&
        gate.reason === "task-projection-corrupt"
      ) {
        yield* finishPass(state, state.requestedEpoch);
        return;
      }
      if (
        state.suspensionReason === "task-projection-corrupt" &&
        gate.reason !== "task-projection-corrupt"
      ) {
        state.suspendedFingerprint = null;
        state.suspensionReason = null;
        state.lastErrorCode = null;
        state.retryAttempt = 0;
      }
      const nextFingerprint = gate.sourceFingerprint ?? "source-unavailable";
      if (state.suspensionReason !== null && state.suspendedFingerprint === nextFingerprint) {
        yield* finishPass(state, state.requestedEpoch);
        return;
      }
      yield* launchProjectWorker(state);
      return;
    }
    if (state.workerState === "backoff") {
      const inspected = yield* Effect.result(guard.inspectProject(projectId));
      if (inspected._tag === "Failure" || runtimes.get(projectId) !== state) return;
      const gate = inspected.success;
      if (gate.activation === "inactive") {
        state.retryToken += 1;
        yield* interruptFiber(state.retryFiber);
        yield* removeProject(projectId, state.generation);
        return;
      }
      if (gate.activation === "waiting-source") {
        state.retryToken += 1;
        yield* interruptFiber(state.retryFiber);
        state.retryFiber = null;
        state.workerState = "stopped";
        state.activity = "waiting-source";
        state.health = "healthy";
        state.retryAttempt = 0;
        state.nextAttemptAt = null;
        state.lastErrorCode = "source-snapshot-unavailable";
        yield* finishPass(state, state.requestedEpoch);
        return;
      }
      const nextFingerprint = gate.sourceFingerprint ?? "source-unavailable";
      if (nextFingerprint === state.retryFingerprint) return;
      state.retryToken += 1;
      yield* interruptFiber(state.retryFiber);
      state.retryFiber = null;
      state.workerState = "stopped";
      resetForFingerprint(state, gate);
      yield* launchProjectWorker(state);
      return;
    }
    if (state.running) {
      const runtime = currentRuntime();
      if (runtime !== null) {
        yield* guard.inspectProject(projectId).pipe(
          Effect.flatMap((current) =>
            current.activation === "observe" || runtimes.get(projectId) !== state
              ? Effect.void
              : removeProject(projectId, state.generation),
          ),
          Effect.catch(() => Effect.void),
          Effect.forkIn(runtime.scope),
        );
      }
      return;
    }
    yield* launchProjectWorker(state);
  });

  const enumerateProjects = Effect.fn("AgentControlTaskIntakeReactor.enumerateProjects")(function* (
    withAcknowledgements: boolean,
  ) {
    const entries = yield* projects.listPersisted;
    const acknowledgements: Array<Deferred.Deferred<void, AgentControlTaskIntakeStartupError>> = [];
    for (const entry of entries) {
      if (entry._tag === "Corrupt") {
        if (entry.projectId !== null) {
          const state = getOrCreateProject(entry.projectId);
          state.activity = "suspended";
          state.health = "degraded";
          state.workerState = "stopped";
          state.lastErrorCode = "internal-persistence-error";
        }
        continue;
      }
      const acknowledgement = withAcknowledgements
        ? yield* Deferred.make<void, AgentControlTaskIntakeStartupError>()
        : undefined;
      if (acknowledgement !== undefined) acknowledgements.push(acknowledgement);
      yield* enqueue({ _tag: "RequestProject", projectId: entry.state.projectId }, acknowledgement);
    }
    return acknowledgements;
  });

  const processMessage = Effect.fn("AgentControlTaskIntakeReactor.processMessage")(function* (
    envelope: ReactorEnvelope,
  ) {
    const message = envelope.message;
    switch (message._tag) {
      case "RequestProject":
        yield* requestProject(message.projectId, envelope.acknowledgement);
        return;
      case "ProjectDeleted":
        yield* removeProject(message.projectId);
        if (envelope.acknowledgement !== undefined) {
          yield* Deferred.succeed(envelope.acknowledgement, undefined).pipe(Effect.ignore);
        }
        return;
      case "RetryDue": {
        const state = runtimes.get(message.projectId);
        if (
          state !== undefined &&
          state.generation === message.generation &&
          state.retryFingerprint === message.fingerprint &&
          state.retryToken === message.token &&
          state.suspendedFingerprint !== message.fingerprint
        ) {
          state.retryFiber = null;
          state.nextAttemptAt = null;
          state.workerState = "stopped";
          yield* launchProjectWorker(state);
        }
        return;
      }
      case "FullReconcile": {
        const runtime = currentRuntime();
        if (runtime === null || runtime.id !== message.attemptId) return;
        fullQueued = false;
        fullRunning = true;
        const result = yield* Effect.result(enumerateProjects(true));
        if (result._tag === "Failure") {
          fullRunning = false;
          globalLastError = "enumeration-failed";
          yield* scheduleGlobalRetry();
          return;
        }
        yield* Effect.forEach(result.success, Deferred.await, {
          concurrency: "unbounded",
          discard: true,
        }).pipe(
          Effect.andThen(
            enqueue({
              _tag: "FullReconcileBarrier",
              attemptId: message.attemptId,
              epoch: message.epoch,
            }),
          ),
          Effect.catch(() => Effect.void),
          Effect.forkIn(runtime.scope),
        );
        return;
      }
      case "FullReconcileBarrier": {
        const runtime = currentRuntime();
        if (runtime === null || runtime.id !== message.attemptId) return;
        fullRunning = false;
        fullCompletedEpoch = Math.max(fullCompletedEpoch, message.epoch);
        if (fullRequestedEpoch > fullCompletedEpoch) {
          fullQueued = true;
          yield* enqueue({
            _tag: "FullReconcile",
            attemptId: runtime.id,
            epoch: fullRequestedEpoch,
          });
        } else {
          globalRecoveryAttempt = 0;
          globalLastError = null;
        }
        return;
      }
      case "Barrier":
        if (envelope.acknowledgement !== undefined) {
          yield* Deferred.succeed(envelope.acknowledgement, undefined).pipe(Effect.ignore);
        }
        return;
    }
  });

  const launchConsumer = Effect.fn("AgentControlTaskIntakeReactor.launchConsumer")(function* (
    runtime: RuntimeAttempt,
  ) {
    yield* Queue.take(runtime.queue).pipe(
      Effect.flatMap(processMessage),
      Effect.forever,
      Effect.forkIn(runtime.scope),
    );
  });

  const updateSubscriptionGeneration = Effect.fn(
    "AgentControlTaskIntakeReactor.updateSubscriptionGeneration",
  )(function* (
    tracker: SubscriptionStartupTracker,
    name: SubscriptionName,
    generation: number,
    phase: SubscriptionGenerationState["phase"],
  ) {
    const updated = yield* Ref.modify(tracker.state, (current) => {
      const existing = current.generations[name];
      if (existing !== undefined && existing.generation > generation) return [false, current];
      return [
        true,
        {
          ...current,
          generations: {
            ...current.generations,
            [name]: { generation, phase },
          },
        },
      ];
    });
    if (updated) yield* Queue.offer(tracker.signal, undefined).pipe(Effect.asVoid);
  });

  const runningSubscriptionGenerations = (
    state: SubscriptionStartupState,
  ): Readonly<Record<SubscriptionName, number>> | null => {
    const result = {} as Record<SubscriptionName, number>;
    for (const name of SUBSCRIPTION_NAMES) {
      const generation = state.generations[name];
      if (generation === undefined || generation.phase !== "running") return null;
      result[name] = generation.generation;
    }
    return result;
  };

  const awaitSubscriptionsRunning = Effect.fn(
    "AgentControlTaskIntakeReactor.awaitSubscriptionsRunning",
  )(function* (tracker: SubscriptionStartupTracker) {
    while (true) {
      const running = runningSubscriptionGenerations(yield* Ref.get(tracker.state));
      if (running !== null) return running;
      yield* Effect.raceFirst(Queue.take(tracker.signal), Deferred.await(tracker.failure));
    }
  });

  const openSubscriptionStartup = Effect.fn(
    "AgentControlTaskIntakeReactor.openSubscriptionStartup",
  )(function* (tracker: SubscriptionStartupTracker) {
    while (true) {
      const opened = yield* Ref.modify(tracker.state, (current) => {
        const running = runningSubscriptionGenerations(current);
        if (running === null) return [false, current];
        return [true, { ...current, startupOpen: true }];
      });
      if (opened) return;
      yield* Effect.raceFirst(Queue.take(tracker.signal), Deferred.await(tracker.failure));
    }
  });

  const launchSubscriptionSupervisor = Effect.fn(
    "AgentControlTaskIntakeReactor.launchSubscriptionSupervisor",
  )(function* <A>(
    runtime: RuntimeAttempt,
    name: SubscriptionName,
    subscribe: Effect.Effect<Stream.Stream<A>, never, Scope.Scope>,
    toMessage: (event: A) => ReactorMessage | null,
    tracker: SubscriptionStartupTracker,
  ) {
    let attempt = 0;
    let hasRun = false;
    const publishEvent = (event: A): Effect.Effect<void> => {
      const message = toMessage(event);
      return message === null ? Effect.void : enqueue(message);
    };
    const supervise: Effect.Effect<void> = Effect.suspend(() =>
      Effect.gen(function* () {
        subscriptionHealth.set(name, "recovering");
        nextSubscriptionGeneration += 1;
        const generation = nextSubscriptionGeneration;
        yield* updateSubscriptionGeneration(tracker, name, generation, "acquiring");
        const exit = yield* Effect.exit(
          Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* subscribe.pipe(
                Effect.timeoutOrElse({
                  duration: Duration.millis(subscriptionStartupTimeoutMs),
                  orElse: () => Effect.fail(startupError("subscription-activation-failed")),
                }),
              );
              const consumer = yield* Stream.runForEach(stream, publishEvent).pipe(
                Effect.forkScoped({ startImmediately: true }),
              );
              const initialExit = consumer.pollUnsafe();
              if (initialExit !== undefined) {
                if (Exit.isFailure(initialExit)) return yield* Effect.failCause(initialExit.cause);
                return;
              }
              yield* updateSubscriptionGeneration(tracker, name, generation, "running");
              subscriptionHealth.set(name, "healthy");
              const startupOpen = (yield* Ref.get(tracker.state)).startupOpen;
              if (startupOpen) attempt = 0;
              if (hasRun && startupOpen) yield* enqueueFullReconcile();
              hasRun = true;
              return yield* Fiber.join(consumer);
            }),
          ),
        );
        if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)) {
          return yield* Effect.interrupt;
        }
        yield* updateSubscriptionGeneration(tracker, name, generation, "terminated");
        attempt += 1;
        subscriptionHealth.set(name, attempt >= 3 ? "degraded" : "recovering");
        const startupOpen = (yield* Ref.get(tracker.state)).startupOpen;
        if (!startupOpen && attempt >= subscriptionStartupAttempts) {
          yield* Deferred.fail(
            tracker.failure,
            startupError("subscription-activation-failed"),
          ).pipe(Effect.ignore);
          return;
        }
        yield* Effect.sleep(
          Duration.millis(
            Math.min(
              subscriptionRetryMaxMs,
              subscriptionRetryBaseMs * 2 ** Math.min(20, attempt - 1),
            ),
          ),
        );
        return yield* supervise;
      }),
    );
    yield* supervise.pipe(Effect.forkIn(runtime.scope));
  });

  const launchWatchdog = Effect.fn("AgentControlTaskIntakeReactor.launchWatchdog")(function* (
    runtime: RuntimeAttempt,
  ) {
    yield* Effect.sleep(Duration.millis(watchdogIntervalMs)).pipe(
      Effect.andThen(enqueueFullReconcile()),
      Effect.forever,
      Effect.forkIn(runtime.scope),
    );
  });

  const shutdownRuntime = Effect.fn("AgentControlTaskIntakeReactor.shutdownRuntime")(function* (
    runtime: RuntimeAttempt,
    completion: Exit.Exit<void, AgentControlTaskIntakeStartupError>,
  ) {
    if (runtime.closed) return;
    runtime.closed = true;
    yield* Deferred.succeed(runtime.shutdownRequested, undefined).pipe(Effect.ignore);
    if (activeRuntime === runtime) activeRuntime = null;
    yield* Scope.close(runtime.scope, completion).pipe(Effect.ignore);
    yield* Queue.shutdown(runtime.queue).pipe(Effect.ignore);
    yield* Queue.shutdown(runtime.subscriptionStartupSignal).pipe(Effect.ignore);
    for (const state of runtimes.values()) {
      yield* Effect.forEach(
        state.waiters,
        (waiter) => Deferred.done(waiter.acknowledgement, completion),
        { concurrency: "unbounded", discard: true },
      );
    }
    runtimes.clear();
    globalRetryFiber = null;
    fullRequestedEpoch = 0;
    fullCompletedEpoch = 0;
    fullQueued = false;
    fullRunning = false;
    for (const name of SUBSCRIPTION_NAMES) subscriptionHealth.set(name, "recovering");
    yield* Ref.set(lifecycle, { _tag: "idle" });
    yield* Deferred.done(runtime.completion, completion).pipe(Effect.ignore);
  });

  const start: AgentControlTaskIntakeReactorShape["start"] = Effect.fn(
    "AgentControlTaskIntakeReactor.start",
  )(() =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const completion = yield* Deferred.make<void, AgentControlTaskIntakeStartupError>();
        nextAttemptId += 1;
        const attemptId = nextAttemptId;
        const decision = yield* Ref.modify<ReactorLifecycle, StartDecision>(
          lifecycle,
          (current) => {
            if (current._tag === "idle") {
              return [
                { _tag: "launch", attemptId, completion },
                { _tag: "starting", attemptId, completion },
              ];
            }
            if (current._tag === "starting") {
              return [{ _tag: "wait", completion: current.completion }, current];
            }
            return [{ _tag: "done" }, current];
          },
        );
        if (decision._tag === "done") return;
        if (decision._tag === "wait") {
          yield* restore(Deferred.await(decision.completion));
          return;
        }

        const runtimeScope = yield* Scope.make("sequential");
        const queue = yield* Queue.unbounded<ReactorEnvelope>();
        const shutdownRequested = yield* Deferred.make<void>();
        const subscriptionStartupSignal = yield* Queue.unbounded<void>();
        const subscriptionStartup: SubscriptionStartupTracker = {
          state: yield* Ref.make<SubscriptionStartupState>({
            startupOpen: false,
            generations: {},
          }),
          signal: subscriptionStartupSignal,
          failure: yield* Deferred.make<void, AgentControlTaskIntakeStartupError>(),
        };
        const runtime: RuntimeAttempt = {
          id: attemptId,
          scope: runtimeScope,
          queue,
          completion,
          shutdownRequested,
          subscriptionStartupSignal,
          closed: false,
        };
        activeRuntime = runtime;
        yield* Effect.addFinalizer(() =>
          shutdownRuntime(runtime, Exit.failCause(Cause.interrupt())),
        );

        const startup = Effect.gen(function* () {
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

          yield* launchConsumer(runtime);
          yield* launchSubscriptionSupervisor(
            runtime,
            "project-controller",
            controllerSubscribe,
            (event: AgentControlEvent) => ({
              _tag: "RequestProject",
              projectId: event.aggregateId,
            }),
            subscriptionStartup,
          );
          yield* launchSubscriptionSupervisor(
            runtime,
            "github-intake",
            githubSubscribe,
            (event: AgentControlGithubEvent) => {
              if (
                event.type === "agentControl.github.config.set" ||
                event.type === "agentControl.github.config.cleared" ||
                event.type === "agentControl.github.poll.succeeded" ||
                (event.type === "agentControl.github.poll.failed" && event.payload.invalidateCursor)
              ) {
                return { _tag: "RequestProject", projectId: event.aggregateId };
              }
              return null;
            },
            subscriptionStartup,
          );
          yield* launchSubscriptionSupervisor(
            runtime,
            "project-delete",
            orchestrationSubscribe,
            (event: OrchestrationEvent) =>
              event.type === "project.deleted"
                ? { _tag: "ProjectDeleted", projectId: event.payload.projectId }
                : null,
            subscriptionStartup,
          );
          yield* awaitSubscriptionsRunning(subscriptionStartup);
          const enumeration = yield* Effect.result(enumerateProjects(true));
          if (enumeration._tag === "Failure") {
            return yield* startupError("enumeration-failed");
          }
          const barrier = yield* Deferred.make<void, AgentControlTaskIntakeStartupError>();
          yield* enqueue({ _tag: "Barrier" }, barrier);
          yield* Deferred.await(barrier);
          yield* Effect.forEach(enumeration.success, Deferred.await, {
            concurrency: "unbounded",
            discard: true,
          }).pipe(Effect.mapError(() => startupError("queue-barrier-failed")));
          yield* openSubscriptionStartup(subscriptionStartup);
        });

        const startupExit = yield* Effect.exit(
          restore(
            Effect.raceFirst(
              Effect.raceFirst(startup, Deferred.await(subscriptionStartup.failure)),
              Deferred.await(shutdownRequested).pipe(Effect.andThen(Effect.interrupt)),
            ),
          ),
        );
        if (Exit.isFailure(startupExit)) {
          startupPreviouslyFailed = !Cause.hasInterruptsOnly(startupExit.cause);
          yield* shutdownRuntime(runtime, startupExit);
          return yield* Effect.failCause(startupExit.cause);
        }

        startupPreviouslyFailed = false;
        yield* launchWatchdog(runtime);
        yield* Ref.set(lifecycle, { _tag: "started", attemptId });
        yield* Deferred.succeed(completion, undefined).pipe(Effect.ignore);
      }),
    ),
  );

  const aggregateSubscriptionHealth = (): AgentControlTaskReactorHealth => {
    const health = [...subscriptionHealth.values()];
    if (health.some((value) => value === "degraded")) return "degraded";
    if (health.some((value) => value === "recovering")) return "recovering";
    return "healthy";
  };

  const aggregateGlobalHealth = (
    lifecycleState: ReactorLifecycle,
    subscriptions: AgentControlTaskReactorHealth,
  ): AgentControlTaskReactorHealth => {
    const projectStates = [...runtimes.values()];
    if (
      subscriptions === "degraded" ||
      (globalLastError !== null && globalRecoveryAttempt >= 3) ||
      projectStates.some(
        (state) =>
          state.health === "degraded" ||
          state.activity === "suspended" ||
          state.suspensionReason !== null,
      )
    ) {
      return "degraded";
    }
    if (
      subscriptions === "recovering" ||
      lifecycleState._tag !== "started" ||
      globalRetryFiber !== null ||
      fullQueued ||
      fullRunning ||
      fullRequestedEpoch > fullCompletedEpoch ||
      projectStates.some(
        (state) =>
          state.requestedEpoch > state.completedEpoch ||
          state.waiters.length > 0 ||
          state.running ||
          state.workerState === "queued" ||
          state.workerState === "running" ||
          state.workerState === "backoff" ||
          state.health === "recovering" ||
          state.activity === "reconciling" ||
          state.activity === "recovering",
      )
    ) {
      return lifecycleState._tag === "idle" && startupPreviouslyFailed ? "degraded" : "recovering";
    }
    return "healthy";
  };

  const getStatus: AgentControlTaskIntakeReactorShape["getStatus"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeInput(rawInput).pipe(
        Effect.mapError(() => rpcError(rawInput.projectId, "validation")),
      );
      yield* availability
        .ensureAvailable(input.projectId)
        .pipe(
          Effect.mapError((error) =>
            rpcError(
              input.projectId,
              error._tag === "AgentControlProjectUnavailableError"
                ? error.reason === "missing"
                  ? "project-missing"
                  : "project-deleted"
                : "internal-persistence-error",
            ),
          ),
        );
      const state = runtimes.get(input.projectId);
      const inspected = yield* Effect.exit(guard.inspectProject(input.projectId));
      if (Exit.isFailure(inspected) && state === undefined) {
        return yield* rpcError(input.projectId, "internal-persistence-error");
      }
      const gate = Exit.isSuccess(inspected)
        ? inspected.value
        : ({
            projectId: input.projectId,
            activation: "inactive",
            currentSourceSequence: null,
            targetSequence: null,
            lastCompletedSequence: null,
            watermarkCompleted: false,
            sequenceCurrent: false,
            sourceFingerprint: state?.sourceFingerprint ?? null,
            reason: "internal-persistence-error",
          } satisfies AgentControlTaskProjectGate);
      const lifecycleState = yield* Ref.get(lifecycle);
      const subscriptions = aggregateSubscriptionHealth();
      const globalHealth = aggregateGlobalHealth(lifecycleState, subscriptions);
      return {
        projectId: input.projectId,
        activity:
          state?.activity ?? (gate.activation === "waiting-source" ? "waiting-source" : "inactive"),
        health: state?.health ?? (globalHealth === "degraded" ? "degraded" : "healthy"),
        workerState: state?.workerState ?? "stopped",
        subscriptionHealth: subscriptions,
        globalHealth,
        currentSourceSequence: gate.currentSourceSequence,
        targetSequence: gate.targetSequence,
        lastCompletedSequence: gate.lastCompletedSequence,
        sequenceCurrent: gate.sequenceCurrent,
        retryAttempt: state?.retryAttempt ?? 0,
        nextAttemptAt: state?.nextAttemptAt ?? null,
        lastErrorCode:
          state?.lastErrorCode ??
          (gate.reason === "source-snapshot-unavailable"
            ? "source-snapshot-unavailable"
            : globalLastError),
      };
    });

  return AgentControlTaskIntakeReactor.of({ start, getStatus });
});

export const layer = Layer.effect(AgentControlTaskIntakeReactor, make());
