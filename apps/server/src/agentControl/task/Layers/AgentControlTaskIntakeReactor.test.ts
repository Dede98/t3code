import {
  AgentControlTaskRpcError,
  CommandId,
  EventId,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  type OrchestrationEvent,
  ProjectId,
  type ProjectId as ProjectIdType,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlPersistenceSqlError, type AgentControlRepositoryError } from "../../Errors.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlGithubIntake } from "../../github/Services/AgentControlGithubIntake.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskProjectGate,
} from "../Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlTaskIntake } from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskIntakeReactor } from "../Services/AgentControlTaskIntakeReactor.ts";
import {
  make,
  type AgentControlTaskIntakeReactorOptions,
} from "./AgentControlTaskIntakeReactor.ts";

const at = "2026-07-23T00:00:00.000Z";
const repository = {
  repositoryNodeId: "repository-node",
  nameWithOwner: "owner/repository",
} as const;

const gate = (
  projectId: ProjectIdType,
  input?: {
    readonly activation?: AgentControlTaskProjectGate["activation"];
    readonly sequence?: number | null;
    readonly current?: boolean;
    readonly fingerprint?: string | null;
  },
): AgentControlTaskProjectGate => ({
  projectId,
  activation: input?.activation ?? "observe",
  currentSourceSequence: input?.sequence === undefined ? 1 : input.sequence,
  targetSequence: input?.current ? (input.sequence ?? 1) : null,
  lastCompletedSequence: input?.current ? (input.sequence ?? 1) : null,
  sequenceCurrent: input?.current ?? false,
  sourceFingerprint:
    input?.fingerprint === undefined ? `fingerprint-${input?.sequence ?? 1}` : input.fingerprint,
  reason:
    input?.activation === "waiting-source"
      ? "source-snapshot-unavailable"
      : input?.activation === "inactive"
        ? "mode-inactive"
        : null,
});

const pollSucceeded = (projectId: ProjectIdType, sequence: number): AgentControlGithubEvent => ({
  eventId: EventId.make(`poll-event-${projectId}-${sequence}`),
  type: "agentControl.github.poll.succeeded",
  aggregateKind: "github-intake",
  aggregateId: projectId,
  streamVersion: sequence,
  sequence,
  occurredAt: at,
  commandId: CommandId.make(`poll-command-${projectId}-${sequence}`),
  causationEventId: null,
  correlationId: CommandId.make(`poll-command-${projectId}-${sequence}`),
  authority: "controller",
  payload: {
    projectId,
    repository,
    attemptedAt: at,
    completedAt: at,
    cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
    issues: [],
  },
  metadata: { schemaVersion: 1 },
});

const modeChanged = (
  projectId: ProjectIdType,
  previousMode: "manual" | "observe" | "paused",
  mode: "manual" | "observe" | "paused",
  sequence = 1,
): AgentControlEvent => ({
  eventId: EventId.make(`mode-event-${projectId}-${sequence}`),
  type: "agentControl.project.mode.changed",
  aggregateKind: "project-controller",
  aggregateId: projectId,
  streamVersion: sequence,
  sequence,
  occurredAt: at,
  commandId: CommandId.make(`mode-command-${projectId}-${sequence}`),
  causationEventId: null,
  correlationId: CommandId.make(`mode-command-${projectId}-${sequence}`),
  authority: "human",
  payload: {
    projectId,
    previousMode,
    mode,
    previousPausedFromMode: null,
    pausedFromMode: null,
    changedAt: at,
  },
  metadata: { schemaVersion: 1 },
});

const projectDeleted = (projectId: ProjectIdType): OrchestrationEvent =>
  ({
    eventId: "project-deleted-event",
    type: "project.deleted",
    aggregateKind: "project",
    aggregateId: projectId,
    streamVersion: 1,
    sequence: 1,
    occurredAt: at,
    commandId: "project-deleted-command",
    causationEventId: null,
    correlationId: "project-deleted-command",
    authority: "system",
    payload: { projectId, deletedAt: at },
    metadata: { schemaVersion: 1 },
  }) as unknown as OrchestrationEvent;

const eventually = Effect.fn("test.eventually")(function* (
  predicate: () => boolean,
  message: string,
) {
  for (let attempt = 0; attempt < 1_000; attempt += 1) {
    if (predicate()) return;
    yield* Effect.yieldNow;
  }
  return yield* Effect.die(new Error(message));
});

type SubscriptionName = "project-controller" | "github-intake" | "project-delete";

const makeHarness = (options?: {
  readonly projects?: ReadonlyArray<ProjectIdType>;
  readonly listPersisted?: (
    activeSubscriptions: Readonly<Record<SubscriptionName, number>>,
  ) => Effect.Effect<
    ReadonlyArray<{
      readonly _tag: "Valid";
      readonly state: {
        readonly schemaVersion: 1;
        readonly projectId: ProjectIdType;
        readonly mode: "observe";
        readonly pausedFromMode: null;
        readonly revision: 1;
        readonly sequence: 1;
        readonly updatedAt: typeof at;
      };
    }>,
    AgentControlRepositoryError
  >;
  readonly reactorOptions?: AgentControlTaskIntakeReactorOptions;
  readonly faultSubscriptionOnce?: SubscriptionName;
  readonly failSubscriptionAcquisitionAttempts?: {
    readonly name: SubscriptionName;
    readonly count: number;
  };
}) =>
  Effect.gen(function* () {
    const projectEvents = yield* PubSub.unbounded<AgentControlEvent>();
    const githubEvents = yield* PubSub.unbounded<AgentControlGithubEvent>();
    const orchestrationEvents = yield* PubSub.unbounded<OrchestrationEvent>();
    const gates = new Map<ProjectIdType, AgentControlTaskProjectGate>();
    const unavailable = new Set<ProjectIdType>();
    const reconcileCalls: Array<{
      readonly projectId: ProjectIdType;
      readonly sequence: number | null;
    }> = [];
    const activeByProject = new Map<ProjectIdType, number>();
    const maxActiveByProject = new Map<ProjectIdType, number>();
    let totalActive = 0;
    let maxTotalActive = 0;
    let reconcileImplementation: AgentControlTaskIntake["Service"]["reconcileOnce"] = ({
      projectId,
    }) => {
      const current = gates.get(projectId) ?? gate(projectId);
      gates.set(
        projectId,
        gate(projectId, {
          sequence: current.currentSourceSequence,
          current: true,
          fingerprint: current.sourceFingerprint,
        }),
      );
      return Effect.succeed({
        projectId,
        githubIntakeSequence: current.currentSourceSequence ?? 1,
        observedCount: 0,
        createdCount: 0,
        updatedCount: 0,
        needsAttentionCount: 0,
        unchangedCount: 0,
      });
    };
    const activeSubscriptions: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const subscriptionStarts: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const subscriptionReleases: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };

    const subscribe = <A>(name: SubscriptionName, pubsub: PubSub.PubSub<A>) =>
      Effect.gen(function* () {
        subscriptionStarts[name] += 1;
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            activeSubscriptions[name] += 1;
          }),
          () =>
            Effect.sync(() => {
              activeSubscriptions[name] -= 1;
              subscriptionReleases[name] += 1;
            }),
        );
        if (
          options?.failSubscriptionAcquisitionAttempts?.name === name &&
          subscriptionStarts[name] <= options.failSubscriptionAcquisitionAttempts.count
        ) {
          return yield* Effect.die(`subscription-acquisition-fault-${name}`);
        }
        const subscription = yield* PubSub.subscribe(pubsub);
        return options?.faultSubscriptionOnce === name && subscriptionStarts[name] === 1
          ? Stream.die(`subscription-fault-${name}`)
          : Stream.fromSubscription(subscription);
      });

    const listedProjects = options?.projects ?? [];
    for (const projectId of listedProjects) gates.set(projectId, gate(projectId));
    const projectEntries = listedProjects.map((projectId) => ({
      _tag: "Valid" as const,
      state: {
        schemaVersion: 1 as const,
        projectId,
        mode: "observe" as const,
        pausedFromMode: null,
        revision: 1 as const,
        sequence: 1 as const,
        updatedAt: at,
      },
    }));

    const reactor = yield* make({
      retryBaseMs: 1_000,
      retryMaxMs: 30_000,
      watchdogIntervalMs: 60_000,
      ...options?.reactorOptions,
    }).pipe(
      Effect.provideService(AgentControlEngine, {
        getProjectState: () => Effect.die("unused"),
        dispatchHuman: () => Effect.die("unused"),
        dispatchController: () => Effect.die("unused"),
        dispatchSystem: () => Effect.die("unused"),
        streamDomainEvents: Stream.fromPubSub(projectEvents),
        subscribeDomainEvents: subscribe("project-controller", projectEvents),
      }),
      Effect.provideService(AgentControlGithubIntake, {
        getTrackerConfig: () => Effect.die("unused"),
        setTrackerConfig: () => Effect.die("unused"),
        clearTrackerConfig: () => Effect.die("unused"),
        getObserveState: () => Effect.die("unused"),
        listObservedIssues: () => Effect.die("unused"),
        pollOnce: () => Effect.die("unused"),
        streamDomainEvents: Stream.fromPubSub(githubEvents),
        subscribeDomainEvents: subscribe("github-intake", githubEvents),
      }),
      Effect.provideService(OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        dispatch: () => Effect.die("unused"),
        dispatchClient: () => Effect.die("unused"),
        dispatchAgentControl: () => Effect.die("unused"),
        streamDomainEvents: Stream.fromPubSub(orchestrationEvents),
        subscribeDomainEvents: subscribe("project-delete", orchestrationEvents),
        latestSequence: Effect.succeed(0),
      }),
      Effect.provideService(AgentControlProjectStateRepository, {
        get: () => Effect.succeed(Option.none()),
        save: () => Effect.die("unused"),
        listPersisted:
          options?.listPersisted?.(activeSubscriptions) ?? Effect.succeed(projectEntries),
        deleteAll: Effect.die("unused"),
      }),
      Effect.provideService(AgentControlProjectAvailability, {
        ensureAvailable: (projectId) =>
          unavailable.has(projectId)
            ? Effect.fail({
                _tag: "AgentControlProjectUnavailableError",
                projectId,
                reason: "deleted",
              } as never)
            : Effect.void,
      }),
      Effect.provideService(AgentControlTaskConsumerGuard, {
        inspectProject: (projectId) =>
          Effect.succeed(gates.get(projectId) ?? gate(projectId, { activation: "inactive" })),
        ensureCurrent: (projectId) =>
          Effect.succeed(gates.get(projectId) ?? gate(projectId, { activation: "inactive" })),
      }),
      Effect.provideService(AgentControlTaskIntake, {
        getTask: () => Effect.die("unused"),
        listTasks: () => Effect.die("unused"),
        reconcileOnce: (input) => {
          const projectId = input.projectId;
          const current = gates.get(projectId);
          reconcileCalls.push({
            projectId,
            sequence: current?.currentSourceSequence ?? null,
          });
          const active = (activeByProject.get(projectId) ?? 0) + 1;
          activeByProject.set(projectId, active);
          maxActiveByProject.set(
            projectId,
            Math.max(maxActiveByProject.get(projectId) ?? 0, active),
          );
          totalActive += 1;
          maxTotalActive = Math.max(maxTotalActive, totalActive);
          return reconcileImplementation(input).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                activeByProject.set(projectId, (activeByProject.get(projectId) ?? 1) - 1);
                totalActive -= 1;
              }),
            ),
          );
        },
      }),
    );

    return {
      reactor,
      projectEvents,
      githubEvents,
      orchestrationEvents,
      gates,
      unavailable,
      reconcileCalls,
      setReconcile: (implementation: AgentControlTaskIntake["Service"]["reconcileOnce"]) => {
        reconcileImplementation = implementation;
      },
      maxActive: (projectId: ProjectIdType) => maxActiveByProject.get(projectId) ?? 0,
      maxTotalActive: () => maxTotalActive,
      activeTotal: () => totalActive,
      activeSubscriptions,
      subscriptionStarts,
      subscriptionReleases,
    };
  });

const start = Effect.fn("test.startReactor")(function* (
  reactor: AgentControlTaskIntakeReactor["Service"],
) {
  const scope = yield* Scope.make("sequential");
  yield* reactor.start().pipe(Scope.provide(scope));
  return scope;
});

it.effect(
  "activates subscriptions before enumeration and waits for project reconcile barriers",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-barrier");
      const release = yield* Deferred.make<void>();
      let subscriptionsWereActive = false;
      const harness = yield* makeHarness({
        projects: [projectId],
        listPersisted: (active) =>
          Effect.sync(() => {
            subscriptionsWereActive = Object.values(active).every((count) => count === 1);
            return [
              {
                _tag: "Valid" as const,
                state: {
                  schemaVersion: 1 as const,
                  projectId,
                  mode: "observe" as const,
                  pausedFromMode: null,
                  revision: 1 as const,
                  sequence: 1 as const,
                  updatedAt: at,
                },
              },
            ];
          }),
      });
      harness.setReconcile(({ projectId: id }) =>
        Deferred.await(release).pipe(
          Effect.tap(() => {
            harness.gates.set(id, gate(id, { current: true }));
            return Effect.void;
          }),
          Effect.as({
            projectId: id,
            githubIntakeSequence: 1,
            observedCount: 0,
            createdCount: 0,
            updatedCount: 0,
            needsAttentionCount: 0,
            unchangedCount: 0,
          }),
        ),
      );
      const scope = yield* Scope.make("sequential");
      const startupCompleted = yield* Deferred.make<void>();
      const startup = yield* harness.reactor.start().pipe(
        Scope.provide(scope),
        Effect.tap(() => Deferred.succeed(startupCompleted, undefined)),
        Effect.forkChild,
      );
      yield* eventually(
        () => harness.reconcileCalls.length === 1,
        "initial reconcile was not launched",
      );
      assert.isTrue(subscriptionsWereActive);
      assert.isFalse(yield* Deferred.isDone(startupCompleted));
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(startup);
      yield* Scope.close(scope, Exit.void);
      assert.deepStrictEqual(Object.values(harness.activeSubscriptions), [0, 0, 0]);
      assert.deepStrictEqual(Object.values(harness.subscriptionReleases), [1, 1, 1]);
    }),
);

it.effect("poll success reconciles once and burst events coalesce onto the newest source", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("poll-coalescing");
    const harness = yield* makeHarness();
    const scope = yield* start(harness.reactor);
    harness.gates.set(projectId, gate(projectId, { sequence: 1 }));
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(projectId, 1));
    yield* eventually(() => harness.reconcileCalls.length === 1, "poll did not reconcile");

    harness.gates.set(projectId, gate(projectId, { sequence: 4, fingerprint: "snapshot-4" }));
    yield* Effect.forEach(
      [2, 3, 4],
      (sequence) => PubSub.publish(harness.githubEvents, pollSucceeded(projectId, sequence)),
      { discard: true },
    );
    yield* eventually(() => harness.reconcileCalls.length === 2, "burst did not reconcile");
    yield* Effect.yieldNow;
    assert.deepStrictEqual(
      harness.reconcileCalls.map((call) => call.sequence),
      [1, 4],
    );
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect(
  "manual and paused stay inactive; Observe transition reconciles; exit and delete clean up",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("mode-rules");
      const harness = yield* makeHarness();
      const scope = yield* start(harness.reactor);

      harness.gates.set(projectId, gate(projectId, { activation: "inactive", fingerprint: null }));
      yield* PubSub.publish(harness.githubEvents, pollSucceeded(projectId, 1));
      yield* PubSub.publish(harness.projectEvents, modeChanged(projectId, "manual", "paused", 2));
      yield* Effect.yieldNow;
      assert.equal(harness.reconcileCalls.length, 0);

      harness.gates.set(projectId, gate(projectId, { sequence: 2 }));
      yield* PubSub.publish(harness.projectEvents, modeChanged(projectId, "manual", "observe", 3));
      yield* eventually(() => harness.reconcileCalls.length === 1, "Observe did not reconcile");

      harness.gates.set(projectId, gate(projectId, { activation: "inactive", fingerprint: null }));
      yield* PubSub.publish(harness.projectEvents, modeChanged(projectId, "observe", "paused", 4));
      let paused = yield* harness.reactor.getStatus({ projectId });
      for (let attempt = 0; attempt < 100 && paused.workerState !== "stopped"; attempt += 1) {
        yield* Effect.yieldNow;
        paused = yield* harness.reactor.getStatus({ projectId });
      }
      assert.equal(paused.activity, "inactive");
      assert.equal(paused.workerState, "stopped");

      harness.gates.set(projectId, gate(projectId, { sequence: 3 }));
      yield* PubSub.publish(harness.orchestrationEvents, projectDeleted(projectId));
      yield* Effect.yieldNow;
      yield* Effect.yieldNow;
      const deletedRuntime = yield* harness.reactor.getStatus({ projectId });
      assert.equal(deletedRuntime.workerState, "stopped");
      yield* Scope.close(scope, Exit.void);
    }),
);

it.effect("leaving Observe interrupts in-flight automatic work without starting an overlap", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("pause-in-flight");
    const release = yield* Deferred.make<void>();
    const harness = yield* makeHarness();
    harness.gates.set(projectId, gate(projectId));
    harness.setReconcile(({ projectId: id }) =>
      Deferred.await(release).pipe(
        Effect.as({
          projectId: id,
          githubIntakeSequence: 1,
          observedCount: 0,
          createdCount: 0,
          updatedCount: 0,
          needsAttentionCount: 0,
          unchangedCount: 0,
        }),
      ),
    );
    const scope = yield* start(harness.reactor);
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(projectId, 1));
    yield* eventually(() => harness.activeTotal() === 1, "automatic reconcile did not start");

    harness.gates.set(projectId, gate(projectId, { activation: "inactive", fingerprint: null }));
    yield* PubSub.publish(harness.projectEvents, modeChanged(projectId, "observe", "paused", 2));
    yield* eventually(() => harness.activeTotal() === 0, "paused reconcile was not interrupted");
    assert.equal(harness.maxActive(projectId), 1);
    assert.equal(harness.reconcileCalls.length, 1);
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("startup reconciles stale recovery state and skips an exact completed watermark", () =>
  Effect.gen(function* () {
    const stale = ProjectId.make("startup-stale");
    const current = ProjectId.make("startup-current");
    const harness = yield* makeHarness({ projects: [stale, current] });
    harness.gates.set(stale, gate(stale, { sequence: 3 }));
    harness.gates.set(current, gate(current, { sequence: 5, current: true }));
    const scope = yield* start(harness.reactor);
    assert.deepStrictEqual(
      harness.reconcileCalls.map((call) => call.projectId),
      [stale],
    );
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("an event during startup enumeration is retained by the already-hot subscription", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("startup-event");
    let githubEvents: PubSub.PubSub<AgentControlGithubEvent> | null = null;
    const harness = yield* makeHarness({
      listPersisted: () =>
        Effect.gen(function* () {
          if (githubEvents !== null) {
            yield* PubSub.publish(githubEvents, pollSucceeded(projectId, 1));
          }
          return [];
        }),
    });
    githubEvents = harness.githubEvents;
    harness.gates.set(projectId, gate(projectId));
    const scope = yield* start(harness.reactor);
    yield* eventually(
      () => harness.reconcileCalls.some((call) => call.projectId === projectId),
      "startup event was lost",
    );
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("projects run in parallel while each project remains strictly serialized", () =>
  Effect.gen(function* () {
    const first = ProjectId.make("parallel-first");
    const second = ProjectId.make("parallel-second");
    const firstRelease = yield* Deferred.make<void>();
    const secondRelease = yield* Deferred.make<void>();
    const harness = yield* makeHarness();
    harness.gates.set(first, gate(first));
    harness.gates.set(second, gate(second));
    harness.setReconcile(({ projectId }) =>
      Deferred.await(projectId === first ? firstRelease : secondRelease).pipe(
        Effect.tap(() => {
          harness.gates.set(projectId, gate(projectId, { current: true }));
          return Effect.void;
        }),
        Effect.as({
          projectId,
          githubIntakeSequence: 1,
          observedCount: 0,
          createdCount: 0,
          updatedCount: 0,
          needsAttentionCount: 0,
          unchangedCount: 0,
        }),
      ),
    );
    const scope = yield* start(harness.reactor);
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(first, 1));
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(second, 1));
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(first, 2));
    yield* eventually(() => harness.maxTotalActive() === 2, "projects did not run in parallel");
    assert.equal(harness.maxActive(first), 1);
    assert.equal(harness.maxActive(second), 1);
    yield* Deferred.succeed(firstRelease, undefined);
    yield* Deferred.succeed(secondRelease, undefined);
    yield* eventually(
      () => harness.reconcileCalls.length === 2,
      "project reconciles did not finish",
    );
    assert.equal(harness.maxActive(first), 1);
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("a source change during reconcile is latched into a follow-up pass", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("follow-up-pass");
    const firstRelease = yield* Deferred.make<void>();
    const harness = yield* makeHarness();
    harness.gates.set(projectId, gate(projectId, { sequence: 1, fingerprint: "source-1" }));
    let calls = 0;
    harness.setReconcile(({ projectId: id }) => {
      calls += 1;
      if (calls === 1) {
        return Deferred.await(firstRelease).pipe(
          Effect.as({
            projectId: id,
            githubIntakeSequence: 1,
            observedCount: 0,
            createdCount: 0,
            updatedCount: 0,
            needsAttentionCount: 0,
            unchangedCount: 0,
          }),
        );
      }
      harness.gates.set(id, gate(id, { sequence: 2, current: true, fingerprint: "source-2" }));
      return Effect.succeed({
        projectId: id,
        githubIntakeSequence: 2,
        observedCount: 0,
        createdCount: 0,
        updatedCount: 0,
        needsAttentionCount: 0,
        unchangedCount: 0,
      });
    });
    const scope = yield* start(harness.reactor);
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(projectId, 1));
    yield* eventually(() => calls === 1, "first pass did not start");
    harness.gates.set(projectId, gate(projectId, { sequence: 2, fingerprint: "source-2" }));
    yield* PubSub.publish(harness.githubEvents, pollSucceeded(projectId, 2));
    yield* Deferred.succeed(firstRelease, undefined);
    yield* eventually(() => calls === 2, "follow-up pass did not run");
    assert.deepStrictEqual(
      harness.reconcileCalls.map((call) => call.sequence),
      [1, 2],
    );
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect(
  "retryable errors back off; hard fingerprint suspension resets only for a new snapshot",
  () =>
    Effect.gen(function* () {
      const retryProject = ProjectId.make("retry-backoff");
      const hardProject = ProjectId.make("hard-fingerprint");
      const harness = yield* makeHarness({ reactorOptions: { watchdogIntervalMs: 10_000 } });
      harness.gates.set(retryProject, gate(retryProject, { fingerprint: "retry-1" }));
      harness.gates.set(hardProject, gate(hardProject, { fingerprint: "hard-1" }));
      let retryFailures = 0;
      harness.setReconcile(({ projectId }) => {
        if (projectId === hardProject) {
          return Effect.fail(
            new AgentControlTaskRpcError({
              code: "source-identity-conflict",
              operation: "reconcile-once",
              projectId,
              taskId: null,
            }),
          );
        }
        retryFailures += 1;
        return Effect.fail(
          new AgentControlTaskRpcError({
            code: "internal-persistence-error",
            operation: "reconcile-once",
            projectId,
            taskId: null,
          }),
        );
      });
      const scope = yield* start(harness.reactor);
      yield* PubSub.publish(harness.githubEvents, pollSucceeded(retryProject, 1));
      yield* eventually(() => retryFailures === 1, "retry pass did not start");
      yield* TestClock.adjust(Duration.millis(999));
      assert.equal(retryFailures, 1);
      yield* TestClock.adjust(Duration.millis(1));
      yield* eventually(() => retryFailures === 2, "one-second retry did not run");
      for (const [delay, expected] of [
        [2, 3],
        [4, 4],
        [8, 5],
        [16, 6],
      ] as const) {
        yield* TestClock.adjust(Duration.seconds(delay));
        yield* eventually(
          () => retryFailures === expected,
          `retry attempt ${expected} did not run`,
        );
      }
      yield* TestClock.adjust(Duration.seconds(30));
      assert.equal(retryFailures, 6);
      const retryStatus = yield* harness.reactor.getStatus({ projectId: retryProject });
      assert.equal(retryStatus.activity, "suspended");
      assert.equal(retryStatus.retryAttempt, 5);

      yield* PubSub.publish(harness.githubEvents, pollSucceeded(hardProject, 1));
      yield* eventually(
        () => harness.reconcileCalls.some((call) => call.projectId === hardProject),
        "hard failure did not run",
      );
      const hardCalls = harness.reconcileCalls.filter(
        (call) => call.projectId === hardProject,
      ).length;
      yield* PubSub.publish(harness.githubEvents, pollSucceeded(hardProject, 2));
      yield* TestClock.adjust(Duration.seconds(10));
      assert.equal(
        harness.reconcileCalls.filter((call) => call.projectId === hardProject).length,
        hardCalls,
      );

      harness.gates.set(hardProject, gate(hardProject, { sequence: 2, fingerprint: "hard-2" }));
      harness.setReconcile(({ projectId }) => {
        harness.gates.set(
          projectId,
          gate(projectId, { sequence: 2, current: true, fingerprint: "hard-2" }),
        );
        return Effect.succeed({
          projectId,
          githubIntakeSequence: 2,
          observedCount: 0,
          createdCount: 0,
          updatedCount: 0,
          needsAttentionCount: 0,
          unchangedCount: 0,
        });
      });
      yield* PubSub.publish(harness.githubEvents, pollSucceeded(hardProject, 3));
      yield* eventually(
        () =>
          harness.reconcileCalls.filter((call) => call.projectId === hardProject).length ===
          hardCalls + 1,
        "new snapshot did not reset suspension",
      );
      yield* Scope.close(scope, Exit.void);
    }),
);

it.effect("global enumeration failure is visible and a bounded retry recovers it", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("global-enumeration-recovery");
    let enumerations = 0;
    const harness = yield* makeHarness({
      projects: [projectId],
      listPersisted: () =>
        Effect.suspend(() => {
          enumerations += 1;
          if (enumerations === 2) {
            return Effect.fail(
              new AgentControlPersistenceSqlError({
                operation: "test.enumeration",
              }),
            );
          }
          return Effect.succeed([
            {
              _tag: "Valid" as const,
              state: {
                schemaVersion: 1 as const,
                projectId,
                mode: "observe" as const,
                pausedFromMode: null,
                revision: 1 as const,
                sequence: 1 as const,
                updatedAt: at,
              },
            },
          ]);
        }),
      reactorOptions: { watchdogIntervalMs: 60_000 },
    });
    harness.gates.set(projectId, gate(projectId, { current: true }));
    const scope = yield* start(harness.reactor);
    harness.gates.set(projectId, gate(projectId, { sequence: 2, fingerprint: "recovery-2" }));

    yield* TestClock.adjust(Duration.seconds(60));
    yield* eventually(() => enumerations === 2, "watchdog enumeration did not run");
    yield* Effect.yieldNow;
    const recovering = yield* harness.reactor.getStatus({ projectId });
    assert.equal(recovering.globalHealth, "recovering");
    assert.equal(recovering.lastErrorCode, "enumeration-failed");

    yield* TestClock.adjust(Duration.seconds(1));
    yield* eventually(
      () => harness.reconcileCalls.some((call) => call.sequence === 2),
      "global retry did not recover enumeration",
    );
    const recovered = yield* harness.reactor.getStatus({ projectId });
    assert.equal(recovered.globalHealth, "healthy");
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("every defective hot subscription is finalized, rebuilt, and reconciled", () =>
  Effect.gen(function* () {
    for (const name of [
      "project-controller",
      "github-intake",
      "project-delete",
    ] satisfies ReadonlyArray<SubscriptionName>) {
      const harness = yield* makeHarness({
        faultSubscriptionOnce: name,
        reactorOptions: {
          subscriptionRetryBaseMs: 250,
          subscriptionRetryMaxMs: 250,
        },
      });
      const scope = yield* Scope.make("sequential");
      const startup = yield* harness.reactor.start().pipe(Scope.provide(scope), Effect.forkChild);
      yield* TestClock.adjust(Duration.millis(250));
      yield* Fiber.join(startup);
      assert.equal(harness.subscriptionStarts[name], 2);
      assert.equal(harness.subscriptionReleases[name], 1);
      assert.equal(harness.activeSubscriptions[name], 1);
      yield* Scope.close(scope, Exit.void);
      assert.equal(harness.subscriptionReleases[name], 2);
    }
  }),
);

it.effect("bounded acquisition failure fails closed and permits a later clean start", () =>
  Effect.gen(function* () {
    const harness = yield* makeHarness({
      failSubscriptionAcquisitionAttempts: {
        name: "github-intake",
        count: 3,
      },
      reactorOptions: {
        subscriptionRetryBaseMs: 250,
        subscriptionRetryMaxMs: 500,
        subscriptionStartupAttempts: 3,
      },
    });
    const firstScope = yield* Scope.make("sequential");
    const firstStart = yield* harness.reactor
      .start()
      .pipe(Scope.provide(firstScope), Effect.result, Effect.forkChild);
    yield* TestClock.adjust(Duration.millis(750));
    const firstResult = yield* Fiber.join(firstStart);
    assert.equal(firstResult._tag, "Failure");
    if (firstResult._tag === "Failure") {
      assert.equal(firstResult.failure.reason, "subscription-activation-failed");
    }
    assert.equal(harness.subscriptionStarts["github-intake"], 3);
    assert.equal(harness.subscriptionReleases["github-intake"], 3);
    assert.equal(harness.activeSubscriptions["github-intake"], 0);

    const secondScope = yield* start(harness.reactor);
    assert.equal(harness.subscriptionStarts["github-intake"], 4);
    yield* Scope.close(secondScope, Exit.void);
  }),
);

it.effect("watchdog repairs missing runtime state and status stays transport-safe", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("watchdog-repair");
    const harness = yield* makeHarness({
      projects: [projectId],
      reactorOptions: { watchdogIntervalMs: 60_000 },
    });
    harness.gates.set(projectId, gate(projectId, { current: true }));
    const scope = yield* start(harness.reactor);
    harness.gates.set(projectId, gate(projectId, { sequence: 2, fingerprint: "watchdog-2" }));
    yield* TestClock.adjust(Duration.seconds(60));
    yield* eventually(
      () => harness.reconcileCalls.some((call) => call.sequence === 2),
      "watchdog did not repair stale state",
    );
    const status = yield* harness.reactor.getStatus({ projectId });
    assert.deepStrictEqual(Object.keys(status).sort(), [
      "activity",
      "currentSourceSequence",
      "globalHealth",
      "health",
      "lastCompletedSequence",
      "lastErrorCode",
      "nextAttemptAt",
      "projectId",
      "retryAttempt",
      "sequenceCurrent",
      "subscriptionHealth",
      "targetSequence",
      "workerState",
    ]);
    assert.equal(status.subscriptionHealth, "healthy");
    yield* Scope.close(scope, Exit.void);
  }),
);

it.effect("an interrupted startup finalizes subscriptions and a clean second start succeeds", () =>
  Effect.gen(function* () {
    const enumerationGate = yield* Deferred.make<void>();
    const interruptionObserved = yield* Deferred.make<void>();
    let enumerationAttempts = 0;
    const projectId = ProjectId.make("restart-after-interrupt");
    const harness = yield* makeHarness({
      listPersisted: () =>
        Effect.suspend(() => {
          enumerationAttempts += 1;
          return enumerationAttempts === 1
            ? Deferred.await(enumerationGate).pipe(
                Effect.onInterrupt(() => Deferred.succeed(interruptionObserved, undefined)),
                Effect.as([]),
              )
            : Effect.succeed([]);
        }),
    });
    harness.gates.set(projectId, gate(projectId));
    const firstScope = yield* Scope.make("sequential");
    const firstStart = yield* harness.reactor
      .start()
      .pipe(Scope.provide(firstScope), Effect.forkChild);
    yield* eventually(
      () => Object.values(harness.activeSubscriptions).every((count) => count === 1),
      "subscriptions did not activate",
    );
    const interruption = yield* Fiber.interrupt(firstStart).pipe(Effect.forkChild);
    yield* Deferred.await(interruptionObserved);
    yield* Deferred.succeed(enumerationGate, undefined);
    yield* Fiber.join(interruption);
    yield* Scope.close(firstScope, Exit.void);
    yield* eventually(
      () => Object.values(harness.activeSubscriptions).every((count) => count === 0),
      "interrupted startup leaked subscriptions",
    );

    const secondScope = yield* start(harness.reactor);
    assert.equal(enumerationAttempts, 2);
    assert.deepStrictEqual(Object.values(harness.activeSubscriptions), [1, 1, 1]);
    yield* Scope.close(secondScope, Exit.void);
    assert.deepStrictEqual(Object.values(harness.activeSubscriptions), [0, 0, 0]);
  }),
);
