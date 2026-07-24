import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlGithubRpcError,
  CommandId,
  EventId,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  AgentControlGithubIntakeState,
  type AgentControlGithubPollErrorCode,
  type AgentControlGithubPollOnceInput,
  type AgentControlGithubTrackerSettings,
  type OrchestrationEvent,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlProjectAvailabilityLive } from "../../../persistence/Layers/AgentControlProjectAvailability.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import {
  AgentControlProjectionStateRepositoryLive,
  AgentControlProjectStateRepositoryLive,
} from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { AgentControlGithubIntake } from "../Services/AgentControlGithubIntake.ts";
import { AgentControlGithubObserveReactor } from "../Services/AgentControlGithubObserveReactor.ts";
import { AgentControlGithubSchedulerStateRepository } from "../Services/AgentControlGithubSchedulerState.ts";
import { AgentControlGithubStateRepository } from "../Services/AgentControlGithubStateRepository.ts";
import { layer as GithubSchedulerStateLive } from "./AgentControlGithubSchedulerState.ts";
import { layer as GithubEventStoreLive } from "./AgentControlGithubEventStore.ts";
import { layer as GithubStateRepositoryLive } from "./AgentControlGithubStateRepository.ts";
import {
  AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS,
  AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS,
  githubObserveBackoffMs,
  make,
  type AgentControlGithubObserveReactorOptions,
} from "./AgentControlGithubObserveReactor.ts";
import { AgentControlPersistenceSqlError } from "../../Errors.ts";

const EPOCH = "1970-01-01T00:00:00.000Z";
const isGithubRpcError = Schema.is(AgentControlGithubRpcError);
const repository = {
  repositoryNodeId: "repository-node",
  nameWithOwner: "owner/repo",
} as const;
const settings = (pollIntervalSeconds = 15): AgentControlGithubTrackerSettings => ({
  trackerKind: "github",
  readyLabel: "agent:ready",
  pausedLabel: "agent:paused",
  trustedLogins: ["trusted"],
  pollIntervalSeconds,
});
const encodeIntakeState = Schema.encodeSync(Schema.fromJsonString(AgentControlGithubIntakeState));
const encodeUnknownJson = Schema.encodeSync(Schema.UnknownFromJsonString);

const persistGithubEvent = Effect.fn("test.persistGithubEvent")(function* (
  event: AgentControlGithubEvent,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT OR IGNORE INTO agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      ${event.eventId}, ${event.aggregateKind}, ${event.aggregateId},
      ${event.streamVersion}, ${event.type}, ${event.occurredAt},
      ${event.commandId}, ${event.causationEventId}, ${event.correlationId},
      ${event.authority}, ${encodeUnknownJson(event.payload)}, ${encodeUnknownJson(event.metadata)}
    )
  `;
});

const persistenceLayer = Layer.mergeAll(
  AgentControlProjectStateRepositoryLive,
  AgentControlProjectionStateRepositoryLive,
  GithubStateRepositoryLive,
  GithubEventStoreLive,
  GithubSchedulerStateLive,
  AgentControlProjectAvailabilityLive,
).pipe(Layer.provideMerge(SqlitePersistenceMemory), Layer.provideMerge(NodeServices.layer));
const testLayer = Layer.merge(persistenceLayer, TestClock.layer());

const intakeState = (
  projectId: ProjectId,
  pollIntervalSeconds = 15,
): AgentControlGithubIntakeState => ({
  schemaVersion: 1,
  projectId,
  config: {
    schemaVersion: 1,
    projectId,
    settings: settings(pollIntervalSeconds),
    repository,
    revision: 1,
    sequence: 1,
    updatedAt: EPOCH,
  },
  cursor: null,
  pollStatus: {
    status: "not-polled",
    attemptedAt: null,
    completedAt: null,
    errorCode: null,
  },
  revision: 1,
  sequence: 1,
  updatedAt: EPOCH,
});

const addProject = Effect.fn("test.addProject")(function* (input: {
  readonly projectId: ProjectId;
  readonly mode: "manual" | "observe" | "paused";
  readonly withConfig?: boolean;
  readonly pollIntervalSeconds?: number;
  readonly corruptControllerProjection?: boolean;
}) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${input.projectId}, 'Observe reactor test', ${`/tmp/${input.projectId}`},
      NULL, '[]', ${EPOCH}, ${EPOCH}, NULL
    )
  `;
  yield* sql`
    INSERT INTO agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (
      ${input.projectId}, ${input.mode},
      ${input.mode === "paused" ? "observe" : null},
      ${input.corruptControllerProjection ? "broken" : 1}, 1, ${EPOCH}
    )
  `;
  if (input.withConfig !== false) {
    const state = intakeState(input.projectId, input.pollIntervalSeconds);
    yield* sql`
      INSERT INTO agent_control_github_intake_states (
        project_id, state_json, revision, last_event_sequence, updated_at
      ) VALUES (
        ${input.projectId}, ${encodeIntakeState(state)}, 1, 1, ${EPOCH}
      )
    `;
    yield* persistGithubEvent(githubConfigEvent(input.projectId, input.pollIntervalSeconds ?? 15));
  }
});

const updateMode = Effect.fn("test.updateMode")(function* (
  projectId: ProjectId,
  mode: "manual" | "observe" | "paused",
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    UPDATE agent_control_project_states
    SET mode = ${mode}, paused_from_mode = ${mode === "paused" ? "observe" : null},
        revision = revision + 1, last_event_sequence = last_event_sequence + 1
    WHERE project_id = ${projectId}
  `;
});

const setGithubState = Effect.fn("test.setGithubState")(function* (
  projectId: ProjectId,
  pollIntervalSeconds: number,
  revision = 1,
) {
  const sql = yield* SqlClient.SqlClient;
  const state: AgentControlGithubIntakeState = {
    ...intakeState(projectId, pollIntervalSeconds),
    revision,
    sequence: revision,
  };
  yield* sql`
    INSERT INTO agent_control_github_intake_states (
      project_id, state_json, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, ${encodeIntakeState(state)}, ${revision}, ${revision}, ${EPOCH})
    ON CONFLICT (project_id) DO UPDATE SET
      state_json = excluded.state_json,
      revision = excluded.revision,
      last_event_sequence = excluded.last_event_sequence,
      updated_at = excluded.updated_at
  `;
});

const setGithubFailureState = Effect.fn("test.setGithubFailureState")(function* (
  projectId: ProjectId,
  sequence: number,
  errorCode: AgentControlGithubPollErrorCode,
) {
  const sql = yield* SqlClient.SqlClient;
  const state: AgentControlGithubIntakeState = {
    ...intakeState(projectId),
    pollStatus: {
      status: "needs-attention",
      attemptedAt: EPOCH,
      completedAt: EPOCH,
      errorCode,
    },
    revision: sequence,
    sequence,
    updatedAt: EPOCH,
  };
  yield* sql`
    UPDATE agent_control_github_intake_states
    SET state_json = ${encodeIntakeState(state)}, revision = ${sequence},
        last_event_sequence = ${sequence}, updated_at = ${EPOCH}
    WHERE project_id = ${projectId}
  `;
  yield* persistGithubEvent(
    githubPollEvent(projectId, { type: "failure", sequence, code: errorCode }),
  );
});

const projectEvent = (
  projectId: ProjectId,
  previousMode: "manual" | "observe" | "paused",
  mode: "manual" | "observe" | "paused",
  sequence = 1,
): AgentControlEvent => ({
  eventId: EventId.make(`mode-${projectId}-${sequence}`),
  type: "agentControl.project.mode.changed",
  aggregateKind: "project-controller",
  aggregateId: projectId,
  occurredAt: EPOCH,
  commandId: CommandId.make(`mode-command-${projectId}-${sequence}`),
  causationEventId: null,
  correlationId: CommandId.make(`mode-command-${projectId}-${sequence}`),
  authority: "human",
  payload: {
    projectId,
    previousMode,
    mode,
    previousPausedFromMode: previousMode === "paused" ? "observe" : null,
    pausedFromMode: mode === "paused" ? "observe" : null,
    changedAt: EPOCH,
  },
  metadata: { schemaVersion: 1 },
  streamVersion: sequence,
  sequence,
});

const githubConfigEvent = (
  projectId: ProjectId,
  pollIntervalSeconds: number,
  sequence = 1,
): AgentControlGithubEvent => ({
  eventId: EventId.make(`github-config-${projectId}-${sequence}`),
  type: "agentControl.github.config.set",
  aggregateKind: "github-intake",
  aggregateId: projectId,
  occurredAt: EPOCH,
  commandId: CommandId.make(`github-config-command-${projectId}-${sequence}`),
  causationEventId: null,
  correlationId: CommandId.make(`github-config-command-${projectId}-${sequence}`),
  authority: "human",
  payload: {
    projectId,
    settings: settings(pollIntervalSeconds),
    repository,
    configuredAt: EPOCH,
  },
  metadata: { schemaVersion: 1 },
  streamVersion: sequence,
  sequence,
});

const githubClearEvent = (projectId: ProjectId, sequence = 2): AgentControlGithubEvent => ({
  eventId: EventId.make(`github-clear-${projectId}-${sequence}`),
  type: "agentControl.github.config.cleared",
  aggregateKind: "github-intake",
  aggregateId: projectId,
  occurredAt: EPOCH,
  commandId: CommandId.make(`github-clear-command-${projectId}-${sequence}`),
  causationEventId: null,
  correlationId: CommandId.make(`github-clear-command-${projectId}-${sequence}`),
  authority: "human",
  payload: { projectId, clearedAt: EPOCH },
  metadata: { schemaVersion: 1 },
  streamVersion: sequence,
  sequence,
});

const githubPollEvent = (
  projectId: ProjectId,
  input:
    | { readonly type: "success"; readonly sequence: number; readonly at?: string }
    | {
        readonly type: "failure";
        readonly sequence: number;
        readonly code: AgentControlGithubPollErrorCode;
        readonly at?: string;
      },
): AgentControlGithubEvent => {
  const at = input.at ?? EPOCH;
  const commandId = CommandId.make(`poll-event-command-${projectId}-${input.sequence}`);
  const base = {
    eventId: EventId.make(`poll-event-${projectId}-${input.sequence}`),
    aggregateKind: "github-intake" as const,
    aggregateId: projectId,
    occurredAt: at,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller" as const,
    metadata: { schemaVersion: 1 as const },
    streamVersion: input.sequence,
    sequence: input.sequence,
  };
  return input.type === "success"
    ? {
        ...base,
        type: "agentControl.github.poll.succeeded",
        payload: {
          projectId,
          repository,
          attemptedAt: at,
          completedAt: at,
          cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
          issues: [],
        },
      }
    : {
        ...base,
        type: "agentControl.github.poll.failed",
        payload: {
          projectId,
          attemptedAt: at,
          completedAt: at,
          errorCode: input.code,
          invalidateCursor: false,
        },
      };
};

interface Harness {
  readonly reactor: AgentControlGithubObserveReactor["Service"];
  readonly projectEvents: PubSub.PubSub<AgentControlEvent>;
  readonly githubEvents: PubSub.PubSub<AgentControlGithubEvent>;
  readonly orchestrationEvents: PubSub.PubSub<OrchestrationEvent>;
  readonly pollInputs: Array<AgentControlGithubPollOnceInput>;
  readonly activePolls: () => number;
  readonly maxActivePolls: () => number;
  readonly setPoll: (implementation: AgentControlGithubIntake["Service"]["pollOnce"]) => void;
  readonly subscriptionStarts: (name: SubscriptionName) => number;
  readonly subscriptionReleases: (name: SubscriptionName) => number;
  readonly activeSubscriptions: (name: SubscriptionName) => number;
  readonly maxActiveSubscriptions: (name: SubscriptionName) => number;
}

type SubscriptionName = "project-controller" | "github-intake" | "project-delete";

const makeHarness = (options?: {
  readonly duringEnumeration?: (input: {
    readonly githubEvents: PubSub.PubSub<AgentControlGithubEvent>;
  }) => Effect.Effect<void, never, SqlClient.SqlClient>;
  readonly listPersisted?: (
    base: AgentControlProjectStateRepository["Service"]["listPersisted"],
  ) => AgentControlProjectStateRepository["Service"]["listPersisted"];
  readonly schedulerStates?: (
    base: AgentControlGithubSchedulerStateRepository["Service"],
  ) => AgentControlGithubSchedulerStateRepository["Service"];
  readonly availability?: (
    base: AgentControlProjectAvailability["Service"],
  ) => AgentControlProjectAvailability["Service"];
  readonly reactorOptions?: AgentControlGithubObserveReactorOptions;
  readonly faultSubscriptionOnce?: SubscriptionName;
  readonly failSubscriptionAcquisitionOnce?: SubscriptionName;
  readonly failSubscriptionAcquisitionAttempts?: {
    readonly name: SubscriptionName;
    readonly count: number;
  };
  readonly endSubscriptionOnce?: SubscriptionName;
  readonly faultSubscriptionOnSignal?: {
    readonly name: SubscriptionName;
    readonly await: Effect.Effect<void>;
  };
  readonly subscriptionGate?: {
    readonly name: SubscriptionName;
    readonly await: Effect.Effect<void>;
  };
}) =>
  Effect.gen(function* () {
    const githubStates = yield* AgentControlGithubStateRepository;
    const projectStates = yield* AgentControlProjectStateRepository;
    const baseSchedulerStates = yield* AgentControlGithubSchedulerStateRepository;
    const baseAvailability = yield* AgentControlProjectAvailability;
    const sql = yield* SqlClient.SqlClient;
    const projectEvents = yield* PubSub.unbounded<AgentControlEvent>();
    const githubEvents = yield* PubSub.unbounded<AgentControlGithubEvent>();
    const orchestrationEvents = yield* PubSub.unbounded<OrchestrationEvent>();
    const pollInputs: Array<AgentControlGithubPollOnceInput> = [];
    let activePolls = 0;
    let maximumActivePolls = 0;
    let pollImplementation: AgentControlGithubIntake["Service"]["pollOnce"] = () => Effect.never;
    const subscriptionStartCounts: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const subscriptionReleaseCounts: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const activeSubscriptionCounts: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const maximumActiveSubscriptionCounts: Record<SubscriptionName, number> = {
      "project-controller": 0,
      "github-intake": 0,
      "project-delete": 0,
    };
    const subscribe = <A>(
      name: SubscriptionName,
      pubsub: PubSub.PubSub<A>,
      transform: (stream: Stream.Stream<A>) => Stream.Stream<A> = (stream) => stream,
    ) =>
      Effect.suspend(() => {
        subscriptionStartCounts[name] += 1;
        const gate =
          options?.subscriptionGate?.name === name ? options.subscriptionGate.await : Effect.void;
        return Effect.gen(function* () {
          yield* Effect.acquireRelease(
            Effect.sync(() => {
              activeSubscriptionCounts[name] += 1;
              maximumActiveSubscriptionCounts[name] = Math.max(
                maximumActiveSubscriptionCounts[name],
                activeSubscriptionCounts[name],
              );
            }),
            () =>
              Effect.sync(() => {
                activeSubscriptionCounts[name] -= 1;
                subscriptionReleaseCounts[name] += 1;
              }),
          );
          yield* gate;
          if (
            (options?.failSubscriptionAcquisitionOnce === name &&
              subscriptionStartCounts[name] === 1) ||
            (options?.failSubscriptionAcquisitionAttempts?.name === name &&
              subscriptionStartCounts[name] <= options.failSubscriptionAcquisitionAttempts.count)
          ) {
            return yield* Effect.die(`acquisition-fault-${name}`);
          }
          const subscription = yield* PubSub.subscribe(pubsub);
          if (options?.faultSubscriptionOnce === name && subscriptionStartCounts[name] === 1) {
            return Stream.die(`fault-${name}`);
          }
          if (options?.endSubscriptionOnce === name && subscriptionStartCounts[name] === 1) {
            return Stream.empty;
          }
          const stream = Stream.fromSubscription(subscription);
          if (
            options?.faultSubscriptionOnSignal?.name === name &&
            subscriptionStartCounts[name] === 1
          ) {
            return transform(
              Stream.merge(
                stream,
                Stream.fromEffect(
                  options.faultSubscriptionOnSignal.await.pipe(
                    Effect.andThen(Effect.die(`signalled-fault-${name}`)),
                  ),
                ),
              ),
            );
          }
          return transform(stream);
        });
      });
    const reactorProjectStates = AgentControlProjectStateRepository.of({
      ...projectStates,
      listPersisted:
        options?.listPersisted?.(projectStates.listPersisted) ??
        (options?.duringEnumeration === undefined
          ? projectStates.listPersisted
          : options
              .duringEnumeration({ githubEvents })
              .pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.andThen(projectStates.listPersisted),
              )),
    });

    const intake = AgentControlGithubIntake.of({
      getTrackerConfig: () => Effect.die("unused"),
      setTrackerConfig: () => Effect.die("unused"),
      clearTrackerConfig: () => Effect.die("unused"),
      getObserveState: ({ projectId }) =>
        githubStates.get(projectId).pipe(
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new AgentControlGithubRpcError({
                    code: "tracker-not-configured",
                    operation: "get-observe-state",
                    projectId,
                  }),
                ),
              onSome: Effect.succeed,
            }),
          ),
          Effect.mapError((error) =>
            isGithubRpcError(error)
              ? error
              : new AgentControlGithubRpcError({
                  code: "internal-persistence-error",
                  operation: "get-observe-state",
                  projectId,
                }),
          ),
        ),
      listObservedIssues: () => Effect.die("unused"),
      pollOnce: (input) => {
        pollInputs.push(input);
        activePolls += 1;
        maximumActivePolls = Math.max(maximumActivePolls, activePolls);
        return pollImplementation(input).pipe(
          Effect.ensuring(Effect.sync(() => (activePolls -= 1))),
        );
      },
      streamDomainEvents: Stream.fromPubSub(githubEvents).pipe(
        Stream.mapEffect((event) =>
          persistGithubEvent(event).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
            Effect.orDie,
            Effect.as(event),
          ),
        ),
      ),
      subscribeDomainEvents: subscribe("github-intake", githubEvents, (stream) =>
        stream.pipe(
          Stream.mapEffect((event) =>
            persistGithubEvent(event).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
              Effect.orDie,
              Effect.as(event),
            ),
          ),
        ),
      ),
    });
    const controller = AgentControlEngine.of({
      getProjectState: ({ projectId }) =>
        projectStates.get(projectId).pipe(
          Effect.map(
            Option.getOrElse(() => ({
              schemaVersion: 1 as const,
              projectId,
              mode: "manual" as const,
              pausedFromMode: null,
              revision: 0,
              sequence: 0,
              updatedAt: null,
            })),
          ),
          Effect.orDie,
        ),
      dispatchHuman: () => Effect.die("unused"),
      dispatchController: () => Effect.die("unused"),
      dispatchSystem: () => Effect.die("unused"),
      streamDomainEvents: Stream.fromPubSub(projectEvents),
      subscribeDomainEvents: subscribe("project-controller", projectEvents),
    });
    const orchestration = OrchestrationEngineService.of({
      readEvents: () => Stream.empty,
      dispatch: () => Effect.succeed({ sequence: 0 }),
      dispatchClient: () => Effect.succeed({ sequence: 0 }),
      dispatchAgentControl: () => Effect.succeed({ sequence: 0 }),
      streamDomainEvents: Stream.fromPubSub(orchestrationEvents),
      subscribeDomainEvents: subscribe("project-delete", orchestrationEvents),
      latestSequence: Effect.succeed(0),
    });
    const reactor = yield* make({
      jitterMillis: () => 0,
      ...options?.reactorOptions,
    }).pipe(
      Effect.provideService(AgentControlEngine, controller),
      Effect.provideService(AgentControlGithubIntake, intake),
      Effect.provideService(OrchestrationEngineService, orchestration),
      Effect.provideService(AgentControlProjectStateRepository, reactorProjectStates),
      Effect.provideService(
        AgentControlGithubSchedulerStateRepository,
        options?.schedulerStates?.(baseSchedulerStates) ?? baseSchedulerStates,
      ),
      Effect.provideService(
        AgentControlProjectAvailability,
        options?.availability?.(baseAvailability) ?? baseAvailability,
      ),
    );

    return {
      reactor,
      projectEvents,
      githubEvents,
      orchestrationEvents,
      pollInputs,
      activePolls: () => activePolls,
      maxActivePolls: () => maximumActivePolls,
      setPoll: (implementation) => {
        pollImplementation = implementation;
      },
      subscriptionStarts: (name) => subscriptionStartCounts[name],
      subscriptionReleases: (name) => subscriptionReleaseCounts[name],
      activeSubscriptions: (name) => activeSubscriptionCounts[name],
      maxActiveSubscriptions: (name) => maximumActiveSubscriptionCounts[name],
    } satisfies Harness;
  });

const flush = Effect.gen(function* () {
  yield* TestClock.adjust(Duration.millis(1));
  for (let index = 0; index < 8; index += 1) yield* Effect.yieldNow;
});

const waitFor = Effect.fn("test.waitFor")(function* (predicate: () => boolean) {
  for (let index = 0; index < 200; index += 1) {
    if (predicate()) return;
    yield* Effect.yieldNow;
  }
  assert.fail("condition was not observed");
});

const waitForEffect = <E, R>(predicate: () => Effect.Effect<boolean, E, R>) =>
  Effect.gen(function* () {
    for (let index = 0; index < 200; index += 1) {
      if (yield* predicate()) return;
      yield* Effect.yieldNow;
    }
    assert.fail("effectful condition was not observed");
  });

const run = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.scoped(effect).pipe(Effect.provide(testLayer));

it.effect(
  "starts exactly one worker for each healthy Observe project and skips manual, paused, missing, and corrupt projects",
  () =>
    run(
      Effect.gen(function* () {
        const observeOne = ProjectId.make("observe-one");
        const observeTwo = ProjectId.make("observe-two");
        const manual = ProjectId.make("manual");
        const paused = ProjectId.make("paused");
        const missingConfig = ProjectId.make("missing-config");
        const corrupt = ProjectId.make("corrupt");
        yield* addProject({ projectId: observeOne, mode: "observe" });
        yield* addProject({ projectId: observeTwo, mode: "observe" });
        yield* addProject({ projectId: manual, mode: "manual" });
        yield* addProject({ projectId: paused, mode: "paused" });
        yield* addProject({ projectId: missingConfig, mode: "observe", withConfig: false });
        yield* addProject({
          projectId: corrupt,
          mode: "observe",
          corruptControllerProjection: true,
        });
        const harness = yield* makeHarness();
        yield* harness.reactor.start();
        yield* flush;

        assert.deepStrictEqual(harness.pollInputs.map(({ projectId }) => projectId).toSorted(), [
          observeOne,
          observeTwo,
        ]);
        assert.equal(
          (yield* harness.reactor.getStatus({ projectId: manual })).activity,
          "inactive",
        );
        assert.equal(
          (yield* harness.reactor.getStatus({ projectId: paused })).activity,
          "inactive",
        );
        assert.equal(
          (yield* harness.reactor.getStatus({ projectId: missingConfig })).activity,
          "inactive",
        );
      }),
    ),
);

it.effect(
  "subscribes before startup enumeration and reconciles an in-flight config change once",
  () =>
    run(
      Effect.gen(function* () {
        const projectId = ProjectId.make("startup-subscription-race");
        yield* addProject({ projectId, mode: "observe", withConfig: false });
        const harness = yield* makeHarness({
          duringEnumeration: ({ githubEvents }) =>
            Effect.gen(function* () {
              yield* setGithubState(projectId, 15);
              yield* PubSub.publish(githubEvents, githubConfigEvent(projectId, 15, 2));
            }).pipe(Effect.orDie),
        });

        yield* harness.reactor.start();
        yield* flush;

        assert.equal(harness.pollInputs.length, 1);
        assert.equal(harness.pollInputs[0]?.projectId, projectId);
      }),
    ),
);

it.effect("retries a transient startup enumeration failure before becoming started", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-enumeration-retry");
      yield* addProject({ projectId, mode: "observe" });
      let attempts = 0;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            attempts += 1;
            return attempts === 1
              ? Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.startup-enumeration",
                  }),
                )
              : base;
          }),
        reactorOptions: { startupMaxAttempts: 2, startupRetryBaseMs: 1 },
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* waitFor(() => attempts === 1);
      assert.isUndefined(startFiber.pollUnsafe());
      yield* TestClock.adjust(Duration.millis(1));
      yield* Fiber.join(startFiber);

      assert.equal(attempts, 2);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
    }),
  ),
);

it.effect("fails closed after exhausted startup retries and permits a later clean start", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-enumeration-fails");
      yield* addProject({ projectId, mode: "observe" });
      let available = false;
      let attempts = 0;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            attempts += 1;
            return available
              ? base
              : Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.startup-enumeration",
                  }),
                );
          }),
        reactorOptions: { startupMaxAttempts: 2, startupRetryBaseMs: 1 },
      });

      const failedStart = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* waitFor(() => attempts === 1);
      yield* TestClock.adjust(Duration.millis(1));
      const error = yield* Effect.flip(Fiber.join(failedStart));
      assert.equal(error._tag, "AgentControlGithubObserveStartupError");
      assert.equal(error.reason, "enumeration-failed");
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "degraded");

      available = true;
      yield* harness.reactor.start();
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
    }),
  ),
);

it.effect("waits for real queue acknowledgement before completing start", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-queue-ack");
      yield* addProject({ projectId, mode: "observe" });
      const saveEntered = yield* Deferred.make<void>();
      const releaseSave = yield* Deferred.make<void>();
      let gateFirstSave = true;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (state, expectedRevision) =>
              gateFirstSave
                ? Effect.gen(function* () {
                    gateFirstSave = false;
                    yield* Deferred.succeed(saveEntered, undefined);
                    yield* Deferred.await(releaseSave);
                    return yield* base.save(state, expectedRevision);
                  })
                : base.save(state, expectedRevision),
          }),
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* Deferred.await(saveEntered);
      assert.isUndefined(startFiber.pollUnsafe());
      yield* Deferred.succeed(releaseSave, undefined);
      yield* Fiber.join(startFiber);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
    }),
  ),
);

it.effect("waits for explicit hot-subscription acquisition", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-subscription-ack");
      yield* addProject({ projectId, mode: "observe" });
      const releaseSubscription = yield* Deferred.make<void>();
      let enumerations = 0;
      const harness = yield* makeHarness({
        subscriptionGate: {
          name: "project-controller",
          await: Deferred.await(releaseSubscription),
        },
        listPersisted: (base) =>
          Effect.sync(() => {
            enumerations += 1;
          }).pipe(Effect.andThen(base)),
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* waitFor(() => harness.subscriptionStarts("project-controller") === 1);
      assert.equal(enumerations, 0);
      assert.isUndefined(startFiber.pollUnsafe());
      yield* Deferred.succeed(releaseSubscription, undefined);
      yield* Fiber.join(startFiber);
      assert.equal(enumerations, 1);
    }),
  ),
);

it.effect("releases every subscriber after exhausted acquisition retries", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-subscription-acquisition-failure");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness({
        failSubscriptionAcquisitionAttempts: {
          name: "project-controller",
          count: 2,
        },
        reactorOptions: {
          startupMaxAttempts: 2,
          subscriptionRetryBaseMs: 1,
          subscriptionRetryMaxMs: 1,
        },
      });

      const failedStart = yield* harness.reactor.start().pipe(Effect.forkChild);
      yield* waitFor(() => harness.subscriptionStarts("project-controller") === 1);
      yield* TestClock.adjust(Duration.millis(1));
      const exit = yield* Fiber.await(failedStart);
      assert.isTrue(Exit.isFailure(exit));
      yield* waitFor(() =>
        (["project-controller", "github-intake", "project-delete"] as const).every(
          (name) => harness.activeSubscriptions(name) === 0,
        ),
      );
      assert.equal(harness.subscriptionReleases("project-controller"), 2);

      yield* harness.reactor.start();
      assert.equal(harness.subscriptionStarts("project-controller"), 3);
      assert.equal(harness.activeSubscriptions("project-controller"), 1);
      assert.equal(harness.maxActiveSubscriptions("project-controller"), 1);
    }),
  ),
);

it.effect("interrupts a blocked subscription start without stranding concurrent callers", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-subscription-interrupt");
      yield* addProject({ projectId, mode: "observe" });
      const releaseSubscription = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        subscriptionGate: {
          name: "project-controller",
          await: Deferred.await(releaseSubscription),
        },
      });
      const ownerScope = yield* Scope.make("sequential");
      const initiator = yield* harness.reactor
        .start()
        .pipe(Scope.provide(ownerScope), Effect.forkChild);
      yield* waitFor(() => harness.activeSubscriptions("project-controller") === 1);
      const waiter = yield* harness.reactor.start().pipe(Effect.forkChild);
      for (let index = 0; index < 8; index += 1) yield* Effect.yieldNow;

      yield* Fiber.interrupt(initiator);
      const waiterExit = yield* Fiber.await(waiter);
      assert.isTrue(Exit.isFailure(waiterExit));
      if (Exit.isFailure(waiterExit)) assert.isTrue(Cause.hasInterruptsOnly(waiterExit.cause));
      yield* waitFor(() =>
        (["project-controller", "github-intake", "project-delete"] as const).every(
          (name) => harness.activeSubscriptions(name) === 0,
        ),
      );
      assert.equal(harness.subscriptionReleases("project-controller"), 1);

      yield* Deferred.succeed(releaseSubscription, undefined);
      const retryScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(retryScope));
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
      assert.equal(harness.activeSubscriptions("project-controller"), 1);
      yield* Scope.close(retryScope, Exit.void);
      yield* Scope.close(ownerScope, Exit.void);
    }),
  ),
);

it.effect("interrupts startup enumeration and permits one clean retry", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-enumeration-interrupt");
      yield* addProject({ projectId, mode: "observe" });
      const enumerationEntered = yield* Deferred.make<void>();
      let blockFirstEnumeration = true;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            if (!blockFirstEnumeration) return base;
            blockFirstEnumeration = false;
            return Deferred.succeed(enumerationEntered, undefined).pipe(
              Effect.andThen(Effect.never),
            );
          }),
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkChild);
      yield* Deferred.await(enumerationEntered);
      yield* Fiber.interrupt(startFiber);
      yield* waitFor(() =>
        (["project-controller", "github-intake", "project-delete"] as const).every(
          (name) => harness.activeSubscriptions(name) === 0,
        ),
      );

      yield* harness.reactor.start();
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.activeSubscriptions(name), 1);
        assert.equal(harness.maxActiveSubscriptions(name), 1);
      }
    }),
  ),
);

it.effect("interrupts an initial queue reconcile and drains the failed attempt", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-queue-interrupt");
      yield* addProject({ projectId, mode: "observe" });
      const saveEntered = yield* Deferred.make<void>();
      let blockFirstSave = true;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (state, expectedRevision) =>
              Effect.suspend(() => {
                if (!blockFirstSave) return base.save(state, expectedRevision);
                blockFirstSave = false;
                return Deferred.succeed(saveEntered, undefined).pipe(Effect.andThen(Effect.never));
              }),
          }),
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkChild);
      yield* Deferred.await(saveEntered);
      yield* Fiber.interrupt(startFiber);
      yield* waitFor(() =>
        (["project-controller", "github-intake", "project-delete"] as const).every(
          (name) => harness.activeSubscriptions(name) === 0,
        ),
      );

      yield* harness.reactor.start();
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);
    }),
  ),
);

it.effect("starts exactly one replacement runtime after successful shutdown", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-shutdown-restart");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      const firstScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(firstScope));
      yield* Scope.close(firstScope, Exit.void);
      yield* waitFor(() =>
        (["project-controller", "github-intake", "project-delete"] as const).every(
          (name) => harness.activeSubscriptions(name) === 0,
        ),
      );

      const secondScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(secondScope));
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.subscriptionStarts(name), 2);
        assert.equal(harness.activeSubscriptions(name), 1);
        assert.equal(harness.maxActiveSubscriptions(name), 1);
      }
      yield* Scope.close(secondScope, Exit.void);
    }),
  ),
);

it.effect("cleans a defective runtime finalizer and acquires a fresh runtime", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-finalizer-defect");
      const replacementProject = ProjectId.make("startup-finalizer-defect-replacement");
      yield* addProject({ projectId, mode: "observe" });
      let runtimeAcquires = 0;
      let runtimeReleases = 0;
      let activeRuntimeResources = 0;
      const firstPollEntered = yield* Deferred.make<void>();
      const replacementPollEntered = yield* Deferred.make<void>();
      const harness = yield* makeHarness({
        reactorOptions: {
          testHooks: {
            acquireRuntimeResource: Effect.acquireRelease(
              Effect.sync(() => {
                runtimeAcquires += 1;
                activeRuntimeResources += 1;
              }),
              () =>
                Effect.sync(() => {
                  runtimeReleases += 1;
                  activeRuntimeResources -= 1;
                  return runtimeReleases;
                }).pipe(
                  Effect.flatMap((release) =>
                    release === 1 ? Effect.die("github-runtime-finalizer-defect") : Effect.void,
                  ),
                ),
            ),
          },
        },
      });
      harness.setPoll(({ projectId: polledProject }) =>
        Deferred.succeed(
          polledProject === replacementProject ? replacementPollEntered : firstPollEntered,
          undefined,
        ).pipe(Effect.andThen(Effect.never)),
      );
      const ownerA = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(ownerA));
      yield* Deferred.await(firstPollEntered);
      assert.equal(harness.activePolls(), 1);

      const ownerAClose = yield* Effect.exit(Scope.close(ownerA, Exit.void));
      assert.isTrue(Exit.isFailure(ownerAClose));
      if (Exit.isFailure(ownerAClose)) assert.isTrue(Cause.hasDies(ownerAClose.cause));
      assert.equal(harness.activePolls(), 0);
      assert.equal(runtimeAcquires, 1);
      assert.equal(runtimeReleases, 1);
      assert.equal(activeRuntimeResources, 0);
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.activeSubscriptions(name), 0);
        assert.equal(harness.subscriptionReleases(name), 1);
      }

      yield* addProject({ projectId: replacementProject, mode: "observe" });
      const ownerB = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(ownerB));
      yield* Deferred.await(replacementPollEntered);
      assert.equal(harness.pollInputs.length, 2);
      assert.equal(harness.activePolls(), 1);
      assert.equal(runtimeAcquires, 2);
      assert.equal(activeRuntimeResources, 1);
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.subscriptionStarts(name), 2);
        assert.equal(harness.activeSubscriptions(name), 1);
        assert.equal(harness.maxActiveSubscriptions(name), 1);
      }

      const ownerBClose = yield* Effect.exit(Scope.close(ownerB, Exit.void));
      assert.isTrue(Exit.isSuccess(ownerBClose));
      assert.equal(harness.activePolls(), 0);
      assert.equal(runtimeReleases, 2);
      assert.equal(activeRuntimeResources, 0);
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.activeSubscriptions(name), 0);
        assert.equal(harness.subscriptionReleases(name), 2);
      }
    }),
  ),
);

it.effect("reloads after CAS conflict without overwriting or stopping a newer generation", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("startup-cas-interleave");
      yield* addProject({ projectId, mode: "observe" });
      let interleave = true;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (candidate, expectedRevision) =>
              interleave
                ? Effect.gen(function* () {
                    interleave = false;
                    yield* base.save(
                      {
                        ...candidate,
                        generation: candidate.generation + 1,
                      },
                      expectedRevision,
                    );
                    return yield* base.save(candidate, expectedRevision);
                  })
                : base.save(candidate, expectedRevision),
          }),
        reactorOptions: { startupMaxAttempts: 2, startupRetryBaseMs: 1 },
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* waitFor(() => !interleave);
      yield* TestClock.adjust(Duration.millis(1));
      yield* Fiber.join(startFiber);

      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      const persisted = Option.getOrThrow(yield* scheduler.get(projectId));
      assert.equal(persisted.generation, 2);
      assert.equal(persisted.schedulerRevision, 1);
      const status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.health, "healthy");
      assert.notEqual(status.workerStatus, "missing");
    }),
  ),
);

it.effect("isolates one project's poll defect from every other worker", () =>
  run(
    Effect.gen(function* () {
      const failingProject = ProjectId.make("isolated-failing-project");
      const healthyProject = ProjectId.make("isolated-healthy-project");
      yield* addProject({ projectId: failingProject, mode: "observe" });
      yield* addProject({ projectId: healthyProject, mode: "observe" });
      const harness = yield* makeHarness();
      let healthySequence = 2;
      harness.setPoll((input) =>
        input.projectId === failingProject
          ? Effect.die("isolated poll defect")
          : Effect.gen(function* () {
              const at = DateTime.formatIso(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));
              yield* PubSub.publish(
                harness.githubEvents,
                githubPollEvent(healthyProject, {
                  type: "success",
                  sequence: healthySequence++,
                  at,
                }),
              );
              return {
                state: intakeState(healthyProject),
                resultSequence: healthySequence,
                eventCreated: true,
              };
            }),
      );

      yield* harness.reactor.start();
      yield* flush;
      assert.equal(
        harness.pollInputs.filter(({ projectId }) => projectId === healthyProject).length,
        1,
      );

      yield* TestClock.adjust(Duration.seconds(15));
      yield* waitFor(
        () =>
          harness.pollInputs.filter(({ projectId }) => projectId === healthyProject).length === 2,
      );
      assert.equal(
        (yield* harness.reactor.getStatus({ projectId: failingProject })).consecutiveFailures,
        0,
      );
    }),
  ),
);

it.effect("reacts idempotently to mode and tracker configuration changes", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("mode-config-transitions");
      yield* addProject({ projectId, mode: "manual" });
      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      yield* flush;
      assert.equal(harness.pollInputs.length, 0);

      yield* updateMode(projectId, "observe");
      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "manual", "observe", 2));
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);

      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "manual", "observe", 2));
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);

      yield* updateMode(projectId, "manual");
      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "observe", "manual", 3));
      yield* flush;
      assert.equal((yield* harness.reactor.getStatus({ projectId })).activity, "inactive");

      yield* updateMode(projectId, "observe");
      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "manual", "observe", 4));
      yield* flush;
      assert.equal(harness.pollInputs.length, 2);

      yield* updateMode(projectId, "paused");
      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "observe", "paused", 5));
      yield* flush;
      assert.equal((yield* harness.reactor.getStatus({ projectId })).activity, "inactive");

      yield* updateMode(projectId, "observe");
      yield* PubSub.publish(harness.projectEvents, projectEvent(projectId, "paused", "observe", 6));
      yield* flush;
      assert.equal(harness.pollInputs.length, 3);

      yield* setGithubState(projectId, 30);
      yield* PubSub.publish(harness.githubEvents, githubConfigEvent(projectId, 30, 2));
      yield* flush;
      assert.equal(harness.pollInputs.length, 4);
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      const changed = yield* scheduler.get(projectId);
      assert.equal(Option.getOrThrow(changed).generation, 2);
      assert.equal(Option.getOrThrow(changed).pollIntervalSeconds, 30);

      yield* PubSub.publish(harness.githubEvents, githubConfigEvent(projectId, 30, 2));
      yield* flush;
      assert.equal(harness.pollInputs.length, 4);

      yield* PubSub.publish(harness.githubEvents, githubClearEvent(projectId, 3));
      yield* flush;
      assert.equal((yield* harness.reactor.getStatus({ projectId })).activity, "inactive");
    }),
  ),
);

it.effect("a stale timer cannot displace the worker for a newer scheduler token", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("stale-timer-token");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness({
        reactorOptions: { jitterMillis: () => 10 },
      });
      yield* harness.reactor.start();
      yield* TestClock.adjust(Duration.millis(1));

      yield* setGithubState(projectId, 30, 2);
      yield* PubSub.publish(harness.githubEvents, githubConfigEvent(projectId, 30, 2));
      yield* waitForEffect(() =>
        AgentControlGithubSchedulerStateRepository.pipe(
          Effect.flatMap((repository) => repository.get(projectId)),
          Effect.map((persisted) =>
            Option.isSome(persisted) ? persisted.value.generation === 2 : false,
          ),
        ),
      );

      yield* TestClock.adjust(Duration.millis(9));
      yield* Effect.yieldNow;
      assert.equal(harness.pollInputs.length, 0);
      yield* TestClock.adjust(Duration.millis(1));
      yield* waitFor(() => harness.pollInputs.length === 1);
      assert.equal(harness.pollInputs[0]?.projectId, projectId);
      const status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.workerStatus, "polling");
    }),
  ),
);

it.effect("schedules only after completion and never overlaps polls", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("non-overlapping");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      let sequence = 2;
      harness.setPoll((input) =>
        Effect.gen(function* () {
          const at = DateTime.formatIso(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));
          yield* PubSub.publish(
            harness.githubEvents,
            githubPollEvent(input.projectId, {
              type: "success",
              sequence: sequence++,
              at,
            }),
          );
          return {
            state: intakeState(input.projectId),
            resultSequence: sequence,
            eventCreated: true,
          };
        }),
      );
      yield* harness.reactor.start();
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);
      yield* setGithubState(projectId, 15, 7);

      yield* TestClock.adjust(Duration.seconds(14));
      yield* Effect.yieldNow;
      assert.equal(harness.pollInputs.length, 1);
      yield* TestClock.adjust(Duration.seconds(1));
      yield* waitFor(() => harness.pollInputs.length === 2);
      assert.equal(harness.maxActivePolls(), 1);
      assert.notEqual(harness.pollInputs[0]?.commandId, harness.pollInputs[1]?.commandId);
      assert.equal(harness.pollInputs[1]?.expectedRevision, 7);
      assert.match(String(harness.pollInputs[0]?.commandId), /^server:github-observe:/);
    }),
  ),
);

it.effect("persists exponential backoff, opens one half-open probe, and caps authentication", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("backoff-circuit");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);

      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      const expectedDelays = [15, 30, 60, 120, 900];
      for (let index = 0; index < expectedDelays.length; index += 1) {
        const event = githubPollEvent(projectId, {
          type: "failure",
          sequence: index + 2,
          code: "github-timeout",
        });
        yield* PubSub.publish(harness.githubEvents, event);
        yield* waitForEffect(() =>
          scheduler
            .get(projectId)
            .pipe(
              Effect.map((row) =>
                Option.isSome(row) ? row.value.consecutiveFailures === index + 1 : false,
              ),
            ),
        );
        if (index === 0) {
          yield* PubSub.publish(harness.githubEvents, event);
          yield* Effect.yieldNow;
          assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).consecutiveFailures, 1);
        }
        const state = Option.getOrThrow(yield* scheduler.get(projectId));
        assert.equal(
          Date.parse(state.nextAttemptAt!) - Date.parse(state.updatedAt),
          expectedDelays[index]! * 1_000,
        );
      }
      const opened = Option.getOrThrow(yield* scheduler.get(projectId));
      assert.equal(opened.circuitState, "open");
      assert.equal(opened.consecutiveFailures, 5);

      yield* TestClock.adjust(Duration.minutes(15));
      yield* waitFor(() => harness.pollInputs.length === 2);
      assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).circuitState, "half-open");
      yield* TestClock.adjust(Duration.minutes(30));
      yield* Effect.yieldNow;
      assert.equal(harness.pollInputs.length, 2);

      assert.equal(
        githubObserveBackoffMs(15, 1, "github-authentication"),
        AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS,
      );
      assert.equal(AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS, AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS);
    }),
  ),
);

it.effect("suspends hard failures and a manual success reactivates Observe", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("hard-suspension");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      yield* flush;

      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "repository-identity-changed",
        }),
      );
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.activity === "suspended")),
      );
      const suspended = yield* harness.reactor.getStatus({ projectId });
      assert.equal(suspended.circuitState, "open");
      assert.equal(suspended.reasonCode, "repository-identity-changed");

      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, { type: "success", sequence: 3 }),
      );
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.activity === "active")),
      );
      const recovered = yield* harness.reactor.getStatus({ projectId });
      assert.equal(recovered.circuitState, "closed");
      assert.equal(recovered.consecutiveFailures, 0);
      assert.equal(recovered.reasonCode, null);
    }),
  ),
);

it.effect("a tracker configuration change resets a suspended circuit generation", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("config-resets-suspension");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      yield* flush;

      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "timeline-incomplete",
        }),
      );
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.activity === "suspended")),
      );

      yield* setGithubState(projectId, 30);
      yield* PubSub.publish(harness.githubEvents, githubConfigEvent(projectId, 30, 3));
      yield* flush;
      const status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.activity, "active");
      assert.equal(status.circuitState, "closed");
      assert.equal(status.consecutiveFailures, 0);
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).generation, 2);
      assert.equal(harness.pollInputs.length, 2);
    }),
  ),
);

it.effect("retains circuit cooldown across a simulated server restart", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("restart-circuit");
      yield* addProject({ projectId, mode: "observe" });
      const first = yield* makeHarness();
      const firstScope = yield* Scope.make("sequential");
      yield* first.reactor.start().pipe(Scope.provide(firstScope));
      yield* flush;
      for (let index = 0; index < 5; index += 1) {
        yield* PubSub.publish(
          first.githubEvents,
          githubPollEvent(projectId, {
            type: "failure",
            sequence: index + 2,
            code: "github-unavailable",
          }),
        );
        yield* Effect.yieldNow;
      }
      yield* waitForEffect(() =>
        first.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.circuitState === "open")),
      );
      yield* Scope.close(firstScope, Exit.void);

      const second = yield* makeHarness();
      const secondScope = yield* Scope.make("sequential");
      yield* second.reactor.start().pipe(Scope.provide(secondScope));
      yield* TestClock.adjust(Duration.minutes(14));
      yield* Effect.yieldNow;
      assert.equal(second.pollInputs.length, 0);
      yield* TestClock.adjust(Duration.minutes(1));
      yield* waitFor(() => second.pollInputs.length === 1);
      assert.equal((yield* second.reactor.getStatus({ projectId })).circuitState, "half-open");
      yield* Scope.close(secondScope, Exit.void);
    }),
  ),
);

it.effect("recovers a committed poll failure missed immediately before restart", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("restart-commit-window");
      yield* addProject({ projectId, mode: "observe" });
      const first = yield* makeHarness();
      const firstScope = yield* Scope.make("sequential");
      yield* first.reactor.start().pipe(Scope.provide(firstScope));
      yield* flush;
      assert.equal(first.pollInputs.length, 1);

      // Simulate the intake commit landing after the poll attempt was persisted
      // but before its hot event reached the reactor.
      yield* setGithubFailureState(projectId, 2, "github-timeout");
      yield* Scope.close(firstScope, Exit.void);

      const second = yield* makeHarness();
      const secondScope = yield* Scope.make("sequential");
      yield* second.reactor.start().pipe(Scope.provide(secondScope));
      yield* flush;
      yield* waitForEffect(() =>
        second.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.consecutiveFailures === 1)),
      );
      const recovered = yield* second.reactor.getStatus({ projectId });
      assert.equal(recovered.reasonCode, "github-timeout");
      assert.equal(recovered.circuitState, "closed");
      assert.equal(second.pollInputs.length, 0);

      yield* TestClock.adjust(Duration.seconds(15));
      yield* waitFor(() => second.pollInputs.length === 1);
      yield* Scope.close(secondScope, Exit.void);
    }),
  ),
);

it.effect("replays every committed outcome over multiple pages without a scheduler row", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("full-history-pagination");
      yield* addProject({ projectId, mode: "observe" });
      for (let sequence = 2; sequence <= 6; sequence += 1) {
        yield* persistGithubEvent(
          githubPollEvent(projectId, {
            type: "failure",
            sequence,
            code: "github-timeout",
          }),
        );
      }
      const harness = yield* makeHarness({ reactorOptions: { replayPageSize: 2 } });
      yield* harness.reactor.start();

      const status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.consecutiveFailures, 5);
      assert.equal(status.circuitState, "open");
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      const persisted = Option.getOrThrow(yield* scheduler.get(projectId));
      assert.equal(persisted.lastGithubEventSequence, 6);
      assert.equal(persisted.schedulerRevision, 6);
    }),
  ),
);

it.effect("reconstructs historical hard suspension when no scheduler row exists", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("full-history-hard-suspension");
      yield* addProject({ projectId, mode: "observe" });
      yield* persistGithubEvent(
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "timeline-incomplete",
        }),
      );
      const harness = yield* makeHarness();
      yield* harness.reactor.start();

      const status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.activity, "suspended");
      assert.equal(status.workerStatus, "stopped");
      assert.equal(status.reasonCode, "timeline-incomplete");
    }),
  ),
);

it.effect("resumes replay from the last confirmed cursor after a partial save failure", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("partial-replay-crash");
      yield* addProject({ projectId, mode: "observe" });
      for (let sequence = 2; sequence <= 6; sequence += 1) {
        yield* persistGithubEvent(
          githubPollEvent(projectId, {
            type: "failure",
            sequence,
            code: "github-timeout",
          }),
        );
      }
      let saves = 0;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (state, expectedRevision) =>
              Effect.suspend(() => {
                saves += 1;
                return saves === 3
                  ? Effect.fail(
                      new AgentControlPersistenceSqlError({
                        operation: "test.partial-replay",
                      }),
                    )
                  : base.save(state, expectedRevision);
              }),
          }),
        reactorOptions: { startupMaxAttempts: 2, startupRetryBaseMs: 1 },
      });

      const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
      yield* waitFor(() => saves === 3);
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).lastGithubEventSequence, 2);
      yield* TestClock.adjust(Duration.millis(1));
      yield* Fiber.join(startFiber);
      const persisted = Option.getOrThrow(yield* scheduler.get(projectId));
      assert.equal(persisted.lastGithubEventSequence, 6);
      assert.equal(persisted.consecutiveFailures, 5);
    }),
  ),
);

it.effect("folds config clear and reconfiguration as a new generation", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("history-config-boundaries");
      yield* addProject({ projectId, mode: "observe" });
      yield* persistGithubEvent(
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "timeline-incomplete",
        }),
      );
      yield* persistGithubEvent(githubClearEvent(projectId, 3));
      yield* setGithubState(projectId, 30, 4);
      yield* persistGithubEvent(githubConfigEvent(projectId, 30, 4));

      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      const persisted = Option.getOrThrow(yield* scheduler.get(projectId));
      assert.equal(persisted.generation, 2);
      assert.equal(persisted.activity, "active");
      assert.equal(persisted.pollIntervalSeconds, 30);
      assert.equal(persisted.consecutiveFailures, 0);
    }),
  ),
);

it.effect("recovers a transient scheduler save failure without dropping the worker", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("transient-scheduler-save");
      yield* addProject({ projectId, mode: "observe" });
      let failNextSave = false;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (state, expectedRevision) =>
              Effect.suspend(() => {
                if (!failNextSave) return base.save(state, expectedRevision);
                failNextSave = false;
                return Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.transient-scheduler-save",
                  }),
                );
              }),
          }),
        reactorOptions: { recoveryRetryBaseMs: 1, recoveryRetryMaxMs: 1 },
      });
      yield* harness.reactor.start();
      failNextSave = true;
      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "github-timeout",
        }),
      );
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "recovering")),
      );

      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      assert.isTrue(Option.isSome(yield* scheduler.get(projectId)));
      yield* TestClock.adjust(Duration.millis(1));
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(
            Effect.map((status) => status.health === "healthy" && status.consecutiveFailures === 1),
          ),
      );
    }),
  ),
);

it.effect("recovers a transient scheduler read failure and preserves persisted state", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("transient-scheduler-get");
      yield* addProject({ projectId, mode: "observe" });
      let failNextGet = false;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            get: (candidate) =>
              Effect.suspend(() => {
                if (!failNextGet) return base.get(candidate);
                failNextGet = false;
                return Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.transient-scheduler-get",
                  }),
                );
              }),
          }),
        reactorOptions: { recoveryRetryBaseMs: 1, recoveryRetryMaxMs: 1 },
      });
      yield* harness.reactor.start();
      failNextGet = true;
      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "github-timeout",
        }),
      );
      yield* waitFor(() => !failNextGet);
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "recovering")),
      );
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).lastGithubEventSequence, 1);

      yield* TestClock.adjust(Duration.millis(1));
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.consecutiveFailures === 1)),
      );
    }),
  ),
);

it.effect(
  "watchdog repairs persisted active state after a transient timer availability error",
  () =>
    run(
      Effect.gen(function* () {
        const projectId = ProjectId.make("watchdog-worker-repair");
        yield* addProject({ projectId, mode: "observe" });
        let availabilityCalls = 0;
        const harness = yield* makeHarness({
          availability: (base) =>
            AgentControlProjectAvailability.of({
              ensureAvailable: (candidate) =>
                Effect.suspend(() => {
                  availabilityCalls += 1;
                  return availabilityCalls === 2
                    ? Effect.fail(
                        new AgentControlPersistenceSqlError({
                          operation: "test.transient-availability",
                        }),
                      )
                    : base.ensureAvailable(candidate);
                }),
            }),
          reactorOptions: {
            watchdogIntervalMs: 1,
            recoveryRetryBaseMs: 1_000,
            recoveryRetryMaxMs: 1_000,
          },
        });
        yield* harness.reactor.start();
        yield* waitFor(() => availabilityCalls >= 2);
        const recovering = yield* harness.reactor.getStatus({ projectId });
        assert.equal(recovering.activity, "active");
        assert.equal(recovering.workerStatus, "missing");
        assert.equal(recovering.health, "recovering");
        const scheduler = yield* AgentControlGithubSchedulerStateRepository;
        assert.isTrue(Option.isSome(yield* scheduler.get(projectId)));

        yield* TestClock.adjust(Duration.millis(1));
        yield* waitFor(() => harness.pollInputs.length === 1);
        const repaired = yield* harness.reactor.getStatus({ projectId });
        assert.equal(repaired.health, "healthy");
        assert.equal(repaired.workerStatus, "polling");
      }),
    ),
);

it.effect("keeps full-reconcile status recovering until its queue barrier is acknowledged", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("watchdog-barrier-status");
      yield* addProject({ projectId, mode: "observe" });
      const watchdogEntered = yield* Deferred.make<void>();
      const releaseWatchdog = yield* Deferred.make<void>();
      let enumerations = 0;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            return enumerations === 2
              ? Deferred.succeed(watchdogEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseWatchdog)),
                  Effect.andThen(base),
                )
              : base;
          }),
        reactorOptions: { watchdogIntervalMs: 1 },
      });
      yield* harness.reactor.start();
      yield* TestClock.adjust(Duration.millis(1));
      yield* Deferred.await(watchdogEntered);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");

      yield* Deferred.succeed(releaseWatchdog, undefined);
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "healthy")),
      );
    }),
  ),
);

it.effect(
  "runs a follow-up pass for a committed event missed during an active full reconcile",
  () =>
    run(
      Effect.gen(function* () {
        const projectId = ProjectId.make("follow-up-missed-event-a");
        const blockerId = ProjectId.make("follow-up-missed-event-z");
        yield* addProject({ projectId, mode: "observe" });
        yield* addProject({ projectId: blockerId, mode: "observe" });
        const firstPassBlocked = yield* Deferred.make<void>();
        const releaseFirstPass = yield* Deferred.make<void>();
        const followUpEntered = yield* Deferred.make<void>();
        const releaseFollowUp = yield* Deferred.make<void>();
        const faultSubscription = yield* Deferred.make<void>();
        let enumerations = 0;
        let blockProject = false;
        const harness = yield* makeHarness({
          listPersisted: (base) =>
            Effect.suspend(() => {
              enumerations += 1;
              if (enumerations === 2) {
                return base.pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      blockProject = true;
                    }),
                  ),
                );
              }
              if (enumerations === 3) {
                return Deferred.succeed(followUpEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseFollowUp)),
                  Effect.andThen(base),
                );
              }
              return base;
            }),
          schedulerStates: (base) =>
            AgentControlGithubSchedulerStateRepository.of({
              ...base,
              get: (candidate) =>
                Effect.suspend(() => {
                  if (candidate !== blockerId || !blockProject) return base.get(candidate);
                  blockProject = false;
                  return Deferred.succeed(firstPassBlocked, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseFirstPass)),
                    Effect.andThen(base.get(candidate)),
                  );
                }),
            }),
          faultSubscriptionOnSignal: {
            name: "github-intake",
            await: Deferred.await(faultSubscription),
          },
          reactorOptions: {
            watchdogIntervalMs: 1_000,
            subscriptionRetryBaseMs: 1,
            subscriptionRetryMaxMs: 1,
          },
        });
        yield* harness.reactor.start();

        yield* TestClock.adjust(Duration.millis(1_000));
        yield* Deferred.await(firstPassBlocked);
        const scheduler = yield* AgentControlGithubSchedulerStateRepository;
        assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).lastGithubEventSequence, 1);

        yield* persistGithubEvent(
          githubPollEvent(projectId, {
            type: "failure",
            sequence: 2,
            code: "github-timeout",
          }),
        );
        yield* Deferred.succeed(faultSubscription, undefined);
        yield* waitFor(() => harness.subscriptionReleases("github-intake") === 1);
        yield* TestClock.adjust(Duration.millis(1));
        yield* waitFor(() => harness.subscriptionStarts("github-intake") === 2);
        assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");

        yield* Deferred.succeed(releaseFirstPass, undefined);
        yield* Deferred.await(followUpEntered);
        assert.equal(Option.getOrThrow(yield* scheduler.get(projectId)).lastGithubEventSequence, 1);
        assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");

        yield* Deferred.succeed(releaseFollowUp, undefined);
        yield* waitForEffect(() =>
          scheduler
            .get(projectId)
            .pipe(
              Effect.map(
                Option.exists(
                  (state) => state.lastGithubEventSequence > 1 && state.consecutiveFailures === 1,
                ),
              ),
            ),
        );
        yield* waitForEffect(() =>
          harness.reactor
            .getStatus({ projectId })
            .pipe(Effect.map((status) => status.health === "healthy")),
        );
        assert.equal(enumerations, 3);
      }),
    ),
);

it.effect("coalesces multiple requests during a running full reconcile into one follow-up", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("follow-up-coalesce-a");
      const blockerId = ProjectId.make("follow-up-coalesce-z");
      yield* addProject({ projectId, mode: "observe" });
      yield* addProject({ projectId: blockerId, mode: "observe" });
      const passBlocked = yield* Deferred.make<void>();
      const releasePass = yield* Deferred.make<void>();
      let enumerations = 0;
      let blockProject = false;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            return enumerations === 2
              ? base.pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      blockProject = true;
                    }),
                  ),
                )
              : base;
          }),
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            get: (candidate) =>
              Effect.suspend(() => {
                if (candidate !== blockerId || !blockProject) return base.get(candidate);
                blockProject = false;
                return Deferred.succeed(passBlocked, undefined).pipe(
                  Effect.andThen(Deferred.await(releasePass)),
                  Effect.andThen(base.get(candidate)),
                );
              }),
          }),
        reactorOptions: { watchdogIntervalMs: 1_000 },
      });
      yield* harness.reactor.start();

      yield* TestClock.adjust(Duration.millis(1_000));
      yield* Deferred.await(passBlocked);
      yield* TestClock.adjust(Duration.millis(1_000));
      yield* TestClock.adjust(Duration.millis(1_000));
      assert.equal(enumerations, 2);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");

      yield* Deferred.succeed(releasePass, undefined);
      yield* waitFor(() => enumerations === 3);
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "healthy")),
      );
      for (let index = 0; index < 8; index += 1) yield* Effect.yieldNow;
      assert.equal(enumerations, 3);
    }),
  ),
);

it.effect("retains requests queued behind project work and covers them with one later pass", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("queued-full-reconcile");
      yield* addProject({ projectId, mode: "observe" });
      const reconcileBlocked = yield* Deferred.make<void>();
      const releaseReconcile = yield* Deferred.make<void>();
      let blockSequenceTwo = true;
      let enumerations = 0;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            return base;
          }),
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            save: (state, expectedRevision) =>
              Effect.suspend(() => {
                if (state.lastGithubEventSequence !== 2 || !blockSequenceTwo) {
                  return base.save(state, expectedRevision);
                }
                blockSequenceTwo = false;
                return Deferred.succeed(reconcileBlocked, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseReconcile)),
                  Effect.andThen(base.save(state, expectedRevision)),
                );
              }),
          }),
        reactorOptions: { watchdogIntervalMs: 1_000 },
      });
      yield* harness.reactor.start();
      yield* PubSub.publish(
        harness.githubEvents,
        githubPollEvent(projectId, {
          type: "failure",
          sequence: 2,
          code: "github-timeout",
        }),
      );
      yield* Deferred.await(reconcileBlocked);

      yield* TestClock.adjust(Duration.millis(1_000));
      yield* TestClock.adjust(Duration.millis(1_000));
      assert.equal(enumerations, 1);
      yield* Deferred.succeed(releaseReconcile, undefined);

      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      yield* waitForEffect(() =>
        scheduler
          .get(projectId)
          .pipe(Effect.map(Option.exists((state) => state.lastGithubEventSequence === 2))),
      );
      yield* waitFor(() => enumerations === 2);
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "healthy")),
      );
      for (let index = 0; index < 8; index += 1) yield* Effect.yieldNow;
      assert.equal(enumerations, 2);
    }),
  ),
);

it.effect("keeps a failed follow-up epoch open until the global retry succeeds", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("follow-up-retry-a");
      const blockerId = ProjectId.make("follow-up-retry-z");
      yield* addProject({ projectId, mode: "observe" });
      yield* addProject({ projectId: blockerId, mode: "observe" });
      const passBlocked = yield* Deferred.make<void>();
      const releasePass = yield* Deferred.make<void>();
      let enumerations = 0;
      let blockProject = false;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            if (enumerations === 2) {
              return base.pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    blockProject = true;
                  }),
                ),
              );
            }
            return enumerations === 3
              ? Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.follow-up-enumeration",
                  }),
                )
              : base;
          }),
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            get: (candidate) =>
              Effect.suspend(() => {
                if (candidate !== blockerId || !blockProject) return base.get(candidate);
                blockProject = false;
                return Deferred.succeed(passBlocked, undefined).pipe(
                  Effect.andThen(Deferred.await(releasePass)),
                  Effect.andThen(base.get(candidate)),
                );
              }),
          }),
        reactorOptions: {
          watchdogIntervalMs: 1_000,
          recoveryRetryBaseMs: 1,
          recoveryRetryMaxMs: 1,
        },
      });
      yield* harness.reactor.start();

      yield* TestClock.adjust(Duration.millis(1_000));
      yield* Deferred.await(passBlocked);
      yield* TestClock.adjust(Duration.millis(1_000));
      yield* Deferred.succeed(releasePass, undefined);
      yield* waitFor(() => enumerations === 3);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");

      yield* TestClock.adjust(Duration.millis(1));
      yield* waitFor(() => enumerations === 4);
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "healthy")),
      );
      assert.equal(enumerations, 4);
    }),
  ),
);

it.effect("drops an open follow-up when its runtime shuts down and restarts cleanly", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("follow-up-shutdown-a");
      const blockerId = ProjectId.make("follow-up-shutdown-z");
      yield* addProject({ projectId, mode: "observe" });
      yield* addProject({ projectId: blockerId, mode: "observe" });
      const passBlocked = yield* Deferred.make<void>();
      const passInterrupted = yield* Deferred.make<void>();
      let enumerations = 0;
      let blockProject = false;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            return enumerations === 2
              ? base.pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      blockProject = true;
                    }),
                  ),
                )
              : base;
          }),
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            get: (candidate) =>
              Effect.suspend(() => {
                if (candidate !== blockerId || !blockProject) return base.get(candidate);
                blockProject = false;
                return Deferred.succeed(passBlocked, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.onInterrupt(() =>
                    Deferred.succeed(passInterrupted, undefined).pipe(Effect.ignore),
                  ),
                );
              }),
          }),
        reactorOptions: { watchdogIntervalMs: 1_000 },
      });
      const firstScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(firstScope));

      yield* TestClock.adjust(Duration.millis(1_000));
      yield* Deferred.await(passBlocked);
      yield* TestClock.adjust(Duration.millis(1_000));
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");
      yield* Scope.close(firstScope, Exit.void);
      yield* Deferred.await(passInterrupted);
      yield* TestClock.adjust(Duration.seconds(10));
      assert.equal(enumerations, 2);

      const secondScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(secondScope));
      assert.equal(enumerations, 3);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "healthy");
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.activeSubscriptions(name), 1);
        assert.equal(harness.maxActiveSubscriptions(name), 1);
      }
      yield* Scope.close(secondScope, Exit.void);
    }),
  ),
);

it.effect("reports global recovery retries and degrades after repeated watchdog failures", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("watchdog-global-recovery-status");
      yield* addProject({ projectId, mode: "observe" });
      let enumerations = 0;
      let failWatchdog = true;
      const harness = yield* makeHarness({
        listPersisted: (base) =>
          Effect.suspend(() => {
            enumerations += 1;
            return enumerations > 1 && failWatchdog
              ? Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.watchdog-global-recovery",
                  }),
                )
              : base;
          }),
        reactorOptions: {
          watchdogIntervalMs: 1,
          recoveryRetryBaseMs: 1,
          recoveryRetryMaxMs: 1,
        },
      });
      yield* harness.reactor.start();

      yield* TestClock.adjust(Duration.millis(1));
      yield* waitFor(() => enumerations >= 2);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "recovering");
      for (let index = 0; index < 4; index += 1) {
        yield* TestClock.adjust(Duration.millis(1));
        yield* Effect.yieldNow;
      }
      yield* waitFor(() => enumerations >= 4);
      assert.equal((yield* harness.reactor.getStatus({ projectId })).health, "degraded");

      failWatchdog = false;
      yield* TestClock.adjust(Duration.millis(1));
      yield* waitForEffect(() =>
        harness.reactor
          .getStatus({ projectId })
          .pipe(Effect.map((status) => status.health === "healthy")),
      );
    }),
  ),
);

it.effect("treats poll-in-progress and revision conflicts as coordination, not failures", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("coordination");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      let code: "poll-in-progress" | "revision-conflict" = "poll-in-progress";
      harness.setPoll((input) =>
        Effect.fail(
          new AgentControlGithubRpcError({
            code,
            operation: "poll-once",
            projectId: input.projectId,
          }),
        ),
      );
      yield* harness.reactor.start();
      yield* flush;
      let status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.consecutiveFailures, 0);
      assert.equal(status.reasonCode, "poll-in-progress");

      code = "revision-conflict";
      yield* TestClock.adjust(Duration.seconds(15));
      yield* waitFor(() => harness.pollInputs.length === 2);
      status = yield* harness.reactor.getStatus({ projectId });
      assert.equal(status.consecutiveFailures, 0);
      assert.equal(status.reasonCode, "revision-conflict");
      yield* TestClock.adjust(Duration.seconds(1));
      assert.equal(harness.pollInputs.length, 2);
    }),
  ),
);

it.effect("project deletion interrupts a running poll and removes recovery state", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("delete-shutdown");
      yield* addProject({ projectId, mode: "observe" });
      const interrupted = yield* Deferred.make<void>();
      const harness = yield* makeHarness();
      harness.setPoll(() =>
        Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined).pipe(Effect.ignore)),
        ),
      );
      const workerScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(workerScope));
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);

      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${EPOCH}
        WHERE project_id = ${projectId}
      `;
      yield* PubSub.publish(harness.orchestrationEvents, {
        sequence: 1,
        eventId: EventId.make("deleted-event"),
        aggregateKind: "project",
        aggregateId: projectId,
        occurredAt: EPOCH,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.deleted",
        payload: { projectId, deletedAt: EPOCH },
      });
      yield* Deferred.await(interrupted);
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      for (let index = 0; index < 200; index += 1) {
        if (Option.isNone(yield* scheduler.get(projectId))) break;
        yield* Effect.yieldNow;
      }
      assert.isTrue(Option.isNone(yield* scheduler.get(projectId)));
      yield* Scope.close(workerScope, Exit.void);
    }),
  ),
);

it.effect("stops a deleted poll before transient scheduler cleanup and retries CAS deletion", () =>
  run(
    Effect.gen(function* () {
      const deletedProject = ProjectId.make("delete-transient-cleanup");
      const healthyProject = ProjectId.make("delete-transient-other");
      yield* addProject({ projectId: deletedProject, mode: "observe" });
      yield* addProject({ projectId: healthyProject, mode: "observe" });
      const interrupted = yield* Deferred.make<void>();
      let pollInterrupted = false;
      let failDeletedGet = false;
      let failDeletedDelete = false;
      const harness = yield* makeHarness({
        schedulerStates: (base) =>
          AgentControlGithubSchedulerStateRepository.of({
            ...base,
            get: (projectId) =>
              Effect.suspend(() => {
                if (projectId !== deletedProject || !failDeletedGet) return base.get(projectId);
                assert.isTrue(pollInterrupted);
                failDeletedGet = false;
                return Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.delete-transient-get",
                  }),
                );
              }),
            delete: (projectId, expectedRevision) =>
              Effect.suspend(() => {
                if (projectId !== deletedProject || !failDeletedDelete) {
                  return base.delete(projectId, expectedRevision);
                }
                failDeletedDelete = false;
                return Effect.fail(
                  new AgentControlPersistenceSqlError({
                    operation: "test.delete-transient-delete",
                  }),
                );
              }),
          }),
        reactorOptions: { recoveryRetryBaseMs: 1, recoveryRetryMaxMs: 1 },
      });
      harness.setPoll(({ projectId }) =>
        projectId === deletedProject
          ? Effect.never.pipe(
              Effect.onInterrupt(() =>
                Effect.sync(() => {
                  pollInterrupted = true;
                }).pipe(Effect.andThen(Deferred.succeed(interrupted, undefined)), Effect.ignore),
              ),
            )
          : Effect.never,
      );
      yield* harness.reactor.start();
      yield* flush;
      assert.equal(harness.pollInputs.length, 2);

      failDeletedGet = true;
      failDeletedDelete = true;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${EPOCH}
        WHERE project_id = ${deletedProject}
      `;
      yield* PubSub.publish(harness.orchestrationEvents, {
        sequence: 1,
        eventId: EventId.make("deleted-transient-event"),
        aggregateKind: "project",
        aggregateId: deletedProject,
        occurredAt: EPOCH,
        commandId: null,
        causationEventId: null,
        correlationId: null,
        metadata: {},
        type: "project.deleted",
        payload: { projectId: deletedProject, deletedAt: EPOCH },
      });

      yield* Deferred.await(interrupted);
      yield* waitFor(() => !failDeletedGet);
      yield* TestClock.adjust(Duration.millis(1));
      yield* waitFor(() => !failDeletedDelete);
      yield* TestClock.adjust(Duration.millis(1));
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      yield* waitForEffect(() => scheduler.get(deletedProject).pipe(Effect.map(Option.isNone)));
      const otherStatus = yield* harness.reactor.getStatus({ projectId: healthyProject });
      assert.equal(otherStatus.workerStatus, "polling");
      assert.equal(
        harness.pollInputs.filter(({ projectId }) => projectId === healthyProject).length,
        1,
      );
    }),
  ),
);

it.effect("a canonical availability reconcile cleans up a missed project deletion event", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("missed-delete-event");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness();
      yield* harness.reactor.start();
      yield* flush;

      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${EPOCH}
        WHERE project_id = ${projectId}
      `;
      yield* PubSub.publish(
        harness.projectEvents,
        projectEvent(projectId, "observe", "observe", 2),
      );
      const scheduler = yield* AgentControlGithubSchedulerStateRepository;
      yield* waitForEffect(() => scheduler.get(projectId).pipe(Effect.map(Option.isNone)));
      assert.equal(
        (yield* Effect.result(harness.reactor.getStatus({ projectId })))._tag,
        "Failure",
      );
    }),
  ),
);

for (const subscription of ["project-controller", "github-intake", "project-delete"] as const) {
  it.effect(`re-subscribes ${subscription} and reconciles committed GitHub history`, () =>
    run(
      Effect.gen(function* () {
        const projectId = ProjectId.make(`subscription-recovery-${subscription}`);
        yield* addProject({ projectId, mode: "observe" });
        const harness = yield* makeHarness({
          faultSubscriptionOnce: subscription,
          reactorOptions: {
            subscriptionRetryBaseMs: 1,
            subscriptionRetryMaxMs: 1,
          },
        });
        const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
        yield* waitFor(() => harness.subscriptionStarts(subscription) === 1);
        yield* persistGithubEvent(
          githubPollEvent(projectId, {
            type: "failure",
            sequence: 2,
            code: "github-timeout",
          }),
        );

        yield* TestClock.adjust(Duration.millis(1));
        yield* Fiber.join(startFiber);
        yield* waitFor(() => harness.subscriptionStarts(subscription) === 2);
        assert.equal(harness.subscriptionReleases(subscription), 1);
        assert.equal(harness.activeSubscriptions(subscription), 1);
        assert.equal(harness.maxActiveSubscriptions(subscription), 1);
        yield* waitForEffect(() =>
          harness.reactor
            .getStatus({ projectId })
            .pipe(
              Effect.map(
                (status) =>
                  status.subscriptionHealth === "healthy" && status.consecutiveFailures === 1,
              ),
            ),
        );
      }),
    ),
  );
}

for (const scenario of ["acquisition failure", "normal stream end"] as const) {
  it.effect(`finalizes a subscription after ${scenario} before replacing it`, () =>
    run(
      Effect.gen(function* () {
        const projectId = ProjectId.make(
          `subscription-${scenario === "acquisition failure" ? "acquire" : "end"}`,
        );
        yield* addProject({ projectId, mode: "observe" });
        const harness = yield* makeHarness({
          ...(scenario === "acquisition failure"
            ? { failSubscriptionAcquisitionOnce: "project-controller" as const }
            : { endSubscriptionOnce: "project-controller" as const }),
          reactorOptions: {
            subscriptionRetryBaseMs: 1,
            subscriptionRetryMaxMs: 1,
          },
        });
        const startFiber = yield* harness.reactor.start().pipe(Effect.forkScoped);
        yield* waitFor(() => harness.subscriptionStarts("project-controller") === 1);
        yield* TestClock.adjust(Duration.millis(1));
        yield* Fiber.join(startFiber);
        yield* waitFor(() => harness.subscriptionStarts("project-controller") === 2);
        assert.equal(harness.subscriptionReleases("project-controller"), 1);
        assert.equal(harness.activeSubscriptions("project-controller"), 1);
        assert.equal(harness.maxActiveSubscriptions("project-controller"), 1);
      }),
    ),
  );
}

it.effect("scope shutdown does not re-subscribe a failed hot stream", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("subscription-shutdown");
      yield* addProject({ projectId, mode: "observe" });
      const harness = yield* makeHarness({
        faultSubscriptionOnce: "github-intake",
        reactorOptions: {
          subscriptionRetryBaseMs: 10,
          subscriptionRetryMaxMs: 10,
        },
      });
      const scope = yield* Scope.make("sequential");
      const startFiber = yield* harness.reactor
        .start()
        .pipe(Scope.provide(scope), Effect.forkScoped);
      yield* waitFor(() => harness.subscriptionStarts("github-intake") === 1);
      yield* TestClock.adjust(Duration.millis(10));
      yield* Fiber.join(startFiber);
      assert.equal(harness.subscriptionStarts("github-intake"), 2);
      yield* Scope.close(scope, Exit.void);
      yield* TestClock.adjust(Duration.seconds(1));
      assert.equal(harness.subscriptionStarts("github-intake"), 2);
      for (const name of ["project-controller", "github-intake", "project-delete"] as const) {
        assert.equal(harness.activeSubscriptions(name), 0);
        assert.equal(harness.subscriptionReleases(name), harness.subscriptionStarts(name));
      }
    }),
  ),
);

it.effect("scope shutdown interrupts a running poll and leaves no scheduled timer", () =>
  run(
    Effect.gen(function* () {
      const projectId = ProjectId.make("scope-shutdown");
      yield* addProject({ projectId, mode: "observe" });
      const interrupted = yield* Deferred.make<void>();
      const harness = yield* makeHarness();
      harness.setPoll(() =>
        Effect.never.pipe(
          Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined).pipe(Effect.ignore)),
        ),
      );
      const workerScope = yield* Scope.make("sequential");
      yield* harness.reactor.start().pipe(Scope.provide(workerScope));
      yield* flush;
      assert.equal(harness.pollInputs.length, 1);

      yield* Scope.close(workerScope, Exit.void);
      yield* Deferred.await(interrupted);
      yield* TestClock.adjust(Duration.hours(1));
      assert.equal(harness.pollInputs.length, 1);
    }),
  ),
);

it("computes interval-based exponential backoff with a fifteen minute cap", () => {
  assert.equal(githubObserveBackoffMs(15, 1, "github-timeout"), 15_000);
  assert.equal(githubObserveBackoffMs(15, 2, "github-timeout"), 30_000);
  assert.equal(githubObserveBackoffMs(15, 3, "github-timeout"), 60_000);
  assert.equal(githubObserveBackoffMs(3_600, 20, "github-timeout"), 900_000);
});
