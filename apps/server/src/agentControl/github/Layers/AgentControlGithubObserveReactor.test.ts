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
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
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
import { layer as GithubStateRepositoryLive } from "./AgentControlGithubStateRepository.ts";
import {
  AGENT_CONTROL_GITHUB_CIRCUIT_COOLDOWN_MS,
  AGENT_CONTROL_GITHUB_MAX_BACKOFF_MS,
  githubObserveBackoffMs,
  make,
} from "./AgentControlGithubObserveReactor.ts";

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

const persistenceLayer = Layer.mergeAll(
  AgentControlProjectStateRepositoryLive,
  AgentControlProjectionStateRepositoryLive,
  GithubStateRepositoryLive,
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
  readonly maxActivePolls: () => number;
  readonly setPoll: (implementation: AgentControlGithubIntake["Service"]["pollOnce"]) => void;
}

const makeHarness = (options?: {
  readonly duringEnumeration?: (input: {
    readonly githubEvents: PubSub.PubSub<AgentControlGithubEvent>;
  }) => Effect.Effect<void, never, SqlClient.SqlClient>;
}) =>
  Effect.gen(function* () {
    const githubStates = yield* AgentControlGithubStateRepository;
    const projectStates = yield* AgentControlProjectStateRepository;
    const sql = yield* SqlClient.SqlClient;
    const projectEvents = yield* PubSub.unbounded<AgentControlEvent>();
    const githubEvents = yield* PubSub.unbounded<AgentControlGithubEvent>();
    const orchestrationEvents = yield* PubSub.unbounded<OrchestrationEvent>();
    const pollInputs: Array<AgentControlGithubPollOnceInput> = [];
    let activePolls = 0;
    let maximumActivePolls = 0;
    let pollImplementation: AgentControlGithubIntake["Service"]["pollOnce"] = () => Effect.never;
    const reactorProjectStates =
      options?.duringEnumeration === undefined
        ? projectStates
        : AgentControlProjectStateRepository.of({
            ...projectStates,
            listPersisted: options
              .duringEnumeration({ githubEvents })
              .pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
                Effect.andThen(projectStates.listPersisted),
              ),
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
      streamDomainEvents: Stream.fromPubSub(githubEvents),
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
    });
    const orchestration = OrchestrationEngineService.of({
      readEvents: () => Stream.empty,
      dispatch: () => Effect.succeed({ sequence: 0 }),
      dispatchClient: () => Effect.succeed({ sequence: 0 }),
      dispatchAgentControl: () => Effect.succeed({ sequence: 0 }),
      streamDomainEvents: Stream.fromPubSub(orchestrationEvents),
      latestSequence: Effect.succeed(0),
    });
    const reactor = yield* make({ jitterMillis: () => 0 }).pipe(
      Effect.provideService(AgentControlEngine, controller),
      Effect.provideService(AgentControlGithubIntake, intake),
      Effect.provideService(OrchestrationEngineService, orchestration),
      Effect.provideService(AgentControlProjectStateRepository, reactorProjectStates),
    );

    return {
      reactor,
      projectEvents,
      githubEvents,
      orchestrationEvents,
      pollInputs,
      maxActivePolls: () => maximumActivePolls,
      setPoll: (implementation) => {
        pollImplementation = implementation;
      },
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
        const corruptRecovery = ProjectId.make("corrupt-recovery");
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
        yield* addProject({ projectId: corruptRecovery, mode: "observe" });
        const sql = yield* SqlClient.SqlClient;
        yield* sql`
          INSERT INTO agent_control_github_scheduler_states (
            project_id, state_json, generation, last_github_event_sequence,
            activity, circuit_state,
            consecutive_failures, last_attempt_at, next_attempt_at,
            cooldown_until, reason_code, updated_at
          ) VALUES (
            ${corruptRecovery}, '{}', 1, 1, 'active', 'closed',
            0, NULL, ${EPOCH}, NULL, NULL, ${EPOCH}
          )
        `;

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
        const corruptStatus = yield* Effect.result(
          harness.reactor.getStatus({ projectId: corruptRecovery }),
        );
        assert.equal(corruptStatus._tag, "Failure");
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
