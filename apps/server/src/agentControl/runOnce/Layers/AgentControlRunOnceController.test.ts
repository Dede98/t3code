import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlRunOnceId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlEvent,
  type AgentControlGithubEvent,
  type AgentControlProjectState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import {
  alreadyActivated,
  makeReactorStartupActivation,
} from "../../../reactorStartupActivation.ts";
import { AgentControlEventStore } from "../../../persistence/Services/AgentControlEventStore.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlControlledThreadActivation } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import {
  createDefaultGithubIntakeState,
  projectGithubIntakeEvent,
} from "../../github/projector.ts";
import { AgentControlGithubEventStore } from "../../github/Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { canonicalJson, type JsonValue } from "../../initialPlanning/eventEvidence.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskEventStore } from "../../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskReconcileStateRepository } from "../../task/Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { fingerprintRunOnceModeCommand } from "../authority.ts";
import { AgentControlRunOnceController } from "../Services/AgentControlRunOnceController.ts";
import {
  AgentControlRunOnceControllerHooks,
  type AgentControlRunOnceControllerHooksShape,
} from "../Services/AgentControlRunOnceControllerHooks.ts";
import {
  AgentControlRunOnceControllerLive,
  readFullRunOnceTaskHistory,
  superviseAgentControlRunOnceListener,
} from "./AgentControlRunOnceController.ts";

const at = "2026-08-31T12:00:00.000Z";
const projectId = ProjectId.make("run-once-controller-race");

it.effect("supervises post-start listener defects with backoff and scoped shutdown", () =>
  Effect.gen(function* () {
    const failInitialListener = yield* Deferred.make<void>();
    const secondFailureSubscribed = yield* Deferred.make<void>();
    const activeListener = yield* Deferred.make<void>();
    const listenerInterrupted = yield* Deferred.make<void>();
    const subscriptionAttempts = yield* Ref.make(0);

    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* superviseAgentControlRunOnceListener(
          Stream.fromEffect(
            Deferred.await(failInitialListener).pipe(
              Effect.andThen(Effect.die(new Error("initial listener defect"))),
            ),
          ),
          Effect.gen(function* () {
            const attempt = yield* Ref.getAndUpdate(subscriptionAttempts, (count) => count + 1);
            if (attempt === 0) {
              yield* Deferred.succeed(secondFailureSubscribed, undefined);
              return Stream.fromEffect(Effect.die(new Error("repeated listener defect")));
            }
            return Stream.fromEffect(
              Deferred.succeed(activeListener, undefined).pipe(
                Effect.andThen(
                  Effect.never.pipe(
                    Effect.onInterrupt(() =>
                      Deferred.succeed(listenerInterrupted, undefined).pipe(Effect.ignore),
                    ),
                  ),
                ),
              ),
            );
          }),
          () => Effect.void,
          "project",
        );
        yield* Deferred.succeed(failInitialListener, undefined);
        yield* Deferred.await(secondFailureSubscribed);
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(subscriptionAttempts), 1);
        yield* TestClock.adjust(Duration.millis(25));
        yield* Deferred.await(activeListener);
        assert.equal(yield* Ref.get(subscriptionAttempts), 2);
      }),
    );
    yield* Deferred.await(listenerInterrupted);
  }),
);

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const insertEvent = Effect.fn("insertRunOnceControllerEvent")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly eventId: string;
    readonly aggregateKind: string;
    readonly streamId: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: JsonValue;
  },
) {
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO main.agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      ${input.eventId}, ${input.aggregateKind}, ${input.streamId}, ${input.streamVersion},
      ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
      ${input.authority}, ${canonicalJson(input.payload)}, '{"schemaVersion":1}'
    ) RETURNING sequence
  `;
  return rows[0]!.sequence;
});

const insertModeReceipt = Effect.fn("insertRunOnceControllerModeReceipt")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly projectId?: ProjectId;
    readonly commandId: string;
    readonly authority: "human" | "system";
    readonly expectedRevision: number;
    readonly mode: string;
    readonly sequence: number;
    readonly streamVersion: number;
  },
) {
  const receiptProjectId = input.projectId ?? projectId;
  yield* sql`
    INSERT INTO main.agent_control_command_receipts (
      command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
      status, result_sequence, result_stream_version, event_created, accepted_at, error_code
    ) VALUES (
      ${input.commandId},
      ${fingerprintRunOnceModeCommand({
        commandId: input.commandId,
        projectId: receiptProjectId,
        expectedRevision: input.expectedRevision,
        mode: input.mode,
      })},
      ${input.authority}, 'project-controller', ${receiptProjectId}, 'accepted', ${input.sequence},
      ${input.streamVersion}, 1, ${at}, NULL
    )
  `;
});

it.effect("paginates Task history through a terminal event beyond the store cap", () =>
  Effect.gen(function* () {
    const taskId = AgentControlTaskId.make("run-once-paginated-task");
    const runId = AgentControlRunOnceId.make("run-once-paginated-run");
    const offsets: Array<number> = [];
    const events = Array.from({ length: 1_001 }, (_, index) => ({
      aggregateKind: "task" as const,
      aggregateId: taskId,
      streamVersion: index + 1,
      type:
        index === 1_000
          ? ("agentControl.task.finalizedAfterVerification" as const)
          : ("agentControl.task.created" as const),
    }));
    const history = yield* readFullRunOnceTaskHistory(
      {
        readStream: (_taskId: AgentControlTaskId, after = 0, limit = 1_000) => {
          offsets.push(after);
          return Effect.succeed(events.slice(after, after + Math.min(limit, 1_000)) as never);
        },
      } as never,
      projectId,
      runId,
      taskId,
    );
    assert.lengthOf(history, 1_001);
    assert.equal(history.at(-1)?.type, "agentControl.task.finalizedAfterVerification");
    assert.deepStrictEqual(offsets, [0, 500, 1_000, 1_001]);
  }),
);

it.live("delivers committed publications exactly once through a durable WAL inbox", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-run-once-controller-" });
      const filename = path.join(directory, "authority.sqlite");
      const connectionA = yield* openDatabase(filename);
      const connectionB = yield* openDatabase(filename);
      const connectionC = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(connectionC.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(connectionB.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(connectionA.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
        Effect.provideService(SqlClient.SqlClient, connectionA.sql),
      );

      const observeSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-observe-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 1,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-observe-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "manual",
          mode: "observe",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      });
      const configSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-github-config-event",
        aggregateKind: "github-intake",
        streamId: projectId,
        streamVersion: 1,
        eventType: "agentControl.github.config.set",
        commandId: "controller-github-config-command",
        authority: "human",
        payload: {
          projectId,
          settings: {
            trackerKind: "github",
            readyLabel: "agent:ready",
            pausedLabel: "agent:paused",
            trustedLogins: [],
            pollIntervalSeconds: 60,
          },
          repository: { repositoryNodeId: "controller-repository", nameWithOwner: "owner/repo" },
          configuredAt: at,
        },
      });
      const githubSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-github-success-event",
        aggregateKind: "github-intake",
        streamId: projectId,
        streamVersion: 2,
        eventType: "agentControl.github.poll.succeeded",
        commandId: "controller-github-success-command",
        authority: "controller",
        payload: {
          projectId,
          repository: { repositoryNodeId: "controller-repository", nameWithOwner: "owner/repo" },
          attemptedAt: at,
          completedAt: at,
          cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
          issues: [],
        },
      });
      const activationSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-run-once-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 2,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-run-once-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "observe",
          mode: "run-once",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      });
      yield* insertModeReceipt(connectionA.sql, {
        commandId: "controller-run-once-command",
        authority: "human",
        expectedRevision: 1,
        mode: "run-once",
        sequence: activationSequence,
        streamVersion: 2,
      });
      yield* connectionA.sql`
        INSERT INTO main.agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (${projectId}, 'run-once', NULL, 2, ${activationSequence}, ${at})
      `;
      yield* connectionA.sql`
        INSERT INTO main.agent_control_task_reconcile_states (
          project_id, target_sequence, last_completed_sequence, revision, status, updated_at
        ) VALUES (${projectId}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
      `;

      const observeEvent = {
        sequence: observeSequence,
        streamVersion: 1,
        eventId: EventId.make("controller-observe-event"),
        type: "agentControl.project.mode.changed",
        aggregateKind: "project-controller",
        aggregateId: projectId,
        occurredAt: at,
        commandId: CommandId.make("controller-observe-command"),
        causationEventId: null,
        correlationId: CommandId.make("controller-observe-command"),
        authority: "human",
        metadata: { schemaVersion: 1 },
        payload: {
          projectId,
          previousMode: "manual",
          mode: "observe",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      } as const satisfies AgentControlEvent;
      const activationEvent = {
        ...observeEvent,
        sequence: activationSequence,
        streamVersion: 2,
        eventId: EventId.make("controller-run-once-event"),
        commandId: CommandId.make("controller-run-once-command"),
        correlationId: CommandId.make("controller-run-once-command"),
        payload: {
          projectId,
          previousMode: "observe",
          mode: "run-once",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      } as const satisfies AgentControlEvent;
      const configEvent = {
        sequence: configSequence,
        streamVersion: 1,
        eventId: EventId.make("controller-github-config-event"),
        type: "agentControl.github.config.set",
        aggregateKind: "github-intake",
        aggregateId: projectId,
        occurredAt: at,
        commandId: CommandId.make("controller-github-config-command"),
        causationEventId: null,
        correlationId: CommandId.make("controller-github-config-command"),
        authority: "human",
        metadata: { schemaVersion: 1 },
        payload: {
          projectId,
          settings: {
            trackerKind: "github",
            readyLabel: "agent:ready",
            pausedLabel: "agent:paused",
            trustedLogins: [],
            pollIntervalSeconds: 60,
          },
          repository: { repositoryNodeId: "controller-repository", nameWithOwner: "owner/repo" },
          configuredAt: at,
        },
      } as const satisfies AgentControlGithubEvent;
      const successEvent = {
        ...configEvent,
        sequence: githubSequence,
        streamVersion: 2,
        eventId: EventId.make("controller-github-success-event"),
        type: "agentControl.github.poll.succeeded",
        commandId: CommandId.make("controller-github-success-command"),
        correlationId: CommandId.make("controller-github-success-command"),
        authority: "controller",
        payload: {
          projectId,
          repository: { repositoryNodeId: "controller-repository", nameWithOwner: "owner/repo" },
          attemptedAt: at,
          completedAt: at,
          cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
          issues: [],
        },
      } as const satisfies AgentControlGithubEvent;
      const githubState = yield* projectGithubIntakeEvent(
        yield* projectGithubIntakeEvent(createDefaultGithubIntakeState(projectId), configEvent),
        successEvent,
      );
      const projectEvents = yield* Ref.make<ReadonlyArray<AgentControlEvent>>([
        observeEvent,
        activationEvent,
      ]);
      const projectState = yield* Ref.make<AgentControlProjectState>({
        schemaVersion: 1,
        projectId,
        mode: "run-once",
        pausedFromMode: null,
        revision: 2,
        sequence: activationSequence,
        updatedAt: at,
      });
      const resetLock = yield* Semaphore.make(1);
      const loseResetResponse = yield* Ref.make(false);

      interface MirroredAuthority {
        readonly projectEvents: ReadonlyArray<AgentControlEvent>;
        readonly projectState: AgentControlProjectState;
        readonly githubEvents: ReadonlyArray<AgentControlGithubEvent>;
        readonly githubState: typeof githubState;
        readonly githubSequence: number;
        readonly repositoryNodeId: string;
      }

      const makeController = Effect.fn("makeRunOnceControllerTestLayer")(function* (
        sql: SqlClient.SqlClient,
        hooks: AgentControlRunOnceControllerHooksShape,
        subscriptions: {
          readonly mirrors?: ReadonlyMap<ProjectId, MirroredAuthority>;
        } = {},
      ) {
        const projectEngine = AgentControlEngine.of({
          getProjectState: (input) => {
            const mirror = subscriptions.mirrors?.get(input.projectId);
            return mirror === undefined
              ? Ref.get(projectState)
              : Effect.succeed(mirror.projectState);
          },
          dispatchHuman: () => Effect.die("unused"),
          dispatchController: () => Effect.die("unused"),
          dispatchSystem: (input) =>
            resetLock
              .withPermit(
                Effect.gen(function* () {
                  const events = yield* Ref.get(projectEvents);
                  const currentState = yield* Ref.get(projectState);
                  const existing = events.find((event) => event.commandId === input.commandId);
                  if (existing !== undefined) {
                    return {
                      state: currentState,
                      resultSequence: existing.sequence,
                      eventCreated: false,
                    };
                  }
                  if (input.expectedRevision !== currentState.revision) {
                    return yield* Effect.die("stale run-once system reset in test authority");
                  }
                  const nextRevision = currentState.revision + 1;
                  const sequence = yield* insertEvent(sql, {
                    eventId: `event-${input.commandId}`,
                    aggregateKind: "project-controller",
                    streamId: projectId,
                    streamVersion: nextRevision,
                    eventType: "agentControl.project.mode.changed",
                    commandId: input.commandId,
                    authority: "system",
                    payload: {
                      projectId,
                      previousMode: "run-once",
                      mode: "observe",
                      previousPausedFromMode: null,
                      pausedFromMode: null,
                      changedAt: at,
                    },
                  });
                  yield* insertModeReceipt(sql, {
                    commandId: input.commandId,
                    authority: "system",
                    expectedRevision: currentState.revision,
                    mode: "observe",
                    sequence,
                    streamVersion: nextRevision,
                  });
                  yield* sql`
              UPDATE main.agent_control_project_states
              SET mode = 'observe', revision = ${nextRevision},
                last_event_sequence = ${sequence}, updated_at = ${at}
              WHERE project_id = ${projectId} AND revision = ${currentState.revision}
            `;
                  const event = {
                    ...observeEvent,
                    sequence,
                    streamVersion: nextRevision,
                    eventId: EventId.make(`event-${input.commandId}`),
                    commandId: input.commandId,
                    correlationId: input.commandId,
                    authority: "system",
                    payload: {
                      projectId,
                      previousMode: "run-once",
                      mode: "observe",
                      previousPausedFromMode: null,
                      pausedFromMode: null,
                      changedAt: at,
                    },
                  } as const satisfies AgentControlEvent;
                  const state: AgentControlProjectState = {
                    schemaVersion: 1,
                    projectId,
                    mode: "observe",
                    pausedFromMode: null,
                    revision: nextRevision,
                    sequence,
                    updatedAt: at,
                  };
                  yield* Ref.set(projectEvents, [...events, event]);
                  yield* Ref.set(projectState, state);
                  if (yield* Ref.getAndSet(loseResetResponse, false)) {
                    return yield* Effect.die(new Error("injected reset response loss"));
                  }
                  return { state, resultSequence: sequence, eventCreated: true };
                }),
              )
              .pipe(Effect.orDie),
          streamDomainEvents: Stream.never,
          subscribeDomainEvents: Effect.succeed(Stream.never),
        });
        const dependencies = Layer.mergeAll(
          Layer.succeed(SqlClient.SqlClient, sql),
          Layer.succeed(AgentControlEventStore, {
            append: () => Effect.die("unused"),
            readStream: (requestedProjectId: ProjectId, after = 0, limit = 500) => {
              const mirror = subscriptions.mirrors?.get(requestedProjectId);
              return (
                mirror === undefined ? Ref.get(projectEvents) : Effect.succeed(mirror.projectEvents)
              ).pipe(
                Effect.map((events) =>
                  events.filter((event) => event.streamVersion > after).slice(0, limit),
                ),
              );
            },
            readGlobal: () => Effect.die("unused"),
            latestSequence: Effect.succeed(activationSequence),
          } as never),
          Layer.succeed(AgentControlProjectStateRepository, {
            get: (requestedProjectId: ProjectId) => {
              const mirror = subscriptions.mirrors?.get(requestedProjectId);
              return mirror === undefined
                ? Ref.get(projectState).pipe(Effect.map(Option.some))
                : Effect.succeed(Option.some(mirror.projectState));
            },
            save: () => Effect.die("unused"),
            listPersisted: Effect.die("unused"),
            deleteAll: Effect.die("unused"),
          }),
          Layer.succeed(AgentControlGithubEventStore, {
            append: () => Effect.die("unused"),
            readStream: (requestedProjectId: ProjectId, after = 0, limit = 500) => {
              const mirror = subscriptions.mirrors?.get(requestedProjectId);
              return Effect.succeed(
                (mirror?.githubEvents ?? [configEvent, successEvent])
                  .filter((event) => event.streamVersion > after)
                  .slice(0, limit),
              );
            },
            readGlobal: () => Effect.die("unused"),
            readProjectAfterSequence: () => Effect.die("unused"),
            latestSequence: Effect.succeed(githubSequence),
          } as never),
          Layer.succeed(AgentControlGithubStateRepository, {
            get: (requestedProjectId: ProjectId) =>
              Effect.succeed(
                Option.some(
                  subscriptions.mirrors?.get(requestedProjectId)?.githubState ?? githubState,
                ),
              ),
            getCompletedSnapshot: (requestedProjectId: ProjectId) => {
              const mirror = subscriptions.mirrors?.get(requestedProjectId);
              return Effect.succeed(
                Option.some({
                  sourcePrecondition: {
                    schemaVersion: 1,
                    projectId: requestedProjectId,
                    githubIntakeSequence: mirror?.githubSequence ?? githubSequence,
                    githubProjectionRevision: 2,
                    githubConfigRevision: 2,
                    repositoryNodeId: mirror?.repositoryNodeId ?? "controller-repository",
                    pollStatus: "success",
                    expectedIssueCount: 0,
                  },
                  issues: [],
                }),
              );
            },
          } as never),
          Layer.succeed(AgentControlTaskReconcileStateRepository, {
            get: (requestedProjectId: ProjectId) => {
              const targetSequence =
                subscriptions.mirrors?.get(requestedProjectId)?.githubSequence ?? githubSequence;
              return Effect.succeed(
                Option.some({
                  schemaVersion: 1,
                  projectId: requestedProjectId,
                  targetSequence,
                  lastCompletedSequence: targetSequence,
                  revision: 1,
                  status: "completed",
                  updatedAt: at,
                }),
              );
            },
          } as never),
          Layer.succeed(AgentControlTaskEventStore, {
            readGlobal: () => Effect.succeed([]),
            readStream: () => Effect.succeed([]),
          } as never),
          Layer.succeed(AgentControlTaskStateRepository, {
            listProject: () => Effect.succeed([]),
          } as never),
          Layer.succeed(AgentControlTaskEngine, {
            subscribeDomainEvents: Effect.succeed(Stream.never),
          } as never),
          Layer.succeed(AgentControlEngine, projectEngine),
          Layer.succeed(AgentControlStageRun, {} as never),
          Layer.succeed(AgentControlStageRunLeaseEngine, {} as never),
          Layer.succeed(AgentControlWorktreeController, {} as never),
          Layer.succeed(AgentControlControlledThreadActivation, {} as never),
          Layer.succeed(AgentControlRunOnceControllerHooks, hooks),
        );
        const context = yield* Layer.build(
          Layer.fresh(AgentControlRunOnceControllerLive).pipe(Layer.provide(dependencies)),
        );
        return Context.get(context, AgentControlRunOnceController);
      });

      const publicationsA = yield* Ref.make(0);
      const publicationsB = yield* Ref.make(0);
      const publicationsC = yield* Ref.make(0);
      const failFirstPublication = yield* Ref.make(true);
      const failBeforeWake = yield* Ref.make(true);
      const publicationHookEntries = yield* Ref.make(0);
      const firstPublicationEntered = yield* Deferred.make<void>();
      const releaseFirstPublication = yield* Deferred.make<void>();
      const humanTakeoverDone = yield* Ref.make(false);
      const noop = () => Effect.void;
      const controllerA = yield* makeController(connectionA.sql, {
        afterSubscriptionsBeforeRecovery: Effect.void,
        afterActivationAuthority: noop,
        afterStepCommitted: noop,
        beforePublication: noop,
        afterPublication: () =>
          Effect.gen(function* () {
            yield* Ref.update(publicationHookEntries, (count) => count + 1);
            const fail = yield* Ref.getAndSet(failFirstPublication, false);
            if (fail) {
              yield* Deferred.succeed(firstPublicationEntered, undefined);
              yield* Deferred.await(releaseFirstPublication);
              return yield* Effect.die(new Error("injected post-commit publication loss"));
            }
          }),
      });
      const controllerB = yield* makeController(connectionB.sql, {
        afterSubscriptionsBeforeRecovery: Effect.void,
        afterActivationAuthority: noop,
        afterStepCommitted: (observation) =>
          observation.step !== "no-eligible-task"
            ? Effect.void
            : Effect.gen(function* () {
                if (yield* Ref.getAndSet(humanTakeoverDone, true)) return;
                const currentState = yield* Ref.get(projectState);
                assert.equal(currentState.mode, "run-once");
                assert.equal(currentState.revision, 4);
                const sequence = yield* insertEvent(connectionB.sql, {
                  eventId: "controller-human-takeover-event",
                  aggregateKind: "project-controller",
                  streamId: projectId,
                  streamVersion: 5,
                  eventType: "agentControl.project.mode.changed",
                  commandId: "controller-human-takeover-command",
                  authority: "human",
                  payload: {
                    projectId,
                    previousMode: "run-once",
                    mode: "manual",
                    previousPausedFromMode: null,
                    pausedFromMode: null,
                    changedAt: at,
                  },
                });
                yield* insertModeReceipt(connectionB.sql, {
                  commandId: "controller-human-takeover-command",
                  authority: "human",
                  expectedRevision: 4,
                  mode: "manual",
                  sequence,
                  streamVersion: 5,
                });
                yield* connectionB.sql`
                  UPDATE main.agent_control_project_states
                  SET mode = 'manual', paused_from_mode = NULL, revision = 5,
                    last_event_sequence = ${sequence}, updated_at = ${at}
                  WHERE project_id = ${projectId} AND revision = 4
                `;
                const event = {
                  ...activationEvent,
                  sequence,
                  streamVersion: 5,
                  eventId: EventId.make("controller-human-takeover-event"),
                  commandId: CommandId.make("controller-human-takeover-command"),
                  correlationId: CommandId.make("controller-human-takeover-command"),
                  payload: {
                    projectId,
                    previousMode: "run-once",
                    mode: "manual",
                    previousPausedFromMode: null,
                    pausedFromMode: null,
                    changedAt: at,
                  },
                } as const satisfies AgentControlEvent;
                yield* Ref.update(projectEvents, (events) => [...events, event]);
                yield* Ref.set(projectState, {
                  schemaVersion: 1,
                  projectId,
                  mode: "manual",
                  pausedFromMode: null,
                  revision: 5,
                  sequence,
                  updatedAt: at,
                });
              }).pipe(Effect.orDie),
        beforePublication: noop,
        afterPublication: noop,
      });
      const controllerC = yield* makeController(connectionC.sql, {
        afterSubscriptionsBeforeRecovery: Effect.void,
        afterActivationAuthority: noop,
        afterStepCommitted: noop,
        beforePublication: () =>
          Ref.getAndSet(failBeforeWake, false).pipe(
            Effect.flatMap((fail) =>
              fail
                ? Effect.die(new Error("injected crash before publication wakeup"))
                : Effect.void,
            ),
          ),
        afterPublication: noop,
      });
      yield* controllerA.subscribePublications.pipe(
        Effect.flatMap((stream) =>
          Stream.runForEach(stream, () => Ref.update(publicationsA, (count) => count + 1)),
        ),
        Effect.forkScoped,
      );
      yield* controllerB.subscribePublications.pipe(
        Effect.flatMap((stream) =>
          Stream.runForEach(stream, () => Ref.update(publicationsB, (count) => count + 1)),
        ),
        Effect.forkScoped,
      );
      yield* controllerC.subscribePublicationWakeups.pipe(
        Effect.flatMap((stream) =>
          Stream.runForEach(stream, () => Ref.update(publicationsC, (count) => count + 1)),
        ),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      yield* connectionA.sql`
        DELETE FROM main.agent_control_command_receipts
        WHERE command_id = 'controller-run-once-command'
      `;
      const corruptStartup = yield* Effect.exit(
        Effect.scoped(controllerA.prepare(alreadyActivated)),
      );
      assert.isTrue(Exit.isFailure(corruptStartup));
      assert.equal(yield* Ref.get(publicationsA), 0);
      assert.deepStrictEqual(
        yield* connectionB.sql`SELECT count(*) AS count FROM main.agent_control_run_once_states`,
        [{ count: 0 }],
      );
      yield* insertModeReceipt(connectionA.sql, {
        commandId: "controller-run-once-command",
        authority: "human",
        expectedRevision: 1,
        mode: "run-once",
        sequence: activationSequence,
        streamVersion: 2,
      });

      const firstProcess = yield* Effect.exit(controllerA.recover).pipe(Effect.forkChild);
      yield* Deferred.await(firstPublicationEntered);
      assert.equal(yield* Ref.get(publicationHookEntries), 1);
      assert.equal(yield* Ref.get(publicationsA), 1);
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT count(*) AS published FROM main.agent_control_run_once_publications
          WHERE published_at IS NOT NULL
        `,
        [{ published: 1 }],
      );

      const pauseSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-pause-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 3,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-pause-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "run-once",
          mode: "paused",
          previousPausedFromMode: null,
          pausedFromMode: "run-once",
          changedAt: at,
        },
      });
      yield* connectionA.sql`
        UPDATE main.agent_control_project_states
        SET mode = 'paused', paused_from_mode = 'run-once', revision = 3,
          last_event_sequence = ${pauseSequence}, updated_at = ${at}
        WHERE project_id = ${projectId} AND revision = 2
      `;
      const pauseEvent = {
        ...activationEvent,
        sequence: pauseSequence,
        streamVersion: 3,
        eventId: EventId.make("controller-pause-event"),
        commandId: CommandId.make("controller-pause-command"),
        correlationId: CommandId.make("controller-pause-command"),
        payload: {
          projectId,
          previousMode: "run-once",
          mode: "paused",
          previousPausedFromMode: null,
          pausedFromMode: "run-once",
          changedAt: at,
        },
      } as const satisfies AgentControlEvent;
      yield* Ref.update(projectEvents, (events) => [...events, pauseEvent]);
      yield* Ref.set(projectState, {
        schemaVersion: 1,
        projectId,
        mode: "paused",
        pausedFromMode: "run-once",
        revision: 3,
        sequence: pauseSequence,
        updatedAt: at,
      });
      yield* controllerB.processProject(projectId);
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(publicationsB), 0);
      yield* Deferred.succeed(releaseFirstPublication, undefined);
      const interruptedPublication = yield* Fiber.join(firstProcess);
      assert.isTrue(Exit.isFailure(interruptedPublication));
      const firstMeaningfulDelivery = yield* controllerB.pullPublications("test-consumer");
      assert.deepStrictEqual(
        firstMeaningfulDelivery.map(({ ordinal, step }) => ({ ordinal, step })),
        [{ ordinal: 1, step: "activation-admitted" }],
      );
      assert.deepStrictEqual(yield* controllerA.pullPublications("test-consumer"), []);
      yield* controllerB.acknowledgePublication(
        "test-consumer",
        firstMeaningfulDelivery[0]!.publicationId,
      );
      yield* controllerC.recoverPublicationConsumer("test-consumer");
      assert.deepStrictEqual(yield* controllerC.pullPublications("test-consumer"), []);
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT status, next_ordinal AS "nextOrdinal", last_step AS "lastStep"
          FROM main.agent_control_run_once_states
        `,
        [{ status: "active", nextOrdinal: 2, lastStep: "activation-admitted" }],
      );
      assert.equal(yield* Ref.get(publicationsB), 0);

      const resumeSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-resume-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 4,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-resume-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "paused",
          mode: "run-once",
          previousPausedFromMode: "run-once",
          pausedFromMode: null,
          changedAt: at,
        },
      });
      yield* connectionA.sql`
        UPDATE main.agent_control_project_states
        SET mode = 'run-once', paused_from_mode = NULL, revision = 4,
          last_event_sequence = ${resumeSequence}, updated_at = ${at}
        WHERE project_id = ${projectId} AND revision = 3
      `;
      const resumeEvent = {
        ...activationEvent,
        sequence: resumeSequence,
        streamVersion: 4,
        eventId: EventId.make("controller-resume-event"),
        commandId: CommandId.make("controller-resume-command"),
        correlationId: CommandId.make("controller-resume-command"),
        payload: {
          projectId,
          previousMode: "paused",
          mode: "run-once",
          previousPausedFromMode: "run-once",
          pausedFromMode: null,
          changedAt: at,
        },
      } as const satisfies AgentControlEvent;
      yield* Ref.update(projectEvents, (events) => [...events, resumeEvent]);
      yield* Ref.set(projectState, {
        schemaVersion: 1,
        projectId,
        mode: "run-once",
        pausedFromMode: null,
        revision: 4,
        sequence: resumeSequence,
        updatedAt: at,
      });
      yield* controllerB.processProject(projectId);
      yield* Effect.yieldNow;

      assert.equal(yield* Ref.get(publicationsA), 1);
      assert.equal(yield* Ref.get(publicationsB), 3);
      assert.deepStrictEqual(
        yield* connectionB.sql`
        SELECT status, next_ordinal AS "nextOrdinal", last_step AS "lastStep"
        FROM main.agent_control_run_once_states
      `,
        [{ status: "no-eligible-task", nextOrdinal: 5, lastStep: "completed" }],
      );
      assert.isTrue(yield* Ref.get(humanTakeoverDone));
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT mode, paused_from_mode AS "pausedFromMode", revision
          FROM main.agent_control_project_states WHERE project_id = ${projectId}
        `,
        [{ mode: "manual", pausedFromMode: null, revision: 5 }],
      );
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT step FROM main.agent_control_run_once_step_evidence ORDER BY ordinal
        `,
        [
          { step: "activation-admitted" },
          { step: "no-eligible-task" },
          { step: "mode-reset-superseded" },
          { step: "completed" },
        ],
      );
      assert.deepStrictEqual(
        yield* connectionB.sql`
        SELECT count(*) AS activations,
          (SELECT count(*) FROM main.agent_control_run_once_step_markers) AS markers,
          (SELECT count(*) FROM main.agent_control_run_once_publications
           WHERE published_at IS NOT NULL) AS published
        FROM main.agent_control_run_once_activations
      `,
        [{ activations: 1, markers: 4, published: 4 }],
      );
      const remainingFirstRun = yield* controllerB.pullPublications("test-consumer");
      assert.deepStrictEqual(
        remainingFirstRun.map(({ ordinal, step }) => ({ ordinal, step })),
        [
          { ordinal: 2, step: "no-eligible-task" },
          { ordinal: 3, step: "mode-reset-superseded" },
          { ordinal: 4, step: "completed" },
        ],
      );
      yield* Effect.forEach(
        remainingFirstRun,
        (publication) =>
          controllerB.acknowledgePublication("test-consumer", publication.publicationId),
        { discard: true },
      );

      const secondObserveSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-second-observe-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 6,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-second-observe-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "manual",
          mode: "observe",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      });
      yield* insertModeReceipt(connectionA.sql, {
        commandId: "controller-second-observe-command",
        authority: "human",
        expectedRevision: 5,
        mode: "observe",
        sequence: secondObserveSequence,
        streamVersion: 6,
      });
      const secondActivationSequence = yield* insertEvent(connectionA.sql, {
        eventId: "controller-second-run-once-event",
        aggregateKind: "project-controller",
        streamId: projectId,
        streamVersion: 7,
        eventType: "agentControl.project.mode.changed",
        commandId: "controller-second-run-once-command",
        authority: "human",
        payload: {
          projectId,
          previousMode: "observe",
          mode: "run-once",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      });
      yield* insertModeReceipt(connectionA.sql, {
        commandId: "controller-second-run-once-command",
        authority: "human",
        expectedRevision: 6,
        mode: "run-once",
        sequence: secondActivationSequence,
        streamVersion: 7,
      });
      yield* connectionA.sql`
        UPDATE main.agent_control_project_states
        SET mode = 'run-once', paused_from_mode = NULL, revision = 7,
          last_event_sequence = ${secondActivationSequence}, updated_at = ${at}
        WHERE project_id = ${projectId} AND revision = 5
      `;
      const secondObserveEvent = {
        ...observeEvent,
        sequence: secondObserveSequence,
        streamVersion: 6,
        eventId: EventId.make("controller-second-observe-event"),
        commandId: CommandId.make("controller-second-observe-command"),
        correlationId: CommandId.make("controller-second-observe-command"),
        payload: {
          projectId,
          previousMode: "manual",
          mode: "observe",
          previousPausedFromMode: null,
          pausedFromMode: null,
          changedAt: at,
        },
      } as const satisfies AgentControlEvent;
      const secondActivationEvent = {
        ...activationEvent,
        sequence: secondActivationSequence,
        streamVersion: 7,
        eventId: EventId.make("controller-second-run-once-event"),
        commandId: CommandId.make("controller-second-run-once-command"),
        correlationId: CommandId.make("controller-second-run-once-command"),
      } as const satisfies AgentControlEvent;
      yield* Ref.update(projectEvents, (events) => [
        ...events,
        secondObserveEvent,
        secondActivationEvent,
      ]);
      yield* Ref.set(projectState, {
        schemaVersion: 1,
        projectId,
        mode: "run-once",
        pausedFromMode: null,
        revision: 7,
        sequence: secondActivationSequence,
        updatedAt: at,
      });
      yield* Ref.set(loseResetResponse, true);
      const crashedBeforeWake = yield* Effect.exit(controllerC.processProject(projectId));
      assert.isTrue(Exit.isFailure(crashedBeforeWake));
      assert.equal(yield* Ref.get(publicationsC), 0);
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT count(*) AS count FROM main.agent_control_run_once_publications publication
          JOIN main.agent_control_run_once_activations activation
            ON activation.run_id = publication.run_id
          WHERE activation.activation_event_stream_version = 7
            AND publication.published_at IS NOT NULL
        `,
        [{ count: 1 }],
      );
      yield* controllerB.recover;
      yield* Effect.yieldNow;
      const recoveredMeaningful = yield* controllerB.pullPublications("test-consumer");
      assert.deepStrictEqual(
        recoveredMeaningful.map(({ ordinal, step }) => ({ ordinal, step })),
        [
          { ordinal: 1, step: "activation-admitted" },
          { ordinal: 2, step: "no-eligible-task" },
          { ordinal: 3, step: "mode-reset" },
          { ordinal: 4, step: "completed" },
        ],
      );
      yield* Effect.forEach(
        recoveredMeaningful,
        (publication) =>
          controllerB.acknowledgePublication("test-consumer", publication.publicationId),
        { discard: true },
      );
      yield* controllerC.recoverPublicationConsumer("test-consumer");
      assert.deepStrictEqual(yield* controllerC.pullPublications("test-consumer"), []);
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT status, last_step AS "lastStep" FROM main.agent_control_run_once_states
          ORDER BY activation_project_revision
        `,
        [
          { status: "no-eligible-task", lastStep: "completed" },
          { status: "no-eligible-task", lastStep: "completed" },
        ],
      );
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT mode, revision FROM main.agent_control_project_states
          WHERE project_id = ${projectId}
        `,
        [{ mode: "observe", revision: 8 }],
      );
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT typeof(mode_event_payload_json) AS payload,
            typeof(mode_event_metadata_json) AS metadata
          FROM main.agent_control_run_once_step_evidence WHERE step = 'mode-reset'
        `,
        [{ payload: "blob", metadata: "blob" }],
      );
      const changesBefore = (yield* connectionA.sql<{ readonly changes: number }>`
        SELECT total_changes() AS changes
      `)[0]!.changes;
      yield* controllerA.processProject(projectId);
      const changesAfter = (yield* connectionA.sql<{ readonly changes: number }>`
        SELECT total_changes() AS changes
      `)[0]!.changes;
      assert.equal(changesAfter, changesBefore);
      assert.deepStrictEqual(yield* connectionB.sql`PRAGMA foreign_key_check`, []);

      const closedActivation = yield* makeReactorStartupActivation;
      yield* Effect.scoped(controllerA.prepare(closedActivation));
      const activationWaiter = yield* closedActivation.await.pipe(Effect.forkChild);
      assert.equal(activationWaiter.pollUnsafe(), undefined);
      yield* Fiber.interrupt(activationWaiter);

      const createMirroredAuthority = Effect.fn("createMirroredRunOnceAuthority")(function* (
        mirroredProjectId: ProjectId,
        suffix: string,
      ) {
        const mirroredConfigSequence = yield* insertEvent(connectionB.sql, {
          eventId: `parallel-github-config-event-${suffix}`,
          aggregateKind: "github-intake",
          streamId: mirroredProjectId,
          streamVersion: 1,
          eventType: "agentControl.github.config.set",
          commandId: `parallel-github-config-command-${suffix}`,
          authority: "human",
          payload: {
            projectId: mirroredProjectId,
            settings: {
              trackerKind: "github",
              readyLabel: "agent:ready",
              pausedLabel: "agent:paused",
              trustedLogins: [],
              pollIntervalSeconds: 60,
            },
            repository: {
              repositoryNodeId: `parallel-repository-${suffix}`,
              nameWithOwner: `owner/${suffix}`,
            },
            configuredAt: at,
          },
        });
        const mirroredGithubSequence = yield* insertEvent(connectionB.sql, {
          eventId: `parallel-github-success-event-${suffix}`,
          aggregateKind: "github-intake",
          streamId: mirroredProjectId,
          streamVersion: 2,
          eventType: "agentControl.github.poll.succeeded",
          commandId: `parallel-github-success-command-${suffix}`,
          authority: "controller",
          payload: {
            projectId: mirroredProjectId,
            repository: {
              repositoryNodeId: `parallel-repository-${suffix}`,
              nameWithOwner: `owner/${suffix}`,
            },
            attemptedAt: at,
            completedAt: at,
            cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
            issues: [],
          },
        });
        const mirroredObserveSequence = yield* insertEvent(connectionB.sql, {
          eventId: `parallel-observe-event-${suffix}`,
          aggregateKind: "project-controller",
          streamId: mirroredProjectId,
          streamVersion: 1,
          eventType: "agentControl.project.mode.changed",
          commandId: `parallel-observe-command-${suffix}`,
          authority: "human",
          payload: {
            projectId: mirroredProjectId,
            previousMode: "manual",
            mode: "observe",
            previousPausedFromMode: null,
            pausedFromMode: null,
            changedAt: at,
          },
        });
        const mirroredActivationSequence = yield* insertEvent(connectionB.sql, {
          eventId: `parallel-activation-event-${suffix}`,
          aggregateKind: "project-controller",
          streamId: mirroredProjectId,
          streamVersion: 2,
          eventType: "agentControl.project.mode.changed",
          commandId: `parallel-activation-command-${suffix}`,
          authority: "human",
          payload: {
            projectId: mirroredProjectId,
            previousMode: "observe",
            mode: "run-once",
            previousPausedFromMode: null,
            pausedFromMode: null,
            changedAt: at,
          },
        });
        yield* insertModeReceipt(connectionB.sql, {
          projectId: mirroredProjectId,
          commandId: `parallel-activation-command-${suffix}`,
          authority: "human",
          expectedRevision: 1,
          mode: "run-once",
          sequence: mirroredActivationSequence,
          streamVersion: 2,
        });
        yield* connectionB.sql`
          INSERT INTO main.agent_control_project_states (
            project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
          ) VALUES (
            ${mirroredProjectId}, 'run-once', NULL, 2, ${mirroredActivationSequence}, ${at}
          )
        `;
        yield* connectionB.sql`
          INSERT INTO main.agent_control_task_reconcile_states (
            project_id, target_sequence, last_completed_sequence, revision, status, updated_at
          ) VALUES (
            ${mirroredProjectId}, ${mirroredGithubSequence}, ${mirroredGithubSequence},
            1, 'completed', ${at}
          )
        `;
        const mirroredObserveEvent = {
          ...observeEvent,
          aggregateId: mirroredProjectId,
          sequence: mirroredObserveSequence,
          eventId: EventId.make(`parallel-observe-event-${suffix}`),
          commandId: CommandId.make(`parallel-observe-command-${suffix}`),
          correlationId: CommandId.make(`parallel-observe-command-${suffix}`),
          payload: { ...observeEvent.payload, projectId: mirroredProjectId },
        } as const satisfies AgentControlEvent;
        const mirroredActivationEvent = {
          ...activationEvent,
          aggregateId: mirroredProjectId,
          sequence: mirroredActivationSequence,
          eventId: EventId.make(`parallel-activation-event-${suffix}`),
          commandId: CommandId.make(`parallel-activation-command-${suffix}`),
          correlationId: CommandId.make(`parallel-activation-command-${suffix}`),
          payload: { ...activationEvent.payload, projectId: mirroredProjectId },
        } as const satisfies AgentControlEvent;
        const mirroredConfigEvent = {
          ...configEvent,
          aggregateId: mirroredProjectId,
          sequence: mirroredConfigSequence,
          eventId: EventId.make(`parallel-github-config-event-${suffix}`),
          commandId: CommandId.make(`parallel-github-config-command-${suffix}`),
          correlationId: CommandId.make(`parallel-github-config-command-${suffix}`),
          payload: {
            ...configEvent.payload,
            projectId: mirroredProjectId,
            repository: {
              repositoryNodeId: `parallel-repository-${suffix}`,
              nameWithOwner: `owner/${suffix}`,
            },
          },
        } as const satisfies AgentControlGithubEvent;
        const mirroredSuccessEvent = {
          ...successEvent,
          aggregateId: mirroredProjectId,
          sequence: mirroredGithubSequence,
          eventId: EventId.make(`parallel-github-success-event-${suffix}`),
          commandId: CommandId.make(`parallel-github-success-command-${suffix}`),
          correlationId: CommandId.make(`parallel-github-success-command-${suffix}`),
          payload: {
            ...successEvent.payload,
            projectId: mirroredProjectId,
            repository: {
              repositoryNodeId: `parallel-repository-${suffix}`,
              nameWithOwner: `owner/${suffix}`,
            },
          },
        } as const satisfies AgentControlGithubEvent;
        const mirroredGithubState = yield* projectGithubIntakeEvent(
          yield* projectGithubIntakeEvent(
            createDefaultGithubIntakeState(mirroredProjectId),
            mirroredConfigEvent,
          ),
          mirroredSuccessEvent,
        );
        return {
          projectEvents: [mirroredObserveEvent, mirroredActivationEvent],
          projectState: {
            schemaVersion: 1,
            projectId: mirroredProjectId,
            mode: "run-once",
            pausedFromMode: null,
            revision: 2,
            sequence: mirroredActivationSequence,
            updatedAt: at,
          } as const satisfies AgentControlProjectState,
          githubEvents: [mirroredConfigEvent, mirroredSuccessEvent],
          githubState: mirroredGithubState,
          githubSequence: mirroredGithubSequence,
          repositoryNodeId: `parallel-repository-${suffix}`,
        } satisfies MirroredAuthority;
      });

      const parallelProjectA = ProjectId.make("run-once-parallel-project-a");
      const parallelProjectB = ProjectId.make("run-once-parallel-project-b");
      const mirroredA = yield* createMirroredAuthority(parallelProjectA, "a");
      const mirroredB = yield* createMirroredAuthority(parallelProjectB, "b");
      const parallelAEntered = yield* Deferred.make<void>();
      const parallelBEntered = yield* Deferred.make<void>();
      const holdParallelProjects = yield* Deferred.make<void>();
      const parallelController = yield* makeController(
        connectionB.sql,
        {
          afterSubscriptionsBeforeRecovery: Effect.void,
          afterActivationAuthority: (observation) =>
            observation.projectId === parallelProjectA
              ? Deferred.succeed(parallelAEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(holdParallelProjects)),
                )
              : observation.projectId === parallelProjectB
                ? Deferred.succeed(parallelBEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(holdParallelProjects)),
                  )
                : Effect.void,
          afterStepCommitted: noop,
          beforePublication: noop,
          afterPublication: noop,
        },
        {
          mirrors: new Map([
            [parallelProjectA, mirroredA],
            [parallelProjectB, mirroredB],
          ]),
        },
      );
      const parallelRecovery = yield* parallelController.recover.pipe(Effect.forkChild);
      yield* Effect.raceFirst(Deferred.await(parallelAEntered), Fiber.join(parallelRecovery));
      yield* Effect.raceFirst(Deferred.await(parallelBEntered), Fiber.join(parallelRecovery));
      assert.deepStrictEqual(
        yield* connectionB.sql`
          SELECT project_id AS "projectId", last_step AS "lastStep"
          FROM main.agent_control_run_once_states
          WHERE project_id IN (${parallelProjectA}, ${parallelProjectB})
          ORDER BY project_id
        `,
        [
          { projectId: parallelProjectA, lastStep: "activation-admitted" },
          { projectId: parallelProjectB, lastStep: "activation-admitted" },
        ],
      );
      yield* Fiber.interrupt(parallelRecovery);

      const closedConnection = yield* openDatabase(filename);
      yield* Scope.close(closedConnection.scope, Exit.void);
      const sqlStartupFailure = yield* Effect.exit(
        Effect.scoped(
          makeController(closedConnection.sql, {
            afterSubscriptionsBeforeRecovery: Effect.void,
            afterActivationAuthority: noop,
            afterStepCommitted: noop,
            beforePublication: noop,
            afterPublication: noop,
          }).pipe(Effect.flatMap((controller) => controller.prepare(alreadyActivated))),
        ),
      );
      assert.isTrue(Exit.isFailure(sqlStartupFailure));

      yield* connectionB.sql`DROP TRIGGER main.agent_control_run_once_state_update_validate`;
      yield* connectionB.sql`
        UPDATE main.agent_control_run_once_states
        SET last_step = 'activation-admitted'
        WHERE project_id = ${projectId} AND activation_project_revision = 2
      `;
      const statesBeforeCorruptStartup = yield* connectionB.sql`
        SELECT count(*) AS count FROM main.agent_control_run_once_states
      `;
      const corruptController = yield* makeController(connectionB.sql, {
        afterSubscriptionsBeforeRecovery: Effect.void,
        afterActivationAuthority: noop,
        afterStepCommitted: noop,
        beforePublication: noop,
        afterPublication: noop,
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(Effect.scoped(corruptController.prepare(alreadyActivated))),
        ),
      );
      assert.deepStrictEqual(
        yield* connectionB.sql`SELECT count(*) AS count FROM main.agent_control_run_once_states`,
        statesBeforeCorruptStartup,
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
