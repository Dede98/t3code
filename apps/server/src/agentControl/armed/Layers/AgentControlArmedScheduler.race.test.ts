import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlControlledThreadActivation } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import { AgentControlGithubEventStore } from "../../github/Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubProjection } from "../../github/Services/AgentControlGithubProjection.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlRunOnceControllerLive } from "../../runOnce/Layers/AgentControlRunOnceController.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntakeReactor } from "../../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskReconcileStateRepository } from "../../task/Services/AgentControlTaskReconcileState.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
import { AgentControlArmedScheduler } from "../Services/AgentControlArmedScheduler.ts";
import {
  layer as AgentControlArmedSchedulerLive,
  make as makeAgentControlArmedScheduler,
  type AgentControlArmedSchedulerOptions,
} from "./AgentControlArmedScheduler.ts";

const at = "2026-09-02T08:00:00.000Z";
const projectId = ProjectId.make("armed-two-production-layer-race");
const repository = {
  repositoryNodeId: "armed-production-race-repository",
  nameWithOwner: "owner/repository",
} as const;

const issue = (number: number): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: `armed-production-race-issue-${number}`,
  number,
  url: `https://example.test/owner/repository/issues/${number}`,
  state: "open",
  title: `Armed production race ${number}`,
  body: null,
  contentTrust: "untrusted-external",
  updatedAt: at,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
});

const taskSource = (
  source: AgentControlGithubIssueSnapshot,
  selectedProjectId: ProjectId = projectId,
) => ({
  projectId: selectedProjectId,
  repositoryNodeId: source.repositoryNodeId,
  issueNodeId: source.issueNodeId,
  issueNumber: source.number,
  issueUrl: source.url,
});

const buildProductionScheduler = Effect.fn("buildProductionArmedScheduler")(function* (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  options?: AgentControlArmedSchedulerOptions,
) {
  const unavailable = (operation: string) => Effect.die(new Error(`${operation} not expected`));
  const runtime = Layer.fresh(AgentControlRuntimeLayerLive).pipe(
    Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provideMerge(NodeServices.layer),
  );
  const runOnce = Layer.fresh(AgentControlRunOnceControllerLive).pipe(
    Layer.provideMerge(runtime),
    Layer.provideMerge(
      Layer.mock(AgentControlWorktreeController)({
        reserveAndMaterialize: () => unavailable("worktree materialization"),
        reserveAndMaterializeForRunOnce: () => unavailable("run-once worktree materialization"),
        reconcile: () => unavailable("worktree reconcile"),
        useReadyWorktree: () => unavailable("worktree use"),
        useReadyWorktreeForRunOnce: () => unavailable("run-once worktree use"),
      }),
    ),
    Layer.provideMerge(
      Layer.mock(AgentControlControlledThreadActivation)({
        activateInitial: () => unavailable("thread activation"),
        activateInitialForRunOnce: () => unavailable("run-once thread activation"),
      }),
    ),
  );
  const intake = Layer.succeed(
    AgentControlTaskIntakeReactor,
    AgentControlTaskIntakeReactor.of({
      start: () => Effect.void,
      getStatus: () => unavailable("task intake status"),
      subscribeCompletions: Effect.succeed(Stream.empty),
    }),
  );
  const armed = Layer.fresh(
    options === undefined
      ? AgentControlArmedSchedulerLive
      : Layer.effect(AgentControlArmedScheduler, makeAgentControlArmedScheduler(options)),
  ).pipe(
    Layer.provideMerge(runOnce),
    Layer.provideMerge(runtime),
    Layer.provideMerge(intake),
    Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provideMerge(NodeServices.layer),
  );
  return yield* Layer.buildWithScope(armed, scope);
});

const runClaimEpochRace = Effect.fn("runArmedClaimEpochRace")(function* (
  kind: "source-epoch" | "earlier-candidate",
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const raceProjectId = ProjectId.make(`armed-claim-${kind}`);
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: `t3-armed-${kind}-` });
  const filename = path.join(directory, "state.sqlite");
  const scopeA = yield* Scope.make("sequential");
  const scopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
  yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
  const sqlA = Context.get(
    yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA),
    SqlClient.SqlClient,
  );
  const sqlB = Context.get(
    yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB),
    SqlClient.SqlClient,
  );
  for (const sql of [sqlA, sqlB]) {
    yield* sql`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
  }
  yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
    Effect.provideService(SqlClient.SqlClient, sqlA),
  );
  const claimReached = yield* Deferred.make<void>();
  const releaseClaim = yield* Deferred.make<void>();
  const contextA = yield* buildProductionScheduler(sqlA, scopeA, {
    hooks: {
      afterClaim: () =>
        Deferred.succeed(claimReached, undefined).pipe(
          Effect.andThen(Deferred.await(releaseClaim)),
        ),
    },
  });
  const contextB = yield* buildProductionScheduler(sqlB, scopeB);
  const engine = Context.get(contextA, AgentControlEngine);
  const githubEvents = Context.get(contextA, AgentControlGithubEventStore);
  const githubProjection = Context.get(contextA, AgentControlGithubProjection);
  const githubStates = Context.get(contextA, AgentControlGithubStateRepository);
  const taskEngineA = Context.get(contextA, AgentControlTaskEngine);
  const taskEngineB = Context.get(contextB, AgentControlTaskEngine);
  const reconcilesA = Context.get(contextA, AgentControlTaskReconcileStateRepository);
  const reconcilesB = Context.get(contextB, AgentControlTaskReconcileStateRepository);
  const laterIssue = issue(2);
  const earlierIssue = issue(1);
  const sourceIssues = kind === "earlier-candidate" ? [earlierIssue, laterIssue] : [laterIssue];

  yield* sqlA`
    INSERT INTO main.projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (${raceProjectId}, 'Armed claim epoch race', '/tmp', NULL, '[]', ${at}, ${at}, NULL)
  `;
  yield* engine.dispatchHuman({
    commandId: CommandId.make(`armed-${kind}-observe`),
    projectId: raceProjectId,
    expectedRevision: 0,
    mode: "observe",
  });
  yield* engine.dispatchHuman({
    commandId: CommandId.make(`armed-${kind}-arm`),
    projectId: raceProjectId,
    expectedRevision: 1,
    mode: "armed",
  });
  const configured = yield* githubEvents.append({
    projectId: raceProjectId,
    expectedStreamVersion: 0,
    events: [
      {
        eventId: EventId.make(`armed-${kind}-config-event`),
        type: "agentControl.github.config.set",
        aggregateKind: "github-intake",
        aggregateId: raceProjectId,
        occurredAt: at,
        commandId: CommandId.make(`armed-${kind}-config-command`),
        causationEventId: null,
        correlationId: CommandId.make(`armed-${kind}-config-command`),
        authority: "human",
        payload: {
          projectId: raceProjectId,
          settings: {
            trackerKind: "github",
            readyLabel: "agent:ready",
            pausedLabel: "agent:paused",
            trustedLogins: [],
            pollIntervalSeconds: 60,
          },
          repository,
          configuredAt: at,
        },
        metadata: { schemaVersion: 1 },
      },
    ],
  });
  yield* githubProjection.projectEvent(configured[0]!);
  const polled = yield* githubEvents.append({
    projectId: raceProjectId,
    expectedStreamVersion: 1,
    events: [
      {
        eventId: EventId.make(`armed-${kind}-poll-event`),
        type: "agentControl.github.poll.succeeded",
        aggregateKind: "github-intake",
        aggregateId: raceProjectId,
        occurredAt: at,
        commandId: CommandId.make(`armed-${kind}-poll-command`),
        causationEventId: null,
        correlationId: CommandId.make(`armed-${kind}-poll-command`),
        authority: "controller",
        payload: {
          projectId: raceProjectId,
          repository,
          attemptedAt: at,
          completedAt: at,
          cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
          issues: sourceIssues,
        },
        metadata: { schemaVersion: 1 },
      },
    ],
  });
  yield* githubProjection.projectEvent(polled[0]!);
  const snapshot = Option.getOrThrow(yield* githubStates.getCompletedSnapshot(raceProjectId));
  const laterTaskId = yield* deriveAgentControlTaskId({
    projectId: raceProjectId,
    repositoryNodeId: laterIssue.repositoryNodeId,
    issueNodeId: laterIssue.issueNodeId,
  });
  yield* taskEngineA.dispatchObservedController({
    type: "agentControl.task.createFromGithubIssue",
    commandId: CommandId.make(`armed-${kind}-later-task`),
    taskId: laterTaskId,
    projectId: raceProjectId,
    expectedRevision: 0,
    sourcePrecondition: snapshot.sourcePrecondition,
    source: taskSource(laterIssue, raceProjectId),
    sourceGate: "eligible",
    sourceUpdatedAt: laterIssue.updatedAt,
    githubIntakeSequence: polled[0]!.sequence,
    sourceSnapshot: laterIssue,
  });
  const reconciling = yield* reconcilesA.begin(raceProjectId, polled[0]!.sequence, at);
  yield* reconcilesA.complete(raceProjectId, polled[0]!.sequence, reconciling.revision, at);

  const processing = yield* Effect.exit(
    Context.get(contextA, AgentControlArmedScheduler).processProject(raceProjectId),
  ).pipe(Effect.forkChild({ startImmediately: true }));
  yield* Deferred.await(claimReached);
  if (kind === "source-epoch") {
    const nextPoll = yield* Context.get(contextB, AgentControlGithubEventStore).append({
      projectId: raceProjectId,
      expectedStreamVersion: 2,
      events: [
        {
          eventId: EventId.make("armed-source-epoch-next-poll-event"),
          type: "agentControl.github.poll.succeeded",
          aggregateKind: "github-intake",
          aggregateId: raceProjectId,
          occurredAt: at,
          commandId: CommandId.make("armed-source-epoch-next-poll-command"),
          causationEventId: null,
          correlationId: CommandId.make("armed-source-epoch-next-poll-command"),
          authority: "controller",
          payload: {
            projectId: raceProjectId,
            repository,
            attemptedAt: at,
            completedAt: at,
            cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
            issues: [laterIssue],
          },
          metadata: { schemaVersion: 1 },
        },
      ],
    });
    yield* Context.get(contextB, AgentControlGithubProjection).projectEvent(nextPoll[0]!);
    const nextReconciling = yield* reconcilesB.begin(raceProjectId, nextPoll[0]!.sequence, at);
    yield* reconcilesB.complete(raceProjectId, nextPoll[0]!.sequence, nextReconciling.revision, at);
  } else {
    const earlierTaskId = yield* deriveAgentControlTaskId({
      projectId: raceProjectId,
      repositoryNodeId: earlierIssue.repositoryNodeId,
      issueNodeId: earlierIssue.issueNodeId,
    });
    yield* taskEngineB.dispatchObservedController({
      type: "agentControl.task.createFromGithubIssue",
      commandId: CommandId.make("armed-earlier-candidate-task"),
      taskId: earlierTaskId,
      projectId: raceProjectId,
      expectedRevision: 0,
      sourcePrecondition: snapshot.sourcePrecondition,
      source: taskSource(earlierIssue, raceProjectId),
      sourceGate: "eligible",
      sourceUpdatedAt: earlierIssue.updatedAt,
      githubIntakeSequence: polled[0]!.sequence,
      sourceSnapshot: earlierIssue,
    });
  }
  yield* Deferred.succeed(releaseClaim, undefined);
  const result = yield* Fiber.join(processing);
  assert.isTrue(Exit.isFailure(result));
  if (Exit.isFailure(result)) {
    const failure = Cause.squash(result.cause);
    assert.equal((failure as { readonly reason?: string }).reason, "source-watermark-stale");
  }
  assert.deepStrictEqual(
    yield* sqlB`
      SELECT project.mode,
        (SELECT count(*) FROM main.agent_control_events event
         WHERE event.aggregate_kind = 'project-controller'
           AND event.stream_id = ${raceProjectId}
           AND event.actor_authority = 'system') AS systemModeEvents,
        (SELECT count(*) FROM main.agent_control_run_once_states run
         WHERE run.project_id = ${raceProjectId}) AS runs
      FROM main.agent_control_project_states project
      WHERE project.project_id = ${raceProjectId}
    `,
    [{ mode: "armed", systemModeEvents: 0, runs: 0 }],
  );
});

it.live(
  "rejects a claimed dispatch when the source epoch advances before activation",
  () => Effect.scoped(runClaimEpochRace("source-epoch")).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "rejects a claimed dispatch when an earlier candidate arrives before activation",
  () =>
    Effect.scoped(runClaimEpochRace("earlier-candidate")).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.live(
  "admits exactly one dispatch and Run-Once across two native WAL production layers",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-layer-race-" });
        const filename = path.join(directory, "state.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const scopeB = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
        const sqlContextA = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          scopeA,
        );
        const sqlContextB = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          scopeB,
        );
        const sqlA = Context.get(sqlContextA, SqlClient.SqlClient);
        const sqlB = Context.get(sqlContextB, SqlClient.SqlClient);
        for (const sql of [sqlA, sqlB]) {
          yield* sql`PRAGMA journal_mode = WAL`;
          yield* sql`PRAGMA foreign_keys = ON`;
        }
        yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        const contextA = yield* buildProductionScheduler(sqlA, scopeA);
        const contextB = yield* buildProductionScheduler(sqlB, scopeB);
        const engine = Context.get(contextA, AgentControlEngine);
        const githubEvents = Context.get(contextA, AgentControlGithubEventStore);
        const githubProjection = Context.get(contextA, AgentControlGithubProjection);
        const githubStates = Context.get(contextA, AgentControlGithubStateRepository);
        const taskEngine = Context.get(contextA, AgentControlTaskEngine);
        const reconciles = Context.get(contextA, AgentControlTaskReconcileStateRepository);

        yield* sqlA`
          INSERT INTO main.projection_projects (
            project_id, title, workspace_root, default_model_selection_json,
            scripts_json, created_at, updated_at, deleted_at
          ) VALUES (${projectId}, 'Armed layer race', '/tmp', NULL, '[]', ${at}, ${at}, NULL)
        `;
        yield* engine.dispatchHuman({
          commandId: CommandId.make("armed-layer-race-observe"),
          projectId,
          expectedRevision: 0,
          mode: "observe",
        });
        yield* engine.dispatchHuman({
          commandId: CommandId.make("armed-layer-race-arm"),
          projectId,
          expectedRevision: 1,
          mode: "armed",
        });
        const configured = yield* githubEvents.append({
          projectId,
          expectedStreamVersion: 0,
          events: [
            {
              eventId: EventId.make("armed-layer-race-config-event"),
              type: "agentControl.github.config.set",
              aggregateKind: "github-intake",
              aggregateId: projectId,
              occurredAt: at,
              commandId: CommandId.make("armed-layer-race-config-command"),
              causationEventId: null,
              correlationId: CommandId.make("armed-layer-race-config-command"),
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
                repository,
                configuredAt: at,
              },
              metadata: { schemaVersion: 1 },
            },
          ],
        });
        yield* githubProjection.projectEvent(configured[0]!);
        const issues = [issue(1), issue(2)];
        const polled = yield* githubEvents.append({
          projectId,
          expectedStreamVersion: 1,
          events: [
            {
              eventId: EventId.make("armed-layer-race-poll-event"),
              type: "agentControl.github.poll.succeeded",
              aggregateKind: "github-intake",
              aggregateId: projectId,
              occurredAt: at,
              commandId: CommandId.make("armed-layer-race-poll-command"),
              causationEventId: null,
              correlationId: CommandId.make("armed-layer-race-poll-command"),
              authority: "controller",
              payload: {
                projectId,
                repository,
                attemptedAt: at,
                completedAt: at,
                cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
                issues,
              },
              metadata: { schemaVersion: 1 },
            },
          ],
        });
        yield* githubProjection.projectEvent(polled[0]!);
        const snapshot = Option.getOrThrow(yield* githubStates.getCompletedSnapshot(projectId));
        for (const source of issues) {
          const taskId = yield* deriveAgentControlTaskId({
            projectId,
            repositoryNodeId: source.repositoryNodeId,
            issueNodeId: source.issueNodeId,
          });
          yield* taskEngine.dispatchObservedController({
            type: "agentControl.task.createFromGithubIssue",
            commandId: CommandId.make(`armed-layer-race-task-${source.number}`),
            taskId,
            projectId,
            expectedRevision: 0,
            sourcePrecondition: snapshot.sourcePrecondition,
            source: taskSource(source),
            sourceGate: "eligible",
            sourceUpdatedAt: source.updatedAt,
            githubIntakeSequence: polled[0]!.sequence,
            sourceSnapshot: source,
          });
        }
        const reconciling = yield* reconciles.begin(projectId, polled[0]!.sequence, at);
        yield* reconciles.complete(projectId, polled[0]!.sequence, reconciling.revision, at);

        const schedulerA = Context.get(contextA, AgentControlArmedScheduler);
        const schedulerB = Context.get(contextB, AgentControlArmedScheduler);
        const exits = yield* Effect.all(
          [
            Effect.exit(schedulerA.processProject(projectId)),
            Effect.exit(schedulerB.processProject(projectId)),
          ],
          { concurrency: "unbounded" },
        );
        assert.isTrue(exits.some(Exit.isFailure));
        const firstTaskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: issues[0]!.repositoryNodeId,
          issueNodeId: issues[0]!.issueNodeId,
        });
        assert.deepStrictEqual(
          yield* sqlA`
            SELECT
              (SELECT count(*) FROM main.agent_control_armed_dispatch_evidence) AS dispatches,
              (SELECT count(*) FROM main.agent_control_events
               WHERE aggregate_kind = 'project-controller' AND actor_authority = 'system'
                 AND event_type = 'agentControl.project.mode.changed') AS systemModeEvents,
              (SELECT count(*) FROM main.agent_control_run_once_activations) AS activations,
              (SELECT count(*) FROM main.agent_control_run_once_states) AS runs,
              (SELECT selected_task_id FROM main.agent_control_armed_dispatch_evidence) AS selectedTask,
              (SELECT count(*) FROM main.agent_control_task_states
               WHERE project_id = ${projectId} AND status = 'candidate'
                 AND source_gate = 'eligible') AS eligibleTasks
          `,
          [
            {
              dispatches: 1,
              systemModeEvents: 1,
              activations: 1,
              runs: 1,
              selectedTask: firstTaskId,
              eligibleTasks: 2,
            },
          ],
        );
        assert.deepStrictEqual(yield* sqlA`PRAGMA main.foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlA`PRAGMA main.integrity_check`, [
          { integrity_check: "ok" },
        ]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);
