import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlStageRunLeaseState,
  AgentControlStageRunState,
  AgentControlTaskId,
  CommandId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlStageRunLeaseId,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlRuntimeLayerLive,
  AgentControlStageRunLeaseEngineLayerLive,
} from "../../runtimeLayer.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "../identity.ts";
import { AgentControlStageRunLeaseEngine } from "../Services/AgentControlStageRunLeaseEngine.ts";
import {
  AgentControlStageRunLeaseTransactionHooks,
  type AgentControlStageRunLeaseTransactionHooksShape,
  type AgentControlStageRunLeaseTransactionObservation,
} from "../Services/AgentControlStageRunLeaseTransactionHooks.ts";

const at = "2026-07-24T10:00:00.000Z";
const barrierTimeout = "3 seconds";
const repository = {
  repositoryNodeId: "lease-race-repository-node",
  nameWithOwner: "owner/repository",
} as const;
const decodeLeaseStateJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLeaseState),
);

const issue = (projectId: ProjectId): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: `issue-${projectId}`,
  number: 1,
  url: `https://example.test/${projectId}/issues/1`,
  state: "open",
  title: "untrusted title",
  body: "untrusted body",
  contentTrust: "untrusted-external",
  updatedAt: at,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
});

const taskFrom = (
  projectId: ProjectId,
  source: AgentControlGithubIssueSnapshot,
): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId: AgentControlTaskId.make(`task-${projectId}`),
  source: {
    projectId,
    repositoryNodeId: source.repositoryNodeId,
    issueNodeId: source.issueNodeId,
    issueNumber: source.number,
    issueUrl: source.url,
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: source.updatedAt,
  githubIntakeSequence: 1,
  sourceSnapshot: {
    repositoryNodeId: source.repositoryNodeId,
    issueNodeId: source.issueNodeId,
    number: source.number,
    url: source.url,
    state: source.state,
    title: source.title,
    body: source.body,
    contentTrust: "untrusted-external",
    updatedAt: source.updatedAt,
    timelineComplete: source.timelineComplete,
    ready: source.ready,
    paused: source.paused,
    eligible: source.eligible,
    eligibilityReason: source.eligibilityReason,
  },
  createdAt: at,
  updatedAt: at,
  revision: 1,
  sequence: 1,
});

const seedPrepared = Effect.fn("seedPreparedStageRunLeaseRace")(function* (projectId: ProjectId) {
  const sql = yield* SqlClient.SqlClient;
  const github = yield* AgentControlGithubStateRepository;
  const taskEngine = yield* AgentControlTaskEngine;
  const stageRuns = yield* AgentControlStageRun;
  const source = issue(projectId);
  const task = {
    ...taskFrom(projectId, source),
    taskId: yield* deriveAgentControlTaskId({
      projectId,
      repositoryNodeId: source.repositoryNodeId,
      issueNodeId: source.issueNodeId,
    }),
  };

  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Lease race test', ${`/tmp/${projectId}`}, NULL, '[]',
      ${at}, ${at}, NULL
    )
  `;
  yield* sql`
    INSERT INTO agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'observe', NULL, 1, 1, ${at})
  `;
  yield* github.save(
    {
      schemaVersion: 1,
      projectId,
      config: {
        schemaVersion: 1,
        projectId,
        settings: {
          trackerKind: "github",
          readyLabel: "agent:ready",
          pausedLabel: "agent:paused",
          trustedLogins: ["trusted"],
          pollIntervalSeconds: 60,
        },
        repository,
        revision: 1,
        sequence: 1,
        updatedAt: at,
      },
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
      pollStatus: {
        status: "success",
        attemptedAt: at,
        completedAt: at,
        errorCode: null,
        issueCount: 1,
      },
      revision: 1,
      sequence: 1,
      updatedAt: at,
    },
    0,
  );
  yield* github.replaceIssues(projectId, [source]);
  const authoritativeTask = (yield* taskEngine.dispatchObservedController({
    type: "agentControl.task.createFromGithubIssue",
    commandId: CommandId.make(`task-create-${projectId}`),
    taskId: task.taskId,
    projectId,
    expectedRevision: 0,
    sourcePrecondition: {
      schemaVersion: 1,
      projectId,
      githubIntakeSequence: 1,
      githubProjectionRevision: 1,
      githubConfigRevision: 1,
      repositoryNodeId: repository.repositoryNodeId,
      pollStatus: "success",
      expectedIssueCount: 1,
    },
    source: task.source,
    sourceGate: task.sourceGate,
    sourceUpdatedAt: task.sourceUpdatedAt,
    githubIntakeSequence: task.githubIntakeSequence,
    sourceSnapshot: task.sourceSnapshot,
  })).state;
  yield* sql`
    INSERT INTO agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence,
      revision, status, updated_at
    ) VALUES (${projectId}, 1, 1, 1, 'completed', ${at})
  `;
  const prepared = yield* stageRuns.prepareInitial({
    commandId: CommandId.make(`prepare-${projectId}`),
    projectId,
    taskId: authoritativeTask.taskId,
  });
  return { task: authoritativeTask, stageRun: prepared.state };
});

const resolvedReserveInput = Effect.fn("resolvedReserveInputRace")(function* (
  stageRun: AgentControlStageRunState,
  commandId: string,
  expectedRevision: number,
  fenceToken: number,
) {
  return {
    type: "agentControl.stageRunLease.reserve" as const,
    commandId: CommandId.make(commandId),
    leaseId: yield* deriveAgentControlStageRunLeaseId({
      projectId: stageRun.projectId,
      taskId: stageRun.taskId,
    }),
    projectId: stageRun.projectId,
    taskId: stageRun.taskId,
    stageRunId: stageRun.stageRunId,
    attemptId: stageRun.attemptId,
    taskRevision: stageRun.taskRevision,
    githubIntakeSequence: stageRun.githubIntakeSequence,
    sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
    fenceToken,
    expectedRevision,
    leaseDurationMs: 60_000,
  };
});

const commandSnapshot = (stageRun: AgentControlStageRunState) => ({
  taskRevision: stageRun.taskRevision,
  githubIntakeSequence: stageRun.githubIntakeSequence,
  sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
});

type HookPhase = "afterAuthoritativeRead" | "beforeAppend" | "beforeTransactionComplete";
interface HookRecord extends AgentControlStageRunLeaseTransactionObservation {
  readonly engine: "A" | "B";
  readonly phase: HookPhase;
}
interface HookGate {
  readonly arrived: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}
interface HookController {
  readonly hooks: AgentControlStageRunLeaseTransactionHooksShape;
  readonly addGate: (commandId: CommandId, phase: HookPhase) => Effect.Effect<HookGate>;
  readonly records: Ref.Ref<ReadonlyArray<HookRecord>>;
}

const gateKey = (commandId: CommandId, phase: HookPhase) => `${commandId}:${phase}`;

const makeHookController = Effect.fn("makeLeaseRaceHookController")(function* (engine: "A" | "B") {
  const records = yield* Ref.make<ReadonlyArray<HookRecord>>([]);
  const gates = new Map<string, HookGate>();

  const addGate = Effect.fn("addLeaseRaceHookGate")(function* (
    commandId: CommandId,
    phase: HookPhase,
  ) {
    const gate = {
      arrived: yield* Deferred.make<void>(),
      release: yield* Deferred.make<void>(),
    };
    gates.set(gateKey(commandId, phase), gate);
    return gate;
  });

  const hook =
    (phase: HookPhase) => (observation: AgentControlStageRunLeaseTransactionObservation) =>
      Effect.gen(function* () {
        yield* Ref.update(records, (current) => [...current, { ...observation, engine, phase }]);
        const gate = gates.get(gateKey(observation.commandId, phase));
        if (gate === undefined) return;
        yield* Deferred.succeed(gate.arrived, undefined).pipe(Effect.ignore);
        yield* Deferred.await(gate.release);
      });

  return {
    hooks: {
      afterAuthoritativeRead: hook("afterAuthoritativeRead"),
      beforeAppend: hook("beforeAppend"),
      beforeTransactionComplete: hook("beforeTransactionComplete"),
    } satisfies AgentControlStageRunLeaseTransactionHooksShape,
    addGate,
    records,
  } satisfies HookController;
});

const awaitGate = (gate: HookGate) =>
  Deferred.await(gate.arrived).pipe(Effect.timeout(barrierTimeout));
const releaseGate = (gate: HookGate) =>
  Deferred.succeed(gate.release, undefined).pipe(Effect.asVoid);
const joinWithin = <A, E>(fiber: Fiber.Fiber<A, E>) =>
  Fiber.join(fiber).pipe(Effect.timeout(barrierTimeout));

const observation = Effect.fn("leaseRaceObservation")(function* (
  controller: HookController,
  commandId: CommandId,
  phase: HookPhase,
) {
  const records = yield* Ref.get(controller.records);
  return records.filter((record) => record.commandId === commandId && record.phase === phase);
});

interface SharedSqliteClients {
  readonly dbPath: string;
  readonly sqlA: SqlClient.SqlClient;
  readonly sqlB: SqlClient.SqlClient;
}

const makeSharedSqliteClients = Effect.fn("makeSharedLeaseRaceSqliteClients")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-lease-race-" });
  const dbPath = path.join(tempDir, "state.sqlite");
  const scopeA = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
  const scopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));

  const contextA = yield* Layer.buildWithScope(
    NodeSqliteClient.layer({ filename: dbPath }),
    scopeA,
  );
  const contextB = yield* Layer.buildWithScope(
    NodeSqliteClient.layer({ filename: dbPath }),
    scopeB,
  );
  const sqlA = Context.get(contextA, SqlClient.SqlClient);
  const sqlB = Context.get(contextB, SqlClient.SqlClient);

  // This is the production setup: WAL plus foreign keys on each independent
  // connection. withTransaction therefore opens DEFERRED transactions, and a
  // reader that upgrades after another writer commits receives BUSY_SNAPSHOT.
  for (const sql of [sqlA, sqlB]) {
    const journal = yield* sql<{ readonly journal_mode: string }>`
      PRAGMA journal_mode = WAL
    `;
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.equal(journal[0]?.journal_mode, "wal");
  }

  const [databaseA] = yield* sqlA<{ readonly file: string }>`PRAGMA database_list`;
  const [databaseB] = yield* sqlB<{ readonly file: string }>`PRAGMA database_list`;
  const canonicalDbPath = yield* fs.realPath(dbPath);
  assert.equal(databaseA?.file, canonicalDbPath);
  assert.equal(databaseB?.file, canonicalDbPath);
  assert.notStrictEqual(sqlA, sqlB);

  return { dbPath, sqlA, sqlB } satisfies SharedSqliteClients;
});

const buildLeaseEngineContext = (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  hooks: AgentControlStageRunLeaseTransactionHooksShape,
) => {
  const engineLayer = Layer.fresh(AgentControlStageRunLeaseEngineLayerLive).pipe(
    Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provideMerge(NodeServices.layer),
  );
  return Layer.buildWithScope(engineLayer, scope).pipe(
    Effect.provideService(AgentControlStageRunLeaseTransactionHooks, hooks),
  );
};

const makeLeaseRaceHarness = Effect.fn("makeLeaseRaceHarness")(function* (
  projectId: ProjectId,
  controllerA: HookController,
  controllerB: HookController,
) {
  const shared = yield* makeSharedSqliteClients();
  yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, shared.sqlA));

  const runtimeScopeA = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(runtimeScopeA, Exit.void));
  const runtimeLayerA = AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, shared.sqlA)),
    Layer.provideMerge(NodeServices.layer),
  );
  const contextA = yield* Layer.buildWithScope(runtimeLayerA, runtimeScopeA).pipe(
    Effect.provideService(AgentControlStageRunLeaseTransactionHooks, controllerA.hooks),
  );
  const seeded = yield* seedPrepared(projectId).pipe(Effect.provide(contextA));

  // Bootstrap B only after migrations and all seed projections completed.
  const runtimeScopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(runtimeScopeB, Exit.void));
  const contextB = yield* buildLeaseEngineContext(shared.sqlB, runtimeScopeB, controllerB.hooks);

  return {
    ...shared,
    ...seeded,
    engineA: Context.get(contextA, AgentControlStageRunLeaseEngine),
    engineB: Context.get(contextB, AgentControlStageRunLeaseEngine),
  };
});

const readCounts = (
  sql: SqlClient.SqlClient,
  leaseId: AgentControlStageRunLeaseId,
  commandId?: CommandId,
) =>
  sql<{
    readonly events: number;
    readonly projections: number;
    readonly receipts: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM agent_control_events
       WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${leaseId}) AS events,
      (SELECT COUNT(*) FROM agent_control_stage_run_lease_states
       WHERE lease_id = ${leaseId}) AS projections,
      (SELECT COUNT(*) FROM agent_control_command_receipts
       WHERE aggregate_kind = 'stage-run-lease' AND aggregate_id = ${leaseId}
         AND (${commandId ?? null} IS NULL OR command_id = ${commandId ?? null})) AS receipts
  `.pipe(Effect.map((rows) => rows[0]!));

const assertNoPublication = Effect.fn("assertNoLeaseRacePublication")(function* (
  fiber: Fiber.Fiber<ReadonlyArray<unknown>, unknown>,
) {
  const completed = yield* Fiber.await(fiber).pipe(Effect.timeoutOption("50 millis"));
  assert.isTrue(Option.isNone(completed));
  yield* Fiber.interrupt(fiber);
});

const publicationPull = (engine: AgentControlStageRunLeaseEngine["Service"]) =>
  engine.subscribeDomainEvents.pipe(Effect.flatMap(Stream.toPull));

it.live("documents the NodeSqliteClient WAL writer-upgrade conflict", () =>
  Effect.gen(function* () {
    const { sqlA, sqlB } = yield* makeSharedSqliteClients();
    yield* sqlA`CREATE TABLE race_control(id INTEGER PRIMARY KEY, value INTEGER NOT NULL)`;
    yield* sqlA`INSERT INTO race_control(id, value) VALUES (1, 0)`;

    const readA = yield* Deferred.make<number>();
    const readB = yield* Deferred.make<number>();
    const writeA = yield* Deferred.make<void>();
    const writeB = yield* Deferred.make<void>();
    const contender = (
      sql: SqlClient.SqlClient,
      read: Deferred.Deferred<number>,
      write: Deferred.Deferred<void>,
    ) =>
      sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{ readonly value: number }>`
              SELECT value FROM race_control WHERE id = 1
            `;
          yield* Deferred.succeed(read, rows[0]!.value);
          yield* Deferred.await(write);
          yield* sql`UPDATE race_control SET value = value + 1 WHERE id = 1`;
        }),
      );

    const fiberA = yield* contender(sqlA, readA, writeA).pipe(Effect.result, Effect.forkChild);
    const fiberB = yield* contender(sqlB, readB, writeB).pipe(Effect.result, Effect.forkChild);
    assert.equal(yield* Deferred.await(readA).pipe(Effect.timeout(barrierTimeout)), 0);
    assert.equal(yield* Deferred.await(readB).pipe(Effect.timeout(barrierTimeout)), 0);

    yield* Deferred.succeed(writeA, undefined);
    const resultA = yield* joinWithin(fiberA);
    assert.equal(resultA._tag, "Success");
    yield* Deferred.succeed(writeB, undefined);
    const resultB = yield* joinWithin(fiberB);
    assert.equal(resultB._tag, "Failure");
    if (resultB._tag === "Failure") {
      assert.equal(resultB.failure._tag, "SqlError");
    }
    const final = yield* sqlA<{ readonly value: number }>`
        SELECT value FROM race_control WHERE id = 1
      `;
    assert.equal(final[0]?.value, 1);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("commits exactly one reservation after both engines read the empty lease", () =>
  Effect.gen(function* () {
    const controllerA = yield* makeHookController("A");
    const controllerB = yield* makeHookController("B");
    const projectId = ProjectId.make("lease-two-connection-reserve-race");
    const harness = yield* makeLeaseRaceHarness(projectId, controllerA, controllerB);
    const inputA = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-reserve-a",
      0,
      1,
    );
    const inputB = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-reserve-b",
      0,
      1,
    );
    const afterReadA = yield* controllerA.addGate(inputA.commandId, "afterAuthoritativeRead");
    const afterReadB = yield* controllerB.addGate(inputB.commandId, "afterAuthoritativeRead");
    const beforeAppendA = yield* controllerA.addGate(inputA.commandId, "beforeAppend");
    const beforeAppendB = yield* controllerB.addGate(inputB.commandId, "beforeAppend");
    const beforeCompleteA = yield* controllerA.addGate(
      inputA.commandId,
      "beforeTransactionComplete",
    );
    const pullA = yield* publicationPull(harness.engineA);
    const pullB = yield* publicationPull(harness.engineB);
    const publishedA = yield* pullA.pipe(Effect.forkChild);
    const publishedB = yield* pullB.pipe(Effect.forkChild);

    const fiberA = yield* harness.engineA
      .dispatchController(inputA)
      .pipe(Effect.result, Effect.forkChild);
    const fiberB = yield* harness.engineB
      .dispatchController(inputB)
      .pipe(Effect.result, Effect.forkChild);
    yield* Effect.all([awaitGate(afterReadA), awaitGate(afterReadB)], {
      concurrency: "unbounded",
    });
    const readA = yield* observation(controllerA, inputA.commandId, "afterAuthoritativeRead");
    const readB = yield* observation(controllerB, inputB.commandId, "afterAuthoritativeRead");
    assert.deepStrictEqual(
      readA.map(({ streamVersion, projectionRevision, fenceToken }) => ({
        streamVersion,
        projectionRevision,
        fenceToken,
      })),
      [{ streamVersion: 0, projectionRevision: null, fenceToken: null }],
    );
    assert.deepStrictEqual(
      readB.map(({ streamVersion, projectionRevision, fenceToken }) => ({
        streamVersion,
        projectionRevision,
        fenceToken,
      })),
      [{ streamVersion: 0, projectionRevision: null, fenceToken: null }],
    );

    yield* Effect.all([releaseGate(afterReadA), releaseGate(afterReadB)]);
    yield* Effect.all([awaitGate(beforeAppendA), awaitGate(beforeAppendB)], {
      concurrency: "unbounded",
    });
    assert.lengthOf(yield* observation(controllerA, inputA.commandId, "beforeAppend"), 1);
    assert.lengthOf(yield* observation(controllerB, inputB.commandId, "beforeAppend"), 1);

    yield* releaseGate(beforeAppendA);
    yield* awaitGate(beforeCompleteA);
    yield* releaseGate(beforeCompleteA);
    const resultA = yield* joinWithin(fiberA);
    assert.equal(resultA._tag, "Success");
    yield* releaseGate(beforeAppendB);
    const resultB = yield* joinWithin(fiberB);
    assert.equal(resultB._tag, "Failure");
    if (resultB._tag === "Failure") {
      assert.equal(resultB.failure.code, "internal-persistence-error");
    }
    assert.deepStrictEqual(yield* readCounts(harness.sqlA, inputA.leaseId), {
      events: 1,
      projections: 1,
      receipts: 1,
    });
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, inputB.leaseId, inputB.commandId), {
      events: 1,
      projections: 1,
      receipts: 0,
    });

    const retryB = yield* harness.engineB.dispatchController(inputB);
    assert.equal(retryB._tag, "Rejected");
    if (retryB._tag === "Rejected") {
      assert.equal(retryB.error.code, "lease-already-reserved");
    }
    const stateRows = yield* harness.sqlA<{
      readonly revision: number;
      readonly fenceToken: number;
    }>`
        SELECT revision, fence_token AS "fenceToken"
        FROM agent_control_stage_run_lease_states WHERE lease_id = ${inputA.leaseId}
      `;
    assert.deepStrictEqual(stateRows, [{ revision: 1, fenceToken: 1 }]);
    const receipts = yield* harness.sqlA<{
      readonly commandId: string;
      readonly status: string;
      readonly errorCode: string | null;
    }>`
        SELECT command_id AS "commandId", status, error_code AS "errorCode"
        FROM agent_control_command_receipts
        WHERE aggregate_kind = 'stage-run-lease' AND aggregate_id = ${inputA.leaseId}
        ORDER BY command_id
      `;
    assert.deepStrictEqual(receipts, [
      { commandId: inputA.commandId, status: "accepted", errorCode: null },
      {
        commandId: inputB.commandId,
        status: "rejected",
        errorCode: "lease-already-reserved",
      },
    ]);

    const published = yield* joinWithin(publishedA);
    assert.equal(published.length, 1);
    assert.equal(published[0]!.commandId, inputA.commandId);
    const extraA = yield* pullA.pipe(Effect.forkChild);
    yield* assertNoPublication(extraA);
    yield* assertNoPublication(publishedB);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rolls event projection and receipt back at the open commit boundary", () =>
  Effect.gen(function* () {
    const controllerA = yield* makeHookController("A");
    const controllerB = yield* makeHookController("B");
    const projectId = ProjectId.make("lease-two-connection-commit-rollback");
    const harness = yield* makeLeaseRaceHarness(projectId, controllerA, controllerB);
    const input = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-rollback",
      0,
      1,
    );
    const beforeComplete = yield* controllerA.addGate(input.commandId, "beforeTransactionComplete");
    const rollbackPublicationScope = yield* Scope.make("sequential");
    yield* Effect.addFinalizer(() => Scope.close(rollbackPublicationScope, Exit.void));
    const initialPull = yield* publicationPull(harness.engineA).pipe(
      Scope.provide(rollbackPublicationScope),
    );
    const publication = yield* initialPull.pipe(Effect.forkChild);
    const dispatch = yield* harness.engineA
      .dispatchController(input)
      .pipe(Effect.result, Effect.forkChild);

    yield* awaitGate(beforeComplete);
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, input.leaseId), {
      events: 0,
      projections: 0,
      receipts: 0,
    });
    yield* Fiber.interrupt(dispatch);
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, input.leaseId), {
      events: 0,
      projections: 0,
      receipts: 0,
    });
    yield* assertNoPublication(publication);
    yield* Scope.close(rollbackPublicationScope, Exit.void);

    yield* releaseGate(beforeComplete);
    const retryPull = yield* publicationPull(harness.engineA);
    const retryPublication = yield* retryPull.pipe(Effect.forkChild);
    const retry = yield* harness.engineA.dispatchController(input);
    assert.equal(retry._tag, "Accepted");
    if (retry._tag !== "Accepted") return;
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, input.leaseId), {
      events: 1,
      projections: 1,
      receipts: 1,
    });
    const events = yield* joinWithin(retryPublication);
    assert.equal(events.length, 1);
    assert.equal(events[0]!.commandId, input.commandId);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("replays one accepted command across independent runtime holders", () =>
  Effect.gen(function* () {
    const controllerA = yield* makeHookController("A");
    const controllerB = yield* makeHookController("B");
    const projectId = ProjectId.make("lease-two-connection-identical-command");
    const harness = yield* makeLeaseRaceHarness(projectId, controllerA, controllerB);
    const input = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-identical",
      0,
      1,
    );
    const afterReadA = yield* controllerA.addGate(input.commandId, "afterAuthoritativeRead");
    const afterReadB = yield* controllerB.addGate(input.commandId, "afterAuthoritativeRead");
    const beforeAppendA = yield* controllerA.addGate(input.commandId, "beforeAppend");
    const beforeAppendB = yield* controllerB.addGate(input.commandId, "beforeAppend");
    const beforeCompleteA = yield* controllerA.addGate(
      input.commandId,
      "beforeTransactionComplete",
    );
    const pullA = yield* publicationPull(harness.engineA);
    const pullB = yield* publicationPull(harness.engineB);
    const publishedA = yield* pullA.pipe(Effect.forkChild);
    const publishedB = yield* pullB.pipe(Effect.forkChild);
    const fiberA = yield* harness.engineA
      .dispatchController(input)
      .pipe(Effect.result, Effect.forkChild);
    const fiberB = yield* harness.engineB
      .dispatchController(input)
      .pipe(Effect.result, Effect.forkChild);

    yield* Effect.all([awaitGate(afterReadA), awaitGate(afterReadB)], {
      concurrency: "unbounded",
    });
    for (const controller of [controllerA, controllerB]) {
      const [record] = yield* observation(controller, input.commandId, "afterAuthoritativeRead");
      assert.deepStrictEqual(
        record === undefined
          ? null
          : {
              streamVersion: record.streamVersion,
              projectionRevision: record.projectionRevision,
              fenceToken: record.fenceToken,
            },
        { streamVersion: 0, projectionRevision: null, fenceToken: null },
      );
    }
    yield* Effect.all([releaseGate(afterReadA), releaseGate(afterReadB)]);
    yield* Effect.all([awaitGate(beforeAppendA), awaitGate(beforeAppendB)], {
      concurrency: "unbounded",
    });

    yield* releaseGate(beforeAppendA);
    yield* awaitGate(beforeCompleteA);
    yield* releaseGate(beforeAppendB);
    const resultB = yield* joinWithin(fiberB);
    assert.equal(resultB._tag, "Failure");
    if (resultB._tag === "Failure") {
      assert.equal(resultB.failure.code, "internal-persistence-error");
    }
    // A has inserted event, projection and receipt, but its outer transaction
    // is still open. B's independent connection sees none of those writes.
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, input.leaseId), {
      events: 0,
      projections: 0,
      receipts: 0,
    });

    yield* releaseGate(beforeCompleteA);
    const resultA = yield* joinWithin(fiberA);
    assert.equal(resultA._tag, "Success");
    if (resultA._tag !== "Success" || resultA.success._tag !== "Accepted") return;
    assert.deepStrictEqual(yield* readCounts(harness.sqlB, input.leaseId), {
      events: 1,
      projections: 1,
      receipts: 1,
    });

    const replayB = yield* harness.engineB.dispatchController(input);
    assert.equal(replayB._tag, "Accepted");
    if (replayB._tag !== "Accepted") return;
    assert.deepStrictEqual(replayB.result, resultA.success.result);
    assert.lengthOf(replayB.events, 0);
    assert.isTrue(replayB.result.state.holderId === resultA.success.result.state.holderId);
    const viewB = yield* harness.engineB.toView(replayB.result.state);
    assert.equal(viewB.ownership, "foreign-runtime");
    assert.equal(viewB.health, "recovery-required");

    for (const mutation of [
      {
        type: "agentControl.stageRunLease.renew" as const,
        commandId: CommandId.make("lease-two-connection-foreign-renew"),
        leaseId: input.leaseId,
        projectId,
        taskId: harness.task.taskId,
        stageRunId: harness.stageRun.stageRunId,
        attemptId: harness.stageRun.attemptId,
        ...commandSnapshot(harness.stageRun),
        fenceToken: 1,
        expectedRevision: 1,
        leaseDurationMs: 60_000,
      },
      {
        type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
        commandId: CommandId.make("lease-two-connection-foreign-release"),
        leaseId: input.leaseId,
        projectId,
        taskId: harness.task.taskId,
        stageRunId: harness.stageRun.stageRunId,
        attemptId: harness.stageRun.attemptId,
        ...commandSnapshot(harness.stageRun),
        fenceToken: 1,
        expectedRevision: 1,
      },
    ]) {
      const denied = yield* harness.engineB.dispatchController(mutation);
      assert.equal(denied._tag, "Rejected");
      if (denied._tag === "Rejected") assert.equal(denied.error.code, "holder-mismatch");
    }
    const acceptedReceipts = yield* harness.sqlA<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
        WHERE aggregate_kind = 'stage-run-lease' AND aggregate_id = ${input.leaseId}
          AND status = 'accepted'
      `;
    assert.equal(acceptedReceipts[0]?.count, 1);
    const event = yield* joinWithin(publishedA);
    assert.equal(event.length, 1);
    assert.equal(event[0]!.commandId, input.commandId);
    const extraA = yield* pullA.pipe(Effect.forkChild);
    yield* assertNoPublication(extraA);
    yield* assertNoPublication(publishedB);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("commits release before reservation and advances the fence exactly once", () =>
  Effect.gen(function* () {
    const controllerA = yield* makeHookController("A");
    const controllerB = yield* makeHookController("B");
    const projectId = ProjectId.make("lease-two-connection-release-reserve");
    const harness = yield* makeLeaseRaceHarness(projectId, controllerA, controllerB);
    const initialInput = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-initial",
      0,
      1,
    );
    const pullA = yield* publicationPull(harness.engineA);
    const pullB = yield* publicationPull(harness.engineB);
    const initialPublished = yield* pullA.pipe(Effect.forkChild);
    const initial = yield* harness.engineA.dispatchController(initialInput);
    assert.equal(initial._tag, "Accepted");
    if (initial._tag !== "Accepted") return;
    const initialEvent = yield* joinWithin(initialPublished);
    assert.equal(initialEvent.length, 1);
    assert.equal(initialEvent[0]!.commandId, initialInput.commandId);

    const release = {
      type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
      commandId: CommandId.make("lease-two-connection-release"),
      leaseId: initialInput.leaseId,
      projectId,
      taskId: harness.task.taskId,
      stageRunId: harness.stageRun.stageRunId,
      attemptId: harness.stageRun.attemptId,
      ...commandSnapshot(harness.stageRun),
      fenceToken: 1,
      expectedRevision: 1,
    };
    const reserve = yield* resolvedReserveInput(
      harness.stageRun,
      "lease-two-connection-reserve-after-release",
      2,
      2,
    );
    const releaseAfterRead = yield* controllerA.addGate(
      release.commandId,
      "afterAuthoritativeRead",
    );
    const releaseBeforeAppend = yield* controllerA.addGate(release.commandId, "beforeAppend");
    const releaseBeforeComplete = yield* controllerA.addGate(
      release.commandId,
      "beforeTransactionComplete",
    );
    const reserveAfterRead = yield* controllerB.addGate(
      reserve.commandId,
      "afterAuthoritativeRead",
    );
    const reserveBeforeAppend = yield* controllerB.addGate(reserve.commandId, "beforeAppend");
    const releasePublished = yield* pullA.pipe(Effect.forkChild);
    const reservePublished = yield* pullB.pipe(Effect.forkChild);

    const releaseFiber = yield* harness.engineA
      .dispatchController(release)
      .pipe(Effect.result, Effect.forkChild);
    yield* awaitGate(releaseAfterRead);
    const [releaseRead] = yield* observation(
      controllerA,
      release.commandId,
      "afterAuthoritativeRead",
    );
    assert.deepStrictEqual(
      releaseRead === undefined
        ? null
        : {
            streamVersion: releaseRead.streamVersion,
            projectionRevision: releaseRead.projectionRevision,
            fenceToken: releaseRead.fenceToken,
          },
      { streamVersion: 1, projectionRevision: 1, fenceToken: 1 },
    );
    yield* releaseGate(releaseAfterRead);
    yield* awaitGate(releaseBeforeAppend);
    yield* releaseGate(releaseBeforeAppend);
    yield* awaitGate(releaseBeforeComplete);

    const reserveFiber = yield* harness.engineB
      .dispatchController(reserve)
      .pipe(Effect.result, Effect.forkChild);
    yield* awaitGate(reserveAfterRead);
    const [reserveRead] = yield* observation(
      controllerB,
      reserve.commandId,
      "afterAuthoritativeRead",
    );
    assert.deepStrictEqual(
      reserveRead === undefined
        ? null
        : {
            streamVersion: reserveRead.streamVersion,
            projectionRevision: reserveRead.projectionRevision,
            fenceToken: reserveRead.fenceToken,
          },
      { streamVersion: 1, projectionRevision: 1, fenceToken: 1 },
    );
    yield* releaseGate(reserveAfterRead);
    const reserveWhileReleaseOpen = yield* joinWithin(reserveFiber);
    assert.equal(reserveWhileReleaseOpen._tag, "Failure");
    if (reserveWhileReleaseOpen._tag === "Failure") {
      assert.equal(reserveWhileReleaseOpen.failure.code, "internal-persistence-error");
    }
    const beforeReleaseCommit = yield* harness.sqlB<{
      readonly events: number;
      readonly receipts: number;
      readonly status: string;
      readonly revision: number;
      readonly fenceToken: number;
    }>`
        SELECT
          (SELECT COUNT(*) FROM agent_control_events
           WHERE aggregate_kind = 'stage-run-lease'
             AND stream_id = ${initialInput.leaseId}) AS events,
          (SELECT COUNT(*) FROM agent_control_command_receipts
           WHERE aggregate_kind = 'stage-run-lease'
             AND aggregate_id = ${initialInput.leaseId}) AS receipts,
          status, revision, fence_token AS "fenceToken"
        FROM agent_control_stage_run_lease_states
        WHERE lease_id = ${initialInput.leaseId}
      `;
    assert.deepStrictEqual(beforeReleaseCommit, [
      { events: 1, receipts: 1, status: "reserved", revision: 1, fenceToken: 1 },
    ]);

    yield* releaseGate(releaseBeforeComplete);
    const released = yield* joinWithin(releaseFiber);
    assert.equal(released._tag, "Success");
    if (released._tag !== "Success" || released.success._tag !== "Accepted") return;
    const releasedEvent = yield* joinWithin(releasePublished);
    assert.equal(releasedEvent.length, 1);
    assert.equal(releasedEvent[0]!.commandId, release.commandId);

    const reserveRetryFiber = yield* harness.engineB
      .dispatchController(reserve)
      .pipe(Effect.forkChild);
    yield* awaitGate(reserveBeforeAppend);
    const [appendRead] = yield* observation(controllerB, reserve.commandId, "beforeAppend");
    assert.deepStrictEqual(
      appendRead === undefined
        ? null
        : {
            streamVersion: appendRead.streamVersion,
            projectionRevision: appendRead.projectionRevision,
            fenceToken: appendRead.fenceToken,
          },
      { streamVersion: 2, projectionRevision: 2, fenceToken: 1 },
    );
    yield* releaseGate(reserveBeforeAppend);
    const reserved = yield* joinWithin(reserveRetryFiber);
    assert.equal(reserved._tag, "Accepted");
    if (reserved._tag !== "Accepted") return;
    assert.equal(reserved.result.state.revision, 3);
    assert.equal(reserved.result.state.fenceToken, 2);
    const reservedEvent = yield* joinWithin(reservePublished);
    assert.equal(reservedEvent.length, 1);
    assert.equal(reservedEvent[0]!.commandId, reserve.commandId);

    const history = yield* harness.sqlA<{
      readonly streamVersion: number;
      readonly eventType: string;
      readonly fenceToken: number;
    }>`
        SELECT stream_version AS "streamVersion", event_type AS "eventType",
          json_extract(payload_json, '$.fenceToken') AS "fenceToken"
        FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${initialInput.leaseId}
        ORDER BY stream_version
      `;
    assert.deepStrictEqual(history, [
      {
        streamVersion: 1,
        eventType: "agentControl.stageRunLease.reserved",
        fenceToken: 1,
      },
      {
        streamVersion: 2,
        eventType: "agentControl.stageRunLease.releasedBeforeExecution",
        fenceToken: 1,
      },
      {
        streamVersion: 3,
        eventType: "agentControl.stageRunLease.reserved",
        fenceToken: 2,
      },
    ]);
    const projection = yield* harness.sqlA<{
      readonly state: string;
      readonly revision: number;
      readonly fenceToken: number;
    }>`
        SELECT state_json AS state, revision, fence_token AS "fenceToken"
        FROM agent_control_stage_run_lease_states WHERE lease_id = ${initialInput.leaseId}
      `;
    assert.equal(projection[0]?.revision, 3);
    assert.equal(projection[0]?.fenceToken, 2);
    const decoded = yield* decodeLeaseStateJson(projection[0]!.state);
    assert.equal(decoded.status, "reserved");
    const acceptedReceipts = yield* harness.sqlA<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
        WHERE aggregate_kind = 'stage-run-lease' AND aggregate_id = ${initialInput.leaseId}
          AND status = 'accepted'
      `;
    assert.equal(acceptedReceipts[0]?.count, 3);

    const extraA = yield* pullA.pipe(Effect.forkChild);
    const extraB = yield* pullB.pipe(Effect.forkChild);
    for (const stale of [
      {
        type: "agentControl.stageRunLease.renew" as const,
        commandId: CommandId.make("lease-two-connection-stale-renew"),
        leaseId: initialInput.leaseId,
        projectId,
        taskId: harness.task.taskId,
        stageRunId: harness.stageRun.stageRunId,
        attemptId: harness.stageRun.attemptId,
        ...commandSnapshot(harness.stageRun),
        fenceToken: 1,
        expectedRevision: 3,
        leaseDurationMs: 60_000,
      },
      {
        type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
        commandId: CommandId.make("lease-two-connection-stale-release"),
        leaseId: initialInput.leaseId,
        projectId,
        taskId: harness.task.taskId,
        stageRunId: harness.stageRun.stageRunId,
        attemptId: harness.stageRun.attemptId,
        ...commandSnapshot(harness.stageRun),
        fenceToken: 1,
        expectedRevision: 3,
      },
    ]) {
      const denied = yield* harness.engineA.dispatchController(stale);
      assert.equal(denied._tag, "Rejected");
      if (denied._tag === "Rejected") assert.equal(denied.error.code, "holder-mismatch");
    }
    yield* assertNoPublication(extraA);
    yield* assertNoPublication(extraB);
  }).pipe(Effect.provide(NodeServices.layer)),
);
