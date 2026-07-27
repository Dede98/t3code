import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  type AgentControlThreadMaterializeCommand,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../config.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../agentControl/controlledThreadReservation/identity.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  AgentControlThreadMaterializationTransactionHooks,
  type AgentControlThreadMaterializationTransactionHooksShape,
  type AgentControlThreadMaterializationTransactionObservation,
} from "../Services/AgentControlThreadMaterializationTransactionHooks.ts";
import { OrchestrationEngineLive } from "./OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

const NOW = "2026-07-27T11:00:00.000Z";
const timeout = "10 seconds";

const makeCommand = Effect.fn("makeMaterializationBoundaryCommand")(function* (
  projectId: ProjectId,
  commandIdValue: string,
  identityValue = commandIdValue,
) {
  const stable = {
    projectId,
    taskId: AgentControlTaskId.make(`task-${identityValue}`),
    taskRevision: 2,
    githubIntakeSequence: 7,
    sourceIdentityFingerprint: "d".repeat(64),
    stageRunId: AgentControlStageRunId.make(`stage-run-${identityValue}`),
    attemptId: AgentControlAttemptId.make(`attempt-${identityValue}`),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1,
    attemptOrdinal: 1,
  };
  return {
    type: "thread.agent-control.materialize",
    commandId: CommandId.make(commandIdValue),
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: AgentControlStageRunLeaseId.make(`lease-${identityValue}`),
    fenceToken: 4,
    worktreeReservationId: AgentControlWorktreeReservationId.make(`worktree-${identityValue}`),
    title: `Planning ${identityValue}`,
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6",
    },
    runtimeMode: "approval-required",
    interactionMode: "plan",
    branch: `t3-auto/${identityValue}`,
    worktreePath: `/tmp/t3-auto/${identityValue}`,
    binding: {
      taskId: stable.taskId,
      stageRunId: stable.stageRunId,
      attemptId: stable.attemptId,
      roleId: stable.roleId,
      controlState: "controlled",
    },
    createdAt: NOW,
  } satisfies AgentControlThreadMaterializeCommand;
});

const engineLayer = (sql: SqlClient.SqlClient) => {
  const receipts = OrchestrationCommandReceiptRepositoryLive;
  return Layer.mergeAll(
    OrchestrationEngineLive.pipe(
      Layer.provide(OrchestrationProjectionSnapshotQueryLive),
      Layer.provide(OrchestrationProjectionPipelineLive),
      Layer.provide(receipts),
    ),
    OrchestrationProjectionSnapshotQueryLive,
    receipts,
  ).pipe(
    Layer.provide(OrchestrationEventStoreLive),
    Layer.provide(RepositoryIdentityResolver.layer),
    Layer.provide(receipts),
    Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), {
        prefix: "t3-materialization-boundary-test-",
      }),
    ),
    Layer.provideMerge(NodeServices.layer),
  );
};

const buildEngine = (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  hooks: AgentControlThreadMaterializationTransactionHooksShape,
) =>
  Layer.buildWithScope(Layer.fresh(engineLayer(sql)), scope).pipe(
    Effect.provideService(AgentControlThreadMaterializationTransactionHooks, hooks),
    Effect.map((context) => Context.get(context, OrchestrationEngineService)),
  );

const noHooks: AgentControlThreadMaterializationTransactionHooksShape = {
  afterAuthoritativeRead: () => Effect.void,
  beforeFirstEventAppend: () => Effect.void,
  afterFirstEventAppend: () => Effect.void,
  afterSecondEventAppend: () => Effect.void,
  afterProjection: () => Effect.void,
  afterReceiptInsert: () => Effect.void,
  afterIntentInsert: () => Effect.void,
  beforeTransactionComplete: () => Effect.void,
};

const seedProject = (
  engine: OrchestrationEngineService["Service"],
  projectId: ProjectId,
  suffix: string,
) =>
  engine.dispatch({
    type: "project.create",
    commandId: CommandId.make(`project-create-${suffix}`),
    projectId,
    title: `Project ${suffix}`,
    workspaceRoot: `/tmp/project-${suffix}`,
    createdAt: NOW,
  });

interface SharedHarness {
  readonly sqlA: SqlClient.SqlClient;
  readonly sqlB: SqlClient.SqlClient;
  readonly scopeA: Scope.Closeable;
  readonly scopeB: Scope.Closeable;
}

const makeSharedHarness = Effect.fn("makeMaterializationSharedHarness")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temp = yield* fs.makeTempDirectoryScoped({ prefix: "t3-materialization-wal-" });
  const dbPath = path.join(temp, "state.sqlite");
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
  for (const sql of [sqlA, sqlB]) {
    const journal = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.strictEqual(journal[0]?.journal_mode, "wal");
  }
  yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
  return { sqlA, sqlB, scopeA, scopeB } satisfies SharedHarness;
});

const counts = (sql: SqlClient.SqlClient, command: AgentControlThreadMaterializeCommand) =>
  sql<{
    readonly events: number;
    readonly threads: number;
    readonly acceptedIntents: number;
    readonly allIntents: number;
    readonly acceptedReceipts: number;
    readonly allReceipts: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM orchestration_events
       WHERE stream_id = ${command.threadId}) AS events,
      (SELECT COUNT(*) FROM projection_threads
       WHERE thread_id = ${command.threadId}) AS threads,
      (SELECT COUNT(*)
       FROM orchestration_agent_control_thread_materialization_intents
       WHERE thread_id = ${command.threadId}
         AND receipt_status = 'accepted') AS acceptedIntents,
      (SELECT COUNT(*)
       FROM orchestration_agent_control_thread_materialization_intents
       WHERE thread_id = ${command.threadId}) AS allIntents,
      (SELECT COUNT(*) FROM orchestration_command_receipts
       WHERE aggregate_id = ${command.threadId}
         AND status = 'accepted') AS acceptedReceipts,
      (SELECT COUNT(*) FROM orchestration_command_receipts
       WHERE aggregate_id = ${command.threadId}) AS allReceipts
  `.pipe(Effect.map((rows) => rows[0]!));

interface Gate {
  readonly arrived: Deferred.Deferred<void>;
  readonly release: Deferred.Deferred<void>;
}

const makeGate = Effect.fn("makeMaterializationGate")(function* (): Effect.fn.Return<Gate> {
  return {
    arrived: yield* Deferred.make<void>(),
    release: yield* Deferred.make<void>(),
  };
});
const awaitGate = (gate: Gate) => Deferred.await(gate.arrived).pipe(Effect.timeout(timeout));
const releaseGate = (gate: Gate) => Deferred.succeed(gate.release, undefined).pipe(Effect.asVoid);

const gatedHooks = (input: {
  readonly afterRead?: Gate;
  readonly beforeAppend?: Gate;
  readonly beforeComplete?: Gate;
}): AgentControlThreadMaterializationTransactionHooksShape => ({
  ...noHooks,
  afterAuthoritativeRead: () =>
    input.afterRead === undefined
      ? Effect.void
      : Deferred.succeed(input.afterRead.arrived, undefined).pipe(
          Effect.andThen(Deferred.await(input.afterRead.release)),
        ),
  beforeFirstEventAppend: () =>
    input.beforeAppend === undefined
      ? Effect.void
      : Deferred.succeed(input.beforeAppend.arrived, undefined).pipe(
          Effect.andThen(Deferred.await(input.beforeAppend.release)),
        ),
  beforeTransactionComplete: () =>
    input.beforeComplete === undefined
      ? Effect.void
      : Deferred.succeed(input.beforeComplete.arrived, undefined).pipe(
          Effect.andThen(Deferred.await(input.beforeComplete.release)),
        ),
});

const subscribeTwo = Effect.fn("subscribeTwoMaterializationEvents")(function* (
  engine: OrchestrationEngineService["Service"],
) {
  const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
  return yield* Stream.runCollect(Stream.take(yield* subscribe, 2)).pipe(Effect.forkChild);
});

it.live("converges identical commands across two production-bound WAL engines", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-identical");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-identical");
    const command = yield* makeCommand(projectId, "materialization-wal-same");

    const readA = yield* makeGate();
    const readB = yield* makeGate();
    const appendA = yield* makeGate();
    const appendB = yield* makeGate();
    const completeA = yield* makeGate();
    const engineA = yield* buildEngine(
      harness.sqlA,
      harness.scopeA,
      gatedHooks({ afterRead: readA, beforeAppend: appendA, beforeComplete: completeA }),
    );
    const engineB = yield* buildEngine(
      harness.sqlB,
      harness.scopeB,
      gatedHooks({ afterRead: readB, beforeAppend: appendB }),
    );
    const publishedA = yield* subscribeTwo(engineA);
    const subscribeB = engineB.subscribeDomainEvents ?? Effect.die("subscription unavailable");
    const publishedB = yield* Stream.runHead(yield* subscribeB).pipe(Effect.forkChild);
    const fiberA = yield* engineA.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    const fiberB = yield* engineB.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    yield* Effect.all([awaitGate(readA), awaitGate(readB)], { concurrency: "unbounded" });
    yield* Effect.all([releaseGate(readA), releaseGate(readB)]);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], {
      concurrency: "unbounded",
    });

    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(appendB);
    const loser = yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isFailure(loser), true);
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 0,
      threads: 0,
      acceptedIntents: 0,
      allIntents: 0,
      acceptedReceipts: 0,
      allReceipts: 0,
    });

    yield* releaseGate(completeA);
    const winner = yield* Fiber.join(fiberA).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isSuccess(winner), true);
    const replay = yield* engineB.dispatchAgentControl(command);
    if (Exit.isSuccess(winner)) assert.deepStrictEqual(replay, winner.value);
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 2,
      threads: 1,
      acceptedIntents: 1,
      allIntents: 1,
      acceptedReceipts: 1,
      allReceipts: 1,
    });
    assert.deepStrictEqual(
      Array.from(yield* Fiber.join(publishedA)).map((event) => event.type),
      ["thread.created", "thread.agent-control-bound"],
    );
    yield* Effect.yieldNow;
    assert.strictEqual(publishedB.pollUnsafe(), undefined);
    yield* Fiber.interrupt(publishedB);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("allows one command id to win a reserved thread and receipts the other closed", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-different");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-different");
    const winnerCommand = yield* makeCommand(
      projectId,
      "materialization-wal-winner",
      "shared-position",
    );
    const loserCommand = {
      ...winnerCommand,
      commandId: CommandId.make("materialization-wal-loser"),
    };

    const readA = yield* makeGate();
    const readB = yield* makeGate();
    const appendA = yield* makeGate();
    const appendB = yield* makeGate();
    const completeA = yield* makeGate();
    const engineA = yield* buildEngine(
      harness.sqlA,
      harness.scopeA,
      gatedHooks({ afterRead: readA, beforeAppend: appendA, beforeComplete: completeA }),
    );
    const engineB = yield* buildEngine(
      harness.sqlB,
      harness.scopeB,
      gatedHooks({ afterRead: readB, beforeAppend: appendB }),
    );
    const fiberA = yield* engineA
      .dispatchAgentControl(winnerCommand)
      .pipe(Effect.exit, Effect.forkChild);
    const fiberB = yield* engineB
      .dispatchAgentControl(loserCommand)
      .pipe(Effect.exit, Effect.forkChild);
    yield* Effect.all([awaitGate(readA), awaitGate(readB)], { concurrency: "unbounded" });
    yield* Effect.all([releaseGate(readA), releaseGate(readB)]);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], {
      concurrency: "unbounded",
    });
    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(appendB);
    assert.strictEqual(
      Exit.isFailure(yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout))),
      true,
    );
    yield* releaseGate(completeA);
    assert.strictEqual(
      Exit.isSuccess(yield* Fiber.join(fiberA).pipe(Effect.timeout(timeout))),
      true,
    );

    const closed = yield* Effect.exit(engineB.dispatchAgentControl(loserCommand));
    assert.strictEqual(Exit.isFailure(closed), true);
    const result = yield* counts(harness.sqlA, winnerCommand);
    assert.deepStrictEqual(result, {
      events: 2,
      threads: 1,
      acceptedIntents: 1,
      allIntents: 2,
      acceptedReceipts: 1,
      allReceipts: 2,
    });
    const receipts = yield* harness.sqlA<{
      readonly commandId: string;
      readonly status: string;
    }>`
      SELECT command_id AS "commandId", status
      FROM orchestration_command_receipts
      WHERE aggregate_id = ${winnerCommand.threadId}
      ORDER BY status, command_id
    `;
    assert.deepStrictEqual(receipts, [
      { commandId: winnerCommand.commandId, status: "accepted" },
      { commandId: loserCommand.commandId, status: "rejected" },
    ]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("replays response loss after restart without new events or publication", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-restart");
    const engineA = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(engineA, projectId, "restart");
    const command = yield* makeCommand(projectId, "materialization-restart-command");
    const committed = yield* engineA.dispatchAgentControl(command);

    yield* harness.sqlA`
      DELETE FROM projection_threads WHERE thread_id = ${command.threadId}
    `;
    yield* harness.sqlA`
      DELETE FROM projection_state WHERE projector = 'projection.threads'
    `;
    const engineB = yield* buildEngine(harness.sqlB, harness.scopeB, noHooks);
    const subscribe = engineB.subscribeDomainEvents ?? Effect.die("subscription unavailable");
    const publication = yield* Stream.runHead(yield* subscribe).pipe(Effect.forkChild);
    assert.deepStrictEqual(yield* engineB.dispatchAgentControl(command), committed);
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 2,
      threads: 1,
      acceptedIntents: 1,
      allIntents: 1,
      acceptedReceipts: 1,
      allReceipts: 1,
    });
    yield* Effect.yieldNow;
    assert.strictEqual(publication.pollUnsafe(), undefined);
    yield* Fiber.interrupt(publication);
  }).pipe(Effect.provide(NodeServices.layer)),
);

type FaultPhase =
  | "afterAuthoritativeRead"
  | "beforeFirstEventAppend"
  | "afterFirstEventAppend"
  | "afterSecondEventAppend"
  | "afterProjection"
  | "afterReceiptInsert"
  | "afterIntentInsert"
  | "beforeTransactionComplete";

interface Fault {
  readonly commandId: CommandId;
  readonly phase: FaultPhase;
  readonly effect: (
    observation: AgentControlThreadMaterializationTransactionObservation,
  ) => Effect.Effect<void>;
}

const rollbackLayer = it.layer(Layer.mergeAll(NodeServices.layer, NodeSqliteClient.layerMemory()));

rollbackLayer("controlled thread materialization rollback boundary", (it) => {
  it.effect(
    "rolls back first/second event, first/second projection, intent and receipt failures",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations();
        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const engine = yield* buildEngine(sql, engineScope, noHooks);
        const projectId = ProjectId.make("materialization-write-fault-project");
        yield* seedProject(engine, projectId, "write-fault");
        const publicationCount = yield* Ref.make(0);
        const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
        const publicationConsumer = yield* (yield* subscribe).pipe(
          Stream.runForEach(() => Ref.update(publicationCount, (count) => count + 1)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;

        for (const [index, phase] of [
          "first-event",
          "second-event",
          "first-projection",
          "second-projection",
          "intent",
          "receipt",
        ].entries()) {
          const command = yield* makeCommand(projectId, `materialization-write-fault-${phase}`);
          const trigger = `materialization_write_fault_${index}`;
          const escapedCommand = command.commandId.replaceAll("'", "''");
          const escapedThread = command.threadId.replaceAll("'", "''");
          const ddl =
            phase === "first-event"
              ? `CREATE TEMP TRIGGER ${trigger} BEFORE INSERT ON orchestration_events
                 WHEN NEW.command_id = '${escapedCommand}'
                   AND NEW.event_type = 'thread.created'
                 BEGIN SELECT RAISE(ABORT, 'first event fault'); END`
              : phase === "second-event"
                ? `CREATE TEMP TRIGGER ${trigger} BEFORE INSERT ON orchestration_events
                   WHEN NEW.command_id = '${escapedCommand}'
                     AND NEW.event_type = 'thread.agent-control-bound'
                   BEGIN SELECT RAISE(ABORT, 'second event fault'); END`
                : phase === "first-projection"
                  ? `CREATE TEMP TRIGGER ${trigger} BEFORE INSERT ON projection_threads
                     WHEN NEW.thread_id = '${escapedThread}'
                     BEGIN SELECT RAISE(ABORT, 'first projection fault'); END`
                  : phase === "second-projection"
                    ? `CREATE TEMP TRIGGER ${trigger} BEFORE UPDATE ON projection_threads
                       WHEN NEW.thread_id = '${escapedThread}'
                         AND NEW.agent_control_json IS NOT NULL
                       BEGIN SELECT RAISE(ABORT, 'second projection fault'); END`
                    : phase === "intent"
                      ? `CREATE TEMP TRIGGER ${trigger}
                         BEFORE INSERT ON orchestration_agent_control_thread_materialization_intents
                         WHEN NEW.command_id = '${escapedCommand}'
                         BEGIN SELECT RAISE(ABORT, 'intent fault'); END`
                      : `CREATE TEMP TRIGGER ${trigger}
                         BEFORE INSERT ON orchestration_command_receipts
                         WHEN NEW.command_id = '${escapedCommand}'
                         BEGIN SELECT RAISE(ABORT, 'receipt fault'); END`;
          yield* sql.unsafe(ddl).unprepared;
          const publicationsBeforeFailure = yield* Ref.get(publicationCount);
          const failed = yield* Effect.exit(engine.dispatchAgentControl(command));
          assert.strictEqual(Exit.isFailure(failed), true);
          assert.deepStrictEqual(yield* counts(sql, command), {
            events: 0,
            threads: 0,
            acceptedIntents: 0,
            allIntents: 0,
            acceptedReceipts: 0,
            allReceipts: 0,
          });
          yield* Effect.yieldNow;
          assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeFailure);
          yield* sql.unsafe(`DROP TRIGGER ${trigger}`).unprepared;
          yield* engine.dispatchAgentControl(command);
          assert.deepStrictEqual(yield* counts(sql, command), {
            events: 2,
            threads: 1,
            acceptedIntents: 1,
            allIntents: 1,
            acceptedReceipts: 1,
            allReceipts: 1,
          });
          yield* Effect.yieldNow;
          assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeFailure + 2);
        }
        yield* Fiber.interrupt(publicationConsumer);
      }),
  );

  it.effect("binds deterministic rejections to the complete command fingerprint", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-rejection-project");
      yield* seedProject(engine, projectId, "rejection");
      const canonical = yield* makeCommand(projectId, "materialization-rejected");
      const rejected = { ...canonical, runtimeMode: "full-access" as const };

      const first = yield* Effect.exit(engine.dispatchAgentControl(rejected));
      assert.strictEqual(Exit.isFailure(first), true);
      assert.deepStrictEqual(yield* counts(sql, rejected), {
        events: 0,
        threads: 0,
        acceptedIntents: 0,
        allIntents: 1,
        acceptedReceipts: 0,
        allReceipts: 1,
      });
      const replay = yield* Effect.flip(engine.dispatchAgentControl(rejected));
      assert.strictEqual(replay._tag, "OrchestrationCommandPreviouslyRejectedError");
      const changed = yield* Effect.flip(
        engine.dispatchAgentControl({ ...rejected, title: "Changed rejection" }),
      );
      assert.strictEqual(changed._tag, "OrchestrationCommandIdentityConflictError");
    }),
  );

  it.effect("fails closed when replay evidence or the controlled projection is corrupted", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-corruption-project");
      yield* seedProject(engine, projectId, "corruption");

      const eventCommand = yield* makeCommand(projectId, "materialization-corrupt-event");
      yield* engine.dispatchAgentControl(eventCommand);
      yield* sql`
        UPDATE orchestration_events
        SET payload_json = json_set(payload_json, '$.title', 'Corrupted')
        WHERE command_id = ${eventCommand.commandId}
          AND event_type = 'thread.created'
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(eventCommand))),
        true,
      );

      const projectionCommand = yield* makeCommand(projectId, "materialization-corrupt-projection");
      yield* engine.dispatchAgentControl(projectionCommand);
      yield* sql`
        UPDATE projection_threads
        SET agent_control_json = json_set(
          agent_control_json, '$.controlState', 'taken-over'
        )
        WHERE thread_id = ${projectionCommand.threadId}
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(projectionCommand))),
        true,
      );

      const intentCommand = yield* makeCommand(projectId, "materialization-corrupt-intent");
      yield* engine.dispatchAgentControl(intentCommand);
      yield* sql`DROP TRIGGER trg_orchestration_materialization_intent_immutable_update`;
      yield* sql`
        UPDATE orchestration_agent_control_thread_materialization_intents
        SET title = 'Corrupted intent'
        WHERE command_id = ${intentCommand.commandId}
      `;
      const corruptIntent = yield* Effect.flip(engine.dispatchAgentControl(intentCommand));
      assert.strictEqual(corruptIntent._tag, "PersistenceDecodeError");

      const receiptCommand = yield* makeCommand(projectId, "materialization-corrupt-receipt");
      yield* engine.dispatchAgentControl(receiptCommand);
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`
        UPDATE orchestration_command_receipts
        SET result_sequence = result_sequence + 10
        WHERE command_id = ${receiptCommand.commandId}
      `;
      yield* sql`PRAGMA foreign_keys = ON`;
      const corruptReceipt = yield* Effect.flip(engine.dispatchAgentControl(receiptCommand));
      assert.strictEqual(corruptReceipt._tag, "PersistenceDecodeError");

      const missingIntentCommand = yield* makeCommand(projectId, "materialization-missing-intent");
      yield* engine.dispatchAgentControl(missingIntentCommand);
      yield* sql`DROP TRIGGER trg_orchestration_materialization_intent_immutable_delete`;
      yield* sql`
        DELETE FROM orchestration_agent_control_thread_materialization_intents
        WHERE command_id = ${missingIntentCommand.commandId}
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(missingIntentCommand))),
        true,
      );
    }),
  );

  it.effect("rolls back hook defects, a real interrupt, and an outer commit failure", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      yield* sql`CREATE TABLE materialization_commit_parent(id INTEGER PRIMARY KEY)`;
      yield* sql`
        CREATE TABLE materialization_commit_child(
          parent_id INTEGER,
          FOREIGN KEY(parent_id) REFERENCES materialization_commit_parent(id)
            DEFERRABLE INITIALLY DEFERRED
        )
      `;
      const fault = yield* Ref.make<Fault | null>(null);
      const run =
        (phase: FaultPhase) =>
        (observation: AgentControlThreadMaterializationTransactionObservation) =>
          Ref.get(fault).pipe(
            Effect.flatMap((current) =>
              current?.commandId === observation.commandId && current.phase === phase
                ? current.effect(observation)
                : Effect.void,
            ),
          );
      const hooks: AgentControlThreadMaterializationTransactionHooksShape = {
        afterAuthoritativeRead: run("afterAuthoritativeRead"),
        beforeFirstEventAppend: run("beforeFirstEventAppend"),
        afterFirstEventAppend: run("afterFirstEventAppend"),
        afterSecondEventAppend: run("afterSecondEventAppend"),
        afterProjection: run("afterProjection"),
        afterReceiptInsert: run("afterReceiptInsert"),
        afterIntentInsert: run("afterIntentInsert"),
        beforeTransactionComplete: run("beforeTransactionComplete"),
      };
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, hooks);
      const projectId = ProjectId.make("materialization-rollback-project");
      yield* seedProject(engine, projectId, "rollback");
      const publicationCount = yield* Ref.make(0);
      const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
      const publicationConsumer = yield* (yield* subscribe).pipe(
        Stream.runForEach(() => Ref.update(publicationCount, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;

      const defectCommand = yield* makeCommand(projectId, "materialization-defect");
      yield* Ref.set(fault, {
        commandId: defectCommand.commandId,
        phase: "afterIntentInsert",
        effect: () => Effect.die("materialization-defect-sentinel"),
      });
      const defect = yield* Effect.flip(engine.dispatchAgentControl(defectCommand));
      assert.include(String(defect), "materialization-defect-sentinel");
      assert.deepStrictEqual(yield* counts(sql, defectCommand), {
        events: 0,
        threads: 0,
        acceptedIntents: 0,
        allIntents: 0,
        acceptedReceipts: 0,
        allReceipts: 0,
      });
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 0);
      yield* Ref.set(fault, null);
      yield* engine.dispatchAgentControl(defectCommand);
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 2);

      const commitCommand = yield* makeCommand(projectId, "materialization-commit-failure");
      yield* Ref.set(fault, {
        commandId: commitCommand.commandId,
        phase: "beforeTransactionComplete",
        effect: () =>
          sql`INSERT INTO materialization_commit_child(parent_id) VALUES (99)`.pipe(
            Effect.asVoid,
            Effect.orDie,
          ),
      });
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(commitCommand))),
        true,
      );
      assert.deepStrictEqual(yield* counts(sql, commitCommand), {
        events: 0,
        threads: 0,
        acceptedIntents: 0,
        allIntents: 0,
        acceptedReceipts: 0,
        allReceipts: 0,
      });
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 2);
      yield* Ref.set(fault, null);
      yield* engine.dispatchAgentControl(commitCommand);
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 4);

      const interruptCommand = yield* makeCommand(projectId, "materialization-interrupt");
      const arrived = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Ref.set(fault, {
        commandId: interruptCommand.commandId,
        phase: "beforeTransactionComplete",
        effect: () =>
          Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release))),
      });
      const dispatch = yield* engine.dispatchAgentControl(interruptCommand).pipe(Effect.forkChild);
      yield* Deferred.await(arrived).pipe(Effect.timeout(timeout));
      yield* Fiber.interrupt(dispatch);
      assert.deepStrictEqual(yield* counts(sql, interruptCommand), {
        events: 0,
        threads: 0,
        acceptedIntents: 0,
        allIntents: 0,
        acceptedReceipts: 0,
        allReceipts: 0,
      });
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 4);
      yield* Ref.set(fault, null);
      yield* Deferred.succeed(release, undefined);
      yield* engine.dispatchAgentControl(interruptCommand);
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), 6);
      yield* Fiber.interrupt(publicationConsumer);
    }),
  );
});
