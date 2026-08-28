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
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
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
import type { SqlError } from "effect/unstable/sql/SqlError";

import { ServerConfig } from "../../config.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../agentControl/controlledThreadReservation/identity.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../agentControl/stageRun/identity.ts";
import { deriveAgentControlStageRunLeaseId } from "../../agentControl/stageRunLease/identity.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../persistence/Layers/OrchestrationEventStore.ts";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import * as RepositoryIdentityResolver from "../../project/RepositoryIdentityResolver.ts";
import { fingerprintAgentControlThreadMaterializationCommand } from "../agentControlThreadMaterializationIntent.ts";
import type { OrchestrationDispatchError } from "../Errors.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  AgentControlThreadMaterializationConvergencePolicy,
  AgentControlThreadMaterializationTransactionHooks,
  type AgentControlThreadMaterializationConvergencePolicyShape,
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
  const taskId = AgentControlTaskId.make(`task-${identityValue}`);
  const taskRevision = 2;
  const githubIntakeSequence = 7;
  const sourceIdentityFingerprint = "d".repeat(64);
  const stageKind = "planning" as const;
  const stageOrdinal = 1;
  const attemptOrdinal = 1;
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision,
    githubIntakeSequence,
    sourceIdentityFingerprint,
    stageKind,
    stageOrdinal,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, attemptOrdinal);
  const stable = {
    projectId,
    taskId,
    taskRevision,
    githubIntakeSequence,
    sourceIdentityFingerprint,
    stageRunId,
    attemptId,
    roleId: AgentControlRoleId.make("planning"),
    stageKind,
    stageOrdinal,
    attemptOrdinal,
  };
  return {
    type: "thread.agent-control.materialize",
    commandId: CommandId.make(commandIdValue),
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: yield* deriveAgentControlStageRunLeaseId({ projectId, taskId }),
    fenceToken: 4,
    worktreeReservationId: AgentControlWorktreeReservationId.make(`worktree-${identityValue}`),
    title: `Planning ${identityValue}`,
    modelSelection: {
      instanceId: ProviderInstanceId.make("codex"),
      model: "gpt-5.6",
      options: [{ id: "reasoning-effort", value: "high" }],
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
  convergencePolicy: AgentControlThreadMaterializationConvergencePolicyShape = {
    maximumReadAttempts: 5,
    delayBetweenAttempts: "5 millis",
  },
) =>
  Layer.buildWithScope(Layer.fresh(engineLayer(sql)), scope).pipe(
    Effect.provideService(AgentControlThreadMaterializationTransactionHooks, hooks),
    Effect.provideService(AgentControlThreadMaterializationConvergencePolicy, convergencePolicy),
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
    yield* releaseGate(completeA);
    const winner = yield* Fiber.join(fiberA).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isSuccess(winner), true);
    yield* releaseGate(appendB);
    const converged = yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isSuccess(converged), true);
    if (Exit.isSuccess(winner) && Exit.isSuccess(converged)) {
      assert.deepStrictEqual(converged.value, winner.value);
      assert.strictEqual(yield* engineB.latestSequence, winner.value.sequence);
    }
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

it.live("rejects changed commands from two original same-command WAL dispatches", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-changed-command");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-changed-command");
    const command = yield* makeCommand(projectId, "materialization-wal-changed-same-id");
    const changedCommand = { ...command, title: "Changed concurrent command" };

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
    const fiberA = yield* engineA.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    const fiberB = yield* engineB
      .dispatchAgentControl(changedCommand)
      .pipe(Effect.exit, Effect.forkChild);
    yield* Effect.all([awaitGate(readA), awaitGate(readB)], { concurrency: "unbounded" });
    yield* Effect.all([releaseGate(readA), releaseGate(readB)]);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], {
      concurrency: "unbounded",
    });

    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(completeA);
    assert.strictEqual(
      Exit.isSuccess(yield* Fiber.join(fiberA).pipe(Effect.timeout(timeout))),
      true,
    );
    yield* releaseGate(appendB);
    const loser = yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isFailure(loser), true);
    if (Exit.isFailure(loser)) {
      const failure = Cause.squash(loser.cause) as { readonly _tag?: string };
      assert.strictEqual(failure._tag, "OrchestrationCommandIdentityConflictError");
    }
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 2,
      threads: 1,
      acceptedIntents: 1,
      allIntents: 1,
      acceptedReceipts: 1,
      allReceipts: 1,
    });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("interrupts convergence sleep and releases the same WAL connection to another caller", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-interrupt-convergence");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-interrupt-convergence");
    const command = yield* makeCommand(projectId, "materialization-wal-interrupt-convergence");
    const appendA = yield* makeGate();
    const appendB = yield* makeGate();
    const completeA = yield* makeGate();
    const convergenceRead = yield* Deferred.make<void>();
    const engineA = yield* buildEngine(
      harness.sqlA,
      harness.scopeA,
      gatedHooks({ beforeAppend: appendA, beforeComplete: completeA }),
    );
    const engineB = yield* buildEngine(
      harness.sqlB,
      harness.scopeB,
      {
        ...gatedHooks({ beforeAppend: appendB }),
        beforeConvergenceReceiptRead: () =>
          Deferred.succeed(convergenceRead, undefined).pipe(Effect.asVoid),
      },
      { maximumReadAttempts: 5, delayBetweenAttempts: "5 seconds" },
    );
    const fiberA = yield* engineA.dispatchAgentControl(command).pipe(Effect.forkChild);
    const fiberB = yield* engineB.dispatchAgentControl(command).pipe(Effect.forkChild);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], { concurrency: "unbounded" });
    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(appendB);
    yield* Deferred.await(convergenceRead).pipe(Effect.timeout(timeout));
    yield* Effect.sleep("20 millis");

    assert.deepStrictEqual(
      yield* harness.sqlB<{ readonly projectCount: number }>`
        SELECT COUNT(*) AS "projectCount"
        FROM projection_projects
        WHERE project_id = ${projectId}
      `.pipe(Effect.timeout("1 second")),
      [{ projectCount: 1 }],
    );

    yield* Fiber.interrupt(fiberB);
    const interrupted = yield* Fiber.await(fiberB);
    assert.strictEqual(Exit.isFailure(interrupted), true);
    if (Exit.isFailure(interrupted)) {
      assert.strictEqual(Cause.hasInterruptsOnly(interrupted.cause), true);
    }
    yield* Fiber.interrupt(fiberA);
    const winnerInterrupted = yield* Fiber.await(fiberA);
    assert.strictEqual(Exit.isFailure(winnerInterrupted), true);
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 0,
      threads: 0,
      acceptedIntents: 0,
      allIntents: 0,
      acceptedReceipts: 0,
      allReceipts: 0,
    });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("returns the original SQLite conflict when no winner receipt becomes visible", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-no-receipt");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-no-receipt");
    const command = yield* makeCommand(projectId, "materialization-wal-no-receipt");
    const appendA = yield* makeGate();
    const appendB = yield* makeGate();
    const completeA = yield* makeGate();
    const engineA = yield* buildEngine(
      harness.sqlA,
      harness.scopeA,
      gatedHooks({ beforeAppend: appendA, beforeComplete: completeA }),
    );
    const engineB = yield* buildEngine(
      harness.sqlB,
      harness.scopeB,
      gatedHooks({ beforeAppend: appendB }),
      { maximumReadAttempts: 2, delayBetweenAttempts: "1 millis" },
    );
    const fiberA = yield* engineA.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    const fiberB = yield* engineB.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], { concurrency: "unbounded" });
    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(appendB);

    const loser = yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isFailure(loser), true);
    if (Exit.isFailure(loser)) {
      const failure = Cause.squash(loser.cause) as {
        readonly _tag?: string;
        readonly operation?: string;
      };
      assert.strictEqual(failure._tag, "PersistenceSqlError");
      assert.include(failure.operation ?? "", "appendAgentControlThreadMaterialization");
    }
    assert.deepStrictEqual(yield* counts(harness.sqlB, command), {
      events: 0,
      threads: 0,
      acceptedIntents: 0,
      allIntents: 0,
      acceptedReceipts: 0,
      allReceipts: 0,
    });
    yield* Fiber.interrupt(fiberA);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("returns the original SQLite conflict when the convergence receipt read fails", () =>
  Effect.gen(function* () {
    const harness = yield* makeSharedHarness();
    const projectId = ProjectId.make("materialization-wal-receipt-read");
    const seedEngine = yield* buildEngine(harness.sqlA, harness.scopeA, noHooks);
    yield* seedProject(seedEngine, projectId, "wal-receipt-read");
    const command = yield* makeCommand(projectId, "materialization-wal-receipt-read");
    const appendA = yield* makeGate();
    const appendB = yield* makeGate();
    const completeA = yield* makeGate();
    const engineA = yield* buildEngine(
      harness.sqlA,
      harness.scopeA,
      gatedHooks({ beforeAppend: appendA, beforeComplete: completeA }),
    );
    const engineB = yield* buildEngine(harness.sqlB, harness.scopeB, {
      ...gatedHooks({ beforeAppend: appendB }),
      beforeConvergenceReceiptRead: () =>
        harness.sqlB`
          ALTER TABLE orchestration_command_receipts
          RENAME TO orchestration_command_receipts_unreadable
        `.pipe(Effect.asVoid, Effect.orDie),
    });
    const fiberA = yield* engineA.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    const fiberB = yield* engineB.dispatchAgentControl(command).pipe(Effect.exit, Effect.forkChild);
    yield* Effect.all([awaitGate(appendA), awaitGate(appendB)], { concurrency: "unbounded" });
    yield* releaseGate(appendA);
    yield* awaitGate(completeA);
    yield* releaseGate(completeA);
    assert.strictEqual(
      Exit.isSuccess(yield* Fiber.join(fiberA).pipe(Effect.timeout(timeout))),
      true,
    );
    yield* releaseGate(appendB);

    const loser = yield* Fiber.join(fiberB).pipe(Effect.timeout(timeout));
    assert.strictEqual(Exit.isFailure(loser), true);
    if (Exit.isFailure(loser)) {
      const failure = Cause.squash(loser.cause) as {
        readonly _tag?: string;
        readonly operation?: string;
      };
      assert.strictEqual(failure._tag, "PersistenceSqlError");
      assert.include(failure.operation ?? "", "appendAgentControlThreadMaterialization");
    }
    assert.deepStrictEqual(
      yield* harness.sqlB<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'table' AND name = 'orchestration_command_receipts'
      `,
      [{ name: "orchestration_command_receipts" }],
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.live("leaves the state-dependent loser receiptless after another command wins", () =>
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
      allIntents: 1,
      acceptedReceipts: 1,
      allReceipts: 1,
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
    assert.deepStrictEqual(receipts, [{ commandId: winnerCommand.commandId, status: "accepted" }]);
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
  const identityMutations: ReadonlyArray<{
    readonly name: string;
    readonly mutate: (
      command: AgentControlThreadMaterializeCommand,
    ) => AgentControlThreadMaterializeCommand;
  }> = [
    {
      name: "stage-run-id",
      mutate: (command) => ({
        ...command,
        stageRunId: AgentControlStageRunId.make("stage-run-noncanonical"),
      }),
    },
    {
      name: "attempt-id",
      mutate: (command) => ({
        ...command,
        attemptId: AgentControlAttemptId.make("attempt-noncanonical"),
      }),
    },
    {
      name: "lease-id",
      mutate: (command) => ({
        ...command,
        leaseId: AgentControlStageRunLeaseId.make("lease-noncanonical"),
      }),
    },
    {
      name: "reservation-id",
      mutate: (command) => ({
        ...command,
        controlledThreadReservationId:
          `${command.controlledThreadReservationId}-noncanonical` as AgentControlThreadMaterializeCommand["controlledThreadReservationId"],
      }),
    },
    {
      name: "thread-id",
      mutate: (command) => ({
        ...command,
        threadId:
          `${command.threadId}-noncanonical` as AgentControlThreadMaterializeCommand["threadId"],
      }),
    },
  ];

  for (const identityMutation of identityMutations) {
    it.effect(`rejects a noncanonical ${identityMutation.name} before every write`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const engine = yield* buildEngine(sql, engineScope, noHooks);
        const projectId = ProjectId.make(`materialization-identity-${identityMutation.name}`);
        yield* seedProject(engine, projectId, `identity-${identityMutation.name}`);
        const canonical = yield* makeCommand(
          projectId,
          `materialization-identity-${identityMutation.name}`,
        );
        const command = identityMutation.mutate(canonical);

        const failure = yield* Effect.flip(engine.dispatchAgentControl(command));
        assert.strictEqual(failure._tag, "OrchestrationCommandIdentityConflictError");
        assert.deepStrictEqual(yield* counts(sql, command), {
          events: 0,
          threads: 0,
          acceptedIntents: 0,
          allIntents: 0,
          acceptedReceipts: 0,
          allReceipts: 0,
        });
        yield* engine.dispatchAgentControl(canonical);
        assert.deepStrictEqual(yield* counts(sql, canonical), {
          events: 2,
          threads: 1,
          acceptedIntents: 1,
          allIntents: 1,
          acceptedReceipts: 1,
          allReceipts: 1,
        });
      }),
    );
  }

  it.effect("allows a regular later thread command at stream version three", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-version-three-project");
      yield* seedProject(engine, projectId, "version-three");
      const command = yield* makeCommand(projectId, "materialization-version-three");
      yield* engine.dispatchAgentControl(command);

      const later = yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("materialization-regular-version-three"),
        threadId: command.threadId,
        title: "Later regular command",
      });
      assert.deepStrictEqual(
        yield* sql<{
          readonly commandId: string;
          readonly streamVersion: number;
          readonly sequence: number;
        }>`
          SELECT
            command_id AS "commandId", stream_version AS "streamVersion", sequence
          FROM orchestration_events
          WHERE command_id = 'materialization-regular-version-three'
        `,
        [
          {
            commandId: "materialization-regular-version-three",
            streamVersion: 3,
            sequence: later.sequence,
          },
        ],
      );
    }),
  );

  const replayAfterLaterEvents: ReadonlyArray<{
    readonly name: string;
    readonly apply: (
      engine: OrchestrationEngineService["Service"],
      command: AgentControlThreadMaterializeCommand,
    ) => Effect.Effect<void, OrchestrationDispatchError>;
  }> = [
    {
      name: "thread.meta.update@3",
      apply: (engine, command) =>
        engine
          .dispatch({
            type: "thread.meta.update",
            commandId: CommandId.make("materialization-replay-after-meta"),
            threadId: command.threadId,
            title: "Later title",
            modelSelection: {
              instanceId: ProviderInstanceId.make("codex"),
              model: "gpt-5.6-later",
              options: [{ id: "reasoning-effort", value: "xhigh" }],
            },
          })
          .pipe(Effect.asVoid),
    },
    {
      name: "legitimate runtime change",
      apply: (engine, command) =>
        engine
          .dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("materialization-replay-after-runtime"),
            threadId: command.threadId,
            runtimeMode: "full-access",
            createdAt: "2026-07-27T11:00:01.000Z",
          })
          .pipe(Effect.asVoid),
    },
    {
      name: "legitimate control-state change",
      apply: (engine, command) =>
        engine
          .dispatchAgentControl({
            type: "thread.agent-control.state.set",
            commandId: CommandId.make("materialization-replay-after-control-state"),
            threadId: command.threadId,
            controlState: "taken-over",
            createdAt: "2026-07-27T11:00:01.000Z",
          })
          .pipe(Effect.asVoid),
    },
    {
      name: "multiple legitimate later events",
      apply: (engine, command) =>
        Effect.gen(function* () {
          yield* engine.dispatch({
            type: "thread.meta.update",
            commandId: CommandId.make("materialization-replay-multiple-meta"),
            threadId: command.threadId,
            title: "Multiple later title",
          });
          yield* engine.dispatch({
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("materialization-replay-multiple-runtime"),
            threadId: command.threadId,
            runtimeMode: "auto-accept-edits",
            createdAt: "2026-07-27T11:00:02.000Z",
          });
          yield* engine.dispatchAgentControl({
            type: "thread.agent-control.state.set",
            commandId: CommandId.make("materialization-replay-multiple-control"),
            threadId: command.threadId,
            controlState: "taken-over",
            createdAt: "2026-07-27T11:00:03.000Z",
          });
        }),
    },
  ];

  for (const scenario of replayAfterLaterEvents) {
    it.effect(`replays the original materialization after ${scenario.name}`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const engine = yield* buildEngine(sql, engineScope, noHooks);
        const projectId = ProjectId.make(`materialization-replay-${scenario.name}`);
        yield* seedProject(engine, projectId, `replay-${scenario.name}`);
        const command = yield* makeCommand(
          projectId,
          `materialization-replay-original-${scenario.name}`,
        );
        const original = yield* engine.dispatchAgentControl(command);
        yield* scenario.apply(engine, command);

        const before = yield* sql<{
          readonly events: number;
          readonly intents: number;
          readonly receipts: number;
          readonly markers: number;
        }>`
          SELECT
            (SELECT count(*) FROM orchestration_events
             WHERE stream_id = ${command.threadId}) AS events,
            (SELECT count(*)
             FROM orchestration_agent_control_thread_materialization_intents
             WHERE command_id = ${command.commandId}) AS intents,
            (SELECT count(*) FROM orchestration_command_receipts
             WHERE aggregate_id = ${command.threadId}) AS receipts,
            (SELECT count(*)
             FROM orchestration_agent_control_thread_materialization_receipts
             WHERE command_id = ${command.commandId}) AS markers
        `;
        const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
        const publication = yield* Stream.runHead(yield* subscribe).pipe(Effect.forkChild);

        assert.deepStrictEqual(yield* engine.dispatchAgentControl(command), original);
        assert.deepStrictEqual(
          yield* sql<{
            readonly events: number;
            readonly intents: number;
            readonly receipts: number;
            readonly markers: number;
          }>`
            SELECT
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${command.threadId}) AS events,
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_intents
               WHERE command_id = ${command.commandId}) AS intents,
              (SELECT count(*) FROM orchestration_command_receipts
               WHERE aggregate_id = ${command.threadId}) AS receipts,
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_receipts
               WHERE command_id = ${command.commandId}) AS markers
          `,
          before,
        );
        yield* Effect.yieldNow;
        assert.strictEqual(publication.pollUnsafe(), undefined);
        yield* Fiber.interrupt(publication);
      }),
    );
  }

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
          "materialization-marker",
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
                      : phase === "receipt"
                        ? `CREATE TEMP TRIGGER ${trigger}
                           BEFORE INSERT ON orchestration_command_receipts
                           WHEN NEW.command_id = '${escapedCommand}'
                           BEGIN SELECT RAISE(ABORT, 'receipt fault'); END`
                        : `CREATE TEMP TRIGGER ${trigger}
                           BEFORE INSERT ON orchestration_agent_control_thread_materialization_receipts
                           WHEN NEW.command_id = '${escapedCommand}'
                           BEGIN SELECT RAISE(ABORT, 'materialization marker fault'); END`;
          yield* sql.unsafe(ddl).unprepared;
          const publicationsBeforeFailure = yield* Ref.get(publicationCount);
          const readModelSequenceBeforeFailure = yield* engine.latestSequence;
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
          assert.deepStrictEqual(
            yield* sql<{ readonly markers: number }>`
              SELECT COUNT(*) AS markers
              FROM orchestration_agent_control_thread_materialization_receipts
              WHERE command_id = ${command.commandId}
            `,
            [{ markers: 0 }],
          );
          assert.strictEqual(yield* engine.latestSequence, readModelSequenceBeforeFailure);
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
          assert.deepStrictEqual(
            yield* sql<{ readonly markers: number }>`
              SELECT COUNT(*) AS markers
              FROM orchestration_agent_control_thread_materialization_receipts
              WHERE command_id = ${command.commandId}
            `,
            [{ markers: 1 }],
          );
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
      const originalError = Exit.isFailure(first) ? Cause.squash(first.cause) : undefined;
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
      const replayDetail =
        replay._tag === "OrchestrationCommandPreviouslyRejectedError" ? replay.detail : "";
      assert.strictEqual(replayDetail, originalError instanceof Error ? originalError.message : "");
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            sql`
              UPDATE orchestration_command_receipts
              SET error = 'changed historical rejection'
              WHERE command_id = ${rejected.commandId}
            `,
          ),
        ),
        true,
      );
      const replayAfterMutation = yield* Effect.flip(engine.dispatchAgentControl(rejected));
      assert.strictEqual(replayAfterMutation._tag, "OrchestrationCommandPreviouslyRejectedError");
      assert.strictEqual(
        replayAfterMutation._tag === "OrchestrationCommandPreviouslyRejectedError"
          ? replayAfterMutation.detail
          : "",
        replayDetail,
      );
      const changed = yield* Effect.flip(
        engine.dispatchAgentControl({ ...rejected, title: "Changed rejection" }),
      );
      assert.strictEqual(changed._tag, "OrchestrationCommandIdentityConflictError");
    }),
  );

  it.effect(
    "validates all rejected replay evidence before returning the historical rejection",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const engine = yield* buildEngine(sql, engineScope, noHooks);
        const projectId = ProjectId.make("materialization-rejected-evidence-project");
        yield* seedProject(engine, projectId, "rejected-evidence");

        const reject = Effect.fn("rejectMaterializationForEvidenceTest")(function* (
          suffix: string,
        ) {
          const canonical = yield* makeCommand(projectId, `materialization-rejected-${suffix}`);
          const command = { ...canonical, runtimeMode: "full-access" as const };
          assert.strictEqual(
            Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
            true,
          );
          return command;
        });
        const replayFailsClosed = Effect.fn("replayRejectedMaterializationFailsClosed")(function* (
          command: AgentControlThreadMaterializeCommand,
        ) {
          const failure = yield* Effect.flip(engine.dispatchAgentControl(command));
          assert.strictEqual(failure._tag, "PersistenceDecodeError");
        });

        const foreignProjectEvent = yield* reject("foreign-project-event");
        yield* sql`DROP TRIGGER IF EXISTS trg_orchestration_rejected_materialization_event_insert`;
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'foreign-project-event-for-rejected-materialization',
          'project', 'foreign-project-for-rejected-materialization', 1,
          'project.created', ${NOW}, ${foreignProjectEvent.commandId}, NULL,
          ${foreignProjectEvent.commandId}, 'server',
          '{"projectId":"foreign-project-for-rejected-materialization","title":"Foreign","workspaceRoot":"/tmp/foreign","defaultModelSelection":null,"scripts":[],"createdAt":"2026-07-27T11:00:00.000Z","updatedAt":"2026-07-27T11:00:00.000Z"}',
          '{}'
        )
      `;
        yield* replayFailsClosed(foreignProjectEvent);

        const foreignThreadEvent = yield* reject("foreign-thread-event");
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'foreign-thread-event-for-rejected-materialization',
          'thread', 'foreign-thread-for-rejected-materialization', 1,
          'thread.meta-updated', ${NOW}, ${foreignThreadEvent.commandId}, NULL,
          ${foreignThreadEvent.commandId}, 'server',
          '{"threadId":"foreign-thread-for-rejected-materialization","title":"Foreign","updatedAt":"2026-07-27T11:00:00.000Z"}',
          '{}'
        )
      `;
        yield* replayFailsClosed(foreignThreadEvent);

        const projectedThread = yield* reject("thread-projection");
        const manualThreadId = `manual-${projectedThread.commandId}`;
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-${manualThreadId}`),
          threadId: projectedThread.threadId.replace(
            "t3-auto-reserved-thread-",
            "manual-thread-",
          ) as AgentControlThreadMaterializeCommand["threadId"],
          projectId,
          title: "Manual projection",
          modelSelection: projectedThread.modelSelection,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: NOW,
        });
        yield* sql`
        UPDATE projection_threads
        SET thread_id = ${projectedThread.threadId}
        WHERE title = 'Manual projection'
      `;
        yield* replayFailsClosed(projectedThread);

        const projectedBinding = yield* reject("binding-projection");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make(`create-binding-${manualThreadId}`),
          threadId: projectedBinding.threadId.replace(
            "t3-auto-reserved-thread-",
            "manual-binding-thread-",
          ) as AgentControlThreadMaterializeCommand["threadId"],
          projectId,
          title: "Manual binding projection",
          modelSelection: projectedBinding.modelSelection,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: NOW,
        });
        yield* sql`
        UPDATE projection_threads
        SET
          thread_id = ${projectedBinding.threadId},
          agent_control_json = (
            SELECT binding_json
            FROM orchestration_agent_control_thread_materialization_intents
            WHERE command_id = ${projectedBinding.commandId}
          )
        WHERE title = 'Manual binding projection'
      `;
        yield* replayFailsClosed(projectedBinding);

        const acceptedContradiction = yield* reject("accepted-contradiction");
        yield* sql`DROP TRIGGER IF EXISTS trg_orchestration_materialization_intent_immutable_update`;
        yield* sql`DROP TRIGGER IF EXISTS trg_orchestration_materialization_receipt_immutable_update`;
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql`PRAGMA ignore_check_constraints = ON`;
        yield* sql`
        UPDATE orchestration_command_receipts
        SET status = 'accepted', error = NULL
        WHERE command_id = ${acceptedContradiction.commandId}
      `;
        yield* sql`
        UPDATE orchestration_agent_control_thread_materialization_intents
        SET receipt_status = 'accepted', receipt_error = NULL
        WHERE command_id = ${acceptedContradiction.commandId}
      `;
        yield* sql`PRAGMA ignore_check_constraints = OFF`;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* replayFailsClosed(acceptedContradiction);
      }),
  );

  it.effect(
    "rejects accepted and rejected replay with internally consistent noncanonical stored identity",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const crypto = yield* Crypto.Crypto;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations();
        const engineScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
        const engine = yield* buildEngine(sql, engineScope, noHooks);
        const projectId = ProjectId.make("materialization-stored-identity-project");
        yield* seedProject(engine, projectId, "stored-identity");

        const accepted = yield* makeCommand(projectId, "materialization-stored-accepted");
        yield* engine.dispatchAgentControl(accepted);
        const noncanonicalAccepted = {
          ...accepted,
          threadId:
            `${accepted.threadId}-stored-noncanonical` as AgentControlThreadMaterializeCommand["threadId"],
        };
        const acceptedFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
          crypto,
          noncanonicalAccepted,
        );

        yield* sql`
          DROP TRIGGER IF EXISTS trg_orchestration_materialization_event_immutable_update
        `;
        yield* sql`
          DROP TRIGGER IF EXISTS trg_orchestration_materialization_receipt_immutable_update
        `;
        yield* sql`
          DROP TRIGGER IF EXISTS trg_orchestration_materialization_intent_immutable_update
        `;
        yield* sql`
          DROP TRIGGER IF EXISTS
            trg_orchestration_materialization_receipt_evidence_immutable_update
        `;
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              UPDATE orchestration_events
              SET
                stream_id = ${noncanonicalAccepted.threadId},
                payload_json = json_set(
                  payload_json,
                  '$.threadId',
                  ${noncanonicalAccepted.threadId}
                )
              WHERE command_id = ${accepted.commandId}
            `;
            yield* sql`
              UPDATE projection_threads
              SET thread_id = ${noncanonicalAccepted.threadId}
              WHERE thread_id = ${accepted.threadId}
            `;
            yield* sql`
              UPDATE orchestration_command_receipts
              SET aggregate_id = ${noncanonicalAccepted.threadId}
              WHERE command_id = ${accepted.commandId}
            `;
            yield* sql`
              UPDATE orchestration_agent_control_thread_materialization_intents
              SET
                thread_id = ${noncanonicalAccepted.threadId},
                command_fingerprint = ${acceptedFingerprint}
              WHERE command_id = ${accepted.commandId}
            `;
            yield* sql`
              UPDATE orchestration_agent_control_thread_materialization_receipts
              SET
                thread_id = ${noncanonicalAccepted.threadId},
                command_fingerprint = ${acceptedFingerprint}
              WHERE command_id = ${accepted.commandId}
            `;
          }),
        );

        const rejectedCanonical = yield* makeCommand(projectId, "materialization-stored-rejected");
        const rejected = { ...rejectedCanonical, runtimeMode: "full-access" as const };
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(rejected))),
          true,
        );
        const noncanonicalRejected = {
          ...rejected,
          threadId:
            `${rejected.threadId}-stored-noncanonical` as AgentControlThreadMaterializeCommand["threadId"],
        };
        const rejectedFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
          crypto,
          noncanonicalRejected,
        );
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              UPDATE orchestration_command_receipts
              SET aggregate_id = ${noncanonicalRejected.threadId}
              WHERE command_id = ${rejected.commandId}
            `;
            yield* sql`
              UPDATE orchestration_agent_control_thread_materialization_intents
              SET
                thread_id = ${noncanonicalRejected.threadId},
                command_fingerprint = ${rejectedFingerprint}
              WHERE command_id = ${rejected.commandId}
            `;
          }),
        );
        yield* sql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`, []);

        const acceptedReplay = yield* Effect.flip(engine.dispatchAgentControl(accepted));
        assert.strictEqual(acceptedReplay._tag, "OrchestrationCommandIdentityConflictError");
        const rejectedReplay = yield* Effect.flip(engine.dispatchAgentControl(rejected));
        assert.strictEqual(rejectedReplay._tag, "OrchestrationCommandIdentityConflictError");
        const noncanonicalAcceptedReplay = yield* Effect.flip(
          engine.dispatchAgentControl(noncanonicalAccepted),
        );
        assert.strictEqual(
          noncanonicalAcceptedReplay._tag,
          "OrchestrationCommandIdentityConflictError",
        );
        const noncanonicalRejectedReplay = yield* Effect.flip(
          engine.dispatchAgentControl(noncanonicalRejected),
        );
        assert.strictEqual(
          noncanonicalRejectedReplay._tag,
          "OrchestrationCommandIdentityConflictError",
        );
      }),
  );

  it.effect("keeps state-dependent and infrastructure materialization failures receiptless", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const command = yield* makeCommand(
        ProjectId.make("materialization-missing-project"),
        "materialization-missing-project-command",
      );

      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
        true,
      );
      assert.deepStrictEqual(yield* counts(sql, command), {
        events: 0,
        threads: 0,
        acceptedIntents: 0,
        allIntents: 0,
        acceptedReceipts: 0,
        allReceipts: 0,
      });
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

      const missingProjectionCommand = yield* makeCommand(
        projectId,
        "materialization-missing-projection",
      );
      yield* engine.dispatchAgentControl(missingProjectionCommand);
      yield* sql`
        DELETE FROM projection_threads
        WHERE thread_id = ${missingProjectionCommand.threadId}
      `;
      const missingProjection = yield* Effect.flip(
        engine.dispatchAgentControl(missingProjectionCommand),
      );
      assert.strictEqual(missingProjection._tag, "PersistenceDecodeError");

      const eventCommand = yield* makeCommand(projectId, "materialization-corrupt-event");
      yield* engine.dispatchAgentControl(eventCommand);
      yield* sql`DROP TRIGGER IF EXISTS trg_orchestration_materialization_event_immutable_update`;
      yield* sql`DROP TRIGGER IF EXISTS agent_control_orchestration_event_update_storage_validate`;
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

      const duplicateEventCommand = yield* makeCommand(
        projectId,
        "materialization-duplicate-event-json",
      );
      yield* engine.dispatchAgentControl(duplicateEventCommand);
      yield* sql`
        UPDATE orchestration_events
        SET payload_json = replace(
          payload_json,
          '"threadId":',
          '"threadId":"duplicate","threadId":'
        )
        WHERE command_id = ${duplicateEventCommand.commandId}
          AND event_type = 'thread.created'
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(duplicateEventCommand))),
        true,
      );

      const eventVersionCommand = yield* makeCommand(
        projectId,
        "materialization-corrupt-event-version",
      );
      yield* engine.dispatchAgentControl(eventVersionCommand);
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`
        UPDATE orchestration_events
        SET stream_version = 0
        WHERE command_id = ${eventVersionCommand.commandId}
          AND event_type = 'thread.created'
      `;
      yield* sql`PRAGMA foreign_keys = ON`;
      const corruptEventVersion = yield* Effect.flip(
        engine.dispatchAgentControl(eventVersionCommand),
      );
      assert.strictEqual(corruptEventVersion._tag, "PersistenceDecodeError");

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

      const laterProjectionCommand = yield* makeCommand(
        projectId,
        "materialization-corrupt-current-projection",
      );
      yield* engine.dispatchAgentControl(laterProjectionCommand);
      yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("materialization-corrupt-current-projection-later"),
        threadId: laterProjectionCommand.threadId,
        title: "Legitimate later title",
      });
      yield* sql`
        UPDATE projection_threads
        SET title = 'Not derivable from the complete stream'
        WHERE thread_id = ${laterProjectionCommand.threadId}
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(laterProjectionCommand))),
        true,
      );

      const intentCommand = yield* makeCommand(projectId, "materialization-corrupt-intent");
      yield* engine.dispatchAgentControl(intentCommand);
      yield* sql`DROP TRIGGER IF EXISTS trg_orchestration_materialization_intent_immutable_update`;
      yield* sql`
        UPDATE orchestration_agent_control_thread_materialization_intents
        SET title = 'Corrupted intent'
        WHERE command_id = ${intentCommand.commandId}
      `;
      const corruptIntent = yield* Effect.flip(engine.dispatchAgentControl(intentCommand));
      assert.strictEqual(corruptIntent._tag, "PersistenceDecodeError");

      const duplicateIntentCommand = yield* makeCommand(
        projectId,
        "materialization-duplicate-intent-json",
      );
      yield* engine.dispatchAgentControl(duplicateIntentCommand);
      yield* sql`
        UPDATE orchestration_agent_control_thread_materialization_intents
        SET model_selection_json = replace(
          model_selection_json,
          '{"instanceId":',
          '{"instanceId":"duplicate","instanceId":'
        )
        WHERE command_id = ${duplicateIntentCommand.commandId}
      `;
      assert.strictEqual(
        Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(duplicateIntentCommand))),
        true,
      );

      const intentVersionCommand = yield* makeCommand(
        projectId,
        "materialization-corrupt-intent-version",
      );
      yield* engine.dispatchAgentControl(intentVersionCommand);
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`PRAGMA ignore_check_constraints = ON`;
      yield* sql`
        UPDATE orchestration_agent_control_thread_materialization_intents
        SET created_event_stream_version = 0
        WHERE command_id = ${intentVersionCommand.commandId}
      `;
      yield* sql`PRAGMA ignore_check_constraints = OFF`;
      yield* sql`PRAGMA foreign_keys = ON`;
      const corruptIntentVersion = yield* Effect.flip(
        engine.dispatchAgentControl(intentVersionCommand),
      );
      assert.strictEqual(corruptIntentVersion._tag, "PersistenceDecodeError");
    }),
  );

  it.effect("fails closed for every accepted receipt and marker coordinate", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-receipt-matrix-project");
      yield* seedProject(engine, projectId, "receipt-matrix");
      const publicationCount = yield* Ref.make(0);
      const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
      const publicationConsumer = yield* (yield* subscribe).pipe(
        Stream.runForEach(() => Ref.update(publicationCount, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* sql`
        DROP TRIGGER IF EXISTS trg_orchestration_materialization_receipt_immutable_update
      `;
      yield* sql`
        DROP TRIGGER IF EXISTS
          trg_orchestration_materialization_receipt_evidence_immutable_update
      `;
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`PRAGMA ignore_check_constraints = ON`;

      const mutations: ReadonlyArray<{
        readonly name: string;
        readonly mutate: (
          command: AgentControlThreadMaterializeCommand,
        ) => Effect.Effect<void, SqlError>;
        readonly restore: (
          command: AgentControlThreadMaterializeCommand,
        ) => Effect.Effect<void, SqlError>;
      }> = [
        {
          name: "command-id",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET command_id = ${`corrupt-${command.commandId}`}
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET command_id = ${command.commandId}
              WHERE command_id = ${`corrupt-${command.commandId}`}
            `.pipe(Effect.asVoid),
        },
        {
          name: "authority",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET authority = 'system'
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET authority = 'agent-control'
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "aggregate",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET aggregate_kind = 'project', aggregate_id = 'foreign-aggregate'
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET aggregate_kind = 'thread', aggregate_id = ${command.threadId}
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "result-sequence",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET result_sequence = result_sequence + 100
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET result_sequence = (
                SELECT binding_event_sequence
                FROM orchestration_agent_control_thread_materialization_intents
                WHERE command_id = ${command.commandId}
              )
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "accepted-at",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET accepted_at = '2026-07-27T11:59:59.000Z'
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET accepted_at = ${command.createdAt}
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "status",
          mutate: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET status = 'rejected', error = 'corrupt'
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_command_receipts
              SET status = 'accepted', error = NULL
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "fingerprint",
          mutate: (command) =>
            sql`
              UPDATE orchestration_agent_control_thread_materialization_receipts
              SET command_fingerprint = ${"c".repeat(64)}
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_agent_control_thread_materialization_receipts
              SET command_fingerprint = (
                SELECT command_fingerprint
                FROM orchestration_agent_control_thread_materialization_intents
                WHERE command_id = ${command.commandId}
              )
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "marker-event-stream-coordinate",
          mutate: (command) =>
            sql`
              UPDATE orchestration_agent_control_thread_materialization_receipts
              SET result_sequence = result_sequence + 100
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
          restore: (command) =>
            sql`
              UPDATE orchestration_agent_control_thread_materialization_receipts
              SET result_sequence = (
                SELECT binding_event_sequence
                FROM orchestration_agent_control_thread_materialization_intents
                WHERE command_id = ${command.commandId}
              )
              WHERE command_id = ${command.commandId}
            `.pipe(Effect.asVoid),
        },
      ];

      for (const mutation of mutations) {
        const command = yield* makeCommand(
          projectId,
          `materialization-receipt-matrix-${mutation.name}`,
        );
        yield* engine.dispatchAgentControl(command);
        yield* Effect.yieldNow;
        yield* sql.withTransaction(mutation.mutate(command));
        const beforeReplay = yield* counts(sql, command);
        const publicationsBeforeReplay = yield* Ref.get(publicationCount);
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        assert.deepStrictEqual(yield* counts(sql, command), beforeReplay);
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);
        yield* sql.withTransaction(mutation.restore(command));
        assert.deepStrictEqual(yield* engine.dispatchAgentControl(command), {
          sequence: (yield* sql<{ readonly sequence: number }>`
                SELECT binding_event_sequence AS sequence
                FROM orchestration_agent_control_thread_materialization_intents
                WHERE command_id = ${command.commandId}
              `)[0]!.sequence,
        });
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);
      }

      yield* sql`PRAGMA ignore_check_constraints = OFF`;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* Fiber.interrupt(publicationConsumer);
    }),
  );

  it.effect("fails closed for every materialization event coordinate", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-event-matrix-project");
      yield* seedProject(engine, projectId, "event-matrix");
      const publicationCount = yield* Ref.make(0);
      const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
      const publicationConsumer = yield* (yield* subscribe).pipe(
        Stream.runForEach(() => Ref.update(publicationCount, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      yield* sql`
        DROP TRIGGER IF EXISTS trg_orchestration_materialization_event_immutable_update
      `;
      yield* sql`PRAGMA foreign_keys = OFF`;

      const mutations: ReadonlyArray<{
        readonly name: string;
        readonly set: string;
        readonly restore: string;
      }> = [
        {
          name: "event-type",
          set: "event_type = 'thread.agent-control-bound'",
          restore: "event_type = 'thread.created'",
        },
        {
          name: "stream-version",
          set: "stream_version = 99",
          restore: "stream_version = 1",
        },
        {
          name: "thread-id",
          set: "stream_id = 'foreign-thread'",
          restore: "",
        },
        {
          name: "aggregate-kind",
          set: "aggregate_kind = 'project'",
          restore: "aggregate_kind = 'thread'",
        },
        {
          name: "global-sequence",
          set: "sequence = sequence + 1000000",
          restore: "",
        },
        {
          name: "command-id",
          set: "command_id = 'foreign-command'",
          restore: "",
        },
      ];

      for (const mutation of mutations) {
        const command = yield* makeCommand(
          projectId,
          `materialization-event-matrix-${mutation.name}`,
        );
        yield* engine.dispatchAgentControl(command);
        yield* Effect.yieldNow;
        const original = (yield* sql<{ readonly eventId: string; readonly sequence: number }>`
            SELECT event_id AS "eventId", sequence
            FROM orchestration_events
            WHERE command_id = ${command.commandId}
              AND event_type = 'thread.created'
          `)[0]!;
        yield* sql.unsafe(
          `UPDATE orchestration_events SET ${mutation.set}
           WHERE event_id = '${original.eventId.replaceAll("'", "''")}'`,
        ).unprepared;
        const beforeReplay = yield* counts(sql, command);
        const publicationsBeforeReplay = yield* Ref.get(publicationCount);
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        assert.deepStrictEqual(yield* counts(sql, command), beforeReplay);
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);

        const restore =
          mutation.name === "thread-id"
            ? `stream_id = '${command.threadId.replaceAll("'", "''")}'`
            : mutation.name === "global-sequence"
              ? `sequence = ${original.sequence}`
              : mutation.name === "command-id"
                ? `command_id = '${command.commandId.replaceAll("'", "''")}'`
                : mutation.restore;
        yield* sql.unsafe(
          `UPDATE orchestration_events SET ${restore}
           WHERE event_id = '${original.eventId.replaceAll("'", "''")}'`,
        ).unprepared;
        assert.strictEqual(
          Exit.isSuccess(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);
      }

      yield* sql`PRAGMA foreign_keys = ON`;
      yield* Fiber.interrupt(publicationConsumer);
    }),
  );

  it.effect("fails closed for every materialization projection coordinate", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const engineScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(engineScope, Exit.void));
      const engine = yield* buildEngine(sql, engineScope, noHooks);
      const projectId = ProjectId.make("materialization-projection-matrix-project");
      yield* seedProject(engine, projectId, "projection-matrix");
      const publicationCount = yield* Ref.make(0);
      const subscribe = engine.subscribeDomainEvents ?? Effect.die("subscription unavailable");
      const publicationConsumer = yield* (yield* subscribe).pipe(
        Stream.runForEach(() => Ref.update(publicationCount, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const mutations = [
        "thread_id = 'foreign-thread'",
        "project_id = 'foreign-project'",
        "title = 'Foreign title'",
        "model_selection_json = json_set(model_selection_json, '$.model', 'foreign-model')",
        `model_selection_json = replace(
          model_selection_json,
          '{"instanceId":',
          '{"instanceId":"duplicate","instanceId":'
        )`,
        `model_selection_json = replace(
          model_selection_json,
          '"id":"reasoning-effort"',
          '"id":"duplicate","id":"reasoning-effort"'
        )`,
        "runtime_mode = 'full-access'",
        "interaction_mode = 'default'",
        "branch = 'foreign-branch'",
        "worktree_path = '/tmp/foreign-worktree'",
        "agent_control_json = json_set(agent_control_json, '$.taskId', 'foreign-task')",
        "agent_control_json = json_set(agent_control_json, '$.stageRunId', 'foreign-stage')",
        "agent_control_json = json_set(agent_control_json, '$.attemptId', 'foreign-attempt')",
        "agent_control_json = json_set(agent_control_json, '$.roleId', 'foreign-role')",
        "agent_control_json = json_set(agent_control_json, '$.controlState', 'taken-over')",
        `agent_control_json = replace(
          agent_control_json,
          '"taskId":',
          '"taskId":"duplicate","taskId":'
        )`,
        "created_at = '2026-07-27T11:59:50.000Z'",
        "updated_at = '2026-07-27T11:59:51.000Z'",
        "archived_at = '2026-07-27T11:59:52.000Z'",
        "deleted_at = '2026-07-27T11:59:53.000Z'",
      ] as const;

      for (const [index, mutation] of mutations.entries()) {
        const command = yield* makeCommand(projectId, `materialization-projection-matrix-${index}`);
        yield* engine.dispatchAgentControl(command);
        yield* Effect.yieldNow;
        yield* sql.unsafe(
          `UPDATE projection_threads SET ${mutation}
           WHERE thread_id = '${command.threadId.replaceAll("'", "''")}'`,
        ).unprepared;
        const beforeReplay = yield* counts(sql, command);
        const publicationsBeforeReplay = yield* Ref.get(publicationCount);
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        assert.deepStrictEqual(yield* counts(sql, command), beforeReplay);
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);

        yield* sql`
          UPDATE projection_threads
          SET
            thread_id = ${command.threadId},
            project_id = ${command.projectId},
            title = ${command.title},
            model_selection_json = (
              SELECT model_selection_json
              FROM orchestration_agent_control_thread_materialization_intents
              WHERE command_id = ${command.commandId}
            ),
            runtime_mode = ${command.runtimeMode},
            interaction_mode = ${command.interactionMode},
            branch = ${command.branch},
            worktree_path = ${command.worktreePath},
            agent_control_json = (
              SELECT binding_json
              FROM orchestration_agent_control_thread_materialization_intents
              WHERE command_id = ${command.commandId}
            ),
            latest_turn_id = NULL,
            created_at = ${command.createdAt},
            updated_at = ${command.createdAt},
            archived_at = NULL,
            latest_user_message_at = NULL,
            pending_approval_count = 0,
            pending_user_input_count = 0,
            has_actionable_proposed_plan = 0,
            deleted_at = NULL
          WHERE thread_id IN (${command.threadId}, 'foreign-thread')
        `;
        assert.strictEqual(
          Exit.isSuccess(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), publicationsBeforeReplay);
      }
      yield* Fiber.interrupt(publicationConsumer);
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

      let expectedPublications = 2;
      const projectionMutations: ReadonlyArray<{
        readonly name: string;
        readonly mutate: (
          command: AgentControlThreadMaterializeCommand,
        ) => Effect.Effect<void, SqlError>;
      }> = [
        {
          name: "title",
          mutate: (command) =>
            sql`
              UPDATE projection_threads
              SET title = 'Changed before commit'
              WHERE thread_id = ${command.threadId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "model",
          mutate: (command) =>
            sql`
              UPDATE projection_threads
              SET model_selection_json = json_set(
                model_selection_json,
                '$.model',
                'changed-before-commit'
              )
              WHERE thread_id = ${command.threadId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "runtime",
          mutate: (command) =>
            sql`
              UPDATE projection_threads
              SET runtime_mode = 'full-access'
              WHERE thread_id = ${command.threadId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "binding",
          mutate: (command) =>
            sql`
              UPDATE projection_threads
              SET agent_control_json = json_set(
                agent_control_json,
                '$.controlState',
                'taken-over'
              )
              WHERE thread_id = ${command.threadId}
            `.pipe(Effect.asVoid),
        },
        {
          name: "delete",
          mutate: (command) =>
            sql`
              DELETE FROM projection_threads
              WHERE thread_id = ${command.threadId}
            `.pipe(Effect.asVoid),
        },
      ];
      for (const projectionMutation of projectionMutations) {
        const command = yield* makeCommand(
          projectId,
          `materialization-before-complete-${projectionMutation.name}`,
        );
        yield* Ref.set(fault, {
          commandId: command.commandId,
          phase: "beforeTransactionComplete",
          effect: () => projectionMutation.mutate(command).pipe(Effect.orDie),
        });
        assert.strictEqual(
          Exit.isFailure(yield* Effect.exit(engine.dispatchAgentControl(command))),
          true,
        );
        assert.deepStrictEqual(yield* counts(sql, command), {
          events: 0,
          threads: 0,
          acceptedIntents: 0,
          allIntents: 0,
          acceptedReceipts: 0,
          allReceipts: 0,
        });
        assert.deepStrictEqual(
          yield* sql<{ readonly markers: number }>`
            SELECT count(*) AS markers
            FROM orchestration_agent_control_thread_materialization_receipts
            WHERE command_id = ${command.commandId}
          `,
          [{ markers: 0 }],
        );
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);

        yield* Ref.set(fault, null);
        yield* engine.dispatchAgentControl(command);
        expectedPublications += 2;
        yield* Effect.yieldNow;
        assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);
      }

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
      assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);
      yield* Ref.set(fault, null);
      yield* engine.dispatchAgentControl(commitCommand);
      expectedPublications += 2;
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);

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
      assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);
      yield* Ref.set(fault, null);
      yield* Deferred.succeed(release, undefined);
      yield* engine.dispatchAgentControl(interruptCommand);
      expectedPublications += 2;
      yield* Effect.yieldNow;
      assert.strictEqual(yield* Ref.get(publicationCount), expectedPublications);
      yield* Fiber.interrupt(publicationConsumer);
    }),
  );
});
