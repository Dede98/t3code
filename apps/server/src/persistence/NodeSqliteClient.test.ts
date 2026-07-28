import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
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
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as SqliteClient from "./NodeSqliteClient.ts";
import { NodeSqliteTransactionHooks } from "./Services/NodeSqliteTransactionHooks.ts";

const layer = it.layer(SqliteClient.layerMemory());

const initializeMaterializationBoundaryTables = Effect.fn(
  "initializeMaterializationBoundaryTables",
)(function* (sql: SqlClient.SqlClient) {
  yield* sql`
    CREATE TABLE IF NOT EXISTS orchestration_agent_control_thread_materialization_receipts(
      id TEXT PRIMARY KEY
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_controlled_thread_materialization_accepted(
      id TEXT PRIMARY KEY
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS boundary_business_writes(
      id TEXT PRIMARY KEY
    )
  `;
  yield* sql`DELETE FROM orchestration_agent_control_thread_materialization_receipts`;
  yield* sql`DELETE FROM agent_control_controlled_thread_materialization_accepted`;
  yield* sql`DELETE FROM boundary_business_writes`;
});

const insertOrchestrationMarker = (sql: SqlClient.SqlClient, id: string) =>
  sql`
    INSERT INTO orchestration_agent_control_thread_materialization_receipts(id)
    VALUES (${id})
  `;

const insertCoordinatorMarker = (sql: SqlClient.SqlClient, id: string) =>
  sql`
    INSERT INTO agent_control_controlled_thread_materialization_accepted(id)
    VALUES (${id})
  `;

const countRows = Effect.fn("countMaterializationBoundaryRows")(function* (
  sql: SqlClient.SqlClient,
  table:
    | "orchestration_agent_control_thread_materialization_receipts"
    | "agent_control_controlled_thread_materialization_accepted"
    | "boundary_business_writes",
  id?: string,
) {
  const rows =
    id === undefined
      ? yield* sql.unsafe<{ readonly count: number }>(`SELECT count(*) AS count FROM ${table}`)
      : yield* sql.unsafe<{ readonly count: number }>(
          `SELECT count(*) AS count FROM ${table} WHERE id = ?`,
          [id],
        );
  return rows[0]!.count;
});

const makeWalClients = Effect.fn("makeNodeSqliteBoundaryWalClients")(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temp = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3-node-sqlite-savepoint-",
  });
  const databasePath = path.join(temp, "state.sqlite");
  const scopeA = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
  const scopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
  const contextA = yield* Layer.buildWithScope(
    SqliteClient.layer({ filename: databasePath }),
    scopeA,
  );
  const contextB = yield* Layer.buildWithScope(
    SqliteClient.layer({ filename: databasePath }),
    scopeB,
  );
  const sqlA = Context.get(contextA, SqlClient.SqlClient);
  const sqlB = Context.get(contextB, SqlClient.SqlClient);
  for (const sql of [sqlA, sqlB]) {
    const journal = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA busy_timeout = 5000`;
    assert.equal(journal[0]?.journal_mode, "wal");
  }
  yield* initializeMaterializationBoundaryTables(sqlA);
  return { sqlA, sqlB };
});

layer("NodeSqliteClient", (it) => {
  it.effect("runs prepared queries and returns positional values", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* sql`CREATE TABLE entries(id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
      yield* sql`INSERT INTO entries(name) VALUES (${"alpha"}), (${"beta"})`;

      const rows = yield* sql<{ readonly id: number; readonly name: string }>`
      SELECT id, name FROM entries ORDER BY id
    `;
      assert.equal(rows.length, 2);
      assert.equal(rows[0]?.name, "alpha");
      assert.equal(rows[1]?.name, "beta");

      const values = yield* sql`SELECT id, name FROM entries ORDER BY id`.values;
      assert.equal(values.length, 2);
      assert.equal(values[0]?.[1], "alpha");
      assert.equal(values[1]?.[1], "beta");
    }),
  );

  it.effect("returns a typed failure when an unprepared statement cannot be prepared", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const error = yield* Effect.flip(sql.unsafe("SELECT FROM").unprepared);

      assert.equal(error._tag, "SqlError");
      assert.equal(error.reason.operation, "prepare");
    }),
  );

  it.effect("restores the boundary after the real nested withTransaction rollback path", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('outer-before')`;
            const innerExit = yield* Effect.exit(
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* insertCoordinatorMarker(sql, "rolled-back-inner");
                  return yield* Effect.fail("rollback inner savepoint");
                }),
              ),
            );
            assert.equal(innerExit._tag, "Failure");
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('outer-after')`;
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));

      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "rolled-back-inner",
        ),
        0,
      );
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(yield* countRows(sql, "boundary_business_writes"), 2);

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('next-normal')`,
      );
      assert.equal(yield* countRows(sql, "boundary_business_writes"), 3);

      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "next-coordinator"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "next-coordinator",
        ),
        1,
      );
    }),
  );

  it.effect("preserves released markers and tracks rollback-to rewrites", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("sAvEpOiNt effect_sql_1");
            yield* insertCoordinatorMarker(sql, "released");
            yield* sql.unsafe("ReLeAsE SaVePoInT EFFECT_SQL_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "released",
        ),
        1,
      );

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertCoordinatorMarker(sql, "rolled-back-before-rewrite");
            yield* sql.unsafe("rOlLbAcK tO EFFECT_SQL_1");
            yield* insertCoordinatorMarker(sql, "rewritten-after-rollback");
            yield* sql.unsafe("RELEASE effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 2);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "rolled-back-before-rewrite",
        ),
        0,
      );
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "rewritten-after-rollback",
        ),
        1,
      );

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql.unsafe("SAVEPOINT effect_sql_1");
          yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('savepoint-without-marker')`;
          yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('after-markerless-savepoint')`;
        }),
      );
      assert.equal(yield* Ref.get(hookCalls), 2);
      assert.equal(yield* countRows(sql, "boundary_business_writes"), 2);
    }),
  );

  it.effect("tracks nested rollback and release against the addressed savepoint", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* sql.unsafe("SAVEPOINT effect_sql_2");
            yield* insertCoordinatorMarker(sql, "innermost-rolled-back");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_2");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "innermost-rolled-back",
        ),
        0,
      );

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertCoordinatorMarker(sql, "middle-survives");
            yield* sql.unsafe("SAVEPOINT effect_sql_2");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_2");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "middle-survives",
        ),
        1,
      );

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* sql.unsafe("SAVEPOINT effect_sql_2");
            yield* insertCoordinatorMarker(sql, "released-then-outer-rollback");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_2");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "released-then-outer-rollback",
        ),
        0,
      );
    }),
  );

  it.effect("restores orchestration and coordinator parent boundaries precisely", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertOrchestrationMarker(sql, "orchestration-rolled-back");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('after-orchestration')`;
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 0);

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertOrchestrationMarker(sql, "full-flow-rolled-back");
            yield* insertCoordinatorMarker(sql, "full-flow-rolled-back");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 0);

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertOrchestrationMarker(sql, "full-flow-released");
            yield* insertCoordinatorMarker(sql, "full-flow-released");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* insertCoordinatorMarker(sql, "parent-coordinator");
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 2);

      const parentOrchestration = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertOrchestrationMarker(sql, "parent-orchestration");
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertCoordinatorMarker(sql, "inner-coordinator-rolled-back");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
            yield* sql`
              INSERT INTO boundary_business_writes(id)
              VALUES ('must-not-follow-restored-orchestration')
            `;
          }),
        ),
      );
      assert.equal(parentOrchestration._tag, "Failure");
      if (Exit.isFailure(parentOrchestration)) {
        assert.include(
          Cause.pretty(parentOrchestration.cause),
          "materialization marker must be the final transaction statement",
        );
      }

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe("SAVEPOINT effect_sql_1");
            yield* insertCoordinatorMarker(sql, "coordinator-rolled-back-before-valid-flow");
            yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_1");
            yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
            yield* insertOrchestrationMarker(sql, "valid-outer-flow");
            yield* insertCoordinatorMarker(sql, "valid-outer-flow");
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 3);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "coordinator-rolled-back-before-valid-flow",
        ),
        0,
      );
    }),
  );

  it.effect("fails closed across savepoint, marker, rollback-to, and release failures", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      const malformedSavepoint = yield* Effect.exit(sql.unsafe("SAVEPOINT").unprepared);
      assert.equal(malformedSavepoint._tag, "Failure");

      const unsupportedSavepoint = yield* Effect.exit(
        sql.withTransaction(sql.unsafe('SAVEPOINT "effect_sql_1"')),
      );
      assert.equal(unsupportedSavepoint._tag, "Failure");
      if (Exit.isFailure(unsupportedSavepoint)) {
        assert.include(
          Cause.pretty(unsupportedSavepoint.cause),
          "unsupported transaction-control statement",
        );
      }

      yield* insertCoordinatorMarker(sql, "duplicate-marker");
      const markerFailure = yield* Effect.exit(
        sql
          .withTransaction(insertCoordinatorMarker(sql, "duplicate-marker"))
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks)),
      );
      assert.equal(markerFailure._tag, "Failure");
      assert.equal(yield* Ref.get(hookCalls), 0);

      const rollbackToFailure = yield* Effect.exit(
        sql.withTransaction(sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_999")),
      );
      assert.equal(rollbackToFailure._tag, "Failure");
      if (Exit.isFailure(rollbackToFailure)) {
        assert.include(Cause.pretty(rollbackToFailure.cause), "no such savepoint");
      }

      const releaseFailure = yield* Effect.exit(
        sql.withTransaction(sql.unsafe("RELEASE SAVEPOINT effect_sql_999")),
      );
      assert.equal(releaseFailure._tag, "Failure");
      if (Exit.isFailure(releaseFailure)) {
        assert.include(Cause.pretty(releaseFailure.cause), "no such savepoint");
      }

      const rollbackFailure = yield* Effect.exit(sql.unsafe("ROLLBACK"));
      assert.equal(rollbackFailure._tag, "Failure");

      yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('usable-after-failures')`;
      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "coordinator-after-failures"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(yield* countRows(sql, "boundary_business_writes"), 1);
    }),
  );

  it.effect("rolls back a failed outer COMMIT without leaking boundary state", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`CREATE TABLE IF NOT EXISTS boundary_parents(id TEXT PRIMARY KEY)`;
      yield* sql`
        CREATE TABLE IF NOT EXISTS boundary_children(
          id TEXT PRIMARY KEY,
          parent_id TEXT NOT NULL,
          FOREIGN KEY(parent_id) REFERENCES boundary_parents(id)
            DEFERRABLE INITIALLY DEFERRED
        )
      `;
      yield* sql`DELETE FROM boundary_children`;
      yield* sql`DELETE FROM boundary_parents`;
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      const commitFailure = yield* Effect.exit(
        sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql`
                INSERT INTO boundary_children(id, parent_id)
                VALUES ('deferred-child', 'missing-parent')
              `;
              yield* insertCoordinatorMarker(sql, "failed-commit-marker");
            }),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks)),
      );
      assert.equal(commitFailure._tag, "Failure");
      if (Exit.isFailure(commitFailure)) {
        assert.include(Cause.pretty(commitFailure.cause), "FOREIGN KEY constraint failed");
      }
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "failed-commit-marker",
        ),
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM boundary_children
        `)[0]!.count,
        0,
      );

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('after-failed-commit')`,
      );
      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "after-failed-commit-coordinator"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
    }),
  );

  it.effect("resets state before propagating post-COMMIT defects", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const defect = yield* Effect.exit(
        sql.withTransaction(insertCoordinatorMarker(sql, "post-commit-defect")).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: () =>
              Ref.update(hookCalls, (count) => count + 1).pipe(
                Effect.andThen(Effect.die(new Error("post-commit-defect"))),
              ),
          }),
        ),
      );
      assert.equal(defect._tag, "Failure");
      if (Exit.isFailure(defect)) {
        assert.include(Cause.pretty(defect.cause), "post-commit-defect");
      }
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "post-commit-defect",
        ),
        1,
      );

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('after-post-commit-defect')`,
      );
      assert.deepStrictEqual(yield* sql`SELECT 1 AS usable`, [{ usable: 1 }]);
    }),
  );

  it.effect(
    "releases the semaphore after an interrupted post-COMMIT hook",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* initializeMaterializationBoundaryTables(sql);
        const hookCalls = yield* Ref.make(0);
        const enteredHook = yield* Deferred.make<void>();
        const releaseHook = yield* Deferred.make<void>();
        const transaction = yield* sql
          .withTransaction(insertCoordinatorMarker(sql, "post-commit-interrupt"))
          .pipe(
            Effect.provideService(NodeSqliteTransactionHooks, {
              afterCommitBeforeReturn: () =>
                Ref.update(hookCalls, (count) => count + 1).pipe(
                  Effect.andThen(Deferred.succeed(enteredHook, undefined)),
                  Effect.andThen(Deferred.await(releaseHook)),
                ),
            }),
            Effect.forkChild({ startImmediately: true }),
          );
        yield* Deferred.await(enteredHook);
        const interruption = yield* Fiber.interrupt(transaction).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseHook, undefined);
        const transactionExit = yield* Fiber.await(transaction);
        yield* Fiber.join(interruption);
        assert.equal(Exit.hasInterrupts(transactionExit), true);
        assert.equal(yield* Ref.get(hookCalls), 1);
        assert.equal(
          yield* countRows(
            sql,
            "agent_control_controlled_thread_materialization_accepted",
            "post-commit-interrupt",
          ),
          1,
        );

        yield* sql.withTransaction(
          sql`INSERT INTO boundary_business_writes(id) VALUES ('after-post-commit-interrupt')`,
        );
        assert.deepStrictEqual(yield* sql`SELECT 1 AS usable`, [{ usable: 1 }]);
      }),
    10_000,
  );
});

it.effect(
  "isolates nested boundary stacks and hook contexts across real WAL clients",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { sqlA, sqlB } = yield* makeWalClients();
        const hookCallsA = yield* Ref.make(0);
        const hookCallsB = yield* Ref.make(0);

        yield* Effect.all(
          [
            sqlA
              .withTransaction(
                Effect.gen(function* () {
                  const nested = yield* Effect.exit(
                    sqlA.withTransaction(
                      insertCoordinatorMarker(sqlA, "wal-a-rolled-back").pipe(
                        Effect.andThen(Effect.fail("rollback A")),
                      ),
                    ),
                  );
                  assert.equal(nested._tag, "Failure");
                  yield* sqlA`
                    INSERT INTO boundary_business_writes(id)
                    VALUES ('wal-a-business')
                  `;
                }),
              )
              .pipe(
                Effect.provideService(NodeSqliteTransactionHooks, {
                  afterCommitBeforeReturn: () => Ref.update(hookCallsA, (count) => count + 1),
                }),
              ),
            sqlB
              .withTransaction(
                sqlB.withTransaction(insertCoordinatorMarker(sqlB, "wal-b-survives")),
              )
              .pipe(
                Effect.provideService(NodeSqliteTransactionHooks, {
                  afterCommitBeforeReturn: () => Ref.update(hookCallsB, (count) => count + 1),
                }),
              ),
          ],
          { concurrency: "unbounded" },
        );

        assert.equal(yield* Ref.get(hookCallsA), 0);
        assert.equal(yield* Ref.get(hookCallsB), 1);
        assert.equal(
          yield* countRows(
            sqlA,
            "agent_control_controlled_thread_materialization_accepted",
            "wal-a-rolled-back",
          ),
          0,
        );
        assert.equal(
          yield* countRows(
            sqlA,
            "agent_control_controlled_thread_materialization_accepted",
            "wal-b-survives",
          ),
          1,
        );
        assert.equal(yield* countRows(sqlB, "boundary_business_writes", "wal-a-business"), 1);
        assert.deepStrictEqual(yield* sqlA`SELECT 1 AS usable`, [{ usable: 1 }]);
        assert.deepStrictEqual(yield* sqlB`SELECT 1 AS usable`, [{ usable: 1 }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  20_000,
);

it.effect("returns a typed failure when the database cannot be opened", () =>
  Effect.gen(function* () {
    const error = yield* Effect.flip(
      Layer.build(SqliteClient.layer({ filename: "\0" })).pipe(Effect.scoped),
    );

    assert.equal(error._tag, "SqlError");
    assert.equal(error.reason.operation, "open");
  }),
);
