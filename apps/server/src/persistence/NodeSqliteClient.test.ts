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
import type { SqlError } from "effect/unstable/sql/SqlError";

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
    CREATE TABLE IF NOT EXISTS agent_control_controlled_thread_prepare_finalizations(
      id TEXT PRIMARY KEY
    )
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_controlled_thread_prepare_final_commit_markers(
      id TEXT PRIMARY KEY
    )
  `;
  for (const table of [
    "agent_control_initial_planning_handoff_intents",
    "agent_control_initial_planning_handoff_receipts",
    "agent_control_initial_planning_handoff_accepted",
    "agent_control_initial_planning_deliveries",
    "agent_control_initial_planning_stage_started",
    "agent_control_initial_planning_result_evidence",
    "agent_control_initial_planning_finalization_receipts",
    "agent_control_initial_planning_finalization_markers",
    "agent_control_implementation_admission_evidence",
    "agent_control_implementation_admission_receipts",
    "agent_control_implementation_admission_markers",
    "agent_control_implementation_materialization_evidence",
    "agent_control_implementation_materialization_receipts",
    "agent_control_implementation_handoff_intents",
    "agent_control_implementation_handoff_receipts",
    "agent_control_implementation_handoff_accepted",
    "agent_control_implementation_deliveries",
    "agent_control_implementation_materialization_markers",
    "agent_control_implementation_turn_accepted",
    "agent_control_implementation_session_evidence",
    "agent_control_implementation_delivery_attestations",
    "agent_control_implementation_stage_started_evidence",
    "agent_control_implementation_stage_started_receipts",
    "agent_control_implementation_stage_started_markers",
    "agent_control_implementation_result_evidence",
    "agent_control_implementation_stage_finalization_receipts",
    "agent_control_implementation_stage_finalization_markers",
    "agent_control_verification_admission_evidence",
    "agent_control_verification_admission_receipts",
    "agent_control_verification_admission_markers",
    "agent_control_verification_materialization_evidence",
    "agent_control_verification_materialization_receipts",
    "agent_control_verification_handoff_intents",
    "agent_control_verification_handoff_receipts",
    "agent_control_verification_handoff_accepted",
    "agent_control_verification_deliveries",
    "agent_control_verification_materialization_markers",
    "agent_control_verification_turn_accepted",
    "agent_control_verification_session_evidence",
    "agent_control_verification_delivery_attestations",
    "agent_control_verification_stage_started_evidence",
    "agent_control_verification_stage_started_receipts",
    "agent_control_verification_stage_started_markers",
    "agent_control_verification_evaluation_evidence",
    "agent_control_verification_evaluation_receipts",
    "agent_control_verification_evaluation_markers",
  ] as const) {
    yield* sql.unsafe(`CREATE TABLE IF NOT EXISTS ${table}(id TEXT PRIMARY KEY)`).unprepared;
  }
  yield* sql`
    CREATE TABLE IF NOT EXISTS boundary_business_writes(
      id TEXT PRIMARY KEY
    )
  `;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`DELETE FROM orchestration_agent_control_thread_materialization_receipts`;
      yield* sql`DELETE FROM agent_control_controlled_thread_materialization_accepted`;
      yield* sql`DELETE FROM agent_control_controlled_thread_prepare_final_commit_markers`;
      yield* sql`DELETE FROM agent_control_controlled_thread_prepare_finalizations`;
      yield* sql`DELETE FROM agent_control_implementation_admission_markers`;
      yield* sql`DELETE FROM agent_control_implementation_admission_receipts`;
      yield* sql`DELETE FROM agent_control_implementation_admission_evidence`;
      yield* sql`DELETE FROM agent_control_verification_admission_markers`;
      yield* sql`DELETE FROM agent_control_verification_admission_receipts`;
      yield* sql`DELETE FROM agent_control_verification_admission_evidence`;
    }),
  );
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
    | "agent_control_controlled_thread_prepare_final_commit_markers"
    | "agent_control_controlled_thread_prepare_finalizations"
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

type SqlExecutionMode = "statement" | "values" | "raw" | "unprepared";

const executeSqlMode = (
  sql: SqlClient.SqlClient,
  text: string,
  mode: SqlExecutionMode,
  params: ReadonlyArray<unknown> = [],
): Effect.Effect<void, SqlError> => {
  const statement = sql.unsafe(text, params);
  switch (mode) {
    case "statement":
      return Effect.asVoid(statement);
    case "values":
      return Effect.asVoid(statement.values);
    case "raw":
      return Effect.asVoid(statement.raw);
    case "unprepared":
      return Effect.asVoid(statement.unprepared);
  }
};

const initialPlanningFinalizationTables = [
  "agent_control_initial_planning_stage_started",
  "agent_control_initial_planning_result_evidence",
  "agent_control_initial_planning_finalization_receipts",
  "agent_control_initial_planning_finalization_markers",
] as const;
const initialPlanningDmlForms = [
  (table: string) => `INSERT INTO ${table}(id) VALUES (?)`,
  (table: string) => `INSERT OR IGNORE INTO ${table}(id) VALUES (?)`,
  (table: string) => `REPLACE INTO ${table}(id) VALUES (?)`,
  (table: string) => `INSERT INTO "${table.toUpperCase()}"(id) VALUES (?)`,
  (table: string) => `INSERT INTO main.${table}(id) VALUES (?)`,
  (table: string) => `WITH source(id) AS (SELECT ?) INSERT INTO ${table}(id) SELECT id FROM source`,
  (table: string) =>
    `WITH RECURSIVE source(id) AS (SELECT ?) INSERT INTO ${table}(id) SELECT id FROM source`,
  (table: string) => `INSERT INTO ${table}(id) SELECT ? WHERE 0`,
  (table: string) => `INSERT INTO ${table}(id) VALUES (?) ON CONFLICT(id) DO NOTHING`,
] as const;
const implementationAdmissionTables = [
  "agent_control_implementation_admission_evidence",
  "agent_control_implementation_admission_receipts",
  "agent_control_implementation_admission_markers",
] as const;
const verificationAdmissionTables = [
  "agent_control_verification_admission_evidence",
  "agent_control_verification_admission_receipts",
  "agent_control_verification_admission_markers",
] as const;
const implementationMaterializationTables = [
  "agent_control_implementation_materialization_evidence",
  "agent_control_implementation_materialization_receipts",
  "agent_control_implementation_handoff_intents",
  "agent_control_implementation_handoff_receipts",
  "agent_control_implementation_handoff_accepted",
  "agent_control_implementation_deliveries",
  "agent_control_implementation_materialization_markers",
] as const;
const implementationStageStartTables = [
  "agent_control_implementation_stage_started_evidence",
  "agent_control_implementation_stage_started_receipts",
  "agent_control_implementation_stage_started_markers",
] as const;
const implementationStageFinalizationTables = [
  "agent_control_implementation_result_evidence",
  "agent_control_implementation_stage_finalization_receipts",
  "agent_control_implementation_stage_finalization_markers",
] as const;
const implementationTransactionalEvidenceTables = [
  "agent_control_implementation_session_evidence",
  "agent_control_implementation_delivery_attestations",
] as const;
const verificationMaterializationTables = [
  "agent_control_verification_materialization_evidence",
  "agent_control_verification_materialization_receipts",
  "agent_control_verification_handoff_intents",
  "agent_control_verification_handoff_receipts",
  "agent_control_verification_handoff_accepted",
  "agent_control_verification_deliveries",
  "agent_control_verification_materialization_markers",
] as const;
const verificationTransactionalEvidenceTables = [
  "agent_control_verification_session_evidence",
  "agent_control_verification_delivery_attestations",
] as const;
const verificationStageStartTables = [
  "agent_control_verification_stage_started_evidence",
  "agent_control_verification_stage_started_receipts",
  "agent_control_verification_stage_started_markers",
] as const;

const insertImplementationAdmissionChain = (
  sql: SqlClient.SqlClient,
  mode: SqlExecutionMode,
  id: string,
) =>
  Effect.forEach(
    implementationAdmissionTables,
    (table) => executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [id]),
    { discard: true },
  );

const insertCompanionChain = (
  sql: SqlClient.SqlClient,
  mode: SqlExecutionMode,
  tables: ReadonlyArray<string>,
  id: string,
) =>
  Effect.forEach(
    tables,
    (table) => executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [id]),
    { discard: true },
  );

interface MarkerInsertForm {
  readonly label: string;
  readonly makeSql: (table: string) => string;
}

const markerInsertForms: ReadonlyArray<MarkerInsertForm> = [
  {
    label: "insert",
    makeSql: (table) => `INSERT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "uppercase-unquoted",
    makeSql: (table) => `INSERT INTO ${table.toUpperCase()}(id) VALUES (?)`,
  },
  {
    label: "or-abort",
    makeSql: (table) => `INSERT OR ABORT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "or-fail",
    makeSql: (table) => `INSERT OR FAIL INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "or-ignore-writing",
    makeSql: (table) => `INSERT OR IGNORE INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "or-replace",
    makeSql: (table) => `INSERT OR REPLACE INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "or-rollback",
    makeSql: (table) => `INSERT OR ROLLBACK INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "replace",
    makeSql: (table) => `REPLACE INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "double-quoted",
    makeSql: (table) => `INSERT INTO "${table.toUpperCase()}"(id) VALUES (?)`,
  },
  {
    label: "backtick-quoted",
    makeSql: (table) => `INSERT INTO \`${table.toUpperCase()}\`(id) VALUES (?)`,
  },
  {
    label: "bracket-quoted",
    makeSql: (table) => `INSERT INTO [${table.toUpperCase()}](id) VALUES (?)`,
  },
  {
    label: "main-qualified",
    makeSql: (table) => `INSERT INTO main.${table}(id) VALUES (?)`,
  },
  {
    label: "double-quoted-main",
    makeSql: (table) => `INSERT INTO "MAIN"."${table.toUpperCase()}"(id) VALUES (?)`,
  },
  {
    label: "backtick-quoted-main",
    makeSql: (table) => `INSERT INTO \`MAIN\`.\`${table.toUpperCase()}\`(id) VALUES (?)`,
  },
  {
    label: "bracket-quoted-main",
    makeSql: (table) => `INSERT INTO [MAIN].[${table.toUpperCase()}](id) VALUES (?)`,
  },
  {
    label: "with",
    makeSql: (table) => `WITH cte AS (SELECT 1) INSERT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "with-multiple",
    makeSql: (table) =>
      `WITH first(value) AS (SELECT 1),
            second(value) AS NOT MATERIALIZED (SELECT value + 1 FROM first)
       INSERT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "with-recursive",
    makeSql: (table) =>
      `WITH RECURSIVE counter(value) AS (
         SELECT 1
         UNION ALL
         SELECT value + 1 FROM counter WHERE value < 1
       )
       INSERT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "with-nested-commented",
    makeSql: (table) =>
      `WITH nested(value) AS MATERIALIZED (
         SELECT coalesce((1 + (2 * (3))), 0)
         /* comma, close-paren ), and ${table} stay opaque */
       )
       INSERT INTO ${table}(id) VALUES (?)`,
  },
  {
    label: "upsert-writing",
    makeSql: (table) => `INSERT INTO ${table}(id) VALUES (?) ON CONFLICT(id) DO NOTHING`,
  },
  {
    label: "returning",
    makeSql: (table) => `INSERT INTO ${table}(id) VALUES (?) RETURNING id`,
  },
];

const markerTables = {
  orchestration: "orchestration_agent_control_thread_materialization_receipts",
  coordinator: "agent_control_controlled_thread_materialization_accepted",
} as const;

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
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`PRAGMA busy_timeout = 5000`;
    assert.equal(journal[0]?.journal_mode, "wal");
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
  }
  yield* initializeMaterializationBoundaryTables(sqlA);
  return { sqlA, sqlB };
});

const makeScopedMemoryClient = Effect.fn("makeScopedNodeSqliteMemoryClient")(function* (
  config: SqliteClient.SqliteMemoryClientConfig = {},
) {
  const context = yield* Layer.build(SqliteClient.layerMemory(config));
  return Context.get(context, SqlClient.SqlClient);
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

  it.effect("registers the fatal UTF-8 roundtrip function on every client", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql<{
          readonly ascii: number;
          readonly multilingual: number;
          readonly replacement: number;
          readonly invalidPair: number;
          readonly truncated2: number;
          readonly truncated3: number;
          readonly truncated4: number;
          readonly loneContinuation: number;
          readonly surrogate: number;
          readonly overlong: number;
          readonly invalidLead: number;
          readonly textValue: number;
          readonly integerValue: number;
          readonly nullValue: number;
        }>`
          SELECT t3_fatal_utf8(CAST('orchestration' AS BLOB)) AS ascii,
            t3_fatal_utf8(CAST(${`Grüße 世界 🌍`} AS BLOB)) AS multilingual,
            t3_fatal_utf8(CAST(${`�`} AS BLOB)) AS replacement,
            t3_fatal_utf8(CAST(X'C328' AS BLOB)) AS "invalidPair",
            t3_fatal_utf8(CAST(X'C3' AS BLOB)) AS "truncated2",
            t3_fatal_utf8(CAST(X'E282' AS BLOB)) AS "truncated3",
            t3_fatal_utf8(CAST(X'F09F92' AS BLOB)) AS "truncated4",
            t3_fatal_utf8(CAST(X'80' AS BLOB)) AS "loneContinuation",
            t3_fatal_utf8(CAST(X'EDA080' AS BLOB)) AS surrogate,
            t3_fatal_utf8(CAST(X'C0AF' AS BLOB)) AS overlong,
            t3_fatal_utf8(CAST(X'FF' AS BLOB)) AS "invalidLead",
            t3_fatal_utf8('orchestration') AS "textValue",
            t3_fatal_utf8(1) AS "integerValue",
            t3_fatal_utf8(NULL) AS "nullValue"
        `,
        [
          {
            ascii: 1,
            multilingual: 1,
            replacement: 1,
            invalidPair: 0,
            truncated2: 0,
            truncated3: 0,
            truncated4: 0,
            loneContinuation: 0,
            surrogate: 0,
            overlong: 0,
            invalidLead: 0,
            textValue: 0,
            integerValue: 0,
            nullValue: 0,
          },
        ],
      );
    }),
  );

  it.effect("registers the closed orchestration JSON storage classifier on every client", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const validPayload = '{"projectId":"udf-project","deletedAt":"2026-08-28T10:00:00.000Z"}';
      assert.deepStrictEqual(
        yield* sql<{
          readonly valid: number;
          readonly duplicate: number;
          readonly extra: number;
          readonly malformed: number;
          readonly nonBlob: number;
          readonly unknownEvent: number;
        }>`
          SELECT t3_orchestration_event_json_storage(
              CAST('project.deleted' AS BLOB), CAST(${validPayload} AS BLOB), CAST('{}' AS BLOB)
            ) AS valid,
            t3_orchestration_event_json_storage(
              CAST('project.deleted' AS BLOB),
              CAST('{"projectId":"udf-project","projectId":"attacker","deletedAt":"2026-08-28T10:00:00.000Z"}' AS BLOB),
              CAST('{}' AS BLOB)
            ) AS duplicate,
            t3_orchestration_event_json_storage(
              CAST('project.deleted' AS BLOB),
              CAST('{"projectId":"udf-project","deletedAt":"2026-08-28T10:00:00.000Z","extra":true}' AS BLOB),
              CAST('{}' AS BLOB)
            ) AS extra,
            t3_orchestration_event_json_storage(
              CAST('project.deleted' AS BLOB), CAST('{"projectId":' AS BLOB), CAST('{}' AS BLOB)
            ) AS malformed,
            t3_orchestration_event_json_storage('project.deleted', ${validPayload}, '{}')
              AS "nonBlob",
            t3_orchestration_event_json_storage(
              CAST('project.unknown' AS BLOB), CAST(${validPayload} AS BLOB), CAST('{}' AS BLOB)
            ) AS "unknownEvent"
        `,
        [{ valid: 1, duplicate: 0, extra: 0, malformed: 0, nonBlob: 0, unknownEvent: 0 }],
      );
    }),
  );

  it.effect("closes a connection when UDF registration fails and allows a clean retry", () =>
    Effect.gen(function* () {
      const failed = yield* Effect.exit(
        Effect.scoped(
          Layer.build(
            SqliteClient.layerMemory({
              _testHooks: {
                registerFunctions: () => {
                  throw new Error("injected registration failure");
                },
              },
            }),
          ),
        ),
      );
      assert.isTrue(Exit.isFailure(failed));

      yield* Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(SqliteClient.layerMemory());
          const retried = Context.get(context, SqlClient.SqlClient);
          assert.deepStrictEqual(
            yield* retried`
              SELECT t3_fatal_utf8(CAST('retry' AS BLOB)) AS valid,
                t3_orchestration_event_json_storage(
                  CAST('project.deleted' AS BLOB),
                  CAST('{"projectId":"retry","deletedAt":"2026-08-28T10:00:00.000Z"}' AS BLOB),
                  CAST('{}' AS BLOB)
                ) AS storage
            `,
            [{ valid: 1, storage: 1 }],
          );
        }),
      );
    }),
  );

  it.effect(
    "allows only the persisted Initial Planning chain between orchestration and final coordinator markers",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
            const sql = yield* makeScopedMemoryClient();
            yield* initializeMaterializationBoundaryTables(sql);
            const hookCalls = yield* Ref.make(0);
            const hooks = {
              afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
            };
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
              mode,
              [`orchestration-${mode}`],
            );
            for (const table of [
              "agent_control_initial_planning_handoff_intents",
              "agent_control_initial_planning_handoff_receipts",
              "agent_control_initial_planning_handoff_accepted",
              "agent_control_initial_planning_deliveries",
            ] as const) {
              yield* executeSqlMode(sql, `INSERT INTO ${table}(id) VALUES (?)`, mode, [
                `handoff-${mode}`,
              ]);
            }
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`,
              mode,
              [`coordinator-${mode}`],
            );
            yield* executeSqlMode(sql, "COMMIT", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 1, mode);

            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
              mode,
              [`reject-${mode}`],
            );
            const unrelated = yield* Effect.exit(
              executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                `reject-${mode}`,
              ]),
            );
            assert.equal(Exit.isFailure(unrelated), true, mode);
            yield* executeSqlMode(sql, "ROLLBACK", mode);
          }
        }),
      ),
  );

  it.effect(
    "guards every Initial Planning finalization table and advances only on the final marker",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const modes: ReadonlyArray<SqlExecutionMode> = [
            "statement",
            "values",
            "raw",
            "unprepared",
          ];
          for (const mode of modes) {
            for (const table of initialPlanningFinalizationTables) {
              const sql = yield* makeScopedMemoryClient();
              yield* initializeMaterializationBoundaryTables(sql);
              for (const [formIndex, form] of initialPlanningDmlForms.entries()) {
                const exit = yield* Effect.exit(
                  executeSqlMode(sql, form(table), mode, [
                    `autocommit-${formIndex}-${mode}-${table}`,
                  ]),
                );
                assert.equal(Exit.isFailure(exit), true, `${formIndex}:${mode}:${table}`);
                if (Exit.isFailure(exit)) {
                  assert.include(
                    Cause.pretty(exit.cause),
                    "persistent materialization marker DML requires an active caller-controlled transaction",
                    `${formIndex}:${mode}:${table}`,
                  );
                }
                assert.deepStrictEqual(
                  yield* sql.unsafe(`SELECT count(*) AS count FROM main.${table}`),
                  [{ count: 0 }],
                  `${formIndex}:${mode}:${table}`,
                );
              }
            }

            const sql = yield* makeScopedMemoryClient();
            yield* initializeMaterializationBoundaryTables(sql);
            const hookCalls = yield* Ref.make(0);
            const hooks = {
              afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
            };
            yield* executeSqlMode(sql, "BEGIN", mode);
            for (const table of initialPlanningFinalizationTables) {
              yield* executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `committed-${mode}`,
              ]);
            }
            assert.equal(yield* Ref.get(hookCalls), 0, mode);
            const afterMarker = yield* Effect.exit(
              executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                `after-marker-${mode}`,
              ]),
            );
            assert.equal(Exit.isFailure(afterMarker), true, mode);
            yield* executeSqlMode(sql, "ROLLBACK", mode);

            yield* executeSqlMode(sql, "BEGIN", mode);
            for (const table of initialPlanningFinalizationTables) {
              yield* executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `final-${mode}`,
              ]);
            }
            yield* executeSqlMode(sql, "COMMIT", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 1, mode);

            yield* executeSqlMode(sql, `SAVEPOINT initial_planning_${mode}`, mode);
            for (const table of initialPlanningFinalizationTables) {
              yield* executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `savepoint-${mode}`,
              ]);
            }
            yield* executeSqlMode(sql, `RELEASE SAVEPOINT initial_planning_${mode}`, mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 2, mode);

            yield* executeSqlMode(sql, `SAVEPOINT initial_planning_rollback_${mode}`, mode);
            for (const table of initialPlanningFinalizationTables) {
              yield* executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `rolled-back-${mode}`,
              ]);
            }
            yield* executeSqlMode(
              sql,
              `ROLLBACK TO SAVEPOINT initial_planning_rollback_${mode}`,
              mode,
            );
            yield* executeSqlMode(
              sql,
              `RELEASE SAVEPOINT initial_planning_rollback_${mode}`,
              mode,
            ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
            assert.equal(yield* Ref.get(hookCalls), 2, mode);
          }
        }),
      ),
  );

  it.effect(
    "guards the complete Implementation Admission companion chain in every execution mode",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
            const sql = yield* makeScopedMemoryClient();
            yield* initializeMaterializationBoundaryTables(sql);

            for (const table of implementationAdmissionTables) {
              for (const [formIndex, form] of initialPlanningDmlForms.entries()) {
                const rejected = yield* Effect.exit(
                  executeSqlMode(sql, form(table), mode, [
                    `autocommit-${mode}-${formIndex}-${table}`,
                  ]),
                );
                assert.isTrue(Exit.isFailure(rejected), `${mode}:${formIndex}:${table}`);
                if (Exit.isFailure(rejected)) {
                  assert.include(
                    Cause.pretty(rejected.cause),
                    "persistent materialization marker DML requires an active caller-controlled transaction",
                  );
                }
              }
            }

            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${implementationAdmissionTables[0]}(id) VALUES (?)`,
              mode,
              [`missing-marker-${mode}`],
            );
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${implementationAdmissionTables[1]}(id) VALUES (?)`,
              mode,
              [`missing-marker-${mode}`],
            );
            const prematureCommit = yield* Effect.exit(executeSqlMode(sql, "COMMIT", mode));
            assert.isTrue(Exit.isFailure(prematureCommit), mode);
            if (Exit.isFailure(prematureCommit)) {
              assert.include(
                Cause.pretty(prematureCommit.cause),
                "implementation admission companion chain requires a final marker",
              );
            }
            const hookBoundaries = yield* Ref.make<ReadonlyArray<string>>([]);
            const hooks = {
              afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
                Ref.update(hookBoundaries, (boundaries) => [...boundaries, boundary]),
            };
            yield* executeSqlMode(sql, `SAVEPOINT implementation_${mode}`, mode);
            yield* insertImplementationAdmissionChain(sql, mode, `committed-${mode}`);
            assert.deepStrictEqual(yield* Ref.get(hookBoundaries), []);
            const afterMarker = yield* Effect.exit(
              executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                `after-marker-${mode}`,
              ]),
            );
            assert.isTrue(Exit.isFailure(afterMarker), mode);
            yield* executeSqlMode(sql, "ROLLBACK", mode);
            assert.deepStrictEqual(yield* Ref.get(hookBoundaries), []);

            yield* executeSqlMode(sql, `SAVEPOINT implementation_commit_${mode}`, mode);
            yield* insertImplementationAdmissionChain(sql, mode, `committed-${mode}`);
            yield* executeSqlMode(
              sql,
              `RELEASE SAVEPOINT implementation_commit_${mode}`,
              mode,
            ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
            assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
              "agent-control-implementation-admission",
            ]);

            yield* executeSqlMode(sql, `SAVEPOINT implementation_noop_${mode}`, mode);
            for (const table of implementationAdmissionTables) {
              yield* executeSqlMode(
                sql,
                `INSERT OR IGNORE INTO main.${table}(id) VALUES (?)`,
                mode,
                [`committed-${mode}`],
              );
            }
            yield* executeSqlMode(
              sql,
              "INSERT INTO boundary_business_writes(id) VALUES (?)",
              mode,
              [`noop-${mode}`],
            );
            yield* executeSqlMode(sql, `RELEASE SAVEPOINT implementation_noop_${mode}`, mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);

            yield* executeSqlMode(sql, `SAVEPOINT implementation_rollback_${mode}`, mode);
            yield* insertImplementationAdmissionChain(sql, mode, `rolled-back-${mode}`);
            yield* executeSqlMode(
              sql,
              `ROLLBACK TO SAVEPOINT implementation_rollback_${mode}`,
              mode,
            );
            yield* executeSqlMode(
              sql,
              `RELEASE SAVEPOINT implementation_rollback_${mode}`,
              mode,
            ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
            assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);
            for (const table of implementationAdmissionTables) {
              assert.deepStrictEqual(
                yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                  `rolled-back-${mode}`,
                ]),
                [{ count: 0 }],
              );
            }
          }

          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          const hookFailure = new Error("implementation admission post-commit hook failed");
          const exit = yield* Effect.exit(
            sql
              .withTransaction(insertImplementationAdmissionChain(sql, "statement", "hook-failure"))
              .pipe(
                Effect.provideService(NodeSqliteTransactionHooks, {
                  afterCommitBeforeReturn: () => Effect.die(hookFailure),
                }),
              ),
          );
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            assert.include(Cause.pretty(exit.cause), hookFailure.message);
          }
          for (const table of implementationAdmissionTables) {
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                "hook-failure",
              ]),
              [{ count: 1 }],
            );
          }
        }),
      ),
  );

  it.effect(
    "guards the complete Verification Admission companion chain in every execution mode",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
            const sql = yield* makeScopedMemoryClient();
            yield* initializeMaterializationBoundaryTables(sql);

            for (const table of verificationAdmissionTables) {
              for (const [formIndex, form] of initialPlanningDmlForms.entries()) {
                const rejected = yield* Effect.exit(
                  executeSqlMode(sql, form(table), mode, [
                    `verification-autocommit-${mode}-${formIndex}-${table}`,
                  ]),
                );
                assert.isTrue(Exit.isFailure(rejected), `${mode}:${formIndex}:${table}`);
                if (Exit.isFailure(rejected)) {
                  assert.include(
                    Cause.pretty(rejected.cause),
                    "persistent materialization marker DML requires an active caller-controlled transaction",
                  );
                }
              }
            }

            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO main.${verificationAdmissionTables[0]}(id) VALUES (?)`,
              mode,
              [`verification-missing-marker-${mode}`],
            );
            yield* executeSqlMode(
              sql,
              `INSERT INTO main.${verificationAdmissionTables[1]}(id) VALUES (?)`,
              mode,
              [`verification-missing-marker-${mode}`],
            );
            const premature = yield* Effect.exit(executeSqlMode(sql, "COMMIT", mode));
            assert.isTrue(Exit.isFailure(premature), mode);
            if (Exit.isFailure(premature)) {
              assert.include(
                Cause.pretty(premature.cause),
                "verification admission companion chain requires a final marker",
              );
            }

            const hookBoundaries = yield* Ref.make<ReadonlyArray<string>>([]);
            const hooks = {
              afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
                Ref.update(hookBoundaries, (boundaries) => [...boundaries, boundary]),
            };
            yield* executeSqlMode(sql, `SAVEPOINT verification_${mode}`, mode);
            yield* insertCompanionChain(
              sql,
              mode,
              verificationAdmissionTables,
              `verification-committed-${mode}`,
            );
            const afterMarker = yield* Effect.exit(
              executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                `verification-after-marker-${mode}`,
              ]),
            );
            assert.isTrue(Exit.isFailure(afterMarker), mode);
            yield* executeSqlMode(sql, "ROLLBACK", mode);
            assert.deepStrictEqual(yield* Ref.get(hookBoundaries), []);

            yield* executeSqlMode(sql, `SAVEPOINT verification_commit_${mode}`, mode);
            yield* insertCompanionChain(
              sql,
              mode,
              verificationAdmissionTables,
              `verification-committed-${mode}`,
            );
            yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_commit_${mode}`, mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
              "agent-control-verification-admission",
            ]);

            yield* executeSqlMode(sql, `SAVEPOINT verification_noop_${mode}`, mode);
            for (const table of verificationAdmissionTables) {
              yield* executeSqlMode(
                sql,
                `/* verification no-op */
                 INSERT OR IGNORE INTO "MAIN".[${table.toUpperCase()}](id) VALUES (?)`,
                mode,
                [`verification-committed-${mode}`],
              );
            }
            yield* executeSqlMode(
              sql,
              "INSERT INTO boundary_business_writes(id) VALUES (?)",
              mode,
              [`verification-noop-${mode}`],
            );
            yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_noop_${mode}`, mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);

            yield* executeSqlMode(sql, `SAVEPOINT verification_rollback_${mode}`, mode);
            yield* insertCompanionChain(
              sql,
              mode,
              verificationAdmissionTables,
              `verification-rolled-back-${mode}`,
            );
            yield* executeSqlMode(sql, `ROLLBACK TO SAVEPOINT verification_rollback_${mode}`, mode);
            yield* executeSqlMode(
              sql,
              `RELEASE SAVEPOINT verification_rollback_${mode}`,
              mode,
            ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
            assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);
          }

          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          const hookDefect = new Error("verification admission post-commit hook failed");
          const defect = yield* Effect.exit(
            sql
              .withTransaction(
                insertCompanionChain(
                  sql,
                  "statement",
                  verificationAdmissionTables,
                  "verification-hook-defect",
                ),
              )
              .pipe(
                Effect.provideService(NodeSqliteTransactionHooks, {
                  afterCommitBeforeReturn: () => Effect.die(hookDefect),
                }),
              ),
          );
          assert.isTrue(Exit.isFailure(defect));
          if (Exit.isFailure(defect))
            assert.include(Cause.pretty(defect.cause), hookDefect.message);
          for (const table of verificationAdmissionTables) {
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                "verification-hook-defect",
              ]),
              [{ count: 1 }],
            );
          }
        }),
      ),
  );

  it.effect("allows read-only Verification inspection after the final marker", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(sql);
        yield* sql`BEGIN`;
        yield* insertCompanionChain(
          sql,
          "statement",
          verificationAdmissionTables,
          "verification-read-after-marker",
        );
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM agent_control_verification_admission_markers`,
          [{ count: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`
            WITH marker_count AS (
              SELECT count(*) AS count
              FROM agent_control_verification_admission_markers
            )
            SELECT count FROM marker_count
          `,
          [{ count: 1 }],
        );
        const mutation = yield* Effect.exit(
          sql`INSERT INTO boundary_business_writes(id) VALUES ('after-read-only-inspection')`,
        );
        assert.isTrue(Exit.isFailure(mutation));
        if (Exit.isFailure(mutation)) {
          assert.include(
            Cause.pretty(mutation.cause),
            "materialization marker must be the final transaction statement",
          );
        }
        yield* sql`ROLLBACK`;
      }),
    ),
  );

  it.effect("guards the permanent post-Verification-marker SQL matrix in every mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
        const reads = [
          "SELECT 1 AS value",
          "WITH x AS (SELECT 1 AS value) SELECT value FROM x",
          `WITH outer_cte AS (
             WITH inner_cte AS (SELECT 1 AS value)
             SELECT value FROM inner_cte
           )
           SELECT value FROM outer_cte`,
          `WITH RECURSIVE numbers(value) AS (
             VALUES (1)
             UNION ALL
             SELECT value + 1 FROM numbers WHERE value < 3
           )
           SELECT sum(value) FROM numbers`,
          `SELECT "id" FROM "boundary_business_writes" WHERE "id" = 'quoted-read'`,
          "SELECT 'UPDATE (DELETE INSERT RETURNING)' AS harmless_text",
          "/* block comment with UPDATE */ SELECT 1 -- line comment with DELETE\n",
          "\t\n WITH\n spaced AS\n ( SELECT 1 AS value )\n SELECT value FROM spaced",
        ] as const;
        const attacks: ReadonlyArray<{
          readonly label: string;
          readonly sql: string;
          readonly firstStatementOnly?: boolean;
        }> = [
          {
            label: "with-update",
            sql: "WITH x AS (SELECT 1) UPDATE post_marker_attack SET value='mutated' WHERE id='update-target'",
          },
          {
            label: "with-insert",
            sql: "WITH x AS (SELECT 1) INSERT INTO post_marker_attack(id, value) VALUES ('inserted', 'mutated')",
          },
          {
            label: "with-delete",
            sql: "WITH x AS (SELECT 1) DELETE FROM post_marker_attack WHERE id='delete-target'",
          },
          {
            label: "recursive-update",
            sql: `WITH RECURSIVE x(value) AS (
                    VALUES (1) UNION ALL SELECT value + 1 FROM x WHERE value < 2
                  )
                  UPDATE post_marker_attack SET value='mutated' WHERE id='update-target'`,
          },
          {
            label: "block-comment-update",
            sql: "/* harmless-looking prefix */ UPDATE post_marker_attack SET value='mutated' WHERE id='update-target'",
          },
          {
            label: "line-comment-delete",
            sql: "-- harmless-looking prefix\nDELETE FROM post_marker_attack WHERE id='delete-target'",
          },
          {
            label: "explain-update",
            sql: "EXPLAIN UPDATE post_marker_attack SET value='mutated' WHERE id='update-target'",
          },
          { label: "writable-schema", sql: "PRAGMA writable_schema = ON" },
          { label: "user-version", sql: "PRAGMA user_version = 1" },
          {
            label: "attach",
            sql: "ATTACH DATABASE ':memory:' AS post_marker_attached_attack",
          },
          { label: "detach", sql: "DETACH DATABASE attached_guard" },
          { label: "vacuum", sql: "VACUUM" },
          { label: "create", sql: "CREATE TABLE post_marker_created(id TEXT)" },
          { label: "drop", sql: "DROP TABLE post_marker_drop_target" },
          {
            label: "alter",
            sql: "ALTER TABLE post_marker_alter_target ADD COLUMN mutated TEXT",
          },
          {
            label: "replace",
            sql: "REPLACE INTO post_marker_attack(id, value) VALUES ('replace-target', 'mutated')",
          },
          {
            label: "update-returning",
            sql: "UPDATE post_marker_attack SET value='mutated' WHERE id='update-target' RETURNING id",
          },
          {
            label: "insert-returning",
            sql: "INSERT INTO post_marker_attack(id, value) VALUES ('returning-insert', 'mutated') RETURNING id",
          },
          {
            label: "delete-returning",
            sql: "DELETE FROM post_marker_attack WHERE id='delete-target' RETURNING id",
          },
          {
            label: "temp-schema-write",
            sql: "INSERT INTO temp.post_marker_schema_target(id) VALUES ('temp-mutated')",
          },
          {
            label: "attached-schema-write",
            sql: "INSERT INTO attached_guard.post_marker_schema_target(id) VALUES ('attached-mutated')",
          },
          {
            label: "select-update-suffix",
            sql: "SELECT 1; UPDATE post_marker_attack SET value='mutated' WHERE id='update-target'",
            firstStatementOnly: true,
          },
          {
            label: "select-insert-suffix",
            sql: "SELECT 1; INSERT INTO post_marker_attack(id, value) VALUES ('suffix-insert', 'mutated')",
            firstStatementOnly: true,
          },
          {
            label: "with-select-delete-suffix",
            sql: "WITH x AS (SELECT 1) SELECT * FROM x; DELETE FROM post_marker_attack WHERE id='delete-target'",
            firstStatementOnly: true,
          },
        ];

        for (const mode of modes) {
          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          yield* executeSqlMode(
            sql,
            "CREATE TABLE post_marker_attack(id TEXT PRIMARY KEY, value TEXT NOT NULL)",
            mode,
          );
          yield* executeSqlMode(
            sql,
            `INSERT INTO post_marker_attack(id, value) VALUES
              ('update-target', 'original'),
              ('delete-target', 'original'),
              ('replace-target', 'original')`,
            mode,
          );
          yield* executeSqlMode(sql, "CREATE TABLE post_marker_drop_target(id TEXT)", mode);
          yield* executeSqlMode(sql, "CREATE TABLE post_marker_alter_target(id TEXT)", mode);
          yield* executeSqlMode(sql, "CREATE TEMP TABLE post_marker_schema_target(id TEXT)", mode);
          yield* executeSqlMode(sql, "ATTACH DATABASE ':memory:' AS attached_guard", mode);
          yield* executeSqlMode(
            sql,
            "CREATE TABLE attached_guard.post_marker_schema_target(id TEXT)",
            mode,
          );
          const hookCalls = yield* Ref.make(0);
          const hooks = {
            afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
          };

          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationAdmissionTables,
            `post-marker-reads-${mode}`,
          );
          for (const read of reads) {
            yield* executeSqlMode(sql, read, mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 0, `${mode}/${read}`);
          }
          assert.deepStrictEqual(
            yield* sql.unsafe(
              `SELECT count(*) AS count
               FROM agent_control_verification_admission_markers
               WHERE id = ?`,
              [`post-marker-reads-${mode}`],
            ),
            [{ count: 1 }],
            mode,
          );
          const afterReads = yield* Effect.exit(
            executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
              `after-read-matrix-${mode}`,
            ]),
          );
          assert.equal(afterReads._tag, "Failure", mode);
          yield* executeSqlMode(sql, "ROLLBACK", mode);
          assert.equal(yield* Ref.get(hookCalls), 0, mode);
          assert.equal(
            yield* countRows(sql, "boundary_business_writes", `after-read-matrix-${mode}`),
            0,
            mode,
          );

          const noOpMarkerId = `post-marker-no-op-${mode}`;
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* insertCompanionChain(sql, mode, verificationAdmissionTables, noOpMarkerId);
          const noOpReplay = yield* Effect.exit(
            executeSqlMode(
              sql,
              "INSERT OR IGNORE INTO agent_control_verification_admission_markers(id) VALUES (?)",
              mode,
              [noOpMarkerId],
            ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks)),
          );
          assert.equal(noOpReplay._tag, "Failure", `${mode}/no-op-replay`);
          assert.equal(yield* Ref.get(hookCalls), 0, `${mode}/no-op-replay`);
          yield* executeSqlMode(sql, "ROLLBACK", mode);

          for (const attack of attacks) {
            const markerId = `post-marker-${mode}-${attack.label}`;
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* insertCompanionChain(sql, mode, verificationAdmissionTables, markerId);
            const outcome = yield* Effect.exit(
              executeSqlMode(sql, attack.sql, mode).pipe(
                Effect.provideService(NodeSqliteTransactionHooks, hooks),
              ),
            );
            if (attack.firstStatementOnly === true) {
              assert.equal(outcome._tag, "Success", `${mode}/${attack.label}`);
              assert.deepStrictEqual(
                yield* sql.unsafe(`SELECT id, value FROM post_marker_attack ORDER BY id`),
                [
                  { id: "delete-target", value: "original" },
                  { id: "replace-target", value: "original" },
                  { id: "update-target", value: "original" },
                ],
                `${mode}/${attack.label}/first-statement-only`,
              );
              const subsequentDml = yield* Effect.exit(
                executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                  `after-${markerId}`,
                ]),
              );
              assert.equal(subsequentDml._tag, "Failure", `${mode}/${attack.label}`);
            } else {
              assert.equal(outcome._tag, "Failure", `${mode}/${attack.label}`);
              if (Exit.isFailure(outcome)) {
                assert.include(
                  Cause.pretty(outcome.cause),
                  "materialization marker must be the final transaction statement",
                  `${mode}/${attack.label}`,
                );
              }
            }
            yield* executeSqlMode(sql, "ROLLBACK", mode);
            assert.equal(yield* Ref.get(hookCalls), 0, `${mode}/${attack.label}`);
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT id, value FROM post_marker_attack ORDER BY id`),
              [
                { id: "delete-target", value: "original" },
                { id: "replace-target", value: "original" },
                { id: "update-target", value: "original" },
              ],
              `${mode}/${attack.label}`,
            );
            assert.deepStrictEqual(
              yield* sql.unsafe(
                `SELECT
                   (SELECT count(*) FROM temp.post_marker_schema_target) AS tempRows,
                   (SELECT count(*) FROM attached_guard.post_marker_schema_target) AS attachedRows,
                   (SELECT user_version FROM pragma_user_version) AS userVersion,
                   (SELECT writable_schema FROM pragma_writable_schema) AS writableSchema,
                   (SELECT count(*) FROM pragma_database_list
                    WHERE name='post_marker_attached_attack') AS attackAttachments,
                   (SELECT count(*) FROM sqlite_schema
                    WHERE type='table' AND name='post_marker_created') AS createdTables,
                   (SELECT count(*) FROM sqlite_schema
                    WHERE type='table' AND name='post_marker_drop_target') AS dropTargets,
                   (SELECT count(*) FROM pragma_table_info('post_marker_alter_target')) AS alterColumns`,
              ),
              [
                {
                  tempRows: 0,
                  attachedRows: 0,
                  userVersion: 0,
                  writableSchema: 0,
                  attackAttachments: 0,
                  createdTables: 0,
                  dropTargets: 1,
                  alterColumns: 1,
                },
              ],
              `${mode}/${attack.label}`,
            );
          }
        }
      }),
    ),
  );

  it.effect("guards every Implementation marker chain in every execution mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          const hookBoundaries = yield* Ref.make<ReadonlyArray<string>>([]);
          const hooks = {
            afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
              Ref.update(hookBoundaries, (boundaries) => [...boundaries, boundary]),
          };

          for (const table of [
            ...implementationMaterializationTables,
            "agent_control_implementation_turn_accepted",
            ...implementationTransactionalEvidenceTables,
            ...implementationStageStartTables,
            ...implementationStageFinalizationTables,
          ]) {
            const rejected = yield* Effect.exit(
              executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `autocommit-${mode}-${table}`,
              ]),
            );
            assert.isTrue(Exit.isFailure(rejected), `${mode}:${table}`);
            if (Exit.isFailure(rejected)) {
              assert.include(
                Cause.pretty(rejected.cause),
                "persistent materialization marker DML requires an active caller-controlled transaction",
              );
            }
          }

          for (const table of implementationStageFinalizationTables) {
            for (const [formIndex, form] of initialPlanningDmlForms.entries()) {
              const rejected = yield* Effect.exit(
                executeSqlMode(sql, form(table), mode, [
                  `finalization-form-${mode}-${formIndex}-${table}`,
                ]),
              );
              assert.isTrue(Exit.isFailure(rejected), `${mode}:${formIndex}:${table}`);
              if (Exit.isFailure(rejected)) {
                assert.include(
                  Cause.pretty(rejected.cause),
                  "persistent materialization marker DML requires an active caller-controlled transaction",
                );
              }
              assert.deepStrictEqual(
                yield* sql.unsafe(`SELECT count(*) AS count FROM main.${table}`),
                [{ count: 0 }],
                `${mode}:${formIndex}:${table}`,
              );
            }
          }

          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationMaterializationTables.slice(0, -1),
            `partial-materialization-${mode}`,
          );
          const partialMaterialization = yield* Effect.exit(executeSqlMode(sql, "COMMIT", mode));
          assert.isTrue(Exit.isFailure(partialMaterialization), mode);
          if (Exit.isFailure(partialMaterialization)) {
            assert.include(
              Cause.pretty(partialMaterialization.cause),
              "implementation companion chain requires a final marker",
            );
          }

          yield* executeSqlMode(sql, `SAVEPOINT materialization_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationMaterializationTables,
            `materialization-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT materialization_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-implementation-materialization",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT materialization_replay_${mode}`, mode);
          for (const table of implementationMaterializationTables) {
            yield* executeSqlMode(sql, `INSERT OR IGNORE INTO main.${table}(id) VALUES (?)`, mode, [
              `materialization-${mode}`,
            ]);
          }
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT materialization_replay_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);

          yield* executeSqlMode(sql, `SAVEPOINT turn_acceptance_${mode}`, mode);
          yield* executeSqlMode(
            sql,
            "INSERT INTO main.agent_control_implementation_turn_accepted(id) VALUES (?)",
            mode,
            [`turn-accepted-${mode}`],
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT turn_acceptance_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-implementation-materialization",
            "agent-control-implementation-turn-acceptance",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT transactional_evidence_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationTransactionalEvidenceTables,
            `transactional-evidence-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT transactional_evidence_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 2, mode);

          yield* executeSqlMode(sql, `SAVEPOINT stage_start_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationStageStartTables,
            `stage-start-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT stage_start_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-implementation-materialization",
            "agent-control-implementation-turn-acceptance",
            "agent-control-implementation-stage-start",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT stage_start_rollback_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationStageStartTables,
            `stage-start-rollback-${mode}`,
          );
          yield* executeSqlMode(sql, `ROLLBACK TO SAVEPOINT stage_start_rollback_${mode}`, mode);
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT stage_start_rollback_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 3, mode);
          for (const table of implementationStageStartTables) {
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                `stage-start-rollback-${mode}`,
              ]),
              [{ count: 0 }],
            );
          }

          yield* executeSqlMode(sql, `SAVEPOINT stage_finalization_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationStageFinalizationTables,
            `stage-finalization-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT stage_finalization_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-implementation-materialization",
            "agent-control-implementation-turn-acceptance",
            "agent-control-implementation-stage-start",
            "agent-control-implementation-stage-finalization",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT finalization_replay_${mode}`, mode);
          for (const table of implementationStageFinalizationTables) {
            yield* executeSqlMode(sql, `INSERT OR IGNORE INTO main.${table}(id) VALUES (?)`, mode, [
              `stage-finalization-${mode}`,
            ]);
          }
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT finalization_replay_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 4, mode);

          yield* executeSqlMode(sql, `SAVEPOINT finalization_rollback_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            implementationStageFinalizationTables,
            `stage-finalization-rollback-${mode}`,
          );
          yield* executeSqlMode(sql, `ROLLBACK TO SAVEPOINT finalization_rollback_${mode}`, mode);
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT finalization_rollback_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 4, mode);
          for (const table of implementationStageFinalizationTables) {
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                `stage-finalization-rollback-${mode}`,
              ]),
              [{ count: 0 }],
            );
          }
        }
      }),
    ),
  );

  it.effect("guards every Verification turn boundary in every execution mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          const hookBoundaries = yield* Ref.make<ReadonlyArray<string>>([]);
          const hooks = {
            afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
              Ref.update(hookBoundaries, (boundaries) => [...boundaries, boundary]),
          };

          for (const table of [
            ...verificationMaterializationTables,
            "agent_control_verification_turn_accepted",
            ...verificationTransactionalEvidenceTables,
            ...verificationStageStartTables,
          ]) {
            const rejected = yield* Effect.exit(
              executeSqlMode(sql, `INSERT INTO main.${table}(id) VALUES (?)`, mode, [
                `autocommit-${mode}-${table}`,
              ]),
            );
            assert.isTrue(Exit.isFailure(rejected), `${mode}:${table}`);
            if (Exit.isFailure(rejected)) {
              assert.include(
                Cause.pretty(rejected.cause),
                "persistent materialization marker DML requires an active caller-controlled transaction",
              );
            }
          }

          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationMaterializationTables.slice(0, -1),
            `verification-partial-${mode}`,
          );
          const partialMaterialization = yield* Effect.exit(executeSqlMode(sql, "COMMIT", mode));
          assert.isTrue(Exit.isFailure(partialMaterialization), mode);
          if (Exit.isFailure(partialMaterialization)) {
            assert.include(
              Cause.pretty(partialMaterialization.cause),
              "verification companion chain requires a final marker",
            );
          }

          yield* executeSqlMode(sql, `SAVEPOINT verification_materialization_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationMaterializationTables,
            `verification-materialization-${mode}`,
          );
          yield* executeSqlMode(
            sql,
            `RELEASE SAVEPOINT verification_materialization_${mode}`,
            mode,
          ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-verification-materialization",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT verification_replay_${mode}`, mode);
          for (const table of verificationMaterializationTables) {
            yield* executeSqlMode(sql, `INSERT OR IGNORE INTO main.${table}(id) VALUES (?)`, mode, [
              `verification-materialization-${mode}`,
            ]);
          }
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_replay_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);

          yield* executeSqlMode(sql, `SAVEPOINT verification_terminal_${mode}`, mode);
          yield* executeSqlMode(
            sql,
            "UPDATE main.agent_control_verification_deliveries SET id = id WHERE id = ?",
            mode,
            [`verification-materialization-${mode}`],
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_terminal_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 1, mode);

          yield* executeSqlMode(sql, `SAVEPOINT verification_turn_${mode}`, mode);
          yield* executeSqlMode(
            sql,
            "INSERT INTO main.agent_control_verification_turn_accepted(id) VALUES (?)",
            mode,
            [`verification-turn-${mode}`],
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_turn_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-verification-materialization",
            "agent-control-verification-turn-acceptance",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT verification_delivery_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationTransactionalEvidenceTables,
            `verification-delivery-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_delivery_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal((yield* Ref.get(hookBoundaries)).length, 2, mode);

          yield* executeSqlMode(sql, `SAVEPOINT verification_stage_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationStageStartTables,
            `verification-stage-${mode}`,
          );
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT verification_stage_${mode}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
            "agent-control-verification-materialization",
            "agent-control-verification-turn-acceptance",
            "agent-control-verification-stage-start",
          ]);

          yield* executeSqlMode(sql, `SAVEPOINT verification_stage_rollback_${mode}`, mode);
          yield* insertCompanionChain(
            sql,
            mode,
            verificationStageStartTables,
            `verification-stage-rollback-${mode}`,
          );
          yield* executeSqlMode(
            sql,
            `ROLLBACK TO SAVEPOINT verification_stage_rollback_${mode}`,
            mode,
          );
          yield* executeSqlMode(
            sql,
            `RELEASE SAVEPOINT verification_stage_rollback_${mode}`,
            mode,
          ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
          assert.equal((yield* Ref.get(hookBoundaries)).length, 3, mode);
          for (const table of verificationStageStartTables) {
            assert.deepStrictEqual(
              yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [
                `verification-stage-rollback-${mode}`,
              ]),
              [{ count: 0 }],
            );
          }
        }
      }),
    ),
  );

  it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
    { phase: "defect" },
    { phase: "interrupt" },
  ])("preserves an Implementation final marker hook $phase after commit", ({ phase }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(sql);
        const id = `implementation-finalization-hook-${phase}`;
        const exit = yield* Effect.exit(
          sql
            .withTransaction(
              insertCompanionChain(sql, "unprepared", implementationStageFinalizationTables, id),
            )
            .pipe(
              Effect.provideService(NodeSqliteTransactionHooks, {
                afterCommitBeforeReturn: () =>
                  phase === "defect"
                    ? Effect.die(new Error("implementation finalization hook defect"))
                    : Effect.interrupt,
              }),
            ),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          if (phase === "defect") {
            assert.include(Cause.pretty(exit.cause), "implementation finalization hook defect");
          } else {
            assert.isTrue(Cause.hasInterruptsOnly(exit.cause));
          }
        }
        for (const table of implementationStageFinalizationTables) {
          assert.deepStrictEqual(
            yield* sql.unsafe(`SELECT count(*) AS count FROM ${table} WHERE id = ?`, [id]),
            [{ count: 1 }],
          );
        }
      }),
    ),
  );

  it.effect("resolves every Initial Planning finalization target against MAIN", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const table of initialPlanningFinalizationTables) {
          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          yield* sql.unsafe(`CREATE TEMP TABLE ${table}(id TEXT PRIMARY KEY)`);
          yield* sql`BEGIN`;
          const shadowed = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('shadowed')`),
          );
          assert.isTrue(Exit.isFailure(shadowed), table);
          yield* sql`ROLLBACK`;
          assert.deepStrictEqual(
            yield* sql.unsafe(`SELECT count(*) AS count FROM temp.${table}`),
            [{ count: 0 }],
            table,
          );
          yield* sql`BEGIN`;
          yield* sql.unsafe(`INSERT INTO main.${table}(id) VALUES ('main-row')`);
          yield* sql`COMMIT`;

          for (const statement of [
            `UPDATE ${table} SET id = 'mutated' WHERE id = 'main-row'`,
            `DELETE FROM ${table} WHERE id = 'main-row'`,
          ]) {
            const exit = yield* Effect.exit(sql.unsafe(statement));
            assert.isTrue(Exit.isFailure(exit), `${table}:${statement}`);
            if (Exit.isFailure(exit)) {
              assert.include(
                Cause.pretty(exit.cause),
                "persistent materialization marker DML requires an active caller-controlled transaction",
              );
            }
          }

          yield* sql.unsafe(`DROP TABLE temp.${table}`);
          yield* sql.unsafe(`DROP TABLE main.${table}`);
          const missingAutocommit = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('missing')`),
          );
          assert.isTrue(Exit.isFailure(missingAutocommit), table);
          if (Exit.isFailure(missingAutocommit)) {
            assert.include(
              Cause.pretty(missingAutocommit.cause),
              "persistent materialization marker DML requires an active caller-controlled transaction",
            );
          }
          yield* sql`BEGIN`;
          const missingMain = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('missing-in-transaction')`),
          );
          assert.isTrue(Exit.isFailure(missingMain), table);
          yield* sql`ROLLBACK`;
          yield* sql.unsafe(`CREATE VIEW main.${table} AS SELECT 'view' AS id`);
          yield* sql`BEGIN`;
          const mainView = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('view-row')`),
          );
          assert.isTrue(Exit.isFailure(mainView), table);
          yield* sql`ROLLBACK`;
        }
      }),
    ),
  );

  it.effect("resolves the complete Implementation finalization chain against MAIN", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(sql);
        for (const table of implementationStageFinalizationTables) {
          yield* sql.unsafe(`CREATE TEMP TABLE ${table}(id TEXT PRIMARY KEY)`);
        }

        yield* sql`BEGIN`;
        for (const table of implementationStageFinalizationTables) {
          const shadowed = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('shadowed')`),
          );
          assert.isTrue(Exit.isFailure(shadowed), table);
        }
        yield* sql`ROLLBACK`;
        for (const table of implementationStageFinalizationTables) {
          assert.deepStrictEqual(
            yield* sql.unsafe(`SELECT count(*) AS count FROM temp.${table}`),
            [{ count: 0 }],
            table,
          );
        }

        yield* sql`BEGIN`;
        for (const table of implementationStageFinalizationTables) {
          yield* sql.unsafe(`INSERT INTO main.${table}(id) VALUES ('main-row')`);
        }
        yield* sql`COMMIT`;

        for (const table of implementationStageFinalizationTables) {
          for (const statement of [
            `UPDATE ${table} SET id = 'mutated' WHERE id = 'main-row'`,
            `DELETE FROM ${table} WHERE id = 'main-row'`,
          ]) {
            const exit = yield* Effect.exit(sql.unsafe(statement));
            assert.isTrue(Exit.isFailure(exit), `${table}:${statement}`);
            if (Exit.isFailure(exit)) {
              assert.include(
                Cause.pretty(exit.cause),
                "persistent materialization marker DML requires an active caller-controlled transaction",
              );
            }
          }

          yield* sql.unsafe(`DROP TABLE temp.${table}`);
          yield* sql.unsafe(`DROP TABLE main.${table}`);
          const autocommit = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('missing')`),
          );
          assert.isTrue(Exit.isFailure(autocommit), table);
          if (Exit.isFailure(autocommit)) {
            assert.include(
              Cause.pretty(autocommit.cause),
              "persistent materialization marker DML requires an active caller-controlled transaction",
            );
          }

          yield* sql`BEGIN`;
          const missing = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('missing-in-transaction')`),
          );
          assert.isTrue(Exit.isFailure(missing), table);
          yield* sql`ROLLBACK`;

          yield* sql.unsafe(`CREATE VIEW main.${table} AS SELECT 'view' AS id`);
          yield* sql`BEGIN`;
          const view = yield* Effect.exit(
            sql.unsafe(`INSERT INTO ${table}(id) VALUES ('view-row')`),
          );
          assert.isTrue(Exit.isFailure(view), table);
          yield* sql`ROLLBACK`;
        }
      }),
    ),
  );

  it.effect(
    "rolls back the complete Initial Planning chain when final marker inspection fails",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* makeScopedMemoryClient({
            _testHooks: {
              beforeMarkerChanges: () => {
                throw new Error("initial planning marker inspection failed");
              },
            },
          });
          yield* initializeMaterializationBoundaryTables(sql);
          const exit = yield* Effect.exit(
            sql.withTransaction(
              Effect.gen(function* () {
                for (const table of initialPlanningFinalizationTables) {
                  yield* sql.unsafe(`INSERT INTO ${table}(id) VALUES (?)`, ["failed-chain"]);
                }
              }),
            ),
          );
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            assert.include(Cause.pretty(exit.cause), "initial planning marker inspection failed");
          }
          for (const table of initialPlanningFinalizationTables) {
            assert.deepStrictEqual(yield* sql.unsafe(`SELECT count(*) AS count FROM ${table}`), [
              { count: 0 },
            ]);
          }
        }),
      ),
  );

  it.effect("returns a typed failure when an unprepared statement cannot be prepared", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const error = yield* Effect.flip(sql.unsafe("SELECT FROM").unprepared);

      assert.equal(error._tag, "SqlError");
      assert.equal(error.reason.operation, "prepare");
    }),
  );

  it.effect(
    "treats only the final Prepare commit marker as the boundary in every execution mode",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const modes: ReadonlyArray<SqlExecutionMode> = [
            "statement",
            "values",
            "raw",
            "unprepared",
          ];
          for (const mode of modes) {
            const sql = yield* makeScopedMemoryClient();
            yield* initializeMaterializationBoundaryTables(sql);
            const hookCalls = yield* Ref.make(0);
            const hooks = {
              afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
            };
            const id = `prepare-state-before-evidence-${mode}`;
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              "INSERT INTO agent_control_controlled_thread_prepare_finalizations(id) VALUES (?)",
              mode,
              [id],
            );
            yield* executeSqlMode(
              sql,
              "INSERT INTO boundary_business_writes(id) VALUES (?)",
              mode,
              [id],
            );
            yield* executeSqlMode(
              sql,
              "INSERT INTO agent_control_controlled_thread_prepare_final_commit_markers(id) VALUES (?)",
              mode,
              [id],
            );
            yield* executeSqlMode(sql, "COMMIT", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 1, mode);

            const rollbackId = `prepare-state-rollback-${mode}`;
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              "INSERT INTO agent_control_controlled_thread_prepare_finalizations(id) VALUES (?)",
              mode,
              [rollbackId],
            );
            yield* executeSqlMode(
              sql,
              "INSERT INTO agent_control_controlled_thread_prepare_final_commit_markers(id) VALUES (?)",
              mode,
              [rollbackId],
            );
            yield* executeSqlMode(sql, "ROLLBACK", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            assert.equal(yield* Ref.get(hookCalls), 1, mode);

            const rejectedId = `prepare-final-marker-not-last-${mode}`;
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              "INSERT INTO agent_control_controlled_thread_prepare_final_commit_markers(id) VALUES (?)",
              mode,
              [rejectedId],
            );
            const rejected = yield* Effect.exit(
              executeSqlMode(sql, "INSERT INTO boundary_business_writes(id) VALUES (?)", mode, [
                rejectedId,
              ]),
            );
            assert.equal(Exit.isFailure(rejected), true, mode);
            yield* executeSqlMode(sql, "ROLLBACK", mode);
            assert.equal(yield* Ref.get(hookCalls), 1, mode);
          }
        }),
      ),
  );

  it.effect("rejects every persistent marker form before autocommit in every mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
        const scenarios: ReadonlyArray<{
          readonly label: string;
          readonly statements: ReadonlyArray<(id: string) => string>;
        }> = [
          {
            label: "orchestration-only",
            statements: [(id) => `INSERT INTO ${markerTables.orchestration}(id) VALUES ('${id}')`],
          },
          {
            label: "coordinator-only",
            statements: [(id) => `INSERT INTO ${markerTables.coordinator}(id) VALUES ('${id}')`],
          },
          {
            label: "prepare-only",
            statements: [
              (id) =>
                `INSERT INTO agent_control_controlled_thread_prepare_final_commit_markers(id)
                 VALUES ('${id}')`,
            ],
          },
          {
            label: "both-sequentially",
            statements: [
              (id) => `INSERT INTO ${markerTables.orchestration}(id) VALUES ('${id}')`,
              (id) => `INSERT INTO ${markerTables.coordinator}(id) VALUES ('${id}')`,
            ],
          },
          {
            label: "quoted",
            statements: [
              (id) => `INSERT INTO "${markerTables.orchestration}"(id) VALUES ('${id}')`,
              (id) => `INSERT INTO [${markerTables.coordinator}](id) VALUES ('${id}')`,
            ],
          },
          {
            label: "main-qualified",
            statements: [
              (id) => `INSERT INTO main.${markerTables.orchestration}(id) VALUES ('${id}')`,
              (id) => `INSERT INTO main.${markerTables.coordinator}(id) VALUES ('${id}')`,
            ],
          },
          {
            label: "with-insert",
            statements: [
              (id) =>
                `WITH source(id) AS (VALUES ('${id}'))
                 INSERT INTO ${markerTables.orchestration}(id) SELECT id FROM source`,
              (id) =>
                `WITH source(id) AS (VALUES ('${id}'))
                 INSERT INTO ${markerTables.coordinator}(id) SELECT id FROM source`,
            ],
          },
          {
            label: "replace",
            statements: [
              (id) => `REPLACE INTO ${markerTables.orchestration}(id) VALUES ('${id}')`,
              (id) => `REPLACE INTO ${markerTables.coordinator}(id) VALUES ('${id}')`,
            ],
          },
          {
            label: "or-abort",
            statements: [
              (id) => `INSERT OR ABORT INTO ${markerTables.orchestration}(id) VALUES ('${id}')`,
              (id) => `INSERT OR ABORT INTO ${markerTables.coordinator}(id) VALUES ('${id}')`,
            ],
          },
          {
            label: "or-ignore",
            statements: [
              (id) => `INSERT OR IGNORE INTO ${markerTables.orchestration}(id) VALUES ('${id}')`,
              (id) => `INSERT OR IGNORE INTO ${markerTables.coordinator}(id) VALUES ('${id}')`,
            ],
          },
          {
            label: "upsert-do-nothing",
            statements: [
              (id) =>
                `INSERT INTO ${markerTables.orchestration}(id) VALUES ('${id}')
                 ON CONFLICT(id) DO NOTHING`,
              (id) =>
                `INSERT INTO ${markerTables.coordinator}(id) VALUES ('${id}')
                 ON CONFLICT(id) DO NOTHING`,
            ],
          },
          {
            label: "returning",
            statements: [
              (id) => `INSERT INTO ${markerTables.orchestration}(id) VALUES ('${id}') RETURNING id`,
              (id) => `INSERT INTO ${markerTables.coordinator}(id) VALUES ('${id}') RETURNING id`,
            ],
          },
        ];

        for (const mode of modes) {
          const sql = yield* makeScopedMemoryClient();
          yield* initializeMaterializationBoundaryTables(sql);
          const hookCalls = yield* Ref.make(0);
          const hooks = {
            afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
          };
          yield* executeSqlMode(
            sql,
            `CREATE TEMP TABLE ${markerTables.orchestration}(id TEXT PRIMARY KEY)`,
            mode,
          );
          yield* executeSqlMode(
            sql,
            `CREATE TEMP TABLE ${markerTables.coordinator}(id TEXT PRIMARY KEY)`,
            mode,
          );
          yield* executeSqlMode(sql, "ATTACH DATABASE ':memory:' AS attached", mode);
          yield* executeSqlMode(
            sql,
            `CREATE TABLE attached.${markerTables.coordinator}(id TEXT PRIMARY KEY)`,
            mode,
          );
          yield* executeSqlMode(
            sql,
            `INSERT INTO temp.${markerTables.orchestration}(id) VALUES (?)`,
            mode,
            [`explicit-temp-${mode}`],
          ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
          yield* executeSqlMode(
            sql,
            `INSERT INTO attached.${markerTables.coordinator}(id) VALUES (?)`,
            mode,
            [`explicit-attached-${mode}`],
          ).pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
          assert.equal(yield* Ref.get(hookCalls), 0, mode);
          assert.deepStrictEqual(
            yield* sql.unsafe(
              `SELECT
                 (SELECT count(*) FROM temp.${markerTables.orchestration}) AS tempMarkers,
                 (SELECT count(*) FROM attached.${markerTables.coordinator}) AS attachedMarkers`,
            ),
            [{ tempMarkers: 1, attachedMarkers: 1 }],
            mode,
          );
          yield* executeSqlMode(sql, `DELETE FROM temp.${markerTables.orchestration}`, mode);
          yield* executeSqlMode(sql, `DELETE FROM attached.${markerTables.coordinator}`, mode);
          let expectedHookCalls = 0;

          for (const scenario of scenarios) {
            const id = `autocommit-${mode}-${scenario.label}`;
            for (const makeStatement of scenario.statements) {
              const failure = yield* Effect.flip(
                executeSqlMode(sql, makeStatement(id), mode).pipe(
                  Effect.provideService(NodeSqliteTransactionHooks, hooks),
                ),
              );
              assert.equal(failure._tag, "SqlError", `${mode}/${scenario.label}`);
              assert.equal(failure.reason.operation, "execute", `${mode}/${scenario.label}`);
              assert.include(
                String((failure.reason as { readonly cause?: unknown }).cause),
                "persistent materialization marker DML requires an active caller-controlled transaction",
                `${mode}/${scenario.label}`,
              );
            }

            assert.equal(yield* Ref.get(hookCalls), expectedHookCalls, `${mode}/${scenario.label}`);
            assert.deepStrictEqual(
              yield* sql.unsafe(
                `SELECT
                   (SELECT count(*) FROM main.${markerTables.orchestration} WHERE id = ?) AS mainOrchestration,
                   (SELECT count(*) FROM main.${markerTables.coordinator} WHERE id = ?) AS mainCoordinator,
                   (SELECT count(*) FROM temp.${markerTables.orchestration}) AS tempOrchestration,
                   (SELECT count(*) FROM temp.${markerTables.coordinator}) AS tempCoordinator`,
                [id, id],
              ),
              [
                {
                  mainOrchestration: 0,
                  mainCoordinator: 0,
                  tempOrchestration: 0,
                  tempCoordinator: 0,
                },
              ],
              `${mode}/${scenario.label}`,
            );

            yield* executeSqlMode(
              sql,
              `INSERT INTO boundary_business_writes(id) VALUES (?)`,
              mode,
              [`ordinary-after-${id}`],
            );

            const recoveryId = `transaction-after-${id}`;
            yield* executeSqlMode(sql, "BEGIN", mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO main.${markerTables.orchestration}(id) VALUES (?)`,
              mode,
              [recoveryId],
            );
            yield* executeSqlMode(
              sql,
              `INSERT INTO main.${markerTables.coordinator}(id) VALUES (?)`,
              mode,
              [recoveryId],
            );
            yield* executeSqlMode(sql, "COMMIT", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            );
            expectedHookCalls += 1;
            assert.equal(yield* Ref.get(hookCalls), expectedHookCalls, `${mode}/${scenario.label}`);
          }

          assert.equal(yield* countRows(sql, "boundary_business_writes"), scenarios.length, mode);
        }
      }),
    ),
  );

  it.effect("keeps contextual keywords and WITH statements SQLite-compatible in every mode", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];

        for (const mode of modes) {
          const sql = yield* makeScopedMemoryClient();
          const hookCalls = yield* Ref.make(0);
          const hooks = {
            afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
          };
          yield* initializeMaterializationBoundaryTables(sql);
          yield* executeSqlMode(sql, "CREATE TABLE abort(id TEXT PRIMARY KEY)", mode);
          for (const identifier of [
            "fail",
            "ignore",
            "replace",
            "rollback",
            "recursive",
            "materialized",
          ]) {
            yield* executeSqlMode(sql, `CREATE TABLE ${identifier}(id TEXT PRIMARY KEY)`, mode);
            yield* executeSqlMode(
              sql,
              `INSERT INTO ${identifier}(id) VALUES ('${identifier}')`,
              mode,
            );
          }
          yield* executeSqlMode(sql, "CREATE TABLE ordinary_table(id TEXT PRIMARY KEY)", mode);
          yield* executeSqlMode(
            sql,
            "CREATE TABLE update_target(id TEXT PRIMARY KEY, value INT)",
            mode,
          );
          yield* executeSqlMode(sql, "INSERT INTO abort VALUES ('keyword-id')", mode);
          yield* executeSqlMode(sql, "WITH abort AS (SELECT 1 AS x) SELECT x FROM abort", mode);
          yield* executeSqlMode(sql, "WITH c AS (SELECT 1) VALUES (2)", mode);
          yield* executeSqlMode(sql, "WITH replace AS (SELECT 1 AS x) SELECT x FROM replace", mode);
          yield* executeSqlMode(sql, "INSERT INTO ordinary_table(id) VALUES ('plain')", mode);
          yield* executeSqlMode(
            sql,
            "INSERT OR ABORT INTO ordinary_table(id) VALUES ('or-abort')",
            mode,
          );
          yield* executeSqlMode(
            sql,
            "INSERT OR IGNORE INTO ordinary_table(id) VALUES ('or-ignore')",
            mode,
          );
          yield* executeSqlMode(sql, "REPLACE INTO ordinary_table(id) VALUES ('replace')", mode);
          yield* executeSqlMode(
            sql,
            "INSERT INTO ordinary_table(id) VALUES ('upsert') ON CONFLICT(id) DO UPDATE SET id = excluded.id",
            mode,
          );
          yield* executeSqlMode(
            sql,
            "INSERT INTO update_target(id, value) VALUES ('update', 1), ('delete', 2)",
            mode,
          );
          yield* executeSqlMode(
            sql,
            "WITH c AS (SELECT 1) UPDATE update_target SET value = 3 WHERE id = 'update'",
            mode,
          );
          yield* executeSqlMode(
            sql,
            "WITH c AS (SELECT 1) DELETE FROM update_target WHERE id = 'delete'",
            mode,
          );
          yield* executeSqlMode(
            sql,
            `WITH ${markerTables.coordinator} AS (
               SELECT count(*) AS value FROM main.${markerTables.coordinator}
             )
             SELECT value FROM ${markerTables.coordinator}`,
            mode,
          );
          yield* executeSqlMode(
            sql,
            `WITH marker_read AS (
               SELECT count(*) AS value FROM main.${markerTables.coordinator}
             )
             INSERT INTO ordinary_table(id) SELECT 'marker-source' FROM marker_read`,
            mode,
          );
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(
            sql,
            `WITH ordinary AS (SELECT id FROM ordinary_table LIMIT 1)
             INSERT INTO ${markerTables.coordinator}(id)
             SELECT 'marker-target' FROM ordinary`,
            mode,
          );
          yield* executeSqlMode(sql, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );

          assert.deepStrictEqual(yield* sql`SELECT id FROM abort`, [{ id: "keyword-id" }], mode);
          assert.deepStrictEqual(
            yield* sql`SELECT id, value FROM update_target`,
            [{ id: "update", value: 3 }],
            mode,
          );
          assert.equal(yield* Ref.get(hookCalls), 1, mode);
        }
      }),
    ),
  );

  it.effect(
    "classifies only executable SQL across comments, literals, and quoted identifiers",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* initializeMaterializationBoundaryTables(sql);
        const hookCalls = yield* Ref.make(0);
        const hooks = {
          afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
        };

        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe(`
            -- INSERT INTO agent_control_controlled_thread_materialization_accepted
            SELECT 'SAVEPOINT ROLLBACK RELEASE COMMIT' AS control_text
          `);
            yield* sql.unsafe(`
            SELECT
              'INSERT INTO agent_control_controlled_thread_materialization_accepted'
                AS marker_text,
              'escaped ''SAVEPOINT'' and ''ROLLBACK''' AS escaped_text
          `);
            yield* sql.unsafe(`
            WITH agent_control_controlled_thread_materialization_accepted AS (SELECT 1 AS value)
            SELECT value
            FROM agent_control_controlled_thread_materialization_accepted
          `);
            yield* sql.unsafe(`
            SELECT
              1 AS "INSERT INTO agent_control_controlled_thread_materialization_accepted",
              2 AS \`ROLLBACK TO SAVEPOINT effect_sql_1\`,
              3 AS [RELEASE SAVEPOINT effect_sql_1]
          `);
            yield* sql.unsafe(`
            SELECT 1;
            INSERT INTO agent_control_controlled_thread_materialization_accepted(id)
            VALUES ('unexecuted-second-statement')
          `);
            yield* sql.unsafe(`
            ; ;
            /* SAVEPOINT false_name */
            /* INSERT INTO agent_control_controlled_thread_materialization_accepted */
            SAVEPOINT effect_sql_1
          `);
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('lexical-savepoint-write')`;
            yield* sql.unsafe(`
            -- INSERT INTO agent_control_controlled_thread_materialization_accepted
            /* RELEASE effect_sql_1 */
            ROLLBACK TO SAVEPOINT effect_sql_1
          `);
            yield* sql.unsafe(`
            /* ROLLBACK TO SAVEPOINT effect_sql_1 */
            RELEASE SAVEPOINT effect_sql_1
          `);
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('lexical-outer-write')`;
          }),
        );

        assert.equal(yield* Ref.get(hookCalls), 0);
        assert.equal(
          yield* countRows(sql, "boundary_business_writes", "lexical-savepoint-write"),
          0,
        );
        assert.equal(yield* countRows(sql, "boundary_business_writes", "lexical-outer-write"), 1);
        assert.equal(
          yield* countRows(
            sql,
            "agent_control_controlled_thread_materialization_accepted",
            "unexecuted-second-statement",
          ),
          0,
        );

        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql.unsafe(`
              /* whitespace and comments */
              -- before the real orchestration marker
              INSERT INTO orchestration_agent_control_thread_materialization_receipts(id)
              VALUES ('commented-real-marker')
              /* comment after the real marker */
            `);
              yield* sql.unsafe(`
              /* INSERT INTO orchestration_agent_control_thread_materialization_receipts */
              INSERT INTO agent_control_controlled_thread_materialization_accepted(id)
              VALUES ('commented-real-marker')
              -- trailing marker comment
            `);
            }),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));

        assert.equal(yield* Ref.get(hookCalls), 1);
        assert.equal(
          yield* countRows(
            sql,
            "agent_control_controlled_thread_materialization_accepted",
            "commented-real-marker",
          ),
          1,
        );

        yield* sql
          .withTransaction(
            sql.unsafe(`
              INSERT INTO "agent_control_controlled_thread_materialization_accepted"(id)
              VALUES ('quoted-marker-identifier')
            `),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.equal(yield* Ref.get(hookCalls), 2);
        assert.equal(
          yield* countRows(
            sql,
            "agent_control_controlled_thread_materialization_accepted",
            "quoted-marker-identifier",
          ),
          1,
        );
      }),
  );

  it.effect("keeps marker text in comments, literals, aliases, and sources inert", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      yield* sql`ATTACH DATABASE ':memory:' AS other`;
      yield* sql`
        CREATE TABLE other.agent_control_controlled_thread_materialization_accepted(
          id TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        CREATE TABLE agent_control_controlled_thread_materialization_accepted_similar(
          id TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        CREATE TABLE marker_name_columns(
          id TEXT PRIMARY KEY,
          orchestration_agent_control_thread_materialization_receipts TEXT,
          "agent_control_controlled_thread_materialization_accepted" TEXT
        )
      `;
      yield* sql`
        CREATE TABLE "agent_control_controlled_thread_materialization_accepted""suffix"(
          id TEXT PRIMARY KEY
        )
      `;
      yield* sql.unsafe(`
        CREATE TABLE \`agent_control_controlled_thread_materialization_accepted\`\`suffix\`(
          id TEXT PRIMARY KEY
        )
      `);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql.unsafe(`
              /* agent_control_controlled_thread_materialization_accepted */
              -- orchestration_agent_control_thread_materialization_receipts
              SELECT 'agent_control_controlled_thread_materialization_accepted' AS marker_text
            `);
            yield* sql.unsafe(`
              WITH agent_control_controlled_thread_materialization_accepted AS (SELECT 1 AS value)
              SELECT value AS orchestration_agent_control_thread_materialization_receipts
              FROM agent_control_controlled_thread_materialization_accepted
            `);
            yield* sql.unsafe(`
              SELECT
                1 AS agent_control_controlled_thread_materialization_accepted,
                2 AS "orchestration_agent_control_thread_materialization_receipts"
            `);
            yield* sql.unsafe(`
              SELECT count(*) AS marker_source
              FROM main.agent_control_controlled_thread_materialization_accepted
            `);
            yield* sql.unsafe(`
              INSERT INTO other.agent_control_controlled_thread_materialization_accepted(id)
              VALUES ('other-schema')
            `);
            yield* sql.unsafe(`
              INSERT INTO agent_control_controlled_thread_materialization_accepted_similar(id)
              VALUES ('similar')
            `);
            yield* sql.unsafe(`
              INSERT INTO boundary_business_writes
                AS agent_control_controlled_thread_materialization_accepted(id)
              VALUES ('unquoted-alias')
            `);
            yield* sql.unsafe(`
              INSERT INTO boundary_business_writes
                AS "orchestration_agent_control_thread_materialization_receipts"(id)
              VALUES ('quoted-alias')
            `);
            yield* sql.unsafe(`
              INSERT INTO marker_name_columns(
                id,
                orchestration_agent_control_thread_materialization_receipts,
                "agent_control_controlled_thread_materialization_accepted"
              )
              VALUES ('marker-columns', 'orchestration-column', 'coordinator-column')
            `);
            yield* sql.unsafe(`
              INSERT INTO "agent_control_controlled_thread_materialization_accepted""suffix"(id)
              VALUES ('double-escaped')
            `);
            yield* sql.unsafe(`
              INSERT INTO \`agent_control_controlled_thread_materialization_accepted\`\`suffix\`(id)
              VALUES ('backtick-escaped')
            `);
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('negative-matrix')`;
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));

      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(yield* countRows(sql, "boundary_business_writes", "negative-matrix"), 1);
      assert.deepStrictEqual(
        yield* sql`SELECT id FROM other.agent_control_controlled_thread_materialization_accepted`,
        [{ id: "other-schema" }],
      );
    }),
  );

  it.effect("fails closed on ambiguous marker DML and resets after rollback", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const malformedStatements = [
        {
          sql: "/* agent_control_controlled_thread_materialization_accepted",
          potentialMarker: false,
        },
        {
          sql: "SELECT 'orchestration_agent_control_thread_materialization_receipts",
          potentialMarker: false,
        },
        {
          sql: 'INSERT INTO "agent_control_controlled_thread_materialization_accepted(id) VALUES (1)',
          potentialMarker: false,
        },
        {
          sql: "INSERT INTO `agent_control_controlled_thread_materialization_accepted(id) VALUES (1)",
          potentialMarker: false,
        },
        {
          sql: "INSERT INTO [agent_control_controlled_thread_materialization_accepted(id) VALUES (1)",
          potentialMarker: false,
        },
        { sql: "INSERT", potentialMarker: true },
        { sql: "INSERT INTO", potentialMarker: true },
        { sql: "WITH", potentialMarker: true },
        { sql: "WITH cte AS (SELECT 1", potentialMarker: true },
        {
          sql: "INSERT OR UPSERT INTO agent_control_controlled_thread_materialization_accepted(id) VALUES (1)",
          potentialMarker: true,
        },
        {
          sql: "INSERT INTO (agent_control_controlled_thread_materialization_accepted)(id) VALUES (1)",
          potentialMarker: true,
        },
        {
          sql: "INSERT INTO 'agent_control_controlled_thread_materialization_accepted'(id) VALUES (1)",
          potentialMarker: true,
        },
      ] as const;

      for (const statement of malformedStatements) {
        yield* sql.unsafe("BEGIN");
        const failure = yield* Effect.exit(sql.unsafe(statement.sql));
        assert.equal(failure._tag, "Failure", statement.sql);
        if (Exit.isFailure(failure)) {
          const error = Cause.pretty(failure.cause);
          if (statement.potentialMarker) {
            assert.include(error, "potential materialization marker DML", statement.sql);
          } else {
            assert.notInclude(error, "potential materialization marker DML", statement.sql);
          }
        }
        yield* sql.unsafe("ROLLBACK");
      }

      const hookCalls = yield* Ref.make(0);
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            yield* insertOrchestrationMarker(sql, "after-ambiguous-dml");
            yield* insertCoordinatorMarker(sql, "after-ambiguous-dml");
          }),
        )
        .pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
          }),
        );
      assert.equal(yield* Ref.get(hookCalls), 1);
    }),
  );

  it.effect("uses native change counts for ignored and empty marker inserts in every mode", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      const fixtureHookCalls = yield* Ref.make(0);
      const fixtureHooks = {
        afterCommitBeforeReturn: () => Ref.update(fixtureHookCalls, (count) => count + 1),
      };
      const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
      let expectedHookCalls = 0;
      let expectedFixtureHookCalls = 0;

      for (const mode of modes) {
        for (const conflict of ["or-ignore", "do-nothing"] as const) {
          const id = `no-op-${conflict}-${mode}`;
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(
            sql,
            `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`,
            mode,
            [id],
          );
          yield* executeSqlMode(sql, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, fixtureHooks),
          );
          expectedFixtureHookCalls += 1;
          assert.equal(yield* Ref.get(fixtureHookCalls), expectedFixtureHookCalls);
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(
            sql,
            `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
            mode,
            [id],
          );
          yield* executeSqlMode(
            sql,
            conflict === "or-ignore"
              ? `INSERT OR IGNORE INTO ${markerTables.coordinator}(id) VALUES (?)`
              : `INSERT INTO ${markerTables.coordinator}(id)
                   VALUES (?) ON CONFLICT(id) DO NOTHING`,
            mode,
            [id],
          );
          yield* executeSqlMode(sql, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
        }

        const emptyId = `empty-select-${mode}`;
        yield* executeSqlMode(sql, "BEGIN", mode);
        for (const table of Object.values(markerTables)) {
          yield* executeSqlMode(sql, `INSERT INTO ${table}(id) SELECT ? WHERE 0`, mode, [emptyId]);
        }
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        assert.equal(yield* Ref.get(hookCalls), expectedHookCalls);

        const validId = `after-no-op-${mode}`;
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(
          sql,
          `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
          mode,
          [validId],
        );
        yield* executeSqlMode(sql, `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`, mode, [
          validId,
        ]);
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        expectedHookCalls += 1;
        assert.equal(yield* Ref.get(hookCalls), expectedHookCalls);

        for (const replace of ["INSERT OR REPLACE", "REPLACE"] as const) {
          const replaceId = `conflicting-${replace.toLowerCase().replaceAll(" ", "-")}-${mode}`;
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(
            sql,
            `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`,
            mode,
            [replaceId],
          );
          yield* executeSqlMode(sql, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, fixtureHooks),
          );
          expectedFixtureHookCalls += 1;
          assert.equal(yield* Ref.get(fixtureHookCalls), expectedFixtureHookCalls);
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(
            sql,
            `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
            mode,
            [replaceId],
          );
          yield* executeSqlMode(
            sql,
            `${replace} INTO ${markerTables.coordinator}(id) VALUES (?)`,
            mode,
            [replaceId],
          );
          yield* executeSqlMode(sql, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          expectedHookCalls += 1;
          assert.equal(yield* Ref.get(hookCalls), expectedHookCalls);
        }
      }
    }),
  );

  it.effect("covers explicit marker DML edge forms in every execution mode", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      const fixtureHookCalls = yield* Ref.make(0);
      const fixtureHooks = {
        afterCommitBeforeReturn: () => Ref.update(fixtureHookCalls, (count) => count + 1),
      };
      let expectedHooks = 0;
      let expectedFixtureHooks = 0;

      for (const mode of modes) {
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(
          sql,
          `INSERT INTO ${markerTables.orchestration} DEFAULT VALUES`,
          mode,
        );
        yield* executeSqlMode(sql, `INSERT INTO ${markerTables.coordinator} DEFAULT VALUES`, mode);
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        expectedHooks += 1;

        const selectId = `insert-select-row-${mode}`;
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(sql, `INSERT INTO ${markerTables.orchestration}(id) SELECT ?`, mode, [
          selectId,
        ]);
        yield* executeSqlMode(sql, `INSERT INTO ${markerTables.coordinator}(id) SELECT ?`, mode, [
          selectId,
        ]);
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        expectedHooks += 1;

        const upsertId = `do-update-${mode}`;
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(sql, `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`, mode, [
          upsertId,
        ]);
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, fixtureHooks),
        );
        expectedFixtureHooks += 1;
        assert.equal(yield* Ref.get(fixtureHookCalls), expectedFixtureHooks, mode);
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(
          sql,
          `INSERT INTO ${markerTables.orchestration}(id) VALUES (?)`,
          mode,
          [upsertId],
        );
        yield* executeSqlMode(
          sql,
          `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)
           ON CONFLICT(id) DO UPDATE SET id = excluded.id`,
          mode,
          [upsertId],
        );
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        expectedHooks += 1;

        const mixedId = `mixed-quoting-${mode}`;
        yield* executeSqlMode(sql, "BEGIN", mode);
        yield* executeSqlMode(
          sql,
          `INSERT\nINTO "MAIN".[${markerTables.orchestration}](id)\nSELECT ?`,
          mode,
          [mixedId],
        );
        yield* executeSqlMode(
          sql,
          `WITH [source]\r\nAS (SELECT ? AS id)\r\nINSERT INTO [MAIN].\`${markerTables.coordinator.toUpperCase()}\`(id)\r\nSELECT id FROM [source]`,
          mode,
          [mixedId],
        );
        yield* executeSqlMode(sql, "COMMIT", mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );
        expectedHooks += 1;

        assert.equal(yield* Ref.get(hookCalls), expectedHooks, mode);
        assert.equal(yield* countRows(sql, markerTables.coordinator, selectId), 1, mode);
        assert.equal(yield* countRows(sql, markerTables.coordinator, upsertId), 1, mode);
        assert.equal(yield* countRows(sql, markerTables.coordinator, mixedId), 1, mode);
      }
    }),
  );

  it.effect("rolls back every marker insert form through every execution mode", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
      let sequence = 0;

      for (const mode of modes) {
        for (const form of markerInsertForms) {
          sequence += 1;
          const rollbackId = `rollback-${sequence}-${mode}-${form.label}`;
          yield* executeSqlMode(sql, "BEGIN", mode);
          yield* executeSqlMode(sql, form.makeSql(markerTables.orchestration), mode, [rollbackId]);
          yield* executeSqlMode(sql, form.makeSql(markerTables.coordinator), mode, [rollbackId]);
          yield* executeSqlMode(sql, "ROLLBACK", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal(yield* countRows(sql, markerTables.coordinator, rollbackId), 0, rollbackId);

          const savepoint = `marker_matrix_${sequence}`;
          const rollbackToId = `rollback-to-${sequence}-${mode}-${form.label}`;
          yield* executeSqlMode(sql, `SAVEPOINT ${savepoint}`, mode);
          yield* executeSqlMode(sql, form.makeSql(markerTables.orchestration), mode, [
            rollbackToId,
          ]);
          yield* executeSqlMode(sql, form.makeSql(markerTables.coordinator), mode, [rollbackToId]);
          yield* executeSqlMode(sql, `ROLLBACK TO SAVEPOINT ${savepoint}`, mode);
          yield* executeSqlMode(sql, `RELEASE SAVEPOINT ${savepoint}`, mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal(
            yield* countRows(sql, markerTables.coordinator, rollbackToId),
            0,
            rollbackToId,
          );
        }
      }
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.deepStrictEqual(yield* sql`SELECT 1 AS usable`, [{ usable: 1 }]);
    }),
  );

  it.effect("reproduces the commented false coordinator marker rollback exactly", () =>
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
            yield* insertOrchestrationMarker(sql, "comment-reproduction");
            yield* sql.unsafe(`
              /* INSERT INTO agent_control_controlled_thread_materialization_accepted */
              ROLLBACK TO SAVEPOINT effect_sql_1
            `);
            yield* sql`INSERT INTO boundary_business_writes(id) VALUES ('independent-outer')`;
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));

      assert.equal(
        yield* countRows(
          sql,
          "orchestration_agent_control_thread_materialization_receipts",
          "comment-reproduction",
        ),
        0,
      );
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "comment-reproduction",
        ),
        0,
      );
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(yield* countRows(sql, "boundary_business_writes", "independent-outer"), 1);

      yield* sql.unsafe(`
        SELECT 'INSERT INTO agent_control_controlled_thread_materialization_accepted'
      `);
      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('next-after-comment-reproduction')`,
      );
      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "next-after-comment-reproduction"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
    }),
  );

  it.effect("preserves native syntax errors without advancing the materialization boundary", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);

      for (const malformed of [
        "/* INSERT INTO agent_control_controlled_thread_materialization_accepted",
        "SELECT 'SAVEPOINT ROLLBACK RELEASE COMMIT",
      ]) {
        yield* sql.unsafe("SAVEPOINT effect_sql_1");
        yield* sql`INSERT INTO boundary_business_writes(id) VALUES (${malformed})`;
        const malformedExit = yield* Effect.exit(sql.unsafe(malformed));
        assert.equal(malformedExit._tag, "Failure");
        if (Exit.isFailure(malformedExit)) {
          assert.notInclude(
            Cause.pretty(malformedExit.cause),
            "potential materialization marker DML",
          );
        }
        yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1");
        assert.equal(yield* countRows(sql, "boundary_business_writes", malformed), 1);
      }

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('usable-after-malformed-sql')`,
      );
      assert.equal(
        yield* countRows(sql, "boundary_business_writes", "usable-after-malformed-sql"),
        1,
      );
    }),
  );

  it.effect("commits an outermost savepoint release once through every execution mode", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];

      for (const [index, mode] of modes.entries()) {
        const id = `outermost-release-${mode}`;
        yield* executeSqlMode(sql, `SAVEPOINT effect_sql_${index + 1}`, mode);
        yield* executeSqlMode(
          sql,
          `INSERT INTO orchestration_agent_control_thread_materialization_receipts(id)
           VALUES ('${id}')`,
          mode,
        );
        yield* executeSqlMode(
          sql,
          `INSERT INTO agent_control_controlled_thread_materialization_accepted(id)
           VALUES ('${id}')`,
          mode,
        );
        yield* executeSqlMode(sql, `RELEASE SAVEPOINT effect_sql_${index + 1}`, mode).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        );

        assert.equal(yield* Ref.get(hookCalls), index + 1);
        assert.equal(
          yield* countRows(sql, "orchestration_agent_control_thread_materialization_receipts", id),
          1,
        );
        assert.equal(
          yield* countRows(sql, "agent_control_controlled_thread_materialization_accepted", id),
          1,
        );
      }

      yield* sql
        .withTransaction(
          sql`INSERT INTO boundary_business_writes(id) VALUES ('normal-after-outermost-release')`,
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), modes.length);

      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "coordinator-after-outermost-release"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), modes.length + 1);
    }),
  );

  it.effect("accepts every native BEGIN mode for complete marker transactions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      const beginModes = [
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN DEFERRED",
        "BEGIN IMMEDIATE",
        "BEGIN EXCLUSIVE",
      ] as const;

      for (const [index, begin] of beginModes.entries()) {
        const id = `native-begin-mode-${index}`;
        yield* sql.unsafe(begin);
        yield* insertOrchestrationMarker(sql, id);
        yield* insertCoordinatorMarker(sql, id);
        yield* sql`COMMIT`.pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.equal(yield* Ref.get(hookCalls), index + 1, begin);
        assert.equal(yield* countRows(sql, markerTables.orchestration, id), 1, begin);
        assert.equal(yield* countRows(sql, markerTables.coordinator, id), 1, begin);
      }
    }),
  );

  it.effect("applies the outermost release negative boundary matrix", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql.unsafe("SAVEPOINT effect_sql_1");
      yield* insertOrchestrationMarker(sql, "release-orchestration-only");
      yield* sql
        .unsafe("RELEASE SAVEPOINT effect_sql_1")
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 0);

      yield* sql.unsafe("SAVEPOINT effect_sql_2");
      yield* insertOrchestrationMarker(sql, "release-rolled-back");
      yield* insertCoordinatorMarker(sql, "release-rolled-back");
      yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_2");
      yield* sql
        .unsafe("RELEASE SAVEPOINT effect_sql_2")
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 0);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "release-rolled-back",
        ),
        0,
      );

      yield* sql.unsafe("SAVEPOINT effect_sql_3");
      yield* insertOrchestrationMarker(sql, "release-before-rewrite");
      yield* insertCoordinatorMarker(sql, "release-before-rewrite");
      yield* sql.unsafe("ROLLBACK TO SAVEPOINT effect_sql_3");
      yield* insertOrchestrationMarker(sql, "release-rewritten");
      yield* insertCoordinatorMarker(sql, "release-rewritten");
      yield* sql
        .unsafe("RELEASE SAVEPOINT effect_sql_3")
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "release-rewritten",
        ),
        1,
      );
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
      const fixtureHookCalls = yield* Ref.make(0);
      yield* sql.withTransaction(insertCoordinatorMarker(sql, "duplicate-marker")).pipe(
        Effect.provideService(NodeSqliteTransactionHooks, {
          afterCommitBeforeReturn: () => Ref.update(fixtureHookCalls, (count) => count + 1),
        }),
      );
      assert.equal(yield* Ref.get(fixtureHookCalls), 1);

      const malformedSavepoint = yield* Effect.exit(sql.unsafe("SAVEPOINT").unprepared);
      assert.equal(malformedSavepoint._tag, "Failure");

      const quotedSavepoint = yield* Effect.exit(
        sql.withTransaction(sql.unsafe('SAVEPOINT "effect_sql_1"')),
      );
      assert.equal(quotedSavepoint._tag, "Success");

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

  it.effect("preserves a native outermost release failure and fails closed", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql`CREATE TABLE boundary_release_parents(id TEXT PRIMARY KEY)`;
      yield* sql`
        CREATE TABLE boundary_release_children(
          id TEXT PRIMARY KEY,
          parent_id TEXT NOT NULL,
          FOREIGN KEY(parent_id) REFERENCES boundary_release_parents(id)
            DEFERRABLE INITIALLY DEFERRED
        )
      `;
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      yield* sql.unsafe("SAVEPOINT effect_sql_1");
      yield* sql`
        INSERT INTO boundary_release_children(id, parent_id)
        VALUES ('release-child', 'missing-parent')
      `;
      yield* insertOrchestrationMarker(sql, "failed-native-release");
      yield* insertCoordinatorMarker(sql, "failed-native-release");
      const releaseFailure = yield* Effect.exit(
        sql
          .unsafe("RELEASE SAVEPOINT effect_sql_1")
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks)),
      );
      assert.equal(releaseFailure._tag, "Failure");
      if (Exit.isFailure(releaseFailure)) {
        assert.include(Cause.pretty(releaseFailure.cause), "FOREIGN KEY constraint failed");
      }
      assert.equal(yield* Ref.get(hookCalls), 0);

      const blockedCommit = yield* Effect.exit(sql.unsafe("COMMIT"));
      assert.equal(blockedCommit._tag, "Failure");
      if (Exit.isFailure(blockedCommit)) {
        assert.include(Cause.pretty(blockedCommit.cause), "materialization boundary is invalid");
      }
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "failed-native-release",
        ),
        0,
      );
      assert.deepStrictEqual(yield* sql`SELECT 1 AS usable`, [{ usable: 1 }]);
    }),
  );

  it.effect("resets state before propagating an outermost release hook defect", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);

      yield* sql.unsafe("SAVEPOINT effect_sql_1");
      yield* insertOrchestrationMarker(sql, "release-hook-defect");
      yield* insertCoordinatorMarker(sql, "release-hook-defect");
      const defect = yield* Effect.exit(
        sql.unsafe("RELEASE SAVEPOINT effect_sql_1").pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: () =>
              Ref.update(hookCalls, (count) => count + 1).pipe(
                Effect.andThen(Effect.die(new Error("release-hook-defect"))),
              ),
          }),
        ),
      );
      assert.equal(defect._tag, "Failure");
      if (Exit.isFailure(defect)) {
        assert.include(Cause.pretty(defect.cause), "release-hook-defect");
      }
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "release-hook-defect",
        ),
        1,
      );

      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };
      yield* sql
        .withTransaction(
          sql`INSERT INTO boundary_business_writes(id) VALUES ('after-release-hook-defect')`,
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);

      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "after-release-hook-defect"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 2);
    }),
  );

  it.effect("releases the semaphore after an interrupted outermost release hook", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const enteredHook = yield* Deferred.make<void>();

      yield* sql.unsafe("SAVEPOINT effect_sql_1");
      yield* insertOrchestrationMarker(sql, "release-hook-interrupt");
      yield* insertCoordinatorMarker(sql, "release-hook-interrupt");
      const release = yield* sql.unsafe("RELEASE SAVEPOINT effect_sql_1").pipe(
        Effect.provideService(NodeSqliteTransactionHooks, {
          afterCommitBeforeReturn: () =>
            Ref.update(hookCalls, (count) => count + 1).pipe(
              Effect.andThen(Deferred.succeed(enteredHook, undefined)),
              Effect.andThen(Effect.never),
            ),
        }),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Deferred.await(enteredHook);
      yield* Fiber.interrupt(release);
      const releaseExit = yield* Fiber.await(release);
      assert.equal(Exit.hasInterrupts(releaseExit), true);
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(
        yield* countRows(
          sql,
          "agent_control_controlled_thread_materialization_accepted",
          "release-hook-interrupt",
        ),
        1,
      );

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('after-release-hook-interrupt')`,
      );
      assert.deepStrictEqual(yield* sql`SELECT 1 AS usable`, [{ usable: 1 }]);
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
  "resolves unqualified marker targets against connection-local TEMP and MAIN schemas",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { sqlA, sqlB } = yield* makeWalClients();
        const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
        const hookCalls = yield* Ref.make(0);
        const hooks = {
          afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
        };

        for (const [modeIndex, mode] of modes.entries()) {
          const shadowName =
            mode === "values" ? markerTables.coordinator.toUpperCase() : markerTables.coordinator;
          yield* executeSqlMode(sqlA, `CREATE TEMP TABLE ${shadowName}(id TEXT PRIMARY KEY)`, mode);
          yield* executeSqlMode(sqlA, "BEGIN", mode);
          const insertFailure = yield* Effect.exit(
            executeSqlMode(sqlA, `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`, mode, [
              `shadow-${mode}`,
            ]),
          );
          assert.equal(insertFailure._tag, "Failure", mode);
          if (Exit.isFailure(insertFailure)) {
            assert.include(Cause.pretty(insertFailure.cause), "temporary schema shadows", mode);
          }
          const commitFailure = yield* Effect.exit(
            executeSqlMode(sqlA, "COMMIT", mode).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, hooks),
            ),
          );
          assert.equal(commitFailure._tag, "Failure", mode);
          assert.deepStrictEqual(
            yield* sqlA.unsafe(
              `SELECT count(*) AS count
                               FROM main.${markerTables.coordinator}
                               WHERE id = ?`,
              [`shadow-${mode}`],
            ),
            [{ count: 0 }],
            mode,
          );
          assert.deepStrictEqual(
            yield* sqlA.unsafe(
              `SELECT count(*) AS count
                               FROM temp.${markerTables.coordinator}
                               WHERE id = ?`,
              [`shadow-${mode}`],
            ),
            [{ count: 0 }],
            mode,
          );
          assert.equal(yield* Ref.get(hookCalls), modeIndex, mode);
          yield* executeSqlMode(sqlA, `DROP TABLE temp.${markerTables.coordinator}`, mode);

          const unshadowedId = `unshadowed-${mode}`;
          yield* executeSqlMode(sqlA, "BEGIN", mode);
          yield* executeSqlMode(
            sqlA,
            `INSERT INTO ${markerTables.coordinator}(id) VALUES (?)`,
            mode,
            [unshadowedId],
          );
          yield* executeSqlMode(sqlA, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          assert.equal(yield* countRows(sqlA, markerTables.coordinator, unshadowedId), 1, mode);
        }

        assert.equal(yield* Ref.get(hookCalls), modes.length);

        yield* sqlA.unsafe(`CREATE TEMP TABLE ${markerTables.coordinator}(id TEXT PRIMARY KEY)`);
        yield* sqlA
          .withTransaction(
            sqlA.unsafe(`INSERT INTO main.${markerTables.coordinator}(id) VALUES (?)`, [
              "explicit-main",
            ]),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.deepStrictEqual(
          yield* sqlA.unsafe(
            `SELECT count(*) AS count
             FROM main.${markerTables.coordinator}
             WHERE id = 'explicit-main'`,
          ),
          [{ count: 1 }],
        );
        assert.deepStrictEqual(
          yield* sqlA.unsafe(`SELECT count(*) AS count FROM temp.${markerTables.coordinator}`),
          [{ count: 0 }],
        );

        yield* sqlA
          .withTransaction(
            sqlA.unsafe(`INSERT INTO temp.${markerTables.coordinator}(id) VALUES (?)`, [
              "explicit-temp",
            ]),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.deepStrictEqual(
          yield* sqlA.unsafe(
            `SELECT id FROM temp.${markerTables.coordinator} WHERE id = 'explicit-temp'`,
          ),
          [{ id: "explicit-temp" }],
        );
        assert.equal(yield* Ref.get(hookCalls), modes.length + 1);
        yield* sqlA.unsafe(`DROP TABLE temp.${markerTables.coordinator}`);

        yield* sqlA.unsafe(
          `CREATE TEMP VIEW ${markerTables.coordinator}
           AS SELECT id FROM main.${markerTables.coordinator}`,
        );
        const viewShadow = yield* Effect.exit(
          sqlA.withTransaction(
            sqlA.unsafe(`INSERT INTO ${markerTables.coordinator}(id) VALUES ('view-shadow')`),
          ),
        );
        assert.equal(viewShadow._tag, "Failure");
        if (Exit.isFailure(viewShadow)) {
          assert.include(Cause.pretty(viewShadow.cause), "temporary schema shadows");
        }
        yield* sqlA.unsafe(`DROP VIEW temp.${markerTables.coordinator}`);

        yield* sqlA.unsafe(
          `CREATE TEMP TABLE ${markerTables.orchestration.toUpperCase()}(id TEXT PRIMARY KEY)`,
        );
        const orchestrationShadow = yield* Effect.exit(
          sqlA.withTransaction(
            sqlA.unsafe(
              `INSERT INTO ${markerTables.orchestration}(id)
               VALUES ('orchestration-shadow')`,
            ),
          ),
        );
        assert.equal(orchestrationShadow._tag, "Failure");
        yield* sqlA.unsafe(`DROP TABLE temp.${markerTables.orchestration}`);

        yield* sqlA.unsafe(`CREATE TEMP TABLE ${markerTables.orchestration}(id TEXT PRIMARY KEY)`);
        yield* sqlA.unsafe(`CREATE TEMP TABLE ${markerTables.coordinator}(id TEXT PRIMARY KEY)`);
        const bothShadowed = yield* Effect.exit(
          sqlA.withTransaction(
            Effect.gen(function* () {
              yield* insertOrchestrationMarker(sqlA, "both-shadowed");
              yield* insertCoordinatorMarker(sqlA, "both-shadowed");
            }),
          ),
        );
        assert.equal(bothShadowed._tag, "Failure");
        assert.deepStrictEqual(
          yield* sqlA.unsafe(
            `SELECT
               (SELECT count(*) FROM temp.${markerTables.orchestration}) AS orchestration,
               (SELECT count(*) FROM temp.${markerTables.coordinator}) AS coordinator`,
          ),
          [{ orchestration: 0, coordinator: 0 }],
        );
        yield* sqlA.unsafe(`DROP TABLE temp.${markerTables.orchestration}`);
        yield* sqlA.unsafe(`DROP TABLE temp.${markerTables.coordinator}`);

        yield* sqlA`ATTACH DATABASE ':memory:' AS attached`;
        yield* sqlA.unsafe(
          `CREATE TABLE attached.${markerTables.coordinator}(id TEXT PRIMARY KEY)`,
        );
        yield* sqlA
          .withTransaction(
            sqlA.unsafe(
              `INSERT INTO attached.${markerTables.coordinator}(id) VALUES ('attached-explicit')`,
            ),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.deepStrictEqual(
          yield* sqlA.unsafe(`SELECT id FROM attached.${markerTables.coordinator}`),
          [{ id: "attached-explicit" }],
        );
        assert.equal(yield* Ref.get(hookCalls), modes.length + 1);

        yield* sqlA.unsafe(`CREATE TEMP TABLE ${markerTables.coordinator}(id TEXT PRIMARY KEY)`);
        yield* sqlB
          .withTransaction(insertCoordinatorMarker(sqlB, "connection-b-unshadowed"))
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
        assert.equal(
          yield* countRows(sqlB, markerTables.coordinator, "connection-b-unshadowed"),
          1,
        );
        const connectionAShadow = yield* Effect.exit(
          sqlA.withTransaction(insertCoordinatorMarker(sqlA, "connection-a-shadowed")),
        );
        assert.equal(connectionAShadow._tag, "Failure");
        yield* sqlA.unsafe(`DROP TABLE temp.${markerTables.coordinator}`);
        assert.deepStrictEqual(yield* sqlA`SELECT 1 AS usable`, [{ usable: 1 }]);
        assert.deepStrictEqual(yield* sqlB`SELECT 1 AS usable`, [{ usable: 1 }]);

        const missingMain = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(missingMain);
        yield* missingMain`ATTACH DATABASE ':memory:' AS attached`;
        yield* missingMain.unsafe(`DROP TABLE main.${markerTables.coordinator}`);
        yield* missingMain.unsafe(
          `CREATE TABLE attached.${markerTables.coordinator}(id TEXT PRIMARY KEY)`,
        );
        const missingMainAutocommit = yield* Effect.exit(
          missingMain.unsafe(
            `INSERT INTO ${markerTables.coordinator}(id) VALUES ('autocommit-missing-main')`,
          ),
        );
        assert.equal(missingMainAutocommit._tag, "Failure");
        if (Exit.isFailure(missingMainAutocommit)) {
          const error = Cause.pretty(missingMainAutocommit.cause);
          assert.include(
            error,
            "persistent materialization marker DML requires an active caller-controlled transaction",
          );
          assert.notInclude(error, "missing or invalid");
        }
        const missingMainFailure = yield* Effect.exit(
          missingMain.withTransaction(
            missingMain.unsafe(
              `INSERT INTO ${markerTables.coordinator}(id) VALUES ('attached-fallback')`,
            ),
          ),
        );
        assert.equal(missingMainFailure._tag, "Failure");
        if (Exit.isFailure(missingMainFailure)) {
          assert.include(Cause.pretty(missingMainFailure.cause), "missing or invalid");
        }
        assert.deepStrictEqual(
          yield* missingMain.unsafe(
            `SELECT count(*) AS count FROM attached.${markerTables.coordinator}`,
          ),
          [{ count: 0 }],
        );

        const mainView = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(mainView);
        yield* mainView.unsafe(`DROP TABLE main.${markerTables.coordinator}`);
        yield* mainView.unsafe(
          `CREATE VIEW main.${markerTables.coordinator} AS SELECT 'view' AS id`,
        );
        const mainViewFailure = yield* Effect.exit(
          mainView.withTransaction(
            mainView.unsafe(`INSERT INTO ${markerTables.coordinator}(id) VALUES ('main-view')`),
          ),
        );
        assert.equal(mainViewFailure._tag, "Failure");
        if (Exit.isFailure(mainViewFailure)) {
          assert.include(Cause.pretty(mainViewFailure.cause), "missing or invalid");
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect("enforces the Verification evaluation Evidence to Receipt to Marker boundary", () =>
  Effect.scoped(
    Effect.gen(function* () {
      for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
        const sql = yield* makeScopedMemoryClient();
        yield* initializeMaterializationBoundaryTables(sql);
        const hookBoundaries = yield* Ref.make<Array<string>>([]);
        const hooks = {
          afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
            Ref.update(hookBoundaries, (entries) => [...entries, boundary]),
        };
        const run = (statement: string, params: ReadonlyArray<unknown> = []) =>
          executeSqlMode(sql, statement, mode, params).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );

        const autocommit = yield* Effect.exit(
          run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
            `autocommit-${mode}`,
          ]),
        );
        assert.equal(autocommit._tag, "Failure", mode);

        yield* run("BEGIN");
        const receiptFirst = yield* Effect.exit(
          run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
            `receipt-first-${mode}`,
          ]),
        );
        assert.equal(receiptFirst._tag, "Failure", mode);
        yield* run("ROLLBACK");

        yield* run("BEGIN");
        const markerFirst = yield* Effect.exit(
          run("INSERT INTO agent_control_verification_evaluation_markers(id) VALUES (?)", [
            `marker-first-${mode}`,
          ]),
        );
        assert.equal(markerFirst._tag, "Failure", mode);
        yield* run("ROLLBACK");

        yield* run("BEGIN");
        yield* run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
          `missing-marker-${mode}`,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
          `missing-marker-${mode}`,
        ]);
        const missingMarker = yield* Effect.exit(run("COMMIT"));
        assert.equal(missingMarker._tag, "Failure", mode);

        const acceptedId = `accepted-${mode}`;
        yield* run("BEGIN");
        yield* run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_markers(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("SELECT 1 AS value");
        const postMarkerDml = yield* Effect.exit(
          run("INSERT INTO boundary_business_writes(id) VALUES (?)", [`after-${acceptedId}`]),
        );
        assert.equal(postMarkerDml._tag, "Failure", mode);
        yield* run("ROLLBACK");

        yield* run("BEGIN");
        yield* run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_markers(id) VALUES (?)", [
          acceptedId,
        ]);
        yield* run("COMMIT");
        assert.deepStrictEqual(yield* Ref.get(hookBoundaries), [
          "agent-control-verification-evaluation",
        ]);

        yield* run("BEGIN");
        yield* run("SAVEPOINT evaluation_rollback");
        yield* run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
          `rolled-back-${mode}`,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
          `rolled-back-${mode}`,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_markers(id) VALUES (?)", [
          `rolled-back-${mode}`,
        ]);
        yield* run("ROLLBACK TO evaluation_rollback");
        yield* run("RELEASE evaluation_rollback");
        yield* run("INSERT INTO boundary_business_writes(id) VALUES (?)", [
          `after-rollback-${mode}`,
        ]);
        yield* run("COMMIT");
        assert.lengthOf(yield* Ref.get(hookBoundaries), 1, mode);

        const savepointId = `savepoint-${mode}`;
        yield* run("BEGIN");
        yield* run("SAVEPOINT evaluation_release");
        yield* run("INSERT INTO agent_control_verification_evaluation_evidence(id) VALUES (?)", [
          savepointId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_receipts(id) VALUES (?)", [
          savepointId,
        ]);
        yield* run("INSERT INTO agent_control_verification_evaluation_markers(id) VALUES (?)", [
          savepointId,
        ]);
        yield* run("RELEASE evaluation_release");
        yield* run("COMMIT");
        assert.lengthOf(yield* Ref.get(hookBoundaries), 2, mode);

        yield* run("BEGIN");
        yield* run(
          "INSERT OR IGNORE INTO agent_control_verification_evaluation_evidence(id) VALUES (?)",
          [acceptedId],
        );
        yield* run(
          "INSERT OR IGNORE INTO agent_control_verification_evaluation_receipts(id) VALUES (?)",
          [acceptedId],
        );
        yield* run(
          "INSERT OR IGNORE INTO agent_control_verification_evaluation_markers(id) VALUES (?)",
          [acceptedId],
        );
        yield* run("COMMIT");
        assert.lengthOf(yield* Ref.get(hookBoundaries), 2, mode);

        yield* run("CREATE TEMP TABLE agent_control_verification_evaluation_evidence(id TEXT)");
        yield* run(
          "INSERT INTO temp.agent_control_verification_evaluation_evidence(id) VALUES (?)",
          [`temp-${mode}`],
        );
        yield* run("ATTACH DATABASE ':memory:' AS evaluation_aux");
        yield* run(
          "CREATE TABLE evaluation_aux.agent_control_verification_evaluation_evidence(id TEXT)",
        );
        yield* run(
          "INSERT INTO evaluation_aux.agent_control_verification_evaluation_evidence(id) VALUES (?)",
          [`attached-${mode}`],
        );
      }
    }),
  ),
);

it.effect("publishes the Verification evaluation hook only after WAL marker visibility", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { sqlA, sqlB } = yield* makeWalClients();
      const visibleAtHook = yield* Ref.make(false);
      const hooks = {
        afterCommitBeforeReturn: ({ boundary }: { readonly boundary: string }) =>
          Effect.gen(function* () {
            assert.equal(boundary, "agent-control-verification-evaluation");
            const rows = yield* sqlB<{ readonly count: number }>`
                SELECT count(*) AS count
                FROM agent_control_verification_evaluation_markers
                WHERE id = 'wal-evaluation'
              `;
            yield* Ref.set(visibleAtHook, rows[0]?.count === 1);
          }).pipe(Effect.orDie),
      };
      yield* sqlA
        .withTransaction(
          Effect.gen(function* () {
            yield* sqlA`
                INSERT INTO agent_control_verification_evaluation_evidence(id)
                VALUES ('wal-evaluation')
              `;
            yield* sqlA`
                INSERT INTO agent_control_verification_evaluation_receipts(id)
                VALUES ('wal-evaluation')
              `;
            yield* sqlA`
                INSERT INTO agent_control_verification_evaluation_markers(id)
                VALUES ('wal-evaluation')
              `;
          }),
        )
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.isTrue(yield* Ref.get(visibleAtHook));
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("rejects autocommit before change-count and preserves transactional faults", () =>
  Effect.scoped(
    Effect.gen(function* () {
      let changeReadAttempts = 0;
      const sql = yield* makeScopedMemoryClient({
        _testHooks: {
          beforeMarkerChanges: () => {
            changeReadAttempts += 1;
            if (changeReadAttempts === 1) {
              throw new Error("test marker changes read failure");
            }
          },
        },
      });
      yield* initializeMaterializationBoundaryTables(sql);
      const hookCalls = yield* Ref.make(0);
      const hooks = {
        afterCommitBeforeReturn: () => Ref.update(hookCalls, (count) => count + 1),
      };

      const autocommitFailure = yield* Effect.exit(
        insertCoordinatorMarker(sql, "autocommit-before-change-read").pipe(
          Effect.provideService(NodeSqliteTransactionHooks, hooks),
        ),
      );
      assert.equal(autocommitFailure._tag, "Failure");
      if (Exit.isFailure(autocommitFailure)) {
        const error = Cause.pretty(autocommitFailure.cause);
        assert.include(
          error,
          "persistent materialization marker DML requires an active caller-controlled transaction",
        );
        assert.notInclude(error, "test marker changes read failure");
      }
      assert.equal(changeReadAttempts, 0);
      assert.equal(
        yield* countRows(sql, markerTables.coordinator, "autocommit-before-change-read"),
        0,
      );
      assert.equal(yield* Ref.get(hookCalls), 0);

      const failed = yield* Effect.exit(
        sql
          .withTransaction(insertCoordinatorMarker(sql, "failed-change-read"))
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks)),
      );
      assert.equal(failed._tag, "Failure");
      if (Exit.isFailure(failed)) {
        assert.include(Cause.pretty(failed.cause), "test marker changes read failure");
      }
      assert.equal(changeReadAttempts, 1);
      assert.equal(yield* countRows(sql, markerTables.coordinator, "failed-change-read"), 0);
      assert.equal(yield* Ref.get(hookCalls), 0);

      yield* sql.withTransaction(
        sql`INSERT INTO boundary_business_writes(id) VALUES ('after-change-read-failure')`,
      );
      yield* sql
        .withTransaction(insertCoordinatorMarker(sql, "after-change-read-failure"))
        .pipe(Effect.provideService(NodeSqliteTransactionHooks, hooks));
      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(yield* countRows(sql, markerTables.coordinator, "after-change-read-failure"), 1);
    }),
  ),
);

it.effect(
  "publishes the Initial Planning final marker hook only after WAL commit visibility",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { sqlA, sqlB } = yield* makeWalClients();
        const observations = yield* Ref.make<ReadonlyArray<string>>([]);
        let expectedId = "";
        const hooks = {
          afterCommitBeforeReturn: (observation: { readonly boundary: string }) =>
            Effect.gen(function* () {
              assert.equal(
                observation.boundary,
                "agent-control-initial-planning-stage-finalization",
              );
              const [marker] = yield* sqlB<{ readonly id: string }>`
                SELECT id FROM agent_control_initial_planning_finalization_markers
                WHERE id = ${expectedId}
              `;
              assert.isDefined(marker);
              yield* Ref.update(observations, (current) => [...current, marker!.id]);
            }).pipe(Effect.orDie),
        };
        for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
          const id = `wal-finalization-${mode}`;
          expectedId = id;
          yield* executeSqlMode(sqlA, "BEGIN IMMEDIATE", mode);
          for (const table of initialPlanningFinalizationTables) {
            yield* executeSqlMode(sqlA, `INSERT INTO ${table}(id) VALUES (?)`, mode, [id]);
          }
          yield* executeSqlMode(sqlA, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          for (const table of initialPlanningFinalizationTables) {
            assert.deepStrictEqual(
              yield* sqlB.unsafe(`SELECT id FROM ${table} WHERE id = ?`, [id]),
              [{ id }],
            );
          }
        }
        assert.equal((yield* Ref.get(observations)).length, 4);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect(
  "publishes the Implementation final marker hook only after WAL commit visibility",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { sqlA, sqlB } = yield* makeWalClients();
        const observations = yield* Ref.make<ReadonlyArray<string>>([]);
        let expectedId = "";
        const hooks = {
          afterCommitBeforeReturn: (observation: { readonly boundary: string }) =>
            Effect.gen(function* () {
              assert.equal(observation.boundary, "agent-control-implementation-stage-finalization");
              const [marker] = yield* sqlB<{ readonly id: string }>`
                SELECT id FROM agent_control_implementation_stage_finalization_markers
                WHERE id = ${expectedId}
              `;
              assert.isDefined(marker);
              yield* Ref.update(observations, (current) => [...current, marker!.id]);
            }).pipe(Effect.orDie),
        };

        for (const mode of ["statement", "values", "raw", "unprepared"] as const) {
          const id = `wal-implementation-finalization-${mode}`;
          expectedId = id;
          yield* executeSqlMode(sqlA, "BEGIN IMMEDIATE", mode);
          yield* insertCompanionChain(sqlA, mode, implementationStageFinalizationTables, id);
          yield* executeSqlMode(sqlA, "COMMIT", mode).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, hooks),
          );
          for (const table of implementationStageFinalizationTables) {
            assert.deepStrictEqual(
              yield* sqlB.unsafe(`SELECT id FROM ${table} WHERE id = ?`, [id]),
              [{ id }],
            );
          }
        }
        assert.equal((yield* Ref.get(observations)).length, 4);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);

it.effect("runs the outermost release hook after WAL commit and before return", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const { sqlA, sqlB } = yield* makeWalClients();
      const hookCalls = yield* Ref.make(0);
      const committedRowsObservedInHook = yield* Ref.make(0);

      yield* sqlA.unsafe("SAVEPOINT effect_sql_1");
      yield* insertOrchestrationMarker(sqlA, "wal-outermost-release");
      yield* insertCoordinatorMarker(sqlA, "wal-outermost-release");
      yield* sqlA.unsafe("RELEASE SAVEPOINT effect_sql_1").pipe(
        Effect.provideService(NodeSqliteTransactionHooks, {
          afterCommitBeforeReturn: () =>
            Effect.gen(function* () {
              yield* Ref.update(hookCalls, (count) => count + 1);
              const count = yield* countRows(
                sqlB,
                "agent_control_controlled_thread_materialization_accepted",
                "wal-outermost-release",
              );
              yield* Ref.set(committedRowsObservedInHook, count);
            }).pipe(Effect.orDie),
        }),
      );

      assert.equal(yield* Ref.get(hookCalls), 1);
      assert.equal(yield* Ref.get(committedRowsObservedInHook), 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "recognizes every marker insert form across execution and commit modes with WAL visibility",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const { sqlA, sqlB } = yield* makeWalClients();
        const modes: ReadonlyArray<SqlExecutionMode> = ["statement", "values", "raw", "unprepared"];
        const commitModes = [
          { label: "commit", begin: "BEGIN", finish: "COMMIT" },
          {
            label: "commit-transaction",
            begin: "BEGIN TRANSACTION",
            finish: "COMMIT TRANSACTION",
          },
          { label: "release", begin: "", finish: "" },
        ] as const;
        const hookCalls = yield* Ref.make(0);
        const walObservations = yield* Ref.make(0);
        let expectedHookCalls = 0;
        let sequence = 0;

        for (const mode of modes) {
          for (const commitMode of commitModes) {
            for (const form of markerInsertForms) {
              sequence += 1;
              const id = `positive-${sequence}-${mode}-${commitMode.label}-${form.label}`;
              const savepoint = `positive_matrix_${sequence}`;
              const begin =
                commitMode.label === "release" ? `SAVEPOINT ${savepoint}` : commitMode.begin;
              const finish =
                commitMode.label === "release"
                  ? `RELEASE SAVEPOINT ${savepoint}`
                  : commitMode.finish;
              yield* executeSqlMode(sqlA, begin, mode);
              yield* executeSqlMode(sqlA, form.makeSql(markerTables.orchestration), mode, [id]);
              yield* executeSqlMode(sqlA, form.makeSql(markerTables.coordinator), mode, [id]);
              yield* executeSqlMode(sqlA, finish, mode).pipe(
                Effect.provideService(NodeSqliteTransactionHooks, {
                  afterCommitBeforeReturn: () =>
                    Effect.gen(function* () {
                      assert.equal(yield* countRows(sqlB, markerTables.orchestration, id), 1, id);
                      assert.equal(yield* countRows(sqlB, markerTables.coordinator, id), 1, id);
                      yield* Ref.update(hookCalls, (count) => count + 1);
                      yield* Ref.update(walObservations, (count) => count + 1);
                    }).pipe(Effect.orDie),
                }),
              );
              expectedHookCalls += 1;
              assert.equal(yield* Ref.get(hookCalls), expectedHookCalls, id);
            }
          }
        }

        assert.equal(yield* Ref.get(walObservations), markerInsertForms.length * 3 * modes.length);
        yield* sqlA.withTransaction(
          sqlA`INSERT INTO boundary_business_writes(id) VALUES ('after-positive-matrix')`,
        );
        assert.equal(yield* Ref.get(hookCalls), expectedHookCalls);
        assert.deepStrictEqual(yield* sqlA`SELECT 1 AS usable`, [{ usable: 1 }]);
        assert.deepStrictEqual(yield* sqlB`SELECT 1 AS usable`, [{ usable: 1 }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  60_000,
);

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
