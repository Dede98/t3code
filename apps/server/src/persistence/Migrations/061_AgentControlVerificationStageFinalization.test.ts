import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { canonicalJson, type JsonValue } from "../../agentControl/initialPlanning/eventEvidence.ts";
import { VERIFICATION_STAGE_FINALIZATION_CANDIDATES_SQL } from "../../agentControl/verificationTurn/Layers/AgentControlVerificationHandoffStore.ts";
import {
  makeMigration061,
  type Migration061FaultPoint,
} from "./061_AgentControlVerificationStageFinalization.ts";

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

it.live("installs Verification finalization atomically on fresh and populated WAL databases", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-finalization-migration-",
      });
      const upgrade = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      const observer = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(upgrade.scope, Exit.void));

      yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
        Effect.provideService(SqlClient.SqlClient, upgrade.sql),
      );
      yield* upgrade.sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'migration-061-project', 'Migration 061', '/tmp/migration-061', NULL,
          '[]', '2026-08-29T00:00:00.000Z', '2026-08-29T00:00:00.000Z', NULL
        )
      `;
      const schemaBefore = yield* upgrade.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
      const sequenceBefore = yield* upgrade.sql<Record<string, unknown>>`
        SELECT name, seq FROM main.sqlite_sequence ORDER BY name
      `;

      for (const faultPoint of [
        "before-events-rebuild",
        "after-events-rebuild",
        "after-companions",
        "after-install",
      ] satisfies ReadonlyArray<Migration061FaultPoint>) {
        const rollback = yield* Effect.exit(
          upgrade.sql.withTransaction(
            makeMigration061(faultPoint).pipe(
              Effect.provideService(SqlClient.SqlClient, upgrade.sql),
            ),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback), faultPoint);
        assert.deepStrictEqual(
          yield* upgrade.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          faultPoint,
        );
        assert.deepStrictEqual(
          yield* upgrade.sql<Record<string, unknown>>`
            SELECT name, seq FROM main.sqlite_sequence ORDER BY name
          `,
          sequenceBefore,
          faultPoint,
        );
      }

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, upgrade.sql),
        ),
        [[61, "AgentControlVerificationStageFinalization"] as const],
      );
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql`
          SELECT title FROM projection_projects WHERE project_id = 'migration-061-project'
        `,
        [{ title: "Migration 061" }],
      );
      assert.deepStrictEqual(
        yield* observer.sql<{ readonly name: string }>`
          SELECT name FROM main.sqlite_schema
          WHERE type = 'table' AND name LIKE 'agent_control_verification_finalization_%'
          ORDER BY name
        `,
        [
          { name: "agent_control_verification_finalization_evidence" },
          { name: "agent_control_verification_finalization_markers" },
          { name: "agent_control_verification_finalization_receipts" },
        ],
      );
      const eventSchema = yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table' AND name = 'agent_control_events'
      `;
      assert.equal(eventSchema.length, 1);
      for (const eventType of [
        "agentControl.stageRun.verificationSucceeded",
        "agentControl.stageRun.verificationFailed",
        "agentControl.stageRun.verificationCancelled",
        "agentControl.stageRunLease.releasedAfterVerification",
      ]) {
        assert.include(eventSchema[0]!.sql, eventType);
      }
      assert.isAbove(
        (yield* observer.sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.sqlite_schema
            WHERE type = 'trigger' AND name LIKE 'agent_control_verification_finalization_%'
          `)[0]!.count,
        8,
      );
      const verificationGuards = Object.fromEntries(
        (yield* observer.sql<{ readonly name: string; readonly sql: string }>`
            SELECT name, sql FROM main.sqlite_schema
            WHERE type = 'trigger' AND name IN (
              'agent_control_verification_stage_event_validate',
              'agent_control_verification_stage_projection_update_validate',
              'agent_control_verification_lease_event_validate',
              'agent_control_verification_lease_projection_update_validate',
              'agent_control_verification_terminal_stage_event_validate',
              'agent_control_verification_lease_release_event_validate',
              'agent_control_verification_terminal_stage_projection_validate',
              'agent_control_verification_lease_release_projection_validate'
            )
            ORDER BY name
          `).map((row) => [row.name, row.sql] as const),
      );
      assert.lengthOf(Object.keys(verificationGuards), 8);
      assert.include(
        verificationGuards.agent_control_verification_stage_event_validate,
        "NEW.event_type NOT IN",
      );
      assert.include(
        verificationGuards.agent_control_verification_stage_projection_update_validate,
        "OLD.status = 'running' AND OLD.revision = 2",
      );
      assert.include(
        verificationGuards.agent_control_verification_lease_event_validate,
        "NEW.event_type <> 'agentControl.stageRunLease.releasedAfterVerification'",
      );
      assert.include(
        verificationGuards.agent_control_verification_lease_projection_update_validate,
        "OLD.status = 'reserved' AND NEW.status = 'released'",
      );
      assert.include(
        verificationGuards.agent_control_verification_terminal_stage_event_validate,
        "t3_verification_stage_terminal_storage",
      );
      assert.include(
        verificationGuards.agent_control_verification_lease_release_event_validate,
        "t3_verification_terminal_payload_pair_match",
      );
      assert.include(
        verificationGuards.agent_control_verification_terminal_stage_projection_validate,
        "t3_verification_stage_projection_match",
      );
      assert.include(
        verificationGuards.agent_control_verification_lease_release_projection_validate,
        "t3_verification_lease_projection_match",
      );
      assert.deepStrictEqual(yield* observer.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* observer.sql`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
      const plan = yield* observer.sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${VERIFICATION_STAGE_FINALIZATION_CANDIDATES_SQL}`,
        ["", 64],
      );
      assert.isFalse(plan.some(({ detail }) => detail.includes("USE TEMP B-TREE")));
      assert.isTrue(
        plan.some(({ detail }) =>
          detail.includes("idx_agent_control_verification_finalization_candidates"),
        ),
      );
      assert.isTrue(
        plan.some(
          ({ detail }) =>
            detail.includes("agent_control_verification_finalization_markers") &&
            detail.includes("INDEX"),
        ),
      );

      const fresh = yield* openDatabase(path.join(directory, "fresh.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(fresh.scope, Exit.void));
      assert.equal(
        (yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, fresh.sql),
        )).at(-1)?.[0],
        61,
      );
      assert.deepStrictEqual(yield* fresh.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* fresh.sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails atomically on a partial marker schema and succeeds after an explicit repair", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-finalization-partial-schema-",
      });
      const filename = path.join(directory, "partial.sqlite");
      const database = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
      );
      yield* database.sql`
        CREATE TABLE main.agent_control_verification_finalization_markers(
          marker_id TEXT PRIMARY KEY
        )
      `;

      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 61 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sql),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(
        yield* database.sql`
          SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id = 61
        `,
        [],
      );
      assert.deepStrictEqual(
        yield* database.sql<{ readonly name: string }>`
          SELECT name FROM main.sqlite_schema
          WHERE type = 'table' AND name LIKE 'agent_control_verification_finalization_%'
          ORDER BY name
        `,
        [{ name: "agent_control_verification_finalization_markers" }],
      );
      assert.deepStrictEqual(
        yield* database.sql<{ readonly name: string }>`
          SELECT name FROM pragma_table_info('agent_control_verification_finalization_markers')
        `,
        [{ name: "marker_id" }],
      );

      yield* database.sql`DROP TABLE main.agent_control_verification_finalization_markers`;
      yield* Scope.close(database.scope, Exit.void);
      const repaired = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(repaired.scope, Exit.void));
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, repaired.sql),
        ),
        [[61, "AgentControlVerificationStageFinalization"] as const],
      );
      assert.deepStrictEqual(yield* repaired.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* repaired.sql`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects every semantically altered schema-60 legacy guard before any migration DDL", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "migration-061-guard-audit-" });
      const database = yield* openDatabase(path.join(directory, "guards.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
      );
      const guards = yield* database.sql<{
        readonly name: string;
        readonly sql: string;
      }>`
        SELECT name, sql FROM main.sqlite_schema
        WHERE type = 'trigger' AND name IN (
          'agent_control_verification_stage_event_validate',
          'agent_control_verification_stage_projection_update_validate',
          'agent_control_verification_lease_event_validate',
          'agent_control_verification_lease_projection_update_validate'
        ) ORDER BY name
      `;
      assert.lengthOf(guards, 4);
      for (const guard of guards) {
        const corrupted = guard.sql.replace("BEGIN SELECT RAISE", "BEGIN\n      SELECT RAISE");
        assert.notEqual(corrupted, guard.sql, guard.name);
        yield* database.sql.unsafe(`DROP TRIGGER main."${guard.name}"`).unprepared;
        yield* database.sql.unsafe(corrupted).unprepared;
        const schemaBefore = yield* database.sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `;
        const attempt = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 61 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sql),
          ),
        );
        assert.isTrue(Exit.isFailure(attempt), guard.name);
        assert.deepStrictEqual(
          yield* database.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          guard.name,
        );
        assert.deepStrictEqual(
          yield* database.sql`
            SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id = 61
          `,
          [],
          guard.name,
        );
        yield* database.sql.unsafe(`DROP TRIGGER main."${guard.name}"`).unprepared;
        yield* database.sql.unsafe(guard.sql).unprepared;
      }
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sql),
        ),
        [[61, "AgentControlVerificationStageFinalization"] as const],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "rejects a reserved Verification lease release that switches to a bogus stage identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "migration-061-lease-identity-",
        });
        const database = yield* openDatabase(path.join(directory, "lease.sqlite"));
        yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sql),
        );
        const insertGuards = yield* database.sql<{ readonly name: string; readonly sql: string }>`
        SELECT name, sql FROM main.sqlite_schema
        WHERE type = 'trigger' AND sql LIKE '%BEFORE INSERT%'
          AND tbl_name IN ('agent_control_stage_run_states', 'agent_control_stage_run_lease_states')
        ORDER BY name
      `;
        for (const guard of insertGuards) {
          yield* database.sql.unsafe(`DROP TRIGGER main."${guard.name}"`).unprepared;
        }
        const sourceFingerprint = "a".repeat(64);
        const acquiredAt = "2026-08-29T08:00:00.000Z";
        const renewedAt = "2026-08-29T09:00:00.000Z";
        const expiresAt = "2026-08-29T11:00:00.000Z";
        const stageState = canonicalJson({
          schemaVersion: 1,
          projectId: "project-identity",
          taskId: "task-identity",
          stageRunId: "verification-stage-old",
          attemptId: "attempt-identity",
          roleId: "verifier",
          stageKind: "verification",
          stageOrdinal: 3,
          attemptOrdinal: 1,
          status: "running",
          taskRevision: 1,
          githubIntakeSequence: 1,
          sourceIdentityFingerprint: sourceFingerprint,
          createdAt: acquiredAt,
          updatedAt: renewedAt,
          revision: 2,
          sequence: 2,
        } as JsonValue);
        const leaseState = canonicalJson({
          schemaVersion: 1,
          leaseId: "lease-identity",
          projectId: "project-identity",
          taskId: "task-identity",
          stageRunId: "verification-stage-old",
          attemptId: "attempt-identity",
          taskRevision: 1,
          githubIntakeSequence: 1,
          sourceIdentityFingerprint: sourceFingerprint,
          holderId: "holder-identity",
          fenceToken: 3,
          status: "reserved",
          acquiredAt,
          renewedAt,
          expiresAt,
          releasedAt: null,
          revision: 5,
          sequence: 7,
        } as JsonValue);
        yield* database.sql`
        INSERT INTO main.agent_control_stage_run_states VALUES (
          'verification-stage-old', 'project-identity', 'task-identity', 'attempt-identity',
          'verifier', 'verification', 3, 1, 'running', 1, 1, ${sourceFingerprint},
          ${stageState}, ${acquiredAt}, ${renewedAt}, 2, 2
        )
      `;
        yield* database.sql`
        INSERT INTO main.agent_control_stage_run_lease_states VALUES (
          'lease-identity', 'project-identity', 'task-identity', 'verification-stage-old',
          'attempt-identity', 1, 1, ${sourceFingerprint}, 'holder-identity', 3, 'reserved',
          ${acquiredAt}, ${renewedAt}, ${expiresAt}, NULL, ${leaseState}, 5, 7
        )
      `;
        for (const guard of insertGuards) yield* database.sql.unsafe(guard.sql).unprepared;

        const before = yield* database.sql<Record<string, unknown>>`
        SELECT * FROM main.agent_control_stage_run_lease_states
      `;
        const mutation = yield* Effect.exit(database.sql`
        UPDATE main.agent_control_stage_run_lease_states
        SET stage_run_id = 'bogus-nonverification-stage', status = 'released',
          released_at = '2026-08-29T10:00:00.000Z', revision = 6,
          last_event_sequence = 8
        WHERE lease_id = 'lease-identity'
      `);
        assert.isTrue(Exit.isFailure(mutation));
        assert.deepStrictEqual(
          yield* database.sql<Record<string, unknown>>`
          SELECT * FROM main.agent_control_stage_run_lease_states
        `,
          before,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "fails UDF preflight before DDL for always-zero and stale functions, then retries cleanly",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "migration-061-udf-preflight-",
        });
        for (const mode of ["always-zero", "stale-always-one"] as const) {
          const filename = path.join(directory, `${mode}.sqlite`);
          const badScope = yield* Scope.make("sequential");
          const badConfig: NodeSqliteClient.SqliteClientConfig & {
            readonly _testHooks: {
              readonly registerFunctions: (native: NodeSqlite.DatabaseSync) => void;
            };
          } = {
            filename,
            _testHooks: {
              registerFunctions: (native: NodeSqlite.DatabaseSync) => {
                NodeSqliteClient.registerNodeSqliteFunctions(native);
                if (mode === "always-zero") {
                  native.function(
                    NodeSqliteClient.NODE_SQLITE_VERIFICATION_STAGE_TERMINAL_STORAGE_FUNCTION,
                    { deterministic: true },
                    (_type: unknown, _payload: unknown, _metadata: unknown) => 0,
                  );
                } else {
                  native.function(
                    NodeSqliteClient.NODE_SQLITE_VERIFICATION_FINALIZATION_DOCUMENT_STORAGE_FUNCTION,
                    { deterministic: true },
                    (_document: unknown) => 1,
                  );
                }
              },
            },
          };
          const badContext = yield* Layer.buildWithScope(
            NodeSqliteClient.layer(badConfig),
            badScope,
          );
          const badSql = Context.get(badContext, SqlClient.SqlClient);
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, badSql),
          );
          const schemaBefore = yield* badSql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                runMigrations({ toMigrationInclusive: 61 }).pipe(
                  Effect.provideService(SqlClient.SqlClient, badSql),
                ),
              ),
            ),
            mode,
          );
          assert.deepStrictEqual(
            yield* badSql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
            schemaBefore,
            mode,
          );
          yield* Scope.close(badScope, Exit.void);

          const retry = yield* openDatabase(filename);
          yield* Effect.addFinalizer(() => Scope.close(retry.scope, Exit.void));
          assert.deepStrictEqual(
            yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
              Effect.provideService(SqlClient.SqlClient, retry.sql),
            ),
            [[61, "AgentControlVerificationStageFinalization"] as const],
            mode,
          );
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
