import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
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
import {
  makeMigration059,
  type Migration059FaultPoint,
} from "./059_AgentControlVerificationTurnTerminalObservation.ts";

it.live("installs the Verification terminal-delivery CAS boundary atomically", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-terminal-migration-",
      });
      const filename = path.join(directory, "state.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(context, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 58 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      const beforeSchema = yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM sqlite_schema ORDER BY type, name
        `;
      for (const faultPoint of [
        "before-copy",
        "after-copy",
        "after-install",
      ] satisfies ReadonlyArray<Migration059FaultPoint>) {
        const rollback = yield* Effect.exit(
          sql.withTransaction(
            makeMigration059(faultPoint).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback), faultPoint);
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM sqlite_schema ORDER BY type, name
            `,
          beforeSchema,
          faultPoint,
        );
        assert.deepStrictEqual(
          yield* sql<{ readonly count: number }>`
              SELECT count(*) AS count FROM sqlite_schema
              WHERE name = 'agent_control_verification_deliveries_rebuild_059'
                OR name IN (
                  'idx_agent_control_verification_delivery_terminal_event',
                  'idx_agent_control_verification_delivery_terminal_recovery'
                )
            `,
          [{ count: 0 }],
          faultPoint,
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], faultPoint);
        assert.deepStrictEqual(
          yield* sql`PRAGMA integrity_check`,
          [{ integrity_check: "ok" }],
          faultPoint,
        );
      }

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[59, "AgentControlVerificationTurnTerminalObservation"] as const],
      );
      const columns = yield* sql<{ readonly name: string }>`
          SELECT name FROM pragma_table_info('agent_control_verification_deliveries')
          WHERE name LIKE 'terminal_%' ORDER BY name
        `;
      assert.deepStrictEqual(columns, [
        { name: "terminal_at" },
        { name: "terminal_event_id" },
        { name: "terminal_event_type" },
        { name: "terminal_observation_digest" },
        { name: "terminal_provider_state" },
      ]);
      const [table, transition, stageGuard] = yield* Effect.all([
        sql<{ readonly sql: string }>`
            SELECT sql FROM sqlite_schema
            WHERE type='table' AND name='agent_control_verification_deliveries'
          `,
        sql<{ readonly sql: string }>`
            SELECT sql FROM sqlite_schema
            WHERE type='trigger'
              AND name='agent_control_verification_delivery_transition_validate'
          `,
        sql<{ readonly sql: string }>`
            SELECT sql FROM sqlite_schema
            WHERE type='trigger'
              AND name='agent_control_verification_stage_event_validate'
          `,
      ]);
      assert.include(table[0]!.sql, "'completed', 'failed', 'interrupted'");
      assert.include(transition[0]!.sql, "OLD.state = 'provider-started'");
      assert.include(
        stageGuard[0]!.sql,
        "delivery.state IN ('provider-started', 'completed', 'failed', 'interrupted')",
      );

      // This separate post-migration constraint probe deliberately creates isolated rows.
      // The valid 058 preservation fixture above never disables its foreign keys or triggers.
      const native = yield* Effect.acquireRelease(
        Effect.sync(() => {
          const database = new NodeSqlite.DatabaseSync(filename);
          database.exec(
            "PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA foreign_keys = OFF",
          );
          return database;
        }),
        (database) => Effect.sync(() => database.close()),
      );
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`DROP TRIGGER agent_control_verification_delivery_insert_validate`;
      const acceptedAt = "2026-08-10T10:00:00.000Z";
      const terminalAt = "2026-08-10T10:00:01.000Z";
      const insertProviderStartedStatement = native.prepare(`
          INSERT INTO agent_control_verification_deliveries (
            provider_delivery_id, handoff_id, handoff_fingerprint, admission_marker_id,
            materialization_evidence_id, controlled_thread_reservation_id, thread_id,
            stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
            provider_instance_id, runtime_mode, model_selection_fingerprint,
            turn_request_command_id, message_id, planning_thread_id, plan_id,
            state, revision, claim_owner_id, claim_generation, claim_expires_at,
            attempt_count, next_attempt_at, provider_turn_id, provider_accepted_at,
            provider_session_created_at, provider_resume_cursor_json, terminal_at,
            terminal_event_id, terminal_event_type, terminal_provider_state,
            terminal_observation_digest, last_error_code, interrupt_requested, updated_at
          ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 3,
            'codex-main', 'approval-required', ?, ?, ?, ?, ?,
            'provider-started', 5, NULL, 1, NULL, 1, NULL, ?, ?, ?, '{}',
            NULL, NULL, NULL, NULL, NULL, NULL, 0, ?
          )
        `);
      let insertOrdinal = 0;
      const insertProviderStarted = (suffix: string) =>
        Effect.sync(() => {
          insertOrdinal += 1;
          return insertProviderStartedStatement.run(
            `delivery-${suffix}`,
            `handoff-${suffix}`,
            insertOrdinal.toString(16).padStart(64, "0"),
            `admission-${suffix}`,
            `materialization-${suffix}`,
            `reservation-${suffix}`,
            `thread-${suffix}`,
            `stage-${suffix}`,
            `attempt-${suffix}`,
            `lease-${suffix}`,
            `holder-${suffix}`,
            "b".repeat(64),
            `turn-command-${suffix}`,
            `message-${suffix}`,
            `planning-thread-${suffix}`,
            `plan-${suffix}`,
            `provider-turn-${suffix}`,
            acceptedAt,
            acceptedAt,
            acceptedAt,
          );
        });
      yield* insertProviderStarted("completed");
      yield* Effect.sync(() =>
        native
          .prepare(`
              UPDATE agent_control_verification_deliveries
              SET state='completed', revision=revision+1, terminal_at=?,
                terminal_event_id='runtime-terminal-completed',
                terminal_event_type='turn.completed', terminal_provider_state='completed',
                terminal_observation_digest=?, last_error_code=NULL, updated_at=?
              WHERE provider_delivery_id='delivery-completed'
            `)
          .run(terminalAt, "c".repeat(64), terminalAt),
      );
      const [completed] = yield* sql<{
        readonly state: string;
        readonly revision: number;
        readonly terminalEventId: string;
      }>`
          SELECT state, revision, terminal_event_id AS "terminalEventId"
          FROM agent_control_verification_deliveries
          WHERE provider_delivery_id='delivery-completed'
        `;
      assert.deepStrictEqual(completed, {
        state: "completed",
        revision: 6,
        terminalEventId: "runtime-terminal-completed",
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(sql`
              UPDATE agent_control_verification_deliveries
              SET revision=revision+1, updated_at=${terminalAt}
              WHERE provider_delivery_id='delivery-completed'
            `),
          ),
        ),
      );

      yield* insertProviderStarted("conflict");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(sql`
              UPDATE agent_control_verification_deliveries
              SET state='interrupted', revision=revision+1, terminal_at=${terminalAt},
                terminal_event_id='runtime-terminal-invalid-map',
                terminal_event_type='turn.completed', terminal_provider_state='failed',
                terminal_observation_digest=${"d".repeat(64)},
                last_error_code='provider-turn-interrupted', updated_at=${terminalAt}
              WHERE provider_delivery_id='delivery-conflict'
            `),
          ),
        ),
      );
      for (const invalidEventIdExpression of [
        "CAST(X'80' AS TEXT)",
        "X'61'",
        "lower(hex(zeroblob(513)))",
      ]) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql.withTransaction(
                sql.unsafe(
                  `UPDATE agent_control_verification_deliveries
                     SET state='completed', revision=revision+1, terminal_at=?,
                       terminal_event_id=${invalidEventIdExpression},
                       terminal_event_type='turn.completed',
                       terminal_provider_state='completed', terminal_observation_digest=?,
                       last_error_code=NULL, updated_at=?
                     WHERE provider_delivery_id='delivery-conflict'`,
                  [terminalAt, "d".repeat(64), terminalAt],
                ),
              ),
            ),
          ),
        );
      }
      const earlyTerminalAt = "2026-08-10T09:59:59.999Z";
      const locallyObservedAt = "2026-08-10T10:00:02.000Z";
      yield* sql.withTransaction(sql`
        UPDATE agent_control_verification_deliveries
        SET state='completed', revision=revision+1,
          terminal_at=${earlyTerminalAt},
          terminal_event_id='runtime-terminal-before-acceptance',
          terminal_event_type='turn.completed', terminal_provider_state='completed',
          terminal_observation_digest=${"d".repeat(64)}, last_error_code=NULL,
          updated_at=${locallyObservedAt}
        WHERE provider_delivery_id='delivery-conflict'
      `);
      assert.deepStrictEqual(
        yield* sql`
          SELECT terminal_at AS "terminalAt", updated_at AS "updatedAt"
          FROM agent_control_verification_deliveries
          WHERE provider_delivery_id='delivery-conflict'
        `,
        [{ terminalAt: earlyTerminalAt, updatedAt: locallyObservedAt }],
      );
      yield* insertProviderStarted("invalid-time");
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(sql`
              UPDATE agent_control_verification_deliveries
              SET state='completed', revision=revision+1,
                terminal_at='2026-08-10T09:59:59Z',
                terminal_event_id='runtime-terminal-invalid-time',
                terminal_event_type='turn.completed', terminal_provider_state='completed',
                terminal_observation_digest=${"f".repeat(64)}, last_error_code=NULL,
                updated_at=${locallyObservedAt}
              WHERE provider_delivery_id='delivery-invalid-time'
            `),
          ),
        ),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.withTransaction(sql`
              UPDATE agent_control_verification_deliveries
              SET state='failed', revision=revision+1, terminal_at=${terminalAt},
                terminal_event_id='runtime-terminal-completed',
                terminal_event_type='turn.aborted', terminal_provider_state=NULL,
                terminal_observation_digest=${"d".repeat(64)},
                last_error_code='provider-turn-aborted', updated_at=${terminalAt}
              WHERE provider_delivery_id='delivery-conflict'
            `),
          ),
        ),
      );
      assert.deepStrictEqual(
        yield* sql`
            SELECT state, revision, terminal_event_id AS "terminalEventId"
            FROM agent_control_verification_deliveries
            WHERE provider_delivery_id='delivery-invalid-time'
          `,
        [{ state: "provider-started", revision: 5, terminalEventId: null }],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
