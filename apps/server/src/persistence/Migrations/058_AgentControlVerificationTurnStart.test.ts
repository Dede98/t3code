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
import Migration058 from "./058_AgentControlVerificationTurnStart.ts";

it.live(
  "installs the durable verification materialization, delivery, and stage-start boundary atomically",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-verification-turn-migration-",
        });
        const filename = path.join(directory, "state.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const scopeB = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
        const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
        const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
        const sqlA = Context.get(contextA, SqlClient.SqlClient);
        const sqlB = Context.get(contextB, SqlClient.SqlClient);

        for (const sql of [sqlA, sqlB]) {
          assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
          yield* sql`PRAGMA foreign_keys = ON`;
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        }

        yield* runMigrations({ toMigrationInclusive: 57 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        const beforeSchema = yield* sqlA<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM sqlite_schema ORDER BY type, name
        `;
        const beforeSequence = yield* sqlA<Record<string, unknown>>`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `;
        const beforeMigrations = yield* sqlA<Record<string, unknown>>`
          SELECT * FROM effect_sql_migrations ORDER BY migration_id
        `;
        const rollback = yield* Effect.exit(
          sqlA.withTransaction(
            Effect.gen(function* () {
              yield* Migration058.pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
              yield* sqlA.unsafe("CREATE TABLE migration_058_broken(").unprepared;
            }),
          ),
        );
        assert.equal(Exit.isFailure(rollback), true);
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM sqlite_schema ORDER BY type, name
          `,
          beforeSchema,
        );
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT name, seq FROM sqlite_sequence ORDER BY name
          `,
          beforeSequence,
        );
        assert.deepStrictEqual(
          yield* sqlA<Record<string, unknown>>`
            SELECT * FROM effect_sql_migrations ORDER BY migration_id
          `,
          beforeMigrations,
        );

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 58 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlA),
          ),
          [[58, "AgentControlVerificationTurnStart"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 58 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          ),
          [],
        );

        const tables = yield* sqlB<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'table' AND name IN (
            'agent_control_verification_materialization_evidence',
            'agent_control_verification_materialization_receipts',
            'agent_control_verification_materialization_markers',
            'agent_control_verification_handoff_intents',
            'agent_control_verification_handoff_receipts',
            'agent_control_verification_handoff_accepted',
            'agent_control_verification_deliveries',
            'agent_control_verification_turn_accepted',
            'agent_control_verification_session_evidence',
            'agent_control_verification_delivery_attestations',
            'agent_control_verification_stage_started_evidence',
            'agent_control_verification_stage_started_receipts',
            'agent_control_verification_stage_started_markers'
          ) ORDER BY name
        `;
        assert.equal(tables.length, 13);
        assert.deepStrictEqual(
          yield* sqlB<{ readonly name: string }>`
            SELECT name FROM pragma_table_info(
              'agent_control_verification_materialization_evidence'
            ) WHERE name IN (
              'repository_display', 'source_revision', 'task_title', 'task_body',
              'task_source_event_id', 'task_source_event_sequence',
              'task_source_event_stream_version', 'worktree_event_id',
              'worktree_event_sequence', 'worktree_event_stream_version'
            ) ORDER BY name
          `,
          [
            { name: "repository_display" },
            { name: "source_revision" },
            { name: "task_body" },
            { name: "task_source_event_id" },
            { name: "task_source_event_sequence" },
            { name: "task_source_event_stream_version" },
            { name: "task_title" },
            { name: "worktree_event_id" },
            { name: "worktree_event_sequence" },
            { name: "worktree_event_stream_version" },
          ],
        );
        const controlledThreadViews = yield* sqlB<{
          readonly name: string;
          readonly sql: string;
        }>`
          SELECT name, sql FROM sqlite_schema
          WHERE type = 'view' AND name IN (
            'agent_control_controlled_thread_stream_catalog_all',
            'agent_control_controlled_thread_reservation_states_all'
          ) ORDER BY name
        `;
        assert.lengthOf(controlledThreadViews, 2);
        for (const view of controlledThreadViews) {
          assert.include(view.sql, "agent_control_controlled_thread_");
          assert.include(view.sql, "agent_control_implementation_thread_");
          assert.include(view.sql, "agent_control_verification_thread_");
        }
        assert.deepStrictEqual(
          yield* sqlB<{ readonly table: string; readonly from: string; readonly to: string }>`
            SELECT "table", "from", "to"
            FROM pragma_foreign_key_list(
              'agent_control_verification_materialization_evidence'
            )
            WHERE "from" IN (
              'worktree_event_id', 'worktree_reservation_id',
              'worktree_event_stream_version'
            ) AND "table" = 'agent_control_events'
            ORDER BY "from"
          `,
          [
            { table: "agent_control_events", from: "worktree_event_id", to: "event_id" },
            {
              table: "agent_control_events",
              from: "worktree_event_stream_version",
              to: "stream_version",
            },
            {
              table: "agent_control_events",
              from: "worktree_reservation_id",
              to: "stream_id",
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sqlB<{ readonly table: string; readonly from: string; readonly to: string }>`
            SELECT "table", "from", "to"
            FROM pragma_foreign_key_list(
              'agent_control_verification_materialization_evidence'
            )
            WHERE "from" = 'task_source_event_id'
          `,
          [{ table: "agent_control_events", from: "task_source_event_id", to: "event_id" }],
        );
        assert.deepStrictEqual(
          yield* sqlB<{ readonly name: string }>`
            SELECT name FROM sqlite_schema
            WHERE type = 'trigger' AND name IN (
              'agent_control_verification_materialization_evidence_validate',
              'agent_control_verification_handoff_intent_validate',
              'agent_control_verification_task_source_event_no_update',
              'agent_control_verification_task_source_event_no_delete'
            ) ORDER BY name
          `,
          [
            { name: "agent_control_verification_handoff_intent_validate" },
            { name: "agent_control_verification_materialization_evidence_validate" },
            { name: "agent_control_verification_task_source_event_no_delete" },
            { name: "agent_control_verification_task_source_event_no_update" },
          ],
        );

        const canonicalTimestamp = "2026-08-07T07:45:12.345Z";
        const timestampColumns = [
          "claim_expires_at",
          "next_attempt_at",
          "provider_accepted_at",
          "provider_session_created_at",
          "terminal_at",
          "updated_at",
        ] as const;
        type TimestampColumn = (typeof timestampColumns)[number];
        const insertDelivery = (
          column: TimestampColumn,
          value: string | Uint8Array,
          ordinal: number,
        ) => {
          const suffix = `migration-058-timestamp-${ordinal}`;
          const state =
            column === "claim_expires_at"
              ? "claimed"
              : column === "next_attempt_at"
                ? "retry-wait"
                : column === "provider_accepted_at"
                  ? "provider-started"
                  : column === "terminal_at"
                    ? "ambiguous"
                    : "pending";
          const timestampValue = (target: TimestampColumn) =>
            column === target ? value : canonicalTimestamp;
          const bind = (target: TimestampColumn) =>
            column === target && value instanceof Uint8Array ? "CAST(? AS TEXT)" : "?";
          return sqlB.unsafe(
            `INSERT INTO agent_control_verification_deliveries (
              provider_delivery_id, handoff_id, handoff_fingerprint,
              admission_marker_id, materialization_evidence_id,
              controlled_thread_reservation_id, thread_id, stage_run_id, attempt_id,
              lease_id, lease_holder_id, fence_token, provider_instance_id, runtime_mode,
              model_selection_fingerprint, turn_request_command_id, message_id,
              planning_thread_id, plan_id, state, revision, claim_owner_id,
              claim_generation, claim_expires_at, attempt_count, next_attempt_at,
              provider_turn_id, provider_accepted_at, provider_session_created_at,
              provider_resume_cursor_json, terminal_at, last_error_code,
              interrupt_requested, updated_at
            ) VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 3, 'codex', 'approval-required',
              ?, ?, ?, ?, ?, ?, 0, ?, ?, ${bind("claim_expires_at")}, ?,
              ${bind("next_attempt_at")}, ?, ${bind("provider_accepted_at")},
              ${bind("provider_session_created_at")}, NULL, ${bind("terminal_at")}, ?, 0,
              ${bind("updated_at")}
            )`,
            [
              `delivery-${suffix}`,
              `handoff-${suffix}`,
              ordinal.toString(16).padStart(64, "0"),
              `admission-${suffix}`,
              `materialization-${suffix}`,
              `reservation-${suffix}`,
              `thread-${suffix}`,
              `stage-${suffix}`,
              `attempt-${suffix}`,
              `lease-${suffix}`,
              `holder-${suffix}`,
              "a".repeat(64),
              `turn-command-${suffix}`,
              `message-${suffix}`,
              `planning-thread-${suffix}`,
              `plan-${suffix}`,
              state,
              state === "claimed" ? `owner-${suffix}` : null,
              state === "claimed" ? 1 : 0,
              state === "claimed" ? timestampValue("claim_expires_at") : null,
              state === "claimed" || state === "retry-wait" ? 1 : 0,
              state === "retry-wait" ? timestampValue("next_attempt_at") : null,
              state === "provider-started" ? `provider-turn-${suffix}` : null,
              state === "provider-started" ? timestampValue("provider_accepted_at") : null,
              column === "provider_session_created_at"
                ? timestampValue("provider_session_created_at")
                : null,
              state === "ambiguous" ? timestampValue("terminal_at") : null,
              state === "retry-wait" || state === "ambiguous" ? "provider-timeout" : null,
              timestampValue("updated_at"),
            ],
          ).unprepared;
        };
        const invalidTimestamps: ReadonlyArray<string | Uint8Array> = [
          "2026-13-01T00:00:00.000Z",
          "2026-02-30T00:00:00.000Z",
          "2026-01-01T24:00:00.000Z",
          "2026-01-01T23:60:00.000Z",
          "2026-01-01T23:59:60.000Z",
          "2026-01-01T00:00:00+00Z",
          "2026-01-01T00:00:00Z",
          "2026-01-01T00:00:00.000000Z",
          "",
          "not-a-timestamp",
          Uint8Array.from([0x80]),
          Uint8Array.from([0xc0, 0x80]),
          Uint8Array.from([0xe2, 0x82]),
        ];

        yield* sqlB`PRAGMA foreign_keys = OFF`;
        const storageBoundary = yield* Effect.exit(
          sqlB.withTransaction(
            Effect.gen(function* () {
              let ordinal = 1;
              for (const column of timestampColumns) {
                yield* insertDelivery(column, canonicalTimestamp, ordinal++);
                for (const invalid of invalidTimestamps) {
                  assert.isTrue(
                    Exit.isFailure(yield* Effect.exit(insertDelivery(column, invalid, ordinal++))),
                    `${column}:${String(invalid)}`,
                  );
                }
              }
              const earlierOrdinal = ordinal++;
              const laterOrdinal = ordinal++;
              yield* insertDelivery("next_attempt_at", "2024-02-29T23:59:59.999Z", earlierOrdinal);
              yield* insertDelivery("next_attempt_at", "2027-01-01T00:00:00.000Z", laterOrdinal);
              assert.deepStrictEqual(
                yield* sqlB<{ readonly handoffId: string }>`
                  SELECT handoff_id AS "handoffId"
                  FROM agent_control_verification_deliveries
                  WHERE handoff_id IN (
                    ${`handoff-migration-058-timestamp-${earlierOrdinal}`},
                    ${`handoff-migration-058-timestamp-${laterOrdinal}`}
                  )
                  ORDER BY next_attempt_at
                `,
                [
                  { handoffId: `handoff-migration-058-timestamp-${earlierOrdinal}` },
                  { handoffId: `handoff-migration-058-timestamp-${laterOrdinal}` },
                ],
              );

              const updateSuffix = "migration-058-update";
              yield* insertDelivery("claim_expires_at", canonicalTimestamp, ordinal++);
              const [updateTarget] = yield* sqlB<{ readonly providerDeliveryId: string }>`
                SELECT provider_delivery_id AS "providerDeliveryId"
                FROM agent_control_verification_deliveries
                WHERE handoff_id = ${`handoff-migration-058-timestamp-${ordinal - 1}`}
              `;
              assert.isDefined(updateTarget);
              for (const invalid of invalidTimestamps) {
                const valueExpression = invalid instanceof Uint8Array ? "CAST(? AS TEXT)" : "?";
                assert.isTrue(
                  Exit.isFailure(
                    yield* Effect.exit(
                      sqlB.unsafe(
                        `UPDATE agent_control_verification_deliveries
                           SET state='retry-wait', revision=revision+1,
                             claim_owner_id=NULL, claim_expires_at=NULL,
                             next_attempt_at=${valueExpression},
                             last_error_code='session-incompatible', updated_at=?
                           WHERE provider_delivery_id=?`,
                        [invalid, canonicalTimestamp, updateTarget!.providerDeliveryId],
                      ).unprepared,
                    ),
                  ),
                  `update:${String(invalid)}`,
                );
              }
              assert.isTrue(
                Exit.isFailure(
                  yield* Effect.exit(
                    sqlB.unsafe(
                      `UPDATE agent_control_verification_deliveries
                         SET state='retry-wait', revision=revision+1,
                           claim_owner_id=NULL, claim_expires_at=NULL,
                           next_attempt_at=?, last_error_code=CAST(? AS TEXT), updated_at=?
                         WHERE provider_delivery_id=?`,
                      [
                        canonicalTimestamp,
                        Uint8Array.from([0xf0, 0x80, 0x80, 0x80]),
                        canonicalTimestamp,
                        updateTarget!.providerDeliveryId,
                      ],
                    ).unprepared,
                  ),
                ),
                updateSuffix,
              );
              return yield* Effect.fail("rollback timestamp boundary fixtures" as const);
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(storageBoundary));
        yield* sqlB`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sqlB`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sqlB`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
