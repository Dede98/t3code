import * as NodeSqlite from "node:sqlite";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import {
  makeMigration060,
  type Migration060FaultPoint,
} from "./060_AgentControlVerificationEvaluation.ts";

const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

it.live("installs Verification evaluation and v2 handoff authority atomically", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-evaluation-migration-",
      });
      const filename = path.join(directory, "state.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(context, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      const schemaBefore = yield* sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM sqlite_schema ORDER BY type, name
      `;
      for (const faultPoint of [
        "before-copy",
        "after-copy",
        "after-install",
      ] satisfies ReadonlyArray<Migration060FaultPoint>) {
        const rollback = yield* Effect.exit(
          sql.withTransaction(
            makeMigration060(faultPoint).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback), faultPoint);
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          faultPoint,
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], faultPoint);
        assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[60, "AgentControlVerificationEvaluation"] as const],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM pragma_table_info('agent_control_verification_handoff_intents')
          WHERE name IN (
            'prompt_template_version', 'prompt_contract_fingerprint',
            'result_schema_version', 'result_schema_fingerprint'
          ) ORDER BY name
        `,
        [
          { name: "prompt_contract_fingerprint" },
          { name: "prompt_template_version" },
          { name: "result_schema_fingerprint" },
          { name: "result_schema_version" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema WHERE type = 'table'
          AND name LIKE 'agent_control_verification_evaluation_%' ORDER BY name
        `,
        [
          { name: "agent_control_verification_evaluation_evidence" },
          { name: "agent_control_verification_evaluation_markers" },
          { name: "agent_control_verification_evaluation_receipts" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly type: string; readonly name: string }>`
          SELECT type, name FROM sqlite_schema
          WHERE name IN (
            'idx_agent_control_verification_evaluation_provider_turn',
            'idx_agent_control_verification_evaluation_candidate',
            'agent_control_verification_handoff_result_contract_storage_validate',
            'agent_control_verification_handoff_result_contract_update_storage_validate',
            'agent_control_orchestration_event_storage_validate',
            'agent_control_orchestration_event_update_storage_validate',
            'agent_control_orchestration_message_structure_validate',
            'agent_control_verification_result_capture_validate',
            'agent_control_verification_result_fragment_structure_validate',
            'agent_control_verification_result_post_seal_reject',
            'agent_control_verification_result_source_seal_validate',
            'agent_control_verification_result_authority_no_update',
            'agent_control_verification_result_authority_no_replace',
            'agent_control_verification_result_authority_no_delete',
            'agent_control_verification_evaluation_evidence_storage_validate',
            'agent_control_verification_evaluation_receipt_storage_validate',
            'agent_control_verification_evaluation_marker_storage_validate',
            'agent_control_verification_evaluation_evidence_validate',
            'agent_control_verification_evaluation_receipt_validate',
            'agent_control_verification_evaluation_marker_validate',
            'agent_control_verification_evaluation_evidence_no_update',
            'agent_control_verification_evaluation_evidence_no_delete',
            'agent_control_verification_evaluation_receipts_no_update',
            'agent_control_verification_evaluation_receipts_no_delete',
            'agent_control_verification_evaluation_markers_no_update',
            'agent_control_verification_evaluation_markers_no_delete'
          ) ORDER BY type, name
        `,
        [
          { type: "index", name: "idx_agent_control_verification_evaluation_candidate" },
          { type: "index", name: "idx_agent_control_verification_evaluation_provider_turn" },
          { type: "trigger", name: "agent_control_orchestration_event_storage_validate" },
          { type: "trigger", name: "agent_control_orchestration_event_update_storage_validate" },
          { type: "trigger", name: "agent_control_orchestration_message_structure_validate" },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_evidence_no_delete",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_evidence_no_update",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_evidence_storage_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_evidence_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_marker_storage_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_marker_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_markers_no_delete",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_markers_no_update",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_receipt_storage_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_receipt_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_receipts_no_delete",
          },
          {
            type: "trigger",
            name: "agent_control_verification_evaluation_receipts_no_update",
          },
          {
            type: "trigger",
            name: "agent_control_verification_handoff_result_contract_storage_validate",
          },
          {
            type: "trigger",
            name: "agent_control_verification_handoff_result_contract_update_storage_validate",
          },
          { type: "trigger", name: "agent_control_verification_result_authority_no_delete" },
          { type: "trigger", name: "agent_control_verification_result_authority_no_replace" },
          { type: "trigger", name: "agent_control_verification_result_authority_no_update" },
          { type: "trigger", name: "agent_control_verification_result_capture_validate" },
          {
            type: "trigger",
            name: "agent_control_verification_result_fragment_structure_validate",
          },
          { type: "trigger", name: "agent_control_verification_result_post_seal_reject" },
          { type: "trigger", name: "agent_control_verification_result_source_seal_validate" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT type, name, tbl_name AS "tableName" FROM sqlite_schema
          WHERE name LIKE '%rebuild_060%' OR tbl_name LIKE '%rebuild_060%'
        `,
        [],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails migration 060 before mutation when its UTF-8 UDF is missing or divergent", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-udf-preflight-",
      });

      for (const mode of ["missing", "divergent"] as const) {
        const filename = path.join(directory, `${mode}.sqlite`);
        const unregisteredScope = yield* Scope.make("sequential");
        const unregisteredContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layerTest({
            filename,
            _testHooks: {
              registerFunctions: (database) => {
                if (mode === "divergent") {
                  database.function("t3_fatal_utf8", { deterministic: true }, () => 1);
                }
              },
            },
          }),
          unregisteredScope,
        );
        const sql = Context.get(unregisteredContext, SqlClient.SqlClient);
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        yield* sql`
          INSERT INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            ${`migration-060-${mode}-legacy-event`}, 'project',
            ${`migration-060-${mode}-legacy-stream`}, 0, 'project.created',
            '2026-08-26T08:00:00.000Z', NULL, NULL, NULL, 'server', '{}', '{}'
          )
        `;
        const schemaBefore = yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `;
        const dataBefore = yield* sql<Record<string, unknown>>`
          SELECT sequence,
            typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
            typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
            typeof(event_type) AS "eventTypeType", hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
            typeof(payload_json) AS "payloadType", hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            typeof(metadata_json) AS "metadataType",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events ORDER BY sequence
        `;

        const failed = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
        );
        assert.isTrue(Exit.isFailure(failed), mode);
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          mode,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT sequence,
              typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
              typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
              typeof(event_type) AS "eventTypeType", hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
              typeof(payload_json) AS "payloadType", hex(CAST(payload_json AS BLOB)) AS "payloadHex",
              typeof(metadata_json) AS "metadataType",
              hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
            FROM main.orchestration_events ORDER BY sequence
          `,
          dataBefore,
          mode,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id = 60`,
          [],
          mode,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT type, name, tbl_name AS "tableName" FROM main.sqlite_schema
            WHERE name LIKE '%rebuild_060%' OR tbl_name LIKE '%rebuild_060%'
              OR name LIKE 'agent_control_verification_evaluation_%'
          `,
          [],
          mode,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT name FROM pragma_table_info('agent_control_verification_handoff_intents')
            WHERE name='prompt_template_version'
          `,
          [],
          mode,
        );
        yield* Scope.close(unregisteredScope, Exit.void);

        const retryScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(retryScope, Exit.void));
        const retryContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          retryScope,
        );
        const retrySql = Context.get(retryContext, SqlClient.SqlClient);
        yield* retrySql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, retrySql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
          mode,
        );
        assert.deepStrictEqual(
          yield* retrySql`
            SELECT stream_version AS "streamVersion" FROM main.orchestration_events
            WHERE event_id=${`migration-060-${mode}-legacy-event`}
          `,
          [{ streamVersion: 0 }],
          mode,
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects corrupt schema-059 orchestration history before any migration-060 mutation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-history-preflight-",
      });
      const occurredAt = "2026-08-26T08:00:00.000Z";

      for (const corruption of [
        "blob-event-type",
        "integer-event-type",
        "blob-stream-id",
        "blob-payload-json",
        "invalid-utf8-json-text",
        "integer-assistant-role",
        "integer-message-id",
        "matching-source-correlation",
      ] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const filename = path.join(directory, `${corruption}.sqlite`);
            const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
            const sql = Context.get(context, SqlClient.SqlClient);
            assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [
              { journal_mode: "wal" },
            ]);
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );

            const threadId = `migration-060-history-thread-${corruption}`;
            const messageId = `assistant:history-${corruption}`;
            const providerInstanceId = `migration-060-history-provider-${corruption}`;
            const providerTurnId = `migration-060-history-turn-${corruption}`;
            const runtimeEventId = `migration-060-history-runtime-${corruption}`;
            const commandId = `provider:${runtimeEventId}:message-complete:${messageId}`;
            const validPayloadValue = {
              threadId,
              messageId,
              role: "assistant",
              text: "durable source",
              turnId: providerTurnId,
              streaming: false,
              createdAt: occurredAt,
              updatedAt: occurredAt,
            };
            const validPayload = encodeUnknownJson(validPayloadValue);
            const validMetadata = encodeUnknownJson({
              providerRuntimeMessage: {
                runtimeEventId,
                runtimeEventType: "item.completed",
                providerInstanceId,
                providerTurnId,
              },
              verificationResultCapture: {
                schemaVersion: 1,
                disposition: "presentation",
                handoffId: `migration-060-history-handoff-${corruption}`,
                providerDeliveryId: `migration-060-history-delivery-${corruption}`,
                providerInstanceId,
                providerTurnId,
                resultSchemaFingerprint: "a".repeat(64),
              },
            });
            const corruptPayload =
              corruption === "integer-assistant-role"
                ? encodeUnknownJson({
                    ...validPayloadValue,
                    role: 1,
                  })
                : corruption === "integer-message-id"
                  ? encodeUnknownJson({
                      ...validPayloadValue,
                      messageId: 1,
                    })
                  : validPayload;
            const corruptEventType =
              corruption === "blob-event-type" || corruption === "matching-source-correlation"
                ? Buffer.from("thread.message-sent")
                : corruption === "integer-event-type"
                  ? 7
                  : "thread.message-sent";
            const corruptStreamId =
              corruption === "blob-stream-id" ? Buffer.from(threadId) : threadId;
            const corruptPayloadValue =
              corruption === "blob-payload-json"
                ? Buffer.from(validPayload)
                : corruption === "invalid-utf8-json-text"
                  ? Buffer.from([
                      0x7b, 0x22, 0x74, 0x65, 0x78, 0x74, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d,
                    ])
                  : corruptPayload;

            yield* Effect.sync(() => {
              const native = new NodeSqlite.DatabaseSync(filename);
              try {
                native.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON");
                native
                  .prepare(
                    `INSERT INTO main.orchestration_events (
                       event_id, aggregate_kind, stream_id, stream_version, event_type,
                       occurred_at, command_id, causation_event_id, correlation_id,
                       actor_kind, payload_json, metadata_json
                     ) VALUES (?, 'project', ?, 0, 'project.created', ?, NULL, NULL, NULL,
                       'server', '{}', '{}')`,
                  )
                  .run(
                    `migration-060-history-control-${corruption}`,
                    `migration-060-history-control-stream-${corruption}`,
                    occurredAt,
                  );
                native
                  .prepare(
                    `INSERT INTO main.orchestration_events (
                       event_id, aggregate_kind, stream_id, stream_version, event_type,
                       occurred_at, command_id, causation_event_id, correlation_id,
                       actor_kind, payload_json, metadata_json
                     ) VALUES (?, 'thread', ?, 0, ?, ?, ?, NULL, ?, 'provider',
                       ${corruption === "invalid-utf8-json-text" ? "CAST(? AS TEXT)" : "?"}, ?)`,
                  )
                  .run(
                    `migration-060-history-corrupt-${corruption}`,
                    corruptStreamId,
                    corruptEventType,
                    occurredAt,
                    commandId,
                    commandId,
                    corruptPayloadValue,
                    validMetadata,
                  );
              } finally {
                native.close();
              }
            });

            const schemaBefore = yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM main.sqlite_schema ORDER BY type, name
            `;
            const rowsBefore = yield* sql<Record<string, unknown>>`
              SELECT sequence,
                typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
                typeof(aggregate_kind) AS "aggregateKindType",
                hex(CAST(aggregate_kind AS BLOB)) AS "aggregateKindHex",
                typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
                typeof(stream_version) AS "streamVersionType", stream_version AS "streamVersion",
                typeof(event_type) AS "eventTypeType",
                hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
                typeof(occurred_at) AS "occurredAtType",
                hex(CAST(occurred_at AS BLOB)) AS "occurredAtHex",
                typeof(command_id) AS "commandIdType",
                hex(CAST(command_id AS BLOB)) AS "commandIdHex",
                typeof(causation_event_id) AS "causationType",
                typeof(correlation_id) AS "correlationType",
                hex(CAST(correlation_id AS BLOB)) AS "correlationHex",
                typeof(actor_kind) AS "actorType", hex(CAST(actor_kind AS BLOB)) AS "actorHex",
                typeof(payload_json) AS "payloadType", hex(CAST(payload_json AS BLOB)) AS "payloadHex",
                typeof(metadata_json) AS "metadataType",
                hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
              FROM main.orchestration_events ORDER BY sequence
            `;
            const controlBefore = rowsBefore[0];

            const failed = yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
            );
            assert.isTrue(Exit.isFailure(failed), corruption);
            assert.deepStrictEqual(
              yield* sql<Record<string, unknown>>`
                SELECT type, name, tbl_name AS "tableName", sql
                FROM main.sqlite_schema ORDER BY type, name
              `,
              schemaBefore,
              corruption,
            );
            assert.deepStrictEqual(
              yield* sql<Record<string, unknown>>`
                SELECT sequence,
                  typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
                  typeof(aggregate_kind) AS "aggregateKindType",
                  hex(CAST(aggregate_kind AS BLOB)) AS "aggregateKindHex",
                  typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
                  typeof(stream_version) AS "streamVersionType", stream_version AS "streamVersion",
                  typeof(event_type) AS "eventTypeType",
                  hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
                  typeof(occurred_at) AS "occurredAtType",
                  hex(CAST(occurred_at AS BLOB)) AS "occurredAtHex",
                  typeof(command_id) AS "commandIdType",
                  hex(CAST(command_id AS BLOB)) AS "commandIdHex",
                  typeof(causation_event_id) AS "causationType",
                  typeof(correlation_id) AS "correlationType",
                  hex(CAST(correlation_id AS BLOB)) AS "correlationHex",
                  typeof(actor_kind) AS "actorType", hex(CAST(actor_kind AS BLOB)) AS "actorHex",
                  typeof(payload_json) AS "payloadType", hex(CAST(payload_json AS BLOB)) AS "payloadHex",
                  typeof(metadata_json) AS "metadataType",
                  hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
                FROM main.orchestration_events ORDER BY sequence
              `,
              rowsBefore,
              corruption,
            );
            assert.deepStrictEqual(
              yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=60`,
              [],
              corruption,
            );
            assert.deepStrictEqual(
              yield* sql`
                SELECT type, name, tbl_name AS "tableName" FROM main.sqlite_schema
                WHERE name LIKE '%rebuild_060%' OR tbl_name LIKE '%rebuild_060%'
                  OR name LIKE 'agent_control_verification_evaluation_%'
              `,
              [],
              corruption,
            );
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], corruption);

            yield* sql`
              UPDATE main.orchestration_events
              SET stream_id=${threadId}, event_type='thread.message-sent',
                payload_json=${validPayload}, metadata_json=${validMetadata}
              WHERE event_id=${`migration-060-history-corrupt-${corruption}`}
            `;
            assert.deepStrictEqual(
              yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
              [[60, "AgentControlVerificationEvaluation"]],
              corruption,
            );
            const rowsAfter = yield* sql<Record<string, unknown>>`
              SELECT sequence,
                typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
                typeof(aggregate_kind) AS "aggregateKindType",
                hex(CAST(aggregate_kind AS BLOB)) AS "aggregateKindHex",
                typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
                typeof(stream_version) AS "streamVersionType", stream_version AS "streamVersion",
                typeof(event_type) AS "eventTypeType",
                hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
                typeof(occurred_at) AS "occurredAtType",
                hex(CAST(occurred_at AS BLOB)) AS "occurredAtHex",
                typeof(command_id) AS "commandIdType",
                hex(CAST(command_id AS BLOB)) AS "commandIdHex",
                typeof(causation_event_id) AS "causationType",
                typeof(correlation_id) AS "correlationType",
                hex(CAST(correlation_id AS BLOB)) AS "correlationHex",
                typeof(actor_kind) AS "actorType", hex(CAST(actor_kind AS BLOB)) AS "actorHex",
                typeof(payload_json) AS "payloadType", hex(CAST(payload_json AS BLOB)) AS "payloadHex",
                typeof(metadata_json) AS "metadataType",
                hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
              FROM main.orchestration_events ORDER BY sequence
            `;
            assert.deepStrictEqual(rowsAfter[0], controlBefore, corruption);
            assert.equal(rowsAfter[1]?.streamIdType, "text", corruption);
            assert.equal(rowsAfter[1]?.eventTypeType, "text", corruption);
            assert.equal(rowsAfter[1]?.payloadType, "text", corruption);
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], corruption);
          }),
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects non-positive versions and non-fatal UTF-8 without sequence gaps", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-orchestration-storage-",
      });
      const filename = path.join(directory, "state.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      const insertEvent = (
        suffix: string,
        overrides: {
          readonly sequence?: string;
          readonly streamId?: string;
          readonly streamVersion?: string;
          readonly eventId?: string;
          readonly commandId?: string;
          readonly payload?: string;
          readonly metadata?: string;
        } = {},
      ) => {
        const sequenceColumn = overrides.sequence === undefined ? "" : "sequence,";
        const sequenceValue = overrides.sequence === undefined ? "" : `${overrides.sequence},`;
        return `
          INSERT INTO orchestration_events(
            ${sequenceColumn} event_id, aggregate_kind, stream_id, stream_version,
            event_type, occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            ${sequenceValue} ${overrides.eventId ?? `'event-${suffix}'`}, 'project',
            ${overrides.streamId ?? `'stream-${suffix}'`},
            ${overrides.streamVersion ?? "1"}, 'project.created',
            '2026-08-26T08:00:00.000Z', ${overrides.commandId ?? "NULL"}, NULL, NULL,
            'server', ${overrides.payload ?? `'{}'`}, ${overrides.metadata ?? `'{}'`}
          )
        `;
      };
      const expectRejected = (statement: string, label: string) =>
        Effect.gen(function* () {
          const result = yield* Effect.exit(sql.unsafe(statement));
          assert.isTrue(Exit.isFailure(result), label);
        });

      assert.deepStrictEqual(
        yield* sql.unsafe<{ readonly jsonValid: number }>(
          `SELECT json_valid(CAST(X'7B2261223A22FF227D' AS TEXT)) AS "jsonValid"`,
        ),
        [{ jsonValid: 1 }],
      );

      for (const [label, statement] of [
        ["sequence zero", insertEvent("sequence-zero", { sequence: "0" })],
        ["sequence negative", insertEvent("sequence-negative", { sequence: "-7" })],
        ["stream version zero", insertEvent("stream-zero", { streamVersion: "0" })],
        ["stream version negative", insertEvent("stream-negative", { streamVersion: "-4" })],
        [
          "OR IGNORE cannot suppress storage rejection",
          insertEvent("or-ignore", { streamVersion: "0" }).replace(
            "INSERT INTO orchestration_events",
            "INSERT OR IGNORE INTO orchestration_events",
          ),
        ],
        [
          "OR REPLACE cannot suppress storage rejection",
          insertEvent("or-replace", { streamVersion: "0" }).replace(
            "INSERT INTO orchestration_events",
            "INSERT OR REPLACE INTO orchestration_events",
          ),
        ],
        [
          "invalid payload UTF-8 accepted by JSON1",
          insertEvent("payload-invalid", {
            payload: "CAST(X'7B2261223A22FF227D' AS TEXT)",
          }),
        ],
        [
          "invalid metadata UTF-8",
          insertEvent("metadata-invalid", {
            metadata: "CAST(X'7B2261223A22FF227D' AS TEXT)",
          }),
        ],
        [
          "invalid required envelope UTF-8",
          insertEvent("event-id-invalid", { eventId: "CAST(X'80' AS TEXT)" }),
        ],
        [
          "invalid nullable envelope UTF-8",
          insertEvent("command-invalid", { commandId: "CAST(X'80' AS TEXT)" }),
        ],
        ["payload BLOB", insertEvent("payload-blob", { payload: "X'7B7D'" })],
        ["event id BLOB", insertEvent("event-id-blob", { eventId: "X'6576656E74'" })],
      ] as const) {
        yield* expectRejected(statement, label);
      }

      yield* sql`
        CREATE TABLE migration_060_receipt_probe (
          event_id TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        CREATE TABLE migration_060_projection_probe (
          event_id TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        CREATE TRIGGER migration_060_receipt_probe_after_insert
        AFTER INSERT ON orchestration_events
        WHEN NEW.event_id LIKE 'event-atomic-%'
        BEGIN
          INSERT INTO migration_060_receipt_probe(event_id) VALUES (NEW.event_id);
        END
      `;
      yield* sql`
        CREATE TRIGGER migration_060_projection_probe_after_insert
        AFTER INSERT ON orchestration_events
        WHEN NEW.event_id LIKE 'event-atomic-%'
        BEGIN
          INSERT INTO migration_060_projection_probe(event_id) VALUES (NEW.event_id);
        END
      `;
      const [beforeAtomicFailure] = yield* sql<{ readonly sequence: number }>`
        SELECT COALESCE(max(sequence), 0) AS sequence FROM orchestration_events
      `;
      yield* expectRejected(
        insertEvent("atomic-rejected", { payload: "CAST(X'80' AS TEXT)" }),
        "caught AFTER INSERT storage rejection",
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*) FROM orchestration_events
             WHERE event_id='event-atomic-rejected') AS events,
            (SELECT count(*) FROM migration_060_receipt_probe) AS receipts,
            (SELECT count(*) FROM migration_060_projection_probe) AS projections
        `,
        [{ events: 0, receipts: 0, projections: 0 }],
      );
      yield* sql.unsafe(insertEvent("atomic-valid"));
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT sequence FROM orchestration_events
             WHERE event_id='event-atomic-valid') AS sequence,
            (SELECT count(*) FROM migration_060_receipt_probe) AS receipts,
            (SELECT count(*) FROM migration_060_projection_probe) AS projections
        `,
        [
          {
            sequence: beforeAtomicFailure!.sequence + 1,
            receipts: 1,
            projections: 1,
          },
        ],
      );

      yield* sql.unsafe(
        insertEvent("replacement", {
          eventId: `'event-replacement-�'`,
          streamId: `'stream-replacement-�'`,
          payload: `'{"value":"�"}'`,
        }),
      );
      const [replacement] = yield* sql<{
        readonly sequence: number;
        readonly eventIdHex: string;
        readonly payloadHex: string;
      }>`
        SELECT sequence, hex(CAST(event_id AS BLOB)) AS "eventIdHex",
          hex(CAST(payload_json AS BLOB)) AS "payloadHex"
        FROM orchestration_events WHERE event_id = ${`event-replacement-�`}
      `;
      assert.match(replacement!.eventIdHex, /EFBFBD$/u);
      assert.match(replacement!.payloadHex, /EFBFBD/u);

      const sequenceBeforeRejectedAutoincrement = replacement!.sequence;
      yield* expectRejected(
        insertEvent("rejected-autoincrement", { payload: "CAST(X'80' AS TEXT)" }),
        "rejected autoincrement insert",
      );
      yield* sql.unsafe(insertEvent("after-rejected-autoincrement"));
      assert.deepStrictEqual(
        yield* sql<{ readonly sequence: number }>`
          SELECT sequence FROM orchestration_events
          WHERE event_id='event-after-rejected-autoincrement'
        `,
        [{ sequence: sequenceBeforeRejectedAutoincrement + 1 }],
      );

      yield* expectRejected(
        insertEvent("stream-retry-corrupt", {
          streamId: "'stream-retry'",
          payload: "CAST(X'80' AS TEXT)",
        }),
        "corrupt first stream event",
      );
      yield* sql.unsafe(
        insertEvent("stream-retry-valid", {
          streamId: "'stream-retry'",
        }),
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly streamVersion: number }>`
          SELECT stream_version AS "streamVersion" FROM orchestration_events
          WHERE stream_id='stream-retry'
        `,
        [{ streamVersion: 1 }],
      );
      yield* Effect.sync(() => {
        const native = new NodeSqlite.DatabaseSync(filename);
        try {
          native.exec("PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON");
          assert.deepStrictEqual(
            native
              .prepare(
                "SELECT count(*) AS count FROM main.orchestration_events WHERE event_id='event-atomic-valid'",
              )
              .all(),
            [{ count: 1 }],
          );
          assert.throws(
            () => native.exec(insertEvent("unregistered-native")),
            /no such function: t3_fatal_utf8/u,
          );
          NodeSqliteClient.registerNodeSqliteFunctions(native);
          native.exec(insertEvent("registered-native"));
        } finally {
          native.close();
        }
      });
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count FROM orchestration_events
          WHERE event_id IN ('event-unregistered-native', 'event-registered-native')
        `,
        [{ count: 1 }],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
