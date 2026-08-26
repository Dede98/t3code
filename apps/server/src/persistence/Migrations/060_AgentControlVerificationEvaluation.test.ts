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
  makeMigration060,
  type Migration060FaultPoint,
} from "./060_AgentControlVerificationEvaluation.ts";

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
