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
