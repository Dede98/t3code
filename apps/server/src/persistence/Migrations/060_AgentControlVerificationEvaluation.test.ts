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
import type { SqlError } from "effect/unstable/sql/SqlError";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import * as SqliteFunctions from "../SqliteFunctions.ts";
import {
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER,
} from "../../agentControl/verificationTurn/runtimeEventAuthority.ts";
import {
  AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_060_SQL,
  canonicalizeVerificationHandoffTriggerSql,
  makeMigration060,
  ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX,
  ORCHESTRATION_AUTHORITY_ROUTE_SEQUENCE_INDEX,
  ORCHESTRATION_STREAM_BYTES_SEQUENCE_INDEX,
  ORCHESTRATION_PROJECT_MEMBERSHIP_ROUTE_SEQUENCE_INDEX,
  type Migration060FaultPoint,
} from "./060_AgentControlVerificationEvaluation.ts";
import { AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL } from "./verificationHandoffIntentTrigger.ts";
import {
  orchestrationEventAuthorityRouteBytes,
  orchestrationEventProjectMembershipRouteBytes,
  ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_NONE,
  ORCHESTRATION_EVENT_ROUTE_INVALID,
} from "../../orchestration/orchestrationEventStorage.ts";

const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

// Production fixture reconstructed from the ProviderRuntimeIngestion correlation
// object and EventMetadataFromJsonString encoder at 6ae31d3cc881380c706b7320cb8d652feaa5fcea.
// Do not rebuild this with the current five-field encoder.
const HISTORICAL_PROVIDER_RUNTIME_METADATA =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
const HISTORICAL_PROVIDER_RUNTIME_METADATA_HEX =
  "7b2270726f766964657252756e74696d654d657373616765223a7b2272756e74696d654576656e744964223a226576656e742d686973746f726963616c222c2272756e74696d654576656e7454797065223a226974656d2e636f6d706c65746564222c2270726f7669646572496e7374616e63654964223a22636f646578222c2270726f76696465725475726e4964223a227475726e2d686973746f726963616c227d7d";
const HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_NULL_ITEM =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}';
const HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_NULL_ITEM_HEX =
  "7b2270726f766964657252756e74696d654d657373616765223a7b2272756e74696d654576656e744964223a226576656e742d686973746f726963616c2d6974656d222c2272756e74696d654576656e7454797065223a226974656d2e636f6d706c65746564222c2270726f7669646572496e7374616e63654964223a22636f646578222c2270726f76696465725475726e4964223a227475726e2d686973746f726963616c2d6974656d222c2270726f76696465724974656d4964223a6e756c6c7d7d";
const HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_TEXT_ITEM =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":"item-historical"}}';
const HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_TEXT_ITEM_HEX =
  "7b2270726f766964657252756e74696d654d657373616765223a7b2272756e74696d654576656e744964223a226576656e742d686973746f726963616c2d6974656d222c2272756e74696d654576656e7454797065223a226974656d2e636f6d706c65746564222c2270726f7669646572496e7374616e63654964223a22636f646578222c2270726f76696465725475726e4964223a227475726e2d686973746f726963616c2d6974656d222c2270726f76696465724974656d4964223a226974656d2d686973746f726963616c227d7d";
// Exact EventMetadataFromJsonString bytes emitted at de63cc314 through
// e13573a25^ when both historical correlations were present.
const HISTORICAL_PROVIDER_RUNTIME_WITH_CAPTURE_METADATA =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-capture","runtimeEventType":"content.delta","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","providerItemId":"item-historical-capture"},"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"handoff-historical-capture","providerDeliveryId":"delivery-historical-capture","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';
const HISTORICAL_PROVIDER_RUNTIME_WITH_CAPTURE_METADATA_HEX =
  "7b2270726f766964657252756e74696d654d657373616765223a7b2272756e74696d654576656e744964223a226576656e742d686973746f726963616c2d63617074757265222c2272756e74696d654576656e7454797065223a22636f6e74656e742e64656c7461222c2270726f7669646572496e7374616e63654964223a22636f646578222c2270726f76696465725475726e4964223a227475726e2d686973746f726963616c2d63617074757265222c2270726f76696465724974656d4964223a226974656d2d686973746f726963616c2d63617074757265227d2c22766572696669636174696f6e526573756c7443617074757265223a7b22736368656d6156657273696f6e223a312c22646973706f736974696f6e223a2270726573656e746174696f6e222c2268616e646f66664964223a2268616e646f66662d686973746f726963616c2d63617074757265222c2270726f766964657244656c69766572794964223a2264656c69766572792d686973746f726963616c2d63617074757265222c2270726f7669646572496e7374616e63654964223a22636f646578222c2270726f76696465725475726e4964223a227475726e2d686973746f726963616c2d63617074757265222c22726573756c74536368656d6146696e6765727072696e74223a2266666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666227d7d";

// Exact field order emitted by the ProjectCreatedPayload schema/object encoder
// at parent 8994c6a900d80c390e99824984c808dd2017ecb9. Keep this fixture independent
// of the current event storage helpers.
const historicalProjectCreatedPayload = (projectId: string, occurredAt: string): string =>
  `{"projectId":${encodeUnknownJson(projectId)},"title":"Historical project","workspaceRoot":${encodeUnknownJson(`/tmp/${projectId}`)},"defaultModelSelection":null,"scripts":[],"createdAt":${encodeUnknownJson(occurredAt)},"updatedAt":${encodeUnknownJson(occurredAt)}}`;

const threadCreatedPayload = (threadId: string, projectId: string, occurredAt: string): string =>
  encodeUnknownJson({
    threadId,
    projectId,
    title: "Route storage thread",
    modelSelection: { instanceId: "codex", model: "gpt-5-codex" },
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    createdAt: occurredAt,
    updatedAt: occurredAt,
  });

const registerMigration060FunctionsExcept = (
  database: NodeSqlite.DatabaseSync,
  omitted:
    | typeof SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION
    | typeof SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
): void => {
  database.function(
    SqliteFunctions.SQLITE_FATAL_UTF8_FUNCTION,
    { deterministic: true },
    SqliteFunctions.isFatalUtf8Blob,
  );
  database.function(
    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
    { deterministic: true },
    SqliteFunctions.sqliteOrchestrationEventJsonStorage,
  );
  database.function(
    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FUNCTION,
    { deterministic: true },
    SqliteFunctions.sqliteOrchestrationEventJsonStorageProtocol,
  );
  if (omitted !== SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION) {
    database.function(
      SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
      { deterministic: true },
      SqliteFunctions.sqliteOrchestrationEventAuthorityRoute,
    );
  }
  if (omitted !== SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION) {
    database.function(
      SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
      { deterministic: true },
      SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute,
    );
  }
  database.function(
    SqliteFunctions.SQLITE_VERIFICATION_DELTA_DIGEST_FUNCTION,
    { deterministic: true },
    SqliteFunctions.sqliteVerificationDeltaDigest,
  );
  database.function(
    SqliteFunctions.SQLITE_VERIFICATION_COMPLETION_DIGEST_FUNCTION,
    { deterministic: true },
    SqliteFunctions.sqliteVerificationCompletionDigest,
  );
  database.function(
    SqliteFunctions.SQLITE_VERIFICATION_EVIDENCE_DIGEST_FUNCTION,
    { deterministic: true },
    SqliteFunctions.sqliteVerificationEvidenceDigest,
  );
};

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
        "after-materialization-authority-install",
        "after-copy",
        "after-runtime-authority-install",
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
            ${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX},
            ${ORCHESTRATION_AUTHORITY_ROUTE_SEQUENCE_INDEX},
            ${ORCHESTRATION_STREAM_BYTES_SEQUENCE_INDEX},
            ${ORCHESTRATION_PROJECT_MEMBERSHIP_ROUTE_SEQUENCE_INDEX},
            ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX},
            'agent_control_verification_handoff_result_contract_storage_validate',
            'agent_control_verification_handoff_result_contract_update_storage_validate',
            'agent_control_orchestration_event_storage_validate',
            'agent_control_orchestration_event_update_storage_validate',
            'agent_control_orchestration_json_storage_validate',
            'agent_control_orchestration_message_structure_validate',
            'agent_control_verification_result_capture_validate',
            'agent_control_verification_result_fragment_structure_validate',
            'agent_control_verification_result_post_seal_reject',
            ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER},
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
          { type: "index", name: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX },
          { type: "index", name: ORCHESTRATION_AUTHORITY_ROUTE_SEQUENCE_INDEX },
          { type: "index", name: ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX },
          { type: "index", name: ORCHESTRATION_PROJECT_MEMBERSHIP_ROUTE_SEQUENCE_INDEX },
          { type: "index", name: ORCHESTRATION_STREAM_BYTES_SEQUENCE_INDEX },
          { type: "trigger", name: "agent_control_orchestration_event_storage_validate" },
          { type: "trigger", name: "agent_control_orchestration_event_update_storage_validate" },
          { type: "trigger", name: "agent_control_orchestration_json_storage_validate" },
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
          { type: "trigger", name: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER },
          { type: "trigger", name: "agent_control_verification_result_source_seal_validate" },
        ],
      );
      const commandLookupPlan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT sequence
        FROM main.orchestration_events
        WHERE sequence > ${32}
          AND command_id IS NOT NULL
          AND CAST(command_id AS BLOB) = ${new TextEncoder().encode("late-command")}
        ORDER BY sequence
        LIMIT 32
      `;
      assert.isTrue(
        commandLookupPlan.some((row) =>
          row.detail.includes(`USING INDEX ${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}`),
        ),
      );
      assert.isTrue(commandLookupPlan.some((row) => row.detail.includes("sequence>?")));
      assert.isFalse(
        commandLookupPlan.some((row) => row.detail.includes("SCAN orchestration_events")),
      );
      assert.isFalse(commandLookupPlan.some((row) => row.detail.includes("USE TEMP B-TREE")));
      const streamLookupPlan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN
         WITH targets(target_ordinal, aggregate_kind_bytes, stream_id_bytes, candidate_sequence)
           AS (VALUES (0, ?, ?, ?), (1, ?, ?, ?))
         SELECT targets.target_ordinal, prior.sequence
         FROM targets
         LEFT JOIN main.orchestration_events AS prior ON prior.sequence = (
           SELECT predecessor.sequence
           FROM main.orchestration_events AS predecessor
           WHERE predecessor.sequence < targets.candidate_sequence
             AND CAST(predecessor.aggregate_kind AS BLOB) = targets.aggregate_kind_bytes
             AND CAST(predecessor.stream_id AS BLOB) = targets.stream_id_bytes
           ORDER BY predecessor.sequence DESC
           LIMIT 1
         )`,
        [
          new TextEncoder().encode("thread"),
          new TextEncoder().encode("thread-query-plan-a"),
          128,
          new TextEncoder().encode("thread"),
          new TextEncoder().encode("thread-query-plan-b"),
          256,
        ],
      );
      assert.isTrue(
        streamLookupPlan.some(
          (row) =>
            row.detail.includes("USING") &&
            row.detail.includes(ORCHESTRATION_STREAM_BYTES_SEQUENCE_INDEX),
        ),
      );
      assert.isFalse(streamLookupPlan.some((row) => row.detail.includes("USE TEMP B-TREE")));
      const payloadThreadLookupPlan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN
         SELECT sequence
         FROM main.orchestration_events
         WHERE sequence > ? AND sequence < ?
           AND t3_orchestration_event_authority_route(
             CAST(event_type AS BLOB), CAST(payload_json AS BLOB), CAST(metadata_json AS BLOB)
           ) = ?
         ORDER BY sequence
         LIMIT 32`,
        [0, 128, orchestrationEventAuthorityRouteBytes("thread", "thread-query-plan")],
      );
      assert.isTrue(
        payloadThreadLookupPlan.some(
          (row) =>
            row.detail.includes("USING") &&
            row.detail.includes(ORCHESTRATION_AUTHORITY_ROUTE_SEQUENCE_INDEX),
        ),
      );
      assert.isFalse(payloadThreadLookupPlan.some((row) => row.detail.includes("USE TEMP B-TREE")));
      const projectThreadLookupPlan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN
         SELECT sequence, stream_id
         FROM main.orchestration_events
         WHERE sequence > ? AND sequence < ?
           AND t3_orchestration_event_project_membership_route(
             CAST(event_type AS BLOB), CAST(payload_json AS BLOB), CAST(metadata_json AS BLOB)
           ) = ?
         ORDER BY sequence
         LIMIT 32`,
        [0, 128, orchestrationEventProjectMembershipRouteBytes("project-query-plan")],
      );
      assert.isTrue(
        projectThreadLookupPlan.some(
          (row) =>
            row.detail.includes("USING") &&
            row.detail.includes(ORCHESTRATION_PROJECT_MEMBERSHIP_ROUTE_SEQUENCE_INDEX),
        ),
      );
      assert.isFalse(projectThreadLookupPlan.some((row) => row.detail.includes("USE TEMP B-TREE")));
      const invalidRouteLookupPlan = yield* sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN
         SELECT sequence
         FROM main.orchestration_events AS event
         WHERE event.sequence > ? AND event.sequence < ?
           AND t3_orchestration_event_authority_route(
             CAST(event.event_type AS BLOB), CAST(event.payload_json AS BLOB),
             CAST(event.metadata_json AS BLOB)
           ) = ?
           AND CASE
             WHEN json_valid(event.payload_json) = 1 THEN EXISTS (
               SELECT 1 FROM json_each(event.payload_json) AS route_claim
               WHERE route_claim.key = 'threadId'
                 AND typeof(route_claim.value) = 'text'
                 AND CAST(route_claim.value AS BLOB) = ?
             )
             ELSE 1
           END = 1
         ORDER BY event.sequence
         LIMIT 32`,
        [0, 128, ORCHESTRATION_EVENT_ROUTE_INVALID, new TextEncoder().encode("thread-query-plan")],
      );
      assert.isTrue(
        invalidRouteLookupPlan.some(
          (row) =>
            row.detail.includes("USING") &&
            row.detail.includes(ORCHESTRATION_AUTHORITY_ROUTE_SEQUENCE_INDEX),
        ),
      );
      assert.isFalse(invalidRouteLookupPlan.some((row) => row.detail.includes("USE TEMP B-TREE")));
      const runtimeAuthoritySchema = yield* Effect.sync(() => {
        const native = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
        try {
          return {
            indexList: native
              .prepare(
                `SELECT name, "unique", origin, partial
                 FROM pragma_index_list('orchestration_events') WHERE name=?`,
              )
              .all(VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX),
            indexXinfo: native
              .prepare(`PRAGMA index_xinfo('${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}')`)
              .all(),
            indexSql: native
              .prepare("SELECT sql FROM main.sqlite_schema WHERE type='index' AND name=?")
              .all(VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX)
              .map((row) => ({ sql: String(row.sql).trimEnd() })),
            triggerSql: native
              .prepare("SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=?")
              .all(VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER)
              .map((row) => ({ sql: String(row.sql).trimEnd() })),
          };
        } finally {
          native.close();
        }
      });
      assert.deepStrictEqual(runtimeAuthoritySchema.indexList, [
        {
          name: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
          unique: 1,
          origin: "c",
          partial: 1,
        },
      ]);
      assert.deepStrictEqual(runtimeAuthoritySchema.indexXinfo, [
        { seqno: 0, cid: -2, name: null, desc: 0, coll: "BINARY", key: 1 },
        { seqno: 1, cid: -1, name: null, desc: 0, coll: "BINARY", key: 0 },
      ]);
      assert.deepStrictEqual(runtimeAuthoritySchema.indexSql, [
        {
          sql: `CREATE UNIQUE INDEX ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}
      ON orchestration_events (
        CAST(json_extract(
          metadata_json, '$.providerRuntimeMessage.runtimeEventId'
        ) AS BLOB)
      )
      WHERE typeof(event_type) = 'text'
        AND CAST(event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)`,
        },
      ]);
      assert.deepStrictEqual(runtimeAuthoritySchema.triggerSql, [
        {
          sql: `CREATE TRIGGER ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER}
      BEFORE INSERT ON orchestration_events
      WHEN typeof(NEW.event_type) = 'text'
        AND CAST(NEW.event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
        AND EXISTS (
          SELECT 1
          FROM main.orchestration_events authoritative
          WHERE typeof(authoritative.event_type) = 'text'
            AND CAST(authoritative.event_type AS BLOB) =
              CAST('thread.verification-result-fragment-captured' AS BLOB)
            AND CAST(json_extract(
              authoritative.metadata_json,
              '$.providerRuntimeMessage.runtimeEventId'
            ) AS BLOB) = CAST(json_extract(
              NEW.metadata_json,
              '$.providerRuntimeMessage.runtimeEventId'
            ) AS BLOB)
        )
      BEGIN
        SELECT RAISE(ABORT, 'verification result runtime event id authority conflict');
      END`,
        },
      ]);
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

it.live(
  "requires the complete canonical schema-059 handoff trigger before installing schema 060",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-verification-handoff-trigger-preflight-",
        });
        const triggerName = "agent_control_verification_handoff_intent_validate";
        const tableName = "agent_control_verification_handoff_intents";
        const semanticVariants = [
          {
            name: "block-comment-tokens",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON ${tableName}
            WHEN 0 BEGIN
              SELECT 1;
              /* AND NEW.template_version IS 'agent-control-verification-prompt-v1'
                 AND NEW.prompt_template_version IS NULL
                 AND NEW.result_schema_fingerprint IS '${"f".repeat(64)}' */
            END`,
          },
          {
            name: "line-comment-tokens",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON ${tableName}
            WHEN 0 BEGIN
              -- AND NEW.template_version IS 'agent-control-verification-prompt-v1'
              -- AND NEW.prompt_template_version IS NULL
              -- AND NEW.result_schema_fingerprint IS '${"f".repeat(64)}'
              SELECT 1;
            END`,
          },
          {
            name: "string-literal-tokens",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON ${tableName}
            WHEN 0 BEGIN
              SELECT 'AND NEW.template_version IS ''agent-control-verification-prompt-v1''
                AND NEW.prompt_template_version IS NULL
                AND NEW.result_schema_fingerprint IS ''${"f".repeat(64)}''';
            END`,
          },
          {
            name: "unreachable-case-tokens",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON ${tableName}
            WHEN 0 BEGIN
              SELECT CASE WHEN 0 THEN
                'AND NEW.template_version IS ''agent-control-verification-prompt-v1''
                 AND NEW.prompt_template_version IS NULL
                 AND NEW.result_schema_fingerprint IS ''${"f".repeat(64)}'''
              ELSE 'unreachable' END;
            END`,
          },
          {
            name: "select-one-body",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON ${tableName}
            WHEN 0 BEGIN SELECT 1; END`,
          },
          {
            name: "permissive-when",
            mutate: (sql: string) => sql.replace("WHEN NOT EXISTS (", "WHEN 1 OR NOT EXISTS ("),
          },
          {
            name: "inverted-comparison",
            mutate: (sql: string) =>
              sql.replace(
                "NEW.template_version IS 'agent-control-verification-prompt-v1'",
                "NEW.template_version IS NOT 'agent-control-verification-prompt-v1'",
              ),
          },
          {
            name: "different-raise",
            mutate: (sql: string) =>
              sql.replace(
                "RAISE(ABORT, 'verification handoff intent is inconsistent')",
                "RAISE(IGNORE)",
              ),
          },
          {
            name: "different-body",
            mutate: (sql: string) =>
              sql.replace(
                "BEGIN SELECT RAISE(ABORT, 'verification handoff intent is inconsistent'); END",
                "BEGIN SELECT 1; END",
              ),
          },
          {
            name: "additional-statement",
            mutate: (sql: string) =>
              sql.replace(
                "BEGIN SELECT RAISE(ABORT, 'verification handoff intent is inconsistent'); END",
                "BEGIN SELECT 1; SELECT RAISE(ABORT, 'verification handoff intent is inconsistent'); END",
              ),
          },
          {
            name: "wrong-table",
            sql: `CREATE TRIGGER ${triggerName} BEFORE INSERT ON orchestration_events
            BEGIN SELECT 1; END`,
          },
          {
            name: "case-folded-name",
            sql: `CREATE TRIGGER ${triggerName.toUpperCase()} BEFORE INSERT ON ${tableName}
            BEGIN SELECT 1; END`,
          },
        ] as const;

        for (const variant of semanticVariants) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const filename = path.join(directory, `${variant.name}.sqlite`);
              const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
              const sql = Context.get(context, SqlClient.SqlClient);
              yield* sql`PRAGMA foreign_keys = ON`;
              yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              );
              const [installed059] = yield* sql<{ readonly sql: string }>`
              SELECT sql FROM main.sqlite_schema
              WHERE type='trigger' AND name=${triggerName} AND tbl_name=${tableName}
            `;
              assert.equal(
                canonicalizeVerificationHandoffTriggerSql(installed059!.sql),
                canonicalizeVerificationHandoffTriggerSql(
                  AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL,
                ),
                variant.name,
              );
              yield* sql`DROP TRIGGER main.agent_control_verification_handoff_intent_validate`;
              const candidateSql =
                "sql" in variant
                  ? variant.sql
                  : variant.mutate(
                      AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL,
                    );
              yield* sql.unsafe(candidateSql).unprepared;
              const schemaBefore = yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM main.sqlite_schema ORDER BY type, name
            `;
              const dataBefore = yield* sql<Record<string, unknown>>`
              SELECT count(*) AS count FROM main.orchestration_events
            `;

              const failed = yield* Effect.exit(
                runMigrations({ toMigrationInclusive: 60 }).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                ),
              );
              assert.isTrue(Exit.isFailure(failed), variant.name);
              assert.deepStrictEqual(
                yield* sql<Record<string, unknown>>`
                SELECT type, name, tbl_name AS "tableName", sql
                FROM main.sqlite_schema ORDER BY type, name
              `,
                schemaBefore,
                variant.name,
              );
              assert.deepStrictEqual(
                yield* sql<Record<string, unknown>>`
                SELECT count(*) AS count FROM main.orchestration_events
              `,
                dataBefore,
                variant.name,
              );
              assert.deepStrictEqual(
                yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
                [],
                variant.name,
              );
              assert.deepStrictEqual(
                yield* sql`
                SELECT name FROM main.sqlite_schema
                WHERE name LIKE 'agent_control_verification_evaluation_%'
                   OR name LIKE '%rebuild_060%'
              `,
                [],
                variant.name,
              );

              yield* sql`DROP TRIGGER main.agent_control_verification_handoff_intent_validate`;
              yield* sql.unsafe(AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL)
                .unprepared;
              assert.deepStrictEqual(
                yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
                  Effect.provideService(SqlClient.SqlClient, sql),
                ),
                [[60, "AgentControlVerificationEvaluation"]],
                variant.name,
              );
              const [installed] = yield* sql<{ readonly sql: string }>`
              SELECT sql FROM main.sqlite_schema
              WHERE type='trigger' AND name=${triggerName} AND tbl_name=${tableName}
            `;
              assert.equal(
                canonicalizeVerificationHandoffTriggerSql(installed!.sql),
                canonicalizeVerificationHandoffTriggerSql(
                  AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_060_SQL,
                ),
                variant.name,
              );
              assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, [], variant.name);
              assert.deepStrictEqual(
                yield* sql`PRAGMA main.integrity_check`,
                [{ integrity_check: "ok" }],
                variant.name,
              );
            }),
          );
        }

        const crossTypeFilename = path.join(directory, "cross-type-name.sqlite");
        const crossTypeContext = yield* Layer.build(
          NodeSqliteClient.layer({ filename: crossTypeFilename }),
        );
        const crossTypeSql = Context.get(crossTypeContext, SqlClient.SqlClient);
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, crossTypeSql),
        );
        yield* crossTypeSql`DROP TRIGGER main.agent_control_verification_handoff_intent_validate`;
        yield* crossTypeSql.unsafe(`CREATE TABLE ${triggerName.toUpperCase()} (value TEXT)`)
          .unprepared;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, crossTypeSql),
              ),
            ),
          ),
        );
        assert.deepStrictEqual(
          yield* crossTypeSql`SELECT type, name FROM main.sqlite_schema
          WHERE lower(name)=lower(${triggerName})`,
          [{ type: "table", name: triggerName.toUpperCase() }],
        );

        const formatFilename = path.join(directory, "format-only.sqlite");
        const formatContext = yield* Layer.build(
          NodeSqliteClient.layer({ filename: formatFilename }),
        );
        const formatSql = Context.get(formatContext, SqlClient.SqlClient);
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, formatSql),
        );
        yield* formatSql`DROP TRIGGER main.agent_control_verification_handoff_intent_validate`;
        yield* formatSql.unsafe(
          AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL.replaceAll(
            "\n",
            "\n    ",
          ),
        ).unprepared;
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, formatSql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("binds every migration-060 object to MAIN despite TEMP and attached shadows", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-evaluation-main-shadow-",
      });
      const filename = path.join(directory, "state.sqlite");
      const attachedFilename = path.join(directory, "attached.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(context, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      yield* sql`CREATE TEMP TABLE orchestration_events AS SELECT * FROM main.orchestration_events WHERE 0`;
      for (const table of [
        "agent_control_verification_evaluation_evidence",
        "agent_control_verification_evaluation_receipts",
        "agent_control_verification_evaluation_markers",
      ]) {
        yield* sql.unsafe(`CREATE TEMP TABLE ${table} (shadow_value TEXT)`).unprepared;
      }
      yield* sql.unsafe(`
        CREATE UNIQUE INDEX temp.${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}
        ON orchestration_events(event_id)
      `).unprepared;
      yield* sql.unsafe(`
        CREATE INDEX temp.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
        ON orchestration_events(event_id)
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TEMP TRIGGER ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER}
        BEFORE INSERT ON orchestration_events BEGIN SELECT 1; END
      `).unprepared;

      yield* sql`ATTACH DATABASE ${attachedFilename} AS migration060_shadow`;
      yield* sql`
        CREATE TABLE migration060_shadow.orchestration_events
        AS SELECT * FROM main.orchestration_events WHERE 0
      `;
      for (const table of [
        "agent_control_verification_evaluation_evidence",
        "agent_control_verification_evaluation_receipts",
        "agent_control_verification_evaluation_markers",
      ]) {
        yield* sql.unsafe(`CREATE TABLE migration060_shadow.${table} (shadow_value TEXT)`)
          .unprepared;
      }
      yield* sql.unsafe(`
        CREATE UNIQUE INDEX migration060_shadow.${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}
        ON orchestration_events(event_id)
      `).unprepared;
      yield* sql.unsafe(`
        CREATE INDEX migration060_shadow.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
        ON orchestration_events(event_id)
      `).unprepared;
      yield* sql.unsafe(`
        CREATE TRIGGER migration060_shadow.${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER}
        BEFORE INSERT ON orchestration_events BEGIN SELECT 1; END
      `).unprepared;

      const schemaSnapshot = (schema: "temp" | "migration060_shadow") =>
        sql.unsafe<Record<string, unknown>>(
          `SELECT type, name, tbl_name AS "tableName", sql
           FROM ${schema}.sqlite_schema ORDER BY type, name`,
        );
      const tempBefore = yield* schemaSnapshot("temp");
      const attachedBefore = yield* schemaSnapshot("migration060_shadow");
      const mainBefore = yield* sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;

      const faulted = yield* Effect.exit(
        sql.withTransaction(
          makeMigration060("after-install").pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        ),
      );
      assert.isTrue(Exit.isFailure(faulted));
      assert.deepStrictEqual(
        yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `,
        mainBefore,
      );
      assert.deepStrictEqual(yield* schemaSnapshot("temp"), tempBefore);
      assert.deepStrictEqual(yield* schemaSnapshot("migration060_shadow"), attachedBefore);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
        [],
      );

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[60, "AgentControlVerificationEvaluation"] as const],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
        [{ migration_id: 60 }],
      );
      assert.deepStrictEqual(yield* schemaSnapshot("temp"), tempBefore);
      assert.deepStrictEqual(yield* schemaSnapshot("migration060_shadow"), attachedBefore);
      assert.deepStrictEqual(
        yield* sql<{ readonly type: string; readonly name: string; readonly tableName: string }>`
          SELECT type, name, tbl_name AS "tableName"
          FROM main.sqlite_schema
          WHERE name IN (
            'agent_control_verification_evaluation_evidence',
            'agent_control_verification_evaluation_receipts',
            'agent_control_verification_evaluation_markers',
            'agent_control_orchestration_event_storage_validate',
            'agent_control_orchestration_json_storage_validate',
            'agent_control_verification_result_source_seal_validate',
            'agent_control_verification_result_fragment_structure_validate',
            'agent_control_verification_result_capture_validate',
            'agent_control_verification_result_post_seal_reject',
            ${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX},
            ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX},
            ${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER}
          ) ORDER BY type, name
        `,
        [
          {
            type: "index",
            name: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
            tableName: "orchestration_events",
          },
          {
            type: "index",
            name: ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX,
            tableName: "orchestration_events",
          },
          {
            type: "table",
            name: "agent_control_verification_evaluation_evidence",
            tableName: "agent_control_verification_evaluation_evidence",
          },
          {
            type: "table",
            name: "agent_control_verification_evaluation_markers",
            tableName: "agent_control_verification_evaluation_markers",
          },
          {
            type: "table",
            name: "agent_control_verification_evaluation_receipts",
            tableName: "agent_control_verification_evaluation_receipts",
          },
          {
            type: "trigger",
            name: "agent_control_orchestration_event_storage_validate",
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: "agent_control_orchestration_json_storage_validate",
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: "agent_control_verification_result_capture_validate",
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: "agent_control_verification_result_fragment_structure_validate",
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: "agent_control_verification_result_post_seal_reject",
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_TRIGGER,
            tableName: "orchestration_events",
          },
          {
            type: "trigger",
            name: "agent_control_verification_result_source_seal_validate",
            tableName: "orchestration_events",
          },
        ],
      );

      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql.unsafe(`
            INSERT INTO main.orchestration_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
              command_id, causation_event_id, correlation_id, actor_kind,
              payload_json, metadata_json
            ) VALUES (
              X'626C6F62', 'project', 'migration-060-main-shadow-stream', 1,
              'project.created', '2026-08-27T12:00:00.000Z', NULL, NULL, NULL,
              'server', '{}', '{}'
            )
          `),
          ),
        ),
      );
      assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
      assert.deepStrictEqual(yield* sql`PRAGMA main.integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rolls back migration 060 on same-name MAIN objects across SQLite object types", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-main-name-collision-",
      });

      for (const variant of [
        {
          name: "trigger-named-like-index",
          install: `CREATE TRIGGER main.idx_agent_control_verification_evaluation_provider_turn
            BEFORE INSERT ON orchestration_events BEGIN SELECT 1; END`,
          remove: "DROP TRIGGER main.idx_agent_control_verification_evaluation_provider_turn",
        },
        {
          name: "case-folded-trigger-named-like-index",
          install: `CREATE TRIGGER main.IDX_AGENT_CONTROL_VERIFICATION_EVALUATION_PROVIDER_TURN
            BEFORE INSERT ON orchestration_events BEGIN SELECT 1; END`,
          remove: "DROP TRIGGER main.IDX_AGENT_CONTROL_VERIFICATION_EVALUATION_PROVIDER_TURN",
        },
        {
          name: "index-named-like-trigger",
          install: `CREATE INDEX main.agent_control_verification_result_capture_validate
            ON orchestration_events(event_id)`,
          remove: "DROP INDEX main.agent_control_verification_result_capture_validate",
        },
        {
          name: "view-named-like-table",
          install: `CREATE VIEW main.agent_control_verification_evaluation_evidence
            AS SELECT event_id FROM orchestration_events`,
          remove: "DROP VIEW main.agent_control_verification_evaluation_evidence",
        },
        {
          name: "trigger-on-wrong-table",
          install: `CREATE TRIGGER main.agent_control_verification_evaluation_evidence_validate
            BEFORE INSERT ON orchestration_command_receipts BEGIN SELECT 1; END`,
          remove: "DROP TRIGGER main.agent_control_verification_evaluation_evidence_validate",
        },
      ] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const filename = path.join(directory, `${variant.name}.sqlite`);
            const collisionScope = yield* Scope.make("sequential");
            yield* Effect.addFinalizer(() => Scope.close(collisionScope, Exit.void));
            const context = yield* Layer.buildWithScope(
              NodeSqliteClient.layer({ filename }),
              collisionScope,
            );
            const sql = Context.get(context, SqlClient.SqlClient);
            assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [
              { journal_mode: "wal" },
            ]);
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            yield* sql.unsafe(variant.install).unprepared;
            const schemaBefore = yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM main.sqlite_schema ORDER BY type, name
            `;

            const failed = yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
            );
            assert.isTrue(Exit.isFailure(failed), variant.name);
            assert.deepStrictEqual(
              yield* sql<Record<string, unknown>>`
                SELECT type, name, tbl_name AS "tableName", sql
                FROM main.sqlite_schema ORDER BY type, name
              `,
              schemaBefore,
              variant.name,
            );
            assert.deepStrictEqual(
              yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=60`,
              [],
              variant.name,
            );
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], variant.name);
            assert.deepStrictEqual(
              yield* sql`PRAGMA integrity_check`,
              [{ integrity_check: "ok" }],
              variant.name,
            );

            yield* Scope.close(collisionScope, Exit.void);
            const retryScope = yield* Scope.make("sequential");
            yield* Effect.addFinalizer(() => Scope.close(retryScope, Exit.void));
            const retryContext = yield* Layer.buildWithScope(
              NodeSqliteClient.layer({ filename }),
              retryScope,
            );
            const retrySql = Context.get(retryContext, SqlClient.SqlClient);
            yield* retrySql`PRAGMA foreign_keys = ON`;
            assert.lengthOf(
              yield* retrySql`
                SELECT type, name FROM main.sqlite_schema
                WHERE lower(name) IN (
                  'idx_agent_control_verification_evaluation_provider_turn',
                  'agent_control_verification_result_capture_validate',
                  'agent_control_verification_evaluation_evidence',
                  'agent_control_verification_evaluation_evidence_validate'
                )
              `,
              1,
              `${variant.name}-preserved-after-restart`,
            );
            yield* retrySql.unsafe(variant.remove).unprepared;
            assert.deepStrictEqual(
              yield* retrySql`
                SELECT type, name FROM main.sqlite_schema
                WHERE lower(name) IN (
                  'idx_agent_control_verification_evaluation_provider_turn',
                  'agent_control_verification_result_capture_validate',
                  'agent_control_verification_evaluation_evidence',
                  'agent_control_verification_evaluation_evidence_validate'
                )
              `,
              [],
              `${variant.name}-removed`,
            );
            assert.deepStrictEqual(
              yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, retrySql),
              ),
              [[60, "AgentControlVerificationEvaluation"]],
              variant.name,
            );
          }),
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("audits the complete command-id expression index structure", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-command-index-audit-",
      });
      for (const variant of [
        {
          name: "wrong-expression",
          replacement: `CREATE INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            ON orchestration_events(CAST(command_id AS TEXT), sequence)
            WHERE command_id IS NOT NULL`,
        },
        {
          name: "swapped-keys",
          replacement: `CREATE INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            ON orchestration_events(sequence, CAST(command_id AS BLOB))
            WHERE command_id IS NOT NULL`,
        },
        {
          name: "wrong-predicate",
          replacement: `CREATE INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            ON orchestration_events(CAST(command_id AS BLOB), sequence)
            WHERE command_id != ''`,
        },
        {
          name: "additional-key",
          replacement: `CREATE INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            ON orchestration_events(CAST(command_id AS BLOB), sequence, event_id)
            WHERE command_id IS NOT NULL`,
        },
        {
          name: "wrong-collation",
          replacement: `CREATE INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            ON orchestration_events(CAST(command_id AS BLOB) COLLATE NOCASE, sequence)
            WHERE command_id IS NOT NULL`,
        },
        {
          name: "same-name-view",
          replacement: `CREATE VIEW main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}
            AS SELECT sequence FROM orchestration_events`,
        },
      ] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const scope = yield* Scope.make("sequential");
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
            const context = yield* Layer.buildWithScope(
              NodeSqliteClient.layer({ filename: path.join(directory, `${variant.name}.sqlite`) }),
              scope,
            );
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            const schema059 = yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM main.sqlite_schema ORDER BY type, name
            `;
            const failed = yield* Effect.exit(
              sql.withTransaction(
                makeMigration060(undefined, {
                  beforeMainAudit: (auditSql) =>
                    Effect.gen(function* () {
                      yield* auditSql.unsafe(
                        `DROP INDEX main.${ORCHESTRATION_COMMAND_ID_BYTES_SEQUENCE_INDEX}`,
                      ).unprepared;
                      yield* auditSql.unsafe(variant.replacement).unprepared;
                    }).pipe(Effect.orDie),
                }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
              ),
            );
            assert.isTrue(Exit.isFailure(failed), variant.name);
            assert.deepStrictEqual(
              yield* sql<Record<string, unknown>>`
                SELECT type, name, tbl_name AS "tableName", sql
                FROM main.sqlite_schema ORDER BY type, name
              `,
              schema059,
              variant.name,
            );
            assert.deepStrictEqual(
              yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
              [],
              variant.name,
            );
            assert.deepStrictEqual(
              yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
              [[60, "AgentControlVerificationEvaluation"]],
              `${variant.name}-retry`,
            );
          }),
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails closed when a migration-060 MAIN object name is already foreign", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-evaluation-main-object-conflict-",
      });
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(
        NodeSqliteClient.layer({ filename: path.join(directory, "state.sqlite") }),
        scope,
      );
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      yield* sql`
        CREATE TABLE main.agent_control_verification_evaluation_evidence (
          wrong_shape TEXT
        )
      `;
      const failed = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM pragma_table_info('agent_control_verification_evaluation_evidence', 'main')`,
        [{ name: "wrong_shape" }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM main.sqlite_schema
          WHERE name='agent_control_verification_evaluation_receipts'
        `,
        [],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails migration 060 before mutation when its SQLite UDF protocol diverges", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-udf-preflight-",
      });

      for (const mode of [
        "missing",
        "fatal-divergent",
        "json-always-success",
        "json-always-fail",
        "json-selective-divergent",
        "protocol-divergent",
        "authority-route-divergent",
        "authority-history-divergent",
        "authority-history-text-storage",
        "authority-route-wrong-arity",
        "membership-route-divergent",
        "membership-none-divergent",
        "membership-invalid-divergent",
        "membership-history-divergent",
        "membership-history-text-storage",
        "membership-route-wrong-arity",
      ] as const) {
        const filename = path.join(directory, `${mode}.sqlite`);
        const legacyEventId = `migration-060-${mode}-legacy-event`;
        const legacyStreamId = `migration-060-${mode}-legacy-stream`;
        const legacyProjectId = `migration-060-${mode}-legacy-project`;
        const membershipHistoryTextStorage = mode === "membership-history-text-storage";
        const legacyAggregateKind = membershipHistoryTextStorage ? "thread" : "project";
        const legacyEventType = membershipHistoryTextStorage ? "thread.created" : "project.created";
        const legacyPayload = membershipHistoryTextStorage
          ? threadCreatedPayload(legacyStreamId, legacyProjectId, "2026-08-26T08:00:00.000Z")
          : historicalProjectCreatedPayload(legacyStreamId, "2026-08-26T08:00:00.000Z");
        const unregisteredScope = yield* Scope.make("sequential");
        const unregisteredContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layerTest({
            filename,
            _testHooks: {
              registerFunctions: (database) => {
                if (mode === "missing") return;
                if (mode === "authority-route-wrong-arity") {
                  registerMigration060FunctionsExcept(
                    database,
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                  );
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                    { deterministic: true },
                    (_eventType, _payload) => ORCHESTRATION_EVENT_ROUTE_INVALID,
                  );
                  return;
                }
                if (mode === "membership-route-wrong-arity") {
                  registerMigration060FunctionsExcept(
                    database,
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                  );
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (_eventType, _payload) => ORCHESTRATION_EVENT_ROUTE_INVALID,
                  );
                  return;
                }
                NodeSqliteClient.registerNodeSqliteFunctions(database);
                if (mode === "fatal-divergent")
                  database.function("t3_fatal_utf8", { deterministic: true }, (_value) => 1);
                if (mode === "json-always-success")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
                    { deterministic: true },
                    (_eventType, _payload, _metadata) => 1,
                  );
                if (mode === "json-always-fail")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
                    { deterministic: true },
                    (_eventType, _payload, _metadata) => 0,
                  );
                if (mode === "json-selective-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      metadata instanceof Uint8Array &&
                      Buffer.from(metadata)
                        .toString("utf8")
                        .startsWith('{"verificationResultCapture"')
                        ? 1
                        : SqliteFunctions.sqliteOrchestrationEventJsonStorage(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "protocol-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_JSON_STORAGE_PROTOCOL_FUNCTION,
                    { deterministic: true },
                    () => "divergent-protocol",
                  );
                if (mode === "authority-route-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      eventType instanceof Uint8Array &&
                      Buffer.from(eventType).toString("utf8") === "thread.created"
                        ? orchestrationEventAuthorityRouteBytes("thread", "wrong-index-route")
                        : SqliteFunctions.sqliteOrchestrationEventAuthorityRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "authority-history-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      eventType instanceof Uint8Array &&
                      Buffer.from(eventType).toString("utf8") === "project.created"
                        ? orchestrationEventAuthorityRouteBytes("project", "wrong-history-route")
                        : SqliteFunctions.sqliteOrchestrationEventAuthorityRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "authority-history-text-storage")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) => {
                      const route = SqliteFunctions.sqliteOrchestrationEventAuthorityRoute(
                        eventType,
                        payload,
                        metadata,
                      );
                      return eventType instanceof Uint8Array &&
                        Buffer.from(eventType).toString("utf8") === "project.created"
                        ? Buffer.from(route).toString("utf8")
                        : route;
                    },
                  );
                if (mode === "membership-route-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      eventType instanceof Uint8Array &&
                      Buffer.from(eventType).toString("utf8") === "thread.created"
                        ? orchestrationEventProjectMembershipRouteBytes("wrong-index-route")
                        : SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "membership-none-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      eventType instanceof Uint8Array &&
                      Buffer.from(eventType).toString("utf8") === "project.deleted"
                        ? orchestrationEventProjectMembershipRouteBytes("wrong-index-route")
                        : SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "membership-invalid-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      payload instanceof Uint8Array && payload[0] === 0x80
                        ? ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_NONE
                        : SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "membership-history-divergent")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) =>
                      eventType instanceof Uint8Array &&
                      Buffer.from(eventType).toString("utf8") === "project.created"
                        ? orchestrationEventProjectMembershipRouteBytes("wrong-history-route")
                        : SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                            eventType,
                            payload,
                            metadata,
                          ),
                  );
                if (mode === "membership-history-text-storage")
                  database.function(
                    SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                    { deterministic: true },
                    (eventType, payload, metadata) => {
                      const route = SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                        eventType,
                        payload,
                        metadata,
                      );
                      return eventType instanceof Uint8Array &&
                        Buffer.from(eventType).toString("utf8") === "thread.created" &&
                        payload instanceof Uint8Array &&
                        Buffer.from(payload).toString("utf8") === legacyPayload
                        ? Buffer.from(route).toString("utf8")
                        : route;
                    },
                  );
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
            ${legacyEventId}, ${legacyAggregateKind}, ${legacyStreamId}, 0, ${legacyEventType},
            '2026-08-26T08:00:00.000Z', NULL, NULL, NULL, 'server',
            ${legacyPayload}, '{}'
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
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], mode);
        assert.deepStrictEqual(
          yield* sql`PRAGMA integrity_check`,
          [{ integrity_check: "ok" }],
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
            WHERE event_id=${legacyEventId}
          `,
          [{ streamVersion: 0 }],
          mode,
        );
        if (mode === "authority-history-text-storage") {
          assert.deepStrictEqual(
            yield* retrySql<{ readonly eventId: string }>`
              SELECT event_id AS "eventId" FROM main.orchestration_events
              WHERE t3_orchestration_event_authority_route(
                CAST(event_type AS BLOB), CAST(payload_json AS BLOB),
                CAST(metadata_json AS BLOB)
              ) = ${orchestrationEventAuthorityRouteBytes("project", legacyStreamId)}
            `,
            [{ eventId: legacyEventId }],
            mode,
          );
        }
        if (mode === "membership-history-text-storage") {
          assert.deepStrictEqual(
            yield* retrySql<{ readonly eventId: string }>`
              SELECT event_id AS "eventId" FROM main.orchestration_events
              WHERE t3_orchestration_event_project_membership_route(
                CAST(event_type AS BLOB), CAST(payload_json AS BLOB),
                CAST(metadata_json AS BLOB)
              ) = ${orchestrationEventProjectMembershipRouteBytes(legacyProjectId)}
            `,
            [{ eventId: legacyEventId }],
            mode,
          );
        }
        assert.deepStrictEqual(yield* retrySql`PRAGMA foreign_key_check`, [], mode);
        assert.deepStrictEqual(
          yield* retrySql`PRAGMA integrity_check`,
          [{ integrity_check: "ok" }],
          mode,
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects non-BLOB route UDF results at installed migration-060 write boundaries", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-route-write-boundary-",
      });
      const occurredAt = "2026-08-26T08:00:00.000Z";

      for (const mode of ["authority", "membership"] as const) {
        const filename = path.join(directory, `${mode}.sqlite`);
        const insertEventId = `route-storage-${mode}-insert`;
        const insertStreamId = `route-storage-${mode}-insert-stream`;
        const updateEventId = `route-storage-${mode}-update`;
        const updateStreamId = `route-storage-${mode}-update-stream`;
        const projectId = `route-storage-${mode}-project`;
        const targetEventType = mode === "authority" ? "project.created" : "thread.created";
        const targetAggregateKind = mode === "authority" ? "project" : "thread";
        const insertPayload =
          mode === "authority"
            ? historicalProjectCreatedPayload(insertStreamId, occurredAt)
            : threadCreatedPayload(insertStreamId, projectId, occurredAt);
        const updatePayload =
          mode === "authority"
            ? historicalProjectCreatedPayload(updateStreamId, occurredAt)
            : threadCreatedPayload(updateStreamId, projectId, occurredAt);
        const seedEventType = mode === "authority" ? "project.deleted" : "thread.deleted";
        const seedPayload = encodeUnknownJson(
          mode === "authority"
            ? { projectId: updateStreamId, deletedAt: occurredAt }
            : { threadId: updateStreamId, deletedAt: occurredAt },
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            yield* sql`
              INSERT INTO main.orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
                command_id, causation_event_id, correlation_id, actor_kind,
                payload_json, metadata_json
              ) VALUES (
                ${updateEventId}, ${targetAggregateKind}, ${updateStreamId}, 1,
                ${seedEventType}, ${occurredAt}, NULL, NULL, NULL, 'server', ${seedPayload}, '{}'
              )
            `;
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(
              NodeSqliteClient.layerTest({
                filename,
                _testHooks: {
                  registerFunctions: (database) => {
                    NodeSqliteClient.registerNodeSqliteFunctions(database);
                    if (mode === "authority") {
                      database.function(
                        SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_AUTHORITY_ROUTE_FUNCTION,
                        { deterministic: true },
                        (eventType, payload, metadata) => {
                          const route = SqliteFunctions.sqliteOrchestrationEventAuthorityRoute(
                            eventType,
                            payload,
                            metadata,
                          );
                          return eventType instanceof Uint8Array &&
                            Buffer.from(eventType).toString("utf8") === targetEventType
                            ? Buffer.from(route).toString("utf8")
                            : route;
                        },
                      );
                    } else {
                      database.function(
                        SqliteFunctions.SQLITE_ORCHESTRATION_EVENT_PROJECT_MEMBERSHIP_ROUTE_FUNCTION,
                        { deterministic: true },
                        (eventType, payload, metadata) => {
                          const route =
                            SqliteFunctions.sqliteOrchestrationEventProjectMembershipRoute(
                              eventType,
                              payload,
                              metadata,
                            );
                          return eventType instanceof Uint8Array &&
                            Buffer.from(eventType).toString("utf8") === targetEventType
                            ? Buffer.from(route).toString("utf8")
                            : route;
                        },
                      );
                    }
                  },
                },
              }),
            );
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* sql`PRAGMA foreign_keys = ON`;
            const expectedRoute =
              mode === "authority"
                ? orchestrationEventAuthorityRouteBytes("project", insertStreamId)
                : orchestrationEventProjectMembershipRouteBytes(projectId);
            const routeProbe =
              mode === "authority"
                ? yield* sql<{
                    readonly storageClass: string;
                    readonly routeHex: string;
                  }>`
                    SELECT typeof(t3_orchestration_event_authority_route(
                      CAST(${targetEventType} AS BLOB), CAST(${insertPayload} AS BLOB),
                      CAST('{}' AS BLOB)
                    )) AS "storageClass",
                    hex(t3_orchestration_event_authority_route(
                      CAST(${targetEventType} AS BLOB), CAST(${insertPayload} AS BLOB),
                      CAST('{}' AS BLOB)
                    )) AS "routeHex"
                  `
                : yield* sql<{
                    readonly storageClass: string;
                    readonly routeHex: string;
                  }>`
                    SELECT typeof(t3_orchestration_event_project_membership_route(
                      CAST(${targetEventType} AS BLOB), CAST(${insertPayload} AS BLOB),
                      CAST('{}' AS BLOB)
                    )) AS "storageClass",
                    hex(t3_orchestration_event_project_membership_route(
                      CAST(${targetEventType} AS BLOB), CAST(${insertPayload} AS BLOB),
                      CAST('{}' AS BLOB)
                    )) AS "routeHex"
                  `;
            assert.deepStrictEqual(routeProbe, [
              {
                storageClass: "text",
                routeHex: Buffer.from(expectedRoute).toString("hex").toUpperCase(),
              },
            ]);
            const schemaBefore = yield* sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM main.sqlite_schema ORDER BY type, name
            `;
            const dataBefore = yield* sql<Record<string, unknown>>`
              SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
                stream_id AS "streamId", stream_version AS "streamVersion",
                event_type AS "eventType", payload_json AS "payload", metadata_json AS "metadata"
              FROM main.orchestration_events ORDER BY sequence
            `;
            const changesBefore = yield* sql<{ readonly changes: number }>`
              SELECT total_changes() AS changes
            `;
            const insertExit = yield* Effect.exit(sql`
              INSERT INTO main.orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
                command_id, causation_event_id, correlation_id, actor_kind,
                payload_json, metadata_json
              ) VALUES (
                ${insertEventId}, ${targetAggregateKind}, ${insertStreamId}, 1,
                ${targetEventType}, ${occurredAt}, NULL, NULL, NULL, 'server',
                ${insertPayload}, '{}'
              )
            `);
            assert.isTrue(Exit.isFailure(insertExit), `${mode}-insert`);
            const updateExit = yield* Effect.exit(sql`
              UPDATE main.orchestration_events
              SET event_type=${targetEventType}, payload_json=${updatePayload}
              WHERE event_id=${updateEventId}
            `);
            assert.isTrue(Exit.isFailure(updateExit), `${mode}-update`);
            assert.deepStrictEqual(
              yield* sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
              changesBefore,
              mode,
            );
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
                SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
                  stream_id AS "streamId", stream_version AS "streamVersion",
                  event_type AS "eventType", payload_json AS "payload",
                  metadata_json AS "metadata"
                FROM main.orchestration_events ORDER BY sequence
              `,
              dataBefore,
              mode,
            );
            assert.deepStrictEqual(
              yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
              [{ migration_id: 60 }],
              mode,
            );
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], mode);
            assert.deepStrictEqual(
              yield* sql`PRAGMA integrity_check`,
              [{ integrity_check: "ok" }],
              mode,
            );
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* sql`PRAGMA foreign_keys = ON`;
            assert.deepStrictEqual(
              yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
              [],
              mode,
            );
            yield* sql`
              INSERT INTO main.orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
                command_id, causation_event_id, correlation_id, actor_kind,
                payload_json, metadata_json
              ) VALUES (
                ${insertEventId}, ${targetAggregateKind}, ${insertStreamId}, 1,
                ${targetEventType}, ${occurredAt}, NULL, NULL, NULL, 'server',
                ${insertPayload}, '{}'
              )
            `;
            yield* sql`
              UPDATE main.orchestration_events
              SET event_type=${targetEventType}, payload_json=${updatePayload}
              WHERE event_id=${updateEventId}
            `;
            const routedRows =
              mode === "authority"
                ? [
                    ...(yield* sql<{ readonly eventId: string }>`
                      SELECT event_id AS "eventId" FROM main.orchestration_events
                      WHERE t3_orchestration_event_authority_route(
                        CAST(event_type AS BLOB), CAST(payload_json AS BLOB),
                        CAST(metadata_json AS BLOB)
                      ) = ${orchestrationEventAuthorityRouteBytes("project", insertStreamId)}
                    `),
                    ...(yield* sql<{ readonly eventId: string }>`
                      SELECT event_id AS "eventId" FROM main.orchestration_events
                      WHERE t3_orchestration_event_authority_route(
                        CAST(event_type AS BLOB), CAST(payload_json AS BLOB),
                        CAST(metadata_json AS BLOB)
                      ) = ${orchestrationEventAuthorityRouteBytes("project", updateStreamId)}
                    `),
                  ]
                : yield* sql<{ readonly eventId: string }>`
                    SELECT event_id AS "eventId" FROM main.orchestration_events
                    WHERE t3_orchestration_event_project_membership_route(
                      CAST(event_type AS BLOB), CAST(payload_json AS BLOB),
                      CAST(metadata_json AS BLOB)
                    ) = ${orchestrationEventProjectMembershipRouteBytes(projectId)}
                    ORDER BY sequence
                  `;
            assert.deepStrictEqual(
              routedRows.map((row) => row.eventId).toSorted(),
              [insertEventId, updateEventId].toSorted(),
              mode,
            );
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, [], mode);
            assert.deepStrictEqual(
              yield* sql`PRAGMA integrity_check`,
              [{ integrity_check: "ok" }],
              mode,
            );
          }),
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
        "legacy-sorted-key-permutation",
        "legacy-other-key-permutation",
        "legacy-top-level-extra",
        "legacy-ascii-whitespace",
        "legacy-event-type-name",
        "legacy-both-event-type-names",
        "legacy-correlation-missing",
        "legacy-correlation-wrong-type",
        "legacy-correlation-extra",
        "new-correlation-extra",
        "new-correlation-whitespace-item",
        "new-correlation-wrong-item-type",
        "duplicate-correlation-key",
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
            const validMetadata = `{"providerRuntimeMessage":{"runtimeEventId":${encodeUnknownJson(runtimeEventId)},"runtimeEventType":"item.completed","providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerTurnId":${encodeUnknownJson(providerTurnId)}}}`;
            const corruptMetadata =
              corruption === "legacy-sorted-key-permutation"
                ? `{"providerRuntimeMessage":{"providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerTurnId":${encodeUnknownJson(providerTurnId)},"runtimeEventId":${encodeUnknownJson(runtimeEventId)},"runtimeEventType":"item.completed"}}`
                : corruption === "legacy-other-key-permutation"
                  ? `{"providerRuntimeMessage":{"runtimeEventType":"item.completed","runtimeEventId":${encodeUnknownJson(runtimeEventId)},"providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerTurnId":${encodeUnknownJson(providerTurnId)}}}`
                  : corruption === "legacy-top-level-extra"
                    ? `{"providerRuntimeMessage":{"runtimeEventId":${encodeUnknownJson(runtimeEventId)},"runtimeEventType":"item.completed","providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerTurnId":${encodeUnknownJson(providerTurnId)}},"extra":true}`
                    : corruption === "legacy-ascii-whitespace"
                      ? `{ ${validMetadata.slice(1)}`
                      : corruption === "legacy-event-type-name"
                        ? encodeUnknownJson({
                            providerRuntimeMessage: {
                              runtimeEventId,
                              eventType: "item.completed",
                              providerInstanceId,
                              providerTurnId,
                            },
                          })
                        : corruption === "legacy-both-event-type-names"
                          ? encodeUnknownJson({
                              providerRuntimeMessage: {
                                runtimeEventId,
                                runtimeEventType: "item.completed",
                                eventType: "item.completed",
                                providerInstanceId,
                                providerTurnId,
                              },
                            })
                          : corruption === "legacy-correlation-missing"
                            ? encodeUnknownJson({
                                providerRuntimeMessage: {
                                  runtimeEventId,
                                  runtimeEventType: "item.completed",
                                  providerInstanceId,
                                },
                              })
                            : corruption === "legacy-correlation-wrong-type"
                              ? encodeUnknownJson({
                                  providerRuntimeMessage: {
                                    runtimeEventId,
                                    runtimeEventType: 1,
                                    providerInstanceId,
                                    providerTurnId,
                                  },
                                })
                              : corruption === "legacy-correlation-extra"
                                ? encodeUnknownJson({
                                    providerRuntimeMessage: {
                                      runtimeEventId,
                                      runtimeEventType: "item.completed",
                                      providerInstanceId,
                                      providerTurnId,
                                      unknown: "field",
                                    },
                                  })
                                : corruption === "new-correlation-extra"
                                  ? encodeUnknownJson({
                                      providerRuntimeMessage: {
                                        runtimeEventId,
                                        eventType: "item.completed",
                                        providerInstanceId,
                                        providerTurnId,
                                        providerItemId: null,
                                        unknown: "field",
                                      },
                                    })
                                  : corruption === "new-correlation-whitespace-item"
                                    ? encodeUnknownJson({
                                        providerRuntimeMessage: {
                                          runtimeEventId,
                                          eventType: "item.completed",
                                          providerInstanceId,
                                          providerTurnId,
                                          providerItemId: " item-space ",
                                        },
                                      })
                                    : corruption === "new-correlation-wrong-item-type"
                                      ? encodeUnknownJson({
                                          providerRuntimeMessage: {
                                            runtimeEventId,
                                            eventType: "item.completed",
                                            providerInstanceId,
                                            providerTurnId,
                                            providerItemId: 1,
                                          },
                                        })
                                      : corruption === "duplicate-correlation-key"
                                        ? `{"providerRuntimeMessage":{"runtimeEventId":${encodeUnknownJson(runtimeEventId)},"runtimeEventId":${encodeUnknownJson(`${runtimeEventId}-duplicate`)},"runtimeEventType":"item.completed","providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerTurnId":${encodeUnknownJson(providerTurnId)}}}`
                                        : validMetadata;
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
                       'server', ?, '{}')`,
                  )
                  .run(
                    `migration-060-history-control-${corruption}`,
                    `migration-060-history-control-stream-${corruption}`,
                    occurredAt,
                    historicalProjectCreatedPayload(
                      `migration-060-history-control-stream-${corruption}`,
                      occurredAt,
                    ),
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
                    corruptMetadata,
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

it.live("accepts only exact legacy or new provider correlations and preserves history bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-correlation-history-",
      });
      const occurredAt = "2026-08-26T08:00:00.000Z";

      for (const variant of [
        {
          mode: "legacy",
          metadata: HISTORICAL_PROVIDER_RUNTIME_METADATA,
          metadataHex: HISTORICAL_PROVIDER_RUNTIME_METADATA_HEX,
          providerInstanceId: "codex",
          providerTurnId: "turn-historical",
          runtimeEventId: "event-historical",
        },
        {
          mode: "legacy-item-null",
          metadata: HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_NULL_ITEM,
          metadataHex: HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_NULL_ITEM_HEX,
          providerInstanceId: "codex",
          providerTurnId: "turn-historical-item",
          runtimeEventId: "event-historical-item",
        },
        {
          mode: "legacy-item-text",
          metadata: HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_TEXT_ITEM,
          metadataHex: HISTORICAL_PROVIDER_RUNTIME_METADATA_WITH_TEXT_ITEM_HEX,
          providerInstanceId: "codex",
          providerTurnId: "turn-historical-item",
          runtimeEventId: "event-historical-item",
        },
        {
          mode: "legacy-item-capture",
          metadata: HISTORICAL_PROVIDER_RUNTIME_WITH_CAPTURE_METADATA,
          metadataHex: HISTORICAL_PROVIDER_RUNTIME_WITH_CAPTURE_METADATA_HEX,
          providerInstanceId: "codex",
          providerTurnId: "turn-historical-capture",
          runtimeEventId: "event-historical-capture",
        },
        { mode: "new", metadata: null, metadataHex: null },
      ] as const) {
        const mode = variant.mode;
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const context = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename: path.join(directory, `${mode}.sqlite`) }),
          scope,
        );
        const sql = Context.get(context, SqlClient.SqlClient);
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const threadId = `migration-060-correlation-${mode}-thread`;
        const providerInstanceId =
          "providerInstanceId" in variant
            ? variant.providerInstanceId
            : `migration-060-correlation-${mode}-provider`;
        const providerTurnId =
          "providerTurnId" in variant
            ? variant.providerTurnId
            : `migration-060-correlation-${mode}-turn`;
        const runtimeEventId =
          "runtimeEventId" in variant
            ? variant.runtimeEventId
            : `migration-060-correlation-${mode}-runtime`;
        const messageId = `assistant:correlation-${mode}`;
        const payload = encodeUnknownJson({
          threadId,
          messageId,
          role: "assistant",
          text: "historical bytes",
          turnId: providerTurnId,
          streaming: false,
          createdAt: occurredAt,
          updatedAt: occurredAt,
        });
        const metadata =
          variant.metadata ??
          `{"providerRuntimeMessage":{"eventType":"item.completed","providerInstanceId":${encodeUnknownJson(providerInstanceId)},"providerItemId":null,"providerTurnId":${encodeUnknownJson(providerTurnId)},"runtimeEventId":${encodeUnknownJson(runtimeEventId)}}}`;
        if (variant.metadataHex !== null) {
          assert.equal(Buffer.from(metadata, "utf8").toString("hex"), variant.metadataHex);
        }
        const commandId = `provider:${runtimeEventId}:message-complete:${messageId}`;
        yield* sql`
          INSERT INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            ${`migration-060-correlation-${mode}-event`}, 'thread', ${threadId}, 0,
            'thread.message-sent', ${occurredAt}, ${commandId}, NULL, ${commandId},
            'provider', ${payload}, ${metadata}
          )
        `;
        const readStoredRow = () => sql<Record<string, unknown>>`
          SELECT sequence,
            typeof(event_id) AS "eventIdType", hex(CAST(event_id AS BLOB)) AS "eventIdHex",
            typeof(aggregate_kind) AS "aggregateKindType",
            hex(CAST(aggregate_kind AS BLOB)) AS "aggregateKindHex",
            typeof(stream_id) AS "streamIdType", hex(CAST(stream_id AS BLOB)) AS "streamIdHex",
            typeof(stream_version) AS "streamVersionType",
            hex(CAST(stream_version AS BLOB)) AS "streamVersionHex",
            typeof(event_type) AS "eventTypeType",
            hex(CAST(event_type AS BLOB)) AS "eventTypeHex",
            typeof(occurred_at) AS "occurredAtType",
            hex(CAST(occurred_at AS BLOB)) AS "occurredAtHex",
            typeof(command_id) AS "commandIdType",
            hex(CAST(command_id AS BLOB)) AS "commandIdHex",
            typeof(causation_event_id) AS "causationEventIdType",
            hex(CAST(causation_event_id AS BLOB)) AS "causationEventIdHex",
            typeof(correlation_id) AS "correlationIdType",
            hex(CAST(correlation_id AS BLOB)) AS "correlationIdHex",
            typeof(actor_kind) AS "actorKindType",
            hex(CAST(actor_kind AS BLOB)) AS "actorKindHex",
            typeof(payload_json) AS "payloadType",
            hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            typeof(metadata_json) AS "metadataType",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events WHERE stream_id=${threadId}
        `;
        const before = yield* readStoredRow();
        if (variant.metadataHex !== null) {
          assert.lengthOf(before, 1);
          assert.equal(before[0]!.payloadType, "text");
          assert.equal(
            before[0]!.payloadHex,
            Buffer.from(payload, "utf8").toString("hex").toUpperCase(),
          );
          assert.equal(before[0]!.metadataType, "text");
          assert.equal(before[0]!.metadataHex, variant.metadataHex.toUpperCase());
        }
        if (variant.metadataHex !== null) {
          const rollback = yield* Effect.exit(
            sql.withTransaction(
              makeMigration060("after-copy").pipe(Effect.provideService(SqlClient.SqlClient, sql)),
            ),
          );
          assert.isTrue(Exit.isFailure(rollback));
          assert.deepStrictEqual(yield* readStoredRow(), before);
          assert.deepStrictEqual(
            yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
            [],
          );
        }
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
          mode,
        );
        assert.deepStrictEqual(yield* readStoredRow(), before, mode);

        const exactNewCorrelation = {
          runtimeEventId: `${runtimeEventId}-post-060`,
          eventType: "item.completed",
          providerInstanceId,
          providerTurnId,
          providerItemId: null,
        } as const;
        const invalidMetadata: ReadonlyArray<readonly [string, unknown]> = [
          [
            "missing-item-id",
            encodeUnknownJson({
              providerRuntimeMessage: {
                runtimeEventId: exactNewCorrelation.runtimeEventId,
                eventType: exactNewCorrelation.eventType,
                providerInstanceId,
                providerTurnId,
              },
            }),
          ],
          [
            "unknown-field",
            encodeUnknownJson({
              providerRuntimeMessage: { ...exactNewCorrelation, unknown: "field" },
            }),
          ],
          [
            "ascii-space",
            encodeUnknownJson({
              providerRuntimeMessage: { ...exactNewCorrelation, providerItemId: " item " },
            }),
          ],
          [
            "tabs-newlines",
            encodeUnknownJson({
              providerRuntimeMessage: { ...exactNewCorrelation, providerItemId: "\titem\n" },
            }),
          ],
          [
            "unicode-space",
            encodeUnknownJson({
              providerRuntimeMessage: { ...exactNewCorrelation, providerItemId: "\u00a0item" },
            }),
          ],
          [
            "wrong-type",
            encodeUnknownJson({
              providerRuntimeMessage: { ...exactNewCorrelation, providerItemId: 1 },
            }),
          ],
          [
            "duplicate-key",
            `{"providerRuntimeMessage":{"runtimeEventId":"${exactNewCorrelation.runtimeEventId}","eventType":"item.completed","providerInstanceId":"${providerInstanceId}","providerTurnId":"${providerTurnId}","providerItemId":null,"providerItemId":"duplicate"}}`,
          ],
          ["blob", Buffer.from(encodeUnknownJson({ providerRuntimeMessage: exactNewCorrelation }))],
          ["integer", 1],
        ];
        for (const [name, candidateMetadata] of invalidMetadata) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(sql`
                INSERT INTO main.orchestration_events (
                  event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
                  command_id, causation_event_id, correlation_id, actor_kind,
                  payload_json, metadata_json
                ) VALUES (
                  ${`migration-060-correlation-${mode}-${name}`}, 'thread', ${threadId}, 1,
                  'thread.message-sent', ${occurredAt}, ${`${commandId}-${name}`}, NULL,
                  ${`${commandId}-${name}`}, 'provider', ${payload}, ${candidateMetadata}
                )
              `),
            ),
            `${mode}-${name}`,
          );
        }
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM main.orchestration_events
            WHERE stream_id=${threadId}`,
          [{ count: 1 }],
          mode,
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects every migration-060-only capture or seal in schema 059 before mutation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-v2-history-reject-",
      });
      const filename = path.join(directory, "state.sqlite");
      const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      assert.deepStrictEqual(
        yield* sql`SELECT count(*) AS count FROM agent_control_verification_deliveries`,
        [{ count: 0 }],
      );
      const schema059 = yield* sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
      const occurredAt = "2026-08-26T08:00:00.000Z";

      for (const variant of [
        "orphan-capture",
        "foreign-delivery",
        "foreign-handoff",
        "foreign-thread",
        "foreign-provider-turn",
        "divergent-result-authority",
        "v2-seal",
      ] as const) {
        const streamId = `migration-060-v2-stream-${variant}`;
        const payloadThreadId = variant === "foreign-thread" ? `${streamId}-foreign` : streamId;
        const providerTurnId = `migration-060-v2-turn-${variant}`;
        const captureTurnId =
          variant === "foreign-provider-turn" ? `${providerTurnId}-foreign` : providerTurnId;
        const runtimeEventId = `migration-060-v2-runtime-${variant}`;
        const messageId = `assistant:migration-060-v2-${variant}`;
        const eventId = `migration-060-v2-event-${variant}`;
        const isSeal = variant === "v2-seal";
        const payload = isSeal
          ? encodeUnknownJson({
              threadId: streamId,
              session: {
                threadId: streamId,
                status: "ready",
                providerName: "codex",
                providerInstanceId: `migration-060-v2-provider-${variant}`,
                runtimeMode: "approval-required",
                activeTurnId: null,
                lastError: null,
                updatedAt: occurredAt,
              },
            })
          : encodeUnknownJson({
              threadId: payloadThreadId,
              messageId,
              turnId: captureTurnId,
              fragment: {
                kind: "delta",
                text: "evidence",
                byteLength: 8,
                cumulativeByteLength: 8,
              },
              createdAt: occurredAt,
            });
        const metadata = isSeal
          ? encodeUnknownJson({
              providerRuntimeLifecycle: {
                runtimeEventId,
                runtimeEventType: "turn.completed",
                providerInstanceId: `migration-060-v2-provider-${variant}`,
                providerTurnId,
                providerState: "completed",
              },
              verificationResultSource: {
                schemaVersion: 1,
                handoffId: `migration-060-v2-handoff-${variant}`,
                providerDeliveryId: `migration-060-v2-delivery-${variant}`,
                providerInstanceId: `migration-060-v2-provider-${variant}`,
                providerTurnId,
                resultSchemaFingerprint: "a".repeat(64),
                sourceDisposition: "missing",
                finalMessageId: null,
                sourceEventId: null,
                outputDigest: null,
                outputByteLength: 0,
              },
            })
          : encodeUnknownJson({
              providerRuntimeMessage: {
                runtimeEventId,
                runtimeEventType: "content.delta",
                providerInstanceId: `migration-060-v2-provider-${variant}`,
                providerTurnId,
              },
              verificationResultCapture: {
                schemaVersion: 1,
                disposition: "authority",
                handoffId:
                  variant === "foreign-handoff"
                    ? "migration-060-v2-handoff-foreign"
                    : `migration-060-v2-handoff-${variant}`,
                providerDeliveryId:
                  variant === "foreign-delivery"
                    ? "migration-060-v2-delivery-foreign"
                    : `migration-060-v2-delivery-${variant}`,
                providerInstanceId: `migration-060-v2-provider-${variant}`,
                providerTurnId,
                resultSchemaFingerprint:
                  variant === "divergent-result-authority" ? "b".repeat(64) : "a".repeat(64),
              },
            });
        const commandId = isSeal
          ? `provider:${runtimeEventId}:thread-session-set:00000000-0000-4000-8000-000000000060`
          : `provider:${runtimeEventId}:verification-result:${messageId}`;
        yield* sql`
          INSERT INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            ${eventId}, 'thread', ${streamId}, 1,
            ${isSeal ? "thread.session-set" : "thread.verification-result-fragment-captured"},
            ${occurredAt}, ${commandId}, NULL, ${commandId}, 'provider', ${payload}, ${metadata}
          )
        `;
        const bytesBefore = yield* sql<Record<string, unknown>>`
          SELECT sequence, typeof(event_id) AS "eventIdType",
            hex(CAST(event_id AS BLOB)) AS "eventIdHex",
            typeof(stream_version) AS "streamVersionType",
            hex(CAST(stream_version AS BLOB)) AS "streamVersionHex",
            hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events WHERE event_id=${eventId}
        `;
        const failed = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
        );
        assert.isTrue(Exit.isFailure(failed), variant);
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schema059,
          variant,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT sequence, typeof(event_id) AS "eventIdType",
              hex(CAST(event_id AS BLOB)) AS "eventIdHex",
              typeof(stream_version) AS "streamVersionType",
              hex(CAST(stream_version AS BLOB)) AS "streamVersionHex",
              hex(CAST(payload_json AS BLOB)) AS "payloadHex",
              hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
            FROM main.orchestration_events WHERE event_id=${eventId}
          `,
          bytesBefore,
          variant,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=60`,
          [],
          variant,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT name FROM main.sqlite_schema
            WHERE name LIKE '%rebuild_060%'
              OR name LIKE 'agent_control_verification_evaluation_%'
          `,
          [],
          variant,
        );
        yield* sql`DELETE FROM main.orchestration_events WHERE event_id=${eventId}`;
      }

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[60, "AgentControlVerificationEvaluation"]],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("validates exact schema-059 stream progression across all streams", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-stream-progression-",
      });
      const occurredAt = "2026-08-26T08:00:00.000Z";
      const open = (name: string) =>
        Effect.gen(function* () {
          const filename = path.join(directory, `${name}.sqlite`);
          const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
          const sql = Context.get(context, SqlClient.SqlClient);
          yield* sql`PRAGMA foreign_keys = ON`;
          yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          return sql;
        });
      const insertProjectEvent = (
        sql: SqlClient.SqlClient,
        input: {
          readonly eventId: string;
          readonly streamId: string;
          readonly streamVersionSql: string;
        },
      ) => {
        const payload = historicalProjectCreatedPayload(input.streamId, occurredAt);
        return sql.unsafe(`
          INSERT INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            '${input.eventId}', 'project', '${input.streamId}', ${input.streamVersionSql},
            'project.created', '${occurredAt}', NULL, NULL, NULL, 'server', '${payload}', '{}'
          )
        `);
      };

      for (const [name, versions] of [
        ["legacy-single", [0]],
        ["legacy-sequence", [0, 1, 2]],
        ["regular-single", [1]],
        ["regular-sequence", [1, 2, 3]],
      ] as const) {
        const sql = yield* open(`valid-${name}`);
        for (const [index, version] of versions.entries()) {
          yield* insertProjectEvent(sql, {
            eventId: `valid-${name}-${index}`,
            streamId: `valid-${name}`,
            streamVersionSql: String(version),
          });
        }
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
          name,
        );
      }

      const interleavedSql = yield* open("valid-interleaved");
      for (const [index, [streamId, streamVersion]] of [
        ["interleaved-a", 0],
        ["interleaved-b", 1],
        ["interleaved-a", 1],
        ["interleaved-b", 2],
        ["interleaved-a", 2],
      ].entries()) {
        yield* insertProjectEvent(interleavedSql, {
          eventId: `valid-interleaved-${index}`,
          streamId: streamId as string,
          streamVersionSql: String(streamVersion),
        });
      }
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, interleavedSql),
        ),
        [[60, "AgentControlVerificationEvaluation"]],
      );

      for (const [name, versions] of [
        ["later-zero", [1, 0]],
        ["duplicate-legacy", [0, 0]],
        ["duplicate-regular", [1, 1]],
        ["gap", [1, 3]],
        ["invalid-first", [2]],
        ["negative", [-1]],
      ] as const) {
        const sql = yield* open(`invalid-${name}`);
        const requiresConstraintNeutralFixture = new Set(versions).size !== versions.length;
        if (requiresConstraintNeutralFixture) {
          yield* sql`DROP INDEX main.idx_orch_events_stream_version`;
        }
        for (const [index, version] of versions.entries()) {
          yield* insertProjectEvent(sql, {
            eventId: `invalid-${name}-${index}`,
            streamId: `invalid-${name}`,
            streamVersionSql: String(version),
          });
        }
        const schemaBefore = yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `;
        const bytesBefore = yield* sql<Record<string, unknown>>`
          SELECT sequence, event_id AS "eventId", typeof(stream_version) AS "storage",
            hex(CAST(stream_version AS BLOB)) AS "bytes"
          FROM main.orchestration_events ORDER BY sequence
        `;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
            ),
          ),
          name,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          name,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT sequence, event_id AS "eventId", typeof(stream_version) AS "storage",
              hex(CAST(stream_version AS BLOB)) AS "bytes"
            FROM main.orchestration_events ORDER BY sequence
          `,
          bytesBefore,
          name,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE migration_id=60`,
          [],
          name,
        );
        for (const [index] of versions.entries()) {
          yield* sql`
            UPDATE main.orchestration_events SET stream_version=${index + 1}
            WHERE event_id=${`invalid-${name}-${index}`}
          `;
        }
        if (requiresConstraintNeutralFixture) {
          yield* sql`
            CREATE UNIQUE INDEX main.idx_orch_events_stream_version
            ON orchestration_events(aggregate_kind, stream_id, stream_version)
          `;
        }
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
          `${name}-retry`,
        );
      }

      for (const [name, storage] of [
        ["blob-version", "X'31'"],
        ["real-version", "1.5"],
      ] as const) {
        const sql = yield* open(`invalid-${name}`);
        yield* insertProjectEvent(sql, {
          eventId: `invalid-${name}`,
          streamId: `invalid-${name}`,
          streamVersionSql: storage,
        });
        const bytesBefore = yield* sql<Record<string, unknown>>`
          SELECT typeof(stream_version) AS storage,
            hex(CAST(stream_version AS BLOB)) AS bytes
          FROM main.orchestration_events WHERE event_id=${`invalid-${name}`}
        `;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 60 }).pipe(
                Effect.provideService(SqlClient.SqlClient, sql),
              ),
            ),
          ),
          name,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT typeof(stream_version) AS storage,
              hex(CAST(stream_version AS BLOB)) AS bytes
            FROM main.orchestration_events WHERE event_id=${`invalid-${name}`}
          `,
          bytesBefore,
          name,
        );
        yield* sql`
          UPDATE main.orchestration_events SET stream_version=1
          WHERE event_id=${`invalid-${name}`}
        `;
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
          [[60, "AgentControlVerificationEvaluation"]],
          `${name}-retry`,
        );
      }

      const boundarySql = yield* open("invalid-pagination-boundary");
      for (let index = 0; index < 65; index += 1) {
        yield* insertProjectEvent(boundarySql, {
          eventId: `boundary-${index}`,
          streamId: "boundary-stream",
          streamVersionSql: String(index === 64 ? 0 : index + 1),
        });
      }
      const boundaryBefore = yield* boundarySql<Record<string, unknown>>`
        SELECT sequence, stream_version AS "streamVersion"
        FROM main.orchestration_events ORDER BY sequence
      `;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 60 }).pipe(
              Effect.provideService(SqlClient.SqlClient, boundarySql),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(
        yield* boundarySql<Record<string, unknown>>`
          SELECT sequence, stream_version AS "streamVersion"
          FROM main.orchestration_events ORDER BY sequence
        `,
        boundaryBefore,
      );
      yield* boundarySql`
        UPDATE main.orchestration_events SET stream_version=65 WHERE event_id='boundary-64'
      `;
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, boundarySql),
        ),
        [[60, "AgentControlVerificationEvaluation"]],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("keyset-paginates large source payloads and rolls back corruption on every page", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-source-pages-",
      });
      const occurredAt = "2026-08-26T08:00:00.000Z";

      for (const corruptIndex of [0, 3, 6] as const) {
        const filename = path.join(directory, `page-${corruptIndex}.sqlite`);
        const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
        const sql = Context.get(context, SqlClient.SqlClient);
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const largeText = "x".repeat(128 * 1024);
        for (let index = 0; index < 7; index += 1) {
          const messageId = `migration-060-page-message-${corruptIndex}-${index}`;
          const commandId = `migration-060-page-command-${corruptIndex}-${index}`;
          const payload = encodeUnknownJson({
            threadId: `migration-060-page-stream-${corruptIndex}`,
            messageId,
            role: index === corruptIndex ? 7 : "assistant",
            text: `${index}:${largeText}`,
            turnId: null,
            streaming: false,
            createdAt: occurredAt,
            updatedAt: occurredAt,
          });
          yield* sql`
            INSERT INTO main.orchestration_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
              command_id, causation_event_id, correlation_id, actor_kind,
              payload_json, metadata_json
            ) VALUES (
              ${`migration-060-page-event-${corruptIndex}-${index}`}, 'thread',
              ${`migration-060-page-stream-${corruptIndex}`}, ${index + 1},
              'thread.message-sent', ${occurredAt}, ${commandId}, NULL, ${commandId},
              'client', ${payload}, '{}'
            )
          `;
        }
        const schemaBefore = yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `;
        const bytesBefore = yield* sql<Record<string, unknown>>`
          SELECT sequence, hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events ORDER BY sequence
        `;
        const pages: Array<{ readonly afterSequence: number; readonly rowCount: number }> = [];
        const failed = yield* Effect.exit(
          sql.withTransaction(
            makeMigration060(undefined, {
              sourcePreflightPageSize: 2,
              onSourcePreflightPage: (page) => pages.push(page),
            }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
          ),
        );
        assert.isTrue(Exit.isFailure(failed), String(corruptIndex));
        assert.isTrue(pages.every((page) => page.rowCount <= 2));
        assert.equal(
          pages.length,
          corruptIndex === 0 ? 1 : corruptIndex === 3 ? 2 : 4,
          String(corruptIndex),
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
        );
        assert.deepStrictEqual(
          yield* sql<Record<string, unknown>>`
            SELECT sequence, hex(CAST(payload_json AS BLOB)) AS "payloadHex",
              hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
            FROM main.orchestration_events ORDER BY sequence
          `,
          bytesBefore,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT name FROM main.sqlite_schema
            WHERE name LIKE '%rebuild_060%'
              OR name LIKE 'agent_control_verification_evaluation_%'
          `,
          [],
        );

        const repairedPayload = encodeUnknownJson({
          threadId: `migration-060-page-stream-${corruptIndex}`,
          messageId: `migration-060-page-message-${corruptIndex}-${corruptIndex}`,
          role: "assistant",
          text: `${corruptIndex}:${largeText}`,
          turnId: null,
          streaming: false,
          createdAt: occurredAt,
          updatedAt: occurredAt,
        });
        yield* sql`
          UPDATE main.orchestration_events SET payload_json=${repairedPayload}
          WHERE event_id=${`migration-060-page-event-${corruptIndex}-${corruptIndex}`}
        `;
        const retryPages: Array<{ readonly afterSequence: number; readonly rowCount: number }> = [];
        yield* sql.withTransaction(
          makeMigration060(undefined, {
            sourcePreflightPageSize: 2,
            onSourcePreflightPage: (page) => retryPages.push(page),
          }).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
        );
        assert.deepStrictEqual(
          retryPages.map((page) => page.rowCount),
          [2, 2, 2, 1, 0],
        );
        assert.isTrue(retryPages.every((page) => page.rowCount <= 2));
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rejects duplicate object keys safely in migration preflight and MAIN DDL", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-verification-duplicate-json-",
      });
      const filename = path.join(directory, "state.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(context, SqlClient.SqlClient);
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      const occurredAt = "2026-08-26T08:00:00.000Z";
      const duplicatePreflightPayload =
        '{"projectId":"duplicate-preflight","title":"first","title":"last","workspaceRoot":"/tmp/duplicate-preflight","defaultModelSelection":null,"scripts":[],"createdAt":"2026-08-26T08:00:00.000Z","updatedAt":"2026-08-26T08:00:00.000Z"}';
      yield* sql`
        INSERT INTO main.orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          'duplicate-preflight-event', 'project', 'duplicate-preflight', 0,
          'project.created', ${occurredAt}, NULL, NULL, NULL, 'server',
          ${duplicatePreflightPayload}, '{}'
        )
      `;
      const schema059 = yield* sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
      const bytes059 = yield* sql<Record<string, unknown>>`
        SELECT hex(CAST(payload_json AS BLOB)) AS "payloadHex",
          hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
        FROM main.orchestration_events WHERE event_id='duplicate-preflight-event'
      `;
      const preflightFailure = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
      );
      assert.isTrue(Exit.isFailure(preflightFailure));
      assert.deepStrictEqual(
        yield* sql<Record<string, unknown>>`
          SELECT type, name, tbl_name AS "tableName", sql
          FROM main.sqlite_schema ORDER BY type, name
        `,
        schema059,
      );
      assert.deepStrictEqual(
        yield* sql<Record<string, unknown>>`
          SELECT hex(CAST(payload_json AS BLOB)) AS "payloadHex",
            hex(CAST(metadata_json AS BLOB)) AS "metadataHex"
          FROM main.orchestration_events WHERE event_id='duplicate-preflight-event'
        `,
        bytes059,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM main.effect_sql_migrations WHERE migration_id=60`,
        [],
      );

      const repairedPayload = historicalProjectCreatedPayload("duplicate-preflight", occurredAt);
      yield* sql`UPDATE main.orchestration_events SET payload_json=${repairedPayload}
        WHERE event_id='duplicate-preflight-event'`;
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 60 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[60, "AgentControlVerificationEvaluation"]],
      );

      const validPayload = historicalProjectCreatedPayload("duplicate-ddl-valid", occurredAt);
      const insert = (suffix: string, payload: unknown, metadata: unknown = "{}") =>
        sql`
          INSERT INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            ${`duplicate-ddl-${suffix}`}, 'project', ${`duplicate-ddl-${suffix}`}, 1,
            'project.created', ${occurredAt}, NULL, NULL, NULL, 'server', ${payload}, ${metadata}
          )
        `;
      const assertRejectedWithoutChanges = Effect.fn("assertDuplicateJsonRejected")(function* (
        label: string,
        statement: Effect.Effect<unknown, SqlError>,
      ) {
        const before = yield* sql<{ readonly changes: number }>`SELECT total_changes() AS changes`;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)), label);
        assert.deepStrictEqual(
          yield* sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          before,
          label,
        );
      });

      for (const [label, payload, metadata] of [
        [
          "payload top-level",
          '{"projectId":"duplicate-ddl-top","projectId":"different","title":"Duplicate","workspaceRoot":"/tmp/duplicate","defaultModelSelection":null,"scripts":[],"createdAt":"2026-08-26T08:00:00.000Z","updatedAt":"2026-08-26T08:00:00.000Z"}',
          "{}",
        ],
        [
          "payload nested",
          '{"projectId":"duplicate-ddl-nested","title":"Duplicate","workspaceRoot":"/tmp/duplicate","defaultModelSelection":{"instanceId":"codex","model":"first","model":"last"},"scripts":[],"createdAt":"2026-08-26T08:00:00.000Z","updatedAt":"2026-08-26T08:00:00.000Z"}',
          "{}",
        ],
        ["metadata top-level", validPayload, '{"adapterKey":"codex","adapterKey":"other"}'],
        [
          "metadata nested",
          validPayload,
          '{"providerRuntimeMessage":{"runtimeEventId":"duplicate-runtime","eventType":"item.completed","providerInstanceId":"codex","providerInstanceId":"other","providerTurnId":"duplicate-turn","providerItemId":null}}',
        ],
        ["malformed payload", "{", "{}"],
        ["malformed metadata", validPayload, "{"],
        [
          "capture only metadata",
          validPayload,
          '{"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"duplicate-capture-handoff","providerDeliveryId":"duplicate-capture-delivery","providerInstanceId":"codex","providerTurnId":"duplicate-capture-turn","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}',
        ],
        [
          "presentation capture on project event",
          validPayload,
          '{"providerRuntimeMessage":{"runtimeEventId":"duplicate-capture-runtime","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"duplicate-capture-turn","providerItemId":null},"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"duplicate-capture-handoff","providerDeliveryId":"duplicate-capture-delivery","providerInstanceId":"codex","providerTurnId":"duplicate-capture-turn","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}',
        ],
      ] as const) {
        yield* assertRejectedWithoutChanges(
          label,
          insert(label.replaceAll(" ", "-"), payload, metadata),
        );
      }

      const repeatedArrayObject = {
        id: "duplicate-ddl-script",
        name: "Duplicate-safe script",
        command: "true",
        icon: "test",
        runOnWorktreeCreate: false,
      } as const;
      const allowedPayload = encodeUnknownJson({
        projectId: "duplicate-ddl-allowed",
        title: "Allowed",
        workspaceRoot: "/tmp/duplicate-ddl-allowed",
        defaultModelSelection: null,
        scripts: [repeatedArrayObject, repeatedArrayObject],
        createdAt: occurredAt,
        updatedAt: occurredAt,
      });
      yield* insert("allowed", allowedPayload);

      yield* assertRejectedWithoutChanges(
        "UPDATE payload duplicate",
        sql`UPDATE main.orchestration_events
          SET payload_json=${'{"value":1,"value":2}'}
          WHERE event_id='duplicate-ddl-allowed'`,
      );
      yield* assertRejectedWithoutChanges(
        "UPDATE metadata duplicate",
        sql`UPDATE main.orchestration_events
          SET metadata_json=${'{"adapterKey":"codex","adapterKey":"other"}'}
          WHERE event_id='duplicate-ddl-allowed'`,
      );
      yield* assertRejectedWithoutChanges(
        "OR REPLACE duplicate",
        sql`
          INSERT OR REPLACE INTO main.orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
            command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            'duplicate-ddl-allowed', 'project', 'duplicate-ddl-allowed', 1,
            'project.created', ${occurredAt}, NULL, NULL, NULL, 'server',
            ${'{"value":1,"value":2}'}, '{}'
          )
        `,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT payload_json AS payload, metadata_json AS metadata
          FROM main.orchestration_events WHERE event_id='duplicate-ddl-allowed'`,
        [{ payload: allowedPayload, metadata: "{}" }],
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
        const defaultPayload = historicalProjectCreatedPayload(
          `stream-${suffix}`,
          "2026-08-26T08:00:00.000Z",
        );
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
            'server', ${overrides.payload ?? `'${defaultPayload}'`},
            ${overrides.metadata ?? `'{}'`}
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
        "caught pre-mutation storage rejection",
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
          streamId: `'project-replacement-�'`,
          payload: `'${historicalProjectCreatedPayload(
            "project-replacement-�",
            "2026-08-26T08:00:00.000Z",
          ).replace("Historical project", "Historical � project")}'`,
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
          payload: `'${historicalProjectCreatedPayload(
            "stream-retry",
            "2026-08-26T08:00:00.000Z",
          )}'`,
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
            /(?:no such|unknown) function: (?:t3_fatal_utf8|t3_orchestration_event_authority_route)/u,
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
