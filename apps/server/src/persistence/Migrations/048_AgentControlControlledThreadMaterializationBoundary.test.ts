import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

interface AcceptedEvidenceVariant {
  readonly includeCreatedEvent?: boolean;
  readonly includeBoundEvent?: boolean;
  readonly createdEventType?: string;
  readonly boundEventType?: string;
  readonly createdStreamVersion?: number;
  readonly boundStreamVersion?: number;
  readonly eventCommandId?: string;
  readonly intentCommandType?: string;
  readonly intentAuthority?: string;
  readonly intentAggregateKind?: string;
  readonly intentThreadId?: string;
  readonly intentFingerprint?: string;
  readonly receiptResultOffset?: number;
  readonly intentCreatedSequenceOffset?: number;
  readonly intentBoundSequenceOffset?: number;
  readonly partialCreatedCoordinates?: boolean;
  readonly includeReceipt?: boolean;
  readonly includeIntent?: boolean;
  readonly includeReceiptEvidence?: boolean;
  readonly markerCommandType?: string;
  readonly markerAuthority?: string;
  readonly markerAggregateKind?: string;
  readonly markerThreadId?: string;
  readonly markerFingerprint?: string;
  readonly markerResultOffset?: number;
}

const insertAcceptedEvidence = Effect.fn("insertAcceptedMaterializationMigrationEvidence")(
  function* (sql: SqlClient.SqlClient, suffix: string, variant: AcceptedEvidenceVariant = {}) {
    const commandId = `accepted-command-${suffix}`;
    const eventCommandId = variant.eventCommandId ?? commandId;
    const threadId = `accepted-thread-${suffix}`;
    const intentThreadId = variant.intentThreadId ?? threadId;
    const createdAt = "2026-07-27T10:00:00.000Z";
    const fingerprint = variant.intentFingerprint ?? "a".repeat(64);
    const createdRows =
      variant.includeCreatedEvent === false
        ? []
        : yield* sql<{ readonly sequence: number }>`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            ${`created-event-${suffix}`}, 'thread', ${threadId},
            ${variant.createdStreamVersion ?? 1},
            ${variant.createdEventType ?? "thread.created"}, ${createdAt},
            ${eventCommandId}, NULL, ${eventCommandId}, 'server', '{}', '{}'
          )
          RETURNING sequence
        `;
    const boundRows =
      variant.includeBoundEvent === false
        ? []
        : yield* sql<{ readonly sequence: number }>`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            ${`bound-event-${suffix}`}, 'thread', ${threadId},
            ${variant.boundStreamVersion ?? 2},
            ${variant.boundEventType ?? "thread.agent-control-bound"}, ${createdAt},
            ${eventCommandId}, NULL, ${eventCommandId}, 'server', '{}', '{}'
          )
          RETURNING sequence
        `;
    const createdSequence = createdRows[0]?.sequence ?? 1;
    const boundSequence = boundRows[0]?.sequence ?? createdSequence + 1;
    const receiptResultSequence = boundSequence + (variant.receiptResultOffset ?? 0);

    if (variant.includeReceipt !== false) {
      yield* sql`
      INSERT INTO orchestration_command_receipts (
        command_id, authority, aggregate_kind, aggregate_id, accepted_at,
        result_sequence, status, error
      ) VALUES (
        ${commandId}, 'agent-control', 'thread', ${threadId}, ${createdAt},
        ${receiptResultSequence}, 'accepted', NULL
      )
    `;
    }
    if (variant.includeIntent !== false) {
      yield* sql`
      INSERT INTO orchestration_agent_control_thread_materialization_intents (
        command_id, command_type, authority, aggregate_kind, command_fingerprint,
        controlled_thread_reservation_id, thread_id, project_id, task_id,
        task_revision, github_intake_sequence, source_identity_fingerprint,
        stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
        attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
        title, model_selection_json, runtime_mode, interaction_mode, branch,
        worktree_path, binding_json, created_event_id, created_event_type,
        created_event_sequence, created_event_stream_version, binding_event_id,
        binding_event_type, binding_event_sequence, binding_event_stream_version,
        accepted_receipt_command_id, receipt_status, receipt_result_sequence,
        receipt_accepted_at, receipt_error, created_at
      ) VALUES (
        ${commandId}, ${variant.intentCommandType ?? "thread.agent-control.materialize"},
        ${variant.intentAuthority ?? "agent-control"},
        ${variant.intentAggregateKind ?? "thread"}, ${fingerprint},
        ${`reservation-${suffix}`}, ${intentThreadId}, ${`project-${suffix}`},
        ${`task-${suffix}`}, 1, 1, ${"b".repeat(64)}, ${`stage-${suffix}`},
        ${`attempt-${suffix}`}, 'planning', 'planning', 1, 1,
        ${`lease-${suffix}`}, 1, ${`worktree-${suffix}`}, 'Accepted',
        '{"instanceId":"codex","model":"gpt"}', 'approval-required', 'plan',
        ${`branch-${suffix}`}, ${`/tmp/${suffix}`},
        ${`{"taskId":"task-${suffix}","stageRunId":"stage-${suffix}","attemptId":"attempt-${suffix}","roleId":"planning","controlState":"controlled"}`},
        ${variant.partialCreatedCoordinates ? null : `created-event-${suffix}`},
        'thread.created', ${createdSequence + (variant.intentCreatedSequenceOffset ?? 0)}, 1,
        ${`bound-event-${suffix}`}, 'thread.agent-control-bound',
        ${boundSequence + (variant.intentBoundSequenceOffset ?? 0)}, 2, ${commandId},
        'accepted', ${boundSequence + (variant.intentBoundSequenceOffset ?? 0)},
        ${createdAt}, NULL, ${createdAt}
      )
    `;
    }
    if (variant.includeReceiptEvidence !== false) {
      yield* sql`
      INSERT INTO orchestration_agent_control_thread_materialization_receipts (
        command_id, command_type, authority, aggregate_kind, thread_id,
        command_fingerprint, result_sequence, accepted_at, status
      ) VALUES (
        ${commandId},
        ${
          variant.markerCommandType ??
          variant.intentCommandType ??
          "thread.agent-control.materialize"
        },
        ${variant.markerAuthority ?? variant.intentAuthority ?? "agent-control"},
        ${variant.markerAggregateKind ?? variant.intentAggregateKind ?? "thread"},
        ${variant.markerThreadId ?? intentThreadId},
        ${variant.markerFingerprint ?? fingerprint},
        ${boundSequence + (variant.markerResultOffset ?? 0)}, ${createdAt}, 'accepted'
      )
    `;
    }
  },
);

const runExplicitTransactionExit = Effect.fn("runExplicitMigrationTransactionExit")(function* (
  sql: SqlClient.SqlClient,
  effect: Effect.Effect<void, SqlError>,
) {
  yield* sql`BEGIN`;
  const exit = yield* Effect.exit(effect.pipe(Effect.andThen(sql`COMMIT`)));
  if (Exit.isFailure(exit)) {
    yield* sql`ROLLBACK`;
  }
  return exit;
});

layer("048_AgentControlControlledThreadMaterializationBoundary", (it) => {
  it.effect("commits one complete accepted intent with exact receipt and event evidence", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      yield* sql.withTransaction(insertAcceptedEvidence(sql, "valid"));
      assert.deepStrictEqual(
        yield* sql<{
          readonly intents: number;
          readonly markers: number;
          readonly receipts: number;
          readonly events: number;
        }>`
          SELECT
            (SELECT COUNT(*) FROM orchestration_agent_control_thread_materialization_intents
             WHERE command_id = 'accepted-command-valid') AS intents,
            (SELECT COUNT(*) FROM orchestration_agent_control_thread_materialization_receipts
             WHERE command_id = 'accepted-command-valid') AS markers,
            (SELECT COUNT(*) FROM orchestration_command_receipts
             WHERE command_id = 'accepted-command-valid') AS receipts,
            (SELECT COUNT(*) FROM orchestration_events
             WHERE command_id = 'accepted-command-valid'
               AND stream_version IN (1, 2)) AS events
        `,
        [{ intents: 1, markers: 1, receipts: 1, events: 2 }],
      );
    }),
  );

  const invalidAcceptedEvidence: ReadonlyArray<{
    readonly name: string;
    readonly variant: AcceptedEvidenceVariant;
  }> = [
    { name: "missing-created-event", variant: { includeCreatedEvent: false } },
    { name: "missing-bound-event", variant: { includeBoundEvent: false } },
    {
      name: "swapped-event-types",
      variant: {
        createdEventType: "thread.agent-control-bound",
        boundEventType: "thread.created",
      },
    },
    {
      name: "wrong-event-sequences",
      variant: { intentCreatedSequenceOffset: 3, intentBoundSequenceOffset: 3 },
    },
    { name: "wrong-created-stream-version", variant: { createdStreamVersion: 0 } },
    { name: "wrong-bound-stream-version", variant: { boundStreamVersion: 1 } },
    { name: "wrong-event-command", variant: { eventCommandId: "foreign-command" } },
    { name: "wrong-thread-aggregate", variant: { intentThreadId: "foreign-thread" } },
    { name: "wrong-aggregate-kind", variant: { markerAggregateKind: "project" } },
    { name: "wrong-result-sequence", variant: { receiptResultOffset: 1 } },
    { name: "partial-intent-fields", variant: { partialCreatedCoordinates: true } },
    {
      name: "accepted-receipt-without-intent",
      variant: { includeIntent: false },
    },
    {
      name: "intent-without-accepted-receipt",
      variant: { includeReceipt: false },
    },
    {
      name: "intent-without-receipt-marker",
      variant: { includeReceiptEvidence: false },
    },
    {
      name: "wrong-command-type",
      variant: { markerCommandType: "thread.agent-control.bind" },
    },
    {
      name: "wrong-authority",
      variant: { markerAuthority: "system" },
    },
    {
      name: "wrong-fingerprint",
      variant: { markerFingerprint: "c".repeat(64) },
    },
    {
      name: "wrong-marker-thread",
      variant: { markerThreadId: "foreign-thread-marker" },
    },
    {
      name: "wrong-marker-result-sequence",
      variant: { markerResultOffset: 1 },
    },
  ];

  for (const invalid of invalidAcceptedEvidence) {
    it.effect(`rejects ${invalid.name}`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations();
        const exit = yield* runExplicitTransactionExit(
          sql,
          insertAcceptedEvidence(sql, invalid.name, invalid.variant),
        );
        assert.strictEqual(Exit.isFailure(exit), true);
        assert.deepStrictEqual(
          yield* sql<{ readonly intents: number; readonly markers: number }>`
            SELECT
              (SELECT COUNT(*)
               FROM orchestration_agent_control_thread_materialization_intents
               WHERE command_id = ${`accepted-command-${invalid.name}`}) AS intents,
              (SELECT COUNT(*)
               FROM orchestration_agent_control_thread_materialization_receipts
               WHERE command_id = ${`accepted-command-${invalid.name}`}) AS markers
          `,
          [{ intents: 0, markers: 0 }],
        );
      }),
    );
  }
});

const preservationLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

preservationLayer("048 materialization preservation and immutability", (it) => {
  it.effect(
    "preserves existing orchestration data, indexes and sequence and is a no-op twice",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations({ toMigrationInclusive: 47 });
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'event-before-048', 'project', 'project-before-048', 0,
          'project.created', '2026-07-27T10:00:00.000Z',
          'command-before-048', NULL, 'command-before-048', 'server',
          '{"projectId":"project-before-048","title":"Before","workspaceRoot":"/tmp/before","defaultModelSelection":null,"scripts":[],"createdAt":"2026-07-27T10:00:00.000Z","updatedAt":"2026-07-27T10:00:00.000Z"}',
          '{}'
        )
      `;
        yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'command-before-048', 'system', 'project', 'project-before-048',
          '2026-07-27T10:00:00.000Z', 1, 'accepted', NULL
        )
      `;
        yield* sql`
        CREATE INDEX preserved_orchestration_index_before_048
        ON orchestration_events(correlation_id)
      `;
        const beforeSequence = yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'
      `;

        const first = yield* runMigrations({ toMigrationInclusive: 48 });
        const second = yield* runMigrations({ toMigrationInclusive: 48 });
        assert.deepStrictEqual(
          first.map(([id]) => id),
          [48],
        );
        assert.deepStrictEqual(second, []);

        const rows = yield* sql<{ readonly events: number; readonly receipts: number }>`
        SELECT
          (SELECT COUNT(*) FROM orchestration_events
           WHERE event_id = 'event-before-048') AS events,
          (SELECT COUNT(*) FROM orchestration_command_receipts
           WHERE command_id = 'command-before-048') AS receipts
      `;
        assert.deepStrictEqual(rows, [{ events: 1, receipts: 1 }]);
        assert.deepStrictEqual(
          yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM sqlite_schema
          WHERE type = 'index'
            AND name = 'preserved_orchestration_index_before_048'
        `,
          [{ count: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'orchestration_events'
        `,
          beforeSequence,
        );
        const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'index'
          AND name IN (
            'idx_orchestration_events_materialization_coordinates',
            'idx_orchestration_receipts_materialization_coordinates',
            'idx_orchestration_materialization_thread',
            'idx_orchestration_materialization_reservation'
          )
        ORDER BY name
      `;
        assert.lengthOf(indexes, 4);
      }),
  );

  it.effect("keeps intents immutable and introduces no projection foreign key", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      const foreignKeys = yield* sql<{ readonly table: string }>`
        PRAGMA foreign_key_list(orchestration_agent_control_thread_materialization_intents)
      `;
      assert.deepStrictEqual(
        new Set(foreignKeys.map((foreignKey) => foreignKey.table)),
        new Set([
          "orchestration_agent_control_thread_materialization_receipts",
          "orchestration_events",
          "orchestration_command_receipts",
        ]),
      );

      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'rejected-048', 'agent-control', 'thread', 'thread-rejected-048',
          '2026-07-27T10:00:00.000Z', 0, 'rejected', 'rejected'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_agent_control_thread_materialization_intents (
          command_id, command_type, authority, aggregate_kind, command_fingerprint,
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          title, model_selection_json, runtime_mode, interaction_mode, branch,
          worktree_path, binding_json, created_event_id, created_event_type,
          created_event_sequence, created_event_stream_version, binding_event_id,
          binding_event_type, binding_event_sequence, binding_event_stream_version,
          receipt_status, receipt_result_sequence, receipt_accepted_at,
          receipt_error, created_at
        ) VALUES (
          'rejected-048', 'thread.agent-control.materialize', 'agent-control',
          'thread', ${"a".repeat(64)}, 'reservation-rejected-048',
          'thread-rejected-048', 'project-rejected-048', 'task-rejected-048',
          1, 1, ${"b".repeat(64)}, 'stage-rejected-048', 'attempt-rejected-048',
          'planning', 'planning', 1, 1, 'lease-rejected-048', 1,
          'worktree-rejected-048', 'Rejected', '{"instanceId":"codex","model":"gpt"}',
          'approval-required', 'plan', 'branch-rejected-048',
          '/tmp/rejected-048',
          '{"taskId":"task-rejected-048","stageRunId":"stage-rejected-048","attemptId":"attempt-rejected-048","roleId":"planning","controlState":"controlled"}',
          NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
          'rejected', 0, '2026-07-27T10:00:00.000Z', 'rejected',
          '2026-07-27T10:00:00.000Z'
        )
      `;
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            sql`
              UPDATE orchestration_agent_control_thread_materialization_intents
              SET title = 'Changed' WHERE command_id = 'rejected-048'
            `,
          ),
        ),
        true,
      );
      assert.strictEqual(
        Exit.isFailure(
          yield* Effect.exit(
            sql`
              DELETE FROM orchestration_agent_control_thread_materialization_intents
              WHERE command_id = 'rejected-048'
            `,
          ),
        ),
        true,
      );
    }),
  );

  it.effect("rejects foreign events for rejected evidence and preserves foreign receipts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'foreign-orchestration-receipt', 'system', 'project', 'foreign-project',
          '2026-07-27T10:00:00.000Z', 0, 'accepted', NULL
        )
      `;
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          'existing-agent-control-bind-receipt', 'agent-control', 'thread',
          'existing-manual-thread', '2026-07-27T10:00:00.000Z',
          0, 'accepted', NULL
        )
      `;
      assert.deepStrictEqual(
        yield* sql<{ readonly receipts: number; readonly markers: number }>`
          SELECT
            (SELECT COUNT(*) FROM orchestration_command_receipts
             WHERE command_id IN (
               'foreign-orchestration-receipt',
               'existing-agent-control-bind-receipt'
             )) AS receipts,
            (SELECT COUNT(*)
             FROM orchestration_agent_control_thread_materialization_receipts
             WHERE command_id = 'existing-agent-control-bind-receipt') AS markers
        `,
        [{ receipts: 2, markers: 0 }],
      );

      const rejectedWithEvent = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              INSERT INTO orchestration_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, command_id, causation_event_id, correlation_id,
                actor_kind, payload_json, metadata_json
              ) VALUES (
                'rejected-foreign-event', 'project', 'rejected-foreign-project', 0,
                'project.created', '2026-07-27T10:00:00.000Z',
                'rejected-with-foreign-event', NULL, 'rejected-with-foreign-event',
                'server', '{}', '{}'
              )
            `;
            yield* sql`
              INSERT INTO orchestration_command_receipts (
                command_id, authority, aggregate_kind, aggregate_id, accepted_at,
                result_sequence, status, error
              ) VALUES (
                'rejected-with-foreign-event', 'agent-control', 'thread',
                'rejected-thread-with-event', '2026-07-27T10:00:00.000Z',
                0, 'rejected', 'rejected'
              )
            `;
            yield* sql`
              INSERT INTO orchestration_agent_control_thread_materialization_intents (
                command_id, command_type, authority, aggregate_kind, command_fingerprint,
                controlled_thread_reservation_id, thread_id, project_id, task_id,
                task_revision, github_intake_sequence, source_identity_fingerprint,
                stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
                attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
                title, model_selection_json, runtime_mode, interaction_mode, branch,
                worktree_path, binding_json, receipt_status, receipt_result_sequence,
                receipt_accepted_at, receipt_error, created_at
              ) VALUES (
                'rejected-with-foreign-event', 'thread.agent-control.materialize',
                'agent-control', 'thread', ${"d".repeat(64)}, 'rejected-reservation',
                'rejected-thread-with-event', 'rejected-project', 'rejected-task',
                1, 1, ${"e".repeat(64)}, 'rejected-stage', 'rejected-attempt',
                'planning', 'planning', 1, 1, 'rejected-lease', 1,
                'rejected-worktree', 'Rejected', '{"instanceId":"codex","model":"gpt"}',
                'approval-required', 'plan', 'rejected-branch', '/tmp/rejected',
                '{"taskId":"rejected-task","stageRunId":"rejected-stage","attemptId":"rejected-attempt","roleId":"planning","controlState":"controlled"}',
                'rejected', 0, '2026-07-27T10:00:00.000Z', 'rejected',
                '2026-07-27T10:00:00.000Z'
              )
            `;
          }),
        ),
      );
      assert.strictEqual(Exit.isFailure(rejectedWithEvent), true);
    }),
  );

  it.effect("keeps all completed accepted evidence immutable and one-to-one", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations();
      yield* sql.withTransaction(insertAcceptedEvidence(sql, "immutable"));

      const mutations = [
        sql`
          UPDATE orchestration_events SET payload_json = '{"changed":true}'
          WHERE event_id = 'created-event-immutable'
        `,
        sql`DELETE FROM orchestration_events WHERE event_id = 'bound-event-immutable'`,
        sql`
          UPDATE orchestration_command_receipts SET result_sequence = result_sequence + 1
          WHERE command_id = 'accepted-command-immutable'
        `,
        sql`
          DELETE FROM orchestration_command_receipts
          WHERE command_id = 'accepted-command-immutable'
        `,
        sql`
          UPDATE orchestration_agent_control_thread_materialization_intents
          SET title = 'Changed' WHERE command_id = 'accepted-command-immutable'
        `,
        sql`
          DELETE FROM orchestration_agent_control_thread_materialization_receipts
          WHERE command_id = 'accepted-command-immutable'
        `,
        sql`
          INSERT INTO orchestration_agent_control_thread_materialization_receipts (
            command_id, command_type, authority, aggregate_kind, thread_id,
            command_fingerprint, result_sequence, accepted_at, status
          ) SELECT
            command_id, command_type, authority, aggregate_kind, thread_id,
            command_fingerprint, result_sequence + 1, accepted_at, status
          FROM orchestration_agent_control_thread_materialization_receipts
          WHERE command_id = 'accepted-command-immutable'
        `,
      ];
      for (const mutation of mutations) {
        assert.strictEqual(Exit.isFailure(yield* Effect.exit(mutation)), true);
      }
    }),
  );
});

rollbackLayer("048_AgentControlControlledThreadMaterializationBoundary rollback", (it) => {
  it.effect("rolls the entire additive migration back when later DDL fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 47 });
      yield* sql`
        CREATE TRIGGER trg_orchestration_materialization_intent_immutable_delete
        BEFORE INSERT ON orchestration_events
        BEGIN
          SELECT 1;
        END
      `;
      const exit = yield* Effect.exit(runMigrations({ toMigrationInclusive: 48 }));
      assert.strictEqual(Exit.isFailure(exit), true);
      const objects = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE name IN (
          'orchestration_agent_control_thread_materialization_intents',
          'orchestration_agent_control_thread_materialization_receipts',
          'idx_orchestration_events_materialization_coordinates',
          'idx_orchestration_receipts_materialization_coordinates',
          'idx_orchestration_materialization_intent_receipt_evidence',
          'idx_orchestration_materialization_thread',
          'idx_orchestration_materialization_reservation',
          'trg_orchestration_materialization_intent_immutable_update'
        )
      `;
      assert.deepStrictEqual(objects, []);
      const migrationRows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM effect_sql_migrations WHERE migration_id = 48
      `;
      assert.deepStrictEqual(migrationRows, [{ count: 0 }]);
    }),
  );
});
