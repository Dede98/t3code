import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
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
  readonly includeProjection?: boolean;
  readonly includeThirdEvent?: boolean;
  readonly projectionTitle?: string;
  readonly projectionControlState?: string;
  readonly markerCommandType?: string;
  readonly markerAuthority?: string;
  readonly markerAggregateKind?: string;
  readonly markerThreadId?: string;
  readonly markerFingerprint?: string;
  readonly markerResultOffset?: number;
  readonly transformModelSelectionJson?: (json: string) => string;
  readonly transformBindingJson?: (json: string) => string;
  readonly transformCreatedPayloadJson?: (json: string) => string;
  readonly transformBindingPayloadJson?: (json: string) => string;
}

const insertAcceptedEvidence = Effect.fn("insertAcceptedMaterializationMigrationEvidence")(
  function* (sql: SqlClient.SqlClient, suffix: string, variant: AcceptedEvidenceVariant = {}) {
    const commandId = `accepted-command-${suffix}`;
    const eventCommandId = variant.eventCommandId ?? commandId;
    const threadId = `accepted-thread-${suffix}`;
    const intentThreadId = variant.intentThreadId ?? threadId;
    const createdAt = "2026-07-27T10:00:00.000Z";
    const fingerprint = variant.intentFingerprint ?? "a".repeat(64);
    const modelSelectionJson =
      variant.transformModelSelectionJson?.(
        '{"instanceId":"codex","model":"gpt","options":[{"id":"reasoningEffort","value":"high"}]}',
      ) ??
      '{"instanceId":"codex","model":"gpt","options":[{"id":"reasoningEffort","value":"high"}]}';
    const bindingJson =
      variant.transformBindingJson?.(
        `{"taskId":"task-${suffix}","stageRunId":"stage-${suffix}","attemptId":"attempt-${suffix}","roleId":"planning","controlState":"controlled"}`,
      ) ??
      `{"taskId":"task-${suffix}","stageRunId":"stage-${suffix}","attemptId":"attempt-${suffix}","roleId":"planning","controlState":"controlled"}`;
    const projectionBindingJson = `{"taskId":"task-${suffix}","stageRunId":"stage-${suffix}","attemptId":"attempt-${suffix}","roleId":"planning","controlState":"${variant.projectionControlState ?? "controlled"}"}`;
    const createdPayloadJson =
      variant.transformCreatedPayloadJson?.(
        `{"threadId":"${threadId}","projectId":"project-${suffix}","title":"Accepted","modelSelection":${modelSelectionJson},"runtimeMode":"approval-required","interactionMode":"plan","branch":"branch-${suffix}","worktreePath":"/tmp/${suffix}","createdAt":"${createdAt}","updatedAt":"${createdAt}"}`,
      ) ??
      `{"threadId":"${threadId}","projectId":"project-${suffix}","title":"Accepted","modelSelection":${modelSelectionJson},"runtimeMode":"approval-required","interactionMode":"plan","branch":"branch-${suffix}","worktreePath":"/tmp/${suffix}","createdAt":"${createdAt}","updatedAt":"${createdAt}"}`;
    const bindingPayloadJson =
      variant.transformBindingPayloadJson?.(
        `{"threadId":"${threadId}","binding":${bindingJson},"updatedAt":"${createdAt}"}`,
      ) ?? `{"threadId":"${threadId}","binding":${bindingJson},"updatedAt":"${createdAt}"}`;
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
            ${eventCommandId}, NULL, ${eventCommandId}, 'server',
            ${createdPayloadJson}, '{}'
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
            ${eventCommandId}, NULL, ${eventCommandId}, 'server',
            ${bindingPayloadJson}, '{}'
          )
          RETURNING sequence
        `;
    const createdSequence = createdRows[0]?.sequence ?? 1;
    const boundSequence = boundRows[0]?.sequence ?? createdSequence + 1;
    const receiptResultSequence = boundSequence + (variant.receiptResultOffset ?? 0);

    if (variant.includeThirdEvent === true) {
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          ${`third-event-${suffix}`}, 'project', ${`third-project-${suffix}`}, 0,
          'project.created', ${createdAt}, ${commandId}, NULL, ${commandId},
          'server',
          ${`{"projectId":"third-project-${suffix}","title":"Third","workspaceRoot":"/tmp/third","defaultModelSelection":null,"scripts":[],"createdAt":"${createdAt}","updatedAt":"${createdAt}"}`},
          '{}'
        )
      `;
    }
    if (variant.includeProjection !== false) {
      yield* sql`
        INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode,
          interaction_mode, branch, worktree_path, agent_control_json,
          latest_turn_id, created_at, updated_at, archived_at,
          latest_user_message_at, pending_approval_count,
          pending_user_input_count, has_actionable_proposed_plan, deleted_at
        ) VALUES (
          ${threadId}, ${`project-${suffix}`},
          ${variant.projectionTitle ?? "Accepted"}, ${modelSelectionJson},
          'approval-required', 'plan', ${`branch-${suffix}`}, ${`/tmp/${suffix}`},
          ${projectionBindingJson}, NULL, ${createdAt}, ${createdAt}, NULL,
          NULL, 0, 0, 0, NULL
        )
      `;
    }
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
        ${modelSelectionJson}, 'approval-required', 'plan',
        ${`branch-${suffix}`}, ${`/tmp/${suffix}`},
        ${bindingJson},
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
    const rollbackExit = yield* Effect.exit(sql`ROLLBACK`);
    if (Exit.isFailure(rollbackExit)) {
      assert.include(Cause.pretty(rollbackExit.cause), "no transaction is active");
    }
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
      name: "accepted-without-projection",
      variant: { includeProjection: false },
    },
    {
      name: "accepted-with-inconsistent-projection",
      variant: { projectionTitle: "Inconsistent" },
    },
    {
      name: "three-same-command-events",
      variant: { includeThirdEvent: true },
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
    {
      name: "duplicate-created-root-key",
      variant: {
        transformCreatedPayloadJson: (json) =>
          json.replace('{"threadId":', '{"threadId":"duplicate","threadId":'),
      },
    },
    {
      name: "duplicate-binding-root-key",
      variant: {
        transformBindingPayloadJson: (json) =>
          json.replace('{"threadId":', '{"threadId":"duplicate","threadId":'),
      },
    },
    {
      name: "duplicate-model-selection-key",
      variant: {
        transformModelSelectionJson: (json) =>
          json.replace('{"instanceId":', '{"instanceId":"duplicate","instanceId":'),
      },
    },
    {
      name: "duplicate-binding-key",
      variant: {
        transformBindingJson: (json) =>
          json.replace('{"taskId":', '{"taskId":"duplicate","taskId":'),
      },
    },
    {
      name: "missing-model-selection-object",
      variant: {
        transformCreatedPayloadJson: (json) =>
          json.replace(/"modelSelection":.*?,"runtimeMode"/, '"runtimeMode"'),
      },
    },
    {
      name: "null-model-selection",
      variant: {
        transformModelSelectionJson: () => "null",
      },
    },
    {
      name: "primitive-model-selection",
      variant: {
        transformModelSelectionJson: () => '"codex"',
      },
    },
    {
      name: "incomplete-model-selection",
      variant: {
        transformModelSelectionJson: (json) => json.replace(',"model":"gpt"', ""),
      },
    },
    {
      name: "incomplete-model-options",
      variant: {
        transformModelSelectionJson: (json) => json.replace(',"value":"high"', ""),
      },
    },
    {
      name: "wrong-model-option-primitive",
      variant: {
        transformModelSelectionJson: (json) => json.replace('"value":"high"', '"value":1'),
      },
    },
    {
      name: "contradictory-model-options",
      variant: {
        transformModelSelectionJson: (json) =>
          json.replace(
            '[{"id":"reasoningEffort","value":"high"}]',
            '[{"id":"reasoningEffort","value":"high"},{"id":"reasoningEffort","value":"low"}]',
          ),
      },
    },
    {
      name: "null-binding",
      variant: {
        transformBindingJson: () => "null",
      },
    },
    {
      name: "wrong-binding-primitive",
      variant: {
        transformBindingJson: () => "true",
      },
    },
    {
      name: "incomplete-binding",
      variant: {
        transformBindingJson: (json) => json.replace(',"controlState":"controlled"', ""),
      },
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

  for (const mutation of ["update", "delete"] as const) {
    it.effect(`rolls back a projection ${mutation} after the final marker`, () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations();
        const suffix = `post-marker-${mutation}`;
        const threadId = `accepted-thread-${suffix}`;
        const failed = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* insertAcceptedEvidence(sql, suffix);
              if (mutation === "update") {
                yield* sql`
                  UPDATE projection_threads
                  SET title = 'Changed after marker'
                  WHERE thread_id = ${threadId}
                `;
              } else {
                yield* sql`
                  DELETE FROM projection_threads
                  WHERE thread_id = ${threadId}
                `;
              }
            }),
          ),
        );
        assert.strictEqual(Exit.isFailure(failed), true);
        assert.deepStrictEqual(
          yield* sql<{
            readonly events: number;
            readonly intents: number;
            readonly receipts: number;
            readonly markers: number;
            readonly projections: number;
          }>`
            SELECT
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${threadId}) AS events,
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_intents
               WHERE thread_id = ${threadId}) AS intents,
              (SELECT count(*) FROM orchestration_command_receipts
               WHERE aggregate_id = ${threadId}) AS receipts,
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_receipts
               WHERE thread_id = ${threadId}) AS markers,
              (SELECT count(*) FROM projection_threads
               WHERE thread_id = ${threadId}) AS projections
          `,
          [{ events: 0, intents: 0, receipts: 0, markers: 0, projections: 0 }],
        );

        yield* sql.withTransaction(insertAcceptedEvidence(sql, suffix));
        assert.deepStrictEqual(
          yield* sql<{ readonly markers: number; readonly projections: number }>`
            SELECT
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_receipts
               WHERE thread_id = ${threadId}) AS markers,
              (SELECT count(*) FROM projection_threads
               WHERE thread_id = ${threadId}) AS projections
          `,
          [{ markers: 1, projections: 1 }],
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
      const rejectedReceiptMutations = [
        sql`
          UPDATE orchestration_command_receipts
          SET error = 'changed rejection'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET status = 'accepted'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET authority = 'system'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET aggregate_kind = 'project', aggregate_id = 'changed-aggregate'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET result_sequence = 1
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET accepted_at = '2026-07-27T10:00:01.000Z'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_command_receipts
          SET command_id = 'changed-rejected-048'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          DELETE FROM orchestration_command_receipts
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_agent_control_thread_materialization_intents
          SET command_type = 'thread.create'
          WHERE command_id = 'rejected-048'
        `,
        sql`
          UPDATE orchestration_agent_control_thread_materialization_intents
          SET command_fingerprint = ${"c".repeat(64)}
          WHERE command_id = 'rejected-048'
        `,
      ];
      for (const mutation of rejectedReceiptMutations) {
        assert.strictEqual(Exit.isFailure(yield* Effect.exit(mutation)), true);
      }
      assert.deepStrictEqual(
        yield* sql<{
          readonly commandId: string;
          readonly authority: string;
          readonly aggregateKind: string;
          readonly aggregateId: string;
          readonly acceptedAt: string;
          readonly resultSequence: number;
          readonly status: string;
          readonly error: string | null;
        }>`
          SELECT
            command_id AS "commandId", authority,
            aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId",
            accepted_at AS "acceptedAt", result_sequence AS "resultSequence",
            status, error
          FROM orchestration_command_receipts
          WHERE command_id = 'rejected-048'
        `,
        [
          {
            commandId: "rejected-048",
            authority: "agent-control",
            aggregateKind: "thread",
            aggregateId: "thread-rejected-048",
            acceptedAt: "2026-07-27T10:00:00.000Z",
            resultSequence: 0,
            status: "rejected",
            error: "rejected",
          },
        ],
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
      yield* sql`
        UPDATE orchestration_command_receipts
        SET error = 'still mutable'
        WHERE command_id = 'foreign-orchestration-receipt'
      `;
      yield* sql`
        DELETE FROM orchestration_command_receipts
        WHERE command_id = 'existing-agent-control-bind-receipt'
      `;
      assert.deepStrictEqual(
        yield* sql<{ readonly error: string | null }>`
          SELECT error
          FROM orchestration_command_receipts
          WHERE command_id = 'foreign-orchestration-receipt'
        `,
        [{ error: "still mutable" }],
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

      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'foreign-event-for-protected-command-update', 'project',
          'foreign-project-for-protected-command-update', 0, 'project.created',
          '2026-07-27T10:00:01.000Z', 'foreign-command-for-protected-update',
          NULL, 'foreign-command-for-protected-update', 'server',
          '{"projectId":"foreign-project-for-protected-command-update","title":"Foreign","workspaceRoot":"/tmp/foreign","defaultModelSelection":null,"scripts":[],"createdAt":"2026-07-27T10:00:01.000Z","updatedAt":"2026-07-27T10:00:01.000Z"}',
          '{}'
        )
      `;
      const commandBoundaryMutations = [
        sql`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            'third-event-after-acceptance', 'project', 'third-project-after-acceptance',
            0, 'project.created', '2026-07-27T10:00:01.000Z',
            'accepted-command-immutable', NULL, 'accepted-command-immutable',
            'server', '{}', '{}'
          )
        `,
        sql`
          UPDATE orchestration_events
          SET command_id = 'accepted-command-immutable'
          WHERE event_id = 'foreign-event-for-protected-command-update'
        `,
        sql`
          UPDATE orchestration_events
          SET command_id = 'changed-protected-command'
          WHERE event_id = 'created-event-immutable'
        `,
        sql`
          DELETE FROM orchestration_events
          WHERE event_id = 'created-event-immutable'
        `,
        sql`
          DELETE FROM orchestration_events
          WHERE event_id = 'bound-event-immutable'
        `,
      ];
      for (const mutation of commandBoundaryMutations) {
        assert.strictEqual(Exit.isFailure(yield* Effect.exit(mutation)), true);
      }

      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'regular-version-three-after-materialization', 'thread',
          'accepted-thread-immutable', 3, 'thread.meta-updated',
          '2026-07-27T10:00:01.000Z', 'regular-thread-command-after-materialization',
          NULL, 'regular-thread-command-after-materialization', 'client',
          '{"threadId":"accepted-thread-immutable","title":"Later","updatedAt":"2026-07-27T10:00:01.000Z"}',
          '{}'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'independent-zero-based-event', 'thread', 'independent-zero-based-thread',
          0, 'thread.created', '2026-07-27T10:00:01.000Z',
          'independent-zero-based-command', NULL, 'independent-zero-based-command',
          'client',
          '{"threadId":"independent-zero-based-thread","projectId":"independent-project","title":"Independent","modelSelection":{"instanceId":"codex","model":"gpt"},"runtimeMode":"full-access","interactionMode":"default","branch":null,"worktreePath":null,"createdAt":"2026-07-27T10:00:01.000Z","updatedAt":"2026-07-27T10:00:01.000Z"}',
          '{}'
        )
      `;
      assert.deepStrictEqual(
        yield* sql<{ readonly eventId: string; readonly streamVersion: number }>`
          SELECT event_id AS "eventId", stream_version AS "streamVersion"
          FROM orchestration_events
          WHERE event_id IN (
            'regular-version-three-after-materialization',
            'independent-zero-based-event'
          )
          ORDER BY event_id
        `,
        [
          { eventId: "independent-zero-based-event", streamVersion: 0 },
          { eventId: "regular-version-three-after-materialization", streamVersion: 3 },
        ],
      );
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
