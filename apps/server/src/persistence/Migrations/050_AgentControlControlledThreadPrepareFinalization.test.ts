import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration050 from "./050_AgentControlControlledThreadPrepareFinalization.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const encodeJson = Schema.encodeSync(Schema.UnknownFromJsonString);

layer("050_AgentControlControlledThreadPrepareFinalization", (it) => {
  const insertAcceptedPrepare = Effect.fn("insertAcceptedPrepareFixture")(function* (input: {
    readonly suffix: string;
    readonly includeReceipt?: boolean;
    readonly includeFinalization?: boolean;
    readonly finalizationOverrides?: Partial<{
      readonly commandId: string;
      readonly fingerprint: string;
      readonly projectId: string;
      readonly taskId: string;
      readonly reservationId: string;
      readonly eventId: string;
      readonly receiptCommandId: string;
      readonly resultSequence: number;
      readonly resultStreamVersion: number;
    }>;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const commandId = `prepare-050-${input.suffix}`;
    const fingerprint = "c".repeat(64);
    const projectId = `project-050-${input.suffix}`;
    const taskId = `task-050-${input.suffix}`;
    const reservationId = `controlled-thread-reservation-050-${input.suffix}`;
    const threadId = `t3-auto-reserved-thread-050-${input.suffix}`;
    const eventId = `event-050-${input.suffix}`;
    const at = "2026-07-30T08:00:00.000Z";
    const payload = {
      controlledThreadReservationId: reservationId,
      threadId,
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: "a".repeat(64),
      stageRunId: `stage-run-050-${input.suffix}`,
      attemptId: `attempt-050-${input.suffix}`,
      roleId: "planning",
      stageKind: "planning",
      stageOrdinal: 1,
      attemptOrdinal: 1,
      leaseId: `lease-050-${input.suffix}`,
      fenceToken: 1,
      worktreeReservationId: `worktree-reservation-050-${input.suffix}`,
      status: "prepared",
      preparedAt: at,
    } as const;

    yield* sql`
      INSERT INTO agent_control_controlled_thread_stream_catalog (
        controlled_thread_reservation_id, event_id, stream_version,
        command_id, event_type, thread_id, project_id, task_id,
        task_revision, github_intake_sequence, source_identity_fingerprint,
        stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
        attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
        prepared_at
      ) VALUES (
        ${reservationId}, ${eventId}, 1, ${commandId},
        'agentControl.controlledThreadReservation.prepared',
        ${threadId}, ${projectId}, ${taskId}, 1, 1, ${"a".repeat(64)},
        ${payload.stageRunId}, ${payload.attemptId}, 'planning', 'planning',
        1, 1, ${payload.leaseId}, 1, ${payload.worktreeReservationId}, ${at}
      )
    `;
    const inserted = yield* sql<{ readonly sequence: number }>`
      INSERT INTO agent_control_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type,
        occurred_at, command_id, causation_event_id, correlation_id,
        actor_authority, payload_json, metadata_json
      ) VALUES (
        ${eventId}, 'controlled-thread-reservation', ${reservationId}, 1,
        'agentControl.controlledThreadReservation.prepared', ${at},
        ${commandId}, NULL, ${commandId}, 'controller',
        ${encodeJson(payload)}, '{"schemaVersion":1}'
      )
      RETURNING sequence
    `;
    const sequence = inserted[0]!.sequence;
    yield* sql`
      INSERT INTO agent_control_controlled_thread_reservation_states (
        controlled_thread_reservation_id, thread_id, project_id, task_id,
        task_revision, github_intake_sequence, source_identity_fingerprint,
        stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
        attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
        status, revision, last_event_sequence, prepared_at, state_json
      ) VALUES (
        ${reservationId}, ${threadId}, ${projectId}, ${taskId}, 1, 1,
        ${"a".repeat(64)}, ${payload.stageRunId}, ${payload.attemptId},
        'planning', 'planning', 1, 1, ${payload.leaseId}, 1,
        ${payload.worktreeReservationId}, 'prepared', 1, ${sequence}, ${at},
        ${encodeJson({ schemaVersion: 1, ...payload, revision: 1, sequence })}
      )
    `;
    yield* sql`
      INSERT INTO agent_control_controlled_thread_command_intents (
        command_id, request_fingerprint, intent_fingerprint, command_type,
        authority, aggregate_kind, aggregate_id, project_id, task_id,
        controlled_thread_reservation_id, thread_id, task_revision,
        github_intake_sequence, source_identity_fingerprint, stage_run_id,
        attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
        lease_id, fence_token, worktree_reservation_id, expected_revision
      ) VALUES (
        ${commandId}, ${fingerprint}, ${fingerprint},
        'agentControl.controlledThreadReservation.prepare', 'controller',
        'controlled-thread-reservation', ${reservationId}, ${projectId},
        ${taskId}, ${reservationId}, ${threadId}, 1, 1, ${"a".repeat(64)},
        ${payload.stageRunId}, ${payload.attemptId}, 'planning', 'planning',
        1, 1, ${payload.leaseId}, 1, ${payload.worktreeReservationId}, 0
      )
    `;
    const insertFinalization =
      input.includeFinalization === true
        ? sql`
            INSERT INTO agent_control_controlled_thread_prepare_finalizations (
              prepare_command_id, prepare_command_fingerprint,
              project_id, task_id, controlled_thread_reservation_id,
              prepared_event_id, prepared_stream_version,
              prepared_event_sequence, receipt_command_id, receipt_status,
              receipt_result_sequence, receipt_result_stream_version,
              receipt_event_created, receipt_accepted_at,
              finalization_owner_id, status, revision
            ) VALUES (
              ${input.finalizationOverrides?.commandId ?? commandId},
              ${input.finalizationOverrides?.fingerprint ?? fingerprint},
              ${input.finalizationOverrides?.projectId ?? projectId},
              ${input.finalizationOverrides?.taskId ?? taskId},
              ${input.finalizationOverrides?.reservationId ?? reservationId},
              ${input.finalizationOverrides?.eventId ?? eventId}, 1, ${sequence},
              ${input.finalizationOverrides?.receiptCommandId ?? commandId}, 'accepted',
              ${input.finalizationOverrides?.resultSequence ?? sequence},
              ${input.finalizationOverrides?.resultStreamVersion ?? 1}, 1, ${at},
              '00000000-0000-0000-0000-000000000050', 'pending', 0
            )
          `
        : Effect.void;
    if (input.includeReceipt !== false) {
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind,
          aggregate_id, status, result_sequence, result_stream_version,
          event_created, accepted_at, error_code
        ) VALUES (
          ${commandId}, ${fingerprint}, 'controller',
          'controlled-thread-reservation', ${reservationId}, 'accepted',
          ${sequence}, 1, 1, ${at}, NULL
        )
      `;
    }
    yield* insertFinalization;
    return { commandId, reservationId, eventId, sequence };
  });

  it.effect("is data-preserving, idempotent, and leaves legacy receipts unbackfilled", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 49 });
      const legacy = yield* sql.withTransaction(
        insertAcceptedPrepare({ suffix: "legacy", includeFinalization: false }),
      );
      const beforeSequence = yield* sql<{ readonly name: string; readonly seq: number }>`
        SELECT name, seq FROM sqlite_sequence ORDER BY name
      `;
      const beforeIndexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'index'
          AND name NOT LIKE 'idx_agent_control_controlled_thread_prepare_%'
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_finalizations_%'
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_accepted_evidence_%'
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_acceptance_obligations_%'
        ORDER BY name
      `;
      const beforeTriggers = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_schema
        WHERE type = 'trigger'
          AND name NOT LIKE
            'agent_control_controlled_thread_prepare_%'
        ORDER BY name
      `;

      assert.deepStrictEqual(yield* runMigrations(), [
        [50, "AgentControlControlledThreadPrepareFinalization"],
      ]);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string; readonly seq: number }>`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `,
        beforeSequence,
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'index'
            AND name NOT LIKE 'idx_agent_control_controlled_thread_prepare_%'
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_finalizations_%'
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_accepted_evidence_%'
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_acceptance_obligations_%'
          ORDER BY name
        `,
        beforeIndexes,
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'trigger'
            AND name NOT LIKE
              'agent_control_controlled_thread_prepare_%'
          ORDER BY name
        `,
        beforeTriggers,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_prepare_finalizations
        `,
        [{ count: 0 }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_acceptance_obligations
             WHERE prepare_command_id = ${legacy.commandId}) AS obligations,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_accepted_evidence
             WHERE prepare_command_id = ${legacy.commandId}) AS acceptedEvidence
        `,
        [{ obligations: 0, acceptedEvidence: 0 }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list(
            'agent_control_controlled_thread_prepare_finalizations'
          )
          WHERE "table" LIKE 'projection_%'
        `)[0]!.count,
        0,
      );
    }),
  );

  it.effect("enforces accepted Prepare IFF complete finalization at COMMIT", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();

      const missingFinalization = yield* Effect.exit(
        sql.withTransaction(
          insertAcceptedPrepare({
            suffix: "missing-finalization",
            includeFinalization: false,
          }),
        ),
      );
      assert.equal(Exit.isFailure(missingFinalization), true);

      const missingReceipt = yield* Effect.exit(
        sql.withTransaction(
          insertAcceptedPrepare({
            suffix: "missing-receipt",
            includeReceipt: false,
            includeFinalization: true,
          }),
        ),
      );
      assert.equal(Exit.isFailure(missingReceipt), true);

      const mismatchCases = [
        ["command", { commandId: "prepare-050-foreign" }],
        ["fingerprint", { fingerprint: "d".repeat(64) }],
        ["project", { projectId: "project-050-foreign" }],
        ["task", { taskId: "task-050-foreign" }],
        ["reservation", { reservationId: "controlled-thread-reservation-050-foreign" }],
        ["event", { eventId: "event-050-foreign" }],
        ["receipt", { receiptCommandId: "prepare-050-foreign-receipt" }],
        ["revision", { resultStreamVersion: 2 }],
        ["sequence", { resultSequence: 999_999 }],
      ] as const;
      for (const [name, finalizationOverrides] of mismatchCases) {
        const failed = yield* Effect.exit(
          sql.withTransaction(
            insertAcceptedPrepare({
              suffix: `mismatch-${name}`,
              includeFinalization: true,
              finalizationOverrides,
            }),
          ),
        );
        assert.equal(Exit.isFailure(failed), true, name);
      }

      const complete = yield* sql.withTransaction(
        insertAcceptedPrepare({ suffix: "complete", includeFinalization: true }),
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_acceptance_obligations
             WHERE prepare_command_id = ${complete.commandId}) AS obligations,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_accepted_evidence
             WHERE prepare_command_id = ${complete.commandId}) AS acceptedEvidence,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_finalizations
             WHERE prepare_command_id = ${complete.commandId}) AS finalizations
        `,
        [{ obligations: 1, acceptedEvidence: 1, finalizations: 1 }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events
             WHERE stream_id LIKE 'controlled-thread-reservation-050-mismatch-%')
              AS events,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_finalizations
             WHERE prepare_command_id LIKE 'prepare-050-mismatch-%')
              AS finalizations
        `,
        [{ events: 0, finalizations: 0 }],
      );
    }),
  );

  it.effect("leaves rejected Prepare and unrelated accepted receipts outside the relation", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_controlled_thread_command_intents (
              command_id, request_fingerprint, intent_fingerprint, command_type,
              authority, aggregate_kind, aggregate_id, project_id, task_id
            ) VALUES (
              'prepare-050-rejected', ${"e".repeat(64)}, ${"e".repeat(64)},
              'agentControl.controlledThreadReservation.prepareInitial',
              'controller', 'controlled-thread-reservation',
              'controlled-thread-reservation-050-rejected',
              'project-050-rejected', 'task-050-rejected'
            )
          `;
          yield* sql`
            INSERT INTO agent_control_command_receipts (
              command_id, command_fingerprint, authority, aggregate_kind,
              aggregate_id, status, result_sequence, result_stream_version,
              event_created, accepted_at, error_code
            ) VALUES (
              'prepare-050-rejected', ${"e".repeat(64)}, 'controller',
              'controlled-thread-reservation',
              'controlled-thread-reservation-050-rejected',
              'rejected', 0, 0, 0, '2026-07-30T08:00:00.000Z',
              'controlled-thread-reservation-identity-conflict'
            )
          `;
          yield* sql`
            INSERT INTO agent_control_command_receipts (
              command_id, command_fingerprint, authority, aggregate_kind,
              aggregate_id, status, result_sequence, result_stream_version,
              event_created, accepted_at, error_code
            ) VALUES (
              'foreign-accepted-050', ${"f".repeat(64)}, 'controller',
              'task', 'task-050-foreign', 'accepted', 1, 1, 1,
              '2026-07-30T08:00:00.000Z', NULL
            )
          `;
        }),
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_prepare_acceptance_obligations
          WHERE prepare_command_id IN (
            'prepare-050-rejected', 'foreign-accepted-050'
          )
        `,
        [{ count: 0 }],
      );
    }),
  );

  it.effect("rejects partial evidence without leaving an outbox row", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const failed = yield* Effect.exit(
        sql.withTransaction(
          sql`
            INSERT INTO agent_control_controlled_thread_prepare_finalizations (
              prepare_command_id, prepare_command_fingerprint,
              project_id, task_id, controlled_thread_reservation_id,
              prepared_event_id, prepared_stream_version,
              prepared_event_sequence, receipt_command_id, receipt_status,
              receipt_result_sequence, receipt_result_stream_version,
              receipt_event_created, receipt_accepted_at,
              finalization_owner_id, status, revision, claimed_at
            ) VALUES (
              'prepare-050-missing', ${"a".repeat(64)},
              'project-050', 'task-050', 'reservation-050',
              'event-050', 1, 1, 'prepare-050-missing', 'accepted',
              1, 1, 1, '2026-07-30T08:00:00.000Z',
              '00000000-0000-0000-0000-000000000050',
              'claimed', 1, '2026-07-30T08:00:00.000Z'
            )
          `,
        ),
      );
      assert.equal(Exit.isFailure(failed), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_prepare_finalizations
          WHERE prepare_command_id = 'prepare-050-missing'
        `,
        [{ count: 0 }],
      );
    }),
  );
});

rollbackLayer("050 prepare finalization rollback", (it) => {
  it.effect("rolls back earlier DDL when a later DDL statement fails", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 49 });
      yield* sql`
        CREATE TABLE migration_050_index_collision (collision INTEGER)
      `;
      yield* sql`
        CREATE INDEX
          idx_agent_control_controlled_thread_prepare_finalizations_open
        ON migration_050_index_collision(collision)
      `;
      const failed = yield* Effect.exit(sql.withTransaction(Migration050));
      assert.equal(Exit.isFailure(failed), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM sqlite_schema
          WHERE type = 'table'
            AND name IN (
              'agent_control_controlled_thread_prepare_finalizations',
              'agent_control_controlled_thread_prepare_accepted_evidence',
              'agent_control_controlled_thread_prepare_acceptance_obligations'
            )
          ORDER BY name
        `,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM sqlite_schema
          WHERE type = 'index'
            AND name =
              'idx_agent_control_controlled_thread_prepare_finalizations_open'
        `,
        [
          {
            name: "idx_agent_control_controlled_thread_prepare_finalizations_open",
          },
        ],
      );
    }),
  );
});
