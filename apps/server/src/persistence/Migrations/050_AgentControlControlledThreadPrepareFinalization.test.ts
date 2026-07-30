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
const finalizationOwnerId = "00000000-0000-0000-0000-000000000050";

const insertFinalizationStateFromEvidence = (
  sql: SqlClient.SqlClient,
  commandId: string,
  input: {
    readonly initialOwnerId?: string;
    readonly ownerId?: string;
    readonly initialStatus?: string;
    readonly initialRevision?: unknown;
    readonly status?: string;
    readonly revision?: unknown;
    readonly claimedAt?: string | null;
    readonly completedAt?: string | null;
  } = {},
) =>
  sql.unsafe(
    `
      INSERT INTO agent_control_controlled_thread_prepare_finalizations (
        prepare_command_id, prepare_command_fingerprint,
        authority, aggregate_kind, project_id, task_id,
        controlled_thread_reservation_id, prepared_event_id,
        prepared_stream_version, prepared_event_sequence,
        receipt_command_id, receipt_status, receipt_result_sequence,
        receipt_result_stream_version, receipt_event_created,
        receipt_accepted_at, initial_finalization_owner_id,
        initial_status, initial_revision, finalization_owner_id,
        status, revision, claimed_at, completed_at
      )
      SELECT
        prepare_command_id, prepare_command_fingerprint,
        authority, aggregate_kind, project_id, task_id,
        controlled_thread_reservation_id, prepared_event_id,
        prepared_stream_version, prepared_event_sequence,
        receipt_command_id, receipt_status, receipt_result_sequence,
        receipt_result_stream_version, receipt_event_created,
        receipt_accepted_at, ?, ?, ?, ?, ?, ?, ?, ?
      FROM agent_control_controlled_thread_prepare_accepted_evidence
      WHERE prepare_command_id = ?
    `,
    [
      input.initialOwnerId ?? finalizationOwnerId,
      input.initialStatus ?? "pending",
      input.initialRevision ?? 0n,
      input.ownerId ?? finalizationOwnerId,
      input.status ?? "pending",
      input.revision ?? 0n,
      input.claimedAt ?? null,
      input.completedAt ?? null,
      commandId,
    ],
  );

const insertFinalCommitMarkerFromState = (sql: SqlClient.SqlClient, commandId: string) =>
  sql`
    INSERT INTO
      agent_control_controlled_thread_prepare_final_commit_markers (
        prepare_command_id, prepare_command_fingerprint,
        authority, aggregate_kind, project_id, task_id,
        controlled_thread_reservation_id, prepared_event_id,
        prepared_stream_version, prepared_event_sequence,
        receipt_command_id, receipt_status, receipt_result_sequence,
        receipt_result_stream_version, receipt_event_created,
        receipt_accepted_at, finalization_owner_id,
        finalization_status, finalization_revision
      )
    SELECT
      prepare_command_id, prepare_command_fingerprint,
      authority, aggregate_kind, project_id, task_id,
      controlled_thread_reservation_id, prepared_event_id,
      prepared_stream_version, prepared_event_sequence,
      receipt_command_id, receipt_status, receipt_result_sequence,
      receipt_result_stream_version, receipt_event_created,
      receipt_accepted_at, initial_finalization_owner_id,
      initial_status, initial_revision
    FROM agent_control_controlled_thread_prepare_finalizations
    WHERE prepare_command_id = ${commandId}
  `;

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
    const finalizationCommandId = input.finalizationOverrides?.commandId ?? commandId;
    const insertFinalization =
      input.includeFinalization === true
        ? Effect.gen(function* () {
            yield* sql`
            INSERT INTO agent_control_controlled_thread_prepare_finalizations (
              prepare_command_id, prepare_command_fingerprint,
              authority, aggregate_kind, project_id, task_id,
              controlled_thread_reservation_id,
              prepared_event_id, prepared_stream_version,
              prepared_event_sequence, receipt_command_id, receipt_status,
              receipt_result_sequence, receipt_result_stream_version,
              receipt_event_created, receipt_accepted_at,
              initial_finalization_owner_id, initial_status, initial_revision,
              finalization_owner_id, status, revision
            ) VALUES (
              ${finalizationCommandId},
              ${input.finalizationOverrides?.fingerprint ?? fingerprint},
              'controller', 'controlled-thread-reservation',
              ${input.finalizationOverrides?.projectId ?? projectId},
              ${input.finalizationOverrides?.taskId ?? taskId},
              ${input.finalizationOverrides?.reservationId ?? reservationId},
              ${input.finalizationOverrides?.eventId ?? eventId}, 1,
              CAST(${sequence} AS INTEGER),
              ${input.finalizationOverrides?.receiptCommandId ?? commandId}, 'accepted',
              CAST(${input.finalizationOverrides?.resultSequence ?? sequence} AS INTEGER),
              CAST(${input.finalizationOverrides?.resultStreamVersion ?? 1} AS INTEGER),
              1, ${at},
              '00000000-0000-0000-0000-000000000050', 'pending', 0,
              '00000000-0000-0000-0000-000000000050', 'pending', 0
            )
          `;
            yield* sql`
              INSERT INTO
                agent_control_controlled_thread_prepare_final_commit_markers (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, prepared_event_id,
                  prepared_stream_version, prepared_event_sequence,
                  receipt_command_id, receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at, finalization_owner_id,
                  finalization_status, finalization_revision
                )
              SELECT
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                prepared_stream_version, prepared_event_sequence,
                receipt_command_id, receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at, initial_finalization_owner_id,
                initial_status, initial_revision
              FROM agent_control_controlled_thread_prepare_finalizations
              WHERE prepare_command_id = ${finalizationCommandId}
            `;
          })
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
    return {
      commandId,
      fingerprint,
      projectId,
      taskId,
      reservationId,
      eventId,
      sequence,
      at,
    };
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
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_final_commit_markers_%'
          AND name NOT LIKE
            'sqlite_autoindex_agent_control_controlled_thread_prepare_legacy_acceptances_%'
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
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_final_commit_markers_%'
            AND name NOT LIKE
              'sqlite_autoindex_agent_control_controlled_thread_prepare_legacy_acceptances_%'
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
             FROM agent_control_controlled_thread_prepare_legacy_acceptances
             WHERE prepare_command_id = ${legacy.commandId}) AS legacyAcceptances,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_acceptance_obligations
             WHERE prepare_command_id = ${legacy.commandId}) AS obligations,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_accepted_evidence
             WHERE prepare_command_id = ${legacy.commandId}) AS acceptedEvidence
        `,
        [{ legacyAcceptances: 1, obligations: 0, acceptedEvidence: 0 }],
      );
      const retrofit = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              INSERT INTO
                agent_control_controlled_thread_prepare_accepted_evidence (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, prepared_event_id,
                  prepared_stream_version, prepared_event_sequence,
                  receipt_command_id, receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at
                ) VALUES (
                  ${legacy.commandId}, ${legacy.fingerprint},
                  'controller', 'controlled-thread-reservation',
                  ${legacy.projectId}, ${legacy.taskId},
                  ${legacy.reservationId}, ${legacy.eventId},
                  1, ${BigInt(legacy.sequence)}, ${legacy.commandId},
                  'accepted', ${BigInt(legacy.sequence)}, 1, 1, ${legacy.at}
                )
            `;
            yield* sql`
              INSERT INTO agent_control_controlled_thread_prepare_finalizations (
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                prepared_stream_version, prepared_event_sequence,
                receipt_command_id, receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at, initial_finalization_owner_id,
                initial_status, initial_revision, finalization_owner_id,
                status, revision
              ) VALUES (
                ${legacy.commandId}, ${legacy.fingerprint},
                'controller', 'controlled-thread-reservation',
                ${legacy.projectId}, ${legacy.taskId},
                ${legacy.reservationId}, ${legacy.eventId},
                1, ${BigInt(legacy.sequence)}, ${legacy.commandId},
                'accepted', ${BigInt(legacy.sequence)}, 1, 1, ${legacy.at},
                '00000000-0000-0000-0000-000000000050',
                'pending', 0,
                '00000000-0000-0000-0000-000000000050',
                'pending', 0
              )
            `;
          }),
        ),
      );
      assert.equal(Exit.isFailure(retrofit), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_acceptance_obligations
             WHERE prepare_command_id = ${legacy.commandId}) AS obligations,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_accepted_evidence
             WHERE prepare_command_id = ${legacy.commandId}) AS acceptedEvidence,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_finalizations
             WHERE prepare_command_id = ${legacy.commandId}) AS finalizations,
            (SELECT COUNT(*)
             FROM agent_control_controlled_thread_prepare_final_commit_markers
             WHERE prepare_command_id = ${legacy.commandId}) AS markers
        `,
        [{ obligations: 0, acceptedEvidence: 0, finalizations: 0, markers: 0 }],
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

  it.effect("admits only pending@0 inserts and preserves the claim/completion CAS path", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const at = "2026-07-30T09:00:00.000Z";
      const invalidCases = [
        { name: "pending@1", status: "pending", revision: 1n },
        { name: "claimed@0", status: "claimed", revision: 0n, claimedAt: at },
        { name: "claimed@1", status: "claimed", revision: 1n, claimedAt: at },
        {
          name: "completed@0",
          status: "completed",
          revision: 0n,
          claimedAt: at,
          completedAt: at,
        },
        {
          name: "completed@1",
          status: "completed",
          revision: 1n,
          claimedAt: at,
          completedAt: at,
        },
        {
          name: "completed@2",
          status: "completed",
          revision: 2n,
          claimedAt: at,
          completedAt: at,
        },
        { name: "pending-with-claim", status: "pending", revision: 0n, claimedAt: at },
        {
          name: "initial-owner-mismatch",
          initialOwnerId: "00000000-0000-0000-0000-000000000051",
        },
        { name: "initial-status-mismatch", initialStatus: "claimed" },
        { name: "initial-revision-mismatch", initialRevision: 1n },
      ] as const;
      for (const input of invalidCases) {
        const failed = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              const fixture = yield* insertAcceptedPrepare({
                suffix: `initial-${input.name}`,
                includeFinalization: false,
              });
              yield* insertFinalizationStateFromEvidence(sql, fixture.commandId, input);
            }),
          ),
        );
        assert.equal(Exit.isFailure(failed), true, input.name);
      }

      const valid = yield* sql.withTransaction(
        Effect.gen(function* () {
          const fixture = yield* insertAcceptedPrepare({
            suffix: "initial-valid",
            includeFinalization: false,
          });
          yield* insertFinalizationStateFromEvidence(sql, fixture.commandId);
          yield* insertFinalCommitMarkerFromState(sql, fixture.commandId);
          return fixture;
        }),
      );
      yield* sql.withTransaction(sql`
        UPDATE agent_control_controlled_thread_prepare_finalizations
        SET status = 'claimed', revision = revision + 1,
            claimed_at = ${at}
        WHERE prepare_command_id = ${valid.commandId}
          AND status = 'pending' AND revision = 0
      `);
      yield* sql.withTransaction(sql`
        UPDATE agent_control_controlled_thread_prepare_finalizations
        SET status = 'completed', revision = revision + 1,
            completed_at = ${at}
        WHERE prepare_command_id = ${valid.commandId}
          AND status = 'claimed' AND revision = 1
      `);
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, revision, claimed_at AS "claimedAt",
            completed_at AS "completedAt"
          FROM agent_control_controlled_thread_prepare_finalizations
          WHERE prepare_command_id = ${valid.commandId}
        `,
        [{ status: "completed", revision: 2, claimedAt: at, completedAt: at }],
      );
    }),
  );

  it.effect(
    "keeps obligation, evidence, receipt, and finalization order deferred until the marker",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        const fixture = yield* sql.withTransaction(
          Effect.gen(function* () {
            const value = yield* insertAcceptedPrepare({
              suffix: "deferred-state-first",
              includeReceipt: false,
              includeFinalization: false,
            });
            yield* sql`
            INSERT INTO agent_control_controlled_thread_prepare_finalizations (
              prepare_command_id, prepare_command_fingerprint,
              authority, aggregate_kind, project_id, task_id,
              controlled_thread_reservation_id, prepared_event_id,
              prepared_stream_version, prepared_event_sequence,
              receipt_command_id, receipt_status, receipt_result_sequence,
              receipt_result_stream_version, receipt_event_created,
              receipt_accepted_at, initial_finalization_owner_id,
              initial_status, initial_revision, finalization_owner_id,
              status, revision
            ) VALUES (
              ${value.commandId}, ${value.fingerprint}, 'controller',
              'controlled-thread-reservation', ${value.projectId},
              ${value.taskId}, ${value.reservationId}, ${value.eventId},
              1, ${BigInt(value.sequence)}, ${value.commandId}, 'accepted',
              ${BigInt(value.sequence)}, 1, 1, ${value.at},
              ${finalizationOwnerId}, 'pending', 0,
              ${finalizationOwnerId}, 'pending', 0
            )
          `;
            yield* sql`
            INSERT INTO
              agent_control_controlled_thread_prepare_accepted_evidence (
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                prepared_stream_version, prepared_event_sequence,
                receipt_command_id, receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at
              ) VALUES (
                ${value.commandId}, ${value.fingerprint}, 'controller',
                'controlled-thread-reservation', ${value.projectId},
                ${value.taskId}, ${value.reservationId}, ${value.eventId},
                1, ${BigInt(value.sequence)}, ${value.commandId}, 'accepted',
                ${BigInt(value.sequence)}, 1, 1, ${value.at}
              )
          `;
            yield* sql`
            INSERT INTO
              agent_control_controlled_thread_prepare_acceptance_obligations (
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, receipt_command_id,
                receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at
              ) VALUES (
                ${value.commandId}, ${value.fingerprint}, 'controller',
                'controlled-thread-reservation', ${value.projectId},
                ${value.taskId}, ${value.reservationId}, ${value.commandId},
                'accepted', ${BigInt(value.sequence)}, 1, 1, ${value.at}
              )
          `;
            yield* sql`
            INSERT INTO agent_control_command_receipts (
              command_id, command_fingerprint, authority, aggregate_kind,
              aggregate_id, status, result_sequence, result_stream_version,
              event_created, accepted_at, error_code
            ) VALUES (
              ${value.commandId}, ${value.fingerprint}, 'controller',
              'controlled-thread-reservation', ${value.reservationId},
              'accepted', ${value.sequence}, 1, 1, ${value.at}, NULL
            )
          `;
            yield* insertFinalCommitMarkerFromState(sql, value.commandId);
            return value;
          }),
        );
        assert.deepStrictEqual(
          yield* sql`
          SELECT
            (SELECT count(*) FROM
              agent_control_controlled_thread_prepare_acceptance_obligations
             WHERE prepare_command_id = ${fixture.commandId}) AS obligations,
            (SELECT count(*) FROM
              agent_control_controlled_thread_prepare_accepted_evidence
             WHERE prepare_command_id = ${fixture.commandId}) AS evidence,
            (SELECT count(*) FROM
              agent_control_controlled_thread_prepare_finalizations
             WHERE prepare_command_id = ${fixture.commandId}) AS finalizations,
            (SELECT count(*) FROM
              agent_control_controlled_thread_prepare_final_commit_markers
             WHERE prepare_command_id = ${fixture.commandId}) AS markers
        `,
          [{ obligations: 1, evidence: 1, finalizations: 1, markers: 1 }],
        );
      }),
  );

  it.effect("enforces total SQLite integer types on every new numeric evidence field", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const capture = (yield* sql<{ readonly definition: string }>`
          SELECT sql AS definition
          FROM sqlite_schema
          WHERE type = 'trigger'
            AND name =
              'agent_control_controlled_thread_prepare_receipt_acceptance_capture'
        `)[0]!.definition;
      yield* sql`
        DROP TRIGGER
          agent_control_controlled_thread_prepare_receipt_acceptance_capture
      `;

      type NumericTable = "obligation" | "evidence" | "finalization" | "marker";
      const positiveFields = [
        ["obligation", "receipt_result_sequence"],
        ["obligation", "receipt_result_stream_version"],
        ["obligation", "receipt_event_created"],
        ["evidence", "prepared_stream_version"],
        ["evidence", "prepared_event_sequence"],
        ["evidence", "receipt_result_sequence"],
        ["evidence", "receipt_result_stream_version"],
        ["evidence", "receipt_event_created"],
        ["finalization", "prepared_stream_version"],
        ["finalization", "prepared_event_sequence"],
        ["finalization", "receipt_result_sequence"],
        ["finalization", "receipt_result_stream_version"],
        ["finalization", "receipt_event_created"],
        ["marker", "prepared_stream_version"],
        ["marker", "prepared_event_sequence"],
        ["marker", "receipt_result_sequence"],
        ["marker", "receipt_result_stream_version"],
        ["marker", "receipt_event_created"],
      ] as const satisfies ReadonlyArray<readonly [NumericTable, string]>;
      const zeroFields = [
        ["finalization", "initial_revision"],
        ["finalization", "revision"],
        ["marker", "finalization_revision"],
      ] as const satisfies ReadonlyArray<readonly [NumericTable, string]>;
      const positiveInvalidValues = [
        ["numeric-text", "'1'"],
        ["text", "'not-an-integer'"],
        ["real", "1.5"],
        ["blob", "x'01'"],
        ["null", "NULL"],
        ["negative", "-1"],
        ["zero", "0"],
      ] as const;
      const zeroInvalidValues = [
        ["numeric-text", "'0'"],
        ["text", "'not-an-integer'"],
        ["real", "0.5"],
        ["blob", "x'00'"],
        ["null", "NULL"],
        ["negative", "-1"],
        ["positive", "1"],
      ] as const;

      const insertChain = Effect.fn("insertPrepareEvidenceTypeChain")(function* (input: {
        readonly suffix: string;
        readonly table?: NumericTable;
        readonly field?: string;
        readonly expression?: string;
      }) {
        const fixture = yield* insertAcceptedPrepare({
          suffix: input.suffix,
          includeFinalization: false,
        });
        const value = (table: NumericTable, field: string, fallback: string) =>
          input.table === table && input.field === field ? input.expression! : fallback;

        yield* sql.unsafe(
          `
              INSERT INTO
                agent_control_controlled_thread_prepare_acceptance_obligations (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, receipt_command_id,
                  receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at
                )
              SELECT
                intent.command_id, intent.request_fingerprint,
                intent.authority, intent.aggregate_kind,
                intent.project_id, intent.task_id, intent.aggregate_id,
                receipt.command_id, receipt.status,
                ${value("obligation", "receipt_result_sequence", "receipt.result_sequence")},
                ${value(
                  "obligation",
                  "receipt_result_stream_version",
                  "receipt.result_stream_version",
                )},
                ${value("obligation", "receipt_event_created", "receipt.event_created")},
                receipt.accepted_at
              FROM agent_control_controlled_thread_command_intents intent
              JOIN agent_control_command_receipts receipt
                ON receipt.command_id = intent.command_id
              WHERE intent.command_id = ?
            `,
          [fixture.commandId],
        ).unprepared;
        yield* sql.unsafe(
          `
              INSERT INTO
                agent_control_controlled_thread_prepare_accepted_evidence (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, prepared_event_id,
                  prepared_stream_version, prepared_event_sequence,
                  receipt_command_id, receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at
                )
              SELECT
                obligation.prepare_command_id,
                obligation.prepare_command_fingerprint,
                obligation.authority, obligation.aggregate_kind,
                obligation.project_id, obligation.task_id,
                obligation.controlled_thread_reservation_id,
                catalog.event_id,
                ${value("evidence", "prepared_stream_version", "catalog.stream_version")},
                ${value("evidence", "prepared_event_sequence", "event.sequence")},
                obligation.receipt_command_id, obligation.receipt_status,
                ${value(
                  "evidence",
                  "receipt_result_sequence",
                  "obligation.receipt_result_sequence",
                )},
                ${value(
                  "evidence",
                  "receipt_result_stream_version",
                  "obligation.receipt_result_stream_version",
                )},
                ${value("evidence", "receipt_event_created", "obligation.receipt_event_created")},
                obligation.receipt_accepted_at
              FROM
                agent_control_controlled_thread_prepare_acceptance_obligations
                  obligation
              JOIN agent_control_controlled_thread_stream_catalog catalog
                ON catalog.command_id = obligation.prepare_command_id
              JOIN agent_control_events event
                ON event.event_id = catalog.event_id
              WHERE obligation.prepare_command_id = ?
            `,
          [fixture.commandId],
        ).unprepared;
        yield* sql.unsafe(
          `
              INSERT INTO agent_control_controlled_thread_prepare_finalizations (
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                prepared_stream_version, prepared_event_sequence,
                receipt_command_id, receipt_status, receipt_result_sequence,
                receipt_result_stream_version, receipt_event_created,
                receipt_accepted_at, initial_finalization_owner_id,
                initial_status, initial_revision, finalization_owner_id,
                status, revision, claimed_at, completed_at
              )
              SELECT
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                ${value("finalization", "prepared_stream_version", "prepared_stream_version")},
                ${value("finalization", "prepared_event_sequence", "prepared_event_sequence")},
                receipt_command_id, receipt_status,
                ${value("finalization", "receipt_result_sequence", "receipt_result_sequence")},
                ${value(
                  "finalization",
                  "receipt_result_stream_version",
                  "receipt_result_stream_version",
                )},
                ${value("finalization", "receipt_event_created", "receipt_event_created")},
                receipt_accepted_at, ?, 'pending',
                ${value("finalization", "initial_revision", "0")},
                ?, 'pending',
                ${value("finalization", "revision", "0")},
                NULL, NULL
              FROM agent_control_controlled_thread_prepare_accepted_evidence
              WHERE prepare_command_id = ?
            `,
          [finalizationOwnerId, finalizationOwnerId, fixture.commandId],
        ).unprepared;
        yield* sql.unsafe(
          `
              INSERT INTO
                agent_control_controlled_thread_prepare_final_commit_markers (
                  prepare_command_id, prepare_command_fingerprint,
                  authority, aggregate_kind, project_id, task_id,
                  controlled_thread_reservation_id, prepared_event_id,
                  prepared_stream_version, prepared_event_sequence,
                  receipt_command_id, receipt_status, receipt_result_sequence,
                  receipt_result_stream_version, receipt_event_created,
                  receipt_accepted_at, finalization_owner_id,
                  finalization_status, finalization_revision
                )
              SELECT
                prepare_command_id, prepare_command_fingerprint,
                authority, aggregate_kind, project_id, task_id,
                controlled_thread_reservation_id, prepared_event_id,
                ${value("marker", "prepared_stream_version", "prepared_stream_version")},
                ${value("marker", "prepared_event_sequence", "prepared_event_sequence")},
                receipt_command_id, receipt_status,
                ${value("marker", "receipt_result_sequence", "receipt_result_sequence")},
                ${value(
                  "marker",
                  "receipt_result_stream_version",
                  "receipt_result_stream_version",
                )},
                ${value("marker", "receipt_event_created", "receipt_event_created")},
                receipt_accepted_at, initial_finalization_owner_id,
                initial_status,
                ${value("marker", "finalization_revision", "initial_revision")}
              FROM agent_control_controlled_thread_prepare_finalizations
              WHERE prepare_command_id = ?
            `,
          [fixture.commandId],
        ).unprepared;
      });

      const matrix = Effect.gen(function* () {
        let index = 0;
        for (const [table, field] of positiveFields) {
          for (const [label, expression] of positiveInvalidValues) {
            index += 1;
            const failed = yield* Effect.exit(
              sql.withTransaction(
                insertChain({
                  suffix: `type-${index}-${table}-${field}-${label}`,
                  table,
                  field,
                  expression,
                }),
              ),
            );
            assert.equal(Exit.isFailure(failed), true, `${table}.${field}/${label}`);
          }
          index += 1;
          yield* sql.withTransaction(
            insertChain({
              suffix: `type-${index}-${table}-${field}-integer`,
            }),
          );
        }
        for (const [table, field] of zeroFields) {
          for (const [label, expression] of zeroInvalidValues) {
            index += 1;
            const failed = yield* Effect.exit(
              sql.withTransaction(
                insertChain({
                  suffix: `type-${index}-${table}-${field}-${label}`,
                  table,
                  field,
                  expression,
                }),
              ),
            );
            assert.equal(Exit.isFailure(failed), true, `${table}.${field}/${label}`);
          }
          index += 1;
          yield* sql.withTransaction(
            insertChain({
              suffix: `type-${index}-${table}-${field}-integer`,
            }),
          );
        }
      }).pipe(Effect.ensuring(sql.unsafe(capture).unprepared.pipe(Effect.orDie)));
      yield* matrix;
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
              'agent_control_controlled_thread_prepare_acceptance_obligations',
              'agent_control_controlled_thread_prepare_final_commit_markers',
              'agent_control_controlled_thread_prepare_legacy_acceptances'
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
