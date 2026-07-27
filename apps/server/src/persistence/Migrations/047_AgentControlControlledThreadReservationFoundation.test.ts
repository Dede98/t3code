import {
  AgentControlControlledThreadReservationPreparedPayload,
  AgentControlControlledThreadReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-26T10:00:00.000Z";
const encodePayload = Schema.encodeUnknownSync(
  Schema.fromJsonString(AgentControlControlledThreadReservationPreparedPayload),
);
const encodeState = Schema.encodeUnknownSync(
  Schema.fromJsonString(AgentControlControlledThreadReservationState),
);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

layer("047_AgentControlControlledThreadReservationFoundation", (it) => {
  it.effect("preserves all prior data and adds isolated reservation constraints", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 46 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-stage', 'stage-run', 'stage-run-047', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-047-stage',
          NULL, 'command-047-stage', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-047-stage', 'fingerprint-047-stage', 'controller', 'stage-run',
          'stage-run-047', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'orchestration-event-047', 'project', 'project-047', 1,
          'project.created', ${at}, 'orchestration-command-047', NULL,
          'orchestration-command-047', 'client', '{}', '{}'
        )
      `;
      const beforeEventSequence = (yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.seq;
      const beforeOrchestration = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM orchestration_events
      `)[0]!.count;
      const beforeReceipts = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
      `)[0]!.count;
      const beforeWorktreeEventTriggers = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_schema
        WHERE type = 'trigger' AND tbl_name = 'agent_control_events'
          AND name LIKE 'agent_control_worktree_event_%'
        ORDER BY name ASC
      `;

      yield* runMigrations({ toMigrationInclusive: 47 });

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM orchestration_events
        `)[0]!.count,
        beforeOrchestration,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
        `)[0]!.count,
        beforeReceipts,
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        beforeEventSequence,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list(
            'agent_control_controlled_thread_reservation_states'
          )
        `)[0]!.count,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name
          FROM pragma_index_info(
            'idx_agent_control_controlled_thread_semantic_position'
          )
          ORDER BY seqno
        `,
        [
          { name: "project_id" },
          { name: "task_id" },
          { name: "stage_run_id" },
          { name: "attempt_id" },
          { name: "role_id" },
          { name: "stage_ordinal" },
          { name: "attempt_ordinal" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name
          FROM sqlite_schema
          WHERE type = 'trigger' AND tbl_name = 'agent_control_events'
            AND name LIKE 'agent_control_worktree_event_%'
          ORDER BY name ASC
        `,
        beforeWorktreeEventTriggers,
      );

      const payload = {
        controlledThreadReservationId: "controlled-thread-reservation-047",
        threadId: "t3-auto-reserved-thread-047",
        projectId: "project-047",
        taskId: "task-047",
        taskRevision: 1,
        githubIntakeSequence: 1,
        sourceIdentityFingerprint: "a".repeat(64),
        stageRunId: "stage-run-047",
        attemptId: "attempt-047",
        roleId: "planning",
        stageKind: "planning",
        stageOrdinal: 1,
        attemptOrdinal: 1,
        leaseId: "lease-047",
        fenceToken: 1,
        worktreeReservationId: "worktree-047",
        status: "prepared",
        preparedAt: at,
      } as const;
      assert.deepStrictEqual(
        yield* sql`
          SELECT "from", "to"
          FROM pragma_foreign_key_list(
            'agent_control_controlled_thread_stream_catalog'
          )
          ORDER BY seq ASC
        `,
        [
          { from: "event_id", to: "event_id" },
          { from: "aggregate_kind", to: "aggregate_kind" },
          { from: "controlled_thread_reservation_id", to: "stream_id" },
          { from: "stream_version", to: "stream_version" },
          { from: "event_type", to: "event_type" },
          { from: "command_id", to: "command_id" },
        ],
      );

      const insertRelationalPair = Effect.fn("insertControlledThreadRelationalPair")(function* (
        suffix: string,
        event: {
          readonly aggregateKind: string;
          readonly streamId?: string;
          readonly streamVersion?: number;
          readonly eventType?: string;
          readonly commandId?: string;
        },
        catalog?: {
          readonly streamVersion?: number;
          readonly eventType?: string;
          readonly commandId?: string;
        },
      ) {
        const reservationId = `controlled-thread-relational-${suffix}`;
        const eventId = `event-047-relational-${suffix}`;
        const commandId = `command-047-relational-${suffix}`;
        const relationalPayload = {
          ...payload,
          controlledThreadReservationId: reservationId,
          threadId: `t3-auto-reserved-thread-relational-${suffix}`,
          stageRunId: `stage-run-relational-${suffix}`,
          attemptId: `attempt-relational-${suffix}`,
          leaseId: `lease-relational-${suffix}`,
          worktreeReservationId: `worktree-relational-${suffix}`,
        };
        const result = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`
                INSERT INTO agent_control_controlled_thread_stream_catalog (
                  controlled_thread_reservation_id, event_id, stream_version,
                  command_id, event_type, thread_id, project_id, task_id,
                  task_revision, github_intake_sequence,
                  source_identity_fingerprint, stage_run_id, attempt_id,
                  role_id, stage_kind, stage_ordinal, attempt_ordinal,
                  lease_id, fence_token, worktree_reservation_id, prepared_at
                ) VALUES (
                  ${reservationId}, ${eventId}, ${catalog?.streamVersion ?? 1},
                  ${catalog?.commandId ?? commandId},
                  ${catalog?.eventType ?? "agentControl.controlledThreadReservation.prepared"},
                  ${relationalPayload.threadId}, ${relationalPayload.projectId},
                  ${relationalPayload.taskId}, ${relationalPayload.taskRevision},
                  ${relationalPayload.githubIntakeSequence},
                  ${relationalPayload.sourceIdentityFingerprint},
                  ${relationalPayload.stageRunId}, ${relationalPayload.attemptId},
                  ${relationalPayload.roleId}, ${relationalPayload.stageKind},
                  ${relationalPayload.stageOrdinal}, ${relationalPayload.attemptOrdinal},
                  ${relationalPayload.leaseId}, ${relationalPayload.fenceToken},
                  ${relationalPayload.worktreeReservationId},
                  ${relationalPayload.preparedAt}
                )
              `;
              yield* sql`
                INSERT INTO agent_control_events (
                  event_id, aggregate_kind, stream_id, stream_version, event_type,
                  occurred_at, command_id, causation_event_id, correlation_id,
                  actor_authority, payload_json, metadata_json
                ) VALUES (
                  ${eventId}, ${event.aggregateKind},
                  ${event.streamId ?? reservationId}, ${event.streamVersion ?? 1},
                  ${event.eventType ?? "agentControl.controlledThreadReservation.prepared"},
                  ${at}, ${event.commandId ?? commandId}, NULL,
                  ${event.commandId ?? commandId}, 'controller',
                  ${encodePayload(relationalPayload)}, '{"schemaVersion":1}'
                )
              `;
            }),
          ),
        );
        assert.equal(result._tag, "Failure", suffix);
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT COUNT(*) FROM agent_control_controlled_thread_stream_catalog
                WHERE event_id = ${eventId}) AS catalog,
              (SELECT COUNT(*) FROM agent_control_events
                WHERE event_id = ${eventId}) AS events
          `,
          [{ catalog: 0, events: 0 }],
        );
      });
      for (const foreign of [
        {
          suffix: "stage-run",
          aggregateKind: "stage-run",
          eventType: "agentControl.stageRun.prepared",
        },
        {
          suffix: "lease",
          aggregateKind: "stage-run-lease",
          eventType: "agentControl.stageRunLease.reserved",
        },
        {
          suffix: "worktree",
          aggregateKind: "worktree-reservation",
          eventType: "agentControl.worktree.reserved",
        },
        {
          suffix: "task",
          aggregateKind: "task",
          eventType: "agentControl.task.created",
        },
      ] as const) {
        yield* insertRelationalPair(foreign.suffix, {
          aggregateKind: foreign.aggregateKind,
          eventType: foreign.eventType,
        });
      }
      yield* insertRelationalPair("stream", {
        aggregateKind: "controlled-thread-reservation",
        streamId: "controlled-thread-relational-other-stream",
      });
      yield* insertRelationalPair(
        "event-type",
        {
          aggregateKind: "controlled-thread-reservation",
        },
        {
          eventType: "agentControl.stageRun.prepared",
        },
      );
      yield* insertRelationalPair("command", {
        aggregateKind: "controlled-thread-reservation",
        commandId: "command-047-relational-command-other",
      });
      yield* insertRelationalPair("stream-version", {
        aggregateKind: "controlled-thread-reservation",
        streamVersion: 2,
      });

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_controlled_thread_stream_catalog (
              controlled_thread_reservation_id, event_id, stream_version,
              command_id, event_type, thread_id, project_id, task_id,
              task_revision, github_intake_sequence, source_identity_fingerprint,
              stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
              attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
              prepared_at
            ) VALUES (
              ${payload.controlledThreadReservationId}, 'event-047-reservation', 1,
              'command-047-reservation',
              'agentControl.controlledThreadReservation.prepared',
              ${payload.threadId}, ${payload.projectId}, ${payload.taskId},
              ${payload.taskRevision}, ${payload.githubIntakeSequence},
              ${payload.sourceIdentityFingerprint}, ${payload.stageRunId},
              ${payload.attemptId}, ${payload.roleId}, ${payload.stageKind},
              ${payload.stageOrdinal}, ${payload.attemptOrdinal}, ${payload.leaseId},
              ${payload.fenceToken}, ${payload.worktreeReservationId},
              ${payload.preparedAt}
            )
          `;
          yield* sql`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            ) VALUES (
              'event-047-reservation', 'controlled-thread-reservation',
              ${payload.controlledThreadReservationId}, 1,
              'agentControl.controlledThreadReservation.prepared', ${at},
              'command-047-reservation', NULL, 'command-047-reservation',
              'controller', ${encodePayload(payload)}, '{"schemaVersion":1}'
            )
          `;
        }),
      );
      const eventSequence = (yield* sql<{ readonly sequence: number }>`
        SELECT sequence FROM agent_control_events
        WHERE event_id = 'event-047-reservation'
      `)[0]!.sequence;
      const state = {
        schemaVersion: 1,
        ...payload,
        revision: 1,
        sequence: eventSequence,
      } as const;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_reservation_states (
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          status, revision, last_event_sequence, prepared_at, state_json
        ) VALUES (
          ${payload.controlledThreadReservationId}, ${payload.threadId},
          ${payload.projectId}, ${payload.taskId}, ${payload.taskRevision},
          ${payload.githubIntakeSequence}, ${payload.sourceIdentityFingerprint},
          ${payload.stageRunId}, ${payload.attemptId}, ${payload.roleId},
          ${payload.stageKind}, ${payload.stageOrdinal}, ${payload.attemptOrdinal},
          ${payload.leaseId}, ${payload.fenceToken}, ${payload.worktreeReservationId},
          ${payload.status}, 1, ${eventSequence}, ${at}, ${encodeState(state)}
        )
      `;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_controlled_thread_command_intents (
              command_id, request_fingerprint, intent_fingerprint, command_type,
              authority, aggregate_kind, aggregate_id, project_id, task_id
            ) VALUES (
              'command-047-rejected', ${"b".repeat(64)}, ${"b".repeat(64)},
              'agentControl.controlledThreadReservation.prepareInitial',
              'controller', 'controlled-thread-reservation',
              'controlled-thread-reservation-rejected-047',
              'project-047', 'task-047'
            )
          `;
          yield* sql`
            INSERT INTO agent_control_command_receipts (
              command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
              status, result_sequence, result_stream_version, event_created,
              accepted_at, error_code
            ) VALUES (
              'command-047-rejected', ${"b".repeat(64)}, 'controller',
              'controlled-thread-reservation',
              'controlled-thread-reservation-rejected-047',
              'rejected', 0, 0, 0, ${at},
              'controlled-thread-reservation-identity-conflict'
            )
          `;
        }),
      );
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO agent_control_controlled_thread_command_intents (
              command_id, request_fingerprint, intent_fingerprint, command_type,
              authority, aggregate_kind, aggregate_id, project_id, task_id,
              controlled_thread_reservation_id, thread_id, task_revision,
              github_intake_sequence, source_identity_fingerprint, stage_run_id,
              attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
              lease_id, fence_token, worktree_reservation_id, expected_revision
            ) VALUES (
              'command-047-reservation', ${"c".repeat(64)}, ${"d".repeat(64)},
              'agentControl.controlledThreadReservation.prepare', 'controller',
              'controlled-thread-reservation',
              ${payload.controlledThreadReservationId}, ${payload.projectId},
              ${payload.taskId}, ${payload.controlledThreadReservationId},
              ${payload.threadId}, ${payload.taskRevision},
              ${payload.githubIntakeSequence}, ${payload.sourceIdentityFingerprint},
              ${payload.stageRunId}, ${payload.attemptId}, ${payload.roleId},
              ${payload.stageKind}, ${payload.stageOrdinal}, ${payload.attemptOrdinal},
              ${payload.leaseId}, ${payload.fenceToken},
              ${payload.worktreeReservationId}, 0
            )
          `;
          yield* sql`
            INSERT INTO agent_control_command_receipts (
              command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
              status, result_sequence, result_stream_version, event_created,
              accepted_at, error_code
            ) VALUES (
              'command-047-reservation', ${"c".repeat(64)}, 'controller',
              'controlled-thread-reservation',
              ${payload.controlledThreadReservationId}, 'accepted',
              ${eventSequence}, 1, 1, ${at}, NULL
            )
          `;
        }),
      );

      const invalidReceiptCases = [
        {
          commandId: "command-047-invalid-accepted-zero",
          status: "accepted",
          resultSequence: 0,
          resultStreamVersion: 0,
          eventCreated: 1,
          errorCode: null,
        },
        {
          commandId: "command-047-invalid-accepted-event",
          status: "accepted",
          resultSequence: eventSequence,
          resultStreamVersion: 1,
          eventCreated: 0,
          errorCode: null,
        },
        {
          commandId: "command-047-invalid-rejected-coordinates",
          status: "rejected",
          resultSequence: eventSequence,
          resultStreamVersion: 1,
          eventCreated: 0,
          errorCode: "state-not-available",
        },
      ] as const;
      for (const invalid of invalidReceiptCases) {
        const rejected = yield* Effect.result(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`
                INSERT INTO agent_control_controlled_thread_command_intents (
                  command_id, request_fingerprint, intent_fingerprint, command_type,
                  authority, aggregate_kind, aggregate_id, project_id, task_id
                ) VALUES (
                  ${invalid.commandId}, ${"e".repeat(64)}, ${"e".repeat(64)},
                  'agentControl.controlledThreadReservation.prepareInitial',
                  'controller', 'controlled-thread-reservation',
                  'controlled-thread-reservation-invalid-receipt',
                  'project-047', 'task-047'
                )
              `;
              yield* sql`
                INSERT INTO agent_control_command_receipts (
                  command_id, command_fingerprint, authority, aggregate_kind,
                  aggregate_id, status, result_sequence, result_stream_version,
                  event_created, accepted_at, error_code
                ) VALUES (
                  ${invalid.commandId}, ${"e".repeat(64)}, 'controller',
                  'controlled-thread-reservation',
                  'controlled-thread-reservation-invalid-receipt',
                  ${invalid.status}, ${invalid.resultSequence},
                  ${invalid.resultStreamVersion}, ${invalid.eventCreated},
                  ${at}, ${invalid.errorCode}
                )
              `;
            }),
          ),
        );
        assert.equal(rejected._tag, "Failure");
      }

      assert.equal(
        (yield* Effect.result(sql`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE event_id = 'event-047-reservation'
        `))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(sql`
          DELETE FROM agent_control_events
          WHERE event_id = 'event-047-reservation'
        `))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(sql`
          UPDATE agent_control_controlled_thread_reservation_states
          SET state_json = '{}'
          WHERE controlled_thread_reservation_id =
            'controlled-thread-reservation-047'
        `))._tag,
        "Failure",
      );
      const competing = yield* Effect.result(sql`
        INSERT INTO agent_control_controlled_thread_reservation_states (
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          status, revision, last_event_sequence, prepared_at, state_json
        ) VALUES (
          'controlled-thread-reservation-047-other',
          't3-auto-reserved-thread-047-other', 'project-047', 'task-047', 1, 1,
          'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
          'stage-run-047', 'attempt-047', 'planning', 'planning', 1, 1,
          'lease-other', 2, 'worktree-other', 'prepared', 1, 3, ${at},
          ${encodeState({ ...state, controlledThreadReservationId: "controlled-thread-reservation-047-other", threadId: "t3-auto-reserved-thread-047-other", leaseId: "lease-other", fenceToken: 2, worktreeReservationId: "worktree-other", sequence: 3 })}
        )
      `);
      assert.equal(competing._tag, "Failure");

      const invalidEvent = yield* Effect.result(sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-invalid', 'controlled-thread-reservation',
          'controlled-thread-reservation-invalid', 1, 'thread.created', ${at},
          'command-047-invalid', NULL, 'command-047-invalid',
          'controller', '{}', '{"schemaVersion":1}'
        )
      `);
      assert.equal(invalidEvent._tag, "Failure");

      const insertInvalidEvent = Effect.fn("insertInvalidControlledThreadEvent")(function* (
        suffix: string,
        rawPayloadJson: string,
      ) {
        const reservationId = `controlled-thread-reservation-invalid-${suffix}`;
        const eventId = `event-047-invalid-${suffix}`;
        const commandId = `command-047-invalid-${suffix}`;
        return yield* Effect.result(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`
                  INSERT INTO agent_control_controlled_thread_stream_catalog (
                    controlled_thread_reservation_id, event_id, stream_version,
                    command_id, event_type, thread_id, project_id, task_id,
                    task_revision, github_intake_sequence,
                    source_identity_fingerprint, stage_run_id, attempt_id,
                    role_id, stage_kind, stage_ordinal, attempt_ordinal,
                    lease_id, fence_token, worktree_reservation_id, prepared_at
                  ) VALUES (
                    ${reservationId}, ${eventId}, 1, ${commandId},
                    'agentControl.controlledThreadReservation.prepared',
                    ${`t3-auto-reserved-thread-invalid-${suffix}`},
                    'project-047', 'task-047', 1, 1, ${"a".repeat(64)},
                    ${`stage-run-invalid-${suffix}`},
                    ${`attempt-invalid-${suffix}`}, 'planning', 'planning',
                    1, 1, ${`lease-invalid-${suffix}`}, 1,
                    ${`worktree-invalid-${suffix}`}, ${at}
                  )
                `;
              yield* sql`
                  INSERT INTO agent_control_events (
                    event_id, aggregate_kind, stream_id, stream_version,
                    event_type, occurred_at, command_id, causation_event_id,
                    correlation_id, actor_authority, payload_json, metadata_json
                  ) VALUES (
                    ${eventId}, 'controlled-thread-reservation', ${reservationId},
                    1, 'agentControl.controlledThreadReservation.prepared',
                    ${at}, ${commandId}, NULL, ${commandId}, 'controller',
                    ${rawPayloadJson}, '{"schemaVersion":1}'
                  )
                `;
            }),
          ),
        );
      });
      const invalidPayload = (suffix: string) => ({
        ...payload,
        controlledThreadReservationId: `controlled-thread-reservation-invalid-${suffix}`,
        threadId: `t3-auto-reserved-thread-invalid-${suffix}`,
        stageRunId: `stage-run-invalid-${suffix}`,
        attemptId: `attempt-invalid-${suffix}`,
        leaseId: `lease-invalid-${suffix}`,
        worktreeReservationId: `worktree-invalid-${suffix}`,
      });
      const { roleId: _missingRole, ...missingRolePayload } = invalidPayload("missing-role");
      assert.equal(
        (yield* insertInvalidEvent("missing-role", encodeUnknownJson(missingRolePayload)))._tag,
        "Failure",
      );
      assert.equal(
        (yield* insertInvalidEvent(
          "wrong-type",
          encodeUnknownJson({
            ...invalidPayload("wrong-type"),
            taskRevision: "1",
          }),
        ))._tag,
        "Failure",
      );
      const duplicatePayloadJson = encodeUnknownJson(invalidPayload("duplicate-key")).replace(
        "{",
        '{"taskId":"duplicate-task",',
      );
      assert.equal(
        (yield* insertInvalidEvent("duplicate-key", duplicatePayloadJson))._tag,
        "Failure",
      );

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 47 }), []);
    }),
  );
});

rollbackLayer("047_AgentControlControlledThreadReservationFoundation rollback", (it) => {
  it.effect("rolls both additive constraint rebuilds back when the final table conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 46 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-047-rollback-stage', 'stage-run', 'stage-run-047-rollback', 1,
          'agentControl.stageRun.prepared', ${at}, 'command-047-rollback-stage',
          NULL, 'command-047-rollback-stage', 'controller',
          '{}', '{"schemaVersion":1}'
        )
      `;
      const beforeSequence = (yield* sql<{ readonly seq: number }>`
        SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.seq;
      yield* sql`
        CREATE TABLE agent_control_controlled_thread_reservation_states (
          conflicting_column TEXT
        )
      `;

      const failed = yield* Effect.exit(runMigrations({ toMigrationInclusive: 47 }));
      assert.equal(Exit.isFailure(failed), true);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE event_id = 'event-047-rollback-stage'
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        beforeSequence,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM effect_sql_migrations
          WHERE migration_id = 47
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_table_info('agent_control_events')
          WHERE name = 'aggregate_kind'
        `)[0]!.count,
        1,
      );
    }),
  );
});
