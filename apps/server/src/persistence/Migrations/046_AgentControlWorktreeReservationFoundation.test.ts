import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-24T10:00:00.000Z";
const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

layer("046_AgentControlWorktreeReservationFoundation", (it) => {
  it.effect("preserves prior Agent Control data and adds isolated reservation constraints", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-before-046', 'stage-run-lease', 'lease-before-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-before-046',
          NULL, 'command-before-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      const eventBefore = yield* sql`
        SELECT * FROM agent_control_events WHERE event_id = 'event-before-046'
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-before-046', 'fingerprint-before-046', 'controller',
          'stage-run-lease', 'lease-before-046', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      const receiptBefore = yield* sql`
        SELECT * FROM agent_control_command_receipts
        WHERE command_id = 'command-before-046'
      `;
      const tables = [
        "agent_control_events",
        "agent_control_command_receipts",
        "agent_control_project_states",
        "agent_control_project_policies",
        "agent_control_github_intake_states",
        "agent_control_github_scheduler_states",
        "agent_control_task_states",
        "agent_control_task_reconcile_states",
        "agent_control_stage_run_states",
        "agent_control_stage_run_lease_states",
      ] as const;
      const before = new Map<string, number>();
      for (const table of tables) {
        before.set(
          table,
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
        );
      }

      yield* runMigrations({ toMigrationInclusive: 46 });

      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM agent_control_events WHERE event_id = 'event-before-046'`,
        eventBefore,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT * FROM agent_control_command_receipts
          WHERE command_id = 'command-before-046'
        `,
        receiptBefore,
      );

      for (const table of tables) {
        assert.equal(
          (yield* sql.unsafe<{ readonly count: number }>(
            `SELECT COUNT(*) AS count FROM ${table}`,
          ))[0]!.count,
          before.get(table),
        );
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_reservation_states')
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_controller_operations')
        `)[0]!.count,
        0,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_worktree_task_stage')
          ORDER BY seqno
        `,
        [
          { name: "project_id" },
          { name: "task_id" },
          { name: "stage_run_id" },
          { name: "attempt_id" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_worktree_branch')
          ORDER BY seqno
        `,
        [{ name: "repository_canonical_key" }, { name: "branch_name" }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT name FROM pragma_index_info('idx_agent_control_worktree_catalog_identity')
          ORDER BY seqno
        `,
        [
          { name: "project_id" },
          { name: "task_id" },
          { name: "stage_run_id" },
          { name: "attempt_id" },
          { name: "lease_id" },
          { name: "fence_token" },
        ],
      );
      yield* sql`
        INSERT INTO agent_control_worktree_stream_catalog (
          reservation_id, project_id, task_id, stage_run_id, attempt_id,
          lease_id, fence_token, created_at
        ) VALUES (
          'catalog-immutable-reservation', 'catalog-project', 'catalog-task',
          'catalog-stage-run', 'catalog-attempt', 'catalog-lease', 1, ${at}
        )
      `;
      assert.equal(
        (yield* Effect.result(sql`
            UPDATE agent_control_worktree_stream_catalog
            SET project_id = 'catalog-mutated-project'
            WHERE reservation_id = 'catalog-immutable-reservation'
          `))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(sql`
            DELETE FROM agent_control_worktree_stream_catalog
            WHERE reservation_id = 'catalog-immutable-reservation'
          `))._tag,
        "Failure",
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT project_id AS "projectId"
          FROM agent_control_worktree_stream_catalog
          WHERE reservation_id = 'catalog-immutable-reservation'
        `,
        [{ projectId: "catalog-project" }],
      );

      const directWithoutCatalog = yield* Effect.result(sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'event-worktree-without-catalog', 'worktree-reservation', 'reservation-046', 1,
            'agentControl.worktree.reserved', ${at}, 'command-worktree-046',
            NULL, 'command-worktree-046', 'controller', '{}', '{"schemaVersion":1}'
          )
        `);
      assert.equal(directWithoutCatalog._tag, "Failure");
      yield* sql`
        INSERT INTO agent_control_worktree_stream_catalog (
          reservation_id, project_id, task_id, stage_run_id, attempt_id,
          lease_id, fence_token, created_at
        ) VALUES (
          'reservation-046', 'project-046', 'task-046', 'stage-046',
          'attempt-046', 'lease-046', 1, ${at}
        )
      `;
      assert.equal(
        (yield* Effect.result(sql`
            INSERT INTO agent_control_worktree_event_envelopes (
              event_id, reservation_id, stream_version, event_type,
              project_id, task_id, stage_run_id, attempt_id, lease_id,
              fence_token, created_at
            ) VALUES (
              'event-wrong-project-046', 'reservation-046', 1,
              'agentControl.worktree.reserved', 'wrong-project', 'task-046',
              'stage-046', 'attempt-046', 'lease-046', 1, ${at}
            )
          `))._tag,
        "Failure",
      );
      yield* sql`
        INSERT INTO agent_control_worktree_event_envelopes (
          event_id, reservation_id, stream_version, event_type,
          project_id, task_id, stage_run_id, attempt_id, lease_id,
          fence_token, created_at
        ) VALUES (
          'event-worktree-046', 'reservation-046', 1,
          'agentControl.worktree.reserved', 'project-046', 'task-046',
          'stage-046', 'attempt-046', 'lease-046', 1, ${at}
        )
      `;
      const validPayload =
        '{"reservationId":"reservation-046","projectId":"project-046",' +
        '"taskId":"task-046","stageRunId":"stage-046","attemptId":"attempt-046",' +
        '"leaseId":"lease-046","fenceToken":1}';
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-worktree-046', 'worktree-reservation', 'reservation-046', 1,
          'agentControl.worktree.reserved', ${at}, 'command-worktree-046',
          NULL, 'command-worktree-046', 'controller', ${validPayload},
          '{"schemaVersion":1}'
        )
      `;
      const rolledBack = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              INSERT INTO agent_control_worktree_stream_catalog (
                reservation_id, project_id, task_id, stage_run_id, attempt_id,
                lease_id, fence_token, created_at
              ) VALUES (
                'reservation-rollback-046', 'project-rollback', 'task-rollback',
                'stage-rollback', 'attempt-rollback', 'lease-rollback', 1, ${at}
              )
            `;
            yield* sql`
              INSERT INTO agent_control_worktree_event_envelopes (
                event_id, reservation_id, stream_version, event_type,
                project_id, task_id, stage_run_id, attempt_id, lease_id,
                fence_token, created_at
              ) VALUES (
                'event-rollback-worktree-046', 'reservation-rollback-046', 1,
                'agentControl.worktree.reserved', 'project-rollback', 'task-rollback',
                'stage-rollback', 'attempt-rollback', 'lease-rollback', 1, ${at}
              )
            `;
            yield* sql`
              INSERT INTO agent_control_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, command_id, causation_event_id, correlation_id,
                actor_authority, payload_json, metadata_json
              ) VALUES (
                'event-rollback-worktree-046', 'worktree-reservation',
                'reservation-rollback-046', 1, 'agentControl.worktree.reserved',
                ${at}, 'command-rollback-worktree-046', NULL,
                'command-rollback-worktree-046', 'controller',
                '{"reservationId":"reservation-rollback-046","projectId":"project-rollback"}',
                '{"schemaVersion":1}'
              )
            `;
          }),
        ),
      );
      assert.equal(rolledBack._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_stream_catalog
          WHERE reservation_id = 'reservation-rollback-046'
        `)[0]!.count,
        0,
      );
      for (const [reservationId, eventId, version, payload] of [
        [
          "reservation-duplicate-046",
          "event-duplicate-046",
          1,
          '{"reservationId":"reservation-duplicate-046",' +
            '"projectId":null,"projectId":"project-negative","taskId":"task-negative",' +
            '"stageRunId":"stage-negative","attemptId":"attempt-negative",' +
            '"leaseId":"lease-negative","fenceToken":1}',
        ],
        [
          "reservation-null-046",
          "event-null-046",
          1,
          '{"reservationId":"reservation-null-046","projectId":null,' +
            '"taskId":"task-negative","stageRunId":"stage-negative",' +
            '"attemptId":"attempt-negative","leaseId":"lease-negative","fenceToken":1}',
        ],
        [
          "reservation-missing-v1-046",
          "event-missing-v1-046",
          2,
          '{"reservationId":"reservation-missing-v1-046","projectId":"project-negative",' +
            '"taskId":"task-negative","stageRunId":"stage-negative",' +
            '"attemptId":"attempt-negative","leaseId":"lease-negative","fenceToken":1}',
        ],
      ] as const) {
        yield* sql`
          INSERT INTO agent_control_worktree_stream_catalog (
            reservation_id, project_id, task_id, stage_run_id, attempt_id,
            lease_id, fence_token, created_at
          ) VALUES (
            ${reservationId}, 'project-negative', 'task-negative', 'stage-negative',
            'attempt-negative', 'lease-negative', 1, ${at}
          )
        `;
        yield* sql`
          INSERT INTO agent_control_worktree_event_envelopes (
            event_id, reservation_id, stream_version, event_type,
            project_id, task_id, stage_run_id, attempt_id, lease_id,
            fence_token, created_at
          ) VALUES (
            ${eventId}, ${reservationId}, ${version},
            ${
              version === 1
                ? "agentControl.worktree.reserved"
                : "agentControl.worktree.materializationStarted"
            },
            'project-negative', 'task-negative', 'stage-negative',
            'attempt-negative', 'lease-negative', 1, ${at}
          )
        `;
        assert.equal(
          (yield* Effect.result(sql`
              INSERT INTO agent_control_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, command_id, causation_event_id, correlation_id,
                actor_authority, payload_json, metadata_json
              ) VALUES (
                ${eventId}, 'worktree-reservation', ${reservationId}, ${version},
                ${
                  version === 1
                    ? "agentControl.worktree.reserved"
                    : "agentControl.worktree.materializationStarted"
                },
                ${at}, ${`command-${eventId}`}, NULL, ${`command-${eventId}`},
                'controller', ${payload}, '{"schemaVersion":1}'
              )
            `))._tag,
          "Failure",
        );
      }
      for (const [suffix, override] of [
        ["wrong-reservation", { reservationId: "foreign-reservation" }],
        ["wrong-project", { projectId: "foreign-project" }],
        ["wrong-task", { taskId: "foreign-task" }],
        ["wrong-stage", { stageRunId: "foreign-stage" }],
        ["wrong-attempt", { attemptId: "foreign-attempt" }],
        ["wrong-lease", { leaseId: "foreign-lease" }],
        ["wrong-fence", { fenceToken: 2 }],
      ] as const) {
        const reservationId = `reservation-${suffix}-046`;
        const eventId = `event-${suffix}-046`;
        yield* sql`
          INSERT INTO agent_control_worktree_stream_catalog (
            reservation_id, project_id, task_id, stage_run_id, attempt_id,
            lease_id, fence_token, created_at
          ) VALUES (
            ${reservationId}, 'project-identity', 'task-identity', 'stage-identity',
            'attempt-identity', 'lease-identity', 1, ${at}
          )
        `;
        yield* sql`
          INSERT INTO agent_control_worktree_event_envelopes (
            event_id, reservation_id, stream_version, event_type,
            project_id, task_id, stage_run_id, attempt_id, lease_id,
            fence_token, created_at
          ) VALUES (
            ${eventId}, ${reservationId}, 1, 'agentControl.worktree.reserved',
            'project-identity', 'task-identity', 'stage-identity',
            'attempt-identity', 'lease-identity', 1, ${at}
          )
        `;
        const payload = yield* encodeJson({
          reservationId,
          projectId: "project-identity",
          taskId: "task-identity",
          stageRunId: "stage-identity",
          attemptId: "attempt-identity",
          leaseId: "lease-identity",
          fenceToken: 1,
          ...override,
        });
        assert.equal(
          (yield* Effect.result(sql`
              INSERT INTO agent_control_events (
                event_id, aggregate_kind, stream_id, stream_version, event_type,
                occurred_at, command_id, causation_event_id, correlation_id,
                actor_authority, payload_json, metadata_json
              ) VALUES (
                ${eventId}, 'worktree-reservation', ${reservationId}, 1,
                'agentControl.worktree.reserved', ${at}, ${`command-${suffix}`},
                NULL, ${`command-${suffix}`}, 'controller', ${payload},
                '{"schemaVersion":1}'
              )
            `))._tag,
          "Failure",
        );
      }
      yield* sql`
        INSERT INTO agent_control_worktree_stream_catalog (
          reservation_id, project_id, task_id, stage_run_id, attempt_id,
          lease_id, fence_token, created_at
        ) VALUES (
          'reservation-wrong-stream-046', 'project-stream', 'task-stream',
          'stage-stream', 'attempt-stream', 'lease-stream', 1, ${at}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_event_envelopes (
          event_id, reservation_id, stream_version, event_type,
          project_id, task_id, stage_run_id, attempt_id, lease_id,
          fence_token, created_at
        ) VALUES (
          'event-wrong-stream-046', 'reservation-wrong-stream-046', 1,
          'agentControl.worktree.reserved', 'project-stream', 'task-stream',
          'stage-stream', 'attempt-stream', 'lease-stream', 1, ${at}
        )
      `;
      assert.equal(
        (yield* Effect.result(sql`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            ) VALUES (
              'event-wrong-stream-046', 'worktree-reservation', 'foreign-stream', 1,
              'agentControl.worktree.reserved', ${at}, 'command-wrong-stream',
              NULL, 'command-wrong-stream', 'controller',
              '{"reservationId":"reservation-wrong-stream-046",' ||
                '"projectId":"project-stream","taskId":"task-stream",' ||
                '"stageRunId":"stage-stream","attemptId":"attempt-stream",' ||
                '"leaseId":"lease-stream","fenceToken":1}',
              '{"schemaVersion":1}'
            )
          `))._tag,
        "Failure",
      );
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          reservation_id, worktree_reservation_id, status, result_json,
          rejection_code, created_at, updated_at, completed_at
        ) VALUES (
          'composite-command-046', 'reserve-and-materialize', ${"a".repeat(64)},
          'project-046', 'task-046', NULL, NULL, 'pending', NULL, NULL,
          ${at}, ${at}, NULL
        )
      `;
      for (const invalid of [
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-invalid-fingerprint', 'reserve-and-materialize', 'not-sha256',
            'project-046', 'task-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-reserve-without-task', 'reserve-and-materialize',
            ${"b".repeat(64)}, 'project-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id,
            status, created_at, updated_at
          ) VALUES (
            'composite-reconcile-without-reservation', 'reconcile',
            ${"c".repeat(64)}, 'project-046', 'pending', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, pending_token, created_at, updated_at
          ) VALUES (
            'composite-partial-claim', 'reserve-and-materialize',
            ${"d".repeat(64)}, 'project-046', 'task-046', 'pending',
            'token-only', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            worktree_reservation_id, status, result_json,
            completed_at, materialization_phase, created_at, updated_at
          ) VALUES (
            'composite-incomplete-accepted', 'reserve-and-materialize',
            ${"e".repeat(64)}, 'project-046', 'task-046', 'reservation-046',
            'accepted', '{}', ${at}, 'terminal', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, rejection_code, result_json, completed_at,
            materialization_phase, created_at, updated_at
          ) VALUES (
            'composite-mixed-rejected', 'reserve-and-materialize',
            ${"f".repeat(64)}, 'project-046', 'task-046', 'rejected',
            'validation', '{}', ${at}, 'terminal', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, created_at, updated_at
          ) VALUES (
            'agent-control-internal-worktree-v1-forged', 'reserve-and-materialize',
            ${"a".repeat(64)}, 'project-046', 'task-046', 'pending', ${at}, ${at}
          )
        `,
        ...[
          {
            commandId: "composite-partial-git-device",
            columns: "git_created_device",
            values: "1",
          },
          {
            commandId: "composite-partial-git-inode",
            columns: "git_created_inode",
            values: "1",
          },
          {
            commandId: "composite-partial-git-dir",
            columns: "git_created_git_dir",
            values: "'/tmp/git-dir'",
          },
          {
            commandId: "composite-partial-marker",
            columns: "marked_ownership_fingerprint",
            values: `'${"1".repeat(64)}'`,
          },
        ].map(({ commandId, columns, values }) =>
          sql.unsafe(`
            INSERT INTO agent_control_worktree_controller_operations (
              command_id, command_type, input_fingerprint, project_id, task_id,
              worktree_reservation_id, status, rejection_code, completed_at,
              materialization_phase, ${columns}, created_at, updated_at
            ) VALUES (
              '${commandId}', 'reserve-and-materialize', '${"2".repeat(64)}',
              'project-046', 'task-046', 'reservation-046', 'rejected',
              'validation', '${at}', 'terminal', ${values}, '${at}', '${at}'
            )
          `),
        ),
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            worktree_reservation_id, status, result_json, result_reservation_id,
            result_revision, result_sequence, completed_at, materialization_phase,
            git_created_device, git_created_inode, git_created_git_dir,
            created_at, updated_at
          ) VALUES (
            'composite-ready-without-marker', 'reserve-and-materialize',
            ${"3".repeat(64)}, 'project-046', 'task-046', 'reservation-046',
            'accepted', '{"status":"ready"}', 'reservation-046', 1, 1, ${at},
            'terminal', 1, 1, '/tmp/git-dir', ${at}, ${at}
          )
        `,
      ]) {
        assert.equal((yield* Effect.result(invalid))._tag, "Failure");
      }
      const acceptedRow = (
        commandId: string,
        resultJson: string,
        resultStatus: string | null,
        gitDevice: number | null,
        gitInode: number | null,
        gitDir: string | null,
        marker: string | null,
      ) => sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          worktree_reservation_id, status, result_json, result_status,
          result_reservation_id, result_revision, result_sequence, completed_at,
          materialization_phase, git_created_device, git_created_inode,
          git_created_git_dir, marked_ownership_fingerprint, created_at, updated_at
        ) VALUES (
          ${commandId}, 'reserve-and-materialize', ${"6".repeat(64)},
          'project-046', 'task-046', 'reservation-046', 'accepted',
          ${resultJson}, ${resultStatus}, 'reservation-046', 1, 1, ${at},
          'terminal', ${gitDevice}, ${gitInode}, ${gitDir}, ${marker}, ${at}, ${at}
        )
      `;
      for (const invalid of [
        acceptedRow("accepted-status-missing", "{}", "ready", 1, 1, "/tmp/git", "a".repeat(64)),
        acceptedRow(
          "accepted-status-null",
          '{"status":null}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-status-null-then-ready",
          '{"status":null,"status":"ready"}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-status-ready-then-null",
          '{"status":"ready","status":null}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-status-ready-twice",
          '{"status":"ready","status":"ready"}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-status-number",
          '{"status":1}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-status-unknown",
          '{"status":"unknown"}',
          "unknown",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-ready-relational-mismatch",
          '{"status":"needs-attention"}',
          "ready",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-attention-relational-mismatch",
          '{"status":"ready"}',
          "needs-attention",
          1,
          1,
          "/tmp/git",
          "a".repeat(64),
        ),
        acceptedRow(
          "accepted-ready-without-git",
          '{"status":"ready"}',
          "ready",
          null,
          null,
          null,
          null,
        ),
        acceptedRow(
          "accepted-ready-without-marker-relational",
          '{"status":"ready"}',
          "ready",
          1,
          1,
          "/tmp/git",
          null,
        ),
        acceptedRow(
          "accepted-partial-git-relational",
          '{"status":"needs-attention"}',
          "needs-attention",
          1,
          null,
          null,
          null,
        ),
      ]) {
        assert.equal((yield* Effect.result(invalid))._tag, "Failure");
      }
      for (const invalid of [
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, result_status, created_at, updated_at
          ) VALUES (
            'pending-with-result-status', 'reserve-and-materialize', ${"7".repeat(64)},
            'project-046', 'task-046', 'pending', 'ready', ${at}, ${at}
          )
        `,
        sql`
          INSERT INTO agent_control_worktree_controller_operations (
            command_id, command_type, input_fingerprint, project_id, task_id,
            status, result_status, rejection_code, materialization_phase,
            completed_at, created_at, updated_at
          ) VALUES (
            'rejected-with-result-status', 'reserve-and-materialize', ${"8".repeat(64)},
            'project-046', 'task-046', 'rejected', 'needs-attention', 'validation',
            'terminal', ${at}, ${at}, ${at}
          )
        `,
      ]) {
        assert.equal((yield* Effect.result(invalid))._tag, "Failure");
      }
      yield* acceptedRow(
        "accepted-valid-attention",
        '{"status":"needs-attention"}',
        "needs-attention",
        null,
        null,
        null,
        null,
      );
      yield* acceptedRow(
        "accepted-valid-ready",
        '{"status":"ready"}',
        "ready",
        1,
        1,
        "/tmp/git",
        "a".repeat(64),
      );
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          worktree_reservation_id, status, rejection_code, completed_at,
          materialization_phase, git_created_device, git_created_inode,
          git_created_git_dir, created_at, updated_at
        ) VALUES (
          'composite-rejected-full-git', 'reserve-and-materialize',
          ${"4".repeat(64)}, 'project-046', 'task-046', 'reservation-046',
          'rejected', 'validation', ${at}, 'terminal', 1, 1, '/tmp/git-dir',
          ${at}, ${at}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          worktree_reservation_id, status, rejection_code, completed_at,
          materialization_phase, git_created_device, git_created_inode,
          git_created_git_dir, marked_ownership_fingerprint, created_at, updated_at
        ) VALUES (
          'composite-rejected-full-marker', 'reserve-and-materialize',
          ${"5".repeat(64)}, 'project-046', 'task-046', 'reservation-046',
          'rejected', 'validation', ${at}, 'terminal', 1, 1, '/tmp/git-dir',
          ${"5".repeat(64)}, ${at}, ${at}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-worktree-rejected-046', 'fingerprint-worktree-046', 'controller',
          'worktree-reservation', 'reservation-046', 'rejected', 1, 1, 0, ${at},
          'lease-expired'
        )
      `;
      const schemaObjectCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM sqlite_master
        WHERE name LIKE 'agent_control_worktree_%'
      `)[0]!.count;
      yield* runMigrations({ toMigrationInclusive: 46 });
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE name LIKE 'agent_control_worktree_%'
        `)[0]!.count,
        schemaObjectCount,
      );
    }),
  );
});

const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

rollbackLayer("046_AgentControlWorktreeReservationFoundation rollback", (it) => {
  it.effect("rolls back the complete migration when a later table conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-rollback-046', 'stage-run-lease', 'lease-rollback-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-rollback-046',
          NULL, 'command-rollback-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      yield* sql`
        CREATE TABLE agent_control_worktree_reservation_states (
          sentinel TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_reservation_states (sentinel)
        VALUES ('must-survive')
      `;

      const migrated = yield* Effect.exit(runMigrations({ toMigrationInclusive: 46 }));
      assert.equal(migrated._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`
          SELECT event_id, aggregate_kind, stream_id
          FROM agent_control_events
          WHERE event_id = 'event-rollback-046'
        `,
        [
          {
            event_id: "event-rollback-046",
            aggregate_kind: "stage-run-lease",
            stream_id: "lease-rollback-046",
          },
        ],
      );
      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT sentinel FROM agent_control_worktree_reservation_states
        `,
        [{ sentinel: "must-survive" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table'
            AND name = 'agent_control_worktree_controller_operations'
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table'
            AND name = 'agent_control_worktree_stream_catalog'
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table'
            AND name IN (
              'agent_control_worktree_event_envelopes',
              'agent_control_worktree_target_claims'
            )
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_sql_migrations
          WHERE migration_id = 46
        `)[0]!.count,
        0,
      );
    }),
  );
});
