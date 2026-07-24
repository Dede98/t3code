import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-24T10:00:00.000Z";

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

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-worktree-046', 'worktree-reservation', 'reservation-046', 1,
          'agentControl.worktree.reserved', ${at}, 'command-worktree-046',
          NULL, 'command-worktree-046', 'controller', '{}', '{"schemaVersion":1}'
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
    }),
  );
});
