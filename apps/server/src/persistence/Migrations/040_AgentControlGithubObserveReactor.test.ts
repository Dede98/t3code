import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("040_AgentControlGithubObserveReactor", (it) => {
  it.effect(
    "adds recovery state without changing existing Agent Control or orchestration data",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 39 });

        yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'project-reactor-migration', 'Existing', '/tmp/existing', NULL, '[]',
          '2026-07-23T08:00:00.000Z', '2026-07-23T08:00:00.000Z', NULL
        )
      `;
        yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'manual-reactor-event', 'project', 'project-reactor-migration', 0,
          'project.deleted', '2026-07-23T08:01:00.000Z', NULL, NULL, NULL,
          'server', '{}', '{}'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (
          'project-reactor-migration', '{"fullAccess":false}', 1,
          '2026-07-23T08:02:00.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (
          'project-reactor-migration', 'observe', NULL, 1, 1,
          '2026-07-23T08:02:30.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_github_intake_states (
          project_id, state_json, revision, last_event_sequence, updated_at
        ) VALUES (
          'project-reactor-migration', '{"preserved":true}', 1, 1,
          '2026-07-23T08:02:45.000Z'
        )
      `;
        yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'github-reactor-event', 'github-intake', 'project-reactor-migration', 1,
          'agentControl.github.config.set', '2026-07-23T08:03:00.000Z',
          'github-reactor-command', NULL, 'github-reactor-command', 'human',
          '{}', '{"schemaVersion":1}'
        )
      `;

        yield* runMigrations({ toMigrationInclusive: 40 });

        assert.deepStrictEqual(yield* sql`SELECT event_id FROM orchestration_events`, [
          { event_id: "manual-reactor-event" },
        ]);
        assert.deepStrictEqual(yield* sql`SELECT event_id FROM agent_control_events`, [
          { event_id: "github-reactor-event" },
        ]);
        assert.deepStrictEqual(
          yield* sql`SELECT project_id, revision FROM agent_control_project_policies`,
          [{ project_id: "project-reactor-migration", revision: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT project_id, mode, revision FROM agent_control_project_states`,
          [{ project_id: "project-reactor-migration", mode: "observe", revision: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT project_id, state_json FROM agent_control_github_intake_states`,
          [{ project_id: "project-reactor-migration", state_json: '{"preserved":true}' }],
        );

        yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state,
          consecutive_failures, last_attempt_at, next_attempt_at,
          cooldown_until, reason_code, updated_at
        ) VALUES (
          'project-reactor-migration', '{}', 1, 1, 'active', 'closed',
          0, NULL, '2026-07-23T08:04:00.000Z',
          NULL, NULL, '2026-07-23T08:03:30.000Z'
        )
      `;
        const foreignKeys =
          yield* sql`PRAGMA foreign_key_list(agent_control_github_scheduler_states)`;
        assert.deepStrictEqual(foreignKeys, []);

        const invalidReason = yield* Effect.result(sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state,
          consecutive_failures, last_attempt_at, next_attempt_at,
          cooldown_until, reason_code, updated_at
        ) VALUES (
          'invalid-reason', '{}', 1, 1, 'suspended', 'open',
          0, NULL, NULL, NULL, 'raw-exception',
          '2026-07-23T08:03:30.000Z'
        )
      `);
        assert.equal(invalidReason._tag, "Failure");
      }),
  );
});
