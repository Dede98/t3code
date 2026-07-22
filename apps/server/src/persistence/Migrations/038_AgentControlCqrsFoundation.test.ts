import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("038_AgentControlCqrsFoundation", (it) => {
  it.effect("adds isolated CQRS tables without changing policies or orchestration state", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 37 });

      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          'project-before-cqrs', 'Existing', '/tmp/existing', NULL, '[]',
          '2026-07-22T10:00:00.000Z', '2026-07-22T10:00:00.000Z', NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (
          'project-before-cqrs', '{"fullAccess":true}', 3,
          '2026-07-22T10:05:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'manual-event', 'project', 'project-before-cqrs', 0,
          'project.deleted', '2026-07-22T10:10:00.000Z', NULL, NULL, NULL,
          'server', '{}', '{}'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 38 });

      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'agent_control_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(
        tables.map((row) => row.name),
        [
          "agent_control_command_receipts",
          "agent_control_events",
          "agent_control_project_policies",
          "agent_control_project_states",
          "agent_control_projection_state",
        ],
      );

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND name LIKE 'idx_agent_control_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(
        indexes.map((row) => row.name),
        [
          "idx_agent_control_events_command_id",
          "idx_agent_control_events_correlation_id",
          "idx_agent_control_events_sequence",
          "idx_agent_control_events_stream_sequence",
          "idx_agent_control_events_stream_version",
          "idx_agent_control_project_states_sequence",
          "idx_agent_control_receipts_aggregate",
          "idx_agent_control_receipts_sequence",
        ],
      );

      const policies = yield* sql<{ readonly revision: number }>`
        SELECT revision FROM agent_control_project_policies
      `;
      const manualEvents = yield* sql<{ readonly eventId: string }>`
        SELECT event_id AS "eventId" FROM orchestration_events
      `;
      const projects = yield* sql<{ readonly title: string }>`
        SELECT title FROM projection_projects
      `;
      assert.deepStrictEqual(policies, [{ revision: 3 }]);
      assert.deepStrictEqual(manualEvents, [{ eventId: "manual-event" }]);
      assert.deepStrictEqual(projects, [{ title: "Existing" }]);
    }),
  );
});
