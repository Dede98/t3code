import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("039_AgentControlGithubObserveFoundation", (it) => {
  it.effect("widens CQRS checks while preserving all existing Agent Control data", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 38 });

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'existing-event', 'project-controller', 'project-existing', 1,
          'agentControl.project.mode.changed', '2026-07-22T10:00:00.000Z',
          'existing-command', NULL, 'existing-command', 'human',
          '{"existing":true}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'existing-command', 'fingerprint', 'human', 'project-controller',
          'project-existing', 'accepted', 1, 1, 1,
          '2026-07-22T10:00:00.000Z', NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (
          'project-existing', 'observe', NULL, 1, 1, '2026-07-22T10:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (
          'project-existing', '{"fullAccess":false}', 2, '2026-07-22T09:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 39 });

      assert.deepStrictEqual(
        yield* sql`
          SELECT event_id, aggregate_kind, stream_id, stream_version, event_type,
                 actor_authority, payload_json
          FROM agent_control_events
        `,
        [
          {
            event_id: "existing-event",
            aggregate_kind: "project-controller",
            stream_id: "project-existing",
            stream_version: 1,
            event_type: "agentControl.project.mode.changed",
            actor_authority: "human",
            payload_json: '{"existing":true}',
          },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT command_id, command_fingerprint, authority FROM agent_control_command_receipts`,
        [
          {
            command_id: "existing-command",
            command_fingerprint: "fingerprint",
            authority: "human",
          },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT project_id, mode, revision FROM agent_control_project_states`,
        [{ project_id: "project-existing", mode: "observe", revision: 1 }],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT project_id, revision FROM agent_control_project_policies`,
        [{ project_id: "project-existing", revision: 2 }],
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'github-event', 'github-intake', 'project-existing', 1,
          'agentControl.github.config.set', '2026-07-22T11:00:00.000Z',
          'github-command', NULL, 'github-command', 'human', '{}', '{"schemaVersion":1}'
        )
      `;
      const invalid = yield* Effect.result(sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'invalid-event', 'github-intake', 'project-existing', 2,
          'agentControl.github.issue.write', '2026-07-22T11:01:00.000Z',
          'invalid-command', NULL, 'invalid-command', 'controller', '{}', '{}'
        )
      `);
      assert.equal(invalid._tag, "Failure");

      const projectionTables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name LIKE 'agent_control_github_%'
        ORDER BY name
      `;
      assert.deepStrictEqual(
        projectionTables.map(({ name }) => name),
        [
          "agent_control_github_intake_states",
          "agent_control_github_issues",
          "agent_control_github_timeline_events",
        ],
      );
    }),
  );
});
