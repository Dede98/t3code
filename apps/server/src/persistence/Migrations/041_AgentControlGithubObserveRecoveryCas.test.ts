import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("041_AgentControlGithubObserveRecoveryCas", (it) => {
  it.effect("adds revision 1 in-column and in-json without replacing existing rows", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 40 });
      yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state, consecutive_failures, last_attempt_at,
          next_attempt_at, cooldown_until, reason_code, updated_at
        ) VALUES (
          'migration-041-project',
          '{"schemaVersion":1,"projectId":"migration-041-project","generation":4,"configFingerprint":"fingerprint","pollIntervalSeconds":15,"lastGithubEventSequence":19,"activity":"active","circuitState":"closed","consecutiveFailures":2,"lastAttemptAt":"2026-07-23T08:00:00.000Z","nextAttemptAt":"2026-07-23T08:01:00.000Z","cooldownUntil":null,"reasonCode":"github-timeout","updatedAt":"2026-07-23T08:00:00.000Z"}',
          4, 19, 'active', 'closed', 2, '2026-07-23T08:00:00.000Z',
          '2026-07-23T08:01:00.000Z', NULL, 'github-timeout',
          '2026-07-23T08:00:00.000Z'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (
          'migration-041-project', '{"fullAccess":false}', 7,
          '2026-07-23T08:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 41 });

      assert.deepStrictEqual(
        yield* sql`
          SELECT project_id, generation, last_github_event_sequence,
                 scheduler_revision,
                 json_extract(state_json, '$.schedulerRevision') AS json_revision
          FROM agent_control_github_scheduler_states
        `,
        [
          {
            project_id: "migration-041-project",
            generation: 4,
            last_github_event_sequence: 19,
            scheduler_revision: 1,
            json_revision: 1,
          },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT project_id, revision
          FROM agent_control_project_policies
        `,
        [{ project_id: "migration-041-project", revision: 7 }],
      );
    }),
  );
});
