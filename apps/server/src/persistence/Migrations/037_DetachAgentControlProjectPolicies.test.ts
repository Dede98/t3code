import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("037_DetachAgentControlProjectPolicies", (it) => {
  it.effect("preserves existing policies while removing the projection foreign key", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 36 });
      yield* sql`
        INSERT INTO projection_projects (
          project_id,
          title,
          workspace_root,
          default_model_selection_json,
          scripts_json,
          created_at,
          updated_at,
          deleted_at
        ) VALUES (
          'project-before-policy-detach',
          'Existing project',
          '/tmp/existing-project',
          NULL,
          '[]',
          '2026-07-22T10:00:00.000Z',
          '2026-07-22T10:00:00.000Z',
          NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id,
          policy_json,
          revision,
          updated_at
        ) VALUES (
          'project-before-policy-detach',
          '{"fullAccess":true}',
          4,
          '2026-07-22T11:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 37 });

      const foreignKeys = yield* sql`PRAGMA foreign_key_list(agent_control_project_policies)`;
      assert.deepStrictEqual(foreignKeys, []);

      yield* sql`
        DELETE FROM projection_projects
        WHERE project_id = 'project-before-policy-detach'
      `;
      const policies = yield* sql<{
        readonly projectId: string;
        readonly policyJson: string;
        readonly revision: number;
        readonly updatedAt: string;
      }>`
        SELECT
          project_id AS "projectId",
          policy_json AS "policyJson",
          revision,
          updated_at AS "updatedAt"
        FROM agent_control_project_policies
      `;
      assert.deepStrictEqual(policies, [
        {
          projectId: "project-before-policy-detach",
          policyJson: '{"fullAccess":true}',
          revision: 4,
          updatedAt: "2026-07-22T11:00:00.000Z",
        },
      ]);
    }),
  );
});
