import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("036_AgentControlProjectPolicies", (it) => {
  it.effect(
    "adds project policies to an existing database without changing existing projects",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;

        yield* runMigrations({ toMigrationInclusive: 35 });
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
          'project-before-policy-migration',
          'Existing project',
          '/tmp/existing-project',
          NULL,
          '[]',
          '2026-07-22T10:00:00.000Z',
          '2026-07-22T10:00:00.000Z',
          NULL
        )
      `;

        yield* runMigrations({ toMigrationInclusive: 36 });

        const columns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(agent_control_project_policies)
      `;
        assert.deepStrictEqual(
          columns.map((column) => column.name),
          ["project_id", "policy_json", "revision", "updated_at"],
        );

        yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id,
          policy_json,
          revision,
          updated_at
        ) VALUES (
          'project-before-policy-migration',
          '{"fullAccess":true}',
          1,
          '2026-07-22T11:00:00.000Z'
        )
      `;

        const projects = yield* sql<{ readonly title: string }>`
        SELECT title
        FROM projection_projects
        WHERE project_id = 'project-before-policy-migration'
      `;
        const policies = yield* sql<{
          readonly projectId: string;
          readonly revision: number;
        }>`
        SELECT project_id AS "projectId", revision
        FROM agent_control_project_policies
      `;
        assert.deepStrictEqual(projects, [{ title: "Existing project" }]);
        assert.deepStrictEqual(policies, [
          { projectId: "project-before-policy-migration", revision: 1 },
        ]);
      }),
  );
});
