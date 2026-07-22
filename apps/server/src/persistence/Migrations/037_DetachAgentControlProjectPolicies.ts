import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE agent_control_project_policies_rebuild (
      project_id TEXT PRIMARY KEY,
      policy_json TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      updated_at TEXT NOT NULL
    )
  `;

  yield* sql`
    INSERT INTO agent_control_project_policies_rebuild (
      project_id,
      policy_json,
      revision,
      updated_at
    )
    SELECT
      project_id,
      policy_json,
      revision,
      updated_at
    FROM agent_control_project_policies
  `;

  yield* sql`DROP TABLE agent_control_project_policies`;
  yield* sql`
    ALTER TABLE agent_control_project_policies_rebuild
    RENAME TO agent_control_project_policies
  `;
});
