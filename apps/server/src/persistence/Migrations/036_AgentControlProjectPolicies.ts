import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_control_project_policies (
      project_id TEXT PRIMARY KEY,
      policy_json TEXT NOT NULL,
      revision INTEGER NOT NULL CHECK (revision >= 1),
      updated_at TEXT NOT NULL,
      FOREIGN KEY (project_id)
        REFERENCES projection_projects(project_id)
        ON DELETE CASCADE
    )
  `;
});
