import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_run_once_diagnostics (
    project_id TEXT PRIMARY KEY,
    run_id TEXT,
    step TEXT,
    error_code TEXT NOT NULL,
    updated_at TEXT NOT NULL
  )`;
});
