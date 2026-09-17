import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_epic_integration_intents (
    integration_id TEXT PRIMARY KEY,
    epic_run_id TEXT NOT NULL,
    child_run_id TEXT NOT NULL,
    input_json TEXT NOT NULL CHECK(json_valid(input_json)),
    expected_commit_sha TEXT NOT NULL,
    captured_commit_sha TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    tree_sha TEXT NOT NULL,
    worktree_path TEXT NOT NULL,
    branch_ref TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(epic_run_id, child_run_id, expected_commit_sha)
  )`;
  yield* sql`CREATE TABLE agent_control_epic_integration_results (
    integration_id TEXT PRIMARY KEY REFERENCES agent_control_epic_integration_intents(integration_id),
    result_json TEXT NOT NULL CHECK(json_valid(result_json)),
    result_digest TEXT NOT NULL,
    accepted_at TEXT NOT NULL
  )`;
  for (const table of ["intents", "results"]) {
    for (const operation of ["UPDATE", "DELETE"]) {
      yield* sql.unsafe(`CREATE TRIGGER agent_control_epic_integration_${table}_no_${operation.toLowerCase()}
        BEFORE ${operation} ON agent_control_epic_integration_${table}
        BEGIN SELECT RAISE(ABORT,'epic integration evidence is immutable'); END`).unprepared;
    }
  }
});
