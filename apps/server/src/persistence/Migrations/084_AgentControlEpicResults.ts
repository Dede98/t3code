import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_epic_capture_intents (
    child_run_id TEXT PRIMARY KEY,
    epic_run_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    input_json TEXT NOT NULL CHECK(json_valid(input_json)),
    source_head TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    tree_sha TEXT NOT NULL,
    code_digest TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE agent_control_epic_capture_results (
    child_run_id TEXT PRIMARY KEY REFERENCES agent_control_epic_capture_intents(child_run_id),
    result_json TEXT NOT NULL CHECK(json_valid(result_json)),
    result_digest TEXT NOT NULL,
    accepted_at TEXT NOT NULL
  )`;
  for (const table of ["intents", "results"]) {
    for (const operation of ["UPDATE", "DELETE"]) {
      yield* sql.unsafe(`CREATE TRIGGER agent_control_epic_capture_${table}_no_${operation.toLowerCase()}
        BEFORE ${operation} ON agent_control_epic_capture_${table}
        BEGIN SELECT RAISE(ABORT,'epic capture evidence is immutable'); END`).unprepared;
    }
  }
});
