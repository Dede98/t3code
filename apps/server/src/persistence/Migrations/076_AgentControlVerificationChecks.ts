import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_control_verification_check_manifests (
    provider_delivery_id TEXT PRIMARY KEY,
    handoff_id TEXT NOT NULL,
    fence_token INTEGER NOT NULL,
    worktree_path TEXT NOT NULL,
    code_digest TEXT NOT NULL,
    checks_json TEXT NOT NULL CHECK(json_valid(checks_json)),
    manifest_digest TEXT NOT NULL,
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE agent_control_verification_check_starts (
    provider_delivery_id TEXT NOT NULL REFERENCES agent_control_verification_check_manifests(provider_delivery_id),
    check_id TEXT NOT NULL,
    provider_turn_id TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    started_at TEXT NOT NULL,
    PRIMARY KEY(provider_delivery_id, check_id)
  )`;
  yield* sql`CREATE TABLE agent_control_verification_check_results (
    provider_delivery_id TEXT NOT NULL,
    check_id TEXT NOT NULL,
    provider_turn_id TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    code_digest TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('passed','failed','unavailable','stale')),
    result_json TEXT NOT NULL CHECK(json_valid(result_json)),
    result_digest TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    PRIMARY KEY(provider_delivery_id,check_id),
    FOREIGN KEY(provider_delivery_id,check_id) REFERENCES agent_control_verification_check_starts(provider_delivery_id,check_id)
  )`;
  yield* sql`CREATE TABLE agent_control_verification_check_assessments (
    provider_delivery_id TEXT PRIMARY KEY,
    provider_turn_id TEXT NOT NULL,
    code TEXT CHECK(code IN ('verification-checks-missing','verification-checks-unavailable','verification-checks-stale','verification-checks-failed')),
    digest TEXT NOT NULL,
    sealed_at TEXT NOT NULL
  )`;
  for (const table of ["manifests", "starts", "results", "assessments"]) {
    for (const operation of ["UPDATE", "DELETE"]) {
      yield* sql.unsafe(`CREATE TRIGGER agent_control_verification_check_${table}_no_${operation.toLowerCase()}
        BEFORE ${operation} ON agent_control_verification_check_${table}
        BEGIN SELECT RAISE(ABORT,'verification check evidence is immutable'); END`).unprepared;
    }
  }
});
