import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Corrections retain the original failed result and bind its exact digest. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.agent_control_epic_review_repair_recoveries (
    request_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    original_result_digest TEXT NOT NULL,
    result_json TEXT NOT NULL CHECK (json_valid(result_json)
      AND json_extract(result_json,'$.status')='succeeded'
      AND json_extract(result_json,'$.candidateCommitSha') IS NOT NULL),
    result_digest TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    PRIMARY KEY(request_id,attempt),
    FOREIGN KEY(request_id,attempt)
      REFERENCES agent_control_epic_review_repair_results(request_id,attempt)
  )`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_repair_recoveries_bind
    BEFORE INSERT ON agent_control_epic_review_repair_recoveries
    WHEN NOT EXISTS (
      SELECT 1 FROM agent_control_epic_review_repair_results original
      WHERE original.request_id=NEW.request_id AND original.attempt=NEW.attempt
        AND original.result_digest=NEW.original_result_digest
        AND json_extract(original.result_json,'$.status')='failed'
        AND json_extract(original.result_json,'$.code')='review-repair-turn-failed'
    )
    BEGIN SELECT RAISE(ABORT, 'Recovery must retain the original checkpoint failure'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_repair_recoveries_no_update
    BEFORE UPDATE ON agent_control_epic_review_repair_recoveries
    BEGIN SELECT RAISE(ABORT, 'Epic review checkpoint recovery is immutable'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_review_repair_recoveries_no_delete
    BEFORE DELETE ON agent_control_epic_review_repair_recoveries
    BEGIN SELECT RAISE(ABORT, 'Epic review checkpoint recovery is immutable'); END`;
});
