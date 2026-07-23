import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Adds the compare-and-swap token used by the Observe scheduler. Existing
 * migration-040 rows become revision 1 without rebuilding or deleting any
 * Agent Control, GitHub, policy, or orchestration data.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    ALTER TABLE agent_control_github_scheduler_states
    ADD COLUMN scheduler_revision INTEGER NOT NULL DEFAULT 1
      CHECK (scheduler_revision >= 1)
  `;
  yield* sql`
    UPDATE agent_control_github_scheduler_states
    SET state_json = json_set(state_json, '$.schedulerRevision', 1)
    WHERE json_valid(state_json)
  `;
});
