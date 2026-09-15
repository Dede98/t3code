import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // Leaving retains durable queue history but explicitly restores ordinary selection.
  for (const name of [
    "agent_control_armed_dispatch_evidence_validate",
    "agent_control_armed_system_activation_validate",
    "agent_control_armed_no_candidate_evidence_validate",
  ]) {
    const rows = yield* sql<{
      sql: string;
    }>`SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=${name}`;
    if (rows.length !== 1) return yield* Effect.die(new Error(`Missing Armed guard: ${name}`));
    const alias =
      name === "agent_control_armed_no_candidate_evidence_validate" ? "candidate" : "selected";
    const condition = `queue.project_id=${alias}.project_id`;
    if (rows[0]!.sql.split(condition).length !== 2)
      return yield* Effect.die(new Error(`Divergent Armed guard: ${name}`));
    const source = rows[0]!.sql.replace(
      condition,
      `${condition} AND json_extract(queue.state_json,'$.enabled') IS NOT 0`,
    );
    yield* sql.unsafe(`DROP TRIGGER main.${name}`);
    yield* sql.unsafe(source.replace(/^CREATE TRIGGER /i, "CREATE TRIGGER main."));
  }
});
