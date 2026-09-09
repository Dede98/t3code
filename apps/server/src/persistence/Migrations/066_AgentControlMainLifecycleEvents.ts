import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { orchestrationEventStorage } from "./060_AgentControlVerificationEvaluation.ts";

/** Upgrade the existing guards without relaxing event routing or JSON validation. */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [name, timing] of [
    ["agent_control_orchestration_event_storage_validate", "AFTER INSERT"],
    ["agent_control_orchestration_event_update_storage_validate", "BEFORE UPDATE"],
  ] as const) {
    yield* sql.unsafe(`DROP TRIGGER main.${name}`).unprepared;
    yield* sql.unsafe(`CREATE TRIGGER main.${name}
      ${timing} ON orchestration_events
      WHEN NOT COALESCE((${orchestrationEventStorage()}), 0)
      BEGIN SELECT RAISE(ABORT, 'invalid orchestration event storage'); END`).unprepared;
  }
});
