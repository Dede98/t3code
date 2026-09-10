import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ sql: string }>`SELECT sql FROM main.sqlite_schema
    WHERE type = 'trigger' AND name = 'agent_control_run_once_activation_event_validate'`;
  const original = rows[0]?.sql;
  const eventArguments =
    "NEW.project_id, NEW.origin_mode, 'run-once', NULL, NULL, NEW.activated_at";
  const commandArguments = "NEW.activation_expected_revision, 'run-once'";
  if (!original?.includes(eventArguments) || !original.includes(commandArguments)) {
    return yield* Effect.die(
      new Error("Run-Once activation authority trigger is missing or changed"),
    );
  }
  // Both the event's exact shape and the command fingerprint must bind the
  // same optional task ID. Legacy events omit it and retain their old hash.
  const task = "json_extract(CAST(NEW.activation_event_payload_json AS TEXT), '$.runOnceTaskId')";
  const updated = original
    .replace(eventArguments, `${eventArguments}, ${task}`)
    .replace(commandArguments, `${commandArguments}, ${task}`);
  yield* sql`DROP TRIGGER main.agent_control_run_once_activation_event_validate`;
  yield* sql.unsafe(updated).unprepared;
});
