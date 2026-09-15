import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.agent_control_epic_queues (
    project_id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK(revision >= 1),
    state_json TEXT NOT NULL CHECK(json_valid(state_json)),
    state_digest TEXT NOT NULL,
    next_check_at TEXT,
    CHECK(json_extract(state_json,'$.projectId') = project_id),
    CHECK(json_extract(state_json,'$.revision') = revision)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_queue_history (
    project_id TEXT NOT NULL REFERENCES agent_control_epic_queues(project_id),
    revision INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    state_digest TEXT NOT NULL,
    PRIMARY KEY(project_id,revision)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_queue_commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    request_digest TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES agent_control_epic_queues(project_id)
  )`;
  for (const table of ["agent_control_epic_queue_history", "agent_control_epic_queue_commands"]) {
    for (const operation of ["UPDATE", "DELETE"]) {
      yield* sql.unsafe(
        `CREATE TRIGGER main.${table}_no_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT, 'Epic queue evidence is immutable'); END`,
      );
    }
  }
  yield* sql`CREATE TRIGGER main.agent_control_epic_queue_revision_guard BEFORE UPDATE ON agent_control_epic_queues
    WHEN NEW.project_id != OLD.project_id OR (NEW.revision != OLD.revision + 1 AND (NEW.revision != OLD.revision OR NEW.state_json != OLD.state_json OR NEW.state_digest != OLD.state_digest))
    BEGIN SELECT RAISE(ABORT, 'Epic queue revision is immutable'); END`;
  // Empty or blocked queues never fall back to the ordinary task backlog.
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
    const condition = `${alias}.stage = 'intake'`;
    if (rows[0]!.sql.split(condition).length !== 2)
      return yield* Effect.die(new Error(`Divergent Armed guard: ${name}`));
    const source = rows[0]!.sql.replace(
      condition,
      `${condition}
      AND (NOT EXISTS (SELECT 1 FROM main.agent_control_epic_queues queue WHERE queue.project_id=${alias}.project_id)
        OR EXISTS (SELECT 1 FROM main.agent_control_epic_targets target WHERE target.project_id=${alias}.project_id))`,
    );
    yield* sql.unsafe(`DROP TRIGGER main.${name}`);
    yield* sql.unsafe(source.replace(/^CREATE TRIGGER /i, "CREATE TRIGGER main."));
  }
});
