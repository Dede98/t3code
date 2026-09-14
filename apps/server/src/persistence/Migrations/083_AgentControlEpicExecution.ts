import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.agent_control_epic_runs (
    epic_run_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 1),
    state_json TEXT NOT NULL CHECK (json_valid(state_json)),
    state_digest TEXT NOT NULL,
    CHECK (json_extract(state_json, '$.epicRunId') = epic_run_id),
    CHECK (json_extract(state_json, '$.projectId') = project_id),
    CHECK (json_extract(state_json, '$.revision') = revision)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_targets (
    project_id TEXT PRIMARY KEY NOT NULL,
    epic_run_id TEXT UNIQUE NOT NULL REFERENCES agent_control_epic_runs(epic_run_id)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_history (
    epic_run_id TEXT NOT NULL REFERENCES agent_control_epic_runs(epic_run_id),
    revision INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    state_digest TEXT NOT NULL,
    PRIMARY KEY (epic_run_id, revision)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_commands (
    command_id TEXT PRIMARY KEY NOT NULL,
    request_digest TEXT NOT NULL,
    epic_run_id TEXT NOT NULL REFERENCES agent_control_epic_runs(epic_run_id)
  )`;
  yield* sql`CREATE TABLE main.agent_control_epic_mode_intents (
    command_id TEXT PRIMARY KEY NOT NULL,
    epic_run_id TEXT NOT NULL REFERENCES agent_control_epic_runs(epic_run_id),
    expected_revision INTEGER NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('armed','observe')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','applied','superseded'))
  )`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_mode_intent_guard BEFORE UPDATE ON agent_control_epic_mode_intents
    WHEN NEW.command_id != OLD.command_id OR NEW.epic_run_id != OLD.epic_run_id
      OR NEW.expected_revision != OLD.expected_revision OR NEW.mode != OLD.mode
      OR OLD.status != 'pending' OR NEW.status NOT IN ('applied','superseded')
    BEGIN SELECT RAISE(ABORT, 'Epic mode authority is immutable'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_mode_intent_no_delete BEFORE DELETE ON agent_control_epic_mode_intents
    BEGIN SELECT RAISE(ABORT, 'Epic mode authority is immutable'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_target_guard BEFORE INSERT ON agent_control_epic_targets
    WHEN NOT EXISTS (SELECT 1 FROM agent_control_epic_runs run WHERE run.epic_run_id=NEW.epic_run_id AND run.project_id=NEW.project_id)
    BEGIN SELECT RAISE(ABORT, 'Epic selection belongs to another project'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_target_no_update BEFORE UPDATE ON agent_control_epic_targets
    BEGIN SELECT RAISE(ABORT, 'Explicitly clear Epic selection before replacing it'); END`;
  for (const table of ["agent_control_epic_history", "agent_control_epic_commands"]) {
    yield* sql.unsafe(
      `CREATE TRIGGER main.${table}_no_update BEFORE UPDATE ON ${table} BEGIN SELECT RAISE(ABORT, 'Epic evidence is immutable'); END`,
    );
    yield* sql.unsafe(
      `CREATE TRIGGER main.${table}_no_delete BEFORE DELETE ON ${table} BEGIN SELECT RAISE(ABORT, 'Epic evidence is immutable'); END`,
    );
  }
  yield* sql`CREATE TRIGGER main.agent_control_epic_revision_guard BEFORE UPDATE ON agent_control_epic_runs
    WHEN NEW.revision != OLD.revision + 1 OR NEW.epic_run_id != OLD.epic_run_id OR NEW.project_id != OLD.project_id
      OR json_extract(NEW.state_json, '$.source') != json_extract(OLD.state_json, '$.source')
      OR json_extract(NEW.state_json, '$.checks') != json_extract(OLD.state_json, '$.checks')
    BEGIN SELECT RAISE(ABORT, 'Epic scope and revision are immutable'); END`;
  // Retain the released Armed guards, replacing only their candidate policy.
  // An Epic is authoritative even when blocked or terminal: no backlog fallback.
  for (const name of [
    "agent_control_armed_dispatch_evidence_validate",
    "agent_control_armed_system_activation_validate",
    "agent_control_armed_no_candidate_evidence_validate",
  ]) {
    const rows = yield* sql<{
      sql: string;
    }>`SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name=${name}`;
    if (rows.length !== 1) return yield* Effect.die(new Error(`Missing Armed guard: ${name}`));
    let source = rows[0]!.sql;
    const alias =
      name === "agent_control_armed_no_candidate_evidence_validate" ? "candidate" : "selected";
    const condition = `${alias}.stage = 'intake'`;
    if (source.split(condition).length !== 2)
      return yield* Effect.die(new Error(`Divergent Armed guard: ${name}`));
    source = source.replace(
      condition,
      `${condition}
      AND (NOT EXISTS (SELECT 1 FROM main.agent_control_epic_targets target WHERE target.project_id=${alias}.project_id)
        OR EXISTS (SELECT 1 FROM main.agent_control_epic_targets target
          JOIN main.agent_control_epic_runs epic ON epic.epic_run_id=target.epic_run_id AND epic.project_id=target.project_id
          WHERE target.project_id=${alias}.project_id AND json_extract(epic.state_json,'$.status')='running'
            AND json_extract(epic.state_json,'$.activeTaskId')=${alias}.task_id))`,
    );
    if (alias === "selected")
      source = source.replace(
        "earlier.stage = 'intake'",
        "earlier.stage = 'intake' AND NOT EXISTS (SELECT 1 FROM main.agent_control_epic_targets target WHERE target.project_id=earlier.project_id)",
      );
    yield* sql.unsafe(`DROP TRIGGER main.${name}`);
    yield* sql.unsafe(source.replace(/^CREATE TRIGGER /i, "CREATE TRIGGER main."));
  }
});
