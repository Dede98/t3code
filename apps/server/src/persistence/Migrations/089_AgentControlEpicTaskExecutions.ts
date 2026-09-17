import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE main.agent_control_epic_task_executions (
    execution_id TEXT PRIMARY KEY NOT NULL,
    epic_run_id TEXT NOT NULL REFERENCES agent_control_epic_runs(epic_run_id),
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL UNIQUE,
    plan_digest TEXT NOT NULL CHECK(length(plan_digest)=64),
    base_commit_sha TEXT NOT NULL,
    project_revision INTEGER NOT NULL CHECK(project_revision>=1),
    stage_run_id TEXT, lease_id TEXT, worktree_reservation_id TEXT,
    controlled_thread_reservation_id TEXT, thread_id TEXT,
    phase TEXT NOT NULL CHECK(phase IN ('reserved','stage-prepared','lease-reserved','worktree-ready','thread-activated')),
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  ) STRICT`;
  yield* sql`CREATE INDEX main.idx_epic_task_executions_project
    ON agent_control_epic_task_executions(project_id,epic_run_id,phase)`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_task_execution_identity
    BEFORE UPDATE ON agent_control_epic_task_executions
    WHEN NEW.execution_id!=OLD.execution_id OR NEW.epic_run_id!=OLD.epic_run_id
      OR NEW.project_id!=OLD.project_id OR NEW.task_id!=OLD.task_id
      OR NEW.plan_digest!=OLD.plan_digest OR NEW.base_commit_sha!=OLD.base_commit_sha
      OR NEW.project_revision!=OLD.project_revision OR NEW.created_at!=OLD.created_at
      OR (OLD.stage_run_id IS NOT NULL AND NEW.stage_run_id IS NOT OLD.stage_run_id)
      OR (OLD.lease_id IS NOT NULL AND NEW.lease_id IS NOT OLD.lease_id)
      OR (OLD.worktree_reservation_id IS NOT NULL AND NEW.worktree_reservation_id IS NOT OLD.worktree_reservation_id)
      OR (OLD.controlled_thread_reservation_id IS NOT NULL AND NEW.controlled_thread_reservation_id IS NOT OLD.controlled_thread_reservation_id)
      OR (OLD.thread_id IS NOT NULL AND NEW.thread_id IS NOT OLD.thread_id)
    BEGIN SELECT RAISE(ABORT,'epic task execution identity is immutable'); END`;
  yield* sql`CREATE TRIGGER main.agent_control_epic_task_execution_no_delete
    BEFORE DELETE ON agent_control_epic_task_executions
    BEGIN SELECT RAISE(ABORT,'epic task execution authority cannot be deleted'); END`;

  yield* sql`CREATE VIEW main.agent_control_task_execution_authority AS
    SELECT run.run_id,run.project_id,run.task_id,run.status,run.last_step,run.worktree_reservation_id,'run-once' AS active_mode
    FROM agent_control_run_once_states run
    UNION ALL
    SELECT execution.execution_id,execution.project_id,execution.task_id,'active',execution.phase,execution.worktree_reservation_id,'armed'
    FROM agent_control_epic_task_executions execution
    JOIN agent_control_epic_targets target ON target.epic_run_id=execution.epic_run_id AND target.project_id=execution.project_id
    JOIN agent_control_epic_runs epic ON epic.epic_run_id=execution.epic_run_id
    JOIN json_each(epic.state_json,'$.members') member
      ON json_extract(member.value,'$.taskId')=execution.task_id
      AND json_extract(member.value,'$.childRunId')=execution.execution_id
    WHERE json_extract(epic.state_json,'$.status')='running' AND json_extract(member.value,'$.status')='running'
      AND json_extract(epic.state_json,'$.dependencyPlanDigest')=execution.plan_digest`;
  // Repairs retain their existing one-attempt journal and all verification
  // evidence checks. Its execution key may now identify either admitted path.
  const dependent = yield* sql<{ type: string; name: string; sql: string }>`
    SELECT type,name,sql FROM main.sqlite_schema WHERE type IN ('trigger','view') AND sql IS NOT NULL`;
  const tables = yield* sql<{
    sql: string;
  }>`SELECT sql FROM main.sqlite_schema WHERE name='agent_control_run_once_repairs' AND type='table'`;
  const quote = (name: string) => '"' + name.replaceAll('"', '""') + '"';
  for (const entry of dependent)
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  const table = tables[0]!.sql.replace("REFERENCES agent_control_run_once_activations(run_id)", "");
  yield* sql.unsafe(
    table.replace("agent_control_run_once_repairs", "agent_control_run_once_repairs_089"),
  ).unprepared;
  yield* sql`INSERT INTO agent_control_run_once_repairs_089 SELECT * FROM agent_control_run_once_repairs`;
  yield* sql`DROP TABLE agent_control_run_once_repairs`;
  yield* sql`ALTER TABLE agent_control_run_once_repairs_089 RENAME TO agent_control_run_once_repairs`;
  for (const entry of dependent) {
    const source =
      entry.name === "agent_control_run_once_repairs_validate"
        ? entry.sql
            .replace(
              "FROM agent_control_run_once_states run",
              "FROM agent_control_task_execution_authority run",
            )
            .replace("project.mode = 'run-once'", "project.mode = run.active_mode")
        : entry.sql;
    yield* sql.unsafe(source).unprepared;
  }
});
