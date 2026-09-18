import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

// Targets are the durable execution set. Existing serial targets retain exactly
// the same run, history and child bindings when the project opts into parallelism.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const dependent = yield* sql<{ type: string; name: string; sql: string }>`
    SELECT type,name,sql FROM main.sqlite_schema
    WHERE type IN ('trigger','view') AND sql IS NOT NULL`;
  const quote = (name: string) => '"' + name.replaceAll('"', '""') + '"';
  for (const entry of dependent)
    yield* sql.unsafe(`DROP ${entry.type.toUpperCase()} main.${quote(entry.name)}`).unprepared;
  yield* sql`CREATE TABLE main.agent_control_epic_targets_093 (
    project_id TEXT NOT NULL,
    epic_run_id TEXT UNIQUE NOT NULL REFERENCES agent_control_epic_runs(epic_run_id),
    PRIMARY KEY(project_id,epic_run_id)
  )`;
  yield* sql`INSERT INTO main.agent_control_epic_targets_093 SELECT * FROM main.agent_control_epic_targets`;
  yield* sql`DROP TABLE main.agent_control_epic_targets`;
  yield* sql`ALTER TABLE main.agent_control_epic_targets_093 RENAME TO agent_control_epic_targets`;
  for (const entry of dependent) yield* sql.unsafe(entry.sql).unprepared;
  yield* sql`CREATE TRIGGER main.agent_control_epic_plans_immutable
    BEFORE UPDATE ON agent_control_epic_runs
    WHEN json_extract(NEW.state_json,'$.dependencyPlan') IS NOT json_extract(OLD.state_json,'$.dependencyPlan')
      OR json_extract(NEW.state_json,'$.dependencyPlanDigest') IS NOT json_extract(OLD.state_json,'$.dependencyPlanDigest')
      OR json_extract(NEW.state_json,'$.parallelism') IS NOT json_extract(OLD.state_json,'$.parallelism')
      OR json_extract(NEW.state_json,'$.projectDependencyPlan') IS NOT json_extract(OLD.state_json,'$.projectDependencyPlan')
      OR json_extract(NEW.state_json,'$.projectDependencyPlanDigest') IS NOT json_extract(OLD.state_json,'$.projectDependencyPlanDigest')
    BEGIN SELECT RAISE(ABORT,'Epic dependency approvals are immutable'); END`;
});
