import {
  AgentControlEpicRpcError,
  AgentControlEpicRuntimeView,
  AgentControlRunOnceId,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { canonicalJson, sha256Utf8, type JsonValue } from "../initialPlanning/eventEvidence.ts";

const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlEpicRuntimeView));
export const epicError = (code: string, message: string) =>
  new AgentControlEpicRpcError({ code, message });
export const epicJson = (value: unknown) => canonicalJson(value as JsonValue);
export const epicDigest = (value: unknown) => sha256Utf8(epicJson(value));

export const loadEpicRun = Effect.fn("loadEpicRun")(function* (
  sql: SqlClient.SqlClient,
  epicRunId: string,
) {
  const rows = yield* sql<{ stateJson: string; stateDigest: string; revision: number }>`
    SELECT state_json AS "stateJson", state_digest AS "stateDigest", revision
    FROM main.agent_control_epic_runs WHERE epic_run_id=${epicRunId}`;
  if (!rows[0]) return null;
  const row = rows[0];
  const state = yield* decode(row.stateJson);
  if (
    sha256Utf8(row.stateJson) !== row.stateDigest ||
    state.epicRunId !== epicRunId ||
    state.revision !== row.revision
  )
    return yield* epicError(
      "authority-conflict",
      "Epic state does not match its persisted authority.",
    );
  const history = yield* sql<{ stateDigest: string }>`SELECT state_digest AS "stateDigest"
    FROM main.agent_control_epic_history WHERE epic_run_id=${epicRunId} AND revision=${state.revision}`;
  if (history.length !== 1 || history[0]!.stateDigest !== row.stateDigest)
    return yield* epicError("authority-conflict", "Epic history is incomplete.");
  return state;
});

/** A competing tick may advance the same run. Its stale proof is retryable, never adoptable. */
export const requireEpicIntegrationAuthority = Effect.fn("requireEpicIntegrationAuthority")(
  function* (expected: AgentControlEpicRuntimeView, selected: AgentControlEpicRuntimeView | null) {
    if (
      !selected ||
      selected.epicRunId !== expected.epicRunId ||
      selected.dependencyPlanDigest !== expected.dependencyPlanDigest ||
      selected.projectDependencyPlanDigest !== expected.projectDependencyPlanDigest
    )
      return yield* epicError("authority-conflict", "Epic integration identity or plan changed.");
    if (selected.revision !== expected.revision)
      return yield* epicError(
        "revision-conflict",
        "Epic integration progress changed; reload before retrying.",
      );
    if (selected.status !== "running")
      return yield* epicError("authority-conflict", "Epic integration authority is inactive.");
  },
);

/** The target set is execution authority, independent of the client's displayed Epic. */
export const loadProjectEpics = Effect.fn("loadProjectEpics")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const installed =
    yield* sql`SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='agent_control_epic_targets'`;
  if (!installed.length) return [];
  const rows = yield* sql<{ epicRunId: string }>`
    SELECT target.epic_run_id AS "epicRunId" FROM main.agent_control_epic_targets target
    LEFT JOIN main.agent_control_epic_runs run ON run.epic_run_id=target.epic_run_id
    WHERE target.project_id=${projectId}
    ORDER BY json_extract(run.state_json,'$.createdAt'), target.epic_run_id`;
  return yield* Effect.forEach(rows, (row) =>
    Effect.gen(function* () {
      const state = yield* loadEpicRun(sql, row.epicRunId);
      if (!state || state.projectId !== projectId)
        return yield* epicError("authority-conflict", "Epic target authority is incomplete.");
      return state;
    }),
  );
});

export const loadProjectEpic = Effect.fn("loadProjectEpic")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  epicRunId: string,
) {
  const targets = yield* sql`SELECT 1 FROM main.agent_control_epic_targets
    WHERE project_id=${projectId} AND epic_run_id=${epicRunId}`;
  if (targets.length !== 1) return null;
  const state = yield* loadEpicRun(sql, epicRunId);
  if (!state || state.projectId !== projectId)
    return yield* epicError("authority-conflict", "Epic target belongs to another project.");
  return state;
});

/** Compatibility for the serial read model. Execution must use its run or task identity. */
export const loadSelectedEpic = Effect.fn("loadSelectedEpic")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  return (yield* loadProjectEpics(sql, projectId))[0] ?? null;
});

export const loadTaskEpic = Effect.fn("loadTaskEpic")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
) {
  const epics = yield* loadProjectEpics(sql, projectId);
  const owners = epics.filter((epic) => epic.members.some((member) => member.taskId === taskId));
  if (owners.length > 1)
    return yield* epicError("authority-conflict", "The task has more than one Epic owner.");
  return owners[0] ?? null;
});

/** Call inside a transaction. The revision CAS and append-only history commit together. */
export const saveEpicRun = Effect.fn("saveEpicRun")(function* (
  sql: SqlClient.SqlClient,
  previous: AgentControlEpicRuntimeView,
  changes: Partial<AgentControlEpicRuntimeView>,
) {
  if (
    (changes.projectDependencyPlan !== undefined &&
      epicDigest(changes.projectDependencyPlan) !==
        epicDigest(previous.projectDependencyPlan ?? null)) ||
    (changes.projectDependencyPlanDigest !== undefined &&
      changes.projectDependencyPlanDigest !== previous.projectDependencyPlanDigest) ||
    (changes.dependencyPlan !== undefined &&
      epicDigest(changes.dependencyPlan) !== epicDigest(previous.dependencyPlan ?? null)) ||
    (changes.dependencyPlanDigest !== undefined &&
      changes.dependencyPlanDigest !== previous.dependencyPlanDigest) ||
    (changes.parallelism !== undefined && changes.parallelism !== previous.parallelism)
  )
    return yield* epicError(
      "authority-conflict",
      "An approved dependency plan and its parallelism are immutable. Start a new run to change them.",
    );
  const state = {
    ...previous,
    ...changes,
    revision: previous.revision + 1,
    updatedAt: DateTime.formatIso(yield* DateTime.now),
  };
  const json = epicJson(state);
  const digest = sha256Utf8(json);
  const updated =
    yield* sql`UPDATE main.agent_control_epic_runs SET revision=${state.revision},state_json=${json},state_digest=${digest}
    WHERE epic_run_id=${state.epicRunId} AND revision=${previous.revision} RETURNING epic_run_id`;
  if (updated.length !== 1)
    return yield* epicError(
      "revision-conflict",
      "Epic progress changed; reload its current state.",
    );
  yield* sql`INSERT INTO main.agent_control_epic_history(epic_run_id,revision,state_json,state_digest)
    VALUES (${state.epicRunId},${state.revision},${json},${digest})`;
  return state;
});

/** Bind the existing Run Once transaction to the one authorized Epic member. */
export const bindEpicChildRun = Effect.fn("bindEpicChildRun")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  childRunId: AgentControlRunOnceId,
) {
  const epic = yield* loadTaskEpic(sql, projectId, taskId);
  if (!epic) {
    if ((yield* loadProjectEpics(sql, projectId)).length)
      return yield* epicError("authority-conflict", "The task has no authorized Epic owner.");
    return;
  }
  const member = epic.members.find((item) => item.taskId === taskId);
  if (
    epic.status !== "running" ||
    (!epic.dependencyPlan && epic.activeTaskId !== taskId) ||
    !member ||
    member.status !== "running" ||
    (member.childRunId !== null && member.childRunId !== childRunId)
  )
    return yield* epicError(
      "authority-conflict",
      "The selected task is outside the active Epic authority.",
    );
  if (member.childRunId === childRunId) return;
  yield* saveEpicRun(sql, epic, {
    members: epic.members.map((item) => (item === member ? { ...item, childRunId } : item)),
  });
});

export const loadEpicRunBase = Effect.fn("loadEpicRunBase")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  childRunId: AgentControlRunOnceId | null,
  targetBranch?: string,
) {
  const epic = yield* loadTaskEpic(sql, projectId, taskId);
  if (!epic) {
    if ((yield* loadProjectEpics(sql, projectId)).length)
      return yield* epicError("authority-conflict", "The task has no authorized Epic owner.");
    return null;
  }
  const member = epic.members.find((item) => item.taskId === taskId);
  if (epic.dependencyPlan && childRunId === null && member?.childRunId) {
    const executions = yield* sql<{ baseCommitSha: string; planDigest: string }>`
      SELECT base_commit_sha AS "baseCommitSha",plan_digest AS "planDigest"
      FROM main.agent_control_epic_task_executions
      WHERE execution_id=${member.childRunId} AND epic_run_id=${epic.epicRunId}
        AND project_id=${projectId} AND task_id=${taskId}`;
    if (
      executions.length !== 1 ||
      executions[0]!.baseCommitSha !== member.baseCommitSha ||
      executions[0]!.planDigest !== epic.dependencyPlanDigest
    )
      return yield* epicError(
        "authority-conflict",
        "The task execution does not match its approved plan and base.",
      );
    childRunId = AgentControlRunOnceId.make(member.childRunId);
  }
  if (
    childRunId === null ||
    epic.status !== "running" ||
    (!epic.dependencyPlan && epic.activeTaskId !== taskId) ||
    !member ||
    member.childRunId !== childRunId ||
    member.status !== "running"
  )
    return yield* epicError(
      "authority-conflict",
      "The worktree is not bound to the active Epic child run.",
    );
  if (
    !epic.dependencyPlan &&
    member.baseCommitSha !== (epic.acceptedCommitSha ?? epic.initialBase?.commitSha ?? null)
  )
    return yield* epicError(
      "authority-conflict",
      "The Epic child base does not match the accepted result.",
    );
  if (
    epic.initialBase &&
    targetBranch !== undefined &&
    epic.initialBase.targetBranch !== targetBranch
  )
    return yield* epicError(
      "authority-conflict",
      "The Epic target branch changed after its base was refreshed.",
    );
  return member.baseCommitSha;
});
