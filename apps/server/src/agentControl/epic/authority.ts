import {
  AgentControlEpicRpcError,
  AgentControlEpicRuntimeView,
  type AgentControlRunOnceId,
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

/** Historical migration tests predate Epic execution; absent tables mean no selection. */
export const loadSelectedEpic = Effect.fn("loadSelectedEpic")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const installed =
    yield* sql`SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='agent_control_epic_targets'`;
  if (!installed.length) return null;
  const rows = yield* sql<{
    epicRunId: string;
  }>`SELECT epic_run_id AS "epicRunId" FROM main.agent_control_epic_targets WHERE project_id=${projectId}`;
  if (!rows[0]) return null;
  const state = yield* loadEpicRun(sql, rows[0].epicRunId);
  if (!state || state.projectId !== projectId)
    return yield* epicError("authority-conflict", "Epic selection is incomplete.");
  return state;
});

/** Call inside a transaction. The revision CAS and append-only history commit together. */
export const saveEpicRun = Effect.fn("saveEpicRun")(function* (
  sql: SqlClient.SqlClient,
  previous: AgentControlEpicRuntimeView,
  changes: Partial<AgentControlEpicRuntimeView>,
) {
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
  const epic = yield* loadSelectedEpic(sql, projectId);
  if (!epic) return;
  const member = epic.members.find((item) => item.taskId === taskId);
  if (
    epic.status !== "running" ||
    epic.activeTaskId !== taskId ||
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
) {
  const epic = yield* loadSelectedEpic(sql, projectId);
  if (!epic) return null;
  const member = epic.members.find((item) => item.taskId === taskId);
  if (
    childRunId === null ||
    epic.status !== "running" ||
    epic.activeTaskId !== taskId ||
    !member ||
    member.childRunId !== childRunId ||
    member.status !== "running"
  )
    return yield* epicError(
      "authority-conflict",
      "The worktree is not bound to the active Epic child run.",
    );
  if (member.baseCommitSha !== epic.acceptedCommitSha)
    return yield* epicError(
      "authority-conflict",
      "The Epic child base does not match the accepted result.",
    );
  return member.baseCommitSha;
});
