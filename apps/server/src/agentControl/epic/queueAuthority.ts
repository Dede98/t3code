import {
  AgentControlEpicQueue,
  isAgentControlEpicQueueEnabled,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";
import { epicDigest, epicError, epicJson } from "./authority.ts";

const decodeView = Schema.decodeUnknownEffect(AgentControlEpicQueue);
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlEpicQueue));
export const loadEpicQueue = Effect.fn("loadEpicQueue")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const installed =
    yield* sql`SELECT 1 FROM main.sqlite_schema WHERE type='table' AND name='agent_control_epic_queues'`;
  if (!installed.length) return null;
  const rows = yield* sql<{
    revision: number;
    json: string;
    digest: string;
    nextCheckAt: string | null;
  }>`SELECT revision,state_json AS json,state_digest AS digest,next_check_at AS "nextCheckAt" FROM main.agent_control_epic_queues WHERE project_id=${projectId}`;
  if (!rows[0]) return null;
  const row = rows[0];
  const state = yield* decode(row.json);
  const history = yield* sql<{
    digest: string;
  }>`SELECT state_digest AS digest FROM main.agent_control_epic_queue_history WHERE project_id=${projectId} AND revision=${row.revision}`;
  if (
    state.projectId !== projectId ||
    state.revision !== row.revision ||
    epicDigest(state) !== row.digest ||
    history.length !== 1 ||
    history[0]!.digest !== row.digest ||
    new Set(state.entries.map((entry) => entry.entryId)).size !== state.entries.length ||
    new Set(state.entries.map((entry) => entry.source.epic.issueNodeId)).size !==
      state.entries.length ||
    state.entries.some((entry) => (entry.status === "pending") !== (entry.epicRunId === null))
  )
    return yield* epicError("authority-conflict", "Epic queue state or history is inconsistent.");
  return yield* decodeView({
    ...state,
    nextCheckAt: row.nextCheckAt,
  });
});

/** The caller transaction also commits run selection or command evidence. */
export const saveEpicQueue = Effect.fn("saveEpicQueue")(function* (
  sql: SqlClient.SqlClient,
  previous: AgentControlEpicQueue | null,
  changes: AgentControlEpicQueue,
) {
  // Scheduling heartbeats do not change queue authority or append full history.
  if (
    previous &&
    epicDigest({ ...previous, nextCheckAt: null }) === epicDigest({ ...changes, nextCheckAt: null })
  ) {
    const updated =
      yield* sql`UPDATE main.agent_control_epic_queues SET next_check_at=${changes.nextCheckAt} WHERE project_id=${previous.projectId} AND revision=${previous.revision} RETURNING project_id`;
    if (updated.length !== 1)
      return yield* epicError("revision-conflict", "The Epic queue changed before rescheduling.");
    return { ...previous, nextCheckAt: changes.nextCheckAt };
  }
  const state = { ...changes, revision: (previous?.revision ?? 0) + 1 };
  const json = epicJson(state);
  const digest = epicDigest(state);
  if (previous) {
    const updated =
      yield* sql`UPDATE main.agent_control_epic_queues SET revision=${state.revision},state_json=${json},state_digest=${digest},next_check_at=${state.nextCheckAt} WHERE project_id=${state.projectId} AND revision=${previous.revision} RETURNING project_id`;
    if (updated.length !== 1)
      return yield* epicError(
        "revision-conflict",
        "The Epic queue changed. Reload before editing it.",
      );
  } else {
    yield* sql`INSERT INTO main.agent_control_epic_queues(project_id,revision,state_json,state_digest,next_check_at) VALUES (${state.projectId},${state.revision},${json},${digest},${state.nextCheckAt})`;
  }
  yield* sql`INSERT INTO main.agent_control_epic_queue_history(project_id,revision,state_json,state_digest) VALUES (${state.projectId},${state.revision},${json},${digest})`;
  return state;
});

export const loadEnabledEpicQueue = Effect.fn("loadEnabledEpicQueue")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const queue = yield* loadEpicQueue(sql, projectId);
  return isAgentControlEpicQueueEnabled(queue) ? queue : null;
});
