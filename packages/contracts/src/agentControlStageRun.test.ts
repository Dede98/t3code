import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlStageRunPrepareInitialInput,
  AgentControlStageRunRpcError,
  AgentControlStageRunState,
} from "./agentControlStageRun.ts";

const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunState);
const decodePrepare = Schema.decodeUnknownEffect(AgentControlStageRunPrepareInitialInput);
const decodeRpcError = Schema.decodeUnknownEffect(AgentControlStageRunRpcError);

it.effect("decodes list-safe prepared stage-run state", () =>
  Effect.gen(function* () {
    const state = yield* decodeState({
      schemaVersion: 1,
      projectId: "project-1",
      taskId: "task-1",
      stageRunId: "stage-run-1",
      attemptId: "attempt-1",
      roleId: "planning",
      stageKind: "planning",
      stageOrdinal: 1,
      attemptOrdinal: 1,
      status: "prepared",
      taskRevision: 3,
      githubIntakeSequence: 7,
      sourceIdentityFingerprint: "fingerprint",
      createdAt: "2026-07-24T10:00:00.000Z",
      updatedAt: "2026-07-24T10:00:00.000Z",
      revision: 1,
      sequence: 9,
    });
    assert.equal(state.status, "prepared");
    assert.equal(state.roleId, "planning");
    assert.notProperty(state, "body");
    assert.notProperty(state, "provider");
    assert.notProperty(state, "workspaceRoot");
  }),
);

it.effect("prepare input exposes only idempotency, project, and task identity", () =>
  Effect.gen(function* () {
    const input = yield* decodePrepare({
      commandId: "command-1",
      projectId: "project-1",
      taskId: "task-1",
      stageRunId: "client-stage-run",
      attemptId: "client-attempt",
      roleId: "client-role",
      stageKind: "merge",
      authority: "human",
      provider: "client-provider",
    });
    assert.deepStrictEqual(Object.keys(input).toSorted(), ["commandId", "projectId", "taskId"]);
  }),
);

it.effect("wire errors remain closed and transport-safe", () =>
  Effect.gen(function* () {
    const error = yield* decodeRpcError({
      _tag: "AgentControlStageRunRpcError",
      code: "source-snapshot-stale",
      operation: "prepare-initial",
      projectId: "project-1",
      taskId: "task-1",
      cause: new Error("secret"),
      issueBody: "untrusted",
      path: "/secret",
    });
    assert.deepStrictEqual(Object.keys(error).toSorted(), [
      "_tag",
      "code",
      "operation",
      "projectId",
      "taskId",
    ]);
  }),
);
