import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
  AgentControlControlledThreadReservationView,
} from "./agentControlControlledThreadReservation.ts";

const decodeView = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationView);
const decodeStrictPrepare = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationPrepareInitialInput,
  { onExcessProperty: "error" },
);
const decodeRpcError = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationRpcError);

it.effect("keeps the Controlled Thread reservation wire view transport-safe", () =>
  Effect.gen(function* () {
    const view = yield* decodeView({
      controlledThreadReservationId: "controlled-thread-reservation-1",
      threadId: "t3-auto-reserved-thread-1",
      projectId: "project-1",
      taskId: "task-1",
      stageRunId: "stage-run-1",
      attemptId: "attempt-1",
      roleId: "planning",
      status: "prepared",
      revision: 1,
      preparedAt: "2026-07-26T10:00:00.000Z",
      holderId: "hidden",
      fenceToken: 9,
      sourceIdentityFingerprint: "hidden",
      worktreePath: "/hidden",
      issueBody: "hidden",
    });
    assert.deepStrictEqual(Object.keys(view).toSorted(), [
      "attemptId",
      "controlledThreadReservationId",
      "preparedAt",
      "projectId",
      "revision",
      "roleId",
      "stageRunId",
      "status",
      "taskId",
      "threadId",
    ]);
  }),
);

it.effect("strict prepare decoding rejects client-supplied authority and internal identities", () =>
  Effect.gen(function* () {
    const decoded = yield* Effect.result(
      decodeStrictPrepare({
        commandId: "command-1",
        projectId: "project-1",
        taskId: "task-1",
        threadId: "client-thread",
        authority: "human",
      }),
    );
    assert.equal(decoded._tag, "Failure");
  }),
);

it.effect("keeps reservation errors closed", () =>
  Effect.gen(function* () {
    const error = yield* decodeRpcError({
      _tag: "AgentControlControlledThreadReservationRpcError",
      code: "worktree-not-ready",
      operation: "prepare-initial",
      projectId: "project-1",
      taskId: "task-1",
      controlledThreadReservationId: null,
      cause: new Error("hidden"),
      path: "/hidden",
      fenceToken: 1,
    });
    assert.deepStrictEqual(Object.keys(error).toSorted(), [
      "_tag",
      "code",
      "controlledThreadReservationId",
      "operation",
      "projectId",
      "taskId",
    ]);
  }),
);
