import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlOrchestrationCommand,
  AgentControlThreadMaterializeCommand,
  ClientOrchestrationCommand,
  OrchestrationCommand,
} from "./orchestration.ts";

const decodeMaterializationCommand = Schema.decodeUnknownEffect(
  AgentControlThreadMaterializeCommand,
);
const decodeAgentControlCommand = Schema.decodeUnknownEffect(AgentControlOrchestrationCommand);
const decodeClientCommand = Schema.decodeUnknownEffect(ClientOrchestrationCommand);
const decodeOrchestrationCommand = Schema.decodeUnknownEffect(OrchestrationCommand);

const command = {
  type: "thread.agent-control.materialize",
  commandId: "materialize-command",
  controlledThreadReservationId: "controlled-thread-reservation-id",
  threadId: "t3-auto-reserved-thread-id",
  projectId: "project-id",
  taskId: "task-id",
  taskRevision: 1,
  githubIntakeSequence: 2,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: "stage-run-id",
  attemptId: "attempt-id",
  roleId: "planning",
  stageKind: "planning",
  stageOrdinal: 1,
  attemptOrdinal: 1,
  leaseId: "lease-id",
  fenceToken: 3,
  worktreeReservationId: "worktree-reservation-id",
  title: "Planning thread",
  modelSelection: { instanceId: "codex", model: "gpt-5.6" },
  runtimeMode: "approval-required",
  interactionMode: "plan",
  branch: "t3-auto/task",
  worktreePath: "/tmp/t3-auto/task",
  binding: {
    taskId: "task-id",
    stageRunId: "stage-run-id",
    attemptId: "attempt-id",
    roleId: "planning",
    controlState: "controlled",
  },
  createdAt: "2026-07-27T10:00:00.000Z",
} as const;

it.effect("decodes the server-internal controlled thread materialization command", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeMaterializationCommand(command);
    assert.strictEqual(decoded.type, "thread.agent-control.materialize");
    assert.strictEqual(decoded.binding.controlState, "controlled");
    assert.strictEqual("authority" in decoded, false);
    yield* decodeAgentControlCommand(command);
    yield* decodeOrchestrationCommand(command);
  }),
);

it.effect("keeps materialization out of the client orchestration contract", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(decodeClientCommand(command));
    assert.strictEqual(result._tag, "Failure");
  }),
);

it.effect("rejects structurally invalid materialization coordinates", () =>
  Effect.gen(function* () {
    for (const invalid of [
      { ...command, taskRevision: 0 },
      { ...command, githubIntakeSequence: 0 },
      { ...command, stageOrdinal: 0 },
      { ...command, attemptOrdinal: 0 },
      { ...command, fenceToken: 0 },
      { ...command, branch: " " },
      { ...command, worktreePath: " " },
    ]) {
      const result = yield* Effect.exit(decodeMaterializationCommand(invalid));
      assert.strictEqual(result._tag, "Failure");
    }
  }),
);
