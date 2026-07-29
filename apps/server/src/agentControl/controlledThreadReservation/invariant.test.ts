import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "./identity.ts";
import { validateAgentControlControlledThreadReservationState } from "./invariant.ts";

const makeState = Effect.fn("makeControlledThreadReservationState")(function* () {
  const stable = {
    projectId: ProjectId.make("project-invariant"),
    taskId: AgentControlTaskId.make("task-invariant"),
    taskRevision: 1,
    githubIntakeSequence: 2,
    sourceIdentityFingerprint: "a".repeat(64),
    stageRunId: AgentControlStageRunId.make("stage-run-invariant"),
    attemptId: AgentControlAttemptId.make("attempt-invariant"),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1 as const,
    attemptOrdinal: 1 as const,
  };
  return {
    schemaVersion: 1 as const,
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: AgentControlStageRunLeaseId.make("lease-invariant"),
    fenceToken: 1,
    worktreeReservationId: AgentControlWorktreeReservationId.make("worktree-invariant"),
    status: "prepared" as const,
    revision: 1 as const,
    sequence: 9,
    preparedAt: "2026-07-26T10:00:00.000Z",
  };
});

it.effect("accepts only canonical prepared revision-one state", () =>
  Effect.gen(function* () {
    const state = yield* makeState();
    assert.deepStrictEqual(
      yield* validateAgentControlControlledThreadReservationState(state),
      state,
    );
    for (const corrupt of [
      { ...state, controlledThreadReservationId: `${state.controlledThreadReservationId}x` },
      { ...state, threadId: `${state.threadId}x` },
      { ...state, sourceIdentityFingerprint: "A".repeat(64) },
      { ...state, sequence: 0 },
      { ...state, fenceToken: 0 },
      { ...state, preparedAt: "2026-07-26T10:00:00Z" },
    ]) {
      assert.equal(
        (yield* Effect.result(
          validateAgentControlControlledThreadReservationState(corrupt as never),
        ))._tag,
        "Failure",
      );
    }
  }),
);

it.effect("validates complete materializing and bound coordinates all-or-none", () =>
  Effect.gen(function* () {
    const prepared = yield* makeState();
    const materializing = {
      ...prepared,
      status: "materializing" as const,
      revision: 2 as const,
      coordinatorCommandId: CommandId.make("coordinator-invariant"),
      coordinatorCommandFingerprint: "b".repeat(64),
      materializingTransitionCommandId: CommandId.make("materializing-invariant"),
      materializationCommandId: CommandId.make("orchestration-invariant"),
      materializationCommandFingerprint: "c".repeat(64),
      leaseHolderId: AgentControlStageRunLeaseHolderId.make("holder-invariant"),
      materializingAt: "2026-07-26T10:00:01.000Z",
    };
    assert.deepStrictEqual(
      yield* validateAgentControlControlledThreadReservationState(materializing),
      materializing,
    );
    const bound = {
      ...materializing,
      status: "bound" as const,
      revision: 3 as const,
      boundTransitionCommandId: CommandId.make("bound-invariant"),
      orchestrationResultSequence: 10,
      materializedAt: "2026-07-26T10:00:02.000Z",
      boundAt: "2026-07-26T10:00:03.000Z",
    };
    assert.deepStrictEqual(
      yield* validateAgentControlControlledThreadReservationState(bound),
      bound,
    );
    for (const corrupt of [
      { ...materializing, revision: 3 },
      { ...materializing, coordinatorCommandFingerprint: "B".repeat(64) },
      {
        ...materializing,
        materializationCommandId: materializing.materializingTransitionCommandId,
      },
      { ...bound, revision: 2 },
      { ...bound, orchestrationResultSequence: 0 },
      { ...bound, materializedAt: "2026-07-26T09:59:59.000Z" },
      { ...bound, boundAt: "2026-07-26T10:00:01.000Z" },
    ]) {
      assert.equal(
        (yield* Effect.result(
          validateAgentControlControlledThreadReservationState(corrupt as never),
        ))._tag,
        "Failure",
      );
    }
  }),
);
