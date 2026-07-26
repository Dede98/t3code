import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideAgentControlControlledThreadReservationCommand } from "./decider.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "./identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "./projector.ts";

const command = Effect.fn("makeControlledThreadReservationCommand")(function* () {
  const stable = {
    projectId: ProjectId.make("project-decider"),
    taskId: AgentControlTaskId.make("task-decider"),
    taskRevision: 1,
    githubIntakeSequence: 2,
    sourceIdentityFingerprint: "b".repeat(64),
    stageRunId: AgentControlStageRunId.make("stage-run-decider"),
    attemptId: AgentControlAttemptId.make("attempt-decider"),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1 as const,
    attemptOrdinal: 1 as const,
  };
  return {
    type: "agentControl.controlledThreadReservation.prepare" as const,
    commandId: CommandId.make("command-decider"),
    authority: "controller" as const,
    controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
    threadId: yield* deriveAgentControlReservedThreadId(stable),
    ...stable,
    leaseId: AgentControlStageRunLeaseId.make("lease-decider"),
    fenceToken: 1,
    worktreeReservationId: AgentControlWorktreeReservationId.make("worktree-decider"),
    expectedRevision: 0 as const,
  };
});

it.effect("prepares once and rejects a second binding for the same stage attempt", () =>
  Effect.gen(function* () {
    const prepared = yield* command();
    const drafts = yield* decideAgentControlControlledThreadReservationCommand({
      state: null,
      command: prepared,
      eventId: EventId.make("event-decider"),
      occurredAt: "2026-07-26T10:00:00.000Z",
    });
    assert.lengthOf(drafts, 1);
    const state = yield* projectAgentControlControlledThreadReservationEvent(null, {
      ...drafts[0]!,
      streamVersion: 1,
      sequence: 10,
    });
    assert.lengthOf(
      yield* decideAgentControlControlledThreadReservationCommand({
        state,
        command: prepared,
        eventId: EventId.make("event-noop"),
        occurredAt: "2026-07-26T10:00:01.000Z",
      }),
      0,
    );
    const conflict = yield* Effect.result(
      decideAgentControlControlledThreadReservationCommand({
        state,
        command: {
          ...prepared,
          commandId: CommandId.make("command-conflict"),
          leaseId: AgentControlStageRunLeaseId.make("lease-other"),
          fenceToken: 2,
          worktreeReservationId: AgentControlWorktreeReservationId.make("worktree-other"),
        },
        eventId: EventId.make("event-conflict"),
        occurredAt: "2026-07-26T10:00:01.000Z",
      }),
    );
    assert.equal(conflict._tag, "Failure");
    if (conflict._tag === "Failure") {
      assert.equal(conflict.failure.code, "controlled-thread-reservation-identity-conflict");
    }
  }),
);
