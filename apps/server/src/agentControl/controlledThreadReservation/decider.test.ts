import {
  AgentControlAttemptId,
  type AgentControlControlledThreadReservationEvent,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
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
  deriveAgentControlBoundTransitionCommandId,
  deriveAgentControlMaterializingTransitionCommandId,
  deriveAgentControlReservedThreadId,
  deriveAgentControlThreadMaterializationCommandId,
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
    } as AgentControlControlledThreadReservationEvent);
    const duplicate = yield* Effect.result(
      decideAgentControlControlledThreadReservationCommand({
        state,
        command: { ...prepared, commandId: CommandId.make("command-duplicate") },
        eventId: EventId.make("event-noop"),
        occurredAt: "2026-07-26T10:00:01.000Z",
      }),
    );
    assert.equal(duplicate._tag, "Failure");
    if (duplicate._tag === "Failure") {
      assert.equal(duplicate.failure.code, "controlled-thread-reservation-identity-conflict");
    }
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

it.effect("projects the closed prepared to materializing to bound transition", () =>
  Effect.gen(function* () {
    const prepared = yield* command();
    const preparedDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state: null,
      command: prepared,
      eventId: EventId.make("event-transition-prepared"),
      occurredAt: "2026-07-26T10:00:00.000Z",
    }))[0]!;
    const preparedState = yield* projectAgentControlControlledThreadReservationEvent(null, {
      ...preparedDraft,
      streamVersion: 1,
      sequence: 10,
    } as AgentControlControlledThreadReservationEvent);
    const coordinatorCommandId = CommandId.make("coordinator-transition");
    const materializingTransitionCommandId =
      yield* deriveAgentControlMaterializingTransitionCommandId(
        coordinatorCommandId,
        prepared.controlledThreadReservationId,
      );
    const materializationCommandId = yield* deriveAgentControlThreadMaterializationCommandId(
      coordinatorCommandId,
      prepared.controlledThreadReservationId,
    );
    const boundTransitionCommandId = yield* deriveAgentControlBoundTransitionCommandId(
      coordinatorCommandId,
      prepared.controlledThreadReservationId,
    );
    const materializingAt = "2026-07-26T10:00:01.000Z";
    const begin = {
      ...prepared,
      type: "agentControl.controlledThreadReservation.beginMaterialization" as const,
      commandId: materializingTransitionCommandId,
      expectedRevision: 1 as const,
      coordinatorCommandId,
      coordinatorCommandFingerprint: "c".repeat(64),
      materializingTransitionCommandId,
      materializationCommandId,
      materializationCommandFingerprint: "d".repeat(64),
      leaseHolderId: AgentControlStageRunLeaseHolderId.make("runtime-holder"),
      materializingAt,
    };
    const materializingDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state: preparedState,
      command: begin,
      eventId: EventId.make("event-transition-materializing"),
      occurredAt: materializingAt,
    }))[0]!;
    const materializingState = yield* projectAgentControlControlledThreadReservationEvent(
      preparedState,
      {
        ...materializingDraft,
        streamVersion: 2,
        sequence: 11,
      } as AgentControlControlledThreadReservationEvent,
    );
    assert.equal(materializingState.status, "materializing");
    assert.equal(materializingState.revision, 2);

    const boundAt = "2026-07-26T10:00:02.000Z";
    const bind = {
      ...begin,
      type: "agentControl.controlledThreadReservation.bindMaterialization" as const,
      commandId: boundTransitionCommandId,
      expectedRevision: 2 as const,
      boundTransitionCommandId,
      orchestrationResultSequence: 42,
      materializedAt: boundAt,
      boundAt,
    };
    const boundDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state: materializingState,
      command: bind,
      eventId: EventId.make("event-transition-bound"),
      occurredAt: boundAt,
    }))[0]!;
    const boundState = yield* projectAgentControlControlledThreadReservationEvent(
      materializingState,
      {
        ...boundDraft,
        streamVersion: 3,
        sequence: 12,
      } as AgentControlControlledThreadReservationEvent,
    );
    assert.equal(boundState.status, "bound");
    if (boundState.status !== "bound") {
      return yield* Effect.die(new Error("expected a bound reservation"));
    }
    assert.equal(boundState.revision, 3);
    assert.equal(boundState.orchestrationResultSequence, 42);
    assert.equal(boundState.coordinatorCommandId, coordinatorCommandId);

    const changedBinding = yield* Effect.result(
      decideAgentControlControlledThreadReservationCommand({
        state: materializingState,
        command: { ...bind, fenceToken: 2 },
        eventId: EventId.make("event-transition-corrupt"),
        occurredAt: boundAt,
      }),
    );
    assert.equal(changedBinding._tag, "Failure");
  }),
);

it.effect("prepares verification and keeps materializing and bound transitions closed", () =>
  Effect.gen(function* () {
    const planning = yield* command();
    const stable = {
      projectId: planning.projectId,
      taskId: planning.taskId,
      taskRevision: planning.taskRevision,
      githubIntakeSequence: planning.githubIntakeSequence,
      sourceIdentityFingerprint: planning.sourceIdentityFingerprint,
      stageRunId: AgentControlStageRunId.make("verification-stage-decider"),
      attemptId: AgentControlAttemptId.make("verification-attempt-decider"),
      roleId: AgentControlRoleId.make("verifier"),
      stageKind: "verification" as const,
      stageOrdinal: 3 as const,
      attemptOrdinal: 1 as const,
    };
    const prepare = {
      ...planning,
      ...stable,
      controlledThreadReservationId: yield* deriveAgentControlControlledThreadReservationId(stable),
      threadId: yield* deriveAgentControlReservedThreadId(stable),
      fenceToken: 3,
    };
    const preparedDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state: null,
      command: prepare,
      eventId: EventId.make("verification-event-prepared"),
      occurredAt: "2026-07-26T10:00:00.000Z",
    }))[0]!;
    const prepared = yield* projectAgentControlControlledThreadReservationEvent(null, {
      ...preparedDraft,
      streamVersion: 1,
      sequence: 20,
    } as AgentControlControlledThreadReservationEvent);
    const materializing = yield* Effect.result(
      decideAgentControlControlledThreadReservationCommand({
        state: prepared,
        command: {
          ...prepare,
          type: "agentControl.controlledThreadReservation.beginMaterialization",
          expectedRevision: 1,
          coordinatorCommandId: CommandId.make("verification-coordinator"),
          coordinatorCommandFingerprint: "c".repeat(64),
          materializingTransitionCommandId: CommandId.make("verification-materializing"),
          materializationCommandId: CommandId.make("verification-materialization"),
          materializationCommandFingerprint: "d".repeat(64),
          leaseHolderId: AgentControlStageRunLeaseHolderId.make("verification-holder"),
          materializingAt: "2026-07-26T10:00:01.000Z",
        },
        eventId: EventId.make("verification-event-materializing"),
        occurredAt: "2026-07-26T10:00:01.000Z",
      }),
    );
    assert.equal(materializing._tag, "Failure");
    if (materializing._tag === "Failure") {
      assert.equal(materializing.failure.code, "state-not-available");
    }
  }),
);
