import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlStageRunId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideAgentControlStageRunCommand } from "./decider.ts";
import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "./identity.ts";
import { projectAgentControlStageRunEvent } from "./projector.ts";

const at = "2026-07-24T10:00:00.000Z";
const makeCommand = Effect.fn("makeCommand")(function* () {
  const projectId = ProjectId.make("stage-run-project");
  const taskId = AgentControlTaskId.make("stage-run-task");
  const sourceIdentityFingerprint = "a".repeat(64);
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 2,
    githubIntakeSequence: 3,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  return {
    type: "agentControl.stageRun.prepare" as const,
    commandId: CommandId.make("stage-run-command"),
    projectId,
    taskId,
    stageRunId,
    attemptId: yield* deriveAgentControlAttemptId(stageRunId, 1),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1,
    attemptOrdinal: 1,
    taskRevision: 2,
    githubIntakeSequence: 3,
    sourceIdentityFingerprint,
    expectedRevision: 0,
  };
});

it.effect("decides and projects only the initial prepared planning stage", () =>
  Effect.gen(function* () {
    const command = yield* makeCommand();
    const drafts = yield* decideAgentControlStageRunCommand({
      state: null,
      command,
      eventId: EventId.make("stage-run-event"),
      occurredAt: at,
    });
    assert.equal(drafts.length, 1);
    const draft = drafts[0]!;
    const state = yield* projectAgentControlStageRunEvent(null, {
      ...draft,
      streamVersion: 1,
      sequence: 7,
    });
    assert.equal(state.status, "prepared");
    assert.equal(state.revision, 1);
    assert.equal(state.sequence, 7);
    const arbitraryStageRunId = AgentControlStageRunId.make("arbitrary-stage-run-id");
    const arbitraryStageRun = yield* Effect.result(
      projectAgentControlStageRunEvent(null, {
        ...draft,
        aggregateId: arbitraryStageRunId,
        payload: { ...draft.payload, stageRunId: arbitraryStageRunId },
        streamVersion: 1,
        sequence: 7,
      }),
    );
    assert.equal(arbitraryStageRun._tag, "Failure");
    const arbitraryAttempt = yield* Effect.result(
      projectAgentControlStageRunEvent(null, {
        ...draft,
        payload: {
          ...draft.payload,
          attemptId: AgentControlAttemptId.make("arbitrary-attempt-id"),
        },
        streamVersion: 1,
        sequence: 7,
      }),
    );
    assert.equal(arbitraryAttempt._tag, "Failure");
    assert.equal(
      (yield* decideAgentControlStageRunCommand({
        state,
        command,
        eventId: EventId.make("stage-run-noop"),
        occurredAt: at,
      })).length,
      0,
    );
  }),
);
it.effect("projects exactly one system-authorized Verification start with a bound causation", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("verification-start-project");
    const taskId = AgentControlTaskId.make("verification-start-task");
    const sourceIdentityFingerprint = "b".repeat(64);
    const stageRunId = yield* deriveAgentControlStageRunId({
      projectId,
      taskId,
      taskRevision: 4,
      githubIntakeSequence: 9,
      sourceIdentityFingerprint,
      stageKind: "verification",
      stageOrdinal: 3,
    });
    const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
    const prepared = yield* projectAgentControlStageRunEvent(null, {
      eventId: EventId.make("verification-stage-prepared-event"),
      type: "agentControl.stageRun.prepared",
      aggregateKind: "stage-run",
      aggregateId: stageRunId,
      occurredAt: at,
      commandId: CommandId.make("verification-stage-prepare"),
      causationEventId: null,
      correlationId: CommandId.make("verification-stage-prepare"),
      authority: "controller",
      metadata: { schemaVersion: 1 },
      payload: {
        projectId,
        taskId,
        stageRunId,
        attemptId,
        roleId: AgentControlRoleId.make("verifier"),
        stageKind: "verification",
        stageOrdinal: 3,
        attemptOrdinal: 1,
        taskRevision: 4,
        githubIntakeSequence: 9,
        sourceIdentityFingerprint,
        status: "prepared",
        preparedAt: at,
      },
      streamVersion: 1,
      sequence: 11,
    });
    const startEvent = {
      eventId: EventId.make("verification-stage-started-event"),
      type: "agentControl.stageRun.verificationStarted" as const,
      aggregateKind: "stage-run" as const,
      aggregateId: stageRunId,
      occurredAt: at,
      commandId: CommandId.make("verification-stage-start"),
      causationEventId: EventId.make("verification-turn-requested-event"),
      correlationId: CommandId.make("verification-stage-start"),
      authority: "system" as const,
      metadata: { schemaVersion: 1 as const },
      payload: {
        projectId,
        taskId,
        stageRunId,
        attemptId,
        roleId: "verifier" as const,
        stageKind: "verification" as const,
        stageOrdinal: 3 as const,
        attemptOrdinal: 1 as const,
        status: "running" as const,
        taskRevision: 4,
        githubIntakeSequence: 9,
        sourceIdentityFingerprint,
        admissionEvidenceId: "admission-evidence",
        admissionReceiptId: "admission-receipt",
        admissionMarkerId: "admission-marker",
        materializationEvidenceId: "materialization-evidence",
        materializationReceiptId: "materialization-receipt",
        materializationMarkerId: "materialization-marker",
        handoffId: "verification-handoff",
        handoffFingerprint: "c".repeat(64),
        providerDeliveryId: "verification-delivery",
        deliveryRevision: 4,
        claimGeneration: 1,
        attemptCount: 1,
        controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
          "verification-reservation",
        ),
        threadId: ThreadId.make("verification-thread"),
        planningThreadId: ThreadId.make("planning-thread"),
        planId: "plan-1",
        proposedPlanDigest: "d".repeat(64),
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerTurnId: "provider-turn-1",
        runtimeMode: "approval-required" as const,
        modelSelectionFingerprint: "e".repeat(64),
        leaseId: AgentControlStageRunLeaseId.make("verification-lease"),
        leaseHolderId: AgentControlStageRunLeaseHolderId.make("verification-holder"),
        fenceToken: 3,
        startedAt: at,
      },
      streamVersion: 2,
      sequence: 12,
    };
    const running = yield* projectAgentControlStageRunEvent(prepared, startEvent);
    assert.equal(running.status, "running");
    assert.equal(running.revision, 2);

    assert.equal(
      (yield* Effect.result(
        projectAgentControlStageRunEvent(prepared, {
          ...startEvent,
          causationEventId: null,
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(projectAgentControlStageRunEvent(running, startEvent)))._tag,
      "Failure",
    );
  }),
);

it.effect("keeps later status transitions reserved and fail-closed", () =>
  Effect.gen(function* () {
    const command = yield* makeCommand();
    const result = yield* Effect.result(
      decideAgentControlStageRunCommand({
        state: null,
        command: {
          ...command,
          type: "agentControl.stageRun.status.set",
          status: "queued",
        },
        eventId: EventId.make("stage-run-reserved"),
        occurredAt: at,
      }),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") {
      assert.equal(result.failure.code, "state-not-available");
    }
  }),
);

it.effect(
  "projects exactly one system-authorized Implementation start with a bound causation",
  () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("implementation-start-project");
      const taskId = AgentControlTaskId.make("implementation-start-task");
      const sourceIdentityFingerprint = "b".repeat(64);
      const stageRunId = yield* deriveAgentControlStageRunId({
        projectId,
        taskId,
        taskRevision: 4,
        githubIntakeSequence: 9,
        sourceIdentityFingerprint,
        stageKind: "implementation",
        stageOrdinal: 2,
      });
      const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
      const prepared = yield* projectAgentControlStageRunEvent(null, {
        eventId: EventId.make("implementation-stage-prepared-event"),
        type: "agentControl.stageRun.prepared",
        aggregateKind: "stage-run",
        aggregateId: stageRunId,
        occurredAt: at,
        commandId: CommandId.make("implementation-stage-prepare"),
        causationEventId: null,
        correlationId: CommandId.make("implementation-stage-prepare"),
        authority: "controller",
        metadata: { schemaVersion: 1 },
        payload: {
          projectId,
          taskId,
          stageRunId,
          attemptId,
          roleId: AgentControlRoleId.make("implementer"),
          stageKind: "implementation",
          stageOrdinal: 2,
          attemptOrdinal: 1,
          taskRevision: 4,
          githubIntakeSequence: 9,
          sourceIdentityFingerprint,
          status: "prepared",
          preparedAt: at,
        },
        streamVersion: 1,
        sequence: 11,
      });
      const startEvent = {
        eventId: EventId.make("implementation-stage-started-event"),
        type: "agentControl.stageRun.implementationStarted" as const,
        aggregateKind: "stage-run" as const,
        aggregateId: stageRunId,
        occurredAt: at,
        commandId: CommandId.make("implementation-stage-start"),
        causationEventId: EventId.make("implementation-turn-requested-event"),
        correlationId: CommandId.make("implementation-stage-start"),
        authority: "system" as const,
        metadata: { schemaVersion: 1 as const },
        payload: {
          projectId,
          taskId,
          stageRunId,
          attemptId,
          roleId: "implementer" as const,
          stageKind: "implementation" as const,
          stageOrdinal: 2 as const,
          attemptOrdinal: 1 as const,
          status: "running" as const,
          taskRevision: 4,
          githubIntakeSequence: 9,
          sourceIdentityFingerprint,
          admissionEvidenceId: "admission-evidence",
          admissionReceiptId: "admission-receipt",
          admissionMarkerId: "admission-marker",
          materializationEvidenceId: "materialization-evidence",
          materializationReceiptId: "materialization-receipt",
          materializationMarkerId: "materialization-marker",
          handoffId: "implementation-handoff",
          handoffFingerprint: "c".repeat(64),
          providerDeliveryId: "implementation-delivery",
          deliveryRevision: 4,
          claimGeneration: 1,
          attemptCount: 1,
          controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
            "implementation-reservation",
          ),
          threadId: ThreadId.make("implementation-thread"),
          planningThreadId: ThreadId.make("planning-thread"),
          planId: "plan-1",
          proposedPlanDigest: "d".repeat(64),
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerTurnId: "provider-turn-1",
          runtimeMode: "approval-required" as const,
          modelSelectionFingerprint: "e".repeat(64),
          leaseId: AgentControlStageRunLeaseId.make("implementation-lease"),
          leaseHolderId: AgentControlStageRunLeaseHolderId.make("implementation-holder"),
          fenceToken: 2,
          startedAt: at,
        },
        streamVersion: 2,
        sequence: 12,
      };
      const running = yield* projectAgentControlStageRunEvent(prepared, startEvent);
      assert.equal(running.status, "running");
      assert.equal(running.revision, 2);

      assert.equal(
        (yield* Effect.result(
          projectAgentControlStageRunEvent(prepared, {
            ...startEvent,
            causationEventId: null,
          }),
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(projectAgentControlStageRunEvent(running, startEvent)))._tag,
        "Failure",
      );
    }),
);
