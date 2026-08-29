import {
  type AgentControlStageRunLeaseEvent,
  AgentControlStageRunLeaseHolderId,
  type AgentControlStageRunLeaseState,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "../stageRun/identity.ts";
import { decideAgentControlStageRunLeaseCommand } from "./decider.ts";
import { deriveAgentControlStageRunLeaseId } from "./identity.ts";
import { projectAgentControlStageRunLeaseEvent } from "./projector.ts";

const at = "2026-07-24T10:00:00.000Z";
const expiresAt = "2026-07-24T10:01:00.000Z";

const fixture = Effect.fn("stageRunLeaseDeciderFixture")(function* () {
  const projectId = ProjectId.make("lease-decider-project");
  const taskId = AgentControlTaskId.make("lease-decider-task");
  const sourceIdentityFingerprint = "a".repeat(64);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  return {
    projectId,
    taskId,
    leaseId,
    stageRunId,
    attemptId,
    sourceIdentityFingerprint,
    holderId: AgentControlStageRunLeaseHolderId.make("holder-decider"),
  };
});

it.effect("reserves token 1, releases, then requires token 2 for a new reservation", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const reserve = {
      type: "agentControl.stageRunLease.reserve" as const,
      commandId: CommandId.make("lease-reserve-1"),
      authority: "controller" as const,
      ...f,
      fenceToken: 1,
      expectedRevision: 0,
      taskRevision: 1,
      githubIntakeSequence: 1,
      leaseDurationMs: 60_000,
    };
    const reservedDraft = (yield* decideAgentControlStageRunLeaseCommand({
      state: null,
      command: reserve,
      eventId: EventId.make("event-reserve-1"),
      occurredAt: at,
      expiresAt,
    }))[0]!;
    const reserved = yield* projectAgentControlStageRunLeaseEvent(null, {
      ...reservedDraft,
      streamVersion: 1,
      sequence: 1,
    });
    assert.equal(reserved.fenceToken, 1);
    assert.equal(reserved.status, "reserved");

    const release = {
      type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
      commandId: CommandId.make("lease-release-1"),
      authority: "controller" as const,
      leaseId: f.leaseId,
      projectId: f.projectId,
      taskId: f.taskId,
      stageRunId: f.stageRunId,
      attemptId: f.attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: f.sourceIdentityFingerprint,
      holderId: f.holderId,
      fenceToken: 1,
      expectedRevision: 1,
    };
    const releasedDraft = (yield* decideAgentControlStageRunLeaseCommand({
      state: reserved,
      command: release,
      eventId: EventId.make("event-release-1"),
      occurredAt: "2026-07-24T10:00:10.000Z",
      expiresAt: null,
    }))[0]!;
    const released = yield* projectAgentControlStageRunLeaseEvent(reserved, {
      ...releasedDraft,
      streamVersion: 2,
      sequence: 2,
    });
    assert.equal(released.status, "released");

    const staleToken = yield* Effect.result(
      decideAgentControlStageRunLeaseCommand({
        state: released,
        command: {
          ...reserve,
          commandId: CommandId.make("lease-reserve-stale"),
          expectedRevision: 2,
        },
        eventId: EventId.make("event-reserve-stale"),
        occurredAt: "2026-07-24T10:00:20.000Z",
        expiresAt: "2026-07-24T10:01:20.000Z",
      }),
    );
    assert.equal(staleToken._tag, "Failure");
    if (staleToken._tag === "Failure") {
      assert.equal(staleToken.failure.code, "fence-token-mismatch");
    }
  }),
);

it.effect("projects all five exact Verification release outcomes from one reserved lease", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    const reserved: AgentControlStageRunLeaseState = {
      schemaVersion: 1,
      leaseId: f.leaseId,
      projectId: f.projectId,
      taskId: f.taskId,
      stageRunId: f.stageRunId,
      attemptId: f.attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: f.sourceIdentityFingerprint,
      holderId: f.holderId,
      fenceToken: 3,
      status: "reserved",
      acquiredAt: at,
      renewedAt: at,
      expiresAt,
      releasedAt: null,
      revision: 2,
      sequence: 10,
    };
    const commonPayload = {
      leaseId: f.leaseId,
      projectId: f.projectId,
      taskId: f.taskId,
      stageRunId: f.stageRunId,
      attemptId: f.attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: f.sourceIdentityFingerprint,
      holderId: f.holderId,
      fenceToken: 3,
      admissionEvidenceId: "admission-evidence",
      admissionReceiptId: "admission-receipt",
      admissionMarkerId: "admission-marker",
      materializationEvidenceId: "materialization-evidence",
      materializationReceiptId: "materialization-receipt",
      materializationMarkerId: "materialization-marker",
      startEvidenceId: "start-evidence",
      startReceiptId: "start-receipt",
      startMarkerId: "start-marker",
      handoffId: "verification-handoff",
      handoffFingerprint: "b".repeat(64),
      controlledThreadReservationId: "verification-reservation",
      threadId: "verification-thread",
      planningThreadId: "planning-thread",
      planId: "plan-1",
      proposedPlanDigest: "c".repeat(64),
      providerDeliveryId: "verification-delivery",
      deliveryRevision: 6,
      providerInstanceId: "codex",
      providerTurnId: "provider-turn-1",
      runtimeMode: "approval-required" as const,
      modelSelectionFingerprint: "d".repeat(64),
      terminalRuntimeEventId: EventId.make("verification-runtime-terminal"),
      finalizationEvidenceId: "verification-finalization-evidence",
      stageEventId: EventId.make("verification-terminal-stage-event"),
      releasedAt: at,
    };
    const acceptedEvaluation = {
      evaluationAuthority: "accepted-evaluation" as const,
      evaluationId: "evaluation-1",
      evaluationEvidenceId: "evaluation-evidence-1",
      evaluationReceiptId: "evaluation-receipt-1",
      evaluationMarkerId: "evaluation-marker-1",
    };
    const notApplicable = {
      evaluationAuthority: "not-applicable" as const,
      evaluationId: null,
      evaluationEvidenceId: null,
      evaluationReceiptId: null,
      evaluationMarkerId: null,
      evaluationDisposition: null,
      verificationVerdict: null,
      invalidOutputCode: null,
    };
    const scenarios = [
      {
        name: "passed",
        stageStatus: "succeeded",
        deliveryTerminalState: "completed",
        terminalCause: "verification-passed",
        evaluation: {
          ...acceptedEvaluation,
          evaluationDisposition: "evaluated",
          verificationVerdict: "passed",
          invalidOutputCode: null,
        },
      },
      {
        name: "failed-verdict",
        stageStatus: "failed",
        deliveryTerminalState: "completed",
        terminalCause: "verification-failed",
        evaluation: {
          ...acceptedEvaluation,
          evaluationDisposition: "evaluated",
          verificationVerdict: "failed",
          invalidOutputCode: null,
        },
      },
      {
        name: "invalid-output",
        stageStatus: "failed",
        deliveryTerminalState: "completed",
        terminalCause: "verification-invalid-output",
        evaluation: {
          ...acceptedEvaluation,
          evaluationDisposition: "invalid-output",
          verificationVerdict: null,
          invalidOutputCode: "malformed-json",
        },
      },
      {
        name: "delivery-failed",
        stageStatus: "failed",
        deliveryTerminalState: "failed",
        terminalCause: "provider-delivery-failed",
        evaluation: notApplicable,
      },
      {
        name: "interrupted",
        stageStatus: "cancelled",
        deliveryTerminalState: "interrupted",
        terminalCause: "provider-delivery-interrupted",
        evaluation: notApplicable,
      },
    ] as const;

    for (const [index, scenario] of scenarios.entries()) {
      const event = {
        eventId: EventId.make(`verification-release-event-${scenario.name}`),
        type: "agentControl.stageRunLease.releasedAfterVerification",
        aggregateKind: "stage-run-lease",
        aggregateId: f.leaseId,
        occurredAt: at,
        commandId: CommandId.make(`verification-release-command-${scenario.name}`),
        causationEventId: commonPayload.stageEventId,
        correlationId: CommandId.make(`verification-release-command-${scenario.name}`),
        authority: "system",
        metadata: { schemaVersion: 1 },
        payload: {
          ...commonPayload,
          stageStatus: scenario.stageStatus,
          deliveryTerminalState: scenario.deliveryTerminalState,
          terminalCause: scenario.terminalCause,
          evaluation: scenario.evaluation,
        },
        streamVersion: 3,
        sequence: 11 + index,
      } as unknown as AgentControlStageRunLeaseEvent;
      const released = yield* projectAgentControlStageRunLeaseEvent(reserved, event);
      assert.equal(released.status, "released");
      assert.equal(released.revision, 3);
      assert.equal(released.sequence, 11 + index);
    }

    const passed = scenarios[0];
    const invalidEnvelope = {
      eventId: EventId.make("verification-release-invalid-envelope"),
      type: "agentControl.stageRunLease.releasedAfterVerification",
      aggregateKind: "stage-run-lease",
      aggregateId: f.leaseId,
      occurredAt: at,
      commandId: CommandId.make("verification-release-invalid-envelope-command"),
      causationEventId: null,
      correlationId: CommandId.make("verification-release-invalid-envelope-command"),
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: {
        ...commonPayload,
        stageStatus: passed.stageStatus,
        deliveryTerminalState: passed.deliveryTerminalState,
        terminalCause: passed.terminalCause,
        evaluation: passed.evaluation,
      },
      streamVersion: 3,
      sequence: 16,
    } as unknown as AgentControlStageRunLeaseEvent;
    assert.equal(
      (yield* Effect.result(projectAgentControlStageRunLeaseEvent(reserved, invalidEnvelope)))._tag,
      "Failure",
    );
  }),
);
