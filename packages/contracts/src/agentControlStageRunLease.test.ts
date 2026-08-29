import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlStageRunLeaseReleasedAfterImplementationPayload,
  AgentControlStageRunLeaseReleasedAfterVerificationPayload,
  AgentControlStageRunLeaseState,
  AgentControlStageRunLeaseView,
} from "./agentControlStageRunLease.ts";

const state = {
  schemaVersion: 1,
  leaseId: "stage-run-lease-test",
  projectId: "project-test",
  taskId: "task-test",
  stageRunId: "stage-run-test",
  attemptId: "attempt-test",
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  holderId: "holder-test",
  fenceToken: 1,
  status: "reserved",
  acquiredAt: "2026-07-24T10:00:00.000Z",
  renewedAt: "2026-07-24T10:00:00.000Z",
  expiresAt: "2026-07-24T10:01:00.000Z",
  releasedAt: null,
  revision: 1,
  sequence: 1,
} as const;
const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunLeaseState);
const encodeView = Schema.encodeUnknownEffect(AgentControlStageRunLeaseView);
const decodeImplementationRelease = Schema.decodeUnknownEffect(
  AgentControlStageRunLeaseReleasedAfterImplementationPayload,
);
const decodeVerificationRelease = Schema.decodeUnknownEffect(
  AgentControlStageRunLeaseReleasedAfterVerificationPayload,
);

it.effect("decodes persistent lease state but keeps holder identity out of wire views", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeState(state);
    assert.equal(decoded.holderId, "holder-test");
    const encodedView = yield* encodeView({
      leaseId: decoded.leaseId,
      projectId: decoded.projectId,
      taskId: decoded.taskId,
      stageRunId: decoded.stageRunId,
      attemptId: decoded.attemptId,
      fenceToken: decoded.fenceToken,
      status: decoded.status,
      ownership: "current-runtime",
      health: "healthy",
      acquiredAt: decoded.acquiredAt,
      renewedAt: decoded.renewedAt,
      expiresAt: decoded.expiresAt,
      releasedAt: decoded.releasedAt,
      revision: decoded.revision,
    });
    assert.notProperty(encodedView, "holderId");
  }),
);

it.effect("rejects non-positive fence tokens and inconsistent release shape", () =>
  Effect.gen(function* () {
    assert.equal(
      (yield* Effect.result(
        decodeState({
          ...state,
          fenceToken: 0,
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeState({
          ...state,
          status: "released",
          releasedAt: null,
        }),
      ))._tag,
      "Failure",
    );
  }),
);

it.effect("binds each Implementation lease release to the matching terminal Stage", () =>
  Effect.gen(function* () {
    const release = {
      leaseId: "lease-1",
      projectId: "project-1",
      taskId: "task-1",
      stageRunId: "stage-run-1",
      attemptId: "attempt-1",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: "source-fingerprint",
      holderId: "holder-1",
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
      handoffId: "handoff-1",
      handoffFingerprint: "handoff-fingerprint",
      controlledThreadReservationId: "controlled-thread-reservation-1",
      threadId: "thread-1",
      planningThreadId: "planning-thread-1",
      planId: "plan-1",
      proposedPlanDigest: "plan-digest",
      providerDeliveryId: "delivery-1",
      deliveryTerminalState: "completed",
      deliveryRevision: 5,
      providerInstanceId: "provider-instance-1",
      providerTurnId: "provider-turn-1",
      runtimeMode: "full-access",
      modelSelectionFingerprint: "model-fingerprint",
      orchestrationHistoryDigest: "orchestration-digest",
      resultEvidenceId: "result-evidence",
      stageEventId: "stage-event",
      stageStatus: "succeeded",
      releasedAt: "2026-08-05T10:00:00.000Z",
    } as const;
    assert.equal((yield* decodeImplementationRelease(release)).stageStatus, "succeeded");
    assert.equal(
      (yield* Effect.result(
        decodeImplementationRelease({ ...release, deliveryTerminalState: "failed" }),
      ))._tag,
      "Failure",
    );
  }),
);

it.effect("binds releasedAfterVerification to one closed Stage and evaluation outcome", () =>
  Effect.gen(function* () {
    const release = {
      leaseId: "verification-lease-1",
      projectId: "project-1",
      taskId: "task-1",
      stageRunId: "verification-stage-run-1",
      attemptId: "verification-attempt-1",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: "source-fingerprint",
      holderId: "holder-1",
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
      handoffFingerprint: "handoff-fingerprint",
      controlledThreadReservationId: "verification-reservation",
      threadId: "verification-thread",
      planningThreadId: "planning-thread",
      planId: "plan-1",
      proposedPlanDigest: "plan-digest",
      providerDeliveryId: "verification-delivery",
      deliveryRevision: 5,
      providerInstanceId: "provider-instance",
      providerTurnId: "provider-turn",
      runtimeMode: "approval-required",
      modelSelectionFingerprint: "model-fingerprint",
      terminalRuntimeEventId: "terminal-runtime-event",
      finalizationEvidenceId: "finalization-evidence",
      stageEventId: "terminal-stage-event",
      deliveryTerminalState: "completed",
      terminalCause: "verification-invalid-output",
      stageStatus: "failed",
      evaluation: {
        evaluationAuthority: "accepted-evaluation",
        evaluationId: "evaluation-1",
        evaluationEvidenceId: "evaluation-evidence-1",
        evaluationReceiptId: "evaluation-receipt-1",
        evaluationMarkerId: "evaluation-marker-1",
        evaluationDisposition: "invalid-output",
        verificationVerdict: null,
        invalidOutputCode: "malformed-json",
      },
      releasedAt: "2026-08-29T10:00:00.000Z",
    } as const;
    const decoded = yield* decodeVerificationRelease({ ...release, report: "secret" });
    assert.equal(decoded.stageStatus, "failed");
    assert.notProperty(decoded, "report");
    assert.equal(
      (yield* Effect.result(
        decodeVerificationRelease({
          ...release,
          stageStatus: "succeeded",
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeVerificationRelease({
          ...release,
          evaluation: {
            evaluationAuthority: "not-applicable",
            evaluationId: null,
            evaluationEvidenceId: null,
            evaluationReceiptId: null,
            evaluationMarkerId: null,
            evaluationDisposition: null,
            verificationVerdict: null,
            invalidOutputCode: null,
          },
        }),
      ))._tag,
      "Failure",
    );
  }),
);
