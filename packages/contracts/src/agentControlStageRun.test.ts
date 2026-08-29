import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  AgentControlStageRunImplementationSucceededPayload,
  AgentControlStageRunPrepareInitialInput,
  AgentControlStageRunRpcError,
  AgentControlStageRunState,
  AgentControlStageRunVerificationCancelledPayload,
  AgentControlStageRunVerificationFailedPayload,
  AgentControlStageRunVerificationStartedPayload,
  AgentControlStageRunVerificationSucceededPayload,
  AgentControlStageRunVerificationTerminalPayloadStorage,
} from "./agentControlStageRun.ts";

const decodeState = Schema.decodeUnknownEffect(AgentControlStageRunState);
const decodePrepare = Schema.decodeUnknownEffect(AgentControlStageRunPrepareInitialInput);
const decodeRpcError = Schema.decodeUnknownEffect(AgentControlStageRunRpcError);
const decodeImplementationSucceeded = Schema.decodeUnknownEffect(
  AgentControlStageRunImplementationSucceededPayload,
);
const decodeVerificationStarted = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationStartedPayload,
);
const encodeVerificationStarted = Schema.encodeEffect(
  AgentControlStageRunVerificationStartedPayload,
);
const decodeVerificationSucceeded = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationSucceededPayload,
);
const decodeVerificationFailed = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationFailedPayload,
);
const decodeVerificationCancelled = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationCancelledPayload,
);
const decodeVerificationTerminalStorage = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationTerminalPayloadStorage,
);

const implementationSucceeded = {
  projectId: "project-1",
  taskId: "task-1",
  stageRunId: "stage-run-1",
  attemptId: "attempt-1",
  roleId: "implementer",
  stageKind: "implementation",
  stageOrdinal: 2,
  attemptOrdinal: 1,
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "source-fingerprint",
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
  providerDeliveryId: "delivery-1",
  deliveryRevision: 5,
  deliveryTerminalState: "completed",
  claimGeneration: 1,
  attemptCount: 1,
  controlledThreadReservationId: "controlled-thread-reservation-1",
  threadId: "thread-1",
  planningThreadId: "planning-thread-1",
  planId: "plan-1",
  proposedPlanDigest: "plan-digest",
  repositoryDisplay: "owner/repository",
  sourceRevision: "source-revision",
  taskSourceEventId: "task-source-event",
  taskSourceEventSequence: 1,
  taskSourceEventStreamVersion: 1,
  worktreeReservationId: "worktree-1",
  worktreeEventId: "worktree-event",
  worktreeEventSequence: 2,
  worktreeEventStreamVersion: 1,
  worktreeOwnershipFingerprint: "worktree-fingerprint",
  turnRequestCommandId: "turn-request-command",
  messageId: "message-1",
  messageEventId: "message-event",
  turnRequestEventId: "turn-request-event",
  providerInstanceId: "provider-instance-1",
  providerTurnId: "provider-turn-1",
  runtimeMode: "full-access",
  modelSelectionFingerprint: "model-fingerprint",
  leaseId: "lease-1",
  leaseHolderId: "holder-1",
  fenceToken: 3,
  providerStartedEventId: "provider-started-event",
  providerStartedSequence: 3,
  providerStartedStreamVersion: 5,
  providerTerminalEventId: "provider-terminal-event",
  providerTerminalSequence: 4,
  providerTerminalStreamVersion: 6,
  orchestrationHistoryDigest: "orchestration-digest",
  orchestrationHistoryEventCount: 6,
  resultEvidenceId: "result-evidence",
  status: "succeeded",
  finalizedAt: "2026-08-05T10:00:00.000Z",
} as const;

const verificationStarted = {
  projectId: "project-1",
  taskId: "task-1",
  stageRunId: "verification-stage-run-1",
  attemptId: "verification-attempt-1",
  roleId: "verifier",
  stageKind: "verification",
  stageOrdinal: 3,
  attemptOrdinal: 1,
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "source-fingerprint",
  status: "running",
  admissionEvidenceId: "admission-evidence",
  admissionReceiptId: "admission-receipt",
  admissionMarkerId: "admission-marker",
  materializationEvidenceId: "materialization-evidence",
  materializationReceiptId: "materialization-receipt",
  materializationMarkerId: "materialization-marker",
  handoffId: "verification-handoff",
  handoffFingerprint: "handoff-fingerprint",
  providerDeliveryId: "verification-delivery",
  deliveryRevision: 5,
  claimGeneration: 1,
  attemptCount: 1,
  controlledThreadReservationId: "verification-reservation",
  threadId: "verification-thread",
  planningThreadId: "planning-thread",
  planId: "plan-1",
  proposedPlanDigest: "plan-digest",
  providerInstanceId: "provider-instance",
  providerTurnId: "provider-turn",
  runtimeMode: "approval-required",
  modelSelectionFingerprint: "model-fingerprint",
  leaseId: "verification-lease",
  leaseHolderId: "historical-holder",
  fenceToken: 3,
  startedAt: "2026-08-06T10:00:00.000Z",
} as const;

const verificationFinalized = {
  ...verificationStarted,
  startEvidenceId: "verification-start-evidence",
  startReceiptId: "verification-start-receipt",
  startMarkerId: "verification-start-marker",
  terminalRuntimeEventId: "verification-terminal-runtime-event",
  finalizationEvidenceId: "verification-finalization-evidence",
  finalizedAt: "2026-08-06T10:01:00.000Z",
} as const;

const acceptedEvaluation = {
  evaluationAuthority: "accepted-evaluation",
  evaluationId: "evaluation-1",
  evaluationEvidenceId: "evaluation-evidence-1",
  evaluationReceiptId: "evaluation-receipt-1",
  evaluationMarkerId: "evaluation-marker-1",
} as const;

const noEvaluation = {
  evaluationAuthority: "not-applicable",
  evaluationId: null,
  evaluationEvidenceId: null,
  evaluationReceiptId: null,
  evaluationMarkerId: null,
  evaluationDisposition: null,
  verificationVerdict: null,
  invalidOutputCode: null,
} as const;

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

it.effect("binds an Implementation success to completed delivery and positive start version", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeImplementationSucceeded(implementationSucceeded);
    assert.equal(decoded.deliveryTerminalState, "completed");
    assert.equal(decoded.status, "succeeded");
    assert.equal(
      (yield* Effect.result(
        decodeImplementationSucceeded({
          ...implementationSucceeded,
          deliveryTerminalState: "failed",
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeImplementationSucceeded({
          ...implementationSucceeded,
          providerStartedStreamVersion: 0,
        }),
      ))._tag,
      "Failure",
    );
  }),
);

it.effect("round-trips the closed Verification started payload", () =>
  Effect.gen(function* () {
    const decoded = yield* decodeVerificationStarted(verificationStarted);
    assert.deepStrictEqual(yield* encodeVerificationStarted(decoded), verificationStarted);
    assert.equal(decoded.roleId, "verifier");
    assert.equal(decoded.runtimeMode, "approval-required");
    assert.equal(
      (yield* Effect.result(
        decodeVerificationStarted({ ...verificationStarted, runtimeMode: "full-access" }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(decodeVerificationStarted({ ...verificationStarted, stageOrdinal: 2 })))
        ._tag,
      "Failure",
    );
  }),
);

it.effect("decodes only the five closed Verification terminal outcomes", () =>
  Effect.gen(function* () {
    const cases = [
      [
        decodeVerificationSucceeded,
        {
          ...verificationFinalized,
          deliveryTerminalState: "completed",
          terminalCause: "verification-passed",
          status: "succeeded",
          evaluation: {
            ...acceptedEvaluation,
            evaluationDisposition: "evaluated",
            verificationVerdict: "passed",
            invalidOutputCode: null,
          },
        },
      ],
      [
        decodeVerificationFailed,
        {
          ...verificationFinalized,
          deliveryTerminalState: "completed",
          terminalCause: "verification-failed",
          status: "failed",
          evaluation: {
            ...acceptedEvaluation,
            evaluationDisposition: "evaluated",
            verificationVerdict: "failed",
            invalidOutputCode: null,
          },
        },
      ],
      [
        decodeVerificationFailed,
        {
          ...verificationFinalized,
          deliveryTerminalState: "completed",
          terminalCause: "verification-invalid-output",
          status: "failed",
          evaluation: {
            ...acceptedEvaluation,
            evaluationDisposition: "invalid-output",
            verificationVerdict: null,
            invalidOutputCode: "schema-violation",
          },
        },
      ],
      [
        decodeVerificationFailed,
        {
          ...verificationFinalized,
          deliveryTerminalState: "failed",
          terminalCause: "provider-delivery-failed",
          status: "failed",
          evaluation: noEvaluation,
        },
      ],
      [
        decodeVerificationCancelled,
        {
          ...verificationFinalized,
          deliveryTerminalState: "interrupted",
          terminalCause: "provider-delivery-interrupted",
          status: "cancelled",
          evaluation: noEvaluation,
        },
      ],
    ] as const;

    for (const [decode, input] of cases) {
      const decoded = yield* decode({ ...input, rawOutput: "secret", report: { secret: true } });
      assert.notProperty(decoded, "rawOutput");
      assert.notProperty(decoded, "report");
      assert.equal(
        (yield* Effect.result(decodeVerificationTerminalStorage({ ...input, rawOutput: "secret" })))
          ._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(
          decodeVerificationTerminalStorage({
            ...input,
            evaluation: { ...input.evaluation, report: "secret" },
          }),
        ))._tag,
        "Failure",
      );
    }
    assert.equal(
      (yield* Effect.result(
        decodeVerificationFailed({
          ...cases[2][1],
          evaluation: { ...cases[2][1].evaluation, verificationVerdict: "failed" },
        }),
      ))._tag,
      "Failure",
    );
    assert.equal(
      (yield* Effect.result(
        decodeVerificationFailed({
          ...cases[3][1],
          evaluation: { ...noEvaluation, evaluationId: "invented-evaluation" },
        }),
      ))._tag,
      "Failure",
    );
  }),
);
