import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  AgentControlTaskListResult,
  AgentControlTaskEventDraft,
  AgentControlTaskReconcileOnceInput,
  AgentControlTaskReactorStatus,
  AgentControlTaskRpcError,
  AgentControlTaskStatus,
} from "./agentControlTask.ts";
import { AgentControlTaskId, ProjectId } from "./baseSchemas.ts";

const decodeReconcile = Schema.decodeUnknownSync(AgentControlTaskReconcileOnceInput);
const decodeList = Schema.decodeUnknownSync(AgentControlTaskListResult);
const decodeStatus = Schema.decodeUnknownSync(AgentControlTaskStatus);
const encodeError = Schema.encodeUnknownSync(AgentControlTaskRpcError);
const decodeReactorStatus = Schema.decodeUnknownSync(AgentControlTaskReactorStatus);
const decodeEventDraft = Schema.decodeUnknownSync(AgentControlTaskEventDraft);

const finalizationSource = {
  projectId: ProjectId.make("project-1"),
  taskId: AgentControlTaskId.make("task-1"),
  verificationTaskRevision: 1,
  previousTaskRevision: 2,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  taskSourceEventId: "task-source-event",
  taskSourceEventSequence: 1,
  taskSourceEventStreamVersion: 1,
  handoffId: "handoff-1",
  handoffFingerprint: "b".repeat(64),
  verificationFinalizationEvidenceId: "verification-evidence-1",
  verificationFinalizationReceiptId: "verification-receipt-1",
  verificationFinalizationMarkerId: "verification-marker-1",
  verificationFinalizationCommandId: "verification-command-1",
  verificationFinalizationFingerprint: "c".repeat(64),
  verificationFinalizationMarkerFingerprint: "d".repeat(64),
  terminalStageRunId: "verification-stage-run-1",
  terminalStageEventId: "verification-stage-event-1",
  terminalStageEventSequence: 20,
  terminalStageEventStreamVersion: 3,
  releasedLeaseId: "verification-lease-1",
  releasedLeaseEventId: "verification-lease-event-1",
  releasedLeaseEventSequence: 21,
  releasedLeaseEventStreamVersion: 8,
  terminalRuntimeEventId: "terminal-runtime-event-1",
  taskFinalizationEvidenceId: "task-finalization-evidence-1",
  previousStatus: "running",
  stage: "verification",
  finalizedAt: "2026-08-30T10:00:00.000Z",
} as const;

const eventDraft = (payload: Record<string, unknown>) => ({
  eventId: "task-final-event-1",
  type: "agentControl.task.finalizedAfterVerification",
  aggregateKind: "task",
  aggregateId: AgentControlTaskId.make("task-1"),
  occurredAt: "2026-08-30T10:00:00.000Z",
  commandId: "task-final-command-1",
  causationEventId: "terminal-runtime-event-1",
  correlationId: "task-final-command-1",
  authority: "system",
  metadata: { schemaVersion: 1 },
  payload,
});

describe("Agent Control task contracts", () => {
  it("allows future execution statuses while keeping reconcile input controller-owned", () => {
    for (const status of [
      "candidate",
      "needs-attention",
      "cancelled",
      "queued",
      "running",
      "waiting",
      "succeeded",
      "failed",
    ]) {
      expect(decodeStatus(status)).toBe(status);
    }
    expect(() =>
      decodeReconcile(
        {
          projectId: "project-1",
          taskId: "attacker-task",
          authority: "human",
          commandId: "attacker-command",
          status: "running",
          stage: "implementation",
        },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });

  it("keeps list results body-free and marks source text untrusted", () => {
    const result = decodeList({
      projectId: ProjectId.make("project-1"),
      quarantinedCount: 0,
      tasks: [
        {
          schemaVersion: 1,
          taskId: AgentControlTaskId.make("task-1"),
          source: {
            projectId: ProjectId.make("project-1"),
            repositoryNodeId: "repo-1",
            issueNodeId: "issue-1",
            issueNumber: 1,
            issueUrl: "https://github.test/o/r/issues/1",
          },
          status: "candidate",
          sourceGate: "eligible",
          stage: "intake",
          sourceUpdatedAt: "2026-07-23T10:00:00.000Z",
          githubIntakeSequence: 1,
          title: "untrusted",
          contentTrust: "untrusted-external",
          createdAt: "2026-07-23T10:00:00.000Z",
          updatedAt: "2026-07-23T10:00:00.000Z",
          revision: 1,
          sequence: 1,
        },
      ],
    });
    expect(JSON.stringify(result)).not.toContain("body");
    expect(result.tasks[0]?.contentTrust).toBe("untrusted-external");
  });

  it("keeps wire errors closed", () => {
    const encoded = encodeError(
      new AgentControlTaskRpcError({
        code: "source-identity-conflict",
        operation: "reconcile-once",
        projectId: ProjectId.make("project-1"),
        taskId: AgentControlTaskId.make("task-1"),
      }),
    );
    expect(encoded).toEqual({
      _tag: "AgentControlTaskRpcError",
      code: "source-identity-conflict",
      operation: "reconcile-once",
      projectId: "project-1",
      taskId: "task-1",
    });
    expect(JSON.stringify(encoded)).not.toMatch(
      /body|title|githubRaw|command|path|exception|credential|token/i,
    );
  });

  it("keeps reactor status operational and transport-safe", () => {
    const status = decodeReactorStatus({
      projectId: ProjectId.make("project-1"),
      activity: "recovering",
      health: "recovering",
      workerState: "backoff",
      subscriptionHealth: "healthy",
      globalHealth: "recovering",
      currentSourceSequence: 7,
      targetSequence: 6,
      lastCompletedSequence: 6,
      sequenceCurrent: false,
      retryAttempt: 2,
      nextAttemptAt: "2026-07-23T10:00:00.000Z",
      lastErrorCode: "source-snapshot-stale",
    });
    expect(status.retryAttempt).toBe(2);
    expect(JSON.stringify(status)).not.toMatch(
      /body|title|command|path|exception|cause|stderr|credential|token/i,
    );
  });

  it("closes the system-authoritative Verification terminal mapping", () => {
    const cases = [
      {
        deliveryTerminalState: "completed",
        verificationOutcome: "succeeded",
        terminalCause: "verification-passed",
        status: "succeeded",
        evaluation: {
          evaluationAuthority: "accepted-evaluation",
          evaluationId: "evaluation-passed",
          evaluationEvidenceId: "evaluation-evidence-passed",
          evaluationReceiptId: "evaluation-receipt-passed",
          evaluationMarkerId: "evaluation-marker-passed",
          evaluationDisposition: "evaluated",
          verificationVerdict: "passed",
          invalidOutputCode: null,
        },
      },
      {
        deliveryTerminalState: "completed",
        verificationOutcome: "failed",
        terminalCause: "verification-failed",
        status: "failed",
        evaluation: {
          evaluationAuthority: "accepted-evaluation",
          evaluationId: "evaluation-failed",
          evaluationEvidenceId: "evaluation-evidence-failed",
          evaluationReceiptId: "evaluation-receipt-failed",
          evaluationMarkerId: "evaluation-marker-failed",
          evaluationDisposition: "evaluated",
          verificationVerdict: "failed",
          invalidOutputCode: null,
        },
      },
      {
        deliveryTerminalState: "completed",
        verificationOutcome: "failed",
        terminalCause: "verification-invalid-output",
        status: "failed",
        evaluation: {
          evaluationAuthority: "accepted-evaluation",
          evaluationId: "evaluation-invalid",
          evaluationEvidenceId: "evaluation-evidence-invalid",
          evaluationReceiptId: "evaluation-receipt-invalid",
          evaluationMarkerId: "evaluation-marker-invalid",
          evaluationDisposition: "invalid-output",
          verificationVerdict: null,
          invalidOutputCode: "schema-violation",
        },
      },
      {
        deliveryTerminalState: "failed",
        verificationOutcome: "failed",
        terminalCause: "provider-delivery-failed",
        status: "failed",
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
      },
      {
        deliveryTerminalState: "interrupted",
        verificationOutcome: "cancelled",
        terminalCause: "provider-delivery-interrupted",
        status: "cancelled",
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
      },
    ] as const;
    for (const mapping of cases) {
      const decoded = decodeEventDraft(eventDraft({ ...finalizationSource, ...mapping }), {
        onExcessProperty: "error",
      });
      expect(decoded.type).toBe("agentControl.task.finalizedAfterVerification");
      expect(decoded.authority).toBe("system");
    }
    expect(() =>
      decodeEventDraft(
        eventDraft({
          ...finalizationSource,
          ...cases[0],
          status: "failed",
        }),
        { onExcessProperty: "error" },
      ),
    ).toThrow();
    expect(() =>
      decodeEventDraft(
        { ...eventDraft({ ...finalizationSource, ...cases[0] }), authority: "controller" },
        { onExcessProperty: "error" },
      ),
    ).toThrow();
  });
});
