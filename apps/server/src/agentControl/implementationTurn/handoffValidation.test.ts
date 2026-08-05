import {
  AgentControlControlledThreadReservationId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";

import { canonicalProviderModelSelectionEvidence } from "../../provider/Services/ProviderAdapter.ts";
import {
  canonicalJson,
  combinedInitialPlanningEventDigest,
  sha256Utf8,
} from "../initialPlanning/eventEvidence.ts";
import {
  buildExpectedAgentControlImplementationHandoff,
  implementationHandoffAuthorityMismatch,
  type AgentControlImplementationHandoffAuthority,
} from "./handoffValidation.ts";
import { fingerprintImplementationHandoff } from "./identity.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("implementation-provider"),
  model: "gpt-5.6",
  options: [{ id: "reasoning-effort", value: "high" }],
} as const;
const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);
const proposedPlanJson = canonicalJson({
  planId: "plan-1",
  planText: "Implement the accepted change.",
  schemaVersion: 1,
});

const authority = {
  materializationEvidenceId: "materialization-evidence-1",
  materializationReceiptId: "materialization-receipt-1",
  materializationMarkerId: "materialization-marker-1",
  admissionEvidenceId: "admission-evidence-1",
  admissionReceiptId: "admission-receipt-1",
  admissionMarkerId: "admission-marker-1",
  projectId: ProjectId.make("project-1"),
  taskId: "task-1",
  taskRevision: 4,
  githubIntakeSequence: 7,
  sourceIdentityFingerprint: "a".repeat(64),
  taskSourceEventId: "task-source-event-1",
  taskSourceEventSequence: 11,
  taskSourceEventStreamVersion: 4,
  stageRunId: "stage-run-1",
  attemptId: "attempt-1",
  leaseId: "lease-1",
  leaseHolderId: "holder-1",
  fenceToken: 9,
  worktreeReservationId: "worktree-1",
  worktreeRevision: 3,
  worktreeEventId: "worktree-event-3",
  worktreeEventSequence: 13,
  worktreeEventStreamVersion: 3,
  worktreeOwnershipFingerprint: "b".repeat(64),
  worktreeVerifiedAt: "2026-08-04T12:00:00.000Z",
  worktreePath: "/durable/evidence/worktree",
  branch: "feat/implementation",
  controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
    "implementation-reservation-1",
  ),
  threadId: ThreadId.make("implementation-thread-1"),
  planningThreadId: ThreadId.make("planning-thread-1"),
  planId: "plan-1",
  proposedPlanJson,
  proposedPlanDigest: sha256Utf8(proposedPlanJson),
  repositoryDisplay: "owner/repository",
  sourceRevision: "c".repeat(40),
  taskTitle: "Implement the accepted plan",
  taskBody: "Keep the change focused.",
  providerInstanceId: modelSelection.instanceId,
  runtimeMode: "full-access",
  modelSelection,
  modelSelectionJson: modelEvidence.modelSelectionJson,
  modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
  createdAt: "2026-08-04T12:01:00.000Z",
} as const satisfies AgentControlImplementationHandoffAuthority;

it("binds every derived implementation handoff field to one authoritative root", () => {
  const canonical = buildExpectedAgentControlImplementationHandoff(authority);
  assert.equal(implementationHandoffAuthorityMismatch(authority, canonical), null);

  const alternateModel = {
    instanceId: ProviderInstanceId.make("foreign-provider"),
    model: "gpt-5.6-mini",
    options: [{ id: "reasoning-effort", value: "medium" }],
  } as const;
  const alternateModelEvidence = canonicalProviderModelSelectionEvidence(alternateModel);
  const alternatePlanJson = canonicalJson({
    planId: "plan-1",
    planText: "Execute a different plan.",
    schemaVersion: 1,
  });
  const variants: ReadonlyArray<readonly [string, AgentControlImplementationHandoffAuthority]> = [
    [
      "plan text",
      {
        ...authority,
        proposedPlanJson: alternatePlanJson,
        proposedPlanDigest: sha256Utf8(alternatePlanJson),
      },
    ],
    ["task title", { ...authority, taskTitle: "Foreign task title" }],
    ["task body", { ...authority, taskBody: "Foreign task body" }],
    ["task source event", { ...authority, taskSourceEventId: "foreign-task-event" }],
    ["task source sequence", { ...authority, taskSourceEventSequence: 12 }],
    ["repository", { ...authority, repositoryDisplay: "foreign/repository" }],
    ["source revision", { ...authority, sourceRevision: "d".repeat(40) }],
    [
      "planning thread",
      { ...authority, planningThreadId: ThreadId.make("foreign-planning-thread") },
    ],
    ["plan id", { ...authority, planId: "foreign-plan" }],
    [
      "implementation thread",
      { ...authority, threadId: ThreadId.make("foreign-implementation-thread") },
    ],
    [
      "provider and model",
      {
        ...authority,
        providerInstanceId: alternateModel.instanceId,
        modelSelection: alternateModel,
        modelSelectionJson: alternateModelEvidence.modelSelectionJson,
        modelSelectionFingerprint: alternateModelEvidence.modelSelectionFingerprint,
      },
    ],
    ["runtime", { ...authority, runtimeMode: "approval-required" }],
  ];
  for (const [name, variant] of variants) {
    const divergent = buildExpectedAgentControlImplementationHandoff(variant);
    assert.notEqual(
      implementationHandoffAuthorityMismatch(authority, divergent),
      null,
      `${name} must not be self-authorizing`,
    );
  }

  assert.throws(() =>
    buildExpectedAgentControlImplementationHandoff({
      ...authority,
      proposedPlanDigest: "e".repeat(64),
    }),
  );

  const wrongVersionBase = {
    ...canonical,
    handoffFingerprint: "",
    templateVersion: "agent-control-implementation-prompt-v2",
  };
  const wrongVersion = {
    ...wrongVersionBase,
    handoffFingerprint: fingerprintImplementationHandoff(wrongVersionBase),
  };
  assert.equal(
    implementationHandoffAuthorityMismatch(authority, wrongVersion),
    "handoffFingerprint",
  );

  const messageTemplate = canonical.messageEventTemplateJson.replace(
    '"metadata":{}',
    '"metadata":{"foreign":true}',
  );
  const wrongMessageBase = {
    ...canonical,
    handoffFingerprint: "",
    messageEventTemplateJson: messageTemplate,
    eventTemplateDigest: combinedInitialPlanningEventDigest(
      messageTemplate,
      canonical.turnRequestEventTemplateJson,
    ),
  };
  const wrongMessage = {
    ...wrongMessageBase,
    handoffFingerprint: fingerprintImplementationHandoff(wrongMessageBase),
  };
  assert.notEqual(implementationHandoffAuthorityMismatch(authority, wrongMessage), null);

  const turnTemplate = canonical.turnRequestEventTemplateJson.replace(
    authority.planningThreadId,
    "foreign-planning-thread",
  );
  const wrongTurnBase = {
    ...canonical,
    handoffFingerprint: "",
    turnRequestEventTemplateJson: turnTemplate,
    eventTemplateDigest: combinedInitialPlanningEventDigest(
      canonical.messageEventTemplateJson,
      turnTemplate,
    ),
  };
  const wrongTurn = {
    ...wrongTurnBase,
    handoffFingerprint: fingerprintImplementationHandoff(wrongTurnBase),
  };
  assert.notEqual(implementationHandoffAuthorityMismatch(authority, wrongTurn), null);
});

it("preserves canonical Unicode plan and task data byte-for-byte", () => {
  const unicodePlanJson = canonicalJson({
    planId: "plan-unicode",
    planText: "Ändere Grüße 👩🏽‍💻 und 東京 ohne Datenverlust.",
    schemaVersion: 1,
  });
  const unicodeAuthority = {
    ...authority,
    planId: "plan-unicode",
    proposedPlanJson: unicodePlanJson,
    proposedPlanDigest: sha256Utf8(unicodePlanJson),
    taskTitle: "Grüße aus Köln 👋",
    taskBody: "Unicode bleibt erhalten: 東京 und café.",
  };
  const evidence = buildExpectedAgentControlImplementationHandoff(unicodeAuthority);
  assert.equal(implementationHandoffAuthorityMismatch(unicodeAuthority, evidence), null);
  assert.include(evidence.promptText, "Grüße aus Köln 👋");
  assert.include(evidence.promptText, "東京");
  assert.include(evidence.promptText, "café");
  assert.include(evidence.promptText, "Ändere Grüße 👩🏽‍💻");
});
