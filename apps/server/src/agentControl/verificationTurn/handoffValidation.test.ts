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
  buildExpectedAgentControlVerificationHandoff,
  verificationHandoffAuthorityMismatch,
  type AgentControlVerificationHandoffAuthority,
} from "./handoffValidation.ts";
import { fingerprintVerificationHandoff } from "./identity.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("verification-provider"),
  model: "gpt-5.6",
  options: [{ id: "reasoning-effort", value: "high" }],
} as const;
const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);
const proposedPlanJson = canonicalJson({
  planId: "plan-1",
  planMarkdown: "Implement the accepted change.",
  schemaVersion: 1,
});
const implementationHandoffJson = canonicalJson({ handoffId: "implementation-handoff" });
const implementationProviderDeliveryJson = canonicalJson({
  providerTurnId: "implementation-provider-turn",
});
const implementationResultJson = canonicalJson({ outcome: "succeeded" });
const verificationAdmissionJson = canonicalJson({ admissionEvidenceId: "verification-admission" });
const verificationIdentityJson = canonicalJson({
  roleId: "verifier",
  stageKind: "verification",
  stageOrdinal: 3,
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
  branch: "feat/verification",
  controlledThreadReservationId: AgentControlControlledThreadReservationId.make(
    "verification-reservation-1",
  ),
  threadId: ThreadId.make("verification-thread-1"),
  planningThreadId: ThreadId.make("planning-thread-1"),
  planId: "plan-1",
  proposedPlanJson,
  proposedPlanDigest: sha256Utf8(proposedPlanJson),
  implementationHandoffJson,
  implementationHandoffDigest: sha256Utf8(implementationHandoffJson),
  implementationProviderDeliveryJson,
  implementationProviderDeliveryDigest: sha256Utf8(implementationProviderDeliveryJson),
  implementationResultJson,
  implementationResultDigest: sha256Utf8(implementationResultJson),
  verificationAdmissionJson,
  verificationAdmissionDigest: sha256Utf8(verificationAdmissionJson),
  verificationIdentityJson,
  verificationIdentityDigest: sha256Utf8(verificationIdentityJson),
  repositoryDisplay: "owner/repository",
  sourceRevision: "c".repeat(40),
  taskTitle: "Implement the accepted plan",
  taskBody: "Keep the change focused.",
  providerInstanceId: modelSelection.instanceId,
  runtimeMode: "approval-required",
  modelSelection,
  modelSelectionJson: modelEvidence.modelSelectionJson,
  modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
  createdAt: "2026-08-04T12:01:00.000Z",
} as const satisfies AgentControlVerificationHandoffAuthority;

it("binds every derived verification handoff field to one authoritative root", () => {
  const canonical = buildExpectedAgentControlVerificationHandoff(authority);
  assert.equal(verificationHandoffAuthorityMismatch(authority, canonical), null);

  const alternateModel = {
    instanceId: ProviderInstanceId.make("foreign-provider"),
    model: "gpt-5.6-mini",
    options: [{ id: "reasoning-effort", value: "medium" }],
  } as const;
  const alternateModelEvidence = canonicalProviderModelSelectionEvidence(alternateModel);
  const alternatePlanJson = canonicalJson({
    planId: "plan-1",
    planMarkdown: "Execute a different plan.",
    schemaVersion: 1,
  });
  const variants: ReadonlyArray<readonly [string, AgentControlVerificationHandoffAuthority]> = [
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
      "verification thread",
      { ...authority, threadId: ThreadId.make("foreign-verification-thread") },
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
    [
      "implementation result",
      {
        ...authority,
        implementationResultJson: canonicalJson({ outcome: "failed" }),
        implementationResultDigest: sha256Utf8(canonicalJson({ outcome: "failed" })),
      },
    ],
  ];
  for (const [name, variant] of variants) {
    const divergent = buildExpectedAgentControlVerificationHandoff(variant);
    assert.notEqual(
      verificationHandoffAuthorityMismatch(authority, divergent),
      null,
      `${name} must not be self-authorizing`,
    );
  }

  assert.throws(() =>
    buildExpectedAgentControlVerificationHandoff({
      ...authority,
      proposedPlanDigest: "e".repeat(64),
    }),
  );

  const wrongVersionBase = {
    ...canonical,
    handoffFingerprint: "",
    templateVersion: "agent-control-verification-prompt-v1",
    promptContractFingerprint: null,
    resultSchemaVersion: null,
    resultSchemaFingerprint: null,
  };
  const wrongVersion = {
    ...wrongVersionBase,
    handoffFingerprint: fingerprintVerificationHandoff(wrongVersionBase),
  };
  assert.equal(verificationHandoffAuthorityMismatch(authority, wrongVersion), "handoffFingerprint");

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
    handoffFingerprint: fingerprintVerificationHandoff(wrongMessageBase),
  };
  assert.notEqual(verificationHandoffAuthorityMismatch(authority, wrongMessage), null);

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
    handoffFingerprint: fingerprintVerificationHandoff(wrongTurnBase),
  };
  assert.notEqual(verificationHandoffAuthorityMismatch(authority, wrongTurn), null);
});

it("preserves canonical Unicode plan and task data byte-for-byte", () => {
  const unicodePlanJson = canonicalJson({
    planId: "plan-unicode",
    planMarkdown: "Ändere Grüße 👩🏽‍💻 und 東京 ohne Datenverlust.",
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
  const evidence = buildExpectedAgentControlVerificationHandoff(unicodeAuthority);
  assert.equal(verificationHandoffAuthorityMismatch(unicodeAuthority, evidence), null);
  assert.include(evidence.promptText, "Grüße aus Köln 👋");
  assert.include(evidence.promptText, "東京");
  assert.include(evidence.promptText, "café");
  assert.include(evidence.promptText, "Ändere Grüße 👩🏽‍💻");
});

it("binds coherently altered excluded evidence to the compact handoff", () => {
  const accepted = buildExpectedAgentControlVerificationHandoff(authority);
  for (const [jsonField, digestField] of [
    ["implementationHandoffJson", "implementationHandoffDigest"],
    ["implementationProviderDeliveryJson", "implementationProviderDeliveryDigest"],
    ["implementationResultJson", "implementationResultDigest"],
    ["verificationAdmissionJson", "verificationAdmissionDigest"],
    ["verificationIdentityJson", "verificationIdentityDigest"],
    ["proposedPlanJson", "proposedPlanDigest"],
  ] as const) {
    const json = canonicalJson({
      ...JSON.parse(authority[jsonField]),
      excludedInternalMetadata: "coherently changed",
    });
    const altered = { ...authority, [jsonField]: json, [digestField]: sha256Utf8(json) };
    const rendered = buildExpectedAgentControlVerificationHandoff(altered);
    assert.notEqual(rendered.promptDigest, accepted.promptDigest);
    assert.notEqual(verificationHandoffAuthorityMismatch(altered, accepted), null, jsonField);
  }
});
