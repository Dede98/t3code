import * as NodeCrypto from "node:crypto";

import {
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderSession,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";

import {
  attestProviderSessionNativeConfiguration,
  canonicalProviderModelSelectionEvidence,
} from "./ProviderAdapter.ts";

const session = {
  provider: ProviderDriverKind.make("codex"),
  providerInstanceId: ProviderInstanceId.make("codex-primary"),
  threadId: ThreadId.make("thread-attestation"),
  status: "ready",
  runtimeMode: "approval-required",
  cwd: "/tmp/attested-worktree",
  model: "gpt-5.4",
  resumeCursor: { threadId: "provider-thread" },
  createdAt: "2026-07-30T12:00:00.000Z",
  updatedAt: "2026-07-30T12:00:00.000Z",
} satisfies ProviderSession;

describe("provider session model attestation", () => {
  it("binds the complete effective selection and derives its fingerprint from canonical bytes", () => {
    const attested = attestProviderSessionNativeConfiguration(session, {
      instanceId: session.providerInstanceId,
      model: session.model,
      options: [
        { id: "thinking", value: "adaptive" },
        { id: "fastMode", value: true },
        { id: "reasoningEffort", value: "high" },
      ],
    }).initialPlanningAttestation!;
    assert.isNotNull(attested.effectiveModelSelection);
    if (attested.effectiveModelSelection === null) return;

    assert.deepStrictEqual(
      attested.effectiveModelSelection.options?.map((option) => option.id),
      ["fastMode", "reasoningEffort", "thinking"],
    );
    assert.equal(
      attested.modelSelectionFingerprint,
      NodeCrypto.createHash("sha256").update(attested.modelSelectionJson, "utf8").digest("hex"),
    );
    assert.deepStrictEqual(
      canonicalProviderModelSelectionEvidence(attested.effectiveModelSelection),
      {
        effectiveModelSelection: attested.effectiveModelSelection,
        modelSelectionJson: attested.modelSelectionJson,
        modelSelectionFingerprint: attested.modelSelectionFingerprint,
      },
    );
    assert.equal(attested.providerInstanceId, session.providerInstanceId);
    assert.equal(attested.runtimeMode, session.runtimeMode);
    assert.equal(attested.cwd, session.cwd);
    assert.equal(attested.threadId, session.threadId);
    assert.equal(attested.sessionCreatedAt, session.createdAt);
    assert.deepStrictEqual(attested.resumeCursor, session.resumeCursor);
  });

  it("refuses to attest a selection not actually bound by the adapter session", () => {
    for (const selection of [
      {
        instanceId: ProviderInstanceId.make("different-instance"),
        model: session.model,
      },
      {
        instanceId: session.providerInstanceId,
        model: "different-model",
      },
    ]) {
      assert.notProperty(
        attestProviderSessionNativeConfiguration(session, selection),
        "initialPlanningAttestation",
      );
    }
    assert.notProperty(
      attestProviderSessionNativeConfiguration(
        { ...session, cwd: undefined },
        {
          instanceId: session.providerInstanceId,
          model: session.model,
        },
      ),
      "initialPlanningAttestation",
    );
  });

  it("changes the attested bytes for every model option and ignores input key order", () => {
    const base = canonicalProviderModelSelectionEvidence({
      instanceId: session.providerInstanceId,
      model: session.model,
      options: [
        { value: "high", id: "reasoningEffort" },
        { value: false, id: "fastMode" },
      ],
    });
    const reordered = canonicalProviderModelSelectionEvidence({
      model: session.model,
      options: [
        { id: "fastMode", value: false },
        { id: "reasoningEffort", value: "high" },
      ],
      instanceId: session.providerInstanceId,
    });
    assert.deepStrictEqual(reordered, base);
    for (const selection of [
      {
        instanceId: session.providerInstanceId,
        model: session.model,
        options: [
          { id: "reasoningEffort", value: "xhigh" },
          { id: "fastMode", value: false },
        ],
      },
      {
        instanceId: session.providerInstanceId,
        model: session.model,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ],
      },
      {
        instanceId: session.providerInstanceId,
        model: session.model,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "thinking", value: "adaptive" },
          { id: "fastMode", value: false },
        ],
      },
    ]) {
      assert.notEqual(
        canonicalProviderModelSelectionEvidence(selection).modelSelectionFingerprint,
        base.modelSelectionFingerprint,
      );
    }
  });
});
