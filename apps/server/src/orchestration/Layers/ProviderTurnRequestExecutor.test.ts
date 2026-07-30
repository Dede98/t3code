import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";

import { canonicalProviderModelSelectionEvidence } from "../../provider/Services/ProviderAdapter.ts";
import {
  buildInitialPlanningSessionEvidence,
  isInitialPlanningSessionEvidenceRow,
} from "./ProviderTurnRequestExecutor.ts";

const selection = {
  instanceId: ProviderInstanceId.make("codex-primary"),
  model: "gpt-5.4",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "fastMode", value: false },
    { id: "thinking", value: "adaptive" },
  ],
} as const;
const modelEvidence = canonicalProviderModelSelectionEvidence(selection);
const attestation = {
  threadId: ThreadId.make("thread-session-evidence"),
  providerInstanceId: selection.instanceId,
  runtimeMode: "approval-required" as const,
  cwd: "/tmp/attested-worktree",
  ...modelEvidence,
  sessionCreatedAt: "2026-07-30T12:00:00.000Z",
  resumeCursor: { threadId: "provider-thread" },
};

describe("Initial Planning session evidence", () => {
  it("accepts only the complete attested row and recomputed ModelSelection fingerprint", () => {
    const expected = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation,
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    assert.isTrue(isInitialPlanningSessionEvidenceRow(expected, expected));

    for (const [field, value] of [
      ["providerDeliveryId", "different-delivery"],
      ["threadId", "different-thread"],
      ["providerInstanceId", "different-provider"],
      ["runtimeMode", "full-access"],
      ["cwd", "/tmp/different-worktree"],
      ["modelSelectionJson", '{"instanceId":"codex-primary","model":"different"}'],
      ["modelSelectionFingerprint", "f".repeat(64)],
      ["sessionCreatedAt", "2026-07-30T12:00:00.001Z"],
      ["resumeCursorJson", '{"threadId":"different-provider-thread"}'],
    ] as const) {
      assert.isFalse(
        isInitialPlanningSessionEvidenceRow({ ...expected, [field]: value }, expected),
        field,
      );
    }
  });

  it("normalizes key and option order but changes evidence for every effective option", () => {
    const reordered = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation: {
        ...attestation,
        ...canonicalProviderModelSelectionEvidence({
          model: selection.model,
          options: [
            { value: "adaptive", id: "thinking" },
            { value: false, id: "fastMode" },
            { value: "high", id: "reasoningEffort" },
          ],
          instanceId: selection.instanceId,
        }),
      },
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    const expected = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation,
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    assert.deepStrictEqual(reordered, expected);

    for (const effectiveModelSelection of [
      {
        ...selection,
        options: [
          { id: "reasoningEffort", value: "xhigh" },
          { id: "fastMode", value: false },
          { id: "thinking", value: "adaptive" },
        ],
      },
      {
        ...selection,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
          { id: "thinking", value: "adaptive" },
        ],
      },
      {
        ...selection,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: false },
          { id: "thinking", value: "disabled" },
        ],
      },
    ] as const) {
      const changed = buildInitialPlanningSessionEvidence({
        providerDeliveryId: "delivery-session-evidence",
        attestation: {
          ...attestation,
          ...canonicalProviderModelSelectionEvidence(effectiveModelSelection),
        },
        resumeCursorJson: '{"threadId":"provider-thread"}',
      });
      assert.notEqual(changed.modelSelectionFingerprint, expected.modelSelectionFingerprint);
      assert.isFalse(isInitialPlanningSessionEvidenceRow(changed, expected));
    }
  });
});
