import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";

import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import { canonicalProviderModelSelectionEvidence } from "../../provider/Services/ProviderAdapter.ts";
import { ProviderTurnDeliveryError } from "../Services/ProviderTurnRequestExecutor.ts";
import {
  buildInitialPlanningSessionEvidence,
  isInitialPlanningSessionEvidenceRow,
  mapProviderTurnDeliveryCause,
} from "./ProviderTurnRequestExecutor.ts";

class SemanticAnnotation extends Context.Service<SemanticAnnotation, { readonly value: string }>()(
  "t3/orchestration/Layers/ProviderTurnRequestExecutor.test/SemanticAnnotation",
) {}

function assertCauseAnnotationsEqual(
  actual: Cause.Reason<unknown>,
  expected: Cause.Reason<unknown>,
): void {
  const actualAnnotations = new Map(actual.annotations);
  const expectedAnnotations = new Map(expected.annotations);
  actualAnnotations.delete(Cause.StackTrace.key);
  expectedAnnotations.delete(Cause.StackTrace.key);
  assert.deepStrictEqual(actualAnnotations, expectedAnnotations);
}

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

describe("Initial Planning delivery Cause mapping", () => {
  it("maps every Failure while preserving a combined Defect and Interrupt", () => {
    const providerFailure = new ProviderAdapterRequestError({
      provider: "cursor",
      method: "session/prompt",
      detail: "combined executor failure",
    });
    const defect = new Error("combined executor defect");
    const annotations = Context.make(SemanticAnnotation, { value: "preserved" });
    const failureReason = Cause.makeFailReason(providerFailure).annotate(annotations);
    const defectReason = Cause.makeDieReason(defect).annotate(annotations);
    const interruptReason = Cause.makeInterruptReason(47_002).annotate(annotations);
    const cause = Cause.fromReasons([failureReason, defectReason, interruptReason]);

    const mapped = mapProviderTurnDeliveryCause(cause, "not-attempted");

    assert.equal(mapped.reasons.length, 3);
    const mappedFailure = mapped.reasons[0]!;
    assert.isTrue(Cause.isFailReason(mappedFailure));
    if (Cause.isFailReason(mappedFailure)) {
      assert.instanceOf(mappedFailure.error, ProviderTurnDeliveryError);
      assert.strictEqual(mappedFailure.error.cause, providerFailure);
      assert.equal(mappedFailure.error.certainty, "not-attempted");
      assertCauseAnnotationsEqual(mappedFailure, failureReason);
    }
    const mappedDefect = mapped.reasons[1]!;
    const mappedInterrupt = mapped.reasons[2]!;
    assert.strictEqual(mappedDefect, defectReason);
    assert.strictEqual(mappedInterrupt, interruptReason);
    assert.isTrue(Cause.isDieReason(mappedDefect));
    if (Cause.isDieReason(mappedDefect)) {
      assert.strictEqual(mappedDefect.defect, defect);
    }
    assert.isTrue(Cause.isInterruptReason(mappedInterrupt));
    if (Cause.isInterruptReason(mappedInterrupt)) {
      assert.equal(mappedInterrupt.fiberId, 47_002);
    }
  });

  it("preserves individual Failure, Defect, and Interrupt paths", () => {
    const providerFailure = new ProviderAdapterRequestError({
      provider: "cursor",
      method: "session/prompt",
      detail: "individual executor failure",
    });
    const failure = mapProviderTurnDeliveryCause(Cause.fail(providerFailure), "acceptance-unknown");
    const failureReason = failure.reasons[0]!;
    assert.isTrue(Cause.isFailReason(failureReason));
    if (Cause.isFailReason(failureReason)) {
      assert.instanceOf(failureReason.error, ProviderTurnDeliveryError);
      assert.strictEqual(failureReason.error.cause, providerFailure);
      assert.equal(failureReason.error.certainty, "acceptance-unknown");
    }

    const defectReason = Cause.makeDieReason(new Error("individual executor defect"));
    const defect = mapProviderTurnDeliveryCause(Cause.fromReasons([defectReason]), "not-attempted");
    assert.strictEqual(defect.reasons[0], defectReason);

    const interruptReason = Cause.makeInterruptReason(47_001);
    const interrupt = mapProviderTurnDeliveryCause(
      Cause.fromReasons([interruptReason]),
      "not-attempted",
    );
    const mappedInterrupt = interrupt.reasons[0]!;
    assert.strictEqual(mappedInterrupt, interruptReason);
    assert.isTrue(Cause.isInterruptReason(mappedInterrupt));
    if (Cause.isInterruptReason(mappedInterrupt)) {
      assert.equal(mappedInterrupt.fiberId, 47_001);
    }
  });
});
