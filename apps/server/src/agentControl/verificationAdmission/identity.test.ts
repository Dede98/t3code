import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  deriveVerificationAdmissionCommandId,
  deriveVerificationAdmissionEvidenceId,
  deriveVerificationAdmissionMarkerId,
  deriveVerificationAdmissionReceiptId,
  deriveVerificationLeaseReservedEventId,
  deriveVerificationReservationPreparedEventId,
  deriveVerificationStagePreparedEventId,
  fingerprintVerificationAdmission,
} from "./identity.ts";

it.effect("derives deterministic domain-separated Verification Admission identities", () =>
  Effect.sync(() => {
    const predecessor = {
      handoffId: "implementation-handoff-a",
      resultEvidenceId: "implementation-result-b",
      finalizationFingerprint: "c".repeat(64),
    };
    const deriveAll = () => [
      deriveVerificationAdmissionCommandId(predecessor),
      deriveVerificationAdmissionEvidenceId(predecessor),
      deriveVerificationAdmissionReceiptId(predecessor),
      deriveVerificationAdmissionMarkerId(predecessor),
      deriveVerificationStagePreparedEventId(predecessor),
      deriveVerificationLeaseReservedEventId(predecessor),
      deriveVerificationReservationPreparedEventId(predecessor),
    ];
    const identities = deriveAll();
    assert.deepStrictEqual(identities, deriveAll());
    assert.equal(new Set(identities).size, identities.length);
    assert.notEqual(
      deriveVerificationAdmissionEvidenceId(predecessor),
      deriveVerificationAdmissionEvidenceId({
        ...predecessor,
        finalizationFingerprint: "d".repeat(64),
      }),
    );
    assert.notEqual(
      fingerprintVerificationAdmission("framing", ["ab", "c"]),
      fingerprintVerificationAdmission("framing", ["a", "bc"]),
    );
    assert.notEqual(
      fingerprintVerificationAdmission("domain-a", ["same"]),
      fingerprintVerificationAdmission("domain-b", ["same"]),
    );
  }),
);
