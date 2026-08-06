import { CommandId, EventId } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const fingerprintVerificationAdmission = (
  domain: string,
  parts: ReadonlyArray<string>,
): string =>
  NodeCrypto.createHash("sha256")
    .update(frame([`agent-control-verification-admission-${domain}-v1`, ...parts]), "utf8")
    .digest("hex");

const identity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${fingerprintVerificationAdmission(domain, parts)}`;

export interface VerificationAdmissionPredecessorIdentity {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly finalizationFingerprint: string;
}

const predecessorParts = (input: VerificationAdmissionPredecessorIdentity) => [
  input.handoffId,
  input.resultEvidenceId,
  input.finalizationFingerprint,
];

export const deriveVerificationAdmissionCommandId = (
  input: VerificationAdmissionPredecessorIdentity,
) => CommandId.make(identity("verification-admission", "command", predecessorParts(input)));

export const deriveVerificationAdmissionEvidenceId = (
  input: VerificationAdmissionPredecessorIdentity,
) => identity("verification-admission-evidence", "evidence", predecessorParts(input));

export const deriveVerificationAdmissionReceiptId = (
  input: VerificationAdmissionPredecessorIdentity,
) => identity("verification-admission-receipt", "receipt", predecessorParts(input));

export const deriveVerificationAdmissionMarkerId = (
  input: VerificationAdmissionPredecessorIdentity,
) => identity("verification-admission-marker", "marker", predecessorParts(input));

export const deriveVerificationStagePreparedEventId = (
  input: VerificationAdmissionPredecessorIdentity,
) => EventId.make(identity("verification-stage-prepared", "stage-event", predecessorParts(input)));

export const deriveVerificationLeaseReservedEventId = (
  input: VerificationAdmissionPredecessorIdentity,
) => EventId.make(identity("verification-lease-reserved", "lease-event", predecessorParts(input)));

export const deriveVerificationReservationPreparedEventId = (
  input: VerificationAdmissionPredecessorIdentity,
) =>
  EventId.make(
    identity("verification-reservation-prepared", "reservation-event", predecessorParts(input)),
  );
