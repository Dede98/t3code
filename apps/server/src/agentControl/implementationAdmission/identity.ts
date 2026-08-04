import { CommandId, EventId } from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

export const fingerprintImplementationAdmission = (domain: string, parts: ReadonlyArray<string>) =>
  NodeCrypto.createHash("sha256")
    .update(frame([`agent-control-implementation-admission-${domain}-v1`, ...parts]), "utf8")
    .digest("hex");

const identity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${fingerprintImplementationAdmission(domain, parts)}`;

export const deriveImplementationAdmissionCommandId = (
  handoffId: string,
  resultEvidenceId: string,
) => CommandId.make(identity("implementation-admission", "command", [handoffId, resultEvidenceId]));

export const deriveImplementationAdmissionEvidenceId = (
  handoffId: string,
  resultEvidenceId: string,
) => identity("implementation-admission-evidence", "evidence", [handoffId, resultEvidenceId]);

export const deriveImplementationAdmissionReceiptId = (
  handoffId: string,
  resultEvidenceId: string,
) => identity("implementation-admission-receipt", "receipt", [handoffId, resultEvidenceId]);

export const deriveImplementationAdmissionMarkerId = (
  handoffId: string,
  resultEvidenceId: string,
) => identity("implementation-admission-marker", "marker", [handoffId, resultEvidenceId]);

export const deriveImplementationStagePreparedEventId = (
  handoffId: string,
  resultEvidenceId: string,
) =>
  EventId.make(
    identity("implementation-stage-prepared", "stage-event", [handoffId, resultEvidenceId]),
  );

export const deriveImplementationLeaseReservedEventId = (
  handoffId: string,
  resultEvidenceId: string,
) =>
  EventId.make(
    identity("implementation-lease-reserved", "lease-event", [handoffId, resultEvidenceId]),
  );

export const deriveImplementationReservationPreparedEventId = (
  handoffId: string,
  resultEvidenceId: string,
) =>
  EventId.make(
    identity("implementation-reservation-prepared", "reservation-event", [
      handoffId,
      resultEvidenceId,
    ]),
  );
