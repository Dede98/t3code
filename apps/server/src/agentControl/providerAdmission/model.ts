import type { ModelSelection, ProviderInstanceId } from "@t3tools/contracts";

import { canonicalJson, sha256Utf8, type JsonValue } from "../initialPlanning/eventEvidence.ts";

export type ProviderAdmissionStage = "initial-planning" | "implementation" | "verification";

export type ProviderAdmissionUsageStatus =
  | "allowed"
  | "warning"
  | "rejected"
  | "unsupported"
  | "supported-unusable";

export interface ProviderAdmissionUsageEvidence {
  readonly status: ProviderAdmissionUsageStatus;
  readonly observedAt: string;
  readonly source: "refresh" | "runtime-event" | "capability" | "refresh-error";
  readonly nextRelevantAt: string | null;
  readonly fingerprint: string;
}

export interface ProviderAdmissionRequest {
  readonly stage: ProviderAdmissionStage;
  readonly projectId: string;
  readonly taskId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly handoffId: string;
  readonly providerDeliveryId: string;
  readonly threadId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly stageLeaseId: string;
  readonly stageLeaseHolderId: string;
  readonly stageFenceToken: number;
  readonly modelSelection: ModelSelection;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly requestedAt: string;
}

export interface ProviderAdmissionPermit {
  readonly admissionId: string;
  readonly admissionMarkerId: string;
  readonly admissionMarkerFingerprint: string;
  readonly stage: ProviderAdmissionStage;
  readonly projectId: string;
  readonly taskId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly handoffId: string;
  readonly providerDeliveryId: string;
  readonly threadId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly stageLeaseId: string;
  readonly stageLeaseHolderId: string;
  readonly stageFenceToken: number;
  readonly admissionOwnerId: string;
  readonly admissionLeaseExpiresAt: string;
  readonly providerFenceToken: number;
  readonly modelSelectionJson: string;
  readonly modelSelectionFingerprint: string;
  readonly usageEvidenceFingerprint: string;
}

export type ProviderAdmissionDecision =
  | { readonly _tag: "Waiting"; readonly admissionId: string; readonly retryAt: string | null }
  | { readonly _tag: "Admitted"; readonly permit: ProviderAdmissionPermit };

export interface ProviderAdmissionBoundary {
  readonly kind: "session-start" | "turn-start";
  readonly enteredAt: string;
}

export const providerAdmissionId = (request: ProviderAdmissionRequest): string =>
  `provider-admission:${sha256Utf8(
    canonicalJson([
      request.stage,
      request.projectId,
      request.taskId,
      request.stageRunId,
      request.attemptId,
      request.handoffId,
      request.providerDeliveryId,
      request.threadId,
      request.providerInstanceId,
      request.modelSelectionFingerprint,
    ]),
  )}`;

export const providerAdmissionAuthorityId = (
  kind: "admission" | "session-entry" | "turn-entry" | "quarantine" | "supersede" | "release",
  admissionId: string,
  discriminator: string,
): string => `provider-${kind}:${sha256Utf8(canonicalJson([admissionId, discriminator]))}`;

export const fingerprintProviderAdmissionDocument = (document: JsonValue): string =>
  sha256Utf8(canonicalJson(document));

export const providerAdmissionUsageEvidence = (input: {
  readonly providerInstanceId: ProviderInstanceId;
  readonly status: ProviderAdmissionUsageStatus;
  readonly observedAt: string;
  readonly source: ProviderAdmissionUsageEvidence["source"];
  readonly nextRelevantAt: string | null;
}): ProviderAdmissionUsageEvidence => {
  const document = {
    nextRelevantAt: input.nextRelevantAt,
    observedAt: input.observedAt,
    providerInstanceId: String(input.providerInstanceId),
    schemaVersion: 1,
    source: input.source,
    status: input.status,
  } as const;
  return { ...input, fingerprint: fingerprintProviderAdmissionDocument(document) };
};

export const usageAllowsAdmission = (status: ProviderAdmissionUsageStatus): boolean =>
  status === "allowed" || status === "warning" || status === "unsupported";
