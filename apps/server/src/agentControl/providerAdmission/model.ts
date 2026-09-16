import type { ModelSelection, ProviderInstanceId } from "@t3tools/contracts";

import { canonicalJson, sha256Utf8, type JsonValue } from "../initialPlanning/eventEvidence.ts";

export type ProviderAdmissionStage = "initial-planning" | "implementation" | "verification";

export type ProviderResourceAdmissionClass = "interactive" | "background";
export type ProviderResourceAdmissionWaitReason =
  | "provider-limit"
  | "provider-usage"
  | "provider-recovery"
  | "interactive-priority";

export interface ProviderResourceAdmissionLimits {
  readonly maxConcurrent: number;
  readonly interactiveReserve: number;
  readonly backgroundAgingMs: number;
  readonly maxInteractiveBurst: number;
}

export const DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS: ProviderResourceAdmissionLimits = {
  maxConcurrent: 4,
  interactiveReserve: 1,
  backgroundAgingMs: 30_000,
  maxInteractiveBurst: 3,
};

export interface ProviderResourceAdmissionRequest {
  /** Stable per logical turn. Replays with different contents fail closed. */
  readonly idempotencyKey: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: string;
  /** Stable credential/account identity. Callers must not derive this from a display instance name. */
  readonly accountScope: string;
  readonly workloadClass: ProviderResourceAdmissionClass;
  readonly source: "manual" | "automatic";
  readonly requestedAt: string;
  readonly stage?: ProviderAdmissionStage;
  readonly handoffId?: string;
}

export interface ProviderResourceAdmissionPermit {
  readonly requestId: string;
  readonly idempotencyKey: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: string;
  readonly accountScope: string;
  readonly workloadClass: ProviderResourceAdmissionClass;
  readonly source: "manual" | "automatic";
  readonly requestedAt: string;
  readonly stage: ProviderAdmissionStage | null;
  readonly handoffId: string | null;
  readonly ownerId: string;
  readonly leaseExpiresAt: string;
  readonly fenceToken: number;
}

export interface ProviderResourceAdmissionActive {
  readonly requestId: string;
  readonly idempotencyKey: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly threadId: string;
  readonly providerTurnId: string | null;
  readonly accountScope: string;
  readonly workloadClass: ProviderResourceAdmissionClass;
  readonly source: "manual" | "automatic";
  readonly stage: ProviderAdmissionStage | null;
  readonly handoffId: string | null;
  readonly status: "waiting" | "admitted" | "entered";
  readonly waitReason: ProviderResourceAdmissionWaitReason | null;
  readonly requestedAt: string;
  readonly lastObservedActivity: "active" | "inactive" | "unknown" | null;
  readonly lastObservedAt: string | null;
  readonly permit: ProviderResourceAdmissionPermit | null;
}

export type ProviderResourceAdmissionDecision =
  | {
      readonly _tag: "Waiting";
      readonly requestId: string;
      readonly reason: ProviderResourceAdmissionWaitReason;
      readonly retryAt: string | null;
    }
  | { readonly _tag: "Admitted"; readonly permit: ProviderResourceAdmissionPermit }
  | { readonly _tag: "Cancelled"; readonly requestId: string };

export const providerResourceAdmissionRequestId = (
  request: ProviderResourceAdmissionRequest,
): string =>
  `provider-resource-admission:${sha256Utf8(
    canonicalJson([
      request.idempotencyKey,
      String(request.providerInstanceId),
      request.accountScope,
    ]),
  )}`;

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

export const automaticProviderResourceAdmissionRequest = (
  request: ProviderAdmissionRequest,
  accountScope: string,
): ProviderResourceAdmissionRequest => ({
  idempotencyKey: `automatic:${request.providerDeliveryId}`,
  providerInstanceId: request.providerInstanceId,
  threadId: request.threadId,
  accountScope,
  workloadClass: "background",
  source: "automatic",
  requestedAt: request.requestedAt,
  stage: request.stage,
  handoffId: request.handoffId,
});

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
