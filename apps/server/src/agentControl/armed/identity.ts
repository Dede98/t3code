import {
  AgentControlArmedClaimId,
  AgentControlArmedDispatchId,
  AgentControlArmedEvidenceId,
  AgentControlArmedMarkerId,
  AgentControlArmedReceiptId,
  CommandId,
  type ProjectId,
} from "@t3tools/contracts";

import { sha256Utf8 } from "../initialPlanning/eventEvidence.ts";

const frame = (parts: ReadonlyArray<string>) =>
  parts.map((part) => `${Buffer.byteLength(part, "utf8")}:${part}`).join("");

const identity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${sha256Utf8(frame([`agent-control-armed-${domain}-v1`, ...parts]))}`;

export interface ArmedIdentityEpoch {
  readonly projectId: ProjectId;
  readonly projectRevision: number;
  readonly githubIntakeSequence: number;
  readonly githubEventId: string;
  readonly githubEventSequence: number;
  readonly githubEventStreamVersion: number;
  readonly sourceFingerprint: string;
  readonly reconcileRevision: number;
  readonly taskFrontierSequence: number;
  readonly taskFrontierRevision: number;
  readonly taskFrontierCount: number;
  readonly taskFrontierFingerprint: string;
}

const epochParts = (epoch: ArmedIdentityEpoch) => [
  epoch.projectId,
  String(epoch.projectRevision),
  String(epoch.githubIntakeSequence),
  epoch.githubEventId,
  String(epoch.githubEventSequence),
  String(epoch.githubEventStreamVersion),
  epoch.sourceFingerprint,
  String(epoch.reconcileRevision),
  String(epoch.taskFrontierSequence),
  String(epoch.taskFrontierRevision),
  String(epoch.taskFrontierCount),
  epoch.taskFrontierFingerprint,
];

// A no-candidate watermark belongs to the observed source/reconcile/task
// frontier, not to a particular visit to Armed. Human leave/resume commands
// advance the project revision without changing that frontier and must replay
// the same durable decision instead of attempting a second publication.
const noCandidateEpochParts = (epoch: ArmedIdentityEpoch) => [
  epoch.projectId,
  String(epoch.githubIntakeSequence),
  epoch.githubEventId,
  String(epoch.githubEventSequence),
  String(epoch.githubEventStreamVersion),
  epoch.sourceFingerprint,
  String(epoch.reconcileRevision),
  String(epoch.taskFrontierSequence),
  String(epoch.taskFrontierRevision),
  String(epoch.taskFrontierCount),
  epoch.taskFrontierFingerprint,
];

export const deriveArmedDispatchId = (
  epoch: ArmedIdentityEpoch,
  taskId: string,
  fenceToken: number,
) =>
  AgentControlArmedDispatchId.make(
    identity("armed-dispatch", "dispatch", [...epochParts(epoch), taskId, String(fenceToken)]),
  );

export const deriveArmedClaimId = (dispatchId: string) =>
  AgentControlArmedClaimId.make(identity("armed-claim", "claim", [dispatchId]));

export const deriveArmedEvidenceId = (dispatchId: string) =>
  AgentControlArmedEvidenceId.make(identity("armed-evidence", "evidence", [dispatchId]));

export const deriveArmedReceiptId = (dispatchId: string) =>
  AgentControlArmedReceiptId.make(identity("armed-receipt", "receipt", [dispatchId]));

export const deriveArmedMarkerId = (dispatchId: string) =>
  AgentControlArmedMarkerId.make(identity("armed-marker", "marker", [dispatchId]));

export const deriveArmedModeCommandId = (dispatchId: string) =>
  CommandId.make(identity("armed-mode-command", "mode-command", [dispatchId]));

export const deriveArmedNoCandidateIdentity = (
  kind: "evidence" | "receipt" | "marker",
  epoch: ArmedIdentityEpoch,
) => identity(`armed-no-candidate-${kind}`, `no-candidate-${kind}`, noCandidateEpochParts(epoch));
