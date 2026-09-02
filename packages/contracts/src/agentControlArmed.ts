/** Schema-only server-internal contracts for durable Armed single-flight. */
import * as Schema from "effect/Schema";

import {
  AgentControlTaskId,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const AgentControlArmedDispatchId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlArmedDispatchId"),
);
export type AgentControlArmedDispatchId = typeof AgentControlArmedDispatchId.Type;

export const AgentControlArmedClaimId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlArmedClaimId"),
);
export type AgentControlArmedClaimId = typeof AgentControlArmedClaimId.Type;

export const AgentControlArmedEvidenceId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlArmedEvidenceId"),
);
export type AgentControlArmedEvidenceId = typeof AgentControlArmedEvidenceId.Type;

export const AgentControlArmedReceiptId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlArmedReceiptId"),
);
export type AgentControlArmedReceiptId = typeof AgentControlArmedReceiptId.Type;

export const AgentControlArmedMarkerId = TrimmedNonEmptyString.pipe(
  Schema.brand("AgentControlArmedMarkerId"),
);
export type AgentControlArmedMarkerId = typeof AgentControlArmedMarkerId.Type;

export const AgentControlArmedEpoch = Schema.Struct({
  githubIntakeSequence: PositiveInt,
  githubEventId: EventId,
  githubEventSequence: PositiveInt,
  githubEventStreamVersion: PositiveInt,
  sourceFingerprint: TrimmedNonEmptyString,
  reconcileRevision: PositiveInt,
  taskFrontierSequence: NonNegativeInt,
  taskFrontierRevision: NonNegativeInt,
  taskFrontierCount: NonNegativeInt,
  taskFrontierFingerprint: TrimmedNonEmptyString,
});
export type AgentControlArmedEpoch = typeof AgentControlArmedEpoch.Type;

export const AgentControlArmedDispatch = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  dispatchId: AgentControlArmedDispatchId,
  claimId: AgentControlArmedClaimId,
  evidenceId: AgentControlArmedEvidenceId,
  receiptId: AgentControlArmedReceiptId,
  markerId: AgentControlArmedMarkerId,
  commandId: CommandId,
  projectId: ProjectId,
  selectedTaskId: AgentControlTaskId,
  projectRevision: PositiveInt,
  projectEventSequence: PositiveInt,
  epoch: AgentControlArmedEpoch,
  ownerId: TrimmedNonEmptyString,
  fenceToken: PositiveInt,
  claimedAt: IsoDateTime,
  expiresAt: IsoDateTime,
});
export type AgentControlArmedDispatch = typeof AgentControlArmedDispatch.Type;

export const AgentControlArmedNoCandidateDecision = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  evidenceId: AgentControlArmedEvidenceId,
  receiptId: AgentControlArmedReceiptId,
  markerId: AgentControlArmedMarkerId,
  projectId: ProjectId,
  projectRevision: PositiveInt,
  projectEventSequence: PositiveInt,
  epoch: AgentControlArmedEpoch,
  decidedAt: IsoDateTime,
});
export type AgentControlArmedNoCandidateDecision = typeof AgentControlArmedNoCandidateDecision.Type;

const AgentControlArmedDispatchStateFields = {
  schemaVersion: Schema.Literal(1),
  dispatchId: AgentControlArmedDispatchId,
  projectId: ProjectId,
  ownerId: TrimmedNonEmptyString,
  fenceToken: PositiveInt,
  expiresAt: IsoDateTime,
  updatedAt: IsoDateTime,
} as const;
const AgentControlArmedDispatchStateWithoutActivation = Schema.Struct({
  ...AgentControlArmedDispatchStateFields,
  status: Schema.Literals(["claimed", "superseded"]),
  activationEventId: Schema.Null,
  activationEventSequence: Schema.Null,
  activationEventStreamVersion: Schema.Null,
});
const AgentControlArmedDispatchStateWithActivation = Schema.Struct({
  ...AgentControlArmedDispatchStateFields,
  status: Schema.Literals(["activated", "completed", "superseded"]),
  activationEventId: EventId,
  activationEventSequence: PositiveInt,
  activationEventStreamVersion: PositiveInt,
});
export const AgentControlArmedDispatchState = Schema.Union([
  AgentControlArmedDispatchStateWithoutActivation,
  AgentControlArmedDispatchStateWithActivation,
]);
export type AgentControlArmedDispatchState = typeof AgentControlArmedDispatchState.Type;

export const AgentControlArmedWakeup = Schema.Struct({
  projectId: ProjectId,
  reason: Schema.Literals(["project", "task", "reconcile", "recovery"]),
});
export type AgentControlArmedWakeup = typeof AgentControlArmedWakeup.Type;
