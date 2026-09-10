import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import type {
  ProviderAdmissionDecision,
  ProviderAdmissionPermit,
  ProviderAdmissionRequest,
  ProviderAdmissionStage,
  ProviderAdmissionUsageEvidence,
} from "../model.ts";

export class ProviderAdmissionError extends Schema.TaggedError<ProviderAdmissionError>()(
  "ProviderAdmissionError",
  {
    operation: Schema.String,
    reason: Schema.Literals([
      "authority-missing",
      "authority-divergent",
      "capacity-busy",
      "usage-ineligible",
      "stale-owner",
      "project-inactive",
      "persistence",
    ]),
    admissionId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface ProviderAdmissionWakeup {
  readonly stage: ProviderAdmissionStage;
  readonly handoffId: string;
  readonly providerInstanceId: string;
}

export interface ProviderAdmissionDeadlineWakeup extends ProviderAdmissionWakeup {
  readonly admissionId: string;
  readonly deadlineAt: string;
  readonly deadlineKind: "usage" | "lease";
}

export interface ProviderAdmissionStoreShape {
  readonly resume: (input: {
    readonly request: ProviderAdmissionRequest;
    readonly ownerId: string;
    readonly leaseExpiresAt: string;
    readonly now: string;
  }) => Effect.Effect<ProviderAdmissionDecision | null, ProviderAdmissionError>;
  readonly request: (input: {
    readonly request: ProviderAdmissionRequest;
    readonly usage: ProviderAdmissionUsageEvidence;
    readonly ownerId: string;
    readonly leaseExpiresAt: string;
    readonly now: string;
  }) => Effect.Effect<ProviderAdmissionDecision, ProviderAdmissionError>;
  readonly validateAndEnterInTransaction: (input: {
    readonly permit: ProviderAdmissionPermit;
    readonly boundary: "session-start" | "turn-start" | "verification-check";
    readonly enteredAt: string;
  }) => Effect.Effect<void, ProviderAdmissionError>;
  readonly admitOldest: (input: {
    readonly providerInstanceId: string;
    readonly ownerId: string;
    readonly leaseExpiresAt: string;
    readonly now: string;
  }) => Effect.Effect<ProviderAdmissionPermit | null, ProviderAdmissionError>;
  readonly quarantine: (input: {
    readonly permit: ProviderAdmissionPermit;
    readonly reason: "external-outcome-unknown" | "owner-lost-after-entry";
    readonly observedAt: string;
  }) => Effect.Effect<void, ProviderAdmissionError>;
  readonly quarantineIfEntered: (input: {
    readonly permit: ProviderAdmissionPermit;
    readonly observedAt: string;
  }) => Effect.Effect<void, ProviderAdmissionError>;
  readonly recordUsage: (
    providerInstanceId: string,
    evidence: ProviderAdmissionUsageEvidence,
  ) => Effect.Effect<ReadonlyArray<ProviderAdmissionWakeup>, ProviderAdmissionError>;
  readonly listWaiting: Effect.Effect<
    ReadonlyArray<ProviderAdmissionWakeup>,
    ProviderAdmissionError
  >;
  readonly listDueDeadlines: (
    now: string,
  ) => Effect.Effect<ReadonlyArray<ProviderAdmissionDeadlineWakeup>, ProviderAdmissionError>;
  readonly listEnteredWithoutRelease: Effect.Effect<
    ReadonlyArray<ProviderAdmissionPermit>,
    ProviderAdmissionError
  >;
  readonly minimumDeadline: Effect.Effect<string | null, ProviderAdmissionError>;
  readonly minimumDeadlineAfter: (
    after: string,
  ) => Effect.Effect<string | null, ProviderAdmissionError>;
  readonly releaseFromFinalizationInTransaction: (input: {
    readonly stage: ProviderAdmissionStage;
    readonly handoffId: string;
    readonly finalizedAt: string;
  }) => Effect.Effect<string | null, ProviderAdmissionError>;
  readonly catchUpFinalized: Effect.Effect<ReadonlyArray<string>, ProviderAdmissionError>;
}

export class ProviderAdmissionStore extends Context.Service<
  ProviderAdmissionStore,
  ProviderAdmissionStoreShape
>()("t3/agentControl/providerAdmission/Services/ProviderAdmissionStore") {}
