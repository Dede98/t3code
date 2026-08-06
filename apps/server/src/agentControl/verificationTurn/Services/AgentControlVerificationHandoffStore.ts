import type { CommandId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type {
  AgentControlVerificationClaim,
  AgentControlVerificationDelivery,
  AgentControlVerificationHandoffEvidence,
} from "../model.ts";
import type { AgentControlVerificationHandoffAuthority } from "../handoffValidation.ts";

export class AgentControlVerificationStoreError extends Schema.TaggedErrorClass<AgentControlVerificationStoreError>()(
  "AgentControlVerificationStoreError",
  {
    operation: Schema.String,
    reason: Schema.Literals(["candidate-evidence", "persistence", "revision-conflict"]),
    handoffId: Schema.optional(Schema.String),
    candidateReason: Schema.optional(
      Schema.Literals([
        "base-candidate-missing",
        "companion-missing",
        "companion-ambiguous",
        "projection-missing",
        "projection-divergent",
        "history-missing",
        "history-divergent",
        "evidence-undecodable",
        "evidence-divergent",
      ]),
    ),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type AgentControlVerificationCandidateEvidenceReason = NonNullable<
  AgentControlVerificationStoreError["candidateReason"]
>;

export const makeAgentControlVerificationCandidateEvidenceError = (input: {
  readonly handoffId: string;
  readonly candidateReason: AgentControlVerificationCandidateEvidenceReason;
  readonly operation: string;
  readonly cause?: unknown;
}) =>
  new AgentControlVerificationStoreError({
    operation: input.operation,
    reason: "candidate-evidence",
    handoffId: input.handoffId,
    candidateReason: input.candidateReason,
    ...(input.cause === undefined ? {} : { cause: input.cause }),
  });

const isVerificationStoreError = Schema.is(AgentControlVerificationStoreError);

export const isAgentControlVerificationCandidateEvidenceError = (
  error: unknown,
): error is AgentControlVerificationStoreError & { readonly reason: "candidate-evidence" } =>
  isVerificationStoreError(error) && error.reason === "candidate-evidence";

export interface AgentControlVerificationTurnAcceptance {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: ThreadId;
  readonly planningThreadId: ThreadId;
  readonly planId: string;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: string;
  readonly messageEventId: string;
  readonly messageEventSequence: number;
  readonly turnRequestEventId: string;
  readonly turnRequestEventSequence: number;
  readonly messageEventEnvelopeJson: string;
  readonly turnRequestEventEnvelopeJson: string;
  readonly eventEvidenceDigest: string;
  readonly acceptedAt: string;
}

export interface AgentControlVerificationHandoffStoreShape {
  readonly insertAcceptedInTransaction: (
    evidence: AgentControlVerificationHandoffEvidence,
    authority: AgentControlVerificationHandoffAuthority,
  ) => Effect.Effect<void, AgentControlVerificationStoreError>;
  readonly loadAcceptedByHandoffId: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlVerificationClaim>,
    AgentControlVerificationStoreError
  >;
  readonly loadAcceptedByTurnRequestCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<
    Option.Option<AgentControlVerificationClaim>,
    AgentControlVerificationStoreError
  >;
  readonly loadAcceptedByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Option.Option<AgentControlVerificationClaim>,
    AgentControlVerificationStoreError
  >;
  readonly listRecoverable: (
    now: string,
    afterExclusive?: string,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<string>, AgentControlVerificationStoreError>;
  readonly isHandoffOwnedTurnRequest: (
    commandId: CommandId,
  ) => Effect.Effect<boolean, AgentControlVerificationStoreError>;
  readonly loadTurnAcceptance: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlVerificationTurnAcceptance>,
    AgentControlVerificationStoreError
  >;
  readonly markTurnAccepted: (
    handoffId: string,
    expectedRevision: number,
    at: string,
  ) => Effect.Effect<AgentControlVerificationDelivery, AgentControlVerificationStoreError>;
  readonly claim: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly now: string;
    readonly expiresAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlVerificationClaim>,
    AgentControlVerificationStoreError
  >;
  readonly markDeliveryAttempted: (input: {
    readonly providerDeliveryId: string;
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly attemptedAt: string;
    readonly providerSessionCreatedAt: string;
    readonly providerResumeCursorJson: string;
    readonly providerInstanceId: string;
    readonly turnModelSelectionJson: string;
    readonly turnModelSelectionFingerprint: string;
  }) => Effect.Effect<AgentControlVerificationDelivery, AgentControlVerificationStoreError>;
  readonly markProviderStarted: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<AgentControlVerificationDelivery, AgentControlVerificationStoreError>;
  readonly scheduleRetry: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly nextAttemptAt: string;
    readonly errorCode: string;
    readonly updatedAt: string;
  }) => Effect.Effect<AgentControlVerificationDelivery, AgentControlVerificationStoreError>;
  readonly markAmbiguous: (input: {
    readonly handoffId: string;
    readonly expectedRevision: number;
    readonly terminalAt: string;
  }) => Effect.Effect<AgentControlVerificationDelivery, AgentControlVerificationStoreError>;
  readonly observeProviderStarted: (input: {
    readonly threadId: ThreadId;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlVerificationDelivery>,
    AgentControlVerificationStoreError
  >;
  readonly listStageStartCandidates: (
    afterExclusive?: string,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<string>, AgentControlVerificationStoreError>;
}

export class AgentControlVerificationHandoffStore extends Context.Service<
  AgentControlVerificationHandoffStore,
  AgentControlVerificationHandoffStoreShape
>()("t3/agentControl/verificationTurn/Services/AgentControlVerificationHandoffStore") {}
