import type { CommandId, ThreadId, ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type {
  AgentControlImplementationClaim,
  AgentControlImplementationDelivery,
  AgentControlImplementationHandoffEvidence,
} from "../model.ts";
import type { AgentControlImplementationHandoffAuthority } from "../handoffValidation.ts";

export class AgentControlImplementationStoreError extends Schema.TaggedError<AgentControlImplementationStoreError>()(
  "AgentControlImplementationStoreError",
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

export type AgentControlImplementationCandidateEvidenceReason = NonNullable<
  AgentControlImplementationStoreError["candidateReason"]
>;

export const makeAgentControlImplementationCandidateEvidenceError = (input: {
  readonly handoffId: string;
  readonly candidateReason: AgentControlImplementationCandidateEvidenceReason;
  readonly operation: string;
  readonly cause?: unknown;
}) =>
  new AgentControlImplementationStoreError({
    operation: input.operation,
    reason: "candidate-evidence",
    handoffId: input.handoffId,
    candidateReason: input.candidateReason,
    ...(input.cause === undefined ? {} : { cause: input.cause }),
  });

const isImplementationStoreError = Schema.is(AgentControlImplementationStoreError);

export const isAgentControlImplementationCandidateEvidenceError = (
  error: unknown,
): error is AgentControlImplementationStoreError & { readonly reason: "candidate-evidence" } =>
  isImplementationStoreError(error) && error.reason === "candidate-evidence";

export interface AgentControlImplementationTurnAcceptance {
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

export interface AgentControlImplementationHandoffStoreShape {
  readonly insertAcceptedInTransaction: (
    evidence: AgentControlImplementationHandoffEvidence,
    authority: AgentControlImplementationHandoffAuthority,
  ) => Effect.Effect<void, AgentControlImplementationStoreError>;
  readonly loadAcceptedByHandoffId: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationClaim>,
    AgentControlImplementationStoreError
  >;
  readonly loadAcceptedByTurnRequestCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationClaim>,
    AgentControlImplementationStoreError
  >;
  readonly loadAcceptedByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationClaim>,
    AgentControlImplementationStoreError
  >;
  readonly listRecoverable: (
    now: string,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<string>, AgentControlImplementationStoreError>;
  readonly isHandoffOwnedTurnRequest: (
    commandId: CommandId,
  ) => Effect.Effect<boolean, AgentControlImplementationStoreError>;
  readonly loadTurnAcceptance: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationTurnAcceptance>,
    AgentControlImplementationStoreError
  >;
  readonly markTurnAccepted: (
    handoffId: string,
    expectedRevision: number,
    at: string,
  ) => Effect.Effect<AgentControlImplementationDelivery, AgentControlImplementationStoreError>;
  readonly claim: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly now: string;
    readonly expiresAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlImplementationClaim>,
    AgentControlImplementationStoreError
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
  }) => Effect.Effect<AgentControlImplementationDelivery, AgentControlImplementationStoreError>;
  readonly markProviderStarted: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<AgentControlImplementationDelivery, AgentControlImplementationStoreError>;
  readonly scheduleRetry: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly nextAttemptAt: string;
    readonly errorCode: string;
    readonly updatedAt: string;
  }) => Effect.Effect<AgentControlImplementationDelivery, AgentControlImplementationStoreError>;
  readonly markAmbiguous: (input: {
    readonly handoffId: string;
    readonly expectedRevision: number;
    readonly terminalAt: string;
  }) => Effect.Effect<AgentControlImplementationDelivery, AgentControlImplementationStoreError>;
  readonly observeProviderStarted: (input: {
    readonly threadId: ThreadId;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlImplementationDelivery>,
    AgentControlImplementationStoreError
  >;
  readonly reconcileAcceptedAmbiguousTerminal: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlImplementationDelivery>,
    AgentControlImplementationStoreError
  >;
  readonly observeProviderTerminal: (input: {
    readonly nativeEvent?: ProviderRuntimeEvent;
    readonly threadId: ThreadId;
    readonly providerTurnId: string;
    readonly state: "completed" | "failed" | "interrupted";
    readonly terminalAt: string;
    readonly errorCode?: string | null;
  }) => Effect.Effect<
    Option.Option<AgentControlImplementationDelivery>,
    AgentControlImplementationStoreError
  >;
  readonly listStageStartCandidates: (
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<string>, AgentControlImplementationStoreError>;
  readonly listStageFinalizationCandidates: (options?: {
    readonly afterHandoffId?: string;
    readonly limit?: number;
  }) => Effect.Effect<ReadonlyArray<string>, AgentControlImplementationStoreError>;
}

export class AgentControlImplementationHandoffStore extends Context.Service<
  AgentControlImplementationHandoffStore,
  AgentControlImplementationHandoffStoreShape
>()("t3/agentControl/implementationTurn/Services/AgentControlImplementationHandoffStore") {}
