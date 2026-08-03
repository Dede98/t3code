import type {
  AgentControlControlledThreadReservationId,
  CommandId,
  MessageId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type {
  AgentControlInitialPlanningClaim,
  AgentControlInitialPlanningDelivery,
  AgentControlInitialPlanningHandoffEvidence,
} from "../model.ts";

export class AgentControlInitialPlanningStoreError extends Schema.TaggedErrorClass<AgentControlInitialPlanningStoreError>()(
  "AgentControlInitialPlanningStoreError",
  {
    operation: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlInitialPlanningTurnAcceptance {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
  readonly threadId: ThreadId;
  readonly turnRequestCommandId: CommandId;
  readonly messageId: MessageId;
  readonly messageEventId: string;
  readonly messageEventSequence: number;
  readonly turnRequestEventId: string;
  readonly turnRequestEventSequence: number;
  readonly messageEventEnvelopeJson: string;
  readonly turnRequestEventEnvelopeJson: string;
  readonly eventEvidenceDigest: string;
  readonly acceptedAt: string;
}

export interface AgentControlInitialPlanningHandoffStoreShape {
  readonly insertAcceptedInTransaction: (
    evidence: AgentControlInitialPlanningHandoffEvidence,
    hooks?: {
      readonly afterIntent?: () => Effect.Effect<void>;
      readonly afterReceipt?: () => Effect.Effect<void>;
      readonly afterAccepted?: () => Effect.Effect<void>;
      readonly afterDelivery?: () => Effect.Effect<void>;
    },
  ) => Effect.Effect<void, AgentControlInitialPlanningStoreError>;
  readonly loadAcceptedByHandoffId: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
  >;
  readonly loadAcceptedByTurnRequestCommandId: (
    commandId: CommandId,
  ) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
  >;
  readonly loadAcceptedByThreadId: (
    threadId: ThreadId,
  ) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
  >;
  readonly listRecoverable: (
    now: string,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
  >;
  readonly isHandoffOwnedTurnRequest: (
    commandId: CommandId,
  ) => Effect.Effect<boolean, AgentControlInitialPlanningStoreError>;
  readonly loadTurnAcceptance: (
    handoffId: string,
  ) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningTurnAcceptance>,
    AgentControlInitialPlanningStoreError
  >;
  readonly markTurnAccepted: (
    handoffId: string,
    expectedRevision: number,
    at: string,
  ) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly claim: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly now: string;
    readonly expiresAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
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
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly markProviderStarted: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly scheduleRetry: (input: {
    readonly handoffId: string;
    readonly ownerId: string;
    readonly claimGeneration: number;
    readonly expectedRevision: number;
    readonly nextAttemptAt: string;
    readonly errorCode: string;
    readonly updatedAt: string;
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly markAmbiguous: (input: {
    readonly handoffId: string;
    readonly expectedRevision: number;
    readonly terminalAt: string;
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly markTerminal: (input: {
    readonly handoffId: string;
    readonly expectedRevision: number;
    readonly state: "completed" | "failed" | "interrupted";
    readonly terminalAt: string;
    readonly errorCode?: string | null;
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly requestInterrupt: (input: {
    readonly handoffId: string;
    readonly expectedRevision: number;
    readonly requestedAt: string;
  }) => Effect.Effect<AgentControlInitialPlanningDelivery, AgentControlInitialPlanningStoreError>;
  readonly observeProviderStarted: (input: {
    readonly threadId: ThreadId;
    readonly providerTurnId: string;
    readonly acceptedAt: string;
  }) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningDelivery>,
    AgentControlInitialPlanningStoreError
  >;
  readonly observeProviderTerminal: (input: {
    readonly threadId: ThreadId;
    readonly providerTurnId: string;
    readonly state: "completed" | "failed" | "interrupted";
    readonly terminalAt: string;
    readonly errorCode?: string | null;
  }) => Effect.Effect<
    Option.Option<AgentControlInitialPlanningDelivery>,
    AgentControlInitialPlanningStoreError
  >;
  readonly listExpired: (
    now: string,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlInitialPlanningClaim>,
    AgentControlInitialPlanningStoreError
  >;
  readonly listStageFinalizationCandidates: (
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<string>, AgentControlInitialPlanningStoreError>;
}

export class AgentControlInitialPlanningHandoffStore extends Context.Service<
  AgentControlInitialPlanningHandoffStore,
  AgentControlInitialPlanningHandoffStoreShape
>()("t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningHandoffStore") {}
