/**
 * ProviderService - Service interface for provider sessions, turns, and checkpoints.
 *
 * Acts as the cross-provider facade used by transports (WebSocket/RPC). It
 * resolves provider adapters through `ProviderAdapterRegistry`, routes
 * session-scoped calls via `ProviderSessionDirectory`, and exposes one unified
 * provider event stream to callers.
 *
 * Uses Effect `Context.Service` for dependency injection and returns typed
 * domain errors for validation, session, codex, and checkpoint workflows.
 *
 * @module ProviderService
 */
import type {
  ProviderInterruptTurnInput,
  ProviderInstanceId,
  ProviderRespondToRequestInput,
  ProviderRespondToUserInputInput,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ProviderStopSessionInput,
  ThreadId,
  ProviderTurnStartResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Cause from "effect/Cause";
import type * as Deferred from "effect/Deferred";
import type * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type { ProviderServiceError } from "../Errors.ts";
import type { ProviderAdapterCapabilities } from "./ProviderAdapter.ts";
import type { ProviderSessionAttestation, ProviderTurnAttestation } from "./ProviderAdapter.ts";
import type { ProviderInstanceRoutingInfo } from "./ProviderAdapterRegistry.ts";
import type { ProviderAdmissionPermit } from "../../agentControl/providerAdmission/model.ts";

/**
 * One finite provider-publication prefix shared by the two required startup
 * consumers. The marker is published only after adapter intake has stopped and
 * every process-owned event has finished the canonical ProviderService handoff.
 */
export interface ProviderRuntimeEventDrainToken {
  readonly id: number;
  readonly runtimeIngestionAcknowledgement: Deferred.Deferred<void, Error>;
  readonly verificationAcknowledgement: Deferred.Deferred<void, Error>;
}

export type ProviderRuntimeEventPublication =
  | { readonly _tag: "Event"; readonly event: ProviderRuntimeEvent }
  | { readonly _tag: "Drain"; readonly token: ProviderRuntimeEventDrainToken };

export interface ProviderRuntimeEventQuiesceResult {
  readonly token: ProviderRuntimeEventDrainToken;
  /** Terminal result of the attempt-local canonical-log/publication pump. */
  readonly sourceExit: Exit.Exit<void>;
}

export interface ProviderRuntimeEventSourceActivation {
  /**
   * Snapshot the events already accepted from adapter pulls and wait until that
   * finite prefix has completed canonical logging and Provider PubSub fan-out.
   */
  readonly handoffAccepted: Effect.Effect<void>;
  /** Stop new adapter pulls, drain accepted events, then publish a drain marker. */
  readonly quiesce: Effect.Effect<ProviderRuntimeEventQuiesceResult>;
  /**
   * Terminal control-plane cutout for a post-barrier/pre-activation failure.
   * It never publishes a normal event or marker and never opens activation.
   */
  readonly abort: (cause: Cause.Cause<unknown>) => Effect.Effect<void>;
  /** Fails with the exact terminal abort Cause and otherwise never completes. */
  readonly awaitAbort: Effect.Effect<never>;
}

/**
 * ProviderServiceShape - Service API for provider session and turn orchestration.
 */
export interface ProviderServiceShape {
  /**
   * Start a provider session.
   */
  readonly startSession: (
    threadId: ThreadId,
    input: ProviderSessionStartInput,
    authority?: { readonly providerAdmissionPermit: ProviderAdmissionPermit },
  ) => Effect.Effect<ProviderSession, ProviderServiceError>;

  /**
   * Send a provider turn.
   */
  readonly sendTurn: (
    input: ProviderSendTurnInput,
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError>;

  readonly sendTurnAtPreInvokeBoundary?: (
    input: ProviderSendTurnInput,
    boundary: {
      readonly expected: ProviderSessionAttestation;
      readonly providerAdmissionPermit: ProviderAdmissionPermit;
      readonly beforeDeliveryCas: () => Effect.Effect<void, ProviderServiceError>;
      readonly persistDeliveryAttempted: (
        attestation: ProviderTurnAttestation,
      ) => Effect.Effect<void, ProviderServiceError>;
      readonly afterDeliveryCas: () => Effect.Effect<void, ProviderServiceError>;
      readonly onAdapterEntered?: () => void;
      readonly onExternalOperationStarted?: () => void;
      readonly onNativeInvocationStarted?: () => void;
    },
  ) => Effect.Effect<ProviderTurnStartResult, ProviderServiceError>;

  readonly quarantineAdmissionIfEntered?: (
    permit: ProviderAdmissionPermit,
  ) => Effect.Effect<void, ProviderServiceError>;

  readonly getSessionAttestation?: (
    threadId: ThreadId,
  ) => Effect.Effect<ProviderSessionAttestation | undefined>;

  /**
   * Interrupt a running provider turn.
   */
  readonly interruptTurn: (
    input: ProviderInterruptTurnInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Respond to a provider approval request.
   */
  readonly respondToRequest: (
    input: ProviderRespondToRequestInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Respond to a provider structured user-input request.
   */
  readonly respondToUserInput: (
    input: ProviderRespondToUserInputInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Stop a provider session.
   */
  readonly stopSession: (
    input: ProviderStopSessionInput,
  ) => Effect.Effect<void, ProviderServiceError>;

  /**
   * List active provider sessions.
   *
   * Aggregates runtime session lists from all registered adapters.
   */
  readonly listSessions: () => Effect.Effect<ReadonlyArray<ProviderSession>>;

  /**
   * Read capabilities for the adapter bound to a configured provider instance.
   */
  readonly getCapabilities: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<ProviderAdapterCapabilities, ProviderServiceError>;

  readonly getInstanceInfo: (
    instanceId: ProviderInstanceId,
  ) => Effect.Effect<ProviderInstanceRoutingInfo, ProviderServiceError>;

  /**
   * Roll back provider conversation state by a number of turns.
   */
  readonly rollbackConversation: (input: {
    readonly threadId: ThreadId;
    readonly numTurns: number;
  }) => Effect.Effect<void, ProviderServiceError>;

  /**
   * Acquire a runtime-event subscription synchronously within the caller's scope.
   * Events published after this effect completes are buffered until consumed.
   */
  readonly subscribeEvents?: Effect.Effect<
    PubSub.Subscription<ProviderRuntimeEvent>,
    never,
    Scope.Scope
  >;

  /**
   * Lifecycle-aware subscription used by the two required runtime consumers.
   * Drain markers are ordered behind the complete process-owned event prefix.
   */
  readonly subscribeRuntimeEventPublications?: Effect.Effect<
    PubSub.Subscription<ProviderRuntimeEventPublication>,
    never,
    Scope.Scope
  >;

  /**
   * Start adapter event sources in the caller's scope.
   *
   * The server startup attempt owns these fibers. Closing that attempt stops
   * every adapter subscription without changing the durable provider state.
   */
  readonly startRuntimeEventSources?: Effect.Effect<
    ProviderRuntimeEventSourceActivation,
    never,
    Scope.Scope
  >;

  /** Open provider runtime publication after required startup subscriptions exist. */
  readonly openRuntimeEventPublishing?: Effect.Effect<void>;

  /**
   * Canonical provider runtime event stream.
   *
   * Fan-out is owned by ProviderService (not by a standalone event-bus service).
   */
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}

/**
 * ProviderService - Service tag for provider orchestration.
 */
export class ProviderService extends Context.Service<ProviderService, ProviderServiceShape>()(
  "t3/provider/Services/ProviderService",
) {}
