/**
 * OrchestrationEngineService - Service interface for orchestration command handling.
 *
 * Owns command validation/dispatch and in-memory read-model updates backed by
 * `OrchestrationEventStore` persistence. It does not own provider process
 * management or transport concerns (e.g. websocket request parsing).
 *
 * Uses Effect `Context.Service` for dependency injection. Command dispatch,
 * replay, and unknown-input decoding all return typed domain errors.
 *
 * @module OrchestrationEngineService
 */
import type {
  AgentControlThreadMaterializeCommand,
  OrchestrationCommand,
  OrchestrationEvent,
  OrchestrationReadModel,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import type * as Stream from "effect/Stream";

import type { OrchestrationDispatchError } from "../Errors.ts";
import type { OrchestrationEventStoreError } from "../../persistence/Errors.ts";

export interface AgentControlThreadMaterializationTransactionResult {
  readonly command: AgentControlThreadMaterializeCommand;
  readonly commandFingerprint: string;
  readonly committedEvents: ReadonlyArray<OrchestrationEvent>;
  readonly lastSequence: number;
  readonly nextCommandReadModel: OrchestrationReadModel;
}

/**
 * OrchestrationEngineShape - Service API for orchestration command and event flow.
 */
export interface OrchestrationEngineShape {
  /**
   * Replay persisted orchestration events from an exclusive sequence cursor.
   *
   * @param fromSequenceExclusive - Sequence cursor (exclusive).
   * @param limit - Maximum number of events to read. Defaults to the event
   *   store's page-bounded default; pass a higher value when the caller must
   *   read every event after the cursor (e.g. per-thread catch-up that filters
   *   a small subset out of a potentially larger global range).
   * @returns Stream containing ordered events.
   */
  readonly readEvents: (
    fromSequenceExclusive: number,
    limit?: number,
  ) => Stream.Stream<OrchestrationEvent, OrchestrationEventStoreError, never>;

  /**
   * Dispatch a validated server-internal orchestration command as `system`.
   *
   * @param command - Valid orchestration command.
   * @returns Effect containing the sequence of the persisted event.
   *
   * Dispatch is serialized through an internal queue and deduplicated via
   * command receipts.
   */
  readonly dispatch: (
    command: OrchestrationCommand,
  ) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError, never>;

  /**
   * Dispatch a command received from a client transport.
   *
   * Client payloads never supply the authority; the server assigns it by
   * selecting this path.
   */
  readonly dispatchClient: (
    command: OrchestrationCommand,
  ) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError, never>;

  /** Server-owned, non-RPC dispatch path for Agent Control commands. */
  readonly dispatchAgentControl: (
    command: OrchestrationCommand,
  ) => Effect.Effect<{ sequence: number }, OrchestrationDispatchError, never>;
  /**
   * Caller-owned transaction primitive for the controlled-thread coordinator.
   * It writes events, projection, intent, and receipt but deliberately leaves
   * the accepted evidence marker and publication to the caller.
   */
  readonly materializeAgentControlInTransaction?: (
    command: AgentControlThreadMaterializeCommand,
  ) => Effect.Effect<
    AgentControlThreadMaterializationTransactionResult,
    OrchestrationDispatchError,
    never
  >;
  /** Inserts the accepted orchestration evidence marker in the active transaction. */
  readonly completeAgentControlMaterializationInTransaction?: (
    result: AgentControlThreadMaterializationTransactionResult,
  ) => Effect.Effect<void, OrchestrationDispatchError, never>;
  /** Validates complete accepted evidence without decision or current authority checks. */
  readonly replayAgentControlMaterialization?: (
    command: AgentControlThreadMaterializeCommand,
  ) => Effect.Effect<
    AgentControlThreadMaterializationTransactionResult,
    OrchestrationDispatchError,
    never
  >;
  /** Refreshes local authority and publishes only newly committed events after outer commit. */
  readonly publishAgentControlMaterialization?: (
    result: AgentControlThreadMaterializationTransactionResult,
  ) => Effect.Effect<void, OrchestrationDispatchError, never>;

  /**
   * Stream persisted domain events in dispatch order.
   *
   * This is a hot runtime stream (new events only), not a historical replay.
   */
  readonly streamDomainEvents: Stream.Stream<OrchestrationEvent>;
  /** Acquires a hot subscription before returning the stream. */
  readonly subscribeDomainEvents?: Effect.Effect<
    Stream.Stream<OrchestrationEvent>,
    never,
    Scope.Scope
  >;

  /**
   * The latest sequence reflected in the engine's authoritative command read
   * model (0 if none). Used to gauge how far behind a resuming client is before
   * choosing between an incremental replay and a fresh projected snapshot.
   */
  readonly latestSequence: Effect.Effect<number, never, never>;
}

/**
 * OrchestrationEngineService - Service tag for orchestration engine access.
 *
 * @example
 * ```ts
 * const program = Effect.gen(function* () {
 *   const engine = yield* OrchestrationEngineService
 *   return yield* engine.dispatch(command)
 * })
 * ```
 */
export class OrchestrationEngineService extends Context.Service<
  OrchestrationEngineService,
  OrchestrationEngineShape
>()("t3/orchestration/Services/OrchestrationEngine/OrchestrationEngineService") {}
