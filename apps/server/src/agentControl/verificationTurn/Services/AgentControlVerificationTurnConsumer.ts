import * as Context from "effect/Context";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import type * as Effect from "effect/Effect";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import type {
  ProviderRuntimeEventDrainToken,
  ProviderRuntimeEventPublication,
} from "../../../provider/Services/ProviderService.ts";

export interface AgentControlVerificationTurnConsumerActivation {
  /** Release initial and periodic durable recovery after full server readiness. */
  readonly commit: Effect.Effect<void>;
  readonly drain: Effect.Effect<void>;
  /**
   * Drain the marked Provider prefix through durable turn adoption and every
   * Verification wakeup caused by that prefix.
   */
  readonly drainProviderEvents: (token: ProviderRuntimeEventDrainToken) => Effect.Effect<void>;
}

export interface AgentControlVerificationTurnConsumerShape {
  readonly processHandoff: (handoffId: string) => Effect.Effect<void, Error>;
  readonly processRuntimeEvent: (event: ProviderRuntimeEvent) => Effect.Effect<void, Error>;
  readonly recover: Effect.Effect<void, Error>;
  readonly subscribeProviderEvents: Effect.Effect<
    PubSub.Subscription<ProviderRuntimeEventPublication | ProviderRuntimeEvent>,
    never,
    Scope.Scope
  >;
  /**
   * Acquire subscriptions, worker, and recovery loop for one startup attempt.
   * Recovery remains parked until the returned activation is committed.
   */
  readonly prepare: (
    providerEvents?: PubSub.Subscription<ProviderRuntimeEventPublication | ProviderRuntimeEvent>,
    activation?: Effect.Effect<void>,
  ) => Effect.Effect<AgentControlVerificationTurnConsumerActivation, never, Scope.Scope>;
  readonly start: (
    providerEvents?: PubSub.Subscription<ProviderRuntimeEventPublication | ProviderRuntimeEvent>,
  ) => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
}

export class AgentControlVerificationTurnConsumer extends Context.Service<
  AgentControlVerificationTurnConsumer,
  AgentControlVerificationTurnConsumerShape
>()("t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer") {}
