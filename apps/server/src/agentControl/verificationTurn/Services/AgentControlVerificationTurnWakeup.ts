import * as Context from "effect/Context";
import type * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import type * as Exit from "effect/Exit";
import type * as PubSub from "effect/PubSub";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export interface AgentControlVerificationWakeupDrainToken {
  readonly id: number;
  readonly acknowledgement: Deferred.Deferred<void>;
}

export type AgentControlVerificationWakeupPublication =
  | { readonly _tag: "Handoff"; readonly handoffId: string }
  | { readonly _tag: "Drain"; readonly token: AgentControlVerificationWakeupDrainToken };

export interface AgentControlVerificationStageStarterSubscription {
  readonly subscription: PubSub.Subscription<AgentControlVerificationWakeupPublication>;
  readonly reportExit: (exit: Exit.Exit<void>) => Effect.Effect<void>;
}

export interface AgentControlVerificationTurnWakeupShape {
  readonly wake: (handoffId: string) => Effect.Effect<void>;
  readonly stream: Stream.Stream<string>;
  readonly subscribe: Effect.Effect<Stream.Stream<string>, never, Scope.Scope>;
  /** Lifecycle-aware subscription owned by the attempt-local Stage-Starter. */
  readonly subscribeStageStarter?: Effect.Effect<
    AgentControlVerificationStageStarterSubscription,
    never,
    Scope.Scope
  >;
  /** Publish a marker and await the Stage-Starter's durable prefix drain. */
  readonly drainStageStarter?: Effect.Effect<void>;
}

export const AgentControlVerificationTurnWakeup =
  Context.Reference<AgentControlVerificationTurnWakeupShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnWakeup",
    {
      defaultValue: () => ({
        wake: () => Effect.void,
        stream: Stream.never,
        subscribe: Effect.succeed(Stream.never),
      }),
    },
  );
