import type {
  AgentControlControlledThreadReservationEvent,
  OrchestrationEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export class AgentControlVerificationTurnCoordinatorError extends Schema.TaggedError<AgentControlVerificationTurnCoordinatorError>()(
  "AgentControlVerificationTurnCoordinatorError",
  {
    operation: Schema.String,
    handoffId: Schema.String,
    reason: Schema.Literals([
      "not-candidate",
      "admission-corrupt",
      "identity-mismatch",
      "source-stale",
      "runtime-policy-unavailable",
      "reservation-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlVerificationTurnMaterializationPublication {
  readonly handoffId: string;
  readonly verificationHandoffId: string;
  readonly threadId: ThreadId;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
  readonly orchestrationEvents: ReadonlyArray<OrchestrationEvent>;
}

export type AgentControlVerificationTurnMaterializationResult =
  | {
      readonly _tag: "Materialized";
      readonly publication: AgentControlVerificationTurnMaterializationPublication;
    }
  | {
      readonly _tag: "Replayed";
      readonly verificationHandoffId: string;
      readonly threadId: ThreadId;
    }
  | { readonly _tag: "NotCandidate" };

export interface AgentControlVerificationTurnCoordinatorShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlVerificationTurnMaterializationResult,
    AgentControlVerificationTurnCoordinatorError
  >;
  readonly recover: Effect.Effect<void, AgentControlVerificationTurnCoordinatorError>;
  /** Prepare one attempt-owned worker and subscriptions, parked behind activation. */
  readonly prepare: (activation: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void, AgentControlVerificationTurnCoordinatorError>;
  readonly streamPublications: Stream.Stream<AgentControlVerificationTurnMaterializationPublication>;
}

export const AgentControlVerificationTurnCoordinator =
  Context.Reference<AgentControlVerificationTurnCoordinatorShape>(
    "t3/agentControl/verificationTurn/Services/AgentControlVerificationTurnCoordinator",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "NotCandidate" }),
        recover: Effect.void,
        prepare: () => Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
      }),
    },
  );
