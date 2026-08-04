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

export class AgentControlImplementationTurnCoordinatorError extends Schema.TaggedErrorClass<AgentControlImplementationTurnCoordinatorError>()(
  "AgentControlImplementationTurnCoordinatorError",
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

export interface AgentControlImplementationTurnMaterializationPublication {
  readonly handoffId: string;
  readonly implementationHandoffId: string;
  readonly threadId: ThreadId;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
  readonly orchestrationEvents: ReadonlyArray<OrchestrationEvent>;
}

export type AgentControlImplementationTurnMaterializationResult =
  | {
      readonly _tag: "Materialized";
      readonly publication: AgentControlImplementationTurnMaterializationPublication;
    }
  | {
      readonly _tag: "Replayed";
      readonly implementationHandoffId: string;
      readonly threadId: ThreadId;
    }
  | { readonly _tag: "NotCandidate" };

export interface AgentControlImplementationTurnCoordinatorShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlImplementationTurnMaterializationResult,
    AgentControlImplementationTurnCoordinatorError
  >;
  readonly recover: Effect.Effect<void, AgentControlImplementationTurnCoordinatorError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlImplementationTurnMaterializationPublication>;
}

export const AgentControlImplementationTurnCoordinator =
  Context.Reference<AgentControlImplementationTurnCoordinatorShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationTurnCoordinator",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "NotCandidate" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
      }),
    },
  );
