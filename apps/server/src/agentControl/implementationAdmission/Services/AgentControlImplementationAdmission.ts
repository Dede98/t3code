import type {
  AgentControlControlledThreadReservationEvent,
  AgentControlStageRunEvent,
  AgentControlStageRunLeaseEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export class AgentControlImplementationAdmissionError extends Schema.TaggedErrorClass<AgentControlImplementationAdmissionError>()(
  "AgentControlImplementationAdmissionError",
  {
    operation: Schema.String,
    handoffId: Schema.String,
    reason: Schema.Literals([
      "persistence",
      "revision-conflict",
      "planning-evidence-corrupt",
      "planning-not-succeeded",
      "task-evidence-stale",
      "worktree-evidence-stale",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "reservation-history-corrupt",
      "identity-mismatch",
      "receipt-mismatch",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlImplementationAdmissionPublication {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly stageEvents: ReadonlyArray<AgentControlStageRunEvent>;
  readonly leaseEvents: ReadonlyArray<AgentControlStageRunLeaseEvent>;
  readonly reservationEvents: ReadonlyArray<AgentControlControlledThreadReservationEvent>;
}

export type AgentControlImplementationAdmissionResult =
  | { readonly _tag: "NotCandidate" }
  | {
      readonly _tag: "Admitted";
      readonly publication: AgentControlImplementationAdmissionPublication;
    }
  | { readonly _tag: "Replayed"; readonly resultEvidenceId: string };

export interface AgentControlImplementationAdmissionShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlImplementationAdmissionResult,
    AgentControlImplementationAdmissionError
  >;
  readonly recover: Effect.Effect<void, AgentControlImplementationAdmissionError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlImplementationAdmissionPublication>;
}

export const AgentControlImplementationAdmission =
  Context.Reference<AgentControlImplementationAdmissionShape>(
    "t3/agentControl/implementationAdmission/Services/AgentControlImplementationAdmission",
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
