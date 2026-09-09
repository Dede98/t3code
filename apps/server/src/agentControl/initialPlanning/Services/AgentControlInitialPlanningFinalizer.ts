import type { AgentControlStageRunEvent, AgentControlStageRunLeaseEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export class AgentControlInitialPlanningFinalizerError extends Schema.TaggedError<AgentControlInitialPlanningFinalizerError>()(
  "AgentControlInitialPlanningFinalizerError",
  {
    operation: Schema.String,
    handoffId: Schema.String,
    reason: Schema.Literals([
      "persistence",
      "corrupt-handoff",
      "corrupt-orchestration-history",
      "corrupt-stage-history",
      "corrupt-lease-history",
      "ambiguous-plan",
      "missing-plan",
      "identity-mismatch",
      "revision-conflict",
      "receipt-mismatch",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlInitialPlanningFinalizationPublication {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly outcome: "succeeded" | "failed" | "cancelled";
  readonly stageEvents: ReadonlyArray<AgentControlStageRunEvent>;
  readonly leaseEvents: ReadonlyArray<AgentControlStageRunLeaseEvent>;
}

export type AgentControlInitialPlanningFinalizerResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Ambiguous" }
  | { readonly _tag: "Started"; readonly event: AgentControlStageRunEvent }
  | {
      readonly _tag: "Finalized";
      readonly publication: AgentControlInitialPlanningFinalizationPublication;
    }
  | { readonly _tag: "Replayed"; readonly resultEvidenceId: string };

export interface AgentControlInitialPlanningFinalizerShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlInitialPlanningFinalizerResult,
    AgentControlInitialPlanningFinalizerError
  >;
  readonly recover: Effect.Effect<void, AgentControlInitialPlanningFinalizerError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlInitialPlanningFinalizationPublication>;
}

export const AgentControlInitialPlanningFinalizer =
  Context.Reference<AgentControlInitialPlanningFinalizerShape>(
    "t3/agentControl/initialPlanning/Services/AgentControlInitialPlanningFinalizer",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
      }),
    },
  );
