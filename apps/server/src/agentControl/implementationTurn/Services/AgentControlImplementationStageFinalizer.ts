import type { AgentControlStageRunEvent, AgentControlStageRunLeaseEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

export class AgentControlImplementationStageFinalizerError extends Schema.TaggedError<AgentControlImplementationStageFinalizerError>()(
  "AgentControlImplementationStageFinalizerError",
  {
    handoffId: Schema.String,
    operation: Schema.String,
    reason: Schema.Literals([
      "candidate-evidence",
      "partial-replay",
      "identity-mismatch",
      "orchestration-history-corrupt",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "revision-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export interface AgentControlImplementationStageFinalizationPublication {
  readonly handoffId: string;
  readonly resultEvidenceId: string;
  readonly outcome: "succeeded" | "failed" | "cancelled";
  readonly stageEvent: AgentControlStageRunEvent;
  readonly leaseEvent: AgentControlStageRunLeaseEvent;
}

export type AgentControlImplementationStageFinalizerResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Ambiguous" }
  | { readonly _tag: "Started"; readonly stageEventSequence: number }
  | { readonly _tag: "Finalized"; readonly resultEvidenceId: string }
  | { readonly _tag: "Replayed"; readonly resultEvidenceId: string };

export interface AgentControlImplementationStageFinalizerShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlImplementationStageFinalizerResult,
    AgentControlImplementationStageFinalizerError
  >;
  readonly recover: Effect.Effect<void, AgentControlImplementationStageFinalizerError>;
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void>;
  readonly streamPublications: Stream.Stream<AgentControlImplementationStageFinalizationPublication>;
  readonly subscribePublications: Effect.Effect<
    Stream.Stream<AgentControlImplementationStageFinalizationPublication>,
    never,
    Scope.Scope
  >;
}

export const AgentControlImplementationStageFinalizer =
  Context.Reference<AgentControlImplementationStageFinalizerShape>(
    "t3/agentControl/implementationTurn/Services/AgentControlImplementationStageFinalizer",
    {
      defaultValue: () => ({
        processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
        recover: Effect.void,
        start: () => Effect.void,
        drain: Effect.void,
        streamPublications: Stream.never,
        subscribePublications: Effect.succeed(Stream.never),
      }),
    },
  );
