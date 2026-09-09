import type { AgentControlStageRunEvent, AgentControlStageRunLeaseEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlVerificationStageFinalizerError extends Schema.TaggedError<AgentControlVerificationStageFinalizerError>()(
  "AgentControlVerificationStageFinalizerError",
  {
    handoffId: Schema.String,
    operation: Schema.String,
    reason: Schema.Literals([
      "authority-conflict",
      "evaluation-conflict",
      "partial-replay",
      "identity-mismatch",
      "stage-history-corrupt",
      "lease-history-corrupt",
      "revision-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type AgentControlVerificationStageFinalizerResult =
  | { readonly _tag: "Waiting" }
  | { readonly _tag: "Finalized"; readonly finalizationEvidenceId: string }
  | { readonly _tag: "Replayed"; readonly finalizationEvidenceId: string };

export interface AgentControlVerificationStageFinalizationPublication {
  readonly handoffId: string;
  readonly finalizationEvidenceId: string;
  readonly outcome: "succeeded" | "failed" | "cancelled";
  readonly stageEvent: AgentControlStageRunEvent;
  readonly leaseEvent: AgentControlStageRunLeaseEvent;
}

export interface AgentControlVerificationStageFinalizerShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlVerificationStageFinalizerResult,
    AgentControlVerificationStageFinalizerError
  >;
  readonly recover: Effect.Effect<void, AgentControlVerificationStageFinalizerError>;
  readonly prepare: (activation: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void, AgentControlVerificationStageFinalizerError>;
}

export class AgentControlVerificationStageFinalizer extends Context.Service<
  AgentControlVerificationStageFinalizer,
  AgentControlVerificationStageFinalizerShape
>()("t3/agentControl/verificationTurn/Services/AgentControlVerificationStageFinalizer") {}
