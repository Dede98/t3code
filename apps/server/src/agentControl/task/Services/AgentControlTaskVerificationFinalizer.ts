import type { AgentControlTaskEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlTaskVerificationFinalizerError extends Schema.TaggedError<AgentControlTaskVerificationFinalizerError>()(
  "AgentControlTaskVerificationFinalizerError",
  {
    handoffId: Schema.String,
    operation: Schema.String,
    reason: Schema.Literals([
      "authority-conflict",
      "partial-replay",
      "identity-mismatch",
      "revision-conflict",
      "persistence",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export type AgentControlTaskVerificationFinalizerResult =
  | { readonly _tag: "RepairPending"; readonly repairHandoffId: string }
  | { readonly _tag: "Finalized"; readonly taskFinalizationEvidenceId: string }
  | { readonly _tag: "Replayed"; readonly taskFinalizationEvidenceId: string };

export interface AgentControlTaskVerificationFinalizationPublication {
  readonly handoffId: string;
  readonly taskFinalizationEvidenceId: string;
  readonly event: AgentControlTaskEvent;
}

export interface AgentControlTaskVerificationFinalizerShape {
  readonly processHandoff: (
    handoffId: string,
  ) => Effect.Effect<
    AgentControlTaskVerificationFinalizerResult,
    AgentControlTaskVerificationFinalizerError
  >;
  readonly recover: Effect.Effect<void, AgentControlTaskVerificationFinalizerError>;
  readonly prepare: (activation: Effect.Effect<void>) => Effect.Effect<void, never, Scope.Scope>;
  readonly drain: Effect.Effect<void, AgentControlTaskVerificationFinalizerError>;
}

export class AgentControlTaskVerificationFinalizer extends Context.Service<
  AgentControlTaskVerificationFinalizer,
  AgentControlTaskVerificationFinalizerShape
>()("t3/agentControl/task/Services/AgentControlTaskVerificationFinalizer") {}
