import type {
  AgentControlStageRunLeaseId,
  AgentControlStageRunLeaseState,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlStageRunLeaseEnumerationEntry =
  | { readonly _tag: "Valid"; readonly state: AgentControlStageRunLeaseState }
  | {
      readonly _tag: "Corrupt";
      readonly leaseId: AgentControlStageRunLeaseId | null;
      readonly projectId: ProjectId | null;
      readonly taskId: AgentControlTaskId | null;
    };

export interface AgentControlStageRunLeaseStateRepositoryShape {
  readonly get: (
    leaseId: AgentControlStageRunLeaseId,
  ) => Effect.Effect<Option.Option<AgentControlStageRunLeaseState>, AgentControlRepositoryError>;
  readonly save: (
    state: AgentControlStageRunLeaseState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly listProject: (
    projectId: ProjectId,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunLeaseEnumerationEntry>,
    AgentControlRepositoryError
  >;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlStageRunLeaseStateRepository extends Context.Service<
  AgentControlStageRunLeaseStateRepository,
  AgentControlStageRunLeaseStateRepositoryShape
>()("t3/agentControl/stageRunLease/Services/AgentControlStageRunLeaseStateRepository") {}
