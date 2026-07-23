import type { AgentControlTaskId, AgentControlTaskState, ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlTaskEnumerationEntry =
  | { readonly _tag: "Valid"; readonly state: AgentControlTaskState }
  | {
      readonly _tag: "Corrupt";
      readonly taskId: AgentControlTaskId | null;
      readonly projectId: ProjectId | null;
    };

export interface AgentControlTaskStateRepositoryShape {
  readonly get: (
    taskId: AgentControlTaskId,
  ) => Effect.Effect<Option.Option<AgentControlTaskState>, AgentControlRepositoryError>;
  readonly save: (
    state: AgentControlTaskState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly listProject: (
    projectId: ProjectId,
  ) => Effect.Effect<ReadonlyArray<AgentControlTaskEnumerationEntry>, AgentControlRepositoryError>;
  readonly listAll: Effect.Effect<
    ReadonlyArray<AgentControlTaskEnumerationEntry>,
    AgentControlRepositoryError
  >;
  readonly findByIdentity: (
    projectId: ProjectId,
    repositoryNodeId: string,
    issueNodeId: string,
  ) => Effect.Effect<Option.Option<AgentControlTaskState>, AgentControlRepositoryError>;
  readonly findBySourceNumber: (
    projectId: ProjectId,
    repositoryNodeId: string,
    issueNumber: number,
  ) => Effect.Effect<Option.Option<AgentControlTaskState>, AgentControlRepositoryError>;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlTaskStateRepository extends Context.Service<
  AgentControlTaskStateRepository,
  AgentControlTaskStateRepositoryShape
>()("t3/agentControl/task/Services/AgentControlTaskStateRepository") {}
