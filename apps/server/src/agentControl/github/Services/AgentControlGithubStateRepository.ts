import type {
  AgentControlGithubIntakeState,
  AgentControlGithubIssueSnapshot,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";

import type { AgentControlRepositoryError } from "../../Errors.ts";

export interface AgentControlGithubStateRepositoryShape {
  readonly get: (
    projectId: ProjectId,
  ) => Effect.Effect<Option.Option<AgentControlGithubIntakeState>, AgentControlRepositoryError>;
  readonly save: (
    state: AgentControlGithubIntakeState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly replaceIssues: (
    projectId: ProjectId,
    issues: ReadonlyArray<AgentControlGithubIssueSnapshot>,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly listIssues: (
    projectId: ProjectId,
  ) => Effect.Effect<ReadonlyArray<AgentControlGithubIssueSnapshot>, AgentControlRepositoryError>;
  readonly deleteProject: (
    projectId: ProjectId,
  ) => Effect.Effect<void, AgentControlRepositoryError>;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlGithubStateRepository extends Context.Service<
  AgentControlGithubStateRepository,
  AgentControlGithubStateRepositoryShape
>()("t3/agentControl/github/Services/AgentControlGithubStateRepository") {}
