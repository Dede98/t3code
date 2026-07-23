import type {
  AgentControlGithubEvent,
  AgentControlProjectionCorruptError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlEventStoreError, AgentControlRepositoryError } from "../../Errors.ts";

export type AgentControlGithubProjectionError =
  | AgentControlEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlGithubProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlGithubProjectionError>;
  readonly projectEvent: (
    event: AgentControlGithubEvent,
  ) => Effect.Effect<void, AgentControlGithubProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlGithubProjectionError>;
}

export class AgentControlGithubProjection extends Context.Service<
  AgentControlGithubProjection,
  AgentControlGithubProjectionShape
>()("t3/agentControl/github/Services/AgentControlGithubProjection") {}
