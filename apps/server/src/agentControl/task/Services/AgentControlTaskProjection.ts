import type { AgentControlProjectionCorruptError, AgentControlTaskEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlRepositoryError, AgentControlTaskEventStoreError } from "../../Errors.ts";

export type AgentControlTaskProjectionError =
  | AgentControlTaskEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlTaskProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlTaskProjectionError>;
  readonly projectEvent: (
    event: AgentControlTaskEvent,
  ) => Effect.Effect<void, AgentControlTaskProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlTaskProjectionError>;
}

export class AgentControlTaskProjection extends Context.Service<
  AgentControlTaskProjection,
  AgentControlTaskProjectionShape
>()("t3/agentControl/task/Services/AgentControlTaskProjection") {}
