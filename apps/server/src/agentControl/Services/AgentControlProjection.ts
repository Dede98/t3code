import type { AgentControlEvent, AgentControlProjectionCorruptError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlEventStoreError } from "../Errors.ts";
import type { AgentControlProjectionRepositoryError } from "../../persistence/Services/AgentControlProjectStates.ts";

export type AgentControlProjectionError =
  | AgentControlEventStoreError
  | AgentControlProjectionRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlProjectionError>;
  readonly projectEvent: (
    event: AgentControlEvent,
  ) => Effect.Effect<void, AgentControlProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlProjectionError>;
}

export class AgentControlProjection extends Context.Service<
  AgentControlProjection,
  AgentControlProjectionShape
>()("t3/agentControl/Services/AgentControlProjection") {}
