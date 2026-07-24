import type {
  AgentControlProjectionCorruptError,
  AgentControlStageRunEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  AgentControlRepositoryError,
  AgentControlStageRunEventStoreError,
} from "../../Errors.ts";

export type AgentControlStageRunProjectionError =
  | AgentControlStageRunEventStoreError
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlStageRunProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlStageRunProjectionError>;
  readonly projectEvent: (
    event: AgentControlStageRunEvent,
  ) => Effect.Effect<void, AgentControlStageRunProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlStageRunProjectionError>;
}

export class AgentControlStageRunProjection extends Context.Service<
  AgentControlStageRunProjection,
  AgentControlStageRunProjectionShape
>()("t3/agentControl/stageRun/Services/AgentControlStageRunProjection") {}
