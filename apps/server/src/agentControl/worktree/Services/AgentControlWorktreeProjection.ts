import type {
  AgentControlProjectionCorruptError,
  AgentControlWorktreeEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlRepositoryError } from "../../Errors.ts";
import type { AgentControlWorktreeEventStoreError } from "./AgentControlWorktreeEventStore.ts";

export type AgentControlWorktreeProjectionError =
  | AgentControlRepositoryError
  | AgentControlWorktreeEventStoreError
  | AgentControlProjectionCorruptError;

export interface AgentControlWorktreeProjectionShape {
  readonly bootstrap: Effect.Effect<void, AgentControlWorktreeProjectionError>;
  readonly projectEvent: (
    event: AgentControlWorktreeEvent,
  ) => Effect.Effect<void, AgentControlWorktreeProjectionError>;
  readonly rebuild: Effect.Effect<void, AgentControlWorktreeProjectionError>;
}

export class AgentControlWorktreeProjection extends Context.Service<
  AgentControlWorktreeProjection,
  AgentControlWorktreeProjectionShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeProjection") {}
