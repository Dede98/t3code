import {
  AgentControlProjectState,
  AgentControlProjectionCorruptError,
  IsoDateTime,
  NonNegativeInt,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type { AgentControlRepositoryError } from "../../agentControl/Errors.ts";

export type AgentControlProjectionRepositoryError =
  | AgentControlRepositoryError
  | AgentControlProjectionCorruptError;

export interface AgentControlProjectStateRepositoryShape {
  readonly get: (
    projectId: AgentControlProjectState["projectId"],
  ) => Effect.Effect<
    Option.Option<AgentControlProjectState>,
    AgentControlProjectionRepositoryError
  >;
  readonly save: (
    state: AgentControlProjectState,
    expectedRevision: number,
  ) => Effect.Effect<void, AgentControlProjectionRepositoryError>;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlProjectStateRepository extends Context.Service<
  AgentControlProjectStateRepository,
  AgentControlProjectStateRepositoryShape
>()("t3/persistence/Services/AgentControlProjectStates/AgentControlProjectStateRepository") {}

export const AgentControlProjectionCursor = Schema.Struct({
  projectorName: Schema.String,
  lastAppliedSequence: NonNegativeInt,
  updatedAt: IsoDateTime,
});
export type AgentControlProjectionCursor = typeof AgentControlProjectionCursor.Type;

export interface AgentControlProjectionStateRepositoryShape {
  readonly get: (
    projectorName: string,
  ) => Effect.Effect<
    Option.Option<AgentControlProjectionCursor>,
    AgentControlProjectionRepositoryError
  >;
  readonly advance: (
    cursor: AgentControlProjectionCursor,
    expectedSequence: number,
  ) => Effect.Effect<void, AgentControlProjectionRepositoryError>;
  readonly deleteAll: Effect.Effect<void, AgentControlRepositoryError>;
}

export class AgentControlProjectionStateRepository extends Context.Service<
  AgentControlProjectionStateRepository,
  AgentControlProjectionStateRepositoryShape
>()("t3/persistence/Services/AgentControlProjectStates/AgentControlProjectionStateRepository") {}
