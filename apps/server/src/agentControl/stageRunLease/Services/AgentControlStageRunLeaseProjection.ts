import type {
  AgentControlProjectionCorruptError,
  AgentControlStageRunLeaseEvent,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  AgentControlStageRunLeaseEventStoreError,
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";

export interface AgentControlStageRunLeaseProjectionShape {
  readonly bootstrap: Effect.Effect<
    void,
    | AgentControlPersistenceDecodeError
    | AgentControlPersistenceSqlError
    | AgentControlProjectionCorruptError
    | AgentControlStageRunLeaseEventStoreError
  >;
  readonly projectEvent: (
    event: AgentControlStageRunLeaseEvent,
  ) => Effect.Effect<
    void,
    | AgentControlPersistenceDecodeError
    | AgentControlPersistenceSqlError
    | AgentControlProjectionCorruptError
    | AgentControlStageRunLeaseEventStoreError
  >;
  readonly rebuild: Effect.Effect<
    void,
    | AgentControlPersistenceDecodeError
    | AgentControlPersistenceSqlError
    | AgentControlProjectionCorruptError
    | AgentControlStageRunLeaseEventStoreError
  >;
}

export class AgentControlStageRunLeaseProjection extends Context.Service<
  AgentControlStageRunLeaseProjection,
  AgentControlStageRunLeaseProjectionShape
>()("t3/agentControl/stageRunLease/Services/AgentControlStageRunLeaseProjection") {}
