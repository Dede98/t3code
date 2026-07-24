import type {
  AgentControlStageRunLeaseEvent,
  AgentControlStageRunLeaseEventDraft,
  AgentControlStageRunLeaseId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlStageRunLeaseEventStoreError } from "../../Errors.ts";

export interface AgentControlStageRunLeaseEventStoreShape {
  readonly append: (input: {
    readonly leaseId: AgentControlStageRunLeaseId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlStageRunLeaseEventDraft>;
  }) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunLeaseEvent>,
    AgentControlStageRunLeaseEventStoreError
  >;
  readonly readStream: (
    leaseId: AgentControlStageRunLeaseId,
    after?: number,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunLeaseEvent>,
    AgentControlStageRunLeaseEventStoreError
  >;
  readonly readGlobal: (
    after?: number,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunLeaseEvent>,
    AgentControlStageRunLeaseEventStoreError
  >;
  readonly latestSequence: Effect.Effect<number, AgentControlStageRunLeaseEventStoreError>;
}

export class AgentControlStageRunLeaseEventStore extends Context.Service<
  AgentControlStageRunLeaseEventStore,
  AgentControlStageRunLeaseEventStoreShape
>()("t3/agentControl/stageRunLease/Services/AgentControlStageRunLeaseEventStore") {}
