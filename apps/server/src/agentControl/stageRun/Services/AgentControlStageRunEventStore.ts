import type {
  AgentControlStageRunEvent,
  AgentControlStageRunEventDraft,
  AgentControlStageRunId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlStageRunEventStoreError } from "../../Errors.ts";

export interface AgentControlStageRunEventStoreShape {
  readonly append: (input: {
    readonly stageRunId: AgentControlStageRunId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlStageRunEventDraft>;
  }) => Effect.Effect<
    ReadonlyArray<AgentControlStageRunEvent>,
    AgentControlStageRunEventStoreError
  >;
  readonly readStream: (
    stageRunId: AgentControlStageRunId,
    afterStreamVersion?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlStageRunEvent>, AgentControlStageRunEventStoreError>;
  readonly readGlobal: (
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlStageRunEvent>, AgentControlStageRunEventStoreError>;
  readonly latestSequence: Effect.Effect<number, AgentControlStageRunEventStoreError>;
}

export class AgentControlStageRunEventStore extends Context.Service<
  AgentControlStageRunEventStore,
  AgentControlStageRunEventStoreShape
>()("t3/agentControl/stageRun/Services/AgentControlStageRunEventStore") {}
