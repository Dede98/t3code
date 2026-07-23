import type {
  AgentControlTaskEvent,
  AgentControlTaskEventDraft,
  AgentControlTaskId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlTaskEventStoreError } from "../../Errors.ts";

export interface AgentControlTaskEventStoreShape {
  readonly append: (input: {
    readonly taskId: AgentControlTaskId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlTaskEventDraft>;
  }) => Effect.Effect<ReadonlyArray<AgentControlTaskEvent>, AgentControlTaskEventStoreError>;
  readonly readStream: (
    taskId: AgentControlTaskId,
    afterStreamVersion?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlTaskEvent>, AgentControlTaskEventStoreError>;
  readonly readGlobal: (
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlTaskEvent>, AgentControlTaskEventStoreError>;
  readonly latestSequence: Effect.Effect<number, AgentControlTaskEventStoreError>;
}

export class AgentControlTaskEventStore extends Context.Service<
  AgentControlTaskEventStore,
  AgentControlTaskEventStoreShape
>()("t3/agentControl/task/Services/AgentControlTaskEventStore") {}
