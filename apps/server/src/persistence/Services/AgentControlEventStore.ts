import type {
  AgentControlEvent,
  AgentControlProjectModeChangedEventDraft,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlEventStoreError } from "../../agentControl/Errors.ts";

export interface AgentControlAppendEventsInput {
  readonly projectId: ProjectId;
  readonly expectedStreamVersion: number;
  readonly events: ReadonlyArray<AgentControlProjectModeChangedEventDraft>;
}

export interface AgentControlEventStoreShape {
  /** Atomically appends the full batch or no events at all. */
  readonly append: (
    input: AgentControlAppendEventsInput,
  ) => Effect.Effect<ReadonlyArray<AgentControlEvent>, AgentControlEventStoreError>;
  readonly readStream: (
    projectId: ProjectId,
    afterStreamVersion?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlEvent>, AgentControlEventStoreError>;
  readonly readGlobal: (
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlEvent>, AgentControlEventStoreError>;
  readonly latestSequence: Effect.Effect<number, AgentControlEventStoreError>;
}

export class AgentControlEventStore extends Context.Service<
  AgentControlEventStore,
  AgentControlEventStoreShape
>()("t3/persistence/Services/AgentControlEventStore") {}
