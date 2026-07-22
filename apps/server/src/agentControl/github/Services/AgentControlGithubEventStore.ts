import type {
  AgentControlGithubEvent,
  AgentControlGithubEventDraft,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlEventStoreError } from "../../Errors.ts";

export interface AgentControlGithubEventStoreShape {
  readonly append: (input: {
    readonly projectId: ProjectId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlGithubEventDraft>;
  }) => Effect.Effect<ReadonlyArray<AgentControlGithubEvent>, AgentControlEventStoreError>;
  readonly readStream: (
    projectId: ProjectId,
    afterStreamVersion?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlGithubEvent>, AgentControlEventStoreError>;
  readonly readGlobal: (
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlGithubEvent>, AgentControlEventStoreError>;
  readonly latestSequence: Effect.Effect<number, AgentControlEventStoreError>;
}

export class AgentControlGithubEventStore extends Context.Service<
  AgentControlGithubEventStore,
  AgentControlGithubEventStoreShape
>()("t3/agentControl/github/Services/AgentControlGithubEventStore") {}
