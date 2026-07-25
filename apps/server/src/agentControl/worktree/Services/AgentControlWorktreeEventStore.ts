import type {
  AgentControlWorktreeEvent,
  AgentControlWorktreeEventDraft,
  AgentControlWorktreeReservationId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type {
  AgentControlRepositoryError,
  AgentControlWorktreeStreamVersionConflictError,
} from "../../Errors.ts";

export type AgentControlWorktreeEventStoreError =
  | AgentControlRepositoryError
  | AgentControlWorktreeStreamVersionConflictError;

export interface AgentControlWorktreeEventStoreShape {
  readonly append: (input: {
    readonly reservationId: AgentControlWorktreeReservationId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlWorktreeEventDraft>;
  }) => Effect.Effect<
    ReadonlyArray<AgentControlWorktreeEvent>,
    AgentControlWorktreeEventStoreError
  >;
  readonly readStream: (
    reservationId: AgentControlWorktreeReservationId,
    after?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlWorktreeEvent>, AgentControlWorktreeEventStoreError>;
  readonly readStreamSnapshot: (
    reservationId: AgentControlWorktreeReservationId,
  ) => Effect.Effect<ReadonlyArray<AgentControlWorktreeEvent>, AgentControlWorktreeEventStoreError>;
  readonly readStreamIds: Effect.Effect<
    ReadonlyArray<AgentControlWorktreeReservationId>,
    AgentControlWorktreeEventStoreError
  >;
  readonly readGlobal: (
    after?: number,
    limit?: number,
  ) => Effect.Effect<ReadonlyArray<AgentControlWorktreeEvent>, AgentControlWorktreeEventStoreError>;
  readonly latestSequence: Effect.Effect<number, AgentControlWorktreeEventStoreError>;
}

export class AgentControlWorktreeEventStore extends Context.Service<
  AgentControlWorktreeEventStore,
  AgentControlWorktreeEventStoreShape
>()("t3/agentControl/worktree/Services/AgentControlWorktreeEventStore") {}
