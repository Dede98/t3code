import type {
  AgentControlControlledThreadReservationEvent,
  AgentControlControlledThreadReservationEventDraft,
  AgentControlControlledThreadReservationId,
  AgentControlTaskId,
  ProjectId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";

import type { AgentControlControlledThreadReservationEventStoreError } from "../../Errors.ts";

export interface AgentControlControlledThreadReservationEventStoreShape {
  readonly append: (input: {
    readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlControlledThreadReservationEventDraft>;
  }) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>,
    AgentControlControlledThreadReservationEventStoreError
  >;
  /** Appends inside an already active caller-owned SQLite transaction. */
  readonly appendInTransaction: (input: {
    readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
    readonly expectedStreamVersion: number;
    readonly events: ReadonlyArray<AgentControlControlledThreadReservationEventDraft>;
  }) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>,
    AgentControlControlledThreadReservationEventStoreError
  >;
  readonly readStream: (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    afterStreamVersion?: number,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>,
    AgentControlControlledThreadReservationEventStoreError
  >;
  readonly readGlobal: (
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>,
    AgentControlControlledThreadReservationEventStoreError
  >;
  readonly readTask: (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
    afterSequence?: number,
    limit?: number,
  ) => Effect.Effect<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>,
    AgentControlControlledThreadReservationEventStoreError
  >;
  readonly latestSequence: Effect.Effect<
    number,
    AgentControlControlledThreadReservationEventStoreError
  >;
}

export class AgentControlControlledThreadReservationEventStore extends Context.Service<
  AgentControlControlledThreadReservationEventStore,
  AgentControlControlledThreadReservationEventStoreShape
>()(
  "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservationEventStore",
) {}
