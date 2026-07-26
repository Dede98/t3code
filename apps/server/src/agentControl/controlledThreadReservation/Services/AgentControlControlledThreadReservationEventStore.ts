import type {
  AgentControlControlledThreadReservationEvent,
  AgentControlControlledThreadReservationEventDraft,
  AgentControlControlledThreadReservationId,
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
