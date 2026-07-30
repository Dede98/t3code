import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

export interface AgentControlControlledThreadReservationTransactionHooksShape {
  readonly afterReadyInspection: Effect.Effect<void>;
  readonly beforeDbAdmission: Effect.Effect<void>;
  readonly afterDbAdmission: Effect.Effect<void>;
  readonly beforeEventAppend: Effect.Effect<void>;
  readonly afterWritesBeforeCommit: Effect.Effect<void>;
  readonly afterPrepareOuterCommit?: Effect.Effect<void>;
  readonly beforePrepareFinalizationRead?: Effect.Effect<void>;
  readonly beforePrepareReservationRefresh?: Effect.Effect<void>;
  readonly beforePreparePublication?: Effect.Effect<void>;
  readonly afterPreparePublicationBeforeCompletion?: Effect.Effect<void>;
  readonly beforePrepareCompletionCas?: Effect.Effect<void>;
}

const noopHooks: AgentControlControlledThreadReservationTransactionHooksShape = {
  afterReadyInspection: Effect.void,
  beforeDbAdmission: Effect.void,
  afterDbAdmission: Effect.void,
  beforeEventAppend: Effect.void,
  afterWritesBeforeCommit: Effect.void,
  afterPrepareOuterCommit: Effect.void,
  beforePrepareFinalizationRead: Effect.void,
  beforePrepareReservationRefresh: Effect.void,
  beforePreparePublication: Effect.void,
  afterPreparePublicationBeforeCompletion: Effect.void,
  beforePrepareCompletionCas: Effect.void,
};

/**
 * Production-bound no-op barriers. Focused SQLite race/rollback tests replace
 * this reference with Deferred-controlled effects at the exact commit points.
 */
export const AgentControlControlledThreadReservationTransactionHooks =
  Context.Reference<AgentControlControlledThreadReservationTransactionHooksShape>(
    "t3/agentControl/controlledThreadReservation/Services/AgentControlControlledThreadReservationTransactionHooks",
    { defaultValue: () => noopHooks },
  );
