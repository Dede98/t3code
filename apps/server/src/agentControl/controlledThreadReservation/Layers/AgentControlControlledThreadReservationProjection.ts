import {
  AgentControlProjectionCorruptError,
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import { sameAgentControlControlledThreadReservationState } from "../authoritative.ts";
import {
  AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  validateAgentControlControlledThreadReservationState,
} from "../invariant.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../projector.ts";
import {
  AgentControlControlledThreadReservationProjection,
  type AgentControlControlledThreadReservationProjectionShape,
} from "../Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

const REPLAY_PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlControlledThreadReservationEventStore;
  const states = yield* AgentControlControlledThreadReservationStateRepository;
  const cursors = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlControlledThreadReservationProjection.applyEvent")(
    function* (event: AgentControlControlledThreadReservationEvent) {
      const cursorOption = yield* cursors.get(
        AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
      );
      const currentSequence = Option.match(cursorOption, {
        onNone: () => 0,
        onSome: (cursor) => cursor.lastAppliedSequence,
      });
      if (event.sequence <= currentSequence) return yield* corrupt();
      const current = Option.getOrNull(yield* states.get(event.aggregateId));
      const next = yield* projectAgentControlControlledThreadReservationEvent(current, event);
      yield* validateAgentControlControlledThreadReservationState(next);
      yield* states.save(next, current?.revision ?? 0);
      yield* cursors.advance(
        {
          projectorName: AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR,
          lastAppliedSequence: event.sequence,
          updatedAt: event.occurredAt,
        },
        currentSequence,
      );
    },
  );

  const projectEvent: AgentControlControlledThreadReservationProjectionShape["projectEvent"] = (
    event,
  ) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlControlledThreadReservationProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlControlledThreadReservationProjection.replayFrom")(
    function* (initialSequence: number) {
      let cursor = initialSequence;
      while (true) {
        const page = yield* events.readGlobal(cursor, REPLAY_PAGE_SIZE);
        if (page.length === 0) break;
        for (const event of page) {
          if (event.sequence <= cursor) return yield* corrupt();
          yield* projectEvent(event);
          cursor = event.sequence;
        }
      }
      if (cursor !== (yield* events.latestSequence)) return yield* corrupt();
    },
  );

  const validateCompleteHistory = Effect.fn(
    "AgentControlControlledThreadReservationProjection.validateCompleteHistory",
  )(function* () {
    const rebuilt = new Map<string, AgentControlControlledThreadReservationState>();
    let cursor = 0;
    while (true) {
      const page = yield* events.readGlobal(cursor, REPLAY_PAGE_SIZE);
      if (page.length === 0) break;
      for (const event of page) {
        if (event.sequence <= cursor || rebuilt.has(event.aggregateId)) {
          return yield* corrupt();
        }
        const state = yield* projectAgentControlControlledThreadReservationEvent(null, event);
        rebuilt.set(
          event.aggregateId,
          yield* validateAgentControlControlledThreadReservationState(state),
        );
        cursor = event.sequence;
      }
    }
    const projected = yield* states.listAll;
    if (projected.length !== rebuilt.size) return yield* corrupt();
    for (const state of projected) {
      const authoritative = rebuilt.get(state.controlledThreadReservationId);
      if (
        authoritative === undefined ||
        !sameAgentControlControlledThreadReservationState(authoritative, state)
      ) {
        return yield* corrupt();
      }
      rebuilt.delete(state.controlledThreadReservationId);
    }
    if (rebuilt.size !== 0) return yield* corrupt();
  });

  const bootstrap: AgentControlControlledThreadReservationProjectionShape["bootstrap"] = Effect.gen(
    function* () {
      const cursor = Option.match(
        yield* cursors.get(AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR),
        {
          onNone: () => 0,
          onSome: (value) => value.lastAppliedSequence,
        },
      );
      const latest = yield* events.latestSequence;
      if (cursor > latest) return yield* corrupt();
      yield* replayFrom(cursor);
      yield* validateCompleteHistory();
    },
  );

  const rebuild: AgentControlControlledThreadReservationProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR);
        yield* replayFrom(0);
        yield* validateCompleteHistory();
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlControlledThreadReservationProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlControlledThreadReservationProjection.of({
    bootstrap,
    projectEvent,
    rebuild,
  });
});

export const layer = Layer.effect(AgentControlControlledThreadReservationProjection, make);
