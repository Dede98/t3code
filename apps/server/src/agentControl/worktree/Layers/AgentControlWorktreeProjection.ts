import {
  AgentControlProjectionCorruptError,
  AgentControlWorktreeReservationId,
  type AgentControlWorktreeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import { AGENT_CONTROL_WORKTREE_PROJECTOR } from "../invariant.ts";
import { projectAgentControlWorktreeEvent } from "../projector.ts";
import {
  AgentControlWorktreeProjection,
  type AgentControlWorktreeProjectionShape,
} from "../Services/AgentControlWorktreeProjection.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeStateRepository } from "../Services/AgentControlWorktreeStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_WORKTREE_PROJECTOR,
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlWorktreeEventStore;
  const states = yield* AgentControlWorktreeStateRepository;
  const cursors = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlWorktreeProjection.applyEvent")(function* (
    event: AgentControlWorktreeEvent,
  ) {
    const cursorOption = yield* cursors.get(AGENT_CONTROL_WORKTREE_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    if (event.sequence <= currentSequence) return yield* corrupt();
    const current = Option.getOrNull(yield* states.get(event.aggregateId));
    const next = yield* projectAgentControlWorktreeEvent(current, event);
    yield* states.save(next, current?.revision ?? 0);
    yield* cursors.advance(
      {
        projectorName: AGENT_CONTROL_WORKTREE_PROJECTOR,
        lastAppliedSequence: event.sequence,
        updatedAt: event.occurredAt,
      },
      currentSequence,
    );
  });

  const projectEvent: AgentControlWorktreeProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlWorktreeProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);

  const loadAllValidatedEvents = Effect.fn("AgentControlWorktreeProjection.loadAllValidatedEvents")(
    function* () {
      const streamIds = new Set(yield* events.readStreamIds);
      const projectionRows = yield* sql<{ readonly reservationId: unknown }>`
        SELECT reservation_id AS "reservationId"
        FROM agent_control_worktree_reservation_states
        ORDER BY reservation_id ASC
      `.pipe(
        Effect.mapError(
          (cause) =>
            new AgentControlPersistenceSqlError({
              operation: "AgentControlWorktreeProjection.loadAllValidatedEvents",
              cause,
            }),
        ),
      );
      for (const row of projectionRows) {
        const reservationId = yield* decodeReservationId(row.reservationId).pipe(
          Effect.mapError(() => corrupt()),
        );
        streamIds.add(reservationId);
      }
      const all: Array<AgentControlWorktreeEvent> = [];
      for (const reservationId of [...streamIds].toSorted()) {
        const stream = yield* events.readStreamSnapshot(reservationId);
        if (stream.length === 0) return yield* corrupt();
        all.push(...stream);
      }
      all.sort((left, right) => left.sequence - right.sequence);
      const sequences = new Set<number>();
      for (const event of all) {
        if (sequences.has(event.sequence)) return yield* corrupt();
        sequences.add(event.sequence);
      }
      const latest = yield* events.latestSequence;
      if ((all.at(-1)?.sequence ?? 0) !== latest) return yield* corrupt();
      return all;
    },
  );

  const replayValidated = Effect.fn("AgentControlWorktreeProjection.replayValidated")(function* (
    initialSequence: number,
    validated: ReadonlyArray<AgentControlWorktreeEvent>,
  ) {
    let cursor = initialSequence;
    for (const event of validated) {
      if (event.sequence <= initialSequence) continue;
      if (event.sequence <= cursor) return yield* corrupt();
      yield* projectEvent(event);
      cursor = event.sequence;
    }
    if (cursor !== (yield* events.latestSequence)) return yield* corrupt();
  });

  const bootstrap = Effect.gen(function* () {
    const cursor = Option.match(yield* cursors.get(AGENT_CONTROL_WORKTREE_PROJECTOR), {
      onNone: () => 0,
      onSome: (value) => value.lastAppliedSequence,
    });
    const latest = yield* events.latestSequence;
    if (cursor > latest) return yield* corrupt();
    const validated = yield* loadAllValidatedEvents();
    yield* replayValidated(cursor, validated);
  });

  const rebuild = sql
    .withTransaction(
      Effect.gen(function* () {
        const validated = yield* loadAllValidatedEvents();
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_WORKTREE_PROJECTOR);
        for (const event of validated) {
          yield* applyEvent(event);
        }
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlWorktreeProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlWorktreeProjection.of({ bootstrap, projectEvent, rebuild });
});

export const layer = Layer.effect(AgentControlWorktreeProjection, make);
