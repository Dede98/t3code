import { AgentControlProjectionCorruptError, type AgentControlTaskEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import { AGENT_CONTROL_TASK_PROJECTOR, projectAgentControlTaskEvent } from "../projector.ts";
import {
  AgentControlTaskProjection,
  type AgentControlTaskProjectionShape,
} from "../Services/AgentControlTaskProjection.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";

const REPLAY_PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_TASK_PROJECTOR,
  });

const makeProjection = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlTaskEventStore;
  const states = yield* AgentControlTaskStateRepository;

  const getCursor = Effect.fn("AgentControlTaskProjection.getCursor")(function* () {
    const rows = yield* sql<{ readonly sequence: unknown }>`
      SELECT last_applied_sequence AS sequence
      FROM main.agent_control_projection_state
      WHERE projector_name = ${AGENT_CONTROL_TASK_PROJECTOR}
    `.pipe(
      Effect.mapError(
        (cause) =>
          new AgentControlPersistenceSqlError({
            operation: "AgentControlTaskProjection.getCursor",
            cause,
          }),
      ),
    );
    if (rows.length === 0) return Option.none<number>();
    const sequence = rows[0]?.sequence;
    if (
      rows.length !== 1 ||
      typeof sequence !== "number" ||
      !Number.isSafeInteger(sequence) ||
      sequence < 0
    ) {
      return yield* corrupt();
    }
    return Option.some(sequence);
  });

  const advanceCursor = Effect.fn("AgentControlTaskProjection.advanceCursor")(function* (
    sequence: number,
    updatedAt: string,
    expectedSequence: number,
  ) {
    if (sequence <= expectedSequence) return yield* corrupt();
    const rows =
      expectedSequence === 0
        ? yield* sql<{ readonly projectorName: unknown }>`
            INSERT INTO main.agent_control_projection_state (
              projector_name, last_applied_sequence, updated_at
            ) VALUES (${AGENT_CONTROL_TASK_PROJECTOR}, ${sequence}, ${updatedAt})
            ON CONFLICT (projector_name) DO NOTHING
            RETURNING projector_name AS "projectorName"
          `.pipe(
            Effect.mapError(
              (cause) =>
                new AgentControlPersistenceSqlError({
                  operation: "AgentControlTaskProjection.advanceCursor:insert",
                  cause,
                }),
            ),
          )
        : yield* sql<{ readonly projectorName: unknown }>`
            UPDATE main.agent_control_projection_state
            SET last_applied_sequence = ${sequence}, updated_at = ${updatedAt}
            WHERE projector_name = ${AGENT_CONTROL_TASK_PROJECTOR}
              AND last_applied_sequence = ${expectedSequence}
            RETURNING projector_name AS "projectorName"
          `.pipe(
            Effect.mapError(
              (cause) =>
                new AgentControlPersistenceSqlError({
                  operation: "AgentControlTaskProjection.advanceCursor:update",
                  cause,
                }),
            ),
          );
    if (rows.length !== 1) return yield* corrupt();
  });

  const deleteCursor = sql`
    DELETE FROM main.agent_control_projection_state
    WHERE projector_name = ${AGENT_CONTROL_TASK_PROJECTOR}
  `.pipe(
    Effect.mapError(
      (cause) =>
        new AgentControlPersistenceSqlError({
          operation: "AgentControlTaskProjection.deleteCursor",
          cause,
        }),
    ),
    Effect.asVoid,
  );

  const applyEvent = Effect.fn("AgentControlTaskProjection.applyEvent")(function* (
    event: AgentControlTaskEvent,
  ) {
    const cursorOption = yield* getCursor();
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (sequence) => sequence,
    });
    if (event.sequence <= currentSequence) return yield* corrupt();
    const currentOption = yield* states.get(event.aggregateId);
    const current = Option.getOrNull(currentOption);
    const next = yield* projectAgentControlTaskEvent(current, event);
    yield* states.save(next, current?.revision ?? 0);
    yield* advanceCursor(event.sequence, event.occurredAt, currentSequence);
  });

  const projectEvent: AgentControlTaskProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlTaskProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlTaskProjection.replayFrom")(function* (
    initialSequence: number,
  ) {
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
  });

  const bootstrap: AgentControlTaskProjectionShape["bootstrap"] = Effect.gen(function* () {
    const cursor = Option.match(yield* getCursor(), {
      onNone: () => 0,
      onSome: (value) => value,
    });
    const latest = yield* events.latestSequence;
    if (cursor > latest) return yield* corrupt();
    yield* replayFrom(cursor);
  }).pipe(
    Effect.tap(() =>
      Effect.logDebug("Agent Control task projection caught up", {
        projector: AGENT_CONTROL_TASK_PROJECTOR,
      }),
    ),
  );

  const rebuild: AgentControlTaskProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* states.deleteAll;
        yield* deleteCursor;
        yield* replayFrom(0);
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlTaskProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlTaskProjection.of({ bootstrap, projectEvent, rebuild });
});

export const layer = Layer.effect(AgentControlTaskProjection, makeProjection);
