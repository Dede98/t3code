import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import {
  AGENT_CONTROL_STAGE_RUN_PROJECTOR,
  projectAgentControlStageRunEvent,
} from "../projector.ts";
import {
  AgentControlStageRunProjection,
  type AgentControlStageRunProjectionShape,
} from "../Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunEventStore } from "../Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../Services/AgentControlStageRunStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

const REPLAY_PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_PROJECTOR,
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlStageRunEventStore;
  const states = yield* AgentControlStageRunStateRepository;
  const cursors = yield* AgentControlProjectionStateRepository;

  const getMainCursor = sql<{ readonly lastAppliedSequence: unknown }>`
    SELECT last_applied_sequence AS "lastAppliedSequence"
    FROM main.agent_control_projection_state
    WHERE projector_name = ${AGENT_CONTROL_STAGE_RUN_PROJECTOR}
  `.pipe(
    Effect.mapError(
      (cause) =>
        new AgentControlPersistenceSqlError({
          operation: "AgentControlStageRunProjection.mainCursor:get",
          cause,
        }),
    ),
    Effect.flatMap((rows) => {
      const value = rows[0]?.lastAppliedSequence;
      if (value === undefined) return Effect.succeed(0);
      return typeof value === "number" && Number.isSafeInteger(value) && value >= 0
        ? Effect.succeed(value)
        : Effect.fail(corrupt());
    }),
  );

  const advanceMainCursor = (sequence: number, occurredAt: string, expectedSequence: number) =>
    (expectedSequence === 0
      ? sql<{ readonly projectorName: unknown }>`
          INSERT INTO main.agent_control_projection_state (
            projector_name, last_applied_sequence, updated_at
          ) VALUES (
            ${AGENT_CONTROL_STAGE_RUN_PROJECTOR}, ${sequence}, ${occurredAt}
          )
          ON CONFLICT (projector_name) DO NOTHING
          RETURNING projector_name AS "projectorName"
        `
      : sql<{ readonly projectorName: unknown }>`
          UPDATE main.agent_control_projection_state
          SET last_applied_sequence = ${sequence}, updated_at = ${occurredAt}
          WHERE projector_name = ${AGENT_CONTROL_STAGE_RUN_PROJECTOR}
            AND last_applied_sequence = ${expectedSequence}
          RETURNING projector_name AS "projectorName"
        `
    ).pipe(
      Effect.mapError(
        (cause) =>
          new AgentControlPersistenceSqlError({
            operation: "AgentControlStageRunProjection.mainCursor:advance",
            cause,
          }),
      ),
      Effect.flatMap((rows) => (rows.length === 1 ? Effect.void : Effect.fail(corrupt()))),
    );

  const applyEvent = Effect.fn("AgentControlStageRunProjection.applyEvent")(function* (
    event: AgentControlStageRunEvent,
  ) {
    const currentSequence = yield* getMainCursor;
    if (event.sequence <= currentSequence) return yield* corrupt();
    const current = Option.getOrNull(yield* states.get(event.aggregateId));
    const next = yield* projectAgentControlStageRunEvent(current, event);
    yield* states.save(next, current?.revision ?? 0);
    yield* advanceMainCursor(event.sequence, event.occurredAt, currentSequence);
  });

  const projectEvent: AgentControlStageRunProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlStageRunProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlStageRunProjection.replayFrom")(function* (
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

  const bootstrap: AgentControlStageRunProjectionShape["bootstrap"] = Effect.gen(function* () {
    const cursor = Option.match(yield* cursors.get(AGENT_CONTROL_STAGE_RUN_PROJECTOR), {
      onNone: () => 0,
      onSome: (value) => value.lastAppliedSequence,
    });
    const latest = yield* events.latestSequence;
    if (cursor > latest) return yield* corrupt();
    yield* replayFrom(cursor);
  }).pipe(
    Effect.tap(() =>
      Effect.logDebug("Agent Control stage-run projection caught up", {
        projector: AGENT_CONTROL_STAGE_RUN_PROJECTOR,
      }),
    ),
  );

  const rebuild: AgentControlStageRunProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_STAGE_RUN_PROJECTOR);
        yield* replayFrom(0);
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlStageRunProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlStageRunProjection.of({ bootstrap, projectEvent, rebuild });
});

export const layer = Layer.effect(AgentControlStageRunProjection, make);
