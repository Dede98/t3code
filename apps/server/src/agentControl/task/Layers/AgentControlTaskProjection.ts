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
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

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
  const cursors = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlTaskProjection.applyEvent")(function* (
    event: AgentControlTaskEvent,
  ) {
    const cursorOption = yield* cursors.get(AGENT_CONTROL_TASK_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    if (event.sequence <= currentSequence) return yield* corrupt();
    const currentOption = yield* states.get(event.aggregateId);
    const current = Option.getOrNull(currentOption);
    const next = yield* projectAgentControlTaskEvent(current, event);
    yield* states.save(next, current?.revision ?? 0);
    yield* cursors.advance(
      {
        projectorName: AGENT_CONTROL_TASK_PROJECTOR,
        lastAppliedSequence: event.sequence,
        updatedAt: event.occurredAt,
      },
      currentSequence,
    );
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
    const cursor = Option.match(yield* cursors.get(AGENT_CONTROL_TASK_PROJECTOR), {
      onNone: () => 0,
      onSome: (value) => value.lastAppliedSequence,
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
        yield* cursors.delete(AGENT_CONTROL_TASK_PROJECTOR);
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
