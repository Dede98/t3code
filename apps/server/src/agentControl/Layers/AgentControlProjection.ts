import { AgentControlProjectionCorruptError, type AgentControlEvent } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../Errors.ts";
import { createDefaultAgentControlProjectState } from "../decider.ts";
import { AGENT_CONTROL_PROJECT_MODE_PROJECTOR, projectAgentControlEvent } from "../projector.ts";
import {
  AgentControlProjection,
  type AgentControlProjectionShape,
} from "../Services/AgentControlProjection.ts";
import { AgentControlEventStore } from "../../persistence/Services/AgentControlEventStore.ts";
import {
  AgentControlProjectionStateRepository,
  AgentControlProjectStateRepository,
} from "../../persistence/Services/AgentControlProjectStates.ts";

const REPLAY_PAGE_SIZE = 500;

const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
  });

const makeAgentControlProjection = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* AgentControlEventStore;
  const projectStates = yield* AgentControlProjectStateRepository;
  const projectionStates = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlProjection.applyEvent")(function* (
    event: AgentControlEvent,
  ) {
    const cursorOption = yield* projectionStates.get(AGENT_CONTROL_PROJECT_MODE_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    if (event.sequence !== currentSequence + 1) return yield* corrupt();

    const currentStateOption = yield* projectStates.get(event.aggregateId);
    const currentState = Option.getOrElse(currentStateOption, () =>
      createDefaultAgentControlProjectState(event.aggregateId),
    );
    const nextState = yield* projectAgentControlEvent(currentState, event);
    yield* projectStates.save(nextState, currentState.revision);
    yield* projectionStates.advance(
      {
        projectorName: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
        lastAppliedSequence: event.sequence,
        updatedAt: event.occurredAt,
      },
      currentSequence,
    );
  });

  const projectEvent: AgentControlProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlProjection.projectEvent:transaction",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlProjection.replayFrom")(function* (
    initialSequence: number,
  ) {
    let cursor = initialSequence;
    while (true) {
      const events = yield* eventStore.readGlobal(cursor, REPLAY_PAGE_SIZE);
      if (events.length === 0) break;
      for (const event of events) {
        if (event.sequence !== cursor + 1) return yield* corrupt();
        yield* projectEvent(event);
        cursor = event.sequence;
      }
    }
    const latestSequence = yield* eventStore.latestSequence;
    if (cursor !== latestSequence) return yield* corrupt();
  });

  const bootstrap: AgentControlProjectionShape["bootstrap"] = Effect.gen(function* () {
    const cursorOption = yield* projectionStates.get(AGENT_CONTROL_PROJECT_MODE_PROJECTOR);
    const cursor = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (state) => state.lastAppliedSequence,
    });
    const latestSequence = yield* eventStore.latestSequence;
    if (cursor > latestSequence) return yield* corrupt();
    yield* replayFrom(cursor);
  }).pipe(
    Effect.tap(() =>
      Effect.logDebug("Agent Control projections caught up", {
        projector: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
      }),
    ),
  );

  const rebuild: AgentControlProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        // Deliberately scoped: events, receipts, policies, and all manual
        // orchestration projections remain untouched.
        yield* projectStates.deleteAll;
        yield* projectionStates.deleteAll;
        yield* replayFrom(0);
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlProjection.rebuild:transaction",
            cause,
          }),
        ),
      ),
    );

  return AgentControlProjection.of({ bootstrap, projectEvent, rebuild });
});

export const AgentControlProjectionLive = Layer.effect(
  AgentControlProjection,
  makeAgentControlProjection,
);
