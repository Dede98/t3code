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
import { AgentControlGithubProjection } from "../github/Services/AgentControlGithubProjection.ts";
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
  const githubProjection = yield* AgentControlGithubProjection;

  const applyEvent = Effect.fn("AgentControlProjection.applyEvent")(function* (
    event: AgentControlEvent,
  ) {
    const cursorOption = yield* projectionStates.get(AGENT_CONTROL_PROJECT_MODE_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    // Global Agent Control sequences can contain events for other aggregate
    // kinds. This projector still requires monotonicity and relies on each
    // aggregate's streamVersion invariant to detect skipped controller events.
    if (event.sequence <= currentSequence) return yield* corrupt();

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
        if (event.sequence <= cursor) return yield* corrupt();
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
    yield* githubProjection.bootstrap;
  }).pipe(
    Effect.tap(() =>
      Effect.logDebug("Agent Control projections caught up", {
        projector: AGENT_CONTROL_PROJECT_MODE_PROJECTOR,
      }),
    ),
  );

  const rebuildController = sql
    .withTransaction(
      Effect.gen(function* () {
        // Deliberately scoped: events, receipts, policies, and all manual
        // orchestration and task projections remain untouched.
        yield* projectStates.deleteAll;
        yield* projectionStates.delete(AGENT_CONTROL_PROJECT_MODE_PROJECTOR);
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

  const rebuild: AgentControlProjectionShape["rebuild"] = rebuildController.pipe(
    Effect.andThen(githubProjection.rebuild),
  );

  return AgentControlProjection.of({ bootstrap, projectEvent, rebuild });
});

export const AgentControlProjectionLive = Layer.effect(
  AgentControlProjection,
  makeAgentControlProjection,
);
