import {
  AgentControlProjectionCorruptError,
  type AgentControlStageRunLeaseEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import { AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR } from "../invariant.ts";
import { projectAgentControlStageRunLeaseEvent } from "../projector.ts";
import {
  AgentControlStageRunLeaseProjection,
  type AgentControlStageRunLeaseProjectionShape,
} from "../Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseEventStore } from "../Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

const REPLAY_PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
  });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlStageRunLeaseEventStore;
  const states = yield* AgentControlStageRunLeaseStateRepository;
  const cursors = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlStageRunLeaseProjection.applyEvent")(function* (
    event: AgentControlStageRunLeaseEvent,
  ) {
    const cursorOption = yield* cursors.get(AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    if (event.sequence <= currentSequence) return yield* corrupt();
    const current = Option.getOrNull(yield* states.get(event.aggregateId));
    const next = yield* projectAgentControlStageRunLeaseEvent(current, event);
    yield* states.save(next, current?.revision ?? 0);
    yield* cursors.advance(
      {
        projectorName: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
        lastAppliedSequence: event.sequence,
        updatedAt: event.occurredAt,
      },
      currentSequence,
    );
  });

  const projectEvent: AgentControlStageRunLeaseProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlStageRunLeaseProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlStageRunLeaseProjection.replayFrom")(function* (
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

  const bootstrap: AgentControlStageRunLeaseProjectionShape["bootstrap"] = Effect.gen(function* () {
    const cursor = Option.match(yield* cursors.get(AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR), {
      onNone: () => 0,
      onSome: (value) => value.lastAppliedSequence,
    });
    const latest = yield* events.latestSequence;
    if (cursor > latest) return yield* corrupt();
    yield* replayFrom(cursor);
  }).pipe(
    Effect.tap(() =>
      Effect.logDebug("Agent Control stage-run lease projection caught up", {
        projector: AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR,
      }),
    ),
  );

  const rebuild: AgentControlStageRunLeaseProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR);
        yield* replayFrom(0);
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlStageRunLeaseProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlStageRunLeaseProjection.of({ bootstrap, projectEvent, rebuild });
});

export const layer = Layer.effect(AgentControlStageRunLeaseProjection, make);
