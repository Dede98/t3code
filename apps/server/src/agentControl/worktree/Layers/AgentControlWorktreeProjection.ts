import {
  AgentControlProjectionCorruptError,
  type AgentControlWorktreeEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
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

const PAGE_SIZE = 500;
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

  const replayFrom = Effect.fn("AgentControlWorktreeProjection.replayFrom")(function* (
    initialSequence: number,
  ) {
    let cursor = initialSequence;
    while (true) {
      const page = yield* events.readGlobal(cursor, PAGE_SIZE);
      if (page.length === 0) break;
      for (const event of page) {
        if (event.sequence <= cursor) return yield* corrupt();
        yield* projectEvent(event);
        cursor = event.sequence;
      }
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
    yield* replayFrom(cursor);
  });

  const assertClosedEventRelations = Effect.fn(
    "AgentControlWorktreeProjection.assertClosedEventRelations",
  )(function* () {
    const rows = yield* sql<{ readonly count: unknown }>`
      SELECT COUNT(*) AS count
      FROM (
        SELECT catalog.reservation_id
        FROM agent_control_worktree_stream_catalog AS catalog
        LEFT JOIN agent_control_worktree_event_envelopes AS envelope
          ON envelope.event_id = catalog.initial_event_id
         AND envelope.reservation_id = catalog.reservation_id
         AND envelope.stream_version = catalog.initial_stream_version
        LEFT JOIN agent_control_events AS event
          ON event.event_id = catalog.initial_event_id
         AND event.aggregate_kind = 'worktree-reservation'
         AND event.stream_id = catalog.reservation_id
         AND event.stream_version = catalog.initial_stream_version
        WHERE envelope.event_id IS NULL OR event.event_id IS NULL
        UNION ALL
        SELECT envelope.reservation_id
        FROM agent_control_worktree_event_envelopes AS envelope
        LEFT JOIN agent_control_worktree_stream_catalog AS catalog
          ON catalog.reservation_id = envelope.reservation_id
         AND catalog.project_id = envelope.project_id
         AND catalog.task_id = envelope.task_id
         AND catalog.stage_run_id = envelope.stage_run_id
         AND catalog.attempt_id = envelope.attempt_id
         AND catalog.lease_id = envelope.lease_id
         AND catalog.fence_token = envelope.fence_token
        LEFT JOIN agent_control_events AS event
          ON event.event_id = envelope.event_id
         AND event.aggregate_kind = 'worktree-reservation'
         AND event.stream_id = envelope.reservation_id
         AND event.stream_version = envelope.stream_version
         AND event.event_type = envelope.event_type
        WHERE catalog.reservation_id IS NULL OR event.event_id IS NULL
        UNION ALL
        SELECT event.stream_id
        FROM agent_control_events AS event
        LEFT JOIN agent_control_worktree_event_envelopes AS envelope
          ON envelope.event_id = event.event_id
         AND envelope.reservation_id = event.stream_id
         AND envelope.stream_version = event.stream_version
         AND envelope.event_type = event.event_type
        LEFT JOIN agent_control_worktree_stream_catalog AS catalog
          ON catalog.reservation_id = envelope.reservation_id
        WHERE event.aggregate_kind = 'worktree-reservation'
          AND (envelope.event_id IS NULL OR catalog.reservation_id IS NULL)
      )
    `;
    if (rows.length !== 1 || rows[0]?.count !== 0) return yield* corrupt();
  });

  const rebuild = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* assertClosedEventRelations();
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_WORKTREE_PROJECTOR);
        yield* replayFrom(0);
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
