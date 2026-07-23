import {
  AgentControlProjectionCorruptError,
  type AgentControlGithubEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlPersistenceSqlError } from "../../Errors.ts";
import {
  AGENT_CONTROL_GITHUB_PROJECTOR,
  createDefaultGithubIntakeState,
  projectGithubIntakeEvent,
} from "../projector.ts";
import {
  AgentControlGithubProjection,
  type AgentControlGithubProjectionShape,
} from "../Services/AgentControlGithubProjection.ts";
import { AgentControlGithubEventStore } from "../Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubStateRepository } from "../Services/AgentControlGithubStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";

const REPLAY_PAGE_SIZE = 500;
const corrupt = () =>
  new AgentControlProjectionCorruptError({
    code: "projection-corrupt",
    projector: AGENT_CONTROL_GITHUB_PROJECTOR,
  });

const makeProjection = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const events = yield* AgentControlGithubEventStore;
  const states = yield* AgentControlGithubStateRepository;
  const cursors = yield* AgentControlProjectionStateRepository;

  const applyEvent = Effect.fn("AgentControlGithubProjection.applyEvent")(function* (
    event: AgentControlGithubEvent,
  ) {
    const cursorOption = yield* cursors.get(AGENT_CONTROL_GITHUB_PROJECTOR);
    const currentSequence = Option.match(cursorOption, {
      onNone: () => 0,
      onSome: (cursor) => cursor.lastAppliedSequence,
    });
    if (event.sequence <= currentSequence) return yield* corrupt();
    const currentOption = yield* states.get(event.aggregateId);
    const current = Option.getOrElse(currentOption, () =>
      createDefaultGithubIntakeState(event.aggregateId),
    );
    const next = yield* projectGithubIntakeEvent(current, event);
    yield* states.save(next, current.revision);
    if (event.type === "agentControl.github.poll.succeeded") {
      yield* states.replaceIssues(event.aggregateId, event.payload.issues);
    } else if (event.type === "agentControl.github.poll.failed" && event.payload.invalidateCursor) {
      // Snapshots belong to the stable repository/issue binding. Once that
      // identity is suspect, retaining an eligible projection would not be
      // fail-closed even though the aggregate cursor has been invalidated.
      yield* states.deleteProject(event.aggregateId);
    } else if (
      event.type === "agentControl.github.config.set" ||
      event.type === "agentControl.github.config.cleared"
    ) {
      yield* states.deleteProject(event.aggregateId);
    }
    yield* cursors.advance(
      {
        projectorName: AGENT_CONTROL_GITHUB_PROJECTOR,
        lastAppliedSequence: event.sequence,
        updatedAt: event.occurredAt,
      },
      currentSequence,
    );
  });

  const projectEvent: AgentControlGithubProjectionShape["projectEvent"] = (event) =>
    sql.withTransaction(applyEvent(event)).pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlGithubProjection.projectEvent",
            cause,
          }),
        ),
      ),
    );

  const replayFrom = Effect.fn("AgentControlGithubProjection.replayFrom")(function* (
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

  const bootstrap: AgentControlGithubProjectionShape["bootstrap"] = Effect.gen(function* () {
    const cursor = Option.match(yield* cursors.get(AGENT_CONTROL_GITHUB_PROJECTOR), {
      onNone: () => 0,
      onSome: (value) => value.lastAppliedSequence,
    });
    const latest = yield* events.latestSequence;
    if (cursor > latest) return yield* corrupt();
    yield* replayFrom(cursor);
  });

  const rebuild: AgentControlGithubProjectionShape["rebuild"] = sql
    .withTransaction(
      Effect.gen(function* () {
        yield* states.deleteAll;
        yield* cursors.delete(AGENT_CONTROL_GITHUB_PROJECTOR);
        yield* replayFrom(0);
      }),
    )
    .pipe(
      Effect.catchTag("SqlError", (cause) =>
        Effect.fail(
          new AgentControlPersistenceSqlError({
            operation: "AgentControlGithubProjection.rebuild",
            cause,
          }),
        ),
      ),
    );

  return AgentControlGithubProjection.of({ bootstrap, projectEvent, rebuild });
});

export const layer = Layer.effect(AgentControlGithubProjection, makeProjection);
