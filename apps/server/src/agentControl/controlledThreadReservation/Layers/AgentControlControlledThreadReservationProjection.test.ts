import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../identity.ts";
import { AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR } from "../invariant.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);
const at = "2026-07-26T10:00:00.000Z";

const makeDraft = Effect.fn("makeControlledThreadReservationRebuildDraft")(function* (
  index: number,
) {
  const stable = {
    projectId: ProjectId.make("controlled-thread-rebuild-project"),
    taskId: AgentControlTaskId.make(`controlled-thread-rebuild-task-${index}`),
    taskRevision: 1,
    githubIntakeSequence: index + 1,
    sourceIdentityFingerprint: index.toString(16).padStart(64, "0"),
    stageRunId: AgentControlStageRunId.make(`controlled-thread-rebuild-stage-${index}`),
    attemptId: AgentControlAttemptId.make(`controlled-thread-rebuild-attempt-${index}`),
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1 as const,
    attemptOrdinal: 1 as const,
  };
  const controlledThreadReservationId =
    yield* deriveAgentControlControlledThreadReservationId(stable);
  const commandId = CommandId.make(`controlled-thread-rebuild-command-${index}`);
  return {
    controlledThreadReservationId,
    draft: {
      eventId: EventId.make(`controlled-thread-rebuild-event-${index}`),
      type: "agentControl.controlledThreadReservation.prepared" as const,
      aggregateKind: "controlled-thread-reservation" as const,
      aggregateId: controlledThreadReservationId,
      occurredAt: at,
      commandId,
      causationEventId: null,
      correlationId: commandId,
      authority: "controller" as const,
      metadata: { schemaVersion: 1 as const },
      payload: {
        controlledThreadReservationId,
        threadId: yield* deriveAgentControlReservedThreadId(stable),
        ...stable,
        leaseId: AgentControlStageRunLeaseId.make(`controlled-thread-rebuild-lease-${index}`),
        fenceToken: 1,
        worktreeReservationId: AgentControlWorktreeReservationId.make(
          `controlled-thread-rebuild-worktree-${index}`,
        ),
        status: "prepared" as const,
        preparedAt: at,
      },
    },
  };
});

layer("AgentControlControlledThreadReservationProjection", (it) => {
  it.effect("rebuilds more than 500 sparse global events without orchestration writes", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const eventStore = yield* AgentControlControlledThreadReservationEventStore;
      const engine = yield* AgentControlControlledThreadReservationEngine;
      for (let index = 0; index < 505; index += 1) {
        const event = yield* makeDraft(index);
        yield* eventStore.append({
          controlledThreadReservationId: event.controlledThreadReservationId,
          expectedStreamVersion: 0,
          events: [event.draft],
        });
        if (index === 250) {
          yield* sql`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            ) VALUES (
              'controlled-thread-rebuild-foreign', 'stage-run',
              'controlled-thread-rebuild-foreign-stage', 1,
              'agentControl.stageRun.prepared', ${at},
              'controlled-thread-rebuild-foreign-command', NULL,
              'controlled-thread-rebuild-foreign-command', 'controller',
              '{}', '{"schemaVersion":1}'
            )
          `;
        }
      }

      yield* engine.rebuild;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_reservation_states
        `)[0]!.count,
        505,
      );
      const cursor = (yield* sql<{ readonly lastAppliedSequence: number }>`
        SELECT last_applied_sequence AS "lastAppliedSequence"
        FROM agent_control_projection_state
        WHERE projector_name = ${AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR}
      `)[0]!.lastAppliedSequence;
      assert.equal(cursor, yield* eventStore.latestSequence);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM orchestration_events
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM projection_threads
        `)[0]!.count,
        0,
      );

      const corrupt = yield* makeDraft(501);
      const encodePayload = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
      yield* sql`
        UPDATE agent_control_events
        SET payload_json = ${yield* encodePayload({
          ...corrupt.draft.payload,
          threadId: "t3-auto-reserved-thread-corrupt",
        })}
        WHERE event_id = ${corrupt.draft.eventId}
      `;
      const failed = yield* Effect.result(engine.rebuild);
      assert.equal(failed._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_controlled_thread_reservation_states
        `)[0]!.count,
        505,
      );
      assert.equal(
        (yield* sql<{ readonly lastAppliedSequence: number }>`
          SELECT last_applied_sequence AS "lastAppliedSequence"
          FROM agent_control_projection_state
          WHERE projector_name = ${AGENT_CONTROL_CONTROLLED_THREAD_RESERVATION_PROJECTOR}
        `)[0]!.lastAppliedSequence,
        cursor,
      );
    }),
  );
});
