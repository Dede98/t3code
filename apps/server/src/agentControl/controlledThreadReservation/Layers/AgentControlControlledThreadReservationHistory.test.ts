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
  type AgentControlControlledThreadReservationEvent,
  type AgentControlControlledThreadReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { loadAuthoritativeControlledThreadReservationTaskHistory } from "../authoritative.ts";
import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
  type AgentControlControlledThreadStableIdentity,
} from "../identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../projector.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);
const at = "2026-07-26T10:00:00.000Z";
const encodeState = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const makeEvent = Effect.fn("makeControlledThreadReservationHistoryEvent")(function* (
  identity: AgentControlControlledThreadStableIdentity,
  suffix: string,
  sequence: number,
) {
  const controlledThreadReservationId =
    yield* deriveAgentControlControlledThreadReservationId(identity);
  const commandId = CommandId.make(`controlled-thread-history-command-${suffix}`);
  return {
    sequence,
    streamVersion: 1 as const,
    eventId: EventId.make(`controlled-thread-history-event-${suffix}`),
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
      threadId: yield* deriveAgentControlReservedThreadId(identity),
      ...identity,
      stageKind: "planning" as const,
      stageOrdinal: 1 as const,
      attemptOrdinal: 1 as const,
      leaseId: AgentControlStageRunLeaseId.make(`controlled-thread-history-lease-${suffix}`),
      fenceToken: 1,
      worktreeReservationId: AgentControlWorktreeReservationId.make(
        `controlled-thread-history-worktree-${suffix}`,
      ),
      status: "prepared" as const,
      preparedAt: at,
    },
  } satisfies AgentControlControlledThreadReservationEvent;
});

const eventDraft = ({
  sequence: _sequence,
  streamVersion: _streamVersion,
  ...draft
}: AgentControlControlledThreadReservationEvent) => draft;

const identity = (
  suffix: string,
  overrides: Partial<AgentControlControlledThreadStableIdentity> = {},
): AgentControlControlledThreadStableIdentity => ({
  projectId: ProjectId.make(`controlled-thread-history-project-${suffix}`),
  taskId: AgentControlTaskId.make(`controlled-thread-history-task-${suffix}`),
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: AgentControlStageRunId.make(`controlled-thread-history-stage-${suffix}`),
  attemptId: AgentControlAttemptId.make(`controlled-thread-history-attempt-${suffix}`),
  roleId: AgentControlRoleId.make("planning"),
  stageKind: "planning",
  stageOrdinal: 1,
  attemptOrdinal: 1,
  ...overrides,
});

layer("Controlled Thread reservation authoritative history", (it) => {
  it.effect("rejects event-only, projection-only, and consistently altered rows", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* AgentControlControlledThreadReservationEventStore;
      const states = yield* AgentControlControlledThreadReservationStateRepository;
      const engine = yield* AgentControlControlledThreadReservationEngine;
      const projection = yield* AgentControlControlledThreadReservationProjection;
      const healthyEvent = yield* makeEvent(identity("sqlite"), "sqlite", 1);
      const appended = yield* events.append({
        controlledThreadReservationId: healthyEvent.aggregateId,
        expectedStreamVersion: 0,
        events: [eventDraft(healthyEvent)],
      });
      yield* engine.rebuild;
      const healthy = Option.getOrThrow(yield* engine.getAuthoritative(healthyEvent.aggregateId));

      const altered = {
        ...healthy,
        threadId: "t3-auto-reserved-thread-consistently-altered",
      };
      yield* sql`
        DROP TRIGGER agent_control_controlled_thread_projection_validate_update
      `;
      yield* sql`
        UPDATE agent_control_controlled_thread_reservation_states
        SET thread_id = ${altered.threadId}, state_json = ${yield* encodeState(altered)}
        WHERE controlled_thread_reservation_id = ${healthy.controlledThreadReservationId}
      `;
      assert.equal(
        (yield* Effect.result(engine.getAuthoritative(healthy.controlledThreadReservationId)))._tag,
        "Failure",
      );

      yield* engine.rebuild;
      yield* sql`
        DELETE FROM agent_control_controlled_thread_reservation_states
        WHERE controlled_thread_reservation_id = ${healthy.controlledThreadReservationId}
      `;
      assert.equal((yield* Effect.result(projection.bootstrap))._tag, "Failure");
      assert.equal(
        (yield* Effect.result(engine.getAuthoritative(healthy.controlledThreadReservationId)))._tag,
        "Failure",
      );

      yield* engine.rebuild;
      const projectionOnlyEvent = yield* makeEvent(identity("projection-only"), "only", 999);
      const projectionOnly = yield* projectAgentControlControlledThreadReservationEvent(
        null,
        projectionOnlyEvent,
      );
      yield* sql`
        DROP TRIGGER agent_control_controlled_thread_projection_validate_insert
      `;
      yield* states.save(projectionOnly, 0);
      assert.equal(
        (yield* Effect.result(
          engine.validateTaskHistory(projectionOnly.projectId, projectionOnly.taskId),
        ))._tag,
        "Failure",
      );

      const catalogOnlyEvent = yield* makeEvent(identity("catalog-only"), "catalog-only", 1_001);
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_stream_catalog (
          controlled_thread_reservation_id, event_id, stream_version,
          command_id, event_type, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          prepared_at
        ) VALUES (
          ${catalogOnlyEvent.aggregateId}, ${catalogOnlyEvent.eventId}, 1,
          ${catalogOnlyEvent.commandId}, ${catalogOnlyEvent.type},
          ${catalogOnlyEvent.payload.threadId}, ${catalogOnlyEvent.payload.projectId},
          ${catalogOnlyEvent.payload.taskId}, ${catalogOnlyEvent.payload.taskRevision},
          ${catalogOnlyEvent.payload.githubIntakeSequence},
          ${catalogOnlyEvent.payload.sourceIdentityFingerprint},
          ${catalogOnlyEvent.payload.stageRunId}, ${catalogOnlyEvent.payload.attemptId},
          ${catalogOnlyEvent.payload.roleId}, ${catalogOnlyEvent.payload.stageKind},
          ${catalogOnlyEvent.payload.stageOrdinal},
          ${catalogOnlyEvent.payload.attemptOrdinal},
          ${catalogOnlyEvent.payload.leaseId}, ${catalogOnlyEvent.payload.fenceToken},
          ${catalogOnlyEvent.payload.worktreeReservationId},
          ${catalogOnlyEvent.payload.preparedAt}
        )
      `;
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.equal(
        (yield* Effect.result(
          engine.validateTaskHistory(
            catalogOnlyEvent.payload.projectId,
            catalogOnlyEvent.payload.taskId,
          ),
        ))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.result(engine.getAuthoritative(catalogOnlyEvent.aggregateId)))._tag,
        "Failure",
      );

      assert.equal(appended.length, 1);
    }),
  );
});

it.effect("rejects two individually valid reservations for one semantic position", () =>
  Effect.gen(function* () {
    const firstIdentity = identity("semantic");
    const secondIdentity = {
      ...firstIdentity,
      taskRevision: 2,
      githubIntakeSequence: 2,
      sourceIdentityFingerprint: "b".repeat(64),
    };
    const firstEvent = yield* makeEvent(firstIdentity, "semantic-first", 1);
    const secondEvent = yield* makeEvent(secondIdentity, "semantic-second", 2);
    const projected: ReadonlyArray<AgentControlControlledThreadReservationState> = [
      yield* projectAgentControlControlledThreadReservationEvent(null, firstEvent),
      yield* projectAgentControlControlledThreadReservationEvent(null, secondEvent),
    ];
    const result = yield* Effect.result(
      loadAuthoritativeControlledThreadReservationTaskHistory(
        firstIdentity.projectId,
        firstIdentity.taskId,
        {
          readGlobal: (after = 0) =>
            Effect.succeed([firstEvent, secondEvent].filter((event) => event.sequence > after)),
        },
        {
          listTask: () => Effect.succeed(projected),
        },
      ),
    );
    assert.equal(result._tag, "Failure");
  }),
);
