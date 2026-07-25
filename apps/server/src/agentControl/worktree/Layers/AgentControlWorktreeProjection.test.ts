import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlWorktreeEventDraft,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { deriveAgentControlWorktreeReservationId } from "../identity.ts";
import { AGENT_CONTROL_WORKTREE_PROJECTOR } from "../invariant.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktree } from "../Services/AgentControlWorktree.ts";

const runtimeLayer = AgentControlRuntimeLayerLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(NodeServices.layer),
);
const layer = it.layer(runtimeLayer);
const at = "2026-07-24T10:00:00.000Z";
const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

const draft = Effect.fn("agentControlWorktreeRebuildDraft")(function* (
  projectId: ProjectId,
  index: number,
) {
  const taskId = AgentControlTaskId.make(`worktree-rebuild-task-${index}`);
  const stageRunId = AgentControlStageRunId.make(`worktree-rebuild-stage-${index}`);
  const attemptId = AgentControlAttemptId.make(`worktree-rebuild-attempt-${index}`);
  const leaseId = AgentControlStageRunLeaseId.make(`worktree-rebuild-lease-${index}`);
  const repository = {
    repositoryNodeId: "worktree-rebuild-repository",
    nameWithOwner: "owner/repository",
    canonicalKey: "github.com/owner/repository",
    remoteName: "origin",
    remoteUrl: "github.com/owner/repository",
    defaultRemoteRef: "refs/remotes/origin/main",
    commonDirDevice: 1,
    commonDirInode: 1,
  };
  const baseCommitSha = index.toString(16).padStart(40, "0");
  const reservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId,
    taskId,
    stageRunId,
    attemptId,
    leaseId,
    fenceToken: 1,
    repositoryIdentity: {
      repositoryNodeId: repository.repositoryNodeId,
      canonicalKey: repository.canonicalKey,
    },
    baseCommitSha,
  });
  const commandId = CommandId.make(`worktree-rebuild-command-${index}`);
  return {
    eventId: EventId.make(`worktree-rebuild-event-${index}`),
    type: "agentControl.worktree.reserved",
    aggregateKind: "worktree-reservation",
    aggregateId: reservationId,
    occurredAt: at,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      reservationId,
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: "a".repeat(64),
      stageRunId,
      attemptId,
      leaseId,
      fenceToken: 1,
      repository,
      repositoryWorkspace: "/tmp/worktree-rebuild-repository",
      repositoryCommonDir: "/tmp/worktree-rebuild-repository/.git",
      baseRef: "origin/main",
      baseCommitSha,
      branchName: `t3auto/issue-${index + 1}-rebuild-${index}`,
      internalWorktreePath: `/tmp/worktree-rebuild/${index}`,
      worktreeRootDevice: 1,
      worktreeRootInode: 1,
      worktreeParentDevice: 1,
      worktreeParentInode: 1,
      reservedAt: at,
    },
  } satisfies AgentControlWorktreeEventDraft;
});

layer("Agent Control worktree projection", (it) => {
  it.effect("rebuilds more than one page across sparse global sequences in isolation", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* AgentControlWorktreeEventStore;
      const projectId = ProjectId.make("worktree-rebuild-project");
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (${projectId}, '{"preserved":true}', 1, ${at})
      `;
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          ${projectId}, 'Projection test', '/tmp/worktree-rebuild-repository',
          NULL, '[]', ${at}, ${at}, NULL
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          reservation_id, worktree_reservation_id, status, result_json,
          rejection_code, created_at, updated_at, completed_at
        ) VALUES (
          'worktree-rebuild-pending-operation', 'reserve-and-materialize',
          ${"b".repeat(64)}, ${projectId}, 'pending-task', NULL, NULL,
          'pending', NULL, NULL, ${at}, ${at}, NULL
        )
      `;
      for (let index = 0; index < 501; index += 1) {
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            ${`foreign-event-${index}`}, 'task', ${`foreign-task-${index}`}, 1,
            'agentControl.task.created', ${at}, ${`foreign-command-${index}`},
            NULL, ${`foreign-command-${index}`}, 'controller', '{}', '{"schemaVersion":1}'
          )
        `;
        const event = yield* draft(projectId, index);
        yield* events.append({
          reservationId: event.aggregateId,
          expectedStreamVersion: 0,
          events: [event],
        });
      }
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'foreign-event-after-worktrees', 'task', 'foreign-task-after', 1,
          'agentControl.task.created', ${at}, 'foreign-command-after',
          NULL, 'foreign-command-after', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;

      yield* (yield* AgentControlWorktreeEngine).rebuild;

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_reservation_states
        `)[0]!.count,
        501,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_stream_catalog
        `)[0]!.count,
        501,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_project_policies
          WHERE project_id = ${projectId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_controller_operations
          WHERE command_id = 'worktree-rebuild-pending-operation' AND status = 'pending'
        `)[0]!.count,
        1,
      );
      const cursor = (yield* sql<{ readonly lastAppliedSequence: number }>`
        SELECT last_applied_sequence AS "lastAppliedSequence"
        FROM agent_control_projection_state
        WHERE projector_name = ${AGENT_CONTROL_WORKTREE_PROJECTOR}
      `)[0]!.lastAppliedSequence;
      assert.equal(cursor, yield* events.latestSequence);
      assert.isBelow(
        cursor,
        (yield* sql<{ readonly sequence: number }>`
          SELECT MAX(sequence) AS sequence FROM agent_control_events
        `)[0]!.sequence,
      );
      const reservationId = (yield* sql<{ readonly reservationId: string }>`
          SELECT reservation_id AS "reservationId"
          FROM agent_control_worktree_reservation_states
          ORDER BY reservation_id ASC LIMIT 1
        `)[0]!.reservationId;
      const decodedReservationId = AgentControlWorktreeReservationId.make(reservationId);
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET state_json = '{}'
        WHERE reservation_id = ${decodedReservationId}
      `;
      const corrupt = yield* Effect.result(
        (yield* AgentControlWorktreeEngine).loadAuthoritative(decodedReservationId),
      );
      assert.equal(corrupt._tag, "Failure");
      if (corrupt._tag === "Failure") {
        assert.equal(corrupt.failure.code, "reservation-projection-corrupt");
      }

      yield* (yield* AgentControlWorktreeEngine).rebuild;
      const splitCandidate = (yield* sql<{ readonly reservationId: string }>`
        SELECT reservation_id AS "reservationId"
        FROM agent_control_worktree_reservation_states
        ORDER BY reservation_id DESC LIMIT 1
      `)[0]!;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET reservation_id = 'projection-only-reservation-046'
        WHERE reservation_id = ${splitCandidate.reservationId}
      `;
      const splitEnumeration = yield* (yield* AgentControlWorktree).listReservations({
        projectId,
      });
      assert.equal(splitEnumeration.quarantinedCount, 2);
      assert.equal(splitEnumeration.reservations.length, 500);
      yield* (yield* AgentControlWorktreeEngine).rebuild;
      const ids = yield* sql<{ readonly reservationId: string }>`
        SELECT reservation_id AS "reservationId"
        FROM agent_control_worktree_reservation_states
        ORDER BY reservation_id ASC LIMIT 10
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET branch_name = 't3auto/issue-999-tampered',
          state_json = json_set(state_json, '$.branchName', 't3auto/issue-999-tampered')
        WHERE reservation_id = ${ids[0]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET status = 'materializing',
          state_json = json_set(state_json, '$.status', 'materializing')
        WHERE reservation_id = ${ids[1]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET base_commit_sha = ${"f".repeat(40)},
          state_json = json_set(state_json, '$.baseCommitSha', ${"f".repeat(40)})
        WHERE reservation_id = ${ids[2]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET repository_canonical_key = 'github.com/other/repository',
          state_json = json_set(
            state_json, '$.repository.canonicalKey', 'github.com/other/repository'
          )
        WHERE reservation_id = ${ids[3]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET lease_id = 'tampered-lease',
          state_json = json_set(state_json, '$.leaseId', 'tampered-lease')
        WHERE reservation_id = ${ids[4]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET fence_token = 2, state_json = json_set(state_json, '$.fenceToken', 2)
        WHERE reservation_id = ${ids[5]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET internal_worktree_path = '/tmp/tampered-path',
          state_json = json_set(state_json, '$.internalWorktreePath', '/tmp/tampered-path')
        WHERE reservation_id = ${ids[6]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET task_revision = 2, state_json = json_set(state_json, '$.taskRevision', 2)
        WHERE reservation_id = ${ids[7]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET github_intake_sequence = 2,
          state_json = json_set(state_json, '$.githubIntakeSequence', 2)
        WHERE reservation_id = ${ids[8]!.reservationId}
      `;
      yield* sql`
        UPDATE agent_control_worktree_reservation_states
        SET revision = 2, last_event_sequence = last_event_sequence + 10000,
          state_json = json_set(
            json_set(state_json, '$.revision', 2),
            '$.sequence', json_extract(state_json, '$.sequence') + 10000
          )
        WHERE reservation_id = ${ids[9]!.reservationId}
      `;
      const rpc = yield* AgentControlWorktree;
      const getCorrupt = yield* Effect.result(
        rpc.getReservation({
          projectId,
          reservationId: AgentControlWorktreeReservationId.make(ids[0]!.reservationId),
        }),
      );
      assert.equal(getCorrupt._tag, "Failure");
      if (getCorrupt._tag === "Failure") {
        assert.equal(getCorrupt.failure.code, "reservation-projection-corrupt");
      }
      const listed = yield* rpc.listReservations({ projectId });
      assert.equal(listed.quarantinedCount, 10);
      assert.equal(listed.reservations.length, 491);
      yield* sql`DROP TABLE agent_control_worktree_reservation_states`;
      const sqlFailure = yield* Effect.result(rpc.listReservations({ projectId }));
      assert.equal(sqlFailure._tag, "Failure");
      if (sqlFailure._tag === "Failure") {
        assert.equal(sqlFailure.failure.code, "internal-persistence-error");
      }
    }),
  );
});

const catalogLayer = it.layer(runtimeLayer);
catalogLayer("Agent Control worktree stream catalog", (it) => {
  it.effect("uses the immutable catalog to quarantine incomplete and competing streams", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const events = yield* AgentControlWorktreeEventStore;
      const engine = yield* AgentControlWorktreeEngine;
      const rpc = yield* AgentControlWorktree;
      const projectId = ProjectId.make("worktree-catalog-integrity-project");
      yield* sql`
        INSERT INTO projection_projects (
          project_id, title, workspace_root, default_model_selection_json,
          scripts_json, created_at, updated_at, deleted_at
        ) VALUES (
          ${projectId}, 'Catalog integrity', '/tmp/worktree-catalog-integrity',
          NULL, '[]', ${at}, ${at}, NULL
        )
      `;
      const first = yield* draft(projectId, 1_001);
      const healthy = yield* draft(projectId, 1_002);
      for (const event of [first, healthy]) {
        yield* events.append({
          reservationId: event.aggregateId,
          expectedStreamVersion: 0,
          events: [event],
        });
      }
      yield* engine.rebuild;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_stream_catalog
        `)[0]!.count,
        2,
      );

      const eventOnly = yield* draft(projectId, 1_003);
      yield* events.append({
        reservationId: eventOnly.aggregateId,
        expectedStreamVersion: 0,
        events: [eventOnly],
      });
      const competingId = AgentControlWorktreeReservationId.make(
        "worktree-catalog-competing-reservation",
      );
      yield* events.append({
        reservationId: competingId,
        expectedStreamVersion: 0,
        events: [
          {
            ...first,
            eventId: EventId.make("worktree-catalog-competing-event"),
            aggregateId: competingId,
            commandId: CommandId.make("worktree-catalog-competing-command"),
            correlationId: CommandId.make("worktree-catalog-competing-command"),
            payload: {
              ...first.payload,
              reservationId: competingId,
              branchName: "t3auto/issue-999-catalog-competing",
              internalWorktreePath: "/tmp/worktree-catalog-integrity/competing",
            },
          },
        ],
      });

      const catalogOnlyId = AgentControlWorktreeReservationId.make(
        "worktree-catalog-only-reservation",
      );
      const missingV1Id = AgentControlWorktreeReservationId.make(
        "worktree-catalog-missing-v1-reservation",
      );
      const corruptV1Id = AgentControlWorktreeReservationId.make(
        "worktree-catalog-corrupt-v1-reservation",
      );
      for (const [reservationId, suffix] of [
        [catalogOnlyId, "catalog-only"],
        [missingV1Id, "missing-v1"],
        [corruptV1Id, "corrupt-v1"],
      ] as const) {
        yield* sql`
          INSERT INTO agent_control_worktree_stream_catalog (
            reservation_id, project_id, task_id, stage_run_id, attempt_id,
            lease_id, fence_token, created_at
          ) VALUES (
            ${reservationId}, ${projectId}, ${`task-${suffix}`},
            ${`stage-${suffix}`}, ${`attempt-${suffix}`},
            ${`lease-${suffix}`}, 1, ${at}
          )
        `;
      }
      const missingV1Payload = yield* encodeJson({
        reservationId: missingV1Id,
        projectId,
        taskId: "task-missing-v1",
        stageRunId: "stage-missing-v1",
        attemptId: "attempt-missing-v1",
        leaseId: "lease-missing-v1",
        fenceToken: 1,
        transitionedAt: at,
      });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'worktree-catalog-missing-v1-event', 'worktree-reservation',
          ${missingV1Id}, 2, 'agentControl.worktree.materializationStarted',
          ${at}, 'worktree-catalog-missing-v1-command', NULL,
          'worktree-catalog-missing-v1-command', 'controller',
          ${missingV1Payload},
          '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'worktree-catalog-corrupt-v1-event', 'worktree-reservation',
          ${corruptV1Id}, 1, 'agentControl.worktree.reserved',
          ${at}, 'worktree-catalog-corrupt-v1-command', NULL,
          'worktree-catalog-corrupt-v1-command', 'controller', '{',
          '{"schemaVersion":1}'
        )
      `;
      const orphan = yield* draft(projectId, 1_009);
      const orphanPayload = yield* encodeJson(orphan.payload);
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'worktree-catalog-orphan-event', 'worktree-reservation',
          ${orphan.aggregateId}, 1, 'agentControl.worktree.reserved',
          ${at}, 'worktree-catalog-orphan-command', NULL,
          'worktree-catalog-orphan-command', 'controller',
          ${orphanPayload}, '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'worktree-catalog-foreign-aggregate', 'task', ${catalogOnlyId}, 1,
          'agentControl.task.created', ${at}, 'worktree-catalog-foreign-command',
          NULL, 'worktree-catalog-foreign-command', 'controller', '{}',
          '{"schemaVersion":1}'
        )
      `;

      const listed = yield* rpc.listReservations({ projectId });
      assert.deepEqual(
        listed.reservations.map((reservation) => reservation.reservationId),
        [healthy.aggregateId],
      );
      assert.equal(listed.quarantinedCount, 7);
    }),
  );
});
