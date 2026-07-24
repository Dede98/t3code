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
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { deriveAgentControlWorktreeReservationId } from "../identity.ts";
import { AGENT_CONTROL_WORKTREE_PROJECTOR } from "../invariant.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);
const at = "2026-07-24T10:00:00.000Z";

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
          SELECT COUNT(*) AS count FROM agent_control_project_policies
          WHERE project_id = ${projectId}
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
    }),
  );
});
