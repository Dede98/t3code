import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  type AgentControlTaskEventDraft,
  type AgentControlWorktreeEventDraft,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import {
  canonicalJson,
  parseJsonStrict,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import { AgentControlRuntimeLayerLive } from "../runtimeLayer.ts";
import { AgentControlTaskEventStore } from "../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskProjection } from "../task/Services/AgentControlTaskProjection.ts";
import {
  deriveAgentControlWorktreePathKeys,
  deriveAgentControlWorktreeReservationId,
} from "../worktree/identity.ts";
import { AgentControlWorktreeEventStore } from "../worktree/Services/AgentControlWorktreeEventStore.ts";
import { projectAgentControlWorktreeEvent } from "../worktree/projector.ts";
import { AgentControlWorktreeStateRepository } from "../worktree/Services/AgentControlWorktreeStateRepository.ts";
import {
  loadAgentControlImplementationTaskAuthorityInTransaction,
  loadAgentControlImplementationWorktreeAuthorityInTransaction,
} from "./historicalAuthority.ts";

const at = "2026-09-02T12:00:00.000Z";
const projectId = ProjectId.make("implementation-history-project");
const taskId = AgentControlTaskId.make("implementation-history-task");
const stageRunId = AgentControlStageRunId.make("implementation-history-stage");
const attemptId = AgentControlAttemptId.make("implementation-history-attempt");
const leaseId = AgentControlStageRunLeaseId.make("implementation-history-lease");
const sourceIdentityFingerprint = "a".repeat(64);
const repository = {
  repositoryNodeId: "implementation-history-repository",
  nameWithOwner: "owner/repository",
  canonicalKey: "github.com/owner/repository",
  remoteName: "origin",
  remoteUrl: "github.com/owner/repository",
  defaultRemoteRef: "refs/remotes/origin/main",
  commonDirDevice: 1,
  commonDirInode: 1,
};
const baseCommitSha = "b".repeat(40);
const targetGenerationId = "c".repeat(64);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

const runtimeLayer = AgentControlRuntimeLayerLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(NodeServices.layer),
);
const layer = it.layer(runtimeLayer);

const expectFailure = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    assert.isTrue(Exit.isFailure(yield* Effect.exit(effect)));
  });

const dropUpdateGuards = Effect.fn("dropImplementationHistoryUpdateGuards")(function* (
  sql: SqlClient.SqlClient,
) {
  const rows = yield* sql<{ readonly name: string }>`
    SELECT name FROM main.sqlite_schema
    WHERE type='trigger'
      AND tbl_name IN (
        'agent_control_events', 'agent_control_task_states',
        'agent_control_worktree_stream_catalog',
        'agent_control_worktree_event_envelopes',
        'agent_control_worktree_reservation_states'
      )
      AND lower(sql) LIKE '%before update%'
  `;
  for (const { name } of rows) {
    assert.match(name, /^[A-Za-z0-9_]+$/u);
    yield* sql.unsafe(`DROP TRIGGER main.${name}`).unprepared;
  }
});

const seed = Effect.fn("seedImplementationHistoricalAuthority")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const taskEvents = yield* AgentControlTaskEventStore;
  const taskProjection = yield* AgentControlTaskProjection;
  const worktreeEvents = yield* AgentControlWorktreeEventStore;
  const worktreeStates = yield* AgentControlWorktreeStateRepository;
  yield* sql`
    INSERT INTO main.projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Implementation history', '/tmp/implementation-history',
      NULL, '[]', ${at}, ${at}, NULL
    )
  `;
  const taskCommandId = CommandId.make("implementation-history-task-command");
  const taskDraft = {
    eventId: EventId.make("implementation-history-task-event"),
    type: "agentControl.task.created",
    aggregateKind: "task",
    aggregateId: taskId,
    occurredAt: at,
    commandId: taskCommandId,
    causationEventId: null,
    correlationId: taskCommandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      taskId,
      source: {
        projectId,
        repositoryNodeId: repository.repositoryNodeId,
        issueNodeId: "implementation-history-issue",
        issueNumber: 1,
        issueUrl: "https://example.test/owner/repository/issues/1",
      },
      status: "candidate",
      sourceGate: "eligible",
      stage: "intake",
      sourceUpdatedAt: at,
      githubIntakeSequence: 1,
      sourceSnapshot: {
        repositoryNodeId: repository.repositoryNodeId,
        issueNodeId: "implementation-history-issue",
        number: 1,
        url: "https://example.test/owner/repository/issues/1",
        state: "open",
        title: "Implementation historical authority",
        body: null,
        contentTrust: "untrusted-external",
        updatedAt: at,
        timelineComplete: true,
        ready: true,
        paused: false,
        eligible: true,
        eligibilityReason: "eligible",
      },
      createdAt: at,
    },
  } satisfies AgentControlTaskEventDraft;
  const [taskEvent] = yield* taskEvents.append({
    taskId,
    expectedStreamVersion: 0,
    events: [taskDraft],
  });
  yield* taskProjection.projectEvent(taskEvent!);

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
  const worktreeCommandId = CommandId.make("implementation-history-worktree-command");
  const pathKeys = deriveAgentControlWorktreePathKeys({
    projectId,
    reservationId,
    targetGenerationId,
  });
  const worktreeDraft = {
    eventId: EventId.make("implementation-history-worktree-event"),
    type: "agentControl.worktree.reserved",
    aggregateKind: "worktree-reservation",
    aggregateId: reservationId,
    occurredAt: at,
    commandId: worktreeCommandId,
    causationEventId: null,
    correlationId: worktreeCommandId,
    authority: "controller",
    metadata: { schemaVersion: 1 },
    payload: {
      reservationId,
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      stageRunId,
      attemptId,
      leaseId,
      fenceToken: 1,
      repository,
      repositoryWorkspace: "/tmp/implementation-history-repository",
      repositoryCommonDir: "/tmp/implementation-history-repository/.git",
      baseRef: "origin/main",
      baseCommitSha,
      branchName: "t3auto/issue-1-implementation-history",
      internalWorktreePath: `/tmp/implementation-history/${pathKeys.reservationKey}-${pathKeys.generationKey}`,
      targetGenerationId,
      worktreeRootDevice: 1,
      worktreeRootInode: 1,
      worktreeParentDevice: 1,
      worktreeParentInode: 1,
      reservedAt: at,
    },
  } satisfies AgentControlWorktreeEventDraft;
  yield* sql`PRAGMA foreign_keys=OFF`;
  const [worktreeEvent] = yield* worktreeEvents.append({
    reservationId,
    expectedStreamVersion: 0,
    events: [worktreeDraft],
  });
  const worktreeState = yield* projectAgentControlWorktreeEvent(null, worktreeEvent!);
  yield* worktreeStates.save(worktreeState, 0);
  return { sql, reservationId } as const;
});

layer("implementation historical authority", (it) => {
  it.effect("accepts only typed legacy/canonical bytes and fails closed on corrupt authority", () =>
    Effect.gen(function* () {
      const { sql, reservationId } = yield* seed();
      assert.equal(
        (yield* loadAgentControlImplementationTaskAuthorityInTransaction(sql, taskId, 1)).state
          .taskId,
        taskId,
      );
      assert.equal(
        (yield* loadAgentControlImplementationWorktreeAuthorityInTransaction(sql, reservationId))
          .state.reservationId,
        reservationId,
      );

      const sources = yield* sql<{
        readonly aggregateKind: "task" | "worktree-reservation";
        readonly payload: string;
        readonly metadata: string;
      }>`
        SELECT aggregate_kind AS "aggregateKind", payload_json AS payload,
          metadata_json AS metadata
        FROM main.agent_control_events
        WHERE event_id IN ('implementation-history-task-event',
          'implementation-history-worktree-event')
        ORDER BY aggregate_kind
      `;
      const taskSource = sources.find((row) => row.aggregateKind === "task")!;
      const worktreeSource = sources.find((row) => row.aggregateKind === "worktree-reservation")!;
      const taskCanonical = canonicalJson(parseJsonStrict(taskSource.payload));
      const worktreeCanonical = canonicalJson(parseJsonStrict(worktreeSource.payload));
      yield* dropUpdateGuards(sql);
      yield* sql`
        UPDATE main.agent_control_events
        SET payload_json=CASE aggregate_kind
              WHEN 'task' THEN ${taskCanonical} ELSE ${worktreeCanonical} END,
            metadata_json=${canonicalJson(parseJsonStrict(taskSource.metadata))}
        WHERE event_id IN ('implementation-history-task-event',
          'implementation-history-worktree-event')
      `;
      yield* loadAgentControlImplementationTaskAuthorityInTransaction(sql, taskId, 1);
      yield* loadAgentControlImplementationWorktreeAuthorityInTransaction(sql, reservationId);

      const taskObject = parseJsonStrict(taskCanonical) as Record<string, JsonValue>;
      const thirdOrder = encodeUnknownJson(
        Object.fromEntries(Object.entries(taskObject).toReversed()),
      );
      const invalidTaskSources = [
        `${taskCanonical} `,
        `{"taskId":${encodeUnknownJson(taskId)},${taskCanonical.slice(1)}`,
        canonicalJson({ ...taskObject, unexpectedAuthority: "forbidden" }),
        thirdOrder,
      ];
      for (const source of invalidTaskSources) {
        yield* sql`
          UPDATE main.agent_control_events SET payload_json=${source}
          WHERE event_id='implementation-history-task-event'
        `;
        yield* expectFailure(
          loadAgentControlImplementationTaskAuthorityInTransaction(sql, taskId, 1),
        );
      }
      yield* sql`
        UPDATE main.agent_control_events SET payload_json=CAST(${taskCanonical} AS BLOB)
        WHERE event_id='implementation-history-task-event'
      `;
      yield* expectFailure(
        loadAgentControlImplementationTaskAuthorityInTransaction(sql, taskId, 1),
      );
      yield* sql`
        UPDATE main.agent_control_events SET payload_json=${taskCanonical}
        WHERE event_id='implementation-history-task-event'
      `;

      yield* sql`
        UPDATE main.agent_control_worktree_event_envelopes SET task_id='divergent-task'
        WHERE reservation_id=${reservationId}
      `;
      yield* expectFailure(
        loadAgentControlImplementationWorktreeAuthorityInTransaction(sql, reservationId),
      );
      yield* sql`
        UPDATE main.agent_control_worktree_event_envelopes SET task_id=${taskId}
        WHERE reservation_id=${reservationId}
      `;
      yield* sql`
        UPDATE main.agent_control_worktree_stream_catalog SET fence_token=2
        WHERE reservation_id=${reservationId}
      `;
      yield* expectFailure(
        loadAgentControlImplementationWorktreeAuthorityInTransaction(sql, reservationId),
      );
      yield* sql`
        UPDATE main.agent_control_worktree_stream_catalog SET fence_token=1
        WHERE reservation_id=${reservationId}
      `;
      yield* sql`
        UPDATE main.agent_control_worktree_reservation_states
        SET repository_node_id='divergent-repository'
        WHERE reservation_id=${reservationId}
      `;
      yield* expectFailure(
        loadAgentControlImplementationWorktreeAuthorityInTransaction(sql, reservationId),
      );
    }),
  );
});
