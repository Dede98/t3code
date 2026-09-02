import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson } from "../initialPlanning/eventEvidence.ts";
import { fingerprintRunOnceModeCommand } from "../runOnce/authority.ts";
import { claimArmedDispatch } from "./authority.ts";

const at = "2026-09-02T08:00:00.000Z";
const later = "2026-09-02T08:00:01.000Z";
const expiresAt = "2026-09-02T08:01:00.000Z";
const afterExpiry = "2026-09-02T08:01:01.000Z";
const reclaimedExpiresAt = "2026-09-02T08:02:00.000Z";
const afterReclaimedExpiry = "2026-09-02T08:02:01.000Z";
const foreignReclaimedExpiresAt = "2026-09-02T08:03:00.000Z";
const projectId = ProjectId.make("armed-authority-project");

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const seedArmedSource = Effect.fn("seedArmedSource")(function* (sql: SqlClient.SqlClient) {
  const insert = Effect.fn("seedArmedEvent")(function* (input: {
    readonly eventId: string;
    readonly aggregateKind: string;
    readonly streamVersion: number;
    readonly eventType: string;
    readonly commandId: string;
    readonly authority: string;
    readonly payload: Record<string, unknown>;
  }) {
    return (yield* sql<{ readonly sequence: number }>`
      INSERT INTO main.agent_control_events (
        event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
        command_id, causation_event_id, correlation_id, actor_authority,
        payload_json, metadata_json
      ) VALUES (
        ${input.eventId}, ${input.aggregateKind}, ${projectId}, ${input.streamVersion},
        ${input.eventType}, ${at}, ${input.commandId}, NULL, ${input.commandId},
        ${input.authority}, ${canonicalJson(input.payload as never)}, '{"schemaVersion":1}'
      ) RETURNING sequence
    `)[0]!.sequence;
  });
  yield* insert({
    eventId: "armed-authority-observe-event",
    aggregateKind: "project-controller",
    streamVersion: 1,
    eventType: "agentControl.project.mode.changed",
    commandId: "armed-authority-observe-command",
    authority: "human",
    payload: {
      projectId,
      previousMode: "manual",
      mode: "observe",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  const projectSequence = yield* insert({
    eventId: "armed-authority-armed-event",
    aggregateKind: "project-controller",
    streamVersion: 2,
    eventType: "agentControl.project.mode.changed",
    commandId: "armed-authority-armed-command",
    authority: "human",
    payload: {
      projectId,
      previousMode: "observe",
      mode: "armed",
      previousPausedFromMode: null,
      pausedFromMode: null,
      changedAt: at,
    },
  });
  yield* insert({
    eventId: "armed-authority-github-config-event",
    aggregateKind: "github-intake",
    streamVersion: 1,
    eventType: "agentControl.github.config.set",
    commandId: "armed-authority-github-config-command",
    authority: "human",
    payload: {
      projectId,
      repository: { repositoryNodeId: "armed-repository", nameWithOwner: "owner/repo" },
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      configuredAt: at,
    },
  });
  const githubSequence = yield* insert({
    eventId: "armed-authority-github-poll-event",
    aggregateKind: "github-intake",
    streamVersion: 2,
    eventType: "agentControl.github.poll.succeeded",
    commandId: "armed-authority-github-poll-command",
    authority: "controller",
    payload: {
      projectId,
      repository: { repositoryNodeId: "armed-repository", nameWithOwner: "owner/repo" },
      attemptedAt: at,
      completedAt: at,
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
      issues: [],
    },
  });
  yield* sql`
    INSERT INTO main.agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'armed', NULL, 2, ${projectSequence}, ${at})
  `;
  const githubState = canonicalJson({
    schemaVersion: 1,
    projectId,
    config: {
      schemaVersion: 1,
      projectId,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: [],
        pollIntervalSeconds: 60,
      },
      repository: { repositoryNodeId: "armed-repository", nameWithOwner: "owner/repo" },
      revision: 2,
      sequence: githubSequence,
      updatedAt: at,
    },
    cursor: { lastSuccessfulPollAt: at, overlapSeconds: 120 },
    pollStatus: {
      status: "success",
      attemptedAt: at,
      completedAt: at,
      errorCode: null,
      issueCount: 0,
    },
    revision: 2,
    sequence: githubSequence,
    updatedAt: at,
  } as never);
  yield* sql`
    INSERT INTO main.agent_control_github_intake_states (
      project_id, state_json, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, ${githubState}, 2, ${githubSequence}, ${at})
  `;
  yield* sql`
    INSERT INTO main.agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence, revision, status, updated_at
    ) VALUES (${projectId}, ${githubSequence}, ${githubSequence}, 1, 'completed', ${at})
  `;
  return githubSequence;
});

const insertCandidate = Effect.fn("insertArmedCandidate")(function* (
  sql: SqlClient.SqlClient,
  githubSequence: number,
) {
  yield* sql`
    INSERT INTO main.agent_control_task_states (
      task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
      status, source_gate, stage, source_updated_at, github_intake_sequence, state_json,
      created_at, updated_at, revision, last_event_sequence
    ) VALUES (
      'armed-task-1', ${projectId}, 'armed-repository', 'armed-issue-1', 1,
      'https://github.test/owner/repo/issues/1', 'candidate', 'eligible', 'intake',
      ${later}, ${githubSequence}, '{}', ${later}, ${later}, 1, ${githubSequence + 1}
    )
  `;
});

it.live("persists one decision per epoch and grants one claim across two WAL connections", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-authority-" });
      const filename = path.join(directory, "authority.sqlite");
      const first = yield* openDatabase(filename);
      const second = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(second.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(first.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
        Effect.provideService(SqlClient.SqlClient, first.sql),
      );
      const githubSequence = yield* seedArmedSource(first.sql);

      const none = yield* claimArmedDispatch(first.sql, {
        projectId,
        ownerId: "controller-none",
        claimedAt: at,
        expiresAt,
      });
      assert.deepStrictEqual(none, { _tag: "no-candidate", replayed: false });
      const noneReplay = yield* claimArmedDispatch(second.sql, {
        projectId,
        ownerId: "controller-none-replay",
        claimedAt: later,
        expiresAt,
      });
      assert.deepStrictEqual(noneReplay, { _tag: "no-candidate", replayed: true });
      assert.deepStrictEqual(
        yield* first.sql`
          SELECT
            (SELECT count(*) FROM main.agent_control_armed_no_candidate_evidence) AS evidence,
            (SELECT count(*) FROM main.agent_control_armed_no_candidate_receipts) AS receipts,
            (SELECT count(*) FROM main.agent_control_armed_no_candidate_markers) AS markers
        `,
        [{ evidence: 1, receipts: 1, markers: 1 }],
      );

      yield* insertCandidate(first.sql, githubSequence);
      const raced = yield* Effect.all(
        [
          claimArmedDispatch(first.sql, {
            projectId,
            ownerId: "controller-a",
            claimedAt: later,
            expiresAt,
          }),
          claimArmedDispatch(second.sql, {
            projectId,
            ownerId: "controller-b",
            claimedAt: later,
            expiresAt,
          }),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(raced.filter((result) => result._tag === "dispatch").length, 1);
      assert.equal(raced.filter((result) => result._tag === "busy").length, 1);
      assert.equal(raced.find((result) => result._tag === "busy")?.retryAt, expiresAt);
      assert.deepStrictEqual(
        yield* first.sql`
          SELECT
            (SELECT count(*) FROM main.agent_control_armed_dispatch_evidence) AS evidence,
            (SELECT count(*) FROM main.agent_control_armed_dispatch_receipts) AS receipts,
            (SELECT count(*) FROM main.agent_control_armed_dispatch_states
              WHERE status = 'claimed') AS states,
            (SELECT count(*) FROM main.agent_control_armed_dispatch_markers) AS markers
        `,
        [{ evidence: 1, receipts: 1, states: 1, markers: 1 }],
      );
      const winner = raced.find((result) => result._tag === "dispatch");
      assert.isDefined(winner);
      const reclaimed = yield* claimArmedDispatch(second.sql, {
        projectId,
        ownerId: winner!.dispatch.ownerId,
        claimedAt: afterExpiry,
        expiresAt: reclaimedExpiresAt,
      });
      assert.equal(reclaimed._tag, "dispatch");
      if (reclaimed._tag === "dispatch") {
        assert.equal(reclaimed.replayed, false);
        assert.notEqual(reclaimed.dispatch.dispatchId, winner!.dispatch.dispatchId);
        assert.notEqual(reclaimed.dispatch.commandId, winner!.dispatch.commandId);
        assert.equal(reclaimed.dispatch.ownerId, winner!.dispatch.ownerId);
        assert.equal(reclaimed.dispatch.fenceToken, 2);
        assert.equal(reclaimed.dispatch.expiresAt, reclaimedExpiresAt);
      }
      const fencedOut = yield* claimArmedDispatch(first.sql, {
        projectId,
        ownerId: "controller-foreign",
        claimedAt: afterExpiry,
        expiresAt: reclaimedExpiresAt,
      });
      assert.deepStrictEqual(fencedOut, { _tag: "busy", retryAt: reclaimedExpiresAt });
      const foreignReclaimed = yield* claimArmedDispatch(first.sql, {
        projectId,
        ownerId: "controller-foreign",
        claimedAt: afterReclaimedExpiry,
        expiresAt: foreignReclaimedExpiresAt,
      });
      assert.equal(foreignReclaimed._tag, "dispatch");
      if (foreignReclaimed._tag === "dispatch") {
        assert.equal(foreignReclaimed.replayed, false);
        assert.equal(foreignReclaimed.dispatch.ownerId, "controller-foreign");
        assert.equal(foreignReclaimed.dispatch.fenceToken, 3);
      }
      assert.deepStrictEqual(
        yield* first.sql`
          SELECT status, owner_id AS "ownerId", fence_token AS "fenceToken",
            expires_at AS "expiresAt"
          FROM main.agent_control_armed_dispatch_states
          WHERE project_id = ${projectId} ORDER BY fence_token
        `,
        [
          { status: "superseded", ownerId: winner!.dispatch.ownerId, fenceToken: 1, expiresAt },
          {
            status: "superseded",
            ownerId: winner!.dispatch.ownerId,
            fenceToken: 2,
            expiresAt: reclaimedExpiresAt,
          },
          {
            status: "claimed",
            ownerId: "controller-foreign",
            fenceToken: 3,
            expiresAt: foreignReclaimedExpiresAt,
          },
        ],
      );
      const staleCommandId = winner!.dispatch.commandId;
      const nextSequence = (yield* first.sql<{ readonly sequence: number }>`
        SELECT COALESCE(MAX(sequence), 0) + 1 AS sequence FROM main.agent_control_events
      `)[0]!.sequence;
      yield* first.sql`
        INSERT INTO main.agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created, accepted_at, error_code
        ) VALUES (
          ${staleCommandId},
          ${fingerprintRunOnceModeCommand({
            commandId: staleCommandId,
            projectId,
            expectedRevision: 2,
            mode: "run-once",
          })},
          'system', 'project-controller', ${projectId}, 'accepted', ${nextSequence}, 3, 1,
          ${afterExpiry}, NULL
        )
      `;
      const staleEvent = yield* Effect.exit(first.sql`
        INSERT INTO main.agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_authority,
          payload_json, metadata_json
        ) VALUES (
          'stale-armed-activation-event', 'project-controller', ${projectId}, 3,
          'agentControl.project.mode.changed', ${afterExpiry}, ${staleCommandId}, NULL,
          ${staleCommandId}, 'system', ${canonicalJson({
            projectId,
            previousMode: "armed",
            mode: "run-once",
            previousPausedFromMode: null,
            pausedFromMode: null,
            changedAt: afterExpiry,
          } as never)}, '{"schemaVersion":1}'
        )
      `);
      assert.isTrue(Exit.isFailure(staleEvent));
      assert.deepStrictEqual(yield* first.sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* first.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
