import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseState,
  AgentControlStageRunState,
  AgentControlTaskId,
  CommandId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { deriveAgentControlStageRunLeaseId } from "../identity.ts";
import { AgentControlStageRunLease } from "../Services/AgentControlStageRunLease.ts";
import { AgentControlStageRunLeaseEngine } from "../Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseStateRepository } from "../Services/AgentControlStageRunLeaseStateRepository.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const at = "2026-07-24T10:00:00.000Z";
const repository = {
  repositoryNodeId: "lease-repository-node",
  nameWithOwner: "owner/repository",
} as const;

const issue = (projectId: ProjectId): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: `issue-${projectId}`,
  number: 1,
  url: `https://example.test/${projectId}/issues/1`,
  state: "open",
  title: "untrusted title",
  body: "untrusted body",
  contentTrust: "untrusted-external",
  updatedAt: at,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
});

const taskFrom = (
  projectId: ProjectId,
  source: AgentControlGithubIssueSnapshot,
): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId: AgentControlTaskId.make(`task-${projectId}`),
  source: {
    projectId,
    repositoryNodeId: source.repositoryNodeId,
    issueNodeId: source.issueNodeId,
    issueNumber: source.number,
    issueUrl: source.url,
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: source.updatedAt,
  githubIntakeSequence: 1,
  sourceSnapshot: {
    repositoryNodeId: source.repositoryNodeId,
    issueNodeId: source.issueNodeId,
    number: source.number,
    url: source.url,
    state: source.state,
    title: source.title,
    body: source.body,
    contentTrust: "untrusted-external",
    updatedAt: source.updatedAt,
    timelineComplete: source.timelineComplete,
    ready: source.ready,
    paused: source.paused,
    eligible: source.eligible,
    eligibilityReason: source.eligibilityReason,
  },
  createdAt: at,
  updatedAt: at,
  revision: 1,
  sequence: 1,
});

const seedPrepared = Effect.fn("seedPreparedStageRunLease")(function* (projectId: ProjectId) {
  const sql = yield* SqlClient.SqlClient;
  const github = yield* AgentControlGithubStateRepository;
  const tasks = yield* AgentControlTaskStateRepository;
  const stageRuns = yield* AgentControlStageRun;
  const source = issue(projectId);
  const task = taskFrom(projectId, source);

  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Lease test', ${`/tmp/${projectId}`}, NULL, '[]',
      ${at}, ${at}, NULL
    )
  `;
  yield* sql`
    INSERT INTO agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, 'observe', NULL, 1, 1, ${at})
  `;
  yield* github.save(
    {
      schemaVersion: 1,
      projectId,
      config: {
        schemaVersion: 1,
        projectId,
        settings: {
          trackerKind: "github",
          readyLabel: "agent:ready",
          pausedLabel: "agent:paused",
          trustedLogins: ["trusted"],
          pollIntervalSeconds: 60,
        },
        repository,
        revision: 1,
        sequence: 1,
        updatedAt: at,
      },
      cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
      pollStatus: {
        status: "success",
        attemptedAt: at,
        completedAt: at,
        errorCode: null,
        issueCount: 1,
      },
      revision: 1,
      sequence: 1,
      updatedAt: at,
    },
    0,
  );
  yield* github.replaceIssues(projectId, [source]);
  yield* tasks.save(task, 0);
  yield* sql`
    INSERT INTO agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence,
      revision, status, updated_at
    ) VALUES (${projectId}, 1, 1, 1, 'completed', ${at})
  `;
  const prepared = yield* stageRuns.prepareInitial({
    commandId: CommandId.make(`prepare-${projectId}`),
    projectId,
    taskId: task.taskId,
  });
  return { task, stageRun: prepared.state };
});

const resolvedReserveInput = Effect.fn("resolvedReserveInput")(function* (
  stageRun: AgentControlStageRunState,
  commandId: string,
  expectedRevision: number,
  fenceToken: number,
  leaseDurationMs = 60_000,
) {
  return {
    type: "agentControl.stageRunLease.reserve" as const,
    commandId: CommandId.make(commandId),
    leaseId: yield* deriveAgentControlStageRunLeaseId({
      projectId: stageRun.projectId,
      taskId: stageRun.taskId,
    }),
    projectId: stageRun.projectId,
    taskId: stageRun.taskId,
    stageRunId: stageRun.stageRunId,
    attemptId: stageRun.attemptId,
    taskRevision: stageRun.taskRevision,
    githubIntakeSequence: stageRun.githubIntakeSequence,
    sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
    fenceToken,
    expectedRevision,
    leaseDurationMs,
  };
});

const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLeaseState),
);
const encodeStageRunState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunState),
);

layer("Agent Control stage-run lease foundation", (it) => {
  it.effect("reserves, replays, renews, releases, and advances the fence token", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const reader = yield* AgentControlStageRunLease;
      const projectId = ProjectId.make("lease-lifecycle");
      const { task, stageRun } = yield* seedPrepared(projectId);

      const reserve = yield* resolvedReserveInput(stageRun, "lease-reserve-1", 0, 1);
      const publishedFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const first = yield* engine.dispatchController({
        ...reserve,
        holderId: AgentControlStageRunLeaseHolderId.make("client-controlled-holder"),
      } as typeof reserve);
      assert.equal(first._tag, "Accepted");
      if (first._tag !== "Accepted") return;
      assert.equal(first.result.state.fenceToken, 1);
      assert.equal(first.result.state.status, "reserved");
      assert.match(first.result.state.holderId, /^stage-run-lease-holder-[0-9a-f]{64}$/);
      assert.notEqual(first.result.state.holderId, "client-controlled-holder");
      const published = yield* Fiber.join(publishedFiber);
      assert.equal(published._tag, "Some");
      if (published._tag === "Some") {
        assert.equal(published.value.sequence, first.result.resultSequence);
      }

      const replayFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const replay = yield* engine.dispatchController(reserve);
      assert.equal(replay._tag, "Accepted");
      if (replay._tag === "Accepted") {
        assert.deepStrictEqual(replay.result, first.result);
        assert.lengthOf(replay.events, 0);
      }
      yield* Effect.yieldNow;
      assert.equal(replayFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(replayFiber);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
        `)[0]!.count,
        1,
      );

      const view = yield* reader.getLease({ projectId, taskId: task.taskId });
      assert.equal(view.ownership, "current-runtime");
      assert.notProperty(view, "holderId");

      const staleRevision = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.renew",
        commandId: CommandId.make("lease-renew-stale-revision"),
        leaseId: first.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 0,
        leaseDurationMs: 120_000,
      });
      assert.equal(staleRevision._tag, "Rejected");
      if (staleRevision._tag === "Rejected") {
        assert.equal(staleRevision.error.code, "revision-conflict");
      }

      const renew = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.renew",
        commandId: CommandId.make("lease-renew-1"),
        leaseId: first.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 1,
        leaseDurationMs: 120_000,
      });
      assert.equal(renew._tag, "Accepted");
      if (renew._tag !== "Accepted") return;
      assert.equal(renew.result.state.revision, 2);

      const releaseInput = {
        type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
        commandId: CommandId.make("lease-release-1"),
        leaseId: first.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 2,
      };
      const released = yield* engine.dispatchController(releaseInput);
      assert.equal(released._tag, "Accepted");
      if (released._tag !== "Accepted") return;
      assert.equal(released.result.state.status, "released");

      const releaseReplay = yield* engine.dispatchController(releaseInput);
      assert.equal(releaseReplay._tag, "Accepted");
      if (releaseReplay._tag === "Accepted") {
        assert.deepStrictEqual(releaseReplay.result, released.result);
        assert.lengthOf(releaseReplay.events, 0);
      }

      const secondReserve = yield* engine.dispatchController(
        yield* resolvedReserveInput(stageRun, "lease-reserve-2", 3, 2),
      );
      assert.equal(secondReserve._tag, "Accepted");
      if (secondReserve._tag === "Accepted") {
        assert.equal(secondReserve.result.state.fenceToken, 2);
        assert.equal(secondReserve.result.state.revision, 4);
      }

      const staleRenew = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.renew",
        commandId: CommandId.make("lease-renew-stale-token"),
        leaseId: first.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 4,
        leaseDurationMs: 60_000,
      });
      assert.equal(staleRenew._tag, "Rejected");
      if (staleRenew._tag === "Rejected") {
        assert.equal(staleRenew.error.code, "fence-token-mismatch");
      }
      const staleRenewReplay = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.renew",
        commandId: CommandId.make("lease-renew-stale-token"),
        leaseId: first.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 4,
        leaseDurationMs: 60_000,
      });
      assert.equal(staleRenewReplay._tag, "Rejected");
      if (staleRenewReplay._tag === "Rejected") {
        assert.equal(staleRenewReplay.error.code, "fence-token-mismatch");
      }

      yield* sql`
        UPDATE projection_projects SET deleted_at = ${at}
        WHERE project_id = ${projectId}
      `;
      const oldAcceptedAfterDelete = yield* engine.dispatchController(reserve);
      assert.equal(oldAcceptedAfterDelete._tag, "Accepted");
      if (oldAcceptedAfterDelete._tag === "Accepted") {
        assert.deepStrictEqual(oldAcceptedAfterDelete.result, first.result);
        assert.lengthOf(oldAcceptedAfterDelete.events, 0);
      }
    }),
  );

  it.effect("keeps expired and foreign-runtime reservations exclusive and fail closed", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const reader = yield* AgentControlStageRunLease;
      const states = yield* AgentControlStageRunLeaseStateRepository;
      const projectId = ProjectId.make("lease-recovery");
      const { task, stageRun } = yield* seedPrepared(projectId);
      const first = yield* engine.dispatchController(
        yield* resolvedReserveInput(stageRun, "lease-recovery-reserve", 0, 1, 1),
      );
      assert.equal(first._tag, "Accepted");
      if (first._tag !== "Accepted") return;

      yield* TestClock.adjust(Duration.millis(5));
      const expired = yield* reader.getLease({ projectId, taskId: task.taskId });
      assert.equal(expired.health, "expired");
      const retry = yield* engine.dispatchController(
        yield* resolvedReserveInput(stageRun, "lease-timeout-takeover", 1, 2),
      );
      assert.equal(retry._tag, "Rejected");
      if (retry._tag === "Rejected") {
        assert.equal(retry.error.code, "lease-already-reserved");
      }

      const foreignState = {
        ...first.result.state,
        holderId: AgentControlStageRunLeaseHolderId.make("foreign-runtime-holder"),
      };
      const stateJson = yield* encodeState(foreignState);
      yield* sql`
        UPDATE agent_control_stage_run_lease_states
        SET holder_id = ${foreignState.holderId}, state_json = ${stateJson}
        WHERE lease_id = ${foreignState.leaseId}
      `;
      const loaded = yield* states.get(foreignState.leaseId);
      assert.equal(loaded._tag, "Some");
      const recovery = yield* reader.getLease({ projectId, taskId: task.taskId });
      assert.equal(recovery.ownership, "foreign-runtime");
      assert.equal(recovery.health, "recovery-required");

      const renew = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.renew",
        commandId: CommandId.make("lease-foreign-renew"),
        leaseId: foreignState.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 1,
        leaseDurationMs: 60_000,
      });
      assert.equal(renew._tag, "Rejected");
      if (renew._tag === "Rejected") assert.equal(renew.error.code, "holder-mismatch");

      const release = yield* engine.dispatchController({
        type: "agentControl.stageRunLease.releaseBeforeExecution",
        commandId: CommandId.make("lease-foreign-release"),
        leaseId: foreignState.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        fenceToken: 1,
        expectedRevision: 1,
      });
      assert.equal(release._tag, "Rejected");
      if (release._tag === "Rejected") assert.equal(release.error.code, "holder-mismatch");
    }),
  );

  it.effect("quarantines corrupt list rows while direct get and mutation fail closed", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const reader = yield* AgentControlStageRunLease;
      const projectId = ProjectId.make("lease-corruption");
      const { task, stageRun } = yield* seedPrepared(projectId);
      const reserve = yield* resolvedReserveInput(stageRun, "lease-corrupt-reserve", 0, 1);
      const first = yield* engine.dispatchController(reserve);
      assert.equal(first._tag, "Accepted");
      if (first._tag !== "Accepted") return;

      yield* sql`
        UPDATE agent_control_stage_run_lease_states SET state_json = '{}'
        WHERE lease_id = ${first.result.state.leaseId}
      `;
      const list = yield* reader.listLeases({ projectId });
      assert.equal(list.quarantinedCount, 1);
      assert.lengthOf(list.leases, 0);
      const get = yield* Effect.result(reader.getLease({ projectId, taskId: task.taskId }));
      assert.equal(get._tag, "Failure");
      if (get._tag === "Failure") assert.equal(get.failure.code, "lease-projection-corrupt");
      const mutation = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.stageRunLease.renew",
          commandId: CommandId.make("lease-corrupt-renew"),
          leaseId: first.result.state.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          fenceToken: 1,
          expectedRevision: 1,
          leaseDurationMs: 60_000,
        }),
      );
      assert.equal(mutation._tag, "Failure");
      if (mutation._tag === "Failure") {
        assert.equal(mutation.failure.code, "lease-projection-corrupt");
      }
      const restoredStateJson = yield* encodeState(first.result.state);
      yield* sql`
        UPDATE agent_control_stage_run_lease_states
        SET state_json = ${restoredStateJson}
        WHERE lease_id = ${first.result.state.leaseId}
      `;
    }),
  );

  it.effect("admits only one parallel reservation and persists the losing domain receipt", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-parallel");
      const { stageRun } = yield* seedPrepared(projectId);
      const firstInput = yield* resolvedReserveInput(stageRun, "lease-parallel-first", 0, 1);
      const secondInput = yield* resolvedReserveInput(stageRun, "lease-parallel-second", 0, 1);
      const results = yield* Effect.all(
        [
          Effect.result(engine.dispatchController(firstInput)),
          Effect.result(engine.dispatchController(secondInput)),
        ],
        { concurrency: "unbounded" },
      );
      const successes = results.filter(
        (result) => result._tag === "Success" && result.success._tag === "Accepted",
      );
      const rejected = results.filter(
        (result) => result._tag === "Success" && result.success._tag === "Rejected",
      );
      assert.lengthOf(successes, 1);
      assert.lengthOf(rejected, 1);
      if (rejected[0]?._tag === "Success" && rejected[0].success._tag === "Rejected") {
        assert.equal(rejected[0].success.error.code, "lease-already-reserved");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${firstInput.leaseId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE aggregate_kind = 'stage-run-lease'
            AND aggregate_id = ${firstInput.leaseId}
        `)[0]!.count,
        2,
      );
    }),
  );

  it.effect("rolls event, projection, and receipt back together and retries the same command", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-rollback");
      const { stageRun } = yield* seedPrepared(projectId);
      const input = yield* resolvedReserveInput(stageRun, "lease-rollback-command", 0, 1);
      yield* sql`
        CREATE TRIGGER fail_lease_projection_insert
        BEFORE INSERT ON agent_control_stage_run_lease_states
        BEGIN
          SELECT RAISE(ABORT, 'lease projection unavailable');
        END
      `;
      const publicationFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const failed = yield* Effect.result(engine.dispatchController(input));
      assert.equal(failed._tag, "Failure");
      if (failed._tag === "Failure") {
        assert.equal(failed.failure.code, "internal-persistence-error");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${input.leaseId}
        `)[0]!.count,
        0,
      );
      yield* Effect.yieldNow;
      assert.equal(publicationFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(publicationFiber);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${input.commandId}
        `)[0]!.count,
        0,
      );
      yield* sql`DROP TRIGGER fail_lease_projection_insert`;
      const retried = yield* engine.dispatchController(input);
      assert.equal(retried._tag, "Accepted");
    }),
  );

  it.effect("rejects ambiguous prepared stage-run history without a terminal receipt", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-ambiguous-stage-history");
      const { stageRun } = yield* seedPrepared(projectId);
      const sourceIdentityFingerprint = "b".repeat(64);
      const stageRunId = yield* deriveAgentControlStageRunId({
        projectId,
        taskId: stageRun.taskId,
        taskRevision: stageRun.taskRevision,
        githubIntakeSequence: stageRun.githubIntakeSequence,
        sourceIdentityFingerprint,
        stageKind: stageRun.stageKind,
        stageOrdinal: stageRun.stageOrdinal,
      });
      const attemptId = yield* deriveAgentControlAttemptId(stageRunId, stageRun.attemptOrdinal);
      const ambiguous = {
        ...stageRun,
        stageRunId,
        attemptId,
        sourceIdentityFingerprint,
      };
      const ambiguousJson = yield* encodeStageRunState(ambiguous);
      yield* sql`DROP INDEX idx_agent_control_stage_run_initial_snapshot`;
      yield* sql`
        INSERT INTO agent_control_stage_run_states (
          stage_run_id, project_id, task_id, attempt_id, role_id,
          stage_kind, stage_ordinal, attempt_ordinal, status,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          state_json, created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          ${ambiguous.stageRunId}, ${ambiguous.projectId}, ${ambiguous.taskId},
          ${ambiguous.attemptId}, ${ambiguous.roleId}, ${ambiguous.stageKind},
          ${ambiguous.stageOrdinal}, ${ambiguous.attemptOrdinal}, ${ambiguous.status},
          ${ambiguous.taskRevision}, ${ambiguous.githubIntakeSequence},
          ${ambiguous.sourceIdentityFingerprint}, ${ambiguousJson},
          ${ambiguous.createdAt}, ${ambiguous.updatedAt}, ${ambiguous.revision},
          ${ambiguous.sequence}
        )
      `;
      const input = yield* resolvedReserveInput(stageRun, "lease-ambiguous-command", 0, 1);
      const result = yield* Effect.result(engine.dispatchController(input));
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "stage-run-history-ambiguous");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${input.commandId}
        `)[0]!.count,
        0,
      );
      yield* sql`
        DELETE FROM agent_control_stage_run_states
        WHERE stage_run_id = ${ambiguous.stageRunId}
      `;
      yield* sql`
        CREATE UNIQUE INDEX idx_agent_control_stage_run_initial_snapshot
        ON agent_control_stage_run_states(
          project_id, task_id, task_revision, github_intake_sequence,
          stage_kind, stage_ordinal
        )
      `;
    }),
  );

  it.effect(
    "receipts an unavailable project before any lease event and replays the rejection",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlStageRunLeaseEngine;
        const projectId = ProjectId.make("lease-admission-delete");
        const { stageRun } = yield* seedPrepared(projectId);
        const input = yield* resolvedReserveInput(stageRun, "lease-deleted-command", 0, 1);
        yield* sql`
        UPDATE projection_projects SET deleted_at = ${at}
        WHERE project_id = ${projectId}
      `;
        const rejected = yield* engine.dispatchController(input);
        assert.equal(rejected._tag, "Rejected");
        if (rejected._tag === "Rejected") {
          assert.equal(rejected.error.code, "project-unavailable");
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${input.leaseId}
        `)[0]!.count,
          0,
        );
        yield* sql`
        UPDATE projection_projects SET deleted_at = NULL
        WHERE project_id = ${projectId}
      `;
        const replay = yield* engine.dispatchController(input);
        assert.equal(replay._tag, "Rejected");
        if (replay._tag === "Rejected") {
          assert.equal(replay.error.code, "project-unavailable");
        }
      }),
  );

  it.effect(
    "rebuilds more than 500 sparse lease events without touching foreign state",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlStageRunLeaseEngine;
        const states = yield* AgentControlStageRunLeaseStateRepository;
        const projectId = ProjectId.make("lease-rebuild");
        const { task, stageRun } = yield* seedPrepared(projectId);
        const reserved = yield* engine.dispatchController(
          yield* resolvedReserveInput(stageRun, "lease-rebuild-reserve", 0, 1),
        );
        assert.equal(reserved._tag, "Accepted");
        if (reserved._tag !== "Accepted") return;

        let revision = 1;
        for (let index = 1; index <= 501; index += 1) {
          if (index % 100 === 0) {
            yield* sql`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            ) VALUES (
              ${`foreign-event-${index}`}, 'project-controller',
              ${`foreign-project-${index}`}, 1, 'agentControl.project.mode.changed',
              ${at}, ${`foreign-command-${index}`}, NULL,
              ${`foreign-command-${index}`}, 'human', '{}', '{"schemaVersion":1}'
            )
          `;
          }
          const renewed = yield* engine.dispatchController({
            type: "agentControl.stageRunLease.renew",
            commandId: CommandId.make(`lease-rebuild-renew-${index}`),
            leaseId: reserved.result.state.leaseId,
            projectId,
            taskId: task.taskId,
            stageRunId: stageRun.stageRunId,
            attemptId: stageRun.attemptId,
            fenceToken: 1,
            expectedRevision: revision,
            leaseDurationMs: 60_000,
          });
          assert.equal(renewed._tag, "Accepted");
          if (renewed._tag !== "Accepted") return;
          revision = renewed.result.state.revision;
        }
        assert.equal(revision, 502);
        const deepReplay = yield* engine.dispatchController({
          type: "agentControl.stageRunLease.renew",
          commandId: CommandId.make("lease-rebuild-renew-501"),
          leaseId: reserved.result.state.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          fenceToken: 1,
          expectedRevision: 501,
          leaseDurationMs: 60_000,
        });
        assert.equal(deepReplay._tag, "Accepted");
        if (deepReplay._tag === "Accepted") {
          assert.equal(deepReplay.result.state.revision, 502);
          assert.lengthOf(deepReplay.events, 0);
        }
        yield* sql`
        INSERT INTO agent_control_projection_state (
          projector_name, last_applied_sequence, updated_at
        ) VALUES ('foreign-projector-lease-rebuild', 7, ${at})
      `;
        const foreignStageRunCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_stage_run_states
      `)[0]!.count;

        yield* engine.rebuild;

        const rebuilt = yield* states.get(reserved.result.state.leaseId);
        assert.equal(rebuilt._tag, "Some");
        if (rebuilt._tag === "Some") assert.equal(rebuilt.value.revision, 502);
        assert.equal(
          (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_stage_run_states
        `)[0]!.count,
          foreignStageRunCount,
        );
        assert.deepStrictEqual(
          yield* sql`
          SELECT last_applied_sequence AS "lastAppliedSequence"
          FROM agent_control_projection_state
          WHERE projector_name = 'foreign-projector-lease-rebuild'
        `,
          [{ lastAppliedSequence: 7 }],
        );
      }),
    30_000,
  );
});
