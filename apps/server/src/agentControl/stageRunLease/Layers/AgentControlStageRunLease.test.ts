import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseState,
  AgentControlStageRunId,
  AgentControlStageRunPreparedPayload,
  AgentControlStageRunState,
  AgentControlTaskId,
  CommandId,
  IsoDateTime,
  ProjectId,
  type AgentControlStageRunLeaseId,
  type AgentControlStageRunLeaseState as LeaseState,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlRuntimeLayerLive,
  AgentControlStageRunLeaseEngineLayerLive,
} from "../../runtimeLayer.ts";
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
const encodePreparedPayload = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunPreparedPayload),
);
const shiftTimestamp = (value: IsoDateTime, milliseconds: number) =>
  IsoDateTime.make(
    DateTime.formatIso(DateTime.add(Option.getOrThrow(DateTime.make(value)), { milliseconds })),
  );
const commandSnapshot = (stageRun: AgentControlStageRunState) => ({
  taskRevision: stageRun.taskRevision,
  githubIntakeSequence: stageRun.githubIntakeSequence,
  sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
});

const replaceLeaseProjection = Effect.fn("replaceLeaseProjection")(function* (state: LeaseState) {
  const sql = yield* SqlClient.SqlClient;
  const stateJson = yield* encodeState(state);
  yield* sql`
    UPDATE agent_control_stage_run_lease_states SET
      project_id = ${state.projectId},
      task_id = ${state.taskId},
      stage_run_id = ${state.stageRunId},
      attempt_id = ${state.attemptId},
      task_revision = ${state.taskRevision},
      github_intake_sequence = ${state.githubIntakeSequence},
      source_identity_fingerprint = ${state.sourceIdentityFingerprint},
      holder_id = ${state.holderId},
      fence_token = ${state.fenceToken},
      status = ${state.status},
      acquired_at = ${state.acquiredAt},
      renewed_at = ${state.renewedAt},
      expires_at = ${state.expiresAt},
      released_at = ${state.releasedAt},
      state_json = ${stateJson},
      revision = ${state.revision},
      last_event_sequence = ${state.sequence}
    WHERE lease_id = ${state.leaseId}
  `;
});

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
        ...commandSnapshot(stageRun),
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
        ...commandSnapshot(stageRun),
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
        ...commandSnapshot(stageRun),
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
        ...commandSnapshot(stageRun),
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
        ...commandSnapshot(stageRun),
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

      for (const mutation of [
        {
          type: "agentControl.stageRunLease.renew" as const,
          commandId: CommandId.make("lease-foreign-renew"),
          leaseId: foreignState.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          ...commandSnapshot(stageRun),
          fenceToken: 1,
          expectedRevision: 1,
          leaseDurationMs: 60_000,
        },
        {
          type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
          commandId: CommandId.make("lease-foreign-release"),
          leaseId: foreignState.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          ...commandSnapshot(stageRun),
          fenceToken: 1,
          expectedRevision: 1,
        },
      ]) {
        const result = yield* Effect.result(engine.dispatchController(mutation));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "lease-projection-corrupt");
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${mutation.commandId}
          `)[0]!.count,
          0,
        );
      }
      yield* replaceLeaseProjection(first.result.state);
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
          ...commandSnapshot(stageRun),
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

  it.effect("rejects locally valid projection drift before every lease mutation", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-authoritative-drift");
      const { task, stageRun } = yield* seedPrepared(projectId);
      const reserved = yield* engine.dispatchController(
        yield* resolvedReserveInput(stageRun, "lease-authoritative-reserve", 0, 1),
      );
      assert.equal(reserved._tag, "Accepted");
      if (reserved._tag !== "Accepted") return;
      const original = reserved.result.state;
      const otherFingerprint = "b".repeat(64);
      const otherStageRunId = yield* deriveAgentControlStageRunId({
        projectId,
        taskId: task.taskId,
        taskRevision: original.taskRevision,
        githubIntakeSequence: original.githubIntakeSequence,
        sourceIdentityFingerprint: otherFingerprint,
        stageKind: "planning",
        stageOrdinal: 1,
      });
      const otherAttemptId = yield* deriveAgentControlAttemptId(otherStageRunId, 1);
      const earlierAcquiredAt = shiftTimestamp(original.acquiredAt, -1);
      const laterRenewedAt = shiftTimestamp(original.renewedAt, 1);
      const laterExpiresAt = shiftTimestamp(original.expiresAt, 1);
      const muchLaterExpiresAt = shiftTimestamp(original.expiresAt, 60_000);
      const mutations = [
        { ...original, fenceToken: 2 },
        {
          ...original,
          holderId: AgentControlStageRunLeaseHolderId.make("locally-valid-foreign-holder"),
        },
        {
          ...original,
          stageRunId: otherStageRunId,
          attemptId: otherAttemptId,
          sourceIdentityFingerprint: otherFingerprint,
        },
        { ...original, acquiredAt: earlierAcquiredAt },
        {
          ...original,
          renewedAt: laterRenewedAt,
          expiresAt: laterExpiresAt,
        },
        { ...original, expiresAt: muchLaterExpiresAt },
        {
          ...original,
          status: "released" as const,
          releasedAt: laterRenewedAt,
        },
        { ...original, revision: 2 },
        { ...original, sequence: original.sequence + 100 },
      ];
      const publication = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      for (const [index, mutated] of mutations.entries()) {
        yield* replaceLeaseProjection(mutated);
        const commandId = CommandId.make(`lease-authoritative-drift-${index}`);
        const result = yield* Effect.result(
          engine.dispatchController({
            type: "agentControl.stageRunLease.renew",
            commandId,
            leaseId: original.leaseId,
            projectId,
            taskId: task.taskId,
            stageRunId: stageRun.stageRunId,
            attemptId: stageRun.attemptId,
            ...commandSnapshot(stageRun),
            fenceToken: 1,
            expectedRevision: 1,
            leaseDurationMs: 60_000,
          }),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "lease-projection-corrupt");
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${commandId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_events
            WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${original.leaseId}
          `)[0]!.count,
          1,
        );
        yield* replaceLeaseProjection(original);
      }
      yield* Effect.yieldNow;
      assert.equal(publication.pollUnsafe(), undefined);
      yield* Fiber.interrupt(publication);
    }),
  );

  it.effect(
    "fails closed for missing stream, missing projection, and a later-page-visible gap",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlStageRunLeaseEngine;

        const assertCorruptRenew = Effect.fn("assertCorruptRenew")(function* (
          projectId: ProjectId,
          stageRun: AgentControlStageRunState,
          leaseId: AgentControlStageRunLeaseId,
          commandId: CommandId,
          expectedRevision: number,
        ) {
          const result = yield* Effect.result(
            engine.dispatchController({
              type: "agentControl.stageRunLease.renew",
              commandId,
              leaseId,
              projectId,
              taskId: stageRun.taskId,
              stageRunId: stageRun.stageRunId,
              attemptId: stageRun.attemptId,
              ...commandSnapshot(stageRun),
              fenceToken: 1,
              expectedRevision,
              leaseDurationMs: 60_000,
            }),
          );
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") {
            assert.equal(result.failure.code, "lease-projection-corrupt");
          }
          assert.equal(
            (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${commandId}
          `)[0]!.count,
            0,
          );
        });

        const streamMissingProject = ProjectId.make("lease-stream-missing");
        const streamMissing = yield* seedPrepared(streamMissingProject);
        const streamMissingReserve = yield* engine.dispatchController(
          yield* resolvedReserveInput(streamMissing.stageRun, "lease-stream-missing-reserve", 0, 1),
        );
        assert.equal(streamMissingReserve._tag, "Accepted");
        if (streamMissingReserve._tag !== "Accepted") return;
        yield* sql`
        DELETE FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${streamMissingReserve.result.state.leaseId}
      `;
        yield* assertCorruptRenew(
          streamMissingProject,
          streamMissing.stageRun,
          streamMissingReserve.result.state.leaseId,
          CommandId.make("lease-stream-missing-renew"),
          1,
        );

        const projectionMissingProject = ProjectId.make("lease-projection-missing");
        const projectionMissing = yield* seedPrepared(projectionMissingProject);
        const projectionMissingReserve = yield* engine.dispatchController(
          yield* resolvedReserveInput(
            projectionMissing.stageRun,
            "lease-projection-missing-reserve",
            0,
            1,
          ),
        );
        assert.equal(projectionMissingReserve._tag, "Accepted");
        if (projectionMissingReserve._tag !== "Accepted") return;
        yield* sql`
        DELETE FROM agent_control_stage_run_lease_states
        WHERE lease_id = ${projectionMissingReserve.result.state.leaseId}
      `;
        yield* assertCorruptRenew(
          projectionMissingProject,
          projectionMissing.stageRun,
          projectionMissingReserve.result.state.leaseId,
          CommandId.make("lease-projection-missing-renew"),
          1,
        );

        const gapProject = ProjectId.make("lease-stream-gap");
        const gap = yield* seedPrepared(gapProject);
        const gapReserve = yield* engine.dispatchController(
          yield* resolvedReserveInput(gap.stageRun, "lease-gap-reserve", 0, 1),
        );
        assert.equal(gapReserve._tag, "Accepted");
        if (gapReserve._tag !== "Accepted") return;
        for (const revision of [1, 2]) {
          const renewed = yield* engine.dispatchController({
            type: "agentControl.stageRunLease.renew",
            commandId: CommandId.make(`lease-gap-renew-${revision}`),
            leaseId: gapReserve.result.state.leaseId,
            projectId: gapProject,
            taskId: gap.stageRun.taskId,
            stageRunId: gap.stageRun.stageRunId,
            attemptId: gap.stageRun.attemptId,
            ...commandSnapshot(gap.stageRun),
            fenceToken: 1,
            expectedRevision: revision,
            leaseDurationMs: 60_000,
          });
          assert.equal(renewed._tag, "Accepted");
        }
        yield* sql`DROP INDEX idx_agent_control_events_stream_version`;
        yield* sql`
        UPDATE agent_control_events SET stream_version = 1
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${gapReserve.result.state.leaseId}
          AND stream_version = 2
      `;
        yield* assertCorruptRenew(
          gapProject,
          gap.stageRun,
          gapReserve.result.state.leaseId,
          CommandId.make("lease-duplicate-stream-version"),
          3,
        );
        yield* sql`
        UPDATE agent_control_events SET stream_version = 2
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${gapReserve.result.state.leaseId}
          AND event_type = 'agentControl.stageRunLease.renewed'
          AND stream_version = 1
      `;
        yield* sql`
        DELETE FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${gapReserve.result.state.leaseId}
          AND stream_version = 2
      `;
        yield* assertCorruptRenew(
          gapProject,
          gap.stageRun,
          gapReserve.result.state.leaseId,
          CommandId.make("lease-gap-after-delete"),
          3,
        );
        yield* sql`
        DELETE FROM agent_control_stage_run_lease_states
        WHERE lease_id IN (
          ${streamMissingReserve.result.state.leaseId},
          ${projectionMissingReserve.result.state.leaseId},
          ${gapReserve.result.state.leaseId}
        )
      `;
        yield* sql`
        DELETE FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id IN (
            ${streamMissingReserve.result.state.leaseId},
            ${projectionMissingReserve.result.state.leaseId},
            ${gapReserve.result.state.leaseId}
          )
      `;
        yield* sql`
        CREATE UNIQUE INDEX idx_agent_control_events_stream_version
        ON agent_control_events(aggregate_kind, stream_id, stream_version)
      `;
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
      const ambiguousPayload = yield* encodePreparedPayload({
        projectId: ambiguous.projectId,
        taskId: ambiguous.taskId,
        stageRunId: ambiguous.stageRunId,
        attemptId: ambiguous.attemptId,
        roleId: ambiguous.roleId,
        stageKind: ambiguous.stageKind,
        stageOrdinal: ambiguous.stageOrdinal,
        attemptOrdinal: ambiguous.attemptOrdinal,
        status: ambiguous.status,
        taskRevision: ambiguous.taskRevision,
        githubIntakeSequence: ambiguous.githubIntakeSequence,
        sourceIdentityFingerprint: ambiguous.sourceIdentityFingerprint,
        preparedAt: ambiguous.createdAt,
      });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'lease-ambiguous-stage-event', 'stage-run', ${ambiguous.stageRunId}, 1,
          'agentControl.stageRun.prepared', ${ambiguous.createdAt},
          'lease-ambiguous-stage-command', NULL, 'lease-ambiguous-stage-command',
          'controller', ${ambiguousPayload}, '{"schemaVersion":1}'
        )
      `;
      const ambiguousSequence = (yield* sql<{ readonly sequence: number }>`
          SELECT sequence FROM agent_control_events
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${ambiguous.stageRunId}
        `)[0]!.sequence;
      const committedAmbiguous = { ...ambiguous, sequence: ambiguousSequence };
      const committedAmbiguousJson = yield* encodeStageRunState(committedAmbiguous);
      yield* sql`
        UPDATE agent_control_stage_run_states
        SET last_event_sequence = ${ambiguousSequence},
          state_json = ${committedAmbiguousJson}
        WHERE stage_run_id = ${ambiguous.stageRunId}
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
        DELETE FROM agent_control_events
        WHERE aggregate_kind = 'stage-run' AND stream_id = ${ambiguous.stageRunId}
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

  it.effect("validates selected and historical Stage Runs against their event streams", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;

      const selectedProject = ProjectId.make("lease-stage-selected-drift");
      const selected = yield* seedPrepared(selectedProject);
      const shiftedAt = shiftTimestamp(selected.stageRun.createdAt, 1);
      const shifted = {
        ...selected.stageRun,
        createdAt: shiftedAt,
        updatedAt: shiftedAt,
      };
      const shiftedJson = yield* encodeStageRunState(shifted);
      yield* sql`
        UPDATE agent_control_stage_run_states
        SET created_at = ${shiftedAt}, updated_at = ${shiftedAt}, state_json = ${shiftedJson}
        WHERE stage_run_id = ${shifted.stageRunId}
      `;
      const selectedInput = yield* resolvedReserveInput(
        selected.stageRun,
        "lease-stage-selected-command",
        0,
        1,
      );
      const selectedResult = yield* Effect.result(engine.dispatchController(selectedInput));
      assert.equal(selectedResult._tag, "Failure");
      if (selectedResult._tag === "Failure") {
        assert.equal(selectedResult.failure.code, "stage-run-projection-corrupt");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${selectedInput.commandId}
        `)[0]!.count,
        0,
      );
      const selectedStateJson = yield* encodeStageRunState(selected.stageRun);
      yield* sql`
        UPDATE agent_control_stage_run_states
        SET created_at = ${selected.stageRun.createdAt},
          updated_at = ${selected.stageRun.updatedAt},
          state_json = ${selectedStateJson}
        WHERE stage_run_id = ${selected.stageRun.stageRunId}
      `;

      const historyProject = ProjectId.make("lease-stage-history-missing");
      const history = yield* seedPrepared(historyProject);
      const otherFingerprint = "c".repeat(64);
      const historicalStageRunId = yield* deriveAgentControlStageRunId({
        projectId: historyProject,
        taskId: history.stageRun.taskId,
        taskRevision: history.stageRun.taskRevision,
        githubIntakeSequence: history.stageRun.githubIntakeSequence,
        sourceIdentityFingerprint: otherFingerprint,
        stageKind: "planning",
        stageOrdinal: 1,
      });
      const historicalAttemptId = yield* deriveAgentControlAttemptId(historicalStageRunId, 1);
      const historical = {
        ...history.stageRun,
        stageRunId: historicalStageRunId,
        attemptId: historicalAttemptId,
        sourceIdentityFingerprint: otherFingerprint,
      };
      const historicalPayload = yield* encodePreparedPayload({
        projectId: historical.projectId,
        taskId: historical.taskId,
        stageRunId: historical.stageRunId,
        attemptId: historical.attemptId,
        roleId: historical.roleId,
        stageKind: historical.stageKind,
        stageOrdinal: historical.stageOrdinal,
        attemptOrdinal: historical.attemptOrdinal,
        status: historical.status,
        taskRevision: historical.taskRevision,
        githubIntakeSequence: historical.githubIntakeSequence,
        sourceIdentityFingerprint: historical.sourceIdentityFingerprint,
        preparedAt: historical.createdAt,
      });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'lease-stage-history-event', 'stage-run', ${historical.stageRunId}, 1,
          'agentControl.stageRun.prepared', ${historical.createdAt},
          'lease-stage-history-command', NULL, 'lease-stage-history-command',
          'controller', ${historicalPayload}, '{"schemaVersion":1}'
        )
      `;
      const historicalSequence = (yield* sql<{ readonly sequence: number }>`
          SELECT sequence FROM agent_control_events
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${historical.stageRunId}
        `)[0]!.sequence;
      const committedHistorical = { ...historical, sequence: historicalSequence };
      const committedHistoricalJson = yield* encodeStageRunState(committedHistorical);
      yield* sql`DROP INDEX idx_agent_control_stage_run_initial_snapshot`;
      yield* sql`
        INSERT INTO agent_control_stage_run_states (
          stage_run_id, project_id, task_id, attempt_id, role_id,
          stage_kind, stage_ordinal, attempt_ordinal, status,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          state_json, created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          ${committedHistorical.stageRunId}, ${committedHistorical.projectId},
          ${committedHistorical.taskId}, ${committedHistorical.attemptId},
          ${committedHistorical.roleId}, ${committedHistorical.stageKind},
          ${committedHistorical.stageOrdinal}, ${committedHistorical.attemptOrdinal},
          ${committedHistorical.status}, ${committedHistorical.taskRevision},
          ${committedHistorical.githubIntakeSequence},
          ${committedHistorical.sourceIdentityFingerprint}, ${committedHistoricalJson},
          ${committedHistorical.createdAt}, ${committedHistorical.updatedAt},
          ${committedHistorical.revision}, ${committedHistorical.sequence}
        )
      `;
      yield* sql`
        DELETE FROM agent_control_stage_run_states
        WHERE stage_run_id = ${committedHistorical.stageRunId}
      `;
      yield* sql`
        CREATE UNIQUE INDEX idx_agent_control_stage_run_initial_snapshot
        ON agent_control_stage_run_states(
          project_id, task_id, task_revision, github_intake_sequence,
          stage_kind, stage_ordinal
        )
      `;
      const historyInput = yield* resolvedReserveInput(
        history.stageRun,
        "lease-stage-history-command-reserve",
        0,
        1,
      );
      const historyResult = yield* Effect.result(engine.dispatchController(historyInput));
      assert.equal(historyResult._tag, "Failure");
      if (historyResult._tag === "Failure") {
        assert.equal(historyResult.failure.code, "stage-run-projection-corrupt");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${historyInput.leaseId}
        `)[0]!.count,
        0,
      );
      yield* sql`
        DELETE FROM agent_control_events
        WHERE aggregate_kind = 'stage-run' AND stream_id = ${historical.stageRunId}
      `;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${historyInput.commandId}
        `)[0]!.count,
        0,
      );
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

  it.effect("terminally binds deterministic identity rejection to the command id", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-identity-rejection");
      const { task, stageRun } = yield* seedPrepared(projectId);
      const reserved = yield* engine.dispatchController(
        yield* resolvedReserveInput(stageRun, "lease-identity-reserve", 0, 1),
      );
      assert.equal(reserved._tag, "Accepted");
      if (reserved._tag !== "Accepted") return;

      const commandId = CommandId.make("lease-wrong-stage-command");
      const wrong = {
        type: "agentControl.stageRunLease.renew" as const,
        commandId,
        leaseId: reserved.result.state.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: AgentControlStageRunId.make("wrong-stage-run"),
        attemptId: stageRun.attemptId,
        ...commandSnapshot(stageRun),
        fenceToken: 1,
        expectedRevision: 1,
        leaseDurationMs: 60_000,
      };
      const rejected = yield* engine.dispatchController(wrong);
      assert.equal(rejected._tag, "Rejected");
      if (rejected._tag === "Rejected") {
        assert.equal(rejected.error.code, "command-identity-mismatch");
      }
      const identical = yield* engine.dispatchController(wrong);
      assert.equal(identical._tag, "Rejected");
      if (identical._tag === "Rejected") {
        assert.equal(identical.error.code, "command-identity-mismatch");
      }
      const corrected = yield* Effect.result(
        engine.dispatchController({
          ...wrong,
          stageRunId: stageRun.stageRunId,
        }),
      );
      assert.equal(corrected._tag, "Failure");
      if (corrected._tag === "Failure") {
        assert.equal(corrected.failure.code, "command-identity-mismatch");
      }
      const wrongAttemptCommandId = CommandId.make("lease-wrong-attempt-command");
      const wrongAttempt = yield* engine.dispatchController({
        ...wrong,
        commandId: wrongAttemptCommandId,
        stageRunId: stageRun.stageRunId,
        attemptId: AgentControlAttemptId.make("wrong-attempt"),
      });
      assert.equal(wrongAttempt._tag, "Rejected");
      if (wrongAttempt._tag === "Rejected") {
        assert.equal(wrongAttempt.error.code, "command-identity-mismatch");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${reserved.result.state.leaseId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${wrongAttemptCommandId}
            AND status = 'rejected'
            AND error_code = 'command-identity-mismatch'
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${commandId}
            AND status = 'rejected'
            AND error_code = 'command-identity-mismatch'
        `)[0]!.count,
        1,
      );
    }),
  );

  it.effect("replays an accepted intent across a real runtime holder change", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engineA = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-restart-replay");
      const { task, stageRun } = yield* seedPrepared(projectId);
      const input = yield* resolvedReserveInput(stageRun, "lease-restart-command", 0, 1);
      const acceptedA = yield* engineA.dispatchController(input);
      assert.equal(acceptedA._tag, "Accepted");
      if (acceptedA._tag !== "Accepted") return;
      const holderA = acceptedA.result.state.holderId;
      const before = (yield* sql<{ readonly events: number; readonly receipts: number }>`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events
             WHERE aggregate_kind = 'stage-run-lease'
               AND stream_id = ${input.leaseId}) AS events,
            (SELECT COUNT(*) FROM agent_control_command_receipts
             WHERE aggregate_kind = 'stage-run-lease'
               AND aggregate_id = ${input.leaseId}) AS receipts
        `)[0]!;

      const runtimeB = yield* Layer.build(
        Layer.fresh(AgentControlStageRunLeaseEngineLayerLive).pipe(
          Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
          Layer.provideMerge(NodeServices.layer),
        ),
      );
      const engineB = Context.get(runtimeB, AgentControlStageRunLeaseEngine);
      const publication = yield* Stream.runHead(engineB.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const replay = yield* engineB.dispatchController(input);
      assert.equal(replay._tag, "Accepted");
      if (replay._tag !== "Accepted") return;
      assert.deepStrictEqual(replay.result, acceptedA.result);
      assert.equal(replay.result.state.holderId, holderA);
      assert.lengthOf(replay.events, 0);
      const view = yield* engineB.toView(replay.result.state);
      assert.equal(view.ownership, "foreign-runtime");
      assert.equal(view.health, "recovery-required");
      yield* Effect.yieldNow;
      assert.equal(publication.pollUnsafe(), undefined);
      yield* Fiber.interrupt(publication);

      const afterReplay = (yield* sql<{ readonly events: number; readonly receipts: number }>`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events
             WHERE aggregate_kind = 'stage-run-lease'
               AND stream_id = ${input.leaseId}) AS events,
            (SELECT COUNT(*) FROM agent_control_command_receipts
             WHERE aggregate_kind = 'stage-run-lease'
               AND aggregate_id = ${input.leaseId}) AS receipts
        `)[0]!;
      assert.deepStrictEqual(afterReplay, before);

      for (const mutation of [
        {
          type: "agentControl.stageRunLease.renew" as const,
          commandId: CommandId.make("lease-restart-renew"),
          leaseId: input.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          ...commandSnapshot(stageRun),
          fenceToken: 1,
          expectedRevision: 1,
          leaseDurationMs: 60_000,
        },
        {
          type: "agentControl.stageRunLease.releaseBeforeExecution" as const,
          commandId: CommandId.make("lease-restart-release"),
          leaseId: input.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          ...commandSnapshot(stageRun),
          fenceToken: 1,
          expectedRevision: 1,
        },
      ]) {
        const denied = yield* engineB.dispatchController(mutation);
        assert.equal(denied._tag, "Rejected");
        if (denied._tag === "Rejected") {
          assert.equal(denied.error.code, "holder-mismatch");
        }
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${input.leaseId}
        `)[0]!.count,
        before.events,
      );
    }),
  );

  it.effect("binds accepted replay to the exact resulting event and receipt coordinates", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunLeaseEngine;
      const projectId = ProjectId.make("lease-replay-event-binding");
      const { stageRun } = yield* seedPrepared(projectId);
      const input = yield* resolvedReserveInput(stageRun, "lease-bound-command", 0, 1);
      const accepted = yield* engine.dispatchController(input);
      assert.equal(accepted._tag, "Accepted");
      if (accepted._tag !== "Accepted") return;

      yield* sql`
        UPDATE agent_control_command_receipts
        SET result_sequence = ${accepted.result.resultSequence + 1}
        WHERE command_id = ${input.commandId}
      `;
      const wrongCoordinate = yield* Effect.result(engine.dispatchController(input));
      assert.equal(wrongCoordinate._tag, "Failure");
      if (wrongCoordinate._tag === "Failure") {
        assert.equal(wrongCoordinate.failure.code, "command-identity-mismatch");
      }
      yield* sql`
        UPDATE agent_control_command_receipts
        SET result_sequence = ${accepted.result.resultSequence}
        WHERE command_id = ${input.commandId}
      `;

      yield* sql`
        UPDATE agent_control_events
        SET command_id = 'lease-bound-other-command',
          correlation_id = 'lease-bound-other-command'
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${input.leaseId}
          AND stream_version = 1
      `;
      const wrongEvent = yield* Effect.result(engine.dispatchController(input));
      assert.equal(wrongEvent._tag, "Failure");
      if (wrongEvent._tag === "Failure") {
        assert.equal(wrongEvent.failure.code, "command-identity-mismatch");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${input.leaseId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${input.commandId}
        `)[0]!.count,
        1,
      );
      yield* sql`
        UPDATE agent_control_events
        SET command_id = ${input.commandId}, correlation_id = ${input.commandId}
        WHERE aggregate_kind = 'stage-run-lease'
          AND stream_id = ${input.leaseId}
          AND stream_version = 1
      `;
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
            ...commandSnapshot(stageRun),
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
          ...commandSnapshot(stageRun),
          fenceToken: 1,
          expectedRevision: 501,
          leaseDurationMs: 60_000,
        });
        assert.equal(deepReplay._tag, "Accepted");
        if (deepReplay._tag === "Accepted") {
          assert.equal(deepReplay.result.state.revision, 502);
          assert.lengthOf(deepReplay.events, 0);
        }
        const removed = (yield* sql<{
          readonly sequence: number;
          readonly eventId: string;
          readonly eventType: string;
          readonly occurredAt: string;
          readonly commandId: string;
          readonly correlationId: string;
          readonly authority: string;
          readonly payloadJson: string;
          readonly metadataJson: string;
        }>`
            SELECT sequence, event_id AS "eventId", event_type AS "eventType",
              occurred_at AS "occurredAt", command_id AS "commandId",
              correlation_id AS "correlationId", actor_authority AS authority,
              payload_json AS "payloadJson", metadata_json AS "metadataJson"
            FROM agent_control_events
            WHERE aggregate_kind = 'stage-run-lease'
              AND stream_id = ${reserved.result.state.leaseId}
              AND stream_version = 501
          `)[0]!;
        yield* sql`
          DELETE FROM agent_control_events
          WHERE aggregate_kind = 'stage-run-lease'
            AND stream_id = ${reserved.result.state.leaseId}
            AND stream_version = 501
        `;
        const lateGapCommandId = CommandId.make("lease-rebuild-late-gap");
        const lateGap = yield* Effect.result(
          engine.dispatchController({
            type: "agentControl.stageRunLease.renew",
            commandId: lateGapCommandId,
            leaseId: reserved.result.state.leaseId,
            projectId,
            taskId: task.taskId,
            stageRunId: stageRun.stageRunId,
            attemptId: stageRun.attemptId,
            ...commandSnapshot(stageRun),
            fenceToken: 1,
            expectedRevision: 502,
            leaseDurationMs: 60_000,
          }),
        );
        assert.equal(lateGap._tag, "Failure");
        if (lateGap._tag === "Failure") {
          assert.equal(lateGap.failure.code, "lease-projection-corrupt");
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${lateGapCommandId}
          `)[0]!.count,
          0,
        );
        yield* sql`
          INSERT INTO agent_control_events (
            sequence, event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            ${removed.sequence}, ${removed.eventId}, 'stage-run-lease',
            ${reserved.result.state.leaseId}, 501,
            ${removed.eventType}, ${removed.occurredAt}, ${removed.commandId}, NULL,
            ${removed.correlationId}, ${removed.authority}, ${removed.payloadJson},
            ${removed.metadataJson}
          )
        `;
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
