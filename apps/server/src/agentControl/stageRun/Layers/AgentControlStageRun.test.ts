import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlTaskId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlTaskState,
  CommandId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import {
  deriveAgentControlSourceIdentityFingerprint,
  deriveAgentControlStageRunId,
} from "../identity.ts";
import { AgentControlStageRun } from "../Services/AgentControlStageRun.ts";
import { AgentControlStageRunEngine } from "../Services/AgentControlStageRunEngine.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const at = "2026-07-24T10:00:00.000Z";
const repository = {
  repositoryNodeId: "stage-run-repository-node",
  nameWithOwner: "owner/repository",
} as const;

const issue = (
  projectId: ProjectId,
  overrides: Partial<AgentControlGithubIssueSnapshot> = {},
): AgentControlGithubIssueSnapshot => ({
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
  ...overrides,
});

const taskFrom = (
  projectId: ProjectId,
  source: AgentControlGithubIssueSnapshot,
  overrides: Partial<AgentControlTaskState> = {},
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
  ...overrides,
});

const seedConsumable = Effect.fn("seedConsumable")(function* (
  projectId: ProjectId,
  options?: {
    readonly mode?: "observe" | "manual" | "paused";
    readonly deleted?: boolean;
    readonly task?: Partial<AgentControlTaskState>;
    readonly issue?: Partial<AgentControlGithubIssueSnapshot>;
    readonly watermarkTarget?: number;
    readonly watermarkCompleted?: number;
    readonly corruptTask?: boolean;
  },
) {
  const sql = yield* SqlClient.SqlClient;
  const github = yield* AgentControlGithubStateRepository;
  const tasks = yield* AgentControlTaskStateRepository;
  const source = issue(projectId, options?.issue);
  const task = taskFrom(projectId, source, options?.task);

  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Stage run test', ${`/tmp/${projectId}`}, NULL, '[]',
      ${at}, ${at}, ${options?.deleted === true ? at : null}
    )
  `;
  yield* sql`
    INSERT INTO agent_control_project_states (
      project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
    ) VALUES (${projectId}, ${options?.mode ?? "observe"}, NULL, 1, 1, ${at})
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
  if (options?.corruptTask === true) {
    yield* sql`
      UPDATE agent_control_task_states SET state_json = '{}'
      WHERE task_id = ${task.taskId}
    `;
  }
  yield* sql`
    INSERT INTO agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence,
      revision, status, updated_at
    ) VALUES (
      ${projectId}, ${options?.watermarkTarget ?? 1},
      ${options?.watermarkCompleted ?? 1}, 1,
      ${
        (options?.watermarkTarget ?? 1) === (options?.watermarkCompleted ?? 1)
          ? "completed"
          : "reconciling"
      }, ${at}
    )
  `;
  return { task, source };
});

const prepare = (projectId: ProjectId, taskId: AgentControlTaskId, commandId: string) =>
  AgentControlStageRun.pipe(
    Effect.flatMap((service) =>
      service.prepareInitial({
        commandId: CommandId.make(commandId),
        projectId,
        taskId,
      }),
    ),
  );

layer("AgentControl stage-run foundation", (it) => {
  it.effect("prepares the canonical current candidate and publishes only after commit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const service = yield* AgentControlStageRun;
      const engine = yield* AgentControlStageRunEngine;
      const projectId = ProjectId.make("stage-run-prepare");
      const { task } = yield* seedConsumable(projectId);
      const eventFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      const result = yield* service.prepareInitial({
        commandId: CommandId.make("stage-run-prepare-command"),
        projectId,
        taskId: task.taskId,
      });
      const published = yield* Fiber.join(eventFiber);
      assert.equal(published._tag, "Some");
      if (published._tag === "Some") {
        assert.equal(published.value.sequence, result.resultSequence);
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = 'stage-run-prepare-command'
          `)[0]?.count,
          1,
        );
      }
      assert.equal(result.eventCreated, true);
      assert.equal(result.state.status, "prepared");
      assert.equal(result.state.stageKind, "planning");
      assert.equal(result.state.roleId, "planning");
      assert.equal(result.state.attemptOrdinal, 1);
      assert.equal(result.state.taskRevision, task.revision);
      assert.equal(result.state.githubIntakeSequence, task.githubIntakeSequence);
      assert.equal(
        result.state.stageRunId,
        yield* deriveAgentControlStageRunId({
          projectId,
          taskId: task.taskId,
          taskRevision: task.revision,
          githubIntakeSequence: task.githubIntakeSequence,
          sourceIdentityFingerprint: result.state.sourceIdentityFingerprint,
          stageKind: "planning",
          stageOrdinal: 1,
        }),
      );
      assert.equal(
        result.state.sourceIdentityFingerprint,
        yield* deriveAgentControlSourceIdentityFingerprint(task),
      );
      assert.equal(
        (yield* service.getStageRun({ projectId, taskId: task.taskId })).stageRunId,
        result.state.stageRunId,
      );
      assert.equal((yield* service.listStageRuns({ projectId })).stageRuns.length, 1);
      const taskRow = (yield* sql<{ readonly status: string }>`
        SELECT status FROM agent_control_task_states WHERE task_id = ${task.taskId}
      `)[0];
      assert.equal(taskRow?.status, "candidate");
    }),
  );

  it.effect("rejects every stale or inactive admission boundary with durable receipts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const cases = [
        {
          suffix: "mode",
          options: { mode: "manual" as const },
          expected: "project-mode-inactive",
        },
        {
          suffix: "delete",
          options: { deleted: true },
          expected: "project-unavailable",
        },
        {
          suffix: "watermark",
          options: { watermarkTarget: 2, watermarkCompleted: 1 },
          expected: "source-watermark-stale",
        },
        {
          suffix: "noncandidate",
          options: { task: { status: "needs-attention" as const } },
          expected: "task-not-candidate",
        },
        {
          suffix: "ineligible",
          options: { task: { sourceGate: "paused" as const } },
          expected: "task-ineligible",
        },
        {
          suffix: "corrupt",
          options: { corruptTask: true },
          expected: "task-projection-corrupt",
        },
      ] as const;
      for (const testCase of cases) {
        const projectId = ProjectId.make(`stage-run-reject-${testCase.suffix}`);
        const { task } = yield* seedConsumable(projectId, testCase.options);
        const commandId = `stage-run-reject-command-${testCase.suffix}`;
        const result = yield* Effect.result(prepare(projectId, task.taskId, commandId));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.code, testCase.expected);
        assert.deepStrictEqual(
          yield* sql`
            SELECT status, error_code FROM agent_control_command_receipts
            WHERE command_id = ${commandId}
          `,
          [{ status: "rejected", error_code: testCase.expected }],
        );
      }

      const missingProject = ProjectId.make("stage-run-reject-missing");
      const { task: missingTask } = yield* seedConsumable(missingProject);
      yield* sql`
        DELETE FROM agent_control_task_states WHERE task_id = ${missingTask.taskId}
      `;
      const missing = yield* Effect.result(
        prepare(missingProject, missingTask.taskId, "stage-run-missing-task-command"),
      );
      assert.equal(missing._tag, "Failure");
      if (missing._tag === "Failure") assert.equal(missing.failure.code, "task-missing");
    }),
  );

  it.effect("rejects stale GitHub state and derives identity from the latest task revision", () =>
    Effect.gen(function* () {
      const tasks = yield* AgentControlTaskStateRepository;
      const staleProject = ProjectId.make("stage-run-stale-github");
      const { task: staleTask } = yield* seedConsumable(staleProject, {
        task: { githubIntakeSequence: 2 },
      });
      const stale = yield* Effect.result(
        prepare(staleProject, staleTask.taskId, "stage-run-stale-github-command"),
      );
      assert.equal(stale._tag, "Failure");
      if (stale._tag === "Failure") {
        assert.include(
          ["source-snapshot-unavailable", "source-snapshot-stale"],
          stale.failure.code,
        );
      }

      const revisionProject = ProjectId.make("stage-run-task-revision");
      const { task } = yield* seedConsumable(revisionProject);
      const revised = { ...task, revision: 2, sequence: 2, updatedAt: `${at}-revision-2` };
      yield* tasks.save(revised, 1);
      const prepared = yield* prepare(
        revisionProject,
        task.taskId,
        "stage-run-task-revision-command",
      );
      assert.equal(prepared.state.taskRevision, 2);
      assert.equal(
        prepared.state.stageRunId,
        yield* deriveAgentControlStageRunId({
          projectId: revisionProject,
          taskId: task.taskId,
          taskRevision: 2,
          githubIntakeSequence: 1,
          sourceIdentityFingerprint: prepared.state.sourceIdentityFingerprint,
          stageKind: "planning",
          stageOrdinal: 1,
        }),
      );
    }),
  );

  it.effect("does not receipt transient repository failures and retries the same command", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("stage-run-transient-repository");
      const { task } = yield* seedConsumable(projectId);
      const commandId = "stage-run-transient-repository-command";
      yield* sql`
        ALTER TABLE agent_control_task_states
        RENAME TO agent_control_task_states_transient_failure
      `;
      const failed = yield* Effect.result(prepare(projectId, task.taskId, commandId));
      assert.equal(failed._tag, "Failure");
      if (failed._tag === "Failure") {
        assert.equal(failed.failure.code, "internal-persistence-error");
      }
      yield* sql`
        ALTER TABLE agent_control_task_states_transient_failure
        RENAME TO agent_control_task_states
      `;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${commandId}
        `)[0]?.count,
        0,
      );
      const retried = yield* prepare(projectId, task.taskId, commandId);
      assert.equal(retried.eventCreated, true);

      const stageReadCommandId = "stage-run-transient-stage-read-command";
      yield* sql`
        ALTER TABLE agent_control_stage_run_states
        RENAME TO agent_control_stage_run_states_transient_failure
      `;
      const stageReadFailed = yield* Effect.result(
        prepare(projectId, task.taskId, stageReadCommandId),
      );
      assert.equal(stageReadFailed._tag, "Failure");
      if (stageReadFailed._tag === "Failure") {
        assert.equal(stageReadFailed.failure.code, "internal-persistence-error");
      }
      yield* sql`
        ALTER TABLE agent_control_stage_run_states_transient_failure
        RENAME TO agent_control_stage_run_states
      `;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${stageReadCommandId}
        `)[0]?.count,
        0,
      );
      const stageReadRetried = yield* prepare(projectId, task.taskId, stageReadCommandId);
      assert.equal(stageReadRetried.eventCreated, false);

      const projectReadCommandId = "stage-run-transient-project-read-command";
      yield* sql`
        ALTER TABLE projection_projects
        RENAME TO projection_projects_transient_failure
      `;
      const service = yield* AgentControlStageRun;
      const projectGetFailed = yield* Effect.result(
        service.getStageRun({ projectId, taskId: task.taskId }),
      );
      assert.equal(projectGetFailed._tag, "Failure");
      if (projectGetFailed._tag === "Failure") {
        assert.equal(projectGetFailed.failure.code, "internal-persistence-error");
      }
      const projectListFailed = yield* Effect.result(service.listStageRuns({ projectId }));
      assert.equal(projectListFailed._tag, "Failure");
      if (projectListFailed._tag === "Failure") {
        assert.equal(projectListFailed.failure.code, "internal-persistence-error");
      }
      const projectPrepareFailed = yield* Effect.result(
        prepare(projectId, task.taskId, projectReadCommandId),
      );
      assert.equal(projectPrepareFailed._tag, "Failure");
      if (projectPrepareFailed._tag === "Failure") {
        assert.equal(projectPrepareFailed.failure.code, "internal-persistence-error");
      }
      yield* sql`
        ALTER TABLE projection_projects_transient_failure
        RENAME TO projection_projects
      `;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${projectReadCommandId}
        `)[0]?.count,
        0,
      );
      const projectReadRetried = yield* prepare(projectId, task.taskId, projectReadCommandId);
      assert.equal(projectReadRetried.eventCreated, false);
    }),
  );

  it.effect("replays receipts before later mode/delete checks and closes fingerprint reuse", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("stage-run-receipt-replay");
      const { task } = yield* seedConsumable(projectId);
      const input = {
        commandId: CommandId.make("stage-run-receipt-replay-command"),
        projectId,
        taskId: task.taskId,
      };
      const service = yield* AgentControlStageRun;
      const accepted = yield* service.prepareInitial(input);
      yield* sql`
        UPDATE agent_control_project_states SET mode = 'paused'
        WHERE project_id = ${projectId}
      `;
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${at}
        WHERE project_id = ${projectId}
      `;
      const replay = yield* service.prepareInitial(input);
      assert.equal(replay.state.stageRunId, accepted.state.stageRunId);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${accepted.state.stageRunId}
        `)[0]?.count,
        1,
      );

      const conflict = yield* Effect.result(
        service.prepareInitial({
          ...input,
          taskId: AgentControlTaskId.make("different-task"),
        }),
      );
      assert.equal(conflict._tag, "Failure");
      if (conflict._tag === "Failure") {
        assert.equal(conflict.failure.code, "command-identity-mismatch");
      }

      const rejectedProject = ProjectId.make("stage-run-rejected-replay");
      const { task: rejectedTask } = yield* seedConsumable(rejectedProject, {
        mode: "manual",
      });
      const rejectedInput = {
        commandId: CommandId.make("stage-run-rejected-replay-command"),
        projectId: rejectedProject,
        taskId: rejectedTask.taskId,
      };
      yield* Effect.result(service.prepareInitial(rejectedInput));
      yield* sql`
        UPDATE agent_control_project_states SET mode = 'observe'
        WHERE project_id = ${rejectedProject}
      `;
      const rejectedReplay = yield* Effect.result(service.prepareInitial(rejectedInput));
      assert.equal(rejectedReplay._tag, "Failure");
      if (rejectedReplay._tag === "Failure") {
        assert.equal(rejectedReplay.failure.code, "command-previously-rejected");
      }
    }),
  );

  it.effect("fully binds internal accepted receipt replay before returning it", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const projectId = ProjectId.make("stage-run-internal-replay");
      const { task } = yield* seedConsumable(projectId);
      const commandId = CommandId.make("stage-run-internal-replay-command");
      const accepted = yield* prepare(projectId, task.taskId, commandId);
      const engine = yield* AgentControlStageRunEngine;
      const receipt = (yield* sql<{ readonly commandFingerprint: string }>`
        SELECT command_fingerprint AS "commandFingerprint"
        FROM agent_control_command_receipts
        WHERE command_id = ${commandId}
      `)[0]!;
      const command = {
        type: "agentControl.stageRun.prepare" as const,
        commandId,
        projectId,
        taskId: task.taskId,
        stageRunId: accepted.state.stageRunId,
        attemptId: accepted.state.attemptId,
        roleId: accepted.state.roleId,
        stageKind: accepted.state.stageKind,
        stageOrdinal: accepted.state.stageOrdinal,
        attemptOrdinal: accepted.state.attemptOrdinal,
        taskRevision: accepted.state.taskRevision,
        githubIntakeSequence: accepted.state.githubIntakeSequence,
        sourceIdentityFingerprint: accepted.state.sourceIdentityFingerprint,
        expectedRevision: 0,
      };
      const eventFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const identical = yield* engine.dispatchPreparedController(
        command,
        receipt.commandFingerprint,
      );
      assert.equal(identical._tag, "Accepted");
      if (identical._tag === "Accepted") assert.equal(identical.events.length, 0);

      const mismatches = [
        { ...command, taskId: AgentControlTaskId.make("different-internal-task") },
        { ...command, attemptId: AgentControlAttemptId.make("different-attempt") },
        { ...command, roleId: AgentControlRoleId.make("implementation") },
        { ...command, stageKind: "implementation" as const },
        { ...command, taskRevision: command.taskRevision + 1 },
        { ...command, githubIntakeSequence: command.githubIntakeSequence + 1 },
        { ...command, sourceIdentityFingerprint: "b".repeat(64) },
        {
          ...command,
          type: "agentControl.stageRun.status.set" as const,
          status: "queued" as const,
        },
      ] as const;
      for (const mismatch of mismatches) {
        const result = yield* Effect.result(
          engine.dispatchPreparedController(mismatch, receipt.commandFingerprint),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "command-identity-mismatch");
        }
      }
      yield* Effect.yieldNow;
      assert.equal(eventFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(eventFiber);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run' AND command_id = ${commandId}
        `)[0]?.count,
        1,
      );
    }),
  );

  it.effect("serializes parallel prepares and rolls back event/projection before receipt", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const identicalProject = ProjectId.make("stage-run-parallel-identical");
      const { task: identicalTask } = yield* seedConsumable(identicalProject);
      const identical = yield* Effect.all(
        [
          prepare(identicalProject, identicalTask.taskId, "stage-run-parallel-identical-command"),
          prepare(identicalProject, identicalTask.taskId, "stage-run-parallel-identical-command"),
        ],
        { concurrency: 2 },
      );
      assert.equal(identical[0]?.state.stageRunId, identical[1]?.state.stageRunId);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run'
            AND stream_id = ${identical[0]!.state.stageRunId}
        `)[0]?.count,
        1,
      );

      const projectId = ProjectId.make("stage-run-parallel");
      const { task } = yield* seedConsumable(projectId);
      const results = yield* Effect.all(
        [
          prepare(projectId, task.taskId, "stage-run-parallel-one"),
          prepare(projectId, task.taskId, "stage-run-parallel-two"),
        ],
        { concurrency: 2 },
      );
      assert.equal(results[0]?.state.stageRunId, results[1]?.state.stageRunId);
      assert.equal(results.filter((result) => result.eventCreated).length, 1);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run'
            AND stream_id = ${results[0]!.state.stageRunId}
        `)[0]?.count,
        1,
      );

      const rollbackProject = ProjectId.make("stage-run-rollback");
      const { task: rollbackTask } = yield* seedConsumable(rollbackProject);
      const engine = yield* AgentControlStageRunEngine;
      const eventFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* sql`
        CREATE TRIGGER fail_stage_run_receipt_before_insert
        BEFORE INSERT ON agent_control_command_receipts
        WHEN NEW.command_id = 'stage-run-rollback-command'
        BEGIN
          SELECT RAISE(ABORT, 'receipt blocked');
        END
      `;
      const rolledBack = yield* Effect.result(
        prepare(rollbackProject, rollbackTask.taskId, "stage-run-rollback-command"),
      );
      assert.equal(rolledBack._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run'
            AND stream_id LIKE 'stage-run-%'
            AND command_id = 'stage-run-rollback-command'
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_stage_run_states
          WHERE project_id = ${rollbackProject}
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'stage-run-rollback-command'
        `)[0]?.count,
        0,
      );
      yield* Effect.yieldNow;
      assert.equal(eventFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(eventFiber);
      yield* sql`DROP TRIGGER fail_stage_run_receipt_before_insert`;
      const retried = yield* prepare(
        rollbackProject,
        rollbackTask.taskId,
        "stage-run-rollback-command",
      );
      assert.equal(retried.eventCreated, true);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'stage-run-rollback-command'
        `)[0]?.count,
        1,
      );
    }),
  );

  it.effect("does not publish no-op or receipt replay events", () =>
    Effect.gen(function* () {
      const projectId = ProjectId.make("stage-run-no-publish");
      const { task } = yield* seedConsumable(projectId);
      const first = yield* prepare(projectId, task.taskId, "stage-run-no-publish-first");
      assert.equal(first.eventCreated, true);
      const engine = yield* AgentControlStageRunEngine;

      const noOpFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const noOp = yield* prepare(projectId, task.taskId, "stage-run-no-publish-noop");
      assert.equal(noOp.eventCreated, false);
      yield* Effect.yieldNow;
      assert.equal(noOpFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(noOpFiber);

      const replayFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const replay = yield* prepare(projectId, task.taskId, "stage-run-no-publish-first");
      assert.equal(replay.eventCreated, true);
      yield* Effect.yieldNow;
      assert.equal(replayFiber.pollUnsafe(), undefined);
      yield* Fiber.interrupt(replayFiber);
    }),
  );

  it.effect("rebuilds only stage-run projection and quarantines corrupt list rows", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const service = yield* AgentControlStageRun;
      const engine = yield* AgentControlStageRunEngine;
      const projectId = ProjectId.make("stage-run-rebuild");
      const { task } = yield* seedConsumable(projectId);
      const prepared = yield* prepare(projectId, task.taskId, "stage-run-rebuild-command");
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'stage-run-rebuild-mixed-task-event', 'task',
          'stage-run-rebuild-mixed-task', 1, 'agentControl.task.created',
          ${at}, 'stage-run-rebuild-mixed-task-command', NULL,
          'stage-run-rebuild-mixed-task-command', 'controller', '{}',
          '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_projection_state (
          projector_name, last_applied_sequence, updated_at
        ) VALUES ('unrelated-stage-run-test-projector', 77, ${at})
      `;
      const taskCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_task_states
      `)[0]!.count;
      const receiptCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
      `)[0]!.count;
      yield* engine.rebuild;
      assert.equal(
        (yield* service.getStageRun({ projectId, taskId: task.taskId })).stageRunId,
        prepared.state.stageRunId,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_task_states
        `)[0]?.count,
        taskCount,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
        `)[0]?.count,
        receiptCount,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT last_applied_sequence FROM agent_control_projection_state
          WHERE projector_name = 'unrelated-stage-run-test-projector'
        `,
        [{ last_applied_sequence: 77 }],
      );

      yield* sql`
        UPDATE agent_control_stage_run_states SET state_json = '{}'
        WHERE stage_run_id = ${prepared.state.stageRunId}
      `;
      const listed = yield* service.listStageRuns({ projectId });
      assert.equal(listed.stageRuns.length, 0);
      assert.equal(listed.quarantinedCount, 1);
      const direct = yield* Effect.result(service.getStageRun({ projectId, taskId: task.taskId }));
      assert.equal(direct._tag, "Failure");
      if (direct._tag === "Failure") {
        assert.equal(direct.failure.code, "stage-run-projection-corrupt");
      }
      const prepareCorrupt = yield* Effect.result(
        prepare(projectId, task.taskId, "stage-run-corrupt-prepare-command"),
      );
      assert.equal(prepareCorrupt._tag, "Failure");
      if (prepareCorrupt._tag === "Failure") {
        assert.equal(prepareCorrupt.failure.code, "stage-run-projection-corrupt");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${prepared.state.stageRunId}
        `)[0]?.count,
        1,
      );
    }),
  );

  it.effect("validates every historical planning run before get or prepare", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const tasks = yield* AgentControlTaskStateRepository;
      const service = yield* AgentControlStageRun;
      const projectId = ProjectId.make("stage-run-history-corrupt");
      const { task } = yield* seedConsumable(projectId);
      const older = yield* prepare(projectId, task.taskId, "stage-run-history-older");
      const revisionTwo = {
        ...task,
        revision: 2,
        sequence: 2,
        updatedAt: "2026-07-24T10:01:00.000Z",
      };
      yield* tasks.save(revisionTwo, 1);
      const newer = yield* prepare(projectId, task.taskId, "stage-run-history-newer");
      assert.notEqual(older.state.stageRunId, newer.state.stageRunId);
      assert.equal(
        (yield* service.getStageRun({ projectId, taskId: task.taskId })).stageRunId,
        newer.state.stageRunId,
      );

      yield* sql`
        UPDATE agent_control_stage_run_states
        SET status = 'queued', state_json = json_set(state_json, '$.status', 'queued')
        WHERE stage_run_id = ${older.state.stageRunId}
      `;
      const getCorrupt = yield* Effect.result(
        service.getStageRun({ projectId, taskId: task.taskId }),
      );
      assert.equal(getCorrupt._tag, "Failure");
      if (getCorrupt._tag === "Failure") {
        assert.equal(getCorrupt.failure.code, "stage-run-projection-corrupt");
      }

      yield* tasks.save(
        {
          ...revisionTwo,
          revision: 3,
          sequence: 3,
          updatedAt: "2026-07-24T10:02:00.000Z",
        },
        2,
      );
      const before = (yield* sql<{
        readonly events: number;
        readonly states: number;
        readonly receipts: number;
      }>`
        SELECT
          (SELECT COUNT(*) FROM agent_control_events WHERE aggregate_kind = 'stage-run') AS events,
          (SELECT COUNT(*) FROM agent_control_stage_run_states) AS states,
          (SELECT COUNT(*) FROM agent_control_command_receipts) AS receipts
      `)[0]!;
      const blocked = yield* Effect.result(
        prepare(projectId, task.taskId, "stage-run-history-blocked"),
      );
      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.equal(blocked.failure.code, "stage-run-projection-corrupt");
      }
      assert.deepStrictEqual(
        (yield* sql`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events WHERE aggregate_kind = 'stage-run') AS events,
            (SELECT COUNT(*) FROM agent_control_stage_run_states) AS states,
            (SELECT COUNT(*) FROM agent_control_command_receipts) AS receipts
        `)[0],
        before,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'stage-run-history-blocked'
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("rejects consistently manipulated projection columns and json", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const service = yield* AgentControlStageRun;
      const cases = [
        "queued",
        "running",
        "attempt-2",
        "attempt-4",
        "role",
        "stage",
        "identity",
      ] as const;
      for (const kind of cases) {
        const projectId = ProjectId.make(`stage-run-consistent-${kind}`);
        const { task } = yield* seedConsumable(projectId);
        const prepared = yield* prepare(
          projectId,
          task.taskId,
          `stage-run-consistent-command-${kind}`,
        );
        switch (kind) {
          case "queued":
          case "running":
            yield* sql`
              UPDATE agent_control_stage_run_states
              SET status = ${kind}, state_json = json_set(state_json, '$.status', ${kind})
              WHERE stage_run_id = ${prepared.state.stageRunId}
            `;
            break;
          case "attempt-2":
          case "attempt-4": {
            const ordinal = kind === "attempt-2" ? 2 : 4;
            const attemptId = `manipulated-${kind}`;
            yield* sql`
              UPDATE agent_control_stage_run_states
              SET attempt_id = ${attemptId}, attempt_ordinal = ${ordinal},
                state_json = json_set(
                  state_json, '$.attemptId', ${attemptId}, '$.attemptOrdinal', ${ordinal}
                )
              WHERE stage_run_id = ${prepared.state.stageRunId}
            `;
            break;
          }
          case "role":
            yield* sql`
              UPDATE agent_control_stage_run_states
              SET role_id = 'implementation',
                state_json = json_set(state_json, '$.roleId', 'implementation')
              WHERE stage_run_id = ${prepared.state.stageRunId}
            `;
            break;
          case "stage":
            yield* sql`
              UPDATE agent_control_stage_run_states
              SET stage_kind = 'implementation',
                state_json = json_set(state_json, '$.stageKind', 'implementation')
              WHERE stage_run_id = ${prepared.state.stageRunId}
            `;
            break;
          case "identity":
            yield* sql`
              UPDATE agent_control_stage_run_states
              SET stage_run_id = ${`manipulated-stage-run-${projectId}`},
                attempt_id = ${`manipulated-attempt-${projectId}`},
                state_json = json_set(
                  state_json,
                  '$.stageRunId', ${`manipulated-stage-run-${projectId}`},
                  '$.attemptId', ${`manipulated-attempt-${projectId}`}
                )
              WHERE stage_run_id = ${prepared.state.stageRunId}
            `;
            break;
        }
        const result = yield* Effect.result(
          service.getStageRun({ projectId, taskId: task.taskId }),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "stage-run-projection-corrupt");
        }
      }
    }),
  );

  it.effect("fails rebuild closed for schema-valid non-derived ids", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlStageRunEngine;
      const projectId = ProjectId.make("stage-run-rebuild-derived");
      const { task } = yield* seedConsumable(projectId);
      yield* prepare(projectId, task.taskId, "stage-run-rebuild-derived-command");
      const original = (yield* sql<{
        readonly streamId: string;
        readonly payload: string;
      }>`
        SELECT stream_id AS "streamId", payload_json AS payload
        FROM agent_control_events
        WHERE command_id = 'stage-run-rebuild-derived-command'
      `)[0]!;
      yield* sql`
        UPDATE agent_control_events
        SET stream_id = 'schema-valid-but-not-derived',
          payload_json = json_set(
            payload_json,
            '$.stageRunId', 'schema-valid-but-not-derived',
            '$.attemptId', 'schema-valid-but-not-derived-attempt'
          )
        WHERE command_id = 'stage-run-rebuild-derived-command'
      `;
      const rebuilt = yield* Effect.result(engine.rebuild);
      assert.equal(rebuilt._tag, "Failure");
      if (rebuilt._tag === "Failure") {
        assert.equal(rebuilt.failure.code, "stage-run-projection-corrupt");
      }
      yield* sql`
        UPDATE agent_control_events
        SET stream_id = ${original.streamId}, payload_json = ${original.payload}
        WHERE command_id = 'stage-run-rebuild-derived-command'
      `;
      yield* engine.rebuild;
    }),
  );
});
