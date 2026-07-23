import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  CommandId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlGithubPollStatus,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { deriveAgentControlTaskId } from "../identity.ts";
import { AGENT_CONTROL_TASK_PROJECTOR } from "../projector.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntake } from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskProjection } from "../Services/AgentControlTaskProjection.ts";
import { AgentControlEngine as AgentControlProjectEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const repository = {
  repositoryNodeId: "repo-node-main",
  nameWithOwner: "owner/repository",
} as const;
const now = "2026-07-23T10:00:00.000Z";

const issue = (
  number: number,
  overrides: Partial<AgentControlGithubIssueSnapshot> = {},
): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: `issue-node-${number}`,
  number,
  url: `https://github.test/owner/repository/issues/${number}`,
  state: "open",
  title: `untrusted title ${number}`,
  body: `untrusted body ${number}`,
  contentTrust: "untrusted-external",
  updatedAt: now,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
  ...overrides,
});

const addProject = (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  deletedAt: string | null = null,
) =>
  sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Task intake test', ${`/tmp/${projectId}`}, NULL, '[]',
      ${now}, ${now}, ${deletedAt}
    )
  `;

const setGithubSnapshot = Effect.fn("setGithubSnapshot")(function* (
  projectId: ProjectId,
  issues: ReadonlyArray<AgentControlGithubIssueSnapshot>,
  pollStatus?: AgentControlGithubPollStatus,
) {
  const states = yield* AgentControlGithubStateRepository;
  const current = yield* states.get(projectId);
  const revision = Option.match(current, {
    onNone: () => 1,
    onSome: (state) => state.revision + 1,
  });
  const sequence = Option.match(current, {
    onNone: () => 1,
    onSome: (state) => state.sequence + 1,
  });
  yield* states.save(
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
        revision,
        sequence,
        updatedAt: now,
      },
      cursor: {
        lastSuccessfulPollAt: now,
        overlapSeconds: 60,
      },
      pollStatus: pollStatus ?? {
        status: "success",
        attemptedAt: now,
        completedAt: now,
        errorCode: null,
        issueCount: issues.length,
      },
      revision,
      sequence,
      updatedAt: now,
    },
    revision - 1,
  );
  yield* states.replaceIssues(projectId, issues);
});

const validTasks = Effect.fn("validTasks")(function* (projectId: ProjectId) {
  const states = yield* AgentControlTaskStateRepository;
  const entries = yield* states.listProject(projectId);
  return entries.flatMap((entry) => (entry._tag === "Valid" ? [entry.state] : []));
});

layer("AgentControl task intake", (it) => {
  it.effect("derives stable task ids from the complete source identity", () =>
    Effect.gen(function* () {
      const source = {
        projectId: ProjectId.make("task-id-project"),
        repositoryNodeId: "repo-a",
        issueNodeId: "issue-a",
      };
      const first = yield* deriveAgentControlTaskId(source);
      const replay = yield* deriveAgentControlTaskId(source);
      const otherIssue = yield* deriveAgentControlTaskId({
        ...source,
        issueNodeId: "issue-b",
      });
      const otherRepository = yield* deriveAgentControlTaskId({
        ...source,
        repositoryNodeId: "repo-b",
      });
      assert.equal(first, replay);
      assert.notEqual(first, otherIssue);
      assert.notEqual(first, otherRepository);
    }),
  );

  it.effect("creates eligible candidates and repeats the same source snapshot as a no-op", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-reconcile-create");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(1)]);

      const first = yield* intake.reconcileOnce({ projectId });
      assert.equal(first.createdCount, 1);
      const tasks = yield* intake.listTasks({ projectId });
      assert.equal(tasks.tasks.length, 1);
      assert.equal(tasks.tasks[0]?.status, "candidate");
      assert.equal(tasks.tasks[0]?.sourceGate, "eligible");
      assert.notProperty(tasks.tasks[0] ?? {}, "body");
      assert.equal(
        yield* (yield* AgentControlProjectEngine)
          .getProjectState({ projectId })
          .pipe(Effect.map((state) => state.mode)),
        "manual",
      );
      const receiptCount = (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE aggregate_kind = 'task'
        `)[0]!.count;

      const replay = yield* intake.reconcileOnce({ projectId });
      assert.equal(replay.createdCount, 0);
      assert.equal(replay.updatedCount, 0);
      assert.equal(replay.unchangedCount, 1);
      assert.equal((yield* validTasks(projectId))[0]?.revision, 1);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE aggregate_kind = 'task'
          `)[0]!.count,
        receiptCount,
      );
    }),
  );

  it.effect("refreshes not-ready, paused, closed, and invalid timeline gates independently", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-reconcile-gates");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(1), issue(2), issue(3), issue(4)]);
      yield* intake.reconcileOnce({ projectId });

      yield* setGithubSnapshot(projectId, [
        issue(1, {
          updatedAt: "2026-07-23T11:00:00.000Z",
          ready: false,
          eligible: false,
          eligibilityReason: "ready-inactive",
        }),
        issue(2, {
          updatedAt: "2026-07-23T11:00:00.000Z",
          paused: true,
          eligible: false,
          eligibilityReason: "paused",
        }),
        issue(3, {
          updatedAt: "2026-07-23T11:00:00.000Z",
          state: "closed",
          eligible: false,
          eligibilityReason: "closed",
        }),
        issue(4, {
          updatedAt: "2026-07-23T11:00:00.000Z",
          timelineComplete: false,
          eligible: false,
          eligibilityReason: "timeline-invalid",
        }),
      ]);
      const result = yield* intake.reconcileOnce({ projectId });
      assert.equal(result.updatedCount, 4);
      assert.deepStrictEqual(
        (yield* validTasks(projectId)).map((task) => task.sourceGate),
        ["not-ready", "paused", "closed", "timeline-invalid"],
      );
      assert.deepStrictEqual(
        (yield* validTasks(projectId)).map((task) => task.status),
        ["candidate", "candidate", "candidate", "candidate"],
      );
    }),
  );

  it.effect("fails closed for node-id reuse, issue transfer, and repository transfer", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-reconcile-identity");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(7)]);
      yield* intake.reconcileOnce({ projectId });

      yield* setGithubSnapshot(projectId, [
        issue(7, {
          issueNodeId: "replacement-node",
          url: "https://github.test/owner/repository/issues/7",
          updatedAt: "2026-07-23T11:00:00.000Z",
        }),
      ]);
      yield* intake.reconcileOnce({ projectId });
      let tasks = yield* validTasks(projectId);
      assert.equal(tasks.length, 1);
      assert.equal(tasks[0]?.source.issueNodeId, "issue-node-7");
      assert.equal(tasks[0]?.sourceGate, "identity-invalid");
      assert.equal(tasks[0]?.status, "needs-attention");

      const transferProjectId = ProjectId.make("task-reconcile-transfer");
      yield* addProject(sql, transferProjectId);
      yield* setGithubSnapshot(transferProjectId, [issue(8)]);
      yield* intake.reconcileOnce({ projectId: transferProjectId });
      yield* setGithubSnapshot(transferProjectId, [
        issue(8, {
          repositoryNodeId: "transferred-repository",
          updatedAt: "2026-07-23T11:00:00.000Z",
        }),
      ]);
      yield* intake.reconcileOnce({ projectId: transferProjectId });
      tasks = yield* validTasks(transferProjectId);
      assert.equal(tasks.length, 1);
      assert.equal(tasks[0]?.source.repositoryNodeId, repository.repositoryNodeId);
      assert.equal(tasks[0]?.sourceGate, "identity-invalid");
    }),
  );

  it.effect(
    "does not mark missing after an incomplete poll but does after a complete snapshot",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const intake = yield* AgentControlTaskIntake;
        const projectId = ProjectId.make("task-reconcile-missing");
        yield* addProject(sql, projectId);
        yield* setGithubSnapshot(projectId, [issue(9)]);
        yield* intake.reconcileOnce({ projectId });

        yield* setGithubSnapshot(projectId, [], {
          status: "needs-attention",
          attemptedAt: "2026-07-23T11:00:00.000Z",
          completedAt: "2026-07-23T11:00:01.000Z",
          errorCode: "github-timeout",
        });
        const incomplete = yield* Effect.result(intake.reconcileOnce({ projectId }));
        assert.equal(incomplete._tag, "Failure");
        assert.equal((yield* validTasks(projectId))[0]?.sourceGate, "eligible");

        yield* setGithubSnapshot(projectId, []);
        const complete = yield* intake.reconcileOnce({ projectId });
        assert.equal(complete.needsAttentionCount, 1);
        const tasks = yield* validTasks(projectId);
        assert.equal(tasks.length, 1);
        assert.equal(tasks[0]?.sourceGate, "source-missing");
        assert.equal(tasks[0]?.status, "needs-attention");
      }),
  );

  it.effect("persists typed rejection receipts and publishes only after commit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const receipts = yield* AgentControlCommandReceiptRepository;
      const projectId = ProjectId.make("task-command-receipts");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(10)]);

      const subscribed = yield* engine.subscribeDomainEvents;
      const eventFiber = yield* Effect.forkChild(Stream.runHead(subscribed));
      yield* intake.reconcileOnce({ projectId });
      const published = yield* Fiber.join(eventFiber);
      assert.equal(Option.isSome(published), true);
      const task = (yield* validTasks(projectId))[0]!;
      const acceptedReceipt = yield* receipts.getByCommandId(
        published.pipe(
          Option.map((event) => event.commandId),
          Option.getOrThrow,
        ),
      );
      assert.equal(Option.isSome(acceptedReceipt), true);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_task_states
            WHERE task_id = ${task.taskId}
          `)[0]?.count,
        1,
      );

      const duplicateIdentity = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-duplicate-source"),
          taskId: AgentControlTaskId.make("non-deterministic-duplicate"),
          projectId,
          expectedRevision: 0,
          source: task.source,
          sourceGate: task.sourceGate,
          sourceUpdatedAt: task.sourceUpdatedAt,
          githubIntakeSequence: task.githubIntakeSequence,
          sourceSnapshot: task.sourceSnapshot,
        }),
      );
      assert.equal(duplicateIdentity._tag, "Failure");
      if (duplicateIdentity._tag === "Failure") {
        assert.equal(duplicateIdentity.failure.code, "source-identity-conflict");
      }

      const revisionCommand = {
        type: "agentControl.task.sourceGate.refresh",
        commandId: CommandId.make("task-revision-conflict"),
        taskId: task.taskId,
        projectId,
        expectedRevision: 0,
        source: task.source,
        sourceGate: "not-ready",
        sourceUpdatedAt: task.sourceUpdatedAt,
        githubIntakeSequence: task.githubIntakeSequence,
        sourceSnapshot: {
          ...task.sourceSnapshot,
          ready: false,
          eligible: false,
          eligibilityReason: "ready-inactive",
        },
      } as const;
      const revisionConflict = yield* Effect.result(engine.dispatchController(revisionCommand));
      assert.equal(revisionConflict._tag, "Failure");
      if (revisionConflict._tag === "Failure") {
        assert.equal(revisionConflict.failure.code, "revision-conflict");
      }
      const revisionReplay = yield* Effect.result(engine.dispatchController(revisionCommand));
      assert.equal(revisionReplay._tag, "Failure");
      if (revisionReplay._tag === "Failure") {
        assert.equal(revisionReplay.failure.code, "command-previously-rejected");
      }

      const unavailableCommand = {
        type: "agentControl.task.status.set",
        commandId: CommandId.make("task-state-unavailable"),
        taskId: task.taskId,
        projectId,
        expectedRevision: task.revision,
        status: "running",
      } as const;
      const unavailable = yield* Effect.result(engine.dispatchController(unavailableCommand));
      assert.equal(unavailable._tag, "Failure");
      if (unavailable._tag === "Failure") {
        assert.equal(unavailable.failure.code, "state-not-available");
      }
      const replay = yield* Effect.result(engine.dispatchController(unavailableCommand));
      assert.equal(replay._tag, "Failure");
      if (replay._tag === "Failure") {
        assert.equal(replay.failure.code, "command-previously-rejected");
      }
      const mismatch = yield* Effect.result(
        engine.dispatchController({ ...unavailableCommand, status: "waiting" }),
      );
      assert.equal(mismatch._tag, "Failure");
      if (mismatch._tag === "Failure") {
        assert.equal(mismatch.failure.code, "command-identity-mismatch");
      }
      const receipt = yield* receipts.getByCommandId(unavailableCommand.commandId);
      assert.equal(Option.isSome(receipt), true);
      if (Option.isSome(receipt)) {
        assert.equal(receipt.value.status, "rejected");
        assert.equal(receipt.value.errorCode, "state-not-available");
      }
    }),
  );

  it.effect("quarantines corrupt enumeration rows and rebuilds only task projections", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projection = yield* AgentControlTaskProjection;
      const projectEngine = yield* AgentControlProjectEngine;
      const taskStates = yield* AgentControlTaskStateRepository;
      const projectId = ProjectId.make("task-projection-rebuild");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(11), issue(12)]);
      yield* intake.reconcileOnce({ projectId });
      yield* projectEngine.dispatchController({
        commandId: CommandId.make("task-rebuild-controller-mode"),
        projectId,
        expectedRevision: 0,
        mode: "observe",
      });
      yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (${projectId}, '{"fullAccess":false}', 1, ${now})
      `;
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_kind, payload_json, metadata_json
        ) VALUES (
          'task-rebuild-orchestration-event', 'project', ${projectId}, 1,
          'project.created', ${now}, 'task-rebuild-orchestration-command',
          NULL, 'task-rebuild-orchestration-command', 'client', '{}', '{}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, last_github_event_sequence,
          activity, circuit_state, consecutive_failures, last_attempt_at,
          next_attempt_at, cooldown_until, reason_code, updated_at, scheduler_revision
        ) VALUES (
          ${projectId}, '{}', 1, 1, 'active', 'closed', 0, NULL, NULL,
          NULL, NULL, ${now}, 1
        )
      `;

      yield* taskStates.deleteAll;
      yield* sql`
        DELETE FROM agent_control_projection_state
        WHERE projector_name = ${AGENT_CONTROL_TASK_PROJECTOR}
      `;
      assert.equal((yield* validTasks(projectId)).length, 0);
      yield* projection.bootstrap;
      const tasks = yield* validTasks(projectId);
      assert.equal(tasks.length, 2);
      const corruptTaskId = tasks[0]!.taskId;
      const receiptCountBefore = (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
        `)[0]!.count;
      const githubCountBefore = (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_github_issues
        `)[0]!.count;
      const schedulerCountBefore = (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_github_scheduler_states
          WHERE project_id = ${projectId}
        `)[0]!.count;
      yield* sql`
        UPDATE agent_control_task_states
        SET state_json = '{"untrusted":"corrupt"}'
        WHERE task_id = ${corruptTaskId}
      `;

      const listed = yield* intake.listTasks({ projectId });
      assert.equal(listed.tasks.length, 1);
      assert.equal(listed.quarantinedCount, 1);
      const globalEntries = yield* (yield* AgentControlTaskStateRepository).listAll;
      assert.equal(globalEntries.filter((entry) => entry._tag === "Valid").length >= 1, true);
      assert.equal(globalEntries.filter((entry) => entry._tag === "Corrupt").length >= 1, true);
      const direct = yield* Effect.result(intake.getTask({ projectId, taskId: corruptTaskId }));
      assert.equal(direct._tag, "Failure");

      yield* projection.rebuild;
      assert.equal((yield* intake.listTasks({ projectId })).tasks.length, 2);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
          `)[0]!.count,
        receiptCountBefore,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_github_issues
          `)[0]!.count,
        githubCountBefore,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_github_scheduler_states
            WHERE project_id = ${projectId}
          `)[0]!.count,
        schedulerCountBefore,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_project_policies
            WHERE project_id = ${projectId}
          `)[0]!.count,
        1,
      );
      assert.equal((yield* projectEngine.getProjectState({ projectId })).mode, "observe");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM projection_projects
            WHERE project_id = ${projectId}
          `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_events
            WHERE stream_id = ${projectId}
          `)[0]!.count,
        1,
      );
    }),
  );

  it.effect("rejects reconcile for deleted projects without mutating tasks", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-reconcile-deleted");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(13)]);
      yield* intake.reconcileOnce({ projectId });
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${now}
        WHERE project_id = ${projectId}
      `;
      const result = yield* Effect.result(intake.reconcileOnce({ projectId }));
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.code, "project-deleted");
      assert.equal((yield* validTasks(projectId)).length, 1);
    }),
  );

  it.effect("resumes safely after a partially committed deterministic command set", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const projectId = ProjectId.make("task-reconcile-partial");
      yield* addProject(sql, projectId);
      const firstIssue = issue(14);
      const secondIssue = issue(15);
      yield* setGithubSnapshot(projectId, [firstIssue, secondIssue]);

      const source = {
        projectId,
        repositoryNodeId: firstIssue.repositoryNodeId,
        issueNodeId: firstIssue.issueNodeId,
        issueNumber: firstIssue.number,
        issueUrl: firstIssue.url,
      };
      const taskId = yield* deriveAgentControlTaskId(source);
      yield* engine.dispatchController({
        type: "agentControl.task.createFromGithubIssue",
        commandId: CommandId.make("partial-first-command"),
        taskId,
        projectId,
        expectedRevision: 0,
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: firstIssue.updatedAt,
        githubIntakeSequence: 1,
        sourceSnapshot: {
          repositoryNodeId: firstIssue.repositoryNodeId,
          issueNodeId: firstIssue.issueNodeId,
          number: firstIssue.number,
          url: firstIssue.url,
          state: firstIssue.state,
          title: firstIssue.title,
          body: firstIssue.body,
          contentTrust: "untrusted-external",
          updatedAt: firstIssue.updatedAt,
          timelineComplete: firstIssue.timelineComplete,
          ready: firstIssue.ready,
          paused: firstIssue.paused,
          eligible: firstIssue.eligible,
          eligibilityReason: firstIssue.eligibilityReason,
        },
      });

      const resumed = yield* intake.reconcileOnce({ projectId });
      assert.equal(resumed.createdCount, 1);
      assert.equal(resumed.unchangedCount, 1);
      assert.equal((yield* validTasks(projectId)).length, 2);
      const replay = yield* intake.reconcileOnce({ projectId });
      assert.equal(replay.createdCount, 0);
      assert.equal(replay.updatedCount, 0);
      assert.equal((yield* validTasks(projectId)).length, 2);
    }),
  );
});
