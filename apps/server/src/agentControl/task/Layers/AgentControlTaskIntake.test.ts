import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  AgentControlTaskState,
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
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { deriveAgentControlTaskId } from "../identity.ts";
import { AGENT_CONTROL_TASK_PROJECTOR } from "../projector.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntake } from "../Services/AgentControlTaskIntake.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskProjection } from "../Services/AgentControlTaskProjection.ts";
import { AgentControlTaskReconcileStateRepository } from "../Services/AgentControlTaskReconcileState.ts";
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
const encodeTaskState = Schema.encodeUnknownEffect(Schema.fromJsonString(AgentControlTaskState));
const sourcePrecondition = (
  projectId: ProjectId,
  expectedIssueCount: number,
  githubIntakeSequence = 1,
) => ({
  schemaVersion: 1 as const,
  projectId,
  githubIntakeSequence,
  githubProjectionRevision: githubIntakeSequence,
  githubConfigRevision: githubIntakeSequence,
  repositoryNodeId: repository.repositoryNodeId,
  pollStatus: "success" as const,
  expectedIssueCount,
});

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
const taskSourceSnapshot = (value: AgentControlGithubIssueSnapshot) => ({
  repositoryNodeId: value.repositoryNodeId,
  issueNodeId: value.issueNodeId,
  number: value.number,
  url: value.url,
  state: value.state,
  title: value.title,
  body: value.body,
  contentTrust: "untrusted-external" as const,
  updatedAt: value.updatedAt,
  timelineComplete: value.timelineComplete,
  ready: value.ready,
  paused: value.paused,
  eligible: value.eligible,
  eligibilityReason: value.eligibilityReason,
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
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
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
    }),
  );
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
      const transferred = yield* Effect.result(
        intake.reconcileOnce({ projectId: transferProjectId }),
      );
      assert.equal(transferred._tag, "Failure");
      if (transferred._tag === "Failure") {
        assert.equal(transferred.failure.code, "source-snapshot-unavailable");
      }
      tasks = yield* validTasks(transferProjectId);
      assert.equal(tasks.length, 1);
      assert.equal(tasks[0]?.source.repositoryNodeId, repository.repositoryNodeId);
      assert.equal(tasks[0]?.sourceGate, "eligible");
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
          sourcePrecondition: sourcePrecondition(projectId, 1),
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
        sourcePrecondition: sourcePrecondition(projectId, 1),
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
        sourcePrecondition: sourcePrecondition(projectId, 2),
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

  it.effect("loads only coherent completed GitHub snapshots and rejects count mismatches", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const github = yield* AgentControlGithubStateRepository;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-atomic-source-snapshot");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(20, { title: "snapshot-sequence-1" })]);

      for (let sequence = 2; sequence <= 12; sequence += 1) {
        const [read] = yield* Effect.all(
          [
            github.getCompletedSnapshot(projectId),
            setGithubSnapshot(projectId, [
              issue(20, {
                title: `snapshot-sequence-${sequence}`,
                updatedAt: `2026-07-23T${String(sequence).padStart(2, "0")}:00:00.000Z`,
              }),
            ]),
          ],
          { concurrency: "unbounded" },
        );
        if (Option.isSome(read)) {
          assert.equal(read.value.issues.length, 1);
          assert.equal(
            read.value.issues[0]?.title,
            `snapshot-sequence-${read.value.sourcePrecondition.githubIntakeSequence}`,
          );
          assert.equal(read.value.sourcePrecondition.expectedIssueCount, read.value.issues.length);
        }
      }

      const [duringClear] = yield* Effect.all(
        [
          github.getCompletedSnapshot(projectId),
          sql.withTransaction(
            Effect.gen(function* () {
              const current = Option.getOrThrow(yield* github.get(projectId));
              yield* github.save(
                {
                  ...current,
                  config: null,
                  cursor: null,
                  pollStatus: {
                    status: "disabled",
                    attemptedAt: null,
                    completedAt: null,
                    errorCode: null,
                  },
                  revision: current.revision + 1,
                  sequence: current.sequence + 1,
                  updatedAt: now,
                },
                current.revision,
              );
              yield* github.deleteProject(projectId);
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      if (Option.isSome(duringClear)) {
        assert.equal(
          duringClear.value.issues.length,
          duringClear.value.sourcePrecondition.expectedIssueCount,
        );
        assert.equal(duringClear.value.issues[0]?.issueNodeId, "issue-node-20");
      }
      yield* setGithubSnapshot(projectId, [issue(20)]);

      const clearProjectId = ProjectId.make("task-atomic-config-clear");
      yield* addProject(sql, clearProjectId);
      yield* setGithubSnapshot(clearProjectId, [issue(200)]);
      yield* intake.reconcileOnce({ projectId: clearProjectId });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = Option.getOrThrow(yield* github.get(clearProjectId));
          yield* github.save(
            {
              ...current,
              config: null,
              cursor: null,
              pollStatus: {
                status: "disabled",
                attemptedAt: null,
                completedAt: null,
                errorCode: null,
              },
              revision: current.revision + 1,
              sequence: current.sequence + 1,
              updatedAt: now,
            },
            current.revision,
          );
          yield* github.deleteProject(clearProjectId);
        }),
      );
      const cleared = yield* Effect.result(intake.reconcileOnce({ projectId: clearProjectId }));
      assert.equal(cleared._tag, "Failure");
      assert.equal((yield* validTasks(clearProjectId))[0]?.sourceGate, "eligible");

      const invalidatedProjectId = ProjectId.make("task-atomic-identity-invalidation");
      yield* addProject(sql, invalidatedProjectId);
      yield* setGithubSnapshot(invalidatedProjectId, [issue(201)]);
      yield* intake.reconcileOnce({ projectId: invalidatedProjectId });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          const current = Option.getOrThrow(yield* github.get(invalidatedProjectId));
          const config = Option.getOrThrow(Option.fromNullishOr(current.config));
          yield* github.save(
            {
              ...current,
              cursor: null,
              pollStatus: {
                status: "needs-attention",
                attemptedAt: now,
                completedAt: now,
                errorCode: "repository-identity-changed",
              },
              revision: current.revision + 1,
              sequence: current.sequence + 1,
              updatedAt: now,
              config: {
                ...config,
                revision: current.revision + 1,
                sequence: current.sequence + 1,
                updatedAt: now,
              },
            },
            current.revision,
          );
          yield* github.deleteProject(invalidatedProjectId);
        }),
      );
      const invalidated = yield* Effect.result(
        intake.reconcileOnce({ projectId: invalidatedProjectId }),
      );
      assert.equal(invalidated._tag, "Failure");
      assert.equal((yield* validTasks(invalidatedProjectId))[0]?.sourceGate, "eligible");

      yield* setGithubSnapshot(projectId, [issue(20)], {
        status: "success",
        attemptedAt: now,
        completedAt: now,
        errorCode: null,
        issueCount: 2,
      });
      const mismatch = yield* Effect.result(intake.reconcileOnce({ projectId }));
      assert.equal(mismatch._tag, "Failure");
      if (mismatch._tag === "Failure") {
        assert.equal(mismatch.failure.code, "source-snapshot-unavailable");
      }
      assert.equal((yield* validTasks(projectId)).length, 0);
    }),
  );

  it.effect("serializes parallel reconciles and commits one completed watermark", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const reconciles = yield* AgentControlTaskReconcileStateRepository;
      const projectId = ProjectId.make("task-parallel-reconcile");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(21)]);

      const results = yield* Effect.all(
        [intake.reconcileOnce({ projectId }), intake.reconcileOnce({ projectId })],
        { concurrency: "unbounded" },
      );
      assert.equal(
        results.reduce((total, result) => total + result.createdCount, 0),
        1,
      );
      assert.equal((yield* validTasks(projectId))[0]?.revision, 1);
      const watermark = yield* reconciles.get(projectId);
      assert.equal(Option.isSome(watermark), true);
      if (Option.isSome(watermark)) {
        assert.equal(watermark.value.status, "completed");
        assert.equal(watermark.value.targetSequence, 1);
        assert.equal(watermark.value.lastCompletedSequence, 1);
      }
    }),
  );

  it.effect("retries the full project after a stale final snapshot check", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const reconciles = yield* AgentControlTaskReconcileStateRepository;
      const projectId = ProjectId.make("task-final-snapshot-retry");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(22)]);

      const subscribed = yield* engine.subscribeDomainEvents;
      const bump = yield* Effect.forkChild(
        Stream.runHead(subscribed).pipe(
          Effect.flatMap(() =>
            setGithubSnapshot(projectId, [
              issue(22, {
                title: "newer snapshot",
                updatedAt: "2026-07-23T11:00:00.000Z",
              }),
            ]),
          ),
        ),
      );
      yield* Effect.yieldNow;
      const result = yield* intake.reconcileOnce({ projectId });
      yield* Fiber.join(bump);

      assert.equal(result.githubIntakeSequence, 2);
      assert.equal((yield* validTasks(projectId))[0]?.githubIntakeSequence, 2);
      const watermark = yield* reconciles.get(projectId);
      if (Option.isSome(watermark)) {
        assert.equal(watermark.value.status, "completed");
        assert.equal(watermark.value.lastCompletedSequence, 2);
        assert.equal(watermark.value.revision, 4);
      } else {
        assert.fail("missing reconcile watermark");
      }
    }),
  );

  it.effect("orders parallel reconciles across different source sequences", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const projectId = ProjectId.make("task-parallel-different-sequences");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(220)]);

      const subscribed = yield* engine.subscribeDomainEvents;
      const bump = yield* Effect.forkChild(
        Stream.runHead(subscribed).pipe(
          Effect.flatMap(() =>
            setGithubSnapshot(projectId, [
              issue(220, {
                updatedAt: "2026-07-23T11:00:00.000Z",
                title: "sequence two",
              }),
            ]),
          ),
        ),
      );
      yield* Effect.yieldNow;
      const results = yield* Effect.all(
        [intake.reconcileOnce({ projectId }), intake.reconcileOnce({ projectId })],
        { concurrency: "unbounded" },
      );
      yield* Fiber.join(bump);
      assert.deepStrictEqual(
        results.map((result) => result.githubIntakeSequence),
        [2, 2],
      );
      const task = (yield* validTasks(projectId))[0]!;
      assert.equal(task.githubIntakeSequence, 2);
      assert.equal(task.revision, 2);
    }),
  );

  it.effect("bounds stale retries, leaves recovery-required, and resumes deterministically", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const reconciles = yield* AgentControlTaskReconcileStateRepository;
      const projectId = ProjectId.make("task-retry-exhaustion");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(23)]);

      const bumpCount = yield* Ref.make(0);
      const subscribed = yield* engine.subscribeDomainEvents;
      const bumper = yield* Effect.forkChild(
        Stream.runForEach(Stream.take(subscribed, 3), () =>
          Ref.updateAndGet(bumpCount, (count) => count + 1).pipe(
            Effect.flatMap((count) =>
              setGithubSnapshot(projectId, [
                issue(23, {
                  title: `retry-snapshot-${count + 1}`,
                  updatedAt: `2026-07-23T${10 + count}:00:00.000Z`,
                }),
              ]),
            ),
          ),
        ),
      );
      yield* Effect.yieldNow;
      const failed = yield* Effect.result(intake.reconcileOnce({ projectId }));
      yield* Fiber.join(bumper);
      assert.equal(failed._tag, "Failure");
      if (failed._tag === "Failure") {
        assert.equal(failed.failure.code, "source-snapshot-stale");
      }
      assert.equal(yield* Ref.get(bumpCount), 3);
      let watermark = yield* reconciles.get(projectId);
      if (Option.isSome(watermark)) {
        assert.equal(watermark.value.status, "recovery-required");
        assert.equal(watermark.value.lastCompletedSequence, 0);
      } else {
        assert.fail("missing recovery-required watermark");
      }

      const resumed = yield* intake.reconcileOnce({ projectId });
      assert.equal(resumed.githubIntakeSequence, 4);
      watermark = yield* reconciles.get(projectId);
      if (Option.isSome(watermark)) {
        assert.equal(watermark.value.status, "completed");
        assert.equal(watermark.value.lastCompletedSequence, 4);
      } else {
        assert.fail("missing completed watermark");
      }
    }),
  );

  it.effect("checks stale source and project deletion inside task commit transactions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const github = yield* AgentControlGithubStateRepository;
      const engine = yield* AgentControlTaskEngine;
      const projectId = ProjectId.make("task-stale-command-source");
      yield* addProject(sql, projectId);
      const firstIssue = issue(24);
      yield* setGithubSnapshot(projectId, [firstIssue]);
      const completed = Option.getOrThrow(yield* github.getCompletedSnapshot(projectId));
      yield* setGithubSnapshot(projectId, [issue(24, { updatedAt: "2026-07-23T11:00:00.000Z" })]);

      const source = {
        projectId,
        repositoryNodeId: firstIssue.repositoryNodeId,
        issueNodeId: firstIssue.issueNodeId,
        issueNumber: firstIssue.number,
        issueUrl: firstIssue.url,
      };
      const taskId = yield* deriveAgentControlTaskId(source);
      const stale = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-stale-source-command"),
          taskId,
          projectId,
          expectedRevision: 0,
          sourcePrecondition: completed.sourcePrecondition,
          source,
          sourceGate: "eligible",
          sourceUpdatedAt: firstIssue.updatedAt,
          githubIntakeSequence: completed.sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: taskSourceSnapshot(firstIssue),
        }),
      );
      assert.equal(stale._tag, "Failure");
      if (stale._tag === "Failure") {
        assert.equal(stale.failure.code, "source-snapshot-stale");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'task-stale-source-command' AND status = 'accepted'
        `)[0]?.count,
        0,
      );

      const deleteProjectId = ProjectId.make("task-delete-during-command");
      yield* addProject(sql, deleteProjectId);
      const deleteIssue = issue(25);
      yield* setGithubSnapshot(deleteProjectId, [deleteIssue]);
      const deleteSnapshot = Option.getOrThrow(yield* github.getCompletedSnapshot(deleteProjectId));
      yield* sql`
        UPDATE projection_projects SET deleted_at = ${now}
        WHERE project_id = ${deleteProjectId}
      `;
      const deleteSource = {
        projectId: deleteProjectId,
        repositoryNodeId: deleteIssue.repositoryNodeId,
        issueNodeId: deleteIssue.issueNodeId,
        issueNumber: deleteIssue.number,
        issueUrl: deleteIssue.url,
      };
      const deleted = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-deleted-source-command"),
          taskId: yield* deriveAgentControlTaskId(deleteSource),
          projectId: deleteProjectId,
          expectedRevision: 0,
          sourcePrecondition: deleteSnapshot.sourcePrecondition,
          source: deleteSource,
          sourceGate: "eligible",
          sourceUpdatedAt: deleteIssue.updatedAt,
          githubIntakeSequence: deleteSnapshot.sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: taskSourceSnapshot(deleteIssue),
        }),
      );
      assert.equal(deleted._tag, "Failure");
      if (deleted._tag === "Failure") {
        assert.equal(deleted.failure.code, "project-deleted");
      }
    }),
  );

  it.effect("fails before writes for corrupt projections and source-number conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const engine = yield* AgentControlTaskEngine;
      const github = yield* AgentControlGithubStateRepository;
      const projectId = ProjectId.make("task-prevalidate-corrupt");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(26)]);
      yield* intake.reconcileOnce({ projectId });
      yield* setGithubSnapshot(projectId, [issue(26), issue(27)]);
      const eventCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_events
        WHERE aggregate_kind = 'task'
      `)[0]!.count;
      const receiptCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
        WHERE aggregate_kind = 'task'
      `)[0]!.count;
      yield* sql`
        UPDATE agent_control_task_states
        SET state_json = '{"corrupt":true}'
        WHERE project_id = ${projectId}
      `;
      const corrupt = yield* Effect.result(intake.reconcileOnce({ projectId }));
      assert.equal(corrupt._tag, "Failure");
      if (corrupt._tag === "Failure") {
        assert.equal(corrupt.failure.code, "task-projection-corrupt");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'task'
        `)[0]?.count,
        eventCount,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE aggregate_kind = 'task'
        `)[0]?.count,
        receiptCount,
      );

      const duplicateProjectId = ProjectId.make("task-existing-source-duplicates");
      yield* addProject(sql, duplicateProjectId);
      yield* setGithubSnapshot(duplicateProjectId, [issue(280)]);
      yield* intake.reconcileOnce({ projectId: duplicateProjectId });
      const original = (yield* validTasks(duplicateProjectId))[0]!;
      const duplicateTaskId = AgentControlTaskId.make("task-existing-source-duplicate");
      const duplicateState = yield* encodeTaskState({
        ...original,
        taskId: duplicateTaskId,
      });
      yield* sql`DROP INDEX idx_agent_control_task_source_identity`;
      yield* sql`DROP INDEX idx_agent_control_task_source_number`;
      yield* sql`
        INSERT INTO agent_control_task_states (
          task_id, project_id, repository_node_id, issue_node_id,
          issue_number, issue_url, status, source_gate, stage,
          source_updated_at, github_intake_sequence, state_json,
          created_at, updated_at, revision, last_event_sequence
        ) VALUES (
          ${duplicateTaskId}, ${duplicateProjectId},
          ${original.source.repositoryNodeId}, ${original.source.issueNodeId},
          ${original.source.issueNumber}, ${original.source.issueUrl},
          ${original.status}, ${original.sourceGate}, ${original.stage},
          ${original.sourceUpdatedAt}, ${original.githubIntakeSequence},
          ${duplicateState}, ${original.createdAt}, ${original.updatedAt},
          ${original.revision}, ${original.sequence}
        )
      `;
      const duplicate = yield* Effect.result(
        intake.reconcileOnce({ projectId: duplicateProjectId }),
      );
      assert.equal(duplicate._tag, "Failure");
      if (duplicate._tag === "Failure") {
        assert.equal(duplicate.failure.code, "source-identity-conflict");
      }
      yield* sql`
        DELETE FROM agent_control_task_states WHERE task_id = ${duplicateTaskId}
      `;
      yield* sql`
        CREATE UNIQUE INDEX idx_agent_control_task_source_identity
        ON agent_control_task_states(project_id, repository_node_id, issue_node_id)
      `;
      yield* sql`
        CREATE UNIQUE INDEX idx_agent_control_task_source_number
        ON agent_control_task_states(project_id, repository_node_id, issue_number)
      `;

      const conflictProjectId = ProjectId.make("task-source-number-conflict");
      yield* addProject(sql, conflictProjectId);
      yield* setGithubSnapshot(conflictProjectId, [issue(28)]);
      yield* intake.reconcileOnce({ projectId: conflictProjectId });
      const completed = Option.getOrThrow(yield* github.getCompletedSnapshot(conflictProjectId));
      const conflictingIssue = issue(28, {
        issueNodeId: "different-issue-node-28",
      });
      const conflictingSource = {
        projectId: conflictProjectId,
        repositoryNodeId: conflictingIssue.repositoryNodeId,
        issueNodeId: conflictingIssue.issueNodeId,
        issueNumber: conflictingIssue.number,
        issueUrl: conflictingIssue.url,
      };
      const conflict = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-source-number-conflict-command"),
          taskId: yield* deriveAgentControlTaskId(conflictingSource),
          projectId: conflictProjectId,
          expectedRevision: 0,
          sourcePrecondition: completed.sourcePrecondition,
          source: conflictingSource,
          sourceGate: "eligible",
          sourceUpdatedAt: conflictingIssue.updatedAt,
          githubIntakeSequence: completed.sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: taskSourceSnapshot(conflictingIssue),
        }),
      );
      assert.equal(conflict._tag, "Failure");
      if (conflict._tag === "Failure") {
        assert.equal(conflict.failure.code, "source-identity-conflict");
      }

      const duplicateNodeIssue = issue(29, {
        issueNodeId: "issue-node-28",
      });
      const duplicateNodeSource = {
        projectId: conflictProjectId,
        repositoryNodeId: duplicateNodeIssue.repositoryNodeId,
        issueNodeId: duplicateNodeIssue.issueNodeId,
        issueNumber: duplicateNodeIssue.number,
        issueUrl: duplicateNodeIssue.url,
      };
      const duplicateNode = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-source-node-conflict-command"),
          taskId: yield* deriveAgentControlTaskId(duplicateNodeSource),
          projectId: conflictProjectId,
          expectedRevision: 0,
          sourcePrecondition: completed.sourcePrecondition,
          source: duplicateNodeSource,
          sourceGate: "eligible",
          sourceUpdatedAt: duplicateNodeIssue.updatedAt,
          githubIntakeSequence: completed.sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: taskSourceSnapshot(duplicateNodeIssue),
        }),
      );
      assert.equal(duplicateNode._tag, "Failure");
      if (duplicateNode._tag === "Failure") {
        assert.equal(duplicateNode.failure.code, "source-identity-conflict");
      }
    }),
  );

  it.effect("recovers source-missing explicitly while identity-invalid remains quarantined", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projectId = ProjectId.make("task-source-recovery");
      yield* addProject(sql, projectId);
      yield* setGithubSnapshot(projectId, [issue(29)]);
      yield* intake.reconcileOnce({ projectId });
      yield* setGithubSnapshot(projectId, []);
      yield* intake.reconcileOnce({ projectId });
      assert.equal((yield* validTasks(projectId))[0]?.sourceGate, "source-missing");

      yield* setGithubSnapshot(projectId, [issue(29, { updatedAt: "2026-07-23T11:00:00.000Z" })]);
      yield* intake.reconcileOnce({ projectId });
      let task = (yield* validTasks(projectId))[0]!;
      assert.equal(task.status, "candidate");
      assert.equal(task.sourceGate, "eligible");
      assert.equal(task.githubIntakeSequence, 3);

      yield* setGithubSnapshot(projectId, [
        issue(29, {
          issueNodeId: "replacement-node-29",
          updatedAt: "2026-07-23T12:00:00.000Z",
        }),
      ]);
      yield* intake.reconcileOnce({ projectId });
      task = (yield* validTasks(projectId))[0]!;
      assert.equal(task.status, "needs-attention");
      assert.equal(task.sourceGate, "identity-invalid");

      yield* setGithubSnapshot(projectId, [issue(29, { updatedAt: "2026-07-23T13:00:00.000Z" })]);
      yield* intake.reconcileOnce({ projectId });
      task = (yield* validTasks(projectId))[0]!;
      assert.equal(task.status, "needs-attention");
      assert.equal(task.sourceGate, "identity-invalid");
    }),
  );

  it.effect("rolls back before receipts, publishes once, and replays without republishing", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlTaskEngine;
      const github = yield* AgentControlGithubStateRepository;
      const projectId = ProjectId.make("task-transaction-publication");
      yield* addProject(sql, projectId);
      const firstIssue = issue(30);
      yield* setGithubSnapshot(projectId, [firstIssue]);
      const completed = Option.getOrThrow(yield* github.getCompletedSnapshot(projectId));
      const source = {
        projectId,
        repositoryNodeId: firstIssue.repositoryNodeId,
        issueNodeId: firstIssue.issueNodeId,
        issueNumber: firstIssue.number,
        issueUrl: firstIssue.url,
      };
      const taskId = yield* deriveAgentControlTaskId(source);
      const command = {
        type: "agentControl.task.createFromGithubIssue",
        commandId: CommandId.make("task-publication-command"),
        taskId,
        projectId,
        expectedRevision: 0,
        sourcePrecondition: completed.sourcePrecondition,
        source,
        sourceGate: "eligible",
        sourceUpdatedAt: firstIssue.updatedAt,
        githubIntakeSequence: completed.sourcePrecondition.githubIntakeSequence,
        sourceSnapshot: taskSourceSnapshot(firstIssue),
      } as const;
      const publishedCount = yield* Ref.make(0);
      const subscribed = yield* engine.subscribeDomainEvents;
      const listener = yield* Effect.forkChild(
        Stream.runForEach(subscribed, () => Ref.update(publishedCount, (n) => n + 1)),
      );
      yield* Effect.yieldNow;
      yield* engine.dispatchController(command);
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(publishedCount), 1);
      yield* engine.dispatchController(command);
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(publishedCount), 1);

      const rollbackIssue = issue(31);
      yield* setGithubSnapshot(projectId, [firstIssue, rollbackIssue]);
      const rollbackSnapshot = Option.getOrThrow(yield* github.getCompletedSnapshot(projectId));
      const rollbackSource = {
        projectId,
        repositoryNodeId: rollbackIssue.repositoryNodeId,
        issueNodeId: rollbackIssue.issueNodeId,
        issueNumber: rollbackIssue.number,
        issueUrl: rollbackIssue.url,
      };
      const rollbackTaskId = yield* deriveAgentControlTaskId(rollbackSource);
      yield* sql`
        CREATE TRIGGER fail_task_receipt_before_insert
        BEFORE INSERT ON agent_control_command_receipts
        WHEN NEW.command_id = 'task-rollback-command'
        BEGIN
          SELECT RAISE(ABORT, 'receipt blocked');
        END
      `;
      const rollback = yield* Effect.result(
        engine.dispatchController({
          type: "agentControl.task.createFromGithubIssue",
          commandId: CommandId.make("task-rollback-command"),
          taskId: rollbackTaskId,
          projectId,
          expectedRevision: 0,
          sourcePrecondition: rollbackSnapshot.sourcePrecondition,
          source: rollbackSource,
          sourceGate: "eligible",
          sourceUpdatedAt: rollbackIssue.updatedAt,
          githubIntakeSequence: rollbackSnapshot.sourcePrecondition.githubIntakeSequence,
          sourceSnapshot: taskSourceSnapshot(rollbackIssue),
        }),
      );
      assert.equal(rollback._tag, "Failure");
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(publishedCount), 1);
      yield* Fiber.interrupt(listener);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'task' AND stream_id = ${rollbackTaskId}
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_task_states
          WHERE task_id = ${rollbackTaskId}
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'task-rollback-command'
        `)[0]?.count,
        0,
      );
    }),
  );

  it.effect("rebuilds more than one task-event page without touching mixed aggregates", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlTaskIntake;
      const projection = yield* AgentControlTaskProjection;
      const projectId = ProjectId.make("task-rebuild-multiple-pages");
      yield* addProject(sql, projectId);
      const issues = Array.from({ length: 501 }, (_, index) => issue(index + 100));
      yield* setGithubSnapshot(projectId, issues);
      yield* intake.reconcileOnce({ projectId });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'task-rebuild-mixed-event', 'project-controller', 'task-rebuild-mixed-project',
          1, 'agentControl.project.mode.changed', ${now}, 'task-rebuild-mixed-command',
          NULL, 'task-rebuild-mixed-command', 'human', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`DELETE FROM agent_control_task_states`;
      yield* sql`
        DELETE FROM agent_control_projection_state
        WHERE projector_name = ${AGENT_CONTROL_TASK_PROJECTOR}
      `;
      yield* projection.bootstrap;
      assert.equal((yield* validTasks(projectId)).length, 501);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE event_id = 'task-rebuild-mixed-event'
        `)[0]?.count,
        1,
      );
    }),
  );
});
