import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlTaskId,
  CommandId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlStageRunLeaseState,
  type AgentControlStageRunState,
  type AgentControlTaskState,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../../config.ts";
import * as GitManager from "../../../git/GitManager.ts";
import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  deriveAgentControlWorktreeBranchName,
  deriveAgentControlWorktreeReservationId,
} from "../identity.ts";
import { deriveSafeAgentControlWorktreePath } from "../pathSafety.ts";
import { AgentControlWorktreeController } from "../Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { layer as AgentControlWorktreeControllerLive } from "./AgentControlWorktreeController.ts";

const configLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "agent-control-worktree-controller-",
});
const processLayer = VcsProcess.layer.pipe(Layer.provide(NodeServices.layer));
const gitLayer = GitVcsDriver.layer.pipe(
  Layer.provide(processLayer),
  Layer.provide(configLayer),
  Layer.provide(NodeServices.layer),
);
const registryLayer = VcsDriverRegistry.layer.pipe(
  Layer.provide(gitLayer),
  Layer.provide(processLayer),
  Layer.provide(configLayer),
  Layer.provide(NodeServices.layer),
);
const workflowLayer = GitWorkflowService.layer.pipe(
  Layer.provide(registryLayer),
  Layer.provide(gitLayer),
  Layer.provide(Layer.mock(GitManager.GitManager)({})),
);
const controllerLayer = AgentControlWorktreeControllerLive.pipe(
  Layer.provideMerge(AgentControlRuntimeLayerLive),
  Layer.provideMerge(workflowLayer),
  Layer.provideMerge(gitLayer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(configLayer),
  Layer.provideMerge(NodeServices.layer),
);
const layer = it.layer(controllerLayer);
const at = "2026-07-24T10:00:00.000Z";
const repository = {
  repositoryNodeId: "worktree-controller-repository-node",
  nameWithOwner: "owner/repository",
} as const;

const git = (cwd: string, args: ReadonlyArray<string>, allowNonZeroExit = false) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    return yield* driver.execute({
      operation: "AgentControlWorktreeController.test.git",
      cwd,
      args,
      allowNonZeroExit,
      timeoutMs: 10_000,
    });
  });

const makeRepository = Effect.fn("makeAgentControlWorktreeRepository")(function* (
  withDefaultRemoteRef = true,
) {
  const fs = yield* FileSystem.FileSystem;
  const cwd = yield* fs.makeTempDirectoryScoped({
    prefix: "agent-control-worktree-repository-",
  });
  yield* git(cwd, ["init", "-b", "main"]);
  yield* git(cwd, ["config", "user.email", "test@example.test"]);
  yield* git(cwd, ["config", "user.name", "Test"]);
  yield* fs.writeFileString(`${cwd}/README.md`, "base\n");
  yield* git(cwd, ["add", "README.md"]);
  yield* git(cwd, ["commit", "-m", "base"]);
  const baseCommitSha = (yield* git(cwd, ["rev-parse", "HEAD"])).stdout.trim();
  yield* git(cwd, ["remote", "add", "origin", "https://github.com/owner/repository.git"]);
  if (withDefaultRemoteRef) {
    yield* git(cwd, ["update-ref", "refs/remotes/origin/main", baseCommitSha]);
    yield* git(cwd, ["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main"]);
  }
  return { cwd, baseCommitSha };
});

const issue = (projectId: ProjectId): AgentControlGithubIssueSnapshot => {
  const number =
    projectId === ProjectId.make("worktree-materialize")
      ? 417
      : 1_000 + [...projectId].reduce((total, character) => total + character.codePointAt(0)!, 0);
  return {
    repositoryNodeId: repository.repositoryNodeId,
    issueNodeId: `issue-${projectId}`,
    number,
    url: `https://example.test/${projectId}/issues/${number}`,
    state: "open",
    title: "Über / ../../ reconnect; $(secret)",
    body: "DO NOT LEAK THIS BODY /private/credential",
    contentTrust: "untrusted-external",
    updatedAt: at,
    timelineComplete: true,
    timelineEvents: [],
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  };
};

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

const seedPrepared = Effect.fn("seedAgentControlWorktreePrepared")(function* (
  projectId: ProjectId,
  workspace: string,
  issueNumber?: number,
) {
  const sql = yield* SqlClient.SqlClient;
  const github = yield* AgentControlGithubStateRepository;
  const tasks = yield* AgentControlTaskStateRepository;
  const stageRuns = yield* AgentControlStageRun;
  const canonicalSource = issue(projectId);
  const source =
    issueNumber === undefined
      ? canonicalSource
      : {
          ...canonicalSource,
          number: issueNumber,
          url: `https://example.test/${projectId}/issues/${issueNumber}`,
        };
  const scriptsJson =
    '[{"id":"must-not-run","name":"Must not run","command":"touch agent-control-setup-must-not-run","icon":"configure","runOnWorktreeCreate":true}]';
  const task = taskFrom(projectId, source);
  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Worktree test', ${workspace}, NULL, ${scriptsJson},
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

const reserveLease = Effect.fn("reserveAgentControlWorktreeLease")(function* (
  stageRun: AgentControlStageRunState,
  fenceToken = 1,
  expectedRevision = 0,
) {
  const engine = yield* AgentControlStageRunLeaseEngine;
  const outcome = yield* engine.dispatchController({
    type: "agentControl.stageRunLease.reserve",
    commandId: CommandId.make(`lease-${stageRun.projectId}-${fenceToken}`),
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
    leaseDurationMs: 120_000,
  });
  assert.equal(outcome._tag, "Accepted");
  if (outcome._tag === "Rejected") return yield* outcome.error;
  return outcome.result.state;
});

const reserveWorktreeOnly = Effect.fn("reserveAgentControlWorktreeOnly")(function* (input: {
  readonly commandId: string;
  readonly task: AgentControlTaskState;
  readonly stageRun: AgentControlStageRunState;
  readonly lease: AgentControlStageRunLeaseState;
  readonly repositoryWorkspace: string;
  readonly baseCommitSha: string;
  readonly repositoryCommonDir?: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const commonResult = yield* git(input.repositoryWorkspace, ["rev-parse", "--git-common-dir"]);
  const commonCandidate = commonResult.stdout.trim().startsWith("/")
    ? commonResult.stdout.trim()
    : `${input.repositoryWorkspace}/${commonResult.stdout.trim()}`;
  const repositoryCommonDir = input.repositoryCommonDir ?? (yield* fs.realPath(commonCandidate));
  const repositoryIdentity = {
    ...repository,
    canonicalKey: "github.com/owner/repository",
  };
  const reservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId: input.task.source.projectId,
    taskId: input.task.taskId,
    stageRunId: input.stageRun.stageRunId,
    attemptId: input.stageRun.attemptId,
    leaseId: input.lease.leaseId,
    fenceToken: input.lease.fenceToken,
    repositoryIdentity: {
      repositoryNodeId: repositoryIdentity.repositoryNodeId,
      canonicalKey: repositoryIdentity.canonicalKey,
    },
    baseCommitSha: input.baseCommitSha,
  });
  const safePath = yield* deriveSafeAgentControlWorktreePath({
    projectId: input.task.source.projectId,
    reservationId,
    repositoryWorkspace: input.repositoryWorkspace,
  });
  const outcome = yield* (yield* AgentControlWorktreeEngine).dispatchController({
    type: "agentControl.worktree.reserve",
    commandId: CommandId.make(input.commandId),
    reservationId,
    projectId: input.task.source.projectId,
    taskId: input.task.taskId,
    taskRevision: input.task.revision,
    githubIntakeSequence: input.task.githubIntakeSequence,
    sourceIdentityFingerprint: input.stageRun.sourceIdentityFingerprint,
    stageRunId: input.stageRun.stageRunId,
    attemptId: input.stageRun.attemptId,
    leaseId: input.lease.leaseId,
    fenceToken: input.lease.fenceToken,
    expectedRevision: 0,
    repository: repositoryIdentity,
    repositoryWorkspace: input.repositoryWorkspace,
    repositoryCommonDir,
    baseRef: "origin/main",
    baseCommitSha: input.baseCommitSha,
    branchName: deriveAgentControlWorktreeBranchName({
      issueNumber: input.task.source.issueNumber,
      title: input.task.sourceSnapshot.title,
      taskId: input.task.taskId,
    }),
    internalWorktreePath: safePath.target,
  });
  assert.equal(outcome._tag, "Accepted");
  if (outcome._tag === "Rejected") return yield* outcome.error;
  return outcome.result.state;
});

const startMaterializing = Effect.fn("startAgentControlWorktreeMaterializing")(function* (
  state: AgentControlWorktreeReservationState,
  commandId: string,
) {
  const outcome = yield* (yield* AgentControlWorktreeEngine).dispatchController({
    type: "agentControl.worktree.materialization.start",
    commandId: CommandId.make(commandId),
    reservationId: state.reservationId,
    projectId: state.projectId,
    taskId: state.taskId,
    taskRevision: state.taskRevision,
    githubIntakeSequence: state.githubIntakeSequence,
    sourceIdentityFingerprint: state.sourceIdentityFingerprint,
    stageRunId: state.stageRunId,
    attemptId: state.attemptId,
    leaseId: state.leaseId,
    fenceToken: state.fenceToken,
    expectedRevision: state.revision,
  });
  assert.equal(outcome._tag, "Accepted");
  if (outcome._tag === "Rejected") return yield* outcome.error;
  return outcome.result.state;
});

layer("Agent Control worktree materialization", (it) => {
  it.effect(
    "creates exactly one validated worktree from the pinned base under parallel replay",
    () =>
      Effect.gen(function* () {
        const { cwd, baseCommitSha } = yield* makeRepository();
        const projectId = ProjectId.make("worktree-materialize");
        const { task, stageRun } = yield* seedPrepared(projectId, cwd);
        yield* reserveLease(stageRun);
        const controller = yield* AgentControlWorktreeController;
        const engine = yield* AgentControlWorktreeEngine;
        const published = yield* Stream.runCollect(
          engine.streamDomainEvents.pipe(Stream.take(3)),
        ).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        const input = {
          commandId: CommandId.make("worktree-materialize-command"),
          projectId,
          taskId: task.taskId,
        };
        const [first, replay] = yield* Effect.all(
          [controller.reserveAndMaterialize(input), controller.reserveAndMaterialize(input)],
          { concurrency: "unbounded" },
        );
        assert.equal(first.reservationId, replay.reservationId);
        assert.equal((yield* Fiber.join(published)).length, 3);
        assert.equal(
          yield* (yield* FileSystem.FileSystem).exists(first.internalWorktreePath),
          true,
        );
        assert.include(
          (yield* git(cwd, ["worktree", "list", "--porcelain"])).stdout,
          first.internalWorktreePath,
        );
        assert.equal(first.attentionCode, null);
        assert.equal(first.status, "ready");
        assert.equal(first.baseCommitSha, baseCommitSha);
        assert.equal(first.headCommitSha, baseCommitSha);
        assert.equal(first.branchName, "t3auto/issue-417-uber-reconnect-secret");
        assert.equal(
          (yield* git(first.internalWorktreePath, ["branch", "--show-current"])).stdout.trim(),
          first.branchName,
        );
        assert.equal(
          (yield* git(first.internalWorktreePath, ["rev-parse", "HEAD"])).stdout.trim(),
          baseCommitSha,
        );
        assert.equal(
          yield* (yield* FileSystem.FileSystem).exists(
            `${first.internalWorktreePath}/agent-control-setup-must-not-run`,
          ),
          false,
        );
        assert.equal(
          (yield* git(cwd, ["check-ref-format", "--branch", first.branchName])).exitCode,
          0,
        );
        const worktrees = (yield* git(cwd, ["worktree", "list", "--porcelain"])).stdout;
        assert.equal(worktrees.split(`branch refs/heads/${first.branchName}`).length - 1, 1);
        const sql = yield* SqlClient.SqlClient;
        assert.equal(
          (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
            AND stream_id = ${first.reservationId}
        `)[0]!.count,
          3,
        );
        const replayPublication = yield* Stream.runHead(engine.streamDomainEvents).pipe(
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const postReadyReplay = yield* controller.reserveAndMaterialize(input);
        assert.equal(postReadyReplay.status, "ready");
        yield* Effect.yieldNow;
        assert.equal(replayPublication.pollUnsafe(), undefined);
        yield* Fiber.interrupt(replayPublication);
        assert.equal(
          (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE aggregate_kind = 'worktree-reservation'
            AND aggregate_id = ${first.reservationId}
            AND status = 'accepted'
        `)[0]!.count,
          3,
        );
        assert.notInclude(first.branchName, "credential");
        assert.notInclude(first.branchName, "private");
      }),
  );

  it.effect("fails closed before reservation when no local default remote ref exists", () =>
    Effect.gen(function* () {
      const { cwd } = yield* makeRepository(false);
      const projectId = ProjectId.make("worktree-default-ref-missing");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      yield* reserveLease(stageRun);
      const result = yield* Effect.result(
        (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("worktree-default-ref-missing-command"),
          projectId,
          taskId: task.taskId,
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "default-remote-ref-unavailable");
      }
    }),
  );

  it.effect("resumes after materializing and adopts an exact pre-existing Git worktree", () =>
    Effect.gen(function* () {
      const { cwd, baseCommitSha } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-crash-adopt");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      const lease = yield* reserveLease(stageRun);
      const reserved = yield* reserveWorktreeOnly({
        commandId: "worktree-crash-reserve",
        task,
        stageRun,
        lease,
        repositoryWorkspace: cwd,
        baseCommitSha,
      });
      const materializing = yield* startMaterializing(reserved, "worktree-crash-materializing");
      yield* (yield* GitWorkflowService.GitWorkflowService).createWorktree({
        cwd,
        refName: materializing.baseCommitSha,
        newRefName: materializing.branchName,
        baseRefName: materializing.baseRef,
        path: materializing.internalWorktreePath,
      });

      const ready = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-crash-reconcile"),
        projectId,
        reservationId: materializing.reservationId,
      });
      assert.equal(ready.status, "ready");
      assert.equal(ready.headCommitSha, baseCommitSha);
    }),
  );

  it.effect("marks a foreign path and a mismatched branch needs-attention without cleanup", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const firstRepo = yield* makeRepository();
      const firstProject = ProjectId.make("worktree-foreign-path");
      const firstPrepared = yield* seedPrepared(firstProject, firstRepo.cwd);
      const firstLease = yield* reserveLease(firstPrepared.stageRun);
      const occupied = yield* reserveWorktreeOnly({
        commandId: "worktree-foreign-path-reserve",
        task: firstPrepared.task,
        stageRun: firstPrepared.stageRun,
        lease: firstLease,
        repositoryWorkspace: firstRepo.cwd,
        baseCommitSha: firstRepo.baseCommitSha,
      });
      yield* fs.makeDirectory(occupied.internalWorktreePath);
      yield* fs.writeFileString(`${occupied.internalWorktreePath}/foreign.txt`, "preserve\n");
      const attentionPath = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-foreign-path-reconcile"),
        projectId: firstProject,
        reservationId: occupied.reservationId,
      });
      assert.equal(attentionPath.status, "needs-attention");
      assert.equal(attentionPath.attentionCode, "path-occupied");
      assert.equal(yield* fs.exists(`${occupied.internalWorktreePath}/foreign.txt`), true);

      const secondRepo = yield* makeRepository();
      const secondProject = ProjectId.make("worktree-branch-mismatch");
      const secondPrepared = yield* seedPrepared(secondProject, secondRepo.cwd);
      const secondLease = yield* reserveLease(secondPrepared.stageRun);
      const mismatch = yield* reserveWorktreeOnly({
        commandId: "worktree-branch-mismatch-reserve",
        task: secondPrepared.task,
        stageRun: secondPrepared.stageRun,
        lease: secondLease,
        repositoryWorkspace: secondRepo.cwd,
        baseCommitSha: secondRepo.baseCommitSha,
      });
      yield* fs.writeFileString(`${secondRepo.cwd}/later.txt`, "later\n");
      yield* git(secondRepo.cwd, ["add", "later.txt"]);
      yield* git(secondRepo.cwd, ["commit", "-m", "later"]);
      yield* git(secondRepo.cwd, ["branch", mismatch.branchName, "HEAD"]);
      const attentionBranch = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-branch-mismatch-reconcile"),
        projectId: secondProject,
        reservationId: mismatch.reservationId,
      });
      assert.equal(attentionBranch.status, "needs-attention");
      assert.equal(attentionBranch.attentionCode, "branch-commit-mismatch");
      assert.equal(yield* fs.exists(mismatch.internalWorktreePath), false);
      assert.equal(
        (yield* git(secondRepo.cwd, ["rev-parse", mismatch.branchName])).stdout.trim(),
        (yield* git(secondRepo.cwd, ["rev-parse", "HEAD"])).stdout.trim(),
      );
    }),
  );

  it.effect("never finalizes an exact worktree after the fence advances", () =>
    Effect.gen(function* () {
      const { cwd, baseCommitSha } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-stale-fence");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      const lease = yield* reserveLease(stageRun);
      const reserved = yield* reserveWorktreeOnly({
        commandId: "worktree-stale-fence-reserve",
        task,
        stageRun,
        lease,
        repositoryWorkspace: cwd,
        baseCommitSha,
      });
      const materializing = yield* startMaterializing(
        reserved,
        "worktree-stale-fence-materializing",
      );
      yield* (yield* GitWorkflowService.GitWorkflowService).createWorktree({
        cwd,
        refName: materializing.baseCommitSha,
        newRefName: materializing.branchName,
        baseRefName: materializing.baseRef,
        path: materializing.internalWorktreePath,
      });
      const leaseEngine = yield* AgentControlStageRunLeaseEngine;
      const released = yield* leaseEngine.dispatchController({
        type: "agentControl.stageRunLease.releaseBeforeExecution",
        commandId: CommandId.make("worktree-stale-fence-release"),
        leaseId: lease.leaseId,
        projectId,
        taskId: task.taskId,
        stageRunId: stageRun.stageRunId,
        attemptId: stageRun.attemptId,
        taskRevision: stageRun.taskRevision,
        githubIntakeSequence: stageRun.githubIntakeSequence,
        sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
        fenceToken: 1,
        expectedRevision: 1,
      });
      assert.equal(released._tag, "Accepted");
      yield* reserveLease(stageRun, 2, 2);

      const result = yield* Effect.result(
        (yield* AgentControlWorktreeController).reconcile({
          commandId: CommandId.make("worktree-stale-fence-reconcile"),
          projectId,
          reservationId: materializing.reservationId,
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "fence-token-mismatch");
      }
      const current = yield* (yield* AgentControlWorktreeEngine).loadAuthoritative(
        materializing.reservationId,
      );
      assert.equal(current?.status, "materializing");
      assert.isNotNull(current);
      const replayCommand = {
        type: "agentControl.worktree.ready" as const,
        commandId: CommandId.make("worktree-stale-fence-receipt-replay"),
        reservationId: materializing.reservationId,
        projectId,
        taskId: materializing.taskId,
        taskRevision: materializing.taskRevision,
        githubIntakeSequence: materializing.githubIntakeSequence,
        sourceIdentityFingerprint: materializing.sourceIdentityFingerprint,
        stageRunId: materializing.stageRunId,
        attemptId: materializing.attemptId,
        leaseId: materializing.leaseId,
        fenceToken: materializing.fenceToken,
        expectedRevision: materializing.revision,
        headCommitSha: materializing.baseCommitSha,
      };
      const engine = yield* AgentControlWorktreeEngine;
      const rejected = yield* engine.dispatchController(replayCommand);
      assert.equal(rejected._tag, "Rejected");
      if (rejected._tag === "Rejected") {
        assert.equal(rejected.error.code, "fence-token-mismatch");
      }
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE agent_control_project_states SET mode = 'paused'
        WHERE project_id = ${projectId}
      `;
      const replayed = yield* engine.dispatchController(replayCommand);
      assert.equal(replayed._tag, "Rejected");
      if (replayed._tag === "Rejected") {
        assert.equal(replayed.error.code, "fence-token-mismatch");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${replayCommand.commandId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* git(materializing.internalWorktreePath, ["rev-parse", "HEAD"])).stdout.trim(),
        baseCommitSha,
      );
    }),
  );

  it.effect("does not adopt a branch in another worktree or a different common directory", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const firstRepo = yield* makeRepository();
      const firstProject = ProjectId.make("worktree-branch-elsewhere");
      const firstPrepared = yield* seedPrepared(firstProject, firstRepo.cwd);
      const firstLease = yield* reserveLease(firstPrepared.stageRun);
      const elsewhere = yield* reserveWorktreeOnly({
        commandId: "worktree-branch-elsewhere-reserve",
        task: firstPrepared.task,
        stageRun: firstPrepared.stageRun,
        lease: firstLease,
        repositoryWorkspace: firstRepo.cwd,
        baseCommitSha: firstRepo.baseCommitSha,
      });
      const otherParent = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-other-worktree-",
      });
      const otherPath = path.join(otherParent, "checked-out");
      yield* (yield* GitWorkflowService.GitWorkflowService).createWorktree({
        cwd: firstRepo.cwd,
        refName: elsewhere.baseCommitSha,
        newRefName: elsewhere.branchName,
        baseRefName: elsewhere.baseRef,
        path: otherPath,
      });
      const elsewhereResult = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-branch-elsewhere-reconcile"),
        projectId: firstProject,
        reservationId: elsewhere.reservationId,
      });
      assert.equal(elsewhereResult.status, "needs-attention");
      assert.equal(elsewhereResult.attentionCode, "branch-in-other-worktree");
      assert.equal(yield* fs.exists(otherPath), true);

      const secondRepo = yield* makeRepository();
      const foreignRepo = yield* makeRepository();
      const secondProject = ProjectId.make("worktree-common-dir-mismatch");
      const secondPrepared = yield* seedPrepared(secondProject, secondRepo.cwd);
      const secondLease = yield* reserveLease(secondPrepared.stageRun);
      const foreignCommon = yield* fs.realPath(`${foreignRepo.cwd}/.git`);
      const commonMismatch = yield* reserveWorktreeOnly({
        commandId: "worktree-common-dir-mismatch-reserve",
        task: secondPrepared.task,
        stageRun: secondPrepared.stageRun,
        lease: secondLease,
        repositoryWorkspace: secondRepo.cwd,
        baseCommitSha: secondRepo.baseCommitSha,
        repositoryCommonDir: foreignCommon,
      });
      const commonResult = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-common-dir-mismatch-reconcile"),
        projectId: secondProject,
        reservationId: commonMismatch.reservationId,
      });
      assert.equal(commonResult.status, "needs-attention");
      assert.equal(commonResult.attentionCode, "repository-identity-mismatch");
    }),
  );

  it.effect("rolls back a branch collision without a terminal infrastructure receipt", () =>
    Effect.gen(function* () {
      const { cwd } = yield* makeRepository();
      const firstProject = ProjectId.make("worktree-collision-first");
      const secondProject = ProjectId.make("worktree-collision-second");
      const first = yield* seedPrepared(firstProject, cwd, 999);
      const second = yield* seedPrepared(secondProject, cwd, 999);
      yield* reserveLease(first.stageRun);
      yield* reserveLease(second.stageRun);
      const controller = yield* AgentControlWorktreeController;
      const firstResult = yield* controller.reserveAndMaterialize({
        commandId: CommandId.make("worktree-collision-first-command"),
        projectId: firstProject,
        taskId: first.task.taskId,
      });
      assert.equal(firstResult.status, "ready");
      const secondCommandId = CommandId.make("worktree-collision-second-command");
      const publication = yield* Stream.runHead(
        (yield* AgentControlWorktreeEngine).streamDomainEvents,
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const collision = yield* Effect.result(
        controller.reserveAndMaterialize({
          commandId: secondCommandId,
          projectId: secondProject,
          taskId: second.task.taskId,
        }),
      );
      assert.equal(collision._tag, "Failure");
      if (collision._tag === "Failure") {
        assert.equal(collision.failure.code, "internal-persistence-error");
      }
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${secondCommandId}
        `)[0]!.count,
        0,
      );
      yield* Effect.yieldNow;
      assert.equal(publication.pollUnsafe(), undefined);
      yield* Fiber.interrupt(publication);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
            AND command_id = ${secondCommandId}
        `)[0]!.count,
        0,
      );
    }),
  );
});
