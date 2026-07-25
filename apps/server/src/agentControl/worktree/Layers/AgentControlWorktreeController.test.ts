import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import {
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlStageRunLeaseState,
  type AgentControlStageRunState,
  type AgentControlTaskState,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../../config.ts";
import * as GitManager from "../../../git/GitManager.ts";
import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
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
  sha256FramedHex,
} from "../identity.ts";
import { deriveSafeAgentControlWorktreePath } from "../pathSafety.ts";
import {
  expectedAgentControlWorktreeOwnershipMarker,
  ownershipMarkerPath,
  writeAgentControlWorktreeOwnershipMarker,
} from "../ownership.ts";
import { AgentControlWorktreeController } from "../Services/AgentControlWorktreeController.ts";
import {
  AgentControlWorktreeControllerHooks,
  type AgentControlWorktreeControllerHooksShape,
} from "../Services/AgentControlWorktreeControllerHooks.ts";
import { AgentControlWorktreeEngine } from "../Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktree } from "../Services/AgentControlWorktree.ts";
import { layer as AgentControlWorktreeControllerLive } from "./AgentControlWorktreeController.ts";
import { layer as AgentControlWorktreeEngineLive } from "./AgentControlWorktreeEngine.ts";

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

const buildControllerContext = (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  hooks?: AgentControlWorktreeControllerHooksShape,
) => {
  const build = Layer.buildWithScope(
    Layer.fresh(AgentControlWorktreeControllerLive).pipe(
      Layer.provideMerge(Layer.fresh(AgentControlRuntimeLayerLive)),
      Layer.provideMerge(workflowLayer),
      Layer.provideMerge(gitLayer),
      Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, sql)),
      Layer.provideMerge(configLayer),
      Layer.provideMerge(NodeServices.layer),
    ),
    scope,
  );
  return hooks === undefined
    ? build
    : build.pipe(Effect.provideService(AgentControlWorktreeControllerHooks, hooks));
};

const makeIndependentControllerContexts = Effect.fn("makeIndependentWorktreeControllerContexts")(
  function* (
    hooksA?: AgentControlWorktreeControllerHooksShape,
    hooksB?: AgentControlWorktreeControllerHooksShape,
    shareRuntimeHolderWithA = true,
  ) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "agent-control-worktree-composite-race-",
    });
    const dbPath = path.join(directory, "state.sqlite");
    const scopeA = yield* Scope.make("sequential");
    const scopeB = yield* Scope.make("sequential");
    yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
    yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
    const sqlContextA = yield* Layer.buildWithScope(
      NodeSqliteClient.layer({ filename: dbPath }),
      scopeA,
    );
    const sqlContextB = yield* Layer.buildWithScope(
      NodeSqliteClient.layer({ filename: dbPath }),
      scopeB,
    );
    const sqlA = Context.get(sqlContextA, SqlClient.SqlClient);
    const sqlB = Context.get(sqlContextB, SqlClient.SqlClient);
    for (const sql of [sqlA, sqlB]) {
      yield* sql`PRAGMA journal_mode = WAL`;
      yield* sql`PRAGMA foreign_keys = ON`;
    }
    yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
    const contextA = yield* buildControllerContext(sqlA, scopeA, hooksA);
    const contextB = yield* buildControllerContext(sqlB, scopeB, hooksB);
    const contextBWithHooks =
      hooksB === undefined
        ? contextB
        : Context.add(contextB, AgentControlWorktreeControllerHooks, hooksB);
    const controllerDependenciesB = yield* Effect.gen(function* () {
      if (!shareRuntimeHolderWithA) return contextBWithHooks;
      const leaseEngineA = Context.get(contextA, AgentControlStageRunLeaseEngine);
      const leaseEngineB = Context.get(contextBWithHooks, AgentControlStageRunLeaseEngine);
      const sharedHolderContextB = Context.add(
        contextBWithHooks,
        AgentControlStageRunLeaseEngine,
        AgentControlStageRunLeaseEngine.of({
          ...leaseEngineB,
          runtimeHolderId: leaseEngineA.runtimeHolderId,
        }),
      );
      const rebuiltEngineContextB = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlWorktreeEngineLive).pipe(
          Layer.provide(Layer.succeedContext(sharedHolderContextB)),
        ),
        scopeB,
      );
      return Context.merge(sharedHolderContextB, rebuiltEngineContextB);
    });
    const controllerContextB = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlWorktreeControllerLive).pipe(
        Layer.provide(Layer.succeedContext(controllerDependenciesB)),
      ),
      scopeB,
    );
    const retryControllerContextA = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlWorktreeControllerLive).pipe(
        Layer.provide(Layer.succeedContext(contextA)),
      ),
      scopeA,
    );
    const retryControllerContextB = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlWorktreeControllerLive).pipe(
        Layer.provide(Layer.succeedContext(controllerDependenciesB)),
      ),
      scopeB,
    );
    return {
      sqlA,
      sqlB,
      contextA,
      contextB,
      controllerA: Context.get(contextA, AgentControlWorktreeController),
      retryControllerA: Context.get(retryControllerContextA, AgentControlWorktreeController),
      controllerB: Context.get(controllerContextB, AgentControlWorktreeController),
      retryControllerB: Context.get(retryControllerContextB, AgentControlWorktreeController),
      engineB: Context.get(controllerDependenciesB, AgentControlWorktreeEngine),
    };
  },
);

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
  const commonInfo = yield* fs.stat(repositoryCommonDir);
  const commonDirInode = commonInfo.ino.pipe(
    // The Node test filesystem always exposes an inode.
    (value) => (value._tag === "Some" ? value.value : 0),
  );
  const repositoryIdentity = {
    ...repository,
    canonicalKey: "github.com/owner/repository",
    remoteName: "origin",
    remoteUrl: "github.com/owner/repository",
    defaultRemoteRef: "refs/remotes/origin/main",
    commonDirDevice: commonInfo.dev,
    commonDirInode,
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
  const targetGenerationId = "d".repeat(64);
  const safePath = yield* deriveSafeAgentControlWorktreePath({
    projectId: input.task.source.projectId,
    reservationId,
    targetGenerationId,
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
    targetGenerationId,
    worktreeRootDevice: safePath.rootIdentity.device,
    worktreeRootInode: safePath.rootIdentity.inode,
    worktreeParentDevice: safePath.parentIdentity.device,
    worktreeParentInode: safePath.parentIdentity.inode,
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

const worktreePersistenceCounts = Effect.fn("worktreePersistenceCounts")(function* (
  reservationId: AgentControlWorktreeReservationId,
) {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    readonly events: number;
    readonly receipts: number;
    readonly projections: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM agent_control_events
        WHERE aggregate_kind = 'worktree-reservation'
          AND stream_id = ${reservationId}) AS events,
      (SELECT COUNT(*) FROM agent_control_command_receipts
        WHERE aggregate_kind = 'worktree-reservation'
          AND aggregate_id = ${reservationId}) AS receipts,
      (SELECT COUNT(*) FROM agent_control_worktree_reservation_states
        WHERE reservation_id = ${reservationId}) AS projections
  `)[0]!;
});

const releaseLease = Effect.fn("releaseAgentControlWorktreeLease")(function* (
  lease: AgentControlStageRunLeaseState,
  commandId: string,
) {
  const outcome = yield* (yield* AgentControlStageRunLeaseEngine).dispatchController({
    type: "agentControl.stageRunLease.releaseBeforeExecution",
    commandId: CommandId.make(commandId),
    leaseId: lease.leaseId,
    projectId: lease.projectId,
    taskId: lease.taskId,
    stageRunId: lease.stageRunId,
    attemptId: lease.attemptId,
    taskRevision: lease.taskRevision,
    githubIntakeSequence: lease.githubIntakeSequence,
    sourceIdentityFingerprint: lease.sourceIdentityFingerprint,
    fenceToken: lease.fenceToken,
    expectedRevision: lease.revision,
  });
  assert.equal(outcome._tag, "Accepted");
  if (outcome._tag === "Rejected") return yield* outcome.error;
  return outcome.result.state;
});

layer("Agent Control worktree materialization", (it) => {
  it.effect(
    "releases its exact composite claim at every interrupt checkpoint and resumes through an independent controller",
    () =>
      Effect.gen(function* () {
        const checkpoints = [
          "after-claim",
          "after-preflight",
          "after-reserved",
          "after-target-acquired",
          "after-materializing",
          "after-git-call",
          "after-git-created",
          "after-marker-publish",
          "after-ownership-marked",
          "before-ready",
          "after-ready-before-accepted",
        ] as const;
        const expectedEvents = new Map<(typeof checkpoints)[number], number>([
          ["after-claim", 0],
          ["after-preflight", 0],
          ["after-reserved", 1],
          ["after-target-acquired", 2],
          ["after-materializing", 2],
          ["after-git-call", 2],
          ["after-git-created", 2],
          ["after-marker-publish", 2],
          ["after-ownership-marked", 2],
          ["before-ready", 2],
          ["after-ready-before-accepted", 3],
        ]);
        const expectedPhases = new Map<(typeof checkpoints)[number], string>([
          ["after-claim", "unbound"],
          ["after-preflight", "unbound"],
          ["after-reserved", "reserved"],
          ["after-target-acquired", "reserved"],
          ["after-materializing", "materializing"],
          ["after-git-call", "materializing"],
          ["after-git-created", "git-created"],
          ["after-marker-publish", "git-created"],
          ["after-ownership-marked", "ownership-marked"],
          ["before-ready", "ownership-marked"],
          ["after-ready-before-accepted", "ownership-marked"],
        ]);

        for (const checkpoint of checkpoints) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const reached = yield* Deferred.make<void>();
              const hold = yield* Deferred.make<void>();
              const hooks: AgentControlWorktreeControllerHooksShape = {
                afterCompositeClaim: () =>
                  checkpoint === "after-claim"
                    ? Deferred.succeed(reached, undefined).pipe(
                        Effect.andThen(Deferred.await(hold)),
                      )
                    : Effect.void,
                afterLifecycleCheckpoint: (current) =>
                  current === checkpoint
                    ? Deferred.succeed(reached, undefined).pipe(
                        Effect.andThen(Deferred.await(hold)),
                      )
                    : Effect.void,
                afterReadyInspection: () => Effect.void,
                beforeCompositeAccept: () =>
                  checkpoint === "after-ready-before-accepted"
                    ? Deferred.succeed(reached, undefined).pipe(
                        Effect.andThen(Deferred.await(hold)),
                      )
                    : Effect.void,
              };
              const harness = yield* makeIndependentControllerContexts(hooks);
              const fs = yield* FileSystem.FileSystem;
              const repo = yield* makeRepository();
              const projectId = ProjectId.make(`worktree-interrupt-${checkpoint}`);
              const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
                Effect.provide(harness.contextA),
              );
              yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
              const input = {
                commandId: CommandId.make(`worktree-interrupt-command-${checkpoint}`),
                projectId,
                taskId: seeded.task.taskId,
              };
              const published = yield* Ref.make(0);
              const publicationFiber = yield* Context.get(
                harness.contextA,
                AgentControlWorktreeEngine,
              ).streamDomainEvents.pipe(
                Stream.runForEach(() => Ref.update(published, (count) => count + 1)),
                Effect.forkChild,
              );
              yield* Effect.yieldNow;
              const operation = yield* harness.controllerA
                .reserveAndMaterialize(input)
                .pipe(Effect.forkChild);
              yield* Deferred.await(reached);
              const claimed = (yield* harness.sqlB<{
                readonly pendingToken: string | null;
                readonly revision: number;
              }>`
                SELECT pending_token AS "pendingToken", revision
                FROM agent_control_worktree_controller_operations
                WHERE command_id = ${input.commandId}
              `)[0]!;
              assert.isNotNull(claimed.pendingToken);
              yield* Fiber.interrupt(operation);
              const interrupted = yield* Fiber.await(operation);
              assert.equal(
                Exit.hasInterrupts(interrupted),
                true,
                `checkpoint ${checkpoint} must remain interruptible: ${
                  Exit.isFailure(interrupted) ? Cause.pretty(interrupted.cause) : "success"
                }`,
              );
              yield* Fiber.interrupt(publicationFiber);

              const composite = (yield* harness.sqlB<{
                readonly status: string;
                readonly pendingToken: string | null;
                readonly claimRuntimeId: string | null;
                readonly claimAttemptId: string | null;
                readonly phase: string;
                readonly revision: number;
              }>`
                SELECT status, pending_token AS "pendingToken",
                  claim_runtime_id AS "claimRuntimeId",
                  claim_attempt_id AS "claimAttemptId",
                  materialization_phase AS phase, revision
                FROM agent_control_worktree_controller_operations
                WHERE command_id = ${input.commandId}
              `)[0]!;
              assert.equal(composite.status, "pending");
              assert.equal(composite.pendingToken, null);
              assert.equal(composite.claimRuntimeId, null);
              assert.equal(composite.claimAttemptId, null);
              assert.equal(composite.phase, expectedPhases.get(checkpoint));
              assert.isAbove(composite.revision, claimed.revision);

              const eventCount = (yield* harness.sqlB<{ readonly count: number }>`
                SELECT COUNT(*) AS count
                FROM agent_control_events
                WHERE aggregate_kind = 'worktree-reservation'
              `)[0]!.count;
              assert.equal(eventCount, expectedEvents.get(checkpoint));
              assert.equal(
                (yield* harness.sqlB<{ readonly count: number }>`
                  SELECT COUNT(*) AS count
                  FROM agent_control_command_receipts
                  WHERE aggregate_kind = 'worktree-reservation'
                `)[0]!.count,
                eventCount,
              );
              assert.equal(
                (yield* harness.sqlB<{ readonly count: number }>`
                  SELECT COUNT(*) AS count
                  FROM agent_control_worktree_reservation_states
                `)[0]!.count,
                eventCount === 0 ? 0 : 1,
              );
              assert.equal(
                (yield* harness.sqlB<{ readonly count: number }>`
                  SELECT COUNT(*) AS count
                  FROM agent_control_worktree_stream_catalog
                `)[0]!.count,
                eventCount === 0 ? 0 : 1,
              );
              assert.equal(yield* Ref.get(published), eventCount);
              assert.equal(
                (yield* harness.sqlB<{ readonly count: number }>`
                  SELECT COUNT(*) AS count
                  FROM agent_control_worktree_target_claims
                  WHERE command_id = ${input.commandId}
                `)[0]!.count,
                checkpoint === "after-target-acquired" ||
                  checkpoint === "after-materializing" ||
                  checkpoint === "after-git-call" ||
                  checkpoint === "after-git-created" ||
                  checkpoint === "after-marker-publish" ||
                  checkpoint === "after-ownership-marked" ||
                  checkpoint === "before-ready" ||
                  checkpoint === "after-ready-before-accepted"
                  ? 1
                  : 0,
              );

              const projected = (yield* harness.sqlB<{
                readonly reservationId: string;
                readonly worktreePath: string;
              }>`
                SELECT reservation_id AS "reservationId",
                  internal_worktree_path AS "worktreePath"
                FROM agent_control_worktree_reservation_states
              `)[0];
              const afterGit =
                checkpoint === "after-git-call" ||
                checkpoint === "after-git-created" ||
                checkpoint === "after-marker-publish" ||
                checkpoint === "after-ownership-marked" ||
                checkpoint === "before-ready" ||
                checkpoint === "after-ready-before-accepted";
              if (projected !== undefined) {
                const registered = (yield* git(repo.cwd, [
                  "worktree",
                  "list",
                  "--porcelain",
                ])).stdout.includes(projected.worktreePath);
                assert.equal(registered, afterGit);
                assert.equal(yield* fs.exists(projected.worktreePath), afterGit);
                if (afterGit) {
                  const gitDir = (yield* git(projected.worktreePath, ["rev-parse", "--git-dir"]))
                    .stdout;
                  const markerPath = yield* ownershipMarkerPath(projected.worktreePath, gitDir);
                  assert.equal(
                    yield* fs.exists(markerPath),
                    checkpoint === "after-marker-publish" ||
                      checkpoint === "after-ownership-marked" ||
                      checkpoint === "before-ready" ||
                      checkpoint === "after-ready-before-accepted",
                  );
                }
              }

              const mismatch = yield* Effect.result(
                harness.controllerB.reserveAndMaterialize({
                  ...input,
                  projectId: ProjectId.make(`worktree-interrupt-foreign-${checkpoint}`),
                }),
              );
              assert.equal(mismatch._tag, "Failure");
              if (mismatch._tag === "Failure") {
                assert.equal(mismatch.failure.code, "command-identity-mismatch");
              }
              const resumed = yield* harness.retryControllerA.reserveAndMaterialize(input);
              assert.equal(resumed.status, "ready", `retry after ${checkpoint}`);
              assert.deepEqual(
                yield* harness.sqlB`
                  SELECT status, pending_token AS "pendingToken",
                    materialization_phase AS phase
                  FROM agent_control_worktree_controller_operations
                  WHERE command_id = ${input.commandId}
                `,
                [{ status: "accepted", pendingToken: null, phase: "terminal" }],
              );
            }),
          );
        }
      }),
  );

  it.effect(
    "replays a committed Ready transition before mutable authority, Git, marker, or lock checks",
    () =>
      Effect.gen(function* () {
        const mutations = [
          "project-deleted",
          "mode-left",
          "source-sequence-changed",
          "lease-released",
          "fence-advanced",
          "marker-removed",
          "head-changed",
          "registration-removed",
          "repository-unavailable",
        ] as const;
        for (const mutation of mutations) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const transitionCommitted = yield* Deferred.make<void>();
              const holdCompositeAccept = yield* Deferred.make<void>();
              const replayUseCalls = yield* Ref.make(0);
              const hooks: AgentControlWorktreeControllerHooksShape = {
                afterCompositeClaim: () => Effect.void,
                afterLifecycleCheckpoint: () => Effect.void,
                afterReadyInspection: () => Effect.void,
                beforeCompositeAccept: () =>
                  Deferred.succeed(transitionCommitted, undefined).pipe(
                    Effect.andThen(Deferred.await(holdCompositeAccept)),
                  ),
              };
              const harness = yield* makeIndependentControllerContexts(
                hooks,
                {
                  afterReadyInspection: () => Effect.void,
                  beforeCompositeUse: () => Ref.update(replayUseCalls, (count) => count + 1),
                },
                false,
              );
              const fs = yield* FileSystem.FileSystem;
              const repo = yield* makeRepository();
              const projectId = ProjectId.make(`worktree-committed-ready-${mutation}`);
              const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
                Effect.provide(harness.contextA),
              );
              const lease = yield* reserveLease(seeded.stageRun).pipe(
                Effect.provide(harness.contextA),
              );
              const input = {
                commandId: CommandId.make(`worktree-committed-ready-command-${mutation}`),
                projectId,
                taskId: seeded.task.taskId,
              };
              const publishedReadyA = yield* Ref.make(0);
              const publishedReadyB = yield* Ref.make(0);
              const publicationA = yield* Context.get(
                harness.contextA,
                AgentControlWorktreeEngine,
              ).streamDomainEvents.pipe(
                Stream.filter((event) => event.type === "agentControl.worktree.ready"),
                Stream.runForEach(() => Ref.update(publishedReadyA, (count) => count + 1)),
                Effect.forkChild,
              );
              const publicationB = yield* harness.engineB.streamDomainEvents.pipe(
                Stream.filter((event) => event.type === "agentControl.worktree.ready"),
                Stream.runForEach(() => Ref.update(publishedReadyB, (count) => count + 1)),
                Effect.forkChild,
              );
              yield* Effect.yieldNow;
              const operation = yield* harness.controllerA
                .reserveAndMaterialize(input)
                .pipe(Effect.forkChild);
              yield* Deferred.await(transitionCommitted);
              yield* Effect.yieldNow;
              assert.equal(yield* Ref.get(publishedReadyA), 1, mutation);
              yield* Fiber.interrupt(operation);
              const interrupted = yield* Fiber.await(operation);
              assert.equal(Exit.hasInterrupts(interrupted), true, mutation);

              const committed = (yield* harness.sqlB<{
                readonly reservationId: AgentControlWorktreeReservationId;
                readonly status: string;
                readonly claimPhase: string;
                readonly compositeStatus: string;
                readonly pendingToken: string | null;
                readonly transitionReceipts: number;
              }>`
                SELECT state.reservation_id AS "reservationId", state.status,
                  claim.phase AS "claimPhase", operation.status AS "compositeStatus",
                  operation.pending_token AS "pendingToken",
                  (SELECT COUNT(*) FROM agent_control_command_receipts AS receipt
                    JOIN agent_control_events AS event
                      ON event.command_id = receipt.command_id
                    WHERE receipt.aggregate_kind = 'worktree-reservation'
                      AND receipt.aggregate_id = state.reservation_id
                      AND receipt.status = 'accepted'
                      AND event.event_type = 'agentControl.worktree.ready')
                    AS "transitionReceipts"
                FROM agent_control_worktree_controller_operations AS operation
                JOIN agent_control_worktree_target_claims AS claim
                  ON claim.command_id = operation.command_id
                JOIN agent_control_worktree_reservation_states AS state
                  ON state.reservation_id = operation.worktree_reservation_id
                WHERE operation.command_id = ${input.commandId}
              `)[0]!;
              assert.deepStrictEqual(
                {
                  status: committed.status,
                  claimPhase: committed.claimPhase,
                  compositeStatus: committed.compositeStatus,
                  pendingToken: committed.pendingToken,
                  transitionReceipts: committed.transitionReceipts,
                },
                {
                  status: "ready",
                  claimPhase: "materialized",
                  compositeStatus: "pending",
                  pendingToken: null,
                  transitionReceipts: 1,
                },
                mutation,
              );
              const before = yield* worktreePersistenceCounts(committed.reservationId).pipe(
                Effect.provideService(SqlClient.SqlClient, harness.sqlB),
              );

              if (mutation === "project-deleted") {
                yield* harness.sqlB`
                  UPDATE projection_projects SET deleted_at = ${at}
                  WHERE project_id = ${projectId}
                `;
              } else if (mutation === "mode-left") {
                yield* harness.sqlB`
                  UPDATE agent_control_project_states SET mode = 'paused'
                  WHERE project_id = ${projectId}
                `;
              } else if (mutation === "source-sequence-changed") {
                yield* Context.get(harness.contextB, AgentControlTaskStateRepository).save(
                  {
                    ...seeded.task,
                    githubIntakeSequence: seeded.task.githubIntakeSequence + 1,
                    revision: seeded.task.revision + 1,
                    sequence: seeded.task.sequence + 1,
                  },
                  seeded.task.revision,
                );
              } else if (mutation === "lease-released") {
                yield* releaseLease(lease, `worktree-committed-ready-release-${mutation}`).pipe(
                  Effect.provide(harness.contextA),
                );
              } else if (mutation === "fence-advanced") {
                yield* releaseLease(lease, `worktree-committed-ready-release-${mutation}`).pipe(
                  Effect.provide(harness.contextA),
                );
                yield* reserveLease(seeded.stageRun, 2, 2).pipe(Effect.provide(harness.contextA));
              } else {
                const state = yield* harness.engineB.loadAuthoritative(committed.reservationId);
                assert.isNotNull(state);
                if (state === null) return;
                if (mutation === "marker-removed") {
                  assert.isNotNull(state.gitCreatedGitDir);
                  yield* fs.remove(
                    yield* ownershipMarkerPath(state.internalWorktreePath, state.gitCreatedGitDir!),
                  );
                } else if (mutation === "head-changed") {
                  yield* fs.writeFileString(
                    `${state.internalWorktreePath}/post-ready.txt`,
                    "changed\n",
                  );
                  yield* git(state.internalWorktreePath, ["add", "post-ready.txt"]);
                  yield* git(state.internalWorktreePath, ["commit", "-m", "post-ready"]);
                } else if (mutation === "registration-removed") {
                  yield* git(repo.cwd, [
                    "worktree",
                    "remove",
                    "--force",
                    state.internalWorktreePath,
                  ]);
                } else {
                  yield* Effect.promise(() =>
                    NodeFSP.rename(`${repo.cwd}/.git`, `${repo.cwd}/.git-unavailable`),
                  );
                }
              }

              const replay = yield* harness.controllerB.reserveAndMaterialize(input);
              assert.equal(replay.status, "ready", mutation);
              assert.equal(replay.reservationId, committed.reservationId, mutation);
              yield* Effect.yieldNow;
              assert.equal(yield* Ref.get(publishedReadyB), 0, mutation);
              assert.equal(yield* Ref.get(replayUseCalls), 0, mutation);
              assert.deepStrictEqual(
                yield* worktreePersistenceCounts(committed.reservationId).pipe(
                  Effect.provideService(SqlClient.SqlClient, harness.sqlB),
                ),
                before,
                mutation,
              );
              assert.deepStrictEqual(
                yield* harness.sqlB`
                  SELECT operation.status, operation.result_status AS "resultStatus",
                    claim.phase AS "claimPhase"
                  FROM agent_control_worktree_controller_operations AS operation
                  JOIN agent_control_worktree_target_claims AS claim
                    ON claim.command_id = operation.command_id
                  WHERE operation.command_id = ${input.commandId}
                `,
                [{ status: "accepted", resultStatus: "ready", claimPhase: "materialized" }],
                mutation,
              );
              yield* Fiber.interrupt(publicationB);
              yield* Fiber.interrupt(publicationA);
            }),
          );
        }
      }),
  );

  it.effect(
    "fails closed on divergent completed-claim, receipt, and stream evidence before composite accept",
    () =>
      Effect.gen(function* () {
        const corruptions = [
          "historical-pending-token",
          "claim-attempt-id",
          "expected-close-revision",
          "resulting-close-revision",
          "target-generation",
          "composite-command-id",
          "operation-type",
          "composite-fingerprint",
          "completed-phase",
          "partial-close-evidence",
          "claim-evidence",
          "missing-receipt",
          "rejected-receipt",
          "receipt-coordinates",
          "stream-bijection",
        ] as const;
        for (const corruption of corruptions) {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const transitionCommitted = yield* Deferred.make<void>();
              const holdCompositeAccept = yield* Deferred.make<void>();
              const harness = yield* makeIndependentControllerContexts(
                {
                  afterCompositeClaim: () => Effect.void,
                  afterLifecycleCheckpoint: () => Effect.void,
                  afterReadyInspection: () => Effect.void,
                  beforeCompositeAccept: () =>
                    Deferred.succeed(transitionCommitted, undefined).pipe(
                      Effect.andThen(Deferred.await(holdCompositeAccept)),
                    ),
                },
                undefined,
                false,
              );
              const repo = yield* makeRepository();
              const projectId = ProjectId.make(`worktree-committed-corrupt-${corruption}`);
              const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
                Effect.provide(harness.contextA),
              );
              yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
              const input = {
                commandId: CommandId.make(`worktree-committed-corrupt-command-${corruption}`),
                projectId,
                taskId: seeded.task.taskId,
              };
              const operation = yield* harness.controllerA
                .reserveAndMaterialize(input)
                .pipe(Effect.forkChild);
              yield* Deferred.await(transitionCommitted);
              yield* Fiber.interrupt(operation);
              assert.equal(Exit.hasInterrupts(yield* Fiber.await(operation)), true);
              const committed = (yield* harness.sqlB<{
                readonly reservationId: AgentControlWorktreeReservationId;
                readonly transitionCommandId: CommandId;
                readonly eventId: string;
              }>`
                SELECT operation.worktree_reservation_id AS "reservationId",
                  event.command_id AS "transitionCommandId", event.event_id AS "eventId"
                FROM agent_control_worktree_controller_operations AS operation
                JOIN agent_control_events AS event
                  ON event.stream_id = operation.worktree_reservation_id
                 AND event.aggregate_kind = 'worktree-reservation'
                 AND event.event_type = 'agentControl.worktree.ready'
                WHERE operation.command_id = ${input.commandId}
              `)[0]!;
              const before = yield* worktreePersistenceCounts(committed.reservationId).pipe(
                Effect.provideService(SqlClient.SqlClient, harness.sqlB),
              );
              if (
                corruption === "historical-pending-token" ||
                corruption === "claim-attempt-id" ||
                corruption === "expected-close-revision" ||
                corruption === "resulting-close-revision" ||
                corruption === "target-generation" ||
                corruption === "composite-command-id" ||
                corruption === "operation-type" ||
                corruption === "composite-fingerprint" ||
                corruption === "completed-phase" ||
                corruption === "partial-close-evidence" ||
                corruption === "claim-evidence"
              ) {
                yield* harness.sqlB`
                  DROP TRIGGER agent_control_worktree_target_claim_authority_update
                `;
                yield* harness.sqlB`PRAGMA ignore_check_constraints = ON`;
                yield* harness.sqlB`
                  UPDATE agent_control_worktree_target_claims
                  SET
                    closed_pending_token = CASE
                      WHEN ${corruption} = 'historical-pending-token' THEN 'corrupt-token'
                      WHEN ${corruption} = 'partial-close-evidence' THEN NULL
                      ELSE closed_pending_token END,
                    closed_claim_attempt_id = CASE
                      WHEN ${corruption} = 'claim-attempt-id' THEN 'corrupt-attempt'
                      ELSE closed_claim_attempt_id END,
                    closed_expected_revision = CASE
                      WHEN ${corruption} = 'expected-close-revision'
                      THEN closed_expected_revision + 1 ELSE closed_expected_revision END,
                    closed_revision = CASE
                      WHEN ${corruption} = 'resulting-close-revision'
                      THEN closed_revision + 1 ELSE closed_revision END,
                    closed_target_generation = CASE
                      WHEN ${corruption} = 'target-generation' THEN ${"e".repeat(64)}
                      ELSE closed_target_generation END,
                    closed_command_id = CASE
                      WHEN ${corruption} = 'composite-command-id' THEN 'other-composite-command'
                      ELSE closed_command_id END,
                    closed_command_type = CASE
                      WHEN ${corruption} = 'operation-type' THEN 'reconcile'
                      ELSE closed_command_type END,
                    closed_input_fingerprint = CASE
                      WHEN ${corruption} = 'composite-fingerprint' THEN ${"f".repeat(64)}
                      ELSE closed_input_fingerprint END,
                    closed_phase = CASE
                      WHEN ${corruption} = 'completed-phase' THEN 'retained-attention'
                      ELSE closed_phase END,
                    closed_verified_at = CASE
                      WHEN ${corruption} = 'claim-evidence'
                      THEN '2026-07-24T10:00:01.000Z' ELSE closed_verified_at END
                  WHERE command_id = ${input.commandId}
                `;
                yield* harness.sqlB`PRAGMA ignore_check_constraints = OFF`;
              } else if (corruption === "missing-receipt") {
                yield* harness.sqlB`
                  DELETE FROM agent_control_command_receipts
                  WHERE command_id = ${committed.transitionCommandId}
                `;
              } else if (corruption === "rejected-receipt") {
                yield* harness.sqlB`
                  UPDATE agent_control_command_receipts
                  SET status = 'rejected', event_created = 0, error_code = 'revision-conflict'
                  WHERE command_id = ${committed.transitionCommandId}
                `;
              } else if (corruption === "receipt-coordinates") {
                yield* harness.sqlB`
                  UPDATE agent_control_command_receipts
                  SET result_sequence = result_sequence + 1
                  WHERE command_id = ${committed.transitionCommandId}
                `;
              } else {
                yield* harness.sqlB`
                  DROP TRIGGER agent_control_worktree_event_envelope_immutable_delete
                `;
                yield* harness.sqlB`
                  DELETE FROM agent_control_worktree_event_envelopes
                  WHERE event_id = ${committed.eventId}
                `;
              }

              const replay = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input));
              assert.equal(replay._tag, "Failure", corruption);
              if (replay._tag === "Failure") {
                assert.equal(replay.failure.code, "reservation-projection-corrupt", corruption);
              }
              assert.notEqual(
                (yield* harness.sqlB<{ readonly status: string }>`
                  SELECT status FROM agent_control_worktree_controller_operations
                  WHERE command_id = ${input.commandId}
                `)[0]!.status,
                "accepted",
                corruption,
              );
              const after = yield* worktreePersistenceCounts(committed.reservationId).pipe(
                Effect.provideService(SqlClient.SqlClient, harness.sqlB),
              );
              assert.equal(after.events, before.events, corruption);
              assert.equal(after.projections, before.projections, corruption);
              assert.equal(
                after.receipts,
                corruption === "missing-receipt" ? before.receipts - 1 : before.receipts,
                corruption,
              );
            }),
          );
        }
      }),
  );

  it.effect("rechecks every historical close coordinate in the terminal accept CAS", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transitionCommitted = yield* Deferred.make<void>();
        const holdInitialAccept = yield* Deferred.make<void>();
        let mutateBeforeAccept: () => Effect.Effect<void> = () => Effect.void;
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: () => Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () =>
              Deferred.succeed(transitionCommitted, undefined).pipe(
                Effect.andThen(Deferred.await(holdInitialAccept)),
              ),
          },
          {
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () => Effect.suspend(mutateBeforeAccept),
          },
          false,
        );
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-terminal-close-evidence-guard");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-terminal-close-evidence-guard-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const initial = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(transitionCommitted);
        yield* Fiber.interrupt(initial);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(initial)), true);
        const baseline = (yield* harness.sqlB<{
          readonly reservationId: AgentControlWorktreeReservationId;
          readonly pendingToken: string;
          readonly claimAttemptId: string;
          readonly expectedRevision: number;
          readonly resultingRevision: number;
          readonly targetGeneration: string;
          readonly commandId: string;
          readonly commandType: string;
          readonly fingerprint: string;
          readonly phase: string;
        }>`
          SELECT reservation_id AS "reservationId",
            closed_pending_token AS "pendingToken",
            closed_claim_attempt_id AS "claimAttemptId",
            closed_expected_revision AS "expectedRevision",
            closed_revision AS "resultingRevision",
            closed_target_generation AS "targetGeneration",
            closed_command_id AS "commandId",
            closed_command_type AS "commandType",
            closed_input_fingerprint AS fingerprint,
            closed_phase AS phase
          FROM agent_control_worktree_target_claims
          WHERE command_id = ${input.commandId}
        `)[0]!;
        const before = yield* worktreePersistenceCounts(baseline.reservationId).pipe(
          Effect.provideService(SqlClient.SqlClient, harness.sqlB),
        );
        assert.equal(
          (yield* Effect.exit(harness.sqlB`
              UPDATE agent_control_worktree_target_claims
              SET closed_pending_token = 'must-be-immutable'
              WHERE command_id = ${input.commandId}
            `))._tag,
          "Failure",
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly pendingToken: string }>`
            SELECT closed_pending_token AS "pendingToken"
            FROM agent_control_worktree_target_claims
            WHERE command_id = ${input.commandId}
          `)[0]!.pendingToken,
          baseline.pendingToken,
        );
        const replayPublications = yield* Ref.make(0);
        const publication = yield* harness.engineB.streamDomainEvents.pipe(
          Stream.runForEach(() => Ref.update(replayPublications, (count) => count + 1)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        yield* harness.sqlB`
          DROP TRIGGER agent_control_worktree_target_claim_authority_update
        `;
        const corruptions = [
          "historical-pending-token",
          "claim-attempt-id",
          "expected-close-revision",
          "resulting-close-revision",
          "target-generation",
          "composite-command-id",
          "operation-type",
          "composite-fingerprint",
          "completed-phase",
          "partial-close-evidence",
        ] as const;
        for (const corruption of corruptions) {
          mutateBeforeAccept = () =>
            Effect.gen(function* () {
              yield* harness.sqlB`PRAGMA ignore_check_constraints = ON`;
              yield* harness.sqlB`
                UPDATE agent_control_worktree_target_claims
                SET
                  closed_pending_token = CASE
                    WHEN ${corruption} = 'historical-pending-token' THEN 'corrupt-token'
                    WHEN ${corruption} = 'partial-close-evidence' THEN NULL
                    ELSE closed_pending_token END,
                  closed_claim_attempt_id = CASE
                    WHEN ${corruption} = 'claim-attempt-id' THEN 'corrupt-attempt'
                    ELSE closed_claim_attempt_id END,
                  closed_expected_revision = CASE
                    WHEN ${corruption} = 'expected-close-revision'
                    THEN closed_expected_revision + 1 ELSE closed_expected_revision END,
                  closed_revision = CASE
                    WHEN ${corruption} = 'resulting-close-revision'
                    THEN closed_revision + 1 ELSE closed_revision END,
                  closed_target_generation = CASE
                    WHEN ${corruption} = 'target-generation' THEN ${"e".repeat(64)}
                    ELSE closed_target_generation END,
                  closed_command_id = CASE
                    WHEN ${corruption} = 'composite-command-id' THEN 'other-composite-command'
                    ELSE closed_command_id END,
                  closed_command_type = CASE
                    WHEN ${corruption} = 'operation-type' THEN 'reconcile'
                    ELSE closed_command_type END,
                  closed_input_fingerprint = CASE
                    WHEN ${corruption} = 'composite-fingerprint' THEN ${"f".repeat(64)}
                    ELSE closed_input_fingerprint END,
                  closed_phase = CASE
                    WHEN ${corruption} = 'completed-phase' THEN 'retained-attention'
                    ELSE closed_phase END
                WHERE command_id = ${input.commandId}
              `;
              yield* harness.sqlB`PRAGMA ignore_check_constraints = OFF`;
            }).pipe(Effect.orDie);
          const guarded = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input));
          assert.equal(guarded._tag, "Failure", corruption);
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT status, pending_token AS "pendingToken"
              FROM agent_control_worktree_controller_operations
              WHERE command_id = ${input.commandId}
            `,
            [{ status: "pending", pendingToken: null }],
            corruption,
          );
          assert.deepStrictEqual(
            yield* worktreePersistenceCounts(baseline.reservationId).pipe(
              Effect.provideService(SqlClient.SqlClient, harness.sqlB),
            ),
            before,
            corruption,
          );
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(replayPublications), 0, corruption);
          yield* harness.sqlB`PRAGMA ignore_check_constraints = ON`;
          yield* harness.sqlB`
            UPDATE agent_control_worktree_target_claims
            SET closed_pending_token = ${baseline.pendingToken},
              closed_claim_attempt_id = ${baseline.claimAttemptId},
              closed_expected_revision = ${baseline.expectedRevision},
              closed_revision = ${baseline.resultingRevision},
              closed_target_generation = ${baseline.targetGeneration},
              closed_command_id = ${baseline.commandId},
              closed_command_type = ${baseline.commandType},
              closed_input_fingerprint = ${baseline.fingerprint},
              closed_phase = ${baseline.phase}
            WHERE command_id = ${input.commandId}
          `;
          yield* harness.sqlB`PRAGMA ignore_check_constraints = OFF`;
        }
        mutateBeforeAccept = () => Effect.void;
        yield* Fiber.interrupt(publication);
      }),
    ),
  );

  it.effect("lets a waiting controller claim after the committed winner is interrupted", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const firstAccept = yield* Deferred.make<void>();
        const holdFirst = yield* Deferred.make<void>();
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: () => Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () =>
              Deferred.succeed(firstAccept, undefined).pipe(
                Effect.andThen(Deferred.await(holdFirst)),
              ),
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 10_000,
            afterReadyInspection: () => Effect.void,
          },
          false,
        );
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-committed-ready-replay-interrupt");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-committed-ready-replay-interrupt-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const first = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(firstAccept);
        const waiting = yield* harness.controllerB
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* TestClock.adjust("5 millis");
        assert.equal(waiting.pollUnsafe(), undefined);
        yield* Fiber.interrupt(first);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(first)), true);
        const reservationId = (yield* harness.sqlB<{
          readonly reservationId: AgentControlWorktreeReservationId;
        }>`
          SELECT worktree_reservation_id AS "reservationId"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `)[0]!.reservationId;
        const before = yield* worktreePersistenceCounts(reservationId).pipe(
          Effect.provideService(SqlClient.SqlClient, harness.sqlB),
        );

        yield* TestClock.adjust("5 millis");
        const replay = yield* Fiber.join(waiting);
        assert.equal(replay.status, "ready");
        assert.deepStrictEqual(
          yield* worktreePersistenceCounts(reservationId).pipe(
            Effect.provideService(SqlClient.SqlClient, harness.sqlB),
          ),
          before,
        );
      }),
    ),
  );

  it.effect("waits for the active cross-connection winner and replays its accepted composite", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transitionCommitted = yield* Deferred.make<void>();
        const holdInitialAccept = yield* Deferred.make<void>();
        const replayWaitStarted = yield* Deferred.make<void>();
        const replayUseCalls = yield* Ref.make(0);
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: () => Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () =>
              Deferred.succeed(transitionCommitted, undefined).pipe(
                Effect.andThen(Deferred.await(holdInitialAccept)),
              ),
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 10_000,
            afterReadyInspection: () => Effect.void,
            afterCompositeWaitStarted: () => Deferred.succeed(replayWaitStarted, undefined),
            beforeCompositeUse: () => Ref.update(replayUseCalls, (count) => count + 1),
          },
          false,
        );
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-committed-ready-parallel-retry");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-committed-ready-parallel-retry-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const first = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(transitionCommitted);
        const reservationId = (yield* harness.sqlB<{
          readonly reservationId: AgentControlWorktreeReservationId;
        }>`
          SELECT worktree_reservation_id AS "reservationId"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `)[0]!.reservationId;
        const before = yield* worktreePersistenceCounts(reservationId).pipe(
          Effect.provideService(SqlClient.SqlClient, harness.sqlB),
        );
        const replay = yield* harness.controllerB
          .reserveAndMaterialize(input)
          .pipe(TestClock.withLive, Effect.forkChild);
        const secondReplay = yield* harness.retryControllerB
          .reserveAndMaterialize(input)
          .pipe(TestClock.withLive, Effect.forkChild);
        yield* Deferred.await(replayWaitStarted);
        yield* Effect.yieldNow;
        assert.equal(replay.pollUnsafe(), undefined);
        assert.equal(secondReplay.pollUnsafe(), undefined);
        assert.equal(yield* Ref.get(replayUseCalls), 0);
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT status, pending_token IS NOT NULL AS claimed
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [{ status: "pending", claimed: 1 }],
        );
        yield* Deferred.succeed(holdInitialAccept, undefined);
        const accepted = yield* Fiber.join(first);
        const replayed = yield* Fiber.join(replay);
        const replayedAgain = yield* Fiber.join(secondReplay);
        assert.deepStrictEqual(replayed, accepted);
        assert.deepStrictEqual(replayedAgain, accepted);
        assert.equal(yield* Ref.get(replayUseCalls), 0);
        assert.deepStrictEqual(
          yield* worktreePersistenceCounts(reservationId).pipe(
            Effect.provideService(SqlClient.SqlClient, harness.sqlB),
          ),
          before,
        );
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT status, result_status AS "resultStatus",
              pending_token AS "pendingToken"
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [{ status: "accepted", resultStatus: "ready", pendingToken: null }],
        );
      }),
    ),
  );

  it.effect("replays the active winner's rejection without entering mutable preconditions", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const claimed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const replayUseCalls = yield* Ref.make(0);
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () =>
              Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(release))),
            afterReadyInspection: () => Effect.void,
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 10_000,
            afterReadyInspection: () => Effect.void,
            beforeCompositeUse: () => Ref.update(replayUseCalls, (count) => count + 1),
          },
          false,
        );
        const input = {
          commandId: CommandId.make("worktree-wait-rejected-command"),
          projectId: ProjectId.make("worktree-wait-rejected-missing-project"),
          taskId: AgentControlTaskId.make("worktree-wait-rejected-task"),
        };
        const winner = yield* Effect.result(harness.controllerA.reserveAndMaterialize(input)).pipe(
          Effect.forkChild,
        );
        yield* Deferred.await(claimed);
        const replay = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input)).pipe(
          Effect.forkChild,
        );
        yield* TestClock.adjust("5 millis");
        assert.equal(replay.pollUnsafe(), undefined);
        assert.equal(yield* Ref.get(replayUseCalls), 0);
        yield* Deferred.succeed(release, undefined);
        const winnerExit = yield* Fiber.join(winner);
        yield* TestClock.adjust("5 millis");
        const replayExit = yield* Fiber.join(replay);
        assert.equal(winnerExit._tag, "Failure");
        assert.equal(replayExit._tag, "Failure");
        if (winnerExit._tag === "Failure" && replayExit._tag === "Failure") {
          assert.equal(winnerExit.failure.code, "project-unavailable");
          assert.equal(replayExit.failure.code, winnerExit.failure.code);
        }
        assert.equal(yield* Ref.get(replayUseCalls), 0);
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${input.commandId}
          `)[0]!.count,
          0,
        );
      }),
    ),
  );

  it.effect("keeps an interrupted waiter receiptless and leaves the winner's claim untouched", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const claimed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () =>
              Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(release))),
            afterReadyInspection: () => Effect.void,
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 10_000,
            afterReadyInspection: () => Effect.void,
          },
          false,
        );
        const input = {
          commandId: CommandId.make("worktree-wait-interrupt-command"),
          projectId: ProjectId.make("worktree-wait-interrupt-project"),
          taskId: AgentControlTaskId.make("worktree-wait-interrupt-task"),
        };
        const winner = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(claimed);
        const ownerBefore = (yield* harness.sqlB<{
          readonly pendingToken: string;
          readonly revision: number;
        }>`
          SELECT pending_token AS "pendingToken", revision
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `)[0]!;
        const waiter = yield* harness.controllerB
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* TestClock.adjust("5 millis");
        yield* Fiber.interrupt(waiter);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(waiter)), true);
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT pending_token AS "pendingToken", revision
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [ownerBefore],
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${input.commandId}
          `)[0]!.count,
          0,
        );
        yield* Fiber.interrupt(winner);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(winner)), true);
      }),
    ),
  );

  it.effect("times out an active same-identity wait as a receiptless retry", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const claimed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () =>
              Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(release))),
            afterReadyInspection: () => Effect.void,
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 20,
            afterReadyInspection: () => Effect.void,
          },
          false,
        );
        const input = {
          commandId: CommandId.make("worktree-wait-timeout-command"),
          projectId: ProjectId.make("worktree-wait-timeout-project"),
          taskId: AgentControlTaskId.make("worktree-wait-timeout-task"),
        };
        const winner = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(claimed);
        const waiter = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input)).pipe(
          Effect.forkChild,
        );
        yield* TestClock.adjust("20 millis");
        const timedOut = yield* Fiber.join(waiter);
        assert.equal(timedOut._tag, "Failure");
        if (timedOut._tag === "Failure") {
          assert.equal(timedOut.failure.code, "lease-recovery-required");
        }
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${input.commandId}
          `)[0]!.count,
          0,
        );
        yield* Fiber.interrupt(winner);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(winner)), true);
      }),
    ),
  );

  it.effect("fails closed when an active composite revision regresses while waiting", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const claimed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const waitStarted = yield* Deferred.make<void>();
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () =>
              Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(release))),
            afterReadyInspection: () => Effect.void,
          },
          {
            compositeWaitPollIntervalMs: 5,
            compositeWaitTimeoutMs: 10_000,
            afterCompositeWaitStarted: () => Deferred.succeed(waitStarted, undefined),
            afterReadyInspection: () => Effect.void,
          },
          false,
        );
        const input = {
          commandId: CommandId.make("worktree-wait-regressing-revision-command"),
          projectId: ProjectId.make("worktree-wait-regressing-revision-project"),
          taskId: AgentControlTaskId.make("worktree-wait-regressing-revision-task"),
        };
        const winner = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(claimed);
        const waiter = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input)).pipe(
          Effect.forkChild,
        );
        yield* Deferred.await(waitStarted);
        yield* harness.sqlB`PRAGMA ignore_check_constraints = ON`;
        yield* harness.sqlB`
          UPDATE agent_control_worktree_controller_operations
          SET revision = 0
          WHERE command_id = ${input.commandId}
        `;
        yield* harness.sqlB`PRAGMA ignore_check_constraints = OFF`;
        yield* TestClock.adjust("5 millis");
        const regressed = yield* Fiber.join(waiter);
        assert.equal(regressed._tag, "Failure");
        if (regressed._tag === "Failure") {
          assert.equal(regressed.failure.code, "reservation-projection-corrupt");
        }
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_command_receipts
            WHERE command_id = ${input.commandId}
          `)[0]!.count,
          0,
        );
        yield* Fiber.interrupt(winner);
      }),
    ),
  );

  it.effect(
    "resumes the exact generation after post-mkdir identity and cleanup observations fail",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let injectTargetFaults = true;
          const observedFaultPoints: Array<string> = [];
          const hooks: AgentControlWorktreeControllerHooksShape = {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: () => Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () => Effect.void,
            targetPathFault: (point) => {
              if (!injectTargetFaults) return;
              observedFaultPoints.push(point);
              if (point === "after-mkdir-before-lstat" || point === "before-cleanup-lstat") {
                throw new Error(`injected ${point}`);
              }
            },
          };
          const harness = yield* makeIndependentControllerContexts(hooks);
          const fs = yield* FileSystem.FileSystem;
          const repo = yield* makeRepository();
          const projectId = ProjectId.make("worktree-target-cleanup-recovery");
          const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
            Effect.provide(harness.contextA),
          );
          yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
          const input = {
            commandId: CommandId.make("worktree-target-cleanup-recovery-command"),
            projectId,
            taskId: seeded.task.taskId,
          };
          const failed = yield* Effect.exit(harness.controllerA.reserveAndMaterialize(input));
          injectTargetFaults = false;
          assert.equal(Exit.isFailure(failed), true);
          assert.deepStrictEqual(observedFaultPoints, [
            "after-mkdir-before-lstat",
            "before-cleanup-lstat",
          ]);
          const target = (yield* harness.sqlB<{
            readonly path: string;
            readonly generation: string;
            readonly phase: string;
          }>`
            SELECT target_path AS path, target_generation AS generation, phase
            FROM agent_control_worktree_target_claims
            WHERE command_id = ${input.commandId}
          `)[0]!;
          assert.equal(target.phase, "prepared");
          assert.equal(yield* fs.exists(target.path), true);
          assert.deepStrictEqual(yield* fs.readDirectory(target.path), []);
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT status, pending_token AS "pendingToken",
                claim_attempt_id AS "claimAttemptId"
              FROM agent_control_worktree_controller_operations
              WHERE command_id = ${input.commandId}
            `,
            [{ status: "pending", pendingToken: null, claimAttemptId: null }],
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM agent_control_events
              WHERE aggregate_kind = 'worktree-reservation'
            `)[0]!.count,
            2,
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM agent_control_command_receipts
              WHERE aggregate_kind = 'worktree-reservation'
            `)[0]!.count,
            2,
          );

          const foreign = yield* Effect.result(
            harness.controllerB.reserveAndMaterialize({
              ...input,
              projectId: ProjectId.make("worktree-target-cleanup-foreign"),
            }),
          );
          assert.equal(foreign._tag, "Failure");
          if (foreign._tag === "Failure") {
            assert.equal(foreign.failure.code, "command-identity-mismatch");
          }

          const ready = yield* harness.controllerB.reserveAndMaterialize(input);
          assert.equal(ready.status, "ready");
          assert.equal(yield* fs.exists(target.path), true);
          assert.deepStrictEqual(
            yield* harness.sqlA`
              SELECT phase FROM agent_control_worktree_target_claims
              WHERE command_id = ${input.commandId}
            `,
            [{ phase: "materialized" }],
          );
          assert.deepStrictEqual(
            yield* harness.sqlA`
              SELECT status, result_status AS "resultStatus",
                pending_token AS "pendingToken", materialization_phase AS phase
              FROM agent_control_worktree_controller_operations
              WHERE command_id = ${input.commandId}
            `,
            [
              {
                status: "accepted",
                resultStatus: "ready",
                pendingToken: null,
                phase: "terminal",
              },
            ],
          );
        }),
      ),
  );

  it.effect(
    "closes a successful Git target as retained-attention before accepting the composite",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let injected = false;
          let worktreesDir = "";
          const hooks: AgentControlWorktreeControllerHooksShape = {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: (checkpoint, _commandId, reservationId) =>
              checkpoint === "after-git-call" && reservationId !== null && !injected
                ? Effect.promise(async () => {
                    injected = true;
                    const root = `${worktreesDir}/agent-control`;
                    const candidates: Array<{ readonly path: string; readonly mtimeMs: number }> =
                      [];
                    for (const projectDirectory of await NodeFSP.readdir(root)) {
                      const projectRoot = `${root}/${projectDirectory}`;
                      for (const targetDirectory of await NodeFSP.readdir(projectRoot)) {
                        const target = `${projectRoot}/${targetDirectory}`;
                        const info = await NodeFSP.stat(target);
                        candidates.push({ path: target, mtimeMs: info.mtimeMs });
                      }
                    }
                    const target = candidates.sort(
                      (left, right) => right.mtimeMs - left.mtimeMs,
                    )[0];
                    if (target === undefined) throw new Error("missing generated target directory");
                    await NodeFSP.writeFile(`${target.path}/post-git-dirty.txt`, "dirty\n");
                  })
                : Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () => Effect.void,
          };
          const harness = yield* makeIndependentControllerContexts(hooks);
          worktreesDir = Context.get(harness.contextA, ServerConfig).worktreesDir;
          const repo = yield* makeRepository();
          const projectId = ProjectId.make("worktree-post-git-attention");
          const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
            Effect.provide(harness.contextA),
          );
          yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
          const input = {
            commandId: CommandId.make("worktree-post-git-attention-command"),
            projectId,
            taskId: seeded.task.taskId,
          };

          const attention = yield* harness.controllerA.reserveAndMaterialize(input);
          assert.equal(attention.status, "needs-attention");
          assert.equal(attention.attentionCode, "worktree-dirty");
          assert.equal(attention.materializationPhase, "git-created");
          assert.isNotNull(attention.gitCreatedDevice);
          assert.isNotNull(attention.gitCreatedInode);
          assert.isNotNull(attention.gitCreatedGitDir);
          assert.equal(attention.markedOwnershipFingerprint, null);
          assert.equal(
            yield* Effect.promise(() =>
              NodeFSP.readFile(`${attention.internalWorktreePath}/post-git-dirty.txt`, "utf8"),
            ),
            "dirty\n",
          );
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT claim.phase, claim.closed_git_device AS "gitDevice",
                claim.closed_git_inode AS "gitInode",
                claim.closed_git_dir AS "gitDir",
                operation.status, operation.result_status AS "resultStatus"
              FROM agent_control_worktree_target_claims AS claim
              JOIN agent_control_worktree_controller_operations AS operation
                ON operation.command_id = claim.command_id
              WHERE claim.command_id = ${input.commandId}
            `,
            [
              {
                phase: "retained-attention",
                gitDevice: attention.gitCreatedDevice,
                gitInode: attention.gitCreatedInode,
                gitDir: attention.gitCreatedGitDir,
                status: "accepted",
                resultStatus: "needs-attention",
              },
            ],
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM agent_control_worktree_target_claims
              WHERE command_id = ${input.commandId}
                AND phase IN ('prepared', 'acquired')
            `)[0]!.count,
            0,
          );
          const replay = yield* harness.controllerB.reserveAndMaterialize(input);
          assert.deepStrictEqual(replay, attention);
        }),
      ),
  );

  it.effect(
    "replays a committed retained Attention transition through reconcile without mutable checks",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let worktreesDir = "";
          let dirtied = false;
          const transitionCommitted = yield* Deferred.make<void>();
          const holdCompositeAccept = yield* Deferred.make<void>();
          const replayUseCalls = yield* Ref.make(0);
          const hooks: AgentControlWorktreeControllerHooksShape = {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: (checkpoint) =>
              checkpoint === "after-ownership-marked" && !dirtied
                ? Effect.promise(async () => {
                    dirtied = true;
                    const root = `${worktreesDir}/agent-control`;
                    const candidates: Array<{ readonly path: string; readonly mtimeMs: number }> =
                      [];
                    for (const projectDirectory of await NodeFSP.readdir(root)) {
                      const projectRoot = `${root}/${projectDirectory}`;
                      for (const targetDirectory of await NodeFSP.readdir(projectRoot)) {
                        const target = `${projectRoot}/${targetDirectory}`;
                        const info = await NodeFSP.stat(target);
                        candidates.push({ path: target, mtimeMs: info.mtimeMs });
                      }
                    }
                    const target = candidates.sort(
                      (left, right) => right.mtimeMs - left.mtimeMs,
                    )[0];
                    if (target === undefined) throw new Error("missing generated target directory");
                    await NodeFSP.writeFile(`${target.path}/post-marker-dirty.txt`, "dirty\n");
                  })
                : Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () =>
              Deferred.succeed(transitionCommitted, undefined).pipe(
                Effect.andThen(Deferred.await(holdCompositeAccept)),
              ),
          };
          const harness = yield* makeIndependentControllerContexts(
            hooks,
            {
              compositeWaitPollIntervalMs: 5,
              compositeWaitTimeoutMs: 10_000,
              afterReadyInspection: () => Effect.void,
              beforeCompositeUse: () => Ref.update(replayUseCalls, (count) => count + 1),
            },
            false,
          );
          worktreesDir = Context.get(harness.contextA, ServerConfig).worktreesDir;
          const fs = yield* FileSystem.FileSystem;
          const repo = yield* makeRepository();
          const projectId = ProjectId.make("worktree-committed-attention-reconcile");
          const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
            Effect.provide(harness.contextA),
          );
          const lease = yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
          const reserved = yield* reserveWorktreeOnly({
            commandId: "worktree-committed-attention-reserve",
            task: seeded.task,
            stageRun: seeded.stageRun,
            lease,
            repositoryWorkspace: repo.cwd,
            baseCommitSha: repo.baseCommitSha,
          }).pipe(Effect.provide(harness.contextA));
          const materializing = yield* startMaterializing(
            reserved,
            "worktree-committed-attention-materializing",
          ).pipe(Effect.provide(harness.contextA));
          const input = {
            commandId: CommandId.make("worktree-committed-attention-reconcile-command"),
            projectId,
            reservationId: materializing.reservationId,
          };
          const publishedAttentionA = yield* Ref.make(0);
          const publishedAttentionB = yield* Ref.make(0);
          const publicationA = yield* Context.get(
            harness.contextA,
            AgentControlWorktreeEngine,
          ).streamDomainEvents.pipe(
            Stream.filter((event) => event.type === "agentControl.worktree.needsAttention"),
            Stream.runForEach(() => Ref.update(publishedAttentionA, (count) => count + 1)),
            Effect.forkChild,
          );
          const publicationB = yield* harness.engineB.streamDomainEvents.pipe(
            Stream.filter((event) => event.type === "agentControl.worktree.needsAttention"),
            Stream.runForEach(() => Ref.update(publishedAttentionB, (count) => count + 1)),
            Effect.forkChild,
          );
          yield* Effect.yieldNow;
          const operation = yield* harness.controllerA.reconcile(input).pipe(Effect.forkChild);
          yield* Deferred.await(transitionCommitted);
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(publishedAttentionA), 1);
          const waitingReplay = yield* harness.controllerB.reconcile(input).pipe(Effect.forkChild);
          yield* TestClock.adjust("5 millis");
          yield* Effect.yieldNow;
          assert.equal(waitingReplay.pollUnsafe(), undefined);
          assert.equal(yield* Ref.get(replayUseCalls), 0);

          const committed = (yield* harness.sqlB<{
            readonly status: string;
            readonly attentionCode: string;
            readonly materializationPhase: string;
            readonly markedOwnershipFingerprint: string;
            readonly gitDir: string;
            readonly claimPhase: string;
            readonly compositeStatus: string;
            readonly pendingToken: string | null;
          }>`
            SELECT state.status, state.attention_code AS "attentionCode",
              state.materialization_phase AS "materializationPhase",
              state.marked_ownership_fingerprint AS "markedOwnershipFingerprint",
              state.git_created_git_dir AS "gitDir", claim.phase AS "claimPhase",
              operation.status AS "compositeStatus",
              operation.pending_token AS "pendingToken"
            FROM agent_control_worktree_controller_operations AS operation
            JOIN agent_control_worktree_target_claims AS claim
              ON claim.command_id = operation.command_id
            JOIN agent_control_worktree_reservation_states AS state
              ON state.reservation_id = operation.worktree_reservation_id
            WHERE operation.command_id = ${input.commandId}
          `)[0]!;
          assert.deepStrictEqual(
            {
              status: committed.status,
              attentionCode: committed.attentionCode,
              materializationPhase: committed.materializationPhase,
              claimPhase: committed.claimPhase,
              compositeStatus: committed.compositeStatus,
              claimed: committed.pendingToken !== null,
            },
            {
              status: "needs-attention",
              attentionCode: "worktree-dirty",
              materializationPhase: "ownership-marked",
              claimPhase: "retained-attention",
              compositeStatus: "pending",
              claimed: true,
            },
          );
          const before = yield* worktreePersistenceCounts(materializing.reservationId).pipe(
            Effect.provideService(SqlClient.SqlClient, harness.sqlB),
          );
          const markerPath = yield* ownershipMarkerPath(
            materializing.internalWorktreePath,
            committed.gitDir,
          );
          assert.equal(yield* fs.exists(markerPath), true);
          yield* fs.remove(markerPath);
          yield* releaseLease(lease, "worktree-committed-attention-release").pipe(
            Effect.provide(harness.contextA),
          );
          yield* reserveLease(seeded.stageRun, 2, 2).pipe(Effect.provide(harness.contextA));
          yield* harness.sqlB`
            UPDATE projection_projects SET deleted_at = ${at}
            WHERE project_id = ${projectId}
          `;
          yield* harness.sqlB`
            UPDATE agent_control_project_states SET mode = 'paused'
            WHERE project_id = ${projectId}
          `;
          yield* Context.get(harness.contextB, AgentControlTaskStateRepository).save(
            {
              ...seeded.task,
              githubIntakeSequence: seeded.task.githubIntakeSequence + 1,
              revision: seeded.task.revision + 1,
              sequence: seeded.task.sequence + 1,
            },
            seeded.task.revision,
          );
          yield* Effect.promise(() =>
            NodeFSP.rename(`${repo.cwd}/.git`, `${repo.cwd}/.git-unavailable`),
          );

          yield* Deferred.succeed(holdCompositeAccept, undefined);
          yield* Fiber.join(operation);
          yield* TestClock.adjust("5 millis");
          const replay = yield* Fiber.join(waitingReplay);
          assert.equal(replay.status, "needs-attention");
          assert.equal(replay.attentionCode, "worktree-dirty");
          assert.equal(replay.markedOwnershipFingerprint, committed.markedOwnershipFingerprint);
          yield* Effect.yieldNow;
          assert.equal(yield* Ref.get(publishedAttentionB), 0);
          assert.equal(yield* Ref.get(replayUseCalls), 0);
          assert.deepStrictEqual(
            yield* worktreePersistenceCounts(materializing.reservationId).pipe(
              Effect.provideService(SqlClient.SqlClient, harness.sqlB),
            ),
            before,
          );
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT operation.status, operation.result_status AS "resultStatus",
                claim.phase AS "claimPhase", claim.closed_attention_code AS "attentionCode"
              FROM agent_control_worktree_controller_operations AS operation
              JOIN agent_control_worktree_target_claims AS claim
                ON claim.command_id = operation.command_id
              WHERE operation.command_id = ${input.commandId}
            `,
            [
              {
                status: "accepted",
                resultStatus: "needs-attention",
                claimPhase: "retained-attention",
                attentionCode: "worktree-dirty",
              },
            ],
          );
          yield* Fiber.interrupt(publicationB);
          yield* Fiber.interrupt(publicationA);
        }),
      ),
  );

  it.effect("fails closed when a historical accepted result disagrees with result_status", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeIndependentControllerContexts();
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-result-status-corruption");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-result-status-corruption-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const ready = yield* harness.controllerA.reserveAndMaterialize(input);
        assert.equal(ready.status, "ready");
        assert.equal(
          (yield* Effect.result(harness.sqlA`
              UPDATE agent_control_worktree_controller_operations
              SET result_status = 'needs-attention'
              WHERE command_id = ${input.commandId}
            `))._tag,
          "Failure",
        );

        yield* harness.sqlA`DROP TRIGGER agent_control_worktree_operation_result_json_update`;
        yield* harness.sqlA`DROP TRIGGER agent_control_worktree_terminal_operation_target_guard`;
        yield* harness.sqlA`PRAGMA ignore_check_constraints = ON`;
        yield* harness.sqlA`
          UPDATE agent_control_worktree_controller_operations
          SET result_status = 'needs-attention'
          WHERE command_id = ${input.commandId}
        `;
        yield* harness.sqlA`PRAGMA ignore_check_constraints = OFF`;

        const rpc = Context.get(harness.contextB, AgentControlWorktree);
        const corruptGet = yield* Effect.result(
          rpc.getReservation({ projectId, reservationId: ready.reservationId }),
        );
        assert.equal(corruptGet._tag, "Failure");
        if (corruptGet._tag === "Failure") {
          assert.equal(corruptGet.failure.code, "reservation-projection-corrupt");
        }
        const corruptList = yield* rpc.listReservations({ projectId });
        assert.deepStrictEqual(corruptList.reservations, []);
        assert.equal(corruptList.quarantinedCount, 1);

        const replay = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input));
        assert.equal(replay._tag, "Failure");
        if (replay._tag === "Failure") {
          assert.equal(replay.failure.code, "reservation-projection-corrupt");
        }
      }),
    ),
  );

  it.effect("recovers a materialized target after the Ready event transaction rolls back", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeIndependentControllerContexts();
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-ready-commit-recovery");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        yield* harness.sqlA`
            CREATE TRIGGER fail_worktree_ready_commit
            BEFORE INSERT ON agent_control_events
            WHEN NEW.aggregate_kind = 'worktree-reservation'
              AND NEW.event_type = 'agentControl.worktree.ready'
            BEGIN
              SELECT RAISE(ABORT, 'injected Ready commit failure');
            END
          `;
        const input = {
          commandId: CommandId.make("worktree-ready-commit-recovery-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const first = yield* Effect.result(harness.controllerA.reserveAndMaterialize(input));
        assert.equal(first._tag, "Failure");
        if (first._tag === "Failure") {
          assert.equal(first.failure.code, "internal-persistence-error");
        }
        const proof = (yield* harness.sqlB<{
          readonly targetPath: string;
          readonly targetInode: number;
          readonly marker: string;
          readonly verifiedAt: string;
        }>`
            SELECT target_path AS "targetPath", target_inode AS "targetInode",
              closed_ownership_fingerprint AS marker,
              closed_verified_at AS "verifiedAt"
            FROM agent_control_worktree_target_claims
            WHERE command_id = ${input.commandId} AND phase = 'materialized'
          `)[0]!;
        assert.isNotNull(proof.marker);
        assert.isNotNull(proof.verifiedAt);
        const targetBefore = yield* Effect.promise(() => NodeFSP.lstat(proof.targetPath));
        assert.equal(targetBefore.ino, proof.targetInode);
        assert.deepStrictEqual(
          yield* harness.sqlB`
              SELECT status, pending_token AS "pendingToken",
                materialization_phase AS phase
              FROM agent_control_worktree_controller_operations
              WHERE command_id = ${input.commandId}
            `,
          [{ status: "pending", pendingToken: null, phase: "ownership-marked" }],
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_events
              WHERE aggregate_kind = 'worktree-reservation'
                AND stream_id = (
                  SELECT reservation_id FROM agent_control_worktree_target_claims
                  WHERE command_id = ${input.commandId}
                )
            `)[0]!.count,
          2,
        );
        yield* harness.sqlB`DROP TRIGGER fail_worktree_ready_commit`;
        const published = yield* Stream.runCollect(
          harness.engineB.streamDomainEvents.pipe(Stream.take(1)),
        ).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        const ready = yield* harness.controllerB.reserveAndMaterialize(input);
        const emitted = yield* Fiber.join(published);
        assert.equal(emitted.length, 1);
        assert.equal(emitted[0]?.type, "agentControl.worktree.ready");
        assert.equal(ready.status, "ready");
        assert.equal(
          (yield* Effect.promise(() => NodeFSP.lstat(proof.targetPath))).ino,
          proof.targetInode,
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_events
              WHERE aggregate_kind = 'worktree-reservation'
                AND stream_id = ${ready.reservationId}
                AND event_type = 'agentControl.worktree.ready'
            `)[0]!.count,
          1,
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_command_receipts
              WHERE aggregate_kind = 'worktree-reservation'
                AND aggregate_id = ${ready.reservationId}
            `)[0]!.count,
          3,
        );
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM agent_control_command_receipts AS receipt
              JOIN agent_control_events AS event
                ON event.command_id = receipt.command_id
              WHERE receipt.aggregate_kind = 'worktree-reservation'
                AND receipt.aggregate_id = ${ready.reservationId}
                AND receipt.status = 'accepted'
                AND event.event_type = 'agentControl.worktree.ready'
            `)[0]!.count,
          1,
        );
        assert.deepStrictEqual(
          yield* harness.sqlB`
              SELECT operation.status, operation.result_status AS "resultStatus",
                claim.phase
              FROM agent_control_worktree_controller_operations AS operation
              JOIN agent_control_worktree_target_claims AS claim
                ON claim.command_id = operation.command_id
              WHERE operation.command_id = ${input.commandId}
            `,
          [{ status: "accepted", resultStatus: "ready", phase: "materialized" }],
        );
      }),
    ),
  );

  it.effect("recovers materialized proof after Ready projection or receipt rollback", () =>
    Effect.gen(function* () {
      for (const failurePoint of ["projection", "receipt"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeIndependentControllerContexts();
            const repo = yield* makeRepository();
            const projectId = ProjectId.make(`worktree-ready-${failurePoint}-recovery`);
            const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
              Effect.provide(harness.contextA),
            );
            yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
            if (failurePoint === "projection") {
              yield* harness.sqlA`
                CREATE TRIGGER fail_worktree_ready_projection
                BEFORE UPDATE ON agent_control_worktree_reservation_states
                WHEN NEW.status = 'ready'
                BEGIN
                  SELECT RAISE(ABORT, 'injected Ready projection failure');
                END
              `;
            } else {
              yield* harness.sqlA`
                CREATE TRIGGER fail_worktree_ready_receipt
                BEFORE INSERT ON agent_control_command_receipts
                WHEN NEW.aggregate_kind = 'worktree-reservation'
                  AND EXISTS (
                    SELECT 1 FROM agent_control_worktree_target_claims AS claim
                    WHERE claim.reservation_id = NEW.aggregate_id
                      AND claim.phase = 'materialized'
                  )
                BEGIN
                  SELECT RAISE(ABORT, 'injected Ready receipt failure');
                END
              `;
            }
            const input = {
              commandId: CommandId.make(`worktree-ready-${failurePoint}-recovery-command`),
              projectId,
              taskId: seeded.task.taskId,
            };
            const first = yield* Effect.result(harness.controllerA.reserveAndMaterialize(input));
            assert.equal(first._tag, "Failure", failurePoint);
            if (first._tag === "Failure") {
              assert.equal(first.failure.code, "internal-persistence-error", failurePoint);
            }
            const proof = (yield* harness.sqlB<{
              readonly reservationId: string;
              readonly targetPath: string;
              readonly targetInode: number;
            }>`
              SELECT reservation_id AS "reservationId", target_path AS "targetPath",
                target_inode AS "targetInode"
              FROM agent_control_worktree_target_claims
              WHERE command_id = ${input.commandId} AND phase = 'materialized'
            `)[0]!;
            assert.equal(
              (yield* Effect.promise(() => NodeFSP.lstat(proof.targetPath))).ino,
              proof.targetInode,
            );
            assert.deepStrictEqual(
              yield* harness.sqlB`
                SELECT operation.status, operation.pending_token AS "pendingToken",
                  projection.status AS "projectionStatus"
                FROM agent_control_worktree_controller_operations AS operation
                JOIN agent_control_worktree_reservation_states AS projection
                  ON projection.reservation_id = operation.worktree_reservation_id
                WHERE operation.command_id = ${input.commandId}
              `,
              [{ status: "pending", pendingToken: null, projectionStatus: "materializing" }],
            );
            assert.equal(
              (yield* harness.sqlB<{ readonly count: number }>`
                SELECT COUNT(*) AS count FROM agent_control_events
                WHERE aggregate_kind = 'worktree-reservation'
                  AND stream_id = ${proof.reservationId}
                  AND event_type = 'agentControl.worktree.ready'
              `)[0]!.count,
              0,
            );
            assert.equal(
              (yield* harness.sqlB<{ readonly count: number }>`
                SELECT COUNT(*) AS count FROM agent_control_command_receipts
                WHERE aggregate_kind = 'worktree-reservation'
                  AND aggregate_id = ${proof.reservationId}
              `)[0]!.count,
              2,
            );
            if (failurePoint === "projection") {
              yield* harness.sqlB`DROP TRIGGER fail_worktree_ready_projection`;
            } else {
              yield* harness.sqlB`DROP TRIGGER fail_worktree_ready_receipt`;
            }
            const published = yield* Stream.runCollect(
              harness.engineB.streamDomainEvents.pipe(Stream.take(1)),
            ).pipe(Effect.forkChild);
            yield* Effect.yieldNow;
            const ready = yield* harness.controllerB.reserveAndMaterialize(input);
            const emitted = yield* Fiber.join(published);
            assert.equal(ready.status, "ready", failurePoint);
            assert.equal(emitted.length, 1, failurePoint);
            assert.equal(emitted[0]?.type, "agentControl.worktree.ready", failurePoint);
            assert.equal(
              (yield* Effect.promise(() => NodeFSP.lstat(proof.targetPath))).ino,
              proof.targetInode,
            );
            assert.deepStrictEqual(
              yield* harness.sqlB`
                SELECT
                  (SELECT COUNT(*) FROM agent_control_events
                    WHERE aggregate_kind = 'worktree-reservation'
                      AND stream_id = ${ready.reservationId}
                      AND event_type = 'agentControl.worktree.ready') AS events,
                  (SELECT COUNT(*) FROM agent_control_command_receipts
                    WHERE aggregate_kind = 'worktree-reservation'
                      AND aggregate_id = ${ready.reservationId}) AS receipts,
                  (SELECT status FROM agent_control_worktree_controller_operations
                    WHERE command_id = ${input.commandId}) AS composite,
                  (SELECT phase FROM agent_control_worktree_target_claims
                    WHERE command_id = ${input.commandId}) AS claim
              `,
              [{ events: 1, receipts: 3, composite: "accepted", claim: "materialized" }],
            );
          }),
        );
      }
    }),
  );

  it.effect(
    "recovers the same retained Attention transition after its event transaction rolls back",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          let worktreesDir = "";
          let injected = false;
          const hooks: AgentControlWorktreeControllerHooksShape = {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: (checkpoint, _commandId, reservationId) =>
              checkpoint === "after-git-call" && reservationId !== null && !injected
                ? Effect.promise(async () => {
                    injected = true;
                    const root = `${worktreesDir}/agent-control`;
                    const candidates: Array<{ readonly path: string; readonly mtimeMs: number }> =
                      [];
                    for (const projectDirectory of await NodeFSP.readdir(root)) {
                      const projectRoot = `${root}/${projectDirectory}`;
                      for (const targetDirectory of await NodeFSP.readdir(projectRoot)) {
                        const target = `${projectRoot}/${targetDirectory}`;
                        const info = await NodeFSP.stat(target);
                        candidates.push({ path: target, mtimeMs: info.mtimeMs });
                      }
                    }
                    const target = candidates.sort(
                      (left, right) => right.mtimeMs - left.mtimeMs,
                    )[0];
                    if (target === undefined) throw new Error("missing generated target directory");
                    await NodeFSP.writeFile(`${target.path}/post-git-dirty.txt`, "dirty\n");
                  })
                : Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAccept: () => Effect.void,
          };
          const harness = yield* makeIndependentControllerContexts(hooks);
          worktreesDir = Context.get(harness.contextA, ServerConfig).worktreesDir;
          const repo = yield* makeRepository();
          const projectId = ProjectId.make("worktree-attention-commit-recovery");
          const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
            Effect.provide(harness.contextA),
          );
          yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
          yield* harness.sqlA`
            CREATE TRIGGER fail_worktree_attention_commit
            BEFORE INSERT ON agent_control_events
            WHEN NEW.aggregate_kind = 'worktree-reservation'
              AND NEW.event_type = 'agentControl.worktree.needsAttention'
            BEGIN
              SELECT RAISE(ABORT, 'injected Attention commit failure');
            END
          `;
          const input = {
            commandId: CommandId.make("worktree-attention-commit-recovery-command"),
            projectId,
            taskId: seeded.task.taskId,
          };
          const first = yield* Effect.result(harness.controllerA.reserveAndMaterialize(input));
          assert.equal(first._tag, "Failure");
          if (first._tag === "Failure") {
            assert.equal(first.failure.code, "internal-persistence-error");
          }
          const proof = (yield* harness.sqlB<{
            readonly targetPath: string;
            readonly targetInode: number;
            readonly attentionCode: string;
            readonly materializationPhase: string;
          }>`
            SELECT target_path AS "targetPath", target_inode AS "targetInode",
              closed_attention_code AS "attentionCode",
              closed_materialization_phase AS "materializationPhase"
            FROM agent_control_worktree_target_claims
            WHERE command_id = ${input.commandId} AND phase = 'retained-attention'
          `)[0]!;
          assert.equal(proof.attentionCode, "worktree-dirty");
          assert.equal(proof.materializationPhase, "git-created");
          assert.equal(
            (yield* Effect.promise(() => NodeFSP.lstat(proof.targetPath))).ino,
            proof.targetInode,
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_worktree_target_claims
              WHERE target_path = ${proof.targetPath}
                AND phase IN ('prepared', 'acquired')
            `)[0]!.count,
            0,
          );
          yield* harness.sqlB`DROP TRIGGER fail_worktree_attention_commit`;
          const published = yield* Stream.runCollect(
            harness.engineB.streamDomainEvents.pipe(Stream.take(1)),
          ).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          const attention = yield* harness.controllerB.reserveAndMaterialize(input);
          const emitted = yield* Fiber.join(published);
          assert.equal(emitted.length, 1);
          assert.equal(emitted[0]?.type, "agentControl.worktree.needsAttention");
          assert.equal(attention.status, "needs-attention");
          assert.equal(attention.attentionCode, proof.attentionCode);
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_events
              WHERE aggregate_kind = 'worktree-reservation'
                AND stream_id = ${attention.reservationId}
                AND event_type = 'agentControl.worktree.needsAttention'
            `)[0]!.count,
            1,
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count FROM agent_control_command_receipts
              WHERE aggregate_kind = 'worktree-reservation'
                AND aggregate_id = ${attention.reservationId}
            `)[0]!.count,
            3,
          );
          assert.equal(
            (yield* harness.sqlB<{ readonly count: number }>`
              SELECT COUNT(*) AS count
              FROM agent_control_command_receipts AS receipt
              JOIN agent_control_events AS event
                ON event.command_id = receipt.command_id
              WHERE receipt.aggregate_kind = 'worktree-reservation'
                AND receipt.aggregate_id = ${attention.reservationId}
                AND receipt.status = 'accepted'
                AND event.event_type = 'agentControl.worktree.needsAttention'
            `)[0]!.count,
            1,
          );
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT operation.status, operation.result_status AS "resultStatus",
                claim.phase, claim.closed_attention_code AS "attentionCode"
              FROM agent_control_worktree_controller_operations AS operation
              JOIN agent_control_worktree_target_claims AS claim
                ON claim.command_id = operation.command_id
              WHERE operation.command_id = ${input.commandId}
            `,
            [
              {
                status: "accepted",
                resultStatus: "needs-attention",
                phase: "retained-attention",
                attentionCode: "worktree-dirty",
              },
            ],
          );
        }),
      ),
  );

  it.effect("binds accepted replay to every relational coordinate and authoritative field", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeIndependentControllerContexts();
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-full-replay-corruption");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-full-replay-corruption-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const ready = yield* harness.controllerA.reserveAndMaterialize(input);
        const original = (yield* harness.sqlA<{
          readonly resultJson: string;
          readonly resultSequence: number;
        }>`
          SELECT result_json AS "resultJson", result_sequence AS "resultSequence"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `)[0]!;
        yield* harness.sqlA`DROP TRIGGER agent_control_worktree_operation_result_json_update`;
        yield* harness.sqlA`PRAGMA ignore_check_constraints = ON`;

        const mutations: ReadonlyArray<readonly [string, string, string | number]> = [
          ["revision", "$.revision", 99],
          ["sequence", "$.sequence", 999],
          ["reservation", "$.reservationId", "worktree-reservation-foreign"],
          ["path", "$.internalWorktreePath", "/private/foreign-worktree"],
          ["repository", "$.repository.canonicalKey", "github.com/other/repository"],
          ["lease", "$.leaseId", "foreign-lease"],
          ["fence", "$.fenceToken", 99],
          ["generation", "$.targetGenerationId", "f".repeat(64)],
          ["git-evidence", "$.gitCreatedInode", 999_999],
        ];
        for (const [label, jsonPath, value] of mutations) {
          yield* harness.sqlA`
            UPDATE agent_control_worktree_controller_operations
            SET result_json = json_set(${original.resultJson}, ${jsonPath}, ${value}),
              result_sequence = ${original.resultSequence}
            WHERE command_id = ${input.commandId}
          `;
          const replay = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input));
          assert.equal(replay._tag, "Failure", label);
          if (replay._tag === "Failure") {
            assert.equal(replay.failure.code, "reservation-projection-corrupt", label);
          }
        }
        yield* harness.sqlA`
          UPDATE agent_control_worktree_controller_operations
          SET result_json = ${original.resultJson},
            result_sequence = ${original.resultSequence + 1}
          WHERE command_id = ${input.commandId}
        `;
        const relationalSequence = yield* Effect.result(
          harness.controllerB.reserveAndMaterialize(input),
        );
        assert.equal(relationalSequence._tag, "Failure");
        if (relationalSequence._tag === "Failure") {
          assert.equal(relationalSequence.failure.code, "reservation-projection-corrupt");
        }
        yield* harness.sqlA`PRAGMA ignore_check_constraints = OFF`;
        assert.equal(ready.status, "ready");
      }),
    ),
  );

  it.effect("reconciles a target claim whose exact empty target was already removed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const reached = yield* Deferred.make<void>();
        const hold = yield* Deferred.make<void>();
        const hooks: AgentControlWorktreeControllerHooksShape = {
          afterCompositeClaim: () => Effect.void,
          afterLifecycleCheckpoint: (checkpoint) =>
            checkpoint === "after-target-acquired"
              ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(hold)))
              : checkpoint === "after-target-remove-before-claim-delete"
                ? Effect.die("injected claim delete failure")
                : Effect.void,
          afterReadyInspection: () => Effect.void,
          beforeCompositeAccept: () => Effect.void,
        };
        const harness = yield* makeIndependentControllerContexts(hooks);
        const fs = yield* FileSystem.FileSystem;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-target-claim-without-target");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-target-claim-without-target-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const operation = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.forkChild);
        yield* Deferred.await(reached);
        yield* Fiber.interrupt(operation);
        yield* Fiber.await(operation);
        const claim = (yield* harness.sqlB<{ readonly path: string }>`
          SELECT target_path AS path
          FROM agent_control_worktree_target_claims
          WHERE command_id = ${input.commandId}
        `)[0]!;
        assert.equal(yield* fs.exists(claim.path), false);
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT status, pending_token AS "pendingToken"
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [{ status: "pending", pendingToken: null }],
        );

        const ready = yield* harness.controllerB.reserveAndMaterialize(input);
        assert.equal(ready.status, "ready");
        assert.deepStrictEqual(
          yield* harness.sqlA`
            SELECT phase FROM agent_control_worktree_target_claims
            WHERE command_id = ${input.commandId}
          `,
          [{ phase: "materialized" }],
        );
      }),
    ),
  );

  it.effect(
    "keeps a foreign fingerprint and operation from poisoning an active claim across two SQLite connections",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const { cwd } = yield* makeRepository();
        const harness = yield* makeIndependentControllerContexts();
        const projectId = ProjectId.make("worktree-composite-cross-layer");
        const seeded = yield* seedPrepared(projectId, cwd).pipe(Effect.provide(harness.contextA));
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const commonOutput = (yield* git(cwd, ["rev-parse", "--git-common-dir"])).stdout.trim();
        const commonDir = yield* fs.realPath(
          path.isAbsolute(commonOutput) ? commonOutput : path.resolve(cwd, commonOutput),
        );
        const lockPath = path.join(commonDir, "t3-agent-control.lock");
        yield* fs.makeDirectory(lockPath, { mode: 0o700 });

        const commandId = CommandId.make("worktree-composite-cross-layer-command");
        const publications = yield* Stream.runCollect(
          Context.get(harness.contextA, AgentControlWorktreeEngine).streamDomainEvents.pipe(
            Stream.take(3),
          ),
        ).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        const owner = yield* harness.controllerA
          .reserveAndMaterialize({
            commandId,
            projectId,
            taskId: seeded.task.taskId,
          })
          .pipe(Effect.forkChild);

        let pendingToken: string | null = null;
        for (let attempt = 0; attempt < 100; attempt += 1) {
          const rows = yield* harness.sqlB<{
            readonly pendingToken: string | null;
            readonly status: string;
          }>`
            SELECT pending_token AS "pendingToken", status
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${commandId}
          `;
          if (rows[0]?.pendingToken) {
            pendingToken = rows[0].pendingToken;
            break;
          }
          yield* Effect.yieldNow;
        }
        assert.isNotNull(pendingToken);

        const mismatch = yield* Effect.result(
          harness.controllerB.reserveAndMaterialize({
            commandId,
            projectId: ProjectId.make("worktree-composite-cross-layer-foreign"),
            taskId: AgentControlTaskId.make("worktree-composite-cross-layer-foreign-task"),
          }),
        );
        assert.equal(mismatch._tag, "Failure");
        if (mismatch._tag === "Failure") {
          assert.equal(mismatch.failure.code, "command-identity-mismatch");
        }
        const operationCollision = yield* Effect.result(
          harness.controllerB.reconcile({
            commandId,
            projectId,
            reservationId: AgentControlWorktreeReservationId.make(
              "worktree-composite-cross-layer-foreign-reservation",
            ),
          }),
        );
        assert.equal(operationCollision._tag, "Failure");
        if (operationCollision._tag === "Failure") {
          assert.equal(operationCollision.failure.code, "command-identity-mismatch");
        }
        const afterForeign = (yield* harness.sqlB<{
          readonly pendingToken: string | null;
          readonly status: string;
        }>`
          SELECT pending_token AS "pendingToken", status
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${commandId}
        `)[0]!;
        assert.equal(afterForeign.pendingToken, pendingToken);
        assert.equal(afterForeign.status, "pending");

        yield* fs.remove(lockPath, { recursive: true });
        yield* TestClock.adjust("50 millis");
        const completed = yield* Fiber.join(owner);
        assert.equal(completed.status, "ready");
        assert.equal((yield* Fiber.join(publications)).length, 3);
        const operation = (yield* harness.sqlB<{
          readonly status: string;
          readonly pendingToken: string | null;
        }>`
          SELECT status, pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${commandId}
        `)[0]!;
        assert.equal(operation.status, "accepted");
        assert.equal(operation.pendingToken, null);
        assert.equal(
          (yield* harness.sqlB<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_events
            WHERE aggregate_kind = 'worktree-reservation'
              AND stream_id = ${completed.reservationId}
          `)[0]!.count,
          3,
        );
      }),
  );

  it.effect("keeps a failed claim release recoverable by the exact local owner", () =>
    Effect.gen(function* () {
      const claimed = yield* Deferred.make<void>();
      const hold = yield* Deferred.make<void>();
      let blockFirstClaim = true;
      const harness = yield* makeIndependentControllerContexts({
        afterCompositeClaim: () => {
          if (!blockFirstClaim) return Effect.void;
          blockFirstClaim = false;
          return Deferred.succeed(claimed, undefined).pipe(Effect.andThen(Deferred.await(hold)));
        },
        afterReadyInspection: () => Effect.void,
      });
      const repo = yield* makeRepository();
      const projectId = ProjectId.make("worktree-claim-release-sql-recovery");
      const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
        Effect.provide(harness.contextA),
      );
      yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const input = {
        commandId: CommandId.make("worktree-claim-release-sql-recovery-command"),
        projectId,
        taskId: seeded.task.taskId,
      };
      const operation = yield* harness.controllerA
        .reserveAndMaterialize(input)
        .pipe(Effect.forkChild);
      yield* Deferred.await(claimed);
      yield* harness.sqlB.unsafe(`
        CREATE TRIGGER fail_worktree_claim_release
        BEFORE UPDATE ON agent_control_worktree_controller_operations
        WHEN OLD.command_id = 'worktree-claim-release-sql-recovery-command'
          AND OLD.pending_token IS NOT NULL
          AND NEW.pending_token IS NULL
          AND NEW.status = 'pending'
        BEGIN
          SELECT RAISE(ABORT, 'injected claim release failure');
        END
      `);
      yield* Fiber.interrupt(operation);
      const interrupted = yield* Fiber.await(operation);
      assert.equal(Exit.hasInterrupts(interrupted), true);
      assert.equal(Exit.hasFails(interrupted), true);
      const stuck = (yield* harness.sqlB<{
        readonly status: string;
        readonly pendingToken: string | null;
      }>`
        SELECT status, pending_token AS "pendingToken"
        FROM agent_control_worktree_controller_operations
        WHERE command_id = ${input.commandId}
      `)[0]!;
      assert.equal(stuck.status, "pending");
      assert.isNotNull(stuck.pendingToken);

      yield* harness.sqlB`DROP TRIGGER fail_worktree_claim_release`;
      const recovered = yield* harness.controllerA.reserveAndMaterialize(input);
      assert.equal(recovered.status, "ready");
      assert.deepEqual(
        yield* harness.sqlB`
          SELECT status, pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `,
        [{ status: "accepted", pendingToken: null }],
      );
    }),
  );

  it.effect("releases an exact claim after a defect without manufacturing a receipt", () =>
    Effect.gen(function* () {
      const harness = yield* makeIndependentControllerContexts({
        afterCompositeClaim: () => Effect.die("injected composite defect"),
        afterReadyInspection: () => Effect.void,
      });
      const repo = yield* makeRepository();
      const projectId = ProjectId.make("worktree-claim-defect-recovery");
      const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
        Effect.provide(harness.contextA),
      );
      yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const input = {
        commandId: CommandId.make("worktree-claim-defect-recovery-command"),
        projectId,
        taskId: seeded.task.taskId,
      };
      const defect = yield* Effect.exit(harness.controllerA.reserveAndMaterialize(input));
      assert.equal(Exit.hasDies(defect), true);
      assert.deepEqual(
        yield* harness.sqlB`
          SELECT status, pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `,
        [{ status: "pending", pendingToken: null }],
      );
      assert.equal(
        (yield* harness.sqlB<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_command_receipts
          WHERE aggregate_kind = 'worktree-reservation'
        `)[0]!.count,
        0,
      );
      const recovered = yield* harness.retryControllerA.reserveAndMaterialize(input);
      assert.equal(recovered.status, "ready");
    }),
  );

  it.effect("fails closed when an old owner loses the CAS before accept", () =>
    Effect.gen(function* () {
      const beforeAccept = yield* Deferred.make<CommandId>();
      const continueAccept = yield* Deferred.make<void>();
      const harness = yield* makeIndependentControllerContexts({
        afterReadyInspection: () => Effect.void,
        beforeCompositeAccept: (commandId) =>
          Deferred.succeed(beforeAccept, commandId).pipe(
            Effect.andThen(Deferred.await(continueAccept)),
          ),
      });
      const { cwd } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-composite-accept-cas-loss");
      const seeded = yield* seedPrepared(projectId, cwd).pipe(Effect.provide(harness.contextA));
      yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const input = {
        commandId: CommandId.make("worktree-composite-accept-cas-loss-command"),
        projectId,
        taskId: seeded.task.taskId,
      };
      const publications = yield* Stream.runCollect(
        Context.get(harness.contextA, AgentControlWorktreeEngine).streamDomainEvents.pipe(
          Stream.take(3),
        ),
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const oldOwner = yield* harness.controllerA
        .reserveAndMaterialize(input)
        .pipe(Effect.result, Effect.forkChild);
      assert.equal(yield* Deferred.await(beforeAccept), input.commandId);
      const oldClaim = (yield* harness.sqlB<{
        readonly pendingToken: string;
        readonly revision: number;
      }>`
        SELECT pending_token AS "pendingToken", revision
        FROM agent_control_worktree_controller_operations
        WHERE command_id = ${input.commandId}
      `)[0]!;
      const released = yield* harness.sqlB<{ readonly commandId: string }>`
        UPDATE agent_control_worktree_controller_operations
        SET pending_token = NULL, claim_runtime_id = NULL, claim_attempt_id = NULL,
          claim_started_at = NULL, revision = revision + 1
        WHERE command_id = ${input.commandId}
          AND status = 'pending'
          AND pending_token = ${oldClaim.pendingToken}
          AND revision = ${oldClaim.revision}
        RETURNING command_id AS "commandId"
      `;
      assert.equal(released.length, 1);

      const newOwner = yield* harness.controllerB.reserveAndMaterialize(input);
      assert.equal(newOwner.status, "ready");
      yield* Deferred.succeed(continueAccept, undefined);
      const staleResult = yield* Fiber.join(oldOwner);
      assert.equal(staleResult._tag, "Failure");
      if (staleResult._tag === "Failure") {
        assert.equal(staleResult.failure.code, "lease-recovery-required");
      }
      assert.equal((yield* Fiber.join(publications)).length, 3);
      assert.deepEqual(
        yield* harness.sqlB`
          SELECT status, pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `,
        [{ status: "accepted", pendingToken: null }],
      );
      assert.equal(
        (yield* harness.sqlB<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
            AND stream_id = ${newOwner.reservationId}
        `)[0]!.count,
        3,
      );
    }),
  );

  it.effect("a foreign fingerprint cannot reject the active owner's command", () =>
    Effect.gen(function* () {
      const claimed = yield* Deferred.make<CommandId>();
      const continueOwner = yield* Deferred.make<void>();
      const { cwd } = yield* makeRepository();
      const harness = yield* makeIndependentControllerContexts({
        afterCompositeClaim: (commandId) =>
          Deferred.succeed(claimed, commandId).pipe(Effect.andThen(Deferred.await(continueOwner))),
        afterReadyInspection: () => Effect.void,
      });
      const projectId = ProjectId.make("worktree-composite-foreign-reject");
      const seeded = yield* seedPrepared(projectId, cwd).pipe(Effect.provide(harness.contextA));
      const lease = yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const input = {
        commandId: CommandId.make("worktree-composite-foreign-reject-command"),
        projectId,
        taskId: seeded.task.taskId,
      };
      const publication = yield* Stream.runHead(
        Context.get(harness.contextA, AgentControlWorktreeEngine).streamDomainEvents,
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      const owner = yield* harness.controllerA
        .reserveAndMaterialize(input)
        .pipe(Effect.result, Effect.forkChild);
      assert.equal(yield* Deferred.await(claimed), input.commandId);
      const pendingToken =
        (yield* harness.sqlB<{ readonly pendingToken: string | null }>`
          SELECT pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `)[0]?.pendingToken ?? null;
      assert.isNotNull(pendingToken);
      const mismatch = yield* Effect.result(
        harness.controllerB.reserveAndMaterialize({
          ...input,
          projectId: ProjectId.make("worktree-composite-foreign-reject-other"),
        }),
      );
      assert.equal(mismatch._tag, "Failure");
      if (mismatch._tag === "Failure") {
        assert.equal(mismatch.failure.code, "command-identity-mismatch");
      }
      assert.equal(
        (yield* harness.sqlB<{ readonly pendingToken: string | null }>`
            SELECT pending_token AS "pendingToken"
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `)[0]!.pendingToken,
        pendingToken,
      );
      const released = yield* Context.get(
        harness.contextA,
        AgentControlStageRunLeaseEngine,
      ).dispatchController({
        type: "agentControl.stageRunLease.releaseBeforeExecution",
        commandId: CommandId.make("worktree-composite-foreign-reject-release"),
        leaseId: lease.leaseId,
        projectId,
        taskId: seeded.task.taskId,
        stageRunId: seeded.stageRun.stageRunId,
        attemptId: seeded.stageRun.attemptId,
        taskRevision: seeded.stageRun.taskRevision,
        githubIntakeSequence: seeded.stageRun.githubIntakeSequence,
        sourceIdentityFingerprint: seeded.stageRun.sourceIdentityFingerprint,
        fenceToken: lease.fenceToken,
        expectedRevision: lease.revision,
      });
      assert.equal(released._tag, "Accepted");
      yield* Deferred.succeed(continueOwner, undefined);
      const result = yield* Fiber.join(owner);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "lease-not-reserved");
      }
      yield* Effect.yieldNow;
      assert.equal(publication.pollUnsafe(), undefined);
      yield* Fiber.interrupt(publication);
      assert.deepEqual(
        yield* harness.sqlB`
          SELECT status, rejection_code AS "rejectionCode",
            pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${input.commandId}
        `,
        [{ status: "rejected", rejectionCode: "lease-not-reserved", pendingToken: null }],
      );
      assert.equal(
        (yield* harness.sqlB<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
        `)[0]!.count,
        0,
      );
    }),
  );

  it.effect("rechecks lease authority after Git inspection before starting the callback", () =>
    Effect.gen(function* () {
      const inspected = yield* Deferred.make<void>();
      const continuePreflight = yield* Deferred.make<void>();
      const harness = yield* makeIndependentControllerContexts({
        afterReadyInspection: () =>
          Deferred.succeed(inspected, undefined).pipe(
            Effect.andThen(Deferred.await(continuePreflight)),
          ),
      });
      const { cwd } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-second-preflight");
      const seeded = yield* seedPrepared(projectId, cwd).pipe(Effect.provide(harness.contextA));
      const lease = yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const ready = yield* harness.controllerA.reserveAndMaterialize({
        commandId: CommandId.make("worktree-second-preflight-reserve"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const callbackStarted = yield* Ref.make(false);
      const guarded = yield* Effect.result(
        harness.controllerA.useReadyWorktree(
          { projectId, reservationId: ready.reservationId },
          () => Ref.set(callbackStarted, true),
        ),
      ).pipe(Effect.forkChild);
      yield* Deferred.await(inspected);

      const released = yield* Context.get(
        harness.contextA,
        AgentControlStageRunLeaseEngine,
      ).dispatchController({
        type: "agentControl.stageRunLease.releaseBeforeExecution",
        commandId: CommandId.make("worktree-second-preflight-release"),
        leaseId: lease.leaseId,
        projectId,
        taskId: seeded.task.taskId,
        stageRunId: seeded.stageRun.stageRunId,
        attemptId: seeded.stageRun.attemptId,
        taskRevision: seeded.stageRun.taskRevision,
        githubIntakeSequence: seeded.stageRun.githubIntakeSequence,
        sourceIdentityFingerprint: seeded.stageRun.sourceIdentityFingerprint,
        fenceToken: lease.fenceToken,
        expectedRevision: lease.revision,
      });
      assert.equal(released._tag, "Accepted");
      yield* Deferred.succeed(continuePreflight, undefined);
      const result = yield* Fiber.join(guarded);
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.equal(result.failure.code, "lease-not-reserved");
      }
      assert.equal(yield* Ref.get(callbackStarted), false);
      assert.equal(
        (yield* harness.sqlB<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
            AND stream_id = ${ready.reservationId}
            AND event_type = 'agentControl.worktree.needsAttention'
        `)[0]!.count,
        0,
      );
    }),
  );

  it.effect(
    "creates exactly one validated worktree from the pinned base under parallel replay",
    () =>
      Effect.gen(function* () {
        const { cwd, baseCommitSha } = yield* makeRepository();
        const projectId = ProjectId.make("worktree-materialize");
        const { task, stageRun } = yield* seedPrepared(projectId, cwd);
        const lease = yield* reserveLease(stageRun);
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
        assert.equal(
          yield* controller.useReadyWorktree(
            { projectId, reservationId: first.reservationId },
            (state) => Effect.succeed(state.internalWorktreePath),
          ),
          first.internalWorktreePath,
        );
        yield* git(cwd, ["remote", "set-url", "origin", "https://github.com/other/repository.git"]);
        const remoteGuard = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.succeed("must-not-run"),
          ),
        );
        assert.equal(remoteGuard._tag, "Failure");
        if (remoteGuard._tag === "Failure") {
          assert.equal(remoteGuard.failure.code, "repository-identity-mismatch");
        }
        yield* git(cwd, ["remote", "set-url", "origin", "https://github.com/owner/repository.git"]);
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const dirtyPath = `${first.internalWorktreePath}/untracked-after-ready.txt`;
        yield* fs.writeFileString(dirtyPath, "dirty\n");
        const dirtyGuard = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.succeed("must-not-run"),
          ),
        );
        assert.equal(dirtyGuard._tag, "Failure");
        if (dirtyGuard._tag === "Failure") {
          assert.equal(dirtyGuard.failure.code, "state-not-available");
        }
        yield* fs.remove(dirtyPath);
        yield* fs.writeFileString(`${first.internalWorktreePath}/README.md`, "modified\n");
        const modifiedGuard = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.succeed("must-not-run"),
          ),
        );
        assert.equal(modifiedGuard._tag, "Failure");
        yield* fs.writeFileString(`${first.internalWorktreePath}/README.md`, "base\n");
        const stagedPath = `${first.internalWorktreePath}/staged.txt`;
        yield* fs.writeFileString(stagedPath, "staged\n");
        yield* git(first.internalWorktreePath, ["add", "staged.txt"]);
        const stagedGuard = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.succeed("must-not-run"),
          ),
        );
        assert.equal(stagedGuard._tag, "Failure");
        yield* git(first.internalWorktreePath, ["restore", "--staged", "staged.txt"]);
        yield* fs.remove(stagedPath);
        const mergePathOutput = (yield* git(first.internalWorktreePath, [
          "rev-parse",
          "--git-path",
          "MERGE_HEAD",
        ])).stdout.trim();
        const mergePath = path.isAbsolute(mergePathOutput)
          ? mergePathOutput
          : path.resolve(first.internalWorktreePath, mergePathOutput);
        yield* fs.writeFileString(mergePath, `${first.baseCommitSha}\n`);
        const sequencerGuard = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.succeed("must-not-run"),
          ),
        );
        assert.equal(sequencerGuard._tag, "Failure");
        yield* fs.remove(mergePath);
        const callbackFailure = yield* Effect.result(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.fail("callback-failed" as const),
          ),
        );
        assert.equal(callbackFailure._tag, "Failure");
        if (callbackFailure._tag === "Failure") {
          assert.equal(callbackFailure.failure, "callback-failed");
        }
        const activeChildren = yield* Ref.make(0);
        yield* controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
          Effect.gen(function* () {
            yield* Effect.acquireRelease(
              Ref.update(activeChildren, (value) => value + 1),
              () => Ref.update(activeChildren, (value) => value - 1),
            ).pipe(Effect.andThen(Effect.never), Effect.forkScoped);
            yield* Effect.yieldNow;
            assert.equal(yield* Ref.get(activeChildren), 1);
          }),
        );
        assert.equal(yield* Ref.get(activeChildren), 0);
        const callbackEntered = yield* Deferred.make<void>();
        const callbackReleased = yield* Ref.make(false);
        const interruptedCallback = yield* controller
          .useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.acquireRelease(Deferred.succeed(callbackEntered, undefined), () =>
              Ref.set(callbackReleased, true),
            ).pipe(Effect.andThen(Effect.never)),
          )
          .pipe(Effect.forkChild);
        yield* Deferred.await(callbackEntered);
        yield* Fiber.interrupt(interruptedCallback);
        assert.equal(yield* Ref.get(callbackReleased), true);
        assert.equal(
          yield* controller.useReadyWorktree(
            { projectId, reservationId: first.reservationId },
            () => Effect.succeed("lock-reacquired"),
          ),
          "lock-reacquired",
        );
        const finalizerDefect = yield* Effect.exit(
          controller.useReadyWorktree({ projectId, reservationId: first.reservationId }, () =>
            Effect.acquireRelease(Effect.void, () => Effect.die("callback-finalizer-defect")).pipe(
              Effect.as("never-successful"),
            ),
          ),
        );
        assert.equal(Exit.isFailure(finalizerDefect), true);
        assert.equal(
          yield* controller.useReadyWorktree(
            { projectId, reservationId: first.reservationId },
            () => Effect.succeed("lock-after-defect"),
          ),
          "lock-after-defect",
        );
        const sql = yield* SqlClient.SqlClient;
        yield* sql`
          UPDATE agent_control_worktree_reservation_states
          SET ownership_fingerprint = ${"c".repeat(64)},
            state_json = json_set(
              state_json, '$.ownershipFingerprint', ${"c".repeat(64)}
            )
          WHERE reservation_id = ${first.reservationId}
        `;
        const corruptReadyRead = yield* Effect.result(
          (yield* AgentControlWorktree).getReservation({
            projectId,
            reservationId: first.reservationId,
          }),
        );
        assert.equal(corruptReadyRead._tag, "Failure");
        if (corruptReadyRead._tag === "Failure") {
          assert.equal(corruptReadyRead.failure.code, "reservation-projection-corrupt");
        }
        yield* engine.rebuild;
        const worktrees = (yield* git(cwd, ["worktree", "list", "--porcelain"])).stdout;
        assert.equal(worktrees.split(`branch refs/heads/${first.branchName}`).length - 1, 1);
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
        const released = yield* (yield* AgentControlStageRunLeaseEngine).dispatchController({
          type: "agentControl.stageRunLease.releaseBeforeExecution",
          commandId: CommandId.make("worktree-replay-after-release"),
          leaseId: lease.leaseId,
          projectId,
          taskId: task.taskId,
          stageRunId: stageRun.stageRunId,
          attemptId: stageRun.attemptId,
          taskRevision: stageRun.taskRevision,
          githubIntakeSequence: stageRun.githubIntakeSequence,
          sourceIdentityFingerprint: stageRun.sourceIdentityFingerprint,
          fenceToken: lease.fenceToken,
          expectedRevision: lease.revision,
        });
        assert.equal(released._tag, "Accepted");
        yield* sql`
          UPDATE agent_control_project_states SET mode = 'paused'
          WHERE project_id = ${projectId}
        `;
        yield* sql`
          UPDATE projection_projects SET deleted_at = ${at}
          WHERE project_id = ${projectId}
        `;
        yield* TestClock.adjust("3 minutes");
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
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId} AND status = 'accepted'
          `)[0]!.count,
          1,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM agent_control_events
            WHERE command_id = ${input.commandId}
          `)[0]!.count,
          0,
        );
        const wrongIdentity = yield* Effect.result(
          controller.reserveAndMaterialize({
            commandId: input.commandId,
            projectId: ProjectId.make("different-project"),
            taskId: AgentControlTaskId.make("different-task"),
          }),
        );
        assert.equal(wrongIdentity._tag, "Failure");
        if (wrongIdentity._tag === "Failure") {
          assert.equal(wrongIdentity.failure.code, "command-identity-mismatch");
        }
        const wrongType = yield* Effect.result(
          controller.reconcile({
            commandId: input.commandId,
            projectId,
            reservationId: first.reservationId,
          }),
        );
        assert.equal(wrongType._tag, "Failure");
        if (wrongType._tag === "Failure") {
          assert.equal(wrongType.failure.code, "command-identity-mismatch");
        }
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
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        UPDATE agent_control_project_states SET mode = 'paused'
        WHERE project_id = ${projectId}
      `;
      const replay = yield* Effect.result(
        (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("worktree-default-ref-missing-command"),
          projectId,
          taskId: task.taskId,
        }),
      );
      assert.equal(replay._tag, "Failure");
      if (replay._tag === "Failure") {
        assert.equal(replay.failure.code, "default-remote-ref-unavailable");
      }
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_controller_operations
          WHERE command_id = 'worktree-default-ref-missing-command'
            AND status = 'rejected'
        `)[0]!.count,
        1,
      );
    }),
  );

  it.effect("keeps a lock infrastructure failure pending for the same-command retry", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { cwd } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-lock-retry");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      yield* reserveLease(stageRun);
      const commonOutput = (yield* git(cwd, ["rev-parse", "--git-common-dir"])).stdout.trim();
      const commonDir = yield* fs.realPath(
        path.isAbsolute(commonOutput) ? commonOutput : path.resolve(cwd, commonOutput),
      );
      const originalMode = (yield* fs.stat(commonDir)).mode & 0o777;
      const commandId = CommandId.make("worktree-lock-retry-command");
      const controller = yield* AgentControlWorktreeController;
      const blocked = yield* Effect.acquireUseRelease(
        fs.chmod(commonDir, 0o500),
        () =>
          Effect.result(
            controller.reserveAndMaterialize({ commandId, projectId, taskId: task.taskId }),
          ),
        () => fs.chmod(commonDir, originalMode),
      );
      const sql = yield* SqlClient.SqlClient;
      assert.equal(blocked._tag, "Failure");
      if (blocked._tag === "Failure") {
        assert.equal(blocked.failure.code, "repository-lock-unavailable");
      }
      const pendingOperation = (yield* sql<{
        readonly status: string;
        readonly reservationId: string;
        readonly pendingToken: string | null;
      }>`
          SELECT status, worktree_reservation_id AS "reservationId",
            pending_token AS "pendingToken"
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${commandId}
        `)[0]!;
      assert.equal(pendingOperation.status, "pending");
      assert.equal(pendingOperation.pendingToken, null);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE event_type = 'agentControl.worktree.needsAttention'
            AND stream_id = ${pendingOperation.reservationId}
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = ${commandId}
        `)[0]!.count,
        0,
      );
      const ready = yield* controller.reserveAndMaterialize({
        commandId,
        projectId,
        taskId: task.taskId,
      });
      assert.equal(ready.status, "ready");
    }),
  );

  it.effect("refuses an exact pre-existing Git worktree without an ownership marker", () =>
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

      const attention = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-crash-reconcile"),
        projectId,
        reservationId: materializing.reservationId,
      });
      assert.equal(attention.status, "needs-attention");
      assert.equal(attention.attentionCode, "ownership-unproven");
      assert.equal(attention.headCommitSha, null);
    }),
  );

  it.effect("refuses a marker without a persisted git-created claim proof", () =>
    Effect.gen(function* () {
      const { cwd, baseCommitSha } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-crash-after-marker");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      const lease = yield* reserveLease(stageRun);
      const reserved = yield* reserveWorktreeOnly({
        commandId: "worktree-crash-after-marker-reserve",
        task,
        stageRun,
        lease,
        repositoryWorkspace: cwd,
        baseCommitSha,
      });
      const materializing = yield* startMaterializing(
        reserved,
        "worktree-crash-after-marker-start",
      );
      yield* (yield* GitWorkflowService.GitWorkflowService).createWorktree({
        cwd,
        refName: materializing.baseCommitSha,
        newRefName: materializing.branchName,
        baseRefName: materializing.baseRef,
        path: materializing.internalWorktreePath,
      });
      const gitDir = yield* git(materializing.internalWorktreePath, ["rev-parse", "--git-dir"]);
      const markerPath = yield* ownershipMarkerPath(
        materializing.internalWorktreePath,
        gitDir.stdout,
      );
      yield* Effect.scoped(
        writeAgentControlWorktreeOwnershipMarker(
          markerPath,
          expectedAgentControlWorktreeOwnershipMarker(materializing),
        ),
      );

      const ready = yield* (yield* AgentControlWorktreeController).reconcile({
        commandId: CommandId.make("worktree-crash-after-marker-reconcile"),
        projectId,
        reservationId: materializing.reservationId,
      });
      assert.equal(ready.status, "needs-attention");
      assert.equal(ready.attentionCode, "ownership-unproven");

      const overwrite = yield* Effect.result(
        Effect.scoped(
          writeAgentControlWorktreeOwnershipMarker(markerPath, {
            ...expectedAgentControlWorktreeOwnershipMarker(materializing),
            fenceToken: 2,
          }),
        ),
      );
      assert.equal(overwrite._tag, "Failure");
    }),
  );

  it.effect("resumes marker publication from a persisted git-created CAS proof", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const sql = yield* SqlClient.SqlClient;
      const { cwd, baseCommitSha } = yield* makeRepository();
      const projectId = ProjectId.make("worktree-git-created-retry");
      const { task, stageRun } = yield* seedPrepared(projectId, cwd);
      const lease = yield* reserveLease(stageRun);
      const reserved = yield* reserveWorktreeOnly({
        commandId: "worktree-git-created-retry-reserve",
        task,
        stageRun,
        lease,
        repositoryWorkspace: cwd,
        baseCommitSha,
      });
      const materializing = yield* startMaterializing(reserved, "worktree-git-created-retry-start");
      yield* (yield* GitWorkflowService.GitWorkflowService).createWorktree({
        cwd,
        refName: materializing.baseCommitSha,
        newRefName: materializing.branchName,
        baseRefName: materializing.baseRef,
        path: materializing.internalWorktreePath,
      });
      const targetInfo = yield* Effect.promise(() =>
        NodeFSP.lstat(materializing.internalWorktreePath),
      );
      const targetInode = targetInfo.ino;
      const targetParent = path.dirname(materializing.internalWorktreePath);
      const targetParentInfo = yield* Effect.promise(() => NodeFSP.lstat(targetParent));
      const targetParentInode = targetParentInfo.ino;
      const gitDirOutput = (yield* git(materializing.internalWorktreePath, [
        "rev-parse",
        "--git-dir",
      ])).stdout.trim();
      const gitDir = yield* fs.realPath(
        path.isAbsolute(gitDirOutput)
          ? gitDirOutput
          : path.resolve(materializing.internalWorktreePath, gitDirOutput),
      );
      const commandId = CommandId.make("worktree-git-created-retry-reconcile");
      const fingerprint = sha256FramedHex([
        "agent-control-worktree-controller-operation-v1",
        commandId,
        "reconcile",
        projectId,
        "",
        materializing.reservationId,
      ]);
      yield* sql`
        INSERT INTO agent_control_worktree_controller_operations (
          command_id, command_type, input_fingerprint, project_id, task_id,
          reservation_id, worktree_reservation_id, status,
          target_generation_id,
          materialization_phase, git_created_device, git_created_inode,
          git_created_git_dir, created_at, updated_at, revision
        ) VALUES (
          ${commandId}, 'reconcile', ${fingerprint}, ${projectId}, NULL,
          ${materializing.reservationId}, ${materializing.reservationId}, 'pending',
          ${materializing.targetGenerationId},
          'git-created', ${targetInfo.dev}, ${targetInode}, ${gitDir},
          ${at}, ${at}, 4
        )
      `;
      yield* sql`DROP TRIGGER agent_control_worktree_target_claim_authority_insert`;
      yield* sql`
        INSERT INTO agent_control_worktree_target_claims (
          command_id, input_fingerprint, pending_token, claim_attempt_id,
          target_generation, reservation_id, target_path, parent_path,
          parent_device, parent_inode, target_device, target_inode,
          target_uid, target_mode, phase, created_at, updated_at
        ) VALUES (
          ${commandId}, ${fingerprint}, 'released-pending-token',
          'released-claim-attempt', ${materializing.targetGenerationId},
          ${materializing.reservationId}, ${materializing.internalWorktreePath},
          ${targetParent}, ${targetParentInfo.dev}, ${targetParentInode},
          ${targetInfo.dev}, ${targetInode}, ${targetInfo.uid}, ${targetInfo.mode},
          'acquired', ${at}, ${at}
        )
      `;
      const controller = yield* AgentControlWorktreeController;
      const markerPath = yield* ownershipMarkerPath(materializing.internalWorktreePath, gitDir);
      yield* fs.writeFileString(markerPath, "{invalid-marker");
      yield* fs.chmod(markerPath, 0o600);
      const incomplete = yield* Effect.result(
        controller.reconcile({
          commandId,
          projectId,
          reservationId: materializing.reservationId,
        }),
      );
      assert.equal(incomplete._tag, "Failure");
      if (incomplete._tag === "Failure") {
        assert.equal(incomplete.failure.code, "repository-unavailable");
      }
      assert.deepEqual(
        yield* sql`
          SELECT status, pending_token AS "pendingToken",
            materialization_phase AS phase
          FROM agent_control_worktree_controller_operations
          WHERE command_id = ${commandId}
        `,
        [{ status: "pending", pendingToken: null, phase: "git-created" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
            AND stream_id = ${materializing.reservationId}
        `)[0]!.count,
        2,
      );
      yield* fs.remove(markerPath);
      const ready = yield* controller.reconcile({
        commandId,
        projectId,
        reservationId: materializing.reservationId,
      });
      assert.equal(ready.status, "ready");
      assert.isNotNull(ready.ownershipFingerprint);
      const operation = (yield* sql<{
        readonly status: string;
        readonly phase: string;
        readonly revision: number;
      }>`
        SELECT status, materialization_phase AS phase, revision
        FROM agent_control_worktree_controller_operations
        WHERE command_id = ${commandId}
      `)[0]!;
      assert.equal(operation.status, "accepted");
      assert.equal(operation.phase, "terminal");
      assert.isAbove(operation.revision, 4);
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
        ownershipFingerprint: "b".repeat(64),
        gitCreatedDevice: 1,
        gitCreatedInode: 1,
        gitCreatedGitDir: "/tmp/worktree-git-dir",
        markedOwnershipFingerprint: "b".repeat(64),
        verifiedAt: at,
        targetClaimCloseEvidence: {
          pendingToken: "stale-fence-pending",
          claimAttemptId: "stale-fence-attempt",
          expectedRevision: 1,
          resultingRevision: 2,
          targetGeneration: materializing.targetGenerationId,
          compositeCommandId: CommandId.make("stale-fence-composite"),
          compositeOperation: "reconcile" as const,
          compositeFingerprint: "f".repeat(64),
          reservationId: materializing.reservationId,
          phase: "materialized" as const,
        },
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
