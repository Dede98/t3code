import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import {
  AgentControlWorktreeCommand,
  AgentControlWorktreeReservationState,
  AgentControlControlledThreadReservationRpcError,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProjectId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlStageRunLeaseState,
  type AgentControlStageRunState,
  type AgentControlTaskState,
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
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerConfig } from "../../../config.ts";
import * as GitManager from "../../../git/GitManager.ts";
import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import * as RepositoryIdentityResolver from "../../../project/RepositoryIdentityResolver.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { NodeSqliteTransactionHooks } from "../../../persistence/Services/NodeSqliteTransactionHooks.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import { AgentControlRuntimeLayerLive } from "../../runtimeLayer.ts";
import { AgentControlControlledThreadReservationLayerLive } from "../../runtimeLayer.ts";
import { AgentControlControlledThreadActivationLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadActivation.ts";
import { AgentControlControlledThreadActivation } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import {
  AgentControlControlledThreadActivationHooks,
  AgentControlControlledThreadActivationHooksNoop,
  type AgentControlControlledThreadActivationHooksShape,
} from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivationHooks.ts";
import {
  AgentControlPolicyService,
  type AgentControlPolicyServiceShape,
} from "../../AgentControlPolicyService.ts";
import { AgentControlControlledThreadMaterializationCoordinatorLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadMaterializationCoordinator.ts";
import { AgentControlControlledThreadMaterializationCoordinator } from "../../controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinator.ts";
import {
  AgentControlControlledThreadMaterializationCoordinatorHooks,
  AgentControlControlledThreadMaterializationCoordinatorHooksNoop,
  type AgentControlControlledThreadMaterializationCoordinatorHooksShape,
} from "../../controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinatorHooks.ts";
import { AgentControlControlledThreadReservation } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservation.ts";
import {
  AgentControlControlledThreadReservationEngine,
  type AgentControlControlledThreadReservationEngineShape,
} from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine.ts";
import {
  AgentControlControlledThreadReservationTransactionHooks,
  type AgentControlControlledThreadReservationTransactionHooksShape,
} from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationTransactionHooks.ts";
import { layer as AgentControlControlledThreadReservationEngineLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadReservationEngine.ts";
import { layer as AgentControlControlledThreadReservationLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadReservation.ts";
import {
  deriveAgentControlControlledThreadActivationCommandId,
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
} from "../../controlledThreadReservation/identity.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlStageRun } from "../../stageRun/Services/AgentControlStageRun.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { OrchestrationLayerLive } from "../../../orchestration/runtimeLayer.ts";
import { ORCHESTRATION_PROJECTOR_NAMES } from "../../../orchestration/Layers/ProjectionPipeline.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../../../orchestration/Services/OrchestrationEngine.ts";
import { OrchestrationProjectionPipeline } from "../../../orchestration/Services/ProjectionPipeline.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
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
import {
  AgentControlWorktreeController,
  type AgentControlWorktreeControllerShape,
} from "../Services/AgentControlWorktreeController.ts";
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
const controlledThreadReservationLayer = AgentControlControlledThreadReservationLayerLive.pipe(
  Layer.provideMerge(controllerLayer),
);
const layer = it.layer(Layer.merge(controllerLayer, controlledThreadReservationLayer));
const coordinatorPolicyLayer = Layer.mock(AgentControlPolicyService)({
  preflightRuntime: () =>
    Effect.succeed({
      ok: true,
      staticPreflight: {
        ok: true,
        roles: [
          {
            role: "planner",
            accessMode: "restricted",
            strict: true,
            validCandidates: [
              {
                selection: {
                  instanceId: ProviderInstanceId.make("coordinator-test-provider"),
                  model: "gpt-5.6",
                },
                source: "role-route",
                driverKind: ProviderDriverKind.make("codex"),
              },
            ],
          },
        ],
      },
      roles: [
        {
          role: "planner",
          accessMode: "restricted",
          strict: true,
          candidates: [
            {
              candidateIndex: 0,
              source: "role-route",
              providerInstanceId: ProviderInstanceId.make("coordinator-test-provider"),
              model: "gpt-5.6",
              driverKind: ProviderDriverKind.make("codex"),
              providerStatus: "ready",
              authStatus: "authenticated",
              checkedAt: at,
              runtimeReady: true,
              errorCode: null,
            },
          ],
          selectedCandidateIndex: 0,
          errorCode: null,
        },
      ],
    }),
});
const coordinatorOrchestrationLayer = OrchestrationLayerLive.pipe(
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(RepositoryIdentityResolver.layer),
  Layer.provideMerge(configLayer),
  Layer.provideMerge(NodeServices.layer),
);
const coordinatorDependencies = Layer.mergeAll(
  controllerLayer,
  controlledThreadReservationLayer,
  coordinatorOrchestrationLayer,
  coordinatorPolicyLayer,
);
const coordinatorServiceLayer = AgentControlControlledThreadMaterializationCoordinatorLive.pipe(
  Layer.provide(coordinatorDependencies),
  Layer.provide(AgentControlControlledThreadMaterializationCoordinatorHooksNoop),
);
const coordinatorLayer = it.layer(Layer.merge(coordinatorDependencies, coordinatorServiceLayer));
const activationServiceLayer = AgentControlControlledThreadActivationLive.pipe(
  Layer.provideMerge(controlledThreadReservationLayer),
  Layer.provideMerge(coordinatorServiceLayer),
  Layer.provide(AgentControlControlledThreadActivationHooksNoop),
);
const activationLayer = it.layer(
  Layer.mergeAll(coordinatorDependencies, coordinatorServiceLayer, activationServiceLayer),
);
const coordinatorNoopHooks: AgentControlControlledThreadMaterializationCoordinatorHooksShape = {
  afterReceiptFirst: () => Effect.void,
  afterAuthoritativeResolution: () => Effect.void,
  beforeTransactionAdmission: () => Effect.void,
  afterMaterializingProjection: () => Effect.void,
  afterOrchestrationMaterialization: () => Effect.void,
  afterBoundProjection: () => Effect.void,
  afterCoordinatorEvidence: () => Effect.void,
  beforeAcceptedMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  beforeReservationFinalization: () => Effect.void,
  afterReservationFinalization: () => Effect.void,
  beforeOrchestrationFinalization: () => Effect.void,
  afterOrchestrationFinalization: () => Effect.void,
  afterPublication: () => Effect.void,
};
const buildCoordinator = Effect.fn("buildControlledThreadMaterializationCoordinator")(function* (
  options: {
    readonly hooks?: AgentControlControlledThreadMaterializationCoordinatorHooksShape;
    readonly reservationEngine?: AgentControlControlledThreadReservationEngineShape;
    readonly orchestrationEngine?: OrchestrationEngineShape;
    readonly policy?: AgentControlPolicyServiceShape;
    readonly worktreeController?: AgentControlWorktreeControllerShape;
  } = {},
) {
  const scope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  let build = Layer.buildWithScope(
    Layer.fresh(AgentControlControlledThreadMaterializationCoordinatorLive),
    scope,
  ).pipe(
    Effect.provideService(
      AgentControlControlledThreadMaterializationCoordinatorHooks,
      options.hooks ?? coordinatorNoopHooks,
    ),
  );
  if (options.reservationEngine !== undefined) {
    build = build.pipe(
      Effect.provideService(
        AgentControlControlledThreadReservationEngine,
        options.reservationEngine,
      ),
    );
  }
  if (options.orchestrationEngine !== undefined) {
    build = build.pipe(
      Effect.provideService(OrchestrationEngineService, options.orchestrationEngine),
    );
  }
  if (options.policy !== undefined) {
    build = build.pipe(Effect.provideService(AgentControlPolicyService, options.policy));
  }
  if (options.worktreeController !== undefined) {
    build = build.pipe(
      Effect.provideService(AgentControlWorktreeController, options.worktreeController),
    );
  }
  const context = yield* build;
  return Context.get(context, AgentControlControlledThreadMaterializationCoordinator);
});
const activationNoopHooks: AgentControlControlledThreadActivationHooksShape = {
  afterPrepareAcceptedBeforeMaterialize: () => Effect.void,
  afterMaterializationAcceptedBeforeReturn: () => Effect.void,
};
const buildActivation = Effect.fn("buildControlledThreadActivation")(function* (
  options: {
    readonly hooks?: AgentControlControlledThreadActivationHooksShape;
    readonly reservation?: AgentControlControlledThreadReservation["Service"];
    readonly coordinator?: AgentControlControlledThreadMaterializationCoordinator["Service"];
  } = {},
) {
  const scope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const reservation = options.reservation ?? (yield* AgentControlControlledThreadReservation);
  const coordinator =
    options.coordinator ?? (yield* AgentControlControlledThreadMaterializationCoordinator);
  const context = yield* Layer.buildWithScope(
    Layer.fresh(AgentControlControlledThreadActivationLive),
    scope,
  ).pipe(
    Effect.provideService(
      AgentControlControlledThreadActivationHooks,
      options.hooks ?? activationNoopHooks,
    ),
    Effect.provideService(AgentControlControlledThreadReservation, reservation),
    Effect.provideService(AgentControlControlledThreadMaterializationCoordinator, coordinator),
  );
  return Context.get(context, AgentControlControlledThreadActivation);
});
const at = "2026-07-24T10:00:00.000Z";
const encodeWorktreeCommand = Schema.encodeSync(Schema.fromJsonString(AgentControlWorktreeCommand));
const decodeReservationState = Schema.decodeUnknownSync(
  Schema.fromJsonString(AgentControlWorktreeReservationState),
);
const decodeUnknownJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
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
    buildControlledThreadEngineB = false,
    controlledThreadHooksB?: AgentControlControlledThreadReservationTransactionHooksShape,
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
    const buildControlledThreadEngineBLayer = Layer.buildWithScope(
      Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
        Layer.provide(Layer.succeedContext(controllerDependenciesB)),
      ),
      scopeB,
    );
    const controlledThreadEngineB = buildControlledThreadEngineB
      ? Context.get(
          yield* controlledThreadHooksB === undefined
            ? buildControlledThreadEngineBLayer
            : buildControlledThreadEngineBLayer.pipe(
                Effect.provideService(
                  AgentControlControlledThreadReservationTransactionHooks,
                  controlledThreadHooksB,
                ),
              ),
          AgentControlControlledThreadReservationEngine,
        )
      : null;
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
      scopeA,
      scopeB,
      sqlA,
      sqlB,
      contextA,
      contextB,
      controllerDependenciesB,
      controllerA: Context.get(contextA, AgentControlWorktreeController),
      retryControllerA: Context.get(retryControllerContextA, AgentControlWorktreeController),
      controllerB: Context.get(controllerContextB, AgentControlWorktreeController),
      retryControllerB: Context.get(retryControllerContextB, AgentControlWorktreeController),
      engineB: Context.get(controllerDependenciesB, AgentControlWorktreeEngine),
      controlledThreadEngineB,
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
  const taskEngine = yield* AgentControlTaskEngine;
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
  const task = {
    ...taskFrom(projectId, source),
    taskId: yield* deriveAgentControlTaskId({
      projectId,
      repositoryNodeId: source.repositoryNodeId,
      issueNodeId: source.issueNodeId,
    }),
  };
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
  const createdTask = yield* taskEngine.dispatchObservedController({
    type: "agentControl.task.createFromGithubIssue",
    commandId: CommandId.make(`task-create-${projectId}`),
    taskId: task.taskId,
    projectId,
    expectedRevision: 0,
    sourcePrecondition: {
      schemaVersion: 1,
      projectId,
      githubIntakeSequence: 1,
      githubProjectionRevision: 1,
      githubConfigRevision: 1,
      repositoryNodeId: repository.repositoryNodeId,
      pollStatus: "success",
      expectedIssueCount: 1,
    },
    source: task.source,
    sourceGate: task.sourceGate,
    sourceUpdatedAt: task.sourceUpdatedAt,
    githubIntakeSequence: task.githubIntakeSequence,
    sourceSnapshot: task.sourceSnapshot,
  });
  const authoritativeTask = createdTask.state;
  yield* sql`
    INSERT INTO agent_control_task_reconcile_states (
      project_id, target_sequence, last_completed_sequence,
      revision, status, updated_at
    ) VALUES (${projectId}, 1, 1, 1, 'completed', ${at})
  `;
  const prepared = yield* stageRuns.prepareInitial({
    commandId: CommandId.make(`prepare-${projectId}`),
    projectId,
    taskId: authoritativeTask.taskId,
  });
  return { task: authoritativeTask, stageRun: prepared.state };
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

const seedCoordinatorReservation = Effect.fn("seedCoordinatorReservation")(function* (
  suffix: string,
) {
  const repo = yield* makeRepository();
  const projectId = ProjectId.make(`controlled-thread-coordinator-${suffix}`);
  const seeded = yield* seedPrepared(projectId, repo.cwd);
  const lease = yield* reserveLease(seeded.stageRun);
  const ready = yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
    commandId: CommandId.make(`coordinator-ready-${suffix}`),
    projectId,
    taskId: seeded.task.taskId,
  });
  assert.equal(ready.status, "ready");
  const reservation = yield* (yield* AgentControlControlledThreadReservation).prepareInitial({
    commandId: CommandId.make(`coordinator-prepare-${suffix}`),
    projectId,
    taskId: seeded.task.taskId,
  });
  const coordinatorCommandId = yield* deriveAgentControlControlledThreadActivationCommandId(
    CommandId.make(`coordinator-prepare-${suffix}`),
    reservation.reservation.controlledThreadReservationId,
  );
  return {
    repo,
    projectId,
    task: seeded.task,
    stageRun: seeded.stageRun,
    lease,
    ready,
    reservation: reservation.reservation,
    command: {
      commandId: coordinatorCommandId,
      projectId,
      controlledThreadReservationId: reservation.reservation.controlledThreadReservationId,
    },
  } as const;
});

const coordinatorPersistenceCounts = Effect.fn("coordinatorPersistenceCounts")(function* (
  controlledThreadReservationId: string,
  threadId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    readonly reservationEvents: number;
    readonly reservationProjections: number;
    readonly orchestrationEvents: number;
    readonly threadProjections: number;
    readonly orchestrationIntents: number;
    readonly orchestrationReceipts: number;
    readonly orchestrationMarkers: number;
    readonly coordinatorIntents: number;
    readonly coordinatorReceipts: number;
    readonly coordinatorMarkers: number;
  }>`
    SELECT
      (SELECT count(*) FROM agent_control_events
       WHERE aggregate_kind = 'controlled-thread-reservation'
         AND stream_id = ${controlledThreadReservationId}) AS reservationEvents,
      (SELECT count(*) FROM agent_control_controlled_thread_reservation_states
       WHERE controlled_thread_reservation_id =
         ${controlledThreadReservationId}) AS reservationProjections,
      (SELECT count(*) FROM orchestration_events
       WHERE stream_id = ${threadId}) AS orchestrationEvents,
      (SELECT count(*) FROM projection_threads
       WHERE thread_id = ${threadId}) AS threadProjections,
      (SELECT count(*) FROM orchestration_agent_control_thread_materialization_intents
       WHERE controlled_thread_reservation_id =
         ${controlledThreadReservationId}) AS orchestrationIntents,
      (SELECT count(*) FROM orchestration_command_receipts
       WHERE aggregate_id = ${threadId}) AS orchestrationReceipts,
      (SELECT count(*) FROM orchestration_agent_control_thread_materialization_receipts
       WHERE thread_id = ${threadId}) AS orchestrationMarkers,
      (SELECT count(*) FROM agent_control_controlled_thread_materialization_intents
       WHERE controlled_thread_reservation_id =
         ${controlledThreadReservationId}) AS coordinatorIntents,
      (SELECT count(*) FROM agent_control_controlled_thread_materialization_receipts
       WHERE controlled_thread_reservation_id =
         ${controlledThreadReservationId}) AS coordinatorReceipts,
      (SELECT count(*) FROM agent_control_controlled_thread_materialization_accepted
       WHERE controlled_thread_reservation_id =
         ${controlledThreadReservationId}) AS coordinatorMarkers
  `)[0]!;
});

activationLayer("Controlled thread activation facade", (it) => {
  it.effect(
    "returns historical prepared while synchronously committing bound exactly once",
    () =>
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-activation-happy-path");
        const seeded = yield* seedPrepared(projectId, repo.cwd);
        yield* reserveLease(seeded.stageRun);
        yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("activation-ready-worktree"),
          projectId,
          taskId: seeded.task.taskId,
        });
        const input = {
          commandId: CommandId.make("activation-prepare"),
          projectId,
          taskId: seeded.task.taskId,
        } as const;
        const sql = yield* SqlClient.SqlClient;
        const githubWriteCounters = sql`
          SELECT
            (SELECT count(*) FROM agent_control_events
             WHERE aggregate_kind = 'github-intake') AS events,
            (SELECT count(*) FROM agent_control_github_intake_states) AS states,
            (SELECT count(*) FROM agent_control_github_issues) AS issues,
            (SELECT count(*) FROM agent_control_github_timeline_events) AS timelineEvents,
            (SELECT count(*) FROM agent_control_github_scheduler_states) AS schedulerStates
        `;
        const githubWriteCountersBefore = yield* githubWriteCounters;
        const coordinator = yield* AgentControlControlledThreadMaterializationCoordinator;
        const coordinatorCalls = yield* Ref.make(0);
        const activation = yield* buildActivation({
          coordinator: AgentControlControlledThreadMaterializationCoordinator.of({
            ...coordinator,
            materializeInitial: (command) =>
              Ref.update(coordinatorCalls, (count) => count + 1).pipe(
                Effect.andThen(coordinator.materializeInitial(command)),
              ),
          }),
        });

        const result = yield* activation.activateInitial(input);
        assert.equal(result.reservation.status, "prepared");
        assert.equal(result.reservation.revision, 1);
        assert.equal(result.eventCreated, true);
        assert.equal(yield* Ref.get(coordinatorCalls), 1);
        const activationCommandId = yield* deriveAgentControlControlledThreadActivationCommandId(
          input.commandId,
          result.reservation.controlledThreadReservationId,
        );
        const reservations = yield* AgentControlControlledThreadReservation;
        const get = yield* reservations.get({
          projectId,
          controlledThreadReservationId: result.reservation.controlledThreadReservationId,
        });
        const list = yield* reservations.list({ projectId });
        assert.equal(get.status, "bound");
        assert.equal(get.revision, 3);
        assert.lengthOf(list.reservations, 1);
        assert.equal(list.reservations[0]!.status, "bound");
        assert.equal(list.reservations[0]!.revision, 3);

        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM agent_control_command_receipts
               WHERE command_id = ${input.commandId}) AS prepareReceipts,
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind = 'controlled-thread-reservation'
                 AND stream_id =
                   ${result.reservation.controlledThreadReservationId}) AS reservationEvents,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${result.reservation.threadId}) AS orchestrationEvents,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_intents
               WHERE coordinator_command_id = ${activationCommandId}) AS coordinatorIntents,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_receipts
               WHERE coordinator_command_id = ${activationCommandId}) AS coordinatorReceipts,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_accepted
               WHERE coordinator_command_id = ${activationCommandId}) AS coordinatorMarkers,
              (SELECT count(*) FROM provider_session_runtime) AS providerSessions,
              (SELECT count(*) FROM orchestration_events
               WHERE event_type LIKE 'provider.%') AS providerCommands,
              (SELECT count(*) FROM projection_turns) AS turns,
              (SELECT count(*) FROM projection_thread_messages) AS messages,
              (SELECT count(*) FROM orchestration_events
               WHERE lower(event_type) LIKE '%prompt%') AS prompts,
              (SELECT count(*) FROM orchestration_events
               WHERE event_type LIKE 'terminal.%'
                  OR event_type LIKE 'process.%') AS terminalOrProcessStarts,
              (SELECT count(*) FROM agent_control_events
               WHERE event_type LIKE 'agentControl.taskExecution.%')
                AS taskExecutionEvents,
              (SELECT count(*) FROM agent_control_github_scheduler_states)
                AS schedulerOrReactorActivity
          `,
          [
            {
              prepareReceipts: 1,
              reservationEvents: 3,
              orchestrationEvents: 2,
              coordinatorIntents: 1,
              coordinatorReceipts: 1,
              coordinatorMarkers: 1,
              providerSessions: 0,
              providerCommands: 0,
              turns: 0,
              messages: 0,
              prompts: 0,
              terminalOrProcessStarts: 0,
              taskExecutionEvents: 0,
              schedulerOrReactorActivity: 0,
            },
          ],
        );
        assert.deepStrictEqual(
          yield* githubWriteCounters,
          githubWriteCountersBefore,
          "activation must not write GitHub-observation state",
        );
        const committedCounts = yield* coordinatorPersistenceCounts(
          result.reservation.controlledThreadReservationId,
          result.reservation.threadId,
        );
        assert.deepStrictEqual(committedCounts, {
          reservationEvents: 3,
          reservationProjections: 1,
          orchestrationEvents: 2,
          threadProjections: 1,
          orchestrationIntents: 1,
          orchestrationReceipts: 1,
          orchestrationMarkers: 1,
          coordinatorIntents: 1,
          coordinatorReceipts: 1,
          coordinatorMarkers: 1,
        });

        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);
        const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
        const reservationSubscriber = yield* reservationEngine.streamDomainEvents.pipe(
          Stream.runForEach(() => Ref.update(reservationPublications, (count) => count + 1)),
          Effect.forkChild,
        );
        const orchestrationSubscriber =
          yield* (yield* OrchestrationEngineService).streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(orchestrationPublications, (count) => count + 1)),
            Effect.forkChild,
          );
        yield* Effect.yieldNow;
        const replay = yield* activation.activateInitial(input);
        assert.deepStrictEqual(replay, result);
        assert.equal(yield* Ref.get(coordinatorCalls), 2);
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(reservationPublications), 0);
        assert.equal(yield* Ref.get(orchestrationPublications), 0);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            result.reservation.controlledThreadReservationId,
            result.reservation.threadId,
          ),
          committedCounts,
        );

        yield* (yield* OrchestrationEngineService).dispatch({
          type: "thread.meta.update",
          commandId: CommandId.make("activation-later-thread-event"),
          threadId: result.reservation.threadId,
          title: "Legitimate later controlled thread event",
        });
        yield* reservationEngine.rebuild;
        assert.deepStrictEqual(yield* reservations.prepareInitial(input), result);
        assert.deepStrictEqual(yield* activation.activateInitial(input), result);
        assert.equal(yield* Ref.get(coordinatorCalls), 3);
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(reservationPublications), 0);
        assert.equal(yield* Ref.get(orchestrationPublications), 1);
        const countsAfterLaterThreadEvent = {
          ...committedCounts,
          orchestrationEvents: 3,
          orchestrationReceipts: 2,
        };
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            result.reservation.controlledThreadReservationId,
            result.reservation.threadId,
          ),
          countsAfterLaterThreadEvent,
        );

        const conflicting = yield* Effect.result(
          activation.activateInitial({
            ...input,
            commandId: CommandId.make("activation-prepare-conflict"),
          }),
        );
        assert.equal(conflicting._tag, "Failure");
        if (conflicting._tag === "Failure") {
          assert.equal(conflicting.failure.code, "controlled-thread-reservation-identity-conflict");
        }
        assert.equal(yield* Ref.get(coordinatorCalls), 3);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            result.reservation.controlledThreadReservationId,
            result.reservation.threadId,
          ),
          countsAfterLaterThreadEvent,
        );
        yield* Fiber.interrupt(orchestrationSubscriber);
        yield* Fiber.interrupt(reservationSubscriber);
      }),
    30_000,
  );

  it.effect(
    "recovers fresh facades after defects and interrupts at both activation hooks",
    () =>
      Effect.gen(function* () {
        const seedActivation = Effect.fn("seedActivationLifecycle")(function* (suffix: string) {
          const repo = yield* makeRepository();
          const projectId = ProjectId.make(`controlled-thread-activation-lifecycle-${suffix}`);
          const seeded = yield* seedPrepared(projectId, repo.cwd);
          yield* reserveLease(seeded.stageRun);
          yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
            commandId: CommandId.make(`activation-lifecycle-ready-${suffix}`),
            projectId,
            taskId: seeded.task.taskId,
          });
          return {
            input: {
              commandId: CommandId.make(`activation-lifecycle-prepare-${suffix}`),
              projectId,
              taskId: seeded.task.taskId,
            },
          } as const;
        });
        const sql = yield* SqlClient.SqlClient;

        const beforeDefect = yield* seedActivation("before-defect");
        const defectBeforeFacade = yield* buildActivation({
          hooks: {
            ...activationNoopHooks,
            afterPrepareAcceptedBeforeMaterialize: () =>
              Effect.die(new Error("activation-after-prepare-defect")),
          },
        });
        const defectBefore = yield* Effect.exit(
          defectBeforeFacade.activateInitial(beforeDefect.input),
        );
        assert.equal(defectBefore._tag, "Failure");
        if (Exit.isFailure(defectBefore)) {
          assert.include(Cause.pretty(defectBefore.cause), "activation-after-prepare-defect");
        }
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind = 'controlled-thread-reservation'
                 AND json_extract(payload_json, '$.projectId') =
                   ${beforeDefect.input.projectId}) AS reservationEvents,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = (
                 SELECT thread_id
                 FROM agent_control_controlled_thread_reservation_states
                 WHERE project_id = ${beforeDefect.input.projectId}
               )) AS orchestrationEvents
          `,
          [{ reservationEvents: 1, orchestrationEvents: 0 }],
        );
        const restartedAfterDefect = yield* buildActivation();
        const recoveredBeforeDefect = yield* restartedAfterDefect.activateInitial(
          beforeDefect.input,
        );
        assert.equal(recoveredBeforeDefect.reservation.status, "prepared");
        assert.equal(
          (yield* (yield* AgentControlControlledThreadReservation).get({
            projectId: beforeDefect.input.projectId,
            controlledThreadReservationId:
              recoveredBeforeDefect.reservation.controlledThreadReservationId,
          })).status,
          "bound",
        );

        const beforeInterrupt = yield* seedActivation("before-interrupt");
        const beforeReached = yield* Deferred.make<void>();
        const holdBefore = yield* Deferred.make<void>();
        const interruptBeforeFacade = yield* buildActivation({
          hooks: {
            ...activationNoopHooks,
            afterPrepareAcceptedBeforeMaterialize: () =>
              Deferred.succeed(beforeReached, undefined).pipe(
                Effect.andThen(Deferred.await(holdBefore)),
              ),
          },
        });
        const beforeFiber = yield* interruptBeforeFacade
          .activateInitial(beforeInterrupt.input)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(beforeReached);
        yield* Fiber.interrupt(beforeFiber);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(beforeFiber)), true);
        const recoveredBeforeInterrupt = yield* (yield* buildActivation()).activateInitial(
          beforeInterrupt.input,
        );
        assert.equal(recoveredBeforeInterrupt.reservation.status, "prepared");

        const afterDefect = yield* seedActivation("after-defect");
        const defectAfterFacade = yield* buildActivation({
          hooks: {
            ...activationNoopHooks,
            afterMaterializationAcceptedBeforeReturn: () =>
              Effect.die(new Error("activation-after-materialization-defect")),
          },
        });
        const defectAfter = yield* Effect.exit(
          defectAfterFacade.activateInitial(afterDefect.input),
        );
        assert.equal(defectAfter._tag, "Failure");
        if (Exit.isFailure(defectAfter)) {
          assert.include(
            Cause.pretty(defectAfter.cause),
            "activation-after-materialization-defect",
          );
        }
        const afterDefectReservationId =
          (yield* (yield* AgentControlControlledThreadReservation).list({
            projectId: afterDefect.input.projectId,
          })).reservations[0]!.controlledThreadReservationId;
        const afterDefectThreadId = (yield* (yield* AgentControlControlledThreadReservation).get({
          projectId: afterDefect.input.projectId,
          controlledThreadReservationId: afterDefectReservationId,
        })).threadId;
        const afterDefectCounts = yield* coordinatorPersistenceCounts(
          afterDefectReservationId,
          afterDefectThreadId,
        );
        const recoveredAfterDefect = yield* (yield* buildActivation()).activateInitial(
          afterDefect.input,
        );
        assert.equal(recoveredAfterDefect.reservation.status, "prepared");
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(afterDefectReservationId, afterDefectThreadId),
          afterDefectCounts,
        );

        const afterInterrupt = yield* seedActivation("after-interrupt");
        const afterReached = yield* Deferred.make<void>();
        const holdAfter = yield* Deferred.make<void>();
        const interruptAfterFacade = yield* buildActivation({
          hooks: {
            ...activationNoopHooks,
            afterMaterializationAcceptedBeforeReturn: () =>
              Deferred.succeed(afterReached, undefined).pipe(
                Effect.andThen(Deferred.await(holdAfter)),
              ),
          },
        });
        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);
        const reservationSubscriber =
          yield* (yield* AgentControlControlledThreadReservationEngine).streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(reservationPublications, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          );
        const orchestrationSubscriber =
          yield* (yield* OrchestrationEngineService).streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(orchestrationPublications, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          );
        const afterFiber = yield* interruptAfterFacade
          .activateInitial(afterInterrupt.input)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(afterReached);
        const afterInterruptView = (yield* (yield* AgentControlControlledThreadReservation).list({
          projectId: afterInterrupt.input.projectId,
        })).reservations[0]!;
        const afterInterruptCounts = yield* coordinatorPersistenceCounts(
          afterInterruptView.controlledThreadReservationId,
          afterInterruptView.threadId,
        );
        yield* Fiber.interrupt(afterFiber);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(afterFiber)), true);
        assert.equal(yield* Ref.get(reservationPublications), 3);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        const recoveredAfterInterrupt = yield* (yield* buildActivation()).activateInitial(
          afterInterrupt.input,
        );
        assert.equal(recoveredAfterInterrupt.reservation.status, "prepared");
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(reservationPublications), 3);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            afterInterruptView.controlledThreadReservationId,
            afterInterruptView.threadId,
          ),
          afterInterruptCounts,
        );
        yield* Fiber.interrupt(orchestrationSubscriber);
        yield* Fiber.interrupt(reservationSubscriber);
      }),
    60_000,
  );

  it.effect(
    "fails closed on corrupted historical prepare evidence and successors after bound",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_event_no_update`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_catalog_no_update`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_intent_no_update`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_receipt_no_update`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_projection_validate_update`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_projection_validate_update_json`;
        yield* sql`DROP TRIGGER agent_control_controlled_thread_projection_json_total_validate_update`;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_intents_no_update
        `;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_receipts_no_update
        `;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_accepted_no_update
        `;
        yield* sql`
          DROP TRIGGER
            trg_orchestration_materialization_intent_immutable_update
        `;
        yield* sql`
          DROP TRIGGER
            trg_orchestration_materialization_receipt_evidence_immutable_update
        `;
        yield* sql`
          DROP TRIGGER trg_orchestration_materialization_event_immutable_update
        `;

        const activation = yield* AgentControlControlledThreadActivation;
        const corruptionCases = [
          {
            name: "prepared-event",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_events
                SET payload_json = json_set(
                  payload_json, '$.preparedAt', '2026-07-29T00:00:00.000Z'
                )
                WHERE aggregate_kind = 'controlled-thread-reservation'
                  AND stream_id = ${input.reservationId}
                  AND stream_version = 1
              `,
          },
          {
            name: "prepare-intent-fingerprint",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_controlled_thread_command_intents
                SET intent_fingerprint = ${"f".repeat(64)}
                WHERE command_id = ${input.commandId}
              `,
          },
          {
            name: "prepare-receipt-coordinate",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_command_receipts
                SET result_sequence = result_sequence + 1
                WHERE command_id = ${input.commandId}
              `,
          },
          {
            name: "current-projection",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_controlled_thread_reservation_states
                SET state_json = json_set(
                  state_json, '$.preparedAt', '2026-07-29T00:00:00.000Z'
                )
                WHERE controlled_thread_reservation_id = ${input.reservationId}
              `,
          },
          {
            name: "prepared-catalog-coordinate",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_controlled_thread_stream_catalog
                SET prepared_at = '2026-07-29T00:00:00.000Z'
                WHERE controlled_thread_reservation_id = ${input.reservationId}
                  AND stream_version = 1
              `,
          },
          {
            name: "materializing-successor",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql`
                UPDATE agent_control_events
                SET payload_json = json_set(
                  payload_json, '$.coordinatorCommandId', 'corrupt-successor-command'
                )
                WHERE aggregate_kind = 'controlled-thread-reservation'
                  AND stream_id = ${input.reservationId}
                  AND stream_version = 2
              `,
          },
          {
            name: "coherent-foreign-successors",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              Effect.gen(function* () {
                yield* sql`PRAGMA foreign_keys = OFF`;
                yield* sql.withTransaction(
                  Effect.gen(function* () {
                    yield* sql`
                      UPDATE agent_control_events
                      SET command_id = 'foreign-materializing-transition',
                          correlation_id = 'foreign-coordinator',
                          payload_json = json_set(
                            payload_json,
                            '$.coordinatorCommandId',
                            'foreign-coordinator',
                            '$.materializingTransitionCommandId',
                            'foreign-materializing-transition',
                            '$.materializationCommandId',
                            'foreign-materialization'
                          )
                      WHERE aggregate_kind =
                          'controlled-thread-reservation'
                        AND stream_id = ${input.reservationId}
                        AND stream_version = 2
                    `;
                    yield* sql`
                      UPDATE agent_control_events
                      SET command_id = 'foreign-bound-transition',
                          correlation_id = 'foreign-coordinator',
                          payload_json = json_set(
                            payload_json,
                            '$.coordinatorCommandId',
                            'foreign-coordinator',
                            '$.materializingTransitionCommandId',
                            'foreign-materializing-transition',
                            '$.materializationCommandId',
                            'foreign-materialization',
                            '$.boundTransitionCommandId',
                            'foreign-bound-transition'
                          )
                      WHERE aggregate_kind =
                          'controlled-thread-reservation'
                        AND stream_id = ${input.reservationId}
                        AND stream_version = 3
                    `;
                    yield* sql`
                      UPDATE agent_control_controlled_thread_stream_catalog
                      SET command_id = CASE stream_version
                            WHEN 2 THEN 'foreign-materializing-transition'
                            ELSE 'foreign-bound-transition'
                          END,
                          coordinator_command_id = 'foreign-coordinator',
                          materializing_transition_command_id =
                            'foreign-materializing-transition',
                          materialization_command_id =
                            'foreign-materialization',
                          bound_transition_command_id = CASE stream_version
                            WHEN 3 THEN 'foreign-bound-transition'
                            ELSE NULL
                          END
                      WHERE controlled_thread_reservation_id =
                          ${input.reservationId}
                        AND stream_version IN (2, 3)
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_reservation_states
                      SET coordinator_command_id = 'foreign-coordinator',
                          materializing_transition_command_id =
                            'foreign-materializing-transition',
                          materialization_command_id =
                            'foreign-materialization',
                          bound_transition_command_id =
                            'foreign-bound-transition',
                          state_json = json_set(
                            state_json,
                            '$.coordinatorCommandId',
                            'foreign-coordinator',
                            '$.materializingTransitionCommandId',
                            'foreign-materializing-transition',
                            '$.materializationCommandId',
                            'foreign-materialization',
                            '$.boundTransitionCommandId',
                            'foreign-bound-transition'
                          )
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                  }),
                );
                yield* sql`PRAGMA foreign_keys = ON`;
              }),
          },
          {
            name: "coordinator-generation",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              Effect.gen(function* () {
                yield* sql`PRAGMA foreign_keys = OFF`;
                yield* sql.withTransaction(
                  Effect.gen(function* () {
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_intents
                      SET coordinator_command_fingerprint = ${"c".repeat(64)}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_receipts
                      SET coordinator_command_fingerprint = ${"c".repeat(64)}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_accepted
                      SET coordinator_command_fingerprint = ${"c".repeat(64)}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                  }),
                );
                yield* sql`PRAGMA foreign_keys = ON`;
              }),
          },
          {
            name: "coherent-foreign-fingerprints",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              Effect.gen(function* () {
                const coordinatorFingerprint = "c".repeat(64);
                const materializationFingerprint = "d".repeat(64);
                yield* sql`PRAGMA foreign_keys = OFF`;
                yield* sql.withTransaction(
                  Effect.gen(function* () {
                    yield* sql`
                      UPDATE agent_control_events
                      SET payload_json = json_set(
                        payload_json,
                        '$.coordinatorCommandFingerprint',
                        ${coordinatorFingerprint},
                        '$.materializationCommandFingerprint',
                        ${materializationFingerprint}
                      )
                      WHERE aggregate_kind =
                          'controlled-thread-reservation'
                        AND stream_id = ${input.reservationId}
                        AND stream_version IN (2, 3)
                    `;
                    yield* sql`
                      UPDATE agent_control_controlled_thread_stream_catalog
                      SET coordinator_command_fingerprint =
                            ${coordinatorFingerprint},
                          materialization_command_fingerprint =
                            ${materializationFingerprint}
                      WHERE controlled_thread_reservation_id =
                          ${input.reservationId}
                        AND stream_version IN (2, 3)
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_reservation_states
                      SET coordinator_command_fingerprint =
                            ${coordinatorFingerprint},
                          materialization_command_fingerprint =
                            ${materializationFingerprint},
                          state_json = json_set(
                            state_json,
                            '$.coordinatorCommandFingerprint',
                            ${coordinatorFingerprint},
                            '$.materializationCommandFingerprint',
                            ${materializationFingerprint}
                          )
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_intents
                      SET coordinator_command_fingerprint =
                            ${coordinatorFingerprint},
                          materialization_command_fingerprint =
                            ${materializationFingerprint}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_receipts
                      SET coordinator_command_fingerprint =
                            ${coordinatorFingerprint},
                          materialization_command_fingerprint =
                            ${materializationFingerprint}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        agent_control_controlled_thread_materialization_accepted
                      SET coordinator_command_fingerprint =
                            ${coordinatorFingerprint},
                          materialization_command_fingerprint =
                            ${materializationFingerprint}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        orchestration_agent_control_thread_materialization_intents
                      SET command_fingerprint = ${materializationFingerprint}
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    `;
                    yield* sql`
                      UPDATE
                        orchestration_agent_control_thread_materialization_receipts
                      SET command_fingerprint = ${materializationFingerprint}
                      WHERE command_id = (
                        SELECT materialization_command_id
                        FROM
                          agent_control_controlled_thread_materialization_intents
                        WHERE controlled_thread_reservation_id =
                          ${input.reservationId}
                      )
                    `;
                  }),
                );
                yield* sql`PRAGMA foreign_keys = ON`;
              }),
          },
          {
            name: "orchestration-generation",
            corrupt: (input: { readonly commandId: CommandId; readonly reservationId: string }) =>
              sql.withTransaction(
                Effect.gen(function* () {
                  yield* sql`
                    UPDATE
                      orchestration_agent_control_thread_materialization_intents
                    SET title = 'Coherently foreign orchestration generation'
                    WHERE controlled_thread_reservation_id =
                      ${input.reservationId}
                  `;
                  yield* sql`
                    UPDATE orchestration_events
                    SET payload_json = json_set(
                      payload_json,
                      '$.title',
                      'Coherently foreign orchestration generation'
                    )
                    WHERE command_id = (
                      SELECT materialization_command_id
                      FROM
                        agent_control_controlled_thread_materialization_intents
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    )
                      AND stream_version = 1
                  `;
                  yield* sql`
                    UPDATE projection_threads
                    SET title = 'Coherently foreign orchestration generation'
                    WHERE thread_id = (
                      SELECT thread_id
                      FROM
                        agent_control_controlled_thread_materialization_intents
                      WHERE controlled_thread_reservation_id =
                        ${input.reservationId}
                    )
                  `;
                }),
              ),
          },
        ] as const;

        const reservations = yield* AgentControlControlledThreadReservation;
        for (const testCase of corruptionCases) {
          const repo = yield* makeRepository();
          const projectId = ProjectId.make(`activation-corrupt-${testCase.name}`);
          const seeded = yield* seedPrepared(projectId, repo.cwd);
          yield* reserveLease(seeded.stageRun);
          yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
            commandId: CommandId.make(`activation-corrupt-ready-${testCase.name}`),
            projectId,
            taskId: seeded.task.taskId,
          });
          const command = {
            commandId: CommandId.make(`activation-corrupt-prepare-${testCase.name}`),
            projectId,
            taskId: seeded.task.taskId,
          } as const;
          const accepted = yield* activation.activateInitial(command);
          const counts = yield* coordinatorPersistenceCounts(
            accepted.reservation.controlledThreadReservationId,
            accepted.reservation.threadId,
          );
          yield* testCase.corrupt({
            commandId: command.commandId,
            reservationId: accepted.reservation.controlledThreadReservationId,
          });

          const directReplay = yield* Effect.result(reservations.prepareInitial(command));
          assert.equal(directReplay._tag, "Failure", `${testCase.name}-direct`);
          if (directReplay._tag === "Failure") {
            assert.equal(
              directReplay.failure.code,
              "controlled-thread-reservation-corrupt",
              `${testCase.name}-direct`,
            );
          }
          const replay = yield* Effect.result(activation.activateInitial(command));
          assert.equal(replay._tag, "Failure", testCase.name);
          if (replay._tag === "Failure") {
            assert.equal(
              replay.failure.code,
              "controlled-thread-reservation-corrupt",
              testCase.name,
            );
          }
          assert.deepStrictEqual(
            yield* coordinatorPersistenceCounts(
              accepted.reservation.controlledThreadReservationId,
              accepted.reservation.threadId,
            ),
            counts,
            testCase.name,
          );
        }
      }),
    60_000,
  );

  it.effect(
    "converges independent WAL activation facades and keeps interrupted callers receipt-safe",
    () =>
      Effect.gen(function* () {
        const inspectionsA = yield* Ref.make(0);
        const inspectionsB = yield* Ref.make(0);
        const harness = yield* makeIndependentControllerContexts(
          {
            afterReadyInspection: () => Ref.update(inspectionsA, (count) => count + 1),
          },
          {
            afterReadyInspection: () => Ref.update(inspectionsB, (count) => count + 1),
          },
        );
        assert.notStrictEqual(harness.sqlA, harness.sqlB);
        for (const sql of [harness.sqlA, harness.sqlB]) {
          assert.deepStrictEqual(
            yield* sql`
              SELECT
                (SELECT journal_mode FROM pragma_journal_mode) AS journalMode,
                (SELECT foreign_keys FROM pragma_foreign_keys) AS foreignKeys
            `,
            [{ journalMode: "wal", foreignKeys: 1 }],
          );
        }

        const reachedA = yield* Deferred.make<void>();
        const reachedB = yield* Deferred.make<void>();
        const releaseA = yield* Deferred.make<void>();
        const releaseB = yield* Deferred.make<void>();
        const reservationHooksA: AgentControlControlledThreadReservationTransactionHooksShape = {
          afterReadyInspection: Effect.void,
          beforeDbAdmission: Effect.void,
          afterDbAdmission: Effect.void,
          beforeEventAppend: Deferred.succeed(reachedA, undefined).pipe(
            Effect.andThen(Deferred.await(releaseA)),
          ),
          afterWritesBeforeCommit: Effect.void,
        };
        const reservationHooksB: AgentControlControlledThreadReservationTransactionHooksShape = {
          afterReadyInspection: Effect.void,
          beforeDbAdmission: Effect.void,
          afterDbAdmission: Effect.void,
          beforeEventAppend: Deferred.succeed(reachedB, undefined).pipe(
            Effect.andThen(Deferred.await(releaseB)),
          ),
          afterWritesBeforeCommit: Effect.void,
        };
        const engineContextA = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
            Layer.provide(Layer.succeedContext(harness.contextA)),
          ),
          harness.scopeA,
        ).pipe(
          Effect.provideService(
            AgentControlControlledThreadReservationTransactionHooks,
            reservationHooksA,
          ),
        );
        const engineA = Context.get(engineContextA, AgentControlControlledThreadReservationEngine);
        const controllerA = AgentControlWorktreeController.of({
          ...harness.controllerA,
          useReadyWorktree: (input, callback) =>
            harness.controllerA
              .useReadyWorktree(input, (readyWorktree) => Effect.succeed(readyWorktree))
              .pipe(Effect.flatMap((readyWorktree) => Effect.scoped(callback(readyWorktree)))),
        });
        const dependenciesA = Context.add(
          Context.add(harness.contextA, AgentControlControlledThreadReservationEngine, engineA),
          AgentControlWorktreeController,
          controllerA,
        );
        const reservationContextA = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(dependenciesA)),
          ),
          harness.scopeA,
        ).pipe(
          Effect.provideService(
            AgentControlControlledThreadReservationTransactionHooks,
            reservationHooksA,
          ),
        );
        const reservationA = Context.get(
          reservationContextA,
          AgentControlControlledThreadReservation,
        );

        const controllerB = AgentControlWorktreeController.of({
          ...harness.controllerB,
          useReadyWorktree: (input, callback) =>
            harness.controllerB
              .useReadyWorktree(input, (readyWorktree) => Effect.succeed(readyWorktree))
              .pipe(Effect.flatMap((readyWorktree) => Effect.scoped(callback(readyWorktree)))),
        });
        const dependenciesBWithoutEngine = Context.add(
          harness.controllerDependenciesB,
          AgentControlWorktreeController,
          controllerB,
        );
        const engineContextB = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
            Layer.provide(Layer.succeedContext(dependenciesBWithoutEngine)),
          ),
          harness.scopeB,
        ).pipe(
          Effect.provideService(
            AgentControlControlledThreadReservationTransactionHooks,
            reservationHooksB,
          ),
        );
        const engineB = Context.get(engineContextB, AgentControlControlledThreadReservationEngine);
        const dependenciesB = Context.add(
          dependenciesBWithoutEngine,
          AgentControlControlledThreadReservationEngine,
          engineB,
        );
        const reservationContextB = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(dependenciesB)),
          ),
          harness.scopeB,
        ).pipe(
          Effect.provideService(
            AgentControlControlledThreadReservationTransactionHooks,
            reservationHooksB,
          ),
        );
        const reservationB = Context.get(
          reservationContextB,
          AgentControlControlledThreadReservation,
        );

        const orchestrationContextA = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, harness.sqlA)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          harness.scopeA,
        );
        const orchestrationContextB = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, harness.sqlB)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          harness.scopeB,
        );
        const policyContextA = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          harness.scopeA,
        );
        const policyContextB = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          harness.scopeB,
        );
        const coordinatorDependenciesA = Context.merge(
          Context.merge(dependenciesA, reservationContextA),
          Context.merge(orchestrationContextA, policyContextA),
        );
        const coordinatorDependenciesB = Context.merge(
          Context.merge(dependenciesB, reservationContextB),
          Context.merge(orchestrationContextB, policyContextB),
        );
        const decisionsA = yield* Ref.make(0);
        const decisionsB = yield* Ref.make(0);
        const buildWalCoordinator = Effect.fn("buildActivationWalCoordinator")(function* (
          dependencies: typeof coordinatorDependenciesA,
          scope: Scope.Closeable,
          hooks: AgentControlControlledThreadMaterializationCoordinatorHooksShape,
        ) {
          const context = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlControlledThreadMaterializationCoordinatorLive).pipe(
              Layer.provide(Layer.succeedContext(dependencies)),
            ),
            scope,
          ).pipe(
            Effect.provideService(
              AgentControlControlledThreadMaterializationCoordinatorHooks,
              hooks,
            ),
          );
          return Context.get(context, AgentControlControlledThreadMaterializationCoordinator);
        });
        const coordinatorA = yield* buildWalCoordinator(coordinatorDependenciesA, harness.scopeA, {
          ...coordinatorNoopHooks,
          afterMaterializingProjection: () => Ref.update(decisionsA, (count) => count + 1),
        });
        const coordinatorB = yield* buildWalCoordinator(
          coordinatorDependenciesB as typeof coordinatorDependenciesA,
          harness.scopeB,
          {
            ...coordinatorNoopHooks,
            afterMaterializingProjection: () => Ref.update(decisionsB, (count) => count + 1),
          },
        );
        const coordinatorCallsA = yield* Ref.make(0);
        const coordinatorCallsB = yield* Ref.make(0);
        const countedCoordinatorA = AgentControlControlledThreadMaterializationCoordinator.of({
          ...coordinatorA,
          materializeInitial: (command) =>
            Ref.update(coordinatorCallsA, (count) => count + 1).pipe(
              Effect.andThen(coordinatorA.materializeInitial(command)),
            ),
        });
        const countedCoordinatorB = AgentControlControlledThreadMaterializationCoordinator.of({
          ...coordinatorB,
          materializeInitial: (command) =>
            Ref.update(coordinatorCallsB, (count) => count + 1).pipe(
              Effect.andThen(coordinatorB.materializeInitial(command)),
            ),
        });
        const activationA = yield* buildActivation({
          reservation: reservationA,
          coordinator: countedCoordinatorA,
        });
        const activationB = yield* buildActivation({
          reservation: reservationB,
          coordinator: countedCoordinatorB,
        });
        const seedWalActivation = Effect.fn("seedWalActivation")(function* (suffix: string) {
          const repo = yield* makeRepository();
          const projectId = ProjectId.make(`controlled-thread-activation-wal-${suffix}`);
          const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
            Effect.provide(harness.contextA),
          );
          yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
          yield* harness.controllerA.reserveAndMaterialize({
            commandId: CommandId.make(`activation-wal-ready-${suffix}`),
            projectId,
            taskId: seeded.task.taskId,
          });
          return {
            commandId: CommandId.make(`activation-wal-prepare-${suffix}`),
            projectId,
            taskId: seeded.task.taskId,
          } as const;
        });

        const input = yield* seedWalActivation("same-command");
        const reservationPublicationsA = yield* Ref.make(0);
        const reservationPublicationsB = yield* Ref.make(0);
        const orchestrationPublicationsA = yield* Ref.make(0);
        const orchestrationPublicationsB = yield* Ref.make(0);
        const subscribers = [
          yield* engineA.streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(reservationPublicationsA, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          ),
          yield* engineB.streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(reservationPublicationsB, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          ),
          yield* Context.get(
            orchestrationContextA,
            OrchestrationEngineService,
          ).streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(orchestrationPublicationsA, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          ),
          yield* Context.get(
            orchestrationContextB,
            OrchestrationEngineService,
          ).streamDomainEvents.pipe(
            Stream.runForEach(() => Ref.update(orchestrationPublicationsB, (count) => count + 1)),
            Effect.forkChild({ startImmediately: true }),
          ),
        ] as const;
        const callerA = yield* activationA
          .activateInitial(input)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(reachedA);
        const callerB = yield* activationB
          .activateInitial(input)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(reachedB);
        yield* Deferred.succeed(releaseA, undefined);
        const resultA = yield* Fiber.join(callerA);
        yield* Deferred.succeed(releaseB, undefined);
        const resultB = yield* Fiber.join(callerB);
        assert.deepStrictEqual(resultB, resultA);
        assert.equal(resultA.reservation.status, "prepared");
        const committedCounts = yield* coordinatorPersistenceCounts(
          resultA.reservation.controlledThreadReservationId,
          resultA.reservation.threadId,
        ).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlA));
        assert.deepStrictEqual(committedCounts, {
          reservationEvents: 3,
          reservationProjections: 1,
          orchestrationEvents: 2,
          threadProjections: 1,
          orchestrationIntents: 1,
          orchestrationReceipts: 1,
          orchestrationMarkers: 1,
          coordinatorIntents: 1,
          coordinatorReceipts: 1,
          coordinatorMarkers: 1,
        });
        assert.deepStrictEqual(
          yield* harness.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_command_receipts
               WHERE command_id = ${input.commandId}) AS prepareReceipts,
              (SELECT count(*) FROM agent_control_controlled_thread_command_intents
               WHERE command_id = ${input.commandId}) AS prepareIntents
          `,
          [{ prepareReceipts: 1, prepareIntents: 1 }],
        );
        assert.equal(yield* Ref.get(decisionsA), 1);
        assert.equal(yield* Ref.get(decisionsB), 0);
        assert.equal(yield* Ref.get(coordinatorCallsA), 1);
        assert.equal(yield* Ref.get(coordinatorCallsB), 1);
        assert.equal(yield* Ref.get(reservationPublicationsA), 3);
        assert.equal(yield* Ref.get(reservationPublicationsB), 0);
        assert.equal(yield* Ref.get(orchestrationPublicationsA), 2);
        assert.equal(yield* Ref.get(orchestrationPublicationsB), 0);

        const conflicting = yield* Effect.result(
          activationB.activateInitial({
            ...input,
            commandId: CommandId.make("activation-wal-different-command"),
          }),
        );
        assert.equal(conflicting._tag, "Failure");
        if (conflicting._tag === "Failure") {
          assert.equal(conflicting.failure.code, "controlled-thread-reservation-identity-conflict");
        }
        assert.equal(yield* Ref.get(coordinatorCallsB), 1);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            resultA.reservation.controlledThreadReservationId,
            resultA.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlB)),
          committedCounts,
        );

        const interruptedInput = yield* seedWalActivation("loser-interrupt");
        const loserReached = yield* Deferred.make<void>();
        const holdLoser = yield* Deferred.make<void>();
        const interruptedLoserFacade = yield* buildActivation({
          reservation: reservationB,
          coordinator: countedCoordinatorB,
          hooks: {
            ...activationNoopHooks,
            afterPrepareAcceptedBeforeMaterialize: () =>
              Deferred.succeed(loserReached, undefined).pipe(
                Effect.andThen(Deferred.await(holdLoser)),
              ),
          },
        });
        const loser = yield* interruptedLoserFacade
          .activateInitial(interruptedInput)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(loserReached);
        const interruptWinner = yield* activationA.activateInitial(interruptedInput);
        yield* Fiber.interrupt(loser);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(loser)), true);
        assert.equal(interruptWinner.reservation.status, "prepared");
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            interruptWinner.reservation.controlledThreadReservationId,
            interruptWinner.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlA)),
          committedCounts,
        );

        const winnerInterruptedInput = yield* seedWalActivation("winner-interrupt");
        const winnerReached = yield* Deferred.make<void>();
        const holdWinner = yield* Deferred.make<void>();
        const interruptedWinnerFacade = yield* buildActivation({
          reservation: reservationA,
          coordinator: countedCoordinatorA,
          hooks: {
            ...activationNoopHooks,
            afterMaterializationAcceptedBeforeReturn: () =>
              Deferred.succeed(winnerReached, undefined).pipe(
                Effect.andThen(Deferred.await(holdWinner)),
              ),
          },
        });
        const interruptedWinner = yield* interruptedWinnerFacade
          .activateInitial(winnerInterruptedInput)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(winnerReached);
        const winnerCounts = (yield* reservationB.list({
          projectId: winnerInterruptedInput.projectId,
        })).reservations[0]!;
        const committedBeforeInterrupt = yield* coordinatorPersistenceCounts(
          winnerCounts.controlledThreadReservationId,
          winnerCounts.threadId,
        ).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlB));
        yield* Fiber.interrupt(interruptedWinner);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(interruptedWinner)), true);
        const winnerReplay = yield* activationB.activateInitial(winnerInterruptedInput);
        assert.equal(winnerReplay.reservation.status, "prepared");
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            winnerReplay.reservation.controlledThreadReservationId,
            winnerReplay.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, harness.sqlA)),
          committedBeforeInterrupt,
        );
        for (const subscriber of subscribers) {
          yield* Fiber.interrupt(subscriber);
        }
      }),
    90_000,
  );
});

coordinatorLayer("Controlled thread materialization coordinator", (it) => {
  it.effect("fails closed on an isolated in-transaction materializing successor", () =>
    Effect.gen(function* () {
      const seeded = yield* seedCoordinatorReservation("historical-materializing-replay");
      const prepareInput = {
        commandId: CommandId.make("coordinator-prepare-historical-materializing-replay"),
        projectId: seeded.projectId,
        taskId: seeded.task.taskId,
      } as const;
      const historical = yield* Ref.make<string | null>(null);
      const reservations = yield* AgentControlControlledThreadReservation;
      const coordinator = yield* buildCoordinator({
        hooks: {
          ...coordinatorNoopHooks,
          afterMaterializingProjection: () =>
            Effect.result(reservations.prepareInitial(prepareInput)).pipe(
              Effect.flatMap((result) =>
                Ref.set(
                  historical,
                  result._tag === "Failure" ? result.failure.code : "unexpected-success",
                ),
              ),
            ),
        },
      });

      yield* coordinator.materializeInitial(seeded.command);
      const replay = yield* Ref.get(historical);
      assert.equal(replay, "controlled-thread-reservation-corrupt");
      assert.deepStrictEqual(
        yield* coordinatorPersistenceCounts(
          seeded.reservation.controlledThreadReservationId,
          seeded.reservation.threadId,
        ),
        {
          reservationEvents: 3,
          reservationProjections: 1,
          orchestrationEvents: 2,
          threadProjections: 1,
          orchestrationIntents: 1,
          orchestrationReceipts: 1,
          orchestrationMarkers: 1,
          coordinatorIntents: 1,
          coordinatorReceipts: 1,
          coordinatorMarkers: 1,
        },
      );
    }),
  );

  it.effect(
    "commits bound reservation and controlled thread once, then replays without side effects",
    () =>
      Effect.gen(function* () {
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-coordinator-happy-path");
        const seeded = yield* seedPrepared(projectId, repo.cwd);
        yield* reserveLease(seeded.stageRun);
        const ready = yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("coordinator-ready-worktree"),
          projectId,
          taskId: seeded.task.taskId,
        });
        assert.equal(ready.status, "ready");
        const prepareCommandId = CommandId.make("coordinator-prepare-reservation");
        const reservation = yield* (yield* AgentControlControlledThreadReservation).prepareInitial({
          commandId: prepareCommandId,
          projectId,
          taskId: seeded.task.taskId,
        });
        const command = {
          commandId: yield* deriveAgentControlControlledThreadActivationCommandId(
            prepareCommandId,
            reservation.reservation.controlledThreadReservationId,
          ),
          projectId,
          controlledThreadReservationId: reservation.reservation.controlledThreadReservationId,
        } as const;
        const coordinator = yield* AgentControlControlledThreadMaterializationCoordinator;
        const first = yield* coordinator.materializeInitial(command);
        assert.equal(first.status, "bound");
        assert.equal(first.replayed, false);

        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(
          yield* sql`
            SELECT status, revision,
              coordinator_command_id AS "coordinatorCommandId",
              orchestration_result_sequence AS "orchestrationResultSequence"
            FROM agent_control_controlled_thread_reservation_states
            WHERE controlled_thread_reservation_id =
              ${command.controlledThreadReservationId}
          `,
          [
            {
              status: "bound",
              revision: 3,
              coordinatorCommandId: command.commandId,
              orchestrationResultSequence: first.orchestrationResultSequence,
            },
          ],
        );
        const ownerEvidence = yield* sql<{
          readonly intentOwnerId: string;
          readonly markerOwnerId: string;
        }>`
          SELECT intent.finalization_owner_id AS "intentOwnerId",
            accepted.finalization_owner_id AS "markerOwnerId"
          FROM agent_control_controlled_thread_materialization_intents intent
          JOIN agent_control_controlled_thread_materialization_accepted accepted
            ON accepted.coordinator_command_id = intent.coordinator_command_id
          WHERE intent.coordinator_command_id = ${command.commandId}
        `;
        assert.equal(ownerEvidence.length, 1);
        assert.equal(ownerEvidence[0]!.intentOwnerId, ownerEvidence[0]!.markerOwnerId);
        assert.match(
          ownerEvidence[0]!.intentOwnerId,
          /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
        );
        for (const evidenceTable of [
          "agent_control_controlled_thread_materialization_intents",
          "agent_control_controlled_thread_materialization_receipts",
          "agent_control_controlled_thread_materialization_accepted",
        ] as const) {
          assert.equal(
            (yield* Effect.exit(
              sql.unsafe(`UPDATE ${evidenceTable} SET accepted_at = accepted_at`).unprepared,
            ))._tag,
            "Failure",
          );
          assert.equal(
            (yield* Effect.exit(sql.unsafe(`DELETE FROM ${evidenceTable}`).unprepared))._tag,
            "Failure",
          );
        }
        const markerCommandIds = (yield* sql<{
          readonly orchestrationCommandId: string;
          readonly coordinatorCommandId: string;
        }>`
            SELECT materialization_command_id AS "orchestrationCommandId",
              coordinator_command_id AS "coordinatorCommandId"
            FROM agent_control_controlled_thread_materialization_intents
            WHERE coordinator_command_id = ${command.commandId}
          `)[0]!;
        const markerHookCalls = yield* Ref.make(0);
        const markerHooks = {
          afterCommitBeforeReturn: () => Ref.update(markerHookCalls, (count) => count + 1),
        };
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sql.unsafe(
                `INSERT OR IGNORE INTO
                   orchestration_agent_control_thread_materialization_receipts
                 SELECT *
                 FROM orchestration_agent_control_thread_materialization_receipts
                 WHERE command_id = ?`,
                [markerCommandIds.orchestrationCommandId],
              );
              assert.deepStrictEqual(yield* sql`SELECT changes() AS changes`, [{ changes: 0 }]);
              yield* sql.unsafe(
                `INSERT OR IGNORE INTO
                   agent_control_controlled_thread_materialization_accepted
                 SELECT *
                 FROM agent_control_controlled_thread_materialization_accepted
                 WHERE coordinator_command_id = ?`,
                [markerCommandIds.coordinatorCommandId],
              );
              assert.deepStrictEqual(yield* sql`SELECT changes() AS changes`, [{ changes: 0 }]);
            }),
          )
          .pipe(Effect.provideService(NodeSqliteTransactionHooks, markerHooks));
        assert.equal(yield* Ref.get(markerHookCalls), 0);

        const conflictCases = [
          {
            table: "orchestration_agent_control_thread_materialization_receipts",
            key: "command_id",
            value: markerCommandIds.orchestrationCommandId,
          },
          {
            table: "agent_control_controlled_thread_materialization_accepted",
            key: "coordinator_command_id",
            value: markerCommandIds.coordinatorCommandId,
          },
        ] as const;
        for (const replaceCase of conflictCases) {
          const replaceFailure = yield* Effect.exit(
            sql
              .withTransaction(
                sql.unsafe(
                  `REPLACE INTO ${replaceCase.table}
                   SELECT * FROM ${replaceCase.table}
                   WHERE ${replaceCase.key} = ?`,
                  [replaceCase.value],
                ),
              )
              .pipe(Effect.provideService(NodeSqliteTransactionHooks, markerHooks)),
          );
          assert.equal(replaceFailure._tag, "Failure", replaceCase.table);
          if (Exit.isFailure(replaceFailure)) {
            assert.match(
              Cause.pretty(replaceFailure.cause),
              /FOREIGN KEY constraint failed|immutable/,
              replaceCase.table,
            );
          }
        }
        assert.equal(yield* Ref.get(markerHookCalls), 0);

        for (const updateCase of conflictCases) {
          const updateFailure = yield* Effect.exit(
            sql
              .withTransaction(
                sql.unsafe(
                  `INSERT INTO ${updateCase.table}
                   SELECT * FROM ${updateCase.table}
                   WHERE ${updateCase.key} = ? AND true
                   ON CONFLICT(${updateCase.key})
                   DO UPDATE SET ${updateCase.key} = excluded.${updateCase.key}`,
                  [updateCase.value],
                ),
              )
              .pipe(Effect.provideService(NodeSqliteTransactionHooks, markerHooks)),
          );
          assert.equal(updateFailure._tag, "Failure", updateCase.table);
          if (Exit.isFailure(updateFailure)) {
            assert.include(Cause.pretty(updateFailure.cause), "immutable", updateCase.table);
          }
        }
        assert.equal(yield* Ref.get(markerHookCalls), 0);
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*)
               FROM orchestration_agent_control_thread_materialization_receipts
               WHERE command_id = ${markerCommandIds.orchestrationCommandId})
                AS orchestrationMarkers,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_accepted
               WHERE coordinator_command_id = ${markerCommandIds.coordinatorCommandId})
                AS coordinatorMarkers
          `,
          [{ orchestrationMarkers: 1, coordinatorMarkers: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind = 'controlled-thread-reservation'
                 AND stream_id = ${command.controlledThreadReservationId})
                AS reservationEvents,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${first.threadId}) AS orchestrationEvents,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_intents
               WHERE coordinator_command_id = ${command.commandId}) AS coordinatorIntents,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_receipts
               WHERE coordinator_command_id = ${command.commandId}) AS coordinatorReceipts,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_accepted
               WHERE coordinator_command_id = ${command.commandId}) AS acceptedMarkers,
              (SELECT count(*) FROM provider_session_runtime) AS providerSessions,
              (SELECT count(*) FROM orchestration_events
               WHERE event_type LIKE 'provider.%') AS providerCommands,
              (SELECT count(*) FROM projection_turns) AS turns,
              (SELECT count(*) FROM projection_thread_messages) AS messages,
              (SELECT count(*) FROM orchestration_events
               WHERE event_type LIKE 'terminal.%'
                  OR event_type LIKE 'process.%') AS terminalOrProcessStarts,
              (SELECT count(*) FROM agent_control_events
               WHERE event_type LIKE 'agentControl.taskExecution.%')
                AS taskExecutionEvents,
              (SELECT count(*) FROM agent_control_github_scheduler_states)
                AS schedulerOrReactorActivity
          `,
          [
            {
              reservationEvents: 3,
              orchestrationEvents: 2,
              coordinatorIntents: 1,
              coordinatorReceipts: 1,
              acceptedMarkers: 1,
              providerSessions: 0,
              providerCommands: 0,
              turns: 0,
              messages: 0,
              terminalOrProcessStarts: 0,
              taskExecutionEvents: 0,
              schedulerOrReactorActivity: 0,
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT json_extract(agent_control_json, '$.controlState') AS controlState,
              latest_turn_id AS "latestTurnId"
            FROM projection_threads
            WHERE thread_id = ${first.threadId}
          `,
          [{ controlState: "controlled", latestTurnId: null }],
        );
        yield* (yield* AgentControlControlledThreadReservationEngine).rebuild;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
              DELETE FROM projection_threads
              WHERE thread_id = ${first.threadId}
            `;
            yield* sql`
              DELETE FROM projection_state
              WHERE projector = ${ORCHESTRATION_PROJECTOR_NAMES.threads}
            `;
          }),
        );
        yield* (yield* OrchestrationProjectionPipeline).bootstrap;
        const replay = yield* coordinator.materializeInitial(command);
        assert.equal(replay.replayed, true);
        assert.equal(replay.threadId, first.threadId);
        assert.equal(replay.orchestrationResultSequence, first.orchestrationResultSequence);
        assert.deepStrictEqual(
          yield* sql`
            SELECT status, revision,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_intents
               WHERE coordinator_command_id = ${command.commandId}) AS coordinatorIntents,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_receipts
               WHERE coordinator_command_id = ${command.commandId}) AS coordinatorReceipts,
              (SELECT count(*)
               FROM agent_control_controlled_thread_materialization_accepted
               WHERE coordinator_command_id = ${command.commandId}) AS acceptedMarkers,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${first.threadId}) AS orchestrationEvents
            FROM agent_control_controlled_thread_reservation_states
            WHERE controlled_thread_reservation_id =
              ${command.controlledThreadReservationId}
          `,
          [
            {
              status: "bound",
              revision: 3,
              coordinatorIntents: 1,
              coordinatorReceipts: 1,
              acceptedMarkers: 1,
              orchestrationEvents: 2,
            },
          ],
        );
      }),
  );

  it.effect(
    "publishes only after both projections are complete and replays before mutable checks",
    () =>
      Effect.gen(function* () {
        const seeded = yield* seedCoordinatorReservation("publish-and-replay");
        const sql = yield* SqlClient.SqlClient;
        const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
        const orchestrationEngine = yield* OrchestrationEngineService;
        const policy = yield* AgentControlPolicyService;
        const worktreeController = yield* AgentControlWorktreeController;
        const policyChecks = yield* Ref.make(0);
        const worktreeUses = yield* Ref.make(0);
        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);
        const assertCommittedProjections = Effect.fn("assertCommittedCoordinatorProjections")(
          function* (threadId: string) {
            assert.deepStrictEqual(
              yield* sql`
                SELECT reservation.status,
                  json_extract(thread.agent_control_json, '$.controlState') AS controlState
                FROM agent_control_controlled_thread_reservation_states reservation
                JOIN projection_threads thread ON thread.thread_id = reservation.thread_id
                WHERE reservation.controlled_thread_reservation_id =
                  ${seeded.command.controlledThreadReservationId}
                  AND thread.thread_id = ${threadId}
              `.pipe(Effect.orDie),
              [{ status: "bound", controlState: "controlled" }],
            );
          },
        );
        const coordinator = yield* buildCoordinator({
          policy: AgentControlPolicyService.of({
            ...policy,
            preflightRuntime: (input) =>
              Ref.update(policyChecks, (count) => count + 1).pipe(
                Effect.andThen(policy.preflightRuntime(input)),
              ),
          }),
          worktreeController: AgentControlWorktreeController.of({
            ...worktreeController,
            useReadyWorktree: (input, callback, options) =>
              Ref.update(worktreeUses, (count) => count + 1).pipe(
                Effect.andThen(worktreeController.useReadyWorktree(input, callback, options)),
              ),
          }),
          reservationEngine: AgentControlControlledThreadReservationEngine.of({
            ...reservationEngine,
            publishCommitted: (events) =>
              Effect.gen(function* () {
                yield* assertCommittedProjections(events[0]!.payload.threadId);
                yield* Ref.update(reservationPublications, (count) => count + events.length);
                yield* reservationEngine.publishCommitted(events);
              }),
          }),
          orchestrationEngine: OrchestrationEngineService.of({
            ...orchestrationEngine,
            publishAgentControlMaterialization: (result) =>
              Effect.gen(function* () {
                yield* assertCommittedProjections(result.command.threadId);
                yield* Ref.update(
                  orchestrationPublications,
                  (count) => count + result.committedEvents.length,
                );
                const publish = orchestrationEngine.publishAgentControlMaterialization;
                if (publish === undefined) {
                  return yield* Effect.die(new Error("missing orchestration publisher"));
                }
                yield* publish(result);
              }),
          }),
        });

        const first = yield* coordinator.materializeInitial(seeded.command);
        assert.equal(first.replayed, false);
        assert.equal(yield* Ref.get(reservationPublications), 2);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        const policyChecksAfterCommit = yield* Ref.get(policyChecks);
        const worktreeUsesAfterCommit = yield* Ref.get(worktreeUses);
        assert.isAbove(policyChecksAfterCommit, 0);
        assert.equal(worktreeUsesAfterCommit, 1);

        yield* sql`
          UPDATE projection_projects
          SET deleted_at = '2026-07-27T11:00:00.000Z'
          WHERE project_id = ${seeded.projectId}
        `;
        const replay = yield* coordinator.materializeInitial(seeded.command);
        assert.equal(replay.replayed, true);
        assert.equal(yield* Ref.get(policyChecks), policyChecksAfterCommit);
        assert.equal(yield* Ref.get(worktreeUses), worktreeUsesAfterCommit);
        assert.equal(yield* Ref.get(reservationPublications), 2);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        const countsBeforeIdentityConflicts = yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        );

        const changedInput = yield* Effect.result(
          coordinator.materializeInitial({
            ...seeded.command,
            projectId: ProjectId.make("changed-project"),
          }),
        );
        assert.equal(changedInput._tag, "Failure");
        if (changedInput._tag === "Failure") {
          assert.equal(changedInput.failure.reason, "command-identity-conflict");
        }
        const competingCommand = yield* Effect.result(
          coordinator.materializeInitial({
            ...seeded.command,
            commandId: CommandId.make("different-command-same-reservation"),
          }),
        );
        assert.equal(competingCommand._tag, "Failure");
        if (competingCommand._tag === "Failure") {
          assert.equal(competingCommand.failure.reason, "command-identity-conflict");
        }
        assert.equal(yield* Ref.get(policyChecks), policyChecksAfterCommit);
        assert.equal(yield* Ref.get(worktreeUses), worktreeUsesAfterCommit);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ),
          countsBeforeIdentityConflicts,
        );
      }),
  );

  it.effect("materializes the exact later non-strict fallback including model options", () =>
    Effect.gen(function* () {
      const seeded = yield* seedCoordinatorReservation("later-runtime-fallback");
      const policy = yield* AgentControlPolicyService;
      const validSelection = {
        instanceId: ProviderInstanceId.make("coordinator-later-provider"),
        model: "same-model",
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
        ],
      } as const;
      const coordinator = yield* buildCoordinator({
        policy: AgentControlPolicyService.of({
          ...policy,
          preflightRuntime: () =>
            Effect.succeed({
              ok: true,
              staticPreflight: {
                ok: false,
                roles: [
                  {
                    role: "planner",
                    accessMode: "restricted",
                    strict: false,
                    validCandidates: [
                      {
                        selection: validSelection,
                        source: "role-route",
                        driverKind: ProviderDriverKind.make("codex"),
                      },
                    ],
                  },
                ],
                errors: [
                  {
                    code: "provider-disabled",
                    role: "planner",
                    source: "role-route",
                    candidateIndex: 0,
                    instanceId: ProviderInstanceId.make("coordinator-invalid-provider"),
                    expectedDriverKind: null,
                    actualDriverKind: ProviderDriverKind.make("codex"),
                  },
                ],
              },
              roles: [
                {
                  role: "planner",
                  accessMode: "restricted",
                  strict: false,
                  candidates: [
                    {
                      candidateIndex: 0,
                      source: "role-route",
                      providerInstanceId: validSelection.instanceId,
                      model: validSelection.model,
                      driverKind: ProviderDriverKind.make("codex"),
                      providerStatus: "ready",
                      authStatus: "authenticated",
                      checkedAt: at,
                      runtimeReady: true,
                      errorCode: null,
                    },
                  ],
                  selectedCandidateIndex: 0,
                  errorCode: null,
                },
              ],
            }),
        }),
      });

      const result = yield* coordinator.materializeInitial(seeded.command);
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`
          SELECT model_selection_json AS "modelSelectionJson"
          FROM projection_threads
          WHERE thread_id = ${result.threadId}
        `,
        [{ modelSelectionJson: encodeUnknownJson(validSelection) }],
      );
    }),
  );

  it.effect("does not hold the SQLite connection permit while runtime preflight is blocked", () =>
    Effect.gen(function* () {
      const seeded = yield* seedCoordinatorReservation("runtime-preflight-permit");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const policy = yield* AgentControlPolicyService;
      const coordinator = yield* buildCoordinator({
        policy: AgentControlPolicyService.of({
          ...policy,
          preflightRuntime: (input) =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(policy.preflightRuntime(input)),
            ),
        }),
      });
      const caller = yield* coordinator
        .materializeInitial(seeded.command)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.addFinalizer(() => Fiber.interrupt(caller).pipe(Effect.asVoid));
      yield* Deferred.await(entered);

      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* sql.withTransaction(sql`SELECT 1 AS available`), [
        { available: 1 },
      ]);
      assert.deepStrictEqual(
        yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        ),
        {
          reservationEvents: 1,
          reservationProjections: 1,
          orchestrationEvents: 0,
          threadProjections: 0,
          orchestrationIntents: 0,
          orchestrationReceipts: 0,
          orchestrationMarkers: 0,
          coordinatorIntents: 0,
          coordinatorReceipts: 0,
          coordinatorMarkers: 0,
        },
      );

      yield* Deferred.succeed(release, undefined);
      const result = yield* Fiber.join(caller);
      assert.equal(result.status, "bound");
    }),
  );

  it.effect("keeps runtime timeout, interrupt, and defect failures receiptless", () =>
    Effect.gen(function* () {
      const policy = yield* AgentControlPolicyService;
      for (const failureKind of ["timeout", "interrupt", "defect"] as const) {
        const seeded = yield* seedCoordinatorReservation(`runtime-${failureKind}`);
        const baseline = yield* policy.preflightRuntime({ projectId: seeded.projectId });
        const unavailable = {
          ...baseline,
          ok: false,
          roles: baseline.roles.map((role) =>
            role.role === "planner"
              ? {
                  ...role,
                  candidates: role.candidates.map((candidate) => ({
                    ...candidate,
                    runtimeReady: false,
                    errorCode: "provider-probe-timeout" as const,
                  })),
                  selectedCandidateIndex: null,
                  errorCode: "role-runtime-unresolved" as const,
                }
              : role,
          ),
        };
        const failedPreflight =
          failureKind === "timeout"
            ? Effect.succeed(unavailable)
            : failureKind === "interrupt"
              ? Effect.interrupt
              : Effect.die(new Error("runtime-preflight-defect"));
        const coordinator = yield* buildCoordinator({
          policy: AgentControlPolicyService.of({
            ...policy,
            preflightRuntime: () => failedPreflight,
          }),
        });

        const failed = yield* Effect.exit(coordinator.materializeInitial(seeded.command));
        assert.equal(failed._tag, "Failure", failureKind);
        if (Exit.isFailure(failed) && failureKind === "interrupt") {
          assert.isTrue(Cause.hasInterrupts(failed.cause));
        }
        if (Exit.isFailure(failed) && failureKind === "defect") {
          assert.isTrue(Cause.hasDies(failed.cause));
        }
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ),
          {
            reservationEvents: 1,
            reservationProjections: 1,
            orchestrationEvents: 0,
            threadProjections: 0,
            orchestrationIntents: 0,
            orchestrationReceipts: 0,
            orchestrationMarkers: 0,
            coordinatorIntents: 0,
            coordinatorReceipts: 0,
            coordinatorMarkers: 0,
          },
          failureKind,
        );
      }
    }),
  );

  it.effect(
    "rolls back real transaction writes for defects and interrupts at every coordinator phase",
    () =>
      Effect.gen(function* () {
        const checkpoints = [
          "afterMaterializingProjection",
          "afterOrchestrationMaterialization",
          "afterBoundProjection",
          "afterCoordinatorEvidence",
          "beforeAcceptedMarker",
        ] as const;
        for (const [index, checkpoint] of checkpoints.entries()) {
          const seeded = yield* seedCoordinatorReservation(`rollback-${checkpoint}`);
          const failure =
            index % 2 === 0 ? Effect.die(new Error(`defect-${checkpoint}`)) : Effect.interrupt;
          const coordinator = yield* buildCoordinator({
            hooks: {
              ...coordinatorNoopHooks,
              [checkpoint]: () => failure,
            },
          });
          const failed = yield* Effect.exit(coordinator.materializeInitial(seeded.command));
          assert.equal(failed._tag, "Failure", checkpoint);
          assert.deepStrictEqual(
            yield* coordinatorPersistenceCounts(
              seeded.command.controlledThreadReservationId,
              seeded.reservation.threadId,
            ),
            {
              reservationEvents: 1,
              reservationProjections: 1,
              orchestrationEvents: 0,
              threadProjections: 0,
              orchestrationIntents: 0,
              orchestrationReceipts: 0,
              orchestrationMarkers: 0,
              coordinatorIntents: 0,
              coordinatorReceipts: 0,
              coordinatorMarkers: 0,
            },
            checkpoint,
          );
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`
              SELECT status, revision
              FROM agent_control_controlled_thread_reservation_states
              WHERE controlled_thread_reservation_id =
                ${seeded.command.controlledThreadReservationId}
            `,
            [{ status: "prepared", revision: 1 }],
            checkpoint,
          );
          const retry =
            yield* (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
              seeded.command,
            );
          assert.equal(retry.status, "bound", checkpoint);
          assert.equal(retry.replayed, false, checkpoint);
        }
      }),
  );

  it.effect("completes both post-commit finalizations while preserving the original cause", () =>
    Effect.gen(function* () {
      const checkpoints = [
        { name: "after-commit-interrupt", hook: "afterOuterCommit", interrupt: true },
        { name: "after-commit-defect", hook: "afterOuterCommit", interrupt: false },
        {
          name: "reservation-finalization-interrupt",
          hook: "beforeReservationFinalization",
          interrupt: true,
        },
        {
          name: "reservation-finalization-defect",
          hook: "beforeReservationFinalization",
          interrupt: false,
        },
        {
          name: "between-finalizations-interrupt",
          hook: "beforeOrchestrationFinalization",
          interrupt: true,
        },
        {
          name: "between-finalizations-defect",
          hook: "beforeOrchestrationFinalization",
          interrupt: false,
        },
        {
          name: "orchestration-finalization-interrupt",
          hook: "afterOrchestrationFinalization",
          interrupt: true,
        },
        {
          name: "orchestration-finalization-defect",
          hook: "afterOrchestrationFinalization",
          interrupt: false,
        },
      ] as const;

      for (const checkpoint of checkpoints) {
        const seeded = yield* seedCoordinatorReservation(checkpoint.name);
        const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
        const orchestrationEngine = yield* OrchestrationEngineService;
        const reservationSubscriber = yield* reservationEngine.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const orchestrationSubscriber = yield* orchestrationEngine.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const armed = yield* Ref.make(true);
        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);
        const injected = Ref.getAndSet(armed, false).pipe(
          Effect.flatMap((shouldFail) =>
            shouldFail
              ? checkpoint.interrupt
                ? Effect.interrupt
                : Effect.die(new Error(`defect-${checkpoint.name}`))
              : Effect.void,
          ),
        );
        const coordinator = yield* buildCoordinator({
          hooks: {
            ...coordinatorNoopHooks,
            [checkpoint.hook]: () => injected,
          },
          reservationEngine: AgentControlControlledThreadReservationEngine.of({
            ...reservationEngine,
            publishCommitted: (events) =>
              Ref.update(reservationPublications, (count) => count + events.length).pipe(
                Effect.andThen(reservationEngine.publishCommitted(events)),
              ),
          }),
          orchestrationEngine: OrchestrationEngineService.of({
            ...orchestrationEngine,
            publishAgentControlMaterialization: (result) =>
              Ref.update(
                orchestrationPublications,
                (count) => count + result.committedEvents.length,
              ).pipe(
                Effect.andThen(orchestrationEngine.publishAgentControlMaterialization!(result)),
              ),
          }),
        });

        const first = yield* Effect.exit(coordinator.materializeInitial(seeded.command));
        assert.equal(first._tag, "Failure", checkpoint.name);
        if (Exit.isFailure(first)) {
          assert.equal(
            checkpoint.interrupt ? Cause.hasInterrupts(first.cause) : Cause.hasDies(first.cause),
            true,
            checkpoint.name,
          );
        }
        assert.equal(yield* Ref.get(reservationPublications), 2, checkpoint.name);
        assert.equal(yield* Ref.get(orchestrationPublications), 2, checkpoint.name);
        assert.equal((yield* Fiber.join(reservationSubscriber)).length, 2, checkpoint.name);
        assert.equal((yield* Fiber.join(orchestrationSubscriber)).length, 2, checkpoint.name);
        const sql = yield* SqlClient.SqlClient;
        const latestPersisted = (yield* sql<{ readonly sequence: number }>`
            SELECT max(sequence) AS sequence FROM orchestration_events
          `)[0]!.sequence;
        assert.equal(yield* orchestrationEngine.latestSequence, latestPersisted, checkpoint.name);
        const authoritative = yield* reservationEngine.getAuthoritative(
          seeded.command.controlledThreadReservationId,
        );
        assert.equal(Option.getOrThrow(authoritative).status, "bound", checkpoint.name);
        const committedCounts = yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        );
        assert.deepStrictEqual(
          committedCounts,
          {
            reservationEvents: 3,
            reservationProjections: 1,
            orchestrationEvents: 2,
            threadProjections: 1,
            orchestrationIntents: 1,
            orchestrationReceipts: 1,
            orchestrationMarkers: 1,
            coordinatorIntents: 1,
            coordinatorReceipts: 1,
            coordinatorMarkers: 1,
          },
          checkpoint.name,
        );

        const retry = yield* coordinator.materializeInitial(seeded.command);
        assert.equal(retry.replayed, true, checkpoint.name);
        assert.equal(yield* Ref.get(reservationPublications), 2, checkpoint.name);
        assert.equal(yield* Ref.get(orchestrationPublications), 2, checkpoint.name);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ),
          committedCounts,
          checkpoint.name,
        );
      }
    }),
  );

  it.effect(
    "recovers and publishes from a native post-COMMIT defect before transaction return",
    () =>
      Effect.gen(function* () {
        const seeded = yield* seedCoordinatorReservation("native-post-commit-defect");
        const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
        const orchestrationEngine = yield* OrchestrationEngineService;
        const reservationSubscriber = yield* reservationEngine.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const orchestrationSubscriber = yield* orchestrationEngine.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);
        const coordinator = yield* buildCoordinator({
          reservationEngine: AgentControlControlledThreadReservationEngine.of({
            ...reservationEngine,
            publishCommitted: (events) =>
              Ref.update(reservationPublications, (count) => count + events.length).pipe(
                Effect.andThen(reservationEngine.publishCommitted(events)),
              ),
          }),
          orchestrationEngine: OrchestrationEngineService.of({
            ...orchestrationEngine,
            publishAgentControlMaterialization: (result) =>
              Ref.update(
                orchestrationPublications,
                (count) => count + result.committedEvents.length,
              ).pipe(
                Effect.andThen(orchestrationEngine.publishAgentControlMaterialization!(result)),
              ),
          }),
        });
        const failed = yield* Effect.exit(
          coordinator.materializeInitial(seeded.command).pipe(
            Effect.provideService(NodeSqliteTransactionHooks, {
              afterCommitBeforeReturn: () =>
                Effect.die(new Error("native-post-commit-return-defect")),
            }),
          ),
        );
        assert.equal(failed._tag, "Failure");
        if (Exit.isFailure(failed)) {
          assert.include(Cause.pretty(failed.cause), "native-post-commit-return-defect");
        }
        assert.equal(yield* Ref.get(reservationPublications), 2);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        assert.equal((yield* Fiber.join(reservationSubscriber)).length, 2);
        assert.equal((yield* Fiber.join(orchestrationSubscriber)).length, 2);
        assert.equal(
          Option.getOrThrow(
            yield* reservationEngine.getAuthoritative(seeded.command.controlledThreadReservationId),
          ).status,
          "bound",
        );
        const sql = yield* SqlClient.SqlClient;
        assert.equal(
          yield* orchestrationEngine.latestSequence,
          (yield* sql<{ readonly sequence: number }>`
            SELECT max(sequence) AS sequence FROM orchestration_events
          `)[0]!.sequence,
        );
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ),
          {
            reservationEvents: 3,
            reservationProjections: 1,
            orchestrationEvents: 2,
            threadProjections: 1,
            orchestrationIntents: 1,
            orchestrationReceipts: 1,
            orchestrationMarkers: 1,
            coordinatorIntents: 1,
            coordinatorReceipts: 1,
            coordinatorMarkers: 1,
          },
        );
        yield* sql`SELECT 1`;
      }),
  );

  it.effect(
    "combines native post-COMMIT defects with recovery, refresh, and publication defects",
    () =>
      Effect.gen(function* () {
        const committedCounts = {
          reservationEvents: 3,
          reservationProjections: 1,
          orchestrationEvents: 2,
          threadProjections: 1,
          orchestrationIntents: 1,
          orchestrationReceipts: 1,
          orchestrationMarkers: 1,
          coordinatorIntents: 1,
          coordinatorReceipts: 1,
          coordinatorMarkers: 1,
        } as const;
        const nativeDefect = {
          afterCommitBeforeReturn: () => Effect.die(new Error("native-return-defect")),
        };

        const recoverySeed = yield* seedCoordinatorReservation("recovery-read-defect");
        const recoveryOrchestration = yield* OrchestrationEngineService;
        const recoveryCoordinator = yield* buildCoordinator({
          orchestrationEngine: OrchestrationEngineService.of({
            ...recoveryOrchestration,
            replayAgentControlMaterialization: () => Effect.die(new Error("recovery-read-defect")),
          }),
        });
        const recoveryExit = yield* Effect.exit(
          recoveryCoordinator
            .materializeInitial(recoverySeed.command)
            .pipe(Effect.provideService(NodeSqliteTransactionHooks, nativeDefect)),
        );
        assert.equal(recoveryExit._tag, "Failure");
        if (Exit.isFailure(recoveryExit)) {
          const rendered = Cause.pretty(recoveryExit.cause);
          assert.include(rendered, "native-return-defect");
          assert.include(rendered, "recovery-read-defect");
        }
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            recoverySeed.command.controlledThreadReservationId,
            recoverySeed.reservation.threadId,
          ),
          committedCounts,
        );

        const refreshSeed = yield* seedCoordinatorReservation("recovery-refresh-defects");
        const refreshReservation = yield* AgentControlControlledThreadReservationEngine;
        const refreshOrchestration = yield* OrchestrationEngineService;
        const refreshCoordinator = yield* buildCoordinator({
          reservationEngine: AgentControlControlledThreadReservationEngine.of({
            ...refreshReservation,
            refreshCommitted: () => Effect.die(new Error("reservation-refresh-defect")),
          }),
          orchestrationEngine: OrchestrationEngineService.of({
            ...refreshOrchestration,
            refreshAgentControlMaterialization: () =>
              Effect.die(new Error("orchestration-refresh-defect")),
          }),
        });
        const refreshExit = yield* Effect.exit(
          refreshCoordinator
            .materializeInitial(refreshSeed.command)
            .pipe(Effect.provideService(NodeSqliteTransactionHooks, nativeDefect)),
        );
        assert.equal(refreshExit._tag, "Failure");
        if (Exit.isFailure(refreshExit)) {
          const rendered = Cause.pretty(refreshExit.cause);
          assert.include(rendered, "native-return-defect");
          assert.include(rendered, "reservation-refresh-defect");
          assert.include(rendered, "orchestration-refresh-defect");
        }
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            refreshSeed.command.controlledThreadReservationId,
            refreshSeed.reservation.threadId,
          ),
          committedCounts,
        );

        const publicationSeed = yield* seedCoordinatorReservation("recovery-publication-defects");
        const publicationReservation = yield* AgentControlControlledThreadReservationEngine;
        const publicationOrchestration = yield* OrchestrationEngineService;
        const publicationCoordinator = yield* buildCoordinator({
          reservationEngine: AgentControlControlledThreadReservationEngine.of({
            ...publicationReservation,
            publishCommitted: () => Effect.die(new Error("reservation-publication-defect")),
          }),
          orchestrationEngine: OrchestrationEngineService.of({
            ...publicationOrchestration,
            publishAgentControlMaterialization: () =>
              Effect.die(new Error("orchestration-publication-defect")),
          }),
        });
        const publicationExit = yield* Effect.exit(
          publicationCoordinator
            .materializeInitial(publicationSeed.command)
            .pipe(Effect.provideService(NodeSqliteTransactionHooks, nativeDefect)),
        );
        assert.equal(publicationExit._tag, "Failure");
        if (Exit.isFailure(publicationExit)) {
          const rendered = Cause.pretty(publicationExit.cause);
          assert.include(rendered, "native-return-defect");
          assert.include(rendered, "reservation-publication-defect");
          assert.include(rendered, "orchestration-publication-defect");
        }
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            publicationSeed.command.controlledThreadReservationId,
            publicationSeed.reservation.threadId,
          ),
          committedCounts,
        );
      }),
  );

  it.effect(
    "rolls back coordinator projection, intent, receipt, marker, and outer commit failures",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const failures = [
          {
            suffix: "projection",
            table: "agent_control_controlled_thread_reservation_states",
            timing: "BEFORE UPDATE",
            when: "WHEN NEW.status = 'bound'",
          },
          {
            suffix: "intent",
            table: "agent_control_controlled_thread_materialization_intents",
            timing: "BEFORE INSERT",
            when: "",
          },
          {
            suffix: "receipt",
            table: "agent_control_controlled_thread_materialization_receipts",
            timing: "BEFORE INSERT",
            when: "",
          },
          {
            suffix: "marker",
            table: "agent_control_controlled_thread_materialization_accepted",
            timing: "BEFORE INSERT",
            when: "",
          },
        ] as const;
        for (const failure of failures) {
          const seeded = yield* seedCoordinatorReservation(`evidence-failure-${failure.suffix}`);
          const trigger = `test_coordinator_${failure.suffix}_failure`;
          yield* sql.unsafe(
            `CREATE TRIGGER ${trigger} ${failure.timing} ON ${failure.table}
               ${failure.when}
               BEGIN SELECT RAISE(ABORT, 'injected coordinator failure'); END`,
          ).unprepared;
          const failed = yield* Effect.result(
            (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
              seeded.command,
            ),
          );
          assert.equal(failed._tag, "Failure", failure.suffix);
          assert.deepStrictEqual(
            yield* coordinatorPersistenceCounts(
              seeded.command.controlledThreadReservationId,
              seeded.reservation.threadId,
            ),
            {
              reservationEvents: 1,
              reservationProjections: 1,
              orchestrationEvents: 0,
              threadProjections: 0,
              orchestrationIntents: 0,
              orchestrationReceipts: 0,
              orchestrationMarkers: 0,
              coordinatorIntents: 0,
              coordinatorReceipts: 0,
              coordinatorMarkers: 0,
            },
            failure.suffix,
          );
          yield* sql.unsafe(`DROP TRIGGER ${trigger}`).unprepared;
          const retry =
            yield* (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
              seeded.command,
            );
          assert.equal(retry.status, "bound", failure.suffix);
        }

        const commitFailure = yield* seedCoordinatorReservation("outer-commit-failure");
        yield* sql`
          CREATE TABLE test_coordinator_commit_parent (
            id TEXT PRIMARY KEY
          )
        `;
        yield* sql`
          CREATE TABLE test_coordinator_commit_child (
            id TEXT PRIMARY KEY,
            parent_id TEXT NOT NULL,
            FOREIGN KEY (parent_id)
              REFERENCES test_coordinator_commit_parent(id)
              DEFERRABLE INITIALLY DEFERRED
          )
        `;
        const commitFailingCoordinator = yield* buildCoordinator({
          hooks: {
            ...coordinatorNoopHooks,
            beforeAcceptedMarker: () =>
              sql`
                INSERT INTO test_coordinator_commit_child (id, parent_id)
                VALUES ('child', 'missing-parent')
              `.pipe(Effect.orDie),
          },
        });
        const failedCommit = yield* Effect.exit(
          commitFailingCoordinator.materializeInitial(commitFailure.command),
        );
        assert.equal(failedCommit._tag, "Failure");
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            commitFailure.command.controlledThreadReservationId,
            commitFailure.reservation.threadId,
          ),
          {
            reservationEvents: 1,
            reservationProjections: 1,
            orchestrationEvents: 0,
            threadProjections: 0,
            orchestrationIntents: 0,
            orchestrationReceipts: 0,
            orchestrationMarkers: 0,
            coordinatorIntents: 0,
            coordinatorReceipts: 0,
            coordinatorMarkers: 0,
          },
        );
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count FROM test_coordinator_commit_child`,
          [{ count: 0 }],
        );
        const retryAfterCommitFailure =
          yield* (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
            commitFailure.command,
          );
        assert.equal(retryAfterCommitFailure.status, "bound");
      }),
  );

  it.effect("replays a response lost after outer commit without writes or current authority", () =>
    Effect.gen(function* () {
      const seeded = yield* seedCoordinatorReservation("response-loss");
      const failing = yield* buildCoordinator({
        hooks: {
          ...coordinatorNoopHooks,
          afterOuterCommit: () => Effect.die(new Error("response lost after coordinator commit")),
        },
      });
      const lost = yield* Effect.exit(failing.materializeInitial(seeded.command));
      assert.equal(lost._tag, "Failure");
      const committedCounts = yield* coordinatorPersistenceCounts(
        seeded.command.controlledThreadReservationId,
        seeded.reservation.threadId,
      );
      assert.deepStrictEqual(committedCounts, {
        reservationEvents: 3,
        reservationProjections: 1,
        orchestrationEvents: 2,
        threadProjections: 1,
        orchestrationIntents: 1,
        orchestrationReceipts: 1,
        orchestrationMarkers: 1,
        coordinatorIntents: 1,
        coordinatorReceipts: 1,
        coordinatorMarkers: 1,
      });
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
          UPDATE projection_projects
          SET deleted_at = '2026-07-27T11:30:00.000Z'
          WHERE project_id = ${seeded.projectId}
        `;
      const restarted = yield* buildCoordinator();
      const replay = yield* restarted.materializeInitial(seeded.command);
      assert.equal(replay.replayed, true);
      assert.deepStrictEqual(
        yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        ),
        committedCounts,
      );
    }),
  );

  it.effect(
    "fails closed on corrupt coordinator intent, receipt, marker, or reservation projection replay",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_intents_no_update
        `;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_receipts_no_update
        `;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_materialization_accepted_no_update
        `;
        yield* sql`
          DROP TRIGGER
            agent_control_controlled_thread_projection_validate_update
        `;
        const coordinator = yield* AgentControlControlledThreadMaterializationCoordinator;

        const intent = yield* seedCoordinatorReservation("corrupt-coordinator-intent");
        yield* coordinator.materializeInitial(intent.command);
        yield* sql.withTransaction(sql`
          UPDATE agent_control_controlled_thread_materialization_intents
          SET title = 'Corrupt historical title'
          WHERE coordinator_command_id = ${intent.command.commandId}
        `);

        const receipt = yield* seedCoordinatorReservation("corrupt-coordinator-receipt");
        yield* coordinator.materializeInitial(receipt.command);
        yield* sql.withTransaction(sql`
          UPDATE agent_control_controlled_thread_materialization_receipts
          SET request_fingerprint = ${"f".repeat(64)}
          WHERE coordinator_command_id = ${receipt.command.commandId}
        `);

        const marker = yield* seedCoordinatorReservation("corrupt-coordinator-marker");
        yield* coordinator.materializeInitial(marker.command);
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql.withTransaction(sql`
          UPDATE agent_control_controlled_thread_materialization_accepted
          SET coordinator_command_fingerprint = ${"e".repeat(64)}
          WHERE coordinator_command_id = ${marker.command.commandId}
        `);
        yield* sql`PRAGMA foreign_keys = ON`;

        const owner = yield* seedCoordinatorReservation("corrupt-coordinator-owner");
        yield* coordinator.materializeInitial(owner.command);
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql.withTransaction(sql`
          UPDATE agent_control_controlled_thread_materialization_accepted
          SET finalization_owner_id = '00000000-0000-4000-8000-000000000000'
          WHERE coordinator_command_id = ${owner.command.commandId}
        `);
        yield* sql`PRAGMA foreign_keys = ON`;

        const projection = yield* seedCoordinatorReservation("corrupt-coordinator-projection");
        yield* coordinator.materializeInitial(projection.command);
        yield* sql`
          UPDATE agent_control_controlled_thread_reservation_states
          SET coordinator_command_fingerprint = ${"d".repeat(64)},
              state_json = json_set(
                state_json,
                '$.coordinatorCommandFingerprint',
                ${"d".repeat(64)}
              )
          WHERE controlled_thread_reservation_id =
            ${projection.command.controlledThreadReservationId}
        `;

        for (const seeded of [intent, receipt, marker, owner, projection]) {
          const replay = yield* Effect.result(coordinator.materializeInitial(seeded.command));
          assert.equal(replay._tag, "Failure");
          if (replay._tag === "Failure") {
            assert.equal(replay.failure.reason, "historical-evidence-corrupt");
          }
        }
      }),
  );

  it.effect("fails closed when each authoritative lifecycle coordinate changes before commit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const cases = [
        {
          name: "project-delete",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE projection_projects
                SET deleted_at = '2026-07-27T12:00:00.000Z'
                WHERE project_id = ${seeded.projectId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE projection_projects SET deleted_at = NULL
                WHERE project_id = ${seeded.projectId}
              `,
        },
        {
          name: "mode-switch",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_project_states SET mode = 'off'
                WHERE project_id = ${seeded.projectId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_project_states SET mode = 'observe'
                WHERE project_id = ${seeded.projectId}
              `,
        },
        {
          name: "task-revision",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_task_states
                SET revision = revision + 1,
                  state_json = json_set(state_json, '$.revision', revision + 1)
                WHERE task_id = ${seeded.task.taskId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_task_states
                SET revision = ${seeded.task.revision},
                  state_json = json_set(
                    state_json, '$.revision', ${seeded.task.revision}
                  )
                WHERE task_id = ${seeded.task.taskId}
              `,
        },
        {
          name: "stage-attempt-binding",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_states
                SET attempt_id = 'foreign-attempt',
                  state_json = json_set(
                    state_json, '$.attemptId', 'foreign-attempt'
                  )
                WHERE stage_run_id = ${seeded.stageRun.stageRunId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_states
                SET attempt_id = ${seeded.stageRun.attemptId},
                  state_json = json_set(
                    state_json, '$.attemptId', ${seeded.stageRun.attemptId}
                  )
                WHERE stage_run_id = ${seeded.stageRun.stageRunId}
              `,
        },
        {
          name: "lease-holder",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET holder_id = 'foreign-holder',
                  state_json = json_set(
                    state_json, '$.holderId', 'foreign-holder'
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET holder_id = ${seeded.lease.holderId},
                  state_json = json_set(
                    state_json, '$.holderId', ${seeded.lease.holderId}
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
        },
        {
          name: "lease-expiry",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET expires_at = '2020-01-01T00:00:00.000Z',
                  state_json = json_set(
                    state_json, '$.expiresAt', '2020-01-01T00:00:00.000Z'
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET expires_at = ${seeded.lease.expiresAt},
                  state_json = json_set(
                    state_json, '$.expiresAt', ${seeded.lease.expiresAt}
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
        },
        {
          name: "lease-fence",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET fence_token = fence_token + 1,
                  state_json = json_set(
                    state_json, '$.fenceToken', fence_token + 1
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_stage_run_lease_states
                SET fence_token = ${seeded.lease.fenceToken},
                  state_json = json_set(
                    state_json, '$.fenceToken', ${seeded.lease.fenceToken}
                  )
                WHERE lease_id = ${seeded.lease.leaseId}
              `,
        },
        {
          name: "worktree-binding",
          mutate: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_worktree_reservation_states
                SET task_revision = task_revision + 1,
                  state_json = json_set(
                    state_json, '$.taskRevision', task_revision + 1
                  )
                WHERE reservation_id = ${seeded.ready.reservationId}
              `,
          restore: (seeded: Effect.Success<ReturnType<typeof seedCoordinatorReservation>>) =>
            sql`
                UPDATE agent_control_worktree_reservation_states
                SET task_revision = ${seeded.ready.taskRevision},
                  state_json = json_set(
                    state_json, '$.taskRevision', ${seeded.ready.taskRevision}
                  )
                WHERE reservation_id = ${seeded.ready.reservationId}
              `,
        },
      ] as const;

      for (const lifecycle of cases) {
        const seeded = yield* seedCoordinatorReservation(`authority-${lifecycle.name}`);
        const coordinator = yield* buildCoordinator({
          hooks: {
            ...coordinatorNoopHooks,
            beforeTransactionAdmission: () =>
              lifecycle.mutate(seeded).pipe(Effect.asVoid, Effect.orDie),
          },
        });
        const rejected = yield* Effect.exit(coordinator.materializeInitial(seeded.command));
        assert.equal(rejected._tag, "Failure", lifecycle.name);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ),
          {
            reservationEvents: 1,
            reservationProjections: 1,
            orchestrationEvents: 0,
            threadProjections: 0,
            orchestrationIntents: 0,
            orchestrationReceipts: 0,
            orchestrationMarkers: 0,
            coordinatorIntents: 0,
            coordinatorReceipts: 0,
            coordinatorMarkers: 0,
          },
          lifecycle.name,
        );
        yield* lifecycle.restore(seeded);
        const retry =
          yield* (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
            seeded.command,
          );
        assert.equal(retry.status, "bound", lifecycle.name);
      }

      const policySeed = yield* seedCoordinatorReservation("authority-policy");
      const policyCoordinator = yield* buildCoordinator({
        hooks: {
          ...coordinatorNoopHooks,
          beforeTransactionAdmission: () =>
            sql`
              INSERT INTO agent_control_project_policies (
                project_id, policy_json, revision, updated_at
              ) VALUES (
                ${policySeed.projectId}, '{"fullAccess":true}', 1,
                '2026-07-27T12:30:00.000Z'
              )
            `.pipe(Effect.asVoid, Effect.orDie),
        },
      });
      const policyRejected = yield* Effect.exit(
        policyCoordinator.materializeInitial(policySeed.command),
      );
      assert.equal(policyRejected._tag, "Failure");
      assert.deepStrictEqual(
        yield* coordinatorPersistenceCounts(
          policySeed.command.controlledThreadReservationId,
          policySeed.reservation.threadId,
        ),
        {
          reservationEvents: 1,
          reservationProjections: 1,
          orchestrationEvents: 0,
          threadProjections: 0,
          orchestrationIntents: 0,
          orchestrationReceipts: 0,
          orchestrationMarkers: 0,
          coordinatorIntents: 0,
          coordinatorReceipts: 0,
          coordinatorMarkers: 0,
        },
      );
      yield* sql`
        DELETE FROM agent_control_project_policies
        WHERE project_id = ${policySeed.projectId}
      `;
      const policyRetry =
        yield* (yield* AgentControlControlledThreadMaterializationCoordinator).materializeInitial(
          policySeed.command,
        );
      assert.equal(policyRetry.status, "bound");
    }),
  );

  it.effect(
    "finalizes the sole owner across the real WAL post-COMMIT interrupt window",
    () =>
      Effect.gen(function* () {
        const contexts = yield* makeIndependentControllerContexts();
        const baseA = contexts.contextA;
        const baseB = Context.add(
          contexts.controllerDependenciesB,
          AgentControlWorktreeController,
          contexts.controllerB,
        );
        const reservationContextA = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(baseA)),
          ),
          contexts.scopeA,
        );
        const reservationContextB = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(baseB)),
          ),
          contexts.scopeB,
        );
        const seeded = yield* seedCoordinatorReservation("wal-post-commit-interrupt").pipe(
          Effect.provide(Context.merge(baseA, reservationContextA)),
        );
        const orchestrationContextA = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, contexts.sqlA)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          contexts.scopeA,
        );
        const orchestrationContextB = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, contexts.sqlB)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          contexts.scopeB,
        );
        const policyContextA = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          contexts.scopeA,
        );
        const policyContextB = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          contexts.scopeB,
        );
        const dependenciesA = Context.merge(
          Context.merge(baseA, reservationContextA),
          Context.merge(orchestrationContextA, policyContextA),
        );
        const dependenciesB = Context.merge(
          Context.merge(baseB, reservationContextB),
          Context.merge(orchestrationContextB, policyContextB),
        );
        const reservationEngineA = Context.get(
          dependenciesA,
          AgentControlControlledThreadReservationEngine,
        );
        const reservationEngineB = Context.get(
          dependenciesB,
          AgentControlControlledThreadReservationEngine,
        );
        const orchestrationEngineA = Context.get(dependenciesA, OrchestrationEngineService);
        const orchestrationEngineB = Context.get(dependenciesB, OrchestrationEngineService);
        const reservationSubscriber = yield* reservationEngineA.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const orchestrationSubscriber = yield* orchestrationEngineA.streamDomainEvents.pipe(
          Stream.take(2),
          Stream.runCollect,
          Effect.forkChild({ startImmediately: true }),
        );
        const decisionsA = yield* Ref.make(0);
        const decisionsB = yield* Ref.make(0);
        const reservationPublicationsA = yield* Ref.make(0);
        const reservationPublicationsB = yield* Ref.make(0);
        const orchestrationPublicationsA = yield* Ref.make(0);
        const orchestrationPublicationsB = yield* Ref.make(0);

        const buildWalCoordinator = Effect.fn("buildPostCommitWalCoordinator")(function* (
          dependencies: typeof dependenciesA,
          scope: Scope.Closeable,
          hooks: AgentControlControlledThreadMaterializationCoordinatorHooksShape,
          reservationEngine: AgentControlControlledThreadReservationEngineShape,
          orchestrationEngine: OrchestrationEngineShape,
          reservationPublications: Ref.Ref<number>,
          orchestrationPublications: Ref.Ref<number>,
        ) {
          const context = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlControlledThreadMaterializationCoordinatorLive).pipe(
              Layer.provide(
                Layer.succeedContext(
                  Context.add(
                    Context.add(
                      Context.add(
                        dependencies,
                        AgentControlControlledThreadMaterializationCoordinatorHooks,
                        hooks,
                      ),
                      AgentControlControlledThreadReservationEngine,
                      AgentControlControlledThreadReservationEngine.of({
                        ...reservationEngine,
                        publishCommitted: (events) =>
                          Ref.update(
                            reservationPublications,
                            (count) => count + events.length,
                          ).pipe(Effect.andThen(reservationEngine.publishCommitted(events))),
                      }),
                    ),
                    OrchestrationEngineService,
                    OrchestrationEngineService.of({
                      ...orchestrationEngine,
                      publishAgentControlMaterialization: (result) =>
                        Ref.update(
                          orchestrationPublications,
                          (count) => count + result.committedEvents.length,
                        ).pipe(
                          Effect.andThen(
                            orchestrationEngine.publishAgentControlMaterialization!(result),
                          ),
                        ),
                    }),
                  ),
                ),
              ),
            ),
            scope,
          );
          return Context.get(context, AgentControlControlledThreadMaterializationCoordinator);
        });
        const coordinatorA = yield* buildWalCoordinator(
          dependenciesA,
          contexts.scopeA,
          {
            ...coordinatorNoopHooks,
            afterMaterializingProjection: () => Ref.update(decisionsA, (count) => count + 1),
          },
          reservationEngineA,
          orchestrationEngineA,
          reservationPublicationsA,
          orchestrationPublicationsA,
        );
        const coordinatorB = yield* buildWalCoordinator(
          dependenciesB as typeof dependenciesA,
          contexts.scopeB,
          {
            ...coordinatorNoopHooks,
            afterMaterializingProjection: () => Ref.update(decisionsB, (count) => count + 1),
          },
          reservationEngineB,
          orchestrationEngineB,
          reservationPublicationsB,
          orchestrationPublicationsB,
        );

        const committedAtDriver = yield* Deferred.make<void>();
        const releaseDriver = yield* Deferred.make<void>();
        const callerA = yield* coordinatorA.materializeInitial(seeded.command).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: () =>
              Deferred.succeed(committedAtDriver, undefined).pipe(
                Effect.andThen(Deferred.await(releaseDriver)),
              ),
          }),
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.addFinalizer(() => Fiber.interrupt(callerA).pipe(Effect.asVoid));
        yield* Deferred.await(committedAtDriver);

        const committedCounts = yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        ).pipe(Effect.provideService(SqlClient.SqlClient, contexts.sqlB));
        assert.deepStrictEqual(committedCounts, {
          reservationEvents: 3,
          reservationProjections: 1,
          orchestrationEvents: 2,
          threadProjections: 1,
          orchestrationIntents: 1,
          orchestrationReceipts: 1,
          orchestrationMarkers: 1,
          coordinatorIntents: 1,
          coordinatorReceipts: 1,
          coordinatorMarkers: 1,
        });

        const resultB = yield* coordinatorB.materializeInitial(seeded.command);
        assert.equal(resultB.replayed, true);
        assert.equal(yield* Ref.get(reservationPublicationsB), 0);
        assert.equal(yield* Ref.get(orchestrationPublicationsB), 0);
        assert.equal(yield* Ref.get(decisionsB), 0);
        assert.equal(
          Option.getOrThrow(
            yield* reservationEngineB.getAuthoritative(
              seeded.command.controlledThreadReservationId,
            ),
          ).status,
          "bound",
        );
        assert.equal(
          yield* orchestrationEngineB.latestSequence,
          resultB.orchestrationResultSequence,
        );

        const interruption = yield* Fiber.interrupt(callerA).pipe(
          Effect.forkChild({ startImmediately: true }),
        );
        yield* Effect.yieldNow;
        yield* Deferred.succeed(releaseDriver, undefined);
        const callerAExit = yield* Fiber.await(callerA);
        yield* Fiber.join(interruption);
        assert.equal(Exit.hasInterrupts(callerAExit), true);
        assert.equal(yield* Ref.get(decisionsA), 1);
        assert.equal(yield* Ref.get(reservationPublicationsA), 2);
        assert.equal(yield* Ref.get(orchestrationPublicationsA), 2);
        assert.equal((yield* Fiber.join(reservationSubscriber)).length, 2);
        assert.equal((yield* Fiber.join(orchestrationSubscriber)).length, 2);
        assert.equal(
          Option.getOrThrow(
            yield* reservationEngineA.getAuthoritative(
              seeded.command.controlledThreadReservationId,
            ),
          ).status,
          "bound",
        );
        assert.equal(
          yield* orchestrationEngineA.latestSequence,
          resultB.orchestrationResultSequence,
        );
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, contexts.sqlA)),
          committedCounts,
        );
        assert.deepStrictEqual(yield* contexts.sqlA`SELECT 1 AS usable`, [{ usable: 1 }]);
        assert.deepStrictEqual(yield* contexts.sqlB`SELECT 1 AS usable`, [{ usable: 1 }]);

        const loserSeed = yield* seedCoordinatorReservation("wal-loser-pre-commit-interrupt").pipe(
          Effect.provide(Context.merge(baseA, reservationContextA)),
        );
        const loserResolved = yield* Deferred.make<void>();
        const holdLoser = yield* Deferred.make<void>();
        const interruptedLoserCoordinator = yield* buildWalCoordinator(
          dependenciesB as typeof dependenciesA,
          contexts.scopeB,
          {
            ...coordinatorNoopHooks,
            afterAuthoritativeResolution: () =>
              Deferred.succeed(loserResolved, undefined).pipe(
                Effect.andThen(Deferred.await(holdLoser)),
              ),
            afterMaterializingProjection: () => Ref.update(decisionsB, (count) => count + 1),
          },
          reservationEngineB,
          orchestrationEngineB,
          reservationPublicationsB,
          orchestrationPublicationsB,
        );
        const loser = yield* interruptedLoserCoordinator
          .materializeInitial(loserSeed.command)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(loserResolved);
        const secondWinner = yield* coordinatorA.materializeInitial(loserSeed.command);
        assert.equal(secondWinner.replayed, false);
        yield* Fiber.interrupt(loser);
        assert.equal(Exit.hasInterrupts(yield* Fiber.await(loser)), true);
        assert.equal(yield* Ref.get(decisionsA), 2);
        assert.equal(yield* Ref.get(decisionsB), 0);
        assert.equal(yield* Ref.get(reservationPublicationsA), 4);
        assert.equal(yield* Ref.get(orchestrationPublicationsA), 4);
        assert.equal(yield* Ref.get(reservationPublicationsB), 0);
        assert.equal(yield* Ref.get(orchestrationPublicationsB), 0);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            loserSeed.command.controlledThreadReservationId,
            loserSeed.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, contexts.sqlA)),
          committedCounts,
        );
      }),
    30_000,
  );

  it.effect(
    "converges two original callers across independent WAL connections without repeated Git or publication",
    () =>
      Effect.gen(function* () {
        const inspectionsA = yield* Ref.make(0);
        const inspectionsB = yield* Ref.make(0);
        const contexts = yield* makeIndependentControllerContexts(
          {
            afterReadyInspection: () => Ref.update(inspectionsA, (count) => count + 1),
          },
          {
            afterReadyInspection: () => Ref.update(inspectionsB, (count) => count + 1),
          },
        );
        const baseA = contexts.contextA;
        const baseB = Context.add(
          contexts.controllerDependenciesB,
          AgentControlWorktreeController,
          contexts.controllerB,
        );
        const reservationContextA = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(baseA)),
          ),
          contexts.scopeA,
        );
        const reservationContextB = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
            Layer.provide(Layer.succeedContext(baseB)),
          ),
          contexts.scopeB,
        );
        const seeded = yield* seedCoordinatorReservation("wal-race").pipe(
          Effect.provide(Context.merge(baseA, reservationContextA)),
        );
        yield* Ref.set(inspectionsA, 0);
        yield* Ref.set(inspectionsB, 0);

        const orchestrationContextA = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, contexts.sqlA)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          contexts.scopeA,
        );
        const orchestrationContextB = yield* Layer.buildWithScope(
          Layer.fresh(OrchestrationLayerLive).pipe(
            Layer.provideMerge(Layer.succeed(SqlClient.SqlClient, contexts.sqlB)),
            Layer.provideMerge(RepositoryIdentityResolver.layer),
            Layer.provideMerge(configLayer),
            Layer.provideMerge(NodeServices.layer),
          ),
          contexts.scopeB,
        );
        const policyContextA = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          contexts.scopeA,
        );
        const policyContextB = yield* Layer.buildWithScope(
          Layer.fresh(coordinatorPolicyLayer),
          contexts.scopeB,
        );
        const dependenciesA = Context.merge(
          Context.merge(baseA, reservationContextA),
          Context.merge(orchestrationContextA, policyContextA),
        );
        const dependenciesB = Context.merge(
          Context.merge(baseB, reservationContextB),
          Context.merge(orchestrationContextB, policyContextB),
        );
        const loserResolved = yield* Deferred.make<void>();
        const releaseLoser = yield* Deferred.make<void>();
        const decisionsA = yield* Ref.make(0);
        const decisionsB = yield* Ref.make(0);
        const reservationPublications = yield* Ref.make(0);
        const orchestrationPublications = yield* Ref.make(0);

        const buildWalCoordinator = Effect.fn("buildWalCoordinator")(function* (
          dependencies: typeof dependenciesA,
          scope: Scope.Closeable,
          hooks: AgentControlControlledThreadMaterializationCoordinatorHooksShape,
        ) {
          const reservationEngine = Context.get(
            dependencies,
            AgentControlControlledThreadReservationEngine,
          );
          const orchestrationEngine = Context.get(dependencies, OrchestrationEngineService);
          const coordinatorDependencies = Context.add(
            Context.add(
              Context.add(
                dependencies,
                AgentControlControlledThreadMaterializationCoordinatorHooks,
                hooks,
              ),
              AgentControlControlledThreadReservationEngine,
              AgentControlControlledThreadReservationEngine.of({
                ...reservationEngine,
                publishCommitted: (events) =>
                  Ref.update(reservationPublications, (count) => count + events.length).pipe(
                    Effect.andThen(reservationEngine.publishCommitted(events)),
                  ),
              }),
            ),
            OrchestrationEngineService,
            OrchestrationEngineService.of({
              ...orchestrationEngine,
              publishAgentControlMaterialization: (result) =>
                Ref.update(
                  orchestrationPublications,
                  (count) => count + result.committedEvents.length,
                ).pipe(
                  Effect.andThen(orchestrationEngine.publishAgentControlMaterialization!(result)),
                ),
            }),
          );
          const context = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlControlledThreadMaterializationCoordinatorLive).pipe(
              Layer.provide(Layer.succeedContext(coordinatorDependencies)),
            ),
            scope,
          );
          return Context.get(context, AgentControlControlledThreadMaterializationCoordinator);
        });
        const coordinatorA = yield* buildWalCoordinator(dependenciesA, contexts.scopeA, {
          ...coordinatorNoopHooks,
          afterMaterializingProjection: () => Ref.update(decisionsA, (count) => count + 1),
        });
        const coordinatorB = yield* buildWalCoordinator(
          dependenciesB as typeof dependenciesA,
          contexts.scopeB,
          {
            ...coordinatorNoopHooks,
            afterAuthoritativeResolution: () =>
              Deferred.succeed(loserResolved, undefined).pipe(
                Effect.andThen(Deferred.await(releaseLoser)),
              ),
            afterMaterializingProjection: () => Ref.update(decisionsB, (count) => count + 1),
          },
        );

        const callerB = yield* coordinatorB
          .materializeInitial(seeded.command)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.addFinalizer(() => Fiber.interrupt(callerB).pipe(Effect.asVoid));
        yield* Deferred.await(loserResolved);
        const readWhileWaiting = yield* contexts.sqlB<{
          readonly journal_mode: string;
        }>`
          PRAGMA journal_mode
        `;
        assert.deepStrictEqual(readWhileWaiting, [{ journal_mode: "wal" }]);
        const callerA = yield* coordinatorA
          .materializeInitial(seeded.command)
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.addFinalizer(() => Fiber.interrupt(callerA).pipe(Effect.asVoid));
        const resultA = yield* Fiber.join(callerA);
        yield* Deferred.succeed(releaseLoser, undefined);
        const resultB = yield* Fiber.join(callerB);
        assert.equal(resultA.status, "bound");
        assert.equal(resultB.status, "bound");
        assert.equal(resultA.threadId, resultB.threadId);
        assert.equal(resultA.orchestrationResultSequence, resultB.orchestrationResultSequence);
        assert.deepStrictEqual(
          yield* coordinatorPersistenceCounts(
            seeded.command.controlledThreadReservationId,
            seeded.reservation.threadId,
          ).pipe(Effect.provideService(SqlClient.SqlClient, contexts.sqlA)),
          {
            reservationEvents: 3,
            reservationProjections: 1,
            orchestrationEvents: 2,
            threadProjections: 1,
            orchestrationIntents: 1,
            orchestrationReceipts: 1,
            orchestrationMarkers: 1,
            coordinatorIntents: 1,
            coordinatorReceipts: 1,
            coordinatorMarkers: 1,
          },
        );
        assert.equal(yield* Ref.get(decisionsA), 1);
        assert.equal(yield* Ref.get(decisionsB), 0);
        assert.equal(yield* Ref.get(inspectionsA), 1);
        assert.equal(yield* Ref.get(inspectionsB), 0);
        assert.equal(yield* Ref.get(reservationPublications), 2);
        assert.equal(yield* Ref.get(orchestrationPublications), 2);
        assert.equal(
          yield* Context.get(dependenciesB, OrchestrationEngineService).latestSequence,
          resultB.orchestrationResultSequence,
        );
        assert.equal(
          Option.getOrThrow(
            yield* Context.get(
              dependenciesB,
              AgentControlControlledThreadReservationEngine,
            ).getAuthoritative(seeded.command.controlledThreadReservationId),
          ).status,
          "bound",
        );
      }),
    30_000,
  );

  it.effect("replays response loss after complete finalization without republishing", () =>
    Effect.gen(function* () {
      const seeded = yield* seedCoordinatorReservation("response-loss-after-publication");
      const reservationEngine = yield* AgentControlControlledThreadReservationEngine;
      const orchestrationEngine = yield* OrchestrationEngineService;
      const reservationPublications = yield* Ref.make(0);
      const orchestrationPublications = yield* Ref.make(0);
      const coordinator = yield* buildCoordinator({
        hooks: {
          ...coordinatorNoopHooks,
          afterPublication: () => Effect.die(new Error("response-lost-after-publication")),
        },
        reservationEngine: AgentControlControlledThreadReservationEngine.of({
          ...reservationEngine,
          publishCommitted: (events) =>
            Ref.update(reservationPublications, (count) => count + events.length).pipe(
              Effect.andThen(reservationEngine.publishCommitted(events)),
            ),
        }),
        orchestrationEngine: OrchestrationEngineService.of({
          ...orchestrationEngine,
          publishAgentControlMaterialization: (result) =>
            Ref.update(
              orchestrationPublications,
              (count) => count + result.committedEvents.length,
            ).pipe(Effect.andThen(orchestrationEngine.publishAgentControlMaterialization!(result))),
        }),
      });
      const lost = yield* Effect.exit(coordinator.materializeInitial(seeded.command));
      assert.equal(lost._tag, "Failure");
      if (Exit.isFailure(lost)) {
        assert.include(Cause.pretty(lost.cause), "response-lost-after-publication");
      }
      assert.equal(yield* Ref.get(reservationPublications), 2);
      assert.equal(yield* Ref.get(orchestrationPublications), 2);
      const committedCounts = yield* coordinatorPersistenceCounts(
        seeded.command.controlledThreadReservationId,
        seeded.reservation.threadId,
      );

      const replay = yield* coordinator.materializeInitial(seeded.command);
      assert.equal(replay.replayed, true);
      assert.equal(yield* Ref.get(reservationPublications), 2);
      assert.equal(yield* Ref.get(orchestrationPublications), 2);
      assert.deepStrictEqual(
        yield* coordinatorPersistenceCounts(
          seeded.command.controlledThreadReservationId,
          seeded.reservation.threadId,
        ),
        committedCounts,
      );
    }),
  );
});

layer("Agent Control worktree materialization", (it) => {
  it.effect(
    "keeps a consistently forged task identity receiptless before controlled-thread admission",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-forged-task-identity");
        const seeded = yield* seedPrepared(projectId, repo.cwd);
        const forgedTaskId = AgentControlTaskId.make("controlled-thread-forged-task");
        yield* sql`
          UPDATE agent_control_events
          SET stream_id = ${forgedTaskId},
              payload_json = json_set(payload_json, '$.taskId', ${forgedTaskId})
          WHERE aggregate_kind = 'task'
            AND stream_id = ${seeded.task.taskId}
        `;
        yield* sql`
          UPDATE agent_control_task_states
          SET task_id = ${forgedTaskId},
              state_json = json_set(state_json, '$.taskId', ${forgedTaskId})
          WHERE task_id = ${seeded.task.taskId}
        `;

        const engine = yield* AgentControlControlledThreadReservationEngine;
        const publications = yield* Ref.make(0);
        const publicationFiber = yield* engine.streamDomainEvents.pipe(
          Stream.runForEach(() => Ref.update(publications, (count) => count + 1)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const result = yield* Effect.result(
          (yield* AgentControlControlledThreadReservation).prepareInitial({
            commandId: CommandId.make("controlled-thread-forged-task-prepare"),
            projectId,
            taskId: forgedTaskId,
          }),
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "task-projection-corrupt");
        }
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT COUNT(*) FROM agent_control_events
                WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
              (SELECT COUNT(*)
                FROM agent_control_controlled_thread_stream_catalog) AS catalogs,
              (SELECT COUNT(*)
                FROM agent_control_controlled_thread_reservation_states) AS projections,
              (SELECT COUNT(*)
                FROM agent_control_controlled_thread_command_intents) AS intents,
              (SELECT COUNT(*) FROM agent_control_command_receipts
                WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts
          `,
          [{ events: 0, catalogs: 0, projections: 0, intents: 0, receipts: 0 }],
        );
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(publications), 0);
        yield* Fiber.interrupt(publicationFiber);
        yield* sql`
          UPDATE agent_control_events
          SET stream_id = ${seeded.task.taskId},
              payload_json = json_set(
                payload_json, '$.taskId', ${seeded.task.taskId}
              )
          WHERE aggregate_kind = 'task'
            AND stream_id = ${forgedTaskId}
        `;
        yield* sql`
          UPDATE agent_control_task_states
          SET task_id = ${seeded.task.taskId},
              state_json = json_set(
                state_json, '$.taskId', ${seeded.task.taskId}
              )
          WHERE task_id = ${forgedTaskId}
        `;
      }),
  );

  it.effect(
    "prepares only a reservation and replays it before changed project, lease, and marker authority",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const fs = yield* FileSystem.FileSystem;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-reservation-e2e");
        const seeded = yield* seedPrepared(projectId, repo.cwd);
        const lease = yield* reserveLease(seeded.stageRun);
        const ready = yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("controlled-thread-reservation-worktree"),
          projectId,
          taskId: seeded.task.taskId,
        });
        const reservations = yield* AgentControlControlledThreadReservation;
        const command = {
          commandId: CommandId.make("controlled-thread-reservation-prepare"),
          projectId,
          taskId: seeded.task.taskId,
        };

        const prepared = yield* Effect.all(
          [reservations.prepareInitial(command), reservations.prepareInitial(command)],
          { concurrency: "unbounded" },
        );
        assert.equal(
          prepared.every((result) => result.eventCreated),
          true,
        );
        assert.equal(
          prepared[0]!.reservation.controlledThreadReservationId,
          prepared[1]!.reservation.controlledThreadReservationId,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
          `)[0]!.count,
          1,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_controlled_thread_reservation_states
          `)[0]!.count,
          1,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_command_receipts
            WHERE aggregate_kind = 'controlled-thread-reservation'
          `)[0]!.count,
          1,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM orchestration_events
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count FROM projection_threads
          `)[0]!.count,
          0,
        );

        const samePosition = yield* Effect.result(
          reservations.prepareInitial({
            ...command,
            commandId: CommandId.make("controlled-thread-reservation-same-position"),
          }),
        );
        assert.equal(samePosition._tag, "Failure");
        if (samePosition._tag === "Failure") {
          assert.equal(
            samePosition.failure.code,
            "controlled-thread-reservation-identity-conflict",
          );
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
          `)[0]!.count,
          1,
        );

        const engine = yield* AgentControlControlledThreadReservationEngine;
        const internalState = Option.getOrThrow(
          yield* engine.getAuthoritative(prepared[0]!.reservation.controlledThreadReservationId),
        );
        const receipt = (yield* sql<{ readonly commandFingerprint: string }>`
          SELECT command_fingerprint AS "commandFingerprint"
          FROM agent_control_command_receipts
          WHERE command_id = ${command.commandId}
        `)[0]!;
        const alteredInternalReplay = yield* Effect.result(
          engine.dispatchPreparedController(
            {
              type: "agentControl.controlledThreadReservation.prepare",
              commandId: command.commandId,
              authority: "controller",
              controlledThreadReservationId: internalState.controlledThreadReservationId,
              threadId: internalState.threadId,
              projectId: internalState.projectId,
              taskId: internalState.taskId,
              taskRevision: internalState.taskRevision,
              githubIntakeSequence: internalState.githubIntakeSequence,
              sourceIdentityFingerprint: internalState.sourceIdentityFingerprint,
              stageRunId: internalState.stageRunId,
              attemptId: internalState.attemptId,
              roleId: internalState.roleId,
              stageKind: internalState.stageKind,
              stageOrdinal: internalState.stageOrdinal,
              attemptOrdinal: internalState.attemptOrdinal,
              leaseId: internalState.leaseId,
              fenceToken: internalState.fenceToken + 1,
              worktreeReservationId: internalState.worktreeReservationId,
              expectedRevision: 0,
            },
            receipt.commandFingerprint,
          ),
        );
        assert.equal(alteredInternalReplay._tag, "Failure");
        if (alteredInternalReplay._tag === "Failure") {
          assert.equal(alteredInternalReplay.failure.code, "command-identity-mismatch");
        }

        const rejectedCommand = {
          commandId: CommandId.make("controlled-thread-reservation-rejected-replay"),
          projectId,
          taskId: AgentControlTaskId.make("controlled-thread-reservation-invented-task"),
        };
        const rejected = yield* Effect.result(reservations.prepareInitial(rejectedCommand));
        assert.equal(rejected._tag, "Failure");
        if (rejected._tag === "Failure") {
          assert.equal(rejected.failure.code, "task-missing");
        }

        yield* releaseLease(lease, "controlled-thread-reservation-release-after-prepare");
        yield* sql`
          UPDATE projection_projects SET deleted_at = ${at}
          WHERE project_id = ${projectId}
        `;
        yield* fs.remove(
          yield* ownershipMarkerPath(ready.internalWorktreePath, ready.gitCreatedGitDir!),
        );
        const replay = yield* reservations.prepareInitial(command);
        assert.equal(replay.eventCreated, true);
        assert.equal(
          replay.reservation.controlledThreadReservationId,
          prepared[0]!.reservation.controlledThreadReservationId,
        );
        const rejectedReplay = yield* Effect.result(reservations.prepareInitial(rejectedCommand));
        assert.equal(rejectedReplay._tag, "Failure");
        if (rejectedReplay._tag === "Failure") {
          assert.equal(rejectedReplay.failure.code, "task-missing");
        }

        const mismatch = yield* Effect.result(
          reservations.prepareInitial({
            ...command,
            taskId: AgentControlTaskId.make("controlled-thread-reservation-other-task"),
          }),
        );
        assert.equal(mismatch._tag, "Failure");
        if (mismatch._tag === "Failure") {
          assert.equal(mismatch.failure.code, "command-identity-mismatch");
        }
      }),
  );

  it.effect(
    "rolls event, projection, cursor, and receipt back when receipt persistence fails",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-reservation-receipt-rollback");
        const seeded = yield* seedPrepared(projectId, repo.cwd);
        yield* reserveLease(seeded.stageRun);
        yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
          commandId: CommandId.make("controlled-thread-reservation-rollback-worktree"),
          projectId,
          taskId: seeded.task.taskId,
        });
        const reservations = yield* AgentControlControlledThreadReservation;
        const engine = yield* AgentControlControlledThreadReservationEngine;
        const command = {
          commandId: CommandId.make("controlled-thread-reservation-rollback-prepare"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const published = yield* Ref.make(0);
        const publicationFiber = yield* engine.streamDomainEvents.pipe(
          Stream.filter((event) => event.commandId === command.commandId),
          Stream.runForEach(() => Ref.update(published, (count) => count + 1)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const beforeCursor =
          (yield* sql<{ readonly sequence: number }>`
          SELECT COALESCE(last_applied_sequence, 0) AS sequence
          FROM agent_control_projection_state
          WHERE projector_name =
            'agent-control-controlled-thread-reservation-v1'
        `)[0]?.sequence ?? 0;
        yield* sql`
          CREATE TRIGGER controlled_thread_reservation_receipt_rollback
          BEFORE INSERT ON agent_control_command_receipts
          WHEN NEW.command_id =
            'controlled-thread-reservation-rollback-prepare'
          BEGIN
            SELECT RAISE(ABORT, 'controlled thread receipt rollback');
          END
        `;

        const failed = yield* Effect.result(reservations.prepareInitial(command));
        assert.equal(failed._tag, "Failure");
        if (failed._tag === "Failure") {
          assert.equal(failed.failure.code, "internal-persistence-error");
          assert.equal(failed.failure.operation, "dispatch");
        }
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
              AND json_extract(payload_json, '$.projectId') = ${projectId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_controlled_thread_stream_catalog
            WHERE project_id = ${projectId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_controlled_thread_command_intents
            WHERE command_id = ${command.commandId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_controlled_thread_reservation_states
            WHERE project_id = ${projectId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_command_receipts
            WHERE command_id = ${command.commandId}
          `)[0]!.count,
          0,
        );
        assert.equal(
          (yield* sql<{ readonly sequence: number }>`
            SELECT COALESCE(last_applied_sequence, 0) AS sequence
            FROM agent_control_projection_state
            WHERE projector_name =
              'agent-control-controlled-thread-reservation-v1'
          `)[0]?.sequence ?? 0,
          beforeCursor,
        );
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(published), 0);

        yield* sql`DROP TRIGGER controlled_thread_reservation_receipt_rollback`;
        const retry = yield* reservations.prepareInitial(command);
        assert.equal(retry.eventCreated, true);
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(published), 1);
        yield* Fiber.interrupt(publicationFiber);
      }),
  );

  it.effect("quarantines malformed controlled-thread streams by project and stage group", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const repo = yield* makeRepository();
      const projectId = ProjectId.make("controlled-thread-list-quarantine");
      const seeded = yield* seedPrepared(projectId, repo.cwd);
      yield* reserveLease(seeded.stageRun);
      yield* (yield* AgentControlWorktreeController).reserveAndMaterialize({
        commandId: CommandId.make("controlled-thread-list-quarantine-worktree"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const reservations = yield* AgentControlControlledThreadReservation;
      const prepared = yield* reservations.prepareInitial({
        commandId: CommandId.make("controlled-thread-list-quarantine-prepare"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const healthyId = prepared.reservation.controlledThreadReservationId;

      const cloneStream = Effect.fn("cloneControlledThreadListStream")(function* (input: {
        readonly id: string;
        readonly eventId: string;
        readonly commandId: string;
        readonly threadId: string;
        readonly projectId: string;
        readonly stageRunId: string;
        readonly attemptId: string;
      }) {
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`PRAGMA defer_foreign_keys = ON`;
            yield* sql`
            INSERT INTO agent_control_controlled_thread_stream_catalog (
              controlled_thread_reservation_id, event_id, stream_version,
              command_id, event_type, thread_id, project_id, task_id,
              task_revision, github_intake_sequence, source_identity_fingerprint,
              stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
              attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
              prepared_at
            )
            SELECT
              ${input.id}, ${input.eventId}, 1, ${input.commandId}, event_type,
              ${input.threadId}, ${input.projectId}, task_id, task_revision,
              github_intake_sequence, source_identity_fingerprint,
              ${input.stageRunId}, ${input.attemptId}, role_id, stage_kind,
              stage_ordinal, attempt_ordinal, lease_id, fence_token,
              worktree_reservation_id, prepared_at
            FROM agent_control_controlled_thread_stream_catalog
            WHERE controlled_thread_reservation_id = ${healthyId}
            `;
            yield* sql`
            INSERT INTO agent_control_events (
              event_id, aggregate_kind, stream_id, stream_version, event_type,
              occurred_at, command_id, causation_event_id, correlation_id,
              actor_authority, payload_json, metadata_json
            )
            SELECT
              ${input.eventId}, aggregate_kind, ${input.id}, 1, event_type,
              occurred_at, ${input.commandId}, NULL, ${input.commandId},
              actor_authority,
              json_set(
                payload_json,
                '$.controlledThreadReservationId', ${input.id},
                '$.threadId', ${input.threadId},
                '$.projectId', ${input.projectId},
                '$.stageRunId', ${input.stageRunId},
                '$.attemptId', ${input.attemptId}
              ),
              metadata_json
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
              AND stream_id = ${healthyId}
            `;
          }),
        );
      });

      const otherProjectId = ProjectId.make("controlled-thread-list-other-project");
      yield* cloneStream({
        id: "controlled-thread-reservation-list-other-project",
        eventId: "controlled-thread-list-other-project-event",
        commandId: "controlled-thread-list-other-project-command",
        threadId: "t3-auto-reserved-thread-list-other-project",
        projectId: otherProjectId,
        stageRunId: "stage-run-list-other-project",
        attemptId: "attempt-list-other-project",
      });
      yield* sql`DROP TRIGGER agent_control_controlled_thread_event_no_update`;
      yield* sql`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE stream_id = 'controlled-thread-reservation-list-other-project'
        `;
      const isolatedProject = yield* reservations.list({ projectId });
      assert.deepEqual(
        isolatedProject.reservations.map((entry) => entry.controlledThreadReservationId),
        [healthyId],
      );
      assert.equal(isolatedProject.quarantinedCount, 0);

      yield* cloneStream({
        id: "controlled-thread-reservation-list-other-stage",
        eventId: "controlled-thread-list-other-stage-event",
        commandId: "controlled-thread-list-other-stage-command",
        threadId: "t3-auto-reserved-thread-list-other-stage",
        projectId,
        stageRunId: "stage-run-list-other-stage",
        attemptId: "attempt-list-other-stage",
      });
      yield* sql`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE stream_id = 'controlled-thread-reservation-list-other-stage'
        `;
      const isolatedStage = yield* reservations.list({ projectId });
      assert.deepEqual(
        isolatedStage.reservations.map((entry) => entry.controlledThreadReservationId),
        [healthyId],
      );
      assert.equal(isolatedStage.quarantinedCount, 1);

      yield* cloneStream({
        id: "controlled-thread-reservation-list-competitor",
        eventId: "controlled-thread-list-competitor-event",
        commandId: "controlled-thread-list-competitor-command",
        threadId: "t3-auto-reserved-thread-list-competitor",
        projectId,
        stageRunId: seeded.stageRun.stageRunId,
        attemptId: seeded.stageRun.attemptId,
      });
      yield* sql`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE stream_id = 'controlled-thread-reservation-list-competitor'
        `;
      const conflicted = yield* reservations.list({ projectId });
      assert.lengthOf(conflicted.reservations, 0);
      assert.equal(conflicted.quarantinedCount, 3);
    }),
  );

  it.effect("converges two prepareInitial services at the append CAS", () =>
    Effect.gen(function* () {
      const worktreeInspectionsA = yield* Ref.make(0);
      const worktreeInspectionsB = yield* Ref.make(0);
      const dispatchesA = yield* Ref.make(0);
      const dispatchesB = yield* Ref.make(0);
      const receiptReadsA = yield* Ref.make(0);
      const receiptReadsB = yield* Ref.make(0);
      const prepareFinalizationFailure = yield* Ref.make<
        "none" | "read" | "refresh" | "publication" | "publication-interrupt" | "after-publication"
      >("none");
      const preparePublicationReached = yield* Deferred.make<void>();
      const releasePreparePublication = yield* Deferred.make<void>();
      const failPrepareFinalizationAt = (
        checkpoint: "read" | "refresh" | "publication" | "after-publication",
      ) =>
        Ref.get(prepareFinalizationFailure).pipe(
          Effect.flatMap((current) =>
            current === checkpoint
              ? Effect.die(new Error(`prepare-finalization-${checkpoint}-defect`))
              : Effect.void,
          ),
        );
      const harness = yield* makeIndependentControllerContexts(
        {
          afterReadyInspection: () => Ref.update(worktreeInspectionsA, (count) => count + 1),
        },
        {
          afterReadyInspection: () => Ref.update(worktreeInspectionsB, (count) => count + 1),
        },
        true,
      );
      assert.notStrictEqual(harness.sqlA, harness.sqlB);
      assert.deepStrictEqual(
        yield* harness.sqlA`
          SELECT
            (SELECT journal_mode FROM pragma_journal_mode) AS journalMode,
            (SELECT foreign_keys FROM pragma_foreign_keys) AS foreignKeys
        `,
        [{ journalMode: "wal", foreignKeys: 1 }],
      );
      assert.deepStrictEqual(
        yield* harness.sqlB`
          SELECT
            (SELECT journal_mode FROM pragma_journal_mode) AS journalMode,
            (SELECT foreign_keys FROM pragma_foreign_keys) AS foreignKeys
        `,
        [{ journalMode: "wal", foreignKeys: 1 }],
      );

      const reachedA = yield* Deferred.make<void>();
      const reachedB = yield* Deferred.make<void>();
      const releaseA = yield* Deferred.make<void>();
      const releaseB = yield* Deferred.make<void>();
      const controlledThreadHooksA: AgentControlControlledThreadReservationTransactionHooksShape = {
        afterReadyInspection: Effect.void,
        beforeDbAdmission: Effect.void,
        afterDbAdmission: Effect.void,
        beforeEventAppend: Deferred.succeed(reachedA, undefined).pipe(
          Effect.andThen(Deferred.await(releaseA)),
        ),
        afterWritesBeforeCommit: Effect.void,
        beforePrepareFinalizationRead: failPrepareFinalizationAt("read"),
        beforePrepareReservationRefresh: failPrepareFinalizationAt("refresh"),
        beforePreparePublication: Ref.get(prepareFinalizationFailure).pipe(
          Effect.flatMap((checkpoint) =>
            checkpoint === "publication"
              ? Effect.die(new Error("prepare-finalization-publication-defect"))
              : checkpoint === "publication-interrupt"
                ? Deferred.succeed(preparePublicationReached, undefined).pipe(
                    Effect.andThen(Deferred.await(releasePreparePublication)),
                  )
                : Effect.void,
          ),
        ),
        afterPreparePublicationBeforeCompletion: failPrepareFinalizationAt("after-publication"),
      };
      const controlledThreadHooksB: AgentControlControlledThreadReservationTransactionHooksShape = {
        afterReadyInspection: Effect.void,
        beforeDbAdmission: Effect.void,
        afterDbAdmission: Effect.void,
        beforeEventAppend: Deferred.succeed(reachedB, undefined).pipe(
          Effect.andThen(Deferred.await(releaseB)),
        ),
        afterWritesBeforeCommit: Effect.void,
      };
      const serviceScopeA = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(serviceScopeA, Exit.void));
      const engineContextA = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
          Layer.provide(Layer.succeedContext(harness.contextA)),
        ),
        serviceScopeA,
      ).pipe(
        Effect.provideService(
          AgentControlControlledThreadReservationTransactionHooks,
          controlledThreadHooksA,
        ),
      );
      const engineA = Context.get(engineContextA, AgentControlControlledThreadReservationEngine);
      const countedEngineA = AgentControlControlledThreadReservationEngine.of({
        ...engineA,
        dispatchPreparedController: (command, commandFingerprint) =>
          Ref.update(dispatchesA, (count) => count + 1).pipe(
            Effect.andThen(engineA.dispatchPreparedController(command, commandFingerprint)),
          ),
        replayReceiptFirst: (input) =>
          Ref.update(receiptReadsA, (count) => count + 1).pipe(
            Effect.andThen(engineA.replayReceiptFirst(input)),
          ),
      });
      // Complete each real controller inspection exactly once, then release its
      // repository lock so both original service callbacks can meet at the
      // production-bound database append barrier.
      const controllerA = AgentControlWorktreeController.of({
        ...harness.controllerA,
        useReadyWorktree: (input, callback) =>
          harness.controllerA
            .useReadyWorktree(input, (readyWorktree) => Effect.succeed(readyWorktree))
            .pipe(Effect.flatMap((readyWorktree) => Effect.scoped(callback(readyWorktree)))),
      });
      const dependenciesA = Context.add(
        harness.contextA,
        AgentControlWorktreeController,
        controllerA,
      );
      const dependenciesAWithEngine = Context.add(
        dependenciesA,
        AgentControlControlledThreadReservationEngine,
        countedEngineA,
      );
      const serviceContextA = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
          Layer.provide(Layer.succeedContext(dependenciesAWithEngine)),
        ),
        serviceScopeA,
      ).pipe(
        Effect.provideService(
          AgentControlControlledThreadReservationTransactionHooks,
          controlledThreadHooksA,
        ),
      );
      const controllerB = AgentControlWorktreeController.of({
        ...harness.controllerB,
        useReadyWorktree: (input, callback) =>
          harness.controllerB
            .useReadyWorktree(input, (readyWorktree) => Effect.succeed(readyWorktree))
            .pipe(Effect.flatMap((readyWorktree) => Effect.scoped(callback(readyWorktree)))),
      });
      const dependenciesB = Context.add(
        harness.controllerDependenciesB,
        AgentControlWorktreeController,
        controllerB,
      );
      const engineContextB = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
          Layer.provide(Layer.succeedContext(dependenciesB)),
        ),
        harness.scopeB,
      ).pipe(
        Effect.provideService(
          AgentControlControlledThreadReservationTransactionHooks,
          controlledThreadHooksB,
        ),
      );
      const engineB = Context.get(engineContextB, AgentControlControlledThreadReservationEngine);
      const countedEngineB = AgentControlControlledThreadReservationEngine.of({
        ...engineB,
        dispatchPreparedController: (command, commandFingerprint) =>
          Ref.update(dispatchesB, (count) => count + 1).pipe(
            Effect.andThen(engineB.dispatchPreparedController(command, commandFingerprint)),
          ),
        replayReceiptFirst: (input) =>
          Ref.update(receiptReadsB, (count) => count + 1).pipe(
            Effect.andThen(engineB.replayReceiptFirst(input)),
          ),
      });
      const serviceContextB = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
          Layer.provide(
            Layer.succeedContext(
              Context.add(
                dependenciesB,
                AgentControlControlledThreadReservationEngine,
                countedEngineB,
              ),
            ),
          ),
        ),
        harness.scopeB,
      ).pipe(
        Effect.provideService(
          AgentControlControlledThreadReservationTransactionHooks,
          controlledThreadHooksB,
        ),
      );
      const serviceA = Context.get(serviceContextA, AgentControlControlledThreadReservation);
      const serviceB = Context.get(serviceContextB, AgentControlControlledThreadReservation);

      const repo = yield* makeRepository();
      const projectId = ProjectId.make("controlled-thread-reservation-two-connections");
      const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
        Effect.provide(harness.contextA),
      );
      yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      yield* harness.controllerA.reserveAndMaterialize({
        commandId: CommandId.make("controlled-thread-reservation-two-connections-worktree"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const command = {
        commandId: CommandId.make("controlled-thread-reservation-two-connections-prepare"),
        projectId,
        taskId: seeded.task.taskId,
      };
      const publications = yield* Ref.make(0);
      const publicationA = yield* engineA.streamDomainEvents.pipe(
        Stream.runForEach(() => Ref.update(publications, (count) => count + 1)),
        Effect.forkChild,
      );
      const publicationB = yield* engineB.streamDomainEvents.pipe(
        Stream.runForEach(() => Ref.update(publications, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;
      const callerA = yield* serviceA.prepareInitial(command).pipe(Effect.forkChild);
      yield* Deferred.await(reachedA);
      const callerB = yield* serviceB.prepareInitial(command).pipe(Effect.forkChild);
      yield* Deferred.await(reachedB);
      assert.equal(yield* Ref.get(worktreeInspectionsA), 1);
      assert.equal(yield* Ref.get(worktreeInspectionsB), 1);

      yield* Deferred.succeed(releaseA, undefined);
      const acceptedA = yield* Fiber.join(callerA);
      yield* Deferred.succeed(releaseB, undefined);
      const acceptedB = yield* Fiber.join(callerB);
      assert.equal(acceptedA.eventCreated, true);
      assert.deepStrictEqual(acceptedB, acceptedA);
      assert.equal(yield* Ref.get(dispatchesA), 1);
      assert.equal(yield* Ref.get(dispatchesB), 1);
      assert.equal(yield* Ref.get(receiptReadsA), 1);
      assert.equal(yield* Ref.get(receiptReadsB), 2);
      assert.deepStrictEqual(
        yield* harness.sqlB`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
            (SELECT COUNT(*)
              FROM agent_control_controlled_thread_stream_catalog) AS catalogs,
            (SELECT COUNT(*)
              FROM agent_control_controlled_thread_reservation_states) AS projections,
            (SELECT COUNT(*)
              FROM agent_control_controlled_thread_command_intents) AS intents,
            (SELECT COUNT(*) FROM agent_control_command_receipts
              WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts,
            (SELECT COUNT(*) FROM agent_control_command_receipts
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND status = 'accepted' AND error_code IS NULL) AS acceptedReceipts,
            (SELECT COUNT(*) FROM agent_control_command_receipts
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND error_code = 'internal-persistence-error') AS conflictReceipts
            ,
            (SELECT COUNT(*)
              FROM agent_control_controlled_thread_prepare_finalizations)
              AS finalizations,
            (SELECT COUNT(*)
              FROM agent_control_controlled_thread_prepare_finalizations
              WHERE status = 'completed' AND revision = 2)
              AS completedFinalizations
        `,
        [
          {
            events: 1,
            catalogs: 1,
            projections: 1,
            intents: 1,
            receipts: 1,
            acceptedReceipts: 1,
            conflictReceipts: 0,
            finalizations: 1,
            completedFinalizations: 1,
          },
        ],
      );
      assert.equal(yield* Ref.get(publications), 1);
      assert.equal(yield* Ref.get(worktreeInspectionsA), 1);
      assert.equal(yield* Ref.get(worktreeInspectionsB), 1);
      assert.deepStrictEqual(
        yield* serviceA.get({
          projectId,
          controlledThreadReservationId: acceptedA.reservation.controlledThreadReservationId,
        }),
        yield* serviceB.get({
          projectId,
          controlledThreadReservationId: acceptedA.reservation.controlledThreadReservationId,
        }),
      );

      const seedAdditionalPrepare = Effect.fn("seedAdditionalPrepare")(function* (suffix: string) {
        const additionalRepo = yield* makeRepository();
        const additionalProjectId = ProjectId.make(
          `controlled-thread-prepare-finalization-${suffix}`,
        );
        const additionalSeed = yield* seedPrepared(additionalProjectId, additionalRepo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(additionalSeed.stageRun).pipe(Effect.provide(harness.contextA));
        yield* harness.controllerA.reserveAndMaterialize({
          commandId: CommandId.make(`prepare-finalization-${suffix}-worktree`),
          projectId: additionalProjectId,
          taskId: additionalSeed.task.taskId,
        });
        return {
          commandId: CommandId.make(`prepare-finalization-${suffix}-command`),
          projectId: additionalProjectId,
          taskId: additionalSeed.task.taskId,
        } as const;
      });
      const assertCompletedPrepare = Effect.fn("assertCompletedPrepare")(function* (
        commandId: CommandId,
      ) {
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind = 'controlled-thread-reservation'
                 AND command_id = ${commandId}) AS events,
              (SELECT count(*) FROM agent_control_controlled_thread_command_intents
               WHERE command_id = ${commandId}) AS intents,
              (SELECT count(*) FROM agent_control_command_receipts
               WHERE command_id = ${commandId}
                 AND status = 'accepted') AS receipts,
              (SELECT count(*)
               FROM agent_control_controlled_thread_prepare_finalizations
               WHERE prepare_command_id = ${commandId}) AS finalizations,
              (SELECT count(*)
               FROM agent_control_controlled_thread_prepare_finalizations
               WHERE prepare_command_id = ${commandId}
                 AND status = 'completed' AND revision >= 2) AS completed
          `,
          [{ events: 1, intents: 1, receipts: 1, finalizations: 1, completed: 1 }],
        );
      });

      const defectCommand = yield* seedAdditionalPrepare("native-defect");
      const defectExit = yield* Effect.exit(
        serviceA.prepareInitial(defectCommand).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: (observation) =>
              observation.boundary === "agent-control-controlled-thread-prepare-finalization"
                ? Effect.die(new Error("prepare-native-post-commit-defect"))
                : Effect.void,
          }),
        ),
      );
      assert.equal(defectExit._tag, "Failure");
      if (defectExit._tag === "Failure") {
        assert.include(Cause.pretty(defectExit.cause), "prepare-native-post-commit-defect");
      }
      yield* assertCompletedPrepare(defectCommand.commandId);
      assert.equal(yield* Ref.get(publications), 2);
      yield* serviceB.prepareInitial(defectCommand);
      assert.equal(yield* Ref.get(publications), 2);

      const combinedRecoveryCommand = yield* seedAdditionalPrepare("combined-recovery");
      yield* Ref.set(prepareFinalizationFailure, "read");
      const combinedRecoveryExit = yield* Effect.exit(
        serviceA.prepareInitial(combinedRecoveryCommand).pipe(
          Effect.provideService(NodeSqliteTransactionHooks, {
            afterCommitBeforeReturn: (observation) =>
              observation.boundary === "agent-control-controlled-thread-prepare-finalization"
                ? Effect.die(new Error("prepare-native-combined-return-defect"))
                : Effect.void,
          }),
        ),
      );
      yield* Ref.set(prepareFinalizationFailure, "none");
      assert.equal(combinedRecoveryExit._tag, "Failure");
      if (combinedRecoveryExit._tag === "Failure") {
        const combinedCause = Cause.pretty(combinedRecoveryExit.cause);
        assert.include(combinedCause, "prepare-native-combined-return-defect");
        assert.include(combinedCause, "prepare-finalization-read-defect");
      }
      const beforeCombinedRecovery = yield* Ref.get(publications);
      yield* serviceA.prepareInitial(combinedRecoveryCommand);
      yield* assertCompletedPrepare(combinedRecoveryCommand.commandId);
      assert.equal(yield* Ref.get(publications), beforeCombinedRecovery + 1);

      const interruptCommand = yield* seedAdditionalPrepare("native-interrupt");
      const beforeNativeInterrupt = yield* Ref.get(publications);
      const committedBeforeReturn = yield* Deferred.make<void>();
      const releaseNativeReturn = yield* Deferred.make<void>();
      const interruptedCaller = yield* serviceA.prepareInitial(interruptCommand).pipe(
        Effect.provideService(NodeSqliteTransactionHooks, {
          afterCommitBeforeReturn: (observation) =>
            observation.boundary === "agent-control-controlled-thread-prepare-finalization"
              ? Deferred.succeed(committedBeforeReturn, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseNativeReturn)),
                )
              : Effect.void,
        }),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.await(committedBeforeReturn);
      const interrupt = yield* Fiber.interrupt(interruptedCaller).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releaseNativeReturn, undefined);
      const interruptedExit = yield* Fiber.await(interruptedCaller);
      yield* Fiber.join(interrupt);
      assert.equal(Exit.hasInterrupts(interruptedExit), true);
      yield* assertCompletedPrepare(interruptCommand.commandId);
      assert.equal(yield* Ref.get(publications), beforeNativeInterrupt + 1);
      yield* serviceB.prepareInitial(interruptCommand);
      assert.equal(yield* Ref.get(publications), beforeNativeInterrupt + 1);

      for (const checkpoint of ["refresh", "publication", "after-publication"] as const) {
        const checkpointCommand = yield* seedAdditionalPrepare(checkpoint);
        yield* Ref.set(prepareFinalizationFailure, checkpoint);
        const checkpointExit = yield* Effect.exit(serviceA.prepareInitial(checkpointCommand));
        yield* Ref.set(prepareFinalizationFailure, "none");
        assert.equal(checkpointExit._tag, "Failure", checkpoint);
        if (checkpointExit._tag === "Failure") {
          assert.include(
            Cause.pretty(checkpointExit.cause),
            `prepare-finalization-${checkpoint}-defect`,
            checkpoint,
          );
        }
        yield* assertCompletedPrepare(checkpointCommand.commandId);
        const publishedAfterFailure = yield* Ref.get(publications);
        yield* serviceA.prepareInitial(checkpointCommand);
        assert.equal(yield* Ref.get(publications), publishedAfterFailure, checkpoint);
      }

      const publicationInterruptCommand = yield* seedAdditionalPrepare("publication-interrupt");
      const beforePublicationInterrupt = yield* Ref.get(publications);
      yield* Ref.set(prepareFinalizationFailure, "publication-interrupt");
      const publicationInterruptCaller = yield* serviceA
        .prepareInitial(publicationInterruptCommand)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(preparePublicationReached);
      const publicationInterrupt = yield* Fiber.interrupt(publicationInterruptCaller).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releasePreparePublication, undefined);
      const publicationInterruptExit = yield* Fiber.await(publicationInterruptCaller);
      yield* Fiber.join(publicationInterrupt);
      yield* Ref.set(prepareFinalizationFailure, "none");
      assert.equal(Exit.hasInterrupts(publicationInterruptExit), true);
      yield* assertCompletedPrepare(publicationInterruptCommand.commandId);
      assert.equal(yield* Ref.get(publications), beforePublicationInterrupt + 1);
      yield* serviceA.prepareInitial(publicationInterruptCommand);
      assert.equal(yield* Ref.get(publications), beforePublicationInterrupt + 1);

      const recoveryReadCommand = yield* seedAdditionalPrepare("recovery-read");
      yield* Ref.set(prepareFinalizationFailure, "read");
      const recoveryReadExit = yield* Effect.exit(serviceA.prepareInitial(recoveryReadCommand));
      yield* Ref.set(prepareFinalizationFailure, "none");
      assert.equal(recoveryReadExit._tag, "Failure");
      if (recoveryReadExit._tag === "Failure") {
        assert.include(Cause.pretty(recoveryReadExit.cause), "prepare-finalization-read-defect");
      }
      assert.deepStrictEqual(
        yield* harness.sqlB`
          SELECT status, revision
          FROM agent_control_controlled_thread_prepare_finalizations
          WHERE prepare_command_id = ${recoveryReadCommand.commandId}
        `,
        [{ status: "pending", revision: 0 }],
      );
      const beforeRecoveryPublication = yield* Ref.get(publications);
      yield* serviceA.prepareInitial(recoveryReadCommand);
      yield* assertCompletedPrepare(recoveryReadCommand.commandId);
      assert.equal(yield* Ref.get(publications), beforeRecoveryPublication + 1);

      const completionCasCommand = yield* seedAdditionalPrepare("completion-cas");
      yield* harness.sqlA`
        CREATE TRIGGER fail_prepare_completion_cas
        BEFORE UPDATE ON agent_control_controlled_thread_prepare_finalizations
        WHEN OLD.prepare_command_id =
          'prepare-finalization-completion-cas-command'
          AND NEW.status = 'completed'
        BEGIN
          SELECT RAISE(ABORT, 'prepare completion CAS failure');
        END
      `;
      const beforeCompletionCasPublication = yield* Ref.get(publications);
      const completionCasExit = yield* Effect.exit(serviceA.prepareInitial(completionCasCommand));
      assert.equal(completionCasExit._tag, "Failure");
      assert.deepStrictEqual(
        yield* harness.sqlB`
          SELECT status, revision
          FROM agent_control_controlled_thread_prepare_finalizations
          WHERE prepare_command_id = ${completionCasCommand.commandId}
        `,
        [{ status: "claimed", revision: 1 }],
      );
      assert.equal(yield* Ref.get(publications), beforeCompletionCasPublication + 1);
      yield* harness.sqlA`DROP TRIGGER fail_prepare_completion_cas`;
      yield* serviceA.prepareInitial(completionCasCommand);
      yield* assertCompletedPrepare(completionCasCommand.commandId);
      assert.equal(yield* Ref.get(publications), beforeCompletionCasPublication + 1);

      const restartPendingCommand = yield* seedAdditionalPrepare("restart-pending");
      yield* Ref.set(prepareFinalizationFailure, "read");
      yield* Effect.exit(serviceA.prepareInitial(restartPendingCommand));
      yield* Ref.set(prepareFinalizationFailure, "none");

      const restartClaimedCommand = yield* seedAdditionalPrepare("restart-claimed");
      yield* Ref.set(prepareFinalizationFailure, "read");
      yield* Effect.exit(serviceA.prepareInitial(restartClaimedCommand));
      yield* Ref.set(prepareFinalizationFailure, "none");
      yield* harness.sqlA.withTransaction(harness.sqlA`
        UPDATE agent_control_controlled_thread_prepare_finalizations
        SET status = 'claimed',
            revision = 1,
            claimed_at = '2026-07-30T00:00:00.000Z'
        WHERE prepare_command_id = ${restartClaimedCommand.commandId}
          AND status = 'pending'
          AND revision = 0
      `);

      const restartCompletedCommand = yield* seedAdditionalPrepare("restart-completed");
      yield* serviceA.prepareInitial(restartCompletedCommand);
      yield* Fiber.interrupt(publicationA);
      yield* Scope.close(serviceScopeA, Exit.void);

      const restartedScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void));
      const restartedEngineContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
          Layer.provide(Layer.succeedContext(dependenciesA)),
        ),
        restartedScope,
      );
      const restartedEngine = Context.get(
        restartedEngineContext,
        AgentControlControlledThreadReservationEngine,
      );
      const restartedServiceContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
          Layer.provide(
            Layer.succeedContext(
              Context.add(
                dependenciesA,
                AgentControlControlledThreadReservationEngine,
                restartedEngine,
              ),
            ),
          ),
        ),
        restartedScope,
      );
      const restartedService = Context.get(
        restartedServiceContext,
        AgentControlControlledThreadReservation,
      );
      const restartPublications = yield* Ref.make(0);
      const restartedSubscriber = yield* restartedEngine.streamDomainEvents.pipe(
        Stream.runForEach(() => Ref.update(restartPublications, (count) => count + 1)),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* restartedService.prepareInitial(restartPendingCommand);
      yield* restartedService.prepareInitial(restartClaimedCommand);
      yield* restartedService.prepareInitial(restartCompletedCommand);
      assert.equal(yield* Ref.get(restartPublications), 2);
      yield* assertCompletedPrepare(restartPendingCommand.commandId);
      yield* assertCompletedPrepare(restartClaimedCommand.commandId);
      yield* assertCompletedPrepare(restartCompletedCommand.commandId);
      yield* Fiber.interrupt(restartedSubscriber);

      assert.deepStrictEqual(yield* harness.sqlA`SELECT 1 AS reusable`, [{ reusable: 1 }]);
      assert.deepStrictEqual(yield* harness.sqlB`SELECT 1 AS reusable`, [{ reusable: 1 }]);
      yield* Fiber.interrupt(publicationB);
    }),
  );

  it.effect("keeps the dispatch error when the one-time receipt read also fails", () =>
    Effect.gen(function* () {
      const harness = yield* makeIndependentControllerContexts();
      const dependencies = Context.add(
        harness.controllerDependenciesB,
        AgentControlWorktreeController,
        harness.controllerB,
      );
      const engineContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationEngineLive).pipe(
          Layer.provide(Layer.succeedContext(dependencies)),
        ),
        harness.scopeB,
      );
      const engine = Context.get(engineContext, AgentControlControlledThreadReservationEngine);
      const replayCalls = yield* Ref.make(0);
      const replayReadFailure = AgentControlControlledThreadReservationEngine.of({
        ...engine,
        replayReceiptFirst: (input) =>
          Ref.getAndUpdate(replayCalls, (count) => count + 1).pipe(
            Effect.flatMap((call) =>
              call === 0
                ? engine.replayReceiptFirst(input)
                : Effect.fail(
                    new AgentControlControlledThreadReservationRpcError({
                      code: "internal-persistence-error",
                      operation: "get",
                      projectId: input.projectId,
                      taskId: input.taskId,
                      controlledThreadReservationId: null,
                    }),
                  ),
            ),
          ),
      });
      const serviceContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
          Layer.provide(
            Layer.succeedContext(
              Context.add(
                dependencies,
                AgentControlControlledThreadReservationEngine,
                replayReadFailure,
              ),
            ),
          ),
        ),
        harness.scopeB,
      );
      const service = Context.get(serviceContext, AgentControlControlledThreadReservation);
      const repo = yield* makeRepository();
      const projectId = ProjectId.make("controlled-thread-receipt-read-failure");
      const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
        Effect.provide(harness.contextA),
      );
      yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      yield* harness.controllerA.reserveAndMaterialize({
        commandId: CommandId.make("controlled-thread-receipt-read-failure-worktree"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const command = {
        commandId: CommandId.make("controlled-thread-receipt-read-failure-prepare"),
        projectId,
        taskId: seeded.task.taskId,
      };
      yield* harness.sqlA`
        CREATE TRIGGER controlled_thread_receipt_read_failure
        BEFORE INSERT ON agent_control_command_receipts
        WHEN NEW.command_id = 'controlled-thread-receipt-read-failure-prepare'
        BEGIN
          SELECT RAISE(ABORT, 'controlled thread dispatch persistence failure');
        END
      `;
      const publications = yield* Ref.make(0);
      const publication = yield* engine.streamDomainEvents.pipe(
        Stream.runForEach(() => Ref.update(publications, (count) => count + 1)),
        Effect.forkChild,
      );
      yield* Effect.yieldNow;

      const failed = yield* Effect.result(service.prepareInitial(command));
      assert.equal(failed._tag, "Failure");
      if (failed._tag === "Failure") {
        assert.equal(failed.failure.code, "internal-persistence-error");
        assert.equal(failed.failure.operation, "dispatch");
      }
      assert.equal(yield* Ref.get(replayCalls), 2);
      assert.deepStrictEqual(
        yield* harness.sqlA`
          SELECT
            (SELECT COUNT(*) FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
            (SELECT COUNT(*) FROM agent_control_controlled_thread_stream_catalog) AS catalogs,
            (SELECT COUNT(*) FROM agent_control_controlled_thread_reservation_states) AS projections,
            (SELECT COUNT(*) FROM agent_control_controlled_thread_command_intents) AS intents,
            (SELECT COUNT(*) FROM agent_control_command_receipts
              WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts
        `,
        [{ events: 0, catalogs: 0, projections: 0, intents: 0, receipts: 0 }],
      );
      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(publications), 0);
      yield* Fiber.interrupt(publication);
    }),
  );

  it.effect("does not route service defects or interrupts into receipt replay", () =>
    Effect.gen(function* () {
      for (const failure of ["defect", "interrupt"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const worktreeInspections = yield* Ref.make(0);
            const hooks: AgentControlControlledThreadReservationTransactionHooksShape = {
              afterReadyInspection: Effect.void,
              beforeDbAdmission: Effect.void,
              afterDbAdmission: Effect.void,
              beforeEventAppend: Effect.void,
              afterWritesBeforeCommit:
                failure === "defect"
                  ? Effect.die("controlled-thread-service-defect")
                  : Effect.interrupt,
            };
            const harness = yield* makeIndependentControllerContexts(
              undefined,
              {
                afterReadyInspection: () => Ref.update(worktreeInspections, (count) => count + 1),
              },
              true,
              true,
              hooks,
            );
            const engine = harness.controlledThreadEngineB;
            assert.isNotNull(engine);
            if (engine === null) return;
            const replayCalls = yield* Ref.make(0);
            const countedEngine = AgentControlControlledThreadReservationEngine.of({
              ...engine,
              replayReceiptFirst: (input) =>
                Ref.update(replayCalls, (count) => count + 1).pipe(
                  Effect.andThen(engine.replayReceiptFirst(input)),
                ),
            });
            const dependencies = Context.add(
              harness.controllerDependenciesB,
              AgentControlWorktreeController,
              harness.controllerB,
            );
            const serviceContext = yield* Layer.buildWithScope(
              Layer.fresh(AgentControlControlledThreadReservationLive).pipe(
                Layer.provide(
                  Layer.succeedContext(
                    Context.add(
                      dependencies,
                      AgentControlControlledThreadReservationEngine,
                      countedEngine,
                    ),
                  ),
                ),
              ),
              harness.scopeB,
            ).pipe(
              Effect.provideService(AgentControlControlledThreadReservationTransactionHooks, hooks),
            );
            const service = Context.get(serviceContext, AgentControlControlledThreadReservation);
            const repo = yield* makeRepository();
            const projectId = ProjectId.make(`controlled-thread-service-${failure}`);
            const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
              Effect.provide(harness.contextA),
            );
            yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
            yield* harness.controllerA.reserveAndMaterialize({
              commandId: CommandId.make(`controlled-thread-service-${failure}-worktree`),
              projectId,
              taskId: seeded.task.taskId,
            });
            const publications = yield* Ref.make(0);
            const publication = yield* engine.streamDomainEvents.pipe(
              Stream.runForEach(() => Ref.update(publications, (count) => count + 1)),
              Effect.forkChild,
            );
            yield* Effect.yieldNow;

            const failed = yield* Effect.exit(
              service.prepareInitial({
                commandId: CommandId.make(`controlled-thread-service-${failure}-prepare`),
                projectId,
                taskId: seeded.task.taskId,
              }),
            );
            assert.equal(Exit.isFailure(failed), true);
            assert.equal(
              failure === "defect" ? Exit.hasDies(failed) : Exit.hasInterrupts(failed),
              true,
            );
            assert.equal(yield* Ref.get(replayCalls), 1);
            assert.equal(yield* Ref.get(worktreeInspections), 1);
            assert.deepStrictEqual(
              yield* harness.sqlA`
                SELECT
                  (SELECT COUNT(*) FROM agent_control_events
                    WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
                  (SELECT COUNT(*)
                    FROM agent_control_controlled_thread_stream_catalog) AS catalogs,
                  (SELECT COUNT(*)
                    FROM agent_control_controlled_thread_reservation_states) AS projections,
                  (SELECT COUNT(*)
                    FROM agent_control_controlled_thread_command_intents) AS intents,
                  (SELECT COUNT(*) FROM agent_control_command_receipts
                    WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts
              `,
              [{ events: 0, catalogs: 0, projections: 0, intents: 0, receipts: 0 }],
            );
            yield* Effect.yieldNow;
            assert.equal(yield* Ref.get(publications), 0);
            yield* Fiber.interrupt(publication);
          }),
        );
      }
    }),
  );

  it.effect(
    "rechecks a lease release committed by an independent SQLite connection before admission",
    () =>
      Effect.gen(function* () {
        const admissionReached = yield* Deferred.make<void>();
        const resumeAdmission = yield* Deferred.make<void>();
        const hooks: AgentControlControlledThreadReservationTransactionHooksShape = {
          afterReadyInspection: Effect.void,
          beforeDbAdmission: Deferred.succeed(admissionReached, undefined).pipe(
            Effect.andThen(Deferred.await(resumeAdmission)),
          ),
          afterDbAdmission: Effect.void,
          beforeEventAppend: Effect.void,
          afterWritesBeforeCommit: Effect.void,
        };
        const harness = yield* makeIndependentControllerContexts(
          undefined,
          undefined,
          true,
          true,
          hooks,
        );
        const engineB = harness.controlledThreadEngineB;
        assert.isNotNull(engineB);
        if (engineB === null) return;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("controlled-thread-lease-release-race");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        const lease = yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const ready = yield* harness.controllerA.reserveAndMaterialize({
          commandId: CommandId.make("controlled-thread-lease-release-race-worktree"),
          projectId,
          taskId: seeded.task.taskId,
        });
        const stableIdentity = {
          projectId,
          taskId: seeded.task.taskId,
          taskRevision: seeded.task.revision,
          githubIntakeSequence: seeded.task.githubIntakeSequence,
          sourceIdentityFingerprint: seeded.stageRun.sourceIdentityFingerprint,
          stageRunId: seeded.stageRun.stageRunId,
          attemptId: seeded.stageRun.attemptId,
          roleId: seeded.stageRun.roleId,
          stageKind: "planning" as const,
          stageOrdinal: 1 as const,
          attemptOrdinal: 1 as const,
        };
        const command = {
          type: "agentControl.controlledThreadReservation.prepare" as const,
          commandId: CommandId.make("controlled-thread-lease-release-race-prepare"),
          authority: "controller" as const,
          controlledThreadReservationId:
            yield* deriveAgentControlControlledThreadReservationId(stableIdentity),
          threadId: yield* deriveAgentControlReservedThreadId(stableIdentity),
          ...stableIdentity,
          leaseId: lease.leaseId,
          fenceToken: lease.fenceToken,
          worktreeReservationId: ready.reservationId,
          expectedRevision: 0 as const,
        };
        const pending = yield* engineB
          .dispatchPreparedController(command, "f".repeat(64))
          .pipe(Effect.forkChild);
        yield* Deferred.await(admissionReached);
        yield* releaseLease(lease, "controlled-thread-lease-release-race-release").pipe(
          Effect.provide(harness.contextA),
        );
        yield* Deferred.succeed(resumeAdmission, undefined);
        const result = yield* Effect.result(Fiber.join(pending));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "lease-not-reserved");
        }
        assert.deepEqual(
          yield* harness.sqlA`
            SELECT
              (SELECT COUNT(*) FROM agent_control_events
                WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
              (SELECT COUNT(*) FROM agent_control_controlled_thread_reservation_states)
                AS projections,
              (SELECT COUNT(*) FROM agent_control_command_receipts
                WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts
          `,
          [{ events: 0, projections: 0, receipts: 0 }],
        );
      }),
  );

  it.effect("replays a committed receipt from a runtime with a different holder id", () =>
    Effect.gen(function* () {
      const harness = yield* makeIndependentControllerContexts(undefined, undefined, false, true);
      const engineB = harness.controlledThreadEngineB;
      assert.isNotNull(engineB);
      if (engineB === null) return;
      const repo = yield* makeRepository();
      const projectId = ProjectId.make("controlled-thread-foreign-holder-replay");
      const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
        Effect.provide(harness.contextA),
      );
      const lease = yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
      const ready = yield* harness.controllerA.reserveAndMaterialize({
        commandId: CommandId.make("controlled-thread-foreign-holder-worktree"),
        projectId,
        taskId: seeded.task.taskId,
      });
      const stableIdentity = {
        projectId,
        taskId: seeded.task.taskId,
        taskRevision: seeded.task.revision,
        githubIntakeSequence: seeded.task.githubIntakeSequence,
        sourceIdentityFingerprint: seeded.stageRun.sourceIdentityFingerprint,
        stageRunId: seeded.stageRun.stageRunId,
        attemptId: seeded.stageRun.attemptId,
        roleId: seeded.stageRun.roleId,
        stageKind: "planning" as const,
        stageOrdinal: 1 as const,
        attemptOrdinal: 1 as const,
      };
      const command = {
        type: "agentControl.controlledThreadReservation.prepare" as const,
        commandId: CommandId.make("controlled-thread-foreign-holder-prepare"),
        authority: "controller" as const,
        controlledThreadReservationId:
          yield* deriveAgentControlControlledThreadReservationId(stableIdentity),
        threadId: yield* deriveAgentControlReservedThreadId(stableIdentity),
        ...stableIdentity,
        leaseId: lease.leaseId,
        fenceToken: lease.fenceToken,
        worktreeReservationId: ready.reservationId,
        expectedRevision: 0 as const,
      };
      const engineA = Context.get(harness.contextA, AgentControlControlledThreadReservationEngine);
      const fingerprint = "3".repeat(64);
      const committed = yield* engineA.dispatchPreparedController(command, fingerprint);
      assert.equal(committed._tag, "Accepted");
      if (committed._tag === "Rejected") return yield* committed.error;
      assert.lengthOf(committed.events, 1);
      const replay = yield* engineB.dispatchPreparedController(command, fingerprint);
      assert.equal(replay._tag, "Accepted");
      if (replay._tag === "Rejected") return yield* replay.error;
      assert.lengthOf(replay.events, 0);
      assert.deepEqual(
        yield* harness.sqlB`
          SELECT COUNT(*) AS count
          FROM agent_control_events
          WHERE aggregate_kind = 'controlled-thread-reservation'
        `,
        [{ count: 1 }],
      );
    }),
  );

  it.effect("rolls back after-write defects and interrupts and leaves the command retryable", () =>
    Effect.gen(function* () {
      for (const failure of ["defect", "interrupt"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const hooks: AgentControlControlledThreadReservationTransactionHooksShape = {
              afterReadyInspection: Effect.void,
              beforeDbAdmission: Effect.void,
              afterDbAdmission: Effect.void,
              beforeEventAppend: Effect.void,
              afterWritesBeforeCommit:
                failure === "defect"
                  ? Effect.die("controlled-thread-after-write-defect")
                  : Effect.interrupt,
            };
            const harness = yield* makeIndependentControllerContexts(
              undefined,
              undefined,
              true,
              true,
              hooks,
            );
            const engineB = harness.controlledThreadEngineB;
            assert.isNotNull(engineB);
            if (engineB === null) return;
            const repo = yield* makeRepository();
            const projectId = ProjectId.make(`controlled-thread-after-write-${failure}`);
            const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
              Effect.provide(harness.contextA),
            );
            const lease = yield* reserveLease(seeded.stageRun).pipe(
              Effect.provide(harness.contextA),
            );
            const ready = yield* harness.controllerA.reserveAndMaterialize({
              commandId: CommandId.make(`controlled-thread-after-write-${failure}-worktree`),
              projectId,
              taskId: seeded.task.taskId,
            });
            const stableIdentity = {
              projectId,
              taskId: seeded.task.taskId,
              taskRevision: seeded.task.revision,
              githubIntakeSequence: seeded.task.githubIntakeSequence,
              sourceIdentityFingerprint: seeded.stageRun.sourceIdentityFingerprint,
              stageRunId: seeded.stageRun.stageRunId,
              attemptId: seeded.stageRun.attemptId,
              roleId: seeded.stageRun.roleId,
              stageKind: "planning" as const,
              stageOrdinal: 1 as const,
              attemptOrdinal: 1 as const,
            };
            const command = {
              type: "agentControl.controlledThreadReservation.prepare" as const,
              commandId: CommandId.make(`controlled-thread-after-write-${failure}-prepare`),
              authority: "controller" as const,
              controlledThreadReservationId:
                yield* deriveAgentControlControlledThreadReservationId(stableIdentity),
              threadId: yield* deriveAgentControlReservedThreadId(stableIdentity),
              ...stableIdentity,
              leaseId: lease.leaseId,
              fenceToken: lease.fenceToken,
              worktreeReservationId: ready.reservationId,
              expectedRevision: 0 as const,
            };
            const fingerprint = failure === "defect" ? "1".repeat(64) : "2".repeat(64);
            const failed = yield* Effect.exit(
              engineB.dispatchPreparedController(command, fingerprint),
            );
            assert.equal(Exit.isFailure(failed), true);
            assert.deepEqual(
              yield* harness.sqlA`
                  SELECT
                    (SELECT COUNT(*) FROM agent_control_events
                      WHERE aggregate_kind = 'controlled-thread-reservation') AS events,
                    (SELECT COUNT(*) FROM agent_control_controlled_thread_reservation_states)
                      AS projections,
                    (SELECT COUNT(*) FROM agent_control_command_receipts
                      WHERE aggregate_kind = 'controlled-thread-reservation') AS receipts
                `,
              [{ events: 0, projections: 0, receipts: 0 }],
            );
            const engineA = Context.get(
              harness.contextA,
              AgentControlControlledThreadReservationEngine,
            );
            const retry = yield* engineA.dispatchPreparedController(command, fingerprint);
            assert.equal(retry._tag, "Accepted");
            if (retry._tag === "Rejected") return yield* retry.error;
            assert.equal(retry.events.length, 1);
          }),
        );
      }
    }),
  );

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
          readonly transitionCommandId: string;
          readonly transitionFingerprint: string;
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
            closed_transition_command_id AS "transitionCommandId",
            closed_transition_fingerprint AS "transitionFingerprint",
            closed_phase AS phase
          FROM agent_control_worktree_target_claims
          WHERE command_id = ${input.commandId}
        `)[0]!;
        assert.match(
          baseline.transitionCommandId,
          /^agent-control-internal-worktree-v1-[0-9a-f]{64}$/,
        );
        assert.match(baseline.transitionFingerprint, /^[0-9a-f]{64}$/);
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
          "transition-command-id",
          "transition-fingerprint",
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
                  closed_transition_command_id = CASE
                    WHEN ${corruption} = 'transition-command-id'
                    THEN 'agent-control-internal-worktree-v1-corrupt'
                    ELSE closed_transition_command_id END,
                  closed_transition_fingerprint = CASE
                    WHEN ${corruption} = 'transition-fingerprint' THEN ${"d".repeat(64)}
                    ELSE closed_transition_fingerprint END,
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
              closed_transition_command_id = ${baseline.transitionCommandId},
              closed_transition_fingerprint = ${baseline.transitionFingerprint},
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

  it.effect(
    "rejects jointly corrupted completed target and final event evidence for Ready and Attention",
    () =>
      Effect.gen(function* () {
        const corruptionCases = [
          "pending-token",
          "claim-attempt",
          "expected-revision",
          "resulting-revision",
          "target-generation",
          "reservation-id",
          "completion-phase",
          "multiple-fields",
          "claim-receipt",
          "event-receipt",
          "stored-transition-id",
          "stored-transition-fingerprint",
        ] as const;
        const corruptions: ReadonlyArray<(typeof corruptionCases)[number]> = corruptionCases.filter(
          (corruption) => corruption === "multiple-fields",
        );
        for (const terminal of ["ready", "attention"] as const) {
          for (const corruption of corruptions) {
            yield* Effect.scoped(
              Effect.gen(function* () {
                let worktreesDir = "";
                let dirtied = false;
                const transitionCommitted = yield* Deferred.make<void>();
                const continueCompositeAccept = yield* Deferred.make<void>();
                const hooks: AgentControlWorktreeControllerHooksShape = {
                  afterCompositeClaim: () => Effect.void,
                  afterLifecycleCheckpoint: (checkpoint) =>
                    terminal === "attention" && checkpoint === "after-ownership-marked" && !dirtied
                      ? Effect.promise(async () => {
                          dirtied = true;
                          const root = `${worktreesDir}/agent-control`;
                          const candidates: Array<{
                            readonly path: string;
                            readonly mtimeMs: number;
                          }> = [];
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
                          if (target === undefined) throw new Error("missing generated target");
                          await NodeFSP.writeFile(`${target.path}/joint-corruption.txt`, "dirty\n");
                        })
                      : Effect.void,
                  afterReadyInspection: () => Effect.void,
                  beforeCompositeAccept: () =>
                    Deferred.succeed(transitionCommitted, undefined).pipe(
                      Effect.andThen(Deferred.await(continueCompositeAccept)),
                    ),
                };
                const harness = yield* makeIndependentControllerContexts(hooks, undefined, false);
                worktreesDir = Context.get(harness.contextA, ServerConfig).worktreesDir;
                const repo = yield* makeRepository();
                const projectId = ProjectId.make(`worktree-joint-${terminal}-${corruption}`);
                const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
                  Effect.provide(harness.contextA),
                );
                const lease = yield* reserveLease(seeded.stageRun).pipe(
                  Effect.provide(harness.contextA),
                );
                const input =
                  terminal === "ready"
                    ? {
                        commandId: CommandId.make(`worktree-joint-ready-command-${corruption}`),
                        projectId,
                        taskId: seeded.task.taskId,
                      }
                    : {
                        commandId: CommandId.make(`worktree-joint-attention-command-${corruption}`),
                        projectId,
                        reservationId: (yield* startMaterializing(
                          yield* reserveWorktreeOnly({
                            commandId: `worktree-joint-attention-reserve-${corruption}`,
                            task: seeded.task,
                            stageRun: seeded.stageRun,
                            lease,
                            repositoryWorkspace: repo.cwd,
                            baseCommitSha: repo.baseCommitSha,
                          }).pipe(Effect.provide(harness.contextA)),
                          `worktree-joint-attention-materializing-${corruption}`,
                        ).pipe(Effect.provide(harness.contextA))).reservationId,
                      };
                const published = yield* Ref.make(0);
                const publication = yield* Context.get(
                  harness.contextA,
                  AgentControlWorktreeEngine,
                ).streamDomainEvents.pipe(
                  Stream.filter((event) =>
                    terminal === "ready"
                      ? event.type === "agentControl.worktree.ready"
                      : event.type === "agentControl.worktree.needsAttention",
                  ),
                  Stream.runForEach(() => Ref.update(published, (count) => count + 1)),
                  Effect.forkChild,
                );
                yield* Effect.yieldNow;
                const operation = yield* (
                  terminal === "ready"
                    ? harness.controllerA.reserveAndMaterialize(
                        input as {
                          readonly commandId: CommandId;
                          readonly projectId: ProjectId;
                          readonly taskId: AgentControlTaskId;
                        },
                      )
                    : harness.controllerA.reconcile(
                        input as {
                          readonly commandId: CommandId;
                          readonly projectId: ProjectId;
                          readonly reservationId: AgentControlWorktreeReservationId;
                        },
                      )
                ).pipe(Effect.result, Effect.forkChild);
                yield* Deferred.await(transitionCommitted);
                yield* Effect.yieldNow;
                assert.equal(yield* Ref.get(published), 1, `${terminal}:${corruption}`);
                const committed = (yield* harness.sqlB<{
                  readonly reservationId: AgentControlWorktreeReservationId;
                  readonly stateJson: string;
                  readonly eventId: string;
                  readonly eventPayloadJson: string;
                  readonly transitionCommandId: CommandId;
                  readonly stateRevision: number;
                  readonly pendingToken: string;
                  readonly claimAttemptId: string;
                  readonly expectedRevision: number;
                  readonly resultingRevision: number;
                  readonly targetGeneration: string;
                  readonly compositeCommandId: CommandId;
                  readonly compositeOperation: "reserve-and-materialize" | "reconcile";
                  readonly compositeFingerprint: string;
                  readonly closedReservationId: AgentControlWorktreeReservationId;
                  readonly phase: "materialized" | "retained-attention";
                }>`
                  SELECT operation.worktree_reservation_id AS "reservationId",
                    state.state_json AS "stateJson",
                    event.event_id AS "eventId",
                    event.payload_json AS "eventPayloadJson",
                    event.command_id AS "transitionCommandId",
                    state.revision AS "stateRevision",
                    claim.closed_pending_token AS "pendingToken",
                    claim.closed_claim_attempt_id AS "claimAttemptId",
                    claim.closed_expected_revision AS "expectedRevision",
                    claim.closed_revision AS "resultingRevision",
                    claim.closed_target_generation AS "targetGeneration",
                    claim.closed_command_id AS "compositeCommandId",
                    claim.closed_command_type AS "compositeOperation",
                    claim.closed_input_fingerprint AS "compositeFingerprint",
                    claim.closed_reservation_id AS "closedReservationId",
                    claim.closed_phase AS phase
                  FROM agent_control_worktree_controller_operations AS operation
                  JOIN agent_control_worktree_target_claims AS claim
                    ON claim.command_id = operation.command_id
                  JOIN agent_control_worktree_reservation_states AS state
                    ON state.reservation_id = operation.worktree_reservation_id
                  JOIN agent_control_events AS event
                    ON event.stream_id = operation.worktree_reservation_id
                   AND event.aggregate_kind = 'worktree-reservation'
                   AND event.event_type = ${
                     terminal === "ready"
                       ? "agentControl.worktree.ready"
                       : "agentControl.worktree.needsAttention"
                   }
                  WHERE operation.command_id = ${input.commandId}
                `)[0]!;
                const before = yield* worktreePersistenceCounts(committed.reservationId).pipe(
                  Effect.provideService(SqlClient.SqlClient, harness.sqlB),
                );
                const anchoredBefore = yield* harness.sqlB`
                  SELECT close_anchor_pending_token AS "pendingToken",
                    close_anchor_claim_attempt_id AS "claimAttemptId",
                    close_anchor_expected_claim_revision AS "expectedRevision",
                    close_anchor_claim_revision AS "resultingRevision",
                    close_anchor_target_generation AS "targetGeneration",
                    close_anchor_reservation_id AS "reservationId",
                    close_anchor_phase AS phase,
                    close_anchor_transition_command_id AS "transitionCommandId",
                    close_anchor_transition_fingerprint AS "transitionFingerprint"
                  FROM agent_control_worktree_controller_operations
                  WHERE command_id = ${input.commandId}
                `;
                assert.deepStrictEqual(anchoredBefore, [
                  {
                    pendingToken: committed.pendingToken,
                    claimAttemptId: committed.claimAttemptId,
                    expectedRevision: committed.expectedRevision,
                    resultingRevision: committed.resultingRevision,
                    targetGeneration: committed.targetGeneration,
                    reservationId: committed.closedReservationId,
                    phase: committed.phase,
                    transitionCommandId: committed.transitionCommandId,
                    transitionFingerprint: (yield* harness.sqlB<{ readonly fingerprint: string }>`
                        SELECT command_fingerprint AS fingerprint
                        FROM agent_control_command_receipts
                        WHERE command_id = ${committed.transitionCommandId}
                      `)[0]!.fingerprint,
                  },
                ]);
                assert.equal(
                  (yield* Effect.result(harness.sqlB`
                      UPDATE agent_control_worktree_controller_operations
                      SET close_anchor_pending_token = 'foreign-close-generation'
                      WHERE command_id = ${input.commandId}
                    `))._tag,
                  "Failure",
                );
                assert.equal(
                  (yield* Effect.result(harness.sqlB`
                      UPDATE agent_control_worktree_controller_operations
                      SET close_anchor_claim_revision = NULL
                      WHERE command_id = ${input.commandId}
                    `))._tag,
                  "Failure",
                );
                yield* harness.sqlB`
                  DROP TRIGGER agent_control_worktree_target_claim_authority_update
                `;
                yield* harness.sqlB`
                  DROP TRIGGER agent_control_worktree_event_immutable_update
                `;
                yield* harness.sqlB`
                  DROP TRIGGER agent_control_worktree_event_envelope_immutable_update
                `;
                yield* harness.sqlB`PRAGMA ignore_check_constraints = ON`;
                const corruptPendingToken = `corrupt-pending-${terminal}-${corruption}`;
                const corruptAttempt = `corrupt-attempt-${terminal}-${corruption}`;
                const corruptGeneration = "e".repeat(64);
                const corruptReservation = `worktree-reservation-${"f".repeat(64)}`;
                const corruptPhase = terminal === "ready" ? "retained-attention" : "materialized";
                const corruptTransitionId = CommandId.make(
                  `agent-control-internal-worktree-v1-${"a".repeat(64)}`,
                );
                const corruptTransitionFingerprint = "d".repeat(64);
                const corruptEventId = `event-joint-${terminal}-${corruption}-${"b".repeat(32)}`;
                const jointlyMutated = Number(
                  [
                    "pending-token",
                    "claim-attempt",
                    "expected-revision",
                    "resulting-revision",
                    "target-generation",
                    "reservation-id",
                    "completion-phase",
                    "multiple-fields",
                  ].includes(corruption),
                );
                const jointlyDerivedTransitionId = CommandId.make(
                  `agent-control-internal-worktree-v1-${sha256FramedHex([
                    "agent-control-worktree-completed-transition-command-v2",
                    input.commandId,
                    committed.reservationId,
                    terminal === "ready" ? "ready" : "attention:worktree-dirty",
                    String(committed.stateRevision - 1),
                    corruption === "pending-token" || corruption === "multiple-fields"
                      ? corruptPendingToken
                      : committed.pendingToken,
                    corruption === "claim-attempt" || corruption === "multiple-fields"
                      ? corruptAttempt
                      : committed.claimAttemptId,
                    String(
                      corruption === "expected-revision" || corruption === "multiple-fields"
                        ? committed.expectedRevision + 7
                        : committed.expectedRevision,
                    ),
                    String(
                      corruption === "resulting-revision" || corruption === "multiple-fields"
                        ? committed.resultingRevision + 7
                        : committed.resultingRevision,
                    ),
                    corruption === "target-generation" || corruption === "multiple-fields"
                      ? corruptGeneration
                      : committed.targetGeneration,
                    committed.compositeCommandId,
                    committed.compositeOperation,
                    committed.compositeFingerprint,
                    corruption === "reservation-id" || corruption === "multiple-fields"
                      ? corruptReservation
                      : committed.closedReservationId,
                    corruption === "completion-phase" ? corruptPhase : committed.phase,
                  ])}`,
                );
                const persistedState = decodeReservationState(committed.stateJson);
                const eventPayload = decodeUnknownJson(committed.eventPayloadJson) as Record<
                  string,
                  unknown
                >;
                const jointlyMutatedCloseEvidence = {
                  pendingToken: corruptPendingToken,
                  claimAttemptId: corruptAttempt,
                  expectedRevision: committed.expectedRevision + 7,
                  resultingRevision: committed.resultingRevision + 7,
                  targetGeneration: corruptGeneration,
                  compositeCommandId: committed.compositeCommandId,
                  compositeOperation: committed.compositeOperation,
                  compositeFingerprint: committed.compositeFingerprint,
                  reservationId: corruptReservation,
                  phase: committed.phase,
                } as const;
                const jointlyMutatedCommand = {
                  type:
                    terminal === "ready"
                      ? ("agentControl.worktree.ready" as const)
                      : ("agentControl.worktree.needsAttention" as const),
                  commandId: jointlyDerivedTransitionId,
                  reservationId: persistedState.reservationId,
                  projectId: persistedState.projectId,
                  taskId: persistedState.taskId,
                  taskRevision: persistedState.taskRevision,
                  githubIntakeSequence: persistedState.githubIntakeSequence,
                  sourceIdentityFingerprint: persistedState.sourceIdentityFingerprint,
                  stageRunId: persistedState.stageRunId,
                  attemptId: persistedState.attemptId,
                  leaseId: persistedState.leaseId,
                  fenceToken: persistedState.fenceToken,
                  expectedRevision: committed.stateRevision - 1,
                  ...(terminal === "ready"
                    ? {
                        headCommitSha: eventPayload.headCommitSha,
                        ownershipFingerprint: eventPayload.ownershipFingerprint,
                        gitCreatedDevice: eventPayload.gitCreatedDevice,
                        gitCreatedInode: eventPayload.gitCreatedInode,
                        gitCreatedGitDir: eventPayload.gitCreatedGitDir,
                        markedOwnershipFingerprint: eventPayload.markedOwnershipFingerprint,
                        verifiedAt: eventPayload.verifiedAt,
                      }
                    : {
                        attentionCode: eventPayload.attentionCode,
                        materializationPhase: eventPayload.materializationPhase,
                        gitCreatedDevice: eventPayload.gitCreatedDevice,
                        gitCreatedInode: eventPayload.gitCreatedInode,
                        gitCreatedGitDir: eventPayload.gitCreatedGitDir,
                        markedOwnershipFingerprint: eventPayload.markedOwnershipFingerprint,
                      }),
                  targetClaimCloseEvidence: jointlyMutatedCloseEvidence,
                } as AgentControlWorktreeCommand;
                const jointlyDerivedTransitionFingerprint = NodeCrypto.createHash("sha256")
                  .update(encodeWorktreeCommand(jointlyMutatedCommand), "utf8")
                  .digest("hex");
                yield* harness.sqlB.withTransaction(
                  Effect.gen(function* () {
                    yield* harness.sqlB`
                  UPDATE agent_control_worktree_target_claims
                  SET pending_token = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptPendingToken} ELSE pending_token END,
                    claim_attempt_id = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptAttempt} ELSE claim_attempt_id END,
                    target_generation = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptGeneration} ELSE target_generation END,
                    reservation_id = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptReservation} ELSE reservation_id END,
                    revision = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN revision + 7 ELSE revision END,
                    closed_pending_token = CASE
                        WHEN ${corruption} IN ('pending-token', 'multiple-fields')
                        THEN ${corruptPendingToken} ELSE closed_pending_token END,
                    closed_claim_attempt_id = CASE
                        WHEN ${corruption} IN ('claim-attempt', 'multiple-fields')
                        THEN ${corruptAttempt} ELSE closed_claim_attempt_id END,
                    closed_expected_revision = CASE
                        WHEN ${corruption} IN ('expected-revision', 'multiple-fields')
                        THEN closed_expected_revision + 7 ELSE closed_expected_revision END,
                    closed_revision = CASE
                        WHEN ${corruption} IN ('resulting-revision', 'multiple-fields')
                        THEN closed_revision + 7 ELSE closed_revision END,
                    closed_target_generation = CASE
                        WHEN ${corruption} IN ('target-generation', 'multiple-fields')
                        THEN ${corruptGeneration} ELSE closed_target_generation END,
                    closed_reservation_id = CASE
                        WHEN ${corruption} IN ('reservation-id', 'multiple-fields')
                        THEN ${corruptReservation} ELSE closed_reservation_id END,
                    closed_phase = CASE
                        WHEN ${corruption} = 'completion-phase'
                        THEN ${corruptPhase} ELSE closed_phase END,
                    closed_verified_at = CASE
                        WHEN ${corruption} = 'completion-phase' AND ${terminal} = 'ready'
                        THEN NULL ELSE closed_verified_at END,
                    closed_attention_code = CASE
                        WHEN ${corruption} = 'completion-phase' AND ${terminal} = 'ready'
                        THEN 'worktree-dirty'
                        WHEN ${corruption} = 'completion-phase' AND ${terminal} = 'attention'
                        THEN NULL ELSE closed_attention_code END,
                    closed_transition_command_id = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${jointlyDerivedTransitionId}
                        WHEN ${corruption} = 'stored-transition-id'
                        THEN ${corruptTransitionId} ELSE closed_transition_command_id END,
                    closed_transition_fingerprint = CASE
                        WHEN ${corruption} = 'multiple-fields'
                        THEN ${jointlyDerivedTransitionFingerprint}
                        WHEN ${corruption} IN (
                          'claim-receipt', 'stored-transition-fingerprint'
                        )
                        THEN ${corruptTransitionFingerprint}
                        ELSE closed_transition_fingerprint END
                  WHERE command_id = ${input.commandId}
                `;
                    yield* harness.sqlB`
                  UPDATE agent_control_worktree_event_envelopes
                  SET event_id = CASE WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptEventId} ELSE event_id END
                  WHERE event_id = ${committed.eventId}
                `;
                    yield* harness.sqlB`
                  UPDATE agent_control_events
                  SET event_id = CASE WHEN ${corruption} = 'multiple-fields'
                        THEN ${corruptEventId} ELSE event_id END,
                    command_id = CASE
                        WHEN ${jointlyMutated} THEN ${jointlyDerivedTransitionId}
                        WHEN ${corruption} = 'event-receipt' THEN ${corruptTransitionId}
                        ELSE command_id END,
                    correlation_id = CASE
                        WHEN ${jointlyMutated} THEN ${jointlyDerivedTransitionId}
                        WHEN ${corruption} = 'event-receipt' THEN ${corruptTransitionId}
                        ELSE correlation_id END,
                    payload_json = json_set(
                    payload_json,
                    '$.targetClaimCloseEvidence.pendingToken',
                    CASE WHEN ${corruption} IN ('pending-token', 'multiple-fields')
                      THEN ${corruptPendingToken}
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.pendingToken'
                      ) END,
                    '$.targetClaimCloseEvidence.claimAttemptId',
                    CASE WHEN ${corruption} IN ('claim-attempt', 'multiple-fields')
                      THEN ${corruptAttempt}
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.claimAttemptId'
                      ) END,
                    '$.targetClaimCloseEvidence.expectedRevision',
                    CASE WHEN ${corruption} IN ('expected-revision', 'multiple-fields')
                      THEN json_extract(
                        payload_json, '$.targetClaimCloseEvidence.expectedRevision'
                      ) + 7
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.expectedRevision'
                      ) END,
                    '$.targetClaimCloseEvidence.resultingRevision',
                    CASE WHEN ${corruption} IN ('resulting-revision', 'multiple-fields')
                      THEN json_extract(
                        payload_json, '$.targetClaimCloseEvidence.resultingRevision'
                      ) + 7
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.resultingRevision'
                      ) END,
                    '$.targetClaimCloseEvidence.targetGeneration',
                    CASE WHEN ${corruption} IN ('target-generation', 'multiple-fields')
                      THEN ${corruptGeneration}
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.targetGeneration'
                      ) END,
                    '$.targetClaimCloseEvidence.reservationId',
                    CASE WHEN ${corruption} IN ('reservation-id', 'multiple-fields')
                      THEN ${corruptReservation}
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.reservationId'
                      ) END,
                    '$.targetClaimCloseEvidence.phase',
                    CASE WHEN ${corruption} = 'completion-phase'
                      THEN ${corruptPhase}
                      ELSE json_extract(
                        payload_json, '$.targetClaimCloseEvidence.phase'
                      ) END
                  )
                  WHERE command_id = ${committed.transitionCommandId}
                `;
                    yield* harness.sqlB`
                  UPDATE agent_control_command_receipts
                  SET command_id = CASE WHEN ${corruption} = 'multiple-fields'
                        THEN ${jointlyDerivedTransitionId}
                        WHEN ${corruption} = 'event-receipt'
                        THEN ${corruptTransitionId} ELSE command_id END,
                    command_fingerprint = CASE WHEN ${corruption} = 'multiple-fields'
                        THEN ${jointlyDerivedTransitionFingerprint}
                        WHEN ${corruption} = 'claim-receipt'
                        THEN ${corruptTransitionFingerprint} ELSE command_fingerprint END
                  WHERE command_id = ${committed.transitionCommandId}
                `;
                  }),
                );
                yield* harness.sqlB`PRAGMA ignore_check_constraints = OFF`;
                yield* Deferred.succeed(continueCompositeAccept, undefined);
                const result = yield* Fiber.join(operation);
                assert.equal(result._tag, "Failure", `${terminal}:${corruption}`);
                assert.deepStrictEqual(
                  yield* harness.sqlB`
                    SELECT status, pending_token AS "pendingToken"
                    FROM agent_control_worktree_controller_operations
                    WHERE command_id = ${input.commandId}
                  `,
                  [{ status: "pending", pendingToken: null }],
                  `${terminal}:${corruption}`,
                );
                assert.deepStrictEqual(
                  yield* worktreePersistenceCounts(committed.reservationId).pipe(
                    Effect.provideService(SqlClient.SqlClient, harness.sqlB),
                  ),
                  before,
                  `${terminal}:${corruption}`,
                );
                assert.deepStrictEqual(
                  yield* harness.sqlB`
                    SELECT close_anchor_pending_token AS "pendingToken",
                      close_anchor_claim_attempt_id AS "claimAttemptId",
                      close_anchor_expected_claim_revision AS "expectedRevision",
                      close_anchor_claim_revision AS "resultingRevision",
                      close_anchor_target_generation AS "targetGeneration",
                      close_anchor_reservation_id AS "reservationId",
                      close_anchor_phase AS phase,
                      close_anchor_transition_command_id AS "transitionCommandId",
                      close_anchor_transition_fingerprint AS "transitionFingerprint"
                    FROM agent_control_worktree_controller_operations
                    WHERE command_id = ${input.commandId}
                  `,
                  anchoredBefore,
                  `${terminal}:${corruption}:anchor`,
                );
                yield* Effect.yieldNow;
                assert.equal(yield* Ref.get(published), 1, `${terminal}:${corruption}`);
                const replay = yield* Effect.result(
                  terminal === "ready"
                    ? harness.controllerB.reserveAndMaterialize(
                        input as {
                          readonly commandId: CommandId;
                          readonly projectId: ProjectId;
                          readonly taskId: AgentControlTaskId;
                        },
                      )
                    : harness.controllerB.reconcile(
                        input as {
                          readonly commandId: CommandId;
                          readonly projectId: ProjectId;
                          readonly reservationId: AgentControlWorktreeReservationId;
                        },
                      ),
                );
                assert.equal(replay._tag, "Failure", `${terminal}:${corruption}:replay`);
                assert.deepStrictEqual(
                  yield* worktreePersistenceCounts(committed.reservationId).pipe(
                    Effect.provideService(SqlClient.SqlClient, harness.sqlB),
                  ),
                  before,
                  `${terminal}:${corruption}:replay`,
                );
                yield* Fiber.interrupt(publication);
              }),
            );
          }
        }
      }),
  );

  it.effect("never accepts a mutation committed after validation but before the terminal CAS", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const validated = yield* Deferred.make<void>();
        const continueAccept = yield* Deferred.make<void>();
        const harness = yield* makeIndependentControllerContexts(
          {
            afterCompositeClaim: () => Effect.void,
            afterLifecycleCheckpoint: () => Effect.void,
            afterReadyInspection: () => Effect.void,
            beforeCompositeAcceptUpdate: () =>
              Deferred.succeed(validated, undefined).pipe(
                Effect.andThen(Deferred.await(continueAccept)),
              ),
          },
          undefined,
          false,
        );
        yield* harness.sqlB`
          DROP TRIGGER agent_control_worktree_target_claim_authority_update
        `;
        yield* harness.sqlB`
          DROP TRIGGER agent_control_worktree_event_immutable_update
        `;
        const repo = yield* makeRepository();
        const projectId = ProjectId.make("worktree-post-validation-snapshot-conflict");
        const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
          Effect.provide(harness.contextA),
        );
        yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
        const input = {
          commandId: CommandId.make("worktree-post-validation-snapshot-conflict-command"),
          projectId,
          taskId: seeded.task.taskId,
        };
        const published = yield* Ref.make(0);
        const publication = yield* Context.get(
          harness.contextA,
          AgentControlWorktreeEngine,
        ).streamDomainEvents.pipe(
          Stream.filter((event) => event.type === "agentControl.worktree.ready"),
          Stream.runForEach(() => Ref.update(published, (count) => count + 1)),
          Effect.forkChild,
        );
        yield* Effect.yieldNow;
        const operation = yield* harness.controllerA
          .reserveAndMaterialize(input)
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(validated);
        const committed = (yield* harness.sqlB<{
          readonly reservationId: AgentControlWorktreeReservationId;
          readonly transitionCommandId: CommandId;
        }>`
          SELECT operation.worktree_reservation_id AS "reservationId",
            event.command_id AS "transitionCommandId"
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
        yield* harness.sqlB`
          UPDATE agent_control_events
          SET payload_json = json_set(
            payload_json,
            '$.targetClaimCloseEvidence.pendingToken',
            'post-validation-corrupt-token'
          )
          WHERE command_id = ${committed.transitionCommandId}
        `;
        yield* Deferred.succeed(continueAccept, undefined);
        const result = yield* Fiber.join(operation);
        assert.equal(result._tag, "Failure");
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT status, pending_token AS "pendingToken"
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [{ status: "pending", pendingToken: null }],
        );
        assert.deepStrictEqual(
          yield* worktreePersistenceCounts(committed.reservationId).pipe(
            Effect.provideService(SqlClient.SqlClient, harness.sqlB),
          ),
          before,
        );
        yield* Effect.yieldNow;
        assert.equal(yield* Ref.get(published), 1);
        const replay = yield* Effect.result(harness.controllerB.reserveAndMaterialize(input));
        assert.equal(replay._tag, "Failure");
        assert.deepStrictEqual(
          yield* worktreePersistenceCounts(committed.reservationId).pipe(
            Effect.provideService(SqlClient.SqlClient, harness.sqlB),
          ),
          before,
        );
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

  it.effect("rolls back both sides when either close-anchor CAS boundary aborts", () =>
    Effect.gen(function* () {
      for (const failurePoint of ["anchor", "target"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeIndependentControllerContexts();
            const repo = yield* makeRepository();
            const projectId = ProjectId.make(`worktree-close-anchor-rollback-${failurePoint}`);
            const seeded = yield* seedPrepared(projectId, repo.cwd).pipe(
              Effect.provide(harness.contextA),
            );
            yield* reserveLease(seeded.stageRun).pipe(Effect.provide(harness.contextA));
            if (failurePoint === "anchor") {
              yield* harness.sqlB`
                CREATE TRIGGER fail_worktree_close_anchor_anchor
                BEFORE UPDATE ON agent_control_worktree_controller_operations
                WHEN OLD.close_anchor_command_id IS NULL
                  AND NEW.close_anchor_command_id IS NOT NULL
                BEGIN
                  SELECT RAISE(ABORT, 'injected composite anchor CAS failure');
                END
              `;
            } else {
              yield* harness.sqlB`
                CREATE TRIGGER fail_worktree_close_anchor_target
                BEFORE UPDATE ON agent_control_worktree_target_claims
                WHEN NEW.phase IN ('materialized', 'retained-attention')
                BEGIN
                  SELECT RAISE(ABORT, 'injected target close CAS failure');
                END
              `;
            }
            const input = {
              commandId: CommandId.make(`worktree-close-anchor-rollback-command-${failurePoint}`),
              projectId,
              taskId: seeded.task.taskId,
            };
            const failed = yield* Effect.result(harness.controllerA.reserveAndMaterialize(input));
            assert.equal(failed._tag, "Failure", failurePoint);
            if (failed._tag === "Failure") {
              assert.equal(failed.failure.code, "internal-persistence-error", failurePoint);
            }
            assert.deepStrictEqual(
              yield* harness.sqlB`
                SELECT status, pending_token AS "pendingToken",
                  close_anchor_command_id AS "anchorCommandId"
                FROM agent_control_worktree_controller_operations
                WHERE command_id = ${input.commandId}
              `,
              [{ status: "pending", pendingToken: null, anchorCommandId: null }],
              failurePoint,
            );
            assert.equal(
              (yield* harness.sqlB<{ readonly count: number }>`
                SELECT COUNT(*) AS count
                FROM agent_control_worktree_target_claims
                WHERE command_id = ${input.commandId}
                  AND phase IN ('materialized', 'retained-attention')
              `)[0]!.count,
              0,
              failurePoint,
            );
            assert.equal(
              (yield* harness.sqlB<{ readonly count: number }>`
                SELECT COUNT(*) AS count
                FROM agent_control_events
                WHERE aggregate_kind = 'worktree-reservation'
                  AND event_type IN (
                    'agentControl.worktree.ready',
                    'agentControl.worktree.needsAttention'
                  )
                  AND stream_id = (
                    SELECT worktree_reservation_id
                    FROM agent_control_worktree_controller_operations
                    WHERE command_id = ${input.commandId}
                  )
              `)[0]!.count,
              0,
              failurePoint,
            );
            if (failurePoint === "anchor") {
              yield* harness.sqlB`DROP TRIGGER fail_worktree_close_anchor_anchor`;
            } else {
              yield* harness.sqlB`DROP TRIGGER fail_worktree_close_anchor_target`;
            }
            const recovered = yield* harness.controllerB.reserveAndMaterialize(input);
            assert.equal(recovered.status, "ready", failurePoint);
            assert.deepStrictEqual(
              yield* harness.sqlB`
                SELECT operation.status,
                  operation.close_anchor_phase AS "anchorPhase",
                  claim.phase AS "claimPhase"
                FROM agent_control_worktree_controller_operations AS operation
                JOIN agent_control_worktree_target_claims AS claim
                  ON claim.command_id = operation.command_id
                WHERE operation.command_id = ${input.commandId}
              `,
              [{ status: "accepted", anchorPhase: "materialized", claimPhase: "materialized" }],
              failurePoint,
            );
          }),
        );
      }
    }),
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
          readonly closedPendingToken: string;
          readonly closedClaimAttemptId: string;
          readonly anchorPendingToken: string;
          readonly anchorClaimAttemptId: string;
          readonly currentPendingToken: string | null;
        }>`
            SELECT claim.target_path AS "targetPath", claim.target_inode AS "targetInode",
              claim.closed_ownership_fingerprint AS marker,
              claim.closed_verified_at AS "verifiedAt",
              claim.closed_pending_token AS "closedPendingToken",
              claim.closed_claim_attempt_id AS "closedClaimAttemptId",
              operation.close_anchor_pending_token AS "anchorPendingToken",
              operation.close_anchor_claim_attempt_id AS "anchorClaimAttemptId",
              operation.pending_token AS "currentPendingToken"
            FROM agent_control_worktree_target_claims AS claim
            JOIN agent_control_worktree_controller_operations AS operation
              ON operation.command_id = claim.command_id
            WHERE claim.command_id = ${input.commandId} AND claim.phase = 'materialized'
          `)[0]!;
        assert.isNotNull(proof.marker);
        assert.isNotNull(proof.verifiedAt);
        assert.equal(proof.anchorPendingToken, proof.closedPendingToken);
        assert.equal(proof.anchorClaimAttemptId, proof.closedClaimAttemptId);
        assert.isNull(proof.currentPendingToken);
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
        assert.deepStrictEqual(
          yield* harness.sqlB`
            SELECT close_anchor_pending_token AS "pendingToken",
              close_anchor_claim_attempt_id AS "claimAttemptId"
            FROM agent_control_worktree_controller_operations
            WHERE command_id = ${input.commandId}
          `,
          [
            {
              pendingToken: proof.anchorPendingToken,
              claimAttemptId: proof.anchorClaimAttemptId,
            },
          ],
        );
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
            readonly closedPendingToken: string;
            readonly closedClaimAttemptId: string;
            readonly anchorPendingToken: string;
            readonly anchorClaimAttemptId: string;
            readonly currentPendingToken: string | null;
          }>`
            SELECT claim.target_path AS "targetPath", claim.target_inode AS "targetInode",
              claim.closed_attention_code AS "attentionCode",
              claim.closed_materialization_phase AS "materializationPhase",
              claim.closed_pending_token AS "closedPendingToken",
              claim.closed_claim_attempt_id AS "closedClaimAttemptId",
              operation.close_anchor_pending_token AS "anchorPendingToken",
              operation.close_anchor_claim_attempt_id AS "anchorClaimAttemptId",
              operation.pending_token AS "currentPendingToken"
            FROM agent_control_worktree_target_claims AS claim
            JOIN agent_control_worktree_controller_operations AS operation
              ON operation.command_id = claim.command_id
            WHERE claim.command_id = ${input.commandId}
              AND claim.phase = 'retained-attention'
          `)[0]!;
          assert.equal(proof.attentionCode, "worktree-dirty");
          assert.equal(proof.materializationPhase, "git-created");
          assert.equal(proof.anchorPendingToken, proof.closedPendingToken);
          assert.equal(proof.anchorClaimAttemptId, proof.closedClaimAttemptId);
          assert.isNull(proof.currentPendingToken);
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
          assert.deepStrictEqual(
            yield* harness.sqlB`
              SELECT close_anchor_pending_token AS "pendingToken",
                close_anchor_claim_attempt_id AS "claimAttemptId"
              FROM agent_control_worktree_controller_operations
              WHERE command_id = ${input.commandId}
            `,
            [
              {
                pendingToken: proof.anchorPendingToken,
                claimAttemptId: proof.anchorClaimAttemptId,
              },
            ],
          );
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
        yield* harness.sqlA`DROP TRIGGER agent_control_worktree_terminal_operation_target_guard`;
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
