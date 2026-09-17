import { makeAgentControlEpic } from "../../epic/Layers/AgentControlEpic.ts";
import { makeEpicResults, EpicCheckExecutor } from "../../epic/results.ts";
import { AgentControlEpicResultHooks } from "../../epic/Services/AgentControlEpicResultHooks.ts";
import { GithubIssueTrackerClient } from "../../github/Services/GithubIssueTrackerClient.ts";
import { epicIssueContentFingerprint } from "../../github/githubEpicSource.ts";
import { loadSelectedEpic, saveEpicRun } from "../../epic/authority.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlEpicProgress } from "../../epic/Services/AgentControlEpicProgress.ts";
import { createEpicRun, insertEpicRun } from "../../epic/runState.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  AgentControlTaskId,
  EventId,
  ModelSelection,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type AgentControlGithubIssueSnapshot,
  type AgentControlEpicSource,
  type AgentControlTaskEvent,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { makeReactorStartupActivation } from "../../../reactorStartupActivation.ts";
import { ServerConfig } from "../../../config.ts";
import * as GitManager from "../../../git/GitManager.ts";
import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import { OrchestrationLayerLive } from "../../../orchestration/runtimeLayer.ts";
import { ProviderTurnRequestExecutorLive } from "../../../orchestration/Layers/ProviderTurnRequestExecutor.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProjectionTurnRepositoryLive } from "../../../persistence/Layers/ProjectionTurns.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import * as ProviderSessionRuntime from "../../../persistence/ProviderSessionRuntime.ts";
import {
  attestProviderNativeTurnConfiguration,
  attestProviderSessionNativeConfiguration,
} from "../../../provider/Services/ProviderAdapter.ts";
import { ProviderService } from "../../../provider/Services/ProviderService.ts";
import { makeProviderRegistryLayer } from "../../../provider/testUtils/providerRegistryMock.ts";
import * as RepositoryIdentityResolver from "../../../project/RepositoryIdentityResolver.ts";
import * as GitVcsDriver from "../../../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../../vcs/VcsProcess.ts";
import { AgentControlPolicyService } from "../../AgentControlPolicyService.ts";
import { AgentControlControlledThreadActivationLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadActivation.ts";
import { AgentControlControlledThreadMaterializationCoordinatorLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadMaterializationCoordinator.ts";
import { AgentControlControlledThreadActivationHooks } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivationHooks.ts";
import { AgentControlControlledThreadMaterializationCoordinatorHooksNoop } from "../../controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinatorHooks.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlGithubEventStore } from "../../github/Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubProjection } from "../../github/Services/AgentControlGithubProjection.ts";
import { AgentControlImplementationAdmissionLive } from "../../implementationAdmission/Layers/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationAdmission } from "../../implementationAdmission/Services/AgentControlImplementationAdmission.ts";
import { AgentControlInitialPlanningConsumerLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningConsumer.ts";
import { AgentControlInitialPlanningFinalizerLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeupLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningWakeup.ts";
import { AgentControlInitialPlanningHandoffStore } from "../../initialPlanning/Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningConsumer } from "../../initialPlanning/Services/AgentControlInitialPlanningConsumer.ts";
import { AgentControlInitialPlanningWakeup } from "../../initialPlanning/Services/AgentControlInitialPlanningWakeup.ts";
import { AgentControlInitialPlanningFinalizer } from "../../initialPlanning/Services/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlImplementationHandoffStoreLive } from "../../implementationTurn/Layers/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationStageFinalizerLive } from "../../implementationTurn/Layers/AgentControlImplementationStageFinalizer.ts";
import { AgentControlImplementationStageStarterLive } from "../../implementationTurn/Layers/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationTurnConsumerLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnConsumer.ts";
import { AgentControlImplementationTurnCoordinatorLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlImplementationTurnWakeupLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnWakeup.ts";
import { AgentControlImplementationStageFinalizer } from "../../implementationTurn/Services/AgentControlImplementationStageFinalizer.ts";
import { AgentControlImplementationStageStarter } from "../../implementationTurn/Services/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationTurnConsumer } from "../../implementationTurn/Services/AgentControlImplementationTurnConsumer.ts";
import { AgentControlImplementationTurnCoordinator } from "../../implementationTurn/Services/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlImplementationTurnCoordinatorHooksNoop } from "../../implementationTurn/Services/AgentControlImplementationTurnCoordinatorHooks.ts";
import {
  AgentControlControlledThreadReservationLayerLive,
  AgentControlRuntimeLayerLive,
  AgentControlWorktreeControllerLayerLive,
} from "../../runtimeLayer.ts";
import { AgentControlRunOnceControllerLive } from "../../runOnce/Layers/AgentControlRunOnceController.ts";
import { AgentControlRunOnceController } from "../../runOnce/Services/AgentControlRunOnceController.ts";
import { providerAdmissionId } from "../../providerAdmission/model.ts";
import { ProviderAdmissionRuntime } from "../../providerAdmission/Services/ProviderAdmissionRuntime.ts";
import { ProviderAdmissionReleaseAuthority } from "../../providerAdmission/Services/ProviderAdmissionReleaseAuthority.ts";
import { AgentControlTaskEventStore } from "../../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntakeReactor } from "../../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskIntake } from "../../task/Services/AgentControlTaskIntake.ts";
import { AgentControlTaskVerificationFinalizerLive } from "../../task/Layers/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlTaskVerificationFinalizer } from "../../task/Services/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
import { AgentControlVerificationAdmissionLive } from "../../verificationAdmission/Layers/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationAdmission } from "../../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationAdmissionHooks } from "../../verificationAdmission/Services/AgentControlVerificationAdmissionHooks.ts";
import {
  executeVerificationCheck,
  prepareVerificationCheckManifest,
} from "../../verificationTurn/checkEvidence.ts";
import {
  makeBoundedVerificationResultCompletion,
  loadSealableVerificationResultSource,
} from "../../verificationTurn/orchestrationResultSource.ts";
import {
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
} from "../../verificationTurn/verificationResult.ts";
import type { ProviderAdmissionPermit } from "../../providerAdmission/model.ts";
import { AgentControlVerificationEvaluatorLive } from "../../verificationTurn/Layers/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationHandoffStoreLive } from "../../verificationTurn/Layers/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationStageFinalizerLive } from "../../verificationTurn/Layers/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationStageStarterLive } from "../../verificationTurn/Layers/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationTurnConsumerLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnConsumer.ts";
import { AgentControlVerificationTurnCoordinatorLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationTurnWakeupLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnWakeup.ts";
import { AgentControlVerificationStageFinalizer } from "../../verificationTurn/Services/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationStageStarter } from "../../verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationTurnConsumer } from "../../verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";
import { AgentControlVerificationTurnCoordinator } from "../../verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationTurnCoordinatorHooksNoop } from "../../verificationTurn/Services/AgentControlVerificationTurnCoordinatorHooks.ts";
import { AgentControlArmedScheduler } from "../Services/AgentControlArmedScheduler.ts";
import { layer as AgentControlArmedSchedulerLive } from "./AgentControlArmedScheduler.ts";

const projectId = ProjectId.make("armed-production-serial");
const providerInstanceId = ProviderInstanceId.make("armed-production-provider");
const provider = ProviderDriverKind.make("codex");
const modelSelection: ModelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.6",
  options: [{ id: "reasoning", value: "high" }],
};
const policyRoles = ["planner", "implementer", "verifier"] as const;
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const git = (cwd: string, args: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const driver = yield* GitVcsDriver.GitVcsDriver;
    return yield* driver.execute({
      operation: "AgentControlArmedProduction.test.git",
      cwd,
      args,
      allowNonZeroExit: false,
      timeoutMs: 10_000,
    });
  });

const makeRepository = Effect.fn("makeArmedProductionRepository")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-production-repository-" });
  yield* git(cwd, ["init", "-b", "main"]);
  yield* git(cwd, ["config", "user.email", "test@example.test"]);
  yield* git(cwd, ["config", "user.name", "Test"]);
  yield* fs.writeFileString(`${cwd}/README.md`, "base\n");
  yield* git(cwd, ["add", "README.md"]);
  yield* git(cwd, ["commit", "-m", "base"]);
  const baseCommitSha = (yield* git(cwd, ["rev-parse", "HEAD"])).stdout.trim();
  yield* git(cwd, ["remote", "add", "origin", "https://github.com/owner/repository.git"]);
  yield* git(cwd, ["update-ref", "refs/remotes/origin/main", baseCommitSha]);
  yield* git(cwd, ["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main"]);
  return { cwd, baseCommitSha };
});

const issue = (number: number, at: string): AgentControlGithubIssueSnapshot => ({
  repositoryNodeId: "armed-production-repository",
  issueNodeId: `armed-production-issue-${number}`,
  number,
  url: `https://example.test/owner/repository/issues/${number}`,
  state: "open",
  title: `Armed production task ${number}`,
  body: null,
  contentTrust: "untrusted-external",
  updatedAt: at,
  timelineComplete: true,
  timelineEvents: [],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
});

const makeProvider = Effect.fn("makeArmedProductionProvider")(function* () {
  const sessions = new Map<ThreadId, ProviderSession>();
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  let turnOrdinal = 0;
  const service = ProviderService.of({
    compactThread: () => Effect.die("Unexpected compactThread"),
    assertConversationRollbackSupported: () => Effect.void,
    uploadFeedback: () => Effect.die("Unexpected uploadFeedback"),
    startSession: (threadId, input) =>
      Effect.gen(function* () {
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const effectiveModel = input.modelSelection ?? modelSelection;
        const session = attestProviderSessionNativeConfiguration(
          {
            provider,
            providerInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? process.cwd(),
            model: effectiveModel.model,
            threadId,
            resumeCursor: null,
            createdAt,
            updatedAt: createdAt,
          },
          effectiveModel,
        );
        sessions.set(threadId, session);
        return session;
      }),
    sendTurn: (input) =>
      Effect.sync(() => ({
        threadId: input.threadId,
        turnId: TurnId.make(`armed-production-turn-${++turnOrdinal}`),
      })),
    sendTurnAtPreInvokeBoundary: (input, boundary) =>
      Effect.gen(function* () {
        yield* boundary.beforeDeliveryCas();
        const selection = input.modelSelection ?? modelSelection;
        yield* boundary.persistDeliveryAttempted(attestProviderNativeTurnConfiguration(selection));
        yield* boundary.afterDeliveryCas();
        boundary.onAdapterEntered?.();
        boundary.onExternalOperationStarted?.();
        const result = {
          threadId: input.threadId,
          turnId: TurnId.make(`armed-production-turn-${++turnOrdinal}`),
        };
        const session = sessions.get(input.threadId);
        if (session !== undefined) {
          sessions.set(input.threadId, {
            ...session,
            status: "running",
            activeTurnId: result.turnId,
            updatedAt: DateTime.formatIso(yield* DateTime.now),
          });
        }
        return result;
      }),
    getSessionAttestation: (threadId) =>
      Effect.sync(() => {
        const session = sessions.get(threadId);
        if (session === undefined) return undefined;
        return attestProviderSessionNativeConfiguration(session, modelSelection)
          .initialPlanningAttestation;
      }),
    interruptTurn: () => Effect.void,
    respondToRequest: () => Effect.void,
    respondToUserInput: () => Effect.void,
    stopSession: (input) => Effect.sync(() => void sessions.delete(input.threadId)),
    listSessions: () => Effect.sync(() => [...sessions.values()]),
    getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" as const }),
    getInstanceInfo: () =>
      Effect.succeed({
        instanceId: providerInstanceId,
        displayName: "Armed production provider",
        driverKind: provider,
        enabled: true,
        continuationIdentity: {
          driverKind: provider,
          continuationKey: `${provider}:instance:${providerInstanceId}`,
        },
      }),
    rollbackConversation: () => Effect.void,
    subscribeEvents: PubSub.subscribe(events),
    streamEvents: Stream.fromPubSub(events),
  });
  return {
    service,
    sessions,
    turnCount: () => turnOrdinal,
    publish: (event: ProviderRuntimeEvent) => PubSub.publish(events, event),
  };
});

const makePolicyLayer = () =>
  Layer.mock(AgentControlPolicyService)({
    preflightRuntime: () =>
      Effect.succeed({
        ok: true as const,
        staticPreflight: {
          ok: true as const,
          roles: policyRoles.map((role) => ({
            role,
            accessMode: "restricted" as const,
            strict: true,
            validCandidates: [
              {
                selection: modelSelection,
                source: "role-route" as const,
                driverKind: provider,
              },
            ],
          })),
        },
        roles: policyRoles.map((role) => ({
          role,
          accessMode: "restricted" as const,
          strict: true,
          candidates: [
            {
              candidateIndex: 0,
              source: "role-route" as const,
              providerInstanceId,
              model: modelSelection.model,
              driverKind: provider,
              providerStatus: "ready" as const,
              authStatus: "authenticated" as const,
              checkedAt: "2026-09-02T12:00:00.000Z",
              runtimeReady: true,
              errorCode: null,
            },
          ],
          selectedCandidateIndex: 0,
          errorCode: null,
        })),
      }),
  });

it.live.each([
  ...(
    [
      "paused",
      "ready-withdrawn",
      "closed",
      "human-takeover",
      "project-paused",
      "changed-before-integration",
      "dependency-before-integration",
      "integration-paused",
      "integration-content",
      "integration-dependency",
      "integration-incomplete",
    ] as const
  ).map((change) => ({
    blockedMode: "armed" as const,
    intakeRefreshes: 0 as const,
    recoverMissing: false as const,
    sourceChange: `epic-parallel-${change}` as const,
  })),
  {
    blockedMode: "armed",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "epic-parallel-polls",
  },
  {
    blockedMode: "armed",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "epic-parallel-preparation-poll",
  },
  {
    blockedMode: "armed",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "epic-parallel",
  },
  { blockedMode: "run-once", intakeRefreshes: 0, recoverMissing: false, sourceChange: "none" },
  { blockedMode: "armed", intakeRefreshes: 0, recoverMissing: false, sourceChange: "none" },
  { blockedMode: "armed", intakeRefreshes: 1, recoverMissing: false, sourceChange: "none" },
  { blockedMode: "armed", intakeRefreshes: 3, recoverMissing: false, sourceChange: "none" },
  { blockedMode: "armed", intakeRefreshes: 0, recoverMissing: true, sourceChange: "none" },
  { blockedMode: "armed", intakeRefreshes: 3, recoverMissing: false, sourceChange: "paused" },
  { blockedMode: "armed", intakeRefreshes: 3, recoverMissing: false, sourceChange: "replaced" },
  {
    blockedMode: "run-once",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "terminal-retry",
  },
  {
    blockedMode: "run-once",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "terminal-retry-shared-git-dir",
  },
  {
    blockedMode: "run-once",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "terminal-retry-cancelled",
  },
  {
    blockedMode: "run-once",
    intakeRefreshes: 0,
    recoverMissing: false,
    sourceChange: "terminal-retry-cancelled-shared-git-dir",
  },
] as const)(
  "isolates a persisted $blockedMode project blocker with $intakeRefreshes intake refreshes (recover missing: $recoverMissing, source change: $sourceChange) while independent tasks run and fresh authority resolves it",
  ({ blockedMode, intakeRefreshes, recoverMissing, sourceChange }) =>
    Effect.scoped(
      Effect.gen(function* () {
        const epicParallel = sourceChange.startsWith("epic-parallel");
        const parallelPolling =
          sourceChange === "epic-parallel-polls" ||
          sourceChange === "epic-parallel-preparation-poll" ||
          sourceChange === "epic-parallel-integration-incomplete";
        const revokeDuringIntegration =
          sourceChange === "epic-parallel-integration-paused" ||
          sourceChange === "epic-parallel-integration-content" ||
          sourceChange === "epic-parallel-integration-dependency";
        let beforeInitialThreadMaterialization = Effect.void;
        let preparationPolls = 0;
        let parallelPollVersion = 2;
        const sourceBeforeIntegration =
          sourceChange === "epic-parallel-changed-before-integration" ||
          sourceChange === "epic-parallel-dependency-before-integration";
        const cancelledRetry =
          sourceChange === "terminal-retry-cancelled" ||
          sourceChange === "terminal-retry-cancelled-shared-git-dir";
        const terminalRetry =
          sourceChange === "terminal-retry" ||
          sourceChange === "terminal-retry-shared-git-dir" ||
          cancelledRetry;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-armed-production-" });
        const filename = path.join(directory, "state.sqlite");
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const sqlContext = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`PRAGMA foreign_keys = ON`;
        yield* runMigrations(
          sourceChange === "epic-parallel-polls" ? { toMigrationInclusive: 91 } : {},
        ).pipe(Effect.provideService(SqlClient.SqlClient, sql));
        const fakeProvider = yield* makeProvider();
        const verificationPermits = new Map<string, ProviderAdmissionPermit>();
        const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
        const configLayer = ServerConfig.layerTest(directory, {
          prefix: "armed-production-",
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
        const repository = yield* makeRepository().pipe(Effect.provide(gitLayer));
        const runtime = AgentControlRuntimeLayerLive.pipe(
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const orchestration = OrchestrationLayerLive.pipe(
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(RepositoryIdentityResolver.layer),
          Layer.provideMerge(configLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const worktree = AgentControlWorktreeControllerLayerLive.pipe(
          Layer.provideMerge(runtime),
          Layer.provideMerge(workflowLayer),
          Layer.provideMerge(gitLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(configLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const reservations = AgentControlControlledThreadReservationLayerLive.pipe(
          Layer.provideMerge(runtime),
          Layer.provideMerge(worktree),
          Layer.provideMerge(workflowLayer),
          Layer.provideMerge(gitLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(configLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const policyLayer = makePolicyLayer();
        const coordinator = Layer.fresh(
          AgentControlControlledThreadMaterializationCoordinatorLive,
        ).pipe(
          Layer.provideMerge(runtime),
          Layer.provideMerge(reservations),
          Layer.provideMerge(worktree),
          Layer.provideMerge(policyLayer),
          Layer.provideMerge(orchestration),
          Layer.provide(AgentControlControlledThreadMaterializationCoordinatorHooksNoop),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(configLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const activation = Layer.fresh(AgentControlControlledThreadActivationLive).pipe(
          Layer.provideMerge(reservations),
          Layer.provideMerge(coordinator),
          Layer.provide(
            Layer.succeed(AgentControlControlledThreadActivationHooks, {
              afterPrepareAcceptedBeforeMaterialize: (observation) =>
                observation.projectId === projectId
                  ? beforeInitialThreadMaterialization
                  : Effect.void,
              afterMaterializationAcceptedBeforeReturn: () => Effect.void,
            }),
          ),
        );
        const runOnce = Layer.fresh(AgentControlRunOnceControllerLive).pipe(
          Layer.provideMerge(runtime),
          Layer.provideMerge(worktree),
          Layer.provideMerge(activation),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const intake = Layer.succeed(
          AgentControlTaskIntakeReactor,
          AgentControlTaskIntakeReactor.of({
            start: () => Effect.void,
            getStatus: () => Effect.die("unused"),
            subscribeCompletions: Effect.succeed(Stream.empty),
          }),
        );
        const armed = Layer.fresh(AgentControlArmedSchedulerLive).pipe(
          Layer.provide(
            Layer.succeed(AgentControlEpicProgress, { processProject: () => Effect.void }),
          ),
          Layer.provideMerge(runOnce),
          Layer.provideMerge(runtime),
          Layer.provideMerge(intake),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const context = yield* Layer.buildWithScope(
          Layer.mergeAll(
            armed,
            runtime,
            orchestration,
            worktree,
            reservations,
            coordinator,
            activation,
            policyLayer,
          ),
          scope,
        );
        const engine = Context.get(context, AgentControlEngine);
        const githubEvents = Context.get(context, AgentControlGithubEventStore);
        const githubProjection = Context.get(context, AgentControlGithubProjection);
        const taskEngine = Context.get(context, AgentControlTaskEngine);
        const taskIntake = Context.get(context, AgentControlTaskIntake);
        const scheduler = Context.get(context, AgentControlArmedScheduler);
        assert.isDefined(Context.get(context, AgentControlRunOnceController));
        assert.isDefined(Context.get(context, OrchestrationEngineService));
        assert.isDefined(Context.get(context, ProjectionSnapshotQuery));
        assert.isDefined(fakeProvider.service);
        assert.isDefined(ProviderTurnRequestExecutorLive);
        assert.isDefined(ProjectionTurnRepositoryLive);
        assert.isDefined(ProviderSessionRuntime.layer);

        const publishSources = Effect.fn("publishArmedProductionSources")(function* (
          projectId: ProjectId,
          issues: ReadonlyArray<AgentControlGithubIssueSnapshot>,
          streamVersion: number,
          reconcileTasks = true,
        ) {
          const at = DateTime.formatIso(yield* DateTime.now);
          const polled = yield* githubEvents.append({
            projectId,
            expectedStreamVersion: streamVersion,
            events: [
              {
                eventId: EventId.make(`${projectId}-${streamVersion}-poll-event`),
                type: "agentControl.github.poll.succeeded",
                aggregateKind: "github-intake",
                aggregateId: projectId,
                occurredAt: at,
                commandId: CommandId.make(`${projectId}-${streamVersion}-poll-command`),
                causationEventId: null,
                correlationId: CommandId.make(`${projectId}-${streamVersion}-poll-command`),
                authority: "controller",
                payload: {
                  projectId,
                  repository: {
                    repositoryNodeId: "armed-production-repository",
                    nameWithOwner: "owner/repository",
                  },
                  attemptedAt: at,
                  completedAt: at,
                  cursor: { lastSuccessfulPollAt: at, overlapSeconds: 60 },
                  issues,
                },
                metadata: { schemaVersion: 1 },
              },
            ],
          });
          yield* githubProjection.projectEvent(polled[0]!);
          if (reconcileTasks) yield* taskIntake.reconcileObservedProject({ projectId });
        });
        const seedProject = Effect.fn("seedArmedProductionProject")(function* (
          projectId: ProjectId,
          repository: { readonly cwd: string },
          issues: ReadonlyArray<AgentControlGithubIssueSnapshot>,
          mode: "armed" | "run-once",
        ) {
          const at = DateTime.formatIso(yield* DateTime.now);
          yield* Context.get(context, OrchestrationEngineService).dispatch({
            type: "project.create",
            commandId: CommandId.make(`${projectId}-create`),
            projectId,
            title: "Armed production project",
            workspaceRoot: repository.cwd,
            defaultModelSelection: modelSelection,
            createdAt: at,
          });
          yield* engine.dispatchHuman({
            commandId: CommandId.make(`${projectId}-observe`),
            projectId,
            expectedRevision: 0,
            mode: "observe",
          });
          const configured = yield* githubEvents.append({
            projectId,
            expectedStreamVersion: 0,
            events: [
              {
                eventId: EventId.make(`${projectId}-config-event`),
                type: "agentControl.github.config.set",
                aggregateKind: "github-intake",
                aggregateId: projectId,
                occurredAt: at,
                commandId: CommandId.make(`${projectId}-config-command`),
                causationEventId: null,
                correlationId: CommandId.make(`${projectId}-config-command`),
                authority: "human",
                payload: {
                  projectId,
                  settings: {
                    trackerKind: "github",
                    readyLabel: "agent:ready",
                    pausedLabel: "agent:paused",
                    trustedLogins: [],
                    pollIntervalSeconds: 60,
                  },
                  repository: {
                    repositoryNodeId: "armed-production-repository",
                    nameWithOwner: "owner/repository",
                  },
                  configuredAt: at,
                },
                metadata: { schemaVersion: 1 },
              },
            ],
          });
          yield* githubProjection.projectEvent(configured[0]!);

          yield* publishSources(projectId, issues, 1);
          yield* engine.dispatchHuman({
            commandId: CommandId.make(`${projectId}-arm`),
            projectId,
            expectedRevision: 1,
            mode,
          });
        });
        const at = DateTime.formatIso(yield* DateTime.now);
        const issues = epicParallel
          ? [issue(1, at), issue(2, at), issue(3, at)]
          : [issue(1, at), issue(2, at)];
        yield* seedProject(projectId, repository, issues, "armed");
        let epicSource: AgentControlEpicSource | undefined;
        if (epicParallel) {
          const epicIssue = (entry: AgentControlGithubIssueSnapshot) => ({
            repositoryNodeId: entry.repositoryNodeId,
            nameWithOwner: "owner/repository",
            issueNodeId: entry.issueNodeId,
            number: entry.number,
            title: entry.title,
            contentFingerprint: epicIssueContentFingerprint(entry),
            url: entry.url,
            state: entry.state,
            subIssueCount: 0,
          });
          const source = {
            format: "github-native-sub-issues-v1" as const,
            repository: {
              repositoryNodeId: "armed-production-repository",
              nameWithOwner: "owner/repository",
            },
            epic: { ...epicIssue(issues[0]!), subIssueCount: 3 },
            tasks: issues.map((entry, position) => ({
              issue: epicIssue(entry),
              position,
              dependencies: [],
            })),
            blockers: [],
            fingerprint: "explicit-independent-markdown-tasks",
            inspectedAt: at,
          };
          epicSource = source;
          const run = yield* createEpicRun({
            projectId,
            commandId: "parallel-epic",
            source,
            checks: [
              {
                id: "integrated-clean-diff",
                command: "git",
                args: ["diff", "--check"],
                cwd: ".",
                required: true,
                timeoutMs: 10_000,
                allowTemporaryFiles: false,
                resultFormat: "exit-code",
              },
            ],
            parallelism: 2,
            initialBase: { commitSha: repository.baseCommitSha, targetBranch: "main" },
            dependencyPlan: {
              version: 1,
              sourceFingerprint: source.fingerprint,
              rationale: "A and B own separate Markdown files. C combines both outputs.",
              tasks: issues.map((entry, index) => ({
                issueNodeId: entry.issueNodeId,
                dependsOn: index === 2 ? [issues[0]!.issueNodeId, issues[1]!.issueNodeId] : [],
              })),
            },
          });
          const members = yield* Effect.forEach(run.members, (member, index) =>
            Effect.gen(function* () {
              const taskId = yield* deriveAgentControlTaskId({
                projectId,
                repositoryNodeId: source.repository.repositoryNodeId,
                issueNodeId: member.issueNodeId,
              });
              return {
                ...member,
                taskId,
                status: index < 2 ? ("running" as const) : ("pending" as const),
                baseCommitSha: repository.baseCommitSha,
              };
            }),
          );
          yield* insertEpicRun(sql, { ...run, members, activeTaskId: members[0]!.taskId });
        }
        if (sourceChange === "epic-parallel-preparation-poll") {
          beforeInitialThreadMaterialization = Effect.gen(function* () {
            if (preparationPolls > 0) return;
            preparationPolls++;
            yield* publishSources(projectId, issues, parallelPollVersion++);
          }).pipe(Effect.orDie);
        }
        for (let refresh = 0; refresh < intakeRefreshes; refresh++) {
          yield* publishSources(projectId, issues, 2 + refresh);
        }
        if (recoverMissing) {
          yield* publishSources(projectId, [], 2);
          yield* publishSources(projectId, issues, 3);
        }
        const refreshedTaskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: issues[0]!.repositoryNodeId,
          issueNodeId: issues[0]!.issueNodeId,
        });
        const refreshedTask = Option.getOrThrow(yield* taskEngine.get(refreshedTaskId));
        assert.equal(refreshedTask.revision, recoverMissing ? 3 : intakeRefreshes + 1);
        assert.equal(refreshedTask.status, "candidate");
        assert.equal(refreshedTask.sourceGate, "eligible");
        const blockedProjectId = ProjectId.make("a-blocked-recovery-project");
        const blockedRepository = yield* makeRepository().pipe(Effect.provide(gitLayer));
        yield* git(blockedRepository.cwd, [
          "symbolic-ref",
          "--delete",
          "refs/remotes/origin/HEAD",
        ]).pipe(Effect.provide(gitLayer));
        yield* seedProject(blockedProjectId, blockedRepository, [issue(101, at)], blockedMode);
        if (blockedMode === "armed") {
          // Establish the automatic activation and its durable rejection before restart.
          yield* scheduler.processProject(blockedProjectId).pipe(Effect.exit);
        }
        const controller = Context.get(context, AgentControlRunOnceController);
        const blocked = yield* controller.processProject(blockedProjectId).pipe(Effect.flip);
        assert.equal(blocked.reason, "downstream-rejected");
        assert.deepStrictEqual(
          yield* sql`SELECT status, rejection_code AS code
            FROM agent_control_worktree_controller_operations WHERE project_id=${blockedProjectId}`,
          [{ status: "rejected", code: "default-remote-ref-unavailable" }],
        );
        yield* git(blockedRepository.cwd, [
          "symbolic-ref",
          "refs/remotes/origin/HEAD",
          "refs/remotes/origin/main",
        ]).pipe(Effect.provide(gitLayer));
        const rejectedOperations = yield* sql`SELECT *
          FROM agent_control_worktree_controller_operations WHERE project_id=${blockedProjectId}`;
        const blockedState = yield* sql`SELECT * FROM agent_control_run_once_states
          WHERE project_id=${blockedProjectId}`;
        const restartRecovery = Effect.fn("restartArmedProductionRecovery")(function* () {
          yield* Effect.scoped(
            Effect.gen(function* () {
              const restarted = yield* Layer.build(
                Layer.fresh(AgentControlRunOnceControllerLive).pipe(
                  Layer.provide(Layer.succeedContext(context)),
                ),
              );
              const startup = yield* makeReactorStartupActivation;
              yield* Context.get(restarted, AgentControlRunOnceController).prepare(startup);
              const restartedArmed = yield* Layer.build(
                Layer.fresh(AgentControlArmedSchedulerLive).pipe(
                  Layer.provide(
                    Layer.succeed(AgentControlEpicProgress, { processProject: () => Effect.void }),
                  ),
                  Layer.provide(Layer.succeedContext(Context.merge(context, restarted))),
                ),
              );
              yield* Context.get(restartedArmed, AgentControlArmedScheduler).prepare(startup);
            }),
          );
        });
        yield* restartRecovery();
        yield* restartRecovery();
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_worktree_controller_operations
          WHERE project_id=${blockedProjectId}`,
          rejectedOperations,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_run_once_states
          WHERE project_id=${blockedProjectId}`,
          blockedState,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT error_code AS code
          FROM agent_control_run_once_diagnostics WHERE project_id=${blockedProjectId}`,
          [{ code: "downstream-rejected: default-remote-ref-unavailable" }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT count(*) AS count
          FROM agent_control_initial_planning_handoff_intents intent
          JOIN agent_control_initial_planning_handoff_accepted accepted USING (handoff_id)
          WHERE intent.project_id=${blockedProjectId}`,
          [{ count: 0 }],
        );

        yield* scheduler.processProject(projectId);
        const firstTaskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: issues[0]!.repositoryNodeId,
          issueNodeId: issues[0]!.issueNodeId,
        });
        assert.deepStrictEqual(
          yield* sql`
            SELECT project.mode,
              (SELECT selected_task_id FROM agent_control_armed_dispatch_evidence
               WHERE project_id=${projectId})
                AS selectedTask,
              (SELECT count(*) FROM agent_control_initial_planning_handoff_accepted)
                AS planningHandoffs
            FROM agent_control_project_states project WHERE project_id=${projectId}
          `,
          epicParallel
            ? [{ mode: "armed", selectedTask: null, planningHandoffs: 2 }]
            : [{ mode: "run-once", selectedTask: firstTaskId, planningHandoffs: 1 }],
        );
        assert.lengthOf(
          yield* sql`
            SELECT thread_id FROM projection_threads
            WHERE thread_id IN (SELECT thread_id FROM agent_control_initial_planning_handoff_accepted)
          `,
          epicParallel ? 2 : 1,
        );

        const coreServices = Layer.succeedContext(context);
        const providerServiceLayer = Layer.succeed(ProviderService, fakeProvider.service);
        const providerRuntime = Layer.fresh(ProviderSessionRuntime.layer).pipe(
          Layer.provide(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const projectionTurns = Layer.fresh(ProjectionTurnRepositoryLive).pipe(
          Layer.provide(sqlLayer),
        );
        const executor = Layer.fresh(ProviderTurnRequestExecutorLive).pipe(
          Layer.provideMerge(coreServices),
          Layer.provideMerge(providerServiceLayer),
          Layer.provideMerge(makeProviderRegistryLayer()),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const planningStore = Layer.fresh(AgentControlInitialPlanningHandoffStoreLive).pipe(
          Layer.provide(sqlLayer),
        );
        const planningWakeupContext = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlInitialPlanningWakeupLive),
          scope,
        );
        const planningWakeupService = Context.get(
          planningWakeupContext,
          AgentControlInitialPlanningWakeup,
        );
        const planningWakeup = Layer.succeed(
          AgentControlInitialPlanningWakeup,
          planningWakeupService,
        );
        const providerAdmissionRuntime = ProviderAdmissionRuntime.of({
          awaitFailure: Effect.never,
          request: (request) =>
            Effect.sync(() => {
              const permit: ProviderAdmissionPermit = {
                ...request,
                admissionId: providerAdmissionId(request),
                admissionMarkerId: `marker-${request.handoffId}`,
                admissionMarkerFingerprint: "a".repeat(64),
                admissionOwnerId: "armed-production-test-owner",
                admissionLeaseExpiresAt: "2099-01-01T00:00:00.000Z",
                providerFenceToken: 1,
                usageEvidenceFingerprint: "b".repeat(64),
              };
              verificationPermits.set(request.handoffId, permit);
              return { _tag: "Admitted" as const, permit };
            }),
          usageChanged: () => Effect.void,
          capacityReleased: () => Effect.void,
        });
        const providerAdmissionRelease = ProviderAdmissionReleaseAuthority.of({
          releaseInTransaction: () => Effect.succeed(null),
          signalCommitted: () => Effect.void,
          recover: Effect.void,
        });
        const providerAdmissionRuntimeLayer = Layer.succeed(
          ProviderAdmissionRuntime,
          providerAdmissionRuntime,
        );
        const providerAdmissionReleaseLayer = Layer.succeed(
          ProviderAdmissionReleaseAuthority,
          providerAdmissionRelease,
        );
        const planningConsumer = Layer.fresh(AgentControlInitialPlanningConsumerLive).pipe(
          Layer.provideMerge(planningStore),
          Layer.provideMerge(planningWakeup),
          Layer.provideMerge(executor),
          Layer.provideMerge(projectionTurns),
          Layer.provideMerge(providerRuntime),
          Layer.provideMerge(providerServiceLayer),
          Layer.provideMerge(providerAdmissionRuntimeLayer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(NodeServices.layer),
        );
        const planningFinalizer = Layer.fresh(AgentControlInitialPlanningFinalizerLive).pipe(
          Layer.provideMerge(planningStore),
          Layer.provideMerge(planningWakeup),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(providerAdmissionReleaseLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const implementationStore = Layer.fresh(AgentControlImplementationHandoffStoreLive).pipe(
          Layer.provide(sqlLayer),
        );
        const implementationWakeup = Layer.fresh(AgentControlImplementationTurnWakeupLive);
        const implementationAdmission = Layer.fresh(AgentControlImplementationAdmissionLive).pipe(
          Layer.provideMerge(planningFinalizer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const implementationCoordinator = Layer.fresh(
          AgentControlImplementationTurnCoordinatorLive,
        ).pipe(
          Layer.provideMerge(implementationAdmission),
          Layer.provideMerge(implementationStore),
          Layer.provideMerge(implementationWakeup),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(policyLayer),
          Layer.provide(AgentControlImplementationTurnCoordinatorHooksNoop),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const implementationConsumer = Layer.fresh(AgentControlImplementationTurnConsumerLive).pipe(
          Layer.provideMerge(implementationStore),
          Layer.provideMerge(implementationWakeup),
          Layer.provideMerge(executor),
          Layer.provideMerge(projectionTurns),
          Layer.provideMerge(providerServiceLayer),
          Layer.provideMerge(providerAdmissionRuntimeLayer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const implementationStarter = Layer.fresh(AgentControlImplementationStageStarterLive).pipe(
          Layer.provideMerge(implementationStore),
          Layer.provideMerge(implementationWakeup),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const implementationFinalizer = Layer.fresh(
          AgentControlImplementationStageFinalizerLive,
        ).pipe(
          Layer.provideMerge(implementationStore),
          Layer.provideMerge(implementationWakeup),
          Layer.provideMerge(implementationStarter),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(providerAdmissionReleaseLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationStore = Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
          Layer.provide(sqlLayer),
        );
        const verificationWakeup = Layer.fresh(AgentControlVerificationTurnWakeupLive);
        const verificationAdmission = Layer.fresh(AgentControlVerificationAdmissionLive).pipe(
          Layer.provideMerge(implementationStore),
          Layer.provideMerge(implementationFinalizer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationCoordinator = Layer.fresh(
          AgentControlVerificationTurnCoordinatorLive,
        ).pipe(
          Layer.provideMerge(verificationAdmission),
          Layer.provideMerge(verificationStore),
          Layer.provideMerge(verificationWakeup),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(policyLayer),
          Layer.provide(AgentControlVerificationTurnCoordinatorHooksNoop),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationConsumer = Layer.fresh(AgentControlVerificationTurnConsumerLive).pipe(
          Layer.provideMerge(verificationStore),
          Layer.provideMerge(verificationWakeup),
          Layer.provideMerge(executor),
          Layer.provideMerge(projectionTurns),
          Layer.provideMerge(providerServiceLayer),
          Layer.provideMerge(providerAdmissionRuntimeLayer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationStarter = Layer.fresh(AgentControlVerificationStageStarterLive).pipe(
          Layer.provideMerge(verificationStore),
          Layer.provideMerge(verificationWakeup),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationEvaluator = Layer.fresh(AgentControlVerificationEvaluatorLive).pipe(
          Layer.provideMerge(verificationStore),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const verificationFinalizer = Layer.fresh(AgentControlVerificationStageFinalizerLive).pipe(
          Layer.provideMerge(verificationStore),
          Layer.provideMerge(verificationWakeup),
          Layer.provideMerge(verificationEvaluator),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(providerAdmissionReleaseLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const taskFinalizer = Layer.fresh(AgentControlTaskVerificationFinalizerLive).pipe(
          Layer.provideMerge(verificationFinalizer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(NodeServices.layer),
        );
        const pipelineContext = yield* Layer.buildWithScope(
          Layer.mergeAll(
            planningConsumer,
            planningFinalizer,
            implementationAdmission,
            implementationCoordinator,
            implementationConsumer,
            implementationStarter,
            implementationFinalizer,
            verificationAdmission,
            verificationCoordinator,
            verificationConsumer,
            verificationStarter,
            verificationEvaluator,
            verificationFinalizer,
            taskFinalizer,
          ),
          scope,
        );
        assert.isDefined(Context.get(pipelineContext, AgentControlInitialPlanningConsumer));
        assert.isDefined(Context.get(pipelineContext, AgentControlInitialPlanningFinalizer));
        assert.isDefined(Context.get(pipelineContext, AgentControlImplementationAdmission));
        assert.isDefined(Context.get(pipelineContext, AgentControlImplementationTurnCoordinator));
        assert.isDefined(Context.get(pipelineContext, AgentControlImplementationTurnConsumer));
        assert.isDefined(Context.get(pipelineContext, AgentControlImplementationStageStarter));
        assert.isDefined(Context.get(pipelineContext, AgentControlImplementationStageFinalizer));
        assert.isDefined(Context.get(pipelineContext, AgentControlVerificationAdmission));
        assert.isDefined(Context.get(pipelineContext, AgentControlVerificationTurnCoordinator));
        assert.isDefined(Context.get(pipelineContext, AgentControlVerificationTurnConsumer));
        assert.isDefined(Context.get(pipelineContext, AgentControlVerificationStageStarter));
        assert.isDefined(Context.get(pipelineContext, AgentControlVerificationStageFinalizer));
        assert.isDefined(Context.get(pipelineContext, AgentControlTaskVerificationFinalizer));

        const planningConsumerService = Context.get(
          pipelineContext,
          AgentControlInitialPlanningConsumer,
        );
        const planningFinalizerService = Context.get(
          pipelineContext,
          AgentControlInitialPlanningFinalizer,
        );
        const implementationAdmissionService = Context.get(
          pipelineContext,
          AgentControlImplementationAdmission,
        );
        const implementationCoordinatorService = Context.get(
          pipelineContext,
          AgentControlImplementationTurnCoordinator,
        );
        const implementationConsumerService = Context.get(
          pipelineContext,
          AgentControlImplementationTurnConsumer,
        );
        const implementationStarterService = Context.get(
          pipelineContext,
          AgentControlImplementationStageStarter,
        );
        const implementationFinalizerService = Context.get(
          pipelineContext,
          AgentControlImplementationStageFinalizer,
        );
        const verificationAdmissionService = Context.get(
          pipelineContext,
          AgentControlVerificationAdmission,
        );
        const verificationCoordinatorService = Context.get(
          pipelineContext,
          AgentControlVerificationTurnCoordinator,
        );
        const verificationConsumerService = Context.get(
          pipelineContext,
          AgentControlVerificationTurnConsumer,
        );
        const verificationStarterService = Context.get(
          pipelineContext,
          AgentControlVerificationStageStarter,
        );
        const verificationFinalizerService = Context.get(
          pipelineContext,
          AgentControlVerificationStageFinalizer,
        );
        const taskFinalizerService = Context.get(
          pipelineContext,
          AgentControlTaskVerificationFinalizer,
        );
        const orchestrationEngine = Context.get(context, OrchestrationEngineService);
        const projectProviderStarted = Effect.fn("projectArmedProductionProviderStarted")(
          function* (input: {
            readonly prefix: string;
            readonly threadId: ThreadId;
            readonly turnId: TurnId;
            readonly runtimeMode: "approval-required" | "full-access";
            readonly acceptedAt: string;
          }) {
            const eventId = EventId.make(`${input.prefix}-started-event`);
            yield* orchestrationEngine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:${eventId}:thread-session-set`),
              threadId: input.threadId,
              session: {
                threadId: input.threadId,
                status: "running",
                providerName: provider,
                providerInstanceId,
                runtimeMode: input.runtimeMode,
                activeTurnId: input.turnId,
                lastError: null,
                updatedAt: input.acceptedAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: eventId,
                runtimeEventType: "turn.started",
                providerInstanceId,
                providerTurnId: input.turnId,
              },
              createdAt: input.acceptedAt,
            });
          },
        );
        const projectProviderTerminal = Effect.fn("projectArmedProductionProviderTerminal")(
          function* (input: {
            readonly prefix: string;
            readonly event: Extract<ProviderRuntimeEvent, { readonly type: "turn.completed" }>;
            readonly runtimeMode: "approval-required" | "full-access";
          }) {
            if (input.event.turnId === undefined) {
              return yield* Effect.die(new Error("terminal provider event is missing turnId"));
            }
            const verificationPermit = [...verificationPermits.values()].find(
              (permit) =>
                permit.stage === "verification" && permit.threadId === input.event.threadId,
            );
            const resultSource =
              verificationPermit === undefined || input.event.payload.state !== "completed"
                ? undefined
                : yield* loadSealableVerificationResultSource(sql, {
                    threadId: input.event.threadId,
                    providerInstanceId,
                    providerTurnId: input.event.turnId,
                    afterStreamVersion: 4,
                    handoffId: verificationPermit.handoffId,
                    providerDeliveryId: verificationPermit.providerDeliveryId,
                    resultSchemaFingerprint: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
                  });
            yield* orchestrationEngine.dispatch({
              type: "thread.session.set",
              ...(verificationPermit === undefined || resultSource === undefined
                ? {}
                : {
                    verificationResultSource: {
                      schemaVersion: 1,
                      handoffId: verificationPermit.handoffId,
                      providerDeliveryId: verificationPermit.providerDeliveryId,
                      providerInstanceId,
                      providerTurnId: input.event.turnId,
                      resultSchemaFingerprint: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
                      sourceDisposition: resultSource.sourceDisposition,
                      finalMessageId: resultSource.finalMessageId,
                      sourceEventId: resultSource.sourceEventId,
                      outputDigest: resultSource.outputDigest,
                      outputByteLength: resultSource.outputByteLength,
                    },
                  }),
              commandId: CommandId.make(
                `provider:${input.event.eventId}:thread-session-set:00000000-0000-4000-8000-000000000001`,
              ),
              threadId: input.event.threadId,
              session: {
                threadId: input.event.threadId,
                status: input.event.payload.state === "failed" ? "error" : "ready",
                providerName: provider,
                providerInstanceId,
                runtimeMode: input.runtimeMode,
                activeTurnId: null,
                lastError: input.event.payload.state === "failed" ? "provider failed" : null,
                updatedAt: input.event.createdAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: input.event.eventId,
                runtimeEventType: "turn.completed",
                providerInstanceId,
                providerTurnId: input.event.turnId,
                providerState: input.event.payload.state,
              },
              createdAt: input.event.createdAt,
            });
          },
        );
        const publishTerminal = Effect.fn("publishArmedProductionTerminal")(function* (input: {
          readonly prefix: string;
          readonly threadId: ThreadId;
          readonly turnId: TurnId;
          readonly state: "completed" | "failed" | "interrupted";
        }) {
          const terminalAt = DateTime.formatIso(yield* DateTime.now);
          const session = fakeProvider.sessions.get(input.threadId);
          assert.isDefined(session);
          const {
            activeTurnId: previousActiveTurnId,
            lastError: previousLastError,
            ...terminalBase
          } = session!;
          void previousActiveTurnId;
          void previousLastError;
          const terminalSession: ProviderSession = {
            ...terminalBase,
            status: input.state === "failed" ? "error" : "ready",
            updatedAt: terminalAt,
            ...(input.state === "failed" ? { lastError: "provider failed" } : {}),
          };
          fakeProvider.sessions.set(input.threadId, terminalSession);
          const event = {
            type: "turn.completed",
            eventId: EventId.make(`${input.prefix}-terminal-event`),
            provider,
            providerInstanceId,
            threadId: input.threadId,
            turnId: input.turnId,
            createdAt: terminalAt,
            payload: { state: input.state },
          } satisfies ProviderRuntimeEvent;
          yield* fakeProvider.publish(event);
          return event;
        });

        const startedEvent = (threadId: ThreadId): ProviderRuntimeEvent => {
          const session = fakeProvider.sessions.get(threadId)!;
          assert.isDefined(session.activeTurnId);
          return {
            type: "turn.started",
            eventId: EventId.make(`started:${session.activeTurnId}`),
            provider,
            providerInstanceId,
            threadId,
            turnId: session.activeTurnId!,
            createdAt: session.updatedAt,
            payload: {},
          };
        };
        yield* planningConsumerService.start().pipe(Scope.provide(scope));
        if (epicParallel) {
          const handoffs = yield* sql<{
            handoffId: string;
          }>`SELECT handoff_id AS "handoffId" FROM agent_control_initial_planning_handoff_intents WHERE project_id=${projectId}`;
          assert.equal(handoffs.length, 2);
          yield* Effect.forEach(
            handoffs,
            ({ handoffId }) => planningWakeupService.wake(handoffId),
            { discard: true },
          );
          yield* planningConsumerService.drain;
          assert.equal(fakeProvider.sessions.size, 2);
          assert.equal(
            [...fakeProvider.sessions.values()].filter(
              (session) => session.status === "running" && session.activeTurnId,
            ).length,
            2,
          );
          assert.equal(fakeProvider.turnCount(), 2);
          for (const session of fakeProvider.sessions.values()) {
            yield* Context.get(
              pipelineContext,
              AgentControlInitialPlanningHandoffStore,
            ).observeProviderStarted({
              threadId: session.threadId,
              providerTurnId: String(session.activeTurnId),
              acceptedAt: session.updatedAt,
            });
          }
          const worktrees = yield* sql<{
            branch: string;
            worktreePath: string;
          }>`SELECT branch_name AS "branch",internal_worktree_path AS "worktreePath" FROM agent_control_worktree_reservation_states WHERE project_id=${projectId}`;
          assert.equal(new Set(worktrees.map((entry) => entry.branch)).size, 2);
          assert.equal(new Set(worktrees.map((entry) => entry.worktreePath)).size, 2);
          yield* restartRecovery();
          yield* scheduler.processProject(projectId);
          yield* planningConsumerService.drain;
          assert.equal(fakeProvider.turnCount(), 2);
          assert.equal(
            (yield* sql`SELECT 1 FROM agent_control_epic_task_executions WHERE project_id=${projectId}`)
              .length,
            2,
          );
          if (!parallelPolling && !sourceBeforeIntegration && !revokeDuringIntegration) {
            const guard = Context.get(context, AgentControlTaskConsumerGuard);
            const taskId = AgentControlTaskId.make(
              (yield* sql<{
                taskId: string;
              }>`SELECT task_id AS "taskId" FROM agent_control_epic_task_executions WHERE project_id=${projectId} LIMIT 1`)[0]!
                .taskId,
            );
            const providerBoundary = guard.useTaskForProviderEffectInTransaction!;
            assert.equal(
              yield* sql.withTransaction(
                providerBoundary(projectId, taskId, () => Effect.succeed("entered")),
              ),
              "entered",
            );
            const epic = (yield* loadSelectedEpic(sql, projectId))!;
            if (
              sourceChange === "epic-parallel-human-takeover" ||
              sourceChange === "epic-parallel-project-paused"
            ) {
              yield* engine.dispatchHuman({
                commandId: CommandId.make("parallel-human-takeover"),
                projectId,
                expectedRevision: (yield* engine.getProjectState({ projectId })).revision,
                mode: sourceChange === "epic-parallel-project-paused" ? "paused" : "manual",
              });
            } else {
              yield* publishSources(
                projectId,
                issues.map((entry) => ({
                  ...entry,
                  ...(sourceChange === "epic-parallel-paused"
                    ? { paused: true, eligible: false, eligibilityReason: "paused" as const }
                    : sourceChange === "epic-parallel-ready-withdrawn"
                      ? {
                          ready: false,
                          eligible: false,
                          eligibilityReason: "ready-inactive" as const,
                        }
                      : sourceChange === "epic-parallel-closed"
                        ? {
                            state: "closed" as const,
                            eligible: false,
                            eligibilityReason: "closed" as const,
                          }
                        : { body: "Edited semantic scope after dependency approval" }),
                })),
                2,
              );
            }
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  sql.withTransaction(
                    providerBoundary(projectId, taskId, () =>
                      Effect.succeed("changed-content-entry"),
                    ),
                  ),
                ),
              ),
            );
            yield* sql.withTransaction(saveEpicRun(sql, epic, { status: "stopped" }));
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  sql.withTransaction(
                    providerBoundary(projectId, taskId, () => Effect.succeed("late-entry")),
                  ),
                ),
              ),
            );
            assert.equal(
              (yield* sql`SELECT 1 FROM agent_control_task_execution_authority WHERE project_id=${projectId}`)
                .length,
              0,
            );
            yield* sql`DELETE FROM agent_control_epic_targets WHERE project_id=${projectId}`;
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  sql.withTransaction(
                    providerBoundary(projectId, taskId, () => Effect.succeed("cleared-entry")),
                  ),
                ),
              ),
            );
            assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
            return;
          }
          const acceptedExecutions =
            yield* sql`SELECT * FROM agent_control_epic_task_executions WHERE project_id=${projectId} ORDER BY task_id`;
          yield* publishSources(projectId, issues, parallelPollVersion++);
          assert.equal(preparationPolls, sourceChange === "epic-parallel-preparation-poll" ? 1 : 0);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_task_executions WHERE project_id=${projectId} ORDER BY task_id`,
            acceptedExecutions,
          );
          const refreshedAuthorities = yield* sql<{
            taskRevision: number;
            executionRevision: number;
            currentSequence: number;
            executionSequence: number;
          }>`
            SELECT task.revision AS "taskRevision", stage.task_revision AS "executionRevision",
              task.github_intake_sequence AS "currentSequence", stage.github_intake_sequence AS "executionSequence"
            FROM agent_control_epic_task_executions execution
            JOIN agent_control_task_states task ON task.task_id=execution.task_id
            JOIN agent_control_stage_run_states stage ON stage.stage_run_id=execution.stage_run_id
            WHERE execution.project_id=${projectId}`;
          assert.lengthOf(refreshedAuthorities, 2);
          for (const authority of refreshedAuthorities) {
            assert.isAbove(authority.taskRevision, authority.executionRevision);
            assert.isAbove(authority.currentSequence, authority.executionSequence);
          }
          if (sourceChange === "epic-parallel-polls") {
            // Upgrade while both provider turns are still active and intake has advanced.
            yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sql));
            assert.deepStrictEqual(
              yield* sql`SELECT * FROM agent_control_epic_task_executions WHERE project_id=${projectId} ORDER BY task_id`,
              acceptedExecutions,
            );
          }
          yield* restartRecovery();
          yield* planningConsumerService.drain;
          assert.equal(fakeProvider.turnCount(), 2);
        }
        let integrationChecks = 0;
        let revokedIssueNodeId: string | undefined;
        const epicResults = yield* makeEpicResults.pipe(
          Effect.provide(context),
          Effect.provideService(EpicCheckExecutor, {
            execute: ({ cwd }) =>
              Effect.gen(function* () {
                integrationChecks++;
                const result = yield* git(cwd, ["diff", "--check"]).pipe(Effect.provide(gitLayer));
                if (
                  sourceChange === "epic-parallel-integration-incomplete" &&
                  integrationChecks === 1
                ) {
                  yield* publishSources(projectId, issues, parallelPollVersion++, false);
                }
                if (revokeDuringIntegration && integrationChecks === 1) {
                  const [completedTask] = yield* sql<{
                    issueNodeId: string;
                  }>`SELECT issue_node_id AS "issueNodeId" FROM agent_control_task_states WHERE project_id=${projectId} AND status='succeeded'`;
                  assert.isDefined(completedTask);
                  revokedIssueNodeId = completedTask!.issueNodeId;
                  const changedIssues = issues.map((entry) =>
                    entry.issueNodeId !== revokedIssueNodeId
                      ? entry
                      : {
                          ...entry,
                          ...(sourceChange === "epic-parallel-integration-paused"
                            ? {
                                paused: true,
                                eligible: false,
                                eligibilityReason: "paused" as const,
                              }
                            : sourceChange === "epic-parallel-integration-content"
                              ? { body: "Scope changed while integration checks were running" }
                              : {}),
                        },
                  );
                  if (sourceChange !== "epic-parallel-integration-paused") {
                    epicSource = {
                      ...epicSource!,
                      tasks: epicSource!.tasks.map((task) =>
                        task.issue.issueNodeId !== revokedIssueNodeId
                          ? task
                          : {
                              ...task,
                              ...(sourceChange === "epic-parallel-integration-dependency"
                                ? { dependencies: [epicSource!.tasks[2]!.issue] }
                                : {
                                    issue: {
                                      ...task.issue,
                                      contentFingerprint: epicIssueContentFingerprint(
                                        changedIssues.find(
                                          (issue) => issue.issueNodeId === revokedIssueNodeId,
                                        )!,
                                      ),
                                    },
                                  }),
                            },
                      ),
                    };
                  }
                  yield* publishSources(projectId, changedIssues, parallelPollVersion++);
                }
                return { exitCode: 0, stdout: result.stdout, stderr: result.stderr };
              }).pipe(Effect.orDie),
          }),
        );
        const epicService = yield* makeAgentControlEpic.pipe(
          Effect.provide(context),
          Effect.provideService(AgentControlEpicResultHooks, epicResults),
          Effect.provideService(GithubIssueTrackerClient, {
            resolveRepository: () => Effect.die("unexpected resolveRepository"),
            pollIssues: () => Effect.die("unexpected pollIssues"),
            inspectEpic: () =>
              epicSource ? Effect.succeed(epicSource) : Effect.die("missing Epic source"),
          }),
        );
        const unchangedParallelPoll = Effect.fn("unchangedParallelPoll")(function* () {
          if (!parallelPolling && !sourceBeforeIntegration && !revokeDuringIntegration) return;
          yield* publishSources(projectId, issues, parallelPollVersion++);
        });
        const completeActiveTask = Effect.fn("completeArmedProductionActiveTask")(function* (
          runOrdinal: number,
        ) {
          const [accepted] = yield* sql<{ readonly handoffId: string }>`
            SELECT handoff_id AS "handoffId"
            FROM agent_control_initial_planning_deliveries
            WHERE state IN ('pending', 'turn-accepted', 'delivery-attempted', 'provider-started')
            ORDER BY rowid DESC
            LIMIT 1
          `;
          assert.isDefined(accepted);
          yield* planningWakeupService.wake(accepted!.handoffId);
          yield* planningConsumerService.drain;
          const [pendingPlanning] = yield* sql<{
            threadId: string;
          }>`SELECT thread_id AS "threadId"
            FROM agent_control_initial_planning_handoff_accepted WHERE handoff_id=${accepted!.handoffId}`;
          const planningStarted = startedEvent(ThreadId.make(pendingPlanning!.threadId));
          yield* Context.get(
            pipelineContext,
            AgentControlInitialPlanningHandoffStore,
          ).observeProviderStarted({
            threadId: planningStarted.threadId,
            providerTurnId: String(planningStarted.turnId),
            acceptedAt: planningStarted.createdAt,
          });
          const [planningDelivery] = yield* sql<{
            readonly handoffId: string;
            readonly threadId: string;
            readonly providerTurnId: string;
            readonly providerAcceptedAt: string;
            readonly runtimeMode: "approval-required" | "full-access";
            readonly state: string;
          }>`
          SELECT delivery.handoff_id AS "handoffId", delivery.thread_id AS "threadId",
            delivery.provider_turn_id AS "providerTurnId",
            delivery.provider_accepted_at AS "providerAcceptedAt",
            intent.runtime_mode AS "runtimeMode", delivery.state
          FROM agent_control_initial_planning_deliveries delivery
          JOIN agent_control_initial_planning_handoff_intents intent
            ON intent.handoff_id=delivery.handoff_id
          WHERE delivery.state='provider-started'
          ORDER BY delivery.provider_accepted_at DESC
          LIMIT 1
        `;
          assert.isDefined(
            planningDelivery,
            encodeUnknownJson(
              yield* sql`
              SELECT accepted.handoff_id AS handoffId, delivery.state,
                delivery.next_attempt_at AS nextAttemptAt,
                delivery.last_error_code AS lastErrorCode
              FROM agent_control_initial_planning_handoff_accepted accepted
              LEFT JOIN agent_control_initial_planning_deliveries delivery
                ON delivery.handoff_id=accepted.handoff_id
              ORDER BY accepted.accepted_at, accepted.handoff_id
            `,
            ),
          );
          assert.equal(planningDelivery!.state, "provider-started");
          const planningThreadId = ThreadId.make(planningDelivery!.threadId);
          const planningTurnId = TurnId.make(planningDelivery!.providerTurnId);
          yield* projectProviderStarted({
            prefix: `armed-production-planning-${runOrdinal}`,
            threadId: planningThreadId,
            turnId: planningTurnId,
            runtimeMode: planningDelivery!.runtimeMode,
            acceptedAt: planningDelivery!.providerAcceptedAt,
          });
          assert.equal(
            (yield* planningFinalizerService.processHandoff(planningDelivery!.handoffId))._tag,
            "Started",
          );
          const planAt = DateTime.formatIso(yield* DateTime.now);
          yield* orchestrationEngine.dispatchAgentControl({
            type: "thread.proposed-plan.upsert",
            commandId: CommandId.make(`provider:armed-production-plan:${runOrdinal}:upsert`),
            threadId: planningThreadId,
            proposedPlan: {
              id: `plan:${planningThreadId}:turn:${planningTurnId}`,
              turnId: planningTurnId,
              planMarkdown: "# Plan\n\n1. Implement.\n2. Verify.",
              implementedAt: null,
              implementationThreadId: null,
              createdAt: planAt,
              updatedAt: planAt,
            },
            createdAt: planAt,
          });
          const planningTerminal = yield* publishTerminal({
            prefix: `armed-production-planning-${runOrdinal}`,
            threadId: planningThreadId,
            turnId: planningTurnId,
            state: "completed",
          });
          yield* projectProviderTerminal({
            prefix: `armed-production-planning-${runOrdinal}`,
            event: planningTerminal,
            runtimeMode: planningDelivery!.runtimeMode,
          });
          yield* planningConsumerService.drain;
          const planningFinalized = yield* planningFinalizerService.processHandoff(
            planningDelivery!.handoffId,
          );
          assert.equal(planningFinalized._tag, "Finalized");
          yield* unchangedParallelPoll();

          const implementationMaterialized = yield* Effect.gen(function* () {
            if (sourceChange === "epic-parallel-polls" && runOrdinal === 1) {
              yield* publishSources(projectId, issues, parallelPollVersion++, false);
              const failed = yield* implementationCoordinatorService
                .processHandoff(planningDelivery!.handoffId)
                .pipe(Effect.flip);
              assert.equal(failed.reason, "admission-corrupt");
              assert.equal(fakeProvider.turnCount(), 2);
              assert.deepStrictEqual(
                yield* sql`SELECT * FROM agent_control_implementation_materialization_evidence`,
                [],
              );
              return yield* Effect.scoped(
                Effect.gen(function* () {
                  const publications = yield* Stream.toPull(
                    implementationCoordinatorService.streamPublications.pipe(
                      Stream.filter(
                        (publication) => publication.handoffId === planningDelivery!.handoffId,
                      ),
                    ),
                  );
                  const resumedRunOnce = yield* Layer.build(
                    Layer.fresh(AgentControlRunOnceControllerLive).pipe(
                      Layer.provide(Layer.succeedContext(pipelineContext)),
                    ),
                  );
                  const resumedScheduler = yield* Layer.build(
                    Layer.fresh(AgentControlArmedSchedulerLive).pipe(
                      Layer.provide(Layer.succeed(AgentControlEpicProgress, epicService)),
                      Layer.provide(
                        Layer.succeedContext(Context.merge(pipelineContext, resumedRunOnce)),
                      ),
                    ),
                  );
                  const activation = yield* makeReactorStartupActivation;
                  yield* Context.get(resumedScheduler, AgentControlArmedScheduler).prepare(
                    activation,
                  );
                  yield* activation.open;
                  yield* taskIntake.reconcileObservedProject({ projectId });
                  const [publication] = yield* publications;
                  assert.deepStrictEqual(
                    yield* sql`SELECT receipt.status FROM agent_control_implementation_materialization_receipts receipt JOIN agent_control_implementation_materialization_evidence evidence USING (materialization_evidence_id) WHERE evidence.admission_handoff_id=${publication.handoffId}`,
                    [{ status: "accepted" }],
                  );
                  assert.equal(fakeProvider.turnCount(), 2);
                  return { _tag: "Materialized" as const, publication };
                }),
              );
            }
            const admitted = yield* implementationAdmissionService.processHandoff(
              planningDelivery!.handoffId,
            );
            assert.equal(admitted._tag, "Admitted");
            return yield* implementationCoordinatorService.processHandoff(
              planningDelivery!.handoffId,
            );
          });
          assert.equal(implementationMaterialized._tag, "Materialized");
          if (implementationMaterialized._tag !== "Materialized") {
            return yield* Effect.die("implementation was not materialized");
          }
          const implementationHandoffId =
            implementationMaterialized.publication.implementationHandoffId;
          yield* implementationConsumerService.processHandoff(implementationHandoffId);
          const [pendingImplementation] = yield* sql<{
            threadId: string;
          }>`SELECT thread_id AS "threadId"
            FROM agent_control_implementation_deliveries WHERE handoff_id=${implementationHandoffId}`;
          yield* implementationConsumerService.processRuntimeEvent(
            startedEvent(ThreadId.make(pendingImplementation!.threadId)),
          );
          const [implementationDelivery] = yield* sql<{
            readonly threadId: string;
            readonly providerTurnId: string;
            readonly providerAcceptedAt: string;
            readonly runtimeMode: "approval-required" | "full-access";
            readonly state: string;
          }>`
          SELECT thread_id AS "threadId", provider_turn_id AS "providerTurnId",
            provider_accepted_at AS "providerAcceptedAt", runtime_mode AS "runtimeMode", state
          FROM agent_control_implementation_deliveries
          WHERE handoff_id=${implementationHandoffId}
        `;
          assert.isDefined(implementationDelivery);
          assert.equal(implementationDelivery!.state, "provider-started");
          yield* projectProviderStarted({
            prefix: `armed-production-implementation-${runOrdinal}`,
            threadId: ThreadId.make(implementationDelivery!.threadId),
            turnId: TurnId.make(implementationDelivery!.providerTurnId),
            runtimeMode: implementationDelivery!.runtimeMode,
            acceptedAt: implementationDelivery!.providerAcceptedAt,
          });
          assert.equal(
            (yield* implementationStarterService.processHandoff(implementationHandoffId))._tag,
            "Started",
          );
          if (epicParallel) {
            const implementationCwd = fakeProvider.sessions.get(
              ThreadId.make(implementationDelivery!.threadId),
            )!.cwd;
            if (runOrdinal === 3) {
              assert.equal(
                yield* fs.readFileString(`${implementationCwd}/task-1.md`),
                "Task 1 completed.\n",
              );
              assert.equal(
                yield* fs.readFileString(`${implementationCwd}/task-2.md`),
                "Task 2 completed.\n",
              );
            }
            yield* fs.writeFileString(
              `${implementationCwd}/task-${runOrdinal}.md`,
              `Task ${runOrdinal} completed.\n`,
            );
          }
          yield* unchangedParallelPoll();
          const implementationTerminal = yield* publishTerminal({
            prefix: `armed-production-implementation-${runOrdinal}`,
            threadId: ThreadId.make(implementationDelivery!.threadId),
            turnId: TurnId.make(implementationDelivery!.providerTurnId),
            state: "completed",
          });
          yield* projectProviderTerminal({
            prefix: `armed-production-implementation-${runOrdinal}`,
            event: implementationTerminal,
            runtimeMode: implementationDelivery!.runtimeMode,
          });
          yield* implementationConsumerService.processRuntimeEvent(implementationTerminal);
          const implementationFinalized =
            yield* implementationFinalizerService.processHandoff(implementationHandoffId);
          assert.equal(implementationFinalized._tag, "Finalized");
          if (implementationFinalized._tag !== "Finalized") {
            return yield* Effect.die("implementation was not finalized");
          }

          const resultEvidenceId = implementationFinalized.resultEvidenceId;
          yield* unchangedParallelPoll();
          if (sourceChange === "paused" || sourceChange === "replaced") {
            const refreshedAt = DateTime.formatIso(yield* DateTime.now);
            const changedSource: AgentControlGithubIssueSnapshot =
              sourceChange === "paused"
                ? {
                    ...issue(1, refreshedAt),
                    paused: true,
                    eligible: false,
                    eligibilityReason: "paused",
                  }
                : issue(3, refreshedAt);
            yield* publishSources(
              projectId,
              [changedSource, issues[1]!],
              2 + intakeRefreshes,
              false,
            );
            assert.equal(
              (yield* verificationAdmissionService.processResultEvidence(resultEvidenceId))._tag,
              "Admitted",
            );
            const rejection = yield* verificationCoordinatorService
              .processHandoff(resultEvidenceId)
              .pipe(Effect.flip);
            assert.equal(rejection.reason, "source-stale");
            assert.equal(rejection.operation, "guard-worktree");
            assert.equal(fakeProvider.turnCount(), 2);
            assert.deepStrictEqual(
              yield* sql`SELECT count(*) AS count FROM agent_control_verification_deliveries`,
              [{ count: 0 }],
            );
            assert.deepStrictEqual(
              yield* sql`SELECT count(*) AS count FROM agent_control_verification_check_starts`,
              [{ count: 0 }],
            );
            return;
          }
          if (
            (intakeRefreshes === 3 || sourceChange === "epic-parallel-polls") &&
            runOrdinal === 1
          ) {
            const beforeCrash = fakeProvider.turnCount();
            const committedAdmission = yield* Effect.scoped(
              Effect.gen(function* () {
                const crashContext = yield* Layer.build(
                  Layer.fresh(AgentControlVerificationAdmissionLive).pipe(
                    Layer.provide(Layer.succeedContext(pipelineContext)),
                    Layer.provide(
                      Layer.succeed(AgentControlVerificationAdmissionHooks, {
                        ...Context.get(Context.empty(), AgentControlVerificationAdmissionHooks),
                        afterNativeCommit: () =>
                          Effect.die("simulated process stop after admission commit"),
                      }),
                    ),
                  ),
                );
                return yield* Context.get(crashContext, AgentControlVerificationAdmission)
                  .processResultEvidence(resultEvidenceId)
                  .pipe(Effect.exit);
              }),
            );
            assert.isTrue(Exit.isFailure(committedAdmission));
            yield* Effect.scoped(
              Effect.gen(function* () {
                const reopenedSql = yield* Layer.build(NodeSqliteClient.layer({ filename }));
                const restarted = yield* Layer.build(
                  Layer.fresh(AgentControlVerificationAdmissionLive).pipe(
                    Layer.provide(
                      Layer.succeedContext(Context.merge(pipelineContext, reopenedSql)),
                    ),
                  ),
                );
                const admission = Context.get(restarted, AgentControlVerificationAdmission);
                yield* admission.recover;
                assert.equal(
                  (yield* admission.processResultEvidence(resultEvidenceId))._tag,
                  "Replayed",
                );
              }),
            );
            assert.equal(fakeProvider.turnCount(), beforeCrash);
          } else {
            assert.equal(
              (yield* verificationAdmissionService.processResultEvidence(resultEvidenceId))._tag,
              "Admitted",
            );
          }
          if (intakeRefreshes === 3 && runOrdinal === 1) {
            const taskEvents = Context.get(context, AgentControlTaskEventStore);
            const admission = Option.getOrThrow(
              yield* verificationAdmissionService.loadAcceptedEvidence(resultEvidenceId),
            );
            const [createdTaskEvent] = yield* taskEvents.readStream(
              AgentControlTaskId.make(admission.taskId),
              0,
              1,
            );
            const mutations: ReadonlyArray<
              (event: AgentControlTaskEvent) => AgentControlTaskEvent
            > = [
              (event) => ({
                ...createdTaskEvent!,
                eventId: event.eventId,
                sequence: event.sequence,
                streamVersion: event.streamVersion,
              }),
              (event) => ({ ...event, eventId: EventId.make("foreign-source-event") }),
              (event) => ({
                ...event,
                aggregateId: AgentControlTaskId.make("foreign-source-task"),
              }),
              (event) => ({ ...event, sequence: event.sequence + 1 }),
              (event) => ({ ...event, streamVersion: event.streamVersion + 1 }),
              (event) =>
                event.type === "agentControl.task.sourceGate.changed"
                  ? {
                      ...event,
                      payload: {
                        ...event.payload,
                        source: {
                          ...event.payload.source,
                          projectId: ProjectId.make("foreign-source-project"),
                        },
                      },
                    }
                  : event,
              (event) =>
                event.type === "agentControl.task.sourceGate.changed"
                  ? { ...event, payload: { ...event.payload, sourceGate: "paused" } }
                  : event,
            ];
            for (const mutate of mutations) {
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const corrupted = yield* Layer.build(
                    Layer.fresh(AgentControlVerificationAdmissionLive).pipe(
                      Layer.provide(
                        Layer.succeedContext(
                          Context.add(pipelineContext, AgentControlTaskEventStore, {
                            ...taskEvents,
                            readStream: (...args) =>
                              taskEvents
                                .readStream(...args)
                                .pipe(
                                  Effect.map((events) =>
                                    events.map((event) =>
                                      event.eventId === admission.taskSourceEventId
                                        ? mutate(event)
                                        : event,
                                    ),
                                  ),
                                ),
                          }),
                        ),
                      ),
                    ),
                  );
                  const rejection = yield* Context.get(corrupted, AgentControlVerificationAdmission)
                    .loadAcceptedEvidence(resultEvidenceId)
                    .pipe(Effect.flip);
                  assert.equal(rejection.reason, "identity-mismatch");
                  assert.equal(rejection.operation, "replay-bound-histories");
                }),
              );
            }
            assert.isTrue(
              Option.isSome(
                yield* verificationAdmissionService.loadAcceptedEvidence(resultEvidenceId),
              ),
            );
            assert.equal(fakeProvider.turnCount(), 2);
            assert.deepStrictEqual(
              yield* sql`SELECT count(*) AS count FROM agent_control_verification_deliveries`,
              [{ count: 0 }],
            );
            assert.deepStrictEqual(
              yield* sql`SELECT count(*) AS count FROM agent_control_verification_check_starts`,
              [{ count: 0 }],
            );
          }
          const verificationMaterialized = yield* verificationCoordinatorService.processHandoff(
            implementationFinalized.resultEvidenceId,
          );
          assert.equal(verificationMaterialized._tag, "Materialized");
          if (verificationMaterialized._tag !== "Materialized") {
            return yield* Effect.die("verification was not materialized");
          }
          const verificationHandoffId = verificationMaterialized.publication.verificationHandoffId;
          yield* verificationConsumerService.processHandoff(verificationHandoffId);
          const [pendingVerification] = yield* sql<{
            threadId: string;
          }>`SELECT thread_id AS "threadId"
            FROM agent_control_verification_deliveries WHERE handoff_id=${verificationHandoffId}`;
          yield* verificationConsumerService.processRuntimeEvent(
            startedEvent(ThreadId.make(pendingVerification!.threadId)),
          );
          const [verificationDelivery] = yield* sql<{
            readonly threadId: string;
            readonly providerTurnId: string;
            readonly providerAcceptedAt: string;
            readonly runtimeMode: "approval-required" | "full-access";
            readonly state: string;
          }>`
          SELECT thread_id AS "threadId", provider_turn_id AS "providerTurnId",
            provider_accepted_at AS "providerAcceptedAt", runtime_mode AS "runtimeMode", state
          FROM agent_control_verification_deliveries
          WHERE handoff_id=${verificationHandoffId}
        `;
          assert.isDefined(verificationDelivery);
          assert.equal(verificationDelivery!.state, "provider-started");
          yield* projectProviderStarted({
            prefix: `armed-production-verification-${runOrdinal}`,
            threadId: ThreadId.make(verificationDelivery!.threadId),
            turnId: TurnId.make(verificationDelivery!.providerTurnId),
            runtimeMode: verificationDelivery!.runtimeMode,
            acceptedAt: verificationDelivery!.providerAcceptedAt,
          });
          assert.equal(
            (yield* verificationStarterService.processHandoff(verificationHandoffId))._tag,
            "Started",
          );
          const permit = verificationPermits.get(verificationHandoffId)!;
          assert.isDefined(permit);
          const verificationCwd = fakeProvider.sessions.get(
            ThreadId.make(verificationDelivery!.threadId),
          )?.cwd;
          if (verificationCwd === undefined) {
            return yield* Effect.die("Verification session is missing its controlled worktree");
          }
          const manifest = yield* prepareVerificationCheckManifest(sql, {
            permit,
            cwd: verificationCwd,
            checks: [
              {
                id: "clean-diff",
                command: "git",
                args: ["diff", "--check"],
                cwd: ".",
                required: true,
                timeoutMs: 10_000,
                allowTemporaryFiles: false,
                resultFormat: "exit-code",
              },
            ],
          });
          let checkExecutions = 0;
          const runCheck = executeVerificationCheck(sql, {
            manifest,
            checkId: "clean-diff",
            providerTurnId: verificationDelivery!.providerTurnId,
            authorize: Effect.void,
            execute: Effect.gen(function* () {
              checkExecutions++;
              const result = yield* git(verificationCwd, ["diff", "--check"]).pipe(
                Effect.provide(gitLayer),
              );
              return { exitCode: 0, stdout: result.stdout, stderr: result.stderr };
            }),
          });
          assert.equal((yield* runCheck).exitCode, 0);
          assert.equal((yield* runCheck).exitCode, 0);
          assert.equal(checkExecutions, 1);
          yield* unchangedParallelPoll();
          const outputAt = DateTime.formatIso(yield* DateTime.now);
          const messageId = MessageId.make(`verification-output-${runOrdinal}`);
          const outputEventId = EventId.make(`verification-output-event-${runOrdinal}`);
          if (!terminalRetry)
            yield* orchestrationEngine.dispatch({
              type: "thread.verification-result.capture",
              commandId: CommandId.make(
                `provider:${outputEventId}:verification-result:${messageId}`,
              ),
              threadId: ThreadId.make(verificationDelivery!.threadId),
              messageId,
              turnId: TurnId.make(verificationDelivery!.providerTurnId),
              fragment: makeBoundedVerificationResultCompletion(
                encodeUnknownJson({
                  schemaVersion: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
                  verdict: "passed",
                  report: "Required clean-diff check passed.",
                }),
                null,
              ),
              providerRuntimeMessage: {
                runtimeEventId: outputEventId,
                eventType: "item.completed",
                providerInstanceId,
                providerTurnId: TurnId.make(verificationDelivery!.providerTurnId),
                providerItemId: null,
              },
              verificationResultCapture: {
                schemaVersion: 1,
                disposition: "authority",
                handoffId: verificationHandoffId,
                providerDeliveryId: permit.providerDeliveryId,
                providerInstanceId,
                providerTurnId: TurnId.make(verificationDelivery!.providerTurnId),
                resultSchemaFingerprint: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT,
              },
              createdAt: outputAt,
            });
          const verificationTerminal = yield* publishTerminal({
            prefix: `armed-production-verification-${runOrdinal}`,
            threadId: ThreadId.make(verificationDelivery!.threadId),
            turnId: TurnId.make(verificationDelivery!.providerTurnId),
            state: cancelledRetry ? "interrupted" : "completed",
          });
          yield* projectProviderTerminal({
            prefix: `armed-production-verification-${runOrdinal}`,
            event: verificationTerminal,
            runtimeMode: verificationDelivery!.runtimeMode,
          });
          yield* verificationConsumerService.processRuntimeEvent(verificationTerminal);
          assert.equal(
            (yield* verificationFinalizerService.processHandoff(verificationHandoffId))._tag,
            "Finalized",
          );
          assert.equal(
            (yield* taskFinalizerService.processHandoff(verificationHandoffId))._tag,
            "Finalized",
          );
          assert.equal(
            (yield* verificationAdmissionService.processResultEvidence(resultEvidenceId))._tag,
            "Replayed",
          );
          assert.equal(
            (yield* verificationFinalizerService.processHandoff(verificationHandoffId))._tag,
            "Replayed",
          );
          assert.equal(
            (yield* taskFinalizerService.processHandoff(verificationHandoffId))._tag,
            "Replayed",
          );
          assert.equal(checkExecutions, 1);
          yield* Effect.scoped(
            Effect.gen(function* () {
              const reopened = yield* Layer.build(NodeSqliteClient.layer({ filename }));
              const replay = yield* executeVerificationCheck(
                Context.get(reopened, SqlClient.SqlClient),
                {
                  manifest,
                  checkId: "clean-diff",
                  providerTurnId: verificationDelivery!.providerTurnId,
                  authorize: Effect.void,
                  execute: Effect.die(
                    "persisted verification check must not execute after restart",
                  ),
                },
              );
              assert.equal(replay.exitCode, 0);
            }),
          );
        });

        yield* completeActiveTask(1);
        if (revokeDuringIntegration) {
          yield* epicService.processProject(projectId);
          const revoked = (yield* loadSelectedEpic(sql, projectId))!;
          const revokedMember = revoked.members.find(
            (member) => member.issueNodeId === revokedIssueNodeId,
          )!;
          assert.equal(revokedMember.status, "failed");
          assert.isNotEmpty(revokedMember.blocker);
          assert.isNotEmpty(revoked.blockers);
          assert.equal(integrationChecks, 1);
          assert.equal(revoked.acceptedCommitSha, null);
          assert.isFalse(revoked.members.some((member) => member.status === "accepted"));
          assert.equal(revoked.members[2]!.status, "pending");
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_integration_results`,
            [],
          );
          yield* restartRecovery();
          yield* epicService.processProject(projectId);
          const recovered = (yield* loadSelectedEpic(sql, projectId))!;
          const recoveredMember = recovered.members.find(
            (member) => member.issueNodeId === revokedIssueNodeId,
          )!;
          assert.equal(recoveredMember.status, "failed");
          assert.isNotEmpty(recoveredMember.blocker);
          assert.equal(recovered.acceptedCommitSha, null);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_integration_results`,
            [],
          );
          assert.equal(fakeProvider.turnCount(), 4);
          return;
        }
        if (sourceBeforeIntegration) {
          assert.isDefined(epicSource);
          epicSource = {
            ...epicSource!,
            tasks: epicSource!.tasks.map((task, index) =>
              index === 1
                ? {
                    ...task,
                    ...(sourceChange === "epic-parallel-dependency-before-integration"
                      ? { dependencies: [epicSource!.tasks[2]!.issue] }
                      : { issue: { ...task.issue, contentFingerprint: "changed-scope" } }),
                  }
                : task,
            ),
          };
          yield* epicService.processProject(projectId);
          const blockedEpic = (yield* loadSelectedEpic(sql, projectId))!;
          assert.equal(blockedEpic.status, "blocked");
          assert.isNotEmpty(blockedEpic.blockers);
          assert.equal(integrationChecks, 0);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_integration_results`,
            [],
          );
          yield* restartRecovery();
          yield* epicService.processProject(projectId);
          assert.deepStrictEqual(yield* loadSelectedEpic(sql, projectId), blockedEpic);
          assert.equal(fakeProvider.turnCount(), 4);
          return;
        }
        if (sourceChange === "epic-parallel-integration-incomplete") {
          yield* epicService.processProject(projectId);
          const waiting = (yield* loadSelectedEpic(sql, projectId))!;
          assert.equal(waiting.status, "running");
          assert.isEmpty(waiting.blockers);
          assert.isFalse(
            waiting.members.some(
              (member) => member.status === "failed" || member.status === "accepted",
            ),
          );
          assert.equal(integrationChecks, 1);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_integration_results`,
            [],
          );
          assert.equal(fakeProvider.turnCount(), 4);
          yield* taskIntake.reconcileObservedProject({ projectId });
        }
        if (parallelPolling) {
          yield* epicService.processProject(projectId);
          let epic = (yield* loadSelectedEpic(sql, projectId))!;
          assert.equal(epic.members.filter((member) => member.status === "accepted").length, 1);
          assert.equal(epic.members[2]!.status, "pending");
          assert.equal(epic.members[2]!.childRunId, null);
          assert.equal(integrationChecks, 1);
          yield* restartRecovery();
          yield* unchangedParallelPoll();
          yield* completeActiveTask(2);
          yield* epicService.processProject(projectId);
          epic = (yield* loadSelectedEpic(sql, projectId))!;
          assert.equal(epic.members.filter((member) => member.status === "accepted").length, 2);
          assert.isTrue(
            epic.members
              .slice(0, 2)
              .every((member) => member.integrationVerification?.status === "passed"),
          );
          assert.equal(integrationChecks, 2);
          assert.equal(epic.members[2]!.status, "running");
          yield* scheduler.processProject(projectId);
          yield* completeActiveTask(3);
          yield* epicService.processProject(projectId);
          epic = (yield* loadSelectedEpic(sql, projectId))!;
          assert.equal(epic.status, "succeeded");
          assert.equal(fakeProvider.turnCount(), 9);
          const completedEpic = epic;
          const completedChecks =
            yield* sql`SELECT * FROM agent_control_verification_check_results`;
          const completedIntegrations =
            yield* sql`SELECT * FROM agent_control_epic_integration_results`;
          assert.lengthOf(
            completedChecks.filter((check) => check.check_id === "clean-diff"),
            3,
          );
          assert.lengthOf(completedIntegrations, 3);
          const completedIntegrationChecks = integrationChecks;
          yield* unchangedParallelPoll();
          yield* restartRecovery();
          yield* epicService.processProject(projectId);
          assert.deepStrictEqual(yield* loadSelectedEpic(sql, projectId), completedEpic);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_verification_check_results`,
            completedChecks,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_epic_integration_results`,
            completedIntegrations,
          );
          assert.equal(integrationChecks, completedIntegrationChecks);
          assert.equal(fakeProvider.turnCount(), 9);
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
          return;
        }
        if (terminalRetry) {
          yield* controller.processProject(projectId);
          assert.deepStrictEqual(
            yield* sql`SELECT status FROM agent_control_task_states WHERE task_id=${firstTaskId}`,
            [{ status: cancelledRetry ? "cancelled" : "failed" }],
          );
          assert.deepStrictEqual(
            yield* sql`SELECT status, last_step AS step FROM agent_control_run_once_states
              WHERE project_id=${projectId}`,
            [{ status: "completed", step: "completed" }],
          );
          assert.deepStrictEqual(
            yield* sql`SELECT status FROM agent_control_stage_run_lease_states
              WHERE project_id=${projectId}`,
            [{ status: "released" }],
          );
          const oldReservations = yield* sql<{
            status: string;
            materialization_phase: string;
            branch_name: string;
            internal_worktree_path: string;
            repository_common_dir: string;
          }>`SELECT * FROM agent_control_worktree_reservation_states
            WHERE project_id=${projectId}`;
          assert.lengthOf(oldReservations, 1);
          assert.equal(oldReservations[0]!.status, "ready");
          assert.equal(oldReservations[0]!.materialization_phase, "ownership-marked");
          const oldTaskEvents = yield* Context.get(context, AgentControlTaskEventStore).readStream(
            firstTaskId,
          );
          const oldRunEvidence = yield* sql`SELECT * FROM agent_control_run_once_step_evidence
            WHERE project_id=${projectId} ORDER BY run_id, ordinal`;
          const oldVerificationEvidence =
            yield* sql`SELECT * FROM agent_control_verification_check_results`;
          const oldFinalization =
            yield* sql`SELECT * FROM agent_control_verification_finalization_evidence
            WHERE project_id=${projectId}`;
          assert.equal(
            oldFinalization[0]!.terminal_cause,
            cancelledRetry ? "provider-delivery-interrupted" : "verification-invalid-output",
          );
          assert.equal(
            oldFinalization[0]!.invalid_output_code,
            cancelledRetry ? null : "missing-final-message",
          );
          const oldWorktreeHead = yield* git(oldReservations[0]!.internal_worktree_path, [
            "rev-parse",
            "HEAD",
          ]).pipe(Effect.provide(gitLayer));
          yield* engine.dispatchHuman({
            commandId: CommandId.make("terminal-project-manual"),
            projectId,
            expectedRevision: (yield* engine.getProjectState({ projectId })).revision,
            mode: "manual",
          });
          const retryProjectId = ProjectId.make("terminal-retry-second-checkout");
          const sharedGitDir =
            sourceChange === "terminal-retry-shared-git-dir" ||
            sourceChange === "terminal-retry-cancelled-shared-git-dir";
          const retryRepository = sharedGitDir
            ? {
                cwd: yield* fs.makeTempDirectoryScoped({
                  prefix: "t3-armed-production-linked-checkout-",
                }),
              }
            : yield* makeRepository().pipe(Effect.provide(gitLayer));
          if (sharedGitDir) {
            yield* git(repository.cwd, [
              "worktree",
              "add",
              "--detach",
              retryRepository.cwd,
              repository.baseCommitSha,
            ]).pipe(Effect.provide(gitLayer));
          }
          yield* seedProject(retryProjectId, retryRepository, [issues[0]!], "run-once");
          yield* controller.processProject(retryProjectId);
          const retryReservations =
            yield* sql`SELECT * FROM agent_control_worktree_reservation_states
            WHERE project_id=${retryProjectId}`;
          assert.lengthOf(retryReservations, 1);
          assert.equal(retryReservations[0]!.status, "ready");
          assert.notEqual(retryReservations[0]!.branch_name, oldReservations[0]!.branch_name);
          assert.equal(
            retryReservations[0]!.repository_common_dir ===
              oldReservations[0]!.repository_common_dir,
            sharedGitDir,
          );
          assert.notEqual(
            retryReservations[0]!.internal_worktree_path,
            oldReservations[0]!.internal_worktree_path,
          );
          const [retryHandoff] = yield* sql<{ handoffId: string }>`SELECT handoff_id AS "handoffId"
            FROM agent_control_initial_planning_handoff_intents WHERE project_id=${retryProjectId}`;
          assert.isDefined(retryHandoff);
          yield* planningWakeupService.wake(retryHandoff!.handoffId);
          yield* planningConsumerService.drain;
          const turnsAfterRetry = fakeProvider.turnCount();
          assert.equal(turnsAfterRetry, 4);
          yield* controller.processProject(retryProjectId);
          yield* restartRecovery();
          yield* restartRecovery();
          yield* planningConsumerService.drain;
          assert.equal(fakeProvider.turnCount(), turnsAfterRetry);
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_worktree_reservation_states WHERE project_id=${retryProjectId}`,
            retryReservations,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_worktree_reservation_states WHERE project_id=${projectId}`,
            oldReservations,
          );
          assert.deepStrictEqual(
            yield* Context.get(context, AgentControlTaskEventStore).readStream(firstTaskId),
            oldTaskEvents,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_run_once_step_evidence
              WHERE project_id=${projectId} ORDER BY run_id, ordinal`,
            oldRunEvidence,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_verification_check_results`,
            oldVerificationEvidence,
          );
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM agent_control_verification_finalization_evidence WHERE project_id=${projectId}`,
            oldFinalization,
          );
          assert.deepStrictEqual(
            yield* git(oldReservations[0]!.internal_worktree_path, ["rev-parse", "HEAD"]).pipe(
              Effect.provide(gitLayer),
            ),
            oldWorktreeHead,
          );
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
          assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
          return;
        }
        if (sourceChange !== "none") return;
        yield* scheduler.processProject(projectId);
        assert.equal((yield* engine.getProjectState({ projectId })).mode, "armed");
        yield* scheduler.processProject(projectId);
        const secondTaskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: issues[1]!.repositoryNodeId,
          issueNodeId: issues[1]!.issueNodeId,
        });
        yield* completeActiveTask(2);
        yield* scheduler.processProject(projectId);
        assert.equal((yield* engine.getProjectState({ projectId })).mode, "armed");
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT mode FROM agent_control_project_states WHERE project_id=${projectId}) AS mode,
              (SELECT count(*) FROM agent_control_armed_dispatch_evidence
               WHERE selected_task_id=${firstTaskId}) AS firstDispatches,
              (SELECT count(*) FROM agent_control_armed_dispatch_evidence
               WHERE selected_task_id=${secondTaskId}) AS secondDispatches,
              (SELECT count(*) FROM agent_control_run_once_activations
               WHERE project_id=${projectId}) AS activations,
              (SELECT count(*) FROM agent_control_task_states
               WHERE task_id=${firstTaskId} AND status='succeeded'
                 AND stage='verification') AS firstFinalized,
              (SELECT count(*) FROM agent_control_task_states
               WHERE task_id=${secondTaskId} AND status='succeeded'
                 AND stage='verification') AS secondFinalized
          `,
          [
            {
              mode: "armed",
              firstDispatches: 1,
              secondDispatches: 1,
              activations: 2,
              firstFinalized: 1,
              secondFinalized: 1,
            },
          ],
        );
        const completedEvidence = yield* sql`SELECT * FROM agent_control_run_once_step_evidence
          WHERE project_id=${projectId} ORDER BY run_id, ordinal`;
        const turnsBefore = fakeProvider.turnCount();
        assert.equal(turnsBefore, 6);
        const checkEvidence = yield* sql`SELECT * FROM agent_control_verification_check_results`;
        assert.lengthOf(checkEvidence, 2);
        assert.deepStrictEqual(
          checkEvidence.map((check) => check.status),
          ["passed", "passed"],
        );
        yield* restartRecovery();
        assert.equal(fakeProvider.turnCount(), turnsBefore);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_run_once_step_evidence
          WHERE project_id=${projectId} ORDER BY run_id, ordinal`,
          completedEvidence,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_verification_check_results`,
          checkEvidence,
        );

        yield* sql`CREATE TRIGGER recovery_diagnostic_write_failure
          BEFORE UPDATE ON agent_control_run_once_diagnostics
          WHEN NEW.project_id = 'a-blocked-recovery-project'
          BEGIN SELECT RAISE(ABORT, 'diagnostic storage unavailable'); END`;
        const diagnosticFailure = yield* restartRecovery().pipe(Effect.flip);
        assert.equal(diagnosticFailure.reason, "persistence");
        yield* sql`DROP TRIGGER recovery_diagnostic_write_failure`;

        const blockedTaskId = yield* deriveAgentControlTaskId({
          projectId: blockedProjectId,
          repositoryNodeId: "armed-production-repository",
          issueNodeId: "armed-production-issue-101",
        });
        const oldRunId = blocked.runId!;
        const [oldCommand] = yield* sql<{ commandId: string }>`SELECT command_id AS "commandId"
          FROM agent_control_worktree_controller_operations WHERE project_id=${blockedProjectId}`;
        const worktreeController = Context.get(context, AgentControlWorktreeController);
        const replayOldCommand = worktreeController.reserveAndMaterializeForRunOnce!(oldRunId, {
          projectId: blockedProjectId,
          taskId: blockedTaskId,
          commandId: CommandId.make(oldCommand!.commandId),
        });
        assert.equal(
          (yield* replayOldCommand.pipe(Effect.flip)).code,
          "default-remote-ref-unavailable",
        );
        yield* engine.dispatchHuman({
          commandId: CommandId.make("blocked-project-end-run"),
          projectId: blockedProjectId,
          expectedRevision: (yield* engine.getProjectState({ projectId: blockedProjectId }))
            .revision,
          mode: "observe",
        });
        if (blockedMode === "armed") {
          // Armed may observe Human takeover before Run-Once cleanup and then crash.
          yield* scheduler.processProject(blockedProjectId);
          assert.deepStrictEqual(
            yield* sql`SELECT status FROM agent_control_armed_dispatch_states
              WHERE project_id=${blockedProjectId}`,
            [{ status: "superseded" }],
          );
          assert.deepStrictEqual(
            yield* sql`SELECT status FROM agent_control_run_once_states
              WHERE run_id=${oldRunId}`,
            [{ status: "active" }],
          );
          yield* restartRecovery();
        } else {
          yield* controller.processProject(blockedProjectId);
        }
        assert.deepStrictEqual(
          yield* sql`SELECT status, last_step AS step
          FROM agent_control_run_once_states WHERE run_id=${oldRunId}`,
          [{ status: "completed", step: "completed" }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT step FROM agent_control_run_once_step_evidence
          WHERE run_id=${oldRunId} AND step IN ('mode-reset-superseded', 'task-terminal-observed')`,
          [{ step: "mode-reset-superseded" }],
        );
        assert.equal(
          (yield* replayOldCommand.pipe(Effect.flip)).code,
          "default-remote-ref-unavailable",
        );
        const revokedAuthority = yield* worktreeController.reserveAndMaterializeForRunOnce!(
          oldRunId,
          {
            projectId: blockedProjectId,
            taskId: blockedTaskId,
            commandId: CommandId.make("revoked-run-new-worktree-command"),
          },
        ).pipe(Effect.flip);
        assert.equal(revokedAuthority.code, "project-mode-inactive");
        const revisionAfterEnd = (yield* engine.getProjectState({ projectId: blockedProjectId }))
          .revision;
        for (const selected of [false, true]) {
          const retry = {
            commandId: CommandId.make(`blocked-task-restart-${selected}`),
            projectId: blockedProjectId,
            expectedRevision: revisionAfterEnd,
            mode: "run-once" as const,
            ...(selected ? { runOnceTaskId: blockedTaskId } : {}),
          };
          const rejected = yield* engine.dispatchHuman(retry).pipe(Effect.flip);
          assert.equal(rejected.code, "mode-not-available");
          const replay = yield* engine.dispatchHuman(retry).pipe(Effect.flip);
          assert.equal(replay.code, "command-previously-rejected");
          assert.equal(
            (yield* engine.getProjectState({ projectId: blockedProjectId })).revision,
            revisionAfterEnd,
          );
          yield* restartRecovery();
        }
        const refreshedAt = DateTime.formatIso(yield* DateTime.now);
        yield* publishSources(
          blockedProjectId,
          [
            {
              ...issue(101, refreshedAt),
              paused: true,
              eligible: false,
              eligibilityReason: "paused",
            },
            issue(102, refreshedAt),
          ],
          2,
        );
        yield* engine.dispatchHuman({
          commandId: CommandId.make("blocked-project-new-run"),
          projectId: blockedProjectId,
          expectedRevision: (yield* engine.getProjectState({ projectId: blockedProjectId }))
            .revision,
          mode: "run-once",
        });
        yield* controller.processProject(blockedProjectId);
        const replacementTaskId = yield* deriveAgentControlTaskId({
          projectId: blockedProjectId,
          repositoryNodeId: "armed-production-repository",
          issueNodeId: "armed-production-issue-102",
        });
        assert.deepStrictEqual(
          yield* sql`SELECT task_id AS taskId
          FROM agent_control_initial_planning_handoff_intents intent
          JOIN agent_control_initial_planning_handoff_accepted accepted USING (handoff_id)
          WHERE intent.project_id=${blockedProjectId}`,
          [{ taskId: replacementTaskId }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_worktree_controller_operations
          WHERE command_id=${oldCommand!.commandId}`,
          rejectedOperations,
        );
        const [replacementHandoff] = yield* sql<{
          handoffId: string;
        }>`SELECT handoff_id AS "handoffId"
          FROM agent_control_initial_planning_handoff_intents WHERE project_id=${blockedProjectId}`;
        yield* planningWakeupService.wake(replacementHandoff!.handoffId);
        yield* planningConsumerService.drain;
        const replacementTurns = fakeProvider.turnCount();
        assert.equal(replacementTurns, turnsBefore + 1);
        yield* restartRecovery();
        yield* planningConsumerService.drain;
        assert.equal(fakeProvider.turnCount(), replacementTurns);
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  60_000,
);
