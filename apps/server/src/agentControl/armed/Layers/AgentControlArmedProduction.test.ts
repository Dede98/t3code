import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  EventId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type AgentControlGithubIssueSnapshot,
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
import { AgentControlControlledThreadActivationHooksNoop } from "../../controlledThreadReservation/Services/AgentControlControlledThreadActivationHooks.ts";
import { AgentControlControlledThreadMaterializationCoordinatorHooksNoop } from "../../controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinatorHooks.ts";
import { AgentControlEngine } from "../../Services/AgentControlEngine.ts";
import { AgentControlGithubEventStore } from "../../github/Services/AgentControlGithubEventStore.ts";
import { AgentControlGithubProjection } from "../../github/Services/AgentControlGithubProjection.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { AgentControlImplementationAdmissionLive } from "../../implementationAdmission/Layers/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationAdmission } from "../../implementationAdmission/Services/AgentControlImplementationAdmission.ts";
import { AgentControlInitialPlanningConsumerLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningConsumer.ts";
import { AgentControlInitialPlanningFinalizerLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeupLive } from "../../initialPlanning/Layers/AgentControlInitialPlanningWakeup.ts";
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
import { AgentControlTaskEngine } from "../../task/Services/AgentControlTaskEngine.ts";
import { AgentControlTaskIntakeReactor } from "../../task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskReconcileStateRepository } from "../../task/Services/AgentControlTaskReconcileState.ts";
import { AgentControlTaskVerificationFinalizerLive } from "../../task/Layers/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlTaskVerificationFinalizer } from "../../task/Services/AgentControlTaskVerificationFinalizer.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
import { AgentControlVerificationAdmissionLive } from "../../verificationAdmission/Layers/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationAdmission } from "../../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
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
const encodeModelSelectionJson = Schema.encodeUnknownSync(Schema.fromJsonString(ModelSelection));
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

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

it.live(
  "runs two initially eligible tasks serially through terminal re-arming on production guards",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
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
        yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const fakeProvider = yield* makeProvider();
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
          Layer.provide(AgentControlControlledThreadActivationHooksNoop),
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
        const githubStates = Context.get(context, AgentControlGithubStateRepository);
        const taskEngine = Context.get(context, AgentControlTaskEngine);
        const reconciles = Context.get(context, AgentControlTaskReconcileStateRepository);
        const scheduler = Context.get(context, AgentControlArmedScheduler);
        assert.isDefined(Context.get(context, AgentControlRunOnceController));
        assert.isDefined(Context.get(context, OrchestrationEngineService));
        assert.isDefined(Context.get(context, ProjectionSnapshotQuery));
        assert.isDefined(fakeProvider.service);
        assert.isDefined(ProviderTurnRequestExecutorLive);
        assert.isDefined(ProjectionTurnRepositoryLive);
        assert.isDefined(ProviderSessionRuntime.layer);

        const at = DateTime.formatIso(yield* DateTime.now);
        yield* sql`
          INSERT INTO main.projection_projects (
            project_id, title, workspace_root, default_model_selection_json,
            scripts_json, created_at, updated_at, deleted_at
          ) VALUES (
            ${projectId}, 'Armed production serial', ${repository.cwd},
            ${encodeModelSelectionJson(modelSelection)}, '[]', ${at}, ${at}, NULL
          )
        `;
        yield* engine.dispatchHuman({
          commandId: CommandId.make("armed-production-observe"),
          projectId,
          expectedRevision: 0,
          mode: "observe",
        });
        yield* engine.dispatchHuman({
          commandId: CommandId.make("armed-production-arm"),
          projectId,
          expectedRevision: 1,
          mode: "armed",
        });
        const configured = yield* githubEvents.append({
          projectId,
          expectedStreamVersion: 0,
          events: [
            {
              eventId: EventId.make("armed-production-config-event"),
              type: "agentControl.github.config.set",
              aggregateKind: "github-intake",
              aggregateId: projectId,
              occurredAt: at,
              commandId: CommandId.make("armed-production-config-command"),
              causationEventId: null,
              correlationId: CommandId.make("armed-production-config-command"),
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
        const issues = [issue(1, at), issue(2, at)];
        const polled = yield* githubEvents.append({
          projectId,
          expectedStreamVersion: 1,
          events: [
            {
              eventId: EventId.make("armed-production-poll-event"),
              type: "agentControl.github.poll.succeeded",
              aggregateKind: "github-intake",
              aggregateId: projectId,
              occurredAt: at,
              commandId: CommandId.make("armed-production-poll-command"),
              causationEventId: null,
              correlationId: CommandId.make("armed-production-poll-command"),
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
        const snapshot = Option.getOrThrow(yield* githubStates.getCompletedSnapshot(projectId));
        for (const source of issues.toReversed()) {
          const taskId = yield* deriveAgentControlTaskId({
            projectId,
            repositoryNodeId: source.repositoryNodeId,
            issueNodeId: source.issueNodeId,
          });
          yield* taskEngine.dispatchObservedController({
            type: "agentControl.task.createFromGithubIssue",
            commandId: CommandId.make(`armed-production-task-${source.number}`),
            taskId,
            projectId,
            expectedRevision: 0,
            sourcePrecondition: snapshot.sourcePrecondition,
            source: {
              projectId,
              repositoryNodeId: source.repositoryNodeId,
              issueNodeId: source.issueNodeId,
              issueNumber: source.number,
              issueUrl: source.url,
            },
            sourceGate: "eligible",
            sourceUpdatedAt: source.updatedAt,
            githubIntakeSequence: polled[0]!.sequence,
            sourceSnapshot: source,
          });
        }
        const reconciling = yield* reconciles.begin(projectId, polled[0]!.sequence, at);
        yield* reconciles.complete(projectId, polled[0]!.sequence, reconciling.revision, at);

        yield* scheduler.processProject(projectId);
        const firstTaskId = yield* deriveAgentControlTaskId({
          projectId,
          repositoryNodeId: issues[0]!.repositoryNodeId,
          issueNodeId: issues[0]!.issueNodeId,
        });
        assert.deepStrictEqual(
          yield* sql`
            SELECT project.mode,
              (SELECT selected_task_id FROM agent_control_armed_dispatch_evidence)
                AS selectedTask,
              (SELECT count(*) FROM agent_control_initial_planning_handoff_accepted)
                AS planningHandoffs
            FROM agent_control_project_states project WHERE project_id=${projectId}
          `,
          [{ mode: "run-once", selectedTask: firstTaskId, planningHandoffs: 1 }],
        );
        assert.lengthOf(
          yield* sql`
            SELECT thread_id FROM projection_threads
            WHERE thread_id IN (SELECT thread_id FROM agent_control_initial_planning_handoff_accepted)
          `,
          1,
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
        const planningConsumer = Layer.fresh(AgentControlInitialPlanningConsumerLive).pipe(
          Layer.provideMerge(planningStore),
          Layer.provideMerge(planningWakeup),
          Layer.provideMerge(executor),
          Layer.provideMerge(projectionTurns),
          Layer.provideMerge(providerRuntime),
          Layer.provideMerge(providerServiceLayer),
          Layer.provideMerge(coreServices),
          Layer.provideMerge(NodeServices.layer),
        );
        const planningFinalizer = Layer.fresh(AgentControlInitialPlanningFinalizerLive).pipe(
          Layer.provideMerge(planningStore),
          Layer.provideMerge(planningWakeup),
          Layer.provideMerge(coreServices),
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
            yield* orchestrationEngine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:${input.event.eventId}:thread-session-set`),
              threadId: input.event.threadId,
              session: {
                threadId: input.event.threadId,
                status: input.event.payload.state === "completed" ? "ready" : "error",
                providerName: provider,
                providerInstanceId,
                runtimeMode: input.runtimeMode,
                activeTurnId: null,
                lastError: input.event.payload.state === "completed" ? null : "provider failed",
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
          readonly state: "completed" | "failed";
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
            status: input.state === "completed" ? "ready" : "error",
            updatedAt: terminalAt,
            ...(input.state === "completed" ? {} : { lastError: "provider failed" }),
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

        yield* planningConsumerService.start().pipe(Scope.provide(scope));
        const completeActiveTask = Effect.fn("completeArmedProductionActiveTask")(function* (
          runOrdinal: number,
        ) {
          const [accepted] = yield* sql<{ readonly handoffId: string }>`
            SELECT handoff_id AS "handoffId"
            FROM agent_control_initial_planning_deliveries
            WHERE state IN ('pending', 'provider-started')
            ORDER BY rowid DESC
            LIMIT 1
          `;
          assert.isDefined(accepted);
          yield* planningWakeupService.wake(accepted!.handoffId);
          yield* planningConsumerService.drain;
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

          const implementationAdmitted = yield* implementationAdmissionService.processHandoff(
            planningDelivery!.handoffId,
          );
          assert.equal(implementationAdmitted._tag, "Admitted");
          const implementationMaterialized = yield* implementationCoordinatorService.processHandoff(
            planningDelivery!.handoffId,
          );
          assert.equal(implementationMaterialized._tag, "Materialized");
          if (implementationMaterialized._tag !== "Materialized") {
            return yield* Effect.die("implementation was not materialized");
          }
          const implementationHandoffId =
            implementationMaterialized.publication.implementationHandoffId;
          yield* implementationConsumerService.processHandoff(implementationHandoffId);
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

          const verificationAdmitted = yield* verificationAdmissionService.processResultEvidence(
            implementationFinalized.resultEvidenceId,
          );
          assert.equal(verificationAdmitted._tag, "Admitted");
          const verificationMaterialized = yield* verificationCoordinatorService.processHandoff(
            implementationFinalized.resultEvidenceId,
          );
          assert.equal(verificationMaterialized._tag, "Materialized");
          if (verificationMaterialized._tag !== "Materialized") {
            return yield* Effect.die("verification was not materialized");
          }
          const verificationHandoffId = verificationMaterialized.publication.verificationHandoffId;
          yield* verificationConsumerService.processHandoff(verificationHandoffId);
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
          const verificationTerminal = yield* publishTerminal({
            prefix: `armed-production-verification-${runOrdinal}`,
            threadId: ThreadId.make(verificationDelivery!.threadId),
            turnId: TurnId.make(verificationDelivery!.providerTurnId),
            state: "failed",
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
        });

        yield* completeActiveTask(1);
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
              (SELECT count(*) FROM agent_control_run_once_activations) AS activations,
              (SELECT count(*) FROM agent_control_task_states
               WHERE task_id=${firstTaskId} AND status='failed'
                 AND stage='verification') AS firstFinalized,
              (SELECT count(*) FROM agent_control_task_states
               WHERE task_id=${secondTaskId} AND status='failed'
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
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  60_000,
);
