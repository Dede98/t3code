import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
import * as NodeSqlite from "node:sqlite";
import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  IsoDateTime,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderSession,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
  type AgentControlStageRunEvent,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseEventDraft,
  AgentControlTaskState,
  AgentControlWorktreeReservationState,
  type AgentControlControlledThreadReservationEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import {
  makeMigration059,
  type Migration059FaultPoint,
} from "../../../persistence/Migrations/059_AgentControlVerificationTurnTerminalObservation.ts";
import { makeReactorStartupAttempt } from "../../../reactorStartupActivation.ts";
import { ServerConfig } from "../../../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../../../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../../../persistence/Layers/OrchestrationEventStore.ts";
import { AgentControlCommandReceiptRepositoryLive } from "../../../persistence/Layers/AgentControlCommandReceipts.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import * as RepositoryIdentityResolver from "../../../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "../../../orchestration/Layers/OrchestrationEngine.ts";
import { ProviderRuntimeIngestionLive } from "../../../orchestration/Layers/ProviderRuntimeIngestion.ts";
import { ProviderTurnRequestExecutorLive } from "../../../orchestration/Layers/ProviderTurnRequestExecutor.ts";
import { OrchestrationProjectionPipelineLive } from "../../../orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "../../../orchestration/Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProviderRuntimeIngestionService } from "../../../orchestration/Services/ProviderRuntimeIngestion.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import {
  ProviderTurnDeliveryError,
  ProviderTurnRequestExecutor,
} from "../../../orchestration/Services/ProviderTurnRequestExecutor.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
  ProviderInstanceNotFoundError,
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderServiceError,
  type ProviderAdapterError,
} from "../../../provider/Errors.ts";
import {
  attestProviderNativeTurnConfiguration,
  attestProviderSessionNativeConfiguration,
  canonicalProviderModelSelectionEvidence,
  type ProviderAdapterShape,
} from "../../../provider/Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../../../provider/Services/ProviderAdapterRegistry.ts";
import { ProviderRegistry } from "../../../provider/Services/ProviderRegistry.ts";
import {
  ProviderService,
  type ProviderRuntimeEventPublication,
} from "../../../provider/Services/ProviderService.ts";
import { makeProviderServiceLive } from "../../../provider/Layers/ProviderService.ts";
import * as ProviderEventLoggers from "../../../provider/Layers/ProviderEventLoggers.ts";
import { ProviderSessionDirectoryLive } from "../../../provider/Layers/ProviderSessionDirectory.ts";
import { ProviderSessionDirectory } from "../../../provider/Services/ProviderSessionDirectory.ts";
import { makeAdapterRegistryMock } from "../../../provider/testUtils/providerAdapterRegistryMock.ts";
import * as ProviderSessionRuntime from "../../../persistence/ProviderSessionRuntime.ts";
import { ServerSettingsService } from "../../../serverSettings.ts";
import * as AnalyticsService from "../../../telemetry/AnalyticsService.ts";
import { AgentControlPolicyService } from "../../AgentControlPolicyService.ts";
import { layer as AgentControlControlledThreadReservationEventStoreLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadReservationEventStore.ts";
import { layer as AgentControlControlledThreadReservationProjectionLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadReservationProjection.ts";
import { layer as AgentControlControlledThreadReservationStateRepositoryLive } from "../../controlledThreadReservation/Layers/AgentControlControlledThreadReservationStateRepository.ts";
import { decideAgentControlControlledThreadReservationCommand } from "../../controlledThreadReservation/decider.ts";
import {
  deriveAgentControlBoundTransitionCommandId,
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlMaterializingTransitionCommandId,
  deriveAgentControlReservedThreadId,
  deriveAgentControlThreadMaterializationCommandId,
} from "../../controlledThreadReservation/identity.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../../controlledThreadReservation/projector.ts";
import { AgentControlControlledThreadReservationEngine } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../../controlledThreadReservation/Services/AgentControlControlledThreadReservationStateRepository.ts";
import { layer as AgentControlStageRunEventStoreLive } from "../../stageRun/Layers/AgentControlStageRunEventStore.ts";
import { layer as AgentControlStageRunEngineLive } from "../../stageRun/Layers/AgentControlStageRunEngine.ts";
import { layer as AgentControlStageRunProjectionLive } from "../../stageRun/Layers/AgentControlStageRunProjection.ts";
import { layer as AgentControlStageRunStateRepositoryLive } from "../../stageRun/Layers/AgentControlStageRunStateRepository.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import {
  AgentControlStageRunEventStore,
  type AgentControlStageRunEventStoreShape,
} from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { layer as AgentControlStageRunLeaseEventStoreLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseEventStore.ts";
import { layer as AgentControlStageRunLeaseProjectionLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseProjection.ts";
import { layer as AgentControlStageRunLeaseStateRepositoryLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseStateRepository.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlTaskConsumerGuard } from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { decideAgentControlTaskCommand } from "../../task/decider.ts";
import { deriveAgentControlTaskId } from "../../task/identity.ts";
import { projectAgentControlTaskEvent } from "../../task/projector.ts";
import { layer as AgentControlTaskEventStoreLive } from "../../task/Layers/AgentControlTaskEventStore.ts";
import { layer as AgentControlTaskStateRepositoryLive } from "../../task/Layers/AgentControlTaskStateRepository.ts";
import { AgentControlTaskEventStore } from "../../task/Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskStateRepository } from "../../task/Services/AgentControlTaskStateRepository.ts";
import {
  AgentControlPersistenceSqlError,
  AgentControlStageRunStreamVersionConflictError,
} from "../../Errors.ts";
import { layer as AgentControlWorktreeEventStoreLive } from "../../worktree/Layers/AgentControlWorktreeEventStore.ts";
import { layer as AgentControlWorktreeStateRepositoryLive } from "../../worktree/Layers/AgentControlWorktreeStateRepository.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlWorktreeEventStore } from "../../worktree/Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeStateRepository } from "../../worktree/Services/AgentControlWorktreeStateRepository.ts";
import {
  deriveAgentControlWorktreePathKeys,
  deriveAgentControlWorktreeReservationId,
} from "../../worktree/identity.ts";
import { AgentControlImplementationAdmissionLive } from "../../implementationAdmission/Layers/AgentControlImplementationAdmission.ts";
import {
  AgentControlImplementationAdmission,
  type AgentControlImplementationAdmissionShape,
} from "../../implementationAdmission/Services/AgentControlImplementationAdmission.ts";
import {
  AgentControlImplementationAdmissionHooks,
  type AgentControlImplementationAdmissionHooksShape,
} from "../../implementationAdmission/Services/AgentControlImplementationAdmissionHooks.ts";
import { AgentControlImplementationHandoffStoreLive } from "../../implementationTurn/Layers/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationStageFinalizerLive } from "../../implementationTurn/Layers/AgentControlImplementationStageFinalizer.ts";
import { AgentControlImplementationStageStarterLive } from "../../implementationTurn/Layers/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationTurnConsumerLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnConsumer.ts";
import { AgentControlImplementationTurnCoordinatorLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlImplementationTurnWakeupLive } from "../../implementationTurn/Layers/AgentControlImplementationTurnWakeup.ts";
import {
  AgentControlImplementationHandoffStore,
  AgentControlImplementationStoreError,
} from "../../implementationTurn/Services/AgentControlImplementationHandoffStore.ts";
import {
  AgentControlImplementationStageStarter,
  type AgentControlImplementationStageStarterShape,
} from "../../implementationTurn/Services/AgentControlImplementationStageStarter.ts";
import {
  AgentControlImplementationStageFinalizer,
  AgentControlImplementationStageFinalizerError,
  type AgentControlImplementationStageFinalizerShape,
} from "../../implementationTurn/Services/AgentControlImplementationStageFinalizer.ts";
import {
  AgentControlImplementationStageFinalizerHooks,
  type AgentControlImplementationStageFinalizerHooksShape,
} from "../../implementationTurn/Services/AgentControlImplementationStageFinalizerHooks.ts";
import {
  AgentControlImplementationStageStarterHooks,
  type AgentControlImplementationStageStarterHooksShape,
} from "../../implementationTurn/Services/AgentControlImplementationStageStarterHooks.ts";
import {
  AgentControlImplementationTurnConsumer,
  type AgentControlImplementationTurnConsumerShape,
} from "../../implementationTurn/Services/AgentControlImplementationTurnConsumer.ts";
import {
  AgentControlImplementationTurnConsumerHooks,
  type AgentControlImplementationTurnConsumerHooksShape,
} from "../../implementationTurn/Services/AgentControlImplementationTurnConsumerHooks.ts";
import {
  AgentControlImplementationTurnCoordinator,
  type AgentControlImplementationTurnCoordinatorShape,
} from "../../implementationTurn/Services/AgentControlImplementationTurnCoordinator.ts";
import {
  AgentControlImplementationTurnCoordinatorHooks,
  type AgentControlImplementationTurnCoordinatorHooksShape,
} from "../../implementationTurn/Services/AgentControlImplementationTurnCoordinatorHooks.ts";
import {
  AgentControlImplementationTurnWakeup,
  type AgentControlImplementationTurnWakeupShape,
} from "../../implementationTurn/Services/AgentControlImplementationTurnWakeup.ts";
import {
  implementationMessagePayload,
  implementationTurnRequestPayload,
} from "../../implementationTurn/eventEvidence.ts";
import { AgentControlVerificationAdmissionLive } from "../../verificationAdmission/Layers/AgentControlVerificationAdmission.ts";
import {
  AgentControlVerificationAdmission,
  AgentControlVerificationAdmissionError,
  type AgentControlVerificationAdmissionShape,
} from "../../verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import {
  AgentControlVerificationAdmissionHooks,
  type AgentControlVerificationAdmissionHooksShape,
} from "../../verificationAdmission/Services/AgentControlVerificationAdmissionHooks.ts";
import { AgentControlVerificationHandoffStoreLive } from "../../verificationTurn/Layers/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationStageStarterLive } from "../../verificationTurn/Layers/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationTurnConsumerLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnConsumer.ts";
import { AgentControlVerificationTurnCoordinatorLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationTurnWakeupLive } from "../../verificationTurn/Layers/AgentControlVerificationTurnWakeup.ts";
import {
  AgentControlVerificationHandoffStore,
  AgentControlVerificationStoreError,
  isAgentControlVerificationCandidateEvidenceError,
} from "../../verificationTurn/Services/AgentControlVerificationHandoffStore.ts";
import {
  AgentControlVerificationStageStarter,
  AgentControlVerificationStageStarterError,
  type AgentControlVerificationStageStarterShape,
} from "../../verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import {
  AgentControlVerificationStageStarterHooks,
  type AgentControlVerificationStageStarterHooksShape,
} from "../../verificationTurn/Services/AgentControlVerificationStageStarterHooks.ts";
import {
  AgentControlVerificationTurnConsumer,
  type AgentControlVerificationTurnConsumerShape,
} from "../../verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";
import {
  AgentControlVerificationTurnConsumerHooks,
  type AgentControlVerificationTurnConsumerHooksShape,
} from "../../verificationTurn/Services/AgentControlVerificationTurnConsumerHooks.ts";
import {
  AgentControlVerificationTurnCoordinator,
  type AgentControlVerificationTurnCoordinatorShape,
} from "../../verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import {
  AgentControlVerificationTurnCoordinatorHooks,
  type AgentControlVerificationTurnCoordinatorHooksShape,
} from "../../verificationTurn/Services/AgentControlVerificationTurnCoordinatorHooks.ts";
import {
  AgentControlVerificationTurnWakeup,
  type AgentControlVerificationTurnWakeupShape,
} from "../../verificationTurn/Services/AgentControlVerificationTurnWakeup.ts";
import {
  deriveVerificationStageStartCommandId,
  deriveVerificationStageStartEvidenceId,
  deriveVerificationStageStartEventId,
  deriveVerificationStageStartMarkerId,
  deriveVerificationStageStartReceiptId,
  fingerprintVerificationTurn,
} from "../../verificationTurn/identity.ts";
import {
  AgentControlVerificationOrchestrationHistoryError,
  loadVerificationTerminalFromOrchestrationHistory,
} from "../../verificationTurn/orchestrationTerminalHistory.ts";
import { normalizeVerificationTerminal } from "../../verificationTurn/terminalObservation.ts";
import {
  deriveImplementationResultEvidenceId,
  fingerprintImplementationHandoff,
} from "../../implementationTurn/identity.ts";
import type { AgentControlImplementationHandoffEvidence } from "../../implementationTurn/model.ts";
import {
  canonicalInitialPlanningEventTemplate,
  canonicalJson,
  combinedInitialPlanningEventDigest,
  initialPlanningMessagePayload,
  initialPlanningTurnRequestPayload,
  sha256Utf8,
  type JsonValue,
} from "../eventEvidence.ts";
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageEventId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  deriveAgentControlInitialPlanningTurnRequestEventId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../identity.ts";
import type { AgentControlInitialPlanningHandoffEvidence } from "../model.ts";
import {
  AgentControlInitialPlanningFinalizer,
  AgentControlInitialPlanningFinalizerError,
  type AgentControlInitialPlanningFinalizerShape,
} from "../Services/AgentControlInitialPlanningFinalizer.ts";
import {
  AgentControlInitialPlanningFinalizerHooks,
  type AgentControlInitialPlanningFinalizerHooksShape,
} from "../Services/AgentControlInitialPlanningFinalizerHooks.ts";
import { AgentControlInitialPlanningHandoffStore } from "../Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningFinalizerLive } from "./AgentControlInitialPlanningFinalizer.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "./AgentControlInitialPlanningHandoffStore.ts";

const createdAt = "2026-08-02T08:00:00.000Z";
const providerAcceptedAt = "2026-08-02T08:01:00.000Z";
const terminalAt = "2026-08-02T08:02:00.000Z";
const shiftIso = (value: string, milliseconds: number) =>
  DateTime.formatIso(DateTime.add(DateTime.makeUnsafe(value), { milliseconds }));
const stableFixtureOrdinal = (value: string) =>
  Number.parseInt(NodeCrypto.createHash("sha256").update(value).digest("hex").slice(0, 9), 16) + 1;
const expiresAt = "2099-08-02T09:00:00.000Z";
const deadlineAt = "2026-08-02T10:00:00.000Z";
const barrierTimeout = "5 seconds";
const isFinalizerError = Schema.is(AgentControlInitialPlanningFinalizerError);
const isImplementationFinalizerError = Schema.is(AgentControlImplementationStageFinalizerError);
const isVerificationAdmissionError = Schema.is(AgentControlVerificationAdmissionError);
const isVerificationStoreError = Schema.is(AgentControlVerificationStoreError);
const decodeUnknownJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);
const encodeUnknownJson = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
const fixtureFingerprint = (value: string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const noopHooks: AgentControlInitialPlanningFinalizerHooksShape = {
  afterAuthoritativeRead: () => Effect.void,
  beforeTransactionComplete: () => Effect.void,
  afterNativeCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopAdmissionHooks: AgentControlImplementationAdmissionHooksShape = {
  afterAuthoritativeRead: () => Effect.void,
  beforeWrites: () => Effect.void,
  beforeFinalMarker: () => Effect.void,
  afterNativeCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopImplementationCoordinatorHooks: AgentControlImplementationTurnCoordinatorHooksShape = {
  afterAdmissionReplay: () => Effect.void,
  afterMaterializingProjection: () => Effect.void,
  afterOrchestrationMaterialization: () => Effect.void,
  afterBoundProjection: () => Effect.void,
  afterHandoffAccepted: () => Effect.void,
  beforeMaterializationMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopImplementationConsumerHooks: AgentControlImplementationTurnConsumerHooksShape = {
  beforeClaim: () => Effect.void,
  afterClaim: () => Effect.void,
};
const noopImplementationStageStarterHooks: AgentControlImplementationStageStarterHooksShape = {
  afterProviderEvidence: () => Effect.void,
  afterStageProjection: () => Effect.void,
  beforeFinalMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopImplementationStageFinalizerHooks: AgentControlImplementationStageFinalizerHooksShape = {
  afterAuthoritativeEvidence: () => Effect.void,
  beforeAppend: () => Effect.void,
  beforeFinalMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopVerificationAdmissionHooks: AgentControlVerificationAdmissionHooksShape = {
  afterFinalizerSubscriptionAcquired: () => Effect.void,
  afterStageRunSubscriptionAcquired: () => Effect.void,
  beforeStartupRecovery: () => Effect.void,
  recoveryPageSize: 100,
  afterAuthoritativeRead: () => Effect.void,
  beforeWrites: () => Effect.void,
  beforeFinalMarker: () => Effect.void,
  afterNativeCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopVerificationCoordinatorHooks: AgentControlVerificationTurnCoordinatorHooksShape = {
  afterAdmissionReplay: () => Effect.void,
  afterMaterializingProjection: () => Effect.void,
  afterOrchestrationMaterialization: () => Effect.void,
  afterBoundProjection: () => Effect.void,
  afterHandoffAccepted: () => Effect.void,
  beforeMaterializationMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopVerificationConsumerHooks: AgentControlVerificationTurnConsumerHooksShape = {
  beforeClaim: () => Effect.void,
  afterClaim: () => Effect.void,
};
const noopVerificationStageStarterHooks: AgentControlVerificationStageStarterHooksShape = {
  afterProviderEvidence: () => Effect.void,
  afterStageProjection: () => Effect.void,
  beforeFinalMarker: () => Effect.void,
  afterOuterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};

interface SharedDatabase {
  readonly filename: string;
  readonly sqlA: SqlClient.SqlClient;
  readonly sqlB: SqlClient.SqlClient;
  readonly scopeA: Scope.Closeable;
  readonly scopeB: Scope.Closeable;
}

const makeSharedDatabase = Effect.fn("makeInitialPlanningFinalizerDatabase")(function* (
  toMigrationInclusive?: number,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({
    prefix: "t3-initial-planning-finalizer-",
  });
  const filename = path.join(directory, "state.sqlite");
  const scopeA = yield* Scope.make("sequential");
  const scopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
  yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
  const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
  const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
  const sqlA = Context.get(contextA, SqlClient.SqlClient);
  const sqlB = Context.get(contextB, SqlClient.SqlClient);
  for (const sql of [sqlA, sqlB]) {
    const journal = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* sql`PRAGMA busy_timeout = 5000`;
    assert.equal(journal[0]?.journal_mode, "wal");
  }
  const canonical = yield* fs.realPath(filename);
  const [databaseA] = yield* sqlA<{ readonly file: string }>`PRAGMA database_list`;
  const [databaseB] = yield* sqlB<{ readonly file: string }>`PRAGMA database_list`;
  assert.equal(databaseA?.file, canonical);
  assert.equal(databaseB?.file, canonical);
  assert.notStrictEqual(sqlA, sqlB);
  yield* runMigrations({ toMigrationInclusive }).pipe(
    Effect.provideService(SqlClient.SqlClient, sqlA),
  );
  return { filename, sqlA, sqlB, scopeA, scopeB } satisfies SharedDatabase;
});

interface FinalizerHarness {
  readonly finalizer: AgentControlInitialPlanningFinalizerShape;
  readonly store: AgentControlInitialPlanningHandoffStore["Service"];
  readonly stageEvents: AgentControlStageRunEventStore["Service"];
  readonly stageStates: AgentControlStageRunStateRepository["Service"];
  readonly stageProjection: AgentControlStageRunProjection["Service"];
  readonly stageEngine: AgentControlStageRunEngine["Service"];
  readonly leaseEvents: AgentControlStageRunLeaseEventStore["Service"];
  readonly leaseStates: AgentControlStageRunLeaseStateRepository["Service"];
  readonly leaseProjection: AgentControlStageRunLeaseProjection["Service"];
  readonly leaseEngine: AgentControlStageRunLeaseEngine["Service"];
  readonly stagePublished: Ref.Ref<ReadonlyArray<AgentControlStageRunEvent>>;
  readonly leasePublished: Ref.Ref<ReadonlyArray<AgentControlStageRunLeaseEvent>>;
}

const buildFinalizer = Effect.fn("buildInitialPlanningFinalizerHarness")(function* (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  hooks: AgentControlInitialPlanningFinalizerHooksShape = noopHooks,
  runtimeHolderId = "runtime-holder",
) {
  const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
  const build = <I, E>(layer: Layer.Layer<I, E, never>) => Layer.buildWithScope(layer, scope);
  const stageEventContext = yield* build(
    Layer.fresh(AgentControlStageRunEventStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const stageStateContext = yield* build(
    Layer.fresh(AgentControlStageRunStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const leaseEventContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseEventStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const leaseStateContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const cursorContext = yield* build(
    Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const stageEvents = Context.get(stageEventContext, AgentControlStageRunEventStore);
  const stageStates = Context.get(stageStateContext, AgentControlStageRunStateRepository);
  const leaseEvents = Context.get(leaseEventContext, AgentControlStageRunLeaseEventStore);
  const leaseStates = Context.get(leaseStateContext, AgentControlStageRunLeaseStateRepository);
  const cursors = Context.get(cursorContext, AgentControlProjectionStateRepository);
  const stageProjectionContext = yield* build(
    Layer.fresh(AgentControlStageRunProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlStageRunEventStore, stageEvents),
          Layer.succeed(AgentControlStageRunStateRepository, stageStates),
          Layer.succeed(AgentControlProjectionStateRepository, cursors),
        ),
      ),
    ),
  );
  const leaseProjectionContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlStageRunLeaseEventStore, leaseEvents),
          Layer.succeed(AgentControlStageRunLeaseStateRepository, leaseStates),
          Layer.succeed(AgentControlProjectionStateRepository, cursors),
        ),
      ),
    ),
  );
  const stageProjection = Context.get(stageProjectionContext, AgentControlStageRunProjection);
  const leaseProjection = Context.get(leaseProjectionContext, AgentControlStageRunLeaseProjection);
  const storeContext = yield* build(
    Layer.fresh(AgentControlInitialPlanningHandoffStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const store = Context.get(storeContext, AgentControlInitialPlanningHandoffStore);
  const stagePublished = yield* Ref.make<ReadonlyArray<AgentControlStageRunEvent>>([]);
  const leasePublished = yield* Ref.make<ReadonlyArray<AgentControlStageRunLeaseEvent>>([]);
  const receiptContext = yield* build(
    Layer.fresh(AgentControlCommandReceiptRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const stageEngineContext = yield* build(
    Layer.fresh(AgentControlStageRunEngineLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          Layer.succeed(AgentControlStageRunEventStore, stageEvents),
          Layer.succeed(AgentControlStageRunStateRepository, stageStates),
          Layer.succeed(AgentControlStageRunProjection, stageProjection),
          Layer.succeed(
            AgentControlCommandReceiptRepository,
            Context.get(receiptContext, AgentControlCommandReceiptRepository),
          ),
        ),
      ),
      Layer.provideMerge(NodeServices.layer),
    ),
  );
  const liveStageEngine = Context.get(stageEngineContext, AgentControlStageRunEngine);
  const stageEngine = AgentControlStageRunEngine.of({
    ...liveStageEngine,
    publishCommitted: (events: ReadonlyArray<AgentControlStageRunEvent>) =>
      liveStageEngine
        .publishCommitted(events)
        .pipe(Effect.andThen(Ref.update(stagePublished, (current) => [...current, ...events]))),
  });
  const leaseEngine = {
    runtimeHolderId: Effect.succeed(AgentControlStageRunLeaseHolderId.make(runtimeHolderId)),
    publishCommitted: (events: ReadonlyArray<AgentControlStageRunLeaseEvent>) =>
      Ref.update(leasePublished, (current) => [...current, ...events]),
  } as AgentControlStageRunLeaseEngine["Service"];
  const orchestrationEngine = {} as OrchestrationEngineService["Service"];
  const dependencies = Layer.mergeAll(
    sqlLayer,
    Layer.succeed(AgentControlInitialPlanningHandoffStore, store),
    Layer.succeed(AgentControlStageRunEventStore, stageEvents),
    Layer.succeed(AgentControlStageRunStateRepository, stageStates),
    Layer.succeed(AgentControlStageRunProjection, stageProjection),
    Layer.succeed(AgentControlStageRunEngine, stageEngine),
    Layer.succeed(AgentControlStageRunLeaseEventStore, leaseEvents),
    Layer.succeed(AgentControlStageRunLeaseStateRepository, leaseStates),
    Layer.succeed(AgentControlStageRunLeaseProjection, leaseProjection),
    Layer.succeed(AgentControlStageRunLeaseEngine, leaseEngine),
    Layer.succeed(OrchestrationEngineService, orchestrationEngine),
    Layer.succeed(AgentControlInitialPlanningFinalizerHooks, hooks),
  );
  const finalizerContext = yield* build(
    Layer.fresh(AgentControlInitialPlanningFinalizerLive).pipe(Layer.provide(dependencies)),
  );
  return {
    finalizer: Context.get(finalizerContext, AgentControlInitialPlanningFinalizer),
    store,
    stageEvents,
    stageStates,
    stageProjection,
    stageEngine,
    leaseEvents,
    leaseStates,
    leaseProjection,
    leaseEngine,
    stagePublished,
    leasePublished,
  } satisfies FinalizerHarness;
});

interface AdmissionHarness {
  readonly admission: AgentControlImplementationAdmissionShape;
  readonly reservationEvents: AgentControlControlledThreadReservationEventStore["Service"];
  readonly reservationStates: AgentControlControlledThreadReservationStateRepository["Service"];
  readonly reservationProjection: AgentControlControlledThreadReservationProjection["Service"];
  readonly reservationEngine: AgentControlControlledThreadReservationEngine["Service"];
  readonly reservationPublished: Ref.Ref<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>
  >;
}

const buildAdmission = Effect.fn("buildImplementationAdmissionHarness")(function* (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  finalizerHarness: FinalizerHarness,
  taskSource: AgentControlTaskState | Map<string, AgentControlTaskState>,
  worktreeSource:
    | AgentControlWorktreeReservationState
    | Map<string, AgentControlWorktreeReservationState>,
  hooks: AgentControlImplementationAdmissionHooksShape,
) {
  const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
  const build = <I, E>(layer: Layer.Layer<I, E, never>) => Layer.buildWithScope(layer, scope);
  const reservationEventContext = yield* build(
    Layer.fresh(AgentControlControlledThreadReservationEventStoreLive).pipe(
      Layer.provide(sqlLayer),
    ),
  );
  const reservationStateContext = yield* build(
    Layer.fresh(AgentControlControlledThreadReservationStateRepositoryLive).pipe(
      Layer.provide(sqlLayer),
    ),
  );
  const cursorContext = yield* build(
    Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const reservationEvents = Context.get(
    reservationEventContext,
    AgentControlControlledThreadReservationEventStore,
  );
  const reservationStates = Context.get(
    reservationStateContext,
    AgentControlControlledThreadReservationStateRepository,
  );
  const reservationProjectionContext = yield* build(
    Layer.fresh(AgentControlControlledThreadReservationProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlControlledThreadReservationEventStore, reservationEvents),
          Layer.succeed(AgentControlControlledThreadReservationStateRepository, reservationStates),
          Layer.succeed(
            AgentControlProjectionStateRepository,
            Context.get(cursorContext, AgentControlProjectionStateRepository),
          ),
        ),
      ),
    ),
  );
  const reservationProjection = Context.get(
    reservationProjectionContext,
    AgentControlControlledThreadReservationProjection,
  );
  const reservationPublished = yield* Ref.make<
    ReadonlyArray<AgentControlControlledThreadReservationEvent>
  >([]);
  const reservationEngine = {
    publishCommitted: (events: ReadonlyArray<AgentControlControlledThreadReservationEvent>) =>
      Ref.update(reservationPublished, (current) => [...current, ...events]),
    streamDomainEvents: Stream.never,
  } as unknown as AgentControlControlledThreadReservationEngine["Service"];
  const resolveTask = (projectId: string, taskId: string) =>
    taskSource instanceof Map
      ? taskSource.get(`${projectId}:${taskId}`)
      : taskSource.source.projectId === projectId && taskSource.taskId === taskId
        ? taskSource
        : undefined;
  const resolveWorktree = (projectId: string, reservationId: string) =>
    worktreeSource instanceof Map
      ? worktreeSource.get(`${projectId}:${reservationId}`)
      : worktreeSource.projectId === projectId && worktreeSource.reservationId === reservationId
        ? worktreeSource
        : undefined;
  const taskGuard = AgentControlTaskConsumerGuard.of({
    inspectProject: (projectId) =>
      Effect.gen(function* () {
        const task =
          taskSource instanceof Map
            ? [...taskSource.values()].find((candidate) => candidate.source.projectId === projectId)
            : taskSource;
        if (task === undefined) return yield* Effect.die("unexpected task project");
        return {
          projectId: task.source.projectId,
          activation: "observe",
          currentSourceSequence: task.githubIntakeSequence,
          targetSequence: task.githubIntakeSequence,
          lastCompletedSequence: task.githubIntakeSequence,
          watermarkCompleted: true,
          sequenceCurrent: true,
          sourceFingerprint: yield* deriveAgentControlSourceIdentityFingerprint(task),
          reason: null,
        } as never;
      }),
    useTaskConsumable: (projectId, taskId, use) => {
      const task = resolveTask(projectId, taskId);
      return task === undefined ? Effect.die("unexpected task identity") : use(task, {} as never);
    },
    useTaskConsumableInTransaction: (projectId, taskId, use) => {
      const task = resolveTask(projectId, taskId);
      return task === undefined ? Effect.die("unexpected task identity") : use(task, {} as never);
    },
  });
  const worktreeController = AgentControlWorktreeController.of({
    reserveAndMaterialize: () => Effect.die("unused"),
    reconcile: () => Effect.die("unused"),
    useReadyWorktree: (input, callback, options) =>
      Effect.gen(function* () {
        if (options?.beforeInspection !== undefined) {
          const replay = yield* options.beforeInspection;
          if (Option.isSome(replay)) return replay.value;
        }
        const worktree = resolveWorktree(input.projectId, input.reservationId);
        if (worktree === undefined) return yield* Effect.die("unexpected worktree identity");
        return yield* Effect.scoped(callback(worktree));
      }),
  });
  const dependencies = Layer.mergeAll(
    sqlLayer,
    Layer.succeed(AgentControlInitialPlanningFinalizer, finalizerHarness.finalizer),
    Layer.succeed(AgentControlImplementationAdmissionHooks, hooks),
    Layer.succeed(AgentControlTaskConsumerGuard, taskGuard),
    Layer.succeed(AgentControlWorktreeController, worktreeController),
    Layer.succeed(AgentControlStageRunEventStore, finalizerHarness.stageEvents),
    Layer.succeed(AgentControlStageRunStateRepository, finalizerHarness.stageStates),
    Layer.succeed(AgentControlStageRunProjection, finalizerHarness.stageProjection),
    Layer.succeed(AgentControlStageRunEngine, finalizerHarness.stageEngine),
    Layer.succeed(AgentControlStageRunLeaseEventStore, finalizerHarness.leaseEvents),
    Layer.succeed(AgentControlStageRunLeaseStateRepository, finalizerHarness.leaseStates),
    Layer.succeed(AgentControlStageRunLeaseProjection, finalizerHarness.leaseProjection),
    Layer.succeed(AgentControlStageRunLeaseEngine, finalizerHarness.leaseEngine),
    Layer.succeed(AgentControlControlledThreadReservationEventStore, reservationEvents),
    Layer.succeed(AgentControlControlledThreadReservationStateRepository, reservationStates),
    Layer.succeed(AgentControlControlledThreadReservationProjection, reservationProjection),
    Layer.succeed(AgentControlControlledThreadReservationEngine, reservationEngine),
  );
  const admissionContext = yield* build(
    Layer.fresh(AgentControlImplementationAdmissionLive).pipe(Layer.provide(dependencies)),
  );
  return {
    admission: Context.get(admissionContext, AgentControlImplementationAdmission),
    reservationEvents,
    reservationStates,
    reservationProjection,
    reservationEngine,
    reservationPublished,
  } satisfies AdmissionHarness;
});

type DeliveryState = "provider-started" | "completed" | "failed" | "interrupted" | "ambiguous";
type IdentityMismatch =
  | "handoff"
  | "reservation"
  | "thread"
  | "task"
  | "stageRun"
  | "attempt"
  | "lease"
  | "holder"
  | "fence"
  | "delivery";

interface SeededPlanning {
  readonly evidence: AgentControlInitialPlanningHandoffEvidence;
  readonly providerTurnId: TurnId;
  readonly planId: string;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly leaseId: AgentControlStageRunLeaseId;
}

const admissionTask = (suffix: string): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId: AgentControlTaskId.make(`task-${suffix}`),
  source: {
    projectId: ProjectId.make(`project-${suffix}`),
    repositoryNodeId: `repository-${suffix}`,
    issueNodeId: `issue-${suffix}`,
    issueNumber: 17,
    issueUrl: `https://example.test/${suffix}/issues/17`,
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt: createdAt,
  githubIntakeSequence: 1,
  sourceSnapshot: {
    repositoryNodeId: `repository-${suffix}`,
    issueNodeId: `issue-${suffix}`,
    number: 17,
    url: `https://example.test/${suffix}/issues/17`,
    state: "open",
    title: "Untrusted implementation admission fixture",
    body: null,
    contentTrust: "untrusted-external",
    updatedAt: createdAt,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt,
  updatedAt: createdAt,
  revision: 1,
  sequence: 1,
});

const appendAuthoritativeTaskSourceEvent = Effect.fn(
  "appendAuthoritativeImplementationTaskSourceEvent",
)(function* (
  sql: SqlClient.SqlClient,
  task: AgentControlTaskState,
  suffix: string,
  sourceSnapshot: AgentControlTaskState["sourceSnapshot"] = task.sourceSnapshot,
) {
  const eventId = EventId.make(`task-source-event-${suffix}`);
  const commandId = CommandId.make(`task-source-command-${suffix}`);
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      ${eventId}, 'task', ${task.taskId}, ${task.revision},
      'agentControl.task.created', ${task.createdAt}, ${commandId}, NULL,
      ${commandId}, 'controller', ${canonicalJson({
        taskId: task.taskId,
        source: task.source,
        status: task.status,
        sourceGate: task.sourceGate,
        stage: task.stage,
        sourceUpdatedAt: task.sourceUpdatedAt,
        githubIntakeSequence: task.githubIntakeSequence,
        sourceSnapshot,
        createdAt: task.createdAt,
      })}, ${canonicalJson({ schemaVersion: 1 })}
    ) RETURNING sequence
  `;
  return { eventId, sequence: rows[0]!.sequence, streamVersion: task.revision };
});

const seedAuthoritativeTaskProjection = Effect.fn("seedAuthoritativeImplementationTaskProjection")(
  function* (sql: SqlClient.SqlClient, task: AgentControlTaskState) {
    const stateJson = canonicalJson(task as unknown as JsonValue);
    yield* sql`
    INSERT INTO agent_control_task_states (
      task_id, project_id, repository_node_id, issue_node_id, issue_number, issue_url,
      status, source_gate, stage, source_updated_at, github_intake_sequence,
      state_json, created_at, updated_at, revision, last_event_sequence
    ) VALUES (
      ${task.taskId}, ${task.source.projectId}, ${task.source.repositoryNodeId},
      ${task.source.issueNodeId}, ${task.source.issueNumber}, ${task.source.issueUrl},
      ${task.status}, ${task.sourceGate}, ${task.stage}, ${task.sourceUpdatedAt},
      ${task.githubIntakeSequence}, ${stateJson}, ${task.createdAt}, ${task.updatedAt},
      ${task.revision}, ${task.sequence}
    )
  `;
  },
);

const seedReadyPlanningWorktree = Effect.fn("seedImplementationAdmissionReadyWorktree")(function* (
  sql: SqlClient.SqlClient,
  seeded: SeededPlanning,
  suffix: string,
) {
  const stable = {
    schemaVersion: 1 as const,
    reservationId: AgentControlWorktreeReservationId.make(seeded.evidence.worktreeReservationId),
    projectId: seeded.evidence.projectId,
    taskId: AgentControlTaskId.make(seeded.evidence.taskId),
    taskRevision: seeded.evidence.taskRevision,
    githubIntakeSequence: seeded.evidence.githubIntakeSequence,
    sourceIdentityFingerprint: seeded.evidence.sourceIdentityFingerprint,
    stageRunId: seeded.stageRunId,
    attemptId: seeded.attemptId,
    leaseId: seeded.leaseId,
    fenceToken: seeded.evidence.fenceToken,
    repository: {
      repositoryNodeId: `repository-${suffix}`,
      nameWithOwner: `owner/${suffix}`,
      canonicalKey: `repository-${suffix}`,
      remoteName: "origin",
      remoteUrl: `https://example.test/owner/${suffix}.git`,
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: 1,
      commonDirInode: 2,
    },
    repositoryWorkspace: `/tmp/repository-${suffix}`,
    repositoryCommonDir: `/tmp/repository-${suffix}/.git`,
    baseRef: "refs/remotes/origin/main",
    baseCommitSha: "a".repeat(40),
    branchName: `t3auto/issue-1-${suffix.replace(/[^a-z0-9]+/g, "-")}`,
    internalWorktreePath: seeded.evidence.worktreePath,
    targetGenerationId: "b".repeat(64),
    worktreeRootDevice: 1,
    worktreeRootInode: 2,
    worktreeParentDevice: 1,
    worktreeParentInode: 3,
    reservedAt: createdAt,
    createdAt,
    updatedAt: createdAt,
  } as const;
  const reservedEventId = EventId.make(`worktree-reserved-${suffix}`);
  const materializingEventId = EventId.make(`worktree-materializing-${suffix}`);
  const readyEventId = EventId.make(`worktree-ready-${suffix}`);
  const metadata = canonicalJson({ schemaVersion: 1 });
  const eventRows = yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO agent_control_worktree_stream_catalog (
          reservation_id, project_id, task_id, stage_run_id, attempt_id, lease_id,
          fence_token, created_at, initial_event_id, initial_stream_version
        ) VALUES (
          ${stable.reservationId}, ${stable.projectId}, ${stable.taskId}, ${stable.stageRunId},
          ${stable.attemptId}, ${stable.leaseId}, ${stable.fenceToken}, ${createdAt},
          ${reservedEventId}, 1
        )
      `;
      const events = [
        {
          eventId: reservedEventId,
          type: "agentControl.worktree.reserved",
          commandId: CommandId.make(`worktree-reserve-${suffix}`),
          streamVersion: 1,
          payload: {
            reservationId: stable.reservationId,
            projectId: stable.projectId,
            taskId: stable.taskId,
            taskRevision: stable.taskRevision,
            githubIntakeSequence: stable.githubIntakeSequence,
            sourceIdentityFingerprint: stable.sourceIdentityFingerprint,
            stageRunId: stable.stageRunId,
            attemptId: stable.attemptId,
            leaseId: stable.leaseId,
            fenceToken: stable.fenceToken,
            repository: stable.repository,
            repositoryWorkspace: stable.repositoryWorkspace,
            repositoryCommonDir: stable.repositoryCommonDir,
            baseRef: stable.baseRef,
            baseCommitSha: stable.baseCommitSha,
            branchName: stable.branchName,
            internalWorktreePath: stable.internalWorktreePath,
            targetGenerationId: stable.targetGenerationId,
            worktreeRootDevice: stable.worktreeRootDevice,
            worktreeRootInode: stable.worktreeRootInode,
            worktreeParentDevice: stable.worktreeParentDevice,
            worktreeParentInode: stable.worktreeParentInode,
            reservedAt: stable.reservedAt,
          },
        },
        {
          eventId: materializingEventId,
          type: "agentControl.worktree.materializationStarted",
          commandId: CommandId.make(`worktree-materialize-${suffix}`),
          streamVersion: 2,
          payload: {
            reservationId: stable.reservationId,
            projectId: stable.projectId,
            taskId: stable.taskId,
            stageRunId: stable.stageRunId,
            attemptId: stable.attemptId,
            leaseId: stable.leaseId,
            fenceToken: stable.fenceToken,
            transitionedAt: createdAt,
          },
        },
        {
          eventId: readyEventId,
          type: "agentControl.worktree.ready",
          commandId: CommandId.make(`worktree-ready-${suffix}`),
          streamVersion: 3,
          payload: {
            reservationId: stable.reservationId,
            projectId: stable.projectId,
            taskId: stable.taskId,
            stageRunId: stable.stageRunId,
            attemptId: stable.attemptId,
            leaseId: stable.leaseId,
            fenceToken: stable.fenceToken,
            transitionedAt: createdAt,
            headCommitSha: stable.baseCommitSha,
            ownershipFingerprint: "d".repeat(64),
            gitCreatedDevice: 1,
            gitCreatedInode: 4,
            gitCreatedGitDir: `/tmp/repository-${suffix}/.git/worktrees/${suffix}`,
            markedOwnershipFingerprint: "d".repeat(64),
            verifiedAt: createdAt,
            targetClaimCloseEvidence: {
              pendingToken: `pending-${suffix}`,
              claimAttemptId: `claim-${suffix}`,
              expectedRevision: 2,
              resultingRevision: 3,
              targetGeneration: stable.targetGenerationId,
              compositeCommandId: CommandId.make(`worktree-composite-${suffix}`),
              compositeOperation: "reserve-and-materialize",
              compositeFingerprint: "e".repeat(64),
              reservationId: stable.reservationId,
              phase: "materialized",
            },
          },
        },
      ] as const;
      for (const event of events) {
        yield* sql`
          INSERT INTO agent_control_worktree_event_envelopes (
            event_id, reservation_id, stream_version, event_type, project_id, task_id,
            stage_run_id, attempt_id, lease_id, fence_token, created_at
          ) VALUES (
            ${event.eventId}, ${stable.reservationId}, ${event.streamVersion}, ${event.type},
            ${stable.projectId}, ${stable.taskId}, ${stable.stageRunId}, ${stable.attemptId},
            ${stable.leaseId}, ${stable.fenceToken}, ${createdAt}
          )
        `;
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            ${event.eventId}, 'worktree-reservation', ${stable.reservationId},
            ${event.streamVersion}, ${event.type}, ${createdAt}, ${event.commandId}, NULL,
            ${event.commandId}, 'controller', ${canonicalJson(event.payload)}, ${metadata}
          )
        `;
      }
      return yield* sql<{ readonly sequence: number }>`
        SELECT sequence FROM agent_control_events WHERE event_id = ${readyEventId}
      `;
    }),
  );
  const state = {
    ...stable,
    materializationPhase: "ownership-marked" as const,
    gitCreatedDevice: 1,
    gitCreatedInode: 4,
    gitCreatedGitDir: `/tmp/repository-${suffix}/.git/worktrees/${suffix}`,
    markedOwnershipFingerprint: "d".repeat(64),
    headCommitSha: stable.baseCommitSha,
    ownershipFingerprint: "d".repeat(64),
    verifiedAt: createdAt,
    status: "ready" as const,
    attentionCode: null,
    revision: 3,
    sequence: eventRows[0]!.sequence,
  } satisfies AgentControlWorktreeReservationState;
  yield* sql`
      INSERT INTO agent_control_worktree_reservation_states (
        reservation_id, project_id, task_id, task_revision, github_intake_sequence,
        source_identity_fingerprint, stage_run_id, attempt_id, lease_id, fence_token,
        repository_node_id, repository_name_with_owner, repository_canonical_key,
        repository_remote_name, repository_remote_url, repository_default_remote_ref,
        repository_common_dir_device, repository_common_dir_inode,
        repository_workspace, repository_common_dir, base_ref, base_commit_sha,
        branch_name, internal_worktree_path, target_generation_id,
        worktree_root_device, worktree_root_inode, worktree_parent_device,
        worktree_parent_inode, materialization_phase, git_created_device,
        git_created_inode, git_created_git_dir, marked_ownership_fingerprint,
        head_commit_sha, ownership_fingerprint, verified_at, status, attention_code,
        state_json, revision, last_event_sequence, created_at, updated_at
      ) VALUES (
        ${state.reservationId}, ${state.projectId}, ${state.taskId}, ${state.taskRevision},
        ${state.githubIntakeSequence}, ${state.sourceIdentityFingerprint}, ${state.stageRunId},
        ${state.attemptId}, ${state.leaseId}, ${state.fenceToken},
        ${state.repository.repositoryNodeId}, ${state.repository.nameWithOwner},
        ${state.repository.canonicalKey}, ${state.repository.remoteName},
        ${state.repository.remoteUrl}, ${state.repository.defaultRemoteRef},
        ${state.repository.commonDirDevice}, ${state.repository.commonDirInode},
        ${state.repositoryWorkspace}, ${state.repositoryCommonDir}, ${state.baseRef},
        ${state.baseCommitSha}, ${state.branchName}, ${state.internalWorktreePath},
        ${state.targetGenerationId}, ${state.worktreeRootDevice}, ${state.worktreeRootInode},
        ${state.worktreeParentDevice}, ${state.worktreeParentInode},
        ${state.materializationPhase}, ${state.gitCreatedDevice}, ${state.gitCreatedInode},
        ${state.gitCreatedGitDir}, ${state.markedOwnershipFingerprint}, ${state.headCommitSha},
        ${state.ownershipFingerprint}, ${state.verifiedAt}, ${state.status}, NULL,
        ${canonicalJson(state as unknown as JsonValue)}, ${state.revision}, ${state.sequence},
        ${state.createdAt}, ${state.updatedAt}
      )
    `;
  return state;
});

const seedBoundPlanningReservation = Effect.fn("seedImplementationAdmissionPlanningReservation")(
  function* (
    sql: SqlClient.SqlClient,
    harness: AdmissionHarness,
    seeded: SeededPlanning,
    suffix: string,
  ) {
    const stable = {
      projectId: seeded.evidence.projectId,
      taskId: AgentControlTaskId.make(seeded.evidence.taskId),
      taskRevision: seeded.evidence.taskRevision,
      githubIntakeSequence: seeded.evidence.githubIntakeSequence,
      sourceIdentityFingerprint: seeded.evidence.sourceIdentityFingerprint,
      stageRunId: seeded.stageRunId,
      attemptId: seeded.attemptId,
      roleId: AgentControlRoleId.make("planning"),
      stageKind: "planning" as const,
      stageOrdinal: 1,
      attemptOrdinal: 1,
      leaseId: seeded.leaseId,
      fenceToken: seeded.evidence.fenceToken,
      worktreeReservationId: AgentControlWorktreeReservationId.make(
        seeded.evidence.worktreeReservationId,
      ),
    };
    const reservationId = yield* deriveAgentControlControlledThreadReservationId(stable);
    const threadId = yield* deriveAgentControlReservedThreadId(stable);
    assert.equal(reservationId, seeded.evidence.controlledThreadReservationId);
    assert.equal(threadId, seeded.evidence.threadId);
    const preparedCommand = {
      type: "agentControl.controlledThreadReservation.prepare" as const,
      commandId: CommandId.make(`reservation-prepare-${suffix}`),
      authority: "controller" as const,
      controlledThreadReservationId: reservationId,
      threadId,
      ...stable,
      expectedRevision: 0 as const,
    };
    const preparedDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state: null,
      command: preparedCommand,
      eventId: EventId.make(`reservation-prepared-event-${suffix}`),
      occurredAt: createdAt,
    }))[0]!;
    const [preparedEvent] = yield* harness.reservationEvents.append({
      controlledThreadReservationId: reservationId,
      expectedStreamVersion: 0,
      events: [preparedDraft],
    });
    yield* harness.reservationProjection.projectEvent(preparedEvent!);
    let state = yield* projectAgentControlControlledThreadReservationEvent(null, preparedEvent!);
    // The materialization/coordinator predecessor is already exhaustively covered
    // by its own production tests. This admission fixture keeps the actual event
    // store, projector and authoritative-history decoder, while bypassing only
    // those predecessor companion-table triggers for the two historical rows.
    yield* sql`DROP TRIGGER IF EXISTS agent_control_controlled_thread_catalog_stable_validate`;
    yield* sql`DROP TRIGGER IF EXISTS agent_control_controlled_thread_projection_validate_update`;
    yield* sql`DROP TRIGGER IF EXISTS agent_control_controlled_thread_projection_validate_update_json`;

    const coordinatorCommandId = seeded.evidence.coordinatorCommandId;
    const materializingTransitionCommandId =
      yield* deriveAgentControlMaterializingTransitionCommandId(
        coordinatorCommandId,
        reservationId,
      );
    const materializationCommandId = yield* deriveAgentControlThreadMaterializationCommandId(
      coordinatorCommandId,
      reservationId,
    );
    assert.equal(materializationCommandId, seeded.evidence.materializationCommandId);
    const materializingAt = "2026-08-02T08:00:30.000Z";
    const beginCommand = {
      ...preparedCommand,
      type: "agentControl.controlledThreadReservation.beginMaterialization" as const,
      commandId: materializingTransitionCommandId,
      expectedRevision: 1 as const,
      coordinatorCommandId,
      coordinatorCommandFingerprint: seeded.evidence.coordinatorCommandFingerprint,
      materializingTransitionCommandId,
      materializationCommandId,
      materializationCommandFingerprint: seeded.evidence.materializationCommandFingerprint,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make(seeded.evidence.leaseHolderId),
      materializingAt,
    };
    const materializingDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state,
      command: beginCommand,
      eventId: EventId.make(`reservation-materializing-event-${suffix}`),
      occurredAt: materializingAt,
    }))[0]!;
    const [materializingEvent] = yield* harness.reservationEvents.append({
      controlledThreadReservationId: reservationId,
      expectedStreamVersion: 1,
      events: [materializingDraft],
    });
    yield* harness.reservationProjection.projectEvent(materializingEvent!);
    state = yield* projectAgentControlControlledThreadReservationEvent(state, materializingEvent!);

    const boundTransitionCommandId = yield* deriveAgentControlBoundTransitionCommandId(
      coordinatorCommandId,
      reservationId,
    );
    const boundAt = "2026-08-02T08:00:40.000Z";
    const boundCommand = {
      ...beginCommand,
      type: "agentControl.controlledThreadReservation.bindMaterialization" as const,
      commandId: boundTransitionCommandId,
      expectedRevision: 2 as const,
      boundTransitionCommandId,
      orchestrationResultSequence: 1,
      materializedAt: boundAt,
      boundAt,
    };
    const boundDraft = (yield* decideAgentControlControlledThreadReservationCommand({
      state,
      command: boundCommand,
      eventId: EventId.make(`reservation-bound-event-${suffix}`),
      occurredAt: boundAt,
    }))[0]!;
    const [boundEvent] = yield* harness.reservationEvents.append({
      controlledThreadReservationId: reservationId,
      expectedStreamVersion: 2,
      events: [boundDraft],
    });
    yield* harness.reservationProjection.projectEvent(boundEvent!);
    return yield* projectAgentControlControlledThreadReservationEvent(state, boundEvent!);
  },
);

const appendOrchestration = Effect.fn("appendInitialPlanningOrchestrationEvidence")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly suffix: string;
    readonly threadId: ThreadId;
    readonly type: "thread.session-set" | "thread.proposed-plan-upserted";
    readonly occurredAt: string;
    readonly payload: JsonValue;
    readonly metadata?: JsonValue;
  },
) {
  const versionRows = yield* sql<{ readonly version: number | null }>`
    SELECT MAX(stream_version) AS version FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${input.threadId}
  `;
  const streamVersion =
    versionRows[0]?.version === null || versionRows[0]?.version === undefined
      ? 0
      : versionRows[0].version + 1;
  const eventId = EventId.make(`provider-event-${input.suffix}-${streamVersion}`);
  const commandId = CommandId.make(`provider:${eventId}:${input.type}`);
  const payloadJson = canonicalJson(input.payload);
  const metadataJson = canonicalJson(input.metadata ?? {});
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_kind, payload_json, metadata_json
    ) VALUES (
      ${eventId}, 'thread', ${input.threadId}, ${streamVersion}, ${input.type},
      ${input.occurredAt}, ${commandId}, NULL, ${commandId}, 'provider',
      ${payloadJson}, ${metadataJson}
    ) RETURNING sequence
  `;
  return { eventId, sequence: rows[0]!.sequence, streamVersion };
});

const appendProviderStart = (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence" | "providerTurnId">,
  suffix: string,
) =>
  appendOrchestration(sql, {
    suffix: `${suffix}-start`,
    threadId: seeded.evidence.threadId,
    type: "thread.session-set",
    occurredAt: providerAcceptedAt,
    payload: {
      session: {
        activeTurnId: seeded.providerTurnId,
        lastError: null,
        providerInstanceId: seeded.evidence.providerInstanceId,
        providerName: "codex",
        runtimeMode: seeded.evidence.runtimeMode,
        status: "running",
        threadId: seeded.evidence.threadId,
        updatedAt: providerAcceptedAt,
      },
      threadId: seeded.evidence.threadId,
    },
  });

const appendProviderTerminal = (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence">,
  suffix: string,
  state: "completed" | "failed" | "interrupted",
  options?: {
    readonly lastError?: string | null;
    readonly providerName?: string;
    readonly status?: "ready" | "error";
  },
) =>
  appendOrchestration(sql, {
    suffix: `${suffix}-terminal`,
    threadId: seeded.evidence.threadId,
    type: "thread.session-set",
    occurredAt: terminalAt,
    payload: {
      session: {
        activeTurnId: null,
        lastError: options?.lastError ?? (state === "failed" ? "provider failed" : null),
        providerInstanceId: seeded.evidence.providerInstanceId,
        providerName: options?.providerName ?? "codex",
        runtimeMode: seeded.evidence.runtimeMode,
        status: options?.status ?? (state === "failed" ? "error" : "ready"),
        threadId: seeded.evidence.threadId,
        updatedAt: terminalAt,
      },
      threadId: seeded.evidence.threadId,
    },
  });

const appendPlan = Effect.fn("appendInitialPlanningPlanEvidence")(function* (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence" | "providerTurnId" | "planId">,
  suffix: string,
  options?: {
    readonly planMarkdown?: string;
    readonly planId?: string;
    readonly turnId?: string;
    readonly project?: boolean;
  },
) {
  const proposedPlan = {
    id: options?.planId ?? seeded.planId,
    turnId: options?.turnId ?? seeded.providerTurnId,
    planMarkdown: options?.planMarkdown ?? "# Proposed Plan\n\n1. Finalize Planning.",
    implementedAt: null,
    implementationThreadId: null,
    createdAt: providerAcceptedAt,
    updatedAt: providerAcceptedAt,
  } as const;
  const event = yield* appendOrchestration(sql, {
    suffix: `${suffix}-plan`,
    threadId: seeded.evidence.threadId,
    type: "thread.proposed-plan-upserted",
    occurredAt: providerAcceptedAt,
    payload: { proposedPlan, threadId: seeded.evidence.threadId },
    metadata: { providerTurnId: seeded.providerTurnId },
  });
  if (options?.project !== false) {
    yield* sql`
      INSERT INTO projection_thread_proposed_plans (
        plan_id, thread_id, turn_id, plan_markdown, created_at, updated_at,
        implemented_at, implementation_thread_id
      ) VALUES (
        ${proposedPlan.id}, ${seeded.evidence.threadId}, ${proposedPlan.turnId},
        ${proposedPlan.planMarkdown}, ${proposedPlan.createdAt}, ${proposedPlan.updatedAt},
        NULL, NULL
      )
      ON CONFLICT(plan_id) DO UPDATE SET
        thread_id = excluded.thread_id, turn_id = excluded.turn_id,
        plan_markdown = excluded.plan_markdown, updated_at = excluded.updated_at
    `;
  }
  return event;
});

const seedPlanning = Effect.fn("seedInitialPlanningFinalization")(function* (
  sql: SqlClient.SqlClient,
  harness: FinalizerHarness,
  suffix: string,
  deliveryState: DeliveryState = "provider-started",
  identityMismatch?: IdentityMismatch,
  options?: {
    readonly sourceIdentityFingerprint?: string;
    readonly authoritativeReservation?: boolean;
    readonly leaseHolderId?: AgentControlStageRunLeaseHolderId;
    readonly taskId?: AgentControlTaskId;
  },
) {
  const projectId = ProjectId.make(`project-${suffix}`);
  const taskId = options?.taskId ?? AgentControlTaskId.make(`task-${suffix}`);
  const sourceIdentityFingerprint = options?.sourceIdentityFingerprint ?? "3".repeat(64);
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
  const leaseHolderId =
    options?.leaseHolderId ?? AgentControlStageRunLeaseHolderId.make(`holder-${suffix}`);
  const worktreeReservationId = yield* deriveAgentControlWorktreeReservationId({
    projectId,
    taskId,
    stageRunId,
    attemptId,
    leaseId,
    fenceToken: 1,
    repositoryIdentity: {
      repositoryNodeId: `repository-${suffix}`,
      canonicalKey: `repository-${suffix}`,
    },
    baseCommitSha: "a".repeat(40),
  });
  const worktreePathKeys = deriveAgentControlWorktreePathKeys({
    projectId,
    reservationId: worktreeReservationId,
    targetGenerationId: "b".repeat(64),
  });
  const planningReservationIdentity = {
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageRunId,
    attemptId,
    roleId: AgentControlRoleId.make("planning"),
    stageKind: "planning" as const,
    stageOrdinal: 1,
    attemptOrdinal: 1,
  };
  const reservationId = options?.authoritativeReservation
    ? yield* deriveAgentControlControlledThreadReservationId(planningReservationIdentity)
    : AgentControlControlledThreadReservationId.make(`reservation-${suffix}`);
  const threadId = options?.authoritativeReservation
    ? yield* deriveAgentControlReservedThreadId(planningReservationIdentity)
    : ThreadId.make(`thread-${suffix}`);
  const providerTurnId = TurnId.make(`turn-${suffix}`);
  const canonicalHandoffId = yield* deriveAgentControlInitialPlanningHandoffId(
    reservationId,
    threadId,
  );
  const evidenceThreadId =
    identityMismatch === "thread" ? ThreadId.make(`foreign-thread-${suffix}`) : threadId;
  const handoffId =
    identityMismatch === "handoff" ? `foreign-handoff-${suffix}` : canonicalHandoffId;
  const turnRequestCommandId =
    yield* deriveAgentControlInitialPlanningTurnRequestCommandId(handoffId);
  const messageId = yield* deriveAgentControlInitialPlanningMessageId(handoffId);
  const canonicalProviderDeliveryId =
    yield* deriveAgentControlInitialPlanningProviderDeliveryId(handoffId);
  const providerDeliveryId =
    identityMismatch === "delivery"
      ? `foreign-provider-delivery-${suffix}`
      : canonicalProviderDeliveryId;
  const messageEventId =
    yield* deriveAgentControlInitialPlanningMessageEventId(turnRequestCommandId);
  const turnRequestEventId =
    yield* deriveAgentControlInitialPlanningTurnRequestEventId(turnRequestCommandId);
  const modelSelection = {
    instanceId: ProviderInstanceId.make(`provider-${suffix}`),
    model: "gpt-5.6",
    options: [{ id: "reasoningEffort", value: "high" }],
  } as const;
  const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);
  const promptText = "Produce exactly one proposed plan.";
  const coordinatorCommandId = CommandId.make(`coordinator-${suffix}`);
  const materializationCommandId = options?.authoritativeReservation
    ? yield* deriveAgentControlThreadMaterializationCommandId(coordinatorCommandId, reservationId)
    : CommandId.make(`materialization-${suffix}`);
  const messageTemplate = canonicalInitialPlanningEventTemplate({
    streamVersion: 3,
    eventId: messageEventId,
    aggregateKind: "thread",
    aggregateId: evidenceThreadId,
    type: "thread.message-sent",
    occurredAt: createdAt,
    commandId: turnRequestCommandId,
    causationEventId: null,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: initialPlanningMessagePayload({
      threadId: evidenceThreadId,
      messageId,
      promptText,
      createdAt,
    }),
    metadata: {},
  });
  const turnTemplate = canonicalInitialPlanningEventTemplate({
    streamVersion: 4,
    eventId: turnRequestEventId,
    aggregateKind: "thread",
    aggregateId: evidenceThreadId,
    type: "thread.turn-start-requested",
    occurredAt: createdAt,
    commandId: turnRequestCommandId,
    causationEventId: messageEventId,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: initialPlanningTurnRequestPayload({
      threadId: evidenceThreadId,
      messageId,
      modelSelection,
      runtimeMode: "approval-required",
      createdAt,
    }),
    metadata: {},
  });
  const base = {
    handoffId,
    coordinatorCommandId,
    coordinatorCommandFingerprint: fixtureFingerprint(`coordinator-${suffix}`),
    materializationCommandId,
    materializationCommandFingerprint: fixtureFingerprint(`materialization-${suffix}`),
    projectId,
    controlledThreadReservationId:
      identityMismatch === "reservation"
        ? AgentControlControlledThreadReservationId.make(`foreign-reservation-${suffix}`)
        : reservationId,
    threadId: evidenceThreadId,
    taskId:
      identityMismatch === "task" ? AgentControlTaskId.make(`foreign-task-${suffix}`) : taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageRunId:
      identityMismatch === "stageRun"
        ? AgentControlStageRunId.make(`foreign-stage-${suffix}`)
        : stageRunId,
    attemptId:
      identityMismatch === "attempt"
        ? AgentControlAttemptId.make(`foreign-attempt-${suffix}`)
        : attemptId,
    roleId: "planning",
    stageKind: "planning",
    stageOrdinal: 1,
    attemptOrdinal: 1,
    leaseId:
      identityMismatch === "lease"
        ? AgentControlStageRunLeaseId.make(`foreign-lease-${suffix}`)
        : leaseId,
    leaseHolderId:
      identityMismatch === "holder"
        ? AgentControlStageRunLeaseHolderId.make(`foreign-holder-${suffix}`)
        : leaseHolderId,
    fenceToken: identityMismatch === "fence" ? 2 : 1,
    worktreeReservationId,
    worktreePath: `/tmp/t3-initial-planning-finalizer-${suffix}/${worktreePathKeys.reservationKey}-${worktreePathKeys.generationKey}`,
    planningRole: "planner",
    providerInstanceId: modelSelection.instanceId,
    runtimeMode: "approval-required",
    modelSelectionJson: modelEvidence.modelSelectionJson,
    templateVersion: "agent-control-initial-planning-prompt-v1",
    promptText,
    turnRequestCommandId,
    messageId,
    messageEventId,
    turnRequestEventId,
    providerDeliveryId,
  } as const;
  const evidence = {
    ...base,
    handoffFingerprint: fingerprintAgentControlInitialPlanningHandoff(base),
    modelSelection,
    messageEventTemplateJson: messageTemplate,
    turnRequestEventTemplateJson: turnTemplate,
    eventTemplateDigest: combinedInitialPlanningEventDigest(messageTemplate, turnTemplate),
    createdAt,
    planningDeadlineAt: deadlineAt,
  } satisfies AgentControlInitialPlanningHandoffEvidence;

  const preparedDraft: AgentControlStageRunEventDraft = {
    eventId: EventId.make(`stage-prepared-${suffix}`),
    type: "agentControl.stageRun.prepared",
    aggregateKind: "stage-run",
    aggregateId: stageRunId,
    occurredAt: createdAt,
    commandId: CommandId.make(`stage-prepare-${suffix}`),
    causationEventId: null,
    correlationId: CommandId.make(`stage-prepare-${suffix}`),
    authority: "controller",
    payload: {
      projectId,
      taskId,
      stageRunId,
      attemptId,
      roleId: AgentControlRoleId.make("planning"),
      stageKind: "planning",
      stageOrdinal: 1,
      attemptOrdinal: 1,
      status: "prepared",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      preparedAt: createdAt,
    },
    metadata: { schemaVersion: 1 },
  };
  const [prepared] = yield* harness.stageEvents.append({
    stageRunId,
    expectedStreamVersion: 0,
    events: [preparedDraft],
  });
  yield* harness.stageProjection.projectEvent(prepared!);
  const reservedDraft: AgentControlStageRunLeaseEventDraft = {
    eventId: EventId.make(`lease-reserved-${suffix}`),
    type: "agentControl.stageRunLease.reserved",
    aggregateKind: "stage-run-lease",
    aggregateId: leaseId,
    occurredAt: createdAt,
    commandId: CommandId.make(`lease-reserve-${suffix}`),
    causationEventId: null,
    correlationId: CommandId.make(`lease-reserve-${suffix}`),
    authority: "controller",
    payload: {
      leaseId,
      projectId,
      taskId,
      stageRunId,
      attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      holderId: leaseHolderId,
      fenceToken: 1,
      acquiredAt: createdAt,
      renewedAt: createdAt,
      expiresAt,
    },
    metadata: { schemaVersion: 1 },
  };
  const [reserved] = yield* harness.leaseEvents.append({
    leaseId,
    expectedStreamVersion: 0,
    events: [reservedDraft],
  });
  yield* harness.leaseProjection.projectEvent(reserved!);

  // Only the already-covered upstream materialization family is bypassed.
  // The handoff store, delivery CAS chain, stage/lease stores, projections,
  // finalization transaction, and replay path remain their production layers.
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_handoff_intent_validate`;
  yield* sql.withTransaction(harness.store.insertAcceptedInTransaction(evidence));
  yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_turn_accepted_validate`;
  const acceptedRows = yield* sql<{ readonly handoffId: string }>`
    SELECT handoff_id AS "handoffId"
    FROM agent_control_initial_planning_turn_accepted
  `;
  const messageEventSequence = acceptedRows.length * 2 + 1;
  const turnRequestEventSequence = messageEventSequence + 1;
  yield* sql`
    INSERT INTO agent_control_initial_planning_turn_accepted (
      handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
      thread_id, turn_request_command_id, message_id, message_event_id,
      message_event_sequence, turn_request_event_id, turn_request_event_sequence,
      message_event_envelope_json, turn_request_event_envelope_json,
      event_evidence_digest, receipt_authority, accepted_at
    ) VALUES (
      ${handoffId}, ${evidence.handoffFingerprint}, ${evidence.controlledThreadReservationId},
      ${evidence.threadId},
      ${turnRequestCommandId}, ${messageId}, ${messageEventId},
      CAST(${messageEventSequence} AS INTEGER), ${turnRequestEventId},
      CAST(${turnRequestEventSequence} AS INTEGER), ${messageTemplate}, ${turnTemplate},
      ${fixtureFingerprint(`event-evidence-${suffix}`)}, 'agent-control', ${createdAt}
    )
  `;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* harness.store.markTurnAccepted(handoffId, 0, createdAt);
  const claim = Option.getOrThrow(
    yield* harness.store.claim({
      handoffId,
      ownerId: `owner-${suffix}`,
      now: createdAt,
      expiresAt,
    }),
  );
  yield* harness.store.markDeliveryAttempted({
    providerDeliveryId,
    handoffId,
    ownerId: `owner-${suffix}`,
    claimGeneration: claim.delivery.claimGeneration,
    expectedRevision: claim.delivery.revision,
    attemptedAt: createdAt,
    providerSessionCreatedAt: createdAt,
    providerResumeCursorJson: "null",
    providerInstanceId: modelSelection.instanceId,
    turnModelSelectionJson: modelEvidence.modelSelectionJson,
    turnModelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
  });
  const started = yield* harness.store.markProviderStarted({
    handoffId,
    ownerId: `owner-${suffix}`,
    claimGeneration: claim.delivery.claimGeneration,
    expectedRevision: claim.delivery.revision + 1,
    providerTurnId,
    acceptedAt: providerAcceptedAt,
  });
  if (deliveryState === "ambiguous") {
    yield* harness.store.markAmbiguous({
      handoffId,
      expectedRevision: started.revision,
      terminalAt,
    });
  } else if (deliveryState !== "provider-started") {
    yield* harness.store.markTerminal({
      handoffId,
      expectedRevision: started.revision,
      state: deliveryState,
      terminalAt,
      errorCode: deliveryState === "failed" ? "provider-defect" : null,
    });
  }
  return {
    evidence,
    providerTurnId,
    planId: `plan:${evidence.threadId}:turn:${providerTurnId}`,
    stageRunId,
    attemptId,
    leaseId,
  } satisfies SeededPlanning;
});

const markTerminal = (
  store: AgentControlInitialPlanningHandoffStore["Service"],
  seeded: SeededPlanning,
  state: "completed" | "failed" | "interrupted",
) =>
  store.markTerminal({
    handoffId: seeded.evidence.handoffId,
    expectedRevision: 4,
    state,
    terminalAt,
    errorCode: state === "failed" ? "provider-defect" : null,
  });

const prepareImplementationAdmissionCandidate = Effect.fn(
  "prepareImplementationAdmissionCandidate",
)(function* (
  database: SharedDatabase,
  finalizerHarness: FinalizerHarness,
  suffix: string,
  hooks: AgentControlImplementationAdmissionHooksShape = noopAdmissionHooks,
  authoritativeSourceSnapshot: AgentControlTaskState["sourceSnapshot"] | undefined = undefined,
  taskSourceSnapshot: AgentControlTaskState["sourceSnapshot"] | undefined = undefined,
  initialTaskOverride: AgentControlTaskState | undefined = undefined,
  completePlanningParents = false,
) {
  const initialTask = initialTaskOverride ?? admissionTask(suffix);
  const taskSnapshot = taskSourceSnapshot ?? initialTask.sourceSnapshot;
  const taskSourceEvent = yield* appendAuthoritativeTaskSourceEvent(
    database.sqlA,
    initialTask,
    suffix,
    authoritativeSourceSnapshot ?? initialTask.sourceSnapshot,
  );
  const task = { ...initialTask, sourceSnapshot: taskSnapshot, sequence: taskSourceEvent.sequence };
  yield* seedAuthoritativeTaskProjection(database.sqlA, {
    ...task,
    sourceSnapshot: authoritativeSourceSnapshot ?? task.sourceSnapshot,
  });
  const sourceIdentityFingerprint = yield* deriveAgentControlSourceIdentityFingerprint(task);
  const seeded = yield* seedPlanning(
    database.sqlA,
    finalizerHarness,
    suffix,
    "provider-started",
    undefined,
    {
      sourceIdentityFingerprint,
      authoritativeReservation: true,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make("runtime-holder"),
      taskId: task.taskId,
    },
  );
  if (completePlanningParents) {
    yield* seedLegacyPlanningParents(database.sqlA, seeded, stableFixtureOrdinal(suffix));
    yield* seedLegacyPlanningTurnParents(database.sqlA, seeded);
  }
  const worktree = yield* seedReadyPlanningWorktree(database.sqlA, seeded, suffix);
  const admissionHarness = yield* buildAdmission(
    database.sqlA,
    database.scopeA,
    finalizerHarness,
    task,
    worktree,
    hooks,
  );
  const bound = yield* seedBoundPlanningReservation(
    database.sqlA,
    admissionHarness,
    seeded,
    suffix,
  );
  assert.equal(bound.status, "bound");
  yield* appendProviderStart(database.sqlA, seeded, suffix);
  assert.equal(
    (yield* finalizerHarness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
    "Started",
  );
  yield* appendPlan(database.sqlA, seeded, suffix);
  yield* appendProviderTerminal(database.sqlA, seeded, suffix, "completed");
  yield* markTerminal(finalizerHarness.store, seeded, "completed");
  assert.equal(
    (yield* finalizerHarness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
    "Finalized",
  );
  yield* Ref.set(finalizerHarness.stagePublished, []);
  yield* Ref.set(finalizerHarness.leasePublished, []);
  return { seeded, task, worktree, admissionHarness };
});

const prepareNonSucceededAdmissionCandidate = Effect.fn(
  "prepareNonSucceededImplementationAdmissionCandidate",
)(function* (
  database: SharedDatabase,
  finalizerHarness: FinalizerHarness,
  suffix: string,
  outcome: "failed" | "cancelled" | "ambiguous" | "running",
) {
  const task = admissionTask(suffix);
  const sourceIdentityFingerprint = yield* deriveAgentControlSourceIdentityFingerprint(task);
  const deliveryState =
    outcome === "failed"
      ? "failed"
      : outcome === "cancelled"
        ? "interrupted"
        : outcome === "ambiguous"
          ? "ambiguous"
          : "provider-started";
  const seeded = yield* seedPlanning(
    database.sqlA,
    finalizerHarness,
    suffix,
    deliveryState,
    undefined,
    {
      sourceIdentityFingerprint,
      authoritativeReservation: true,
      leaseHolderId: AgentControlStageRunLeaseHolderId.make("runtime-holder"),
    },
  );
  const worktree = yield* seedReadyPlanningWorktree(database.sqlA, seeded, suffix);
  const admissionHarness = yield* buildAdmission(
    database.sqlA,
    database.scopeA,
    finalizerHarness,
    task,
    worktree,
    noopAdmissionHooks,
  );
  yield* seedBoundPlanningReservation(database.sqlA, admissionHarness, seeded, suffix);
  yield* appendProviderStart(database.sqlA, seeded, suffix);
  if (outcome !== "running") {
    yield* appendProviderTerminal(
      database.sqlA,
      seeded,
      suffix,
      outcome === "failed" ? "failed" : "interrupted",
    );
  }
  return { seeded, admissionHarness };
});

interface ImplementationCoordinatorHarness {
  readonly coordinator: AgentControlImplementationTurnCoordinatorShape;
  readonly handoffStore: AgentControlImplementationHandoffStore["Service"];
  readonly orchestration: OrchestrationEngineService["Service"];
  readonly snapshots: ProjectionSnapshotQuery["Service"];
  readonly wakeup: AgentControlImplementationTurnWakeupShape;
}

const seedPlanningSourceProjection = (sql: SqlClient.SqlClient, seeded: SeededPlanning) => sql`
  INSERT INTO projection_threads (
    thread_id, project_id, title, model_selection_json, runtime_mode,
    interaction_mode, branch, worktree_path, agent_control_json,
    latest_turn_id, created_at, updated_at, archived_at,
    latest_user_message_at, pending_approval_count,
    pending_user_input_count, has_actionable_proposed_plan, deleted_at
  ) VALUES (
    ${seeded.evidence.threadId}, ${seeded.evidence.projectId}, 'Planning source',
    ${seeded.evidence.modelSelectionJson}, 'approval-required', 'plan',
    ${seeded.evidence.worktreePath}, ${seeded.evidence.worktreePath}, NULL, NULL,
    ${createdAt}, ${providerAcceptedAt}, NULL, NULL, 0, 0, 1, NULL
  )
`;

const buildImplementationCoordinator = Effect.fn("buildImplementationCoordinatorHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly suffix: string;
    readonly admission: AgentControlImplementationAdmissionShape;
    readonly finalizer: FinalizerHarness;
    readonly admissionHarness: AdmissionHarness;
    readonly task: AgentControlTaskState;
    readonly worktree: AgentControlWorktreeReservationState;
    readonly hooks?: AgentControlImplementationTurnCoordinatorHooksShape;
  }) {
    const sqlLayer = Layer.succeed(SqlClient.SqlClient, input.sql);
    const build = <I, E>(layer: Layer.Layer<I, E, never>) =>
      Layer.buildWithScope(layer, input.scope);
    yield* input.sql`
      INSERT OR IGNORE INTO projection_projects (
        project_id, title, workspace_root, default_model_selection_json,
        scripts_json, created_at, updated_at, deleted_at
      ) VALUES (
        ${input.task.source.projectId}, ${`Project ${input.suffix}`},
        ${input.worktree.repositoryWorkspace}, NULL, '[]', ${createdAt}, ${createdAt}, NULL
      )
    `;
    const receiptLayer = OrchestrationCommandReceiptRepositoryLive;
    const snapshotContext = yield* build(
      Layer.fresh(OrchestrationProjectionSnapshotQueryLive).pipe(
        Layer.provide(sqlLayer),
        Layer.provide(RepositoryIdentityResolver.layer),
        Layer.provide(NodeServices.layer),
      ),
    );
    const snapshots = Context.get(snapshotContext, ProjectionSnapshotQuery);
    const orchestrationContext = yield* build(
      Layer.fresh(
        Layer.mergeAll(
          OrchestrationEngineLive.pipe(
            Layer.provide(Layer.succeed(ProjectionSnapshotQuery, snapshots)),
            Layer.provide(OrchestrationProjectionPipelineLive),
            Layer.provide(receiptLayer),
          ),
          Layer.succeed(ProjectionSnapshotQuery, snapshots),
          receiptLayer,
        ).pipe(
          Layer.provide(OrchestrationEventStoreLive),
          Layer.provide(RepositoryIdentityResolver.layer),
          Layer.provide(receiptLayer),
          Layer.provideMerge(sqlLayer),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), {
              prefix: `t3-implementation-coordinator-${input.suffix}-`,
            }),
          ),
          Layer.provideMerge(NodeServices.layer),
        ),
      ),
    );
    const orchestration = Context.get(orchestrationContext, OrchestrationEngineService);
    const modelSelection = {
      instanceId: ProviderInstanceId.make("implementation-test-provider"),
      model: "gpt-5.6",
      options: [{ id: "reasoning-effort", value: "high" }],
    } as const;
    const policy = AgentControlPolicyService.of({
      getPolicy: () => Effect.die("unused"),
      setProjectPolicy: () => Effect.die("unused"),
      clearProjectPolicy: () => Effect.die("unused"),
      preflightPolicy: () => Effect.die("unused"),
      preflightRuntime: () =>
        Effect.succeed({
          ok: true,
          staticPreflight: {
            ok: true,
            roles: [
              {
                role: "implementer",
                accessMode: "full-access",
                strict: true,
                validCandidates: [
                  { selection: modelSelection, source: "role-route", driverKind: null },
                ],
              },
            ],
          },
          roles: [
            {
              role: "implementer",
              accessMode: "full-access",
              strict: true,
              candidates: [
                {
                  candidateIndex: 0,
                  source: "role-route",
                  providerInstanceId: modelSelection.instanceId,
                  model: modelSelection.model,
                  driverKind: null,
                  providerStatus: "ready",
                  authStatus: "authenticated",
                  checkedAt: createdAt,
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
    const taskGuard = AgentControlTaskConsumerGuard.of({
      inspectProject: () => Effect.die("unused"),
      useTaskConsumable: (_projectId, _taskId, use) => use(input.task, {} as never),
      useTaskConsumableInTransaction: (_projectId, _taskId, use) => use(input.task, {} as never),
    });
    const worktreeController = AgentControlWorktreeController.of({
      reserveAndMaterialize: () => Effect.die("unused"),
      reconcile: () => Effect.die("unused"),
      useReadyWorktree: (_identity, use, options) =>
        Effect.gen(function* () {
          if (options?.beforeInspection !== undefined) {
            const replay = yield* options.beforeInspection;
            if (Option.isSome(replay)) return replay.value;
          }
          return yield* Effect.scoped(use(input.worktree));
        }),
    });
    const worktreeEngine = AgentControlWorktreeEngine.of({
      dispatchController: () => Effect.die("unused"),
      loadAuthoritative: () => Effect.succeed(input.worktree),
      rebuild: Effect.void,
      streamDomainEvents: Stream.never,
      subscribeDomainEvents: Effect.succeed(Stream.never),
    });
    const reservationPublished = yield* Ref.make<
      ReadonlyArray<AgentControlControlledThreadReservationEvent>
    >([]);
    const reservationEngine = AgentControlControlledThreadReservationEngine.of({
      dispatchPreparedController: () => Effect.die("unused"),
      replayReceiptFirst: () => Effect.succeed(Option.none()),
      validateAcceptedReplayEvidence: ({ controlledThreadReservationId }) =>
        Effect.gen(function* () {
          const history = yield* input.admissionHarness.reservationEvents.readStream(
            controlledThreadReservationId,
            0,
          );
          if (history.length === 0) return yield* Effect.die("missing reservation history");
          let state = yield* projectAgentControlControlledThreadReservationEvent(null, history[0]!);
          const preparedState = state;
          for (const event of history.slice(1)) {
            state = yield* projectAgentControlControlledThreadReservationEvent(state, event);
          }
          return {
            currentState: state,
            preparedState,
            preparedEvent: history[0]!,
            history,
            result: {} as never,
          };
        }).pipe(Effect.orDie),
      getAuthoritative: () => Effect.die("unused"),
      validateTaskHistory: () => Effect.die("unused"),
      refreshCommitted: () => Effect.void,
      publishCommitted: (events) =>
        Ref.update(reservationPublished, (published) => [...published, ...events]),
      rebuild: Effect.void,
      streamDomainEvents: Stream.never,
    });
    const wakeupContext = yield* build(Layer.fresh(AgentControlImplementationTurnWakeupLive));
    const wakeup = Context.get(wakeupContext, AgentControlImplementationTurnWakeup);
    const dependencies = Layer.mergeAll(
      sqlLayer,
      Layer.succeed(AgentControlImplementationAdmission, input.admission),
      Layer.succeed(AgentControlPolicyService, policy),
      Layer.succeed(AgentControlTaskConsumerGuard, taskGuard),
      Layer.succeed(AgentControlWorktreeController, worktreeController),
      Layer.succeed(AgentControlWorktreeEngine, worktreeEngine),
      Layer.succeed(AgentControlStageRunEventStore, input.finalizer.stageEvents),
      Layer.succeed(AgentControlStageRunStateRepository, input.finalizer.stageStates),
      Layer.succeed(AgentControlStageRunLeaseEventStore, input.finalizer.leaseEvents),
      Layer.succeed(AgentControlStageRunLeaseStateRepository, input.finalizer.leaseStates),
      Layer.succeed(AgentControlStageRunLeaseEngine, input.finalizer.leaseEngine),
      Layer.succeed(
        AgentControlControlledThreadReservationEventStore,
        input.admissionHarness.reservationEvents,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationStateRepository,
        input.admissionHarness.reservationStates,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationProjection,
        input.admissionHarness.reservationProjection,
      ),
      Layer.succeed(AgentControlControlledThreadReservationEngine, reservationEngine),
      Layer.succeed(OrchestrationEngineService, orchestration),
      Layer.succeed(AgentControlImplementationTurnWakeup, wakeup),
      Layer.succeed(
        AgentControlImplementationTurnCoordinatorHooks,
        AgentControlImplementationTurnCoordinatorHooks.of(
          input.hooks ?? noopImplementationCoordinatorHooks,
        ),
      ),
    );
    const coordinatorContext = yield* build(
      Layer.fresh(AgentControlImplementationTurnCoordinatorLive).pipe(
        Layer.provide(dependencies),
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    const handoffContext = yield* build(
      Layer.fresh(AgentControlImplementationHandoffStoreLive).pipe(
        Layer.provide(sqlLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
    );
    return {
      coordinator: Context.get(coordinatorContext, AgentControlImplementationTurnCoordinator),
      handoffStore: Context.get(handoffContext, AgentControlImplementationHandoffStore),
      orchestration,
      snapshots,
      wakeup,
    } satisfies ImplementationCoordinatorHarness;
  },
);

interface ImplementationConsumerHarness {
  readonly consumer: AgentControlImplementationTurnConsumerShape;
}

const buildImplementationConsumer = Effect.fn("buildImplementationConsumerHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly coordinator: ImplementationCoordinatorHarness;
    readonly executorCalls: Ref.Ref<number>;
    readonly preparedMessages?: Ref.Ref<ReadonlyArray<string>>;
    readonly providerEvents: PubSub.PubSub<ProviderRuntimeEvent>;
    readonly responseLoss?: boolean;
    readonly hooks?: AgentControlImplementationTurnConsumerHooksShape;
    readonly store?: AgentControlImplementationHandoffStore["Service"];
  }) {
    const store = input.store ?? input.coordinator.handoffStore;
    const provider = ProviderService.of({
      startSession: () => Effect.die("unused"),
      sendTurn: () => Effect.die("unused"),
      interruptTurn: () => Effect.die("unused"),
      respondToRequest: () => Effect.die("unused"),
      respondToUserInput: () => Effect.die("unused"),
      stopSession: () => Effect.die("unused"),
      listSessions: () => Effect.succeed([]),
      getCapabilities: () => Effect.die("unused"),
      getInstanceInfo: () => Effect.die("unused"),
      rollbackConversation: () => Effect.die("unused"),
      subscribeEvents: PubSub.subscribe(input.providerEvents),
      streamEvents: Stream.fromPubSub(input.providerEvents),
    });
    const executor = ProviderTurnRequestExecutor.of({
      ensureSessionForThread: (threadId) => Effect.succeed(threadId),
      execute: () => Effect.die("unused"),
      prepareTurnDelivery: (request) =>
        Effect.gen(function* () {
          if (input.preparedMessages !== undefined) {
            yield* Ref.update(input.preparedMessages, (messages) => [
              ...messages,
              request.messageText,
            ]);
          }
          const claim = Option.getOrThrow(
            yield* store.loadAcceptedByThreadId(request.threadId).pipe(Effect.orDie),
          );
          if (request.modelSelection === undefined) {
            return yield* Effect.die(new Error("missing implementation model selection"));
          }
          const modelEvidence = canonicalProviderModelSelectionEvidence(request.modelSelection);
          yield* input.sql
            .withTransaction(input.sql`
              INSERT OR IGNORE INTO agent_control_implementation_session_evidence (
                provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                cwd, model_selection_json, model_selection_fingerprint,
                session_created_at, resume_cursor_json, recorded_at
              ) VALUES (
                ${request.providerDeliveryId!}, ${request.threadId},
                ${request.modelSelection.instanceId}, ${claim.evidence.runtimeMode},
                ${claim.evidence.worktreePath}, ${modelEvidence.modelSelectionJson},
                ${modelEvidence.modelSelectionFingerprint}, ${createdAt}, 'null',
                ${request.createdAt}
              )
            `)
            .pipe(Effect.orDie);
          return {
            input: {
              threadId: request.threadId,
              input: request.messageText,
              attachments: request.attachments ?? [],
              modelSelection: request.modelSelection,
              interactionMode: request.interactionMode ?? "default",
            },
            ...(request.providerDeliveryId === undefined
              ? {}
              : { providerDeliveryId: request.providerDeliveryId }),
            ...(request.durableDeliveryKind === undefined
              ? {}
              : { durableDeliveryKind: request.durableDeliveryKind }),
            sessionResumeCursorJson: "null",
            sessionAttestation: {
              threadId: request.threadId,
              providerInstanceId: request.modelSelection.instanceId,
              runtimeMode: claim.evidence.runtimeMode,
              cwd: claim.evidence.worktreePath,
              ...modelEvidence,
              sessionCreatedAt: createdAt,
              resumeCursor: null,
            },
            entryState: {
              adapterEntered: false,
              externalOperationStarted: false,
              adapterReturned: false,
            },
          };
        }),
      sendPreparedTurn: () => Effect.die("unused"),
      sendPreparedTurnAtPreInvokeBoundary: (prepared, boundary) =>
        Effect.gen(function* () {
          if (prepared.input.modelSelection === undefined) {
            return yield* Effect.die(new Error("missing turn model selection"));
          }
          yield* boundary.beforeDeliveryCas();
          yield* boundary
            .persistDeliveryAttempted(
              attestProviderNativeTurnConfiguration(prepared.input.modelSelection),
            )
            .pipe(Effect.orDie);
          yield* boundary.afterDeliveryCas();
          yield* Ref.update(input.executorCalls, (count) => count + 1);
          if (prepared.entryState !== undefined) {
            prepared.entryState.adapterEntered = true;
            prepared.entryState.externalOperationStarted = true;
          }
          if (input.responseLoss === true) {
            return yield* Effect.die(new Error("provider response lost after acceptance"));
          }
          if (prepared.entryState !== undefined) prepared.entryState.adapterReturned = true;
          return {
            certainty: "accepted" as const,
            result: {
              threadId: prepared.input.threadId,
              turnId: TurnId.make("implementation-provider-turn"),
            },
          };
        }),
    });
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlImplementationTurnConsumerLive).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqlClient.SqlClient, input.sql),
            Layer.succeed(AgentControlImplementationHandoffStore, store),
            Layer.succeed(AgentControlImplementationTurnWakeup, input.coordinator.wakeup),
            Layer.succeed(OrchestrationEngineService, input.coordinator.orchestration),
            Layer.succeed(ProjectionSnapshotQuery, input.coordinator.snapshots),
            Layer.succeed(ProviderService, provider),
            Layer.succeed(ProviderTurnRequestExecutor, executor),
            Layer.succeed(
              AgentControlImplementationTurnConsumerHooks,
              AgentControlImplementationTurnConsumerHooks.of(
                input.hooks ?? noopImplementationConsumerHooks,
              ),
            ),
          ),
        ),
        Layer.provideMerge(NodeServices.layer),
      ),
      input.scope,
    );
    return {
      consumer: Context.get(context, AgentControlImplementationTurnConsumer),
    } satisfies ImplementationConsumerHarness;
  },
);

const buildImplementationStageStarter = Effect.fn("buildImplementationStageStarterHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly coordinator: ImplementationCoordinatorHarness;
    readonly finalizer: FinalizerHarness;
    readonly hooks?: AgentControlImplementationStageStarterHooksShape;
  }) {
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlImplementationStageStarterLive).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqlClient.SqlClient, input.sql),
            Layer.succeed(AgentControlImplementationHandoffStore, input.coordinator.handoffStore),
            Layer.succeed(AgentControlImplementationTurnWakeup, input.coordinator.wakeup),
            Layer.succeed(AgentControlStageRunEventStore, input.finalizer.stageEvents),
            Layer.succeed(AgentControlStageRunStateRepository, input.finalizer.stageStates),
            Layer.succeed(AgentControlStageRunProjection, input.finalizer.stageProjection),
            Layer.succeed(AgentControlStageRunEngine, input.finalizer.stageEngine),
            Layer.succeed(AgentControlStageRunLeaseEventStore, input.finalizer.leaseEvents),
            Layer.succeed(AgentControlStageRunLeaseStateRepository, input.finalizer.leaseStates),
            Layer.succeed(
              AgentControlImplementationStageStarterHooks,
              AgentControlImplementationStageStarterHooks.of(
                input.hooks ?? noopImplementationStageStarterHooks,
              ),
            ),
          ),
        ),
      ),
      input.scope,
    );
    return Context.get(context, AgentControlImplementationStageStarter);
  },
);

interface ImplementationStageFinalizerHarness {
  readonly finalizer: AgentControlImplementationStageFinalizerShape;
}

const buildImplementationStageFinalizer = Effect.fn("buildImplementationStageFinalizerHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly coordinator: ImplementationCoordinatorHarness;
    readonly planningFinalizer: FinalizerHarness;
    readonly starter: AgentControlImplementationStageStarterShape;
    readonly hooks?: AgentControlImplementationStageFinalizerHooksShape;
  }) {
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlImplementationStageFinalizerLive).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqlClient.SqlClient, input.sql),
            Layer.succeed(AgentControlImplementationHandoffStore, input.coordinator.handoffStore),
            Layer.succeed(AgentControlImplementationTurnWakeup, input.coordinator.wakeup),
            Layer.succeed(OrchestrationEngineService, input.coordinator.orchestration),
            Layer.succeed(AgentControlImplementationStageStarter, input.starter),
            Layer.succeed(AgentControlStageRunEventStore, input.planningFinalizer.stageEvents),
            Layer.succeed(AgentControlStageRunStateRepository, input.planningFinalizer.stageStates),
            Layer.succeed(AgentControlStageRunProjection, input.planningFinalizer.stageProjection),
            Layer.succeed(AgentControlStageRunEngine, input.planningFinalizer.stageEngine),
            Layer.succeed(AgentControlStageRunLeaseEventStore, input.planningFinalizer.leaseEvents),
            Layer.succeed(
              AgentControlStageRunLeaseStateRepository,
              input.planningFinalizer.leaseStates,
            ),
            Layer.succeed(
              AgentControlStageRunLeaseProjection,
              input.planningFinalizer.leaseProjection,
            ),
            Layer.succeed(AgentControlStageRunLeaseEngine, input.planningFinalizer.leaseEngine),
            Layer.succeed(
              AgentControlImplementationStageFinalizerHooks,
              AgentControlImplementationStageFinalizerHooks.of(
                input.hooks ?? noopImplementationStageFinalizerHooks,
              ),
            ),
          ),
        ),
      ),
      input.scope,
    );
    return {
      finalizer: Context.get(context, AgentControlImplementationStageFinalizer),
    } satisfies ImplementationStageFinalizerHarness;
  },
);

interface VerificationAdmissionHarness {
  readonly admission: AgentControlVerificationAdmissionShape;
  readonly taskEvents: AgentControlTaskEventStore["Service"];
  readonly taskStates: AgentControlTaskStateRepository["Service"];
}

const buildVerificationAdmission = Effect.fn("buildVerificationAdmissionHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly planningFinalizer: FinalizerHarness;
    readonly implementationFinalizer: AgentControlImplementationStageFinalizerShape;
    readonly handoffStore: AgentControlImplementationHandoffStore["Service"];
    readonly admissionHarness: AdmissionHarness;
    readonly stageEngine?: AgentControlStageRunEngine["Service"];
    readonly hooks?: AgentControlVerificationAdmissionHooksShape;
  }) {
    const sqlLayer = Layer.succeed(SqlClient.SqlClient, input.sql);
    const build = <I, E>(layer: Layer.Layer<I, E, never>) =>
      Layer.buildWithScope(layer, input.scope);
    const taskEventContext = yield* build(
      Layer.fresh(AgentControlTaskEventStoreLive).pipe(Layer.provide(sqlLayer)),
    );
    const taskStateContext = yield* build(
      Layer.fresh(AgentControlTaskStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
    );
    const worktreeEventContext = yield* build(
      Layer.fresh(AgentControlWorktreeEventStoreLive).pipe(Layer.provide(sqlLayer)),
    );
    const worktreeStateContext = yield* build(
      Layer.fresh(AgentControlWorktreeStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
    );
    const dependencies = Layer.mergeAll(
      sqlLayer,
      Layer.succeed(AgentControlImplementationStageFinalizer, input.implementationFinalizer),
      Layer.succeed(AgentControlImplementationHandoffStore, input.handoffStore),
      Layer.succeed(
        AgentControlTaskEventStore,
        Context.get(taskEventContext, AgentControlTaskEventStore),
      ),
      Layer.succeed(
        AgentControlTaskStateRepository,
        Context.get(taskStateContext, AgentControlTaskStateRepository),
      ),
      Layer.succeed(
        AgentControlWorktreeEventStore,
        Context.get(worktreeEventContext, AgentControlWorktreeEventStore),
      ),
      Layer.succeed(
        AgentControlWorktreeStateRepository,
        Context.get(worktreeStateContext, AgentControlWorktreeStateRepository),
      ),
      Layer.succeed(AgentControlStageRunEventStore, input.planningFinalizer.stageEvents),
      Layer.succeed(AgentControlStageRunStateRepository, input.planningFinalizer.stageStates),
      Layer.succeed(AgentControlStageRunProjection, input.planningFinalizer.stageProjection),
      Layer.succeed(
        AgentControlStageRunEngine,
        input.stageEngine ?? input.planningFinalizer.stageEngine,
      ),
      Layer.succeed(AgentControlStageRunLeaseEventStore, input.planningFinalizer.leaseEvents),
      Layer.succeed(AgentControlStageRunLeaseStateRepository, input.planningFinalizer.leaseStates),
      Layer.succeed(AgentControlStageRunLeaseProjection, input.planningFinalizer.leaseProjection),
      Layer.succeed(AgentControlStageRunLeaseEngine, input.planningFinalizer.leaseEngine),
      Layer.succeed(
        AgentControlControlledThreadReservationEventStore,
        input.admissionHarness.reservationEvents,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationStateRepository,
        input.admissionHarness.reservationStates,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationProjection,
        input.admissionHarness.reservationProjection,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationEngine,
        input.admissionHarness.reservationEngine,
      ),
      Layer.succeed(
        AgentControlVerificationAdmissionHooks,
        AgentControlVerificationAdmissionHooks.of(input.hooks ?? noopVerificationAdmissionHooks),
      ),
    );
    const context = yield* build(
      Layer.fresh(AgentControlVerificationAdmissionLive).pipe(Layer.provide(dependencies)),
    );
    return {
      admission: Context.get(context, AgentControlVerificationAdmission),
      taskEvents: Context.get(taskEventContext, AgentControlTaskEventStore),
      taskStates: Context.get(taskStateContext, AgentControlTaskStateRepository),
    } satisfies VerificationAdmissionHarness;
  },
);

interface VerificationTurnCoordinatorHarness {
  readonly coordinator: AgentControlVerificationTurnCoordinatorShape;
  readonly handoffStore: AgentControlVerificationHandoffStore["Service"];
  readonly orchestration: OrchestrationEngineService["Service"];
  readonly snapshots: ProjectionSnapshotQuery["Service"];
  readonly wakeup: AgentControlVerificationTurnWakeupShape;
}

const buildVerificationTurnCoordinator = Effect.fn("buildVerificationTurnCoordinatorHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly admission: AgentControlVerificationAdmissionShape;
    readonly planningFinalizer: FinalizerHarness;
    readonly admissionHarness: AdmissionHarness;
    readonly task: AgentControlTaskState;
    readonly worktree: AgentControlWorktreeReservationState;
    readonly orchestration: OrchestrationEngineService["Service"];
    readonly snapshots: ProjectionSnapshotQuery["Service"];
    readonly hooks?: AgentControlVerificationTurnCoordinatorHooksShape;
  }) {
    const sqlLayer = Layer.succeed(SqlClient.SqlClient, input.sql);
    const modelSelection = {
      instanceId: ProviderInstanceId.make("verification-test-provider"),
      model: "gpt-5.6",
      options: [{ id: "reasoning-effort", value: "high" }],
    } as const;
    const policy = AgentControlPolicyService.of({
      getPolicy: () => Effect.die("unused"),
      setProjectPolicy: () => Effect.die("unused"),
      clearProjectPolicy: () => Effect.die("unused"),
      preflightPolicy: () => Effect.die("unused"),
      preflightRuntime: () =>
        Effect.succeed({
          ok: true,
          staticPreflight: {
            ok: true,
            roles: [
              {
                role: "verifier",
                accessMode: "restricted",
                strict: true,
                validCandidates: [
                  { selection: modelSelection, source: "role-route", driverKind: null },
                ],
              },
            ],
          },
          roles: [
            {
              role: "verifier",
              accessMode: "restricted",
              strict: true,
              candidates: [
                {
                  candidateIndex: 0,
                  source: "role-route",
                  providerInstanceId: modelSelection.instanceId,
                  model: modelSelection.model,
                  driverKind: null,
                  providerStatus: "ready",
                  authStatus: "authenticated",
                  checkedAt: createdAt,
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
    const taskGuard = AgentControlTaskConsumerGuard.of({
      inspectProject: () => Effect.die("unused"),
      useTaskConsumable: (_projectId, _taskId, use) => use(input.task, {} as never),
      useTaskConsumableInTransaction: (_projectId, _taskId, use) => use(input.task, {} as never),
    });
    const worktreeController = AgentControlWorktreeController.of({
      reserveAndMaterialize: () => Effect.die("unused"),
      reconcile: () => Effect.die("unused"),
      useReadyWorktree: (_identity, use, options) =>
        Effect.gen(function* () {
          if (options?.beforeInspection !== undefined) {
            const replay = yield* options.beforeInspection;
            if (Option.isSome(replay)) return replay.value;
          }
          return yield* Effect.scoped(use(input.worktree));
        }),
    });
    const worktreeEngine = AgentControlWorktreeEngine.of({
      dispatchController: () => Effect.die("unused"),
      loadAuthoritative: () => Effect.succeed(input.worktree),
      rebuild: Effect.void,
      streamDomainEvents: Stream.never,
      subscribeDomainEvents: Effect.succeed(Stream.never),
    });
    const reservationEngine = AgentControlControlledThreadReservationEngine.of({
      dispatchPreparedController: () => Effect.die("unused"),
      replayReceiptFirst: () => Effect.succeed(Option.none()),
      validateAcceptedReplayEvidence: ({ controlledThreadReservationId }) =>
        Effect.gen(function* () {
          const history = yield* input.admissionHarness.reservationEvents.readStream(
            controlledThreadReservationId,
            0,
          );
          if (history.length === 0) return yield* Effect.die("missing reservation history");
          let state = yield* projectAgentControlControlledThreadReservationEvent(null, history[0]!);
          const preparedState = state;
          for (const event of history.slice(1)) {
            state = yield* projectAgentControlControlledThreadReservationEvent(state, event);
          }
          return {
            currentState: state,
            preparedState,
            preparedEvent: history[0]!,
            history,
            result: {} as never,
          };
        }).pipe(Effect.orDie),
      getAuthoritative: () => Effect.die("unused"),
      validateTaskHistory: () => Effect.die("unused"),
      refreshCommitted: () => Effect.void,
      publishCommitted: () => Effect.void,
      rebuild: Effect.void,
      streamDomainEvents: Stream.never,
    });
    const wakeupContext = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlVerificationTurnWakeupLive),
      input.scope,
    );
    const wakeup = Context.get(wakeupContext, AgentControlVerificationTurnWakeup);
    const dependencies = Layer.mergeAll(
      sqlLayer,
      Layer.succeed(AgentControlVerificationAdmission, input.admission),
      Layer.succeed(AgentControlPolicyService, policy),
      Layer.succeed(AgentControlTaskConsumerGuard, taskGuard),
      Layer.succeed(AgentControlWorktreeController, worktreeController),
      Layer.succeed(AgentControlWorktreeEngine, worktreeEngine),
      Layer.succeed(AgentControlStageRunEventStore, input.planningFinalizer.stageEvents),
      Layer.succeed(AgentControlStageRunStateRepository, input.planningFinalizer.stageStates),
      Layer.succeed(AgentControlStageRunLeaseEventStore, input.planningFinalizer.leaseEvents),
      Layer.succeed(AgentControlStageRunLeaseStateRepository, input.planningFinalizer.leaseStates),
      Layer.succeed(AgentControlStageRunLeaseEngine, input.planningFinalizer.leaseEngine),
      Layer.succeed(
        AgentControlControlledThreadReservationEventStore,
        input.admissionHarness.reservationEvents,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationStateRepository,
        input.admissionHarness.reservationStates,
      ),
      Layer.succeed(
        AgentControlControlledThreadReservationProjection,
        input.admissionHarness.reservationProjection,
      ),
      Layer.succeed(AgentControlControlledThreadReservationEngine, reservationEngine),
      Layer.succeed(OrchestrationEngineService, input.orchestration),
      Layer.succeed(AgentControlVerificationTurnWakeup, wakeup),
      Layer.succeed(
        AgentControlVerificationTurnCoordinatorHooks,
        AgentControlVerificationTurnCoordinatorHooks.of(
          input.hooks ?? noopVerificationCoordinatorHooks,
        ),
      ),
    );
    const coordinatorContext = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlVerificationTurnCoordinatorLive).pipe(
        Layer.provide(dependencies),
        Layer.provideMerge(NodeServices.layer),
      ),
      input.scope,
    );
    const handoffContext = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
        Layer.provide(sqlLayer),
        Layer.provideMerge(NodeServices.layer),
      ),
      input.scope,
    );
    return {
      coordinator: Context.get(coordinatorContext, AgentControlVerificationTurnCoordinator),
      handoffStore: Context.get(handoffContext, AgentControlVerificationHandoffStore),
      orchestration: input.orchestration,
      snapshots: input.snapshots,
      wakeup,
    } satisfies VerificationTurnCoordinatorHarness;
  },
);

const buildVerificationTurnConsumer = Effect.fn("buildVerificationTurnConsumerHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly coordinator: VerificationTurnCoordinatorHarness;
    readonly executorCalls: Ref.Ref<number>;
    readonly responseLossDefect?: unknown;
    readonly responseLossCause?: Cause.Cause<never>;
    readonly prepareFailures?: Ref.Ref<number>;
    readonly prepareError?: ProviderServiceError;
    readonly afterDeliveryCasError?: ProviderServiceError;
    readonly providerEvents?: PubSub.PubSub<ProviderRuntimeEvent>;
    readonly providerPublications?: PubSub.PubSub<ProviderRuntimeEventPublication>;
    readonly providerService?: ProviderService["Service"];
    readonly executorService?: ProviderTurnRequestExecutor["Service"];
    readonly hooks?: AgentControlVerificationTurnConsumerHooksShape;
  }) {
    const providerEvents =
      input.providerEvents ?? (yield* PubSub.unbounded<ProviderRuntimeEvent>());
    const provider =
      input.providerService ??
      ProviderService.of({
        startSession: () => Effect.die("unused"),
        sendTurn: () => Effect.die("unused"),
        interruptTurn: () => Effect.die("unused"),
        respondToRequest: () => Effect.die("unused"),
        respondToUserInput: () => Effect.die("unused"),
        stopSession: () => Effect.die("unused"),
        listSessions: () => Effect.succeed([]),
        getCapabilities: () => Effect.die("unused"),
        getInstanceInfo: () => Effect.die("unused"),
        rollbackConversation: () => Effect.die("unused"),
        subscribeEvents: PubSub.subscribe(providerEvents),
        ...(input.providerPublications === undefined
          ? {}
          : {
              subscribeRuntimeEventPublications: PubSub.subscribe(input.providerPublications),
            }),
        streamEvents: Stream.fromPubSub(providerEvents),
      });
    const executor =
      input.executorService ??
      ProviderTurnRequestExecutor.of({
        ensureSessionForThread: (threadId) => Effect.succeed(threadId),
        execute: () => Effect.die("unused"),
        prepareTurnDelivery: (request) =>
          Effect.gen(function* () {
            if (input.prepareError !== undefined) return yield* input.prepareError;
            if (input.prepareFailures !== undefined) {
              const remaining = yield* Ref.getAndUpdate(input.prepareFailures, (count) =>
                Math.max(0, count - 1),
              );
              if (remaining > 0) {
                return yield* new ProviderAdapterRequestError({
                  provider: "verification-test-provider",
                  method: "thread.turn.start",
                  detail: "Verification test provider timeout.",
                });
              }
            }
            assert.equal(request.durableDeliveryKind, "verification");
            const claim = Option.getOrThrow(
              yield* input.coordinator.handoffStore
                .loadAcceptedByThreadId(request.threadId)
                .pipe(Effect.orDie),
            );
            if (request.modelSelection === undefined || request.providerDeliveryId === undefined) {
              return yield* Effect.die(new Error("missing verification delivery authority"));
            }
            const modelEvidence = canonicalProviderModelSelectionEvidence(request.modelSelection);
            yield* input.sql
              .withTransaction(input.sql`
              INSERT OR IGNORE INTO main.agent_control_verification_session_evidence (
                provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                cwd, model_selection_json, model_selection_fingerprint,
                session_created_at, resume_cursor_json, recorded_at
              ) VALUES (
                ${request.providerDeliveryId}, ${request.threadId},
                ${request.modelSelection.instanceId}, ${claim.evidence.runtimeMode},
                ${claim.evidence.worktreePath}, ${modelEvidence.modelSelectionJson},
                ${modelEvidence.modelSelectionFingerprint}, ${createdAt}, 'null',
                ${request.createdAt}
              )
            `)
              .pipe(Effect.orDie);
            return {
              input: {
                threadId: request.threadId,
                input: request.messageText,
                attachments: request.attachments ?? [],
                modelSelection: request.modelSelection,
                interactionMode: request.interactionMode ?? "default",
              },
              providerDeliveryId: request.providerDeliveryId,
              durableDeliveryKind: "verification" as const,
              sessionResumeCursorJson: "null",
              sessionAttestation: {
                threadId: request.threadId,
                providerInstanceId: request.modelSelection.instanceId,
                runtimeMode: claim.evidence.runtimeMode,
                cwd: claim.evidence.worktreePath,
                ...modelEvidence,
                sessionCreatedAt: createdAt,
                resumeCursor: null,
              },
              entryState: {
                adapterEntered: false,
                externalOperationStarted: false,
                adapterReturned: false,
              },
            };
          }),
        sendPreparedTurn: () => Effect.die("unused"),
        sendPreparedTurnAtPreInvokeBoundary: (prepared, boundary) =>
          Effect.gen(function* () {
            if (prepared.input.modelSelection === undefined) {
              return yield* Effect.die(new Error("missing verification model selection"));
            }
            yield* boundary.beforeDeliveryCas();
            yield* boundary
              .persistDeliveryAttempted(
                attestProviderNativeTurnConfiguration(prepared.input.modelSelection),
              )
              .pipe(Effect.orDie);
            yield* boundary.afterDeliveryCas();
            if (input.afterDeliveryCasError !== undefined) {
              return yield* new ProviderTurnDeliveryError({
                certainty: "not-attempted",
                cause: input.afterDeliveryCasError,
              });
            }
            yield* Ref.update(input.executorCalls, (count) => count + 1);
            if (prepared.entryState !== undefined) {
              prepared.entryState.adapterEntered = true;
              prepared.entryState.externalOperationStarted = true;
            }
            if (input.responseLossDefect !== undefined) {
              return yield* Effect.die(input.responseLossDefect);
            }
            if (input.responseLossCause !== undefined) {
              return yield* Effect.failCause(input.responseLossCause);
            }
            if (prepared.entryState !== undefined) prepared.entryState.adapterReturned = true;
            return {
              certainty: "accepted" as const,
              result: {
                threadId: prepared.input.threadId,
                turnId: TurnId.make("verification-provider-turn"),
              },
            };
          }),
      });
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlVerificationTurnConsumerLive).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqlClient.SqlClient, input.sql),
            Layer.succeed(AgentControlVerificationHandoffStore, input.coordinator.handoffStore),
            Layer.succeed(AgentControlVerificationTurnWakeup, input.coordinator.wakeup),
            Layer.succeed(OrchestrationEngineService, input.coordinator.orchestration),
            Layer.succeed(ProjectionSnapshotQuery, input.coordinator.snapshots),
            Layer.succeed(ProviderService, provider),
            Layer.succeed(ProviderTurnRequestExecutor, executor),
            Layer.succeed(
              AgentControlVerificationTurnConsumerHooks,
              AgentControlVerificationTurnConsumerHooks.of(
                input.hooks ?? noopVerificationConsumerHooks,
              ),
            ),
          ),
        ),
        Layer.provideMerge(NodeServices.layer),
      ),
      input.scope,
    );
    return Context.get(
      context,
      AgentControlVerificationTurnConsumer,
    ) satisfies AgentControlVerificationTurnConsumerShape;
  },
);

const prepareVerificationTurnDelivery = Effect.fn("prepareVerificationTurnDelivery")(function* (
  suffix: string,
  completePlanningParents = false,
  existing?: {
    readonly database: SharedDatabase;
    readonly planningFinalizer: FinalizerHarness;
  },
) {
  const database = existing?.database ?? (yield* makeSharedDatabase());
  const planningFinalizer =
    existing?.planningFinalizer ?? (yield* buildFinalizer(database.sqlA, database.scopeA));
  const prepared = yield* prepareSucceededImplementationFinalization(
    database,
    planningFinalizer,
    suffix,
    false,
    completePlanningParents,
  );
  const verificationAdmission = yield* buildVerificationAdmission({
    sql: database.sqlA,
    scope: database.scopeA,
    planningFinalizer,
    implementationFinalizer: prepared.setup.finalizer.finalizer,
    handoffStore: prepared.setup.coordinator.handoffStore,
    admissionHarness: prepared.setup.candidate.admissionHarness,
  });
  assert.equal(
    (yield* verificationAdmission.admission.processResultEvidence(
      prepared.implementation.resultEvidenceId,
    ))._tag,
    "Admitted",
  );
  const coordinator = yield* buildVerificationTurnCoordinator({
    sql: database.sqlA,
    scope: database.scopeA,
    admission: verificationAdmission.admission,
    planningFinalizer,
    admissionHarness: prepared.setup.candidate.admissionHarness,
    task: prepared.setup.candidate.task,
    worktree: prepared.setup.candidate.worktree,
    orchestration: prepared.setup.coordinator.orchestration,
    snapshots: prepared.setup.coordinator.snapshots,
  });
  assert.equal(
    (yield* coordinator.coordinator.processHandoff(prepared.implementation.resultEvidenceId))._tag,
    "Materialized",
  );
  const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
    SELECT handoff_id AS "handoffId" FROM agent_control_verification_handoff_accepted
    ORDER BY rowid DESC LIMIT 1
  `;
  assert.isDefined(handoff);
  return {
    database,
    planningFinalizer,
    coordinator,
    handoffId: handoff!.handoffId,
  };
});

const buildVerificationStageStarter = Effect.fn("buildVerificationStageStarterHarness")(
  function* (input: {
    readonly sql: SqlClient.SqlClient;
    readonly scope: Scope.Closeable;
    readonly coordinator: VerificationTurnCoordinatorHarness;
    readonly planningFinalizer: FinalizerHarness;
    readonly stageEvents?: AgentControlStageRunEventStoreShape;
    readonly hooks?: AgentControlVerificationStageStarterHooksShape;
  }) {
    const context = yield* Layer.buildWithScope(
      Layer.fresh(AgentControlVerificationStageStarterLive).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqlClient.SqlClient, input.sql),
            Layer.succeed(AgentControlVerificationHandoffStore, input.coordinator.handoffStore),
            Layer.succeed(AgentControlVerificationTurnWakeup, input.coordinator.wakeup),
            Layer.succeed(
              AgentControlStageRunEventStore,
              input.stageEvents ?? input.planningFinalizer.stageEvents,
            ),
            Layer.succeed(AgentControlStageRunStateRepository, input.planningFinalizer.stageStates),
            Layer.succeed(AgentControlStageRunProjection, input.planningFinalizer.stageProjection),
            Layer.succeed(AgentControlStageRunEngine, input.planningFinalizer.stageEngine),
            Layer.succeed(AgentControlStageRunLeaseEventStore, input.planningFinalizer.leaseEvents),
            Layer.succeed(
              AgentControlStageRunLeaseStateRepository,
              input.planningFinalizer.leaseStates,
            ),
            Layer.succeed(
              AgentControlVerificationStageStarterHooks,
              AgentControlVerificationStageStarterHooks.of(
                input.hooks ?? noopVerificationStageStarterHooks,
              ),
            ),
          ),
        ),
      ),
      input.scope,
    );
    return Context.get(
      context,
      AgentControlVerificationStageStarter,
    ) satisfies AgentControlVerificationStageStarterShape;
  },
);

const appendLegitimateTaskHistorySuffix = Effect.fn("appendLegitimateTaskHistorySuffix")(function* (
  harness: VerificationAdmissionHarness,
  taskId: AgentControlTaskId,
  suffix: string,
) {
  const current = Option.getOrThrow(yield* harness.taskStates.get(taskId));
  const changedAt = IsoDateTime.make("2026-08-02T08:03:00.000Z");
  const githubIntakeSequence = current.githubIntakeSequence + 1;
  const sourceSnapshot = {
    ...current.sourceSnapshot,
    updatedAt: changedAt,
    paused: true,
    eligible: false,
    eligibilityReason: "paused" as const,
  };
  const drafts = yield* decideAgentControlTaskCommand({
    state: current,
    command: {
      type: "agentControl.task.sourceGate.refresh",
      commandId: CommandId.make(`task-source-suffix-command-${suffix}`),
      taskId,
      projectId: current.source.projectId,
      expectedRevision: current.revision,
      sourcePrecondition: {
        schemaVersion: 1,
        projectId: current.source.projectId,
        githubIntakeSequence,
        githubProjectionRevision: githubIntakeSequence,
        githubConfigRevision: 1,
        repositoryNodeId: current.source.repositoryNodeId,
        pollStatus: "success",
        expectedIssueCount: 1,
      },
      source: current.source,
      sourceGate: "paused",
      sourceUpdatedAt: changedAt,
      githubIntakeSequence,
      sourceSnapshot,
    },
    eventId: EventId.make(`task-source-suffix-event-${suffix}`),
    occurredAt: changedAt,
  });
  assert.lengthOf(drafts, 1);
  const committed = yield* harness.taskEvents.append({
    taskId,
    expectedStreamVersion: current.revision,
    events: drafts,
  });
  assert.lengthOf(committed, 1);
  const next = yield* projectAgentControlTaskEvent(current, committed[0]!);
  yield* harness.taskStates.save(next, current.revision);
  return committed[0]!;
});

const prepareImplementationDeliveryRecoveryCandidates = Effect.fn(
  "prepareImplementationDeliveryRecoveryCandidates",
)(function* (
  database: SharedDatabase,
  finalizer: FinalizerHarness,
  suffixes: ReadonlyArray<string>,
  taskOverrides: ReadonlyMap<string, AgentControlTaskState> = new Map(),
  completePlanningParents = false,
) {
  const candidates = yield* Effect.forEach(suffixes, (suffix) =>
    Effect.gen(function* () {
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizer,
        suffix,
        noopAdmissionHooks,
        undefined,
        undefined,
        taskOverrides.get(suffix),
        completePlanningParents,
      );
      const admissionHandoffId = candidate.seeded.evidence.handoffId;
      assert.equal(
        (yield* candidate.admissionHarness.admission.processHandoff(admissionHandoffId))._tag,
        "Admitted",
      );
      const coordinator = yield* buildImplementationCoordinator({
        sql: database.sqlA,
        scope: database.scopeA,
        suffix,
        admission: candidate.admissionHarness.admission,
        finalizer,
        admissionHarness: candidate.admissionHarness,
        task: candidate.task,
        worktree: candidate.worktree,
      });
      assert.equal(
        (yield* coordinator.coordinator.processHandoff(admissionHandoffId))._tag,
        "Materialized",
      );
      if (!completePlanningParents) {
        yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
      }
      const rows = yield* database.sqlA<{ readonly handoffId: string }>`
        SELECT accepted.handoff_id AS "handoffId"
        FROM agent_control_implementation_handoff_accepted accepted
        JOIN agent_control_implementation_materialization_evidence materialization
          ON materialization.materialization_evidence_id = accepted.materialization_evidence_id
        WHERE materialization.admission_handoff_id = ${admissionHandoffId}
      `;
      assert.lengthOf(rows, 1);
      return { candidate, coordinator, handoffId: rows[0]!.handoffId };
    }),
  );
  return candidates.toSorted((left, right) => left.handoffId.localeCompare(right.handoffId));
});

const prepareImplementationStageFinalizationCandidate = Effect.fn(
  "prepareImplementationStageFinalizationCandidate",
)(function* (
  database: SharedDatabase,
  planningFinalizer: FinalizerHarness,
  suffix: string,
  startStage = true,
  taskOverride?: AgentControlTaskState,
  completePlanningParents = false,
) {
  const prepared = (yield* prepareImplementationDeliveryRecoveryCandidates(
    database,
    planningFinalizer,
    [suffix],
    taskOverride === undefined ? new Map() : new Map([[suffix, taskOverride]]),
    completePlanningParents,
  ))[0]!;
  const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const executorCalls = yield* Ref.make(0);
  const consumer = yield* buildImplementationConsumer({
    sql: database.sqlA,
    scope: database.scopeA,
    coordinator: prepared.coordinator,
    executorCalls,
    providerEvents,
  });
  yield* consumer.consumer.processHandoff(prepared.handoffId);
  assert.equal(yield* Ref.get(executorCalls), 1);
  const claim = Option.getOrThrow(
    yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
  );
  assert.equal(claim.delivery.state, "provider-started");
  assert.notEqual(claim.delivery.providerTurnId, null);
  yield* prepared.coordinator.orchestration.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`provider:${suffix}:implementation-start`),
    threadId: claim.evidence.threadId,
    session: {
      threadId: claim.evidence.threadId,
      status: "running",
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: claim.evidence.providerInstanceId,
      runtimeMode: claim.evidence.runtimeMode,
      activeTurnId: TurnId.make(claim.delivery.providerTurnId!),
      lastError: null,
      updatedAt: claim.delivery.providerAcceptedAt!,
    },
    createdAt: claim.delivery.providerAcceptedAt!,
  });
  const starter = yield* buildImplementationStageStarter({
    sql: database.sqlA,
    scope: database.scopeA,
    coordinator: prepared.coordinator,
    finalizer: planningFinalizer,
  });
  if (startStage) {
    assert.equal((yield* starter.processHandoff(prepared.handoffId))._tag, "Started");
  }
  const finalizer = yield* buildImplementationStageFinalizer({
    sql: database.sqlA,
    scope: database.scopeA,
    coordinator: prepared.coordinator,
    planningFinalizer,
    starter,
  });
  return { ...prepared, claim, starter, finalizer };
});

const prepareSucceededImplementationForFinalization = Effect.fn(
  "prepareSucceededImplementationForFinalization",
)(function* (
  database: SharedDatabase,
  planningFinalizer: FinalizerHarness,
  suffix: string,
  useLiveTerminalTime = false,
  completePlanningParents = false,
) {
  const initialTask = admissionTask(suffix);
  const canonicalTask = {
    ...initialTask,
    taskId: yield* deriveAgentControlTaskId(initialTask.source),
  };
  const setup = yield* prepareImplementationStageFinalizationCandidate(
    database,
    planningFinalizer,
    suffix,
    true,
    canonicalTask,
    completePlanningParents,
  );
  const claim = Option.getOrThrow(
    yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
  );
  const successfulTerminalAt = useLiveTerminalTime
    ? DateTime.formatIso(yield* DateTime.now)
    : terminalAt;
  assert.isTrue(
    Option.isSome(
      yield* setup.coordinator.handoffStore.observeProviderTerminal({
        threadId: claim.evidence.threadId,
        providerTurnId: claim.delivery.providerTurnId!,
        state: "completed",
        terminalAt: successfulTerminalAt,
      }),
    ),
  );
  yield* setup.coordinator.orchestration.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`provider:${suffix}:implementation-terminal`),
    threadId: claim.evidence.threadId,
    session: {
      threadId: claim.evidence.threadId,
      status: "ready",
      providerName: ProviderDriverKind.make("codex"),
      providerInstanceId: claim.evidence.providerInstanceId,
      runtimeMode: claim.evidence.runtimeMode,
      activeTurnId: null,
      lastError: null,
      updatedAt: successfulTerminalAt,
    },
    createdAt: successfulTerminalAt,
  });
  return { setup, claim };
});

const prepareSucceededImplementationFinalization = Effect.fn(
  "prepareSucceededImplementationFinalization",
)(function* (
  database: SharedDatabase,
  planningFinalizer: FinalizerHarness,
  suffix: string,
  useLiveTerminalTime = false,
  completePlanningParents = false,
) {
  const { setup, claim } = yield* prepareSucceededImplementationForFinalization(
    database,
    planningFinalizer,
    suffix,
    useLiveTerminalTime,
    completePlanningParents,
  );
  assert.equal(
    (yield* setup.finalizer.finalizer.processHandoff(setup.handoffId))._tag,
    "Finalized",
  );
  const [implementation] = yield* database.sqlA<{
    readonly resultEvidenceId: string;
    readonly leaseId: string;
    readonly holderId: string;
    readonly fenceToken: number;
  }>`
    SELECT result_evidence_id AS "resultEvidenceId", lease_id AS "leaseId",
      lease_holder_id AS "holderId", fence_token AS "fenceToken"
    FROM agent_control_implementation_result_evidence
    WHERE handoff_id = ${setup.handoffId}
  `;
  assert.isDefined(implementation);
  return { setup, claim, implementation: implementation! };
});

const verificationAdmissionCounts = (sql: SqlClient.SqlClient) =>
  sql<{
    readonly stageEvents: number;
    readonly leaseEvents: number;
    readonly reservationEvents: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
  }>`
    SELECT
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'stage-run'
          AND json_extract(payload_json, '$.stageKind') = 'verification') AS "stageEvents",
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease'
          AND json_extract(payload_json, '$.stageRunId') IN (
            SELECT verification_stage_run_id
            FROM agent_control_verification_admission_evidence
          )) AS "leaseEvents",
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'controlled-thread-reservation'
          AND json_extract(payload_json, '$.stageKind') = 'verification')
        AS "reservationEvents",
      (SELECT count(*) FROM agent_control_verification_admission_evidence) AS evidence,
      (SELECT count(*) FROM agent_control_verification_admission_receipts) AS receipts,
      (SELECT count(*) FROM agent_control_verification_admission_markers) AS markers
  `.pipe(Effect.map((rows) => rows[0]!));

const noVerificationAdmission = {
  stageEvents: 0,
  leaseEvents: 0,
  reservationEvents: 0,
  evidence: 0,
  receipts: 0,
  markers: 0,
} as const;

const implementationFinalizationCounts = (sql: SqlClient.SqlClient, handoffId: string) =>
  sql<{
    readonly terminalStageEvents: number;
    readonly leaseReleaseEvents: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
    readonly stageStatus: string;
    readonly stageRevision: number;
    readonly leaseStatus: string;
    readonly leaseRevision: number;
    readonly fenceToken: number;
  }>`
    SELECT
      (SELECT count(*) FROM agent_control_events event
       JOIN agent_control_implementation_deliveries delivery
         ON delivery.stage_run_id = event.stream_id
       WHERE delivery.handoff_id = ${handoffId}
         AND event.event_type IN (
           'agentControl.stageRun.implementationSucceeded',
           'agentControl.stageRun.implementationFailed',
           'agentControl.stageRun.implementationCancelled'
         )) AS "terminalStageEvents",
      (SELECT count(*) FROM agent_control_events event
       JOIN agent_control_implementation_deliveries delivery
         ON delivery.lease_id = event.stream_id
       WHERE delivery.handoff_id = ${handoffId}
         AND event.event_type = 'agentControl.stageRunLease.releasedAfterImplementation')
        AS "leaseReleaseEvents",
      (SELECT count(*) FROM agent_control_implementation_result_evidence
       WHERE handoff_id = ${handoffId}) AS evidence,
      (SELECT count(*) FROM agent_control_implementation_stage_finalization_receipts
       WHERE handoff_id = ${handoffId}) AS receipts,
      (SELECT count(*) FROM agent_control_implementation_stage_finalization_markers
       WHERE handoff_id = ${handoffId}) AS markers,
      stage.status AS "stageStatus", stage.revision AS "stageRevision",
      lease.status AS "leaseStatus", lease.revision AS "leaseRevision",
      lease.fence_token AS "fenceToken"
    FROM agent_control_implementation_deliveries delivery
    JOIN agent_control_stage_run_states stage ON stage.stage_run_id = delivery.stage_run_id
    JOIN agent_control_stage_run_lease_states lease ON lease.lease_id = delivery.lease_id
    WHERE delivery.handoff_id = ${handoffId}
  `.pipe(Effect.map((rows) => rows[0]!));

const implementationTurnRollbackTables = [
  "effect_sql_migrations",
  "agent_control_events",
  "agent_control_task_states",
  "agent_control_worktree_reservation_states",
  "agent_control_stage_run_states",
  "agent_control_stage_run_lease_states",
  "orchestration_events",
  "agent_control_implementation_admission_evidence",
  "agent_control_implementation_admission_receipts",
  "agent_control_implementation_admission_markers",
  "agent_control_implementation_materialization_evidence",
  "agent_control_implementation_materialization_receipts",
  "agent_control_implementation_materialization_markers",
  "agent_control_implementation_handoff_intents",
  "agent_control_implementation_handoff_receipts",
  "agent_control_implementation_handoff_accepted",
  "agent_control_implementation_deliveries",
  "agent_control_implementation_turn_accepted",
  "agent_control_implementation_stage_started_evidence",
  "agent_control_implementation_stage_started_receipts",
  "agent_control_implementation_stage_started_markers",
] as const;

const captureImplementationTurnRollbackState = Effect.fn("captureImplementationTurnRollbackState")(
  function* (sql: SqlClient.SqlClient) {
    const tables = yield* Effect.forEach(implementationTurnRollbackTables, (table) =>
      sql
        .unsafe<Record<string, unknown>>(`SELECT * FROM ${table} ORDER BY rowid`)
        .pipe(Effect.map((rows) => [table, rows] as const)),
    );
    return {
      schema: yield* sql<Record<string, unknown>>`
      SELECT type, name, tbl_name AS "tableName", sql
      FROM sqlite_schema ORDER BY type, name
    `,
      sequence: yield* sql<Record<string, unknown>>`
      SELECT name, seq FROM sqlite_sequence ORDER BY name
    `,
      tables,
    };
  },
);

const finalizationCounts = (sql: SqlClient.SqlClient, seeded: SeededPlanning) =>
  sql<{
    readonly stageEvents: number;
    readonly leaseEvents: number;
    readonly started: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM agent_control_events
       WHERE aggregate_kind = 'stage-run' AND stream_id = ${seeded.stageRunId}) AS "stageEvents",
      (SELECT COUNT(*) FROM agent_control_events
       WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${seeded.leaseId}) AS "leaseEvents",
      (SELECT COUNT(*) FROM agent_control_initial_planning_stage_started
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS started,
      (SELECT COUNT(*) FROM agent_control_initial_planning_result_evidence
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS evidence,
      (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_receipts
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS receipts,
      (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_markers
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS markers
  `.pipe(Effect.map((rows) => rows[0]!));

const implementationAdmissionCounts = (sql: SqlClient.SqlClient, handoffId: string) =>
  sql<{
    readonly stageEvents: number;
    readonly stageStates: number;
    readonly leaseEvents: number;
    readonly leaseStates: number;
    readonly reservationEvents: number;
    readonly reservationStates: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
  }>`
    SELECT
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'stage-run'
          AND json_extract(payload_json, '$.stageKind') = 'implementation'
          AND command_id LIKE 'implementation-admission-%') AS "stageEvents",
      (SELECT count(*) FROM agent_control_stage_run_states
        WHERE stage_kind = 'implementation') AS "stageStates",
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'stage-run-lease'
          AND json_extract(payload_json, '$.stageRunId') IN (
            SELECT implementation_stage_run_id
            FROM agent_control_implementation_admission_evidence
            WHERE handoff_id = ${handoffId}
          )) AS "leaseEvents",
      (SELECT count(*) FROM agent_control_stage_run_lease_states
        WHERE stage_run_id IN (
          SELECT implementation_stage_run_id
          FROM agent_control_implementation_admission_evidence
          WHERE handoff_id = ${handoffId}
        )) AS "leaseStates",
      (SELECT count(*) FROM agent_control_events
        WHERE aggregate_kind = 'controlled-thread-reservation'
          AND json_extract(payload_json, '$.stageKind') = 'implementation')
        AS "reservationEvents",
      (SELECT count(*) FROM agent_control_implementation_thread_reservation_states)
        AS "reservationStates",
      (SELECT count(*) FROM agent_control_implementation_admission_evidence
        WHERE handoff_id = ${handoffId}) AS evidence,
      (SELECT count(*) FROM agent_control_implementation_admission_receipts
        WHERE handoff_id = ${handoffId}) AS receipts,
      (SELECT count(*) FROM agent_control_implementation_admission_markers
        WHERE handoff_id = ${handoffId}) AS markers
  `.pipe(Effect.map((rows) => rows[0]!));

const noImplementationAdmission = {
  stageEvents: 0,
  stageStates: 0,
  leaseEvents: 0,
  leaseStates: 0,
  reservationEvents: 0,
  reservationStates: 0,
  evidence: 0,
  receipts: 0,
  markers: 0,
} as const;

const migration053HardeningTriggers = [
  "agent_control_initial_planning_stage_event_validate",
  "agent_control_initial_planning_lease_event_validate",
  "agent_control_initial_planning_lifecycle_event_no_update",
  "agent_control_initial_planning_lifecycle_event_no_delete",
  "agent_control_initial_planning_stage_started_validate",
  "agent_control_initial_planning_result_evidence_validate",
  "agent_control_initial_planning_finalization_receipt_validate",
  "agent_control_initial_planning_finalization_marker_validate",
] as const;

const prepareHistoricalMigration052 = Effect.fn("prepareHistoricalMigration052")(function* (
  sql: SqlClient.SqlClient,
) {
  for (const trigger of migration053HardeningTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${trigger}`).unprepared;
  }
  yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 53`;
});

const captureMigration053State = Effect.fn("captureMigration053State")(function* (
  sql: SqlClient.SqlClient,
) {
  return {
    migrations: yield* sql`
      SELECT migration_id, name, created_at
      FROM effect_sql_migrations
      ORDER BY migration_id
    `,
    schema: yield* sql`
      SELECT type, name, tbl_name, sql
      FROM sqlite_schema
      ORDER BY type, name
    `,
    deliveries: yield* sql`
      SELECT * FROM agent_control_initial_planning_deliveries ORDER BY handoff_id
    `,
    orchestrationEvents: yield* sql`
      SELECT *,
        typeof(payload_json) AS payload_storage,
        hex(CAST(payload_json AS BLOB)) AS payload_bytes_hex,
        typeof(metadata_json) AS metadata_storage,
        hex(CAST(metadata_json AS BLOB)) AS metadata_bytes_hex
      FROM orchestration_events
      ORDER BY stream_id, stream_version, sequence
    `,
    triggers: yield* sql`
      SELECT name, sql
      FROM sqlite_schema
      WHERE type = 'trigger'
        AND name LIKE 'agent_control_initial_planning_%'
      ORDER BY name
    `,
    stageStarted: yield* sql`
      SELECT * FROM agent_control_initial_planning_stage_started ORDER BY handoff_id
    `,
    resultEvidence: yield* sql`
      SELECT * FROM agent_control_initial_planning_result_evidence ORDER BY handoff_id
    `,
    receipts: yield* sql`
      SELECT * FROM agent_control_initial_planning_finalization_receipts ORDER BY handoff_id
    `,
    markers: yield* sql`
      SELECT * FROM agent_control_initial_planning_finalization_markers ORDER BY handoff_id
    `,
    sequences: yield* sql`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
  };
});

const seedLegacyPlanningParents = Effect.fn("seedLegacyPlanningParents")(function* (
  sql: SqlClient.SqlClient,
  seeded: SeededPlanning,
  ordinal: number,
) {
  const evidence = seeded.evidence;
  const createdEventId = `legacy-materialization-created-${ordinal}`;
  const boundEventId = `legacy-materialization-bound-${ordinal}`;
  const [latestVersion] = yield* sql<{ readonly streamVersion: number | null }>`
    SELECT MAX(stream_version) AS "streamVersion"
    FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${evidence.threadId}
  `;
  const [latestSequence] = yield* sql<{ readonly sequence: number | null }>`
    SELECT MAX(sequence) AS sequence FROM orchestration_events
  `;
  const needsPrelude = latestVersion?.streamVersion === null || latestVersion === undefined;
  const createdSequence = (latestSequence?.sequence ?? 0) + (needsPrelude ? 2 : 1);
  const boundSequence = createdSequence + 1;
  const createdStreamVersion = (latestVersion?.streamVersion ?? 0) + 1;
  const boundStreamVersion = createdStreamVersion + 1;
  const finalizationOwnerId = `00000000-0000-4000-8000-${String(ordinal).padStart(12, "0")}`;
  const bindingJson = canonicalJson({
    attemptId: evidence.attemptId,
    controlState: "controlled",
    roleId: "planning",
    stageRunId: evidence.stageRunId,
    taskId: evidence.taskId,
  });
  const parentTables = [
    "orchestration_agent_control_thread_materialization_intents",
    "orchestration_agent_control_thread_materialization_receipts",
    "agent_control_controlled_thread_materialization_intents",
    "agent_control_controlled_thread_materialization_receipts",
    "agent_control_controlled_thread_materialization_accepted",
  ] as const;
  const triggers = yield* sql<{ readonly name: string; readonly sql: string }>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND sql IS NOT NULL
      AND tbl_name IN ${sql.in(parentTables)}
    ORDER BY name
  `;
  yield* sql`PRAGMA foreign_keys = OFF`;
  for (const trigger of triggers) {
    yield* sql.unsafe(`DROP TRIGGER ${trigger.name}`).unprepared;
  }
  yield* sql.withTransaction(
    Effect.gen(function* () {
      if (needsPrelude) {
        yield* sql`
          INSERT INTO orchestration_events (
            sequence, event_id, aggregate_kind, stream_id, stream_version,
            event_type, occurred_at, command_id, causation_event_id,
            correlation_id, actor_kind, payload_json, metadata_json
          ) VALUES (
            ${createdSequence - 1}, ${`legacy-materialization-prelude-${ordinal}`},
            'thread', ${evidence.threadId}, 0, 'thread.meta-updated',
            ${evidence.createdAt}, ${`legacy-materialization-prelude-command-${ordinal}`},
            NULL, ${`legacy-materialization-prelude-command-${ordinal}`}, 'server',
            ${canonicalJson({
              threadId: evidence.threadId,
              title: "Initial planning",
              updatedAt: evidence.createdAt,
            })}, '{}'
          )
        `;
      }
      yield* sql`
        INSERT INTO orchestration_events (
          sequence, event_id, aggregate_kind, stream_id, stream_version,
          event_type, occurred_at, command_id, causation_event_id,
          correlation_id, actor_kind, payload_json, metadata_json
        ) VALUES
          (${createdSequence}, ${createdEventId}, 'thread', ${evidence.threadId},
            ${createdStreamVersion},
            'thread.created', ${evidence.createdAt}, ${evidence.materializationCommandId},
            NULL, ${evidence.materializationCommandId}, 'server',
            ${canonicalJson({
              branch: `legacy-branch-${ordinal}`,
              createdAt: evidence.createdAt,
              interactionMode: "plan",
              modelSelection: evidence.modelSelection,
              projectId: evidence.projectId,
              runtimeMode: evidence.runtimeMode,
              threadId: evidence.threadId,
              title: "Initial planning",
              updatedAt: evidence.createdAt,
              worktreePath: evidence.worktreePath,
            })}, '{}'),
          (${boundSequence}, ${boundEventId}, 'thread', ${evidence.threadId},
            ${boundStreamVersion},
            'thread.agent-control-bound', ${evidence.createdAt},
            ${evidence.materializationCommandId}, ${createdEventId},
            ${evidence.materializationCommandId}, 'server',
            ${canonicalJson({
              binding: {
                attemptId: evidence.attemptId,
                controlState: "controlled",
                roleId: "planning",
                stageRunId: evidence.stageRunId,
                taskId: evidence.taskId,
              },
              threadId: evidence.threadId,
              updatedAt: evidence.createdAt,
            })}, '{}')
      `;
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          ${evidence.materializationCommandId}, 'agent-control', 'thread',
          ${evidence.threadId}, ${evidence.createdAt}, ${boundSequence}, 'accepted', NULL
        )
      `;
      yield* sql`
        INSERT INTO orchestration_agent_control_thread_materialization_intents (
          command_id, command_type, authority, aggregate_kind, command_fingerprint,
          controlled_thread_reservation_id, thread_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
          attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
          title, model_selection_json, runtime_mode, interaction_mode, branch,
          worktree_path, binding_json, created_event_id, created_event_type,
          created_event_sequence, created_event_stream_version, binding_event_id,
          binding_event_type, binding_event_sequence, binding_event_stream_version,
          accepted_receipt_command_id, receipt_status, receipt_result_sequence,
          receipt_accepted_at, receipt_error, created_at
        ) VALUES (
          ${evidence.materializationCommandId}, 'thread.agent-control.materialize',
          'agent-control', 'thread', ${evidence.materializationCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.projectId}, ${evidence.taskId}, ${evidence.taskRevision},
          ${evidence.githubIntakeSequence}, ${evidence.sourceIdentityFingerprint},
          ${evidence.stageRunId}, ${evidence.attemptId}, 'planning', 'planning', 1, 1,
          ${evidence.leaseId}, ${evidence.fenceToken}, ${evidence.worktreeReservationId},
          'Initial planning', ${evidence.modelSelectionJson}, 'approval-required', 'plan',
          ${`legacy-branch-${ordinal}`}, ${evidence.worktreePath}, ${bindingJson},
          ${createdEventId}, 'thread.created', ${createdSequence}, ${createdStreamVersion},
          ${boundEventId}, 'thread.agent-control-bound', ${boundSequence},
          ${boundStreamVersion},
          ${evidence.materializationCommandId}, 'accepted', ${boundSequence},
          ${evidence.createdAt}, NULL, ${evidence.createdAt}
        )
      `;
      yield* sql`
        INSERT INTO orchestration_agent_control_thread_materialization_receipts (
          command_id, command_type, authority, aggregate_kind, thread_id,
          command_fingerprint, result_sequence, accepted_at, status
        ) VALUES (
          ${evidence.materializationCommandId}, 'thread.agent-control.materialize',
          'agent-control', 'thread', ${evidence.threadId},
          ${evidence.materializationCommandFingerprint}, ${boundSequence},
          ${evidence.createdAt}, 'accepted'
        )
      `;
    }),
  );
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_intents (
          coordinator_command_id, finalization_owner_id, request_fingerprint,
          coordinator_command_fingerprint, policy_binding_fingerprint,
          runtime_observation_fingerprint, project_id,
          controlled_thread_reservation_id, thread_id, task_id, task_revision,
          github_intake_sequence, source_identity_fingerprint, stage_run_id,
          attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
          lease_id, lease_holder_id, fence_token, worktree_reservation_id,
          materializing_transition_command_id, bound_transition_command_id,
          materialization_command_id, materialization_command_fingerprint,
          title, model_selection_json, runtime_mode, interaction_mode, branch,
          worktree_path, binding_json, materializing_event_id,
          materializing_event_sequence, bound_event_id, bound_event_sequence,
          orchestration_result_sequence, materializing_at, materialized_at,
          bound_at, accepted_at, accepted_marker_command_id
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${finalizationOwnerId},
          ${fixtureFingerprint(`legacy-request-${ordinal}`)},
          ${evidence.coordinatorCommandFingerprint},
          ${fixtureFingerprint(`legacy-policy-${ordinal}`)},
          ${fixtureFingerprint(`legacy-runtime-${ordinal}`)}, ${evidence.projectId},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.taskId}, ${evidence.taskRevision}, ${evidence.githubIntakeSequence},
          ${evidence.sourceIdentityFingerprint}, ${evidence.stageRunId},
          ${evidence.attemptId}, 'planning', 'planning', 1, 1, ${evidence.leaseId},
          ${evidence.leaseHolderId}, ${evidence.fenceToken},
          ${evidence.worktreeReservationId}, ${`legacy-materializing-${ordinal}`},
          ${`legacy-bound-${ordinal}`}, ${evidence.materializationCommandId},
          ${evidence.materializationCommandFingerprint}, 'Initial planning',
          ${evidence.modelSelectionJson}, 'approval-required', 'plan',
          ${`legacy-branch-${ordinal}`}, ${evidence.worktreePath}, ${bindingJson},
          ${`legacy-materializing-event-${ordinal}`}, 1,
          ${`legacy-bound-event-${ordinal}`}, 2, ${boundSequence},
          ${evidence.createdAt}, ${evidence.createdAt}, ${evidence.createdAt},
          ${evidence.createdAt}, ${evidence.coordinatorCommandId}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_receipts (
          coordinator_command_id, request_fingerprint,
          coordinator_command_fingerprint, controlled_thread_reservation_id,
          thread_id, materialization_command_id,
          materialization_command_fingerprint, orchestration_result_sequence,
          status, accepted_at, accepted_marker_command_id
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${fixtureFingerprint(`legacy-request-${ordinal}`)},
          ${evidence.coordinatorCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.materializationCommandId}, ${evidence.materializationCommandFingerprint},
          ${boundSequence}, 'accepted', ${evidence.createdAt},
          ${evidence.coordinatorCommandId}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_accepted (
          coordinator_command_id, finalization_owner_id,
          coordinator_command_fingerprint, controlled_thread_reservation_id,
          thread_id, materialization_command_id,
          materialization_command_fingerprint, orchestration_result_sequence,
          accepted_at
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${finalizationOwnerId},
          ${evidence.coordinatorCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.materializationCommandId}, ${evidence.materializationCommandFingerprint},
          ${boundSequence}, ${evidence.createdAt}
        )
      `;
    }),
  );
  for (const trigger of triggers) {
    yield* sql.unsafe(trigger.sql).unprepared;
  }
  yield* sql`PRAGMA foreign_keys = ON`;
});

const seedLegacyPlanningTurnParents = Effect.fn("seedLegacyPlanningTurnParents")(function* (
  sql: SqlClient.SqlClient,
  seeded: SeededPlanning,
) {
  const evidence = seeded.evidence;
  const [latestSequence] = yield* sql<{ readonly sequence: number | null }>`
    SELECT MAX(sequence) AS sequence FROM orchestration_events
  `;
  const messageSequence = (latestSequence?.sequence ?? 0) + 1;
  const turnSequence = messageSequence + 1;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO orchestration_events (
          sequence, event_id, aggregate_kind, stream_id, stream_version,
          event_type, occurred_at, command_id, causation_event_id,
          correlation_id, actor_kind, payload_json, metadata_json
        ) VALUES
          (${messageSequence}, ${evidence.messageEventId}, 'thread', ${evidence.threadId}, 3,
            'thread.message-sent', ${evidence.createdAt}, ${evidence.turnRequestCommandId},
            NULL, ${evidence.turnRequestCommandId}, 'client',
            ${canonicalJson(
              initialPlanningMessagePayload({
                threadId: evidence.threadId,
                messageId: evidence.messageId,
                promptText: evidence.promptText,
                createdAt: evidence.createdAt,
              }),
            )}, '{}'),
          (${turnSequence}, ${evidence.turnRequestEventId}, 'thread', ${evidence.threadId}, 4,
            'thread.turn-start-requested', ${evidence.createdAt},
            ${evidence.turnRequestCommandId}, ${evidence.messageEventId},
            ${evidence.turnRequestCommandId}, 'client',
            ${canonicalJson(
              initialPlanningTurnRequestPayload({
                threadId: evidence.threadId,
                messageId: evidence.messageId,
                modelSelection: evidence.modelSelection,
                runtimeMode: evidence.runtimeMode,
                createdAt: evidence.createdAt,
              }),
            )}, '{}')
      `;
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          ${evidence.turnRequestCommandId}, 'agent-control', 'thread',
          ${evidence.threadId}, ${evidence.createdAt}, ${turnSequence}, 'accepted', NULL
        )
      `;
    }),
  );
});

const makeLegacyFinalizations = Effect.fn("makeLegacyFinalizations")(function* (
  suffixes: ReadonlyArray<string>,
) {
  const database = yield* makeSharedDatabase(53);
  const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
  const seeded: SeededPlanning[] = [];
  for (const [index, suffix] of suffixes.entries()) {
    const candidate = yield* seedPlanning(database.sqlA, harness, suffix);
    seeded.push(candidate);
    yield* seedLegacyPlanningParents(database.sqlA, candidate, index + 1);
    yield* appendProviderStart(database.sqlA, candidate, suffix);
    assert.equal(
      (yield* harness.finalizer.processHandoff(candidate.evidence.handoffId))._tag,
      "Started",
    );
    yield* appendPlan(database.sqlA, candidate, suffix);
    yield* appendProviderTerminal(database.sqlA, candidate, suffix, "completed");
    yield* markTerminal(harness.store, candidate, "completed");
    assert.equal(
      (yield* harness.finalizer.processHandoff(candidate.evidence.handoffId))._tag,
      "Finalized",
    );
  }
  yield* database.sqlA.unsafe("DROP TRIGGER agent_control_initial_planning_turn_accepted_no_delete")
    .unprepared;
  yield* database.sqlA.withTransaction(
    database.sqlA`DELETE FROM agent_control_initial_planning_turn_accepted`,
  );
  yield* database.sqlA.unsafe(
    `CREATE TRIGGER agent_control_initial_planning_turn_accepted_no_delete
     BEFORE DELETE ON agent_control_initial_planning_turn_accepted
     BEGIN SELECT RAISE(ABORT, 'initial planning turn acceptance is immutable'); END`,
  ).unprepared;
  assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
  yield* prepareHistoricalMigration052(database.sqlA);
  return { database, seeded };
});

const makeLegacyFinalization = Effect.fn("makeLegacyFinalization")(function* (suffix: string) {
  const fixture = yield* makeLegacyFinalizations([suffix]);
  return { database: fixture.database, seeded: fixture.seeded[0]! };
});

const makeRecoverableMigration052Planning = Effect.fn("makeRecoverableMigration052Planning")(
  function* (suffix: string) {
    const database = yield* makeSharedDatabase(52);
    const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
    const seeded = yield* seedPlanning(database.sqlA, harness, suffix);
    yield* appendProviderStart(database.sqlA, seeded, suffix);
    assert.equal(
      (yield* harness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
      "Started",
    );
    yield* seedLegacyPlanningParents(database.sqlA, seeded, 1);
    yield* database.sqlA.unsafe(
      "DROP TRIGGER agent_control_initial_planning_turn_accepted_no_delete",
    ).unprepared;
    yield* database.sqlA.withTransaction(database.sqlA`
    DELETE FROM agent_control_initial_planning_turn_accepted
    WHERE handoff_id = ${seeded.evidence.handoffId}
  `);
    yield* database.sqlA.unsafe(
      `CREATE TRIGGER agent_control_initial_planning_turn_accepted_no_delete
     BEFORE DELETE ON agent_control_initial_planning_turn_accepted
     BEGIN SELECT RAISE(ABORT, 'initial planning turn acceptance is immutable'); END`,
    ).unprepared;
    assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
      stageEvents: 2,
      leaseEvents: 1,
      started: 1,
      evidence: 0,
      receipts: 0,
      markers: 0,
    });
    assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
    return { database, harness, seeded };
  },
);

const makeRecoverableMigration052CompletedPlanning = Effect.fn(
  "makeRecoverableMigration052CompletedPlanning",
)(function* (suffix: string, planMarkdown?: string) {
  const fixture = yield* makeRecoverableMigration052Planning(suffix);
  const plan = yield* appendPlan(
    fixture.database.sqlA,
    fixture.seeded,
    suffix,
    planMarkdown === undefined ? undefined : { planMarkdown },
  );
  const terminal = yield* appendProviderTerminal(
    fixture.database.sqlA,
    fixture.seeded,
    suffix,
    "completed",
  );
  yield* markTerminal(fixture.harness.store, fixture.seeded, "completed");
  return { ...fixture, plan, terminal };
});

type LegacyMutation =
  | "stage-project"
  | "stage-task"
  | "stage-thread"
  | "stage-provider"
  | "stage-provider-turn"
  | "stage-run"
  | "stage-attempt"
  | "stage-lease"
  | "stage-holder"
  | "stage-fence"
  | "stage-sequence"
  | "stage-revision"
  | "result-outcome"
  | "result-plan-digest"
  | "result-stage-coordinate"
  | "result-lease-coordinate"
  | "result-storage-class"
  | "result-noncanonical-json"
  | "receipt-command"
  | "receipt-fingerprint"
  | "receipt-handoff"
  | "receipt-outcome"
  | "receipt-coordinate"
  | "marker-incomplete"
  | "foreign-key";

const mutateLegacyEvidence = Effect.fn("mutateLegacyInitialPlanningEvidence")(function* (
  sql: SqlClient.SqlClient,
  handoffId: string,
  mutation: LegacyMutation,
  original: {
    readonly stage: Record<string, unknown>;
    readonly result: Record<string, unknown>;
    readonly receipt: Record<string, unknown>;
    readonly marker: Record<string, unknown>;
  },
  restore = false,
) {
  const table =
    mutation.startsWith("stage-") || mutation === "foreign-key"
      ? "agent_control_initial_planning_stage_started"
      : mutation.startsWith("result-")
        ? "agent_control_initial_planning_result_evidence"
        : mutation.startsWith("receipt-")
          ? "agent_control_initial_planning_finalization_receipts"
          : "agent_control_initial_planning_finalization_markers";
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql`PRAGMA ignore_check_constraints = ON`;
  yield* sql.unsafe(`DROP TRIGGER ${table}_no_update`).unprepared;
  switch (mutation) {
    case "stage-project":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET project_id = ${restore ? original.stage.project_id : "legacy-foreign-project"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-task":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET task_id = ${restore ? original.stage.task_id : "legacy-foreign-task"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-thread":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET thread_id = ${restore ? original.stage.thread_id : "legacy-foreign-thread"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-provider":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET provider_instance_id = ${
          restore ? original.stage.provider_instance_id : "legacy-foreign-provider"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-provider-turn":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET provider_turn_id = ${restore ? original.stage.provider_turn_id : "legacy-foreign-turn"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-run":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_run_id = ${restore ? original.stage.stage_run_id : "legacy-foreign-stage"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-attempt":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET attempt_id = ${restore ? original.stage.attempt_id : "legacy-foreign-attempt"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-lease":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET lease_id = ${restore ? original.stage.lease_id : "legacy-foreign-lease"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-holder":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET lease_holder_id = ${restore ? original.stage.lease_holder_id : "legacy-foreign-holder"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-fence":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET fence_token = ${restore ? original.stage.fence_token : 999}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-sequence":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_event_sequence = ${restore ? original.stage.stage_event_sequence : 900001}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-revision":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_event_stream_version = ${restore ? original.stage.stage_event_stream_version : 9}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-outcome":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET outcome = ${restore ? original.result.outcome : "failed"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-plan-digest":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET proposed_plan_digest = ${
          restore ? original.result.proposed_plan_digest : "f".repeat(64)
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-stage-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET stage_event_sequence = ${restore ? original.result.stage_event_sequence : 900002}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-lease-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET lease_event_sequence = ${restore ? original.result.lease_event_sequence : 900003}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-storage-class":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET project_id = ${
          restore
            ? original.result.project_id
            : new TextEncoder().encode(String(original.result.project_id))
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-noncanonical-json":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET proposed_plan_json = ${
          restore
            ? original.result.proposed_plan_json
            : ` ${String(original.result.proposed_plan_json)}`
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-command":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET finalization_command_id = ${
          restore ? original.receipt.finalization_command_id : "legacy-foreign-command"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-fingerprint":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET finalization_fingerprint = ${
          restore ? original.receipt.finalization_fingerprint : "e".repeat(64)
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-handoff":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET handoff_id = ${restore ? original.receipt.handoff_id : "legacy-foreign-handoff"}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "receipt-outcome":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET outcome = ${restore ? original.receipt.outcome : "failed"}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "receipt-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET stage_event_sequence = ${restore ? original.receipt.stage_event_sequence : 900004}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "marker-incomplete":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET finalization_command_id = ${
          restore ? original.marker.finalization_command_id : "legacy-missing-command"
        }
        WHERE result_evidence_id = ${original.marker.result_evidence_id}
      `);
      break;
    case "foreign-key":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET orchestration_started_event_id = ${
          restore ? original.stage.orchestration_started_event_id : "legacy-missing-event"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
  }
  yield* sql.unsafe(
    `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
     BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
  ).unprepared;
  yield* sql`PRAGMA ignore_check_constraints = OFF`;
  yield* sql`PRAGMA foreign_keys = ON`;
});

const swapLegacyMarkerEvidence = Effect.fn("swapLegacyMarkerEvidence")(function* (
  sql: SqlClient.SqlClient,
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  restore = false,
) {
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql.unsafe("DROP TRIGGER agent_control_initial_planning_finalization_markers_no_update")
    .unprepared;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = 'legacy-marker-swap-temporary'
        WHERE marker_id = ${left.marker_id}
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = ${restore ? right.result_evidence_id : left.result_evidence_id}
        WHERE marker_id = ${right.marker_id}
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = ${restore ? left.result_evidence_id : right.result_evidence_id}
        WHERE marker_id = ${left.marker_id}
      `;
    }),
  );
  yield* sql.unsafe(
    `CREATE TRIGGER agent_control_initial_planning_finalization_markers_no_update
     BEFORE UPDATE ON agent_control_initial_planning_finalization_markers
     BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
  ).unprepared;
  yield* sql`PRAGMA foreign_keys = ON`;
});

const withNode = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer));

it.effect("materializes one admitted Implementation thread and replays the complete boundary", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          "implementation-materialization-production",
        );
        const handoffId = candidate.seeded.evidence.handoffId;
        assert.equal(
          (yield* candidate.admissionHarness.admission.processHandoff(handoffId))._tag,
          "Admitted",
        );
        const harness = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix: "implementation-materialization-production",
          admission: candidate.admissionHarness.admission,
          finalizer,
          admissionHarness: candidate.admissionHarness,
          task: candidate.task,
          worktree: candidate.worktree,
        });

        assert.equal((yield* harness.coordinator.processHandoff(handoffId))._tag, "Materialized");
        assert.equal((yield* harness.coordinator.processHandoff(handoffId))._tag, "Replayed");
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence)
                AS materializationEvidence,
              (SELECT count(*) FROM agent_control_implementation_materialization_receipts)
                AS materializationReceipts,
              (SELECT count(*) FROM agent_control_implementation_materialization_markers)
                AS materializationMarkers,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted)
                AS acceptedHandoffs,
              (SELECT count(*) FROM agent_control_implementation_deliveries)
                AS deliveries,
              (SELECT count(*) FROM orchestration_events event
               JOIN agent_control_implementation_materialization_evidence evidence
                 ON evidence.thread_id = event.stream_id
               WHERE event.event_type IN ('thread.created','thread.agent-control-bound'))
                AS materializationEvents,
              (SELECT count(*) FROM agent_control_events event
               JOIN agent_control_implementation_admission_evidence admission
                 ON admission.implementation_controlled_thread_reservation_id = event.stream_id
               WHERE event.aggregate_kind = 'controlled-thread-reservation')
                AS reservationEvents
          `,
          [
            {
              materializationEvidence: 1,
              materializationReceipts: 1,
              materializationMarkers: 1,
              acceptedHandoffs: 1,
              deliveries: 1,
              materializationEvents: 2,
              reservationEvents: 3,
            },
          ],
        );
        const implementationHandoffId = (yield* database.sqlA<{
          readonly handoffId: string;
        }>`
          SELECT handoff_id AS "handoffId"
          FROM agent_control_implementation_handoff_accepted
        `)[0]!.handoffId;
        const claim = Option.getOrThrow(
          yield* harness.handoffStore.loadAcceptedByHandoffId(implementationHandoffId),
        );
        assert.equal(claim.evidence.planningThreadId, candidate.seeded.evidence.threadId);
        assert.equal(claim.evidence.planId, candidate.seeded.planId);
        assert.equal(claim.evidence.proposedPlanDigest.length, 64);
        assert.equal(claim.evidence.runtimeMode, "full-access");
      }),
    ),
  ),
);

it.effect("rejects task prompt data that diverges from authoritative task history", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          "implementation-task-history-red",
          noopAdmissionHooks,
          {
            ...admissionTask("implementation-task-history-red").sourceSnapshot,
            title: "Authoritative task title",
            body: "Authoritative task body",
          },
        );
        const handoffId = candidate.seeded.evidence.handoffId;
        assert.equal(
          (yield* candidate.admissionHarness.admission.processHandoff(handoffId))._tag,
          "Admitted",
        );
        const harness = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix: "implementation-task-history-red",
          admission: candidate.admissionHarness.admission,
          finalizer,
          admissionHarness: candidate.admissionHarness,
          task: candidate.task,
          worktree: candidate.worktree,
        });

        const exit = yield* Effect.exit(harness.coordinator.processHandoff(handoffId));
        assert.isTrue(Exit.isFailure(exit));
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted) AS handoffs,
              (SELECT count(*) FROM agent_control_implementation_deliveries) AS deliveries
          `,
          [{ evidence: 0, handoffs: 0, deliveries: 0 }],
        );
      }),
    ),
  ),
);

it.effect("preserves an authoritative Unicode task title and allowed empty body", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const suffix = "implementation-task-history-unicode-empty";
        const sourceSnapshot = {
          ...admissionTask(suffix).sourceSnapshot,
          title: "Grüße aus Köln 👩🏽‍💻 東京",
          body: "",
        };
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          suffix,
          noopAdmissionHooks,
          sourceSnapshot,
          sourceSnapshot,
        );
        const admissionHandoffId = candidate.seeded.evidence.handoffId;
        assert.equal(
          (yield* candidate.admissionHarness.admission.processHandoff(admissionHandoffId))._tag,
          "Admitted",
        );
        const harness = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix,
          admission: candidate.admissionHarness.admission,
          finalizer,
          admissionHarness: candidate.admissionHarness,
          task: candidate.task,
          worktree: candidate.worktree,
        });
        assert.equal(
          (yield* harness.coordinator.processHandoff(admissionHandoffId))._tag,
          "Materialized",
        );
        const implementationHandoffId = (yield* database.sqlA<{
          readonly handoffId: string;
        }>`SELECT handoff_id AS "handoffId"
           FROM agent_control_implementation_handoff_accepted`)[0]!.handoffId;
        const claim = Option.getOrThrow(
          yield* harness.handoffStore.loadAcceptedByHandoffId(implementationHandoffId),
        );
        assert.include(claim.evidence.promptText, "Grüße aus Köln 👩🏽‍💻 東京");
        assert.include(claim.evidence.promptText, '"taskBody":""');
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_task_states
          SET revision = revision + 1,
            last_event_sequence = last_event_sequence + 1,
            state_json = json_set(
              state_json,
              '$.revision', revision + 1,
              '$.sequence', last_event_sequence + 1
            )
          WHERE task_id = ${candidate.task.taskId}
        `);
        assert.equal(
          (yield* harness.coordinator.processHandoff(admissionHandoffId))._tag,
          "Replayed",
        );
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted) AS handoffs,
              (SELECT count(*) FROM agent_control_implementation_deliveries) AS deliveries
          `,
          [{ evidence: 1, handoffs: 1, deliveries: 1 }],
        );
      }),
    ),
  ),
);

it.effect.each<{
  readonly field: "task_title" | "task_body" | "repository_display" | "source_revision";
}>([
  { field: "task_title" },
  { field: "task_body" },
  { field: "repository_display" },
  { field: "source_revision" },
])(
  "migration 055 rejects a materialization with non-authoritative $field atomically",
  ({ field }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const suffix = `impl-trigger-${field.replaceAll("_", "-")}`;
          const candidate = yield* prepareImplementationAdmissionCandidate(
            database,
            finalizer,
            suffix,
          );
          const admissionHandoffId = candidate.seeded.evidence.handoffId;
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(admissionHandoffId))._tag,
            "Admitted",
          );
          const harness = yield* buildImplementationCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            suffix,
            admission: candidate.admissionHarness.admission,
            finalizer,
            admissionHarness: candidate.admissionHarness,
            task: candidate.task,
            worktree: candidate.worktree,
          });
          assert.equal(
            (yield* harness.coordinator.processHandoff(admissionHandoffId))._tag,
            "Materialized",
          );
          const before = yield* captureImplementationTurnRollbackState(database.sqlA);
          const rejected = yield* Effect.sync(() => {
            const native = new NodeSqlite.DatabaseSync(database.filename);
            try {
              native.exec(
                "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA ignore_check_constraints = ON; BEGIN IMMEDIATE",
              );
              const evidence = native
                .prepare(
                  `SELECT * FROM agent_control_implementation_materialization_evidence
                     WHERE admission_handoff_id = ?`,
                )
                .get(admissionHandoffId) as Record<string, NodeSqlite.SQLInputValue>;
              for (const trigger of [
                "agent_control_implementation_deliveries_no_delete",
                "agent_control_implementation_materialization_markers_no_delete",
                "agent_control_implementation_handoff_accepted_no_delete",
                "agent_control_implementation_handoff_receipts_no_delete",
                "agent_control_implementation_handoff_intents_no_delete",
                "agent_control_implementation_materialization_receipts_no_delete",
                "agent_control_implementation_materialization_evidence_no_delete",
              ]) {
                native.exec(`DROP TRIGGER ${trigger}`);
              }
              native
                .prepare(
                  `DELETE FROM agent_control_implementation_deliveries
                     WHERE materialization_evidence_id = ?`,
                )
                .run(evidence.materialization_evidence_id!);
              native
                .prepare(
                  `DELETE FROM agent_control_implementation_materialization_markers
                     WHERE materialization_evidence_id = ?`,
                )
                .run(evidence.materialization_evidence_id!);
              const handoffId = native
                .prepare(
                  `SELECT handoff_id AS "handoffId"
                     FROM agent_control_implementation_handoff_accepted
                     WHERE materialization_evidence_id = ?`,
                )
                .get(evidence.materialization_evidence_id!) as { readonly handoffId: string };
              native
                .prepare(
                  "DELETE FROM agent_control_implementation_handoff_accepted WHERE handoff_id = ?",
                )
                .run(handoffId.handoffId);
              native
                .prepare(
                  "DELETE FROM agent_control_implementation_handoff_receipts WHERE handoff_id = ?",
                )
                .run(handoffId.handoffId);
              native
                .prepare(
                  "DELETE FROM agent_control_implementation_handoff_intents WHERE handoff_id = ?",
                )
                .run(handoffId.handoffId);
              native
                .prepare(
                  `DELETE FROM agent_control_implementation_materialization_receipts
                     WHERE materialization_evidence_id = ?`,
                )
                .run(evidence.materialization_evidence_id!);
              native
                .prepare(
                  `DELETE FROM agent_control_implementation_materialization_evidence
                     WHERE materialization_evidence_id = ?`,
                )
                .run(evidence.materialization_evidence_id!);
              evidence[field] = `Non-authoritative ${field}`;
              const columns = Object.keys(evidence);
              native
                .prepare(
                  `INSERT INTO agent_control_implementation_materialization_evidence (
                       ${columns.join(", ")}
                     ) VALUES (${columns.map(() => "?").join(", ")})`,
                )
                .run(...Object.values(evidence));
              native.exec("COMMIT");
              return false;
            } catch (cause) {
              if (native.isTransaction) native.exec("ROLLBACK");
              if (
                !(cause instanceof Error) ||
                !cause.message.includes("implementation materialization evidence is inconsistent")
              ) {
                throw cause;
              }
              return true;
            } finally {
              native.close();
            }
          });
          assert.isTrue(rejected);
          assert.deepStrictEqual(
            yield* captureImplementationTurnRollbackState(database.sqlA),
            before,
          );
        }),
      ),
    ),
);

it.effect(
  "rejects an internally refingerprinted Implementation prompt that diverges from admission",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const candidate = yield* prepareImplementationAdmissionCandidate(
            database,
            finalizer,
            "implementation-handoff-authority-red",
          );
          const planningHandoffId = candidate.seeded.evidence.handoffId;
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(planningHandoffId))._tag,
            "Admitted",
          );
          const coordinator = yield* buildImplementationCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            suffix: "implementation-handoff-authority-red",
            admission: candidate.admissionHarness.admission,
            finalizer,
            admissionHarness: candidate.admissionHarness,
            task: candidate.task,
            worktree: candidate.worktree,
          });
          assert.equal(
            (yield* coordinator.coordinator.processHandoff(planningHandoffId))._tag,
            "Materialized",
          );
          yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
          const handoffId = (yield* database.sqlA<{ readonly handoffId: string }>`
            SELECT handoff_id AS "handoffId"
            FROM agent_control_implementation_handoff_accepted
          `)[0]!.handoffId;
          const canonical = Option.getOrThrow(
            yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoffId),
          );
          const promptAuthority = (yield* database.sqlA<{
            readonly proposedPlanJson: string;
            readonly repositoryDisplay: string;
            readonly sourceRevision: string;
            readonly taskTitle: string;
            readonly taskBody: string | null;
          }>`
            SELECT proposed_plan_json AS "proposedPlanJson",
              repository_display AS "repositoryDisplay",
              source_revision AS "sourceRevision", task_title AS "taskTitle",
              task_body AS "taskBody"
            FROM agent_control_implementation_materialization_evidence
            WHERE materialization_evidence_id = ${canonical.evidence.materializationEvidenceId}
          `)[0]!;
          const freshInsertExit = yield* Effect.exit(
            database.sqlA.withTransaction(
              coordinator.handoffStore.insertAcceptedInTransaction(canonical.evidence, {
                ...canonical.evidence,
                ...promptAuthority,
                taskTitle: "A task title the Admission never authorized",
              }),
            ),
          );
          assert.isTrue(Exit.isFailure(freshInsertExit));
          assert.equal(
            (yield* database.sqlA<{ readonly count: number }>`
              SELECT count(*) AS count
              FROM agent_control_implementation_handoff_intents
            `)[0]!.count,
            1,
          );
          const promptText = canonical.evidence.promptText.replace(
            "Finalize Planning.",
            "Execute a refingerprinted plan that Admission never accepted.",
          );
          assert.notEqual(promptText, canonical.evidence.promptText);
          const messageEventTemplateJson = canonicalInitialPlanningEventTemplate({
            streamVersion: 3,
            eventId: EventId.make(canonical.evidence.messageEventId),
            aggregateKind: "thread",
            aggregateId: canonical.evidence.threadId,
            type: "thread.message-sent",
            occurredAt: canonical.evidence.createdAt,
            commandId: canonical.evidence.turnRequestCommandId,
            causationEventId: null,
            correlationId: canonical.evidence.turnRequestCommandId,
            actorKind: "client",
            payload: implementationMessagePayload({
              threadId: canonical.evidence.threadId,
              messageId: canonical.evidence.messageId,
              promptText,
              createdAt: canonical.evidence.createdAt,
            }),
            metadata: {},
          });
          const turnRequestEventTemplateJson = canonicalInitialPlanningEventTemplate({
            streamVersion: 4,
            eventId: EventId.make(canonical.evidence.turnRequestEventId),
            aggregateKind: "thread",
            aggregateId: canonical.evidence.threadId,
            type: "thread.turn-start-requested",
            occurredAt: canonical.evidence.createdAt,
            commandId: canonical.evidence.turnRequestCommandId,
            causationEventId: EventId.make(canonical.evidence.messageEventId),
            correlationId: canonical.evidence.turnRequestCommandId,
            actorKind: "client",
            payload: implementationTurnRequestPayload({
              threadId: canonical.evidence.threadId,
              messageId: canonical.evidence.messageId,
              modelSelection: canonical.evidence.modelSelection,
              runtimeMode: canonical.evidence.runtimeMode,
              sourceProposedPlan: {
                threadId: canonical.evidence.planningThreadId,
                planId: canonical.evidence.planId,
              },
              createdAt: canonical.evidence.createdAt,
            }),
            metadata: {},
          });
          const refingerprintedBase = {
            ...canonical.evidence,
            handoffFingerprint: "",
            promptText,
            promptDigest: sha256Utf8(promptText),
            messageEventTemplateJson,
            turnRequestEventTemplateJson,
            eventTemplateDigest: combinedInitialPlanningEventDigest(
              messageEventTemplateJson,
              turnRequestEventTemplateJson,
            ),
          } satisfies AgentControlImplementationHandoffEvidence;
          const handoffFingerprint = fingerprintImplementationHandoff(refingerprintedBase);

          yield* Effect.sync(() => {
            const native = new NodeSqlite.DatabaseSync(database.filename);
            const triggerNames = [
              "agent_control_implementation_handoff_intents_no_update",
              "agent_control_implementation_handoff_receipts_no_update",
              "agent_control_implementation_handoff_accepted_no_update",
              "agent_control_implementation_delivery_transition_validate",
            ] as const;
            try {
              native.exec(
                "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA ignore_check_constraints = ON; BEGIN IMMEDIATE",
              );
              const triggers = native
                .prepare(
                  `SELECT name, sql FROM sqlite_schema
                   WHERE type = 'trigger' AND name IN (${triggerNames.map(() => "?").join(",")})
                   ORDER BY name`,
                )
                .all(...triggerNames) as unknown as ReadonlyArray<{
                readonly name: string;
                readonly sql: string;
              }>;
              assert.lengthOf(triggers, triggerNames.length);
              for (const trigger of triggers) native.exec(`DROP TRIGGER "${trigger.name}"`);
              native
                .prepare(
                  `UPDATE agent_control_implementation_handoff_intents
                   SET prompt_text = ?, prompt_digest = ?, message_event_template_json = ?,
                     turn_request_event_template_json = ?, event_template_digest = ?,
                     handoff_fingerprint = ? WHERE handoff_id = ?`,
                )
                .run(
                  promptText,
                  refingerprintedBase.promptDigest,
                  messageEventTemplateJson,
                  turnRequestEventTemplateJson,
                  refingerprintedBase.eventTemplateDigest,
                  handoffFingerprint,
                  handoffId,
                );
              for (const table of [
                "agent_control_implementation_handoff_receipts",
                "agent_control_implementation_handoff_accepted",
                "agent_control_implementation_deliveries",
              ]) {
                native
                  .prepare(`UPDATE ${table} SET handoff_fingerprint = ? WHERE handoff_id = ?`)
                  .run(handoffFingerprint, handoffId);
              }
              for (const trigger of triggers) native.exec(trigger.sql);
              native.exec("COMMIT");
            } catch (cause) {
              if (native.isTransaction) native.exec("ROLLBACK");
              throw cause;
            } finally {
              native.close();
            }
          });

          const storeExit = yield* Effect.exit(
            coordinator.handoffStore.loadAcceptedByHandoffId(handoffId),
          );
          const replayExit = yield* Effect.exit(
            coordinator.coordinator.processHandoff(planningHandoffId),
          );
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const executorCalls = yield* Ref.make(0);
          const preparedMessages = yield* Ref.make<ReadonlyArray<string>>([]);
          const consumer = yield* buildImplementationConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator,
            executorCalls,
            preparedMessages,
            providerEvents,
          });
          const consumerExit = yield* Effect.exit(consumer.consumer.processHandoff(handoffId));
          const persisted = (yield* database.sqlA<{
            readonly state: string;
            readonly revision: number;
            readonly turnEvents: number;
            readonly startedEvents: number;
          }>`
            SELECT delivery.state, delivery.revision,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = delivery.thread_id
                 AND event_type IN ('thread.message-sent','thread.turn-start-requested'))
                AS "turnEvents",
              (SELECT count(*) FROM agent_control_events
               WHERE stream_id = delivery.stage_run_id
                 AND event_type = 'agentControl.stageRun.implementationStarted')
                AS "startedEvents"
            FROM agent_control_implementation_deliveries delivery
            WHERE delivery.handoff_id = ${handoffId}
          `)[0]!;
          const actual = {
            storeRejected: Exit.isFailure(storeExit),
            replayRejected: Exit.isFailure(replayExit),
            consumerRejected: Exit.isFailure(consumerExit),
            executorCalls: yield* Ref.get(executorCalls),
            preparedMessages: yield* Ref.get(preparedMessages),
            ...persisted,
          };
          assert.deepStrictEqual(actual, {
            storeRejected: true,
            replayRejected: true,
            consumerRejected: true,
            executorCalls: 0,
            preparedMessages: [],
            state: "pending",
            revision: 0,
            turnEvents: 0,
            startedEvents: 0,
          });
        }),
      ),
    ),
);

it.effect("converges two fresh Implementation materializers on one durable boundary", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerA,
          "implementation-materialization-race",
        );
        const handoffId = candidate.seeded.evidence.handoffId;
        assert.equal(
          (yield* candidate.admissionHarness.admission.processHandoff(handoffId))._tag,
          "Admitted",
        );
        const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          finalizerB,
          candidate.task,
          candidate.worktree,
          noopAdmissionHooks,
        );
        const reachedA = yield* Deferred.make<void>();
        const reachedB = yield* Deferred.make<void>();
        const releaseA = yield* Deferred.make<void>();
        const releaseB = yield* Deferred.make<void>();
        const hooks = (
          reached: Deferred.Deferred<void>,
          release: Deferred.Deferred<void>,
        ): AgentControlImplementationTurnCoordinatorHooksShape => ({
          ...noopImplementationCoordinatorHooks,
          afterAdmissionReplay: () =>
            Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
        });
        const coordinatorA = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix: "implementation-materialization-race-a",
          admission: candidate.admissionHarness.admission,
          finalizer: finalizerA,
          admissionHarness: candidate.admissionHarness,
          task: candidate.task,
          worktree: candidate.worktree,
          hooks: hooks(reachedA, releaseA),
        });
        const coordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: "implementation-materialization-race-b",
          admission: admissionB.admission,
          finalizer: finalizerB,
          admissionHarness: admissionB,
          task: candidate.task,
          worktree: candidate.worktree,
          hooks: hooks(reachedB, releaseB),
        });

        const fiberA = yield* coordinatorA.coordinator
          .processHandoff(handoffId)
          .pipe(Effect.forkChild);
        const fiberB = yield* coordinatorB.coordinator
          .processHandoff(handoffId)
          .pipe(Effect.forkChild);
        yield* Effect.all([Deferred.await(reachedA), Deferred.await(reachedB)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseA, undefined);
        const outcomeA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseB, undefined);
        const outcomeB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
        const outcomes = [outcomeA, outcomeB];
        assert.deepStrictEqual(outcomes.map((outcome) => outcome._tag).sort(), [
          "Materialized",
          "Replayed",
        ]);
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_implementation_materialization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_implementation_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted)
                AS handoffs,
              (SELECT count(*) FROM agent_control_implementation_deliveries)
                AS deliveries,
              (SELECT count(*) FROM orchestration_events event
               JOIN agent_control_implementation_materialization_evidence materialized
                 ON materialized.thread_id = event.stream_id
               WHERE event.event_type IN ('thread.created','thread.agent-control-bound'))
                AS threadEvents,
              (SELECT count(*) FROM agent_control_events event
               JOIN agent_control_implementation_admission_evidence admitted
                 ON admitted.implementation_controlled_thread_reservation_id = event.stream_id
               WHERE event.aggregate_kind = 'controlled-thread-reservation')
                AS reservationEvents
          `,
          [
            {
              evidence: 1,
              receipts: 1,
              markers: 1,
              handoffs: 1,
              deliveries: 1,
              threadEvents: 2,
              reservationEvents: 3,
            },
          ],
        );
      }),
    ),
  ),
);

it.effect(
  "claims one Provider turn and starts one Implementation StageRun across fresh workers",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
          const candidate = yield* prepareImplementationAdmissionCandidate(
            database,
            finalizerA,
            "implementation-delivery-stage-race",
          );
          const planningHandoffId = candidate.seeded.evidence.handoffId;
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(planningHandoffId))._tag,
            "Admitted",
          );
          const coordinatorA = yield* buildImplementationCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            suffix: "implementation-delivery-stage-race-a",
            admission: candidate.admissionHarness.admission,
            finalizer: finalizerA,
            admissionHarness: candidate.admissionHarness,
            task: candidate.task,
            worktree: candidate.worktree,
          });
          assert.equal(
            (yield* coordinatorA.coordinator.processHandoff(planningHandoffId))._tag,
            "Materialized",
          );
          const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
          const admissionB = yield* buildAdmission(
            database.sqlB,
            database.scopeB,
            finalizerB,
            candidate.task,
            candidate.worktree,
            noopAdmissionHooks,
          );
          const coordinatorB = yield* buildImplementationCoordinator({
            sql: database.sqlB,
            scope: database.scopeB,
            suffix: "implementation-delivery-stage-race-b",
            admission: admissionB.admission,
            finalizer: finalizerB,
            admissionHarness: admissionB,
            task: candidate.task,
            worktree: candidate.worktree,
          });
          assert.equal(
            (yield* coordinatorB.coordinator.processHandoff(planningHandoffId))._tag,
            "Replayed",
          );
          yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
          assert.deepStrictEqual(
            yield* database.sqlA`
            SELECT count(*) AS count
            FROM projection_threads
            WHERE thread_id IN (
              SELECT thread_id FROM agent_control_implementation_handoff_accepted
            )
          `,
            [{ count: 1 }],
          );
          const implementationHandoffId = (yield* database.sqlA<{
            readonly handoffId: string;
          }>`SELECT handoff_id AS "handoffId" FROM agent_control_implementation_handoff_accepted`)[0]!
            .handoffId;
          const implementationClaim = Option.getOrThrow(
            yield* coordinatorA.handoffStore.loadAcceptedByHandoffId(implementationHandoffId),
          );
          assert.isTrue(
            (yield* coordinatorA.snapshots.getCommandReadModel()).threads.some(
              (thread) => thread.id === implementationClaim.evidence.threadId,
            ),
          );
          assert.notEqual(
            implementationClaim.evidence.threadId,
            implementationClaim.evidence.planningThreadId,
          );
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const executorCalls = yield* Ref.make(0);
          const claimReachedA = yield* Deferred.make<void>();
          const claimReachedB = yield* Deferred.make<void>();
          const releaseClaimA = yield* Deferred.make<void>();
          const releaseClaimB = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() =>
            Effect.all(
              [
                Deferred.succeed(releaseClaimA, undefined),
                Deferred.succeed(releaseClaimB, undefined),
              ],
              { discard: true },
            ),
          );
          const consumerHooks = (
            reached: Deferred.Deferred<void>,
            release: Deferred.Deferred<void>,
          ): AgentControlImplementationTurnConsumerHooksShape => ({
            ...noopImplementationConsumerHooks,
            beforeClaim: () =>
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
          });
          const consumerA = yield* buildImplementationConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: coordinatorA,
            executorCalls,
            providerEvents,
            hooks: consumerHooks(claimReachedA, releaseClaimA),
          });
          const consumerB = yield* buildImplementationConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: coordinatorB,
            executorCalls,
            providerEvents,
            hooks: consumerHooks(claimReachedB, releaseClaimB),
          });
          const consumerFiberA = yield* consumerA.consumer
            .processHandoff(implementationHandoffId)
            .pipe(Effect.forkChild);
          yield* Deferred.await(claimReachedA).pipe(Effect.timeout(barrierTimeout));
          const consumerFiberB = yield* consumerB.consumer
            .processHandoff(implementationHandoffId)
            .pipe(Effect.forkChild);
          yield* Deferred.await(claimReachedB).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseClaimA, undefined);
          yield* Fiber.join(consumerFiberA).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseClaimB, undefined);
          yield* Fiber.join(consumerFiberB).pipe(Effect.timeout(barrierTimeout));

          assert.equal(yield* Ref.get(executorCalls), 1);
          const startedClaim = Option.getOrThrow(
            yield* coordinatorA.handoffStore.loadAcceptedByHandoffId(implementationHandoffId),
          );
          assert.equal(startedClaim.delivery.state, "provider-started");
          assert.equal(startedClaim.delivery.providerTurnId, "implementation-provider-turn");
          assert.equal(startedClaim.delivery.claimGeneration, 1);
          assert.equal(startedClaim.delivery.attemptCount, 1);
          assert.deepStrictEqual(
            yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_turn_accepted) AS accepted,
              (SELECT count(*) FROM agent_control_implementation_session_evidence) AS sessions,
              (SELECT count(*) FROM agent_control_implementation_delivery_attestations)
                AS attestations,
              (SELECT count(*) FROM orchestration_events
               WHERE stream_id = ${startedClaim.evidence.threadId}
                 AND event_type IN ('thread.message-sent','thread.turn-start-requested'))
                AS turnEvents
          `,
            [{ accepted: 1, sessions: 1, attestations: 1, turnEvents: 2 }],
          );

          const stageReachedA = yield* Deferred.make<void>();
          const stageReachedB = yield* Deferred.make<void>();
          const releaseStageA = yield* Deferred.make<void>();
          const releaseStageB = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() =>
            Effect.all(
              [
                Deferred.succeed(releaseStageA, undefined),
                Deferred.succeed(releaseStageB, undefined),
              ],
              { discard: true },
            ),
          );
          const stageHooks = (
            reached: Deferred.Deferred<void>,
            release: Deferred.Deferred<void>,
          ): AgentControlImplementationStageStarterHooksShape => ({
            ...noopImplementationStageStarterHooks,
            afterProviderEvidence: () =>
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
          });
          const starterA = yield* buildImplementationStageStarter({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: coordinatorA,
            finalizer: finalizerA,
            hooks: stageHooks(stageReachedA, releaseStageA),
          });
          const starterB = yield* buildImplementationStageStarter({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: coordinatorB,
            finalizer: finalizerB,
            hooks: stageHooks(stageReachedB, releaseStageB),
          });
          const stageFiberA = yield* starterA
            .processHandoff(implementationHandoffId)
            .pipe(Effect.forkChild);
          const stageFiberB = yield* starterB
            .processHandoff(implementationHandoffId)
            .pipe(Effect.forkChild);
          yield* Effect.all([Deferred.await(stageReachedA), Deferred.await(stageReachedB)], {
            concurrency: "unbounded",
          }).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseStageA, undefined);
          const stageA = yield* Fiber.join(stageFiberA).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseStageB, undefined);
          const stageB = yield* Fiber.join(stageFiberB).pipe(Effect.timeout(barrierTimeout));
          assert.deepStrictEqual([stageA._tag, stageB._tag].sort(), ["Replayed", "Started"]);
          assert.deepStrictEqual(
            yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE stream_id = ${startedClaim.evidence.stageRunId}
                 AND event_type = 'agentControl.stageRun.implementationStarted') AS started,
              (SELECT count(*) FROM agent_control_implementation_stage_started_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_implementation_stage_started_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_implementation_stage_started_markers)
                AS markers,
              (SELECT status FROM agent_control_stage_run_states
               WHERE stage_run_id = ${startedClaim.evidence.stageRunId}) AS stageStatus,
              (SELECT revision FROM agent_control_stage_run_states
               WHERE stage_run_id = ${startedClaim.evidence.stageRunId}) AS stageRevision,
              (SELECT status FROM agent_control_stage_run_lease_states
               WHERE lease_id = ${startedClaim.evidence.leaseId}) AS leaseStatus,
              (SELECT fence_token FROM agent_control_stage_run_lease_states
               WHERE lease_id = ${startedClaim.evidence.leaseId}) AS fenceToken
          `,
            [
              {
                started: 1,
                evidence: 1,
                receipts: 1,
                markers: 1,
                stageStatus: "running",
                stageRevision: 2,
                leaseStatus: "reserved",
                fenceToken: startedClaim.evidence.fenceToken,
              },
            ],
          );
        }),
      ),
    ),
);

it.effect("keeps response-loss ambiguous and adopts one identity-matched runtime start", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          "implementation-response-loss",
        );
        const planningHandoffId = candidate.seeded.evidence.handoffId;
        assert.equal(
          (yield* candidate.admissionHarness.admission.processHandoff(planningHandoffId))._tag,
          "Admitted",
        );
        const coordinator = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix: "implementation-response-loss",
          admission: candidate.admissionHarness.admission,
          finalizer,
          admissionHarness: candidate.admissionHarness,
          task: candidate.task,
          worktree: candidate.worktree,
        });
        assert.equal(
          (yield* coordinator.coordinator.processHandoff(planningHandoffId))._tag,
          "Materialized",
        );
        yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
        const handoffId = (yield* database.sqlA<{ readonly handoffId: string }>`
          SELECT handoff_id AS "handoffId"
          FROM agent_control_implementation_handoff_accepted
        `)[0]!.handoffId;
        const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const executorCalls = yield* Ref.make(0);
        const lossy = yield* buildImplementationConsumer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator,
          executorCalls,
          providerEvents,
          responseLoss: true,
        });
        yield* lossy.consumer.processHandoff(handoffId);
        const ambiguous = Option.getOrThrow(
          yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoffId),
        );
        assert.equal(ambiguous.delivery.state, "ambiguous");
        assert.equal(ambiguous.delivery.providerTurnId, null);
        assert.equal(ambiguous.delivery.claimGeneration, 1);
        assert.equal(ambiguous.delivery.attemptCount, 1);
        assert.equal(yield* Ref.get(executorCalls), 1);

        const restarted = yield* buildImplementationConsumer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator,
          executorCalls,
          providerEvents,
        });
        yield* restarted.consumer.processHandoff(handoffId);
        assert.equal(yield* Ref.get(executorCalls), 1);
        yield* restarted.consumer.processRuntimeEvent({
          type: "turn.started",
          eventId: EventId.make("implementation-response-loss-runtime-start"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ambiguous.evidence.providerInstanceId,
          threadId: ambiguous.evidence.threadId,
          createdAt: providerAcceptedAt,
          turnId: TurnId.make("implementation-response-loss-provider-turn"),
          payload: {},
        });
        const adopted = Option.getOrThrow(
          yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoffId),
        );
        assert.equal(adopted.delivery.state, "provider-started");
        assert.equal(adopted.delivery.providerTurnId, "implementation-response-loss-provider-turn");
        assert.equal(adopted.delivery.claimGeneration, 1);
        assert.equal(adopted.delivery.attemptCount, 1);
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ),
);

it.effect("isolates an invalid earlier materialization candidate from a healthy later one", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const left = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          "implementation-isolation-left",
        );
        const right = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizer,
          "implementation-isolation-right",
        );
        for (const candidate of [left, right]) {
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(
              candidate.seeded.evidence.handoffId,
            ))._tag,
            "Admitted",
          );
        }
        const ordered = yield* database.sqlA<{ readonly handoffId: string }>`
          SELECT handoff_id AS "handoffId"
          FROM agent_control_implementation_admission_markers
          ORDER BY handoff_id
        `;
        assert.lengthOf(ordered, 2);
        const healthyHandoffId = ordered[1]!.handoffId;
        const healthy = left.seeded.evidence.handoffId === healthyHandoffId ? left : right;
        const invalidHandoffId = ordered[0]!.handoffId;
        const coordinator = yield* buildImplementationCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          suffix: "implementation-isolation",
          admission: healthy.admissionHarness.admission,
          finalizer,
          admissionHarness: healthy.admissionHarness,
          task: healthy.task,
          worktree: healthy.worktree,
        });

        yield* coordinator.coordinator.recover;
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted)
                AS handoffs,
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence
               WHERE admission_handoff_id = ${healthyHandoffId}) AS healthy,
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence
               WHERE admission_handoff_id = ${invalidHandoffId}) AS invalid
          `,
          [{ markers: 1, handoffs: 1, healthy: 1, invalid: 0 }],
        );
      }),
    ),
  ),
);

it.effect("isolates invalid UTF-8 delivery evidence and starts the healthy later candidate", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const prepare = Effect.fn("prepareImplementationDeliveryIsolationCandidate")(function* (
          suffix: string,
        ) {
          const candidate = yield* prepareImplementationAdmissionCandidate(
            database,
            finalizer,
            suffix,
          );
          const admissionHandoffId = candidate.seeded.evidence.handoffId;
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(admissionHandoffId))._tag,
            "Admitted",
          );
          const coordinator = yield* buildImplementationCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            suffix,
            admission: candidate.admissionHarness.admission,
            finalizer,
            admissionHarness: candidate.admissionHarness,
            task: candidate.task,
            worktree: candidate.worktree,
          });
          assert.equal(
            (yield* coordinator.coordinator.processHandoff(admissionHandoffId))._tag,
            "Materialized",
          );
          yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
          return { admissionHandoffId, candidate, coordinator };
        });
        const left = yield* prepare("implementation-delivery-isolation-left");
        const right = yield* prepare("implementation-delivery-isolation-right");
        const ordered = yield* database.sqlA<{
          readonly handoffId: string;
          readonly admissionHandoffId: string;
        }>`
          SELECT accepted.handoff_id AS "handoffId",
            materialization.admission_handoff_id AS "admissionHandoffId"
          FROM agent_control_implementation_handoff_accepted accepted
          JOIN agent_control_implementation_materialization_evidence materialization
            ON materialization.materialization_evidence_id =
              accepted.materialization_evidence_id
          ORDER BY accepted.handoff_id
        `;
        assert.lengthOf(ordered, 2);
        const invalid = ordered[0]!;
        const healthy = ordered[1]!;
        const healthySetup = [left, right].find(
          (entry) => entry.admissionHandoffId === healthy.admissionHandoffId,
        );
        assert.isDefined(healthySetup);
        if (healthySetup === undefined) {
          return yield* Effect.die(new Error("healthy delivery candidate is unavailable"));
        }
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec("DROP TRIGGER agent_control_implementation_handoff_intents_no_update");
            native
              .prepare(
                "UPDATE agent_control_implementation_handoff_intents SET prompt_text = CAST(X'80' AS TEXT) WHERE handoff_id = ?",
              )
              .run(invalid.handoffId);
          } finally {
            native.close();
          }
        });

        const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildImplementationConsumer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: healthySetup.coordinator,
          executorCalls,
          providerEvents,
        });
        yield* consumer.consumer.recover;

        assert.equal(yield* Ref.get(executorCalls), 1);
        const healthyClaim = Option.getOrThrow(
          yield* healthySetup.coordinator.handoffStore.loadAcceptedByHandoffId(healthy.handoffId),
        );
        assert.equal(healthyClaim.delivery.state, "provider-started");
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT handoff_id AS "handoffId", state, attempt_count AS "attemptCount"
            FROM agent_control_implementation_deliveries
            ORDER BY handoff_id
          `,
          [
            { handoffId: invalid.handoffId, state: "pending", attemptCount: 0 },
            { handoffId: healthy.handoffId, state: "provider-started", attemptCount: 1 },
          ],
        );
      }),
    ),
  ),
);

it.effect.each<{
  readonly companion: "receipt" | "acceptance" | "marker" | "delivery" | "worktree-projection";
}>([
  { companion: "receipt" },
  { companion: "acceptance" },
  { companion: "marker" },
  { companion: "delivery" },
  { companion: "worktree-projection" },
])("isolates a missing $companion and recovers the next healthy candidate", ({ companion }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = yield* prepareImplementationDeliveryRecoveryCandidates(
          database,
          finalizer,
          ["implementation-missing-companion-left", "implementation-missing-companion-right"],
        );
        const invalid = candidates[0]!;
        const healthy = candidates[1]!;
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec("PRAGMA journal_mode = WAL; PRAGMA foreign_keys = OFF; BEGIN IMMEDIATE");
            const companionTable =
              companion === "receipt"
                ? "agent_control_implementation_handoff_receipts"
                : companion === "acceptance"
                  ? "agent_control_implementation_handoff_accepted"
                  : companion === "marker"
                    ? "agent_control_implementation_materialization_markers"
                    : companion === "delivery"
                      ? "agent_control_implementation_deliveries"
                      : "agent_control_worktree_reservation_states";
            const triggerName =
              companion === "worktree-projection" ? undefined : `${companionTable}_no_delete`;
            const trigger =
              triggerName === undefined
                ? undefined
                : (native
                    .prepare(`SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = ?`)
                    .get(triggerName) as { readonly sql: string });
            if (triggerName !== undefined) native.exec(`DROP TRIGGER ${triggerName}`);
            if (companion === "worktree-projection") {
              native
                .prepare(
                  `DELETE FROM agent_control_worktree_reservation_states
                   WHERE reservation_id = (
                     SELECT worktree_reservation_id
                     FROM agent_control_implementation_handoff_intents
                     WHERE handoff_id = ?
                   )`,
                )
                .run(invalid.handoffId);
            } else {
              native
                .prepare(`DELETE FROM ${companionTable} WHERE handoff_id = ?`)
                .run(invalid.handoffId);
            }
            if (trigger !== undefined) native.exec(trigger.sql);
            native.exec("COMMIT");
          } catch (cause) {
            if (native.isTransaction) native.exec("ROLLBACK");
            throw cause;
          } finally {
            native.close();
          }
        });

        const executorCalls = yield* Ref.make(0);
        const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const consumer = yield* buildImplementationConsumer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: healthy.coordinator,
          executorCalls,
          providerEvents,
        });
        yield* consumer.consumer.recover;
        assert.equal(yield* Ref.get(executorCalls), 1);
        assert.deepStrictEqual(
          yield* database.sqlB`
            SELECT intent.handoff_id AS "handoffId", delivery.state, delivery.revision,
              delivery.attempt_count AS "attemptCount"
            FROM agent_control_implementation_handoff_intents intent
            LEFT JOIN agent_control_implementation_deliveries delivery
              ON delivery.handoff_id = intent.handoff_id
            ORDER BY intent.handoff_id
          `,
          [
            {
              handoffId: invalid.handoffId,
              state: companion === "delivery" ? null : "pending",
              revision: companion === "delivery" ? null : 0,
              attemptCount: companion === "delivery" ? null : 0,
            },
            {
              handoffId: healthy.handoffId,
              state: "provider-started",
              revision: 4,
              attemptCount: 1,
            },
          ],
        );
      }),
    ),
  ),
);

it.effect.each<{
  readonly authorityPosition:
    | "prompt-plan"
    | "task-title"
    | "task-body"
    | "other-task-text"
    | "task-revision"
    | "github-sequence"
    | "source-fingerprint"
    | "repository"
    | "source-revision"
    | "projection"
    | "projection-project-id"
    | "projection-revision"
    | "projection-sequence"
    | "projection-repository-node-id"
    | "projection-issue-node-id"
    | "projection-issue-number"
    | "projection-issue-url"
    | "projection-status"
    | "projection-source-gate"
    | "projection-stage"
    | "projection-source-updated-at"
    | "projection-github-sequence"
    | "projection-created-at"
    | "projection-updated-at"
    | "worktree-projection-repository"
    | "worktree-projection-source-revision"
    | "worktree-event-id"
    | "worktree-event-sequence"
    | "worktree-event-stream-version"
    | "worktree-event-version-gap"
    | "worktree-event-payload"
    | "worktree-event-metadata"
    | "event-template"
    | "task-source";
}>([
  { authorityPosition: "prompt-plan" },
  { authorityPosition: "task-title" },
  { authorityPosition: "task-body" },
  { authorityPosition: "other-task-text" },
  { authorityPosition: "task-revision" },
  { authorityPosition: "github-sequence" },
  { authorityPosition: "source-fingerprint" },
  { authorityPosition: "repository" },
  { authorityPosition: "source-revision" },
  { authorityPosition: "projection" },
  { authorityPosition: "projection-project-id" },
  { authorityPosition: "projection-revision" },
  { authorityPosition: "projection-sequence" },
  { authorityPosition: "projection-repository-node-id" },
  { authorityPosition: "projection-issue-node-id" },
  { authorityPosition: "projection-issue-number" },
  { authorityPosition: "projection-issue-url" },
  { authorityPosition: "projection-status" },
  { authorityPosition: "projection-source-gate" },
  { authorityPosition: "projection-stage" },
  { authorityPosition: "projection-source-updated-at" },
  { authorityPosition: "projection-github-sequence" },
  { authorityPosition: "projection-created-at" },
  { authorityPosition: "projection-updated-at" },
  { authorityPosition: "worktree-projection-repository" },
  { authorityPosition: "worktree-projection-source-revision" },
  { authorityPosition: "worktree-event-id" },
  { authorityPosition: "worktree-event-sequence" },
  { authorityPosition: "worktree-event-stream-version" },
  { authorityPosition: "worktree-event-version-gap" },
  { authorityPosition: "worktree-event-payload" },
  { authorityPosition: "worktree-event-metadata" },
  { authorityPosition: "event-template" },
  { authorityPosition: "task-source" },
])(
  "isolates an authority-invalid $authorityPosition candidate from a healthy later one",
  ({ authorityPosition }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const prepare = Effect.fn("prepareImplementationAuthorityIsolationCandidate")(function* (
            suffix: string,
          ) {
            const candidate = yield* prepareImplementationAdmissionCandidate(
              database,
              finalizer,
              suffix,
            );
            const admissionHandoffId = candidate.seeded.evidence.handoffId;
            assert.equal(
              (yield* candidate.admissionHarness.admission.processHandoff(admissionHandoffId))._tag,
              "Admitted",
            );
            const coordinator = yield* buildImplementationCoordinator({
              sql: database.sqlA,
              scope: database.scopeA,
              suffix,
              admission: candidate.admissionHarness.admission,
              finalizer,
              admissionHarness: candidate.admissionHarness,
              task: candidate.task,
              worktree: candidate.worktree,
            });
            assert.equal(
              (yield* coordinator.coordinator.processHandoff(admissionHandoffId))._tag,
              "Materialized",
            );
            yield* seedPlanningSourceProjection(database.sqlA, candidate.seeded);
            return { admissionHandoffId, candidate, coordinator };
          });
          const left = yield* prepare("implementation-authority-isolation-left");
          const right = yield* prepare("implementation-authority-isolation-right");
          const ordered = yield* database.sqlA<{
            readonly handoffId: string;
            readonly admissionHandoffId: string;
            readonly materializationEvidenceId: string;
            readonly taskSourceEventId: string;
            readonly worktreeEventId: string;
          }>`
          SELECT accepted.handoff_id AS "handoffId",
            materialization.admission_handoff_id AS "admissionHandoffId",
            materialization.materialization_evidence_id AS "materializationEvidenceId",
            materialization.task_source_event_id AS "taskSourceEventId",
            materialization.worktree_event_id AS "worktreeEventId"
          FROM agent_control_implementation_handoff_accepted accepted
          JOIN agent_control_implementation_materialization_evidence materialization
            ON materialization.materialization_evidence_id =
              accepted.materialization_evidence_id
          ORDER BY accepted.handoff_id
        `;
          assert.lengthOf(ordered, 2);
          const invalid = ordered[0]!;
          const healthy = ordered[1]!;
          const healthySetup = [left, right].find(
            (entry) => entry.admissionHandoffId === healthy.admissionHandoffId,
          );
          assert.isDefined(healthySetup);
          if (healthySetup === undefined) {
            return yield* Effect.die(new Error("healthy authority candidate is unavailable"));
          }
          yield* Ref.set(finalizer.stagePublished, []);
          yield* Ref.set(finalizer.leasePublished, []);
          yield* Effect.sync(() => {
            const native = new NodeSqlite.DatabaseSync(database.filename);
            try {
              const bypassForeignKeys =
                authorityPosition === "worktree-event-id" ||
                authorityPosition === "worktree-event-sequence" ||
                authorityPosition === "worktree-event-stream-version" ||
                authorityPosition === "worktree-event-version-gap";
              native.exec(
                `PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ${bypassForeignKeys ? "OFF" : "ON"}; PRAGMA ignore_check_constraints = ON; BEGIN IMMEDIATE`,
              );
              const triggerName =
                authorityPosition === "task-title" ||
                authorityPosition === "task-body" ||
                authorityPosition === "other-task-text" ||
                authorityPosition === "repository" ||
                authorityPosition === "source-revision" ||
                authorityPosition === "worktree-projection-repository" ||
                authorityPosition === "worktree-projection-source-revision" ||
                authorityPosition === "worktree-event-id" ||
                authorityPosition === "worktree-event-sequence" ||
                authorityPosition === "worktree-event-stream-version"
                  ? "agent_control_implementation_materialization_evidence_no_update"
                  : authorityPosition === "task-source"
                    ? "agent_control_implementation_task_source_event_no_update"
                    : authorityPosition === "worktree-event-metadata" ||
                        authorityPosition === "worktree-event-payload" ||
                        authorityPosition === "worktree-event-version-gap"
                      ? "agent_control_worktree_event_immutable_update"
                      : "agent_control_implementation_handoff_intents_no_update";
              const trigger = native
                .prepare(
                  `SELECT sql FROM sqlite_schema
                 WHERE type = 'trigger'
                   AND name = ?`,
                )
                .get(triggerName) as { readonly sql: string } | undefined;
              assert.isDefined(trigger);
              native.exec(`DROP TRIGGER ${triggerName}`);
              if (authorityPosition === "prompt-plan") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_handoff_intents
                   SET prompt_text = replace(prompt_text, 'Finalize Planning.', 'Foreign Plan.')
                   WHERE handoff_id = ?`,
                  )
                  .run(invalid.handoffId);
              } else if (authorityPosition === "task-title") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                   SET task_title = 'Authority-invalid task title'
                   WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "task-body") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                   SET task_body = 'Authority-invalid task body'
                   WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "other-task-text") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                   SET task_title = 'Other valid task title', task_body = 'Other valid task body'
                   WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "task-revision") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_handoff_intents
                   SET task_revision = task_revision + 1 WHERE handoff_id = ?`,
                  )
                  .run(invalid.handoffId);
              } else if (authorityPosition === "github-sequence") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_handoff_intents
                   SET github_intake_sequence = github_intake_sequence + 1 WHERE handoff_id = ?`,
                  )
                  .run(invalid.handoffId);
              } else if (authorityPosition === "source-fingerprint") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_handoff_intents
                   SET source_identity_fingerprint = ? WHERE handoff_id = ?`,
                  )
                  .run("e".repeat(64), invalid.handoffId);
              } else if (authorityPosition === "repository") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                   SET repository_display = 'foreign/repository'
                   WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "source-revision") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                   SET source_revision = ? WHERE materialization_evidence_id = ?`,
                  )
                  .run("f".repeat(40), invalid.materializationEvidenceId);
              } else if (authorityPosition === "projection") {
                native
                  .prepare(
                    `UPDATE agent_control_task_states SET state_json = json_set(
                     state_json, '$.sourceSnapshot.title', 'Projection-invalid task title'
                   ) WHERE last_event_sequence = (
                     SELECT task_source_event_sequence
                     FROM agent_control_implementation_materialization_evidence
                     WHERE materialization_evidence_id = ?
                   )`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition.startsWith("projection-")) {
                const mutation = {
                  "projection-project-id": ["project_id", "foreign-project"],
                  "projection-revision": ["revision", 2],
                  "projection-sequence": ["last_event_sequence", 999999],
                  "projection-repository-node-id": ["repository_node_id", "foreign-repository"],
                  "projection-issue-node-id": ["issue_node_id", "foreign-issue"],
                  "projection-issue-number": ["issue_number", 999999],
                  "projection-issue-url": ["issue_url", "https://example.test/foreign/999999"],
                  "projection-status": ["status", "needs-attention"],
                  "projection-source-gate": ["source_gate", "source-missing"],
                  "projection-stage": ["stage", "implementation"],
                  "projection-source-updated-at": ["source_updated_at", "2026-08-04T00:00:00.000Z"],
                  "projection-github-sequence": ["github_intake_sequence", 999999],
                  "projection-created-at": ["created_at", "2026-08-04T00:00:00.000Z"],
                  "projection-updated-at": ["updated_at", "2026-08-04T00:00:00.000Z"],
                } as const;
                const [column, value] = mutation[authorityPosition as keyof typeof mutation];
                native
                  .prepare(
                    `UPDATE agent_control_task_states SET ${column} = ?
                     WHERE last_event_sequence = (
                       SELECT task_source_event_sequence
                       FROM agent_control_implementation_materialization_evidence
                       WHERE materialization_evidence_id = ?
                     )`,
                  )
                  .run(value, invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-projection-repository") {
                native
                  .prepare(
                    `UPDATE agent_control_worktree_reservation_states
                     SET repository_name_with_owner = 'foreign/repository',
                       state_json = json_set(
                         state_json, '$.repository.nameWithOwner', 'foreign/repository'
                       )
                     WHERE reservation_id = (
                       SELECT worktree_reservation_id
                       FROM agent_control_implementation_materialization_evidence
                       WHERE materialization_evidence_id = ?
                     )`,
                  )
                  .run(invalid.materializationEvidenceId);
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                     SET repository_display = 'foreign/repository'
                     WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-projection-source-revision") {
                native
                  .prepare(
                    `UPDATE agent_control_worktree_reservation_states
                     SET base_commit_sha = ?, head_commit_sha = ?,
                       state_json = json_set(
                         state_json, '$.baseCommitSha', ?, '$.headCommitSha', ?
                       )
                     WHERE reservation_id = (
                       SELECT worktree_reservation_id
                       FROM agent_control_implementation_materialization_evidence
                       WHERE materialization_evidence_id = ?
                     )`,
                  )
                  .run(
                    "f".repeat(40),
                    "f".repeat(40),
                    "f".repeat(40),
                    "f".repeat(40),
                    invalid.materializationEvidenceId,
                  );
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                     SET source_revision = ? WHERE materialization_evidence_id = ?`,
                  )
                  .run("f".repeat(40), invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-event-id") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                     SET worktree_event_id = 'foreign-worktree-event'
                     WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-event-sequence") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                     SET worktree_event_sequence = worktree_event_sequence + 999999
                     WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-event-stream-version") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_materialization_evidence
                     SET worktree_event_stream_version = worktree_event_stream_version + 99
                     WHERE materialization_evidence_id = ?`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-event-version-gap") {
                native
                  .prepare(
                    `UPDATE agent_control_events SET stream_version = 5
                     WHERE aggregate_kind = 'worktree-reservation'
                       AND stream_id = (
                         SELECT worktree_reservation_id
                         FROM agent_control_implementation_materialization_evidence
                         WHERE materialization_evidence_id = ?
                       ) AND stream_version = 2`,
                  )
                  .run(invalid.materializationEvidenceId);
              } else if (authorityPosition === "worktree-event-payload") {
                native
                  .prepare(
                    `UPDATE agent_control_events SET payload_json = ' ' || payload_json
                     WHERE event_id = ?`,
                  )
                  .run(invalid.worktreeEventId);
              } else if (authorityPosition === "worktree-event-metadata") {
                native
                  .prepare(
                    `UPDATE agent_control_events SET metadata_json = '{ "schemaVersion": 1 }'
                     WHERE event_id = ?`,
                  )
                  .run(invalid.worktreeEventId);
              } else if (authorityPosition === "event-template") {
                native
                  .prepare(
                    `UPDATE agent_control_implementation_handoff_intents
                   SET message_event_template_json = json_set(
                     message_event_template_json, '$.metadata.authorityInvalid', 1
                   ) WHERE handoff_id = ?`,
                  )
                  .run(invalid.handoffId);
              } else {
                native
                  .prepare(
                    `UPDATE agent_control_events
                   SET payload_json = json_set(
                     payload_json, '$.sourceSnapshot.title', 'Authority-invalid source title'
                   ) WHERE event_id = ?`,
                  )
                  .run(invalid.taskSourceEventId);
              }
              native.exec(trigger!.sql);
              native.exec("COMMIT");
            } catch (cause) {
              if (native.isTransaction) native.exec("ROLLBACK");
              throw cause;
            } finally {
              native.close();
            }
          });

          const invalidLoad = yield* Effect.exit(
            healthySetup.coordinator.handoffStore.loadAcceptedByHandoffId(invalid.handoffId),
          );
          assert.isTrue(Exit.isFailure(invalidLoad));
          if (Exit.isFailure(invalidLoad)) {
            const found = Cause.findErrorOption(invalidLoad.cause);
            assert.isTrue(Option.isSome(found));
            if (Option.isSome(found)) {
              const isStoreError = Schema.is(AgentControlImplementationStoreError);
              assert.isTrue(isStoreError(found.value));
              if (isStoreError(found.value)) {
                assert.equal(found.value.reason, "candidate-evidence");
              }
            }
          }
          const invalidReplay = yield* Effect.exit(
            healthySetup.coordinator.coordinator.processHandoff(invalid.admissionHandoffId),
          );
          assert.isTrue(Exit.isFailure(invalidReplay));
          yield* healthySetup.coordinator.coordinator.recover;
          assert.deepStrictEqual(
            yield* database.sqlB`
              SELECT handoff_id AS "handoffId", state, revision, attempt_count AS "attemptCount"
              FROM agent_control_implementation_deliveries ORDER BY handoff_id
            `,
            [
              { handoffId: invalid.handoffId, state: "pending", revision: 0, attemptCount: 0 },
              { handoffId: healthy.handoffId, state: "pending", revision: 0, attemptCount: 0 },
            ],
          );
          assert.equal((yield* Ref.get(finalizer.stagePublished)).length, 0);
          assert.equal((yield* Ref.get(finalizer.leasePublished)).length, 0);

          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const executorCalls = yield* Ref.make(0);
          const healthyLoad = yield* Effect.exit(
            healthySetup.coordinator.handoffStore.loadAcceptedByHandoffId(healthy.handoffId),
          );
          assert.isTrue(
            Exit.isSuccess(healthyLoad),
            Exit.isFailure(healthyLoad) ? Cause.pretty(healthyLoad.cause) : undefined,
          );
          const consumer = yield* buildImplementationConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: healthySetup.coordinator,
            executorCalls,
            providerEvents,
          });
          yield* consumer.consumer.recover;

          assert.equal(yield* Ref.get(executorCalls), 1);
          const healthyClaim = Option.getOrThrow(
            yield* healthySetup.coordinator.handoffStore.loadAcceptedByHandoffId(healthy.handoffId),
          );
          const starter = yield* buildImplementationStageStarter({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: healthySetup.coordinator,
            finalizer,
          });
          assert.equal((yield* starter.processHandoff(healthy.handoffId))._tag, "Started");
          assert.deepStrictEqual(
            yield* database.sqlB`
            SELECT handoff_id AS "handoffId", state, revision, attempt_count AS "attemptCount"
            FROM agent_control_implementation_deliveries
            ORDER BY handoff_id
          `,
            [
              { handoffId: invalid.handoffId, state: "pending", revision: 0, attemptCount: 0 },
              {
                handoffId: healthy.handoffId,
                state: "provider-started",
                revision: 4,
                attemptCount: 1,
              },
            ],
          );
          assert.deepStrictEqual(
            yield* database.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE stream_id = ${healthyClaim.evidence.stageRunId}
                 AND event_type = 'agentControl.stageRun.implementationStarted') AS started,
              (SELECT count(*) FROM agent_control_implementation_stage_started_markers marker
               JOIN agent_control_implementation_stage_started_evidence evidence
                 ON evidence.start_evidence_id = marker.start_evidence_id
               WHERE evidence.handoff_id = ${healthy.handoffId}) AS markers,
              (SELECT count(*) FROM agent_control_implementation_stage_started_markers marker
               JOIN agent_control_implementation_stage_started_evidence evidence
                 ON evidence.start_evidence_id = marker.start_evidence_id
               WHERE evidence.handoff_id = ${invalid.handoffId}) AS invalidMarkers
          `,
            [{ started: 1, markers: 1, invalidMarkers: 0 }],
          );
          assert.equal((yield* Ref.get(finalizer.stagePublished)).length, 1);
          assert.equal((yield* Ref.get(finalizer.leasePublished)).length, 0);
        }),
      ),
    ),
);

it.effect("revalidates task authority after turn acceptance and before provider delivery", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = yield* prepareImplementationDeliveryRecoveryCandidates(
          database,
          finalizer,
          ["implementation-delivery-pre-provider-authority"],
        );
        const candidate = candidates[0]!;
        const executorCalls = yield* Ref.make(0);
        const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const consumer = yield* buildImplementationConsumer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: candidate.coordinator,
          executorCalls,
          providerEvents,
          hooks: {
            ...noopImplementationConsumerHooks,
            beforeClaim: () =>
              Effect.sync(() => {
                const native = new NodeSqlite.DatabaseSync(database.filename);
                try {
                  native.exec(
                    "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; BEGIN IMMEDIATE",
                  );
                  const trigger = native
                    .prepare(
                      `SELECT sql FROM sqlite_schema
                       WHERE type = 'trigger'
                         AND name = 'agent_control_implementation_materialization_evidence_no_update'`,
                    )
                    .get() as { readonly sql: string };
                  native.exec(
                    "DROP TRIGGER agent_control_implementation_materialization_evidence_no_update",
                  );
                  native
                    .prepare(
                      `UPDATE agent_control_implementation_materialization_evidence
                       SET task_body = 'Authority changed before provider delivery'
                       WHERE admission_handoff_id = ?`,
                    )
                    .run(candidate.candidate.seeded.evidence.handoffId);
                  native.exec(trigger.sql);
                  native.exec("COMMIT");
                } catch (cause) {
                  if (native.isTransaction) native.exec("ROLLBACK");
                  throw cause;
                } finally {
                  native.close();
                }
              }),
          },
        });
        const exit = yield* Effect.exit(consumer.consumer.processHandoff(candidate.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        assert.equal(yield* Ref.get(executorCalls), 0);
        assert.deepStrictEqual(
          yield* database.sqlB`
            SELECT state, revision, attempt_count AS "attemptCount",
              (SELECT count(*) FROM agent_control_implementation_session_evidence)
                AS sessions,
              (SELECT count(*) FROM agent_control_implementation_delivery_attestations)
                AS attestations,
              (SELECT count(*) FROM agent_control_implementation_stage_started_markers)
                AS started
            FROM agent_control_implementation_deliveries
            WHERE handoff_id = ${candidate.handoffId}
          `,
          [
            {
              state: "turn-accepted",
              revision: 1,
              attemptCount: 0,
              sessions: 0,
              attestations: 0,
              started: 0,
            },
          ],
        );
      }),
    ),
  ),
);

it.effect.each<{ readonly exceptional: "defect" | "interrupt" }>([
  { exceptional: "defect" },
  { exceptional: "interrupt" },
])(
  "propagates a delivery recovery $exceptional and stops before the later candidate",
  ({ exceptional }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const candidates = yield* prepareImplementationDeliveryRecoveryCandidates(
            database,
            finalizer,
            [
              `implementation-delivery-${exceptional}-left`,
              `implementation-delivery-${exceptional}-right`,
            ],
          );
          const first = candidates[0]!;
          const second = candidates[1]!;
          const arrived = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const executorCalls = yield* Ref.make(0);
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const consumer = yield* buildImplementationConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: second.coordinator,
            executorCalls,
            providerEvents,
            hooks: {
              ...noopImplementationConsumerHooks,
              beforeClaim: (handoffId) =>
                handoffId !== first.handoffId
                  ? Effect.void
                  : exceptional === "defect"
                    ? Effect.die(new Error("implementation-delivery-recovery-defect"))
                    : Deferred.succeed(arrived, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      ),
            },
          });
          if (exceptional === "defect") {
            const exit = yield* Effect.exit(consumer.consumer.recover);
            assert.isTrue(Exit.isFailure(exit));
            if (Exit.isFailure(exit)) {
              assert.include(Cause.pretty(exit.cause), "implementation-delivery-recovery-defect");
            }
          } else {
            const fiber = yield* consumer.consumer.recover.pipe(Effect.forkChild);
            yield* Deferred.await(arrived).pipe(Effect.timeout(barrierTimeout));
            yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);
            assert.isTrue(Exit.isFailure(exit));
            if (Exit.isFailure(exit)) assert.isTrue(Cause.hasInterruptsOnly(exit.cause));
          }
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.deepStrictEqual(
            yield* database.sqlB`
              SELECT handoff_id AS "handoffId", state, revision, attempt_count AS "attemptCount"
              FROM agent_control_implementation_deliveries ORDER BY handoff_id
            `,
            [
              { handoffId: first.handoffId, state: "turn-accepted", revision: 1, attemptCount: 0 },
              { handoffId: second.handoffId, state: "pending", revision: 0, attemptCount: 0 },
            ],
          );
        }),
      ),
    ),
);

it.effect.each<{ readonly globalFailure: "persistence" | "revision-conflict" }>([
  { globalFailure: "persistence" },
  { globalFailure: "revision-conflict" },
])(
  "propagates a global delivery $globalFailure and stops before the later candidate",
  ({ globalFailure }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const candidates = yield* prepareImplementationDeliveryRecoveryCandidates(
            database,
            finalizer,
            [
              `implementation-delivery-${globalFailure}-left`,
              `implementation-delivery-${globalFailure}-right`,
            ],
          );
          const first = candidates[0]!;
          const second = candidates[1]!;
          const baseStore = second.coordinator.handoffStore;
          const failure = new AgentControlImplementationStoreError({
            operation: `test-global-${globalFailure}`,
            reason: globalFailure,
          });
          const store = AgentControlImplementationHandoffStore.of({
            ...baseStore,
            ...(globalFailure === "persistence"
              ? { listRecoverable: () => Effect.fail(failure) }
              : { markTurnAccepted: () => Effect.fail(failure) }),
          });
          const executorCalls = yield* Ref.make(0);
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const consumer = yield* buildImplementationConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: second.coordinator,
            executorCalls,
            providerEvents,
            store,
          });
          const exit = yield* Effect.exit(consumer.consumer.recover);
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            const found = Cause.findErrorOption(exit.cause);
            assert.isTrue(Option.isSome(found));
            if (Option.isSome(found)) assert.strictEqual(found.value, failure);
          }
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.deepStrictEqual(
            yield* database.sqlB`
              SELECT handoff_id AS "handoffId", state, revision, attempt_count AS "attemptCount"
              FROM agent_control_implementation_deliveries ORDER BY handoff_id
            `,
            [
              { handoffId: first.handoffId, state: "pending", revision: 0, attemptCount: 0 },
              { handoffId: second.handoffId, state: "pending", revision: 0, attemptCount: 0 },
            ],
          );
        }),
      ),
    ),
);

it.effect(
  "propagates materialization recovery defects and interrupts without trailing writes",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
          const candidate = yield* prepareImplementationAdmissionCandidate(
            database,
            finalizerA,
            "implementation-recovery-exceptional",
          );
          const handoffId = candidate.seeded.evidence.handoffId;
          assert.equal(
            (yield* candidate.admissionHarness.admission.processHandoff(handoffId))._tag,
            "Admitted",
          );
          const defectCoordinator = yield* buildImplementationCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            suffix: "implementation-recovery-defect",
            admission: candidate.admissionHarness.admission,
            finalizer: finalizerA,
            admissionHarness: candidate.admissionHarness,
            task: candidate.task,
            worktree: candidate.worktree,
            hooks: {
              ...noopImplementationCoordinatorHooks,
              afterAdmissionReplay: () =>
                Effect.die(new Error("implementation-materialization-recovery-defect")),
            },
          });
          const defectExit = yield* Effect.exit(defectCoordinator.coordinator.recover);
          assert.isTrue(Exit.isFailure(defectExit));
          if (Exit.isFailure(defectExit)) {
            assert.include(
              Cause.pretty(defectExit.cause),
              "implementation-materialization-recovery-defect",
            );
          }

          const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
          const admissionB = yield* buildAdmission(
            database.sqlB,
            database.scopeB,
            finalizerB,
            candidate.task,
            candidate.worktree,
            noopAdmissionHooks,
          );
          const arrived = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const interruptCoordinator = yield* buildImplementationCoordinator({
            sql: database.sqlB,
            scope: database.scopeB,
            suffix: "implementation-recovery-interrupt",
            admission: admissionB.admission,
            finalizer: finalizerB,
            admissionHarness: admissionB,
            task: candidate.task,
            worktree: candidate.worktree,
            hooks: {
              ...noopImplementationCoordinatorHooks,
              afterAdmissionReplay: () =>
                Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release))),
            },
          });
          const fiber = yield* interruptCoordinator.coordinator.recover.pipe(Effect.forkChild);
          yield* Deferred.await(arrived).pipe(Effect.timeout(barrierTimeout));
          yield* Fiber.interrupt(fiber);
          const interrupted = yield* Fiber.await(fiber);
          assert.isTrue(Exit.isFailure(interrupted));
          if (Exit.isFailure(interrupted)) {
            assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
          }
          assert.deepStrictEqual(
            yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_implementation_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_implementation_materialization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_implementation_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_implementation_handoff_accepted)
                AS handoffs,
              (SELECT count(*) FROM orchestration_events event
               JOIN agent_control_implementation_admission_evidence admitted
                 ON admitted.implementation_thread_id = event.stream_id) AS threadEvents,
              (SELECT count(*) FROM agent_control_implementation_thread_reservation_states
               WHERE status = 'prepared' AND revision = 1) AS preparedReservations
          `,
            [
              {
                evidence: 0,
                receipts: 0,
                markers: 0,
                handoffs: 0,
                threadEvents: 0,
                preparedReservations: 1,
              },
            ],
          );
        }),
      ),
    ),
);

it.effect.each<{
  readonly claimGeneration: number;
  readonly attemptCount: number;
}>([
  { claimGeneration: 2, attemptCount: 1 },
  { claimGeneration: 1, attemptCount: 2 },
])(
  "migration 053 rejects unreachable legacy delivery counters $claimGeneration/$attemptCount",
  ({ claimGeneration, attemptCount }) =>
    withNode(
      Effect.gen(function* () {
        const { database, seeded } = yield* makeRecoverableMigration052CompletedPlanning(
          `legacy-counter-${claimGeneration}-${attemptCount}`,
        );
        yield* database.sqlA.unsafe(
          "DROP TRIGGER agent_control_initial_planning_delivery_transition_validate",
        ).unprepared;
        if (claimGeneration === 2) {
          yield* database.sqlA.withTransaction(database.sqlA`
            UPDATE agent_control_initial_planning_deliveries
            SET claim_generation = 2, attempt_count = 1
            WHERE handoff_id = ${seeded.evidence.handoffId}
          `);
        } else {
          yield* database.sqlA.withTransaction(database.sqlA`
            UPDATE agent_control_initial_planning_deliveries
            SET claim_generation = 1, attempt_count = 2
            WHERE handoff_id = ${seeded.evidence.handoffId}
          `);
        }
        const before = yield* captureMigration053State(database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
        );

        if (Exit.isSuccess(upgrade)) {
          const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
          yield* recovered.finalizer.recover;
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
            stageEvents: 3,
            leaseEvents: 2,
            started: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
          });
          assert.fail(
            `migration 053 accepted and recovered unreachable counters ${claimGeneration}/${attemptCount}`,
          );
        }
        assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
      }),
    ),
);

it.effect.each<{
  readonly corruption: "stream-version-gap" | "payload-whitespace" | "metadata-whitespace";
  readonly recoveryOperation:
    | "orchestration-order"
    | "canonical-orchestration-payload"
    | "canonical-orchestration-metadata";
}>([
  { corruption: "stream-version-gap", recoveryOperation: "orchestration-order" },
  {
    corruption: "payload-whitespace",
    recoveryOperation: "canonical-orchestration-payload",
  },
  {
    corruption: "metadata-whitespace",
    recoveryOperation: "canonical-orchestration-metadata",
  },
])(
  "migration 053 rejects legacy terminal orchestration $corruption",
  ({ corruption, recoveryOperation }) =>
    withNode(
      Effect.gen(function* () {
        const { database, seeded, terminal } = yield* makeRecoverableMigration052CompletedPlanning(
          `legacy-terminal-history-${corruption}`,
        );
        if (corruption === "stream-version-gap") {
          yield* database.sqlA.withTransaction(database.sqlA`
            UPDATE orchestration_events
            SET stream_version = stream_version + 100
            WHERE event_id = ${terminal.eventId}
          `);
        } else if (corruption === "payload-whitespace") {
          yield* database.sqlA.withTransaction(database.sqlA`
            UPDATE orchestration_events
            SET payload_json = ' ' || payload_json
            WHERE event_id = ${terminal.eventId}
          `);
        } else {
          yield* database.sqlA.withTransaction(database.sqlA`
            UPDATE orchestration_events
            SET metadata_json = ' ' || metadata_json
            WHERE event_id = ${terminal.eventId}
          `);
        }
        const before = yield* captureMigration053State(database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
        );

        if (Exit.isSuccess(upgrade)) {
          const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
          const recovery = yield* Effect.exit(
            recovered.finalizer.processHandoff(seeded.evidence.handoffId),
          );
          assert.isTrue(Exit.isFailure(recovery));
          if (Exit.isFailure(recovery)) {
            assert.isTrue(
              recovery.cause.reasons.some(
                (reason) =>
                  Cause.isFailReason(reason) &&
                  isFinalizerError(reason.error) &&
                  reason.error.operation === recoveryOperation,
              ),
            );
          }
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
            stageEvents: 2,
            leaseEvents: 1,
            started: 1,
            evidence: 0,
            receipts: 0,
            markers: 0,
          });
          assert.fail(`migration 053 accepted ${corruption} rejected by recovery`);
        }
        assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
      }),
    ),
);

it.effect.each<{
  readonly column: "payload_json" | "metadata_json";
  readonly recoveryOperation:
    | "canonical-orchestration-payload"
    | "canonical-orchestration-metadata";
}>([
  { column: "payload_json", recoveryOperation: "canonical-orchestration-payload" },
  { column: "metadata_json", recoveryOperation: "canonical-orchestration-metadata" },
])(
  "migration 053 rejects invalid UTF-8 in legacy orchestration $column",
  ({ column, recoveryOperation }) =>
    withNode(
      Effect.gen(function* () {
        const { database, seeded, plan } = yield* makeRecoverableMigration052CompletedPlanning(
          `legacy-invalid-utf8-${column}`,
        );
        if (column === "payload_json") {
          yield* database.sqlA.withTransaction(
            database.sqlA.unsafe(
              `UPDATE orchestration_events
             SET payload_json = CAST(X'7B2261223A22FF227D' AS TEXT)
             WHERE event_id = ?`,
              [plan.eventId],
            ),
          );
        } else {
          yield* database.sqlA.withTransaction(
            database.sqlA.unsafe(
              `UPDATE orchestration_events
             SET metadata_json = CAST(X'7B2261223A22FF227D' AS TEXT)
             WHERE event_id = ?`,
              [plan.eventId],
            ),
          );
        }
        const storedRows =
          column === "payload_json"
            ? yield* database.sqlA<{
                readonly storage: string;
                readonly bytesHex: string;
              }>`
              SELECT typeof(payload_json) AS storage,
                hex(CAST(payload_json AS BLOB)) AS "bytesHex"
              FROM orchestration_events
              WHERE event_id = ${plan.eventId}
            `
            : yield* database.sqlA<{
                readonly storage: string;
                readonly bytesHex: string;
              }>`
              SELECT typeof(metadata_json) AS storage,
                hex(CAST(metadata_json AS BLOB)) AS "bytesHex"
              FROM orchestration_events
              WHERE event_id = ${plan.eventId}
            `;
        const stored = storedRows[0] as {
          readonly storage: string;
          readonly bytesHex: string;
        };
        assert.deepStrictEqual(stored, {
          storage: "text",
          bytesHex: "7B2261223A22FF227D",
        });
        assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
        const before = yield* captureMigration053State(database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
        );

        if (Exit.isSuccess(upgrade)) {
          const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
          const recovery = yield* Effect.exit(
            recovered.finalizer.processHandoff(seeded.evidence.handoffId),
          );
          assert.isTrue(Exit.isFailure(recovery));
          if (Exit.isFailure(recovery)) {
            assert.isTrue(
              recovery.cause.reasons.some(
                (reason) =>
                  Cause.isFailReason(reason) &&
                  isFinalizerError(reason.error) &&
                  reason.error.operation === recoveryOperation,
              ),
            );
          }
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
            stageEvents: 2,
            leaseEvents: 1,
            started: 1,
            evidence: 0,
            receipts: 0,
            markers: 0,
          });
          assert.fail(`migration 053 accepted invalid UTF-8 in ${column}`);
        }
        assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
      }),
    ),
);

it.effect("one invalid UTF-8 thread rolls migration 053 back for all legacy threads", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeLegacyFinalizations([
        "legacy-valid-utf8-thread",
        "legacy-invalid-utf8-thread",
      ]);
      const invalid = seeded[1]!;
      yield* database.sqlA.withTransaction(
        database.sqlA.unsafe(
          `UPDATE orchestration_events
           SET metadata_json = CAST(X'7B2261223A22FF227D' AS TEXT)
           WHERE aggregate_kind = 'thread' AND stream_id = ? AND stream_version = 0`,
          [invalid.evidence.threadId],
        ),
      );
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 preserves canonical Unicode history and recovery finalizes once", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded, plan } = yield* makeRecoverableMigration052CompletedPlanning(
        "legacy-canonical-unicode",
        "# Grüner Plan 🚀\n\n1. Straße prüfen.",
      );
      yield* database.sqlA.withTransaction(database.sqlA`
        UPDATE orchestration_events
        SET metadata_json = ${canonicalJson({
          note: "Grüße aus Köln 🚀",
          providerTurnId: seeded.providerTurnId,
        })}
        WHERE event_id = ${plan.eventId}
      `);
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* recovered.finalizer.recover;
      const finalized = yield* finalizationCounts(database.sqlB, seeded);
      assert.deepStrictEqual(finalized, {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(recovered.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);

      yield* recovered.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), finalized);
      assert.equal((yield* Ref.get(recovered.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);
    }),
  ),
);

it.effect(
  "migration 053 rejects an orchestration history that does not start at version zero",
  () =>
    withNode(
      Effect.gen(function* () {
        const fixture = yield* makeLegacyFinalization("legacy-history-start-version");
        yield* fixture.database.sqlA.withTransaction(fixture.database.sqlA`
        DELETE FROM orchestration_events
        WHERE aggregate_kind = 'thread'
          AND stream_id = ${fixture.seeded.evidence.threadId}
          AND stream_version = 0
      `);
        const before = yield* captureMigration053State(fixture.database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, fixture.database.sqlA),
          ),
        );

        assert.isTrue(Exit.isFailure(upgrade));
        assert.deepStrictEqual(yield* captureMigration053State(fixture.database.sqlA), before);
      }),
    ),
);

it.effect.each<{
  readonly corruption:
    | "sequence-regression"
    | "payload-whitespace"
    | "metadata-whitespace"
    | "invalid-payload-json"
    | "payload-blob"
    | "metadata-blob";
}>([
  { corruption: "sequence-regression" },
  { corruption: "payload-whitespace" },
  { corruption: "metadata-whitespace" },
  { corruption: "invalid-payload-json" },
  { corruption: "payload-blob" },
  { corruption: "metadata-blob" },
])("migration 053 rejects legacy intermediate orchestration $corruption", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const { database, plan, terminal } = yield* makeRecoverableMigration052CompletedPlanning(
        `legacy-intermediate-history-${corruption}`,
      );
      if (corruption === "sequence-regression") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events
          SET sequence = ${terminal.sequence + 100}
          WHERE event_id = ${plan.eventId}
        `);
      } else if (corruption === "payload-whitespace") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET payload_json = ' ' || payload_json
          WHERE event_id = ${plan.eventId}
        `);
      } else if (corruption === "metadata-whitespace") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET metadata_json = ' ' || metadata_json
          WHERE event_id = ${plan.eventId}
        `);
      } else if (corruption === "invalid-payload-json") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET payload_json = '{'
          WHERE event_id = ${plan.eventId}
        `);
      } else if (corruption === "payload-blob") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET payload_json = CAST(payload_json AS BLOB)
          WHERE event_id = ${plan.eventId}
        `);
      } else {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET metadata_json = CAST(metadata_json AS BLOB)
          WHERE event_id = ${plan.eventId}
        `);
      }
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("SQLite rejects duplicate orchestration event sequences before migration 053", () =>
  withNode(
    Effect.gen(function* () {
      const { database, plan, terminal } = yield* makeRecoverableMigration052CompletedPlanning(
        "legacy-duplicate-sequence",
      );
      const before = yield* captureMigration053State(database.sqlA);

      const mutation = yield* Effect.exit(
        database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events
          SET sequence = (
            SELECT sequence FROM orchestration_events WHERE event_id = ${terminal.eventId}
          )
          WHERE event_id = ${plan.eventId}
        `),
      );

      assert.isTrue(Exit.isFailure(mutation));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 rejects CHECK-inconsistent recoverable delivery state", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeRecoverableMigration052Planning(
        "legacy-check-inconsistent-delivery",
      );
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_delivery_transition_validate",
      ).unprepared;
      yield* database.sqlA`PRAGMA ignore_check_constraints = ON`;
      yield* database.sqlA.withTransaction(database.sqlA`
        UPDATE agent_control_initial_planning_deliveries
        SET claim_owner_id = 'stale-owner', claim_expires_at = ${expiresAt}
        WHERE handoff_id = ${seeded.evidence.handoffId}
      `);
      yield* database.sqlA`PRAGMA ignore_check_constraints = OFF`;
      const integrity = yield* database.sqlA<{ readonly integrity_check: string }>`
        PRAGMA integrity_check
      `;
      assert.notEqual(integrity[0]?.integrity_check, "ok");
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect.each<{
  readonly corruption:
    | "claim-expiry"
    | "next-attempt"
    | "interrupt-storage"
    | "session-created"
    | "resume-cursor"
    | "terminal-time"
    | "last-error"
    | "claim-generation"
    | "attempt-count";
}>([
  { corruption: "claim-expiry" },
  { corruption: "next-attempt" },
  { corruption: "interrupt-storage" },
  { corruption: "session-created" },
  { corruption: "resume-cursor" },
  { corruption: "terminal-time" },
  { corruption: "last-error" },
  { corruption: "claim-generation" },
  { corruption: "attempt-count" },
])("migration 053 rejects legacy delivery $corruption corruption", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-delivery-${corruption}`,
      );
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_delivery_transition_validate",
      ).unprepared;
      yield* database.sqlA`PRAGMA ignore_check_constraints = ON`;
      if (corruption === "claim-expiry") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET claim_expires_at = ${expiresAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "next-attempt") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET next_attempt_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "interrupt-storage") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET interrupt_requested = CAST('0' AS TEXT)
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "session-created") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET provider_session_created_at = NULL
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "resume-cursor") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET provider_resume_cursor_json = CAST('null' AS BLOB)
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "terminal-time") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET terminal_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "last-error") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET last_error_code = 'provider-defect'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "claim-generation") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET claim_generation = 0
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET attempt_count = 0
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      }
      yield* database.sqlA`PRAGMA ignore_check_constraints = OFF`;
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 rejects a terminal candidate with a foreign provider name", () =>
  withNode(
    Effect.gen(function* () {
      const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
        "legacy-foreign-terminal-provider-name",
      );
      yield* appendPlan(database.sqlA, seeded, "legacy-foreign-terminal-provider-name");
      yield* appendProviderTerminal(
        database.sqlA,
        seeded,
        "legacy-foreign-terminal-provider-name",
        "completed",
        { providerName: "foreign-provider" },
      );
      yield* markTerminal(harness.store, seeded, "completed");
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect.each<{
  readonly corruption: "command" | "actor" | "order" | "conflict";
}>([
  { corruption: "command" },
  { corruption: "actor" },
  { corruption: "order" },
  { corruption: "conflict" },
])("migration 053 rejects terminal candidate $corruption corruption", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-terminal-${corruption}`,
      );
      yield* appendPlan(database.sqlA, seeded, `legacy-terminal-${corruption}`);
      const terminal = yield* appendProviderTerminal(
        database.sqlA,
        seeded,
        `legacy-terminal-${corruption}`,
        "completed",
      );
      yield* markTerminal(harness.store, seeded, "completed");
      if (corruption === "command") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET command_id = 'client:foreign-terminal'
          WHERE event_id = ${terminal.eventId}
        `);
      } else if (corruption === "actor") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET actor_kind = 'server'
          WHERE event_id = ${terminal.eventId}
        `);
      } else if (corruption === "order") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE orchestration_events SET sequence = -1
          WHERE event_id = ${terminal.eventId}
        `);
      } else {
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          "legacy-terminal-conflicting-candidate",
          "completed",
          { lastError: "conflicting terminal evidence" },
        );
      }
      const before = yield* captureMigration053State(database.sqlA);

      const upgrade = yield* Effect.exit(
        runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
      );

      assert.isTrue(Exit.isFailure(upgrade));
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 accepts interrupted error evidence and recovery finalizes once", () =>
  withNode(
    Effect.gen(function* () {
      const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
        "legacy-interrupted-error",
      );
      yield* appendProviderTerminal(
        database.sqlA,
        seeded,
        "legacy-interrupted-error",
        "interrupted",
        { status: "error" },
      );
      const interruptRequested = yield* harness.store.requestInterrupt({
        handoffId: seeded.evidence.handoffId,
        expectedRevision: 4,
        requestedAt: terminalAt,
      });
      yield* harness.store.markTerminal({
        handoffId: seeded.evidence.handoffId,
        expectedRevision: interruptRequested.revision,
        state: "interrupted",
        terminalAt,
        errorCode: "provider-aborted",
      });

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* recovered.finalizer.recover;
      const finalized = yield* finalizationCounts(database.sqlB, seeded);
      assert.deepStrictEqual(finalized, {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.deepStrictEqual(
        (yield* Ref.get(recovered.stagePublished)).map((event) => event.payload.status),
        ["cancelled"],
      );
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);

      yield* recovered.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), finalized);
      assert.equal((yield* Ref.get(recovered.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);
    }),
  ),
);

it.effect.each<{
  readonly delivery: "completed" | "failed" | "interrupted";
  readonly expectedStage: "succeeded" | "failed" | "cancelled";
}>([
  { delivery: "completed", expectedStage: "succeeded" },
  { delivery: "failed", expectedStage: "failed" },
  { delivery: "interrupted", expectedStage: "cancelled" },
])(
  "migration 053 preserves a recoverable $delivery delivery and recovery finalizes it once",
  ({ delivery, expectedStage }) =>
    withNode(
      Effect.gen(function* () {
        const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
          `legacy-recoverable-${delivery}`,
        );
        if (delivery === "completed") {
          yield* appendPlan(database.sqlA, seeded, `legacy-recoverable-${delivery}`);
        }
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `legacy-recoverable-${delivery}`,
          delivery,
        );
        if (delivery === "interrupted") {
          const interruptRequested = yield* harness.store.requestInterrupt({
            handoffId: seeded.evidence.handoffId,
            expectedRevision: 4,
            requestedAt: terminalAt,
          });
          yield* harness.store.markTerminal({
            handoffId: seeded.evidence.handoffId,
            expectedRevision: interruptRequested.revision,
            state: delivery,
            terminalAt,
            errorCode: "provider-aborted",
          });
        } else {
          yield* markTerminal(harness.store, seeded, delivery);
        }
        const [counters] = yield* database.sqlA<{
          readonly claimGeneration: number;
          readonly attemptCount: number;
        }>`
          SELECT claim_generation AS "claimGeneration", attempt_count AS "attemptCount"
          FROM agent_control_initial_planning_deliveries
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `;
        assert.deepStrictEqual(counters, { claimGeneration: 1, attemptCount: 1 });
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
          stageEvents: 2,
          leaseEvents: 1,
          started: 1,
          evidence: 0,
          receipts: 0,
          markers: 0,
        });

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
          [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
        );

        const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
        yield* recovered.finalizer.recover;
        const finalized = yield* finalizationCounts(database.sqlB, seeded);
        assert.deepStrictEqual(finalized, {
          stageEvents: 3,
          leaseEvents: 2,
          started: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.deepStrictEqual(
          (yield* Ref.get(recovered.stagePublished)).map((event) => event.payload.status),
          [expectedStage],
        );
        assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);

        yield* recovered.finalizer.recover;
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), finalized);
        assert.equal((yield* Ref.get(recovered.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);
      }),
    ),
);

it.effect.each<{
  readonly successor:
    | "provider-started"
    | "interrupt-requested"
    | "ambiguous"
    | "interrupt-requested-ambiguous";
}>([
  { successor: "provider-started" },
  { successor: "interrupt-requested" },
  { successor: "ambiguous" },
  { successor: "interrupt-requested-ambiguous" },
])("migration 053 accepts the recoverable $successor successor", ({ successor }) =>
  withNode(
    Effect.gen(function* () {
      const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-successor-${successor}`,
      );
      if (successor === "interrupt-requested") {
        yield* harness.store.requestInterrupt({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          requestedAt: terminalAt,
        });
      } else if (successor === "ambiguous") {
        yield* harness.store.markAmbiguous({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          terminalAt,
        });
      } else if (successor === "interrupt-requested-ambiguous") {
        const interruptRequested = yield* harness.store.requestInterrupt({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          requestedAt: terminalAt,
        });
        yield* harness.store.markAmbiguous({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: interruptRequested.revision,
          terminalAt,
        });
      }
      const before = yield* finalizationCounts(database.sqlA, seeded);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* recovered.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), before);
      assert.equal((yield* Ref.get(recovered.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{
  readonly corruption: "provider-turn" | "revision-backward" | "revision-gap" | "illegal-state";
}>([
  { corruption: "provider-turn" },
  { corruption: "revision-backward" },
  { corruption: "revision-gap" },
  { corruption: "illegal-state" },
])("migration 053 rejects a recoverable delivery with $corruption corruption", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-successor-${corruption}`,
      );
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_delivery_transition_validate",
      ).unprepared;
      if (corruption === "provider-turn") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET provider_turn_id = 'foreign-provider-turn'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "revision-backward") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET revision = 3
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "revision-gap") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET state = 'completed', revision = 6,
            terminal_at = ${terminalAt}, updated_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET state = 'retry-wait', revision = 5,
            provider_turn_id = NULL, provider_accepted_at = NULL,
            next_attempt_at = ${terminalAt}, last_error_code = 'transient-not-accepted',
            updated_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      }
      const before = yield* captureMigration053State(database.sqlA);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sqlA),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 rejects a partially present legacy result chain", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeLegacyFinalization("legacy-partial-result-chain");
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_finalization_markers_no_delete",
      ).unprepared;
      yield* database.sqlA.withTransaction(database.sqlA`
        DELETE FROM agent_control_initial_planning_finalization_markers
        WHERE handoff_id = ${seeded.evidence.handoffId}
      `);
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
      const before = yield* captureMigration053State(database.sqlA);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sqlA),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect(
  "rejects mismatched legacy planning companion evidence before migration 053 hardening",
  () =>
    withNode(
      Effect.gen(function* () {
        const { database, seeded } = yield* makeLegacyFinalization("legacy-project-mismatch");
        yield* database.sqlA.unsafe(
          "DROP TRIGGER agent_control_initial_planning_stage_started_no_update",
        ).unprepared;
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_stage_started
          SET project_id = 'legacy-foreign-project'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
        yield* database.sqlA.unsafe(
          `CREATE TRIGGER agent_control_initial_planning_stage_started_no_update
           BEFORE UPDATE ON agent_control_initial_planning_stage_started
           BEGIN
             SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable');
           END`,
        ).unprepared;
        const before = yield* captureMigration053State(database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
        );

        assert.isTrue(Exit.isFailure(upgrade));
        assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
      }),
    ),
);

it.effect("validates all legacy companion rows and rolls migration 053 back atomically", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeLegacyFinalizations([
        "legacy-validation-a",
        "legacy-validation-b",
        "legacy-validation-c",
      ]);
      const sql = database.sqlA;
      const valid = yield* captureMigration053State(sql);
      const target = seeded.toSorted((left, right) =>
        left.evidence.handoffId.localeCompare(right.evidence.handoffId),
      )[1]!;
      const rowFor = (rows: ReadonlyArray<Record<string, unknown>>) =>
        rows.find((row) => row.handoff_id === target.evidence.handoffId)!;
      const original = {
        stage: rowFor(valid.stageStarted),
        result: rowFor(valid.resultEvidence),
        receipt: rowFor(valid.receipts),
        marker: rowFor(valid.markers),
      };
      const mutations: ReadonlyArray<LegacyMutation> = [
        "stage-project",
        "stage-task",
        "stage-thread",
        "stage-provider",
        "stage-provider-turn",
        "stage-run",
        "stage-attempt",
        "stage-lease",
        "stage-holder",
        "stage-fence",
        "stage-sequence",
        "stage-revision",
        "result-outcome",
        "result-plan-digest",
        "result-stage-coordinate",
        "result-lease-coordinate",
        "result-storage-class",
        "result-noncanonical-json",
        "receipt-command",
        "receipt-fingerprint",
        "receipt-handoff",
        "receipt-outcome",
        "receipt-coordinate",
        "marker-incomplete",
        "foreign-key",
      ];
      for (const mutation of mutations) {
        yield* mutateLegacyEvidence(sql, target.evidence.handoffId, mutation, original);
        const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
        if (mutation === "foreign-key") assert.isAbove(foreignKeys.length, 0);
        const before = yield* captureMigration053State(sql);
        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
        );
        assert.isTrue(Exit.isFailure(upgrade), mutation);
        if (mutation === "foreign-key" && Exit.isFailure(upgrade)) {
          const rendered = Cause.pretty(upgrade.cause);
          assert.include(rendered, "agent_control_initial_planning_stage_started");
          assert.include(rendered, "orchestration_events");
        }
        assert.deepStrictEqual(yield* captureMigration053State(sql), before, mutation);
        yield* mutateLegacyEvidence(sql, target.evidence.handoffId, mutation, original, true);
      }

      const [leftMarker, rightMarker] = valid.markers;
      yield* swapLegacyMarkerEvidence(sql, leftMarker!, rightMarker!);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      const mismatchedMarkers = yield* captureMigration053State(sql);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(sql), mismatchedMarkers);
      yield* swapLegacyMarkerEvidence(sql, leftMarker!, rightMarker!, true);

      const restored = yield* captureMigration053State(sql);
      assert.deepStrictEqual(restored.stageStarted, valid.stageStarted);
      assert.deepStrictEqual(restored.resultEvidence, valid.resultEvidence);
      assert.deepStrictEqual(restored.receipts, valid.receipts);
      assert.deepStrictEqual(restored.markers, valid.markers);
      assert.deepStrictEqual(restored.sequences, valid.sequences);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const hardened = yield* captureMigration053State(sql);
      assert.deepStrictEqual(hardened.stageStarted, valid.stageStarted);
      assert.deepStrictEqual(hardened.resultEvidence, valid.resultEvidence);
      assert.deepStrictEqual(hardened.receipts, valid.receipts);
      assert.deepStrictEqual(hardened.markers, valid.markers);
      assert.deepStrictEqual(hardened.sequences, valid.sequences);
      assert.equal(hardened.migrations.filter((row) => row.migration_id === 53).length, 1);
      const hardenedTriggerNames = new Set(hardened.triggers.map((row) => row.name));
      for (const trigger of migration053HardeningTriggers) {
        assert.isTrue(hardenedTriggerNames.has(trigger), trigger);
      }
    }),
  ),
);

it.effect(
  "finalizes plan-before-completion exactly once and replays duplicate notifications receipt-first",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const afterCommitCounts = yield* Ref.make<ReadonlyArray<number>>([]);
        const publicationCount = yield* Ref.make(0);
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
          ...noopHooks,
          afterNativeCommit: () =>
            database.sqlA<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_initial_planning_finalization_receipts
          `.pipe(
              Effect.flatMap((rows) =>
                Ref.update(afterCommitCounts, (current) => [...current, rows[0]!.count]),
              ),
              Effect.orDie,
            ),
          afterPublication: () => Ref.update(publicationCount, (count) => count + 1),
        });
        const seeded = yield* seedPlanning(database.sqlA, harness, "plan-first");
        yield* appendProviderStart(database.sqlA, seeded, "plan-first");
        const started = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(started._tag, "Started");
        yield* appendPlan(database.sqlA, seeded, "plan-first");
        yield* appendProviderTerminal(database.sqlA, seeded, "plan-first", "completed");
        yield* markTerminal(harness.store, seeded, "completed");

        const finalized = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(finalized._tag, "Finalized");
        const replayed = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(replayed._tag, "Replayed");
        const counts = yield* finalizationCounts(database.sqlA, seeded);
        assert.deepStrictEqual(counts, {
          stageEvents: 3,
          leaseEvents: 2,
          started: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.deepStrictEqual(yield* Ref.get(afterCommitCounts), [0, 1]);
        assert.equal(yield* Ref.get(publicationCount), 2);
        assert.deepStrictEqual(
          (yield* Ref.get(harness.stagePublished)).map((event) => event.type),
          ["agentControl.stageRun.planningStarted", "agentControl.stageRun.planningSucceeded"],
        );
        assert.deepStrictEqual(
          (yield* Ref.get(harness.leasePublished)).map((event) => event.type),
          ["agentControl.stageRunLease.releasedAfterPlanning"],
        );
      }),
    ),
);

it.effect("joins completion-before-plan after a fresh-layer restart", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "terminal-first");
      yield* appendProviderStart(database.sqlA, seeded, "terminal-first");
      yield* appendProviderTerminal(database.sqlA, seeded, "terminal-first", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const started = yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(started._tag, "Started");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 2,
        leaseEvents: 1,
        started: 1,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });

      yield* appendPlan(database.sqlA, seeded, "terminal-first");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const finalized = yield* harnessB.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(finalized._tag, "Finalized");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 1);
    }),
  ),
);

it.effect("fails closed when a second plan commits after the authoritative snapshot", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const authoritativeRead = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(authoritativeRead, undefined);
            yield* Deferred.await(releaseFinalizer);
          }),
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "late-second-plan");
      yield* appendProviderStart(database.sqlA, seeded, "late-second-plan");
      yield* appendPlan(database.sqlA, seeded, "late-second-plan-a");
      yield* appendProviderTerminal(database.sqlA, seeded, "late-second-plan", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");

      const fiber = yield* harnessA.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(authoritativeRead).pipe(Effect.timeout(barrierTimeout));
      yield* appendPlan(database.sqlB, seeded, "late-second-plan-b");
      yield* Deferred.succeed(releaseFinalizer, undefined);
      const raced = yield* Fiber.join(fiber).pipe(Effect.timeout(barrierTimeout));
      assert.isTrue(Exit.isFailure(raced));

      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const retry = yield* Effect.exit(
        harnessB.finalizer.processHandoff(seeded.evidence.handoffId),
      );
      assert.isTrue(Exit.isFailure(retry));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessA.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
    }),
  ),
);

it.effect(
  "startup recovery closes the finalized-before-subscription window without replay effects",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const { setup, implementation } = yield* prepareSucceededImplementationFinalization(
            database,
            planningFinalizer,
            "verification-admission-startup-recovery",
          );
          const first = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer,
            implementationFinalizer: setup.finalizer.finalizer,
            handoffStore: setup.coordinator.handoffStore,
            admissionHarness: setup.candidate.admissionHarness,
          });
          yield* Ref.set(planningFinalizer.stagePublished, []);
          yield* Ref.set(planningFinalizer.leasePublished, []);
          yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);
          yield* first.admission.start();
          yield* first.admission.drain;
          assert.deepStrictEqual(
            yield* database.sqlB<{
              readonly evidence: number;
              readonly receipts: number;
              readonly markers: number;
            }>`
            SELECT
              (SELECT count(*) FROM agent_control_verification_admission_evidence
                WHERE implementation_result_evidence_id = ${implementation.resultEvidenceId})
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_admission_receipts
                WHERE implementation_result_evidence_id = ${implementation.resultEvidenceId})
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_admission_markers
                WHERE implementation_result_evidence_id = ${implementation.resultEvidenceId})
                AS markers
          `,
            [{ evidence: 1, receipts: 1, markers: 1 }],
          );
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
          assert.equal(
            (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
            1,
          );

          const restartHookCalls = yield* Ref.make(0);
          const restarted = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer,
            implementationFinalizer: setup.finalizer.finalizer,
            handoffStore: setup.coordinator.handoffStore,
            admissionHarness: setup.candidate.admissionHarness,
            hooks: {
              ...noopVerificationAdmissionHooks,
              afterNativeCommit: () => Ref.update(restartHookCalls, (count) => count + 1),
              afterPublication: () => Ref.update(restartHookCalls, (count) => count + 1),
            },
          });
          yield* restarted.admission.start();
          yield* restarted.admission.drain;
          assert.equal(yield* Ref.get(restartHookCalls), 0);
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
          assert.equal(
            (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
            1,
          );
        }),
      ),
    ),
);

it.effect("acquires both verification wakeup subscriptions before startup recovery", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const { setup, claim } = yield* prepareSucceededImplementationForFinalization(
          database,
          planningFinalizer,
          "verification-admission-subscriptions-ready",
        );
        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionHarnessB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          setup.candidate.task,
          setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const resultEvidenceId = deriveImplementationResultEvidenceId(
          claim.evidence.handoffId,
          claim.evidence.handoffFingerprint,
        );
        const finalizerSubscriptionAcquired = yield* Deferred.make<void>();
        const stageRunSubscriptionAcquired = yield* Deferred.make<void>();
        const recoveryBlocked = yield* Deferred.make<void>();
        const admissionPublished = yield* Deferred.make<void>();
        const releaseRecovery = yield* Deferred.make<void>();
        const publicationCount = yield* Ref.make(0);
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlB,
          scope: database.scopeB,
          planningFinalizer: planningFinalizerB,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: admissionHarnessB,
          stageEngine: planningFinalizer.stageEngine,
          hooks: {
            ...noopVerificationAdmissionHooks,
            afterFinalizerSubscriptionAcquired: () =>
              Deferred.succeed(finalizerSubscriptionAcquired, undefined),
            afterStageRunSubscriptionAcquired: () =>
              Deferred.succeed(stageRunSubscriptionAcquired, undefined),
            beforeStartupRecovery: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(recoveryBlocked, undefined);
                yield* Deferred.await(releaseRecovery);
              }),
            afterPublication: () =>
              Ref.update(publicationCount, (count) => count + 1).pipe(
                Effect.andThen(Deferred.succeed(admissionPublished, undefined)),
              ),
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizerB.leasePublished, []);
        yield* Ref.set(admissionHarnessB.reservationPublished, []);

        const startFiber = yield* verification.admission
          .start()
          .pipe(Effect.result, Effect.forkChild);
        yield* Effect.gen(function* () {
          yield* Deferred.await(finalizerSubscriptionAcquired).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.await(stageRunSubscriptionAcquired).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.await(recoveryBlocked).pipe(Effect.timeout(barrierTimeout));
          const finalized = yield* setup.finalizer.finalizer.processHandoff(setup.handoffId);
          assert.equal(finalized._tag, "Finalized");
          yield* Deferred.await(admissionPublished).pipe(Effect.timeout(barrierTimeout));
        }).pipe(Effect.ensuring(Deferred.succeed(releaseRecovery, undefined)));
        const started = yield* Fiber.join(startFiber).pipe(Effect.timeout(barrierTimeout));
        assert.equal(started._tag, "Success");
        yield* verification.admission.drain;

        assert.deepStrictEqual(yield* verificationAdmissionCounts(database.sqlA), {
          stageEvents: 1,
          leaseEvents: 1,
          reservationEvents: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.deepStrictEqual(
          yield* database.sqlA<{ readonly resultEvidenceId: string }>`
            SELECT implementation_result_evidence_id AS "resultEvidenceId"
            FROM agent_control_verification_admission_evidence
          `,
          [{ resultEvidenceId }],
        );
        assert.equal(yield* Ref.get(publicationCount), 1);
        assert.equal(
          (yield* Ref.get(planningFinalizer.stagePublished)).filter(
            (event) => event.payload.stageKind === "verification",
          ).length,
          1,
        );
        assert.equal((yield* Ref.get(admissionHarnessB.reservationPublished)).length, 1);
      }),
    ),
  ),
);

it.effect("an independent WAL wakeup cannot observe uncommitted Implementation finalization", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = "verification-admission-uncommitted-finalization";
        const database = yield* makeSharedDatabase();
        const planningFinalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const initialTask = admissionTask(suffix);
        const canonicalTask = {
          ...initialTask,
          taskId: yield* deriveAgentControlTaskId(initialTask.source),
        };
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizerA,
          suffix,
          true,
          canonicalTask,
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`provider:${suffix}:implementation-terminal`),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });
        const markerReached = yield* Deferred.make<void>();
        const releaseMarker = yield* Deferred.make<void>();
        const finalizerA = yield* buildImplementationStageFinalizer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          planningFinalizer: planningFinalizerA,
          starter: setup.starter,
          hooks: {
            ...noopImplementationStageFinalizerHooks,
            beforeFinalMarker: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(markerReached, undefined);
                yield* Deferred.await(releaseMarker);
              }),
          },
        });

        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionHarnessB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          setup.candidate.task,
          setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const coordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: `${suffix}-b`,
          admission: admissionHarnessB.admission,
          finalizer: planningFinalizerB,
          admissionHarness: admissionHarnessB,
          task: setup.candidate.task,
          worktree: setup.candidate.worktree,
        });
        const starterB = yield* buildImplementationStageStarter({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          finalizer: planningFinalizerB,
        });
        const finalizerB = yield* buildImplementationStageFinalizer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          planningFinalizer: planningFinalizerB,
          starter: starterB,
        });
        const verificationB = yield* buildVerificationAdmission({
          sql: database.sqlB,
          scope: database.scopeB,
          planningFinalizer: planningFinalizerB,
          implementationFinalizer: finalizerB.finalizer,
          handoffStore: coordinatorB.handoffStore,
          admissionHarness: admissionHarnessB,
        });
        const resultEvidenceId = deriveImplementationResultEvidenceId(
          claim.evidence.handoffId,
          claim.evidence.handoffFingerprint,
        );
        yield* Ref.set(planningFinalizerA.stagePublished, []);
        yield* Ref.set(planningFinalizerA.leasePublished, []);
        yield* Ref.set(planningFinalizerB.stagePublished, []);
        yield* Ref.set(planningFinalizerB.leasePublished, []);
        yield* Ref.set(admissionHarnessB.reservationPublished, []);
        const finalizationFiber = yield* finalizerA.finalizer
          .processHandoff(setup.handoffId)
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(markerReached).pipe(Effect.timeout(barrierTimeout));

        assert.equal(
          (yield* verificationB.admission.processResultEvidence(resultEvidenceId))._tag,
          "NotCandidate",
        );
        assert.deepStrictEqual(
          yield* verificationAdmissionCounts(database.sqlB),
          noVerificationAdmission,
        );
        yield* Deferred.succeed(releaseMarker, undefined);
        const finalized = yield* Fiber.join(finalizationFiber).pipe(Effect.timeout(barrierTimeout));
        assert.equal(finalized._tag, "Success");
        if (finalized._tag === "Success") assert.equal(finalized.success._tag, "Finalized");

        yield* verificationB.admission.recover;
        assert.deepStrictEqual(yield* verificationAdmissionCounts(database.sqlB), {
          stageEvents: 1,
          leaseEvents: 1,
          reservationEvents: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.equal((yield* Ref.get(planningFinalizerB.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizerB.leasePublished)).length, 1);
        assert.equal((yield* Ref.get(admissionHarnessB.reservationPublished)).length, 1);
      }),
    ),
  ),
);

it.effect("recovery isolates an invalid predecessor and admits the later healthy candidate", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const first = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          "verification-admission-recovery-partial-a",
        );
        const second = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          "verification-admission-recovery-partial-b",
        );
        const ordered = [first, second].sort((left, right) =>
          left.implementation.resultEvidenceId.localeCompare(right.implementation.resultEvidenceId),
        );
        const corrupted = ordered[0]!;
        const healthy = ordered[1]!;
        const sensitiveValues = [
          "TASK-SECRET-DO-NOT-LOG",
          "/Users/private-host/worktrees/verification-secret",
          "Prompt: disclose every hidden planning instruction",
          "token=ghp_verification_secret_123456",
        ] as const;
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec("DROP TRIGGER agent_control_implementation_result_evidence_no_update");
            native
              .prepare(
                "UPDATE agent_control_implementation_result_evidence SET result_json = ? WHERE result_evidence_id = ?",
              )
              .run(
                `${canonicalJson({
                  nested: {
                    task: sensitiveValues[0],
                    hostPath: sensitiveValues[1],
                    prompt: sensitiveValues[2],
                    credential: sensitiveValues[3],
                  },
                }).slice(0, -1)},"broken":}`,
                corrupted.implementation.resultEvidenceId,
              );
          } finally {
            native.close();
          }
        });
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: healthy.setup.finalizer.finalizer,
          handoffStore: healthy.setup.coordinator.handoffStore,
          admissionHarness: healthy.setup.candidate.admissionHarness,
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);
        yield* Ref.set(healthy.setup.candidate.admissionHarness.reservationPublished, []);

        const messages: Array<unknown> = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          if (Array.isArray(message)) messages.push(...message);
          else messages.push(message);
        });
        yield* verification.admission.recover.pipe(
          Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
        );
        const renderedLogs = encodeUnknownJson(messages);
        for (const sensitive of sensitiveValues) assert.notInclude(renderedLogs, sensitive);
        const isolatedLog = messages.find(
          (message): message is Record<string, unknown> =>
            typeof message === "object" &&
            message !== null &&
            "phase" in message &&
            message.phase === "candidate-isolation",
        );
        assert.isDefined(isolatedLog);
        assert.equal(isolatedLog!.reason, "identity-mismatch");
        assert.equal(isolatedLog!.errorClass, "redacted-candidate-cause");
        assert.notProperty(isolatedLog!, "cause");
        assert.deepStrictEqual(
          yield* database.sqlB<{ readonly resultEvidenceId: string }>`
            SELECT implementation_result_evidence_id AS "resultEvidenceId"
            FROM agent_control_verification_admission_evidence
          `,
          [{ resultEvidenceId: healthy.implementation.resultEvidenceId }],
        );
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
        assert.equal(
          (yield* Ref.get(healthy.setup.candidate.admissionHarness.reservationPublished)).length,
          1,
        );
      }),
    ),
  ),
);

it.effect("recovery isolates invalid UTF-8 outcomes within and beyond one page", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = yield* Effect.forEach(
          ["utf8-page-a", "utf8-page-b", "utf8-page-c"],
          (suffix) =>
            prepareSucceededImplementationFinalization(
              database,
              planningFinalizer,
              `verification-admission-recovery-${suffix}`,
            ),
          { concurrency: 1 },
        );
        const ordered = candidates.toSorted((left, right) =>
          left.implementation.resultEvidenceId.localeCompare(right.implementation.resultEvidenceId),
        );
        const corrupted = ordered[0]!;
        const healthy = ordered.slice(1);
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            // External-corruption probe: bypass only the immutable UPDATE trigger and
            // CHECK enforcement on this disposable connection; Migration 056 remains
            // unchanged.
            native.exec(
              "PRAGMA ignore_check_constraints = ON; DROP TRIGGER agent_control_implementation_result_evidence_no_update",
            );
            native
              .prepare(
                "UPDATE agent_control_implementation_result_evidence SET outcome = CAST(X'80' AS TEXT) WHERE result_evidence_id = ?",
              )
              .run(corrupted.implementation.resultEvidenceId);
          } finally {
            native.close();
          }
        });
        const publicationCount = yield* Ref.make(0);
        const authority = healthy.at(-1)!;
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: authority.setup.finalizer.finalizer,
          handoffStore: authority.setup.coordinator.handoffStore,
          admissionHarness: authority.setup.candidate.admissionHarness,
          hooks: {
            ...noopVerificationAdmissionHooks,
            recoveryPageSize: 2,
            afterPublication: () => Ref.update(publicationCount, (count) => count + 1),
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);
        yield* Ref.set(authority.setup.candidate.admissionHarness.reservationPublished, []);

        yield* verification.admission.recover;
        assert.deepStrictEqual(
          yield* database.sqlB<{ readonly resultEvidenceId: string }>`
            SELECT implementation_result_evidence_id AS "resultEvidenceId"
            FROM agent_control_verification_admission_evidence
            ORDER BY implementation_result_evidence_id
          `,
          healthy
            .map(({ implementation }) => ({
              resultEvidenceId: implementation.resultEvidenceId,
            }))
            .toSorted((left, right) => left.resultEvidenceId.localeCompare(right.resultEvidenceId)),
        );
        assert.deepStrictEqual(yield* verificationAdmissionCounts(database.sqlB), {
          stageEvents: 2,
          leaseEvents: 2,
          reservationEvents: 2,
          evidence: 2,
          receipts: 2,
          markers: 2,
        });
        assert.equal(yield* Ref.get(publicationCount), 2);
        const totalChangesBeforeReplay = (yield* database.sqlA<{
          readonly count: number;
        }>`SELECT total_changes() AS count`)[0]!.count;
        yield* verification.admission.recover;
        assert.equal(
          (yield* database.sqlA<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
            .count,
          totalChangesBeforeReplay,
        );
        assert.equal(yield* Ref.get(publicationCount), 2);
      }),
    ),
  ),
);

it.live("two fresh WAL verification admissions converge through the production Deferred seam", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const { setup, implementation } = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizerA,
          "verification-admission-race",
          true,
        );
        const arrivedA = yield* Deferred.make<void>();
        const releaseA = yield* Deferred.make<void>();
        const arrivedB = yield* Deferred.make<void>();
        const releaseB = yield* Deferred.make<void>();
        const verificationA = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer: planningFinalizerA,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: setup.candidate.admissionHarness,
          hooks: {
            ...noopVerificationAdmissionHooks,
            afterAuthoritativeRead: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(arrivedA, undefined);
                yield* Deferred.await(releaseA);
              }),
          },
        });
        yield* Ref.set(planningFinalizerA.stagePublished, []);
        yield* Ref.set(planningFinalizerA.leasePublished, []);
        yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);
        const fiberA = yield* verificationA.admission
          .processResultEvidence(implementation.resultEvidenceId)
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(arrivedA).pipe(Effect.timeout(barrierTimeout));

        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionHarnessB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          setup.candidate.task,
          setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const coordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: "verification-admission-race-b",
          admission: setup.candidate.admissionHarness.admission,
          finalizer: planningFinalizerB,
          admissionHarness: admissionHarnessB,
          task: setup.candidate.task,
          worktree: setup.candidate.worktree,
        });
        const starterB = yield* buildImplementationStageStarter({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          finalizer: planningFinalizerB,
        });
        const implementationFinalizerB = yield* buildImplementationStageFinalizer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          planningFinalizer: planningFinalizerB,
          starter: starterB,
        });
        const verificationB = yield* buildVerificationAdmission({
          sql: database.sqlB,
          scope: database.scopeB,
          planningFinalizer: planningFinalizerB,
          implementationFinalizer: implementationFinalizerB.finalizer,
          handoffStore: coordinatorB.handoffStore,
          admissionHarness: admissionHarnessB,
          hooks: {
            ...noopVerificationAdmissionHooks,
            afterAuthoritativeRead: () =>
              Effect.gen(function* () {
                yield* Deferred.succeed(arrivedB, undefined);
                yield* Deferred.await(releaseB);
              }),
          },
        });
        yield* Ref.set(planningFinalizerB.stagePublished, []);
        yield* Ref.set(planningFinalizerB.leasePublished, []);
        yield* Ref.set(admissionHarnessB.reservationPublished, []);

        const fiberB = yield* verificationB.admission
          .processResultEvidence(implementation.resultEvidenceId)
          .pipe(Effect.result, Effect.forkChild);
        yield* Deferred.await(arrivedB).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseA, undefined);
        const resultA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
        assert.equal(resultA._tag, "Success");
        if (resultA._tag === "Success") assert.equal(resultA.success._tag, "Admitted");
        yield* Deferred.succeed(releaseB, undefined);
        const resultB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
        assert.equal(resultB._tag, "Success");
        if (resultB._tag === "Success") assert.equal(resultB.success._tag, "Replayed");

        assert.deepStrictEqual(
          yield* database.sqlB<{
            readonly stageEvents: number;
            readonly leaseEvents: number;
            readonly reservationEvents: number;
            readonly evidence: number;
            readonly receipts: number;
            readonly markers: number;
          }>`
            SELECT
              (SELECT count(*) FROM agent_control_events
                WHERE aggregate_kind = 'stage-run'
                  AND json_extract(payload_json, '$.stageKind') = 'verification')
                AS "stageEvents",
              (SELECT count(*) FROM agent_control_events
                WHERE aggregate_kind = 'stage-run-lease'
                  AND json_extract(payload_json, '$.stageRunId') IN (
                    SELECT verification_stage_run_id
                    FROM agent_control_verification_admission_evidence
                  )) AS "leaseEvents",
              (SELECT count(*) FROM agent_control_events
                WHERE aggregate_kind = 'controlled-thread-reservation'
                  AND json_extract(payload_json, '$.stageKind') = 'verification')
                AS "reservationEvents",
              (SELECT count(*) FROM agent_control_verification_admission_evidence) AS evidence,
              (SELECT count(*) FROM agent_control_verification_admission_receipts) AS receipts,
              (SELECT count(*) FROM agent_control_verification_admission_markers) AS markers
          `,
          [
            {
              stageEvents: 1,
              leaseEvents: 1,
              reservationEvents: 1,
              evidence: 1,
              receipts: 1,
              markers: 1,
            },
          ],
        );
        assert.equal(
          (yield* Ref.get(planningFinalizerA.stagePublished)).length +
            (yield* Ref.get(planningFinalizerB.stagePublished)).length,
          1,
        );
        assert.equal(
          (yield* Ref.get(planningFinalizerA.leasePublished)).length +
            (yield* Ref.get(planningFinalizerB.leasePublished)).length,
          1,
        );
        assert.equal(
          (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length +
            (yield* Ref.get(admissionHarnessB.reservationPublished)).length,
          1,
        );
      }),
    ),
  ),
);

it.effect("rejoins provider-start and plan across restart before the terminal observation", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "restart-between");
      yield* appendProviderStart(database.sqlA, seeded, "restart-between");
      yield* appendPlan(database.sqlA, seeded, "restart-between");
      const started = yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(started._tag, "Started");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 2,
        leaseEvents: 1,
        started: 1,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });

      yield* appendProviderTerminal(database.sqlA, seeded, "restart-between", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* harnessB.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.deepStrictEqual(
        (yield* Ref.get(harnessB.stagePublished)).map((event) => event.type),
        ["agentControl.stageRun.planningSucceeded"],
      );
    }),
  ),
);

it.live("commits once when two fresh finalizers race on independent WAL connections", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const arrivedA = yield* Deferred.make<void>();
      const arrivedB = yield* Deferred.make<void>();
      const releaseA = yield* Deferred.make<void>();
      const releaseB = yield* Deferred.make<void>();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(arrivedA, undefined);
            yield* Deferred.await(releaseA);
          }),
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "two-finalizers");
      yield* appendProviderStart(database.sqlA, seeded, "two-finalizers");
      yield* appendPlan(database.sqlA, seeded, "two-finalizers");
      yield* appendProviderTerminal(database.sqlA, seeded, "two-finalizers", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB, {
        ...noopHooks,
        beforeTransactionComplete: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(arrivedB, undefined);
            yield* Deferred.await(releaseB);
          }),
      });

      const fiberB = yield* harnessB.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      const statusB = yield* Effect.race(
        Deferred.await(arrivedB).pipe(Effect.as({ _tag: "Arrived" as const })),
        Fiber.join(fiberB).pipe(Effect.map((result) => ({ _tag: "Completed" as const, result }))),
      ).pipe(Effect.timeout(barrierTimeout));
      assert.equal(statusB._tag, "Arrived");
      const fiberA = yield* harnessA.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      const statusA = yield* Effect.race(
        Deferred.await(arrivedA).pipe(Effect.as({ _tag: "Arrived" as const })),
        Fiber.join(fiberA).pipe(Effect.map((result) => ({ _tag: "Completed" as const, result }))),
      ).pipe(Effect.timeout(barrierTimeout));
      assert.equal(statusA._tag, "Arrived");
      yield* Deferred.succeed(releaseB, undefined);
      const resultB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
      yield* Deferred.succeed(releaseA, undefined);
      const resultA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
      assert.equal(resultA._tag, "Success");
      assert.equal(resultB._tag, "Success");
      if (resultA._tag === "Success" && resultB._tag === "Success") {
        assert.deepStrictEqual([resultA.success._tag, resultB.success._tag].sort(), [
          "Finalized",
          "Replayed",
        ]);
      }
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal(
        (yield* Ref.get(harnessA.stagePublished)).length +
          (yield* Ref.get(harnessB.stagePublished)).length,
        2,
      );
      assert.equal(
        (yield* Ref.get(harnessA.leasePublished)).length +
          (yield* Ref.get(harnessB.leasePublished)).length,
        1,
      );
    }),
  ),
);

it.effect.each<{
  readonly delivery: "failed" | "interrupted" | "ambiguous";
  readonly expectedResult: "Finalized" | "Ambiguous";
  readonly expectedStage: "failed" | "cancelled" | "running";
  readonly expectedStageEvents: number;
  readonly expectedLeaseEvents: number;
}>([
  {
    delivery: "failed",
    expectedResult: "Finalized",
    expectedStage: "failed",
    expectedStageEvents: 3,
    expectedLeaseEvents: 2,
  },
  {
    delivery: "interrupted",
    expectedResult: "Finalized",
    expectedStage: "cancelled",
    expectedStageEvents: 3,
    expectedLeaseEvents: 2,
  },
  {
    delivery: "ambiguous",
    expectedResult: "Ambiguous",
    expectedStage: "running",
    expectedStageEvents: 2,
    expectedLeaseEvents: 1,
  },
])(
  "$delivery maps to the exact stage and lease transition",
  ({ delivery, expectedResult, expectedStage, expectedStageEvents, expectedLeaseEvents }) =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
        const seeded = yield* seedPlanning(database.sqlA, harness, `outcome-${delivery}`, delivery);
        yield* appendProviderStart(database.sqlA, seeded, `outcome-${delivery}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `outcome-${delivery}`,
          delivery === "ambiguous" ? "interrupted" : delivery,
        );
        const result = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(result._tag, expectedResult);
        const [stage] = yield* database.sqlA<{ readonly status: string }>`
          SELECT status FROM agent_control_stage_run_states
          WHERE stage_run_id = ${seeded.stageRunId}
        `;
        const [lease] = yield* database.sqlA<{ readonly status: string }>`
          SELECT status FROM agent_control_stage_run_lease_states
          WHERE lease_id = ${seeded.leaseId}
        `;
        assert.equal(stage?.status, expectedStage);
        assert.equal(lease?.status, delivery === "ambiguous" ? "reserved" : "released");
        const counts = yield* finalizationCounts(database.sqlA, seeded);
        assert.equal(counts.stageEvents, expectedStageEvents);
        assert.equal(counts.leaseEvents, expectedLeaseEvents);
        assert.equal(counts.evidence, delivery === "ambiguous" ? 0 : 1);
        assert.equal(counts.receipts, delivery === "ambiguous" ? 0 : 1);
        assert.equal(counts.markers, delivery === "ambiguous" ? 0 : 1);
      }),
    ),
);

it.effect(
  "materializes, delivers, and starts one durable verification turn through running@2",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const suffix = "verification-turn-start-e2e";
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const prepared = yield* prepareSucceededImplementationFinalization(
            database,
            planningFinalizer,
            suffix,
          );
          const verificationAdmission = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer,
            implementationFinalizer: prepared.setup.finalizer.finalizer,
            handoffStore: prepared.setup.coordinator.handoffStore,
            admissionHarness: prepared.setup.candidate.admissionHarness,
          });
          assert.equal(
            (yield* verificationAdmission.admission.processResultEvidence(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Admitted",
          );
          const [leaseBefore] = yield* database.sqlA<{
            readonly revision: number;
            readonly holderId: string;
            readonly fenceToken: number;
          }>`
            SELECT revision, holder_id AS "holderId", fence_token AS "fenceToken"
            FROM agent_control_stage_run_lease_states
            WHERE lease_id = ${prepared.implementation.leaseId}
          `;
          assert.isDefined(leaseBefore);
          const coordinator = yield* buildVerificationTurnCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            admission: verificationAdmission.admission,
            planningFinalizer,
            admissionHarness: prepared.setup.candidate.admissionHarness,
            task: prepared.setup.candidate.task,
            worktree: prepared.setup.candidate.worktree,
            orchestration: prepared.setup.coordinator.orchestration,
            snapshots: prepared.setup.coordinator.snapshots,
          });
          assert.equal(
            (yield* coordinator.coordinator.processHandoff(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Materialized",
          );
          const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
            SELECT accepted.handoff_id AS "handoffId"
            FROM agent_control_verification_handoff_accepted accepted
            JOIN agent_control_verification_materialization_evidence materialization
              ON materialization.materialization_evidence_id = accepted.materialization_evidence_id
            WHERE materialization.implementation_result_evidence_id =
              ${prepared.implementation.resultEvidenceId}
          `;
          assert.isDefined(handoff);
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator,
            executorCalls,
          });
          yield* consumer.processHandoff(handoff!.handoffId);
          assert.equal(yield* Ref.get(executorCalls), 1);
          const starter = yield* buildVerificationStageStarter({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator,
            planningFinalizer,
          });
          assert.equal((yield* starter.processHandoff(handoff!.handoffId))._tag, "Started");

          const [state] = yield* database.sqlB<{
            readonly reservationStatus: string;
            readonly reservationRevision: number;
            readonly turnAccepted: number;
            readonly deliveryState: string;
            readonly deliveryRevision: number;
            readonly providerInstanceId: string;
            readonly providerTurnId: string;
            readonly runtimeMode: string;
            readonly sessionEvidence: number;
            readonly deliveryAttestations: number;
            readonly stageStatus: string;
            readonly stageRevision: number;
            readonly stageStartedEvents: number;
            readonly stageEvidence: number;
            readonly stageReceipts: number;
            readonly stageMarkers: number;
            readonly leaseRevision: number;
            readonly leaseHolderId: string;
            readonly leaseFenceToken: number;
          }>`
            SELECT reservation.status AS "reservationStatus",
              reservation.revision AS "reservationRevision",
              (SELECT count(*) FROM agent_control_verification_turn_accepted)
                AS "turnAccepted",
              delivery.state AS "deliveryState", delivery.revision AS "deliveryRevision",
              delivery.provider_instance_id AS "providerInstanceId",
              delivery.provider_turn_id AS "providerTurnId",
              delivery.runtime_mode AS "runtimeMode",
              (SELECT count(*) FROM agent_control_verification_session_evidence)
                AS "sessionEvidence",
              (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                AS "deliveryAttestations",
              stage.status AS "stageStatus", stage.revision AS "stageRevision",
              (SELECT count(*) FROM agent_control_events
                WHERE event_type = 'agentControl.stageRun.verificationStarted')
                AS "stageStartedEvents",
              (SELECT count(*) FROM agent_control_verification_stage_started_evidence)
                AS "stageEvidence",
              (SELECT count(*) FROM agent_control_verification_stage_started_receipts)
                AS "stageReceipts",
              (SELECT count(*) FROM agent_control_verification_stage_started_markers)
                AS "stageMarkers",
              lease.revision AS "leaseRevision", lease.holder_id AS "leaseHolderId",
              lease.fence_token AS "leaseFenceToken"
            FROM agent_control_verification_deliveries delivery
            JOIN agent_control_verification_thread_reservation_states reservation
              ON reservation.controlled_thread_reservation_id =
                delivery.controlled_thread_reservation_id
            JOIN agent_control_stage_run_states stage
              ON stage.stage_run_id = delivery.stage_run_id
            JOIN agent_control_stage_run_lease_states lease
              ON lease.lease_id = delivery.lease_id
            WHERE delivery.handoff_id = ${handoff!.handoffId}
          `;
          assert.deepStrictEqual(state, {
            reservationStatus: "bound",
            reservationRevision: 3,
            turnAccepted: 1,
            deliveryState: "provider-started",
            deliveryRevision: 4,
            providerInstanceId: "verification-test-provider",
            providerTurnId: "verification-provider-turn",
            runtimeMode: "approval-required",
            sessionEvidence: 1,
            deliveryAttestations: 1,
            stageStatus: "running",
            stageRevision: 2,
            stageStartedEvents: 1,
            stageEvidence: 1,
            stageReceipts: 1,
            stageMarkers: 1,
            leaseRevision: leaseBefore!.revision,
            leaseHolderId: leaseBefore!.holderId,
            leaseFenceToken: leaseBefore!.fenceToken,
          });
          const [versions] = yield* database.sqlB<{
            readonly messageVersion: number;
            readonly turnVersion: number;
            readonly interactionMode: string;
            readonly sourceThreadId: string;
            readonly sourcePlanId: string;
          }>`
            SELECT message.stream_version AS "messageVersion",
              turn_event.stream_version AS "turnVersion",
              json_extract(turn_event.payload_json, '$.interactionMode') AS "interactionMode",
              json_extract(turn_event.payload_json, '$.sourceProposedPlan.threadId')
                AS "sourceThreadId",
              json_extract(turn_event.payload_json, '$.sourceProposedPlan.planId')
                AS "sourcePlanId"
            FROM agent_control_verification_turn_accepted accepted
            JOIN orchestration_events message ON message.event_id = accepted.message_event_id
            JOIN orchestration_events turn_event
              ON turn_event.event_id = accepted.turn_request_event_id
            WHERE accepted.handoff_id = ${handoff!.handoffId}
          `;
          const claim = Option.getOrThrow(
            yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
          );
          assert.deepStrictEqual(versions, {
            messageVersion: 3,
            turnVersion: 4,
            interactionMode: "default",
            sourceThreadId: claim.evidence.planningThreadId,
            sourcePlanId: claim.evidence.planId,
          });

          const changesBeforeReplay = (yield* database.sqlA<{
            readonly count: number;
          }>`SELECT total_changes() AS count`)[0]!.count;
          assert.equal(
            (yield* coordinator.coordinator.processHandoff(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Replayed",
          );
          yield* consumer.processHandoff(handoff!.handoffId);
          assert.equal((yield* starter.processHandoff(handoff!.handoffId))._tag, "Replayed");
          assert.equal(yield* Ref.get(executorCalls), 1);
          assert.equal(
            (yield* database.sqlA<{
              readonly count: number;
            }>`SELECT total_changes() AS count`)[0]!.count,
            changesBeforeReplay,
          );
        }),
      ),
    ),
);

it.effect(
  "discards unactivated Verification workers and reloads durable recovery with fresh attempts",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const suffix = "verification-attempt-worker-ownership";
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const prepared = yield* prepareSucceededImplementationFinalization(
            database,
            planningFinalizer,
            suffix,
          );
          const admission = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer,
            implementationFinalizer: prepared.setup.finalizer.finalizer,
            handoffStore: prepared.setup.coordinator.handoffStore,
            admissionHarness: prepared.setup.candidate.admissionHarness,
          });
          assert.equal(
            (yield* admission.admission.processResultEvidence(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Admitted",
          );
          const coordinatorRecoveries = yield* Ref.make(0);
          const coordinatorPublications = yield* Ref.make(0);
          const blockFailedCoordinator = yield* Ref.make(true);
          const failedCoordinatorEntered = yield* Deferred.make<void>();
          const failCoordinatorFatally = yield* Ref.make(false);
          const fatalCoordinatorEntered = yield* Deferred.make<void>();
          const releaseFatalCoordinator = yield* Deferred.make<void>();
          const coordinatorDefect = { _tag: "VerificationCoordinatorWorkerDefect" } as const;
          const coordinator = yield* buildVerificationTurnCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            admission: admission.admission,
            planningFinalizer,
            admissionHarness: prepared.setup.candidate.admissionHarness,
            task: prepared.setup.candidate.task,
            worktree: prepared.setup.candidate.worktree,
            orchestration: prepared.setup.coordinator.orchestration,
            snapshots: prepared.setup.coordinator.snapshots,
            hooks: {
              ...noopVerificationCoordinatorHooks,
              afterAdmissionReplay: () =>
                Ref.get(blockFailedCoordinator).pipe(
                  Effect.flatMap((block) =>
                    block
                      ? Deferred.succeed(failedCoordinatorEntered, undefined).pipe(
                          Effect.andThen(Effect.never),
                        )
                      : Ref.get(failCoordinatorFatally).pipe(
                          Effect.flatMap((failFatally) =>
                            failFatally
                              ? Deferred.succeed(fatalCoordinatorEntered, undefined).pipe(
                                  Effect.andThen(Deferred.await(releaseFatalCoordinator)),
                                  Effect.andThen(Effect.die(coordinatorDefect)),
                                )
                              : Ref.update(coordinatorRecoveries, (count) => count + 1),
                          ),
                        ),
                  ),
                ),
              afterPublication: () => Ref.update(coordinatorPublications, (count) => count + 1),
            },
          });

          const failedAttempt = yield* Scope.make("sequential");
          const failedGate = yield* Deferred.make<void>();
          yield* coordinator.coordinator
            .prepare(Deferred.await(failedGate))
            .pipe(Scope.provide(failedAttempt));
          yield* Deferred.succeed(failedGate, undefined);
          yield* Deferred.await(failedCoordinatorEntered);
          const coordinatorDrainWaiters = [
            yield* coordinator.coordinator.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* coordinator.coordinator.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* coordinator.coordinator.drain.pipe(Effect.forkChild({ startImmediately: true })),
          ];
          yield* Scope.close(failedAttempt, Exit.die("reaper-startup-defect"));
          for (const waiter of coordinatorDrainWaiters) {
            const drainExit = yield* Fiber.await(waiter);
            assert.isTrue(Exit.isFailure(drainExit));
            if (Exit.isFailure(drainExit)) assert.isTrue(Cause.hasInterruptsOnly(drainExit.cause));
          }
          const laterCoordinatorDrain = yield* Effect.exit(coordinator.coordinator.drain);
          assert.isTrue(Exit.isFailure(laterCoordinatorDrain));
          if (Exit.isFailure(laterCoordinatorDrain)) {
            assert.isTrue(Cause.hasInterruptsOnly(laterCoordinatorDrain.cause));
          }
          assert.equal(yield* Ref.get(coordinatorRecoveries), 0);
          assert.equal(yield* Ref.get(coordinatorPublications), 0);
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT count(*) AS count
              FROM agent_control_verification_materialization_markers
            `,
            [{ count: 0 }],
          );

          yield* Ref.set(blockFailedCoordinator, false);
          yield* Ref.set(failCoordinatorFatally, true);
          const fatalCoordinatorAttempt = yield* Scope.make("sequential");
          const fatalCoordinatorGate = yield* Deferred.make<void>();
          yield* coordinator.coordinator
            .prepare(Deferred.await(fatalCoordinatorGate))
            .pipe(Scope.provide(fatalCoordinatorAttempt));
          yield* Deferred.succeed(fatalCoordinatorGate, undefined);
          yield* Deferred.await(fatalCoordinatorEntered);
          const fatalCoordinatorWaiters = [
            yield* coordinator.coordinator.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* coordinator.coordinator.drain.pipe(Effect.forkChild({ startImmediately: true })),
          ];
          yield* Deferred.succeed(releaseFatalCoordinator, undefined);
          for (const waiter of fatalCoordinatorWaiters) {
            const drainExit = yield* Fiber.await(waiter);
            assert.isTrue(Exit.isFailure(drainExit));
            if (Exit.isFailure(drainExit)) {
              assert.isTrue(
                drainExit.cause.reasons.some(
                  (reason) => Cause.isDieReason(reason) && reason.defect === coordinatorDefect,
                ),
              );
            }
          }
          const repeatedCoordinatorDefect = yield* Effect.exit(coordinator.coordinator.drain);
          assert.isTrue(Exit.isFailure(repeatedCoordinatorDefect));
          if (Exit.isFailure(repeatedCoordinatorDefect)) {
            assert.isTrue(
              repeatedCoordinatorDefect.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === coordinatorDefect,
              ),
            );
          }
          yield* Scope.close(fatalCoordinatorAttempt, Exit.void);

          const retryAttempt = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(retryAttempt, Exit.void));
          yield* Ref.set(failCoordinatorFatally, false);
          const retryGate = yield* Deferred.make<void>();
          yield* coordinator.coordinator
            .prepare(Deferred.await(retryGate))
            .pipe(Scope.provide(retryAttempt));
          assert.equal(yield* Ref.get(coordinatorRecoveries), 0);
          yield* Deferred.succeed(retryGate, undefined);
          yield* coordinator.coordinator.drain.pipe(Effect.timeout(barrierTimeout));
          assert.equal(yield* Ref.get(coordinatorRecoveries), 1);
          assert.equal(yield* Ref.get(coordinatorPublications), 1);

          const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
            SELECT handoff_id AS "handoffId"
            FROM agent_control_verification_handoff_accepted
          `;
          assert.isDefined(handoff);
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator,
            executorCalls,
          });
          yield* consumer.processHandoff(handoff!.handoffId);
          const verificationStageRunId = Option.getOrThrow(
            yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
          ).evidence.stageRunId;
          const stageRecoveries = yield* Ref.make(0);
          const stagePublications = yield* Ref.make(0);
          const blockFailedStageStarter = yield* Ref.make(true);
          const failedStageStarterEntered = yield* Deferred.make<void>();
          const failStageStarterFatally = yield* Ref.make(false);
          const fatalStageStarterEntered = yield* Deferred.make<void>();
          const releaseFatalStageStarter = yield* Deferred.make<void>();
          const stageStarterDefect = { _tag: "VerificationStageStarterWorkerDefect" } as const;
          const starter = yield* buildVerificationStageStarter({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator,
            planningFinalizer,
            hooks: {
              ...noopVerificationStageStarterHooks,
              afterProviderEvidence: () =>
                Ref.get(blockFailedStageStarter).pipe(
                  Effect.flatMap((block) =>
                    block
                      ? Deferred.succeed(failedStageStarterEntered, undefined).pipe(
                          Effect.andThen(Effect.never),
                        )
                      : Ref.get(failStageStarterFatally).pipe(
                          Effect.flatMap((failFatally) =>
                            failFatally
                              ? Deferred.succeed(fatalStageStarterEntered, undefined).pipe(
                                  Effect.andThen(Deferred.await(releaseFatalStageStarter)),
                                  Effect.andThen(Effect.die(stageStarterDefect)),
                                )
                              : Ref.update(stageRecoveries, (count) => count + 1),
                          ),
                        ),
                  ),
                ),
              afterPublication: () => Ref.update(stagePublications, (count) => count + 1),
            },
          });

          const failedStageAttempt = yield* Scope.make("sequential");
          const failedStageGate = yield* Deferred.make<void>();
          yield* starter
            .prepare(Deferred.await(failedStageGate))
            .pipe(Scope.provide(failedStageAttempt));
          yield* Deferred.succeed(failedStageGate, undefined);
          yield* Deferred.await(failedStageStarterEntered);
          yield* coordinator.wakeup.wake(handoff!.handoffId);
          const stageDrainWaiters = [
            yield* starter.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* starter.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* starter.drain.pipe(Effect.forkChild({ startImmediately: true })),
          ];
          yield* Scope.close(failedStageAttempt, Exit.die("reaper-startup-defect"));
          for (const waiter of stageDrainWaiters) {
            const drainExit = yield* Fiber.await(waiter);
            assert.isTrue(Exit.isFailure(drainExit));
            if (Exit.isFailure(drainExit)) assert.isTrue(Cause.hasInterruptsOnly(drainExit.cause));
          }
          const laterStageDrain = yield* Effect.exit(starter.drain);
          assert.isTrue(Exit.isFailure(laterStageDrain));
          if (Exit.isFailure(laterStageDrain)) {
            assert.isTrue(Cause.hasInterruptsOnly(laterStageDrain.cause));
          }
          assert.equal(yield* Ref.get(stageRecoveries), 0);
          assert.equal(yield* Ref.get(stagePublications), 0);
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id = ${verificationStageRunId}
            `,
            [{ status: "prepared", revision: 1 }],
          );

          yield* Ref.set(blockFailedStageStarter, false);
          yield* Ref.set(failStageStarterFatally, true);
          const fatalStageAttempt = yield* Scope.make("sequential");
          const fatalStageGate = yield* Deferred.make<void>();
          yield* starter
            .prepare(Deferred.await(fatalStageGate))
            .pipe(Scope.provide(fatalStageAttempt));
          yield* Deferred.succeed(fatalStageGate, undefined);
          yield* Deferred.await(fatalStageStarterEntered);
          const fatalStageWaiters = [
            yield* starter.drain.pipe(Effect.forkChild({ startImmediately: true })),
            yield* starter.drain.pipe(Effect.forkChild({ startImmediately: true })),
          ];
          yield* Deferred.succeed(releaseFatalStageStarter, undefined);
          for (const waiter of fatalStageWaiters) {
            const drainExit = yield* Fiber.await(waiter);
            assert.isTrue(Exit.isFailure(drainExit));
            if (Exit.isFailure(drainExit)) {
              assert.isTrue(
                drainExit.cause.reasons.some(
                  (reason) => Cause.isDieReason(reason) && reason.defect === stageStarterDefect,
                ),
              );
            }
          }
          const repeatedStageDefect = yield* Effect.exit(starter.drain);
          assert.isTrue(Exit.isFailure(repeatedStageDefect));
          if (Exit.isFailure(repeatedStageDefect)) {
            assert.isTrue(
              repeatedStageDefect.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === stageStarterDefect,
              ),
            );
          }
          yield* Scope.close(fatalStageAttempt, Exit.void);

          const retryStageAttempt = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(retryStageAttempt, Exit.void));
          yield* Ref.set(failStageStarterFatally, false);
          const retryStageGate = yield* Deferred.make<void>();
          yield* starter
            .prepare(Deferred.await(retryStageGate))
            .pipe(Scope.provide(retryStageAttempt));
          yield* Deferred.succeed(retryStageGate, undefined);
          yield* starter.drain.pipe(Effect.timeout(barrierTimeout));
          assert.equal(yield* Ref.get(stageRecoveries), 1);
          assert.equal(yield* Ref.get(stagePublications), 1);
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id = ${verificationStageRunId}
            `,
            [{ status: "running", revision: 2 }],
          );
        }),
      ),
    ),
);

it.effect("converges two fresh Verification materializers on one durable boundary", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = "verification-materialization-race";
        const database = yield* makeSharedDatabase();
        const planningFinalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const prepared = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizerA,
          suffix,
        );
        const verificationAdmissionA = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer: planningFinalizerA,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: prepared.setup.candidate.admissionHarness,
        });
        assert.equal(
          (yield* verificationAdmissionA.admission.processResultEvidence(
            prepared.implementation.resultEvidenceId,
          ))._tag,
          "Admitted",
        );

        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const implementationAdmissionB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          prepared.setup.candidate.task,
          prepared.setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const implementationCoordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: `${suffix}-peer`,
          admission: implementationAdmissionB.admission,
          finalizer: planningFinalizerB,
          admissionHarness: implementationAdmissionB,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
        });
        const verificationAdmissionB = yield* buildVerificationAdmission({
          sql: database.sqlB,
          scope: database.scopeB,
          planningFinalizer: planningFinalizerB,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: implementationAdmissionB,
        });

        const reachedA = yield* Deferred.make<void>();
        const reachedB = yield* Deferred.make<void>();
        const releaseA = yield* Deferred.make<void>();
        const releaseB = yield* Deferred.make<void>();
        const publishedA = yield* Ref.make(0);
        const publishedB = yield* Ref.make(0);
        yield* Effect.addFinalizer(() =>
          Effect.all(
            [Deferred.succeed(releaseA, undefined), Deferred.succeed(releaseB, undefined)],
            { discard: true },
          ),
        );
        const hooks = (
          reached: Deferred.Deferred<void>,
          release: Deferred.Deferred<void>,
          published: Ref.Ref<number>,
        ): AgentControlVerificationTurnCoordinatorHooksShape => ({
          ...noopVerificationCoordinatorHooks,
          afterAdmissionReplay: () =>
            Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
          afterPublication: () => Ref.update(published, (count) => count + 1),
        });
        const coordinatorA = yield* buildVerificationTurnCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          admission: verificationAdmissionA.admission,
          planningFinalizer: planningFinalizerA,
          admissionHarness: prepared.setup.candidate.admissionHarness,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: prepared.setup.coordinator.orchestration,
          snapshots: prepared.setup.coordinator.snapshots,
          hooks: hooks(reachedA, releaseA, publishedA),
        });
        const coordinatorB = yield* buildVerificationTurnCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          admission: verificationAdmissionB.admission,
          planningFinalizer: planningFinalizerB,
          admissionHarness: implementationAdmissionB,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: implementationCoordinatorB.orchestration,
          snapshots: implementationCoordinatorB.snapshots,
          hooks: hooks(reachedB, releaseB, publishedB),
        });

        const fiberA = yield* coordinatorA.coordinator
          .processHandoff(prepared.implementation.resultEvidenceId)
          .pipe(Effect.forkChild);
        const fiberB = yield* coordinatorB.coordinator
          .processHandoff(prepared.implementation.resultEvidenceId)
          .pipe(Effect.forkChild);
        yield* Effect.all([Deferred.await(reachedA), Deferred.await(reachedB)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseA, undefined);
        const resultA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseB, undefined);
        const resultB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
        assert.deepStrictEqual([resultA._tag, resultB._tag].sort(), ["Materialized", "Replayed"]);
        assert.equal((yield* Ref.get(publishedA)) + (yield* Ref.get(publishedB)), 1);
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_verification_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_materialization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_verification_handoff_accepted) AS handoffs,
              (SELECT count(*) FROM agent_control_verification_deliveries) AS deliveries,
              (SELECT count(*) FROM orchestration_events event
               JOIN agent_control_verification_materialization_evidence materialized
                 ON materialized.thread_id = event.stream_id
               WHERE event.event_type IN ('thread.created','thread.agent-control-bound'))
                AS threadEvents,
              (SELECT count(*) FROM agent_control_events event
               JOIN agent_control_verification_admission_evidence admitted
                 ON admitted.verification_controlled_thread_reservation_id = event.stream_id
               WHERE event.aggregate_kind = 'controlled-thread-reservation')
                AS reservationEvents
          `,
          [
            {
              evidence: 1,
              receipts: 1,
              markers: 1,
              handoffs: 1,
              deliveries: 1,
              threadEvents: 2,
              reservationEvents: 3,
            },
          ],
        );
      }),
    ),
  ),
);

it.effect.each<{ readonly phase: "before-marker" | "after-commit" }>([
  { phase: "before-marker" },
  { phase: "after-commit" },
])("recovers Verification materialization after a $phase crash", ({ phase }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = `verification-materialization-${phase}`;
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const prepared = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          suffix,
        );
        const verificationAdmission = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: prepared.setup.candidate.admissionHarness,
        });
        assert.equal(
          (yield* verificationAdmission.admission.processResultEvidence(
            prepared.implementation.resultEvidenceId,
          ))._tag,
          "Admitted",
        );
        const publications = yield* Ref.make(0);
        const crashing = yield* buildVerificationTurnCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          admission: verificationAdmission.admission,
          planningFinalizer,
          admissionHarness: prepared.setup.candidate.admissionHarness,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: prepared.setup.coordinator.orchestration,
          snapshots: prepared.setup.coordinator.snapshots,
          hooks: {
            ...noopVerificationCoordinatorHooks,
            beforeMaterializationMarker: () =>
              phase === "before-marker" ? Effect.interrupt : Effect.void,
            afterOuterCommit: () =>
              phase === "after-commit"
                ? Effect.die(new Error("verification-materialization-response-loss"))
                : Effect.void,
            afterPublication: () => Ref.update(publications, (count) => count + 1),
          },
        });
        const exit = yield* Effect.exit(
          crashing.coordinator.processHandoff(prepared.implementation.resultEvidenceId),
        );
        assert.isTrue(Exit.isFailure(exit));
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_verification_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_materialization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_verification_handoff_accepted) AS handoffs,
              (SELECT count(*) FROM agent_control_verification_deliveries) AS deliveries,
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind = 'controlled-thread-reservation'
                 AND json_extract(payload_json, '$.stageKind') = 'verification')
                AS reservationEvents
          `,
          [
            {
              evidence: phase === "before-marker" ? 0 : 1,
              receipts: phase === "before-marker" ? 0 : 1,
              markers: phase === "before-marker" ? 0 : 1,
              handoffs: phase === "before-marker" ? 0 : 1,
              deliveries: phase === "before-marker" ? 0 : 1,
              reservationEvents: phase === "before-marker" ? 1 : 3,
            },
          ],
        );
        assert.equal(yield* Ref.get(publications), 0);

        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const implementationAdmissionB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          prepared.setup.candidate.task,
          prepared.setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const implementationCoordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: `${suffix}-recovery`,
          admission: implementationAdmissionB.admission,
          finalizer: planningFinalizerB,
          admissionHarness: implementationAdmissionB,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
        });
        const verificationAdmissionB = yield* buildVerificationAdmission({
          sql: database.sqlB,
          scope: database.scopeB,
          planningFinalizer: planningFinalizerB,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: implementationAdmissionB,
        });
        const recovered = yield* buildVerificationTurnCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          admission: verificationAdmissionB.admission,
          planningFinalizer: planningFinalizerB,
          admissionHarness: implementationAdmissionB,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: implementationCoordinatorB.orchestration,
          snapshots: implementationCoordinatorB.snapshots,
        });
        assert.equal(
          (yield* recovered.coordinator.processHandoff(prepared.implementation.resultEvidenceId))
            ._tag,
          phase === "before-marker" ? "Materialized" : "Replayed",
        );
        assert.deepStrictEqual(
          yield* database.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_verification_materialization_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_materialization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_materialization_markers)
                AS markers,
              (SELECT count(*) FROM agent_control_verification_handoff_accepted) AS handoffs,
              (SELECT count(*) FROM agent_control_verification_deliveries) AS deliveries
          `,
          [{ evidence: 1, receipts: 1, markers: 1, handoffs: 1, deliveries: 1 }],
        );
      }),
    ),
  ),
);

it.effect(
  "claims one Provider turn and starts one Verification StageRun across fresh workers",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const suffix = "verification-delivery-stage-race";
          const database = yield* makeSharedDatabase();
          const planningFinalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
          const prepared = yield* prepareSucceededImplementationFinalization(
            database,
            planningFinalizerA,
            suffix,
          );
          const verificationAdmissionA = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer: planningFinalizerA,
            implementationFinalizer: prepared.setup.finalizer.finalizer,
            handoffStore: prepared.setup.coordinator.handoffStore,
            admissionHarness: prepared.setup.candidate.admissionHarness,
          });
          assert.equal(
            (yield* verificationAdmissionA.admission.processResultEvidence(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Admitted",
          );
          const coordinatorA = yield* buildVerificationTurnCoordinator({
            sql: database.sqlA,
            scope: database.scopeA,
            admission: verificationAdmissionA.admission,
            planningFinalizer: planningFinalizerA,
            admissionHarness: prepared.setup.candidate.admissionHarness,
            task: prepared.setup.candidate.task,
            worktree: prepared.setup.candidate.worktree,
            orchestration: prepared.setup.coordinator.orchestration,
            snapshots: prepared.setup.coordinator.snapshots,
          });
          assert.equal(
            (yield* coordinatorA.coordinator.processHandoff(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Materialized",
          );

          const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
          const implementationAdmissionB = yield* buildAdmission(
            database.sqlB,
            database.scopeB,
            planningFinalizerB,
            prepared.setup.candidate.task,
            prepared.setup.candidate.worktree,
            noopAdmissionHooks,
          );
          const implementationCoordinatorB = yield* buildImplementationCoordinator({
            sql: database.sqlB,
            scope: database.scopeB,
            suffix: `${suffix}-peer`,
            admission: implementationAdmissionB.admission,
            finalizer: planningFinalizerB,
            admissionHarness: implementationAdmissionB,
            task: prepared.setup.candidate.task,
            worktree: prepared.setup.candidate.worktree,
          });
          const verificationAdmissionB = yield* buildVerificationAdmission({
            sql: database.sqlB,
            scope: database.scopeB,
            planningFinalizer: planningFinalizerB,
            implementationFinalizer: prepared.setup.finalizer.finalizer,
            handoffStore: prepared.setup.coordinator.handoffStore,
            admissionHarness: implementationAdmissionB,
          });
          const coordinatorB = yield* buildVerificationTurnCoordinator({
            sql: database.sqlB,
            scope: database.scopeB,
            admission: verificationAdmissionB.admission,
            planningFinalizer: planningFinalizerB,
            admissionHarness: implementationAdmissionB,
            task: prepared.setup.candidate.task,
            worktree: prepared.setup.candidate.worktree,
            orchestration: implementationCoordinatorB.orchestration,
            snapshots: implementationCoordinatorB.snapshots,
          });
          assert.equal(
            (yield* coordinatorB.coordinator.processHandoff(
              prepared.implementation.resultEvidenceId,
            ))._tag,
            "Replayed",
          );
          const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
            SELECT handoff_id AS "handoffId"
            FROM agent_control_verification_handoff_accepted
          `;
          assert.isDefined(handoff);

          const executorCalls = yield* Ref.make(0);
          const claimReachedA = yield* Deferred.make<void>();
          const claimReachedB = yield* Deferred.make<void>();
          const releaseClaimA = yield* Deferred.make<void>();
          const releaseClaimB = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() =>
            Effect.all(
              [
                Deferred.succeed(releaseClaimA, undefined),
                Deferred.succeed(releaseClaimB, undefined),
              ],
              { discard: true },
            ),
          );
          const consumerHooks = (
            reached: Deferred.Deferred<void>,
            release: Deferred.Deferred<void>,
          ): AgentControlVerificationTurnConsumerHooksShape => ({
            ...noopVerificationConsumerHooks,
            beforeClaim: () =>
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
          });
          const consumerA = yield* buildVerificationTurnConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: coordinatorA,
            executorCalls,
            hooks: consumerHooks(claimReachedA, releaseClaimA),
          });
          const consumerB = yield* buildVerificationTurnConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: coordinatorB,
            executorCalls,
            hooks: consumerHooks(claimReachedB, releaseClaimB),
          });
          const consumerFiberA = yield* consumerA
            .processHandoff(handoff!.handoffId)
            .pipe(Effect.forkChild);
          yield* Deferred.await(claimReachedA).pipe(Effect.timeout(barrierTimeout));
          const consumerFiberB = yield* consumerB
            .processHandoff(handoff!.handoffId)
            .pipe(Effect.forkChild);
          yield* Deferred.await(claimReachedB).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseClaimA, undefined);
          yield* Fiber.join(consumerFiberA).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseClaimB, undefined);
          yield* Fiber.join(consumerFiberB).pipe(Effect.timeout(barrierTimeout));

          assert.equal(yield* Ref.get(executorCalls), 1);
          const providerStarted = Option.getOrThrow(
            yield* coordinatorA.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
          );
          assert.equal(providerStarted.delivery.state, "provider-started");
          assert.equal(providerStarted.delivery.providerTurnId, "verification-provider-turn");
          assert.equal(providerStarted.delivery.claimGeneration, 1);
          assert.equal(providerStarted.delivery.attemptCount, 1);
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT
                (SELECT count(*) FROM agent_control_verification_turn_accepted) AS accepted,
                (SELECT count(*) FROM agent_control_verification_session_evidence) AS sessions,
                (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                  AS attestations,
                (SELECT count(*) FROM orchestration_events
                 WHERE stream_id = ${providerStarted.evidence.threadId}
                   AND event_type IN ('thread.message-sent','thread.turn-start-requested'))
                  AS turnEvents
            `,
            [{ accepted: 1, sessions: 1, attestations: 1, turnEvents: 2 }],
          );

          const stageReachedA = yield* Deferred.make<void>();
          const stageReachedB = yield* Deferred.make<void>();
          const releaseStageA = yield* Deferred.make<void>();
          const releaseStageB = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() =>
            Effect.all(
              [
                Deferred.succeed(releaseStageA, undefined),
                Deferred.succeed(releaseStageB, undefined),
              ],
              { discard: true },
            ),
          );
          const stageHooks = (
            reached: Deferred.Deferred<void>,
            release: Deferred.Deferred<void>,
          ): AgentControlVerificationStageStarterHooksShape => ({
            ...noopVerificationStageStarterHooks,
            afterProviderEvidence: () =>
              Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
          });
          yield* Ref.set(planningFinalizerA.stagePublished, []);
          yield* Ref.set(planningFinalizerB.stagePublished, []);
          const starterA = yield* buildVerificationStageStarter({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: coordinatorA,
            planningFinalizer: planningFinalizerA,
            hooks: stageHooks(stageReachedA, releaseStageA),
          });
          const starterB = yield* buildVerificationStageStarter({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: coordinatorB,
            planningFinalizer: planningFinalizerB,
            hooks: stageHooks(stageReachedB, releaseStageB),
          });
          const stageFiberA = yield* starterA
            .processHandoff(handoff!.handoffId)
            .pipe(Effect.forkChild);
          const stageFiberB = yield* starterB
            .processHandoff(handoff!.handoffId)
            .pipe(Effect.forkChild);
          yield* Effect.all([Deferred.await(stageReachedA), Deferred.await(stageReachedB)], {
            concurrency: "unbounded",
          }).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseStageA, undefined);
          const stageA = yield* Fiber.join(stageFiberA).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(releaseStageB, undefined);
          const stageB = yield* Fiber.join(stageFiberB).pipe(Effect.timeout(barrierTimeout));
          assert.deepStrictEqual([stageA._tag, stageB._tag].sort(), ["Replayed", "Started"]);
          assert.equal(
            (yield* Ref.get(planningFinalizerA.stagePublished)).length +
              (yield* Ref.get(planningFinalizerB.stagePublished)).length,
            1,
          );
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT
                (SELECT count(*) FROM agent_control_events
                 WHERE stream_id = ${providerStarted.evidence.stageRunId}
                   AND event_type = 'agentControl.stageRun.verificationStarted') AS started,
                (SELECT count(*) FROM agent_control_verification_stage_started_evidence)
                  AS evidence,
                (SELECT count(*) FROM agent_control_verification_stage_started_receipts)
                  AS receipts,
                (SELECT count(*) FROM agent_control_verification_stage_started_markers)
                  AS markers,
                (SELECT status FROM agent_control_stage_run_states
                 WHERE stage_run_id = ${providerStarted.evidence.stageRunId}) AS stageStatus,
                (SELECT revision FROM agent_control_stage_run_states
                 WHERE stage_run_id = ${providerStarted.evidence.stageRunId}) AS stageRevision,
                (SELECT status FROM agent_control_stage_run_lease_states
                 WHERE lease_id = ${providerStarted.evidence.leaseId}) AS leaseStatus,
                (SELECT holder_id FROM agent_control_stage_run_lease_states
                 WHERE lease_id = ${providerStarted.evidence.leaseId}) AS leaseHolderId,
                (SELECT fence_token FROM agent_control_stage_run_lease_states
                 WHERE lease_id = ${providerStarted.evidence.leaseId}) AS fenceToken
            `,
            [
              {
                started: 1,
                evidence: 1,
                receipts: 1,
                markers: 1,
                stageStatus: "running",
                stageRevision: 2,
                leaseStatus: "reserved",
                leaseHolderId: providerStarted.evidence.leaseHolderId,
                fenceToken: providerStarted.evidence.fenceToken,
              },
            ],
          );
        }),
      ),
    ),
);

const verificationSessionIncompatibleErrors = [
  {
    name: "ProviderValidationError",
    make: () =>
      new ProviderValidationError({
        operation: "ProviderService.sendTurn",
        issue: "invalid verification route",
      }),
  },
  {
    name: "ProviderUnsupportedError",
    make: () => new ProviderUnsupportedError({ provider: "verification-provider" }),
  },
  {
    name: "ProviderInstanceNotFoundError",
    make: () => new ProviderInstanceNotFoundError({ instanceId: "verification-provider" }),
  },
  {
    name: "ProviderAdapterValidationError",
    make: () =>
      new ProviderAdapterValidationError({
        provider: "verification-provider",
        operation: "prepareTurn",
        issue: "invalid native turn",
      }),
  },
] as const;

it.effect.each(verificationSessionIncompatibleErrors)(
  "retries $name before the Verification delivery-attempt CAS",
  ({ name, make }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery(`verification-pre-cas-${name}`);
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            prepareError: make(),
          });

          yield* consumer.processHandoff(prepared.handoffId);

          const waiting = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(waiting.delivery.state, "retry-wait");
          assert.equal(waiting.delivery.revision, 3);
          assert.equal(waiting.delivery.claimGeneration, 1);
          assert.equal(waiting.delivery.attemptCount, 1);
          assert.equal(waiting.delivery.lastErrorCode, "session-incompatible");
          assert.equal(waiting.delivery.terminalAt, null);
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.deepStrictEqual(
            yield* prepared.database.sqlA`
              SELECT
                (SELECT count(*) FROM agent_control_verification_session_evidence) AS sessions,
                (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                  AS attestations
            `,
            [{ sessions: 0, attestations: 0 }],
          );
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect(
  "propagates a Verification defect before the delivery-attempt CAS without ambiguity",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery("verification-pre-cas-defect");
          const executorCalls = yield* Ref.make(0);
          const defect = { _tag: "VerificationPreCasDefect" } as const;
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            hooks: {
              ...noopVerificationConsumerHooks,
              beforeDeliveryCas: () => Effect.die(defect),
            },
          });

          const exit = yield* Effect.exit(consumer.processHandoff(prepared.handoffId));
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            const reason = exit.cause.reasons.find(Cause.isDieReason);
            assert.isDefined(reason);
            if (reason !== undefined && Cause.isDieReason(reason)) {
              assert.strictEqual(reason.defect, defect);
            }
          }

          const claimed = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(claimed.delivery.state, "claimed");
          assert.equal(claimed.delivery.terminalAt, null);
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                AS attestations,
              (SELECT count(*) FROM agent_control_verification_deliveries
               WHERE state = 'ambiguous') AS ambiguous
          `,
            [{ attestations: 0, ambiguous: 0 }],
          );
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect.each(verificationSessionIncompatibleErrors)(
  "marks $name ambiguous after the Verification delivery-attempt CAS",
  ({ name, make }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery(`verification-post-cas-${name}`);
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            afterDeliveryCasError: make(),
          });

          yield* consumer.processHandoff(prepared.handoffId);

          const ambiguous = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(ambiguous.delivery.state, "ambiguous");
          assert.equal(ambiguous.delivery.revision, 4);
          assert.equal(ambiguous.delivery.claimGeneration, 1);
          assert.equal(ambiguous.delivery.attemptCount, 1);
          assert.equal(ambiguous.delivery.lastErrorCode, "provider-acceptance-ambiguous");
          assert.equal(ambiguous.delivery.nextAttemptAt, null);
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.deepStrictEqual(
            yield* prepared.database.sqlA`
              SELECT
                (SELECT count(*) FROM agent_control_verification_session_evidence) AS sessions,
                (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                  AS attestations
            `,
            [{ sessions: 1, attestations: 1 }],
          );
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("preserves Verification retry revision, owner, and claim-generation CAS", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-retry-cas");
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: prepared.database.scopeA,
          coordinator: prepared.coordinator,
          executorCalls,
          hooks: {
            ...noopVerificationConsumerHooks,
            afterClaim: () => Effect.interrupt,
          },
        });
        const interrupted = yield* Effect.exit(consumer.processHandoff(prepared.handoffId));
        assert.isTrue(Exit.isFailure(interrupted));
        if (Exit.isFailure(interrupted)) {
          assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
        }
        const claimed = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(claimed.delivery.state, "claimed");
        assert.isNotNull(claimed.delivery.claimOwnerId);
        const retryInput = {
          handoffId: prepared.handoffId,
          ownerId: claimed.delivery.claimOwnerId!,
          claimGeneration: claimed.delivery.claimGeneration,
          expectedRevision: claimed.delivery.revision,
          nextAttemptAt: "2026-08-02T08:00:30.000Z",
          errorCode: "session-incompatible" as const,
          updatedAt: createdAt,
        };
        for (const invalid of [
          { ...retryInput, expectedRevision: retryInput.expectedRevision - 1 },
          { ...retryInput, ownerId: `${retryInput.ownerId}-foreign` },
          { ...retryInput, claimGeneration: retryInput.claimGeneration + 1 },
        ]) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(prepared.coordinator.handoffStore.scheduleRetry(invalid)),
            ),
          );
          assert.equal(
            Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            ).delivery.state,
            "claimed",
          );
        }
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              prepared.coordinator.handoffStore.markAmbiguous({
                handoffId: prepared.handoffId,
                expectedRevision: claimed.delivery.revision,
                terminalAt: createdAt,
              }),
            ),
          ),
        );
        const waiting = yield* prepared.coordinator.handoffStore.scheduleRetry(retryInput);
        assert.equal(waiting.state, "retry-wait");
        assert.equal(waiting.revision, claimed.delivery.revision + 1);
        assert.equal(waiting.claimGeneration, claimed.delivery.claimGeneration);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("enforces the post-attempt Verification delivery state machine against direct SQL", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const attempted = yield* prepareVerificationTurnDelivery(
          "verification-direct-sql-attempted",
        );
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildVerificationTurnConsumer({
          sql: attempted.database.sqlA,
          scope: attempted.database.scopeA,
          coordinator: attempted.coordinator,
          executorCalls,
          hooks: {
            ...noopVerificationConsumerHooks,
            afterDeliveryCas: () => Effect.interrupt,
          },
        });
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(consumer.processHandoff(attempted.handoffId))),
        );
        assert.equal(
          Option.getOrThrow(
            yield* attempted.coordinator.handoffStore.loadAcceptedByHandoffId(attempted.handoffId),
          ).delivery.state,
          "delivery-attempted",
        );

        const retryAttempted = attempted.database.sqlA.unsafe(
          `UPDATE agent_control_verification_deliveries
             SET state='retry-wait', revision=revision+1,
               claim_owner_id=NULL, claim_expires_at=NULL,
               next_attempt_at=?, last_error_code='provider-timeout', updated_at=?
             WHERE handoff_id=?`,
          ["2026-08-02T08:03:00.000Z", createdAt, attempted.handoffId],
        ).unprepared;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(retryAttempted)));
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(attempted.database.sqlA.withTransaction(retryAttempted)),
          ),
        );

        const reclaimAttempted = attempted.database.sqlA.unsafe(
          `UPDATE agent_control_verification_deliveries
             SET state='claimed', revision=revision+1,
               claim_owner_id='recovery-owner', claim_generation=claim_generation+1,
               claim_expires_at=?, attempt_count=attempt_count+1, updated_at=?
             WHERE handoff_id=?`,
          ["2026-08-02T08:04:00.000Z", createdAt, attempted.handoffId],
        ).unprepared;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(reclaimAttempted)));

        for (const transition of [
          {
            state: "ambiguous",
            update: attempted.database.sqlA.unsafe(
              `UPDATE agent_control_verification_deliveries
                 SET state='ambiguous', revision=revision+1,
                   claim_owner_id=NULL, claim_expires_at=NULL,
                   terminal_at=?, last_error_code='provider-acceptance-ambiguous', updated_at=?
                 WHERE handoff_id=?`,
              [createdAt, createdAt, attempted.handoffId],
            ).unprepared,
          },
          {
            state: "provider-started",
            update: attempted.database.sqlA.unsafe(
              `UPDATE agent_control_verification_deliveries
                 SET state='provider-started', revision=revision+1,
                   claim_owner_id=NULL, claim_expires_at=NULL,
                   provider_turn_id='direct-provider-turn', provider_accepted_at=?,
                   last_error_code=NULL, updated_at=?
                 WHERE handoff_id=?`,
              [providerAcceptedAt, providerAcceptedAt, attempted.handoffId],
            ).unprepared,
          },
        ] as const) {
          const observed = yield* Ref.make(false);
          const rolledBack = yield* Effect.exit(
            attempted.database.sqlA.withTransaction(
              Effect.gen(function* () {
                yield* transition.update;
                const [row] = yield* attempted.database.sqlA<{ readonly state: string }>`
                  SELECT state FROM agent_control_verification_deliveries
                  WHERE handoff_id = ${attempted.handoffId}
                `;
                assert.equal(row?.state, transition.state);
                yield* Ref.set(observed, true);
                return yield* Effect.fail("rollback direct transition fixture" as const);
              }),
            ),
          );
          assert.isTrue(Exit.isFailure(rolledBack));
          assert.isTrue(yield* Ref.get(observed));
          assert.equal(
            Option.getOrThrow(
              yield* attempted.coordinator.handoffStore.loadAcceptedByHandoffId(
                attempted.handoffId,
              ),
            ).delivery.state,
            "delivery-attempted",
          );
        }

        yield* consumer.processHandoff(attempted.handoffId);
        assert.equal(yield* Ref.get(executorCalls), 0);
        yield* TestClock.adjust("2 minutes");
        yield* consumer.processHandoff(attempted.handoffId);
        assert.equal(
          Option.getOrThrow(
            yield* attempted.coordinator.handoffStore.loadAcceptedByHandoffId(attempted.handoffId),
          ).delivery.state,
          "ambiguous",
        );
        assert.equal(yield* Ref.get(executorCalls), 0);

        const claimed = yield* prepareVerificationTurnDelivery("verification-direct-sql-claimed");
        const claimedConsumer = yield* buildVerificationTurnConsumer({
          sql: claimed.database.sqlA,
          scope: claimed.database.scopeA,
          coordinator: claimed.coordinator,
          executorCalls: yield* Ref.make(0),
          hooks: {
            ...noopVerificationConsumerHooks,
            afterClaim: () => Effect.interrupt,
          },
        });
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(claimedConsumer.processHandoff(claimed.handoffId))),
        );
        yield* claimed.database.sqlA.withTransaction(
          claimed.database.sqlA.unsafe(
            `UPDATE agent_control_verification_deliveries
               SET state='retry-wait', revision=revision+1,
                 claim_owner_id=NULL, claim_expires_at=NULL,
                 next_attempt_at=?, last_error_code='provider-timeout', updated_at=?
               WHERE handoff_id=?`,
            ["2026-08-02T08:05:00.000Z", createdAt, claimed.handoffId],
          ).unprepared,
        );
        assert.equal(
          Option.getOrThrow(
            yield* claimed.coordinator.handoffStore.loadAcceptedByHandoffId(claimed.handoffId),
          ).delivery.state,
          "retry-wait",
        );
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("retries a Verification delivery after nextAttemptAt without an external wakeup", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-periodic-retry");
        const consumerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(consumerScope, Exit.void));
        const executorCalls = yield* Ref.make(0);
        const prepareFailures = yield* Ref.make(1);
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: consumerScope,
          coordinator: prepared.coordinator,
          executorCalls,
          prepareFailures,
        });

        yield* consumer.start().pipe(Scope.provide(consumerScope));
        yield* consumer.drain;
        const waiting = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(waiting.delivery.state, "retry-wait");
        assert.equal(waiting.delivery.claimGeneration, 1);
        assert.equal(waiting.delivery.attemptCount, 1);
        assert.equal(waiting.delivery.nextAttemptAt, "2026-08-02T08:00:30.000Z");
        assert.equal(yield* Ref.get(executorCalls), 0);

        yield* TestClock.adjust("25 seconds");
        yield* consumer.drain;
        assert.equal(
          Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          ).delivery.state,
          "retry-wait",
        );

        yield* TestClock.adjust("5 seconds");
        yield* consumer.drain;
        const delivered = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(delivered.delivery.state, "provider-started");
        assert.equal(delivered.delivery.claimGeneration, 2);
        assert.equal(delivered.delivery.attemptCount, 2);
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect(
  "isolates a failed Verification recovery input and keeps ticks, wakeups, runtime events, and drain alive",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-worker-failure-isolation",
          );
          const consumerScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(consumerScope, Exit.void));
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const recoveryCalls = yield* Ref.make(0);
          const observeQueueInputs = yield* Ref.make(false);
          const handoffFailures = yield* Ref.make(0);
          const runtimeFailures = yield* Ref.make(0);
          const wakeupFailureObserved = yield* Deferred.make<void>();
          const runtimeFailureObserved = yield* Deferred.make<void>();
          const wakeupObserved = yield* Deferred.make<void>();
          const runtimeObserved = yield* Deferred.make<void>();
          const backingStore = prepared.coordinator.handoffStore;
          const instrumentedStore = AgentControlVerificationHandoffStore.of({
            ...backingStore,
            listRecoverable: (now, afterExclusive, limit) =>
              Effect.gen(function* () {
                const call = yield* Ref.getAndUpdate(recoveryCalls, (count) => count + 1);
                if (call === 0) {
                  return yield* new AgentControlVerificationStoreError({
                    operation: "test-list-recoverable",
                    reason: "persistence",
                  });
                }
                return yield* backingStore.listRecoverable(now, afterExclusive, limit);
              }),
            loadAcceptedByHandoffId: (handoffId) =>
              Effect.gen(function* () {
                const failures = yield* Ref.getAndUpdate(handoffFailures, (count) =>
                  Math.max(0, count - 1),
                );
                if (failures > 0) {
                  yield* Deferred.succeed(wakeupFailureObserved, undefined);
                  return yield* new AgentControlVerificationStoreError({
                    operation: "test-load-handoff",
                    reason: "persistence",
                  });
                }
                if (yield* Ref.get(observeQueueInputs)) {
                  yield* Deferred.succeed(wakeupObserved, undefined);
                }
                return yield* backingStore.loadAcceptedByHandoffId(handoffId);
              }),
            loadAcceptedByThreadId: (threadId) =>
              Effect.gen(function* () {
                const failures = yield* Ref.getAndUpdate(runtimeFailures, (count) =>
                  Math.max(0, count - 1),
                );
                if (failures > 0) {
                  yield* Deferred.succeed(runtimeFailureObserved, undefined);
                  return yield* new AgentControlVerificationStoreError({
                    operation: "test-load-runtime-thread",
                    reason: "persistence",
                  });
                }
                if (yield* Ref.get(observeQueueInputs)) {
                  yield* Deferred.succeed(runtimeObserved, undefined);
                }
                return yield* backingStore.loadAcceptedByThreadId(threadId);
              }),
          });
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: consumerScope,
            coordinator: { ...prepared.coordinator, handoffStore: instrumentedStore },
            executorCalls,
            providerEvents,
          });

          const providerSubscription = yield* consumer.subscribeProviderEvents.pipe(
            Scope.provide(consumerScope),
          );
          yield* consumer.start(providerSubscription).pipe(Scope.provide(consumerScope));
          yield* consumer.drain.pipe(Effect.timeout(barrierTimeout));
          assert.equal(yield* Ref.get(recoveryCalls), 1);
          assert.equal(yield* Ref.get(executorCalls), 0);

          yield* TestClock.adjust("5 seconds");
          yield* consumer.drain.pipe(Effect.timeout(barrierTimeout));
          assert.isAbove(yield* Ref.get(recoveryCalls), 1);
          const delivered = Option.getOrThrow(
            yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(delivered.delivery.state, "provider-started");
          assert.equal(yield* Ref.get(executorCalls), 1);

          yield* Ref.set(observeQueueInputs, true);
          yield* Ref.set(handoffFailures, 1);
          yield* prepared.coordinator.wakeup.wake(prepared.handoffId);
          yield* Deferred.await(wakeupFailureObserved).pipe(Effect.timeout(barrierTimeout));
          yield* consumer.drain.pipe(Effect.timeout(barrierTimeout));
          yield* prepared.coordinator.wakeup.wake(prepared.handoffId);
          yield* Deferred.await(wakeupObserved).pipe(Effect.timeout(barrierTimeout));
          yield* Ref.set(runtimeFailures, 1);
          yield* PubSub.publish(providerEvents, {
            type: "turn.started",
            eventId: EventId.make("verification-worker-runtime-failure"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: delivered.evidence.providerInstanceId,
            threadId: delivered.evidence.threadId,
            createdAt: providerAcceptedAt,
            turnId: TurnId.make("verification-worker-runtime-turn"),
            payload: {},
          });
          yield* Deferred.await(runtimeFailureObserved).pipe(Effect.timeout(barrierTimeout));
          yield* consumer.drain.pipe(Effect.timeout(barrierTimeout));
          yield* PubSub.publish(providerEvents, {
            type: "turn.started",
            eventId: EventId.make("verification-worker-runtime-event"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: delivered.evidence.providerInstanceId,
            threadId: delivered.evidence.threadId,
            createdAt: providerAcceptedAt,
            turnId: TurnId.make("verification-worker-runtime-turn"),
            payload: {},
          });
          yield* Deferred.await(runtimeObserved).pipe(Effect.timeout(barrierTimeout));
          yield* consumer.drain.pipe(Effect.timeout(barrierTimeout));
          assert.equal(
            Option.getOrThrow(yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId))
              .delivery.state,
            "provider-started",
          );
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("surfaces a fatal Verification worker defect through drain", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-worker-defect");
        const consumerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(consumerScope, Exit.void));
        const executorCalls = yield* Ref.make(0);
        const defect = { _tag: "VerificationWorkerDefect" } as const;
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: consumerScope,
          coordinator: prepared.coordinator,
          executorCalls,
          hooks: {
            ...noopVerificationConsumerHooks,
            beforeDeliveryCas: () => Effect.die(defect),
          },
        });

        yield* consumer.start().pipe(Scope.provide(consumerScope));
        const drainExit = yield* Effect.exit(consumer.drain).pipe(Effect.timeout(barrierTimeout));
        assert.isTrue(Exit.isFailure(drainExit));
        if (Exit.isFailure(drainExit)) {
          const reason = drainExit.cause.reasons.find(Cause.isDieReason);
          assert.isDefined(reason);
          if (reason !== undefined && Cause.isDieReason(reason)) {
            assert.strictEqual(reason.defect, defect);
          }
        }
        assert.equal(
          Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          ).delivery.state,
          "claimed",
        );
        assert.equal(yield* Ref.get(executorCalls), 0);

        yield* prepared.coordinator.wakeup.wake(prepared.handoffId);
        const repeatedDrainExit = yield* Effect.exit(consumer.drain).pipe(
          Effect.timeout(barrierTimeout),
        );
        assert.isTrue(Exit.isFailure(repeatedDrainExit));
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("reclaims a Verification delivery after a pre-expiry consumer restart", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-periodic-claim");
        const crashedScope = yield* Scope.make("sequential");
        const executorCalls = yield* Ref.make(0);
        const crashed = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: crashedScope,
          coordinator: prepared.coordinator,
          executorCalls,
          hooks: {
            ...noopVerificationConsumerHooks,
            afterClaim: () => Effect.interrupt,
          },
        });
        const crashedExit = yield* Effect.exit(crashed.processHandoff(prepared.handoffId));
        assert.isTrue(Exit.isFailure(crashedExit));
        yield* Scope.close(crashedScope, Exit.void);
        const claimed = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(claimed.delivery.state, "claimed");
        assert.equal(claimed.delivery.claimExpiresAt, "2026-08-02T08:02:00.000Z");

        const restartedScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void));
        const restarted = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlB,
          scope: restartedScope,
          coordinator: prepared.coordinator,
          executorCalls,
        });
        yield* restarted.start().pipe(Scope.provide(restartedScope));
        yield* restarted.drain;
        assert.equal(yield* Ref.get(executorCalls), 0);

        yield* TestClock.adjust("115 seconds");
        yield* restarted.drain;
        assert.equal(
          Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          ).delivery.state,
          "claimed",
        );

        yield* TestClock.adjust("5 seconds");
        yield* restarted.drain;
        const delivered = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(delivered.delivery.state, "provider-started");
        assert.equal(delivered.delivery.claimGeneration, 2);
        assert.equal(delivered.delivery.attemptCount, 2);
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("marks an expired Verification delivery attempt ambiguous without a provider event", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-periodic-attempt");
        const crashedScope = yield* Scope.make("sequential");
        const executorCalls = yield* Ref.make(0);
        const crashed = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: crashedScope,
          coordinator: prepared.coordinator,
          executorCalls,
          hooks: {
            ...noopVerificationConsumerHooks,
            afterDeliveryCas: () => Effect.interrupt,
          },
        });
        const crashedExit = yield* Effect.exit(crashed.processHandoff(prepared.handoffId));
        assert.isTrue(Exit.isFailure(crashedExit));
        yield* Scope.close(crashedScope, Exit.void);
        const attempted = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(attempted.delivery.state, "delivery-attempted");
        assert.equal(attempted.delivery.claimExpiresAt, "2026-08-02T08:02:00.000Z");
        assert.equal(yield* Ref.get(executorCalls), 0);

        const restartedScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void));
        const restarted = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlB,
          scope: restartedScope,
          coordinator: prepared.coordinator,
          executorCalls,
        });
        yield* restarted.start().pipe(Scope.provide(restartedScope));
        yield* restarted.drain;
        assert.equal(
          Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          ).delivery.state,
          "delivery-attempted",
        );

        yield* TestClock.adjust("2 minutes");
        yield* restarted.drain;
        const ambiguous = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(ambiguous.delivery.state, "ambiguous");
        assert.equal(ambiguous.delivery.lastErrorCode, "provider-acceptance-ambiguous");
        assert.equal(ambiguous.delivery.claimGeneration, 1);
        assert.equal(ambiguous.delivery.attemptCount, 1);
        assert.equal(yield* Ref.get(executorCalls), 0);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("serializes periodic Verification recovery and stops it with the consumer scope", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-periodic-scope");
        const consumerScope = yield* Scope.make("sequential");
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const activeRecoveries = yield* Ref.make(0);
        const maxActiveRecoveries = yield* Ref.make(0);
        const recoveryCalls = yield* Ref.make(0);
        const backingStore = prepared.coordinator.handoffStore;
        const instrumentedStore = AgentControlVerificationHandoffStore.of({
          ...backingStore,
          listRecoverable: (now, afterExclusive, limit) =>
            Effect.gen(function* () {
              const call = yield* Ref.getAndUpdate(recoveryCalls, (count) => count + 1);
              const active = yield* Ref.updateAndGet(activeRecoveries, (count) => count + 1);
              yield* Ref.update(maxActiveRecoveries, (current) => Math.max(current, active));
              if (call === 0) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return yield* backingStore.listRecoverable(now, afterExclusive, limit);
            }).pipe(Effect.ensuring(Ref.update(activeRecoveries, (count) => count - 1))),
        });
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: consumerScope,
          coordinator: { ...prepared.coordinator, handoffStore: instrumentedStore },
          executorCalls,
        });

        yield* consumer.start().pipe(Scope.provide(consumerScope));
        yield* Deferred.await(entered);
        yield* TestClock.adjust("20 seconds");
        assert.equal(yield* Ref.get(recoveryCalls), 1);
        assert.equal(yield* Ref.get(maxActiveRecoveries), 1);

        yield* Deferred.succeed(release, undefined);
        yield* consumer.drain;
        assert.isAbove(yield* Ref.get(recoveryCalls), 1);
        assert.equal(yield* Ref.get(maxActiveRecoveries), 1);
        const callsBeforeClose = yield* Ref.get(recoveryCalls);
        yield* Scope.close(consumerScope, Exit.void);
        assert.equal(yield* Ref.get(activeRecoveries), 0);

        yield* TestClock.adjust("20 seconds");
        assert.equal(yield* Ref.get(recoveryCalls), callsBeforeClose);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect(
  "parks attempt-owned Verification recovery until commit and reloads durable work on retry",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-attempt-owned-recovery",
          );
          const failedScope = yield* Scope.make("sequential");
          const executorCalls = yield* Ref.make(0);
          const recoveryCalls = yield* Ref.make(0);
          const backingStore = prepared.coordinator.handoffStore;
          const instrumentedStore = AgentControlVerificationHandoffStore.of({
            ...backingStore,
            listRecoverable: (now, afterExclusive, limit) =>
              Ref.update(recoveryCalls, (count) => count + 1).pipe(
                Effect.andThen(backingStore.listRecoverable(now, afterExclusive, limit)),
              ),
          });
          const failedConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: failedScope,
            coordinator: { ...prepared.coordinator, handoffStore: instrumentedStore },
            executorCalls,
          });

          yield* failedConsumer.prepare().pipe(Scope.provide(failedScope));
          yield* prepared.coordinator.wakeup.wake(prepared.handoffId);
          yield* TestClock.adjust("20 seconds");
          assert.equal(yield* Ref.get(recoveryCalls), 0);
          assert.equal(yield* Ref.get(executorCalls), 0);
          assert.equal(
            Option.getOrThrow(yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId))
              .delivery.state,
            "pending",
          );
          yield* Scope.close(failedScope, Exit.void);
          yield* TestClock.adjust("20 seconds");
          assert.equal(yield* Ref.get(recoveryCalls), 0);
          assert.equal(yield* Ref.get(executorCalls), 0);

          const retryScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(retryScope, Exit.void));
          const retryStoreContext = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, prepared.database.sqlB)),
              Layer.provideMerge(NodeServices.layer),
            ),
            retryScope,
          );
          const retryStore = Context.get(retryStoreContext, AgentControlVerificationHandoffStore);
          const retryConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: retryScope,
            coordinator: { ...prepared.coordinator, handoffStore: retryStore },
            executorCalls,
          });
          const activation = yield* retryConsumer.prepare().pipe(Scope.provide(retryScope));
          assert.equal(yield* Ref.get(executorCalls), 0);
          yield* activation.commit;
          yield* activation.drain.pipe(Effect.timeout(barrierTimeout));

          const delivered = Option.getOrThrow(
            yield* retryStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(delivered.delivery.state, "provider-started");
          assert.equal(delivered.delivery.claimGeneration, 1);
          assert.equal(delivered.delivery.attemptCount, 1);
          assert.equal(yield* Ref.get(executorCalls), 1);
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("adopts a Verification provider start after response-loss ambiguity", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = "verification-response-loss";
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const prepared = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          suffix,
        );
        const verificationAdmission = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: prepared.setup.candidate.admissionHarness,
        });
        assert.equal(
          (yield* verificationAdmission.admission.processResultEvidence(
            prepared.implementation.resultEvidenceId,
          ))._tag,
          "Admitted",
        );
        const coordinator = yield* buildVerificationTurnCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          admission: verificationAdmission.admission,
          planningFinalizer,
          admissionHarness: prepared.setup.candidate.admissionHarness,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: prepared.setup.coordinator.orchestration,
          snapshots: prepared.setup.coordinator.snapshots,
        });
        assert.equal(
          (yield* coordinator.coordinator.processHandoff(prepared.implementation.resultEvidenceId))
            ._tag,
          "Materialized",
        );
        const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
          SELECT handoff_id AS "handoffId" FROM agent_control_verification_handoff_accepted
        `;
        assert.isDefined(handoff);
        const executorCalls = yield* Ref.make(0);
        const responseLossDefect = { _tag: "VerificationProviderResponseLossDefect" } as const;
        const lossy = yield* buildVerificationTurnConsumer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator,
          executorCalls,
          responseLossDefect,
        });
        const lossyExit = yield* Effect.exit(lossy.processHandoff(handoff!.handoffId));
        assert.isTrue(Exit.isFailure(lossyExit));
        if (Exit.isFailure(lossyExit)) {
          const reason = lossyExit.cause.reasons.find(Cause.isDieReason);
          assert.isDefined(reason);
          if (reason !== undefined && Cause.isDieReason(reason)) {
            assert.strictEqual(reason.defect, responseLossDefect);
          }
        }
        const ambiguous = Option.getOrThrow(
          yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
        );
        assert.equal(ambiguous.delivery.state, "ambiguous");
        assert.equal(ambiguous.delivery.providerTurnId, null);
        assert.equal(ambiguous.delivery.claimGeneration, 1);
        assert.equal(ambiguous.delivery.attemptCount, 1);
        assert.equal(yield* Ref.get(executorCalls), 1);
        assert.deepStrictEqual(
          yield* database.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_verification_session_evidence) AS sessions,
              (SELECT count(*) FROM agent_control_verification_delivery_attestations)
                AS attestations,
              (SELECT state FROM agent_control_verification_deliveries
               WHERE handoff_id = ${handoff!.handoffId}) AS state
          `,
          [{ sessions: 1, attestations: 1, state: "ambiguous" }],
        );

        const restarted = yield* buildVerificationTurnConsumer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator,
          executorCalls,
        });
        yield* restarted.processHandoff(handoff!.handoffId);
        assert.equal(yield* Ref.get(executorCalls), 1);
        yield* restarted.processRuntimeEvent({
          type: "turn.started",
          eventId: EventId.make("verification-response-loss-runtime-start"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: ambiguous.evidence.providerInstanceId,
          threadId: ambiguous.evidence.threadId,
          createdAt: providerAcceptedAt,
          turnId: TurnId.make("verification-response-loss-provider-turn"),
          payload: {},
        });
        const adopted = Option.getOrThrow(
          yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
        );
        assert.equal(adopted.delivery.state, "provider-started");
        assert.equal(adopted.delivery.providerTurnId, "verification-response-loss-provider-turn");
        assert.equal(adopted.delivery.claimGeneration, 1);
        assert.equal(adopted.delivery.attemptCount, 1);
        yield* restarted.processRuntimeEvent({
          type: "turn.completed",
          eventId: EventId.make("verification-response-loss-runtime-terminal"),
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: adopted.evidence.providerInstanceId,
          threadId: adopted.evidence.threadId,
          createdAt: "2026-08-02T08:01:01.000Z",
          turnId: TurnId.make("verification-response-loss-provider-turn"),
          payload: { state: "interrupted" },
        });
        assert.equal(
          Option.getOrThrow(
            yield* coordinator.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
          ).delivery.state,
          "interrupted",
        );
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ),
);

it.effect(
  "Verification terminal observation is replay-safe, conflicts fail closed, and StageRun remains running",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-terminal-observation",
          );
          const executorCalls = yield* Ref.make(0);
          const terminalCasCompletions = yield* Ref.make(0);
          const terminalCasCommitted = yield* Deferred.make<void>();
          const releaseTerminalWakeup = yield* Deferred.make<void>();
          yield* Effect.addFinalizer(() =>
            Deferred.succeed(releaseTerminalWakeup, undefined).pipe(Effect.asVoid),
          );
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            hooks: {
              ...noopVerificationConsumerHooks,
              afterProviderTerminalCas: () =>
                Ref.update(terminalCasCompletions, (count) => count + 1).pipe(
                  Effect.andThen(Deferred.succeed(terminalCasCommitted, undefined)),
                  Effect.andThen(Deferred.await(releaseTerminalWakeup)),
                ),
            },
          });
          yield* consumer.processHandoff(prepared.handoffId);
          const providerStarted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(providerStarted.delivery.state, "provider-started");
          assert.isNotNull(providerStarted.delivery.providerTurnId);
          const fastTerminalAt = shiftIso(providerStarted.delivery.providerAcceptedAt!, -1);

          const terminalEvent = {
            type: "turn.completed",
            eventId: EventId.make("verification-runtime-terminal-failed"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: providerStarted.evidence.providerInstanceId,
            threadId: providerStarted.evidence.threadId,
            turnId: TurnId.make(providerStarted.delivery.providerTurnId!),
            createdAt: fastTerminalAt,
            payload: {
              state: "failed",
              errorMessage: "untrusted provider detail must not be persisted",
            },
            raw: {
              source: "codex.eventmsg",
              payload: { secret: "must not affect the observation" },
            },
          } satisfies ProviderRuntimeEvent;
          for (const unrelatedEvent of [
            {
              ...terminalEvent,
              eventId: EventId.make("verification-runtime-terminal-wrong-thread"),
              threadId: ThreadId.make("verification-unrelated-thread"),
            },
            {
              ...terminalEvent,
              eventId: EventId.make("verification-runtime-terminal-wrong-provider"),
              providerInstanceId: ProviderInstanceId.make("codex-unrelated"),
            },
            {
              ...terminalEvent,
              eventId: EventId.make("verification-runtime-terminal-wrong-turn"),
              turnId: TurnId.make("verification-unrelated-provider-turn"),
            },
          ] satisfies ReadonlyArray<ProviderRuntimeEvent>) {
            yield* consumer.processRuntimeEvent(unrelatedEvent);
          }
          const identityUnchanged = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(identityUnchanged.delivery.state, "provider-started");
          assert.equal(identityUnchanged.delivery.revision, providerStarted.delivery.revision);
          const missingTurn = yield* Effect.flip(
            consumer.processRuntimeEvent({
              ...terminalEvent,
              eventId: EventId.make("verification-runtime-terminal-missing-turn"),
              turnId: undefined,
            }),
          );
          assert.isTrue(isAgentControlVerificationCandidateEvidenceError(missingTurn));
          if (isAgentControlVerificationCandidateEvidenceError(missingTurn)) {
            assert.equal(missingTurn.candidateReason, "terminal-identity-divergent");
          }
          const terminalFiber = yield* consumer
            .processRuntimeEvent(terminalEvent)
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(terminalCasCommitted).pipe(Effect.timeout(barrierTimeout));
          const terminal = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(terminal.delivery.state, "failed");
          assert.equal(terminal.delivery.terminalEventId, terminalEvent.eventId);
          assert.equal(terminal.delivery.terminalEventType, "turn.completed");
          assert.equal(terminal.delivery.terminalProviderState, "failed");
          assert.equal(terminal.delivery.lastErrorCode, "provider-turn-failed");
          assert.equal(terminal.delivery.terminalAt, terminalEvent.createdAt);
          assert.isTrue(terminal.delivery.terminalAt! < terminal.delivery.providerAcceptedAt!);
          assert.notEqual(terminal.delivery.updatedAt, terminal.delivery.terminalAt);
          assert.match(terminal.delivery.terminalObservationDigest!, /^[0-9a-f]{64}$/u);
          assert.notEqual(terminal.delivery.lastErrorCode, terminalEvent.payload.errorMessage);
          assert.equal(yield* Ref.get(terminalCasCompletions), 1);

          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            planningFinalizer: prepared.planningFinalizer,
          });
          assert.equal((yield* starter.processHandoff(prepared.handoffId))._tag, "Started");
          assert.equal((yield* starter.processHandoff(prepared.handoffId))._tag, "Replayed");
          yield* Deferred.succeed(releaseTerminalWakeup, undefined);
          yield* Fiber.join(terminalFiber);

          yield* consumer.processRuntimeEvent({
            ...terminalEvent,
            payload: { state: "failed", errorMessage: "different untrusted detail" },
            raw: { source: "codex.eventmsg", payload: { different: "ignored" } },
          });
          const replayed = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(replayed.delivery.revision, terminal.delivery.revision);
          assert.equal(
            replayed.delivery.terminalObservationDigest,
            terminal.delivery.terminalObservationDigest,
          );
          assert.equal(yield* Ref.get(terminalCasCompletions), 1);

          const timeConflict = yield* Effect.flip(
            consumer.processRuntimeEvent({
              ...terminalEvent,
              createdAt: shiftIso(fastTerminalAt, -1),
            }),
          );
          assert.isTrue(isVerificationStoreError(timeConflict));
          if (isVerificationStoreError(timeConflict)) {
            assert.equal(timeConflict.reason, "terminal-conflict");
          }

          const conflict = yield* Effect.flip(
            consumer.processRuntimeEvent({
              ...terminalEvent,
              eventId: EventId.make("verification-runtime-terminal-conflict"),
            }),
          );
          assert.isTrue(isVerificationStoreError(conflict));
          if (isVerificationStoreError(conflict)) {
            assert.equal(conflict.reason, "terminal-conflict");
          }
          assert.equal(yield* Ref.get(terminalCasCompletions), 1);
          assert.equal(
            Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            ).delivery.revision,
            terminal.delivery.revision,
          );

          assert.deepStrictEqual(
            yield* prepared.database.sqlA`
              SELECT
                (SELECT status FROM agent_control_stage_run_states
                 WHERE stage_run_id=${terminal.evidence.stageRunId}) AS stageStatus,
                (SELECT revision FROM agent_control_stage_run_states
                 WHERE stage_run_id=${terminal.evidence.stageRunId}) AS stageRevision,
                (SELECT status FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${terminal.evidence.leaseId}) AS leaseStatus,
                (SELECT holder_id FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${terminal.evidence.leaseId}) AS leaseHolder,
                (SELECT fence_token FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${terminal.evidence.leaseId}) AS fenceToken
            `,
            [
              {
                stageStatus: "running",
                stageRevision: 2,
                leaseStatus: "reserved",
                leaseHolder: terminal.evidence.leaseHolderId,
                fenceToken: terminal.evidence.fenceToken,
              },
            ],
          );
        }),
      ),
    ),
);

it.effect(
  "acknowledges a provider drain only after durable Verification adoption and StageRun start",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-provider-prefix-drain",
          );
          const executorCalls = yield* Ref.make(0);
          const responseLossDefect = { _tag: "VerificationPrefixResponseLoss" } as const;
          const lossy = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            responseLossDefect,
          });
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(lossy.processHandoff(prepared.handoffId))),
          );
          const ambiguous = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(ambiguous.delivery.state, "ambiguous");
          assert.equal(ambiguous.delivery.providerTurnId, null);

          const attemptScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(attemptScope, Exit.void));
          const adoptionEntered = yield* Deferred.make<void>();
          const releaseAdoption = yield* Deferred.make<void>();
          const backingStore = prepared.coordinator.handoffStore;
          const instrumentedStore = AgentControlVerificationHandoffStore.of({
            ...backingStore,
            observeProviderStarted: (input) =>
              Deferred.succeed(adoptionEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseAdoption)),
                Effect.andThen(backingStore.observeProviderStarted(input)),
              ),
          });
          const coordinator = {
            ...prepared.coordinator,
            handoffStore: instrumentedStore,
          } satisfies VerificationTurnCoordinatorHarness;
          const providerPublications = yield* PubSub.unbounded<ProviderRuntimeEventPublication>();
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: attemptScope,
            coordinator,
            executorCalls,
            providerPublications,
          });
          const providerSubscription = yield* consumer.subscribeProviderEvents.pipe(
            Scope.provide(attemptScope),
          );
          const consumerActivation = yield* consumer
            .prepare(providerSubscription, Effect.void)
            .pipe(Scope.provide(attemptScope));
          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlB,
            scope: attemptScope,
            coordinator,
            planningFinalizer: prepared.planningFinalizer,
          });
          yield* starter.prepare(Effect.void).pipe(Scope.provide(attemptScope));

          const token = {
            id: 41,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
          const providerTurnId = TurnId.make("verification-provider-prefix-turn");
          yield* PubSub.publish(providerPublications, {
            _tag: "Event",
            event: {
              type: "turn.started",
              eventId: EventId.make("verification-provider-prefix-started"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ambiguous.evidence.providerInstanceId,
              threadId: ambiguous.evidence.threadId,
              createdAt: providerAcceptedAt,
              turnId: providerTurnId,
              payload: {},
            },
          });
          yield* PubSub.publish(providerPublications, { _tag: "Drain", token });
          const drain = yield* consumerActivation
            .drainProviderEvents(token)
            .pipe(Effect.forkChild({ startImmediately: true }));

          yield* Deferred.await(adoptionEntered);
          assert.isUndefined(drain.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));
          yield* Deferred.succeed(releaseAdoption, undefined);
          assert.isTrue(Exit.isSuccess(yield* Fiber.await(drain)));

          const adopted = Option.getOrThrow(
            yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(adopted.delivery.state, "provider-started");
          assert.equal(adopted.delivery.providerTurnId, providerTurnId);
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id = ${adopted.evidence.stageRunId}
            `,
            [{ status: "running", revision: 2 }],
          );
          assert.equal(yield* Ref.get(executorCalls), 1);
        }),
      ),
    ),
);

it.effect(
  "Verification fast provider terminal CAS waits for provider-prefix and StageRun shutdown drain",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-fast-provider-terminal-shutdown",
          );
          const initialClaim = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          const [durableAuthorityBefore] = yield* prepared.database.sqlA<{
            readonly leaseStatus: string;
            readonly leaseRevision: number;
            readonly leaseHolder: string;
            readonly leaseFenceToken: number;
            readonly leaseAttemptId: string;
            readonly leaseStageRunId: string;
            readonly leaseEvents: number;
            readonly worktreeStatus: string;
            readonly worktreeRevision: number;
            readonly worktreeStageRunId: string;
            readonly worktreeAttemptId: string;
            readonly worktreeLeaseId: string;
            readonly worktreeFenceToken: number;
          }>`
            SELECT lease.status AS "leaseStatus", lease.revision AS "leaseRevision",
              lease.holder_id AS "leaseHolder", lease.fence_token AS "leaseFenceToken",
              lease.attempt_id AS "leaseAttemptId", lease.stage_run_id AS "leaseStageRunId",
              (SELECT count(*) FROM agent_control_events
               WHERE aggregate_kind='stage-run-lease'
                 AND stream_id=lease.lease_id) AS "leaseEvents",
              worktree.status AS "worktreeStatus", worktree.revision AS "worktreeRevision",
              worktree.stage_run_id AS "worktreeStageRunId",
              worktree.attempt_id AS "worktreeAttemptId",
              worktree.lease_id AS "worktreeLeaseId",
              worktree.fence_token AS "worktreeFenceToken"
            FROM agent_control_stage_run_lease_states lease
            JOIN agent_control_worktree_reservation_states worktree
              ON worktree.reservation_id=${initialClaim.evidence.worktreeReservationId}
            WHERE lease.lease_id=${initialClaim.evidence.leaseId}
          `;
          assert.isDefined(durableAuthorityBefore);
          const resourcesScope = yield* Scope.make("sequential");
          const resourcesFinalized = yield* Ref.make(false);
          yield* Scope.addFinalizer(resourcesScope, Ref.set(resourcesFinalized, true));
          yield* Effect.addFinalizer(() => Scope.close(resourcesScope, Exit.void));
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);

          const providerTurnId = TurnId.make("verification-fast-provider-turn");
          const startedAt = "2026-08-02T08:04:00.000Z";
          const fastTerminalAt = "2026-08-02T08:03:00.000Z";
          const startedEvent = {
            type: "turn.started",
            eventId: EventId.make("verification-fast-provider-started"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: initialClaim.evidence.providerInstanceId,
            threadId: initialClaim.evidence.threadId,
            turnId: providerTurnId,
            createdAt: startedAt,
            payload: {},
          } satisfies ProviderRuntimeEvent;
          const terminalEvent = {
            type: "turn.completed",
            eventId: EventId.make("verification-fast-provider-terminal"),
            provider: startedEvent.provider,
            providerInstanceId: startedEvent.providerInstanceId,
            threadId: startedEvent.threadId,
            turnId: providerTurnId,
            createdAt: fastTerminalAt,
            payload: { state: "completed" },
          } satisfies ProviderRuntimeEvent;

          const releaseAdapterChunk = yield* Deferred.make<void>();
          const adapterSendEntered = yield* Deferred.make<void>();
          const terminalPublished = yield* Deferred.make<void>();
          const releaseAdapterReturn = yield* Deferred.make<void>();
          const adapterSendReturned = yield* Deferred.make<void>();
          const chunkAccepted = yield* Deferred.make<void>();
          const providerSourceDrainEntered = yield* Deferred.make<void>();
          const releaseProviderSource = yield* Deferred.make<void>();
          const pullCount = yield* Ref.make(0);
          const pullCountAtTerminalAcceptance = yield* Ref.make(0);
          const acceptedEventIds = yield* Ref.make<ReadonlyArray<string>>([]);
          const sessions = new Map<ThreadId, ProviderSession>();
          const adapter: ProviderAdapterShape<ProviderAdapterError> = {
            provider: startedEvent.provider,
            capabilities: { sessionModelSwitch: "in-session" },
            startSession: (input) =>
              Effect.sync(() => {
                const session: ProviderSession = {
                  provider: startedEvent.provider,
                  providerInstanceId: startedEvent.providerInstanceId,
                  status: "ready",
                  runtimeMode: input.runtimeMode,
                  threadId: input.threadId,
                  resumeCursor: { cursor: `resume-${input.threadId}` },
                  cwd: input.cwd ?? process.cwd(),
                  ...(input.modelSelection === undefined
                    ? {}
                    : { model: input.modelSelection.model }),
                  createdAt,
                  updatedAt: createdAt,
                };
                const attested = attestProviderSessionNativeConfiguration(
                  session,
                  input.modelSelection ?? null,
                );
                sessions.set(session.threadId, attested);
                return attested;
              }),
            sendTurn: (input) =>
              Effect.sync(() => {
                const session = sessions.get(input.threadId);
                if (session !== undefined) {
                  sessions.set(input.threadId, {
                    ...session,
                    status: "running",
                    activeTurnId: providerTurnId,
                    updatedAt: startedAt,
                  });
                }
              }).pipe(
                Effect.andThen(Deferred.succeed(adapterSendEntered, undefined)),
                Effect.andThen(Deferred.succeed(releaseAdapterChunk, undefined)),
                Effect.andThen(Deferred.await(terminalPublished)),
                Effect.andThen(Deferred.await(releaseAdapterReturn)),
                Effect.tap(() =>
                  Effect.sync(() => {
                    const session = sessions.get(input.threadId);
                    if (session !== undefined) {
                      sessions.set(input.threadId, {
                        ...session,
                        status: "ready",
                        activeTurnId: undefined,
                        updatedAt: fastTerminalAt,
                      });
                    }
                  }),
                ),
                Effect.andThen(Deferred.succeed(adapterSendReturned, undefined)),
                Effect.as({ threadId: input.threadId, turnId: providerTurnId }),
              ),
            prepareTurn: (input) =>
              input.modelSelection === undefined
                ? Effect.fail(
                    new ProviderAdapterRequestError({
                      provider: startedEvent.provider,
                      method: "thread.turn.start",
                      detail: "model selection required",
                    }),
                  )
                : Effect.succeed({
                    attestation: attestProviderNativeTurnConfiguration(input.modelSelection),
                    invoke: (entry) =>
                      entry
                        .adapterEntered()
                        .pipe(Effect.andThen(entry.startExternal(() => adapter.sendTurn(input)))),
                  }),
            interruptTurn: () => Effect.void,
            respondToRequest: () => Effect.void,
            respondToUserInput: () => Effect.void,
            stopSession: (threadId) =>
              Effect.sync(() => {
                sessions.delete(threadId);
              }),
            listSessions: () => Effect.sync(() => Array.from(sessions.values())),
            hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
            readThread: (threadId) => Effect.succeed({ threadId, turns: [] }),
            rollbackThread: (threadId) => Effect.succeed({ threadId, turns: [] }),
            stopAll: () =>
              Effect.sync(() => {
                sessions.clear();
              }),
            streamEvents: Stream.fromEffect(Deferred.await(releaseAdapterChunk)).pipe(
              Stream.flatMap(() => Stream.fromIterable([startedEvent, terminalEvent])),
              Stream.concat(
                Stream.fromEffect(
                  Deferred.succeed(providerSourceDrainEntered, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseProviderSource)),
                  ),
                ).pipe(Stream.drain),
              ),
            ),
          };
          const defaultProviderAdapterRegistry = makeAdapterRegistryMock({
            [startedEvent.provider]: adapter,
          });
          const providerAdapterRegistry = {
            ...defaultProviderAdapterRegistry,
            getByInstance: (instanceId) =>
              instanceId === startedEvent.providerInstanceId
                ? Effect.succeed(adapter)
                : defaultProviderAdapterRegistry.getByInstance(instanceId),
            getInstanceInfo: (instanceId) =>
              instanceId === startedEvent.providerInstanceId
                ? Effect.succeed({
                    instanceId,
                    driverKind: startedEvent.provider,
                    displayName: undefined,
                    enabled: true,
                    continuationIdentity: {
                      driverKind: startedEvent.provider,
                      continuationKey: `${startedEvent.provider}:instance:${instanceId}`,
                    },
                  })
                : defaultProviderAdapterRegistry.getInstanceInfo(instanceId),
            listInstances: () => Effect.succeed([startedEvent.providerInstanceId]),
          } satisfies ProviderAdapterRegistry.ProviderAdapterRegistryShape;

          const sqlBLayer = Layer.succeed(SqlClient.SqlClient, prepared.database.sqlB);
          const runtimeRepositoryContext = yield* Layer.buildWithScope(
            Layer.fresh(ProviderSessionRuntime.layer).pipe(
              Layer.provide(sqlBLayer),
              Layer.provideMerge(NodeServices.layer),
            ),
            resourcesScope,
          );
          const runtimeRepository = Context.get(
            runtimeRepositoryContext,
            ProviderSessionRuntime.ProviderSessionRuntimeRepository,
          );
          const directoryContext = yield* Layer.buildWithScope(
            Layer.fresh(ProviderSessionDirectoryLive).pipe(
              Layer.provide(
                Layer.succeed(
                  ProviderSessionRuntime.ProviderSessionRuntimeRepository,
                  runtimeRepository,
                ),
              ),
            ),
            resourcesScope,
          );
          const directory = Context.get(directoryContext, ProviderSessionDirectory);
          const providerContext = yield* Layer.buildWithScope(
            Layer.fresh(
              makeProviderServiceLive({
                runtimeEventLifecycleObserver: {
                  beforePull: () => Ref.update(pullCount, (count) => count + 1),
                  onAccepted: (event) =>
                    Ref.updateAndGet(acceptedEventIds, (ids) => [
                      ...ids,
                      String(event.eventId),
                    ]).pipe(
                      Effect.tap(() =>
                        event.eventId === terminalEvent.eventId
                          ? Ref.get(pullCount).pipe(
                              Effect.tap((count) => Ref.set(pullCountAtTerminalAcceptance, count)),
                              Effect.andThen(Deferred.succeed(chunkAccepted, undefined)),
                            )
                          : Effect.void,
                      ),
                      Effect.asVoid,
                    ),
                  afterLifecyclePublish: (event) =>
                    event.eventId === terminalEvent.eventId
                      ? Deferred.succeed(terminalPublished, undefined).pipe(Effect.asVoid)
                      : Effect.void,
                },
              }),
            ).pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(
                    ProviderAdapterRegistry.ProviderAdapterRegistry,
                    providerAdapterRegistry,
                  ),
                  Layer.succeed(ProviderSessionDirectory, directory),
                  ServerSettingsService.layerTest(),
                  AnalyticsService.layerTest,
                  Layer.succeed(
                    ProviderEventLoggers.ProviderEventLoggers,
                    ProviderEventLoggers.NoOpProviderEventLoggers,
                  ),
                ),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
            resourcesScope,
          );
          const provider = Context.get(providerContext, ProviderService);
          // This legacy fixture recursively embeds predecessor evidence beyond the public
          // message-size limit. Normalize only that irrelevant body before delegating the
          // complete pre-invoke boundary and adapter call to the real ProviderService.
          const turnDeliveryProvider = ProviderService.of({
            ...provider,
            sendTurnAtPreInvokeBoundary: (input, boundary) =>
              provider.sendTurnAtPreInvokeBoundary!(
                {
                  ...input,
                  input: "Run the prepared focused Verification turn.",
                },
                boundary,
              ),
          });

          const providerRegistry = ProviderRegistry.of({
            getProviders: Effect.succeed([]),
            refresh: () => Effect.succeed([]),
            refreshInstance: () => Effect.succeed([]),
            getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
            setProviderMaintenanceActionState: () => Effect.succeed([]),
            streamChanges: Stream.empty,
          });
          const executorContext = yield* Layer.buildWithScope(
            Layer.fresh(ProviderTurnRequestExecutorLive).pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(SqlClient.SqlClient, prepared.database.sqlA),
                  Layer.succeed(OrchestrationEngineService, prepared.coordinator.orchestration),
                  Layer.succeed(ProjectionSnapshotQuery, prepared.coordinator.snapshots),
                  Layer.succeed(ProviderService, turnDeliveryProvider),
                  Layer.succeed(ProviderRegistry, providerRegistry),
                ),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
            resourcesScope,
          );
          const executor = Context.get(executorContext, ProviderTurnRequestExecutor);

          const runtimeTerminalPersisted = yield* Deferred.make<void>();
          const releaseRuntimeIngestion = yield* Deferred.make<void>();
          const runtimeOrchestration = OrchestrationEngineService.of({
            ...prepared.coordinator.orchestration,
            dispatch: (command) =>
              prepared.coordinator.orchestration
                .dispatch(command)
                .pipe(
                  Effect.tap(() =>
                    command.type === "thread.session.set" &&
                    command.providerRuntimeLifecycle?.runtimeEventId === terminalEvent.eventId
                      ? Deferred.succeed(runtimeTerminalPersisted, undefined).pipe(
                          Effect.andThen(Deferred.await(releaseRuntimeIngestion)),
                        )
                      : Effect.void,
                  ),
                ),
          });
          const runtimeIngestionContext = yield* Layer.buildWithScope(
            Layer.fresh(ProviderRuntimeIngestionLive).pipe(
              Layer.provide(
                Layer.mergeAll(
                  sqlBLayer,
                  Layer.succeed(OrchestrationEngineService, runtimeOrchestration),
                  Layer.succeed(ProjectionSnapshotQuery, prepared.coordinator.snapshots),
                  Layer.succeed(ProviderService, provider),
                  Layer.succeed(ProviderSessionDirectory, directory),
                  ServerSettingsService.layerTest(),
                ),
              ),
              Layer.provideMerge(NodeServices.layer),
            ),
            resourcesScope,
          );
          const runtimeIngestion = Context.get(
            runtimeIngestionContext,
            ProviderRuntimeIngestionService,
          );

          const terminalCasEntered = yield* Deferred.make<void>();
          const releaseTerminalCas = yield* Deferred.make<void>();
          const terminalCasCommitted = yield* Deferred.make<void>();
          const releaseTerminalAcknowledgement = yield* Deferred.make<void>();
          const terminalHookReleased = yield* Deferred.make<void>();
          const stageStartCommitted = yield* Deferred.make<void>();
          const releaseStageStarter = yield* Deferred.make<void>();
          const stageDrainEntered = yield* Deferred.make<void>();
          yield* Scope.addFinalizer(
            resourcesScope,
            Effect.all(
              [
                Deferred.succeed(releaseAdapterChunk, undefined),
                Deferred.succeed(releaseAdapterReturn, undefined),
                Deferred.succeed(releaseProviderSource, undefined),
                Deferred.succeed(releaseRuntimeIngestion, undefined),
                Deferred.succeed(releaseTerminalCas, undefined),
                Deferred.succeed(releaseTerminalAcknowledgement, undefined),
                Deferred.succeed(releaseStageStarter, undefined),
              ],
              { concurrency: "unbounded", discard: true },
            ),
          );
          const backingWakeup = prepared.coordinator.wakeup;
          const instrumentedWakeup = AgentControlVerificationTurnWakeup.of({
            ...backingWakeup,
            ...(backingWakeup.drainStageStarter === undefined
              ? {}
              : {
                  drainStageStarter: Deferred.succeed(stageDrainEntered, undefined).pipe(
                    Effect.andThen(backingWakeup.drainStageStarter),
                  ),
                }),
          });
          const coordinator = {
            ...prepared.coordinator,
            wakeup: instrumentedWakeup,
          } satisfies VerificationTurnCoordinatorHarness;
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: resourcesScope,
            coordinator,
            executorCalls,
            providerService: provider,
            executorService: executor,
            hooks: {
              ...noopVerificationConsumerHooks,
              beforeProviderTerminalCas: () =>
                Deferred.await(runtimeTerminalPersisted).pipe(
                  Effect.andThen(Deferred.await(stageStartCommitted)),
                  Effect.andThen(Deferred.succeed(terminalCasEntered, undefined)),
                  Effect.andThen(Deferred.await(releaseTerminalCas)),
                ),
              afterProviderTerminalCas: () =>
                Deferred.succeed(terminalCasCommitted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseTerminalAcknowledgement)),
                  Effect.andThen(Deferred.succeed(terminalHookReleased, undefined)),
                ),
            },
          });
          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlA,
            scope: resourcesScope,
            coordinator,
            planningFinalizer: prepared.planningFinalizer,
            hooks: {
              ...noopVerificationStageStarterHooks,
              afterOuterCommit: () =>
                Deferred.succeed(stageStartCommitted, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseStageStarter)),
                ),
            },
          });

          const runtimeSubscription = yield* runtimeIngestion.subscribeProviderEvents.pipe(
            Scope.provide(resourcesScope),
          );
          const verificationSubscription = yield* consumer.subscribeProviderEvents.pipe(
            Scope.provide(resourcesScope),
          );
          const source = yield* runtimeIngestion.startProviderRuntimeEventSources.pipe(
            Scope.provide(resourcesScope),
          );
          yield* attempt.activation.registerTerminalAbort(source.abort);
          const runtimeActivation = yield* runtimeIngestion
            .start(runtimeSubscription, source.awaitAbort)
            .pipe(Scope.provide(resourcesScope));
          const verificationGate = yield* Deferred.make<void>();
          const verificationActivation = yield* consumer
            .prepare(verificationSubscription, Deferred.await(verificationGate), source.awaitAbort)
            .pipe(Scope.provide(resourcesScope));
          yield* starter.prepare(attempt.activation.await).pipe(Scope.provide(resourcesScope));
          const closeToken = yield* Deferred.make<{
            readonly runtimeIngestionAcknowledgement: Deferred.Deferred<void, Error>;
            readonly verificationAcknowledgement: Deferred.Deferred<void, Error>;
          }>();
          yield* attempt.activation.registerShutdownDrain(
            Effect.gen(function* () {
              const quiesce = yield* source.quiesce;
              yield* Deferred.succeed(closeToken, quiesce.token);
              if (Exit.isFailure(quiesce.sourceExit)) {
                return yield* Effect.failCause(quiesce.sourceExit.cause);
              }
              yield* Effect.all(
                [
                  runtimeActivation.drainProviderEvents(quiesce.token),
                  verificationActivation.drainProviderEvents(quiesce.token),
                ],
                { concurrency: "unbounded", discard: true },
              );
            }),
          );
          const handoffFiber = yield* consumer
            .processHandoff(prepared.handoffId)
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
          const handoffStart = yield* Effect.race(
            Deferred.await(adapterSendEntered).pipe(Effect.as("adapter-entered" as const)),
            Fiber.join(handoffFiber).pipe(Effect.map((exit) => ({ exit }) as const)),
          );
          assert.equal(handoffStart, "adapter-entered");
          assert.isUndefined(handoffFiber.pollUnsafe());
          yield* attempt.commit(
            runtimeIngestion.openProviderRuntimeEventPublishing.pipe(
              Effect.andThen(source.handoffAccepted),
              Effect.andThen(attempt.activation.open),
            ),
          );
          yield* Deferred.await(chunkAccepted);
          yield* Deferred.await(terminalPublished);
          assert.deepStrictEqual(yield* Ref.get(acceptedEventIds), [
            startedEvent.eventId,
            terminalEvent.eventId,
          ]);
          assert.equal(yield* Ref.get(pullCountAtTerminalAcceptance), 1);
          assert.isFalse(yield* Deferred.isDone(adapterSendReturned));
          assert.isUndefined(handoffFiber.pollUnsafe());
          yield* Deferred.succeed(verificationGate, undefined);

          yield* Deferred.await(terminalCasEntered);
          const providerStarted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(providerStarted.delivery.state, "provider-started");
          assert.equal(providerStarted.delivery.providerTurnId, providerTurnId);
          assert.notEqual(providerStarted.delivery.providerAcceptedAt, null);
          assert.isTrue(fastTerminalAt < providerStarted.delivery.providerAcceptedAt!);
          const expectedTerminalObservation = yield* normalizeVerificationTerminal(terminalEvent, {
            providerDeliveryId: providerStarted.evidence.providerDeliveryId,
            threadId: providerStarted.evidence.threadId,
            providerInstanceId: providerStarted.evidence.providerInstanceId,
            providerTurnId,
          });
          yield* Deferred.succeed(releaseTerminalCas, undefined);
          yield* Deferred.await(terminalCasCommitted);
          const closeFiber = yield* attempt
            .close(Exit.interrupt("verification-fast-provider-parent-close" as never))
            .pipe(Effect.forkChild({ startImmediately: true }));
          assert.isUndefined(handoffFiber.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(adapterSendReturned));
          yield* Deferred.await(providerSourceDrainEntered);
          assert.isUndefined(closeFiber.pollUnsafe());
          assert.isFalse(yield* Ref.get(resourcesFinalized));
          yield* Deferred.succeed(releaseProviderSource, undefined);
          const token = yield* Deferred.await(closeToken);
          assert.isUndefined(closeFiber.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(token.runtimeIngestionAcknowledgement));
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));

          const [durableTerminal] = yield* prepared.database.sqlB<{
            readonly state: string;
            readonly revision: number;
            readonly providerTurnId: string;
            readonly providerAcceptedAt: string;
            readonly terminalAt: string;
            readonly terminalEventId: string;
            readonly terminalEventType: string;
            readonly terminalProviderState: string;
            readonly terminalObservationDigest: string;
            readonly lastErrorCode: string | null;
          }>`
            SELECT state, revision, provider_turn_id AS "providerTurnId",
              provider_accepted_at AS "providerAcceptedAt", terminal_at AS "terminalAt",
              terminal_event_id AS "terminalEventId",
              terminal_event_type AS "terminalEventType",
              terminal_provider_state AS "terminalProviderState",
              terminal_observation_digest AS "terminalObservationDigest",
              last_error_code AS "lastErrorCode"
            FROM agent_control_verification_deliveries
            WHERE provider_delivery_id=${providerStarted.evidence.providerDeliveryId}
          `;
          assert.isDefined(durableTerminal);
          assert.equal(durableTerminal!.state, "completed");
          assert.equal(durableTerminal!.terminalEventId, terminalEvent.eventId);
          assert.equal(durableTerminal!.terminalEventType, terminalEvent.type);
          assert.equal(durableTerminal!.terminalProviderState, "completed");
          assert.equal(durableTerminal!.terminalAt, fastTerminalAt);
          assert.equal(
            durableTerminal!.providerAcceptedAt,
            providerStarted.delivery.providerAcceptedAt,
          );
          assert.equal(durableTerminal!.providerTurnId, providerTurnId);
          assert.equal(durableTerminal!.revision, providerStarted.delivery.revision + 1);
          assert.equal(durableTerminal!.lastErrorCode, null);
          assert.equal(
            durableTerminal!.terminalObservationDigest,
            expectedTerminalObservation.observationDigest,
          );
          assert.isTrue(durableTerminal!.terminalAt < durableTerminal!.providerAcceptedAt);
          for (const key of [
            "stageRunId",
            "attemptId",
            "leaseId",
            "leaseHolderId",
            "fenceToken",
            "worktreeReservationId",
            "worktreePath",
          ] as const) {
            assert.equal(providerStarted.evidence[key], initialClaim.evidence[key], key);
          }
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
              SELECT
                (SELECT status FROM agent_control_stage_run_states
                 WHERE stage_run_id=${providerStarted.evidence.stageRunId}) AS stageStatus,
                (SELECT revision FROM agent_control_stage_run_states
                 WHERE stage_run_id=${providerStarted.evidence.stageRunId}) AS stageRevision,
                (SELECT count(*) FROM agent_control_events
                 WHERE aggregate_kind='stage-run'
                   AND stream_id=${providerStarted.evidence.stageRunId}) AS stageEvents,
                (SELECT status FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS leaseStatus,
                (SELECT revision FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS leaseRevision,
                (SELECT holder_id FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS leaseHolder,
                (SELECT fence_token FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS fenceToken,
                (SELECT attempt_id FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS leaseAttemptId,
                (SELECT stage_run_id FROM agent_control_stage_run_lease_states
                 WHERE lease_id=${providerStarted.evidence.leaseId}) AS leaseStageRunId,
                (SELECT count(*) FROM agent_control_events
                 WHERE aggregate_kind='stage-run-lease'
                   AND stream_id=${providerStarted.evidence.leaseId}) AS leaseEvents,
                (SELECT count(*) FROM agent_control_verification_stage_started_evidence
                 WHERE provider_delivery_id=${providerStarted.evidence.providerDeliveryId})
                  AS startEvidence,
                (SELECT count(*) FROM agent_control_verification_stage_started_receipts
                 WHERE provider_delivery_id=${providerStarted.evidence.providerDeliveryId})
                  AS startReceipt,
                (SELECT count(*) FROM agent_control_verification_stage_started_markers
                 WHERE provider_delivery_id=${providerStarted.evidence.providerDeliveryId})
                  AS startMarker,
                (SELECT status FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeStatus,
                (SELECT revision FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeRevision,
                (SELECT stage_run_id FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeStageRunId,
                (SELECT attempt_id FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeAttemptId,
                (SELECT lease_id FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeLeaseId,
                (SELECT fence_token FROM agent_control_worktree_reservation_states
                 WHERE reservation_id=${providerStarted.evidence.worktreeReservationId})
                  AS worktreeFenceToken
            `,
            [
              {
                stageStatus: "running",
                stageRevision: 2,
                stageEvents: 2,
                leaseStatus: durableAuthorityBefore!.leaseStatus,
                leaseRevision: durableAuthorityBefore!.leaseRevision,
                leaseHolder: durableAuthorityBefore!.leaseHolder,
                fenceToken: durableAuthorityBefore!.leaseFenceToken,
                leaseAttemptId: durableAuthorityBefore!.leaseAttemptId,
                leaseStageRunId: durableAuthorityBefore!.leaseStageRunId,
                leaseEvents: durableAuthorityBefore!.leaseEvents,
                startEvidence: 1,
                startReceipt: 1,
                startMarker: 1,
                worktreeStatus: durableAuthorityBefore!.worktreeStatus,
                worktreeRevision: durableAuthorityBefore!.worktreeRevision,
                worktreeStageRunId: durableAuthorityBefore!.worktreeStageRunId,
                worktreeAttemptId: durableAuthorityBefore!.worktreeAttemptId,
                worktreeLeaseId: durableAuthorityBefore!.worktreeLeaseId,
                worktreeFenceToken: durableAuthorityBefore!.worktreeFenceToken,
              },
            ],
          );
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
              SELECT count(*) AS count
              FROM orchestration_events
              WHERE stream_id=${providerStarted.evidence.threadId}
                AND json_extract(
                  metadata_json,
                  '$.providerRuntimeLifecycle.runtimeEventId'
                )=${terminalEvent.eventId}
            `,
            [{ count: 1 }],
          );

          yield* Deferred.succeed(releaseAdapterReturn, undefined);
          yield* Deferred.await(adapterSendReturned);
          assert.isTrue(Exit.isSuccess(yield* Fiber.join(handoffFiber)));
          assert.isUndefined(closeFiber.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(token.runtimeIngestionAcknowledgement));
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));

          yield* Deferred.succeed(releaseRuntimeIngestion, undefined);
          yield* Deferred.await(token.runtimeIngestionAcknowledgement);
          assert.isUndefined(closeFiber.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));
          yield* Deferred.succeed(releaseTerminalAcknowledgement, undefined);
          yield* Deferred.await(terminalHookReleased);
          yield* Deferred.await(stageDrainEntered);
          assert.isUndefined(closeFiber.pollUnsafe());
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));
          assert.isFalse(yield* Ref.get(resourcesFinalized));

          yield* Deferred.succeed(releaseStageStarter, undefined);
          assert.isTrue(Exit.isSuccess(yield* Fiber.await(closeFiber)));
          assert.isTrue(yield* Deferred.isDone(token.runtimeIngestionAcknowledgement));
          assert.isTrue(yield* Deferred.isDone(token.verificationAcknowledgement));
          assert.isTrue(yield* Ref.get(resourcesFinalized));

          const replayConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
          });
          const [changesBeforeReplay] = yield* prepared.database.sqlA<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          assert.isDefined(changesBeforeReplay);
          yield* replayConsumer.processRuntimeEvent(terminalEvent);
          const [changesAfterReplay] = yield* prepared.database.sqlA<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          assert.isDefined(changesAfterReplay);
          assert.equal(changesAfterReplay!.changes, changesBeforeReplay!.changes);
          const replayed = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(replayed.delivery.revision, durableTerminal!.revision);
          assert.equal(
            replayed.delivery.terminalObservationDigest,
            durableTerminal!.terminalObservationDigest,
          );
        }),
      ),
    ),
);

it.effect(
  "Verification terminal CAS replays one identical winner across two native SQLite WAL connections",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery("verification-terminal-cas-race");
          const executorCalls = yield* Ref.make(0);
          const deliveryConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
          });
          yield* deliveryConsumer.processHandoff(prepared.handoffId);
          const started = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(started.delivery.state, "provider-started");

          const storeContextB = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, prepared.database.sqlB)),
              Layer.provideMerge(NodeServices.layer),
            ),
            prepared.database.scopeB,
          );
          const storeB = Context.get(storeContextB, AgentControlVerificationHandoffStore);
          const entered = yield* Ref.make(0);
          const bothEntered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const hooks: AgentControlVerificationTurnConsumerHooksShape = {
            ...noopVerificationConsumerHooks,
            beforeProviderTerminalCas: () =>
              Ref.updateAndGet(entered, (count) => count + 1).pipe(
                Effect.tap((count) =>
                  count === 2 ? Deferred.succeed(bothEntered, undefined) : Effect.void,
                ),
                Effect.andThen(Deferred.await(release)),
              ),
          };
          const consumerA = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            hooks,
          });
          const consumerB = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: prepared.database.scopeB,
            coordinator: { ...prepared.coordinator, handoffStore: storeB },
            executorCalls,
            hooks,
          });
          const fastTerminalAt = shiftIso(started.delivery.providerAcceptedAt!, -1);
          const terminalEvent = {
            type: "turn.completed",
            eventId: EventId.make("verification-runtime-terminal-race"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: started.evidence.providerInstanceId,
            threadId: started.evidence.threadId,
            turnId: TurnId.make(started.delivery.providerTurnId!),
            createdAt: fastTerminalAt,
            payload: { state: "completed" },
          } satisfies ProviderRuntimeEvent;
          const fiberA = yield* Effect.forkScoped(consumerA.processRuntimeEvent(terminalEvent));
          const fiberB = yield* Effect.forkScoped(consumerB.processRuntimeEvent(terminalEvent));
          yield* Deferred.await(bothEntered).pipe(Effect.timeout(barrierTimeout));
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(fiberA);
          yield* Fiber.join(fiberB);

          const observed = Option.getOrThrow(
            yield* storeB.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(observed.delivery.state, "completed");
          assert.equal(observed.delivery.revision, started.delivery.revision + 1);
          assert.equal(observed.delivery.terminalEventId, terminalEvent.eventId);
          assert.equal(observed.delivery.terminalAt, fastTerminalAt);
          assert.isTrue(observed.delivery.terminalAt! < observed.delivery.providerAcceptedAt!);
          assert.equal(observed.delivery.lastErrorCode, null);
          assert.equal(yield* Ref.get(entered), 2);
        }),
      ),
    ),
);

it.effect(
  "Verification terminal recovery proves one pre-059 start without timestamp equality",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-terminal-history-recovery",
          );
          const executorCalls = yield* Ref.make(0);
          const initialConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
          });
          yield* initialConsumer.processHandoff(prepared.handoffId);
          const started = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          const providerTurnId = TurnId.make(started.delivery.providerTurnId!);
          const runtimeStartedAt = shiftIso(started.delivery.providerAcceptedAt!, -2_000);
          assert.notEqual(runtimeStartedAt, started.delivery.providerAcceptedAt);
          const baseSession = {
            threadId: started.evidence.threadId,
            providerName: "codex",
            providerInstanceId: started.evidence.providerInstanceId,
            runtimeMode: started.evidence.runtimeMode,
            lastError: null,
          } as const;
          yield* prepared.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("provider:verification-history:start"),
            threadId: started.evidence.threadId,
            session: {
              ...baseSession,
              status: "running",
              activeTurnId: providerTurnId,
              updatedAt: runtimeStartedAt,
            },
            createdAt: runtimeStartedAt,
          });
          const terminalAt = shiftIso(runtimeStartedAt, 1);
          const appendTerminalReplay = Effect.fn("appendVerificationTerminalReplay")(function* (
            replay: "first" | "second" | "third",
          ) {
            yield* prepared.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:verification-history:terminal:${replay}`),
              threadId: started.evidence.threadId,
              session: {
                ...baseSession,
                status: "ready",
                activeTurnId: null,
                updatedAt: terminalAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: EventId.make("verification-history-runtime-terminal"),
                runtimeEventType: "turn.completed",
                providerInstanceId: started.evidence.providerInstanceId,
                providerTurnId,
                providerState: "cancelled",
              },
              createdAt: terminalAt,
            });
          });
          yield* appendTerminalReplay("first");
          const historyClaim = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          const historyAcceptance = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadTurnAcceptance(prepared.handoffId),
          );
          const singleTerminal = yield* loadVerificationTerminalFromOrchestrationHistory(
            prepared.database.sqlA,
            historyClaim,
            historyAcceptance,
          );
          assert.equal(singleTerminal._tag, "Ready");
          yield* appendTerminalReplay("second");
          yield* appendTerminalReplay("third");
          const replayLineage = yield* prepared.database.sqlA<{
            readonly eventId: string;
            readonly commandId: string;
            readonly causationEventId: string | null;
            readonly correlationId: string;
          }>`
            SELECT event_id AS "eventId", command_id AS "commandId",
              causation_event_id AS "causationEventId", correlation_id AS "correlationId"
            FROM orchestration_events
            WHERE stream_id=${started.evidence.threadId}
              AND json_extract(
                metadata_json,
                '$.providerRuntimeLifecycle.runtimeEventId'
              )='verification-history-runtime-terminal'
            ORDER BY stream_version
          `;
          assert.equal(replayLineage.length, 3);
          assert.equal(new Set(replayLineage.map(({ eventId }) => eventId)).size, 3);
          assert.equal(new Set(replayLineage.map(({ commandId }) => commandId)).size, 3);
          assert.isTrue(
            replayLineage.every(
              ({ commandId, causationEventId, correlationId }) =>
                causationEventId === null && correlationId === commandId,
            ),
          );
          const replayedTerminals = yield* loadVerificationTerminalFromOrchestrationHistory(
            prepared.database.sqlA,
            historyClaim,
            historyAcceptance,
          );
          assert.equal(replayedTerminals._tag, "Ready");
          if (singleTerminal._tag === "Ready" && replayedTerminals._tag === "Ready") {
            assert.deepStrictEqual(replayedTerminals.observation, singleTerminal.observation);
            assert.equal(
              replayedTerminals.observation.observationDigest,
              singleTerminal.observation.observationDigest,
            );
          }
          yield* prepared.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("provider:verification-history:suffix"),
            threadId: started.evidence.threadId,
            session: {
              ...baseSession,
              status: "ready",
              activeTurnId: null,
              updatedAt: shiftIso(terminalAt, 1),
            },
            createdAt: shiftIso(terminalAt, 1),
          });

          const storeContextB = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, prepared.database.sqlB)),
              Layer.provideMerge(NodeServices.layer),
            ),
            prepared.database.scopeB,
          );
          const storeB = Context.get(storeContextB, AgentControlVerificationHandoffStore);
          const crashing = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: prepared.database.scopeB,
            coordinator: { ...prepared.coordinator, handoffStore: storeB },
            executorCalls,
            hooks: {
              ...noopVerificationConsumerHooks,
              beforeProviderTerminalCas: () => Effect.interrupt,
            },
          });
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(crashing.processHandoff(prepared.handoffId))),
          );
          const beforeRecovery = Option.getOrThrow(
            yield* storeB.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(beforeRecovery.delivery.state, "provider-started");
          assert.equal(beforeRecovery.delivery.revision, started.delivery.revision);
          const recovered = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: prepared.database.scopeB,
            coordinator: { ...prepared.coordinator, handoffStore: storeB },
            executorCalls,
          });
          yield* recovered.processHandoff(prepared.handoffId);
          const terminal = Option.getOrThrow(
            yield* storeB.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(terminal.delivery.state, "interrupted");
          assert.equal(terminal.delivery.terminalEventId, "verification-history-runtime-terminal");
          assert.equal(terminal.delivery.terminalProviderState, "cancelled");
          assert.equal(terminal.delivery.lastErrorCode, "provider-turn-cancelled");
          assert.equal(terminal.delivery.terminalAt, terminalAt);
          assert.isTrue(terminal.delivery.terminalAt! < terminal.delivery.providerAcceptedAt!);
          assert.equal(terminal.delivery.revision, started.delivery.revision + 1);
          const [changesBeforeReplay] = yield* prepared.database.sqlB<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          yield* recovered.processRuntimeEvent({
            type: "turn.completed",
            eventId: EventId.make("verification-history-runtime-terminal"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: started.evidence.providerInstanceId,
            threadId: started.evidence.threadId,
            turnId: providerTurnId,
            createdAt: terminalAt,
            payload: { state: "cancelled" },
          });
          const [changesAfterReplay] = yield* prepared.database.sqlB<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          assert.equal(changesAfterReplay!.changes, changesBeforeReplay!.changes);
          const replayed = Option.getOrThrow(
            yield* storeB.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(replayed.delivery.revision, terminal.delivery.revision);
          assert.equal(
            replayed.delivery.terminalObservationDigest,
            terminal.delivery.terminalObservationDigest,
          );
        }),
      ),
    ),
);

it.effect(
  "Verification terminal history rejects every later lifecycle or envelope-lineage conflict",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const variants = [
            "foreign-provider-after",
            "foreign-turn-after",
            "runtime-event-after",
            "foreign-after-replays",
            "foreign-before-match",
            "causation-after-match",
            "correlation-after-match",
          ] as const;
          const executorCalls = yield* Ref.make(0);

          for (const variant of variants) {
            const database = yield* makeSharedDatabase();
            const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
            const prepared = yield* prepareVerificationTurnDelivery(
              `verification-terminal-conflict-${variant}`,
              false,
              { database, planningFinalizer },
            );
            const consumer = yield* buildVerificationTurnConsumer({
              sql: database.sqlA,
              scope: database.scopeA,
              coordinator: prepared.coordinator,
              executorCalls,
            });
            yield* consumer.processHandoff(prepared.handoffId);
            const claim = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            );
            const acceptance = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadTurnAcceptance(prepared.handoffId),
            );
            assert.equal(claim.delivery.state, "provider-started", variant);
            const providerTurnId = TurnId.make(claim.delivery.providerTurnId!);
            const startAt = shiftIso(claim.delivery.providerAcceptedAt!, -2);
            const terminalAt = shiftIso(startAt, 1);
            const runtimeEventId = EventId.make(`runtime:${variant}:terminal`);
            const baseSession = {
              threadId: claim.evidence.threadId,
              providerName: "codex" as const,
              providerInstanceId: claim.evidence.providerInstanceId,
              runtimeMode: claim.evidence.runtimeMode,
              lastError: null,
            };
            yield* prepared.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:${variant}:start`),
              threadId: claim.evidence.threadId,
              session: {
                ...baseSession,
                status: "running",
                activeTurnId: providerTurnId,
                updatedAt: startAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: EventId.make(`runtime:${variant}:start`),
                runtimeEventType: "turn.started",
                providerInstanceId: claim.evidence.providerInstanceId,
                providerTurnId,
              },
              createdAt: startAt,
            });

            const appendTerminal = Effect.fn("appendConflictingVerificationTerminal")(function* (
              suffix: string,
              divergence: "none" | "provider" | "turn" | "runtime-event",
            ) {
              const commandId = CommandId.make(`provider:${variant}:terminal:${suffix}`);
              yield* prepared.coordinator.orchestration.dispatch({
                type: "thread.session.set",
                commandId,
                threadId: claim.evidence.threadId,
                session: {
                  ...baseSession,
                  status: "ready",
                  activeTurnId: null,
                  updatedAt: terminalAt,
                },
                providerRuntimeLifecycle: {
                  runtimeEventId:
                    divergence === "runtime-event"
                      ? EventId.make(`runtime:${variant}:replacement`)
                      : runtimeEventId,
                  runtimeEventType: "turn.completed",
                  providerInstanceId:
                    divergence === "provider"
                      ? ProviderInstanceId.make(`foreign-provider-${variant}`)
                      : claim.evidence.providerInstanceId,
                  providerTurnId:
                    divergence === "turn" ? TurnId.make(`foreign-turn-${variant}`) : providerTurnId,
                  providerState: "completed",
                },
                createdAt: terminalAt,
              });
              return commandId;
            });

            let corruptedLineageCommandId: CommandId | undefined;
            if (variant === "foreign-before-match") {
              yield* appendTerminal("foreign", "turn");
              yield* appendTerminal("matching", "none");
            } else {
              yield* appendTerminal("matching", "none");
              if (variant === "foreign-after-replays") {
                yield* appendTerminal("replay-1", "none");
                yield* appendTerminal("replay-2", "none");
              }
              corruptedLineageCommandId = yield* appendTerminal(
                "conflicting",
                variant === "foreign-provider-after" || variant === "foreign-after-replays"
                  ? "provider"
                  : variant === "foreign-turn-after"
                    ? "turn"
                    : variant === "runtime-event-after"
                      ? "runtime-event"
                      : "none",
              );
            }

            if (variant === "causation-after-match" || variant === "correlation-after-match") {
              yield* Effect.sync(() => {
                const native = new NodeSqlite.DatabaseSync(database.filename);
                try {
                  native
                    .prepare(
                      variant === "causation-after-match"
                        ? "UPDATE orchestration_events SET causation_event_id='unexpected-causation' WHERE command_id=?"
                        : "UPDATE orchestration_events SET correlation_id='unrelated-correlation' WHERE command_id=?",
                    )
                    .run(corruptedLineageCommandId!);
                } finally {
                  native.close();
                }
              });
            }

            const projected = yield* database.sqlB<{
              readonly status: string;
              readonly updatedAt: string;
            }>`
              SELECT status, updated_at AS "updatedAt"
              FROM projection_thread_sessions
              WHERE thread_id=${claim.evidence.threadId}
            `;
            assert.deepStrictEqual(
              projected,
              [{ status: "ready", updatedAt: terminalAt }],
              variant,
            );
            const historyError = yield* Effect.flip(
              loadVerificationTerminalFromOrchestrationHistory(database.sqlB, claim, acceptance),
            );
            assert.instanceOf(
              historyError,
              AgentControlVerificationOrchestrationHistoryError,
              variant,
            );
            assert.equal(historyError.reason, "terminal-conflict", variant);
            const unchanged = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            );
            assert.equal(unchanged.delivery.state, "provider-started", variant);
            assert.equal(unchanged.delivery.revision, claim.delivery.revision, variant);
            assert.equal(unchanged.delivery.terminalEventId, null, variant);
          }
        }),
      ),
    ),
);

it.effect(
  "Verification terminal history fails closed for every corrupt authoritative session row",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const executorCalls = yield* Ref.make(0);
          const corruptionCases = [
            "payload-json",
            "metadata-json",
            "payload-utf8",
            "metadata-utf8",
            "payload-blob",
            "metadata-blob",
            "both-blob",
            "payload-integer",
            "metadata-real",
            "payload-null",
            "payload-thread-id",
            "metadata-additional",
            "projection-status-blob",
          ] as const;

          for (const corruption of corruptionCases) {
            const database = yield* makeSharedDatabase();
            const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
            const native = yield* Effect.acquireRelease(
              Effect.sync(() => {
                const connection = new NodeSqlite.DatabaseSync(database.filename);
                connection.exec(
                  "PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON",
                );
                return connection;
              }),
              (connection) => Effect.sync(() => connection.close()),
            );
            const prepared = yield* prepareVerificationTurnDelivery(
              `verification-terminal-history-${corruption}`,
              false,
              { database, planningFinalizer },
            );
            const consumer = yield* buildVerificationTurnConsumer({
              sql: database.sqlA,
              scope: database.scopeA,
              coordinator: prepared.coordinator,
              executorCalls,
            });
            yield* consumer.processHandoff(prepared.handoffId);
            const claim = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            );
            assert.equal(claim.delivery.state, "provider-started", corruption);
            const acceptance = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadTurnAcceptance(prepared.handoffId),
            );
            const providerTurnId = TurnId.make(claim.delivery.providerTurnId!);
            const startAt = shiftIso(claim.delivery.providerAcceptedAt!, -3);
            const firstTerminalAt = shiftIso(startAt, 1);
            const firstIsHiddenConflict =
              corruption === "payload-json" || corruption === "payload-blob";
            const laterTerminalAt = firstIsHiddenConflict ? shiftIso(startAt, 2) : firstTerminalAt;
            const baseSession = {
              threadId: claim.evidence.threadId,
              providerName: "codex",
              providerInstanceId: claim.evidence.providerInstanceId,
              runtimeMode: claim.evidence.runtimeMode,
              lastError: null,
            } as const;
            yield* prepared.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:${corruption}:start`),
              threadId: claim.evidence.threadId,
              session: {
                ...baseSession,
                status: "running",
                activeTurnId: providerTurnId,
                updatedAt: startAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: EventId.make(`runtime:${corruption}:start`),
                runtimeEventType: "turn.started",
                providerInstanceId: claim.evidence.providerInstanceId,
                providerTurnId,
              },
              createdAt: startAt,
            });
            const firstCommandId = CommandId.make(`provider:${corruption}:terminal:first`);
            const laterCommandId = CommandId.make(`provider:${corruption}:terminal:later`);
            yield* prepared.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: firstCommandId,
              threadId: claim.evidence.threadId,
              session: {
                ...baseSession,
                status: firstIsHiddenConflict ? "error" : "ready",
                activeTurnId: null,
                lastError: firstIsHiddenConflict ? "hidden-provider-failure" : null,
                updatedAt: firstTerminalAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: EventId.make(`runtime:${corruption}:terminal`),
                runtimeEventType: "turn.completed",
                providerInstanceId: claim.evidence.providerInstanceId,
                providerTurnId,
                providerState: firstIsHiddenConflict ? "failed" : "completed",
              },
              createdAt: firstTerminalAt,
            });
            yield* prepared.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: laterCommandId,
              threadId: claim.evidence.threadId,
              session: {
                ...baseSession,
                status: "ready",
                activeTurnId: null,
                updatedAt: laterTerminalAt,
              },
              providerRuntimeLifecycle: {
                runtimeEventId: EventId.make(`runtime:${corruption}:terminal`),
                runtimeEventType: "turn.completed",
                providerInstanceId: claim.evidence.providerInstanceId,
                providerTurnId,
                providerState: "completed",
              },
              createdAt: laterTerminalAt,
            });
            const corruptedCommandId = [
              "metadata-json",
              "metadata-utf8",
              "metadata-blob",
              "metadata-additional",
            ].includes(corruption)
              ? laterCommandId
              : firstCommandId;

            yield* Effect.sync(() => {
              switch (corruption) {
                case "payload-json":
                  native
                    .prepare("UPDATE orchestration_events SET payload_json='{' WHERE command_id=?")
                    .run(corruptedCommandId);
                  break;
                case "metadata-json":
                  native
                    .prepare("UPDATE orchestration_events SET metadata_json='{' WHERE command_id=?")
                    .run(corruptedCommandId);
                  break;
                case "payload-utf8":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET payload_json=CAST(X'80' AS TEXT) WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "metadata-utf8":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET metadata_json=CAST(X'80' AS TEXT) WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "payload-blob":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET payload_json=CAST(payload_json AS BLOB) WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "metadata-blob":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET metadata_json=CAST(metadata_json AS BLOB) WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "both-blob":
                  native
                    .prepare(
                      `UPDATE orchestration_events
                       SET payload_json=CAST(payload_json AS BLOB),
                           metadata_json=CAST(metadata_json AS BLOB)
                       WHERE command_id=?`,
                    )
                    .run(corruptedCommandId);
                  break;
                case "payload-integer":
                case "metadata-real":
                case "payload-null": {
                  native.exec(`
                    PRAGMA foreign_keys = OFF;
                    ALTER TABLE orchestration_events RENAME TO orchestration_events_typed_fixture;
                    CREATE TABLE orchestration_events (
                      sequence, event_id, aggregate_kind, stream_id, stream_version,
                      event_type, occurred_at, command_id, causation_event_id,
                      correlation_id, actor_kind, payload_json, metadata_json
                    );
                    INSERT INTO orchestration_events
                    SELECT sequence, event_id, aggregate_kind, stream_id, stream_version,
                      event_type, occurred_at, command_id, causation_event_id,
                      correlation_id, actor_kind, payload_json, metadata_json
                    FROM orchestration_events_typed_fixture;
                  `);
                  native
                    .prepare(
                      corruption === "payload-integer"
                        ? "UPDATE orchestration_events SET payload_json=1 WHERE command_id=?"
                        : corruption === "metadata-real"
                          ? "UPDATE orchestration_events SET metadata_json=1.5 WHERE command_id=?"
                          : "UPDATE orchestration_events SET payload_json=NULL WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                }
                case "payload-thread-id":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET payload_json=json_set(payload_json, '$.threadId', 'schema-valid-foreign-thread') WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "metadata-additional":
                  native
                    .prepare(
                      "UPDATE orchestration_events SET metadata_json=json_set(metadata_json, '$.ingestedAt', '2026-08-02T08:59:59.000Z') WHERE command_id=?",
                    )
                    .run(corruptedCommandId);
                  break;
                case "projection-status-blob":
                  native
                    .prepare(
                      "UPDATE projection_thread_sessions SET status=CAST(status AS BLOB) WHERE thread_id=?",
                    )
                    .run(claim.evidence.threadId);
                  break;
              }
            });

            const storageExpectation =
              corruption === "payload-blob"
                ? { payloadStorage: "blob", metadataStorage: "text" }
                : corruption === "metadata-blob"
                  ? { payloadStorage: "text", metadataStorage: "blob" }
                  : corruption === "both-blob"
                    ? { payloadStorage: "blob", metadataStorage: "blob" }
                    : corruption === "payload-integer"
                      ? { payloadStorage: "integer", metadataStorage: "text" }
                      : corruption === "metadata-real"
                        ? { payloadStorage: "text", metadataStorage: "real" }
                        : corruption === "payload-null"
                          ? { payloadStorage: "null", metadataStorage: "text" }
                          : undefined;
            if (storageExpectation !== undefined) {
              assert.deepStrictEqual(
                yield* database.sqlB`
                  SELECT typeof(payload_json) AS "payloadStorage",
                    typeof(metadata_json) AS "metadataStorage"
                  FROM orchestration_events
                  WHERE command_id=${corruptedCommandId}
                `,
                [storageExpectation],
                corruption,
              );
            } else if (corruption === "projection-status-blob") {
              assert.deepStrictEqual(
                yield* database.sqlB`
                  SELECT typeof(status) AS storage
                  FROM projection_thread_sessions
                  WHERE thread_id=${claim.evidence.threadId}
                `,
                [{ storage: "blob" }],
                corruption,
              );
            }

            const historyError = yield* Effect.flip(
              loadVerificationTerminalFromOrchestrationHistory(database.sqlB, claim, acceptance),
            );
            assert.instanceOf(
              historyError,
              AgentControlVerificationOrchestrationHistoryError,
              corruption,
            );
            assert.isTrue(
              historyError.reason === "corrupt-history" ||
                historyError.reason === "terminal-conflict",
              corruption,
            );
            if (storageExpectation !== undefined) {
              assert.equal(
                historyError.operation,
                storageExpectation.payloadStorage === "text"
                  ? "orchestration-metadata-storage-class"
                  : "orchestration-payload-storage-class",
                corruption,
              );
            } else if (corruption === "projection-status-blob") {
              assert.equal(
                historyError.operation,
                "session-projection-status-storage-class",
                corruption,
              );
            }
            const unchanged = Option.getOrThrow(
              yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
            );
            assert.equal(unchanged.delivery.state, "provider-started", corruption);
            assert.equal(unchanged.delivery.revision, claim.delivery.revision, corruption);
          }
        }),
      ),
    ),
);

it.effect(
  "Verification terminal recovery paginates past corrupt history to a later healthy candidate",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const executorCalls = yield* Ref.make(0);
          const candidates = yield* Effect.forEach(
            ["terminal-page-a", "terminal-page-b"],
            (suffix) =>
              Effect.gen(function* () {
                const prepared = yield* prepareVerificationTurnDelivery(
                  `verification-${suffix}`,
                  false,
                  { database, planningFinalizer },
                );
                const deliveryConsumer = yield* buildVerificationTurnConsumer({
                  sql: database.sqlA,
                  scope: database.scopeA,
                  coordinator: prepared.coordinator,
                  executorCalls,
                });
                yield* deliveryConsumer.processHandoff(prepared.handoffId);
                const started = Option.getOrThrow(
                  yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(
                    prepared.handoffId,
                  ),
                );
                assert.equal(started.delivery.state, "provider-started");
                return { ...prepared, started };
              }),
            { concurrency: 1 },
          );
          const [corrupt, healthy] = candidates.toSorted((left, right) =>
            left.handoffId.localeCompare(right.handoffId),
          );
          assert.isDefined(corrupt);
          assert.isDefined(healthy);

          const appendRuntimeHistory = Effect.fn("appendVerificationTerminalPageHistory")(
            function* (candidate: (typeof candidates)[number], divergentReplay: boolean) {
              const providerTurnId = TurnId.make(candidate.started.delivery.providerTurnId!);
              const startAt = shiftIso(candidate.started.delivery.providerAcceptedAt!, -3);
              const terminalAt = shiftIso(startAt, 1);
              const baseSession = {
                threadId: candidate.started.evidence.threadId,
                providerName: "codex" as const,
                providerInstanceId: candidate.started.evidence.providerInstanceId,
                runtimeMode: candidate.started.evidence.runtimeMode,
                lastError: null,
              };
              yield* candidate.coordinator.orchestration.dispatch({
                type: "thread.session.set",
                commandId: CommandId.make(`provider:${candidate.handoffId}:start`),
                threadId: candidate.started.evidence.threadId,
                session: {
                  ...baseSession,
                  status: "running",
                  activeTurnId: providerTurnId,
                  updatedAt: startAt,
                },
                providerRuntimeLifecycle: {
                  runtimeEventId: EventId.make(`runtime:${candidate.handoffId}:start`),
                  runtimeEventType: "turn.started",
                  providerInstanceId: candidate.started.evidence.providerInstanceId,
                  providerTurnId,
                },
                createdAt: startAt,
              });
              const runtimeEventId = EventId.make(`runtime:${candidate.handoffId}:terminal`);
              const firstTerminalCommandId = CommandId.make(
                `provider:${candidate.handoffId}:terminal:0`,
              );
              for (const [index, occurredAt] of [
                terminalAt,
                ...(divergentReplay ? [shiftIso(terminalAt, 1)] : []),
              ].entries()) {
                yield* candidate.coordinator.orchestration.dispatch({
                  type: "thread.session.set",
                  commandId:
                    index === 0
                      ? firstTerminalCommandId
                      : CommandId.make(`provider:${candidate.handoffId}:terminal:${index}`),
                  threadId: candidate.started.evidence.threadId,
                  session: {
                    ...baseSession,
                    status: "ready",
                    activeTurnId: null,
                    updatedAt: occurredAt,
                  },
                  providerRuntimeLifecycle: {
                    runtimeEventId,
                    runtimeEventType: "turn.completed",
                    providerInstanceId: candidate.started.evidence.providerInstanceId,
                    providerTurnId,
                    providerState: "completed",
                  },
                  createdAt: occurredAt,
                });
              }
              return { runtimeEventId, terminalAt, firstTerminalCommandId };
            },
          );
          const corruptHistory = yield* appendRuntimeHistory(corrupt!, true);
          const healthyTerminal = yield* appendRuntimeHistory(healthy!, false);
          const sensitiveValues = [
            "Prompt: terminal history must not disclose this",
            "/Users/private-host/verification-terminal-history",
            "credential=ghp_terminal_history_secret",
            "payload-bytes-secret",
          ] as const;
          const native = yield* Effect.acquireRelease(
            Effect.sync(() => {
              const connection = new NodeSqlite.DatabaseSync(database.filename);
              connection.exec(
                "PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON",
              );
              return connection;
            }),
            (connection) => Effect.sync(() => connection.close()),
          );
          const corruptedPayloadJson = yield* Effect.sync(() => {
            const row = native
              .prepare(
                "SELECT payload_json AS payloadJson FROM orchestration_events WHERE command_id=?",
              )
              .get(corruptHistory.firstTerminalCommandId) as
              | { readonly payloadJson: string }
              | undefined;
            assert.isDefined(row);
            const payload = decodeUnknownJson(row!.payloadJson) as {
              threadId: string;
              session: {
                threadId: string;
                providerName: string;
                lastError: string | null;
              };
            };
            payload.threadId = sensitiveValues[0];
            payload.session.threadId = sensitiveValues[1];
            payload.session.providerName = sensitiveValues[2];
            payload.session.lastError = sensitiveValues[3];
            const encoded = encodeUnknownJson(payload);
            native
              .prepare("UPDATE orchestration_events SET payload_json=? WHERE command_id=?")
              .run(Buffer.from(encoded), corruptHistory.firstTerminalCommandId);
            return encoded;
          });

          const recoveryConsumer = yield* buildVerificationTurnConsumer({
            sql: database.sqlB,
            scope: database.scopeB,
            coordinator: healthy!.coordinator,
            executorCalls,
            hooks: { ...noopVerificationConsumerHooks, recoveryPageSize: 1 },
          });
          const messages: Array<unknown> = [];
          const logger = Logger.make<unknown, void>(({ message }) => {
            if (Array.isArray(message)) messages.push(...message);
            else messages.push(message);
          });
          yield* recoveryConsumer.recover.pipe(
            Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
          );
          const renderedLogs = encodeUnknownJson(messages);
          for (const sensitive of sensitiveValues) assert.notInclude(renderedLogs, sensitive);
          assert.notInclude(renderedLogs, corruptedPayloadJson);
          assert.notInclude(renderedLogs, Buffer.from(corruptedPayloadJson).toString("hex"));
          const isolated = messages.find(
            (message): message is Record<string, unknown> =>
              typeof message === "object" &&
              message !== null &&
              "operation" in message &&
              message.operation === "orchestration-payload-storage-class",
          );
          assert.isDefined(isolated);
          assert.equal(isolated!.candidateReason, "orchestration-history-divergent");

          const corruptAfter = Option.getOrThrow(
            yield* corrupt!.coordinator.handoffStore.loadAcceptedByHandoffId(corrupt!.handoffId),
          );
          const healthyAfter = Option.getOrThrow(
            yield* healthy!.coordinator.handoffStore.loadAcceptedByHandoffId(healthy!.handoffId),
          );
          assert.equal(corruptAfter.delivery.state, "provider-started");
          assert.equal(corruptAfter.delivery.revision, corrupt!.started.delivery.revision);
          assert.equal(healthyAfter.delivery.state, "completed");
          assert.equal(healthyAfter.delivery.terminalEventId, healthyTerminal.runtimeEventId);
          assert.equal(healthyAfter.delivery.terminalAt, healthyTerminal.terminalAt);
          assert.equal(healthyAfter.delivery.revision, healthy!.started.delivery.revision + 1);
        }),
      ),
    ),
);

it.effect(
  "Verification terminal recovery keeps pre-059 session completion without lifecycle metadata waiting",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-pre-059-terminal-history",
          );
          const executorCalls = yield* Ref.make(0);
          const initialConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
          });
          yield* initialConsumer.processHandoff(prepared.handoffId);
          const started = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          const providerTurnId = TurnId.make(started.delivery.providerTurnId!);
          const providerAcceptedAt = started.delivery.providerAcceptedAt!;
          const legacyStartedAt = shiftIso(providerAcceptedAt, -1);
          assert.notEqual(legacyStartedAt, providerAcceptedAt);
          const baseSession = {
            threadId: started.evidence.threadId,
            providerName: "codex",
            providerInstanceId: started.evidence.providerInstanceId,
            runtimeMode: started.evidence.runtimeMode,
            lastError: null,
          } as const;
          yield* prepared.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("provider:verification-pre-059:start"),
            threadId: started.evidence.threadId,
            session: {
              ...baseSession,
              status: "running",
              activeTurnId: providerTurnId,
              updatedAt: legacyStartedAt,
            },
            createdAt: legacyStartedAt,
          });
          yield* prepared.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("provider:verification-pre-059:terminal"),
            threadId: started.evidence.threadId,
            session: {
              ...baseSession,
              status: "ready",
              activeTurnId: null,
              updatedAt: "2027-01-01T00:00:00.000Z",
            },
            createdAt: "2027-01-01T00:00:00.000Z",
          });

          const storeContextB = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, prepared.database.sqlB)),
              Layer.provideMerge(NodeServices.layer),
            ),
            prepared.database.scopeB,
          );
          const storeB = Context.get(storeContextB, AgentControlVerificationHandoffStore);
          const recovered = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: prepared.database.scopeB,
            coordinator: { ...prepared.coordinator, handoffStore: storeB },
            executorCalls,
          });
          yield* recovered.processHandoff(prepared.handoffId);
          const waiting = Option.getOrThrow(
            yield* storeB.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(waiting.delivery.state, "provider-started");
          assert.equal(waiting.delivery.revision, started.delivery.revision);
          assert.equal(waiting.delivery.terminalEventId, null);
        }),
      ),
    ),
);

it.effect(
  "Verification migration 059 preserves a complete real 058 delivery chain before terminal CAS",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const seedStates = [
            "pending",
            "turn-accepted",
            "claimed",
            "retry-wait",
            "delivery-attempted",
            "provider-started",
            "ambiguous",
          ] as const;
          type LegacyDeliveryState = (typeof seedStates)[number];
          interface LegacySeedRow {
            readonly targetState: LegacyDeliveryState;
            readonly prepared: Effect.Success<ReturnType<typeof prepareVerificationTurnDelivery>>;
            readonly handoffId: string;
            readonly handoffFingerprint: string;
            readonly admissionEvidenceId: string;
            readonly admissionReceiptId: string;
            readonly admissionMarkerId: string;
            readonly materializationEvidenceId: string;
            readonly materializationReceiptId: string;
            readonly materializationMarkerId: string;
            readonly projectId: string;
            readonly taskId: string;
            readonly taskRevision: number;
            readonly githubIntakeSequence: number;
            readonly sourceIdentityFingerprint: string;
            readonly stageRunId: string;
            readonly attemptId: string;
            readonly controlledThreadReservationId: AgentControlControlledThreadReservationId;
            readonly threadId: ThreadId;
            readonly planningThreadId: ThreadId;
            readonly planId: string;
            readonly proposedPlanDigest: string;
            readonly leaseId: string;
            readonly leaseHolderId: string;
            readonly fenceToken: number;
            readonly providerInstanceId: ProviderInstanceId;
            readonly runtimeMode: "approval-required";
            readonly modelSelectionJson: string;
            readonly modelSelectionFingerprint: string;
            readonly worktreePath: string;
            readonly promptText: string;
            readonly turnRequestCommandId: CommandId;
            readonly messageId: MessageId;
            readonly messageEventId: string;
            readonly turnRequestEventId: string;
            readonly messageEventTemplateJson: string;
            readonly turnRequestEventTemplateJson: string;
            readonly eventTemplateDigest: string;
            readonly providerDeliveryId: string;
            readonly createdAt: string;
          }
          interface LegacyDeliverySnapshot extends Record<string, unknown> {
            readonly state: LegacyDeliveryState;
            readonly revision: number;
            readonly claim_generation: number;
            readonly attempt_count: number;
            readonly revision_storage: string;
            readonly claim_generation_storage: string;
            readonly attempt_count_storage: string;
          }

          const database = yield* makeSharedDatabase(58);
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const decodeModelSelection = Schema.decodeUnknownEffect(
            Schema.fromJsonString(ModelSelection),
          );
          const seeds = yield* Effect.forEach(
            seedStates,
            (targetState) =>
              Effect.gen(function* () {
                const prepared = yield* prepareVerificationTurnDelivery(
                  `verification-migration-059-${targetState}`,
                  true,
                  { database, planningFinalizer },
                );
                const [raw] = yield* database.sqlA<Omit<LegacySeedRow, "targetState" | "prepared">>`
                  SELECT intent.handoff_id AS "handoffId",
                    intent.handoff_fingerprint AS "handoffFingerprint",
                    materialization.admission_evidence_id AS "admissionEvidenceId",
                    materialization.admission_receipt_id AS "admissionReceiptId",
                    intent.admission_marker_id AS "admissionMarkerId",
                    intent.materialization_evidence_id AS "materializationEvidenceId",
                    intent.materialization_receipt_id AS "materializationReceiptId",
                    marker.materialization_marker_id AS "materializationMarkerId",
                    intent.project_id AS "projectId", intent.task_id AS "taskId",
                    intent.task_revision AS "taskRevision",
                    intent.github_intake_sequence AS "githubIntakeSequence",
                    intent.source_identity_fingerprint AS "sourceIdentityFingerprint",
                    intent.stage_run_id AS "stageRunId", intent.attempt_id AS "attemptId",
                    intent.controlled_thread_reservation_id AS "controlledThreadReservationId",
                    intent.thread_id AS "threadId", intent.planning_thread_id AS "planningThreadId",
                    intent.plan_id AS "planId", intent.proposed_plan_digest AS "proposedPlanDigest",
                    intent.lease_id AS "leaseId", intent.lease_holder_id AS "leaseHolderId",
                    intent.fence_token AS "fenceToken",
                    intent.provider_instance_id AS "providerInstanceId",
                    intent.runtime_mode AS "runtimeMode",
                    intent.model_selection_json AS "modelSelectionJson",
                    intent.model_selection_fingerprint AS "modelSelectionFingerprint",
                    intent.worktree_path AS "worktreePath", intent.prompt_text AS "promptText",
                    intent.turn_request_command_id AS "turnRequestCommandId",
                    intent.message_id AS "messageId", intent.message_event_id AS "messageEventId",
                    intent.turn_request_event_id AS "turnRequestEventId",
                    intent.message_event_template_json AS "messageEventTemplateJson",
                    intent.turn_request_event_template_json AS "turnRequestEventTemplateJson",
                    intent.event_template_digest AS "eventTemplateDigest",
                    intent.provider_delivery_id AS "providerDeliveryId",
                    intent.created_at AS "createdAt"
                  FROM agent_control_verification_handoff_intents intent
                  JOIN agent_control_verification_materialization_evidence materialization
                    ON materialization.materialization_evidence_id =
                      intent.materialization_evidence_id
                  JOIN agent_control_verification_materialization_markers marker
                    ON marker.materialization_evidence_id = intent.materialization_evidence_id
                  WHERE intent.handoff_id=${prepared.handoffId}
                `;
                assert.isDefined(raw, targetState);
                return { ...raw!, targetState, prepared } satisfies LegacySeedRow;
              }),
            { concurrency: 1 },
          );
          for (const seed of seeds) {
            if (seed.targetState === "pending") continue;
            const dispatch =
              seed.prepared.coordinator.orchestration.dispatchAgentControlVerificationTurn;
            assert.isDefined(dispatch, seed.targetState);
            const modelSelection = yield* decodeModelSelection(seed.modelSelectionJson);
            yield* dispatch!(
              {
                type: "thread.turn.start",
                commandId: seed.turnRequestCommandId,
                threadId: seed.threadId,
                message: {
                  messageId: seed.messageId,
                  role: "user",
                  text: seed.promptText,
                  attachments: [],
                },
                modelSelection,
                runtimeMode: seed.runtimeMode,
                interactionMode: "default",
                sourceProposedPlan: {
                  threadId: seed.planningThreadId,
                  planId: seed.planId,
                },
                createdAt: seed.createdAt,
              },
              {
                handoffId: seed.handoffId,
                handoffFingerprint: seed.handoffFingerprint,
                controlledThreadReservationId: seed.controlledThreadReservationId,
                threadId: seed.threadId,
                planningThreadId: seed.planningThreadId,
                planId: seed.planId,
                turnRequestCommandId: seed.turnRequestCommandId,
                messageId: seed.messageId,
                messageEventId: seed.messageEventId,
                turnRequestEventId: seed.turnRequestEventId,
                messageEventTemplateJson: seed.messageEventTemplateJson,
                turnRequestEventTemplateJson: seed.turnRequestEventTemplateJson,
                eventTemplateDigest: seed.eventTemplateDigest,
              },
            );
            yield* database.sqlA.withTransaction(database.sqlA`
              UPDATE main.agent_control_verification_deliveries
              SET state='turn-accepted', revision=revision+1, updated_at=${seed.createdAt}
              WHERE provider_delivery_id=${seed.providerDeliveryId}
                AND state='pending' AND revision=0
            `);
            if (seed.targetState === "turn-accepted") continue;

            const ownerId = `migration-059-owner-${seed.targetState}`;
            const claimAt = shiftIso(seed.createdAt, 1);
            const claimExpiresAt = "2099-01-01T00:00:00.000Z";
            yield* database.sqlA.withTransaction(database.sqlA`
              UPDATE main.agent_control_verification_deliveries
              SET state='claimed', revision=revision+1, claim_owner_id=${ownerId},
                claim_generation=claim_generation+1, claim_expires_at=${claimExpiresAt},
                attempt_count=attempt_count+1, next_attempt_at=NULL, updated_at=${claimAt}
              WHERE provider_delivery_id=${seed.providerDeliveryId}
                AND state='turn-accepted' AND revision=1
            `);
            if (seed.targetState === "claimed") continue;
            if (seed.targetState === "retry-wait") {
              yield* database.sqlA.withTransaction(database.sqlA`
                UPDATE main.agent_control_verification_deliveries
                SET state='retry-wait', revision=revision+1, claim_owner_id=NULL,
                  claim_expires_at=NULL, next_attempt_at='2099-01-01T00:00:01.000Z',
                  last_error_code='provider-timeout', updated_at=${shiftIso(claimAt, 1)}
                WHERE provider_delivery_id=${seed.providerDeliveryId}
                  AND state='claimed' AND revision=2
              `);
              continue;
            }

            const sessionCreatedAt = shiftIso(claimAt, 1);
            const resumeCursorJson = canonicalJson({ cursor: `resume-${seed.targetState}` });
            yield* database.sqlA.withTransaction(
              Effect.gen(function* () {
                yield* database.sqlA`
                  INSERT INTO main.agent_control_verification_session_evidence (
                provider_delivery_id, thread_id, provider_instance_id, runtime_mode,
                cwd, model_selection_json, model_selection_fingerprint,
                session_created_at, resume_cursor_json, recorded_at
              ) VALUES (
                ${seed.providerDeliveryId}, ${seed.threadId}, ${seed.providerInstanceId},
                ${seed.runtimeMode}, ${seed.worktreePath}, ${seed.modelSelectionJson},
                ${seed.modelSelectionFingerprint}, ${sessionCreatedAt},
                ${resumeCursorJson}, ${sessionCreatedAt}
                  )
                `;
                yield* database.sqlA`
                  INSERT INTO main.agent_control_verification_delivery_attestations (
                provider_delivery_id, provider_instance_id, model_selection_json,
                model_selection_fingerprint, recorded_at
              ) VALUES (
                ${seed.providerDeliveryId}, ${seed.providerInstanceId},
                ${seed.modelSelectionJson}, ${seed.modelSelectionFingerprint},
                ${sessionCreatedAt}
                  )
                `;
                yield* database.sqlA`
                  UPDATE main.agent_control_verification_deliveries
              SET state='delivery-attempted', revision=revision+1,
                provider_session_created_at=${sessionCreatedAt},
                provider_resume_cursor_json=${resumeCursorJson}, updated_at=${sessionCreatedAt}
              WHERE provider_delivery_id=${seed.providerDeliveryId}
                AND state='claimed' AND revision=2
                AND claim_owner_id=${ownerId} AND claim_generation=1
                `;
              }),
            );
            if (seed.targetState === "delivery-attempted") continue;
            const providerTransitionAt = shiftIso(sessionCreatedAt, 1);
            if (seed.targetState === "ambiguous") {
              yield* database.sqlA.withTransaction(database.sqlA`
                UPDATE main.agent_control_verification_deliveries
                SET state='ambiguous', revision=revision+1, claim_owner_id=NULL,
                  claim_expires_at=NULL, next_attempt_at=NULL,
                  terminal_at=${providerTransitionAt},
                  last_error_code='provider-acceptance-ambiguous',
                  updated_at=${providerTransitionAt}
                WHERE provider_delivery_id=${seed.providerDeliveryId}
                  AND state='delivery-attempted' AND revision=3
              `);
              continue;
            }
            yield* database.sqlA.withTransaction(database.sqlA`
              UPDATE main.agent_control_verification_deliveries
              SET state='provider-started', revision=revision+1,
                claim_owner_id=NULL, claim_expires_at=NULL,
                provider_turn_id=${`provider-turn-${seed.targetState}`},
                provider_accepted_at=${providerTransitionAt}, last_error_code=NULL,
                updated_at=${providerTransitionAt}
              WHERE provider_delivery_id=${seed.providerDeliveryId}
                AND state='delivery-attempted' AND revision=3
            `);
          }
          const providerSeed = seeds.find((seed) => seed.targetState === "provider-started")!;
          const [providerDelivery] = yield* database.sqlA<{
            readonly revision: number;
            readonly claimGeneration: number;
            readonly attemptCount: number;
            readonly providerTurnId: TurnId;
            readonly providerAcceptedAt: string;
          }>`
            SELECT revision, claim_generation AS "claimGeneration",
              attempt_count AS "attemptCount", provider_turn_id AS "providerTurnId",
              provider_accepted_at AS "providerAcceptedAt"
            FROM main.agent_control_verification_deliveries
            WHERE provider_delivery_id=${providerSeed.providerDeliveryId}
          `;
          assert.isDefined(providerDelivery);
          const startCommandId = deriveVerificationStageStartCommandId(
            providerSeed.providerDeliveryId,
            providerDelivery!.providerTurnId,
          );
          const stageEventId = deriveVerificationStageStartEventId(startCommandId);
          const startEvidenceId = deriveVerificationStageStartEvidenceId(startCommandId);
          const startReceiptId = deriveVerificationStageStartReceiptId(startCommandId);
          const startMarkerId = deriveVerificationStageStartMarkerId(startCommandId);
          const startFingerprint = fingerprintVerificationTurn("stage-start", [
            providerSeed.admissionEvidenceId,
            providerSeed.admissionReceiptId,
            providerSeed.admissionMarkerId,
            providerSeed.materializationEvidenceId,
            providerSeed.materializationReceiptId,
            providerSeed.materializationMarkerId,
            providerSeed.handoffId,
            providerSeed.handoffFingerprint,
            providerSeed.providerDeliveryId,
            String(providerDelivery!.revision),
            String(providerDelivery!.claimGeneration),
            String(providerDelivery!.attemptCount),
            providerSeed.threadId,
            providerSeed.planningThreadId,
            providerSeed.planId,
            providerDelivery!.providerTurnId,
            stageEventId,
            providerDelivery!.providerAcceptedAt,
          ]);
          const stageDraft: AgentControlStageRunEventDraft = {
            eventId: stageEventId,
            type: "agentControl.stageRun.verificationStarted",
            aggregateKind: "stage-run",
            aggregateId: AgentControlStageRunId.make(providerSeed.stageRunId),
            occurredAt: providerDelivery!.providerAcceptedAt,
            commandId: startCommandId,
            causationEventId: EventId.make(providerSeed.turnRequestEventId),
            correlationId: startCommandId,
            authority: "system",
            metadata: { schemaVersion: 1 },
            payload: {
              projectId: ProjectId.make(providerSeed.projectId),
              taskId: AgentControlTaskId.make(providerSeed.taskId),
              stageRunId: AgentControlStageRunId.make(providerSeed.stageRunId),
              attemptId: AgentControlAttemptId.make(providerSeed.attemptId),
              roleId: "verifier",
              stageKind: "verification",
              stageOrdinal: 3,
              attemptOrdinal: 1,
              status: "running",
              taskRevision: providerSeed.taskRevision,
              githubIntakeSequence: providerSeed.githubIntakeSequence,
              sourceIdentityFingerprint: providerSeed.sourceIdentityFingerprint,
              admissionEvidenceId: providerSeed.admissionEvidenceId,
              admissionReceiptId: providerSeed.admissionReceiptId,
              admissionMarkerId: providerSeed.admissionMarkerId,
              materializationEvidenceId: providerSeed.materializationEvidenceId,
              materializationReceiptId: providerSeed.materializationReceiptId,
              materializationMarkerId: providerSeed.materializationMarkerId,
              handoffId: providerSeed.handoffId,
              handoffFingerprint: providerSeed.handoffFingerprint,
              providerDeliveryId: providerSeed.providerDeliveryId,
              deliveryRevision: providerDelivery!.revision,
              claimGeneration: providerDelivery!.claimGeneration,
              attemptCount: providerDelivery!.attemptCount,
              controlledThreadReservationId: providerSeed.controlledThreadReservationId,
              threadId: providerSeed.threadId,
              planningThreadId: providerSeed.planningThreadId,
              planId: providerSeed.planId,
              proposedPlanDigest: providerSeed.proposedPlanDigest,
              providerInstanceId: providerSeed.providerInstanceId,
              providerTurnId: providerDelivery!.providerTurnId,
              runtimeMode: providerSeed.runtimeMode,
              modelSelectionFingerprint: providerSeed.modelSelectionFingerprint,
              leaseId: AgentControlStageRunLeaseId.make(providerSeed.leaseId),
              leaseHolderId: AgentControlStageRunLeaseHolderId.make(providerSeed.leaseHolderId),
              fenceToken: providerSeed.fenceToken,
              startedAt: providerDelivery!.providerAcceptedAt,
            },
          };
          const committedStageEvents = yield* planningFinalizer.stageEvents.append({
            stageRunId: AgentControlStageRunId.make(providerSeed.stageRunId),
            expectedStreamVersion: 1,
            events: [stageDraft],
          });
          assert.lengthOf(committedStageEvents, 1);
          const stageEvent = committedStageEvents[0]!;
          yield* planningFinalizer.stageProjection.projectEvent(stageEvent);
          yield* database.sqlA`BEGIN IMMEDIATE`;
          yield* Effect.gen(function* () {
            const event = stageEvent;
            yield* database.sqlA`
                INSERT INTO agent_control_verification_stage_started_evidence (
                  start_evidence_id, start_command_id, start_fingerprint,
                  admission_evidence_id, admission_receipt_id, admission_marker_id,
                  materialization_evidence_id, materialization_receipt_id,
                  materialization_marker_id, handoff_id, handoff_fingerprint,
                  provider_delivery_id, delivery_revision, claim_generation, attempt_count,
                  project_id, task_id, task_revision, github_intake_sequence,
                  source_identity_fingerprint, stage_run_id, attempt_id,
                  controlled_thread_reservation_id, thread_id, planning_thread_id, plan_id,
                  proposed_plan_digest, lease_id, lease_holder_id, fence_token,
                  provider_instance_id, provider_turn_id, runtime_mode,
                  model_selection_fingerprint, stage_event_id, stage_event_sequence,
                  stage_event_stream_version, started_at
                ) VALUES (
                  ${startEvidenceId}, ${startCommandId}, ${startFingerprint},
                  ${providerSeed.admissionEvidenceId}, ${providerSeed.admissionReceiptId},
                  ${providerSeed.admissionMarkerId}, ${providerSeed.materializationEvidenceId},
                  ${providerSeed.materializationReceiptId},
                  ${providerSeed.materializationMarkerId}, ${providerSeed.handoffId},
                  ${providerSeed.handoffFingerprint}, ${providerSeed.providerDeliveryId},
                  ${providerDelivery!.revision}, ${providerDelivery!.claimGeneration},
                  ${providerDelivery!.attemptCount}, ${providerSeed.projectId},
                  ${providerSeed.taskId}, ${providerSeed.taskRevision},
                  ${providerSeed.githubIntakeSequence},
                  ${providerSeed.sourceIdentityFingerprint}, ${providerSeed.stageRunId},
                  ${providerSeed.attemptId}, ${providerSeed.controlledThreadReservationId},
                  ${providerSeed.threadId}, ${providerSeed.planningThreadId},
                  ${providerSeed.planId}, ${providerSeed.proposedPlanDigest},
                  ${providerSeed.leaseId}, ${providerSeed.leaseHolderId},
                  ${providerSeed.fenceToken}, ${providerSeed.providerInstanceId},
                  ${providerDelivery!.providerTurnId}, ${providerSeed.runtimeMode},
                  ${providerSeed.modelSelectionFingerprint}, ${event.eventId},
                  ${event.sequence}, ${event.streamVersion},
                  ${providerDelivery!.providerAcceptedAt}
                )
              `;
            yield* database.sqlA`
                INSERT INTO agent_control_verification_stage_started_receipts (
                  start_receipt_id, start_evidence_id, start_command_id, start_fingerprint,
                  provider_delivery_id, stage_event_id, stage_event_sequence, accepted_at
                ) VALUES (
                  ${startReceiptId}, ${startEvidenceId}, ${startCommandId},
                  ${startFingerprint}, ${providerSeed.providerDeliveryId}, ${event.eventId},
                  ${event.sequence}, ${providerDelivery!.providerAcceptedAt}
                )
              `;
            yield* database.sqlA`
                INSERT INTO agent_control_verification_stage_started_markers (
                  start_marker_id, start_evidence_id, start_receipt_id, start_command_id,
                  start_fingerprint, provider_delivery_id, stage_event_id,
                  stage_event_sequence, committed_at
                ) VALUES (
                  ${startMarkerId}, ${startEvidenceId}, ${startReceiptId}, ${startCommandId},
                  ${startFingerprint}, ${providerSeed.providerDeliveryId}, ${event.eventId},
                  ${event.sequence}, ${providerDelivery!.providerAcceptedAt}
                )
              `;
          }).pipe(Effect.tapError(() => database.sqlA`ROLLBACK`));
          yield* database.sqlA`COMMIT`;
          assert.isDefined(stageEvent);

          yield* database.sqlA`
            CREATE TABLE migration_059_restore_audit (
              ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
              restored_table TEXT NOT NULL
            )
          `;
          for (const [table, label] of [
            ["agent_control_verification_stage_started_evidence", "evidence"],
            ["agent_control_verification_stage_started_receipts", "receipt"],
            ["agent_control_verification_stage_started_markers", "marker"],
          ] as const) {
            yield* database.sqlA.unsafe(`
              CREATE TRIGGER migration_059_audit_${label}
              AFTER INSERT ON ${table}
              WHEN NEW.provider_delivery_id='${providerSeed.providerDeliveryId.replaceAll("'", "''")}'
              BEGIN
                INSERT INTO migration_059_restore_audit(restored_table) VALUES ('${label}');
              END
            `).unprepared;
          }

          const legacyColumns = (yield* database.sqlA<{ readonly name: string }>`
              SELECT name FROM pragma_table_info('agent_control_verification_deliveries')
              ORDER BY cid
            `).map(({ name }) => name);
          assert.notInclude(legacyColumns, "terminal_observation_digest");
          const companionTables = [
            "agent_control_verification_session_evidence",
            "agent_control_verification_delivery_attestations",
            "agent_control_verification_stage_started_evidence",
            "agent_control_verification_stage_started_receipts",
            "agent_control_verification_stage_started_markers",
          ] as const;
          const byteProjection = (columns: ReadonlyArray<string>) =>
            columns
              .flatMap((name) => {
                const identifier = `"${name.replaceAll('"', '""')}"`;
                return [
                  identifier,
                  `typeof(${identifier}) AS "${name}_storage"`,
                  `hex(CAST(${identifier} AS BLOB)) AS "${name}_hex"`,
                ];
              })
              .join(", ");
          const readDeliveries = (sql: SqlClient.SqlClient) =>
            sql.unsafe<LegacyDeliverySnapshot>(
              `SELECT ${byteProjection(legacyColumns)}
                 FROM agent_control_verification_deliveries ORDER BY provider_delivery_id`,
            ).unprepared;
          const companionColumns = yield* Effect.forEach(companionTables, (table) =>
            database.sqlA<{ readonly name: string }>`
              SELECT name FROM pragma_table_info(${table}) ORDER BY cid
            `.pipe(Effect.map((columns) => ({ table, columns: columns.map(({ name }) => name) }))),
          );
          const readCompanions = (sql: SqlClient.SqlClient) =>
            Effect.forEach(companionColumns, ({ table, columns }) =>
              sql
                .unsafe<Record<string, unknown>>(
                  `SELECT ${byteProjection(columns)} FROM "${table}" ORDER BY rowid`,
                )
                .unprepared.pipe(Effect.map((rows) => ({ table, rows }))),
            );
          const readSchema = (sql: SqlClient.SqlClient) =>
            sql<Record<string, unknown>>`
              SELECT type, name, tbl_name AS "tableName", sql
              FROM sqlite_schema ORDER BY type, name
            `;
          const assertHealthy = Effect.fn("assertMigration059FixtureHealthy")(function* (
            sql: SqlClient.SqlClient,
          ) {
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
            assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
            assert.deepStrictEqual(yield* sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
            assert.deepStrictEqual(
              yield* sql`
                SELECT type, name, tbl_name AS "tableName"
                FROM sqlite_schema
                WHERE name LIKE '%rebuild_059%' OR tbl_name LIKE '%rebuild_059%'
              `,
              [],
            );
          });

          const beforeRows = yield* readDeliveries(database.sqlA);
          const beforeCompanions = yield* readCompanions(database.sqlA);
          const beforeSchema = yield* readSchema(database.sqlA);
          assert.deepStrictEqual(
            beforeRows.map((row) => row.state).toSorted(),
            [...seedStates].toSorted(),
          );
          const expectedCounters = new Map<LegacyDeliveryState, readonly [number, number, number]>([
            ["pending", [0, 0, 0]],
            ["turn-accepted", [1, 0, 0]],
            ["claimed", [2, 1, 1]],
            ["retry-wait", [3, 1, 1]],
            ["delivery-attempted", [3, 1, 1]],
            ["provider-started", [4, 1, 1]],
            ["ambiguous", [4, 1, 1]],
          ]);
          for (const row of beforeRows) {
            const expected = expectedCounters.get(row.state as LegacyDeliveryState)!;
            assert.deepStrictEqual(
              [row.revision, row.claim_generation, row.attempt_count],
              expected,
              String(row.state),
            );
            assert.equal(row.revision_storage, "integer");
            assert.equal(row.claim_generation_storage, "integer");
            assert.equal(row.attempt_count_storage, "integer");
          }
          assert.lengthOf(
            beforeCompanions.find(({ table }) => table.endsWith("session_evidence"))!.rows,
            3,
          );
          assert.lengthOf(
            beforeCompanions.find(({ table }) => table.endsWith("delivery_attestations"))!.rows,
            3,
          );
          for (const suffix of ["evidence", "receipts", "markers"]) {
            assert.lengthOf(
              beforeCompanions.find(({ table }) => table.endsWith(`stage_started_${suffix}`))!.rows,
              1,
            );
          }
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id=${providerSeed.stageRunId}
            `,
            [{ status: "running", revision: 2 }],
          );
          yield* assertHealthy(database.sqlA);

          const path = yield* Path.Path;
          const openClone = Effect.fn("openMigration059FixtureClone")(function* (label: string) {
            const filename = path.join(path.dirname(database.filename), `${label}.sqlite`);
            yield* database.sqlA.unsafe("VACUUM INTO ?", [filename]).withoutTransform;
            const scope = yield* Scope.make("sequential");
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
            const context = yield* Layer.buildWithScope(
              NodeSqliteClient.layer({ filename }),
              scope,
            );
            const sql = Context.get(context, SqlClient.SqlClient);
            yield* sql`PRAGMA foreign_keys = ON`;
            yield* sql`PRAGMA busy_timeout = 5000`;
            return { filename, sql, scope };
          });

          for (const faultPoint of [
            "before-copy",
            "after-copy",
            "after-install",
          ] satisfies ReadonlyArray<Migration059FaultPoint>) {
            const clone = yield* openClone(`migration-059-${faultPoint}`);
            const failed = yield* Effect.exit(
              clone.sql.withTransaction(
                makeMigration059(faultPoint).pipe(
                  Effect.provideService(SqlClient.SqlClient, clone.sql),
                ),
              ),
            );
            assert.isTrue(Exit.isFailure(failed), faultPoint);
            assert.deepStrictEqual(yield* readDeliveries(clone.sql), beforeRows, faultPoint);
            assert.deepStrictEqual(yield* readCompanions(clone.sql), beforeCompanions, faultPoint);
            assert.deepStrictEqual(yield* readSchema(clone.sql), beforeSchema, faultPoint);
            yield* assertHealthy(clone.sql);
            assert.deepStrictEqual(
              yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
                Effect.provideService(SqlClient.SqlClient, clone.sql),
              ),
              [[59, "AgentControlVerificationTurnTerminalObservation"] as const],
              faultPoint,
            );
            yield* assertHealthy(clone.sql);
          }

          const corruptClone = yield* openClone("migration-059-corrupt-storage");
          const triggerRows = yield* corruptClone.sql<{
            readonly name: string;
            readonly sql: string;
          }>`
            SELECT name, sql FROM sqlite_schema
            WHERE type='trigger' AND name IN (
              'agent_control_verification_deliveries_update_storage_validate',
              'agent_control_verification_delivery_transition_validate'
            ) ORDER BY name
          `;
          assert.lengthOf(triggerRows, 2);
          const native = yield* Effect.acquireRelease(
            Effect.sync(() => {
              const connection = new NodeSqlite.DatabaseSync(corruptClone.filename);
              connection.exec(
                "PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON",
              );
              return connection;
            }),
            (connection) => Effect.sync(() => connection.close()),
          );
          yield* Effect.sync(() => {
            native.exec("BEGIN IMMEDIATE; PRAGMA ignore_check_constraints = ON");
            try {
              for (const trigger of triggerRows) {
                native.exec(`DROP TRIGGER "${trigger.name.replaceAll('"', '""')}"`);
              }
              native
                .prepare(
                  "UPDATE agent_control_verification_deliveries SET provider_resume_cursor_json=X'80' WHERE state='delivery-attempted'",
                )
                .run();
              native.exec("PRAGMA ignore_check_constraints = OFF");
              for (const trigger of triggerRows) native.exec(trigger.sql);
              native.exec("COMMIT");
            } catch (cause) {
              native.exec("ROLLBACK");
              throw cause;
            }
          });
          const corruptRows = yield* readDeliveries(corruptClone.sql);
          const corruptSchema = yield* readSchema(corruptClone.sql);
          const corruptMigration = yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 59 }).pipe(
              Effect.provideService(SqlClient.SqlClient, corruptClone.sql),
            ),
          );
          assert.isTrue(Exit.isFailure(corruptMigration));
          assert.deepStrictEqual(yield* readDeliveries(corruptClone.sql), corruptRows);
          assert.deepStrictEqual(yield* readCompanions(corruptClone.sql), beforeCompanions);
          assert.deepStrictEqual(yield* readSchema(corruptClone.sql), corruptSchema);
          assert.deepStrictEqual(yield* corruptClone.sql`PRAGMA foreign_key_check`, []);
          assert.deepStrictEqual(
            yield* corruptClone.sql`
              SELECT type, name, tbl_name AS "tableName"
              FROM sqlite_schema
              WHERE name LIKE '%rebuild_059%' OR tbl_name LIKE '%rebuild_059%'
            `,
            [],
          );

          assert.deepStrictEqual(
            yield* runMigrations({ toMigrationInclusive: 59 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sqlA),
            ),
            [[59, "AgentControlVerificationTurnTerminalObservation"] as const],
          );
          assert.deepStrictEqual(yield* readDeliveries(database.sqlA), beforeRows);
          assert.deepStrictEqual(yield* readCompanions(database.sqlA), beforeCompanions);
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT terminal_event_id AS "terminalEventId",
                terminal_event_type AS "terminalEventType",
                terminal_provider_state AS "terminalProviderState",
                terminal_observation_digest AS "terminalObservationDigest"
              FROM agent_control_verification_deliveries ORDER BY provider_delivery_id
            `,
            seedStates.map(() => ({
              terminalEventId: null,
              terminalEventType: null,
              terminalProviderState: null,
              terminalObservationDigest: null,
            })),
          );
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT restored_table AS "restoredTable"
              FROM migration_059_restore_audit ORDER BY ordinal
            `,
            [
              { restoredTable: "evidence" },
              { restoredTable: "receipt" },
              { restoredTable: "marker" },
            ],
          );
          const recoveryIndexList = yield* database.sqlA<{
            readonly name: string;
            readonly isUnique: number;
            readonly partial: number;
          }>`
            SELECT name, "unique" AS "isUnique", partial
            FROM pragma_index_list('agent_control_verification_deliveries')
            WHERE name='idx_agent_control_verification_delivery_terminal_recovery'
          `;
          assert.deepStrictEqual(recoveryIndexList, [
            {
              name: "idx_agent_control_verification_delivery_terminal_recovery",
              isUnique: 0,
              partial: 0,
            },
          ]);
          const recoveryIndexXinfo = yield* database.sqlA<{
            readonly seqno: number;
            readonly name: string | null;
            readonly descending: number;
            readonly collation: string;
            readonly key: number;
          }>`
            SELECT seqno, name, "desc" AS descending, coll AS collation, key
            FROM pragma_index_xinfo(
              'idx_agent_control_verification_delivery_terminal_recovery'
            ) ORDER BY seqno
          `;
          assert.deepStrictEqual(
            recoveryIndexXinfo.filter((column) => column.key === 1),
            [
              { seqno: 0, name: "state", descending: 0, collation: "BINARY", key: 1 },
              { seqno: 1, name: "handoff_id", descending: 0, collation: "BINARY", key: 1 },
            ],
          );
          assert.lengthOf(
            recoveryIndexXinfo.filter((column) => column.key === 1),
            2,
          );
          assert.deepStrictEqual(
            yield* database.sqlA`
              SELECT tbl_name AS "tableName", sql
              FROM sqlite_master
              WHERE type='index'
                AND name='idx_agent_control_verification_delivery_terminal_recovery'
            `,
            [
              {
                tableName: "agent_control_verification_deliveries",
                sql: "CREATE INDEX idx_agent_control_verification_delivery_terminal_recovery\n    ON agent_control_verification_deliveries(state, handoff_id)\n  ",
              },
            ],
          );
          yield* assertHealthy(database.sqlA);

          const migratedStoreContext = yield* Layer.buildWithScope(
            Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, database.sqlA)),
              Layer.provideMerge(NodeServices.layer),
            ),
            database.scopeA,
          );
          const migratedStore = Context.get(
            migratedStoreContext,
            AgentControlVerificationHandoffStore,
          );
          const migratedStageStarter = yield* buildVerificationStageStarter({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: { ...providerSeed.prepared.coordinator, handoffStore: migratedStore },
            planningFinalizer,
          });
          assert.equal(
            (yield* migratedStageStarter.processHandoff(providerSeed.handoffId))._tag,
            "Replayed",
          );
          const started = Option.getOrThrow(
            yield* migratedStore.loadAcceptedByHandoffId(providerSeed.handoffId),
          );
          const executorCalls = yield* Ref.make(0);
          const consumer = yield* buildVerificationTurnConsumer({
            sql: database.sqlA,
            scope: database.scopeA,
            coordinator: { ...providerSeed.prepared.coordinator, handoffStore: migratedStore },
            executorCalls,
          });
          const migratedTerminalAt = shiftIso(started.delivery.providerAcceptedAt!, -1);
          const terminalEvent = {
            type: "turn.completed",
            eventId: EventId.make("verification-migration-059-terminal"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: started.evidence.providerInstanceId,
            threadId: started.evidence.threadId,
            turnId: TurnId.make(started.delivery.providerTurnId!),
            createdAt: migratedTerminalAt,
            payload: { state: "completed" },
          } satisfies ProviderRuntimeEvent;
          yield* consumer.processRuntimeEvent(terminalEvent);
          const terminal = Option.getOrThrow(
            yield* migratedStore.loadAcceptedByHandoffId(providerSeed.handoffId),
          );
          assert.equal(terminal.delivery.state, "completed");
          assert.equal(terminal.delivery.terminalAt, migratedTerminalAt);
          assert.equal(terminal.delivery.revision, started.delivery.revision + 1);
          const [changesBeforeReplay] = yield* database.sqlA<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          yield* consumer.processRuntimeEvent(terminalEvent);
          const [changesAfterReplay] = yield* database.sqlA<{
            readonly changes: number;
          }>`SELECT total_changes() AS changes`;
          assert.equal(changesAfterReplay!.changes, changesBeforeReplay!.changes);
          assert.equal(
            Option.getOrThrow(yield* migratedStore.loadAcceptedByHandoffId(providerSeed.handoffId))
              .delivery.revision,
            terminal.delivery.revision,
          );
        }),
      ),
    ),
);

it.effect.each<{ readonly stageFailure: "persistence" | "revision-conflict" }>([
  { stageFailure: "persistence" },
  { stageFailure: "revision-conflict" },
])(
  "preserves a typed Stage-Starter $stageFailure cause through provider-prefix acknowledgement and attempt close",
  ({ stageFailure }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            `verification-stage-prefix-${stageFailure}`,
          );
          const executorCalls = yield* Ref.make(0);
          const responseLossDefect = {
            _tag: "VerificationStagePrefixResponseLoss",
            stageFailure,
          } as const;
          const lossy = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            responseLossDefect,
          });
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(lossy.processHandoff(prepared.handoffId))),
          );
          const ambiguous = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(ambiguous.delivery.state, "ambiguous");

          const resourcesScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() =>
            Effect.exit(Scope.close(resourcesScope, Exit.void)).pipe(Effect.asVoid),
          );
          const resourcesFinalized = yield* Ref.make(false);
          const cleanupDefect = {
            _tag: "VerificationStagePrefixCleanupDefect",
            stageFailure,
          } as const;
          yield* Scope.addFinalizer(resourcesScope, Effect.die(cleanupDefect));
          yield* Scope.addFinalizer(resourcesScope, Ref.set(resourcesFinalized, true));
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);
          const providerPublications = yield* PubSub.unbounded<ProviderRuntimeEventPublication>();
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: resourcesScope,
            coordinator: prepared.coordinator,
            executorCalls,
            providerPublications,
          });
          const providerSubscription = yield* consumer.subscribeProviderEvents.pipe(
            Scope.provide(resourcesScope),
          );
          const consumerActivation = yield* consumer
            .prepare(providerSubscription, attempt.activation.await)
            .pipe(Scope.provide(resourcesScope));
          const appendEntered = yield* Deferred.make<void>();
          const releaseAppend = yield* Deferred.make<void>();
          const stageStoreFailure =
            stageFailure === "persistence"
              ? new AgentControlPersistenceSqlError({
                  operation: "test.verification-stage-prefix.append",
                })
              : new AgentControlStageRunStreamVersionConflictError({
                  stageRunId: AgentControlStageRunId.make(ambiguous.evidence.stageRunId),
                  expectedVersion: 1,
                  actualVersion: 2,
                });
          const backingStageEvents = prepared.planningFinalizer.stageEvents;
          const failingStageEvents = AgentControlStageRunEventStore.of({
            ...backingStageEvents,
            append: () =>
              Deferred.succeed(appendEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseAppend)),
                Effect.andThen(Effect.fail(stageStoreFailure)),
              ),
          });
          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlB,
            scope: resourcesScope,
            coordinator: prepared.coordinator,
            planningFinalizer: prepared.planningFinalizer,
            stageEvents: failingStageEvents,
          });
          yield* starter.prepare(attempt.activation.await).pipe(Scope.provide(resourcesScope));
          yield* attempt.commit(attempt.activation.open);

          const token = {
            id: stageFailure === "persistence" ? 44 : 45,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
          const shutdownDrainRuns = yield* Ref.make(0);
          yield* attempt.activation.registerShutdownDrain(
            Ref.update(shutdownDrainRuns, (count) => count + 1).pipe(
              Effect.andThen(consumerActivation.drainProviderEvents(token)),
            ),
          );
          yield* PubSub.publish(providerPublications, {
            _tag: "Event",
            event: {
              type: "turn.started",
              eventId: EventId.make(`verification-stage-prefix-started-${stageFailure}`),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: ambiguous.evidence.providerInstanceId,
              threadId: ambiguous.evidence.threadId,
              createdAt: providerAcceptedAt,
              turnId: TurnId.make(`verification-stage-prefix-turn-${stageFailure}`),
              payload: {},
            },
          });
          yield* PubSub.publish(providerPublications, { _tag: "Drain", token });
          yield* Deferred.await(appendEntered);

          const stageDrainWaiters = yield* Effect.forEach(
            [starter.drain, starter.drain, starter.drain],
            (drain) => drain.pipe(Effect.forkChild({ startImmediately: true })),
          );
          const acknowledgementWaiter = yield* Deferred.await(
            token.verificationAcknowledgement,
          ).pipe(Effect.forkChild({ startImmediately: true }));
          yield* Effect.yieldNow;
          assert.isFalse(yield* Deferred.isDone(token.verificationAcknowledgement));
          for (const waiter of stageDrainWaiters) assert.isUndefined(waiter.pollUnsafe());

          yield* Deferred.succeed(releaseAppend, undefined);
          const stageDrainExits = yield* Effect.forEach(stageDrainWaiters, Fiber.await);
          const acknowledgementExit = yield* Fiber.await(acknowledgementWaiter);
          const laterStageDrainExit = yield* Effect.exit(starter.drain);
          const observedExits = [...stageDrainExits, acknowledgementExit, laterStageDrainExit];
          const isStageStarterError = Schema.is(AgentControlVerificationStageStarterError);
          let originalStageError: AgentControlVerificationStageStarterError | undefined;
          let originalStageCause: Cause.Cause<Error> | undefined;
          for (const observedExit of observedExits) {
            assert.isTrue(Exit.isFailure(observedExit));
            if (Exit.isFailure(observedExit)) {
              if (originalStageCause === undefined) originalStageCause = observedExit.cause;
              else assert.deepStrictEqual(observedExit.cause, originalStageCause);
              const failReason = observedExit.cause.reasons.find(Cause.isFailReason);
              assert.isDefined(failReason);
              if (
                failReason !== undefined &&
                Cause.isFailReason(failReason) &&
                isStageStarterError(failReason.error)
              ) {
                const stageError = failReason.error;
                assert.equal(stageError.reason, stageFailure);
                assert.equal(stageError.operation, "append-stage-started");
                if (originalStageError === undefined) originalStageError = stageError;
                else assert.strictEqual(stageError, originalStageError);
              }
              assert.isTrue(observedExit.cause.reasons.some(Cause.isFailReason));
              assert.isFalse(observedExit.cause.reasons.some(Cause.isDieReason));
            }
          }
          assert.isDefined(originalStageError);
          const repeatedAcknowledgementExit = yield* Effect.exit(
            consumerActivation.drainProviderEvents(token),
          );
          assert.isTrue(Exit.isFailure(repeatedAcknowledgementExit));
          if (Exit.isFailure(repeatedAcknowledgementExit)) {
            assert.isTrue(
              repeatedAcknowledgementExit.cause.reasons.some(
                (reason) => Cause.isFailReason(reason) && reason.error === originalStageError,
              ),
            );
          }

          const firstCloseExit = yield* Effect.exit(
            attempt.close(
              Exit.interrupt(`verification-stage-prefix-close-${stageFailure}` as never),
            ),
          );
          const laterCloseExits = yield* Effect.forEach(
            [0, 1],
            () => Effect.exit(attempt.close(Exit.void)),
            { concurrency: "unbounded" },
          );
          let originalCloseCause: Cause.Cause<never> | undefined;
          for (const closeExit of [firstCloseExit, ...laterCloseExits]) {
            assert.isTrue(Exit.isFailure(closeExit));
            if (Exit.isFailure(closeExit)) {
              if (originalCloseCause === undefined) originalCloseCause = closeExit.cause;
              else assert.strictEqual(closeExit.cause, originalCloseCause);
              const failReasons = closeExit.cause.reasons.filter(Cause.isFailReason);
              assert.isTrue(failReasons.some((reason) => reason.error === originalStageError));
              assert.isTrue(
                closeExit.cause.reasons.some(
                  (reason) => Cause.isDieReason(reason) && reason.defect === cleanupDefect,
                ),
              );
            }
          }
          assert.equal(yield* Ref.get(shutdownDrainRuns), 1);
          assert.isTrue(yield* Ref.get(resourcesFinalized));
          const postCloseStageDrainExit = yield* Effect.exit(starter.drain);
          assert.isTrue(Exit.isFailure(postCloseStageDrainExit));
          if (Exit.isFailure(postCloseStageDrainExit)) {
            assert.deepStrictEqual(postCloseStageDrainExit.cause, originalStageCause);
            assert.isTrue(
              postCloseStageDrainExit.cause.reasons.some(
                (reason) => Cause.isFailReason(reason) && reason.error === originalStageError,
              ),
            );
            assert.isFalse(postCloseStageDrainExit.cause.reasons.some(Cause.isDieReason));
          }

          const adopted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(adopted.delivery.state, "provider-started");
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
              SELECT
                (SELECT status FROM agent_control_stage_run_states
                 WHERE stage_run_id = ${adopted.evidence.stageRunId}) AS status,
                (SELECT revision FROM agent_control_stage_run_states
                 WHERE stage_run_id = ${adopted.evidence.stageRunId}) AS revision,
                (SELECT count(*) FROM agent_control_verification_stage_started_evidence)
                  AS evidence,
                (SELECT count(*) FROM agent_control_verification_stage_started_receipts)
                  AS receipts,
                (SELECT count(*) FROM agent_control_verification_stage_started_markers)
                  AS markers
            `,
            [{ status: "prepared", revision: 1, evidence: 0, receipts: 0, markers: 0 }],
          );
          assert.equal(yield* Ref.get(executorCalls), 1);
        }),
      ),
    ),
);

it.effect("fails provider-prefix acknowledgements after typed Verification adoption failure", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const prepared = yield* prepareVerificationTurnDelivery(
          "verification-provider-prefix-typed-failure",
        );
        const executorCalls = yield* Ref.make(0);
        const responseLossDefect = { _tag: "VerificationTypedPrefixResponseLoss" } as const;
        const lossy = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: prepared.database.scopeA,
          coordinator: prepared.coordinator,
          executorCalls,
          responseLossDefect,
        });
        assert.isTrue(Exit.isFailure(yield* Effect.exit(lossy.processHandoff(prepared.handoffId))));
        const ambiguous = Option.getOrThrow(
          yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(ambiguous.delivery.state, "ambiguous");

        const attemptScope = yield* Scope.make("sequential");
        const finalized = yield* Ref.make(false);
        yield* Scope.addFinalizer(attemptScope, Ref.set(finalized, true));
        yield* Effect.addFinalizer(() => Scope.close(attemptScope, Exit.void));
        const adoptionFailure = new AgentControlVerificationStoreError({
          operation: "test-provider-prefix-observe",
          reason: "persistence",
        });
        const backingStore = prepared.coordinator.handoffStore;
        const instrumentedStore = AgentControlVerificationHandoffStore.of({
          ...backingStore,
          observeProviderStarted: () => Effect.fail(adoptionFailure),
        });
        const coordinator = {
          ...prepared.coordinator,
          handoffStore: instrumentedStore,
        } satisfies VerificationTurnCoordinatorHarness;
        const providerPublications = yield* PubSub.unbounded<ProviderRuntimeEventPublication>();
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlB,
          scope: attemptScope,
          coordinator,
          executorCalls,
          providerPublications,
        });
        const providerSubscription = yield* consumer.subscribeProviderEvents.pipe(
          Scope.provide(attemptScope),
        );
        const consumerActivation = yield* consumer
          .prepare(providerSubscription, Effect.void)
          .pipe(Scope.provide(attemptScope));
        const starter = yield* buildVerificationStageStarter({
          sql: prepared.database.sqlB,
          scope: attemptScope,
          coordinator,
          planningFinalizer: prepared.planningFinalizer,
        });
        yield* starter.prepare(Effect.void).pipe(Scope.provide(attemptScope));

        const makeToken = Effect.fn("makeVerificationFailureDrainToken")(function* (id: number) {
          return {
            id,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
        });
        const firstToken = yield* makeToken(42);
        yield* PubSub.publish(providerPublications, {
          _tag: "Event",
          event: {
            type: "turn.started",
            eventId: EventId.make("verification-provider-prefix-typed-started"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: ambiguous.evidence.providerInstanceId,
            threadId: ambiguous.evidence.threadId,
            createdAt: providerAcceptedAt,
            turnId: TurnId.make("verification-provider-prefix-typed-turn"),
            payload: {},
          },
        });
        yield* PubSub.publish(providerPublications, { _tag: "Drain", token: firstToken });
        const firstExit = yield* Effect.exit(consumerActivation.drainProviderEvents(firstToken));
        assert.isTrue(Exit.isFailure(firstExit));
        if (Exit.isFailure(firstExit)) {
          assert.isTrue(
            firstExit.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === adoptionFailure,
            ),
          );
        }

        const secondToken = yield* makeToken(43);
        yield* PubSub.publish(providerPublications, { _tag: "Drain", token: secondToken });
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(consumerActivation.drainProviderEvents(secondToken))),
        );
        const stillAmbiguous = Option.getOrThrow(
          yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId),
        );
        assert.equal(stillAmbiguous.delivery.state, "ambiguous");
        assert.equal(stillAmbiguous.delivery.providerTurnId, null);
        assert.equal(stillAmbiguous.delivery.providerAcceptedAt, null);
        assert.deepStrictEqual(
          yield* prepared.database.sqlB`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id = ${stillAmbiguous.evidence.stageRunId}
            `,
          [{ status: "prepared", revision: 1 }],
        );

        yield* Scope.close(attemptScope, firstExit);
        assert.isTrue(yield* Ref.get(finalized));
      }),
    ),
  ),
);

it.effect(
  "persists ambiguity before propagating a combined Verification interrupt and defect",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery("verification-mixed-cause");
          const executorCalls = yield* Ref.make(0);
          const defect = { _tag: "VerificationMixedProviderDefect" } as const;
          const originalCause = Cause.combine(Cause.interrupt(), Cause.die(defect));
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
            responseLossCause: originalCause,
          });

          const exit = yield* Effect.exit(consumer.processHandoff(prepared.handoffId));
          assert.isTrue(Exit.isFailure(exit));
          if (Exit.isFailure(exit)) {
            assert.isTrue(Cause.hasInterrupts(exit.cause));
            assert.isTrue(
              exit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === defect,
              ),
            );
          }
          const persisted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(persisted.delivery.state, "ambiguous");
          assert.equal(persisted.delivery.nextAttemptAt, null);
          assert.equal(yield* Ref.get(executorCalls), 1);
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect(
  "fails provider-prefix acknowledgement after typed Verification terminal CAS failure",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-terminal-prefix-failure",
          );
          const executorCalls = yield* Ref.make(0);
          const deliveryConsumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: prepared.database.scopeA,
            coordinator: prepared.coordinator,
            executorCalls,
          });
          yield* deliveryConsumer.processHandoff(prepared.handoffId);
          const started = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          const terminalFailure = new AgentControlVerificationStoreError({
            operation: "test-provider-prefix-terminal",
            reason: "persistence",
          });
          const instrumentedStore = AgentControlVerificationHandoffStore.of({
            ...prepared.coordinator.handoffStore,
            observeProviderTerminal: () => Effect.fail(terminalFailure),
          });
          const coordinator = {
            ...prepared.coordinator,
            handoffStore: instrumentedStore,
          } satisfies VerificationTurnCoordinatorHarness;
          const attemptScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(attemptScope, Exit.void));
          const providerPublications = yield* PubSub.unbounded<ProviderRuntimeEventPublication>();
          const consumer = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: attemptScope,
            coordinator,
            executorCalls,
            providerPublications,
          });
          const subscription = yield* consumer.subscribeProviderEvents.pipe(
            Scope.provide(attemptScope),
          );
          const activation = yield* consumer
            .prepare(subscription, Effect.void)
            .pipe(Scope.provide(attemptScope));
          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlB,
            scope: attemptScope,
            coordinator,
            planningFinalizer: prepared.planningFinalizer,
          });
          yield* starter.prepare(Effect.void).pipe(Scope.provide(attemptScope));
          const token = {
            id: 46,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
          yield* PubSub.publish(providerPublications, {
            _tag: "Event",
            event: {
              type: "turn.completed",
              eventId: EventId.make("verification-terminal-prefix-event"),
              provider: ProviderDriverKind.make("codex"),
              providerInstanceId: started.evidence.providerInstanceId,
              threadId: started.evidence.threadId,
              turnId: TurnId.make(started.delivery.providerTurnId!),
              createdAt: "2027-01-01T00:00:00.000Z",
              payload: { state: "completed" },
            },
          });
          yield* PubSub.publish(providerPublications, { _tag: "Drain", token });
          const drainExit = yield* Effect.exit(activation.drainProviderEvents(token));
          assert.isTrue(Exit.isFailure(drainExit));
          if (Exit.isFailure(drainExit)) {
            assert.isTrue(
              drainExit.cause.reasons.some(
                (reason) => Cause.isFailReason(reason) && reason.error === terminalFailure,
              ),
            );
          }
          const unchanged = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(unchanged.delivery.state, "provider-started");
          assert.equal(unchanged.delivery.terminalEventId, null);
        }),
      ),
    ),
);

it.effect("combines a mixed post-CAS Verification cause with a failing durable reload", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery("verification-mixed-load-fail");
        const executorCalls = yield* Ref.make(0);
        const defect = { _tag: "VerificationMixedLoadProviderDefect" } as const;
        const loadFailure = new AgentControlVerificationStoreError({
          operation: "test-post-cas-load",
          reason: "persistence",
        });
        const loadCalls = yield* Ref.make(0);
        const backingStore = prepared.coordinator.handoffStore;
        const instrumentedStore = AgentControlVerificationHandoffStore.of({
          ...backingStore,
          loadAcceptedByHandoffId: (handoffId) =>
            Ref.getAndUpdate(loadCalls, (count) => count + 1).pipe(
              Effect.flatMap((call) =>
                call === 0
                  ? backingStore.loadAcceptedByHandoffId(handoffId)
                  : Effect.fail(loadFailure),
              ),
            ),
        });
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: prepared.database.scopeA,
          coordinator: { ...prepared.coordinator, handoffStore: instrumentedStore },
          executorCalls,
          responseLossCause: Cause.combine(Cause.interrupt(), Cause.die(defect)),
        });

        const exit = yield* Effect.exit(consumer.processHandoff(prepared.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.isTrue(Cause.hasInterrupts(exit.cause));
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === defect,
            ),
          );
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === loadFailure,
            ),
          );
        }
        assert.equal(
          Option.getOrThrow(yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId))
            .delivery.state,
          "delivery-attempted",
        );
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect("combines a provider defect with a failing durable Verification ambiguity write", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.parse(createdAt));
        const prepared = yield* prepareVerificationTurnDelivery(
          "verification-ambiguity-persistence-fail",
        );
        const executorCalls = yield* Ref.make(0);
        const providerDefect = { _tag: "VerificationProviderDefectBeforeAmbiguity" } as const;
        const persistenceFailure = new AgentControlVerificationStoreError({
          operation: "test-mark-ambiguous",
          reason: "persistence",
        });
        const backingStore = prepared.coordinator.handoffStore;
        const instrumentedStore = AgentControlVerificationHandoffStore.of({
          ...backingStore,
          markAmbiguous: () => Effect.fail(persistenceFailure),
        });
        const consumer = yield* buildVerificationTurnConsumer({
          sql: prepared.database.sqlA,
          scope: prepared.database.scopeA,
          coordinator: { ...prepared.coordinator, handoffStore: instrumentedStore },
          executorCalls,
          responseLossDefect: providerDefect,
        });

        const exit = yield* Effect.exit(consumer.processHandoff(prepared.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === providerDefect,
            ),
          );
          assert.isTrue(
            exit.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === persistenceFailure,
            ),
          );
        }
        assert.equal(
          Option.getOrThrow(yield* backingStore.loadAcceptedByHandoffId(prepared.handoffId))
            .delivery.state,
          "delivery-attempted",
        );
        assert.equal(yield* Ref.get(executorCalls), 1);
      }),
    ),
  ).pipe(Effect.provide(TestClock.layer())),
);

it.effect(
  "recovers a buffered Verification provider start across restart and starts the StageRun",
  () =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.parse(createdAt));
          const prepared = yield* prepareVerificationTurnDelivery(
            "verification-startup-buffered-provider-start",
          );
          const crashedScope = yield* Scope.make("sequential");
          const executorCalls = yield* Ref.make(0);
          const crashed = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlA,
            scope: crashedScope,
            coordinator: prepared.coordinator,
            executorCalls,
            hooks: {
              ...noopVerificationConsumerHooks,
              afterDeliveryCas: () => Effect.interrupt,
            },
          });
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(crashed.processHandoff(prepared.handoffId))),
          );
          yield* Scope.close(crashedScope, Exit.void);
          const attempted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(attempted.delivery.state, "delivery-attempted");

          const restartedScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void));
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const restarted = yield* buildVerificationTurnConsumer({
            sql: prepared.database.sqlB,
            scope: restartedScope,
            coordinator: prepared.coordinator,
            executorCalls,
            providerEvents,
          });
          const subscription = yield* restarted.subscribeProviderEvents.pipe(
            Scope.provide(restartedScope),
          );
          yield* PubSub.publish(providerEvents, {
            type: "turn.started",
            eventId: EventId.make("verification-startup-buffered-runtime-start"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: attempted.evidence.providerInstanceId,
            threadId: attempted.evidence.threadId,
            createdAt: providerAcceptedAt,
            turnId: TurnId.make("verification-startup-buffered-provider-turn"),
            payload: {},
          });
          yield* restarted.start(subscription).pipe(Scope.provide(restartedScope));
          yield* restarted.drain.pipe(Effect.timeout(barrierTimeout));

          const providerStarted = Option.getOrThrow(
            yield* prepared.coordinator.handoffStore.loadAcceptedByHandoffId(prepared.handoffId),
          );
          assert.equal(providerStarted.delivery.state, "provider-started");
          assert.equal(
            providerStarted.delivery.providerTurnId,
            "verification-startup-buffered-provider-turn",
          );
          assert.equal(yield* Ref.get(executorCalls), 0);

          const starter = yield* buildVerificationStageStarter({
            sql: prepared.database.sqlB,
            scope: restartedScope,
            coordinator: prepared.coordinator,
            planningFinalizer: prepared.planningFinalizer,
          });
          yield* starter.start().pipe(Scope.provide(restartedScope));
          yield* starter.drain.pipe(Effect.timeout(barrierTimeout));
          assert.deepStrictEqual(
            yield* prepared.database.sqlB`
              SELECT status, revision FROM agent_control_stage_run_states
              WHERE stage_run_id = ${providerStarted.evidence.stageRunId}
            `,
            [{ status: "running", revision: 2 }],
          );
        }),
      ),
    ).pipe(Effect.provide(TestClock.layer())),
);

it.effect.each<{ readonly phase: "before-marker" | "after-commit" }>([
  { phase: "before-marker" },
  { phase: "after-commit" },
])("recovers Verification stage start after a $phase crash", ({ phase }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = `verification-stage-start-${phase}`;
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const prepared = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          suffix,
        );
        const verificationAdmission = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: prepared.setup.finalizer.finalizer,
          handoffStore: prepared.setup.coordinator.handoffStore,
          admissionHarness: prepared.setup.candidate.admissionHarness,
        });
        assert.equal(
          (yield* verificationAdmission.admission.processResultEvidence(
            prepared.implementation.resultEvidenceId,
          ))._tag,
          "Admitted",
        );
        const coordinator = yield* buildVerificationTurnCoordinator({
          sql: database.sqlA,
          scope: database.scopeA,
          admission: verificationAdmission.admission,
          planningFinalizer,
          admissionHarness: prepared.setup.candidate.admissionHarness,
          task: prepared.setup.candidate.task,
          worktree: prepared.setup.candidate.worktree,
          orchestration: prepared.setup.coordinator.orchestration,
          snapshots: prepared.setup.coordinator.snapshots,
        });
        assert.equal(
          (yield* coordinator.coordinator.processHandoff(prepared.implementation.resultEvidenceId))
            ._tag,
          "Materialized",
        );
        const [handoff] = yield* database.sqlA<{ readonly handoffId: string }>`
          SELECT handoff_id AS "handoffId" FROM agent_control_verification_handoff_accepted
        `;
        assert.isDefined(handoff);
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildVerificationTurnConsumer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator,
          executorCalls,
        });
        yield* consumer.processHandoff(handoff!.handoffId);
        assert.equal(yield* Ref.get(executorCalls), 1);

        yield* Ref.set(planningFinalizer.stagePublished, []);
        const publications = yield* Ref.make(0);
        const crashing = yield* buildVerificationStageStarter({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator,
          planningFinalizer,
          hooks: {
            ...noopVerificationStageStarterHooks,
            beforeFinalMarker: () => (phase === "before-marker" ? Effect.interrupt : Effect.void),
            afterOuterCommit: () =>
              phase === "after-commit"
                ? Effect.die(new Error("verification-stage-start-response-loss"))
                : Effect.void,
            afterPublication: () => Ref.update(publications, (count) => count + 1),
          },
        });
        const exit = yield* Effect.exit(crashing.processHandoff(handoff!.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        assert.equal(yield* Ref.get(publications), 0);
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
        assert.deepStrictEqual(
          yield* database.sqlA`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE event_type = 'agentControl.stageRun.verificationStarted') AS started,
              (SELECT count(*) FROM agent_control_verification_stage_started_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_stage_started_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_stage_started_markers)
                AS markers,
              (SELECT status FROM agent_control_stage_run_states
               WHERE stage_run_id = (
                 SELECT stage_run_id FROM agent_control_verification_deliveries LIMIT 1
               )) AS stageStatus
          `,
          [
            {
              started: phase === "before-marker" ? 0 : 1,
              evidence: phase === "before-marker" ? 0 : 1,
              receipts: phase === "before-marker" ? 0 : 1,
              markers: phase === "before-marker" ? 0 : 1,
              stageStatus: phase === "before-marker" ? "prepared" : "running",
            },
          ],
        );

        const handoffStoreContextB = yield* Layer.buildWithScope(
          Layer.fresh(AgentControlVerificationHandoffStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, database.sqlB)),
            Layer.provideMerge(NodeServices.layer),
          ),
          database.scopeB,
        );
        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const coordinatorB = {
          ...coordinator,
          handoffStore: Context.get(handoffStoreContextB, AgentControlVerificationHandoffStore),
        } satisfies VerificationTurnCoordinatorHarness;
        const recovered = yield* buildVerificationStageStarter({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          planningFinalizer: planningFinalizerB,
        });
        assert.equal(
          (yield* recovered.processHandoff(handoff!.handoffId))._tag,
          phase === "before-marker" ? "Started" : "Replayed",
        );
        const finalClaim = Option.getOrThrow(
          yield* coordinatorB.handoffStore.loadAcceptedByHandoffId(handoff!.handoffId),
        );
        assert.deepStrictEqual(
          yield* database.sqlB`
            SELECT
              (SELECT count(*) FROM agent_control_events
               WHERE stream_id = ${finalClaim.evidence.stageRunId}
                 AND event_type = 'agentControl.stageRun.verificationStarted') AS started,
              (SELECT count(*) FROM agent_control_verification_stage_started_evidence)
                AS evidence,
              (SELECT count(*) FROM agent_control_verification_stage_started_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_verification_stage_started_markers)
                AS markers,
              (SELECT status FROM agent_control_stage_run_states
               WHERE stage_run_id = ${finalClaim.evidence.stageRunId}) AS stageStatus,
              (SELECT revision FROM agent_control_stage_run_states
               WHERE stage_run_id = ${finalClaim.evidence.stageRunId}) AS stageRevision,
              (SELECT status FROM agent_control_stage_run_lease_states
               WHERE lease_id = ${finalClaim.evidence.leaseId}) AS leaseStatus,
              (SELECT holder_id FROM agent_control_stage_run_lease_states
               WHERE lease_id = ${finalClaim.evidence.leaseId}) AS leaseHolderId,
              (SELECT fence_token FROM agent_control_stage_run_lease_states
               WHERE lease_id = ${finalClaim.evidence.leaseId}) AS fenceToken
          `,
          [
            {
              started: 1,
              evidence: 1,
              receipts: 1,
              markers: 1,
              stageStatus: "running",
              stageRevision: 2,
              leaseStatus: "reserved",
              leaseHolderId: finalClaim.evidence.leaseHolderId,
              fenceToken: finalClaim.evidence.fenceToken,
            },
          ],
        );
      }),
    ),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase before commit rolls back the complete terminal boundary", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        beforeTransactionComplete: () =>
          phase === "defect" ? Effect.die(new Error("before-commit")) : Effect.interrupt,
      });
      const seeded = yield* seedPlanning(database.sqlA, harness, `before-${phase}`);
      yield* appendProviderStart(database.sqlA, seeded, `before-${phase}`);
      yield* appendPlan(database.sqlA, seeded, `before-${phase}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `before-${phase}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect("replays accepted verification after a legitimate task history suffix", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const { setup, claim, implementation } = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          "verification-admission-replay-task-suffix",
        );
        const publicationCount = yield* Ref.make(0);
        const hookCount = yield* Ref.make(0);
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: setup.candidate.admissionHarness,
          hooks: {
            ...noopVerificationAdmissionHooks,
            afterAuthoritativeRead: () => Ref.update(hookCount, (count) => count + 1),
            beforeWrites: () => Ref.update(hookCount, (count) => count + 1),
            beforeFinalMarker: () => Ref.update(hookCount, (count) => count + 1),
            afterNativeCommit: () => Ref.update(hookCount, (count) => count + 1),
            afterPublication: () =>
              Effect.all([
                Ref.update(hookCount, (count) => count + 1),
                Ref.update(publicationCount, (count) => count + 1),
              ]).pipe(Effect.asVoid),
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);
        yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);
        assert.equal(
          (yield* verification.admission.processResultEvidence(implementation.resultEvidenceId))
            ._tag,
          "Admitted",
        );
        yield* appendLegitimateTaskHistorySuffix(
          verification,
          AgentControlTaskId.make(claim.evidence.taskId),
          "verification-admission-replay-task-suffix",
        );
        const totalChangesBeforeReplay = (yield* database.sqlA<{
          readonly count: number;
        }>`SELECT total_changes() AS count`)[0]!.count;

        assert.equal(
          (yield* verification.admission.processResultEvidence(implementation.resultEvidenceId))
            ._tag,
          "Replayed",
        );
        assert.equal(
          (yield* database.sqlA<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
            .count,
          totalChangesBeforeReplay,
        );
        assert.deepStrictEqual(yield* verificationAdmissionCounts(database.sqlB), {
          stageEvents: 1,
          leaseEvents: 1,
          reservationEvents: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.equal(yield* Ref.get(hookCount), 5);
        assert.equal(yield* Ref.get(publicationCount), 1);
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
        assert.equal(
          (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
          1,
        );
      }),
    ),
  ),
);

it.effect("rejects replay when a bound immutable verification event is missing", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const { setup, implementation } = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          "verification-admission-replay-missing-companion",
        );
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: setup.candidate.admissionHarness,
        });
        assert.equal(
          (yield* verification.admission.processResultEvidence(implementation.resultEvidenceId))
            ._tag,
          "Admitted",
        );
        const [bound] = yield* database.sqlA<{ readonly eventId: string }>`
          SELECT verification_stage_event_id AS "eventId"
          FROM agent_control_verification_admission_evidence
          WHERE implementation_result_evidence_id = ${implementation.resultEvidenceId}
        `;
        assert.isDefined(bound);
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            // External-corruption probe: bypass this connection's foreign-key
            // enforcement only; production schema and triggers stay intact.
            native.exec("PRAGMA foreign_keys = OFF");
            native
              .prepare("DELETE FROM agent_control_events WHERE event_id = ?")
              .run(bound!.eventId);
          } finally {
            native.close();
          }
        });
        const totalChangesBeforeReplay = (yield* database.sqlA<{
          readonly count: number;
        }>`SELECT total_changes() AS count`)[0]!.count;
        const replay = yield* Effect.exit(
          verification.admission.processResultEvidence(implementation.resultEvidenceId),
        );
        assert.isTrue(Exit.isFailure(replay));
        if (Exit.isFailure(replay)) {
          const found = Cause.findErrorOption(replay.cause);
          assert.isTrue(Option.isSome(found));
          if (Option.isSome(found)) {
            assert.isTrue(isVerificationAdmissionError(found.value));
          }
        }
        assert.equal(
          (yield* database.sqlA<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
            .count,
          totalChangesBeforeReplay,
        );
      }),
    ),
  ),
);

it.effect("durably admits verification once and replays without DML, hooks, or publication", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const suffix = "verification-admission-fresh";
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const initialTask = admissionTask(suffix);
        const canonicalTask = {
          ...initialTask,
          taskId: yield* deriveAgentControlTaskId(initialTask.source),
        };
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizer,
          suffix,
          true,
          canonicalTask,
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`provider:${suffix}:implementation-terminal`),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });
        assert.equal(
          (yield* setup.finalizer.finalizer.processHandoff(setup.handoffId))._tag,
          "Finalized",
        );
        const [implementation] = yield* database.sqlA<{
          readonly resultEvidenceId: string;
          readonly leaseId: string;
          readonly holderId: string;
          readonly fenceToken: number;
        }>`
          SELECT result_evidence_id AS "resultEvidenceId", lease_id AS "leaseId",
            lease_holder_id AS "holderId", fence_token AS "fenceToken"
          FROM agent_control_implementation_result_evidence
          WHERE handoff_id = ${setup.handoffId}
        `;
        assert.isDefined(implementation);
        type VerificationHookCounts = {
          readonly afterAuthoritativeRead: number;
          readonly beforeWrites: number;
          readonly beforeFinalMarker: number;
          readonly afterNativeCommit: number;
          readonly afterPublication: number;
        };
        const hookCounts = yield* Ref.make<VerificationHookCounts>({
          afterAuthoritativeRead: 0,
          beforeWrites: 0,
          beforeFinalMarker: 0,
          afterNativeCommit: 0,
          afterPublication: 0,
        });
        const increment = (key: keyof VerificationHookCounts) =>
          Ref.update(hookCounts, (counts) => ({ ...counts, [key]: counts[key] + 1 }));
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: setup.candidate.admissionHarness,
          hooks: {
            ...noopVerificationAdmissionHooks,
            afterAuthoritativeRead: () => increment("afterAuthoritativeRead"),
            beforeWrites: () => increment("beforeWrites"),
            beforeFinalMarker: () => increment("beforeFinalMarker"),
            afterNativeCommit: () => increment("afterNativeCommit"),
            afterPublication: () => increment("afterPublication"),
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);
        yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);
        const fresh = yield* verification.admission.processResultEvidence(
          implementation!.resultEvidenceId,
        );
        assert.equal(fresh._tag, "Admitted");
        assert.deepStrictEqual(yield* Ref.get(hookCounts), {
          afterAuthoritativeRead: 1,
          beforeWrites: 1,
          beforeFinalMarker: 1,
          afterNativeCommit: 1,
          afterPublication: 1,
        });
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
        assert.equal(
          (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
          1,
        );
        const [counts] = yield* database.sqlB<{
          readonly stageEvents: number;
          readonly stageStates: number;
          readonly leaseEvents: number;
          readonly taskLeaseIds: number;
          readonly reservationEvents: number;
          readonly reservationStates: number;
          readonly evidence: number;
          readonly receipts: number;
          readonly markers: number;
          readonly leaseId: string;
          readonly holderId: string;
          readonly fenceToken: number;
          readonly leaseRevision: number;
        }>`
          SELECT
            (SELECT count(*) FROM agent_control_events
              WHERE aggregate_kind = 'stage-run'
                AND json_extract(payload_json, '$.stageKind') = 'verification') AS "stageEvents",
            (SELECT count(*) FROM agent_control_stage_run_states
              WHERE stage_kind = 'verification') AS "stageStates",
            (SELECT count(*) FROM agent_control_events
              WHERE aggregate_kind = 'stage-run-lease'
                AND json_extract(payload_json, '$.stageRunId') IN (
                  SELECT verification_stage_run_id
                  FROM agent_control_verification_admission_evidence
                )) AS "leaseEvents",
            (SELECT count(DISTINCT lease_id) FROM agent_control_events
              WHERE aggregate_kind = 'stage-run-lease'
                AND json_extract(payload_json, '$.taskId') = ${claim.evidence.taskId})
              AS "taskLeaseIds",
            (SELECT count(*) FROM agent_control_events
              WHERE aggregate_kind = 'controlled-thread-reservation'
                AND json_extract(payload_json, '$.stageKind') = 'verification')
              AS "reservationEvents",
            (SELECT count(*) FROM agent_control_verification_thread_reservation_states)
              AS "reservationStates",
            (SELECT count(*) FROM agent_control_verification_admission_evidence) AS evidence,
            (SELECT count(*) FROM agent_control_verification_admission_receipts) AS receipts,
            (SELECT count(*) FROM agent_control_verification_admission_markers) AS markers,
            lease.lease_id AS "leaseId", lease.holder_id AS "holderId",
            lease.fence_token AS "fenceToken", lease.revision AS "leaseRevision"
          FROM agent_control_stage_run_lease_states lease
          WHERE lease.task_id = ${claim.evidence.taskId}
        `;
        assert.deepStrictEqual(counts, {
          stageEvents: 1,
          stageStates: 1,
          leaseEvents: 1,
          taskLeaseIds: 1,
          reservationEvents: 1,
          reservationStates: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
          leaseId: implementation!.leaseId,
          holderId: implementation!.holderId,
          fenceToken: implementation!.fenceToken + 1,
          leaseRevision: 5,
        });

        const totalChangesBefore = (yield* database.sqlA<{
          readonly count: number;
        }>`SELECT total_changes() AS count`)[0]!.count;
        const replay = yield* verification.admission.processResultEvidence(
          implementation!.resultEvidenceId,
        );
        assert.equal(replay._tag, "Replayed");
        assert.equal(
          (yield* database.sqlA<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
            .count,
          totalChangesBefore,
        );
        assert.deepStrictEqual(yield* Ref.get(hookCounts), {
          afterAuthoritativeRead: 1,
          beforeWrites: 1,
          beforeFinalMarker: 1,
          afterNativeCommit: 1,
          afterPublication: 1,
        });
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
        assert.equal(
          (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
          1,
        );
      }),
    ),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("preserves a Verification Admission $phase and rolls back before its marker", ({ phase }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const { setup, implementation } = yield* prepareSucceededImplementationFinalization(
          database,
          planningFinalizer,
          `verification-admission-before-marker-${phase}`,
        );
        const defect = { _tag: "VerificationAdmissionBeforeMarkerDefect", phase } as const;
        const verification = yield* buildVerificationAdmission({
          sql: database.sqlA,
          scope: database.scopeA,
          planningFinalizer,
          implementationFinalizer: setup.finalizer.finalizer,
          handoffStore: setup.coordinator.handoffStore,
          admissionHarness: setup.candidate.admissionHarness,
          hooks: {
            ...noopVerificationAdmissionHooks,
            beforeFinalMarker: () => (phase === "defect" ? Effect.die(defect) : Effect.interrupt),
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);
        yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);

        const exit = yield* Effect.exit(
          verification.admission.processResultEvidence(implementation.resultEvidenceId),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          if (phase === "defect") {
            const reason = exit.cause.reasons.find(Cause.isDieReason);
            assert.isDefined(reason);
            if (reason !== undefined && Cause.isDieReason(reason)) {
              assert.strictEqual(reason.defect, defect);
            }
          } else {
            assert.isTrue(exit.cause.reasons.some(Cause.isInterruptReason));
          }
        }
        assert.deepStrictEqual(
          yield* verificationAdmissionCounts(database.sqlB),
          noVerificationAdmission,
        );
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
        assert.equal(
          (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
          0,
        );
      }),
    ),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])(
  "preserves a Verification Admission $phase after commit and replays without effects",
  ({ phase }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const { setup, implementation } = yield* prepareSucceededImplementationFinalization(
            database,
            planningFinalizer,
            `verification-admission-after-commit-${phase}`,
          );
          const defect = { _tag: "VerificationAdmissionAfterCommitDefect", phase } as const;
          const verificationA = yield* buildVerificationAdmission({
            sql: database.sqlA,
            scope: database.scopeA,
            planningFinalizer,
            implementationFinalizer: setup.finalizer.finalizer,
            handoffStore: setup.coordinator.handoffStore,
            admissionHarness: setup.candidate.admissionHarness,
            hooks: {
              ...noopVerificationAdmissionHooks,
              afterNativeCommit: () => (phase === "defect" ? Effect.die(defect) : Effect.interrupt),
            },
          });
          yield* Ref.set(planningFinalizer.stagePublished, []);
          yield* Ref.set(planningFinalizer.leasePublished, []);
          yield* Ref.set(setup.candidate.admissionHarness.reservationPublished, []);

          const lost = yield* Effect.exit(
            verificationA.admission.processResultEvidence(implementation.resultEvidenceId),
          );
          assert.isTrue(Exit.isFailure(lost));
          if (Exit.isFailure(lost)) {
            if (phase === "defect") {
              const reason = lost.cause.reasons.find(Cause.isDieReason);
              assert.isDefined(reason);
              if (reason !== undefined && Cause.isDieReason(reason)) {
                assert.strictEqual(reason.defect, defect);
              }
            } else {
              assert.isTrue(lost.cause.reasons.some(Cause.isInterruptReason));
            }
          }
          assert.deepStrictEqual(yield* verificationAdmissionCounts(database.sqlB), {
            stageEvents: 1,
            leaseEvents: 1,
            reservationEvents: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
          });
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
          assert.equal(
            (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
            0,
          );

          const verificationB = yield* buildVerificationAdmission({
            sql: database.sqlB,
            scope: database.scopeB,
            planningFinalizer,
            implementationFinalizer: setup.finalizer.finalizer,
            handoffStore: setup.coordinator.handoffStore,
            admissionHarness: setup.candidate.admissionHarness,
            hooks: {
              ...noopVerificationAdmissionHooks,
              afterAuthoritativeRead: () => Effect.die("replay-authority"),
              beforeWrites: () => Effect.die("replay-writes"),
              beforeFinalMarker: () => Effect.die("replay-marker"),
              afterNativeCommit: () => Effect.die("replay-commit"),
              afterPublication: () => Effect.die("replay-publication"),
            },
          });
          const totalChangesBefore = (yield* database.sqlB<{
            readonly count: number;
          }>`SELECT total_changes() AS count`)[0]!.count;
          assert.equal(
            (yield* verificationB.admission.processResultEvidence(implementation.resultEvidenceId))
              ._tag,
            "Replayed",
          );
          assert.equal(
            (yield* database.sqlB<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
              .count,
            totalChangesBefore,
          );
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
          assert.equal(
            (yield* Ref.get(setup.candidate.admissionHarness.reservationPublished)).length,
            0,
          );
        }),
      ),
    ),
);

it.effect.each<{
  readonly deliveryState: "completed" | "failed" | "interrupted";
  readonly expectedOutcome: "succeeded" | "failed" | "cancelled";
  readonly terminalSessionStatus: "ready" | "error";
}>([
  { deliveryState: "completed", expectedOutcome: "succeeded", terminalSessionStatus: "ready" },
  { deliveryState: "failed", expectedOutcome: "failed", terminalSessionStatus: "error" },
  {
    deliveryState: "interrupted",
    expectedOutcome: "cancelled",
    terminalSessionStatus: "ready",
  },
])(
  "durably finalizes Implementation delivery $deliveryState as $expectedOutcome and replays receipt-first",
  ({ deliveryState, expectedOutcome, terminalSessionStatus }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const suffix = `implementation-finalization-${deliveryState}`;
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const setup = yield* prepareImplementationStageFinalizationCandidate(
            database,
            planningFinalizer,
            suffix,
            deliveryState !== "completed",
          );
          const runningClaim = Option.getOrThrow(
            yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
          );
          const observed = yield* setup.coordinator.handoffStore.observeProviderTerminal({
            threadId: runningClaim.evidence.threadId,
            providerTurnId: runningClaim.delivery.providerTurnId!,
            state: deliveryState,
            terminalAt,
            ...(deliveryState === "failed" ? { errorCode: "provider-terminal" } : {}),
          });
          assert.isTrue(Option.isSome(observed));

          if (deliveryState !== "completed") {
            assert.equal(
              (yield* setup.finalizer.finalizer.processHandoff(setup.handoffId))._tag,
              "Waiting",
            );
            assert.deepStrictEqual(
              yield* implementationFinalizationCounts(database.sqlA, setup.handoffId),
              {
                terminalStageEvents: 0,
                leaseReleaseEvents: 0,
                evidence: 0,
                receipts: 0,
                markers: 0,
                stageStatus: "running",
                stageRevision: 2,
                leaseStatus: "reserved",
                leaseRevision: 3,
                fenceToken: runningClaim.evidence.fenceToken,
              },
            );
          }

          yield* setup.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`provider:${suffix}:implementation-terminal`),
            threadId: runningClaim.evidence.threadId,
            session: {
              threadId: runningClaim.evidence.threadId,
              status: terminalSessionStatus,
              providerName: ProviderDriverKind.make("codex"),
              providerInstanceId: runningClaim.evidence.providerInstanceId,
              runtimeMode: runningClaim.evidence.runtimeMode,
              activeTurnId: null,
              lastError: deliveryState === "failed" ? "provider-terminal" : null,
              updatedAt: terminalAt,
            },
            createdAt: terminalAt,
          });

          if (deliveryState === "completed") {
            const reconstructed = yield* setup.finalizer.finalizer.processHandoff(setup.handoffId);
            assert.equal(reconstructed._tag, "Started");
            assert.deepStrictEqual(
              yield* implementationFinalizationCounts(database.sqlA, setup.handoffId),
              {
                terminalStageEvents: 0,
                leaseReleaseEvents: 0,
                evidence: 0,
                receipts: 0,
                markers: 0,
                stageStatus: "running",
                stageRevision: 2,
                leaseStatus: "reserved",
                leaseRevision: 3,
                fenceToken: runningClaim.evidence.fenceToken,
              },
            );
          }

          yield* Ref.set(planningFinalizer.stagePublished, []);
          yield* Ref.set(planningFinalizer.leasePublished, []);
          const finalized = yield* setup.finalizer.finalizer.processHandoff(setup.handoffId);
          assert.equal(finalized._tag, "Finalized");
          assert.deepStrictEqual(
            yield* implementationFinalizationCounts(database.sqlA, setup.handoffId),
            {
              terminalStageEvents: 1,
              leaseReleaseEvents: 1,
              evidence: 1,
              receipts: 1,
              markers: 1,
              stageStatus: expectedOutcome,
              stageRevision: 3,
              leaseStatus: "released",
              leaseRevision: 4,
              fenceToken: runningClaim.evidence.fenceToken,
            },
          );
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
          const resultRows = yield* database.sqlB<{
            readonly resultEvidenceId: string;
            readonly outcome: string;
            readonly orchestrationHistoryEventCount: number;
            readonly historyCount: number;
            readonly fenceToken: number;
          }>`
            SELECT result_evidence_id AS "resultEvidenceId", outcome,
              orchestration_history_event_count AS "orchestrationHistoryEventCount",
              json_array_length(orchestration_history_json) AS "historyCount",
              fence_token AS "fenceToken"
            FROM agent_control_implementation_result_evidence
            WHERE handoff_id = ${setup.handoffId}
          `;
          assert.equal(resultRows[0]?.outcome, expectedOutcome);
          assert.equal(resultRows[0]?.orchestrationHistoryEventCount, resultRows[0]?.historyCount);
          assert.equal(resultRows[0]?.fenceToken, runningClaim.evidence.fenceToken);

          yield* database.sqlA`
            UPDATE projection_projects SET title = ${`Mutable ${suffix}`}
            WHERE project_id = ${runningClaim.evidence.projectId}
          `;
          const totalChangesBefore = (yield* database.sqlA<{
            readonly count: number;
          }>`SELECT total_changes() AS count`)[0]!.count;
          const replay = yield* setup.finalizer.finalizer.processHandoff(setup.handoffId);
          assert.equal(replay._tag, "Replayed");
          assert.equal(
            (yield* database.sqlA<{ readonly count: number }>`SELECT total_changes() AS count`)[0]!
              .count,
            totalChangesBefore,
          );
          assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
          assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
          if (deliveryState !== "completed") {
            const verification = yield* buildVerificationAdmission({
              sql: database.sqlB,
              scope: database.scopeB,
              planningFinalizer,
              implementationFinalizer: setup.finalizer.finalizer,
              handoffStore: setup.coordinator.handoffStore,
              admissionHarness: setup.candidate.admissionHarness,
              hooks: {
                ...noopVerificationAdmissionHooks,
                afterAuthoritativeRead: () => Effect.die("not-candidate-authority"),
                beforeWrites: () => Effect.die("not-candidate-writes"),
                beforeFinalMarker: () => Effect.die("not-candidate-marker"),
                afterNativeCommit: () => Effect.die("not-candidate-commit"),
                afterPublication: () => Effect.die("not-candidate-publication"),
              },
            });
            assert.equal(
              (yield* verification.admission.processResultEvidence(resultRows[0]!.resultEvidenceId))
                ._tag,
              "NotCandidate",
            );
            assert.deepStrictEqual(
              yield* database.sqlB`
                SELECT
                  (SELECT count(*) FROM agent_control_verification_admission_evidence)
                    AS evidence,
                  (SELECT count(*) FROM agent_control_verification_admission_receipts)
                    AS receipts,
                  (SELECT count(*) FROM agent_control_verification_admission_markers)
                    AS markers
              `,
              [{ evidence: 0, receipts: 0, markers: 0 }],
            );
          }
          if (deliveryState === "completed") {
            yield* Effect.sync(() => {
              const native = new NodeSqlite.DatabaseSync(database.filename);
              try {
                native.exec(
                  "PRAGMA foreign_keys = OFF; DROP TRIGGER agent_control_implementation_stage_finalization_markers_no_delete",
                );
                native
                  .prepare(
                    "DELETE FROM agent_control_implementation_stage_finalization_markers WHERE handoff_id = ?",
                  )
                  .run(setup.handoffId);
              } finally {
                native.close();
              }
            });
            const partial = yield* Effect.exit(
              setup.finalizer.finalizer.processHandoff(setup.handoffId),
            );
            assert.isTrue(Exit.isFailure(partial));
            if (Exit.isFailure(partial)) {
              const found = Cause.findErrorOption(partial.cause);
              assert.isTrue(Option.isSome(found));
              if (Option.isSome(found)) {
                assert.isTrue(isImplementationFinalizerError(found.value));
                if (isImplementationFinalizerError(found.value)) {
                  assert.equal(found.value.reason, "partial-replay");
                }
              }
            }
          }
        }),
      ),
    ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("preserves an Implementation $phase after commit and recovers receipt-first", ({ phase }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizer,
          `implementation-finalization-post-commit-${phase}`,
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`provider:implementation-post-commit-${phase}:terminal`),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });
        const injected = yield* buildImplementationStageFinalizer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          planningFinalizer,
          starter: setup.starter,
          hooks: {
            ...noopImplementationStageFinalizerHooks,
            afterOuterCommit: () =>
              phase === "defect"
                ? Effect.die(new Error("implementation-post-commit-defect"))
                : Effect.interrupt,
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);

        const exit = yield* Effect.exit(injected.finalizer.processHandoff(setup.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          if (phase === "defect") {
            assert.include(Cause.pretty(exit.cause), "implementation-post-commit-defect");
          } else {
            assert.isTrue(Cause.hasInterruptsOnly(exit.cause));
          }
        }
        assert.deepStrictEqual(
          yield* implementationFinalizationCounts(database.sqlB, setup.handoffId),
          {
            terminalStageEvents: 1,
            leaseReleaseEvents: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
            stageStatus: "succeeded",
            stageRevision: 3,
            leaseStatus: "released",
            leaseRevision: 4,
            fenceToken: claim.evidence.fenceToken,
          },
        );
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
        assert.equal(
          (yield* setup.finalizer.finalizer.processHandoff(setup.handoffId))._tag,
          "Replayed",
        );
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
      }),
    ),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("rolls back the Implementation boundary on a pre-marker $phase", ({ phase }) =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizer,
          `implementation-finalization-pre-marker-${phase}`,
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make(`provider:implementation-pre-marker-${phase}:terminal`),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });
        const injected = yield* buildImplementationStageFinalizer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          planningFinalizer,
          starter: setup.starter,
          hooks: {
            ...noopImplementationStageFinalizerHooks,
            beforeFinalMarker: () =>
              phase === "defect"
                ? Effect.die(new Error("implementation-pre-marker-defect"))
                : Effect.interrupt,
          },
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);

        const exit = yield* Effect.exit(injected.finalizer.processHandoff(setup.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          if (phase === "defect") {
            assert.include(Cause.pretty(exit.cause), "implementation-pre-marker-defect");
          } else {
            assert.isTrue(Cause.hasInterruptsOnly(exit.cause));
          }
        }
        assert.deepStrictEqual(
          yield* implementationFinalizationCounts(database.sqlB, setup.handoffId),
          {
            terminalStageEvents: 0,
            leaseReleaseEvents: 0,
            evidence: 0,
            receipts: 0,
            markers: 0,
            stageStatus: "running",
            stageRevision: 2,
            leaseStatus: "reserved",
            leaseRevision: 3,
            fenceToken: claim.evidence.fenceToken,
          },
        );
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
      }),
    ),
  ),
);

it.effect.each<{
  readonly caseName:
    | "completed-error"
    | "failed-ready"
    | "interrupted-error"
    | "wrong-occurred-at"
    | "wrong-updated-at"
    | "missing-terminal"
    | "matching-terminal";
  readonly deliveryState: "completed" | "failed" | "interrupted";
  readonly terminalSessionStatus: "ready" | "error" | null;
  readonly occurredAt: string;
  readonly updatedAt: string;
  readonly expected: "Ambiguous" | "Waiting" | "Finalized";
}>([
  {
    caseName: "completed-error",
    deliveryState: "completed",
    terminalSessionStatus: "error",
    occurredAt: terminalAt,
    updatedAt: terminalAt,
    expected: "Ambiguous",
  },
  {
    caseName: "failed-ready",
    deliveryState: "failed",
    terminalSessionStatus: "ready",
    occurredAt: terminalAt,
    updatedAt: terminalAt,
    expected: "Ambiguous",
  },
  {
    caseName: "interrupted-error",
    deliveryState: "interrupted",
    terminalSessionStatus: "error",
    occurredAt: terminalAt,
    updatedAt: terminalAt,
    expected: "Ambiguous",
  },
  {
    caseName: "wrong-occurred-at",
    deliveryState: "completed",
    terminalSessionStatus: "ready",
    occurredAt: "2026-08-02T08:02:01.000Z",
    updatedAt: terminalAt,
    expected: "Ambiguous",
  },
  {
    caseName: "wrong-updated-at",
    deliveryState: "completed",
    terminalSessionStatus: "ready",
    occurredAt: terminalAt,
    updatedAt: "2026-08-02T08:02:01.000Z",
    expected: "Ambiguous",
  },
  {
    caseName: "missing-terminal",
    deliveryState: "completed",
    terminalSessionStatus: null,
    occurredAt: terminalAt,
    updatedAt: terminalAt,
    expected: "Waiting",
  },
  {
    caseName: "matching-terminal",
    deliveryState: "completed",
    terminalSessionStatus: "ready",
    occurredAt: terminalAt,
    updatedAt: terminalAt,
    expected: "Finalized",
  },
])(
  "classifies a single persisted Implementation terminal for $caseName as $expected",
  ({ caseName, deliveryState, terminalSessionStatus, occurredAt, updatedAt, expected }) =>
    withNode(
      Effect.scoped(
        Effect.gen(function* () {
          const database = yield* makeSharedDatabase();
          const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
          const setup = yield* prepareImplementationStageFinalizationCandidate(
            database,
            planningFinalizer,
            `implementation-single-terminal-${caseName}`,
          );
          const claim = Option.getOrThrow(
            yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
          );
          assert.isTrue(
            Option.isSome(
              yield* setup.coordinator.handoffStore.observeProviderTerminal({
                threadId: claim.evidence.threadId,
                providerTurnId: claim.delivery.providerTurnId!,
                state: deliveryState,
                terminalAt,
                ...(deliveryState === "failed" ? { errorCode: "provider-terminal" } : {}),
              }),
            ),
          );
          if (terminalSessionStatus !== null) {
            yield* setup.coordinator.orchestration.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`provider:implementation-single-terminal:${caseName}`),
              threadId: claim.evidence.threadId,
              session: {
                threadId: claim.evidence.threadId,
                status: terminalSessionStatus,
                providerName: ProviderDriverKind.make("codex"),
                providerInstanceId: claim.evidence.providerInstanceId,
                runtimeMode: claim.evidence.runtimeMode,
                activeTurnId: null,
                lastError: terminalSessionStatus === "error" ? "provider-terminal" : null,
                updatedAt,
              },
              createdAt: occurredAt,
            });
          }

          yield* Ref.set(planningFinalizer.stagePublished, []);
          yield* Ref.set(planningFinalizer.leasePublished, []);
          const result = yield* setup.finalizer.finalizer.processHandoff(setup.handoffId);
          assert.equal(result._tag, expected);

          const counts = yield* implementationFinalizationCounts(database.sqlA, setup.handoffId);
          const leaseRows = yield* database.sqlA<{
            readonly holderId: string;
            readonly fenceToken: number;
          }>`
            SELECT holder_id AS "holderId", fence_token AS "fenceToken"
            FROM agent_control_stage_run_lease_states
            WHERE lease_id = ${claim.evidence.leaseId}
          `;
          assert.deepStrictEqual(leaseRows, [
            {
              holderId: claim.evidence.leaseHolderId,
              fenceToken: claim.evidence.fenceToken,
            },
          ]);

          if (expected === "Finalized") {
            assert.deepStrictEqual(counts, {
              terminalStageEvents: 1,
              leaseReleaseEvents: 1,
              evidence: 1,
              receipts: 1,
              markers: 1,
              stageStatus: "succeeded",
              stageRevision: 3,
              leaseStatus: "released",
              leaseRevision: 4,
              fenceToken: claim.evidence.fenceToken,
            });
            assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
            assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
          } else {
            assert.deepStrictEqual(counts, {
              terminalStageEvents: 0,
              leaseReleaseEvents: 0,
              evidence: 0,
              receipts: 0,
              markers: 0,
              stageStatus: "running",
              stageRevision: 2,
              leaseStatus: "reserved",
              leaseRevision: 3,
              fenceToken: claim.evidence.fenceToken,
            });
            assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 0);
            assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 0);
          }
        }),
      ),
    ),
);

it.effect("treats contradictory Implementation terminal sessions as ambiguous", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizer,
          "implementation-finalization-terminal-conflict",
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        for (const [index, status] of (["ready", "error"] as const).entries()) {
          yield* setup.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`provider:implementation-terminal-conflict:${index}`),
            threadId: claim.evidence.threadId,
            session: {
              threadId: claim.evidence.threadId,
              status,
              providerName: ProviderDriverKind.make("codex"),
              providerInstanceId: claim.evidence.providerInstanceId,
              runtimeMode: claim.evidence.runtimeMode,
              activeTurnId: null,
              lastError: status === "error" ? "contradictory-terminal" : null,
              updatedAt: terminalAt,
            },
            createdAt: terminalAt,
          });
        }

        assert.equal(
          (yield* setup.finalizer.finalizer.processHandoff(setup.handoffId))._tag,
          "Ambiguous",
        );
        const counts = yield* implementationFinalizationCounts(database.sqlA, setup.handoffId);
        assert.equal(counts.terminalStageEvents, 0);
        assert.equal(counts.leaseReleaseEvents, 0);
        assert.equal(counts.evidence, 0);
        assert.equal(counts.receipts, 0);
        assert.equal(counts.markers, 0);
        assert.equal(counts.stageStatus, "running");
        assert.equal(counts.leaseStatus, "reserved");
      }),
    ),
  ),
);

it.effect("rejects a divergent terminal orchestration projection without release", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizer,
          "implementation-finalization-projection-divergence",
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("provider:implementation-projection-divergence:terminal"),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });
        yield* database.sqlA`
          UPDATE projection_thread_sessions
          SET status = 'running', active_turn_id = ${claim.delivery.providerTurnId}
          WHERE thread_id = ${claim.evidence.threadId}
        `;

        const exit = yield* Effect.exit(setup.finalizer.finalizer.processHandoff(setup.handoffId));
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          const found = Cause.findErrorOption(exit.cause);
          assert.isTrue(Option.isSome(found));
          if (Option.isSome(found) && isImplementationFinalizerError(found.value)) {
            assert.equal(found.value.reason, "orchestration-history-corrupt");
          }
        }
        const counts = yield* implementationFinalizationCounts(database.sqlA, setup.handoffId);
        assert.equal(counts.terminalStageEvents, 0);
        assert.equal(counts.leaseReleaseEvents, 0);
        assert.equal(counts.markers, 0);
        assert.equal(counts.stageStatus, "running");
        assert.equal(counts.leaseStatus, "reserved");
      }),
    ),
  ),
);

it.effect("isolates a missing Implementation companion before a healthy recovery candidate", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = yield* Effect.forEach(
          ["implementation-finalization-recovery-a", "implementation-finalization-recovery-b"],
          (suffix) =>
            prepareImplementationStageFinalizationCandidate(database, planningFinalizer, suffix),
          { concurrency: 1 },
        );
        for (const setup of candidates) {
          const claim = Option.getOrThrow(
            yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
          );
          assert.isTrue(
            Option.isSome(
              yield* setup.coordinator.handoffStore.observeProviderTerminal({
                threadId: claim.evidence.threadId,
                providerTurnId: claim.delivery.providerTurnId!,
                state: "completed",
                terminalAt,
              }),
            ),
          );
          yield* setup.coordinator.orchestration.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`provider:${setup.handoffId}:terminal`),
            threadId: claim.evidence.threadId,
            session: {
              threadId: claim.evidence.threadId,
              status: "ready",
              providerName: ProviderDriverKind.make("codex"),
              providerInstanceId: claim.evidence.providerInstanceId,
              runtimeMode: claim.evidence.runtimeMode,
              activeTurnId: null,
              lastError: null,
              updatedAt: terminalAt,
            },
            createdAt: terminalAt,
          });
        }
        const [invalid, healthy] = candidates.toSorted((left, right) =>
          left.handoffId.localeCompare(right.handoffId),
        );
        assert.isDefined(invalid);
        assert.isDefined(healthy);
        const firstPage = yield* invalid!.coordinator.handoffStore.listStageFinalizationCandidates({
          limit: 1,
        });
        const secondPage = yield* invalid!.coordinator.handoffStore.listStageFinalizationCandidates(
          {
            afterHandoffId: firstPage[0]!,
            limit: 1,
          },
        );
        assert.deepStrictEqual(
          [...firstPage, ...secondPage],
          [invalid!.handoffId, healthy!.handoffId],
        );
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec(
              "PRAGMA foreign_keys = OFF; DROP TRIGGER agent_control_implementation_handoff_receipts_no_delete",
            );
            native
              .prepare(
                "DELETE FROM agent_control_implementation_handoff_receipts WHERE handoff_id = ?",
              )
              .run(invalid!.handoffId);
          } finally {
            native.close();
          }
        });
        yield* Ref.set(planningFinalizer.stagePublished, []);
        yield* Ref.set(planningFinalizer.leasePublished, []);

        yield* invalid!.finalizer.finalizer.recover;

        const invalidCounts = yield* implementationFinalizationCounts(
          database.sqlA,
          invalid!.handoffId,
        );
        assert.equal(invalidCounts.terminalStageEvents, 0);
        assert.equal(invalidCounts.leaseReleaseEvents, 0);
        assert.equal(invalidCounts.markers, 0);
        const healthyCounts = yield* implementationFinalizationCounts(
          database.sqlA,
          healthy!.handoffId,
        );
        assert.equal(healthyCounts.terminalStageEvents, 1);
        assert.equal(healthyCounts.leaseReleaseEvents, 1);
        assert.equal(healthyCounts.evidence, 1);
        assert.equal(healthyCounts.receipts, 1);
        assert.equal(healthyCounts.markers, 1);
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);

        yield* invalid!.finalizer.finalizer.recover;
        assert.equal((yield* Ref.get(planningFinalizer.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizer.leasePublished)).length, 1);
      }),
    ),
  ),
);

it.effect("keeps an ambiguous Implementation result running with its lease reserved", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = (yield* prepareImplementationDeliveryRecoveryCandidates(
          database,
          planningFinalizer,
          ["implementation-finalization-ambiguous"],
        ))[0]!;
        const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
        const executorCalls = yield* Ref.make(0);
        const consumer = yield* buildImplementationConsumer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          executorCalls,
          providerEvents,
          responseLoss: true,
        });
        yield* consumer.consumer.processHandoff(setup.handoffId);
        const ambiguousClaim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.equal(ambiguousClaim.delivery.state, "ambiguous");
        const starter = yield* buildImplementationStageStarter({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          finalizer: planningFinalizer,
        });
        const finalizer = yield* buildImplementationStageFinalizer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          planningFinalizer,
          starter,
        });
        assert.equal(
          (yield* finalizer.finalizer.processHandoff(setup.handoffId))._tag,
          "Ambiguous",
        );
        const counts = yield* implementationFinalizationCounts(database.sqlA, setup.handoffId);
        assert.equal(counts.terminalStageEvents, 0);
        assert.equal(counts.leaseReleaseEvents, 0);
        assert.equal(counts.evidence, 0);
        assert.equal(counts.receipts, 0);
        assert.equal(counts.markers, 0);
        assert.equal(counts.stageStatus, "prepared");
        assert.equal(counts.stageRevision, 1);
        assert.equal(counts.leaseStatus, "reserved");
        assert.equal(counts.fenceToken, ambiguousClaim.evidence.fenceToken);
      }),
    ),
  ),
);

it.effect("converges two WAL finalizers on one terminal Stage and Lease release", () =>
  withNode(
    Effect.scoped(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const planningFinalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const setup = yield* prepareImplementationStageFinalizationCandidate(
          database,
          planningFinalizerA,
          "implementation-finalization-race",
        );
        const claim = Option.getOrThrow(
          yield* setup.coordinator.handoffStore.loadAcceptedByHandoffId(setup.handoffId),
        );
        assert.isTrue(
          Option.isSome(
            yield* setup.coordinator.handoffStore.observeProviderTerminal({
              threadId: claim.evidence.threadId,
              providerTurnId: claim.delivery.providerTurnId!,
              state: "completed",
              terminalAt,
            }),
          ),
        );
        yield* setup.coordinator.orchestration.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("provider:implementation-finalization-race:terminal"),
          threadId: claim.evidence.threadId,
          session: {
            threadId: claim.evidence.threadId,
            status: "ready",
            providerName: ProviderDriverKind.make("codex"),
            providerInstanceId: claim.evidence.providerInstanceId,
            runtimeMode: claim.evidence.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: terminalAt,
          },
          createdAt: terminalAt,
        });

        const planningFinalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          planningFinalizerB,
          setup.candidate.task,
          setup.candidate.worktree,
          noopAdmissionHooks,
        );
        const coordinatorB = yield* buildImplementationCoordinator({
          sql: database.sqlB,
          scope: database.scopeB,
          suffix: "implementation-finalization-race-b",
          admission: admissionB.admission,
          finalizer: planningFinalizerB,
          admissionHarness: admissionB,
          task: setup.candidate.task,
          worktree: setup.candidate.worktree,
        });
        const starterB = yield* buildImplementationStageStarter({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          finalizer: planningFinalizerB,
        });

        const reachedA = yield* Deferred.make<void>();
        const reachedB = yield* Deferred.make<void>();
        const releaseA = yield* Deferred.make<void>();
        const releaseB = yield* Deferred.make<void>();
        yield* Effect.addFinalizer(() =>
          Effect.all(
            [Deferred.succeed(releaseA, undefined), Deferred.succeed(releaseB, undefined)],
            { discard: true },
          ),
        );
        const raceHooks = (
          reached: Deferred.Deferred<void>,
          release: Deferred.Deferred<void>,
        ): AgentControlImplementationStageFinalizerHooksShape => ({
          ...noopImplementationStageFinalizerHooks,
          afterAuthoritativeEvidence: () =>
            Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release))),
        });
        const finalizerA = yield* buildImplementationStageFinalizer({
          sql: database.sqlA,
          scope: database.scopeA,
          coordinator: setup.coordinator,
          planningFinalizer: planningFinalizerA,
          starter: setup.starter,
          hooks: raceHooks(reachedA, releaseA),
        });
        const finalizerB = yield* buildImplementationStageFinalizer({
          sql: database.sqlB,
          scope: database.scopeB,
          coordinator: coordinatorB,
          planningFinalizer: planningFinalizerB,
          starter: starterB,
          hooks: raceHooks(reachedB, releaseB),
        });
        yield* Ref.set(planningFinalizerA.stagePublished, []);
        yield* Ref.set(planningFinalizerA.leasePublished, []);
        yield* Ref.set(planningFinalizerB.stagePublished, []);
        yield* Ref.set(planningFinalizerB.leasePublished, []);

        const fiberA = yield* finalizerA.finalizer
          .processHandoff(setup.handoffId)
          .pipe(Effect.forkChild);
        const fiberB = yield* finalizerB.finalizer
          .processHandoff(setup.handoffId)
          .pipe(Effect.forkChild);
        yield* Effect.all([Deferred.await(reachedA), Deferred.await(reachedB)], {
          concurrency: "unbounded",
        }).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseA, undefined);
        const winner = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
        yield* Deferred.succeed(releaseB, undefined);
        const loser = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
        assert.equal(winner._tag, "Finalized");
        assert.equal(loser._tag, "Replayed");

        assert.deepStrictEqual(
          yield* implementationFinalizationCounts(database.sqlB, setup.handoffId),
          {
            terminalStageEvents: 1,
            leaseReleaseEvents: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
            stageStatus: "succeeded",
            stageRevision: 3,
            leaseStatus: "released",
            leaseRevision: 4,
            fenceToken: claim.evidence.fenceToken,
          },
        );
        assert.equal((yield* Ref.get(planningFinalizerA.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizerA.leasePublished)).length, 1);
        assert.equal((yield* Ref.get(planningFinalizerB.stagePublished)).length, 0);
        assert.equal((yield* Ref.get(planningFinalizerB.leasePublished)).length, 0);
      }),
    ),
  ),
);

it.live(
  "durably admits one prepared implementation successor and replays without publication",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizerHarness = yield* buildFinalizer(database.sqlA, database.scopeA);
        const hookOrder = yield* Ref.make<ReadonlyArray<string>>([]);
        let admissionHarness: AdmissionHarness | undefined;
        const candidate = yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerHarness,
          "implementation-success",
          {
            ...noopAdmissionHooks,
            beforeFinalMarker: () => Ref.update(hookOrder, (order) => [...order, "before-marker"]),
            afterNativeCommit: (observation) =>
              Effect.gen(function* () {
                assert.deepStrictEqual(
                  yield* implementationAdmissionCounts(database.sqlB, observation.handoffId),
                  {
                    stageEvents: 1,
                    stageStates: 1,
                    leaseEvents: 1,
                    leaseStates: 1,
                    reservationEvents: 1,
                    reservationStates: 1,
                    evidence: 1,
                    receipts: 1,
                    markers: 1,
                  },
                );
                assert.equal((yield* Ref.get(finalizerHarness.stagePublished)).length, 0);
                assert.equal((yield* Ref.get(finalizerHarness.leasePublished)).length, 0);
                assert.equal((yield* Ref.get(admissionHarness!.reservationPublished)).length, 0);
                yield* Ref.update(hookOrder, (order) => [...order, "after-commit"]);
              }).pipe(Effect.orDie),
            afterPublication: () =>
              Effect.gen(function* () {
                assert.equal((yield* Ref.get(finalizerHarness.stagePublished)).length, 1);
                assert.equal((yield* Ref.get(finalizerHarness.leasePublished)).length, 1);
                assert.equal((yield* Ref.get(admissionHarness!.reservationPublished)).length, 1);
                yield* Ref.update(hookOrder, (order) => [...order, "after-publication"]);
              }),
          },
        );
        admissionHarness = candidate.admissionHarness;
        const result = yield* candidate.admissionHarness.admission.processHandoff(
          candidate.seeded.evidence.handoffId,
        );
        assert.equal(result._tag, "Admitted");
        const replay = yield* candidate.admissionHarness.admission.processHandoff(
          candidate.seeded.evidence.handoffId,
        );
        assert.equal(replay._tag, "Replayed");

        const [counts] = yield* database.sqlA<{
          readonly implementationStages: number;
          readonly implementationLeases: number;
          readonly implementationReservations: number;
          readonly evidence: number;
          readonly receipts: number;
          readonly markers: number;
        }>`
        SELECT
          (SELECT count(*) FROM agent_control_stage_run_states
            WHERE task_id = ${candidate.seeded.evidence.taskId}
              AND role_id = 'implementer' AND stage_kind = 'implementation'
              AND stage_ordinal = 2 AND attempt_ordinal = 1
              AND status = 'prepared' AND revision = 1) AS "implementationStages",
          (SELECT count(*) FROM agent_control_stage_run_lease_states
            WHERE lease_id = ${candidate.seeded.leaseId} AND status = 'reserved'
              AND fence_token = ${candidate.seeded.evidence.fenceToken + 1})
            AS "implementationLeases",
          (SELECT count(*) FROM agent_control_implementation_thread_reservation_states
            WHERE task_id = ${candidate.seeded.evidence.taskId}
              AND status = 'prepared' AND revision = 1) AS "implementationReservations",
          (SELECT count(*) FROM agent_control_implementation_admission_evidence
            WHERE handoff_id = ${candidate.seeded.evidence.handoffId}) AS evidence,
          (SELECT count(*) FROM agent_control_implementation_admission_receipts
            WHERE handoff_id = ${candidate.seeded.evidence.handoffId}) AS receipts,
          (SELECT count(*) FROM agent_control_implementation_admission_markers
            WHERE handoff_id = ${candidate.seeded.evidence.handoffId}) AS markers
      `;
        assert.deepStrictEqual(counts, {
          implementationStages: 1,
          implementationLeases: 1,
          implementationReservations: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.equal((yield* Ref.get(finalizerHarness.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(finalizerHarness.leasePublished)).length, 1);
        assert.equal((yield* Ref.get(candidate.admissionHarness.reservationPublished)).length, 1);
        assert.deepStrictEqual(yield* Ref.get(hookOrder), [
          "before-marker",
          "after-commit",
          "after-publication",
        ]);

        const [binding] = yield* database.sqlA<{
          readonly resultEvidenceId: string;
          readonly planningStageRunId: string;
          readonly planningAttemptId: string;
          readonly planningThreadId: string;
          readonly planningReservationId: string;
          readonly planningFenceToken: number;
          readonly planId: string;
          readonly proposedPlanDigest: string;
          readonly worktreeReservationId: string;
          readonly worktreeRevision: number;
          readonly worktreeEventSequence: number;
          readonly worktreeOwnershipFingerprint: string;
          readonly implementationStageRunId: string;
          readonly implementationAttemptId: string;
          readonly implementationFenceToken: number;
          readonly implementationHolderId: string;
          readonly implementationThreadId: string;
          readonly stageRole: string;
          readonly stageStatus: string;
          readonly leaseStatus: string;
          readonly reservationRole: string;
          readonly reservationStatus: string;
          readonly reservationWorktreeId: string;
        }>`
        SELECT evidence.result_evidence_id AS "resultEvidenceId",
          evidence.planning_stage_run_id AS "planningStageRunId",
          evidence.planning_attempt_id AS "planningAttemptId",
          evidence.planning_thread_id AS "planningThreadId",
          evidence.planning_controlled_thread_reservation_id AS "planningReservationId",
          evidence.planning_fence_token AS "planningFenceToken",
          evidence.plan_id AS "planId", evidence.proposed_plan_digest AS "proposedPlanDigest",
          evidence.worktree_reservation_id AS "worktreeReservationId",
          evidence.worktree_revision AS "worktreeRevision",
          evidence.worktree_event_sequence AS "worktreeEventSequence",
          evidence.worktree_ownership_fingerprint AS "worktreeOwnershipFingerprint",
          evidence.implementation_stage_run_id AS "implementationStageRunId",
          evidence.implementation_attempt_id AS "implementationAttemptId",
          evidence.implementation_fence_token AS "implementationFenceToken",
          evidence.implementation_lease_holder_id AS "implementationHolderId",
          evidence.implementation_thread_id AS "implementationThreadId",
          stage.role_id AS "stageRole", stage.status AS "stageStatus",
          lease.status AS "leaseStatus", reservation.role_id AS "reservationRole",
          reservation.status AS "reservationStatus",
          reservation.worktree_reservation_id AS "reservationWorktreeId"
        FROM agent_control_implementation_admission_evidence evidence
        JOIN agent_control_stage_run_states stage
          ON stage.stage_run_id = evidence.implementation_stage_run_id
        JOIN agent_control_stage_run_lease_states lease
          ON lease.lease_id = evidence.implementation_lease_id
        JOIN agent_control_implementation_thread_reservation_states reservation
          ON reservation.controlled_thread_reservation_id =
            evidence.implementation_controlled_thread_reservation_id
        WHERE evidence.handoff_id = ${candidate.seeded.evidence.handoffId}
      `;
        assert.isDefined(binding);
        assert.equal(binding!.planningStageRunId, candidate.seeded.stageRunId);
        assert.equal(binding!.planningAttemptId, candidate.seeded.attemptId);
        assert.equal(binding!.planningThreadId, candidate.seeded.evidence.threadId);
        assert.equal(
          binding!.planningReservationId,
          candidate.seeded.evidence.controlledThreadReservationId,
        );
        assert.equal(binding!.planningFenceToken, candidate.seeded.evidence.fenceToken);
        assert.equal(binding!.planId, candidate.seeded.planId);
        assert.equal(binding!.worktreeReservationId, candidate.worktree.reservationId);
        assert.equal(binding!.worktreeRevision, candidate.worktree.revision);
        assert.equal(binding!.worktreeEventSequence, candidate.worktree.sequence);
        assert.equal(
          binding!.worktreeOwnershipFingerprint,
          candidate.worktree.ownershipFingerprint,
        );
        assert.equal(binding!.implementationFenceToken, candidate.seeded.evidence.fenceToken + 1);
        assert.equal(binding!.implementationHolderId, "runtime-holder");
        assert.equal(binding!.stageRole, "implementer");
        assert.equal(binding!.stageStatus, "prepared");
        assert.equal(binding!.leaseStatus, "reserved");
        assert.equal(binding!.reservationRole, "implementer");
        assert.equal(binding!.reservationStatus, "prepared");
        assert.equal(binding!.reservationWorktreeId, candidate.worktree.reservationId);
      }),
    ),
);

it.live("rejects the total controlled-thread lifecycle Direct-SQL matrix", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizer,
        "implementation-event-boundary",
      );
      assert.equal(
        (yield* candidate.admissionHarness.admission.processHandoff(
          candidate.seeded.evidence.handoffId,
        ))._tag,
        "Admitted",
      );
      interface ReservationEventRow {
        readonly sequence: number;
        readonly eventId: string;
        readonly aggregateKind: string;
        readonly streamId: string;
        readonly streamVersion: number;
        readonly eventType: string;
        readonly occurredAt: string;
        readonly commandId: string;
        readonly causationEventId: string | null;
        readonly correlationId: string;
        readonly actorAuthority: string;
        readonly payloadJson: string;
        readonly metadataJson: string;
      }
      const originals = yield* database.sqlA<ReservationEventRow>`
        SELECT sequence, event_id AS "eventId", aggregate_kind AS "aggregateKind",
          stream_id AS "streamId", stream_version AS "streamVersion",
          event_type AS "eventType", occurred_at AS "occurredAt",
          command_id AS "commandId", causation_event_id AS "causationEventId",
          correlation_id AS "correlationId", actor_authority AS "actorAuthority",
          payload_json AS "payloadJson", metadata_json AS "metadataJson"
        FROM agent_control_events
        WHERE aggregate_kind = 'controlled-thread-reservation'
        ORDER BY sequence
      `;
      const implementation = originals.find(
        ({ payloadJson }) =>
          (decodeUnknownJson(payloadJson) as { readonly stageKind?: unknown }).stageKind ===
          "implementation",
      );
      const planningPrepared = originals.find(
        ({ payloadJson, streamVersion }) =>
          streamVersion === 1 &&
          (decodeUnknownJson(payloadJson) as { readonly stageKind?: unknown }).stageKind ===
            "planning",
      );
      const planningBound = originals.find(
        ({ payloadJson, streamVersion }) =>
          streamVersion === 3 &&
          (decodeUnknownJson(payloadJson) as { readonly stageKind?: unknown }).stageKind ===
            "planning",
      );
      assert.isDefined(implementation);
      assert.isDefined(planningPrepared);
      assert.isDefined(planningBound);
      const implementationPayload = decodeUnknownJson(implementation!.payloadJson) as Record<
        string,
        unknown
      >;
      const planningPayload = decodeUnknownJson(planningPrepared!.payloadJson) as Record<
        string,
        unknown
      >;
      const boundPayload = decodeUnknownJson(planningBound!.payloadJson) as Record<string, unknown>;
      type Mutation = {
        readonly name: string;
        readonly target?: ReservationEventRow;
        readonly payload?: unknown;
        readonly payloadJson?: string | Uint8Array | null;
        readonly metadataJson?: string | Uint8Array | null;
        readonly eventId?: string;
        readonly aggregateKind?: string;
        readonly streamId?: string;
        readonly streamVersion?: number;
        readonly eventType?: string;
        readonly commandId?: string;
        readonly correlationId?: string;
        readonly actorAuthority?: string;
      };
      const variants = [
        {
          name: "missing-stage-kind",
          payload: Object.fromEntries(
            Object.entries(implementationPayload).filter(([key]) => key !== "stageKind"),
          ),
        },
        { name: "null-stage-kind", payload: { ...implementationPayload, stageKind: null } },
        { name: "empty-stage-kind", payload: { ...implementationPayload, stageKind: "" } },
        {
          name: "unknown-stage-kind",
          payload: { ...implementationPayload, stageKind: "review" },
        },
        { name: "stage-kind-storage", payload: { ...implementationPayload, stageKind: 2 } },
        { name: "payload-sql-null", payloadJson: null },
        {
          name: "payload-storage",
          payloadJson: Buffer.from(implementation!.payloadJson, "utf8"),
        },
        {
          name: "metadata-storage",
          metadataJson: Buffer.from(implementation!.metadataJson, "utf8"),
        },
        {
          name: "numeric-identity-storage",
          payload: {
            ...implementationPayload,
            taskRevision: String(implementationPayload.taskRevision),
          },
        },
        { name: "wrong-role", payload: { ...implementationPayload, roleId: "planning" } },
        { name: "null-central-role", payload: { ...implementationPayload, roleId: null } },
        {
          name: "wrong-stage-ordinal",
          payload: { ...implementationPayload, stageOrdinal: 1 },
        },
        {
          name: "wrong-attempt-ordinal",
          payload: { ...implementationPayload, attemptOrdinal: 2 },
        },
        { name: "wrong-status", payload: { ...implementationPayload, status: "bound" } },
        { name: "null-status", payload: { ...implementationPayload, status: null } },
        { name: "wrong-revision", streamVersion: 2 },
        {
          name: "foreign-stage-run",
          payload: { ...implementationPayload, stageRunId: "foreign-stage" },
        },
        {
          name: "foreign-reservation-id",
          payload: {
            ...implementationPayload,
            controlledThreadReservationId: "foreign-reservation",
          },
        },
        {
          name: "foreign-attempt",
          payload: { ...implementationPayload, attemptId: "foreign-attempt" },
        },
        {
          name: "foreign-thread",
          payload: { ...implementationPayload, threadId: "foreign-thread" },
        },
        {
          name: "foreign-lease",
          payload: { ...implementationPayload, leaseId: "foreign-lease" },
        },
        {
          name: "wrong-fence",
          payload: {
            ...implementationPayload,
            fenceToken: Number(implementationPayload.fenceToken) + 1,
          },
        },
        {
          name: "foreign-worktree",
          payload: { ...implementationPayload, worktreeReservationId: "foreign-worktree" },
        },
        { name: "foreign-task", payload: { ...implementationPayload, taskId: "foreign-task" } },
        {
          name: "foreign-project",
          payload: { ...implementationPayload, projectId: "foreign-project" },
        },
        {
          name: "foreign-source",
          payload: { ...implementationPayload, sourceIdentityFingerprint: "f".repeat(64) },
        },
        {
          name: "partial-catalog-task-revision",
          payload: {
            ...implementationPayload,
            taskRevision: Number(implementationPayload.taskRevision) + 1,
          },
        },
        { name: "cross-assigned-planning-payload", payload: planningPayload },
        { name: "noncanonical-payload", payloadJson: ` ${implementation!.payloadJson}` },
        { name: "noncanonical-metadata", metadataJson: ` ${implementation!.metadataJson}` },
        { name: "wrong-metadata", metadataJson: "{}" },
        { name: "wrong-authority", actorAuthority: "human" },
        { name: "wrong-event-type", eventType: "agentControl.stageRun.prepared" },
        { name: "wrong-aggregate", aggregateKind: "stage-run" },
        { name: "foreign-stream", streamId: "foreign-reservation" },
        { name: "foreign-event", eventId: "foreign-reservation-event" },
        { name: "foreign-command", commandId: "foreign-reservation-command" },
        { name: "foreign-correlation", correlationId: "foreign-reservation-command" },
        {
          name: "planning-materialization-command",
          target: planningBound!,
          payload: { ...boundPayload, materializationCommandId: "foreign-materialization" },
        },
        {
          name: "planning-lease-holder",
          target: planningBound!,
          payload: { ...boundPayload, leaseHolderId: "foreign-holder" },
        },
        {
          name: "planning-bound-time",
          target: planningBound!,
          payload: { ...boundPayload, boundAt: "2026-08-02T08:00:41.000Z" },
        },
        {
          name: "planning-null-result-sequence",
          target: planningBound!,
          payload: { ...boundPayload, orchestrationResultSequence: null },
        },
      ] satisfies ReadonlyArray<Mutation>;
      yield* Effect.sync(() => {
        const native = new NodeSqlite.DatabaseSync(database.filename);
        try {
          native.exec("PRAGMA busy_timeout = 5000");
          native.exec("PRAGMA journal_mode = WAL");
          native.exec("DROP TRIGGER agent_control_controlled_thread_event_no_delete");
          const remove = native.prepare("DELETE FROM agent_control_events WHERE event_id = ?");
          const insert = native.prepare(`
            INSERT INTO agent_control_events (
              sequence, event_id, aggregate_kind, stream_id, stream_version,
              event_type, occurred_at, command_id, causation_event_id,
              correlation_id, actor_authority, payload_json, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          `);
          const count = native.prepare(
            "SELECT count(*) AS count FROM agent_control_events WHERE event_id = ?",
          );
          for (const variant of variants) {
            const original = variant.target ?? implementation!;
            native.exec("PRAGMA foreign_keys = OFF");
            remove.run(original.eventId);
            native.exec("PRAGMA foreign_keys = ON");
            let rejected: unknown;
            try {
              insert.run(
                original.sequence,
                variant.eventId ?? original.eventId,
                variant.aggregateKind ?? original.aggregateKind,
                variant.streamId ?? original.streamId,
                variant.streamVersion ?? original.streamVersion,
                variant.eventType ?? original.eventType,
                original.occurredAt,
                variant.commandId ?? original.commandId,
                original.causationEventId,
                variant.correlationId ?? original.correlationId,
                variant.actorAuthority ?? original.actorAuthority,
                Object.hasOwn(variant, "payloadJson")
                  ? variant.payloadJson!
                  : variant.payload === undefined
                    ? original.payloadJson
                    : encodeUnknownJson(variant.payload),
                Object.hasOwn(variant, "metadataJson")
                  ? variant.metadataJson!
                  : original.metadataJson,
              );
            } catch (cause) {
              rejected = cause;
            }
            assert.isDefined(rejected, variant.name);
            assert.deepStrictEqual(count.get(original.eventId), { count: 0 }, variant.name);
          }
          for (const original of [implementation!, planningBound!]) {
            insert.run(
              original.sequence,
              original.eventId,
              original.aggregateKind,
              original.streamId,
              original.streamVersion,
              original.eventType,
              original.occurredAt,
              original.commandId,
              original.causationEventId,
              original.correlationId,
              original.actorAuthority,
              original.payloadJson,
              original.metadataJson,
            );
            assert.deepStrictEqual(count.get(original.eventId), { count: 1 });
          }
        } finally {
          native.close();
        }
      });
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
    }),
  ),
);

it.live("rebuilds mixed admission reservations twice after restart and rolls defects back", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizer = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizer,
        "implementation-rebuild",
      );
      assert.equal(
        (yield* candidate.admissionHarness.admission.processHandoff(
          candidate.seeded.evidence.handoffId,
        ))._tag,
        "Admitted",
      );
      const snapshot = (sql: SqlClient.SqlClient) =>
        Effect.all({
          events: sql`
            SELECT sequence, event_id, aggregate_kind, stream_id, stream_version,
              event_type, occurred_at, command_id, causation_event_id,
              correlation_id, actor_authority, hex(CAST(payload_json AS BLOB)) AS payload_bytes,
              hex(CAST(metadata_json AS BLOB)) AS metadata_bytes
            FROM agent_control_events
            WHERE aggregate_kind = 'controlled-thread-reservation'
            ORDER BY sequence
          `,
          planningCatalog: sql`
            SELECT * FROM agent_control_controlled_thread_stream_catalog
            ORDER BY controlled_thread_reservation_id, stream_version
          `,
          implementationCatalog: sql`
            SELECT * FROM agent_control_implementation_thread_stream_catalog
            ORDER BY controlled_thread_reservation_id
          `,
          projection: sql`
            SELECT * FROM agent_control_controlled_thread_reservation_states_all
            ORDER BY controlled_thread_reservation_id
          `,
          unionView: sql`
            SELECT controlled_thread_reservation_id, thread_id, project_id, task_id,
              task_revision, github_intake_sequence, source_identity_fingerprint,
              stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
              attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
              status, revision, last_event_sequence, prepared_at,
              hex(CAST(state_json AS BLOB)) AS state_bytes
            FROM agent_control_controlled_thread_reservation_states_all
            ORDER BY controlled_thread_reservation_id
          `,
          evidence: sql`
            SELECT * FROM agent_control_implementation_admission_evidence
            ORDER BY admission_evidence_id
          `,
          receipts: sql`
            SELECT * FROM agent_control_implementation_admission_receipts
            ORDER BY receipt_id
          `,
          markers: sql`
            SELECT * FROM agent_control_implementation_admission_markers
            ORDER BY marker_id
          `,
          sequences: sql`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
        });
      const before = yield* snapshot(database.sqlB);
      assert.equal(before.events.length, 4);
      assert.equal(before.planningCatalog.length, 3);
      assert.equal(before.implementationCatalog.length, 1);
      assert.equal(before.projection.length, 2);
      assert.equal(before.evidence.length, 1);
      assert.equal(before.receipts.length, 1);
      assert.equal(before.markers.length, 1);

      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        candidate.task,
        candidate.worktree,
        noopAdmissionHooks,
      );
      yield* database.sqlB`DELETE FROM agent_control_implementation_thread_reservation_states`;
      const planningOnly = yield* database.sqlB<{ readonly stageKind: string }>`
        SELECT stage_kind AS "stageKind"
        FROM agent_control_controlled_thread_reservation_states_all
        ORDER BY controlled_thread_reservation_id
      `;
      assert.deepStrictEqual(planningOnly, [{ stageKind: "planning" }]);
      yield* admissionB.reservationProjection.rebuild;
      assert.deepStrictEqual(yield* snapshot(database.sqlB), before);

      yield* database.sqlB`DELETE FROM agent_control_controlled_thread_reservation_states`;
      const implementationOnly = yield* database.sqlB<{ readonly stageKind: string }>`
        SELECT stage_kind AS "stageKind"
        FROM agent_control_controlled_thread_reservation_states_all
        ORDER BY controlled_thread_reservation_id
      `;
      assert.deepStrictEqual(implementationOnly, [{ stageKind: "implementation" }]);
      yield* admissionB.reservationProjection.rebuild;
      assert.deepStrictEqual(yield* snapshot(database.sqlB), before);
      yield* admissionB.reservationProjection.rebuild;
      assert.deepStrictEqual(yield* snapshot(database.sqlB), before);

      const rebuildDefect = new Error("controlled-thread-rebuild-injected-defect");
      const sqlLayer = Layer.succeed(SqlClient.SqlClient, database.sqlA);
      const stateContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationStateRepositoryLive).pipe(
          Layer.provide(sqlLayer),
        ),
        database.scopeA,
      );
      const cursorContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
        database.scopeA,
      );
      const failingEvents = AgentControlControlledThreadReservationEventStore.of({
        ...candidate.admissionHarness.reservationEvents,
        readGlobal: () => Effect.die(rebuildDefect),
      });
      const failingProjectionContext = yield* Layer.buildWithScope(
        Layer.fresh(AgentControlControlledThreadReservationProjectionLive).pipe(
          Layer.provide(
            Layer.mergeAll(
              sqlLayer,
              Layer.succeed(AgentControlControlledThreadReservationEventStore, failingEvents),
              Layer.succeed(
                AgentControlControlledThreadReservationStateRepository,
                Context.get(stateContext, AgentControlControlledThreadReservationStateRepository),
              ),
              Layer.succeed(
                AgentControlProjectionStateRepository,
                Context.get(cursorContext, AgentControlProjectionStateRepository),
              ),
            ),
          ),
        ),
        database.scopeA,
      );
      const failingProjection = Context.get(
        failingProjectionContext,
        AgentControlControlledThreadReservationProjection,
      );
      const failed = yield* Effect.exit(failingProjection.rebuild);
      assert.isTrue(Exit.isFailure(failed));
      if (Exit.isFailure(failed)) {
        assert.include(Cause.pretty(failed.cause), rebuildDefect.message);
      }
      assert.deepStrictEqual(yield* snapshot(database.sqlA), before);
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
    }),
  ),
);

it.effect.each<{ readonly outcome: "failed" | "cancelled" | "ambiguous" | "running" }>([
  { outcome: "failed" },
  { outcome: "cancelled" },
  { outcome: "ambiguous" },
  { outcome: "running" },
])("planning $outcome never becomes an implementation admission", ({ outcome }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerHarness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareNonSucceededAdmissionCandidate(
        database,
        finalizerHarness,
        `implementation-${outcome}`,
        outcome,
      );
      const result = yield* candidate.admissionHarness.admission.processHandoff(
        candidate.seeded.evidence.handoffId,
      );
      assert.equal(result._tag, "NotCandidate");
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlA, candidate.seeded.evidence.handoffId),
        noImplementationAdmission,
      );
      assert.equal((yield* Ref.get(candidate.admissionHarness.reservationPublished)).length, 0);
    }),
  ),
);

it.effect.each<{
  readonly corruption:
    | "missing-result"
    | "missing-receipt"
    | "missing-marker"
    | "wrong-plan-digest"
    | "noncanonical-plan"
    | "foreign-task"
    | "foreign-provider"
    | "receipt-mismatch"
    | "marker-mismatch"
    | "multiple-plans"
    | "noncanonical-orchestration";
}>([
  { corruption: "missing-result" },
  { corruption: "missing-receipt" },
  { corruption: "missing-marker" },
  { corruption: "wrong-plan-digest" },
  { corruption: "noncanonical-plan" },
  { corruption: "foreign-task" },
  { corruption: "foreign-provider" },
  { corruption: "receipt-mismatch" },
  { corruption: "marker-mismatch" },
  { corruption: "multiple-plans" },
  { corruption: "noncanonical-orchestration" },
])("$corruption planning evidence cannot admit implementation", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerHarness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerHarness,
        `admission-${corruption}`,
      );
      if (corruption === "multiple-plans") {
        yield* appendPlan(database.sqlA, candidate.seeded, "admission-extra-plan", {
          planId: `${candidate.seeded.planId}-foreign`,
          planMarkdown: "# Conflicting implementation plan",
        });
      } else {
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec("PRAGMA foreign_keys = OFF");
            native.exec("PRAGMA ignore_check_constraints = ON");
            if (corruption === "missing-result") {
              native.exec(
                "DROP TRIGGER agent_control_initial_planning_finalization_markers_no_delete",
              );
              native.exec(
                "DROP TRIGGER agent_control_initial_planning_finalization_receipts_no_delete",
              );
              native.exec("DROP TRIGGER agent_control_initial_planning_result_evidence_no_delete");
              native
                .prepare(
                  "DELETE FROM agent_control_initial_planning_finalization_markers WHERE handoff_id = ?",
                )
                .run(candidate.seeded.evidence.handoffId);
              native
                .prepare(
                  "DELETE FROM agent_control_initial_planning_finalization_receipts WHERE handoff_id = ?",
                )
                .run(candidate.seeded.evidence.handoffId);
              native
                .prepare(
                  "DELETE FROM agent_control_initial_planning_result_evidence WHERE handoff_id = ?",
                )
                .run(candidate.seeded.evidence.handoffId);
            } else if (corruption === "missing-receipt") {
              native.exec(
                "DROP TRIGGER agent_control_initial_planning_finalization_receipts_no_delete",
              );
              native
                .prepare(
                  "DELETE FROM agent_control_initial_planning_finalization_receipts WHERE handoff_id = ?",
                )
                .run(candidate.seeded.evidence.handoffId);
            } else if (corruption === "missing-marker") {
              native.exec(
                "DROP TRIGGER agent_control_initial_planning_finalization_markers_no_delete",
              );
              native
                .prepare(
                  "DELETE FROM agent_control_initial_planning_finalization_markers WHERE handoff_id = ?",
                )
                .run(candidate.seeded.evidence.handoffId);
            } else if (corruption === "noncanonical-orchestration") {
              native
                .prepare(
                  "UPDATE orchestration_events SET payload_json = ' ' || payload_json WHERE event_id = (SELECT plan_event_id FROM agent_control_initial_planning_result_evidence WHERE handoff_id = ?)",
                )
                .run(candidate.seeded.evidence.handoffId);
            } else {
              const table =
                corruption === "receipt-mismatch"
                  ? "agent_control_initial_planning_finalization_receipts"
                  : corruption === "marker-mismatch"
                    ? "agent_control_initial_planning_finalization_markers"
                    : "agent_control_initial_planning_result_evidence";
              native.exec(`DROP TRIGGER ${table}_no_update`);
              const mutation =
                corruption === "wrong-plan-digest"
                  ? "UPDATE agent_control_initial_planning_result_evidence SET proposed_plan_digest = ? WHERE handoff_id = ?"
                  : corruption === "noncanonical-plan"
                    ? "UPDATE agent_control_initial_planning_result_evidence SET proposed_plan_json = ' ' || proposed_plan_json WHERE handoff_id = ?"
                    : corruption === "foreign-task"
                      ? "UPDATE agent_control_initial_planning_result_evidence SET task_id = 'foreign-task' WHERE handoff_id = ?"
                      : corruption === "foreign-provider"
                        ? "UPDATE agent_control_initial_planning_result_evidence SET provider_instance_id = 'foreign-provider' WHERE handoff_id = ?"
                        : corruption === "receipt-mismatch"
                          ? "UPDATE agent_control_initial_planning_finalization_receipts SET outcome = 'failed' WHERE handoff_id = ?"
                          : "UPDATE agent_control_initial_planning_finalization_markers SET finalization_command_id = 'foreign-command' WHERE handoff_id = ?";
              const statement = native.prepare(mutation);
              if (corruption === "wrong-plan-digest") {
                statement.run("f".repeat(64), candidate.seeded.evidence.handoffId);
              } else {
                statement.run(candidate.seeded.evidence.handoffId);
              }
            }
          } finally {
            native.close();
          }
        });
      }
      const exit = yield* Effect.exit(
        candidate.admissionHarness.admission.processHandoff(candidate.seeded.evidence.handoffId),
      );
      if (Exit.isSuccess(exit)) {
        assert.equal(exit.value._tag, "NotCandidate");
      }
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlA, candidate.seeded.evidence.handoffId),
        noImplementationAdmission,
      );
      assert.equal((yield* Ref.get(finalizerHarness.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(finalizerHarness.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(candidate.admissionHarness.reservationPublished)).length, 0);
    }),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase before the admission marker rolls back every successor write", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        `admission-before-marker-${phase}`,
        {
          ...noopAdmissionHooks,
          beforeFinalMarker: () =>
            phase === "defect"
              ? Effect.die(new Error("before-admission-marker"))
              : Effect.interrupt,
        },
      );
      const firstExit = yield* Effect.exit(
        candidate.admissionHarness.admission.processHandoff(candidate.seeded.evidence.handoffId),
      );
      assert.isTrue(Exit.isFailure(firstExit));
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlA, candidate.seeded.evidence.handoffId),
        noImplementationAdmission,
      );
      assert.equal((yield* Ref.get(finalizerA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(finalizerA.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(candidate.admissionHarness.reservationPublished)).length, 0);

      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        candidate.task,
        candidate.worktree,
        noopAdmissionHooks,
      );
      const recovered = yield* admissionB.admission.processHandoff(
        candidate.seeded.evidence.handoffId,
      );
      assert.equal(recovered._tag, "Admitted");
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, candidate.seeded.evidence.handoffId),
        {
          stageEvents: 1,
          stageStates: 1,
          leaseEvents: 1,
          leaseStates: 1,
          reservationEvents: 1,
          reservationStates: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        },
      );
    }),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase after admission commit restarts as accepted replay without publication", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        `admission-after-commit-${phase}`,
        {
          ...noopAdmissionHooks,
          afterNativeCommit: () =>
            phase === "defect"
              ? Effect.die(new Error("admission-response-loss"))
              : Effect.interrupt,
        },
      );
      const lost = yield* Effect.exit(
        candidate.admissionHarness.admission.processHandoff(candidate.seeded.evidence.handoffId),
      );
      assert.isTrue(Exit.isFailure(lost));
      assert.equal((yield* Ref.get(finalizerA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(finalizerA.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(candidate.admissionHarness.reservationPublished)).length, 0);

      const committed = yield* implementationAdmissionCounts(
        database.sqlB,
        candidate.seeded.evidence.handoffId,
      );
      assert.deepStrictEqual(committed, {
        stageEvents: 1,
        stageStates: 1,
        leaseEvents: 1,
        leaseStates: 1,
        reservationEvents: 1,
        reservationStates: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        candidate.task,
        candidate.worktree,
        noopAdmissionHooks,
      );
      assert.equal(
        (yield* admissionB.admission.processHandoff(candidate.seeded.evidence.handoffId))._tag,
        "Replayed",
      );
      yield* admissionB.admission.recover;
      yield* admissionB.admission.recover;
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, candidate.seeded.evidence.handoffId),
        committed,
      );
      assert.equal((yield* Ref.get(finalizerB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(finalizerB.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(admissionB.reservationPublished)).length, 0);
    }),
  ),
);

it.effect("startup-style recovery admits a persisted planning success on a fresh layer", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        "admission-startup-recovery",
      );
      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        candidate.task,
        candidate.worktree,
        noopAdmissionHooks,
      );
      yield* admissionB.admission.recover;
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, candidate.seeded.evidence.handoffId),
        {
          stageEvents: 1,
          stageStates: 1,
          leaseEvents: 1,
          leaseStates: 1,
          reservationEvents: 1,
          reservationStates: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        },
      );
      assert.equal((yield* Ref.get(finalizerB.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(finalizerB.leasePublished)).length, 1);
      assert.equal((yield* Ref.get(admissionB.reservationPublished)).length, 1);
    }),
  ),
);

it.live("two fresh WAL admissions converge on one commit and one accepted replay", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const arrivedA = yield* Deferred.make<void>();
      const arrivedB = yield* Deferred.make<void>();
      const releaseA = yield* Deferred.make<void>();
      const releaseB = yield* Deferred.make<void>();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        "implementation-admission-race",
        {
          ...noopAdmissionHooks,
          afterAuthoritativeRead: () =>
            Deferred.succeed(arrivedA, undefined).pipe(Effect.andThen(Deferred.await(releaseA))),
        },
      );
      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        candidate.task,
        candidate.worktree,
        {
          ...noopAdmissionHooks,
          afterAuthoritativeRead: () =>
            Deferred.succeed(arrivedB, undefined).pipe(Effect.andThen(Deferred.await(releaseB))),
        },
      );

      const fiberA = yield* candidate.admissionHarness.admission
        .processHandoff(candidate.seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      const fiberB = yield* admissionB.admission
        .processHandoff(candidate.seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      yield* Effect.all([Deferred.await(arrivedA), Deferred.await(arrivedB)], {
        discard: true,
      }).pipe(Effect.timeout(barrierTimeout));
      yield* Deferred.succeed(releaseA, undefined);
      const resultA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
      yield* Deferred.succeed(releaseB, undefined);
      const resultB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
      assert.equal(resultA._tag, "Success");
      assert.equal(resultB._tag, "Success");
      if (resultA._tag === "Success" && resultB._tag === "Success") {
        assert.deepStrictEqual([resultA.success._tag, resultB.success._tag].sort(), [
          "Admitted",
          "Replayed",
        ]);
      }
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlA, candidate.seeded.evidence.handoffId),
        {
          stageEvents: 1,
          stageStates: 1,
          leaseEvents: 1,
          leaseStates: 1,
          reservationEvents: 1,
          reservationStates: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        },
      );
      assert.equal(
        (yield* Ref.get(finalizerA.stagePublished)).length +
          (yield* Ref.get(finalizerB.stagePublished)).length,
        1,
      );
      assert.equal(
        (yield* Ref.get(finalizerA.leasePublished)).length +
          (yield* Ref.get(finalizerB.leasePublished)).length,
        1,
      );
      assert.equal(
        (yield* Ref.get(candidate.admissionHarness.reservationPublished)).length +
          (yield* Ref.get(admissionB.reservationPublished)).length,
        1,
      );
    }),
  ),
);

it.effect(
  "recovery isolates corrupt admission evidence and admits the later healthy candidate",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = [
          yield* prepareImplementationAdmissionCandidate(
            database,
            finalizerA,
            "admission-recovery-isolation-a",
          ),
          yield* prepareImplementationAdmissionCandidate(
            database,
            finalizerA,
            "admission-recovery-isolation-b",
          ),
        ].toSorted((left, right) =>
          left.seeded.evidence.handoffId.localeCompare(right.seeded.evidence.handoffId),
        );
        const invalid = candidates[0]!;
        const healthy = candidates[1]!;
        yield* Effect.sync(() => {
          const native = new NodeSqlite.DatabaseSync(database.filename);
          try {
            native.exec("DROP TRIGGER agent_control_initial_planning_result_evidence_no_update");
            native
              .prepare(
                "UPDATE agent_control_initial_planning_result_evidence SET proposed_plan_digest = ? WHERE handoff_id = ?",
              )
              .run("f".repeat(64), invalid.seeded.evidence.handoffId);
          } finally {
            native.close();
          }
        });
        const taskMap = new Map(
          candidates.map((candidate) => [
            `${candidate.task.source.projectId}:${candidate.task.taskId}`,
            candidate.task,
          ]),
        );
        const worktreeMap = new Map(
          candidates.map((candidate) => [
            `${candidate.worktree.projectId}:${candidate.worktree.reservationId}`,
            candidate.worktree,
          ]),
        );
        const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
        const admissionB = yield* buildAdmission(
          database.sqlB,
          database.scopeB,
          finalizerB,
          taskMap,
          worktreeMap,
          noopAdmissionHooks,
        );
        yield* admissionB.admission.recover;
        const admitted = yield* database.sqlB<{ readonly handoffId: string }>`
        SELECT handoff_id AS "handoffId"
        FROM agent_control_implementation_admission_evidence
      `;
        assert.deepStrictEqual(admitted, [{ handoffId: healthy.seeded.evidence.handoffId }]);
        assert.deepStrictEqual(
          yield* implementationAdmissionCounts(database.sqlB, healthy.seeded.evidence.handoffId),
          {
            stageEvents: 1,
            stageStates: 1,
            leaseEvents: 1,
            leaseStates: 1,
            reservationEvents: 1,
            reservationStates: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
          },
        );
        assert.equal((yield* Ref.get(finalizerB.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(finalizerB.leasePublished)).length, 1);
        assert.equal((yield* Ref.get(admissionB.reservationPublished)).length, 1);
      }),
    ),
);

it.effect("recovery preserves an admission defect and stops before the later candidate", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidates = [
        yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerA,
          "admission-recovery-defect-a",
        ),
        yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerA,
          "admission-recovery-defect-b",
        ),
      ].toSorted((left, right) =>
        left.seeded.evidence.handoffId.localeCompare(right.seeded.evidence.handoffId),
      );
      const firstHandoffId = candidates[0]!.seeded.evidence.handoffId;
      const taskMap = new Map(
        candidates.map((candidate) => [
          `${candidate.task.source.projectId}:${candidate.task.taskId}`,
          candidate.task,
        ]),
      );
      const worktreeMap = new Map(
        candidates.map((candidate) => [
          `${candidate.worktree.projectId}:${candidate.worktree.reservationId}`,
          candidate.worktree,
        ]),
      );
      const defect = new Error("implementation-admission-recovery-defect");
      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        taskMap,
        worktreeMap,
        {
          ...noopAdmissionHooks,
          afterAuthoritativeRead: (observation) =>
            observation.handoffId === firstHandoffId ? Effect.die(defect) : Effect.void,
        },
      );
      const exit = yield* Effect.exit(admissionB.admission.recover);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.include(Cause.pretty(exit.cause), "implementation-admission-recovery-defect");
      }
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, firstHandoffId),
        noImplementationAdmission,
      );
    }),
  ),
);

it.effect("a real admission recovery fiber interrupt preserves its cause and stops globally", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidates = [
        yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerA,
          "admission-recovery-interrupt-a",
        ),
        yield* prepareImplementationAdmissionCandidate(
          database,
          finalizerA,
          "admission-recovery-interrupt-b",
        ),
      ].toSorted((left, right) =>
        left.seeded.evidence.handoffId.localeCompare(right.seeded.evidence.handoffId),
      );
      const firstHandoffId = candidates[0]!.seeded.evidence.handoffId;
      const arrived = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const taskMap = new Map(
        candidates.map((candidate) => [
          `${candidate.task.source.projectId}:${candidate.task.taskId}`,
          candidate.task,
        ]),
      );
      const worktreeMap = new Map(
        candidates.map((candidate) => [
          `${candidate.worktree.projectId}:${candidate.worktree.reservationId}`,
          candidate.worktree,
        ]),
      );
      const finalizerB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        taskMap,
        worktreeMap,
        {
          ...noopAdmissionHooks,
          afterAuthoritativeRead: (observation) =>
            observation.handoffId === firstHandoffId
              ? Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        },
      );
      const fiber = yield* Effect.forkChild(admissionB.admission.recover);
      yield* Deferred.await(arrived).pipe(Effect.timeout(barrierTimeout));
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(interrupted));
      if (Exit.isFailure(interrupted)) assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, firstHandoffId),
        noImplementationAdmission,
      );
    }),
  ),
);

it.effect.each<{ readonly stale: "task" | "worktree" | "runtime-holder" }>([
  { stale: "task" },
  { stale: "worktree" },
  { stale: "runtime-holder" },
])("a stale $stale preflight cannot create any admission companion", ({ stale }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        `admission-stale-${stale}`,
      );
      const finalizerB = yield* buildFinalizer(
        database.sqlB,
        database.scopeB,
        noopHooks,
        stale === "runtime-holder" ? "foreign-runtime-holder" : "runtime-holder",
      );
      const task =
        stale === "task"
          ? ({
              ...candidate.task,
              revision: candidate.task.revision + 1,
            } satisfies AgentControlTaskState)
          : candidate.task;
      const worktree =
        stale === "worktree"
          ? ({
              ...candidate.worktree,
              revision: candidate.worktree.revision + 1,
            } satisfies AgentControlWorktreeReservationState)
          : candidate.worktree;
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        task,
        worktree,
        noopAdmissionHooks,
      );
      const exit = yield* Effect.exit(
        admissionB.admission.processHandoff(candidate.seeded.evidence.handoffId),
      );
      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(
        yield* implementationAdmissionCounts(database.sqlB, candidate.seeded.evidence.handoffId),
        noImplementationAdmission,
      );
    }),
  ),
);

it.effect("accepted replay precedes stale task, worktree, and runtime preflight", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const finalizerA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const candidate = yield* prepareImplementationAdmissionCandidate(
        database,
        finalizerA,
        "admission-replay-first",
      );
      assert.equal(
        (yield* candidate.admissionHarness.admission.processHandoff(
          candidate.seeded.evidence.handoffId,
        ))._tag,
        "Admitted",
      );
      const finalizerB = yield* buildFinalizer(
        database.sqlB,
        database.scopeB,
        noopHooks,
        "foreign-runtime-holder",
      );
      const staleTask = {
        ...candidate.task,
        revision: candidate.task.revision + 1,
      } satisfies AgentControlTaskState;
      const staleWorktree = {
        ...candidate.worktree,
        revision: candidate.worktree.revision + 1,
      } satisfies AgentControlWorktreeReservationState;
      const admissionB = yield* buildAdmission(
        database.sqlB,
        database.scopeB,
        finalizerB,
        staleTask,
        staleWorktree,
        noopAdmissionHooks,
      );
      assert.equal(
        (yield* admissionB.admission.processHandoff(candidate.seeded.evidence.handoffId))._tag,
        "Replayed",
      );
      assert.equal((yield* Ref.get(finalizerB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(finalizerB.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(admissionB.reservationPublished)).length, 0);
    }),
  ),
);

it.effect("receipt-first replay rejects targeted accepted-evidence corruption", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "receipt-corruption");
      yield* appendProviderStart(database.sqlA, seeded, "receipt-corruption");
      yield* appendPlan(database.sqlA, seeded, "receipt-corruption");
      yield* appendProviderTerminal(database.sqlA, seeded, "receipt-corruption", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      assert.equal(
        (yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
        "Finalized",
      );
      yield* database.sqlA`
        DROP TRIGGER agent_control_initial_planning_finalization_receipts_no_update
      `;
      yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_finalization_receipts SET outcome = 'failed'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const exit = yield* Effect.exit(harnessB.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
    }),
  ),
);

it.effect("finalization evidence tables reject every update and delete", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, "immutable-evidence");
      yield* appendProviderStart(database.sqlA, seeded, "immutable-evidence");
      yield* appendPlan(database.sqlA, seeded, "immutable-evidence");
      yield* appendProviderTerminal(database.sqlA, seeded, "immutable-evidence", "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      assert.equal(
        (yield* harness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
        "Finalized",
      );
      for (const table of [
        "agent_control_initial_planning_stage_started",
        "agent_control_initial_planning_result_evidence",
        "agent_control_initial_planning_finalization_receipts",
        "agent_control_initial_planning_finalization_markers",
      ] as const) {
        for (const statement of [
          `UPDATE ${table} SET handoff_id = handoff_id`,
          `DELETE FROM ${table}`,
        ]) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(database.sqlA.withTransaction(database.sqlA.unsafe(statement))),
            ),
            statement,
          );
        }
      }
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
    }),
  ),
);

it.effect("recovery isolates an invalid first candidate and finalizes the healthy successor", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seededCandidates = [
        yield* seedPlanning(database.sqlA, harness, "recovery-isolation-a"),
        yield* seedPlanning(database.sqlA, harness, "recovery-isolation-b"),
      ];
      for (const seeded of seededCandidates) {
        yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `terminal-${seeded.evidence.handoffId}`,
          "completed",
        );
        yield* markTerminal(harness.store, seeded, "completed");
      }
      const [invalid, healthy] = seededCandidates.toSorted((left, right) =>
        left.evidence.handoffId.localeCompare(right.evidence.handoffId),
      );
      assert.isDefined(invalid);
      assert.isDefined(healthy);
      yield* database.sqlA`
        UPDATE agent_control_stage_run_states SET status = 'running'
        WHERE stage_run_id = ${invalid!.stageRunId}
      `;

      yield* harness.finalizer.recover;

      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, invalid!), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, healthy!), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 2);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 1);
    }),
  ),
);

it.effect(
  "recovery records every invalid candidate and replays healthy candidates exactly once",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = [
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-a"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-b"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-c"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-d"),
        ].toSorted((left, right) =>
          left.evidence.handoffId.localeCompare(right.evidence.handoffId),
        );
        for (const seeded of candidates) {
          yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
          yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
          yield* appendProviderTerminal(
            database.sqlA,
            seeded,
            `terminal-${seeded.evidence.handoffId}`,
            "completed",
          );
          yield* markTerminal(harness.store, seeded, "completed");
        }
        const invalid = candidates.slice(0, 2);
        const healthy = candidates.slice(2);
        for (const seeded of invalid) {
          yield* database.sqlA`
          UPDATE agent_control_stage_run_states SET status = 'running'
          WHERE stage_run_id = ${seeded!.stageRunId}
        `;
        }

        yield* harness.finalizer.recover;
        const publicationsAfterFirstRun = {
          stage: (yield* Ref.get(harness.stagePublished)).length,
          lease: (yield* Ref.get(harness.leasePublished)).length,
        };
        assert.deepStrictEqual(publicationsAfterFirstRun, { stage: 4, lease: 2 });
        for (const seeded of invalid) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(harness.finalizer.processHandoff(seeded!.evidence.handoffId)),
            ),
          );
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded!), {
            stageEvents: 1,
            leaseEvents: 1,
            started: 0,
            evidence: 0,
            receipts: 0,
            markers: 0,
          });
        }
        for (const seeded of healthy) {
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded!), {
            stageEvents: 3,
            leaseEvents: 2,
            started: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
          });
        }

        yield* harness.finalizer.recover;
        assert.deepStrictEqual(
          {
            stage: (yield* Ref.get(harness.stagePublished)).length,
            lease: (yield* Ref.get(harness.leasePublished)).length,
          },
          publicationsAfterFirstRun,
        );
      }),
    ),
);

it.effect("recovery preserves candidate defects and stops before later candidates", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const seededById = new Map<string, SeededPlanning>();
      const defect = new Error("candidate-defect");
      let firstHandoffId = "";
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: (observation) =>
          observation.handoffId === firstHandoffId ? Effect.die(defect) : Effect.void,
      });
      for (const suffix of ["recovery-defect-a", "recovery-defect-b"] as const) {
        const seeded = yield* seedPlanning(database.sqlA, harness, suffix);
        seededById.set(seeded.evidence.handoffId, seeded);
        yield* appendProviderStart(database.sqlA, seeded, `start-${suffix}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${suffix}`);
        yield* appendProviderTerminal(database.sqlA, seeded, `terminal-${suffix}`, "completed");
        yield* markTerminal(harness.store, seeded, "completed");
      }
      const handoffIds = [...seededById.keys()].toSorted();
      firstHandoffId = handoffIds[0]!;
      const exit = yield* Effect.exit(harness.finalizer.recover);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), "candidate-defect");
      for (const handoffId of handoffIds) {
        assert.deepStrictEqual(
          yield* finalizationCounts(database.sqlA, seededById.get(handoffId)!),
          {
            stageEvents: 1,
            leaseEvents: 1,
            started: 0,
            evidence: 0,
            receipts: 0,
            markers: 0,
          },
        );
      }
    }),
  ),
);

it.effect("a real recovery fiber interrupt is preserved without trailing candidate work", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const arrived = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let firstHandoffId = "";
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: (observation) =>
          observation.handoffId === firstHandoffId
            ? Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
      });
      const candidates = [
        yield* seedPlanning(database.sqlA, harness, "recovery-interrupt-a"),
        yield* seedPlanning(database.sqlA, harness, "recovery-interrupt-b"),
      ].toSorted((left, right) => left.evidence.handoffId.localeCompare(right.evidence.handoffId));
      firstHandoffId = candidates[0]!.evidence.handoffId;
      for (const seeded of candidates) {
        yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `terminal-${seeded.evidence.handoffId}`,
          "completed",
        );
        yield* markTerminal(harness.store, seeded, "completed");
      }

      const fiber = yield* Effect.forkChild(harness.finalizer.recover);
      yield* Deferred.await(arrived).pipe(Effect.timeout(barrierTimeout));
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(interrupted));
      if (Exit.isFailure(interrupted)) assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
      for (const seeded of candidates) {
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
          stageEvents: 1,
          leaseEvents: 1,
          started: 0,
          evidence: 0,
          receipts: 0,
          markers: 0,
        });
      }
    }),
  ),
);

it.effect.each<{
  readonly caseName:
    | "missing"
    | "empty"
    | "multiple"
    | "conflicting"
    | "projection-mismatch"
    | "damaged";
}>([
  { caseName: "missing" },
  { caseName: "empty" },
  { caseName: "multiple" },
  { caseName: "conflicting" },
  { caseName: "projection-mismatch" },
  { caseName: "damaged" },
])("$caseName plan evidence cannot terminalize success", ({ caseName }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `plan-${caseName}`);
      yield* appendProviderStart(database.sqlA, seeded, `plan-${caseName}`);
      if (caseName === "empty") {
        yield* appendOrchestration(database.sqlA, {
          suffix: "empty-plan",
          threadId: seeded.evidence.threadId,
          type: "thread.proposed-plan-upserted",
          occurredAt: providerAcceptedAt,
          payload: {
            proposedPlan: {
              id: seeded.planId,
              turnId: seeded.providerTurnId,
              planMarkdown: "",
              implementedAt: null,
              implementationThreadId: null,
              createdAt: providerAcceptedAt,
              updatedAt: providerAcceptedAt,
            },
            threadId: seeded.evidence.threadId,
          },
        });
      } else if (caseName === "multiple") {
        yield* appendPlan(database.sqlA, seeded, "multiple-a");
        yield* appendPlan(database.sqlA, seeded, "multiple-b");
      } else if (caseName === "conflicting") {
        yield* appendPlan(database.sqlA, seeded, "conflicting-a");
        yield* appendPlan(database.sqlA, seeded, "conflicting-b", {
          planId: `${seeded.planId}-other`,
          planMarkdown: "# A different plan",
        });
      } else if (caseName === "projection-mismatch") {
        yield* appendPlan(database.sqlA, seeded, "projection-mismatch", { project: false });
      } else if (caseName === "damaged") {
        const versionRows = yield* database.sqlA<{ readonly version: number }>`
          SELECT MAX(stream_version) AS version FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${seeded.evidence.threadId}
        `;
        const version = versionRows[0]!.version + 1;
        const eventId = EventId.make("provider-event-damaged-plan");
        const commandId = CommandId.make("provider:damaged-plan");
        yield* database.sqlA`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            ${eventId}, 'thread', ${seeded.evidence.threadId}, ${version},
            'thread.proposed-plan-upserted', ${providerAcceptedAt}, ${commandId}, NULL,
            ${commandId}, 'provider', ${"{} "}, '{}'
          )
        `;
      }
      yield* appendProviderTerminal(database.sqlA, seeded, `plan-${caseName}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      if (caseName === "missing") {
        assert.isTrue(Exit.isSuccess(exit));
        if (Exit.isSuccess(exit)) assert.equal(exit.value._tag, "Started");
      } else {
        assert.isTrue(Exit.isFailure(exit));
      }
      const counts = yield* finalizationCounts(database.sqlA, seeded);
      assert.equal(counts.stageEvents, caseName === "missing" ? 2 : 1);
      assert.equal(counts.leaseEvents, 1);
      assert.equal(counts.evidence, 0);
      assert.equal(counts.receipts, 0);
      assert.equal(counts.markers, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{
  readonly mismatch: "task" | "stageRun" | "attempt" | "lease" | "holder" | "fence";
}>([
  { mismatch: "task" },
  { mismatch: "stageRun" },
  { mismatch: "attempt" },
  { mismatch: "lease" },
  { mismatch: "holder" },
  { mismatch: "fence" },
])("foreign $mismatch identity cannot mutate either aggregate", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(
        database.sqlA,
        harness,
        `identity-${mismatch}`,
        "provider-started",
        mismatch,
      );
      yield* appendProviderStart(database.sqlA, seeded, `identity-${mismatch}`);
      yield* appendPlan(database.sqlA, seeded, `identity-${mismatch}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `identity-${mismatch}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
    }),
  ),
);

it.effect.each<{ readonly mismatch: "handoff" | "reservation" | "thread" | "delivery" }>([
  { mismatch: "handoff" },
  { mismatch: "reservation" },
  { mismatch: "thread" },
  { mismatch: "delivery" },
])("the accepted-handoff authority rejects foreign $mismatch identity", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const exit = yield* Effect.exit(
        seedPlanning(database.sqlA, harness, `handoff-${mismatch}`, "provider-started", mismatch),
      );
      assert.isTrue(Exit.isFailure(exit));
      const writes = yield* database.sqlA<{
        readonly started: number;
        readonly evidence: number;
        readonly receipts: number;
        readonly markers: number;
      }>`
        SELECT
          (SELECT COUNT(*) FROM agent_control_initial_planning_stage_started) AS started,
          (SELECT COUNT(*) FROM agent_control_initial_planning_result_evidence) AS evidence,
          (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_receipts) AS receipts,
          (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_markers) AS markers
      `;
      assert.deepStrictEqual(writes[0], { started: 0, evidence: 0, receipts: 0, markers: 0 });
    }),
  ),
);

it.effect.each<{ readonly mismatch: "provider" | "providerTurn" }>([
  { mismatch: "provider" },
  { mismatch: "providerTurn" },
])("foreign $mismatch orchestration evidence is ignored without mutation", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `orchestration-${mismatch}`);
      const foreignTurn = TurnId.make(`foreign-turn-${mismatch}`);
      yield* appendOrchestration(database.sqlA, {
        suffix: `foreign-${mismatch}`,
        threadId: seeded.evidence.threadId,
        type: "thread.session-set",
        occurredAt: providerAcceptedAt,
        payload: {
          session: {
            activeTurnId: mismatch === "providerTurn" ? foreignTurn : seeded.providerTurnId,
            lastError: null,
            providerInstanceId:
              mismatch === "provider"
                ? ProviderInstanceId.make("foreign-provider")
                : seeded.evidence.providerInstanceId,
            providerName: "codex",
            runtimeMode: seeded.evidence.runtimeMode,
            status: "running",
            threadId: seeded.evidence.threadId,
            updatedAt: providerAcceptedAt,
          },
          threadId: seeded.evidence.threadId,
        },
      });
      const result = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(result._tag, "Waiting");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
    }),
  ),
);

it.effect.each<{
  readonly corruption: "stage-projection" | "stage-history" | "lease-projection" | "lease-history";
}>([
  { corruption: "stage-projection" },
  { corruption: "stage-history" },
  { corruption: "lease-projection" },
  { corruption: "lease-history" },
])("$corruption prevents every finalization write and publication", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `corrupt-${corruption}`);
      yield* appendProviderStart(database.sqlA, seeded, `corrupt-${corruption}`);
      yield* appendPlan(database.sqlA, seeded, `corrupt-${corruption}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `corrupt-${corruption}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      if (corruption === "stage-projection") {
        yield* database.sqlA`
          UPDATE agent_control_stage_run_states SET status = 'running'
          WHERE stage_run_id = ${seeded.stageRunId}
        `;
      } else if (corruption === "stage-history") {
        yield* database.sqlA`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${seeded.stageRunId}
        `;
      } else if (corruption === "lease-projection") {
        yield* database.sqlA`
          UPDATE agent_control_stage_run_lease_states SET fence_token = 2
          WHERE lease_id = ${seeded.leaseId}
        `;
      } else {
        yield* database.sqlA`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${seeded.leaseId}
        `;
      }
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      const counts = yield* finalizationCounts(database.sqlA, seeded);
      assert.equal(counts.stageEvents, 1);
      assert.equal(counts.leaseEvents, 1);
      assert.equal(counts.started, 0);
      assert.equal(counts.evidence, 0);
      assert.equal(counts.receipts, 0);
      assert.equal(counts.markers, 0);
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase after native commit replays complete evidence on a fresh layer", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterNativeCommit: () =>
          phase === "defect" ? Effect.die(new Error("response-loss")) : Effect.interrupt,
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, `after-${phase}`);
      yield* appendProviderStart(database.sqlA, seeded, `after-${phase}`);
      yield* appendPlan(database.sqlA, seeded, `after-${phase}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `after-${phase}`, "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const lost = yield* Effect.exit(harnessA.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(lost));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessA.leasePublished)).length, 0);

      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const replayed = yield* harnessB.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(replayed._tag, "Replayed");
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
    }),
  ),
);
