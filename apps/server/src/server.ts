import { EnvironmentHttpApi, ProviderDriverKind } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as Ref from "effect/Ref";
import { FetchHttpClient, HttpRouter, HttpServer } from "effect/unstable/http";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "./background/HostPowerMonitor.ts";
import * as ServerConfig from "./config.ts";
import { nodeRuntimeRequiredError } from "./serverRuntimeGate.ts";
import {
  otlpTracesProxyRouteLayer,
  assetRouteLayer,
  attachmentUploadRouteLayer,
  serverEnvironmentHttpApiLayer,
  staticAndDevRouteLayer,
  browserApiCorsLayer,
  httpCompressionLayer,
} from "./http.ts";
import { guardHttpResponseWriteErrors } from "./httpResponseErrorGuard.ts";
import { fixPath } from "./os-jank.ts";
import { websocketRpcRouteLayer } from "./ws.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import { pullRequestHttpApiLayer } from "./pullRequest/http.ts";
import * as PullRequestProviderRegistry from "./pullRequest/PullRequestProviderRegistry.ts";
import * as PullRequestService from "./pullRequest/PullRequestService.ts";
import { layerConfig as SqlitePersistenceLayerLive } from "./persistence/Layers/Sqlite.ts";
import { AgentControlProjectPolicyRepositoryLive } from "./persistence/Layers/AgentControlProjectPolicies.ts";
import { AgentControlPolicyServiceLive } from "./agentControl/AgentControlPolicyService.ts";
import {
  AgentControlControlledThreadReservationLayerLive,
  AgentControlRunOnceControllerLayerLive,
  AgentControlRuntimeLayerLive,
  AgentControlTaskConsumerGuardLayerLive,
  AgentControlWorktreeControllerLayerLive,
} from "./agentControl/runtimeLayer.ts";
import { AgentControlControlledThreadActivationLive } from "./agentControl/controlledThreadReservation/Layers/AgentControlControlledThreadActivation.ts";
import { AgentControlControlledThreadActivationHooksNoop } from "./agentControl/controlledThreadReservation/Services/AgentControlControlledThreadActivationHooks.ts";
import { AgentControlControlledThreadMaterializationCoordinatorLive } from "./agentControl/controlledThreadReservation/Layers/AgentControlControlledThreadMaterializationCoordinator.ts";
import { AgentControlControlledThreadMaterializationCoordinatorHooksNoop } from "./agentControl/controlledThreadReservation/Services/AgentControlControlledThreadMaterializationCoordinatorHooks.ts";
import { AgentControlInitialPlanningConsumerLive } from "./agentControl/initialPlanning/Layers/AgentControlInitialPlanningConsumer.ts";
import { AgentControlInitialPlanningFinalizerLive } from "./agentControl/initialPlanning/Layers/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlImplementationAdmissionLive } from "./agentControl/implementationAdmission/Layers/AgentControlImplementationAdmission.ts";
import { AgentControlImplementationHandoffStoreLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationStageStarterLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationStageStarter.ts";
import { AgentControlImplementationStageFinalizerLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationStageFinalizer.ts";
import { AgentControlImplementationTurnConsumerLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationTurnConsumer.ts";
import { AgentControlImplementationTurnCoordinatorLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlImplementationTurnWakeupLive } from "./agentControl/implementationTurn/Layers/AgentControlImplementationTurnWakeup.ts";
import { AgentControlImplementationTurnCoordinatorHooksNoop } from "./agentControl/implementationTurn/Services/AgentControlImplementationTurnCoordinatorHooks.ts";
import { AgentControlVerificationAdmissionLive } from "./agentControl/verificationAdmission/Layers/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationHandoffStoreLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationStageStarterLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationEvaluatorLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationStageFinalizerLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationTurnConsumerLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationTurnConsumer.ts";
import { AgentControlVerificationTurnCoordinatorLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationTurnCoordinator.ts";
import { AgentControlVerificationTurnWakeupLive } from "./agentControl/verificationTurn/Layers/AgentControlVerificationTurnWakeup.ts";
import { AgentControlVerificationTurnCoordinatorHooksNoop } from "./agentControl/verificationTurn/Services/AgentControlVerificationTurnCoordinatorHooks.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "./agentControl/initialPlanning/Layers/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeupLive } from "./agentControl/initialPlanning/Layers/AgentControlInitialPlanningWakeup.ts";
import { ProviderAdmissionStoreLive } from "./agentControl/providerAdmission/Layers/ProviderAdmissionStore.ts";
import { ProviderAdmissionGuardLive } from "./agentControl/providerAdmission/Layers/ProviderAdmissionGuard.ts";
import { ProviderAdmissionRuntimeLive } from "./agentControl/providerAdmission/Layers/ProviderAdmissionRuntime.ts";
import { ProviderAdmissionReleaseAuthorityLive } from "./agentControl/providerAdmission/Layers/ProviderAdmissionReleaseAuthority.ts";
import { layer as AgentControlGithubObserveReactorLive } from "./agentControl/github/Layers/AgentControlGithubObserveReactor.ts";
import { layer as AgentControlTaskIntakeReactorLive } from "./agentControl/task/Layers/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskVerificationFinalizerLive } from "./agentControl/task/Layers/AgentControlTaskVerificationFinalizer.ts";
import { layer as AgentControlArmedSchedulerLive } from "./agentControl/armed/Layers/AgentControlArmedScheduler.ts";
import { layer as AgentControlReactorLive } from "./agentControl/Layers/AgentControlReactor.ts";
import { AgentControlReactor } from "./agentControl/Services/AgentControlReactor.ts";
import * as ServerLifecycleEvents from "./serverLifecycleEvents.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import { ProviderSessionDirectoryLive } from "./provider/Layers/ProviderSessionDirectory.ts";
import { ClaudeSessionStoreLive } from "./provider/Layers/ClaudeSessionStore.ts";
import * as ProviderSessionRuntime from "./persistence/ProviderSessionRuntime.ts";
import { ProviderAdapterRegistryLive } from "./provider/Layers/ProviderAdapterRegistry.ts";
import * as ModelManifest from "./provider/ModelManifest.ts";
import * as CodexResetCredit from "./provider/Layers/codexResetCredit.ts";
import * as ProviderEventLoggers from "./provider/Layers/ProviderEventLoggers.ts";
import { ProviderServiceLive } from "./provider/Layers/ProviderService.ts";
import { ProviderUsageLive } from "./provider/Layers/ProviderUsage.ts";
import { ProviderThreadContinuationSyncLive } from "./provider/Layers/ProviderThreadContinuationSync.ts";
import { ProviderRegistryRebuildBarrierLive } from "./provider/Layers/ProviderRegistryRebuildBarrier.ts";
import { ProviderThreadOperationLockLive } from "./provider/Layers/ProviderThreadOperationLock.ts";
import { ProviderAuthServiceLive } from "./provider/Layers/ProviderAuthService.ts";
import { AntigravityInstallation } from "./provider/AntigravityInstallation.ts";
import { ProviderInstanceRegistry } from "./provider/Services/ProviderInstanceRegistry.ts";
import { ProviderRegistry } from "./provider/Services/ProviderRegistry.ts";
import { ProviderSessionReaperLive } from "./provider/Layers/ProviderSessionReaper.ts";
import { ProviderUsageLimitsIngestionLive } from "./provider/Layers/ProviderUsageLimitsIngestion.ts";
import * as OpenCodeRuntime from "./provider/opencodeRuntime.ts";
import * as CheckpointDiffQuery from "./checkpointing/CheckpointDiffQuery.ts";
import * as CheckpointStore from "./checkpointing/CheckpointStore.ts";
import * as AzureDevOpsCli from "./sourceControl/AzureDevOpsCli.ts";
import * as BitbucketApi from "./sourceControl/BitbucketApi.ts";
import * as GitHubCli from "./sourceControl/GitHubCli.ts";
import * as GitLabCli from "./sourceControl/GitLabCli.ts";
import * as TextGeneration from "./textGeneration/TextGeneration.ts";
import { ProviderInstanceRegistryHydrationLive } from "./provider/Layers/ProviderInstanceRegistryHydration.ts";
import * as TerminalManager from "./terminal/Manager.ts";
import * as McpHttpServer from "./mcp/McpHttpServer.ts";
import * as McpSessionRegistry from "./mcp/McpSessionRegistry.ts";
import * as PreviewAutomationBroker from "./mcp/PreviewAutomationBroker.ts";
import * as PreviewManager from "./preview/Manager.ts";
import * as PortScanner from "./preview/PortScanner.ts";
import * as ProcessRunner from "./processRunner.ts";
import * as GitManager from "./git/GitManager.ts";
import * as EnvironmentTheme from "./environmentTheme.ts";
import * as Keybindings from "./keybindings.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import { OrchestrationReactorLive } from "./orchestration/Layers/OrchestrationReactor.ts";
import { RuntimeReceiptBusLive } from "./orchestration/Layers/RuntimeReceiptBus.ts";
import { ProviderRuntimeIngestionLive } from "./orchestration/Layers/ProviderRuntimeIngestion.ts";
import { ProviderCommandReactorLive } from "./orchestration/Layers/ProviderCommandReactor.ts";
import { CheckpointReactorLive } from "./orchestration/Layers/CheckpointReactor.ts";
import { ThreadDeletionReactorLive } from "./orchestration/Layers/ThreadDeletionReactor.ts";
import { ProviderTurnRequestExecutorLive } from "./orchestration/Layers/ProviderTurnRequestExecutor.ts";
import { ProjectionTurnRepositoryLive } from "./persistence/Layers/ProjectionTurns.ts";
import * as ThreadSettlementReactor from "./orchestration/ThreadSettlementReactor.ts";
import * as ThreadPullRequestReactor from "./orchestration/ThreadPullRequestReactor.ts";
import * as AgentAwarenessRelay from "./relay/AgentAwarenessRelay.ts";
import { hasCloudPublicConfig } from "./cloud/publicConfig.ts";
import { ProviderRegistryLive } from "./provider/Layers/ProviderRegistry.ts";
import * as ServerSettings from "./serverSettings.ts";
import * as NativeAppIconResolver from "./assets/NativeAppIconResolver.ts";
import * as ProjectFaviconResolver from "./project/ProjectFaviconResolver.ts";
import * as T3ProjectFileLoader from "./project/T3ProjectFileLoader.ts";
import * as RepositoryIdentityResolver from "./project/RepositoryIdentityResolver.ts";
import * as WorkspaceEntries from "./workspace/WorkspaceEntries.ts";
import * as WorkspaceFileSystem from "./workspace/WorkspaceFileSystem.ts";
import * as WorkspacePaths from "./workspace/WorkspacePaths.ts";
import * as GitVcsDriver from "./vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "./vcs/VcsDriverRegistry.ts";
import * as VcsProjectConfig from "./vcs/VcsProjectConfig.ts";
import * as VcsProcess from "./vcs/VcsProcess.ts";
import * as VcsProvisioningService from "./vcs/VcsProvisioningService.ts";
import * as VcsStatusBroadcaster from "./vcs/VcsStatusBroadcaster.ts";
import * as GitWorkflowService from "./git/GitWorkflowService.ts";
import * as ReviewService from "./review/ReviewService.ts";
import * as SourceControlProviderRegistry from "./sourceControl/SourceControlProviderRegistry.ts";
import * as SourceControlRateLimit from "./sourceControl/SourceControlRateLimit.ts";
import * as SourceControlRepositoryService from "./sourceControl/SourceControlRepositoryService.ts";
import * as ProjectSetupScriptRunner from "./project/ProjectSetupScriptRunner.ts";
import { ObservabilityLive } from "./observability/Layers/Observability.ts";
import * as ServerEnvironment from "./environment/ServerEnvironment.ts";
import * as RemoteOpenTargets from "./environment/RemoteOpenTargets.ts";
import { authHttpApiLayer, environmentAuthenticatedAuthLayer } from "./auth/http.ts";
import * as ServerSecretStore from "./auth/ServerSecretStore.ts";
import * as EnvironmentAuth from "./auth/EnvironmentAuth.ts";
import {
  connectHttpApiLayer,
  pendingServiceUpdateExists,
  reconcileDesiredCloudLink,
  releaseManagedTunnelOnShutdown,
} from "./cloud/http.ts";
import { serverRelayBrokerTracingLayer } from "./cloud/relayTracing.ts";
import { shouldRetryCloudLink } from "./cloud/relayResponse.ts";
import * as CloudManagedEndpointRuntime from "./cloud/ManagedEndpointRuntime.ts";
import * as CloudCliTokenManager from "./cloud/CliTokenManager.ts";
import * as CloudCliState from "./cloud/CliState.ts";
import * as ServerSelfUpdate from "./cloud/selfUpdate.ts";
import * as DesktopAppUpdate from "./desktopUpdate/DesktopAppUpdate.ts";
import * as ServiceLauncherClient from "./cloud/serviceLauncherClient.ts";
import * as ProcessDiagnostics from "./diagnostics/ProcessDiagnostics.ts";
import * as HostResources from "./resourceTelemetry/HostResources.ts";
import * as ProcessResourceMonitor from "./diagnostics/ProcessResourceMonitor.ts";
import * as TraceDiagnostics from "./diagnostics/TraceDiagnostics.ts";
import * as DesktopTelemetryReceiver from "./resourceTelemetry/DesktopTelemetryReceiver.ts";
import * as NativeTelemetryClient from "./resourceTelemetry/NativeTelemetryClient.ts";
import * as ResourceAttribution from "./resourceTelemetry/ResourceAttribution.ts";
import * as ResourceMonitorBinary from "./resourceTelemetry/ResourceMonitorBinary.ts";
import * as ResourceTelemetry from "./resourceTelemetry/ResourceTelemetry.ts";
import * as UsageLimitSources from "./usage/UsageLimitSources.ts";
import * as UsageService from "./usage/UsageService.ts";
import { OrchestrationLayerLive } from "./orchestration/runtimeLayer.ts";
import {
  clearPersistedServerRuntimeState,
  makePersistedServerRuntimeState,
  persistServerRuntimeState,
} from "./serverRuntimeState.ts";
import { orchestrationHttpApiLayer } from "./orchestration/http.ts";
import * as NetService from "@t3tools/shared/Net";
import * as RelayClient from "@t3tools/shared/relayClient";
import { disableTailscaleServe, ensureTailscaleServe } from "@t3tools/tailscale";
import { forkParked, ServerActivation } from "./serverActivation.ts";

// MCP handoff thread IDs include escaped provenance and can exceed find-my-way's
// 100-character default for one path segment.
export const HTTP_ROUTER_CONFIG = {
  maxParamLength: 512,
} as const;

// Effect's default preemptive shutdown waits 20s before finalizing request scopes.
// T3's primary transport is long-lived WebSocket RPC, whose Effect scope finalizer
// already closes the websocket gracefully. Do not add an artificial drain before
// those finalizers get a chance to run.
const HTTP_PREEMPTIVE_SHUTDOWN_GRACE_MS = 0;
const ResourceAttributionLayerLive = ResourceAttribution.layer;
const ApplicationObservabilityLive = ObservabilityLive.pipe(
  Layer.provideMerge(ResourceAttributionLayerLive),
);

const PtyAdapterLive = Layer.unwrap(
  Effect.gen(function* () {
    if (typeof Bun !== "undefined") {
      const BunPtyAdapter = yield* Effect.promise(() => import("./terminal/BunPtyAdapter.ts"));
      return BunPtyAdapter.layer;
    } else {
      const NodePtyAdapter = yield* Effect.promise(() => import("./terminal/NodePtyAdapter.ts"));
      return NodePtyAdapter.layer;
    }
  }),
);

const ServerSettingsLayerLive = ServerSettings.layer.pipe(
  Layer.provide(ServerSecretStore.layer),
  Layer.provideMerge(SqlitePersistenceLayerLive),
);

const NativeTelemetryLayerLive = NativeTelemetryClient.layer.pipe(
  Layer.provide(ResourceMonitorBinary.layer),
);
const DesktopTelemetryReceiverLayerLive = DesktopTelemetryReceiver.layer.pipe(
  Layer.provideMerge(ServerSettingsLayerLive),
);

const ResourceTelemetryLayerLive = ResourceTelemetry.layer.pipe(
  Layer.provideMerge(NativeTelemetryLayerLive),
  Layer.provideMerge(DesktopTelemetryReceiverLayerLive),
);

const HostPowerMonitorLayerLive = HostPowerMonitor.layer.pipe(
  Layer.provide(DesktopTelemetryReceiverLayerLive),
);

// Reuses DesktopTelemetryReceiverLayerLive: a fresh receiver layer here
// would open a second reader on the desktop telemetry fd.
const DesktopAppUpdateLayerLive = DesktopAppUpdate.layer.pipe(
  Layer.provide(DesktopTelemetryReceiverLayerLive),
);

const BackgroundLayerLive = BackgroundPolicy.layer.pipe(
  Layer.provide(HostPowerMonitorLayerLive),
  Layer.provideMerge(ServerSettingsLayerLive),
);

const ResourceDiagnosticsLayerLive = Layer.mergeAll(
  HostResources.layer,
  ResourceTelemetryLayerLive,
  ProcessDiagnostics.layer.pipe(Layer.provide(ResourceTelemetryLayerLive)),
  ProcessResourceMonitor.layer.pipe(Layer.provide(ResourceTelemetryLayerLive)),
);

const RelayClientLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return RelayClient.layerCloudflared({ baseDir: config.baseDir });
  }),
);

const HttpServerLive = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    if (typeof Bun !== "undefined") {
      const BunHttpServer = yield* Effect.promise(
        () => import("@effect/platform-bun/BunHttpServer"),
      );
      return BunHttpServer.layer({
        port: config.port,
        hostname: config.host ?? "127.0.0.1",
        gracefulShutdownTimeout: HTTP_PREEMPTIVE_SHUTDOWN_GRACE_MS,
        websocket: {
          // Negotiate permessage-deflate with clients that offer it; clients
          // that don't still get uncompressed frames on their connection. A
          // dedicated compressor keeps a per-connection sliding window
          // (context takeover) so the compression dictionary is shared across
          // server-to-client frames. Decompression uses the shared
          // decompressor: uWebSockets' dedicated decompressor path can abort
          // connections (close 1006) on valid DEFLATE input — see
          // https://github.com/uNetworking/uWebSockets.js/issues/633.
          perMessageDeflate: {
            compress: "dedicated",
            decompress: "shared",
          },
        },
      });
    } else {
      const [NodeHttpServer, NodeHttp] = yield* Effect.all([
        Effect.promise(() => import("@effect/platform-node/NodeHttpServer")),
        Effect.promise(() => import("node:http")),
      ]);
      return NodeHttpServer.layer(() => guardHttpResponseWriteErrors(NodeHttp.createServer()), {
        host: config.host ?? "127.0.0.1",
        port: config.port,
        gracefulShutdownTimeout: HTTP_PREEMPTIVE_SHUTDOWN_GRACE_MS,
        // Negotiate permessage-deflate with clients that offer it; clients
        // that don't still get uncompressed frames on their connection.
        // Context takeover stays enabled (ws default) so the compression
        // window is shared across frames — that also makes small frames cheap
        // to compress, so no size threshold is set (ws only honors
        // `threshold` when context takeover is disabled).
        websocket: { perMessageDeflate: true },
      });
    }
  }),
);

const PlatformServicesLive = Layer.unwrap(
  Effect.gen(function* () {
    if (typeof Bun !== "undefined") {
      const { layer } = yield* Effect.promise(() => import("@effect/platform-bun/BunServices"));
      return layer;
    } else {
      const { layer } = yield* Effect.promise(() => import("@effect/platform-node/NodeServices"));
      return layer;
    }
  }),
);

const InitialPlanningWakeupLayerLive = AgentControlInitialPlanningWakeupLive;
const InitialPlanningConsumerLayerLive = AgentControlInitialPlanningConsumerLive.pipe(
  Layer.provideMerge(AgentControlInitialPlanningHandoffStoreLive),
  Layer.provideMerge(ProviderTurnRequestExecutorLive),
  Layer.provideMerge(ProjectionTurnRepositoryLive),
  Layer.provideMerge(ProviderSessionRuntime.layer),
  Layer.provideMerge(InitialPlanningWakeupLayerLive),
);
const ImplementationTurnWakeupLayerLive = AgentControlImplementationTurnWakeupLive;
const ImplementationHandoffStoreLayerLive = AgentControlImplementationHandoffStoreLive;
const ImplementationTurnConsumerLayerLive = AgentControlImplementationTurnConsumerLive.pipe(
  Layer.provideMerge(ImplementationHandoffStoreLayerLive),
  Layer.provideMerge(ProviderTurnRequestExecutorLive),
  Layer.provideMerge(ProjectionTurnRepositoryLive),
  Layer.provideMerge(ImplementationTurnWakeupLayerLive),
);
const VerificationTurnWakeupLayerLive = AgentControlVerificationTurnWakeupLive;
const VerificationHandoffStoreLayerLive = AgentControlVerificationHandoffStoreLive;
const VerificationTurnConsumerLayerLive = AgentControlVerificationTurnConsumerLive.pipe(
  Layer.provideMerge(VerificationHandoffStoreLayerLive),
  Layer.provideMerge(ProviderTurnRequestExecutorLive),
  Layer.provideMerge(ProjectionTurnRepositoryLive),
  Layer.provideMerge(VerificationTurnWakeupLayerLive),
);

const ReactorLayerLive = Layer.empty.pipe(
  Layer.provideMerge(OrchestrationReactorLive),
  Layer.provideMerge(ProviderRuntimeIngestionLive),
  Layer.provideMerge(ProviderCommandReactorLive),
  Layer.provideMerge(CheckpointReactorLive),
  Layer.provideMerge(ThreadDeletionReactorLive),
  Layer.provideMerge(InitialPlanningConsumerLayerLive),
  Layer.provideMerge(ImplementationTurnConsumerLayerLive),
  Layer.provideMerge(VerificationTurnConsumerLayerLive),
  Layer.provideMerge(ThreadSettlementReactor.layer),
  Layer.provideMerge(ThreadPullRequestReactor.layer),
  Layer.provideMerge(AgentAwarenessRelay.layer.pipe(Layer.provide(ServerSecretStore.layer))),
  Layer.provideMerge(RuntimeReceiptBusLive),
);

const ProviderSessionDirectoryLayerLive = ProviderSessionDirectoryLive.pipe(
  Layer.provide(ProviderSessionRuntime.layer),
);

// `ProviderAdapterRegistryLive` is now a facade that resolves kind → adapter
// by looking up the default `ProviderInstance` per driver in the instance
// registry. Adapter construction itself moved inside each driver's
// `create()`; `ProviderEventLoggers.layer` owns the shared native/canonical
// NDJSON writers and is provided at the outer runtime layer so both
// `ProviderService` and the per-instance drivers read the same logger pair.
const PersistenceLayerLive = Layer.empty.pipe(Layer.provideMerge(SqlitePersistenceLayerLive));
const ProviderAdmissionStoreLayerLive = ProviderAdmissionStoreLive.pipe(
  Layer.provide(PersistenceLayerLive),
);
const ProviderAdmissionTaskGuardLayerLive = AgentControlTaskConsumerGuardLayerLive.pipe(
  Layer.provide(PersistenceLayerLive),
);
const ProviderAdmissionGuardLayerLive = ProviderAdmissionGuardLive.pipe(
  Layer.provideMerge(ProviderAdmissionStoreLayerLive),
  Layer.provide(ProviderAdmissionTaskGuardLayerLive),
);
const ProviderLayerLive = ProviderServiceLive.pipe(
  Layer.provideMerge(ProviderAdapterRegistryLive),
  Layer.provideMerge(ProviderSessionDirectoryLayerLive),
  Layer.provide(ProviderAdmissionGuardLayerLive),
);

const ClaudeSessionStoreLayerLive = ClaudeSessionStoreLive.pipe(
  Layer.provide(PersistenceLayerLive),
);
const ProviderPersistenceLayerLive = Layer.merge(PersistenceLayerLive, ClaudeSessionStoreLayerLive);
const ProviderCoordinationLayerLive = Layer.merge(
  ProviderThreadOperationLockLive,
  ProviderRegistryRebuildBarrierLive,
);
const ProviderInstanceRegistryHydrationLayerLive = ProviderInstanceRegistryHydrationLive.pipe(
  // Driver instances are built inside the hydration layer's child scopes.
  // Provide the shared store directly to that layer so ClaudeDriver.create()
  // receives the same process-wide transcript store as the rest of the
  // provider runtime.
  Layer.provideMerge(ProviderPersistenceLayerLive),
  Layer.provideMerge(ProviderCoordinationLayerLive),
);
const ProviderInstanceRegistryWithSettingsLayerLive =
  ProviderInstanceRegistryHydrationLayerLive.pipe(Layer.provideMerge(ServerSettingsLayerLive));
const UsageLayerLive = UsageService.layer.pipe(
  Layer.provideMerge(ProviderInstanceRegistryWithSettingsLayerLive),
);

const VcsDriverRegistryLayerLive = VcsDriverRegistry.layer.pipe(
  Layer.provide(VcsProjectConfig.layer),
);

const SourceControlProviderRegistryLayerLive = SourceControlProviderRegistry.layer.pipe(
  Layer.provide(
    Layer.mergeAll(AzureDevOpsCli.layer, BitbucketApi.layer, GitHubCli.layer, GitLabCli.layer),
  ),
  Layer.provideMerge(GitVcsDriver.layer),
  Layer.provideMerge(VcsDriverRegistryLayerLive),
);

const PullRequestServiceLive = PullRequestService.layer.pipe(
  Layer.provide(PullRequestProviderRegistry.layer),
  Layer.provide(SourceControlProviderRegistryLayerLive),
  Layer.provide(SourceControlRateLimit.layer),
);

const GitManagerLayerLive = GitManager.layer.pipe(
  Layer.provideMerge(ProjectSetupScriptRunner.layer.pipe(Layer.provide(ServerSettingsLayerLive))),
  Layer.provideMerge(GitVcsDriver.layer),
  Layer.provideMerge(SourceControlProviderRegistryLayerLive),
  Layer.provideMerge(TextGeneration.layer),
);

const GitLayerLive = Layer.empty.pipe(
  Layer.provideMerge(GitManagerLayerLive),
  Layer.provideMerge(GitVcsDriver.layer),
);

const GitWorkflowLayerLive = GitWorkflowService.layer.pipe(
  Layer.provideMerge(VcsDriverRegistryLayerLive),
  Layer.provideMerge(GitLayerLive),
);

const SourceControlRepositoryServiceLayerLive = SourceControlRepositoryService.layer.pipe(
  Layer.provideMerge(GitVcsDriver.layer),
  Layer.provideMerge(SourceControlProviderRegistryLayerLive),
);

const ReviewLayerLive = ReviewService.layer.pipe(
  Layer.provideMerge(GitVcsDriver.layer),
  Layer.provideMerge(VcsDriverRegistryLayerLive),
);

const VcsLayerLive = Layer.empty.pipe(
  Layer.provideMerge(VcsProjectConfig.layer),
  Layer.provideMerge(VcsDriverRegistryLayerLive),
  Layer.provideMerge(VcsProvisioningService.layer.pipe(Layer.provide(VcsDriverRegistryLayerLive))),
  Layer.provideMerge(GitWorkflowLayerLive),
  Layer.provideMerge(ReviewLayerLive),
  Layer.provideMerge(SourceControlRepositoryServiceLayerLive),
  Layer.provideMerge(
    VcsStatusBroadcaster.layer.pipe(
      Layer.provide(GitWorkflowLayerLive),
      Layer.provide(
        VcsStatusBroadcaster.autoPullPolicyLayer.pipe(Layer.provide(ServerSettingsLayerLive)),
      ),
    ),
  ),
);

const CheckpointingLayerLive = Layer.empty.pipe(
  Layer.provideMerge(CheckpointDiffQuery.layer),
  Layer.provideMerge(CheckpointStore.layer.pipe(Layer.provide(VcsDriverRegistryLayerLive))),
);

const PortScannerLayerLive = PortScanner.layer.pipe(Layer.provide(ProcessRunner.layer));

const TerminalLayerLive = TerminalManager.layer.pipe(
  Layer.provide(PtyAdapterLive),
  Layer.provide(PortScannerLayerLive),
  Layer.provide(NativeTelemetryLayerLive),
);

const PreviewLayerLive = Layer.empty.pipe(
  Layer.provideMerge(PreviewManager.layer),
  Layer.provideMerge(PortScannerLayerLive),
);

const WorkspaceEntriesLayerLive = WorkspaceEntries.layer.pipe(Layer.provide(WorkspacePaths.layer));

const WorkspaceFileSystemLayerLive = WorkspaceFileSystem.layer.pipe(
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(WorkspaceEntriesLayerLive),
);

const WorkspaceLayerLive = Layer.mergeAll(
  WorkspacePaths.layer,
  WorkspaceEntriesLayerLive,
  WorkspaceFileSystemLayerLive,
);

const ProjectFaviconResolverLayerLive = ProjectFaviconResolver.layer.pipe(
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(T3ProjectFileLoader.layer),
);

const ServerEnvironmentLayerLive = ServerEnvironment.layer.pipe(
  Layer.provide(ServerSecretStore.layer),
);

const AuthLayerLive = EnvironmentAuth.layer.pipe(
  Layer.provideMerge(PersistenceLayerLive),
  Layer.provide(ServerEnvironmentLayerLive),
  Layer.provide(ServerSecretStore.layer),
);

const CloudManagedEndpointRuntimeLive = Layer.mergeAll(
  RelayClientLive,
  CloudManagedEndpointRuntime.layer.pipe(
    Layer.provide(ServerSecretStore.layer),
    Layer.provide(RelayClientLive),
  ),
);

const ProviderUsageLayerLive = ProviderUsageLive.pipe(Layer.provideMerge(ProviderLayerLive));
const ProviderAdmissionRuntimeLayerLive = ProviderAdmissionRuntimeLive.pipe(
  Layer.provideMerge(ProviderAdmissionStoreLayerLive),
  Layer.provideMerge(ProviderUsageLayerLive),
  Layer.provideMerge(InitialPlanningWakeupLayerLive),
  Layer.provideMerge(ImplementationTurnWakeupLayerLive),
  Layer.provide(VerificationTurnWakeupLayerLive),
);
const ProviderAdmissionReleaseAuthorityLayerLive = ProviderAdmissionReleaseAuthorityLive.pipe(
  Layer.provideMerge(ProviderAdmissionStoreLayerLive),
  Layer.provide(ProviderAdmissionRuntimeLayerLive),
);

const ProviderThreadContinuationSyncLayerLive = ProviderThreadContinuationSyncLive.pipe(
  Layer.provideMerge(ProviderAdapterRegistryLive),
  Layer.provideMerge(ProviderSessionDirectoryLayerLive),
);

const ProviderRuntimeLayerLive = ProviderSessionReaperLive.pipe(
  Layer.provideMerge(ProviderUsageLimitsIngestionLive),
  Layer.provideMerge(ProviderUsageLayerLive),
  // Subscribes to `account.rate-limits.updated` so usage bars track live
  // telemetry instead of waiting for the next status probe.
  Layer.provideMerge(OrchestrationLayerLive),
);
const ProviderRuntimeServicesLayerLive = Layer.merge(
  ProviderRuntimeLayerLive,
  ProviderThreadContinuationSyncLayerLive,
).pipe(Layer.provideMerge(ProviderCoordinationLayerLive));

const AntigravityInstallationRefreshLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const installation = yield* AntigravityInstallation;
    const instances = yield* ProviderInstanceRegistry;
    const providers = yield* ProviderRegistry;
    yield* installation.changes.pipe(
      Stream.map((state) => state.installedVersion),
      Stream.changes,
      Stream.drop(1),
      Stream.runForEach(() =>
        instances.listInstances.pipe(
          Effect.flatMap((entries) =>
            Effect.forEach(
              entries.filter(
                (instance) => instance.driverKind === ProviderDriverKind.make("antigravity"),
              ),
              (instance) => providers.refreshInstance(instance.instanceId),
              { discard: true },
            ),
          ),
        ),
      ),
      Effect.forkScoped,
    );
  }),
);

const RuntimeCoreDependenciesAdmissionLive = ReactorLayerLive.pipe(
  Layer.provideMerge(AntigravityInstallationRefreshLive),
  Layer.provideMerge(ProviderAuthServiceLive),
  // Core Services
  Layer.provideMerge(ServerSettingsLayerLive),
  Layer.provideMerge(CheckpointingLayerLive),
  Layer.provideMerge(
    Layer.mergeAll(SourceControlProviderRegistryLayerLive, PullRequestServiceLive),
  ),
  Layer.provideMerge(GitLayerLive),
  Layer.provideMerge(VcsLayerLive),
  Layer.provideMerge(ProviderRuntimeServicesLayerLive),
  Layer.provideMerge(ProviderAdmissionRuntimeLayerLive),
  Layer.provideMerge(ProviderAdmissionReleaseAuthorityLayerLive),
);
const RuntimeCoreDependenciesBaseLive = RuntimeCoreDependenciesAdmissionLive.pipe(
  Layer.provideMerge(Layer.mergeAll(TerminalLayerLive, PreviewLayerLive)),
  Layer.provideMerge(PersistenceLayerLive),
  // Both read a user-owned file out of the state directory and stream changes
  // to clients; neither depends on the other.
  Layer.provideMerge(
    Layer.mergeAll(Keybindings.layer, EnvironmentTheme.layer, UsageLimitSources.layer),
  ),
  Layer.provideMerge(ProviderRegistryLive),
  // The instance registry is the new routing keystone — text generation,
  // adapter lookup, and runtime ingestion all resolve `ProviderInstanceId`
  // through this layer. Built-in drivers come from `BUILT_IN_DRIVERS`;
  // `providerInstances` hydration merges `settings.providers.<kind>`
  // with explicit `providerInstances` entries on boot.
  Layer.provideMerge(UsageLayerLive),
).pipe(
  Layer.provideMerge(AntigravityInstallation.layer),
  // Shared native/canonical NDJSON writers used by both the per-instance
  // drivers (native stream, written from inside each `<X>Adapter`) and
  // `ProviderService` (canonical stream, written after event normalization).
  // Provided once at the runtime level so every consumer sees the same
  // logger instances.
  // `ModelManifest.layer` is the legacy-model classification data, refreshed
  // from the repo's `model-manifest.json` on `main` and applied by the
  // Codex/Claude drivers.
  Layer.provideMerge(
    Layer.mergeAll(ProviderEventLoggers.layer, ModelManifest.layer, CodexResetCredit.layer),
  ),
  // `OpenCodeDriver.create()` yields `OpenCodeRuntime`; previously the old
  // `ProviderRegistryLive` pulled `OpenCodeRuntimeLive` in for itself, but
  // the rewritten registry reads snapshots off the instance registry and
  // no longer transitively provides it. Exposing it at the runtime level
  // keeps a single Live for all opencode consumers.
  Layer.provideMerge(OpenCodeRuntime.OpenCodeRuntimeLive),
  Layer.provideMerge(WorkspaceLayerLive),
  Layer.provideMerge(Layer.mergeAll(NativeAppIconResolver.layer, ProjectFaviconResolverLayerLive)),
  Layer.provideMerge(RepositoryIdentityResolver.layer),
  Layer.provideMerge(ServerEnvironmentLayerLive),
  Layer.provideMerge(AuthLayerLive),
  Layer.provideMerge(ServerSecretStore.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      CloudCliTokenManager.layer.pipe(
        Layer.provide(ServerSecretStore.layer),
        Layer.provide(ExternalLauncher.layer),
      ),
      CloudManagedEndpointRuntimeLive,
    ),
  ),
);

const AgentControlPolicyLayerLive = AgentControlPolicyServiceLive.pipe(
  Layer.provideMerge(AgentControlProjectPolicyRepositoryLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlRuntimeBaseServicesLayerLive = AgentControlRuntimeLayerLive.pipe(
  Layer.provideMerge(PersistenceLayerLive),
  Layer.provideMerge(GitHubCli.layer),
  Layer.provideMerge(RepositoryIdentityResolver.layer),
);

const AgentControlControlledThreadReservationServiceLayerLive =
  AgentControlControlledThreadReservationLayerLive.pipe(
    Layer.provideMerge(AgentControlRuntimeBaseServicesLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlWorktreeControllerServiceLayerLive = AgentControlWorktreeControllerLayerLive.pipe(
  Layer.provideMerge(AgentControlRuntimeBaseServicesLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlControlledThreadMaterializationCoordinatorServiceLayerLive =
  AgentControlControlledThreadMaterializationCoordinatorLive.pipe(
    Layer.provideMerge(AgentControlRuntimeBaseServicesLayerLive),
    Layer.provideMerge(AgentControlControlledThreadReservationServiceLayerLive),
    Layer.provideMerge(AgentControlWorktreeControllerServiceLayerLive),
    Layer.provideMerge(AgentControlPolicyLayerLive),
    Layer.provideMerge(OrchestrationLayerLive),
    Layer.provide(AgentControlControlledThreadMaterializationCoordinatorHooksNoop),
    Layer.provide(InitialPlanningWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlControlledThreadActivationServiceLayerLive =
  AgentControlControlledThreadActivationLive.pipe(
    Layer.provideMerge(AgentControlControlledThreadReservationServiceLayerLive),
    Layer.provideMerge(AgentControlControlledThreadMaterializationCoordinatorServiceLayerLive),
    Layer.provide(AgentControlControlledThreadActivationHooksNoop),
  );

const AgentControlRuntimeServicesLayerLive = Layer.mergeAll(
  AgentControlRuntimeBaseServicesLayerLive,
  AgentControlControlledThreadReservationServiceLayerLive,
  AgentControlWorktreeControllerServiceLayerLive,
  AgentControlControlledThreadMaterializationCoordinatorServiceLayerLive,
  AgentControlControlledThreadActivationServiceLayerLive,
);

const AgentControlGithubObserveReactorLayerLive = AgentControlGithubObserveReactorLive.pipe(
  Layer.provide(Layer.merge(AgentControlRuntimeServicesLayerLive, OrchestrationLayerLive)),
  Layer.provide(PersistenceLayerLive),
  Layer.provide(RepositoryIdentityResolver.layer),
);

const AgentControlTaskIntakeReactorLayerLive = AgentControlTaskIntakeReactorLive.pipe(
  Layer.provide(Layer.merge(AgentControlRuntimeServicesLayerLive, OrchestrationLayerLive)),
  Layer.provide(PersistenceLayerLive),
  Layer.provide(RepositoryIdentityResolver.layer),
);

const AgentControlInitialPlanningFinalizerLayerLive = AgentControlInitialPlanningFinalizerLive.pipe(
  Layer.provideMerge(AgentControlInitialPlanningHandoffStoreLive),
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provideMerge(OrchestrationLayerLive),
  Layer.provide(InitialPlanningWakeupLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlImplementationAdmissionLayerLive = AgentControlImplementationAdmissionLive.pipe(
  Layer.provideMerge(AgentControlInitialPlanningFinalizerLayerLive),
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provideMerge(AgentControlWorktreeControllerServiceLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlImplementationTurnCoordinatorLayerLive =
  AgentControlImplementationTurnCoordinatorLive.pipe(
    Layer.provideMerge(AgentControlImplementationAdmissionLayerLive),
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provideMerge(AgentControlWorktreeControllerServiceLayerLive),
    Layer.provideMerge(AgentControlPolicyLayerLive),
    Layer.provideMerge(OrchestrationLayerLive),
    Layer.provide(AgentControlImplementationTurnCoordinatorHooksNoop),
    Layer.provide(ImplementationTurnWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlImplementationStageStarterLayerLive =
  AgentControlImplementationStageStarterLive.pipe(
    Layer.provideMerge(ImplementationHandoffStoreLayerLive),
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provide(ImplementationTurnWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlImplementationStageFinalizerLayerLive =
  AgentControlImplementationStageFinalizerLive.pipe(
    Layer.provideMerge(ImplementationHandoffStoreLayerLive),
    Layer.provideMerge(AgentControlImplementationStageStarterLayerLive),
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provideMerge(OrchestrationLayerLive),
    Layer.provide(ImplementationTurnWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlVerificationAdmissionLayerLive = AgentControlVerificationAdmissionLive.pipe(
  Layer.provideMerge(ImplementationHandoffStoreLayerLive),
  Layer.provideMerge(AgentControlImplementationStageFinalizerLayerLive),
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlVerificationStageStarterLayerLive = AgentControlVerificationStageStarterLive.pipe(
  Layer.provideMerge(VerificationHandoffStoreLayerLive),
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provide(VerificationTurnWakeupLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlVerificationEvaluatorLayerLive = AgentControlVerificationEvaluatorLive.pipe(
  Layer.provideMerge(VerificationHandoffStoreLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlVerificationStageFinalizerLayerLive =
  AgentControlVerificationStageFinalizerLive.pipe(
    Layer.provideMerge(VerificationHandoffStoreLayerLive),
    Layer.provideMerge(AgentControlVerificationEvaluatorLayerLive),
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provide(VerificationTurnWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlTaskVerificationFinalizerLayerLive =
  AgentControlTaskVerificationFinalizerLive.pipe(
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlRunOnceControllerServiceLayerLive = AgentControlRunOnceControllerLayerLive.pipe(
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provideMerge(AgentControlWorktreeControllerServiceLayerLive),
  Layer.provideMerge(AgentControlControlledThreadActivationServiceLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlArmedSchedulerLayerLive = AgentControlArmedSchedulerLive.pipe(
  Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
  Layer.provideMerge(AgentControlTaskIntakeReactorLayerLive),
  Layer.provideMerge(AgentControlRunOnceControllerServiceLayerLive),
  Layer.provide(RuntimeCoreDependenciesBaseLive),
);

const AgentControlVerificationTurnCoordinatorLayerLive =
  AgentControlVerificationTurnCoordinatorLive.pipe(
    Layer.provideMerge(AgentControlVerificationAdmissionLayerLive),
    Layer.provideMerge(AgentControlRuntimeServicesLayerLive),
    Layer.provideMerge(AgentControlWorktreeControllerServiceLayerLive),
    Layer.provideMerge(AgentControlPolicyLayerLive),
    Layer.provideMerge(OrchestrationLayerLive),
    Layer.provide(AgentControlVerificationTurnCoordinatorHooksNoop),
    Layer.provide(VerificationTurnWakeupLayerLive),
    Layer.provide(RuntimeCoreDependenciesBaseLive),
  );

const AgentControlReactorServicesLayerLive = AgentControlReactorLive.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      AgentControlGithubObserveReactorLayerLive,
      AgentControlTaskIntakeReactorLayerLive,
      AgentControlInitialPlanningFinalizerLayerLive,
      AgentControlImplementationAdmissionLayerLive,
      AgentControlImplementationTurnCoordinatorLayerLive,
      AgentControlImplementationStageStarterLayerLive,
      AgentControlImplementationStageFinalizerLayerLive,
      AgentControlVerificationAdmissionLayerLive,
      AgentControlVerificationStageStarterLayerLive,
      AgentControlVerificationTurnCoordinatorLayerLive,
      AgentControlVerificationEvaluatorLayerLive,
      AgentControlVerificationStageFinalizerLayerLive,
      AgentControlTaskVerificationFinalizerLayerLive,
      AgentControlRunOnceControllerServiceLayerLive,
      AgentControlArmedSchedulerLayerLive,
    ),
  ),
);

const RuntimeCoreDependenciesLive = Layer.mergeAll(
  RuntimeCoreDependenciesBaseLive,
  AgentControlPolicyLayerLive,
  AgentControlRuntimeServicesLayerLive,
  AgentControlReactorServicesLayerLive,
);

const RuntimeDependenciesLive = RuntimeCoreDependenciesLive.pipe(
  // Misc.
  Layer.provideMerge(BackgroundLayerLive),
  Layer.provideMerge(ResourceDiagnosticsLayerLive),
  Layer.provideMerge(TraceDiagnostics.layer),
  Layer.provideMerge(AnalyticsService.layer),
  Layer.provideMerge(ExternalLauncher.layer),
  Layer.provideMerge(RemoteOpenTargets.layer),
  Layer.provideMerge(ServerLifecycleEvents.layer),
  Layer.provide(NetService.layer),
);

export const makeServerRuntimeStartupFailClosed = Effect.fn("makeServerRuntimeStartupFailClosed")(
  function* (
    startup: ServerRuntimeStartup.ServerRuntimeStartup["Service"],
    agentControl: AgentControlReactor["Service"],
    config: Pick<ServerConfig.ServerConfig["Service"], "mode" | "host" | "port">,
  ) {
    const terminalError = yield* Ref.make<ServerRuntimeStartup.ServerRuntimeStartupError | null>(
      null,
    );
    const terminal = yield* Deferred.make<never, ServerRuntimeStartup.ServerRuntimeStartupError>();
    yield* Effect.flip(agentControl.awaitFailure).pipe(
      Effect.flatMap((cause) => {
        const error = new ServerRuntimeStartup.ServerRuntimeStartupError({
          mode: config.mode,
          host: config.host ?? null,
          port: config.port,
          cause,
        });
        return Ref.set(terminalError, error).pipe(
          Effect.andThen(Deferred.fail(terminal, error)),
          Effect.asVoid,
        );
      }),
      Effect.forkScoped({ startImmediately: true }),
    );
    const runtimeFailure = Deferred.await(terminal);
    const ensureOpen = Ref.get(terminalError).pipe(
      Effect.flatMap((error) => (error === null ? Effect.void : Effect.fail(error))),
    );
    return ServerRuntimeStartup.ServerRuntimeStartup.of({
      ...startup,
      awaitCommandReady: ensureOpen.pipe(
        Effect.andThen(Effect.raceFirst(startup.awaitCommandReady, runtimeFailure)),
        Effect.andThen(ensureOpen),
      ),
      markHttpListening: startup.markHttpListening,
      enqueueCommand: (effect) =>
        ensureOpen.pipe(
          Effect.andThen(Effect.raceFirst(startup.enqueueCommand(effect), runtimeFailure)),
        ),
    });
  },
);

const ServerRuntimeStartupFailClosedLive = Layer.effect(
  ServerRuntimeStartup.ServerRuntimeStartup,
  Effect.gen(function* () {
    const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
    const agentControl = yield* AgentControlReactor;
    const config = yield* ServerConfig.ServerConfig;
    return yield* makeServerRuntimeStartupFailClosed(startup, agentControl, config);
  }),
);

const commandReadinessLayer = HttpRouter.middleware(
  (httpEffect) =>
    Effect.flatMap(ServerRuntimeStartup.ServerRuntimeStartup, (startup) =>
      startup.awaitCommandReady.pipe(Effect.orDie, Effect.andThen(httpEffect)),
    ),
  { global: true },
);

export const makeRoutesLayer = Layer.mergeAll(
  Layer.mergeAll(
    HttpApiBuilder.layer(EnvironmentHttpApi).pipe(
      Layer.provide(authHttpApiLayer),
      Layer.provide(connectHttpApiLayer),
      Layer.provide(orchestrationHttpApiLayer),
      Layer.provide(pullRequestHttpApiLayer),
      Layer.provide(serverEnvironmentHttpApiLayer),
      Layer.provide(environmentAuthenticatedAuthLayer),
    ),
    otlpTracesProxyRouteLayer,
    assetRouteLayer,
    attachmentUploadRouteLayer,
    staticAndDevRouteLayer,
    websocketRpcRouteLayer,
  ),
  McpHttpServer.layer.pipe(Layer.provide(McpSessionRegistry.layer)),
).pipe(
  // Both transports consume the same service instance, so caches single-flight across clients
  // and mutations observed on WebSocket invalidate patches subsequently read over HTTP.
  Layer.provide(PullRequestServiceLive),
  Layer.provide(PreviewAutomationBroker.layer),
  Layer.provide(ServerSelfUpdate.layer.pipe(Layer.provide(DesktopAppUpdateLayerLive))),
  Layer.provide(commandReadinessLayer),
  Layer.provide(browserApiCorsLayer),
  Layer.provide(httpCompressionLayer),
);

const makeServerLayer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    const activation = yield* Deferred.make<void>();
    const awaitActivation = Deferred.await(activation);
    const activationLayer = Layer.succeed(ServerActivation, awaitActivation);
    const runtimeStateParked = yield* Deferred.make<void>();
    const tailscaleParked = yield* Deferred.make<void>();
    const cloudLinkParked = yield* Deferred.make<void>();
    const routesReady = yield* Deferred.make<void>();
    const launcherLayer = ServiceLauncherClient.layer;

    yield* fixPath();

    const httpListeningLayer = Layer.effectDiscard(
      Effect.gen(function* () {
        yield* HttpServer.HttpServer;
        const startup = yield* ServerRuntimeStartup.ServerRuntimeStartup;
        yield* startup.markHttpListening;
      }),
    );
    const runtimeStateLayer = Layer.effectDiscard(
      Effect.acquireRelease(
        Effect.gen(function* () {
          yield* Deferred.succeed(runtimeStateParked, undefined).pipe(Effect.orDie);
          yield* awaitActivation;
          const server = yield* HttpServer.HttpServer;
          const address = server.address;
          if (typeof address === "string" || !("port" in address)) {
            return;
          }

          const state = yield* makePersistedServerRuntimeState({
            config,
            port: address.port,
          });
          yield* persistServerRuntimeState({
            path: config.serverRuntimeStatePath,
            state,
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to persist server runtime state", { cause }),
            ),
          );
        }),
        () =>
          clearPersistedServerRuntimeState(config.serverRuntimeStatePath).pipe(
            Effect.catchCause((cause) =>
              Effect.logWarning("Failed to clear server runtime state", { cause }),
            ),
          ),
      ),
    );
    const tailscaleServeLayer = config.tailscaleServeEnabled
      ? Layer.effectDiscard(
          Effect.acquireRelease(
            Effect.gen(function* () {
              yield* Deferred.succeed(tailscaleParked, undefined).pipe(Effect.orDie);
              yield* awaitActivation;
              const server = yield* HttpServer.HttpServer;
              const address = server.address;
              if (typeof address === "string" || !("port" in address)) {
                return null;
              }

              const localPort = address.port;
              return yield* ensureTailscaleServe({
                localPort,
                servePort: config.tailscaleServePort,
                localHost: "127.0.0.1",
              }).pipe(
                Effect.as({ localPort, servePort: config.tailscaleServePort }),
                Effect.tap(() =>
                  Effect.logInfo("Tailscale Serve configured", {
                    localPort,
                    servePort: config.tailscaleServePort,
                  }),
                ),
                Effect.catch((cause) =>
                  Effect.logWarning("Failed to configure Tailscale Serve", {
                    cause,
                    localPort,
                    servePort: config.tailscaleServePort,
                  }).pipe(Effect.as(null)),
                ),
              );
            }),
            (configured) =>
              configured
                ? disableTailscaleServe({ servePort: configured.servePort }).pipe(
                    Effect.tap(() =>
                      Effect.logInfo("Tailscale Serve disabled", {
                        servePort: configured.servePort,
                      }),
                    ),
                    Effect.catch((cause) =>
                      Effect.logWarning("Failed to disable Tailscale Serve", {
                        cause,
                        servePort: configured.servePort,
                      }),
                    ),
                  )
                : Effect.void,
          ),
        )
      : Layer.empty;
    const cloudDesiredLinkReconcileLayer = Layer.effectDiscard(
      Effect.gen(function* () {
        if (!hasCloudPublicConfig) {
          yield* Deferred.succeed(cloudLinkParked, undefined).pipe(Effect.orDie);
          return;
        }
        const releaseManagedTunnel = releaseManagedTunnelOnShutdown().pipe(
          Effect.timeout("10 seconds"),
          Effect.tap((released) =>
            released ? Effect.logInfo("Released the managed tunnel on shutdown") : Effect.void,
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning(
              "Failed to release the managed tunnel on shutdown; the next link reuses it",
              { errors: Cause.prettyErrors(cause).map((error) => error.message) },
            ),
          ),
          Effect.asVoid,
        );
        // A launcher trial can be stopped before activation. The previous
        // server is already gone, so the trial owns cleanup immediately; the
        // pending-state check keeps the tunnel for normal commit or rollback,
        // while the launcher's explicit-stop marker allows it to be released.
        // Other runtimes wait for activation so a failed standby cannot tear
        // down the active runtime's tunnel.
        const cleanupBeforeActivation = yield* pendingServiceUpdateExists;
        if (cleanupBeforeActivation) {
          yield* Effect.addFinalizer(() => releaseManagedTunnel);
        }
        yield* forkParked(
          Effect.gen(function* () {
            if (!cleanupBeforeActivation) {
              yield* Effect.addFinalizer(() => releaseManagedTunnel);
            }
            if (!(yield* CloudCliState.readCliDesiredCloudLink)) return;
            const server = yield* HttpServer.HttpServer;
            const address = server.address;
            if (typeof address === "string" || !("port" in address)) return;
            // No settling delay before the first attempt: routes are already
            // serving by the time activation opens this gate (the startup
            // sequence awaits routesReady), and the retry schedule below
            // covers anything this sleep used to hedge against. Every
            // millisecond here is dead time on the path to remote
            // reachability after a restart.
            yield* reconcileDesiredCloudLink(`http://127.0.0.1:${address.port}`).pipe(
              Effect.retry({
                while: shouldRetryCloudLink,
                schedule: Schedule.exponential("1 second").pipe(
                  Schedule.modifyDelay(({ duration }) =>
                    Effect.succeed(Duration.min(duration, Duration.seconds(30))),
                  ),
                  Schedule.upTo({ duration: "10 minutes" }),
                ),
              }),
              Effect.tap(() => Effect.logInfo("T3 Connect desired link reconciled on startup")),
              Effect.catch((cause) =>
                Effect.logWarning("Failed to reconcile T3 Connect desired link on startup", {
                  message: cause.message,
                }),
              ),
            );
          }),
        );
        yield* Deferred.succeed(cloudLinkParked, undefined).pipe(Effect.orDie);
      }),
    );

    const runtimeServicesLive = ServerRuntimeStartupFailClosedLive.pipe(
      Layer.provideMerge(
        ServerRuntimeStartup.layerWithOptions({
          activate: Deferred.succeed(activation, undefined).pipe(Effect.asVoid),
          abort: (error) => Deferred.die(activation, error).pipe(Effect.asVoid),
          awaitAuxiliaryParked: Effect.all(
            [
              Deferred.await(runtimeStateParked),
              Deferred.await(cloudLinkParked),
              Deferred.await(routesReady),
              ...(config.tailscaleServeEnabled ? [Deferred.await(tailscaleParked)] : []),
            ],
            { concurrency: "unbounded" },
          ).pipe(Effect.asVoid),
        }),
      ),
      Layer.provideMerge(RuntimeDependenciesLive),
      Layer.provide(launcherLayer),
    );

    const routesLayer = HttpRouter.serve(makeRoutesLayer.pipe(Layer.provide(launcherLayer)), {
      disableLogger: !config.logWebSocketEvents,
      routerConfig: HTTP_ROUTER_CONFIG,
    }).pipe(Layer.tap(() => Deferred.succeed(routesReady, undefined).pipe(Effect.orDie)));
    const serverApplicationLayer = Layer.mergeAll(
      routesLayer,
      httpListeningLayer,
      runtimeStateLayer,
      tailscaleServeLayer,
      cloudDesiredLinkReconcileLayer,
    );

    return serverApplicationLayer.pipe(
      Layer.provideMerge(runtimeServicesLive),
      Layer.provide(activationLayer),
      Layer.provideMerge(serverRelayBrokerTracingLayer),
      Layer.provideMerge(HttpServerLive),
      Layer.provide(ApplicationObservabilityLive),
      Layer.provideMerge(FetchHttpClient.layer),
      // PR reads, Git operations, and WebSocket discovery share one process limiter.
      Layer.provide(VcsProcess.layer),
      Layer.provideMerge(PlatformServicesLive),
    );
  }),
);

// Important: Only `ServerConfig` should be provided by the CLI layer!!! Don't let other requirements leak into the launch layer.
export const runServer = Effect.gen(function* () {
  const runtimeError = nodeRuntimeRequiredError();
  if (runtimeError !== undefined) {
    return yield* Effect.fail(runtimeError);
  }
  return yield* Layer.launch(makeServerLayer);
});
