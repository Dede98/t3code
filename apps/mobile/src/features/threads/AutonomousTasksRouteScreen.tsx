import { RegistryContext, useAtomRefresh, useAtomValue } from "@effect/atom-react";
import {
  CommandId,
  type AgentControlRunOnceView,
  type AgentControlSetProjectModeInput,
  type AgentControlTaskId,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import {
  agentControlEndPausedInput,
  agentControlEndPausedExplanation,
  agentControlCommandErrorMessage,
  agentControlRunStatus,
  agentControlEndBlockedRunInput,
  agentControlCanEndBlockedRun,
  agentControlModeChangeBlocker,
  agentControlSnapshotReady,
  agentControlSnapshotFresh,
  agentControlStageHeading,
  agentControlVerificationPassed,
  agentControlStartBlockers,
  agentControlStartInput,
  agentControlArmedStatus,
  agentControlArmBlockers,
  agentControlArmInput,
  agentControlDisarmInput,
  agentControlArmedExplanation,
  agentControlDisarmExplanation,
} from "@t3tools/client-runtime/state/agent-control";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import * as Cause from "effect/Cause";
import {
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  agentControlSetupPermissionBlocker,
  agentControlPreflightErrorMessage,
  bindAgentControlSetupApi,
  createAgentControlSetupController,
} from "@t3tools/client-runtime/state/agent-control-setup";
import { Platform, Pressable, ScrollView, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AndroidScreenHeader } from "../../components/AndroidScreenHeader";
import { AppText as Text } from "../../components/AppText";
import { copyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import { uuidv4 } from "../../lib/uuid";
import { NativeStackScreenOptions } from "../../native/StackHeader";
import { agentControlEnvironment } from "../../state/agent-control";
import { agentControlSetupEnvironment } from "../../state/agent-control-setup";
import { serverEnvironment } from "../../state/server";
import { AutonomousProjectSetupForm } from "./AutonomousProjectSetupForm";
import { useProject } from "../../state/entities";
import { useEnvironmentPresentation } from "../../state/presentation";
import { useEnvironmentQuery } from "../../state/query";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSavedRemoteConnection } from "../../state/use-remote-environment-registry";

function Action(props: {
  readonly children: string;
  readonly onPress: () => void;
  readonly disabled?: boolean;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      disabled={props.disabled}
      accessibilityState={{ disabled: props.disabled ?? false }}
      onPress={props.onPress}
      className={`rounded-xl bg-card px-4 py-3 active:opacity-70 ${props.disabled ? "opacity-40" : ""}`}
    >
      <Text className="text-sm font-t3-bold text-primary">{props.children}</Text>
    </Pressable>
  );
}

function RunResult(props: {
  readonly run: AgentControlRunOnceView;
  readonly environmentId: EnvironmentId;
}) {
  const navigation = useNavigation();
  const status = agentControlRunStatus(props.run);
  return (
    <View className="gap-3 rounded-2xl border border-border-subtle p-4">
      <Text className="text-base font-t3-bold">{props.run.task?.title ?? "Run"}</Text>
      {props.run.originMode ? (
        <Text className="text-xs text-foreground-muted">
          {props.run.originMode === "armed" ? "Started automatically" : "Started with Run once"}
        </Text>
      ) : null}
      <Text className="text-sm font-t3-bold">{status.label}</Text>
      {status.tone === "warning" || status.tone === "danger" ? (
        <Text className="text-sm text-foreground-muted">
          Open the affected stage thread for details. Check the reported provider or verification
          error in this environment before taking further action.
        </Text>
      ) : null}
      <Text selectable className="text-xs text-foreground-muted">
        {props.run.state.runId} · Updated {props.run.state.updatedAt}
      </Text>
      {props.run.stages.length === 0 ? (
        <Text className="text-sm text-foreground-muted">No stage evidence available yet.</Text>
      ) : null}
      {props.run.stages.map((stage) => (
        <View key={stage.stageRunId} className="gap-2 border-t border-border-subtle pt-3">
          <Text className="text-sm font-t3-bold">
            {agentControlStageHeading(stage, props.run.stages)} ·{" "}
            {stage.displayStage === "verification" &&
            stage.status === "succeeded" &&
            !agentControlVerificationPassed(stage)
              ? "Evidence incomplete"
              : stage.status}
          </Text>
          <Text className="text-xs text-foreground-muted">
            Provider: {stage.providerInstanceId ?? "Unavailable"} · Model:{" "}
            {stage.model ?? "Unavailable"}
          </Text>
          {stage.errorCode ? (
            <Text selectable className="text-sm text-destructive">
              {stage.errorCode}
            </Text>
          ) : null}
          {stage.displayStage === "verification" ? (
            <>
              <Text className="text-sm">
                Verification:{" "}
                {agentControlVerificationPassed(stage)
                  ? "Passed with required evidence"
                  : stage.verification?.verdict === "failed"
                    ? "Failed"
                    : "Not confirmed — evidence incomplete or unavailable"}
              </Text>
              {stage.verification?.errorCode ? (
                <Text selectable className="text-sm text-destructive">
                  {stage.verification.errorCode}
                </Text>
              ) : null}
              {!stage.verification || stage.verification.checks.length === 0 ? (
                <Text className="text-sm text-foreground-muted">
                  Check evidence is not available.
                </Text>
              ) : null}
              {stage.verification?.checks.map((check) => (
                <View key={check.id} className="gap-1 rounded-xl bg-card p-3">
                  <Text className="text-sm font-t3-bold">
                    {check.id} · {check.required ? "Required" : "Optional"} · {check.status}
                  </Text>
                  <Text selectable className="text-xs text-foreground-muted">
                    {[check.command, ...check.args].join(" ")} · {check.cwd}
                  </Text>
                  <Text className="text-xs text-foreground-muted">
                    Exit code: {check.exitCode ?? "Unavailable"}
                    {check.completedAt ? ` · ${check.completedAt}` : ""}
                  </Text>
                  {check.output ? (
                    <Text selectable className="text-xs">
                      {check.output}
                    </Text>
                  ) : null}
                </View>
              ))}
            </>
          ) : null}
          {stage.threadId ? (
            <View className="flex-row flex-wrap gap-2">
              <Action
                onPress={() =>
                  navigation.navigate("Thread", {
                    environmentId: props.environmentId,
                    threadId: stage.threadId!,
                  })
                }
              >
                Open thread
              </Action>
              <Action
                onPress={() =>
                  navigation.navigate("ThreadReview", {
                    environmentId: props.environmentId,
                    threadId: stage.threadId!,
                  })
                }
              >
                Open changes
              </Action>
            </View>
          ) : null}
          {stage.worktreePath ? (
            <>
              <Text selectable className="text-xs text-foreground-muted">
                {stage.branch} · {stage.worktreePath}
              </Text>
              <Action
                onPress={() => copyTextWithHaptic(stage.worktreePath!, { target: "worktree path" })}
              >
                Copy worktree path
              </Action>
            </>
          ) : null}
        </View>
      ))}
    </View>
  );
}

type AutonomousTasksRouteParams = {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
};

export function AutonomousTasksRouteScreen({
  route,
}: StaticScreenProps<AutonomousTasksRouteParams>) {
  return <AutonomousTasksProjectScreen key={JSON.stringify(route.params)} {...route.params} />;
}

function AutonomousTasksProjectScreen({ environmentId, projectId }: AutonomousTasksRouteParams) {
  const navigation = useNavigation();
  const project = useProject({ environmentId, projectId });
  const { presentation } = useEnvironmentPresentation(environmentId);
  const connection = useSavedRemoteConnection(environmentId);
  const insets = useSafeAreaInsets();
  const target = { environmentId, input: { projectId } };
  const snapshotAtom = agentControlEnvironment.snapshot(target);
  const snapshot = useEnvironmentQuery(snapshotAtom);
  const snapshotResult = useAtomValue(snapshotAtom);
  const sessionAtom = environmentSession.sessionStateAtom(environmentId);
  const sessionResult = useAtomValue(sessionAtom);
  const refreshSession = useAtomRefresh(sessionAtom);
  const preflightAtom = agentControlEnvironment.preflight(target);
  const preflight = useEnvironmentQuery(preflightAtom);
  const preflightResult = useAtomValue(preflightAtom);
  const policyAtom = agentControlEnvironment.policy(target);
  const policy = useEnvironmentQuery(policyAtom);
  const policyResult = useAtomValue(policyAtom);
  const setMode = useAtomCommand(agentControlEnvironment.setMode);
  const [selectedTaskId, setSelectedTaskId] = useState<AgentControlTaskId | null>(null);
  const [localPending, setPending] = useState(false);
  const sharedPending = useAtomValue(agentControlEnvironment.pending(target));
  const pending = localPending || sharedPending;
  const [error, setError] = useState<string | null>(null);
  const pendingRef = useRef(false);
  const mountedRef = useRef(true);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);
  const connected = presentation?.connection.phase === "connected";
  const snapshotReady = agentControlSnapshotReady(snapshotResult, connected);
  const currentFreshness = {
    connected,
    snapshot: snapshot.data,
    sessionResult,
    preflightResult,
    policyResult,
  };
  const [freshness, setFreshness] = useState(currentFreshness);
  if (freshness.connected !== connected) setFreshness(currentFreshness);
  const refreshSnapshot = snapshot.refresh;
  const refreshPreflight = preflight.refresh;
  const refreshPolicy = policy.refresh;
  useEffect(() => {
    if (!connected) return;
    refreshSession();
    refreshSnapshot();
    refreshPreflight();
    refreshPolicy();
  }, [connected, refreshSession, refreshSnapshot, refreshPreflight, refreshPolicy]);
  const fresh =
    connected &&
    freshness.connected === connected &&
    agentControlSnapshotFresh(snapshotResult, freshness.snapshot ?? null, connected) &&
    sessionResult !== freshness.sessionResult;
  const registry = useContext(RegistryContext);
  const serverConfig = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  const setupFreshBlocker = fresh
    ? null
    : "Checking current state and permissions in this environment.";
  const activeRun = snapshot.data?.runs.some((run) => run.state.status === "active") ?? false;
  const setupWriteBlocker =
    setupFreshBlocker ??
    (pending ? "Wait for the current project action to finish." : null) ??
    agentControlSetupPermissionBlocker(sessionResult, "configure") ??
    (activeRun
      ? "Finish the active run before changing configuration. Later stages can read updated settings."
      : null);
  const setupImportBlocker =
    setupFreshBlocker ?? agentControlSetupPermissionBlocker(sessionResult, "import");
  const [setupGuards] = useState(() => ({
    setupWriteBlocker,
    setupImportBlocker,
    connected,
    mounted: true,
  }));
  useLayoutEffect(() => {
    Object.assign(setupGuards, { setupWriteBlocker, setupImportBlocker, connected, mounted: true });
    return () => {
      setupGuards.mounted = false;
    };
  }, [setupGuards, setupWriteBlocker, setupImportBlocker, connected]);
  const setupController = useMemo(
    () =>
      createAgentControlSetupController(
        bindAgentControlSetupApi(
          agentControlSetupEnvironment,
          registry,
          {
            environmentId,
            input: { projectId },
          },
          {
            canWrite: () => setupGuards.setupWriteBlocker,
            canImport: () => setupGuards.setupImportBlocker,
            isCurrent: () => setupGuards.mounted && setupGuards.connected,
          },
        ),
      ),
    [environmentId, projectId, registry, setupGuards],
  );
  useEffect(() => {
    if (connected) void setupController.load();
    else setupController.invalidate();
    return () => setupController.invalidate();
  }, [connected, setupController]);
  const setupState = useSyncExternalStore(
    setupController.subscribe,
    setupController.getSnapshot,
    setupController.getSnapshot,
  );
  const modeChangeBlocker =
    agentControlModeChangeBlocker(sessionResult) ??
    (fresh ? null : "Checking current state and permissions in this environment.");
  const readiness = {
    policy:
      policyResult !== freshness.policyResult && policy.error === null && !policy.isPending
        ? policy.data
        : null,
    snapshot: snapshotReady ? snapshot.data : null,
    preflight:
      preflightResult === freshness.preflightResult || preflight.error || preflight.isPending
        ? null
        : preflight.data,
    connected,
    pending: pending || setupState.pending,
    modeChangeBlocker:
      modeChangeBlocker ??
      (setupState.policyDirty || setupState.githubDirty
        ? "Save or discard setup changes before starting autonomous work."
        : null),
  };
  const blockers = agentControlStartBlockers({ ...readiness, selectedTaskId });
  const armedStatus = agentControlArmedStatus(readiness.snapshot);
  const armBlockers = agentControlArmBlockers(readiness);
  const modeChangeReadiness = {
    snapshot: snapshot.data,
    connected: snapshotReady,
    pending,
    modeChangeBlocker,
  };
  const endBlockedRunInput = agentControlEndBlockedRunInput(modeChangeReadiness);
  const disarmInput = agentControlDisarmInput(modeChangeReadiness);
  const endPausedInput = agentControlEndPausedInput(modeChangeReadiness);

  async function changeMode(
    input: AgentControlSetProjectModeInput,
    action?: "end-blocked" | "disarm" | "end-paused",
  ) {
    if (
      pendingRef.current ||
      pending ||
      (setupController.getSnapshot().pending && action === undefined) ||
      modeChangeBlocker !== null ||
      !snapshotReady
    )
      return;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    try {
      const result = await setMode({ environmentId, input });
      if (mountedRef.current && result._tag === "Failure") {
        const cause = Cause.squash(result.cause);
        setError(agentControlCommandErrorMessage(cause));
      }
      if (mountedRef.current) {
        refreshSnapshot();
        refreshPreflight();
        refreshPolicy();
        refreshSession();
      }
    } finally {
      pendingRef.current = false;
      if (mountedRef.current) setPending(false);
    }
  }

  return (
    <View className="flex-1 bg-background">
      {Platform.OS === "android" ? (
        <>
          <NativeStackScreenOptions options={{ headerShown: false }} />
          <AndroidScreenHeader title="Autonomous tasks" onBack={() => navigation.goBack()} />
        </>
      ) : null}
      <ScrollView
        contentInsetAdjustmentBehavior="automatic"
        contentContainerStyle={{ gap: 16, padding: 20, paddingBottom: insets.bottom + 24 }}
      >
        <View className="gap-1">
          <Text className="text-xl font-t3-bold">{project?.title ?? "Project"}</Text>
          <Text selectable className="text-xs text-foreground-muted">
            {project?.workspaceRoot ?? projectId} · {connection?.environmentLabel ?? environmentId}
          </Text>
          <Text className="text-sm text-foreground-muted">
            Run one eligible task or enable automatic tasks for this project and environment.
            Worktree files remain on this environment; mobile can copy the path and open changes.
          </Text>
        </View>
        {!connected ? (
          <Text className="text-sm text-warning-foreground">
            {presentation?.connection.error ??
              "Reconnecting to the environment. Run updates will resume when connected."}
          </Text>
        ) : null}
        {error || snapshot.error || preflight.error || policy.error ? (
          <Text selectable className="text-sm text-destructive">
            {error ?? snapshot.error ?? preflight.error ?? policy.error}
          </Text>
        ) : null}
        <Action
          onPress={() => {
            setFreshness(currentFreshness);
            refreshSnapshot();
            refreshPreflight();
            refreshPolicy();
            refreshSession();
          }}
          disabled={!connected || pending}
        >
          Refresh status and preflight
        </Action>
        <AutonomousProjectSetupForm
          controller={setupController}
          providers={serverConfig?.providers ?? []}
          repositoryIdentity={project?.repositoryIdentity ?? null}
          writeBlocker={setupWriteBlocker}
          importBlocker={setupImportBlocker}
          connected={connected}
          onSaved={() => {
            refreshSnapshot();
            refreshPreflight();
            refreshPolicy();
          }}
        />
        <View className="gap-3 rounded-2xl border border-border-subtle p-4">
          <Text className="text-base font-t3-bold">Automatic tasks</Text>
          <Text accessibilityLiveRegion="polite" className="text-sm font-t3-bold">
            {armedStatus.label}
          </Text>
          {snapshot.data?.runs
            .filter((run) => run.state.status === "active")
            .map((run) => (
              <Text key={run.state.runId} className="text-sm text-foreground-muted">
                Current task: {run.task?.title ?? "Waiting for task data"} ·{" "}
                {agentControlRunStatus(run).label}
              </Text>
            ))}
          {armedStatus.enabled ? (
            <>
              <Text className="text-sm text-foreground-muted">{agentControlDisarmExplanation}</Text>
              {modeChangeBlocker ? (
                <Text className="text-sm text-foreground-muted">{modeChangeBlocker}</Text>
              ) : null}
              <Action
                disabled={disarmInput === null}
                onPress={() => {
                  if (disarmInput) void changeMode(disarmInput, "disarm");
                }}
              >
                {pending ? "Submitting…" : "Turn off automation"}
              </Action>
            </>
          ) : (
            <>
              <Text className="text-sm text-foreground-muted">{agentControlArmedExplanation}</Text>
              {armBlockers.map((blocker) => (
                <Text key={blocker} className="text-sm text-foreground-muted">
                  {blocker}
                </Text>
              ))}
              <Action
                disabled={armBlockers.length > 0}
                onPress={() => {
                  if (readiness.snapshot && armBlockers.length === 0)
                    void changeMode(agentControlArmInput(readiness.snapshot));
                }}
              >
                {pending ? "Submitting…" : "Turn on automation"}
              </Action>
            </>
          )}
        </View>
        <Text className="text-base font-t3-bold">Configured provider and model readiness</Text>
        {preflight.data?.roles.map((role) => (
          <View key={role.role} className="gap-1">
            <Text className="text-sm font-t3-bold">
              {role.role} · {role.accessMode}
            </Text>
            {role.candidates.map((candidate) => (
              <Text key={candidate.candidateIndex} className="text-xs text-foreground-muted">
                {candidate.candidateIndex === role.selectedCandidateIndex
                  ? "Selected: "
                  : "Fallback: "}
                {candidate.providerInstanceId} · {candidate.model} ·{" "}
                {candidate.errorCode
                  ? agentControlPreflightErrorMessage(candidate.errorCode)
                  : candidate.runtimeReady
                    ? "Ready"
                    : "Unavailable"}
              </Text>
            ))}
            {role.errorCode ? (
              <Text className="text-sm text-destructive">
                {agentControlPreflightErrorMessage(role.errorCode)}
              </Text>
            ) : null}
          </View>
        ))}
        {preflight.data && !preflight.data.staticPreflight.ok
          ? preflight.data.staticPreflight.errors.map((error) => (
              <Text
                key={`${error.role}:${error.code}:${"candidateIndex" in error ? `${error.source}:${error.candidateIndex}:${error.instanceId}` : ""}`}
                className="text-sm text-destructive"
              >
                {error.role}: {agentControlPreflightErrorMessage(error.code)}
              </Text>
            ))
          : null}
        <Text className="text-base font-t3-bold">Verification checks</Text>
        {policy.data?.projectPolicy?.policy.verificationChecks?.map((check) => (
          <View key={check.id} className="gap-1">
            <Text className="text-sm">
              {check.id} · {check.required ? "Required" : "Optional"}
            </Text>
            <Text selectable className="text-xs text-foreground-muted">
              {[check.command, ...check.args].join(" ")} · {check.cwd}
            </Text>
          </View>
        ))}
        {!policy.data?.projectPolicy?.policy.verificationChecks?.length ? (
          <Text className="text-sm text-foreground-muted">No verification checks configured.</Text>
        ) : null}
        {snapshot.data?.projectState.mode === "manual" ? (
          <Action
            disabled={!snapshotReady || pending || setupState.pending || modeChangeBlocker !== null}
            onPress={() => {
              if (!snapshotReady || !snapshot.data) return;
              void changeMode({
                commandId: CommandId.make(uuidv4()),
                projectId,
                expectedRevision: snapshot.data.projectState.revision,
                mode: "observe",
              });
            }}
          >
            Enable task observation
          </Action>
        ) : null}
        {snapshot.data?.projectState.mode === "paused" ? (
          <View className="gap-2">
            <Action
              disabled={endPausedInput === null}
              onPress={() => {
                if (endPausedInput) void changeMode(endPausedInput, "end-paused");
              }}
            >
              End paused mode
            </Action>
            <Text className="text-sm text-foreground-muted">
              {agentControlEndPausedExplanation}
            </Text>
          </View>
        ) : null}
        {snapshot.data?.projectState.mode === "observe" &&
        !snapshot.data.runs.some((run) => run.state.status === "active") ? (
          <Action
            disabled={!snapshotReady || pending || setupState.pending || modeChangeBlocker !== null}
            onPress={() => {
              if (!snapshotReady || !snapshot.data) return;
              void changeMode({
                commandId: CommandId.make(uuidv4()),
                projectId,
                expectedRevision: snapshot.data.projectState.revision,
                mode: "manual",
              });
            }}
          >
            Disable task observation
          </Action>
        ) : null}
        {armedStatus.enabled !== true &&
        snapshot.data?.projectState.mode === "run-once" &&
        snapshot.data.runs.some(agentControlCanEndBlockedRun) ? (
          <View className="gap-2">
            <Action
              disabled={endBlockedRunInput === null}
              onPress={() => {
                if (endBlockedRunInput) void changeMode(endBlockedRunInput, "end-blocked");
              }}
            >
              End blocked run
            </Action>
            <Text className="text-sm text-foreground-muted">
              Ending the run prevents further automatic steps and keeps its failure history. After
              fixing the cause, remove the old issue from ready intake in GitHub and start a new
              eligible task.
            </Text>
          </View>
        ) : null}
        <Text className="text-base font-t3-bold">Run once · Choose task</Text>
        {snapshot.data?.tasks.length === 0 ? (
          <Text className="text-sm text-foreground-muted">
            No tasks available. Check the project's GitHub intake configuration and observe mode.
          </Text>
        ) : null}
        {snapshot.data?.tasks.map((task) => (
          <Pressable
            key={task.taskId}
            accessibilityRole="radio"
            accessibilityState={{ checked: task.taskId === selectedTaskId, disabled: pending }}
            disabled={pending}
            onPress={() => setSelectedTaskId(task.taskId)}
            className={`gap-1 rounded-xl border p-3 ${task.taskId === selectedTaskId ? "border-primary" : "border-border-subtle"}`}
          >
            <Text className="text-sm font-t3-bold">{task.title}</Text>
            <Text className="text-xs text-foreground-muted">
              {task.status} · {task.sourceGate}
              {snapshot.data?.nextTaskId === task.taskId ? " · Eligible next by server order" : ""}
            </Text>
          </Pressable>
        ))}
        {blockers.map((blocker) => (
          <Text key={blocker} className="text-sm text-foreground-muted">
            {blocker}
          </Text>
        ))}
        <Action
          disabled={blockers.length > 0}
          onPress={() => {
            if (!snapshot.data || !selectedTaskId || blockers.length > 0) return;
            void changeMode(agentControlStartInput(snapshot.data, selectedTaskId));
          }}
        >
          {pending ? "Submitting…" : "Run selected task once"}
        </Action>
        <Text className="text-base font-t3-bold">Runs</Text>
        {snapshot.data?.runs.length === 0 ? (
          <Text className="text-sm text-foreground-muted">No runs yet.</Text>
        ) : null}
        {snapshot.data?.runs.map((run) => (
          <RunResult key={run.state.runId} run={run} environmentId={environmentId} />
        ))}
      </ScrollView>
    </View>
  );
}
