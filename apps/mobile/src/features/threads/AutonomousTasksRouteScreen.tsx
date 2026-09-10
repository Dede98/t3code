import { useAtomValue } from "@effect/atom-react";
import {
  CommandId,
  type AgentControlRunOnceView,
  type AgentControlSetProjectModeInput,
  type AgentControlTaskId,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import {
  agentControlRunStatus,
  agentControlSnapshotReady,
  agentControlStageHeading,
  agentControlVerificationPassed,
  agentControlStartBlockers,
  agentControlStartInput,
} from "@t3tools/client-runtime/state/agent-control";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import * as Cause from "effect/Cause";
import { useRef, useState } from "react";
import { Platform, Pressable, ScrollView, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AndroidScreenHeader } from "../../components/AndroidScreenHeader";
import { AppText as Text } from "../../components/AppText";
import { copyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import { uuidv4 } from "../../lib/uuid";
import { NativeStackScreenOptions } from "../../native/StackHeader";
import { agentControlEnvironment } from "../../state/agent-control";
import { useProject } from "../../state/entities";
import { useEnvironmentPresentation } from "../../state/presentation";
import { useEnvironmentQuery } from "../../state/query";
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
  const { environmentId, projectId } = route.params;
  const navigation = useNavigation();
  const project = useProject({ environmentId, projectId });
  const { presentation } = useEnvironmentPresentation(environmentId);
  const connection = useSavedRemoteConnection(environmentId);
  const insets = useSafeAreaInsets();
  const target = { environmentId, input: { projectId } };
  const snapshotAtom = agentControlEnvironment.snapshot(target);
  const snapshot = useEnvironmentQuery(snapshotAtom);
  const snapshotResult = useAtomValue(snapshotAtom);
  const preflight = useEnvironmentQuery(agentControlEnvironment.preflight(target));
  const policy = useEnvironmentQuery(agentControlEnvironment.policy(target));
  const setMode = useAtomCommand(agentControlEnvironment.setMode);
  const [selectedTaskId, setSelectedTaskId] = useState<AgentControlTaskId | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const pendingRef = useRef(false);
  const connected = presentation?.connection.phase === "connected";
  const snapshotReady = agentControlSnapshotReady(snapshotResult, connected);
  const blockers = agentControlStartBlockers({
    policy: policy.error === null && !policy.isPending ? policy.data : null,
    snapshot: snapshotReady ? snapshot.data : null,
    preflight: preflight.error || preflight.isPending ? null : preflight.data,
    selectedTaskId,
    connected,
    pending,
  });

  async function changeMode(input: AgentControlSetProjectModeInput) {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setPending(true);
    setError(null);
    try {
      const result = await setMode({ environmentId, input });
      if (result._tag === "Failure") {
        const cause = Cause.squash(result.cause);
        setError(
          cause instanceof Error
            ? cause.message
            : "The request failed. Reload the current state before retrying.",
        );
      }
    } finally {
      pendingRef.current = false;
      setPending(false);
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
            Start one eligible task and review its verified result. Worktree files remain on this
            environment; mobile can copy the path and open changes.
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
            snapshot.refresh();
            preflight.refresh();
            policy.refresh();
          }}
          disabled={!connected || pending}
        >
          Refresh status and preflight
        </Action>
        <Text className="text-base font-t3-bold">Provider and model</Text>
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
                {candidate.errorCode ?? (candidate.runtimeReady ? "Ready" : "Unavailable")}
              </Text>
            ))}
            {role.errorCode ? (
              <Text className="text-sm text-destructive">{role.errorCode}</Text>
            ) : null}
          </View>
        ))}
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
        {snapshot.data?.projectState.mode === "manual" ||
        snapshot.data?.projectState.mode === "paused" ? (
          <Action
            disabled={!snapshotReady || pending}
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
        {snapshot.data?.projectState.mode === "observe" &&
        !snapshot.data.runs.some((run) => run.state.status === "active") ? (
          <Action
            disabled={!snapshotReady || pending}
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
        <Text className="text-base font-t3-bold">Choose task</Text>
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
              {snapshot.data?.nextTaskId === task.taskId ? " · Next eligible task" : ""}
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
