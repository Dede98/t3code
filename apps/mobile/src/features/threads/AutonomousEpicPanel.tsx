import { useEffect, useRef, useState, type ReactNode } from "react";
import { Pressable, View } from "react-native";
import { useNavigation } from "@react-navigation/native";
import {
  agentControlCommandErrorMessage,
  agentControlEpicControlAllowed,
  agentControlEpicControlInput,
  agentControlEpicStartBlockers,
  agentControlEpicStartInput,
  agentControlEpicStatus,
  type AgentControlReadiness,
} from "@t3tools/client-runtime/state/agent-control";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  AgentControlRunOnceId,
  type AgentControlRunOnceView,
  type AgentControlEpicPreview,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";
import { agentControlEnvironment } from "../../state/agent-control";
import { useAtomCommand } from "../../state/use-atom-command";

function Action({
  children,
  disabled,
  onPress,
}: {
  children: string;
  disabled?: boolean;
  onPress: () => void;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityState={{ disabled: disabled ?? false }}
      disabled={disabled}
      onPress={onPress}
      className={`rounded-xl border border-border-subtle p-3 ${disabled ? "opacity-40" : "bg-card"}`}
    >
      <Text className="text-sm">{children}</Text>
    </Pressable>
  );
}

type Props = {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  readiness: AgentControlReadiness;
  onRefresh: () => void;
  readOnly?: boolean;
  renderRun: (run: AgentControlRunOnceView) => ReactNode;
};

export function AutonomousEpicPanel({
  environmentId,
  projectId,
  readiness,
  onRefresh,
  readOnly = false,
  renderRun,
}: Props) {
  const navigation = useNavigation();
  const [loadedRun, setLoadedRun] = useState<AgentControlRunOnceView | null>(null);
  const requestedRunId = useRef<string | null>(null);
  const [savedEpicId, setSavedEpicId] = useState<string | null>(null);
  const [number, setNumber] = useState("");
  const [preview, setPreview] = useState<AgentControlEpicPreview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const mounted = useRef(true);
  const inFlight = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const [wasConnected, setWasConnected] = useState(readiness.connected);
  if (wasConnected !== readiness.connected) {
    setWasConnected(readiness.connected);
    setPreview(null);
  }
  const loadSavedRun = useAtomCommand(agentControlEnvironment.getRun);
  const inspect = useAtomCommand(agentControlEnvironment.epicPreview);
  const start = useAtomCommand(agentControlEnvironment.epicStart);
  const resume = useAtomCommand(agentControlEnvironment.epicResume);
  const stop = useAtomCommand(agentControlEnvironment.epicStop);
  const clear = useAtomCommand(agentControlEnvironment.epicClear);
  const epic = readiness.snapshot?.epic;
  const verifications = epic
    ? [
        ...epic.finalVerificationHistory.filter(
          (verification) => verification.evidenceId !== epic.finalVerification?.evidenceId,
        ),
        ...(epic.finalVerification ? [epic.finalVerification] : []),
      ]
    : [];
  const blockers = agentControlEpicStartBlockers(readiness, preview);
  const previewMessages = [
    ...new Set([...blockers, ...(preview?.blockers.map((blocker) => blocker.message) ?? [])]),
  ];
  const source = epic?.source ?? preview?.source;
  const issueNumber = Number(number);
  const canInspect =
    readiness.connected &&
    !readiness.pending &&
    /^\d+$/.test(number) &&
    Number.isSafeInteger(issueNumber) &&
    issueNumber > 0;

  async function openSavedRun(runId: string) {
    requestedRunId.current = runId;
    const existing = readiness.snapshot?.runs.find((run) => run.state.runId === runId);
    if (existing) {
      setLoadedRun(existing);
      return;
    }
    const result = await loadSavedRun({
      environmentId,
      input: { projectId, runId: AgentControlRunOnceId.make(runId) },
    });
    if (!mounted.current || requestedRunId.current !== runId) return;
    if (result._tag === "Failure") {
      setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      return;
    }
    const saved = result.value.runs.find((run) => run.state.runId === runId);
    if (saved) setLoadedRun(saved);
    else setError("The saved task run is unavailable in this environment.");
  }

  async function execute(action: "preview" | "start" | "resume" | "stop" | "clear") {
    if (inFlight.current || readiness.pending || !readiness.connected) return;
    if (
      action === "preview"
        ? !canInspect
        : action === "start"
          ? blockers.length > 0
          : !agentControlEpicControlAllowed(readiness, action)
    )
      return;
    inFlight.current = true;
    setError(null);
    try {
      if (action === "preview") {
        setPreview(null);
        const result = await inspect({
          environmentId,
          input: { projectId, epicNumber: issueNumber },
        });
        if (!mounted.current) return;
        if (result._tag === "Success") setPreview(result.value);
        else setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      } else {
        const result =
          action === "start" && readiness.snapshot && preview
            ? await start({
                environmentId,
                input: agentControlEpicStartInput(readiness.snapshot, preview),
              })
            : epic && action !== "start"
              ? await { resume, stop, clear }[action]({
                  environmentId,
                  input: agentControlEpicControlInput(epic, action),
                })
              : null;
        if (!mounted.current) return;
        if (result?._tag === "Failure")
          setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
        if (result?._tag === "Success") setPreview(null);
      }
    } finally {
      inFlight.current = false;
      if (mounted.current) onRefresh();
    }
  }

  return (
    <View className="gap-3 rounded-xl border border-border-subtle p-3">
      {!readOnly ? (
        <>
          <Text className="text-base font-t3-bold">Execute a GitHub Epic</Text>
          <Text className="text-sm text-foreground-muted">
            Inspect native GitHub sub-issues and dependencies before starting. One level in this
            repository is supported. Accepted work becomes the next task’s starting point, followed
            by verification of the common result.
          </Text>
          <TextInput
            accessibilityLabel="Epic issue number"
            placeholder="Epic issue number"
            keyboardType="number-pad"
            value={number}
            editable={!readiness.pending}
            onChangeText={(value) => {
              setNumber(value);
              setPreview(null);
            }}
            className="rounded-lg border border-border-subtle bg-card p-3 text-foreground"
          />
          <Action disabled={!canInspect} onPress={() => void execute("preview")}>
            Inspect Epic
          </Action>
          {preview ? (
            <View className="gap-2">
              <Text className="text-sm font-t3-bold">
                Preview: #{preview.source.epic.number} {preview.source.epic.title}
              </Text>
              {preview.source !== source
                ? preview.source.tasks.map((task) => (
                    <Text key={task.issue.issueNodeId} className="text-sm">
                      #{task.issue.number} {task.issue.title} · {task.issue.state}
                      {task.dependencies.length
                        ? ` · Requires ${task.dependencies.map((dependency) => `#${dependency.number}`).join(", ")}`
                        : ""}
                    </Text>
                  ))
                : null}
              {previewMessages.map((blocker) => (
                <Text key={blocker} className="text-sm text-destructive">
                  {blocker}
                </Text>
              ))}
              <Action disabled={blockers.length > 0} onPress={() => void execute("start")}>
                Start inspected Epic
              </Action>
            </View>
          ) : null}
        </>
      ) : null}
      {source ? (
        <View className="gap-2">
          <Text selectable className="text-sm font-t3-bold">
            #{source.epic.number} {source.epic.title}
          </Text>
          <Text className="text-xs text-foreground-muted">
            Closed issues are external prerequisites, never reported as verified T3Auto work.
          </Text>
          {source.tasks.map((task) => {
            const member = epic?.members.find(
              (candidate) => candidate.issueNodeId === task.issue.issueNodeId,
            );
            const child = readiness.snapshot?.runs.find(
              (run) => run.state.runId === member?.childRunId,
            );
            const stage = child?.stages
              .toSorted(
                (a, b) => a.stageOrdinal - b.stageOrdinal || a.attemptOrdinal - b.attemptOrdinal,
              )
              .at(-1);
            return (
              <View key={task.issue.issueNodeId} className="gap-1 rounded-xl bg-card p-3">
                <Text className="text-sm font-t3-bold">
                  #{task.issue.number} {task.issue.title} · {member?.status ?? task.issue.state}
                  {member?.taskId && member.taskId === epic?.activeTaskId ? " · Active task" : ""}
                </Text>
                {task.dependencies.length ? (
                  <Text className="text-xs text-foreground-muted">
                    Requires{" "}
                    {task.dependencies.map((dependency) => `#${dependency.number}`).join(", ")}
                  </Text>
                ) : null}
                {member?.baseCommitSha ? (
                  <Text selectable className="text-xs">
                    Starting commit: {member.baseCommitSha}
                  </Text>
                ) : null}
                {member?.accepted ? (
                  <Text selectable className="text-xs">
                    Accepted commit: {member.accepted.commitSha} · Evidence:{" "}
                    {member.accepted.evidenceId}
                  </Text>
                ) : null}
                {stage?.threadId ? (
                  <View className="gap-2">
                    <Action
                      onPress={() =>
                        navigation.navigate("Thread", { environmentId, threadId: stage.threadId! })
                      }
                    >
                      Open task thread
                    </Action>
                    <Action
                      onPress={() =>
                        navigation.navigate("ThreadReview", {
                          environmentId,
                          threadId: stage.threadId!,
                        })
                      }
                    >
                      Open task changes
                    </Action>
                  </View>
                ) : null}
                {member?.childRunId ? (
                  <Action onPress={() => void openSavedRun(member.childRunId!)}>
                    Open task run and evidence
                  </Action>
                ) : null}
                {member?.childRunId ? (
                  <Text selectable className="text-xs text-foreground-muted">
                    Run {member.childRunId} · Stage and check evidence below in Runs.
                  </Text>
                ) : null}
              </View>
            );
          })}
        </View>
      ) : null}
      {epic ? (
        <View className="gap-2">
          <Text accessibilityRole="summary" className="text-sm font-t3-bold">
            {agentControlEpicStatus(epic).label}
          </Text>
          <Text selectable className="text-xs">
            Epic run: {epic.epicRunId}
          </Text>
          <Text className="text-sm">
            {epic.members.filter((member) => member.status === "accepted").length} accepted ·{" "}
            {epic.members.filter((member) => member.status === "external-closed").length} externally
            closed · {epic.members.length} total
          </Text>
          {epic.externalPrerequisites?.length ? (
            <Text className="text-xs text-foreground-muted">
              External prerequisites observed closed:{" "}
              {epic.externalPrerequisites
                .map((item) => `#${item.issueNumber} (${item.observedAt})`)
                .join(", ")}
              . These are not T3-verified results.
            </Text>
          ) : null}
          {epic.acceptedCommitSha ? (
            <Text selectable className="text-xs">
              Common commit: {epic.acceptedCommitSha}
            </Text>
          ) : null}
          {epic.blockers.map((blocker) => (
            <Text
              key={`${blocker.code}:${blocker.issueNumber}`}
              className="text-sm text-destructive"
            >
              {blocker.message}
            </Text>
          ))}
          {!readOnly && readiness.modeChangeBlocker ? (
            <Text className="text-sm text-foreground-muted">{readiness.modeChangeBlocker}</Text>
          ) : null}
          {!readOnly ? (
            <>
              {epic.status !== "stopped" && epic.status !== "succeeded" ? (
                <>
                  <Action
                    disabled={!agentControlEpicControlAllowed(readiness, "resume")}
                    onPress={() => void execute("resume")}
                  >
                    Resume Epic
                  </Action>
                  <Action
                    disabled={!agentControlEpicControlAllowed(readiness, "stop")}
                    onPress={() => void execute("stop")}
                  >
                    End Epic
                  </Action>
                </>
              ) : (
                <Action
                  disabled={!agentControlEpicControlAllowed(readiness, "clear")}
                  onPress={() => void execute("clear")}
                >
                  Return to ordinary tasks
                </Action>
              )}
              <Text className="text-xs text-foreground-muted">
                Automation off pauses new task starts. Ending retains evidence and prevents further
                Epic work. Turn off automation before returning to ordinary tasks.
              </Text>
            </>
          ) : null}
          {epic.blockerHistory.length ? (
            <View className="gap-1">
              <Text className="text-sm font-t3-bold">Previous blockers</Text>
              {epic.blockerHistory.map((entry) => (
                <Text key={JSON.stringify(entry)} className="text-xs">
                  {entry.recordedAt}: {entry.blockers.map((blocker) => blocker.message).join("; ")}
                </Text>
              ))}
            </View>
          ) : null}
          {verifications.map((verification) => (
            <View key={verification.evidenceId} className="gap-2">
              <Text className="text-sm font-t3-bold">
                {verification === epic.finalVerification
                  ? "Common result verification"
                  : "Previous common result verification"}{" "}
                · {verification.status}
              </Text>
              <Text className="text-sm">{verification.detail}</Text>
              <Text selectable className="text-xs">
                Checked commit: {verification.commitSha} · Evidence: {verification.evidenceId}
              </Text>
              {verification.checks.map((check) => (
                <View key={check.id} className="gap-1 rounded-xl bg-card p-3">
                  <Text className="text-sm font-t3-bold">
                    {check.id} · {check.required ? "Required" : "Optional"} · {check.status}
                  </Text>
                  <Text selectable className="text-xs">
                    {[check.command, ...check.args].join(" ")} · {check.cwd}
                  </Text>
                  <Text className="text-xs">
                    Exit code: {check.exitCode ?? "Unavailable"} ·{" "}
                    {check.completedAt ?? "No completion recorded"}
                  </Text>
                  {check.output ? (
                    <Text selectable className="text-xs">
                      {check.output}
                    </Text>
                  ) : null}
                </View>
              ))}
            </View>
          ))}
          {!epic.finalVerification ? (
            <Text className="text-sm text-foreground-muted">
              Common result verification has not completed.
            </Text>
          ) : null}
        </View>
      ) : null}
      {!readOnly && readiness.snapshot?.epicHistory?.length ? (
        <View className="gap-2">
          <Text className="text-sm font-t3-bold">Previous Epics</Text>
          {readiness.snapshot.epicHistory.map((saved) => (
            <View key={saved.epicRunId} className="gap-2">
              <Action
                onPress={() =>
                  setSavedEpicId(savedEpicId === saved.epicRunId ? null : saved.epicRunId)
                }
              >
                {`#${saved.source.epic.number} ${saved.source.epic.title} · ${agentControlEpicStatus(saved).label}`}
              </Action>
              {savedEpicId === saved.epicRunId ? (
                <AutonomousEpicPanel
                  environmentId={environmentId}
                  projectId={projectId}
                  readOnly
                  readiness={{
                    ...readiness,
                    snapshot: { ...readiness.snapshot!, epic: saved, epicHistory: [] },
                  }}
                  onRefresh={onRefresh}
                  renderRun={renderRun}
                />
              ) : null}
            </View>
          ))}
        </View>
      ) : null}
      {loadedRun ? renderRun(loadedRun) : null}
      {error ? (
        <Text accessibilityRole="alert" className="text-sm text-destructive">
          {error}
        </Text>
      ) : null}
    </View>
  );
}
