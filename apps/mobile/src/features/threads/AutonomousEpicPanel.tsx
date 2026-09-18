import { useEffect, useEffectEvent, useRef, useState, type ReactNode } from "react";
import { Pressable, View } from "react-native";
import { useNavigation } from "@react-navigation/native";
import {
  agentControlCommandErrorMessage,
  agentControlEpicQueueChangeBlockers,
  agentControlEpicQueueLeaveBlockers,
  agentControlEpicQueueApproveBlockers,
  agentControlEpicQueueChangeInput,
  agentControlEpicQueueMoveInput,
  agentControlEpicQueueView,
  agentControlEpicRuns,
  agentControlEpicDependencyLabel,
  agentControlEpicProjectPlan,
  agentControlEpicProjectTaskDependencies,
  agentControlEpicProjectPlanAdditions,
  agentControlEpicControlAllowed,
  agentControlEpicStopPresentation,
  agentControlEpicControlInput,
  agentControlEpicStartBlockers,
  agentControlEpicStartInput,
  agentControlEpicExecutionOptions,
  agentControlEpicMemberProgress,
  agentControlEpicStatus,
  agentControlEpicHandoffBlockers,
  agentControlEpicPublishHandoffInput,
  agentControlEpicReviewReworkBlockers,
  agentControlEpicReviewReworkInput,
  agentControlEpicReviewReworkStatus,
  type AgentControlReadiness,
} from "@t3tools/client-runtime/state/agent-control";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  AgentControlRunOnceId,
  ThreadId,
  type AgentControlRunOnceView,
  type AgentControlEpicPreview,
  type AgentControlEpicHandoffPreview,
  type AgentControlEpicRuntimeView,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";
import { agentControlEnvironment } from "../../state/agent-control";
import { useAtomCommand } from "../../state/use-atom-command";
import { tryOpenExternalUrl } from "../../lib/openExternalUrl";

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
  handoffPermissionBlocker: string | null;
  reviewReworkPermissionBlocker: string | null;
  onRefresh: () => void;
  readOnly?: boolean;
  renderRun: (run: AgentControlRunOnceView) => ReactNode;
};

export function AutonomousEpicPanel({
  environmentId,
  projectId,
  readiness,
  handoffPermissionBlocker,
  reviewReworkPermissionBlocker,
  onRefresh,
  readOnly = false,
  renderRun,
}: Props) {
  const navigation = useNavigation();
  const [loadedRun, setLoadedRun] = useState<AgentControlRunOnceView | null>(null);
  const requestedRunId = useRef<string | null>(null);
  const [savedEpicId, setSavedEpicId] = useState<string | null>(null);
  const [number, setNumber] = useState("");
  const [parallelism, setParallelism] = useState("1");
  const [rationale, setRationale] = useState("");
  const [reviewed, setReviewed] = useState(false);
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
  const changeQueue = useAtomCommand(agentControlEnvironment.epicQueueChange);
  const queue = agentControlEpicQueueView(readiness.snapshot);
  const leaveBlockers = agentControlEpicQueueLeaveBlockers(readiness);
  const queueBlockers = agentControlEpicQueueChangeBlockers(readiness);
  const approvalBlockers = agentControlEpicQueueApproveBlockers(readiness, preview);
  const epics = agentControlEpicRuns(readiness.snapshot);
  const blockers = agentControlEpicStartBlockers(readiness, preview);
  const previewMessages = [
    ...new Set([
      ...(preview?.blockers.map((blocker) => blocker.message) ?? []),
      ...(preview?.source.blockers.map((blocker) => blocker.message) ?? []),
    ]),
  ];
  const limit = Number(parallelism);
  const executionBlocker =
    !Number.isInteger(limit) || limit < 1 || limit > 4
      ? "Choose a parallel task limit from 1 to 4."
      : (limit > 1 || reviewed) && (!reviewed || !rationale.trim())
        ? "Review the complete dependency graph and explain why unrelated tasks can run independently."
        : null;
  const executionOptions = preview
    ? agentControlEpicExecutionOptions(preview, limit, rationale, reviewed)
    : {};
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

  async function editQueue(
    action: "approve" | "remove" | "up" | "down" | "leave",
    entryId?: string,
  ) {
    const snapshot = readiness.snapshot;
    if (!snapshot || readOnly || inFlight.current || queueBlockers.length > 0) return;
    if (
      action === "approve" &&
      (!preview || approvalBlockers.length > 0 || executionBlocker !== null)
    )
      return;
    if (action === "leave" && leaveBlockers.length > 0) return;
    if (
      action !== "approve" &&
      action !== "leave" &&
      !queue?.entries.some((entry) => entry.entryId === entryId && entry.status === "pending")
    )
      return;
    const input =
      action === "leave"
        ? agentControlEpicQueueChangeInput(snapshot, { kind: "leave" })
        : action === "approve" && preview
          ? agentControlEpicQueueChangeInput(snapshot, {
              kind: "approve",
              epicNumber: preview.source.epic.number,
              expectedFingerprint: preview.source.fingerprint,
              ...executionOptions,
            })
          : action === "remove" && entryId
            ? agentControlEpicQueueChangeInput(snapshot, { kind: "remove", entryId })
            : entryId
              ? agentControlEpicQueueMoveInput(snapshot, entryId, action === "up" ? -1 : 1)
              : null;
    if (!input) return;
    inFlight.current = true;
    setError(null);
    try {
      const result = await changeQueue({ environmentId, input });
      if (!mounted.current) return;
      if (result._tag === "Failure")
        setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      else if (action === "approve") setPreview(null);
    } finally {
      inFlight.current = false;
      if (mounted.current) onRefresh();
    }
  }

  async function execute(
    action: "preview" | "start" | "resume" | "stop" | "clear",
    epic?: AgentControlEpicRuntimeView,
  ) {
    if (readOnly || inFlight.current || readiness.pending || !readiness.connected) return;
    if (
      action === "preview"
        ? !canInspect
        : action === "start"
          ? blockers.length > 0 || executionBlocker !== null
          : !agentControlEpicControlAllowed(readiness, action, epic?.epicRunId)
    )
      return;
    inFlight.current = true;
    setError(null);
    try {
      if (action === "preview") {
        setPreview(null);
        setReviewed(false);
        setRationale("");
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
                input: agentControlEpicStartInput(readiness.snapshot, preview, executionOptions),
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
            repository is supported. Reviewed independent tasks can run concurrently in separate
            worktrees. Each result is integrated and checked before dependents start.
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
              <Text className="text-sm">Concurrent tasks (1–4)</Text>
              <TextInput
                accessibilityLabel="Concurrent Epic tasks"
                keyboardType="number-pad"
                value={parallelism}
                editable={!readiness.pending}
                onChangeText={setParallelism}
                className="rounded-lg border border-border-subtle bg-card p-3 text-foreground"
              />
              <View className="gap-2">
                <Action disabled={readiness.pending} onPress={() => setReviewed(!reviewed)}>
                  {reviewed
                    ? "Dependency review confirmed — undo"
                    : limit === 1
                      ? "Use reviewed dependency plan (optional)"
                      : "Confirm complete dependency review"}
                </Action>
                <Text className="text-xs text-foreground-muted">
                  I reviewed every task: the shown dependencies are complete, and tasks with no
                  dependency path between them can safely run independently.
                </Text>
                {limit > 1 || reviewed ? (
                  <TextInput
                    accessibilityLabel="Parallel task independence rationale"
                    placeholder="Why can unrelated tasks run independently?"
                    value={rationale}
                    editable={!readiness.pending}
                    onChangeText={setRationale}
                    className="rounded-lg border border-border-subtle bg-card p-3 text-foreground"
                  />
                ) : null}
                <Text className="text-xs text-foreground-muted">
                  Missing edges alone do not prove independence. Update GitHub dependencies and
                  inspect again if needed. This approval is frozen when started or queued.
                </Text>
              </View>
              {executionBlocker ? (
                <Text className="text-sm text-destructive">{executionBlocker}</Text>
              ) : null}
              {previewMessages.map((blocker) => (
                <Text key={blocker} className="text-sm text-destructive">
                  {blocker}
                </Text>
              ))}
              <Action
                disabled={approvalBlockers.length > 0 || executionBlocker !== null}
                onPress={() => void editQueue("approve")}
              >
                Approve for Epic queue
              </Action>
              {approvalBlockers.map((blocker) => (
                <Text key={blocker} className="text-sm text-destructive">
                  {blocker}
                </Text>
              ))}
              {!queue ? (
                <>
                  <Action
                    disabled={blockers.length > 0 || executionBlocker !== null}
                    onPress={() => void execute("start")}
                  >
                    Start inspected Epic
                  </Action>
                  {blockers.map((blocker) => (
                    <Text key={blocker} className="text-xs text-foreground-muted">
                      Single Epic start: {blocker}
                    </Text>
                  ))}
                </>
              ) : null}
            </View>
          ) : null}
        </>
      ) : null}
      {!readOnly ? (
        <Text className="text-xs text-foreground-muted">
          Approve each inspected Epic to add it to the ordered queue. The first approval includes
          the already selected Epic as active. Only waiting entries can be removed or reordered.
          Serial mode waits for publication and human merge. Parallel mode requires a reviewed
          project dependency plan; dependent tasks still wait for merged results.
        </Text>
      ) : null}
      {queue ? (
        <View className="gap-2">
          <Text className="text-base font-t3-bold">Approved Epic queue</Text>
          {!readOnly && readiness.snapshot ? (
            <ProjectEpicPlan
              key={queue.maxActiveEpics + ":" + readiness.snapshot.epicQueue?.revision}
              environmentId={environmentId}
              readiness={readiness}
              onRefresh={onRefresh}
            />
          ) : null}
          {!readOnly ? (
            <View className="gap-1">
              <Action disabled={leaveBlockers.length > 0} onPress={() => void editQueue("leave")}>
                Leave Epic queue
              </Action>
              <Text className="text-xs text-foreground-muted">
                Leaving ends the remaining Epics and returns to ordinary tasks. Run history,
                verification and PR links are retained.
              </Text>
              {leaveBlockers.map((message) => (
                <Text key={message} className="text-xs text-foreground-muted">
                  {message}
                </Text>
              ))}
            </View>
          ) : null}
          <Text className="text-sm">
            Active:{" "}
            {queue.activeEntries.length
              ? queue.activeEntries
                  .map((entry) => `#${entry.source.epic.number} ${entry.source.epic.title}`)
                  .join(", ")
              : "None"}
          </Text>
          <Text className="text-sm">
            Next candidate:{" "}
            {queue.next
              ? `#${queue.next.source.epic.number} ${queue.next.source.epic.title}`
              : "None"}
          </Text>
          {queue.waitReason ? (
            <Text className="text-sm text-foreground-muted">{queue.waitReason}</Text>
          ) : null}
          {queue.nextCheckAt ? (
            <Text className="text-xs text-foreground-muted">
              While Armed, the server checks the PR about once a minute. Candidates are rechecked
              before starting.
            </Text>
          ) : null}
          {queue.entries.length === 0 ? (
            <Text className="text-sm">The queue is empty. Approve an Epic to continue.</Text>
          ) : null}
          {queue.entries.map((entry, index) => (
            <View key={entry.entryId} className="gap-2 rounded-xl border border-border-subtle p-3">
              <Text className="text-sm font-t3-bold">
                {index + 1}. #{entry.source.epic.number} {entry.source.epic.title}
              </Text>
              <Text className="text-sm">
                {entry.status === "pending"
                  ? "Waiting"
                  : entry.status === "active"
                    ? "Active"
                    : entry.status === "stopped"
                      ? "Stopped"
                      : "Merged"}
                {queue.next?.entryId === entry.entryId ? " · Next candidate" : ""}
              </Text>
              {entry.blockers.map((blocker) => (
                <Text
                  key={`${blocker.code}:${blocker.message}`}
                  className="text-sm text-foreground-muted"
                >
                  {blocker.message}
                </Text>
              ))}
              {!readOnly && entry.status === "pending" ? (
                <View className="gap-2">
                  <Action
                    disabled={
                      queueBlockers.length > 0 ||
                      !readiness.snapshot ||
                      !agentControlEpicQueueMoveInput(readiness.snapshot, entry.entryId, -1)
                    }
                    onPress={() => void editQueue("up", entry.entryId)}
                  >
                    Move up
                  </Action>
                  <Action
                    disabled={
                      queueBlockers.length > 0 ||
                      !readiness.snapshot ||
                      !agentControlEpicQueueMoveInput(readiness.snapshot, entry.entryId, 1)
                    }
                    onPress={() => void editQueue("down", entry.entryId)}
                  >
                    Move down
                  </Action>
                  <Action
                    disabled={queueBlockers.length > 0}
                    onPress={() => void editQueue("remove", entry.entryId)}
                  >
                    Remove
                  </Action>
                </View>
              ) : null}
            </View>
          ))}
          {!readOnly
            ? queueBlockers.map((blocker) => (
                <Text key={blocker} className="text-xs text-foreground-muted">
                  {blocker}
                </Text>
              ))
            : null}
        </View>
      ) : null}
      {[
        ...epics.map((epic) => ({ epic, source: epic.source })),
        ...(preview ? [{ epic: null, source: preview.source }] : []),
      ].map(({ epic, source }) => {
        const verifications = epic
          ? [
              ...epic.finalVerificationHistory.filter(
                (verification) => verification.evidenceId !== epic.finalVerification?.evidenceId,
              ),
              ...(epic.finalVerification ? [epic.finalVerification] : []),
            ]
          : [];
        return (
          <View key={epic?.epicRunId ?? `preview:${source.fingerprint}`} className="gap-3">
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
                  const approvedDependencies =
                    epic?.projectDependencyPlan?.tasks.find(
                      (entry) => entry.issueNodeId === task.issue.issueNodeId,
                    )?.dependsOn ??
                    epic?.dependencyPlan?.tasks.find(
                      (entry) => entry.issueNodeId === task.issue.issueNodeId,
                    )?.dependsOn;
                  const dependencyLabels = approvedDependencies
                    ? approvedDependencies.map((id) =>
                        agentControlEpicDependencyLabel(readiness.snapshot, source, id),
                      )
                    : task.dependencies.map((dependency) => `#${dependency.number}`);
                  const progress = member
                    ? agentControlEpicMemberProgress(member, readiness.snapshot?.runs ?? [])
                    : null;
                  return (
                    <View key={task.issue.issueNodeId} className="gap-1 rounded-xl bg-card p-3">
                      <Text className="text-sm font-t3-bold">
                        #{task.issue.number} {task.issue.title} ·{" "}
                        {member?.status ?? task.issue.state}
                        {member?.status === "running" ? " · Active task" : ""}
                      </Text>
                      {progress ? <Text className="text-sm">{progress.label}</Text> : null}
                      {dependencyLabels.length ? (
                        <Text className="text-xs text-foreground-muted">
                          Requires {dependencyLabels.join(", ")}
                        </Text>
                      ) : (
                        <Text className="text-xs text-foreground-muted">
                          {approvedDependencies
                            ? "No dependencies (reviewed)"
                            : "No declared dependencies · independence not reviewed"}
                        </Text>
                      )}
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
                      {progress?.threadId ? (
                        <View className="gap-2">
                          <Action
                            onPress={() =>
                              navigation.navigate("Thread", {
                                environmentId,
                                threadId: progress.threadId!,
                              })
                            }
                          >
                            Open task thread
                          </Action>
                          <Action
                            onPress={() =>
                              navigation.navigate("ThreadReview", {
                                environmentId,
                                threadId: progress.threadId!,
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
                <Text className="text-xs">Concurrent task limit: {epic.parallelism ?? 1}</Text>
                {epic.dependencyPlan ? (
                  <Text className="text-xs">
                    Approved independence: {epic.dependencyPlan.rationale}
                  </Text>
                ) : null}
                <Text selectable className="text-xs">
                  Epic run: {epic.epicRunId}
                </Text>
                <Text className="text-sm">
                  {epic.members.filter((member) => member.status === "accepted").length} accepted ·{" "}
                  {epic.members.filter((member) => member.status === "external-closed").length}{" "}
                  externally closed · {epic.members.length} total
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
                  <Text className="text-sm text-foreground-muted">
                    {readiness.modeChangeBlocker}
                  </Text>
                ) : null}
                {!readOnly ? (
                  <>
                    {epic.status !== "stopped" &&
                    (epic.status !== "succeeded" ||
                      agentControlEpicControlAllowed(readiness, "stop", epic.epicRunId)) ? (
                      <>
                        {epic.status !== "succeeded" ? (
                          <Action
                            disabled={
                              !agentControlEpicControlAllowed(readiness, "resume", epic.epicRunId)
                            }
                            onPress={() => void execute("resume", epic)}
                          >
                            Resume Epic
                          </Action>
                        ) : null}
                        <Action
                          disabled={
                            !agentControlEpicControlAllowed(readiness, "stop", epic.epicRunId)
                          }
                          onPress={() => void execute("stop", epic)}
                        >
                          {agentControlEpicStopPresentation(readiness.snapshot, epic).label}
                        </Action>
                      </>
                    ) : (
                      <Action
                        disabled={
                          !agentControlEpicControlAllowed(readiness, "clear", epic.epicRunId)
                        }
                        onPress={() => void execute("clear", epic)}
                      >
                        Return to ordinary tasks
                      </Action>
                    )}
                    <Text className="text-xs text-foreground-muted">
                      {agentControlEpicStopPresentation(readiness.snapshot, epic).explanation}
                    </Text>
                  </>
                ) : null}
                {epic.blockerHistory.length ? (
                  <View className="gap-1">
                    <Text className="text-sm font-t3-bold">Previous blockers</Text>
                    {epic.blockerHistory.map((entry) => (
                      <Text key={JSON.stringify(entry)} className="text-xs">
                        {entry.recordedAt}:{" "}
                        {entry.blockers.map((blocker) => blocker.message).join("; ")}
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
                <EpicHandoff
                  key={JSON.stringify([
                    environmentId,
                    projectId,
                    epic.epicRunId,
                    readiness.connected,
                  ])}
                  environmentId={environmentId}
                  epic={epic}
                  connected={readiness.connected}
                  pending={readiness.pending}
                  permissionBlocker={handoffPermissionBlocker}
                  reviewReworkPermissionBlocker={reviewReworkPermissionBlocker}
                  onRefresh={onRefresh}
                  onOpenThread={(threadId, changes) => {
                    if (changes) navigation.navigate("ThreadReview", { environmentId, threadId });
                    else navigation.navigate("Thread", { environmentId, threadId });
                  }}
                />
                {!epic.finalVerification ? (
                  <Text className="text-sm text-foreground-muted">
                    Common result verification has not completed.
                  </Text>
                ) : null}
              </View>
            ) : null}
          </View>
        );
      })}
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
                  handoffPermissionBlocker={handoffPermissionBlocker}
                  reviewReworkPermissionBlocker={reviewReworkPermissionBlocker}
                  readiness={{
                    ...readiness,
                    snapshot: {
                      ...readiness.snapshot!,
                      epic: saved,
                      epics: [saved],
                      epicHistory: [],
                    },
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

function ProjectEpicPlan({
  environmentId,
  readiness,
  onRefresh,
}: Pick<Props, "environmentId" | "readiness" | "onRefresh">) {
  const snapshot = readiness.snapshot!;
  const queue = snapshot.epicQueue!;
  const [limit, setLimit] = useState(String(queue.maxActiveEpics ?? 1));
  const [rationale, setRationale] = useState(queue.projectDependencyPlan?.rationale ?? "");
  const [reviewed, setReviewed] = useState(false);
  const [additions, setAdditions] = useState<Record<string, string>>(() =>
    agentControlEpicProjectPlanAdditions(snapshot),
  );
  const [error, setError] = useState<string | null>(null);
  const inFlight = useRef(false);
  const changeQueue = useAtomCommand(agentControlEnvironment.epicQueueChange);
  const plan = agentControlEpicProjectPlan(snapshot, Number(limit), rationale, reviewed, additions);
  const blockers = [
    ...agentControlEpicQueueChangeBlockers(readiness),
    ...plan.blockers,
    ...(queue.entries.some((entry) => entry.status === "active")
      ? ["Finish or end all active Epics before changing the project plan or active Epic limit."]
      : []),
  ];
  const tasks = queue.entries.flatMap((entry) => entry.source.tasks);
  async function save() {
    if (inFlight.current || blockers.length) return;
    inFlight.current = true;
    try {
      const result = await changeQueue({
        environmentId,
        input: agentControlEpicQueueChangeInput(snapshot, {
          kind: "configure",
          maxActiveEpics: Number(limit),
          ...(plan.projectDependencyPlan
            ? { projectDependencyPlan: plan.projectDependencyPlan }
            : {}),
        }),
      });
      if (result._tag === "Failure")
        setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      else onRefresh();
    } finally {
      inFlight.current = false;
    }
  }
  return (
    <View className="gap-2">
      <Text>
        Active Epic limit: {queue.maxActiveEpics ?? 1}. Task limits apply separately per Epic; all
        work shares host and provider capacity.
      </Text>
      <TextInput accessibilityLabel="Maximum active Epics" value={limit} onChangeText={setLimit} />
      {Number(limit) > 1 ? (
        <View className="gap-2">
          <Text>
            Review the complete graph before enabling parallel Epics. Existing dependencies cannot
            be removed here. Prerequisites across Epics require human merge and verified integration
            into the dependent task base.
          </Text>
          {queue.entries.map((entry) => (
            <View key={entry.entryId} className="gap-2">
              <Text>
                Epic #{entry.source.epic.number}: {entry.source.epic.title}
              </Text>
              {entry.source.tasks.map((task) => {
                const edges = agentControlEpicProjectTaskDependencies(
                  snapshot,
                  task.issue.issueNodeId,
                );
                return (
                  <View key={task.issue.issueNodeId} className="gap-2">
                    <Text>
                      Task #{task.issue.number}: {task.issue.title}. Requires:{" "}
                      {edges.length
                        ? edges
                            .map(
                              (id) =>
                                `#${tasks.find((item) => item.issue.issueNodeId === id)?.issue.number ?? id}`,
                            )
                            .join(", ")
                        : "none declared (review required)"}
                    </Text>
                    <TextInput
                      accessibilityLabel={`Additional prerequisites for task #${task.issue.number}`}
                      placeholder="Additional issue numbers, comma separated"
                      value={additions[task.issue.issueNodeId] ?? ""}
                      onChangeText={(value) => {
                        setReviewed(false);
                        setAdditions((current) => ({
                          ...current,
                          [task.issue.issueNodeId]: value,
                        }));
                      }}
                    />
                  </View>
                );
              })}
            </View>
          ))}
          <TextInput
            accessibilityLabel="Cross-Epic independence rationale"
            placeholder="Why can unrelated work proceed independently?"
            value={rationale}
            onChangeText={setRationale}
          />
          <Action onPress={() => setReviewed(!reviewed)}>
            {reviewed
              ? "Dependency review confirmed"
              : "Confirm: I reviewed every Epic and all task dependencies"}
          </Action>
        </View>
      ) : null}
      <Action disabled={blockers.length > 0} onPress={() => void save()}>
        Save Epic parallelism and reviewed plan
      </Action>
      {blockers.map((blocker) => (
        <Text key={blocker}>{blocker}</Text>
      ))}
      {error ? <Text>{error}</Text> : null}
    </View>
  );
}

type HandoffProps = {
  environmentId: EnvironmentId;
  epic: AgentControlEpicRuntimeView;
  connected: boolean;
  pending: boolean;
  permissionBlocker: string | null;
  reviewReworkPermissionBlocker: string | null;
  onRefresh: () => void;
  onOpenThread: (threadId: ThreadId, changes?: boolean) => void;
};

type ReviewFindingDraft = {
  findingId: string;
  summary: string;
  correctionCriteria: string;
  acceptanceCriteria: string;
};

function EpicHandoff({
  environmentId,
  epic: savedEpic,
  connected,
  pending,
  permissionBlocker,
  reviewReworkPermissionBlocker,
  onRefresh,
  onOpenThread,
}: HandoffProps) {
  const [preview, setPreview] = useState<AgentControlEpicHandoffPreview | null>(null);
  const [observed, setObserved] = useState<AgentControlEpicRuntimeView | null>(null);
  const [busy, setBusy] = useState<"inspect" | "publish" | "refresh" | "rework" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const nextFindingId = useRef(2);
  const [findings, setFindings] = useState<ReviewFindingDraft[]>([
    {
      findingId: `${savedEpic.epicRunId}:review-finding:1`,
      summary: "",
      correctionCriteria: "",
      acceptanceCriteria: "",
    },
  ]);
  const mounted = useRef(true);
  const inFlight = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const inspect = useAtomCommand(agentControlEnvironment.epicPreviewHandoff, {
    reportFailure: false,
  });
  const publish = useAtomCommand(agentControlEnvironment.epicPublishHandoff, {
    reportFailure: false,
  });
  const requestReviewRework = useAtomCommand(agentControlEnvironment.epicRequestReviewRework, {
    reportFailure: false,
  });
  const epic = observed && observed.revision > savedEpic.revision ? observed : savedEpic;
  const handoff =
    preview?.handoff && (!epic.handoff || preview.handoff.updatedAt >= epic.handoff.updatedAt)
      ? preview.handoff
      : epic.handoff;
  const currentEpic = handoff ? { ...epic, handoff } : epic;
  const blockers = agentControlEpicHandoffBlockers({
    epic: currentEpic,
    connected,
    pending: pending || busy !== null,
    permissionBlocker,
  });
  const publishBlockers = agentControlEpicHandoffBlockers({
    epic: currentEpic,
    connected,
    pending: pending || busy !== null,
    permissionBlocker,
    preview,
  });
  const reviewReworkActive = epic.activeReviewReworkId != null;
  const handoffUpdateRequired = handoff?.status === "update-required";
  const pullRequest = reviewReworkActive ? null : handoff?.pullRequest;
  const target = reviewReworkActive ? null : (handoff ?? preview);
  const reviewReworkBlockers = agentControlEpicReviewReworkBlockers({
    epic,
    findings,
    connected,
    pending: pending || busy !== null,
    permissionBlocker: reviewReworkPermissionBlocker,
  });

  function updateFinding(
    findingId: string,
    field: "summary" | "correctionCriteria" | "acceptanceCriteria",
    value: string,
  ) {
    setFindings((current) =>
      current.map((finding) =>
        finding.findingId === findingId ? { ...finding, [field]: value } : finding,
      ),
    );
  }

  async function submitReviewRework() {
    if (inFlight.current || reviewReworkBlockers.length > 0) return;
    inFlight.current = true;
    setBusy("rework");
    setError(null);
    try {
      const result = await requestReviewRework({
        environmentId,
        input: agentControlEpicReviewReworkInput(epic, findings),
      });
      if (!mounted.current) return;
      if (result._tag === "Success") {
        setPreview(null);
        setObserved(result.value);
      } else setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      onRefresh();
    } catch (cause) {
      if (mounted.current) setError(agentControlCommandErrorMessage(cause));
    } finally {
      inFlight.current = false;
      if (mounted.current) setBusy(null);
    }
  }

  async function execute(action: "inspect" | "publish" | "refresh") {
    if (inFlight.current) return;
    if (action === "refresh" ? !connected || !pullRequest : blockers.length > 0) return;
    if (action === "publish" && (!preview || publishBlockers.length > 0)) return;
    inFlight.current = true;
    setBusy(action);
    setError(null);
    try {
      if (action === "inspect" || action === "refresh") {
        if (action === "inspect") setPreview(null);
        const result = await inspect({
          environmentId,
          input: { projectId: epic.projectId, epicRunId: epic.epicRunId },
        });
        if (!mounted.current) return;
        if (result._tag === "Success") setPreview(result.value);
        else setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
      } else if (preview) {
        const result = await publish({
          environmentId,
          input: agentControlEpicPublishHandoffInput(currentEpic, preview),
        });
        if (!mounted.current) return;
        setPreview(null);
        if (result._tag === "Success") setObserved(result.value);
        else setError(agentControlCommandErrorMessage(squashAtomCommandFailure(result)));
        onRefresh();
      }
    } catch (cause) {
      if (mounted.current) setError(agentControlCommandErrorMessage(cause));
    } finally {
      inFlight.current = false;
      if (mounted.current) setBusy(null);
    }
  }

  const refreshSavedPullRequest = useEffectEvent(() => void execute("refresh"));
  useEffect(() => {
    if (!connected || !pullRequest?.number) return;
    let active = true;
    queueMicrotask(() => {
      if (active) refreshSavedPullRequest();
    });
    return () => {
      active = false;
    };
  }, [connected, pullRequest?.number]);

  return (
    <View className="gap-2 rounded-xl border border-border-subtle p-3">
      <Text className="text-sm font-t3-bold">Human review</Text>
      {epic.reviewReworks?.length ? (
        <View className="gap-2">
          <Text className="text-sm font-t3-bold">Review repair history</Text>
          {epic.reviewReworks.map((rework) => (
            <View
              key={rework.requestId}
              className="gap-2 rounded-xl border border-border-subtle p-3"
            >
              <Text accessibilityRole="summary" className="text-sm font-t3-bold">
                {agentControlEpicReviewReworkStatus(rework).label}
              </Text>
              <Text selectable className="text-xs">
                Previously reviewed: {rework.reviewedCommitSha} · Evidence:{" "}
                {rework.reviewedVerificationEvidenceId}
              </Text>
              {rework.candidateCommitSha ? (
                <Text selectable className="text-xs">
                  Repair candidate: {rework.candidateCommitSha}
                </Text>
              ) : null}
              {rework.verification ? (
                <Text selectable className="text-xs">
                  {rework.verification.status === "passed"
                    ? "New verified commit"
                    : "Checked candidate"}
                  : {rework.verification.commitSha} · Evidence: {rework.verification.evidenceId}
                </Text>
              ) : null}
              {rework.blocker ? (
                <Text accessibilityRole="alert" className="text-sm text-destructive">
                  {rework.blocker.message}
                </Text>
              ) : null}
              {rework.findings.map((finding) => (
                <View key={finding.findingId} className="gap-1 rounded-xl bg-card p-3">
                  <Text className="text-sm font-t3-bold">{finding.summary}</Text>
                  <Text className="text-sm">Required correction: {finding.correctionCriteria}</Text>
                  <Text className="text-sm">Acceptance: {finding.acceptanceCriteria}</Text>
                </View>
              ))}
              {rework.repairAttempts.map((attempt) => (
                <View key={attempt.attempt} className="gap-2 rounded-xl bg-card p-3">
                  <Text className="text-sm">
                    Repair {attempt.attempt} · {attempt.status} · {attempt.providerInstanceId} /{" "}
                    {attempt.model}
                  </Text>
                  <Action onPress={() => onOpenThread(ThreadId.make(attempt.threadId))}>
                    Open repair thread
                  </Action>
                  <Action onPress={() => onOpenThread(ThreadId.make(attempt.threadId), true)}>
                    Open repair changes
                  </Action>
                  {attempt.error ? (
                    <Text accessibilityRole="alert" className="text-sm text-destructive">
                      {attempt.error.message}
                    </Text>
                  ) : null}
                </View>
              ))}
            </View>
          ))}
        </View>
      ) : null}
      {epic.handoffHistory?.length ? (
        <View className="gap-2">
          <Text className="text-sm font-t3-bold">Previous review handoffs</Text>
          {epic.handoffHistory.map((entry) => (
            <View
              key={entry.handoff.intentId}
              className="gap-1 rounded-xl border border-border-subtle p-3"
            >
              <Text selectable className="text-xs">
                Previous verified commit: {entry.handoff.commitSha} · Evidence:{" "}
                {entry.handoff.verificationEvidenceId}
              </Text>
              <Text className="text-xs text-foreground-muted">
                Superseded when review repair was requested at {entry.supersededAt}.
              </Text>
              {entry.handoff.pullRequest ? (
                <Action
                  onPress={() => {
                    void tryOpenExternalUrl(entry.handoff.pullRequest!.url, "pull-request");
                  }}
                >
                  {`Open previous PR #${entry.handoff.pullRequest.number} · ${entry.handoff.pullRequest.state}`}
                </Action>
              ) : null}
            </View>
          ))}
        </View>
      ) : null}
      {epic.status === "succeeded" && !reviewReworkActive ? (
        <View className="gap-2 rounded-xl border border-border-subtle p-3">
          <Text className="text-sm font-t3-bold">Report review findings</Text>
          <Text selectable className="text-xs">
            Findings apply to verified commit {epic.acceptedCommitSha ?? "Unavailable"}.
          </Text>
          {findings.map((finding, index) => (
            <View key={finding.findingId} className="gap-2 rounded-xl bg-card p-3">
              <Text className="text-sm font-t3-bold">Finding {index + 1}</Text>
              <TextInput
                accessibilityLabel={`Finding ${index + 1} summary`}
                placeholder="What is wrong?"
                multiline
                value={finding.summary}
                onChangeText={(value) => updateFinding(finding.findingId, "summary", value)}
                className="min-h-20"
              />
              <TextInput
                accessibilityLabel={`Finding ${index + 1} required correction`}
                placeholder="What must change?"
                multiline
                value={finding.correctionCriteria}
                onChangeText={(value) =>
                  updateFinding(finding.findingId, "correctionCriteria", value)
                }
                className="min-h-20"
              />
              <TextInput
                accessibilityLabel={`Finding ${index + 1} acceptance criteria`}
                placeholder="How can the correction be verified?"
                multiline
                value={finding.acceptanceCriteria}
                onChangeText={(value) =>
                  updateFinding(finding.findingId, "acceptanceCriteria", value)
                }
                className="min-h-20"
              />
              {findings.length > 1 ? (
                <Action
                  onPress={() =>
                    setFindings((current) =>
                      current.filter((item) => item.findingId !== finding.findingId),
                    )
                  }
                >
                  Remove finding
                </Action>
              ) : null}
            </View>
          ))}
          <Action
            disabled={findings.length >= 20}
            onPress={() => {
              const id = nextFindingId.current++;
              setFindings((current) => [
                ...current,
                {
                  findingId: `${epic.epicRunId}:review-finding:${id}`,
                  summary: "",
                  correctionCriteria: "",
                  acceptanceCriteria: "",
                },
              ]);
            }}
          >
            Add finding
          </Action>
          <Action
            disabled={reviewReworkBlockers.length > 0}
            onPress={() => void submitReviewRework()}
          >
            Request repair and re-verification
          </Action>
          {reviewReworkBlockers.map((blocker) => (
            <Text key={blocker} className="text-sm text-destructive">
              {blocker}
            </Text>
          ))}
        </View>
      ) : null}
      {reviewReworkActive ? (
        <Text className="text-sm text-foreground-muted">
          The previous handoff is historical while repair and re-verification are in progress.
        </Text>
      ) : target ? (
        <View className="gap-1">
          <Text selectable className="text-xs">
            Repository: {target.repository.nameWithOwner}
          </Text>
          <Text selectable className="text-xs">
            Target branch: {target.targetBranch ?? "Unavailable"}
          </Text>
          <Text selectable className="text-xs">
            Verified commit: {target.commitSha ?? "Unavailable"}
          </Text>
          <Text selectable className="text-xs">
            Review branch: {target.branchName ?? "Assigned when publication starts"}
          </Text>
        </View>
      ) : (
        <Text className="text-sm text-foreground-muted">
          Review the repository, target branch and verified common commit before publishing a Draft
          PR.
        </Text>
      )}
      {busy || handoff?.status === "publishing" ? (
        <Text accessibilityLiveRegion="polite" className="text-sm">
          {busy === "rework"
            ? "Submitting review findings…"
            : busy === "refresh"
              ? "Refreshing PR state…"
              : busy === "inspect"
                ? "Checking publication…"
                : "Handoff in progress…"}
        </Text>
      ) : null}
      {pullRequest && !handoffUpdateRequired ? (
        <>
          <Action
            onPress={() => {
              void tryOpenExternalUrl(pullRequest.url, "pull-request").then((opened) => {
                if (!opened && mounted.current)
                  setError("Could not open the pull request. Try opening it again.");
              });
            }}
          >
            {`Open ${pullRequest.isDraft && pullRequest.state === "open" ? "Draft PR" : "PR"} #${pullRequest.number} · ${pullRequest.state}`}
          </Action>
          <Action disabled={!connected || busy !== null} onPress={() => void execute("refresh")}>
            Refresh PR state
          </Action>
          {preview?.blockers
            .filter((blocker) => blocker.code !== "handoff-pr-unavailable")
            .map((blocker) => (
              <Text
                key={blocker.code}
                accessibilityRole="alert"
                className="text-sm text-destructive"
              >
                {blocker.message}
              </Text>
            ))}
        </>
      ) : (
        <>
          {(preview ? publishBlockers : blockers).map((blocker) => (
            <Text key={blocker} className="text-sm text-destructive">
              {blocker}
            </Text>
          ))}
          <Action disabled={blockers.length > 0} onPress={() => void execute("inspect")}>
            {handoffUpdateRequired
              ? "Review PR update"
              : handoff || error
                ? "Review and retry handoff"
                : "Review publication"}
          </Action>
          {preview ? (
            <Action disabled={publishBlockers.length > 0} onPress={() => void execute("publish")}>
              {handoffUpdateRequired
                ? "Update Draft PR"
                : handoff
                  ? "Retry Draft PR handoff"
                  : "Create Draft PR"}
            </Action>
          ) : null}
        </>
      )}
      {handoff?.error ? (
        <Text accessibilityRole="alert" className="text-sm text-destructive">
          {handoff.error.message}
        </Text>
      ) : null}
      {error ? (
        <Text accessibilityRole="alert" className="text-sm text-destructive">
          {error}
        </Text>
      ) : null}
    </View>
  );
}
