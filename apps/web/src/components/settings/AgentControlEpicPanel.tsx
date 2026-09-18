import { useEffect, useEffectEvent, useRef, useState } from "react";
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
  type AgentControlReadiness,
} from "@t3tools/client-runtime/state/agent-control";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type {
  AgentControlEpicPreview,
  AgentControlEpicHandoffPreview,
  AgentControlEpicRuntimeView,
  EnvironmentId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import { agentControlEnvironment } from "../../state/agentControl";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";

type Props = {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  readiness: AgentControlReadiness;
  handoffPermissionBlocker: string | null;
  onRefresh: () => void;
  onOpenRun: (runId: string) => void;
  onOpenThread: (threadId: ThreadId, changes?: boolean) => void;
  readOnly?: boolean;
};

export function AgentControlEpicPanel({
  environmentId,
  projectId,
  readiness,
  handoffPermissionBlocker,
  onRefresh,
  onOpenRun,
  onOpenThread,
  readOnly = false,
}: Props) {
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
  const inspect = useAtomCommand(agentControlEnvironment.epicPreview, { reportFailure: false });
  const start = useAtomCommand(agentControlEnvironment.epicStart, { reportFailure: false });
  const resume = useAtomCommand(agentControlEnvironment.epicResume, { reportFailure: false });
  const stop = useAtomCommand(agentControlEnvironment.epicStop, { reportFailure: false });
  const clear = useAtomCommand(agentControlEnvironment.epicClear, { reportFailure: false });
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
    <section className="space-y-3 rounded-md border p-3" aria-label="Epic execution">
      {!readOnly ? (
        <>
          <h3 className="text-sm font-medium">Execute a GitHub Epic</h3>
          <p className="text-sm text-muted-foreground">
            Inspect native GitHub sub-issues and dependencies before starting. One level in this
            repository is supported. Reviewed independent tasks can run concurrently in separate
            worktrees. Each result is integrated and checked before dependents start.
          </p>
          <div className="flex flex-wrap gap-2">
            <Input
              className="w-40"
              aria-label="Epic issue number"
              inputMode="numeric"
              placeholder="Epic issue number"
              value={number}
              disabled={readiness.pending}
              onChange={(event) => {
                setNumber(event.target.value);
                setPreview(null);
              }}
            />
            <Button
              size="sm"
              variant="outline"
              disabled={!canInspect}
              onClick={() => void execute("preview")}
            >
              Inspect Epic
            </Button>
          </div>
          {preview ? (
            <div className="space-y-2">
              <p className="text-sm font-medium">
                Preview: #{preview.source.epic.number} {preview.source.epic.title}
              </p>
              <label className="block text-sm">
                Concurrent tasks (1–4)
                <Input
                  aria-label="Concurrent Epic tasks"
                  type="number"
                  min={1}
                  max={4}
                  value={parallelism}
                  disabled={readiness.pending}
                  onChange={(event) => setParallelism(event.target.value)}
                  className="mt-1 w-24"
                />
              </label>
              <div className="space-y-2">
                <label className="flex items-start gap-2 text-sm">
                  <input
                    type="checkbox"
                    checked={reviewed}
                    disabled={readiness.pending}
                    onChange={(event) => setReviewed(event.target.checked)}
                  />
                  {limit === 1 ? "Use reviewed dependency plan (optional). " : ""}
                  I reviewed every task: the shown dependencies are complete, and tasks with no
                  dependency path between them can safely run independently.
                </label>
                {limit > 1 || reviewed ? (
                  <Input
                    aria-label="Parallel task independence rationale"
                    placeholder="Why can unrelated tasks run independently?"
                    value={rationale}
                    disabled={readiness.pending}
                    onChange={(event) => setRationale(event.target.value)}
                  />
                ) : null}
                <p className="text-xs text-muted-foreground">
                  Missing edges alone do not prove independence. Update GitHub dependencies and
                  inspect again if needed. This approval is frozen when started or queued.
                </p>
              </div>
              {executionBlocker ? (
                <p className="text-sm text-amber-600">{executionBlocker}</p>
              ) : null}
              {previewMessages.map((blocker) => (
                <p key={blocker} className="text-sm text-amber-600">
                  {blocker}
                </p>
              ))}
              <Button
                size="sm"
                disabled={approvalBlockers.length > 0 || executionBlocker !== null}
                onClick={() => void editQueue("approve")}
              >
                Approve for Epic queue
              </Button>
              {approvalBlockers.map((blocker) => (
                <p key={blocker} className="text-sm text-amber-600">
                  {blocker}
                </p>
              ))}
              {!queue ? (
                <>
                  <Button
                    size="sm"
                    disabled={blockers.length > 0 || executionBlocker !== null}
                    onClick={() => void execute("start")}
                  >
                    Start inspected Epic
                  </Button>
                  {blockers.map((blocker) => (
                    <p key={blocker} className="text-xs text-muted-foreground">
                      Single Epic start: {blocker}
                    </p>
                  ))}
                </>
              ) : null}
            </div>
          ) : null}
        </>
      ) : null}
      {!readOnly ? (
        <p className="text-xs text-muted-foreground">
          Approve each inspected Epic to add it to the ordered queue. The first approval includes
          the already selected Epic as active. Only waiting entries can be removed or reordered.
          Serial mode waits for publication and human merge. Parallel mode requires a reviewed
          project dependency plan; dependent tasks still wait for merged results.
        </p>
      ) : null}
      {queue ? (
        <section className="space-y-2" aria-label="Approved Epic queue">
          <h3 className="text-sm font-medium">Approved Epic queue</h3>
          {!readOnly && readiness.snapshot ? (
            <ProjectEpicPlan
              key={queue.maxActiveEpics + ":" + readiness.snapshot.epicQueue?.revision}
              environmentId={environmentId}
              readiness={readiness}
              onRefresh={onRefresh}
            />
          ) : null}
          {!readOnly ? (
            <div className="space-y-1">
              <Button
                size="sm"
                variant="outline"
                disabled={leaveBlockers.length > 0}
                onClick={() => void editQueue("leave")}
              >
                Leave Epic queue
              </Button>
              <p className="text-xs text-muted-foreground">
                Leaving ends the remaining Epics and returns to ordinary tasks. Run history,
                verification and PR links are retained.
              </p>
              {leaveBlockers.map((message) => (
                <p key={message} className="text-xs text-muted-foreground">
                  {message}
                </p>
              ))}
            </div>
          ) : null}
          <p className="text-sm">
            Active:{" "}
            {queue.activeEntries.length
              ? queue.activeEntries
                  .map((entry) => `#${entry.source.epic.number} ${entry.source.epic.title}`)
                  .join(", ")
              : "None"}
          </p>
          <p className="text-sm">
            Next candidate:{" "}
            {queue.next
              ? `#${queue.next.source.epic.number} ${queue.next.source.epic.title}`
              : "None"}
          </p>
          {queue.waitReason ? (
            <p role="status" className="text-sm text-amber-600">
              {queue.waitReason}
            </p>
          ) : null}
          {queue.nextCheckAt ? (
            <p className="text-xs text-muted-foreground">
              While Armed, the server checks the PR about once a minute. Candidates are rechecked
              before starting.
            </p>
          ) : null}
          {queue.entries.length === 0 ? (
            <p className="text-sm">The queue is empty. Approve an Epic to continue.</p>
          ) : null}
          <ol className="space-y-2">
            {queue.entries.map((entry, index) => (
              <li key={entry.entryId} className="space-y-1 rounded border p-2 text-sm">
                <a
                  href={entry.source.epic.url}
                  target="_blank"
                  rel="noreferrer"
                  className="underline"
                >
                  {index + 1}. #{entry.source.epic.number} {entry.source.epic.title}
                </a>
                <p>
                  {entry.status === "pending"
                    ? "Waiting"
                    : entry.status === "active"
                      ? "Active"
                      : entry.status === "stopped"
                        ? "Stopped"
                        : "Merged"}
                  {queue.next?.entryId === entry.entryId ? " · Next candidate" : ""}
                </p>
                {entry.blockers.map((blocker) => (
                  <p key={`${blocker.code}:${blocker.message}`} className="text-amber-600">
                    {blocker.message}
                  </p>
                ))}
                {!readOnly && entry.status === "pending" ? (
                  <div className="flex flex-wrap gap-2">
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={
                        queueBlockers.length > 0 ||
                        !readiness.snapshot ||
                        !agentControlEpicQueueMoveInput(readiness.snapshot, entry.entryId, -1)
                      }
                      onClick={() => void editQueue("up", entry.entryId)}
                    >
                      Move up
                    </Button>
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={
                        queueBlockers.length > 0 ||
                        !readiness.snapshot ||
                        !agentControlEpicQueueMoveInput(readiness.snapshot, entry.entryId, 1)
                      }
                      onClick={() => void editQueue("down", entry.entryId)}
                    >
                      Move down
                    </Button>
                    <Button
                      size="xs"
                      variant="outline"
                      disabled={queueBlockers.length > 0}
                      onClick={() => void editQueue("remove", entry.entryId)}
                    >
                      Remove
                    </Button>
                  </div>
                ) : null}
              </li>
            ))}
          </ol>
          {!readOnly
            ? queueBlockers.map((blocker) => (
                <p key={blocker} className="text-xs text-muted-foreground">
                  {blocker}
                </p>
              ))
            : null}
        </section>
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
          <section
            key={epic?.epicRunId ?? `preview:${source.fingerprint}`}
            className="space-y-3 rounded border p-3"
          >
            {source ? (
              <>
                <a
                  href={source.epic.url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-sm underline"
                >
                  #{source.epic.number} {source.epic.title}
                </a>
                <p className="text-xs text-muted-foreground">
                  Closed issues are external prerequisites, never reported as verified T3Auto work.
                </p>
                <ol className="space-y-2 text-sm" aria-label="Epic sub-task progress">
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
                      <li key={task.issue.issueNodeId} className="space-y-1 rounded border p-2">
                        <p>
                          #{task.issue.number} {task.issue.title} ·{" "}
                          {member?.status ?? task.issue.state}
                          {member?.status === "running" ? " · Active task" : ""}
                        </p>
                        {progress ? <p role="status">{progress.label}</p> : null}
                        {progress?.threadId ? (
                          <div className="flex gap-2">
                            <Button
                              size="xs"
                              variant="outline"
                              onClick={() => onOpenThread(progress.threadId!)}
                            >
                              Open task thread
                            </Button>
                            <Button
                              size="xs"
                              variant="outline"
                              onClick={() => onOpenThread(progress.threadId!, true)}
                            >
                              Open task changes
                            </Button>
                          </div>
                        ) : null}
                        {dependencyLabels.length ? (
                          <p className="text-muted-foreground">
                            Requires {dependencyLabels.join(", ")}
                          </p>
                        ) : (
                          <p className="text-xs text-muted-foreground">
                            {approvedDependencies
                              ? "No dependencies (reviewed)"
                              : "No declared dependencies · independence not reviewed"}
                          </p>
                        )}
                        {member?.baseCommitSha ? (
                          <p className="break-all text-xs">
                            Starting commit: {member.baseCommitSha}
                          </p>
                        ) : null}
                        {member?.accepted ? (
                          <p className="break-all text-xs">
                            Accepted commit: {member.accepted.commitSha} · Evidence:{" "}
                            {member.accepted.evidenceId}
                          </p>
                        ) : null}
                        {member?.childRunId ? (
                          <Button
                            size="xs"
                            variant="outline"
                            onClick={() => onOpenRun(member.childRunId!)}
                          >
                            Open task run and evidence
                          </Button>
                        ) : null}
                      </li>
                    );
                  })}
                </ol>
              </>
            ) : null}
            {epic ? (
              <div className="space-y-2">
                <p role="status" className="text-sm font-medium">
                  {agentControlEpicStatus(epic).label}
                </p>
                <p className="break-all text-xs">Epic run: {epic.epicRunId}</p>
                <p className="text-xs">Concurrent task limit: {epic.parallelism ?? 1}</p>
                {epic.dependencyPlan ? (
                  <p className="text-xs">Approved independence: {epic.dependencyPlan.rationale}</p>
                ) : null}
                <p className="text-sm">
                  {epic.members.filter((member) => member.status === "accepted").length} accepted ·{" "}
                  {epic.members.filter((member) => member.status === "external-closed").length}{" "}
                  externally closed · {epic.members.length} total
                </p>
                {epic.externalPrerequisites?.length ? (
                  <p className="text-xs text-muted-foreground">
                    External prerequisites observed closed:{" "}
                    {epic.externalPrerequisites
                      .map((item) => `#${item.issueNumber} (${item.observedAt})`)
                      .join(", ")}
                    . These are not T3-verified results.
                  </p>
                ) : null}
                {epic.acceptedCommitSha ? (
                  <p className="break-all text-xs">Common commit: {epic.acceptedCommitSha}</p>
                ) : null}
                {epic.blockers.map((blocker) => (
                  <p
                    key={`${blocker.code}:${blocker.issueNumber}`}
                    className="text-sm text-amber-600"
                  >
                    {blocker.message}
                  </p>
                ))}
                {!readOnly && readiness.modeChangeBlocker ? (
                  <p className="text-sm text-muted-foreground">{readiness.modeChangeBlocker}</p>
                ) : null}
                {!readOnly ? (
                  <>
                    <div className="flex flex-wrap gap-2">
                      {epic.status !== "stopped" &&
                      (epic.status !== "succeeded" ||
                        agentControlEpicControlAllowed(readiness, "stop", epic.epicRunId)) ? (
                        <>
                          {epic.status !== "succeeded" ? (
                            <Button
                              size="sm"
                              disabled={
                                !agentControlEpicControlAllowed(readiness, "resume", epic.epicRunId)
                              }
                              onClick={() => void execute("resume", epic)}
                            >
                              Resume Epic
                            </Button>
                          ) : null}
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={
                              !agentControlEpicControlAllowed(readiness, "stop", epic.epicRunId)
                            }
                            onClick={() => void execute("stop", epic)}
                          >
                            {agentControlEpicStopPresentation(readiness.snapshot, epic).label}
                          </Button>
                        </>
                      ) : (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={
                            !agentControlEpicControlAllowed(readiness, "clear", epic.epicRunId)
                          }
                          onClick={() => void execute("clear", epic)}
                        >
                          Return to ordinary tasks
                        </Button>
                      )}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      {agentControlEpicStopPresentation(readiness.snapshot, epic).explanation}
                    </p>
                  </>
                ) : null}
                {epic.blockerHistory.length ? (
                  <details className="text-sm">
                    <summary>Previous blockers</summary>
                    {epic.blockerHistory.map((entry) => (
                      <p key={JSON.stringify(entry)}>
                        {entry.recordedAt}:{" "}
                        {entry.blockers.map((blocker) => blocker.message).join("; ")}
                      </p>
                    ))}
                  </details>
                ) : null}
                {verifications.map((verification) => (
                  <details
                    key={verification.evidenceId}
                    open={verification === epic.finalVerification}
                    className="space-y-2 text-sm"
                  >
                    <summary>
                      {verification === epic.finalVerification
                        ? "Common result verification"
                        : "Previous common result verification"}{" "}
                      · {verification.status}
                    </summary>
                    <p>{verification.detail}</p>
                    <p className="break-all text-xs">
                      Checked commit: {verification.commitSha} · Evidence: {verification.evidenceId}
                    </p>
                    {verification.checks.map((check) => (
                      <details key={check.id} className="rounded border p-2">
                        <summary>
                          {check.id} · {check.required ? "Required" : "Optional"} · {check.status}
                        </summary>
                        <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all text-xs">
                          {[check.command, ...check.args].join(" ")}
                          {`\nDirectory: ${check.cwd}\nExit code: ${check.exitCode ?? "unavailable"}\nCompleted: ${check.completedAt ?? "unavailable"}\n${check.output ?? "No captured output"}`}
                        </pre>
                      </details>
                    ))}
                  </details>
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
                  onRefresh={onRefresh}
                />
                {!epic.finalVerification ? (
                  <p className="text-sm text-muted-foreground">
                    Common result verification has not completed.
                  </p>
                ) : null}
              </div>
            ) : null}
          </section>
        );
      })}
      {!readOnly && readiness.snapshot?.epicHistory?.length ? (
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Previous Epics</h3>
          {readiness.snapshot.epicHistory.map((saved) => (
            <details key={saved.epicRunId}>
              <summary className="cursor-pointer text-sm">
                #{saved.source.epic.number} {saved.source.epic.title} ·{" "}
                {agentControlEpicStatus(saved).label}
              </summary>
              <AgentControlEpicPanel
                environmentId={environmentId}
                projectId={projectId}
                readOnly
                handoffPermissionBlocker={handoffPermissionBlocker}
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
                onOpenRun={onOpenRun}
                onOpenThread={onOpenThread}
              />
            </details>
          ))}
        </div>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
    </section>
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
    <div className="space-y-2">
      <p>
        Active Epic limit: {queue.maxActiveEpics ?? 1}. Task limits apply separately per Epic; all
        work shares host and provider capacity.
      </p>
      <Input
        aria-label="Maximum active Epics"
        value={limit}
        onChange={(event) => setLimit(event.target.value)}
      />
      {Number(limit) > 1 ? (
        <div className="space-y-2">
          <p>
            Review the complete graph before enabling parallel Epics. Existing dependencies cannot
            be removed here. Prerequisites across Epics require human merge and verified integration
            into the dependent task base.
          </p>
          {queue.entries.map((entry) => (
            <div key={entry.entryId} className="space-y-2">
              <p>
                Epic #{entry.source.epic.number}: {entry.source.epic.title}
              </p>
              {entry.source.tasks.map((task) => {
                const edges = agentControlEpicProjectTaskDependencies(
                  snapshot,
                  task.issue.issueNodeId,
                );
                return (
                  <div key={task.issue.issueNodeId} className="space-y-2">
                    <p>
                      Task #{task.issue.number}: {task.issue.title}. Requires:{" "}
                      {edges.length
                        ? edges
                            .map(
                              (id) =>
                                `#${tasks.find((item) => item.issue.issueNodeId === id)?.issue.number ?? id}`,
                            )
                            .join(", ")
                        : "none declared (review required)"}
                    </p>
                    <Input
                      aria-label={`Additional prerequisites for task #${task.issue.number}`}
                      placeholder="Additional issue numbers, comma separated"
                      value={additions[task.issue.issueNodeId] ?? ""}
                      onChange={(event) => {
                        setReviewed(false);
                        setAdditions((current) => ({
                          ...current,
                          [task.issue.issueNodeId]: event.target.value,
                        }));
                      }}
                    />
                  </div>
                );
              })}
            </div>
          ))}
          <Input
            aria-label="Cross-Epic independence rationale"
            placeholder="Why can unrelated work proceed independently?"
            value={rationale}
            onChange={(event) => setRationale(event.target.value)}
          />
          <label className="flex gap-2 text-sm">
            <input
              type="checkbox"
              checked={reviewed}
              onChange={(event) => setReviewed(event.target.checked)}
            />
            I reviewed every Epic and all task dependencies; unrelated tasks can safely run
            independently.
          </label>
        </div>
      ) : null}
      <Button disabled={blockers.length > 0} onClick={() => void save()}>
        Save Epic parallelism and reviewed plan
      </Button>
      {blockers.map((blocker) => (
        <p key={blocker}>{blocker}</p>
      ))}
      {error ? <p>{error}</p> : null}
    </div>
  );
}

type HandoffProps = {
  environmentId: EnvironmentId;
  epic: AgentControlEpicRuntimeView;
  connected: boolean;
  pending: boolean;
  permissionBlocker: string | null;
  onRefresh: () => void;
};

function EpicHandoff({
  environmentId,
  epic: savedEpic,
  connected,
  pending,
  permissionBlocker,
  onRefresh,
}: HandoffProps) {
  const [preview, setPreview] = useState<AgentControlEpicHandoffPreview | null>(null);
  const [observed, setObserved] = useState<AgentControlEpicRuntimeView | null>(null);
  const [busy, setBusy] = useState<"inspect" | "publish" | "refresh" | null>(null);
  const [error, setError] = useState<string | null>(null);
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
  const pullRequest = handoff?.pullRequest;
  const target = handoff ?? preview;

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
    <section className="space-y-2 rounded border p-3" aria-label="Epic review handoff">
      <h4 className="text-sm font-medium">Human review</h4>
      {target ? (
        <div className="space-y-1 text-xs">
          <p>Repository: {target.repository.nameWithOwner}</p>
          <p className="break-all">Target branch: {target.targetBranch ?? "Unavailable"}</p>
          <p className="break-all">Verified commit: {target.commitSha ?? "Unavailable"}</p>
          <p className="break-all">
            Review branch: {target.branchName ?? "Assigned when publication starts"}
          </p>
        </div>
      ) : (
        <p className="text-sm text-muted-foreground">
          Review the repository, target branch and verified common commit before publishing a Draft
          PR.
        </p>
      )}
      {busy || handoff?.status === "publishing" ? (
        <p role="status" className="text-sm">
          {busy === "refresh"
            ? "Refreshing PR state…"
            : busy === "inspect"
              ? "Checking publication…"
              : "Handoff in progress…"}
        </p>
      ) : null}
      {pullRequest ? (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <a
              href={pullRequest.url}
              target="_blank"
              rel="noreferrer"
              className="text-sm underline"
            >
              Open {pullRequest.isDraft && pullRequest.state === "open" ? "Draft PR" : "PR"} #
              {pullRequest.number} · {pullRequest.state}
            </a>
            <Button
              size="sm"
              variant="outline"
              disabled={!connected || busy !== null}
              onClick={() => void execute("refresh")}
            >
              Refresh PR state
            </Button>
          </div>
          {preview?.blockers
            .filter((blocker) => blocker.code !== "handoff-pr-unavailable")
            .map((blocker) => (
              <p key={blocker.code} role="alert" className="text-sm text-amber-600">
                {blocker.message}
              </p>
            ))}
        </>
      ) : (
        <>
          {(preview ? publishBlockers : blockers).map((blocker) => (
            <p key={blocker} className="text-sm text-amber-600">
              {blocker}
            </p>
          ))}
          <div className="flex flex-wrap gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={blockers.length > 0}
              onClick={() => void execute("inspect")}
            >
              {handoff || error ? "Review and retry handoff" : "Review publication"}
            </Button>
            {preview ? (
              <Button
                size="sm"
                disabled={publishBlockers.length > 0}
                onClick={() => void execute("publish")}
              >
                {handoff ? "Retry Draft PR handoff" : "Create Draft PR"}
              </Button>
            ) : null}
          </div>
        </>
      )}
      {handoff?.error ? (
        <p role="alert" className="text-sm text-destructive">
          {handoff.error.message}
        </p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
    </section>
  );
}
