import { useEffect, useEffectEvent, useRef, useState } from "react";
import {
  agentControlCommandErrorMessage,
  agentControlEpicControlAllowed,
  agentControlEpicControlInput,
  agentControlEpicStartBlockers,
  agentControlEpicStartInput,
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
  readOnly?: boolean;
};

export function AgentControlEpicPanel({
  environmentId,
  projectId,
  readiness,
  handoffPermissionBlocker,
  onRefresh,
  onOpenRun,
  readOnly = false,
}: Props) {
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
  const inspect = useAtomCommand(agentControlEnvironment.epicPreview, { reportFailure: false });
  const start = useAtomCommand(agentControlEnvironment.epicStart, { reportFailure: false });
  const resume = useAtomCommand(agentControlEnvironment.epicResume, { reportFailure: false });
  const stop = useAtomCommand(agentControlEnvironment.epicStop, { reportFailure: false });
  const clear = useAtomCommand(agentControlEnvironment.epicClear, { reportFailure: false });
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
    <section className="space-y-3 rounded-md border p-3" aria-label="Epic execution">
      {!readOnly ? (
        <>
          <h3 className="text-sm font-medium">Execute a GitHub Epic</h3>
          <p className="text-sm text-muted-foreground">
            Inspect native GitHub sub-issues and dependencies before starting. One level in this
            repository is supported. Each accepted task becomes the next task’s starting point. The
            common result receives a final verification.
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
              {preview.source !== source ? (
                <ol className="space-y-1 text-sm">
                  {preview.source.tasks.map((task) => (
                    <li key={task.issue.issueNodeId}>
                      #{task.issue.number} {task.issue.title} · {task.issue.state}
                      {task.dependencies.length
                        ? ` · Requires ${task.dependencies.map((dependency) => `#${dependency.number}`).join(", ")}`
                        : ""}
                    </li>
                  ))}
                </ol>
              ) : null}
              {previewMessages.map((blocker) => (
                <p key={blocker} className="text-sm text-amber-600">
                  {blocker}
                </p>
              ))}
              <Button
                size="sm"
                disabled={blockers.length > 0}
                onClick={() => void execute("start")}
              >
                Start inspected Epic
              </Button>
            </div>
          ) : null}
        </>
      ) : null}
      {source ? (
        <>
          <a href={source.epic.url} target="_blank" rel="noreferrer" className="text-sm underline">
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
              return (
                <li key={task.issue.issueNodeId} className="space-y-1 rounded border p-2">
                  <p>
                    #{task.issue.number} {task.issue.title} · {member?.status ?? task.issue.state}
                    {member?.taskId && member.taskId === epic?.activeTaskId ? " · Active task" : ""}
                  </p>
                  {task.dependencies.length ? (
                    <p className="text-muted-foreground">
                      Requires{" "}
                      {task.dependencies.map((dependency) => `#${dependency.number}`).join(", ")}
                    </p>
                  ) : null}
                  {member?.baseCommitSha ? (
                    <p className="break-all text-xs">Starting commit: {member.baseCommitSha}</p>
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
          <p className="text-sm">
            {epic.members.filter((member) => member.status === "accepted").length} accepted ·{" "}
            {epic.members.filter((member) => member.status === "external-closed").length} externally
            closed · {epic.members.length} total
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
            <p key={`${blocker.code}:${blocker.issueNumber}`} className="text-sm text-amber-600">
              {blocker.message}
            </p>
          ))}
          {!readOnly && readiness.modeChangeBlocker ? (
            <p className="text-sm text-muted-foreground">{readiness.modeChangeBlocker}</p>
          ) : null}
          {!readOnly ? (
            <>
              <div className="flex flex-wrap gap-2">
                {epic.status !== "stopped" && epic.status !== "succeeded" ? (
                  <>
                    <Button
                      size="sm"
                      disabled={!agentControlEpicControlAllowed(readiness, "resume")}
                      onClick={() => void execute("resume")}
                    >
                      Resume Epic
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={!agentControlEpicControlAllowed(readiness, "stop")}
                      onClick={() => void execute("stop")}
                    >
                      End Epic
                    </Button>
                  </>
                ) : (
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!agentControlEpicControlAllowed(readiness, "clear")}
                    onClick={() => void execute("clear")}
                  >
                    Return to ordinary tasks
                  </Button>
                )}
              </div>
              <p className="text-xs text-muted-foreground">
                Automation off pauses new task starts. Ending retains evidence and prevents further
                Epic work. Turn off automation before returning to ordinary tasks.
              </p>
            </>
          ) : null}
          {epic.blockerHistory.length ? (
            <details className="text-sm">
              <summary>Previous blockers</summary>
              {epic.blockerHistory.map((entry) => (
                <p key={JSON.stringify(entry)}>
                  {entry.recordedAt}: {entry.blockers.map((blocker) => blocker.message).join("; ")}
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
            key={JSON.stringify([environmentId, projectId, epic.epicRunId, readiness.connected])}
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
                  snapshot: { ...readiness.snapshot!, epic: saved, epicHistory: [] },
                }}
                onRefresh={onRefresh}
                onOpenRun={onOpenRun}
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
