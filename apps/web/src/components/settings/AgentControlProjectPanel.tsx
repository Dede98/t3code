import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  agentControlRunStatus,
  agentControlEndBlockedRunInput,
  agentControlCanEndBlockedRun,
  agentControlModeChangeBlocker,
  agentControlSnapshotReady,
  agentControlStartBlockers,
  agentControlStartInput,
  agentControlStageHeading,
  agentControlVerificationPassed,
} from "@t3tools/client-runtime/state/agent-control";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  CommandId,
  type AgentControlPreflightRuntimeResult,
  type AgentControlTaskId,
  type EnvironmentId,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import { DEFAULT_RESOLVED_KEYBINDINGS } from "@t3tools/shared/keybindings";
import { useNavigate } from "@tanstack/react-router";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useRef, useState } from "react";

import { writeTextToClipboard } from "../../hooks/useCopyToClipboard";
import { useRightPanelStore } from "../../rightPanelStore";
import { agentControlEnvironment } from "../../state/agentControl";
import { useEnvironment } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { OpenInPicker } from "../chat/OpenInPicker";
import { Button } from "../ui/button";
import { Select, SelectItem, SelectPopup, SelectTrigger, SelectValue } from "../ui/select";
import { SettingsSection } from "./settingsLayout";

function PreflightDetails({ preflight }: { preflight: AgentControlPreflightRuntimeResult }) {
  return (
    <details className="text-sm" open={!preflight.ok}>
      <summary className="cursor-pointer">Provider and model configuration</summary>
      <ul className="mt-2 space-y-2">
        {preflight.roles.map((role) => (
          <li key={role.role}>
            <span className="font-medium capitalize">{role.role}</span>
            <span className="text-muted-foreground"> · {role.accessMode}</span>
            {role.candidates.map((candidate) => (
              <div key={candidate.candidateIndex} className="break-words text-muted-foreground">
                {candidate.providerInstanceId} / {candidate.model}
                {candidate.candidateIndex === role.selectedCandidateIndex ? " · Selected" : ""}
                {candidate.errorCode ? ` · ${candidate.errorCode}` : ""}
              </div>
            ))}
            {role.errorCode ? <p className="text-destructive">{role.errorCode}</p> : null}
          </li>
        ))}
      </ul>
      {!preflight.staticPreflight.ok ? (
        <ul className="mt-2 text-destructive">
          {preflight.staticPreflight.errors.map((error) => (
            <li key={JSON.stringify(error)}>
              {error.role}: {error.code}
            </li>
          ))}
        </ul>
      ) : null}
    </details>
  );
}

export function AgentControlProjectPanel({
  environmentId,
  projectId,
  workspaceRoot,
}: {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  workspaceRoot: string;
}) {
  const target = { environmentId, input: { projectId } };
  const snapshotResult = useAtomValue(agentControlEnvironment.snapshot(target));
  const preflightResult = useAtomValue(agentControlEnvironment.preflight(target));
  const policyResult = useAtomValue(agentControlEnvironment.policy(target));
  const refreshPreflight = useAtomRefresh(agentControlEnvironment.preflight(target));
  const refreshPolicy = useAtomRefresh(agentControlEnvironment.policy(target));
  const refreshSnapshot = useAtomRefresh(agentControlEnvironment.snapshot(target));
  const snapshot = Option.getOrNull(AsyncResult.value(snapshotResult));
  const preflight = Option.getOrNull(AsyncResult.value(preflightResult));
  const policy = Option.getOrNull(AsyncResult.value(policyResult));
  const config = useAtomValue(serverEnvironment.configValueAtom(environmentId));
  const environment = useEnvironment(environmentId);
  const sessionResult = useAtomValue(environmentSession.sessionStateAtom(environmentId));
  const refreshSession = useAtomRefresh(environmentSession.sessionStateAtom(environmentId));
  const modeChangeBlocker = agentControlModeChangeBlocker(sessionResult);
  const connected = environment?.connection.phase === "connected";
  const setMode = useAtomCommand(agentControlEnvironment.setMode, "start autonomous task");
  const navigate = useNavigate();
  const [taskId, setTaskId] = useState<AgentControlTaskId | null>(null);
  const [pending, setPending] = useState(false);
  const requestPending = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const selectedTaskId = taskId ?? snapshot?.nextTaskId ?? null;
  const selectedTask = snapshot?.tasks.find((task) => task.taskId === selectedTaskId);
  const selectedRun = snapshot?.runs[0];
  const canChangeIntake =
    modeChangeBlocker === null &&
    agentControlSnapshotReady(snapshotResult, connected) &&
    !pending &&
    !snapshot?.runs.some((run) => run.state.status === "active");
  const blockers = agentControlStartBlockers({
    policy: policyResult._tag === "Success" && !policyResult.waiting ? policy : null,
    snapshot,
    preflight: preflightResult._tag === "Success" && !preflightResult.waiting ? preflight : null,
    selectedTaskId,
    connected: agentControlSnapshotReady(snapshotResult, connected),
    pending,
    modeChangeBlocker,
  });
  const endBlockedRunInput = agentControlEndBlockedRunInput({
    snapshot,
    connected: agentControlSnapshotReady(snapshotResult, connected),
    pending,
    modeChangeBlocker,
  });

  const changeMode = async (mode: "manual" | "observe" | "run-once", endBlocked = false) => {
    if (!snapshot || requestPending.current || modeChangeBlocker !== null) return;
    if (mode === "run-once" && (blockers.length > 0 || !selectedTaskId)) return;
    if (endBlocked ? endBlockedRunInput === null : mode !== "run-once" && !canChangeIntake) return;
    if (mode === "manual" && snapshot.projectState.mode !== "observe") return;
    requestPending.current = true;
    setPending(true);
    setError(null);
    try {
      const input =
        endBlocked && endBlockedRunInput
          ? endBlockedRunInput
          : mode === "run-once" && selectedTaskId
            ? agentControlStartInput(snapshot, selectedTaskId)
            : {
                projectId,
                commandId: CommandId.make(
                  `t3auto-${mode}:${JSON.stringify([projectId, snapshot.projectState.revision])}`,
                ),
                expectedRevision: snapshot.projectState.revision,
                mode,
              };
      const result = await setMode({ environmentId, input });
      if (result._tag === "Failure") {
        const failure = squashAtomCommandFailure(result);
        setError(
          failure instanceof Error
            ? failure.message
            : "The request failed. Reconnect and check the saved run before trying again.",
        );
      }
    } finally {
      requestPending.current = false;
      setPending(false);
    }
  };

  const openThread = (threadId: ThreadId, changes = false) => {
    if (changes)
      useRightPanelStore.getState().open(scopeThreadRef(environmentId, threadId), "diff");
    void navigate({ to: "/$environmentId/$threadId", params: { environmentId, threadId } });
  };

  return (
    <SettingsSection title="Autonomous task" id="project-autonomous-task">
      <div className="space-y-4 px-3 py-4 sm:px-4">
        <p className="text-sm text-muted-foreground">
          Start one task, follow its progress, and review its checked changes.
          <span className="block break-all">
            {environment?.label ?? environmentId} · {workspaceRoot}
          </span>
        </p>
        {!connected ? (
          <p role="status" className="text-sm text-amber-600">
            Disconnected. Showing the last saved state; reconnect to receive progress.
          </p>
        ) : null}
        {snapshotResult._tag === "Failure" ? (
          <p role="alert" className="text-sm text-destructive">
            Could not load autonomous tasks from this environment.
          </p>
        ) : null}
        {!snapshot ? (
          <p className="text-sm text-muted-foreground">Loading saved tasks and runs…</p>
        ) : (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <Select
                value={selectedTaskId ?? ""}
                onValueChange={(value) => {
                  const task = snapshot.tasks.find((candidate) => candidate.taskId === value);
                  if (task) setTaskId(task.taskId);
                }}
                disabled={pending}
              >
                <SelectTrigger className="max-w-full" size="sm" aria-label="Autonomous task">
                  <SelectValue>
                    {selectedTask
                      ? `#${selectedTask.source.issueNumber} ${selectedTask.title}`
                      : "Choose a task"}
                  </SelectValue>
                </SelectTrigger>
                <SelectPopup>
                  {snapshot.tasks.map((task) => (
                    <SelectItem key={task.taskId} value={task.taskId}>
                      <span className="whitespace-normal">
                        #{task.source.issueNumber} {task.title} · {task.status}
                      </span>
                    </SelectItem>
                  ))}
                </SelectPopup>
              </Select>
              <Button
                size="sm"
                disabled={blockers.length > 0}
                onClick={() => void changeMode("run-once")}
              >
                {pending ? "Submitting…" : "Run once"}
              </Button>
              {snapshot.projectState.mode === "manual" ||
              snapshot.projectState.mode === "paused" ? (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canChangeIntake}
                  onClick={() => void changeMode("observe")}
                >
                  Enable task intake
                </Button>
              ) : null}
              {snapshot.projectState.mode === "observe" ? (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canChangeIntake}
                  onClick={() => void changeMode("manual")}
                >
                  Disable task intake
                </Button>
              ) : null}
              {snapshot.projectState.mode === "run-once" &&
              snapshot.runs.some(agentControlCanEndBlockedRun) ? (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={endBlockedRunInput === null}
                  onClick={() => void changeMode("observe", true)}
                >
                  End blocked run
                </Button>
              ) : null}
              <Button
                size="sm"
                variant="outline"
                disabled={!connected || pending || preflightResult.waiting}
                onClick={() => {
                  refreshPreflight();
                  refreshPolicy();
                  refreshSnapshot();
                  refreshSession();
                }}
              >
                Check readiness again
              </Button>
            </div>
            {snapshot.projectState.mode === "run-once" &&
            snapshot.runs.some(agentControlCanEndBlockedRun) ? (
              <p className="text-sm text-muted-foreground">
                Ending the run prevents further automatic steps and keeps its failure history. After
                fixing the cause, remove the old issue from ready intake in GitHub and start a new
                eligible task.
              </p>
            ) : null}
            {snapshot.tasks.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No tasks have been imported. Check the project’s GitHub intake configuration and
                ready label.
              </p>
            ) : null}
            {blockers.length > 0 ? (
              <ul className="space-y-1 text-sm text-muted-foreground" aria-label="Start blockers">
                {blockers.map((blocker) => (
                  <li key={blocker}>{blocker}</li>
                ))}
              </ul>
            ) : null}
          </>
        )}
        {error ? (
          <p role="alert" className="break-words text-sm text-destructive">
            {error}
          </p>
        ) : null}
        {preflight ? (
          <PreflightDetails preflight={preflight} />
        ) : (
          <p className="text-sm text-muted-foreground">
            {preflightResult._tag === "Failure"
              ? "Provider preflight unavailable. Check the environment connection."
              : "Checking providers and models…"}
          </p>
        )}
        {policy?.projectPolicy?.policy.verificationChecks?.length ? (
          <details className="text-sm">
            <summary className="cursor-pointer">Configured verification checks</summary>
            <ul className="mt-2 space-y-1">
              {policy.projectPolicy.policy.verificationChecks.map((check) => (
                <li key={check.id} className="break-all">
                  {check.id}
                  {check.required ? " (required)" : ""}:{" "}
                  <code>{[check.command, ...check.args].join(" ")}</code> · {check.cwd}
                </li>
              ))}
            </ul>
          </details>
        ) : (
          <p className="text-sm text-muted-foreground">
            Project verification checks are not configured or could not be loaded.
          </p>
        )}
        {selectedRun ? (
          <div className="space-y-3 border-t pt-4">
            <p className="text-sm text-muted-foreground">
              Latest saved run · {new Date(selectedRun.state.updatedAt).toLocaleString()}
            </p>
            <div>
              <h3 className="font-medium">
                {selectedRun.task?.title ?? "Run without an eligible task"}
              </h3>
              <p role="status" className="text-sm">
                {agentControlRunStatus(selectedRun).label}
              </p>
              <p className="break-all text-xs text-muted-foreground">
                Run {selectedRun.state.runId}
              </p>
            </div>
            <ol className="space-y-3" aria-label="Run progress">
              {selectedRun.stages.map((stage) => (
                <li key={stage.stageRunId} className="space-y-2 rounded-md border p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <h4 className="text-sm font-medium">
                      {agentControlStageHeading(stage, selectedRun.stages)} ·{" "}
                      {stage.displayStage === "verification" &&
                      stage.status === "succeeded" &&
                      !agentControlVerificationPassed(stage)
                        ? "Evidence unavailable"
                        : stage.status}
                    </h4>
                    {stage.threadId ? (
                      <div className="flex gap-2">
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() => openThread(stage.threadId!)}
                        >
                          Open thread
                        </Button>
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() => openThread(stage.threadId!, true)}
                        >
                          View changes
                        </Button>
                      </div>
                    ) : null}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Updated {new Date(stage.updatedAt).toLocaleString()}
                  </p>
                  {stage.errorCode ? (
                    <p className="text-sm text-destructive">
                      {stage.errorCode}. Open the thread to inspect the failure and any pending
                      action.
                    </p>
                  ) : null}
                  {stage.displayStage === "verification" ? (
                    <div className="space-y-2 text-sm">
                      <p>
                        {agentControlVerificationPassed(stage)
                          ? "Verification passed · required checks passed"
                          : stage.verification?.verdict === "failed"
                            ? "Verification failed"
                            : "Verification has no complete passing evidence"}
                      </p>
                      {stage.verification?.errorCode ? (
                        <p className="text-destructive">{stage.verification.errorCode}</p>
                      ) : null}
                      {stage.verification?.checks.map((check) => (
                        <details key={check.id} className="rounded border px-2 py-1">
                          <summary className="cursor-pointer break-words">
                            {check.id}
                            {check.required ? " (required)" : ""} · {check.status}
                            {check.exitCode !== null ? ` · exit ${check.exitCode}` : ""}
                          </summary>
                          <pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-all text-xs">
                            {[check.command, ...check.args].join(" ")}
                            {`\nDirectory: ${check.cwd}`}
                            {check.completedAt
                              ? `\nCompleted: ${new Date(check.completedAt).toLocaleString()}`
                              : "\nNo completion recorded."}
                            {check.output ? `\n\n${check.output}` : "\nNo captured output."}
                          </pre>
                        </details>
                      ))}
                    </div>
                  ) : null}
                  {stage.worktreePath ? (
                    <div className="space-y-2">
                      <p className="break-all text-xs text-muted-foreground">
                        {stage.branch ? `${stage.branch} · ` : ""}
                        {stage.worktreePath}
                      </p>
                      <div className="flex flex-wrap gap-2">
                        <Button
                          size="xs"
                          variant="outline"
                          onClick={() =>
                            void writeTextToClipboard(stage.worktreePath!, "worktree path").catch(
                              () =>
                                setError(
                                  "Could not copy the worktree path. Select and copy the path above.",
                                ),
                            )
                          }
                        >
                          Copy worktree path
                        </Button>
                        <OpenInPicker
                          environmentId={environmentId}
                          keybindings={config?.keybindings ?? DEFAULT_RESOLVED_KEYBINDINGS}
                          availableEditors={config?.availableEditors ?? []}
                          openInCwd={stage.worktreePath}
                          enableShortcut={false}
                          compact
                        />
                      </div>
                    </div>
                  ) : null}
                </li>
              ))}
            </ol>
          </div>
        ) : null}
      </div>
    </SettingsSection>
  );
}
