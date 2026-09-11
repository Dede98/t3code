import { useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import {
  agentControlEndPausedInput,
  agentControlEndPausedExplanation,
  agentControlCommandErrorMessage,
  agentControlRunStatus,
  agentControlArmedStatus,
  agentControlArmBlockers,
  agentControlArmInput,
  agentControlDisarmInput,
  agentControlArmedExplanation,
  agentControlDisarmExplanation,
  agentControlEndBlockedRunInput,
  agentControlCanEndBlockedRun,
  agentControlModeChangeBlocker,
  agentControlSnapshotReady,
  agentControlSnapshotFresh,
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
import { useEffect, useRef, useState } from "react";

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

type AgentControlProjectPanelProps = {
  environmentId: EnvironmentId;
  projectId: ProjectId;
  workspaceRoot: string;
};

export function AgentControlProjectPanel(props: AgentControlProjectPanelProps) {
  return (
    <AgentControlProjectPanelContent
      key={JSON.stringify([props.environmentId, props.projectId])}
      {...props}
    />
  );
}

function AgentControlProjectPanelContent({
  environmentId,
  projectId,
  workspaceRoot,
}: AgentControlProjectPanelProps) {
  const target = { environmentId, input: { projectId } };
  const snapshotResult = useAtomValue(agentControlEnvironment.snapshot(target));
  const commandPending = useAtomValue(agentControlEnvironment.pending(target));
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
  const connected = environment?.connection.phase === "connected";
  const setMode = useAtomCommand(agentControlEnvironment.setMode, {
    label: "change autonomous task mode",
    reportFailure: false,
  });
  const mounted = useRef(false);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const [freshness, setFreshness] = useState({
    connected,
    snapshot,
    preflightResult,
    policyResult,
    sessionResult,
  });
  if (freshness.connected !== connected) {
    setFreshness({ connected, snapshot, preflightResult, policyResult, sessionResult });
  }
  useEffect(() => {
    if (!connected) return;
    refreshSession();
    refreshPreflight();
    refreshPolicy();
    refreshSnapshot();
  }, [connected, refreshSession, refreshPreflight, refreshPolicy, refreshSnapshot]);
  const fresh =
    freshness.connected === connected &&
    sessionResult !== freshness.sessionResult &&
    agentControlSnapshotFresh(snapshotResult, freshness.snapshot, connected);
  const modeChangeBlocker =
    agentControlModeChangeBlocker(sessionResult) ??
    (!fresh ? "Checking the current environment state before allowing changes." : null);
  const navigate = useNavigate();
  const [taskId, setTaskId] = useState<AgentControlTaskId | null>(null);
  const [localPending, setPending] = useState(false);
  const pending = localPending || commandPending;
  const requestPending = useRef(false);
  const [error, setError] = useState<string | null>(null);
  const selectedTaskId = taskId ?? snapshot?.nextTaskId ?? null;
  const selectedTask = snapshot?.tasks.find((task) => task.taskId === selectedTaskId);
  const selectedRun =
    snapshot?.runs.find((run) => run.state.status === "active") ?? snapshot?.runs[0];
  const armedStatus = agentControlArmedStatus(snapshot);
  const canChangeIntake =
    modeChangeBlocker === null &&
    agentControlSnapshotReady(snapshotResult, connected) &&
    !pending &&
    !snapshot?.runs.some((run) => run.state.status === "active");
  const blockers = agentControlStartBlockers({
    policy:
      policyResult !== freshness.policyResult &&
      policyResult._tag === "Success" &&
      !policyResult.waiting
        ? policy
        : null,
    snapshot,
    preflight:
      preflightResult !== freshness.preflightResult &&
      preflightResult._tag === "Success" &&
      !preflightResult.waiting
        ? preflight
        : null,
    selectedTaskId,
    connected: agentControlSnapshotReady(snapshotResult, connected),
    pending,
    modeChangeBlocker,
  });
  const armedBlockers = agentControlArmBlockers({
    policy:
      policyResult !== freshness.policyResult &&
      policyResult._tag === "Success" &&
      !policyResult.waiting
        ? policy
        : null,
    snapshot,
    preflight:
      preflightResult !== freshness.preflightResult &&
      preflightResult._tag === "Success" &&
      !preflightResult.waiting
        ? preflight
        : null,
    connected: agentControlSnapshotReady(snapshotResult, connected),
    pending,
    modeChangeBlocker,
  });
  const disarmInput = agentControlDisarmInput({
    snapshot,
    connected: agentControlSnapshotReady(snapshotResult, connected),
    pending,
    modeChangeBlocker,
  });
  const endPausedInput = agentControlEndPausedInput({
    snapshot,
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

  const changeMode = async (
    mode: "manual" | "observe" | "run-once" | "armed",
    action?: "end-blocked" | "disarm" | "end-paused",
  ) => {
    if (!snapshot || pending || requestPending.current || modeChangeBlocker !== null) return;
    if (mode === "run-once" && (blockers.length > 0 || !selectedTaskId)) return;
    if (mode === "armed" && armedBlockers.length > 0) return;
    if (action === "end-blocked" && endBlockedRunInput === null) return;
    if (action === "disarm" && disarmInput === null) return;
    if (action === "end-paused" && endPausedInput === null) return;
    if (!action && mode !== "run-once" && mode !== "armed" && !canChangeIntake) return;
    if (mode === "manual" && action !== "end-paused" && snapshot.projectState.mode !== "observe")
      return;
    requestPending.current = true;
    setPending(true);
    setError(null);
    try {
      const input =
        action === "end-paused" && endPausedInput
          ? endPausedInput
          : action === "disarm" && disarmInput
            ? disarmInput
            : action === "end-blocked" && endBlockedRunInput
              ? endBlockedRunInput
              : mode === "armed"
                ? agentControlArmInput(snapshot)
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
      if (!mounted.current) return;
      if (result._tag === "Failure") {
        const failure = squashAtomCommandFailure(result);
        setError(agentControlCommandErrorMessage(failure));
      }
    } finally {
      requestPending.current = false;
      if (mounted.current) {
        setPending(false);
        refreshSnapshot();
        refreshSession();
      }
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
          Run one task or turn on automation for this project. Follow progress and review checked
          changes.
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
            <div className="space-y-2 rounded-md border p-3">
              <p role="status" className="text-sm font-medium">
                {armedStatus.label}
              </p>
              <p className="text-sm text-muted-foreground">{agentControlArmedExplanation}</p>
              {armedStatus.enabled === true ? (
                <>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={disarmInput === null}
                    onClick={() => void changeMode("observe", "disarm")}
                  >
                    Turn off automation
                  </Button>
                  <p className="text-sm text-muted-foreground">{agentControlDisarmExplanation}</p>
                </>
              ) : (
                <Button
                  size="sm"
                  disabled={armedBlockers.length > 0}
                  onClick={() => void changeMode("armed")}
                >
                  Turn on automation
                </Button>
              )}
              {armedStatus.enabled !== true && armedBlockers.length > 0 ? (
                <ul
                  className="space-y-1 text-sm text-muted-foreground"
                  aria-label="Automation blockers"
                >
                  {armedBlockers.map((blocker) => (
                    <li key={blocker}>{blocker}</li>
                  ))}
                </ul>
              ) : null}
              {pending ? (
                <p role="status" className="text-sm">
                  Waiting for server confirmation…
                </p>
              ) : null}
            </div>
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
                Run once
              </Button>
              {snapshot.projectState.mode === "manual" ? (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canChangeIntake}
                  onClick={() => void changeMode("observe")}
                >
                  Enable task intake
                </Button>
              ) : null}
              {snapshot.projectState.mode === "paused" ? (
                <div className="space-y-2">
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={endPausedInput === null}
                    onClick={() => void changeMode("manual", "end-paused")}
                  >
                    End paused mode
                  </Button>
                  <p className="text-sm text-muted-foreground">
                    {agentControlEndPausedExplanation}
                  </p>
                </div>
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
              {armedStatus.enabled !== true &&
              snapshot.projectState.mode === "run-once" &&
              snapshot.runs.some(agentControlCanEndBlockedRun) ? (
                <Button
                  size="sm"
                  variant="outline"
                  disabled={endBlockedRunInput === null}
                  onClick={() => void changeMode("observe", "end-blocked")}
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
            {armedStatus.enabled !== true &&
            snapshot.projectState.mode === "run-once" &&
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
              {selectedRun.state.status === "active" ? "Current run" : "Latest saved run"} ·{" "}
              {new Date(selectedRun.state.updatedAt).toLocaleString()}
            </p>
            <div>
              <h3 className="font-medium">
                {selectedRun.task?.title ?? "Run without an eligible task"}
              </h3>
              <p role="status" className="text-sm">
                {agentControlRunStatus(selectedRun).label}
              </p>
              {selectedRun.originMode ? (
                <p className="text-xs text-muted-foreground">
                  {selectedRun.originMode === "armed" ? "Started automatically" : "Run once"}
                </p>
              ) : null}
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
                  <p className="break-words text-xs text-muted-foreground">
                    {stage.providerInstanceId ?? "Provider not recorded"} /{" "}
                    {stage.model ?? "Model not recorded"}
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
