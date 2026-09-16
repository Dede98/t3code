import {
  AGENT_CONTROL_RPC_METHODS,
  AGENT_CONTROL_RUN_ONCE_RPC_METHODS,
  AGENT_CONTROL_RUNTIME_RPC_METHODS,
  AGENT_CONTROL_EPIC_RPC_METHODS,
  AGENT_CONTROL_EPIC_QUEUE_RPC_METHODS,
  isAgentControlEpicQueueEnabled,
  type AgentControlEpicQueueChangeInput,
  CommandId,
  AuthAccessWriteScope,
  type AuthSessionState,
  type AgentControlPreflightRuntimeResult,
  type AgentControlPolicyStateResult,
  type AgentControlRunOnceSnapshot,
  type AgentControlRunOnceStageView,
  type AgentControlRunOnceView,
  type AgentControlSetProjectModeInput,
  type AgentControlTaskId,
  type AgentControlEpicPreview,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicStartInput,
  type AgentControlEpicControlInput,
  type AgentControlEpicHandoffPreview,
  type AgentControlEpicHandoffPublishInput,
  type EnvironmentId,
  type ProjectId,
  type ResourceAdmissionWait,
} from "@t3tools/contracts";
import { AsyncResult, Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
  type AtomCommand,
} from "./runtime.ts";

/** One project subscription follows the environment session across reconnects. */
export function createAgentControlEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  const commandKey = (target: { environmentId: EnvironmentId; input: { projectId: ProjectId } }) =>
    JSON.stringify([target.environmentId, target.input.projectId]);
  const pendingCount = Atom.family((_key: string) => Atom.make(0).pipe(Atom.keepAlive));
  const pending = Atom.family((key: string) =>
    Atom.make((get) => get(pendingCount(key)) > 0).pipe(Atom.keepAlive),
  );
  function withPending<
    W extends { environmentId: EnvironmentId; input: { projectId: ProjectId } },
    A,
    F,
  >(command: AtomCommand<W, A, F>): AtomCommand<W, A, F> {
    return {
      ...command,
      run: async (registry, target) => {
        const atom = pendingCount(commandKey(target));
        registry.set(atom, registry.get(atom) + 1);
        try {
          return await command.run(registry, target);
        } finally {
          registry.set(atom, registry.get(atom) - 1);
        }
      },
    };
  }
  const setMode = createEnvironmentRpcCommand(runtime, {
    label: "environment-data:agent-control:set-mode",
    tag: AGENT_CONTROL_RUNTIME_RPC_METHODS.setProjectMode,
    scheduler: createAtomCommandScheduler(),
    concurrency: {
      mode: "singleFlight",
      key: ({ environmentId, input }) => JSON.stringify([environmentId, input.projectId]),
    },
  });
  return {
    epicQueueChange: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:change-queue",
        tag: AGENT_CONTROL_EPIC_QUEUE_RPC_METHODS.change,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    getRun: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control:load-saved-run",
        tag: AGENT_CONTROL_RUN_ONCE_RPC_METHODS.getSnapshot,
        concurrency: {
          mode: "singleFlight",
          key: (target) =>
            JSON.stringify([target.environmentId, target.input.projectId, target.input.runId]),
        },
      }),
    ),
    epicPreviewHandoff: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:preview-handoff",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.previewHandoff,
        concurrency: {
          mode: "singleFlight",
          key: ({ environmentId, input }) =>
            JSON.stringify([environmentId, input.projectId, input.epicRunId]),
        },
      }),
    ),
    epicPublishHandoff: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:publish-handoff",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.publishHandoff,
        concurrency: {
          mode: "singleFlight",
          key: ({ environmentId, input }) =>
            JSON.stringify([environmentId, input.projectId, input.epicRunId]),
        },
      }),
    ),
    epicPreview: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:preview",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.preview,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    epicStart: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:start",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.start,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    epicResume: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:resume",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.resume,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    epicStop: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:stop",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.stop,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    epicClear: withPending(
      createEnvironmentRpcCommand(runtime, {
        label: "agent-control-epic:clear",
        tag: AGENT_CONTROL_EPIC_RPC_METHODS.clear,
        concurrency: { mode: "singleFlight", key: commandKey },
      }),
    ),
    snapshot: createEnvironmentRpcSubscriptionAtomFamily(runtime, {
      label: "environment-data:agent-control:run-once",
      tag: AGENT_CONTROL_RUN_ONCE_RPC_METHODS.subscribe,
      idleTtlMs: 0,
    }),
    preflight: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:agent-control:preflight",
      tag: AGENT_CONTROL_RPC_METHODS.preflightRuntime,
      staleTimeMs: 0,
    }),
    policy: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:agent-control:policy",
      tag: AGENT_CONTROL_RPC_METHODS.getPolicy,
      staleTimeMs: 0,
    }),
    pending: (target: { environmentId: EnvironmentId; input: { projectId: ProjectId } }) =>
      pending(commandKey(target)),
    setMode: withPending(setMode),
  };
}

/** An open snapshot stream remains waiting between events even after a valid snapshot arrives. */
export function agentControlSnapshotReady<E>(
  result: AsyncResult.AsyncResult<AgentControlRunOnceSnapshot, E>,
  connected: boolean,
): boolean {
  return connected && AsyncResult.isSuccess(result);
}

/** A refresh may retain the previous value while the subscription reconnects. */
export function agentControlSnapshotFresh<E>(
  result: AsyncResult.AsyncResult<AgentControlRunOnceSnapshot, E>,
  previousSnapshot: AgentControlRunOnceSnapshot | null,
  connected: boolean,
): boolean {
  return connected && AsyncResult.isSuccess(result) && result.value !== previousSnapshot;
}

/** Start and intake changes share the server's administrative mode-change permission. */
export function agentControlModeChangeBlocker<E>(
  session: AsyncResult.AsyncResult<AuthSessionState, E>,
): string | null {
  if (session._tag === "Failure") {
    return "Could not verify your permissions in this environment. Reconnect before starting a run or changing task intake.";
  }
  if (session._tag !== "Success" || session.waiting) {
    return "Checking your permissions in this environment before allowing run starts or task intake changes.";
  }
  if (!session.value.authenticated || !session.value.scopes?.includes(AuthAccessWriteScope)) {
    return "Your session in this environment lacks administrative permission (access:write) to start runs or change task intake. Ask the environment administrator for an admin pairing link; you can still review saved runs.";
  }
  return null;
}

/** Publication requires known administrative scope in the selected environment. */
export function agentControlEpicHandoffPermissionBlocker<E>(
  session: AsyncResult.AsyncResult<AuthSessionState, E>,
): string | null {
  if (session._tag !== "Success" || session.waiting) {
    return "Verify your permissions in this environment before publishing. Reconnect if the permission check failed.";
  }
  if (!session.value.authenticated || !session.value.scopes?.includes(AuthAccessWriteScope)) {
    return "Publishing requires administrative permission (access:write) in this environment. Ask its administrator for an admin pairing link.";
  }
  return null;
}

/** Schema-tagged RPC errors may be Error instances with an empty message. */
export function agentControlCommandErrorMessage(error: unknown): string {
  const code = typeof error === "object" && error !== null && "code" in error ? error.code : null;
  if (code === "revision-conflict") {
    return "The project changed before this request was accepted. Review the refreshed state and try again.";
  }
  if (code === "command-previously-rejected") {
    return "The server previously rejected this request. Review the refreshed state and resolve any reported blocker before trying again.";
  }
  if (code === "reservation-conflict") return agentControlErrorMessage(code);
  if (error instanceof Error && error.message.trim()) return error.message;
  return `The request failed${typeof code === "string" ? ` (${code})` : ""}. Check the refreshed state and your permissions in this environment before trying again.`;
}

/** Retries of the same displayed selection retain both command identity and revision. */
export function agentControlStartInput(
  snapshot: AgentControlRunOnceSnapshot,
  taskId: AgentControlTaskId,
): AgentControlSetProjectModeInput {
  return {
    commandId: CommandId.make(
      `t3auto-run-once:${JSON.stringify([snapshot.projectId, snapshot.projectState.revision, taskId])}`,
    ),
    projectId: snapshot.projectId,
    expectedRevision: snapshot.projectState.revision,
    mode: "run-once",
    runOnceTaskId: taskId,
  };
}

/** This pre-turn worktree rejection retains immutable failure evidence after human takeover. */
export function agentControlCanEndBlockedRun(run: AgentControlRunOnceView): boolean {
  return (
    run.state.status === "active" &&
    run.state.lastStep === "lease-reserved" &&
    (run.errorCode === "downstream-rejected: default-remote-ref-unavailable" ||
      run.errorCode === "downstream-rejected: reservation-conflict")
  );
}

/** Human takeover ends the blocked activation; it never retries its rejected commands. */
export function agentControlEndBlockedRunInput(input: {
  snapshot: AgentControlRunOnceSnapshot | null;
  connected: boolean;
  pending: boolean;
  modeChangeBlocker: string | null;
}): AgentControlSetProjectModeInput | null {
  const snapshot = input.snapshot;
  if (
    !snapshot ||
    !input.connected ||
    input.pending ||
    input.modeChangeBlocker !== null ||
    snapshot.projectState.mode !== "run-once" ||
    !snapshot.runs.some(agentControlCanEndBlockedRun)
  )
    return null;
  return {
    commandId: CommandId.make(
      `t3auto-end-blocked:${JSON.stringify([snapshot.projectId, snapshot.projectState.revision])}`,
    ),
    projectId: snapshot.projectId,
    expectedRevision: snapshot.projectState.revision,
    mode: "observe",
  };
}

type AgentControlStartContext = {
  snapshot: AgentControlRunOnceSnapshot | null;
  preflight: AgentControlPreflightRuntimeResult | null;
  policy: AgentControlPolicyStateResult | null;
  selectedTaskId: AgentControlTaskId | null;
  connected: boolean;
  pending: boolean;
  modeChangeBlocker: string | null;
};

export type AgentControlReadiness = Omit<AgentControlStartContext, "selectedTaskId">;

/** Queue approval is independent of execution readiness: dependencies may still be open. */
export function agentControlEpicQueueChangeBlockers(input: AgentControlReadiness): string[] {
  const blockers: string[] = [];
  if (input.modeChangeBlocker !== null) blockers.push(input.modeChangeBlocker);
  if (!input.connected)
    blockers.push("Reconnect to this environment before changing the Epic queue.");
  if (input.pending) blockers.push("Wait for the current project action to finish.");
  if (!input.snapshot) blockers.push("Waiting for the current project state.");
  return blockers;
}

export function agentControlEpicQueueLeaveBlockers(input: AgentControlReadiness): string[] {
  const blockers = agentControlEpicQueueChangeBlockers(input);
  if (!isAgentControlEpicQueueEnabled(input.snapshot?.epicQueue))
    blockers.push("No Epic queue is enabled.");
  if (
    input.snapshot?.armed?.enabled !== false ||
    input.snapshot.projectState.mode === "run-once" ||
    input.snapshot.projectState.pausedFromMode === "run-once" ||
    input.snapshot.runs.some((run) => run.state.status === "active")
  )
    blockers.push("Turn Armed off and wait for active work to settle before leaving the queue.");
  if (input.snapshot?.epicQueue?.entries.some((entry) => entry.status === "pending"))
    blockers.push("Remove waiting entries before leaving the queue.");
  return blockers;
}

export function agentControlEpicQueueApproveBlockers(
  input: AgentControlReadiness,
  preview: AgentControlEpicPreview | null,
): string[] {
  const blockers = agentControlEpicQueueChangeBlockers(input);
  if (!preview || preview.projectId !== input.snapshot?.projectId) {
    blockers.push("Inspect an Epic in this project before approving it.");
  } else if (
    isAgentControlEpicQueueEnabled(input.snapshot?.epicQueue) &&
    input.snapshot?.epicQueue?.entries.some(
      (entry) => entry.source.epic.issueNodeId === preview.source.epic.issueNodeId,
    )
  ) {
    blockers.push("This Epic is already in the approved queue.");
  }
  return blockers;
}

export function agentControlEpicQueueChangeInput(
  snapshot: AgentControlRunOnceSnapshot,
  action: AgentControlEpicQueueChangeInput["action"],
): AgentControlEpicQueueChangeInput {
  const expectedRevision = snapshot.epicQueue?.revision ?? 0;
  return {
    projectId: snapshot.projectId,
    expectedRevision,
    action,
    commandId: CommandId.make(
      `t3auto-epic-queue:${JSON.stringify([snapshot.projectId, expectedRevision, action])}`,
    ),
  };
}

/** Move only waiting entries; the request contains their complete order for revision checking. */
export function agentControlEpicQueueMoveInput(
  snapshot: AgentControlRunOnceSnapshot,
  entryId: string,
  direction: -1 | 1,
): AgentControlEpicQueueChangeInput | null {
  const entryIds =
    snapshot.epicQueue?.entries
      .filter((entry) => entry.status === "pending")
      .map((entry) => entry.entryId) ?? [];
  const index = entryIds.indexOf(entryId);
  const other = index + direction;
  if (index < 0 || other < 0 || other >= entryIds.length) return null;
  [entryIds[index], entryIds[other]] = [entryIds[other]!, entryIds[index]!];
  return agentControlEpicQueueChangeInput(snapshot, { kind: "reorder", entryIds });
}

export function agentControlEpicQueueView(snapshot: AgentControlRunOnceSnapshot | null) {
  const queue = snapshot?.epicQueue;
  if (!queue || !isAgentControlEpicQueueEnabled(queue)) return null;
  return {
    entries: queue.entries,
    active: queue.entries.find((entry) => entry.status === "active") ?? null,
    next: queue.entries.find((entry) => entry.entryId === queue.nextEntryId) ?? null,
    waitReason: queue.waitReason,
    nextCheckAt: queue.nextCheckAt,
  };
}

export function agentControlEpicStartBlockers(
  input: AgentControlReadiness,
  preview: AgentControlEpicPreview | null,
): string[] {
  const blockers = agentControlActivationBlockers({ ...input, selectedTaskId: null }, "epic");
  if (!preview || preview.projectId !== input.snapshot?.projectId) {
    blockers.push("Inspect an Epic in this project before starting.");
  } else {
    if (!preview.canStart) {
      blockers.push(...preview.blockers.map((blocker) => blocker.message));
      blockers.push(...preview.source.blockers.map((blocker) => blocker.message));
      if (preview.blockers.length === 0 && preview.source.blockers.length === 0)
        blockers.push("The inspected Epic has no executable work.");
    }
  }
  if (isAgentControlEpicQueueEnabled(input.snapshot?.epicQueue)) {
    blockers.push("Approve this Epic for the queue and enable Armed to start it.");
  }
  const epic = input.snapshot?.epic;
  if (epic) {
    blockers.push(
      "Resume the existing Epic or end it and return to ordinary tasks before selecting another Epic.",
    );
  }
  return [...new Set(blockers)];
}

export function agentControlEpicStartInput(
  snapshot: AgentControlRunOnceSnapshot,
  preview: AgentControlEpicPreview,
): AgentControlEpicStartInput {
  return {
    projectId: snapshot.projectId,
    commandId: CommandId.make(
      `t3auto-epic-start:${JSON.stringify([
        snapshot.projectId,
        snapshot.projectState.revision,
        preview.source.epic.issueNodeId,
        preview.source.fingerprint,
      ])}`,
    ),
    expectedRevision: snapshot.projectState.revision,
    epicNumber: preview.source.epic.number,
    expectedFingerprint: preview.source.fingerprint,
  };
}

export function agentControlEpicControlInput(
  epic: AgentControlEpicRuntimeView,
  action: "resume" | "stop" | "clear",
): AgentControlEpicControlInput {
  return {
    projectId: epic.projectId,
    epicRunId: epic.epicRunId,
    expectedRevision: epic.revision,
    commandId: CommandId.make(
      `t3auto-epic-${action}:${JSON.stringify([epic.projectId, epic.epicRunId, epic.revision])}`,
    ),
  };
}

export function agentControlEpicControlAllowed(
  readiness: AgentControlReadiness,
  action: "resume" | "stop" | "clear",
): boolean {
  const epic = readiness.snapshot?.epic;
  if (!epic || !readiness.connected || readiness.pending || readiness.modeChangeBlocker !== null)
    return false;
  const terminal = epic.status === "succeeded" || epic.status === "stopped";
  if (action === "clear")
    return (
      terminal &&
      !isAgentControlEpicQueueEnabled(readiness.snapshot?.epicQueue) &&
      readiness.snapshot?.armed?.enabled === false &&
      !readiness.snapshot.runs.some((run) => run.state.status === "active")
    );
  if (action === "stop") return !terminal;
  return (
    !epic.members.some((member) => member.status === "failed") &&
    (epic.status === "blocked" ||
      (epic.status === "running" && readiness.snapshot?.armed?.enabled === false)) &&
    readiness.preflight?.ok === true &&
    readiness.policy?.projectPolicy?.policy.verificationChecks?.some((check) => check.required) ===
      true
  );
}

/** Success requires the final checks to identify the accepted common commit. */
export function agentControlEpicStatus(epic: AgentControlEpicRuntimeView): AgentControlStatusView {
  if (epic.status === "succeeded") {
    const verification = epic.finalVerification;
    const verified =
      verification?.status === "passed" &&
      verification.commitSha === epic.acceptedCommitSha &&
      epic.checks.some((check) => check.required) &&
      epic.checks.every((configured) => {
        if (!configured.required) return true;
        const check = verification.checks.find((item) => item.id === configured.id);
        return (
          check?.required === true &&
          check.status === "passed" &&
          check.exitCode === 0 &&
          check.completedAt !== null
        );
      });
    return verified
      ? { label: "Epic succeeded · common result verified", tone: "success" }
      : { label: "Epic verification evidence incomplete", tone: "warning" };
  }
  if (epic.status === "blocked")
    return { label: "Epic blocked · action required", tone: "warning" };
  if (epic.status === "stopped")
    return { label: "Epic ended · evidence retained", tone: "neutral" };
  if (epic.status === "verifying")
    return { label: "Verifying common Epic result", tone: "running" };
  return { label: "Epic in progress", tone: "running" };
}

/** A server preview is usable only for the displayed project, run and accepted commit. */
export function agentControlEpicHandoffBlockers(input: {
  epic: AgentControlEpicRuntimeView;
  connected: boolean;
  pending: boolean;
  permissionBlocker: string | null;
  preview?: AgentControlEpicHandoffPreview | null;
}): string[] {
  const { epic, preview } = input;
  const blockers: string[] = [];
  if (input.permissionBlocker !== null) blockers.push(input.permissionBlocker);
  if (!input.connected) blockers.push("Reconnect to this environment before publishing.");
  if (input.pending) blockers.push("A request is in progress. Wait for the server response.");
  if (agentControlEpicStatus(epic).tone !== "success")
    blockers.push("Publication requires successful verification of the accepted common commit.");
  if (epic.handoff?.pullRequest)
    blockers.push(
      "This Epic already has a pull request. Open the saved pull request to review it.",
    );
  if (epic.handoff?.status === "publishing")
    blockers.push("The environment is publishing this Epic. Its saved progress will update here.");
  if (preview) {
    if (
      preview.projectId !== epic.projectId ||
      preview.epicRunId !== epic.epicRunId ||
      preview.commitSha !== epic.acceptedCommitSha ||
      preview.repository.repositoryNodeId !== epic.source.repository.repositoryNodeId ||
      preview.repository.nameWithOwner !== epic.source.repository.nameWithOwner
    )
      blockers.push(
        "The publication preview no longer matches this Epic. Review publication again.",
      );
    blockers.push(...preview.blockers.map((blocker) => blocker.message));
    if (!preview.canPublish || !preview.targetBranch || !preview.commitSha)
      blockers.push("Review publication again after resolving the reported blockers.");
  }
  return [...new Set(blockers)];
}

/** Retry identity is tied to the durable run and the exact reviewed publication target. */
export function agentControlEpicPublishHandoffInput(
  epic: AgentControlEpicRuntimeView,
  preview: AgentControlEpicHandoffPreview,
): AgentControlEpicHandoffPublishInput {
  if (
    agentControlEpicHandoffBlockers({
      epic,
      preview,
      connected: true,
      pending: false,
      permissionBlocker: null,
    }).length > 0 ||
    !preview.commitSha ||
    !preview.targetBranch
  )
    throw new Error("Review a valid publication preview before creating the Draft PR.");
  return {
    projectId: epic.projectId,
    epicRunId: epic.epicRunId,
    expectedRevision: epic.revision,
    expectedCommitSha: preview.commitSha,
    expectedTargetBranch: preview.targetBranch,
    commandId: CommandId.make(
      `t3auto-epic-publish:${JSON.stringify([
        epic.projectId,
        epic.epicRunId,
        epic.revision,
        preview.commitSha,
        preview.targetBranch,
      ])}`,
    ),
  };
}

export function agentControlStartBlockers(input: AgentControlStartContext): string[] {
  return agentControlActivationBlockers(input, "run-once");
}

export function agentControlArmBlockers(
  input: Omit<AgentControlStartContext, "selectedTaskId">,
): string[] {
  const blockers = agentControlActivationBlockers({ ...input, selectedTaskId: null }, "armed");
  if (input.snapshot?.armed === undefined)
    blockers.push(
      "This environment has not supplied automatic mode authority. Reconnect to an updated server before enabling automatic mode.",
    );
  if (
    !isAgentControlEpicQueueEnabled(input.snapshot?.epicQueue) &&
    input.snapshot?.nextTaskId === null &&
    input.snapshot.tasks.some(
      (task) => task.status === "candidate" && task.sourceGate === "eligible",
    )
  ) {
    blockers.push(
      "An eligible task has unresolved readiness or execution history. Refresh intake; remove an already attempted issue's ready label or pause it in GitHub before enabling automatic mode.",
    );
  }
  return blockers;
}

function agentControlActivationBlockers(
  input: AgentControlStartContext,
  mode: "run-once" | "armed" | "epic",
): string[] {
  const blockers: string[] = [];
  if (input.modeChangeBlocker !== null) blockers.push(input.modeChangeBlocker);
  if (input.policy === null) {
    blockers.push("Load the project's verification configuration before starting.");
  } else if (
    !input.policy.projectPolicy?.policy.verificationChecks?.some((check) => check.required)
  ) {
    blockers.push(
      "Configure at least one required verification check for this project before starting.",
    );
  }
  if (!input.connected) blockers.push("Reconnect to this environment before starting a task.");
  if (input.pending) blockers.push("A request is in progress. Wait for the server response.");
  const snapshot = input.snapshot;
  if (snapshot === null) {
    blockers.push("Waiting for the server's task and run snapshot.");
  } else {
    if (
      mode !== "epic" &&
      snapshot.epic &&
      !(mode === "armed" && isAgentControlEpicQueueEnabled(snapshot.epicQueue))
    ) {
      blockers.push("This project has an Epic execution target. Use its resume or end controls.");
    }
    if (!(mode === "armed" && isAgentControlEpicQueueEnabled(snapshot.epicQueue)))
      blockers.push(...snapshot.blockers.map(agentControlErrorMessage));
    if (snapshot.projectState.mode !== "observe") {
      blockers.push(
        snapshot.projectState.mode === "run-once"
          ? "A run is already active. Open its progress below."
          : snapshot.projectState.mode === "armed"
            ? "Turn off automation before starting a single task."
            : "Enable task intake before starting autonomous work.",
      );
    }
    if (snapshot.runs.some((run) => run.state.status === "active")) {
      blockers.push("The previous run has not finished releasing its resources.");
    }
    if (mode === "run-once") {
      if (isAgentControlEpicQueueEnabled(snapshot.epicQueue))
        blockers.push("This project uses an approved Epic queue. Enable Armed to continue it.");
      const task = snapshot.tasks.find((candidate) => candidate.taskId === input.selectedTaskId);
      if (!task) blockers.push("Select an eligible task.");
      else if (
        snapshot.runs.some(
          (run) =>
            run.state.status === "completed" &&
            run.errorCode !== null &&
            run.state.taskId === task.taskId,
        )
      ) {
        blockers.push(
          "This task belongs to an ended blocked run and cannot start again. Fix the reported cause, remove the old issue's ready label or pause it in GitHub, and wait for intake to update. Then select a new eligible task.",
        );
      } else if (task.status !== "candidate" || task.sourceGate !== "eligible") {
        blockers.push(`This task cannot start: ${task.status}, source ${task.sourceGate}.`);
      } else if (snapshot.nextTaskId === null) {
        blockers.push(
          "Task readiness changed or the next task already has execution history. Refresh intake; if the old issue was already attempted, remove its ready label or pause it in GitHub, then choose a new eligible task.",
        );
      } else if (snapshot.nextTaskId !== task.taskId) {
        blockers.push(
          "Run Once currently accepts the next eligible task in issue-number order. Select that task.",
        );
      }
    }
  }
  if (input.preflight === null)
    blockers.push("Check provider and model readiness before starting.");
  else if (!input.preflight.ok) {
    const unresolved = input.preflight.roles.filter((role) => role.selectedCandidateIndex === null);
    for (const role of unresolved) {
      const reasons = role.candidates.map(
        (candidate) =>
          `${candidate.providerInstanceId} / ${candidate.model}: ${candidate.verificationCheckError ? `Check ${candidate.verificationCheckError.checkId}: ${candidate.verificationCheckError.message}` : (candidate.errorCode ?? "not ready")}`,
      );
      blockers.push(
        `${role.role}: ${reasons.join("; ") || "No configured provider/model route"}. Check provider settings in this environment.`,
      );
    }
    if (unresolved.length === 0)
      blockers.push("Provider preflight failed. Review the configuration and check again.");
  }
  return blockers;
}

export type AgentControlStatusView = {
  label: string;
  tone: "neutral" | "running" | "success" | "danger" | "warning";
};

export type ResourceAdmissionWaitSummary = `Waiting${string}`;

const RESOURCE_ADMISSION_WAIT_LABELS = {
  "provider-limit": "Waiting for provider capacity",
  "local-capacity": "Waiting for local check capacity",
  "cpu-pressure": "Waiting for CPU pressure to fall",
  "ram-pressure": "Waiting for available memory",
  "gpu-pressure": "Waiting for GPU capacity",
  "interactive-priority": "Waiting while interactive work has priority",
  "telemetry-unavailable": "Waiting for host resource telemetry",
  "unsupported-requirement": "Waiting: this host cannot satisfy a required resource",
} as const satisfies Record<ResourceAdmissionWait["reason"], ResourceAdmissionWaitSummary>;

export function resourceAdmissionWaitSummary(
  wait: ResourceAdmissionWait,
): ResourceAdmissionWaitSummary {
  return RESOURCE_ADMISSION_WAIT_LABELS[wait.reason];
}

/** Presents only server-host observations; clients never substitute browser or phone resources. */
export function resourceAdmissionWaitMessage(wait: ResourceAdmissionWait): string {
  const host = wait.hostId ? ` on ${wait.hostId}` : " on the server host";
  const detail = wait.detail ? ` ${wait.detail}` : "";
  return `${resourceAdmissionWaitSummary(wait)}${host}.${detail}`;
}

export function agentControlErrorMessage(code: string): string {
  if (code === "reservation-conflict" || code === "downstream-rejected: reservation-conflict") {
    return "Worktree reservation conflict. Another attempt still owns this issue or its worktree target. Review the other run in this environment and end this blocked run or disarm the project. This rejected attempt will not retry automatically.";
  }
  const action =
    code.includes("capacity") || code.includes("slot")
      ? "Wait for a provider slot to become available."
      : code.includes("source") || code.includes("task-changed") || code.includes("watermark")
        ? "Refresh task intake and review the selected task's eligibility."
        : code.includes("verification") || code.includes("evidence")
          ? "Open the verification thread and inspect the recorded checks; success is not confirmed."
          : code.includes("provider") || code.includes("model") || code.includes("runtime")
            ? "Check provider readiness and model configuration in this environment."
            : code === "persistence"
              ? "The server could not save run progress. Check this environment's server logs; the run will retry automatically."
              : "Open the stage thread for details and check readiness again.";
  return `${code}: ${action}`;
}

export function agentControlStageLabel(stage: AgentControlRunOnceStageView): string {
  // Repair retains an implementation stage kind internally; the server resolves its role.
  const kind = stage.displayStage;
  return kind.charAt(0).toUpperCase() + kind.slice(1);
}

export function agentControlStageHeading(
  stage: AgentControlRunOnceStageView,
  stages: readonly AgentControlRunOnceStageView[],
): string {
  const label = agentControlStageLabel(stage);
  if (stage.displayStage !== "verification") return label;
  const preceding = stages.filter(
    (candidate) =>
      candidate.stageOrdinal < stage.stageOrdinal ||
      (candidate.stageOrdinal === stage.stageOrdinal &&
        candidate.attemptOrdinal < stage.attemptOrdinal),
  );
  const number =
    preceding.filter((candidate) => candidate.displayStage === "verification").length + 1;
  return `${label} ${number}${preceding.some((candidate) => candidate.displayStage === "repair") ? " (after repair)" : ""}`;
}

export function agentControlVerificationPassed(stage: AgentControlRunOnceStageView): boolean {
  const verification = stage.verification;
  return (
    stage.displayStage === "verification" &&
    stage.status === "succeeded" &&
    stage.errorCode === null &&
    verification !== null &&
    verification.verdict === "passed" &&
    verification.errorCode === null &&
    verification.evaluatedAt !== null &&
    verification.checks.some((check) => check.required) &&
    verification.checks.every(
      (check) =>
        !check.required ||
        (check.status === "passed" && check.exitCode === 0 && check.completedAt !== null),
    )
  );
}

export function agentControlRunStatus(run: AgentControlRunOnceView): AgentControlStatusView {
  if (run.errorCode !== null)
    return {
      label: `${run.state.status === "completed" && run.task?.status !== "succeeded" ? "Ended · blocked" : "Blocked"} · ${agentControlErrorMessage(run.errorCode)}`,
      tone: "warning",
    };
  if (run.state.status === "no-eligible-task")
    return { label: "Blocked: no eligible task", tone: "warning" };
  if (run.task === null) {
    return run.state.status === "active" &&
      run.state.lastStep === "activation-admitted" &&
      run.state.taskId === null
      ? { label: "Starting", tone: "running" }
      : { label: "Waiting for task data", tone: "warning" };
  }
  if (run.task.status === "failed") return { label: "Failed", tone: "danger" };
  if (["waiting", "needs-attention", "cancelled"].includes(run.task.status)) {
    return { label: "Blocked: action required", tone: "warning" };
  }
  const stages = [...run.stages].sort(
    (a, b) => a.stageOrdinal - b.stageOrdinal || a.attemptOrdinal - b.attemptOrdinal,
  );
  const latest = stages.at(-1);
  if (latest?.admissionWait) {
    return {
      label: resourceAdmissionWaitMessage(latest.admissionWait),
      tone: "neutral",
    };
  }
  if (run.task.status === "succeeded") {
    return latest !== undefined && agentControlVerificationPassed(latest)
      ? { label: "Succeeded · verified", tone: "success" }
      : { label: "Verification evidence unavailable", tone: "warning" };
  }
  if (latest?.status === "waiting" || latest?.errorCode) {
    return {
      label: `Blocked${latest ? ` · ${agentControlStageLabel(latest)}` : ""}`,
      tone: "warning",
    };
  }
  if (run.state.status === "completed")
    return {
      label:
        run.originMode === "armed" && run.state.terminalTaskEventId === null
          ? "Automatic run ended · admitted work may continue"
          : "Finished without verified success",
      tone: "warning",
    };
  return {
    label: latest ? `Running · ${agentControlStageLabel(latest)}` : "Starting",
    tone: "running",
  };
}

export const agentControlArmedExplanation =
  "Automatic mode (Armed) starts eligible work for this project. With an Epic queue, it follows your approved order and waits for explicit PR publication and a confirmed human merge before continuing. An empty Epic queue waits for approval of more Epics. Without a queue, eligible tasks run in server order. Armed stays enabled until you turn it off.";
export const agentControlDisarmExplanation =
  "Turning off automatic mode prevents new tasks from starting automatically. Work already admitted can continue, including later stages of the current task. This does not interrupt provider turns or guarantee that the task will finish. Saved threads, changes and check evidence remain available. Task intake stays enabled.";

/** The server reports current durable authority, independent of the latest run's history. */
export function agentControlArmedStatus(
  snapshot: AgentControlRunOnceSnapshot | null,
): AgentControlStatusView & { enabled: boolean | null } {
  if (!snapshot?.armed) return { enabled: null, label: "Automatic mode unknown", tone: "warning" };
  if (!snapshot.armed.enabled)
    return {
      enabled: false,
      label:
        snapshot.projectState.mode === "paused"
          ? "Automatic mode off · project paused"
          : "Automatic mode off",
      tone: "neutral",
    };
  const queue = agentControlEpicQueueView(snapshot);
  if (queue) {
    if (queue.waitReason)
      return {
        enabled: true,
        label: `Automatic mode on · ${queue.waitReason}`,
        tone: "warning",
      };
    if (queue.active)
      return {
        enabled: true,
        label: `Automatic mode on · Epic #${queue.active.source.epic.number} active`,
        tone: "running",
      };
    return {
      enabled: true,
      label: queue.next
        ? `Automatic mode on · next candidate Epic #${queue.next.source.epic.number}`
        : "Automatic mode on · waiting for an eligible approved Epic",
      tone: "neutral",
    };
  }
  const active = snapshot.runs.find((run) => run.state.status === "active");
  const status = active ? agentControlRunStatus(active) : null;
  if (snapshot.blockers.length > 0 || status?.tone === "warning" || status?.tone === "danger") {
    return { enabled: true, label: "Automatic mode on · blocked", tone: "warning" };
  }
  return active || snapshot.projectState.mode === "run-once"
    ? { enabled: true, label: "Automatic mode on · task running", tone: "running" }
    : { enabled: true, label: "Automatic mode on · waiting for eligible tasks", tone: "neutral" };
}

export function agentControlArmInput(
  snapshot: AgentControlRunOnceSnapshot,
): AgentControlSetProjectModeInput {
  return {
    commandId: CommandId.make(
      `t3auto-armed:${JSON.stringify([snapshot.projectId, snapshot.projectState.revision])}`,
    ),
    projectId: snapshot.projectId,
    expectedRevision: snapshot.projectState.revision,
    mode: "armed",
  };
}

export function agentControlDisarmInput(input: {
  snapshot: AgentControlRunOnceSnapshot | null;
  connected: boolean;
  pending: boolean;
  modeChangeBlocker: string | null;
}): AgentControlSetProjectModeInput | null {
  const snapshot = input.snapshot;
  if (
    !snapshot?.armed?.enabled ||
    !input.connected ||
    input.pending ||
    input.modeChangeBlocker !== null ||
    (snapshot.projectState.mode !== "armed" && snapshot.projectState.mode !== "run-once")
  )
    return null;
  return {
    commandId: CommandId.make(
      `t3auto-disarm:${JSON.stringify([snapshot.projectId, snapshot.projectState.revision])}`,
    ),
    projectId: snapshot.projectId,
    expectedRevision: snapshot.projectState.revision,
    mode: "observe",
  };
}

/** Leaving a paused project requires Manual; Observe could resume the wrong operation. */
export function agentControlEndPausedInput(input: {
  snapshot: AgentControlRunOnceSnapshot | null;
  connected: boolean;
  pending: boolean;
  modeChangeBlocker: string | null;
}): AgentControlSetProjectModeInput | null {
  const snapshot = input.snapshot;
  if (
    !snapshot ||
    snapshot.projectState.mode !== "paused" ||
    !input.connected ||
    input.pending ||
    input.modeChangeBlocker !== null
  )
    return null;
  return {
    commandId: CommandId.make(
      `t3auto-end-paused:${JSON.stringify([snapshot.projectId, snapshot.projectState.revision])}`,
    ),
    projectId: snapshot.projectId,
    expectedRevision: snapshot.projectState.revision,
    mode: "manual",
  };
}

export const agentControlEndPausedExplanation =
  "End paused mode to return this project to manual control. Then enable task intake before starting fresh autonomous work. This does not interrupt a provider turn already running.";
