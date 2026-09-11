import {
  AGENT_CONTROL_RPC_METHODS,
  AGENT_CONTROL_RUN_ONCE_RPC_METHODS,
  AGENT_CONTROL_RUNTIME_RPC_METHODS,
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
} from "@t3tools/contracts";
import { AsyncResult, type Atom } from "effect/unstable/reactivity";

import type { EnvironmentRegistry } from "../connection/registry.ts";
import {
  createAtomCommandScheduler,
  createEnvironmentRpcCommand,
  createEnvironmentRpcQueryAtomFamily,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "./runtime.ts";

/** One project subscription follows the environment session across reconnects. */
export function createAgentControlEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
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
    setMode: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:agent-control:set-mode",
      tag: AGENT_CONTROL_RUNTIME_RPC_METHODS.setProjectMode,
      scheduler: createAtomCommandScheduler(),
      concurrency: {
        mode: "singleFlight",
        key: ({ environmentId, input }) => JSON.stringify([environmentId, input.projectId]),
      },
    }),
  };
}

/** An open snapshot stream remains waiting between events even after a valid snapshot arrives. */
export function agentControlSnapshotReady<E>(
  result: AsyncResult.AsyncResult<AgentControlRunOnceSnapshot, E>,
  connected: boolean,
): boolean {
  return connected && AsyncResult.isSuccess(result);
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
    !snapshot.runs.some((run) => run.state.status === "active" && run.errorCode !== null)
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

export function agentControlStartBlockers(input: {
  snapshot: AgentControlRunOnceSnapshot | null;
  preflight: AgentControlPreflightRuntimeResult | null;
  policy: AgentControlPolicyStateResult | null;
  selectedTaskId: AgentControlTaskId | null;
  connected: boolean;
  pending: boolean;
  modeChangeBlocker: string | null;
}): string[] {
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
    blockers.push(...snapshot.blockers.map(agentControlErrorMessage));
    if (snapshot.projectState.mode !== "observe") {
      blockers.push(
        snapshot.projectState.mode === "run-once"
          ? "A run is already active. Open its progress below."
          : "Enable Observe mode to prepare a single autonomous run.",
      );
    }
    if (snapshot.runs.some((run) => run.state.status === "active")) {
      blockers.push("The previous run has not finished releasing its resources.");
    }
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
    } else if (snapshot.nextTaskId !== task.taskId) {
      blockers.push(
        "Run Once currently accepts the next eligible task in issue-number order. Select that task.",
      );
    }
  }
  if (input.preflight === null)
    blockers.push("Check provider and model readiness before starting.");
  else if (!input.preflight.ok) {
    const unresolved = input.preflight.roles.filter((role) => role.selectedCandidateIndex === null);
    for (const role of unresolved) {
      const reasons = role.candidates.map(
        (candidate) =>
          `${candidate.providerInstanceId} / ${candidate.model}: ${candidate.errorCode ?? "not ready"}`,
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

export function agentControlErrorMessage(code: string): string {
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
  if (run.task === null) return { label: "Waiting for task data", tone: "warning" };
  if (run.task.status === "failed") return { label: "Failed", tone: "danger" };
  if (["waiting", "needs-attention", "cancelled"].includes(run.task.status)) {
    return { label: "Blocked: action required", tone: "warning" };
  }
  const stages = [...run.stages].sort(
    (a, b) => a.stageOrdinal - b.stageOrdinal || a.attemptOrdinal - b.attemptOrdinal,
  );
  const latest = stages.at(-1);
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
    return { label: "Finished without verified success", tone: "warning" };
  return {
    label: latest ? `Running · ${agentControlStageLabel(latest)}` : "Starting",
    tone: "running",
  };
}
