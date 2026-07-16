import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import type { DesktopNotificationKind } from "@t3tools/contracts";
import type { AgentAwarenessPhase } from "@t3tools/shared/agentAwareness";

export interface DesktopNotificationObservation {
  readonly phase: AgentAwarenessPhase;
  readonly activityIdentity: string;
}

export function createDesktopNotificationObservation(
  thread: Pick<EnvironmentThreadShell, "latestTurn" | "latestUserMessageAt">,
  phase: AgentAwarenessPhase,
): DesktopNotificationObservation {
  return {
    phase,
    activityIdentity:
      thread.latestUserMessageAt ??
      thread.latestTurn?.turnId ??
      thread.latestTurn?.completedAt ??
      "",
  };
}

export function desktopNotificationKindForTransition(
  previous: DesktopNotificationObservation | undefined,
  current: DesktopNotificationObservation,
): DesktopNotificationKind | null {
  if (previous === undefined) {
    return null;
  }

  switch (current.phase) {
    case "waiting_for_approval":
      return previous.phase === current.phase ? null : "approval";
    case "waiting_for_input":
      return previous.phase === current.phase ? null : "input";
    case "completed":
      return previous.phase !== current.phase ||
        previous.activityIdentity !== current.activityIdentity
        ? "completion"
        : null;
    case "failed":
      return previous.phase !== current.phase ||
        previous.activityIdentity !== current.activityIdentity
        ? "failure"
        : null;
    case "starting":
    case "running":
    case "stale":
      return null;
  }
}
