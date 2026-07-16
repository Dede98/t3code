import type { ProviderUsageSnapshot, ProviderUsageWindow } from "@t3tools/contracts";

export type ProviderUsageAttention = {
  readonly severity: "warning" | "unavailable";
  readonly label: string;
  readonly reason: string;
};

const CLAUDE_SCOPED_WINDOW_IDS = new Set([
  "seven_day_overage_included",
  "seven_day_opus",
  "seven_day_sonnet",
]);

function isScopedClaudeWindow(window: ProviderUsageWindow): boolean {
  return window.id.startsWith("weekly_scoped_") || CLAUDE_SCOPED_WINDOW_IDS.has(window.id);
}

function formatReset(resetAt: string | null): string {
  if (!resetAt) return "";
  const reset = new Date(resetAt);
  if (!Number.isFinite(reset.getTime())) return "";
  return ` Resets ${reset.toLocaleString(undefined, {
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
  })}.`;
}

export function getProviderUsageUnavailableReason(
  usage: ProviderUsageSnapshot | undefined,
): string | null {
  if (!usage || usage.status !== "rejected") return null;

  const exhaustedWindows = usage.windows.filter((window) => window.usedPercent >= 100);
  const blockingWindow =
    usage.driver === "claudeAgent"
      ? exhaustedWindows.find((window) => !isScopedClaudeWindow(window))
      : exhaustedWindows[0];

  if (blockingWindow) {
    return `${blockingWindow.label} usage limit reached.${formatReset(blockingWindow.resetsAt)}`;
  }

  // A scoped Claude model limit does not make the rest of the provider unavailable.
  if (usage.driver === "claudeAgent" && exhaustedWindows.length > 0) return null;
  return "Usage limit reached.";
}

export function getProviderUsageAttention(
  usage: ProviderUsageSnapshot | undefined,
): ProviderUsageAttention | null {
  if (!usage) return null;
  const unavailableReason = getProviderUsageUnavailableReason(usage);
  if (unavailableReason) {
    return { severity: "unavailable", label: "Limit reached", reason: unavailableReason };
  }

  const warningWindow = usage.windows
    .filter((window) => window.usedPercent > 90)
    .toSorted((left, right) => right.usedPercent - left.usedPercent)[0];
  if (!warningWindow) return null;
  const percent = Math.round(warningWindow.usedPercent);
  return {
    severity: "warning",
    label: `${warningWindow.label} ${percent}%`,
    reason: `${warningWindow.label} usage is at ${percent}%.${formatReset(warningWindow.resetsAt)}`,
  };
}
