import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import { formatResetsIn, remainingPercent } from "@t3tools/shared/usageLimits";

export type ProviderUsageAttention = {
  readonly severity: "warning";
  readonly label: string;
  readonly reason: string;
};

export function currentUsageWindows(limits: ServerProviderUsageLimits | undefined, now: number) {
  if (!limits || limits.unavailable) return [];
  return limits.windows.filter(
    (window) => window.resetsAt === undefined || Date.parse(window.resetsAt) > now,
  );
}

export function selectPrimaryUsageWindow(
  limits: ServerProviderUsageLimits | undefined,
  now: number = Date.now(),
): ServerProviderUsageWindow | null {
  const windows = currentUsageWindows(limits, now);
  return (
    windows
      .filter((window) => window.usedPercent > 90)
      .toSorted((a, b) => b.usedPercent - a.usedPercent)[0] ??
    windows.find((window) => window.kind === "session") ??
    windows.find((window) => window.kind === "weekly") ??
    windows[0] ??
    null
  );
}

/** Limits describe quota, not admission: scoped limits and overage can still allow turns. */
export function getProviderUsageAttention(
  limits: ServerProviderUsageLimits | undefined,
  now: number = Date.now(),
): ProviderUsageAttention | null {
  const window = selectPrimaryUsageWindow(limits, now);
  if (!window || window.usedPercent <= 90) return null;
  const label = `${window.label} ${remainingPercent(window)}% left`;
  const reset = formatResetsIn(window, now);
  return {
    severity: "warning",
    label,
    reason: `${label}.${reset ? ` ${reset}.` : ""}`,
  };
}
