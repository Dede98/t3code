import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import { remainingPercent } from "@t3tools/shared/usageLimits";
import { formatResetsIn } from "@t3tools/shared/usageLimitsCopy";
import { msg } from "gt-react";

import { m, type MessageResolver } from "../../i18n/messages";

export type ProviderUsageAttention = {
  readonly severity: "warning";
  readonly label: string;
  readonly reason: string;
};

function currentUsageWindows(limits: ServerProviderUsageLimits | undefined, now: number) {
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
  resolve: MessageResolver = m,
): ProviderUsageAttention | null {
  const window = selectPrimaryUsageWindow(limits, now);
  if (!window || window.usedPercent <= 90) return null;
  const variables = { label: window.label, percent: remainingPercent(window) };
  const reset = formatResetsIn(window, now, resolve);
  return {
    severity: "warning",
    label: resolve(msg("{label} {percent}% left", variables)),
    reason: reset
      ? resolve(msg("{label} {percent}% left. {reset}.", { ...variables, reset }))
      : resolve(msg("{label} {percent}% left.", variables)),
  };
}
