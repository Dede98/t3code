export function formatProviderUsageResetAt(
  value: string | null,
  nowMs: number = Date.now(),
): string {
  if (!value) return "Reset time unavailable";
  const reset = new Date(value);
  const remainingMinutes = Math.max(0, Math.round((reset.getTime() - nowMs) / 60_000));
  if (!Number.isFinite(remainingMinutes)) return "Reset time unavailable";
  if (remainingMinutes < 60) return `Resets in ${remainingMinutes}m`;
  const hours = Math.floor(remainingMinutes / 60);
  const minutes = remainingMinutes % 60;
  if (hours < 48) return `Resets in ${hours}h${minutes > 0 ? ` ${minutes}m` : ""}`;
  return `Resets ${reset.toLocaleString(undefined, {
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
  })}`;
}
