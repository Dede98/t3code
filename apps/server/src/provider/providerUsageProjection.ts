import {
  type ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderUsageSnapshot,
  type ProviderUsageStatus,
  type ProviderUsageWindow,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";

type UnknownRecord = Readonly<Record<string, unknown>>;

const asRecord = (value: unknown): UnknownRecord | null =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as UnknownRecord)
    : null;

const finiteNumber = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) ? value : null;

const clampPercent = (value: number): number => Math.max(0, Math.min(100, value));

function epochToIso(value: unknown): string | null {
  const epoch = finiteNumber(value);
  if (epoch === null || epoch <= 0) return null;
  const millis = epoch < 1_000_000_000_000 ? epoch * 1_000 : epoch;
  return Option.match(DateTime.make(millis), {
    onNone: () => null,
    onSome: DateTime.formatIso,
  });
}

function statusFromClaude(value: unknown): ProviderUsageStatus {
  if (value === "rejected") return "rejected";
  if (value === "allowed_warning") return "warning";
  return "allowed";
}

function claudeWindowMetadata(rateLimitType: string): {
  readonly label: string;
  readonly durationMinutes?: number;
} {
  switch (rateLimitType) {
    case "five_hour":
      return { label: "5 hours", durationMinutes: 300 };
    case "seven_day":
      return { label: "Weekly", durationMinutes: 10_080 };
    case "seven_day_overage_included":
      return { label: "Fable 5", durationMinutes: 10_080 };
    case "seven_day_opus":
      return { label: "Weekly Opus", durationMinutes: 10_080 };
    case "seven_day_sonnet":
      return { label: "Weekly Sonnet", durationMinutes: 10_080 };
    default:
      return { label: rateLimitType.replaceAll("_", " ") };
  }
}

function headerResetToIso(value: string | undefined): string | null {
  if (!value) return null;
  const numericValue = Number(value);
  return epochToIso(Number.isFinite(numericValue) ? numericValue : Date.parse(value));
}

function labelForDuration(durationMinutes: number | null, fallback: string): string {
  if (durationMinutes === 300) return "5 hours";
  if (durationMinutes === 10_080) return "Weekly";
  if (durationMinutes !== null && durationMinutes % 1_440 === 0) {
    return `${durationMinutes / 1_440} days`;
  }
  if (durationMinutes !== null && durationMinutes % 60 === 0) {
    return `${durationMinutes / 60} hours`;
  }
  return fallback;
}

function upsertWindow(
  windows: ReadonlyArray<ProviderUsageWindow>,
  window: ProviderUsageWindow,
): ReadonlyArray<ProviderUsageWindow> {
  const index = windows.findIndex((candidate) => candidate.id === window.id);
  if (index === -1) return [...windows, window];
  return windows.map((candidate, candidateIndex) =>
    candidateIndex === index ? window : candidate,
  );
}

function codexWindow(id: "primary" | "secondary", value: unknown): ProviderUsageWindow | null {
  const record = asRecord(value);
  const usedPercent = finiteNumber(record?.usedPercent);
  if (!record || usedPercent === null) return null;
  const durationMinutes = finiteNumber(record.windowDurationMins);
  return {
    id,
    label: labelForDuration(durationMinutes, id === "primary" ? "Session" : "Secondary"),
    usedPercent: clampPercent(usedPercent),
    resetsAt: epochToIso(record.resetsAt),
    ...(durationMinutes !== null ? { durationMinutes } : {}),
  };
}

export function projectCodexUsage(input: {
  providerInstanceId: ProviderInstanceId;
  driver: ProviderDriverKind;
  observedAt: string;
  source: ProviderUsageSnapshot["source"];
  rateLimits: unknown;
  previous?: ProviderUsageSnapshot;
}): ProviderUsageSnapshot | null {
  const notification = asRecord(input.rateLimits);
  const limits = asRecord(notification?.rateLimits) ?? notification;
  if (!limits) return null;
  let windows = input.previous?.windows ?? [];
  const primary = codexWindow("primary", limits.primary);
  const secondary = codexWindow("secondary", limits.secondary);
  if (primary) windows = upsertWindow(windows, primary);
  if (secondary) windows = upsertWindow(windows, secondary);
  if (!primary && !secondary && !input.previous) return null;
  const highestUsage = windows.reduce(
    (highest, window) => Math.max(highest, window.usedPercent),
    0,
  );
  return {
    providerInstanceId: input.providerInstanceId,
    driver: input.driver,
    observedAt: input.observedAt,
    source: input.source,
    status:
      typeof limits.rateLimitReachedType === "string"
        ? "rejected"
        : highestUsage >= 90
          ? "warning"
          : "allowed",
    windows,
  };
}

export function projectClaudeUsageHeaders(input: {
  providerInstanceId: ProviderInstanceId;
  driver: ProviderDriverKind;
  observedAt: string;
  headers: Readonly<Record<string, string | undefined>>;
}): ProviderUsageSnapshot | null {
  const windows: ProviderUsageWindow[] = [];
  for (const window of [
    {
      id: "five_hour",
      label: "5 hours",
      prefix: "anthropic-ratelimit-unified-5h",
      durationMinutes: 300,
    },
    {
      id: "seven_day",
      label: "Weekly",
      prefix: "anthropic-ratelimit-unified-7d",
      durationMinutes: 10_080,
    },
    {
      id: "seven_day_overage_included",
      label: "Fable 5",
      prefix: "anthropic-ratelimit-unified-7d_oi",
      durationMinutes: 10_080,
    },
  ] as const) {
    const rawUtilization = input.headers[`${window.prefix}-utilization`];
    const utilization = rawUtilization === undefined ? Number.NaN : Number(rawUtilization);
    if (!Number.isFinite(utilization)) continue;
    const rawReset = input.headers[`${window.prefix}-reset`];
    windows.push({
      id: window.id,
      label: window.label,
      usedPercent: clampPercent(utilization <= 1 ? utilization * 100 : utilization),
      resetsAt: headerResetToIso(rawReset),
      durationMinutes: window.durationMinutes,
    });
  }
  if (windows.length === 0) return null;
  const status = statusFromClaude(input.headers["anthropic-ratelimit-unified-status"]);
  return {
    providerInstanceId: input.providerInstanceId,
    driver: input.driver,
    observedAt: input.observedAt,
    source: "refresh",
    status,
    windows,
    isUsingOverage: input.headers["anthropic-ratelimit-unified-overage-status"] === "allowed",
  };
}

export function projectClaudeUsageResponse(input: {
  providerInstanceId: ProviderInstanceId;
  driver: ProviderDriverKind;
  observedAt: string;
  response: unknown;
}): ProviderUsageSnapshot | null {
  const response = asRecord(input.response);
  if (!response) return null;
  const windows: ProviderUsageWindow[] = [];
  for (const [id, label] of [
    ["five_hour", "5 hours"],
    ["seven_day", "Weekly"],
    ["seven_day_overage_included", "Fable 5"],
    ["seven_day_opus", "Weekly Opus"],
    ["seven_day_sonnet", "Weekly Sonnet"],
  ] as const) {
    const value = asRecord(response[id]);
    const utilization = finiteNumber(value?.utilization);
    if (utilization === null) continue;
    windows.push({
      id,
      label,
      usedPercent: clampPercent(utilization),
      resetsAt: typeof value?.resets_at === "string" ? headerResetToIso(value.resets_at) : null,
      durationMinutes: id === "five_hour" ? 300 : 10_080,
    });
  }

  const scopedLimits = Array.isArray(response.limits) ? response.limits : [];
  for (const rawLimit of scopedLimits) {
    const limit = asRecord(rawLimit);
    const scope = asRecord(limit?.scope);
    const model = asRecord(scope?.model);
    const displayName = typeof model?.display_name === "string" ? model.display_name.trim() : "";
    const percent = finiteNumber(limit?.percent);
    if (limit?.kind !== "weekly_scoped" || !displayName || percent === null) continue;
    const isFable = displayName.toLowerCase().includes("fable");
    const id = isFable
      ? "seven_day_overage_included"
      : `weekly_scoped_${displayName.toLowerCase().replaceAll(/[^a-z0-9]+/g, "_")}`;
    const window = {
      id,
      label: displayName,
      usedPercent: clampPercent(percent),
      resetsAt: typeof limit.resets_at === "string" ? headerResetToIso(limit.resets_at) : null,
      durationMinutes: 10_080,
    } satisfies ProviderUsageWindow;
    const existingIndex = windows.findIndex((candidate) => candidate.id === id);
    if (existingIndex === -1) windows.push(window);
    else windows[existingIndex] = window;
  }

  if (windows.length === 0) return null;
  const highestUsage = windows.reduce(
    (highest, window) => Math.max(highest, window.usedPercent),
    0,
  );
  return {
    providerInstanceId: input.providerInstanceId,
    driver: input.driver,
    observedAt: input.observedAt,
    source: "refresh",
    status: highestUsage >= 100 ? "rejected" : highestUsage >= 90 ? "warning" : "allowed",
    windows,
  };
}

export function projectProviderUsageEvent(
  event: ProviderRuntimeEvent,
  previous?: ProviderUsageSnapshot,
): ProviderUsageSnapshot | null {
  const providerInstanceId = event.providerInstanceId;
  if (event.type !== "account.rate-limits.updated" || providerInstanceId === undefined) return null;
  if (event.provider === "codex") {
    return projectCodexUsage({
      providerInstanceId,
      driver: event.provider,
      observedAt: event.createdAt,
      source: "runtime-event",
      rateLimits: event.payload.rateLimits,
      ...(previous ? { previous } : {}),
    });
  }
  if (event.provider !== "claudeAgent") return null;
  const raw = asRecord(event.payload.rateLimits);
  const info = asRecord(raw?.rate_limit_info);
  if (!info) return null;
  const rateLimitType = typeof info.rateLimitType === "string" ? info.rateLimitType : null;
  const utilization = finiteNumber(info.utilization);
  let windows = previous?.windows ?? [];
  if (rateLimitType && rateLimitType !== "overage" && utilization !== null) {
    const meta = claudeWindowMetadata(rateLimitType);
    windows = upsertWindow(windows, {
      id: rateLimitType,
      label: meta.label,
      usedPercent: clampPercent(utilization <= 1 ? utilization * 100 : utilization),
      resetsAt: epochToIso(info.resetsAt),
      ...(meta.durationMinutes ? { durationMinutes: meta.durationMinutes } : {}),
    });
  }
  const isUsingOverage =
    typeof (info.isUsingOverage ?? info.overageInUse) === "boolean"
      ? ((info.isUsingOverage ?? info.overageInUse) as boolean)
      : previous?.isUsingOverage;
  return {
    providerInstanceId,
    driver: event.provider,
    observedAt: event.createdAt,
    source: "runtime-event",
    status: statusFromClaude(info.status),
    windows,
    ...(info.overageStatus !== undefined
      ? { overageStatus: statusFromClaude(info.overageStatus) }
      : previous?.overageStatus
        ? { overageStatus: previous.overageStatus }
        : {}),
    ...(info.overageResetsAt !== undefined
      ? { overageResetsAt: epochToIso(info.overageResetsAt) }
      : previous?.overageResetsAt !== undefined
        ? { overageResetsAt: previous.overageResetsAt }
        : {}),
    ...(isUsingOverage !== undefined ? { isUsingOverage } : {}),
  };
}
