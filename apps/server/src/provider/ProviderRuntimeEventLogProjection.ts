import * as NodeCrypto from "node:crypto";

import type { ProviderRuntimeEvent, ThreadTokenUsageSnapshot } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

const LOGGER_IDENTIFIER_MAX_CHARS = 256;
// Built-in Codex/Claude/OpenCode/ACP identifiers are UUID/ULID-like tokens or
// synthetic colon-separated tokens. Their observed alphabet is alphanumeric
// plus dot, underscore, colon, and hyphen; path separators are never needed.
const LOGGER_IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/u;
const SIGNED_INT32_MIN = -2_147_483_648;
const SIGNED_INT32_MAX = 2_147_483_647;

const loggerIdentifier = (value: unknown): string | undefined =>
  typeof value === "string" &&
  value.length <= LOGGER_IDENTIFIER_MAX_CHARS &&
  LOGGER_IDENTIFIER_PATTERN.test(value)
    ? value
    : undefined;

const unsafeIdentifierDigest = (domain: string, value: string): string =>
  `sha256:${NodeCrypto.createHash("sha256")
    .update("t3-provider-canonical-log-identifier\0", "utf8")
    .update(domain, "utf8")
    .update("\0", "utf8")
    .update(value, "utf8")
    .digest("hex")}`;

const canonicalLoggerTimestamp = (value: unknown): string | undefined => {
  if (typeof value !== "string" || value.length > 40) return undefined;
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|([+-])(\d{2}):(\d{2}))$/u.exec(
      value,
    );
  if (match === null) return undefined;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[10] === undefined ? 0 : Number(match[10]);
  const offsetMinute = match[11] === undefined ? 0 : Number(match[11]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth[month - 1]! ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 14 ||
    offsetMinute > 59 ||
    (offsetHour === 14 && offsetMinute !== 0)
  ) {
    return undefined;
  }
  try {
    return DateTime.formatIso(DateTime.makeUnsafe(value));
  } catch {
    return undefined;
  }
};

const loggerNonNegativeSafeInteger = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;

const loggerNonNegativeFiniteNumber = (value: unknown): number | undefined =>
  typeof value === "number" &&
  Number.isFinite(value) &&
  value >= 0 &&
  value <= Number.MAX_SAFE_INTEGER
    ? value
    : undefined;

// Providers expose process status through several native runtimes. POSIX commonly uses 0..255,
// but no narrower cross-provider contract exists and Windows/native failures may be signed.
// Keep only exactly represented signed-32-bit values; never round or clamp.
const loggerExitCode = (value: unknown): number | undefined =>
  typeof value === "number" &&
  Number.isSafeInteger(value) &&
  value >= SIGNED_INT32_MIN &&
  value <= SIGNED_INT32_MAX
    ? value
    : undefined;

const loggerBoolean = (value: unknown): boolean | undefined =>
  typeof value === "boolean" ? value : undefined;

const LOGGER_EVENT_TYPES = new Set([
  "session.started",
  "session.configured",
  "session.state.changed",
  "session.exited",
  "thread.started",
  "thread.state.changed",
  "thread.metadata.updated",
  "thread.token-usage.updated",
  "thread.realtime.started",
  "thread.realtime.item-added",
  "thread.realtime.audio.delta",
  "thread.realtime.error",
  "thread.realtime.closed",
  "turn.started",
  "turn.completed",
  "turn.aborted",
  "turn.plan.updated",
  "turn.proposed.delta",
  "turn.proposed.completed",
  "turn.diff.updated",
  "item.started",
  "item.updated",
  "item.completed",
  "content.delta",
  "request.opened",
  "request.resolved",
  "user-input.requested",
  "user-input.resolved",
  "task.started",
  "task.progress",
  "task.updated",
  "task.completed",
  "task.backgrounds.changed",
  "hook.started",
  "hook.progress",
  "hook.completed",
  "tool.progress",
  "tool.summary",
  "auth.status",
  "account.updated",
  "account.rate-limits.updated",
  "mcp.status.updated",
  "mcp.oauth.completed",
  "model.rerouted",
  "config.warning",
  "deprecation.notice",
  "files.persisted",
  "tool.denied",
  "runtime.warning",
  "runtime.error",
]);
const loggerEventType = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_EVENT_TYPES.has(value) ? value : undefined;

const LOGGER_SESSION_STATES = new Set([
  "starting",
  "ready",
  "running",
  "waiting",
  "stopped",
  "error",
]);
const loggerSessionState = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_SESSION_STATES.has(value) ? value : undefined;

const LOGGER_THREAD_STATES = new Set([
  "active",
  "idle",
  "archived",
  "closed",
  "compacted",
  "error",
]);
const loggerThreadState = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_THREAD_STATES.has(value) ? value : undefined;

const LOGGER_TURN_STATES = new Set(["completed", "failed", "interrupted", "cancelled"]);
const loggerTurnState = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_TURN_STATES.has(value) ? value : undefined;

const LOGGER_ITEM_STATUSES = new Set(["inProgress", "completed", "failed", "declined"]);
const loggerItemStatus = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_ITEM_STATUSES.has(value) ? value : undefined;

const LOGGER_ITEM_TYPES = new Set([
  "user_message",
  "assistant_message",
  "reasoning",
  "plan",
  "command_execution",
  "file_change",
  "mcp_tool_call",
  "dynamic_tool_call",
  "collab_agent_tool_call",
  "web_search",
  "image_view",
  "review_entered",
  "review_exited",
  "context_compaction",
  "error",
  "unknown",
]);
const loggerItemType = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_ITEM_TYPES.has(value) ? value : undefined;

const LOGGER_STREAM_KINDS = new Set([
  "assistant_text",
  "reasoning_text",
  "reasoning_summary_text",
  "plan_text",
  "command_output",
  "file_change_output",
  "unknown",
]);
const loggerStreamKind = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_STREAM_KINDS.has(value) ? value : undefined;

const LOGGER_REQUEST_TYPES = new Set([
  "command_execution_approval",
  "file_read_approval",
  "file_change_approval",
  "apply_patch_approval",
  "exec_command_approval",
  "tool_user_input",
  "dynamic_tool_call",
  "auth_tokens_refresh",
  "unknown",
]);
const loggerRequestType = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_REQUEST_TYPES.has(value) ? value : undefined;

const LOGGER_TASK_STATUSES = new Set([
  "pending",
  "running",
  "paused",
  "completed",
  "failed",
  "stopped",
]);
const loggerTaskStatus = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_TASK_STATUSES.has(value) ? value : undefined;

const LOGGER_HOOK_OUTCOMES = new Set(["success", "error", "cancelled"]);
const loggerHookOutcome = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_HOOK_OUTCOMES.has(value) ? value : undefined;

const LOGGER_SESSION_EXIT_KINDS = new Set(["graceful", "error"]);
const loggerSessionExitKind = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_SESSION_EXIT_KINDS.has(value) ? value : undefined;

const LOGGER_ERROR_CLASSES = new Set([
  "provider_error",
  "transport_error",
  "permission_error",
  "validation_error",
  "unknown",
]);
const loggerErrorClass = (value: unknown): string | undefined =>
  typeof value === "string" && LOGGER_ERROR_CLASSES.has(value) ? value : undefined;

const technicalEventFields = (event: ProviderRuntimeEvent) => {
  const eventId = loggerIdentifier(event.eventId);
  const provider = loggerIdentifier(event.provider);
  const threadId = loggerIdentifier(event.threadId);
  const providerInstanceId = loggerIdentifier(event.providerInstanceId);
  const turnId = loggerIdentifier(event.turnId);
  const itemId = loggerIdentifier(event.itemId);
  const requestId = loggerIdentifier(event.requestId);
  const createdAt = canonicalLoggerTimestamp(event.createdAt);
  const providerTurnId = loggerIdentifier(event.providerRefs?.providerTurnId);
  const providerItemId = loggerIdentifier(event.providerRefs?.providerItemId);
  const providerRequestId = loggerIdentifier(event.providerRefs?.providerRequestId);
  const providerRefs = {
    ...(providerTurnId === undefined ? {} : { providerTurnId }),
    ...(providerItemId === undefined ? {} : { providerItemId }),
    ...(providerRequestId === undefined ? {} : { providerRequestId }),
  };
  return {
    ...(eventId === undefined
      ? { eventIdDigest: unsafeIdentifierDigest("event-id", String(event.eventId)) }
      : { eventId }),
    ...(provider === undefined
      ? { providerDigest: unsafeIdentifierDigest("provider", String(event.provider)) }
      : { provider }),
    ...(providerInstanceId === undefined ? {} : { providerInstanceId }),
    ...(threadId === undefined
      ? { threadIdDigest: unsafeIdentifierDigest("thread-id", String(event.threadId)) }
      : { threadId }),
    ...(createdAt === undefined ? {} : { createdAt }),
    ...(turnId === undefined ? {} : { turnId }),
    ...(itemId === undefined ? {} : { itemId }),
    ...(requestId === undefined ? {} : { requestId }),
    ...(Object.keys(providerRefs).length === 0 ? {} : { providerRefs }),
  };
};

const projectTechnicalEvent = (event: ProviderRuntimeEvent, payload: unknown): unknown => ({
  ...technicalEventFields(event),
  ...(loggerEventType(event.type) === undefined ? {} : { type: loggerEventType(event.type) }),
  payload,
});

const projectTokenUsage = (usage: ThreadTokenUsageSnapshot) => {
  const usedTokens = loggerNonNegativeSafeInteger(usage.usedTokens);
  const totalProcessedTokens = loggerNonNegativeSafeInteger(usage.totalProcessedTokens);
  const maxTokens = loggerNonNegativeSafeInteger(usage.maxTokens);
  const inputTokens = loggerNonNegativeSafeInteger(usage.inputTokens);
  const cachedInputTokens = loggerNonNegativeSafeInteger(usage.cachedInputTokens);
  const outputTokens = loggerNonNegativeSafeInteger(usage.outputTokens);
  const reasoningOutputTokens = loggerNonNegativeSafeInteger(usage.reasoningOutputTokens);
  const lastUsedTokens = loggerNonNegativeSafeInteger(usage.lastUsedTokens);
  const lastInputTokens = loggerNonNegativeSafeInteger(usage.lastInputTokens);
  const lastCachedInputTokens = loggerNonNegativeSafeInteger(usage.lastCachedInputTokens);
  const lastOutputTokens = loggerNonNegativeSafeInteger(usage.lastOutputTokens);
  const lastReasoningOutputTokens = loggerNonNegativeSafeInteger(usage.lastReasoningOutputTokens);
  const toolUses = loggerNonNegativeSafeInteger(usage.toolUses);
  const durationMs = loggerNonNegativeSafeInteger(usage.durationMs);
  const compactsAutomatically = loggerBoolean(usage.compactsAutomatically);
  return {
    ...(usedTokens === undefined ? {} : { usedTokens }),
    ...(totalProcessedTokens === undefined ? {} : { totalProcessedTokens }),
    ...(maxTokens === undefined ? {} : { maxTokens }),
    ...(inputTokens === undefined ? {} : { inputTokens }),
    ...(cachedInputTokens === undefined ? {} : { cachedInputTokens }),
    ...(outputTokens === undefined ? {} : { outputTokens }),
    ...(reasoningOutputTokens === undefined ? {} : { reasoningOutputTokens }),
    ...(lastUsedTokens === undefined ? {} : { lastUsedTokens }),
    ...(lastInputTokens === undefined ? {} : { lastInputTokens }),
    ...(lastCachedInputTokens === undefined ? {} : { lastCachedInputTokens }),
    ...(lastOutputTokens === undefined ? {} : { lastOutputTokens }),
    ...(lastReasoningOutputTokens === undefined ? {} : { lastReasoningOutputTokens }),
    ...(toolUses === undefined ? {} : { toolUses }),
    ...(durationMs === undefined ? {} : { durationMs }),
    ...(compactsAutomatically === undefined ? {} : { compactsAutomatically }),
  };
};

const commandExitCode = (event: ProviderRuntimeEvent): number | undefined => {
  if (
    (event.type !== "item.started" &&
      event.type !== "item.updated" &&
      event.type !== "item.completed") ||
    event.payload.itemType !== "command_execution" ||
    typeof event.payload.data !== "object" ||
    event.payload.data === null ||
    Array.isArray(event.payload.data)
  ) {
    return undefined;
  }
  return loggerExitCode((event.payload.data as Record<string, unknown>).exitCode);
};

const projectItemLifecycle = (
  event: Extract<
    ProviderRuntimeEvent,
    { readonly type: "item.started" | "item.updated" | "item.completed" }
  >,
) => {
  const exitCode = commandExitCode(event);
  const itemType = loggerItemType(event.payload.itemType);
  const status = loggerItemStatus(event.payload.status);
  return projectTechnicalEvent(event, {
    ...(itemType === undefined ? {} : { itemType }),
    ...(status === undefined ? {} : { status }),
    ...(exitCode === undefined ? {} : { exitCode }),
  });
};

/**
 * Build the observability-only projection written to the canonical provider
 * event log. Every known runtime variant constructs a new closed DTO. The
 * original event remains available to ingestion and evidence capture, while
 * raw objects and free-form content never cross this sink boundary.
 */
export function projectProviderRuntimeEventForCanonicalLog(
  event: ProviderRuntimeEvent,
): unknown | undefined {
  switch (event.type) {
    case "session.started":
      return projectTechnicalEvent(event, {});
    case "session.configured":
      return projectTechnicalEvent(event, {});
    case "session.state.changed": {
      const state = loggerSessionState(event.payload.state);
      return projectTechnicalEvent(event, state === undefined ? {} : { state });
    }
    case "session.exited": {
      const recoverable = loggerBoolean(event.payload.recoverable);
      const exitKind = loggerSessionExitKind(event.payload.exitKind);
      return projectTechnicalEvent(event, {
        ...(recoverable === undefined ? {} : { recoverable }),
        ...(exitKind === undefined ? {} : { exitKind }),
      });
    }
    case "thread.started": {
      const providerThreadId = loggerIdentifier(event.payload.providerThreadId);
      return projectTechnicalEvent(
        event,
        providerThreadId === undefined ? {} : { providerThreadId },
      );
    }
    case "thread.state.changed": {
      const state = loggerThreadState(event.payload.state);
      return projectTechnicalEvent(event, state === undefined ? {} : { state });
    }
    case "thread.metadata.updated":
      return projectTechnicalEvent(event, {});
    case "thread.token-usage.updated":
      return projectTechnicalEvent(event, { usage: projectTokenUsage(event.payload.usage) });
    case "thread.realtime.started": {
      const realtimeSessionId = loggerIdentifier(event.payload.realtimeSessionId);
      return projectTechnicalEvent(
        event,
        realtimeSessionId === undefined ? {} : { realtimeSessionId },
      );
    }
    case "thread.realtime.item-added":
      return projectTechnicalEvent(event, {});
    case "thread.realtime.audio.delta":
      return projectTechnicalEvent(event, {});
    case "thread.realtime.error":
      return projectTechnicalEvent(event, {});
    case "thread.realtime.closed":
      return projectTechnicalEvent(event, {});
    case "turn.started":
      return projectTechnicalEvent(event, {});
    case "turn.completed": {
      const state = loggerTurnState(event.payload.state);
      const totalCostUsd = loggerNonNegativeFiniteNumber(event.payload.totalCostUsd);
      return projectTechnicalEvent(event, {
        ...(state === undefined ? {} : { state }),
        ...(totalCostUsd === undefined ? {} : { totalCostUsd }),
      });
    }
    case "turn.aborted":
      return projectTechnicalEvent(event, {});
    case "turn.plan.updated":
      return projectTechnicalEvent(event, {});
    case "turn.proposed.delta":
      return projectTechnicalEvent(event, {});
    case "turn.proposed.completed":
      return projectTechnicalEvent(event, {});
    case "turn.diff.updated":
      return projectTechnicalEvent(event, {});
    case "item.started":
      return projectItemLifecycle(event);
    case "item.updated":
      return projectItemLifecycle(event);
    case "item.completed":
      return projectItemLifecycle(event);
    case "content.delta": {
      const streamKind = loggerStreamKind(event.payload.streamKind);
      const contentIndex = loggerNonNegativeSafeInteger(event.payload.contentIndex);
      const summaryIndex = loggerNonNegativeSafeInteger(event.payload.summaryIndex);
      return projectTechnicalEvent(event, {
        ...(streamKind === undefined ? {} : { streamKind }),
        ...(contentIndex === undefined ? {} : { contentIndex }),
        ...(summaryIndex === undefined ? {} : { summaryIndex }),
      });
    }
    case "request.opened": {
      const requestType = loggerRequestType(event.payload.requestType);
      return projectTechnicalEvent(event, requestType === undefined ? {} : { requestType });
    }
    case "request.resolved": {
      const requestType = loggerRequestType(event.payload.requestType);
      return projectTechnicalEvent(event, requestType === undefined ? {} : { requestType });
    }
    case "user-input.requested":
      return projectTechnicalEvent(event, {});
    case "user-input.resolved":
      return projectTechnicalEvent(event, {});
    case "task.started": {
      const taskId = loggerIdentifier(event.payload.taskId);
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const isBackgrounded = loggerBoolean(event.payload.isBackgrounded);
      const skipTranscript = loggerBoolean(event.payload.skipTranscript);
      return projectTechnicalEvent(event, {
        ...(taskId === undefined ? {} : { taskId }),
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(isBackgrounded === undefined ? {} : { isBackgrounded }),
        ...(skipTranscript === undefined ? {} : { skipTranscript }),
      });
    }
    case "task.progress": {
      const taskId = loggerIdentifier(event.payload.taskId);
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const isBackgrounded = loggerBoolean(event.payload.isBackgrounded);
      const skipTranscript = loggerBoolean(event.payload.skipTranscript);
      return projectTechnicalEvent(event, {
        ...(taskId === undefined ? {} : { taskId }),
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(isBackgrounded === undefined ? {} : { isBackgrounded }),
        ...(skipTranscript === undefined ? {} : { skipTranscript }),
      });
    }
    case "task.updated": {
      const taskId = loggerIdentifier(event.payload.taskId);
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const status = loggerTaskStatus(event.payload.status);
      const isBackgrounded = loggerBoolean(event.payload.isBackgrounded);
      const endedAtMs = loggerNonNegativeSafeInteger(event.payload.endedAtMs);
      const totalPausedMs = loggerNonNegativeSafeInteger(event.payload.totalPausedMs);
      const skipTranscript = loggerBoolean(event.payload.skipTranscript);
      return projectTechnicalEvent(event, {
        ...(taskId === undefined ? {} : { taskId }),
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(status === undefined ? {} : { status }),
        ...(isBackgrounded === undefined ? {} : { isBackgrounded }),
        ...(endedAtMs === undefined ? {} : { endedAtMs }),
        ...(totalPausedMs === undefined ? {} : { totalPausedMs }),
        ...(skipTranscript === undefined ? {} : { skipTranscript }),
      });
    }
    case "task.completed": {
      const taskId = loggerIdentifier(event.payload.taskId);
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const status = loggerTaskStatus(event.payload.status);
      const isBackgrounded = loggerBoolean(event.payload.isBackgrounded);
      const skipTranscript = loggerBoolean(event.payload.skipTranscript);
      return projectTechnicalEvent(event, {
        ...(taskId === undefined ? {} : { taskId }),
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(status === undefined ? {} : { status }),
        ...(isBackgrounded === undefined ? {} : { isBackgrounded }),
        ...(skipTranscript === undefined ? {} : { skipTranscript }),
      });
    }
    case "task.backgrounds.changed": {
      const taskCount = loggerNonNegativeSafeInteger(event.payload.tasks.length);
      return projectTechnicalEvent(event, taskCount === undefined ? {} : { taskCount });
    }
    case "hook.started": {
      const hookId = loggerIdentifier(event.payload.hookId);
      return projectTechnicalEvent(event, hookId === undefined ? {} : { hookId });
    }
    case "hook.progress": {
      const hookId = loggerIdentifier(event.payload.hookId);
      return projectTechnicalEvent(event, hookId === undefined ? {} : { hookId });
    }
    case "hook.completed": {
      const hookId = loggerIdentifier(event.payload.hookId);
      const outcome = loggerHookOutcome(event.payload.outcome);
      const exitCode = loggerExitCode(event.payload.exitCode);
      return projectTechnicalEvent(event, {
        ...(hookId === undefined ? {} : { hookId }),
        ...(outcome === undefined ? {} : { outcome }),
        ...(exitCode === undefined ? {} : { exitCode }),
      });
    }
    case "tool.progress": {
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const elapsedSeconds = loggerNonNegativeFiniteNumber(event.payload.elapsedSeconds);
      return projectTechnicalEvent(event, {
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(elapsedSeconds === undefined ? {} : { elapsedSeconds }),
      });
    }
    case "tool.summary": {
      const precedingToolUseIds = event.payload.precedingToolUseIds?.flatMap((value) => {
        const identifier = loggerIdentifier(value);
        return identifier === undefined ? [] : [identifier];
      });
      return projectTechnicalEvent(
        event,
        precedingToolUseIds === undefined ? {} : { precedingToolUseIds },
      );
    }
    case "auth.status": {
      const isAuthenticating = loggerBoolean(event.payload.isAuthenticating);
      return projectTechnicalEvent(
        event,
        isAuthenticating === undefined ? {} : { isAuthenticating },
      );
    }
    case "account.updated":
      return projectTechnicalEvent(event, {});
    case "account.rate-limits.updated":
      return projectTechnicalEvent(event, {});
    case "mcp.status.updated":
      return projectTechnicalEvent(event, {});
    case "mcp.oauth.completed": {
      const success = loggerBoolean(event.payload.success);
      return projectTechnicalEvent(event, success === undefined ? {} : { success });
    }
    case "model.rerouted":
      return projectTechnicalEvent(event, {});
    case "config.warning":
      return projectTechnicalEvent(event, {});
    case "deprecation.notice":
      return projectTechnicalEvent(event, {});
    case "files.persisted": {
      const fileCount = loggerNonNegativeSafeInteger(event.payload.files.length);
      const failedCount = loggerNonNegativeSafeInteger(event.payload.failed?.length ?? 0);
      return projectTechnicalEvent(event, {
        ...(fileCount === undefined ? {} : { fileCount }),
        ...(failedCount === undefined ? {} : { failedCount }),
      });
    }
    case "tool.denied": {
      const toolUseId = loggerIdentifier(event.payload.toolUseId);
      const agentId = loggerIdentifier(event.payload.agentId);
      return projectTechnicalEvent(event, {
        ...(toolUseId === undefined ? {} : { toolUseId }),
        ...(agentId === undefined ? {} : { agentId }),
      });
    }
    case "runtime.warning":
      return projectTechnicalEvent(event, {});
    case "runtime.error": {
      const errorClass = loggerErrorClass(event.payload.class);
      return projectTechnicalEvent(event, errorClass === undefined ? {} : { class: errorClass });
    }
    default: {
      // Runtime input is schema-validated today; this default remains closed if
      // an unvalidated future or foreign event reaches the logger. Adding a
      // typed contract variant fails this assignment until it receives a DTO.
      const unhandled: never = event;
      void unhandled;
      return undefined;
    }
  }
}
