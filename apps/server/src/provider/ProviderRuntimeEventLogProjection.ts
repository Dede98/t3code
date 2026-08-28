import type { ProviderRuntimeEvent, ThreadTokenUsageSnapshot } from "@t3tools/contracts";

const technicalEventFields = (event: ProviderRuntimeEvent) => ({
  eventId: event.eventId,
  provider: event.provider,
  ...(event.providerInstanceId === undefined
    ? {}
    : { providerInstanceId: event.providerInstanceId }),
  threadId: event.threadId,
  createdAt: event.createdAt,
  ...(event.turnId === undefined ? {} : { turnId: event.turnId }),
  ...(event.itemId === undefined ? {} : { itemId: event.itemId }),
  ...(event.requestId === undefined ? {} : { requestId: event.requestId }),
  ...(event.providerRefs === undefined
    ? {}
    : {
        providerRefs: {
          ...(event.providerRefs.providerTurnId === undefined
            ? {}
            : { providerTurnId: event.providerRefs.providerTurnId }),
          ...(event.providerRefs.providerItemId === undefined
            ? {}
            : { providerItemId: event.providerRefs.providerItemId }),
          ...(event.providerRefs.providerRequestId === undefined
            ? {}
            : { providerRequestId: event.providerRefs.providerRequestId }),
        },
      }),
});

const projectTechnicalEvent = (event: ProviderRuntimeEvent, payload: unknown): unknown => ({
  ...technicalEventFields(event),
  type: event.type,
  payload,
});

const projectTokenUsage = (usage: ThreadTokenUsageSnapshot) => ({
  usedTokens: usage.usedTokens,
  ...(usage.totalProcessedTokens === undefined
    ? {}
    : { totalProcessedTokens: usage.totalProcessedTokens }),
  ...(usage.maxTokens === undefined ? {} : { maxTokens: usage.maxTokens }),
  ...(usage.inputTokens === undefined ? {} : { inputTokens: usage.inputTokens }),
  ...(usage.cachedInputTokens === undefined ? {} : { cachedInputTokens: usage.cachedInputTokens }),
  ...(usage.outputTokens === undefined ? {} : { outputTokens: usage.outputTokens }),
  ...(usage.reasoningOutputTokens === undefined
    ? {}
    : { reasoningOutputTokens: usage.reasoningOutputTokens }),
  ...(usage.lastUsedTokens === undefined ? {} : { lastUsedTokens: usage.lastUsedTokens }),
  ...(usage.lastInputTokens === undefined ? {} : { lastInputTokens: usage.lastInputTokens }),
  ...(usage.lastCachedInputTokens === undefined
    ? {}
    : { lastCachedInputTokens: usage.lastCachedInputTokens }),
  ...(usage.lastOutputTokens === undefined ? {} : { lastOutputTokens: usage.lastOutputTokens }),
  ...(usage.lastReasoningOutputTokens === undefined
    ? {}
    : { lastReasoningOutputTokens: usage.lastReasoningOutputTokens }),
  ...(usage.toolUses === undefined ? {} : { toolUses: usage.toolUses }),
  ...(usage.durationMs === undefined ? {} : { durationMs: usage.durationMs }),
  ...(usage.compactsAutomatically === undefined
    ? {}
    : { compactsAutomatically: usage.compactsAutomatically }),
});

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
  const exitCode = (event.payload.data as Record<string, unknown>).exitCode;
  return typeof exitCode === "number" && Number.isInteger(exitCode) ? exitCode : undefined;
};

const projectItemLifecycle = (
  event: Extract<
    ProviderRuntimeEvent,
    { readonly type: "item.started" | "item.updated" | "item.completed" }
  >,
) => {
  const exitCode = commandExitCode(event);
  return projectTechnicalEvent(event, {
    itemType: event.payload.itemType,
    ...(event.payload.status === undefined ? {} : { status: event.payload.status }),
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
    case "session.state.changed":
      return projectTechnicalEvent(event, { state: event.payload.state });
    case "session.exited":
      return projectTechnicalEvent(event, {
        ...(event.payload.recoverable === undefined
          ? {}
          : { recoverable: event.payload.recoverable }),
        ...(event.payload.exitKind === undefined ? {} : { exitKind: event.payload.exitKind }),
      });
    case "thread.started":
      return projectTechnicalEvent(
        event,
        event.payload.providerThreadId === undefined
          ? {}
          : { providerThreadId: event.payload.providerThreadId },
      );
    case "thread.state.changed":
      return projectTechnicalEvent(event, { state: event.payload.state });
    case "thread.metadata.updated":
      return projectTechnicalEvent(event, {});
    case "thread.token-usage.updated":
      return projectTechnicalEvent(event, { usage: projectTokenUsage(event.payload.usage) });
    case "thread.realtime.started":
      return projectTechnicalEvent(
        event,
        event.payload.realtimeSessionId === undefined
          ? {}
          : { realtimeSessionId: event.payload.realtimeSessionId },
      );
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
    case "turn.completed":
      return projectTechnicalEvent(event, {
        state: event.payload.state,
        ...(event.payload.totalCostUsd === undefined
          ? {}
          : { totalCostUsd: event.payload.totalCostUsd }),
      });
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
    case "content.delta":
      return projectTechnicalEvent(event, {
        streamKind: event.payload.streamKind,
        ...(event.payload.contentIndex === undefined
          ? {}
          : { contentIndex: event.payload.contentIndex }),
        ...(event.payload.summaryIndex === undefined
          ? {}
          : { summaryIndex: event.payload.summaryIndex }),
      });
    case "request.opened":
      return projectTechnicalEvent(event, { requestType: event.payload.requestType });
    case "request.resolved":
      return projectTechnicalEvent(event, { requestType: event.payload.requestType });
    case "user-input.requested":
      return projectTechnicalEvent(event, {});
    case "user-input.resolved":
      return projectTechnicalEvent(event, {});
    case "task.started":
      return projectTechnicalEvent(event, {
        taskId: event.payload.taskId,
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        ...(event.payload.isBackgrounded === undefined
          ? {}
          : { isBackgrounded: event.payload.isBackgrounded }),
        ...(event.payload.skipTranscript === undefined
          ? {}
          : { skipTranscript: event.payload.skipTranscript }),
      });
    case "task.progress":
      return projectTechnicalEvent(event, {
        taskId: event.payload.taskId,
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        ...(event.payload.isBackgrounded === undefined
          ? {}
          : { isBackgrounded: event.payload.isBackgrounded }),
        ...(event.payload.skipTranscript === undefined
          ? {}
          : { skipTranscript: event.payload.skipTranscript }),
      });
    case "task.updated":
      return projectTechnicalEvent(event, {
        taskId: event.payload.taskId,
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        ...(event.payload.status === undefined ? {} : { status: event.payload.status }),
        ...(event.payload.isBackgrounded === undefined
          ? {}
          : { isBackgrounded: event.payload.isBackgrounded }),
        ...(event.payload.endedAtMs === undefined ? {} : { endedAtMs: event.payload.endedAtMs }),
        ...(event.payload.totalPausedMs === undefined
          ? {}
          : { totalPausedMs: event.payload.totalPausedMs }),
        ...(event.payload.skipTranscript === undefined
          ? {}
          : { skipTranscript: event.payload.skipTranscript }),
      });
    case "task.completed":
      return projectTechnicalEvent(event, {
        taskId: event.payload.taskId,
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        status: event.payload.status,
        ...(event.payload.isBackgrounded === undefined
          ? {}
          : { isBackgrounded: event.payload.isBackgrounded }),
        ...(event.payload.skipTranscript === undefined
          ? {}
          : { skipTranscript: event.payload.skipTranscript }),
      });
    case "task.backgrounds.changed":
      return projectTechnicalEvent(event, { taskCount: event.payload.tasks.length });
    case "hook.started":
      return projectTechnicalEvent(event, { hookId: event.payload.hookId });
    case "hook.progress":
      return projectTechnicalEvent(event, { hookId: event.payload.hookId });
    case "hook.completed":
      return projectTechnicalEvent(event, {
        hookId: event.payload.hookId,
        outcome: event.payload.outcome,
        ...(event.payload.exitCode === undefined ? {} : { exitCode: event.payload.exitCode }),
      });
    case "tool.progress":
      return projectTechnicalEvent(event, {
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        ...(event.payload.elapsedSeconds === undefined
          ? {}
          : { elapsedSeconds: event.payload.elapsedSeconds }),
      });
    case "tool.summary":
      return projectTechnicalEvent(
        event,
        event.payload.precedingToolUseIds === undefined
          ? {}
          : { precedingToolUseIds: [...event.payload.precedingToolUseIds] },
      );
    case "auth.status":
      return projectTechnicalEvent(
        event,
        event.payload.isAuthenticating === undefined
          ? {}
          : { isAuthenticating: event.payload.isAuthenticating },
      );
    case "account.updated":
      return projectTechnicalEvent(event, {});
    case "account.rate-limits.updated":
      return projectTechnicalEvent(event, {});
    case "mcp.status.updated":
      return projectTechnicalEvent(event, {});
    case "mcp.oauth.completed":
      return projectTechnicalEvent(event, { success: event.payload.success });
    case "model.rerouted":
      return projectTechnicalEvent(event, {});
    case "config.warning":
      return projectTechnicalEvent(event, {});
    case "deprecation.notice":
      return projectTechnicalEvent(event, {});
    case "files.persisted":
      return projectTechnicalEvent(event, {
        fileCount: event.payload.files.length,
        failedCount: event.payload.failed?.length ?? 0,
      });
    case "tool.denied":
      return projectTechnicalEvent(event, {
        ...(event.payload.toolUseId === undefined ? {} : { toolUseId: event.payload.toolUseId }),
        ...(event.payload.agentId === undefined ? {} : { agentId: event.payload.agentId }),
      });
    case "runtime.warning":
      return projectTechnicalEvent(event, {});
    case "runtime.error":
      return projectTechnicalEvent(
        event,
        event.payload.class === undefined ? {} : { class: event.payload.class },
      );
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
