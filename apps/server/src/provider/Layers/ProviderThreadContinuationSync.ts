import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import { ProviderThreadContinuationSyncError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ProjectionStoreV2 } from "../../orchestration-v2/ProjectionStore.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ProviderThreadContinuationSync } from "../Services/ProviderThreadContinuationSync.ts";

export const makeProviderThreadContinuationSync = Effect.gen(function* () {
  const projections = yield* ProjectionStoreV2;
  const registry = yield* ProviderInstanceRegistry;
  const barrier = yield* ProviderRegistryRebuildBarrier;
  return ProviderThreadContinuationSync.of({
    sync: Effect.fn("ProviderThreadContinuationSync.sync")(function* (input) {
      const projection = yield* projections.getThreadProjection(input.threadId).pipe(
        Effect.mapError(
          () =>
            new ProviderThreadContinuationSyncError({
              code: "thread-not-bound",
              detail: "Thread projection is unavailable.",
            }),
        ),
      );
      const providerThread = projection.providerThreads.find(
        (thread) => thread.id === projection.thread.activeProviderThreadId,
      );
      if (!providerThread)
        return yield* new ProviderThreadContinuationSyncError({
          code: "thread-not-bound",
          detail: "Thread has no active native provider thread.",
        });
      if (providerThread.driver !== "claudeAgent")
        return yield* new ProviderThreadContinuationSyncError({
          code: "unsupported-provider",
          detail: "Native history sync is available for Claude.",
        });
      if (
        projection.runs.some((run) => run.status === "running") ||
        providerThread.status === "active"
      )
        return yield* new ProviderThreadContinuationSyncError({
          code: "turn-active",
          detail: "Wait for the active turn before syncing history.",
        });
      const instance = yield* registry.getInstance(providerThread.providerInstanceId);
      if (!instance?.orchestrationAdapter.syncContinuation)
        return yield* new ProviderThreadContinuationSyncError({
          code: "feature-disabled",
          detail: "The provider does not support native history sync.",
        });
      const nativeThreadId = providerThread.nativeThreadRef?.nativeId;
      if (!nativeThreadId)
        return yield* new ProviderThreadContinuationSyncError({
          code: "resume-state-missing",
          detail: "Thread has no native session id.",
        });
      const session = projection.providerSessions.find(
        (session) => session.id === providerThread.providerSessionId,
      );
      const state = yield* instance.orchestrationAdapter.syncContinuation({
        nativeThreadId,
        cwd: session?.cwd ?? "manual-sync",
        ...(providerThread.nativeConversationHeadRef?.nativeId
          ? { expectedAssistantUuid: providerThread.nativeConversationHeadRef.nativeId }
          : {}),
      });
      return {
        threadId: input.threadId,
        providerInstanceId: providerThread.providerInstanceId,
        state,
      };
    }, barrier.withOperation),
  });
});

export const ProviderThreadContinuationSyncLive = Layer.effect(
  ProviderThreadContinuationSync,
  makeProviderThreadContinuationSync,
);
