import {
  ProviderDriverKind,
  ProviderThreadContinuationSyncError,
  type ProviderThreadContinuationSyncErrorCode,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { CLAUDE_SESSION_STORE_CONTINUATION_KEY } from "../Services/ClaudeSessionStore.ts";
import { ProviderContinuationSyncCapabilityError } from "../Services/ProviderAdapter.ts";
import { ProviderAdapterRegistry } from "../Services/ProviderAdapterRegistry.ts";
import { ProviderSessionDirectory } from "../Services/ProviderSessionDirectory.ts";
import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import { ProviderThreadOperationLock } from "../Services/ProviderThreadOperationLock.ts";
import {
  ProviderThreadContinuationSync,
  type ProviderThreadContinuationSyncShape,
} from "../Services/ProviderThreadContinuationSync.ts";

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const isCapabilityError = Schema.is(ProviderContinuationSyncCapabilityError);

function syncError(
  code: ProviderThreadContinuationSyncErrorCode,
  detail: string,
): ProviderThreadContinuationSyncError {
  return new ProviderThreadContinuationSyncError({ code, detail });
}

function readPersistedCwd(runtimePayload: unknown): string | undefined {
  if (typeof runtimePayload !== "object" || runtimePayload === null) return undefined;
  const cwd = Reflect.get(runtimePayload, "cwd");
  return typeof cwd === "string" && cwd.trim().length > 0 ? cwd : undefined;
}

export const makeProviderThreadContinuationSync = Effect.gen(function* () {
  const directory = yield* ProviderSessionDirectory;
  const registry = yield* ProviderAdapterRegistry;
  const operationLock = yield* ProviderThreadOperationLock;
  const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;

  const syncUnlocked = Effect.fn("ProviderThreadContinuationSync.sync")(function* (
    input: Parameters<ProviderThreadContinuationSyncShape["sync"]>[0],
  ) {
    const binding = Option.getOrUndefined(
      yield* directory
        .getBinding(input.threadId)
        .pipe(
          Effect.mapError(() =>
            syncError(
              "sync-failed",
              `Failed to load the provider binding for thread '${input.threadId}'.`,
            ),
          ),
        ),
    );
    if (binding === undefined || binding.providerInstanceId === undefined) {
      return yield* syncError(
        "thread-not-bound",
        `Thread '${input.threadId}' is not bound to a provider instance.`,
      );
    }

    const instanceInfo = yield* registry
      .getInstanceInfo(binding.providerInstanceId)
      .pipe(
        Effect.mapError(() =>
          syncError(
            "sync-failed",
            `Provider instance '${binding.providerInstanceId}' is unavailable.`,
          ),
        ),
      );
    if (instanceInfo.driverKind !== CLAUDE_DRIVER) {
      return yield* syncError(
        "unsupported-provider",
        `Provider '${instanceInfo.driverKind}' does not support manual thread continuation sync.`,
      );
    }
    if (
      instanceInfo.continuationIdentity.continuationKey !== CLAUDE_SESSION_STORE_CONTINUATION_KEY
    ) {
      return yield* syncError(
        "feature-disabled",
        "Claude cross-account thread continuation must be enabled before syncing this thread.",
      );
    }
    const adapter = yield* registry
      .getByInstance(binding.providerInstanceId)
      .pipe(
        Effect.mapError(() =>
          syncError(
            "sync-failed",
            `Provider instance '${binding.providerInstanceId}' is unavailable.`,
          ),
        ),
      );
    const liveSession = (yield* adapter.listSessions()).find(
      (session) => session.threadId === input.threadId,
    );
    if (
      liveSession?.status === "connecting" ||
      liveSession?.status === "running" ||
      liveSession?.activeTurnId !== undefined
    ) {
      return yield* syncError(
        "turn-active",
        `Thread '${input.threadId}' cannot be synced while its provider turn is active.`,
      );
    }

    const resumeCursor = liveSession?.resumeCursor ?? binding.resumeCursor;
    if (resumeCursor === undefined || resumeCursor === null) {
      return yield* syncError(
        "resume-state-missing",
        `Thread '${input.threadId}' has no Claude resume state to sync.`,
      );
    }

    if (adapter.syncContinuation === undefined) {
      return yield* syncError(
        "feature-disabled",
        "The bound Claude provider instance does not expose cross-account continuation sync.",
      );
    }

    const persistedCwd = readPersistedCwd(binding.runtimePayload);
    const state = yield* adapter
      .syncContinuation({
        threadId: input.threadId,
        resumeCursor,
        ...(persistedCwd !== undefined ? { cwd: persistedCwd } : {}),
      })
      .pipe(
        Effect.mapError((cause) =>
          isCapabilityError(cause)
            ? syncError(cause.code, cause.detail)
            : syncError(
                "sync-failed",
                `Failed to sync the Claude transcript for thread '${input.threadId}'.`,
              ),
        ),
      );

    return {
      threadId: input.threadId,
      providerInstanceId: binding.providerInstanceId,
      state,
    };
  });
  const sync: ProviderThreadContinuationSyncShape["sync"] = (input) =>
    rebuildBarrier.withOperation(operationLock.withLock(input.threadId, syncUnlocked(input)));

  return ProviderThreadContinuationSync.of({ sync });
});

export const ProviderThreadContinuationSyncLive = Layer.effect(
  ProviderThreadContinuationSync,
  makeProviderThreadContinuationSync,
);
