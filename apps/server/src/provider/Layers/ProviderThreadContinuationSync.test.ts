import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { describe, expect, it, vi } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { CLAUDE_SESSION_STORE_CONTINUATION_KEY } from "../Services/ClaudeSessionStore.ts";
import {
  ProviderContinuationSyncCapabilityError,
  type ProviderAdapterShape,
} from "../Services/ProviderAdapter.ts";
import {
  ProviderAdapterRegistry,
  type ProviderAdapterRegistryShape,
} from "../Services/ProviderAdapterRegistry.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBinding,
  type ProviderSessionDirectoryShape,
} from "../Services/ProviderSessionDirectory.ts";
import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import {
  ProviderThreadOperationLock,
  type ProviderThreadOperationLockShape,
} from "../Services/ProviderThreadOperationLock.ts";
import { makeProviderThreadContinuationSync } from "./ProviderThreadContinuationSync.ts";
import { makeProviderThreadOperationLock } from "./ProviderThreadOperationLock.ts";
import { makeProviderRegistryRebuildBarrier } from "./ProviderRegistryRebuildBarrier.ts";

const THREAD_ID = ThreadId.make("thread-sync");
const INSTANCE_ID = ProviderInstanceId.make("claude-work");
const RESUME_CURSOR = { resume: "session-1" };

function makeAdapter(input?: {
  readonly state?: "imported" | "already-synced";
  readonly sessionStatus?: "connecting" | "ready" | "running";
  readonly capability?: "present" | "missing";
  readonly capabilityError?: "transcript-not-found" | "sync-failed";
  readonly liveResumeCursor?: unknown;
}): ProviderAdapterShape<never> {
  const syncContinuation = () =>
    input?.capabilityError
      ? Effect.fail(
          new ProviderContinuationSyncCapabilityError({
            code: input.capabilityError,
            detail: "Native Claude transcript was not found.",
          }),
        )
      : Effect.succeed(input?.state ?? "imported");
  return {
    provider: ProviderDriverKind.make("claudeAgent"),
    capabilities: { sessionModelSwitch: "in-session" },
    startSession: () => Effect.die("unused"),
    sendTurn: () => Effect.die("unused"),
    interruptTurn: () => Effect.die("unused"),
    respondToRequest: () => Effect.die("unused"),
    respondToUserInput: () => Effect.die("unused"),
    stopSession: () => Effect.die("unused"),
    listSessions: () =>
      input?.sessionStatus
        ? Effect.succeed([
            {
              provider: ProviderDriverKind.make("claudeAgent"),
              providerInstanceId: INSTANCE_ID,
              status: input.sessionStatus,
              runtimeMode: "full-access" as const,
              threadId: THREAD_ID,
              ...(input.sessionStatus === "running" ? { activeTurnId: "turn-1" as never } : {}),
              ...(input.liveResumeCursor !== undefined
                ? { resumeCursor: input.liveResumeCursor }
                : {}),
              createdAt: "2026-07-17T00:00:00.000Z",
              updatedAt: "2026-07-17T00:00:00.000Z",
            },
          ])
        : Effect.succeed([]),
    hasSession: () => Effect.succeed(false),
    readThread: () => Effect.die("unused"),
    rollbackThread: () => Effect.die("unused"),
    stopAll: () => Effect.die("unused"),
    streamEvents: Stream.empty,
    ...(input?.capability === "missing" ? {} : { syncContinuation }),
  } as ProviderAdapterShape<never>;
}

function makeService(input?: {
  readonly adapter?: ProviderAdapterShape<never>;
  readonly binding?: ProviderRuntimeBinding | undefined;
  readonly driverKind?: "claudeAgent" | "codex";
  readonly continuationKey?: string;
  readonly operationLock?: ProviderThreadOperationLockShape;
}) {
  const binding =
    "binding" in (input ?? {})
      ? input?.binding
      : {
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: INSTANCE_ID,
          resumeCursor: RESUME_CURSOR,
        };
  const directory = {
    getBinding: () => Effect.succeed(Option.fromNullishOr(binding)),
  } as unknown as ProviderSessionDirectoryShape;
  const adapter = input?.adapter ?? makeAdapter();
  const registry = {
    getInstanceInfo: () =>
      Effect.succeed({
        instanceId: INSTANCE_ID,
        driverKind: ProviderDriverKind.make(input?.driverKind ?? "claudeAgent"),
        displayName: "Claude Work",
        enabled: true,
        continuationIdentity: {
          driverKind: ProviderDriverKind.make(input?.driverKind ?? "claudeAgent"),
          continuationKey: input?.continuationKey ?? CLAUDE_SESSION_STORE_CONTINUATION_KEY,
        },
      }),
    getByInstance: () => Effect.succeed(adapter),
  } as unknown as ProviderAdapterRegistryShape;

  return Effect.gen(function* () {
    const operationLock = input?.operationLock ?? (yield* makeProviderThreadOperationLock);
    const rebuildBarrier = yield* makeProviderRegistryRebuildBarrier;
    return yield* makeProviderThreadContinuationSync.pipe(
      Effect.provideService(ProviderSessionDirectory, directory),
      Effect.provideService(ProviderAdapterRegistry, registry),
      Effect.provideService(ProviderThreadOperationLock, operationLock),
      Effect.provideService(ProviderRegistryRebuildBarrier, rebuildBarrier),
    );
  });
}

describe("ProviderThreadContinuationSync", () => {
  it.effect("serializes manual sync behind the shared thread operation lock", () =>
    Effect.gen(function* () {
      const operationLock = yield* makeProviderThreadOperationLock;
      const adapter = makeAdapter();
      const syncContinuation = vi.spyOn(adapter, "syncContinuation");
      const service = yield* makeService({ adapter, operationLock });
      const lockHeld = yield* Deferred.make<void>();
      const releaseLock = yield* Deferred.make<void>();
      const holder = yield* operationLock
        .withLock(
          THREAD_ID,
          Deferred.succeed(lockHeld, undefined).pipe(Effect.andThen(Deferred.await(releaseLock))),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(lockHeld);

      const sync = yield* service.sync({ threadId: THREAD_ID }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      expect(syncContinuation).not.toHaveBeenCalled();

      yield* Deferred.succeed(releaseLock, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(sync);
      expect(syncContinuation).toHaveBeenCalledOnce();
    }),
  );

  it.effect("syncs the currently bound provider instance", () =>
    Effect.gen(function* () {
      const adapter = makeAdapter({ state: "imported" });
      const syncContinuation = vi.spyOn(adapter, "syncContinuation");
      const service = yield* makeService({
        adapter,
        binding: {
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: INSTANCE_ID,
          resumeCursor: RESUME_CURSOR,
          runtimePayload: { cwd: "/workspace/project" },
        },
      });
      expect(yield* service.sync({ threadId: THREAD_ID })).toEqual({
        threadId: THREAD_ID,
        providerInstanceId: INSTANCE_ID,
        state: "imported",
      });
      expect(syncContinuation).toHaveBeenCalledWith({
        threadId: THREAD_ID,
        resumeCursor: RESUME_CURSOR,
        cwd: "/workspace/project",
      });
    }),
  );

  it.effect("prefers the current live-session resume cursor", () =>
    Effect.gen(function* () {
      const liveResumeCursor = {
        resume: "session-2",
        resumeSessionAt: "assistant-newest",
      };
      const adapter = makeAdapter({
        sessionStatus: "ready",
        liveResumeCursor,
      });
      const syncContinuation = vi.spyOn(adapter, "syncContinuation");
      const service = yield* makeService({ adapter });

      yield* service.sync({ threadId: THREAD_ID });

      expect(syncContinuation).toHaveBeenCalledWith({
        threadId: THREAD_ID,
        resumeCursor: liveResumeCursor,
      });
    }),
  );

  it.effect("rejects threads without a provider binding", () =>
    Effect.gen(function* () {
      const service = yield* makeService({ binding: undefined });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("thread-not-bound");
    }),
  );

  it.effect("rejects non-Claude providers", () =>
    Effect.gen(function* () {
      const service = yield* makeService({ driverKind: "codex" });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("unsupported-provider");
    }),
  );

  it.effect("rejects Claude instances outside the shared continuation group", () =>
    Effect.gen(function* () {
      const service = yield* makeService({ continuationKey: "claude:config:work" });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("feature-disabled");
    }),
  );

  it.effect("rejects live running sessions", () =>
    Effect.gen(function* () {
      const service = yield* makeService({
        adapter: makeAdapter({ sessionStatus: "running" }),
      });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("turn-active");
    }),
  );

  it.effect("rejects bindings without persisted resume state", () =>
    Effect.gen(function* () {
      const service = yield* makeService({
        binding: {
          threadId: THREAD_ID,
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: INSTANCE_ID,
        },
      });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("resume-state-missing");
    }),
  );

  it.effect("preserves the adapter transcript-not-found failure code", () =>
    Effect.gen(function* () {
      const service = yield* makeService({
        adapter: makeAdapter({ capabilityError: "transcript-not-found" }),
      });
      const error = yield* Effect.flip(service.sync({ threadId: THREAD_ID }));
      expect(error.code).toBe("transcript-not-found");
      expect(error.detail).toBe("Native Claude transcript was not found.");
    }),
  );
});
