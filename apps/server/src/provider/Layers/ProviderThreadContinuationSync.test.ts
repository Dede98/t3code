import {
  ProviderInstanceId,
  ProviderThreadId,
  ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ProjectionStoreV2 } from "../../orchestration-v2/ProjectionStore.ts";
import { ProviderInstanceRegistry } from "../Services/ProviderInstanceRegistry.ts";
import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import { makeProviderRegistryRebuildBarrier } from "./ProviderRegistryRebuildBarrier.ts";
import { makeProviderThreadContinuationSync } from "./ProviderThreadContinuationSync.ts";

const threadId = ThreadId.make("thread:sync");
const source = ProviderInstanceId.make("claude-source");
const target = ProviderInstanceId.make("claude-target");
const nativeThread = ProviderThreadId.make("provider-thread:source");

function fixture(
  options: {
    driver?: string;
    status?: string;
    missing?: boolean;
    nativeMissing?: boolean;
    disabled?: boolean;
  } = {},
) {
  const calls: unknown[] = [];
  const projection = {
    thread: { activeProviderThreadId: nativeThread, providerInstanceId: target },
    providerThreads: options.missing
      ? []
      : [
          {
            id: nativeThread,
            providerInstanceId: source,
            driver: options.driver ?? "claudeAgent",
            status: options.status ?? "idle",
            providerSessionId: "session:source",
            nativeThreadRef: options.nativeMissing ? null : { nativeId: "native:source" },
            nativeConversationHeadRef: { nativeId: "assistant:latest" },
          },
        ],
    providerSessions: [{ id: "session:source", cwd: "/project/source" }],
    runs: [],
  } as unknown as OrchestrationV2ThreadProjection;
  return Effect.gen(function* () {
    const barrier = yield* makeProviderRegistryRebuildBarrier;
    const service = yield* makeProviderThreadContinuationSync.pipe(
      Effect.provideService(ProjectionStoreV2, {
        getThreadProjection: () => Effect.succeed(projection),
      } as unknown as ProjectionStoreV2["Service"]),
      Effect.provideService(ProviderInstanceRegistry, {
        getInstance: (id: ProviderInstanceId) => {
          calls.push(id);
          return Effect.succeed({
            orchestrationAdapter: options.disabled
              ? {}
              : {
                  syncContinuation: (input: unknown) => {
                    calls.push(input);
                    return Effect.succeed("imported" as const);
                  },
                },
          });
        },
      } as unknown as ProviderInstanceRegistry["Service"]),
      Effect.provideService(ProviderRegistryRebuildBarrier, barrier),
    );
    return { service, calls };
  });
}

describe("V2 native continuation sync", () => {
  it.effect("uses the active native source even when the next turn selects another account", () =>
    Effect.gen(function* () {
      const { service, calls } = yield* fixture();
      expect(yield* service.sync({ threadId })).toEqual({
        threadId,
        providerInstanceId: source,
        state: "imported",
      });
      expect(calls).toEqual([
        source,
        {
          nativeThreadId: "native:source",
          cwd: "/project/source",
          expectedAssistantUuid: "assistant:latest",
        },
      ]);
    }),
  );
  it.effect.each(
    (
      [
        [{ missing: true }, "thread-not-bound"],
        [{ driver: "codex" }, "unsupported-provider"],
        [{ status: "active" }, "turn-active"],
        [{ nativeMissing: true }, "resume-state-missing"],
        [{ disabled: true }, "feature-disabled"],
      ] as const
    ).map(([options, code]) => ({ options, code })),
  )("rejects $code", ({ options, code }) =>
    Effect.gen(function* () {
      const { service } = yield* fixture(options);
      expect((yield* Effect.flip(service.sync({ threadId }))).code).toBe(code);
    }),
  );
});
