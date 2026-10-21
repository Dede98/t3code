import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import { describe, expect } from "vite-plus/test";

import { ProviderInstanceId } from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";

import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import { makeProviderRegistryRebuildBarrier } from "../provider/ProviderRegistryRebuildBarrier.ts";
import * as ProviderRegistryRebuildBarrier from "../provider/ProviderRegistryRebuildBarrier.ts";
import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as TextGeneration from "./TextGeneration.ts";
import * as SourceControlProviderRegistry from "../sourceControl/SourceControlProviderRegistry.ts";

const makeStubTextGeneration = (
  overrides: Partial<TextGeneration.TextGeneration["Service"]>,
): TextGeneration.TextGeneration["Service"] =>
  TextGeneration.TextGeneration.of({
    generateCommitMessage: () =>
      Effect.die("generateCommitMessage stub not configured for this test"),
    generatePrContent: () => Effect.die("generatePrContent stub not configured for this test"),
    generateBranchName: () => Effect.die("generateBranchName stub not configured for this test"),
    generateThreadTitle: () => Effect.die("generateThreadTitle stub not configured for this test"),
    generateReviewGuide: () => Effect.die("generateReviewGuide stub not configured for this test"),
    ...overrides,
  });

const makeStubInstance = (
  instanceId: ProviderInstanceId,
  textGeneration: TextGeneration.TextGeneration["Service"],
): ProviderInstance =>
  ({
    instanceId,
    driverKind: instanceId as unknown as ProviderInstance["driverKind"],
    continuationIdentity: {
      driverKind: instanceId as unknown as ProviderInstance["driverKind"],
      continuationKey: `${instanceId}:test`,
    },
    displayName: undefined,
    enabled: true,
    snapshot: {} as ProviderInstance["snapshot"],
    orchestrationAdapter: {} as ProviderInstance["orchestrationAdapter"],
    textGeneration,
  }) satisfies ProviderInstance;

const makeStubRegistry = (
  instances: ReadonlyArray<ProviderInstance>,
): ProviderInstanceRegistry.ProviderInstanceRegistry["Service"] => {
  const byId = new Map(instances.map((instance) => [instance.instanceId, instance] as const));
  return {
    getInstance: (id) => Effect.succeed(byId.get(id)),
    listInstances: Effect.succeed(instances),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
      PubSub.subscribe(pubsub),
    ),
  };
};

describe("TextGeneration.make", () => {
  it.effect.each(["generateBranchName", "generateThreadTitle"] as const)(
    "keeps a registry rebuild behind an in-flight %s",
    (operation) =>
      Effect.gen(function* () {
        const instanceId = ProviderInstanceId.make("codex_guarded");
        const rebuildBarrier = yield* makeProviderRegistryRebuildBarrier;
        const generationStarted = yield* Deferred.make<void>();
        const releaseGeneration = yield* Deferred.make<void>();
        const rebuildStarted = yield* Deferred.make<void>();
        const instance = makeStubInstance(
          instanceId,
          makeStubTextGeneration({
            [operation]: () =>
              Deferred.succeed(generationStarted, undefined).pipe(
                Effect.andThen(Deferred.await(releaseGeneration)),
                Effect.as({ branch: "guarded-branch", title: "Guarded title" }),
              ),
          }),
        );
        const tg = yield* TextGeneration.make.pipe(
          Effect.provideService(
            ProviderRegistryRebuildBarrier.ProviderRegistryRebuildBarrier,
            rebuildBarrier,
          ),
          Effect.provideService(
            ProviderInstanceRegistry.ProviderInstanceRegistry,
            makeStubRegistry([instance]),
          ),
          Effect.provide(
            Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
              resolveLink: () => Effect.die("No link lookup expected"),
            }),
          ),
        );

        const generation = yield* tg[operation]({
          cwd: process.cwd(),
          message: "guard the generation",
          modelSelection: createModelSelection(instanceId, "gpt-5"),
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(generationStarted);
        const rebuild = yield* rebuildBarrier
          .withRebuild(Deferred.succeed(rebuildStarted, undefined))
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        expect(Option.isNone(yield* Deferred.poll(rebuildStarted))).toBe(true);

        yield* Deferred.succeed(releaseGeneration, undefined);
        expect(yield* Fiber.join(generation)).toMatchObject({
          branch: "guarded-branch",
          title: "Guarded title",
        });
        yield* Fiber.join(rebuild);
        expect(Option.isSome(yield* Deferred.poll(rebuildStarted))).toBe(true);
      }),
  );
});
