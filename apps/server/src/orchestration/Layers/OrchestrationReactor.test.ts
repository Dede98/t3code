import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { it as effectIt } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";

import { CheckpointReactor } from "../Services/CheckpointReactor.ts";
import { ProviderCommandReactor } from "../Services/ProviderCommandReactor.ts";
import { ProviderRuntimeIngestionService } from "../Services/ProviderRuntimeIngestion.ts";
import { ThreadDeletionReactor } from "../Services/ThreadDeletionReactor.ts";
import { OrchestrationReactor } from "../Services/OrchestrationReactor.ts";
import { makeOrchestrationReactor } from "./OrchestrationReactor.ts";
import * as AgentAwarenessRelay from "../../relay/AgentAwarenessRelay.ts";
import { AgentControlInitialPlanningConsumer } from "../../agentControl/initialPlanning/Services/AgentControlInitialPlanningConsumer.ts";
import { AgentControlImplementationTurnConsumer } from "../../agentControl/implementationTurn/Services/AgentControlImplementationTurnConsumer.ts";
import { AgentControlVerificationTurnConsumer } from "../../agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";
import type { AgentControlVerificationTurnConsumerActivation } from "../../agentControl/verificationTurn/Services/AgentControlVerificationTurnConsumer.ts";
import type {
  ProviderRuntimeEventPublication,
  ProviderRuntimeEventSourceActivation,
} from "../../provider/Services/ProviderService.ts";
import {
  makeReactorStartupActivation,
  makeReactorStartupAttempt,
} from "../../reactorStartupActivation.ts";

const makeNoopProviderSourceActivation: Effect.Effect<ProviderRuntimeEventSourceActivation> =
  Effect.gen(function* () {
    const token = {
      id: 0,
      runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
      verificationAcknowledgement: yield* Deferred.make<void, Error>(),
    };
    return {
      handoffAccepted: Effect.void,
      quiesce: Effect.succeed({ token, sourceExit: Exit.void }),
      abort: () => Effect.void,
      awaitAbort: Effect.never,
    };
  });

const noopRuntimeActivation = {
  drainProviderEvents: () => Effect.void,
};

const withNoopVerificationDrain = (activation: {
  readonly commit: Effect.Effect<void>;
  readonly drain: Effect.Effect<void>;
}): AgentControlVerificationTurnConsumerActivation => ({
  ...activation,
  drainProviderEvents: () => Effect.void,
});

const makeLifecycleTestLayer = (input?: {
  readonly subscribeRuntime?: Effect.Effect<void>;
  readonly subscribeVerification?: Effect.Effect<void>;
  readonly startProviderSources?: Effect.Effect<void, never, Scope.Scope>;
  readonly providerSourceActivation?: Effect.Effect<ProviderRuntimeEventSourceActivation>;
  readonly startRuntime?: Effect.Effect<void, never, Scope.Scope>;
  readonly runtimeActivation?: typeof noopRuntimeActivation;
  readonly prepareVerification?: (
    activation?: Effect.Effect<void>,
  ) => Effect.Effect<
    { readonly commit: Effect.Effect<void>; readonly drain: Effect.Effect<void> },
    never,
    Scope.Scope
  >;
  readonly startProviderCommand?: Effect.Effect<void, never, Scope.Scope>;
  readonly openBarrier?: Effect.Effect<void>;
  readonly verificationDrain?: AgentControlVerificationTurnConsumerActivation["drainProviderEvents"];
}) =>
  Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
    Layer.provideMerge(
      Layer.succeed(ProviderRuntimeIngestionService, {
        subscribeProviderEvents: (input?.subscribeRuntime ?? Effect.void).pipe(
          Effect.as(undefined as never),
        ),
        startProviderRuntimeEventSources: (input?.startProviderSources ?? Effect.void).pipe(
          Effect.andThen(input?.providerSourceActivation ?? makeNoopProviderSourceActivation),
        ),
        openProviderRuntimeEventPublishing: input?.openBarrier ?? Effect.void,
        start: () =>
          (input?.startRuntime ?? Effect.void).pipe(
            Effect.as(input?.runtimeActivation ?? noopRuntimeActivation),
          ),
        drain: Effect.void,
      }),
    ),
    Layer.provideMerge(
      Layer.succeed(AgentControlVerificationTurnConsumer, {
        processHandoff: () => Effect.void,
        processRuntimeEvent: () => Effect.void,
        recover: Effect.void,
        subscribeProviderEvents: (input?.subscribeVerification ?? Effect.void).pipe(
          Effect.as(undefined as never),
        ),
        prepare: (_events, activation) =>
          (
            input?.prepareVerification?.(activation) ??
            Effect.succeed({ commit: Effect.void, drain: Effect.void })
          ).pipe(
            Effect.map((prepared) => ({
              ...withNoopVerificationDrain(prepared),
              ...(input?.verificationDrain === undefined
                ? {}
                : { drainProviderEvents: input.verificationDrain }),
            })),
          ),
        start: () => Effect.void,
        drain: Effect.void,
      }),
    ),
    Layer.provideMerge(
      Layer.mergeAll(
        Layer.succeed(ProviderCommandReactor, {
          start: () => input?.startProviderCommand ?? Effect.void,
          drain: Effect.void,
        }),
        Layer.succeed(CheckpointReactor, { start: () => Effect.void, drain: Effect.void }),
        Layer.succeed(ThreadDeletionReactor, { start: () => Effect.void, drain: Effect.void }),
        Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
          publishThread: () => Effect.void,
          start: () => Effect.void,
        }),
        Layer.succeed(AgentControlInitialPlanningConsumer, {
          start: () => Effect.void,
          drain: Effect.void,
        }),
        Layer.succeed(AgentControlImplementationTurnConsumer, {
          processHandoff: () => Effect.void,
          processRuntimeEvent: () => Effect.void,
          recover: Effect.void,
          start: () => Effect.void,
          drain: Effect.void,
        }),
      ),
    ),
  );

describe("OrchestrationReactor", () => {
  effectIt.effect("starts each component once and closes the committed lifecycle", () => {
    const started: string[] = [];

    const layer = Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
      Layer.provideMerge(
        Layer.succeed(ProviderRuntimeIngestionService, {
          subscribeProviderEvents: Effect.sync(() => {
            started.push("provider-runtime-subscription");
            return undefined as never;
          }),
          openProviderRuntimeEventPublishing: Effect.sync(() => {
            started.push("provider-runtime-publishing-open");
          }),
          startProviderRuntimeEventSources: Effect.sync(() => {
            started.push("provider-runtime-event-sources");
          }).pipe(Effect.andThen(makeNoopProviderSourceActivation)),
          start: () => {
            started.push("provider-runtime-ingestion");
            return Effect.succeed(noopRuntimeActivation);
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(AgentControlVerificationTurnConsumer, {
          processHandoff: () => Effect.void,
          processRuntimeEvent: () => Effect.void,
          recover: Effect.void,
          subscribeProviderEvents: Effect.sync(() => {
            started.push("verification-runtime-subscription");
            return undefined as never;
          }),
          prepare: () => {
            started.push("verification-turn-consumer");
            return Effect.succeed(
              withNoopVerificationDrain({ commit: Effect.void, drain: Effect.void }),
            );
          },
          start: () => {
            started.push("verification-turn-consumer");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(ProviderCommandReactor, {
          start: () => {
            started.push("provider-command-reactor");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(CheckpointReactor, {
          start: () => {
            started.push("checkpoint-reactor");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(ThreadDeletionReactor, {
          start: () => {
            started.push("thread-deletion-reactor");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
          publishThread: () => Effect.void,
          start: () => {
            started.push("agent-awareness-relay");
            return Effect.void;
          },
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(AgentControlInitialPlanningConsumer, {
          start: () => {
            started.push("initial-planning-consumer");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
      Layer.provideMerge(
        Layer.succeed(AgentControlImplementationTurnConsumer, {
          processHandoff: () => Effect.void,
          processRuntimeEvent: () => Effect.void,
          recover: Effect.void,
          start: () => {
            started.push("implementation-turn-consumer");
            return Effect.void;
          },
          drain: Effect.void,
        }),
      ),
    );

    return Effect.gen(function* () {
      const reactor = yield* OrchestrationReactor;
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      yield* reactor.start().pipe(Scope.provide(scope));
      yield* reactor.start().pipe(Scope.provide(scope));
      yield* reactor.commit().pipe(Scope.provide(scope));
      yield* reactor.commit().pipe(Scope.provide(scope));

      expect(started.slice(0, 2).sort()).toEqual([
        "provider-runtime-subscription",
        "verification-runtime-subscription",
      ]);
      expect(started.slice(2)).toEqual([
        "provider-runtime-event-sources",
        "provider-runtime-ingestion",
        "verification-turn-consumer",
        "provider-command-reactor",
        "checkpoint-reactor",
        "thread-deletion-reactor",
        "agent-awareness-relay",
        "initial-planning-consumer",
        "implementation-turn-consumer",
        "provider-runtime-publishing-open",
      ]);
      expect(started.filter((entry) => entry === "provider-runtime-publishing-open")).toHaveLength(
        1,
      );

      yield* Scope.close(scope, Exit.void);
      const restartedScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(restartedScope, Exit.void));
      const restartError = yield* Effect.flip(reactor.start().pipe(Scope.provide(restartedScope)));
      expect(restartError.reason).toBe("lifecycle-closed");
    }).pipe(Effect.scoped, Effect.provide(layer));
  });

  effectIt.effect(
    "rolls back the first provider subscription when the second acquisition fails",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const firstSubscriptionAcquired = yield* Deferred.make<void>();
          const releasedSubscriptions = yield* Ref.make(0);
          const barrierOpenCalls = yield* Ref.make(0);
          const subscriptionDefect = new Error("verification-subscription-defect");
          const layer = Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
            Layer.provideMerge(
              Layer.succeed(ProviderRuntimeIngestionService, {
                subscribeProviderEvents: Effect.acquireRelease(
                  PubSub.subscribe(providerEvents).pipe(
                    Effect.tap(() => Deferred.succeed(firstSubscriptionAcquired, undefined)),
                  ),
                  () => Ref.update(releasedSubscriptions, (count) => count + 1),
                ),
                openProviderRuntimeEventPublishing: Ref.update(
                  barrierOpenCalls,
                  (count) => count + 1,
                ),
                startProviderRuntimeEventSources: makeNoopProviderSourceActivation,
                start: () => Effect.die("provider ingestion must not start"),
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.succeed(AgentControlVerificationTurnConsumer, {
                processHandoff: () => Effect.void,
                processRuntimeEvent: () => Effect.void,
                recover: Effect.void,
                subscribeProviderEvents: Deferred.await(firstSubscriptionAcquired).pipe(
                  Effect.andThen(Effect.die(subscriptionDefect)),
                ),
                prepare: () => Effect.die("verification consumer must not prepare"),
                start: () => Effect.die("verification consumer must not start"),
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.mergeAll(
                Layer.succeed(ProviderCommandReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(CheckpointReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(ThreadDeletionReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
                  publishThread: () => Effect.void,
                  start: () => Effect.void,
                }),
              ),
            ),
          );
          const context = yield* Layer.build(layer);
          const reactor = Context.get(context, OrchestrationReactor);
          const ownerScope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));

          const exit = yield* Effect.exit(reactor.start().pipe(Scope.provide(ownerScope)));

          expect(Exit.isFailure(exit)).toBe(true);
          expect(yield* Ref.get(releasedSubscriptions)).toBe(1);
          expect(yield* Ref.get(barrierOpenCalls)).toBe(0);
        }),
      ),
  );

  effectIt.effect(
    "interrupts partial consumer fibers, keeps the barrier closed, and retries without stale resources",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const providerEvents = yield* PubSub.unbounded<ProviderRuntimeEvent>();
          const activeSubscriptions = yield* Ref.make(0);
          const startedFibers = yield* Ref.make(0);
          const stoppedFibers = yield* Ref.make(0);
          const barrierOpenCalls = yield* Ref.make(0);
          const releasedRuntimeEvents = yield* Ref.make(0);
          const providerBarrier = yield* Deferred.make<void>();
          const startFailures = yield* Ref.make(1);
          const startupDefect = new Error("provider-command-startup-defect");
          const subscribe = Effect.acquireRelease(
            PubSub.subscribe(providerEvents).pipe(
              Effect.tap(() => Ref.update(activeSubscriptions, (count) => count + 1)),
            ),
            () => Ref.update(activeSubscriptions, (count) => count - 1),
          );
          const startConsumerFiber = Ref.update(startedFibers, (count) => count + 1).pipe(
            Effect.andThen(
              Effect.forkScoped(
                Effect.never.pipe(
                  Effect.onInterrupt(() => Ref.update(stoppedFibers, (count) => count + 1)),
                ),
                { startImmediately: true },
              ),
            ),
            Effect.asVoid,
          );
          yield* Deferred.await(providerBarrier).pipe(
            Effect.andThen(Ref.update(releasedRuntimeEvents, (count) => count + 1)),
            Effect.forkScoped,
          );
          const layer = Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
            Layer.provideMerge(
              Layer.succeed(ProviderRuntimeIngestionService, {
                subscribeProviderEvents: subscribe,
                openProviderRuntimeEventPublishing: Ref.update(
                  barrierOpenCalls,
                  (count) => count + 1,
                ).pipe(Effect.andThen(Deferred.succeed(providerBarrier, undefined)), Effect.asVoid),
                startProviderRuntimeEventSources: makeNoopProviderSourceActivation,
                start: () => startConsumerFiber.pipe(Effect.as(noopRuntimeActivation)),
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.succeed(AgentControlVerificationTurnConsumer, {
                processHandoff: () => Effect.void,
                processRuntimeEvent: () => Effect.void,
                recover: Effect.void,
                subscribeProviderEvents: subscribe,
                prepare: () =>
                  startConsumerFiber.pipe(
                    Effect.as(
                      withNoopVerificationDrain({ commit: Effect.void, drain: Effect.void }),
                    ),
                  ),
                start: () => startConsumerFiber,
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.mergeAll(
                Layer.succeed(ProviderCommandReactor, {
                  start: () =>
                    Ref.getAndUpdate(startFailures, (count) => Math.max(0, count - 1)).pipe(
                      Effect.flatMap((remaining) =>
                        remaining > 0 ? Effect.die(startupDefect) : Effect.void,
                      ),
                    ),
                  drain: Effect.void,
                }),
                Layer.succeed(CheckpointReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(ThreadDeletionReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
                  publishThread: () => Effect.void,
                  start: () => Effect.void,
                }),
              ),
            ),
          );
          const context = yield* Layer.build(layer);
          const reactor = Context.get(context, OrchestrationReactor);
          const failedOwner = yield* Scope.make("sequential");
          const failedExit = yield* Effect.exit(reactor.start().pipe(Scope.provide(failedOwner)));

          expect(Exit.isFailure(failedExit)).toBe(true);
          expect(yield* Ref.get(startedFibers)).toBe(2);
          expect(yield* Ref.get(stoppedFibers)).toBe(2);
          expect(yield* Ref.get(activeSubscriptions)).toBe(0);
          expect(yield* Ref.get(barrierOpenCalls)).toBe(0);
          expect(yield* Deferred.isDone(providerBarrier)).toBe(false);
          expect(yield* Ref.get(releasedRuntimeEvents)).toBe(0);
          yield* Scope.close(failedOwner, Exit.void);

          const retryOwner = yield* Scope.make("sequential");
          yield* reactor.start().pipe(Scope.provide(retryOwner));
          yield* reactor.commit().pipe(Scope.provide(retryOwner));
          yield* Effect.yieldNow;
          expect(yield* Ref.get(startedFibers)).toBe(4);
          expect(yield* Ref.get(stoppedFibers)).toBe(2);
          expect(yield* Ref.get(activeSubscriptions)).toBe(2);
          expect(yield* Ref.get(barrierOpenCalls)).toBe(1);
          expect(yield* Ref.get(releasedRuntimeEvents)).toBe(1);

          yield* Scope.close(retryOwner, Exit.void);
          expect(yield* Ref.get(stoppedFibers)).toBe(4);
          expect(yield* Ref.get(activeSubscriptions)).toBe(0);
        }),
      ),
  );

  effectIt.effect(
    "buffers one provider event for both consumers before either subscribed stream starts",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const events = yield* PubSub.unbounded<
            ProviderRuntimeEventPublication | ProviderRuntimeEvent
          >();
          const runtimeObserved = yield* Deferred.make<ProviderRuntimeEvent>();
          const verificationObserved = yield* Deferred.make<ProviderRuntimeEvent>();
          const event: ProviderRuntimeEvent = {
            type: "turn.started",
            eventId: EventId.make("startup-ready-turn-started"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: ProviderInstanceId.make("codex"),
            threadId: ThreadId.make("startup-ready-thread"),
            turnId: TurnId.make("startup-ready-turn"),
            createdAt: "2026-08-07T08:00:00.000Z",
            payload: {},
          };
          const layer = Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
            Layer.provideMerge(
              Layer.succeed(ProviderRuntimeIngestionService, {
                subscribeProviderEvents: PubSub.subscribe(events),
                openProviderRuntimeEventPublishing: Effect.void,
                startProviderRuntimeEventSources: makeNoopProviderSourceActivation,
                start: (subscription) =>
                  Effect.gen(function* () {
                    yield* Effect.forkScoped(
                      Stream.runForEach(Stream.fromSubscription(subscription!), (observed) =>
                        "_tag" in observed
                          ? Effect.die("unexpected lifecycle marker")
                          : Deferred.succeed(runtimeObserved, observed),
                      ),
                      { startImmediately: true },
                    );
                    yield* PubSub.publish(events, event);
                    return noopRuntimeActivation;
                  }),
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.succeed(AgentControlVerificationTurnConsumer, {
                processHandoff: () => Effect.void,
                processRuntimeEvent: () => Effect.void,
                recover: Effect.void,
                subscribeProviderEvents: PubSub.subscribe(events),
                prepare: (subscription) =>
                  Effect.as(
                    Effect.forkScoped(
                      Stream.runForEach(Stream.fromSubscription(subscription!), (observed) =>
                        "_tag" in observed
                          ? Effect.die("unexpected lifecycle marker")
                          : Deferred.succeed(verificationObserved, observed),
                      ),
                      { startImmediately: true },
                    ),
                    withNoopVerificationDrain({ commit: Effect.void, drain: Effect.void }),
                  ),
                start: (subscription) =>
                  Effect.asVoid(
                    Effect.forkScoped(
                      Stream.runForEach(Stream.fromSubscription(subscription!), (observed) =>
                        "_tag" in observed
                          ? Effect.die("unexpected lifecycle marker")
                          : Deferred.succeed(verificationObserved, observed),
                      ),
                      { startImmediately: true },
                    ),
                  ),
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.mergeAll(
                Layer.succeed(ProviderCommandReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(CheckpointReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(ThreadDeletionReactor, {
                  start: () => Effect.void,
                  drain: Effect.void,
                }),
                Layer.succeed(AgentAwarenessRelay.AgentAwarenessRelay, {
                  publishThread: () => Effect.void,
                  start: () => Effect.void,
                }),
              ),
            ),
          );
          const scope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
          const context = yield* Layer.buildWithScope(layer, scope);
          const reactor = Context.get(context, OrchestrationReactor);
          yield* reactor.start().pipe(Scope.provide(scope));
          yield* reactor.commit().pipe(Scope.provide(scope));

          expect(yield* Deferred.await(runtimeObserved)).toEqual(event);
          expect(yield* Deferred.await(verificationObserved)).toEqual(event);
        }),
      ),
  );

  effectIt.effect("serializes concurrent starts and commits one prepared lifecycle once", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commandEntered = yield* Deferred.make<void>();
        const releaseCommand = yield* Deferred.make<void>();
        const runtimeSubscriptions = yield* Ref.make(0);
        const verificationSubscriptions = yield* Ref.make(0);
        const providerSources = yield* Ref.make(0);
        const verificationPreparations = yield* Ref.make(0);
        const barrierCommits = yield* Ref.make(0);
        const context = yield* Layer.build(
          makeLifecycleTestLayer({
            subscribeRuntime: Ref.update(runtimeSubscriptions, (count) => count + 1),
            subscribeVerification: Ref.update(verificationSubscriptions, (count) => count + 1),
            startProviderSources: Ref.update(providerSources, (count) => count + 1),
            prepareVerification: () =>
              Ref.update(verificationPreparations, (count) => count + 1).pipe(
                Effect.as({
                  commit: Effect.void,
                  drain: Effect.void,
                }),
              ),
            startProviderCommand: Deferred.succeed(commandEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCommand)),
            ),
            openBarrier: Ref.update(barrierCommits, (count) => count + 1),
          }),
        );
        const reactor = Context.get(context, OrchestrationReactor);
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));

        const first = yield* reactor
          .start()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(commandEntered);
        const second = yield* reactor
          .start()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(releaseCommand, undefined);
        expect(Exit.isSuccess(yield* Fiber.await(first))).toBe(true);
        expect(Exit.isSuccess(yield* Fiber.await(second))).toBe(true);

        yield* Effect.all([reactor.commit(), reactor.commit()], {
          concurrency: "unbounded",
          discard: true,
        }).pipe(Scope.provide(ownerScope));
        yield* reactor.start().pipe(Scope.provide(ownerScope));

        expect(yield* Ref.get(runtimeSubscriptions)).toBe(1);
        expect(yield* Ref.get(verificationSubscriptions)).toBe(1);
        expect(yield* Ref.get(providerSources)).toBe(1);
        expect(yield* Ref.get(verificationPreparations)).toBe(1);
        expect(yield* Ref.get(barrierCommits)).toBe(1);
      }),
    ),
  );

  effectIt.effect(
    "holds the attempt scope through the atomic barrier and shared activation cutover",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const activation = yield* makeReactorStartupActivation;
          const activationObserved = yield* Deferred.make<void>();
          const barrierOpened = yield* Deferred.make<void>();
          const resourcesFinalized = yield* Ref.make(0);
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              startProviderSources: Effect.acquireRelease(Effect.void, () =>
                Ref.update(resourcesFinalized, (count) => count + 1),
              ),
              prepareVerification: (awaitActivation) =>
                Effect.gen(function* () {
                  yield* Effect.forkScoped(
                    awaitActivation!.pipe(
                      Effect.andThen(Deferred.succeed(activationObserved, undefined)),
                    ),
                    { startImmediately: true },
                  );
                  return { commit: Effect.void, drain: Effect.void };
                }),
              openBarrier: Effect.gen(function* () {
                expect(yield* Ref.get(resourcesFinalized)).toBe(0);
                yield* Deferred.succeed(barrierOpened, undefined);
              }),
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const ownerScope = yield* Scope.make("sequential");
          yield* reactor.start(activation).pipe(Scope.provide(ownerScope));

          const commitFiber = yield* reactor
            .commit()
            .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(barrierOpened);
          yield* Deferred.await(activationObserved);
          const closeFiber = yield* Scope.close(ownerScope, Exit.void).pipe(
            Effect.forkChild({ startImmediately: true }),
          );

          expect(Exit.isSuccess(yield* Fiber.await(commitFiber))).toBe(true);
          expect(Exit.isSuccess(yield* Fiber.await(closeFiber))).toBe(true);
          expect(yield* Ref.get(resourcesFinalized)).toBe(1);
          const afterClose = yield* Effect.exit(reactor.commit().pipe(Scope.provide(ownerScope)));
          expect(Exit.isFailure(afterClose)).toBe(true);
          if (Exit.isFailure(afterClose)) {
            expect(
              afterClose.cause.reasons.some(
                (reason) =>
                  Cause.isFailReason(reason) && reason.error.reason === "lifecycle-closed",
              ),
            ).toBe(true);
          }
        }),
      ),
  );

  effectIt.effect.each(["runtime", "verification"] as const)(
    "quiesces provider intake before draining a slower %s consumer and finalizing resources",
    (slower) =>
      Effect.scoped(
        Effect.gen(function* () {
          const order = yield* Ref.make<ReadonlyArray<string>>([]);
          const runtimeFinished = yield* Deferred.make<void>();
          const verificationFinished = yield* Deferred.make<void>();
          const token = {
            id: 91,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
          const runtimeDrain = () =>
            (slower === "runtime" ? Deferred.await(verificationFinished) : Effect.void).pipe(
              Effect.andThen(Ref.update(order, (entries) => [...entries, "runtime"])),
              Effect.andThen(Deferred.succeed(runtimeFinished, undefined)),
              Effect.asVoid,
            );
          const verificationDrain = () =>
            (slower === "verification" ? Deferred.await(runtimeFinished) : Effect.void).pipe(
              Effect.andThen(Ref.update(order, (entries) => [...entries, "verification"])),
              Effect.andThen(Deferred.succeed(verificationFinished, undefined)),
              Effect.asVoid,
            );
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              startProviderSources: Effect.acquireRelease(Effect.void, () =>
                Ref.update(order, (entries) => [...entries, "resources-finalized"]),
              ),
              providerSourceActivation: Effect.succeed({
                handoffAccepted: Effect.void,
                quiesce: Ref.update(order, (entries) => [...entries, "source-quiesced"]).pipe(
                  Effect.as({ token, sourceExit: Exit.void }),
                ),
                abort: () => Effect.void,
                awaitAbort: Effect.never,
              }),
              runtimeActivation: { drainProviderEvents: runtimeDrain },
              verificationDrain,
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const resourcesScope = yield* Scope.make("sequential");
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);
          yield* reactor.start(attempt.activation).pipe(Scope.provide(resourcesScope));
          yield* attempt.commit(reactor.commit().pipe(Scope.provide(resourcesScope)));
          yield* attempt.close(Exit.interrupt(`parent-${slower}-slow` as never));

          expect(yield* Ref.get(order)).toEqual(
            slower === "runtime"
              ? ["source-quiesced", "verification", "runtime", "resources-finalized"]
              : ["source-quiesced", "runtime", "verification", "resources-finalized"],
          );
        }),
      ),
  );

  effectIt.effect(
    "combines source interruption, consumer defect, and cleanup defect without skipping the peer drain",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const sourceInterrupt = Cause.interrupt("provider-source-interrupt" as never);
          const runtimeDefect = new Error("runtime-ingestion-drain-defect");
          const cleanupDefect = new Error("provider-lifecycle-cleanup-defect");
          const verificationDrained = yield* Ref.make(false);
          const token = {
            id: 92,
            runtimeIngestionAcknowledgement: yield* Deferred.make<void, Error>(),
            verificationAcknowledgement: yield* Deferred.make<void, Error>(),
          };
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              startProviderSources: Effect.acquireRelease(Effect.void, () =>
                Effect.die(cleanupDefect),
              ),
              providerSourceActivation: Effect.succeed({
                handoffAccepted: Effect.void,
                quiesce: Effect.succeed({
                  token,
                  sourceExit: Exit.failCause(sourceInterrupt),
                }),
                abort: () => Effect.void,
                awaitAbort: Effect.never,
              }),
              runtimeActivation: { drainProviderEvents: () => Effect.die(runtimeDefect) },
              verificationDrain: () => Ref.set(verificationDrained, true),
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const resourcesScope = yield* Scope.make("sequential");
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);
          yield* reactor.start(attempt.activation).pipe(Scope.provide(resourcesScope));
          yield* attempt.commit(reactor.commit().pipe(Scope.provide(resourcesScope)));
          const closeExit = yield* Effect.exit(
            attempt.close(Exit.interrupt("parent-during-provider-drain" as never)),
          );

          expect(Exit.isFailure(closeExit)).toBe(true);
          expect(yield* Ref.get(verificationDrained)).toBe(true);
          if (Exit.isFailure(closeExit)) {
            expect(Cause.hasInterrupts(closeExit.cause)).toBe(true);
            expect(
              closeExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === runtimeDefect,
              ),
            ).toBe(true);
            expect(
              closeExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === cleanupDefect,
              ),
            ).toBe(true);
          }
        }),
      ),
  );

  effectIt.effect(
    "terminal-aborts a failed post-barrier handoff without opening activation or waiting for markers",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const handoffDefect = new Error("post-barrier-handoff-defect");
          const abortCleanupDefect = new Error("provider-abort-cleanup-defect");
          const scopeCleanupDefect = new Error("provider-scope-cleanup-defect");
          const abortSignal = yield* Deferred.make<never>();
          const abortObserved = yield* Deferred.make<Cause.Cause<unknown>>();
          const quiesceCalls = yield* Ref.make(0);
          const recoveryStarts = yield* Ref.make(0);
          const resourcesFinalized = yield* Ref.make(false);
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              providerSourceActivation: Effect.succeed({
                handoffAccepted: Effect.die(handoffDefect),
                quiesce: Ref.update(quiesceCalls, (count) => count + 1).pipe(
                  Effect.andThen(Effect.never),
                ),
                abort: (cause) =>
                  Deferred.succeed(abortObserved, cause).pipe(
                    Effect.andThen(Deferred.failCause(abortSignal, cause as Cause.Cause<never>)),
                    Effect.andThen(Effect.die(abortCleanupDefect)),
                  ),
                awaitAbort: Deferred.await(abortSignal),
              }),
              prepareVerification: (activation) =>
                Effect.gen(function* () {
                  yield* Effect.forkScoped(
                    activation!.pipe(
                      Effect.andThen(Ref.update(recoveryStarts, (count) => count + 1)),
                    ),
                  );
                  return { commit: Effect.void, drain: Effect.void };
                }),
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const resourcesScope = yield* Scope.make("sequential");
          yield* Scope.addFinalizer(
            resourcesScope,
            Ref.set(resourcesFinalized, true).pipe(Effect.andThen(Effect.die(scopeCleanupDefect))),
          );
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);
          yield* reactor.start(attempt.activation).pipe(Scope.provide(resourcesScope));

          const commitExit = yield* Effect.exit(
            attempt.commit(reactor.commit().pipe(Scope.provide(resourcesScope))),
          );
          expect(Exit.isFailure(commitExit)).toBe(true);
          expect(yield* attempt.activation.closeDisposition).toBe("terminal");
          const abortCause = yield* Deferred.await(abortObserved);
          expect(
            abortCause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === handoffDefect,
            ),
          ).toBe(true);
          if (Exit.isFailure(commitExit)) {
            expect(
              commitExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === handoffDefect,
              ),
            ).toBe(true);
            expect(
              commitExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === abortCleanupDefect,
              ),
            ).toBe(true);
          }

          const closeFibers = yield* Effect.forEach(["first", "second", "third"], (name) =>
            attempt
              .close(Exit.interrupt(`post-barrier-close-${name}` as never))
              .pipe(Effect.exit, Effect.forkChild({ startImmediately: true })),
          );
          for (const closeFiber of closeFibers) {
            const fiberExit = yield* Fiber.await(closeFiber);
            expect(Exit.isSuccess(fiberExit)).toBe(true);
            if (Exit.isSuccess(fiberExit)) {
              const closeExit = fiberExit.value;
              expect(Exit.isFailure(closeExit)).toBe(true);
              if (Exit.isFailure(closeExit)) {
                for (const defect of [handoffDefect, abortCleanupDefect, scopeCleanupDefect]) {
                  expect(
                    closeExit.cause.reasons.some(
                      (reason) => Cause.isDieReason(reason) && reason.defect === defect,
                    ),
                  ).toBe(true);
                }
              }
            }
          }
          expect(yield* Ref.get(quiesceCalls)).toBe(0);
          expect(yield* Ref.get(recoveryStarts)).toBe(0);
          expect(yield* Ref.get(resourcesFinalized)).toBe(true);
        }),
      ),
  );

  effectIt.effect("lets shutdown win before cutover without opening either gate", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const activation = yield* makeReactorStartupActivation;
        const activated = yield* Ref.make(0);
        const barrierOpens = yield* Ref.make(0);
        const finalized = yield* Ref.make(0);
        const context = yield* Layer.build(
          makeLifecycleTestLayer({
            startProviderSources: Effect.acquireRelease(Effect.void, () =>
              Ref.update(finalized, (count) => count + 1),
            ),
            prepareVerification: (awaitActivation) =>
              Effect.gen(function* () {
                yield* Effect.forkScoped(
                  awaitActivation!.pipe(
                    Effect.andThen(Ref.update(activated, (count) => count + 1)),
                  ),
                  { startImmediately: true },
                );
                return { commit: Effect.void, drain: Effect.void };
              }),
            openBarrier: Ref.update(barrierOpens, (count) => count + 1),
          }),
        );
        const reactor = Context.get(context, OrchestrationReactor);
        const ownerScope = yield* Scope.make("sequential");
        yield* reactor.start(activation).pipe(Scope.provide(ownerScope));
        yield* Scope.close(ownerScope, Exit.interrupt("shutdown-before-cutover" as never));

        const commitExit = yield* Effect.exit(reactor.commit().pipe(Scope.provide(ownerScope)));
        expect(Exit.isFailure(commitExit)).toBe(true);
        expect(yield* Ref.get(finalized)).toBe(1);
        expect(yield* Ref.get(barrierOpens)).toBe(0);
        expect(yield* Ref.get(activated)).toBe(0);
      }),
    ),
  );

  effectIt.effect(
    "finishes the cutover after an interrupt exactly at the provider-open boundary",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const activation = yield* makeReactorStartupActivation;
          const openingReached = yield* Deferred.make<void>();
          const releaseOpening = yield* Deferred.make<void>();
          const activated = yield* Ref.make(0);
          const barrierOpens = yield* Ref.make(0);
          const finalized = yield* Ref.make(0);
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              startProviderSources: Effect.acquireRelease(Effect.void, () =>
                Ref.update(finalized, (count) => count + 1),
              ),
              prepareVerification: (awaitActivation) =>
                Effect.gen(function* () {
                  yield* Effect.forkScoped(
                    awaitActivation!.pipe(
                      Effect.andThen(Ref.update(activated, (count) => count + 1)),
                    ),
                    { startImmediately: true },
                  );
                  return { commit: Effect.void, drain: Effect.void };
                }),
              openBarrier: Deferred.succeed(openingReached, undefined).pipe(
                Effect.andThen(Deferred.await(releaseOpening)),
                Effect.andThen(Ref.update(barrierOpens, (count) => count + 1)),
              ),
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const ownerScope = yield* Scope.make("sequential");
          yield* reactor.start(activation).pipe(Scope.provide(ownerScope));
          const commitFiber = yield* reactor
            .commit()
            .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
          yield* Deferred.await(openingReached);
          const interrupter = yield* Fiber.interrupt(commitFiber).pipe(
            Effect.forkChild({ startImmediately: true }),
          );
          yield* Effect.yieldNow;
          expect(yield* Ref.get(finalized)).toBe(0);
          yield* Deferred.succeed(releaseOpening, undefined);
          expect(Exit.hasInterrupts(yield* Fiber.await(commitFiber))).toBe(true);
          yield* Fiber.join(interrupter);
          yield* Effect.yieldNow;
          expect(yield* Ref.get(barrierOpens)).toBe(1);
          expect(yield* Ref.get(activated)).toBe(1);
          expect(yield* Ref.get(finalized)).toBe(0);
          yield* reactor.commit().pipe(Scope.provide(ownerScope));
          yield* Scope.close(ownerScope, Exit.void);
          expect(yield* Ref.get(finalized)).toBe(1);
        }),
      ),
  );

  effectIt.effect("shares a concurrent startup failure and combines it with rollback failure", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commandEntered = yield* Deferred.make<void>();
        const releaseCommand = yield* Deferred.make<void>();
        const startupDefect = new Error("orchestration-concurrent-startup-defect");
        const rollbackDefect = new Error("orchestration-concurrent-rollback-defect");
        const context = yield* Layer.build(
          makeLifecycleTestLayer({
            startProviderSources: Effect.acquireRelease(Effect.void, () =>
              Effect.die(rollbackDefect),
            ),
            startProviderCommand: Deferred.succeed(commandEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCommand)),
              Effect.andThen(Effect.die(startupDefect)),
            ),
          }),
        );
        const reactor = Context.get(context, OrchestrationReactor);
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));

        const first = yield* reactor
          .start()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(commandEntered);
        const second = yield* reactor
          .start()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(releaseCommand, undefined);

        for (const fiber of [first, second]) {
          const exit = yield* Fiber.await(fiber);
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            expect(
              exit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === startupDefect,
              ),
            ).toBe(true);
            expect(
              exit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === rollbackDefect,
              ),
            ).toBe(true);
          }
        }
      }),
    ),
  );

  effectIt.effect("shares one terminal pre-open commit failure with concurrent callers", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const openingReached = yield* Deferred.make<void>();
        const releaseFailure = yield* Deferred.make<void>();
        const commitDefect = new Error("provider-barrier-open-defect");
        const context = yield* Layer.build(
          makeLifecycleTestLayer({
            openBarrier: Deferred.succeed(openingReached, undefined).pipe(
              Effect.andThen(Deferred.await(releaseFailure)),
              Effect.andThen(Effect.die(commitDefect)),
            ),
          }),
        );
        const reactor = Context.get(context, OrchestrationReactor);
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        yield* reactor.start().pipe(Scope.provide(ownerScope));

        const first = yield* reactor
          .commit()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(openingReached);
        const second = yield* reactor
          .commit()
          .pipe(Scope.provide(ownerScope), Effect.forkChild({ startImmediately: true }));
        yield* Deferred.succeed(releaseFailure, undefined);

        for (const fiber of [first, second]) {
          const exit = yield* Fiber.await(fiber);
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            expect(
              exit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === commitDefect,
              ),
            ).toBe(true);
          }
        }
      }),
    ),
  );

  effectIt.effect("keeps a shared post-cutover barrier defect terminal after rollback", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const commitDefect = new Error("shared-provider-barrier-open-defect");
        const finalized = yield* Ref.make(0);
        const context = yield* Layer.build(
          makeLifecycleTestLayer({
            startProviderSources: Effect.acquireRelease(Effect.void, () =>
              Ref.update(finalized, (count) => count + 1),
            ),
            openBarrier: Effect.die(commitDefect),
          }),
        );
        const reactor = Context.get(context, OrchestrationReactor);
        const resourcesScope = yield* Scope.make("sequential");
        const attempt = yield* makeReactorStartupAttempt(resourcesScope);
        yield* reactor.start(attempt.activation).pipe(Scope.provide(resourcesScope));
        const commitExit = yield* Effect.exit(
          attempt.commit(reactor.commit().pipe(Scope.provide(resourcesScope))),
        );
        expect(Exit.isFailure(commitExit)).toBe(true);
        if (Exit.isFailure(commitExit)) {
          expect(
            commitExit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === commitDefect,
            ),
          ).toBe(true);
        }
        const closeExit = yield* Effect.exit(attempt.close(commitExit));
        expect(Exit.isFailure(closeExit)).toBe(true);
        if (Exit.isFailure(closeExit)) {
          expect(
            closeExit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === commitDefect,
            ),
          ).toBe(true);
        }
        expect(yield* Ref.get(finalized)).toBe(1);

        const retryScope = yield* Scope.make("sequential");
        const retry = yield* Effect.exit(reactor.start().pipe(Scope.provide(retryScope)));
        expect(Exit.isFailure(retry)).toBe(true);
        if (Exit.isFailure(retry)) {
          expect(
            retry.cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error.reason === "lifecycle-closed",
            ),
          ).toBe(true);
        }
        yield* Scope.close(retryScope, Exit.void);
      }),
    ),
  );

  effectIt.effect("combines typed and interrupted startup causes with rollback defects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const typedStartupCause = Cause.fail("typed-orchestration-startup") as Cause.Cause<never>;
        const cases = [
          {
            name: "typed",
            startup: Effect.failCause(typedStartupCause),
            assertOriginal: (cause: Cause.Cause<unknown>) =>
              cause.reasons.some(
                (reason) =>
                  Cause.isFailReason(reason) && reason.error === "typed-orchestration-startup",
              ),
          },
          {
            name: "interrupt",
            startup: Effect.interrupt,
            assertOriginal: (cause: Cause.Cause<unknown>) => Cause.hasInterrupts(cause),
          },
        ] as const;

        for (const testCase of cases) {
          const rollbackDefect = new Error(`${testCase.name}-orchestration-rollback-defect`);
          const context = yield* Layer.build(
            makeLifecycleTestLayer({
              startProviderSources: Effect.acquireRelease(Effect.void, () =>
                Effect.die(rollbackDefect),
              ),
              startProviderCommand: testCase.startup,
            }),
          );
          const reactor = Context.get(context, OrchestrationReactor);
          const ownerScope = yield* Scope.make("sequential");
          const exit = yield* Effect.exit(reactor.start().pipe(Scope.provide(ownerScope)));
          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isFailure(exit)) {
            expect(testCase.assertOriginal(exit.cause)).toBe(true);
            expect(
              exit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === rollbackDefect,
              ),
            ).toBe(true);
          }
          yield* Scope.close(ownerScope, Exit.void);
        }
      }),
    ),
  );
});
