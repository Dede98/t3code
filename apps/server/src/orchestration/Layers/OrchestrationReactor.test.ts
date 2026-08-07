import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { it as effectIt } from "@effect/vitest";
import { afterEach, describe, expect, it } from "vite-plus/test";

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

describe("OrchestrationReactor", () => {
  let runtime: ManagedRuntime.ManagedRuntime<OrchestrationReactor, never> | null = null;

  afterEach(async () => {
    if (runtime) {
      await runtime.dispose();
    }
    runtime = null;
  });

  it("starts provider ingestion, provider command, checkpoint, and thread deletion reactors", async () => {
    const started: string[] = [];

    runtime = ManagedRuntime.make(
      Layer.effect(OrchestrationReactor, makeOrchestrationReactor).pipe(
        Layer.provideMerge(
          Layer.succeed(ProviderRuntimeIngestionService, {
            subscribeProviderEvents: Effect.sync(() => {
              started.push("provider-runtime-subscription");
              return undefined as never;
            }),
            openProviderRuntimeEventPublishing: Effect.sync(() => {
              started.push("provider-runtime-publishing-open");
            }),
            start: () => {
              started.push("provider-runtime-ingestion");
              return Effect.void;
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
      ),
    );

    const reactor = await runtime!.runPromise(Effect.service(OrchestrationReactor));
    const scope = await Effect.runPromise(Scope.make("sequential"));
    await Effect.runPromise(reactor.start().pipe(Scope.provide(scope)));

    expect(started.slice(0, 2).sort()).toEqual([
      "provider-runtime-subscription",
      "verification-runtime-subscription",
    ]);
    expect(started.slice(2)).toEqual([
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
    expect(started.filter((entry) => entry === "provider-runtime-publishing-open")).toHaveLength(1);

    await Effect.runPromise(Scope.close(scope, Exit.void));
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
                start: () => startConsumerFiber,
                drain: Effect.void,
              }),
            ),
            Layer.provideMerge(
              Layer.succeed(AgentControlVerificationTurnConsumer, {
                processHandoff: () => Effect.void,
                processRuntimeEvent: () => Effect.void,
                recover: Effect.void,
                subscribeProviderEvents: subscribe,
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
          const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
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
                start: (subscription) =>
                  Effect.gen(function* () {
                    yield* Effect.forkScoped(
                      Stream.runForEach(Stream.fromSubscription(subscription!), (observed) =>
                        Deferred.succeed(runtimeObserved, observed),
                      ),
                      { startImmediately: true },
                    );
                    yield* PubSub.publish(events, event);
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
                start: (subscription) =>
                  Effect.asVoid(
                    Effect.forkScoped(
                      Stream.runForEach(Stream.fromSubscription(subscription!), (observed) =>
                        Deferred.succeed(verificationObserved, observed),
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

          expect(yield* Deferred.await(runtimeObserved)).toEqual(event);
          expect(yield* Deferred.await(verificationObserved)).toEqual(event);
        }),
      ),
  );
});
