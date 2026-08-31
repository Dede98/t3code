import * as NodeServices from "@effect/platform-node/NodeServices";
import { DEFAULT_MODEL, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Ref from "effect/Ref";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";

import * as ServerConfig from "./config.ts";
import * as OrchestrationEngine from "./orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import * as AnalyticsService from "./telemetry/AnalyticsService.ts";
import * as ServerRuntimeStartup from "./serverRuntimeStartup.ts";
import { AgentControlGithubObserveStartupError } from "./agentControl/github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeStartupError } from "./agentControl/task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlGithubObserveReactor } from "./agentControl/github/Services/AgentControlGithubObserveReactor.ts";
import { AgentControlTaskIntakeReactor } from "./agentControl/task/Services/AgentControlTaskIntakeReactor.ts";
import { AgentControlTaskVerificationFinalizer } from "./agentControl/task/Services/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlRunOnceController } from "./agentControl/runOnce/Services/AgentControlRunOnceController.ts";
import { AgentControlRunOnceError } from "./agentControl/runOnce/model.ts";
import { AgentControlReactor } from "./agentControl/Services/AgentControlReactor.ts";
import { layer as AgentControlReactorLive } from "./agentControl/Layers/AgentControlReactor.ts";
import { AgentControlVerificationAdmission } from "./agentControl/verificationAdmission/Services/AgentControlVerificationAdmission.ts";
import { AgentControlVerificationStageStarter } from "./agentControl/verificationTurn/Services/AgentControlVerificationStageStarter.ts";
import { AgentControlVerificationEvaluator } from "./agentControl/verificationTurn/Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationStageFinalizer } from "./agentControl/verificationTurn/Services/AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationTurnCoordinator } from "./agentControl/verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import type { ReactorStartupActivation } from "./reactorStartupActivation.ts";

it("uses the canonical Codex default for auto-bootstrapped model selection", () => {
  assert.deepStrictEqual(ServerRuntimeStartup.getAutoBootstrapDefaultModelSelection(), {
    instanceId: ProviderInstanceId.make("codex"),
    model: DEFAULT_MODEL,
  });
});

it.effect("enqueueCommand waits for readiness and then drains queued work", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const executionCount = yield* Ref.make(0);
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Ref.updateAndGet(executionCount, (count) => count + 1))
        .pipe(Effect.forkScoped);

      yield* Effect.yieldNow;
      assert.equal(yield* Ref.get(executionCount), 0);

      yield* commandGate.signalCommandReady;

      const result = yield* Fiber.join(queuedCommandFiber);
      assert.equal(result, 1);
      assert.equal(yield* Ref.get(executionCount), 1);
    }),
  ),
);

it.effect("enqueueCommand fails queued work when readiness fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
      const failure = yield* Deferred.make<void, never>();

      const queuedCommandFiber = yield* commandGate
        .enqueueCommand(Deferred.await(failure).pipe(Effect.as("should-not-run")))
        .pipe(Effect.forkScoped);

      yield* commandGate.failCommandReady(
        new ServerRuntimeStartup.ServerRuntimeStartupError({
          mode: "web",
          host: "127.0.0.1",
          port: 3773,
          cause: new Error("test startup failure"),
        }),
      );

      const error = yield* Effect.flip(Fiber.join(queuedCommandFiber));
      assert.equal(error.message, "Server runtime startup failed before command readiness.");
    }),
  ),
);

it.effect("does not open command readiness when Agent Control reactor startup fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
      const executed = yield* Ref.make(false);
      const queued = yield* commandGate
        .enqueueCommand(Ref.set(executed, true))
        .pipe(Effect.forkScoped);

      const opened = yield* ServerRuntimeStartup.openCommandReadinessAfterStartup(
        Effect.fail(
          new AgentControlGithubObserveStartupError({
            reason: "enumeration-failed",
          }),
        ),
        commandGate,
        { mode: "web", host: "127.0.0.1", port: 3773 },
      );

      assert.isFalse(opened);
      const error = yield* Effect.flip(Fiber.join(queued));
      assert.equal(error._tag, "ServerRuntimeStartupError");
      assert.isFalse(yield* Ref.get(executed));
    }),
  ),
);

it.effect(
  "rolls back partial server reactors before failing readiness and permits a clean retry",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
        const activeFibers = yield* Ref.make(0);
        const stoppedFibers = yield* Ref.make(0);
        const agentControlFailures = yield* Ref.make(1);
        const reaperStarts = yield* Ref.make(0);
        const queued = yield* commandGate.enqueueCommand(Effect.void).pipe(Effect.forkScoped);
        const orchestrationReactor = {
          start: () =>
            Ref.update(activeFibers, (count) => count + 1).pipe(
              Effect.andThen(
                Effect.forkScoped(
                  Effect.never.pipe(
                    Effect.onInterrupt(() =>
                      Ref.update(activeFibers, (count) => count - 1).pipe(
                        Effect.andThen(Ref.update(stoppedFibers, (count) => count + 1)),
                      ),
                    ),
                  ),
                  { startImmediately: true },
                ),
              ),
              Effect.asVoid,
            ),
          commit: () => Effect.void,
        };
        const agentControlReactor = {
          start: () =>
            Ref.getAndUpdate(agentControlFailures, (count) => Math.max(0, count - 1)).pipe(
              Effect.flatMap((remaining) =>
                remaining > 0
                  ? Effect.fail(
                      new AgentControlGithubObserveStartupError({
                        reason: "enumeration-failed",
                      }),
                    )
                  : Effect.void,
              ),
            ),
        };
        const providerSessionReaper = {
          start: () => Ref.update(reaperStarts, (count) => count + 1),
        };

        const opened = yield* ServerRuntimeStartup.openCommandReadinessAfterStartup(
          ServerRuntimeStartup.startReactorsAtomically({
            ownerScope,
            orchestrationReactor,
            agentControlReactor,
            providerSessionReaper,
          }),
          commandGate,
          { mode: "web", host: "127.0.0.1", port: 3773 },
        );

        assert.isFalse(opened);
        assert.equal(yield* Ref.get(activeFibers), 0);
        assert.equal(yield* Ref.get(stoppedFibers), 1);
        assert.equal(yield* Ref.get(reaperStarts), 0);
        const readinessError = yield* Effect.flip(Fiber.join(queued));
        assert.equal(readinessError._tag, "ServerRuntimeStartupError");

        yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper,
        });
        assert.equal(yield* Ref.get(activeFibers), 1);
        assert.equal(yield* Ref.get(stoppedFibers), 1);
        assert.equal(yield* Ref.get(reaperStarts), 1);

        yield* Scope.close(ownerScope, Exit.void);
        assert.equal(yield* Ref.get(activeFibers), 0);
        assert.equal(yield* Ref.get(stoppedFibers), 2);
      }),
    ),
);

it.effect("does not open readiness before Task Intake subscriptions and barriers succeed", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
      const queued = yield* commandGate
        .enqueueCommand(Effect.succeed("should-not-run"))
        .pipe(Effect.forkScoped);

      const opened = yield* ServerRuntimeStartup.openCommandReadinessAfterStartup(
        Effect.fail(
          new AgentControlTaskIntakeStartupError({
            reason: "queue-barrier-failed",
          }),
        ),
        commandGate,
        { mode: "web", host: "127.0.0.1", port: 3773 },
      );

      assert.isFalse(opened);
      const error = yield* Effect.flip(Fiber.join(queued));
      assert.equal(error._tag, "ServerRuntimeStartupError");
    }),
  ),
);

it.effect(
  "keeps provider publication and recovery closed through Agent Control and Reaper defects",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ownerScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
        const providerEvents = yield* PubSub.unbounded<string>();
        const providerBarrier = yield* Deferred.make<void>();
        const activeSubscriptions = yield* Ref.make(0);
        const releasedSubscriptions = yield* Ref.make(0);
        const observedEvents = yield* Ref.make<ReadonlyArray<string>>([]);
        const recoveryCalls = yield* Ref.make(0);
        const parkedProviderReleases = yield* Ref.make(0);
        const parkedProviderInterrupts = yield* Ref.make(0);
        const barrierOpens = yield* Ref.make(0);
        const agentControlFailures = yield* Ref.make(1);
        const reaperFailures = yield* Ref.make(1);
        let currentRecoveryGate: Deferred.Deferred<void> | undefined;

        const orchestrationReactor = {
          start: () =>
            Effect.gen(function* () {
              const subscribe = Effect.acquireRelease(
                PubSub.subscribe(providerEvents).pipe(
                  Effect.tap(() => Ref.update(activeSubscriptions, (count) => count + 1)),
                ),
                () =>
                  Ref.update(activeSubscriptions, (count) => count - 1).pipe(
                    Effect.andThen(Ref.update(releasedSubscriptions, (count) => count + 1)),
                  ),
              );
              const runtimeSubscription = yield* subscribe;
              const verificationSubscription = yield* subscribe;
              for (const subscription of [runtimeSubscription, verificationSubscription]) {
                yield* PubSub.take(subscription).pipe(
                  Effect.flatMap((event) =>
                    Ref.update(observedEvents, (events) => [...events, event]),
                  ),
                  Effect.forkScoped({ startImmediately: true }),
                );
              }
              currentRecoveryGate = yield* Deferred.make<void>();
              yield* Deferred.await(currentRecoveryGate).pipe(
                Effect.andThen(Ref.update(recoveryCalls, (count) => count + 1)),
                Effect.forkScoped({ startImmediately: true }),
              );
              yield* Deferred.await(providerBarrier).pipe(
                Effect.andThen(Ref.update(parkedProviderReleases, (count) => count + 1)),
                Effect.onInterrupt(() =>
                  Ref.update(parkedProviderInterrupts, (count) => count + 1),
                ),
                Effect.forkScoped({ startImmediately: true }),
              );
            }),
          commit: () =>
            Effect.gen(function* () {
              assert.equal(yield* Ref.get(activeSubscriptions), 2);
              assert.isDefined(currentRecoveryGate);
              yield* Ref.update(barrierOpens, (count) => count + 1);
              yield* Deferred.succeed(providerBarrier, undefined);
              yield* Deferred.succeed(currentRecoveryGate!, undefined);
              yield* PubSub.publish(providerEvents, "retry-provider-event");
            }),
        };
        const agentControlReactor = {
          start: () =>
            Ref.getAndUpdate(agentControlFailures, (count) => Math.max(0, count - 1)).pipe(
              Effect.flatMap((remaining) =>
                remaining > 0 ? Effect.die(new Error("agent-control-startup-defect")) : Effect.void,
              ),
            ),
        };
        const providerSessionReaper = {
          start: () =>
            Ref.getAndUpdate(reaperFailures, (count) => Math.max(0, count - 1)).pipe(
              Effect.flatMap((remaining) =>
                remaining > 0 ? Effect.die(new Error("reaper-startup-defect")) : Effect.void,
              ),
            ),
        };

        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              ServerRuntimeStartup.startReactorsAtomically({
                ownerScope,
                orchestrationReactor,
                agentControlReactor,
                providerSessionReaper,
              }),
            ),
          ),
        );
        assert.equal(yield* Ref.get(activeSubscriptions), 0);
        assert.equal(yield* Ref.get(barrierOpens), 0);
        assert.equal(yield* Ref.get(recoveryCalls), 0);

        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              ServerRuntimeStartup.startReactorsAtomically({
                ownerScope,
                orchestrationReactor,
                agentControlReactor,
                providerSessionReaper,
              }),
            ),
          ),
        );
        assert.equal(yield* Ref.get(activeSubscriptions), 0);
        assert.equal(yield* Ref.get(barrierOpens), 0);
        assert.equal(yield* Ref.get(recoveryCalls), 0);

        yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper,
        });
        yield* Effect.yieldNow;

        assert.equal(yield* Ref.get(activeSubscriptions), 2);
        assert.equal(yield* Ref.get(releasedSubscriptions), 4);
        assert.equal(yield* Ref.get(barrierOpens), 1);
        assert.equal(yield* Ref.get(recoveryCalls), 1);
        assert.equal(yield* Ref.get(parkedProviderInterrupts), 2);
        assert.equal(yield* Ref.get(parkedProviderReleases), 1);
        assert.deepStrictEqual([...(yield* Ref.get(observedEvents))].sort(), [
          "retry-provider-event",
          "retry-provider-event",
        ]);
      }),
    ),
);

it.effect("shares one attempt activation and discards it when Reaper startup fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const ownerScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(ownerScope, Exit.void));
      const activations = yield* Ref.make(0);
      const finalized = yield* Ref.make(0);
      const reaperFailures = yield* Ref.make(1);
      let orchestrationActivation: ReactorStartupActivation | undefined;
      let agentControlActivation: ReactorStartupActivation | undefined;
      const park = (activation: ReactorStartupActivation) =>
        activation.await.pipe(
          Effect.andThen(Ref.update(activations, (count) => count + 1)),
          Effect.forkScoped({ startImmediately: true }),
          Effect.asVoid,
        );
      const orchestrationReactor = {
        start: (activation?: ReactorStartupActivation) =>
          Effect.gen(function* () {
            orchestrationActivation = activation;
            yield* Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1));
            yield* park(activation!);
          }),
        commit: () => orchestrationActivation!.open,
      };
      const agentControlReactor = {
        start: (activation?: ReactorStartupActivation) =>
          Effect.gen(function* () {
            agentControlActivation = activation;
            yield* Effect.addFinalizer(() => Ref.update(finalized, (count) => count + 1));
            yield* park(activation!);
          }),
      };
      const providerSessionReaper = {
        start: () =>
          Ref.getAndUpdate(reaperFailures, (count) => Math.max(0, count - 1)).pipe(
            Effect.flatMap((remaining) =>
              remaining > 0 ? Effect.die("reaper-startup-defect") : Effect.void,
            ),
          ),
      };

      const failed = yield* Effect.exit(
        ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper,
        }),
      );
      assert.isTrue(Exit.isFailure(failed));
      assert.strictEqual(orchestrationActivation, agentControlActivation);
      assert.equal(yield* Ref.get(activations), 0);
      assert.equal(yield* Ref.get(finalized), 2);

      orchestrationActivation = undefined;
      agentControlActivation = undefined;
      yield* ServerRuntimeStartup.startReactorsAtomically({
        ownerScope,
        orchestrationReactor,
        agentControlReactor,
        providerSessionReaper,
      });
      yield* Effect.yieldNow;
      assert.strictEqual(orchestrationActivation, agentControlActivation);
      assert.equal(yield* Ref.get(activations), 2);
      assert.equal(yield* Ref.get(finalized), 2);
      yield* Scope.close(ownerScope, Exit.void);
      assert.equal(yield* Ref.get(finalized), 4);
    }),
  ),
);

it.effect("lets parent shutdown close every owned attempt before cutover and retries cleanly", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const finalized = yield* Ref.make<ReadonlyArray<string>>([]);
      const barrierOpens = yield* Ref.make(0);
      const reaperEntered = yield* Deferred.make<void>();
      const releaseReaper = yield* Deferred.make<void>();
      const pauseReaper = yield* Ref.make(true);
      let activation: ReactorStartupActivation | undefined;
      const startOwnedAttempt = (name: string) =>
        Effect.gen(function* () {
          const ownerScope = yield* Scope.Scope;
          const attemptScope = yield* Scope.make("sequential");
          yield* Scope.addFinalizerExit(ownerScope, (exit) => Scope.close(attemptScope, exit));
          yield* Scope.addFinalizer(
            attemptScope,
            Ref.update(finalized, (entries) => [...entries, name]),
          );
        });
      const orchestrationReactor = {
        start: (attemptActivation?: ReactorStartupActivation) =>
          Effect.sync(() => {
            activation = attemptActivation;
          }).pipe(Effect.andThen(startOwnedAttempt("orchestration"))),
        commit: () =>
          Ref.update(barrierOpens, (count) => count + 1).pipe(Effect.andThen(activation!.open)),
      };
      const agentControlReactor = {
        start: (attemptActivation?: ReactorStartupActivation) =>
          Effect.gen(function* () {
            assert.strictEqual(attemptActivation, activation);
            yield* startOwnedAttempt("agent-control");
          }),
      };
      const providerSessionReaper = {
        start: () =>
          Effect.gen(function* () {
            yield* startOwnedAttempt("reaper");
            if (!(yield* Ref.get(pauseReaper))) return;
            yield* Deferred.succeed(reaperEntered, undefined);
            yield* Deferred.await(releaseReaper);
          }),
      };
      const failedOwner = yield* Scope.make("sequential");
      const startup = yield* ServerRuntimeStartup.startReactorsAtomically({
        ownerScope: failedOwner,
        orchestrationReactor,
        agentControlReactor,
        providerSessionReaper,
      }).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(reaperEntered);
      yield* Scope.close(failedOwner, Exit.interrupt("shutdown-before-cutover" as never));
      assert.deepStrictEqual(yield* Ref.get(finalized), [
        "reaper",
        "agent-control",
        "orchestration",
      ]);
      assert.equal(yield* Ref.get(barrierOpens), 0);
      const parkedActivation = activation!;
      const activationWaiter = yield* parkedActivation.await.pipe(Effect.forkChild);
      assert.equal(activationWaiter.pollUnsafe(), undefined);
      yield* Fiber.interrupt(activationWaiter);
      yield* Deferred.succeed(releaseReaper, undefined);
      assert.isTrue(Exit.isFailure(yield* Fiber.await(startup)));

      yield* Ref.set(pauseReaper, false);
      const retryOwner = yield* Scope.make("sequential");
      yield* ServerRuntimeStartup.startReactorsAtomically({
        ownerScope: retryOwner,
        orchestrationReactor,
        agentControlReactor,
        providerSessionReaper,
      });
      yield* activation!.await;
      assert.equal(yield* Ref.get(barrierOpens), 1);
      yield* Scope.close(retryOwner, Exit.void);
      assert.deepStrictEqual(yield* Ref.get(finalized), [
        "reaper",
        "agent-control",
        "orchestration",
        "reaper",
        "agent-control",
        "orchestration",
      ]);
    }),
  ),
);

it.effect.each(["before-barrier", "after-barrier", "after-gate"] as const)(
  "keeps all server attempt resources alive through the %s cutover phase",
  (phase) =>
    Effect.scoped(
      Effect.gen(function* () {
        const ownerScope = yield* Scope.make("sequential");
        const finalized = yield* Ref.make<ReadonlyArray<string>>([]);
        const reached = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const barrierOpens = yield* Ref.make(0);
        let activation: ReactorStartupActivation | undefined;
        const startOwnedAttempt = (name: string) =>
          Effect.gen(function* () {
            const resourcesOwner = yield* Scope.Scope;
            const attemptScope = yield* Scope.make("sequential");
            yield* Scope.addFinalizerExit(resourcesOwner, (exit) =>
              Scope.close(attemptScope, exit),
            );
            yield* Scope.addFinalizer(
              attemptScope,
              Ref.update(finalized, (entries) => [...entries, name]),
            );
          });
        const pause = (at: typeof phase) =>
          phase === at
            ? Deferred.succeed(reached, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void;
        const startup = yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor: {
            start: (attemptActivation?: ReactorStartupActivation) =>
              Effect.sync(() => {
                activation = attemptActivation;
              }).pipe(Effect.andThen(startOwnedAttempt("orchestration"))),
            commit: () =>
              Effect.gen(function* () {
                yield* pause("before-barrier");
                yield* Ref.update(barrierOpens, (count) => count + 1);
                yield* pause("after-barrier");
                yield* activation!.open;
                yield* pause("after-gate");
              }),
          },
          agentControlReactor: {
            start: (attemptActivation?: ReactorStartupActivation) =>
              Effect.gen(function* () {
                assert.strictEqual(attemptActivation, activation);
                yield* startOwnedAttempt("agent-control");
              }),
          },
          providerSessionReaper: { start: () => startOwnedAttempt("reaper") },
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(reached);
        const shutdown = yield* Scope.close(
          ownerScope,
          Exit.interrupt(`shutdown-${phase}` as never),
        ).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Effect.yieldNow;
        assert.equal(shutdown.pollUnsafe(), undefined);
        assert.deepStrictEqual(yield* Ref.get(finalized), []);
        assert.equal(yield* Ref.get(barrierOpens), phase === "before-barrier" ? 0 : 1);

        yield* Deferred.succeed(release, undefined);
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(startup)));
        assert.isTrue(Exit.isSuccess(yield* Fiber.await(shutdown)));
        assert.deepStrictEqual(yield* Ref.get(finalized), [
          "reaper",
          "agent-control",
          "orchestration",
        ]);
        assert.equal(yield* Ref.get(barrierOpens), 1);
      }),
    ),
);

it.effect(
  "drains a provider event accepted before cutover when parent close follows immediately",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ownerScope = yield* Scope.make("sequential");
        type Publication =
          | { readonly _tag: "Event"; readonly event: string }
          | {
              readonly _tag: "Drain";
              readonly runtime: Deferred.Deferred<void>;
              readonly verification: Deferred.Deferred<void>;
            };
        const providerEvents = yield* PubSub.unbounded<Publication>();
        const observed = yield* Ref.make<ReadonlyArray<string>>([]);
        const eventTaken = yield* Deferred.make<void>();
        const providerBarrier = yield* Deferred.make<void>();
        const eventPublished = yield* Deferred.make<void>();
        let activation: ReactorStartupActivation | undefined;
        const orchestrationReactor = {
          start: (attemptActivation?: ReactorStartupActivation) =>
            Effect.gen(function* () {
              activation = attemptActivation;
              const runtimeSubscription = yield* PubSub.subscribe(providerEvents);
              const verificationSubscription = yield* PubSub.subscribe(providerEvents);
              const startConsumer = (
                role: "runtime" | "verification",
                subscription: PubSub.Subscription<Publication>,
              ) =>
                Stream.runForEach(Stream.fromSubscription(subscription), (publication) =>
                  (role === "verification" ? activation!.await : Effect.void).pipe(
                    Effect.andThen(
                      publication._tag === "Event"
                        ? Ref.update(observed, (events) => [
                            ...events,
                            `${role}:${publication.event}`,
                          ])
                        : Deferred.succeed(
                            role === "runtime" ? publication.runtime : publication.verification,
                            undefined,
                          ).pipe(Effect.asVoid),
                    ),
                  ),
                ).pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid);
              yield* startConsumer("runtime", runtimeSubscription);
              yield* startConsumer("verification", verificationSubscription);
              yield* Deferred.succeed(eventTaken, undefined).pipe(
                Effect.andThen(Deferred.await(providerBarrier)),
                Effect.andThen(
                  PubSub.publish(providerEvents, { _tag: "Event", event: "turn.started" }),
                ),
                Effect.andThen(Deferred.succeed(eventPublished, undefined)),
                Effect.forkScoped({ startImmediately: true }),
              );
              yield* Deferred.await(eventTaken);
              const runtime = yield* Deferred.make<void>();
              const verification = yield* Deferred.make<void>();
              yield* activation!.registerShutdownDrain(
                PubSub.publish(providerEvents, { _tag: "Drain", runtime, verification }).pipe(
                  Effect.andThen(
                    Effect.all([Deferred.await(runtime), Deferred.await(verification)], {
                      concurrency: "unbounded",
                      discard: true,
                    }),
                  ),
                  Effect.asVoid,
                ),
              );
            }),
          commit: () =>
            Deferred.succeed(providerBarrier, undefined).pipe(
              Effect.andThen(Deferred.await(eventPublished)),
              Effect.andThen(activation!.open),
            ),
        };
        const agentFinalized = yield* Ref.make(0);
        const finalizedAfterBothConsumers = yield* Ref.make(false);
        const agentControlReactor = {
          start: (attemptActivation?: ReactorStartupActivation) =>
            Effect.gen(function* () {
              assert.strictEqual(attemptActivation, activation);
              yield* Effect.addFinalizer(() =>
                Effect.gen(function* () {
                  const entries = yield* Ref.get(observed);
                  yield* Ref.set(
                    finalizedAfterBothConsumers,
                    entries.includes("runtime:turn.started") &&
                      entries.includes("verification:turn.started"),
                  );
                  yield* Ref.update(agentFinalized, (count) => count + 1);
                }),
              );
            }),
        };

        yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper: { start: () => Effect.void },
        });
        yield* Scope.close(ownerScope, Exit.interrupt("immediate-parent-close" as never));
        assert.deepStrictEqual([...(yield* Ref.get(observed))].sort(), [
          "runtime:turn.started",
          "verification:turn.started",
        ]);
        assert.equal(yield* Ref.get(agentFinalized), 1);
        assert.isTrue(yield* Ref.get(finalizedAfterBothConsumers));
      }),
    ),
);

it.effect(
  "waits for attempt-owned Coordinator and Stage-Starter termination before rollback and retries",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const coordinatorEntered = yield* Deferred.make<void>();
        const stageStarterEntered = yield* Deferred.make<void>();
        const reaperEntered = yield* Deferred.make<void>();
        const releaseReaper = yield* Deferred.make<void>();
        const reaperFailures = yield* Ref.make(1);
        const coordinatorAttempts = yield* Ref.make(0);
        const stageStarterAttempts = yield* Ref.make(0);
        const completedWrites = yield* Ref.make<ReadonlyArray<string>>([]);
        let coordinatorDrain: Effect.Effect<void> = Effect.void;
        let stageStarterDrain: Effect.Effect<void> = Effect.void;

        const makeAttemptWorker = Effect.fn("makeAttemptWorker")(function* (
          name: "coordinator" | "stage-starter",
          attempts: Ref.Ref<number>,
          entered: Deferred.Deferred<void>,
        ) {
          const attempt = yield* Ref.getAndUpdate(attempts, (count) => count + 1);
          const worker = yield* makeDrainableWorker(
            () =>
              attempt === 0
                ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
                : Ref.update(completedWrites, (writes) => [...writes, name]),
            { failureMode: "observable" },
          );
          yield* worker.enqueue(undefined);
          return yield* Effect.succeed(worker.drain);
        });
        const agentLayer = AgentControlReactorLive.pipe(
          Layer.provide(
            Layer.mergeAll(
              Layer.succeed(
                AgentControlGithubObserveReactor,
                AgentControlGithubObserveReactor.of({
                  start: () => Effect.void,
                  getStatus: () => Effect.die("unused"),
                }),
              ),
              Layer.succeed(
                AgentControlTaskIntakeReactor,
                AgentControlTaskIntakeReactor.of({
                  start: () => Effect.void,
                  getStatus: () => Effect.die("unused"),
                }),
              ),
              Layer.succeed(
                AgentControlVerificationStageStarter,
                AgentControlVerificationStageStarter.of({
                  processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                  recover: Effect.void,
                  prepare: () =>
                    makeAttemptWorker(
                      "stage-starter",
                      stageStarterAttempts,
                      stageStarterEntered,
                    ).pipe(
                      Effect.tap((drain) =>
                        Effect.sync(() => {
                          stageStarterDrain = drain;
                        }),
                      ),
                      Effect.asVoid,
                    ),
                  start: () => Effect.void,
                  drain: Effect.suspend(() => stageStarterDrain),
                }),
              ),
              Layer.succeed(
                AgentControlVerificationTurnCoordinator,
                AgentControlVerificationTurnCoordinator.of({
                  processHandoff: () => Effect.succeed({ _tag: "NotCandidate" }),
                  recover: Effect.void,
                  prepare: () =>
                    makeAttemptWorker("coordinator", coordinatorAttempts, coordinatorEntered).pipe(
                      Effect.tap((drain) =>
                        Effect.sync(() => {
                          coordinatorDrain = drain;
                        }),
                      ),
                      Effect.asVoid,
                    ),
                  start: () => Effect.void,
                  drain: Effect.suspend(() => coordinatorDrain),
                  streamPublications: Stream.never,
                }),
              ),
              Layer.succeed(
                AgentControlVerificationEvaluator,
                AgentControlVerificationEvaluator.of({
                  processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                  recover: Effect.void,
                  prepare: () => Effect.void,
                  drain: Effect.void,
                }),
              ),
              Layer.succeed(
                AgentControlVerificationStageFinalizer,
                AgentControlVerificationStageFinalizer.of({
                  processHandoff: () => Effect.succeed({ _tag: "Waiting" }),
                  recover: Effect.void,
                  prepare: () => Effect.void,
                  drain: Effect.void,
                }),
              ),
              Layer.succeed(
                AgentControlTaskVerificationFinalizer,
                AgentControlTaskVerificationFinalizer.of({
                  processHandoff: () => Effect.die("unused"),
                  recover: Effect.void,
                  prepare: () => Effect.void,
                  drain: Effect.void,
                }),
              ),
              Layer.succeed(
                AgentControlRunOnceController,
                AgentControlRunOnceController.of({
                  recover: Effect.void,
                  processProject: () => Effect.void,
                  prepare: () => Effect.void,
                  recoverPublicationConsumer: () => Effect.void,
                  pullPublications: () => Effect.succeed([]),
                  acknowledgePublication: () => Effect.void,
                  subscribePublicationWakeups: Effect.succeed(Stream.never),
                  subscribePublications: Effect.succeed(Stream.never),
                }),
              ),
              Layer.succeed(
                AgentControlVerificationAdmission,
                AgentControlVerificationAdmission.of({
                  processResultEvidence: () => Effect.succeed({ _tag: "NotCandidate" }),
                  recover: Effect.void,
                  start: () => Effect.void,
                  drain: Effect.void,
                  streamPublications: Stream.never,
                  subscribePublications: Effect.succeed(Stream.never),
                  loadAcceptedEvidence: () => Effect.succeed(Option.none()),
                }),
              ),
            ),
          ),
        );
        const agentControlReactor = yield* AgentControlReactor.pipe(Effect.provide(agentLayer));
        let activation: ReactorStartupActivation | undefined;
        const orchestrationReactor = {
          start: (attemptActivation?: ReactorStartupActivation) =>
            Effect.sync(() => {
              activation = attemptActivation;
            }),
          commit: () => activation!.open,
        };
        const providerSessionReaper = {
          start: () =>
            Ref.getAndUpdate(reaperFailures, (count) => Math.max(0, count - 1)).pipe(
              Effect.flatMap((remaining) =>
                remaining === 0
                  ? Effect.void
                  : Deferred.succeed(reaperEntered, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseReaper)),
                      Effect.andThen(Effect.die("reaper-startup-defect")),
                    ),
              ),
            ),
        };
        const ownerScope = yield* Scope.make("sequential");
        const failedStartup = yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper,
        }).pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(coordinatorEntered);
        yield* Deferred.await(stageStarterEntered);
        yield* Deferred.await(reaperEntered);
        const oldCoordinatorDrain = coordinatorDrain;
        const oldStageStarterDrain = stageStarterDrain;
        const coordinatorWaiter = yield* oldCoordinatorDrain.pipe(Effect.forkChild);
        const stageStarterWaiter = yield* oldStageStarterDrain.pipe(Effect.forkChild);
        yield* Deferred.succeed(releaseReaper, undefined);

        const failedExit = yield* Fiber.await(failedStartup);
        assert.isTrue(Exit.isFailure(failedExit));
        for (const waiter of [coordinatorWaiter, stageStarterWaiter]) {
          const drainExit = yield* Fiber.await(waiter);
          assert.isTrue(Exit.isFailure(drainExit));
          if (Exit.isFailure(drainExit)) assert.isTrue(Cause.hasInterruptsOnly(drainExit.cause));
        }
        assert.deepStrictEqual(yield* Ref.get(completedWrites), []);
        yield* Effect.yieldNow;
        assert.deepStrictEqual(yield* Ref.get(completedWrites), []);

        yield* ServerRuntimeStartup.startReactorsAtomically({
          ownerScope,
          orchestrationReactor,
          agentControlReactor,
          providerSessionReaper,
        });
        yield* Effect.all([coordinatorDrain, stageStarterDrain], { concurrency: "unbounded" });
        assert.deepStrictEqual([...(yield* Ref.get(completedWrites))].sort(), [
          "coordinator",
          "stage-starter",
        ]);
        assert.equal(yield* Ref.get(coordinatorAttempts), 2);
        assert.equal(yield* Ref.get(stageStarterAttempts), 2);
        yield* Scope.close(ownerScope, Exit.void);
      }),
    ),
);

it.effect("preserves startup and rollback causes at the server readiness boundary", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const typedFailure = new AgentControlGithubObserveStartupError({
        reason: "enumeration-failed",
      });
      const runOnceFailure = new AgentControlRunOnceError({
        projectId: ProjectId.make("run-once-startup-failure"),
        runId: null,
        step: null,
        reason: "projection-corrupt",
      });
      const startupDefect = new Error("server-startup-defect");
      const cases = [
        {
          name: "defect-clean-rollback",
          startup: Effect.die(startupDefect),
          rollbackDefect: undefined,
          assertOriginal: (cause: Cause.Cause<unknown>) =>
            cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === startupDefect,
            ),
        },
        {
          name: "defect-rollback-defect",
          startup: Effect.die(startupDefect),
          rollbackDefect: new Error("server-defect-rollback-defect"),
          assertOriginal: (cause: Cause.Cause<unknown>) =>
            cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === startupDefect,
            ),
        },
        {
          name: "typed-rollback-defect",
          startup: Effect.fail(typedFailure),
          rollbackDefect: new Error("server-typed-rollback-defect"),
          assertOriginal: (cause: Cause.Cause<unknown>) =>
            cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === typedFailure,
            ),
        },
        {
          name: "run-once-typed-clean-rollback",
          startup: Effect.fail(runOnceFailure),
          rollbackDefect: undefined,
          assertOriginal: (cause: Cause.Cause<unknown>) =>
            cause.reasons.some(
              (reason) => Cause.isFailReason(reason) && reason.error === runOnceFailure,
            ),
        },
        {
          name: "interrupt-rollback-defect",
          startup: Effect.interrupt,
          rollbackDefect: new Error("server-interrupt-rollback-defect"),
          assertOriginal: (cause: Cause.Cause<unknown>) => Cause.hasInterrupts(cause),
        },
      ] as const;

      for (const testCase of cases) {
        const ownerScope = yield* Scope.make("sequential");
        const commandGate = yield* ServerRuntimeStartup.makeCommandGate;
        const queued = yield* commandGate.enqueueCommand(Effect.void).pipe(Effect.forkChild);
        const barrierOpens = yield* Ref.make(0);
        const orchestrationReactor = {
          start: () =>
            Effect.acquireRelease(Effect.void, () =>
              testCase.rollbackDefect === undefined
                ? Effect.void
                : Effect.die(testCase.rollbackDefect),
            ),
          commit: () => Ref.update(barrierOpens, (count) => count + 1),
        };
        const opened = yield* ServerRuntimeStartup.openCommandReadinessAfterStartup(
          ServerRuntimeStartup.startReactorsAtomically({
            ownerScope,
            orchestrationReactor,
            agentControlReactor: { start: () => testCase.startup as never },
            providerSessionReaper: { start: () => Effect.void },
          }),
          commandGate,
          { mode: "web", host: "127.0.0.1", port: 3773 },
        );
        assert.isFalse(opened, testCase.name);
        const readinessError = yield* Effect.flip(Fiber.join(queued));
        const cause = readinessError.cause as Cause.Cause<unknown>;
        assert.isTrue(testCase.assertOriginal(cause), testCase.name);
        if (testCase.rollbackDefect !== undefined) {
          assert.isTrue(
            cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === testCase.rollbackDefect,
            ),
            testCase.name,
          );
        }
        assert.equal(yield* Ref.get(barrierOpens), 0);
        yield* Scope.close(ownerScope, Exit.void).pipe(Effect.ignore);
      }
    }),
  ),
);

it.effect(
  "combines post-cutover commit and close defects without making the attempt retryable",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const ownerScope = yield* Scope.make("sequential");
        const commitDefect = new Error("server-post-cutover-commit-defect");
        const closeDefect = new Error("server-post-cutover-close-defect");
        let activation: ReactorStartupActivation | undefined;
        const startupExit = yield* Effect.exit(
          ServerRuntimeStartup.startReactorsAtomically({
            ownerScope,
            orchestrationReactor: {
              start: (attemptActivation?: ReactorStartupActivation) =>
                Effect.sync(() => {
                  activation = attemptActivation;
                }).pipe(
                  Effect.andThen(Effect.acquireRelease(Effect.void, () => Effect.die(closeDefect))),
                ),
              commit: () => activation!.open.pipe(Effect.andThen(Effect.die(commitDefect))),
            },
            agentControlReactor: { start: () => Effect.void },
            providerSessionReaper: { start: () => Effect.void },
          }),
        );
        assert.isTrue(Exit.isFailure(startupExit));
        if (Exit.isFailure(startupExit)) {
          assert.isTrue(
            startupExit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === commitDefect,
            ),
          );
          assert.isTrue(
            startupExit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === closeDefect,
            ),
          );
        }
        assert.equal(yield* activation!.closeDisposition, "terminal");
        yield* Scope.close(ownerScope, Exit.void).pipe(Effect.ignore);
      }),
    ),
);

it.effect("launchStartupHeartbeat does not block the caller while counts are loading", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const releaseCounts = yield* Deferred.make<void, never>();

      yield* ServerRuntimeStartup.launchStartupHeartbeat.pipe(
        Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
          getCommandReadModel: () => Effect.die("unused"),
          getSnapshot: () => Effect.die("unused"),
          getShellSnapshot: () => Effect.die("unused"),
          getArchivedShellSnapshot: () => Effect.die("unused"),
          getSnapshotSequence: () => Effect.die("unused"),
          getCounts: () =>
            Deferred.await(releaseCounts).pipe(
              Effect.as({
                projectCount: 2,
                threadCount: 3,
              }),
            ),
          getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
          getProjectShellById: () => Effect.succeed(Option.none()),
          getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
          getThreadCheckpointContext: () => Effect.succeed(Option.none()),
          getFullThreadDiffContext: () => Effect.succeed(Option.none()),
          getThreadShellById: () => Effect.succeed(Option.none()),
          getThreadDetailById: () => Effect.succeed(Option.none()),
          getThreadDetailSnapshot: () => Effect.succeed(Option.none()),
        }),
        Effect.provideService(AnalyticsService.AnalyticsService, {
          record: () => Effect.void,
          flush: Effect.void,
        }),
      );
    }),
  ),
);

it.effect("resolveWelcomeBase derives cwd and project name from server config", () =>
  Effect.gen(function* () {
    const welcome = yield* ServerRuntimeStartup.resolveWelcomeBase.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
      } as never),
    );

    assert.deepStrictEqual(welcome, {
      cwd: "/tmp/startup-project",
      projectName: "startup-project",
    });
  }),
);

it.effect("resolveAutoBootstrapWelcomeTargets returns existing project and thread ids", () => {
  const bootstrapProjectId = ProjectId.make("project-startup-bootstrap");
  const bootstrapThreadId = ThreadId.make("thread-startup-bootstrap");

  return Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const targets = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () =>
          Effect.succeed(
            Option.some({
              id: bootstrapProjectId,
              title: "Startup Project",
              workspaceRoot: "/tmp/startup-project",
              defaultModelSelection: ServerRuntimeStartup.getAutoBootstrapDefaultModelSelection(),
              scripts: [],
              createdAt: "2026-01-01T00:00:00.000Z",
              updatedAt: "2026-01-01T00:00:00.000Z",
              deletedAt: null,
            }),
          ),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.some(bootstrapThreadId)),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getFullThreadDiffContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        dispatchClient: () => Effect.die("unused"),
        dispatchAgentControl: () => Effect.die("unused"),
        streamDomainEvents: Stream.empty,
        latestSequence: Effect.succeed(0),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provide(NodeServices.layer),
    );

    assert.deepStrictEqual(targets, {
      bootstrapProjectId,
      bootstrapThreadId,
    });
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
  });
});

it.effect("resolveAutoBootstrapWelcomeTargets creates a project and thread when missing", () =>
  Effect.gen(function* () {
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const targets = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getFullThreadDiffContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        dispatchClient: () => Effect.die("unused"),
        dispatchAgentControl: () => Effect.die("unused"),
        streamDomainEvents: Stream.empty,
        latestSequence: Effect.succeed(0),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provide(NodeServices.layer),
    );

    assert.equal(typeof targets.bootstrapProjectId, "string");
    assert.equal(typeof targets.bootstrapThreadId, "string");
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), ["project.create", "thread.create"]);
  }),
);

it.effect("resolveAutoBootstrapWelcomeTargets preserves typed UUID generation failures", () =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const uuidError = PlatformError.systemError({
      _tag: "Unknown",
      module: "Crypto",
      method: "randomUUIDv4",
      description: "UUID generation unavailable",
    });
    const dispatchCalls = yield* Ref.make<ReadonlyArray<string>>([]);

    const error = yield* ServerRuntimeStartup.resolveAutoBootstrapWelcomeTargets.pipe(
      Effect.provideService(ServerConfig.ServerConfig, {
        cwd: "/tmp/startup-project",
        autoBootstrapProjectFromCwd: true,
      } as never),
      Effect.provideService(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getCommandReadModel: () => Effect.die("unused"),
        getSnapshot: () => Effect.die("unused"),
        getShellSnapshot: () => Effect.die("unused"),
        getArchivedShellSnapshot: () => Effect.die("unused"),
        getSnapshotSequence: () => Effect.die("unused"),
        getCounts: () => Effect.die("unused"),
        getActiveProjectByWorkspaceRoot: () => Effect.succeed(Option.none()),
        getProjectShellById: () => Effect.die("unused"),
        getFirstActiveThreadIdByProjectId: () => Effect.succeed(Option.none()),
        getThreadCheckpointContext: () => Effect.succeed(Option.none()),
        getFullThreadDiffContext: () => Effect.succeed(Option.none()),
        getThreadShellById: () => Effect.die("unused"),
        getThreadDetailById: () => Effect.die("unused"),
        getThreadDetailSnapshot: () => Effect.die("unused"),
      }),
      Effect.provideService(OrchestrationEngine.OrchestrationEngineService, {
        readEvents: () => Stream.empty,
        dispatch: (command) =>
          Ref.update(dispatchCalls, (calls) => [...calls, command.type]).pipe(
            Effect.as({ sequence: 1 }),
          ),
        dispatchClient: () => Effect.die("unused"),
        dispatchAgentControl: () => Effect.die("unused"),
        streamDomainEvents: Stream.empty,
        latestSequence: Effect.succeed(0),
      } satisfies OrchestrationEngine.OrchestrationEngineService["Service"]),
      Effect.provideService(Crypto.Crypto, {
        ...crypto,
        randomUUIDv4: Effect.fail(uuidError),
      }),
      Effect.flip,
    );

    assert.strictEqual(error, uuidError);
    assert.deepStrictEqual(yield* Ref.get(dispatchCalls), []);
  }).pipe(Effect.provide(NodeServices.layer)),
);
