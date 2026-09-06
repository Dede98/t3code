import { ProviderInstanceId, type ProviderUsageSnapshot } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";

import { AgentControlImplementationTurnWakeup } from "../../implementationTurn/Services/AgentControlImplementationTurnWakeup.ts";
import { AgentControlInitialPlanningWakeup } from "../../initialPlanning/Services/AgentControlInitialPlanningWakeup.ts";
import { AgentControlVerificationTurnWakeup } from "../../verificationTurn/Services/AgentControlVerificationTurnWakeup.ts";
import { ProviderUsage } from "../../../provider/Services/ProviderUsage.ts";
import { providerAdmissionUsageEvidence, type ProviderAdmissionUsageEvidence } from "../model.ts";
import {
  ProviderAdmissionRuntime,
  type ProviderAdmissionRuntimeShape,
} from "../Services/ProviderAdmissionRuntime.ts";
import {
  ProviderAdmissionError,
  ProviderAdmissionStore,
  type ProviderAdmissionWakeup,
} from "../Services/ProviderAdmissionStore.ts";

const CLAIM_DURATION = Duration.minutes(2);

const nextRelevantAt = (snapshot: ProviderUsageSnapshot): string | null => {
  const deadlines = [
    ...snapshot.windows.map((window) => window.resetsAt),
    snapshot.overageResetsAt ?? null,
  ].filter((value): value is string => value !== null);
  return deadlines.length === 0 ? null : deadlines.sort()[0]!;
};

export const providerUsageAdmissionEvidence = (
  providerInstanceId: ProviderInstanceId,
  observation:
    | { readonly _tag: "Unsupported"; readonly observedAt: string }
    | { readonly _tag: "Observed"; readonly snapshot: ProviderUsageSnapshot }
    | { readonly _tag: "SupportedUnusable"; readonly observedAt: string },
): ProviderAdmissionUsageEvidence =>
  observation._tag === "Observed"
    ? providerAdmissionUsageEvidence({
        providerInstanceId,
        status: observation.snapshot.status,
        observedAt: observation.snapshot.observedAt,
        source: observation.snapshot.source,
        nextRelevantAt: nextRelevantAt(observation.snapshot),
      })
    : providerAdmissionUsageEvidence({
        providerInstanceId,
        status: observation._tag === "Unsupported" ? "unsupported" : "supported-unusable",
        observedAt: observation.observedAt,
        source: observation._tag === "Unsupported" ? "capability" : "refresh-error",
        nextRelevantAt: null,
      });

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const usage = yield* ProviderUsage;
  const store = yield* ProviderAdmissionStore;
  const initialPlanningWakeup = yield* AgentControlInitialPlanningWakeup;
  const implementationWakeup = yield* AgentControlImplementationTurnWakeup;
  const verificationWakeup = yield* AgentControlVerificationTurnWakeup;
  const runtimeScope = yield* Effect.scope;
  const providerSignals = yield* PubSub.unbounded<string>();
  const deadlineSignals = yield* PubSub.unbounded<void>();
  const runtimeFailure = yield* Deferred.make<never, ProviderAdmissionError>();
  const ownerId = yield* crypto.randomUUIDv4.pipe(
    Effect.orDie,
    Effect.map((uuid) => `provider-admission:${uuid}`),
  );

  const wake = (publication: ProviderAdmissionWakeup) =>
    publication.stage === "initial-planning"
      ? initialPlanningWakeup.wake(publication.handoffId)
      : publication.stage === "implementation"
        ? implementationWakeup.wake(publication.handoffId)
        : verificationWakeup.wake(publication.handoffId);

  const forkPump = <A>(operation: string, pump: Effect.Effect<A, ProviderAdmissionError>) =>
    pump.pipe(
      Effect.catchCause((cause) =>
        cause.reasons.every(Cause.isInterruptReason)
          ? Effect.void
          : Deferred.fail(
              runtimeFailure,
              new ProviderAdmissionError({
                operation,
                reason: "persistence",
                cause,
              }),
            ).pipe(Effect.asVoid),
      ),
      Effect.forkIn(runtimeScope, { startImmediately: true }),
    );

  const inspect = (
    providerInstanceId: ProviderInstanceId,
  ): Effect.Effect<ProviderAdmissionUsageEvidence, ProviderAdmissionError> => {
    const inspectForAdmission = usage.inspectForAdmission;
    if (inspectForAdmission === undefined) {
      return Effect.fail(
        new ProviderAdmissionError({
          operation: "usage-inspection",
          reason: "authority-missing",
        }),
      );
    }
    return inspectForAdmission(providerInstanceId).pipe(
      Effect.map((observation) => providerUsageAdmissionEvidence(providerInstanceId, observation)),
    );
  };

  const advanceProvider = Effect.fn("ProviderAdmissionRuntime.advanceProvider")(function* (
    providerInstanceId: string,
  ) {
    const now = yield* DateTime.now;
    const permit = yield* store.admitOldest({
      providerInstanceId,
      ownerId,
      now: DateTime.formatIso(now),
      leaseExpiresAt: DateTime.formatIso(DateTime.addDuration(now, CLAIM_DURATION)),
    });
    if (permit !== null) {
      yield* wake({
        stage: permit.stage,
        handoffId: permit.handoffId,
        providerInstanceId: String(permit.providerInstanceId),
      });
    }
  });

  const usageChanged: ProviderAdmissionRuntimeShape["usageChanged"] = Effect.fn(
    "ProviderAdmissionRuntime.usageChanged",
  )(function* (providerInstanceId, evidence) {
    yield* store.recordUsage(providerInstanceId, evidence);
    yield* advanceProvider(providerInstanceId);
    yield* PubSub.publish(deadlineSignals, undefined);
  });

  const request: ProviderAdmissionRuntimeShape["request"] = Effect.fn(
    "ProviderAdmissionRuntime.request",
  )(function* (input) {
    const now = yield* DateTime.now;
    const nowText = DateTime.formatIso(now);
    const leaseExpiresAt = DateTime.formatIso(DateTime.addDuration(now, CLAIM_DURATION));
    const persisted = yield* store.resume({
      request: input,
      ownerId,
      leaseExpiresAt,
      now: nowText,
    });
    const decision =
      persisted ??
      (yield* store.request({
        request: input,
        usage: yield* inspect(input.providerInstanceId),
        ownerId,
        leaseExpiresAt,
        now: nowText,
      }));
    yield* PubSub.publish(deadlineSignals, undefined);
    if (decision._tag === "Admitted") {
      yield* wake({
        stage: input.stage,
        handoffId: input.handoffId,
        providerInstanceId: String(input.providerInstanceId),
      });
    } else {
      yield* advanceProvider(String(input.providerInstanceId));
    }
    return decision;
  });

  const capacityReleased: ProviderAdmissionRuntimeShape["capacityReleased"] = (
    providerInstanceId,
  ) => PubSub.publish(providerSignals, providerInstanceId).pipe(Effect.asVoid);

  const providerSubscription = yield* PubSub.subscribe(providerSignals);
  yield* Effect.gen(function* () {
    while (true) yield* advanceProvider(yield* PubSub.take(providerSubscription));
  }).pipe((pump) => forkPump("capacity-pump", pump));

  // Subscribe before startup catch-up so an update cannot be lost between the
  // durable scan and live event consumption.
  const usageSubscription = yield* usage.subscribeEvents;
  yield* Effect.gen(function* () {
    while (true) {
      const event = yield* PubSub.take(usageSubscription);
      if (event.type === "removed") {
        const providerInstanceId = String(event.providerInstanceId);
        yield* usageChanged(providerInstanceId, yield* inspect(event.providerInstanceId));
        continue;
      }
      const snapshots = event.type === "snapshot" ? event.usage : [event.usage];
      yield* Effect.forEach(
        snapshots,
        (snapshot) =>
          usageChanged(
            String(snapshot.providerInstanceId),
            providerUsageAdmissionEvidence(snapshot.providerInstanceId, {
              _tag: "Observed",
              snapshot,
            }),
          ),
        { discard: true },
      );
    }
  }).pipe((pump) => forkPump("usage-pump", pump));

  const deadlineSubscription = yield* PubSub.subscribe(deadlineSignals);
  yield* Effect.gen(function* () {
    let lastFiredDeadline: string | null = null;
    while (true) {
      const deadline = yield* store.minimumDeadline;
      if (deadline === null || deadline === lastFiredDeadline) {
        yield* PubSub.take(deadlineSubscription);
        continue;
      }
      const delay = Math.max(
        0,
        DateTime.toEpochMillis(DateTime.makeUnsafe(deadline)) - (yield* Clock.currentTimeMillis),
      );
      const outcome = yield* Effect.raceFirst(
        Effect.sleep(Duration.millis(delay)).pipe(Effect.as("deadline" as const)),
        PubSub.take(deadlineSubscription).pipe(Effect.as("changed" as const)),
      );
      if (outcome === "changed") continue;
      // A reset deadline is a one-shot wakeup for this runtime. If refreshing
      // yields the same evidence, wait for a typed usage/deadline change rather
      // than turning the expired deadline into a polling loop.
      lastFiredDeadline = deadline;
      const waiting = yield* store.listWaiting;
      const providerIds = Array.from(
        new Set(waiting.map((publication) => publication.providerInstanceId)),
      );
      yield* Effect.forEach(
        providerIds,
        (providerId) =>
          Effect.gen(function* () {
            const evidence = yield* inspect(ProviderInstanceId.make(providerId));
            yield* usageChanged(providerId, evidence);
          }),
        { discard: true },
      );
    }
  }).pipe((pump) => forkPump("deadline-pump", pump));

  const waitingProviders = Array.from(
    new Set((yield* store.listWaiting).map((publication) => publication.providerInstanceId)),
  );
  yield* Effect.forEach(waitingProviders, advanceProvider, { discard: true });

  return ProviderAdmissionRuntime.of({
    awaitFailure: Deferred.await(runtimeFailure),
    request,
    usageChanged,
    capacityReleased,
  });
});

export const ProviderAdmissionRuntimeLive = Layer.effect(ProviderAdmissionRuntime, make);
