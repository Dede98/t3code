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
import {
  DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS,
  providerAdmissionUsageEvidence,
  type ProviderAdmissionUsageEvidence,
  type ProviderResourceAdmissionPermit,
} from "../model.ts";
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
const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

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
  const resourceSignals = yield* PubSub.unbounded<void>();
  const deadlineSignals = yield* PubSub.unbounded<void>();
  const resourceDeadlineSignals = yield* PubSub.unbounded<void>();
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
    yield* PubSub.publish(deadlineSignals, undefined);
  });

  const usageChanged: ProviderAdmissionRuntimeShape["usageChanged"] = Effect.fn(
    "ProviderAdmissionRuntime.usageChanged",
  )(function* (providerInstanceId, evidence) {
    const wakeups = yield* store.recordUsage(providerInstanceId, evidence);
    yield* Effect.forEach(wakeups, wake, { discard: true });
    yield* PubSub.publish(resourceSignals, undefined);
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
    yield* advanceProvider(providerInstanceId);
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
    if (decision._tag === "Admitted") {
      yield* PubSub.publish(deadlineSignals, undefined);
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
  ) =>
    Effect.all(
      [
        PubSub.publish(providerSignals, providerInstanceId),
        PubSub.publish(resourceSignals, undefined),
        PubSub.publish(resourceDeadlineSignals, undefined),
      ],
      { discard: true },
    );

  const resourceSettingsChanged = Effect.all(
    [
      PubSub.publish(resourceSignals, undefined),
      PubSub.publish(resourceDeadlineSignals, undefined),
    ],
    { discard: true },
  );

  const wakeAll = (wakeups: ReadonlyArray<ProviderAdmissionWakeup>) =>
    Effect.forEach(wakeups, wake, { discard: true });

  const requestResource: NonNullable<ProviderAdmissionRuntimeShape["requestResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.requestResource",
  )(function* (resourceRequest, limits = DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS) {
    const now = yield* DateTime.now;
    const result = yield* store.requestResource!({
      request: resourceRequest,
      usage: yield* inspect(resourceRequest.providerInstanceId),
      limits,
      ownerId,
      now: DateTime.formatIso(now),
      leaseExpiresAt: DateTime.formatIso(DateTime.addDuration(now, CLAIM_DURATION)),
    });
    yield* wakeAll(result.wakeups);
    if (result.capacityChanged) yield* PubSub.publish(resourceSignals, undefined);
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
    return result.decision;
  });

  const acquireResource: NonNullable<ProviderAdmissionRuntimeShape["acquireResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.acquireResource",
  )(function* (resourceRequest, limits = DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS, readLimits) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(resourceSignals);
        return yield* Effect.uninterruptibleMask((restore) => {
          const loop: Effect.Effect<ProviderResourceAdmissionPermit, ProviderAdmissionError> =
            Effect.suspend(() =>
              (readLimits ?? Effect.succeed(limits)).pipe(
                Effect.flatMap((currentLimits) => requestResource(resourceRequest, currentLimits)),
                Effect.flatMap((decision) => {
                  if (decision._tag === "Admitted") return Effect.succeed(decision.permit);
                  if (decision._tag === "Cancelled")
                    return Effect.fail(
                      new ProviderAdmissionError({
                        operation: "resource-acquire-cancelled",
                        reason: "stale-owner",
                        admissionId: decision.requestId,
                      }),
                    );
                  return restore(PubSub.take(subscription)).pipe(Effect.andThen(loop));
                }),
              ),
            );
          return loop.pipe(
            Effect.onInterrupt(() =>
              cancelResource(resourceRequest).pipe(
                Effect.catchTag("ProviderAdmissionError", (error) =>
                  error.reason === "stale-owner" ? Effect.void : Effect.fail(error),
                ),
              ),
            ),
          );
        });
      }),
    );
  });

  const enterResource: NonNullable<ProviderAdmissionRuntimeShape["enterResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.enterResource",
  )(function* (permit, providerTurnId) {
    yield* store.enterResource!({
      permit,
      enteredAt: yield* nowIso,
      ...(providerTurnId === undefined ? {} : { providerTurnId }),
    });
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
  });

  const releaseResource: NonNullable<ProviderAdmissionRuntimeShape["releaseResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.releaseResource",
  )(function* (permit) {
    const wakeups = yield* store.releaseResource!({
      permit,
      releasedAt: DateTime.formatIso(yield* DateTime.now),
    });
    yield* wakeAll(wakeups);
    yield* PubSub.publish(resourceSignals, undefined);
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
  });

  const cancelResource: NonNullable<ProviderAdmissionRuntimeShape["cancelResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.cancelResource",
  )(function* (resourceRequest) {
    const wakeups = yield* store.cancelResource!({
      request: resourceRequest,
      cancelledAt: DateTime.formatIso(yield* DateTime.now),
    });
    yield* wakeAll(wakeups);
    yield* PubSub.publish(resourceSignals, undefined);
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
  });

  const configureResourceScope: NonNullable<
    ProviderAdmissionRuntimeShape["configureResourceScope"]
  > = (accountScope, limits) =>
    nowIso.pipe(
      Effect.flatMap((updatedAt) =>
        store.configureResourceScope!({ accountScope, limits, updatedAt }),
      ),
      Effect.tap(() => PubSub.publish(resourceSignals, undefined)),
      Effect.tap(() => PubSub.publish(resourceDeadlineSignals, undefined)),
    );

  const reconcileResource: NonNullable<ProviderAdmissionRuntimeShape["reconcileResource"]> =
    Effect.fn("ProviderAdmissionRuntime.reconcileResource")(
      function* (requestId, observedActivity) {
        const now = yield* DateTime.now;
        const permit = yield* store.reconcileResource!({
          requestId,
          observedActivity,
          ownerId,
          observedAt: DateTime.formatIso(now),
          leaseExpiresAt: DateTime.formatIso(DateTime.addDuration(now, CLAIM_DURATION)),
        });
        yield* PubSub.publish(resourceSignals, undefined);
        yield* PubSub.publish(resourceDeadlineSignals, undefined);
        const active = yield* store.listResourceActive!;
        yield* wakeAll(
          active.flatMap((row) =>
            row.source === "automatic" && row.stage !== null && row.handoffId !== null
              ? [
                  {
                    stage: row.stage,
                    handoffId: row.handoffId,
                    providerInstanceId: String(row.providerInstanceId),
                  },
                ]
              : [],
          ),
        );
        return permit;
      },
    );

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
    const firedDeadlineKeys = new Set<string>();
    const waitUntilDeadline = Effect.fn("ProviderAdmissionRuntime.waitUntilDeadline")(function* (
      deadline: string,
    ) {
      const delay = Math.max(
        0,
        DateTime.toEpochMillis(DateTime.makeUnsafe(deadline)) - (yield* Clock.currentTimeMillis),
      );
      if (delay === 0) return "deadline" as const;
      return yield* Effect.raceFirst(
        Effect.sleep(Duration.millis(delay)).pipe(Effect.as("deadline" as const)),
        PubSub.take(deadlineSubscription).pipe(Effect.as("changed" as const)),
      );
    });
    while (true) {
      const deadline = yield* store.minimumDeadline;
      if (deadline === null) {
        firedDeadlineKeys.clear();
        yield* PubSub.take(deadlineSubscription);
        continue;
      }
      if ((yield* waitUntilDeadline(deadline)) === "changed") continue;
      const now = DateTime.formatIso(yield* DateTime.now);
      const due = yield* store.listDueDeadlines(now);
      const dueKeys = new Set(
        due.map(
          (publication) =>
            `${publication.deadlineKind}:${publication.admissionId}:${publication.deadlineAt}`,
        ),
      );
      for (const key of firedDeadlineKeys) {
        if (!dueKeys.has(key)) firedDeadlineKeys.delete(key);
      }
      const unfired = due.filter((publication) => {
        const key = `${publication.deadlineKind}:${publication.admissionId}:${publication.deadlineAt}`;
        if (firedDeadlineKeys.has(key)) return false;
        firedDeadlineKeys.add(key);
        return true;
      });
      // Each persisted deadline identity fires once. Once every currently due
      // identity has fired, schedule the next strictly later persisted deadline;
      // only an authority with no later deadline waits for a typed change signal.
      if (unfired.length === 0) {
        const futureDeadline = yield* store.minimumDeadlineAfter(now);
        if (futureDeadline === null) {
          yield* PubSub.take(deadlineSubscription);
          continue;
        }
        yield* waitUntilDeadline(futureDeadline);
        continue;
      }
      yield* Effect.forEach(
        unfired,
        (publication) =>
          publication.deadlineKind === "lease"
            ? wake(publication)
            : Effect.gen(function* () {
                const evidence = yield* inspect(
                  ProviderInstanceId.make(publication.providerInstanceId),
                );
                yield* usageChanged(publication.providerInstanceId, evidence);
              }),
        { discard: true },
      );
    }
  }).pipe((pump) => forkPump("deadline-pump", pump));

  if (
    store.minimumResourceDeadline !== undefined &&
    store.minimumResourceDeadlineAfter !== undefined &&
    store.listDueResourceDeadlines !== undefined &&
    store.advanceResourceScope !== undefined
  ) {
    const minimumResourceDeadline = store.minimumResourceDeadline;
    const minimumResourceDeadlineAfter = store.minimumResourceDeadlineAfter;
    const listDueResourceDeadlines = store.listDueResourceDeadlines;
    const advanceResourceScope = store.advanceResourceScope;
    const resourceDeadlineSubscription = yield* PubSub.subscribe(resourceDeadlineSignals);
    yield* Effect.gen(function* () {
      const firedDeadlineKeys = new Set<string>();
      const waitUntilResourceDeadline = Effect.fn(
        "ProviderAdmissionRuntime.waitUntilResourceDeadline",
      )(function* (deadline: string) {
        const delay = Math.max(
          0,
          DateTime.toEpochMillis(DateTime.makeUnsafe(deadline)) - (yield* Clock.currentTimeMillis),
        );
        if (delay === 0) return "deadline" as const;
        return yield* Effect.raceFirst(
          Effect.sleep(Duration.millis(delay)).pipe(Effect.as("deadline" as const)),
          PubSub.take(resourceDeadlineSubscription).pipe(Effect.as("changed" as const)),
        );
      });
      while (true) {
        const deadline = yield* minimumResourceDeadline;
        if (deadline === null) {
          firedDeadlineKeys.clear();
          yield* PubSub.take(resourceDeadlineSubscription);
          continue;
        }
        if ((yield* waitUntilResourceDeadline(deadline)) === "changed") {
          firedDeadlineKeys.clear();
          continue;
        }
        const nowValue = yield* DateTime.now;
        const now = DateTime.formatIso(nowValue);
        const due = yield* listDueResourceDeadlines(now);
        const dueKeys = new Set(
          due.map(
            (publication) =>
              `${publication.deadlineKind}:${publication.requestId}:${publication.deadlineAt}`,
          ),
        );
        for (const key of firedDeadlineKeys) {
          if (!dueKeys.has(key)) firedDeadlineKeys.delete(key);
        }
        const unfired = due.filter((publication) => {
          const key = `${publication.deadlineKind}:${publication.requestId}:${publication.deadlineAt}`;
          if (firedDeadlineKeys.has(key)) return false;
          firedDeadlineKeys.add(key);
          return true;
        });
        if (unfired.length === 0) {
          const futureDeadline = yield* minimumResourceDeadlineAfter(now);
          if (futureDeadline === null) {
            yield* PubSub.take(resourceDeadlineSubscription);
            firedDeadlineKeys.clear();
            continue;
          }
          if ((yield* waitUntilResourceDeadline(futureDeadline)) === "changed")
            firedDeadlineKeys.clear();
          continue;
        }
        const usageProviders = Array.from(
          new Set(
            unfired
              .filter((publication) => publication.deadlineKind === "usage")
              .map((publication) => publication.providerInstanceId),
          ),
        );
        yield* Effect.forEach(
          usageProviders,
          (providerInstanceId) =>
            inspect(ProviderInstanceId.make(providerInstanceId)).pipe(
              Effect.flatMap((evidence) => usageChanged(providerInstanceId, evidence)),
            ),
          { discard: true },
        );
        const scopes = Array.from(new Set(unfired.map((publication) => publication.accountScope)));
        yield* Effect.forEach(
          scopes,
          (accountScope) =>
            advanceResourceScope({
              accountScope,
              ownerId,
              now,
              leaseExpiresAt: DateTime.formatIso(DateTime.addDuration(nowValue, CLAIM_DURATION)),
            }).pipe(
              Effect.tap((result) => wakeAll(result.wakeups)),
              Effect.tap((result) =>
                result.capacityChanged ? PubSub.publish(resourceSignals, undefined) : Effect.void,
              ),
              Effect.asVoid,
            ),
          { discard: true },
        );
      }
    }).pipe((pump) => forkPump("resource-deadline-pump", pump));
  }

  const waitingProviders = Array.from(
    new Set((yield* store.listWaiting).map((publication) => publication.providerInstanceId)),
  );

  const deferResource: NonNullable<ProviderAdmissionRuntimeShape["deferResource"]> = Effect.fn(
    "ProviderAdmissionRuntime.deferResource",
  )(function* (permit) {
    const wakeups = yield* store.deferResource!({
      permit,
      deferredAt: DateTime.formatIso(yield* DateTime.now),
    });
    yield* wakeAll(wakeups);
    yield* PubSub.publish(resourceSignals, undefined);
    yield* PubSub.publish(resourceDeadlineSignals, undefined);
  });
  yield* Effect.forEach(waitingProviders, advanceProvider, { discard: true });

  return ProviderAdmissionRuntime.of({
    awaitFailure: Deferred.await(runtimeFailure),
    request,
    usageChanged,
    capacityReleased,
    resourceSettingsChanged,
    requestResource,
    acquireResource,
    enterResource,
    releaseResource,
    deferResource,
    cancelResource,
    configureResourceScope,
    listResourceActive: store.listResourceActive!,
    reconcileResource,
  });
});

export const ProviderAdmissionRuntimeLive = Layer.effect(ProviderAdmissionRuntime, make);
