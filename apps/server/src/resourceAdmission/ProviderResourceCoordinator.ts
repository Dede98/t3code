import type {
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ProviderSession,
  ResourceAdmissionWait,
  ResourceAdmissionWaitReason,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import {
  ProviderAdmissionRuntime,
  type ProviderAdmissionRuntimeShape,
} from "../agentControl/providerAdmission/Services/ProviderAdmissionRuntime.ts";
import type {
  ProviderAdmissionStage,
  ProviderResourceAdmissionLimits,
  ProviderResourceAdmissionPermit,
  ProviderResourceAdmissionRequest,
} from "../agentControl/providerAdmission/model.ts";
import { providerResourceAdmissionRequestId } from "../agentControl/providerAdmission/model.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { ResourceAdmission } from "./ResourceAdmission.ts";
import type {
  ResourceAdmissionDecision,
  ResourceAdmissionRequest,
  ResourceReservationAuthority,
} from "./model.ts";

export class ProviderResourceCoordinatorError extends Schema.TaggedError<ProviderResourceCoordinatorError>()(
  "ProviderResourceCoordinatorError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const isProviderResourceCoordinatorError = Schema.is(ProviderResourceCoordinatorError);

export interface CoordinatedProviderPermit {
  readonly provider: ProviderResourceAdmissionPermit;
  readonly host: ResourceReservationAuthority;
}

export interface ProviderResourceCoordinatorShape {
  readonly acquire: (input: {
    readonly idempotencyKey: string;
    readonly providerInstanceId: ProviderInstanceId;
    readonly continuationKey: string;
    readonly threadId: string;
    readonly requestedAt: string;
    readonly workloadClass: "interactive" | "background";
    readonly source: "manual" | "automatic";
    readonly stage?: ProviderAdmissionStage;
    readonly handoffId?: string;
    readonly onWait?: (wait: ResourceAdmissionWait) => Effect.Effect<void>;
  }) => Effect.Effect<CoordinatedProviderPermit, ProviderResourceCoordinatorError>;
  readonly enter: (
    permit: CoordinatedProviderPermit,
    providerTurnId?: string,
  ) => Effect.Effect<void, ProviderResourceCoordinatorError>;
  readonly release: (
    permit: CoordinatedProviderPermit,
  ) => Effect.Effect<void, ProviderResourceCoordinatorError>;
  readonly observeRuntimeEvent: (
    event: ProviderRuntimeEvent,
    currentSession?: ProviderSession,
  ) => Effect.Effect<void, ProviderResourceCoordinatorError>;
  readonly reconcile: (
    sessions: ReadonlyArray<ProviderSession>,
  ) => Effect.Effect<void, ProviderResourceCoordinatorError>;
}

export class ProviderResourceCoordinator extends Context.Service<
  ProviderResourceCoordinator,
  ProviderResourceCoordinatorShape
>()("t3/resourceAdmission/ProviderResourceCoordinator") {}

const providerLimits = (settings: {
  readonly providerMaxConcurrent: number;
  readonly interactiveReserve: number;
  readonly backgroundAgingSeconds: number;
  readonly backgroundGrantInterval: number;
}): ProviderResourceAdmissionLimits => ({
  maxConcurrent: settings.providerMaxConcurrent,
  interactiveReserve: settings.interactiveReserve,
  backgroundAgingMs: settings.backgroundAgingSeconds * 1_000,
  maxInteractiveBurst: Math.max(1, settings.backgroundGrantInterval - 1),
});

const providerWait = (
  reason: "provider-limit" | "provider-usage" | "provider-recovery" | "interactive-priority",
): ResourceAdmissionWait => ({
  reason: reason === "interactive-priority" ? "interactive-priority" : "provider-limit",
  ...(reason === "provider-usage" ? { detail: "Provider usage limits are not ready." } : {}),
  ...(reason === "provider-recovery"
    ? { detail: "A previous provider execution has an unknown outcome and still holds capacity." }
    : {}),
});

const hostWaitReason = (
  reason: Extract<ResourceAdmissionDecision, { readonly _tag: "Waiting" }>["reason"],
): ResourceAdmissionWaitReason => {
  switch (reason) {
    case "local-check-limit":
      return "local-capacity";
    case "memory-pressure":
      return "ram-pressure";
    case "gpu-capacity":
      return "gpu-pressure";
    case "recovery-capacity":
      return "provider-limit";
    default:
      return reason;
  }
};

const hostWait = (
  reason: Extract<ResourceAdmissionDecision, { readonly _tag: "Waiting" }>["reason"],
): ResourceAdmissionWait => ({
  reason: hostWaitReason(reason),
  ...(reason === "recovery-capacity"
    ? { detail: "A previous managed execution has an unknown outcome and still holds capacity." }
    : {}),
});

const notify = (
  callback: ((wait: ResourceAdmissionWait) => Effect.Effect<void>) | undefined,
  wait: ResourceAdmissionWait,
) => callback?.(wait).pipe(Effect.ignore) ?? Effect.void;

const requiredProviderMethod = <K extends keyof ProviderAdmissionRuntimeShape>(
  runtime: ProviderAdmissionRuntimeShape,
  key: K,
): NonNullable<ProviderAdmissionRuntimeShape[K]> => {
  const method = runtime[key];
  if (method === undefined) {
    throw new ProviderResourceCoordinatorError({
      operation: String(key),
      message: "Shared provider admission is unavailable.",
    });
  }
  return method as NonNullable<ProviderAdmissionRuntimeShape[K]>;
};

let lastCoordinatorOwnerFenceToken = 0;

function nextCoordinatorOwnerFenceToken(nowMs: number): number {
  // Wall-clock ordering makes a replacement process newer in the normal case;
  // the process-local increment also keeps multiple layers in one process fenced.
  const candidate = nowMs * 1_000 + (process.pid % 1_000);
  lastCoordinatorOwnerFenceToken = Math.max(candidate, lastCoordinatorOwnerFenceToken + 1);
  return lastCoordinatorOwnerFenceToken;
}

const make = Effect.gen(function* () {
  const providerAdmission = yield* ProviderAdmissionRuntime;
  const hostAdmission = yield* ResourceAdmission;
  const settingsService = yield* ServerSettingsService;
  const coordinatorScope = yield* Effect.scope;
  const pendingByThread = new Map<string, Map<string, CoordinatedProviderPermit>>();
  const activeByTurn = new Map<string, CoordinatedProviderPermit>();
  const earlyTerminals = new Set<string>();
  const hostOwnerFenceToken = nextCoordinatorOwnerFenceToken(yield* Clock.currentTimeMillis);
  const hostOwnerId = `provider-coordinator:${process.pid}:${hostOwnerFenceToken}`;

  const settingsChanges = yield* settingsService.subscribeChanges;
  yield* Stream.runForEach(settingsChanges, () =>
    Effect.all([providerAdmission.resourceSettingsChanged ?? Effect.void, hostAdmission.refresh], {
      concurrency: "unbounded",
      discard: true,
    }).pipe(Effect.ignore),
  ).pipe(Effect.forkIn(coordinatorScope, { startImmediately: true }));
  const turnKey = (instanceId: ProviderInstanceId, threadId: string, turnId: string) =>
    `${instanceId}:${threadId}:${turnId}`;
  const removePermit = (permit: CoordinatedProviderPermit) => {
    const pending = pendingByThread.get(permit.provider.threadId);
    pending?.delete(permit.provider.requestId);
    if (pending?.size === 0) pendingByThread.delete(permit.provider.threadId);
    for (const [key, active] of activeByTurn) {
      if (active.provider.requestId === permit.provider.requestId) activeByTurn.delete(key);
    }
  };
  const hostRequestFor = (
    request: Pick<
      ProviderResourceAdmissionRequest,
      "idempotencyKey" | "providerInstanceId" | "threadId" | "accountScope" | "workloadClass"
    >,
    providerRequestId: string,
  ): ResourceAdmissionRequest => ({
    requestId: `host:${providerRequestId}`,
    kind: "providerTurn",
    priority: request.workloadClass,
    accountScope: request.accountScope,
    ownerId: hostOwnerId,
    ownerFenceToken: hostOwnerFenceToken,
    executionKey: `provider:${request.threadId}`,
  });

  const acquire: ProviderResourceCoordinatorShape["acquire"] = Effect.fn(
    "ProviderResourceCoordinator.acquire",
  )(
    function* (input) {
      const settings = (yield* settingsService.getSettings).resourceAdmission;
      const accountScope =
        settings.providerAccountScopes[input.providerInstanceId] ?? input.continuationKey;
      const request: ProviderResourceAdmissionRequest = {
        idempotencyKey: input.idempotencyKey,
        providerInstanceId: input.providerInstanceId,
        threadId: input.threadId,
        accountScope,
        workloadClass: input.workloadClass,
        source: input.source,
        requestedAt: input.requestedAt,
        ...(input.stage === undefined ? {} : { stage: input.stage }),
        ...(input.handoffId === undefined ? {} : { handoffId: input.handoffId }),
      };
      const limits = providerLimits(settings);
      const requestResource = requiredProviderMethod(providerAdmission, "requestResource");
      const acquireResource = requiredProviderMethod(providerAdmission, "acquireResource");
      const deferResource = requiredProviderMethod(providerAdmission, "deferResource");
      const releaseResource = requiredProviderMethod(providerAdmission, "releaseResource");
      const cancelResource = requiredProviderMethod(providerAdmission, "cancelResource");
      const hostRequest = hostRequestFor(request, providerResourceAdmissionRequestId(request));
      let hostAuthority: ResourceReservationAuthority | undefined;
      let providerPermit: ProviderResourceAdmissionPermit | undefined;
      let retained = false;
      return yield* Effect.gen(function* () {
        const readLimits = settingsService.getSettings.pipe(
          Effect.map((current) => providerLimits(current.resourceAdmission)),
          Effect.orElseSucceed(() => limits),
        );
        while (true) {
          if (hostAuthority === undefined) {
            const firstHostDecision = yield* hostAdmission.request(hostRequest);
            if (firstHostDecision.result._tag === "Waiting")
              yield* notify(input.onWait, hostWait(firstHostDecision.result.reason));
            const hostDecision = yield* hostAdmission.acquire(hostRequest);
            if (hostDecision._tag !== "Admitted")
              return yield* new ProviderResourceCoordinatorError({
                operation: "acquire-host",
                message: hostDecision.message,
              });
            hostAuthority = hostDecision.authority;
          }

          const currentLimits = yield* readLimits;
          const firstProviderDecision = yield* requestResource(request, currentLimits);
          if (firstProviderDecision._tag === "Admitted") {
            providerPermit = firstProviderDecision.permit;
            retained = true;
            return { provider: providerPermit, host: hostAuthority };
          }
          if (firstProviderDecision._tag === "Cancelled")
            return yield* new ProviderResourceCoordinatorError({
              operation: "acquire-provider",
              message: "The provider capacity request was already cancelled.",
            });
          yield* notify(input.onWait, providerWait(firstProviderDecision.reason));

          // Never hold one scarce authority for the full wait on the other.
          // Paused requests keep their stable identity but are ineligible until
          // this coordinator explicitly retries them.
          const deferredHost = yield* hostAdmission.defer(hostAuthority).pipe(
            Effect.tap(() => Effect.sync(() => (hostAuthority = undefined))),
            Effect.uninterruptible,
          );
          if (!deferredHost.result)
            return yield* new ProviderResourceCoordinatorError({
              operation: "defer-host",
              message: "The provisional host reservation could not be deferred.",
            });
          providerPermit = yield* acquireResource(request, currentLimits, readLimits);

          const retryHost = yield* hostAdmission.request(hostRequest);
          if (retryHost.result._tag === "Admitted") {
            hostAuthority = retryHost.result.authority;
            retained = true;
            return { provider: providerPermit, host: hostAuthority };
          }
          if (retryHost.result._tag === "Rejected")
            return yield* new ProviderResourceCoordinatorError({
              operation: "acquire-host",
              message: retryHost.result.message,
            });
          yield* notify(input.onWait, hostWait(retryHost.result.reason));
          yield* deferResource(providerPermit).pipe(
            Effect.tap(() => Effect.sync(() => (providerPermit = undefined))),
            Effect.uninterruptible,
          );
          // The next loop waits for host capacity, then rechecks the provider
          // scope with fresh limits. No provisional slot is retained meanwhile.
        }
      }).pipe(
        Effect.ensuring(
          Effect.suspend(() => {
            if (retained) return Effect.void;
            return Effect.all(
              [
                providerPermit === undefined
                  ? cancelResource(request).pipe(Effect.ignore)
                  : releaseResource(providerPermit).pipe(Effect.ignore),
                hostAuthority === undefined
                  ? hostAdmission
                      .cancelWaiting({
                        reservationId: hostRequest.requestId,
                        ownerId: hostRequest.ownerId,
                        ownerFenceToken: hostRequest.ownerFenceToken,
                      })
                      .pipe(Effect.ignore)
                  : hostAdmission.release(hostAuthority).pipe(Effect.ignore),
              ],
              { concurrency: "unbounded", discard: true },
            );
          }),
        ),
      );
    },
    Effect.mapError((cause) =>
      isProviderResourceCoordinatorError(cause)
        ? cause
        : new ProviderResourceCoordinatorError({
            operation: "acquire",
            message: "Shared provider capacity could not be acquired.",
            cause,
          }),
    ),
  );

  const releasePermit = Effect.fn("ProviderResourceCoordinator.releasePermit")(function* (
    permit: CoordinatedProviderPermit,
  ) {
    const releaseResource = requiredProviderMethod(providerAdmission, "releaseResource");
    yield* Effect.all(
      [releaseResource(permit.provider), hostAdmission.release(permit.host).pipe(Effect.asVoid)],
      { concurrency: "unbounded", discard: true },
    );
    removePermit(permit);
  });

  const enter: ProviderResourceCoordinatorShape["enter"] = Effect.fn(
    "ProviderResourceCoordinator.enter",
  )(
    function* (permit, providerTurnId) {
      const enterResource = requiredProviderMethod(providerAdmission, "enterResource");
      // These authorities cannot share a transaction. Mark the host side
      // first so a crash can only fail closed by retaining capacity; startup
      // reconciliation adopts a host reservation for every entered provider
      // row to close the inverse half-written state from older versions.
      yield* hostAdmission
        .observeActivity(permit.host, "active")
        .pipe(Effect.asVoid, Effect.andThen(enterResource(permit.provider, providerTurnId)))
        .pipe(
          Effect.catchCause((cause) =>
            (providerTurnId === undefined
              ? releasePermit(permit).pipe(Effect.ignore)
              : Effect.void
            ).pipe(Effect.andThen(Effect.failCause(cause))),
          ),
        );
      if (providerTurnId === undefined) {
        const pending = pendingByThread.get(permit.provider.threadId) ?? new Map();
        pending.set(permit.provider.requestId, permit);
        pendingByThread.set(permit.provider.threadId, pending);
        return;
      }
      const key = turnKey(
        permit.provider.providerInstanceId,
        permit.provider.threadId,
        providerTurnId,
      );
      pendingByThread.get(permit.provider.threadId)?.delete(permit.provider.requestId);
      activeByTurn.set(key, permit);
      if (earlyTerminals.delete(key)) {
        yield* releasePermit(permit);
      }
    },
    Effect.mapError(
      (cause) =>
        new ProviderResourceCoordinatorError({
          operation: "enter",
          message: "Shared provider capacity rejected the start fence.",
          cause,
        }),
    ),
  );

  const release: ProviderResourceCoordinatorShape["release"] = Effect.fn(
    "ProviderResourceCoordinator.release",
  )(
    function* (permit) {
      yield* releasePermit(permit);
    },
    Effect.mapError(
      (cause) =>
        new ProviderResourceCoordinatorError({
          operation: "release",
          message: "Shared provider capacity could not be released.",
          cause,
        }),
    ),
  );

  const adopt = Effect.fn("ProviderResourceCoordinator.adopt")(function* (
    row: import("../agentControl/providerAdmission/model.ts").ProviderResourceAdmissionActive,
  ) {
    const reconcileResource = requiredProviderMethod(providerAdmission, "reconcileResource");
    const providerPermit = yield* reconcileResource(row.requestId, "active");
    if (providerPermit === null) {
      return yield* new ProviderResourceCoordinatorError({
        operation: "reconcile-provider",
        message: `Provider reservation '${row.requestId}' could not be adopted.`,
      });
    }
    const host = yield* hostAdmission.adoptActive(
      hostRequestFor(providerPermit, providerPermit.requestId),
    );
    const permit = { provider: providerPermit, host } satisfies CoordinatedProviderPermit;
    const pending = pendingByThread.get(providerPermit.threadId) ?? new Map();
    pending.set(providerPermit.requestId, permit);
    pendingByThread.set(providerPermit.threadId, pending);
    if (row.providerTurnId !== null) {
      pending.delete(providerPermit.requestId);
      activeByTurn.set(
        turnKey(providerPermit.providerInstanceId, providerPermit.threadId, row.providerTurnId),
        permit,
      );
    }
    return permit;
  });

  const observeRuntimeEvent: ProviderResourceCoordinatorShape["observeRuntimeEvent"] = Effect.fn(
    "ProviderResourceCoordinator.observeRuntimeEvent",
  )(
    function* (event, currentSession) {
      if (
        event.type === "turn.started" &&
        event.providerInstanceId !== undefined &&
        event.turnId !== undefined
      ) {
        // Runtime event receipt times are not a session generation. Only the
        // adapter's current session snapshot can prove that this start belongs
        // to the present in-process request rather than an older turn.
        if (
          currentSession === undefined ||
          currentSession.threadId !== event.threadId ||
          currentSession.providerInstanceId !== event.providerInstanceId ||
          currentSession.activeTurnId !== event.turnId ||
          currentSession.status === "closed"
        ) {
          return;
        }
        const pending = [...(pendingByThread.get(event.threadId)?.values() ?? [])].find(
          (permit) =>
            permit.provider.providerInstanceId === event.providerInstanceId &&
            event.createdAt >= permit.provider.requestedAt,
        );
        if (pending !== undefined) {
          yield* enter(pending, String(event.turnId));
        }
        return;
      }
      if (
        (event.type === "turn.completed" || event.type === "turn.aborted") &&
        event.providerInstanceId !== undefined &&
        event.turnId !== undefined
      ) {
        const key = turnKey(event.providerInstanceId, event.threadId, event.turnId);
        const permit = activeByTurn.get(key);
        if (permit === undefined) {
          const listResourceActive = requiredProviderMethod(
            providerAdmission,
            "listResourceActive",
          );
          const rows = yield* listResourceActive;
          const persisted = rows.find(
            (row) =>
              row.providerInstanceId === event.providerInstanceId &&
              row.threadId === event.threadId &&
              row.providerTurnId === event.turnId,
          );
          if (persisted !== undefined) {
            yield* release(yield* adopt(persisted));
            return;
          }
          // Completion alone cannot identify an unbound reservation: event
          // timestamps are receipt times, so a delayed terminal from an old
          // turn could otherwise free a newer turn. Keep it as an early
          // terminal; a later fenced turn.started binding will consume it.
          earlyTerminals.add(key);
          return;
        }
        yield* release(permit);
        return;
      }
      // A session exit has no provider-session generation, so it cannot prove
      // ownership of a newer reservation on the same thread and instance.
      // Precise turn terminal events release capacity. An exit without one is
      // deliberately reconciled as unknown on restart rather than allowing a
      // stale owner to free a successor's reservation.
    },
    Effect.mapError((cause) =>
      isProviderResourceCoordinatorError(cause)
        ? cause
        : new ProviderResourceCoordinatorError({
            operation: "observe-runtime-event",
            message: "Provider resource completion could not be reconciled.",
            cause,
          }),
    ),
  );

  const reconcile: ProviderResourceCoordinatorShape["reconcile"] = Effect.fn(
    "ProviderResourceCoordinator.reconcile",
  )(
    function* (sessions) {
      const listResourceActive = requiredProviderMethod(providerAdmission, "listResourceActive");
      const reconcileResource = requiredProviderMethod(providerAdmission, "reconcileResource");
      const requestResource = requiredProviderMethod(providerAdmission, "requestResource");
      const enterResource = requiredProviderMethod(providerAdmission, "enterResource");
      const rows = yield* listResourceActive;
      yield* Effect.forEach(
        rows,
        (row) => {
          const session = sessions.find(
            (candidate) =>
              String(candidate.threadId) === row.threadId &&
              candidate.providerInstanceId === row.providerInstanceId,
          );
          const active =
            row.status === "entered" &&
            session !== undefined &&
            session.status !== "closed" &&
            session.activeTurnId !== undefined &&
            (row.providerTurnId === null || String(session.activeTurnId) === row.providerTurnId);
          if (active)
            return adopt(row).pipe(
              Effect.flatMap((permit) =>
                row.providerTurnId === null
                  ? enter(permit, String(session.activeTurnId!))
                  : Effect.void,
              ),
            );
          if (row.status === "waiting") {
            // Automatic deliveries are durable and can re-enter with their
            // stable delivery id. Manual request fibers are not replayable, so
            // retire those and let startup surface the interrupted start.
            return row.source === "automatic" ||
              row.idempotencyKey.startsWith("startup-continuation:")
              ? reconcileResource(row.requestId, "unknown").pipe(Effect.asVoid)
              : reconcileResource(row.requestId, "inactive").pipe(Effect.asVoid);
          }
          // Entered means the external provider invocation may already have
          // started. A single missing session snapshot is not inactivity.
          return row.status === "entered"
            ? Effect.all(
                [
                  reconcileResource(row.requestId, "unknown").pipe(Effect.asVoid),
                  hostAdmission.adoptActive(hostRequestFor(row, row.requestId)).pipe(Effect.asVoid),
                ],
                { concurrency: "unbounded", discard: true },
              )
            : adopt(row).pipe(Effect.flatMap(release));
        },
        { concurrency: 1, discard: true },
      );
      const settings = (yield* settingsService.getSettings).resourceAdmission;
      const missingActiveSessions = sessions.filter(
        (session) =>
          session.providerInstanceId !== undefined &&
          session.activeTurnId !== undefined &&
          session.status !== "closed" &&
          !rows.some(
            (row) =>
              row.status === "entered" &&
              row.providerInstanceId === session.providerInstanceId &&
              row.threadId === String(session.threadId) &&
              (row.providerTurnId === null || row.providerTurnId === String(session.activeTurnId)),
          ),
      );
      yield* Effect.forEach(
        missingActiveSessions,
        (session) =>
          Effect.gen(function* () {
            const instanceId = session.providerInstanceId!;
            const turnId = String(session.activeTurnId!);
            const accountScope =
              settings.providerAccountScopes[instanceId] ?? String(session.provider);
            const request: ProviderResourceAdmissionRequest = {
              idempotencyKey: `recovery:${instanceId}:${session.threadId}:${turnId}`,
              providerInstanceId: instanceId,
              threadId: String(session.threadId),
              accountScope,
              workloadClass: "interactive",
              source: "manual",
              requestedAt: session.createdAt,
            };
            const decision = yield* requestResource(request, providerLimits(settings));
            const providerPermit =
              decision._tag === "Admitted"
                ? decision.permit
                : decision._tag === "Waiting"
                  ? yield* reconcileResource(decision.requestId, "active")
                  : null;
            if (providerPermit === null) {
              return yield* new ProviderResourceCoordinatorError({
                operation: "reconcile-upgrade",
                message: `Running provider turn '${turnId}' could not be admitted during upgrade.`,
              });
            }
            yield* enterResource(providerPermit, turnId);
            const host = yield* hostAdmission.adoptActive(
              hostRequestFor(request, providerPermit.requestId),
            );
            const permit = { provider: providerPermit, host } satisfies CoordinatedProviderPermit;
            activeByTurn.set(turnKey(instanceId, String(session.threadId), turnId), permit);
          }),
        { concurrency: 1, discard: true },
      );
    },
    Effect.mapError((cause) =>
      isProviderResourceCoordinatorError(cause)
        ? cause
        : new ProviderResourceCoordinatorError({
            operation: "reconcile",
            message: "Provider resource reconciliation failed.",
            cause,
          }),
    ),
  );

  return ProviderResourceCoordinator.of({
    acquire,
    enter,
    release,
    observeRuntimeEvent,
    reconcile,
  });
});

export const layer = Layer.effect(ProviderResourceCoordinator, make);
