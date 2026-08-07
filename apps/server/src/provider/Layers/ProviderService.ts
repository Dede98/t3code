/**
 * ProviderServiceLive - Cross-provider orchestration layer.
 *
 * Routes validated transport/API calls to provider adapters through
 * `ProviderAdapterRegistry` and `ProviderSessionDirectory`, and exposes a
 * unified provider event stream for subscribers.
 *
 * It does not implement provider protocol details (adapter concern).
 *
 * @module ProviderServiceLive
 */
import {
  ModelSelection,
  NonNegativeInt,
  ThreadId,
  ProviderInterruptTurnInput,
  ProviderRespondToRequestInput,
  ProviderRespondToUserInputInput,
  ProviderSendTurnInput,
  ProviderSessionStartInput,
  ProviderStopSessionInput,
  type ProviderInstanceId,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import * as Stream from "effect/Stream";

import {
  increment,
  providerMetricAttributes,
  providerRuntimeEventsTotal,
  providerSessionBindingsQuarantinedTotal,
  providerSessionsTotal,
  providerTurnDuration,
  providerTurnsTotal,
  providerTurnMetricAttributes,
  withMetrics,
} from "../../observability/Metrics.ts";
import {
  type ProviderAdapterError,
  ProviderAdapterRequestError,
  isProviderSessionBindingDecodeError,
  ProviderValidationError,
} from "../Errors.ts";
import type {
  ProviderAdapterShape,
  ProviderSessionAttestation,
  ProviderSessionWithAttestation,
  ProviderTurnAttestation,
} from "../Services/ProviderAdapter.ts";
import {
  attestProviderSessionNativeConfiguration,
  canonicalProviderModelSelectionEvidence,
} from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import { ProviderRegistryRebuildBarrier } from "../Services/ProviderRegistryRebuildBarrier.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { ProviderThreadOperationLock } from "../Services/ProviderThreadOperationLock.ts";
import { type EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { ProviderRegistryRebuildBarrierLive } from "./ProviderRegistryRebuildBarrier.ts";
import {
  makeProviderThreadOperationLockLive,
  ProviderThreadOperationLockLive,
  type ProviderThreadOperationLockObserver,
} from "./ProviderThreadOperationLock.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import * as McpSessionRegistry from "../../mcp/McpSessionRegistry.ts";
const isModelSelection = Schema.is(ModelSelection);

/**
 * Hook for tests that want to override the canonical event logger pulled
 * from `ProviderEventLoggers`. Production wiring leaves this undefined and
 * reads the logger off the tag.
 */
export interface ProviderServiceLiveOptions {
  readonly canonicalEventLogger?: EventNdjsonLogger;
  /** Internal lock identity observer; production leaves this undefined. */
  readonly threadOperationLockObserver?: ProviderThreadOperationLockObserver;
}

type ProviderServiceMethod<Name extends keyof ProviderService.ProviderService["Service"]> =
  ProviderService.ProviderService["Service"][Name];
type SendTurnPreInvokeBoundary = Parameters<
  NonNullable<ProviderService.ProviderService["Service"]["sendTurnAtPreInvokeBoundary"]>
>[1];

type ProviderRuntimeEventWithInstance = ProviderRuntimeEvent & {
  readonly providerInstanceId: ProviderInstanceId;
};

type ProviderSendRoute = {
  readonly adapter: ProviderAdapterShape<ProviderAdapterError>;
  readonly instanceId: ProviderInstanceId;
  readonly threadId: ThreadId;
};

type ProviderSessionWithInstance = ProviderSessionWithAttestation & {
  readonly providerInstanceId: ProviderInstanceId;
};

const ProviderRollbackConversationInput = Schema.Struct({
  threadId: ThreadId,
  numTurns: NonNegativeInt,
});

function toValidationError(
  operation: string,
  issue: string,
  cause?: unknown,
): ProviderValidationError {
  return new ProviderValidationError({
    operation,
    issue,
    ...(cause !== undefined ? { cause } : {}),
  });
}

function requireProviderInstanceId(
  operation: string,
  payload: {
    readonly providerInstanceId?: ProviderInstanceId | undefined;
    readonly provider?: ProviderDriverKind | undefined;
  },
): Effect.Effect<ProviderInstanceId, ProviderValidationError> {
  if (payload.providerInstanceId !== undefined) {
    return Effect.succeed(payload.providerInstanceId);
  }
  return Effect.fail(
    toValidationError(
      operation,
      payload.provider === undefined
        ? "Provider instance id is required."
        : `Provider instance id is required for provider '${payload.provider}'.`,
    ),
  );
}

const decodeInputOrValidationError = <S extends Schema.Top>(input: {
  readonly operation: string;
  readonly schema: S;
  readonly payload: unknown;
}) => {
  const decodeProviderRequestInput = Schema.decodeUnknownEffect(input.schema);
  return decodeProviderRequestInput(input.payload).pipe(
    Effect.mapError(
      (schemaError) =>
        new ProviderValidationError({
          operation: input.operation,
          issue: SchemaIssue.makeFormatterDefault()(schemaError.issue),
          cause: schemaError,
        }),
    ),
  );
};

function toRuntimeStatus(session: ProviderSession): "starting" | "running" | "stopped" | "error" {
  switch (session.status) {
    case "connecting":
      return "starting";
    case "error":
      return "error";
    case "closed":
      return "stopped";
    case "ready":
    case "running":
    default:
      return "running";
  }
}

function toRuntimePayloadFromSession(
  session: ProviderSession,
  extra?: {
    readonly modelSelection?: unknown;
    readonly lastRuntimeEvent?: string;
    readonly lastRuntimeEventAt?: string;
  },
): Record<string, unknown> {
  return {
    cwd: session.cwd ?? null,
    model: session.model ?? null,
    sessionCreatedAt: session.createdAt,
    activeTurnId: session.activeTurnId ?? null,
    lastError: session.lastError ?? null,
    ...(extra?.modelSelection !== undefined ? { modelSelection: extra.modelSelection } : {}),
    ...(extra?.lastRuntimeEvent !== undefined ? { lastRuntimeEvent: extra.lastRuntimeEvent } : {}),
    ...(extra?.lastRuntimeEventAt !== undefined
      ? { lastRuntimeEventAt: extra.lastRuntimeEventAt }
      : {}),
  };
}

function readPersistedModelSelection(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): ModelSelection | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const raw = "modelSelection" in runtimePayload ? runtimePayload.modelSelection : undefined;
  return isModelSelection(raw) ? raw : undefined;
}

function readPersistedCwd(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const rawCwd = "cwd" in runtimePayload ? runtimePayload.cwd : undefined;
  if (typeof rawCwd !== "string") return undefined;
  const trimmed = rawCwd.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function readPersistedSessionCreatedAt(
  runtimePayload: ProviderSessionDirectory.ProviderRuntimeBinding["runtimePayload"],
): string | undefined {
  if (!runtimePayload || typeof runtimePayload !== "object" || Array.isArray(runtimePayload)) {
    return undefined;
  }
  const value = "sessionCreatedAt" in runtimePayload ? runtimePayload.sessionCreatedAt : undefined;
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

export const correlateRuntimeEventWithInstance = (
  source: {
    readonly instanceId: ProviderInstanceId;
    readonly provider: ProviderDriverKind;
  },
  event: ProviderRuntimeEvent,
): ProviderRuntimeEventWithInstance => {
  if (event.provider !== source.provider) {
    throw new Error(
      `ProviderService.streamEvents: provider instance '${source.instanceId}' is backed by driver '${source.provider}' but emitted driver '${event.provider}'.`,
    );
  }
  if (event.providerInstanceId !== source.instanceId) {
    throw new Error(
      event.providerInstanceId === undefined
        ? `ProviderService.streamEvents: provider instance '${source.instanceId}' emitted an event without a provider instance id.`
        : `ProviderService.streamEvents: provider instance '${source.instanceId}' emitted event for instance '${event.providerInstanceId}'.`,
    );
  }
  return { ...event, providerInstanceId: event.providerInstanceId };
};

const makeProviderService = Effect.fn("makeProviderService")(function* (
  options?: ProviderServiceLiveOptions,
) {
  const analytics = yield* Effect.service(AnalyticsService.AnalyticsService);
  const eventLoggers = yield* ProviderEventLoggers.ProviderEventLoggers;
  // Options-provided logger wins (test overrides); otherwise we take whatever
  // the `ProviderEventLoggers` tag exposes — `undefined` means "no canonical
  // log writer is attached", which downstream code already handles as a
  // no-op.
  const canonicalEventLogger = options?.canonicalEventLogger ?? eventLoggers.canonical;

  const registry = yield* ProviderAdapterRegistry.ProviderAdapterRegistry;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const rebuildBarrier = yield* ProviderRegistryRebuildBarrier;
  const threadOperationLock = yield* ProviderThreadOperationLock;
  const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const runtimeEventPublishingReady = yield* Deferred.make<void>();
  const sessionAttestations = new Map<ThreadId, ProviderSessionAttestation>();
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const recordSessionAttestation = Effect.fn("ProviderService.recordSessionAttestation")(function* (
    session: ProviderSessionWithAttestation,
  ) {
    const attestation = session.initialPlanningAttestation;
    if (attestation === undefined) {
      sessionAttestations.delete(session.threadId);
      return;
    }
    if (
      session.providerInstanceId === undefined ||
      session.cwd === undefined ||
      attestation.threadId !== session.threadId ||
      attestation.providerInstanceId !== session.providerInstanceId ||
      attestation.runtimeMode !== session.runtimeMode ||
      attestation.cwd !== session.cwd ||
      attestation.sessionCreatedAt !== session.createdAt ||
      !Equal.equals(attestation.resumeCursor, session.resumeCursor ?? null) ||
      (attestation.effectiveModelSelection !== null &&
        (attestation.effectiveModelSelection.instanceId !== session.providerInstanceId ||
          attestation.effectiveModelSelection.model !== session.model))
    ) {
      sessionAttestations.delete(session.threadId);
      return yield* toValidationError(
        "ProviderService.startSession",
        `Adapter '${session.provider}' returned inconsistent session model attestation.`,
      );
    }
    const canonicalEvidence = canonicalProviderModelSelectionEvidence(
      attestation.effectiveModelSelection,
    );
    if (
      !Equal.equals(
        canonicalEvidence.effectiveModelSelection,
        attestation.effectiveModelSelection,
      ) ||
      canonicalEvidence.modelSelectionJson !== attestation.modelSelectionJson ||
      canonicalEvidence.modelSelectionFingerprint !== attestation.modelSelectionFingerprint
    ) {
      sessionAttestations.delete(session.threadId);
      return yield* toValidationError(
        "ProviderService.startSession",
        `Adapter '${session.provider}' returned noncanonical session model attestation.`,
      );
    }
    sessionAttestations.set(session.threadId, attestation);
  });
  const prepareMcpSession = (threadId: ThreadId, providerInstanceId: ProviderInstanceId) =>
    McpSessionRegistry.issueActiveMcpCredential({ threadId, providerInstanceId }).pipe(
      Effect.tap((credential) =>
        credential
          ? Effect.sync(() => McpProviderSession.setMcpProviderSession(credential.config))
          : Effect.void,
      ),
    );
  const clearMcpSession = (threadId: ThreadId) =>
    McpSessionRegistry.revokeActiveMcpThread(threadId).pipe(
      Effect.tap(() => Effect.sync(() => McpProviderSession.clearMcpProviderSession(threadId))),
    );

  const publishRuntimeEvent = (event: ProviderRuntimeEvent): Effect.Effect<void> =>
    Effect.succeed(event).pipe(
      Effect.tap((canonicalEvent) =>
        canonicalEventLogger
          ? canonicalEventLogger.write(canonicalEvent, canonicalEvent.threadId)
          : Effect.void,
      ),
      Effect.flatMap((canonicalEvent) => PubSub.publish(runtimeEventPubSub, canonicalEvent)),
      Effect.asVoid,
    );

  const upsertSessionBinding = (
    session: ProviderSession,
    threadId: ThreadId,
    extra?: {
      readonly modelSelection?: unknown;
      readonly lastRuntimeEvent?: string;
      readonly lastRuntimeEventAt?: string;
    },
  ) =>
    Effect.gen(function* () {
      const providerInstanceId = yield* requireProviderInstanceId(
        "ProviderService.upsertSessionBinding",
        session,
      );
      yield* directory.upsert({
        threadId,
        provider: session.provider,
        providerInstanceId,
        runtimeMode: session.runtimeMode,
        status: toRuntimeStatus(session),
        ...(session.resumeCursor !== undefined ? { resumeCursor: session.resumeCursor } : {}),
        runtimePayload: toRuntimePayloadFromSession(session, extra),
      });
    });

  const processRuntimeEvent = (
    source: {
      readonly instanceId: ProviderInstanceId;
      readonly provider: ProviderDriverKind;
    },
    event: ProviderRuntimeEvent,
  ): Effect.Effect<void> =>
    Effect.sync(() => correlateRuntimeEventWithInstance(source, event)).pipe(
      Effect.flatMap((canonicalEvent) =>
        Deferred.await(runtimeEventPublishingReady).pipe(
          Effect.andThen(
            increment(providerRuntimeEventsTotal, {
              provider: canonicalEvent.provider,
              eventType: canonicalEvent.type,
            }),
          ),
          Effect.andThen(publishRuntimeEvent(canonicalEvent)),
        ),
      ),
    );

  // Routing remains available for ordinary provider operations as soon as the
  // layer is built. Adapter event subscriptions themselves are attempt-owned
  // and are therefore tracked separately inside `startRuntimeEventSources`.
  const availableAdapters = yield* Ref.make(
    new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>(),
  );

  const getAdapterEntries = Ref.get(availableAdapters).pipe(
    Effect.map((map) => Array.from(map.entries())),
  );

  const loadAvailableAdapters = Effect.gen(function* () {
    const currentIds = yield* registry.listInstances();
    const next = new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>();
    for (const id of currentIds) {
      const adapterOption = yield* registry
        .getByInstance(id)
        .pipe(Effect.tapError(Effect.logWarning), Effect.option);
      if (Option.isNone(adapterOption)) continue;
      const adapter = adapterOption.value;
      next.set(id, adapter);
    }
    yield* Ref.set(availableAdapters, next);
    return next;
  });

  yield* loadAvailableAdapters;

  const startRuntimeEventSources = Effect.gen(function* () {
    const subscribedForAttempt = yield* Ref.make(
      new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>(),
    );
    const instanceChanges = yield* registry.subscribeChanges;

    const reconcileInstanceSubscriptions = Effect.gen(function* () {
      const previous = yield* Ref.get(subscribedForAttempt);
      const next = yield* loadAvailableAdapters;
      for (const [id, adapter] of next) {
        if (previous.get(id) === adapter) continue;
        yield* Stream.runForEach(adapter.streamEvents, (event) =>
          processRuntimeEvent(
            {
              instanceId: id,
              provider: adapter.provider,
            },
            event,
          ),
        ).pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid);
      }
      yield* Ref.set(subscribedForAttempt, next);
    });

    yield* reconcileInstanceSubscriptions;
    yield* Stream.runForEach(
      Stream.fromSubscription(instanceChanges),
      () => reconcileInstanceSubscriptions,
    ).pipe(Effect.forkScoped({ startImmediately: true }), Effect.asVoid);
  });

  const recoverSessionForThread = Effect.fn("recoverSessionForThread")(function* (input: {
    readonly binding: ProviderSessionDirectory.ProviderRuntimeBinding;
    readonly operation: string;
  }) {
    const bindingInstanceId = input.binding.providerInstanceId;
    yield* Effect.annotateCurrentSpan({
      "provider.operation": "recover-session",
      "provider.kind": input.binding.provider,
      "provider.instance_id": bindingInstanceId,
      "provider.thread_id": input.binding.threadId,
    });
    return yield* Effect.gen(function* () {
      const adapter = yield* registry.getByInstance(bindingInstanceId);
      const hasResumeCursor =
        input.binding.resumeCursor !== null && input.binding.resumeCursor !== undefined;
      const hasActiveSession = yield* adapter.hasSession(input.binding.threadId);
      if (hasActiveSession) {
        const activeSessions = yield* adapter.listSessions();
        const existing = activeSessions.find(
          (session) => session.threadId === input.binding.threadId,
        );
        if (existing) {
          if (existing.providerInstanceId !== bindingInstanceId) {
            return yield* toValidationError(
              input.operation,
              `Provider session instance mismatch while recovering thread '${input.binding.threadId}'. Expected '${bindingInstanceId}', received '${existing.providerInstanceId}'.`,
            );
          }
          yield* recordSessionAttestation(existing as ProviderSessionWithAttestation);
          yield* upsertSessionBinding(existing, input.binding.threadId);
          yield* analytics.record("provider.session.recovered", {
            provider: existing.provider,
            strategy: "adopt-existing",
            hasResumeCursor: existing.resumeCursor !== undefined,
          });
          return { adapter, session: existing } as const;
        }
      }

      if (!hasResumeCursor) {
        return yield* toValidationError(
          input.operation,
          `Cannot recover thread '${input.binding.threadId}' because no provider resume state is persisted.`,
        );
      }

      const persistedCwd = readPersistedCwd(input.binding.runtimePayload);
      const persistedModelSelection = readPersistedModelSelection(input.binding.runtimePayload);
      const persistedSessionCreatedAt = readPersistedSessionCreatedAt(input.binding.runtimePayload);

      yield* prepareMcpSession(input.binding.threadId, bindingInstanceId);
      const resumedNative = yield* adapter
        .startSession({
          threadId: input.binding.threadId,
          provider: input.binding.provider,
          providerInstanceId: bindingInstanceId,
          ...(persistedCwd ? { cwd: persistedCwd } : {}),
          ...(persistedModelSelection ? { modelSelection: persistedModelSelection } : {}),
          ...(hasResumeCursor ? { resumeCursor: input.binding.resumeCursor } : {}),
          runtimeMode: input.binding.runtimeMode ?? "full-access",
        })
        .pipe(Effect.onError(() => clearMcpSession(input.binding.threadId)));
      const resumed =
        persistedSessionCreatedAt !== undefined &&
        resumedNative.initialPlanningAttestation !== undefined
          ? attestProviderSessionNativeConfiguration(
              { ...resumedNative, createdAt: persistedSessionCreatedAt },
              resumedNative.initialPlanningAttestation.effectiveModelSelection,
            )
          : resumedNative;
      if (resumed.provider !== adapter.provider) {
        yield* clearMcpSession(input.binding.threadId);
        return yield* toValidationError(
          input.operation,
          `Adapter/provider mismatch while recovering thread '${input.binding.threadId}'. Expected '${adapter.provider}', received '${resumed.provider}'.`,
        );
      }

      if (resumed.providerInstanceId !== bindingInstanceId) {
        yield* clearMcpSession(input.binding.threadId);
        return yield* toValidationError(
          input.operation,
          `Provider session instance mismatch while recovering thread '${input.binding.threadId}'. Expected '${bindingInstanceId}', received '${resumed.providerInstanceId}'.`,
        );
      }

      yield* recordSessionAttestation(resumed);
      yield* upsertSessionBinding(resumed, input.binding.threadId);
      yield* analytics.record("provider.session.recovered", {
        provider: resumed.provider,
        strategy: "resume-thread",
        hasResumeCursor: resumed.resumeCursor !== undefined,
      });
      return { adapter, session: resumed } as const;
    }).pipe(
      withMetrics({
        counter: providerSessionsTotal,
        attributes: providerMetricAttributes(input.binding.provider, {
          operation: "recover",
        }),
      }),
    );
  });

  const resolveRoutableSession = Effect.fn("resolveRoutableSession")(function* (input: {
    readonly threadId: ThreadId;
    readonly operation: string;
    readonly allowRecovery: boolean;
  }) {
    const bindingOption = yield* directory.getBinding(input.threadId);
    const binding = Option.getOrUndefined(bindingOption);
    if (!binding) {
      return yield* toValidationError(
        input.operation,
        `Cannot route thread '${input.threadId}' because no persisted provider binding exists.`,
      );
    }
    const instanceId = binding.providerInstanceId;
    const adapter = yield* registry.getByInstance(instanceId);

    const hasRequestedSession = yield* adapter.hasSession(input.threadId);
    if (hasRequestedSession) {
      return {
        adapter,
        instanceId,
        threadId: input.threadId,
        isActive: true,
      } as const;
    }

    if (!input.allowRecovery) {
      return {
        adapter,
        instanceId,
        threadId: input.threadId,
        isActive: false,
      } as const;
    }

    const recovered = yield* recoverSessionForThread({
      binding,
      operation: input.operation,
    });
    return {
      adapter: recovered.adapter,
      instanceId,
      threadId: input.threadId,
      isActive: true,
    } as const;
  });

  const resolveProviderSendRoute = Effect.fn("resolveProviderSendRoute")(function* (input: {
    readonly threadId: ThreadId;
    readonly operation: string;
  }) {
    const binding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
    if (binding === undefined) {
      return yield* toValidationError(
        input.operation,
        `Cannot route thread '${input.threadId}' because no persisted provider binding exists.`,
      );
    }
    return {
      adapter: yield* registry.getByInstance(binding.providerInstanceId),
      instanceId: binding.providerInstanceId,
      threadId: input.threadId,
    };
  });

  const stopStaleSessionsForThread = Effect.fn("stopStaleSessionsForThread")(function* (input: {
    readonly threadId: ThreadId;
    readonly currentInstanceId: ProviderInstanceId;
  }) {
    const currentAdapters = yield* getAdapterEntries;
    yield* Effect.forEach(
      currentAdapters,
      ([instanceId, adapter]) =>
        instanceId === input.currentInstanceId
          ? Effect.void
          : Effect.gen(function* () {
              const hasSession = yield* adapter.hasSession(input.threadId);
              if (!hasSession) {
                return;
              }

              yield* adapter.stopSession(input.threadId).pipe(
                Effect.tap(() =>
                  analytics.record("provider.session.stopped", {
                    provider: adapter.provider,
                  }),
                ),
                Effect.catchCause((cause) =>
                  Effect.logWarning("provider.session.stop-stale-failed", {
                    threadId: input.threadId,
                    provider: adapter.provider,
                    cause,
                  }),
                ),
              );
            }),
      { discard: true },
    );
  });

  const startSessionUnlocked: ProviderServiceMethod<"startSession"> = Effect.fn("startSession")(
    function* (threadId, rawInput) {
      const parsed = yield* decodeInputOrValidationError({
        operation: "ProviderService.startSession",
        schema: ProviderSessionStartInput,
        payload: rawInput,
      });

      const resolvedInstanceId = yield* requireProviderInstanceId(
        "ProviderService.startSession",
        parsed,
      );
      let metricProvider = parsed.provider ?? String(resolvedInstanceId);
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "start-session",
        "provider.instance_id": resolvedInstanceId,
        "provider.thread_id": threadId,
        "provider.runtime_mode": parsed.runtimeMode,
      });
      return yield* Effect.gen(function* () {
        const instanceInfo = yield* registry.getInstanceInfo(resolvedInstanceId);
        const resolvedProvider = instanceInfo.driverKind;
        metricProvider = resolvedProvider;
        if (parsed.provider !== undefined && parsed.provider !== resolvedProvider) {
          return yield* toValidationError(
            "ProviderService.startSession",
            `Provider instance '${resolvedInstanceId}' belongs to driver '${resolvedProvider}', not '${parsed.provider}'.`,
          );
        }
        const input = {
          ...parsed,
          threadId,
          provider: resolvedProvider,
        };
        if (!instanceInfo.enabled) {
          return yield* toValidationError(
            "ProviderService.startSession",
            `Provider instance '${resolvedInstanceId}' is disabled in T3 Code settings.`,
          );
        }
        const persistedBinding = Option.getOrUndefined(
          yield* directory.getBinding(threadId).pipe(
            Effect.catchIf(isProviderSessionBindingDecodeError, (error) =>
              Effect.logWarning("provider.session.binding.quarantined", {
                threadId,
                persistedProvider: parsed.provider,
                operation: "start-session",
                reason: error.reason,
                detail: error.detail,
              }).pipe(
                Effect.andThen(
                  increment(providerSessionBindingsQuarantinedTotal, {
                    operation: "start-session",
                    reason: error.reason ?? "decode-failed",
                  }),
                ),
                Effect.as(Option.none()),
              ),
            ),
          ),
        );
        const persistedBindingInstanceId = persistedBinding?.providerInstanceId;
        const persistedSourceInfo =
          persistedBindingInstanceId !== undefined &&
          persistedBindingInstanceId !== resolvedInstanceId &&
          persistedBinding?.provider === resolvedProvider
            ? Option.getOrUndefined(
                yield* registry.getInstanceInfo(persistedBindingInstanceId).pipe(Effect.option),
              )
            : undefined;
        const canReusePersistedContinuation =
          persistedBindingInstanceId === resolvedInstanceId ||
          (persistedSourceInfo !== undefined &&
            persistedSourceInfo.driverKind === resolvedProvider &&
            persistedSourceInfo.continuationIdentity.continuationKey ===
              instanceInfo.continuationIdentity.continuationKey);
        const effectiveResumeCursor =
          input.resumeCursor ??
          (canReusePersistedContinuation ? persistedBinding?.resumeCursor : undefined);
        const effectiveCwd =
          input.cwd ??
          (canReusePersistedContinuation
            ? readPersistedCwd(persistedBinding?.runtimePayload)
            : undefined);
        yield* Effect.annotateCurrentSpan({
          "provider.kind": resolvedProvider,
          "provider.resume_cursor.source":
            input.resumeCursor !== undefined
              ? "request"
              : effectiveResumeCursor !== undefined && canReusePersistedContinuation
                ? "persisted"
                : "none",
          "provider.resume_cursor.present": effectiveResumeCursor !== undefined,
          "provider.cwd.source":
            input.cwd !== undefined
              ? "request"
              : effectiveCwd !== undefined && canReusePersistedContinuation
                ? "persisted"
                : "none",
          "provider.cwd.effective": effectiveCwd ?? "",
        });
        const isCompatibleCrossInstanceSwitch =
          persistedBindingInstanceId !== undefined &&
          persistedBindingInstanceId !== resolvedInstanceId &&
          persistedSourceInfo !== undefined &&
          canReusePersistedContinuation;
        if (
          isCompatibleCrossInstanceSwitch &&
          resolvedProvider === "claudeAgent" &&
          (effectiveResumeCursor === undefined || effectiveResumeCursor === null)
        ) {
          return yield* new ProviderAdapterRequestError({
            provider: resolvedProvider,
            method: "thread/continuation/sync",
            detail:
              "Compatible Claude account switching requires persisted resume state before the target provider can start.",
          });
        }
        if (isCompatibleCrossInstanceSwitch && effectiveResumeCursor !== undefined) {
          const sourceAdapter = yield* registry.getByInstance(persistedBindingInstanceId);
          if (resolvedProvider === "claudeAgent" && sourceAdapter.syncContinuation === undefined) {
            return yield* new ProviderAdapterRequestError({
              provider: sourceAdapter.provider,
              method: "thread/continuation/sync",
              detail:
                "Compatible Claude account switching requires the source provider's continuation sync capability.",
            });
          }
          // Codex-compatible instances can resume directly from their shared
          // CODEX_HOME. They intentionally do not implement the Claude-only
          // local transcript import capability.
          if (sourceAdapter.syncContinuation !== undefined) {
            yield* sourceAdapter
              .syncContinuation({
                threadId,
                resumeCursor: effectiveResumeCursor,
                ...(effectiveCwd !== undefined ? { cwd: effectiveCwd } : {}),
              })
              .pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: sourceAdapter.provider,
                      method: "thread/continuation/sync",
                      detail: cause.message,
                      cause,
                    }),
                ),
              );
          }
        }
        const adapter = yield* registry.getByInstance(resolvedInstanceId);
        yield* prepareMcpSession(threadId, resolvedInstanceId);
        const sessionNative = yield* adapter
          .startSession({
            ...input,
            providerInstanceId: resolvedInstanceId,
            ...(effectiveCwd !== undefined ? { cwd: effectiveCwd } : {}),
            ...(effectiveResumeCursor !== undefined ? { resumeCursor: effectiveResumeCursor } : {}),
          })
          .pipe(Effect.onError(() => clearMcpSession(threadId)));
        const persistedSessionCreatedAt = canReusePersistedContinuation
          ? readPersistedSessionCreatedAt(persistedBinding?.runtimePayload)
          : undefined;
        const session =
          effectiveResumeCursor !== undefined &&
          persistedSessionCreatedAt !== undefined &&
          sessionNative.initialPlanningAttestation !== undefined
            ? attestProviderSessionNativeConfiguration(
                { ...sessionNative, createdAt: persistedSessionCreatedAt },
                sessionNative.initialPlanningAttestation.effectiveModelSelection,
              )
            : sessionNative;

        if (session.provider !== adapter.provider) {
          yield* clearMcpSession(threadId);
          return yield* toValidationError(
            "ProviderService.startSession",
            `Adapter/provider mismatch: requested '${adapter.provider}', received '${session.provider}'.`,
          );
        }
        if (session.providerInstanceId === undefined) {
          yield* clearMcpSession(threadId);
          return yield* toValidationError(
            "ProviderService.startSession",
            `Adapter '${adapter.provider}' returned a session without a provider instance id.`,
          );
        }
        if (session.providerInstanceId !== resolvedInstanceId) {
          yield* clearMcpSession(threadId);
          return yield* toValidationError(
            "ProviderService.startSession",
            `Adapter/provider instance mismatch: requested '${resolvedInstanceId}', received '${session.providerInstanceId}'.`,
          );
        }
        const sessionWithInstance: ProviderSessionWithInstance = {
          ...session,
          providerInstanceId: session.providerInstanceId,
        };

        yield* recordSessionAttestation(sessionWithInstance);
        yield* upsertSessionBinding(sessionWithInstance, threadId, {
          modelSelection: input.modelSelection,
        }).pipe(
          Effect.onError(() =>
            adapter.stopSession(threadId).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("provider.session.rollback-target-failed", {
                  threadId,
                  provider: adapter.provider,
                  providerInstanceId: resolvedInstanceId,
                  cause,
                }),
              ),
              Effect.andThen(
                persistedBinding
                  ? prepareMcpSession(threadId, persistedBinding.providerInstanceId)
                  : clearMcpSession(threadId),
              ),
            ),
          ),
        );
        // The persisted binding is the routing authority. Commit the target
        // before stopping old adapters so failed target setup remains
        // rollback-safe and late events from the old instance are stale.
        yield* stopStaleSessionsForThread({
          threadId,
          currentInstanceId: resolvedInstanceId,
        });
        yield* analytics.record("provider.session.started", {
          provider: sessionWithInstance.provider,
          runtimeMode: input.runtimeMode,
          hasResumeCursor: sessionWithInstance.resumeCursor !== undefined,
          hasCwd: typeof effectiveCwd === "string" && effectiveCwd.trim().length > 0,
          hasModel:
            typeof input.modelSelection?.model === "string" &&
            input.modelSelection.model.trim().length > 0,
        });

        return sessionWithInstance;
      }).pipe(
        withMetrics({
          counter: providerSessionsTotal,
          attributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "start",
            }),
        }),
      );
    },
  );
  const startSession: ProviderServiceMethod<"startSession"> = (threadId, input) =>
    threadOperationLock.withLock(threadId, startSessionUnlocked(threadId, input));

  const sendTurnUnlocked = Effect.fn("sendTurn")(function* (
    parsed: ProviderSendTurnInput,
    boundary?: SendTurnPreInvokeBoundary,
    preResolvedRoute?: ProviderSendRoute,
  ) {
    const input = {
      ...parsed,
      attachments: parsed.attachments ?? [],
    };
    if (!input.input && input.attachments.length === 0) {
      return yield* toValidationError(
        "ProviderService.sendTurn",
        "Either input text or at least one attachment is required",
      );
    }
    yield* Effect.annotateCurrentSpan({
      "provider.operation": "send-turn",
      "provider.thread_id": input.threadId,
      "provider.interaction_mode": input.interactionMode,
      "provider.attachment_count": input.attachments.length,
    });
    let metricProvider = "unknown";
    let metricModel = input.modelSelection?.model;
    return yield* Effect.gen(function* () {
      const routed =
        preResolvedRoute ??
        (yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.sendTurn",
          allowRecovery: true,
        }));
      if (preResolvedRoute !== undefined) {
        const currentBinding = Option.getOrUndefined(yield* directory.getBinding(input.threadId));
        if (
          currentBinding === undefined ||
          currentBinding.providerInstanceId !== preResolvedRoute.instanceId ||
          currentBinding.provider !== preResolvedRoute.adapter.provider
        ) {
          return yield* toValidationError(
            "ProviderService.sendTurn",
            `Initial Planning session '${input.threadId}' changed routing authority before invocation.`,
          );
        }
      }
      metricProvider = routed.adapter.provider;
      metricModel = input.modelSelection?.model;
      yield* Effect.annotateCurrentSpan({
        "provider.kind": routed.adapter.provider,
        ...(input.modelSelection?.model ? { "provider.model": input.modelSelection.model } : {}),
      });
      const turn =
        boundary === undefined
          ? yield* routed.adapter.sendTurn(input)
          : yield* Effect.gen(function* () {
              const activeSessions = yield* routed.adapter.listSessions();
              const active = activeSessions.filter(
                (session) => session.threadId === input.threadId,
              );
              const sessionAttestation = sessionAttestations.get(input.threadId);
              const prepareAdapterTurn = routed.adapter.prepareTurn;
              if (
                active.length !== 1 ||
                sessionAttestation === undefined ||
                prepareAdapterTurn === undefined ||
                active[0]?.providerInstanceId !== routed.instanceId ||
                active[0]?.runtimeMode !== boundary.expected.runtimeMode ||
                active[0]?.cwd !== boundary.expected.cwd ||
                !Equal.equals(active[0]?.resumeCursor ?? null, boundary.expected.resumeCursor) ||
                !Equal.equals(sessionAttestation, boundary.expected) ||
                input.modelSelection === undefined
              ) {
                return yield* toValidationError(
                  "ProviderService.sendTurn",
                  `Initial Planning session '${input.threadId}' failed authoritative pre-invoke recheck.`,
                );
              }
              const preparedTurn = yield* prepareAdapterTurn(input);
              const turnAttestation: ProviderTurnAttestation = preparedTurn.attestation;
              const canonicalTurnEvidence = canonicalProviderModelSelectionEvidence(
                turnAttestation.effectiveModelSelection,
              );
              if (
                turnAttestation.providerInstanceId !== routed.instanceId ||
                turnAttestation.effectiveModelSelection.instanceId !== routed.instanceId ||
                canonicalTurnEvidence.modelSelectionJson !== turnAttestation.modelSelectionJson ||
                canonicalTurnEvidence.modelSelectionFingerprint !==
                  turnAttestation.modelSelectionFingerprint
              ) {
                return yield* toValidationError(
                  "ProviderService.sendTurn",
                  `Initial Planning adapter '${routed.adapter.provider}' returned invalid native turn attestation.`,
                );
              }
              return yield* Effect.uninterruptibleMask((restore) =>
                Effect.gen(function* () {
                  yield* restore(boundary.beforeDeliveryCas());
                  yield* boundary.persistDeliveryAttempted(turnAttestation);
                  yield* restore(boundary.afterDeliveryCas());
                  return yield* restore(
                    preparedTurn.invoke({
                      adapterEntered: () => Effect.sync(() => boundary.onAdapterEntered?.()),
                      nativeInvocationStarted: () =>
                        Effect.sync(() => boundary.onNativeInvocationStarted?.()),
                      startExternal: (operation) =>
                        Effect.gen(function* () {
                          const externalOperation = yield* Effect.sync(operation);
                          boundary.onExternalOperationStarted?.();
                          const fiber = yield* externalOperation.pipe(
                            Effect.forkChild({
                              startImmediately: true,
                              uninterruptible: false,
                            }),
                          );
                          return yield* Fiber.join(fiber);
                        }),
                    }),
                  );
                }),
              );
            });
      yield* directory.upsert({
        threadId: input.threadId,
        provider: routed.adapter.provider,
        providerInstanceId: routed.instanceId,
        status: "running",
        ...(turn.resumeCursor !== undefined ? { resumeCursor: turn.resumeCursor } : {}),
        runtimePayload: {
          ...(input.modelSelection !== undefined ? { modelSelection: input.modelSelection } : {}),
          activeTurnId: turn.turnId,
          lastRuntimeEvent: "provider.sendTurn",
          lastRuntimeEventAt: yield* nowIso,
        },
      });
      yield* analytics.record("provider.turn.sent", {
        provider: routed.adapter.provider,
        model: input.modelSelection?.model,
        interactionMode: input.interactionMode,
        attachmentCount: input.attachments.length,
        hasInput: typeof input.input === "string" && input.input.trim().length > 0,
      });
      return turn;
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        timer: providerTurnDuration,
        attributes: () =>
          providerTurnMetricAttributes({
            provider: metricProvider,
            model: metricModel,
            extra: {
              operation: "send",
            },
          }),
      }),
    );
  });
  const sendTurn: ProviderServiceMethod<"sendTurn"> = (rawInput) =>
    decodeInputOrValidationError({
      operation: "ProviderService.sendTurn",
      schema: ProviderSendTurnInput,
      payload: rawInput,
    }).pipe(
      Effect.flatMap((input) =>
        threadOperationLock.withLock(input.threadId, sendTurnUnlocked(input)),
      ),
    );
  const sendTurnAtPreInvokeBoundary: NonNullable<
    ProviderService.ProviderService["Service"]["sendTurnAtPreInvokeBoundary"]
  > = (rawInput, boundary) =>
    decodeInputOrValidationError({
      operation: "ProviderService.sendTurn",
      schema: ProviderSendTurnInput,
      payload: rawInput,
    }).pipe(
      Effect.flatMap((input) =>
        resolveProviderSendRoute({
          threadId: input.threadId,
          operation: "ProviderService.sendTurn",
        }).pipe(Effect.map((route) => ({ input, route }))),
      ),
      Effect.flatMap(({ input, route }) =>
        threadOperationLock.withLock(input.threadId, sendTurnUnlocked(input, boundary, route)),
      ),
    );
  const getSessionAttestation: NonNullable<
    ProviderService.ProviderService["Service"]["getSessionAttestation"]
  > = (threadId) => Effect.sync(() => sessionAttestations.get(threadId));

  const interruptTurn: ProviderServiceMethod<"interruptTurn"> = Effect.fn("interruptTurn")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.interruptTurn",
        schema: ProviderInterruptTurnInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.interruptTurn",
          allowRecovery: true,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "interrupt-turn",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
          "provider.turn_id": input.turnId,
        });
        yield* routed.adapter.interruptTurn(routed.threadId, input.turnId);
        yield* analytics.record("provider.turn.interrupted", {
          provider: routed.adapter.provider,
        });
      }).pipe(
        withMetrics({
          counter: providerTurnsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "interrupt",
            }),
        }),
      );
    },
  );

  const respondToRequest: ProviderServiceMethod<"respondToRequest"> = Effect.fn("respondToRequest")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.respondToRequest",
        schema: ProviderRespondToRequestInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.respondToRequest",
          allowRecovery: true,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "respond-to-request",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
          "provider.request_id": input.requestId,
        });
        yield* routed.adapter.respondToRequest(routed.threadId, input.requestId, input.decision);
        yield* analytics.record("provider.request.responded", {
          provider: routed.adapter.provider,
          decision: input.decision,
        });
      }).pipe(
        withMetrics({
          counter: providerTurnsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "approval-response",
            }),
        }),
      );
    },
  );

  const respondToUserInput: ProviderServiceMethod<"respondToUserInput"> = Effect.fn(
    "respondToUserInput",
  )(function* (rawInput) {
    const input = yield* decodeInputOrValidationError({
      operation: "ProviderService.respondToUserInput",
      schema: ProviderRespondToUserInputInput,
      payload: rawInput,
    });
    let metricProvider = "unknown";
    return yield* Effect.gen(function* () {
      const routed = yield* resolveRoutableSession({
        threadId: input.threadId,
        operation: "ProviderService.respondToUserInput",
        allowRecovery: true,
      });
      metricProvider = routed.adapter.provider;
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "respond-to-user-input",
        "provider.kind": routed.adapter.provider,
        "provider.thread_id": input.threadId,
        "provider.request_id": input.requestId,
      });
      yield* routed.adapter.respondToUserInput(routed.threadId, input.requestId, input.answers);
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        outcomeAttributes: () =>
          providerMetricAttributes(metricProvider, {
            operation: "user-input-response",
          }),
      }),
    );
  });

  const stopSessionUnlocked: ProviderServiceMethod<"stopSession"> = Effect.fn("stopSession")(
    function* (rawInput) {
      const input = yield* decodeInputOrValidationError({
        operation: "ProviderService.stopSession",
        schema: ProviderStopSessionInput,
        payload: rawInput,
      });
      let metricProvider = "unknown";
      return yield* Effect.gen(function* () {
        const routed = yield* resolveRoutableSession({
          threadId: input.threadId,
          operation: "ProviderService.stopSession",
          allowRecovery: false,
        });
        metricProvider = routed.adapter.provider;
        yield* Effect.annotateCurrentSpan({
          "provider.operation": "stop-session",
          "provider.kind": routed.adapter.provider,
          "provider.thread_id": input.threadId,
        });
        if (routed.isActive) {
          yield* routed.adapter.stopSession(routed.threadId);
        }
        yield* clearMcpSession(input.threadId);
        yield* directory.upsert({
          threadId: input.threadId,
          provider: routed.adapter.provider,
          providerInstanceId: routed.instanceId,
          status: "stopped",
          runtimePayload: {
            activeTurnId: null,
          },
        });
        yield* analytics.record("provider.session.stopped", {
          provider: routed.adapter.provider,
        });
      }).pipe(
        withMetrics({
          counter: providerSessionsTotal,
          outcomeAttributes: () =>
            providerMetricAttributes(metricProvider, {
              operation: "stop",
            }),
        }),
      );
    },
  );
  const stopSession: ProviderServiceMethod<"stopSession"> = (rawInput) =>
    decodeInputOrValidationError({
      operation: "ProviderService.stopSession",
      schema: ProviderStopSessionInput,
      payload: rawInput,
    }).pipe(
      Effect.flatMap((input) =>
        threadOperationLock.withLock(input.threadId, stopSessionUnlocked(rawInput)),
      ),
    );

  const listSessions: ProviderServiceMethod<"listSessions"> = Effect.fn("listSessions")(
    function* () {
      const currentAdapters = yield* getAdapterEntries;
      const sessionsByProvider = yield* Effect.forEach(currentAdapters, ([instanceId, adapter]) =>
        adapter
          .listSessions()
          .pipe(
            Effect.flatMap((sessions) =>
              Effect.forEach(sessions, (session) =>
                session.providerInstanceId === instanceId
                  ? Effect.succeed(session)
                  : Effect.die(
                      new Error(
                        `ProviderService.listSessions: adapter for instance '${instanceId}' emitted session for instance '${session.providerInstanceId}'.`,
                      ),
                    ),
              ),
            ),
          ),
      );
      const activeSessions = sessionsByProvider.flatMap((sessions) => sessions);
      const persistedBindings = yield* directory.listBindings().pipe(Effect.orDie);
      const bindingsByThreadId = new Map<
        ThreadId,
        ProviderSessionDirectory.ProviderRuntimeBinding
      >();
      for (const binding of persistedBindings) {
        bindingsByThreadId.set(binding.threadId, binding);
      }

      const sessions: ProviderSession[] = [];
      for (const session of activeSessions) {
        const binding = bindingsByThreadId.get(session.threadId);
        if (!binding) {
          yield* recordSessionAttestation(session as ProviderSessionWithAttestation).pipe(
            Effect.orDie,
          );
          sessions.push(session);
          continue;
        }

        const overrides: {
          resumeCursor?: ProviderSession["resumeCursor"];
          runtimeMode?: ProviderSession["runtimeMode"];
          createdAt?: ProviderSession["createdAt"];
        } = {};
        if (binding.provider !== session.provider) {
          return yield* Effect.die(
            new Error(
              `ProviderService.listSessions: thread '${session.threadId}' is active on provider '${session.provider}' but persisted binding names provider '${binding.provider}'.`,
            ),
          );
        }
        if (binding.providerInstanceId !== session.providerInstanceId) {
          return yield* Effect.die(
            new Error(
              `ProviderService.listSessions: thread '${session.threadId}' is active on provider instance '${session.providerInstanceId}' but persisted binding names '${binding.providerInstanceId}'.`,
            ),
          );
        }
        if (session.resumeCursor === undefined && binding.resumeCursor !== undefined) {
          overrides.resumeCursor = binding.resumeCursor;
        }
        if (binding.runtimeMode !== undefined) {
          overrides.runtimeMode = binding.runtimeMode;
        }
        const persistedSessionCreatedAt = readPersistedSessionCreatedAt(binding.runtimePayload);
        if (persistedSessionCreatedAt !== undefined) {
          overrides.createdAt = persistedSessionCreatedAt;
        }
        const effectiveSessionBase = Object.assign(
          {},
          session,
          overrides,
        ) as ProviderSessionWithAttestation;
        const effectiveSession =
          effectiveSessionBase.initialPlanningAttestation === undefined
            ? effectiveSessionBase
            : attestProviderSessionNativeConfiguration(
                effectiveSessionBase,
                effectiveSessionBase.initialPlanningAttestation.effectiveModelSelection,
              );
        yield* recordSessionAttestation(effectiveSession).pipe(Effect.orDie);
        sessions.push(effectiveSession);
      }
      return sessions;
    },
  );

  const getCapabilities: ProviderServiceMethod<"getCapabilities"> = (instanceId) =>
    registry.getByInstance(instanceId).pipe(Effect.map((adapter) => adapter.capabilities));

  const getInstanceInfo: ProviderServiceMethod<"getInstanceInfo"> = (instanceId) =>
    registry.getInstanceInfo(instanceId);

  const rollbackConversation: ProviderServiceMethod<"rollbackConversation"> = Effect.fn(
    "rollbackConversation",
  )(function* (rawInput) {
    const input = yield* decodeInputOrValidationError({
      operation: "ProviderService.rollbackConversation",
      schema: ProviderRollbackConversationInput,
      payload: rawInput,
    });
    if (input.numTurns === 0) {
      return;
    }
    let metricProvider = "unknown";
    return yield* Effect.gen(function* () {
      const routed = yield* resolveRoutableSession({
        threadId: input.threadId,
        operation: "ProviderService.rollbackConversation",
        allowRecovery: true,
      });
      metricProvider = routed.adapter.provider;
      yield* Effect.annotateCurrentSpan({
        "provider.operation": "rollback-conversation",
        "provider.kind": routed.adapter.provider,
        "provider.thread_id": input.threadId,
        "provider.rollback_turns": input.numTurns,
      });
      yield* routed.adapter.rollbackThread(routed.threadId, input.numTurns);
      yield* analytics.record("provider.conversation.rolled_back", {
        provider: routed.adapter.provider,
        turns: input.numTurns,
      });
    }).pipe(
      withMetrics({
        counter: providerTurnsTotal,
        outcomeAttributes: () =>
          providerMetricAttributes(metricProvider, {
            operation: "rollback",
          }),
      }),
    );
  });

  const runStopAll = Effect.fn("runStopAll")(function* () {
    const threadIds = yield* directory.listThreadIds();
    const currentAdapters = yield* getAdapterEntries;
    const activeSessions = yield* Effect.forEach(currentAdapters, ([instanceId, adapter]) =>
      adapter
        .listSessions()
        .pipe(
          Effect.flatMap((sessions) =>
            Effect.forEach(sessions, (session) =>
              session.providerInstanceId === instanceId
                ? Effect.succeed(session)
                : Effect.die(
                    new Error(
                      `ProviderService.stopAll: adapter for instance '${instanceId}' emitted session for instance '${session.providerInstanceId}'.`,
                    ),
                  ),
            ),
          ),
        ),
    ).pipe(Effect.map((sessionsByAdapter) => sessionsByAdapter.flatMap((sessions) => sessions)));
    yield* Effect.forEach(activeSessions, (session) =>
      Effect.flatMap(nowIso, (lastRuntimeEventAt) =>
        upsertSessionBinding(session, session.threadId, {
          lastRuntimeEvent: "provider.stopAll",
          lastRuntimeEventAt,
        }),
      ),
    ).pipe(Effect.asVoid);
    yield* Effect.forEach(currentAdapters, ([, adapter]) => adapter.stopAll()).pipe(Effect.asVoid);
    yield* McpSessionRegistry.revokeAllActiveMcpCredentials();
    McpProviderSession.clearAllMcpProviderSessions();
    const bindings = yield* directory.listBindings();
    yield* Effect.forEach(bindings, (binding) =>
      Effect.flatMap(nowIso, (lastRuntimeEventAt) =>
        directory.upsert({
          threadId: binding.threadId,
          provider: binding.provider,
          providerInstanceId: binding.providerInstanceId,
          status: "stopped",
          runtimePayload: {
            activeTurnId: null,
            lastRuntimeEvent: "provider.stopAll",
            lastRuntimeEventAt,
          },
        }),
      ),
    ).pipe(Effect.asVoid);
    yield* analytics.record("provider.sessions.stopped_all", {
      sessionCount: threadIds.length,
    });
    yield* analytics.flush;
  });

  yield* Effect.addFinalizer(() =>
    rebuildBarrier.withOperation(
      runStopAll().pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("failed to stop provider service", {
            errorTag: causeErrorTag(cause),
          }),
        ),
      ),
    ),
  );

  return {
    startSession: (threadId, input) => rebuildBarrier.withOperation(startSession(threadId, input)),
    sendTurn: (input) => rebuildBarrier.withOperation(sendTurn(input)),
    sendTurnAtPreInvokeBoundary: (input, boundary) =>
      rebuildBarrier.withOperation(sendTurnAtPreInvokeBoundary(input, boundary)),
    getSessionAttestation,
    interruptTurn: (input) => rebuildBarrier.withOperation(interruptTurn(input)),
    respondToRequest: (input) => rebuildBarrier.withOperation(respondToRequest(input)),
    respondToUserInput: (input) => rebuildBarrier.withOperation(respondToUserInput(input)),
    stopSession: (input) => rebuildBarrier.withOperation(stopSession(input)),
    listSessions: () => rebuildBarrier.withOperation(listSessions()),
    getCapabilities: (instanceId) => rebuildBarrier.withOperation(getCapabilities(instanceId)),
    getInstanceInfo,
    rollbackConversation: (input) => rebuildBarrier.withOperation(rollbackConversation(input)),
    subscribeEvents: PubSub.subscribe(runtimeEventPubSub),
    startRuntimeEventSources,
    openRuntimeEventPublishing: Deferred.succeed(runtimeEventPublishingReady, undefined).pipe(
      Effect.asVoid,
    ),
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (ProviderRuntimeIngestion, CheckpointReactor, etc.) each
    // independently receive all runtime events.
    get streamEvents(): ProviderServiceMethod<"streamEvents"> {
      return Stream.fromPubSub(runtimeEventPubSub);
    },
  } satisfies ProviderService.ProviderService["Service"];
});

export const ProviderServiceLive = Layer.effect(
  ProviderService.ProviderService,
  makeProviderService(),
);

export function makeProviderServiceLive(options?: ProviderServiceLiveOptions) {
  return Layer.effect(ProviderService.ProviderService, makeProviderService(options)).pipe(
    Layer.provideMerge(
      options?.threadOperationLockObserver === undefined
        ? ProviderThreadOperationLockLive
        : makeProviderThreadOperationLockLive(options.threadOperationLockObserver),
    ),
    Layer.provideMerge(ProviderRegistryRebuildBarrierLive),
  );
}
