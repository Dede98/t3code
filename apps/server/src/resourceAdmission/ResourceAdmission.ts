import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import type { HostBudgetLedger, HostBudgetLedgerError } from "./HostBudgetLedger.ts";
import { makeFileHostBudgetLedger } from "./HostBudgetLedger.ts";
import {
  defaultResourceAdmissionSettings,
  type PersistedReservation,
  type ResourceAdmissionDecision,
  type ResourceAdmissionGrant,
  type ResourceAdmissionKind,
  type ResourceAdmissionLedgerState,
  type ResourceAdmissionRequest,
  type ResourceAdmissionSettings,
  type ResourceAdmissionSnapshot,
  type ResourceAdmissionSnapshotEntry,
  type ResourceAdmissionUpdate,
  type ResourceAdmissionWaitReason,
  type ResourcePressureSample,
  type ResourceReservationAuthority,
} from "./model.ts";
import { ResourcePressure } from "./ResourcePressure.ts";

export class ResourceAdmissionError extends Schema.TaggedError<ResourceAdmissionError>()(
  "ResourceAdmissionError",
  {
    operation: Schema.String,
    reason: Schema.Union([
      Schema.Literal("invalid-settings"),
      Schema.Literal("invalid-authority"),
      Schema.Literal("active-children"),
    ]),
    message: Schema.String,
  },
) {}

export interface ResourceAdmissionShape {
  /** Reconciles an already-running managed job, even when the configured budget is full. */
  readonly adoptActive: (
    request: ResourceAdmissionRequest,
  ) => Effect.Effect<ResourceReservationAuthority, HostBudgetLedgerError | ResourceAdmissionError>;
  readonly acquire: (
    request: ResourceAdmissionRequest,
  ) => Effect.Effect<
    Extract<ResourceAdmissionDecision, { readonly _tag: "Admitted" | "Rejected" }>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly request: (
    request: ResourceAdmissionRequest,
  ) => Effect.Effect<
    ResourceAdmissionUpdate<ResourceAdmissionDecision>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly cancelWaiting: (
    authority: Pick<ResourceReservationAuthority, "reservationId" | "ownerId" | "ownerFenceToken">,
  ) => Effect.Effect<
    ResourceAdmissionUpdate<boolean>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly release: (
    authority: ResourceReservationAuthority,
  ) => Effect.Effect<
    ResourceAdmissionUpdate<boolean>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  /** Returns a not-yet-active grant to the queue without making its stable id terminal. */
  readonly defer: (
    authority: ResourceReservationAuthority,
  ) => Effect.Effect<
    ResourceAdmissionUpdate<boolean>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly observeActivity: (
    authority: ResourceReservationAuthority,
    activity: "active" | "inactive" | "unknown",
  ) => Effect.Effect<
    ResourceAdmissionUpdate<boolean>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly refresh: Effect.Effect<
    ReadonlyArray<ResourceAdmissionGrant>,
    HostBudgetLedgerError | ResourceAdmissionError
  >;
  readonly snapshot: Effect.Effect<ResourceAdmissionSnapshot, HostBudgetLedgerError>;
}

export class ResourceAdmission extends Context.Service<ResourceAdmission, ResourceAdmissionShape>()(
  "t3/resourceAdmission/ResourceAdmission",
) {}

type PendingUpdate<A> = Omit<ResourceAdmissionUpdate<A>, "ledgerRevision" | "pressureSampledAtMs">;

function capacityKey(reservation: Pick<PersistedReservation, "kind">): string {
  return reservation.kind === "providerTurn" ? "provider:host" : "localCheck:host";
}

function cloneState(state: ResourceAdmissionLedgerState): ResourceAdmissionLedgerState {
  return {
    ...state,
    reservations: { ...state.reservations },
    interactiveGrantBursts: { ...state.interactiveGrantBursts },
    settingsRegistrations: { ...state.settingsRegistrations },
    effectiveSettings: state.effectiveSettings === null ? null : { ...state.effectiveSettings },
    pressure: { ...state.pressure },
  };
}

function composeEffectiveSettings(
  current: ResourceAdmissionSettings | null,
  incoming: ResourceAdmissionSettings,
): ResourceAdmissionSettings {
  if (current === null) return incoming;
  return {
    providerMaxConcurrent: Math.min(current.providerMaxConcurrent, incoming.providerMaxConcurrent),
    interactiveReserve: Math.max(current.interactiveReserve, incoming.interactiveReserve),
    backgroundMaxGrantDelayMs: Math.min(
      current.backgroundMaxGrantDelayMs,
      incoming.backgroundMaxGrantDelayMs,
    ),
    maxInteractiveGrantBurst: Math.min(
      current.maxInteractiveGrantBurst,
      incoming.maxInteractiveGrantBurst,
    ),
    localCheckMaxConcurrent: Math.min(
      current.localCheckMaxConcurrent,
      incoming.localCheckMaxConcurrent,
    ),
    cpuPauseUtilization: Math.min(current.cpuPauseUtilization, incoming.cpuPauseUtilization),
    cpuResumeUtilization: Math.min(current.cpuResumeUtilization, incoming.cpuResumeUtilization),
    availableMemoryPauseBytes: Math.max(
      current.availableMemoryPauseBytes,
      incoming.availableMemoryPauseBytes,
    ),
    availableMemoryResumeBytes: Math.max(
      current.availableMemoryResumeBytes,
      incoming.availableMemoryResumeBytes,
    ),
    gpuMaxConcurrent: Math.min(current.gpuMaxConcurrent, incoming.gpuMaxConcurrent),
    missingTelemetryPolicy:
      current.missingTelemetryPolicy === "defer-background" ||
      incoming.missingTelemetryPolicy === "defer-background"
        ? "defer-background"
        : "allow",
  };
}

function effectiveSettings(
  state: ResourceAdmissionLedgerState,
  fallback: ResourceAdmissionSettings,
): ResourceAdmissionSettings {
  return state.effectiveSettings ?? fallback;
}

function updatePressure(
  state: ResourceAdmissionLedgerState,
  sample: ResourcePressureSample,
  settings: ResourceAdmissionSettings,
  settingsOwnerId: string,
): ResourceAdmissionLedgerState {
  const settingsRegistrations: Record<
    string,
    { readonly pid: number; readonly settings: ResourceAdmissionSettings }
  > = Object.fromEntries(
    Object.entries(state.settingsRegistrations).filter(
      ([ownerId, registration]) =>
        ownerId === settingsOwnerId || !ownerProcessIsDead(`local-process:${registration.pid}:`),
    ),
  );
  settingsRegistrations[settingsOwnerId] = { pid: process.pid, settings };
  const composedSettings = Object.values(settingsRegistrations).reduce<ResourceAdmissionSettings>(
    (current, registration) => composeEffectiveSettings(current, registration.settings),
    settings,
  );
  const telemetryAvailable =
    sample.telemetry === "available" &&
    sample.cpuUtilization !== null &&
    sample.availableMemoryBytes !== null;
  let cpuPaused = state.pressure.cpuPaused;
  let memoryPaused = state.pressure.memoryPaused;
  if (sample.cpuUtilization !== null) {
    if (sample.cpuUtilization >= composedSettings.cpuPauseUtilization) cpuPaused = true;
    else if (sample.cpuUtilization <= composedSettings.cpuResumeUtilization) cpuPaused = false;
  }
  if (sample.availableMemoryBytes !== null) {
    if (sample.availableMemoryBytes <= composedSettings.availableMemoryPauseBytes)
      memoryPaused = true;
    else if (sample.availableMemoryBytes >= composedSettings.availableMemoryResumeBytes)
      memoryPaused = false;
  }
  return {
    ...state,
    settingsRegistrations,
    effectiveSettings: composedSettings,
    pressure: {
      telemetryAvailable,
      cpuPaused,
      memoryPaused,
      sampledAtMs: sample.sampledAtMs,
    },
  };
}

function backgroundPressureReason(
  state: ResourceAdmissionLedgerState,
  settings: ResourceAdmissionSettings,
): ResourceAdmissionWaitReason | null {
  const resolvedSettings = effectiveSettings(state, settings);
  if (
    !state.pressure.telemetryAvailable &&
    resolvedSettings.missingTelemetryPolicy === "defer-background"
  )
    return "telemetry-unavailable";
  if (state.pressure.cpuPaused) return "cpu-pressure";
  if (state.pressure.memoryPaused) return "memory-pressure";
  return null;
}

function activeReservations(
  state: ResourceAdmissionLedgerState,
): ReadonlyArray<PersistedReservation> {
  return Object.values(state.reservations).filter(
    (reservation) => reservation.state === "admitted" && reservation.accounted,
  );
}

function configuredCapacity(
  reservation: Pick<PersistedReservation, "kind">,
  settings: ResourceAdmissionSettings,
): number {
  return reservation.kind === "providerTurn"
    ? settings.providerMaxConcurrent
    : settings.localCheckMaxConcurrent;
}

function capacityReason(kind: ResourceAdmissionKind): ResourceAdmissionWaitReason {
  return kind === "providerTurn" ? "provider-limit" : "local-check-limit";
}

function gpuCapacity(
  state: ResourceAdmissionLedgerState,
  sample: ResourcePressureSample,
  settings: ResourceAdmissionSettings,
): number {
  if (sample.gpu.status !== "reliable") return 0;
  const activeGpu = activeReservations(state).filter(
    (reservation) => reservation.gpuRequired,
  ).length;
  return Math.min(settings.gpuMaxConcurrent, sample.gpu.available) - activeGpu;
}

function candidateEligibility(input: {
  readonly reservation: PersistedReservation;
  readonly state: ResourceAdmissionLedgerState;
  readonly sample: ResourcePressureSample;
  readonly settings: ResourceAdmissionSettings;
  readonly allowInteractiveReserve?: boolean;
}): ResourceAdmissionWaitReason | null {
  const { reservation, state, sample } = input;
  const settings = effectiveSettings(state, input.settings);
  const active = activeReservations(state);
  const key = capacityKey(reservation);
  const activeInPool = active.filter((entry) => capacityKey(entry) === key);
  const capacity = configuredCapacity(reservation, settings);
  if (activeInPool.length >= capacity) return capacityReason(reservation.kind);
  if (reservation.priority === "background") {
    const reserve =
      reservation.kind === "providerTurn" ? Math.min(settings.interactiveReserve, capacity) : 0;
    const activeBackground = activeInPool.filter((entry) => entry.priority === "background").length;
    if (activeBackground >= capacity - reserve && input.allowInteractiveReserve !== true)
      return "interactive-priority";
    const pressureReason = backgroundPressureReason(state, settings);
    if (pressureReason !== null) return pressureReason;
  }
  if (reservation.gpuRequired && gpuCapacity(state, sample, settings) <= 0) return "gpu-capacity";
  return null;
}

function waitReason(input: {
  readonly reservation: PersistedReservation;
  readonly state: ResourceAdmissionLedgerState;
  readonly sample: ResourcePressureSample;
  readonly settings: ResourceAdmissionSettings;
}): ResourceAdmissionWaitReason {
  const recoveryBlocksPool = activeReservations(input.state).some(
    (entry) =>
      capacityKey(entry) === capacityKey(input.reservation) &&
      (entry.activity === "orphaned-active" || entry.activity === "orphaned-unknown"),
  );
  if (recoveryBlocksPool) return "recovery-capacity";
  const direct = candidateEligibility(input);
  if (direct !== null) return direct;
  if (input.reservation.priority === "background") {
    const samePoolInteractive = Object.values(input.state.reservations).some(
      (entry) =>
        entry.state === "waiting" &&
        entry.priority === "interactive" &&
        capacityKey(entry) === capacityKey(input.reservation),
    );
    if (samePoolInteractive) return "interactive-priority";
  }
  return capacityReason(input.reservation.kind);
}

function grant(
  state: ResourceAdmissionLedgerState,
  requestId: string,
  nowMs: number,
  updateFairness = true,
): { readonly state: ResourceAdmissionLedgerState; readonly grant: ResourceAdmissionGrant } {
  const reservation = state.reservations[requestId]!;
  const reservationFenceToken = state.nextReservationFenceToken;
  const admitted: PersistedReservation = {
    ...reservation,
    state: "admitted",
    admittedAtMs: nowMs,
    reservationFenceToken,
    activity: "possible",
    activityObservedAtMs: nowMs,
  };
  const key = capacityKey(reservation);
  const previousBurst = state.interactiveGrantBursts[key] ?? 0;
  const nextState: ResourceAdmissionLedgerState = {
    ...state,
    nextReservationFenceToken: reservationFenceToken + 1,
    reservations: { ...state.reservations, [requestId]: admitted },
    interactiveGrantBursts: {
      ...state.interactiveGrantBursts,
      [key]: updateFairness
        ? reservation.priority === "interactive"
          ? previousBurst + 1
          : 0
        : previousBurst,
    },
  };
  return {
    state: nextState,
    grant: {
      requestId,
      accounted: admitted.accounted,
      authority: {
        reservationId: requestId,
        ownerId: admitted.ownerId,
        ownerFenceToken: admitted.ownerFenceToken,
        reservationFenceToken,
      },
    },
  };
}

function preferredPoolCandidate(input: {
  readonly state: ResourceAdmissionLedgerState;
  readonly sample: ResourcePressureSample;
  readonly settings: ResourceAdmissionSettings;
  readonly nowMs: number;
  readonly key: string;
}): PersistedReservation | null {
  const settings = effectiveSettings(input.state, input.settings);
  const waiting = Object.values(input.state.reservations)
    .filter(
      (entry) =>
        entry.state === "waiting" &&
        entry.activity === "possible" &&
        capacityKey(entry) === input.key,
    )
    .sort((left, right) => left.sequence - right.sequence);
  const interactive = waiting.filter((entry) => entry.priority === "interactive");
  const background = waiting.filter((entry) => entry.priority === "background");
  const burst = input.state.interactiveGrantBursts[input.key] ?? 0;
  const backgroundAged = background.some(
    (entry) => input.nowMs - entry.requestedAtMs >= settings.backgroundMaxGrantDelayMs,
  );
  const preferBackground =
    background.length > 0 && (backgroundAged || burst >= settings.maxInteractiveGrantBurst);
  const ordered = preferBackground
    ? [...background, ...interactive]
    : [...interactive, ...background];
  return (
    ordered.find(
      (reservation) =>
        candidateEligibility({
          reservation,
          state: input.state,
          sample: input.sample,
          settings,
          allowInteractiveReserve: reservation.priority === "background" && preferBackground,
        }) === null,
    ) ?? null
  );
}

function schedule(input: {
  readonly state: ResourceAdmissionLedgerState;
  readonly sample: ResourcePressureSample;
  readonly settings: ResourceAdmissionSettings;
  readonly nowMs: number;
}): {
  readonly state: ResourceAdmissionLedgerState;
  readonly grants: ReadonlyArray<ResourceAdmissionGrant>;
} {
  let state = input.state;
  const settings = effectiveSettings(state, input.settings);
  const grants: ResourceAdmissionGrant[] = [];
  while (true) {
    const poolKeys = new Set(
      Object.values(state.reservations)
        .filter((entry) => entry.state === "waiting")
        .map(capacityKey),
    );
    const candidates = [...poolKeys]
      .map((key) => preferredPoolCandidate({ ...input, state, settings, key }))
      .filter((candidate): candidate is PersistedReservation => candidate !== null)
      .sort((left, right) => left.sequence - right.sequence);
    const candidate = candidates[0];
    if (candidate === undefined) break;
    const admitted = grant(state, candidate.requestId, input.nowMs);
    state = admitted.state;
    grants.push(admitted.grant);
  }
  return { state, grants };
}

function decisionFor(input: {
  readonly reservation: PersistedReservation;
  readonly state: ResourceAdmissionLedgerState;
  readonly sample: ResourcePressureSample;
  readonly settings: ResourceAdmissionSettings;
}): ResourceAdmissionDecision {
  const reservation = input.reservation;
  if (reservation.state === "admitted" && reservation.reservationFenceToken !== null) {
    return {
      _tag: "Admitted",
      accounted: reservation.accounted,
      authority: {
        reservationId: reservation.requestId,
        ownerId: reservation.ownerId,
        ownerFenceToken: reservation.ownerFenceToken,
        reservationFenceToken: reservation.reservationFenceToken,
      },
    };
  }
  if (reservation.state === "waiting") {
    return {
      _tag: "Waiting",
      requestId: reservation.requestId,
      reason: waitReason(input),
    };
  }
  return {
    _tag: "Rejected",
    requestId: reservation.requestId,
    reason: "invalid-request",
    message: `Resource request '${reservation.requestId}' is already ${reservation.state}.`,
  };
}

function sameRequest(existing: PersistedReservation, request: ResourceAdmissionRequest): boolean {
  return (
    existing.kind === request.kind &&
    existing.priority === request.priority &&
    existing.accountScope === (request.accountScope ?? null) &&
    existing.executionKey === (request.executionKey ?? null) &&
    (existing.replayable ?? true) === (request.replayable ?? true) &&
    existing.gpuRequired === (request.gpuRequired ?? false) &&
    existing.parentReservationId === (request.parent?.reservationId ?? null)
  );
}

function validAuthority(
  reservation: PersistedReservation,
  authority: Pick<
    ResourceReservationAuthority,
    "ownerId" | "ownerFenceToken" | "reservationFenceToken"
  >,
): boolean {
  return (
    reservation.ownerId === authority.ownerId &&
    reservation.ownerFenceToken === authority.ownerFenceToken &&
    reservation.reservationFenceToken === authority.reservationFenceToken
  );
}

function validateSettings(settings: ResourceAdmissionSettings): void {
  const positiveIntegers = [
    settings.providerMaxConcurrent,
    settings.localCheckMaxConcurrent,
    settings.maxInteractiveGrantBurst,
    settings.backgroundMaxGrantDelayMs,
  ];
  if (
    positiveIntegers.some((value) => !Number.isSafeInteger(value) || value <= 0) ||
    !Number.isSafeInteger(settings.interactiveReserve) ||
    settings.interactiveReserve < 0 ||
    !Number.isSafeInteger(settings.gpuMaxConcurrent) ||
    settings.gpuMaxConcurrent < 0 ||
    settings.cpuResumeUtilization < 0 ||
    settings.cpuPauseUtilization > 1 ||
    settings.cpuResumeUtilization >= settings.cpuPauseUtilization ||
    settings.availableMemoryPauseBytes < 0 ||
    settings.availableMemoryResumeBytes <= settings.availableMemoryPauseBytes
  ) {
    throw new ResourceAdmissionError({
      operation: "make",
      reason: "invalid-settings",
      message: "Resource admission settings violate capacity or hysteresis invariants.",
    });
  }
}

function ownerProcessIsDead(ownerId: string): boolean {
  const pid = /^(?:local-process|provider-coordinator):(\d+):/.exec(ownerId)?.[1];
  if (pid === undefined) return false;
  try {
    process.kill(Number(pid), 0);
    return false;
  } catch (error) {
    return typeof error === "object" && error !== null && "code" in error && error.code === "ESRCH";
  }
}

let nextSettingsOwnerSequence = 1;

export const make = Effect.fn("resourceAdmission.make")(function* (options: {
  readonly ledger: HostBudgetLedger;
  readonly settings?: ResourceAdmissionSettings;
  readonly readSettings?: Effect.Effect<ResourceAdmissionSettings>;
}) {
  const pressure = yield* ResourcePressure;
  const settingsOwnerId = `resource-admission-settings:${process.pid}:${nextSettingsOwnerSequence++}`;
  const initialSettings = options.settings ?? defaultResourceAdmissionSettings;
  yield* Effect.sync(() => validateSettings(initialSettings));
  const currentSettings = (options.readSettings ?? Effect.succeed(initialSettings)).pipe(
    Effect.tap((settings) => Effect.sync(() => validateSettings(settings))),
  );

  const sampleNow = Effect.fn("resourceAdmission.sampleNow")(function* () {
    const [sample, now] = yield* Effect.all([pressure.sample, DateTime.now]);
    return { sample, nowMs: DateTime.toEpochMillis(now) };
  });

  const request = Effect.fn("resourceAdmission.request")(function* (
    request: ResourceAdmissionRequest,
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    if (
      request.requestId.length === 0 ||
      request.ownerId.length === 0 ||
      (request.executionKey !== undefined && request.executionKey.length === 0) ||
      !Number.isSafeInteger(request.ownerFenceToken) ||
      request.ownerFenceToken < 0 ||
      (request.kind === "providerTurn" && !request.accountScope?.trim()) ||
      (request.kind === "localCheck" && request.accountScope !== undefined)
    ) {
      return {
        result: {
          _tag: "Rejected",
          requestId: request.requestId,
          reason: "invalid-request",
          message: "The resource request identity, fence, kind, or account scope is invalid.",
        },
        newlyAdmitted: [],
        ledgerRevision: null,
        pressureSampledAtMs: sample.sampledAtMs,
      } satisfies ResourceAdmissionUpdate<ResourceAdmissionDecision>;
    }
    if (
      request.gpuRequired === true &&
      (settings.gpuMaxConcurrent === 0 || sample.gpu.status !== "reliable")
    ) {
      return {
        result: {
          _tag: "Rejected",
          requestId: request.requestId,
          reason: "gpu-unavailable",
          message: "Reliable GPU capacity is not available on this host execution path.",
        },
        newlyAdmitted: [],
        ledgerRevision: null,
        pressureSampledAtMs: sample.sampledAtMs,
      } satisfies ResourceAdmissionUpdate<ResourceAdmissionDecision>;
    }
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const existing = state.reservations[request.requestId];
      if (existing !== undefined) {
        if (!sameRequest(existing, request)) {
          return {
            state,
            value: {
              result: {
                _tag: "Rejected",
                requestId: request.requestId,
                reason: "invalid-request",
                message: "The stable request id was reused for different resource requirements.",
              },
              newlyAdmitted: [],
            } satisfies PendingUpdate<ResourceAdmissionDecision>,
          };
        }
        if (
          existing.state === "admitted" &&
          existing.activity !== "possible" &&
          existing.ownerId === request.ownerId &&
          existing.ownerFenceToken === request.ownerFenceToken
        ) {
          return {
            state,
            value: {
              result: {
                _tag: "Waiting",
                requestId: request.requestId,
                reason: "recovery-capacity",
              },
              newlyAdmitted: [],
            } satisfies PendingUpdate<ResourceAdmissionDecision>,
          };
        }
        if (
          existing.ownerId !== request.ownerId ||
          existing.ownerFenceToken !== request.ownerFenceToken
        ) {
          if (request.ownerFenceToken <= existing.ownerFenceToken) {
            return {
              state,
              value: {
                result: {
                  _tag: "Rejected",
                  requestId: request.requestId,
                  reason: "ownership-conflict",
                  message: "A current or newer owner already controls this resource request.",
                },
                newlyAdmitted: [],
              } satisfies PendingUpdate<ResourceAdmissionDecision>,
            };
          }
          if (existing.state === "admitted" && existing.activity !== "possible") {
            return {
              state,
              value: {
                result: {
                  _tag: "Rejected",
                  requestId: request.requestId,
                  reason: "ownership-conflict",
                  message:
                    "An active or uncertain execution can only be taken over through explicit reconciliation.",
                },
                newlyAdmitted: [],
              } satisfies PendingUpdate<ResourceAdmissionDecision>,
            };
          }
          const reservationFenceToken =
            existing.state === "admitted" ? state.nextReservationFenceToken : null;
          state = {
            ...state,
            nextReservationFenceToken:
              reservationFenceToken === null
                ? state.nextReservationFenceToken
                : reservationFenceToken + 1,
            reservations: {
              ...state.reservations,
              [request.requestId]: {
                ...existing,
                ownerId: request.ownerId,
                ownerFenceToken: request.ownerFenceToken,
                reservationFenceToken,
                activity: "unknown",
                activityObservedAtMs: nowMs,
              },
            },
          };
        }
        let grants: ReadonlyArray<ResourceAdmissionGrant> = [];
        if (state.reservations[request.requestId]!.state === "waiting") {
          if (state.reservations[request.requestId]!.activity === "unknown") {
            state = {
              ...state,
              reservations: {
                ...state.reservations,
                [request.requestId]: {
                  ...state.reservations[request.requestId]!,
                  activity: "possible",
                  activityObservedAtMs: nowMs,
                },
              },
            };
          }
          const scheduled = schedule({ state, sample, settings, nowMs });
          state = scheduled.state;
          grants = scheduled.grants;
        }
        return {
          state,
          value: {
            result: decisionFor({
              reservation: state.reservations[request.requestId]!,
              state,
              sample,
              settings,
            }),
            newlyAdmitted: grants,
          },
        };
      }

      let accounted = true;
      if (request.parent !== undefined) {
        const parent = state.reservations[request.parent.reservationId];
        if (
          parent === undefined ||
          parent.state !== "admitted" ||
          parent.reservationFenceToken !== request.parent.reservationFenceToken
        ) {
          return {
            state,
            value: {
              result: {
                _tag: "Rejected",
                requestId: request.requestId,
                reason: "invalid-request",
                message: "The parent reservation is not active under the supplied fence.",
              },
              newlyAdmitted: [],
            } satisfies PendingUpdate<ResourceAdmissionDecision>,
          };
        }
        if (parent.kind === request.kind) {
          const sameCapacity =
            parent.accountScope === (request.accountScope ?? null) &&
            (!request.gpuRequired || parent.gpuRequired);
          if (!sameCapacity) {
            return {
              state,
              value: {
                result: {
                  _tag: "Rejected",
                  requestId: request.requestId,
                  reason: "invalid-request",
                  message: "A child cannot widen the resource budget shared with its parent.",
                },
                newlyAdmitted: [],
              } satisfies PendingUpdate<ResourceAdmissionDecision>,
            };
          }
          accounted = false;
        }
      }

      const reservation: PersistedReservation = {
        requestId: request.requestId,
        kind: request.kind,
        priority: request.priority,
        accountScope: request.accountScope ?? null,
        ownerId: request.ownerId,
        ownerFenceToken: request.ownerFenceToken,
        executionKey: request.executionKey ?? null,
        replayable: request.replayable ?? true,
        reservationFenceToken: null,
        requestedAtMs: nowMs,
        sequence: state.nextSequence,
        admittedAtMs: null,
        state: "waiting",
        accounted,
        gpuRequired: request.gpuRequired ?? false,
        activity: "possible",
        activityObservedAtMs: nowMs,
        parentReservationId: request.parent?.reservationId ?? null,
        parentReservationFenceToken: request.parent?.reservationFenceToken ?? null,
      };
      state = {
        ...state,
        nextSequence: state.nextSequence + 1,
        reservations: { ...state.reservations, [request.requestId]: reservation },
      };

      let grants: ReadonlyArray<ResourceAdmissionGrant>;
      if (!accounted) {
        const admitted = grant(state, request.requestId, nowMs, false);
        state = admitted.state;
        grants = [admitted.grant];
      } else {
        const scheduled = schedule({ state, sample, settings, nowMs });
        state = scheduled.state;
        grants = scheduled.grants;
      }
      return {
        state,
        value: {
          result: decisionFor({
            reservation: state.reservations[request.requestId]!,
            state,
            sample,
            settings,
          }),
          newlyAdmitted: grants,
        },
      };
    });
    return {
      ...transaction.value,
      ledgerRevision: transaction.revision,
      pressureSampledAtMs: sample.sampledAtMs,
    };
  });

  const adoptActive = Effect.fn("resourceAdmission.adoptActive")(function* (
    request: ResourceAdmissionRequest,
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const existing = state.reservations[request.requestId];
      if (
        request.requestId.length === 0 ||
        request.ownerId.length === 0 ||
        !Number.isSafeInteger(request.ownerFenceToken) ||
        request.ownerFenceToken < 0 ||
        (request.kind === "providerTurn" && !request.accountScope?.trim()) ||
        (existing !== undefined &&
          (!sameRequest(existing, request) ||
            (existing.ownerId !== request.ownerId &&
              request.ownerFenceToken <= existing.ownerFenceToken)))
      ) {
        return { state, value: null };
      }
      const reservationFenceToken = state.nextReservationFenceToken;
      const reservation: PersistedReservation =
        existing === undefined
          ? {
              requestId: request.requestId,
              kind: request.kind,
              priority: request.priority,
              accountScope: request.accountScope ?? null,
              ownerId: request.ownerId,
              ownerFenceToken: request.ownerFenceToken,
              executionKey: request.executionKey ?? null,
              replayable: request.replayable ?? true,
              reservationFenceToken,
              requestedAtMs: nowMs,
              sequence: state.nextSequence,
              admittedAtMs: nowMs,
              state: "admitted",
              accounted: true,
              gpuRequired: request.gpuRequired ?? false,
              activity: "active",
              activityObservedAtMs: nowMs,
              parentReservationId: null,
              parentReservationFenceToken: null,
            }
          : {
              ...existing,
              ownerId: request.ownerId,
              ownerFenceToken: request.ownerFenceToken,
              reservationFenceToken,
              state: "admitted",
              activity: "active",
              activityObservedAtMs: nowMs,
            };
      state = {
        ...state,
        nextSequence: existing === undefined ? state.nextSequence + 1 : state.nextSequence,
        nextReservationFenceToken: reservationFenceToken + 1,
        reservations: { ...state.reservations, [request.requestId]: reservation },
      };
      return {
        state,
        value: {
          reservationId: request.requestId,
          ownerId: request.ownerId,
          ownerFenceToken: request.ownerFenceToken,
          reservationFenceToken,
        } satisfies ResourceReservationAuthority,
      };
    });
    if (transaction.value === null) {
      return yield* new ResourceAdmissionError({
        operation: "adopt-active",
        reason: "invalid-authority",
        message: "Active resource reconciliation was rejected by a current owner or identity.",
      });
    }
    return transaction.value;
  });

  const refreshNow = Effect.fn("resourceAdmission.refresh")(function* () {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      const scheduled = schedule({
        state: updatePressure(cloneState(persisted), sample, settings, settingsOwnerId),
        sample,
        settings,
        nowMs,
      });
      return { state: scheduled.state, value: scheduled.grants };
    });
    return transaction.value;
  });
  const refresh = refreshNow();

  const cancelWaiting = Effect.fn("resourceAdmission.cancelWaiting")(function* (
    authority: Pick<ResourceReservationAuthority, "reservationId" | "ownerId" | "ownerFenceToken">,
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const reservation = state.reservations[authority.reservationId];
      if (
        reservation === undefined ||
        !(
          reservation.state === "waiting" ||
          (reservation.state === "admitted" && reservation.activity === "possible")
        ) ||
        reservation.ownerId !== authority.ownerId ||
        reservation.ownerFenceToken !== authority.ownerFenceToken
      ) {
        return { state, value: { result: false, newlyAdmitted: [] } };
      }
      state = {
        ...state,
        reservations: {
          ...state.reservations,
          [authority.reservationId]: {
            ...reservation,
            state: "canceled",
            activity: "inactive",
            activityObservedAtMs: nowMs,
          },
        },
      };
      const scheduled = schedule({ state, sample, settings, nowMs });
      return {
        state: scheduled.state,
        value: { result: true, newlyAdmitted: scheduled.grants },
      };
    });
    return {
      ...transaction.value,
      ledgerRevision: transaction.revision,
      pressureSampledAtMs: sample.sampledAtMs,
    };
  });

  const release = Effect.fn("resourceAdmission.release")(function* (
    authority: ResourceReservationAuthority,
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const reservation = state.reservations[authority.reservationId];
      if (
        reservation === undefined ||
        reservation.state !== "admitted" ||
        !validAuthority(reservation, authority)
      ) {
        return { state, value: { result: false, newlyAdmitted: [] } };
      }
      const hasActiveChildren = Object.values(state.reservations).some(
        (entry) =>
          entry.parentReservationId === reservation.requestId &&
          (entry.state === "waiting" || entry.state === "admitted"),
      );
      if (hasActiveChildren) {
        return {
          state,
          value: {
            result: false,
            newlyAdmitted: [],
          },
        };
      }
      state = {
        ...state,
        reservations: {
          ...state.reservations,
          [authority.reservationId]: {
            ...reservation,
            state: "released",
            activity: "inactive",
            activityObservedAtMs: nowMs,
          },
        },
      };
      const scheduled = schedule({ state, sample, settings, nowMs });
      return {
        state: scheduled.state,
        value: { result: true, newlyAdmitted: scheduled.grants },
      };
    });
    return {
      ...transaction.value,
      ledgerRevision: transaction.revision,
      pressureSampledAtMs: sample.sampledAtMs,
    };
  });

  const defer = Effect.fn("resourceAdmission.defer")(function* (
    authority: ResourceReservationAuthority,
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const reservation = state.reservations[authority.reservationId];
      if (
        reservation === undefined ||
        reservation.state !== "admitted" ||
        reservation.activity !== "possible" ||
        !validAuthority(reservation, authority)
      ) {
        return { state, value: { result: false, newlyAdmitted: [] } };
      }
      state = {
        ...state,
        reservations: {
          ...state.reservations,
          [authority.reservationId]: {
            ...reservation,
            state: "waiting",
            admittedAtMs: null,
            reservationFenceToken: null,
            activity: "unknown",
            activityObservedAtMs: nowMs,
          },
        },
      };
      const scheduled = schedule({ state, sample, settings, nowMs });
      return {
        state: scheduled.state,
        value: { result: true, newlyAdmitted: scheduled.grants },
      };
    });
    return {
      ...transaction.value,
      ledgerRevision: transaction.revision,
      pressureSampledAtMs: sample.sampledAtMs,
    };
  });

  const observeActivity = Effect.fn("resourceAdmission.observeActivity")(function* (
    authority: ResourceReservationAuthority,
    activity: "active" | "inactive" | "unknown",
  ) {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    const transaction = yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const reservation = state.reservations[authority.reservationId];
      if (
        reservation === undefined ||
        reservation.state !== "admitted" ||
        !validAuthority(reservation, authority)
      ) {
        return { state, value: { result: false, newlyAdmitted: [] } };
      }
      if (activity === "inactive") {
        const hasActiveChildren = Object.values(state.reservations).some(
          (entry) =>
            entry.parentReservationId === reservation.requestId &&
            (entry.state === "waiting" || entry.state === "admitted"),
        );
        if (hasActiveChildren) {
          return { state, value: { result: false, newlyAdmitted: [] } };
        }
      }
      state = {
        ...state,
        reservations: {
          ...state.reservations,
          [authority.reservationId]: {
            ...reservation,
            state: activity === "inactive" ? "released" : reservation.state,
            activity,
            activityObservedAtMs: nowMs,
          },
        },
      };
      const scheduled = schedule({ state, sample, settings, nowMs });
      return {
        state: scheduled.state,
        value: { result: true, newlyAdmitted: scheduled.grants },
      };
    });
    return {
      ...transaction.value,
      ledgerRevision: transaction.revision,
      pressureSampledAtMs: sample.sampledAtMs,
    };
  });

  const acquire = Effect.fn("resourceAdmission.acquire")((input: ResourceAdmissionRequest) =>
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        while (true) {
          const update = yield* request(input);
          if (update.result._tag !== "Waiting") return update.result;
          const revision = update.ledgerRevision;
          if (revision === null) {
            return {
              _tag: "Rejected" as const,
              requestId: input.requestId,
              reason: "invalid-request" as const,
              message: "Waiting admission did not persist a ledger revision.",
            };
          }
          const awaitBackgroundAging =
            input.priority === "background" && update.result.reason === "interactive-priority"
              ? Effect.gen(function* () {
                  const [state, configuredSettings, now] = yield* Effect.all([
                    options.ledger.read,
                    currentSettings,
                    DateTime.now,
                  ]);
                  const reservation = state.reservations[input.requestId];
                  if (reservation === undefined || reservation.state !== "waiting") return;
                  const delayMs =
                    reservation.requestedAtMs +
                    effectiveSettings(state, configuredSettings).backgroundMaxGrantDelayMs -
                    DateTime.toEpochMillis(now);
                  if (delayMs <= 0) return yield* Effect.never;
                  yield* Effect.sleep(delayMs);
                })
              : Effect.never;
          yield* Effect.race(
            Effect.race(
              options.ledger.awaitChange(revision),
              pressure.awaitChange(update.pressureSampledAtMs),
            ),
            awaitBackgroundAging,
          ).pipe(
            restore,
            Effect.onInterrupt(() =>
              cancelWaiting({
                reservationId: input.requestId,
                ownerId: input.ownerId,
                ownerFenceToken: input.ownerFenceToken,
              }).pipe(Effect.ignore),
            ),
          );
        }
      }),
    ),
  );

  const snapshot = Effect.gen(function* () {
    const configuredSettings = yield* currentSettings;
    const state = yield* options.ledger.read;
    const settings = effectiveSettings(state, configuredSettings);
    const entries: ResourceAdmissionSnapshotEntry[] = Object.values(state.reservations)
      .sort((left, right) => left.sequence - right.sequence)
      .map((reservation) => ({
        requestId: reservation.requestId,
        kind: reservation.kind,
        priority: reservation.priority,
        accountScope: reservation.accountScope,
        state: reservation.state,
        waitReason:
          reservation.state === "waiting"
            ? reservation.priority === "background"
              ? (backgroundPressureReason(state, settings) ?? capacityReason(reservation.kind))
              : capacityReason(reservation.kind)
            : null,
        accounted: reservation.accounted,
        gpuRequired: reservation.gpuRequired,
        ownerId: reservation.ownerId,
        ownerFenceToken: reservation.ownerFenceToken,
        executionKey: reservation.executionKey,
        reservationFenceToken: reservation.reservationFenceToken,
        requestedAtMs: reservation.requestedAtMs,
        admittedAtMs: reservation.admittedAtMs,
        activity: reservation.activity,
        activityObservedAtMs: reservation.activityObservedAtMs,
        parentReservationId: reservation.parentReservationId,
      }));
    return {
      entries,
      pressure: state.pressure,
      enforcement: {
        slotAccounting: "host-process-atomic",
        cpu: "observed-soft-threshold",
        memory: "observed-soft-threshold",
        gpu: settings.gpuMaxConcurrent > 0 ? "slot-accounting-only" : "unavailable",
        osHardLimits: "unsupported",
      },
      effectiveSettings: state.effectiveSettings,
    } satisfies ResourceAdmissionSnapshot;
  });

  // A dead server cannot later start work which was only waiting or had not
  // crossed an external boundary. Replayable provider requests can pass to a
  // newer fenced owner. Local checks and per-claim provider requests are
  // terminal because their durable owner retries under a new identity.
  // Active work stays accounted because its provider/child tree may survive.
  yield* Effect.gen(function* () {
    const settings = yield* currentSettings;
    const { sample, nowMs } = yield* sampleNow();
    yield* options.ledger.transact((persisted) => {
      let state = updatePressure(cloneState(persisted), sample, settings, settingsOwnerId);
      const reservations = { ...state.reservations };
      for (const reservation of Object.values(reservations)) {
        if (
          !ownerProcessIsDead(reservation.ownerId) ||
          (reservation.state !== "waiting" && reservation.state !== "admitted")
        ) {
          continue;
        }
        if (reservation.state === "waiting" || reservation.activity === "possible") {
          const terminal = reservation.kind === "localCheck" || reservation.replayable === false;
          reservations[reservation.requestId] = {
            ...reservation,
            state: terminal ? "canceled" : "waiting",
            admittedAtMs: null,
            reservationFenceToken: null,
            activity: terminal ? "inactive" : "unknown",
            activityObservedAtMs: nowMs,
          };
        } else if (
          reservation.activity === "active" ||
          reservation.activity === "orphaned-active"
        ) {
          reservations[reservation.requestId] = {
            ...reservation,
            activity: "orphaned-active",
            activityObservedAtMs: nowMs,
          };
        } else if (
          reservation.activity === "unknown" ||
          reservation.activity === "orphaned-unknown"
        ) {
          reservations[reservation.requestId] = {
            ...reservation,
            activity: "orphaned-unknown",
            activityObservedAtMs: nowMs,
          };
        }
      }
      state = { ...state, reservations };
      const scheduled = schedule({ state, sample, settings, nowMs });
      return { state: scheduled.state, value: undefined };
    });
  });

  return ResourceAdmission.of({
    adoptActive,
    acquire,
    request,
    cancelWaiting,
    release,
    defer,
    observeActivity,
    refresh,
    snapshot,
  });
});

export function layer(options: {
  readonly hostBudgetPath: string;
  readonly settings?: ResourceAdmissionSettings;
  readonly readSettings?: Effect.Effect<ResourceAdmissionSettings>;
}) {
  return Layer.effect(
    ResourceAdmission,
    make({
      ledger: makeFileHostBudgetLedger(options.hostBudgetPath),
      ...(options.settings === undefined ? {} : { settings: options.settings }),
      ...(options.readSettings === undefined ? {} : { readSettings: options.readSettings }),
    }),
  );
}
