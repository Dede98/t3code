import { EventId, ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { ProviderAdmissionRuntime } from "../agentControl/providerAdmission/Services/ProviderAdmissionRuntime.ts";
import type {
  ProviderResourceAdmissionActive,
  ProviderResourceAdmissionPermit,
} from "../agentControl/providerAdmission/model.ts";
import { layerTest as serverSettingsTest } from "../serverSettings.ts";
import {
  ProviderResourceCoordinator,
  layer as coordinatorLayer,
} from "./ProviderResourceCoordinator.ts";
import { ResourceAdmission } from "./ResourceAdmission.ts";
import type { ResourceReservationAuthority } from "./model.ts";

it.effect("coordinates both authorities and ignores unfenced session exits", () =>
  Effect.gen(function* () {
    const instanceId = ProviderInstanceId.make("codex-shared-admission");
    const providerPermit: ProviderResourceAdmissionPermit = {
      requestId: "provider-request",
      idempotencyKey: "manual:event-1",
      providerInstanceId: instanceId,
      threadId: "thread-1",
      accountScope: "codex",
      workloadClass: "interactive",
      source: "manual",
      requestedAt: "2026-09-16T10:00:00.000Z",
      stage: null,
      handoffId: null,
      ownerId: "provider-owner",
      leaseExpiresAt: "2099-01-01T00:00:00.000Z",
      fenceToken: 7,
    };
    const hostAuthority: ResourceReservationAuthority = {
      reservationId: "host:provider-request",
      ownerId: "provider-owner",
      ownerFenceToken: 7,
      reservationFenceToken: 11,
    };
    const calls: string[] = [];
    let activeRows: ReadonlyArray<ProviderResourceAdmissionActive> = [];
    const provider = ProviderAdmissionRuntime.of({
      awaitFailure: Effect.never,
      request: () => Effect.die("unused"),
      usageChanged: () => Effect.die("unused"),
      capacityReleased: () => Effect.void,
      requestResource: () =>
        Effect.sync(() => {
          calls.push("provider-request");
          return {
            _tag: "Waiting",
            requestId: providerPermit.requestId,
            reason: "provider-limit",
            retryAt: null,
          } as const;
        }),
      acquireResource: () =>
        Effect.sync(() => {
          calls.push("provider-acquire");
          return providerPermit;
        }),
      enterResource: (_permit, turnId) =>
        Effect.sync(() => calls.push(`provider-enter:${turnId ?? "pending"}`)),
      releaseResource: () => Effect.sync(() => calls.push("provider-release")),
      deferResource: () => Effect.sync(() => calls.push("provider-defer")),
      cancelResource: () => Effect.void,
      configureResourceScope: () => Effect.void,
      listResourceActive: Effect.sync(() => activeRows),
      reconcileResource: (_requestId, activity) =>
        Effect.sync(() => {
          calls.push(`provider-reconcile:${activity}`);
          return activity === "active" ? providerPermit : null;
        }),
    });
    const host = ResourceAdmission.of({
      request: () =>
        Effect.sync(() => {
          calls.push("host-request");
          return {
            result: { _tag: "Admitted", authority: hostAuthority, accounted: true },
            newlyAdmitted: [],
            ledgerRevision: 1,
            pressureSampledAtMs: 1,
          } as const;
        }),
      acquire: () =>
        Effect.succeed({ _tag: "Admitted", authority: hostAuthority, accounted: true }),
      adoptActive: () =>
        Effect.sync(() => {
          calls.push("host-adopt");
          return hostAuthority;
        }),
      cancelWaiting: () => Effect.die("unused"),
      release: () =>
        Effect.sync(() => {
          calls.push("host-release");
          return {
            result: true,
            newlyAdmitted: [],
            ledgerRevision: 2,
            pressureSampledAtMs: 1,
          };
        }),
      defer: () =>
        Effect.sync(() => {
          calls.push("host-defer");
          return {
            result: true,
            newlyAdmitted: [],
            ledgerRevision: 2,
            pressureSampledAtMs: 1,
          };
        }),
      observeActivity: (_authority, activity) =>
        Effect.sync(() => {
          calls.push(`host-${activity}`);
          return {
            result: true,
            newlyAdmitted: [],
            ledgerRevision: 2,
            pressureSampledAtMs: 1,
          };
        }),
      refresh: Effect.succeed([]),
      snapshot: Effect.die("unused"),
    });
    const testLayer = coordinatorLayer.pipe(
      Layer.provide(Layer.succeed(ProviderAdmissionRuntime, provider)),
      Layer.provide(Layer.succeed(ResourceAdmission, host)),
      Layer.provide(serverSettingsTest()),
    );
    const coordinator = yield* ProviderResourceCoordinator.pipe(Effect.provide(testLayer));
    const permit = yield* coordinator.acquire({
      idempotencyKey: "manual:event-1",
      providerInstanceId: instanceId,
      continuationKey: "codex",
      threadId: "thread-1",
      requestedAt: "2026-09-16T10:00:00.000Z",
      workloadClass: "interactive",
      source: "manual",
    });
    yield* coordinator.enter(permit);
    const callsBeforeStaleEvents = [...calls];
    yield* coordinator.observeRuntimeEvent({
      type: "turn.completed",
      eventId: EventId.make("old-turn-terminal"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: instanceId,
      threadId: ThreadId.make("thread-1"),
      turnId: "old-turn",
      createdAt: "2026-09-16T10:00:01.000Z",
      payload: { state: "completed" },
    });
    yield* coordinator.observeRuntimeEvent(
      {
        type: "turn.started",
        eventId: EventId.make("old-turn-start"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: ThreadId.make("thread-1"),
        turnId: "old-turn",
        createdAt: "2026-09-16T10:00:02.000Z",
        payload: {},
      },
      {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        status: "running",
        runtimeMode: "approval-required",
        threadId: ThreadId.make("thread-1"),
        activeTurnId: "turn-1",
        createdAt: "2026-09-16T10:00:00.000Z",
        updatedAt: "2026-09-16T10:00:02.000Z",
      },
    );
    assert.deepStrictEqual(calls, callsBeforeStaleEvents);
    yield* coordinator.observeRuntimeEvent(
      {
        type: "turn.started",
        eventId: EventId.make("current-turn-start"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: ThreadId.make("thread-1"),
        turnId: "turn-1",
        createdAt: "2026-09-16T10:00:01.000Z",
        payload: {},
      },
      {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        status: "running",
        runtimeMode: "approval-required",
        threadId: ThreadId.make("thread-1"),
        activeTurnId: "turn-1",
        createdAt: "2026-09-16T10:00:00.000Z",
        updatedAt: "2026-09-16T10:00:01.000Z",
      },
    );
    yield* coordinator.observeRuntimeEvent({
      type: "session.exited",
      eventId: EventId.make("old-session-exit"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: instanceId,
      threadId: ThreadId.make("thread-1"),
      // Session exits carry no generation. Even a later timestamp must not
      // release a replacement reservation on the same provider instance.
      createdAt: "2026-09-16T10:01:00.000Z",
      payload: {},
    });
    yield* coordinator.release(permit);
    assert.deepStrictEqual(calls, [
      "host-request",
      "provider-request",
      "host-defer",
      "provider-acquire",
      "host-request",
      "host-active",
      "provider-enter:pending",
      "host-active",
      "provider-enter:turn-1",
      "provider-release",
      "host-release",
    ]);

    calls.length = 0;
    activeRows = [
      {
        ...providerPermit,
        providerTurnId: null,
        status: "entered",
        waitReason: null,
        lastObservedActivity: "unknown",
        lastObservedAt: "2026-09-16T10:02:00.000Z",
        permit: providerPermit,
      },
    ];
    yield* coordinator.reconcile([]);
    assert.deepStrictEqual(calls, ["provider-reconcile:unknown", "host-adopt"]);

    calls.length = 0;
    yield* coordinator.reconcile([
      {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        status: "running",
        runtimeMode: "approval-required",
        threadId: ThreadId.make("thread-1"),
        activeTurnId: "turn-after-restart",
        createdAt: "2026-09-16T10:00:00.000Z",
        updatedAt: "2026-09-16T10:03:00.000Z",
      },
    ]);
    yield* coordinator.observeRuntimeEvent({
      type: "turn.completed",
      eventId: EventId.make("terminal-after-restart"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: instanceId,
      threadId: ThreadId.make("thread-1"),
      turnId: "turn-after-restart",
      createdAt: "2026-09-16T10:04:00.000Z",
      payload: { state: "completed" },
    });
    assert.deepStrictEqual(calls, [
      "provider-reconcile:active",
      "host-adopt",
      "host-active",
      "provider-enter:turn-after-restart",
      "provider-release",
      "host-release",
    ]);

    calls.length = 0;
    yield* coordinator.observeRuntimeEvent({
      type: "turn.completed",
      eventId: EventId.make("completion-without-start-after-restart"),
      provider: ProviderDriverKind.make("codex"),
      providerInstanceId: instanceId,
      threadId: ThreadId.make("thread-1"),
      turnId: "turn-whose-start-was-lost",
      createdAt: "2026-09-16T10:07:00.000Z",
      payload: { state: "completed" },
    });
    assert.deepStrictEqual(calls, []);
    yield* coordinator.observeRuntimeEvent(
      {
        type: "turn.started",
        eventId: EventId.make("delayed-start-after-completion"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        threadId: ThreadId.make("thread-1"),
        turnId: "turn-whose-start-was-lost",
        createdAt: "2026-09-16T10:08:00.000Z",
        payload: {},
      },
      {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: instanceId,
        status: "running",
        runtimeMode: "approval-required",
        threadId: ThreadId.make("thread-1"),
        activeTurnId: "turn-whose-start-was-lost",
        createdAt: "2026-09-16T10:00:00.000Z",
        updatedAt: "2026-09-16T10:08:00.000Z",
      },
    );
    // Persisted unbound rows are never adopted from an event alone. They need
    // startup reconciliation with an active session snapshot.
    assert.deepStrictEqual(calls, []);
  }),
);
