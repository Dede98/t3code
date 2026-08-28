// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import type {
  ProviderApprovalDecision,
  ProviderRuntimeEvent,
  ProviderSendTurnInput,
  ProviderSession,
  ProviderSessionStartInput,
  ProviderTurnStartResult,
} from "@t3tools/contracts";
import {
  ApprovalRequestId,
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import { it, assert, vi } from "@effect/vitest";

import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderUnsupportedError,
  ProviderValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import {
  attestProviderNativeTurnConfiguration,
  attestProviderSessionNativeConfiguration,
  ProviderContinuationSyncCapabilityError,
  type ProviderAdapterShape,
} from "../Services/ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "../Services/ProviderAdapterRegistry.ts";
import * as ProviderService from "../Services/ProviderService.ts";
import * as ProviderSessionDirectory from "../Services/ProviderSessionDirectory.ts";
import { ProviderThreadOperationLock } from "../Services/ProviderThreadOperationLock.ts";
import { correlateRuntimeEventWithInstance, makeProviderServiceLive } from "./ProviderService.ts";
import * as ProviderEventLoggers from "./ProviderEventLoggers.ts";
import { makeEventNdjsonLogger } from "./EventNdjsonLogger.ts";
import { ProviderSessionDirectoryLive } from "./ProviderSessionDirectory.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ProviderSessionRuntime from "../../persistence/ProviderSessionRuntime.ts";
import {
  makeSqlitePersistenceLive,
  SqlitePersistenceMemory,
} from "../../persistence/Layers/Sqlite.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as AnalyticsService from "../../telemetry/AnalyticsService.ts";
import { makeAdapterRegistryMock } from "../testUtils/providerAdapterRegistryMock.ts";

const encodeUnknownJsonString = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);
const decodeUnknownJsonString = Schema.decodeUnknownSync(Schema.UnknownFromJsonString);
import { makeReactorStartupAttempt } from "../../reactorStartupActivation.ts";

const defaultServerSettingsLayer = ServerSettings.ServerSettingsService.layerTest();

const asRequestId = (value: string): ApprovalRequestId => ApprovalRequestId.make(value);
const asEventId = (value: string): EventId => EventId.make(value);
const asThreadId = (value: string): ThreadId => ThreadId.make(value);
const asTurnId = (value: string): TurnId => TurnId.make(value);
const codexInstanceId = ProviderInstanceId.make("codex");
const claudeAgentInstanceId = ProviderInstanceId.make("claudeAgent");
const CODEX_DRIVER = ProviderDriverKind.make("codex");
const CLAUDE_AGENT_DRIVER = ProviderDriverKind.make("claudeAgent");
const CURSOR_DRIVER = ProviderDriverKind.make("cursor");

type LegacyProviderRuntimeEvent = {
  readonly type: string;
  readonly eventId: EventId;
  readonly provider: ProviderDriverKind;
  readonly createdAt: string;
  readonly threadId: ThreadId;
  readonly turnId?: string | undefined;
  readonly itemId?: string | undefined;
  readonly requestId?: string | undefined;
  readonly payload?: unknown | undefined;
  readonly [key: string]: unknown;
};

function makeFakeCodexAdapter(
  provider: ProviderDriverKind = CODEX_DRIVER,
  options?: {
    readonly omitGeneratedResumeCursor?: boolean;
    readonly providerInstanceId?: ProviderInstanceId;
    readonly runtimeEventStream?: Stream.Stream<ProviderRuntimeEvent>;
  },
) {
  const providerInstanceId =
    options?.providerInstanceId ?? ProviderInstanceId.make(String(provider));
  const sessions = new Map<ThreadId, ProviderSession>();
  const runtimeEventPubSub = Effect.runSync(PubSub.unbounded<ProviderRuntimeEvent>());

  const startSession = vi.fn((input: ProviderSessionStartInput) =>
    Effect.sync(() => {
      if (input.providerInstanceId !== providerInstanceId) {
        throw new Error(
          `Expected provider instance '${providerInstanceId}' but received '${input.providerInstanceId}'.`,
        );
      }
      const now = "2026-01-01T00:00:00.000Z";
      const session: ProviderSession = {
        provider,
        providerInstanceId,
        status: "ready",
        runtimeMode: input.runtimeMode,
        threadId: input.threadId,
        ...(input.resumeCursor !== undefined
          ? { resumeCursor: input.resumeCursor }
          : options?.omitGeneratedResumeCursor
            ? {}
            : { resumeCursor: { opaque: `resume-${String(input.threadId)}` } }),
        cwd: input.cwd ?? process.cwd(),
        ...(input.modelSelection === undefined ? {} : { model: input.modelSelection.model }),
        createdAt: now,
        updatedAt: now,
      };
      sessions.set(session.threadId, session);
      return attestProviderSessionNativeConfiguration(session, input.modelSelection ?? null);
    }),
  );

  const sendTurn = vi.fn(
    (
      input: ProviderSendTurnInput,
    ): Effect.Effect<ProviderTurnStartResult, ProviderAdapterError> => {
      if (!sessions.has(input.threadId)) {
        return Effect.fail(
          new ProviderAdapterSessionNotFoundError({
            provider,
            threadId: input.threadId,
          }),
        );
      }

      return Effect.succeed({
        threadId: input.threadId,
        turnId: TurnId.make(`turn-${String(input.threadId)}`),
      });
    },
  );
  const defaultPrepareTurnImplementation: NonNullable<
    ProviderAdapterShape<ProviderAdapterError>["prepareTurn"]
  > = (input) => {
    if (input.modelSelection === undefined) {
      return Effect.fail(
        new ProviderAdapterRequestError({
          provider: String(provider),
          method: "thread.turn.start",
          detail: "model selection required",
        }),
      );
    }
    return Effect.succeed({
      attestation: attestProviderNativeTurnConfiguration(input.modelSelection),
      invoke: (entry) =>
        entry.adapterEntered().pipe(Effect.andThen(entry.startExternal(() => sendTurn(input)))),
    });
  };
  let prepareTurnImplementation = defaultPrepareTurnImplementation;
  const prepareTurn: NonNullable<ProviderAdapterShape<ProviderAdapterError>["prepareTurn"]> = (
    input,
  ) => prepareTurnImplementation(input);

  const interruptTurn = vi.fn(
    (_threadId: ThreadId, _turnId?: TurnId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.void,
  );

  const respondToRequest = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _decision: ProviderApprovalDecision,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const respondToUserInput = vi.fn(
    (
      _threadId: ThreadId,
      _requestId: string,
      _answers: Record<string, unknown>,
    ): Effect.Effect<void, ProviderAdapterError> => Effect.void,
  );

  const stopSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.delete(threadId);
      }),
  );

  const listSessions = vi.fn(
    (): Effect.Effect<ReadonlyArray<ProviderSession>> =>
      Effect.sync(() => Array.from(sessions.values())),
  );

  const hasSession = vi.fn(
    (threadId: ThreadId): Effect.Effect<boolean> => Effect.succeed(sessions.has(threadId)),
  );

  const readThread = vi.fn(
    (
      threadId: ThreadId,
    ): Effect.Effect<
      {
        threadId: ThreadId;
        turns: ReadonlyArray<{ id: TurnId; items: readonly [] }>;
      },
      ProviderAdapterError
    > =>
      Effect.succeed({
        threadId,
        turns: [{ id: asTurnId("turn-1"), items: [] }],
      }),
  );

  const rollbackThread = vi.fn(
    (
      threadId: ThreadId,
      _numTurns: number,
    ): Effect.Effect<{ threadId: ThreadId; turns: readonly [] }, ProviderAdapterError> =>
      Effect.succeed({ threadId, turns: [] }),
  );

  const stopAll = vi.fn(
    (): Effect.Effect<void, ProviderAdapterError> =>
      Effect.sync(() => {
        sessions.clear();
      }),
  );

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    prepareTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
    get streamEvents() {
      return options?.runtimeEventStream ?? Stream.fromPubSub(runtimeEventPubSub);
    },
  };

  const emit = (event: LegacyProviderRuntimeEvent): void => {
    const canonicalEvent = {
      ...event,
      providerInstanceId,
    } as unknown as ProviderRuntimeEvent;
    Effect.runSync(PubSub.publish(runtimeEventPubSub, canonicalEvent));
  };

  const updateSession = (
    threadId: ThreadId,
    update: (session: ProviderSession) => ProviderSession,
  ): void => {
    const existing = sessions.get(threadId);
    if (!existing) {
      return;
    }
    sessions.set(threadId, update(existing));
  };

  return {
    adapter,
    emit,
    updateSession,
    setPrepareTurn: (
      implementation: NonNullable<ProviderAdapterShape<ProviderAdapterError>["prepareTurn"]>,
    ) => {
      prepareTurnImplementation = implementation;
    },
    resetPrepareTurn: () => {
      prepareTurnImplementation = defaultPrepareTurnImplementation;
    },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    readThread,
    rollbackThread,
    stopAll,
  };
}

function makeCompatibleInstanceRegistry(input: {
  readonly driverKind: ProviderDriverKind;
  readonly continuationKey: string;
  readonly adapters: ReadonlyMap<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>;
}): ProviderAdapterRegistry.ProviderAdapterRegistryShape {
  const unsupported = (instanceId: ProviderInstanceId) =>
    new ProviderUnsupportedError({ provider: ProviderDriverKind.make(instanceId) });

  return {
    getByInstance: (instanceId) => {
      const adapter = input.adapters.get(instanceId);
      return adapter ? Effect.succeed(adapter) : Effect.fail(unsupported(instanceId));
    },
    getInstanceInfo: (instanceId) =>
      input.adapters.has(instanceId)
        ? Effect.succeed({
            instanceId,
            driverKind: input.driverKind,
            displayName: undefined,
            enabled: true,
            continuationIdentity: {
              driverKind: input.driverKind,
              continuationKey: input.continuationKey,
            },
          })
        : Effect.fail(unsupported(instanceId)),
    listInstances: () => Effect.succeed(Array.from(input.adapters.keys())),
    listProviders: () => Effect.succeed([input.driverKind]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), PubSub.subscribe),
  };
}

const advanceTestClock = (ms: number) =>
  TestClock.adjust(`${ms} millis`).pipe(Effect.andThen(Effect.yieldNow));

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

function makeProviderServiceLayer(
  options?: Parameters<typeof makeProviderServiceLive>[0],
  adapterOverrides?: {
    readonly codex?: ReturnType<typeof makeFakeCodexAdapter>;
  },
) {
  const codex = adapterOverrides?.codex ?? makeFakeCodexAdapter(CODEX_DRIVER);
  const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
  const cursor = makeFakeCodexAdapter(CURSOR_DRIVER);
  const baseRegistry = makeAdapterRegistryMock({
    [ProviderDriverKind.make("codex")]: codex.adapter,
    [ProviderDriverKind.make("claudeAgent")]: claude.adapter,
    [ProviderDriverKind.make("cursor")]: cursor.adapter,
  });
  const routedInstances: ProviderInstanceId[] = [];
  const registry: ProviderAdapterRegistry.ProviderAdapterRegistryShape = {
    ...baseRegistry,
    getByInstance: (instanceId) =>
      Effect.sync(() => {
        routedInstances.push(instanceId);
      }).pipe(Effect.andThen(baseRegistry.getByInstance(instanceId))),
  };

  const providerAdapterLayer = Layer.succeed(
    ProviderAdapterRegistry.ProviderAdapterRegistry,
    registry,
  );
  const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
    Layer.provide(SqlitePersistenceMemory),
  );
  const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

  const layer = it.layer(
    Layer.mergeAll(
      makeProviderServiceLive(options).pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,

      runtimeRepositoryLayer,
      NodeServices.layer,
    ),
  );

  return {
    codex,
    claude,
    cursor,
    routedInstances,
    layer,
  };
}

it.effect("ProviderServiceLive catches stopAll failures during shutdown", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    codex.stopAll.mockImplementation(() =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: String(CODEX_DRIVER),
          method: "stopAll",
          detail: "simulated stopAll failure",
        }),
      ),
    );
    const registry = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
    });
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = Layer.mergeAll(
      makeProviderServiceLive().pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,
      runtimeRepositoryLayer,
      NodeServices.layer,
    );
    const scope = yield* Scope.make();
    const runtimeServices = yield* Layer.build(providerLayer).pipe(Scope.provide(scope));

    yield* ProviderService.ProviderService.pipe(Effect.provide(runtimeServices));
    const closeExit = yield* Scope.close(scope, Exit.void).pipe(Effect.exit);

    assert.equal(Exit.isSuccess(closeExit), true);
    assert.equal(codex.stopAll.mock.calls.length, 1);
  }),
);

it.effect("ProviderServiceLive stopAll continues past a quarantined legacy binding", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const registry = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
    });
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = Layer.mergeAll(
      makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provideMerge(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,
      runtimeRepositoryLayer,
      NodeServices.layer,
    );
    const scope = yield* Scope.make();
    const runtimeServices = yield* Layer.build(providerLayer).pipe(Scope.provide(scope));
    const provider = yield* ProviderService.ProviderService.pipe(Effect.provide(runtimeServices));
    const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository.pipe(
      Effect.provide(runtimeServices),
    );
    const healthyThreadId = asThreadId("thread-stop-all-healthy");

    yield* provider.startSession(healthyThreadId, {
      provider: CODEX_DRIVER,
      providerInstanceId: codexInstanceId,
      threadId: healthyThreadId,
      runtimeMode: "full-access",
    });
    yield* runtimeRepository.upsert({
      threadId: asThreadId("thread-stop-all-legacy"),
      providerName: "codex",
      providerInstanceId: null,
      adapterKey: "codex",
      runtimeMode: "full-access",
      status: "running",
      lastSeenAt: "2026-01-01T00:00:00.000Z",
      resumeCursor: null,
      runtimePayload: null,
    });

    const closeExit = yield* Scope.close(scope, Exit.void).pipe(Effect.exit);

    assert.equal(Exit.isSuccess(closeExit), true);
    assert.equal(codex.stopAll.mock.calls.length, 1);
  }),
);

it.effect("ProviderServiceLive rejects new sessions for disabled providers", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const claude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
    const registryBase = makeAdapterRegistryMock({
      [CODEX_DRIVER]: codex.adapter,
      [CLAUDE_AGENT_DRIVER]: claude.adapter,
    });
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      ...registryBase,
      getInstanceInfo: (instanceId) =>
        instanceId === claudeAgentInstanceId
          ? Effect.succeed({
              instanceId,
              driverKind: CLAUDE_AGENT_DRIVER,
              displayName: undefined,
              enabled: false,
              continuationIdentity: {
                driverKind: CLAUDE_AGENT_DRIVER,
                continuationKey: "claudeAgent:instance:claudeAgent",
              },
            })
          : registryBase.getInstanceInfo(instanceId),
    };
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(providerAdapterLayer),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    const failure = yield* Effect.flip(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-disabled"), {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-disabled"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer)),
    );

    assert.instanceOf(failure, ProviderValidationError);
    assert.include(failure.issue, "Provider instance 'claudeAgent' is disabled");
    assert.equal(claude.startSession.mock.calls.length, 0);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive allows enabled custom instances when legacy driver is disabled",
  () =>
    Effect.gen(function* () {
      const instanceId = ProviderInstanceId.make("codex_personal");
      const driverKind = CODEX_DRIVER;
      const codex = makeFakeCodexAdapter(CODEX_DRIVER, { providerInstanceId: instanceId });
      const unsupported = () =>
        new ProviderUnsupportedError({
          provider: driverKind,
        });
      const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
        getByInstance: (requestedInstanceId) =>
          requestedInstanceId === instanceId
            ? Effect.succeed(codex.adapter)
            : Effect.fail(unsupported()),
        getInstanceInfo: (requestedInstanceId) =>
          requestedInstanceId === instanceId
            ? Effect.succeed({
                instanceId,
                driverKind,
                displayName: "Codex Personal",
                enabled: true,
                continuationIdentity: {
                  driverKind,
                  continuationKey: "codex:/Users/example/.codex",
                },
              })
            : Effect.fail(unsupported()),
        listInstances: () => Effect.succeed([instanceId]),
        listProviders: () => Effect.succeed([driverKind] as const),
        streamChanges: Stream.empty,
        subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
          PubSub.subscribe(pubsub),
        ),
      };
      const providerAdapterLayer = Layer.succeed(
        ProviderAdapterRegistry.ProviderAdapterRegistry,
        registry,
      );
      const serverSettingsLayer = ServerSettings.ServerSettingsService.layerTest({
        providers: {
          codex: {
            enabled: false,
          },
        },
      });
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(providerAdapterLayer),
        Layer.provide(directoryLayer),
        Layer.provide(serverSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      const session = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-enabled-custom"), {
          provider: driverKind,
          providerInstanceId: instanceId,
          threadId: asThreadId("thread-enabled-custom"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer));

      assert.equal(session.providerInstanceId, instanceId);
      assert.equal(codex.startSession.mock.calls.length, 1);
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive rejects new sessions for disabled custom instances", () =>
  Effect.gen(function* () {
    const instanceId = ProviderInstanceId.make("codex_personal");
    const driverKind = ProviderDriverKind.make("codex");
    const codex = makeFakeCodexAdapter();
    const unsupported = () =>
      new ProviderUnsupportedError({
        provider: ProviderDriverKind.make("codex"),
      });
    const registry: ProviderAdapterRegistry.ProviderAdapterRegistry["Service"] = {
      getByInstance: (requestedInstanceId) =>
        requestedInstanceId === instanceId
          ? Effect.succeed(codex.adapter)
          : Effect.fail(unsupported()),
      getInstanceInfo: (requestedInstanceId) =>
        requestedInstanceId === instanceId
          ? Effect.succeed({
              instanceId,
              driverKind,
              displayName: "Codex Personal",
              enabled: false,
              continuationIdentity: {
                driverKind,
                continuationKey: "codex:/Users/example/.codex",
              },
            })
          : Effect.fail(unsupported()),
      listInstances: () => Effect.succeed([instanceId]),
      listProviders: () => Effect.succeed([CODEX_DRIVER] as const),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.flatMap(PubSub.unbounded<void>(), (pubsub) =>
        PubSub.subscribe(pubsub),
      ),
    };
    const providerAdapterLayer = Layer.succeed(
      ProviderAdapterRegistry.ProviderAdapterRegistry,
      registry,
    );
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(providerAdapterLayer),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    const failure = yield* Effect.flip(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-disabled-instance"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: instanceId,
          threadId: asThreadId("thread-disabled-instance"),
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(providerLayer)),
    );

    assert.instanceOf(failure, ProviderValidationError);
    assert.include(failure.issue, "Provider instance 'codex_personal' is disabled");
    assert.equal(codex.startSession.mock.calls.length, 0);
  }).pipe(Effect.provide(NodeServices.layer)),
);

const routing = makeProviderServiceLayer();

it.effect("ProviderServiceLive writes canonical events to the emitting thread segment", () =>
  Effect.gen(function* () {
    const codex = makeFakeCodexAdapter();
    const canonicalEvents: ProviderRuntimeEvent[] = [];
    const canonicalThreadIds: Array<string | null> = [];
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = makeProviderServiceLive({
      canonicalEventLogger: {
        filePath: "memory://provider-canonical-events",
        write: (event, threadId) => {
          canonicalEvents.push(event as ProviderRuntimeEvent);
          canonicalThreadIds.push(threadId ?? null);
          return Effect.void;
        },
        close: () => Effect.void,
      },
    }).pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startRuntimeEventSources!;
      yield* provider.openRuntimeEventPublishing!;
      yield* advanceTestClock(10);
      codex.emit({
        eventId: asEventId("evt-canonical-thread-segment"),
        provider: ProviderDriverKind.make("codex"),
        threadId: asThreadId("thread-canonical-thread-segment"),
        createdAt: "2026-01-01T00:00:00.000Z",
        type: "turn.completed",
        payload: {
          state: "completed",
        },
      });
      yield* advanceTestClock(20);
    }).pipe(Effect.provide(providerLayer));

    assert.equal(canonicalEvents.length, 1);
    assert.match(String(canonicalEvents[0]?.threadId), /^sha256:[0-9a-f]{64}$/u);
    assert.deepEqual(canonicalThreadIds, ["thread-canonical-thread-segment"]);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive writes only safe closed primitives to canonical NDJSON", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-canonical-redaction-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true })),
      );
      const logger = yield* makeEventNdjsonLogger(
        NodePath.join(tempDir, "provider-canonical.ndjson"),
        { stream: "canonical", batchWindowMs: 0 },
      );
      assert.exists(logger);
      if (!logger) return;

      const codex = makeFakeCodexAdapter();
      const registry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: codex.adapter,
      });
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(ProviderEventLoggers.ProviderEventLoggers, {
            native: undefined,
            canonical: logger,
          }),
        ),
      );
      const canary = '  T3_CANARY_\t\r\n\u00a0\u2028\u2029多字_"\\  ';
      const escapedCanary = encodeUnknownJsonString(canary).slice(1, -1);
      const identityMarker = "CONFIDENTIALRESULTABC123";
      const event = {
        eventId: asEventId("evt-canonical-assistant-redaction"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-canonical-assistant-redaction"),
        turnId: asTurnId("turn-canonical-assistant-redaction"),
        itemId: RuntimeItemId.make("item-canonical-assistant-redaction"),
        createdAt: "2026-08-28T10:00:00.000Z",
        type: "item.completed",
        payload: {
          itemType: "assistant_message",
          status: "completed",
          title: "Assistant message",
          detail: canary.trim(),
          authorityDetail: canary,
          data: { item: { type: "agentMessage", text: canary } },
        },
        raw: {
          source: "codex.app-server.notification",
          method: "item/completed",
          payload: { item: { type: "agentMessage", text: canary } },
        },
      } satisfies ProviderRuntimeEvent;
      const before = structuredClone(event);
      const commandEvent = {
        eventId: asEventId("evt-canonical-command-redaction"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: event.threadId,
        turnId: event.turnId,
        itemId: RuntimeItemId.make("item-canonical-command-redaction"),
        createdAt: "2026-08-28T10:00:01.000Z",
        type: "item.completed",
        payload: {
          itemType: "command_execution",
          status: "failed",
          title: canary,
          detail: canary,
          data: { command: canary, output: [canary, { nested: canary }], exitCode: 17 },
        },
        raw: {
          source: "codex.app-server.notification",
          payload: { command: canary, output: canary },
        },
      } satisfies ProviderRuntimeEvent;
      const commandBefore = structuredClone(commandEvent);
      const unsafeIdentifierEvent = {
        eventId: identityMarker as unknown as EventId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: identityMarker as unknown as ThreadId,
        turnId: identityMarker as unknown as TurnId,
        itemId: identityMarker as unknown as RuntimeItemId,
        requestId: identityMarker,
        providerRefs: {
          providerTurnId: identityMarker,
          providerItemId: identityMarker,
          providerRequestId: identityMarker,
        },
        createdAt: canary,
        type: "task.updated",
        payload: {
          taskId: identityMarker,
          toolUseId: identityMarker,
          status: canary,
        },
      } as unknown as ProviderRuntimeEvent;
      const unsafeIdentifierBefore = structuredClone(unsafeIdentifierEvent);

      const observed = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const subscription = yield* provider.subscribeEvents!;
        const take = yield* PubSub.take(subscription).pipe(Effect.forkChild);
        yield* provider.startRuntimeEventSources!;
        yield* provider.openRuntimeEventPublishing!;
        codex.emit(event);
        codex.emit(commandEvent);
        codex.emit(unsafeIdentifierEvent);
        yield* advanceTestClock(20);
        return yield* Fiber.join(take);
      }).pipe(Effect.provide(providerLayer));

      assert.deepStrictEqual(event, before);
      assert.deepStrictEqual(commandEvent, commandBefore);
      assert.deepStrictEqual(unsafeIdentifierEvent, unsafeIdentifierBefore);
      assert.deepStrictEqual(observed, before);
      assert.equal(
        observed.type === "item.completed" ? observed.payload.authorityDetail : undefined,
        canary,
      );
      assert.equal(
        observed.type === "item.completed" ? observed.payload.detail : undefined,
        canary.trim(),
      );

      yield* logger.close();
      const line = NodeFS.readFileSync(
        NodePath.join(tempDir, "thread-canonical-assistant-redaction.log"),
        "utf8",
      );
      assert.notInclude(line, canary);
      assert.notInclude(line, escapedCanary);
      assert.notInclude(line, "authorityDetail");
      assert.notInclude(line, '"detail"');
      assert.notInclude(line, '"data"');
      assert.notInclude(line, '"raw"');
      assert.notInclude(line, '"title"');
      assert.notInclude(line, '"command"');
      assert.notInclude(line, '"output"');
      assert.notInclude(line, '"unknownTop"');
      const marker = "] CANON: ";
      const payloads = line
        .trimEnd()
        .split("\n")
        .map((entry) => {
          const markerIndex = entry.indexOf(marker);
          assert.isAtLeast(markerIndex, 0);
          return decodeUnknownJsonString(entry.slice(markerIndex + marker.length));
        }) as ReadonlyArray<Record<string, unknown>>;
      for (const [index, expected] of [
        {
          createdAt: event.createdAt,
          type: event.type,
          payload: { itemType: "assistant_message", status: "completed" },
        },
        {
          createdAt: commandEvent.createdAt,
          type: commandEvent.type,
          payload: {
            itemType: "command_execution",
            status: "failed",
            exitCode: 17,
          },
        },
      ].entries()) {
        const payload = payloads[index]!;
        assert.equal(payload.createdAt, expected.createdAt);
        assert.equal(payload.type, expected.type);
        assert.deepStrictEqual(payload.payload, expected.payload);
        for (const field of [
          "eventId",
          "provider",
          "providerInstanceId",
          "threadId",
          "turnId",
          "itemId",
        ]) {
          assert.match(String(payload[field]), /^sha256:[0-9a-f]{64}$/u, `${index}:${field}`);
        }
      }
      const allPayloads = NodeFS.readdirSync(tempDir)
        .filter((fileName) => fileName.endsWith(".log"))
        .flatMap((fileName) =>
          NodeFS.readFileSync(NodePath.join(tempDir, fileName), "utf8")
            .trimEnd()
            .split("\n")
            .filter((entry) => entry.length > 0)
            .map((entry) => {
              const markerIndex = entry.indexOf(marker);
              assert.isAtLeast(markerIndex, 0);
              return decodeUnknownJsonString(entry.slice(markerIndex + marker.length));
            }),
        ) as ReadonlyArray<Record<string, unknown>>;
      const unsafeProjection = allPayloads.find((payload) => payload.type === "task.updated");
      assert.exists(unsafeProjection);
      assert.match(String(unsafeProjection?.eventId), /^sha256:[0-9a-f]{64}$/u);
      assert.match(String(unsafeProjection?.threadId), /^sha256:[0-9a-f]{64}$/u);
      assert.match(
        String((unsafeProjection?.payload as Record<string, unknown>)?.taskId),
        /^sha256:[0-9a-f]{64}$/u,
      );
      const serializedPayloads = encodeUnknownJsonString(allPayloads);
      assert.notInclude(serializedPayloads, canary);
      assert.notInclude(serializedPayloads, escapedCanary);
      assert.notInclude(serializedPayloads, identityMarker);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("ProviderServiceLive keeps persisted resumable sessions on startup", () =>
  Effect.gen(function* () {
    const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-provider-service-"));
    const dbPath = NodePath.join(tempDir, "orchestration.sqlite");

    const codex = makeFakeCodexAdapter();
    const registry = makeAdapterRegistryMock({
      [ProviderDriverKind.make("codex")]: codex.adapter,
    });

    const persistenceLayer = makeSqlitePersistenceLive(dbPath);
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(persistenceLayer),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));

    yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      yield* directory.upsert({
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: ThreadId.make("thread-stale"),
      });
    }).pipe(Effect.provide(directoryLayer));

    const providerLayer = makeProviderServiceLive().pipe(
      Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
      Layer.provide(directoryLayer),
      Layer.provide(defaultServerSettingsLayer),
      Layer.provide(AnalyticsService.layerTest),
      Layer.provide(
        Layer.succeed(
          ProviderEventLoggers.ProviderEventLoggers,
          ProviderEventLoggers.NoOpProviderEventLoggers,
        ),
      ),
    );

    yield* ProviderService.ProviderService.pipe(Effect.provide(providerLayer));

    const persistedProvider = yield* Effect.gen(function* () {
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      return yield* directory.getProvider(asThreadId("thread-stale"));
    }).pipe(Effect.provide(directoryLayer));
    assert.equal(persistedProvider, "codex");

    const runtime = yield* Effect.gen(function* () {
      const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      return yield* repository.getByThreadId({
        threadId: asThreadId("thread-stale"),
      });
    }).pipe(Effect.provide(runtimeRepositoryLayer));
    assert.equal(Option.isSome(runtime), true);

    const legacyTableRows = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      return yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'provider_sessions'
      `;
    }).pipe(Effect.provide(persistenceLayer));
    assert.equal(legacyTableRows.length, 0);

    NodeFS.rmSync(tempDir, { recursive: true, force: true });
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "ProviderServiceLive restores rollback routing after restart using persisted thread mapping",
  () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-restart-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstCodex = makeFakeCodexAdapter();
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: firstCodex.adapter,
      });

      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
        ),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const updatedResumeCursor = {
        threadId: asThreadId("thread-1"),
        resume: "resume-session-1",
        resumeSessionAt: "assistant-message-1",
        turnCount: 1,
      };

      const startedSession = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const threadId = asThreadId("thread-1");
        const session = yield* provider.startSession(threadId, {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          cwd: "/tmp/project",
          runtimeMode: "full-access",
          threadId,
        });
        firstCodex.updateSession(threadId, (existing) => ({
          ...existing,
          status: "ready",
          resumeCursor: updatedResumeCursor,
          updatedAt: "2026-01-01T00:00:01.000Z",
        }));
        return session;
      }).pipe(Effect.provide(firstProviderLayer));

      const persistedAfterStopAll = yield* Effect.gen(function* () {
        const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
        return yield* repository.getByThreadId({
          threadId: startedSession.threadId,
        });
      }).pipe(Effect.provide(runtimeRepositoryLayer));
      assert.equal(Option.isSome(persistedAfterStopAll), true);
      if (Option.isSome(persistedAfterStopAll)) {
        assert.equal(persistedAfterStopAll.value.status, "stopped");
        assert.deepEqual(persistedAfterStopAll.value.resumeCursor, updatedResumeCursor);
      }

      const secondCodex = makeFakeCodexAdapter();
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("codex")]: secondCodex.adapter,
      });
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      secondCodex.startSession.mockClear();
      secondCodex.rollbackThread.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.rollbackConversation({
          threadId: startedSession.threadId,
          numTurns: 1,
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondCodex.startSession.mock.calls.length, 1);
      const resumedStartInput = secondCodex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, updatedResumeCursor);
        assert.equal(startPayload.threadId, startedSession.threadId);
      }
      assert.equal(secondCodex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = secondCodex.rollbackThread.mock.calls[0];
      assert.equal(typeof rollbackCall?.[0], "string");
      assert.equal(rollbackCall?.[1], 1);

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
);

routing.layer("ProviderServiceLive routing", (it) => {
  it.effect("places the Initial Planning marker at the actual adapter invoke boundary", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const threadId = asThreadId("thread-initial-planning-boundary");
      const modelSelection = createModelSelection(codexInstanceId, "gpt-5.4", [
        { id: "reasoningEffort", value: "high" },
        { id: "fastMode", value: true },
      ]);
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/initial-planning-boundary",
        modelSelection,
        runtimeMode: "approval-required",
      });
      const attestation = yield* provider.getSessionAttestation!(threadId);
      assert.isDefined(attestation);
      if (attestation === undefined) return;
      const call = provider.sendTurnAtPreInvokeBoundary!;
      const order: string[] = [];
      const callsBefore = routing.codex.sendTurn.mock.calls.length;
      const result = yield* call(
        {
          threadId,
          input: "plan",
          attachments: [],
          modelSelection,
          interactionMode: "plan",
        },
        {
          expected: attestation,
          beforeDeliveryCas: () => Effect.sync(() => order.push("before-cas")).pipe(Effect.asVoid),
          persistDeliveryAttempted: (actual) =>
            Effect.sync(() => {
              assert.deepStrictEqual(actual, attestProviderNativeTurnConfiguration(modelSelection));
              order.push("cas");
            }),
          afterDeliveryCas: () => Effect.sync(() => order.push("after-cas")).pipe(Effect.asVoid),
          onAdapterEntered: () => {
            order.push("adapter-entry");
          },
          onExternalOperationStarted: () => {
            order.push("external-started");
          },
        },
      );
      assert.equal(result.turnId, `turn-${threadId}`);
      assert.deepStrictEqual(order, [
        "before-cas",
        "cas",
        "after-cas",
        "adapter-entry",
        "external-started",
      ]);
      assert.equal(routing.codex.sendTurn.mock.calls.length, callsBefore + 1);

      const beforeMismatch = routing.codex.sendTurn.mock.calls.length;
      const mismatch = yield* Effect.exit(
        call(
          {
            threadId,
            input: "must not invoke",
            attachments: [],
            modelSelection,
            interactionMode: "plan",
          },
          {
            expected: { ...attestation, cwd: "/tmp/different-cwd" },
            beforeDeliveryCas: () => Effect.die("unexpected-before-cas"),
            persistDeliveryAttempted: () => Effect.die("unexpected-cas"),
            afterDeliveryCas: () => Effect.die("unexpected-after-cas"),
          },
        ),
      );
      assert.equal(mismatch._tag, "Failure");
      assert.equal(routing.codex.sendTurn.mock.calls.length, beforeMismatch);

      const operationLock = yield* ProviderThreadOperationLock;
      const lockHeld = yield* Deferred.make<void>();
      const releaseLock = yield* Deferred.make<void>();
      routing.routedInstances.length = 0;
      const holder = yield* operationLock
        .withLock(
          threadId,
          Deferred.succeed(lockHeld, undefined).pipe(Effect.andThen(Deferred.await(releaseLock))),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(lockHeld);
      const lockWaitOrder: string[] = [];
      const lockWaitCalls = routing.codex.sendTurn.mock.calls.length;
      const waiting = yield* call(
        {
          threadId,
          input: "interrupt while waiting for lock",
          attachments: [],
          modelSelection,
          interactionMode: "plan",
        },
        {
          expected: attestation,
          beforeDeliveryCas: () =>
            Effect.sync(() => lockWaitOrder.push("before-cas")).pipe(Effect.asVoid),
          persistDeliveryAttempted: () =>
            Effect.sync(() => lockWaitOrder.push("cas")).pipe(Effect.asVoid),
          afterDeliveryCas: () =>
            Effect.sync(() => lockWaitOrder.push("after-cas")).pipe(Effect.asVoid),
        },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Effect.yieldNow;
      assert.deepStrictEqual(routing.routedInstances, [codexInstanceId]);
      yield* Fiber.interrupt(waiting);
      const lockWaitExit = yield* Fiber.await(waiting);
      yield* Deferred.succeed(releaseLock, undefined);
      yield* Fiber.join(holder);
      assert.equal(lockWaitExit._tag, "Failure");
      assert.deepStrictEqual(lockWaitOrder, []);
      assert.equal(routing.codex.sendTurn.mock.calls.length, lockWaitCalls);

      for (const checkpoint of ["before-cas", "cas", "after-cas"] as const) {
        const checkpointOrder: string[] = [];
        const providerCalls = routing.codex.sendTurn.mock.calls.length;
        const fail = () => Effect.die(new Error(`fail-${checkpoint}`));
        const exit = yield* Effect.exit(
          call(
            {
              threadId,
              input: `fail ${checkpoint}`,
              attachments: [],
              modelSelection,
              interactionMode: "plan",
            },
            {
              expected: attestation,
              beforeDeliveryCas: () =>
                checkpoint === "before-cas"
                  ? fail()
                  : Effect.sync(() => checkpointOrder.push("before-cas")).pipe(Effect.asVoid),
              persistDeliveryAttempted: () =>
                checkpoint === "cas"
                  ? fail()
                  : Effect.sync(() => checkpointOrder.push("cas")).pipe(Effect.asVoid),
              afterDeliveryCas: () =>
                checkpoint === "after-cas"
                  ? fail()
                  : Effect.sync(() => checkpointOrder.push("after-cas")).pipe(Effect.asVoid),
            },
          ),
        );
        assert.equal(exit._tag, "Failure", checkpoint);
        assert.equal(routing.codex.sendTurn.mock.calls.length, providerCalls, checkpoint);
        assert.deepStrictEqual(
          checkpointOrder,
          checkpoint === "before-cas"
            ? []
            : checkpoint === "cas"
              ? ["before-cas"]
              : ["before-cas", "cas"],
          checkpoint,
        );
      }

      for (const [name, afterEntry, failure] of [
        [
          "failure-before-entry",
          false,
          Effect.fail(
            new ProviderAdapterRequestError({
              provider: "codex",
              method: "thread.turn.start",
              detail: "rejected before external operation",
            }),
          ),
        ],
        ["defect-before-entry", false, Effect.die(new Error("defect-before-entry"))],
        ["interrupt-before-entry", false, Effect.interrupt],
        [
          "failure-inside-entry",
          true,
          Effect.fail(
            new ProviderAdapterRequestError({
              provider: "codex",
              method: "thread.turn.start",
              detail: "rejected inside adapter entry",
            }),
          ),
        ],
        ["defect-inside-entry", true, Effect.die(new Error("defect-inside-entry"))],
        ["interrupt-inside-entry", true, Effect.interrupt],
      ] as const) {
        const entryOrder: string[] = [];
        const providerCalls = routing.codex.sendTurn.mock.calls.length;
        routing.codex.setPrepareTurn((input) =>
          Effect.succeed({
            attestation: attestProviderNativeTurnConfiguration(input.modelSelection!),
            invoke: (entry) =>
              afterEntry ? entry.adapterEntered().pipe(Effect.andThen(failure)) : failure,
          }),
        );
        const exit = yield* Effect.exit(
          call(
            {
              threadId,
              input: name,
              attachments: [],
              modelSelection,
              interactionMode: "plan",
            },
            {
              expected: attestation,
              beforeDeliveryCas: () => Effect.sync(() => entryOrder.push("before-cas")),
              afterDeliveryCas: () => Effect.sync(() => entryOrder.push("after-cas")),
              persistDeliveryAttempted: () => Effect.sync(() => entryOrder.push("cas")),
              onAdapterEntered: () => {
                entryOrder.push("adapter-entry");
              },
              onExternalOperationStarted: () => {
                entryOrder.push("external-started");
              },
            },
          ),
        );
        assert.equal(exit._tag, "Failure", name);
        assert.deepStrictEqual(
          entryOrder,
          ["before-cas", "cas", "after-cas", ...(afterEntry ? ["adapter-entry"] : [])],
          name,
        );
        assert.equal(routing.codex.sendTurn.mock.calls.length, providerCalls, name);
      }
      routing.codex.resetPrepareTurn();

      const pendingEntryReached = yield* Deferred.make<void>();
      const releasePendingEntry = yield* Deferred.make<void>();
      const pendingOrder: string[] = [];
      routing.codex.setPrepareTurn((input) =>
        Effect.succeed({
          attestation: attestProviderNativeTurnConfiguration(input.modelSelection!),
          invoke: (entry) =>
            Deferred.succeed(pendingEntryReached, undefined).pipe(
              Effect.andThen(Deferred.await(releasePendingEntry)),
              Effect.andThen(entry.adapterEntered()),
              Effect.andThen(entry.startExternal(() => Effect.never)),
            ),
        }),
      );
      const pendingInterrupt = yield* call(
        {
          threadId,
          input: "pending interrupt between CAS and entry",
          attachments: [],
          modelSelection,
          interactionMode: "plan",
        },
        {
          expected: attestation,
          beforeDeliveryCas: () => Effect.sync(() => pendingOrder.push("before-cas")),
          afterDeliveryCas: () => Effect.sync(() => pendingOrder.push("after-cas")),
          persistDeliveryAttempted: () => Effect.sync(() => pendingOrder.push("cas")),
          onAdapterEntered: () => {
            pendingOrder.push("adapter-entry");
          },
          onExternalOperationStarted: () => {
            pendingOrder.push("external-started");
          },
        },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(pendingEntryReached);
      const pendingInterrupter = yield* Fiber.interrupt(pendingInterrupt).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Effect.yieldNow;
      yield* Deferred.succeed(releasePendingEntry, undefined);
      assert.equal((yield* Fiber.await(pendingInterrupt))._tag, "Failure");
      yield* Fiber.join(pendingInterrupter);
      assert.deepStrictEqual(pendingOrder, ["before-cas", "cas", "after-cas"]);
      routing.codex.resetPrepareTurn();

      const nativeInvocationOrder: string[] = [];
      routing.codex.setPrepareTurn((input) =>
        Effect.succeed({
          attestation: attestProviderNativeTurnConfiguration(input.modelSelection!),
          invoke: (entry) =>
            entry
              .adapterEntered()
              .pipe(
                Effect.andThen(entry.nativeInvocationStarted?.() ?? Effect.void),
                Effect.andThen(Effect.die(new Error("response-lost-after-native-start"))),
              ),
        }),
      );
      const nativeInvocationExit = yield* Effect.exit(
        call(
          {
            threadId,
            input: "native invocation response loss",
            attachments: [],
            modelSelection,
            interactionMode: "plan",
          },
          {
            expected: attestation,
            beforeDeliveryCas: () => Effect.sync(() => nativeInvocationOrder.push("before-cas")),
            persistDeliveryAttempted: () => Effect.sync(() => nativeInvocationOrder.push("cas")),
            afterDeliveryCas: () => Effect.sync(() => nativeInvocationOrder.push("after-cas")),
            onAdapterEntered: () => {
              nativeInvocationOrder.push("adapter-entry");
            },
            onExternalOperationStarted: () => {
              nativeInvocationOrder.push("incorrect-outer-start");
            },
            onNativeInvocationStarted: () => {
              nativeInvocationOrder.push("native-invocation-started");
            },
          },
        ),
      );
      assert.equal(nativeInvocationExit._tag, "Failure");
      assert.deepStrictEqual(nativeInvocationOrder, [
        "before-cas",
        "cas",
        "after-cas",
        "adapter-entry",
        "native-invocation-started",
      ]);
      routing.codex.resetPrepareTurn();

      const adapterFailureOrder: string[] = [];
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderAdapterRequestError({
            provider: "codex",
            method: "thread.turn.start",
            detail: "definitely rejected is not expressible by this adapter",
          }),
        ),
      );
      const adapterFailure = yield* Effect.exit(
        call(
          {
            threadId,
            input: "adapter failure",
            attachments: [],
            modelSelection,
            interactionMode: "plan",
          },
          {
            expected: attestation,
            beforeDeliveryCas: () =>
              Effect.sync(() => adapterFailureOrder.push("before-cas")).pipe(Effect.asVoid),
            persistDeliveryAttempted: () =>
              Effect.sync(() => adapterFailureOrder.push("cas")).pipe(Effect.asVoid),
            afterDeliveryCas: () =>
              Effect.sync(() => adapterFailureOrder.push("after-cas")).pipe(Effect.asVoid),
            onAdapterEntered: () => {
              adapterFailureOrder.push("adapter-entry");
            },
            onExternalOperationStarted: () => {
              adapterFailureOrder.push("external-started");
            },
          },
        ),
      );
      assert.equal(adapterFailure._tag, "Failure");
      assert.deepStrictEqual(adapterFailureOrder, [
        "before-cas",
        "cas",
        "after-cas",
        "adapter-entry",
        "external-started",
      ]);

      const externalDefectOrder: string[] = [];
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Effect.die(new Error("external-operation-defect")),
      );
      const externalDefect = yield* Effect.exit(
        call(
          {
            threadId,
            input: "external operation defect",
            attachments: [],
            modelSelection,
            interactionMode: "plan",
          },
          {
            expected: attestation,
            beforeDeliveryCas: () => Effect.sync(() => externalDefectOrder.push("before-cas")),
            persistDeliveryAttempted: () => Effect.sync(() => externalDefectOrder.push("cas")),
            afterDeliveryCas: () => Effect.sync(() => externalDefectOrder.push("after-cas")),
            onAdapterEntered: () => {
              externalDefectOrder.push("adapter-entry");
            },
            onExternalOperationStarted: () => {
              externalDefectOrder.push("external-started");
            },
          },
        ),
      );
      assert.equal(externalDefect._tag, "Failure");
      assert.deepStrictEqual(externalDefectOrder, [
        "before-cas",
        "cas",
        "after-cas",
        "adapter-entry",
        "external-started",
      ]);

      const adapterEntered = yield* Deferred.make<void>();
      routing.codex.sendTurn.mockImplementationOnce(() =>
        Deferred.succeed(adapterEntered, undefined).pipe(Effect.andThen(Effect.never)),
      );
      const interruptedOrder: string[] = [];
      const interrupted = yield* call(
        {
          threadId,
          input: "adapter interrupt",
          attachments: [],
          modelSelection,
          interactionMode: "plan",
        },
        {
          expected: attestation,
          beforeDeliveryCas: () =>
            Effect.sync(() => interruptedOrder.push("before-cas")).pipe(Effect.asVoid),
          persistDeliveryAttempted: () =>
            Effect.sync(() => interruptedOrder.push("cas")).pipe(Effect.asVoid),
          afterDeliveryCas: () =>
            Effect.sync(() => interruptedOrder.push("after-cas")).pipe(Effect.asVoid),
          onAdapterEntered: () => {
            interruptedOrder.push("adapter-entry");
          },
          onExternalOperationStarted: () => {
            interruptedOrder.push("external-started");
          },
        },
      ).pipe(Effect.forkChild({ startImmediately: true }));
      yield* Deferred.await(adapterEntered);
      yield* Fiber.interrupt(interrupted);
      const interruptedExit = yield* Fiber.await(interrupted);
      assert.equal(interruptedExit._tag, "Failure");
      assert.deepStrictEqual(interruptedOrder, [
        "before-cas",
        "cas",
        "after-cas",
        "adapter-entry",
        "external-started",
      ]);
      yield* provider.stopSession({ threadId });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();
      routing.codex.stopSession.mockClear();
      routing.codex.listSessions.mockClear();
      routing.codex.hasSession.mockClear();
    }),
  );

  it.effect("serializes sendTurn behind the shared thread operation lock", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const operationLock = yield* ProviderThreadOperationLock;
      const threadId = asThreadId("thread-lock-send-turn");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      const sendsBeforeLock = routing.codex.sendTurn.mock.calls.length;

      const lockHeld = yield* Deferred.make<void>();
      const releaseLock = yield* Deferred.make<void>();
      const holder = yield* operationLock
        .withLock(
          threadId,
          Deferred.succeed(lockHeld, undefined).pipe(Effect.andThen(Deferred.await(releaseLock))),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(lockHeld);

      const send = yield* provider
        .sendTurn({ threadId, input: "wait for lock", attachments: [] })
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.equal(routing.codex.sendTurn.mock.calls.length, sendsBeforeLock);

      yield* Deferred.succeed(releaseLock, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(send);
      assert.equal(routing.codex.sendTurn.mock.calls.length, sendsBeforeLock + 1);
      yield* provider.stopSession({ threadId });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();
      routing.codex.stopSession.mockClear();
      routing.codex.listSessions.mockClear();
      routing.codex.hasSession.mockClear();
    }),
  );

  it.effect("serializes stopSession behind the shared thread operation lock", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const operationLock = yield* ProviderThreadOperationLock;
      const threadId = asThreadId("thread-lock-stop-session");
      yield* provider.startSession(threadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      const stopsBeforeLock = routing.codex.stopSession.mock.calls.length;

      const lockHeld = yield* Deferred.make<void>();
      const releaseLock = yield* Deferred.make<void>();
      const holder = yield* operationLock
        .withLock(
          threadId,
          Deferred.succeed(lockHeld, undefined).pipe(Effect.andThen(Deferred.await(releaseLock))),
        )
        .pipe(Effect.forkScoped);
      yield* Deferred.await(lockHeld);

      const stop = yield* provider.stopSession({ threadId }).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      assert.equal(routing.codex.stopSession.mock.calls.length, stopsBeforeLock);

      yield* Deferred.succeed(releaseLock, undefined);
      yield* Fiber.join(holder);
      yield* Fiber.join(stop);
      assert.equal(routing.codex.stopSession.mock.calls.length, stopsBeforeLock + 1);
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();
      routing.codex.stopSession.mockClear();
      routing.codex.listSessions.mockClear();
      routing.codex.hasSession.mockClear();
    }),
  );

  it.effect("routes provider operations and rollback conversation", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      assert.equal(session.provider, "codex");

      const sessions = yield* provider.listSessions();
      assert.equal(sessions.length, 1);

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);

      yield* provider.interruptTurn({ threadId: session.threadId });
      assert.deepEqual(routing.codex.interruptTurn.mock.calls, [[session.threadId, undefined]]);

      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-1"),
        decision: "accept",
      });
      assert.deepEqual(routing.codex.respondToRequest.mock.calls, [
        [session.threadId, asRequestId("req-1"), "accept"],
      ]);

      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-user-input-1"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      assert.deepEqual(routing.codex.respondToUserInput.mock.calls, [
        [
          session.threadId,
          asRequestId("req-user-input-1"),
          {
            sandbox_mode: "workspace-write",
          },
        ],
      ]);

      yield* provider.rollbackConversation({
        threadId: session.threadId,
        numTurns: 0,
      });

      yield* provider.stopSession({ threadId: session.threadId });
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "after-stop",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, session.resumeCursor);
        assert.equal(startPayload.threadId, session.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("recovers stale persisted sessions for rollback by resuming thread identity", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });
      yield* routing.codex.stopSession(initial.threadId);
      routing.codex.startSession.mockClear();
      routing.codex.rollbackThread.mockClear();

      yield* provider.rollbackConversation({
        threadId: initial.threadId,
        numTurns: 1,
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.rollbackThread.mock.calls.length, 1);
      const rollbackCall = routing.codex.rollbackThread.mock.calls[0];
      assert.equal(rollbackCall?.[1], 1);
    }),
  );

  it.effect("preserves the persisted binding when stopping a session", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const initial = yield* provider.startSession(asThreadId("thread-reap-preserve"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-reap-preserve"),
        cwd: "/tmp/project-reap-preserve",
        runtimeMode: "full-access",
      });

      yield* provider.stopSession({ threadId: initial.threadId });

      const persistedAfterStop = yield* runtimeRepository.getByThreadId({
        threadId: initial.threadId,
      });
      assert.equal(Option.isSome(persistedAfterStop), true);
      if (Option.isSome(persistedAfterStop)) {
        assert.equal(persistedAfterStop.value.status, "stopped");
        assert.deepEqual(persistedAfterStop.value.resumeCursor, initial.resumeCursor);
      }

      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume after reap",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project-reap-preserve");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("routes explicit claudeAgent provider session starts to the claude adapter", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-claude"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-claude"),
        cwd: "/tmp/project-claude",
        runtimeMode: "full-access",
      });

      assert.equal(session.provider, "claudeAgent");
      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const startInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof startInput === "object" && startInput !== null, true);
      if (startInput && typeof startInput === "object") {
        const startPayload = startInput as {
          provider?: string;
          providerInstanceId?: ProviderInstanceId;
          cwd?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.providerInstanceId, claudeAgentInstanceId);
        assert.equal(startPayload.cwd, "/tmp/project-claude");
      }
    }),
  );

  it.effect("dies when an active session conflicts with its persisted binding", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-binding-mismatch");

      yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-binding-mismatch",
        runtimeMode: "full-access",
      });
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        runtimeMode: "full-access",
      });

      const exit = yield* Effect.exit(provider.listSessions());
      assert.equal(Exit.hasDies(exit), true);
      yield* directory.upsert({
        threadId,
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        runtimeMode: "full-access",
      });
    }),
  );

  it.effect("stops stale sessions in other providers after a successful replacement start", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const threadId = asThreadId("thread-provider-replacement");

      const codexSession = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        cwd: "/tmp/project-provider-replacement",
        runtimeMode: "full-access",
      });

      routing.codex.stopSession.mockClear();
      routing.claude.stopSession.mockClear();
      const stopCodex = routing.codex.stopSession.getMockImplementation();
      let bindingSeenWhileStoppingOld: ProviderInstanceId | undefined;
      routing.codex.stopSession.mockImplementationOnce((stoppedThreadId) =>
        Effect.gen(function* () {
          const binding = Option.getOrUndefined(
            yield* directory.getBinding(stoppedThreadId).pipe(Effect.orDie),
          );
          bindingSeenWhileStoppingOld = binding?.providerInstanceId;
          if (stopCodex) {
            yield* stopCodex(stoppedThreadId);
          }
        }),
      );

      const claudeSession = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId,
        cwd: "/tmp/project-provider-replacement",
        runtimeMode: "full-access",
      });

      assert.equal(codexSession.provider, "codex");
      assert.equal(claudeSession.provider, "claudeAgent");
      assert.equal(bindingSeenWhileStoppingOld, claudeAgentInstanceId);
      assert.deepEqual(routing.codex.stopSession.mock.calls, [[threadId]]);
      assert.equal(routing.claude.stopSession.mock.calls.length, 0);

      const sessions = yield* provider.listSessions();
      assert.deepEqual(
        sessions
          .filter((session) => session.threadId === threadId)
          .map((session) => session.provider),
        ["claudeAgent"],
      );
    }),
  );

  it.effect("recovers stale sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        cwd: "/tmp/project-send-turn",
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      routing.codex.startSession.mockClear();
      routing.codex.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume",
        attachments: [],
      });

      assert.equal(routing.codex.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.codex.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "codex");
        assert.equal(startPayload.cwd, "/tmp/project-send-turn");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.codex.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("recovers stale claudeAgent sessions for sendTurn using persisted cwd", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const initial = yield* provider.startSession(asThreadId("thread-claude-send-turn"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-claude-send-turn"),
        cwd: "/tmp/project-claude-send-turn",
        modelSelection: createModelSelection(
          ProviderInstanceId.make("claudeAgent"),
          "claude-opus-4-6",
          [{ id: "effort", value: "max" }],
        ),
        runtimeMode: "full-access",
      });

      yield* routing.claude.stopAll();
      routing.claude.startSession.mockClear();
      routing.claude.sendTurn.mockClear();

      yield* provider.sendTurn({
        threadId: initial.threadId,
        input: "resume with claude",
        attachments: [],
      });

      assert.equal(routing.claude.startSession.mock.calls.length, 1);
      const resumedStartInput = routing.claude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          modelSelection?: unknown;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude-send-turn");
        assert.deepEqual(
          startPayload.modelSelection,
          createModelSelection(ProviderInstanceId.make("claudeAgent"), "claude-opus-4-6", [
            { id: "effort", value: "max" },
          ]),
        );
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }
      assert.equal(routing.claude.sendTurn.mock.calls.length, 1);
    }),
  );

  it.effect("lists no sessions after adapter runtime clears", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });
      yield* provider.startSession(asThreadId("thread-2"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-2"),
        runtimeMode: "full-access",
      });

      yield* routing.codex.stopAll();
      yield* routing.claude.stopAll();

      const remaining = yield* provider.listSessions();
      assert.equal(remaining.length, 0);
    }),
  );

  it.effect("persists runtime status transitions in provider_session_runtime", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

      const threadId = asThreadId("thread-runtime-status");
      const session = yield* provider.startSession(threadId, {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId,
        runtimeMode: "full-access",
      });
      yield* provider.sendTurn({
        threadId: session.threadId,
        input: "hello",
        attachments: [],
      });

      const runningRuntime = yield* runtimeRepository.getByThreadId({
        threadId: session.threadId,
      });
      assert.equal(Option.isSome(runningRuntime), true);
      if (Option.isSome(runningRuntime)) {
        assert.equal(runningRuntime.value.status, "running");
        assert.deepEqual(runningRuntime.value.resumeCursor, session.resumeCursor);
        const payload = runningRuntime.value.runtimePayload;
        assert.equal(payload !== null && typeof payload === "object", true);
        if (payload !== null && typeof payload === "object" && !Array.isArray(payload)) {
          const runtimePayload = payload as {
            cwd: string;
            model: string | null;
            activeTurnId: string | null;
            lastError: string | null;
            lastRuntimeEvent: string | null;
          };
          assert.equal(runtimePayload.cwd, session.cwd);
          assert.equal(runtimePayload.model, null);
          assert.equal(runtimePayload.activeTurnId, `turn-${String(session.threadId)}`);
          assert.equal(runtimePayload.lastError, null);
          assert.equal(runtimePayload.lastRuntimeEvent, "provider.sendTurn");
        }
      }
    }),
  );

  it.effect("reuses persisted resume cursor when startSession is called after a restart", () =>
    Effect.gen(function* () {
      const tempDir = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-provider-service-start-"),
      );
      const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
      const persistenceLayer = makeSqlitePersistenceLive(dbPath);
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(persistenceLayer),
      );

      const firstClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
      const firstRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("claudeAgent")]: firstClaude.adapter,
      });
      const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const firstProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
        ),
        Layer.provide(firstDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      const initial = yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        return yield* provider.startSession(asThreadId("thread-claude-start"), {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-claude-start"),
          cwd: "/tmp/project-claude-start",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(firstProviderLayer));

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.listSessions();
      }).pipe(Effect.provide(firstProviderLayer));

      const secondClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
      const secondRegistry = makeAdapterRegistryMock({
        [ProviderDriverKind.make("claudeAgent")]: secondClaude.adapter,
      });
      const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const secondProviderLayer = makeProviderServiceLive().pipe(
        Layer.provide(
          Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
        ),
        Layer.provide(secondDirectoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );

      secondClaude.startSession.mockClear();

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        yield* provider.startSession(initial.threadId, {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: initial.threadId,
          cwd: "/tmp/project-claude-start",
          runtimeMode: "full-access",
        });
      }).pipe(Effect.provide(secondProviderLayer));

      assert.equal(secondClaude.startSession.mock.calls.length, 1);
      const resumedStartInput = secondClaude.startSession.mock.calls[0]?.[0];
      assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
      if (resumedStartInput && typeof resumedStartInput === "object") {
        const startPayload = resumedStartInput as {
          provider?: string;
          cwd?: string;
          resumeCursor?: unknown;
          threadId?: string;
        };
        assert.equal(startPayload.provider, "claudeAgent");
        assert.equal(startPayload.cwd, "/tmp/project-claude-start");
        assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
        assert.equal(startPayload.threadId, initial.threadId);
      }

      NodeFS.rmSync(tempDir, { recursive: true, force: true });
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "reuses persisted cwd when startSession resumes a claude session without cwd input",
    () =>
      Effect.gen(function* () {
        const tempDir = NodeFS.mkdtempSync(
          NodePath.join(NodeOS.tmpdir(), "t3-provider-service-cwd-"),
        );
        const dbPath = NodePath.join(tempDir, "orchestration.sqlite");
        const persistenceLayer = makeSqlitePersistenceLive(dbPath);
        const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
          Layer.provide(persistenceLayer),
        );

        const firstClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const firstRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: firstClaude.adapter,
        });
        const firstDirectoryLayer = ProviderSessionDirectoryLive.pipe(
          Layer.provide(runtimeRepositoryLayer),
        );
        const firstProviderLayer = makeProviderServiceLive().pipe(
          Layer.provide(
            Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, firstRegistry),
          ),
          Layer.provide(firstDirectoryLayer),
          Layer.provide(defaultServerSettingsLayer),
          Layer.provide(AnalyticsService.layerTest),
          Layer.provide(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
        );

        const initial = yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          return yield* provider.startSession(asThreadId("thread-claude-cwd"), {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: claudeAgentInstanceId,
            threadId: asThreadId("thread-claude-cwd"),
            cwd: "/tmp/project-claude-cwd",
            runtimeMode: "full-access",
          });
        }).pipe(Effect.provide(firstProviderLayer));

        const secondClaude = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER);
        const secondRegistry = makeAdapterRegistryMock({
          [ProviderDriverKind.make("claudeAgent")]: secondClaude.adapter,
        });
        const secondDirectoryLayer = ProviderSessionDirectoryLive.pipe(
          Layer.provide(runtimeRepositoryLayer),
        );
        const secondProviderLayer = makeProviderServiceLive().pipe(
          Layer.provide(
            Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, secondRegistry),
          ),
          Layer.provide(secondDirectoryLayer),
          Layer.provide(defaultServerSettingsLayer),
          Layer.provide(AnalyticsService.layerTest),
          Layer.provide(
            Layer.succeed(
              ProviderEventLoggers.ProviderEventLoggers,
              ProviderEventLoggers.NoOpProviderEventLoggers,
            ),
          ),
        );

        secondClaude.startSession.mockClear();

        yield* Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          yield* provider.startSession(initial.threadId, {
            provider: ProviderDriverKind.make("claudeAgent"),
            providerInstanceId: claudeAgentInstanceId,
            threadId: initial.threadId,
            runtimeMode: "full-access",
          });
        }).pipe(Effect.provide(secondProviderLayer));

        assert.equal(secondClaude.startSession.mock.calls.length, 1);
        const resumedStartInput = secondClaude.startSession.mock.calls[0]?.[0];
        assert.equal(typeof resumedStartInput === "object" && resumedStartInput !== null, true);
        if (resumedStartInput && typeof resumedStartInput === "object") {
          const startPayload = resumedStartInput as {
            provider?: string;
            cwd?: string;
            resumeCursor?: unknown;
            threadId?: string;
          };
          assert.equal(startPayload.provider, "claudeAgent");
          assert.equal(startPayload.cwd, "/tmp/project-claude-cwd");
          assert.deepEqual(startPayload.resumeCursor, initial.resumeCursor);
          assert.equal(startPayload.threadId, initial.threadId);
        }

        NodeFS.rmSync(tempDir, { recursive: true, force: true });
      }).pipe(Effect.provide(NodeServices.layer)),
  );
});

it.effect("reuses persisted resume state for a compatible cross-instance cold start", () =>
  Effect.gen(function* () {
    const sourceInstanceId = ProviderInstanceId.make("claude_work");
    const targetInstanceId = ProviderInstanceId.make("claude_personal");
    const continuationKey = "claude:session-store:t3-local:v1";
    const source = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER, {
      providerInstanceId: sourceInstanceId,
    });
    const target = makeFakeCodexAdapter(CLAUDE_AGENT_DRIVER, {
      providerInstanceId: targetInstanceId,
    });
    const syncContinuation = vi.fn<
      NonNullable<ProviderAdapterShape<ProviderAdapterError>["syncContinuation"]>
    >(() => Effect.succeed("imported" as const));
    const sourceAdapter = {
      ...source.adapter,
      syncContinuation,
    };
    const adapters = new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>([
      [sourceInstanceId, source.adapter],
      [targetInstanceId, target.adapter],
    ]);
    const registry = makeCompatibleInstanceRegistry({
      driverKind: CLAUDE_AGENT_DRIVER,
      continuationKey,
      adapters,
    });
    const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
      Layer.provide(SqlitePersistenceMemory),
    );
    const directoryLayer = ProviderSessionDirectoryLive.pipe(Layer.provide(runtimeRepositoryLayer));
    const providerLayer = Layer.mergeAll(
      makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      ),
      directoryLayer,
    );
    const threadId = asThreadId("thread-compatible-cold-switch");

    yield* Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const sessionDirectory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const sourceSession = yield* provider.startSession(threadId, {
        provider: CLAUDE_AGENT_DRIVER,
        providerInstanceId: sourceInstanceId,
        threadId,
        cwd: "/tmp/compatible-cold-switch",
        runtimeMode: "full-access",
      });
      target.startSession.mockClear();
      source.stopSession.mockClear();

      yield* sessionDirectory.upsert({
        threadId,
        provider: CLAUDE_AGENT_DRIVER,
        providerInstanceId: sourceInstanceId,
        resumeCursor: null,
      });
      const missingResumeFailure = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: targetInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);
      assert.instanceOf(missingResumeFailure, ProviderAdapterRequestError);
      assert.include(missingResumeFailure.detail, "persisted resume state");
      assert.equal(target.startSession.mock.calls.length, 0);
      assert.equal(source.stopSession.mock.calls.length, 0);
      assert.equal(
        Option.getOrUndefined(yield* sessionDirectory.getBinding(threadId))?.providerInstanceId,
        sourceInstanceId,
      );
      yield* sessionDirectory.upsert({
        threadId,
        provider: CLAUDE_AGENT_DRIVER,
        providerInstanceId: sourceInstanceId,
        resumeCursor: sourceSession.resumeCursor,
      });

      const missingCapabilityFailure = yield* provider
        .startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: targetInstanceId,
          threadId,
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);
      assert.instanceOf(missingCapabilityFailure, ProviderAdapterRequestError);
      assert.include(missingCapabilityFailure.detail, "continuation sync capability");
      assert.equal(target.startSession.mock.calls.length, 0);
      assert.equal(source.stopSession.mock.calls.length, 0);
      assert.equal(
        Option.getOrUndefined(yield* sessionDirectory.getBinding(threadId))?.providerInstanceId,
        sourceInstanceId,
      );

      adapters.set(sourceInstanceId, sourceAdapter);
      syncContinuation.mockImplementationOnce(() =>
        Effect.fail(
          new ProviderContinuationSyncCapabilityError({
            code: "sync-failed",
            detail: "simulated local sync failure",
          }),
        ),
      );
      const failedSwitch = yield* Effect.result(
        provider.startSession(threadId, {
          provider: CLAUDE_AGENT_DRIVER,
          providerInstanceId: targetInstanceId,
          threadId,
          runtimeMode: "full-access",
        }),
      );
      assert.equal(failedSwitch._tag, "Failure");
      assert.equal(target.startSession.mock.calls.length, 0);
      assert.equal(source.stopSession.mock.calls.length, 0);

      yield* provider.startSession(threadId, {
        provider: CLAUDE_AGENT_DRIVER,
        providerInstanceId: targetInstanceId,
        threadId,
        runtimeMode: "full-access",
      });

      const targetInput = target.startSession.mock.calls[0]?.[0];
      assert.equal(syncContinuation.mock.calls.length, 2);
      assert.deepEqual(syncContinuation.mock.calls[1], [
        {
          threadId,
          resumeCursor: sourceSession.resumeCursor,
          cwd: "/tmp/compatible-cold-switch",
        },
      ]);
      assert.ok(
        (syncContinuation.mock.invocationCallOrder[1] ?? Number.MAX_SAFE_INTEGER) <
          (target.startSession.mock.invocationCallOrder[0] ?? 0),
      );
      assert.deepEqual(targetInput?.resumeCursor, sourceSession.resumeCursor);
      assert.equal(targetInput?.cwd, "/tmp/compatible-cold-switch");
      assert.deepEqual(source.stopSession.mock.calls, [[threadId]]);
    }).pipe(Effect.provide(providerLayer));
  }),
);

it.effect(
  "allows compatible Codex instances to switch without continuation sync or resume state",
  () =>
    Effect.gen(function* () {
      const sourceInstanceId = ProviderInstanceId.make("codex_work");
      const targetInstanceId = ProviderInstanceId.make("codex_personal");
      const source = makeFakeCodexAdapter(CODEX_DRIVER, {
        omitGeneratedResumeCursor: true,
        providerInstanceId: sourceInstanceId,
      });
      const target = makeFakeCodexAdapter(CODEX_DRIVER, {
        providerInstanceId: targetInstanceId,
      });
      const adapters = new Map<ProviderInstanceId, ProviderAdapterShape<ProviderAdapterError>>([
        [sourceInstanceId, source.adapter],
        [targetInstanceId, target.adapter],
      ]);
      const registry = makeCompatibleInstanceRegistry({
        driverKind: CODEX_DRIVER,
        continuationKey: "codex:/Users/example/.codex",
        adapters,
      });
      const runtimeRepositoryLayer = ProviderSessionRuntime.layer.pipe(
        Layer.provide(SqlitePersistenceMemory),
      );
      const directoryLayer = ProviderSessionDirectoryLive.pipe(
        Layer.provide(runtimeRepositoryLayer),
      );
      const providerLayer = makeProviderServiceLive().pipe(
        Layer.provide(Layer.succeed(ProviderAdapterRegistry.ProviderAdapterRegistry, registry)),
        Layer.provide(directoryLayer),
        Layer.provide(defaultServerSettingsLayer),
        Layer.provide(AnalyticsService.layerTest),
        Layer.provide(
          Layer.succeed(
            ProviderEventLoggers.ProviderEventLoggers,
            ProviderEventLoggers.NoOpProviderEventLoggers,
          ),
        ),
      );
      const threadId = asThreadId("thread-compatible-codex-switch");

      yield* Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const sourceSession = yield* provider.startSession(threadId, {
          provider: CODEX_DRIVER,
          providerInstanceId: sourceInstanceId,
          threadId,
          cwd: "/tmp/compatible-codex-switch",
          runtimeMode: "full-access",
        });
        target.startSession.mockClear();
        source.stopSession.mockClear();
        assert.equal(sourceSession.resumeCursor, undefined);

        yield* provider.startSession(threadId, {
          provider: CODEX_DRIVER,
          providerInstanceId: targetInstanceId,
          threadId,
          runtimeMode: "full-access",
        });

        assert.equal(target.startSession.mock.calls[0]?.[0].resumeCursor, undefined);
        assert.equal(target.startSession.mock.calls[0]?.[0].cwd, "/tmp/compatible-codex-switch");
        assert.deepEqual(source.stopSession.mock.calls, [[threadId]]);
      }).pipe(Effect.provide(providerLayer));
    }),
);

const fanout = makeProviderServiceLayer();
fanout.layer("ProviderServiceLive fanout", (it) => {
  it.effect("holds adapter events until required runtime subscriptions are ready", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      assert.isDefined(provider.subscribeEvents);
      assert.isDefined(provider.startRuntimeEventSources);
      assert.isDefined(provider.openRuntimeEventPublishing);
      yield* provider.startRuntimeEventSources!;
      const runtimeSubscription = yield* provider.subscribeEvents!;
      const verificationSubscription = yield* provider.subscribeEvents!;
      const runtimeTake = yield* PubSub.take(runtimeSubscription).pipe(Effect.forkChild);
      const verificationTake = yield* PubSub.take(verificationSubscription).pipe(Effect.forkChild);
      const event: LegacyProviderRuntimeEvent = {
        type: "turn.started",
        eventId: asEventId("evt-runtime-ready-gate"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: asThreadId("thread-runtime-ready-gate"),
        turnId: asTurnId("turn-runtime-ready-gate"),
      };

      yield* advanceTestClock(50);
      fanout.codex.emit(event);
      yield* advanceTestClock(50);
      assert.isUndefined(runtimeTake.pollUnsafe());
      assert.isUndefined(verificationTake.pollUnsafe());

      yield* provider.openRuntimeEventPublishing!;
      const [runtimeObserved, verificationObserved] = yield* Effect.all(
        [Fiber.join(runtimeTake), Fiber.join(verificationTake)],
        { concurrency: "unbounded" },
      );
      assert.equal(runtimeObserved.eventId, event.eventId);
      assert.equal(verificationObserved.eventId, event.eventId);
    }),
  );

  it.effect("fans out adapter turn completion events", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startRuntimeEventSources!;
      yield* provider.openRuntimeEventPublishing!;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const eventsRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.runForEach(provider.streamEvents, (event) =>
        Ref.update(eventsRef, (current) => [...current, event]),
      ).pipe(Effect.forkChild);
      yield* advanceTestClock(50);

      const completedEvent: LegacyProviderRuntimeEvent = {
        type: "turn.completed",
        eventId: asEventId("evt-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      };

      fanout.codex.emit(completedEvent);
      yield* advanceTestClock(50);

      const events = yield* Ref.get(eventsRef);
      yield* Fiber.interrupt(consumer);

      assert.equal(
        events.some((entry) => entry.type === "turn.completed"),
        true,
      );
      assert.equal(
        events.some(
          (entry) =>
            entry.type === "turn.completed" && entry.providerInstanceId === codexInstanceId,
        ),
        true,
      );
    }),
  );

  it.effect("fans out canonical runtime events in emission order", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startRuntimeEventSources!;
      yield* provider.openRuntimeEventPublishing!;
      const session = yield* provider.startSession(asThreadId("thread-seq"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-seq"),
        runtimeMode: "full-access",
      });

      const receivedRef = yield* Ref.make<Array<ProviderRuntimeEvent>>([]);
      const consumer = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) => Ref.update(receivedRef, (current) => [...current, event])),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      fanout.codex.emit({
        type: "tool.started",
        eventId: asEventId("evt-seq-1"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "tool.completed",
        eventId: asEventId("evt-seq-2"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        toolKind: "command",
        title: "Ran command",
      });
      fanout.codex.emit({
        type: "turn.completed",
        eventId: asEventId("evt-seq-3"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: session.threadId,
        turnId: asTurnId("turn-1"),
        status: "completed",
      });

      yield* Fiber.join(consumer);
      const received = yield* Ref.get(receivedRef);
      assert.deepEqual(
        received.map((event) => event.eventId),
        [asEventId("evt-seq-1"), asEventId("evt-seq-2"), asEventId("evt-seq-3")],
      );
    }),
  );

  it.effect("keeps subscriber delivery ordered and isolates failing subscribers", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      yield* provider.startRuntimeEventSources!;
      yield* provider.openRuntimeEventPublishing!;
      const session = yield* provider.startSession(asThreadId("thread-1"), {
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: codexInstanceId,
        threadId: asThreadId("thread-1"),
        runtimeMode: "full-access",
      });

      const receivedByHealthy: string[] = [];
      const expectedEventIds = new Set<string>(["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"]);
      const healthyFiber = yield* Stream.take(provider.streamEvents, 3).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            receivedByHealthy.push(event.eventId);
          }),
        ),
        Effect.forkChild,
      );
      const failingFiber = yield* Stream.take(provider.streamEvents, 1).pipe(
        Stream.runForEach(() => Effect.fail("listener crash")),
        Effect.forkChild,
      );
      yield* advanceTestClock(50);

      const events: ReadonlyArray<LegacyProviderRuntimeEvent> = [
        {
          type: "tool.completed",
          eventId: asEventId("evt-ordered-1"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          toolKind: "command",
          title: "Ran command",
          detail: "echo one",
        },
        {
          type: "message.delta",
          eventId: asEventId("evt-ordered-2"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          delta: "hello",
        },
        {
          type: "turn.completed",
          eventId: asEventId("evt-ordered-3"),
          provider: ProviderDriverKind.make("codex"),
          createdAt: "2026-01-01T00:00:00.000Z",
          threadId: session.threadId,
          turnId: asTurnId("turn-1"),
          status: "completed",
        },
      ];

      for (const event of events) {
        fanout.codex.emit(event);
      }
      const failingResult = yield* Effect.result(Fiber.join(failingFiber));
      assert.equal(failingResult._tag, "Failure");
      yield* Fiber.join(healthyFiber);

      assert.deepEqual(
        receivedByHealthy.filter((eventId) => expectedEventIds.has(eventId)).slice(0, 3),
        ["evt-ordered-1", "evt-ordered-2", "evt-ordered-3"],
      );
    }),
  );

  it.effect("records provider metrics with the routed provider label", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const session = yield* provider.startSession(asThreadId("thread-metrics"), {
        provider: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claudeAgentInstanceId,
        threadId: asThreadId("thread-metrics"),
        cwd: "/tmp/project",
        runtimeMode: "full-access",
      });

      yield* provider.interruptTurn({ threadId: session.threadId });
      yield* provider.respondToRequest({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-1"),
        decision: "accept",
      });
      yield* provider.respondToUserInput({
        threadId: session.threadId,
        requestId: asRequestId("req-metrics-2"),
        answers: {
          sandbox_mode: "workspace-write",
        },
      });
      yield* provider.rollbackConversation({
        threadId: session.threadId,
        numTurns: 1,
      });
      yield* provider.stopSession({ threadId: session.threadId });

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "interrupt",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "approval-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "user-input-response",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "rollback",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_provider_sessions_total", {
          provider: ProviderDriverKind.make("claudeAgent"),
          operation: "stop",
          outcome: "success",
        }),
        true,
      );
    }),
  );

  it.effect(
    "records sendTurn metrics with the resolved provider when modelSelection is omitted",
    () =>
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;

        const session = yield* provider.startSession(asThreadId("thread-send-metrics"), {
          provider: ProviderDriverKind.make("claudeAgent"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-send-metrics"),
          cwd: "/tmp/project-send-metrics",
          runtimeMode: "full-access",
        });

        yield* provider.sendTurn({
          threadId: session.threadId,
          input: "hello",
          attachments: [],
        });

        const snapshots = yield* Metric.snapshot;

        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turns_total", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
            outcome: "success",
          }),
          true,
        );
        assert.equal(
          hasMetricSnapshot(snapshots, "t3_provider_turn_duration", {
            provider: ProviderDriverKind.make("claudeAgent"),
            operation: "send",
          }),
          true,
        );
      }),
  );
});

const attemptLifecycle = makeProviderServiceLayer();
attemptLifecycle.layer("ProviderServiceLive attempt lifecycle", (it) => {
  it.effect("discards parked adapter event fibers with a failed attempt and retries fresh", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const output = yield* provider.subscribeEvents!;
      const failedScope = yield* Scope.make("sequential");
      yield* provider.startRuntimeEventSources!.pipe(Scope.provide(failedScope));
      attemptLifecycle.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-failed-provider-attempt"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:00.000Z",
        threadId: asThreadId("thread-provider-attempt"),
        turnId: asTurnId("turn-failed-provider-attempt"),
      });
      yield* advanceTestClock(50);
      yield* Scope.close(failedScope, Exit.void);

      const retryScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(retryScope, Exit.void));
      yield* provider.startRuntimeEventSources!.pipe(Scope.provide(retryScope));
      const observed = yield* PubSub.take(output).pipe(Effect.forkChild);
      attemptLifecycle.codex.emit({
        type: "turn.started",
        eventId: asEventId("evt-retry-provider-attempt"),
        provider: ProviderDriverKind.make("codex"),
        createdAt: "2026-01-01T00:00:01.000Z",
        threadId: asThreadId("thread-provider-attempt"),
        turnId: asTurnId("turn-retry-provider-attempt"),
      });
      yield* provider.openRuntimeEventPublishing!;
      yield* advanceTestClock(50);

      assert.equal((yield* Fiber.join(observed)).eventId, "evt-retry-provider-attempt");
      assert.deepStrictEqual(yield* PubSub.takeUpTo(output, 16), []);
    }),
  );
});

let observeAtomicAccepted: (event: ProviderRuntimeEvent) => Effect.Effect<void> = () => Effect.void;
let observeAtomicIntakeClosed: Effect.Effect<void> = Effect.void;
let observeAtomicQuiesceStarted: Effect.Effect<void> = Effect.void;
let observeAtomicBeforePull: (source: {
  readonly instanceId: ProviderInstanceId;
}) => Effect.Effect<void> = () => Effect.void;
const atomicCutoverLifecycle = makeProviderServiceLayer({
  runtimeEventLifecycleObserver: {
    beforePull: (source) => Effect.suspend(() => observeAtomicBeforePull(source)),
    onAccepted: (event) => Effect.suspend(() => observeAtomicAccepted(event)),
    onQuiesceStarted: Effect.suspend(() => observeAtomicQuiesceStarted),
    onIntakeClosed: Effect.suspend(() => observeAtomicIntakeClosed),
  },
});

atomicCutoverLifecycle.layer("ProviderServiceLive atomic event cutover", (it) => {
  it.effect(
    "drains a pre-cutover adapter event through both consumers before immediate close",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          const resourcesScope = yield* Scope.make("sequential");
          const finalized = yield* Ref.make(false);
          yield* Scope.addFinalizer(resourcesScope, Ref.set(finalized, true));
          const attempt = yield* makeReactorStartupAttempt(resourcesScope);
          const runtimeSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
            Scope.provide(resourcesScope),
          );
          const verificationSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
            Scope.provide(resourcesScope),
          );
          const acceptedCutoverEvent = yield* Deferred.make<ProviderRuntimeEvent>();
          const pullEntered = yield* Deferred.make<void>();
          observeAtomicBeforePull = (source) =>
            source.instanceId === codexInstanceId
              ? Deferred.succeed(pullEntered, undefined).pipe(Effect.asVoid)
              : Effect.void;
          observeAtomicAccepted = (event) =>
            Deferred.succeed(acceptedCutoverEvent, event).pipe(Effect.asVoid);
          const eventId = asEventId("evt-atomic-cutover");
          const source = yield* provider.startRuntimeEventSources!.pipe(
            Scope.provide(resourcesScope),
          );
          yield* Deferred.await(pullEntered);
          atomicCutoverLifecycle.codex.emit({
            type: "turn.started",
            eventId,
            provider: CODEX_DRIVER,
            createdAt: "2026-08-07T20:00:00.000Z",
            threadId: asThreadId("thread-atomic-cutover"),
            turnId: asTurnId("turn-atomic-cutover"),
          });
          const runtimeEntered = yield* Deferred.make<void>();
          const verificationEntered = yield* Deferred.make<void>();
          const releaseRuntime = yield* Deferred.make<void>();
          const releaseVerification = yield* Deferred.make<void>();
          const runtimeDurable = yield* Ref.make<ReadonlyArray<string>>([]);
          const verificationDurable = yield* Ref.make<ReadonlyArray<string>>([]);

          const startConsumer = Effect.fn("ProviderServiceTest.startConsumer")(function* (input: {
            readonly subscription: PubSub.Subscription<ProviderService.ProviderRuntimeEventPublication>;
            readonly role: "runtime" | "verification";
            readonly entered: Deferred.Deferred<void>;
            readonly release: Deferred.Deferred<void>;
            readonly durable: Ref.Ref<ReadonlyArray<string>>;
          }) {
            const worker = yield* makeDrainableWorker(
              (event: ProviderRuntimeEvent) =>
                Deferred.succeed(input.entered, undefined).pipe(
                  Effect.andThen(Deferred.await(input.release)),
                  Effect.andThen(
                    Ref.update(input.durable, (events) => [...events, String(event.eventId)]),
                  ),
                ),
              { failureMode: "observable" },
            );
            yield* Stream.runForEach(Stream.fromSubscription(input.subscription), (publication) => {
              if (publication._tag === "Event") {
                const enqueue = worker.enqueue(publication.event);
                return input.role === "verification"
                  ? attempt.activation.await.pipe(Effect.andThen(enqueue))
                  : enqueue;
              }
              return Effect.gen(function* () {
                const drainExit = yield* Effect.exit(worker.drain);
                const acknowledgement =
                  input.role === "runtime"
                    ? publication.token.runtimeIngestionAcknowledgement
                    : publication.token.verificationAcknowledgement;
                yield* Deferred.done(acknowledgement, drainExit).pipe(Effect.ignore);
                if (Exit.isFailure(drainExit)) return yield* Effect.failCause(drainExit.cause);
              });
            }).pipe(
              Scope.provide(resourcesScope),
              Effect.forkIn(resourcesScope, { startImmediately: true }),
            );
          });

          yield* startConsumer({
            subscription: runtimeSubscription,
            role: "runtime",
            entered: runtimeEntered,
            release: releaseRuntime,
            durable: runtimeDurable,
          });
          yield* startConsumer({
            subscription: verificationSubscription,
            role: "verification",
            entered: verificationEntered,
            release: releaseVerification,
            durable: verificationDurable,
          });
          yield* attempt.activation.registerShutdownDrain(
            Effect.gen(function* () {
              const quiesce = yield* source.quiesce;
              const [runtimeExit, verificationExit] = yield* Effect.all(
                [
                  Effect.exit(Deferred.await(quiesce.token.runtimeIngestionAcknowledgement)),
                  Effect.exit(Deferred.await(quiesce.token.verificationAcknowledgement)),
                ],
                { concurrency: "unbounded" },
              );
              for (const exit of [quiesce.sourceExit, runtimeExit, verificationExit]) {
                if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
              }
            }),
          );

          assert.equal((yield* Deferred.await(acceptedCutoverEvent)).eventId, eventId);

          yield* attempt.commit(
            provider.openRuntimeEventPublishing!.pipe(
              Effect.andThen(source.handoffAccepted),
              Effect.andThen(attempt.activation.open),
            ),
          );
          const close = yield* attempt
            .close(Exit.interrupt("immediate-parent-close" as never))
            .pipe(Effect.forkChild({ startImmediately: true }));
          yield* Effect.all([Deferred.await(runtimeEntered), Deferred.await(verificationEntered)], {
            concurrency: "unbounded",
          });
          assert.isUndefined(close.pollUnsafe());
          assert.isFalse(yield* Ref.get(finalized));

          yield* Effect.all(
            [
              Deferred.succeed(releaseRuntime, undefined),
              Deferred.succeed(releaseVerification, undefined),
            ],
            { concurrency: "unbounded" },
          );
          const closeExit = yield* Fiber.await(close);
          assert.isTrue(
            Exit.isSuccess(closeExit),
            Exit.isFailure(closeExit) ? Cause.pretty(closeExit.cause) : undefined,
          );
          assert.deepStrictEqual(yield* Ref.get(runtimeDurable), [eventId]);
          assert.deepStrictEqual(yield* Ref.get(verificationDurable), [eventId]);
          assert.isTrue(yield* Ref.get(finalized));
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            observeAtomicAccepted = () => Effect.void;
            observeAtomicQuiesceStarted = Effect.void;
            observeAtomicIntakeClosed = Effect.void;
            observeAtomicBeforePull = () => Effect.void;
          }),
        ),
      ),
  );
});

const quiesceLifecycle = makeProviderServiceLayer({
  runtimeEventLifecycleObserver: {
    beforePull: (source) => Effect.suspend(() => observeAtomicBeforePull(source)),
    onAccepted: (event) => Effect.suspend(() => observeAtomicAccepted(event)),
    onQuiesceStarted: Effect.suspend(() => observeAtomicQuiesceStarted),
    onIntakeClosed: Effect.suspend(() => observeAtomicIntakeClosed),
  },
});

quiesceLifecycle.layer("ProviderServiceLive finite-prefix quiesce", (it) => {
  it.effect(
    "atomically quiesces concurrent intake, preserves order, and shares one finite drain prefix",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const provider = yield* ProviderService.ProviderService;
          const lifecycle = yield* provider.subscribeRuntimeEventPublications!;
          const acceptedIds = yield* Ref.make<ReadonlyArray<string>>([]);
          const firstPrefixAccepted = yield* Deferred.make<void>();
          const concurrentAccepted = yield* Deferred.make<void>();
          const quiesceStarted = yield* Deferred.make<void>();
          const releaseQuiesce = yield* Deferred.make<void>();
          const intakeClosed = yield* Deferred.make<void>();
          const pullEntered = yield* Deferred.make<void>();
          observeAtomicBeforePull = (source) =>
            source.instanceId === codexInstanceId
              ? Deferred.succeed(pullEntered, undefined).pipe(Effect.asVoid)
              : Effect.void;
          observeAtomicAccepted = (event) =>
            Ref.update(acceptedIds, (ids) => [...ids, String(event.eventId)]).pipe(
              Effect.andThen(
                Ref.get(acceptedIds).pipe(
                  Effect.flatMap((ids) =>
                    ids.length === 2
                      ? Deferred.succeed(firstPrefixAccepted, undefined).pipe(Effect.asVoid)
                      : Effect.void,
                  ),
                ),
              ),
              Effect.andThen(
                event.eventId === "evt-quiesce-3"
                  ? Deferred.succeed(concurrentAccepted, undefined).pipe(Effect.asVoid)
                  : Effect.void,
              ),
            );
          observeAtomicQuiesceStarted = Deferred.succeed(quiesceStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseQuiesce)),
          );
          observeAtomicIntakeClosed = Deferred.succeed(intakeClosed, undefined).pipe(Effect.asVoid);
          const source = yield* provider.startRuntimeEventSources!;
          yield* Deferred.await(pullEntered);
          for (const index of [1, 2]) {
            quiesceLifecycle.codex.emit({
              type: "turn.started",
              eventId: asEventId(`evt-quiesce-${index}`),
              provider: CODEX_DRIVER,
              createdAt: `2026-08-07T20:00:0${index}.000Z`,
              threadId: asThreadId("thread-quiesce-prefix"),
              turnId: asTurnId(`turn-quiesce-${index}`),
            });
          }
          yield* provider.openRuntimeEventPublishing!;
          yield* Deferred.await(firstPrefixAccepted);
          const firstQuiesce = yield* source.quiesce.pipe(Effect.forkChild);
          yield* Deferred.await(quiesceStarted);
          quiesceLifecycle.codex.emit({
            type: "turn.started",
            eventId: asEventId("evt-quiesce-3"),
            provider: CODEX_DRIVER,
            createdAt: "2026-08-07T20:00:03.000Z",
            threadId: asThreadId("thread-quiesce-prefix"),
            turnId: asTurnId("turn-quiesce-3"),
          });
          yield* Deferred.await(concurrentAccepted);
          assert.isFalse(yield* Deferred.isDone(intakeClosed));
          yield* Deferred.succeed(releaseQuiesce, undefined);
          const firstResult = yield* Fiber.join(firstQuiesce);

          const repeatedResults = yield* Effect.all([source.quiesce, source.quiesce], {
            concurrency: "unbounded",
          });
          const quiesceResults = [firstResult, ...repeatedResults];
          assert.isTrue(yield* Deferred.isDone(intakeClosed));
          assert.deepStrictEqual(
            quiesceResults.map((result) => result.token.id),
            [quiesceResults[0]!.token.id, quiesceResults[0]!.token.id, quiesceResults[0]!.token.id],
          );
          for (const result of quiesceResults) assert.isTrue(Exit.isSuccess(result.sourceExit));

          quiesceLifecycle.codex.emit({
            type: "turn.started",
            eventId: asEventId("evt-after-quiesce"),
            provider: CODEX_DRIVER,
            createdAt: "2026-08-07T20:00:04.000Z",
            threadId: asThreadId("thread-quiesce-prefix"),
            turnId: asTurnId("turn-after-quiesce"),
          });

          const publications = yield* Effect.forEach([0, 1, 2, 3], () => PubSub.take(lifecycle));
          assert.deepStrictEqual(
            publications.map((publication) =>
              publication._tag === "Event" ? publication.event.eventId : "drain",
            ),
            ["evt-quiesce-1", "evt-quiesce-2", "evt-quiesce-3", "drain"],
          );

          yield* Effect.yieldNow;
          assert.deepStrictEqual(yield* Ref.get(acceptedIds), [
            "evt-quiesce-1",
            "evt-quiesce-2",
            "evt-quiesce-3",
          ]);
          assert.deepStrictEqual(yield* PubSub.takeUpTo(lifecycle, 16), []);
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            observeAtomicAccepted = () => Effect.void;
            observeAtomicQuiesceStarted = Effect.void;
            observeAtomicIntakeClosed = Effect.void;
            observeAtomicBeforePull = () => Effect.void;
          }),
        ),
      ),
  );
});

const multiChunkDefect = new Error("provider-multi-chunk-first-event-defect");
const multiChunkEvents: ReadonlyArray<ProviderRuntimeEvent> = [
  {
    type: "turn.started",
    eventId: asEventId("evt-multi-chunk-1"),
    provider: CODEX_DRIVER,
    providerInstanceId: codexInstanceId,
    createdAt: "2026-08-10T10:00:00.000Z",
    threadId: asThreadId("thread-multi-chunk"),
    turnId: asTurnId("turn-multi-chunk-1"),
    payload: {},
  },
  {
    type: "turn.started",
    eventId: asEventId("evt-multi-chunk-2"),
    provider: CODEX_DRIVER,
    providerInstanceId: codexInstanceId,
    createdAt: "2026-08-10T10:00:01.000Z",
    threadId: asThreadId("thread-multi-chunk"),
    turnId: asTurnId("turn-multi-chunk-2"),
    payload: {},
  },
];
let observeMultiChunkAccepted: (event: ProviderRuntimeEvent) => Effect.Effect<void> = () =>
  Effect.void;
const multiChunkAdapter = makeFakeCodexAdapter(CODEX_DRIVER, {
  runtimeEventStream: Stream.fromIterable(multiChunkEvents),
});
const multiChunkLifecycle = makeProviderServiceLayer(
  {
    canonicalEventLogger: {
      filePath: "memory://provider-multi-chunk",
      write: () => Effect.die(multiChunkDefect),
      close: () => Effect.void,
    },
    runtimeEventLifecycleObserver: {
      onAccepted: (event) => Effect.suspend(() => observeMultiChunkAccepted(event)),
    },
  },
  { codex: multiChunkAdapter },
);

multiChunkLifecycle.layer("ProviderServiceLive atomic adapter chunks", (it) => {
  it.effect("accepts every event in one pulled chunk when canonical logging defects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const accepted = yield* Ref.make<ReadonlyArray<string>>([]);
        const chunkAccepted = yield* Deferred.make<void>();
        observeMultiChunkAccepted = (event) =>
          Ref.updateAndGet(accepted, (ids) => [...ids, String(event.eventId)]).pipe(
            Effect.flatMap((ids) =>
              ids.length === multiChunkEvents.length
                ? Deferred.succeed(chunkAccepted, undefined)
                : Effect.void,
            ),
            Effect.asVoid,
          );
        const attemptScope = yield* Scope.make("sequential");
        const finalized = yield* Ref.make(false);
        yield* Scope.addFinalizer(attemptScope, Ref.set(finalized, true));
        yield* Effect.addFinalizer(() => Scope.close(attemptScope, Exit.void));
        const source = yield* provider.startRuntimeEventSources!.pipe(Scope.provide(attemptScope));

        yield* Deferred.await(chunkAccepted);
        assert.deepStrictEqual(yield* Ref.get(accepted), [
          "evt-multi-chunk-1",
          "evt-multi-chunk-2",
        ]);
        yield* provider.openRuntimeEventPublishing!;
        const handoffExit = yield* Effect.exit(source.handoffAccepted);
        assert.isTrue(Exit.isSuccess(handoffExit));
        const quiesce = yield* source.quiesce;
        assert.isTrue(Exit.isSuccess(quiesce.sourceExit));
        yield* Scope.close(attemptScope, Exit.void);
        assert.isTrue(yield* Ref.get(finalized));
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          observeMultiChunkAccepted = () => Effect.void;
        }),
      ),
    ),
  );
});

let observeSuccessfulMultiChunkAccepted: (
  event: ProviderRuntimeEvent,
) => Effect.Effect<void> = () => Effect.void;
const successfulMultiChunkAdapter = makeFakeCodexAdapter(CODEX_DRIVER, {
  runtimeEventStream: Stream.fromIterable(multiChunkEvents),
});
const successfulMultiChunkLifecycle = makeProviderServiceLayer(
  {
    runtimeEventLifecycleObserver: {
      onAccepted: (event) => Effect.suspend(() => observeSuccessfulMultiChunkAccepted(event)),
    },
  },
  { codex: successfulMultiChunkAdapter },
);

successfulMultiChunkLifecycle.layer("ProviderServiceLive successful multi-event cutover", (it) => {
  it.effect("drains one accepted multi-event chunk through both consumers on immediate close", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const resourcesScope = yield* Scope.make("sequential");
        const finalized = yield* Ref.make(false);
        yield* Scope.addFinalizer(resourcesScope, Ref.set(finalized, true));
        const attempt = yield* makeReactorStartupAttempt(resourcesScope);
        const runtimeSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
          Scope.provide(resourcesScope),
        );
        const verificationSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
          Scope.provide(resourcesScope),
        );
        const acceptedIds = yield* Ref.make<ReadonlyArray<string>>([]);
        const chunkAccepted = yield* Deferred.make<void>();
        observeSuccessfulMultiChunkAccepted = (event) =>
          Ref.updateAndGet(acceptedIds, (ids) => [...ids, String(event.eventId)]).pipe(
            Effect.flatMap((ids) =>
              ids.length === multiChunkEvents.length
                ? Deferred.succeed(chunkAccepted, undefined)
                : Effect.void,
            ),
            Effect.asVoid,
          );
        const runtimeDurable = yield* Ref.make<ReadonlyArray<string>>([]);
        const verificationDurable = yield* Ref.make<ReadonlyArray<string>>([]);
        const startConsumer = (
          role: "runtime" | "verification",
          subscription: PubSub.Subscription<ProviderService.ProviderRuntimeEventPublication>,
          durable: Ref.Ref<ReadonlyArray<string>>,
        ) =>
          Stream.runForEach(Stream.fromSubscription(subscription), (publication) => {
            const process =
              publication._tag === "Event"
                ? Ref.update(durable, (ids) => [...ids, String(publication.event.eventId)])
                : Deferred.succeed(
                    role === "runtime"
                      ? publication.token.runtimeIngestionAcknowledgement
                      : publication.token.verificationAcknowledgement,
                    undefined,
                  ).pipe(Effect.asVoid);
            return role === "verification"
              ? attempt.activation.await.pipe(Effect.andThen(process))
              : process;
          }).pipe(
            Scope.provide(resourcesScope),
            Effect.forkIn(resourcesScope, { startImmediately: true }),
            Effect.asVoid,
          );
        yield* startConsumer("runtime", runtimeSubscription, runtimeDurable);
        yield* startConsumer("verification", verificationSubscription, verificationDurable);
        const source = yield* provider.startRuntimeEventSources!.pipe(
          Scope.provide(resourcesScope),
        );
        yield* Deferred.await(chunkAccepted);
        yield* attempt.activation.registerShutdownDrain(
          Effect.gen(function* () {
            const quiesce = yield* source.quiesce;
            if (Exit.isFailure(quiesce.sourceExit)) {
              return yield* Effect.failCause(quiesce.sourceExit.cause);
            }
            yield* Effect.all(
              [
                Deferred.await(quiesce.token.runtimeIngestionAcknowledgement),
                Deferred.await(quiesce.token.verificationAcknowledgement),
              ],
              { concurrency: "unbounded", discard: true },
            );
          }),
        );

        yield* attempt.commit(
          provider.openRuntimeEventPublishing!.pipe(
            Effect.andThen(source.handoffAccepted),
            Effect.andThen(attempt.activation.open),
          ),
        );
        yield* attempt.close(Exit.interrupt("multi-chunk-immediate-parent-close" as never));
        const expected = multiChunkEvents.map((event) => String(event.eventId));
        assert.deepStrictEqual(yield* Ref.get(acceptedIds), expected);
        assert.deepStrictEqual(yield* Ref.get(runtimeDurable), expected);
        assert.deepStrictEqual(yield* Ref.get(verificationDurable), expected);
        assert.isTrue(yield* Ref.get(finalized));
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          observeSuccessfulMultiChunkAccepted = () => Effect.void;
        }),
      ),
    ),
  );
});

const fanoutPublishDefect = new Error("provider-runtime-fanout-publish-defect");
let fanoutFailureAccepted: Deferred.Deferred<void> | undefined;
let fanoutPublishEntered: Deferred.Deferred<void> | undefined;
let releaseFanoutPublish: Deferred.Deferred<void> | undefined;
const fanoutFailureLifecycle = makeProviderServiceLayer({
  runtimeEventLifecycleObserver: {
    onAccepted: () =>
      fanoutFailureAccepted === undefined
        ? Effect.void
        : Deferred.succeed(fanoutFailureAccepted, undefined).pipe(Effect.asVoid),
    afterLifecyclePublish: () =>
      fanoutPublishEntered === undefined || releaseFanoutPublish === undefined
        ? Effect.die(fanoutPublishDefect)
        : Deferred.succeed(fanoutPublishEntered, undefined).pipe(
            Effect.andThen(Deferred.await(releaseFanoutPublish)),
            Effect.andThen(Effect.die(fanoutPublishDefect)),
          ),
  },
});

fanoutFailureLifecycle.layer("ProviderServiceLive terminal fan-out abort", (it) => {
  it.effect("aborts a parked lifecycle consumer when downstream publication fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const resourcesScope = yield* Scope.make("sequential");
        const resourcesFinalized = yield* Ref.make(false);
        yield* Scope.addFinalizer(resourcesScope, Ref.set(resourcesFinalized, true));
        const attempt = yield* makeReactorStartupAttempt(resourcesScope);
        const verificationSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
          Scope.provide(resourcesScope),
        );
        const auditSubscription = yield* provider.subscribeRuntimeEventPublications!.pipe(
          Scope.provide(resourcesScope),
        );
        const eventDequeued = yield* Deferred.make<void>();
        fanoutFailureAccepted = yield* Deferred.make<void>();
        fanoutPublishEntered = yield* Deferred.make<void>();
        releaseFanoutPublish = yield* Deferred.make<void>();
        const source = yield* provider.startRuntimeEventSources!.pipe(
          Scope.provide(resourcesScope),
        );
        yield* attempt.activation.registerTerminalAbort(source.abort);
        yield* attempt.activation.registerShutdownDrain(source.quiesce.pipe(Effect.asVoid));
        const verificationPump = yield* Effect.raceFirst(
          Stream.runForEach(Stream.fromSubscription(verificationSubscription), (publication) =>
            publication._tag === "Event"
              ? Deferred.succeed(eventDequeued, undefined).pipe(
                  Effect.andThen(attempt.activation.await),
                )
              : Effect.die("Terminal abort must not publish a drain marker."),
          ),
          source.awaitAbort,
        ).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));

        fanoutFailureLifecycle.codex.emit({
          type: "turn.started",
          eventId: asEventId("evt-fanout-publish-defect"),
          provider: CODEX_DRIVER,
          createdAt: "2026-08-10T10:01:00.000Z",
          threadId: asThreadId("thread-fanout-publish-defect"),
          turnId: asTurnId("turn-fanout-publish-defect"),
        });
        yield* Deferred.await(fanoutFailureAccepted);
        const commitFiber = yield* attempt
          .commit(
            provider.openRuntimeEventPublishing!.pipe(
              Effect.andThen(source.handoffAccepted),
              Effect.andThen(attempt.activation.open),
            ),
          )
          .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(fanoutPublishEntered);
        yield* Deferred.await(eventDequeued);
        assert.isUndefined(verificationPump.pollUnsafe());
        const auditPublication = yield* PubSub.take(auditSubscription);
        assert.equal(auditPublication._tag, "Event");
        assert.deepStrictEqual(yield* PubSub.takeUpTo(auditSubscription, 16), []);
        const closeFibers = yield* Effect.forEach(["first", "second"], (name) =>
          attempt
            .close(Exit.interrupt(`fanout-parent-close-${name}` as never))
            .pipe(Effect.exit, Effect.forkChild({ startImmediately: true })),
        );
        yield* Effect.yieldNow;
        for (const closeFiber of closeFibers) assert.isUndefined(closeFiber.pollUnsafe());
        assert.isFalse(yield* Ref.get(resourcesFinalized));
        yield* Deferred.succeed(releaseFanoutPublish, undefined);
        const handoffExit = yield* Fiber.join(commitFiber);
        assert.isTrue(Exit.isFailure(handoffExit));
        if (Exit.isSuccess(handoffExit)) return;
        assert.isTrue(
          handoffExit.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect === fanoutPublishDefect,
          ),
        );

        const verificationExit = yield* Fiber.join(verificationPump);
        assert.isTrue(Exit.isFailure(verificationExit));
        if (Exit.isFailure(verificationExit)) {
          assert.isTrue(
            verificationExit.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === fanoutPublishDefect,
            ),
          );
        }
        assert.isTrue(Exit.isFailure(yield* Effect.exit(source.quiesce)));
        for (const closeFiber of closeFibers) {
          const closeExit = yield* Fiber.join(closeFiber);
          assert.isTrue(Exit.isFailure(closeExit));
          if (Exit.isFailure(closeExit)) {
            assert.isTrue(
              closeExit.cause.reasons.some(
                (reason) => Cause.isDieReason(reason) && reason.defect === fanoutPublishDefect,
              ),
            );
          }
        }
        assert.isTrue(yield* Ref.get(resourcesFinalized));
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          fanoutFailureAccepted = undefined;
          fanoutPublishEntered = undefined;
          releaseFanoutPublish = undefined;
        }),
      ),
    ),
  );
});

const canonicalPumpDefect = new Error("provider-canonical-pump-defect");
let canonicalFailureAccepted: Deferred.Deferred<void> | undefined;
const canonicalFailureLifecycle = makeProviderServiceLayer({
  canonicalEventLogger: {
    filePath: "memory://provider-canonical-pump-defect",
    write: () => Effect.die(canonicalPumpDefect),
    close: () => Effect.void,
  },
  runtimeEventLifecycleObserver: {
    beforePull: (source) => Effect.suspend(() => observeAtomicBeforePull(source)),
    onAccepted: () =>
      canonicalFailureAccepted === undefined
        ? Effect.void
        : Deferred.succeed(canonicalFailureAccepted, undefined).pipe(Effect.asVoid),
  },
});

canonicalFailureLifecycle.layer("ProviderServiceLive canonical logger isolation", (it) => {
  it.effect("ignores a canonical-log defect without changing publication or drain", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        const lifecycle = yield* provider.subscribeRuntimeEventPublications!;
        const runtime = yield* provider.subscribeEvents!;
        const runtimeTake = yield* PubSub.take(runtime).pipe(Effect.forkChild);
        canonicalFailureAccepted = yield* Deferred.make<void>();
        const pullEntered = yield* Deferred.make<void>();
        observeAtomicBeforePull = (source) =>
          source.instanceId === codexInstanceId
            ? Deferred.succeed(pullEntered, undefined).pipe(Effect.asVoid)
            : Effect.void;
        const source = yield* provider.startRuntimeEventSources!;
        yield* Deferred.await(pullEntered);
        canonicalFailureLifecycle.codex.emit({
          type: "turn.started",
          eventId: asEventId("evt-canonical-pump-defect"),
          provider: CODEX_DRIVER,
          createdAt: "2026-08-07T20:01:00.000Z",
          threadId: asThreadId("thread-canonical-pump-defect"),
          turnId: asTurnId("turn-canonical-pump-defect"),
        });
        yield* Deferred.await(canonicalFailureAccepted);
        yield* provider.openRuntimeEventPublishing!;
        yield* source.handoffAccepted;
        assert.equal((yield* Fiber.join(runtimeTake)).eventId, "evt-canonical-pump-defect");

        const results = yield* Effect.all([source.quiesce, source.quiesce], {
          concurrency: "unbounded",
        });
        for (const result of results) {
          assert.isTrue(Exit.isSuccess(result.sourceExit));
        }
        assert.equal(results[0].token.id, results[1].token.id);
        assert.deepStrictEqual(
          (yield* PubSub.takeUpTo(lifecycle, 16)).map((publication) => publication._tag),
          ["Event", "Drain"],
        );
        canonicalFailureAccepted = undefined;
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          canonicalFailureAccepted = undefined;
          observeAtomicBeforePull = () => Effect.void;
        }),
      ),
    ),
  );
});

let canonicalInterruptPullEntered: Deferred.Deferred<void> | undefined;
const canonicalInterruptLifecycle = makeProviderServiceLayer({
  canonicalEventLogger: {
    filePath: "memory://provider-canonical-interrupt",
    write: () => Effect.interrupt,
    close: () => Effect.void,
  },
  runtimeEventLifecycleObserver: {
    beforePull: () =>
      canonicalInterruptPullEntered === undefined
        ? Effect.void
        : Deferred.succeed(canonicalInterruptPullEntered, undefined).pipe(Effect.asVoid),
  },
});

canonicalInterruptLifecycle.layer("ProviderServiceLive canonical logger interrupt", (it) => {
  it.effect("preserves a canonical-log interrupt as the source terminal cause", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const provider = yield* ProviderService.ProviderService;
        canonicalInterruptPullEntered = yield* Deferred.make<void>();
        const source = yield* provider.startRuntimeEventSources!;
        yield* Deferred.await(canonicalInterruptPullEntered);
        yield* provider.openRuntimeEventPublishing!;
        canonicalInterruptLifecycle.codex.emit({
          type: "turn.started",
          eventId: asEventId("evt-canonical-interrupt"),
          provider: CODEX_DRIVER,
          createdAt: "2026-08-07T20:02:00.000Z",
          threadId: asThreadId("thread-canonical-interrupt"),
          turnId: asTurnId("turn-canonical-interrupt"),
        });
        const quiesced = yield* source.quiesce;
        assert.isTrue(Exit.isFailure(quiesced.sourceExit));
        if (Exit.isFailure(quiesced.sourceExit)) {
          assert.isTrue(Cause.hasInterrupts(quiesced.sourceExit.cause));
        }
      }),
    ).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          canonicalInterruptPullEntered = undefined;
        }),
      ),
    ),
  );
});

const validation = makeProviderServiceLayer();

it("rejects runtime events emitted for a different provider instance", () => {
  const event: ProviderRuntimeEvent = {
    type: "turn.completed",
    eventId: asEventId("evt-instance-mismatch"),
    provider: CODEX_DRIVER,
    providerInstanceId: ProviderInstanceId.make("codex_personal"),
    createdAt: "2026-01-01T00:00:00.000Z",
    threadId: asThreadId("thread-instance-mismatch"),
    turnId: asTurnId("turn-instance-mismatch"),
    payload: { state: "completed" },
  };

  assert.throws(
    () =>
      correlateRuntimeEventWithInstance(
        { instanceId: ProviderInstanceId.make("codex_work"), provider: CODEX_DRIVER },
        event,
      ),
    /emitted event for instance 'codex_personal'/u,
  );
});

it("rejects runtime events emitted without a provider instance id", () => {
  const event: ProviderRuntimeEvent = {
    type: "turn.completed",
    eventId: asEventId("evt-instance-missing"),
    provider: CODEX_DRIVER,
    createdAt: "2026-01-01T00:00:00.000Z",
    threadId: asThreadId("thread-instance-missing"),
    turnId: asTurnId("turn-instance-missing"),
    payload: { state: "completed" },
  };

  assert.throws(
    () =>
      correlateRuntimeEventWithInstance(
        { instanceId: ProviderInstanceId.make("codex_work"), provider: CODEX_DRIVER },
        event,
      ),
    /emitted an event without a provider instance id/u,
  );
});

validation.layer("ProviderServiceLive validation", (it) => {
  it.effect("quarantines legacy routing while healthy sessions remain usable and repairable", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;
      const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
      const runtimeRepository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
      const legacyThreadId = asThreadId("thread-legacy-routing");
      const healthyThreadId = asThreadId("thread-healthy-routing");

      yield* provider.startSession(healthyThreadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId: healthyThreadId,
        runtimeMode: "full-access",
      });

      yield* runtimeRepository.upsert({
        threadId: legacyThreadId,
        providerName: "codex",
        providerInstanceId: null,
        adapterKey: "codex",
        runtimeMode: "full-access",
        status: "running",
        lastSeenAt: "2026-01-01T00:00:00.000Z",
        resumeCursor: null,
        runtimePayload: null,
      });

      const bindingError = yield* directory.getBinding(legacyThreadId).pipe(Effect.flip);
      assert.include(bindingError.detail, "cannot be routed safely");

      const sessionsBeforeRepair = yield* provider.listSessions();
      assert.equal(
        sessionsBeforeRepair.some((session) => session.threadId === healthyThreadId),
        true,
      );
      assert.equal(
        sessionsBeforeRepair.some((session) => session.threadId === legacyThreadId),
        false,
      );

      yield* provider.startSession(legacyThreadId, {
        provider: CODEX_DRIVER,
        providerInstanceId: codexInstanceId,
        threadId: legacyThreadId,
        runtimeMode: "full-access",
      });

      const repaired = yield* runtimeRepository.getByThreadId({ threadId: legacyThreadId });
      assert.equal(Option.isSome(repaired), true);
      if (Option.isSome(repaired)) {
        assert.equal(repaired.value.providerInstanceId, codexInstanceId);
      }

      const sessionsAfterRepair = yield* provider.listSessions();
      assert.equal(
        sessionsAfterRepair.some((session) => session.threadId === legacyThreadId),
        true,
      );
    }),
  );

  it.effect("rejects session starts without an explicit provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-missing-instance-id"), {
          provider: ProviderDriverKind.make("codex"),
          threadId: asThreadId("thread-missing-instance-id"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "Provider instance id is required for provider 'codex'.");
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("rejects mismatched provider kind and provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockClear();
      validation.claude.startSession.mockClear();
      const failure = yield* Effect.flip(
        provider.startSession(asThreadId("thread-instance-mismatch"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: claudeAgentInstanceId,
          threadId: asThreadId("thread-instance-mismatch"),
          runtimeMode: "full-access",
        }),
      );

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(
        failure.issue,
        "Provider instance 'claudeAgent' belongs to driver 'claudeAgent', not 'codex'.",
      );
      assert.equal(validation.codex.startSession.mock.calls.length, 0);
      assert.equal(validation.claude.startSession.mock.calls.length, 0);
    }),
  );

  it.effect("returns ProviderValidationError for invalid input payloads", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      const failure = yield* Effect.result(
        provider.startSession(asThreadId("thread-validation"), {
          threadId: asThreadId("thread-validation"),
          provider: "invalid-provider",
          providerInstanceId: codexInstanceId,
          runtimeMode: "full-access",
        } as never),
      );

      assert.equal(failure._tag, "Failure");
      if (failure._tag !== "Failure") {
        return;
      }
      assert.equal(failure.failure._tag, "ProviderValidationError");
      if (failure.failure._tag !== "ProviderValidationError") {
        return;
      }
      assert.equal(failure.failure.operation, "ProviderService.startSession");
      assert.equal(failure.failure.issue.includes("invalid-provider"), true);
    }),
  );

  it.effect("rejects a session missing the adapter's bound provider instance id", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockImplementationOnce((input: ProviderSessionStartInput) =>
        Effect.sync(() => {
          const now = "2026-01-01T00:00:00.000Z";
          return {
            provider: ProviderDriverKind.make("codex"),
            status: "ready",
            threadId: input.threadId,
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? process.cwd(),
            createdAt: now,
            updatedAt: now,
          } as unknown as ProviderSession;
        }),
      );

      const failure = yield* provider
        .startSession(asThreadId("thread-missing"), {
          provider: ProviderDriverKind.make("codex"),
          providerInstanceId: codexInstanceId,
          threadId: asThreadId("thread-missing"),
          cwd: "/tmp/project",
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "returned a session without a provider instance id");
    }),
  );

  it.effect("rejects a session emitted for a different provider instance", () =>
    Effect.gen(function* () {
      const provider = yield* ProviderService.ProviderService;

      validation.codex.startSession.mockImplementationOnce((input) =>
        Effect.succeed({
          provider: CODEX_DRIVER,
          providerInstanceId: ProviderInstanceId.make("codex_work"),
          status: "ready",
          threadId: input.threadId,
          runtimeMode: input.runtimeMode,
          cwd: input.cwd ?? process.cwd(),
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      );

      const failure = yield* provider
        .startSession(asThreadId("thread-session-instance-mismatch"), {
          provider: CODEX_DRIVER,
          providerInstanceId: codexInstanceId,
          threadId: asThreadId("thread-session-instance-mismatch"),
          runtimeMode: "full-access",
        })
        .pipe(Effect.flip);

      assert.instanceOf(failure, ProviderValidationError);
      assert.include(failure.issue, "requested 'codex', received 'codex_work'");
    }),
  );
});
