import { assert, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, ThreadId, TurnId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import type { JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  AgentControlVerificationOrchestrationHistoryError,
  selectVerificationProviderStart,
  selectVerificationProviderTerminal,
  type VerificationProviderStartHistoryEntry,
  type VerificationProviderTerminalHistoryEntry,
} from "./orchestrationTerminalHistory.ts";

const identity = {
  threadId: ThreadId.make("verification-thread"),
  providerInstanceId: ProviderInstanceId.make("codex-main"),
  providerTurnId: TurnId.make("provider-turn"),
  runtimeMode: "approval-required",
  turnRequestStreamVersion: 4,
} as const;

const start = (
  streamVersion: number,
  overrides: Partial<VerificationProviderStartHistoryEntry> = {},
): VerificationProviderStartHistoryEntry => {
  const {
    session: sessionOverrides,
    payload: payloadOverrides,
    metadata: metadataOverrides,
    envelopeLineage: envelopeLineageOverrides,
    occurredAt = "2020-01-01T00:00:00.000Z",
    lifecycle,
    ...entryOverrides
  } = overrides;
  const session: VerificationProviderStartHistoryEntry["session"] = {
    threadId: identity.threadId,
    status: "running",
    providerName: "codex",
    providerInstanceId: identity.providerInstanceId,
    runtimeMode: identity.runtimeMode,
    activeTurnId: identity.providerTurnId,
    lastError: null,
    updatedAt: occurredAt,
    ...sessionOverrides,
  };
  return {
    streamVersion,
    actorKind: "provider",
    eventType: "thread.session-set",
    occurredAt,
    envelopeLineage: {
      eventId: `orchestration-start-${streamVersion}`,
      commandId: `provider:start:${streamVersion}`,
      causationEventId: null,
      correlationId: `provider:start:${streamVersion}`,
      ...envelopeLineageOverrides,
    },
    payload: payloadOverrides ?? { threadId: identity.threadId, session },
    metadata:
      metadataOverrides ?? (lifecycle === undefined ? {} : { providerRuntimeLifecycle: lifecycle }),
    ...entryOverrides,
    session,
    lifecycle,
  };
};

const expectHistoryError = Effect.fn("expectVerificationStartHistoryError")(function* (
  entries: ReadonlyArray<VerificationProviderStartHistoryEntry>,
  operation: string,
) {
  const cause = yield* Effect.flip(selectVerificationProviderStart(entries, identity));
  assert.instanceOf(cause, AgentControlVerificationOrchestrationHistoryError);
  assert.equal(cause.operation, operation);
});

it.effect("selects one metadata-less pre-059 start without any timestamp equality", () =>
  Effect.gen(function* () {
    const selected = yield* selectVerificationProviderStart(
      [
        start(5, { occurredAt: "2001-02-03T04:05:06.007Z" }),
        start(6, {
          session: {
            threadId: identity.threadId,
            status: "ready",
            providerName: "codex",
            providerInstanceId: identity.providerInstanceId,
            runtimeMode: identity.runtimeMode,
            activeTurnId: null,
            lastError: null,
            updatedAt: "2020-01-01T00:00:00.000Z",
          },
        }),
      ],
      identity,
    );
    assert.deepStrictEqual(selected, { _tag: "Ready", index: 0 });
  }),
);

it.effect("ignores foreign provider and turn starts and keeps waiting without a match", () =>
  Effect.gen(function* () {
    const selected = yield* selectVerificationProviderStart(
      [
        start(5, {
          session: { ...start(5).session, activeTurnId: TurnId.make("foreign-turn") },
        }),
        start(6, {
          session: {
            ...start(6).session,
            providerInstanceId: ProviderInstanceId.make("foreign-provider"),
          },
        }),
      ],
      identity,
    );
    assert.deepStrictEqual(selected, { _tag: "Waiting" });
  }),
);

it.effect("collapses only an authoritative replay of the same runtime start", () =>
  Effect.gen(function* () {
    const lifecycle = {
      runtimeEventId: EventId.make("runtime-start"),
      runtimeEventType: "turn.started" as const,
      providerInstanceId: identity.providerInstanceId,
      providerTurnId: identity.providerTurnId,
    };
    const first = start(5, { lifecycle });
    const selected = yield* selectVerificationProviderStart(
      [
        first,
        start(6, {
          lifecycle,
          envelopeLineage: {
            eventId: "orchestration-start-replay",
            commandId: "provider:start:replay",
            causationEventId: null,
            correlationId: "provider:start:replay",
          },
        }),
      ],
      identity,
    );
    assert.deepStrictEqual(selected, { _tag: "Ready", index: 1 });
  }),
);

it.effect("compares complete start payload, metadata, and normalized lineage", () =>
  Effect.gen(function* () {
    const lifecycle = {
      runtimeEventId: EventId.make("runtime-start"),
      runtimeEventType: "turn.started" as const,
      providerInstanceId: identity.providerInstanceId,
      providerTurnId: identity.providerTurnId,
    };
    const first = start(5, { lifecycle });
    yield* expectHistoryError(
      [
        first,
        start(6, {
          lifecycle,
          metadata: {
            providerRuntimeLifecycle: lifecycle,
            ingestedAt: "2020-01-01T00:00:01.000Z",
          },
        }),
      ],
      "provider-start-ambiguous",
    );
    yield* expectHistoryError(
      [
        first,
        start(6, {
          lifecycle,
          payload: {
            threadId: identity.threadId,
            session: { ...first.session, lastError: "different payload" },
          },
          session: { ...first.session, lastError: "different payload" },
        }),
      ],
      "provider-start-ambiguous",
    );

    const badLineage = yield* Effect.flip(
      selectVerificationProviderStart(
        [
          first,
          start(6, {
            lifecycle,
            envelopeLineage: {
              eventId: "orchestration-start-replay",
              commandId: "provider:start:replay",
              causationEventId: null,
              correlationId: "unrelated-command",
            },
          }),
        ],
        identity,
      ),
    );
    assert.instanceOf(badLineage, AgentControlVerificationOrchestrationHistoryError);
    assert.equal(badLineage.operation, "provider-start-envelope-lineage");
  }),
);

it.effect("fails closed for duplicate legacy or contradictory runtime starts", () =>
  Effect.gen(function* () {
    yield* expectHistoryError([start(5), start(6)], "provider-start-ambiguous");
    yield* expectHistoryError(
      [
        start(5, {
          lifecycle: {
            runtimeEventId: EventId.make("runtime-start-a"),
            runtimeEventType: "turn.started",
            providerInstanceId: identity.providerInstanceId,
            providerTurnId: identity.providerTurnId,
          },
        }),
        start(6, {
          lifecycle: {
            runtimeEventId: EventId.make("runtime-start-b"),
            runtimeEventType: "turn.started",
            providerInstanceId: identity.providerInstanceId,
            providerTurnId: identity.providerTurnId,
          },
        }),
      ],
      "provider-start-ambiguous",
    );
  }),
);

it.effect("rejects a matching start before the durable turn request", () =>
  expectHistoryError([start(3), start(5)], "provider-start-before-turn-request"),
);

const terminalIdentity = {
  providerDeliveryId: "verification-delivery",
  threadId: identity.threadId,
  providerInstanceId: identity.providerInstanceId,
  providerTurnId: identity.providerTurnId,
} as const;

const terminal = (
  streamVersion: number,
  overrides: Partial<VerificationProviderTerminalHistoryEntry> = {},
): VerificationProviderTerminalHistoryEntry => {
  const source = {
    runtimeEventId: EventId.make("runtime-terminal"),
    runtimeEventType: "turn.completed",
    threadId: identity.threadId,
    providerInstanceId: identity.providerInstanceId,
    providerTurnId: identity.providerTurnId,
    providerState: "completed",
    terminalAt: "2020-01-01T00:00:01.000Z",
  } as const;
  const session = {
    activeTurnId: null,
    lastError: null,
    providerInstanceId: identity.providerInstanceId,
    providerName: "codex",
    runtimeMode: identity.runtimeMode,
    status: "ready",
    threadId: identity.threadId,
    updatedAt: "2020-01-01T00:00:01.000Z",
  } as const;
  return {
    streamVersion,
    envelopeLineage: {
      eventId: `orchestration-terminal-${streamVersion}`,
      commandId: `provider-terminal-command-${streamVersion}`,
      causationEventId: null,
      correlationId: `provider-terminal-command-${streamVersion}`,
    },
    source,
    payload: { threadId: identity.threadId, session },
    metadata: {
      providerRuntimeLifecycle: {
        runtimeEventId: source.runtimeEventId,
        runtimeEventType: source.runtimeEventType,
        providerInstanceId: source.providerInstanceId,
        providerTurnId: source.providerTurnId,
        providerState: source.providerState,
      },
    },
    ...overrides,
  };
};

const expectTerminalHistoryError = Effect.fn("expectVerificationTerminalHistoryError")(function* (
  entries: ReadonlyArray<VerificationProviderTerminalHistoryEntry>,
) {
  const cause = yield* Effect.flip(selectVerificationProviderTerminal(entries, terminalIdentity));
  assert.instanceOf(cause, AgentControlVerificationOrchestrationHistoryError);
  assert.equal(cause.operation, "provider-terminal-ambiguous");
  assert.equal(cause.reason, "terminal-conflict");
});

it.effect("collapses legitimate self-correlated replay commands into one Ready observation", () =>
  Effect.gen(function* () {
    const single = yield* selectVerificationProviderTerminal([terminal(6)], terminalIdentity);
    const replayed = yield* selectVerificationProviderTerminal(
      [terminal(6), terminal(7)],
      terminalIdentity,
    );
    assert.equal(single._tag, "Ready");
    assert.equal(replayed._tag, "Ready");
    if (single._tag === "Ready" && replayed._tag === "Ready") {
      assert.deepStrictEqual(replayed.observation, single.observation);
      assert.equal(replayed.observation.observationDigest, single.observation.observationDigest);
    }
  }),
);

it.effect("collapses three exact terminal replays independently of their stream versions", () =>
  Effect.gen(function* () {
    const selected = yield* selectVerificationProviderTerminal(
      [terminal(6), terminal(8), terminal(11)],
      terminalIdentity,
    );
    assert.equal(selected._tag, "Ready");
    if (selected._tag === "Ready") {
      assert.equal(selected.observation.runtimeEventId, "runtime-terminal");
      assert.equal(selected.observation.deliveryState, "completed");
    }
  }),
);

it.effect("fails closed when one replay changes the terminal timestamp", () =>
  expectTerminalHistoryError([
    terminal(6),
    terminal(7, {
      source: { ...terminal(7).source, terminalAt: "2020-01-01T00:00:02.000Z" },
    }),
  ]),
);

it.effect("fails closed when one replay changes provider state or runtime event type", () =>
  Effect.gen(function* () {
    yield* expectTerminalHistoryError([
      terminal(6),
      terminal(7, {
        source: {
          ...terminal(7).source,
          runtimeEventType: "turn.completed",
          providerState: "failed",
        },
      }),
    ]);
    yield* expectTerminalHistoryError([
      terminal(6),
      terminal(7, {
        source: {
          runtimeEventId: EventId.make("runtime-terminal"),
          runtimeEventType: "turn.aborted",
          threadId: identity.threadId,
          providerInstanceId: identity.providerInstanceId,
          providerTurnId: identity.providerTurnId,
          terminalAt: "2020-01-01T00:00:01.000Z",
        },
      }),
    ]);
  }),
);

it.effect("fails closed for different runtime event ids with the same normalized outcome", () =>
  expectTerminalHistoryError([
    terminal(6),
    terminal(7, {
      source: {
        ...terminal(7).source,
        runtimeEventId: EventId.make("runtime-terminal-replacement"),
      },
    }),
  ]),
);

it.effect("fails closed for non-production terminal envelope lineage", () =>
  Effect.gen(function* () {
    for (const envelopeLineage of [
      {
        ...terminal(6).envelopeLineage,
        causationEventId: "unexpected-causation-event",
      },
      {
        ...terminal(6).envelopeLineage,
        correlationId: "unrelated-correlation",
      },
      {
        ...terminal(6).envelopeLineage,
        commandId: null,
        correlationId: null,
      },
    ]) {
      const cause = yield* Effect.flip(
        selectVerificationProviderTerminal(
          [terminal(6), terminal(7, { envelopeLineage })],
          terminalIdentity,
        ),
      );
      assert.instanceOf(cause, AgentControlVerificationOrchestrationHistoryError);
      assert.equal(cause.operation, "provider-terminal-envelope-lineage");
      assert.equal(cause.reason, "terminal-conflict");
    }
  }),
);

it.effect("fails closed for terminal payload or lifecycle metadata divergence", () =>
  Effect.gen(function* () {
    yield* expectTerminalHistoryError([
      terminal(6),
      terminal(7, {
        payload: {
          ...(terminal(7).payload as Record<string, JsonValue>),
          session: {
            ...(terminal(7).payload as { readonly session: Record<string, unknown> }).session,
            lastError: "divergent",
          },
        },
      }),
    ]);
    const lifecycleDivergence = yield* Effect.flip(
      selectVerificationProviderTerminal(
        [
          terminal(6),
          terminal(7, {
            source: {
              ...terminal(7).source,
              providerTurnId: TurnId.make("divergent-provider-turn"),
            },
          }),
        ],
        terminalIdentity,
      ),
    );
    assert.instanceOf(lifecycleDivergence, AgentControlVerificationOrchestrationHistoryError);
    assert.equal(lifecycleDivergence.operation, "normalize-provider-terminal");
    assert.equal(lifecycleDivergence.reason, "terminal-conflict");
  }),
);

it.effect("compares complete payload and metadata while ignoring object key order", () =>
  Effect.gen(function* () {
    const first = terminal(6);
    const reordered = terminal(7, {
      payload: {
        session: (first.payload as { readonly session: JsonValue }).session,
        threadId: identity.threadId,
      },
      metadata: {
        providerRuntimeLifecycle: (
          first.metadata as { readonly providerRuntimeLifecycle: JsonValue }
        ).providerRuntimeLifecycle,
      },
    });
    const selected = yield* selectVerificationProviderTerminal(
      [first, reordered],
      terminalIdentity,
    );
    assert.equal(selected._tag, "Ready");

    yield* expectTerminalHistoryError([
      first,
      terminal(7, {
        payload: {
          ...(first.payload as Record<string, JsonValue>),
          threadId: "schema-valid-foreign-thread",
        },
      }),
    ]);
    yield* expectTerminalHistoryError([
      first,
      terminal(7, {
        metadata: {
          ...(first.metadata as Record<string, JsonValue>),
          ingestedAt: "2020-01-01T00:00:02.000Z",
        },
      }),
    ]);
  }),
);
