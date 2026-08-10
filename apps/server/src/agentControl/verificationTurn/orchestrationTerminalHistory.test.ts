import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import {
  AgentControlVerificationOrchestrationHistoryError,
  selectVerificationProviderStart,
  type VerificationProviderStartHistoryEntry,
} from "./orchestrationTerminalHistory.ts";

const identity = {
  threadId: "verification-thread",
  providerInstanceId: "codex-main",
  providerTurnId: "provider-turn",
  runtimeMode: "approval-required",
  turnRequestStreamVersion: 4,
} as const;

const start = (
  streamVersion: number,
  overrides: Partial<VerificationProviderStartHistoryEntry> = {},
): VerificationProviderStartHistoryEntry => {
  const { session: sessionOverrides, ...entryOverrides } = overrides;
  const session = {
    threadId: identity.threadId,
    status: "running",
    providerName: "codex",
    providerInstanceId: identity.providerInstanceId,
    runtimeMode: identity.runtimeMode,
    activeTurnId: identity.providerTurnId,
    ...sessionOverrides,
  };
  return {
    streamVersion,
    occurredAt: "2020-01-01T00:00:00.000Z",
    canonicalSessionJson: JSON.stringify(session),
    ...entryOverrides,
    session,
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
        start(5, { session: { ...start(5).session, activeTurnId: "foreign-turn" } }),
        start(6, {
          session: { ...start(6).session, providerInstanceId: "foreign-provider" },
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
      runtimeEventId: "runtime-start",
      runtimeEventType: "turn.started" as const,
      providerInstanceId: identity.providerInstanceId,
      providerTurnId: identity.providerTurnId,
    };
    const first = start(5, { lifecycle });
    const selected = yield* selectVerificationProviderStart(
      [first, { ...first, streamVersion: 6 }],
      identity,
    );
    assert.deepStrictEqual(selected, { _tag: "Ready", index: 1 });
  }),
);

it.effect("fails closed for duplicate legacy or contradictory runtime starts", () =>
  Effect.gen(function* () {
    yield* expectHistoryError([start(5), start(6)], "provider-start-ambiguous");
    yield* expectHistoryError(
      [
        start(5, {
          lifecycle: {
            runtimeEventId: "runtime-start-a",
            runtimeEventType: "turn.started",
            providerInstanceId: identity.providerInstanceId,
            providerTurnId: identity.providerTurnId,
          },
        }),
        start(6, {
          lifecycle: {
            runtimeEventId: "runtime-start-b",
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
