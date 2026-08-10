import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { normalizeVerificationTerminal } from "./terminalObservation.ts";

const identity = {
  providerDeliveryId: "verification-delivery-1",
  threadId: ThreadId.make("verification-thread-1"),
  providerInstanceId: ProviderInstanceId.make("codex-main"),
  providerTurnId: TurnId.make("provider-turn-1"),
};

const completedEvent = (
  state: "completed" | "failed" | "interrupted" | "cancelled",
): Extract<ProviderRuntimeEvent, { readonly type: "turn.completed" }> => ({
  eventId: EventId.make(`runtime-terminal-${state}`),
  provider: ProviderDriverKind.make("codex"),
  providerInstanceId: identity.providerInstanceId,
  threadId: identity.threadId,
  turnId: identity.providerTurnId,
  type: "turn.completed",
  createdAt: "2026-08-10T10:11:12.345Z",
  payload: {
    state,
    errorMessage: "must not affect the terminal observation",
    stopReason: "must not affect the terminal observation",
    usage: { secret: "ignored" },
  },
  raw: { source: "codex.eventmsg", payload: { ignored: true } },
});

it.effect.each([
  ["completed", "completed", null],
  ["failed", "failed", "provider-turn-failed"],
  ["interrupted", "interrupted", "provider-turn-interrupted"],
  ["cancelled", "interrupted", "provider-turn-cancelled"],
] as const)("maps turn.completed state %s", ([providerState, deliveryState, errorCode]) =>
  Effect.gen(function* () {
    const observation = yield* normalizeVerificationTerminal(
      completedEvent(providerState),
      identity,
    );
    assert.equal(observation.providerState, providerState);
    assert.equal(observation.deliveryState, deliveryState);
    assert.equal(observation.lastErrorCode, errorCode);
    assert.match(observation.observationDigest, /^[0-9a-f]{64}$/u);
  }),
);

it.effect("maps turn.aborted to a technical failure", () =>
  Effect.gen(function* () {
    const observation = yield* normalizeVerificationTerminal(
      {
        eventId: EventId.make("runtime-terminal-aborted"),
        provider: ProviderDriverKind.make("codex"),
        providerInstanceId: identity.providerInstanceId,
        threadId: identity.threadId,
        turnId: identity.providerTurnId,
        type: "turn.aborted",
        createdAt: "2026-08-10T10:11:12.345Z",
        payload: { reason: "untrusted provider text" },
      },
      identity,
    );
    assert.equal(observation.providerState, null);
    assert.equal(observation.deliveryState, "failed");
    assert.equal(observation.lastErrorCode, "provider-turn-aborted");
  }),
);

it.effect("excludes non-authoritative provider text from the digest", () =>
  Effect.gen(function* () {
    const left = yield* normalizeVerificationTerminal(completedEvent("failed"), identity);
    const right = yield* normalizeVerificationTerminal(
      {
        ...completedEvent("failed"),
        payload: { state: "failed", errorMessage: "different", stopReason: null },
        raw: {
          source: "codex.eventmsg",
          payload: { completely: "different" },
        },
      },
      identity,
    );
    assert.equal(left.observationDigest, right.observationDigest);
  }),
);
