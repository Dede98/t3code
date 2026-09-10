import { assert, it } from "@effect/vitest";
import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  OrchestrationCommand,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { makeProviderTerminalSessionCommand } from "./providerTerminalSessionCommand.ts";

const identity = {
  eventId: EventId.make("native-terminal-receipt"),
  provider: ProviderDriverKind.make("codex"),
  providerInstanceId: ProviderInstanceId.make("codex-primary"),
  threadId: ThreadId.make("controlled-native-terminal"),
  turnId: TurnId.make("accepted-native-turn"),
  createdAt: "2026-09-10T08:00:00.000Z",
};
const decodeCommand = Schema.decodeUnknownEffect(OrchestrationCommand);

it.effect.each([
  { type: "turn.completed", payload: { state: "completed" }, status: "ready", error: null },
  {
    type: "turn.completed",
    payload: { state: "failed", errorMessage: "Native failure" },
    status: "error",
    error: "Native failure",
  },
  { type: "turn.completed", payload: { state: "failed" }, status: "error", error: "Turn failed" },
  { type: "turn.completed", payload: { state: "interrupted" }, status: "ready", error: null },
  { type: "turn.completed", payload: { state: "cancelled" }, status: "ready", error: null },
  {
    type: "turn.aborted",
    payload: { reason: "Native stop" },
    status: "error",
    error: "Native stop",
  },
] as const)(
  "replays actual native terminal $type $payload through one stable lifecycle command",
  ({ type, payload, status, error }) =>
    Effect.gen(function* () {
      const event = { ...identity, type, payload };
      const original = yield* makeProviderTerminalSessionCommand(event, "approval-required");
      const replayed = yield* makeProviderTerminalSessionCommand({ ...event }, "approval-required");
      assert.deepStrictEqual(replayed, original);
      yield* decodeCommand(original);
      assert.equal(original.commandId, "provider:native-terminal-receipt:native-terminal-session");
      assert.equal(original.session.status, status);
      assert.equal(original.session.lastError, error);
      assert.equal(original.session.updatedAt, identity.createdAt);
      assert.equal(original.providerRuntimeLifecycle.providerTurnId, identity.turnId);
      // A conflicting repeat cannot obtain a fresh receipt id and silently supersede authority.
      const conflict = yield* makeProviderTerminalSessionCommand(
        {
          ...identity,
          type: "turn.completed",
          payload: { state: "failed", errorMessage: "conflicting failure" },
        },
        "approval-required",
      );
      assert.equal(conflict.commandId, original.commandId);
      assert.isFalse(Equal.equals(conflict, original));
    }),
);

it.effect.each([
  { ...identity, type: "turn.completed", payload: { state: "completed" }, turnId: undefined },
  {
    ...identity,
    type: "turn.completed",
    payload: { state: "completed" },
    providerInstanceId: undefined,
  },
  { ...identity, type: "turn.started", payload: {} },
  { ...identity, type: "thread.session-set", payload: { state: "completed" } },
  { ...identity, type: "turn.completed", payload: { state: "invented" } },
  { ...identity, type: "turn.completed", payload: { state: "completed" }, createdAt: "invalid" },
])("rejects an incomplete or non-native terminal source %#", (event) =>
  Effect.gen(function* () {
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(makeProviderTerminalSessionCommand(event, "approval-required")),
      ),
    );
  }),
);
