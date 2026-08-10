import type {
  EventId,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";

export type VerificationTechnicalTerminalState = "completed" | "failed" | "interrupted";

export type VerificationTerminalErrorCode =
  | "provider-turn-failed"
  | "provider-turn-aborted"
  | "provider-turn-interrupted"
  | "provider-turn-cancelled";

export interface VerificationTerminalObservation {
  readonly runtimeEventId: EventId;
  readonly runtimeEventType: "turn.completed" | "turn.aborted";
  readonly providerState: "completed" | "failed" | "interrupted" | "cancelled" | null;
  readonly deliveryState: VerificationTechnicalTerminalState;
  readonly terminalAt: string;
  readonly lastErrorCode: VerificationTerminalErrorCode | null;
  readonly observationDigest: string;
}

export class VerificationTerminalMappingError extends Schema.TaggedErrorClass<VerificationTerminalMappingError>()(
  "VerificationTerminalMappingError",
  {
    reason: Schema.Literals([
      "missing-provider-instance-id",
      "missing-provider-turn-id",
      "identity-divergent",
      "invalid-runtime-event-id",
      "invalid-terminal-at",
    ]),
  },
) {}

type TerminalEvent = Extract<
  ProviderRuntimeEvent,
  { readonly type: "turn.completed" | "turn.aborted" }
>;

export type VerificationTerminalSource =
  | {
      readonly runtimeEventId: EventId;
      readonly runtimeEventType: "turn.completed";
      readonly threadId: ThreadId;
      readonly providerInstanceId: ProviderInstanceId;
      readonly providerTurnId: TurnId;
      readonly providerState: "completed" | "failed" | "interrupted" | "cancelled";
      readonly terminalAt: string;
    }
  | {
      readonly runtimeEventId: EventId;
      readonly runtimeEventType: "turn.aborted";
      readonly threadId: ThreadId;
      readonly providerInstanceId: ProviderInstanceId;
      readonly providerTurnId: TurnId;
      readonly terminalAt: string;
    };

const canonicalTerminalTime = (value: string): boolean => {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u.test(value)) return false;
  const parsed = DateTime.make(value);
  return Option.isSome(parsed) && DateTime.formatIso(parsed.value) === value;
};

const eventIdIsSafe = (value: string): boolean => {
  const bytes = new TextEncoder().encode(value);
  return (
    bytes.length >= 1 &&
    bytes.length <= 1024 &&
    !value.includes("\0") &&
    new TextDecoder("utf-8", { fatal: true }).decode(bytes) === value
  );
};

const normalizeOutcome = (
  source: VerificationTerminalSource,
): {
  readonly providerState: VerificationTerminalObservation["providerState"];
  readonly deliveryState: VerificationTechnicalTerminalState;
  readonly lastErrorCode: VerificationTerminalErrorCode | null;
} => {
  if (source.runtimeEventType === "turn.aborted") {
    return {
      providerState: null,
      deliveryState: "failed",
      lastErrorCode: "provider-turn-aborted",
    };
  }
  switch (source.providerState) {
    case "completed":
      return { providerState: "completed", deliveryState: "completed", lastErrorCode: null };
    case "failed":
      return {
        providerState: "failed",
        deliveryState: "failed",
        lastErrorCode: "provider-turn-failed",
      };
    case "interrupted":
      return {
        providerState: "interrupted",
        deliveryState: "interrupted",
        lastErrorCode: "provider-turn-interrupted",
      };
    case "cancelled":
      return {
        providerState: "cancelled",
        deliveryState: "interrupted",
        lastErrorCode: "provider-turn-cancelled",
      };
  }
};

export const normalizeVerificationTerminalSource = Effect.fn("normalizeVerificationTerminalSource")(
  function* (
    source: VerificationTerminalSource,
    identity: {
      readonly providerDeliveryId: string;
      readonly threadId: ThreadId;
      readonly providerInstanceId: ProviderInstanceId;
      readonly providerTurnId: TurnId;
    },
  ) {
    if (
      source.threadId !== identity.threadId ||
      source.providerInstanceId !== identity.providerInstanceId ||
      source.providerTurnId !== identity.providerTurnId
    ) {
      return yield* new VerificationTerminalMappingError({ reason: "identity-divergent" });
    }
    if (!eventIdIsSafe(source.runtimeEventId)) {
      return yield* new VerificationTerminalMappingError({ reason: "invalid-runtime-event-id" });
    }
    if (!canonicalTerminalTime(source.terminalAt)) {
      return yield* new VerificationTerminalMappingError({ reason: "invalid-terminal-at" });
    }
    const normalized = normalizeOutcome(source);
    const observationDigest = sha256Utf8(
      canonicalJson({
        deliveryState: normalized.deliveryState,
        lastErrorCode: normalized.lastErrorCode,
        providerDeliveryId: identity.providerDeliveryId,
        providerInstanceId: identity.providerInstanceId,
        providerState: normalized.providerState,
        providerTurnId: identity.providerTurnId,
        runtimeEventId: source.runtimeEventId,
        runtimeEventType: source.runtimeEventType,
        terminalAt: source.terminalAt,
        threadId: identity.threadId,
        version: 1,
      }),
    );
    return {
      runtimeEventId: source.runtimeEventId,
      runtimeEventType: source.runtimeEventType,
      providerState: normalized.providerState,
      deliveryState: normalized.deliveryState,
      terminalAt: source.terminalAt,
      lastErrorCode: normalized.lastErrorCode,
      observationDigest,
    } satisfies VerificationTerminalObservation;
  },
);

export const normalizeVerificationTerminal = Effect.fn("normalizeVerificationTerminal")(function* (
  event: TerminalEvent,
  identity: {
    readonly providerDeliveryId: string;
    readonly threadId: ThreadId;
    readonly providerInstanceId: ProviderInstanceId;
    readonly providerTurnId: TurnId;
  },
) {
  if (event.providerInstanceId === undefined) {
    return yield* new VerificationTerminalMappingError({
      reason: "missing-provider-instance-id",
    });
  }
  if (event.turnId === undefined) {
    return yield* new VerificationTerminalMappingError({ reason: "missing-provider-turn-id" });
  }
  return yield* normalizeVerificationTerminalSource(
    event.type === "turn.completed"
      ? {
          runtimeEventId: event.eventId,
          runtimeEventType: event.type,
          threadId: event.threadId,
          providerInstanceId: event.providerInstanceId,
          providerTurnId: event.turnId,
          providerState: event.payload.state,
          terminalAt: event.createdAt,
        }
      : {
          runtimeEventId: event.eventId,
          runtimeEventType: event.type,
          threadId: event.threadId,
          providerInstanceId: event.providerInstanceId,
          providerTurnId: event.turnId,
          terminalAt: event.createdAt,
        },
    identity,
  );
});
