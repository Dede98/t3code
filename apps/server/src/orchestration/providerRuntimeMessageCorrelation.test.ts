import { assert, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, TurnId } from "@t3tools/contracts";

import { canonicalJson } from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  decodeCanonicalOrLegacyOrchestrationMetadata,
  decodeLegacyProviderRuntimeMessageCorrelation,
  decodeProviderRuntimeMessageCorrelation,
} from "./providerRuntimeMessageCorrelation.ts";

const historical = {
  runtimeEventId: "event-historical",
  runtimeEventType: "item.completed",
  providerInstanceId: "codex",
  providerTurnId: "turn-historical",
} as const;
const historicalBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';

it("decodes only the exact historical runtimeEventType correlation", () => {
  assert.deepStrictEqual(decodeLegacyProviderRuntimeMessageCorrelation(historical), {
    runtimeEventId: EventId.make("event-historical"),
    eventType: "item.completed",
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerTurnId: TurnId.make("turn-historical"),
    providerItemId: null,
  });

  for (const invalid of [
    { ...historical, runtimeEventType: undefined },
    { ...historical, eventType: historical.runtimeEventType },
    {
      runtimeEventId: historical.runtimeEventId,
      eventType: historical.runtimeEventType,
      providerInstanceId: historical.providerInstanceId,
      providerTurnId: historical.providerTurnId,
    },
    { ...historical, unknown: "field" },
    { ...historical, runtimeEventId: 1 },
    { ...historical, runtimeEventId: " event-historical" },
    { ...historical, providerInstanceId: "codex " },
    { ...historical, providerTurnId: "turn-historical\n" },
  ]) {
    assert.throws(() => decodeLegacyProviderRuntimeMessageCorrelation(invalid));
  }
});

it("keeps the new five-field eventType correlation required and closed", () => {
  const current = {
    runtimeEventId: EventId.make("event-current"),
    eventType: "item.completed",
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerTurnId: TurnId.make("turn-current"),
    providerItemId: null,
  } as const;
  assert.deepStrictEqual(decodeProviderRuntimeMessageCorrelation(current), current);
  assert.throws(() =>
    decodeProviderRuntimeMessageCorrelation({
      runtimeEventId: current.runtimeEventId,
      eventType: current.eventType,
      providerInstanceId: current.providerInstanceId,
      providerTurnId: current.providerTurnId,
    }),
  );
  assert.throws(() =>
    decodeProviderRuntimeMessageCorrelation({ ...current, runtimeEventType: current.eventType }),
  );
  assert.throws(() => decodeProviderRuntimeMessageCorrelation({ ...current, unknown: "field" }));
});

it("decodes current canonical metadata and only the exact historical encoder bytes", () => {
  const current = {
    providerRuntimeMessage: {
      runtimeEventId: EventId.make("event-current"),
      eventType: "item.completed" as const,
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerTurnId: TurnId.make("turn-current"),
      providerItemId: null,
    },
    ingestedAt: "2026-08-28T10:00:00.000Z",
  };
  assert.deepStrictEqual(
    decodeCanonicalOrLegacyOrchestrationMetadata(canonicalJson(current)),
    current,
  );
  assert.deepStrictEqual(decodeCanonicalOrLegacyOrchestrationMetadata(historicalBytes), {
    providerRuntimeMessage: {
      runtimeEventId: EventId.make("event-historical"),
      eventType: "item.completed",
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerTurnId: TurnId.make("turn-historical"),
      providerItemId: null,
    },
  });
});

it("fails closed outside the exact historical byte and key-order seam", () => {
  const invalid = [
    canonicalJson({ providerRuntimeMessage: historical }),
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical","extra":true}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventId":"event-divergent","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{ "providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    `${historicalBytes}\n`,
    '{"ingestedAt":"2026-08-28T10:00:00.000Z","providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":" event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
  ];
  for (const source of invalid) {
    assert.throws(() => decodeCanonicalOrLegacyOrchestrationMetadata(source));
  }
});
