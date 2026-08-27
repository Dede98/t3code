import { assert, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, TurnId } from "@t3tools/contracts";

import { parseJsonStrict } from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  decodeLegacyProviderRuntimeMessageCorrelation,
  decodeProviderRuntimeMessageCorrelation,
} from "./providerRuntimeMessageCorrelation.ts";

const historical = {
  runtimeEventId: "event-historical",
  runtimeEventType: "item.completed",
  providerInstanceId: "codex",
  providerTurnId: "turn-historical",
} as const;

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

it("rejects duplicate historical correlation keys at the raw JSON seam", () => {
  const duplicate =
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical",' +
    '"runtimeEventId":"event-divergent","runtimeEventType":"item.completed",' +
    '"providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
  assert.throws(() => parseJsonStrict(duplicate), /duplicate object key 'runtimeEventId'/u);
});
