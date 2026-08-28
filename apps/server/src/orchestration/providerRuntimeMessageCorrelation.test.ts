import { assert, it } from "@effect/vitest";
import { EventId, ProviderInstanceId, RuntimeItemId, TurnId } from "@t3tools/contracts";

import { canonicalJson } from "../agentControl/initialPlanning/eventEvidence.ts";
import { decodePersistedOrchestrationMetadata } from "./providerRuntimeMessageCorrelation.ts";

const historicalBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
// Exact EventMetadataFromJsonString encoder bytes at de63cc314 before the
// e13573a25 field rename. The historical contract required providerItemId and
// admitted both null and canonical RuntimeItemId text.
const historicalWithNullItemBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}';
const historicalWithTextItemBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":"item-historical"}}';

const decodeStored = (
  text: unknown,
  storageClass: unknown = "text",
  bytes: unknown = typeof text === "string" ? Buffer.from(text, "utf8") : text,
) => decodePersistedOrchestrationMetadata({ storageClass, bytes, text });

it("decodes both current storage encoders and only the exact historical storage bytes", () => {
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
  const currentSource = canonicalJson(current);
  assert.deepStrictEqual(decodeStored(currentSource), {
    source: currentSource,
    value: current,
  });
  const currentSchemaOrderSource =
    '{"ingestedAt":"2026-08-28T10:00:00.000Z","providerRuntimeMessage":{"runtimeEventId":"event-current","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-current","providerItemId":null}}';
  assert.deepStrictEqual(decodeStored(currentSchemaOrderSource), {
    source: currentSchemaOrderSource,
    value: current,
  });
  assert.deepStrictEqual(decodeStored(historicalBytes), {
    source: historicalBytes,
    value: {
      providerRuntimeMessage: {
        runtimeEventId: EventId.make("event-historical"),
        eventType: "item.completed",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerTurnId: TurnId.make("turn-historical"),
        providerItemId: null,
      },
    },
  });
  for (const [source, providerItemId] of [
    [historicalWithNullItemBytes, null],
    [historicalWithTextItemBytes, RuntimeItemId.make("item-historical")],
  ] as const) {
    assert.deepStrictEqual(decodeStored(source), {
      source,
      value: {
        providerRuntimeMessage: {
          runtimeEventId: EventId.make("event-historical-item"),
          eventType: "item.completed",
          providerInstanceId: ProviderInstanceId.make("codex"),
          providerTurnId: TurnId.make("turn-historical-item"),
          providerItemId,
        },
      },
    });
  }
  assert.deepStrictEqual(decodeStored("{}"), { source: "{}", value: {} });
});

it("keeps the historical five-field runtimeEventType family exact, ordered, and closed", () => {
  for (const source of [historicalWithNullItemBytes, historicalWithTextItemBytes]) {
    assert.doesNotThrow(() => decodeStored(source));
  }
  for (const source of [
    // Same keys, but not the proven encoder order.
    '{"providerRuntimeMessage":{"providerItemId":null,"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","providerItemId":null,"runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item"}}',
    // Ambiguous type fields are not historical five-field bytes.
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}',
    // Missing non-item fields, extras, and duplicate keys remain closed.
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerItemId":null}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null,"extra":true}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null,"providerItemId":"item-divergent"}}',
    // The old RuntimeItemId transform must not trim or admit invalid storage bytes.
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":" item-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":""}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":7}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item\\u0000","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}',
    ` ${historicalWithNullItemBytes}`,
    `${historicalWithNullItemBytes}\n`,
  ]) {
    assert.throws(() => decodeStored(source));
  }

  // The current eventType spelling is deliberately accepted by the distinct
  // current schema-order family, never by the historical runtimeEventType seam.
  assert.doesNotThrow(() =>
    decodeStored(
      '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}',
    ),
  );

  // Omitting providerItemId is not admitted by this family; those exact bytes
  // remain accepted solely as the separately named historical four-field v0.
  assert.deepStrictEqual(
    decodeStored(historicalBytes).value.providerRuntimeMessage?.providerItemId,
    null,
  );
});

it("fails closed outside the exact historical byte and key-order seam", () => {
  const invalid = [
    // Canonically sorted and other invented historical permutations.
    '{"providerRuntimeMessage":{"providerInstanceId":"codex","providerTurnId":"turn-historical","runtimeEventId":"event-historical","runtimeEventType":"item.completed"}}',
    '{"providerRuntimeMessage":{"runtimeEventType":"item.completed","runtimeEventId":"event-historical","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","providerInstanceId":"codex","runtimeEventType":"item.completed","providerTurnId":"turn-historical"}}',
    // Wrong, ambiguous, incomplete, or open shapes.
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical","extra":true}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"},"extra":true}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventId":"event-divergent","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    // Whitespace and noncanonical identities.
    '{ "providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":\t{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    `${historicalBytes}\r\n`,
    `\u00a0${historicalBytes}`,
    '{"providerRuntimeMessage":{"runtimeEventId":" event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex ","providerTurnId":"turn-historical"}}',
    '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical\\n"}}',
  ];
  for (const source of invalid) {
    assert.throws(() => decodeStored(source));
  }
});

it("requires SQLite TEXT plus fatal roundtripping original UTF-8 bytes", () => {
  for (const storageClass of ["blob", "integer", "real", "null"]) {
    assert.throws(() => decodeStored(historicalBytes, storageClass));
  }
  assert.throws(() =>
    decodeStored(historicalBytes, "text", Buffer.from([0x7b, 0x22, 0xff, 0x22, 0x7d])),
  );
  assert.throws(() => decodeStored(historicalBytes, "text", Buffer.from("{}")));
  assert.throws(() =>
    decodeStored(historicalBytes, "text", new TextEncoder().encode(`${historicalBytes}\n`)),
  );
});

it("keeps the current five-field eventType correlation required and closed", () => {
  const current = {
    providerRuntimeMessage: {
      runtimeEventId: EventId.make("event-current"),
      eventType: "item.completed" as const,
      providerInstanceId: ProviderInstanceId.make("codex"),
      providerTurnId: TurnId.make("turn-current"),
      providerItemId: null,
    },
  };
  assert.deepStrictEqual(decodeStored(canonicalJson(current)).value, current);
  for (const invalid of [
    canonicalJson({
      providerRuntimeMessage: {
        runtimeEventId: "event-current",
        eventType: "item.completed",
        providerInstanceId: "codex",
        providerTurnId: "turn-current",
      },
    }),
    canonicalJson({
      providerRuntimeMessage: {
        ...current.providerRuntimeMessage,
        runtimeEventType: "item.completed",
      },
    }),
    canonicalJson({
      providerRuntimeMessage: { ...current.providerRuntimeMessage, unknown: "field" },
    }),
  ]) {
    assert.throws(() => decodeStored(invalid));
  }
});
