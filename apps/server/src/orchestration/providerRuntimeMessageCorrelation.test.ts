import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  ProviderInstanceId,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";

import { canonicalJson } from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  decodeOrchestrationEventJsonStorage,
  encodeOrchestrationEventSchemaOrderStorage,
} from "./orchestrationEventStorage.ts";
import {
  classifyPersistedOrchestrationMetadata,
  decodePersistedOrchestrationMetadata,
  ORCHESTRATION_METADATA_STORAGE_ENCODING_SCHEMA_ORDER_V1,
  providerRuntimeEventMatchesVerificationResultFragment,
} from "./providerRuntimeMessageCorrelation.ts";

const historicalBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
// Exact EventMetadataFromJsonString encoder bytes at de63cc314 before the
// e13573a25 field rename. The historical contract required providerItemId and
// admitted both null and canonical RuntimeItemId text.
const historicalWithNullItemBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":null}}';
const historicalWithTextItemBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":"item-historical"}}';
// Exact EventMetadataFromJsonString bytes emitted from the ordered metadata
// object and contracts at de63cc314 through e13573a25^.
const historicalWithItemAndCaptureBytes =
  '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-capture","runtimeEventType":"content.delta","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","providerItemId":"item-historical-capture"},"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"handoff-historical-capture","providerDeliveryId":"delivery-historical-capture","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';

const decodeStored = (
  text: unknown,
  storageClass: unknown = "text",
  bytes: unknown = typeof text === "string" ? Buffer.from(text, "utf8") : text,
) => decodePersistedOrchestrationMetadata({ storageClass, bytes, text });

it("keeps the verification fragment runtime-event matrix closed", () => {
  for (const [fragmentKind, runtimeEventType, expected] of [
    ["delta", "content.delta", true],
    ["delta", "item.completed", false],
    ["completion", "item.completed", true],
    ["completion", "turn.completed", true],
    ["completion", "request.opened", true],
    ["completion", "user-input.requested", true],
    ["completion", "content.delta", false],
    ["completion", "unknown.event", false],
  ] as const) {
    assert.equal(
      providerRuntimeEventMatchesVerificationResultFragment(fragmentKind, runtimeEventType),
      expected,
      `${fragmentKind}:${runtimeEventType}`,
    );
  }
});

it("classifies the current message payload and two-key correlation encoding together", () => {
  const storage = encodeOrchestrationEventSchemaOrderStorage({
    sequence: 1,
    eventId: EventId.make("event-current-storage"),
    aggregateKind: "thread",
    aggregateId: ThreadId.make("thread-current-storage"),
    type: "thread.message-sent",
    occurredAt: "2026-08-28T10:00:00.000Z",
    commandId: null,
    causationEventId: null,
    correlationId: null,
    payload: {
      threadId: ThreadId.make("thread-current-storage"),
      messageId: MessageId.make("message-current-storage"),
      role: "assistant",
      text: "current storage",
      turnId: TurnId.make("turn-current-storage"),
      streaming: true,
      createdAt: "2026-08-28T10:00:00.000Z",
      updatedAt: "2026-08-28T10:00:00.000Z",
    },
    metadata: {
      providerRuntimeMessage: {
        runtimeEventId: EventId.make("event-current-storage"),
        eventType: "content.delta",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerTurnId: TurnId.make("turn-current-storage"),
        providerItemId: RuntimeItemId.make("item-current-storage"),
      },
      verificationResultCapture: {
        schemaVersion: 1,
        disposition: "presentation",
        handoffId: "handoff-current-storage",
        providerDeliveryId: "delivery-current-storage",
        providerInstanceId: ProviderInstanceId.make("codex"),
        providerTurnId: TurnId.make("turn-current-storage"),
        resultSchemaFingerprint: "f".repeat(64),
      },
    },
  });
  assert.doesNotThrow(() =>
    decodeOrchestrationEventJsonStorage({
      eventType: "thread.message-sent",
      payload: {
        storageClass: "text",
        text: storage.payloadJson,
        bytes: Buffer.from(storage.payloadJson),
      },
      metadata: {
        storageClass: "text",
        text: storage.metadataJson,
        bytes: Buffer.from(storage.metadataJson),
      },
    }),
  );
});

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

it("admits only the exact historical two-key five-field runtime and capture family", () => {
  const decoded = decodeStored(historicalWithItemAndCaptureBytes);
  assert.deepStrictEqual(decoded.value.providerRuntimeMessage, {
    runtimeEventId: EventId.make("event-historical-capture"),
    eventType: "content.delta",
    providerInstanceId: ProviderInstanceId.make("codex"),
    providerTurnId: TurnId.make("turn-historical-capture"),
    providerItemId: RuntimeItemId.make("item-historical-capture"),
  });
  assert.deepStrictEqual(decoded.value.verificationResultCapture, {
    schemaVersion: 1,
    disposition: "presentation",
    handoffId: "handoff-historical-capture",
    providerDeliveryId: "delivery-historical-capture",
    providerInstanceId: "codex",
    providerTurnId: "turn-historical-capture",
    resultSchemaFingerprint: "f".repeat(64),
  });
  const nullItem = historicalWithItemAndCaptureBytes.replace(
    '"providerItemId":"item-historical-capture"',
    '"providerItemId":null',
  );
  assert.equal(decodeStored(nullItem).value.providerRuntimeMessage?.providerItemId, null);
  const currentEventTypeBytes = historicalWithItemAndCaptureBytes.replace(
    '"runtimeEventType"',
    '"eventType"',
  );
  assert.equal(
    classifyPersistedOrchestrationMetadata({
      storageClass: "text",
      text: currentEventTypeBytes,
      bytes: Buffer.from(currentEventTypeBytes),
    }).encoding,
    ORCHESTRATION_METADATA_STORAGE_ENCODING_SCHEMA_ORDER_V1,
  );
  const captureOnlyBytes =
    '{"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"handoff-historical-capture","providerDeliveryId":"delivery-historical-capture","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';
  assert.throws(() =>
    classifyPersistedOrchestrationMetadata({
      storageClass: "text",
      text: captureOnlyBytes,
      bytes: Buffer.from(captureOnlyBytes),
    }),
  );

  for (const [index, source] of [
    // Top-level order and the mandatory two-key shape are immutable.
    '{"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"handoff-historical-capture","providerDeliveryId":"delivery-historical-capture","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},"providerRuntimeMessage":{"runtimeEventId":"event-historical-capture","runtimeEventType":"content.delta","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","providerItemId":"item-historical-capture"}}',
    historicalWithItemAndCaptureBytes.replace(/}$/, ',"extra":true}'),
    // Nested order, extras, current spelling, and ambiguous spelling are not legacy bytes.
    historicalWithItemAndCaptureBytes.replace(
      '"schemaVersion":1,"disposition":"presentation"',
      '"disposition":"presentation","schemaVersion":1',
    ),
    historicalWithItemAndCaptureBytes.replace(
      '"resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"',
      '"resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","extra":true',
    ),
    historicalWithItemAndCaptureBytes.replace(
      '"runtimeEventType":"content.delta"',
      '"runtimeEventType":"content.delta","eventType":"content.delta"',
    ),
    // providerItemId is required; invalid values and identity transformations fail closed.
    historicalWithItemAndCaptureBytes.replace(',"providerItemId":"item-historical-capture"', ""),
    historicalWithItemAndCaptureBytes.replace(
      '"providerItemId":"item-historical-capture"',
      '"providerItemId":" item-historical-capture"',
    ),
    historicalWithItemAndCaptureBytes.replace(
      '"providerItemId":"item-historical-capture"',
      '"providerItemId":7',
    ),
    historicalWithItemAndCaptureBytes.replace(
      '"providerItemId":"item-historical-capture"',
      '"providerItemId":"item-historical-capture","providerItemId":null',
    ),
    historicalWithItemAndCaptureBytes.replace(
      '"handoffId":"handoff-historical-capture"',
      '"handoffId":"handoff\\u0000historical-capture"',
    ),
    ` ${historicalWithItemAndCaptureBytes}`,
    `${historicalWithItemAndCaptureBytes}\n`,
  ].entries()) {
    let failure: unknown;
    try {
      decodeStored(source);
    } catch (error) {
      failure = error;
    }
    assert.isDefined(failure, `historical two-key negative ${index}`);
  }
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
