import {
  CommandId,
  EventId,
  MessageId,
  type OrchestrationEvent,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson, sha256Utf8, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
  loadOpenVerificationResultMessageIds,
  loadSealableVerificationResultSource,
  loadVerificationResultCapturedMessage,
  makeBoundedVerificationResultCompletion,
  makeBoundedVerificationResultDelta,
  VerificationResultHistoryError,
} from "./orchestrationResultSource.ts";

const isVerificationResultHistoryError = Schema.is(VerificationResultHistoryError);

const layer = it.layer(NodeSqliteClient.layerMemory());
const threadId = ThreadId.make("verification-source-thread");
const providerInstanceId = ProviderInstanceId.make("codex");
const providerTurnId = TurnId.make("verification-provider-turn");
const at = "2026-08-22T12:00:00.000Z";
const captureAuthority = {
  schemaVersion: 1 as const,
  disposition: "authority" as const,
  handoffId: "verification-handoff",
  providerDeliveryId: "verification-delivery",
  providerInstanceId,
  providerTurnId,
  resultSchemaFingerprint: "f".repeat(64),
};

const captureEvent = (input: {
  readonly streamVersion: number;
  readonly messageId: string;
  readonly fragment:
    | {
        readonly kind: "delta";
        readonly text: string;
        readonly byteLength: number;
        readonly cumulativeByteLength: number;
      }
    | {
        readonly kind: "completion";
        readonly completionText: string | null;
        readonly outputByteLength: number;
      };
}): OrchestrationEvent => {
  const runtimeEventId = EventId.make(`runtime-capture-${input.streamVersion}`);
  const commandId = CommandId.make(
    `provider:${runtimeEventId}:verification-result:${input.messageId}`,
  );
  return {
    sequence: input.streamVersion,
    eventId: EventId.make(`verification-capture-event-${input.streamVersion}`),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: at,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    metadata: {
      providerRuntimeMessage: {
        runtimeEventId,
        runtimeEventType: input.fragment.kind === "delta" ? "content.delta" : "item.completed",
        providerInstanceId,
        providerTurnId,
        providerItemId: null,
      },
      verificationResultCapture: captureAuthority,
    },
    type: "thread.verification-result-fragment-captured",
    payload: {
      threadId,
      messageId: MessageId.make(input.messageId),
      turnId: providerTurnId,
      fragment: input.fragment,
      createdAt: at,
    },
  };
};

const messageEvent = (input: {
  readonly streamVersion: number;
  readonly messageId: string;
  readonly text: string;
  readonly streaming: boolean;
  readonly actor?: "client" | "provider";
  readonly correlationProviderInstanceId?: ProviderInstanceId;
}): OrchestrationEvent => ({
  sequence: input.streamVersion,
  eventId: EventId.make(`verification-source-event-${input.streamVersion}`),
  aggregateKind: "thread",
  aggregateId: threadId,
  occurredAt: at,
  commandId: CommandId.make(
    `provider:runtime-source-${input.streamVersion}:source:${input.streamVersion}`,
  ),
  causationEventId: null,
  correlationId: CommandId.make(
    `provider:runtime-source-${input.streamVersion}:source:${input.streamVersion}`,
  ),
  metadata:
    input.actor === "client"
      ? {}
      : {
          providerRuntimeMessage: {
            runtimeEventId: EventId.make(`runtime-source-${input.streamVersion}`),
            runtimeEventType: input.streaming ? "content.delta" : "item.completed",
            providerInstanceId: input.correlationProviderInstanceId ?? providerInstanceId,
            providerTurnId,
            providerItemId: null,
          },
        },
  type: "thread.message-sent",
  payload: {
    threadId,
    messageId: MessageId.make(input.messageId),
    role: input.actor === "client" ? "user" : "assistant",
    text: input.text,
    turnId: input.actor === "client" ? null : providerTurnId,
    streaming: input.streaming,
    createdAt: at,
    updatedAt: at,
  },
});

const initialize = Effect.fn("initializeVerificationResultSourceTest")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DROP TABLE IF EXISTS orchestration_events`;
  yield* sql`
    CREATE TABLE orchestration_events (
      sequence INTEGER PRIMARY KEY,
      stream_version INTEGER NOT NULL,
      event_id TEXT NOT NULL,
      aggregate_kind TEXT NOT NULL,
      stream_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      occurred_at TEXT NOT NULL,
      command_id TEXT,
      causation_event_id TEXT,
      correlation_id TEXT,
      actor_kind TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      metadata_json TEXT NOT NULL
    )
  `;
  const insert = (event: OrchestrationEvent, actorKind: "client" | "provider") =>
    sql`
      INSERT INTO orchestration_events (
        sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
        occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
        payload_json, metadata_json
      ) VALUES (
        ${event.sequence}, ${event.sequence}, ${event.eventId}, ${event.aggregateKind},
        ${event.aggregateId}, ${event.type}, ${event.occurredAt}, ${event.commandId},
        ${event.causationEventId}, ${event.correlationId}, ${actorKind},
        ${canonicalJson(event.payload as JsonValue)},
        ${canonicalJson(event.metadata as JsonValue)}
      )
    `;
  for (let index = 1; index <= 4; index += 1) {
    yield* insert(
      messageEvent({
        streamVersion: index,
        messageId: `bootstrap-${index}`,
        text: "bootstrap",
        streaming: false,
        actor: "client",
      }),
      "client",
    );
  }
  return { sql, insert };
});

layer("orchestration verification result source", (it) => {
  it("bounds a single untrusted delta before it becomes capture authority", () => {
    const rawDelta = "🙂".repeat(256 * 1024);
    const bounded = makeBoundedVerificationResultDelta(rawDelta, null);
    assert.equal(bounded.byteLength, Buffer.byteLength(rawDelta));
    assert.equal(bounded.cumulativeByteLength, AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL);
    assert.isAtMost(Buffer.byteLength(bounded.text), 64 * 1024);
    assert.isBelow(bounded.text.length, rawDelta.length);
  });

  it("bounds completion-only fallback text without duplicating an existing durable delta", () => {
    const rawCompletion = "🙂".repeat(256 * 1024);
    const completionOnly = makeBoundedVerificationResultCompletion(rawCompletion, null);
    assert.equal(completionOnly.kind, "completion");
    assert.equal(
      completionOnly.outputByteLength,
      AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
    );
    assert.isString(completionOnly.completionText);
    assert.isAtMost(Buffer.byteLength(completionOnly.completionText!, "utf8"), 64 * 1024);

    const afterDelta = makeBoundedVerificationResultCompletion("must not be duplicated", {
      outputByteLength: 7,
      storedByteLength: 7,
    });
    assert.deepStrictEqual(afterDelta, {
      kind: "completion",
      completionText: null,
      outputByteLength: 7,
    });
  });

  it.effect("reconstructs bounded completion-only text exactly once", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const text = canonicalJson({
        report: "Completion-only source.",
        schemaVersion: "agent-control-verification-result-v1",
        verdict: "passed",
      });
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "completion-only",
          fragment: makeBoundedVerificationResultCompletion(text, null),
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(source.finalMessageId, "completion-only");
      assert.equal(source.sourceEventId, "verification-capture-event-5");
      assert.equal(source.outputByteLength, Buffer.byteLength(text));
      assert.equal(new TextDecoder().decode(source.bytes), text);
      assert.equal(source.outputDigest, sha256Utf8(text));
    }),
  );

  it.effect("reconstructs an oversize completion-only capture as the bounded sentinel", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const text = "x".repeat(64 * 1024 + 1);
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "completion-only-oversize",
          fragment: makeBoundedVerificationResultCompletion(text, null),
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(source.finalMessageId, "completion-only-oversize");
      assert.equal(source.sourceDisposition, "oversize");
      assert.equal(source.outputByteLength, AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL);
      assert.equal(source.bytes.byteLength, 0);
      assert.equal(source.outputDigest, null);
    }),
  );

  it.effect("reconstructs the last fully finalized Assistant message and exact bytes", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "first",
          fragment: { kind: "delta", text: "old", byteLength: 3, cumulativeByteLength: 3 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "first",
          fragment: { kind: "completion", completionText: null, outputByteLength: 3 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 7,
          messageId: "last",
          fragment: { kind: "delta", text: '{"schema', byteLength: 8, cumulativeByteLength: 8 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 8,
          messageId: "last",
          fragment: {
            kind: "delta",
            text: 'Version":1}',
            byteLength: 11,
            cumulativeByteLength: 19,
          },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 9,
          messageId: "last",
          fragment: { kind: "completion", completionText: null, outputByteLength: 19 },
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(new TextDecoder().decode(source.bytes), '{"schemaVersion":1}');
      assert.equal(source.finalMessageId, "last");
      assert.equal(source.sourceEventStreamVersion, 9);
      assert.equal(source.sourceEventId, "verification-capture-event-9");
      assert.equal(source.sourceDisposition, "captured");
      assert.match(source.outputDigest!, /^[0-9a-f]{64}$/u);
    }),
  );

  it.effect("seals a deterministic missing source when no Assistant message exists", () =>
    Effect.gen(function* () {
      const { sql } = yield* initialize();
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(source.sourceDisposition, "missing");
      assert.equal(source.finalMessageId, null);
      assert.equal(source.outputDigest, null);
      assert.equal(source.outputByteLength, 0);
    }),
  );

  it.effect("selects a distinct empty captured completion instead of an older verdict", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "message-a",
          fragment: {
            kind: "delta",
            text: '{"verdict":"passed"}',
            byteLength: 20,
            cumulativeByteLength: 20,
          },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "message-a",
          fragment: { kind: "completion", completionText: null, outputByteLength: 20 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 7,
          messageId: "message-b",
          fragment: { kind: "completion", completionText: "", outputByteLength: 0 },
        }),
        "provider",
      );
      const identity = {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
        handoffId: captureAuthority.handoffId,
        providerDeliveryId: captureAuthority.providerDeliveryId,
        resultSchemaFingerprint: captureAuthority.resultSchemaFingerprint,
      } as const;
      const source = yield* loadSealableVerificationResultSource(sql, identity);
      assert.equal(source.finalMessageId, "message-b");
      assert.equal(source.sourceEventStreamVersion, 7);
      assert.equal(source.outputByteLength, 0);
      assert.equal(new TextDecoder().decode(source.bytes), "");
      assert.deepStrictEqual(yield* loadOpenVerificationResultMessageIds(sql, identity), []);
      assert.deepStrictEqual(
        yield* loadVerificationResultCapturedMessage(sql, identity, MessageId.make("message-b")),
        { text: "", completed: true, outputByteLength: 0, storedByteLength: 0 },
      );
    }),
  );

  it.effect("seals oversize bytes from bounded fragments without retaining a raw digest", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "oversize",
          fragment: {
            kind: "delta",
            text: "x".repeat(64 * 1024),
            byteLength: 64 * 1024,
            cumulativeByteLength: 64 * 1024,
          },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "oversize",
          fragment: { kind: "delta", text: "", byteLength: 1, cumulativeByteLength: 65537 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 7,
          messageId: "oversize",
          fragment: { kind: "completion", completionText: null, outputByteLength: 65537 },
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(source.sourceDisposition, "oversize");
      assert.equal(source.outputByteLength, 64 * 1024 + 1);
      assert.equal(source.bytes.byteLength, 0);
      assert.equal(source.outputDigest, null);
    }),
  );

  it.effect("keeps an exact 64 KiB source captured rather than oversize", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const text = "x".repeat(64 * 1024);
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "exact-limit",
          fragment: {
            kind: "delta",
            text,
            byteLength: 64 * 1024,
            cumulativeByteLength: 64 * 1024,
          },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "exact-limit",
          fragment: {
            kind: "completion",
            completionText: null,
            outputByteLength: 64 * 1024,
          },
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
      });
      assert.equal(source.sourceDisposition, "captured");
      assert.equal(source.outputByteLength, 64 * 1024);
      assert.equal(source.bytes.byteLength, 64 * 1024);
      assert.equal(source.outputDigest, sha256Utf8(text));
    }),
  );

  it.effect("reconstructs many fragments across multiple keyset pages with a fixed buffer", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      let streamVersion = 5;
      let cumulativeByteLength = 0;
      for (let index = 0; index < 70; index += 1) {
        const byteLength = 1024;
        const text = cumulativeByteLength < 64 * 1024 ? "x".repeat(byteLength) : "";
        cumulativeByteLength = Math.min(
          AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
          cumulativeByteLength + byteLength,
        );
        yield* insert(
          captureEvent({
            streamVersion,
            messageId: "many-fragments",
            fragment: { kind: "delta", text, byteLength, cumulativeByteLength },
          }),
          "provider",
        );
        streamVersion += 1;
      }
      yield* insert(
        captureEvent({
          streamVersion,
          messageId: "many-fragments",
          fragment: {
            kind: "completion",
            completionText: null,
            outputByteLength: cumulativeByteLength,
          },
        }),
        "provider",
      );
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
        handoffId: captureAuthority.handoffId,
        providerDeliveryId: captureAuthority.providerDeliveryId,
        resultSchemaFingerprint: captureAuthority.resultSchemaFingerprint,
      });
      assert.equal(source.sourceDisposition, "oversize");
      assert.equal(source.outputByteLength, AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL);
      assert.equal(source.bytes.byteLength, 0);
      assert.equal(source.outputDigest, null);
      assert.equal(source.sourceEventStreamVersion, streamVersion);
    }),
  );

  it.effect("fails closed on foreign Provider correlation and runtime lineage", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        messageEvent({
          streamVersion: 5,
          messageId: "foreign",
          text: "untrusted",
          streaming: true,
          correlationProviderInstanceId: ProviderInstanceId.make("foreign-provider"),
        }),
        "provider",
      );
      const foreign = yield* Effect.flip(
        loadSealableVerificationResultSource(sql, {
          threadId,
          providerInstanceId,
          providerTurnId,
          afterStreamVersion: 4,
        }),
      );
      assert.equal(foreign.operation, "result-source-message-identity");

      yield* sql`DELETE FROM orchestration_events WHERE sequence = 5`;
      const event = messageEvent({
        streamVersion: 5,
        messageId: "lineage",
        text: "untrusted",
        streaming: true,
      });
      yield* insert(
        {
          ...event,
          commandId: CommandId.make("provider:foreign-runtime-event:assistant-delta"),
          correlationId: CommandId.make("provider:foreign-runtime-event:assistant-delta"),
        },
        "provider",
      );
      const lineage = yield* Effect.flip(
        loadSealableVerificationResultSource(sql, {
          threadId,
          providerInstanceId,
          providerTurnId,
          afterStreamVersion: 4,
        }),
      );
      assert.equal(lineage.operation, "result-source-message-identity");
    }),
  );

  it.effect("fails closed on an untagged Prompt-v2 Assistant completion", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        messageEvent({
          streamVersion: 5,
          messageId: "untagged-completion",
          text: '{"verdict":"passed"}',
          streaming: false,
        }),
        "provider",
      );
      const failure = yield* Effect.flip(
        loadSealableVerificationResultSource(sql, {
          threadId,
          providerInstanceId,
          providerTurnId,
          afterStreamVersion: 4,
          handoffId: captureAuthority.handoffId,
          providerDeliveryId: captureAuthority.providerDeliveryId,
          resultSchemaFingerprint: captureAuthority.resultSchemaFingerprint,
        }),
      );
      assert.equal(failure.operation, "result-source-capture-authority");
      assert.equal(failure.reason, "authority-conflict");
    }),
  );

  it.effect("fails closed on an Assistant suffix after the sealed stream version", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "sealed",
          fragment: { kind: "delta", text: "ok", byteLength: 2, cumulativeByteLength: 2 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "sealed",
          fragment: { kind: "completion", completionText: null, outputByteLength: 2 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 7,
          messageId: "suffix",
          fragment: { kind: "delta", text: "late", byteLength: 4, cumulativeByteLength: 4 },
        }),
        "provider",
      );
      const failure = yield* Effect.flip(
        loadSealableVerificationResultSource(sql, {
          threadId,
          providerInstanceId,
          providerTurnId,
          afterStreamVersion: 4,
          sealedAtStreamVersion: 6,
        }),
      );
      assert.isTrue(isVerificationResultHistoryError(failure));
      assert.equal(failure.operation, "result-source-message-identity");
    }),
  );

  it.effect("rejects non-TEXT routing-correlated storage", () =>
    Effect.gen(function* () {
      const { sql } = yield* initialize();
      yield* sql`
        UPDATE orchestration_events
        SET actor_kind = CAST(actor_kind AS BLOB)
        WHERE sequence = 1
      `;
      const failure = yield* Effect.flip(
        loadSealableVerificationResultSource(sql, {
          threadId,
          providerInstanceId,
          providerTurnId,
          afterStreamVersion: 4,
        }),
      );
      assert.equal(failure.operation, "result-source-actor-kind-storage");
    }),
  );

  it.effect("reads result authority from MAIN despite TEMP and attached shadows", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "main-source",
          fragment: { kind: "delta", text: "main", byteLength: 4, cumulativeByteLength: 4 },
        }),
        "provider",
      );
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "main-source",
          fragment: { kind: "completion", completionText: null, outputByteLength: 4 },
        }),
        "provider",
      );
      yield* sql`
        CREATE TEMP TABLE orchestration_events
        AS SELECT * FROM main.orchestration_events WHERE 0
      `;
      yield* sql`ATTACH ':memory:' AS result_shadow`;
      yield* sql`
        CREATE TABLE result_shadow.orchestration_events
        AS SELECT * FROM main.orchestration_events WHERE 0
      `;
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId,
        providerInstanceId,
        providerTurnId,
        afterStreamVersion: 4,
        handoffId: captureAuthority.handoffId,
        providerDeliveryId: captureAuthority.providerDeliveryId,
        resultSchemaFingerprint: captureAuthority.resultSchemaFingerprint,
      });
      assert.equal(new TextDecoder().decode(source.bytes), "main");
      assert.equal(source.sourceEventId, "verification-capture-event-6");
      assert.deepStrictEqual(yield* sql`SELECT count(*) AS count FROM temp.orchestration_events`, [
        { count: 0 },
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT count(*) AS count FROM result_shadow.orchestration_events`,
        [{ count: 0 }],
      );
    }),
  );
});
