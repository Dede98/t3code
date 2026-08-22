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
import { canonicalJson, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  loadSealableVerificationResultSource,
  VerificationResultHistoryError,
} from "./orchestrationResultSource.ts";

const isVerificationResultHistoryError = Schema.is(VerificationResultHistoryError);

const layer = it.layer(NodeSqliteClient.layerMemory());
const threadId = ThreadId.make("verification-source-thread");
const providerInstanceId = ProviderInstanceId.make("codex");
const providerTurnId = TurnId.make("verification-provider-turn");
const at = "2026-08-22T12:00:00.000Z";

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
  it.effect("reconstructs the last fully finalized Assistant message and exact bytes", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        messageEvent({ streamVersion: 5, messageId: "first", text: "old", streaming: true }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 6, messageId: "first", text: "", streaming: false }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 7, messageId: "last", text: '{"schema', streaming: true }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 8, messageId: "last", text: 'Version":1}', streaming: true }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 9, messageId: "last", text: "", streaming: false }),
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

  it.effect("seals oversize bytes without truncating or inventing a verdict", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const output = "x".repeat(64 * 1024 + 1);
      yield* insert(
        messageEvent({ streamVersion: 5, messageId: "oversize", text: output, streaming: true }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 6, messageId: "oversize", text: "", streaming: false }),
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
      assert.equal(source.bytes.byteLength, source.outputByteLength);
      assert.match(source.outputDigest!, /^[0-9a-f]{64}$/u);
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

  it.effect("fails closed on an Assistant suffix after the sealed stream version", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      yield* insert(
        messageEvent({ streamVersion: 5, messageId: "sealed", text: "ok", streaming: true }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 6, messageId: "sealed", text: "", streaming: false }),
        "provider",
      );
      yield* insert(
        messageEvent({ streamVersion: 7, messageId: "suffix", text: "late", streaming: true }),
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
      assert.equal(failure.operation, "result-source-message-after-seal");
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
});
