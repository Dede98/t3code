// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import * as NodeSqliteClient from "../persistence/NodeSqliteClient.ts";
import { canonicalJson } from "../agentControl/initialPlanning/eventEvidence.ts";
import {
  loadOrchestrationEventBySequence,
  loadOrchestrationEventStreamPage,
  loadOrchestrationEventsByCommandIdPage,
  loadOrchestrationProjectThreadCreationsPage,
  loadOrchestrationThreadAuthorityStreamsPage,
  type OrchestrationCommandReplayQueryObservation,
  OrchestrationEventRawHistoryError,
} from "./orchestrationEventRaw.ts";

const isRawHistoryError = Schema.is(OrchestrationEventRawHistoryError);
const layer = it.layer(NodeSqliteClient.layerMemory());
const at = "2026-08-28T10:00:00.000Z";
const threadId = "raw-authority-thread";
const commandId = "raw-authority-command";

const payloadValue = {
  attachments: [],
  createdAt: at,
  messageId: "raw-authority-message",
  role: "user",
  streaming: false,
  text: "raw authority",
  threadId,
  turnId: null,
  updatedAt: at,
} as const;
const payload = canonicalJson(payloadValue);

const metadataValue = {
  providerRuntimeMessage: {
    runtimeEventId: "raw-runtime-event",
    eventType: "item.completed",
    providerInstanceId: "codex",
    providerTurnId: "raw-provider-turn",
    providerItemId: "raw-provider-item",
  },
  verificationResultCapture: {
    schemaVersion: 1,
    disposition: "authority",
    handoffId: "raw-handoff",
    providerDeliveryId: "raw-delivery",
    providerInstanceId: "codex",
    providerTurnId: "raw-provider-turn",
    resultSchemaFingerprint: "f".repeat(64),
  },
} as const;
const metadata = canonicalJson(metadataValue);

const createTable = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    yield* sql`DROP TABLE IF EXISTS main.orchestration_events`;
    yield* sql`
      CREATE TABLE main.orchestration_events (
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
    yield* sql`CREATE INDEX main.idx_orch_events_stream_sequence
      ON orchestration_events(aggregate_kind, stream_id, sequence)`;
    yield* sql`CREATE INDEX main.idx_orchestration_events_command_id_bytes_sequence
      ON orchestration_events(CAST(command_id AS BLOB), sequence)
      WHERE command_id IS NOT NULL`;
    yield* sql`CREATE INDEX main.idx_orchestration_events_stream_bytes_sequence
      ON orchestration_events(
        CAST(aggregate_kind AS BLOB), CAST(stream_id AS BLOB), sequence
      )`;
    yield* sql`CREATE INDEX main.idx_orchestration_events_payload_thread_bytes_sequence
      ON orchestration_events(
        CAST(json_extract(payload_json, '$.threadId') AS BLOB), sequence
      )`;
    yield* sql`CREATE INDEX main.idx_orchestration_events_thread_project_bytes_sequence
      ON orchestration_events(
        CAST(event_type AS BLOB), CAST(json_extract(payload_json, '$.projectId') AS BLOB),
        sequence, CAST(stream_id AS BLOB)
      )`;
  });

const insertEvent = (
  sql: SqlClient.SqlClient,
  input: {
    readonly sequence?: number;
    readonly streamVersion?: number;
    readonly eventId?: unknown;
    readonly aggregateKind?: unknown;
    readonly streamId?: unknown;
    readonly eventType?: unknown;
    readonly occurredAt?: unknown;
    readonly commandId?: unknown;
    readonly causationEventId?: unknown;
    readonly correlationId?: unknown;
    readonly actorKind?: unknown;
    readonly payload?: unknown;
    readonly metadata?: unknown;
  } = {},
) => {
  const sequence = input.sequence ?? 1;
  const streamId = input.streamId ?? threadId;
  const eventPayload =
    input.payload ??
    (typeof streamId === "string"
      ? canonicalJson({
          ...payloadValue,
          threadId: streamId,
        })
      : payload);
  return sql`
    INSERT INTO main.orchestration_events (
      sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
      occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
      payload_json, metadata_json
    ) VALUES (
      ${sequence}, ${input.streamVersion ?? sequence},
      ${input.eventId ?? `raw-authority-event-${sequence}`},
      ${input.aggregateKind ?? "thread"}, ${streamId},
      ${input.eventType ?? "thread.message-sent"}, ${input.occurredAt ?? at},
      ${input.commandId ?? commandId}, ${input.causationEventId ?? "raw-causation-event"},
      ${input.correlationId ?? commandId}, ${input.actorKind ?? "provider"},
      ${eventPayload}, ${input.metadata ?? metadata}
    )
  `;
};

const insertThreadCreated = (
  sql: SqlClient.SqlClient,
  sequence: number,
  projectId: string,
  createdThreadId: string,
) =>
  insertEvent(sql, {
    sequence,
    streamVersion: 1,
    streamId: createdThreadId,
    eventType: "thread.created",
    commandId: `create-${createdThreadId}`,
    actorKind: "client",
    payload: canonicalJson({
      branch: null,
      createdAt: at,
      interactionMode: "default",
      modelSelection: { instanceId: "codex", model: "gpt-5-codex" },
      projectId,
      runtimeMode: "approval-required",
      threadId: createdThreadId,
      title: createdThreadId,
      updatedAt: at,
      worktreePath: null,
    }),
    metadata: "{}",
  });

const readSequence = (sql: SqlClient.SqlClient, sequence = 1) =>
  loadOrchestrationEventBySequence(sql, {
    sequence,
    operationPrefix: "raw-authority-test",
  });

const expectRawFailure = Effect.fn("expectRawFailure")(function* (
  effect: Effect.Effect<unknown, OrchestrationEventRawHistoryError>,
  label: string,
) {
  const failure = yield* Effect.match(effect, {
    onFailure: (error) => error,
    onSuccess: () => undefined,
  });
  assert.isTrue(isRawHistoryError(failure), label);
  if (!isRawHistoryError(failure)) return;
  assert.equal(failure.reason, "corrupt-history", label);
});

const loadAllCommandCandidates = Effect.fn("loadAllCommandCandidates")(function* (
  sql: SqlClient.SqlClient,
  expectedCommandId: string,
  onQuery?: (observation: OrchestrationCommandReplayQueryObservation) => void,
) {
  const rows = [];
  let cursor = 0;
  while (true) {
    const page = yield* loadOrchestrationEventsByCommandIdPage(sql, {
      commandId: expectedCommandId,
      sequenceExclusive: cursor,
      operationPrefix: "raw-command-candidate-test",
      ...(onQuery === undefined ? {} : { onQuery }),
    });
    rows.push(...page.rows);
    if (page.rows.length === 0) return rows;
    cursor = page.nextSequenceExclusive;
  }
});

layer("raw orchestration event authority", (it) => {
  it.effect("accepts the closed current encoders and exact historical four-field form", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* createTable(sql);
      yield* insertEvent(sql);
      const current = yield* readSequence(sql);
      assert.equal(current?.event.eventId, "raw-authority-event-1");
      assert.equal(current?.payloadSource, payload);
      assert.equal(current?.metadataSource, metadata);

      // Exact ThreadMessageSentPayload field order emitted by the schema/object
      // encoder at parent 8994c6a900d80c390e99824984c808dd2017ecb9.
      // Do not rebuild this with the current helper.
      const parentPayload =
        '{"threadId":"raw-authority-thread","messageId":"raw-authority-message","role":"user","text":"raw authority","attachments":[],"turnId":null,"streaming":false,"createdAt":"2026-08-28T10:00:00.000Z","updatedAt":"2026-08-28T10:00:00.000Z"}';
      yield* sql`UPDATE main.orchestration_events SET payload_json=${parentPayload} WHERE sequence=1`;
      assert.equal((yield* readSequence(sql))?.payloadSource, parentPayload);

      const schemaOrder =
        '{"providerRuntimeMessage":{"runtimeEventId":"raw-runtime-event","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"raw-provider-turn","providerItemId":"raw-provider-item"},"verificationResultCapture":{"schemaVersion":1,"disposition":"authority","handoffId":"raw-handoff","providerDeliveryId":"raw-delivery","providerInstanceId":"codex","providerTurnId":"raw-provider-turn","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';
      yield* sql`UPDATE main.orchestration_events SET metadata_json=${schemaOrder} WHERE sequence=1`;
      assert.equal((yield* readSequence(sql))?.metadataSource, schemaOrder);

      const historical =
        '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}';
      yield* sql`UPDATE main.orchestration_events SET metadata_json=${historical} WHERE sequence=1`;
      const legacy = yield* readSequence(sql);
      assert.equal(legacy?.metadataSource, historical);
      assert.deepStrictEqual(legacy?.event.metadata.providerRuntimeMessage as unknown, {
        runtimeEventId: "event-historical",
        eventType: "item.completed",
        providerInstanceId: "codex",
        providerTurnId: "turn-historical",
        providerItemId: null,
      });

      const historicalWithItem =
        '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-item","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical-item","providerItemId":"item-historical"}}';
      yield* sql`UPDATE main.orchestration_events SET metadata_json=${historicalWithItem} WHERE sequence=1`;
      const legacyWithItem = yield* readSequence(sql);
      assert.equal(legacyWithItem?.metadataSource, historicalWithItem);
      assert.deepStrictEqual(legacyWithItem?.event.metadata.providerRuntimeMessage as unknown, {
        runtimeEventId: "event-historical-item",
        eventType: "item.completed",
        providerInstanceId: "codex",
        providerTurnId: "turn-historical-item",
        providerItemId: "item-historical",
      });

      const historicalWithItemAndCapture =
        '{"providerRuntimeMessage":{"runtimeEventId":"event-historical-capture","runtimeEventType":"content.delta","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","providerItemId":"item-historical-capture"},"verificationResultCapture":{"schemaVersion":1,"disposition":"presentation","handoffId":"handoff-historical-capture","providerDeliveryId":"delivery-historical-capture","providerInstanceId":"codex","providerTurnId":"turn-historical-capture","resultSchemaFingerprint":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}}';
      yield* sql`UPDATE main.orchestration_events
        SET metadata_json=${historicalWithItemAndCapture} WHERE sequence=1`;
      const legacyWithCapture = yield* readSequence(sql);
      assert.equal(legacyWithCapture?.metadataSource, historicalWithItemAndCapture);
      assert.deepStrictEqual(
        legacyWithCapture?.event.metadata.verificationResultCapture as unknown,
        {
          schemaVersion: 1,
          disposition: "presentation",
          handoffId: "handoff-historical-capture",
          providerDeliveryId: "delivery-historical-capture",
          providerInstanceId: "codex",
          providerTurnId: "turn-historical-capture",
          resultSchemaFingerprint: "f".repeat(64),
        },
      );
    }),
  );

  it.effect("rejects transformed envelope identifiers, NUL, and non-production timestamps", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const variants: ReadonlyArray<{
        readonly label: string;
        readonly update: (sql: SqlClient.SqlClient) => Effect.Effect<unknown, SqlError>;
      }> = [
        {
          label: "event-id-leading",
          update: (db) =>
            db`UPDATE main.orchestration_events SET event_id=' raw-authority-event-1' WHERE sequence=1`,
        },
        {
          label: "event-id-trailing",
          update: (db) =>
            db`UPDATE main.orchestration_events SET event_id='raw-authority-event-1 ' WHERE sequence=1`,
        },
        {
          label: "stream-id-leading",
          update: (db) =>
            db`UPDATE main.orchestration_events SET stream_id=' raw-authority-thread' WHERE sequence=1`,
        },
        {
          label: "stream-id-trailing",
          update: (db) =>
            db`UPDATE main.orchestration_events SET stream_id='raw-authority-thread ' WHERE sequence=1`,
        },
        {
          label: "command-id-leading",
          update: (db) =>
            db`UPDATE main.orchestration_events SET command_id=' raw-authority-command' WHERE sequence=1`,
        },
        {
          label: "command-id-trailing",
          update: (db) =>
            db`UPDATE main.orchestration_events SET command_id='raw-authority-command ' WHERE sequence=1`,
        },
        {
          label: "causation-leading",
          update: (db) =>
            db`UPDATE main.orchestration_events SET causation_event_id=' raw-causation-event' WHERE sequence=1`,
        },
        {
          label: "causation-trailing",
          update: (db) =>
            db`UPDATE main.orchestration_events SET causation_event_id='raw-causation-event ' WHERE sequence=1`,
        },
        {
          label: "correlation-leading",
          update: (db) =>
            db`UPDATE main.orchestration_events SET correlation_id=' raw-authority-command' WHERE sequence=1`,
        },
        {
          label: "correlation-trailing",
          update: (db) =>
            db`UPDATE main.orchestration_events SET correlation_id='raw-authority-command ' WHERE sequence=1`,
        },
        {
          label: "aggregate-kind",
          update: (db) =>
            db`UPDATE main.orchestration_events SET aggregate_kind=' thread' WHERE sequence=1`,
        },
        {
          label: "event-type",
          update: (db) =>
            db`UPDATE main.orchestration_events SET event_type='thread.message-sent ' WHERE sequence=1`,
        },
        {
          label: "actor-kind",
          update: (db) =>
            db`UPDATE main.orchestration_events SET actor_kind=' provider' WHERE sequence=1`,
        },
        {
          label: "envelope-nul",
          update: (db) =>
            db`UPDATE main.orchestration_events SET event_id=${"raw\0event"} WHERE sequence=1`,
        },
        {
          label: "not-a-time",
          update: (db) =>
            db`UPDATE main.orchestration_events SET occurred_at='not-a-time' WHERE sequence=1`,
        },
        {
          label: "invalid-calendar",
          update: (db) =>
            db`UPDATE main.orchestration_events SET occurred_at='2026-02-30T10:00:00.000Z' WHERE sequence=1`,
        },
        {
          label: "missing-timezone",
          update: (db) =>
            db`UPDATE main.orchestration_events SET occurred_at='2026-08-28T10:00:00.000' WHERE sequence=1`,
        },
        {
          label: "offset-form",
          update: (db) =>
            db`UPDATE main.orchestration_events SET occurred_at='2026-08-28T12:00:00.000+02:00' WHERE sequence=1`,
        },
      ];
      for (const variant of variants) {
        yield* createTable(sql);
        yield* insertEvent(sql);
        yield* variant.update(sql);
        yield* expectRawFailure(readSequence(sql), variant.label);
      }
    }),
  );

  it.effect(
    "rejects schema transformations, defaults, extra keys, NUL, and noncanonical JSON bytes",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const metadataVariants = [
          [
            "handoff-id",
            {
              ...metadataValue,
              verificationResultCapture: {
                ...metadataValue.verificationResultCapture,
                handoffId: " raw-handoff ",
              },
            },
          ],
          [
            "delivery-id",
            {
              ...metadataValue,
              verificationResultCapture: {
                ...metadataValue.verificationResultCapture,
                providerDeliveryId: " raw-delivery ",
              },
            },
          ],
          [
            "fingerprint",
            {
              ...metadataValue,
              verificationResultCapture: {
                ...metadataValue.verificationResultCapture,
                resultSchemaFingerprint: ` ${"f".repeat(64)} `,
              },
            },
          ],
          [
            "runtime-event-id",
            {
              ...metadataValue,
              providerRuntimeMessage: {
                ...metadataValue.providerRuntimeMessage,
                runtimeEventId: " raw-runtime-event ",
              },
            },
          ],
          [
            "runtime-provider-instance",
            {
              ...metadataValue,
              providerRuntimeMessage: {
                ...metadataValue.providerRuntimeMessage,
                providerInstanceId: " codex ",
              },
            },
          ],
          [
            "runtime-provider-turn",
            {
              ...metadataValue,
              providerRuntimeMessage: {
                ...metadataValue.providerRuntimeMessage,
                providerTurnId: " raw-provider-turn ",
              },
            },
          ],
          [
            "runtime-provider-item",
            {
              ...metadataValue,
              providerRuntimeMessage: {
                ...metadataValue.providerRuntimeMessage,
                providerItemId: " raw-provider-item ",
              },
            },
          ],
          [
            "metadata-nul",
            {
              ...metadataValue,
              providerRuntimeMessage: {
                ...metadataValue.providerRuntimeMessage,
                providerItemId: "raw\0item",
              },
            },
          ],
          ["metadata-extra", { ...metadataValue, unexpected: true }],
        ] as const;
        for (const [label, value] of metadataVariants) {
          yield* createTable(sql);
          yield* insertEvent(sql, { metadata: canonicalJson(value) });
          yield* expectRawFailure(readSequence(sql), label);
        }

        const payloadVariants = [
          ["payload-trim", canonicalJson({ ...payloadValue, threadId: ` ${threadId} ` })],
          ["payload-nul", canonicalJson({ ...payloadValue, text: "raw\0payload" })],
          ["payload-extra", canonicalJson({ ...payloadValue, unexpected: true })],
          ["payload-noncanonical", ` ${payload}`],
          [
            "payload-unregistered-order",
            '{"messageId":"raw-authority-message","threadId":"raw-authority-thread","role":"user","text":"raw authority","attachments":[],"turnId":null,"streaming":false,"createdAt":"2026-08-28T10:00:00.000Z","updatedAt":"2026-08-28T10:00:00.000Z"}',
          ],
        ] as const;
        for (const [label, value] of payloadVariants) {
          yield* createTable(sql);
          yield* insertEvent(sql, { payload: value });
          yield* expectRawFailure(readSequence(sql), label);
        }

        yield* createTable(sql);
        yield* insertEvent(sql, {
          eventType: "thread.turn-start-requested",
          payload: canonicalJson({
            threadId,
            messageId: "raw-authority-message",
            createdAt: at,
          }),
        });
        yield* expectRawFailure(readSequence(sql), "missing-decoding-defaults");
      }),
  );

  it.effect("rejects historical permutations and current or legacy mixed metadata", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const variants = [
        '{"providerRuntimeMessage":{"providerInstanceId":"codex","providerTurnId":"turn-historical","runtimeEventId":"event-historical","runtimeEventType":"item.completed"}}',
        '{"providerRuntimeMessage":{"runtimeEventType":"item.completed","runtimeEventId":"event-historical","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
        '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}',
      ] as const;
      for (const [index, value] of variants.entries()) {
        yield* createTable(sql);
        yield* insertEvent(sql, { metadata: value });
        yield* expectRawFailure(readSequence(sql), `historical-variant-${index}`);
      }
    }),
  );

  it.effect(
    "loads the exact natural TEXT command chain and rejects BLOB siblings before or after it",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* createTable(sql);
        yield* insertEvent(sql, { sequence: 1 });
        assert.equal((yield* loadAllCommandCandidates(sql, commandId)).length, 1);

        for (const blobSequence of [1, 3]) {
          yield* createTable(sql);
          if (blobSequence === 1) {
            yield* insertEvent(sql, { sequence: 1, commandId: Buffer.from(commandId) });
            yield* insertEvent(sql, { sequence: 2 });
          } else {
            yield* insertEvent(sql, { sequence: 1 });
            yield* insertEvent(sql, { sequence: 2 });
            yield* insertEvent(sql, { sequence: 3, commandId: Buffer.from(commandId) });
          }
          yield* expectRawFailure(loadAllCommandCandidates(sql, commandId), `blob-${blobSequence}`);
        }

        yield* createTable(sql);
        yield* insertEvent(sql, { sequence: 1 });
        yield* insertEvent(sql, { sequence: 2 });
        const natural = yield* loadAllCommandCandidates(sql, commandId);
        assert.deepStrictEqual(
          natural.map((row) => row.event.sequence),
          [1, 2],
        );
      }),
  );

  it.effect(
    "documents that INTEGER command input is not physically representable in the TEXT column",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* createTable(sql);
        yield* insertEvent(sql, { sequence: 1, commandId: 123 });
        assert.deepStrictEqual(
          yield* sql`SELECT typeof(command_id) AS storage FROM main.orchestration_events`,
          [{ storage: "text" }],
        );
      }),
  );

  it.effect("finds a wrong-storage candidate at the 32/33 keyset page boundary", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* createTable(sql);
      for (let sequence = 1; sequence <= 32; sequence += 1) {
        yield* insertEvent(sql, { sequence });
      }
      yield* insertEvent(sql, { sequence: 33, commandId: Buffer.from(commandId) });
      const first = yield* loadOrchestrationEventsByCommandIdPage(sql, {
        commandId,
        sequenceExclusive: 0,
        operationPrefix: "raw-page-boundary",
      });
      assert.equal(first.rows.length, 32);
      assert.equal(first.nextSequenceExclusive, 32);
      yield* expectRawFailure(
        loadOrchestrationEventsByCommandIdPage(sql, {
          commandId,
          sequenceExclusive: first.nextSequenceExclusive,
          operationPrefix: "raw-page-boundary",
        }),
        "page-33-blob",
      );
    }),
  );

  it.effect("validates command candidates against each physical stream predecessor", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* createTable(sql);
      const predecessorPlan = yield* sql<{ readonly detail: string }>`
        EXPLAIN QUERY PLAN
        SELECT sequence
        FROM main.orchestration_events INDEXED BY idx_orch_events_stream_sequence
        WHERE sequence < 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
        ORDER BY sequence DESC
        LIMIT 1
      `;
      assert.isTrue(
        predecessorPlan.some((row) => row.detail.includes("idx_orch_events_stream_sequence")),
      );
      yield* insertEvent(sql, { sequence: 1, streamVersion: 0 });
      assert.deepStrictEqual(
        (yield* loadAllCommandCandidates(sql, commandId)).map((row) => row.streamVersion),
        [0],
      );

      yield* createTable(sql);
      yield* insertEvent(sql, { sequence: 1, streamVersion: 1, commandId: "other-command" });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 0 });
      yield* expectRawFailure(
        loadAllCommandCandidates(sql, commandId),
        "version-zero-after-version-one",
      );

      yield* createTable(sql);
      for (let version = 1; version <= 4; version += 1) {
        yield* insertEvent(sql, {
          sequence: version,
          streamVersion: version,
          commandId: `other-command-${version}`,
        });
      }
      yield* insertEvent(sql, { sequence: 5, streamVersion: 5 });
      assert.deepStrictEqual(
        (yield* loadAllCommandCandidates(sql, commandId)).map((row) => row.streamVersion),
        [5],
      );

      yield* createTable(sql);
      yield* insertEvent(sql, { sequence: 1, streamVersion: 4, commandId: "other-command" });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 6 });
      yield* expectRawFailure(loadAllCommandCandidates(sql, commandId), "missing-version-five");

      yield* createTable(sql);
      yield* insertEvent(sql, { sequence: 1, streamVersion: 1 });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 2, commandId: "other-command" });
      yield* insertEvent(sql, { sequence: 3, streamVersion: 3 });
      assert.deepStrictEqual(
        (yield* loadAllCommandCandidates(sql, commandId)).map((row) => row.streamVersion),
        [1, 3],
      );

      yield* createTable(sql);
      yield* insertEvent(sql, { sequence: 1, streamVersion: 1, streamId: "interleaved-a" });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 1, streamId: "interleaved-b" });
      assert.deepStrictEqual(
        (yield* loadAllCommandCandidates(sql, commandId)).map((row) => row.event.aggregateId),
        ["interleaved-a", "interleaved-b"],
      );

      yield* createTable(sql);
      for (let sequence = 1; sequence <= 33; sequence += 1) {
        yield* insertEvent(sql, { sequence, streamVersion: sequence });
      }
      assert.equal((yield* loadAllCommandCandidates(sql, commandId)).length, 33);

      yield* createTable(sql);
      yield* insertEvent(sql, {
        sequence: 1,
        streamVersion: 1,
        commandId: "other-command",
        payload:
          '{"threadId":"raw-authority-thread","threadId":"attacker","messageId":"raw-authority-message","role":"user","text":"raw authority","attachments":[],"turnId":null,"streaming":false,"createdAt":"2026-08-28T10:00:00.000Z","updatedAt":"2026-08-28T10:00:00.000Z"}',
      });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 2 });
      yield* expectRawFailure(loadAllCommandCandidates(sql, commandId), "corrupt-predecessor");

      yield* createTable(sql);
      yield* insertEvent(sql, {
        sequence: 1,
        streamVersion: 1,
        commandId: "other-command",
        aggregateKind: Buffer.from("thread"),
        streamId: Buffer.from(threadId),
      });
      yield* insertEvent(sql, { sequence: 2, streamVersion: 2 });
      yield* expectRawFailure(loadAllCommandCandidates(sql, commandId), "blob-predecessor");
    }),
  );

  it.effect(
    "batches no-hit, early-hit, and late-hit predecessors by 32 independent of global history",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        for (const { candidateCount, candidatesFirst } of [
          { candidateCount: 0, candidatesFirst: false },
          { candidateCount: 31, candidatesFirst: true },
          { candidateCount: 32, candidatesFirst: false },
          { candidateCount: 33, candidatesFirst: false },
        ]) {
          yield* createTable(sql);
          const irrelevantCount = 1_088;
          for (let ordinal = 1; ordinal <= candidateCount; ordinal += 1) {
            yield* insertEvent(sql, {
              sequence: (candidatesFirst ? 0 : irrelevantCount) + ordinal,
              streamVersion: 1,
              streamId: `candidate-stream-${ordinal}`,
            });
          }
          for (let sequence = 1; sequence <= irrelevantCount; sequence += 1) {
            yield* insertEvent(sql, {
              sequence: (candidatesFirst ? candidateCount : 0) + sequence,
              streamVersion: 1,
              streamId: `irrelevant-stream-${sequence}`,
              commandId: `irrelevant-command-${sequence}`,
            });
          }
          const observations: Array<OrchestrationCommandReplayQueryObservation> = [];
          assert.equal(
            (yield* loadAllCommandCandidates(sql, commandId, (entry) => observations.push(entry)))
              .length,
            candidateCount,
          );
          const candidateQueries = observations.filter(
            (entry) => entry.kind === "command-candidates",
          );
          const predecessorQueries = observations.filter(
            (entry) => entry.kind === "stream-predecessors",
          );
          assert.equal(candidateQueries.length, Math.ceil(candidateCount / 32) + 1);
          assert.equal(predecessorQueries.length, Math.ceil(candidateCount / 32));
          assert.equal(
            candidateQueries.reduce((sum, entry) => sum + entry.rowCount, 0),
            candidateCount,
          );
          assert.equal(
            predecessorQueries.reduce((sum, entry) => sum + entry.rowCount, 0),
            candidateCount,
          );

          const repeated: Array<OrchestrationCommandReplayQueryObservation> = [];
          yield* loadAllCommandCandidates(sql, commandId, (entry) => repeated.push(entry));
          assert.deepStrictEqual(repeated, observations);
        }
      }),
  );

  it.effect(
    "bounds project thread discovery and authority reconstruction per 32-thread group",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const projectId = "raw-project-delete-cost";
        for (const threadCount of [1_023, 1_024, 1_025]) {
          yield* createTable(sql);
          for (let sequence = 1; sequence <= threadCount; sequence += 1) {
            yield* insertThreadCreated(sql, sequence, projectId, `cost-thread-${sequence}`);
          }
          const observations: Array<OrchestrationCommandReplayQueryObservation> = [];
          let creationCursor = 0;
          let reconstructedRows = 0;
          while (true) {
            const creations = yield* loadOrchestrationProjectThreadCreationsPage(sql, {
              projectId,
              sequenceExclusive: creationCursor,
              sequenceUpperExclusive: threadCount + 1,
              operationPrefix: "raw-project-delete-cost",
              onQuery: (entry) => observations.push(entry),
            });
            if (creations.rows.length === 0) break;
            const threadIds = creations.rows.map((row) => row.event.aggregateId);
            let groupCursor = 0;
            let streamVersions = new Map<string, number>();
            while (true) {
              const streams = yield* loadOrchestrationThreadAuthorityStreamsPage(sql, {
                threadIds,
                sequenceExclusive: groupCursor,
                sequenceUpperExclusive: threadCount + 1,
                previousStreamVersions: streamVersions,
                operationPrefix: "raw-project-delete-cost",
                onQuery: (entry) => observations.push(entry),
              });
              reconstructedRows += streams.rows.length;
              if (streams.rows.length === 0) break;
              groupCursor = streams.nextSequenceExclusive;
              streamVersions = streams.nextStreamVersions;
            }
            creationCursor = creations.nextSequenceExclusive;
          }
          const groupCount = Math.ceil(threadCount / 32);
          const discovery = observations.filter(
            (entry) => entry.kind === "project-thread-creations",
          );
          const authority = observations.filter(
            (entry) => entry.kind === "thread-authority-streams",
          );
          assert.equal(discovery.length, groupCount + 1);
          assert.equal(authority.length, groupCount * 2);
          assert.equal(
            discovery.reduce((sum, entry) => sum + entry.rowCount, 0),
            threadCount,
          );
          assert.equal(
            authority.reduce((sum, entry) => sum + entry.rowCount, 0),
            threadCount,
          );
          assert.equal(reconstructedRows, threadCount);
        }
      }),
  );

  it.effect(
    "accepts only zero-or-one stream origins and exact progression across interleaving and pages",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const readStream = Effect.fn("readRawTestStream")(function* (streamId: string) {
          const versions: Array<number> = [];
          let sequenceExclusive = 0;
          let previousSequence = 0;
          let previousStreamVersion = 0;
          while (true) {
            const page = yield* loadOrchestrationEventStreamPage(sql, {
              aggregateKind: "thread",
              aggregateId: streamId,
              sequenceExclusive,
              previousSequence,
              previousStreamVersion,
              operationPrefix: "raw-stream-version-test",
            });
            versions.push(...page.rows.map((row) => row.streamVersion));
            if (page.rows.length === 0) return versions;
            sequenceExclusive = page.nextSequenceExclusive;
            previousSequence = page.nextSequenceExclusive;
            previousStreamVersion = page.nextStreamVersion;
          }
        });

        for (const [index, versions] of [[0], [0, 1, 2], [1], [1, 2, 3]].entries()) {
          yield* createTable(sql);
          const streamId = `valid-stream-${index}`;
          for (const [offset, streamVersion] of versions.entries()) {
            yield* insertEvent(sql, {
              sequence: offset + 1,
              streamVersion,
              streamId,
            });
          }
          assert.deepStrictEqual(yield* readStream(streamId), versions);
        }

        for (const [index, versions] of [[1, 0], [0, 0], [0, 2], [2]].entries()) {
          yield* createTable(sql);
          const streamId = `invalid-stream-${index}`;
          for (const [offset, streamVersion] of versions.entries()) {
            yield* insertEvent(sql, {
              sequence: offset + 1,
              streamVersion,
              streamId,
            });
          }
          yield* expectRawFailure(readStream(streamId), `invalid-stream-${versions.join("-")}`);
        }

        yield* createTable(sql);
        yield* insertEvent(sql, { sequence: 1, streamVersion: 0, streamId: "interleaved-a" });
        yield* insertEvent(sql, { sequence: 2, streamVersion: 1, streamId: "interleaved-b" });
        yield* insertEvent(sql, { sequence: 3, streamVersion: 1, streamId: "interleaved-a" });
        yield* insertEvent(sql, { sequence: 4, streamVersion: 2, streamId: "interleaved-b" });
        assert.deepStrictEqual(yield* readStream("interleaved-a"), [0, 1]);
        assert.deepStrictEqual(yield* readStream("interleaved-b"), [1, 2]);

        yield* createTable(sql);
        for (let sequence = 1; sequence <= 33; sequence += 1) {
          yield* insertEvent(sql, {
            sequence,
            streamVersion: sequence - 1,
            streamId: "paged-stream",
          });
        }
        assert.deepStrictEqual(
          yield* readStream("paged-stream"),
          Array.from({ length: 33 }, (_, index) => index),
        );
      }),
  );
});

it.live("rejects a byte-identical BLOB command sibling after a full WAL connection restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-raw-command-wal-"));
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      const scopeA = yield* Scope.make("sequential");
      const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
      const sqlA = Context.get(contextA, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sqlA`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      yield* createTable(sqlA);
      yield* insertEvent(sqlA, { sequence: 1 });
      yield* insertEvent(sqlA, { sequence: 2, commandId: Buffer.from(commandId) });
      yield* Scope.close(scopeA, Exit.void);

      const scopeB = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
      const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
      const sqlB = Context.get(contextB, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sqlB`PRAGMA journal_mode`, [{ journal_mode: "wal" }]);
      yield* expectRawFailure(loadAllCommandCandidates(sqlB, commandId), "wal-restart-blob");
    }),
  ),
);

it.live("keeps command-candidate predecessor validation across a WAL restart", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = NodeFS.mkdtempSync(
        NodePath.join(NodeOS.tmpdir(), "t3-raw-command-predecessor-wal-"),
      );
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      const scopeA = yield* Scope.make("sequential");
      const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
      const sqlA = Context.get(contextA, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sqlA`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
      yield* createTable(sqlA);
      yield* insertEvent(sqlA, { sequence: 1, streamVersion: 1, commandId: "other-command" });
      yield* insertEvent(sqlA, { sequence: 2, streamVersion: 2 });
      yield* Scope.close(scopeA, Exit.void);

      const scopeB = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
      const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
      const sqlB = Context.get(contextB, SqlClient.SqlClient);
      assert.deepStrictEqual(yield* sqlB`PRAGMA journal_mode`, [{ journal_mode: "wal" }]);
      assert.deepStrictEqual(
        (yield* loadAllCommandCandidates(sqlB, commandId)).map((row) => row.streamVersion),
        [2],
      );
    }),
  ),
);
