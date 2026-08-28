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
  loadOrchestrationEventsByCommandIdPage,
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
  return sql`
    INSERT INTO main.orchestration_events (
      sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
      occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
      payload_json, metadata_json
    ) VALUES (
      ${sequence}, ${input.streamVersion ?? sequence},
      ${input.eventId ?? `raw-authority-event-${sequence}`},
      ${input.aggregateKind ?? "thread"}, ${input.streamId ?? threadId},
      ${input.eventType ?? "thread.message-sent"}, ${input.occurredAt ?? at},
      ${input.commandId ?? commandId}, ${input.causationEventId ?? "raw-causation-event"},
      ${input.correlationId ?? commandId}, ${input.actorKind ?? "provider"},
      ${input.payload ?? payload}, ${input.metadata ?? metadata}
    )
  `;
};

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
) {
  const rows = [];
  let cursor = 0;
  while (true) {
    const page = yield* loadOrchestrationEventsByCommandIdPage(sql, {
      commandId: expectedCommandId,
      sequenceExclusive: cursor,
      operationPrefix: "raw-command-candidate-test",
    });
    rows.push(...page.rows);
    if (page.rows.length === 0) return rows;
    cursor = page.nextSequenceExclusive;
  }
});

layer("raw orchestration event authority", (it) => {
  it.effect("accepts today's canonical form and the exact historical four-field form", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* createTable(sql);
      yield* insertEvent(sql);
      const current = yield* readSequence(sql);
      assert.equal(current?.event.eventId, "raw-authority-event-1");
      assert.equal(current?.payloadSource, payload);
      assert.equal(current?.metadataSource, metadata);

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
        '{"providerRuntimeMessage":{"runtimeEventId":"event-historical","eventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical","providerItemId":null}}',
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
