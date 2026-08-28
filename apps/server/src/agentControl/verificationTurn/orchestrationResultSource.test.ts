// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import {
  CommandId,
  EventId,
  MessageId,
  type OrchestrationEvent,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type VerificationResultFragment,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { canonicalJson, sha256Utf8, type JsonValue } from "../initialPlanning/eventEvidence.ts";
import {
  AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
  loadOpenVerificationResultMessageIds,
  loadSealableVerificationResultSource,
  loadVerificationResultCapturedMessage,
  loadVerificationResultSealSummary,
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

const captureProgress = new Map<
  string,
  {
    readonly outputByteLength: number;
    readonly storedByteLength: number;
    readonly fragmentOrdinal: number;
    readonly cumulativeEvidenceDigest: string;
  }
>();

const captureEvent = (input: {
  readonly streamVersion: number;
  readonly messageId: string;
  readonly fragment:
    | VerificationResultFragment
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
  const previous = captureProgress.get(input.messageId) ?? null;
  const fragment: VerificationResultFragment =
    input.fragment.kind === "delta"
      ? "textPrefix" in input.fragment
        ? input.fragment
        : makeBoundedVerificationResultDelta(input.fragment.text, previous)
      : "completionTextPrefix" in input.fragment
        ? input.fragment
        : makeBoundedVerificationResultCompletion(input.fragment.completionText, previous);
  captureProgress.set(input.messageId, {
    outputByteLength:
      fragment.kind === "delta" ? fragment.cumulativeSourceByteLength : fragment.outputByteLength,
    storedByteLength:
      (previous?.storedByteLength ?? 0) +
      (fragment.kind === "delta"
        ? fragment.prefixByteLength
        : Buffer.byteLength(fragment.completionTextPrefix ?? "", "utf8")),
    fragmentOrdinal: fragment.fragmentOrdinal,
    cumulativeEvidenceDigest: fragment.cumulativeEvidenceDigest,
  });
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
        eventType: fragment.kind === "delta" ? "content.delta" : "item.completed",
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
      fragment,
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
            eventType: input.streaming ? "content.delta" : "item.completed",
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

it.live(
  "decodes canonical seal authority from raw MAIN rows and fails closed across WAL restart corruption",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = NodeFS.mkdtempSync(
          NodePath.join(NodeOS.tmpdir(), "t3-verification-seal-raw-"),
        );
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
        const sqlA = Context.get(contextA, SqlClient.SqlClient);
        assert.deepStrictEqual(yield* sqlA`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
        yield* sqlA`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sqlA`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        yield* sqlA`
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
        const canonicalBootstrapPayload = canonicalJson({
          threadId,
          messageId: "bootstrap",
          role: "user",
          text: "bootstrap",
          turnId: null,
          streaming: false,
          createdAt: at,
          updatedAt: at,
        });
        for (let sequence = 1; sequence <= 4; sequence += 1) {
          yield* sqlA`
            INSERT INTO main.orchestration_events (
              sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
              occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
              payload_json, metadata_json
            ) VALUES (
              ${sequence}, ${sequence}, ${`bootstrap-${sequence}`}, 'thread', ${threadId},
              'thread.message-sent', ${at}, ${`bootstrap-command-${sequence}`}, NULL,
              ${`bootstrap-command-${sequence}`}, 'client', ${canonicalBootstrapPayload}, '{}'
            )
          `;
        }
        const lifecycle = {
          runtimeEventId: "runtime-seal-completed",
          runtimeEventType: "turn.completed",
          providerInstanceId,
          providerTurnId,
          providerState: "completed",
        } as const;
        const seal = {
          schemaVersion: 1,
          handoffId: captureAuthority.handoffId,
          providerDeliveryId: captureAuthority.providerDeliveryId,
          providerInstanceId,
          providerTurnId,
          resultSchemaFingerprint: captureAuthority.resultSchemaFingerprint,
          sourceDisposition: "missing",
          finalMessageId: null,
          sourceEventId: null,
          outputDigest: null,
          outputByteLength: 0,
        } as const;
        const canonicalPayload = canonicalJson({
          threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "codex",
            providerInstanceId,
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: at,
          },
        });
        const canonicalMetadata = canonicalJson({
          providerRuntimeLifecycle: lifecycle,
          verificationResultSource: seal,
        });
        const sealCommandId = `provider:${lifecycle.runtimeEventId}:thread-session-set:seal`;
        yield* sqlA`
          INSERT INTO main.orchestration_events (
            sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
            occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
            payload_json, metadata_json
          ) VALUES (
            5, 5, 'seal-event', 'thread', ${threadId}, 'thread.session-set', ${at},
            ${sealCommandId}, NULL, ${sealCommandId}, 'provider',
            ${canonicalPayload}, ${canonicalMetadata}
          )
        `;
        const identity = {
          handoffId: seal.handoffId,
          providerDeliveryId: seal.providerDeliveryId,
          providerInstanceId: seal.providerInstanceId,
          providerTurnId: seal.providerTurnId,
          resultSchemaFingerprint: seal.resultSchemaFingerprint,
        };
        const valid = yield* loadVerificationResultSealSummary(sqlA, {
          threadId,
          matchingIdentity: identity,
        });
        assert.equal(valid.sealCount, 1);
        assert.equal(valid.matchingSealCount, 1);
        assert.equal(valid.firstSeal?.event.eventId, "seal-event");
        const foreign = yield* loadVerificationResultSealSummary(sqlA, {
          threadId,
          matchingIdentity: { ...identity, providerTurnId: "foreign-turn" },
        });
        assert.equal(foreign.sealCount, 1);
        assert.equal(foreign.matchingSealCount, 0);

        const noncanonicalMetadata = `{"verificationResultSource":${canonicalJson(
          seal,
        )},"providerRuntimeLifecycle":${canonicalJson(lifecycle)}}`;
        const duplicateMetadata = `{"providerRuntimeLifecycle":${canonicalJson(
          lifecycle,
        )},"verificationResultSource":${canonicalJson(
          seal,
        )},"verificationResultSource":${canonicalJson(seal)}}`;
        const variants: ReadonlyArray<{
          readonly name: string;
          readonly mutate: Effect.Effect<unknown, SqlError>;
        }> = [
          {
            name: "noncanonical-key-order",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=${noncanonicalMetadata} WHERE sequence=5`,
          },
          {
            name: "duplicate-keys",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=${duplicateMetadata} WHERE sequence=5`,
          },
          {
            name: "extra-metadata-field",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=${canonicalJson({
              providerRuntimeLifecycle: lifecycle,
              verificationResultSource: seal,
              unexpected: true,
            })} WHERE sequence=5`,
          },
          {
            name: "extra-seal-field",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=${canonicalJson({
              providerRuntimeLifecycle: lifecycle,
              verificationResultSource: { ...seal, unexpected: true },
            })} WHERE sequence=5`,
          },
          {
            name: "wrong-storage-class",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=CAST(metadata_json AS BLOB) WHERE sequence=5`,
          },
          {
            name: "invalid-utf8-text-blob",
            mutate: sqlA`UPDATE main.orchestration_events SET metadata_json=CAST(X'80' AS TEXT) WHERE sequence=5`,
          },
          {
            name: "matching-json-subset-invalid-event",
            mutate: sqlA`UPDATE main.orchestration_events SET payload_json=${canonicalJson({
              threadId,
              session: {
                threadId,
                status: "ready",
                providerName: "codex",
                providerInstanceId,
                runtimeMode: "full-access",
                activeTurnId: null,
                lastError: null,
                updatedAt: at,
              },
              unexpected: true,
            })} WHERE sequence=5`,
          },
          {
            name: "wrong-envelope-storage",
            mutate: sqlA`UPDATE main.orchestration_events SET actor_kind=CAST(actor_kind AS BLOB) WHERE sequence=5`,
          },
        ];
        for (const variant of variants) {
          yield* variant.mutate;
          const before = yield* sqlA<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.orchestration_events
          `;
          const failure = yield* Effect.flip(
            loadVerificationResultSealSummary(sqlA, { threadId, matchingIdentity: identity }),
          );
          assert.isTrue(isVerificationResultHistoryError(failure), variant.name);
          assert.deepStrictEqual(
            yield* sqlA`SELECT count(*) AS count FROM main.orchestration_events`,
            before,
            variant.name,
          );
          yield* sqlA`
            UPDATE main.orchestration_events
            SET payload_json=${canonicalPayload}, metadata_json=${canonicalMetadata},
              actor_kind='provider'
            WHERE sequence=5
          `;
        }

        yield* Scope.close(scopeA, Exit.void);
        const scopeB = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
        const sqlB = Context.get(contextB, SqlClient.SqlClient);
        assert.deepStrictEqual(yield* sqlB`PRAGMA journal_mode`, [{ journal_mode: "wal" }]);
        yield* sqlB`PRAGMA foreign_keys = ON`;
        const restarted = yield* loadVerificationResultSealSummary(sqlB, {
          threadId,
          matchingIdentity: identity,
        });
        assert.equal(restarted.sealCount, 1);
        assert.equal(restarted.matchingSealCount, 1);
      }),
    ),
);

const initialize = Effect.fn("initializeVerificationResultSourceTest")(function* () {
  captureProgress.clear();
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
    assert.equal(bounded.fullTextByteLength, Buffer.byteLength(rawDelta));
    assert.equal(
      bounded.cumulativeSourceByteLength,
      AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
    );
    assert.isAtMost(Buffer.byteLength(bounded.textPrefix), 64 * 1024);
    assert.isBelow(bounded.textPrefix.length, rawDelta.length);
  });

  it("bounds completion-only fallback text without duplicating an existing durable delta", () => {
    const rawCompletion = "🙂".repeat(256 * 1024);
    const completionOnly = makeBoundedVerificationResultCompletion(rawCompletion, null);
    assert.equal(completionOnly.kind, "completion");
    assert.equal(
      completionOnly.outputByteLength,
      AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
    );
    assert.isString(completionOnly.completionTextPrefix);
    assert.isAtMost(Buffer.byteLength(completionOnly.completionTextPrefix!, "utf8"), 64 * 1024);

    const previous = makeBoundedVerificationResultDelta("already", null);
    const afterDelta = makeBoundedVerificationResultCompletion("must not be duplicated", {
      outputByteLength: previous.cumulativeSourceByteLength,
      storedByteLength: previous.prefixByteLength,
      fragmentOrdinal: previous.fragmentOrdinal,
      cumulativeEvidenceDigest: previous.cumulativeEvidenceDigest,
    });
    assert.equal(afterDelta.completionTextPrefix, null);
    assert.equal(afterDelta.outputByteLength, 7);
    assert.deepStrictEqual(afterDelta.completionDetail.present, true);
  });

  it("binds complete delta and completion evidence beyond the bounded source prefix", () => {
    const prefix = "x".repeat(64 * 1024);
    const deltaA = makeBoundedVerificationResultDelta(`${prefix}suffix-a`, null);
    const deltaB = makeBoundedVerificationResultDelta(`${prefix}suffix-b`, null);
    assert.equal(deltaA.textPrefix, deltaB.textPrefix);
    assert.equal(deltaA.prefixByteLength, 64 * 1024);
    assert.equal(deltaA.fullTextByteLength, deltaB.fullTextByteLength);
    assert.notEqual(deltaA.fullTextDigest, deltaB.fullTextDigest);
    assert.notEqual(deltaA.cumulativeEvidenceDigest, deltaB.cumulativeEvidenceDigest);

    const completionA = makeBoundedVerificationResultCompletion(`${prefix}suffix-a`, null);
    const completionB = makeBoundedVerificationResultCompletion(`${prefix}suffix-b`, null);
    assert.equal(completionA.completionTextPrefix, completionB.completionTextPrefix);
    assert.notDeepEqual(completionA.completionDetail, completionB.completionDetail);
    assert.notEqual(completionA.cumulativeEvidenceDigest, completionB.cumulativeEvidenceDigest);

    const longer = makeBoundedVerificationResultDelta(`${prefix}suffix-longer`, null);
    assert.equal(deltaA.textPrefix, longer.textPrefix);
    assert.notEqual(deltaA.fullTextByteLength, longer.fullTextByteLength);
  });

  it("keeps completion detail presence and full UTF-8 identity after deltas", () => {
    const delta = makeBoundedVerificationResultDelta("source-from-delta", null);
    const previous = {
      outputByteLength: delta.cumulativeSourceByteLength,
      storedByteLength: delta.prefixByteLength,
      fragmentOrdinal: delta.fragmentOrdinal,
      cumulativeEvidenceDigest: delta.cumulativeEvidenceDigest,
    } as const;
    const absent = makeBoundedVerificationResultCompletion(null, previous);
    const empty = makeBoundedVerificationResultCompletion("", previous);
    const detailA = makeBoundedVerificationResultCompletion("🙂", previous);
    const detailB = makeBoundedVerificationResultCompletion("🚀", previous);
    const detailAReplay = makeBoundedVerificationResultCompletion("🙂", previous);

    assert.equal(absent.completionTextPrefix, null);
    assert.deepStrictEqual(absent.completionDetail, { present: false });
    assert.equal(empty.completionTextPrefix, null);
    assert.equal(empty.completionDetail.present, true);
    if (empty.completionDetail.present) {
      assert.equal(empty.completionDetail.fullByteLength, 0);
      assert.match(empty.completionDetail.fullDigest, /^[0-9a-f]{64}$/u);
    }
    assert.notEqual(absent.cumulativeEvidenceDigest, empty.cumulativeEvidenceDigest);
    assert.deepStrictEqual(detailA, detailAReplay);
    assert.equal(detailA.completionDetail.present, true);
    assert.equal(detailB.completionDetail.present, true);
    if (detailA.completionDetail.present && detailB.completionDetail.present) {
      assert.equal(detailA.completionDetail.fullByteLength, 4);
      assert.equal(detailB.completionDetail.fullByteLength, 4);
      assert.notEqual(detailA.completionDetail.fullDigest, detailB.completionDetail.fullDigest);
    }
    assert.notEqual(detailA.cumulativeEvidenceDigest, detailB.cumulativeEvidenceDigest);
  });

  it("continues divergent oversize evidence across later equal fragments", () => {
    const prefix = "x".repeat(64 * 1024);
    const earlyA = makeBoundedVerificationResultDelta(`${prefix}A`, null);
    const earlyB = makeBoundedVerificationResultDelta(`${prefix}B`, null);
    const next = (previous: typeof earlyA) =>
      makeBoundedVerificationResultDelta("same-later-fragment", {
        outputByteLength: previous.cumulativeSourceByteLength,
        storedByteLength: previous.prefixByteLength,
        fragmentOrdinal: previous.fragmentOrdinal,
        cumulativeEvidenceDigest: previous.cumulativeEvidenceDigest,
      });
    const laterA = next(earlyA);
    const laterB = next(earlyB);
    assert.equal(laterA.textPrefix, "");
    assert.equal(laterB.textPrefix, "");
    assert.equal(laterA.fullTextDigest, laterB.fullTextDigest);
    assert.notEqual(laterA.cumulativeEvidenceDigest, laterB.cumulativeEvidenceDigest);
  });

  it("cuts bounded UTF-8 prefixes only at code-point boundaries", () => {
    const exactly = makeBoundedVerificationResultDelta(`${"x".repeat(64 * 1024 - 4)}🙂`, null);
    assert.equal(exactly.prefixByteLength, 64 * 1024);
    assert.equal(exactly.textPrefix.at(-2), "\ud83d");
    assert.equal(exactly.textPrefix.at(-1), "\ude42");

    const crossing = makeBoundedVerificationResultDelta(`${"x".repeat(64 * 1024 - 1)}🙂tail`, null);
    assert.equal(crossing.prefixByteLength, 64 * 1024 - 1);
    assert.equal(crossing.textPrefix, "x".repeat(64 * 1024 - 1));
    assert.notInclude(crossing.textPrefix, "�");
  });

  it.effect("persists only the bounded prefix while retaining restartable full evidence", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const raw = `${"x".repeat(64 * 1024)}UNIQUE_UNSTORED_SUFFIX`;
      const fragment = makeBoundedVerificationResultDelta(raw, null);
      yield* insert(
        captureEvent({
          streamVersion: 5,
          messageId: "restartable-oversize",
          fragment,
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
      const persisted = yield* loadVerificationResultCapturedMessage(
        sql,
        identity,
        MessageId.make("restartable-oversize"),
      );
      assert.equal(persisted?.storedByteLength, 64 * 1024);
      assert.equal(
        persisted?.outputByteLength,
        AGENT_CONTROL_VERIFICATION_RESULT_OVERSIZE_SENTINEL,
      );
      assert.equal(persisted?.fragmentOrdinal, fragment.fragmentOrdinal);
      assert.equal(persisted?.cumulativeEvidenceDigest, fragment.cumulativeEvidenceDigest);
      const [rawRow] = yield* sql<{ readonly payload: string }>`
        SELECT payload_json AS payload FROM orchestration_events WHERE sequence=5
      `;
      assert.notInclude(rawRow!.payload, "UNIQUE_UNSTORED_SUFFIX");
      assert.include(rawRow!.payload, fragment.fullTextDigest);

      const afterRestart = makeBoundedVerificationResultDelta("same-later-fragment", {
        outputByteLength: persisted!.outputByteLength,
        storedByteLength: persisted!.storedByteLength,
        fragmentOrdinal: persisted!.fragmentOrdinal,
        cumulativeEvidenceDigest: persisted!.cumulativeEvidenceDigest,
      });
      assert.equal(afterRestart.fragmentOrdinal, 2);
      assert.notEqual(afterRestart.cumulativeEvidenceDigest, fragment.cumulativeEvidenceDigest);
    }),
  );

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

  it.effect("reads a historical runtimeEventType row before current capture authority", () =>
    Effect.gen(function* () {
      const { sql, insert } = yield* initialize();
      const legacyCommandId =
        "provider:event-historical:message-complete:assistant:legacy-result-source";
      yield* sql`
        INSERT INTO orchestration_events (
          sequence, stream_version, event_id, aggregate_kind, stream_id, event_type,
          occurred_at, command_id, causation_event_id, correlation_id, actor_kind,
          payload_json, metadata_json
        ) VALUES (
          5, 5, 'legacy-result-source-row', 'thread', ${threadId}, 'thread.message-sent',
          ${at}, ${legacyCommandId}, NULL, ${legacyCommandId}, 'provider',
          ${canonicalJson({
            threadId,
            messageId: "assistant:legacy-result-source",
            role: "assistant",
            text: "historical presentation",
            turnId: "turn-historical",
            streaming: false,
            createdAt: at,
            updatedAt: at,
          })},
          ${'{"providerRuntimeMessage":{"runtimeEventId":"event-historical","runtimeEventType":"item.completed","providerInstanceId":"codex","providerTurnId":"turn-historical"}}'}
        )
      `;
      const text = canonicalJson({
        report: "Current capture after historical row.",
        schemaVersion: "agent-control-verification-result-v1",
        verdict: "passed",
      });
      yield* insert(
        captureEvent({
          streamVersion: 6,
          messageId: "current-completion",
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
      assert.equal(source.finalMessageId, "current-completion");
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
      const captured = yield* loadVerificationResultCapturedMessage(
        sql,
        identity,
        MessageId.make("message-b"),
      );
      assert.equal(captured?.text, "");
      assert.equal(captured?.completed, true);
      assert.equal(captured?.outputByteLength, 0);
      assert.equal(captured?.storedByteLength, 0);
      assert.equal(captured?.fragmentOrdinal, 1);
      assert.match(captured!.cumulativeEvidenceDigest, /^[0-9a-f]{64}$/u);
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
          fragment: { kind: "delta", text: "x", byteLength: 1, cumulativeByteLength: 65537 },
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
        const text = "x".repeat(byteLength);
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
