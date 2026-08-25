import * as NodeCrypto from "node:crypto";

import type { CommandId, EventId, MessageId, ModelSelection, ThreadId } from "@t3tools/contracts";

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | ReadonlyArray<JsonValue>
  | { readonly [key: string]: JsonValue };

export interface InitialPlanningEventEnvelope {
  readonly sequence: number | null;
  readonly streamVersion: number;
  readonly eventId: EventId;
  readonly aggregateKind: "thread";
  readonly aggregateId: ThreadId;
  readonly type: "thread.message-sent" | "thread.turn-start-requested";
  readonly occurredAt: string;
  readonly commandId: CommandId;
  readonly causationEventId: EventId | null;
  readonly correlationId: CommandId;
  readonly actorKind: "client";
  readonly payload: JsonValue;
  readonly metadata: { readonly [key: string]: JsonValue };
}

const failJson = (issue: string): never => {
  throw new Error(`Invalid canonical JSON: ${issue}`);
};

/**
 * Parses JSON without losing duplicate object keys. JSON.parse alone cannot be
 * used for immutable evidence because it silently keeps only the last value.
 */
export const parseJsonStrict = (source: string): JsonValue => {
  let offset = 0;
  const whitespace = () => {
    while (offset < source.length) {
      const code = source.charCodeAt(offset);
      if (code !== 0x20 && code !== 0x09 && code !== 0x0a && code !== 0x0d) break;
      offset += 1;
    }
  };
  const string = (): string => {
    const start = offset;
    if (source[offset] !== '"') return failJson(`expected string at byte ${offset}`);
    offset += 1;
    while (offset < source.length) {
      const current = source[offset]!;
      if (current === '"') {
        offset += 1;
        try {
          return JSON.parse(source.slice(start, offset)) as string;
        } catch {
          return failJson(`invalid string at byte ${start}`);
        }
      }
      if (current === "\\") {
        offset += 2;
        continue;
      }
      if (current.charCodeAt(0) < 0x20) {
        return failJson(`unescaped control character at byte ${offset}`);
      }
      offset += 1;
    }
    return failJson(`unterminated string at byte ${start}`);
  };
  const value = (): JsonValue => {
    whitespace();
    const current = source[offset];
    if (current === '"') return string();
    if (current === "{") {
      offset += 1;
      whitespace();
      const result: Record<string, JsonValue> = Object.create(null);
      const keys = new Set<string>();
      if (source[offset] === "}") {
        offset += 1;
        return result;
      }
      while (offset < source.length) {
        whitespace();
        const key = string();
        if (keys.has(key)) return failJson(`duplicate object key '${key}'`);
        keys.add(key);
        whitespace();
        if (source[offset] !== ":") return failJson(`expected ':' at byte ${offset}`);
        offset += 1;
        result[key] = value();
        whitespace();
        if (source[offset] === "}") {
          offset += 1;
          return result;
        }
        if (source[offset] !== ",") return failJson(`expected ',' at byte ${offset}`);
        offset += 1;
      }
      return failJson("unterminated object");
    }
    if (current === "[") {
      offset += 1;
      whitespace();
      const result: JsonValue[] = [];
      if (source[offset] === "]") {
        offset += 1;
        return result;
      }
      while (offset < source.length) {
        result.push(value());
        whitespace();
        if (source[offset] === "]") {
          offset += 1;
          return result;
        }
        if (source[offset] !== ",") return failJson(`expected ',' at byte ${offset}`);
        offset += 1;
      }
      return failJson("unterminated array");
    }
    for (const [token, parsed] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const) {
      if (source.startsWith(token, offset)) {
        offset += token.length;
        return parsed;
      }
    }
    const number = source
      .slice(offset)
      .match(/^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/u)?.[0];
    if (number !== undefined) {
      offset += number.length;
      const parsed = Number(number);
      if (!Number.isFinite(parsed)) return failJson(`non-finite number at byte ${offset}`);
      return parsed;
    }
    return failJson(`unexpected token at byte ${offset}`);
  };
  const parsed = value();
  whitespace();
  if (offset !== source.length) failJson(`trailing content at byte ${offset}`);
  return parsed;
};

export const canonicalJson = (value: JsonValue): string => {
  if (value === null || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return failJson("non-finite number");
    return JSON.stringify(value);
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const entries = Object.entries(value).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );
  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
};

export const parseCanonicalJson = (source: string): JsonValue => {
  const parsed = parseJsonStrict(source);
  if (canonicalJson(parsed) !== source) failJson("noncanonical encoding");
  return parsed;
};

export const parseCanonicalJsonObject = (
  source: string,
  expectedKeys: ReadonlyArray<string>,
): { readonly [key: string]: JsonValue } => {
  const parsed = parseCanonicalJson(source);
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return failJson("expected object root");
  }
  const actualKeys = Object.keys(parsed).sort();
  const canonicalExpectedKeys = [...expectedKeys].sort();
  if (
    actualKeys.length !== canonicalExpectedKeys.length ||
    actualKeys.some((key, index) => key !== canonicalExpectedKeys[index])
  ) {
    return failJson("unexpected object keys");
  }
  return parsed as { readonly [key: string]: JsonValue };
};

const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true });
const utf8Encoder = new TextEncoder();

const bytesEqual = (left: Uint8Array, right: Uint8Array): boolean =>
  left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);

/**
 * Decodes the bytes SQLite actually stores for a TEXT value. The fatal decode
 * and byte-for-byte roundtrip intentionally happen before JSON parsing so a
 * driver's replacement decoding can never become immutable replay evidence.
 */
export const decodeCanonicalUtf8Bytes = (raw: unknown): string => {
  if (!(raw instanceof Uint8Array)) {
    return failJson("expected SQLite BLOB bytes");
  }
  let decoded: string;
  try {
    decoded = fatalUtf8Decoder.decode(raw);
  } catch {
    return failJson("invalid UTF-8 bytes");
  }
  const reencoded = utf8Encoder.encode(decoded);
  if (!bytesEqual(raw, reencoded)) {
    return failJson("UTF-8 roundtrip changed stored bytes");
  }
  return decoded;
};

export const parseCanonicalJsonObjectBytes = (
  raw: unknown,
  expectedKeys: ReadonlyArray<string>,
): {
  readonly source: string;
  readonly value: { readonly [key: string]: JsonValue };
  readonly bytes: Uint8Array;
} => {
  const source = decodeCanonicalUtf8Bytes(raw);
  return {
    source,
    value: parseCanonicalJsonObject(source, expectedKeys),
    bytes: raw as Uint8Array,
  };
};

export const sha256Utf8 = (source: string): string =>
  NodeCrypto.createHash("sha256").update(source, "utf8").digest("hex");

export const sha256Bytes = (source: Uint8Array): string =>
  NodeCrypto.createHash("sha256").update(source).digest("hex");

export const canonicalInitialPlanningEventEnvelope = (
  envelope: InitialPlanningEventEnvelope,
): string =>
  canonicalJson({
    actorKind: envelope.actorKind,
    aggregateId: envelope.aggregateId,
    aggregateKind: envelope.aggregateKind,
    causationEventId: envelope.causationEventId,
    commandId: envelope.commandId,
    correlationId: envelope.correlationId,
    eventId: envelope.eventId,
    metadata: envelope.metadata,
    occurredAt: envelope.occurredAt,
    payload: envelope.payload,
    sequence: envelope.sequence,
    streamVersion: envelope.streamVersion,
    type: envelope.type,
  });

export const canonicalInitialPlanningEventEnvelopeFromStoredJson = (
  envelope: Omit<InitialPlanningEventEnvelope, "payload" | "metadata"> & {
    readonly payloadJson: string;
    readonly metadataJson: string;
  },
): string => {
  // Both raw values are validated before they are embedded. Keeping the
  // already-confirmed bytes here makes the digest bind the SQLite strings
  // themselves instead of a normalized reconstruction.
  parseCanonicalJson(envelope.payloadJson);
  parseCanonicalJson(envelope.metadataJson);
  return [
    '{"actorKind":',
    canonicalJson(envelope.actorKind),
    ',"aggregateId":',
    canonicalJson(envelope.aggregateId),
    ',"aggregateKind":',
    canonicalJson(envelope.aggregateKind),
    ',"causationEventId":',
    canonicalJson(envelope.causationEventId),
    ',"commandId":',
    canonicalJson(envelope.commandId),
    ',"correlationId":',
    canonicalJson(envelope.correlationId),
    ',"eventId":',
    canonicalJson(envelope.eventId),
    ',"metadata":',
    envelope.metadataJson,
    ',"occurredAt":',
    canonicalJson(envelope.occurredAt),
    ',"payload":',
    envelope.payloadJson,
    ',"sequence":',
    canonicalJson(envelope.sequence),
    ',"streamVersion":',
    canonicalJson(envelope.streamVersion),
    ',"type":',
    canonicalJson(envelope.type),
    "}",
  ].join("");
};

export const canonicalInitialPlanningEventTemplate = (
  envelope: Omit<InitialPlanningEventEnvelope, "sequence">,
): string => canonicalInitialPlanningEventEnvelope({ ...envelope, sequence: null });

export const combinedInitialPlanningEventDigest = (
  messageEnvelopeJson: string,
  turnRequestEnvelopeJson: string,
): string =>
  sha256Utf8(
    canonicalJson([
      parseCanonicalJson(messageEnvelopeJson),
      parseCanonicalJson(turnRequestEnvelopeJson),
    ]),
  );

export const initialPlanningMessagePayload = (input: {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly promptText: string;
  readonly createdAt: string;
}): JsonValue => ({
  attachments: [],
  createdAt: input.createdAt,
  messageId: input.messageId,
  role: "user",
  streaming: false,
  text: input.promptText,
  threadId: input.threadId,
  turnId: null,
  updatedAt: input.createdAt,
});

export const initialPlanningTurnRequestPayload = (input: {
  readonly threadId: ThreadId;
  readonly messageId: MessageId;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: "approval-required" | "full-access";
  readonly createdAt: string;
}): JsonValue => ({
  createdAt: input.createdAt,
  interactionMode: "plan",
  messageId: input.messageId,
  modelSelection: input.modelSelection as JsonValue,
  runtimeMode: input.runtimeMode,
  threadId: input.threadId,
});
