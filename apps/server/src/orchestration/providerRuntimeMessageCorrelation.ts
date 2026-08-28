import {
  OrchestrationEventMetadata,
  ProviderRuntimeMessageCorrelation,
  type OrchestrationEventMetadata as OrchestrationEventMetadataType,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  type JsonValue,
} from "../agentControl/initialPlanning/eventEvidence.ts";

const LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS = [
  "providerInstanceId",
  "providerTurnId",
  "runtimeEventId",
  "runtimeEventType",
] as const;

const LegacyProviderRuntimeMessageCorrelation = Schema.Struct({
  runtimeEventId: ProviderRuntimeMessageCorrelation.fields.runtimeEventId,
  runtimeEventType: ProviderRuntimeMessageCorrelation.fields.eventType,
  providerInstanceId: ProviderRuntimeMessageCorrelation.fields.providerInstanceId,
  providerTurnId: ProviderRuntimeMessageCorrelation.fields.providerTurnId,
}).annotate({ parseOptions: { onExcessProperty: "error" } });

const decodeLegacyCorrelation = Schema.decodeUnknownSync(LegacyProviderRuntimeMessageCorrelation);
const ClosedOrchestrationEventMetadata = Schema.Struct({
  ...OrchestrationEventMetadata.fields,
}).annotate({ parseOptions: { onExcessProperty: "error" } });
const decodeClosedMetadata = Schema.decodeUnknownSync(ClosedOrchestrationEventMetadata);
const encodeClosedMetadata = Schema.encodeUnknownSync(ClosedOrchestrationEventMetadata);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasExactKeys = (value: Record<string, unknown>, expected: ReadonlyArray<string>): boolean => {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
};

const containsNul = (value: unknown): boolean => {
  if (typeof value === "string") return value.includes("\0");
  if (Array.isArray(value)) return value.some(containsNul);
  if (!isRecord(value)) return false;
  return Object.entries(value).some(([key, child]) => key.includes("\0") || containsNul(child));
};

/** Decode only the four fields emitted by the historical production encoder. */
const decodeLegacyProviderRuntimeMessageCorrelation = (
  input: unknown,
): ProviderRuntimeMessageCorrelation => {
  const legacy = decodeLegacyCorrelation(input);
  return {
    runtimeEventId: legacy.runtimeEventId,
    eventType: legacy.runtimeEventType,
    providerInstanceId: legacy.providerInstanceId,
    providerTurnId: legacy.providerTurnId,
    providerItemId: null,
  };
};

const encodeHistoricalProviderRuntimeMessageMetadata = (
  legacy: typeof LegacyProviderRuntimeMessageCorrelation.Type,
): string =>
  JSON.stringify({
    providerRuntimeMessage: {
      runtimeEventId: legacy.runtimeEventId,
      runtimeEventType: legacy.runtimeEventType,
      providerInstanceId: legacy.providerInstanceId,
      providerTurnId: legacy.providerTurnId,
    },
  });

/**
 * Decode orchestration metadata at its immutable storage boundary.
 *
 * Current metadata must retain the canonical JSON encoding. The sole
 * historical exception is the exact object emitted by the former assistant
 * message encoder: one top-level `providerRuntimeMessage` field and the four
 * nested fields in their production insertion order. Strict parsing happens
 * first so duplicate keys can never collapse before this distinction.
 */
const decodeCanonicalOrLegacyOrchestrationMetadata = (
  source: string,
): OrchestrationEventMetadataType => {
  const parsed = parseJsonStrict(source);
  if (containsNul(parsed)) {
    throw new Error("NUL is not valid in persisted orchestration metadata");
  }
  if (isRecord(parsed)) {
    const runtime = parsed.providerRuntimeMessage;
    if (
      hasExactKeys(parsed, ["providerRuntimeMessage"]) &&
      isRecord(runtime) &&
      hasExactKeys(runtime, LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS)
    ) {
      const legacy = decodeLegacyCorrelation(runtime);
      if (encodeHistoricalProviderRuntimeMessageMetadata(legacy) !== source) {
        throw new Error("Invalid historical orchestration metadata encoding");
      }
      return decodeClosedMetadata({
        providerRuntimeMessage: decodeLegacyProviderRuntimeMessageCorrelation(legacy),
      });
    }
  }

  if (canonicalJson(parsed) !== source) {
    throw new Error("Invalid canonical orchestration metadata encoding");
  }
  const decoded = decodeClosedMetadata(parsed);
  const encoded = encodeClosedMetadata(decoded);
  if (canonicalJson(encoded as JsonValue) !== source) {
    throw new Error("Persisted orchestration metadata was transformed by schema decoding");
  }
  return decoded;
};

export interface PersistedOrchestrationMetadata {
  readonly storageClass: unknown;
  readonly bytes: unknown;
  readonly text: unknown;
}

export interface DecodedPersistedOrchestrationMetadata {
  readonly source: string;
  readonly value: OrchestrationEventMetadataType;
}

/**
 * The single authority boundary for metadata read from orchestration storage.
 * It validates SQLite's storage class and original bytes before admitting the
 * exact historical encoder output or today's canonical closed schema. Legacy
 * bytes are preserved in SQLite and normalized only in the returned value.
 */
export const decodePersistedOrchestrationMetadata = (
  input: PersistedOrchestrationMetadata,
): DecodedPersistedOrchestrationMetadata => {
  if (input.storageClass !== "text" || typeof input.text !== "string") {
    throw new Error("Invalid orchestration metadata SQLite storage class");
  }
  const source = decodeCanonicalUtf8Bytes(input.bytes);
  if (source !== input.text) {
    throw new Error("Orchestration metadata TEXT/BLOB mismatch");
  }
  return {
    source,
    value: decodeCanonicalOrLegacyOrchestrationMetadata(source),
  };
};
