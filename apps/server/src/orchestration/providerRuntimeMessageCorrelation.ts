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

const LEGACY_PROVIDER_RUNTIME_MESSAGE_WITH_ITEM_KEYS = [
  "providerInstanceId",
  "providerItemId",
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

const LegacyProviderRuntimeMessageCorrelationWithItem = Schema.Struct({
  runtimeEventId: ProviderRuntimeMessageCorrelation.fields.runtimeEventId,
  runtimeEventType: ProviderRuntimeMessageCorrelation.fields.eventType,
  providerInstanceId: ProviderRuntimeMessageCorrelation.fields.providerInstanceId,
  providerTurnId: ProviderRuntimeMessageCorrelation.fields.providerTurnId,
  providerItemId: ProviderRuntimeMessageCorrelation.fields.providerItemId,
}).annotate({ parseOptions: { onExcessProperty: "error" } });

const decodeLegacyCorrelation = Schema.decodeUnknownSync(LegacyProviderRuntimeMessageCorrelation);
const decodeLegacyCorrelationWithItem = Schema.decodeUnknownSync(
  LegacyProviderRuntimeMessageCorrelationWithItem,
);
const ClosedOrchestrationEventMetadata = Schema.Struct({
  ...OrchestrationEventMetadata.fields,
}).annotate({ parseOptions: { onExcessProperty: "error" } });
const decodeClosedMetadata = Schema.decodeUnknownSync(ClosedOrchestrationEventMetadata);
const encodeClosedMetadata = Schema.encodeUnknownSync(ClosedOrchestrationEventMetadata);

export const ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_V0 =
  "orchestration-metadata-legacy-runtime-v0";
export const ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_WITH_ITEM_V1 =
  "orchestration-metadata-legacy-runtime-with-item-v1";
export const ORCHESTRATION_METADATA_STORAGE_ENCODING_SCHEMA_ORDER_V1 =
  "orchestration-metadata-schema-order-v1";
export const ORCHESTRATION_METADATA_STORAGE_ENCODING_ALPHABETICAL_V1 =
  "orchestration-metadata-alphabetical-v1";
export type OrchestrationMetadataStorageEncoding =
  | typeof ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_V0
  | typeof ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_WITH_ITEM_V1
  | typeof ORCHESTRATION_METADATA_STORAGE_ENCODING_SCHEMA_ORDER_V1
  | typeof ORCHESTRATION_METADATA_STORAGE_ENCODING_ALPHABETICAL_V1;

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

const encodeHistoricalProviderRuntimeMessageMetadataWithItem = (
  legacy: typeof LegacyProviderRuntimeMessageCorrelationWithItem.Type,
): string =>
  JSON.stringify({
    providerRuntimeMessage: {
      runtimeEventId: legacy.runtimeEventId,
      runtimeEventType: legacy.runtimeEventType,
      providerInstanceId: legacy.providerInstanceId,
      providerTurnId: legacy.providerTurnId,
      providerItemId: legacy.providerItemId,
    },
  });

/**
 * Decode orchestration metadata at its immutable storage boundary.
 *
 * Current metadata must retain one of the two named encodings that this slice
 * could have persisted: schema order or alphabetical canonical order. The
 * older exception is the exact four-field object emitted by the former
 * assistant-message encoder. Strict parsing happens first so duplicate keys
 * can never collapse before this distinction.
 */
const decodeCanonicalOrLegacyOrchestrationMetadata = (
  source: string,
): {
  readonly value: OrchestrationEventMetadataType;
  readonly encoding: OrchestrationMetadataStorageEncoding;
} => {
  const parsed = parseJsonStrict(source);
  if (containsNul(parsed)) {
    throw new Error("NUL is not valid in persisted orchestration metadata");
  }
  if (isRecord(parsed)) {
    const runtime = parsed.providerRuntimeMessage;
    if (
      hasExactKeys(parsed, ["providerRuntimeMessage"]) &&
      isRecord(runtime) &&
      hasExactKeys(runtime, LEGACY_PROVIDER_RUNTIME_MESSAGE_WITH_ITEM_KEYS)
    ) {
      const legacy = decodeLegacyCorrelationWithItem(runtime);
      if (encodeHistoricalProviderRuntimeMessageMetadataWithItem(legacy) !== source) {
        throw new Error("Invalid historical orchestration metadata encoding");
      }
      return {
        value: decodeClosedMetadata({
          providerRuntimeMessage: {
            runtimeEventId: legacy.runtimeEventId,
            eventType: legacy.runtimeEventType,
            providerInstanceId: legacy.providerInstanceId,
            providerTurnId: legacy.providerTurnId,
            providerItemId: legacy.providerItemId,
          },
        }),
        encoding: ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_WITH_ITEM_V1,
      };
    }
    if (
      hasExactKeys(parsed, ["providerRuntimeMessage"]) &&
      isRecord(runtime) &&
      hasExactKeys(runtime, LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS)
    ) {
      const legacy = decodeLegacyCorrelation(runtime);
      if (encodeHistoricalProviderRuntimeMessageMetadata(legacy) !== source) {
        throw new Error("Invalid historical orchestration metadata encoding");
      }
      return {
        value: decodeClosedMetadata({
          providerRuntimeMessage: decodeLegacyProviderRuntimeMessageCorrelation(legacy),
        }),
        encoding: ORCHESTRATION_METADATA_STORAGE_ENCODING_LEGACY_RUNTIME_V0,
      };
    }
  }

  const decoded = decodeClosedMetadata(parsed);
  const encoded = encodeClosedMetadata(decoded);
  const schemaOrder = JSON.stringify(encoded);
  const alphabetical = canonicalJson(encoded as JsonValue);
  const encoding =
    source === schemaOrder
      ? ORCHESTRATION_METADATA_STORAGE_ENCODING_SCHEMA_ORDER_V1
      : source === alphabetical
        ? ORCHESTRATION_METADATA_STORAGE_ENCODING_ALPHABETICAL_V1
        : undefined;
  if (encoding === undefined) {
    throw new Error("Persisted orchestration metadata was transformed by schema decoding");
  }
  return { value: decoded, encoding };
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
  const decoded = decodeCanonicalOrLegacyOrchestrationMetadata(source);
  return { source, value: decoded.value };
};
