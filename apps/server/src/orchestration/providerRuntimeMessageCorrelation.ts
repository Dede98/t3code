import { ProviderRuntimeMessageCorrelation } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

const LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS = [
  "providerInstanceId",
  "providerTurnId",
  "runtimeEventId",
  "runtimeEventType",
] as const;

const PROVIDER_RUNTIME_MESSAGE_KEYS = [
  "eventType",
  "providerInstanceId",
  "providerItemId",
  "providerTurnId",
  "runtimeEventId",
] as const;

const LegacyProviderRuntimeMessageCorrelation = Schema.Struct({
  runtimeEventId: ProviderRuntimeMessageCorrelation.fields.runtimeEventId,
  runtimeEventType: ProviderRuntimeMessageCorrelation.fields.eventType,
  providerInstanceId: ProviderRuntimeMessageCorrelation.fields.providerInstanceId,
  providerTurnId: ProviderRuntimeMessageCorrelation.fields.providerTurnId,
}).annotate({ parseOptions: { onExcessProperty: "error" } });

const decodeLegacyCorrelation = Schema.decodeUnknownSync(LegacyProviderRuntimeMessageCorrelation);
const decodeCorrelation = Schema.decodeUnknownSync(ProviderRuntimeMessageCorrelation);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasExactKeys = (value: Record<string, unknown>, expected: ReadonlyArray<string>): boolean => {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
};

/** Decode only the four fields emitted by the historical production encoder. */
export const decodeLegacyProviderRuntimeMessageCorrelation = (
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

/** Decode only the required, closed five-field production contract. */
export const decodeProviderRuntimeMessageCorrelation = (
  input: unknown,
): ProviderRuntimeMessageCorrelation => decodeCorrelation(input);

/**
 * The only storage compatibility admitted for provider runtime message
 * correlation is the exact historical four-field object. The stored JSON is
 * never rewritten; the missing item id is supplied only in memory. Raw
 * storage callers must parse with the duplicate-key-preserving strict parser
 * before reaching this object seam.
 */
export const normalizeLegacyProviderRuntimeMessageCorrelationMetadata = (
  metadata: unknown,
): unknown => {
  if (!isRecord(metadata)) return metadata;
  const runtime = metadata.providerRuntimeMessage;
  if (!isRecord(runtime)) return metadata;
  if (hasExactKeys(runtime, LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS)) {
    return {
      ...metadata,
      providerRuntimeMessage: decodeLegacyProviderRuntimeMessageCorrelation(runtime),
    };
  }
  if (hasExactKeys(runtime, PROVIDER_RUNTIME_MESSAGE_KEYS)) {
    return {
      ...metadata,
      providerRuntimeMessage: decodeProviderRuntimeMessageCorrelation(runtime),
    };
  }
  return metadata;
};
