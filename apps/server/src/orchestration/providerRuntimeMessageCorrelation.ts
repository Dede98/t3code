const LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS = [
  "eventType",
  "providerInstanceId",
  "providerTurnId",
  "runtimeEventId",
] as const;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const hasExactKeys = (value: Record<string, unknown>, expected: ReadonlyArray<string>): boolean => {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
};

/**
 * The only storage compatibility admitted for provider runtime message
 * correlation is the exact historical four-field object. The stored JSON is
 * never rewritten; the missing item id is supplied only to the in-memory
 * strict new-contract decoder. Any extra/missing key remains untouched and is
 * rejected by that decoder.
 */
export const normalizeLegacyProviderRuntimeMessageCorrelationMetadata = (
  metadata: unknown,
): unknown => {
  if (!isRecord(metadata)) return metadata;
  const runtime = metadata.providerRuntimeMessage;
  if (!isRecord(runtime) || !hasExactKeys(runtime, LEGACY_PROVIDER_RUNTIME_MESSAGE_KEYS)) {
    return metadata;
  }
  return {
    ...metadata,
    providerRuntimeMessage: {
      ...runtime,
      providerItemId: null,
    },
  };
};
