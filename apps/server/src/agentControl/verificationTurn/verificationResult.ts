import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  canonicalJson,
  parseJsonStrict,
  sha256Utf8,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";

export const AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION =
  "agent-control-verification-result-v1";
export const AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES = 64 * 1024;
export const AGENT_CONTROL_VERIFICATION_REPORT_MAX_BYTES = 32 * 1024;

const resultSchemaDescriptor = canonicalJson({
  additionalProperties: false,
  properties: {
    report: { maxUtf8Bytes: AGENT_CONTROL_VERIFICATION_REPORT_MAX_BYTES, type: "string" },
    schemaVersion: { const: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION },
    verdict: { enum: ["failed", "passed"] },
  },
  required: ["report", "schemaVersion", "verdict"],
  root: "object",
});

export const AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT =
  sha256Utf8(resultSchemaDescriptor);

export const VerificationResultDecodeErrorCode = Schema.Literals([
  "missing-final-message",
  "output-too-large",
  "invalid-utf8",
  "malformed-json",
  "unsupported-schema-version",
  "schema-violation",
]);
export type VerificationResultDecodeErrorCode = typeof VerificationResultDecodeErrorCode.Type;

export class VerificationResultDecodeError extends Schema.TaggedErrorClass<VerificationResultDecodeError>()(
  "VerificationResultDecodeError",
  {
    code: VerificationResultDecodeErrorCode,
  },
) {}

export interface DecodedVerificationResult {
  readonly verdict: "passed" | "failed";
  readonly semanticDigest: string;
}

const fail = (code: VerificationResultDecodeErrorCode) =>
  new VerificationResultDecodeError({ code });

const isJsonObject = (value: JsonValue): value is { readonly [key: string]: JsonValue } =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/**
 * Pure, service-free decoder for the final Verification assistant message.
 * Errors intentionally expose only a closed safe code and never retain input.
 */
export const decodeVerificationResult = Effect.fn("decodeVerificationResult")(function* (
  bytes: Uint8Array,
): Effect.fn.Return<DecodedVerificationResult, VerificationResultDecodeError> {
  if (bytes.byteLength === 0) {
    return yield* fail("missing-final-message");
  }
  if (bytes.byteLength > AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES) {
    return yield* fail("output-too-large");
  }

  const source = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: () => fail("invalid-utf8"),
  });
  const parsed = yield* Effect.try({
    try: () => parseJsonStrict(source),
    catch: () => fail("malformed-json"),
  });
  if (!isJsonObject(parsed)) {
    return yield* fail("schema-violation");
  }
  if (
    typeof parsed.schemaVersion === "string" &&
    parsed.schemaVersion !== AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION
  ) {
    return yield* fail("unsupported-schema-version");
  }
  const keys = Object.keys(parsed).sort();
  const expectedKeys = ["report", "schemaVersion", "verdict"] as const;
  if (
    keys.length !== expectedKeys.length ||
    keys.some((key, index) => key !== expectedKeys[index]) ||
    parsed.schemaVersion !== AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION ||
    (parsed.verdict !== "passed" && parsed.verdict !== "failed") ||
    typeof parsed.report !== "string" ||
    new TextEncoder().encode(parsed.report).byteLength > AGENT_CONTROL_VERIFICATION_REPORT_MAX_BYTES
  ) {
    return yield* fail("schema-violation");
  }

  const semanticJson = canonicalJson({
    report: parsed.report,
    schemaVersion: parsed.schemaVersion,
    verdict: parsed.verdict,
  });
  return {
    verdict: parsed.verdict,
    semanticDigest: sha256Utf8(semanticJson),
  };
});
