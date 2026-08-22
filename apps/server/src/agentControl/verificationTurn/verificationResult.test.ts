import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { canonicalJson, sha256Utf8 } from "../initialPlanning/eventEvidence.ts";
import {
  AGENT_CONTROL_VERIFICATION_REPORT_MAX_BYTES,
  AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES,
  AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
  decodeVerificationResult,
  type VerificationResultDecodeErrorCode,
} from "./verificationResult.ts";

const bytes = (source: string) => new TextEncoder().encode(source);
const valid = (verdict: "passed" | "failed", report = "Focused verification completed.") => ({
  schemaVersion: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION,
  verdict,
  report,
});

const expectCode = (source: Uint8Array, code: VerificationResultDecodeErrorCode) =>
  Effect.gen(function* () {
    const result = yield* Effect.flip(decodeVerificationResult(source));
    assert.equal(result.code, code);
    assert.deepStrictEqual(Object.keys(result).sort(), ["_tag", "code"]);
  });

it.effect("decodes passed and failed Result-v1 objects", () =>
  Effect.gen(function* () {
    const passed = yield* decodeVerificationResult(bytes(canonicalJson(valid("passed"))));
    const failed = yield* decodeVerificationResult(bytes(canonicalJson(valid("failed"))));
    assert.equal(passed.verdict, "passed");
    assert.equal(failed.verdict, "failed");
    assert.notEqual(passed.semanticDigest, failed.semanticDigest);
  }),
);

it.effect("canonicalizes whitespace and key order into one semantic digest", () =>
  Effect.gen(function* () {
    const first = yield* decodeVerificationResult(bytes(canonicalJson(valid("passed"))));
    const second = yield* decodeVerificationResult(
      bytes(
        ` { "report" : "Focused verification completed.", "verdict" : "passed", "schemaVersion" : "${AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION}" } `,
      ),
    );
    assert.equal(first.semanticDigest, second.semanticDigest);
    assert.equal(first.semanticDigest, sha256Utf8(canonicalJson(valid("passed"))));
  }),
);

it.effect("rejects code fences, prose, multiple values, and malformed JSON", () =>
  Effect.gen(function* () {
    const result = canonicalJson(valid("passed"));
    yield* expectCode(bytes(`\`\`\`json\n${result}\n\`\`\``), "malformed-json");
    yield* expectCode(bytes(`prefix ${result}`), "malformed-json");
    yield* expectCode(bytes(`${result} suffix`), "malformed-json");
    yield* expectCode(bytes(`${result}${result}`), "malformed-json");
    yield* expectCode(bytes("{"), "malformed-json");
    yield* expectCode(
      bytes(
        `{"schemaVersion":"${AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION}","verdict":"passed","report":"one","report":"two"}`,
      ),
      "malformed-json",
    );
  }),
);

it.effect("rejects unsupported versions and every closed-schema violation", () =>
  Effect.gen(function* () {
    yield* expectCode(
      bytes(canonicalJson({ ...valid("passed"), schemaVersion: "result-v2" })),
      "unsupported-schema-version",
    );
    yield* expectCode(
      bytes(canonicalJson({ ...valid("passed"), extra: true })),
      "schema-violation",
    );
    yield* expectCode(
      bytes(canonicalJson({ ...valid("passed"), verdict: "indeterminate" })),
      "schema-violation",
    );
    yield* expectCode(
      bytes(canonicalJson({ schemaVersion: AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_VERSION })),
      "schema-violation",
    );
    yield* expectCode(bytes(canonicalJson([valid("passed")])), "schema-violation");
  }),
);

it.effect("enforces missing, UTF-8, result, and report byte limits", () =>
  Effect.gen(function* () {
    yield* expectCode(new Uint8Array(), "missing-final-message");
    yield* expectCode(Uint8Array.from([0xc3, 0x28]), "invalid-utf8");
    yield* expectCode(
      new Uint8Array(AGENT_CONTROL_VERIFICATION_RESULT_MAX_BYTES + 1).fill(0x20),
      "output-too-large",
    );
    yield* expectCode(
      bytes(
        canonicalJson(valid("passed", "x".repeat(AGENT_CONTROL_VERIFICATION_REPORT_MAX_BYTES + 1))),
      ),
      "schema-violation",
    );
  }),
);

it.effect("changes the semantic digest when report evidence changes", () =>
  Effect.gen(function* () {
    const first = yield* decodeVerificationResult(bytes(canonicalJson(valid("passed", "one"))));
    const second = yield* decodeVerificationResult(bytes(canonicalJson(valid("passed", "two"))));
    assert.notEqual(first.semanticDigest, second.semanticDigest);
  }),
);
