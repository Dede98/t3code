import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { evaluateCheckedVerificationResult } from "./checkedResult.ts";

const verdictBytes = (verdict: "passed" | "failed") =>
  new TextEncoder().encode(
    JSON.stringify({
      schemaVersion: "agent-control-verification-result-v1",
      verdict,
      report: "Implementation says its tests passed.",
    }),
  );

it.effect.each([
  "verification-checks-missing",
  "verification-checks-unavailable",
  "verification-checks-stale",
  "verification-checks-failed",
] as const)("a valid passed verdict cannot override %s", (code) =>
  Effect.gen(function* () {
    assert.deepStrictEqual(yield* evaluateCheckedVerificationResult(verdictBytes("passed"), code), {
      disposition: "invalid-output",
      verdict: null,
      errorCode: code,
      semanticResultDigest: null,
    });
  }),
);

it.effect("accepts a valid verdict after all mandatory checks passed", () =>
  Effect.gen(function* () {
    const result = yield* evaluateCheckedVerificationResult(verdictBytes("passed"), null);
    assert.equal(result.disposition, "evaluated");
    assert.equal(result.verdict, "passed");
    assert.equal(result.errorCode, null);
    assert.match(result.semanticResultDigest!, /^[0-9a-f]{64}$/u);
  }),
);

it.effect("retains a confirmed code failure for Repair only with a failed verifier verdict", () =>
  Effect.gen(function* () {
    const result = yield* evaluateCheckedVerificationResult(
      verdictBytes("failed"),
      "verification-checks-failed",
    );
    assert.equal(result.disposition, "evaluated");
    assert.equal(result.verdict, "failed");
    assert.equal(result.errorCode, null);
  }),
);

it.effect("an unavailable check cannot become a code repair through a failed verdict", () =>
  Effect.gen(function* () {
    assert.deepStrictEqual(
      yield* evaluateCheckedVerificationResult(
        verdictBytes("failed"),
        "verification-checks-unavailable",
      ),
      {
        disposition: "invalid-output",
        verdict: null,
        errorCode: "verification-checks-unavailable",
        semanticResultDigest: null,
      },
    );
  }),
);

it.effect("successful checks cannot replace a valid final verifier message", () =>
  Effect.gen(function* () {
    const result = yield* evaluateCheckedVerificationResult(
      new TextEncoder().encode("passed"),
      null,
    );
    assert.equal(result.disposition, "invalid-output");
    assert.equal(result.verdict, null);
    assert.equal(result.errorCode, "malformed-json");
  }),
);
