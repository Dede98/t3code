import * as Effect from "effect/Effect";
import * as Result from "effect/Result";

import type { AgentControlVerificationInvalidOutputCode } from "@t3tools/contracts";
import { decodeVerificationResult } from "./verificationResult.ts";

/** A model verdict is usable only alongside this verification's controller evidence. */
export const evaluateCheckedVerificationResult = Effect.fn("evaluateCheckedVerificationResult")(
  function* (
    bytes: Uint8Array,
    checkError: AgentControlVerificationInvalidOutputCode | null,
    oversized = false,
  ) {
    const decoded = yield* Effect.result(decodeVerificationResult(bytes));
    if (
      checkError !== null &&
      !(
        checkError === "verification-checks-failed" &&
        Result.isSuccess(decoded) &&
        decoded.success.verdict === "failed"
      )
    ) {
      return {
        disposition: "invalid-output",
        verdict: null,
        errorCode: checkError,
        semanticResultDigest: null,
      } as const;
    }
    if (oversized) {
      return {
        disposition: "invalid-output",
        verdict: null,
        errorCode: "output-too-large",
        semanticResultDigest: null,
      } as const;
    }
    return Result.isSuccess(decoded)
      ? ({
          disposition: "evaluated",
          verdict: decoded.success.verdict,
          errorCode: null,
          semanticResultDigest: decoded.success.semanticDigest,
        } as const)
      : ({
          disposition: "invalid-output",
          verdict: null,
          errorCode: decoded.failure.code,
          semanticResultDigest: null,
        } as const);
  },
);
