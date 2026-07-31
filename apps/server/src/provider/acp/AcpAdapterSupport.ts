import {
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as EffectAcpErrors from "effect-acp/errors";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  type ProviderAdapterError,
} from "../Errors.ts";
const isAcpProcessExitedError = Schema.is(EffectAcpErrors.AcpProcessExitedError);
const isAcpRequestError = Schema.is(EffectAcpErrors.AcpRequestError);

export function mapAcpToAdapterError(
  provider: ProviderDriverKind,
  threadId: ThreadId,
  method: string,
  error: EffectAcpErrors.AcpError,
): ProviderAdapterError {
  if (isAcpProcessExitedError(error)) {
    return new ProviderAdapterSessionClosedError({
      provider,
      threadId,
      cause: error,
    });
  }
  if (isAcpRequestError(error)) {
    return new ProviderAdapterRequestError({
      provider,
      method,
      detail: error.message,
      cause: error,
    });
  }
  return new ProviderAdapterRequestError({
    provider,
    method,
    detail: error.message,
    cause: error,
  });
}

export const mapEffectFailuresPreservingReasons =
  <E, E2>(mapFailure: (error: E) => E2) =>
  <A, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E2, R> =>
    effect.pipe(
      Effect.catchCause((cause) =>
        Effect.failCause(
          Cause.fromReasons(
            cause.reasons.map((reason) =>
              Cause.isFailReason(reason)
                ? Cause.makeFailReason(mapFailure(reason.error)).annotate(
                    Context.makeUnsafe(reason.annotations),
                  )
                : reason,
            ),
          ),
        ),
      ),
    );

export const mapAcpEffectToAdapterError =
  (provider: ProviderDriverKind, threadId: ThreadId, method: string) =>
  <A, R>(
    effect: Effect.Effect<A, EffectAcpErrors.AcpError, R>,
  ): Effect.Effect<A, ProviderAdapterError, R> =>
    effect.pipe(
      mapEffectFailuresPreservingReasons((error) =>
        mapAcpToAdapterError(provider, threadId, method, error),
      ),
    );

export function acpPermissionOutcome(decision: ProviderApprovalDecision): string {
  switch (decision) {
    case "acceptForSession":
      return "allow-always";
    case "accept":
      return "allow-once";
    case "decline":
    default:
      return "reject-once";
  }
}
