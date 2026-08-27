import type { CommandId, EventId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

import type { PersistenceSqlError } from "../../persistence/Errors.ts";

export interface VerificationResultRuntimeEventAuthorityObservation {
  readonly runtimeEventId: EventId;
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly fragmentKind: "delta" | "completion";
}

export interface VerificationResultRuntimeEventAuthorityHooksShape {
  readonly beforeAuthorityWrite: (
    observation: VerificationResultRuntimeEventAuthorityObservation,
  ) => Effect.Effect<void, PersistenceSqlError>;
  readonly beforeCommittedWinnerRead: (
    observation: VerificationResultRuntimeEventAuthorityObservation,
    originalError: PersistenceSqlError,
  ) => Effect.Effect<void>;
}

const beforeAuthorityWriteNoop = (
  _observation: VerificationResultRuntimeEventAuthorityObservation,
) => Effect.void;
const beforeCommittedWinnerReadNoop = (
  _observation: VerificationResultRuntimeEventAuthorityObservation,
  _originalError: PersistenceSqlError,
) => Effect.void;

/**
 * Server-internal deterministic seam for the RuntimeEventId write boundary.
 * Production uses the cached no-op default; no transport or provider input can
 * configure these callbacks.
 */
export const VerificationResultRuntimeEventAuthorityHooks =
  Context.Reference<VerificationResultRuntimeEventAuthorityHooksShape>(
    "t3/orchestration/Services/VerificationResultRuntimeEventAuthorityHooks",
    {
      defaultValue: () => ({
        beforeAuthorityWrite: beforeAuthorityWriteNoop,
        beforeCommittedWinnerRead: beforeCommittedWinnerReadNoop,
      }),
    },
  );
