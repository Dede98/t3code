import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";

export type DurablePrefixOutcome =
  | { readonly _tag: "durably-applied" }
  | {
      readonly _tag: "isolated-but-prefix-failed";
      readonly cause: Cause.Cause<unknown>;
    };

export const combineCauses = (
  left: Cause.Cause<unknown> | undefined,
  right: Cause.Cause<unknown> | undefined,
): Cause.Cause<unknown> | undefined => {
  if (left === undefined) return right;
  if (right === undefined) return left;
  return Cause.combine(left, right);
};

export const causeFromExit = (exit: Exit.Exit<void, unknown>): Cause.Cause<unknown> | undefined =>
  Exit.isFailure(exit) ? exit.cause : undefined;

export interface DurablePrefixOutcomeTracker {
  /** Operational isolation continues, but this attempt's durable prefix stays failed. */
  readonly recordIsolatedFailure: (cause: Cause.Cause<unknown>) => Effect.Effect<void>;
  readonly snapshot: Effect.Effect<DurablePrefixOutcome>;
  /** Complete a lifecycle acknowledgement with the complete durable-prefix outcome. */
  readonly acknowledge: (
    acknowledgement: Deferred.Deferred<void>,
    additionalExit?: Exit.Exit<void, unknown>,
  ) => Effect.Effect<void>;
}

export const makeDurablePrefixOutcomeTracker: Effect.Effect<DurablePrefixOutcomeTracker> =
  Effect.gen(function* () {
    const state = yield* Ref.make<DurablePrefixOutcome>({ _tag: "durably-applied" });

    const recordIsolatedFailure: DurablePrefixOutcomeTracker["recordIsolatedFailure"] = (cause) =>
      Ref.update(state, (current) => ({
        _tag: "isolated-but-prefix-failed" as const,
        cause: current._tag === "durably-applied" ? cause : Cause.combine(current.cause, cause),
      }));

    const acknowledge: DurablePrefixOutcomeTracker["acknowledge"] = (
      acknowledgement,
      additionalExit = Exit.void,
    ) =>
      Effect.gen(function* () {
        const current = yield* Ref.get(state);
        const prefixCause =
          current._tag === "isolated-but-prefix-failed" ? current.cause : undefined;
        const cause = combineCauses(prefixCause, causeFromExit(additionalExit));
        const exit = cause === undefined ? Exit.void : Exit.failCause(cause as Cause.Cause<never>);
        yield* Deferred.done(acknowledgement, exit).pipe(Effect.ignore);
      });

    return {
      recordIsolatedFailure,
      snapshot: Ref.get(state),
      acknowledge,
    } satisfies DurablePrefixOutcomeTracker;
  });
