import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";

export type ReactorStartupCloseDisposition = "retryable" | "terminal";

export class ReactorStartupAttemptError extends Schema.TaggedErrorClass<ReactorStartupAttemptError>()(
  "ReactorStartupAttemptError",
  {
    reason: Schema.Literals(["attempt-closing", "attempt-closed"]),
  },
) {}

/** One attempt-local, infallible gate shared by every commit-gated reactor. */
export interface ReactorStartupActivation {
  readonly await: Effect.Effect<void>;
  readonly open: Effect.Effect<void>;
  /** Whether closing this attempt may return a participating reactor to idle. */
  readonly closeDisposition: Effect.Effect<ReactorStartupCloseDisposition>;
  /**
   * Install an ordered drain that the shared attempt runs before any resource
   * scope is finalized. Returns whether this activation owns that drain.
   */
  readonly registerShutdownDrain: (drain: Effect.Effect<void>) => Effect.Effect<boolean>;
}

/**
 * Server-owned lifecycle boundary for one complete reactor startup attempt.
 *
 * `commit` and `close` share one permit. Once `commit` owns it, the complete
 * provider-barrier/activation cutover is uninterruptible and no participating
 * attempt resource can be finalized until it returns. If `close` owns it
 * first, all resources close while both gates remain parked and commit fails.
 */
export interface ReactorStartupAttempt {
  readonly activation: ReactorStartupActivation;
  readonly commit: <E, R>(
    effect: Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | ReactorStartupAttemptError, R>;
  readonly close: (exit: Exit.Exit<unknown, unknown>) => Effect.Effect<void>;
}

export const makeReactorStartupActivation: Effect.Effect<ReactorStartupActivation> = Effect.map(
  Deferred.make<void>(),
  (gate) => ({
    await: Deferred.await(gate),
    open: Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
    closeDisposition: Effect.succeed("retryable" as const),
    registerShutdownDrain: () => Effect.succeed(false),
  }),
);

type AttemptState =
  | { readonly _tag: "preparing" }
  | { readonly _tag: "committing" }
  | { readonly _tag: "committed" }
  | { readonly _tag: "terminal"; readonly cause: Cause.Cause<unknown> }
  | { readonly _tag: "closing"; readonly disposition: ReactorStartupCloseDisposition }
  | {
      readonly _tag: "closed";
      readonly disposition: ReactorStartupCloseDisposition;
      readonly closeExit: Exit.Exit<void>;
      readonly terminalCause?: Cause.Cause<unknown>;
    };

export const makeReactorStartupAttempt = Effect.fn("makeReactorStartupAttempt")(function* (
  resourcesScope: Scope.Closeable,
): Effect.fn.Return<ReactorStartupAttempt> {
  const gate = yield* Deferred.make<void>();
  const lifecycleSemaphore = yield* Semaphore.make(1);
  let state: AttemptState = { _tag: "preparing" };
  let irreversible = false;
  const shutdownDrains: Array<Effect.Effect<void>> = [];

  const activation: ReactorStartupActivation = {
    await: Deferred.await(gate),
    open: Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
    closeDisposition: Effect.sync(() => (irreversible ? "terminal" : "retryable")),
    registerShutdownDrain: (drain) =>
      Effect.sync(() => {
        shutdownDrains.push(drain);
        return true;
      }),
  };

  const commit: ReactorStartupAttempt["commit"] = (effect) =>
    Effect.uninterruptibleMask((restore) =>
      restore(
        lifecycleSemaphore.withPermits(1)(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (state._tag === "committed") return;
              if (state._tag === "terminal") {
                return yield* Effect.failCause(state.cause as Cause.Cause<never>);
              }
              if (state._tag === "closing") {
                return yield* new ReactorStartupAttemptError({ reason: "attempt-closing" });
              }
              if (state._tag === "closed") {
                if (state.terminalCause !== undefined) {
                  return yield* Effect.failCause(state.terminalCause as Cause.Cause<never>);
                }
                return yield* new ReactorStartupAttemptError({ reason: "attempt-closed" });
              }
              if (state._tag === "committing") {
                return yield* Effect.die("Reactor startup cutover permit was re-entered.");
              }

              irreversible = true;
              state = { _tag: "committing" };
              const commitExit = yield* Effect.exit(effect);
              state = Exit.isSuccess(commitExit)
                ? { _tag: "committed" }
                : { _tag: "terminal", cause: commitExit.cause };
              if (Exit.isFailure(commitExit)) return yield* Effect.failCause(commitExit.cause);
            }),
          ),
        ),
      ),
    );

  const combineExits = (exits: ReadonlyArray<Exit.Exit<void>>): Exit.Exit<void> => {
    const causes = exits.flatMap((candidate) =>
      Exit.isFailure(candidate) ? [candidate.cause] : ([] as Array<Cause.Cause<never>>),
    );
    if (causes.length === 0) return Exit.void;
    return Exit.failCause(
      causes
        .slice(1)
        .reduce<Cause.Cause<never>>(
          (left, right) => Cause.combine(left, right) as Cause.Cause<never>,
          causes[0]!,
        ),
    );
  };

  const close: ReactorStartupAttempt["close"] = (exit) =>
    Effect.uninterruptible(
      lifecycleSemaphore.withPermits(1)(
        Effect.gen(function* () {
          if (state._tag === "closed") {
            if (Exit.isFailure(state.closeExit))
              return yield* Effect.failCause(state.closeExit.cause);
            return;
          }
          if (state._tag === "closing") {
            return yield* Effect.die("Reactor startup close permit was re-entered.");
          }

          const disposition: ReactorStartupCloseDisposition = irreversible
            ? "terminal"
            : "retryable";
          const terminalCause = state._tag === "terminal" ? state.cause : undefined;
          state = { _tag: "closing", disposition };
          const drainExits = irreversible
            ? yield* Effect.forEach(shutdownDrains, (drain) => Effect.exit(drain), {
                concurrency: 1,
              })
            : [];
          const resourcesExit = yield* Effect.exit(Scope.close(resourcesScope, exit));
          const closeExit = combineExits([...drainExits, resourcesExit]);
          state = {
            _tag: "closed",
            disposition,
            closeExit,
            ...(terminalCause === undefined ? {} : { terminalCause }),
          };
          if (Exit.isFailure(closeExit)) return yield* Effect.failCause(closeExit.cause);
        }),
      ),
    );

  return { activation, commit, close };
});

export const alreadyActivated: ReactorStartupActivation = {
  await: Effect.void,
  open: Effect.void,
  closeDisposition: Effect.succeed("retryable"),
  registerShutdownDrain: () => Effect.succeed(false),
};
