import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";

/** One attempt-local, infallible gate shared by every commit-gated reactor. */
export interface ReactorStartupActivation {
  readonly await: Effect.Effect<void>;
  readonly open: Effect.Effect<void>;
}

export const makeReactorStartupActivation: Effect.Effect<ReactorStartupActivation> = Effect.map(
  Deferred.make<void>(),
  (gate) => ({
    await: Deferred.await(gate),
    open: Deferred.succeed(gate, undefined).pipe(Effect.asVoid),
  }),
);

export const alreadyActivated: ReactorStartupActivation = {
  await: Effect.void,
  open: Effect.void,
};
