/**
 * DrainableWorker - A queue-based worker that exposes a `drain()` effect.
 *
 * Wraps the common `Queue.unbounded` + `Effect.forever` pattern and adds
 * a signal that resolves when the queue is empty **and** the current item
 * has finished processing. This lets tests replace timing-sensitive
 * `Effect.sleep` calls with deterministic `drain()`.
 *
 * @module DrainableWorker
 */
import * as Scope from "effect/Scope";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as TxQueue from "effect/TxQueue";
import * as TxRef from "effect/TxRef";

export interface DrainableWorker<A, E = never> {
  /**
   * Enqueue a work item and track it for `drain()`.
   *
   * This wraps `Queue.offer` so drain state is updated atomically with the
   * enqueue path instead of inferring it from queue internals.
   */
  readonly enqueue: (item: A) => Effect.Effect<void, E>;

  /**
   * Resolves when the queue is empty and the worker is idle (not processing).
   */
  readonly drain: Effect.Effect<void, E>;
}

export interface DrainableWorkerOptions {
  /**
   * Make a terminal worker Cause observable through both `enqueue` and
   * `drain`. Buffered work is discarded once the worker terminates.
   *
   * This is opt-in so existing best-effort workers retain their established
   * failure behavior.
   */
  readonly failureMode: "observable";
}

/**
 * Create a drainable worker that processes items from an unbounded queue.
 *
 * The worker is forked into the current scope and will be interrupted when
 * the scope closes. A finalizer shuts down the queue.
 *
 * @param process - The effect to run for each queued item.
 * @returns A `DrainableWorker` with `queue` and `drain`.
 */
const makeLegacyDrainableWorker = <A, E, R>(
  process: (item: A) => Effect.Effect<void, E, R>,
): Effect.Effect<DrainableWorker<A>, never, Scope.Scope | R> =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(TxQueue.unbounded<A>(), TxQueue.shutdown);
    const outstanding = yield* TxRef.make(0);

    yield* TxQueue.take(queue).pipe(
      Effect.tap((a) =>
        Effect.ensuring(
          process(a),
          TxRef.update(outstanding, (n) => n - 1),
        ),
      ),
      Effect.forever,
      Effect.forkScoped,
    );

    const drain: DrainableWorker<A>["drain"] = TxRef.get(outstanding).pipe(
      Effect.tap((n) => (n > 0 ? Effect.txRetry : Effect.void)),
      Effect.tx,
    );

    const enqueue = (element: A): Effect.Effect<void> =>
      TxQueue.offer(queue, element).pipe(
        Effect.tap(() => TxRef.update(outstanding, (n) => n + 1)),
        Effect.tx,
        Effect.asVoid,
      );

    return { enqueue, drain } satisfies DrainableWorker<A>;
  });

type ObservableWorkerState<E> =
  | { readonly _tag: "Running"; readonly outstanding: number }
  | { readonly _tag: "Terminated"; readonly cause: Cause.Cause<E> };

const makeObservableDrainableWorker = <A, E, R>(
  process: (item: A) => Effect.Effect<void, E, R>,
): Effect.Effect<DrainableWorker<A, E>, never, Scope.Scope | R> =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(TxQueue.unbounded<A>(), TxQueue.shutdown);
    const state = yield* TxRef.make<ObservableWorkerState<E>>({
      _tag: "Running",
      outstanding: 0,
    });

    const recordTermination = (cause: Cause.Cause<E>) =>
      Effect.gen(function* () {
        const current = yield* TxRef.get(state);
        if (current._tag === "Terminated") return;
        yield* TxQueue.clear(queue).pipe(Effect.ignore);
        yield* TxRef.set(state, { _tag: "Terminated", cause });
      }).pipe(Effect.tx);

    const processTracked = (item: A) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const exit = yield* Effect.exit(restore(process(item)));
          yield* Effect.gen(function* () {
            const current = yield* TxRef.get(state);
            if (current._tag === "Terminated") return;
            if (Exit.isFailure(exit)) {
              yield* TxQueue.clear(queue).pipe(Effect.ignore);
              yield* TxRef.set(state, { _tag: "Terminated", cause: exit.cause });
              return;
            }
            yield* TxRef.set(state, {
              _tag: "Running",
              outstanding: current.outstanding - 1,
            });
          }).pipe(Effect.tx);
          if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
        }),
      );

    yield* TxQueue.take(queue).pipe(
      Effect.flatMap(processTracked),
      Effect.forever,
      Effect.onExit((exit) => (Exit.isFailure(exit) ? recordTermination(exit.cause) : Effect.void)),
      Effect.forkScoped,
    );

    const drain: DrainableWorker<A, E>["drain"] = Effect.gen(function* () {
      const current = yield* TxRef.get(state);
      if (current._tag === "Terminated") {
        return yield* Effect.failCause(current.cause);
      }
      if (current.outstanding > 0) return yield* Effect.txRetry;
    }).pipe(Effect.tx);

    const enqueue = (element: A): Effect.Effect<void, E> =>
      Effect.gen(function* () {
        const current = yield* TxRef.get(state);
        if (current._tag === "Terminated") {
          return yield* Effect.failCause(current.cause);
        }
        const accepted = yield* TxQueue.offer(queue, element);
        if (!accepted) return yield* Effect.interrupt;
        yield* TxRef.set(state, {
          _tag: "Running",
          outstanding: current.outstanding + 1,
        });
      }).pipe(Effect.tx);

    return { enqueue, drain } satisfies DrainableWorker<A, E>;
  });

export function makeDrainableWorker<A, E, R>(
  process: (item: A) => Effect.Effect<void, E, R>,
  options: DrainableWorkerOptions,
): Effect.Effect<DrainableWorker<A, E>, never, Scope.Scope | R>;
export function makeDrainableWorker<A, E, R>(
  process: (item: A) => Effect.Effect<void, E, R>,
): Effect.Effect<DrainableWorker<A>, never, Scope.Scope | R>;
export function makeDrainableWorker<A, E, R>(
  process: (item: A) => Effect.Effect<void, E, R>,
  options?: DrainableWorkerOptions,
): Effect.Effect<DrainableWorker<A, E>, never, Scope.Scope | R> {
  return options?.failureMode === "observable"
    ? makeObservableDrainableWorker(process)
    : makeLegacyDrainableWorker(process);
}
