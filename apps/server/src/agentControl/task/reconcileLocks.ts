import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";
import * as SynchronizedRef from "effect/SynchronizedRef";

export const makeAgentControlTaskReconcileLocks = Effect.fn("makeAgentControlTaskReconcileLocks")(
  function* <ProjectId>() {
    const locks = yield* SynchronizedRef.make(new Map<ProjectId, Semaphore.Semaphore>());

    const getLock = (projectId: ProjectId) =>
      SynchronizedRef.modifyEffect(locks, (current) => {
        const existing = current.get(projectId);
        if (existing !== undefined) return Effect.succeed([existing, current] as const);
        return Semaphore.make(1).pipe(
          Effect.map((lock) => {
            const next = new Map(current);
            next.set(projectId, lock);
            return [lock, next] as const;
          }),
        );
      });

    const withLock = <A, E, R, E2, R2>(
      projectId: ProjectId,
      preflight: Effect.Effect<void, E, R>,
      run: Effect.Effect<A, E2, R2>,
    ): Effect.Effect<A, E | E2, R | R2> =>
      Effect.gen(function* () {
        yield* preflight;
        const lock = yield* getLock(projectId);
        return yield* lock.withPermit(run);
      });

    return {
      withLock,
      size: SynchronizedRef.get(locks).pipe(Effect.map((current) => current.size)),
    } as const;
  },
);
