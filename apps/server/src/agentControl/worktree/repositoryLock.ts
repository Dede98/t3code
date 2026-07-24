import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export class AgentControlRepositoryLockError extends Schema.TaggedErrorClass<AgentControlRepositoryLockError>()(
  "AgentControlRepositoryLockError",
  { reason: Schema.Literals(["busy", "io"]) },
) {}

const lockError = (reason: AgentControlRepositoryLockError["reason"]) =>
  new AgentControlRepositoryLockError({ reason });

const isAlreadyExists = (cause: unknown) =>
  typeof cause === "object" &&
  cause !== null &&
  "reason" in cause &&
  typeof (cause as { readonly reason?: unknown }).reason === "object" &&
  (cause as { readonly reason: { readonly _tag?: unknown } }).reason._tag === "AlreadyExists";

const LockMetadata = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  runtimeHolderId: Schema.String,
  pid: Schema.Int,
  acquiredAt: Schema.String,
});
const encodeLockMetadata = Schema.encodeUnknownEffect(Schema.fromJsonString(LockMetadata));

/**
 * Cross-process lock for Agent-Control Git observations. The directory create
 * is the atomic ownership operation. A live or merely unknown owner is never
 * stolen; callers wait for a bounded interval and remain interruptible.
 */
export const withAgentControlRepositoryLock = <A, E, R>(input: {
  readonly repositoryCommonDir: string;
  readonly runtimeHolderId: string;
  readonly timeoutMs?: number;
  readonly effect: Effect.Effect<A, E, R>;
}): Effect.Effect<
  A,
  E | AgentControlRepositoryLockError,
  Exclude<R, Scope.Scope> | FileSystem.FileSystem | Path.Path
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const lockPath = path.join(input.repositoryCommonDir, "t3-agent-control.lock");
      const metadataPath = path.join(lockPath, "owner.json");
      const metadataTemporaryPath = path.join(lockPath, "owner.tmp");
      const timeoutMs = input.timeoutMs ?? 10_000;
      let remainingAttempts = Math.max(1, Math.ceil(timeoutMs / 25));
      while (true) {
        const attempt = yield* Effect.result(fs.makeDirectory(lockPath, { mode: 0o700 }));
        if (attempt._tag === "Success") break;
        if (!isAlreadyExists(attempt.failure)) return yield* lockError("io");
        remainingAttempts -= 1;
        if (remainingAttempts <= 0) return yield* lockError("busy");
        yield* Effect.sleep("25 millis");
      }
      yield* Effect.addFinalizer(() =>
        fs
          .remove(metadataPath, { force: true })
          .pipe(
            Effect.andThen(fs.remove(metadataTemporaryPath, { force: true })),
            Effect.andThen(fs.remove(lockPath, { recursive: true })),
            Effect.ignore,
          ),
      );
      const metadata = yield* encodeLockMetadata({
        schemaVersion: 1,
        runtimeHolderId: input.runtimeHolderId,
        pid: process.pid,
        acquiredAt: DateTime.formatIso(yield* DateTime.now),
      }).pipe(Effect.mapError(() => lockError("io")));
      yield* fs.writeFileString(metadataTemporaryPath, `${metadata}\n`).pipe(
        Effect.andThen(fs.chmod(metadataTemporaryPath, 0o600)),
        Effect.andThen(fs.rename(metadataTemporaryPath, metadataPath)),
        Effect.mapError(() => lockError("io")),
      );
      const directory = yield* fs
        .open(lockPath, { flag: "r" })
        .pipe(Effect.mapError(() => lockError("io")));
      yield* directory.sync.pipe(Effect.mapError(() => lockError("io")));
      return yield* input.effect;
    }),
  );
