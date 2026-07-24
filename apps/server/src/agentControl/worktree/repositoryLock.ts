// @effect-diagnostics nodeBuiltinImport:off - inode-bound lstat/rmdir operations have no Effect FileSystem equivalent.
import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

export class AgentControlRepositoryLockError extends Schema.TaggedErrorClass<AgentControlRepositoryLockError>()(
  "AgentControlRepositoryLockError",
  { reason: Schema.Literals(["busy", "io", "ownership-lost"]) },
) {}

const lockError = (reason: AgentControlRepositoryLockError["reason"]) =>
  new AgentControlRepositoryLockError({ reason });

const errno = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "code" in cause
    ? (cause as { readonly code?: unknown }).code
    : undefined;

const LockMetadata = Schema.Struct({
  schemaVersion: Schema.Literal(2),
  ownerToken: Schema.String,
  runtimeHolderId: Schema.String,
  attemptId: Schema.String,
  pid: Schema.Int,
  lockPath: Schema.String,
  device: Schema.Number,
  inode: Schema.Number,
  acquiredAt: Schema.String,
});
type LockMetadata = typeof LockMetadata.Type;
const decodeLockMetadata = Schema.decodeUnknownEffect(Schema.fromJsonString(LockMetadata));
const encodeLockMetadata = Schema.encodeUnknownEffect(Schema.fromJsonString(LockMetadata));

interface LockClaim {
  readonly metadata: LockMetadata;
  readonly metadataPath: string;
}

const node = <A>(
  operation: () => Promise<A>,
  reason: AgentControlRepositoryLockError["reason"] = "io",
) =>
  Effect.tryPromise({
    try: operation,
    catch: () => lockError(reason),
  });

const removeOwnedLock = Effect.fn("AgentControlRepositoryLock.removeOwnedLock")(function* (
  claim: LockClaim,
) {
  const current = yield* node(() => NodeFSP.lstat(claim.metadata.lockPath), "ownership-lost");
  if (
    !current.isDirectory() ||
    current.isSymbolicLink() ||
    current.dev !== claim.metadata.device ||
    current.ino !== claim.metadata.inode
  ) {
    return yield* lockError("ownership-lost");
  }
  const ownerInfo = yield* node(() => NodeFSP.lstat(claim.metadataPath), "ownership-lost");
  if (!ownerInfo.isFile() || ownerInfo.isSymbolicLink() || ownerInfo.nlink !== 1) {
    return yield* lockError("ownership-lost");
  }
  const contents = yield* node(
    () => NodeFSP.readFile(claim.metadataPath, "utf8"),
    "ownership-lost",
  );
  const persisted = yield* decodeLockMetadata(contents).pipe(
    Effect.mapError(() => lockError("ownership-lost")),
  );
  if (
    persisted.ownerToken !== claim.metadata.ownerToken ||
    persisted.runtimeHolderId !== claim.metadata.runtimeHolderId ||
    persisted.attemptId !== claim.metadata.attemptId ||
    persisted.lockPath !== claim.metadata.lockPath ||
    persisted.device !== claim.metadata.device ||
    persisted.inode !== claim.metadata.inode
  ) {
    return yield* lockError("ownership-lost");
  }
  const rechecked = yield* node(() => NodeFSP.lstat(claim.metadata.lockPath), "ownership-lost");
  if (
    !rechecked.isDirectory() ||
    rechecked.dev !== claim.metadata.device ||
    rechecked.ino !== claim.metadata.inode
  ) {
    return yield* lockError("ownership-lost");
  }
  yield* node(() => NodeFSP.unlink(claim.metadataPath));
  yield* node(() => NodeFSP.rmdir(claim.metadata.lockPath), "ownership-lost");
});

const acquireLock = Effect.fn("AgentControlRepositoryLock.acquireLock")(function* (input: {
  readonly lockPath: string;
  readonly runtimeHolderId: string;
  readonly timeoutMs: number;
}) {
  let remainingAttempts = Math.max(1, Math.ceil(input.timeoutMs / 25));
  while (true) {
    const attempt = yield* Effect.tryPromise({
      try: () => NodeFSP.mkdir(input.lockPath, { mode: 0o700 }),
      catch: (cause) => lockError(errno(cause) === "EEXIST" ? "busy" : "io"),
    }).pipe(Effect.result);
    if (attempt._tag === "Success") break;
    if (attempt.failure.reason !== "busy") return yield* attempt.failure;
    remainingAttempts -= 1;
    if (remainingAttempts <= 0) return yield* lockError("busy");
    yield* Effect.sleep("25 millis");
  }

  const directory = yield* node(() => NodeFSP.lstat(input.lockPath));
  if (!directory.isDirectory() || directory.isSymbolicLink()) {
    return yield* lockError("ownership-lost");
  }
  const metadataPath = `${input.lockPath}/owner.json`;
  const temporaryPath = `${input.lockPath}/owner.${NodeCrypto.randomUUID()}.tmp`;
  const metadata: LockMetadata = {
    schemaVersion: 2,
    ownerToken: NodeCrypto.randomUUID(),
    runtimeHolderId: input.runtimeHolderId,
    attemptId: NodeCrypto.randomUUID(),
    pid: process.pid,
    lockPath: input.lockPath,
    device: directory.dev,
    inode: directory.ino,
    acquiredAt: DateTime.formatIso(yield* DateTime.now),
  };
  const encoded = yield* encodeLockMetadata(metadata).pipe(Effect.mapError(() => lockError("io")));
  const setup = node(async () => {
    const file = await NodeFSP.open(temporaryPath, "wx", 0o600);
    try {
      await file.writeFile(`${encoded}\n`, "utf8");
      await file.sync();
    } finally {
      await file.close();
    }
    await NodeFSP.chmod(temporaryPath, 0o600);
    await NodeFSP.rename(temporaryPath, metadataPath);
    const lockDirectory = await NodeFSP.open(input.lockPath, "r");
    try {
      await lockDirectory.sync();
    } finally {
      await lockDirectory.close();
    }
  });
  const setupResult = yield* Effect.result(setup);
  if (setupResult._tag === "Failure") {
    const current = yield* Effect.promise(async () => {
      try {
        return await NodeFSP.lstat(input.lockPath);
      } catch {
        return null;
      }
    });
    if (
      current !== null &&
      current.isDirectory() &&
      current.dev === metadata.device &&
      current.ino === metadata.inode
    ) {
      yield* Effect.promise(async () => {
        try {
          await NodeFSP.rm(temporaryPath, { force: true });
          await NodeFSP.rm(metadataPath, { force: true });
          await NodeFSP.rmdir(input.lockPath);
        } catch {
          // Acquisition already failed; best-effort cleanup is limited to the
          // exact inode created by this attempt.
        }
      });
    }
    return yield* setupResult.failure;
  }
  return { metadata, metadataPath } satisfies LockClaim;
});

/**
 * Cross-process repository lock with owner-token and inode-bound release.
 *
 * Node does not expose an inode-conditional unlink. The final lstat followed by
 * unlink/rmdir therefore retains a small pathname TOCTOU window, but an old
 * finalizer never intentionally removes a replacement inode or foreign token,
 * and cleanup is never recursive.
 */
export const withAgentControlRepositoryLock = <A, E, R>(input: {
  readonly repositoryCommonDir: string;
  readonly runtimeHolderId: string;
  readonly timeoutMs?: number;
  readonly effect: Effect.Effect<A, E, R>;
}): Effect.Effect<A, E | AgentControlRepositoryLockError, R | Path.Path> =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const lockPath = path.join(input.repositoryCommonDir, "t3-agent-control.lock");
    return yield* Effect.acquireUseRelease(
      acquireLock({
        lockPath,
        runtimeHolderId: input.runtimeHolderId,
        timeoutMs: input.timeoutMs ?? 10_000,
      }),
      () => input.effect,
      (claim) => removeOwnedLock(claim),
    );
  });
