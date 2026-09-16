// @effect-diagnostics nodeBuiltinImport:off - host-wide locking needs fs.watch, hard links, fsync, and PID liveness.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { ResourceAdmissionLedgerState, emptyResourceAdmissionLedgerState } from "./model.ts";

export class HostBudgetLedgerError extends Schema.TaggedError<HostBudgetLedgerError>()(
  "HostBudgetLedgerError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Host resource budget ledger operation '${this.operation}' failed.`;
  }
}

export interface HostBudgetLedger {
  readonly transact: <A>(
    update: (state: ResourceAdmissionLedgerState) => {
      readonly state: ResourceAdmissionLedgerState;
      readonly value: A;
    },
  ) => Effect.Effect<{ readonly value: A; readonly revision: number }, HostBudgetLedgerError>;
  readonly read: Effect.Effect<ResourceAdmissionLedgerState, HostBudgetLedgerError>;
  readonly awaitChange: (afterRevision: number) => Effect.Effect<void, HostBudgetLedgerError>;
}

const decodeState = Schema.decodeUnknownSync(ResourceAdmissionLedgerState);
export const MAX_RECENT_TERMINAL_RESERVATIONS = 256;

function compactTerminalReservations(
  state: ResourceAdmissionLedgerState,
): ResourceAdmissionLedgerState {
  const terminal = Object.values(state.reservations)
    .filter((reservation) => reservation.state === "canceled" || reservation.state === "released")
    .sort((left, right) => right.sequence - left.sequence);
  if (terminal.length <= MAX_RECENT_TERMINAL_RESERVATIONS) return state;
  const remove = new Set(
    terminal.slice(MAX_RECENT_TERMINAL_RESERVATIONS).map((reservation) => reservation.requestId),
  );
  const reservations = { ...state.reservations };
  for (const requestId of remove) delete reservations[requestId];
  return { ...state, reservations };
}

interface ProcessPathLock {
  readonly done: Promise<void>;
  readonly release: () => void;
}

const processPathLocks = new Map<string, ProcessPathLock>();

async function withProcessPathLock<A>(path: string, use: () => Promise<A>): Promise<A> {
  const previous = processPathLocks.get(path);
  let release: () => void = () => undefined;
  const current: ProcessPathLock = {
    done: new Promise<void>((resolve) => {
      release = resolve;
    }),
    release: () => release(),
  };
  processPathLocks.set(path, current);
  if (previous !== undefined) await previous.done;
  try {
    return await use();
  } finally {
    current.release();
    if (processPathLocks.get(path) === current) processPathLocks.delete(path);
  }
}

function processCanStillExist(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    );
  }
}

async function waitForLockChange(lockPath: string, signal: AbortSignal): Promise<void> {
  const directory = NodePath.dirname(lockPath);
  const basename = NodePath.basename(lockPath);
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    let watcher: NodeFS.FSWatcher | undefined;
    const finish = (error?: unknown) => {
      if (settled) return;
      settled = true;
      watcher?.close();
      signal.removeEventListener("abort", onAbort);
      if (error !== undefined) reject(error);
      else resolve();
    };
    const onAbort = () => finish(signal.reason ?? new Error("Lock wait aborted"));
    watcher = NodeFS.watch(directory, (_event, filename) => {
      if (filename === null || filename.toString() === basename) finish();
    });
    watcher.once("error", finish);
    signal.addEventListener("abort", onAbort, { once: true });
    void NodeFSP.access(lockPath).catch(() => finish());
  });
}

async function removeDeadOwnerLock(lockPath: string): Promise<boolean> {
  let contents: string;
  try {
    contents = await NodeFSP.readFile(lockPath, "utf8");
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      return true;
    }
    return false;
  }
  let owner: { readonly pid?: unknown };
  try {
    owner = JSON.parse(contents) as { readonly pid?: unknown };
  } catch {
    return false;
  }
  if (typeof owner.pid !== "number" || processCanStillExist(owner.pid)) return false;
  const tombstone = `${lockPath}.dead-${NodeCrypto.randomUUID()}`;
  try {
    await NodeFSP.rename(lockPath, tombstone);
    await NodeFSP.unlink(tombstone);
    return true;
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error.code === "ENOENT" || error.code === "EEXIST")
    ) {
      return true;
    }
    return false;
  }
}

async function acquireLock(lockPath: string, signal: AbortSignal): Promise<string> {
  const token = NodeCrypto.randomUUID();
  const candidatePath = `${lockPath}.candidate-${process.pid}-${token}`;
  const candidate = await NodeFSP.open(candidatePath, "wx", 0o600);
  try {
    await candidate.writeFile(JSON.stringify({ pid: process.pid, token }), "utf8");
    await candidate.sync();
  } finally {
    await candidate.close();
  }
  while (true) {
    try {
      signal.throwIfAborted();
    } catch (error) {
      await NodeFSP.unlink(candidatePath).catch(() => undefined);
      throw error;
    }
    try {
      await NodeFSP.link(candidatePath, lockPath);
      await NodeFSP.unlink(candidatePath);
      return token;
    } catch (error) {
      if (
        !(typeof error === "object" && error !== null && "code" in error && error.code === "EEXIST")
      ) {
        await NodeFSP.unlink(candidatePath).catch(() => undefined);
        throw error;
      }
      if (await removeDeadOwnerLock(lockPath)) continue;
      try {
        await waitForLockChange(lockPath, signal);
      } catch (waitError) {
        await NodeFSP.unlink(candidatePath).catch(() => undefined);
        throw waitError;
      }
    }
  }
}

async function releaseLock(lockPath: string, token: string): Promise<void> {
  try {
    const owner = JSON.parse(await NodeFSP.readFile(lockPath, "utf8")) as {
      readonly token?: unknown;
    };
    if (owner.token !== token) return;
    await NodeFSP.unlink(lockPath);
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      return;
    }
    throw error;
  }
}

async function readLedgerFile(path: string): Promise<ResourceAdmissionLedgerState> {
  try {
    const persisted = JSON.parse(await NodeFSP.readFile(path, "utf8")) as Record<string, unknown>;
    return decodeState({
      ...persisted,
      // Compatibility with ledgers written by the first shared-admission
      // build, before settings were registered per live environment.
      settingsRegistrations: persisted.settingsRegistrations ?? {},
    });
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") {
      return emptyResourceAdmissionLedgerState();
    }
    throw error;
  }
}

async function syncDirectory(directory: string): Promise<void> {
  try {
    const handle = await NodeFSP.open(directory, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch (error) {
    if (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error.code === "EINVAL" || error.code === "ENOTSUP" || error.code === "EISDIR")
    ) {
      return;
    }
    throw error;
  }
}

async function writeLedgerFile(path: string, state: ResourceAdmissionLedgerState): Promise<void> {
  const directory = NodePath.dirname(path);
  const temporaryPath = `${path}.${process.pid}.${NodeCrypto.randomUUID()}.tmp`;
  const handle = await NodeFSP.open(temporaryPath, "wx", 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(state)}\n`, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await NodeFSP.rename(temporaryPath, path);
    await syncDirectory(directory);
  } catch (error) {
    await NodeFSP.unlink(temporaryPath).catch(() => undefined);
    throw error;
  }
}

/**
 * Coordinates every server process that is configured with the same explicit
 * host path. Slots are never inferred from a per-environment database.
 */
export function makeFileHostBudgetLedger(path: string): HostBudgetLedger {
  const lockPath = `${path}.lock`;

  const run = <A>(
    operation: string,
    use: (state: ResourceAdmissionLedgerState) => Promise<A>,
  ): Effect.Effect<A, HostBudgetLedgerError> =>
    Effect.tryPromise({
      try: async (signal) => {
        return await withProcessPathLock(path, async () => {
          await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
          const token = await acquireLock(lockPath, signal);
          try {
            return await use(await readLedgerFile(path));
          } finally {
            await releaseLock(lockPath, token);
          }
        });
      },
      catch: (cause) => new HostBudgetLedgerError({ operation, cause }),
    });

  return {
    transact: (update) =>
      run("transact", async (state) => {
        const result = update(state);
        const nextState = compactTerminalReservations({
          ...result.state,
          revision: state.revision + 1,
        });
        await writeLedgerFile(path, nextState);
        return { value: result.value, revision: nextState.revision };
      }),
    read: run("read", async (state) => state),
    awaitChange: (afterRevision) =>
      Effect.tryPromise({
        try: async (signal) => {
          await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
          while (true) {
            signal.throwIfAborted();
            let closeWatch: () => void = () => undefined;
            const changed = new Promise<void>((resolve, reject) => {
              let settled = false;
              let watcher: NodeFS.FSWatcher | undefined;
              const finish = (error?: unknown) => {
                if (settled) return;
                settled = true;
                watcher?.close();
                signal.removeEventListener("abort", onAbort);
                if (error === undefined) resolve();
                else reject(error);
              };
              const onAbort = () => finish(signal.reason ?? new Error("Ledger wait aborted"));
              watcher = NodeFS.watch(NodePath.dirname(path), (_event, filename) => {
                if (filename === null || filename.toString() === NodePath.basename(path)) finish();
              });
              watcher.once("error", finish);
              signal.addEventListener("abort", onAbort, { once: true });
              closeWatch = () => finish();
            });
            const state = await readLedgerFile(path);
            if (state.revision > afterRevision) {
              closeWatch();
              return;
            }
            await changed;
          }
        },
        catch: (cause) => new HostBudgetLedgerError({ operation: "awaitChange", cause }),
      }),
  };
}

export const makeMemoryHostBudgetLedger = Effect.fn("makeMemoryHostBudgetLedger")(function* () {
  let state = emptyResourceAdmissionLedgerState();
  const mutex = yield* Semaphore.make(1);
  let changed = yield* Deferred.make<void>();
  return {
    transact: <A>(
      update: (current: ResourceAdmissionLedgerState) => {
        readonly state: ResourceAdmissionLedgerState;
        readonly value: A;
      },
    ) =>
      mutex.withPermits(1)(
        Effect.gen(function* () {
          const result = update(state);
          state = compactTerminalReservations({
            ...result.state,
            revision: state.revision + 1,
          });
          const previous = changed;
          changed = Deferred.makeUnsafe<void>();
          yield* Deferred.succeed(previous, undefined);
          return { value: result.value, revision: state.revision };
        }),
      ),
    read: mutex.withPermits(1)(Effect.sync(() => state)),
    awaitChange: (afterRevision) =>
      mutex
        .withPermits(1)(
          Effect.sync(() =>
            state.revision > afterRevision ? Effect.void : Deferred.await(changed),
          ),
        )
        .pipe(Effect.flatten),
  } satisfies HostBudgetLedger;
});
