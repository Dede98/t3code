// @effect-diagnostics nodeBuiltinImport:off - host-wide locking needs node:sqlite, fs.watch, and fsync.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeTimersPromises from "node:timers/promises";
import * as NodeUtil from "node:util";
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

function isSqliteBusy(error: unknown): boolean {
  return typeof error === "object" && error !== null && "errcode" in error && error.errcode === 5;
}

async function beginImmediate(
  database: NodeSqlite.DatabaseSync,
  signal: AbortSignal,
): Promise<void> {
  while (true) {
    signal.throwIfAborted();
    try {
      database.exec("BEGIN IMMEDIATE");
      return;
    } catch (error) {
      if (!isSqliteBusy(error)) throw error;
      // node:sqlite is synchronous. A zero busy timeout plus an abortable,
      // bounded retry keeps cancellation responsive without blocking the
      // server event loop inside SQLite.
      await NodeTimersPromises.setTimeout(25, undefined, { signal });
    }
  }
}

function samePersistedState(
  previous: ResourceAdmissionLedgerState,
  candidate: ResourceAdmissionLedgerState,
): boolean {
  return NodeUtil.isDeepStrictEqual(previous, {
    ...candidate,
    revision: previous.revision,
    pressure: {
      ...candidate.pressure,
      // Sampling alone is not a capacity change. Persist the timestamp when a
      // pressure gate changes, but do not turn every retry into a wakeup.
      sampledAtMs: previous.pressure.sampledAtMs,
    },
  });
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
  const mutexPath = `${path}.mutex.sqlite`;

  const run = <A>(
    operation: string,
    use: (state: ResourceAdmissionLedgerState) => Promise<A>,
  ): Effect.Effect<A, HostBudgetLedgerError> =>
    Effect.tryPromise({
      try: async (signal) => {
        await NodeFSP.mkdir(NodePath.dirname(path), { recursive: true, mode: 0o700 });
        const database = new NodeSqlite.DatabaseSync(mutexPath);
        let transactionOpen = false;
        try {
          database.exec("PRAGMA busy_timeout = 0");
          await beginImmediate(database, signal);
          transactionOpen = true;
          const result = await use(await readLedgerFile(path));
          database.exec("COMMIT");
          transactionOpen = false;
          return result;
        } finally {
          if (transactionOpen) {
            try {
              database.exec("ROLLBACK");
            } catch {
              // Closing the native handle below releases the OS lock even if
              // SQLite has already rolled the transaction back.
            }
          }
          database.close();
        }
      },
      catch: (cause) => new HostBudgetLedgerError({ operation, cause }),
    });

  return {
    transact: (update) =>
      run("transact", async (state) => {
        const result = update(state);
        const candidate = compactTerminalReservations({
          ...result.state,
          revision: state.revision,
        });
        if (samePersistedState(state, candidate)) {
          return { value: result.value, revision: state.revision };
        }
        const nextState = { ...candidate, revision: state.revision + 1 };
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
          const candidate = compactTerminalReservations({
            ...result.state,
            revision: state.revision,
          });
          if (samePersistedState(state, candidate)) {
            return { value: result.value, revision: state.revision };
          }
          state = { ...candidate, revision: state.revision + 1 };
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
