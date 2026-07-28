/**
 * Port of `@effect/sql-sqlite-node` that uses the native `node:sqlite`
 * bindings instead of `better-sqlite3`.
 *
 * @module SqliteClient
 */
import * as NodeSqlite from "node:sqlite";

import * as Cache from "effect/Cache";
import * as Config from "effect/Config";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { identity } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Context from "effect/Context";
import * as Stream from "effect/Stream";
import * as Reactivity from "effect/unstable/reactivity/Reactivity";
import * as Client from "effect/unstable/sql/SqlClient";
import type { Connection } from "effect/unstable/sql/SqlConnection";
import { SqlError, classifySqliteError } from "effect/unstable/sql/SqlError";
import * as Statement from "effect/unstable/sql/Statement";

import { NodeSqliteTransactionHooks } from "./Services/NodeSqliteTransactionHooks.ts";

const ATTR_DB_SYSTEM_NAME = "db.system.name";

export const TypeId: TypeId = "~local/sqlite-node/SqliteClient";

export type TypeId = "~local/sqlite-node/SqliteClient";

type MaterializationCommitBoundary = "open" | "orchestration" | "coordinator";

interface MaterializationSavepointFrame {
  readonly name: string;
  readonly boundaryBeforeSavepoint: MaterializationCommitBoundary;
}

interface MaterializationStatementSnapshot {
  readonly wasInTransaction: boolean;
  readonly boundaryValid: boolean;
  readonly boundary: MaterializationCommitBoundary;
  readonly savepoints: ReadonlyArray<MaterializationSavepointFrame>;
}

type MaterializationStatement =
  | { readonly _tag: "none" }
  | { readonly _tag: "begin" }
  | { readonly _tag: "commit" }
  | { readonly _tag: "rollback" }
  | { readonly _tag: "savepoint"; readonly name: string }
  | { readonly _tag: "rollbackTo"; readonly name: string }
  | { readonly _tag: "release"; readonly name: string }
  | { readonly _tag: "orchestrationMarker" }
  | { readonly _tag: "coordinatorMarker" }
  | { readonly _tag: "unknown" };

interface SqlWordToken {
  readonly _tag: "word";
  readonly value: string;
}

interface SqlSymbolToken {
  readonly _tag: "symbol";
  readonly value: string;
}

interface SqlOpaqueToken {
  readonly _tag: "string" | "quotedIdentifier";
}

type SqlToken = SqlWordToken | SqlSymbolToken | SqlOpaqueToken;

const ORCHESTRATION_MARKER_TABLE = "ORCHESTRATION_AGENT_CONTROL_THREAD_MATERIALIZATION_RECEIPTS";
const COORDINATOR_MARKER_TABLE = "AGENT_CONTROL_CONTROLLED_THREAD_MATERIALIZATION_ACCEPTED";
const TRANSACTION_CONTROL_WORDS = new Set([
  "BEGIN",
  "COMMIT",
  "END",
  "ROLLBACK",
  "SAVEPOINT",
  "RELEASE",
]);

const lexFirstSqlStatement = (sql: string): ReadonlyArray<SqlToken> => {
  const tokens: Array<SqlToken> = [];
  let index = 0;

  const readQuoted = (quote: "'" | '"' | "`") => {
    index += 1;
    while (index < sql.length) {
      if (sql[index] !== quote) {
        index += 1;
        continue;
      }
      if (sql[index + 1] === quote) {
        index += 2;
        continue;
      }
      index += 1;
      tokens.push({ _tag: quote === "'" ? "string" : "quotedIdentifier" });
      return;
    }
    throw new Error(`unterminated SQL ${quote === "'" ? "string literal" : "quoted identifier"}`);
  };

  while (index < sql.length) {
    const character = sql[index]!;
    if (/\s/.test(character)) {
      index += 1;
      continue;
    }
    if (character === "-" && sql[index + 1] === "-") {
      index += 2;
      while (index < sql.length && sql[index] !== "\n" && sql[index] !== "\r") {
        index += 1;
      }
      continue;
    }
    if (character === "/" && sql[index + 1] === "*") {
      const end = sql.indexOf("*/", index + 2);
      if (end < 0) {
        throw new Error("unterminated SQL block comment");
      }
      index = end + 2;
      continue;
    }
    if (character === "'" || character === '"' || character === "`") {
      readQuoted(character);
      continue;
    }
    if (character === "[") {
      const end = sql.indexOf("]", index + 1);
      if (end < 0) {
        throw new Error("unterminated SQL bracket identifier");
      }
      index = end + 1;
      tokens.push({ _tag: "quotedIdentifier" });
      continue;
    }
    if (character === ";") {
      if (tokens.length === 0) {
        index += 1;
        continue;
      }
      break;
    }
    if (/[A-Za-z_]/.test(character)) {
      const start = index;
      index += 1;
      while (index < sql.length && /[A-Za-z0-9_$]/.test(sql[index]!)) {
        index += 1;
      }
      tokens.push({ _tag: "word", value: sql.slice(start, index).toUpperCase() });
      continue;
    }
    tokens.push({ _tag: "symbol", value: character });
    index += 1;
  }

  return tokens;
};

const isWord = (token: SqlToken | undefined, value?: string): token is SqlWordToken =>
  token?._tag === "word" && (value === undefined || token.value === value);

const parseMaterializationStatement = (sql: string): MaterializationStatement => {
  let tokens: ReadonlyArray<SqlToken>;
  try {
    tokens = lexFirstSqlStatement(sql);
  } catch {
    return { _tag: "unknown" };
  }

  const first = tokens[0];
  if (
    tokens.length >= 3 &&
    isWord(first, "INSERT") &&
    isWord(tokens[1], "INTO") &&
    isWord(tokens[2])
  ) {
    if (tokens[2].value === ORCHESTRATION_MARKER_TABLE) {
      return { _tag: "orchestrationMarker" };
    }
    if (tokens[2].value === COORDINATOR_MARKER_TABLE) {
      return { _tag: "coordinatorMarker" };
    }
  }

  if (tokens.length === 1 && isWord(first, "BEGIN")) {
    return { _tag: "begin" };
  }
  if (tokens.length === 2 && isWord(first, "BEGIN") && isWord(tokens[1], "TRANSACTION")) {
    return { _tag: "begin" };
  }
  if (tokens.length === 1 && isWord(first, "COMMIT")) {
    return { _tag: "commit" };
  }
  if (tokens.length === 2 && isWord(first, "COMMIT") && isWord(tokens[1], "TRANSACTION")) {
    return { _tag: "commit" };
  }
  if (tokens.length === 1 && isWord(first, "ROLLBACK")) {
    return { _tag: "rollback" };
  }
  if (tokens.length === 2 && isWord(first, "ROLLBACK") && isWord(tokens[1], "TRANSACTION")) {
    return { _tag: "rollback" };
  }
  if (tokens.length === 2 && isWord(first, "SAVEPOINT") && isWord(tokens[1])) {
    return { _tag: "savepoint", name: tokens[1].value.toLowerCase() };
  }
  if (
    tokens.length === 3 &&
    isWord(first, "ROLLBACK") &&
    isWord(tokens[1], "TO") &&
    isWord(tokens[2])
  ) {
    return { _tag: "rollbackTo", name: tokens[2].value.toLowerCase() };
  }
  if (
    tokens.length === 4 &&
    isWord(first, "ROLLBACK") &&
    isWord(tokens[1], "TO") &&
    isWord(tokens[2], "SAVEPOINT") &&
    isWord(tokens[3])
  ) {
    return { _tag: "rollbackTo", name: tokens[3].value.toLowerCase() };
  }
  if (tokens.length === 2 && isWord(first, "RELEASE") && isWord(tokens[1])) {
    return { _tag: "release", name: tokens[1].value.toLowerCase() };
  }
  if (
    tokens.length === 3 &&
    isWord(first, "RELEASE") &&
    isWord(tokens[1], "SAVEPOINT") &&
    isWord(tokens[2])
  ) {
    return { _tag: "release", name: tokens[2].value.toLowerCase() };
  }

  return isWord(first) && TRANSACTION_CONTROL_WORDS.has(first.value)
    ? { _tag: "unknown" }
    : { _tag: "none" };
};

export interface SqliteClientConfig {
  readonly filename: string;
  readonly readonly?: boolean | undefined;
  readonly allowExtension?: boolean | undefined;
  readonly prepareCacheSize?: number | undefined;
  readonly prepareCacheTTL?: Duration.Input | undefined;
  readonly spanAttributes?: Record<string, unknown> | undefined;
  readonly transformResultNames?: ((str: string) => string) | undefined;
  readonly transformQueryNames?: ((str: string) => string) | undefined;
}

export interface SqliteMemoryClientConfig extends Omit<
  SqliteClientConfig,
  "filename" | "readonly"
> {}

export class UnsupportedNodeSqliteVersionError extends Schema.TaggedErrorClass<UnsupportedNodeSqliteVersionError>()(
  "UnsupportedNodeSqliteVersionError",
  {
    nodeVersion: Schema.String,
    requirement: Schema.String,
  },
) {
  override get message(): string {
    return `Node.js ${this.nodeVersion} is missing required node:sqlite APIs. Upgrade to ${this.requirement}.`;
  }
}

export class UnsupportedNodeSqliteOperationError extends Schema.TaggedErrorClass<UnsupportedNodeSqliteOperationError>()(
  "UnsupportedNodeSqliteOperationError",
  {},
) {
  override get message(): string {
    return "Node SQLite does not support executeStream.";
  }
}

/**
 * Verify that the current Node.js version includes the `node:sqlite` APIs
 * used by `NodeSqliteClient` — specifically `StatementSync.columns()` (added
 * in Node 22.16.0 / 23.11.0).
 *
 * @see https://github.com/nodejs/node/pull/57490
 */
const checkNodeSqliteCompat = () => {
  const parts = process.versions.node.split(".").map(Number);
  const major = parts[0] ?? 0;
  const minor = parts[1] ?? 0;
  const supported = (major === 22 && minor >= 16) || (major === 23 && minor >= 11) || major >= 24;

  if (!supported) {
    return Effect.die(
      new UnsupportedNodeSqliteVersionError({
        nodeVersion: process.versions.node,
        requirement: "Node.js >=22.16, >=23.11, or >=24",
      }),
    );
  }
  return Effect.void;
};

const makeWithDatabase = Effect.fn("makeWithDatabase")(function* (
  options: SqliteClientConfig,
  openDatabase: () => NodeSqlite.DatabaseSync,
): Effect.fn.Return<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> {
  yield* checkNodeSqliteCompat();

  const compiler = Statement.makeCompilerSqlite(options.transformQueryNames);
  const transformRows = options.transformResultNames
    ? Statement.defaultTransforms(options.transformResultNames).array
    : undefined;

  const makeConnection = Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const db = yield* Effect.try({
      try: openDatabase,
      catch: (cause) =>
        new SqlError({
          reason: classifySqliteError(cause, {
            message: "Failed to open database",
            operation: "open",
          }),
        }),
    });
    yield* Scope.addFinalizer(
      scope,
      Effect.try({
        try: () => db.close(),
        catch: (cause) =>
          new SqlError({
            reason: classifySqliteError(cause, {
              message: "Failed to close database",
              operation: "close",
            }),
          }),
      }).pipe(Effect.orDie),
    );

    const statementReaderCache = new WeakMap<NodeSqlite.StatementSync, boolean>();
    let materializationCommitBoundary: MaterializationCommitBoundary = "open";
    const materializationSavepoints: Array<MaterializationSavepointFrame> = [];
    let materializationBoundaryValid = true;
    const resetMaterializationCommitState = () => {
      materializationCommitBoundary = "open";
      materializationSavepoints.length = 0;
      materializationBoundaryValid = true;
    };
    const findMaterializationSavepoint = (name: string): number => {
      for (let index = materializationSavepoints.length - 1; index >= 0; index -= 1) {
        if (materializationSavepoints[index]?.name === name) {
          return index;
        }
      }
      return -1;
    };
    const snapshotMaterializationStatement = (): MaterializationStatementSnapshot => ({
      wasInTransaction: db.isTransaction,
      boundaryValid: materializationBoundaryValid,
      boundary: materializationCommitBoundary,
      savepoints: materializationSavepoints.slice(),
    });
    const ensureMaterializationCommitBoundary = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ) => {
      if (statement._tag === "unknown") {
        if (snapshot.wasInTransaction) {
          materializationBoundaryValid = false;
        }
        throw new Error(
          "unsupported transaction-control statement at controlled thread materialization boundary",
        );
      }
      if (!snapshot.wasInTransaction) {
        return;
      }
      if (!snapshot.boundaryValid) {
        if (statement._tag === "rollback") {
          return;
        }
        throw new Error(
          "controlled thread materialization boundary is invalid after transaction-control failure",
        );
      }
      if (
        statement._tag === "begin" ||
        statement._tag === "commit" ||
        statement._tag === "rollback" ||
        statement._tag === "savepoint" ||
        statement._tag === "rollbackTo" ||
        statement._tag === "release"
      ) {
        return;
      }
      const coordinatorHandoff =
        snapshot.boundary === "orchestration" && statement._tag === "coordinatorMarker";
      if (snapshot.boundary !== "open" && !coordinatorHandoff) {
        materializationBoundaryValid = false;
        throw new Error(
          "controlled thread materialization marker must be the final transaction statement",
        );
      }
    };
    const updateMaterializationCommitBoundary = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ): boolean => {
      const transactionEnded = snapshot.wasInTransaction && !db.isTransaction;
      const committed =
        transactionEnded && (statement._tag === "commit" || statement._tag === "release");
      if (transactionEnded) {
        resetMaterializationCommitState();
        return committed && snapshot.boundaryValid && snapshot.boundary === "coordinator";
      }

      switch (statement._tag) {
        case "begin": {
          resetMaterializationCommitState();
          return false;
        }
        case "savepoint": {
          materializationSavepoints.push({
            name: statement.name,
            boundaryBeforeSavepoint: snapshot.boundary,
          });
          return false;
        }
        case "rollbackTo": {
          const savepointIndex = findMaterializationSavepoint(statement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(`untracked materialization savepoint rollback: ${statement.name}`);
          }
          materializationCommitBoundary =
            materializationSavepoints[savepointIndex]!.boundaryBeforeSavepoint;
          materializationSavepoints.length = savepointIndex + 1;
          return false;
        }
        case "release": {
          const savepointIndex = findMaterializationSavepoint(statement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(`untracked materialization savepoint release: ${statement.name}`);
          }
          materializationSavepoints.length = savepointIndex;
          return false;
        }
        case "commit":
        case "rollback": {
          resetMaterializationCommitState();
          return false;
        }
        case "unknown": {
          materializationBoundaryValid = false;
          return false;
        }
        case "orchestrationMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "orchestration";
          }
          break;
        }
        case "coordinatorMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "coordinator";
          }
          break;
        }
        case "none":
          break;
      }
      if (!db.isTransaction) {
        resetMaterializationCommitState();
      }
      return false;
    };
    const handleMaterializationStatementFailure = (statement: MaterializationStatement) => {
      if (
        db.isTransaction &&
        (statement._tag === "savepoint" ||
          statement._tag === "rollbackTo" ||
          statement._tag === "release" ||
          statement._tag === "orchestrationMarker" ||
          statement._tag === "coordinatorMarker" ||
          statement._tag === "unknown")
      ) {
        materializationBoundaryValid = false;
      }
      if (statement._tag === "commit") {
        if (db.isTransaction) {
          try {
            db.exec("ROLLBACK");
          } catch {
            // Preserve the original commit failure. The connection will
            // remain unavailable until its owning scope is closed.
          }
        }
        resetMaterializationCommitState();
        if (db.isTransaction) {
          materializationBoundaryValid = false;
        }
      } else if (statement._tag === "rollback") {
        if (db.isTransaction) {
          try {
            db.exec("ROLLBACK");
          } catch {
            // Preserve the original rollback failure. The connection will
            // remain unavailable until its owning scope is closed.
          }
        }
        resetMaterializationCommitState();
        if (db.isTransaction) {
          materializationBoundaryValid = false;
        }
      }
    };
    const makeExecutionError = (cause: unknown) =>
      new SqlError({
        reason: classifySqliteError(cause, {
          message: "Failed to execute statement",
          operation: "execute",
        }),
      });
    const validateSqlBeforePrepare = (sql: string) =>
      Effect.try({
        try: () => {
          const statement = parseMaterializationStatement(sql);
          if (statement._tag === "unknown") {
            handleMaterializationStatementFailure(statement);
            throw new Error(
              "unsupported transaction-control statement at controlled thread materialization boundary",
            );
          }
        },
        catch: makeExecutionError,
      });
    const hasRows = (statement: NodeSqlite.StatementSync): boolean => {
      const cached = statementReaderCache.get(statement);
      if (cached !== undefined) {
        return cached;
      }
      const value = statement.columns().length > 0;
      statementReaderCache.set(statement, value);
      return value;
    };

    const prepareCache = yield* Cache.make({
      capacity: options.prepareCacheSize ?? 200,
      timeToLive: options.prepareCacheTTL ?? Duration.minutes(10),
      lookup: (sql: string) =>
        Effect.try({
          try: () => db.prepare(sql),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to prepare statement",
                operation: "prepare",
              }),
            }),
        }),
    });

    const runStatement = <A>(
      statement: NodeSqlite.StatementSync,
      params: ReadonlyArray<unknown>,
      execute: (statement: NodeSqlite.StatementSync, params: ReadonlyArray<unknown>) => A,
    ) =>
      Effect.withFiber<A, SqlError>((fiber) => {
        const materializationStatement = parseMaterializationStatement(statement.sourceSQL);
        const snapshot = snapshotMaterializationStatement();
        try {
          ensureMaterializationCommitBoundary(materializationStatement, snapshot);
          statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
          const result = execute(statement, params);
          const runCoordinatorHook = updateMaterializationCommitBoundary(
            materializationStatement,
            snapshot,
          );
          return runCoordinatorHook
            ? Context.get(fiber.context, NodeSqliteTransactionHooks)
                .afterCommitBeforeReturn({
                  boundary: "agent-control-controlled-thread-materialization-coordinator",
                })
                .pipe(Effect.as(result))
            : Effect.succeed(result);
        } catch (cause) {
          handleMaterializationStatementFailure(materializationStatement);
          return Effect.fail(makeExecutionError(cause));
        }
      });

    const prepareCached = (sql: string) =>
      Effect.andThen(validateSqlBeforePrepare(sql), Cache.get(prepareCache, sql)).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            handleMaterializationStatementFailure(parseMaterializationStatement(sql));
          }),
        ),
      );
    const prepareUncached = (sql: string) =>
      Effect.andThen(
        validateSqlBeforePrepare(sql),
        Effect.try({
          try: () => db.prepare(sql),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to prepare statement",
                operation: "prepare",
              }),
            }),
        }),
      ).pipe(
        Effect.tapError(() =>
          Effect.sync(() => {
            handleMaterializationStatementFailure(parseMaterializationStatement(sql));
          }),
        ),
      );

    const run = (sql: string, params: ReadonlyArray<unknown>, raw = false) =>
      Effect.flatMap(prepareCached(sql), (statement) =>
        runStatement(statement, params, (statement, params) => {
          if (hasRows(statement)) {
            return statement.all(...(params as any));
          }
          const result = statement.run(...(params as any));
          return raw ? (result as unknown as ReadonlyArray<any>) : [];
        }),
      );

    const runValues = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.acquireUseRelease(
        prepareCached(sql),
        (statement) =>
          runStatement(statement, params, (statement, params) => {
            if (hasRows(statement)) {
              statement.setReturnArrays(true);
              return statement.all(...(params as any)) as unknown as ReadonlyArray<
                ReadonlyArray<unknown>
              >;
            }
            statement.run(...(params as any));
            return [];
          }),
        (statement) =>
          Effect.try({
            try: () => {
              if (hasRows(statement)) {
                statement.setReturnArrays(false);
              }
            },
            catch: (cause) =>
              new SqlError({
                reason: classifySqliteError(cause, {
                  message: "Failed to reset statement result mode",
                  operation: "resetResultMode",
                }),
              }),
          }).pipe(Effect.orDie),
      );

    return identity<Connection>({
      execute(sql, params, rowTransform) {
        return rowTransform ? Effect.map(run(sql, params), rowTransform) : run(sql, params);
      },
      executeRaw(sql, params) {
        return run(sql, params, true);
      },
      executeValues(sql, params) {
        return runValues(sql, params);
      },
      executeUnprepared(sql, params, rowTransform) {
        const effect = prepareUncached(sql).pipe(
          Effect.flatMap((statement) =>
            runStatement(statement, params ?? [], (statement, params) => {
              if (hasRows(statement)) {
                return statement.all(...(params as any));
              }
              statement.run(...(params as any));
              return [];
            }),
          ),
        );
        return rowTransform ? Effect.map(effect, rowTransform) : effect;
      },
      executeStream(_sql, _params) {
        return Stream.die(new UnsupportedNodeSqliteOperationError());
      },
    });
  });

  const semaphore = yield* Semaphore.make(1);
  const connection = yield* makeConnection;

  const acquirer = semaphore.withPermits(1)(Effect.succeed(connection));
  const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
    const fiber = Fiber.getCurrent()!;
    const scope = Context.getUnsafe(fiber.context, Scope.Scope);
    return Effect.as(
      Effect.tap(restore(semaphore.take(1)), () => Scope.addFinalizer(scope, semaphore.release(1))),
      connection,
    );
  });

  return yield* Client.make({
    acquirer,
    compiler,
    transactionAcquirer,
    spanAttributes: [
      ...(options.spanAttributes ? Object.entries(options.spanAttributes) : []),
      [ATTR_DB_SYSTEM_NAME, "sqlite"],
    ],
    transformRows,
  });
});

const make = (
  options: SqliteClientConfig,
): Effect.Effect<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  makeWithDatabase(
    options,
    () =>
      new NodeSqlite.DatabaseSync(options.filename, {
        readOnly: options.readonly ?? false,
        allowExtension: options.allowExtension ?? false,
      }),
  );

const makeMemory = (
  config: SqliteMemoryClientConfig = {},
): Effect.Effect<Client.SqlClient, SqlError, Scope.Scope | Reactivity.Reactivity> =>
  makeWithDatabase(
    {
      ...config,
      filename: ":memory:",
      readonly: false,
    },
    () => {
      const database = new NodeSqlite.DatabaseSync(":memory:", {
        allowExtension: config.allowExtension ?? false,
      });
      return database;
    },
  );

export const layerConfig = (
  config: Config.Wrap<SqliteClientConfig>,
): Layer.Layer<Client.SqlClient, Config.ConfigError | SqlError> =>
  Layer.effect(Client.SqlClient, Config.unwrap(config).pipe(Effect.flatMap(make))).pipe(
    Layer.provide(Reactivity.layer),
  );

export const layer = (config: SqliteClientConfig): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, make(config)).pipe(Layer.provide(Reactivity.layer));

export const layerMemory = (
  config: SqliteMemoryClientConfig = {},
): Layer.Layer<Client.SqlClient, SqlError> =>
  Layer.effect(Client.SqlClient, makeMemory(config)).pipe(Layer.provide(Reactivity.layer));
