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

type TransactionControlStatement =
  | { readonly _tag: "none" }
  | { readonly _tag: "begin" }
  | { readonly _tag: "commit" }
  | { readonly _tag: "rollback" }
  | { readonly _tag: "savepoint"; readonly name: string }
  | { readonly _tag: "rollbackTo"; readonly name: string }
  | { readonly _tag: "release"; readonly name: string }
  | { readonly _tag: "unknown" };

const EFFECT_SQL_SAVEPOINT_NAME = "(effect_sql_[0-9]+)";
const TRANSACTION_CONTROL_PREFIX = /^\s*(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b/i;

const parseTransactionControlStatement = (sql: string): TransactionControlStatement => {
  if (/^\s*BEGIN(?:\s+TRANSACTION)?\s*;?\s*$/i.test(sql)) {
    return { _tag: "begin" };
  }
  if (/^\s*(?:COMMIT|END)(?:\s+TRANSACTION)?\s*;?\s*$/i.test(sql)) {
    return { _tag: "commit" };
  }
  if (/^\s*ROLLBACK(?:\s+TRANSACTION)?\s*;?\s*$/i.test(sql)) {
    return { _tag: "rollback" };
  }

  const savepoint = new RegExp(
    `^\\s*SAVEPOINT\\s+${EFFECT_SQL_SAVEPOINT_NAME}\\s*;?\\s*$`,
    "i",
  ).exec(sql);
  if (savepoint?.[1] !== undefined) {
    return { _tag: "savepoint", name: savepoint[1].toLowerCase() };
  }

  const rollbackTo = new RegExp(
    `^\\s*ROLLBACK(?:\\s+TRANSACTION)?\\s+TO(?:\\s+SAVEPOINT)?\\s+${EFFECT_SQL_SAVEPOINT_NAME}\\s*;?\\s*$`,
    "i",
  ).exec(sql);
  if (rollbackTo?.[1] !== undefined) {
    return { _tag: "rollbackTo", name: rollbackTo[1].toLowerCase() };
  }

  const release = new RegExp(
    `^\\s*RELEASE(?:\\s+SAVEPOINT)?\\s+${EFFECT_SQL_SAVEPOINT_NAME}\\s*;?\\s*$`,
    "i",
  ).exec(sql);
  if (release?.[1] !== undefined) {
    return { _tag: "release", name: release[1].toLowerCase() };
  }

  return TRANSACTION_CONTROL_PREFIX.test(sql) ? { _tag: "unknown" } : { _tag: "none" };
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
    const isOrchestrationMaterializationMarkerInsert = (sql: string): boolean =>
      /\bINSERT\s+INTO\s+orchestration_agent_control_thread_materialization_receipts\b/i.test(sql);
    const isCoordinatorMaterializationMarkerInsert = (sql: string): boolean =>
      /\bINSERT\s+INTO\s+agent_control_controlled_thread_materialization_accepted\b/i.test(sql);
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
    const ensureMaterializationCommitBoundary = (
      sql: string,
      control: TransactionControlStatement,
    ) => {
      if (!db.isTransaction) {
        return;
      }
      if (!materializationBoundaryValid) {
        if (control._tag === "rollback") {
          return;
        }
        throw new Error(
          "controlled thread materialization boundary is invalid after transaction-control failure",
        );
      }
      if (control._tag !== "none") {
        if (control._tag === "unknown") {
          throw new Error(
            "unsupported transaction-control statement at controlled thread materialization boundary",
          );
        }
        return;
      }
      const coordinatorHandoff =
        materializationCommitBoundary === "orchestration" &&
        isCoordinatorMaterializationMarkerInsert(sql);
      if (materializationCommitBoundary !== "open" && !coordinatorHandoff) {
        throw new Error(
          "controlled thread materialization marker must be the final transaction statement",
        );
      }
    };
    const updateMaterializationCommitBoundary = (
      sql: string,
      control: TransactionControlStatement,
    ) => {
      switch (control._tag) {
        case "begin": {
          resetMaterializationCommitState();
          return;
        }
        case "savepoint": {
          materializationSavepoints.push({
            name: control.name,
            boundaryBeforeSavepoint: materializationCommitBoundary,
          });
          return;
        }
        case "rollbackTo": {
          const savepointIndex = findMaterializationSavepoint(control.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(`untracked materialization savepoint rollback: ${control.name}`);
          }
          materializationCommitBoundary =
            materializationSavepoints[savepointIndex]!.boundaryBeforeSavepoint;
          materializationSavepoints.length = savepointIndex + 1;
          return;
        }
        case "release": {
          const savepointIndex = findMaterializationSavepoint(control.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(`untracked materialization savepoint release: ${control.name}`);
          }
          materializationSavepoints.length = savepointIndex;
          if (!db.isTransaction) {
            resetMaterializationCommitState();
          }
          return;
        }
        case "commit":
        case "rollback": {
          resetMaterializationCommitState();
          return;
        }
        case "unknown": {
          materializationBoundaryValid = false;
          return;
        }
        case "none": {
          break;
        }
      }
      if (isOrchestrationMaterializationMarkerInsert(sql) && db.isTransaction) {
        materializationCommitBoundary = "orchestration";
      }
      if (isCoordinatorMaterializationMarkerInsert(sql) && db.isTransaction) {
        materializationCommitBoundary = "coordinator";
      }
      if (!db.isTransaction) {
        resetMaterializationCommitState();
      }
    };
    const handleMaterializationStatementFailure = (control: TransactionControlStatement) => {
      if (
        db.isTransaction &&
        (control._tag === "savepoint" ||
          control._tag === "rollbackTo" ||
          control._tag === "release" ||
          control._tag === "unknown")
      ) {
        materializationBoundaryValid = false;
      }
      if (control._tag === "commit") {
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
      } else if (control._tag === "rollback") {
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

    const runStatement = (
      statement: NodeSqlite.StatementSync,
      params: ReadonlyArray<unknown>,
      raw: boolean,
    ) =>
      Effect.withFiber<ReadonlyArray<any>, SqlError>((fiber) => {
        const control = parseTransactionControlStatement(statement.sourceSQL);
        const completingCoordinatorMaterialization =
          materializationBoundaryValid &&
          materializationCommitBoundary === "coordinator" &&
          control._tag === "commit";
        try {
          ensureMaterializationCommitBoundary(statement.sourceSQL, control);
          statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
          if (hasRows(statement)) {
            const rows = statement.all(...(params as any));
            updateMaterializationCommitBoundary(statement.sourceSQL, control);
            return Effect.succeed(rows);
          }
          const result = statement.run(...(params as any));
          updateMaterializationCommitBoundary(statement.sourceSQL, control);
          const rows = raw ? (result as unknown as ReadonlyArray<any>) : [];
          return completingCoordinatorMaterialization
            ? Context.get(fiber.context, NodeSqliteTransactionHooks)
                .afterCommitBeforeReturn({
                  boundary: "agent-control-controlled-thread-materialization-coordinator",
                })
                .pipe(Effect.as(rows))
            : Effect.succeed(rows);
        } catch (cause) {
          handleMaterializationStatementFailure(control);
          return Effect.fail(
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to execute statement",
                operation: "execute",
              }),
            }),
          );
        }
      });

    const run = (sql: string, params: ReadonlyArray<unknown>, raw = false) =>
      Effect.flatMap(Cache.get(prepareCache, sql), (s) => runStatement(s, params, raw));

    const runValues = (sql: string, params: ReadonlyArray<unknown>) =>
      Effect.acquireUseRelease(
        Cache.get(prepareCache, sql),
        (statement) =>
          Effect.withFiber<ReadonlyArray<ReadonlyArray<unknown>>, SqlError>((fiber) => {
            const control = parseTransactionControlStatement(sql);
            const completingCoordinatorMaterialization =
              materializationBoundaryValid &&
              materializationCommitBoundary === "coordinator" &&
              control._tag === "commit";
            try {
              ensureMaterializationCommitBoundary(sql, control);
              statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
              if (hasRows(statement)) {
                statement.setReturnArrays(true);
                const rows = statement.all(...(params as any)) as unknown as ReadonlyArray<
                  ReadonlyArray<unknown>
                >;
                updateMaterializationCommitBoundary(sql, control);
                return Effect.succeed(rows);
              }
              statement.run(...(params as any));
              updateMaterializationCommitBoundary(sql, control);
              const rows: ReadonlyArray<ReadonlyArray<unknown>> = [];
              return completingCoordinatorMaterialization
                ? Context.get(fiber.context, NodeSqliteTransactionHooks)
                    .afterCommitBeforeReturn({
                      boundary: "agent-control-controlled-thread-materialization-coordinator",
                    })
                    .pipe(Effect.as(rows))
                : Effect.succeed(rows);
            } catch (cause) {
              handleMaterializationStatementFailure(control);
              return Effect.fail(
                new SqlError({
                  reason: classifySqliteError(cause, {
                    message: "Failed to execute statement",
                    operation: "execute",
                  }),
                }),
              );
            }
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
        const effect = Effect.try({
          try: () => db.prepare(sql),
          catch: (cause) =>
            new SqlError({
              reason: classifySqliteError(cause, {
                message: "Failed to prepare statement",
                operation: "prepare",
              }),
            }),
        }).pipe(Effect.flatMap((statement) => runStatement(statement, params ?? [], false)));
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
