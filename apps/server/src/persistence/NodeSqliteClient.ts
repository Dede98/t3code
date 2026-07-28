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

interface SqlKeywordToken {
  readonly _tag: "keyword";
  readonly value: string;
}

interface SqlIdentifierToken {
  readonly _tag:
    | "identifier"
    | "doubleQuotedIdentifier"
    | "backtickIdentifier"
    | "bracketIdentifier";
  readonly value: string;
}

interface SqlStringToken {
  readonly _tag: "string";
}

interface SqlParameterToken {
  readonly _tag: "parameter";
}

interface SqlPunctuationToken {
  readonly _tag: "dot" | "comma" | "openParenthesis" | "closeParenthesis" | "semicolon";
}

interface SqlOperatorToken {
  readonly _tag: "operator";
  readonly value: string;
}

type SqlToken =
  | SqlKeywordToken
  | SqlIdentifierToken
  | SqlStringToken
  | SqlParameterToken
  | SqlPunctuationToken
  | SqlOperatorToken;

const ORCHESTRATION_MARKER_TABLE = "orchestration_agent_control_thread_materialization_receipts";
const COORDINATOR_MARKER_TABLE = "agent_control_controlled_thread_materialization_accepted";
const SQL_KEYWORDS = new Set([
  "ABORT",
  "AS",
  "BEGIN",
  "COMMIT",
  "DELETE",
  "END",
  "FAIL",
  "IGNORE",
  "INSERT",
  "INTO",
  "MATERIALIZED",
  "NOT",
  "OR",
  "RECURSIVE",
  "REPLACE",
  "ROLLBACK",
  "SAVEPOINT",
  "RELEASE",
  "SELECT",
  "TO",
  "TRANSACTION",
  "UPDATE",
  "WITH",
]);
const TRANSACTION_CONTROL_KEYWORDS = new Set([
  "BEGIN",
  "COMMIT",
  "END",
  "ROLLBACK",
  "SAVEPOINT",
  "RELEASE",
]);
const INSERT_CONFLICT_ALGORITHMS = new Set(["ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"]);

const isSqlIdentifierStart = (character: string): boolean =>
  /[A-Za-z_]/.test(character) || character.charCodeAt(0) >= 0x80;

const isSqlIdentifierContinue = (character: string): boolean =>
  /[A-Za-z0-9_$]/.test(character) || character.charCodeAt(0) >= 0x80;

const lexFirstSqlStatement = (sql: string): ReadonlyArray<SqlToken> => {
  const tokens: Array<SqlToken> = [];
  let index = 0;

  const readString = () => {
    index += 1;
    while (index < sql.length) {
      if (sql[index] !== "'") {
        index += 1;
        continue;
      }
      if (sql[index + 1] === "'") {
        index += 2;
        continue;
      }
      index += 1;
      tokens.push({ _tag: "string" });
      return;
    }
    throw new Error("unterminated SQL string literal");
  };

  const readQuotedIdentifier = (
    quote: '"' | "`",
    tag: "doubleQuotedIdentifier" | "backtickIdentifier",
  ) => {
    let value = "";
    index += 1;
    while (index < sql.length) {
      const character = sql[index]!;
      if (character !== quote) {
        value += character;
        index += 1;
        continue;
      }
      if (sql[index + 1] === quote) {
        value += quote;
        index += 2;
        continue;
      }
      index += 1;
      tokens.push({ _tag: tag, value });
      return;
    }
    throw new Error("unterminated SQL quoted identifier");
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
    if (character === "'") {
      readString();
      continue;
    }
    if (character === '"') {
      readQuotedIdentifier(character, "doubleQuotedIdentifier");
      continue;
    }
    if (character === "`") {
      readQuotedIdentifier(character, "backtickIdentifier");
      continue;
    }
    if (character === "[") {
      const end = sql.indexOf("]", index + 1);
      if (end < 0) {
        throw new Error("unterminated SQL bracket identifier");
      }
      const value = sql.slice(index + 1, end);
      index = end + 1;
      tokens.push({ _tag: "bracketIdentifier", value });
      continue;
    }
    if (character === ";") {
      if (tokens.length === 0) {
        index += 1;
        continue;
      }
      tokens.push({ _tag: "semicolon" });
      break;
    }
    if (character === ".") {
      tokens.push({ _tag: "dot" });
      index += 1;
      continue;
    }
    if (character === ",") {
      tokens.push({ _tag: "comma" });
      index += 1;
      continue;
    }
    if (character === "(") {
      tokens.push({ _tag: "openParenthesis" });
      index += 1;
      continue;
    }
    if (character === ")") {
      tokens.push({ _tag: "closeParenthesis" });
      index += 1;
      continue;
    }
    if (
      character === "?" ||
      ((character === ":" || character === "@" || character === "$") &&
        sql[index + 1] !== undefined &&
        isSqlIdentifierStart(sql[index + 1]!))
    ) {
      index += 1;
      while (index < sql.length && isSqlIdentifierContinue(sql[index]!)) {
        index += 1;
      }
      tokens.push({ _tag: "parameter" });
      continue;
    }
    if (isSqlIdentifierStart(character)) {
      const start = index;
      index += 1;
      while (index < sql.length && isSqlIdentifierContinue(sql[index]!)) {
        index += 1;
      }
      const value = sql.slice(start, index);
      const upper = value.toUpperCase();
      tokens.push(
        SQL_KEYWORDS.has(upper) ? { _tag: "keyword", value: upper } : { _tag: "identifier", value },
      );
      continue;
    }
    tokens.push({ _tag: "operator", value: character });
    index += 1;
  }

  return tokens;
};

const isKeyword = (token: SqlToken | undefined, value?: string): token is SqlKeywordToken =>
  token?._tag === "keyword" && (value === undefined || token.value === value);

const isIdentifier = (token: SqlToken | undefined): token is SqlIdentifierToken =>
  token?._tag === "identifier" ||
  token?._tag === "doubleQuotedIdentifier" ||
  token?._tag === "backtickIdentifier" ||
  token?._tag === "bracketIdentifier";

const normalizedIdentifier = (token: SqlIdentifierToken): string => token.value.toLowerCase();

const unquotedName = (token: SqlToken | undefined): string | undefined => {
  if (token?._tag === "identifier") {
    return token.value.toLowerCase();
  }
  if (token?._tag === "keyword") {
    return token.value.toLowerCase();
  }
  return undefined;
};

const skipParenthesizedTokens = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): number | undefined => {
  if (tokens[start]?._tag !== "openParenthesis") {
    return undefined;
  }
  let depth = 0;
  for (let index = start; index < tokens.length; index += 1) {
    const token = tokens[index]!;
    if (token._tag === "openParenthesis") {
      depth += 1;
    } else if (token._tag === "closeParenthesis") {
      depth -= 1;
      if (depth === 0) {
        return index + 1;
      }
    } else if (token._tag === "semicolon") {
      return undefined;
    }
  }
  return undefined;
};

const parseWithPrefix = (tokens: ReadonlyArray<SqlToken>, start: number): number | undefined => {
  let index = start;
  if (!isKeyword(tokens[index], "WITH")) {
    return undefined;
  }
  index += 1;
  if (isKeyword(tokens[index], "RECURSIVE")) {
    index += 1;
  }

  while (index < tokens.length) {
    if (!isIdentifier(tokens[index])) {
      return undefined;
    }
    index += 1;

    if (tokens[index]?._tag === "openParenthesis") {
      const afterColumns = skipParenthesizedTokens(tokens, index);
      if (afterColumns === undefined) {
        return undefined;
      }
      index = afterColumns;
    }

    if (!isKeyword(tokens[index], "AS")) {
      return undefined;
    }
    index += 1;
    if (isKeyword(tokens[index], "MATERIALIZED")) {
      index += 1;
    } else if (isKeyword(tokens[index], "NOT") && isKeyword(tokens[index + 1], "MATERIALIZED")) {
      index += 2;
    }

    const afterBody = skipParenthesizedTokens(tokens, index);
    if (afterBody === undefined) {
      return undefined;
    }
    index = afterBody;
    if (tokens[index]?._tag !== "comma") {
      return index;
    }
    index += 1;
  }
  return undefined;
};

const parseInsertTarget = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): MaterializationStatement => {
  let index = start;
  if (isKeyword(tokens[index], "INSERT")) {
    index += 1;
    if (isKeyword(tokens[index], "OR")) {
      const conflictAlgorithm = tokens[index + 1];
      if (
        !isKeyword(conflictAlgorithm) ||
        !INSERT_CONFLICT_ALGORITHMS.has(conflictAlgorithm.value)
      ) {
        return { _tag: "unknown" };
      }
      index += 2;
    }
  } else if (isKeyword(tokens[index], "REPLACE")) {
    index += 1;
  } else {
    return { _tag: "none" };
  }

  if (!isKeyword(tokens[index], "INTO")) {
    return { _tag: "unknown" };
  }
  index += 1;

  const firstIdentifier = tokens[index];
  if (!isIdentifier(firstIdentifier)) {
    return { _tag: "unknown" };
  }
  let schema: string | undefined;
  let table = normalizedIdentifier(firstIdentifier);
  index += 1;

  if (tokens[index]?._tag === "dot") {
    const tableIdentifier = tokens[index + 1];
    if (!isIdentifier(tableIdentifier)) {
      return { _tag: "unknown" };
    }
    schema = table;
    table = normalizedIdentifier(tableIdentifier);
    index += 2;
    if (tokens[index]?._tag === "dot") {
      return { _tag: "unknown" };
    }
  }

  if (schema !== undefined && schema !== "main") {
    return { _tag: "none" };
  }
  if (table === ORCHESTRATION_MARKER_TABLE) {
    return { _tag: "orchestrationMarker" };
  }
  if (table === COORDINATOR_MARKER_TABLE) {
    return { _tag: "coordinatorMarker" };
  }
  return { _tag: "none" };
};

const parseMaterializationStatement = (sql: string): MaterializationStatement => {
  let tokens: ReadonlyArray<SqlToken>;
  try {
    tokens = lexFirstSqlStatement(sql);
  } catch {
    return { _tag: "unknown" };
  }
  const semicolonIndex = tokens.findIndex((token) => token._tag === "semicolon");
  if (semicolonIndex >= 0) {
    tokens = tokens.slice(0, semicolonIndex);
  }

  const first = tokens[0];
  const firstKeyword = first?._tag === "keyword" ? first.value : undefined;
  if (isKeyword(first, "INSERT") || isKeyword(first, "REPLACE")) {
    return parseInsertTarget(tokens, 0);
  }
  if (isKeyword(first, "WITH")) {
    const statementStart = parseWithPrefix(tokens, 0);
    if (statementStart === undefined) {
      return { _tag: "unknown" };
    }
    if (
      isKeyword(tokens[statementStart], "INSERT") ||
      isKeyword(tokens[statementStart], "REPLACE")
    ) {
      return parseInsertTarget(tokens, statementStart);
    }
    if (
      isKeyword(tokens[statementStart], "SELECT") ||
      isKeyword(tokens[statementStart], "UPDATE") ||
      isKeyword(tokens[statementStart], "DELETE")
    ) {
      return { _tag: "none" };
    }
    return { _tag: "unknown" };
  }

  if (tokens.length === 1 && isKeyword(first, "BEGIN")) {
    return { _tag: "begin" };
  }
  if (tokens.length === 2 && isKeyword(first, "BEGIN") && isKeyword(tokens[1], "TRANSACTION")) {
    return { _tag: "begin" };
  }
  if (tokens.length === 1 && isKeyword(first, "COMMIT")) {
    return { _tag: "commit" };
  }
  if (tokens.length === 2 && isKeyword(first, "COMMIT") && isKeyword(tokens[1], "TRANSACTION")) {
    return { _tag: "commit" };
  }
  if (tokens.length === 1 && isKeyword(first, "ROLLBACK")) {
    return { _tag: "rollback" };
  }
  if (tokens.length === 2 && isKeyword(first, "ROLLBACK") && isKeyword(tokens[1], "TRANSACTION")) {
    return { _tag: "rollback" };
  }
  if (tokens.length === 2 && isKeyword(first, "SAVEPOINT")) {
    const name = unquotedName(tokens[1]);
    return name === undefined ? { _tag: "unknown" } : { _tag: "savepoint", name };
  }
  if (tokens.length === 3 && isKeyword(first, "ROLLBACK") && isKeyword(tokens[1], "TO")) {
    const name = unquotedName(tokens[2]);
    return name === undefined ? { _tag: "unknown" } : { _tag: "rollbackTo", name };
  }
  if (
    tokens.length === 4 &&
    isKeyword(first, "ROLLBACK") &&
    isKeyword(tokens[1], "TO") &&
    isKeyword(tokens[2], "SAVEPOINT")
  ) {
    const name = unquotedName(tokens[3]);
    return name === undefined ? { _tag: "unknown" } : { _tag: "rollbackTo", name };
  }
  if (tokens.length === 2 && isKeyword(first, "RELEASE")) {
    const name = unquotedName(tokens[1]);
    return name === undefined ? { _tag: "unknown" } : { _tag: "release", name };
  }
  if (tokens.length === 3 && isKeyword(first, "RELEASE") && isKeyword(tokens[1], "SAVEPOINT")) {
    const name = unquotedName(tokens[2]);
    return name === undefined ? { _tag: "unknown" } : { _tag: "release", name };
  }

  return firstKeyword !== undefined && TRANSACTION_CONTROL_KEYWORDS.has(firstKeyword)
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
        throw new Error("unsupported SQL statement at controlled thread materialization boundary");
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
      markerWriteChangedRows: boolean,
    ): boolean => {
      const transactionEnded = snapshot.wasInTransaction && !db.isTransaction;
      const committed =
        transactionEnded && (statement._tag === "commit" || statement._tag === "release");
      if (transactionEnded) {
        resetMaterializationCommitState();
        return committed && snapshot.boundaryValid && snapshot.boundary === "coordinator";
      }

      const effectiveStatement =
        !markerWriteChangedRows &&
        (statement._tag === "orchestrationMarker" || statement._tag === "coordinatorMarker")
          ? ({ _tag: "none" } as const)
          : statement;
      switch (effectiveStatement._tag) {
        case "begin": {
          resetMaterializationCommitState();
          return false;
        }
        case "savepoint": {
          materializationSavepoints.push({
            name: effectiveStatement.name,
            boundaryBeforeSavepoint: snapshot.boundary,
          });
          return false;
        }
        case "rollbackTo": {
          const savepointIndex = findMaterializationSavepoint(effectiveStatement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(
              `untracked materialization savepoint rollback: ${effectiveStatement.name}`,
            );
          }
          materializationCommitBoundary =
            materializationSavepoints[savepointIndex]!.boundaryBeforeSavepoint;
          materializationSavepoints.length = savepointIndex + 1;
          return false;
        }
        case "release": {
          const savepointIndex = findMaterializationSavepoint(effectiveStatement.name);
          if (savepointIndex < 0) {
            materializationBoundaryValid = false;
            throw new Error(
              `untracked materialization savepoint release: ${effectiveStatement.name}`,
            );
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
              "unsupported SQL statement at controlled thread materialization boundary",
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
    const markerStatementChangedRows = (): boolean => {
      const row = db.prepare("SELECT changes() AS changes").get() as
        | { readonly changes?: number | bigint }
        | undefined;
      const changes = row?.changes;
      if (typeof changes !== "number" && typeof changes !== "bigint") {
        throw new Error("SQLite did not return a valid marker statement change count");
      }
      return changes !== 0 && changes !== 0n;
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
          const markerWriteChangedRows =
            materializationStatement._tag === "orchestrationMarker" ||
            materializationStatement._tag === "coordinatorMarker"
              ? markerStatementChangedRows()
              : false;
          const runCoordinatorHook = updateMaterializationCommitBoundary(
            materializationStatement,
            snapshot,
            markerWriteChangedRows,
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
