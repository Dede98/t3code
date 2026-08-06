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

type MaterializationCommitBoundary =
  | "open"
  | "orchestration"
  | "coordinator"
  | "prepare"
  | "initialPlanningFinalization"
  | "implementationAdmissionPending"
  | "implementationAdmission"
  | "implementationMaterializationPending"
  | "implementationMaterialization"
  | "implementationTurnAcceptance"
  | "implementationStageStartPending"
  | "implementationStageStart"
  | "implementationStageFinalizationPending"
  | "implementationStageFinalization"
  | "verificationAdmissionPending"
  | "verificationAdmission";

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
  | {
      readonly _tag: "orchestrationMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "coordinatorMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "prepareMarker";
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "initialPlanningHandoff";
      readonly table: string;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "initialPlanningFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationAdmission";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationMaterialization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationTurnAcceptance";
      readonly table: string;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationStageStart";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "implementationStageFinalization";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "verificationAdmission";
      readonly table: string;
      readonly final: boolean;
      readonly target: "unqualified" | "main";
    }
  | {
      readonly _tag: "markerMutation";
      readonly table?: string;
      readonly target?: "unqualified" | "main";
    }
  | { readonly _tag: "potentialMarkerDml" };

interface SqlWordToken {
  readonly _tag: "word";
  readonly value: string;
}

interface SqlIdentifierToken {
  readonly _tag: "doubleQuotedIdentifier" | "backtickIdentifier" | "bracketIdentifier";
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
  | SqlWordToken
  | SqlIdentifierToken
  | SqlStringToken
  | SqlParameterToken
  | SqlPunctuationToken
  | SqlOperatorToken;

const ORCHESTRATION_MARKER_TABLE = "orchestration_agent_control_thread_materialization_receipts";
const COORDINATOR_MARKER_TABLE = "agent_control_controlled_thread_materialization_accepted";
const PREPARE_STATE_TABLE = "agent_control_controlled_thread_prepare_finalizations";
const PREPARE_MARKER_TABLE = "agent_control_controlled_thread_prepare_final_commit_markers";
const INITIAL_PLANNING_HANDOFF_TABLES = new Set([
  "agent_control_initial_planning_handoff_intents",
  "agent_control_initial_planning_handoff_receipts",
  "agent_control_initial_planning_handoff_accepted",
  "agent_control_initial_planning_deliveries",
]);
const INITIAL_PLANNING_FINALIZATION_MARKER_TABLE =
  "agent_control_initial_planning_finalization_markers";
const INITIAL_PLANNING_FINALIZATION_TABLES = new Set([
  "agent_control_initial_planning_stage_started",
  "agent_control_initial_planning_result_evidence",
  "agent_control_initial_planning_finalization_receipts",
  INITIAL_PLANNING_FINALIZATION_MARKER_TABLE,
]);
const IMPLEMENTATION_ADMISSION_MARKER_TABLE = "agent_control_implementation_admission_markers";
const IMPLEMENTATION_ADMISSION_TABLES = new Set([
  "agent_control_implementation_admission_evidence",
  "agent_control_implementation_admission_receipts",
  IMPLEMENTATION_ADMISSION_MARKER_TABLE,
]);
const IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE =
  "agent_control_implementation_materialization_markers";
const IMPLEMENTATION_MATERIALIZATION_TABLES = new Set([
  "agent_control_implementation_materialization_evidence",
  "agent_control_implementation_materialization_receipts",
  "agent_control_implementation_handoff_intents",
  "agent_control_implementation_handoff_receipts",
  "agent_control_implementation_handoff_accepted",
  "agent_control_implementation_deliveries",
  IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE,
]);
const IMPLEMENTATION_TURN_ACCEPTANCE_TABLE = "agent_control_implementation_turn_accepted";
const IMPLEMENTATION_STAGE_START_MARKER_TABLE =
  "agent_control_implementation_stage_started_markers";
const IMPLEMENTATION_STAGE_START_TABLES = new Set([
  "agent_control_implementation_stage_started_evidence",
  "agent_control_implementation_stage_started_receipts",
  IMPLEMENTATION_STAGE_START_MARKER_TABLE,
]);
const IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE =
  "agent_control_implementation_stage_finalization_markers";
const IMPLEMENTATION_STAGE_FINALIZATION_TABLES = new Set([
  "agent_control_implementation_result_evidence",
  "agent_control_implementation_stage_finalization_receipts",
  IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE,
]);
const VERIFICATION_ADMISSION_MARKER_TABLE = "agent_control_verification_admission_markers";
const VERIFICATION_ADMISSION_TABLES = new Set([
  "agent_control_verification_admission_evidence",
  "agent_control_verification_admission_receipts",
  VERIFICATION_ADMISSION_MARKER_TABLE,
]);
const IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES = new Set([
  "agent_control_implementation_session_evidence",
  "agent_control_implementation_delivery_attestations",
]);
const INSERT_CONFLICT_ALGORITHMS = new Set(["ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"]);
const MATERIALIZATION_MARKER_TRANSACTION_REQUIRED =
  "persistent materialization marker DML requires an active caller-controlled transaction";

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
      tokens.push({ _tag: "word", value: value.toUpperCase() });
      continue;
    }
    tokens.push({ _tag: "operator", value: character });
    index += 1;
  }

  return tokens;
};

const isWord = (token: SqlToken | undefined, value?: string): token is SqlWordToken =>
  token?._tag === "word" && (value === undefined || token.value === value);

const isIdentifier = (token: SqlToken | undefined): token is SqlWordToken | SqlIdentifierToken =>
  token?._tag === "word" ||
  token?._tag === "doubleQuotedIdentifier" ||
  token?._tag === "backtickIdentifier" ||
  token?._tag === "bracketIdentifier";

const normalizedIdentifier = (token: SqlWordToken | SqlIdentifierToken): string =>
  token.value.toLowerCase();

const identifierName = (token: SqlToken | undefined): string | undefined =>
  isIdentifier(token) ? normalizedIdentifier(token) : undefined;

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
  if (!isWord(tokens[index], "WITH")) {
    return undefined;
  }
  index += 1;
  if (isWord(tokens[index], "RECURSIVE")) {
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

    if (!isWord(tokens[index], "AS")) {
      return undefined;
    }
    index += 1;
    if (isWord(tokens[index], "MATERIALIZED")) {
      index += 1;
    } else if (isWord(tokens[index], "NOT") && isWord(tokens[index + 1], "MATERIALIZED")) {
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
  if (isWord(tokens[index], "INSERT")) {
    index += 1;
    if (isWord(tokens[index], "OR")) {
      const conflictAlgorithm = tokens[index + 1];
      if (!isWord(conflictAlgorithm) || !INSERT_CONFLICT_ALGORITHMS.has(conflictAlgorithm.value)) {
        return { _tag: "potentialMarkerDml" };
      }
      index += 2;
    }
  } else if (isWord(tokens[index], "REPLACE")) {
    index += 1;
  } else {
    return { _tag: "none" };
  }

  if (!isWord(tokens[index], "INTO")) {
    return { _tag: "potentialMarkerDml" };
  }
  index += 1;

  const firstIdentifier = tokens[index];
  if (!isIdentifier(firstIdentifier)) {
    return { _tag: "potentialMarkerDml" };
  }
  let schema: string | undefined;
  let table = normalizedIdentifier(firstIdentifier);
  index += 1;

  if (tokens[index]?._tag === "dot") {
    const tableIdentifier = tokens[index + 1];
    if (!isIdentifier(tableIdentifier)) {
      return { _tag: "potentialMarkerDml" };
    }
    schema = table;
    table = normalizedIdentifier(tableIdentifier);
    index += 2;
    if (tokens[index]?._tag === "dot") {
      return { _tag: "potentialMarkerDml" };
    }
  }

  if (schema !== undefined && schema !== "main") {
    return { _tag: "none" };
  }
  if (table === ORCHESTRATION_MARKER_TABLE) {
    return { _tag: "orchestrationMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (table === COORDINATOR_MARKER_TABLE) {
    return { _tag: "coordinatorMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (table === PREPARE_MARKER_TABLE) {
    return { _tag: "prepareMarker", target: schema === "main" ? "main" : "unqualified" };
  }
  if (INITIAL_PLANNING_HANDOFF_TABLES.has(table)) {
    return {
      _tag: "initialPlanningHandoff",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (INITIAL_PLANNING_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "initialPlanningFinalization",
      table,
      final: table === INITIAL_PLANNING_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_ADMISSION_TABLES.has(table)) {
    return {
      _tag: "implementationAdmission",
      table,
      final: table === IMPLEMENTATION_ADMISSION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_MATERIALIZATION_TABLES.has(table)) {
    return {
      _tag: "implementationMaterialization",
      table,
      final: table === IMPLEMENTATION_MATERIALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === IMPLEMENTATION_TURN_ACCEPTANCE_TABLE) {
    return {
      _tag: "implementationTurnAcceptance",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_STAGE_START_TABLES.has(table)) {
    return {
      _tag: "implementationStageStart",
      table,
      final: table === IMPLEMENTATION_STAGE_START_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_STAGE_FINALIZATION_TABLES.has(table)) {
    return {
      _tag: "implementationStageFinalization",
      table,
      final: table === IMPLEMENTATION_STAGE_FINALIZATION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (VERIFICATION_ADMISSION_TABLES.has(table)) {
    return {
      _tag: "verificationAdmission",
      table,
      final: table === VERIFICATION_ADMISSION_MARKER_TABLE,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table)) {
    return {
      _tag: "markerMutation",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  if (table === PREPARE_STATE_TABLE) {
    return {
      _tag: "markerMutation",
      table,
      target: schema === "main" ? "main" : "unqualified",
    };
  }
  return { _tag: "none" };
};

const parseUpdateOrDeleteTarget = (
  tokens: ReadonlyArray<SqlToken>,
  start: number,
): MaterializationStatement => {
  let index = start;
  if (isWord(tokens[index], "UPDATE")) {
    index += 1;
    if (isWord(tokens[index], "OR")) {
      const conflictAlgorithm = tokens[index + 1];
      if (!isWord(conflictAlgorithm) || !INSERT_CONFLICT_ALGORITHMS.has(conflictAlgorithm.value)) {
        return { _tag: "potentialMarkerDml" };
      }
      index += 2;
    }
  } else if (isWord(tokens[index], "DELETE")) {
    index += 1;
    if (!isWord(tokens[index], "FROM")) {
      return { _tag: "potentialMarkerDml" };
    }
    index += 1;
  } else {
    return { _tag: "none" };
  }

  const firstIdentifier = tokens[index];
  if (!isIdentifier(firstIdentifier)) {
    return { _tag: "potentialMarkerDml" };
  }
  let schema: string | undefined;
  let table = normalizedIdentifier(firstIdentifier);
  index += 1;
  if (tokens[index]?._tag === "dot") {
    const tableIdentifier = tokens[index + 1];
    if (!isIdentifier(tableIdentifier)) {
      return { _tag: "potentialMarkerDml" };
    }
    schema = table;
    table = normalizedIdentifier(tableIdentifier);
  }

  if (schema !== undefined && schema !== "main") {
    return { _tag: "none" };
  }
  return table === ORCHESTRATION_MARKER_TABLE ||
    table === COORDINATOR_MARKER_TABLE ||
    table === PREPARE_STATE_TABLE ||
    table === PREPARE_MARKER_TABLE ||
    INITIAL_PLANNING_FINALIZATION_TABLES.has(table) ||
    IMPLEMENTATION_ADMISSION_TABLES.has(table) ||
    IMPLEMENTATION_MATERIALIZATION_TABLES.has(table) ||
    table === IMPLEMENTATION_TURN_ACCEPTANCE_TABLE ||
    IMPLEMENTATION_STAGE_START_TABLES.has(table) ||
    IMPLEMENTATION_STAGE_FINALIZATION_TABLES.has(table) ||
    VERIFICATION_ADMISSION_TABLES.has(table) ||
    IMPLEMENTATION_TRANSACTIONAL_EVIDENCE_TABLES.has(table)
    ? {
        _tag: "markerMutation",
        table,
        target: schema === "main" ? "main" : "unqualified",
      }
    : { _tag: "none" };
};

const parseMaterializationStatement = (sql: string): MaterializationStatement => {
  let tokens: ReadonlyArray<SqlToken>;
  try {
    tokens = lexFirstSqlStatement(sql);
  } catch {
    return { _tag: "none" };
  }
  const semicolonIndex = tokens.findIndex((token) => token._tag === "semicolon");
  if (semicolonIndex >= 0) {
    tokens = tokens.slice(0, semicolonIndex);
  }

  const first = tokens[0];
  if (isWord(first, "INSERT") || isWord(first, "REPLACE")) {
    return parseInsertTarget(tokens, 0);
  }
  if (isWord(first, "UPDATE") || isWord(first, "DELETE")) {
    return parseUpdateOrDeleteTarget(tokens, 0);
  }
  if (isWord(first, "WITH")) {
    const statementStart = parseWithPrefix(tokens, 0);
    if (statementStart === undefined) {
      return { _tag: "potentialMarkerDml" };
    }
    if (isWord(tokens[statementStart], "INSERT") || isWord(tokens[statementStart], "REPLACE")) {
      return parseInsertTarget(tokens, statementStart);
    }
    if (isWord(tokens[statementStart], "UPDATE") || isWord(tokens[statementStart], "DELETE")) {
      return parseUpdateOrDeleteTarget(tokens, statementStart);
    }
    return isWord(tokens[statementStart], "SELECT") || isWord(tokens[statementStart], "VALUES")
      ? { _tag: "none" }
      : { _tag: "potentialMarkerDml" };
  }

  if (isWord(first, "BEGIN")) {
    let index = 1;
    if (
      isWord(tokens[index], "DEFERRED") ||
      isWord(tokens[index], "IMMEDIATE") ||
      isWord(tokens[index], "EXCLUSIVE")
    ) {
      index += 1;
    }
    if (isWord(tokens[index], "TRANSACTION")) {
      index += 1;
    }
    return index === tokens.length ? { _tag: "begin" } : { _tag: "none" };
  }
  if (isWord(first, "COMMIT") || isWord(first, "END")) {
    return tokens.length === 1 || (tokens.length === 2 && isWord(tokens[1], "TRANSACTION"))
      ? { _tag: "commit" }
      : { _tag: "none" };
  }
  if (isWord(first, "ROLLBACK")) {
    let index = 1;
    if (isWord(tokens[index], "TRANSACTION")) {
      index += 1;
    }
    if (index === tokens.length) {
      return { _tag: "rollback" };
    }
    if (!isWord(tokens[index], "TO")) {
      return { _tag: "none" };
    }
    index += 1;
    if (isWord(tokens[index], "SAVEPOINT")) {
      index += 1;
    }
    const name = identifierName(tokens[index]);
    return name !== undefined && index + 1 === tokens.length
      ? { _tag: "rollbackTo", name }
      : { _tag: "none" };
  }
  if (tokens.length === 2 && isWord(first, "SAVEPOINT")) {
    const name = identifierName(tokens[1]);
    return name === undefined ? { _tag: "none" } : { _tag: "savepoint", name };
  }
  if (isWord(first, "RELEASE")) {
    let index = 1;
    if (isWord(tokens[index], "SAVEPOINT")) {
      index += 1;
    }
    const name = identifierName(tokens[index]);
    return name !== undefined && index + 1 === tokens.length
      ? { _tag: "release", name }
      : { _tag: "none" };
  }

  return { _tag: "none" };
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

interface SqliteClientInternalConfig extends SqliteClientConfig {
  /** @internal Test-only fault injection for synchronous marker change-count reads. */
  readonly _testHooks?: {
    readonly beforeMarkerChanges?: (() => void) | undefined;
  };
}

export interface SqliteMemoryClientConfig extends Omit<
  SqliteClientConfig,
  "filename" | "readonly"
> {
  /** @internal Test-only fault injection for synchronous marker change-count reads. */
  readonly _testHooks?: {
    readonly beforeMarkerChanges?: (() => void) | undefined;
  };
}

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
  options: SqliteClientInternalConfig,
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
    const requireMaterializationMarkerTransaction = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ) => {
      if (
        snapshot.wasInTransaction ||
        (statement._tag !== "orchestrationMarker" &&
          statement._tag !== "coordinatorMarker" &&
          statement._tag !== "prepareMarker" &&
          statement._tag !== "initialPlanningHandoff" &&
          statement._tag !== "initialPlanningFinalization" &&
          statement._tag !== "implementationAdmission" &&
          statement._tag !== "implementationMaterialization" &&
          statement._tag !== "implementationTurnAcceptance" &&
          statement._tag !== "implementationStageStart" &&
          statement._tag !== "implementationStageFinalization" &&
          statement._tag !== "verificationAdmission" &&
          statement._tag !== "markerMutation")
      ) {
        return;
      }
      resetMaterializationCommitState();
      throw new Error(MATERIALIZATION_MARKER_TRANSACTION_REQUIRED);
    };
    const ensureMaterializationCommitBoundary = (
      statement: MaterializationStatement,
      snapshot: MaterializationStatementSnapshot,
    ) => {
      if (statement._tag === "potentialMarkerDml") {
        if (snapshot.wasInTransaction) {
          materializationBoundaryValid = false;
        }
        throw new Error("potential materialization marker DML could not be classified safely");
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
        if (
          (snapshot.boundary === "implementationAdmissionPending" ||
            snapshot.boundary === "implementationMaterializationPending" ||
            snapshot.boundary === "implementationStageStartPending" ||
            snapshot.boundary === "implementationStageFinalizationPending" ||
            snapshot.boundary === "verificationAdmissionPending") &&
          (statement._tag === "commit" ||
            (statement._tag === "release" &&
              snapshot.savepoints.length === 1 &&
              snapshot.savepoints[0]?.name === statement.name))
        ) {
          materializationBoundaryValid = false;
          throw new Error(
            snapshot.boundary === "implementationAdmissionPending"
              ? "implementation admission companion chain requires a final marker"
              : snapshot.boundary === "verificationAdmissionPending"
                ? "verification admission companion chain requires a final marker"
                : "implementation companion chain requires a final marker",
          );
        }
        return;
      }
      const coordinatorHandoff =
        snapshot.boundary === "orchestration" && statement._tag === "coordinatorMarker";
      const initialPlanningHandoff =
        snapshot.boundary === "orchestration" && statement._tag === "initialPlanningHandoff";
      const implementationAdmissionCompanion =
        snapshot.boundary === "implementationAdmissionPending" &&
        statement._tag === "implementationAdmission";
      const implementationMaterializationCompanion =
        (snapshot.boundary === "orchestration" ||
          snapshot.boundary === "implementationMaterializationPending") &&
        statement._tag === "implementationMaterialization";
      const implementationStageStartCompanion =
        snapshot.boundary === "implementationStageStartPending" &&
        statement._tag === "implementationStageStart";
      const implementationStageFinalizationCompanion =
        snapshot.boundary === "implementationStageFinalizationPending" &&
        statement._tag === "implementationStageFinalization";
      const verificationAdmissionCompanion =
        snapshot.boundary === "verificationAdmissionPending" &&
        statement._tag === "verificationAdmission";
      if (
        snapshot.boundary !== "open" &&
        !coordinatorHandoff &&
        !initialPlanningHandoff &&
        !implementationAdmissionCompanion &&
        !implementationMaterializationCompanion &&
        !implementationStageStartCompanion &&
        !implementationStageFinalizationCompanion &&
        !verificationAdmissionCompanion
      ) {
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
        return (
          committed &&
          snapshot.boundaryValid &&
          (snapshot.boundary === "coordinator" ||
            snapshot.boundary === "prepare" ||
            snapshot.boundary === "initialPlanningFinalization" ||
            snapshot.boundary === "implementationAdmission" ||
            snapshot.boundary === "implementationMaterialization" ||
            snapshot.boundary === "implementationTurnAcceptance" ||
            snapshot.boundary === "implementationStageStart" ||
            snapshot.boundary === "implementationStageFinalization" ||
            snapshot.boundary === "verificationAdmission")
        );
      }

      const effectiveStatement =
        !markerWriteChangedRows &&
        (statement._tag === "orchestrationMarker" ||
          statement._tag === "coordinatorMarker" ||
          statement._tag === "prepareMarker" ||
          (statement._tag === "initialPlanningFinalization" && statement.final) ||
          (statement._tag === "implementationAdmission" && statement.final) ||
          (statement._tag === "implementationMaterialization" && statement.final) ||
          statement._tag === "implementationTurnAcceptance" ||
          (statement._tag === "implementationStageStart" && statement.final) ||
          (statement._tag === "implementationStageFinalization" && statement.final) ||
          (statement._tag === "verificationAdmission" && statement.final))
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
        case "potentialMarkerDml": {
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
        case "prepareMarker": {
          if (db.isTransaction) {
            materializationCommitBoundary = "prepare";
          }
          break;
        }
        case "initialPlanningFinalization": {
          if (effectiveStatement.final && db.isTransaction) {
            materializationCommitBoundary = "initialPlanningFinalization";
          }
          break;
        }
        case "implementationAdmission": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationAdmission"
              : "implementationAdmissionPending";
          }
          break;
        }
        case "implementationMaterialization": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationMaterialization"
              : "implementationMaterializationPending";
          }
          break;
        }
        case "implementationTurnAcceptance": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = "implementationTurnAcceptance";
          }
          break;
        }
        case "implementationStageStart": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationStageStart"
              : "implementationStageStartPending";
          }
          break;
        }
        case "implementationStageFinalization": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "implementationStageFinalization"
              : "implementationStageFinalizationPending";
          }
          break;
        }
        case "verificationAdmission": {
          if (markerWriteChangedRows && db.isTransaction) {
            materializationCommitBoundary = effectiveStatement.final
              ? "verificationAdmission"
              : "verificationAdmissionPending";
          }
          break;
        }
        case "markerMutation":
        case "initialPlanningHandoff":
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
          statement._tag === "prepareMarker" ||
          statement._tag === "initialPlanningHandoff" ||
          statement._tag === "initialPlanningFinalization" ||
          statement._tag === "implementationAdmission" ||
          statement._tag === "implementationMaterialization" ||
          statement._tag === "implementationTurnAcceptance" ||
          statement._tag === "implementationStageStart" ||
          statement._tag === "implementationStageFinalization" ||
          statement._tag === "verificationAdmission" ||
          statement._tag === "markerMutation" ||
          statement._tag === "potentialMarkerDml")
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
          if (statement._tag === "potentialMarkerDml") {
            handleMaterializationStatementFailure(statement);
            throw new Error("potential materialization marker DML could not be classified safely");
          }
          requireMaterializationMarkerTransaction(statement, snapshotMaterializationStatement());
          assertPersistentMarkerTarget(statement);
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
      options._testHooks?.beforeMarkerChanges?.();
      const row = db.prepare("SELECT changes() AS changes").get() as
        | { readonly changes?: number | bigint }
        | undefined;
      const changes = row?.changes;
      if (typeof changes !== "number" && typeof changes !== "bigint") {
        throw new Error("SQLite did not return a valid marker statement change count");
      }
      return changes !== 0 && changes !== 0n;
    };
    const assertPersistentMarkerTarget = (statement: MaterializationStatement): void => {
      if (
        statement._tag !== "orchestrationMarker" &&
        statement._tag !== "coordinatorMarker" &&
        statement._tag !== "prepareMarker" &&
        statement._tag !== "initialPlanningHandoff" &&
        statement._tag !== "initialPlanningFinalization" &&
        statement._tag !== "implementationAdmission" &&
        statement._tag !== "implementationMaterialization" &&
        statement._tag !== "implementationTurnAcceptance" &&
        statement._tag !== "implementationStageStart" &&
        statement._tag !== "implementationStageFinalization" &&
        statement._tag !== "verificationAdmission" &&
        !(statement._tag === "markerMutation" && statement.table !== undefined)
      ) {
        return;
      }
      const table =
        statement._tag === "orchestrationMarker"
          ? ORCHESTRATION_MARKER_TABLE
          : statement._tag === "coordinatorMarker"
            ? COORDINATOR_MARKER_TABLE
            : statement._tag === "prepareMarker"
              ? PREPARE_MARKER_TABLE
              : statement.table!;
      // Keep authority on the native connection and in the same synchronous
      // call stack as marker execution. These reads do not change changes().
      const mainEntry = db
        .prepare("SELECT type FROM main.sqlite_schema WHERE name = ? COLLATE NOCASE LIMIT 1")
        .get(table) as { readonly type?: string } | undefined;
      if (mainEntry?.type !== "table") {
        throw new Error(`persistent materialization marker table is missing or invalid: ${table}`);
      }
      if (statement.target === "main") {
        return;
      }
      const tempEntry = db
        .prepare("SELECT type FROM sqlite_temp_schema WHERE name = ? COLLATE NOCASE LIMIT 1")
        .get(table) as { readonly type?: string } | undefined;
      if (tempEntry !== undefined) {
        throw new Error(`temporary schema shadows materialization marker target: ${table}`);
      }
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
          requireMaterializationMarkerTransaction(materializationStatement, snapshot);
          ensureMaterializationCommitBoundary(materializationStatement, snapshot);
          assertPersistentMarkerTarget(materializationStatement);
          statement.setReadBigInts(Boolean(Context.get(fiber.context, Client.SafeIntegers)));
          const result = execute(statement, params);
          const markerWriteChangedRows =
            materializationStatement._tag === "orchestrationMarker" ||
            materializationStatement._tag === "coordinatorMarker" ||
            materializationStatement._tag === "prepareMarker" ||
            (materializationStatement._tag === "initialPlanningFinalization" &&
              materializationStatement.final) ||
            materializationStatement._tag === "implementationAdmission" ||
            materializationStatement._tag === "implementationMaterialization" ||
            materializationStatement._tag === "implementationTurnAcceptance" ||
            materializationStatement._tag === "implementationStageStart" ||
            materializationStatement._tag === "implementationStageFinalization" ||
            materializationStatement._tag === "verificationAdmission"
              ? markerStatementChangedRows()
              : false;
          const runPostCommitHook = updateMaterializationCommitBoundary(
            materializationStatement,
            snapshot,
            markerWriteChangedRows,
          );
          return runPostCommitHook
            ? Context.get(fiber.context, NodeSqliteTransactionHooks)
                .afterCommitBeforeReturn({
                  boundary:
                    snapshot.boundary === "prepare"
                      ? "agent-control-controlled-thread-prepare-finalization"
                      : snapshot.boundary === "initialPlanningFinalization"
                        ? "agent-control-initial-planning-stage-finalization"
                        : snapshot.boundary === "implementationAdmission"
                          ? "agent-control-implementation-admission"
                          : snapshot.boundary === "implementationMaterialization"
                            ? "agent-control-implementation-materialization"
                            : snapshot.boundary === "implementationTurnAcceptance"
                              ? "agent-control-implementation-turn-acceptance"
                              : snapshot.boundary === "implementationStageStart"
                                ? "agent-control-implementation-stage-start"
                                : snapshot.boundary === "implementationStageFinalization"
                                  ? "agent-control-implementation-stage-finalization"
                                  : snapshot.boundary === "verificationAdmission"
                                    ? "agent-control-verification-admission"
                                    : "agent-control-controlled-thread-materialization-coordinator",
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
            if (db.isTransaction && materializationCommitBoundary !== "open") {
              materializationBoundaryValid = false;
            }
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
            if (db.isTransaction && materializationCommitBoundary !== "open") {
              materializationBoundaryValid = false;
            }
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
