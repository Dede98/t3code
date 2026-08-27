export const SQLITE_NODE_RUNTIME_REQUIRED_CODE = "T3_SQLITE_NODE_RUNTIME_REQUIRED";

export class SqliteNodeRuntimeRequiredError extends Error {
  readonly code = SQLITE_NODE_RUNTIME_REQUIRED_CODE;

  constructor() {
    super(`${SQLITE_NODE_RUNTIME_REQUIRED_CODE}: apps/server persistence requires Node.js.`);
    this.name = "SqliteNodeRuntimeRequiredError";
  }
}

export const nodeRuntimeRequiredError = (): SqliteNodeRuntimeRequiredError | undefined =>
  process.versions.bun === undefined ? undefined : new SqliteNodeRuntimeRequiredError();
