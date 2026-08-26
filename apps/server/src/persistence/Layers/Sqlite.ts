import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { runMigrations } from "../Migrations.ts";
import { ServerConfig } from "../../config.ts";

export const SQLITE_NODE_RUNTIME_REQUIRED_CODE = "T3_SQLITE_NODE_RUNTIME_REQUIRED";

export class SqliteNodeRuntimeRequiredError extends Error {
  readonly code = SQLITE_NODE_RUNTIME_REQUIRED_CODE;

  constructor() {
    super(`${SQLITE_NODE_RUNTIME_REQUIRED_CODE}: apps/server persistence requires Node.js.`);
    this.name = "SqliteNodeRuntimeRequiredError";
  }
}

const requireNodePersistenceRuntime = () =>
  process.versions.bun === undefined
    ? Effect.void
    : Effect.fail(new SqliteNodeRuntimeRequiredError());

type RuntimeSqliteLayerConfig = {
  readonly filename: string;
  readonly spanAttributes?: Record<string, unknown>;
};

type Loader = {
  layer: (config: RuntimeSqliteLayerConfig) => Layer.Layer<SqlClient.SqlClient, SqlError>;
};
const defaultSqliteClientLoader = () => import("../NodeSqliteClient.ts");

const makeRuntimeSqliteLayer = Effect.fn("makeRuntimeSqliteLayer")(function* (
  config: RuntimeSqliteLayerConfig,
) {
  yield* requireNodePersistenceRuntime();
  const clientModule = yield* Effect.promise<Loader>(defaultSqliteClientLoader);
  return clientModule.layer(config);
}, Layer.unwrap);

const setup = Layer.effectDiscard(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA journal_mode = WAL;`;
    yield* sql`PRAGMA foreign_keys = ON;`;
    yield* runMigrations();
  }),
);

export const makeSqlitePersistenceLive = Effect.fn("makeSqlitePersistenceLive")(function* (
  dbPath: string,
) {
  yield* requireNodePersistenceRuntime();
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(dbPath), { recursive: true });

  return Layer.provideMerge(
    setup,
    makeRuntimeSqliteLayer({
      filename: dbPath,
      spanAttributes: {
        "db.name": path.basename(dbPath),
        "service.name": "t3-server",
      },
    }),
  );
}, Layer.unwrap);

export const SqlitePersistenceMemory = Layer.provideMerge(
  setup,
  makeRuntimeSqliteLayer({ filename: ":memory:" }),
);

export const layerConfig = Layer.unwrap(
  Effect.map(Effect.service(ServerConfig), ({ dbPath }) => makeSqlitePersistenceLive(dbPath)),
);
