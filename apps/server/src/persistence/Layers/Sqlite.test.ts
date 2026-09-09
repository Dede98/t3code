import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";
import { assert, it } from "@effect/vitest";
import {
  SQLITE_NODE_RUNTIME_REQUIRED_CODE,
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "./Sqlite.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

it("fails fast under the installed Bun runtime before persistence setup or migrations", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-node-runtime-"));
  try {
    const filename = NodePath.join(directory, "state.sqlite");
    const nestedFilename = NodePath.join(directory, "not-created", "state.sqlite");
    const original = Buffer.from([0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x00, 0xff]);
    NodeFS.writeFileSync(filename, original);
    const moduleUrl = NodeURL.pathToFileURL(
      NodePath.join(process.cwd(), "apps/server/src/persistence/Layers/Sqlite.ts"),
    ).href;
    const program = `
      import * as Effect from "effect/Effect";
      import * as Layer from "effect/Layer";
      import {
        makeSqlitePersistenceLive,
        SQLITE_NODE_RUNTIME_REQUIRED_CODE,
      } from ${JSON.stringify(moduleUrl)};

      for (const dbPath of ${JSON.stringify([filename, nestedFilename])}) {
        try {
          await Effect.runPromise(
            Effect.scoped(Layer.build(makeSqlitePersistenceLive(dbPath))),
          );
          process.exit(2);
        } catch (cause) {
          const diagnostic = String(cause);
          if (
            !diagnostic.includes(SQLITE_NODE_RUNTIME_REQUIRED_CODE) ||
            diagnostic.includes(dbPath)
          ) {
            process.exit(3);
          }
        }
      }
      process.stdout.write(SQLITE_NODE_RUNTIME_REQUIRED_CODE);
    `;
    const bun = NodeChildProcess.spawnSync("bun", ["--eval", program], {
      cwd: NodePath.join(process.cwd(), "apps/server"),
      encoding: "utf8",
    });

    assert.equal(bun.status, 0, bun.stderr);
    assert.equal(bun.stdout, SQLITE_NODE_RUNTIME_REQUIRED_CODE);
    assert.equal(bun.stderr, "");
    assert.deepStrictEqual(NodeFS.readFileSync(filename), original);
    assert.isFalse(NodeFS.existsSync(`${filename}-wal`));
    assert.isFalse(NodeFS.existsSync(`${filename}-shm`));
    assert.isFalse(NodeFS.existsSync(NodePath.join(directory, "not-created")));
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
});

const lockHolderSource = `
const { DatabaseSync } = require("node:sqlite");
const db = new DatabaseSync(process.argv[1]);
db.exec("BEGIN IMMEDIATE");
process.stdout.write("locked\\n");
setTimeout(() => {
  db.exec("COMMIT");
  db.close();
}, Number(process.argv[2]));
`;

const spawnWriteLockHolder = (dbPath: string, holdMs: number) =>
  Effect.promise(
    () =>
      new Promise<void>((resolve, reject) => {
        const holder = NodeChildProcess.spawn(
          process.execPath,
          ["-e", lockHolderSource, dbPath, String(holdMs)],
          { stdio: ["ignore", "pipe", "ignore"] },
        );
        holder.stdout.once("data", () => resolve());
        holder.on("error", reject);
        holder.on("exit", () =>
          reject(new Error("lock holder exited before acquiring the write lock")),
        );
      }),
  );

it.effect("waits out a concurrent writer instead of failing with SQLITE_BUSY", () => {
  const tempDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-sqlite-busy-"));
  const dbPath = NodePath.join(tempDir, "state.sqlite");

  return Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TABLE busy_probe(id INTEGER PRIMARY KEY)`;
    yield* spawnWriteLockHolder(dbPath, 300);
    yield* sql`INSERT INTO busy_probe(id) VALUES (${1})`;
    const rows = yield* sql<{ readonly id: number }>`SELECT id FROM busy_probe`;
    assert.deepEqual([...rows], [{ id: 1 }]);
  }).pipe(
    Effect.provide(makeSqlitePersistenceLive(dbPath).pipe(Layer.provide(NodeServices.layer))),
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(tempDir, { recursive: true, force: true }))),
  );
});

it.effect("applies busy_timeout in the shared persistence setup", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly timeout: number }>`PRAGMA busy_timeout`;
    assert.equal(rows[0]?.timeout, 5000);
  }).pipe(Effect.provide(SqlitePersistenceMemory)),
);
