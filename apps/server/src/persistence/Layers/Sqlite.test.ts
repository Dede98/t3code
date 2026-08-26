// @effect-diagnostics nodeBuiltinImport:off - verifies the installed Bun process and pre-I/O fail-fast boundary.
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeURL from "node:url";

import { assert, it } from "@effect/vitest";

import { SQLITE_NODE_RUNTIME_REQUIRED_CODE } from "./Sqlite.ts";

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
