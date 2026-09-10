// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { snapshotVerificationCode, VerificationCheckError } from "./checkEvidence.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const io = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new VerificationCheckError({ cause }) });

it.effect(
  "distinguishes successive tracked and untracked changes inside an already dirty submodule",
  () =>
    Effect.gen(function* () {
      const root = yield* Effect.acquireRelease(
        io(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-check-submodule-"))),
        (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
      );
      const parent = NodePath.join(root, "parent");
      yield* io(async () => {
        const sub = NodePath.join(root, "source");
        for (const cwd of [parent, sub]) {
          await NodeFSP.mkdir(cwd);
          await exec("git", ["init", "-q"], { cwd });
          await NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "original");
          await exec("git", ["add", "."], { cwd });
          await exec(
            "git",
            [
              "-c",
              "user.name=Test",
              "-c",
              "user.email=test@example.invalid",
              "commit",
              "-qm",
              "initial",
            ],
            { cwd },
          );
        }
        await exec("git", ["-c", "protocol.file.allow=always", "submodule", "add", sub, "module"], {
          cwd: parent,
        });
        await exec(
          "git",
          [
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qam",
            "module",
          ],
          { cwd: parent },
        );
      });
      const module = NodePath.join(parent, "module");
      yield* io(() => NodeFSP.writeFile(NodePath.join(module, "source.txt"), "first change"));
      const first = yield* snapshotVerificationCode(parent);
      const firstParentDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd: parent }));
      yield* io(() => NodeFSP.writeFile(NodePath.join(module, "source.txt"), "second change"));
      const second = yield* snapshotVerificationCode(parent);
      const secondParentDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd: parent }));
      assert.equal(firstParentDiff.stdout, secondParentDiff.stdout);
      assert.notEqual(first, second);
      yield* io(() => NodeFSP.writeFile(NodePath.join(module, "new.txt"), "new source"));
      assert.notEqual(second, yield* snapshotVerificationCode(parent));
    }).pipe(Effect.scoped),
);
