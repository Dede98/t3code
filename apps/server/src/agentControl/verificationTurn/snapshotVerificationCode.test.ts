// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  snapshotVerificationCode,
  VerificationCheckError,
  VERIFICATION_CODE_SNAPSHOT_PREFIX,
} from "./checkEvidence.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const io = <A>(run: () => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: (cause) => new VerificationCheckError({ cause }) });

const rawRepository = Effect.acquireRelease(
  io(async () => {
    const cwd = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-check-raw-"));
    await exec("git", ["init", "-q"], { cwd });
    await NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "base\n");
    await NodeFSP.writeFile(NodePath.join(cwd, ".gitattributes"), "source.txt filter=collapse\n");
    await exec("git", ["add", "."], { cwd });
    await exec(
      "git",
      ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "initial"],
      { cwd },
    );
    return cwd;
  }),
  (cwd) => Effect.promise(() => NodeFSP.rm(cwd, { recursive: true, force: true })),
);

it.effect("binds raw tracked bytes when a clean filter hides a change from Git diff", () =>
  Effect.gen(function* () {
    const cwd = yield* rawRepository;
    yield* io(() =>
      exec(
        "git",
        [
          "config",
          "filter.collapse.clean",
          "sed -e 's/checked A/stored C/' -e 's/changed B/stored C/'",
        ],
        { cwd },
      ),
    );
    yield* io(() => NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "checked A\n"));
    const before = yield* snapshotVerificationCode(cwd);
    const beforeDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd }));
    yield* io(() => NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "changed B\n"));
    const afterDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd }));
    assert.equal(beforeDiff.stdout, afterDiff.stdout);
    assert.isTrue(before.startsWith(VERIFICATION_CODE_SNAPSHOT_PREFIX));
    assert.notEqual(before, yield* snapshotVerificationCode(cwd));
  }).pipe(Effect.scoped),
);

it.effect("binds tracked symlink targets, permissions and deletion", () =>
  Effect.gen(function* () {
    const cwd = yield* rawRepository;
    const file = NodePath.join(cwd, "source.txt");
    yield* io(() => exec("git", ["config", "core.filemode", "false"], { cwd }));
    const before = yield* snapshotVerificationCode(cwd);
    yield* io(() => NodeFSP.chmod(file, 0o755));
    const executable = yield* snapshotVerificationCode(cwd);
    assert.notEqual(before, executable);
    yield* io(() => NodeFSP.unlink(file));
    const deleted = yield* snapshotVerificationCode(cwd);
    assert.notEqual(executable, deleted);
    yield* io(() => NodeFSP.symlink("missing-one", file));
    const firstLink = yield* snapshotVerificationCode(cwd);
    yield* io(() => NodeFSP.unlink(file));
    yield* io(() => NodeFSP.symlink("missing-two", file));
    assert.notEqual(firstLink, yield* snapshotVerificationCode(cwd));
  }).pipe(Effect.scoped),
);

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

it.effect(
  "binds HEAD-tracked bytes even after removing the path from the index and ignoring it",
  () =>
    Effect.gen(function* () {
      const cwd = yield* rawRepository;
      yield* io(() => exec("git", ["rm", "--cached", "source.txt"], { cwd }));
      yield* io(() =>
        NodeFSP.writeFile(NodePath.join(cwd, ".gitignore"), "source.txt\nnode_modules/\n"),
      );
      const before = yield* snapshotVerificationCode(cwd);
      const beforeDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd }));
      yield* io(() =>
        NodeFSP.writeFile(NodePath.join(cwd, "source.txt"), "hidden raw modification\n"),
      );
      const afterDiff = yield* io(() => exec("git", ["diff", "HEAD"], { cwd }));
      assert.equal(beforeDiff.stdout, afterDiff.stdout);
      const modified = yield* snapshotVerificationCode(cwd);
      assert.notEqual(before, modified);
      yield* io(async () => {
        await NodeFSP.mkdir(NodePath.join(cwd, "node_modules"));
        await NodeFSP.writeFile(NodePath.join(cwd, "node_modules", "ignored.txt"), "dependency");
      });
      assert.equal(yield* snapshotVerificationCode(cwd), modified);
    }).pipe(Effect.scoped),
);
