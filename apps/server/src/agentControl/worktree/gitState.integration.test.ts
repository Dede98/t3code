// @effect-diagnostics nodeBuiltinImport:off - exercises the exact bytes emitted by real Git.
import { assert, describe, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { parseAgentControlPorcelainV2Status } from "./gitState.ts";

const git = (
  cwd: string,
  args: ReadonlyArray<string>,
  options: { readonly allowFailure?: boolean } = {},
) => {
  const result = NodeChildProcess.spawnSync("git", args, {
    cwd,
    encoding: "utf8",
  });
  if (!options.allowFailure && result.status !== 0) {
    throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
  }
  return result;
};

const initializeRepository = (directory: string) => {
  git(directory, ["init", "-b", "main"]);
  git(directory, ["config", "user.name", "T3 Test"]);
  git(directory, ["config", "user.email", "t3@example.invalid"]);
};

const status = (directory: string) =>
  git(directory, [
    "status",
    "--porcelain=v2",
    "-z",
    "--untracked-files=all",
    "--ignore-submodules=none",
  ]).stdout;

const withTemporaryDirectory = (run: (directory: string) => void) => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-git-state-"));
  try {
    run(directory);
  } finally {
    NodeFS.rmSync(directory, { recursive: true, force: true });
  }
};

describe("Agent Control real porcelain-v2 status", () => {
  it("parses modified, staged, untracked, rename, newline, and unmerged records", () =>
    withTemporaryDirectory((directory) => {
      initializeRepository(directory);
      NodeFS.writeFileSync(NodePath.join(directory, "tracked.txt"), "base\n");
      git(directory, ["add", "tracked.txt"]);
      git(directory, ["commit", "-m", "base"]);

      NodeFS.writeFileSync(NodePath.join(directory, "tracked.txt"), "modified\n");
      assert.deepEqual(
        parseAgentControlPorcelainV2Status(status(directory)).map((entry) => entry.type),
        ["ordinary"],
      );
      git(directory, ["reset", "--hard", "HEAD"]);

      NodeFS.writeFileSync(NodePath.join(directory, "staged.txt"), "staged\n");
      git(directory, ["add", "staged.txt"]);
      assert.deepEqual(
        parseAgentControlPorcelainV2Status(status(directory)).map((entry) => entry.type),
        ["ordinary"],
      );
      git(directory, ["reset", "--hard", "HEAD"]);
      git(directory, ["clean", "-fd"]);

      NodeFS.writeFileSync(NodePath.join(directory, "untracked.txt"), "untracked\n");
      assert.deepEqual(parseAgentControlPorcelainV2Status(status(directory)), [
        { type: "untracked", path: "untracked.txt" },
      ]);
      git(directory, ["clean", "-fd"]);

      git(directory, ["mv", "tracked.txt", "renamed.txt"]);
      assert.deepEqual(parseAgentControlPorcelainV2Status(status(directory)), [
        { type: "rename-copy", path: "renamed.txt", originPath: "tracked.txt" },
      ]);
      git(directory, ["reset", "--hard", "HEAD"]);

      const newlinePath = "line\nbreak.txt";
      NodeFS.writeFileSync(NodePath.join(directory, newlinePath), "literal newline\n");
      assert.deepEqual(parseAgentControlPorcelainV2Status(status(directory)), [
        { type: "untracked", path: newlinePath },
      ]);
      git(directory, ["clean", "-fd"]);

      git(directory, ["checkout", "-b", "conflict-side"]);
      NodeFS.writeFileSync(NodePath.join(directory, "tracked.txt"), "side\n");
      git(directory, ["commit", "-am", "side"]);
      git(directory, ["checkout", "main"]);
      NodeFS.writeFileSync(NodePath.join(directory, "tracked.txt"), "main\n");
      git(directory, ["commit", "-am", "main"]);
      const merge = git(directory, ["merge", "conflict-side"], { allowFailure: true });
      assert.notEqual(merge.status, 0);
      assert.deepEqual(parseAgentControlPorcelainV2Status(status(directory)), [
        { type: "unmerged", path: "tracked.txt" },
      ]);
    }));

  it("parses real modified, untracked, and conflicted submodule records", () =>
    withTemporaryDirectory((directory) => {
      const child = NodePath.join(directory, "child");
      const parent = NodePath.join(directory, "parent");
      NodeFS.mkdirSync(child);
      NodeFS.mkdirSync(parent);
      initializeRepository(child);
      NodeFS.writeFileSync(NodePath.join(child, "child.txt"), "base\n");
      git(child, ["add", "child.txt"]);
      git(child, ["commit", "-m", "child base"]);
      const base = git(child, ["rev-parse", "HEAD"]).stdout.trim();

      git(child, ["checkout", "-b", "side"]);
      NodeFS.writeFileSync(NodePath.join(child, "child.txt"), "side\n");
      git(child, ["commit", "-am", "child side"]);
      const side = git(child, ["rev-parse", "HEAD"]).stdout.trim();
      git(child, ["checkout", "main"]);
      NodeFS.writeFileSync(NodePath.join(child, "child.txt"), "main\n");
      git(child, ["commit", "-am", "child main"]);
      const main = git(child, ["rev-parse", "HEAD"]).stdout.trim();

      initializeRepository(parent);
      git(parent, ["-c", "protocol.file.allow=always", "submodule", "add", child, "module"]);
      git(NodePath.join(parent, "module"), ["checkout", base]);
      git(parent, ["add", "module"]);
      git(parent, ["commit", "-m", "parent base"]);

      NodeFS.writeFileSync(NodePath.join(parent, "module", "child.txt"), "modified\n");
      let records = parseAgentControlPorcelainV2Status(status(parent));
      assert.equal(records.length, 1);
      assert.equal(records[0]!.type, "ordinary");
      git(NodePath.join(parent, "module"), ["reset", "--hard", base]);

      NodeFS.writeFileSync(NodePath.join(parent, "module", "untracked.txt"), "untracked\n");
      records = parseAgentControlPorcelainV2Status(status(parent));
      assert.equal(records.length, 1);
      assert.equal(records[0]!.type, "ordinary");
      NodeFS.rmSync(NodePath.join(parent, "module", "untracked.txt"));

      git(parent, ["checkout", "-b", "parent-side"]);
      git(NodePath.join(parent, "module"), ["checkout", side]);
      git(parent, ["add", "module"]);
      git(parent, ["commit", "-m", "parent side"]);
      git(parent, ["checkout", "main"]);
      git(NodePath.join(parent, "module"), ["checkout", main]);
      git(parent, ["add", "module"]);
      git(parent, ["commit", "-m", "parent main"]);
      const merge = git(parent, ["merge", "parent-side"], { allowFailure: true });
      assert.notEqual(merge.status, 0);
      records = parseAgentControlPorcelainV2Status(status(parent));
      assert.equal(records.length, 1);
      assert.equal(records[0]!.type, "unmerged");
    }));
});
