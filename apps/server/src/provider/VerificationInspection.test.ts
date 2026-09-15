// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import { afterEach, beforeEach, describe, expect, it } from "vite-plus/test";
import { inspectVerificationChanges } from "./VerificationInspection.ts";
const exec = NodeUtil.promisify(NodeChildProcess.execFile);
let root: string;
let base: string;
const git = async (...args: string[]) => (await exec("git", args, { cwd: root })).stdout.trim();
beforeEach(async () => {
  root = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-inspection-"));
  await git("init", "-q");
  await NodeFSP.writeFile(NodePath.join(root, "source.txt"), "original\n");
  await NodeFSP.writeFile(NodePath.join(root, "deleted.txt"), "deleted content\n");
  await NodeFSP.writeFile(NodePath.join(root, "rename.txt"), "rename content\n");
  await NodeFSP.writeFile(NodePath.join(root, ".gitignore"), "node_modules/\ndist/\n");
  await git("add", ".");
  await git(
    "-c",
    "user.name=Test",
    "-c",
    "user.email=test@example.invalid",
    "commit",
    "-qm",
    "base",
  );
  base = await git("rev-parse", "HEAD");
});
afterEach(async () => {
  await NodeFSP.rm(root, { recursive: true, force: true });
});
describe("complete fixed-base verification inspection", () => {
  it("shows staged, unstaged, untracked, deletion and rename contents even after git add", async () => {
    await NodeFSP.writeFile(NodePath.join(root, "source.txt"), "unstaged content\n");
    await NodeFSP.writeFile(NodePath.join(root, "new.txt"), "new HTTP test\n");
    await NodeFSP.unlink(NodePath.join(root, "deleted.txt"));
    await git("mv", "rename.txt", "renamed.txt");
    await NodeFSP.mkdir(NodePath.join(root, "node_modules"));
    await NodeFSP.writeFile(NodePath.join(root, "node_modules", "ignored.txt"), "IGNORE-ME");
    const first = await inspectVerificationChanges(root, base);
    expect(first.exitCode, first.stderr).toBe(0);
    for (const content of [
      "original",
      "unstaged content",
      "new HTTP test",
      "deleted content",
      "rename content",
      "renamed.txt",
    ])
      expect(first.stdout).toContain(content);
    expect(first.stdout).not.toContain("IGNORE-ME");
    await git("add", ".");
    expect(await inspectVerificationChanges(root, base)).toEqual(first);
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.invalid",
      "commit",
      "-qm",
      "candidate",
    );
    expect(await inspectVerificationChanges(root, base)).toEqual(first);
  });
  it("reads raw candidate bytes even when a clean filter hides changes", async () => {
    await NodeFSP.writeFile(NodePath.join(root, ".gitattributes"), "source.txt filter=hide\n");
    await git("config", "filter.hide.clean", "printf 'original\\n'");
    await NodeFSP.writeFile(NodePath.join(root, "source.txt"), "actual tested bytes\n");
    expect((await inspectVerificationChanges(root, base)).stdout).toContain("actual tested bytes");
  });
  it("includes committed candidate files subsequently removed from the index and ignored", async () => {
    await NodeFSP.writeFile(NodePath.join(root, "hidden.txt"), "candidate still used by checks\n");
    await git("add", ".");
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.invalid",
      "commit",
      "-qm",
      "candidate",
    );
    await git("rm", "--cached", "hidden.txt");
    await NodeFSP.appendFile(NodePath.join(root, ".gitignore"), "hidden.txt\n");
    const result = await inspectVerificationChanges(root, base);
    expect(result.exitCode, result.stderr).toBe(0);
    expect(result.stdout).toContain('added "hidden.txt"');
    expect(result.stdout).toContain("candidate still used by checks");
  });
  it.each(["index", "base"])(
    "rejects non-UTF-8 %s paths instead of aliasing a valid filename",
    async (source) => {
      await NodeFSP.writeFile(NodePath.join(root, "\ufffd"), "visible alias\n");
      const oid = await git("rev-parse", `${base}:source.txt`);
      const entry = Buffer.concat([
        Buffer.from(`100644 ${source === "base" ? "blob " : ""}${oid}\t`),
        Buffer.from([255, 0]),
      ]);
      let inspectionBase = base;
      if (source === "index") {
        NodeChildProcess.execFileSync("git", ["update-index", "-z", "--index-info"], {
          cwd: root,
          input: entry,
        });
      } else {
        inspectionBase = NodeChildProcess.execFileSync("git", ["mktree", "-z"], {
          cwd: root,
          input: entry,
          encoding: "utf8",
        }).trim();
      }
      const result = await inspectVerificationChanges(root, inspectionBase);
      expect(result.exitCode).toBe(125);
      expect(result.stdout).toBe("");
      expect(result.stderr).toContain("Non-UTF-8 Git paths");
    },
  );
  it("ignores replacement refs and never invokes repository fsmonitor hooks", async () => {
    await NodeFSP.writeFile(NodePath.join(root, "source.txt"), "replacement base\n");
    await git("add", ".");
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.invalid",
      "commit",
      "-qm",
      "replacement",
    );
    const replacement = await git("rev-parse", "HEAD");
    await git("replace", base, replacement);
    await NodeFSP.writeFile(NodePath.join(root, "source.txt"), "candidate bytes\n");
    const hook = NodePath.join(root, ".git", "unsafe-fsmonitor");
    const marker = NodePath.join(root, ".git", "fsmonitor-ran");
    await NodeFSP.writeFile(hook, `#!/bin/sh\ntouch '${marker}'\n`, { mode: 0o700 });
    await git("config", "core.fsmonitor", hook);
    const result = await inspectVerificationChanges(root, base);
    expect(result.exitCode, result.stderr).toBe(0);
    expect(result.stdout).toContain("original");
    expect(result.stdout).not.toContain("replacement base");
    await expect(NodeFSP.access(marker)).rejects.toThrow();
  });
  it("does not follow untracked symlinks or tracked parent symlinks outside the worktree", async () => {
    await NodeFSP.symlink("/etc/passwd", NodePath.join(root, "escape"));
    expect((await inspectVerificationChanges(root, base)).stderr).toContain("Symlink escapes");
    await NodeFSP.unlink(NodePath.join(root, "escape"));
    await NodeFSP.mkdir(NodePath.join(root, "nested"));
    await NodeFSP.writeFile(NodePath.join(root, "nested", "file"), "inside");
    await git("add", "nested");
    await NodeFSP.rm(NodePath.join(root, "nested"), { recursive: true });
    await NodeFSP.symlink("/etc", NodePath.join(root, "nested"));
    const result = await inspectVerificationChanges(root, base);
    expect(result.exitCode).toBe(125);
    expect(result.stdout).toBe("");
  });
  it("rejects non-UTF-8 symlink targets rather than reviewing replacement characters", async () => {
    await NodeFSP.symlink(Buffer.from([255]), NodePath.join(root, "link"));
    const result = await inspectVerificationChanges(root, base);
    expect(result.exitCode).toBe(125);
    expect(result.stdout).toBe("");
    expect(result.stderr).toContain("Non-UTF-8 symlink target");
  });
  it.each(["binary", "large", "total"])(
    "fails closed for incomplete %s inspection",
    async (kind) => {
      if (kind === "binary")
        await NodeFSP.writeFile(NodePath.join(root, "binary"), Buffer.from([0, 1, 2]));
      if (kind === "large")
        await NodeFSP.writeFile(NodePath.join(root, "large"), "x".repeat(24_001));
      if (kind === "total")
        for (const name of ["one", "two"])
          await NodeFSP.writeFile(NodePath.join(root, name), "x".repeat(16_000));
      const result = await inspectVerificationChanges(root, base);
      expect(result.exitCode).toBe(125);
      expect(result.stdout).toBe("");
      expect(result.stderr).toContain("T3_INSPECTION_INCOMPLETE");
    },
  );
  it("ignores unchanged large files but never accepts an arbitrary base argument", async () => {
    await NodeFSP.writeFile(NodePath.join(root, "large"), "x".repeat(30_000));
    await git("add", ".");
    await git(
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.invalid",
      "commit",
      "-qm",
      "large",
    );
    const largeBase = await git("rev-parse", "HEAD");
    expect((await inspectVerificationChanges(root, largeBase)).exitCode).toBe(0);
    expect((await inspectVerificationChanges(root, "--help")).exitCode).toBe(125);
  });
});
