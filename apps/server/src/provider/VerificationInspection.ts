// @effect-diagnostics nodeBuiltinImport:off
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as NodeCrypto from "node:crypto";
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
const FILE_LIMIT = 24_000;
const OUTPUT_LIMIT = 28_000;
const PATH_LIMIT = 10_000;
export const VERIFICATION_INSPECTIONS = ["git-status", "git-diff-check", "git-diff"] as const;
export type VerificationInspection = (typeof VERIFICATION_INSPECTIONS)[number];
export const VERIFICATION_INSPECTION_DISPLAY = {
  id: "git-diff",
  command: "t3_verification_check",
  args: ["git-diff"],
  cwd: ".",
  required: true,
} as const;

/** Git permits arbitrary filename bytes; never silently alias them to UTF-8 replacement characters. */
export const decodeVerificationGitOutput = (output: Buffer) => {
  const text = output.toString("utf8");
  if (!Buffer.from(text).equals(output))
    throw new Error("Non-UTF-8 Git paths require another review capability");
  return text;
};

/** Check each ancestor without following links, including deleted tracked paths. */
export const verificationFilePath = async (root: string, name: string) => {
  if (
    !name ||
    name.includes("\\") ||
    name.includes("\0") ||
    NodePath.isAbsolute(name) ||
    name.split("/").some((part) => part === ".." || part === "." || !part)
  )
    throw new Error("Unsafe verification path");
  const parts = name.split("/");
  for (let i = 1; i < parts.length; i++) {
    const parent = NodePath.join(root, ...parts.slice(0, i));
    const stat = await NodeFSP.lstat(parent).catch((error: NodeJS.ErrnoException) => {
      if (error.code === "ENOENT") return null;
      throw error;
    });
    if (stat && (!stat.isDirectory() || stat.isSymbolicLink()))
      throw new Error(`Unsafe verification ancestor: ${name}`);
  }
  return NodePath.join(root, name);
};

export const verificationInspectionCapability = async () => {
  const supported =
    HostProcessPlatform.defaultValue() === "darwin" ||
    (HostProcessPlatform.defaultValue() === "linux" &&
      (await NodeFSP.access("/proc/self/fd").then(
        () => true,
        () => false,
      )));
  return {
    supported,
    reason: supported
      ? null
      : "Complete safe source inspection requires macOS or Linux with /proc/self/fd.",
  };
};

/** O_NOFOLLOW_ANY is Darwin's kernel-enforced whole-path no-symlink flag.
 * Linux opens one component at a time relative to pinned directory descriptors.
 */
export const openVerificationFile = async (root: string, name: string) => {
  const path = await verificationFilePath(root, name);
  if (HostProcessPlatform.defaultValue() === "darwin")
    return NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NONBLOCK | 0x20000000);
  if (HostProcessPlatform.defaultValue() !== "linux")
    throw new Error("Safe inspection is unavailable on this platform");
  let directory = await NodeFSP.open(
    root,
    NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
  );
  try {
    const parts = name.split("/");
    for (const part of parts.slice(0, -1)) {
      const next = await NodeFSP.open(
        `/proc/self/fd/${directory.fd}/${part}`,
        NodeFS.constants.O_RDONLY | NodeFS.constants.O_DIRECTORY | NodeFS.constants.O_NOFOLLOW,
      );
      await directory.close();
      directory = next;
    }
    return await NodeFSP.open(
      `/proc/self/fd/${directory.fd}/${parts.at(-1)!}`,
      NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW | NodeFS.constants.O_NONBLOCK,
    );
  } finally {
    await directory.close();
  }
};

/** Fixed raw before/after inspection; Git filters, external diffs and textconv never execute. */
export const inspectVerificationChanges = async (
  worktree: string,
  baseCommit: string,
  inspection: VerificationInspection = "git-diff",
) => {
  try {
    if (!/^[a-f0-9]{40,64}$/.test(baseCommit)) throw new Error("Missing fixed verification base");
    const root = await NodeFSP.realpath(worktree);
    const git = async (args: string[], maxBuffer = 2_000_000) =>
      decodeVerificationGitOutput(
        (
          await exec("git", ["--no-optional-locks", "-c", "core.fsmonitor=false", ...args], {
            cwd: root,
            encoding: "buffer",
            maxBuffer,
            timeout: 15_000,
            env: {
              ...process.env,
              GIT_OPTIONAL_LOCKS: "0",
              GIT_NO_REPLACE_OBJECTS: "1",
              GIT_NO_LAZY_FETCH: "1",
              GIT_EXTERNAL_DIFF: "",
            },
          })
        ).stdout,
      );
    const base = new Map<string, { mode: string; oid: string }>();
    for (const entry of (await git(["ls-tree", "-rz", "--full-tree", baseCommit]))
      .split("\0")
      .filter(Boolean)) {
      const separator = entry.indexOf("\t");
      const [mode, , oid] = entry.slice(0, separator).split(" ");
      if (separator < 0 || !mode || !oid) throw new Error("Invalid base tree");
      base.set(entry.slice(separator + 1), { mode, oid });
    }
    const current = (await git(["ls-files", "--cached", "--others", "--exclude-standard", "-z"]))
      .split("\0")
      .filter(Boolean);
    // A committed candidate can remove a path from the index and ignore it while
    // its raw bytes still participate in checks and the bound code snapshot.
    const head = (await git(["ls-tree", "-rz", "--name-only", "HEAD"])).split("\0").filter(Boolean);
    const names = [...new Set([...base.keys(), ...head, ...current])].sort();
    if (names.length > PATH_LIMIT) throw new Error("Inspection exceeds 10000 paths");
    const sections: string[] = [];
    let bytes = 0;
    for (const name of names) {
      const path = await verificationFilePath(root, name);
      const previous = base.get(name);
      if (previous?.mode === "160000") throw new Error(`Submodule inspection unavailable: ${name}`);
      const stat = await NodeFSP.lstat(path).catch((error: NodeJS.ErrnoException) => {
        if (error.code === "ENOENT" || error.code === "ENOTDIR") return null;
        throw error;
      });
      let after: Buffer | null = null;
      let mode: string | null = null;
      if (stat?.isSymbolicLink()) {
        after = await NodeFSP.readlink(path, { encoding: "buffer" });
        const target = after.toString("utf8");
        if (!Buffer.from(target).equals(after))
          throw new Error(`Non-UTF-8 symlink target requires another review capability: ${name}`);
        const relative = NodePath.relative(root, NodePath.resolve(NodePath.dirname(path), target));
        if (
          relative === ".." ||
          relative.startsWith(`..${NodePath.sep}`) ||
          NodePath.isAbsolute(relative)
        )
          throw new Error(`Symlink escapes verification root: ${name}`);
        mode = "120000";
      } else if (stat) {
        if (!stat.isFile()) throw new Error(`Unsupported file type: ${name}`);
        mode = stat.mode & 0o111 ? "100755" : "100644";
        const file = await openVerificationFile(root, name);
        try {
          const opened = await file.stat();
          if (!opened.isFile()) throw new Error(`Unsupported file: ${name}`);
          const hash = NodeCrypto.createHash(previous?.oid.length === 64 ? "sha256" : "sha1");
          hash.update(`blob ${opened.size}\0`);
          const chunks: Buffer[] = [];
          let size = 0;
          for await (const chunk of file.createReadStream({ autoClose: false })) {
            size += chunk.length;
            hash.update(chunk);
            if (size <= FILE_LIMIT) chunks.push(chunk);
          }
          if (previous?.oid === hash.digest("hex") && previous.mode === mode) continue;
          if (size > FILE_LIMIT)
            throw new Error(`Changed file exceeds ${FILE_LIMIT} bytes: ${name}`);
          after = Buffer.concat(chunks);
        } finally {
          await file.close();
        }
      }
      let before: Buffer | null = null;
      if (previous) {
        const size = Number((await git(["cat-file", "-s", previous.oid])).trim());
        if (!Number.isSafeInteger(size) || size > FILE_LIMIT)
          throw new Error(`Base file exceeds ${FILE_LIMIT} bytes: ${name}`);
        before = (
          await exec("git", ["-c", "core.fsmonitor=false", "cat-file", "blob", previous.oid], {
            cwd: root,
            env: { ...process.env, GIT_NO_REPLACE_OBJECTS: "1", GIT_NO_LAZY_FETCH: "1" },
            encoding: "buffer",
            maxBuffer: FILE_LIMIT + 1,
            timeout: 15_000,
          })
        ).stdout;
      }
      if (before?.equals(after ?? Buffer.alloc(0)) && previous?.mode === mode) continue;
      if (!before && !after) continue;
      for (const content of [before, after]) {
        if (
          content &&
          (content.includes(0) || !Buffer.from(content.toString("utf8")).equals(content))
        )
          throw new Error(`Binary or non-UTF-8 change requires another review capability: ${name}`);
      }
      const status = !before ? "added" : !after ? "deleted" : "modified";
      const header = `${status} ${JSON.stringify(name)} (${previous?.mode ?? "absent"} -> ${mode ?? "absent"})`;
      const section =
        inspection === "git-status"
          ? header
          : `${header}\n--- BASE\n${before?.toString("utf8") ?? "[absent]"}\n+++ CANDIDATE\n${after?.toString("utf8") ?? "[absent]"}`;
      bytes += Buffer.byteLength(section);
      if (bytes > OUTPUT_LIMIT)
        throw new Error(`Complete inspection exceeds ${OUTPUT_LIMIT} output bytes`);
      sections.push(section);
    }
    return {
      exitCode: 0,
      stdout: `Complete raw changes against ${baseCommit}. Renames are shown as deletion/addition.\n${sections.join("\n\n")}\n`,
      stderr: "",
    };
  } catch (error) {
    return {
      exitCode: 125,
      stdout: "",
      stderr: `T3_INSPECTION_INCOMPLETE: ${error instanceof Error ? error.message : String(error)}. No complete review evidence is available.`,
    };
  }
};
