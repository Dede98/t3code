// @effect-diagnostics nodeBuiltinImport:off - lstat/rmdir are required for no-follow identity checks.
import type { AgentControlWorktreeReservationId, ProjectId } from "@t3tools/contracts";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { ServerConfig } from "../../config.ts";
import { deriveAgentControlWorktreePathKeys } from "./identity.ts";

export class AgentControlWorktreePathSafetyError extends Schema.TaggedErrorClass<AgentControlWorktreePathSafetyError>()(
  "AgentControlWorktreePathSafetyError",
  {
    reason: Schema.Literals([
      "root-invalid",
      "target-invalid",
      "target-exists",
      "path-escape",
      "path-identity-conflict",
      "parent-permissions-invalid",
      "observation-failed",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const fail = (reason: AgentControlWorktreePathSafetyError["reason"], cause?: unknown) =>
  new AgentControlWorktreePathSafetyError({
    reason,
    ...(cause === undefined ? {} : { cause }),
  });
const isPathSafetyError = Schema.is(AgentControlWorktreePathSafetyError);

const errno = (cause: unknown) =>
  typeof cause === "object" && cause !== null && "code" in cause
    ? (cause as { readonly code?: unknown }).code
    : undefined;

const lstatNoFollow = (target: string) =>
  Effect.tryPromise({
    try: async () => {
      try {
        return Option.some(await NodeFSP.lstat(target));
      } catch (cause) {
        if (errno(cause) === "ENOENT") return Option.none();
        throw cause;
      }
    },
    catch: () => fail("observation-failed"),
  });

const isStrictlyInside = (
  path: {
    readonly relative: (from: string, to: string) => string;
    readonly sep: string;
  },
  parent: string,
  child: string,
) => {
  const relative = path.relative(parent, child);
  return relative.length > 0 && relative !== ".." && !relative.startsWith(`..${path.sep}`);
};

export interface AgentControlPathIdentity {
  readonly path: string;
  readonly device: number;
  readonly inode: number;
}

export interface AgentControlWorktreeTargetIdentity extends AgentControlPathIdentity {
  readonly uid: number;
  readonly mode: number;
  readonly parentIdentity: AgentControlPathIdentity;
}

const validateControlledDirectory = Effect.fn("validateControlledDirectory")(function* (
  directory: string,
  _reason: AgentControlWorktreePathSafetyError["reason"],
) {
  const observed = yield* lstatNoFollow(directory);
  if (Option.isNone(observed)) return yield* fail("observation-failed");
  const info = observed.value;
  if (
    !info.isDirectory() ||
    info.isSymbolicLink() ||
    (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
    (info.mode & 0o022) !== 0 ||
    info.ino < 0
  ) {
    return yield* fail("parent-permissions-invalid");
  }
  return { path: directory, device: info.dev, inode: info.ino } satisfies AgentControlPathIdentity;
});

export const deriveSafeAgentControlWorktreePath = Effect.fn("deriveSafeAgentControlWorktreePath")(
  function* (input: {
    readonly projectId: ProjectId;
    readonly reservationId: AgentControlWorktreeReservationId;
    readonly targetGenerationId: string;
    readonly repositoryWorkspace: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig;
    const worktreesDir = yield* fs
      .realPath(config.worktreesDir)
      .pipe(Effect.mapError(() => fail("observation-failed")));
    const repositoryWorkspace = yield* fs
      .realPath(input.repositoryWorkspace)
      .pipe(Effect.mapError(() => fail("observation-failed")));
    const root = path.join(worktreesDir, "agent-control");
    yield* fs
      .makeDirectory(root, { recursive: true, mode: 0o700 })
      .pipe(Effect.mapError(() => fail("observation-failed")));
    const canonicalRoot = yield* fs
      .realPath(root)
      .pipe(Effect.mapError(() => fail("observation-failed")));
    if (
      !isStrictlyInside(path, worktreesDir, canonicalRoot) ||
      canonicalRoot === repositoryWorkspace ||
      worktreesDir === repositoryWorkspace
    ) {
      return yield* fail("path-identity-conflict");
    }
    const rootIdentity = yield* validateControlledDirectory(canonicalRoot, "root-invalid");

    const keys = deriveAgentControlWorktreePathKeys(input);
    const projectParent = path.join(canonicalRoot, keys.projectKey);
    yield* fs
      .makeDirectory(projectParent, { recursive: true, mode: 0o700 })
      .pipe(Effect.mapError(() => fail("observation-failed")));
    const canonicalParent = yield* fs
      .realPath(projectParent)
      .pipe(Effect.mapError(() => fail("observation-failed")));
    if (!isStrictlyInside(path, canonicalRoot, canonicalParent)) {
      return yield* fail("path-escape");
    }
    const parentIdentity = yield* validateControlledDirectory(canonicalParent, "path-escape");

    const target = path.resolve(canonicalParent, `${keys.reservationKey}-${keys.generationKey}`);
    if (
      !isStrictlyInside(path, canonicalRoot, target) ||
      target === canonicalRoot ||
      target === repositoryWorkspace
    ) {
      return yield* fail("path-escape");
    }
    if (Option.isSome(yield* lstatNoFollow(target))) return yield* fail("target-exists");
    return {
      root: canonicalRoot,
      parent: canonicalParent,
      target,
      rootIdentity,
      parentIdentity,
    };
  },
);
const lstatNoFollowSync = (target: string) =>
  Effect.try({
    try: () => {
      try {
        return Option.some(NodeFS.lstatSync(target));
      } catch (cause) {
        if (errno(cause) === "ENOENT") return Option.none();
        throw cause;
      }
    },
    catch: () => fail("observation-failed"),
  });

export const validateExistingAgentControlWorktreePath = Effect.fn(
  "validateExistingAgentControlWorktreePath",
)(function* (input: { readonly target: string; readonly repositoryWorkspace: string }) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const worktreesDir = yield* fs
    .realPath(config.worktreesDir)
    .pipe(Effect.mapError(() => fail("observation-failed")));
  const root = yield* fs
    .realPath(path.join(worktreesDir, "agent-control"))
    .pipe(Effect.mapError(() => fail("observation-failed")));
  if (!isStrictlyInside(path, worktreesDir, root)) {
    return yield* fail("path-escape");
  }
  const rootIdentity = yield* validateControlledDirectory(root, "root-invalid");
  const parent = yield* fs
    .realPath(path.dirname(input.target))
    .pipe(Effect.mapError(() => fail("observation-failed")));
  const lexicalTarget = path.resolve(parent, path.basename(input.target));
  if (lexicalTarget !== input.target || !isStrictlyInside(path, root, lexicalTarget)) {
    return yield* fail("path-escape");
  }
  const repositoryWorkspace = yield* fs
    .realPath(input.repositoryWorkspace)
    .pipe(Effect.mapError(() => fail("observation-failed")));
  if (lexicalTarget === repositoryWorkspace || root === repositoryWorkspace) {
    return yield* fail("path-identity-conflict");
  }
  const parentIdentity = yield* validateControlledDirectory(parent, "path-escape");
  return { root, parent, target: lexicalTarget, rootIdentity, parentIdentity };
});

export const revalidateAgentControlWorktreePathIdentity = Effect.fn(
  "revalidateAgentControlWorktreePathIdentity",
)(function* (identity: {
  readonly rootIdentity: AgentControlPathIdentity;
  readonly parentIdentity: AgentControlPathIdentity;
}) {
  const root = yield* validateControlledDirectory(identity.rootIdentity.path, "root-invalid");
  const parent = yield* validateControlledDirectory(identity.parentIdentity.path, "path-escape");
  if (
    root.device !== identity.rootIdentity.device ||
    root.inode !== identity.rootIdentity.inode ||
    parent.device !== identity.parentIdentity.device ||
    parent.inode !== identity.parentIdentity.inode
  ) {
    return yield* fail("path-identity-conflict");
  }
  return identity;
});

/**
 * Exclusively claims the still-absent target as an empty, mode-0700 directory,
 * verifies it without following links, and leaves the empty claim in place for
 * `git worktree add`, which supports an existing empty target on the supported
 * Git platform. The parent identity is checked on both sides. This narrows but
 * cannot sandbox a malicious process
 * running with the same OS-user authority; post-Git top-level and inode checks
 * remain mandatory.
 */
export const acquireAgentControlWorktreeTargetPath = Effect.fn(
  "acquireAgentControlWorktreeTargetPath",
)(function* (input: {
  readonly target: string;
  readonly rootIdentity: AgentControlPathIdentity;
  readonly parentIdentity: AgentControlPathIdentity;
  readonly fault?: AgentControlWorktreeControllerTargetPathFault | undefined;
}) {
  yield* revalidateAgentControlWorktreePathIdentity(input);
  const target = yield* Effect.try({
    try: () => {
      let created = false;
      try {
        NodeFS.mkdirSync(input.target, { mode: 0o700 });
        created = true;
        input.fault?.("after-mkdir-before-lstat");
        const info = NodeFS.lstatSync(input.target);
        if (
          !info.isDirectory() ||
          info.isSymbolicLink() ||
          (typeof process.getuid === "function" && info.uid !== process.getuid()) ||
          (info.mode & 0o777) !== 0o700 ||
          info.ino < 0
        ) {
          throw fail("target-invalid");
        }
        return {
          path: input.target,
          device: info.dev,
          inode: info.ino,
          uid: info.uid,
          mode: info.mode,
          parentIdentity: input.parentIdentity,
        } satisfies AgentControlWorktreeTargetIdentity;
      } catch (cause) {
        if (!created) throw cause;
        try {
          input.fault?.("before-cleanup-lstat");
          const observed = NodeFS.lstatSync(input.target);
          input.fault?.("before-cleanup-read-directory");
          const children = NodeFS.readdirSync(input.target);
          if (
            !observed.isDirectory() ||
            observed.isSymbolicLink() ||
            (typeof process.getuid === "function" && observed.uid !== process.getuid()) ||
            (observed.mode & 0o777) !== 0o700 ||
            children.length !== 0
          ) {
            throw fail("path-identity-conflict");
          }
          input.fault?.("before-cleanup-rmdir");
          NodeFS.rmdirSync(input.target);
        } catch (cleanupCause) {
          const combined = new AggregateError(
            [cause, cleanupCause],
            "target acquire and acquisition cleanup both failed",
            { cause: cleanupCause },
          );
          throw combined;
        }
        throw cause;
      }
    },
    catch: (cause) =>
      isPathSafetyError(cause)
        ? cause
        : fail(errno(cause) === "EEXIST" ? "target-exists" : "observation-failed", cause),
  });
  return target;
});

export type AgentControlWorktreeControllerTargetPathFault = (
  point:
    | "after-mkdir-before-lstat"
    | "before-cleanup-lstat"
    | "before-cleanup-read-directory"
    | "before-cleanup-rmdir",
) => void;

export const verifyAgentControlWorktreeTargetPath = Effect.fn(
  "verifyAgentControlWorktreeTargetPath",
)(function* (
  input: {
    readonly rootIdentity: AgentControlPathIdentity;
    readonly parentIdentity: AgentControlPathIdentity;
  },
  target: AgentControlWorktreeTargetIdentity,
) {
  const fs = yield* FileSystem.FileSystem;
  const children = yield* fs
    .readDirectory(target.path)
    .pipe(Effect.mapError(() => fail("observation-failed")));
  if (children.length !== 0) return yield* fail("target-exists");
  yield* revalidateAgentControlWorktreePathIdentity(input);
  const verified = yield* lstatNoFollow(target.path);
  if (
    Option.isNone(verified) ||
    !verified.value.isDirectory() ||
    verified.value.isSymbolicLink() ||
    verified.value.dev !== target.device ||
    verified.value.ino !== target.inode ||
    verified.value.uid !== target.uid ||
    verified.value.mode !== target.mode
  ) {
    return yield* fail("path-identity-conflict");
  }
  return target;
});

export const reserveAgentControlWorktreeTargetPath = Effect.fn(
  "reserveAgentControlWorktreeTargetPath",
)(function* (input: {
  readonly target: string;
  readonly rootIdentity: AgentControlPathIdentity;
  readonly parentIdentity: AgentControlPathIdentity;
}) {
  const target = yield* acquireAgentControlWorktreeTargetPath(input);
  return yield* verifyAgentControlWorktreeTargetPath(input, target);
});

/** Releases only the still-empty directory claim with the exact captured identity. */
export const releaseAgentControlWorktreeTargetPath = Effect.fn(
  "releaseAgentControlWorktreeTargetPath",
)(function* (identity: AgentControlWorktreeTargetIdentity) {
  const parent = yield* lstatNoFollowSync(identity.parentIdentity.path);
  if (Option.isNone(parent)) return yield* fail("observation-failed");
  if (
    !parent.value.isDirectory() ||
    parent.value.isSymbolicLink() ||
    parent.value.dev !== identity.parentIdentity.device ||
    parent.value.ino !== identity.parentIdentity.inode ||
    (typeof process.getuid === "function" && parent.value.uid !== process.getuid()) ||
    (parent.value.mode & 0o022) !== 0
  ) {
    return yield* fail("path-identity-conflict");
  }
  const observed = yield* lstatNoFollowSync(identity.path);
  if (Option.isNone(observed)) return yield* fail("observation-failed");
  const current = observed.value;
  const children = yield* Effect.try({
    try: () => NodeFS.readdirSync(identity.path),
    catch: () => fail("observation-failed"),
  });
  if (
    !current.isDirectory() ||
    current.isSymbolicLink() ||
    current.dev !== identity.device ||
    current.ino !== identity.inode ||
    current.uid !== identity.uid ||
    current.mode !== identity.mode ||
    (typeof process.getuid === "function" && current.uid !== process.getuid()) ||
    (current.mode & 0o777) !== 0o700 ||
    children.length !== 0
  ) {
    return yield* fail("path-identity-conflict");
  }
  yield* Effect.try({
    try: () => NodeFS.rmdirSync(identity.path),
    catch: () => fail("observation-failed"),
  });
  if (Option.isSome(yield* lstatNoFollowSync(identity.path))) {
    return yield* fail("observation-failed");
  }
});
