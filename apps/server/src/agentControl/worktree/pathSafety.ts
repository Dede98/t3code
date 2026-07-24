import type { AgentControlWorktreeReservationId, ProjectId } from "@t3tools/contracts";
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
    ]),
  },
) {}

const fail = (reason: AgentControlWorktreePathSafetyError["reason"]) =>
  new AgentControlWorktreePathSafetyError({ reason });

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

const validateControlledDirectory = Effect.fn("validateControlledDirectory")(function* (
  directory: string,
  reason: AgentControlWorktreePathSafetyError["reason"],
) {
  const fs = yield* FileSystem.FileSystem;
  const link = yield* Effect.result(fs.readLink(directory));
  if (link._tag === "Success") return yield* fail(reason);
  const info = yield* fs.stat(directory).pipe(Effect.mapError(() => fail(reason)));
  const uid = Option.getOrUndefined(info.uid);
  const inode = Option.getOrUndefined(info.ino);
  if (
    info.type !== "Directory" ||
    (uid !== undefined && typeof process.getuid === "function" && uid !== process.getuid()) ||
    (info.mode & 0o022) !== 0 ||
    inode === undefined
  ) {
    return yield* fail("parent-permissions-invalid");
  }
  return { path: directory, device: info.dev, inode } satisfies AgentControlPathIdentity;
});

export const deriveSafeAgentControlWorktreePath = Effect.fn("deriveSafeAgentControlWorktreePath")(
  function* (input: {
    readonly projectId: ProjectId;
    readonly reservationId: AgentControlWorktreeReservationId;
    readonly repositoryWorkspace: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig;
    const worktreesDir = yield* fs
      .realPath(config.worktreesDir)
      .pipe(Effect.mapError(() => fail("root-invalid")));
    const repositoryWorkspace = yield* fs
      .realPath(input.repositoryWorkspace)
      .pipe(Effect.mapError(() => fail("path-identity-conflict")));
    const root = path.join(worktreesDir, "agent-control");
    yield* fs
      .makeDirectory(root, { recursive: true, mode: 0o700 })
      .pipe(Effect.mapError(() => fail("root-invalid")));
    const canonicalRoot = yield* fs
      .realPath(root)
      .pipe(Effect.mapError(() => fail("root-invalid")));
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
      .pipe(Effect.mapError(() => fail("target-invalid")));
    const canonicalParent = yield* fs
      .realPath(projectParent)
      .pipe(Effect.mapError(() => fail("target-invalid")));
    if (!isStrictlyInside(path, canonicalRoot, canonicalParent)) {
      return yield* fail("path-escape");
    }
    const parentIdentity = yield* validateControlledDirectory(canonicalParent, "path-escape");

    const target = path.resolve(canonicalParent, keys.reservationKey);
    if (
      !isStrictlyInside(path, canonicalRoot, target) ||
      target === canonicalRoot ||
      target === repositoryWorkspace
    ) {
      return yield* fail("path-escape");
    }
    const [exists, link] = yield* Effect.all([
      fs.exists(target).pipe(Effect.mapError(() => fail("target-invalid"))),
      Effect.result(fs.readLink(target)),
    ]);
    if (exists || link._tag === "Success") return yield* fail("target-exists");
    return {
      root: canonicalRoot,
      parent: canonicalParent,
      target,
      rootIdentity,
      parentIdentity,
    };
  },
);

export const validateExistingAgentControlWorktreePath = Effect.fn(
  "validateExistingAgentControlWorktreePath",
)(function* (input: { readonly target: string; readonly repositoryWorkspace: string }) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const config = yield* ServerConfig;
  const worktreesDir = yield* fs
    .realPath(config.worktreesDir)
    .pipe(Effect.mapError(() => fail("root-invalid")));
  const root = yield* fs
    .realPath(path.join(worktreesDir, "agent-control"))
    .pipe(Effect.mapError(() => fail("root-invalid")));
  if (!isStrictlyInside(path, worktreesDir, root)) {
    return yield* fail("path-escape");
  }
  const rootIdentity = yield* validateControlledDirectory(root, "root-invalid");
  const parent = yield* fs
    .realPath(path.dirname(input.target))
    .pipe(Effect.mapError(() => fail("target-invalid")));
  const lexicalTarget = path.resolve(parent, path.basename(input.target));
  if (lexicalTarget !== input.target || !isStrictlyInside(path, root, lexicalTarget)) {
    return yield* fail("path-escape");
  }
  const repositoryWorkspace = yield* fs
    .realPath(input.repositoryWorkspace)
    .pipe(Effect.mapError(() => fail("path-identity-conflict")));
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
export const reserveAgentControlWorktreeTargetPath = Effect.fn(
  "reserveAgentControlWorktreeTargetPath",
)(function* (input: {
  readonly target: string;
  readonly rootIdentity: AgentControlPathIdentity;
  readonly parentIdentity: AgentControlPathIdentity;
}) {
  const fs = yield* FileSystem.FileSystem;
  yield* revalidateAgentControlWorktreePathIdentity(input);
  yield* fs
    .makeDirectory(input.target, { mode: 0o700 })
    .pipe(Effect.mapError(() => fail("target-exists")));
  const target = yield* validateControlledDirectory(input.target, "target-invalid");
  const children = yield* fs
    .readDirectory(input.target)
    .pipe(Effect.mapError(() => fail("target-invalid")));
  if (children.length !== 0) return yield* fail("target-exists");
  yield* revalidateAgentControlWorktreePathIdentity(input);
  return target;
});

/** Releases only the still-empty directory claim with the exact captured identity. */
export const releaseAgentControlWorktreeTargetPath = Effect.fn(
  "releaseAgentControlWorktreeTargetPath",
)(function* (identity: AgentControlPathIdentity) {
  const fs = yield* FileSystem.FileSystem;
  const current = yield* validateControlledDirectory(identity.path, "target-invalid");
  const children = yield* fs
    .readDirectory(identity.path)
    .pipe(Effect.mapError(() => fail("target-invalid")));
  if (
    current.device !== identity.device ||
    current.inode !== identity.inode ||
    children.length !== 0
  ) {
    return yield* fail("path-identity-conflict");
  }
  yield* fs
    .remove(identity.path, { recursive: true })
    .pipe(Effect.mapError(() => fail("target-invalid")));
});
