import type { AgentControlWorktreeReservationId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
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
      .makeDirectory(root, { recursive: true })
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

    const keys = deriveAgentControlWorktreePathKeys(input);
    const projectParent = path.join(canonicalRoot, keys.projectKey);
    yield* fs
      .makeDirectory(projectParent, { recursive: true })
      .pipe(Effect.mapError(() => fail("target-invalid")));
    const canonicalParent = yield* fs
      .realPath(projectParent)
      .pipe(Effect.mapError(() => fail("target-invalid")));
    if (!isStrictlyInside(path, canonicalRoot, canonicalParent)) {
      return yield* fail("path-escape");
    }

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
    return { root: canonicalRoot, parent: canonicalParent, target };
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
  return { root, parent, target: lexicalTarget };
});
