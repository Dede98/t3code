import * as NodeServices from "@effect/platform-node/NodeServices";
import { AgentControlWorktreeReservationId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import { ServerConfig } from "../../config.ts";
import { deriveAgentControlWorktreePathKeys } from "./identity.ts";
import {
  deriveSafeAgentControlWorktreePath,
  releaseAgentControlWorktreeTargetPath,
  reserveAgentControlWorktreeTargetPath,
  validateExistingAgentControlWorktreePath,
} from "./pathSafety.ts";

const layer = it.layer(
  ServerConfig.layerTest(process.cwd(), {
    prefix: "agent-control-worktree-path-",
  }).pipe(Layer.provideMerge(NodeServices.layer)),
);
const ids = (suffix: string) => ({
  projectId: ProjectId.make(`path-project-${suffix}`),
  reservationId: AgentControlWorktreeReservationId.make(
    `worktree-reservation-${suffix.padEnd(64, "a").slice(0, 64)}`,
  ),
});

layer("Agent Control worktree path boundary", (it) => {
  it.effect("derives a target strictly inside the server-owned root", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* ServerConfig;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const { projectId, reservationId } = ids("inside");
      const result = yield* deriveSafeAgentControlWorktreePath({
        projectId,
        reservationId,
        repositoryWorkspace: repository,
      });
      assert.equal(path.relative(result.root, result.target).startsWith(".."), false);
      assert.notEqual(result.root, result.target);
      assert.notEqual(repository, result.target);
      assert.equal(
        result.root,
        yield* fs.realPath(path.join(config.worktreesDir, "agent-control")),
      );
    }),
  );

  it.effect("rejects a symlinked project parent that escapes the root", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const config = yield* ServerConfig;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const outside = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-outside-",
      });
      const { projectId, reservationId } = ids("symlink");
      const root = path.join(config.worktreesDir, "agent-control");
      yield* fs.makeDirectory(root, { recursive: true });
      const keys = deriveAgentControlWorktreePathKeys({ projectId, reservationId });
      yield* fs.symlink(outside, path.join(root, keys.projectKey));

      const result = yield* Effect.result(
        deriveSafeAgentControlWorktreePath({
          projectId,
          reservationId,
          repositoryWorkspace: repository,
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, "path-escape");
    }),
  );

  it.effect("refuses existing files, directories, and broken symlinks", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const { projectId, reservationId } = ids("occupied");
      const first = yield* deriveSafeAgentControlWorktreePath({
        projectId,
        reservationId,
        repositoryWorkspace: repository,
      });
      yield* fs.symlink(path.join(first.parent, "missing"), first.target);
      const result = yield* Effect.result(
        deriveSafeAgentControlWorktreePath({
          projectId,
          reservationId,
          repositoryWorkspace: repository,
        }),
      );
      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure") assert.equal(result.failure.reason, "target-exists");
    }),
  );

  it.effect("validates only the exact reserved lexical path", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const { projectId, reservationId } = ids("lexical");
      const safe = yield* deriveSafeAgentControlWorktreePath({
        projectId,
        reservationId,
        repositoryWorkspace: repository,
      });
      const escaped = yield* Effect.result(
        validateExistingAgentControlWorktreePath({
          target: path.join(safe.root, "..", "escaped"),
          repositoryWorkspace: repository,
        }),
      );
      assert.equal(escaped._tag, "Failure");
    }),
  );

  it.effect("rejects a writable parent and a parent identity swap before spawn", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const first = yield* deriveSafeAgentControlWorktreePath({
        ...ids("permissions"),
        repositoryWorkspace: repository,
      });
      yield* fs.chmod(first.parent, 0o777);
      const writable = yield* Effect.result(
        validateExistingAgentControlWorktreePath({
          target: first.target,
          repositoryWorkspace: repository,
        }),
      );
      assert.equal(writable._tag, "Failure");
      yield* fs.chmod(first.parent, 0o700);

      const replacement = `${first.parent}.replacement`;
      yield* fs.makeDirectory(replacement, { mode: 0o700 });
      yield* fs.rename(first.parent, `${first.parent}.original`);
      yield* fs.rename(replacement, first.parent);
      const swapped = yield* Effect.result(reserveAgentControlWorktreeTargetPath(first));
      assert.equal(swapped._tag, "Failure");
      assert.equal(yield* fs.exists(first.target), false);
      assert.equal(path.relative(first.root, first.target).startsWith(".."), false);
    }),
  );

  it.effect("keeps transient parent and lstat observations retryable", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repository = yield* fs.makeTempDirectoryScoped({
        prefix: "agent-control-path-repository-",
      });
      const safe = yield* deriveSafeAgentControlWorktreePath({
        ...ids("transient-observation"),
        repositoryWorkspace: repository,
      });
      const movedParent = `${safe.parent}.temporarily-missing`;
      yield* fs.rename(safe.parent, movedParent);
      const missingParent = yield* Effect.result(
        validateExistingAgentControlWorktreePath({
          target: safe.target,
          repositoryWorkspace: repository,
        }),
      );
      assert.equal(missingParent._tag, "Failure");
      if (missingParent._tag === "Failure") {
        assert.equal(missingParent.failure.reason, "observation-failed");
      }
      yield* fs.rename(movedParent, safe.parent);
      assert.equal(
        (yield* Effect.result(
          validateExistingAgentControlWorktreePath({
            target: safe.target,
            repositoryWorkspace: repository,
          }),
        ))._tag,
        "Success",
      );

      const lstatFailure = yield* Effect.acquireUseRelease(
        fs.chmod(path.dirname(safe.root), 0o000),
        () => Effect.result(reserveAgentControlWorktreeTargetPath(safe)),
        () => fs.chmod(path.dirname(safe.root), 0o700),
      );
      assert.equal(lstatFailure._tag, "Failure");
      if (lstatFailure._tag === "Failure") {
        assert.equal(lstatFailure.failure.reason, "observation-failed");
      }
      const reserved = yield* reserveAgentControlWorktreeTargetPath(safe);
      yield* releaseAgentControlWorktreeTargetPath(reserved);
    }),
  );
});
