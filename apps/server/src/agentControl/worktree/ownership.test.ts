// @effect-diagnostics nodeBuiltinImport:off - exercises symlink, hardlink, FIFO, and inode metadata.
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeChildProcess from "node:child_process";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import {
  type AgentControlWorktreeOwnershipMarker,
  inspectAgentControlWorktreeOwnershipMarker,
  writeAgentControlWorktreeOwnershipMarker,
} from "./ownership.ts";

const layer = it.layer(NodeServices.layer);
const marker: AgentControlWorktreeOwnershipMarker = {
  schemaVersion: 1,
  reservationId: AgentControlWorktreeReservationId.make("ownership-test-reservation"),
  projectId: ProjectId.make("ownership-test-project"),
  taskId: AgentControlTaskId.make("ownership-test-task"),
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: AgentControlStageRunId.make("ownership-test-stage"),
  attemptId: AgentControlAttemptId.make("ownership-test-attempt"),
  leaseId: AgentControlStageRunLeaseId.make("ownership-test-lease"),
  fenceToken: 1,
  repositoryNodeId: "repository-node",
  repositoryCanonicalKey: "github.com/owner/repository",
  repositoryCommonDir: "/tmp/repository/.git",
  repositoryCommonDirDevice: 1,
  repositoryCommonDirInode: 2,
  branchName: "t3auto/issue-1-test",
  baseCommitSha: "b".repeat(40),
  targetGenerationId: "c".repeat(64),
  worktreeRootDevice: 1,
  worktreeRootInode: 2,
  worktreeParentDevice: 1,
  worktreeParentInode: 3,
};

const inspect = Effect.fn("ownershipTestInspect")(function* (
  markerPath: string,
  expectedUid = typeof process.getuid === "function" ? process.getuid() : null,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const info = yield* fs.stat(path.dirname(markerPath));
  return yield* Effect.result(
    inspectAgentControlWorktreeOwnershipMarker({
      markerPath,
      expectedDevice: info.dev,
      expectedUid,
    }),
  );
});

layer("Agent Control ownership marker metadata", (it) => {
  it.effect("writes a mode-0600 single-link marker and never overwrites it", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ownership-marker-" });
      const markerPath = path.join(directory, "marker.json");
      yield* Effect.scoped(
        writeAgentControlWorktreeOwnershipMarker(markerPath, marker).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        ),
      );
      const info = yield* fs.stat(markerPath);
      assert.equal(info.type, "File");
      assert.equal(info.mode & 0o777, 0o600);
      assert.equal(Option.getOrNull(info.nlink), 1);
      assert.equal((yield* inspect(markerPath))._tag, "Success");
      assert.equal(
        (yield* Effect.result(
          Effect.scoped(
            writeAgentControlWorktreeOwnershipMarker(markerPath, marker).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
            ),
          ),
        ))._tag,
        "Failure",
      );
    }),
  );

  for (const kind of ["hardlink", "symlink", "directory", "fifo", "mode", "owner"] as const) {
    it.effect(`rejects ${kind} marker metadata`, () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: `ownership-marker-${kind}-`,
        });
        const markerPath = path.join(directory, "marker.json");
        if (kind === "directory") {
          yield* fs.makeDirectory(markerPath);
        } else if (kind === "fifo") {
          const made = NodeChildProcess.spawnSync("mkfifo", [markerPath]);
          assert.equal(made.status, 0);
        } else {
          yield* Effect.scoped(
            writeAgentControlWorktreeOwnershipMarker(markerPath, marker).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
            ),
          );
          if (kind === "hardlink") {
            yield* fs.link(markerPath, path.join(directory, "second-link"));
          } else if (kind === "symlink") {
            const target = path.join(directory, "target.json");
            yield* fs.rename(markerPath, target);
            yield* fs.symlink(target, markerPath);
          } else if (kind === "mode") {
            yield* fs.chmod(markerPath, 0o644);
          }
        }
        const result = yield* inspect(
          markerPath,
          kind === "owner" && typeof process.getuid === "function"
            ? process.getuid() + 1
            : undefined,
        );
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.reason, "unsafe-type");
      }),
    );
  }

  it.effect("classifies invalid marker payloads as retryable corrupt observations", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "ownership-marker-corrupt-",
      });
      const markerPath = path.join(directory, "marker.json");
      yield* fs.writeFileString(markerPath, "{not-json");
      yield* fs.chmod(markerPath, 0o600);
      const corrupt = yield* inspect(markerPath);
      assert.equal(corrupt._tag, "Failure");
      if (corrupt._tag === "Failure") {
        assert.equal(corrupt.failure.reason, "corrupt");
      }
      yield* fs.remove(markerPath);
      yield* Effect.scoped(
        writeAgentControlWorktreeOwnershipMarker(markerPath, marker).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        ),
      );
      assert.equal((yield* inspect(markerPath))._tag, "Success");
    }),
  );

  it.effect("retries a transient marker read I/O failure", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "ownership-marker-io-",
      });
      const markerPath = path.join(directory, "marker.json");
      yield* Effect.scoped(
        writeAgentControlWorktreeOwnershipMarker(markerPath, marker).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        ),
      );
      const unavailable = yield* Effect.acquireUseRelease(
        fs.chmod(directory, 0o000),
        () => inspect(markerPath),
        () => fs.chmod(directory, 0o700),
      );
      assert.equal(unavailable._tag, "Failure");
      if (unavailable._tag === "Failure") {
        assert.equal(unavailable.failure.reason, "io");
      }
      assert.equal((yield* inspect(markerPath))._tag, "Success");
    }),
  );
});
