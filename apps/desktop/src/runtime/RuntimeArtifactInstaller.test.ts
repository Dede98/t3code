import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeArtifactManifest,
  type RuntimeArtifactManifest as RuntimeArtifactManifestValue,
  RuntimeBuildHash,
  RuntimeCurrentPointer,
  RuntimeProfileId,
  RuntimeVersion,
} from "@t3tools/contracts/runtimeProfile";
import {
  makeRuntimeProfileLayout,
  runtimeArtifactVersionDirectory,
} from "@t3tools/shared/runtimeProfile";
import { assert, describe, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import * as RuntimeArtifactInstaller from "./RuntimeArtifactInstaller.ts";
import * as RuntimeProfileStore from "./RuntimeProfileStore.ts";

const PROFILE_ID = RuntimeProfileId.make("dev");
const TARGET_PLATFORM = "darwin" as const;
const TARGET_ARCHITECTURE = "arm64" as const;
const DEFAULT_VERSION = RuntimeVersion.make("0.0.29");
const DEFAULT_HASH = RuntimeBuildHash.make("0123456789abcdef");
const ENTRYPOINT = "apps/server/dist/bin.mjs" as const;
const NODE_EXECUTABLE = "node/bin/node" as const;

const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);
const encodeManifest = Schema.encodeSync(Schema.fromJsonString(RuntimeArtifactManifest));
const encodePointer = Schema.encodeSync(Schema.fromJsonString(RuntimeCurrentPointer));

interface RuntimeHarness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly profilesRoot: string;
  readonly store: RuntimeProfileStore.RuntimeProfileStore["Service"];
  readonly installer: RuntimeArtifactInstaller.RuntimeArtifactInstaller["Service"];
}

const makeHarness = Effect.fn("RuntimeArtifactInstallerTest.makeHarness")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3-artifact-profiles-" });
  const store = yield* RuntimeProfileStore.make({ profilesRoot });
  yield* store.ensureProfile({
    schemaVersion: 1,
    profileId: PROFILE_ID,
    port: 3773,
    createdAt: "2026-07-22T00:00:00.000Z",
    updatedAt: "2026-07-22T00:00:00.000Z",
  });
  const installer = yield* RuntimeArtifactInstaller.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
  );
  return { fs, path, profilesRoot, store, installer } satisfies RuntimeHarness;
});

const makeFixture = Effect.fn("RuntimeArtifactInstallerTest.makeFixture")(function* (input?: {
  readonly runtimeVersion?: RuntimeVersion;
  readonly buildHash?: RuntimeBuildHash;
  readonly entrypointContents?: string;
  readonly nodeContents?: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const sourceDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-artifact-source-" });
  const files = new Map<string, string>([
    [ENTRYPOINT, input?.entrypointContents ?? "console.log('runtime');\n"],
    [NODE_EXECUTABLE, input?.nodeContents ?? "node-binary\n"],
    ["node_modules/runtime/package.json", '{"name":"runtime"}\n'],
  ]);
  const manifestFiles: Array<RuntimeArtifactManifestValue["files"][number]> = [];
  for (const [relativePath, contents] of files) {
    const filePath = path.join(sourceDirectory, ...relativePath.split("/"));
    yield* fs.makeDirectory(path.dirname(filePath), { recursive: true });
    yield* fs.writeFileString(filePath, contents);
    const bytes = new TextEncoder().encode(contents);
    const digest = yield* crypto.digest("SHA-256", bytes);
    manifestFiles.push({
      path: relativePath as never,
      byteSize: bytes.byteLength,
      sha256: Encoding.encodeHex(digest) as never,
    });
  }
  const manifest = decodeManifest({
    schemaVersion: 1,
    runtimeVersion: input?.runtimeVersion ?? DEFAULT_VERSION,
    buildHash: input?.buildHash ?? DEFAULT_HASH,
    platform: TARGET_PLATFORM,
    architecture: TARGET_ARCHITECTURE,
    entrypoint: ENTRYPOINT,
    nodeExecutable: NODE_EXECUTABLE,
    files: manifestFiles,
  });
  yield* fs.writeFileString(path.join(sourceDirectory, "manifest.json"), encodeManifest(manifest));
  return { sourceDirectory, manifest };
});

const installInput = (sourceDirectory: string) => ({
  profileId: PROFILE_ID,
  sourceDirectory,
  targetPlatform: TARGET_PLATFORM,
  targetArchitecture: TARGET_ARCHITECTURE,
});

const identity = (manifest: RuntimeArtifactManifestValue) => ({
  profileId: PROFILE_ID,
  runtimeVersion: manifest.runtimeVersion,
  buildHash: manifest.buildHash,
});

describe("RuntimeArtifactInstaller", () => {
  it.effect("validates, installs, lists, and idempotently reuses an independent copy", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const fixture = yield* makeFixture();

        assert.deepEqual(
          yield* harness.installer.validateArtifact(installInput(fixture.sourceDirectory)),
          fixture.manifest,
        );
        const installed = yield* harness.installer.installArtifact(
          installInput(fixture.sourceDirectory),
        );
        assert.equal(installed.status, "installed");
        assert.isTrue(Option.isNone(yield* harness.store.getCurrentRuntime(PROFILE_ID)));

        const artifact = yield* harness.installer.getInstalledArtifact(identity(fixture.manifest));
        assert.notEqual(artifact.installDirectory, fixture.sourceDirectory);
        assert.isTrue(artifact.entrypointPath.startsWith(artifact.installDirectory));
        assert.isTrue(artifact.nodeExecutablePath.startsWith(artifact.installDirectory));
        const repeated = yield* harness.installer.installArtifact(
          installInput(fixture.sourceDirectory),
        );
        assert.equal(repeated.status, "already-installed");

        yield* harness.fs.writeFileString(
          harness.path.join(fixture.sourceDirectory, ENTRYPOINT),
          "source changed after install\n",
        );
        assert.equal(
          yield* harness.fs.readFileString(artifact.entrypointPath),
          "console.log('runtime');\n",
        );
        assert.lengthOf(yield* harness.installer.listInstalledArtifacts(PROFILE_ID), 1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports digest, size, and missing-file failures precisely", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();

        const digestFixture = yield* makeFixture();
        yield* harness.fs.writeFileString(
          harness.path.join(digestFixture.sourceDirectory, ENTRYPOINT),
          "Console.log('runtime');\n",
        );
        const digestError = yield* harness.installer
          .validateArtifact(installInput(digestFixture.sourceDirectory))
          .pipe(Effect.flip);
        assert.equal(digestError._tag, "RuntimeArtifactDigestMismatchError");

        const sizeFixture = yield* makeFixture();
        yield* harness.fs.writeFileString(
          harness.path.join(sizeFixture.sourceDirectory, ENTRYPOINT),
          "console.log('runtime'); extra\n",
        );
        const sizeError = yield* harness.installer
          .validateArtifact(installInput(sizeFixture.sourceDirectory))
          .pipe(Effect.flip);
        assert.equal(sizeError._tag, "RuntimeArtifactSizeMismatchError");

        const missingFixture = yield* makeFixture();
        yield* harness.fs.remove(harness.path.join(missingFixture.sourceDirectory, ENTRYPOINT));
        const missingError = yield* harness.installer
          .validateArtifact(installInput(missingFixture.sourceDirectory))
          .pipe(Effect.flip);
        assert.equal(missingError._tag, "RuntimeArtifactFileMissingError");

        const missingNodeFixture = yield* makeFixture();
        yield* harness.fs.remove(
          harness.path.join(missingNodeFixture.sourceDirectory, NODE_EXECUTABLE),
        );
        const missingNodeError = yield* harness.installer
          .validateArtifact(installInput(missingNodeFixture.sourceDirectory))
          .pipe(Effect.flip);
        assert.equal(missingNodeError._tag, "RuntimeArtifactFileMissingError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects absolute paths, traversal, and symlink escapes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();

        for (const unsafePath of ["/tmp/external/bin.mjs", "../external/bin.mjs"]) {
          const fixture = yield* makeFixture();
          const unsafeManifest = encodeManifest(fixture.manifest).replaceAll(
            ENTRYPOINT,
            unsafePath,
          );
          yield* harness.fs.writeFileString(
            harness.path.join(fixture.sourceDirectory, "manifest.json"),
            unsafeManifest,
          );
          const error = yield* harness.installer
            .validateArtifact(installInput(fixture.sourceDirectory))
            .pipe(Effect.flip);
          assert.equal(error._tag, "RuntimeArtifactPathEscapeError");
        }

        const symlinkFixture = yield* makeFixture();
        const externalDirectory = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "t3-artifact-external-",
        });
        const externalFile = harness.path.join(externalDirectory, "bin.mjs");
        yield* harness.fs.writeFileString(externalFile, "console.log('runtime');\n");
        const entrypointPath = harness.path.join(symlinkFixture.sourceDirectory, ENTRYPOINT);
        yield* harness.fs.remove(entrypointPath);
        yield* harness.fs.symlink(externalFile, entrypointPath);

        const symlinkError = yield* harness.installer
          .validateArtifact(installInput(symlinkFixture.sourceDirectory))
          .pipe(Effect.flip);
        assert.equal(symlinkError._tag, "RuntimeArtifactPathEscapeError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a target platform or architecture mismatch", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const fixture = yield* makeFixture();

        const error = yield* harness.installer
          .validateArtifact({
            sourceDirectory: fixture.sourceDirectory,
            targetPlatform: TARGET_PLATFORM,
            targetArchitecture: "x64",
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeArtifactPlatformMismatchError");

        const platformError = yield* harness.installer
          .validateArtifact({
            sourceDirectory: fixture.sourceDirectory,
            targetPlatform: "linux",
            targetArchitecture: TARGET_ARCHITECTURE,
          })
          .pipe(Effect.flip);
        assert.equal(platformError._tag, "RuntimeArtifactPlatformMismatchError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "detects same-directory identity conflicts and leaves no active or temporary version",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness();
          const first = yield* makeFixture();
          yield* harness.installer.installArtifact(installInput(first.sourceDirectory));

          const conflicting = yield* makeFixture({ entrypointContents: "different runtime\n" });
          const conflict = yield* harness.installer
            .installArtifact(installInput(conflicting.sourceDirectory))
            .pipe(Effect.flip);
          assert.equal(conflict._tag, "RuntimeArtifactInstallConflictError");

          const bad = yield* makeFixture({
            runtimeVersion: RuntimeVersion.make("0.0.30"),
            buildHash: RuntimeBuildHash.make("abcdef0123456789"),
          });
          yield* harness.fs.writeFileString(
            harness.path.join(bad.sourceDirectory, ENTRYPOINT),
            "tampered\n",
          );
          yield* harness.installer
            .installArtifact(installInput(bad.sourceDirectory))
            .pipe(Effect.flip);
          const missingInstall = yield* harness.installer
            .getInstalledArtifact(identity(bad.manifest))
            .pipe(Effect.flip);
          assert.equal(missingInstall._tag, "RuntimeArtifactNotInstalledError");

          const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);
          assert.deepEqual(
            (yield* harness.fs.readDirectory(layout.versionsDirectory)).filter((name) =>
              name.startsWith(".install-"),
            ),
            [],
          );
          assert.isTrue(Option.isNone(yield* harness.store.getCurrentRuntime(PROFILE_ID)));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("atomically activates a second version while retaining the first for rollback", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const first = yield* makeFixture();
        const second = yield* makeFixture({
          runtimeVersion: RuntimeVersion.make("0.0.30"),
          buildHash: RuntimeBuildHash.make("abcdef0123456789"),
        });
        yield* harness.installer.installArtifact(installInput(first.sourceDirectory));
        yield* harness.installer.installArtifact(installInput(second.sourceDirectory));

        const firstPointer = yield* harness.installer.activateArtifact({
          ...identity(first.manifest),
          activatedAt: "2026-07-22T01:00:00.000Z",
        });
        assert.deepEqual(
          Option.getOrThrow(yield* harness.store.getCurrentRuntime(PROFILE_ID)),
          firstPointer,
        );
        const secondPointer = yield* harness.installer.activateArtifact({
          ...identity(second.manifest),
          activatedAt: "2026-07-22T02:00:00.000Z",
        });
        assert.deepEqual(
          Option.getOrThrow(yield* harness.store.getCurrentRuntime(PROFILE_ID)),
          secondPointer,
        );

        const installed = yield* harness.installer.listInstalledArtifacts(PROFILE_ID);
        assert.lengthOf(installed, 2);
        assert.deepEqual(
          installed.map((artifact) => artifact.manifest.runtimeVersion),
          [first.manifest.runtimeVersion, second.manifest.runtimeVersion],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects corrupt current pointers aimed at app bundles or worktrees", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const fixture = yield* makeFixture();
        yield* harness.installer.installArtifact(installInput(fixture.sourceDirectory));
        const pointer = yield* harness.installer.activateArtifact(identity(fixture.manifest));
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);
        const externalRoot = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "t3-pointer-external-",
        });

        for (const externalPointer of [
          harness.path.join(externalRoot, "T3 Code.app", "Contents", "Resources"),
          harness.path.join(externalRoot, "workspace", "apps", "server", "dist"),
          harness.path.join(externalRoot, "workspace", "node_modules"),
          fixture.sourceDirectory,
        ]) {
          const corruptPointer = encodePointer(pointer).replace(
            pointer.versionDirectory,
            externalPointer,
          );
          yield* harness.fs.writeFileString(layout.currentRuntimePath, corruptPointer);
          const error = yield* harness.store.getCurrentRuntime(PROFILE_ID).pipe(Effect.flip);
          assert.equal(error._tag, "RuntimeCurrentPointerCorruptError");
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("cannot activate an installed-version symlink to an app bundle or worktree", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const fixture = yield* makeFixture();
        yield* harness.installer.installArtifact(installInput(fixture.sourceDirectory));
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);
        const installDirectory = runtimeArtifactVersionDirectory(
          harness.path,
          layout,
          fixture.manifest,
        );
        yield* harness.fs.remove(installDirectory, { recursive: true });

        for (const suffix of ["T3 Code.app", "workspace/apps/server/dist"]) {
          const externalRoot = yield* harness.fs.makeTempDirectoryScoped({
            prefix: "t3-external-runtime-",
          });
          const externalDirectory = harness.path.join(externalRoot, ...suffix.split("/"));
          yield* harness.fs.makeDirectory(externalDirectory, { recursive: true });
          yield* harness.fs.symlink(externalDirectory, installDirectory);

          const error = yield* harness.installer
            .activateArtifact(identity(fixture.manifest))
            .pipe(Effect.flip);
          assert.equal(error._tag, "RuntimeArtifactPathEscapeError");
          yield* harness.fs.remove(installDirectory);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("preserves the old pointer when an atomic pointer rename fails", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const first = yield* makeFixture();
        const second = yield* makeFixture({
          runtimeVersion: RuntimeVersion.make("0.0.30"),
          buildHash: RuntimeBuildHash.make("abcdef0123456789"),
        });
        yield* harness.installer.installArtifact(installInput(first.sourceDirectory));
        yield* harness.installer.installArtifact(installInput(second.sourceDirectory));
        const firstPointer = yield* harness.installer.activateArtifact(identity(first.manifest));
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);
        const renameError = PlatformError.systemError({
          _tag: "PermissionDenied",
          module: "FileSystem",
          method: "rename",
        });
        const failingFileSystem = FileSystem.FileSystem.of({
          ...harness.fs,
          rename: (oldPath, newPath) =>
            newPath === layout.currentRuntimePath
              ? Effect.fail(renameError)
              : harness.fs.rename(oldPath, newPath),
        });
        const failingInstaller = yield* RuntimeArtifactInstaller.make({
          profilesRoot: harness.profilesRoot,
        }).pipe(
          Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, harness.store),
          Effect.provideService(FileSystem.FileSystem, failingFileSystem),
        );

        const error = yield* failingInstaller
          .activateArtifact(identity(second.manifest))
          .pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeFilesystemError");
        if (error._tag === "RuntimeFilesystemError") {
          assert.equal(error.operation, "rename");
        }
        assert.deepEqual(
          Option.getOrThrow(yield* harness.store.getCurrentRuntime(PROFILE_ID)),
          firstPointer,
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
