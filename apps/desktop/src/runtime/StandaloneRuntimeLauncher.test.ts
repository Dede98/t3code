import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeArtifactManifest,
  type RuntimeArtifactManifest as RuntimeArtifactManifestValue,
  RuntimeBuildHash,
  RuntimeDaemonLock,
  RuntimeProfileId,
  RuntimeVersion,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { makeRuntimeProfileLayout } from "@t3tools/shared/runtimeProfile";
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
import * as RuntimeDaemonLaunchPlan from "./RuntimeDaemonLaunchPlan.ts";
import * as RuntimeProfileStore from "./RuntimeProfileStore.ts";
import * as StandaloneRuntimeLauncher from "./StandaloneRuntimeLauncher.ts";
import { STANDALONE_RUNTIME_LAUNCHER_SOURCE } from "./StandaloneRuntimeLauncherSource.ts";

const PROFILE_ID = RuntimeProfileId.make("dev");
const ENTRYPOINT = "apps/server/dist/bin.mjs" as const;
const NODE_EXECUTABLE = "node/bin/node" as const;
const CREATED_AT = "2026-07-22T00:00:00.000Z" as const;
const TARGET_PLATFORM = "darwin" as const;
const TARGET_ARCHITECTURE = "arm64" as const;

const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);
const encodeManifest = Schema.encodeSync(Schema.fromJsonString(RuntimeArtifactManifest));
const encodeLock = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonLock));
const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

interface Harness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly profilesRoot: string;
  readonly store: RuntimeProfileStore.RuntimeProfileStore["Service"];
  readonly installer: RuntimeArtifactInstaller.RuntimeArtifactInstaller["Service"];
  readonly planner: RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService["Service"];
  readonly launcher: StandaloneRuntimeLauncher.StandaloneRuntimeLauncher["Service"];
}

const makeHarness = Effect.fn("StandaloneRuntimeLauncherTest.makeHarness")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profilesRoot = yield* fs.makeTempDirectoryScoped({
    prefix: "t3 standalone profiles with spaces ",
  });
  const store = yield* RuntimeProfileStore.make({ profilesRoot });
  const installer = yield* RuntimeArtifactInstaller.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
  );
  const planner = yield* RuntimeDaemonLaunchPlan.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
    Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
    Effect.provideService(HostProcessPlatform, TARGET_PLATFORM),
    Effect.provideService(HostProcessArchitecture, TARGET_ARCHITECTURE),
  );
  const launcher = yield* StandaloneRuntimeLauncher.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
    Effect.provideService(RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService, planner),
  );
  return { fs, path, profilesRoot, store, installer, planner, launcher } satisfies Harness;
});

const ensureProfile = Effect.fn("StandaloneRuntimeLauncherTest.ensureProfile")(function* (
  harness: Harness,
) {
  yield* harness.store.ensureProfile({
    schemaVersion: 1,
    profileId: PROFILE_ID,
    port: 4773,
    createdAt: CREATED_AT,
    updatedAt: CREATED_AT,
  });
});

const makeFixture = Effect.fn("StandaloneRuntimeLauncherTest.makeFixture")(function* (
  harness: Harness,
  input: {
    readonly runtimeVersion: string;
    readonly buildHash: string;
    readonly nodeContents?: string;
  },
) {
  const crypto = yield* Crypto.Crypto;
  const sourceDirectory = yield* harness.fs.makeTempDirectory({
    prefix: "t3 launcher source checkout ",
  });
  yield* Effect.addFinalizer(() =>
    harness.fs.remove(sourceDirectory, { recursive: true, force: true }).pipe(Effect.orDie),
  );
  const files = new Map<string, string>([
    [ENTRYPOINT, "console.log('profile runtime');\n"],
    [NODE_EXECUTABLE, input.nodeContents ?? "profile node executable\n"],
  ]);
  const manifestFiles: Array<RuntimeArtifactManifestValue["files"][number]> = [];
  for (const [relativePath, contents] of files) {
    const filePath = harness.path.join(sourceDirectory, ...relativePath.split("/"));
    yield* harness.fs.makeDirectory(harness.path.dirname(filePath), { recursive: true });
    yield* harness.fs.writeFileString(filePath, contents);
    if (relativePath === NODE_EXECUTABLE) yield* harness.fs.chmod(filePath, 0o755);
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
    runtimeVersion: input.runtimeVersion,
    buildHash: input.buildHash,
    platform: TARGET_PLATFORM,
    architecture: TARGET_ARCHITECTURE,
    entrypoint: ENTRYPOINT,
    nodeExecutable: NODE_EXECUTABLE,
    files: manifestFiles,
  });
  yield* harness.fs.writeFileString(
    harness.path.join(sourceDirectory, "manifest.json"),
    encodeManifest(manifest),
  );
  return { sourceDirectory, manifest };
});

const installAndActivate = Effect.fn("StandaloneRuntimeLauncherTest.installAndActivate")(function* (
  harness: Harness,
  input: { readonly runtimeVersion: string; readonly buildHash: string },
) {
  const fixture = yield* makeFixture(harness, input);
  yield* harness.installer.installArtifact({
    profileId: PROFILE_ID,
    sourceDirectory: fixture.sourceDirectory,
    targetPlatform: TARGET_PLATFORM,
    targetArchitecture: TARGET_ARCHITECTURE,
  });
  yield* harness.installer.activateArtifact({
    profileId: PROFILE_ID,
    runtimeVersion: fixture.manifest.runtimeVersion,
    buildHash: fixture.manifest.buildHash,
    activatedAt: CREATED_AT,
  });
  return fixture;
});

describe("StandaloneRuntimeLauncher", () => {
  it("treats EPERM as alive and only ESRCH as dead during PID probes", () => {
    const signalError = (code: "EPERM" | "ESRCH") => {
      throw Object.assign(new Error(code), { code });
    };
    assert.equal(
      StandaloneRuntimeLauncher.probeRuntimeDaemonPid(1, () => signalError("EPERM")),
      "alive",
    );
    assert.equal(
      StandaloneRuntimeLauncher.probeRuntimeDaemonPid(1, () => signalError("ESRCH")),
      "dead",
    );
  });

  it.effect("materializes an atomic profile-only launcher that survives source removal", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* ensureProfile(harness);
        const fixture = yield* installAndActivate(harness, {
          runtimeVersion: "0.0.29",
          buildHash: "0123456789abcdef",
        });

        const materialized = yield* harness.launcher.materialize(PROFILE_ID);
        yield* harness.fs.remove(fixture.sourceDirectory, { recursive: true });
        const inspected = Option.getOrThrow(yield* harness.launcher.inspect(PROFILE_ID));
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);

        assert.equal(materialized.status, "installed");
        assert.equal((yield* harness.launcher.materialize(PROFILE_ID)).status, "already-installed");
        assert.strictEqual(inspected.paths.nodePath, materialized.paths.nodePath);
        assert.isTrue(materialized.paths.nodePath.startsWith(`${layout.launcherDirectory}/`));
        assert.isTrue(
          materialized.config.launchPlan.serverEntrypointPath.startsWith(
            `${yield* harness.fs.realPath(layout.versionsDirectory)}/`,
          ),
        );
        assert.notInclude(encodeUnknownJson(materialized), fixture.sourceDirectory);
        assert.notInclude(encodeUnknownJson(materialized), process.cwd());
        assert.notInclude(STANDALONE_RUNTIME_LAUNCHER_SOURCE, "@t3tools/");
        assert.match(STANDALONE_RUNTIME_LAUNCHER_SOURCE, /node:child_process/u);
        assert.equal(materialized.config.schemaVersion, 2);
        assert.deepEqual(materialized.config.recovery, {
          schemaVersion: 1,
          enabled: true,
          maxRestarts: 5,
          slidingWindowMs: 300_000,
          initialBackoffMs: 1_000,
          maxBackoffMs: 30_000,
          healthcheckIntervalMs: 30_000,
          consecutiveHealthFailuresBeforeRestart: 3,
          healthyResetAfterMs: 300_000,
        });
        assert.include(STANDALONE_RUNTIME_LAUNCHER_SOURCE, "writeRecoveryAtomically");
        assert.include(STANDALONE_RUNTIME_LAUNCHER_SOURCE, "await handle.sync()");
        assert.include(STANDALONE_RUNTIME_LAUNCHER_SOURCE, "await fs.rename(temporaryPath");
        assert.notMatch(
          encodeUnknownJson(materialized.config.recovery),
          /(?:command|stderr|secret|token|credential)/iu,
        );
        assert.include(materialized.config.launchPlan.argv[0] ?? "", "profiles with spaces");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rechecks Node size and digest before materialization", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* ensureProfile(harness);
        yield* installAndActivate(harness, {
          runtimeVersion: "0.0.29",
          buildHash: "0123456789abcdef",
        });
        const plan = yield* harness.planner.create(PROFILE_ID);

        yield* harness.fs.writeFileString(plan.nodeExecutablePath, "tampered node executable\n");
        const sizeError = yield* harness.launcher.materializePlan(plan).pipe(Effect.flip);
        assert.equal(sizeError._tag, "RuntimeArtifactSizeMismatchError");

        yield* harness.fs.writeFileString(
          plan.nodeExecutablePath,
          "x".repeat(new TextEncoder().encode("profile node executable\n").byteLength),
        );
        const digestError = yield* harness.launcher.materializePlan(plan).pipe(Effect.flip);
        assert.equal(digestError._tag, "RuntimeArtifactDigestMismatchError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a non-executable or symlinked runtime Node", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* ensureProfile(harness);
        yield* installAndActivate(harness, {
          runtimeVersion: "0.0.29",
          buildHash: "0123456789abcdef",
        });
        const plan = yield* harness.planner.create(PROFILE_ID);
        yield* harness.fs.chmod(plan.nodeExecutablePath, 0o644);
        const executableError = yield* harness.launcher.materializePlan(plan).pipe(Effect.flip);
        assert.equal(executableError._tag, "RuntimeDaemonLifecycleError");
        if (executableError._tag === "RuntimeDaemonLifecycleError") {
          assert.equal(executableError.code, "launcher-invalid");
        }

        yield* harness.fs.chmod(plan.nodeExecutablePath, 0o755);
        const externalDirectory = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "t3 external node ",
        });
        const externalNode = harness.path.join(externalDirectory, "node");
        yield* harness.fs.writeFileString(externalNode, "profile node executable\n");
        yield* harness.fs.chmod(externalNode, 0o755);
        yield* harness.fs.remove(plan.nodeExecutablePath);
        yield* harness.fs.symlink(externalNode, plan.nodeExecutablePath);
        const symlinkError = yield* harness.launcher.materializePlan(plan).pipe(Effect.flip);
        assert.equal(symlinkError._tag, "RuntimeArtifactPathEscapeError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not activate an incomplete launcher installation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* ensureProfile(harness);
        yield* installAndActivate(harness, {
          runtimeVersion: "0.0.29",
          buildHash: "0123456789abcdef",
        });
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, PROFILE_ID);
        const renameError = PlatformError.systemError({
          _tag: "PermissionDenied",
          module: "FileSystem",
          method: "rename",
        });
        const failingFileSystem = FileSystem.FileSystem.of({
          ...harness.fs,
          rename: (oldPath, newPath) =>
            newPath === harness.path.join(layout.launcherDirectory, "installation.json")
              ? Effect.fail(renameError)
              : harness.fs.rename(oldPath, newPath),
        });
        const failingLauncher = yield* StandaloneRuntimeLauncher.make({
          profilesRoot: harness.profilesRoot,
        }).pipe(
          Effect.provideService(
            RuntimeArtifactInstaller.RuntimeArtifactInstaller,
            harness.installer,
          ),
          Effect.provideService(
            RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService,
            harness.planner,
          ),
          Effect.provideService(FileSystem.FileSystem, failingFileSystem),
        );

        const error = yield* failingLauncher.materialize(PROFILE_ID).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeDaemonLifecycleError");
        assert.isFalse(
          yield* harness.fs.exists(
            harness.path.join(layout.launcherDirectory, "installation.json"),
          ),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects a launcher update while a live lock owner exists", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* ensureProfile(harness);
        yield* installAndActivate(harness, {
          runtimeVersion: "0.0.29",
          buildHash: "0123456789abcdef",
        });
        const first = yield* harness.launcher.materialize(PROFILE_ID);
        yield* installAndActivate(harness, {
          runtimeVersion: "0.0.30",
          buildHash: "abcdef0123456789",
        });
        const nextPlan = yield* harness.planner.create(PROFILE_ID);
        yield* harness.fs.writeFileString(
          nextPlan.daemonLockPath,
          encodeLock({
            schemaVersion: 1,
            profileId: PROFILE_ID,
            launcherPid: process.pid,
            ownershipId: "a".repeat(32) as never,
            createdAt: CREATED_AT,
            runtimeVersion: RuntimeVersion.make("0.0.29"),
            buildHash: RuntimeBuildHash.make("0123456789abcdef"),
          }),
        );

        const error = yield* harness.launcher.materializePlan(nextPlan).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeDaemonLifecycleError");
        if (error._tag === "RuntimeDaemonLifecycleError") {
          assert.equal(error.code, "launcher-already-running");
        }
        const current = Option.getOrThrow(yield* harness.launcher.inspect(PROFILE_ID));
        assert.equal(current.installation.installationId, first.installation.installationId);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
