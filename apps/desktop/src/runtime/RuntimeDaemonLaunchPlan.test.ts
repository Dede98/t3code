import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeArtifactManifest,
  type RuntimeArtifactManifest as RuntimeArtifactManifestValue,
  type RuntimeArtifactArchitecture,
  type RuntimeArtifactPlatform,
  RuntimeBuildHash,
  RuntimeDaemonLaunchPlan as RuntimeDaemonLaunchPlanSchema,
  RuntimeProfileId,
  type RuntimeProfileId as RuntimeProfileIdValue,
  RuntimeVersion,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import {
  isPathWithin,
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
import * as Schema from "effect/Schema";

import * as RuntimeArtifactInstaller from "./RuntimeArtifactInstaller.ts";
import * as RuntimeDaemonLaunchPlan from "./RuntimeDaemonLaunchPlan.ts";
import * as RuntimeProfileStore from "./RuntimeProfileStore.ts";

const TARGET_PLATFORM = "darwin" as const;
const TARGET_ARCHITECTURE = "arm64" as const;
const RUNTIME_VERSION = RuntimeVersion.make("0.0.29");
const BUILD_HASH = RuntimeBuildHash.make("0123456789abcdef");
const ENTRYPOINT = "apps/server/dist/bin.mjs" as const;
const NODE_EXECUTABLE = "node/bin/node" as const;
const CREATED_AT = "2026-07-22T00:00:00.000Z" as const;

const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);
const encodeManifest = Schema.encodeSync(Schema.fromJsonString(RuntimeArtifactManifest));
const encodeLaunchPlan = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonLaunchPlanSchema));
const encodeUnknownJson = Schema.encodeSync(Schema.UnknownFromJsonString);

interface RuntimeHarness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly profilesRoot: string;
  readonly store: RuntimeProfileStore.RuntimeProfileStore["Service"];
  readonly installer: RuntimeArtifactInstaller.RuntimeArtifactInstaller["Service"];
  readonly planner: RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService["Service"];
}

const makeHarness = Effect.fn("RuntimeDaemonLaunchPlanTest.makeHarness")(function* (options?: {
  readonly platform?: RuntimeArtifactPlatform;
  readonly architecture?: RuntimeArtifactArchitecture;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profilesRoot = yield* fs.makeTempDirectoryScoped({
    prefix: "t3 daemon profiles with spaces ",
  });
  const store = yield* RuntimeProfileStore.make({ profilesRoot });
  const installer = yield* RuntimeArtifactInstaller.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
  );
  const planner = yield* RuntimeDaemonLaunchPlan.make({ profilesRoot }).pipe(
    Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
    Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
    Effect.provideService(HostProcessPlatform, options?.platform ?? TARGET_PLATFORM),
    Effect.provideService(HostProcessArchitecture, options?.architecture ?? TARGET_ARCHITECTURE),
  );
  return { fs, path, profilesRoot, store, installer, planner } satisfies RuntimeHarness;
});

const ensureProfile = Effect.fn("RuntimeDaemonLaunchPlanTest.ensureProfile")(function* (
  harness: RuntimeHarness,
  profileId: RuntimeProfileIdValue,
  port: number,
) {
  return yield* harness.store.ensureProfile({
    schemaVersion: 1,
    profileId,
    port,
    createdAt: CREATED_AT,
    updatedAt: CREATED_AT,
  });
});

const makeFixture = Effect.fn("RuntimeDaemonLaunchPlanTest.makeFixture")(function* (
  harness: RuntimeHarness,
  options?: {
    readonly platform?: RuntimeArtifactPlatform;
    readonly architecture?: RuntimeArtifactArchitecture;
    readonly executable?: boolean;
    readonly sourceDirectory?: string;
  },
) {
  const crypto = yield* Crypto.Crypto;
  const sourceDirectory =
    options?.sourceDirectory ??
    (yield* harness.fs.makeTempDirectoryScoped({ prefix: "t3 source checkout " }));
  yield* harness.fs.makeDirectory(sourceDirectory, { recursive: true });
  const files = new Map<string, string>([
    [ENTRYPOINT, "console.log('independent runtime');\n"],
    [NODE_EXECUTABLE, "independent node binary\n"],
    ["node_modules/runtime/package.json", '{"name":"runtime"}\n'],
  ]);
  const manifestFiles: Array<RuntimeArtifactManifestValue["files"][number]> = [];
  for (const [relativePath, contents] of files) {
    const filePath = harness.path.join(sourceDirectory, ...relativePath.split("/"));
    yield* harness.fs.makeDirectory(harness.path.dirname(filePath), { recursive: true });
    yield* harness.fs.writeFileString(filePath, contents);
    if (relativePath === NODE_EXECUTABLE) {
      yield* harness.fs.chmod(filePath, options?.executable === false ? 0o644 : 0o755);
    }
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
    runtimeVersion: RUNTIME_VERSION,
    buildHash: BUILD_HASH,
    platform: options?.platform ?? TARGET_PLATFORM,
    architecture: options?.architecture ?? TARGET_ARCHITECTURE,
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

const installAndActivate = Effect.fn("RuntimeDaemonLaunchPlanTest.installAndActivate")(function* (
  harness: RuntimeHarness,
  profileId: RuntimeProfileIdValue,
  port: number,
  options?: Parameters<typeof makeFixture>[1],
) {
  yield* ensureProfile(harness, profileId, port);
  const fixture = yield* makeFixture(harness, options);
  yield* harness.installer.installArtifact({
    profileId,
    sourceDirectory: fixture.sourceDirectory,
    targetPlatform: fixture.manifest.platform,
    targetArchitecture: fixture.manifest.architecture,
  });
  yield* harness.installer.activateArtifact({
    profileId,
    runtimeVersion: fixture.manifest.runtimeVersion,
    buildHash: fixture.manifest.buildHash,
    activatedAt: "2026-07-22T01:00:00.000Z",
  });
  return fixture;
});

describe("RuntimeDaemonLaunchPlan", () => {
  it.effect("builds deterministic plans for built-in and custom profiles", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profiles = [
          [RuntimeProfileId.make("dev"), 3773],
          [RuntimeProfileId.make("alpha"), 3774],
          [RuntimeProfileId.make("nightly"), 3775],
          [RuntimeProfileId.make("custom:mac-mini"), 4773],
        ] as const;

        for (const [profileId, port] of profiles) {
          yield* installAndActivate(harness, profileId, port);
          const plan = yield* harness.planner.create(profileId);
          const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);
          assert.equal(plan.profileId, profileId);
          assert.equal(plan.port, port);
          assert.equal(plan.origin, `http://127.0.0.1:${port}`);
          assert.equal(plan.cwd, layout.profileDirectory);
          assert.equal(plan.stateDirectory, layout.stateDirectory);
          assert.equal(plan.logsDirectory, layout.logsDirectory);
          assert.equal(plan.runDirectory, layout.runDirectory);
          assert.equal(plan.daemonLockPath, layout.daemonLockPath);
          assert.equal(plan.discoveryPath, layout.discoveryPath);
          assert.isTrue(plan.preflight.ok);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("uses only the active independent artifact and keeps spaced paths as argv values", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profileId = RuntimeProfileId.make("dev");
        const externalRoot = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "external app source ",
        });
        const sourceDirectory = harness.path.join(
          externalRoot,
          "T3 Code.app",
          "Contents",
          "worktree source",
        );
        const fixture = yield* installAndActivate(harness, profileId, 4888, {
          sourceDirectory,
        });
        const plan = yield* harness.planner.create(profileId);
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);

        assert.equal(plan.argv[0], plan.serverEntrypointPath);
        assert.equal(plan.argv[1], "start");
        assert.equal(plan.argv[plan.argv.indexOf("--host") + 1], "127.0.0.1");
        assert.equal(plan.argv[plan.argv.indexOf("--port") + 1], "4888");
        assert.equal(plan.argv[plan.argv.indexOf("--base-dir") + 1], layout.profileDirectory);
        assert.equal(plan.argv[plan.argv.indexOf("--state-dir") + 1], layout.stateDirectory);
        assert.equal(plan.argv[plan.argv.indexOf("--logs-dir") + 1], layout.logsDirectory);
        assert.include(plan.argv, "--no-browser");
        assert.include(plan.argv, "--no-auto-bootstrap-project-from-cwd");
        assert.include(plan.argv, "--no-tailscale-serve");
        assert.notInclude(plan.argv, "serve");
        assert.isTrue(harness.path.isAbsolute(plan.nodeExecutablePath));
        assert.isTrue(harness.path.isAbsolute(plan.serverEntrypointPath));
        assert.isTrue(
          isPathWithin(harness.path, plan.runtimeVersionDirectory, plan.nodeExecutablePath),
        );
        assert.isTrue(
          isPathWithin(harness.path, plan.runtimeVersionDirectory, plan.serverEntrypointPath),
        );
        assert.include(
          plan.nodeExecutablePath,
          `${harness.path.sep}runtime${harness.path.sep}versions${harness.path.sep}`,
        );
        const serializedPlan = encodeLaunchPlan(plan);
        assert.notInclude(serializedPlan, fixture.sourceDirectory);
        assert.notInclude(serializedPlan, "T3 Code.app");
        assert.notInclude(serializedPlan, "worktree source");
        assert.deepEqual(plan.environment, {
          T3CODE_MODE: "web",
          T3CODE_HOST: "127.0.0.1",
          T3CODE_PORT: "4888",
          T3CODE_HOME: layout.profileDirectory,
          T3CODE_STATE_DIR: layout.stateDirectory,
          T3CODE_LOGS_DIR: layout.logsDirectory,
          T3CODE_NO_BROWSER: "true",
          T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
          T3CODE_TAILSCALE_SERVE: "false",
        });
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("creates missing state, logs, and run directories during preflight", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profileId = RuntimeProfileId.make("alpha");
        yield* installAndActivate(harness, profileId, 4773);
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);
        for (const directory of [
          layout.stateDirectory,
          layout.logsDirectory,
          layout.runDirectory,
        ]) {
          yield* harness.fs.remove(directory, { recursive: true });
        }

        yield* harness.planner.create(profileId);
        for (const directory of [
          layout.stateDirectory,
          layout.logsDirectory,
          layout.runDirectory,
        ]) {
          assert.isTrue(yield* harness.fs.exists(directory));
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed for a missing profile, current pointer, or installed artifact", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const missingProfile = RuntimeProfileId.make("nightly");
        const profileError = yield* harness.planner.create(missingProfile).pipe(Effect.flip);
        assert.equal(profileError._tag, "RuntimeProfileNotFoundError");

        const profileId = RuntimeProfileId.make("dev");
        yield* ensureProfile(harness, profileId, 3773);
        const pointerError = yield* harness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(pointerError._tag, "RuntimeCurrentPointerMissingError");

        const fixture = yield* installAndActivate(harness, profileId, 3773);
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);
        const installDirectory = runtimeArtifactVersionDirectory(
          harness.path,
          layout,
          fixture.manifest,
        );
        yield* harness.fs.remove(installDirectory, { recursive: true });
        assert.isTrue(Option.isSome(yield* harness.store.readCurrentRuntimePointer(profileId)));
        const strictPointerError = yield* harness.store
          .getCurrentRuntime(profileId)
          .pipe(Effect.flip);
        assert.equal(strictPointerError._tag, "RuntimeCurrentPointerCorruptError");
        const artifactError = yield* harness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(artifactError._tag, "RuntimeArtifactNotInstalledError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("does not echo corrupt current-pointer contents into typed errors", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profileId = RuntimeProfileId.make("custom:secure");
        yield* ensureProfile(harness, profileId, 5773);
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);
        const secret = "private-token-must-not-leak";
        yield* harness.fs.writeFileString(
          layout.currentRuntimePath,
          `{"schemaVersion":1,"token":"${secret}"}`,
        );

        const error = yield* harness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(error._tag, "RuntimeCurrentPointerCorruptError");
        assert.notInclude(encodeUnknownJson(error), secret);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects artifact and profile-directory symlink escapes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profileId = RuntimeProfileId.make("dev");
        const fixture = yield* installAndActivate(harness, profileId, 3773);
        const layout = makeRuntimeProfileLayout(harness.path, harness.profilesRoot, profileId);
        const installDirectory = runtimeArtifactVersionDirectory(
          harness.path,
          layout,
          fixture.manifest,
        );
        const externalRoot = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "t3 daemon external ",
        });
        const externalEntrypoint = harness.path.join(externalRoot, "bin.mjs");
        yield* harness.fs.writeFileString(
          externalEntrypoint,
          "console.log('independent runtime');\n",
        );
        const installedEntrypoint = harness.path.join(installDirectory, ENTRYPOINT);
        yield* harness.fs.remove(installedEntrypoint);
        yield* harness.fs.symlink(externalEntrypoint, installedEntrypoint);

        const artifactError = yield* harness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(artifactError._tag, "RuntimeArtifactPathEscapeError");

        yield* harness.fs.remove(installedEntrypoint);
        yield* harness.fs.writeFileString(
          installedEntrypoint,
          "console.log('independent runtime');\n",
        );
        yield* harness.fs.remove(layout.stateDirectory, { recursive: true });
        yield* harness.fs.symlink(externalRoot, layout.stateDirectory);
        const profileError = yield* harness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(profileError._tag, "RuntimeProfilePathEscapeError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("requires executable Node files on Darwin and Linux", () =>
    Effect.gen(function* () {
      for (const platform of ["darwin", "linux"] as const) {
        yield* Effect.scoped(
          Effect.gen(function* () {
            const harness = yield* makeHarness({ platform });
            const profileId = RuntimeProfileId.make("dev");
            yield* installAndActivate(harness, profileId, 3773, {
              platform,
              executable: false,
            });
            const error = yield* harness.planner.create(profileId).pipe(Effect.flip);
            assert.equal(error._tag, "RuntimeNodeNotExecutableError");
            assert.notProperty(error, "path");
          }),
        );
      }
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("rejects an active artifact for another platform or architecture", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const platformHarness = yield* makeHarness({ platform: "linux" });
        const profileId = RuntimeProfileId.make("dev");
        yield* installAndActivate(platformHarness, profileId, 3773);
        const platformError = yield* platformHarness.planner.create(profileId).pipe(Effect.flip);
        assert.equal(platformError._tag, "RuntimeArtifactPlatformMismatchError");

        const architectureHarness = yield* makeHarness({ architecture: "x64" });
        yield* installAndActivate(architectureHarness, profileId, 3773);
        const architectureError = yield* architectureHarness.planner
          .create(profileId)
          .pipe(Effect.flip);
        assert.equal(architectureError._tag, "RuntimeArtifactPlatformMismatchError");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps launch plans free of secret and token fields", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const profileId = RuntimeProfileId.make("nightly");
        yield* installAndActivate(harness, profileId, 6773);
        const plan = yield* harness.planner.create(profileId);
        const serialized = encodeLaunchPlan(plan);
        assert.notMatch(serialized, /(?:secret|token|credential)/iu);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
