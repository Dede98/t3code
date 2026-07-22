import {
  RUNTIME_DAEMON_LAUNCH_PLAN_SCHEMA_VERSION,
  RUNTIME_DAEMON_PREFLIGHT_SCHEMA_VERSION,
  RuntimeArtifactFileTypeInvalidError,
  type RuntimeArtifactArchitecture,
  RuntimeArtifactPathEscapeError,
  type RuntimeArtifactPlatform,
  RuntimeArtifactPlatformMismatchError,
  RuntimeCurrentPointerMissingError,
  type RuntimeDaemonLaunchPlan as RuntimeDaemonLaunchPlanValue,
  type RuntimeDaemonLaunchPlanError,
  RuntimeFilesystemError,
  RuntimeHostUnsupportedError,
  RuntimeNodeNotExecutableError,
  RuntimeProfileDirectoryInvalidError,
  type RuntimeProfileId,
  RuntimeProfileNotFoundError,
  RuntimeProfilePathEscapeError,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { isPathWithin, makeRuntimeProfileLayout } from "@t3tools/shared/runtimeProfile";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";

import { RuntimeArtifactInstaller } from "./RuntimeArtifactInstaller.ts";
import { RuntimeProfileStore } from "./RuntimeProfileStore.ts";

export interface RuntimeDaemonLaunchPlanOptions {
  readonly profilesRoot: string;
}

export class RuntimeDaemonLaunchPlanService extends Context.Service<
  RuntimeDaemonLaunchPlanService,
  {
    readonly create: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<RuntimeDaemonLaunchPlanValue, RuntimeDaemonLaunchPlanError>;
  }
>()("@t3tools/desktop/runtime/RuntimeDaemonLaunchPlan/RuntimeDaemonLaunchPlanService") {}

const mapFilesystemError =
  (operation: RuntimeFilesystemError["operation"]) =>
  <A, R>(
    effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  ): Effect.Effect<A, RuntimeFilesystemError, R> =>
    effect.pipe(
      Effect.mapError(() => new RuntimeFilesystemError({ code: "filesystem-error", operation })),
    );

const resolveHostPlatform = Effect.fn("RuntimeDaemonLaunchPlan.resolveHostPlatform")(function* (
  platform: NodeJS.Platform,
  architecture: NodeJS.Architecture,
): Effect.fn.Return<
  readonly [RuntimeArtifactPlatform, RuntimeArtifactArchitecture],
  RuntimeHostUnsupportedError
> {
  if (
    (platform !== "darwin" && platform !== "linux" && platform !== "win32") ||
    (architecture !== "arm64" && architecture !== "x64")
  ) {
    return yield* new RuntimeHostUnsupportedError({ code: "host-unsupported" });
  }
  return [platform, architecture] as const;
});

export const make = Effect.fn("RuntimeDaemonLaunchPlan.make")(function* (
  options: RuntimeDaemonLaunchPlanOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profileStore = yield* RuntimeProfileStore;
  const artifactInstaller = yield* RuntimeArtifactInstaller;
  const [platform, architecture] = yield* resolveHostPlatform(
    yield* HostProcessPlatform,
    yield* HostProcessArchitecture,
  );
  const profilesRoot = path.resolve(options.profilesRoot);

  const assertProfileDirectory = Effect.fn("RuntimeDaemonLaunchPlan.assertProfileDirectory")(
    function* (
      profileId: RuntimeProfileId,
      realProfileDirectory: string,
      candidate: string,
      directory: RuntimeProfileDirectoryInvalidError["directory"],
    ) {
      const realCandidate = yield* mapFilesystemError("realpath")(fs.realPath(candidate));
      if (!isPathWithin(path, realProfileDirectory, realCandidate)) {
        return yield* new RuntimeProfilePathEscapeError({
          code: "profile-path-escape",
          profileId,
        });
      }
      const info = yield* mapFilesystemError("stat")(fs.stat(realCandidate));
      if (info.type !== "Directory") {
        return yield* new RuntimeProfileDirectoryInvalidError({
          code: "profile-directory-invalid",
          profileId,
          directory,
        });
      }
    },
  );

  const resolveArtifactFile = Effect.fn("RuntimeDaemonLaunchPlan.resolveArtifactFile")(function* (
    profileId: RuntimeProfileId,
    realVersionDirectory: string,
    candidate: string,
    role: RuntimeArtifactFileTypeInvalidError["role"],
  ) {
    const realCandidate = yield* mapFilesystemError("realpath")(fs.realPath(candidate));
    if (!isPathWithin(path, realVersionDirectory, realCandidate)) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }
    const info = yield* mapFilesystemError("stat")(fs.stat(realCandidate));
    if (info.type !== "File") {
      return yield* new RuntimeArtifactFileTypeInvalidError({
        code: "artifact-file-type-invalid",
        profileId,
        role,
      });
    }
    return { path: realCandidate, info };
  });

  const create = Effect.fn("RuntimeDaemonLaunchPlan.create")(function* (
    profileId: RuntimeProfileId,
  ): Effect.fn.Return<RuntimeDaemonLaunchPlanValue, RuntimeDaemonLaunchPlanError> {
    const profile = yield* profileStore.getProfile(profileId);
    if (Option.isNone(profile)) {
      return yield* new RuntimeProfileNotFoundError({
        code: "profile-not-found",
        profileId,
      });
    }

    yield* profileStore.ensureProfile(profile.value);
    const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
    const realProfileDirectory = yield* mapFilesystemError("realpath")(
      fs.realPath(layout.profileDirectory),
    );
    yield* assertProfileDirectory(profileId, realProfileDirectory, layout.stateDirectory, "state");
    yield* assertProfileDirectory(profileId, realProfileDirectory, layout.logsDirectory, "logs");
    yield* assertProfileDirectory(profileId, realProfileDirectory, layout.runDirectory, "run");

    const current = yield* profileStore.readCurrentRuntimePointer(profileId);
    if (Option.isNone(current)) {
      return yield* new RuntimeCurrentPointerMissingError({
        code: "current-pointer-missing",
        profileId,
      });
    }

    const installed = yield* artifactInstaller.getInstalledArtifact({
      profileId,
      runtimeVersion: current.value.runtimeVersion,
      buildHash: current.value.buildHash,
    });
    if (
      installed.manifest.platform !== platform ||
      installed.manifest.architecture !== architecture
    ) {
      return yield* new RuntimeArtifactPlatformMismatchError({
        code: "artifact-platform-mismatch",
        expectedPlatform: platform,
        expectedArchitecture: architecture,
        actualPlatform: installed.manifest.platform,
        actualArchitecture: installed.manifest.architecture,
      });
    }

    const realVersionsDirectory = yield* mapFilesystemError("realpath")(
      fs.realPath(layout.versionsDirectory),
    );
    const realVersionDirectory = yield* mapFilesystemError("realpath")(
      fs.realPath(installed.installDirectory),
    );
    if (!isPathWithin(path, realVersionsDirectory, realVersionDirectory)) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }

    const nodeExecutable = yield* resolveArtifactFile(
      profileId,
      realVersionDirectory,
      installed.nodeExecutablePath,
      "node-executable",
    );
    const serverEntrypoint = yield* resolveArtifactFile(
      profileId,
      realVersionDirectory,
      installed.entrypointPath,
      "server-entrypoint",
    );
    if (
      (platform === "darwin" || platform === "linux") &&
      (nodeExecutable.info.mode & 0o111) === 0
    ) {
      return yield* new RuntimeNodeNotExecutableError({
        code: "node-not-executable",
        profileId,
      });
    }

    const port = profile.value.port;
    const origin = `http://127.0.0.1:${port}`;
    const argv = [
      serverEntrypoint.path,
      "start",
      "--mode",
      "web",
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--base-dir",
      layout.profileDirectory,
      "--state-dir",
      layout.stateDirectory,
      "--logs-dir",
      layout.logsDirectory,
      "--no-browser",
      "--no-auto-bootstrap-project-from-cwd",
      "--no-tailscale-serve",
    ];
    const preflight = {
      schemaVersion: RUNTIME_DAEMON_PREFLIGHT_SCHEMA_VERSION,
      profileId,
      platform,
      architecture,
      ok: true,
      checks: [
        { check: "profile-config", status: "ready" },
        { check: "runtime-current", status: "ready" },
        { check: "runtime-artifact", status: "ready" },
        { check: "runtime-platform", status: "ready" },
        { check: "runtime-architecture", status: "ready" },
        { check: "node-executable", status: "ready" },
        { check: "server-entrypoint", status: "ready" },
        { check: "state-directory", status: "ready" },
        { check: "logs-directory", status: "ready" },
        { check: "run-directory", status: "ready" },
      ],
    } as const;

    return {
      schemaVersion: RUNTIME_DAEMON_LAUNCH_PLAN_SCHEMA_VERSION,
      profileId,
      runtimeVersion: installed.manifest.runtimeVersion,
      buildHash: installed.manifest.buildHash,
      versionDirectory: installed.versionDirectory,
      nodeExecutablePath: nodeExecutable.path,
      serverEntrypointPath: serverEntrypoint.path,
      argv,
      cwd: layout.profileDirectory,
      environment: {
        T3CODE_MODE: "web",
        T3CODE_HOST: "127.0.0.1",
        T3CODE_PORT: String(port),
        T3CODE_HOME: layout.profileDirectory,
        T3CODE_STATE_DIR: layout.stateDirectory,
        T3CODE_LOGS_DIR: layout.logsDirectory,
        T3CODE_NO_BROWSER: "true",
        T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
        T3CODE_TAILSCALE_SERVE: "false",
      },
      port,
      origin,
      profileDirectory: layout.profileDirectory,
      runtimeVersionDirectory: realVersionDirectory,
      stateDirectory: layout.stateDirectory,
      logsDirectory: layout.logsDirectory,
      runDirectory: layout.runDirectory,
      daemonLockPath: layout.daemonLockPath,
      discoveryPath: layout.discoveryPath,
      recoveryPath: layout.recoveryPath,
      preflight,
    } satisfies RuntimeDaemonLaunchPlanValue;
  });

  return RuntimeDaemonLaunchPlanService.of({ create });
});

export const layer = (options: RuntimeDaemonLaunchPlanOptions) =>
  Layer.effect(RuntimeDaemonLaunchPlanService, make(options));
