import {
  RUNTIME_DAEMON_LAUNCHER_CONFIG_SCHEMA_VERSION,
  RUNTIME_DAEMON_LAUNCHER_INSTALLATION_SCHEMA_VERSION,
  RUNTIME_DAEMON_RECOVERY_CONFIG_SCHEMA_VERSION,
  RuntimeDaemonLauncherConfig,
  RuntimeDaemonLauncherInstallation,
  RuntimeDaemonLifecycleError,
  RuntimeDaemonLock,
  type RuntimeDaemonLaunchPlan,
  type RuntimeDaemonLaunchPlanError,
  type RuntimeProfileId,
  type RuntimeSha256Digest,
} from "@t3tools/contracts/runtimeProfile";
import {
  isPathWithin,
  makeRuntimeProfileLayout,
  resolveRuntimeArtifactPath,
} from "@t3tools/shared/runtimeProfile";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";

import {
  RuntimeDaemonLaunchPlanService,
  type RuntimeDaemonLaunchPlanOptions,
} from "./RuntimeDaemonLaunchPlan.ts";
import { RuntimeArtifactInstaller } from "./RuntimeArtifactInstaller.ts";
import { STANDALONE_RUNTIME_LAUNCHER_SOURCE } from "./StandaloneRuntimeLauncherSource.ts";

const DEFAULT_HEALTHCHECK_TIMEOUT_MS = 30_000;
const DEFAULT_HEALTHCHECK_POLL_INTERVAL_MS = 100;
const DEFAULT_HEALTHCHECK_REQUEST_TIMEOUT_MS = 1_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5_000;
const DEFAULT_RECOVERY_MAX_RESTARTS = 5;
const DEFAULT_RECOVERY_SLIDING_WINDOW_MS = 5 * 60_000;
const DEFAULT_RECOVERY_INITIAL_BACKOFF_MS = 1_000;
const DEFAULT_RECOVERY_MAX_BACKOFF_MS = 30_000;
const DEFAULT_RECOVERY_HEALTHCHECK_INTERVAL_MS = 30_000;
const DEFAULT_RECOVERY_CONSECUTIVE_HEALTH_FAILURES = 3;
const DEFAULT_RECOVERY_HEALTHY_RESET_AFTER_MS = 5 * 60_000;

export type RuntimeDaemonPidState = "alive" | "dead" | "unknown";

export function probeRuntimeDaemonPid(
  pid: number,
  signal: (pid: number, signal: 0) => unknown = process.kill,
): RuntimeDaemonPidState {
  try {
    signal(pid, 0);
    return "alive";
  } catch (cause) {
    if (cause !== null && typeof cause === "object" && "code" in cause) {
      if (cause.code === "ESRCH") return "dead";
      if (cause.code === "EPERM") return "alive";
    }
    return "unknown";
  }
}

export const RuntimeDaemonProcessProbe = Context.Reference<(pid: number) => RuntimeDaemonPidState>(
  "@t3tools/desktop/runtime/StandaloneRuntimeLauncher/RuntimeDaemonProcessProbe",
  {
    defaultValue: () => probeRuntimeDaemonPid,
  },
);

export interface StandaloneRuntimeLauncherOptions extends RuntimeDaemonLaunchPlanOptions {
  readonly healthcheckTimeoutMs?: number;
  readonly healthcheckPollIntervalMs?: number;
  readonly healthcheckRequestTimeoutMs?: number;
  readonly shutdownTimeoutMs?: number;
  readonly recovery?: {
    readonly maxRestarts?: number;
    readonly slidingWindowMs?: number;
    readonly initialBackoffMs?: number;
    readonly maxBackoffMs?: number;
    readonly healthcheckIntervalMs?: number;
    readonly consecutiveHealthFailuresBeforeRestart?: number;
    readonly healthyResetAfterMs?: number;
  };
}

export interface StandaloneRuntimeLauncherPaths {
  readonly launcherDirectory: string;
  readonly installationsDirectory: string;
  readonly installationDirectory: string;
  readonly installationPointerPath: string;
  readonly nodeRuntimeDirectory: string;
  readonly nodePath: string;
  readonly scriptPath: string;
  readonly configPath: string;
}

export interface MaterializedRuntimeLauncher {
  readonly status: "installed" | "already-installed";
  readonly installation: RuntimeDaemonLauncherInstallation;
  readonly config: RuntimeDaemonLauncherConfig;
  readonly paths: StandaloneRuntimeLauncherPaths;
}

export type StandaloneRuntimeLauncherError =
  | RuntimeDaemonLaunchPlanError
  | RuntimeDaemonLifecycleError;

export class StandaloneRuntimeLauncher extends Context.Service<
  StandaloneRuntimeLauncher,
  {
    readonly materialize: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<MaterializedRuntimeLauncher, StandaloneRuntimeLauncherError>;
    readonly materializePlan: (
      plan: RuntimeDaemonLaunchPlan,
    ) => Effect.Effect<MaterializedRuntimeLauncher, StandaloneRuntimeLauncherError>;
    readonly inspect: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<Option.Option<MaterializedRuntimeLauncher>, RuntimeDaemonLifecycleError>;
  }
>()("@t3tools/desktop/runtime/StandaloneRuntimeLauncher") {}

const decodeConfig = Schema.decodeUnknownEffect(RuntimeDaemonLauncherConfig);
const decodeInstallation = Schema.decodeUnknownEffect(RuntimeDaemonLauncherInstallation);
const decodeConfigJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RuntimeDaemonLauncherConfig),
);
const decodeInstallationJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RuntimeDaemonLauncherInstallation),
);
const decodeLockJson = Schema.decodeUnknownEffect(Schema.fromJsonString(RuntimeDaemonLock));

const lifecycleError = (profileId: RuntimeProfileId, code: RuntimeDaemonLifecycleError["code"]) =>
  new RuntimeDaemonLifecycleError({
    code,
    profileId,
    operation: "materialize-launcher",
  });

const mapFilesystemError =
  (profileId: RuntimeProfileId) =>
  <A, R>(
    effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  ): Effect.Effect<A, RuntimeDaemonLifecycleError, R> =>
    effect.pipe(Effect.mapError(() => lifecycleError(profileId, "filesystem-error")));

const isNotFound = (error: PlatformError.PlatformError): boolean =>
  error.reason._tag === "NotFound";

const encodeJson = (value: unknown): string => `${JSON.stringify(value)}\n`;

export function makeStandaloneRuntimeLauncherPaths(
  path: Path.Path,
  launcherDirectory: string,
  installationId: RuntimeSha256Digest,
  nodeRelativePath: string,
): StandaloneRuntimeLauncherPaths {
  const installationsDirectory = path.join(launcherDirectory, "installations");
  const installationDirectory = path.join(installationsDirectory, installationId);
  return {
    launcherDirectory,
    installationsDirectory,
    installationDirectory,
    installationPointerPath: path.join(launcherDirectory, "installation.json"),
    nodeRuntimeDirectory: path.join(installationDirectory, "node-runtime"),
    nodePath: path.join(installationDirectory, ...nodeRelativePath.split("/")),
    scriptPath: path.join(installationDirectory, "launcher.mjs"),
    configPath: path.join(installationDirectory, "launcher.json"),
  };
}

export const make = Effect.fn("StandaloneRuntimeLauncher.make")(function* (
  options: StandaloneRuntimeLauncherOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const artifactInstaller = yield* RuntimeArtifactInstaller;
  const planner = yield* RuntimeDaemonLaunchPlanService;
  const probePid = yield* RuntimeDaemonProcessProbe;
  const profilesRoot = path.resolve(options.profilesRoot);
  const digestBytes = Effect.fn("StandaloneRuntimeLauncher.digestBytes")(function* (
    bytes: Uint8Array,
  ) {
    return yield* crypto.digest("SHA-256", bytes).pipe(
      Effect.map((digest) => Encoding.encodeHex(digest) as RuntimeSha256Digest),
      Effect.orDie,
    );
  });
  const digestString = (value: string) => digestBytes(new TextEncoder().encode(value));
  const scriptBytes = new TextEncoder().encode(STANDALONE_RUNTIME_LAUNCHER_SOURCE);
  const launcherScriptSha256 = yield* digestBytes(scriptBytes);

  const assertRegularFile = Effect.fn("StandaloneRuntimeLauncher.assertRegularFile")(function* (
    profileId: RuntimeProfileId,
    candidate: string,
    root: string,
    executable: boolean,
  ) {
    const info = yield* mapFilesystemError(profileId)(fs.stat(candidate));
    if (info.type !== "File" || (executable && (info.mode & 0o111) === 0)) {
      return yield* lifecycleError(profileId, "launcher-invalid");
    }
    const realRoot = yield* mapFilesystemError(profileId)(fs.realPath(root));
    const realCandidate = yield* mapFilesystemError(profileId)(fs.realPath(candidate));
    const relativeCandidate = path.relative(path.resolve(root), path.resolve(candidate));
    if (
      !isPathWithin(path, realRoot, realCandidate) ||
      path.normalize(path.resolve(realRoot, relativeCandidate)) !== path.normalize(realCandidate)
    ) {
      return yield* lifecycleError(profileId, "unsafe-path");
    }
    return info;
  });

  const assertDirectoryWithinProfile = Effect.fn(
    "StandaloneRuntimeLauncher.assertDirectoryWithinProfile",
  )(function* (profileId: RuntimeProfileId, profileDirectory: string, candidate: string) {
    const info = yield* mapFilesystemError(profileId)(fs.stat(candidate));
    if (info.type !== "Directory") {
      return yield* lifecycleError(profileId, "unsafe-path");
    }
    const realProfile = yield* mapFilesystemError(profileId)(fs.realPath(profileDirectory));
    const realCandidate = yield* mapFilesystemError(profileId)(fs.realPath(candidate));
    const relativeCandidate = path.relative(
      path.resolve(profileDirectory),
      path.resolve(candidate),
    );
    if (
      !isPathWithin(path, realProfile, realCandidate) ||
      path.normalize(path.resolve(realProfile, relativeCandidate)) !== path.normalize(realCandidate)
    ) {
      return yield* lifecycleError(profileId, "unsafe-path");
    }
  });

  const readOptionalString = Effect.fn("StandaloneRuntimeLauncher.readOptionalString")(function* (
    profileId: RuntimeProfileId,
    filePath: string,
  ) {
    return yield* fs.readFileString(filePath).pipe(
      Effect.map(Option.some),
      Effect.catch((error) =>
        isNotFound(error)
          ? Effect.succeed(Option.none<string>())
          : Effect.fail(lifecycleError(profileId, "filesystem-error")),
      ),
    );
  });

  const writeAtomically = Effect.fn("StandaloneRuntimeLauncher.writeAtomically")(function* (
    profileId: RuntimeProfileId,
    filePath: string,
    contents: string,
  ) {
    const directory = path.dirname(filePath);
    yield* Effect.scoped(
      Effect.gen(function* () {
        const temporaryDirectory = yield* mapFilesystemError(profileId)(
          fs.makeTempDirectoryScoped({ directory, prefix: `.${path.basename(filePath)}.` }),
        );
        const temporaryPath = path.join(temporaryDirectory, "contents.tmp");
        yield* mapFilesystemError(profileId)(fs.writeFileString(temporaryPath, contents));
        yield* mapFilesystemError(profileId)(fs.chmod(temporaryPath, 0o600));
        yield* mapFilesystemError(profileId)(fs.rename(temporaryPath, filePath));
      }),
    );
  });

  const readInstallationPointer = Effect.fn("StandaloneRuntimeLauncher.readInstallationPointer")(
    function* (profileId: RuntimeProfileId, filePath: string) {
      const raw = yield* readOptionalString(profileId, filePath);
      if (Option.isNone(raw)) return Option.none<RuntimeDaemonLauncherInstallation>();
      yield* assertRegularFile(profileId, filePath, path.dirname(filePath), false);
      return yield* decodeInstallationJson(raw.value).pipe(
        Effect.map(Option.some),
        Effect.mapError(() => lifecycleError(profileId, "launcher-invalid")),
      );
    },
  );

  const assertNoRunningUpdate = Effect.fn("StandaloneRuntimeLauncher.assertNoRunningUpdate")(
    function* (plan: RuntimeDaemonLaunchPlan) {
      const raw = yield* readOptionalString(plan.profileId, plan.daemonLockPath);
      if (Option.isNone(raw)) return;
      yield* assertRegularFile(plan.profileId, plan.daemonLockPath, plan.runDirectory, false);
      const lock = yield* decodeLockJson(raw.value).pipe(
        Effect.mapError(() => lifecycleError(plan.profileId, "state-corrupt")),
      );
      const state = probePid(lock.launcherPid);
      if (state !== "dead") {
        return yield* lifecycleError(plan.profileId, "launcher-already-running");
      }
    },
  );

  const verifySourceNode = Effect.fn("StandaloneRuntimeLauncher.verifySourceNode")(function* (
    plan: RuntimeDaemonLaunchPlan,
  ) {
    const installed = yield* artifactInstaller.getInstalledArtifact({
      profileId: plan.profileId,
      runtimeVersion: plan.runtimeVersion,
      buildHash: plan.buildHash,
    });
    const nodeManifestEntry = installed.manifest.files.find(
      (file) => file.path === installed.manifest.nodeExecutable,
    );
    if (nodeManifestEntry === undefined) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    const realNode = yield* mapFilesystemError(plan.profileId)(
      fs.realPath(installed.nodeExecutablePath),
    );
    if (path.normalize(realNode) !== path.normalize(plan.nodeExecutablePath)) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    const info = yield* assertRegularFile(
      plan.profileId,
      installed.nodeExecutablePath,
      installed.installDirectory,
      true,
    );
    if (info.size !== BigInt(nodeManifestEntry.byteSize)) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    const bytes = yield* mapFilesystemError(plan.profileId)(
      fs.readFile(installed.nodeExecutablePath),
    );
    const digest = yield* digestBytes(bytes);
    if (digest !== nodeManifestEntry.sha256) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    const nodeSegments = installed.manifest.nodeExecutable.split("/");
    const nodeRootPrefix = nodeSegments.length > 1 ? `${nodeSegments[0]}/` : undefined;
    const runtimeManifestEntries = installed.manifest.files.filter((file) =>
      nodeRootPrefix === undefined
        ? file.path === installed.manifest.nodeExecutable
        : file.path.startsWith(nodeRootPrefix),
    );
    const runtimeFiles = runtimeManifestEntries.map((file) => ({
      sourcePath: resolveRuntimeArtifactPath(path, installed.installDirectory, file.path),
      targetRelativePath: `node-runtime/${
        nodeRootPrefix === undefined ? file.path : file.path.slice(nodeRootPrefix.length)
      }`,
      manifestEntry: file,
    }));
    if (runtimeFiles.some((file) => file.sourcePath === undefined)) {
      return yield* lifecycleError(plan.profileId, "unsafe-path");
    }
    const nodeRelativePath = `node-runtime/${
      nodeRootPrefix === undefined
        ? installed.manifest.nodeExecutable
        : installed.manifest.nodeExecutable.slice(nodeRootPrefix.length)
    }`;
    return { installed, manifestEntry: nodeManifestEntry, runtimeFiles, nodeRelativePath };
  });

  const verifyMaterialized = Effect.fn("StandaloneRuntimeLauncher.verifyMaterialized")(function* (
    plan: RuntimeDaemonLaunchPlan,
    installation: RuntimeDaemonLauncherInstallation,
    config: RuntimeDaemonLauncherConfig,
    paths: StandaloneRuntimeLauncherPaths,
  ) {
    yield* assertDirectoryWithinProfile(
      plan.profileId,
      plan.profileDirectory,
      paths.installationDirectory,
    );
    const nodeInfo = yield* assertRegularFile(
      plan.profileId,
      paths.nodePath,
      paths.launcherDirectory,
      true,
    );
    const nodeRuntimePaths = installation.nodeRuntimeFiles.map((file) => file.path);
    const nodeEntry = installation.nodeRuntimeFiles.find(
      (file) => file.path === installation.nodeRelativePath,
    );
    if (
      !installation.nodeRelativePath.startsWith("node-runtime/") ||
      installation.nodeRuntimeFiles.some((file) => !file.path.startsWith("node-runtime/")) ||
      new Set(nodeRuntimePaths).size !== nodeRuntimePaths.length ||
      nodeEntry === undefined ||
      nodeEntry.byteSize !== installation.nodeByteSize ||
      nodeEntry.sha256 !== installation.nodeSha256
    ) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    yield* assertRegularFile(plan.profileId, paths.scriptPath, paths.launcherDirectory, false);
    yield* assertRegularFile(plan.profileId, paths.configPath, paths.launcherDirectory, false);
    if (nodeInfo.size !== BigInt(installation.nodeByteSize)) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    for (const file of installation.nodeRuntimeFiles) {
      const filePath = path.join(paths.installationDirectory, ...file.path.split("/"));
      const info = yield* assertRegularFile(
        plan.profileId,
        filePath,
        paths.installationDirectory,
        file.path === installation.nodeRelativePath,
      );
      if (info.size !== BigInt(file.byteSize)) {
        return yield* lifecycleError(plan.profileId, "launcher-invalid");
      }
      const bytes = yield* mapFilesystemError(plan.profileId)(fs.readFile(filePath));
      if ((yield* digestBytes(bytes)) !== file.sha256) {
        return yield* lifecycleError(plan.profileId, "launcher-invalid");
      }
    }
    const [nodeBytes, scriptContents, configContents] = yield* Effect.all(
      [
        mapFilesystemError(plan.profileId)(fs.readFile(paths.nodePath)),
        mapFilesystemError(plan.profileId)(fs.readFileString(paths.scriptPath)),
        mapFilesystemError(plan.profileId)(fs.readFileString(paths.configPath)),
      ],
      { concurrency: "unbounded" },
    );
    if (
      (yield* digestBytes(nodeBytes)) !== installation.nodeSha256 ||
      (yield* digestString(scriptContents)) !== installation.launcherScriptSha256 ||
      scriptContents !== STANDALONE_RUNTIME_LAUNCHER_SOURCE ||
      configContents !== encodeJson(config)
    ) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    yield* decodeConfigJson(configContents).pipe(
      Effect.mapError(() => lifecycleError(plan.profileId, "launcher-invalid")),
    );
  });

  const configFor = Effect.fn("StandaloneRuntimeLauncher.configFor")(function* (
    plan: RuntimeDaemonLaunchPlan,
  ) {
    const config = yield* decodeConfig({
      schemaVersion: RUNTIME_DAEMON_LAUNCHER_CONFIG_SCHEMA_VERSION,
      profileId: plan.profileId,
      runtimeVersion: plan.runtimeVersion,
      buildHash: plan.buildHash,
      versionDirectory: plan.versionDirectory,
      launchPlan: plan,
      healthcheckTimeoutMs: options.healthcheckTimeoutMs ?? DEFAULT_HEALTHCHECK_TIMEOUT_MS,
      healthcheckPollIntervalMs:
        options.healthcheckPollIntervalMs ?? DEFAULT_HEALTHCHECK_POLL_INTERVAL_MS,
      healthcheckRequestTimeoutMs:
        options.healthcheckRequestTimeoutMs ?? DEFAULT_HEALTHCHECK_REQUEST_TIMEOUT_MS,
      shutdownTimeoutMs: options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS,
      recovery: {
        schemaVersion: RUNTIME_DAEMON_RECOVERY_CONFIG_SCHEMA_VERSION,
        enabled: true,
        maxRestarts: options.recovery?.maxRestarts ?? DEFAULT_RECOVERY_MAX_RESTARTS,
        slidingWindowMs: options.recovery?.slidingWindowMs ?? DEFAULT_RECOVERY_SLIDING_WINDOW_MS,
        initialBackoffMs: options.recovery?.initialBackoffMs ?? DEFAULT_RECOVERY_INITIAL_BACKOFF_MS,
        maxBackoffMs: options.recovery?.maxBackoffMs ?? DEFAULT_RECOVERY_MAX_BACKOFF_MS,
        healthcheckIntervalMs:
          options.recovery?.healthcheckIntervalMs ?? DEFAULT_RECOVERY_HEALTHCHECK_INTERVAL_MS,
        consecutiveHealthFailuresBeforeRestart:
          options.recovery?.consecutiveHealthFailuresBeforeRestart ??
          DEFAULT_RECOVERY_CONSECUTIVE_HEALTH_FAILURES,
        healthyResetAfterMs:
          options.recovery?.healthyResetAfterMs ?? DEFAULT_RECOVERY_HEALTHY_RESET_AFTER_MS,
      },
    }).pipe(Effect.mapError(() => lifecycleError(plan.profileId, "launcher-invalid")));
    if (config.recovery.maxBackoffMs < config.recovery.initialBackoffMs) {
      return yield* lifecycleError(plan.profileId, "launcher-invalid");
    }
    return config;
  });

  const materializePlan = Effect.fn("StandaloneRuntimeLauncher.materializePlan")(function* (
    plan: RuntimeDaemonLaunchPlan,
  ): Effect.fn.Return<MaterializedRuntimeLauncher, StandaloneRuntimeLauncherError> {
    const layout = makeRuntimeProfileLayout(path, profilesRoot, plan.profileId);
    if (
      path.normalize(path.resolve(plan.profileDirectory)) !==
        path.normalize(path.resolve(layout.profileDirectory)) ||
      !isPathWithin(path, layout.profileDirectory, layout.launcherDirectory)
    ) {
      return yield* lifecycleError(plan.profileId, "unsafe-path");
    }
    yield* assertDirectoryWithinProfile(
      plan.profileId,
      layout.profileDirectory,
      layout.launcherDirectory,
    );
    const installationsDirectory = path.join(layout.launcherDirectory, "installations");
    yield* mapFilesystemError(plan.profileId)(
      fs.makeDirectory(installationsDirectory, { recursive: true, mode: 0o700 }),
    );
    yield* assertDirectoryWithinProfile(
      plan.profileId,
      layout.profileDirectory,
      installationsDirectory,
    );

    const sourceNode = yield* verifySourceNode(plan);
    const config = yield* configFor(plan);
    const configContents = encodeJson(config);
    const installationId = yield* digestString(
      `${encodeJson(
        sourceNode.runtimeFiles.map((file) => ({
          path: file.targetRelativePath,
          byteSize: file.manifestEntry.byteSize,
          sha256: file.manifestEntry.sha256,
        })),
      )}:${launcherScriptSha256}:${yield* digestString(configContents)}`,
    );
    const paths = makeStandaloneRuntimeLauncherPaths(
      path,
      layout.launcherDirectory,
      installationId,
      sourceNode.nodeRelativePath,
    );
    const existingPointer = yield* readInstallationPointer(
      plan.profileId,
      paths.installationPointerPath,
    );
    if (Option.isNone(existingPointer) || existingPointer.value.installationId !== installationId) {
      yield* assertNoRunningUpdate(plan);
    }

    const targetExists = yield* mapFilesystemError(plan.profileId)(
      fs.exists(paths.installationDirectory),
    );
    const installedAt =
      Option.isSome(existingPointer) && existingPointer.value.installationId === installationId
        ? existingPointer.value.installedAt
        : DateTime.formatIso(yield* DateTime.now);
    const installation = yield* decodeInstallation({
      schemaVersion: RUNTIME_DAEMON_LAUNCHER_INSTALLATION_SCHEMA_VERSION,
      installationId,
      profileId: plan.profileId,
      runtimeVersion: plan.runtimeVersion,
      buildHash: plan.buildHash,
      versionDirectory: plan.versionDirectory,
      nodeRelativePath: sourceNode.nodeRelativePath,
      nodeRuntimeFiles: sourceNode.runtimeFiles.map((file) => ({
        path: file.targetRelativePath,
        byteSize: file.manifestEntry.byteSize,
        sha256: file.manifestEntry.sha256,
      })),
      nodeByteSize: sourceNode.manifestEntry.byteSize,
      nodeSha256: sourceNode.manifestEntry.sha256,
      launcherScriptSha256,
      installedAt,
    }).pipe(Effect.mapError(() => lifecycleError(plan.profileId, "launcher-invalid")));

    if (!targetExists) {
      yield* Effect.scoped(
        Effect.gen(function* () {
          const temporaryDirectory = yield* mapFilesystemError(plan.profileId)(
            fs.makeTempDirectoryScoped({
              directory: layout.launcherDirectory,
              prefix: ".materialize-",
            }),
          );
          const stagedDirectory = path.join(temporaryDirectory, "installation");
          yield* mapFilesystemError(plan.profileId)(
            fs.makeDirectory(stagedDirectory, { mode: 0o700 }),
          );
          const stagedPaths = {
            ...paths,
            installationDirectory: stagedDirectory,
            nodeRuntimeDirectory: path.join(stagedDirectory, "node-runtime"),
            nodePath: path.join(stagedDirectory, ...installation.nodeRelativePath.split("/")),
            scriptPath: path.join(stagedDirectory, "launcher.mjs"),
            configPath: path.join(stagedDirectory, "launcher.json"),
          };
          for (const file of sourceNode.runtimeFiles) {
            if (file.sourcePath === undefined) {
              return yield* lifecycleError(plan.profileId, "unsafe-path");
            }
            const targetPath = path.join(stagedDirectory, ...file.targetRelativePath.split("/"));
            yield* mapFilesystemError(plan.profileId)(
              fs.makeDirectory(path.dirname(targetPath), { recursive: true, mode: 0o700 }),
            );
            yield* mapFilesystemError(plan.profileId)(fs.copyFile(file.sourcePath, targetPath));
            yield* mapFilesystemError(plan.profileId)(
              fs.chmod(
                targetPath,
                file.manifestEntry.path === sourceNode.installed.manifest.nodeExecutable
                  ? 0o700
                  : 0o600,
              ),
            );
          }
          yield* mapFilesystemError(plan.profileId)(
            fs.writeFileString(stagedPaths.scriptPath, STANDALONE_RUNTIME_LAUNCHER_SOURCE),
          );
          yield* mapFilesystemError(plan.profileId)(fs.chmod(stagedPaths.scriptPath, 0o600));
          yield* mapFilesystemError(plan.profileId)(
            fs.writeFileString(stagedPaths.configPath, configContents),
          );
          yield* mapFilesystemError(plan.profileId)(fs.chmod(stagedPaths.configPath, 0o600));
          yield* verifyMaterialized(plan, installation, config, stagedPaths);
          yield* verifySourceNode(plan);
          yield* mapFilesystemError(plan.profileId)(
            fs.rename(stagedDirectory, paths.installationDirectory),
          );
        }),
      );
    }

    yield* verifySourceNode(plan);
    yield* verifyMaterialized(plan, installation, config, paths);
    const pointerCurrent =
      Option.isSome(existingPointer) &&
      existingPointer.value.installationId === installation.installationId;
    if (!pointerCurrent) {
      yield* assertNoRunningUpdate(plan);
      yield* writeAtomically(
        plan.profileId,
        paths.installationPointerPath,
        encodeJson(installation),
      );
    }
    return {
      status: pointerCurrent && targetExists ? "already-installed" : "installed",
      installation,
      config,
      paths,
    };
  });

  const inspect = Effect.fn("StandaloneRuntimeLauncher.inspect")(function* (
    profileId: RuntimeProfileId,
  ) {
    const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
    const pointerPath = path.join(layout.launcherDirectory, "installation.json");
    const pointer = yield* readInstallationPointer(profileId, pointerPath);
    if (Option.isNone(pointer)) return Option.none<MaterializedRuntimeLauncher>();
    const paths = makeStandaloneRuntimeLauncherPaths(
      path,
      layout.launcherDirectory,
      pointer.value.installationId,
      pointer.value.nodeRelativePath,
    );
    const rawConfig = yield* readOptionalString(profileId, paths.configPath);
    if (Option.isNone(rawConfig)) {
      return yield* lifecycleError(profileId, "launcher-invalid");
    }
    const config = yield* decodeConfigJson(rawConfig.value).pipe(
      Effect.mapError(() => lifecycleError(profileId, "launcher-invalid")),
    );
    yield* verifyMaterialized(config.launchPlan, pointer.value, config, paths);
    return Option.some({
      status: "already-installed" as const,
      installation: pointer.value,
      config,
      paths,
    });
  });

  const materialize = (profileId: RuntimeProfileId) =>
    planner.create(profileId).pipe(Effect.flatMap(materializePlan));

  return StandaloneRuntimeLauncher.of({ materialize, materializePlan, inspect });
});

export const layer = (options: StandaloneRuntimeLauncherOptions) =>
  Layer.effect(StandaloneRuntimeLauncher, make(options));
