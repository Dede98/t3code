import {
  RUNTIME_CURRENT_POINTER_SCHEMA_VERSION,
  RuntimeArtifactDigestMismatchError,
  type RuntimeArtifactError,
  RuntimeArtifactFileMissingError,
  RuntimeArtifactInstallConflictError,
  RuntimeArtifactManifest,
  RuntimeArtifactManifestInvalidError,
  RuntimeArtifactNotInstalledError,
  RuntimeArtifactPathEscapeError,
  RuntimeArtifactPlatformMismatchError,
  RuntimeArtifactSizeMismatchError,
  type RuntimeArtifactArchitecture,
  type RuntimeArtifactPlatform,
  type RuntimeBuildHash,
  RuntimeCurrentPointer,
  RuntimeFilesystemError,
  type RuntimeInstallResult,
  type RuntimeProfileId,
  RuntimeProfileNotFoundError,
  type RuntimeSha256Digest,
  type RuntimeVersion,
  type RuntimeVersionDirectory,
} from "@t3tools/contracts/runtimeProfile";
import {
  isPathWithin,
  isSafeRuntimeArtifactRelativePath,
  makeRuntimeProfileLayout,
  normalizeRuntimeArtifactRelativePath,
  resolveRuntimeArtifactPath,
  runtimeArtifactVersionDirectory,
  runtimeVersionDirectoryName,
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
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

import { RuntimeProfileStore } from "./RuntimeProfileStore.ts";

export interface ValidateRuntimeArtifactInput {
  readonly sourceDirectory: string;
  readonly targetPlatform: RuntimeArtifactPlatform;
  readonly targetArchitecture: RuntimeArtifactArchitecture;
}

export interface RuntimeArtifactIdentity {
  readonly profileId: RuntimeProfileId;
  readonly runtimeVersion: RuntimeVersion;
  readonly buildHash: RuntimeBuildHash;
}

export interface InstallRuntimeArtifactInput extends ValidateRuntimeArtifactInput {
  readonly profileId: RuntimeProfileId;
}

export interface InstalledRuntimeArtifact {
  readonly profileId: RuntimeProfileId;
  readonly manifest: RuntimeArtifactManifest;
  readonly versionDirectory: RuntimeVersionDirectory;
  readonly installDirectory: string;
  readonly entrypointPath: string;
  readonly nodeExecutablePath: string;
}

export interface ActivateRuntimeArtifactInput extends RuntimeArtifactIdentity {
  readonly activatedAt?: string;
}

export class RuntimeArtifactInstaller extends Context.Service<
  RuntimeArtifactInstaller,
  {
    readonly validateArtifact: (
      input: ValidateRuntimeArtifactInput,
    ) => Effect.Effect<RuntimeArtifactManifest, RuntimeArtifactError>;
    readonly installArtifact: (
      input: InstallRuntimeArtifactInput,
    ) => Effect.Effect<RuntimeInstallResult, RuntimeArtifactError>;
    readonly activateArtifact: (
      input: ActivateRuntimeArtifactInput,
    ) => Effect.Effect<RuntimeCurrentPointer, RuntimeArtifactError>;
    readonly getInstalledArtifact: (
      input: RuntimeArtifactIdentity,
    ) => Effect.Effect<InstalledRuntimeArtifact, RuntimeArtifactError>;
    readonly listInstalledArtifacts: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<readonly InstalledRuntimeArtifact[], RuntimeArtifactError>;
  }
>()("@t3tools/desktop/runtime/RuntimeArtifactInstaller") {}

const decodeUnknownJson = Schema.decodeUnknownEffect(Schema.UnknownFromJsonString);
const decodeManifest = Schema.decodeUnknownEffect(RuntimeArtifactManifest);
const decodeCurrentPointer = Schema.decodeUnknownEffect(RuntimeCurrentPointer);
const encodeUnknownJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

function isNotFound(error: PlatformError.PlatformError): boolean {
  return error.reason._tag === "NotFound";
}

const mapFilesystemError =
  (operation: RuntimeFilesystemError["operation"]) =>
  <A, R>(
    effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  ): Effect.Effect<A, RuntimeFilesystemError, R> =>
    effect.pipe(
      Effect.mapError(() => new RuntimeFilesystemError({ code: "filesystem-error", operation })),
    );

const writeJsonAtomically = Effect.fn("RuntimeArtifactInstaller.writeJsonAtomically")(function* (
  filePath: string,
  value: unknown,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const targetDirectory = path.dirname(filePath);
  const contents = yield* encodeUnknownJson(value).pipe(Effect.orDie);
  yield* mapFilesystemError("create-directory")(
    fs.makeDirectory(targetDirectory, { recursive: true }),
  );
  yield* Effect.scoped(
    Effect.gen(function* () {
      const temporaryDirectory = yield* mapFilesystemError("create-directory")(
        fs.makeTempDirectoryScoped({
          directory: targetDirectory,
          prefix: `.${path.basename(filePath)}.`,
        }),
      );
      const temporaryPath = path.join(temporaryDirectory, "contents.tmp");
      yield* mapFilesystemError("write-file")(fs.writeFileString(temporaryPath, `${contents}\n`));
      yield* mapFilesystemError("rename")(fs.rename(temporaryPath, filePath));
    }),
  );
});

function manifestPathCandidates(value: unknown): readonly string[] {
  if (typeof value !== "object" || value === null) return [];
  const record = value as Record<string, unknown>;
  const candidates: string[] = [];
  if (typeof record.entrypoint === "string") candidates.push(record.entrypoint);
  if (typeof record.nodeExecutable === "string") candidates.push(record.nodeExecutable);
  if (Array.isArray(record.files)) {
    for (const file of record.files) {
      if (typeof file === "object" && file !== null) {
        const filePath = (file as Record<string, unknown>).path;
        if (typeof filePath === "string") candidates.push(filePath);
      }
    }
  }
  return candidates;
}

function manifestsEqual(left: RuntimeArtifactManifest, right: RuntimeArtifactManifest): boolean {
  if (
    left.schemaVersion !== right.schemaVersion ||
    left.runtimeVersion !== right.runtimeVersion ||
    left.buildHash !== right.buildHash ||
    left.platform !== right.platform ||
    left.architecture !== right.architecture ||
    left.entrypoint !== right.entrypoint ||
    left.nodeExecutable !== right.nodeExecutable ||
    left.files.length !== right.files.length
  ) {
    return false;
  }
  const leftFiles = [...left.files].toSorted((a, b) => a.path.localeCompare(b.path));
  const rightFiles = [...right.files].toSorted((a, b) => a.path.localeCompare(b.path));
  return leftFiles.every((file, index) => {
    const other = rightFiles[index];
    return (
      other !== undefined &&
      file.path === other.path &&
      file.byteSize === other.byteSize &&
      file.sha256 === other.sha256
    );
  });
}

export const make = Effect.fn("RuntimeArtifactInstaller.make")(function* (options: {
  readonly profilesRoot: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const profileStore = yield* RuntimeProfileStore;
  const mutex = yield* Semaphore.make(1);
  const profilesRoot = path.resolve(options.profilesRoot);
  const writeJson = (filePath: string, value: unknown) =>
    writeJsonAtomically(filePath, value).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );

  const readManifest = Effect.fn("RuntimeArtifactInstaller.readManifest")(function* (
    artifactRoot: string,
    realArtifactRoot: string,
  ): Effect.fn.Return<RuntimeArtifactManifest, RuntimeArtifactError> {
    const manifestPath = path.join(artifactRoot, "manifest.json");
    const manifestInfoResult = yield* Effect.result(fs.stat(manifestPath));
    if (Result.isFailure(manifestInfoResult)) {
      if (isNotFound(manifestInfoResult.failure)) {
        return yield* new RuntimeArtifactManifestInvalidError({
          code: "artifact-manifest-invalid",
        });
      }
      return yield* new RuntimeFilesystemError({
        code: "filesystem-error",
        operation: "read-file",
      });
    }
    if (manifestInfoResult.success.type !== "File") {
      return yield* new RuntimeArtifactManifestInvalidError({
        code: "artifact-manifest-invalid",
      });
    }
    const realManifestPath = yield* mapFilesystemError("realpath")(fs.realPath(manifestPath));
    if (!isPathWithin(path, realArtifactRoot, realManifestPath)) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }
    const raw = yield* mapFilesystemError("read-file")(fs.readFileString(manifestPath));

    const unknownManifest = yield* decodeUnknownJson(raw).pipe(
      Effect.mapError(
        () =>
          new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          }),
      ),
    );
    if (
      manifestPathCandidates(unknownManifest).some(
        (candidate) => !isSafeRuntimeArtifactRelativePath(candidate),
      )
    ) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }
    return yield* decodeManifest(unknownManifest).pipe(
      Effect.mapError(
        () =>
          new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          }),
      ),
    );
  });

  const validateArtifactInternal = Effect.fn("RuntimeArtifactInstaller.validateArtifactInternal")(
    function* (
      input: ValidateRuntimeArtifactInput,
    ): Effect.fn.Return<RuntimeArtifactManifest, RuntimeArtifactError> {
      const artifactRoot = path.resolve(input.sourceDirectory);
      const realArtifactRoot = yield* mapFilesystemError("realpath")(fs.realPath(artifactRoot));
      const manifest = yield* readManifest(artifactRoot, realArtifactRoot);

      if (
        manifest.platform !== input.targetPlatform ||
        manifest.architecture !== input.targetArchitecture
      ) {
        return yield* new RuntimeArtifactPlatformMismatchError({
          code: "artifact-platform-mismatch",
          expectedPlatform: input.targetPlatform,
          expectedArchitecture: input.targetArchitecture,
          actualPlatform: manifest.platform,
          actualArchitecture: manifest.architecture,
        });
      }

      const listedPaths = new Set<string>();
      for (const file of manifest.files) {
        if (file.path === "manifest.json" || listedPaths.has(file.path)) {
          return yield* new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          });
        }
        listedPaths.add(file.path);
      }
      if (!listedPaths.has(manifest.entrypoint) || !listedPaths.has(manifest.nodeExecutable)) {
        return yield* new RuntimeArtifactManifestInvalidError({
          code: "artifact-manifest-invalid",
        });
      }

      for (const file of manifest.files) {
        const filePath = resolveRuntimeArtifactPath(path, artifactRoot, file.path);
        if (filePath === undefined) {
          return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
        }
        const exists = yield* mapFilesystemError("stat")(fs.exists(filePath));
        if (!exists) {
          return yield* new RuntimeArtifactFileMissingError({
            code: "artifact-file-missing",
            file: file.path,
          });
        }
        const realFilePath = yield* fs.realPath(filePath).pipe(
          Effect.mapError((error) =>
            isNotFound(error)
              ? new RuntimeArtifactFileMissingError({
                  code: "artifact-file-missing",
                  file: file.path,
                })
              : new RuntimeFilesystemError({ code: "filesystem-error", operation: "realpath" }),
          ),
        );
        if (!isPathWithin(path, realArtifactRoot, realFilePath)) {
          return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
        }
        const info = yield* fs.stat(filePath).pipe(
          Effect.mapError((error) =>
            isNotFound(error)
              ? new RuntimeArtifactFileMissingError({
                  code: "artifact-file-missing",
                  file: file.path,
                })
              : new RuntimeFilesystemError({ code: "filesystem-error", operation: "stat" }),
          ),
        );
        if (info.type !== "File") {
          return yield* new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          });
        }
        if (info.size !== BigInt(file.byteSize)) {
          return yield* new RuntimeArtifactSizeMismatchError({
            code: "artifact-size-mismatch",
            file: file.path,
          });
        }
        const bytes = yield* fs.readFile(filePath).pipe(
          Effect.mapError((error) =>
            isNotFound(error)
              ? new RuntimeArtifactFileMissingError({
                  code: "artifact-file-missing",
                  file: file.path,
                })
              : new RuntimeFilesystemError({ code: "filesystem-error", operation: "read-file" }),
          ),
        );
        const digest = yield* crypto.digest("SHA-256", bytes).pipe(
          Effect.mapError(
            () => new RuntimeFilesystemError({ code: "filesystem-error", operation: "digest" }),
          ),
          Effect.map((digest) => Encoding.encodeHex(digest) as RuntimeSha256Digest),
        );
        if (digest !== file.sha256) {
          return yield* new RuntimeArtifactDigestMismatchError({
            code: "artifact-digest-mismatch",
            file: file.path,
          });
        }
      }

      const artifactEntries = yield* mapFilesystemError("read-directory")(
        fs.readDirectory(artifactRoot, { recursive: true }),
      );
      for (const entry of artifactEntries) {
        const normalized = normalizeRuntimeArtifactRelativePath(path, entry);
        if (normalized === undefined) {
          return yield* new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          });
        }
        const entryPath = resolveRuntimeArtifactPath(path, artifactRoot, normalized);
        if (entryPath === undefined) {
          return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
        }
        const realEntryPath = yield* mapFilesystemError("realpath")(fs.realPath(entryPath));
        if (!isPathWithin(path, realArtifactRoot, realEntryPath)) {
          return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
        }
        const info = yield* mapFilesystemError("stat")(fs.stat(entryPath));
        if (info.type === "Directory") {
          const canonicalEntryPath = path.resolve(realArtifactRoot, ...normalized.split("/"));
          if (path.normalize(realEntryPath) !== path.normalize(canonicalEntryPath)) {
            return yield* new RuntimeArtifactManifestInvalidError({
              code: "artifact-manifest-invalid",
            });
          }
          continue;
        }
        if (normalized !== "manifest.json" && !listedPaths.has(normalized)) {
          return yield* new RuntimeArtifactManifestInvalidError({
            code: "artifact-manifest-invalid",
          });
        }
      }

      return manifest;
    },
  );

  const getInstalledArtifactInternal = Effect.fn(
    "RuntimeArtifactInstaller.getInstalledArtifactInternal",
  )(function* (
    input: RuntimeArtifactIdentity,
  ): Effect.fn.Return<InstalledRuntimeArtifact, RuntimeArtifactError> {
    const profile = yield* profileStore.getProfile(input.profileId);
    if (Option.isNone(profile)) {
      return yield* new RuntimeProfileNotFoundError({
        code: "profile-not-found",
        profileId: input.profileId,
      });
    }
    const layout = makeRuntimeProfileLayout(path, profilesRoot, input.profileId);
    const versionDirectory = runtimeVersionDirectoryName(input);
    const installDirectory = runtimeArtifactVersionDirectory(path, layout, input);
    const exists = yield* mapFilesystemError("stat")(fs.exists(installDirectory));
    if (!exists) {
      return yield* new RuntimeArtifactNotInstalledError({
        code: "artifact-not-installed",
        profileId: input.profileId,
        versionDirectory,
      });
    }

    const realVersionsDirectory = yield* mapFilesystemError("realpath")(
      fs.realPath(layout.versionsDirectory),
    );
    const realInstallDirectory = yield* mapFilesystemError("realpath")(
      fs.realPath(installDirectory),
    );
    if (!isPathWithin(path, realVersionsDirectory, realInstallDirectory)) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }
    const info = yield* mapFilesystemError("stat")(fs.stat(installDirectory));
    if (info.type !== "Directory") {
      return yield* new RuntimeArtifactInstallConflictError({
        code: "artifact-install-conflict",
        profileId: input.profileId,
        versionDirectory,
      });
    }

    const unvalidatedManifest = yield* readManifest(installDirectory, realInstallDirectory);
    const manifest = yield* validateArtifactInternal({
      sourceDirectory: installDirectory,
      targetPlatform: unvalidatedManifest.platform,
      targetArchitecture: unvalidatedManifest.architecture,
    });
    if (
      manifest.runtimeVersion !== input.runtimeVersion ||
      manifest.buildHash !== input.buildHash ||
      runtimeVersionDirectoryName(manifest) !== versionDirectory
    ) {
      return yield* new RuntimeArtifactInstallConflictError({
        code: "artifact-install-conflict",
        profileId: input.profileId,
        versionDirectory,
      });
    }
    const entrypointPath = resolveRuntimeArtifactPath(path, installDirectory, manifest.entrypoint);
    const nodeExecutablePath = resolveRuntimeArtifactPath(
      path,
      installDirectory,
      manifest.nodeExecutable,
    );
    if (entrypointPath === undefined || nodeExecutablePath === undefined) {
      return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
    }
    return {
      profileId: input.profileId,
      manifest,
      versionDirectory,
      installDirectory,
      entrypointPath,
      nodeExecutablePath,
    } satisfies InstalledRuntimeArtifact;
  });

  const installArtifactInternal = Effect.fn("RuntimeArtifactInstaller.installArtifactInternal")(
    function* (
      input: InstallRuntimeArtifactInput,
    ): Effect.fn.Return<RuntimeInstallResult, RuntimeArtifactError> {
      const sourceManifest = yield* validateArtifactInternal(input);
      const profile = yield* profileStore.getProfile(input.profileId);
      if (Option.isNone(profile)) {
        return yield* new RuntimeProfileNotFoundError({
          code: "profile-not-found",
          profileId: input.profileId,
        });
      }
      const layout = makeRuntimeProfileLayout(path, profilesRoot, input.profileId);
      const versionDirectory = runtimeVersionDirectoryName(sourceManifest);
      const installDirectory = runtimeArtifactVersionDirectory(path, layout, sourceManifest);
      const exists = yield* mapFilesystemError("stat")(fs.exists(installDirectory));
      if (exists) {
        const existing = yield* Effect.result(
          getInstalledArtifactInternal({
            profileId: input.profileId,
            runtimeVersion: sourceManifest.runtimeVersion,
            buildHash: sourceManifest.buildHash,
          }),
        );
        if (Result.isSuccess(existing)) {
          if (manifestsEqual(existing.success.manifest, sourceManifest)) {
            return {
              profileId: input.profileId,
              runtimeVersion: sourceManifest.runtimeVersion,
              buildHash: sourceManifest.buildHash,
              versionDirectory,
              status: "already-installed" as const,
            } satisfies RuntimeInstallResult;
          }
        } else if (
          existing.failure._tag === "RuntimeArtifactPathEscapeError" ||
          existing.failure._tag === "RuntimeFilesystemError"
        ) {
          return yield* existing.failure;
        }
        return yield* new RuntimeArtifactInstallConflictError({
          code: "artifact-install-conflict",
          profileId: input.profileId,
          versionDirectory,
        });
      }

      yield* Effect.scoped(
        Effect.gen(function* () {
          const temporaryDirectory = yield* mapFilesystemError("create-directory")(
            fs.makeTempDirectoryScoped({
              directory: layout.versionsDirectory,
              prefix: ".install-",
            }),
          );
          const stagedDirectory = path.join(temporaryDirectory, "artifact");
          yield* mapFilesystemError("copy")(
            fs.copy(path.resolve(input.sourceDirectory), stagedDirectory, { overwrite: false }),
          );
          const stagedManifest = yield* validateArtifactInternal({
            sourceDirectory: stagedDirectory,
            targetPlatform: input.targetPlatform,
            targetArchitecture: input.targetArchitecture,
          });
          if (!manifestsEqual(stagedManifest, sourceManifest)) {
            return yield* new RuntimeArtifactInstallConflictError({
              code: "artifact-install-conflict",
              profileId: input.profileId,
              versionDirectory,
            });
          }
          yield* mapFilesystemError("rename")(fs.rename(stagedDirectory, installDirectory));
        }),
      );

      return {
        profileId: input.profileId,
        runtimeVersion: sourceManifest.runtimeVersion,
        buildHash: sourceManifest.buildHash,
        versionDirectory,
        status: "installed" as const,
      } satisfies RuntimeInstallResult;
    },
  );

  const activateArtifactInternal = Effect.fn("RuntimeArtifactInstaller.activateArtifactInternal")(
    function* (
      input: ActivateRuntimeArtifactInput,
    ): Effect.fn.Return<RuntimeCurrentPointer, RuntimeArtifactError> {
      const installed = yield* getInstalledArtifactInternal(input);
      const activatedAt = input.activatedAt ?? DateTime.formatIso(yield* DateTime.now);
      const pointer = yield* decodeCurrentPointer({
        schemaVersion: RUNTIME_CURRENT_POINTER_SCHEMA_VERSION,
        profileId: input.profileId,
        runtimeVersion: installed.manifest.runtimeVersion,
        buildHash: installed.manifest.buildHash,
        versionDirectory: installed.versionDirectory,
        activatedAt,
      }).pipe(
        Effect.mapError(
          () => new RuntimeArtifactManifestInvalidError({ code: "artifact-manifest-invalid" }),
        ),
      );
      const layout = makeRuntimeProfileLayout(path, profilesRoot, input.profileId);
      yield* writeJson(layout.currentRuntimePath, pointer);
      return pointer;
    },
  );

  const listInstalledArtifactsInternal = Effect.fn(
    "RuntimeArtifactInstaller.listInstalledArtifactsInternal",
  )(function* (
    profileId: RuntimeProfileId,
  ): Effect.fn.Return<readonly InstalledRuntimeArtifact[], RuntimeArtifactError> {
    const profile = yield* profileStore.getProfile(profileId);
    if (Option.isNone(profile)) {
      return yield* new RuntimeProfileNotFoundError({
        code: "profile-not-found",
        profileId,
      });
    }
    const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
    const directoryNames = yield* mapFilesystemError("read-directory")(
      fs.readDirectory(layout.versionsDirectory),
    );
    const installed: InstalledRuntimeArtifact[] = [];
    for (const directoryName of directoryNames.toSorted()) {
      if (directoryName.startsWith(".")) continue;
      const installDirectory = path.join(layout.versionsDirectory, directoryName);
      const info = yield* mapFilesystemError("stat")(fs.stat(installDirectory));
      if (info.type !== "Directory") {
        return yield* new RuntimeArtifactManifestInvalidError({
          code: "artifact-manifest-invalid",
        });
      }
      const realInstallDirectory = yield* mapFilesystemError("realpath")(
        fs.realPath(installDirectory),
      );
      const realVersionsDirectory = yield* mapFilesystemError("realpath")(
        fs.realPath(layout.versionsDirectory),
      );
      if (!isPathWithin(path, realVersionsDirectory, realInstallDirectory)) {
        return yield* new RuntimeArtifactPathEscapeError({ code: "artifact-path-escape" });
      }
      const manifest = yield* readManifest(installDirectory, realInstallDirectory);
      const expectedDirectory = runtimeVersionDirectoryName(manifest);
      if (directoryName !== expectedDirectory) {
        return yield* new RuntimeArtifactInstallConflictError({
          code: "artifact-install-conflict",
          profileId,
          versionDirectory: expectedDirectory,
        });
      }
      installed.push(
        yield* getInstalledArtifactInternal({
          profileId,
          runtimeVersion: manifest.runtimeVersion,
          buildHash: manifest.buildHash,
        }),
      );
    }
    return installed;
  });

  return RuntimeArtifactInstaller.of({
    validateArtifact: (input) => mutex.withPermits(1)(validateArtifactInternal(input)),
    installArtifact: (input) => mutex.withPermits(1)(installArtifactInternal(input)),
    activateArtifact: (input) => mutex.withPermits(1)(activateArtifactInternal(input)),
    getInstalledArtifact: (input) => mutex.withPermits(1)(getInstalledArtifactInternal(input)),
    listInstalledArtifacts: (profileId) =>
      mutex.withPermits(1)(listInstalledArtifactsInternal(profileId)),
  });
});

export const layer = (options: { readonly profilesRoot: string }) =>
  Layer.effect(RuntimeArtifactInstaller, make(options));
