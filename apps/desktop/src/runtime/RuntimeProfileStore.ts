import {
  RuntimeCurrentPointer,
  RuntimeCurrentPointerCorruptError,
  RuntimeFilesystemError,
  RuntimeInvalidProfileConfigError,
  RuntimeInvalidProfileIdError,
  RuntimeProfileConfig,
  RuntimeProfileConfigConflictError,
  RuntimeProfileCorruptError,
  RuntimeProfileId,
  type RuntimeProfileListEntry,
  RuntimeProfilePathEscapeError,
} from "@t3tools/contracts/runtimeProfile";
import {
  isPathWithin,
  makeRuntimeProfileLayout,
  runtimeProfileIdFromDirectoryName,
  runtimeVersionDirectoryName,
  type RuntimeProfileLayout,
} from "@t3tools/shared/runtimeProfile";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";

type EnsureProfileError =
  | RuntimeInvalidProfileIdError
  | RuntimeInvalidProfileConfigError
  | RuntimeProfileConfigConflictError
  | RuntimeProfileCorruptError
  | RuntimeProfilePathEscapeError
  | RuntimeFilesystemError;

type GetProfileError =
  | RuntimeInvalidProfileIdError
  | RuntimeProfileCorruptError
  | RuntimeProfilePathEscapeError
  | RuntimeFilesystemError;

type GetCurrentRuntimeError = GetProfileError | RuntimeCurrentPointerCorruptError;

export class RuntimeProfileStore extends Context.Service<
  RuntimeProfileStore,
  {
    readonly ensureProfile: (
      config: RuntimeProfileConfig,
    ) => Effect.Effect<RuntimeProfileConfig, EnsureProfileError>;
    readonly getProfile: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<Option.Option<RuntimeProfileConfig>, GetProfileError>;
    readonly getCurrentRuntime: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<Option.Option<RuntimeCurrentPointer>, GetCurrentRuntimeError>;
    /**
     * Reads and validates current.json without resolving its referenced artifact.
     * Callers must validate the returned identity through RuntimeArtifactInstaller.
     */
    readonly readCurrentRuntimePointer: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<Option.Option<RuntimeCurrentPointer>, GetCurrentRuntimeError>;
    readonly listProfiles: Effect.Effect<
      readonly RuntimeProfileListEntry[],
      RuntimeFilesystemError
    >;
  }
>()("@t3tools/desktop/runtime/RuntimeProfileStore") {}

const decodeProfileId = Schema.decodeUnknownEffect(RuntimeProfileId);
const decodeProfileConfigJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RuntimeProfileConfig),
);
const decodeProfileConfig = Schema.decodeUnknownEffect(RuntimeProfileConfig);
const decodeCurrentPointerJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RuntimeCurrentPointer),
);
const encodeUnknownJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

function isNotFound(error: PlatformError.PlatformError): boolean {
  return error.reason._tag === "NotFound";
}

const mapFilesystemError =
  (operation: RuntimeFilesystemError["operation"]) =>
  <A, R>(
    effect: Effect.Effect<A, PlatformError.PlatformError, R>,
  ): Effect.Effect<A, RuntimeFilesystemError, R> =>
    effect.pipe(
      Effect.mapError(
        () =>
          new RuntimeFilesystemError({
            code: "filesystem-error",
            operation,
          }),
      ),
    );

const readOptionalFile = Effect.fn("RuntimeProfileStore.readOptionalFile")(function* (
  filePath: string,
) {
  const fs = yield* FileSystem.FileSystem;
  return yield* fs.readFileString(filePath).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        isNotFound(error)
          ? Effect.succeed(Option.none<string>())
          : Effect.fail(
              new RuntimeFilesystemError({
                code: "filesystem-error",
                operation: "read-file",
              }),
            ),
      onSuccess: (contents) => Effect.succeed(Option.some(contents)),
    }),
  );
});

const writeJsonAtomically = Effect.fn("RuntimeProfileStore.writeJsonAtomically")(function* (
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
      const tempDirectory = yield* mapFilesystemError("create-directory")(
        fs.makeTempDirectoryScoped({
          directory: targetDirectory,
          prefix: `.${path.basename(filePath)}.`,
        }),
      );
      const tempPath = path.join(tempDirectory, "contents.tmp");
      yield* mapFilesystemError("write-file")(fs.writeFileString(tempPath, `${contents}\n`));
      yield* mapFilesystemError("rename")(fs.rename(tempPath, filePath));
    }),
  );
});

function profileConfigConflictFields(
  existing: RuntimeProfileConfig,
  requested: RuntimeProfileConfig,
): RuntimeProfileConfigConflictError["conflictingFields"] {
  const fields = ["schemaVersion", "profileId", "port", "createdAt", "updatedAt"] as const;
  return fields.filter((field) => existing[field] !== requested[field]);
}

export const make = Effect.fn("RuntimeProfileStore.make")(function* (options: {
  readonly profilesRoot: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const mutex = yield* Semaphore.make(1);
  const profilesRoot = path.resolve(options.profilesRoot);
  const readOptional = (filePath: string) =>
    readOptionalFile(filePath).pipe(Effect.provideService(FileSystem.FileSystem, fs));
  const writeJson = (filePath: string, value: unknown) =>
    writeJsonAtomically(filePath, value).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Path.Path, path),
    );

  const layoutFor = (profileId: RuntimeProfileId) =>
    makeRuntimeProfileLayout(path, profilesRoot, profileId);

  const assertProfilePathWithinRoot = Effect.fn("RuntimeProfileStore.assertProfilePathWithinRoot")(
    function* (profileId: RuntimeProfileId, realRoot: string, candidate: string) {
      const realCandidate = yield* mapFilesystemError("realpath")(fs.realPath(candidate));
      if (!isPathWithin(path, realRoot, realCandidate)) {
        return yield* new RuntimeProfilePathEscapeError({
          code: "profile-path-escape",
          profileId,
        });
      }
      return realCandidate;
    },
  );

  const assertExistingPathWithinRoot = Effect.fn(
    "RuntimeProfileStore.assertExistingPathWithinRoot",
  )(function* (profileId: RuntimeProfileId, realRoot: string, candidate: string) {
    const exists = yield* mapFilesystemError("stat")(fs.exists(candidate));
    if (!exists) return;
    yield* assertProfilePathWithinRoot(profileId, realRoot, candidate);
  });

  const prepareProfileDirectories = Effect.fn("RuntimeProfileStore.prepareProfileDirectories")(
    function* (layout: RuntimeProfileLayout) {
      yield* mapFilesystemError("create-directory")(
        fs.makeDirectory(layout.profilesRoot, { recursive: true }),
      );
      const realRoot = yield* mapFilesystemError("realpath")(fs.realPath(layout.profilesRoot));
      yield* mapFilesystemError("create-directory")(
        fs.makeDirectory(layout.profileDirectory, { recursive: true }),
      );
      yield* assertProfilePathWithinRoot(layout.profileId, realRoot, layout.profileDirectory);

      const directories = [
        layout.runtimeDirectory,
        layout.launcherDirectory,
        layout.versionsDirectory,
        layout.stateDirectory,
        layout.logsDirectory,
        layout.runDirectory,
      ];
      for (const directory of directories) {
        yield* mapFilesystemError("create-directory")(
          fs.makeDirectory(directory, { recursive: true }),
        );
        yield* assertProfilePathWithinRoot(layout.profileId, realRoot, directory);
      }
      return realRoot;
    },
  );

  const getProfileInternal = Effect.fn("RuntimeProfileStore.getProfileInternal")(function* (
    rawProfileId: RuntimeProfileId,
  ) {
    const profileId = yield* decodeProfileId(rawProfileId).pipe(
      Effect.mapError(() => new RuntimeInvalidProfileIdError({ code: "invalid-profile-id" })),
    );
    const layout = layoutFor(profileId);
    const rootExists = yield* mapFilesystemError("stat")(fs.exists(layout.profilesRoot));
    if (!rootExists) return Option.none<RuntimeProfileConfig>();
    const realRoot = yield* mapFilesystemError("realpath")(fs.realPath(layout.profilesRoot));

    const profileExists = yield* mapFilesystemError("stat")(fs.exists(layout.profileDirectory));
    if (!profileExists) return Option.none<RuntimeProfileConfig>();
    yield* assertProfilePathWithinRoot(profileId, realRoot, layout.profileDirectory);

    yield* assertExistingPathWithinRoot(profileId, realRoot, layout.profileConfigPath);
    const raw = yield* readOptional(layout.profileConfigPath);
    if (Option.isNone(raw)) {
      return yield* new RuntimeProfileCorruptError({
        code: "profile-corrupt",
        profileId,
      });
    }
    const config = yield* decodeProfileConfigJson(raw.value).pipe(
      Effect.mapError(
        () =>
          new RuntimeProfileCorruptError({
            code: "profile-corrupt",
            profileId,
          }),
      ),
    );
    if (config.profileId !== profileId) {
      return yield* new RuntimeProfileCorruptError({
        code: "profile-corrupt",
        profileId,
      });
    }
    return Option.some(config);
  });

  const readCurrentRuntimePointerInternal = Effect.fn(
    "RuntimeProfileStore.readCurrentRuntimePointerInternal",
  )(function* (rawProfileId: RuntimeProfileId) {
    const profile = yield* getProfileInternal(rawProfileId);
    if (Option.isNone(profile)) return Option.none<RuntimeCurrentPointer>();
    const profileId = profile.value.profileId;
    const layout = layoutFor(profileId);
    const realRoot = yield* mapFilesystemError("realpath")(fs.realPath(layout.profilesRoot));
    yield* assertExistingPathWithinRoot(profileId, realRoot, layout.currentRuntimePath);
    const raw = yield* readOptional(layout.currentRuntimePath);
    if (Option.isNone(raw)) return Option.none<RuntimeCurrentPointer>();

    const pointer = yield* decodeCurrentPointerJson(raw.value).pipe(
      Effect.mapError(
        () =>
          new RuntimeCurrentPointerCorruptError({
            code: "current-pointer-corrupt",
            profileId,
          }),
      ),
    );
    const expectedVersionDirectory = runtimeVersionDirectoryName(pointer);
    if (pointer.profileId !== profileId || pointer.versionDirectory !== expectedVersionDirectory) {
      return yield* new RuntimeCurrentPointerCorruptError({
        code: "current-pointer-corrupt",
        profileId,
      });
    }
    return Option.some(pointer);
  });

  const getCurrentRuntimeInternal = Effect.fn("RuntimeProfileStore.getCurrentRuntimeInternal")(
    function* (rawProfileId: RuntimeProfileId) {
      const pointer = yield* readCurrentRuntimePointerInternal(rawProfileId);
      if (Option.isNone(pointer)) return pointer;
      const profileId = pointer.value.profileId;
      const layout = layoutFor(profileId);
      const versionsRealPath = yield* mapFilesystemError("realpath")(
        fs.realPath(layout.versionsDirectory),
      );
      const installedDirectory = path.join(
        layout.versionsDirectory,
        pointer.value.versionDirectory,
      );
      const installedExists = yield* mapFilesystemError("stat")(fs.exists(installedDirectory));
      if (!installedExists) {
        return yield* new RuntimeCurrentPointerCorruptError({
          code: "current-pointer-corrupt",
          profileId,
        });
      }
      const installedRealPath = yield* mapFilesystemError("realpath")(
        fs.realPath(installedDirectory),
      );
      if (!isPathWithin(path, versionsRealPath, installedRealPath)) {
        return yield* new RuntimeCurrentPointerCorruptError({
          code: "current-pointer-corrupt",
          profileId,
        });
      }
      const installedInfo = yield* mapFilesystemError("stat")(fs.stat(installedDirectory));
      if (installedInfo.type !== "Directory") {
        return yield* new RuntimeCurrentPointerCorruptError({
          code: "current-pointer-corrupt",
          profileId,
        });
      }
      return pointer;
    },
  );

  const ensureProfile: RuntimeProfileStore["Service"]["ensureProfile"] = (rawConfig) =>
    mutex.withPermits(1)(
      Effect.gen(function* () {
        const rawProfileId =
          typeof rawConfig === "object" && rawConfig !== null
            ? (rawConfig as { readonly profileId?: unknown }).profileId
            : undefined;
        yield* decodeProfileId(rawProfileId).pipe(
          Effect.mapError(() => new RuntimeInvalidProfileIdError({ code: "invalid-profile-id" })),
        );
        const config = yield* decodeProfileConfig(rawConfig).pipe(
          Effect.mapError(
            () =>
              new RuntimeInvalidProfileConfigError({
                code: "invalid-profile-config",
              }),
          ),
        );
        const layout = layoutFor(config.profileId);
        const realRoot = yield* prepareProfileDirectories(layout);
        yield* assertExistingPathWithinRoot(config.profileId, realRoot, layout.profileConfigPath);

        const existingRaw = yield* readOptional(layout.profileConfigPath);
        if (Option.isSome(existingRaw)) {
          const existing = yield* decodeProfileConfigJson(existingRaw.value).pipe(
            Effect.mapError(
              () =>
                new RuntimeProfileCorruptError({
                  code: "profile-corrupt",
                  profileId: config.profileId,
                }),
            ),
          );
          const conflictingFields = profileConfigConflictFields(existing, config);
          if (conflictingFields.length > 0) {
            return yield* new RuntimeProfileConfigConflictError({
              code: "profile-config-conflict",
              profileId: config.profileId,
              conflictingFields,
            });
          }
          return existing;
        }

        yield* writeJson(layout.profileConfigPath, config);
        return config;
      }),
    );

  const getProfile: RuntimeProfileStore["Service"]["getProfile"] = (profileId) =>
    mutex.withPermits(1)(getProfileInternal(profileId));

  const getCurrentRuntime: RuntimeProfileStore["Service"]["getCurrentRuntime"] = (profileId) =>
    mutex.withPermits(1)(getCurrentRuntimeInternal(profileId));

  const readCurrentRuntimePointer: RuntimeProfileStore["Service"]["readCurrentRuntimePointer"] = (
    profileId,
  ) => mutex.withPermits(1)(readCurrentRuntimePointerInternal(profileId));

  const listProfiles: RuntimeProfileStore["Service"]["listProfiles"] = mutex.withPermits(1)(
    Effect.gen(function* () {
      const rootExists = yield* mapFilesystemError("stat")(fs.exists(profilesRoot));
      if (!rootExists) return [];
      const directoryNames = yield* mapFilesystemError("read-directory")(
        fs.readDirectory(profilesRoot),
      );
      const entries: RuntimeProfileListEntry[] = [];
      for (const directoryName of directoryNames.toSorted()) {
        const profileId = runtimeProfileIdFromDirectoryName(directoryName);
        if (profileId === undefined) continue;

        const result = yield* Effect.result(
          Effect.gen(function* () {
            const profile = yield* getProfileInternal(profileId);
            if (Option.isNone(profile)) {
              return yield* new RuntimeProfileCorruptError({
                code: "profile-corrupt",
                profileId,
              });
            }
            const currentRuntime = yield* getCurrentRuntimeInternal(profileId);
            return {
              status: "ready" as const,
              profileId,
              config: profile.value,
              currentRuntime: Option.getOrNull(currentRuntime),
            };
          }),
        );
        if (Result.isSuccess(result)) {
          entries.push(result.success);
          continue;
        }

        const error = result.failure;
        switch (error._tag) {
          case "RuntimeProfileCorruptError":
            entries.push({ status: "quarantined", profileId, reason: "profile-corrupt" });
            break;
          case "RuntimeCurrentPointerCorruptError":
            entries.push({
              status: "quarantined",
              profileId,
              reason: "current-pointer-corrupt",
            });
            break;
          case "RuntimeProfilePathEscapeError":
            entries.push({ status: "quarantined", profileId, reason: "profile-path-escape" });
            break;
          case "RuntimeFilesystemError":
            return yield* error;
          case "RuntimeInvalidProfileIdError":
            return yield* new RuntimeFilesystemError({
              code: "filesystem-error",
              operation: "read-directory",
            });
        }
      }
      return entries;
    }),
  );

  return RuntimeProfileStore.of({
    ensureProfile,
    getProfile,
    getCurrentRuntime,
    readCurrentRuntimePointer,
    listProfiles,
  });
});

export const layer = (options: { readonly profilesRoot: string }) =>
  Layer.effect(RuntimeProfileStore, make(options));
