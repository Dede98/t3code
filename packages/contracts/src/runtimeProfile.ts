/**
 * Schema-only contracts for isolated, profile-bound daemon runtimes.
 *
 * This module deliberately contains no filesystem, credential, process, or
 * lifecycle logic. Runtime implementations live outside packages/contracts.
 *
 * @module runtimeProfile
 */
import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, PortSchema } from "./baseSchemas.ts";

export const RUNTIME_PROFILE_SCHEMA_VERSION = 1 as const;
export const RUNTIME_ARTIFACT_SCHEMA_VERSION = 1 as const;
export const RUNTIME_CURRENT_POINTER_SCHEMA_VERSION = 1 as const;
export const RUNTIME_PREFLIGHT_SCHEMA_VERSION = 1 as const;

const CUSTOM_PROFILE_SLUG_MAX_CHARS = 63;
const CUSTOM_PROFILE_SLUG_PATTERN = /^[a-z0-9][a-z0-9-]*$/;
const RUNTIME_VERSION_PATTERN = /^(?!.*\.\.)[A-Za-z0-9][A-Za-z0-9._-]*$/;
const BUILD_HASH_PATTERN = /^[a-f0-9]{7,64}$/;
const VERSION_DIRECTORY_PATTERN = /^(?!.*\.\.)(?!.*[\\/:])[A-Za-z0-9][A-Za-z0-9._-]*$/;
const SHA_256_PATTERN = /^[a-f0-9]{64}$/;
const PORTABLE_RELATIVE_PATH_PATTERN =
  /^(?!\/)(?!.*\\)(?!.*:)(?!.*\/\/)(?!.*(?:^|\/)\.\.?(?:\/|$)).+$/;

const CustomProfileSlug = Schema.String.check(
  Schema.isMaxLength(CUSTOM_PROFILE_SLUG_MAX_CHARS),
  Schema.isPattern(CUSTOM_PROFILE_SLUG_PATTERN),
);

export const RuntimeProfileKind = Schema.Literals(["dev", "alpha", "nightly", "custom"]);
export type RuntimeProfileKind = typeof RuntimeProfileKind.Type;

export const RuntimeProfileId = Schema.Union([
  Schema.Literals(["dev", "alpha", "nightly"]),
  Schema.TemplateLiteral(["custom:", CustomProfileSlug]),
]).pipe(Schema.brand("RuntimeProfileId"));
export type RuntimeProfileId = typeof RuntimeProfileId.Type;

export const RuntimeVersion = Schema.String.check(
  Schema.isMaxLength(128),
  Schema.isPattern(RUNTIME_VERSION_PATTERN),
).pipe(Schema.brand("RuntimeVersion"));
export type RuntimeVersion = typeof RuntimeVersion.Type;

export const RuntimeBuildHash = Schema.String.check(
  Schema.isMaxLength(64),
  Schema.isPattern(BUILD_HASH_PATTERN),
).pipe(Schema.brand("RuntimeBuildHash"));
export type RuntimeBuildHash = typeof RuntimeBuildHash.Type;

export const RuntimeVersionDirectory = Schema.String.check(
  Schema.isMaxLength(196),
  Schema.isPattern(VERSION_DIRECTORY_PATTERN),
).pipe(Schema.brand("RuntimeVersionDirectory"));
export type RuntimeVersionDirectory = typeof RuntimeVersionDirectory.Type;

export const RuntimeArtifactRelativePath = Schema.String.check(
  Schema.isMaxLength(1_024),
  Schema.isPattern(PORTABLE_RELATIVE_PATH_PATTERN),
  Schema.makeFilter((input) => !input.includes("\0") || "Path must not contain a null byte."),
).pipe(Schema.brand("RuntimeArtifactRelativePath"));
export type RuntimeArtifactRelativePath = typeof RuntimeArtifactRelativePath.Type;

export const RuntimeSha256Digest = Schema.String.check(Schema.isPattern(SHA_256_PATTERN)).pipe(
  Schema.brand("RuntimeSha256Digest"),
);
export type RuntimeSha256Digest = typeof RuntimeSha256Digest.Type;

export const RuntimeArtifactPlatform = Schema.Literals(["darwin", "linux", "win32"]);
export type RuntimeArtifactPlatform = typeof RuntimeArtifactPlatform.Type;

export const RuntimeArtifactArchitecture = Schema.Literals(["arm64", "x64"]);
export type RuntimeArtifactArchitecture = typeof RuntimeArtifactArchitecture.Type;

export const RuntimeProfileConfig = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_PROFILE_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  port: PortSchema,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type RuntimeProfileConfig = typeof RuntimeProfileConfig.Type;

export const RuntimeArtifactFile = Schema.Struct({
  path: RuntimeArtifactRelativePath,
  byteSize: NonNegativeInt,
  sha256: RuntimeSha256Digest,
});
export type RuntimeArtifactFile = typeof RuntimeArtifactFile.Type;

export const RuntimeArtifactManifest = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_ARTIFACT_SCHEMA_VERSION),
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  platform: RuntimeArtifactPlatform,
  architecture: RuntimeArtifactArchitecture,
  entrypoint: RuntimeArtifactRelativePath,
  nodeExecutable: RuntimeArtifactRelativePath,
  files: Schema.Array(RuntimeArtifactFile),
});
export type RuntimeArtifactManifest = typeof RuntimeArtifactManifest.Type;

export const RuntimeCurrentPointer = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_CURRENT_POINTER_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  versionDirectory: RuntimeVersionDirectory,
  activatedAt: IsoDateTime,
});
export type RuntimeCurrentPointer = typeof RuntimeCurrentPointer.Type;

export const RuntimeInstallResult = Schema.Struct({
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  versionDirectory: RuntimeVersionDirectory,
  status: Schema.Literals(["installed", "already-installed"]),
});
export type RuntimeInstallResult = typeof RuntimeInstallResult.Type;

export const RuntimeProfileListEntry = Schema.Union([
  Schema.Struct({
    status: Schema.Literal("ready"),
    profileId: RuntimeProfileId,
    config: RuntimeProfileConfig,
    currentRuntime: Schema.NullOr(RuntimeCurrentPointer),
  }),
  Schema.Struct({
    status: Schema.Literal("quarantined"),
    profileId: RuntimeProfileId,
    reason: Schema.Literals(["profile-corrupt", "current-pointer-corrupt", "profile-path-escape"]),
  }),
]);
export type RuntimeProfileListEntry = typeof RuntimeProfileListEntry.Type;

export const RuntimePreflightCheck = Schema.Struct({
  check: Schema.Literals([
    "profile-config",
    "runtime-current",
    "runtime-artifact",
    "state-directory",
    "logs-directory",
    "run-directory",
    "daemon-lock",
    "discovery-file",
    "credential-service",
  ]),
  status: Schema.Literals(["ready", "missing", "invalid"]),
});
export type RuntimePreflightCheck = typeof RuntimePreflightCheck.Type;

export const RuntimePreflightResult = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_PREFLIGHT_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  platform: RuntimeArtifactPlatform,
  architecture: RuntimeArtifactArchitecture,
  credentialServiceName: Schema.String,
  ok: Schema.Boolean,
  checks: Schema.Array(RuntimePreflightCheck),
});
export type RuntimePreflightResult = typeof RuntimePreflightResult.Type;

export class RuntimeInvalidProfileIdError extends Schema.TaggedErrorClass<RuntimeInvalidProfileIdError>()(
  "RuntimeInvalidProfileIdError",
  { code: Schema.Literal("invalid-profile-id") },
) {}

export class RuntimeInvalidProfileConfigError extends Schema.TaggedErrorClass<RuntimeInvalidProfileConfigError>()(
  "RuntimeInvalidProfileConfigError",
  { code: Schema.Literal("invalid-profile-config") },
) {}

export class RuntimeProfileConfigConflictError extends Schema.TaggedErrorClass<RuntimeProfileConfigConflictError>()(
  "RuntimeProfileConfigConflictError",
  {
    code: Schema.Literal("profile-config-conflict"),
    profileId: RuntimeProfileId,
    conflictingFields: Schema.Array(
      Schema.Literals(["schemaVersion", "profileId", "port", "createdAt", "updatedAt"]),
    ),
  },
) {}

export class RuntimeProfileNotFoundError extends Schema.TaggedErrorClass<RuntimeProfileNotFoundError>()(
  "RuntimeProfileNotFoundError",
  {
    code: Schema.Literal("profile-not-found"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeProfileCorruptError extends Schema.TaggedErrorClass<RuntimeProfileCorruptError>()(
  "RuntimeProfileCorruptError",
  {
    code: Schema.Literal("profile-corrupt"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeProfilePathEscapeError extends Schema.TaggedErrorClass<RuntimeProfilePathEscapeError>()(
  "RuntimeProfilePathEscapeError",
  {
    code: Schema.Literal("profile-path-escape"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeArtifactManifestInvalidError extends Schema.TaggedErrorClass<RuntimeArtifactManifestInvalidError>()(
  "RuntimeArtifactManifestInvalidError",
  { code: Schema.Literal("artifact-manifest-invalid") },
) {}

export class RuntimeArtifactFileMissingError extends Schema.TaggedErrorClass<RuntimeArtifactFileMissingError>()(
  "RuntimeArtifactFileMissingError",
  {
    code: Schema.Literal("artifact-file-missing"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactSizeMismatchError extends Schema.TaggedErrorClass<RuntimeArtifactSizeMismatchError>()(
  "RuntimeArtifactSizeMismatchError",
  {
    code: Schema.Literal("artifact-size-mismatch"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactDigestMismatchError extends Schema.TaggedErrorClass<RuntimeArtifactDigestMismatchError>()(
  "RuntimeArtifactDigestMismatchError",
  {
    code: Schema.Literal("artifact-digest-mismatch"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactPlatformMismatchError extends Schema.TaggedErrorClass<RuntimeArtifactPlatformMismatchError>()(
  "RuntimeArtifactPlatformMismatchError",
  {
    code: Schema.Literal("artifact-platform-mismatch"),
    expectedPlatform: RuntimeArtifactPlatform,
    expectedArchitecture: RuntimeArtifactArchitecture,
    actualPlatform: RuntimeArtifactPlatform,
    actualArchitecture: RuntimeArtifactArchitecture,
  },
) {}

export class RuntimeArtifactPathEscapeError extends Schema.TaggedErrorClass<RuntimeArtifactPathEscapeError>()(
  "RuntimeArtifactPathEscapeError",
  { code: Schema.Literal("artifact-path-escape") },
) {}

export class RuntimeArtifactInstallConflictError extends Schema.TaggedErrorClass<RuntimeArtifactInstallConflictError>()(
  "RuntimeArtifactInstallConflictError",
  {
    code: Schema.Literal("artifact-install-conflict"),
    profileId: RuntimeProfileId,
    versionDirectory: RuntimeVersionDirectory,
  },
) {}

export class RuntimeArtifactNotInstalledError extends Schema.TaggedErrorClass<RuntimeArtifactNotInstalledError>()(
  "RuntimeArtifactNotInstalledError",
  {
    code: Schema.Literal("artifact-not-installed"),
    profileId: RuntimeProfileId,
    versionDirectory: RuntimeVersionDirectory,
  },
) {}

export class RuntimeCurrentPointerCorruptError extends Schema.TaggedErrorClass<RuntimeCurrentPointerCorruptError>()(
  "RuntimeCurrentPointerCorruptError",
  {
    code: Schema.Literal("current-pointer-corrupt"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeFilesystemError extends Schema.TaggedErrorClass<RuntimeFilesystemError>()(
  "RuntimeFilesystemError",
  {
    code: Schema.Literal("filesystem-error"),
    operation: Schema.Literals([
      "create-directory",
      "read-directory",
      "read-file",
      "write-file",
      "rename",
      "remove",
      "copy",
      "stat",
      "realpath",
      "digest",
    ]),
  },
) {}

export type RuntimeProfileError =
  | RuntimeInvalidProfileIdError
  | RuntimeInvalidProfileConfigError
  | RuntimeProfileConfigConflictError
  | RuntimeProfileNotFoundError
  | RuntimeProfileCorruptError
  | RuntimeProfilePathEscapeError
  | RuntimeCurrentPointerCorruptError
  | RuntimeFilesystemError;

export type RuntimeArtifactError =
  | RuntimeArtifactManifestInvalidError
  | RuntimeArtifactFileMissingError
  | RuntimeArtifactSizeMismatchError
  | RuntimeArtifactDigestMismatchError
  | RuntimeArtifactPlatformMismatchError
  | RuntimeArtifactPathEscapeError
  | RuntimeArtifactInstallConflictError
  | RuntimeArtifactNotInstalledError
  | RuntimeProfileError;
