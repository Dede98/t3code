/**
 * Schema-only contracts for isolated, profile-bound daemon runtimes.
 *
 * This module deliberately contains no filesystem, credential, process, or
 * lifecycle logic. Runtime implementations live outside packages/contracts.
 *
 * @module runtimeProfile
 */
import * as Schema from "effect/Schema";

import { IsoDateTime, NonNegativeInt, PortSchema, PositiveInt } from "./baseSchemas.ts";

export const RUNTIME_PROFILE_SCHEMA_VERSION = 1 as const;
export const RUNTIME_ARTIFACT_SCHEMA_VERSION = 1 as const;
export const RUNTIME_CURRENT_POINTER_SCHEMA_VERSION = 1 as const;
export const RUNTIME_PREFLIGHT_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_PREFLIGHT_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_LAUNCH_PLAN_SCHEMA_VERSION = 2 as const;
export const RUNTIME_DAEMON_LAUNCHER_CONFIG_SCHEMA_VERSION = 2 as const;
export const RUNTIME_DAEMON_LAUNCHER_INSTALLATION_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_LOCK_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_DISCOVERY_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_RECOVERY_CONFIG_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_RECOVERY_STATE_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_RECOVERY_RESET_SCHEMA_VERSION = 1 as const;
export const RUNTIME_DAEMON_STATUS_SCHEMA_VERSION = 2 as const;

const CUSTOM_PROFILE_SLUG_MAX_CHARS = 63;
const CUSTOM_PROFILE_SLUG_PATTERN = /^[a-z0-9][a-z0-9-]*$/;
const RUNTIME_VERSION_PATTERN = /^(?!.*\.\.)[A-Za-z0-9][A-Za-z0-9._-]*$/;
const BUILD_HASH_PATTERN = /^[a-f0-9]{7,64}$/;
const VERSION_DIRECTORY_PATTERN = /^(?!.*\.\.)(?!.*[\\/:])[A-Za-z0-9][A-Za-z0-9._-]*$/;
const SHA_256_PATTERN = /^[a-f0-9]{64}$/;
const OWNERSHIP_ID_PATTERN = /^[a-f0-9]{32}$/;
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

export const RuntimeDaemonPreflightCheck = Schema.Struct({
  check: Schema.Literals([
    "profile-config",
    "runtime-current",
    "runtime-artifact",
    "runtime-platform",
    "runtime-architecture",
    "node-executable",
    "server-entrypoint",
    "state-directory",
    "logs-directory",
    "run-directory",
  ]),
  status: Schema.Literal("ready"),
});
export type RuntimeDaemonPreflightCheck = typeof RuntimeDaemonPreflightCheck.Type;

export const RuntimeDaemonPreflightResult = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_PREFLIGHT_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  platform: RuntimeArtifactPlatform,
  architecture: RuntimeArtifactArchitecture,
  ok: Schema.Literal(true),
  checks: Schema.Array(RuntimeDaemonPreflightCheck),
});
export type RuntimeDaemonPreflightResult = typeof RuntimeDaemonPreflightResult.Type;

export const RuntimeDaemonEnvironment = Schema.Struct({
  T3CODE_MODE: Schema.Literal("web"),
  T3CODE_HOST: Schema.Literal("127.0.0.1"),
  T3CODE_PORT: Schema.String,
  T3CODE_HOME: Schema.String,
  T3CODE_STATE_DIR: Schema.String,
  T3CODE_LOGS_DIR: Schema.String,
  T3CODE_NO_BROWSER: Schema.Literal("true"),
  T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: Schema.Literal("false"),
  T3CODE_TAILSCALE_SERVE: Schema.Literal("false"),
});
export type RuntimeDaemonEnvironment = typeof RuntimeDaemonEnvironment.Type;

export const RuntimeDaemonLaunchPlan = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_LAUNCH_PLAN_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  versionDirectory: RuntimeVersionDirectory,
  nodeExecutablePath: Schema.String,
  serverEntrypointPath: Schema.String,
  argv: Schema.Array(Schema.String),
  cwd: Schema.String,
  environment: RuntimeDaemonEnvironment,
  port: PortSchema,
  origin: Schema.String,
  profileDirectory: Schema.String,
  runtimeVersionDirectory: Schema.String,
  stateDirectory: Schema.String,
  logsDirectory: Schema.String,
  runDirectory: Schema.String,
  daemonLockPath: Schema.String,
  discoveryPath: Schema.String,
  recoveryPath: Schema.String,
  preflight: RuntimeDaemonPreflightResult,
});
export type RuntimeDaemonLaunchPlan = typeof RuntimeDaemonLaunchPlan.Type;

export const RuntimeDaemonOwnershipId = Schema.String.check(
  Schema.isPattern(OWNERSHIP_ID_PATTERN),
).pipe(Schema.brand("RuntimeDaemonOwnershipId"));
export type RuntimeDaemonOwnershipId = typeof RuntimeDaemonOwnershipId.Type;

const RuntimeDaemonRecoveryRestartLimit = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 100 }),
);
const RuntimeDaemonRecoveryDurationMs = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 86_400_000 }),
);
const RuntimeDaemonRecoveryConsecutiveFailureLimit = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 100 }),
);

export const RuntimeDaemonRecoveryConfig = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_RECOVERY_CONFIG_SCHEMA_VERSION),
  enabled: Schema.Literal(true),
  maxRestarts: RuntimeDaemonRecoveryRestartLimit,
  slidingWindowMs: RuntimeDaemonRecoveryDurationMs,
  initialBackoffMs: RuntimeDaemonRecoveryDurationMs,
  maxBackoffMs: RuntimeDaemonRecoveryDurationMs,
  healthcheckIntervalMs: RuntimeDaemonRecoveryDurationMs,
  consecutiveHealthFailuresBeforeRestart: RuntimeDaemonRecoveryConsecutiveFailureLimit,
  healthyResetAfterMs: RuntimeDaemonRecoveryDurationMs,
}).check(
  Schema.makeFilter(
    (input) =>
      input.maxBackoffMs >= input.initialBackoffMs ||
      "Maximum recovery backoff must not be lower than initial backoff.",
  ),
);
export type RuntimeDaemonRecoveryConfig = typeof RuntimeDaemonRecoveryConfig.Type;

export const RuntimeDaemonRecoveryCircuitState = Schema.Literals(["closed", "backoff", "open"]);
export type RuntimeDaemonRecoveryCircuitState = typeof RuntimeDaemonRecoveryCircuitState.Type;

export const RuntimeDaemonRecoveryFailureReason = Schema.Literals([
  "server-exited",
  "startup-health-timeout",
  "runtime-state-invalid",
  "runtime-state-mismatch",
  "healthcheck-failed",
  "shutdown-timeout",
  "launcher-internal",
]);
export type RuntimeDaemonRecoveryFailureReason = typeof RuntimeDaemonRecoveryFailureReason.Type;

const RuntimeDaemonRecoveryIsoDateTime = Schema.String.check(
  Schema.makeFilter(
    (input) => Number.isFinite(Date.parse(input)) || "Recovery timestamp must be ISO-compatible.",
  ),
);

export const RuntimeDaemonRecoveryState = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_RECOVERY_STATE_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  circuitState: RuntimeDaemonRecoveryCircuitState,
  failureTimestamps: Schema.Array(RuntimeDaemonRecoveryIsoDateTime),
  lastFailureReason: Schema.NullOr(RuntimeDaemonRecoveryFailureReason),
  nextRestartAt: Schema.NullOr(RuntimeDaemonRecoveryIsoDateTime),
  lastSuccessfulHealthcheckAt: Schema.NullOr(RuntimeDaemonRecoveryIsoDateTime),
  continuousHealthySince: Schema.NullOr(RuntimeDaemonRecoveryIsoDateTime),
  circuitOpenedAt: Schema.NullOr(RuntimeDaemonRecoveryIsoDateTime),
}).check(
  Schema.makeFilter((input) => {
    if (input.circuitState === "closed") {
      return (
        (input.nextRestartAt === null && input.circuitOpenedAt === null) ||
        "Closed recovery state must not retain backoff or open timestamps."
      );
    }
    if (input.continuousHealthySince !== null || input.lastFailureReason === null) {
      return "Non-closed recovery state requires a failure and cannot be continuously healthy.";
    }
    if (input.failureTimestamps.length === 0) {
      return "Non-closed recovery state requires failure history.";
    }
    return input.circuitState === "backoff"
      ? (input.nextRestartAt !== null && input.circuitOpenedAt === null) ||
          "Backoff recovery state requires only a next restart timestamp."
      : (input.nextRestartAt === null && input.circuitOpenedAt !== null) ||
          "Open recovery state requires only a circuit-open timestamp.";
  }),
);
export type RuntimeDaemonRecoveryState = typeof RuntimeDaemonRecoveryState.Type;

export const RuntimeDaemonRecoveryResetResult = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_RECOVERY_RESET_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  status: Schema.Literals(["reset", "already-reset"]),
});
export type RuntimeDaemonRecoveryResetResult = typeof RuntimeDaemonRecoveryResetResult.Type;

export const RuntimeDaemonLauncherConfig = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_LAUNCHER_CONFIG_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  versionDirectory: RuntimeVersionDirectory,
  launchPlan: RuntimeDaemonLaunchPlan,
  healthcheckTimeoutMs: PositiveInt,
  healthcheckPollIntervalMs: PositiveInt,
  healthcheckRequestTimeoutMs: PositiveInt,
  shutdownTimeoutMs: PositiveInt,
  recovery: RuntimeDaemonRecoveryConfig,
});
export type RuntimeDaemonLauncherConfig = typeof RuntimeDaemonLauncherConfig.Type;

export const RuntimeDaemonLauncherInstallation = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_LAUNCHER_INSTALLATION_SCHEMA_VERSION),
  installationId: RuntimeSha256Digest,
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  versionDirectory: RuntimeVersionDirectory,
  nodeRelativePath: RuntimeArtifactRelativePath,
  nodeRuntimeFiles: Schema.Array(RuntimeArtifactFile),
  nodeByteSize: NonNegativeInt,
  nodeSha256: RuntimeSha256Digest,
  launcherScriptSha256: RuntimeSha256Digest,
  installedAt: IsoDateTime,
});
export type RuntimeDaemonLauncherInstallation = typeof RuntimeDaemonLauncherInstallation.Type;

export const RuntimeDaemonLock = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_LOCK_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  launcherPid: PositiveInt,
  ownershipId: RuntimeDaemonOwnershipId,
  createdAt: IsoDateTime,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
});
export type RuntimeDaemonLock = typeof RuntimeDaemonLock.Type;

export const RuntimeDaemonDiscovery = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_DISCOVERY_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  runtimeVersion: RuntimeVersion,
  buildHash: RuntimeBuildHash,
  ownershipId: RuntimeDaemonOwnershipId,
  launcherPid: PositiveInt,
  serverPid: PositiveInt,
  port: PortSchema,
  origin: Schema.String,
  startedAt: IsoDateTime,
  readyAt: IsoDateTime,
});
export type RuntimeDaemonDiscovery = typeof RuntimeDaemonDiscovery.Type;

export const RuntimeDaemonStatusState = Schema.Literals([
  "not-installed",
  "installed-not-loaded",
  "loaded-starting",
  "healthy",
  "recovering",
  "recovery-open",
  "recovery-corrupt",
  "unhealthy",
  "stale-corrupt",
  "installed-outdated",
]);
export type RuntimeDaemonStatusState = typeof RuntimeDaemonStatusState.Type;

export const RuntimeDaemonStatusDetail = Schema.Literals([
  "none",
  "launcher-missing",
  "launcher-invalid",
  "launch-agent-missing",
  "launch-agent-invalid",
  "lock-corrupt",
  "lock-stale",
  "discovery-corrupt",
  "discovery-stale",
  "health-failed",
  "recovery-backoff",
  "recovery-circuit-open",
  "recovery-state-corrupt",
  "recovery-state-stale",
  "current-runtime-changed",
]);
export type RuntimeDaemonStatusDetail = typeof RuntimeDaemonStatusDetail.Type;

export const RuntimeDaemonStatus = Schema.Struct({
  schemaVersion: Schema.Literal(RUNTIME_DAEMON_STATUS_SCHEMA_VERSION),
  profileId: RuntimeProfileId,
  state: RuntimeDaemonStatusState,
  detail: RuntimeDaemonStatusDetail,
  label: Schema.String,
  installed: Schema.Boolean,
  loaded: Schema.Boolean,
  current: Schema.Boolean,
  runtimeVersion: Schema.NullOr(RuntimeVersion),
  buildHash: Schema.NullOr(RuntimeBuildHash),
});
export type RuntimeDaemonStatus = typeof RuntimeDaemonStatus.Type;

export class RuntimeInvalidProfileIdError extends Schema.TaggedError<RuntimeInvalidProfileIdError>()(
  "RuntimeInvalidProfileIdError",
  { code: Schema.Literal("invalid-profile-id") },
) {}

export class RuntimeInvalidProfileConfigError extends Schema.TaggedError<RuntimeInvalidProfileConfigError>()(
  "RuntimeInvalidProfileConfigError",
  { code: Schema.Literal("invalid-profile-config") },
) {}

export class RuntimeProfileConfigConflictError extends Schema.TaggedError<RuntimeProfileConfigConflictError>()(
  "RuntimeProfileConfigConflictError",
  {
    code: Schema.Literal("profile-config-conflict"),
    profileId: RuntimeProfileId,
    conflictingFields: Schema.Array(
      Schema.Literals(["schemaVersion", "profileId", "port", "createdAt", "updatedAt"]),
    ),
  },
) {}

export class RuntimeProfileNotFoundError extends Schema.TaggedError<RuntimeProfileNotFoundError>()(
  "RuntimeProfileNotFoundError",
  {
    code: Schema.Literal("profile-not-found"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeProfileCorruptError extends Schema.TaggedError<RuntimeProfileCorruptError>()(
  "RuntimeProfileCorruptError",
  {
    code: Schema.Literal("profile-corrupt"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeProfilePathEscapeError extends Schema.TaggedError<RuntimeProfilePathEscapeError>()(
  "RuntimeProfilePathEscapeError",
  {
    code: Schema.Literal("profile-path-escape"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeArtifactManifestInvalidError extends Schema.TaggedError<RuntimeArtifactManifestInvalidError>()(
  "RuntimeArtifactManifestInvalidError",
  { code: Schema.Literal("artifact-manifest-invalid") },
) {}

export class RuntimeArtifactFileMissingError extends Schema.TaggedError<RuntimeArtifactFileMissingError>()(
  "RuntimeArtifactFileMissingError",
  {
    code: Schema.Literal("artifact-file-missing"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactSizeMismatchError extends Schema.TaggedError<RuntimeArtifactSizeMismatchError>()(
  "RuntimeArtifactSizeMismatchError",
  {
    code: Schema.Literal("artifact-size-mismatch"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactDigestMismatchError extends Schema.TaggedError<RuntimeArtifactDigestMismatchError>()(
  "RuntimeArtifactDigestMismatchError",
  {
    code: Schema.Literal("artifact-digest-mismatch"),
    file: RuntimeArtifactRelativePath,
  },
) {}

export class RuntimeArtifactPlatformMismatchError extends Schema.TaggedError<RuntimeArtifactPlatformMismatchError>()(
  "RuntimeArtifactPlatformMismatchError",
  {
    code: Schema.Literal("artifact-platform-mismatch"),
    expectedPlatform: RuntimeArtifactPlatform,
    expectedArchitecture: RuntimeArtifactArchitecture,
    actualPlatform: RuntimeArtifactPlatform,
    actualArchitecture: RuntimeArtifactArchitecture,
  },
) {}

export class RuntimeArtifactPathEscapeError extends Schema.TaggedError<RuntimeArtifactPathEscapeError>()(
  "RuntimeArtifactPathEscapeError",
  { code: Schema.Literal("artifact-path-escape") },
) {}

export class RuntimeArtifactInstallConflictError extends Schema.TaggedError<RuntimeArtifactInstallConflictError>()(
  "RuntimeArtifactInstallConflictError",
  {
    code: Schema.Literal("artifact-install-conflict"),
    profileId: RuntimeProfileId,
    versionDirectory: RuntimeVersionDirectory,
  },
) {}

export class RuntimeArtifactNotInstalledError extends Schema.TaggedError<RuntimeArtifactNotInstalledError>()(
  "RuntimeArtifactNotInstalledError",
  {
    code: Schema.Literal("artifact-not-installed"),
    profileId: RuntimeProfileId,
    versionDirectory: RuntimeVersionDirectory,
  },
) {}

export class RuntimeCurrentPointerCorruptError extends Schema.TaggedError<RuntimeCurrentPointerCorruptError>()(
  "RuntimeCurrentPointerCorruptError",
  {
    code: Schema.Literal("current-pointer-corrupt"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeCurrentPointerMissingError extends Schema.TaggedError<RuntimeCurrentPointerMissingError>()(
  "RuntimeCurrentPointerMissingError",
  {
    code: Schema.Literal("current-pointer-missing"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeArtifactFileTypeInvalidError extends Schema.TaggedError<RuntimeArtifactFileTypeInvalidError>()(
  "RuntimeArtifactFileTypeInvalidError",
  {
    code: Schema.Literal("artifact-file-type-invalid"),
    profileId: RuntimeProfileId,
    role: Schema.Literals(["node-executable", "server-entrypoint"]),
  },
) {}

export class RuntimeNodeNotExecutableError extends Schema.TaggedError<RuntimeNodeNotExecutableError>()(
  "RuntimeNodeNotExecutableError",
  {
    code: Schema.Literal("node-not-executable"),
    profileId: RuntimeProfileId,
  },
) {}

export class RuntimeProfileDirectoryInvalidError extends Schema.TaggedError<RuntimeProfileDirectoryInvalidError>()(
  "RuntimeProfileDirectoryInvalidError",
  {
    code: Schema.Literal("profile-directory-invalid"),
    profileId: RuntimeProfileId,
    directory: Schema.Literals(["state", "logs", "run"]),
  },
) {}

export class RuntimeHostUnsupportedError extends Schema.TaggedError<RuntimeHostUnsupportedError>()(
  "RuntimeHostUnsupportedError",
  { code: Schema.Literal("host-unsupported") },
) {}

export class RuntimeDaemonLifecycleError extends Schema.TaggedError<RuntimeDaemonLifecycleError>()(
  "RuntimeDaemonLifecycleError",
  {
    code: Schema.Literals([
      "unsupported-platform",
      "gui-domain-unavailable",
      "launcher-already-running",
      "launcher-invalid",
      "launcher-not-installed",
      "launcher-outdated",
      "unsafe-path",
      "launch-agent-corrupt",
      "launchctl-failed",
      "health-timeout",
      "state-corrupt",
      "recovery-circuit-open",
      "recovery-reset-not-allowed",
      "filesystem-error",
      "daemon-not-stopped",
    ]),
    profileId: RuntimeProfileId,
    operation: Schema.Literals([
      "install",
      "start",
      "stop",
      "status",
      "reset-recovery",
      "uninstall",
      "materialize-launcher",
    ]),
  },
) {}

export class RuntimeFilesystemError extends Schema.TaggedError<RuntimeFilesystemError>()(
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

export type RuntimeDaemonLaunchPlanError =
  | RuntimeArtifactError
  | RuntimeCurrentPointerMissingError
  | RuntimeArtifactFileTypeInvalidError
  | RuntimeNodeNotExecutableError
  | RuntimeProfileDirectoryInvalidError
  | RuntimeHostUnsupportedError;

export type RuntimeDaemonLifecycleOperation = RuntimeDaemonLifecycleError["operation"];
