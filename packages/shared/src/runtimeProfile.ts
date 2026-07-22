import type {
  RuntimeArtifactManifest,
  RuntimeArtifactRelativePath,
  RuntimeBuildHash,
  RuntimeProfileId,
  RuntimeProfileKind,
  RuntimeVersion,
  RuntimeVersionDirectory,
} from "@t3tools/contracts/runtimeProfile";
import {
  RuntimeProfileId as RuntimeProfileIdSchema,
  RuntimeVersionDirectory as RuntimeVersionDirectorySchema,
} from "@t3tools/contracts/runtimeProfile";
import type * as Path from "effect/Path";
import * as Schema from "effect/Schema";

const CREDENTIAL_SERVICE_PREFIX = "com.t3tools.t3code.runtime-profile";
const isRuntimeProfileId = Schema.is(RuntimeProfileIdSchema);

export interface RuntimeProfileLayout {
  readonly profilesRoot: string;
  readonly profileId: RuntimeProfileId;
  readonly profileDirectoryName: string;
  readonly profileDirectory: string;
  readonly profileConfigPath: string;
  readonly runtimeDirectory: string;
  readonly currentRuntimePath: string;
  readonly launcherDirectory: string;
  readonly versionsDirectory: string;
  readonly stateDirectory: string;
  readonly logsDirectory: string;
  readonly runDirectory: string;
  readonly daemonLockPath: string;
  readonly discoveryPath: string;
  readonly recoveryPath: string;
  readonly credentialServiceName: string;
}

export function runtimeProfileKind(profileId: RuntimeProfileId): RuntimeProfileKind {
  if (profileId === "dev") return "dev";
  if (profileId === "alpha") return "alpha";
  if (profileId === "nightly") return "nightly";
  return "custom";
}

export function runtimeProfileDirectoryName(profileId: RuntimeProfileId): string {
  return profileId.startsWith("custom:")
    ? `custom-${profileId.slice("custom:".length)}`
    : profileId;
}

export function runtimeProfileIdFromDirectoryName(
  directoryName: string,
): RuntimeProfileId | undefined {
  const candidate =
    directoryName === "dev" || directoryName === "alpha" || directoryName === "nightly"
      ? directoryName
      : directoryName.startsWith("custom-")
        ? `custom:${directoryName.slice("custom-".length)}`
        : undefined;

  return candidate !== undefined && isRuntimeProfileId(candidate)
    ? (candidate as RuntimeProfileId)
    : undefined;
}

export function runtimeCredentialServiceName(profileId: RuntimeProfileId): string {
  return `${CREDENTIAL_SERVICE_PREFIX}.${runtimeProfileDirectoryName(profileId)}`;
}

export function runtimeVersionDirectoryName(input: {
  readonly runtimeVersion: RuntimeVersion;
  readonly buildHash: RuntimeBuildHash;
}): RuntimeVersionDirectory {
  return RuntimeVersionDirectorySchema.make(`${input.runtimeVersion}-${input.buildHash}`);
}

export function isPathWithin(path: Path.Path, root: string, candidate: string): boolean {
  const resolvedRoot = path.resolve(root);
  const resolvedCandidate = path.resolve(candidate);
  const relative = path.relative(resolvedRoot, resolvedCandidate);
  return (
    relative === "" ||
    (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative))
  );
}

export function isSafeRuntimeArtifactRelativePath(value: string): boolean {
  if (
    value.length === 0 ||
    value.length > 1_024 ||
    value.startsWith("/") ||
    value.includes("\\") ||
    value.includes(":") ||
    value.includes("\0")
  ) {
    return false;
  }

  const segments = value.split("/");
  return segments.every((segment) => segment.length > 0 && segment !== "." && segment !== "..");
}

export function resolveRuntimeArtifactPath(
  path: Path.Path,
  artifactRoot: string,
  relativePath: string,
): string | undefined {
  if (!isSafeRuntimeArtifactRelativePath(relativePath)) {
    return undefined;
  }
  const candidate = path.resolve(artifactRoot, ...relativePath.split("/"));
  return isPathWithin(path, artifactRoot, candidate) ? candidate : undefined;
}

export function makeRuntimeProfileLayout(
  path: Path.Path,
  profilesRoot: string,
  profileId: RuntimeProfileId,
): RuntimeProfileLayout {
  const resolvedProfilesRoot = path.resolve(profilesRoot);
  const profileDirectoryName = runtimeProfileDirectoryName(profileId);
  const profileDirectory = path.join(resolvedProfilesRoot, profileDirectoryName);
  const runtimeDirectory = path.join(profileDirectory, "runtime");
  const versionsDirectory = path.join(runtimeDirectory, "versions");
  const runDirectory = path.join(profileDirectory, "run");

  return {
    profilesRoot: resolvedProfilesRoot,
    profileId,
    profileDirectoryName,
    profileDirectory,
    profileConfigPath: path.join(profileDirectory, "profile.json"),
    runtimeDirectory,
    currentRuntimePath: path.join(runtimeDirectory, "current.json"),
    launcherDirectory: path.join(runtimeDirectory, "launcher"),
    versionsDirectory,
    stateDirectory: path.join(profileDirectory, "state"),
    logsDirectory: path.join(profileDirectory, "logs"),
    runDirectory,
    daemonLockPath: path.join(runDirectory, "daemon.lock"),
    discoveryPath: path.join(runDirectory, "discovery.json"),
    recoveryPath: path.join(runDirectory, "recovery.json"),
    credentialServiceName: runtimeCredentialServiceName(profileId),
  };
}

export function runtimeArtifactVersionDirectory(
  path: Path.Path,
  layout: RuntimeProfileLayout,
  manifest: Pick<RuntimeArtifactManifest, "runtimeVersion" | "buildHash">,
): string {
  return path.join(layout.versionsDirectory, runtimeVersionDirectoryName(manifest));
}

export function normalizeRuntimeArtifactRelativePath(
  path: Path.Path,
  value: string,
): RuntimeArtifactRelativePath | undefined {
  const normalized = path.sep === "/" ? value : value.split(path.sep).join("/");
  if (!isSafeRuntimeArtifactRelativePath(normalized)) {
    return undefined;
  }
  return normalized as RuntimeArtifactRelativePath;
}
