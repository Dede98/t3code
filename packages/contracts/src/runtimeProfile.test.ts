import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";

import {
  RuntimeArtifactManifest,
  RuntimeDaemonDiscovery,
  RuntimeDaemonLaunchPlan,
  RuntimeDaemonLauncherInstallation,
  RuntimeDaemonLifecycleError,
  RuntimeDaemonLock,
  RuntimeDaemonStatus,
  RuntimeNodeNotExecutableError,
  RuntimeProfileConfig,
  RuntimeProfileId,
} from "./runtimeProfile.ts";

const decodeProfileId = Schema.decodeUnknownSync(RuntimeProfileId);
const decodeProfileConfig = Schema.decodeUnknownSync(RuntimeProfileConfig);
const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);
const decodeLaunchPlan = Schema.decodeUnknownSync(RuntimeDaemonLaunchPlan);
const decodeLauncherInstallation = Schema.decodeUnknownSync(RuntimeDaemonLauncherInstallation);
const decodeLock = Schema.decodeUnknownSync(RuntimeDaemonLock);
const decodeDiscovery = Schema.decodeUnknownSync(RuntimeDaemonDiscovery);
const decodeStatus = Schema.decodeUnknownSync(RuntimeDaemonStatus);

const validManifest = {
  schemaVersion: 1,
  runtimeVersion: "0.0.29",
  buildHash: "0123456789abcdef",
  platform: "darwin",
  architecture: "arm64",
  entrypoint: "apps/server/dist/bin.mjs",
  nodeExecutable: "node/bin/node",
  files: [
    {
      path: "apps/server/dist/bin.mjs",
      byteSize: 4,
      sha256: "0".repeat(64),
    },
    {
      path: "node/bin/node",
      byteSize: 4,
      sha256: "1".repeat(64),
    },
  ],
};

describe("runtime profile contracts", () => {
  it.each(["dev", "alpha", "nightly", "custom:mac-mini"])("accepts profile id %s", (profileId) => {
    expect(decodeProfileId(profileId)).toBe(profileId);
  });

  it.each([
    "stable",
    "custom:",
    "custom:Mac-mini",
    "custom:-mac-mini",
    "custom:mac_mini",
    "custom:mac mini",
    "custom:../nightly",
    "custom:mac/mini",
    "custom:mac\\mini",
    `custom:${"a".repeat(64)}`,
  ])("rejects invalid or unsafe profile id %s", (profileId) => {
    expect(() => decodeProfileId(profileId)).toThrow();
  });

  it.each([1, 3773, 65_535])("accepts TCP port %s", (port) => {
    expect(
      decodeProfileConfig({
        schemaVersion: 1,
        profileId: "dev",
        port,
        createdAt: "2026-07-22T00:00:00.000Z",
        updatedAt: "2026-07-22T00:00:00.000Z",
      }).port,
    ).toBe(port);
  });

  it.each([0, -1, 65_536, 3773.5, "3773"])("rejects invalid TCP port %s", (port) => {
    expect(() =>
      decodeProfileConfig({
        schemaVersion: 1,
        profileId: "dev",
        port,
        createdAt: "2026-07-22T00:00:00.000Z",
        updatedAt: "2026-07-22T00:00:00.000Z",
      }),
    ).toThrow();
  });

  it("accepts a complete safe artifact manifest", () => {
    expect(decodeManifest(validManifest).entrypoint).toBe("apps/server/dist/bin.mjs");
  });

  it.each(["/apps/server/dist/bin.mjs", "../bin.mjs", "apps/../bin.mjs", "C:/bin.mjs"])(
    "rejects unsafe manifest path %s",
    (entrypoint) => {
      expect(() => decodeManifest({ ...validManifest, entrypoint })).toThrow();
    },
  );

  it("accepts the versioned, non-secret daemon launch-plan shape", () => {
    const profileDirectory = "/tmp/t3 profiles/dev";
    const runtimeVersionDirectory = `${profileDirectory}/runtime/versions/0.0.29-0123456789abcdef`;
    const stateDirectory = `${profileDirectory}/state`;
    const logsDirectory = `${profileDirectory}/logs`;
    const plan = decodeLaunchPlan({
      schemaVersion: 1,
      profileId: "dev",
      runtimeVersion: "0.0.29",
      buildHash: "0123456789abcdef",
      versionDirectory: "0.0.29-0123456789abcdef",
      nodeExecutablePath: `${runtimeVersionDirectory}/node/bin/node`,
      serverEntrypointPath: `${runtimeVersionDirectory}/apps/server/dist/bin.mjs`,
      argv: [
        `${runtimeVersionDirectory}/apps/server/dist/bin.mjs`,
        "start",
        "--state-dir",
        stateDirectory,
        "--logs-dir",
        logsDirectory,
      ],
      cwd: profileDirectory,
      environment: {
        T3CODE_MODE: "web",
        T3CODE_HOST: "127.0.0.1",
        T3CODE_PORT: "3773",
        T3CODE_HOME: profileDirectory,
        T3CODE_STATE_DIR: stateDirectory,
        T3CODE_LOGS_DIR: logsDirectory,
        T3CODE_NO_BROWSER: "true",
        T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
        T3CODE_TAILSCALE_SERVE: "false",
      },
      port: 3773,
      origin: "http://127.0.0.1:3773",
      profileDirectory,
      runtimeVersionDirectory,
      stateDirectory,
      logsDirectory,
      runDirectory: `${profileDirectory}/run`,
      daemonLockPath: `${profileDirectory}/run/daemon.lock`,
      discoveryPath: `${profileDirectory}/run/discovery.json`,
      preflight: {
        schemaVersion: 1,
        profileId: "dev",
        platform: "darwin",
        architecture: "arm64",
        ok: true,
        checks: [
          { check: "profile-config", status: "ready" },
          { check: "runtime-current", status: "ready" },
          { check: "runtime-artifact", status: "ready" },
          { check: "node-executable", status: "ready" },
        ],
      },
    });

    expect(plan.argv).toContain(stateDirectory);
    expect(JSON.stringify(plan)).not.toMatch(/(?:secret|token|credential)/iu);
  });

  it("uses a typed code for non-executable runtime Node files", () => {
    const error = new RuntimeNodeNotExecutableError({
      code: "node-not-executable",
      profileId: RuntimeProfileId.make("dev"),
    });
    expect(error.code).toBe("node-not-executable");
    expect(JSON.stringify(error)).not.toContain("/tmp");
  });

  it("validates versioned launcher, lock, discovery, and status documents", () => {
    const installation = decodeLauncherInstallation({
      schemaVersion: 1,
      installationId: "2".repeat(64),
      profileId: "custom:mac-mini",
      runtimeVersion: "0.0.29",
      buildHash: "0123456789abcdef",
      versionDirectory: "0.0.29-0123456789abcdef",
      nodeRelativePath: "node-runtime/bin/node",
      nodeRuntimeFiles: [
        {
          path: "node-runtime/bin/node",
          byteSize: 42,
          sha256: "0".repeat(64),
        },
      ],
      nodeByteSize: 42,
      nodeSha256: "0".repeat(64),
      launcherScriptSha256: "1".repeat(64),
      installedAt: "2026-07-22T00:00:00.000Z",
    });
    const lock = decodeLock({
      schemaVersion: 1,
      profileId: installation.profileId,
      launcherPid: 101,
      ownershipId: "a".repeat(32),
      createdAt: "2026-07-22T00:00:01.000Z",
      runtimeVersion: installation.runtimeVersion,
      buildHash: installation.buildHash,
    });
    const discovery = decodeDiscovery({
      schemaVersion: 1,
      profileId: installation.profileId,
      runtimeVersion: installation.runtimeVersion,
      buildHash: installation.buildHash,
      ownershipId: lock.ownershipId,
      launcherPid: lock.launcherPid,
      serverPid: 102,
      port: 4773,
      origin: "http://127.0.0.1:4773",
      startedAt: "2026-07-22T00:00:01.000Z",
      readyAt: "2026-07-22T00:00:02.000Z",
    });
    const status = decodeStatus({
      schemaVersion: 1,
      profileId: installation.profileId,
      state: "healthy",
      detail: "none",
      label: "com.t3tools.t3code.runtime.custom-mac-mini",
      installed: true,
      loaded: true,
      current: true,
      runtimeVersion: installation.runtimeVersion,
      buildHash: installation.buildHash,
    });

    expect(discovery.ownershipId).toBe(lock.ownershipId);
    expect(status.state).toBe("healthy");
    expect(JSON.stringify({ installation, lock, discovery, status })).not.toMatch(
      /(?:secret|token|credential)/iu,
    );
  });

  it("rejects corrupt ownership and exposes only safe lifecycle error fields", () => {
    expect(() =>
      decodeLock({
        schemaVersion: 1,
        profileId: "dev",
        launcherPid: 1,
        ownershipId: "not-an-owner",
        createdAt: "2026-07-22T00:00:00.000Z",
        runtimeVersion: "0.0.29",
        buildHash: "0123456789abcdef",
      }),
    ).toThrow();

    const error = new RuntimeDaemonLifecycleError({
      code: "health-timeout",
      profileId: RuntimeProfileId.make("dev"),
      operation: "start",
    });
    expect(error).toEqual(expect.objectContaining({ code: "health-timeout", operation: "start" }));
    expect(JSON.stringify(error)).not.toMatch(/(?:stdout|stderr|secret|token)/iu);
  });
});
