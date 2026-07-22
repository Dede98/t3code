import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeBuildHash,
  RuntimeDaemonDiscovery,
  RuntimeDaemonLauncherConfig,
  RuntimeDaemonLauncherInstallation,
  RuntimeDaemonLaunchPlan,
  RuntimeDaemonLock,
  RuntimeProfileId,
  RuntimeVersion,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { makeRuntimeProfileLayout } from "@t3tools/shared/runtimeProfile";
import { assert, describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as MacOsRuntimeLaunchAgent from "./MacOsRuntimeLaunchAgent.ts";
import * as RuntimeDaemonLaunchPlanService from "./RuntimeDaemonLaunchPlan.ts";
import * as StandaloneRuntimeLauncher from "./StandaloneRuntimeLauncher.ts";

const decodePlan = Schema.decodeUnknownSync(RuntimeDaemonLaunchPlan);
const decodeConfig = Schema.decodeUnknownSync(RuntimeDaemonLauncherConfig);
const decodeInstallation = Schema.decodeUnknownSync(RuntimeDaemonLauncherInstallation);
const encodeLock = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonLock));
const encodeDiscovery = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonDiscovery));

interface HarnessControl {
  loaded: boolean;
  healthy: boolean;
  readonly alivePids: Set<number>;
  onBootstrap: Effect.Effect<void>;
  onBootout: Effect.Effect<void>;
}

interface Harness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly profileId: RuntimeProfileId;
  readonly profilesRoot: string;
  readonly launchAgentsDirectory: string;
  readonly plan: RuntimeDaemonLaunchPlan;
  readonly materialized: StandaloneRuntimeLauncher.MaterializedRuntimeLauncher;
  readonly commands: readonly (readonly string[])[];
  readonly control: HarnessControl;
  readonly service: MacOsRuntimeLaunchAgent.MacOsRuntimeLaunchAgent["Service"];
  setCurrentPlan(plan: RuntimeDaemonLaunchPlan): void;
}

const makePlan = (
  path: Path.Path,
  profilesRoot: string,
  profileId: RuntimeProfileId,
): RuntimeDaemonLaunchPlan => {
  const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
  const versionDirectory = "0.0.29-0123456789abcdef";
  const runtimeVersionDirectory = path.join(layout.versionsDirectory, versionDirectory);
  const nodeExecutablePath = path.join(runtimeVersionDirectory, "node", "bin", "node");
  const serverEntrypointPath = path.join(runtimeVersionDirectory, "apps", "server", "bin.mjs");
  return decodePlan({
    schemaVersion: 1,
    profileId,
    runtimeVersion: "0.0.29",
    buildHash: "0123456789abcdef",
    versionDirectory,
    nodeExecutablePath,
    serverEntrypointPath,
    argv: [serverEntrypointPath, "start", "--port", "4773"],
    cwd: layout.profileDirectory,
    environment: {
      T3CODE_MODE: "web",
      T3CODE_HOST: "127.0.0.1",
      T3CODE_PORT: "4773",
      T3CODE_HOME: layout.profileDirectory,
      T3CODE_STATE_DIR: layout.stateDirectory,
      T3CODE_LOGS_DIR: layout.logsDirectory,
      T3CODE_NO_BROWSER: "true",
      T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "false",
      T3CODE_TAILSCALE_SERVE: "false",
    },
    port: 4773,
    origin: "http://127.0.0.1:4773",
    profileDirectory: layout.profileDirectory,
    runtimeVersionDirectory,
    stateDirectory: layout.stateDirectory,
    logsDirectory: layout.logsDirectory,
    runDirectory: layout.runDirectory,
    daemonLockPath: layout.daemonLockPath,
    discoveryPath: layout.discoveryPath,
    preflight: {
      schemaVersion: 1,
      profileId,
      platform: "darwin",
      architecture: "arm64",
      ok: true,
      checks: [],
    },
  });
};

const makeHarness = Effect.fn("MacOsRuntimeLaunchAgentTest.makeHarness")(function* (options?: {
  readonly profileId?: RuntimeProfileId;
  readonly platform?: NodeJS.Platform;
  readonly startTimeoutMs?: number;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profileId = options?.profileId ?? RuntimeProfileId.make("dev");
  const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3 mac daemon profiles " });
  const launchAgentsDirectory = yield* fs.makeTempDirectoryScoped({
    prefix: "t3 fake LaunchAgents ",
  });
  const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
  for (const directory of [
    layout.profileDirectory,
    layout.runtimeDirectory,
    layout.launcherDirectory,
    layout.versionsDirectory,
    layout.stateDirectory,
    layout.logsDirectory,
    layout.runDirectory,
  ]) {
    yield* fs.makeDirectory(directory, { recursive: true });
  }
  const plan = makePlan(path, profilesRoot, profileId);
  yield* fs.makeDirectory(plan.runtimeVersionDirectory, { recursive: true });
  yield* fs.writeFileString(path.join(plan.runtimeVersionDirectory, "sentinel"), "runtime");
  const installationId = "a".repeat(64) as never;
  const installationDirectory = path.join(
    layout.launcherDirectory,
    "installations",
    installationId,
  );
  const materialized: StandaloneRuntimeLauncher.MaterializedRuntimeLauncher = {
    status: "installed",
    installation: decodeInstallation({
      schemaVersion: 1,
      installationId,
      profileId,
      runtimeVersion: plan.runtimeVersion,
      buildHash: plan.buildHash,
      versionDirectory: plan.versionDirectory,
      nodeRelativePath: "node-runtime/bin/node",
      nodeRuntimeFiles: [
        {
          path: "node-runtime/bin/node",
          byteSize: 42,
          sha256: "b".repeat(64),
        },
      ],
      nodeByteSize: 42,
      nodeSha256: "b".repeat(64),
      launcherScriptSha256: "c".repeat(64),
      installedAt: "2026-07-22T00:00:00.000Z",
    }),
    config: decodeConfig({
      schemaVersion: 1,
      profileId,
      runtimeVersion: plan.runtimeVersion,
      buildHash: plan.buildHash,
      versionDirectory: plan.versionDirectory,
      launchPlan: plan,
      healthcheckTimeoutMs: 100,
      healthcheckPollIntervalMs: 5,
      healthcheckRequestTimeoutMs: 10,
      shutdownTimeoutMs: 20,
    }),
    paths: {
      launcherDirectory: layout.launcherDirectory,
      installationsDirectory: path.dirname(installationDirectory),
      installationDirectory,
      installationPointerPath: path.join(layout.launcherDirectory, "installation.json"),
      nodeRuntimeDirectory: path.join(installationDirectory, "node-runtime"),
      nodePath: path.join(installationDirectory, "node-runtime", "bin", "node"),
      scriptPath: path.join(installationDirectory, "launcher.mjs"),
      configPath: path.join(installationDirectory, "launcher.json"),
    },
  };
  let currentPlan = plan;
  const planner = RuntimeDaemonLaunchPlanService.RuntimeDaemonLaunchPlanService.of({
    create: () => Effect.succeed(currentPlan),
  });
  const standalone = StandaloneRuntimeLauncher.StandaloneRuntimeLauncher.of({
    materialize: () => Effect.succeed(materialized),
    materializePlan: () => Effect.succeed(materialized),
    inspect: () => Effect.succeed(Option.some(materialized)),
  });
  const commands: (readonly string[])[] = [];
  const control: HarnessControl = {
    loaded: false,
    healthy: true,
    alivePids: new Set([111, 222]),
    onBootstrap: Effect.void,
    onBootout: Effect.void,
  };
  const runner = MacOsRuntimeLaunchAgent.RuntimeLaunchctlRunner.of({
    run: (arguments_) =>
      Effect.gen(function* () {
        commands.push(arguments_);
        if (arguments_[0] === "print" && arguments_[1] === "gui/501") {
          return { exitCode: 0 };
        }
        if (arguments_[0] === "print") return { exitCode: control.loaded ? 0 : 3 };
        if (arguments_[0] === "bootstrap") {
          control.loaded = true;
          yield* control.onBootstrap;
          return { exitCode: 0 };
        }
        if (arguments_[0] === "bootout") {
          control.loaded = false;
          yield* control.onBootout;
          return { exitCode: 0 };
        }
        return { exitCode: 1 };
      }),
  });
  const health = MacOsRuntimeLaunchAgent.RuntimeDaemonHealthProbe.of({
    check: () => Effect.sync(() => control.healthy),
  });
  const service = yield* MacOsRuntimeLaunchAgent.make({
    profilesRoot,
    homeDirectory: path.dirname(launchAgentsDirectory),
    launchAgentsDirectory,
    uid: 501,
    startTimeoutMs: options?.startTimeoutMs ?? 100,
    stopTimeoutMs: 100,
    pollIntervalMs: 5,
  }).pipe(
    Effect.provideService(HostProcessPlatform, options?.platform ?? "darwin"),
    Effect.provideService(MacOsRuntimeLaunchAgent.RuntimeLaunchctlRunner, runner),
    Effect.provideService(MacOsRuntimeLaunchAgent.RuntimeDaemonHealthProbe, health),
    Effect.provideService(StandaloneRuntimeLauncher.StandaloneRuntimeLauncher, standalone),
    Effect.provideService(RuntimeDaemonLaunchPlanService.RuntimeDaemonLaunchPlanService, planner),
    Effect.provideService(StandaloneRuntimeLauncher.RuntimeDaemonProcessProbe, (pid) =>
      control.alivePids.has(pid) ? "alive" : "dead",
    ),
  );
  return {
    fs,
    path,
    profileId,
    profilesRoot,
    launchAgentsDirectory,
    plan,
    materialized,
    commands,
    control,
    service,
    setCurrentPlan: (next) => {
      currentPlan = next;
    },
  } satisfies Harness;
});

const writeHealthyDocuments = Effect.fn("MacOsRuntimeLaunchAgentTest.writeHealthyDocuments")(
  function* (harness: Harness) {
    yield* harness.fs.writeFileString(
      harness.plan.daemonLockPath,
      encodeLock({
        schemaVersion: 1,
        profileId: harness.profileId,
        launcherPid: 111,
        ownershipId: "d".repeat(32) as never,
        createdAt: "2026-07-22T00:00:00.000Z",
        runtimeVersion: harness.plan.runtimeVersion,
        buildHash: harness.plan.buildHash,
      }),
    );
    yield* harness.fs.writeFileString(
      harness.plan.discoveryPath,
      encodeDiscovery({
        schemaVersion: 1,
        profileId: harness.profileId,
        runtimeVersion: harness.plan.runtimeVersion,
        buildHash: harness.plan.buildHash,
        ownershipId: "d".repeat(32) as never,
        launcherPid: 111,
        serverPid: 222,
        port: harness.plan.port,
        origin: harness.plan.origin,
        startedAt: "2026-07-22T00:00:00.000Z",
        readyAt: "2026-07-22T00:00:01.000Z",
      }),
    );
  },
);

describe("MacOsRuntimeLaunchAgent", () => {
  it("renders deterministic, escaped, profile-only plists without KeepAlive or secrets", () => {
    const profileDirectory = "/Users/test/T3 & Profiles/<custom>";
    const launcherDirectory = `${profileDirectory}/runtime/launcher/installations/abc`;
    const plist = MacOsRuntimeLaunchAgent.renderMacOsRuntimeLaunchAgentPlist({
      label: "com.t3tools.t3code.runtime.custom-mac-mini",
      profileDirectory,
      nodePath: `${launcherDirectory}/node`,
      scriptPath: `${launcherDirectory}/launcher.mjs`,
      configPath: `${launcherDirectory}/launcher.json`,
      stdoutPath: `${profileDirectory}/logs/daemon.stdout.log`,
      stderrPath: `${profileDirectory}/logs/daemon.stderr.log`,
      throttleIntervalSeconds: 10,
    });

    expect(plist).toMatchInlineSnapshot(`
      "<?xml version="1.0" encoding="UTF-8"?>
      <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
      <plist version="1.0">
      <dict>
        <key>Label</key>
          <string>com.t3tools.t3code.runtime.custom-mac-mini</string>
        <key>ProgramArguments</key>
        <array>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;/runtime/launcher/installations/abc/node</string>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;/runtime/launcher/installations/abc/launcher.mjs</string>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;/runtime/launcher/installations/abc/launcher.json</string>
        </array>
        <key>WorkingDirectory</key>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;</string>
        <key>RunAtLoad</key>
        <true/>
        <key>ProcessType</key>
          <string>Background</string>
        <key>StandardOutPath</key>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;/logs/daemon.stdout.log</string>
        <key>StandardErrorPath</key>
          <string>/Users/test/T3 &amp; Profiles/&lt;custom&gt;/logs/daemon.stderr.log</string>
        <key>ThrottleInterval</key>
        <integer>10</integer>
      </dict>
      </plist>
      "
    `);
    const builtInPlist = MacOsRuntimeLaunchAgent.renderMacOsRuntimeLaunchAgentPlist({
      label: "com.t3tools.t3code.runtime.dev",
      profileDirectory: "/Users/test/T3 Profiles/dev",
      nodePath:
        "/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/node-runtime/bin/node",
      scriptPath: "/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/launcher.mjs",
      configPath: "/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/launcher.json",
      stdoutPath: "/Users/test/T3 Profiles/dev/logs/daemon.stdout.log",
      stderrPath: "/Users/test/T3 Profiles/dev/logs/daemon.stderr.log",
      throttleIntervalSeconds: 10,
    });
    expect(builtInPlist).toMatchInlineSnapshot(`
      "<?xml version="1.0" encoding="UTF-8"?>
      <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
      <plist version="1.0">
      <dict>
        <key>Label</key>
          <string>com.t3tools.t3code.runtime.dev</string>
        <key>ProgramArguments</key>
        <array>
          <string>/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/node-runtime/bin/node</string>
          <string>/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/launcher.mjs</string>
          <string>/Users/test/T3 Profiles/dev/runtime/launcher/installations/abc/launcher.json</string>
        </array>
        <key>WorkingDirectory</key>
          <string>/Users/test/T3 Profiles/dev</string>
        <key>RunAtLoad</key>
        <true/>
        <key>ProcessType</key>
          <string>Background</string>
        <key>StandardOutPath</key>
          <string>/Users/test/T3 Profiles/dev/logs/daemon.stdout.log</string>
        <key>StandardErrorPath</key>
          <string>/Users/test/T3 Profiles/dev/logs/daemon.stderr.log</string>
        <key>ThrottleInterval</key>
        <integer>10</integer>
      </dict>
      </plist>
      "
    `);
    assert.equal(
      MacOsRuntimeLaunchAgent.macOsRuntimeLaunchAgentLabel(RuntimeProfileId.make("dev")),
      "com.t3tools.t3code.runtime.dev",
    );
    assert.equal(
      MacOsRuntimeLaunchAgent.macOsRuntimeLaunchAgentLabel(
        RuntimeProfileId.make("custom:mac-mini"),
      ),
      "com.t3tools.t3code.runtime.custom-mac-mini",
    );
    assert.notInclude(plist, "KeepAlive");
    assert.notMatch(plist, /(?:secret|token|credential)/iu);
  });

  it.effect("runs an idempotent install, start, stop, status, and uninstall lifecycle", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        harness.control.onBootstrap = writeHealthyDocuments(harness).pipe(Effect.orDie);
        harness.control.onBootout = Effect.all(
          [
            harness.fs.remove(harness.plan.daemonLockPath, { force: true }),
            harness.fs.remove(harness.plan.discoveryPath, { force: true }),
          ],
          { discard: true },
        ).pipe(Effect.orDie);

        assert.equal(
          (yield* harness.service.install(harness.profileId)).status.state,
          "installed-not-loaded",
        );
        assert.equal(
          (yield* harness.service.install(harness.profileId)).status.state,
          "installed-not-loaded",
        );
        const started = yield* harness.service.start(harness.profileId);
        assert.equal(started.state, "healthy");
        assert.equal((yield* harness.service.install(harness.profileId)).status.state, "healthy");
        assert.equal((yield* harness.service.start(harness.profileId)).state, "healthy");
        assert.equal(
          (yield* harness.service.stop(harness.profileId)).state,
          "installed-not-loaded",
        );
        assert.equal(
          (yield* harness.service.stop(harness.profileId)).state,
          "installed-not-loaded",
        );
        assert.equal((yield* harness.service.uninstall(harness.profileId)).state, "not-installed");
        assert.equal((yield* harness.service.uninstall(harness.profileId)).state, "not-installed");

        const bootstrapCalls = harness.commands.filter((command) => command[0] === "bootstrap");
        const bootoutCalls = harness.commands.filter((command) => command[0] === "bootout");
        assert.lengthOf(bootstrapCalls, 1);
        assert.lengthOf(bootoutCalls, 1);
        assert.deepEqual(bootoutCalls[0], ["bootout", "gui/501/com.t3tools.t3code.runtime.dev"]);
        assert.isTrue(yield* harness.fs.exists(harness.plan.runtimeVersionDirectory));
        assert.isTrue(yield* harness.fs.exists(harness.plan.logsDirectory));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("distinguishes loaded-starting, health timeout, and stale discovery", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ startTimeoutMs: 20 });
        yield* harness.service.install(harness.profileId);
        harness.control.loaded = true;
        assert.equal((yield* harness.service.status(harness.profileId)).state, "loaded-starting");

        const timeout = yield* harness.service.start(harness.profileId).pipe(Effect.flip);
        assert.equal(timeout._tag, "RuntimeDaemonLifecycleError");
        if (timeout._tag === "RuntimeDaemonLifecycleError") {
          assert.equal(timeout.code, "health-timeout");
        }

        yield* writeHealthyDocuments(harness);
        harness.control.healthy = false;
        const unhealthy = yield* harness.service.status(harness.profileId);
        assert.equal(unhealthy.state, "unhealthy");
        assert.equal(unhealthy.detail, "health-failed");
        harness.control.alivePids.delete(222);
        const stale = yield* harness.service.status(harness.profileId);
        assert.equal(stale.state, "stale-corrupt");
        assert.equal(stale.detail, "discovery-stale");
        yield* harness.fs.writeFileString(harness.plan.discoveryPath, "{corrupt\n");
        const corrupt = yield* harness.service.status(harness.profileId);
        assert.equal(corrupt.state, "stale-corrupt");
        assert.equal(corrupt.detail, "discovery-corrupt");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("reports an installed launcher as outdated after the current runtime changes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        yield* harness.service.install(harness.profileId);
        harness.setCurrentPlan({
          ...harness.plan,
          runtimeVersion: RuntimeVersion.make("0.0.30"),
          buildHash: RuntimeBuildHash.make("abcdef0123456789"),
        });

        const status = yield* harness.service.status(harness.profileId);
        assert.equal(status.state, "installed-outdated");
        assert.equal(status.detail, "current-runtime-changed");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed on unsupported platforms without invoking launchctl", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ platform: "linux" });
        const error = yield* harness.service.status(harness.profileId).pipe(Effect.flip);
        assert.equal(error.code, "unsupported-platform");
        assert.isEmpty(harness.commands);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
