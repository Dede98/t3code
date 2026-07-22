import {
  RUNTIME_DAEMON_STATUS_SCHEMA_VERSION,
  RuntimeDaemonDiscovery,
  RuntimeDaemonLifecycleError,
  RuntimeDaemonLock,
  type RuntimeDaemonLaunchPlan,
  type RuntimeDaemonLaunchPlanError,
  type RuntimeDaemonStatus,
  type RuntimeDaemonStatusDetail,
  type RuntimeProfileId,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import {
  isPathWithin,
  makeRuntimeProfileLayout,
  runtimeProfileDirectoryName,
} from "@t3tools/shared/runtimeProfile";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import type * as PlatformError from "effect/PlatformError";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as NodeOS from "node:os";

import { RuntimeDaemonLaunchPlanService } from "./RuntimeDaemonLaunchPlan.ts";
import {
  type MaterializedRuntimeLauncher,
  RuntimeDaemonProcessProbe,
  StandaloneRuntimeLauncher,
} from "./StandaloneRuntimeLauncher.ts";

const DEFAULT_START_TIMEOUT_MS = 30_000;
const DEFAULT_STOP_TIMEOUT_MS = 10_000;
const DEFAULT_POLL_INTERVAL_MS = 100;
const DEFAULT_THROTTLE_INTERVAL_SECONDS = 10;

/** Recovery/restart policy follows in the next slice; this plist intentionally has no KeepAlive. */
export const MACOS_RUNTIME_AUTOMATIC_RECOVERY_ENABLED = false;

export interface RuntimeLaunchctlResult {
  readonly exitCode: number;
}

export class RuntimeLaunchctlRunnerError extends Schema.TaggedErrorClass<RuntimeLaunchctlRunnerError>()(
  "RuntimeLaunchctlRunnerError",
  {},
) {}

export class RuntimeLaunchctlRunner extends Context.Service<
  RuntimeLaunchctlRunner,
  {
    readonly run: (
      arguments_: readonly string[],
    ) => Effect.Effect<RuntimeLaunchctlResult, RuntimeLaunchctlRunnerError>;
  }
>()("@t3tools/desktop/runtime/MacOsRuntimeLaunchAgent/RuntimeLaunchctlRunner") {}

export const liveRuntimeLaunchctlRunnerLayer = Layer.effect(
  RuntimeLaunchctlRunner,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const run: RuntimeLaunchctlRunner["Service"]["run"] = (arguments_) =>
      Effect.scoped(
        Effect.gen(function* () {
          const child = yield* spawner.spawn(
            ChildProcess.make("/bin/launchctl", [...arguments_], {
              shell: false,
              stdin: "ignore",
              stdout: "ignore",
              stderr: "ignore",
            }),
          );
          return { exitCode: Number(yield* child.exitCode) };
        }).pipe(Effect.mapError(() => new RuntimeLaunchctlRunnerError())),
      );
    return RuntimeLaunchctlRunner.of({ run });
  }),
);

export interface RuntimeDaemonHealthProbeInput {
  readonly plan: RuntimeDaemonLaunchPlan;
  readonly discovery: RuntimeDaemonDiscovery;
  readonly requestTimeoutMs: number;
}

export class RuntimeDaemonHealthProbe extends Context.Service<
  RuntimeDaemonHealthProbe,
  {
    readonly check: (input: RuntimeDaemonHealthProbeInput) => Effect.Effect<boolean>;
  }
>()("@t3tools/desktop/runtime/MacOsRuntimeLaunchAgent/RuntimeDaemonHealthProbe") {}

const ServerRuntimeState = Schema.Struct({
  version: Schema.Literal(1),
  pid: Schema.Int,
  host: Schema.optional(Schema.String),
  port: Schema.Int,
  origin: Schema.String,
  startedAt: Schema.String,
});
const decodeServerRuntimeState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(ServerRuntimeState),
);

export const liveRuntimeDaemonHealthProbeLayer = Layer.effect(
  RuntimeDaemonHealthProbe,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const client = yield* HttpClient.HttpClient;
    const check = Effect.fn("RuntimeDaemonHealthProbe.check")(function* (
      input: RuntimeDaemonHealthProbeInput,
    ) {
      const raw = yield* Effect.result(
        fs.readFileString(`${input.plan.stateDirectory}/server-runtime.json`),
      );
      if (Result.isFailure(raw)) return false;
      const decoded = yield* Effect.result(decodeServerRuntimeState(raw.success));
      if (
        Result.isFailure(decoded) ||
        decoded.success.pid !== input.discovery.serverPid ||
        decoded.success.port !== input.discovery.port ||
        decoded.success.origin !== input.discovery.origin ||
        !Number.isFinite(Date.parse(decoded.success.startedAt)) ||
        Date.parse(decoded.success.startedAt) < Date.parse(input.discovery.startedAt) - 1_000
      ) {
        return false;
      }
      const response = yield* Effect.result(
        client
          .get(`${input.discovery.origin}/.well-known/t3/environment`)
          .pipe(
            Effect.flatMap(HttpClientResponse.filterStatusOk),
            Effect.timeout(input.requestTimeoutMs),
          ),
      );
      return Result.isSuccess(response);
    });
    return RuntimeDaemonHealthProbe.of({ check });
  }),
);

export interface MacOsRuntimeLaunchAgentOptions {
  readonly profilesRoot: string;
  readonly homeDirectory?: string;
  readonly uid?: number;
  readonly launchAgentsDirectory?: string;
  readonly startTimeoutMs?: number;
  readonly stopTimeoutMs?: number;
  readonly pollIntervalMs?: number;
  readonly throttleIntervalSeconds?: number;
}

export interface MacOsRuntimeLaunchAgentInstallResult {
  readonly status: RuntimeDaemonStatus;
  readonly launcher: MaterializedRuntimeLauncher;
}

export type MacOsRuntimeLaunchAgentError =
  | RuntimeDaemonLifecycleError
  | RuntimeDaemonLaunchPlanError;

export class MacOsRuntimeLaunchAgent extends Context.Service<
  MacOsRuntimeLaunchAgent,
  {
    readonly install: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<MacOsRuntimeLaunchAgentInstallResult, MacOsRuntimeLaunchAgentError>;
    readonly start: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<RuntimeDaemonStatus, MacOsRuntimeLaunchAgentError>;
    readonly stop: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<RuntimeDaemonStatus, RuntimeDaemonLifecycleError>;
    readonly status: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<RuntimeDaemonStatus, RuntimeDaemonLifecycleError>;
    readonly uninstall: (
      profileId: RuntimeProfileId,
    ) => Effect.Effect<RuntimeDaemonStatus, RuntimeDaemonLifecycleError>;
  }
>()("@t3tools/desktop/runtime/MacOsRuntimeLaunchAgent") {}

export interface RenderMacOsRuntimeLaunchAgentPlistInput {
  readonly label: string;
  readonly profileDirectory: string;
  readonly nodePath: string;
  readonly scriptPath: string;
  readonly configPath: string;
  readonly stdoutPath: string;
  readonly stderrPath: string;
  readonly throttleIntervalSeconds: number;
}

const escapeXml = (value: string): string =>
  value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");

const plistString = (value: string): string => `    <string>${escapeXml(value)}</string>`;

export function renderMacOsRuntimeLaunchAgentPlist(
  input: RenderMacOsRuntimeLaunchAgentPlistInput,
): string {
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">',
    '<plist version="1.0">',
    "<dict>",
    "  <key>Label</key>",
    plistString(input.label),
    "  <key>ProgramArguments</key>",
    "  <array>",
    plistString(input.nodePath),
    plistString(input.scriptPath),
    plistString(input.configPath),
    "  </array>",
    "  <key>WorkingDirectory</key>",
    plistString(input.profileDirectory),
    "  <key>RunAtLoad</key>",
    "  <true/>",
    "  <key>ProcessType</key>",
    plistString("Background"),
    "  <key>StandardOutPath</key>",
    plistString(input.stdoutPath),
    "  <key>StandardErrorPath</key>",
    plistString(input.stderrPath),
    "  <key>ThrottleInterval</key>",
    `  <integer>${input.throttleIntervalSeconds}</integer>`,
    "</dict>",
    "</plist>",
    "",
  ].join("\n");
}

export function macOsRuntimeLaunchAgentLabel(profileId: RuntimeProfileId): string {
  return `com.t3tools.t3code.runtime.${runtimeProfileDirectoryName(profileId)}`;
}

const decodeLockJson = Schema.decodeUnknownEffect(Schema.fromJsonString(RuntimeDaemonLock));
const decodeDiscoveryJson = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RuntimeDaemonDiscovery),
);

const isNotFound = (error: PlatformError.PlatformError): boolean =>
  error.reason._tag === "NotFound";

export const make = Effect.fn("MacOsRuntimeLaunchAgent.make")(function* (
  options: MacOsRuntimeLaunchAgentOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const platform = yield* HostProcessPlatform;
  const runner = yield* RuntimeLaunchctlRunner;
  const launcher = yield* StandaloneRuntimeLauncher;
  const planner = yield* RuntimeDaemonLaunchPlanService;
  const probePid = yield* RuntimeDaemonProcessProbe;
  const healthProbe = yield* RuntimeDaemonHealthProbe;
  const profilesRoot = path.resolve(options.profilesRoot);
  const homeDirectory = path.resolve(options.homeDirectory ?? NodeOS.homedir());
  const launchAgentsDirectory = path.resolve(
    options.launchAgentsDirectory ?? path.join(homeDirectory, "Library", "LaunchAgents"),
  );
  const uid = options.uid ?? process.getuid?.();
  const startTimeoutMs = options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS;
  const stopTimeoutMs = options.stopTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const throttleIntervalSeconds =
    options.throttleIntervalSeconds ?? DEFAULT_THROTTLE_INTERVAL_SECONDS;

  const errorFor = (
    profileId: RuntimeProfileId,
    operation: RuntimeDaemonLifecycleError["operation"],
    code: RuntimeDaemonLifecycleError["code"],
  ) => new RuntimeDaemonLifecycleError({ code, profileId, operation });

  const mapFilesystemError =
    (profileId: RuntimeProfileId, operation: RuntimeDaemonLifecycleError["operation"]) =>
    <A, R>(
      effect: Effect.Effect<A, PlatformError.PlatformError, R>,
    ): Effect.Effect<A, RuntimeDaemonLifecycleError, R> =>
      effect.pipe(Effect.mapError(() => errorFor(profileId, operation, "filesystem-error")));

  const runLaunchctl = Effect.fn("MacOsRuntimeLaunchAgent.runLaunchctl")(function* (
    profileId: RuntimeProfileId,
    operation: RuntimeDaemonLifecycleError["operation"],
    arguments_: readonly string[],
  ) {
    return yield* runner
      .run(arguments_)
      .pipe(Effect.mapError(() => errorFor(profileId, operation, "launchctl-failed")));
  });

  const ensureDomain = Effect.fn("MacOsRuntimeLaunchAgent.ensureDomain")(function* (
    profileId: RuntimeProfileId,
    operation: RuntimeDaemonLifecycleError["operation"],
  ) {
    if (platform !== "darwin") {
      return yield* errorFor(profileId, operation, "unsupported-platform");
    }
    if (uid === undefined || !Number.isSafeInteger(uid) || uid < 0) {
      return yield* errorFor(profileId, operation, "gui-domain-unavailable");
    }
    const domain = `gui/${uid}`;
    const result = yield* runLaunchctl(profileId, operation, ["print", domain]);
    if (result.exitCode !== 0) {
      return yield* errorFor(profileId, operation, "gui-domain-unavailable");
    }
    return domain;
  });

  const pathsFor = (profileId: RuntimeProfileId) => {
    const layout = makeRuntimeProfileLayout(path, profilesRoot, profileId);
    const label = macOsRuntimeLaunchAgentLabel(profileId);
    return {
      layout,
      label,
      plistPath: path.join(launchAgentsDirectory, `${label}.plist`),
      stdoutPath: path.join(layout.logsDirectory, "runtime-daemon.stdout.log"),
      stderrPath: path.join(layout.logsDirectory, "runtime-daemon.stderr.log"),
    };
  };

  const readOptionalString = Effect.fn("MacOsRuntimeLaunchAgent.readOptionalString")(function* (
    profileId: RuntimeProfileId,
    operation: RuntimeDaemonLifecycleError["operation"],
    filePath: string,
  ) {
    return yield* fs.readFileString(filePath).pipe(
      Effect.map(Option.some),
      Effect.catch((cause) =>
        isNotFound(cause)
          ? Effect.succeed(Option.none<string>())
          : Effect.fail(errorFor(profileId, operation, "filesystem-error")),
      ),
    );
  });

  const assertRegularFileWithin = Effect.fn("MacOsRuntimeLaunchAgent.assertRegularFileWithin")(
    function* (
      profileId: RuntimeProfileId,
      operation: RuntimeDaemonLifecycleError["operation"],
      filePath: string,
      root: string,
    ) {
      const info = yield* mapFilesystemError(profileId, operation)(fs.stat(filePath));
      if (info.type !== "File") {
        return yield* errorFor(profileId, operation, "unsafe-path");
      }
      const realRoot = yield* mapFilesystemError(profileId, operation)(fs.realPath(root));
      const realFile = yield* mapFilesystemError(profileId, operation)(fs.realPath(filePath));
      const relativeFile = path.relative(path.resolve(root), path.resolve(filePath));
      if (
        !isPathWithin(path, realRoot, realFile) ||
        path.normalize(path.resolve(realRoot, relativeFile)) !== path.normalize(realFile)
      ) {
        return yield* errorFor(profileId, operation, "unsafe-path");
      }
    },
  );

  const assertLaunchAgentsDirectory = Effect.fn(
    "MacOsRuntimeLaunchAgent.assertLaunchAgentsDirectory",
  )(function* (profileId: RuntimeProfileId) {
    yield* mapFilesystemError(
      profileId,
      "install",
    )(fs.makeDirectory(launchAgentsDirectory, { recursive: true, mode: 0o700 }));
    const info = yield* mapFilesystemError(profileId, "install")(fs.stat(launchAgentsDirectory));
    if (info.type !== "Directory") {
      return yield* errorFor(profileId, "install", "unsafe-path");
    }
    const realDirectory = yield* mapFilesystemError(
      profileId,
      "install",
    )(fs.realPath(launchAgentsDirectory));
    const realParent = yield* mapFilesystemError(
      profileId,
      "install",
    )(fs.realPath(path.dirname(launchAgentsDirectory)));
    if (
      path.normalize(realDirectory) !==
      path.normalize(path.join(realParent, path.basename(launchAgentsDirectory)))
    ) {
      return yield* errorFor(profileId, "install", "unsafe-path");
    }
  });

  const writeAtomically = Effect.fn("MacOsRuntimeLaunchAgent.writeAtomically")(function* (
    profileId: RuntimeProfileId,
    filePath: string,
    contents: string,
  ) {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const temporaryDirectory = yield* mapFilesystemError(
          profileId,
          "install",
        )(
          fs.makeTempDirectoryScoped({
            directory: launchAgentsDirectory,
            prefix: `.${path.basename(filePath)}.`,
          }),
        );
        const temporaryPath = path.join(temporaryDirectory, "agent.plist");
        yield* mapFilesystemError(
          profileId,
          "install",
        )(fs.writeFileString(temporaryPath, contents));
        yield* mapFilesystemError(profileId, "install")(fs.chmod(temporaryPath, 0o600));
        yield* mapFilesystemError(profileId, "install")(fs.rename(temporaryPath, filePath));
      }),
    );
  });

  const renderPlist = (materialized: MaterializedRuntimeLauncher): string => {
    const profileId = materialized.installation.profileId;
    const { layout, label, stdoutPath, stderrPath } = pathsFor(profileId);
    return renderMacOsRuntimeLaunchAgentPlist({
      label,
      profileDirectory: layout.profileDirectory,
      nodePath: materialized.paths.nodePath,
      scriptPath: materialized.paths.scriptPath,
      configPath: materialized.paths.configPath,
      stdoutPath,
      stderrPath,
      throttleIntervalSeconds,
    });
  };

  const isLoaded = Effect.fn("MacOsRuntimeLaunchAgent.isLoaded")(function* (
    profileId: RuntimeProfileId,
    operation: RuntimeDaemonLifecycleError["operation"],
    domain: string,
  ) {
    const { label } = pathsFor(profileId);
    const result = yield* runLaunchctl(profileId, operation, ["print", `${domain}/${label}`]);
    return result.exitCode === 0;
  });

  const statusValue = (
    profileId: RuntimeProfileId,
    state: RuntimeDaemonStatus["state"],
    detail: RuntimeDaemonStatusDetail,
    loaded: boolean,
    materialized?: MaterializedRuntimeLauncher,
    current = false,
  ): RuntimeDaemonStatus => ({
    schemaVersion: RUNTIME_DAEMON_STATUS_SCHEMA_VERSION,
    profileId,
    state,
    detail,
    label: macOsRuntimeLaunchAgentLabel(profileId),
    installed: materialized !== undefined && state !== "not-installed",
    loaded,
    current,
    runtimeVersion: materialized?.installation.runtimeVersion ?? null,
    buildHash: materialized?.installation.buildHash ?? null,
  });

  const readDecodedDocument = Effect.fn("MacOsRuntimeLaunchAgent.readDecodedDocument")(function* <
    A,
  >(
    profileId: RuntimeProfileId,
    filePath: string,
    decode: (input: string) => Effect.Effect<A, Schema.SchemaError>,
  ) {
    const raw = yield* readOptionalString(profileId, "status", filePath);
    if (Option.isNone(raw)) return Option.none<A>();
    const safeFile = yield* Effect.result(
      assertRegularFileWithin(profileId, "status", filePath, path.dirname(filePath)),
    );
    if (Result.isFailure(safeFile)) return "corrupt" as const;
    const decoded = yield* Effect.result(decode(raw.value));
    return Result.isSuccess(decoded) ? Option.some(decoded.success) : "corrupt";
  });

  const plansEqual = (left: RuntimeDaemonLaunchPlan, right: RuntimeDaemonLaunchPlan): boolean =>
    JSON.stringify(left) === JSON.stringify(right);

  const statusInternal = Effect.fn("MacOsRuntimeLaunchAgent.statusInternal")(function* (
    profileId: RuntimeProfileId,
    domain: string,
  ): Effect.fn.Return<RuntimeDaemonStatus, RuntimeDaemonLifecycleError> {
    const { layout, plistPath } = pathsFor(profileId);
    const inspected = yield* Effect.result(launcher.inspect(profileId));
    const plist = yield* readOptionalString(profileId, "status", plistPath);
    const loaded = yield* isLoaded(profileId, "status", domain);

    if (Result.isFailure(inspected)) {
      return statusValue(profileId, "stale-corrupt", "launcher-invalid", loaded);
    }
    if (Option.isNone(inspected.success)) {
      return Option.isNone(plist)
        ? statusValue(profileId, "not-installed", "launcher-missing", loaded)
        : statusValue(profileId, "stale-corrupt", "launcher-missing", loaded);
    }
    const materialized = inspected.success.value;
    if (Option.isNone(plist)) {
      return statusValue(profileId, "not-installed", "launch-agent-missing", loaded, materialized);
    }
    const safePlist = yield* Effect.result(
      assertRegularFileWithin(profileId, "status", plistPath, launchAgentsDirectory),
    );
    if (Result.isFailure(safePlist)) {
      return statusValue(profileId, "stale-corrupt", "launch-agent-invalid", loaded, materialized);
    }
    const expectedPlist = renderPlist(materialized);
    if (plist.value !== expectedPlist) {
      return statusValue(profileId, "stale-corrupt", "launch-agent-invalid", loaded, materialized);
    }

    const currentPlanResult = yield* Effect.result(planner.create(profileId));
    const current =
      Result.isSuccess(currentPlanResult) &&
      plansEqual(currentPlanResult.success, materialized.config.launchPlan);
    if (!current) {
      return statusValue(
        profileId,
        "installed-outdated",
        "current-runtime-changed",
        loaded,
        materialized,
      );
    }

    const lock = yield* readDecodedDocument(profileId, layout.daemonLockPath, decodeLockJson);
    if (lock === "corrupt") {
      return statusValue(profileId, "stale-corrupt", "lock-corrupt", loaded, materialized, true);
    }
    const discovery = yield* readDecodedDocument(
      profileId,
      layout.discoveryPath,
      decodeDiscoveryJson,
    );
    if (discovery === "corrupt") {
      return statusValue(
        profileId,
        "stale-corrupt",
        "discovery-corrupt",
        loaded,
        materialized,
        true,
      );
    }
    if (Option.isNone(discovery)) {
      if (Option.isSome(lock)) {
        if (
          lock.value.profileId !== profileId ||
          lock.value.runtimeVersion !== materialized.installation.runtimeVersion ||
          lock.value.buildHash !== materialized.installation.buildHash
        ) {
          return statusValue(profileId, "stale-corrupt", "lock-stale", loaded, materialized, true);
        }
        const pidState = probePid(lock.value.launcherPid);
        if (pidState === "dead" || !loaded) {
          return statusValue(profileId, "stale-corrupt", "lock-stale", loaded, materialized, true);
        }
      }
      return statusValue(
        profileId,
        loaded ? "loaded-starting" : "installed-not-loaded",
        "none",
        loaded,
        materialized,
        true,
      );
    }
    if (
      Option.isNone(lock) ||
      lock.value.profileId !== profileId ||
      lock.value.runtimeVersion !== materialized.installation.runtimeVersion ||
      lock.value.buildHash !== materialized.installation.buildHash ||
      discovery.value.profileId !== profileId ||
      discovery.value.runtimeVersion !== materialized.installation.runtimeVersion ||
      discovery.value.buildHash !== materialized.installation.buildHash ||
      discovery.value.ownershipId !== lock.value.ownershipId ||
      discovery.value.launcherPid !== lock.value.launcherPid ||
      discovery.value.port !== materialized.config.launchPlan.port ||
      discovery.value.origin !== materialized.config.launchPlan.origin
    ) {
      return statusValue(profileId, "stale-corrupt", "discovery-stale", loaded, materialized, true);
    }
    if (
      probePid(discovery.value.launcherPid) !== "alive" ||
      probePid(discovery.value.serverPid) !== "alive" ||
      !loaded
    ) {
      return statusValue(profileId, "stale-corrupt", "discovery-stale", loaded, materialized, true);
    }
    const healthy = yield* healthProbe.check({
      plan: materialized.config.launchPlan,
      discovery: discovery.value,
      requestTimeoutMs: materialized.config.healthcheckRequestTimeoutMs,
    });
    return statusValue(
      profileId,
      healthy ? "healthy" : "unhealthy",
      healthy ? "none" : "health-failed",
      loaded,
      materialized,
      true,
    );
  });

  const status = Effect.fn("MacOsRuntimeLaunchAgent.status")(function* (
    profileId: RuntimeProfileId,
  ) {
    const domain = yield* ensureDomain(profileId, "status");
    return yield* statusInternal(profileId, domain);
  });

  const install = Effect.fn("MacOsRuntimeLaunchAgent.install")(function* (
    profileId: RuntimeProfileId,
  ): Effect.fn.Return<MacOsRuntimeLaunchAgentInstallResult, MacOsRuntimeLaunchAgentError> {
    const domain = yield* ensureDomain(profileId, "install");
    yield* assertLaunchAgentsDirectory(profileId);
    const plan = yield* planner.create(profileId);
    if (yield* isLoaded(profileId, "install", domain)) {
      const runningLauncher = yield* Effect.result(launcher.inspect(profileId));
      const runningStatus = yield* statusInternal(profileId, domain);
      if (
        Result.isSuccess(runningLauncher) &&
        Option.isSome(runningLauncher.success) &&
        plansEqual(plan, runningLauncher.success.value.config.launchPlan) &&
        (runningStatus.state === "loaded-starting" ||
          runningStatus.state === "healthy" ||
          runningStatus.state === "unhealthy")
      ) {
        return { status: runningStatus, launcher: runningLauncher.success.value };
      }
      return yield* errorFor(profileId, "install", "launcher-already-running");
    }
    const materialized = yield* launcher.materializePlan(plan);
    const { plistPath } = pathsFor(profileId);
    const desiredPlist = renderPlist(materialized);
    const existing = yield* readOptionalString(profileId, "install", plistPath);
    if (Option.isSome(existing)) {
      yield* assertRegularFileWithin(profileId, "install", plistPath, launchAgentsDirectory);
      if (existing.value !== desiredPlist && (yield* isLoaded(profileId, "install", domain))) {
        return yield* errorFor(profileId, "install", "launcher-already-running");
      }
    }
    if (Option.isNone(existing) || existing.value !== desiredPlist) {
      yield* writeAtomically(profileId, plistPath, desiredPlist);
    }
    return { status: yield* statusInternal(profileId, domain), launcher: materialized };
  });

  const waitForHealthy = Effect.fn("MacOsRuntimeLaunchAgent.waitForHealthy")(function* (
    profileId: RuntimeProfileId,
    domain: string,
  ) {
    const deadline = (yield* Clock.currentTimeMillis) + startTimeoutMs;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      const current = yield* statusInternal(profileId, domain);
      if (current.state === "healthy") return current;
      if (
        current.state === "stale-corrupt" ||
        current.state === "installed-outdated" ||
        current.state === "not-installed"
      ) {
        return yield* errorFor(profileId, "start", "state-corrupt");
      }
      yield* Effect.sleep(pollIntervalMs);
    }
    return yield* errorFor(profileId, "start", "health-timeout");
  });

  const start = Effect.fn("MacOsRuntimeLaunchAgent.start")(function* (
    profileId: RuntimeProfileId,
  ): Effect.fn.Return<RuntimeDaemonStatus, MacOsRuntimeLaunchAgentError> {
    const domain = yield* ensureDomain(profileId, "start");
    const inspected = yield* launcher.inspect(profileId);
    if (Option.isNone(inspected)) {
      return yield* errorFor(profileId, "start", "launcher-not-installed");
    }
    const plan = yield* planner.create(profileId);
    if (!plansEqual(plan, inspected.value.config.launchPlan)) {
      return yield* errorFor(profileId, "start", "launcher-outdated");
    }
    const { label, plistPath } = pathsFor(profileId);
    const plist = yield* readOptionalString(profileId, "start", plistPath);
    if (Option.isNone(plist)) {
      return yield* errorFor(profileId, "start", "launcher-not-installed");
    }
    yield* assertRegularFileWithin(profileId, "start", plistPath, launchAgentsDirectory);
    if (plist.value !== renderPlist(inspected.value)) {
      return yield* errorFor(profileId, "start", "launch-agent-corrupt");
    }
    const before = yield* statusInternal(profileId, domain);
    if (before.state === "healthy") return before;
    if (before.state === "installed-outdated" || before.state === "stale-corrupt") {
      return yield* errorFor(profileId, "start", "state-corrupt");
    }
    if (!before.loaded) {
      const bootstrapped = yield* runLaunchctl(profileId, "start", [
        "bootstrap",
        domain,
        plistPath,
      ]);
      if (bootstrapped.exitCode !== 0 && !(yield* isLoaded(profileId, "start", domain))) {
        return yield* errorFor(profileId, "start", "launchctl-failed");
      }
    } else {
      const kicked = yield* runLaunchctl(profileId, "start", ["kickstart", `${domain}/${label}`]);
      if (kicked.exitCode !== 0 && !(yield* isLoaded(profileId, "start", domain))) {
        return yield* errorFor(profileId, "start", "launchctl-failed");
      }
    }
    return yield* waitForHealthy(profileId, domain);
  });

  const stopFilesSettled = Effect.fn("MacOsRuntimeLaunchAgent.stopFilesSettled")(function* (
    profileId: RuntimeProfileId,
  ) {
    const { layout } = pathsFor(profileId);
    const lock = yield* readDecodedDocument(profileId, layout.daemonLockPath, decodeLockJson);
    const discovery = yield* readDecodedDocument(
      profileId,
      layout.discoveryPath,
      decodeDiscoveryJson,
    );
    if (lock === "corrupt" || discovery === "corrupt") return false;
    const lockSettled = Option.isNone(lock) || probePid(lock.value.launcherPid) === "dead";
    const discoverySettled =
      Option.isNone(discovery) ||
      (probePid(discovery.value.launcherPid) === "dead" &&
        probePid(discovery.value.serverPid) === "dead");
    return lockSettled && discoverySettled;
  });

  const stopInternal = Effect.fn("MacOsRuntimeLaunchAgent.stopInternal")(function* (
    profileId: RuntimeProfileId,
    domain: string,
    operation: "stop" | "uninstall",
  ) {
    const { label } = pathsFor(profileId);
    if (yield* isLoaded(profileId, operation, domain)) {
      const result = yield* runLaunchctl(profileId, operation, ["bootout", `${domain}/${label}`]);
      if (result.exitCode !== 0 && (yield* isLoaded(profileId, operation, domain))) {
        return yield* errorFor(profileId, operation, "launchctl-failed");
      }
    }
    const deadline = (yield* Clock.currentTimeMillis) + stopTimeoutMs;
    while ((yield* Clock.currentTimeMillis) < deadline) {
      if (
        !(yield* isLoaded(profileId, operation, domain)) &&
        (yield* stopFilesSettled(profileId))
      ) {
        return;
      }
      yield* Effect.sleep(pollIntervalMs);
    }
    return yield* errorFor(profileId, operation, "daemon-not-stopped");
  });

  const stop = Effect.fn("MacOsRuntimeLaunchAgent.stop")(function* (profileId: RuntimeProfileId) {
    const domain = yield* ensureDomain(profileId, "stop");
    yield* stopInternal(profileId, domain, "stop");
    return yield* statusInternal(profileId, domain);
  });

  const assertGeneratedLauncherTreeSafe = Effect.fn(
    "MacOsRuntimeLaunchAgent.assertGeneratedLauncherTreeSafe",
  )(function* (profileId: RuntimeProfileId, launcherDirectory: string) {
    const exists = yield* mapFilesystemError(profileId, "uninstall")(fs.exists(launcherDirectory));
    if (!exists) return;
    const rootInfo = yield* mapFilesystemError(profileId, "uninstall")(fs.stat(launcherDirectory));
    if (rootInfo.type !== "Directory") {
      return yield* errorFor(profileId, "uninstall", "unsafe-path");
    }
    const entries = yield* mapFilesystemError(
      profileId,
      "uninstall",
    )(fs.readDirectory(launcherDirectory, { recursive: true }));
    const realLauncherDirectory = yield* mapFilesystemError(
      profileId,
      "uninstall",
    )(fs.realPath(launcherDirectory));
    for (const entry of entries) {
      const entryPath = path.join(launcherDirectory, entry);
      if (!isPathWithin(path, launcherDirectory, entryPath)) {
        return yield* errorFor(profileId, "uninstall", "unsafe-path");
      }
      const realEntry = yield* mapFilesystemError(profileId, "uninstall")(fs.realPath(entryPath));
      const relativeEntry = path.relative(path.resolve(launcherDirectory), path.resolve(entryPath));
      if (
        !isPathWithin(path, realLauncherDirectory, realEntry) ||
        path.normalize(path.resolve(realLauncherDirectory, relativeEntry)) !==
          path.normalize(realEntry)
      ) {
        return yield* errorFor(profileId, "uninstall", "unsafe-path");
      }
    }
  });

  const uninstall = Effect.fn("MacOsRuntimeLaunchAgent.uninstall")(function* (
    profileId: RuntimeProfileId,
  ) {
    const domain = yield* ensureDomain(profileId, "uninstall");
    yield* stopInternal(profileId, domain, "uninstall");
    const { layout, plistPath } = pathsFor(profileId);
    const plistExists = yield* mapFilesystemError(profileId, "uninstall")(fs.exists(plistPath));
    if (plistExists) {
      yield* assertRegularFileWithin(profileId, "uninstall", plistPath, launchAgentsDirectory);
      yield* mapFilesystemError(profileId, "uninstall")(fs.remove(plistPath));
    }
    yield* assertGeneratedLauncherTreeSafe(profileId, layout.launcherDirectory);
    yield* mapFilesystemError(
      profileId,
      "uninstall",
    )(fs.remove(path.join(layout.launcherDirectory, "installation.json"), { force: true }));
    yield* mapFilesystemError(
      profileId,
      "uninstall",
    )(
      fs.remove(path.join(layout.launcherDirectory, "installations"), {
        recursive: true,
        force: true,
      }),
    );
    return statusValue(profileId, "not-installed", "launcher-missing", false);
  });

  return MacOsRuntimeLaunchAgent.of({ install, start, stop, status, uninstall });
});

export const layer = (options: MacOsRuntimeLaunchAgentOptions) =>
  Layer.effect(MacOsRuntimeLaunchAgent, make(options));
