import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeArtifactManifest,
  type RuntimeArtifactManifest as RuntimeArtifactManifestValue,
  RuntimeDaemonDiscovery,
  RuntimeDaemonLock,
  RuntimeDaemonRecoveryState,
  RuntimeProfileId,
} from "@t3tools/contracts/runtimeProfile";
import { HostProcessArchitecture, HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { assert, describe, it } from "@effect/vitest";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as Clock from "effect/Clock";
import * as NodeNet from "node:net";

import * as RuntimeArtifactInstaller from "./RuntimeArtifactInstaller.ts";
import * as RuntimeDaemonLaunchPlan from "./RuntimeDaemonLaunchPlan.ts";
import * as RuntimeProfileStore from "./RuntimeProfileStore.ts";
import * as StandaloneRuntimeLauncher from "./StandaloneRuntimeLauncher.ts";

const PROFILE_ID = RuntimeProfileId.make("dev");
const ENTRYPOINT = "server/fake-server.mjs" as const;
const NODE_EXECUTABLE = "node/bin/node" as const;
const CREATED_AT = "2026-07-22T00:00:00.000Z" as const;

const decodeManifest = Schema.decodeUnknownSync(RuntimeArtifactManifest);
const decodeDiscoveryJson = Schema.decodeUnknownSync(Schema.fromJsonString(RuntimeDaemonDiscovery));
const encodeManifest = Schema.encodeSync(Schema.fromJsonString(RuntimeArtifactManifest));
const encodeLock = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonLock));
const encodeDiscovery = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonDiscovery));
const decodeRecoveryJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(RuntimeDaemonRecoveryState),
);
const encodeRecovery = Schema.encodeSync(Schema.fromJsonString(RuntimeDaemonRecoveryState));
const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const FakeSpawnRecordSchema = Schema.Struct({
  pid: Schema.Int,
  at: Schema.Number,
  attempt: Schema.Int,
  mode: Schema.String,
});
const decodeFakeSpawnRecord = Schema.decodeUnknownSync(
  Schema.fromJsonString(FakeSpawnRecordSchema),
);

const FAKE_SERVER_SOURCE = String.raw`import * as fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';

const argument = (name) => process.argv[process.argv.indexOf(name) + 1];
const port = Number(argument('--port'));
const stateDirectory = argument('--state-dir');
const mode = await fs.readFile(path.join(stateDirectory, 'fake-mode'), 'utf8').catch(() => 'healthy');
const spawnLogPath = path.join(stateDirectory, 'spawn-log.ndjson');
const previousSpawns = await fs.readFile(spawnLogPath, 'utf8').catch(() => '');
const attempt = previousSpawns.trim() === '' ? 1 : previousSpawns.trim().split('\n').length + 1;
await fs.appendFile(spawnLogPath, JSON.stringify({ pid: process.pid, at: Date.now(), attempt, mode }) + '\n');
if (mode === 'always-crash' || (mode === 'crash-once' && attempt === 1)) process.exit(0);
const origin = mode === 'wrong-origin' ? 'http://127.0.0.1:1' : 'http://127.0.0.1:' + String(port);
const statePort = mode === 'wrong-port' ? port + 1 : port;
const statePid = mode === 'wrong-pid' ? process.pid + 1 : process.pid;
if (mode !== 'stale-runtime') {
  await fs.writeFile(path.join(stateDirectory, 'server-runtime.json'), JSON.stringify({
    version: 1,
    pid: statePid,
    host: '127.0.0.1',
    port: statePort,
    origin,
    startedAt: new Date().toISOString(),
  }) + '\n');
}

let healthRequest = 0;
const server = http.createServer((request, response) => {
  if (request.url === '/.well-known/t3/environment') {
    healthRequest += 1;
    fs.appendFile(
      path.join(stateDirectory, 'health-log.ndjson'),
      JSON.stringify({ pid: process.pid, at: Date.now(), request: healthRequest }) + '\n',
    ).catch(() => {});
    fs.readFile(path.join(stateDirectory, 'health-script'), 'utf8').then(
      (raw) => JSON.parse(raw),
      () => ['healthy'],
    ).then((script) => {
      const behavior = script[Math.min(healthRequest - 1, script.length - 1)] ?? 'healthy';
      if (behavior === 'hang') return;
      if (behavior === 'fail') {
        response.writeHead(503);
        response.end();
        return;
      }
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end('{}');
    });
    return;
  }
  response.writeHead(404);
  response.end();
});
let keepAlive;
if (mode !== 'no-health' && mode !== 'startup-timeout') {
  if (mode === 'delayed-health') await new Promise((resolve) => setTimeout(resolve, 200));
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
} else {
  keepAlive = setInterval(() => {}, 1000);
}
if (mode === 'crash-after-ready-once' && attempt === 1) {
  setTimeout(() => process.exit(0), 100);
}
const shutdown = () => {
  if (keepAlive !== undefined) clearInterval(keepAlive);
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 20);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
`;

interface IntegrationHarness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly sourceDirectory: string;
  readonly materialized: StandaloneRuntimeLauncher.MaterializedRuntimeLauncher;
}

class IntegrationTestError extends Schema.TaggedError<IntegrationTestError>()(
  "IntegrationTestError",
  {},
) {}

const findAvailablePort = () =>
  Effect.tryPromise({
    try: () =>
      new Promise<number>((resolve, reject) => {
        const server = NodeNet.createServer();
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
          const address = server.address();
          if (address === null || typeof address === "string") {
            server.close();
            reject(new Error("missing TCP address"));
            return;
          }
          server.close((error) => (error ? reject(error) : resolve(address.port)));
        });
      }),
    catch: () => new IntegrationTestError(),
  });

const makeHarness = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.makeHarness")(
  function* (options?: {
    readonly maxRestarts?: number;
    readonly initialBackoffMs?: number;
    readonly maxBackoffMs?: number;
    readonly healthcheckTimeoutMs?: number;
    readonly healthcheckIntervalMs?: number;
    readonly consecutiveHealthFailuresBeforeRestart?: number;
    readonly healthyResetAfterMs?: number;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const hostPlatform = yield* HostProcessPlatform;
    const hostArchitecture = yield* HostProcessArchitecture;
    const profilesRoot = yield* fs.makeTempDirectoryScoped({ prefix: "t3 launcher integration " });
    const sourceDirectory = yield* fs.makeTempDirectory({ prefix: "t3 deleted source checkout " });
    yield* Effect.addFinalizer(() =>
      fs.remove(sourceDirectory, { recursive: true, force: true }).pipe(Effect.orDie),
    );
    const store = yield* RuntimeProfileStore.make({ profilesRoot });
    const installer = yield* RuntimeArtifactInstaller.make({ profilesRoot }).pipe(
      Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
    );
    const platform = hostPlatform === "linux" ? "linux" : "darwin";
    const architecture = hostArchitecture === "x64" ? "x64" : "arm64";
    const port = yield* findAvailablePort();
    yield* store.ensureProfile({
      schemaVersion: 1,
      profileId: PROFILE_ID,
      port,
      createdAt: CREATED_AT,
      updatedAt: CREATED_AT,
    });

    const nodePath = path.join(sourceDirectory, ...NODE_EXECUTABLE.split("/"));
    const serverPath = path.join(sourceDirectory, ...ENTRYPOINT.split("/"));
    yield* fs.makeDirectory(path.dirname(nodePath), { recursive: true });
    yield* fs.makeDirectory(path.dirname(serverPath), { recursive: true });
    yield* fs.copyFile(process.execPath, nodePath);
    yield* fs.chmod(nodePath, 0o755);
    yield* fs.writeFileString(serverPath, FAKE_SERVER_SOURCE);
    const artifactFiles: Array<readonly [string, string]> = [
      [NODE_EXECUTABLE, nodePath],
      [ENTRYPOINT, serverPath],
    ];
    if (platform === "darwin") {
      const nodeLibraryDirectory = path.resolve(path.dirname(process.execPath), "..", "lib");
      const nodeLibraryName = (yield* fs.readDirectory(nodeLibraryDirectory)).find(
        (name) => name.startsWith("libnode.") && name.endsWith(".dylib"),
      );
      if (nodeLibraryName === undefined) {
        return yield* Effect.die(new Error("Homebrew Node libnode dylib not found"));
      }
      const targetLibraryPath = path.join(sourceDirectory, "node", "lib", nodeLibraryName);
      yield* fs.makeDirectory(path.dirname(targetLibraryPath), { recursive: true });
      yield* fs.copyFile(path.join(nodeLibraryDirectory, nodeLibraryName), targetLibraryPath);
      artifactFiles.push([`node/lib/${nodeLibraryName}`, targetLibraryPath]);
    }
    const manifestFiles: Array<RuntimeArtifactManifestValue["files"][number]> = [];
    for (const [relativePath, filePath] of artifactFiles) {
      const bytes = yield* fs.readFile(filePath);
      const digest = yield* crypto.digest("SHA-256", bytes);
      manifestFiles.push({
        path: relativePath as never,
        byteSize: bytes.byteLength,
        sha256: Encoding.encodeHex(digest) as never,
      });
    }
    const manifest = decodeManifest({
      schemaVersion: 1,
      runtimeVersion: "0.0.29",
      buildHash: "0123456789abcdef",
      platform,
      architecture,
      entrypoint: ENTRYPOINT,
      nodeExecutable: NODE_EXECUTABLE,
      files: manifestFiles,
    });
    yield* fs.writeFileString(
      path.join(sourceDirectory, "manifest.json"),
      encodeManifest(manifest),
    );
    yield* installer.installArtifact({
      profileId: PROFILE_ID,
      sourceDirectory,
      targetPlatform: platform,
      targetArchitecture: architecture,
    });
    yield* installer.activateArtifact({
      profileId: PROFILE_ID,
      runtimeVersion: manifest.runtimeVersion,
      buildHash: manifest.buildHash,
      activatedAt: CREATED_AT,
    });
    const planner = yield* RuntimeDaemonLaunchPlan.make({ profilesRoot }).pipe(
      Effect.provideService(RuntimeProfileStore.RuntimeProfileStore, store),
      Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
      Effect.provideService(HostProcessPlatform, platform),
      Effect.provideService(HostProcessArchitecture, architecture),
    );
    const launcher = yield* StandaloneRuntimeLauncher.make({
      profilesRoot,
      healthcheckTimeoutMs: options?.healthcheckTimeoutMs ?? 250,
      healthcheckPollIntervalMs: 10,
      healthcheckRequestTimeoutMs: 30,
      shutdownTimeoutMs: 100,
      recovery: {
        maxRestarts: options?.maxRestarts ?? 2,
        slidingWindowMs: 2_000,
        initialBackoffMs: options?.initialBackoffMs ?? 30,
        maxBackoffMs: options?.maxBackoffMs ?? 120,
        healthcheckIntervalMs: options?.healthcheckIntervalMs ?? 30,
        consecutiveHealthFailuresBeforeRestart:
          options?.consecutiveHealthFailuresBeforeRestart ?? 3,
        healthyResetAfterMs: options?.healthyResetAfterMs ?? 150,
      },
    }).pipe(
      Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
      Effect.provideService(RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService, planner),
    );
    const materialized = yield* launcher.materialize(PROFILE_ID);
    yield* fs.remove(sourceDirectory, { recursive: true });
    return { fs, path, sourceDirectory, materialized } satisfies IntegrationHarness;
  },
);

const spawnLauncher = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.spawnLauncher")(
  function* (harness: IntegrationHarness) {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    return yield* spawner.spawn(
      ChildProcess.make(
        harness.materialized.paths.nodePath,
        [harness.materialized.paths.scriptPath, harness.materialized.paths.configPath],
        { stdin: "ignore", stdout: "ignore", stderr: "ignore" },
      ),
    );
  },
);

const waitForExit = (child: ChildProcessSpawner.ChildProcessHandle, timeoutMs = 2_000) =>
  child.exitCode.pipe(Effect.map(Number), Effect.timeout(timeoutMs));

const waitForFile = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.waitForFile")(function* (
  fs: FileSystem.FileSystem,
  filePath: string,
  expected: boolean,
  child?: ChildProcessSpawner.ChildProcessHandle,
) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if ((yield* fs.exists(filePath)) === expected) return;
    if (child !== undefined) {
      const exited = yield* child.exitCode.pipe(Effect.timeoutOption(0));
      if (Option.isSome(exited)) {
        return yield* Effect.die(
          new Error(`launcher exited before file state: code=${String(exited.value)}`),
        );
      }
    }
    yield* Effect.sleep(20);
  }
  return yield* Effect.die(new Error(`file state timeout: ${filePath}`));
});

const readSpawnRecords = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.readSpawnRecords")(
  function* (harness: IntegrationHarness) {
    const raw = yield* harness.fs
      .readFileString(
        harness.path.join(
          harness.materialized.config.launchPlan.stateDirectory,
          "spawn-log.ndjson",
        ),
      )
      .pipe(Effect.orElseSucceed(() => ""));
    return raw
      .trim()
      .split("\n")
      .filter((line) => line.length > 0)
      .map((line) => decodeFakeSpawnRecord(line));
  },
);

const waitForSpawnCount = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.waitForSpawnCount")(
  function* (harness: IntegrationHarness, expected: number) {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const records = yield* readSpawnRecords(harness);
      if (records.length >= expected) return records;
      yield* Effect.sleep(10);
    }
    return yield* Effect.die(new Error(`spawn count timeout: ${String(expected)}`));
  },
);

const waitForHealthRequestCount = Effect.fn(
  "StandaloneRuntimeLauncherIntegrationTest.waitForHealthRequestCount",
)(function* (harness: IntegrationHarness, expected: number) {
  const logPath = harness.path.join(
    harness.materialized.config.launchPlan.stateDirectory,
    "health-log.ndjson",
  );
  for (let attempt = 0; attempt < 300; attempt += 1) {
    const raw = yield* harness.fs.readFileString(logPath).pipe(Effect.orElseSucceed(() => ""));
    const count = raw.trim() === "" ? 0 : raw.trim().split("\n").length;
    if (count >= expected) return;
    yield* Effect.sleep(10);
  }
  return yield* Effect.die(new Error(`health request count timeout: ${String(expected)}`));
});

const waitForRecovery = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.waitForRecovery")(
  function* (
    harness: IntegrationHarness,
    predicate: (state: RuntimeDaemonRecoveryState) => boolean,
  ) {
    const recoveryPath = harness.materialized.config.launchPlan.recoveryPath;
    for (let attempt = 0; attempt < 300; attempt += 1) {
      const raw = yield* Effect.option(harness.fs.readFileString(recoveryPath));
      if (Option.isSome(raw)) {
        const state = yield* Effect.option(
          Effect.try({
            try: () => decodeRecoveryJson(raw.value),
            catch: () => new IntegrationTestError(),
          }),
        );
        if (Option.isSome(state) && predicate(state.value)) return state.value;
      }
      yield* Effect.sleep(10);
    }
    return yield* Effect.die(new Error("recovery state timeout"));
  },
);

const writeMode = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.writeMode")(function* (
  harness: IntegrationHarness,
  mode: string,
) {
  const stateDirectory = harness.materialized.config.launchPlan.stateDirectory;
  yield* harness.fs.writeFileString(harness.path.join(stateDirectory, "fake-mode"), mode);
  yield* harness.fs.writeFileString(
    harness.path.join(stateDirectory, "health-script"),
    encodeUnknownJson(["healthy"]),
  );
});

const clearFakeLogs = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.clearFakeLogs")(
  function* (harness: IntegrationHarness) {
    const stateDirectory = harness.materialized.config.launchPlan.stateDirectory;
    yield* Effect.all(
      ["spawn-log.ndjson", "health-log.ndjson"].map((name) =>
        harness.fs.remove(harness.path.join(stateDirectory, name), { force: true }),
      ),
      { discard: true },
    );
  },
);

describe("StandaloneRuntimeLauncher integration", () => {
  it.live("publishes discovery only after health and preserves foreign ownership on cleanup", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ healthcheckTimeoutMs: 700 });
        const plan = harness.materialized.config.launchPlan;
        assert.isFalse(yield* harness.fs.exists(harness.sourceDirectory));
        yield* harness.fs.writeFileString(
          harness.path.join(plan.stateDirectory, "fake-mode"),
          "delayed-health",
        );
        const child = yield* spawnLauncher(harness);
        yield* Effect.sleep(75);
        assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
        yield* waitForFile(harness.fs, plan.discoveryPath, true, child);
        const discovery = decodeDiscoveryJson(yield* harness.fs.readFileString(plan.discoveryPath));
        assert.equal(discovery.launcherPid, Number(child.pid));
        assert.equal(discovery.port, plan.port);

        const foreignOwnershipId = "f".repeat(32) as never;
        yield* harness.fs.writeFileString(
          plan.daemonLockPath,
          encodeLock({
            schemaVersion: 1,
            profileId: PROFILE_ID,
            launcherPid: process.pid,
            ownershipId: foreignOwnershipId,
            createdAt: CREATED_AT,
            runtimeVersion: plan.runtimeVersion,
            buildHash: plan.buildHash,
          }),
        );
        yield* harness.fs.writeFileString(
          plan.discoveryPath,
          encodeDiscovery({
            ...discovery,
            ownershipId: foreignOwnershipId,
            launcherPid: process.pid,
            serverPid: process.pid,
          }),
        );
        yield* child.kill({ killSignal: "SIGTERM" });
        assert.equal(yield* waitForExit(child), 0);
        assert.equal(
          decodeDiscoveryJson(yield* harness.fs.readFileString(plan.discoveryPath)).ownershipId,
          foreignOwnershipId,
        );
        assert.include(yield* harness.fs.readFileString(plan.daemonLockPath), foreignOwnershipId);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("rejects live and corrupt locks, then takes over a dead owner safely", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const plan = harness.materialized.config.launchPlan;
        yield* harness.fs.writeFileString(plan.daemonLockPath, "{corrupt\n");
        const corruptAttempt = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(corruptAttempt), 74);
        assert.equal(yield* harness.fs.readFileString(plan.daemonLockPath), "{corrupt\n");

        yield* harness.fs.writeFileString(
          plan.daemonLockPath,
          encodeLock({
            schemaVersion: 1,
            profileId: PROFILE_ID,
            launcherPid: process.pid,
            ownershipId: "1".repeat(32) as never,
            createdAt: CREATED_AT,
            runtimeVersion: plan.runtimeVersion,
            buildHash: plan.buildHash,
          }),
        );
        const liveAttempt = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(liveAttempt), 73);

        yield* harness.fs.writeFileString(
          plan.daemonLockPath,
          encodeLock({
            schemaVersion: 1,
            profileId: PROFILE_ID,
            launcherPid: 2_147_483_647,
            ownershipId: "2".repeat(32) as never,
            createdAt: CREATED_AT,
            runtimeVersion: plan.runtimeVersion,
            buildHash: plan.buildHash,
          }),
        );
        yield* harness.fs.writeFileString(
          harness.path.join(plan.stateDirectory, "fake-mode"),
          "healthy",
        );
        const owner = yield* spawnLauncher(harness);
        yield* waitForFile(harness.fs, plan.discoveryPath, true);
        const contender = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(contender), 73);
        yield* owner.kill({ killSignal: "SIGTERM" });
        assert.equal(yield* waitForExit(owner), 0);
        yield* waitForFile(harness.fs, plan.daemonLockPath, false);
        yield* waitForFile(harness.fs, plan.discoveryPath, false);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("recovers a crashed healthy child while retaining the launcher lock", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ initialBackoffMs: 120, maxBackoffMs: 120 });
        const plan = harness.materialized.config.launchPlan;
        yield* writeMode(harness, "crash-after-ready-once");
        const launcher = yield* spawnLauncher(harness);
        yield* waitForFile(harness.fs, plan.discoveryPath, true, launcher);
        const firstDiscovery = decodeDiscoveryJson(
          yield* harness.fs.readFileString(plan.discoveryPath),
        );
        const lockBefore = yield* harness.fs.readFileString(plan.daemonLockPath);

        yield* waitForFile(harness.fs, plan.discoveryPath, false, launcher);
        assert.isTrue(yield* harness.fs.exists(plan.daemonLockPath));
        const backoff = yield* waitForRecovery(
          harness,
          (state) => state.circuitState === "backoff",
        );
        assert.equal(backoff.lastFailureReason, "server-exited");
        yield* waitForFile(harness.fs, plan.discoveryPath, true, launcher);
        const secondDiscovery = decodeDiscoveryJson(
          yield* harness.fs.readFileString(plan.discoveryPath),
        );
        const records = yield* waitForSpawnCount(harness, 2);

        assert.notEqual(firstDiscovery.serverPid, secondDiscovery.serverPid);
        assert.equal(firstDiscovery.launcherPid, secondDiscovery.launcherPid);
        assert.equal(secondDiscovery.launcherPid, Number(launcher.pid));
        assert.equal(yield* harness.fs.readFileString(plan.daemonLockPath), lockBefore);
        assert.notEqual(records[0]?.pid, records[1]?.pid);
        yield* Effect.sleep(80);
        assert.equal(
          decodeDiscoveryJson(yield* harness.fs.readFileString(plan.discoveryPath)).serverPid,
          secondDiscovery.serverPid,
        );

        yield* launcher.kill({ killSignal: "SIGTERM" });
        assert.equal(yield* waitForExit(launcher), 0);
        const stoppedRecovery = decodeRecoveryJson(
          yield* harness.fs.readFileString(plan.recoveryPath),
        );
        assert.lengthOf(stoppedRecovery.failureTimestamps, 1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("uses increasing bounded backoff, opens the circuit, and refuses another child", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          maxRestarts: 3,
          initialBackoffMs: 60,
          maxBackoffMs: 240,
        });
        const plan = harness.materialized.config.launchPlan;
        yield* writeMode(harness, "always-crash");
        const launcher = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(launcher, 3_000), 0);

        const records = yield* readSpawnRecords(harness);
        assert.lengthOf(records, 4);
        const intervals = records.slice(1).map((record, index) => record.at - records[index]!.at);
        assert.isAtLeast(intervals[0] ?? 0, 40);
        assert.isAtLeast(intervals[1] ?? 0, 90);
        assert.isAtLeast(intervals[2] ?? 0, 180);
        const open = decodeRecoveryJson(yield* harness.fs.readFileString(plan.recoveryPath));
        assert.equal(open.circuitState, "open");
        assert.equal(open.lastFailureReason, "server-exited");
        assert.lengthOf(open.failureTimestamps, 4);
        assert.isNotNull(open.circuitOpenedAt);
        assert.isNull(open.nextRestartAt);
        assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
        assert.isFalse(yield* harness.fs.exists(plan.daemonLockPath));
        assert.notMatch(encodeUnknownJson(open), /(?:command|stderr|token|credential)/iu);
        assert.isFalse(
          (yield* harness.fs.readDirectory(plan.runDirectory)).some((name) =>
            name.startsWith("recovery.json.tmp-"),
          ),
        );

        const refused = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(refused), 0);
        assert.lengthOf(yield* readSpawnRecords(harness), 4);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("requires consecutive health failures and resets history only after stable health", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({
          healthcheckIntervalMs: 25,
          consecutiveHealthFailuresBeforeRestart: 3,
          healthyResetAfterMs: 180,
          initialBackoffMs: 80,
          maxBackoffMs: 80,
        });
        const plan = harness.materialized.config.launchPlan;
        yield* writeMode(harness, "healthy");
        yield* harness.fs.writeFileString(
          harness.path.join(plan.stateDirectory, "health-script"),
          encodeUnknownJson(["healthy", "fail", "fail", "healthy", "fail", "fail", "healthy"]),
        );
        const launcher = yield* spawnLauncher(harness);
        yield* waitForFile(harness.fs, plan.discoveryPath, true, launcher);
        const firstDiscovery = decodeDiscoveryJson(
          yield* harness.fs.readFileString(plan.discoveryPath),
        );
        yield* waitForHealthRequestCount(harness, 7);
        assert.lengthOf(yield* readSpawnRecords(harness), 1);
        assert.equal(
          decodeDiscoveryJson(yield* harness.fs.readFileString(plan.discoveryPath)).serverPid,
          firstDiscovery.serverPid,
        );

        yield* harness.fs.writeFileString(
          harness.path.join(plan.stateDirectory, "health-script"),
          encodeUnknownJson(["hang"]),
        );
        const backoff = yield* waitForRecovery(
          harness,
          (state) => state.circuitState === "backoff",
        );
        assert.equal(backoff.lastFailureReason, "healthcheck-failed");
        assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
        yield* harness.fs.writeFileString(
          harness.path.join(plan.stateDirectory, "health-script"),
          encodeUnknownJson(["healthy"]),
        );
        yield* waitForFile(harness.fs, plan.discoveryPath, true, launcher);
        const restarted = yield* waitForRecovery(
          harness,
          (state) => state.circuitState === "closed" && state.lastSuccessfulHealthcheckAt !== null,
        );
        assert.lengthOf(restarted.failureTimestamps, 1);

        const stable = yield* waitForRecovery(
          harness,
          (state) => state.circuitState === "closed" && state.failureTimestamps.length === 0,
        );
        assert.isNull(stable.lastFailureReason);
        yield* launcher.kill({ killSignal: "SIGTERM" });
        assert.equal(yield* waitForExit(launcher), 0);
        const stopped = decodeRecoveryJson(yield* harness.fs.readFileString(plan.recoveryPath));
        assert.isEmpty(stopped.failureTimestamps);
        assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live(
    "classifies startup timeout, PID mismatch, and stale runtime state without discovery",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const harness = yield* makeHarness({
            maxRestarts: 1,
            initialBackoffMs: 20,
            maxBackoffMs: 20,
            healthcheckTimeoutMs: 120,
          });
          const plan = harness.materialized.config.launchPlan;
          const cases = [
            { mode: "startup-timeout", reason: "startup-health-timeout" },
            { mode: "wrong-pid", reason: "runtime-state-mismatch" },
            { mode: "stale-runtime", reason: "runtime-state-mismatch" },
          ] as const;
          for (const testCase of cases) {
            yield* Effect.all(
              [
                plan.recoveryPath,
                plan.discoveryPath,
                harness.path.join(plan.stateDirectory, "server-runtime.json"),
              ].map((filePath) => harness.fs.remove(filePath, { force: true })),
              { discard: true },
            );
            yield* clearFakeLogs(harness);
            if (testCase.mode === "stale-runtime") {
              yield* harness.fs.writeFileString(
                harness.path.join(plan.stateDirectory, "server-runtime.json"),
                `${encodeUnknownJson({
                  version: 1,
                  pid: 2_147_483_647,
                  port: plan.port,
                  origin: plan.origin,
                  startedAt: "2020-01-01T00:00:00.000Z",
                })}\n`,
              );
            }
            yield* writeMode(harness, testCase.mode);
            const launcher = yield* spawnLauncher(harness);
            assert.equal(yield* waitForExit(launcher), 0);
            const state = decodeRecoveryJson(yield* harness.fs.readFileString(plan.recoveryPath));
            assert.equal(state.circuitState, "open");
            assert.equal(state.lastFailureReason, testCase.reason);
            assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("fails closed for corrupt, symlinked, or identity-mismatched recovery state", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const plan = harness.materialized.config.launchPlan;
        yield* writeMode(harness, "healthy");

        yield* harness.fs.writeFileString(plan.recoveryPath, "{corrupt\n");
        const corrupt = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(corrupt), 74);
        assert.equal(yield* harness.fs.readFileString(plan.recoveryPath), "{corrupt\n");

        const externalDirectory = yield* harness.fs.makeTempDirectoryScoped({
          prefix: "t3 external recovery ",
        });
        const externalPath = harness.path.join(externalDirectory, "foreign.json");
        yield* harness.fs.writeFileString(externalPath, "foreign\n");
        yield* harness.fs.remove(plan.recoveryPath);
        yield* harness.fs.symlink(externalPath, plan.recoveryPath);
        const symlinked = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(symlinked), 65);
        assert.equal(yield* harness.fs.readFileString(externalPath), "foreign\n");

        yield* harness.fs.remove(plan.recoveryPath);
        const mismatched = encodeRecovery({
          schemaVersion: 1,
          profileId: PROFILE_ID,
          runtimeVersion: plan.runtimeVersion,
          buildHash: "abcdef0123456789" as never,
          circuitState: "open",
          failureTimestamps: [CREATED_AT],
          lastFailureReason: "server-exited",
          nextRestartAt: null,
          lastSuccessfulHealthcheckAt: null,
          continuousHealthySince: null,
          circuitOpenedAt: CREATED_AT,
        });
        yield* harness.fs.writeFileString(plan.recoveryPath, mismatched);
        const stale = yield* spawnLauncher(harness);
        assert.equal(yield* waitForExit(stale), 74);
        assert.equal(yield* harness.fs.readFileString(plan.recoveryPath), mismatched);
        assert.isEmpty(yield* readSpawnRecords(harness));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.live("interrupts backoff immediately without recording an intentional shutdown", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness({ initialBackoffMs: 1_000, maxBackoffMs: 1_000 });
        const plan = harness.materialized.config.launchPlan;
        yield* writeMode(harness, "always-crash");
        const launcher = yield* spawnLauncher(harness);
        const backoff = yield* waitForRecovery(
          harness,
          (state) => state.circuitState === "backoff",
        );
        assert.lengthOf(backoff.failureTimestamps, 1);
        const signalAt = yield* Clock.currentTimeMillis;
        yield* launcher.kill({ killSignal: "SIGTERM" });
        assert.equal(yield* waitForExit(launcher), 0);
        assert.isBelow((yield* Clock.currentTimeMillis) - signalAt, 500);
        assert.lengthOf(yield* readSpawnRecords(harness), 1);
        const stopped = decodeRecoveryJson(yield* harness.fs.readFileString(plan.recoveryPath));
        assert.lengthOf(stopped.failureTimestamps, 1);
        assert.equal(stopped.lastFailureReason, "server-exited");
        assert.isFalse(yield* harness.fs.exists(plan.daemonLockPath));
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
