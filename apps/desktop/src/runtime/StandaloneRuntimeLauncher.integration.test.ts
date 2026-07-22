import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  RuntimeArtifactManifest,
  type RuntimeArtifactManifest as RuntimeArtifactManifestValue,
  RuntimeDaemonDiscovery,
  RuntimeDaemonLock,
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

const FAKE_SERVER_SOURCE = String.raw`import * as fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';

const argument = (name) => process.argv[process.argv.indexOf(name) + 1];
const port = Number(argument('--port'));
const stateDirectory = argument('--state-dir');
const mode = await fs.readFile(path.join(stateDirectory, 'fake-mode'), 'utf8').catch(() => 'healthy');
const origin = mode === 'wrong-origin' ? 'http://127.0.0.1:1' : 'http://127.0.0.1:' + String(port);
const statePort = mode === 'wrong-port' ? port + 1 : port;
const statePid = mode === 'wrong-pid' ? process.pid + 1 : process.pid;
await fs.writeFile(path.join(stateDirectory, 'server-runtime.json'), JSON.stringify({
  version: 1,
  pid: statePid,
  host: '127.0.0.1',
  port: statePort,
  origin,
  startedAt: new Date().toISOString(),
}) + '\n');

const server = http.createServer((request, response) => {
  if (request.url === '/.well-known/t3/environment') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end('{}');
    return;
  }
  response.writeHead(404);
  response.end();
});
if (mode !== 'no-health') {
  if (mode === 'delayed-health') await new Promise((resolve) => setTimeout(resolve, 200));
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
}
const shutdown = () => server.close(() => process.exit(0));
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
`;

interface IntegrationHarness {
  readonly fs: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly sourceDirectory: string;
  readonly materialized: StandaloneRuntimeLauncher.MaterializedRuntimeLauncher;
}

class IntegrationTestError extends Schema.TaggedErrorClass<IntegrationTestError>()(
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

const makeHarness = Effect.fn("StandaloneRuntimeLauncherIntegrationTest.makeHarness")(function* () {
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
  yield* fs.writeFileString(path.join(sourceDirectory, "manifest.json"), encodeManifest(manifest));
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
    healthcheckTimeoutMs: 600,
    healthcheckPollIntervalMs: 20,
    healthcheckRequestTimeoutMs: 100,
    shutdownTimeoutMs: 300,
  }).pipe(
    Effect.provideService(RuntimeArtifactInstaller.RuntimeArtifactInstaller, installer),
    Effect.provideService(RuntimeDaemonLaunchPlan.RuntimeDaemonLaunchPlanService, planner),
  );
  const materialized = yield* launcher.materialize(PROFILE_ID);
  yield* fs.remove(sourceDirectory, { recursive: true });
  return { fs, path, sourceDirectory, materialized } satisfies IntegrationHarness;
});

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

describe("StandaloneRuntimeLauncher integration", () => {
  it.live("publishes discovery only after health and preserves foreign ownership on cleanup", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
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

  it.live("rejects mismatched server PID, port, and origin without publishing discovery", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const harness = yield* makeHarness();
        const plan = harness.materialized.config.launchPlan;
        for (const mode of ["wrong-pid", "wrong-port", "wrong-origin"] as const) {
          yield* harness.fs.writeFileString(
            harness.path.join(plan.stateDirectory, "fake-mode"),
            mode,
          );
          const child = yield* spawnLauncher(harness);
          assert.equal(yield* waitForExit(child), 75);
          assert.isFalse(yield* harness.fs.exists(plan.discoveryPath));
          assert.isFalse(yield* harness.fs.exists(plan.daemonLockPath));
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
