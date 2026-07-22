/**
 * Self-contained launcher source installed into each runtime profile.
 *
 * Keep this file dependency-free: the materialized script may only use Node
 * built-ins and must remain runnable after the app and checkout disappear.
 */
export const STANDALONE_RUNTIME_LAUNCHER_SOURCE = String.raw`import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { constants as fsConstants } from 'node:fs';
import * as fs from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';

const LOCK_SCHEMA_VERSION = 1;
const DISCOVERY_SCHEMA_VERSION = 1;
const CONFIG_SCHEMA_VERSION = 1;
const LAUNCH_PLAN_SCHEMA_VERSION = 1;
const ENVIRONMENT_KEYS = [
  'T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD',
  'T3CODE_HOME',
  'T3CODE_HOST',
  'T3CODE_LOGS_DIR',
  'T3CODE_MODE',
  'T3CODE_NO_BROWSER',
  'T3CODE_PORT',
  'T3CODE_STATE_DIR',
  'T3CODE_TAILSCALE_SERVE',
];
const SIGNAL_EXIT_CODES = { SIGHUP: 129, SIGINT: 130, SIGTERM: 143 };

class LauncherFailure extends Error {
  constructor(code, exitCode) {
    super(code);
    this.code = code;
    this.exitCode = exitCode;
  }
}

const fail = (code, exitCode) => {
  throw new LauncherFailure(code, exitCode);
};

const isObject = (value) => value !== null && typeof value === 'object' && !Array.isArray(value);
const isPositiveInt = (value) => Number.isInteger(value) && value > 0;
const isPort = (value) => isPositiveInt(value) && value <= 65535;
const isString = (value) => typeof value === 'string' && value.length > 0;
const isIsoDate = (value) => isString(value) && Number.isFinite(Date.parse(value));
const isOwnershipId = (value) => typeof value === 'string' && /^[a-f0-9]{32}$/.test(value);
const isProfileId = (value) => value === 'dev' || value === 'alpha' || value === 'nightly' ||
  (typeof value === 'string' && /^custom:[a-z0-9][a-z0-9-]{0,62}$/.test(value));
const isRuntimeVersion = (value) => typeof value === 'string' && value.length <= 128 &&
  /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value) && !value.includes('..');
const isBuildHash = (value) => typeof value === 'string' && /^[a-f0-9]{7,64}$/.test(value);
const isVersionDirectory = (value) => typeof value === 'string' && value.length <= 196 &&
  /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value) && !value.includes('..');

const isPathWithin = (root, candidate) => {
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
};

const assertExactKeys = (value, expected) => {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    fail('invalid-config', 64);
  }
};

const assertRegularPath = async (candidate, root, executable) => {
  const info = await fs.lstat(candidate).catch(() => fail('invalid-path', 65));
  if (!info.isFile() || info.isSymbolicLink()) fail('invalid-path', 65);
  const realRoot = await fs.realpath(root).catch(() => fail('invalid-path', 65));
  const realCandidate = await fs.realpath(candidate).catch(() => fail('invalid-path', 65));
  const relativeCandidate = path.relative(path.resolve(root), path.resolve(candidate));
  if (!isPathWithin(realRoot, realCandidate) || path.resolve(realRoot, relativeCandidate) !== path.resolve(realCandidate)) {
    fail('invalid-path', 65);
  }
  if (executable && (info.mode & 0o111) === 0) fail('node-not-executable', 65);
};

const decodeLock = (value, profileId) => {
  if (!isObject(value)) return undefined;
  assertExactKeys(value, ['schemaVersion', 'profileId', 'launcherPid', 'ownershipId', 'createdAt', 'runtimeVersion', 'buildHash']);
  if (
    value.schemaVersion !== LOCK_SCHEMA_VERSION ||
    value.profileId !== profileId ||
    !isPositiveInt(value.launcherPid) ||
    !isOwnershipId(value.ownershipId) ||
    !isIsoDate(value.createdAt) ||
    !isRuntimeVersion(value.runtimeVersion) ||
    !isBuildHash(value.buildHash)
  ) return undefined;
  return value;
};

const decodeDiscovery = (value, profileId) => {
  if (!isObject(value)) return undefined;
  assertExactKeys(value, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'ownershipId',
    'launcherPid', 'serverPid', 'port', 'origin', 'startedAt', 'readyAt',
  ]);
  if (
    value.schemaVersion !== DISCOVERY_SCHEMA_VERSION ||
    value.profileId !== profileId ||
    !isRuntimeVersion(value.runtimeVersion) ||
    !isBuildHash(value.buildHash) ||
    !isOwnershipId(value.ownershipId) ||
    !isPositiveInt(value.launcherPid) ||
    !isPositiveInt(value.serverPid) ||
    !isPort(value.port) ||
    !isString(value.origin) ||
    !isIsoDate(value.startedAt) ||
    !isIsoDate(value.readyAt)
  ) return undefined;
  return value;
};

const parseDocument = (raw, decoder, corruptCode) => {
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    fail(corruptCode, 74);
  }
  let decoded;
  try {
    decoded = decoder(value);
  } catch {
    fail(corruptCode, 74);
  }
  if (decoded === undefined) fail(corruptCode, 74);
  return decoded;
};

const readOptional = async (filePath) => {
  try {
    return await fs.readFile(filePath, 'utf8');
  } catch (error) {
    if (error && error.code === 'ENOENT') return undefined;
    fail('filesystem-error', 74);
  }
};

const probePid = (pid) => {
  try {
    process.kill(pid, 0);
    return 'alive';
  } catch (error) {
    if (error && error.code === 'ESRCH') return 'dead';
    if (error && error.code === 'EPERM') return 'alive';
    return 'unknown';
  }
};

const removeIfUnchanged = async (filePath, originalRaw) => {
  const currentRaw = await readOptional(filePath);
  if (currentRaw === undefined || currentRaw !== originalRaw) fail('ownership-changed', 74);
  await fs.unlink(filePath).catch(() => fail('filesystem-error', 74));
};

const acquireLock = async (config, ownershipId, createdAt) => {
  const lock = {
    schemaVersion: LOCK_SCHEMA_VERSION,
    profileId: config.profileId,
    launcherPid: process.pid,
    ownershipId,
    createdAt,
    runtimeVersion: config.runtimeVersion,
    buildHash: config.buildHash,
  };
  const encoded = JSON.stringify(lock) + '\n';
  for (let attempt = 0; attempt < 2; attempt += 1) {
    let handle;
    try {
      handle = await fs.open(config.launchPlan.daemonLockPath, 'wx', 0o600);
      await handle.writeFile(encoded, 'utf8');
      await handle.sync();
      await handle.close();
      return lock;
    } catch (error) {
      await handle?.close().catch(() => {});
      if (!error || error.code !== 'EEXIST') fail('filesystem-error', 74);
      await assertRegularPath(config.launchPlan.daemonLockPath, config.launchPlan.runDirectory, false);
      const raw = await readOptional(config.launchPlan.daemonLockPath);
      if (raw === undefined) continue;
      const existing = parseDocument(raw, (value) => decodeLock(value, config.profileId), 'corrupt-lock');
      if (probePid(existing.launcherPid) !== 'dead') fail('daemon-already-running', 73);
      await removeIfUnchanged(config.launchPlan.daemonLockPath, raw);
    }
  }
  fail('daemon-already-running', 73);
};

const prepareDiscovery = async (config) => {
  const raw = await readOptional(config.launchPlan.discoveryPath);
  if (raw === undefined) return;
  await assertRegularPath(config.launchPlan.discoveryPath, config.launchPlan.runDirectory, false);
  const existing = parseDocument(raw, (value) => decodeDiscovery(value, config.profileId), 'corrupt-discovery');
  if (probePid(existing.launcherPid) !== 'dead' || probePid(existing.serverPid) !== 'dead') {
    fail('discovery-owned-by-live-process', 73);
  }
  await removeIfUnchanged(config.launchPlan.discoveryPath, raw);
};

const writeExclusiveAtomically = async (filePath, value, ownershipId) => {
  const temporaryPath = filePath + '.tmp-' + ownershipId;
  const encoded = JSON.stringify(value) + '\n';
  let handle;
  try {
    handle = await fs.open(temporaryPath, 'wx', 0o600);
    await handle.writeFile(encoded, 'utf8');
    await handle.sync();
    await handle.close();
    handle = undefined;
    await fs.link(temporaryPath, filePath);
  } catch {
    await handle?.close().catch(() => {});
    fail('filesystem-error', 74);
  } finally {
    await fs.unlink(temporaryPath).catch((error) => {
      if (!error || error.code !== 'ENOENT') return undefined;
    });
  }
};

const cleanupOwnedDocument = async (filePath, decoder, ownershipId, serverPid) => {
  const raw = await readOptional(filePath);
  if (raw === undefined) return;
  try {
    await assertRegularPath(filePath, path.dirname(filePath), false);
  } catch {
    return;
  }
  let value;
  try {
    value = JSON.parse(raw);
    value = decoder(value);
  } catch {
    return;
  }
  if (
    value === undefined ||
    value.ownershipId !== ownershipId ||
    value.launcherPid !== process.pid ||
    (serverPid !== undefined && value.serverPid !== serverPid)
  ) return;
  const currentRaw = await readOptional(filePath);
  if (currentRaw !== raw) return;
  await fs.unlink(filePath).catch(() => {});
};

const requestHealth = (origin, timeoutMs) => new Promise((resolve) => {
  const request = http.get(origin + '/.well-known/t3/environment', { timeout: timeoutMs }, (response) => {
    response.resume();
    resolve(response.statusCode !== undefined && response.statusCode >= 200 && response.statusCode < 300);
  });
  request.on('timeout', () => {
    request.destroy();
    resolve(false);
  });
  request.on('error', () => resolve(false));
});

const readServerRuntime = async (config, serverPid, startedAt) => {
  const statePath = path.join(config.launchPlan.stateDirectory, 'server-runtime.json');
  const raw = await readOptional(statePath);
  if (raw === undefined) return undefined;
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (
    !isObject(value) || value.version !== 1 || value.pid !== serverPid ||
    value.port !== config.launchPlan.port || value.origin !== config.launchPlan.origin ||
    !isIsoDate(value.startedAt) || Date.parse(value.startedAt) < Date.parse(startedAt) - 1000
  ) return undefined;
  return value;
};

const waitForHealth = async (config, serverPid, startedAt, childOutcome) => {
  const deadline = Date.now() + config.healthcheckTimeoutMs;
  while (Date.now() < deadline) {
    const state = await readServerRuntime(config, serverPid, startedAt);
    if (state !== undefined && await requestHealth(config.launchPlan.origin, config.healthcheckRequestTimeoutMs)) {
      return;
    }
    const outcome = await Promise.race([
      childOutcome.then((value) => ({ kind: 'exit', value })),
      new Promise((resolve) => setTimeout(() => resolve({ kind: 'poll' }), config.healthcheckPollIntervalMs)),
    ]);
    if (outcome.kind === 'exit') fail('server-exited-before-ready', 75);
  }
  fail('health-timeout', 75);
};

const validateConfig = async (value, configPath) => {
  if (!isObject(value)) fail('invalid-config', 64);
  assertExactKeys(value, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'versionDirectory',
    'launchPlan', 'healthcheckTimeoutMs', 'healthcheckPollIntervalMs',
    'healthcheckRequestTimeoutMs', 'shutdownTimeoutMs',
  ]);
  const plan = value.launchPlan;
  if (
    value.schemaVersion !== CONFIG_SCHEMA_VERSION || !isProfileId(value.profileId) ||
    !isRuntimeVersion(value.runtimeVersion) || !isBuildHash(value.buildHash) || !isVersionDirectory(value.versionDirectory) ||
    !isPositiveInt(value.healthcheckTimeoutMs) || !isPositiveInt(value.healthcheckPollIntervalMs) ||
    !isPositiveInt(value.healthcheckRequestTimeoutMs) || !isPositiveInt(value.shutdownTimeoutMs) ||
    !isObject(plan)
  ) fail('invalid-config', 64);
  assertExactKeys(plan, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'versionDirectory',
    'nodeExecutablePath', 'serverEntrypointPath', 'argv', 'cwd', 'environment', 'port',
    'origin', 'profileDirectory', 'runtimeVersionDirectory', 'stateDirectory',
    'logsDirectory', 'runDirectory', 'daemonLockPath', 'discoveryPath', 'preflight',
  ]);
  if (
    plan.schemaVersion !== LAUNCH_PLAN_SCHEMA_VERSION || plan.profileId !== value.profileId ||
    plan.runtimeVersion !== value.runtimeVersion || plan.buildHash !== value.buildHash ||
    plan.versionDirectory !== value.versionDirectory || !isPort(plan.port) ||
    plan.origin !== 'http://127.0.0.1:' + String(plan.port) || !Array.isArray(plan.argv) ||
    plan.argv.length === 0 || plan.argv.some((argument) => typeof argument !== 'string') ||
    !isString(plan.nodeExecutablePath) || !isString(plan.serverEntrypointPath) ||
    !isString(plan.cwd) || !isString(plan.profileDirectory) || !isString(plan.runtimeVersionDirectory) ||
    !isString(plan.stateDirectory) || !isString(plan.logsDirectory) || !isString(plan.runDirectory) ||
    !isString(plan.daemonLockPath) || !isString(plan.discoveryPath) ||
    !isObject(plan.environment) || !isObject(plan.preflight)
  ) fail('invalid-config', 64);
  assertExactKeys(plan.preflight, ['schemaVersion', 'profileId', 'platform', 'architecture', 'ok', 'checks']);
  if (
    plan.preflight.schemaVersion !== 1 || plan.preflight.profileId !== value.profileId ||
    plan.preflight.ok !== true || !['darwin', 'linux', 'win32'].includes(plan.preflight.platform) ||
    !['arm64', 'x64'].includes(plan.preflight.architecture) || !Array.isArray(plan.preflight.checks)
  ) fail('invalid-config', 64);
  assertExactKeys(plan.environment, ENVIRONMENT_KEYS);
  if (
    plan.environment.T3CODE_MODE !== 'web' || plan.environment.T3CODE_HOST !== '127.0.0.1' ||
    plan.environment.T3CODE_PORT !== String(plan.port) || plan.environment.T3CODE_HOME !== plan.profileDirectory ||
    plan.environment.T3CODE_STATE_DIR !== plan.stateDirectory || plan.environment.T3CODE_LOGS_DIR !== plan.logsDirectory ||
    plan.environment.T3CODE_NO_BROWSER !== 'true' ||
    plan.environment.T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD !== 'false' ||
    plan.environment.T3CODE_TAILSCALE_SERVE !== 'false'
  ) fail('invalid-config', 64);

  const configuredProfileRoot = path.resolve(plan.profileDirectory);
  const profileRoot = await fs.realpath(plan.profileDirectory).catch(() => fail('invalid-path', 65));
  const launcherRoot = path.join(profileRoot, 'runtime', 'launcher');
  const runtimeVersionsRoot = path.join(profileRoot, 'runtime', 'versions');
  const realConfigPath = await fs.realpath(configPath).catch(() => fail('invalid-path', 65));
  const realScriptPath = await fs.realpath(process.argv[1]).catch(() => fail('invalid-path', 65));
  const realExecutablePath = await fs.realpath(process.execPath).catch(() => fail('invalid-path', 65));
  const realLauncherRoot = await fs.realpath(launcherRoot).catch(() => fail('invalid-path', 65));
  const realRuntimeVersionsRoot = await fs.realpath(runtimeVersionsRoot).catch(() => fail('invalid-path', 65));
  const realRuntimeVersionDirectory = await fs.realpath(plan.runtimeVersionDirectory).catch(() => fail('invalid-path', 65));
  if (
    !isPathWithin(realLauncherRoot, realConfigPath) || !isPathWithin(realLauncherRoot, realScriptPath) ||
    !isPathWithin(realLauncherRoot, realExecutablePath) || !isPathWithin(realRuntimeVersionsRoot, realRuntimeVersionDirectory) ||
    !isPathWithin(plan.runtimeVersionDirectory, plan.serverEntrypointPath) ||
    !isPathWithin(plan.runtimeVersionDirectory, plan.nodeExecutablePath) ||
    plan.argv[0] !== plan.serverEntrypointPath || path.resolve(plan.cwd) !== configuredProfileRoot ||
    path.resolve(plan.stateDirectory) !== path.join(configuredProfileRoot, 'state') ||
    path.resolve(plan.logsDirectory) !== path.join(configuredProfileRoot, 'logs') ||
    path.resolve(plan.runDirectory) !== path.join(configuredProfileRoot, 'run') ||
    path.resolve(plan.daemonLockPath) !== path.join(configuredProfileRoot, 'run', 'daemon.lock') ||
    path.resolve(plan.discoveryPath) !== path.join(configuredProfileRoot, 'run', 'discovery.json')
  ) fail('invalid-path', 65);
  await assertRegularPath(plan.serverEntrypointPath, plan.runtimeVersionDirectory, false);
  await assertRegularPath(plan.nodeExecutablePath, plan.runtimeVersionDirectory, true);
  await assertRegularPath(process.execPath, launcherRoot, true);
  return value;
};

const terminateChild = async (child, outcome, timeoutMs) => {
  if (child.exitCode !== null || child.signalCode !== null) return;
  child.kill('SIGTERM');
  const completed = await Promise.race([
    outcome.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), timeoutMs)),
  ]);
  if (!completed && child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  await outcome.catch(() => {});
};

const main = async () => {
  const configPath = process.argv[2];
  if (!isString(configPath) || process.argv.length !== 3) fail('invalid-arguments', 64);
  const rawConfig = await fs.readFile(configPath, 'utf8').catch(() => fail('invalid-config', 64));
  let unvalidated;
  try {
    unvalidated = JSON.parse(rawConfig);
  } catch {
    fail('invalid-config', 64);
  }
  const config = await validateConfig(unvalidated, configPath);
  await fs.access(config.launchPlan.runDirectory, fsConstants.W_OK).catch(() => fail('invalid-path', 65));

  const ownershipId = randomBytes(16).toString('hex');
  const startedAt = new Date().toISOString();
  await acquireLock(config, ownershipId, startedAt);
  let child;
  let childOutcome;
  let serverPid;
  const signalHandlers = new Map();
  try {
    await prepareDiscovery(config);
    const childEnvironment = { ...process.env };
    for (const key of ENVIRONMENT_KEYS) childEnvironment[key] = config.launchPlan.environment[key];
    child = spawn(process.execPath, config.launchPlan.argv, {
      cwd: config.launchPlan.cwd,
      env: childEnvironment,
      shell: false,
      stdio: 'inherit',
    });
    childOutcome = new Promise((resolve, reject) => {
      child.once('error', () => reject(new LauncherFailure('server-spawn-failed', 76)));
      child.once('exit', (code, signal) => resolve({ code, signal }));
    });
    serverPid = child.pid;
    if (!isPositiveInt(serverPid)) fail('server-spawn-failed', 76);
    for (const signal of ['SIGHUP', 'SIGINT', 'SIGTERM']) {
      const handler = () => {
        if (child.exitCode === null && child.signalCode === null) child.kill(signal);
      };
      signalHandlers.set(signal, handler);
      process.on(signal, handler);
    }

    try {
      await waitForHealth(config, serverPid, startedAt, childOutcome);
    } catch (error) {
      await terminateChild(child, childOutcome, config.shutdownTimeoutMs);
      throw error;
    }
    const discovery = {
      schemaVersion: DISCOVERY_SCHEMA_VERSION,
      profileId: config.profileId,
      runtimeVersion: config.runtimeVersion,
      buildHash: config.buildHash,
      ownershipId,
      launcherPid: process.pid,
      serverPid,
      port: config.launchPlan.port,
      origin: config.launchPlan.origin,
      startedAt,
      readyAt: new Date().toISOString(),
    };
    await writeExclusiveAtomically(config.launchPlan.discoveryPath, discovery, ownershipId);
    const outcome = await childOutcome;
    if (outcome.code !== null) return outcome.code;
    return SIGNAL_EXIT_CODES[outcome.signal] ?? 1;
  } finally {
    for (const [signal, handler] of signalHandlers) process.off(signal, handler);
    if (serverPid !== undefined) {
      await cleanupOwnedDocument(
        config.launchPlan.discoveryPath,
        (value) => decodeDiscovery(value, config.profileId),
        ownershipId,
        serverPid,
      );
    }
    await cleanupOwnedDocument(
      config.launchPlan.daemonLockPath,
      (value) => decodeLock(value, config.profileId),
      ownershipId,
      undefined,
    );
  }
};

main().then(
  (exitCode) => { process.exitCode = exitCode; },
  (error) => {
    const failure = error instanceof LauncherFailure ? error : new LauncherFailure('internal-error', 70);
    process.stderr.write('T3 runtime launcher failed: ' + failure.code + '\n');
    process.exitCode = failure.exitCode;
  },
);
`;
