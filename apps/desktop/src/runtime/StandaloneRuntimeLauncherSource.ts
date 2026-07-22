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
const RECOVERY_CONFIG_SCHEMA_VERSION = 1;
const RECOVERY_STATE_SCHEMA_VERSION = 1;
const CONFIG_SCHEMA_VERSION = 2;
const LAUNCH_PLAN_SCHEMA_VERSION = 2;
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
const isNullableIsoDate = (value) => value === null || isIsoDate(value);
const isOwnershipId = (value) => typeof value === 'string' && /^[a-f0-9]{32}$/.test(value);
const isProfileId = (value) => value === 'dev' || value === 'alpha' || value === 'nightly' ||
  (typeof value === 'string' && /^custom:[a-z0-9][a-z0-9-]{0,62}$/.test(value));
const isRuntimeVersion = (value) => typeof value === 'string' && value.length <= 128 &&
  /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value) && !value.includes('..');
const isBuildHash = (value) => typeof value === 'string' && /^[a-f0-9]{7,64}$/.test(value);
const isVersionDirectory = (value) => typeof value === 'string' && value.length <= 196 &&
  /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(value) && !value.includes('..');
const CIRCUIT_STATES = ['closed', 'backoff', 'open'];
const FAILURE_REASONS = [
  'server-exited',
  'startup-health-timeout',
  'runtime-state-invalid',
  'runtime-state-mismatch',
  'healthcheck-failed',
  'shutdown-timeout',
  'launcher-internal',
];

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

const decodeRecovery = (value, config) => {
  if (!isObject(value)) return undefined;
  assertExactKeys(value, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'circuitState',
    'failureTimestamps', 'lastFailureReason', 'nextRestartAt',
    'lastSuccessfulHealthcheckAt', 'continuousHealthySince', 'circuitOpenedAt',
  ]);
  if (
    value.schemaVersion !== RECOVERY_STATE_SCHEMA_VERSION ||
    value.profileId !== config.profileId ||
    value.runtimeVersion !== config.runtimeVersion ||
    value.buildHash !== config.buildHash ||
    !CIRCUIT_STATES.includes(value.circuitState) ||
    !Array.isArray(value.failureTimestamps) ||
    value.failureTimestamps.some((timestamp) => !isIsoDate(timestamp)) ||
    !(value.lastFailureReason === null || FAILURE_REASONS.includes(value.lastFailureReason)) ||
    !isNullableIsoDate(value.nextRestartAt) ||
    !isNullableIsoDate(value.lastSuccessfulHealthcheckAt) ||
    !isNullableIsoDate(value.continuousHealthySince) ||
    !isNullableIsoDate(value.circuitOpenedAt)
  ) return undefined;
  if (
    (value.circuitState === 'closed' && (value.nextRestartAt !== null || value.circuitOpenedAt !== null)) ||
    (value.circuitState === 'backoff' &&
      (value.nextRestartAt === null || value.circuitOpenedAt !== null || value.lastFailureReason === null)) ||
    (value.circuitState === 'open' &&
      (value.nextRestartAt !== null || value.circuitOpenedAt === null || value.lastFailureReason === null)) ||
    (value.circuitState !== 'closed' && value.continuousHealthySince !== null)
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

const readRecoveryRaw = async (config) => {
  try {
    await assertRegularPath(config.launchPlan.recoveryPath, config.launchPlan.runDirectory, false);
  } catch (error) {
    if (error && error.code === 'invalid-path') {
      const exists = await fs.access(config.launchPlan.recoveryPath).then(() => true, (cause) => {
        if (cause && cause.code === 'ENOENT') return false;
        fail('filesystem-error', 74);
      });
      if (!exists) return undefined;
    }
    throw error;
  }
  return await fs.readFile(config.launchPlan.recoveryPath, 'utf8').catch(() => fail('filesystem-error', 74));
};

const readRecovery = async (config) => {
  const raw = await readRecoveryRaw(config);
  if (raw === undefined) return undefined;
  return parseDocument(raw, (value) => decodeRecovery(value, config), 'corrupt-recovery');
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

const writeRecoveryAtomically = async (config, value, ownershipId) => {
  const existingRaw = await readRecoveryRaw(config);
  if (existingRaw !== undefined) {
    parseDocument(existingRaw, (document) => decodeRecovery(document, config), 'corrupt-recovery');
  }
  const temporaryPath = config.launchPlan.recoveryPath + '.tmp-' + ownershipId;
  const encoded = JSON.stringify(value) + '\n';
  let handle;
  try {
    handle = await fs.open(temporaryPath, 'wx', 0o600);
    await handle.writeFile(encoded, 'utf8');
    await handle.sync();
    await handle.close();
    handle = undefined;
    const currentRaw = await readRecoveryRaw(config);
    if (currentRaw !== existingRaw) fail('ownership-changed', 74);
    if (existingRaw === undefined) {
      await fs.link(temporaryPath, config.launchPlan.recoveryPath);
    } else {
      await fs.rename(temporaryPath, config.launchPlan.recoveryPath);
      return;
    }
  } catch (error) {
    await handle?.close().catch(() => {});
    if (error instanceof LauncherFailure) throw error;
    fail('filesystem-error', 74);
  } finally {
    await fs.unlink(temporaryPath).catch(() => {});
  }
};

const initialRecoveryState = (config) => ({
  schemaVersion: RECOVERY_STATE_SCHEMA_VERSION,
  profileId: config.profileId,
  runtimeVersion: config.runtimeVersion,
  buildHash: config.buildHash,
  circuitState: 'closed',
  failureTimestamps: [],
  lastFailureReason: null,
  nextRestartAt: null,
  lastSuccessfulHealthcheckAt: null,
  continuousHealthySince: null,
  circuitOpenedAt: null,
});

const pruneFailureTimestamps = (config, timestamps, nowMs) => timestamps.filter(
  (timestamp) => Date.parse(timestamp) >= nowMs - config.recovery.slidingWindowMs &&
    Date.parse(timestamp) <= nowMs,
);

const registerFailure = (config, state, reason, nowMs) => {
  const failureTimestamps = [
    ...pruneFailureTimestamps(config, state.failureTimestamps, nowMs),
    new Date(nowMs).toISOString(),
  ];
  if (failureTimestamps.length > config.recovery.maxRestarts) {
    return {
      ...state,
      circuitState: 'open',
      failureTimestamps,
      lastFailureReason: reason,
      nextRestartAt: null,
      continuousHealthySince: null,
      circuitOpenedAt: new Date(nowMs).toISOString(),
    };
  }
  const exponent = Math.max(0, failureTimestamps.length - 1);
  const backoffMs = Math.min(
    config.recovery.maxBackoffMs,
    config.recovery.initialBackoffMs * (2 ** exponent),
  );
  return {
    ...state,
    circuitState: 'backoff',
    failureTimestamps,
    lastFailureReason: reason,
    nextRestartAt: new Date(nowMs + backoffMs).toISOString(),
    continuousHealthySince: null,
    circuitOpenedAt: null,
  };
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
  if (raw === undefined) return { kind: 'missing' };
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    return { kind: 'invalid' };
  }
  if (
    !isObject(value) || value.version !== 1 || !isPositiveInt(value.pid) ||
    !isPort(value.port) || !isString(value.origin) || !isIsoDate(value.startedAt)
  ) return { kind: 'invalid' };
  if (
    value.pid !== serverPid || value.port !== config.launchPlan.port ||
    value.origin !== config.launchPlan.origin ||
    Date.parse(value.startedAt) < Date.parse(startedAt) - 1000
  ) return { kind: 'mismatch' };
  return { kind: 'match', value };
};

const waitForHealth = async (config, serverPid, startedAt, childOutcome, isShuttingDown) => {
  const deadline = Date.now() + config.healthcheckTimeoutMs;
  let lastRuntimeStateKind = 'missing';
  while (Date.now() < deadline) {
    const state = await readServerRuntime(config, serverPid, startedAt);
    lastRuntimeStateKind = state.kind;
    if (
      state.kind === 'match' &&
      await requestHealth(config.launchPlan.origin, config.healthcheckRequestTimeoutMs)
    ) {
      return { kind: 'healthy' };
    }
    const outcome = await Promise.race([
      childOutcome.then((value) => ({ kind: 'exit', value })),
      new Promise((resolve) => setTimeout(() => resolve({ kind: 'poll' }), config.healthcheckPollIntervalMs)),
    ]);
    if (outcome.kind === 'exit') {
      return isShuttingDown() ? { kind: 'shutdown' } : { kind: 'failure', reason: 'server-exited' };
    }
    if (isShuttingDown()) return { kind: 'shutdown' };
  }
  if (lastRuntimeStateKind === 'invalid') {
    return { kind: 'failure', reason: 'runtime-state-invalid' };
  }
  if (lastRuntimeStateKind === 'mismatch') {
    return { kind: 'failure', reason: 'runtime-state-mismatch' };
  }
  return { kind: 'failure', reason: 'startup-health-timeout' };
};

const validateConfig = async (value, configPath) => {
  if (!isObject(value)) fail('invalid-config', 64);
  assertExactKeys(value, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'versionDirectory',
    'launchPlan', 'healthcheckTimeoutMs', 'healthcheckPollIntervalMs',
    'healthcheckRequestTimeoutMs', 'shutdownTimeoutMs', 'recovery',
  ]);
  const plan = value.launchPlan;
  const recovery = value.recovery;
  if (
    value.schemaVersion !== CONFIG_SCHEMA_VERSION || !isProfileId(value.profileId) ||
    !isRuntimeVersion(value.runtimeVersion) || !isBuildHash(value.buildHash) || !isVersionDirectory(value.versionDirectory) ||
    !isPositiveInt(value.healthcheckTimeoutMs) || !isPositiveInt(value.healthcheckPollIntervalMs) ||
    !isPositiveInt(value.healthcheckRequestTimeoutMs) || !isPositiveInt(value.shutdownTimeoutMs) ||
    !isObject(plan) || !isObject(recovery)
  ) fail('invalid-config', 64);
  assertExactKeys(recovery, [
    'schemaVersion', 'enabled', 'maxRestarts', 'slidingWindowMs', 'initialBackoffMs',
    'maxBackoffMs', 'healthcheckIntervalMs', 'consecutiveHealthFailuresBeforeRestart',
    'healthyResetAfterMs',
  ]);
  if (
    recovery.schemaVersion !== RECOVERY_CONFIG_SCHEMA_VERSION || recovery.enabled !== true ||
    !isPositiveInt(recovery.maxRestarts) || recovery.maxRestarts > 100 ||
    !isPositiveInt(recovery.slidingWindowMs) || recovery.slidingWindowMs > 86400000 ||
    !isPositiveInt(recovery.initialBackoffMs) || recovery.initialBackoffMs > 86400000 ||
    !isPositiveInt(recovery.maxBackoffMs) || recovery.maxBackoffMs > 86400000 ||
    recovery.maxBackoffMs < recovery.initialBackoffMs ||
    !isPositiveInt(recovery.healthcheckIntervalMs) || recovery.healthcheckIntervalMs > 86400000 ||
    !isPositiveInt(recovery.consecutiveHealthFailuresBeforeRestart) ||
    recovery.consecutiveHealthFailuresBeforeRestart > 100 ||
    !isPositiveInt(recovery.healthyResetAfterMs) || recovery.healthyResetAfterMs > 86400000
  ) fail('invalid-config', 64);
  assertExactKeys(plan, [
    'schemaVersion', 'profileId', 'runtimeVersion', 'buildHash', 'versionDirectory',
    'nodeExecutablePath', 'serverEntrypointPath', 'argv', 'cwd', 'environment', 'port',
    'origin', 'profileDirectory', 'runtimeVersionDirectory', 'stateDirectory',
    'logsDirectory', 'runDirectory', 'daemonLockPath', 'discoveryPath', 'recoveryPath',
    'preflight',
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
    !isString(plan.recoveryPath) ||
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
    path.resolve(plan.discoveryPath) !== path.join(configuredProfileRoot, 'run', 'discovery.json') ||
    path.resolve(plan.recoveryPath) !== path.join(configuredProfileRoot, 'run', 'recovery.json')
  ) fail('invalid-path', 65);
  await assertRegularPath(plan.serverEntrypointPath, plan.runtimeVersionDirectory, false);
  await assertRegularPath(plan.nodeExecutablePath, plan.runtimeVersionDirectory, true);
  await assertRegularPath(process.execPath, launcherRoot, true);
  return value;
};

const terminateChild = async (child, outcome, timeoutMs) => {
  if (child.exitCode !== null || child.signalCode !== null) return false;
  child.kill('SIGTERM');
  const completed = await Promise.race([
    outcome.then(() => true),
    new Promise((resolve) => setTimeout(() => resolve(false), timeoutMs)),
  ]);
  if (!completed && child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  await outcome;
  return !completed;
};

const recordSuccessfulHealth = (config, state, nowMs) => {
  const now = new Date(nowMs).toISOString();
  const continuousHealthySince = state.continuousHealthySince ?? now;
  const resetHistory = nowMs - Date.parse(continuousHealthySince) >= config.recovery.healthyResetAfterMs;
  return {
    ...state,
    circuitState: 'closed',
    failureTimestamps: resetHistory ? [] : state.failureTimestamps,
    lastFailureReason: resetHistory ? null : state.lastFailureReason,
    nextRestartAt: null,
    lastSuccessfulHealthcheckAt: now,
    continuousHealthySince,
    circuitOpenedAt: null,
  };
};

const waitForDelayOrShutdown = (delayMs, shutdownPromise) => new Promise((resolve) => {
  let settled = false;
  const finish = (result) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    resolve(result);
  };
  const timer = setTimeout(() => finish('elapsed'), delayMs);
  shutdownPromise.then(() => finish('shutdown'));
});

const waitForMonitorEvent = (childOutcome, delayMs, shutdownPromise) => new Promise((resolve) => {
  let settled = false;
  const finish = (event) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    resolve(event);
  };
  const timer = setTimeout(() => finish({ kind: 'healthcheck' }), delayMs);
  childOutcome.then((outcome) => finish({ kind: 'exit', outcome }));
  shutdownPromise.then(() => finish({ kind: 'shutdown' }));
});

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
  const launcherStartedAt = new Date().toISOString();
  await acquireLock(config, ownershipId, launcherStartedAt);
  let child;
  let childOutcome;
  let serverPid;
  let recovery;
  let shuttingDown = false;
  let resolveShutdown;
  const shutdownPromise = new Promise((resolve) => {
    resolveShutdown = resolve;
  });
  const signalHandlers = new Map();
  try {
    for (const signal of ['SIGHUP', 'SIGINT', 'SIGTERM']) {
      const handler = () => {
        if (!shuttingDown) {
          shuttingDown = true;
          resolveShutdown(signal);
        }
        if (child && child.exitCode === null && child.signalCode === null) child.kill(signal);
      };
      signalHandlers.set(signal, handler);
      process.on(signal, handler);
    }
    await prepareDiscovery(config);
    recovery = await readRecovery(config);
    if (recovery === undefined) {
      recovery = initialRecoveryState(config);
      await writeRecoveryAtomically(config, recovery, ownershipId);
    }
    if (recovery.circuitState === 'open') return 0;

    const childEnvironment = { ...process.env };
    for (const key of ENVIRONMENT_KEYS) childEnvironment[key] = config.launchPlan.environment[key];
    while (!shuttingDown) {
      if (recovery.circuitState === 'backoff') {
        const remainingMs = Math.max(0, Date.parse(recovery.nextRestartAt) - Date.now());
        if (remainingMs > 0) {
          const backoffResult = await waitForDelayOrShutdown(remainingMs, shutdownPromise);
          if (backoffResult === 'shutdown') return 0;
        }
        recovery = { ...recovery, circuitState: 'closed', nextRestartAt: null };
        await writeRecoveryAtomically(config, recovery, ownershipId);
      }

      if (recovery.continuousHealthySince !== null) {
        recovery = { ...recovery, continuousHealthySince: null };
        await writeRecoveryAtomically(config, recovery, ownershipId);
      }

      const startedAt = new Date().toISOString();
      child = spawn(process.execPath, config.launchPlan.argv, {
        cwd: config.launchPlan.cwd,
        env: childEnvironment,
        shell: false,
        stdio: 'inherit',
      });
      childOutcome = new Promise((resolve) => {
        child.once('error', () => resolve({ code: null, signal: null, spawnError: true }));
        child.once('exit', (code, signal) => resolve({ code, signal, spawnError: false }));
      });
      serverPid = child.pid;
      let failureReason;
      if (!isPositiveInt(serverPid)) {
        failureReason = 'launcher-internal';
        await childOutcome;
      } else {
        const startup = await waitForHealth(
          config,
          serverPid,
          startedAt,
          childOutcome,
          () => shuttingDown,
        );
        if (startup.kind === 'shutdown') {
          await terminateChild(child, childOutcome, config.shutdownTimeoutMs);
          return 0;
        }
        if (startup.kind === 'failure') {
          const shutdownTimedOut = await terminateChild(
            child,
            childOutcome,
            config.shutdownTimeoutMs,
          );
          failureReason = shutdownTimedOut ? 'shutdown-timeout' : startup.reason;
        } else {
          const readyAtMs = Date.now();
          recovery = recordSuccessfulHealth(config, recovery, readyAtMs);
          await writeRecoveryAtomically(config, recovery, ownershipId);
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
            readyAt: new Date(readyAtMs).toISOString(),
          };
          await writeExclusiveAtomically(config.launchPlan.discoveryPath, discovery, ownershipId);
          let consecutiveHealthFailures = 0;
          while (failureReason === undefined && !shuttingDown) {
            const event = await waitForMonitorEvent(
              childOutcome,
              config.recovery.healthcheckIntervalMs,
              shutdownPromise,
            );
            if (event.kind === 'shutdown') {
              await terminateChild(child, childOutcome, config.shutdownTimeoutMs);
              return 0;
            }
            if (event.kind === 'exit') {
              failureReason = 'server-exited';
              break;
            }
            const healthy = await requestHealth(
              config.launchPlan.origin,
              config.healthcheckRequestTimeoutMs,
            );
            if (shuttingDown) {
              await terminateChild(child, childOutcome, config.shutdownTimeoutMs);
              return 0;
            }
            if (healthy) {
              consecutiveHealthFailures = 0;
              recovery = recordSuccessfulHealth(config, recovery, Date.now());
              await writeRecoveryAtomically(config, recovery, ownershipId);
              continue;
            }
            consecutiveHealthFailures += 1;
            if (
              consecutiveHealthFailures >=
              config.recovery.consecutiveHealthFailuresBeforeRestart
            ) {
              const shutdownTimedOut = await terminateChild(
                child,
                childOutcome,
                config.shutdownTimeoutMs,
              );
              failureReason = shutdownTimedOut ? 'shutdown-timeout' : 'healthcheck-failed';
            }
          }
        }
      }

      if (serverPid !== undefined) {
        await cleanupOwnedDocument(
          config.launchPlan.discoveryPath,
          (value) => decodeDiscovery(value, config.profileId),
          ownershipId,
          serverPid,
        );
      }
      child = undefined;
      childOutcome = undefined;
      serverPid = undefined;
      if (shuttingDown) return 0;
      recovery = registerFailure(config, recovery, failureReason ?? 'launcher-internal', Date.now());
      await writeRecoveryAtomically(config, recovery, ownershipId);
      if (recovery.circuitState === 'open') return 0;
    }
    return 0;
  } catch (error) {
    if (!shuttingDown && recovery !== undefined) {
      try {
        recovery = registerFailure(config, recovery, 'launcher-internal', Date.now());
        await writeRecoveryAtomically(config, recovery, ownershipId);
      } catch {
        // Preserve the original typed launcher failure when recovery persistence is unavailable.
      }
    }
    throw error;
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
