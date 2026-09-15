// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics globalTimers:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { AgentControlVerificationCheck } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

// oxlint-disable-next-line t3code/no-global-process-runtime -- This native sandbox boundary must probe the real host, outside an Effect runtime.
const hostPlatform = process.platform;

const OUTPUT_LIMIT = 32_768;
const EXECUTOR = "/usr/bin/sandbox-exec";

const isWithin = (root: string, path: string) => {
  const relative = NodePath.relative(root, path);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
};

/**
 * Seatbelt applies to every descendant and socket connect, including redirects.
 * An explicit bind denial is essential: network-inbound otherwise also permits
 * wildcard binds. Only the controller-prebound IPv4 loopback descriptor can listen.
 */
const profile = (port: number | null, temporary: string | null) => `
(version 1)
(deny default)
(deny network-bind)
(allow process-exec)
${port === null ? "(allow process-fork)" : "(deny process-fork)"}
(allow signal (target same-sandbox))
(allow process-info* (target same-sandbox))
(allow file-read*)
(allow sysctl-read)
(allow file-write-data (require-all (literal "/dev/null") (vnode-type CHARACTER-DEVICE)))
${temporary ? `(allow file-write* (subpath ${JSON.stringify(temporary)}))` : ""}
${
  port === null
    ? ""
    : `
(allow network-inbound (local tcp "localhost:${port}"))
(allow network-outbound (remote tcp "localhost:${port}"))`
}
`;

interface LoopbackListener {
  readonly port: number;
  readonly descriptor: number;
  readonly release: () => void;
}

const reserveLoopbackListener = () =>
  new Promise<LoopbackListener>((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen({ host: "127.0.0.1", port: 0, exclusive: true }, () => {
      const address = server.address();
      // Node exposes no public descriptor accessor for servers. The descriptor is only
      // passed through spawn's stdio duplication; its ownership stays with this server.
      const descriptor = (server as NodeNet.Server & { _handle?: { fd?: number } })._handle?.fd;
      if (!address || typeof address === "string" || descriptor === undefined || descriptor < 0) {
        server.close();
        reject(new Error("Could not reserve the verification loopback listener"));
        return;
      }
      let released = false;
      resolve({
        port: address.port,
        descriptor,
        release: () => {
          if (!released) {
            released = true;
            server.close();
          }
        },
      });
    });
  });

export interface VerificationSandboxResult {
  readonly exitCode: number;
  readonly stdout: string;
  readonly stderr: string;
  readonly outputTruncated: boolean;
  readonly unavailableReason: string | null;
  readonly network: {
    readonly access: "none" | "loopback";
    readonly host: "127.0.0.1";
    readonly port: number | null;
  };
}

const execute = (options: {
  command: string;
  args: ReadonlyArray<string>;
  cwd: string;
  temporary: string | null;
  listener: LoopbackListener | null;
  timeoutMs: number;
  signal?: AbortSignal;
}): Promise<VerificationSandboxResult> =>
  new Promise((resolve) => {
    const port = options.listener?.port ?? null;
    const network = {
      access: port === null ? ("none" as const) : ("loopback" as const),
      host: "127.0.0.1" as const,
      port,
    };
    if (options.signal?.aborted) {
      resolve({
        exitCode: 125,
        stdout: "",
        stderr: "T3_CHECK_UNAVAILABLE: Verification cancelled",
        outputTruncated: false,
        unavailableReason: "Verification cancelled",
        network,
      });
      return;
    }
    const child = NodeChildProcess.spawn(
      EXECUTOR,
      ["-p", profile(port, options.temporary), options.command, ...options.args],
      {
        cwd: options.cwd,
        detached: true,
        shell: false,
        stdio: [
          "ignore",
          "pipe",
          "pipe",
          ...(options.listener ? [options.listener.descriptor] : []),
        ],
        // Avoid inherited proxies, runtime injection, credentials and package-runner configuration.
        env: {
          PATH: process.env.PATH,
          LANG: "en_US.UTF-8",
          HOME: options.temporary ?? options.cwd,
          TMPDIR: options.temporary ?? options.cwd,
          TMP: options.temporary ?? options.cwd,
          TEMP: options.temporary ?? options.cwd,
          ...(port === null
            ? {}
            : {
                T3_VERIFICATION_HOST: "127.0.0.1",
                T3_VERIFICATION_PORT: String(port),
                T3_VERIFICATION_LISTEN_FD: "3",
                T3_VERIFICATION_URL: `http://127.0.0.1:${port}`,
              }),
        },
      },
    );
    // spawn has duplicated fd 3; close the parent copy before it can accept clients.
    options.listener?.release();
    let stdout = "";
    let stderr = "";
    let outputBytes = 0;
    let outputTruncated = false;
    let unavailableReason: string | null = null;
    const stop = () => {
      if (!child.pid) return;
      // Only this process group, captured at spawn, is ours to stop.
      try {
        process.kill(-child.pid, "SIGKILL");
      } catch {
        /* It may already have exited. */
      }
    };
    const cancel = () => {
      unavailableReason = "Verification cancelled";
      stop();
    };
    const timer = setTimeout(() => {
      unavailableReason = "Verification check timed out";
      stop();
    }, options.timeoutMs);
    options.signal?.addEventListener("abort", cancel, { once: true });
    const collect = (data: Buffer, stream: "stdout" | "stderr") => {
      const remaining = Math.max(0, OUTPUT_LIMIT - outputBytes);
      const text = data.subarray(0, remaining).toString("utf8");
      if (stream === "stdout") stdout += text;
      else stderr += text;
      outputBytes += data.length;
      if (outputBytes > OUTPUT_LIMIT) {
        outputTruncated = true;
        unavailableReason = "Verification output exceeded 32768 bytes; result is incomplete";
        stop();
      }
    };
    child.stdout!.on("data", (data: Buffer) => collect(data, "stdout"));
    child.stderr!.on("data", (data: Buffer) => collect(data, "stderr"));
    child.once("error", (error) => {
      unavailableReason = `Verification sandbox could not start: ${error.message}`;
    });
    child.once("exit", stop);
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", cancel);
      stop();
      if (signal && !unavailableReason) unavailableReason = `Verification terminated by ${signal}`;
      if (/sandbox-exec:|sandbox_apply:|T3_CHECK_UNAVAILABLE:|EADDRINUSE/.test(stderr + stdout)) {
        unavailableReason ??= "Verification sandbox or assigned loopback port is unavailable";
      }
      resolve({
        exitCode: unavailableReason ? 125 : (code ?? 125),
        stdout,
        stderr: unavailableReason
          ? `${stderr}\nT3_CHECK_UNAVAILABLE: ${unavailableReason}\n`
          : stderr,
        outputTruncated,
        unavailableReason,
        network,
      });
    });
  });

/** Reject known incompatible runners before Planning, rather than repairing their code. */
export const verificationSandboxCheckConfigurationError = (
  check: AgentControlVerificationCheck,
): string | null => {
  if (check.networkAccess !== "loopback") return null;
  if (
    check.command !== process.execPath &&
    !["node", "node.exe"].includes(NodePath.basename(check.command))
  ) {
    return "Local HTTP checks require a direct Node executable; package runners and subprocesses are unavailable";
  }
  const end = check.args.indexOf("--");
  const options = end === -1 ? check.args : check.args.slice(0, end);
  if (options.some((arg) => arg === "--run" || arg.startsWith("--run="))) {
    return "Local HTTP checks cannot use node --run; invoke the test file directly";
  }
  if (options.includes("--test") && !options.includes("--test-isolation=none")) {
    return "Local HTTP node:test checks require --test-isolation=none to preserve the assigned listener and avoid subprocesses";
  }
  return null;
};

/**
 * Called only after the adapter has checked the manifest ID and current stage authority.
 * Loopback checks run directly (no shell/package-runner subprocesses): bind HTTP to
 * T3_VERIFICATION_LISTEN_FD and fetch T3_VERIFICATION_URL. Native node:test uses
 * --test-isolation=none so fd 3 remains the prebound listener. Denying forks also
 * prevents detached descendants from surviving cancellation or losing the listener.
 */
export const runVerificationSandboxCheck = async (input: {
  check: AgentControlVerificationCheck;
  worktreePath: string;
  temporaryDirectory?: string;
  signal?: AbortSignal;
}): Promise<VerificationSandboxResult> => {
  const configurationError = verificationSandboxCheckConfigurationError(input.check);
  if (configurationError) throw new Error(configurationError);
  if (hostPlatform !== "darwin")
    throw new Error(
      "Port-restricted verification requires macOS Seatbelt; this platform is unsupported",
    );
  const root = await NodeFSP.realpath(input.worktreePath);
  const cwd = await NodeFSP.realpath(NodePath.resolve(root, input.check.cwd));
  if (!isWithin(root, cwd)) throw new Error("Verification cwd escapes the controlled worktree");
  let temporary: string | null = null;
  if (input.check.allowTemporaryFiles) {
    if (!input.temporaryDirectory)
      throw new Error("Verification requires a controller-owned temporary directory");
    const parent = await NodeFSP.realpath(input.temporaryDirectory);
    if (isWithin(root, parent) || isWithin(parent, root))
      throw new Error("Verification temporary directory must be separate from the worktree");
    temporary = await NodeFSP.mkdtemp(NodePath.join(parent, "check-"));
    await NodeFSP.chmod(temporary, 0o700);
  }
  let listener: LoopbackListener | null = null;
  try {
    listener = input.check.networkAccess === "loopback" ? await reserveLoopbackListener() : null;
    return await execute({
      command: input.check.command,
      args: input.check.args,
      cwd,
      temporary,
      listener,
      timeoutMs: input.check.timeoutMs,
      ...(input.signal ? { signal: input.signal } : {}),
    });
  } finally {
    listener?.release();
    if (temporary) await NodeFSP.rm(temporary, { recursive: true, force: true });
  }
};

const CAPABILITY_PROBE = `
const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const net = require('node:net');
(async () => {
  assert.throws(() => fs.writeFileSync('forbidden', 'no'), e => e.code === 'EPERM' || e.code === 'EACCES');
  fs.writeFileSync(process.env.TMPDIR + '/allowed', 'yes');
  await new Promise((resolve, reject) => {
    const socket = net.connect({host:'192.0.2.1',port:80});
    socket.on('connect', () => { socket.destroy(); reject(new Error('External connection was allowed')); });
    socket.on('error', e => e.code === 'EPERM' || e.code === 'EACCES' ? resolve() : reject(e));
  });
  await new Promise((resolve,reject) => {
    const extra = net.createServer();
    extra.once('error', e => e.code === 'EPERM' || e.code === 'EACCES' ? resolve() : reject(e));
    extra.listen(Number(process.env.T3_VERIFICATION_PORT), '0.0.0.0', () => { extra.close(); reject(new Error('Wildcard binding was allowed')); });
  });
  const server = http.createServer((req, res) => res.end('t3-loopback-probe'));
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen({fd:Number(process.env.T3_VERIFICATION_LISTEN_FD)}, resolve); });
  try { assert.equal(await (await fetch(process.env.T3_VERIFICATION_URL)).text(), 't3-loopback-probe'); }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
})().catch(e => { console.error(e); process.exitCode = 125; });
`;

const OFFLINE_PROBE = `
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
assert.throws(() => fs.writeFileSync('forbidden', 'no'), e => e.code === 'EPERM' || e.code === 'EACCES');
const server = net.createServer();
server.on('error', e => { assert.ok(e.code === 'EPERM' || e.code === 'EACCES'); console.log('T3_OFFLINE_PROBE_OK'); });
server.listen(0, '127.0.0.1', () => { server.close(); process.exitCode = 125; });
`;
const decodeEnvelope = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.optional(Schema.Int),
      result: Schema.optional(Schema.Unknown),
      error: Schema.optional(Schema.Unknown),
    }),
  ),
);
const decodeCommandResult = Schema.decodeUnknownOption(
  Schema.Struct({
    exitCode: Schema.Int,
    stdout: Schema.String,
    stderr: Schema.String,
  }),
);

const probeCodexOffline = (cwd: string, codexBinaryPath: string) =>
  new Promise<{ supported: boolean; reason: string | null }>((resolve) => {
    const child = NodeChildProcess.spawn(codexBinaryPath, ["app-server", "--listen", "stdio://"], {
      cwd,
      detached: true,
      stdio: ["pipe", "pipe", "pipe"],
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        CODEX_HOME: NodePath.join(cwd, "codex-home"),
      },
    });
    let buffer = "";
    let receivedBytes = 0;
    let verdict = {
      supported: false,
      reason: "Codex command/exec sandbox probe did not complete" as string | null,
    };
    const stop = () => {
      if (child.pid) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          /* Already exited. */
        }
      }
    };
    const timer = setTimeout(() => {
      verdict.reason = "Codex command/exec sandbox probe timed out";
      stop();
    }, 10_000);
    const send = (request: object) => child.stdin.write(`${JSON.stringify(request)}\n`);
    child.stdin.on("error", () => {});
    child.stderr!.on("data", (data: Buffer) => {
      receivedBytes += data.length;
      if (receivedBytes > OUTPUT_LIMIT) stop();
    });
    child.stdout!.on("data", (data: Buffer) => {
      receivedBytes += data.length;
      if (receivedBytes > OUTPUT_LIMIT) {
        stop();
        return;
      }
      buffer += data.toString("utf8");
      let newline: number;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const decoded = decodeEnvelope(buffer.slice(0, newline));
        buffer = buffer.slice(newline + 1);
        if (decoded._tag === "None") continue;
        const response = decoded.value;
        if (response.id === 1) {
          if (response.error) {
            verdict.reason = "Codex command/exec initialization is unavailable";
            stop();
            return;
          }
          send({ method: "initialized" });
          send({
            id: 2,
            method: "command/exec",
            params: {
              command: [process.execPath, "-e", OFFLINE_PROBE],
              cwd,
              sandboxPolicy: { type: "readOnly", networkAccess: false },
              timeoutMs: 5_000,
              outputBytesCap: 4096,
            },
          });
        }
        if (response.id === 2) {
          const result = decodeCommandResult(response.result);
          verdict =
            result._tag === "Some" &&
            result.value.exitCode === 0 &&
            result.value.stdout.trim() === "T3_OFFLINE_PROBE_OK"
              ? { supported: true, reason: null }
              : {
                  supported: false,
                  reason: `Codex read-only/offline command/exec sandbox is unavailable${result._tag === "Some" ? `: ${result.value.stderr.slice(0, 500)}` : ""}`,
                };
          stop();
        }
      }
    });
    child.once("error", (error) => {
      verdict.reason = `Codex sandbox executor could not start: ${error.message}`;
    });
    child.once("close", () => {
      clearTimeout(timer);
      stop();
      resolve(verdict);
    });
    send({
      id: 1,
      method: "initialize",
      params: { clientInfo: { name: "t3-verification-preflight", version: "1" } },
    });
  });

/** Run a real kernel-enforced probe before scheduling any provider turn. */
export const probeVerificationCheckCapabilities = async (input: {
  driverKind: string;
  networkAccess?: "none" | "loopback";
  codexBinaryPath?: string;
}): Promise<{ supported: boolean; reason: string | null }> => {
  if (input.driverKind !== "codex")
    return {
      supported: false,
      reason: `Verification checks are not supported by provider ${input.driverKind}; select Codex`,
    };
  if (input.networkAccess !== "loopback") {
    if (hostPlatform !== "darwin" && hostPlatform !== "linux") {
      return {
        supported: false,
        reason: "Read-only network-disabled verification is supported on macOS and Linux only",
      };
    }
    let directory: string | null = null;
    try {
      directory = await NodeFSP.realpath(
        await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-verification-offline-probe-")),
      );
      await NodeFSP.mkdir(NodePath.join(directory, "codex-home"), { mode: 0o700 });
      return await probeCodexOffline(directory, input.codexBinaryPath ?? "codex");
    } catch (error) {
      return {
        supported: false,
        reason: `Codex sandbox probe is unavailable: ${error instanceof Error ? error.message : String(error)}`,
      };
    } finally {
      if (directory) await NodeFSP.rm(directory, { recursive: true, force: true });
    }
  }
  if (hostPlatform !== "darwin")
    return {
      supported: false,
      reason:
        "Local HTTP verification requires macOS Seatbelt; Linux and Windows do not yet have a port-restricted executor",
    };
  const providerProbe = await probeVerificationCheckCapabilities({
    driverKind: input.driverKind,
    networkAccess: "none",
    ...(input.codexBinaryPath ? { codexBinaryPath: input.codexBinaryPath } : {}),
  });
  if (!providerProbe.supported) return providerProbe;
  let directory: string | null = null;
  let listener: LoopbackListener | null = null;
  try {
    await NodeFSP.access(EXECUTOR, NodeFSP.constants.X_OK);
    directory = await NodeFSP.realpath(
      await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-verification-probe-")),
    );
    const cwd = NodePath.join(directory, "readonly");
    const temporary = NodePath.join(directory, "temporary");
    await NodeFSP.mkdir(cwd);
    await NodeFSP.mkdir(temporary, { mode: 0o700 });
    listener = await reserveLoopbackListener();
    const result = await execute({
      command: process.execPath,
      args: ["-e", CAPABILITY_PROBE],
      cwd,
      temporary,
      listener,
      timeoutMs: 10_000,
    });
    return result.exitCode === 0
      ? { supported: true, reason: null }
      : {
          supported: false,
          reason: `Local HTTP verification sandbox probe failed: ${result.unavailableReason ?? result.stderr.slice(0, 500)}`,
        };
  } catch (error) {
    return {
      supported: false,
      reason: `Local HTTP verification sandbox is unavailable: ${error instanceof Error ? error.message : String(error)}`,
    };
  } finally {
    listener?.release();
    if (directory) await NodeFSP.rm(directory, { recursive: true, force: true });
  }
};
