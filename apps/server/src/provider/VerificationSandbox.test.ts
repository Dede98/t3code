// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import type { AgentControlVerificationCheck } from "@t3tools/contracts";
import { afterEach, beforeEach, describe, expect, it } from "vite-plus/test";
import {
  probeVerificationCheckCapabilities,
  probeVerificationCheckExecutor,
  runVerificationSandboxCheck,
  verificationSandboxCheckConfigurationError,
} from "./VerificationSandbox.ts";

let root: string;
let worktree: string;
let temporary: string;
beforeEach(async () => {
  root = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-sandbox-test-")),
  );
  worktree = NodePath.join(root, "worktree");
  temporary = NodePath.join(root, "private");
  await NodeFSP.mkdir(worktree);
  await NodeFSP.mkdir(temporary);
});
afterEach(async () => {
  await NodeFSP.rm(root, { force: true, recursive: true });
});
const check = (
  code: string,
  overrides: Partial<AgentControlVerificationCheck> = {},
): AgentControlVerificationCheck => ({
  id: "http-tests",
  command: process.execPath,
  args: ["-e", code],
  cwd: ".",
  required: true,
  allowTemporaryFiles: false,
  timeoutMs: 5_000,
  resultFormat: "exit-code",
  ...overrides,
});
const run = (code: string, overrides: Partial<AgentControlVerificationCheck> = {}) =>
  runVerificationSandboxCheck({
    check: check(code, overrides),
    worktreePath: worktree,
    temporaryDirectory: temporary,
  });

// These assertions execute actual native sockets and filesystem calls under the NodeOS sandbox.
// oxlint-disable-next-line t3code/no-global-process-runtime -- These integration tests exercise the actual host kernel.
describe.skipIf(process.platform !== "darwin")("macOS verification sandbox", () => {
  it("keeps default checks offline, including loopback servers", async () => {
    const result = await run(`
      const assert = require('node:assert/strict');
      const server = require('node:net').createServer();
      server.on('error', e => { assert.equal(e.code, 'EPERM'); console.log('denied'); });
      server.listen(0, '127.0.0.1', () => { server.close(); process.exitCode = 1; });
    `);
    expect(result.exitCode).toBe(0);
    expect(result.stdout.trim()).toBe("denied");
    expect(result.network.access).toBe("none");
  });

  it("runs a real assigned-port HTTP server and native fetch while denying external destinations and redirects", async () => {
    const result = await run(
      `
      const assert = require('node:assert/strict');
      const http = require('node:http');
      (async () => {
        const server = http.createServer((req, res) => {
          if (req.url === '/redirect') { res.writeHead(302, {location:'http://192.0.2.1/'}); res.end(); }
          else res.end('integration passed');
        });
        await new Promise((resolve,reject) => { server.once('error',reject); server.listen({fd:Number(process.env.T3_VERIFICATION_LISTEN_FD)},resolve); });
        const denied = e => e.cause?.code === 'EPERM';
        try {
          assert.equal(await (await fetch(process.env.T3_VERIFICATION_URL)).text(), 'integration passed');
          await assert.rejects(fetch('http://192.0.2.1/'), denied);
          await assert.rejects(fetch(process.env.T3_VERIFICATION_URL + '/redirect'), denied);
          for (const [host,port] of [['127.0.0.1', Number(process.env.T3_VERIFICATION_PORT) - 1], ['2001:db8::1', 80]]) {
            await new Promise((resolve,reject) => {
              const socket=require('node:net').connect({host,port});
              socket.once('connect',()=>{socket.destroy();reject(new Error('socket escaped'))});
              socket.once('error',e=>{try{assert.equal(e.code,'EPERM');resolve()}catch(e){reject(e)}});
            });
          }
          console.log('HTTP and redirect boundaries verified');
        } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
      })().catch(e => {console.error(e);process.exitCode=1});
    `,
      { networkAccess: "loopback" },
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(result.network.port).toBeGreaterThan(0);
    expect(result.stdout).toContain("HTTP and redirect boundaries verified");
  });

  it("runs a native node:test HTTP check with TAP evidence", async () => {
    await NodeFSP.writeFile(
      NodePath.join(worktree, "http.test.cjs"),
      `
      const test=require('node:test'), assert=require('node:assert/strict'), http=require('node:http');
      test('native HTTP integration', async()=>{
        const server=http.createServer((req,res)=>res.end('verified'));
        await new Promise((resolve,reject)=>{server.once('error',reject);server.listen({fd:Number(process.env.T3_VERIFICATION_LISTEN_FD)},resolve)});
        try {assert.equal(await(await fetch(process.env.T3_VERIFICATION_URL)).text(),'verified')}
        finally {server.closeAllConnections();await new Promise(resolve=>server.close(resolve))}
      });
    `,
    );
    const result = await run("", {
      args: ["--test", "--test-isolation=none", "--test-reporter=tap", "http.test.cjs"],
      networkAccess: "loopback",
      resultFormat: "node-test",
    });
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toMatch(/^# pass 1$/m);
    expect(result.stdout).toMatch(/^# fail 0$/m);
  });

  it("denies wildcard binds and all additional listeners", async () => {
    const result = await run(
      `
      const assert = require('node:assert/strict');
      const net = require('node:net');
      const denied = (host, port) => new Promise((resolve,reject) => {
        const server=net.createServer();server.once('error',e=>{try{assert.equal(e.code,'EPERM');resolve()}catch(e){reject(e)}});
        server.listen(port,host,()=>{server.close();reject(new Error('bind escaped ' + host + ':' + port))});
      });
      Promise.all([denied('127.0.0.1',0),denied('::1',0),denied('0.0.0.0',Number(process.env.T3_VERIFICATION_PORT)),denied('::',Number(process.env.T3_VERIFICATION_PORT)),denied(undefined,process.env.TMPDIR+'/unix-socket')])
        .catch(e=>{console.error(e);process.exitCode=1});
    `,
      { networkAccess: "loopback" },
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
  });

  it("keeps the worktree and symlink targets read-only while allowing only private temporary writes", async () => {
    await NodeFSP.writeFile(NodePath.join(root, "outside"), "original");
    await NodeFSP.symlink(NodePath.join(root, "outside"), NodePath.join(worktree, "escape"));
    const result = await run(
      `
      const assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
      const denied=e=>e.code==='EPERM'||e.code==='EACCES';
      assert.throws(()=>fs.writeFileSync('new-file','bad'),denied);
      assert.throws(()=>fs.writeFileSync('escape','bad'),denied);
      assert.throws(()=>fs.writeFileSync(path.join(process.env.TMPDIR,'../escape'),'bad'),denied);
      fs.symlinkSync(path.resolve('escape'),path.join(process.env.TMPDIR,'symlink'));
      assert.throws(()=>fs.writeFileSync(path.join(process.env.TMPDIR,'symlink'),'bad'),denied);
      try {
        fs.linkSync(path.resolve('../outside'),path.join(process.env.TMPDIR,'hardlink'));
        assert.throws(()=>fs.writeFileSync(path.join(process.env.TMPDIR,'hardlink'),'bad'),denied);
      } catch (e) { assert.ok(denied(e)); }
      fs.writeFileSync(path.join(process.env.TMPDIR,'allowed'),'yes');
      assert.equal(fs.readFileSync(path.join(process.env.TMPDIR,'allowed'),'utf8'),'yes');
    `,
      { networkAccess: "loopback", allowTemporaryFiles: true },
    );
    expect(result.stderr).toBe("");
    expect(result.exitCode).toBe(0);
    expect(await NodeFSP.readFile(NodePath.join(root, "outside"), "utf8")).toBe("original");
    expect(await NodeFSP.readdir(temporary)).toEqual([]);
  });

  it("denies temporary writes without explicit permission", async () => {
    const result = await run(
      `
      const assert=require('node:assert/strict'),fs=require('node:fs');
      assert.throws(()=>fs.writeFileSync(process.env.TMPDIR+'/unapproved','bad'),e=>e.code==='EPERM');
    `,
      { networkAccess: "loopback" },
    );
    expect(result.exitCode).toBe(0);
  });

  it("rejects escaped cwd and overlapping temporary roots", async () => {
    await NodeFSP.symlink(temporary, NodePath.join(worktree, "escape"));
    await expect(run("", { cwd: "escape" })).rejects.toThrow("cwd escapes");
    await expect(
      runVerificationSandboxCheck({
        check: check("", { allowTemporaryFiles: true }),
        worktreePath: worktree,
        temporaryDirectory: worktree,
      }),
    ).rejects.toThrow("separate");
  });

  it("reports output truncation and timeout as unavailable", async () => {
    const output = await run("process.stdout.write('x'.repeat(100000))");
    expect(output.exitCode).toBe(125);
    expect(output.outputTruncated).toBe(true);
    expect(output.stdout.length).toBeLessThanOrEqual(32_768);
    const timeout = await run("setInterval(()=>{},1000)", { timeoutMs: 100 });
    expect(timeout.exitCode).toBe(125);
    expect(timeout.unavailableReason).toContain("timed out");
  });

  it("denies detached subprocesses in loopback checks", async () => {
    const result = await run(
      `
      const assert=require('node:assert/strict');
      assert.throws(()=>require('node:child_process').spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{detached:true}),e=>e.code==='EPERM');
      console.log('fork denied');
    `,
      { networkAccess: "loopback" },
    );
    expect(result.exitCode, result.stderr).toBe(0);
    expect(result.stdout.trim(), result.stderr).toBe("fork denied");
  });

  it("stops descendants when their check process exits", async () => {
    const result = await run(`
      const child=require('node:child_process').spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'inherit'});
      console.log(child.pid);child.unref();
    `);
    expect(result.exitCode).toBe(0);
    expect(() => process.kill(Number(result.stdout.trim()), 0)).toThrow();
  });

  it("probes the actual local HTTP sandbox before any provider turn", async () => {
    expect(
      await probeVerificationCheckCapabilities({ driverKind: "codex", networkAccess: "loopback" }),
    ).toEqual({ supported: true, reason: null });
  });

  it("reports missing Codex executors before any provider turn", async () => {
    const result = await probeVerificationCheckCapabilities({
      driverKind: "codex",
      codexBinaryPath: NodePath.join(root, "missing-codex"),
    });
    expect(result.supported).toBe(false);
    expect(result.reason).toContain("could not start");
  });

  it("rejects an unavailable configured Node executable before evaluating product code", async () => {
    const result = await probeVerificationCheckExecutor(
      check("", { command: NodePath.join(root, "missing", "node"), networkAccess: "loopback" }),
    );
    expect(result.supported).toBe(false);
    expect(result.reason).toContain("Node executable is unavailable");
  });

  it("probes actual Node features without requiring the future test file or cwd", async () => {
    expect(
      await probeVerificationCheckExecutor(
        check("", {
          args: ["--test", "--test-isolation=none", "future.test.cjs"],
          cwd: "future-directory",
          networkAccess: "loopback",
        }),
      ),
    ).toEqual({ supported: true, reason: null });
    const incompatible = NodePath.join(root, "node");
    await NodeFSP.writeFile(
      incompatible,
      "#!/bin/sh\nprintf 'bad option: --test-isolation=none' >&2\nexit 9\n",
      { mode: 0o700 },
    );
    const result = await probeVerificationCheckExecutor(
      check("", {
        command: incompatible,
        args: ["--test", "--test-isolation=none", "future.test.cjs"],
        networkAccess: "loopback",
      }),
    );
    expect(result.supported).toBe(false);
    expect(result.reason).toContain("lacks the required runtime capabilities");
  });

  it("rejects Node launcher shims that require subprocesses under the real loopback restrictions", async () => {
    const launcher = NodePath.join(root, "node");
    await NodeFSP.writeFile(
      launcher,
      `#!${process.execPath}\nconst result = require('node:child_process').spawnSync(process.execPath, process.argv.slice(2), {stdio:'inherit'}); if (result.error) console.error(result.error.code); process.exit(result.status ?? 125);\n`,
      { mode: 0o700 },
    );
    const result = await probeVerificationCheckExecutor(
      check("", { command: launcher, networkAccess: "loopback" }),
    );
    expect(result.supported).toBe(false);
    expect(result.reason).toContain("EPERM");
  });
});

it("explicitly rejects unsupported providers", async () => {
  const result = await probeVerificationCheckCapabilities({
    driverKind: "claude",
    networkAccess: "loopback",
  });
  expect(result.supported).toBe(false);
  expect(result.reason).toContain("select Codex");
});

it("rejects incompatible local HTTP runners before execution", () => {
  expect(
    verificationSandboxCheckConfigurationError(
      check("", { command: "npm", networkAccess: "loopback" }),
    ),
  ).toContain("direct Node");
  expect(
    verificationSandboxCheckConfigurationError(
      check("", { args: ["--test", "test.cjs"], networkAccess: "loopback" }),
    ),
  ).toContain("--test-isolation=none");
  expect(
    verificationSandboxCheckConfigurationError(
      check("", { args: ["--run", "test"], networkAccess: "loopback" }),
    ),
  ).toContain("cannot use node --run");
  expect(
    verificationSandboxCheckConfigurationError(
      check("", {
        args: ["--test", "--test-isolation=none", "test.cjs"],
        networkAccess: "loopback",
      }),
    ),
  ).toBeNull();
  expect(verificationSandboxCheckConfigurationError(check("", { command: "npm" }))).toBeNull();
  expect(
    verificationSandboxCheckConfigurationError(
      check("", { command: "./node", networkAccess: "loopback" }),
    ),
  ).toContain("relative executables");
  expect(
    verificationSandboxCheckConfigurationError(
      check("", {
        args: ["--test", "--test-isolation=none", "--test-isolation=process", "test.cjs"],
        networkAccess: "loopback",
      }),
    ),
  ).toContain("--test-isolation=none");
});
