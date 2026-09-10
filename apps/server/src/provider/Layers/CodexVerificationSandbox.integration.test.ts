// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import type { AgentControlVerificationCheck } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as CodexClient from "effect-codex-app-server/client";
import { afterAll, beforeAll, describe, expect } from "vite-plus/test";
import { buildCodexInitializeParams } from "./CodexProvider.ts";
import { verificationCheckParams } from "../CodexVerificationChecks.ts";

// Opt in on a host with Codex and its native sandbox installed. No model/API calls.
const enabled =
  process.env.T3CODE_TEST_CODEX_SANDBOX === "1" && HostProcessPlatform.defaultValue() === "darwin";
describe.skipIf(!enabled)("Codex verification command/exec read-only sandbox", () => {
  let cwd: string;
  let outside: string;
  let temporary: string;
  const projectCheck: AgentControlVerificationCheck = {
    id: "scoped-tests",
    command: process.execPath,
    args: ["--test", "--test-reporter=tap", "check.test.cjs"],
    cwd: ".",
    required: true,
    timeoutMs: 10_000,
    allowTemporaryFiles: false,
    resultFormat: "node-test",
  };
  const run = (check: string, allowTemporaryFiles = false) =>
    Effect.gen(function* () {
      const params = yield* verificationCheckParams(
        { check },
        cwd,
        [{ ...projectCheck, allowTemporaryFiles }],
        temporary,
      );
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const child = yield* spawner.spawn(
        ChildProcess.make("codex", ["app-server"], {
          cwd: allowTemporaryFiles ? temporary : cwd,
          env: { CODEX_HOME: NodePath.join(cwd, "codex-home") },
          extendEnv: true,
          forceKillAfter: "2 seconds",
        }),
      );
      const context = yield* Layer.build(CodexClient.layerChildProcess(child));
      const client = yield* CodexClient.CodexAppServerClient.pipe(Effect.provide(context));
      yield* client.request("initialize", buildCodexInitializeParams());
      yield* client.notify("initialized", undefined);
      return yield* client.request("command/exec", params);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer));
  beforeAll(() => {
    cwd = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-verification-sandbox-"));
    temporary = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-verification-check-tmp-"));
    NodeFS.mkdirSync(NodePath.join(cwd, "codex-home"));
    NodeFS.writeFileSync(NodePath.join(cwd, "protected.txt"), "unchanged");
    outside = `${cwd}-outside`;
    NodeFS.writeFileSync(outside, "unchanged");
    NodeFS.symlinkSync(outside, NodePath.join(cwd, "escape.txt"));
    expect(NodeChildProcess.spawnSync("git", ["init", "-q"], { cwd }).status).toBe(0);
  });
  afterAll(() => {
    NodeFS.rmSync(cwd, { recursive: true, force: true });
    NodeFS.rmSync(outside, { force: true });
    NodeFS.rmSync(temporary, { recursive: true, force: true });
  });
  it.effect.each(["scoped-tests", "git-status", "git-diff-check", "git-diff"])(
    "allows the registered check: %s",
    (check) =>
      Effect.gen(function* () {
        NodeFS.writeFileSync(
          NodePath.join(cwd, "check.test.cjs"),
          'require("node:test")("check", () => require("node:assert/strict").equal(2 + 2, 4));',
        );
        const result = yield* run(check);
        expect(result.exitCode, result.stderr).toBe(0);
      }),
  );
  it.effect("allows temporary fixtures without allowing worktree writes or network", () =>
    Effect.gen(function* () {
      NodeFS.writeFileSync(
        NodePath.join(cwd, "check.test.cjs"),
        `
        const fs = require('node:fs');
        const assert = require('node:assert/strict');
        const path = require('node:path');
        const test = require('node:test');
        test('temporary fixture', () => {
          const file = path.join(require('node:os').tmpdir(), 'fixture');
          fs.writeFileSync(file, 'temporary');
          assert.equal(fs.readFileSync(file, 'utf8'), 'temporary');
        });
        test('worktree stays read-only', () => {
          assert.throws(() => fs.writeFileSync('protected.txt', 'changed'), /EPERM|EACCES/);
          assert.throws(() => fs.writeFileSync('escape.txt', 'changed'), /EPERM|EACCES/);
        });
        test('network stays disabled', async () => {
          await new Promise((resolve, reject) => {
            const server = require('node:net').createServer();
            server.once('error', (error) => /EPERM|EACCES/.test(error.code) ? resolve() : reject(error));
            server.listen(0, '127.0.0.1', () => server.close(() => reject(new Error('network was allowed'))));
          });
        });
      `,
      );
      const result = yield* run("scoped-tests", true);
      expect(result.exitCode, result.stderr + result.stdout).toBe(0);
      expect(NodeFS.readFileSync(NodePath.join(cwd, "protected.txt"), "utf8")).toBe("unchanged");
      expect(NodeFS.readFileSync(outside, "utf8")).toBe("unchanged");
      expect(NodeFS.readFileSync(NodePath.join(temporary, "fixture"), "utf8")).toBe("temporary");
    }),
  );
  it.effect.each([
    'require("node:fs").writeFileSync("protected.txt", "changed")',
    'require("node:fs").unlinkSync("protected.txt")',
    'require("node:fs").writeFileSync("escape.txt", "changed")',
    'require("node:net").createServer().listen(0, "127.0.0.1")',
    'require("node:net").createConnection(9, "127.0.0.1")',
    'require("node:child_process").execFileSync("git", ["add", "protected.txt"])',
  ])("does not escalate when authorized test code attempts %s", (body) =>
    Effect.gen(function* () {
      NodeFS.writeFileSync(NodePath.join(cwd, "check.test.cjs"), body);
      const result = yield* run("scoped-tests");
      expect(result.exitCode).not.toBe(0);
      expect(result.stderr + result.stdout).toMatch(/not permitted|EPERM|Permission denied/i);
      expect(NodeFS.readFileSync(NodePath.join(cwd, "protected.txt"), "utf8")).toBe("unchanged");
      expect(NodeFS.readFileSync(outside, "utf8")).toBe("unchanged");
    }),
  );
});
