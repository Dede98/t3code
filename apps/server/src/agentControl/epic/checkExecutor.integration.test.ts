// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import { ServerSettings, type AgentControlVerificationCheck } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { afterAll, beforeAll, describe, expect, vi } from "vite-plus/test";

import { ServerSettingsService } from "../../serverSettings.ts";
import { EpicCheckExecutor, EpicCheckExecutorLive } from "./results.ts";

// Uses a local command-only Codex app-server and native sandbox, with no model turns.
const enabled =
  process.env.T3CODE_TEST_CODEX_SANDBOX === "1" && HostProcessPlatform.defaultValue() === "darwin";
const decodeSettings = Schema.decodeUnknownSync(ServerSettings);
describe.skipIf(!enabled)("Epic final check executor with real Codex sandbox", () => {
  let root: string;
  let cwd: string;
  let home: string;
  const check: AgentControlVerificationCheck = {
    id: "common-check",
    command: process.execPath,
    args: ["--test", "--test-reporter=tap", "check.test.cjs"],
    cwd: ".",
    required: true,
    timeoutMs: 10_000,
    allowTemporaryFiles: false,
    resultFormat: "node-test",
  };
  const execute = (
    settings: ServerSettings,
    providerInstanceId = "codex",
    allowTemporaryFiles = false,
    overrides: Partial<Pick<AgentControlVerificationCheck, "networkAccess" | "args">> = {},
  ) =>
    Effect.gen(function* () {
      const executor = yield* EpicCheckExecutor;
      return yield* executor.execute({
        cwd,
        providerInstanceId,
        checks: [{ ...check, allowTemporaryFiles, ...overrides }],
        checkId: check.id,
      });
    }).pipe(
      Effect.provide(
        EpicCheckExecutorLive.pipe(
          Layer.provide(
            Layer.mock(ServerSettingsService)({ getSettings: Effect.succeed(settings) }),
          ),
        ),
      ),
      Effect.provide(NodeServices.layer),
    );
  beforeAll(() => {
    root = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-epic-executor-"));
    cwd = NodePath.join(root, "worktree");
    home = NodePath.join(root, "codex-home");
    NodeFS.mkdirSync(cwd);
    NodeFS.mkdirSync(home);
    NodeFS.writeFileSync(NodePath.join(cwd, "protected.txt"), "accepted common result");
    vi.stubEnv("CODEX_HOME", home);
  });
  afterAll(() => {
    vi.unstubAllEnvs();
    NodeFS.rmSync(root, { recursive: true, force: true });
  });

  it.effect("hydrates the default verifier from decoded legacy settings", () =>
    Effect.gen(function* () {
      const settings = decodeSettings({});
      expect(Object.keys(settings.providerInstances)).toEqual([]);
      NodeFS.writeFileSync(
        NodePath.join(cwd, "check.test.cjs"),
        `
      const test = require('node:test');
      const assert = require('node:assert/strict');
      test('checks accepted source', () => assert.equal(require('node:fs').readFileSync('protected.txt', 'utf8'), 'accepted common result'));
    `,
      );
      const result = yield* execute(settings);
      expect(result.exitCode, result.stderr + result.stdout).toBe(0);
      expect(result.stdout).toMatch(/# pass 1/);
    }),
  );

  it.effect("honors an explicit selected instance and its environment over the legacy config", () =>
    Effect.gen(function* () {
      const settings = decodeSettings({
        providers: { codex: { binaryPath: NodePath.join(root, "legacy-must-not-run") } },
        providerInstances: {
          "epic-verifier": {
            driver: "codex",
            enabled: true,
            config: { binaryPath: "codex", homePath: home },
            environment: [{ name: "T3AUTO_EPIC_EXECUTOR_TEST", value: "selected-instance" }],
          },
        },
      });
      NodeFS.writeFileSync(
        NodePath.join(cwd, "check.test.cjs"),
        `
      require('node:test')('selected environment', () => require('node:assert/strict').equal(process.env.T3AUTO_EPIC_EXECUTOR_TEST, 'selected-instance'));
    `,
      );
      const result = yield* execute(settings, "epic-verifier");
      expect(result.exitCode, result.stderr + result.stdout).toBe(0);
    }),
  );

  it.effect.each([false, true])(
    "denies source writes with temporary fixtures enabled=%s",
    (allowTemporaryFiles) =>
      Effect.gen(function* () {
        NodeFS.writeFileSync(
          NodePath.join(cwd, "check.test.cjs"),
          `
      const fs = require('node:fs');
      const assert = require('node:assert/strict');
      const test = require('node:test');
      test('source stays read-only', () => assert.throws(() => fs.writeFileSync('protected.txt', 'changed'), /EPERM|EACCES/));
      ${allowTemporaryFiles ? "test('private temporary fixture', () => { const file = require('node:path').join(require('node:os').tmpdir(), 'epic-fixture'); fs.writeFileSync(file, 'ok'); assert.equal(fs.readFileSync(file, 'utf8'), 'ok'); });" : ""}
    `,
        );
        const result = yield* execute(decodeSettings({}), "codex", allowTemporaryFiles);
        expect(result.exitCode, result.stderr + result.stdout).toBe(0);
        expect(NodeFS.readFileSync(NodePath.join(cwd, "protected.txt"), "utf8")).toBe(
          "accepted common result",
        );
      }),
  );

  it.effect.each([false, true])(
    "verifies native loopback HTTP with the shared executor and temporary fixtures enabled=%s",
    (allowTemporaryFiles) =>
      Effect.gen(function* () {
        NodeFS.writeFileSync(
          NodePath.join(cwd, "check.test.cjs"),
          `
      const fs = require('node:fs');
      const http = require('node:http');
      const assert = require('node:assert/strict');
      const test = require('node:test');
      test('accepted epic supports local HTTP without writable source', async () => {
        assert.throws(() => fs.writeFileSync('protected.txt', 'changed'), /EPERM|EACCES/);
        const fixture = require('node:path').join(require('node:os').tmpdir(), 'epic-http-fixture');
        ${allowTemporaryFiles ? "fs.writeFileSync(fixture, 'private fixture'); assert.equal(fs.readFileSync(fixture, 'utf8'), 'private fixture');" : "assert.throws(() => fs.writeFileSync(fixture, 'unapproved'), /EPERM|EACCES/);"}
        const server = http.createServer((request, response) => response.end('accepted common result'));
        await new Promise((resolve, reject) => {
          server.once('error', reject);
          server.listen({ fd: Number(process.env.T3_VERIFICATION_LISTEN_FD) }, resolve);
        });
        try {
          const address = server.address();
          assert.equal(address.address, '127.0.0.1');
          assert.equal(address.port, Number(process.env.T3_VERIFICATION_PORT));
          const response = await fetch(process.env.T3_VERIFICATION_URL);
          assert.equal(response.status, 200);
          assert.equal(await response.text(), 'accepted common result');
        } finally {
          server.closeAllConnections();
          await new Promise(resolve => server.close(resolve));
        }
      });
    `,
        );
        const result = yield* execute(decodeSettings({}), "codex", allowTemporaryFiles, {
          networkAccess: "loopback",
          args: ["--test", "--test-isolation=none", "--test-reporter=tap", "check.test.cjs"],
        });
        expect(result.exitCode, result.stderr + result.stdout).toBe(0);
        expect(result.stdout).toMatch(/^# pass 1$/m);
        expect(result.stdout).toMatch(/^# fail 0$/m);
        expect(NodeFS.readFileSync(NodePath.join(cwd, "protected.txt"), "utf8")).toBe(
          "accepted common result",
        );
      }),
  );

  it.effect("rejects an explicitly disabled default instance before spawning", () =>
    Effect.gen(function* () {
      const settings = decodeSettings({
        providerInstances: { codex: { driver: "codex", enabled: false } },
      });
      const error = yield* Effect.gen(function* () {
        const executor = yield* EpicCheckExecutor;
        return yield* executor
          .execute({ cwd, providerInstanceId: "codex", checks: [check], checkId: check.id })
          .pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          EpicCheckExecutorLive.pipe(
            Layer.provide(
              Layer.merge(
                Layer.mock(ServerSettingsService)({ getSettings: Effect.succeed(settings) }),
                Layer.mock(ChildProcessSpawner.ChildProcessSpawner)({
                  spawn: () => Effect.die("Disabled verifier must not spawn a process"),
                }),
              ),
            ),
          ),
        ),
      );
      expect(error.code).toBe("epic-result-unavailable");
    }),
  );
});
