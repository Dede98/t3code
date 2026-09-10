// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeChildProcess from "node:child_process";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeUtil from "node:util";
import type { AgentControlVerificationCheck } from "@t3tools/contracts";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { afterEach, assert, beforeEach, describe } from "vite-plus/test";
import {
  classifyVerificationCheckResult,
  createCodexVerificationTool,
  verificationCheckParams,
} from "./CodexVerificationChecks.ts";

const exec = NodeUtil.promisify(NodeChildProcess.execFile);
let worktree: string;
let temporary: string;
const check: AgentControlVerificationCheck = {
  id: "scoped-tests",
  command: process.execPath,
  args: ["--test", "--test-reporter=tap", "selected.test.cjs"],
  cwd: "package",
  required: true,
  timeoutMs: 10_000,
  allowTemporaryFiles: false,
  resultFormat: "node-test",
};
beforeEach(async () => {
  worktree = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-verification-worktree-")),
  );
  temporary = await NodeFSP.realpath(
    await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-verification-tmp-")),
  );
  await NodeFSP.mkdir(NodePath.join(worktree, "package"));
});
afterEach(async () => {
  await Promise.all([
    NodeFSP.rm(worktree, { recursive: true, force: true }),
    NodeFSP.rm(temporary, { recursive: true, force: true }),
  ]);
});

describe("controller-owned verification commands", () => {
  it.effect("runs only the selected file in the configured project directory", () =>
    Effect.gen(function* () {
      yield* Effect.promise(() =>
        Promise.all([
          NodeFSP.writeFile(
            NodePath.join(worktree, "package", "selected.test.cjs"),
            "require('node:test')('selected scope', () => {});",
          ),
          NodeFSP.writeFile(
            NodePath.join(worktree, "package", "unrelated.test.cjs"),
            "throw new Error('unrelated test must not run');",
          ),
        ]),
      );
      const params = yield* verificationCheckParams({ check: check.id }, worktree, [check]);
      assert.deepStrictEqual(params.command, [process.execPath, ...check.args]);
      assert.equal(params.cwd, NodePath.join(worktree, "package"));
      assert.deepStrictEqual(params.sandboxPolicy, { type: "readOnly", networkAccess: false });
      const result = yield* Effect.promise(() =>
        exec(params.command[0]!, params.command.slice(1), { cwd: params.cwd! }),
      );
      assert.include(result.stdout, "selected scope");
      assert.notInclude(result.stdout, "unrelated");
    }),
  );

  it.effect(
    "limits temporary-file authority to a separate directory while running tests in the worktree",
    () =>
      Effect.gen(function* () {
        const temporaryCheck = { ...check, allowTemporaryFiles: true };
        yield* Effect.promise(() =>
          NodeFSP.writeFile(
            NodePath.join(worktree, "package", "selected.test.cjs"),
            `
      const assert = require('node:assert/strict');
      const fs = require('node:fs');
      const path = require('node:path');
      require('node:test')('temporary fixture', () => {
        assert.equal(process.cwd(), ${JSON.stringify(NodePath.join(worktree, "package"))});
        fs.writeFileSync(path.join(require('node:os').tmpdir(), 'fixture'), 'ok');
      });
    `,
          ),
        );
        const params = yield* verificationCheckParams(
          { check: check.id },
          worktree,
          [temporaryCheck],
          temporary,
        );
        assert.equal(params.cwd, temporary);
        assert.deepStrictEqual(params.sandboxPolicy, {
          type: "workspaceWrite",
          writableRoots: [temporary],
          networkAccess: false,
          excludeSlashTmp: true,
          excludeTmpdirEnvVar: true,
        });
        const result = yield* Effect.promise(() =>
          exec(params.command[0]!, params.command.slice(1), {
            cwd: params.cwd!,
            env: { ...process.env, ...params.env } as NodeJS.ProcessEnv,
          }),
        );
        assert.include(result.stdout, "# pass 1");
        assert.equal(
          (yield* verificationCheckParams(
            { check: check.id },
            worktree,
            [temporaryCheck],
            worktree,
          ).pipe(Effect.result))._tag,
          "Failure",
        );
      }),
  );

  it.effect.each([
    { check: "node-test" },
    { check: "scoped-tests; touch escape" },
    { check: "scoped-tests", cwd: "/other" },
    { check: "scoped-tests", command: ["python3"] },
    { check: "scoped-tests", args: ["--eval", "process.exit()"] },
    { check: "git-diff", sandboxPolicy: { type: "dangerFullAccess" } },
    { check: "git-status", env: { GIT_DIR: "/other" } },
    {},
    null,
    "scoped-tests",
  ])("rejects unregistered commands and model-supplied execution options: %j", (args) =>
    Effect.gen(function* () {
      assert.equal(
        (yield* verificationCheckParams(args, worktree, [check]).pipe(Effect.result))._tag,
        "Failure",
      );
    }),
  );

  it("advertises the required scoped manifest without a generic test runner", () => {
    const tool = createCodexVerificationTool([check]);
    assert.deepStrictEqual(tool.inputSchema.properties.check.enum, [
      "scoped-tests",
      "git-status",
      "git-diff-check",
      "git-diff",
    ]);
    assert.include(tool.description, '"required":true');
    assert.include(tool.description, "selected.test.cjs");
  });

  it.effect("rejects a configured cwd that resolves through a symlink outside the worktree", () =>
    Effect.gen(function* () {
      yield* Effect.promise(() =>
        NodeFSP.symlink(temporary, NodePath.join(worktree, "escape"), "dir"),
      );
      const result = yield* verificationCheckParams({ check: check.id }, worktree, [
        { ...check, cwd: "escape" },
      ]).pipe(Effect.result);
      assert.equal(result._tag, "Failure");
    }),
  );
});

describe("verification check outcomes", () => {
  it.effect("distinguishes an actual node assertion failure from a missing dependency", () =>
    Effect.gen(function* () {
      const execute = async (source: string) => {
        await NodeFSP.writeFile(NodePath.join(worktree, "failure.test.cjs"), source);
        try {
          const output = await exec(
            process.execPath,
            ["--test", "--test-reporter=tap", "failure.test.cjs"],
            { cwd: worktree },
          );
          return { exitCode: 0, ...output };
        } catch (error) {
          const output = error as { code: number; stdout: string; stderr: string };
          return { exitCode: output.code, stdout: output.stdout, stderr: output.stderr };
        }
      };
      const failure = yield* Effect.promise(() =>
        execute("require('node:test')('broken', () => require('node:assert/strict').equal(1, 2));"),
      );
      assert.equal(classifyVerificationCheckResult(check, failure), "failed");
      const unavailable = yield* Effect.promise(() =>
        execute("require('t3-verification-dependency-does-not-exist');"),
      );
      assert.equal(classifyVerificationCheckResult(check, unavailable), "unavailable");
    }),
  );

  it.effect("does not accept a successful runner that skipped every selected test", () =>
    Effect.gen(function* () {
      yield* Effect.promise(() =>
        NodeFSP.writeFile(
          NodePath.join(worktree, "skipped.test.cjs"),
          "require('node:test').skip('selected test', () => {});",
        ),
      );
      const output = yield* Effect.promise(() =>
        exec(process.execPath, ["--test", "--test-reporter=tap", "skipped.test.cjs"], {
          cwd: worktree,
        }),
      );
      assert.equal(
        classifyVerificationCheckResult(check, { exitCode: 0, ...output }),
        "unavailable",
      );
    }),
  );

  it("requires successful executed assertions in a successful Vitest process", () => {
    const passed = {
      success: true,
      numPassedTests: 1,
      numFailedTests: 0,
      numFailedTestSuites: 0,
      testResults: [{ status: "passed", assertionResults: [{ status: "passed" }] }],
    };
    const classify = (report: unknown) =>
      classifyVerificationCheckResult(
        { resultFormat: "vitest-json" },
        { exitCode: 0, stdout: JSON.stringify(report), stderr: "" },
      );
    assert.equal(classify(passed), "passed");
    assert.equal(classify({ ...passed, numPassedTests: 0, testResults: [] }), "unavailable");
    assert.equal(classify({ ...passed, success: false }), "unavailable");
    assert.equal(classify({ ...passed, numFailedTests: 1 }), "unavailable");
    assert.equal(
      classify({ ...passed, testResults: [{ status: "passed", assertionResults: [] }] }),
      "unavailable",
    );
    assert.equal(
      classify({
        ...passed,
        testResults: [{ status: "passed", assertionResults: [{ status: "failed" }] }],
      }),
      "unavailable",
    );
    assert.equal(classify("truncated reporter output"), "unavailable");
    assert.equal(
      classifyVerificationCheckResult(
        { resultFormat: "exit-code" },
        { exitCode: 0, stdout: "", stderr: "" },
      ),
      "passed",
    );
  });

  it("requires an assertion result for a failed Vitest check", () => {
    const classify = (report: unknown) =>
      classifyVerificationCheckResult(
        { resultFormat: "vitest-json" },
        { exitCode: 1, stdout: JSON.stringify(report), stderr: "" },
      );
    assert.equal(
      classify({ numFailedTests: 1, testResults: [{ assertionResults: [{ status: "failed" }] }] }),
      "failed",
    );
    assert.equal(
      classify({ numFailedTests: 0, testResults: [{ assertionResults: [] }] }),
      "unavailable",
    );
    assert.equal(classify("No test files found"), "unavailable");
    assert.equal(
      classify({
        numFailedTests: 1,
        testResults: [
          {
            assertionResults: [
              { status: "failed", failureMessages: ["Cannot find package 'missing-dependency'"] },
            ],
          },
        ],
      }),
      "unavailable",
    );
  });

  it.each([124, 125, 127, 137])(
    "classifies aborted checks and missing runners as unavailable: %i",
    (exitCode) => {
      assert.equal(
        classifyVerificationCheckResult(check, {
          exitCode,
          stdout: "",
          stderr: "failed to execute",
        }),
        "unavailable",
      );
    },
  );
});
