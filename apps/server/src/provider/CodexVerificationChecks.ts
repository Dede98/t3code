// @effect-diagnostics nodeBuiltinImport:off
// @effect-diagnostics preferSchemaOverJson:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import {
  AgentControlVerificationChecks,
  type AgentControlVerificationCheck,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as CodexSchema from "effect-codex-app-server/schema";

const INSPECTIONS = {
  "git-status": ["git", "--no-optional-locks", "status", "--short"],
  "git-diff-check": ["git", "diff", "--no-ext-diff", "--no-textconv", "--check"],
  "git-diff": ["git", "diff", "--no-ext-diff", "--no-textconv"],
} as const;

class VerificationCheckError extends Schema.TaggedError<VerificationCheckError>()(
  "VerificationCheckError",
  { message: Schema.String },
) {}

const CheckInput = Schema.Struct({ check: Schema.String });
const decodeInput = Schema.decodeUnknownEffect(CheckInput, { onExcessProperty: "error" });
const decodeChecks = Schema.decodeUnknownEffect(AgentControlVerificationChecks);

/** Only registered IDs cross the model boundary; the controller owns every execution option. */
export const createCodexVerificationTool = (checks: AgentControlVerificationChecks = []) =>
  ({
    type: "function" as const,
    name: "t3_verification_check",
    description:
      "Run a preauthorized check in the controlled Verification worktree with no network. " +
      "The worktree is read-only; approved checks may write only to a controller-owned temporary directory. " +
      `Project checks: ${JSON.stringify(checks.map(({ id, command, args, cwd, required }) => ({ id, command, args, cwd, required })))}. ` +
      "Optional inspections: git-status, git-diff-check, git-diff. " +
      "Run every required project check yourself. Implementation reports are not verification evidence. " +
      "Use this tool instead of the shell. Report unavailable or failed checks; do not repair or escalate permissions.",
    inputSchema: {
      type: "object",
      properties: {
        check: {
          type: "string",
          enum: [...checks.map((check) => check.id), ...Object.keys(INSPECTIONS)],
        },
      },
      required: ["check"],
      additionalProperties: false,
    },
  }) satisfies CodexSchema.V2ThreadStartParams__DynamicToolSpec;

export const CODEX_VERIFICATION_TOOL = createCodexVerificationTool();

// workspaceWrite implicitly permits the app-server's process cwd as well as
// command/exec's cwd. The caller MUST use a separate command-only app-server
// rooted in this private directory; the fixed argv runs in the read-only worktree.
const TEMPORARY_CHECK_WRAPPER = `
const { spawnSync } = require('node:child_process');
const config = JSON.parse(process.argv[1]);
const result = spawnSync(config.command, config.args, {
  cwd: config.cwd, shell: false, encoding: 'utf8',
  timeout: config.timeoutMs, maxBuffer: 32768,
});
if (result.stdout) process.stdout.write(result.stdout);
if (result.stderr) process.stderr.write(result.stderr);
if (result.error) process.stderr.write('\\nT3_CHECK_UNAVAILABLE: ' + result.error.code + '\\n');
process.exit(result.error || result.signal ? 125 : (result.status ?? 125));
`;

const isWithin = (root: string, path: string) => {
  const relative = NodePath.relative(root, path);
  return (
    relative === "" ||
    (!relative.startsWith(`..${NodePath.sep}`) &&
      relative !== ".." &&
      !NodePath.isAbsolute(relative))
  );
};

export const verificationCheckParams = Effect.fn("verificationCheckParams")(function* (
  args: unknown,
  worktreePath: string,
  checks: AgentControlVerificationChecks = [],
  temporaryDirectory?: string,
) {
  const { check: id } = yield* decodeInput(args);
  const manifest = yield* decodeChecks(checks);
  const check = manifest.find((candidate) => candidate.id === id);
  const inspection = Object.entries(INSPECTIONS).find(([key]) => key === id)?.[1];
  if (!check && !inspection)
    return yield* new VerificationCheckError({ message: "Verification check is not authorized" });
  const paths = yield* Effect.tryPromise(async () => {
    const root = await NodeFSP.realpath(worktreePath);
    const cwd = await NodeFSP.realpath(NodePath.resolve(root, check?.cwd ?? "."));
    if (!isWithin(root, cwd))
      throw new VerificationCheckError({
        message: "Verification cwd escapes the controlled worktree",
      });
    const temporary =
      check?.allowTemporaryFiles && temporaryDirectory
        ? await NodeFSP.realpath(temporaryDirectory)
        : null;
    if (
      check?.allowTemporaryFiles &&
      (!temporary || isWithin(root, temporary) || isWithin(temporary, root))
    ) {
      throw new VerificationCheckError({
        message: "Verification requires a separate controller-owned temporary directory",
      });
    }
    return { cwd, temporary };
  });
  const command = check ? [check.command, ...check.args] : inspection!;
  const timeoutMs = check?.timeoutMs ?? 60_000;
  if (paths.temporary) {
    return {
      command: [
        process.execPath,
        "-e",
        TEMPORARY_CHECK_WRAPPER,
        JSON.stringify({ command: command[0], args: command.slice(1), cwd: paths.cwd, timeoutMs }),
      ],
      cwd: paths.temporary,
      env: { TMPDIR: paths.temporary, TMP: paths.temporary, TEMP: paths.temporary },
      sandboxPolicy: {
        type: "workspaceWrite",
        writableRoots: [paths.temporary],
        networkAccess: false,
        excludeSlashTmp: true,
        excludeTmpdirEnvVar: true,
      },
      timeoutMs: timeoutMs + 1_000,
      outputBytesCap: 32_768,
    } satisfies CodexSchema.V2CommandExecParams;
  }
  return {
    command,
    cwd: paths.cwd,
    sandboxPolicy: { type: "readOnly", networkAccess: false },
    timeoutMs,
    outputBytesCap: 32_768,
  } satisfies CodexSchema.V2CommandExecParams;
});

const VitestReport = Schema.Struct({
  numFailedTests: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  testResults: Schema.Array(
    Schema.Struct({
      assertionResults: Schema.Array(Schema.Struct({ status: Schema.String })),
    }),
  ),
});
const decodeVitestReport = Schema.decodeUnknownOption(Schema.fromJsonString(VitestReport));
const decodeSuccessfulVitestReport = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      success: Schema.Literal(true),
      numPassedTests: Schema.Int.check(Schema.isGreaterThan(0)),
      numFailedTests: Schema.Literal(0),
      numFailedTestSuites: Schema.Literal(0),
      testResults: Schema.Array(
        Schema.Struct({
          status: Schema.Literal("passed"),
          assertionResults: Schema.Array(Schema.Struct({ status: Schema.String })),
        }),
      ),
    }),
  ),
);

/** A nonzero exit alone cannot distinguish a broken runner from a failed assertion. */
export const classifyVerificationCheckResult = (
  check: Pick<AgentControlVerificationCheck, "resultFormat">,
  result: { readonly exitCode: number; readonly stdout: string; readonly stderr: string },
): "passed" | "failed" | "unavailable" => {
  if (result.exitCode === 0) {
    if (check.resultFormat === "exit-code") return "passed";
    if (check.resultFormat === "node-test") {
      return /^# pass [1-9][0-9]*$/m.test(result.stdout) &&
        /^# fail 0$/m.test(result.stdout) &&
        /^# cancelled 0$/m.test(result.stdout)
        ? "passed"
        : "unavailable";
    }
    const report = decodeSuccessfulVitestReport(result.stdout.slice(result.stdout.indexOf("{")));
    if (report._tag === "None") return "unavailable";
    const assertions = report.value.testResults.flatMap((suite) => suite.assertionResults);
    return assertions.filter((assertion) => assertion.status === "passed").length ===
      report.value.numPassedTests &&
      assertions.every((assertion) =>
        ["passed", "pending", "skipped", "todo"].includes(assertion.status),
      )
      ? "passed"
      : "unavailable";
  }
  if (result.exitCode !== 1) return "unavailable";
  if (
    /ERR_MODULE_NOT_FOUND|MODULE_NOT_FOUND|ERR_DLOPEN_FAILED|Cannot find (?:module|package)|No test files found|Failed to load (?:config|url)|T3_CHECK_UNAVAILABLE/.test(
      result.stderr + result.stdout,
    )
  )
    return "unavailable";
  if (check.resultFormat === "node-test") {
    return /^\s*code: ['"]ERR_ASSERTION['"]\s*$/m.test(result.stdout) &&
      /^# fail [1-9][0-9]*$/m.test(result.stdout)
      ? "failed"
      : "unavailable";
  }
  if (check.resultFormat === "vitest-json") {
    // Package runners may print their command before Vitest's JSON reporter.
    const start = result.stdout.indexOf("{");
    const report = decodeVitestReport(result.stdout.slice(start));
    if (
      report._tag === "Some" &&
      report.value.numFailedTests > 0 &&
      report.value.testResults.some((suite) =>
        suite.assertionResults.some((assertion) => assertion.status === "failed"),
      )
    ) {
      return "failed";
    }
  }
  return "unavailable";
};

export const verificationToolFailure = (text: string): CodexSchema.DynamicToolCallResponse => ({
  success: false,
  contentItems: [{ type: "inputText", text }],
});
