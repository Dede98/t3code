import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as CodexSchema from "effect-codex-app-server/schema";

const CHECKS = {
  "node-test": ["node", "--test"],
  "git-status": ["git", "--no-optional-locks", "status", "--short"],
  "git-diff-check": ["git", "diff", "--no-ext-diff", "--no-textconv", "--check"],
  "git-diff": ["git", "diff", "--no-ext-diff", "--no-textconv"],
} as const;

const CheckInput = Schema.Struct({
  check: Schema.Literals(Object.keys(CHECKS) as Array<keyof typeof CHECKS>),
});
const decodeInput = Schema.decodeUnknownEffect(CheckInput, { onExcessProperty: "error" });

/** Controller-owned commands: neither argv, cwd nor sandbox can come from the model. */
export const CODEX_VERIFICATION_TOOL = {
  type: "function",
  name: "t3_verification_check",
  description:
    "Run a preauthorized check in the controlled Verification worktree, with read-only filesystem and no network. " +
    "Checks: node-test (node --test), git-status, git-diff-check, git-diff. " +
    "Use this tool for checks instead of the shell. Other command approvals are declined. " +
    "Report unavailable or failed checks; do not repair files or retry outside the sandbox.",
  inputSchema: {
    type: "object",
    properties: { check: { type: "string", enum: Object.keys(CHECKS) } },
    required: ["check"],
    additionalProperties: false,
  },
} satisfies CodexSchema.V2ThreadStartParams__DynamicToolSpec;

export const verificationCheckParams = (args: unknown, cwd: string) =>
  decodeInput(args).pipe(
    Effect.map(({ check }): CodexSchema.V2CommandExecParams => ({
      command: CHECKS[check],
      cwd,
      sandboxPolicy: { type: "readOnly", networkAccess: false },
      timeoutMs: 60_000,
      outputBytesCap: 32_768,
    })),
  );

export const verificationToolFailure = (text: string): CodexSchema.DynamicToolCallResponse => ({
  success: false,
  contentItems: [{ type: "inputText", text }],
});
