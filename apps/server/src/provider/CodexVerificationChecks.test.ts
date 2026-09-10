import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { assert, describe } from "vite-plus/test";
import { verificationCheckParams } from "./CodexVerificationChecks.ts";

describe("controller-owned verification commands", () => {
  it.effect(
    "binds a registered check to the authorized cwd without a shell or sandbox escape",
    () =>
      Effect.gen(function* () {
        const params = yield* verificationCheckParams(
          { check: "node-test" },
          "/controlled/worktree",
        );
        assert.deepStrictEqual(params, {
          command: ["node", "--test"],
          cwd: "/controlled/worktree",
          sandboxPolicy: { type: "readOnly", networkAccess: false },
          timeoutMs: 60_000,
          outputBytesCap: 32_768,
        });
      }),
  );
  it.effect.each([
    { check: "python3 -c print(1)" },
    { check: "node-test; touch escape" },
    { check: "node-test", cwd: "/other" },
    { check: "node-test", command: ["python3", "-c", "print(1)"] },
    { check: "node-test", args: ["--eval", "process.exit()"] },
    { check: "git-diff", sandboxPolicy: { type: "dangerFullAccess" } },
    { check: "git-status", env: { GIT_DIR: "/other" } },
    {},
    null,
    "node-test",
  ])("rejects unregistered commands and model-supplied execution options: %j", (args) =>
    Effect.gen(function* () {
      assert.equal(
        (yield* verificationCheckParams(args, "/controlled/worktree").pipe(Effect.result))._tag,
        "Failure",
      );
    }),
  );
});
