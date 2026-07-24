import { assert, describe, it } from "@effect/vitest";

import { parseAgentControlWorktreeList } from "./gitState.ts";

const sha = "a".repeat(40);
const record = (path: string, branch = "main") =>
  `worktree ${path}\0HEAD ${sha}\0branch refs/heads/${branch}\0\0`;

describe("Agent Control NUL worktree parser", () => {
  it("preserves spaces, tabs, and newlines in paths", () => {
    const path = "/tmp/a path\twith\nnewlines";
    assert.equal(parseAgentControlWorktreeList(record(path))[0]!.path, path);
  });

  it("skips unknown fields but preserves explicit locked and prunable state", () => {
    const parsed = parseAgentControlWorktreeList(
      `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0future harmless\0locked reason\0prunable stale\0\0`,
    )[0]!;
    assert.equal(parsed.locked, true);
    assert.equal(parsed.prunable, true);
  });

  it("represents detached state explicitly", () => {
    const parsed = parseAgentControlWorktreeList(`worktree /tmp/w\0HEAD ${sha}\0detached\0\0`)[0]!;
    assert.equal(parsed.detached, true);
    assert.equal(parsed.branch, null);
  });

  for (const [name, output] of [
    ["unterminated", `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0`],
    ["missing HEAD", "worktree /tmp/w\0branch refs/heads/main\0\0"],
    ["missing branch", `worktree /tmp/w\0HEAD ${sha}\0\0`],
    [
      "contradictory detached branch",
      `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0detached\0\0`,
    ],
    ["malformed HEAD", "worktree /tmp/w\0HEAD nope\0branch refs/heads/main\0\0"],
  ] as const) {
    it(`fails closed for ${name}`, () => {
      assert.throws(() => parseAgentControlWorktreeList(output));
    });
  }

  it("returns all duplicate path and branch records for ambiguity checks", () => {
    const parsed = parseAgentControlWorktreeList(`${record("/tmp/w")}${record("/tmp/w")}`);
    assert.equal(parsed.length, 2);
  });
});
