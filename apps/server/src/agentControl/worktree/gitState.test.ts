import { assert, describe, it } from "@effect/vitest";

import { parseAgentControlPorcelainV2Status, parseAgentControlWorktreeList } from "./gitState.ts";

const sha = "a".repeat(40);
const zero = "0".repeat(40);
const record = (path: string, branch = "main") =>
  `worktree ${path}\0HEAD ${sha}\0branch refs/heads/${branch}\0\0`;

describe("Agent Control NUL worktree parser", () => {
  it("preserves spaces, tabs, and newlines in paths", () => {
    const path = "/tmp/a path\twith\nnewlines";
    assert.equal(parseAgentControlWorktreeList(record(path))[0]!.path, path);
  });

  it("models normal, detached, bare, locked, and prunable records", () => {
    const parsed = parseAgentControlWorktreeList(
      `${record("/tmp/normal")}worktree /tmp/detached\0HEAD ${sha}\0detached\0locked reason\0\0worktree /repo.git\0bare\0prunable stale\0\0`,
    );
    assert.equal(parsed[0]!.branch, "main");
    assert.equal(parsed[1]!.detached, true);
    assert.equal(parsed[1]!.locked, true);
    assert.equal(parsed[2]!.bare, true);
    assert.equal(parsed[2]!.head, null);
    assert.equal(parsed[2]!.prunable, true);
  });

  for (const [name, output] of [
    ["leading NUL", `\0${record("/tmp/w")}`],
    ["double terminal NUL", `${record("/tmp/w")}\0`],
    ["triple terminal NUL", `${record("/tmp/w")}\0\0`],
    ["unterminated", `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0`],
    ["missing HEAD", "worktree /tmp/w\0branch refs/heads/main\0\0"],
    ["missing branch", `worktree /tmp/w\0HEAD ${sha}\0\0`],
    ["duplicate HEAD", `worktree /tmp/w\0HEAD ${sha}\0HEAD ${sha}\0detached\0\0`],
    [
      "duplicate branch",
      `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0branch refs/heads/other\0\0`,
    ],
    [
      "contradictory detached branch",
      `worktree /tmp/w\0HEAD ${sha}\0branch refs/heads/main\0detached\0\0`,
    ],
    ["malformed HEAD", "worktree /tmp/w\0HEAD nope\0branch refs/heads/main\0\0"],
    ["unknown critical field", `worktree /tmp/w\0HEAD ${sha}\0future value\0detached\0\0`],
    ["truncated record", `worktree /tmp/w\0HEAD ${sha}\0detached\0\0worktree /tmp/x`],
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

describe("Agent Control porcelain-v2 status parser", () => {
  const ordinary = (path: string, xy = ".M", sub = "N...") =>
    `1 ${xy} ${sub} 100644 100644 100644 ${sha} ${sha} ${path}\0`;
  const rename = (path: string, origin: string, score = "R100") =>
    `2 R. N... 100644 100644 100644 ${sha} ${sha} ${score} ${path}\0${origin}\0`;
  const unmerged = (path: string) =>
    `u UU N... 100644 100644 100644 100644 ${sha} ${sha} ${zero} ${path}\0`;

  it("accepts clean output and all supported dirty records with literal paths", () => {
    assert.deepEqual(parseAgentControlPorcelainV2Status(""), []);
    const path = "space tab\tnewline\nname";
    const parsed = parseAgentControlPorcelainV2Status(
      `${ordinary(path)}${rename("renamed", "origin name")}`,
    );
    assert.equal(parsed[0]!.path, path);
    assert.equal(parsed[1]!.type, "rename-copy");
  });

  it("accepts unmerged, untracked, ignored, and submodule records", () => {
    const parsed = parseAgentControlPorcelainV2Status(
      `${unmerged("conflict")}? untracked\npath\0! ignored path\0${ordinary("submodule", ".M", "S.MU")}`,
    );
    assert.deepEqual(
      parsed.map((entry) => entry.type),
      ["unmerged", "untracked", "ignored", "ordinary"],
    );
  });

  for (const [name, output] of [
    ["truncated ordinary", ordinary("file").slice(0, -1)],
    ["truncated rename", rename("new", "old").slice(0, -"old\0".length)],
    ["missing rename origin", rename("new", "old").slice(0, -"old\0".length)],
    ["unknown type", "x nonsense\0"],
    ["extra NUL", `${ordinary("file")}\0`],
    ["header", "# branch.oid abc\0"],
    ["bad XY", ordinary("file", "ZZ")],
    ["bad submodule", ordinary("file", ".M", "SM")],
    ["bad mode", `1 .M N... 10064x 100644 100644 ${sha} ${sha} file\0`],
    ["bad oid", `1 .M N... 100644 100644 100644 nope ${sha} file\0`],
    ["bad score", `2 R. N... 100644 100644 100644 ${sha} ${sha} R101 new\0old\0`],
  ] as const) {
    it(`fails closed for ${name}`, () => {
      assert.throws(() => parseAgentControlPorcelainV2Status(output));
    });
  }
});
