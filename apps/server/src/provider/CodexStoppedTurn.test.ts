import { TurnId } from "@t3tools/contracts";
import { describe, it, expect } from "vite-plus/test";
import { readCodexStoppedTurnEvidence } from "./CodexStoppedTurn.ts";

const input = {
  cwd: "/controlled/worktree",
  resumeCursor: { threadId: "native-thread" },
  providerTurnId: TurnId.make("native-turn"),
};
const turn = {
  id: "native-turn",
  status: "interrupted",
  startedAt: 1_789_040_393,
  completedAt: 1_789_040_399,
};
const snapshot = (overrides: Partial<typeof turn> = {}) => ({
  thread: { id: "native-thread", cwd: input.cwd, turns: [{ ...turn, ...overrides }] },
});

describe("durable Codex stopped-turn evidence", () => {
  it.each(["interrupted", "failed"] as const)("preserves an explicit %s terminal", (state) => {
    expect(readCodexStoppedTurnEvidence(input, snapshot({ status: state }))).toEqual({
      provider: "codex",
      providerTurnId: input.providerTurnId,
      state,
      terminalAt: "2026-09-10T11:39:59.000Z",
    });
  });
  it.each([
    snapshot({ id: "other-turn" }),
    snapshot({ status: "inProgress" }),
    snapshot({ status: "completed" }),
    snapshot({ completedAt: 0 }),
    snapshot({ completedAt: Number.NaN }),
    snapshot({ completedAt: 1.5 }),
    { thread: { ...snapshot().thread, id: "other-thread" } },
    { thread: { ...snapshot().thread, cwd: "/other" } },
    { thread: { ...snapshot().thread, turns: [turn, turn] } },
    { thread: { ...snapshot().thread, turns: [] } },
    { thread: { ...snapshot().thread, turns: [{ ...turn, completedAt: null }] } },
    { thread: { ...snapshot().thread, turns: [{ ...turn, startedAt: null }] } },
    {},
    null,
  ])("does not infer termination from missing or divergent evidence: %j", (value) => {
    expect(readCodexStoppedTurnEvidence(input, value)).toBeUndefined();
  });
});
