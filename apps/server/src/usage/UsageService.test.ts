import { describe, expect, it } from "@effect/vitest";

import { collectUsageTranscriptSources } from "./UsageService.ts";

describe("collectUsageTranscriptSources", () => {
  it("keeps every distinct Claude and Codex root while collapsing shared homes", () => {
    const sources = collectUsageTranscriptSources([
      {
        usageHistorySource: {
          provider: "claude",
          transcriptDirectory: "/Users/theo/.claude/projects",
        },
      },
      {
        usageHistorySource: {
          provider: "claude",
          transcriptDirectory: "/Users/theo/.claude-work/projects",
        },
      },
      {
        usageHistorySource: {
          provider: "claude",
          transcriptDirectory: "/Users/theo/.claude-work/projects",
        },
      },
      {
        usageHistorySource: {
          provider: "codex",
          transcriptDirectory: "/Users/theo/.codex/sessions",
        },
      },
      {
        usageHistorySource: {
          provider: "codex",
          transcriptDirectory: "/Users/theo/.codex/sessions",
        },
      },
      {
        usageHistorySource: {
          provider: "codex",
          transcriptDirectory: "/Users/theo/.codex-work/sessions",
        },
      },
      {},
    ]);

    expect(sources).toEqual([
      {
        sourceId: "claude:1",
        provider: "claude",
        dir: "/Users/theo/.claude/projects",
      },
      {
        sourceId: "claude:2",
        provider: "claude",
        dir: "/Users/theo/.claude-work/projects",
      },
      {
        sourceId: "codex:1",
        provider: "codex",
        dir: "/Users/theo/.codex/sessions",
      },
      {
        sourceId: "codex:2",
        provider: "codex",
        dir: "/Users/theo/.codex-work/sessions",
      },
    ]);
  });
});
