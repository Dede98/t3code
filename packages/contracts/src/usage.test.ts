import { describe, expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";

import { UsageSummary } from "./usage.ts";

const decodeUsageSummary = Schema.decodeUnknownSync(UsageSummary);

describe("UsageSummary", () => {
  it("decodes a v3 summary far enough for clients to apply the version guard", () => {
    const decoded = decodeUsageSummary({
      contractVersion: 3,
      readAt: "2026-08-07T00:00:00.000Z",
      timeZone: "UTC",
      sinceDay: "2026-08-07",
      untilDay: "2026-08-07",
      buckets: [
        {
          day: "2026-08-07",
          provider: "claude",
          model: "claude-fable-5",
          totals: {
            uncachedInputTokens: 1,
            cachedInputTokens: 0,
            cacheCreationTokens: 0,
            outputTokens: 1,
            reasoningTokens: 0,
          },
          costUsd: 0,
          cacheSavingsUsd: 0,
          costSource: "unpriced",
          records: 1,
          unpricedRecords: 1,
          sessions: 1,
        },
      ],
      sources: [
        {
          fingerprint: {
            hostId: "host",
            provider: "claude",
            resolvedHomePath: "/home/.claude/projects",
            volumeId: "1:2",
          },
          status: "ok",
          scannedFiles: 1,
          skippedFiles: 0,
          malformedRecords: 0,
          distinctSessions: 1,
          message: null,
        },
      ],
      pricing: {
        status: "unavailable",
        source: "litellm",
        fetchedAt: null,
        knownModels: 0,
      },
      scanDurationMs: 1,
    });

    expect(decoded.contractVersion).toBe(3);
    expect(decoded.buckets[0]?.sourceId).toBe("legacy");
    expect(decoded.sources[0]?.sourceId).toBe("legacy");
  });
});
