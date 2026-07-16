import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderUsageSnapshot,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import { selectPrimaryUsageWindow } from "./ProviderUsageMeter";
import { formatProviderUsageResetAt } from "../providerUsageFormatting";

const usage = {
  providerInstanceId: ProviderInstanceId.make("claude-work"),
  driver: ProviderDriverKind.make("claudeAgent"),
  observedAt: "2026-07-14T10:00:00.000Z",
  source: "runtime-event",
  status: "allowed",
  windows: [
    { id: "seven_day", label: "Weekly", usedPercent: 50, resetsAt: null, durationMinutes: 10_080 },
    { id: "five_hour", label: "5 hours", usedPercent: 20, resetsAt: null, durationMinutes: 300 },
  ],
} satisfies ProviderUsageSnapshot;

describe("selectPrimaryUsageWindow", () => {
  it("prefers the active-session window over weekly usage", () => {
    expect(selectPrimaryUsageWindow(usage)?.id).toBe("five_hour");
  });

  it("uses weekly usage when no session window is available", () => {
    expect(
      selectPrimaryUsageWindow({
        ...usage,
        windows: usage.windows.filter((window) => window.id === "seven_day"),
      })?.id,
    ).toBe("seven_day");
  });

  it("uses a weekly Codex primary window when the session window is disabled", () => {
    expect(
      selectPrimaryUsageWindow({
        ...usage,
        driver: ProviderDriverKind.make("codex"),
        windows: [
          {
            id: "primary",
            label: "Weekly",
            usedPercent: 22,
            resetsAt: null,
            durationMinutes: 10_080,
          },
        ],
      })?.id,
    ).toBe("primary");
  });

  it("surfaces a critical weekly window ahead of an unused session window", () => {
    expect(
      selectPrimaryUsageWindow({
        ...usage,
        windows: [
          { id: "five_hour", label: "5 hours", usedPercent: 0, resetsAt: null },
          { id: "seven_day", label: "Weekly", usedPercent: 99, resetsAt: null },
        ],
      })?.id,
    ).toBe("seven_day");
  });
});

describe("formatProviderUsageResetAt", () => {
  const now = Date.parse("2026-07-15T10:00:00.000Z");

  it("formats nearby reset times relative to now", () => {
    expect(formatProviderUsageResetAt("2026-07-15T11:30:00.000Z", now)).toBe("Resets in 1h 30m");
  });

  it("uses the shared unavailable fallback", () => {
    expect(formatProviderUsageResetAt(null, now)).toBe("Reset time unavailable");
    expect(formatProviderUsageResetAt("not-a-date", now)).toBe("Reset time unavailable");
  });
});
