import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderUsageSnapshot,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";

import {
  getProviderUsageAttention,
  getProviderUsageUnavailableReason,
} from "./providerUsageAvailability";

function usage(
  driver: "claudeAgent" | "codex",
  windows: ProviderUsageSnapshot["windows"],
  status: ProviderUsageSnapshot["status"] = "rejected",
): ProviderUsageSnapshot {
  return {
    providerInstanceId: ProviderInstanceId.make(driver),
    driver: ProviderDriverKind.make(driver),
    observedAt: "2026-07-14T10:00:00.000Z",
    source: "refresh",
    status,
    windows,
  };
}

describe("getProviderUsageUnavailableReason", () => {
  it("marks a provider unavailable when a general window is exhausted", () => {
    const reason = getProviderUsageUnavailableReason(
      usage("claudeAgent", [
        {
          id: "five_hour",
          label: "5 hours",
          usedPercent: 100,
          resetsAt: "2026-07-14T12:00:00.000Z",
        },
      ]),
    );

    expect(reason).toContain("5 hours usage limit reached");
  });

  it("does not block all of Claude for an exhausted model-scoped limit", () => {
    expect(
      getProviderUsageUnavailableReason(
        usage("claudeAgent", [
          {
            id: "seven_day_overage_included",
            label: "Fable 5",
            usedPercent: 100,
            resetsAt: null,
          },
        ]),
      ),
    ).toBeNull();
  });

  it("uses an explicit rejected status when no window reports 100 percent", () => {
    expect(
      getProviderUsageUnavailableReason(
        usage("codex", [{ id: "secondary", label: "Weekly", usedPercent: 99, resetsAt: null }]),
      ),
    ).toBe("Usage limit reached.");
  });

  it("keeps allowed providers selectable", () => {
    expect(
      getProviderUsageUnavailableReason(
        usage(
          "codex",
          [{ id: "secondary", label: "Weekly", usedPercent: 42, resetsAt: null }],
          "allowed",
        ),
      ),
    ).toBeNull();
  });

  it("warns for the highest window above 90 percent without disabling the provider", () => {
    const snapshot = usage(
      "claudeAgent",
      [
        { id: "five_hour", label: "5 hours", usedPercent: 0, resetsAt: null },
        { id: "seven_day", label: "Weekly", usedPercent: 99, resetsAt: null },
      ],
      "warning",
    );

    expect(getProviderUsageUnavailableReason(snapshot)).toBeNull();
    expect(getProviderUsageAttention(snapshot)).toMatchObject({
      severity: "warning",
      label: "Weekly 99%",
    });
  });

  it("warns for a scoped limit without disabling all Claude models", () => {
    const attention = getProviderUsageAttention(
      usage("claudeAgent", [
        {
          id: "seven_day_overage_included",
          label: "Fable 5",
          usedPercent: 100,
          resetsAt: null,
        },
      ]),
    );

    expect(attention).toMatchObject({ severity: "warning", label: "Fable 5 100%" });
  });
});
