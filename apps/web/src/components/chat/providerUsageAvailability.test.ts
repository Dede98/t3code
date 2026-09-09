import type { ServerProviderUsageLimits, ServerProviderUsageWindow } from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import { getProviderUsageAttention, selectPrimaryUsageWindow } from "./providerUsageAvailability";

const now = Date.parse("2026-09-07T10:00:00Z");
const session: ServerProviderUsageWindow = {
  id: "five_hour",
  kind: "session",
  label: "Session",
  usedPercent: 20,
};
const weekly: ServerProviderUsageWindow = {
  id: "seven_day",
  kind: "weekly",
  label: "Weekly",
  usedPercent: 50,
};
const limits = (windows: readonly ServerProviderUsageWindow[]): ServerProviderUsageLimits => ({
  checkedAt: new Date(now).toISOString(),
  windows,
});

describe("composer limits", () => {
  it("prefers session headroom and falls back to weekly or monthly plans", () => {
    expect(selectPrimaryUsageWindow(limits([weekly, session]), now)).toEqual(session);
    expect(selectPrimaryUsageWindow(limits([weekly]), now)).toEqual(weekly);
    const monthly = { ...weekly, kind: "monthly" as const, id: "primary", label: "Monthly" };
    expect(selectPrimaryUsageWindow(limits([monthly]), now)).toEqual(monthly);
  });
  it("surfaces the most constrained window above ninety percent", () => {
    const critical = { ...weekly, usedPercent: 99 };
    expect(selectPrimaryUsageWindow(limits([session, critical]), now)).toEqual(critical);
    expect(getProviderUsageAttention(limits([session, critical]), now)).toMatchObject({
      severity: "warning",
      label: "Weekly 1% left",
    });
  });
  it("warns without asserting rejection for an exhausted general or scoped quota", () => {
    for (const window of [session, { ...weekly, id: "weekly_scoped_fable_5", label: "Fable 5" }]) {
      expect(
        getProviderUsageAttention(limits([{ ...window, usedPercent: 100 }]), now),
      ).toMatchObject({ severity: "warning", label: `${window.label} 0% left` });
    }
  });
  it("drops expired warnings and accepts replenished windows after refresh or reset", () => {
    const exhausted = { ...weekly, usedPercent: 100, resetsAt: new Date(now + 1000).toISOString() };
    expect(getProviderUsageAttention(limits([exhausted]), now)).not.toBeNull();
    expect(getProviderUsageAttention(limits([exhausted]), now + 1000)).toBeNull();
    expect(getProviderUsageAttention(limits([{ ...exhausted, usedPercent: 0 }]), now)).toBeNull();
    expect(
      selectPrimaryUsageWindow(
        limits([{ ...exhausted, resetsAt: new Date(now).toISOString() }, session]),
        now,
      ),
    ).toEqual(session);
  });
  it("keeps unavailable or absent quota out of the meter and picker warnings", () => {
    for (const unavailable of [
      undefined,
      { reason: "unsupported" as const },
      { reason: "probeFailed" as const },
    ]) {
      const value = unavailable
        ? { ...limits([{ ...session, usedPercent: 100 }]), unavailable }
        : undefined;
      expect(selectPrimaryUsageWindow(value, now)).toBeNull();
      expect(getProviderUsageAttention(value, now)).toBeNull();
    }
  });
});
