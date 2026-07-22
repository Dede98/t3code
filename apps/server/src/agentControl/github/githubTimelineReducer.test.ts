import type { AgentControlGithubLabelTimelineEvent } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { reduceGithubIssueTimeline } from "./githubTimelineReducer.ts";

const event = (
  id: string,
  type: AgentControlGithubLabelTimelineEvent["type"],
  labelName: string,
  actorLogin: string | null,
  occurredAt = `2026-07-22T00:00:0${id}.000Z`,
): AgentControlGithubLabelTimelineEvent => ({
  externalEventId: `event-${id}`,
  type,
  labelName,
  actorLogin,
  occurredAt,
});

const reduce = (
  events: ReadonlyArray<AgentControlGithubLabelTimelineEvent>,
  issueState: "open" | "closed" = "open",
  timelineComplete = true,
) =>
  reduceGithubIssueTimeline({
    issueState,
    timelineComplete,
    events,
    readyLabel: "agent:ready",
    pausedLabel: "agent:paused",
    trustedLogins: ["Trusted-User"],
  });

describe("reduceGithubIssueTimeline", () => {
  it("activates Ready only for a case-insensitively trusted actor", () => {
    const result = reduce([event("1", "labeled", "AGENT:READY", "trusted-user")]);
    expect(result).toMatchObject({ ready: true, paused: false, eligible: true });
  });

  it("does not activate Ready for an untrusted actor", () => {
    expect(reduce([event("1", "labeled", "agent:ready", "stranger")])).toMatchObject({
      ready: false,
      eligible: false,
      eligibilityReason: "ready-inactive",
    });
  });

  it("lets any later unlabel revoke Ready", () => {
    expect(
      reduce([
        event("1", "labeled", "agent:ready", "trusted-user"),
        event("2", "unlabeled", "agent:ready", "stranger"),
      ]),
    ).toMatchObject({ ready: false, eligible: false });
  });

  it("does not let an untrusted relabel restore Ready", () => {
    expect(
      reduce([
        event("1", "labeled", "agent:ready", "trusted-user"),
        event("2", "unlabeled", "agent:ready", "trusted-user"),
        event("3", "labeled", "agent:ready", "stranger"),
      ]),
    ).toMatchObject({ ready: false, eligible: false });
  });

  it("keeps Paused active until a later trusted unlabel and lets Paused win", () => {
    const paused = reduce([
      event("1", "labeled", "agent:ready", "trusted-user"),
      event("2", "labeled", "agent:paused", "stranger"),
      event("3", "unlabeled", "agent:paused", "stranger"),
    ]);
    expect(paused).toMatchObject({ ready: true, paused: true, eligible: false });

    const resumed = reduce([
      ...paused.deduplicatedEvents,
      event("4", "unlabeled", "agent:paused", "TRUSTED-USER"),
    ]);
    expect(resumed).toMatchObject({ ready: true, paused: false, eligible: true });
  });

  it("fails closed for a missing actor, unknown event, or incomplete timeline", () => {
    for (const result of [
      reduce([event("1", "labeled", "agent:ready", null)]),
      reduce([event("1", "labeled", "unrelated", null)]),
      reduce([event("1", "unknown", "agent:ready", "trusted-user")]),
      reduce([event("1", "labeled", "agent:ready", "trusted-user")], "open", false),
    ]) {
      expect(result).toMatchObject({
        ready: false,
        paused: true,
        eligible: false,
        eligibilityReason: "timeline-invalid",
      });
    }
  });

  it("fails closed for contradictory same-time events", () => {
    expect(
      reduce([
        event("1", "labeled", "agent:ready", "trusted-user", "2026-07-22T00:00:00.000Z"),
        event("2", "unlabeled", "agent:ready", "trusted-user", "2026-07-22T02:00:00.000+02:00"),
      ]),
    ).toMatchObject({ eligibilityReason: "timeline-invalid", eligible: false });
  });

  it("deduplicates identical stable event ids and rejects conflicting duplicates", () => {
    const ready = event("1", "labeled", "agent:ready", "trusted-user");
    expect(reduce([ready, ready]).deduplicatedEvents).toHaveLength(1);
    expect(reduce([ready, { ...ready, type: "unlabeled" }]).eligibilityReason).toBe(
      "timeline-invalid",
    );
  });

  it("never marks a closed issue eligible", () => {
    expect(reduce([event("1", "labeled", "agent:ready", "trusted-user")], "closed")).toMatchObject({
      ready: true,
      eligible: false,
      eligibilityReason: "closed",
    });
  });
});
