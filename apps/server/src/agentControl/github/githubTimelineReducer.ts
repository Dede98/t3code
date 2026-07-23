import type {
  AgentControlGithubEligibilityReason,
  AgentControlGithubLabelTimelineEvent,
} from "@t3tools/contracts";

export interface GithubTimelineReductionInput {
  readonly issueState: "open" | "closed";
  readonly timelineComplete: boolean;
  readonly events: ReadonlyArray<AgentControlGithubLabelTimelineEvent>;
  readonly readyLabel: string;
  readonly pausedLabel: string;
  readonly trustedLogins: ReadonlyArray<string>;
}

export interface GithubTimelineReduction {
  readonly ready: boolean;
  readonly paused: boolean;
  readonly eligible: boolean;
  readonly eligibilityReason: AgentControlGithubEligibilityReason;
  readonly deduplicatedEvents: ReadonlyArray<AgentControlGithubLabelTimelineEvent>;
}

const normalize = (value: string) => value.trim().toLocaleLowerCase("en-US");

const invalid = (
  events: ReadonlyArray<AgentControlGithubLabelTimelineEvent>,
): GithubTimelineReduction => ({
  ready: false,
  paused: true,
  eligible: false,
  eligibilityReason: "timeline-invalid",
  deduplicatedEvents: events,
});

const sameEvent = (
  left: AgentControlGithubLabelTimelineEvent,
  right: AgentControlGithubLabelTimelineEvent,
) =>
  left.type === right.type &&
  normalize(left.labelName) === normalize(right.labelName) &&
  (left.actorLogin === null ? null : normalize(left.actorLogin)) ===
    (right.actorLogin === null ? null : normalize(right.actorLogin)) &&
  left.occurredAt === right.occurredAt;

/**
 * Pure fail-closed reducer for GitHub label timeline authorization.
 *
 * External event ids are the sole deduplication key. Timestamps are used only
 * for ordering; any same-time operations whose order could alter the result
 * make the whole timeline ineligible.
 */
export function reduceGithubIssueTimeline(
  input: GithubTimelineReductionInput,
): GithubTimelineReduction {
  const byId = new Map<string, AgentControlGithubLabelTimelineEvent>();
  for (const event of input.events) {
    const existing = byId.get(event.externalEventId);
    if (existing !== undefined && !sameEvent(existing, event)) {
      return invalid([...byId.values()]);
    }
    if (existing === undefined) byId.set(event.externalEventId, event);
  }

  const deduplicated = [...byId.values()];
  for (const event of deduplicated) {
    if (!Number.isFinite(Date.parse(event.occurredAt))) return invalid(deduplicated);
  }
  const events = deduplicated.toSorted(
    (left, right) =>
      Date.parse(left.occurredAt) - Date.parse(right.occurredAt) ||
      left.externalEventId.localeCompare(right.externalEventId),
  );
  if (!input.timelineComplete) return invalid(events);

  const readyLabel = normalize(input.readyLabel);
  const pausedLabel = normalize(input.pausedLabel);
  if (readyLabel === pausedLabel) return invalid(events);
  const trusted = new Set(input.trustedLogins.map(normalize));

  for (const event of events) {
    if (event.type === "unknown") return invalid(events);
    if (event.actorLogin === null) return invalid(events);
  }

  for (let index = 0; index < events.length; ) {
    const timestamp = Date.parse(events[index]!.occurredAt);
    const group: Array<AgentControlGithubLabelTimelineEvent> = [];
    while (events[index] !== undefined && Date.parse(events[index]!.occurredAt) === timestamp) {
      group.push(events[index]!);
      index += 1;
    }

    const readyAssignments = new Set<boolean>();
    const pausedOperations = new Set<"set" | "clear" | "retain">();
    for (const event of group) {
      const label = normalize(event.labelName);
      const actorTrusted = trusted.has(normalize(event.actorLogin ?? ""));
      if (label === readyLabel) {
        readyAssignments.add(event.type === "labeled" && actorTrusted);
      }
      if (label === pausedLabel) {
        pausedOperations.add(event.type === "labeled" ? "set" : actorTrusted ? "clear" : "retain");
      }
    }
    if (readyAssignments.size > 1 || pausedOperations.size > 1) return invalid(events);
  }

  let ready = false;
  let paused = false;
  for (const event of events) {
    const label = normalize(event.labelName);
    const actorTrusted = trusted.has(normalize(event.actorLogin ?? ""));
    if (label === readyLabel) {
      ready = event.type === "labeled" && actorTrusted;
    } else if (label === pausedLabel) {
      if (event.type === "labeled") paused = true;
      if (event.type === "unlabeled" && actorTrusted) paused = false;
    }
  }

  let eligibilityReason: AgentControlGithubEligibilityReason;
  if (input.issueState === "closed") eligibilityReason = "closed";
  else if (paused) eligibilityReason = "paused";
  else if (!ready) eligibilityReason = "ready-inactive";
  else eligibilityReason = "eligible";

  return {
    ready,
    paused,
    eligible: eligibilityReason === "eligible",
    eligibilityReason,
    deduplicatedEvents: events,
  };
}
