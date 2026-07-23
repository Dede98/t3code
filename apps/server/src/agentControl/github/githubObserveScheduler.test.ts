import {
  CommandId,
  EventId,
  type AgentControlGithubEvent,
  type AgentControlGithubPollErrorCode,
  ProjectId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";

import {
  reduceGithubObserveScheduler,
  type GithubObserveSchedulerFold,
} from "./githubObserveScheduler.ts";

const EPOCH = "2026-07-23T08:00:00.000Z";
const projectId = ProjectId.make("scheduler-machine");
const repository = {
  repositoryNodeId: "repository-node",
  nameWithOwner: "owner/repo",
} as const;
const settings = {
  trackerKind: "github" as const,
  readyLabel: "agent:ready",
  pausedLabel: "agent:paused",
  trustedLogins: ["trusted"],
  pollIntervalSeconds: 15,
};
const context = {
  now: 0,
  jitterMillis: () => 0,
};

const base = (sequence: number) => {
  const commandId = CommandId.make(`machine-command-${sequence}`);
  return {
    eventId: EventId.make(`machine-event-${sequence}`),
    aggregateKind: "github-intake" as const,
    aggregateId: projectId,
    occurredAt: EPOCH,
    commandId,
    causationEventId: null,
    correlationId: commandId,
    metadata: { schemaVersion: 1 as const },
    streamVersion: sequence,
    sequence,
  };
};

const config = (sequence: number, pollIntervalSeconds = 15): AgentControlGithubEvent => ({
  ...base(sequence),
  type: "agentControl.github.config.set",
  authority: "human",
  payload: {
    projectId,
    repository,
    settings: { ...settings, pollIntervalSeconds },
    configuredAt: EPOCH,
  },
});

const failure = (
  sequence: number,
  errorCode: AgentControlGithubPollErrorCode,
): AgentControlGithubEvent => ({
  ...base(sequence),
  type: "agentControl.github.poll.failed",
  authority: "controller",
  payload: {
    projectId,
    attemptedAt: EPOCH,
    completedAt: EPOCH,
    errorCode,
    invalidateCursor: false,
  },
});

const success = (sequence: number): AgentControlGithubEvent => ({
  ...base(sequence),
  type: "agentControl.github.poll.succeeded",
  authority: "controller",
  payload: {
    projectId,
    repository,
    attemptedAt: EPOCH,
    completedAt: EPOCH,
    cursor: { lastSuccessfulPollAt: EPOCH, overlapSeconds: 120 },
    issues: [],
  },
});

const apply = (
  events: ReadonlyArray<AgentControlGithubEvent>,
  initial: GithubObserveSchedulerFold = { state: null, lastGeneration: 0 },
) => events.reduce((fold, event) => reduceGithubObserveScheduler(fold, event, context), initial);

it("counts every committed failure and opens the circuit on the fifth", () => {
  const folded = apply([
    config(1),
    failure(2, "github-timeout"),
    failure(3, "github-timeout"),
    failure(4, "github-timeout"),
    failure(5, "github-timeout"),
    failure(6, "github-timeout"),
  ]);
  assert.equal(folded.state?.consecutiveFailures, 5);
  assert.equal(folded.state?.circuitState, "open");
  assert.equal(folded.state?.lastGithubEventSequence, 6);
});

it("preserves hard suspension across manual timeout and authentication failures", () => {
  const suspended = apply([
    config(1),
    failure(2, "repository-identity-changed"),
    failure(3, "github-timeout"),
    failure(4, "github-authentication"),
  ]);
  assert.equal(suspended.state?.activity, "suspended");
  assert.equal(suspended.state?.circuitState, "open");
  assert.equal(suspended.state?.reasonCode, "repository-identity-changed");
  assert.equal(suspended.state?.lastGithubEventSequence, 4);

  const recovered = apply([success(5)], suspended);
  assert.equal(recovered.state?.activity, "active");
  assert.equal(recovered.state?.circuitState, "closed");
  assert.equal(recovered.state?.consecutiveFailures, 0);
});

it("uses config boundaries to create a new generation and reactivate", () => {
  const suspended = apply([config(1), failure(2, "timeline-incomplete"), config(3, 30)]);
  assert.equal(suspended.state?.generation, 2);
  assert.equal(suspended.state?.activity, "active");
  assert.equal(suspended.state?.pollIntervalSeconds, 30);
  assert.equal(suspended.state?.consecutiveFailures, 0);
});
