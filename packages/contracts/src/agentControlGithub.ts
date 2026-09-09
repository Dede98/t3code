/**
 * Schema-only contracts for the read-only Agent Control GitHub intake.
 *
 * GitHub issue content is external, untrusted data. The schemas retain that
 * provenance explicitly and never model issue text as controller commands.
 *
 * @module agentControlGithub
 */
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import {
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const AGENT_CONTROL_GITHUB_RPC_METHODS = {
  getTrackerConfig: "agentControlGithub.getTrackerConfig",
  setTrackerConfig: "agentControlGithub.setTrackerConfig",
  clearTrackerConfig: "agentControlGithub.clearTrackerConfig",
  getObserveState: "agentControlGithub.getObserveState",
  listObservedIssues: "agentControlGithub.listObservedIssues",
  pollOnce: "agentControlGithub.pollOnce",
  getReactorStatus: "agentControlGithub.getReactorStatus",
} as const;

export const AGENT_CONTROL_GITHUB_DEFAULT_READY_LABEL = "agent:ready";
export const AGENT_CONTROL_GITHUB_DEFAULT_PAUSED_LABEL = "agent:paused";
export const AGENT_CONTROL_GITHUB_DEFAULT_POLL_INTERVAL_SECONDS = 60;
export const AGENT_CONTROL_GITHUB_MIN_POLL_INTERVAL_SECONDS = 15;
export const AGENT_CONTROL_GITHUB_MAX_POLL_INTERVAL_SECONDS = 3_600;

export const AgentControlGithubLogin = TrimmedNonEmptyString;
export type AgentControlGithubLogin = typeof AgentControlGithubLogin.Type;

export const AgentControlGithubPollIntervalSeconds = Schema.Int.check(
  Schema.isBetween({
    minimum: AGENT_CONTROL_GITHUB_MIN_POLL_INTERVAL_SECONDS,
    maximum: AGENT_CONTROL_GITHUB_MAX_POLL_INTERVAL_SECONDS,
  }),
);

export const AgentControlGithubRepositoryBinding = Schema.Struct({
  repositoryNodeId: TrimmedNonEmptyString,
  nameWithOwner: TrimmedNonEmptyString,
});
export type AgentControlGithubRepositoryBinding = typeof AgentControlGithubRepositoryBinding.Type;

export const AgentControlGithubTrackerSettings = Schema.Struct({
  trackerKind: Schema.Literal("github"),
  readyLabel: TrimmedNonEmptyString,
  pausedLabel: TrimmedNonEmptyString,
  trustedLogins: Schema.Array(AgentControlGithubLogin),
  pollIntervalSeconds: AgentControlGithubPollIntervalSeconds,
});
export type AgentControlGithubTrackerSettings = typeof AgentControlGithubTrackerSettings.Type;

export const AgentControlGithubTrackerConfig = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  settings: AgentControlGithubTrackerSettings,
  repository: AgentControlGithubRepositoryBinding,
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
  updatedAt: IsoDateTime,
});
export type AgentControlGithubTrackerConfig = typeof AgentControlGithubTrackerConfig.Type;

export const AgentControlGithubPollCursor = Schema.Struct({
  lastSuccessfulPollAt: IsoDateTime,
  overlapSeconds: PositiveInt,
});
export type AgentControlGithubPollCursor = typeof AgentControlGithubPollCursor.Type;

export const AGENT_CONTROL_GITHUB_POLL_ERROR_CODES = [
  "github-unavailable",
  "github-authentication",
  "github-timeout",
  "github-command-failed",
  "github-decode-failed",
  "pagination-overflow",
  "timeline-incomplete",
  "repository-identity-changed",
  "issue-repository-changed",
  "poll-in-progress",
  "internal-persistence-error",
] as const;
export const AgentControlGithubPollErrorCode = Schema.Literals(
  AGENT_CONTROL_GITHUB_POLL_ERROR_CODES,
);
export type AgentControlGithubPollErrorCode = typeof AgentControlGithubPollErrorCode.Type;

export const AgentControlGithubPollStatus = Schema.Union([
  Schema.Struct({
    status: Schema.Literals(["disabled", "not-polled"]),
    attemptedAt: Schema.Null,
    completedAt: Schema.Null,
    errorCode: Schema.Null,
  }),
  Schema.Struct({
    status: Schema.Literal("success"),
    attemptedAt: IsoDateTime,
    completedAt: IsoDateTime,
    errorCode: Schema.Null,
    issueCount: NonNegativeInt,
  }),
  Schema.Struct({
    status: Schema.Literal("needs-attention"),
    attemptedAt: IsoDateTime,
    completedAt: IsoDateTime,
    errorCode: AgentControlGithubPollErrorCode,
  }),
]);
export type AgentControlGithubPollStatus = typeof AgentControlGithubPollStatus.Type;

export const AgentControlGithubLabelTimelineEvent = Schema.Struct({
  externalEventId: TrimmedNonEmptyString,
  type: Schema.Literals(["labeled", "unlabeled", "unknown"]),
  labelName: TrimmedNonEmptyString,
  actorLogin: Schema.NullOr(TrimmedNonEmptyString),
  occurredAt: IsoDateTime,
});
export type AgentControlGithubLabelTimelineEvent = typeof AgentControlGithubLabelTimelineEvent.Type;

export const AgentControlGithubEligibilityReason = Schema.Literals([
  "eligible",
  "closed",
  "ready-inactive",
  "paused",
  "timeline-invalid",
]);
export type AgentControlGithubEligibilityReason = typeof AgentControlGithubEligibilityReason.Type;

export const AgentControlGithubIssueSnapshot = Schema.Struct({
  repositoryNodeId: TrimmedNonEmptyString,
  issueNodeId: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "closed"]),
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  contentTrust: Schema.Literal("untrusted-external"),
  updatedAt: IsoDateTime,
  timelineComplete: Schema.Boolean,
  timelineEvents: Schema.Array(AgentControlGithubLabelTimelineEvent),
  ready: Schema.Boolean,
  paused: Schema.Boolean,
  eligible: Schema.Boolean,
  eligibilityReason: AgentControlGithubEligibilityReason,
});
export type AgentControlGithubIssueSnapshot = typeof AgentControlGithubIssueSnapshot.Type;

export const AgentControlGithubIssueSummary = Schema.Struct({
  issueNodeId: TrimmedNonEmptyString,
  number: PositiveInt,
  url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "closed"]),
  title: Schema.String,
  contentTrust: Schema.Literal("untrusted-external"),
  updatedAt: IsoDateTime,
  ready: Schema.Boolean,
  paused: Schema.Boolean,
  eligible: Schema.Boolean,
  eligibilityReason: AgentControlGithubEligibilityReason,
});
export type AgentControlGithubIssueSummary = typeof AgentControlGithubIssueSummary.Type;

export const AgentControlGithubIntakeState = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  projectId: ProjectId,
  config: Schema.NullOr(AgentControlGithubTrackerConfig),
  cursor: Schema.NullOr(AgentControlGithubPollCursor),
  pollStatus: AgentControlGithubPollStatus,
  revision: NonNegativeInt,
  sequence: NonNegativeInt,
  updatedAt: Schema.NullOr(IsoDateTime),
});
export type AgentControlGithubIntakeState = typeof AgentControlGithubIntakeState.Type;

export const AgentControlGithubProjectInput = Schema.Struct({ projectId: ProjectId });
export type AgentControlGithubProjectInput = typeof AgentControlGithubProjectInput.Type;

export const AgentControlGithubSetTrackerConfigInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
  trackerKind: Schema.Literal("github"),
  readyLabel: TrimmedNonEmptyString.pipe(
    Schema.withDecodingDefault(Effect.succeed(AGENT_CONTROL_GITHUB_DEFAULT_READY_LABEL)),
  ),
  pausedLabel: TrimmedNonEmptyString.pipe(
    Schema.withDecodingDefault(Effect.succeed(AGENT_CONTROL_GITHUB_DEFAULT_PAUSED_LABEL)),
  ),
  trustedLogins: Schema.Array(AgentControlGithubLogin),
  pollIntervalSeconds: AgentControlGithubPollIntervalSeconds.pipe(
    Schema.withDecodingDefault(Effect.succeed(AGENT_CONTROL_GITHUB_DEFAULT_POLL_INTERVAL_SECONDS)),
  ),
});
export type AgentControlGithubSetTrackerConfigInput =
  typeof AgentControlGithubSetTrackerConfigInput.Type;

export const AgentControlGithubClearTrackerConfigInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
});
export type AgentControlGithubClearTrackerConfigInput =
  typeof AgentControlGithubClearTrackerConfigInput.Type;

export const AgentControlGithubPollOnceInput = Schema.Struct({
  commandId: CommandId,
  projectId: ProjectId,
  expectedRevision: NonNegativeInt,
});
export type AgentControlGithubPollOnceInput = typeof AgentControlGithubPollOnceInput.Type;

export const AgentControlGithubCommandResult = Schema.Struct({
  state: AgentControlGithubIntakeState,
  resultSequence: NonNegativeInt,
  eventCreated: Schema.Boolean,
});
export type AgentControlGithubCommandResult = typeof AgentControlGithubCommandResult.Type;

export const AgentControlGithubListIssuesResult = Schema.Struct({
  projectId: ProjectId,
  issues: Schema.Array(AgentControlGithubIssueSummary),
});
export type AgentControlGithubListIssuesResult = typeof AgentControlGithubListIssuesResult.Type;

export const AgentControlGithubReactorActivity = Schema.Literals([
  "active",
  "inactive",
  "suspended",
]);
export type AgentControlGithubReactorActivity = typeof AgentControlGithubReactorActivity.Type;

export const AgentControlGithubReactorHealth = Schema.Literals([
  "healthy",
  "recovering",
  "degraded",
]);
export type AgentControlGithubReactorHealth = typeof AgentControlGithubReactorHealth.Type;

export const AgentControlGithubWorkerStatus = Schema.Literals([
  "stopped",
  "scheduled",
  "polling",
  "missing",
]);
export type AgentControlGithubWorkerStatus = typeof AgentControlGithubWorkerStatus.Type;

export const AgentControlGithubCircuitState = Schema.Literals(["closed", "open", "half-open"]);
export type AgentControlGithubCircuitState = typeof AgentControlGithubCircuitState.Type;

export const AGENT_CONTROL_GITHUB_REACTOR_REASON_CODES = [
  "github-unavailable",
  "github-authentication",
  "github-timeout",
  "github-command-failed",
  "github-decode-failed",
  "pagination-overflow",
  "timeline-incomplete",
  "repository-identity-changed",
  "issue-repository-changed",
  "poll-in-progress",
  "revision-conflict",
  "project-unavailable",
  "tracker-not-configured",
  "internal-coordination-error",
] as const;
export const AgentControlGithubReactorReasonCode = Schema.Literals(
  AGENT_CONTROL_GITHUB_REACTOR_REASON_CODES,
);
export type AgentControlGithubReactorReasonCode = typeof AgentControlGithubReactorReasonCode.Type;

/** Transport-safe operational status. It intentionally excludes process and issue data. */
export const AgentControlGithubReactorStatus = Schema.Struct({
  projectId: ProjectId,
  activity: AgentControlGithubReactorActivity,
  health: AgentControlGithubReactorHealth,
  workerStatus: AgentControlGithubWorkerStatus,
  subscriptionHealth: AgentControlGithubReactorHealth,
  circuitState: AgentControlGithubCircuitState,
  consecutiveFailures: NonNegativeInt,
  lastAttemptAt: Schema.NullOr(IsoDateTime),
  nextAttemptAt: Schema.NullOr(IsoDateTime),
  reasonCode: Schema.NullOr(AgentControlGithubReactorReasonCode),
});
export type AgentControlGithubReactorStatus = typeof AgentControlGithubReactorStatus.Type;

export const AGENT_CONTROL_GITHUB_RPC_ERROR_CODES = [
  "validation",
  "project-missing",
  "project-deleted",
  "tracker-not-configured",
  "repository-not-github",
  "repository-identity-conflict",
  "revision-conflict",
  "command-previously-rejected",
  "command-identity-mismatch",
  "poll-in-progress",
  "github-unavailable",
  "github-authentication",
  "github-timeout",
  "github-command-failed",
  "github-decode-failed",
  "pagination-overflow",
  "timeline-incomplete",
  "repository-identity-changed",
  "issue-repository-changed",
  "internal-persistence-error",
] as const;
export const AgentControlGithubRpcErrorCode = Schema.Literals(AGENT_CONTROL_GITHUB_RPC_ERROR_CODES);
export type AgentControlGithubRpcErrorCode = typeof AgentControlGithubRpcErrorCode.Type;

/** Wire-safe by construction: no cwd, argv, stderr, exception, token, or issue content. */
export class AgentControlGithubRpcError extends Schema.TaggedError<AgentControlGithubRpcError>()(
  "AgentControlGithubRpcError",
  {
    code: AgentControlGithubRpcErrorCode,
    operation: Schema.Literals([
      "get-tracker-config",
      "set-tracker-config",
      "clear-tracker-config",
      "get-observe-state",
      "list-observed-issues",
      "poll-once",
      "get-reactor-status",
    ]),
    projectId: ProjectId,
  },
) {}

const AgentControlGithubEventBase = {
  eventId: EventId,
  aggregateKind: Schema.Literal("github-intake"),
  aggregateId: ProjectId,
  occurredAt: IsoDateTime,
  commandId: CommandId,
  causationEventId: Schema.NullOr(EventId),
  correlationId: CommandId,
  metadata: Schema.Struct({ schemaVersion: Schema.Literal(1) }),
} as const;

export const AgentControlGithubConfigSetPayload = Schema.Struct({
  projectId: ProjectId,
  settings: AgentControlGithubTrackerSettings,
  repository: AgentControlGithubRepositoryBinding,
  configuredAt: IsoDateTime,
});
export type AgentControlGithubConfigSetPayload = typeof AgentControlGithubConfigSetPayload.Type;

export const AgentControlGithubConfigClearedPayload = Schema.Struct({
  projectId: ProjectId,
  clearedAt: IsoDateTime,
});
export type AgentControlGithubConfigClearedPayload =
  typeof AgentControlGithubConfigClearedPayload.Type;

export const AgentControlGithubPollSucceededPayload = Schema.Struct({
  projectId: ProjectId,
  repository: AgentControlGithubRepositoryBinding,
  attemptedAt: IsoDateTime,
  completedAt: IsoDateTime,
  cursor: AgentControlGithubPollCursor,
  issues: Schema.Array(AgentControlGithubIssueSnapshot),
});
export type AgentControlGithubPollSucceededPayload =
  typeof AgentControlGithubPollSucceededPayload.Type;

export const AgentControlGithubPollFailedPayload = Schema.Struct({
  projectId: ProjectId,
  attemptedAt: IsoDateTime,
  completedAt: IsoDateTime,
  errorCode: AgentControlGithubPollErrorCode,
  invalidateCursor: Schema.Boolean,
});
export type AgentControlGithubPollFailedPayload = typeof AgentControlGithubPollFailedPayload.Type;

const configSetDraftFields = {
  ...AgentControlGithubEventBase,
  authority: Schema.Literal("human"),
  type: Schema.Literal("agentControl.github.config.set"),
  payload: AgentControlGithubConfigSetPayload,
} as const;
const configClearedDraftFields = {
  ...AgentControlGithubEventBase,
  authority: Schema.Literal("human"),
  type: Schema.Literal("agentControl.github.config.cleared"),
  payload: AgentControlGithubConfigClearedPayload,
} as const;
const pollSucceededDraftFields = {
  ...AgentControlGithubEventBase,
  authority: Schema.Literal("controller"),
  type: Schema.Literal("agentControl.github.poll.succeeded"),
  payload: AgentControlGithubPollSucceededPayload,
} as const;
const pollFailedDraftFields = {
  ...AgentControlGithubEventBase,
  authority: Schema.Literal("controller"),
  type: Schema.Literal("agentControl.github.poll.failed"),
  payload: AgentControlGithubPollFailedPayload,
} as const;

export const AgentControlGithubEventDraft = Schema.Union([
  Schema.Struct(configSetDraftFields),
  Schema.Struct(configClearedDraftFields),
  Schema.Struct(pollSucceededDraftFields),
  Schema.Struct(pollFailedDraftFields),
]);
export type AgentControlGithubEventDraft = typeof AgentControlGithubEventDraft.Type;

export const AgentControlGithubEvent = Schema.Union([
  Schema.Struct({ ...configSetDraftFields, streamVersion: PositiveInt, sequence: PositiveInt }),
  Schema.Struct({
    ...configClearedDraftFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({
    ...pollSucceededDraftFields,
    streamVersion: PositiveInt,
    sequence: PositiveInt,
  }),
  Schema.Struct({ ...pollFailedDraftFields, streamVersion: PositiveInt, sequence: PositiveInt }),
]);
export type AgentControlGithubEvent = typeof AgentControlGithubEvent.Type;
