import type {
  AgentControlGithubIssueSnapshot,
  AgentControlGithubRepositoryBinding,
  AgentControlGithubTrackerSettings,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

export const GithubIssueTrackerClientErrorCode = Schema.Literals([
  "github-unavailable",
  "github-authentication",
  "github-timeout",
  "github-command-failed",
  "github-decode-failed",
  "pagination-overflow",
  "timeline-incomplete",
  "repository-identity-changed",
  "issue-repository-changed",
]);
export type GithubIssueTrackerClientErrorCode = typeof GithubIssueTrackerClientErrorCode.Type;

export class GithubIssueTrackerClientError extends Schema.TaggedErrorClass<GithubIssueTrackerClientError>()(
  "GithubIssueTrackerClientError",
  {
    code: GithubIssueTrackerClientErrorCode,
    operation: Schema.Literals(["resolve-repository", "list-issues", "read-timeline"]),
  },
) {}

export interface GithubRepositoryLocator {
  readonly owner: string;
  readonly name: string;
}

export interface GithubIssuePollResult {
  readonly repository: AgentControlGithubRepositoryBinding;
  readonly issues: ReadonlyArray<AgentControlGithubIssueSnapshot>;
}

export interface GithubIssueTrackerClientShape {
  readonly resolveRepository: (input: {
    readonly cwd: string;
    readonly locator: GithubRepositoryLocator;
  }) => Effect.Effect<AgentControlGithubRepositoryBinding, GithubIssueTrackerClientError>;
  readonly pollIssues: (input: {
    readonly cwd: string;
    readonly locator: GithubRepositoryLocator;
    readonly expectedRepository: AgentControlGithubRepositoryBinding;
    readonly settings: AgentControlGithubTrackerSettings;
    readonly knownIssues: ReadonlyArray<AgentControlGithubIssueSnapshot>;
    readonly since: string | null;
  }) => Effect.Effect<GithubIssuePollResult, GithubIssueTrackerClientError>;
}

export class GithubIssueTrackerClient extends Context.Service<
  GithubIssueTrackerClient,
  GithubIssueTrackerClientShape
>()("t3/agentControl/github/Services/GithubIssueTrackerClient") {}
