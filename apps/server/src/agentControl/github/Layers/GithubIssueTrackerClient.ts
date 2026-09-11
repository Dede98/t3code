import {
  AgentControlGithubIssueSnapshot,
  AgentControlGithubLabelTimelineEvent,
  AgentControlGithubRepositoryBinding,
  IsoDateTime,
  PositiveInt,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as GitHubCli from "../../../sourceControl/GitHubCli.ts";
import { reduceGithubIssueTimeline } from "../githubTimelineReducer.ts";
import {
  GithubIssueTrackerClient,
  GithubIssueTrackerClientError,
  type GithubIssueTrackerClientShape,
} from "../Services/GithubIssueTrackerClient.ts";

const REPOSITORY_QUERY = `
query AgentControlRepository($owner: String!, $name: String!) {
  repository(owner: $owner, name: $name) { id nameWithOwner hasIssuesEnabled }
}`;

const TIMELINE_QUERY = `
query AgentControlIssueTimeline($id: ID!, $cursor: String) {
  node(id: $id) {
    __typename
    ... on Issue {
      id
      repository { id nameWithOwner }
      timelineItems(
        first: 100,
        after: $cursor,
        itemTypes: [LABELED_EVENT, UNLABELED_EVENT]
      ) {
        nodes {
          __typename
          ... on LabeledEvent { id createdAt actor { login } label { name } }
          ... on UnlabeledEvent { id createdAt actor { login } label { name } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`;

const RawRepository = Schema.Struct({
  id: TrimmedNonEmptyString,
  nameWithOwner: TrimmedNonEmptyString,
});
const RawRepositoryResponse = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.NullOr(
      Schema.Struct({ ...RawRepository.fields, hasIssuesEnabled: Schema.Boolean }),
    ),
  }),
});

const RawRestIssue = Schema.Struct({
  node_id: TrimmedNonEmptyString,
  number: PositiveInt,
  html_url: TrimmedNonEmptyString,
  state: Schema.Literals(["open", "closed"]),
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  updated_at: IsoDateTime,
  pull_request: Schema.optionalKey(Schema.Unknown),
});
const RawRestIssuePage = Schema.Array(RawRestIssue);

const RawTimelineNode = Schema.Struct({
  __typename: Schema.String,
  id: Schema.optionalKey(TrimmedNonEmptyString),
  createdAt: Schema.optionalKey(IsoDateTime),
  actor: Schema.optionalKey(Schema.NullOr(Schema.Struct({ login: TrimmedNonEmptyString }))),
  label: Schema.optionalKey(Schema.Struct({ name: TrimmedNonEmptyString })),
});
const RawTimelineResponse = Schema.Struct({
  data: Schema.Struct({
    node: Schema.NullOr(
      Schema.Struct({
        __typename: Schema.String,
        id: Schema.optionalKey(TrimmedNonEmptyString),
        repository: Schema.optionalKey(RawRepository),
        timelineItems: Schema.optionalKey(
          Schema.Struct({
            nodes: Schema.Array(RawTimelineNode),
            pageInfo: Schema.Struct({
              hasNextPage: Schema.Boolean,
              endCursor: Schema.NullOr(Schema.String),
            }),
          }),
        ),
      }),
    ),
  }),
});

const decodeRepositoryResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RawRepositoryResponse),
);
const decodeRestIssuePage = Schema.decodeUnknownEffect(Schema.fromJsonString(RawRestIssuePage));
const decodeTimelineResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(RawTimelineResponse),
);
const decodeSnapshot = Schema.decodeUnknownEffect(AgentControlGithubIssueSnapshot);

export interface GithubIssueTrackerClientOptions {
  readonly restPageSize?: number;
  readonly maxIssuePages?: number;
  readonly maxTimelinePages?: number;
  readonly timeoutMs?: number;
}

const mapCliError = (
  error: GitHubCli.GitHubCliError,
  operation: "resolve-repository" | "list-issues" | "read-timeline",
) => {
  let code: GithubIssueTrackerClientError["code"];
  if (error._tag === "GitHubCliUnavailableError") code = "github-unavailable";
  else if (error._tag === "GitHubCliAuthenticationError") code = "github-authentication";
  else if (
    error._tag === "GitHubCliCommandError" &&
    typeof error.cause === "object" &&
    error.cause !== null &&
    "_tag" in error.cause &&
    error.cause._tag === "VcsProcessTimeoutError"
  ) {
    code = "github-timeout";
  } else code = "github-command-failed";
  return new GithubIssueTrackerClientError({ code, operation });
};

const decodeFailure = (operation: "resolve-repository" | "list-issues" | "read-timeline") =>
  new GithubIssueTrackerClientError({ code: "github-decode-failed", operation });

const sameRepository = (
  left: AgentControlGithubRepositoryBinding,
  right: AgentControlGithubRepositoryBinding,
) =>
  left.repositoryNodeId === right.repositoryNodeId &&
  left.nameWithOwner.toLocaleLowerCase("en-US") === right.nameWithOwner.toLocaleLowerCase("en-US");

const normalizeRepository = (
  raw: typeof RawRepository.Type,
): AgentControlGithubRepositoryBinding => ({
  repositoryNodeId: raw.id,
  nameWithOwner: raw.nameWithOwner,
});

export const make = Effect.fn("GithubIssueTrackerClient.make")(function* (
  options: GithubIssueTrackerClientOptions = {},
) {
  const github = yield* GitHubCli.GitHubCli;
  const restPageSize = Math.max(1, Math.min(100, Math.floor(options.restPageSize ?? 100)));
  const maxIssuePages = Math.max(1, Math.floor(options.maxIssuePages ?? 50));
  const maxTimelinePages = Math.max(1, Math.floor(options.maxTimelinePages ?? 20));
  const timeoutMs = Math.max(1_000, Math.floor(options.timeoutMs ?? 30_000));

  const executeJson = Effect.fn("GithubIssueTrackerClient.executeJson")(function* (
    cwd: string,
    args: ReadonlyArray<string>,
    operation: "resolve-repository" | "list-issues" | "read-timeline",
  ) {
    const output = yield* github
      .execute({ cwd, args, timeoutMs })
      .pipe(Effect.mapError((error) => mapCliError(error, operation)));
    if (output.stdoutTruncated || output.stderrTruncated) {
      return yield* new GithubIssueTrackerClientError({
        code: "pagination-overflow",
        operation,
      });
    }
    return output.stdout.trim();
  });

  const resolveRepository: GithubIssueTrackerClientShape["resolveRepository"] = (input) =>
    executeJson(
      input.cwd,
      [
        "api",
        "graphql",
        "-f",
        `query=${REPOSITORY_QUERY}`,
        "-F",
        `owner=${input.locator.owner}`,
        "-F",
        `name=${input.locator.name}`,
      ],
      "resolve-repository",
    ).pipe(
      Effect.flatMap((raw) =>
        decodeRepositoryResponse(raw).pipe(
          Effect.mapError(() => decodeFailure("resolve-repository")),
        ),
      ),
      Effect.flatMap(({ data }) =>
        data.repository === null
          ? Effect.fail(decodeFailure("resolve-repository"))
          : !data.repository.hasIssuesEnabled
            ? Effect.fail(
                new GithubIssueTrackerClientError({
                  code: "github-issues-disabled",
                  operation: "resolve-repository",
                }),
              )
            : Effect.succeed(normalizeRepository(data.repository)),
      ),
    );

  const listIssues = Effect.fn("GithubIssueTrackerClient.listIssues")(function* (input: {
    readonly cwd: string;
    readonly locator: { readonly owner: string; readonly name: string };
    readonly since: string | null;
  }) {
    const issues = new Map<string, (typeof RawRestIssue)["Type"]>();
    for (let page = 1; page <= maxIssuePages; page += 1) {
      const raw: string = yield* executeJson(
        input.cwd,
        [
          "api",
          "--method",
          "GET",
          `repos/${input.locator.owner}/${input.locator.name}/issues`,
          "-f",
          "state=all",
          "-f",
          `per_page=${restPageSize}`,
          "-f",
          `page=${page}`,
          ...(input.since === null ? [] : ["-f", `since=${input.since}`]),
        ],
        "list-issues",
      );
      const decoded = yield* decodeRestIssuePage(raw).pipe(
        Effect.mapError(() => decodeFailure("list-issues")),
      );
      for (const issue of decoded) {
        if (issue.pull_request !== undefined) continue;
        const existing = issues.get(issue.node_id);
        if (existing === undefined || existing.updated_at <= issue.updated_at) {
          issues.set(issue.node_id, issue);
        }
      }
      if (decoded.length < restPageSize) return [...issues.values()];
    }
    return yield* new GithubIssueTrackerClientError({
      code: "pagination-overflow",
      operation: "list-issues",
    });
  });

  const readTimeline = Effect.fn("GithubIssueTrackerClient.readTimeline")(function* (input: {
    readonly cwd: string;
    readonly issueNodeId: string;
    readonly expectedRepository: AgentControlGithubRepositoryBinding;
  }) {
    const events: Array<AgentControlGithubLabelTimelineEvent> = [];
    let cursor: string | null = null;
    for (let page = 1; page <= maxTimelinePages; page += 1) {
      const raw: string = yield* executeJson(
        input.cwd,
        [
          "api",
          "graphql",
          "-f",
          `query=${TIMELINE_QUERY}`,
          "-F",
          `id=${input.issueNodeId}`,
          ...(cursor === null ? [] : ["-F", `cursor=${cursor}`]),
        ],
        "read-timeline",
      );
      const decoded: typeof RawTimelineResponse.Type = yield* decodeTimelineResponse(raw).pipe(
        Effect.mapError(() => decodeFailure("read-timeline")),
      );
      const node: typeof decoded.data.node = decoded.data.node;
      if (
        node === null ||
        node.__typename !== "Issue" ||
        node.id !== input.issueNodeId ||
        node.repository === undefined ||
        node.timelineItems === undefined
      ) {
        return yield* new GithubIssueTrackerClientError({
          code: "issue-repository-changed",
          operation: "read-timeline",
        });
      }
      if (!sameRepository(normalizeRepository(node.repository), input.expectedRepository)) {
        return yield* new GithubIssueTrackerClientError({
          code: "issue-repository-changed",
          operation: "read-timeline",
        });
      }
      for (const item of node.timelineItems.nodes) {
        if (item.id === undefined || item.createdAt === undefined || item.label === undefined) {
          return yield* new GithubIssueTrackerClientError({
            code: "timeline-incomplete",
            operation: "read-timeline",
          });
        }
        events.push({
          externalEventId: item.id,
          type:
            item.__typename === "LabeledEvent"
              ? "labeled"
              : item.__typename === "UnlabeledEvent"
                ? "unlabeled"
                : "unknown",
          labelName: item.label.name,
          actorLogin: item.actor?.login ?? null,
          occurredAt: item.createdAt,
        });
      }
      if (!node.timelineItems.pageInfo.hasNextPage) return events;
      const nextCursor: string | null = node.timelineItems.pageInfo.endCursor;
      if (nextCursor === null || nextCursor.length === 0 || nextCursor === cursor) {
        return yield* new GithubIssueTrackerClientError({
          code: "timeline-incomplete",
          operation: "read-timeline",
        });
      }
      cursor = nextCursor;
    }
    return yield* new GithubIssueTrackerClientError({
      code: "pagination-overflow",
      operation: "read-timeline",
    });
  });

  const pollIssues: GithubIssueTrackerClientShape["pollIssues"] = Effect.fn(
    "GithubIssueTrackerClient.pollIssues",
  )(function* (input) {
    const repository = yield* resolveRepository({ cwd: input.cwd, locator: input.locator });
    if (!sameRepository(repository, input.expectedRepository)) {
      return yield* new GithubIssueTrackerClientError({
        code: "repository-identity-changed",
        operation: "resolve-repository",
      });
    }
    const rawIssues = yield* listIssues({
      cwd: input.cwd,
      locator: input.locator,
      since: input.since,
    });
    const changedIssues = yield* Effect.forEach(
      rawIssues,
      (issue) =>
        Effect.gen(function* () {
          const timelineEvents = yield* readTimeline({
            cwd: input.cwd,
            issueNodeId: issue.node_id,
            expectedRepository: repository,
          });
          const reduction = reduceGithubIssueTimeline({
            issueState: issue.state,
            timelineComplete: true,
            events: timelineEvents,
            readyLabel: input.settings.readyLabel,
            pausedLabel: input.settings.pausedLabel,
            trustedLogins: input.settings.trustedLogins,
          });
          return yield* decodeSnapshot({
            repositoryNodeId: repository.repositoryNodeId,
            issueNodeId: issue.node_id,
            number: issue.number,
            url: issue.html_url,
            state: issue.state,
            title: issue.title,
            body: issue.body,
            contentTrust: "untrusted-external",
            updatedAt: issue.updated_at,
            timelineComplete: reduction.eligibilityReason !== "timeline-invalid",
            timelineEvents: reduction.deduplicatedEvents,
            ready: reduction.ready,
            paused: reduction.paused,
            eligible: reduction.eligible,
            eligibilityReason: reduction.eligibilityReason,
          }).pipe(Effect.mapError(() => decodeFailure("read-timeline")));
        }),
      { concurrency: 4 },
    );
    const changedIds = new Set(changedIssues.map((issue) => issue.issueNodeId));
    const unchangedIssues = yield* Effect.forEach(
      input.knownIssues.filter((issue) => !changedIds.has(issue.issueNodeId)),
      (issue) =>
        Effect.gen(function* () {
          if (issue.repositoryNodeId !== repository.repositoryNodeId) {
            return yield* new GithubIssueTrackerClientError({
              code: "issue-repository-changed",
              operation: "read-timeline",
            });
          }
          const timelineEvents = yield* readTimeline({
            cwd: input.cwd,
            issueNodeId: issue.issueNodeId,
            expectedRepository: repository,
          });
          const reduction = reduceGithubIssueTimeline({
            issueState: issue.state,
            timelineComplete: true,
            events: timelineEvents,
            readyLabel: input.settings.readyLabel,
            pausedLabel: input.settings.pausedLabel,
            trustedLogins: input.settings.trustedLogins,
          });
          return yield* decodeSnapshot({
            ...issue,
            timelineComplete: reduction.eligibilityReason !== "timeline-invalid",
            timelineEvents: reduction.deduplicatedEvents,
            ready: reduction.ready,
            paused: reduction.paused,
            eligible: reduction.eligible,
            eligibilityReason: reduction.eligibilityReason,
          }).pipe(Effect.mapError(() => decodeFailure("read-timeline")));
        }),
      { concurrency: 4 },
    );
    const issues = [...changedIssues, ...unchangedIssues];
    return {
      repository,
      issues: issues.toSorted(
        (left, right) =>
          left.number - right.number || left.issueNodeId.localeCompare(right.issueNodeId),
      ),
    };
  });

  return GithubIssueTrackerClient.of({ resolveRepository, pollIssues });
});

export const layer = Layer.effect(GithubIssueTrackerClient, make());
