import {
  VcsProcessTimeoutError,
  type AgentControlGithubIssueSnapshot,
  type AgentControlGithubRepositoryBinding,
} from "@t3tools/contracts";
import { assert, describe, expect, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as GitHubCli from "../../../sourceControl/GitHubCli.ts";
import type * as VcsProcess from "../../../vcs/VcsProcess.ts";
import { GithubIssueTrackerClient } from "../Services/GithubIssueTrackerClient.ts";
import { make } from "./GithubIssueTrackerClient.ts";

const output = (value: unknown): VcsProcess.VcsProcessOutput => {
  const stdout = JSON.stringify(value);
  return {
    exitCode: ChildProcessSpawner.ExitCode(0),
    stdout,
    stderr: "",
    stdoutTruncated: false,
    stderrTruncated: false,
  };
};

const repository: AgentControlGithubRepositoryBinding = {
  repositoryNodeId: "repository-node-1",
  nameWithOwner: "owner/repo",
};

const rawRepository = { id: repository.repositoryNodeId, nameWithOwner: repository.nameWithOwner };
const repositoryResponse = { data: { repository: rawRepository } };
const timelineResponse = (input: {
  readonly nodes: ReadonlyArray<unknown>;
  readonly hasNextPage: boolean;
  readonly endCursor: string | null;
}) => ({
  data: {
    node: {
      __typename: "Issue",
      id: "issue-node-1",
      repository: rawRepository,
      timelineItems: {
        nodes: input.nodes,
        pageInfo: {
          hasNextPage: input.hasNextPage,
          endCursor: input.endCursor,
        },
      },
    },
  },
});

const readyEvent = {
  __typename: "LabeledEvent",
  id: "label-event-1",
  createdAt: "2026-07-22T10:00:00.000Z",
  actor: { login: "trusted" },
  label: { name: "agent:ready" },
};

const makeLayer = (
  execute: GitHubCli.GitHubCli["Service"]["execute"],
  options: Parameters<typeof make>[0] = {},
) =>
  Layer.effect(GithubIssueTrackerClient, make(options)).pipe(
    Layer.provide(Layer.mock(GitHubCli.GitHubCli)({ execute })),
  );

const poll = (knownIssues: ReadonlyArray<AgentControlGithubIssueSnapshot> = []) =>
  Effect.gen(function* () {
    const client = yield* GithubIssueTrackerClient;
    return yield* client.pollIssues({
      cwd: "/server/selected/repo",
      locator: { owner: "owner", name: "repo" },
      expectedRepository: repository,
      knownIssues,
      settings: {
        trackerKind: "github",
        readyLabel: "agent:ready",
        pausedLabel: "agent:paused",
        trustedLogins: ["trusted"],
        pollIntervalSeconds: 60,
      },
      since: null,
    });
  });

const knownIssue: AgentControlGithubIssueSnapshot = {
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: "issue-node-1",
  number: 1,
  url: "https://github.com/owner/repo/issues/1",
  state: "open",
  title: "previous untrusted title",
  body: "previous untrusted body",
  contentTrust: "untrusted-external",
  updatedAt: "2026-07-22T09:00:00.000Z",
  timelineComplete: true,
  timelineEvents: [],
  ready: false,
  paused: false,
  eligible: false,
  eligibilityReason: "ready-inactive",
};

describe("GithubIssueTrackerClient", () => {
  it.effect("paginates issues and timelines, filters PRs, and deduplicates external events", () =>
    Effect.gen(function* () {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      execute
        .mockReturnValueOnce(Effect.succeed(output(repositoryResponse)))
        .mockReturnValueOnce(
          Effect.succeed(
            output([
              {
                node_id: "issue-node-1",
                number: 1,
                html_url: "https://github.com/owner/repo/issues/1",
                state: "open",
                title: "untrusted issue title",
                body: "untrusted issue body",
                updated_at: "2026-07-22T10:00:01.000Z",
              },
              {
                node_id: "pull-node-2",
                number: 2,
                html_url: "https://github.com/owner/repo/pull/2",
                state: "open",
                title: "pull request",
                body: null,
                updated_at: "2026-07-22T10:00:02.000Z",
                pull_request: { url: "https://api.github.com/pulls/2" },
              },
            ]),
          ),
        )
        .mockReturnValueOnce(Effect.succeed(output([])))
        .mockReturnValueOnce(
          Effect.succeed(
            output(timelineResponse({ nodes: [readyEvent], hasNextPage: true, endCursor: "c1" })),
          ),
        )
        .mockReturnValueOnce(
          Effect.succeed(
            output(timelineResponse({ nodes: [readyEvent], hasNextPage: false, endCursor: null })),
          ),
        );

      const result = yield* poll().pipe(Effect.provide(makeLayer(execute, { restPageSize: 2 })));
      assert.equal(result.issues.length, 1);
      assert.equal(result.issues[0]?.eligible, true);
      assert.equal(result.issues[0]?.timelineEvents.length, 1);
      assert.equal(result.issues[0]?.contentTrust, "untrusted-external");
      expect(execute).toHaveBeenCalledTimes(5);
      for (const [input] of execute.mock.calls) {
        expect(input.args[0]).toBe("api");
        expect(input.args).not.toContain("issue");
        expect(input.args).not.toContain("edit");
        expect(input.args).not.toContain("comment");
      }
    }),
  );

  it.effect("returns a closed overflow error instead of accepting a partial issue poll", () =>
    Effect.gen(function* () {
      const messages: Array<string> = [];
      const logger = Logger.make(({ message }) => messages.push(String(message)));
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      execute.mockReturnValueOnce(Effect.succeed(output(repositoryResponse))).mockReturnValueOnce(
        Effect.succeed(
          output([
            {
              node_id: "issue-node-1",
              number: 1,
              html_url: "https://github.com/owner/repo/issues/1",
              state: "open",
              title: "never copied into the error",
              body: "secret-looking untrusted body",
              updated_at: "2026-07-22T10:00:01.000Z",
            },
          ]),
        ),
      );
      const error = yield* Effect.flip(
        poll().pipe(
          Effect.provide(
            Layer.mergeAll(
              makeLayer(execute, { restPageSize: 1, maxIssuePages: 1 }),
              Logger.layer([logger], { mergeWithExisting: false }),
            ),
          ),
        ),
      );
      assert.equal(error._tag, "GithubIssueTrackerClientError");
      assert.equal(error.code, "pagination-overflow");
      assert.equal(error.operation, "list-issues");
      expect(String(error)).not.toMatch(/never copied|secret-looking|body/i);
      expect(messages.join("\n")).not.toMatch(/never copied|secret-looking|body/i);
    }),
  );

  it.effect("classifies timeout and decode failures without process details", () =>
    Effect.gen(function* () {
      const timeout = new GitHubCli.GitHubCliCommandError({
        command: "gh",
        cwd: "/sensitive/path",
        cause: new VcsProcessTimeoutError({
          operation: "GitHubCli.execute",
          command: "gh",
          cwd: "/sensitive/path",
          timeoutMs: 1,
        }),
      });
      const timeoutError = yield* Effect.flip(
        poll().pipe(Effect.provide(makeLayer(() => Effect.fail(timeout)))),
      );
      assert.equal(timeoutError.code, "github-timeout");
      assert.notProperty(timeoutError, "cause");
      assert.notProperty(timeoutError, "cwd");

      const decodeError = yield* Effect.flip(
        poll().pipe(
          Effect.provide(
            makeLayer(() =>
              Effect.succeed({
                ...output(null),
                stdout: "not-json",
              }),
            ),
          ),
        ),
      );
      assert.equal(decodeError.code, "github-decode-failed");
    }),
  );

  it.effect("revalidates known issues and rejects a transferred issue", () =>
    Effect.gen(function* () {
      const execute = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      execute
        .mockReturnValueOnce(Effect.succeed(output(repositoryResponse)))
        .mockReturnValueOnce(Effect.succeed(output([])))
        .mockReturnValueOnce(
          Effect.succeed(
            output(timelineResponse({ nodes: [readyEvent], hasNextPage: false, endCursor: null })),
          ),
        );
      const result = yield* poll([knownIssue]).pipe(Effect.provide(makeLayer(execute)));
      assert.equal(result.issues.length, 1);
      assert.equal(result.issues[0]?.eligible, true);

      const transferred = vi.fn<GitHubCli.GitHubCli["Service"]["execute"]>();
      transferred
        .mockReturnValueOnce(Effect.succeed(output(repositoryResponse)))
        .mockReturnValueOnce(Effect.succeed(output([])))
        .mockReturnValueOnce(
          Effect.succeed(
            output({
              data: {
                node: {
                  ...timelineResponse({ nodes: [], hasNextPage: false, endCursor: null }).data.node,
                  repository: { id: "repository-node-2", nameWithOwner: "owner/other" },
                },
              },
            }),
          ),
        );
      const error = yield* Effect.flip(
        poll([knownIssue]).pipe(Effect.provide(makeLayer(transferred))),
      );
      assert.equal(error.code, "issue-repository-changed");
    }),
  );
});
