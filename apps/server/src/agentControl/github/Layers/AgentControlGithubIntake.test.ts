import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, ProjectId, type AgentControlGithubIssueSnapshot } from "@t3tools/contracts";
import { afterEach, assert, it, vi } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { layer as GithubEventStoreLive } from "./AgentControlGithubEventStore.ts";
import { layer as GithubProjectionLive } from "./AgentControlGithubProjection.ts";
import { layer as GithubStateRepositoryLive } from "./AgentControlGithubStateRepository.ts";
import { layer as GithubIntakeLive } from "./AgentControlGithubIntake.ts";
import { AgentControlGithubIntake } from "../Services/AgentControlGithubIntake.ts";
import { AgentControlGithubProjection } from "../Services/AgentControlGithubProjection.ts";
import {
  GithubIssueTrackerClient,
  GithubIssueTrackerClientError,
} from "../Services/GithubIssueTrackerClient.ts";
import { AgentControlCommandReceiptRepositoryLive } from "../../../persistence/Layers/AgentControlCommandReceipts.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import { RepositoryIdentityResolver } from "../../../project/RepositoryIdentityResolver.ts";

const repository = {
  repositoryNodeId: "repository-node-1",
  nameWithOwner: "owner/repo",
} as const;
const issue: AgentControlGithubIssueSnapshot = {
  repositoryNodeId: repository.repositoryNodeId,
  issueNodeId: "issue-node-1",
  number: 1,
  url: "https://github.com/owner/repo/issues/1",
  state: "open",
  title: "untrusted title",
  body: "untrusted body",
  contentTrust: "untrusted-external",
  updatedAt: "2026-07-22T10:00:00.000Z",
  timelineComplete: true,
  timelineEvents: [
    {
      externalEventId: "event-ready-1",
      type: "labeled",
      labelName: "agent:ready",
      actorLogin: "trusted",
      occurredAt: "2026-07-22T09:00:00.000Z",
    },
  ],
  ready: true,
  paused: false,
  eligible: true,
  eligibilityReason: "eligible",
};

const mockResolveRepository = vi.fn<GithubIssueTrackerClient["Service"]["resolveRepository"]>();
const mockPollIssues = vi.fn<GithubIssueTrackerClient["Service"]["pollIssues"]>();
const mockResolveIdentity = vi.fn<RepositoryIdentityResolver["Service"]["resolve"]>();

const infrastructure = Layer.mergeAll(
  AgentControlCommandReceiptRepositoryLive,
  AgentControlProjectionStateRepositoryLive,
  GithubEventStoreLive,
  GithubStateRepositoryLive,
);
const projectionLayer = GithubProjectionLive.pipe(Layer.provide(infrastructure));
const intakeLayer = GithubIntakeLive.pipe(
  Layer.provide(
    Layer.mergeAll(
      infrastructure,
      projectionLayer,
      Layer.mock(GithubIssueTrackerClient)({
        resolveRepository: mockResolveRepository,
        pollIssues: mockPollIssues,
      }),
      Layer.mock(RepositoryIdentityResolver)({ resolve: mockResolveIdentity }),
    ),
  ),
);
const layer = it.layer(
  Layer.mergeAll(infrastructure, projectionLayer, intakeLayer).pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const addProject = (sql: SqlClient.SqlClient, projectId: ProjectId) =>
  sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'GitHub intake test', '/server/project', NULL, '[]',
      '2026-07-22T08:00:00.000Z', '2026-07-22T08:00:00.000Z', NULL
    )
  `;

const setConfig = (
  intake: AgentControlGithubIntake["Service"],
  projectId: ProjectId,
  command = `config-${projectId}`,
) =>
  intake.setTrackerConfig({
    commandId: CommandId.make(command),
    projectId,
    expectedRevision: 0,
    trackerKind: "github",
    readyLabel: "agent:ready",
    pausedLabel: "agent:paused",
    trustedLogins: ["trusted"],
    pollIntervalSeconds: 60,
  });

const poll = (
  intake: AgentControlGithubIntake["Service"],
  projectId: ProjectId,
  command: string,
  expectedRevision: number,
) =>
  intake.pollOnce({
    commandId: CommandId.make(command),
    projectId,
    expectedRevision,
  });

afterEach(() => {
  mockResolveRepository.mockReset();
  mockPollIssues.mockReset();
  mockResolveIdentity.mockReset();
});

layer("AgentControlGithubIntake", (it) => {
  it.effect(
    "persists config, a successful cursor, normalized issues, and idempotent receipts",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const intake = yield* AgentControlGithubIntake;
        const receipts = yield* AgentControlCommandReceiptRepository;
        const projectId = ProjectId.make("project-github-success");
        yield* addProject(sql, projectId);
        mockResolveIdentity.mockReturnValue(
          Effect.succeed({
            canonicalKey: "github.com/owner/repo",
            locator: {
              source: "git-remote",
              remoteName: "origin",
              remoteUrl: "git@github.com:owner/repo.git",
            },
            rootPath: "/server/project",
            provider: "github",
            owner: "owner",
            name: "repo",
          }),
        );
        mockResolveRepository.mockReturnValue(Effect.succeed(repository));
        mockPollIssues.mockReturnValue(Effect.succeed({ repository, issues: [issue] }));

        const configured = yield* setConfig(intake, projectId);
        assert.equal(configured.state.revision, 1);
        assert.deepStrictEqual(configured.state.config?.repository, repository);
        assert.deepStrictEqual(yield* setConfig(intake, projectId), configured);
        const staleConfig = yield* Effect.result(
          setConfig(intake, projectId, `config-stale-${projectId}`),
        );
        assert.equal(staleConfig._tag, "Failure");
        if (staleConfig._tag === "Failure") {
          assert.equal(staleConfig.failure.code, "revision-conflict");
        }
        yield* sql`
        UPDATE agent_control_command_receipts
        SET authority = 'system'
        WHERE command_id = ${`config-${projectId}`}
      `;
        const wrongAuthorityReplay = yield* Effect.result(setConfig(intake, projectId));
        assert.equal(wrongAuthorityReplay._tag, "Failure");
        if (wrongAuthorityReplay._tag === "Failure") {
          assert.equal(wrongAuthorityReplay.failure.code, "command-identity-mismatch");
        }

        const firstPoll = yield* poll(intake, projectId, "poll-command", 1);
        assert.equal(firstPoll.state.revision, 2);
        assert.equal(firstPoll.state.pollStatus.status, "success");
        assert.notEqual(firstPoll.state.cursor, null);
        assert.deepStrictEqual(mockPollIssues.mock.calls[0]?.[0].knownIssues, []);
        const replay = yield* poll(intake, projectId, "poll-command", 1);
        assert.deepStrictEqual(replay, firstPoll);
        assert.equal(mockPollIssues.mock.calls.length, 1);

        const repeatedSnapshot = yield* poll(intake, projectId, "poll-command-repeat", 2);
        assert.equal(repeatedSnapshot.state.revision, 3);
        assert.equal(mockPollIssues.mock.calls[1]?.[0].knownIssues.length, 1);
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT COUNT(*) FROM agent_control_github_issues WHERE project_id = ${projectId}) AS issues,
              (SELECT COUNT(*) FROM agent_control_github_timeline_events WHERE project_id = ${projectId}) AS events
          `,
          [{ issues: 1, events: 1 }],
        );

        const listed = yield* intake.listObservedIssues({ projectId });
        assert.deepStrictEqual(listed.issues, [
          {
            issueNodeId: issue.issueNodeId,
            number: issue.number,
            url: issue.url,
            state: issue.state,
            title: issue.title,
            contentTrust: "untrusted-external",
            updatedAt: issue.updatedAt,
            ready: true,
            paused: false,
            eligible: true,
            eligibilityReason: "eligible",
          },
        ]);

        const receipt = yield* receipts.getByCommandId(CommandId.make("poll-command"));
        assert.equal(receipt._tag, "Some");
        if (receipt._tag === "Some") assert.equal(receipt.value.authority, "controller");

        const changedFingerprint = yield* Effect.result(
          intake.pollOnce({
            commandId: CommandId.make("poll-command"),
            projectId,
            expectedRevision: 3,
          }),
        );
        assert.equal(changedFingerprint._tag, "Failure");
        if (changedFingerprint._tag === "Failure") {
          assert.equal(changedFingerprint.failure.code, "command-identity-mismatch");
        }

        const clearInput = {
          commandId: CommandId.make("clear-config"),
          projectId,
          expectedRevision: 3,
        } as const;
        const cleared = yield* intake.clearTrackerConfig(clearInput);
        assert.equal(cleared.state.revision, 4);
        assert.equal(cleared.state.config, null);
        assert.equal(cleared.state.cursor, null);
        assert.equal(cleared.state.pollStatus.status, "disabled");
        assert.deepStrictEqual((yield* intake.listObservedIssues({ projectId })).issues, []);
        assert.deepStrictEqual(yield* intake.clearTrackerConfig(clearInput), cleared);
      }),
  );

  it.effect("does not advance the cursor on timeout, decode failure, or overflow", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlGithubIntake;
      const projectId = ProjectId.make("project-github-failures");
      yield* addProject(sql, projectId);
      mockResolveIdentity.mockReturnValue(
        Effect.succeed({
          canonicalKey: "github.com/owner/repo",
          locator: {
            source: "git-remote",
            remoteName: "origin",
            remoteUrl: "https://github.com/owner/repo.git",
          },
          rootPath: "/server/project",
          provider: "github",
          owner: "owner",
          name: "repo",
        }),
      );
      mockResolveRepository.mockReturnValue(Effect.succeed(repository));
      mockPollIssues.mockReturnValueOnce(Effect.succeed({ repository, issues: [issue] }));
      yield* setConfig(intake, projectId);
      const success = yield* poll(intake, projectId, "poll-success", 1);
      const cursor = success.state.cursor;

      const codes = ["github-timeout", "github-decode-failed", "pagination-overflow"] as const;
      let revision = 2;
      for (const code of codes) {
        mockPollIssues.mockReturnValueOnce(
          Effect.fail(new GithubIssueTrackerClientError({ code, operation: "list-issues" })),
        );
        const failed = yield* poll(intake, projectId, `poll-${code}`, revision);
        revision += 1;
        assert.deepStrictEqual(failed.state.cursor, cursor);
        assert.equal(failed.state.pollStatus.status, "needs-attention");
        if (failed.state.pollStatus.status === "needs-attention") {
          assert.equal(failed.state.pollStatus.errorCode, code);
          assert.equal(typeof failed.state.pollStatus.attemptedAt, "string");
          assert.equal(typeof failed.state.pollStatus.completedAt, "string");
        }
      }
      assert.equal(typeof mockPollIssues.mock.calls[1]?.[0].since, "string");
      assert.equal(
        mockPollIssues.mock.calls[1]?.[0].since,
        DateTime.formatIso(
          DateTime.subtract(DateTime.makeUnsafe(cursor!.lastSuccessfulPollAt), { seconds: 120 }),
        ),
      );
    }),
  );

  it.effect("invalidates the cursor when the repository identity changes", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlGithubIntake;
      const projectId = ProjectId.make("project-github-identity");
      yield* addProject(sql, projectId);
      mockResolveIdentity.mockReturnValue(
        Effect.succeed({
          canonicalKey: "github.com/owner/repo",
          locator: {
            source: "git-remote",
            remoteName: "origin",
            remoteUrl: "https://github.com/owner/repo.git",
          },
          rootPath: "/server/project",
          provider: "github",
          owner: "owner",
          name: "repo",
        }),
      );
      mockResolveRepository.mockReturnValue(Effect.succeed(repository));
      mockPollIssues.mockReturnValue(Effect.succeed({ repository, issues: [issue] }));
      yield* setConfig(intake, projectId);
      const success = yield* poll(intake, projectId, "identity-success", 1);
      assert.notEqual(success.state.cursor, null);

      mockPollIssues.mockReturnValueOnce(
        Effect.fail(
          new GithubIssueTrackerClientError({
            code: "repository-identity-changed",
            operation: "resolve-repository",
          }),
        ),
      );
      const remoteChanged = yield* poll(intake, projectId, "identity-node-changed", 2);
      assert.equal(remoteChanged.state.cursor, null);
      assert.equal(remoteChanged.state.pollStatus.errorCode, "repository-identity-changed");
      assert.deepStrictEqual((yield* intake.listObservedIssues({ projectId })).issues, []);

      mockResolveIdentity.mockReturnValue(
        Effect.succeed({
          canonicalKey: "github.com/owner/renamed",
          locator: {
            source: "git-remote",
            remoteName: "origin",
            remoteUrl: "https://github.com/owner/renamed.git",
          },
          rootPath: "/server/project",
          provider: "github",
          owner: "owner",
          name: "renamed",
        }),
      );
      const failed = yield* poll(intake, projectId, "identity-changed", 3);
      assert.equal(failed.state.cursor, null);
      assert.equal(failed.state.pollStatus.errorCode, "repository-identity-changed");
      assert.equal(mockPollIssues.mock.calls.length, 2);
    }),
  );

  it.effect("rejects a concurrent poll for the same project", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const intake = yield* AgentControlGithubIntake;
      const projectId = ProjectId.make("project-github-concurrency");
      yield* addProject(sql, projectId);
      mockResolveIdentity.mockReturnValue(
        Effect.succeed({
          canonicalKey: "github.com/owner/repo",
          locator: {
            source: "git-remote",
            remoteName: "origin",
            remoteUrl: "https://github.com/owner/repo.git",
          },
          rootPath: "/server/project",
          provider: "github",
          owner: "owner",
          name: "repo",
        }),
      );
      mockResolveRepository.mockReturnValue(Effect.succeed(repository));
      yield* setConfig(intake, projectId);
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      mockPollIssues.mockReturnValue(
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as({ repository, issues: [] }),
        ),
      );
      const first = yield* Effect.forkChild(poll(intake, projectId, "parallel-first", 1));
      yield* Deferred.await(started);
      const second = yield* Effect.result(poll(intake, projectId, "parallel-second", 1));
      assert.equal(second._tag, "Failure");
      if (second._tag === "Failure") assert.equal(second.failure.code, "poll-in-progress");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(first);
      assert.equal(mockPollIssues.mock.calls.length, 1);
    }),
  );

  it.effect(
    "rebuilds Observe projections without touching policies or controller projections",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const intake = yield* AgentControlGithubIntake;
        const projection = yield* AgentControlGithubProjection;
        const projectId = ProjectId.make("project-github-rebuild");
        yield* addProject(sql, projectId);
        mockResolveIdentity.mockReturnValue(
          Effect.succeed({
            canonicalKey: "github.com/owner/repo",
            locator: {
              source: "git-remote",
              remoteName: "origin",
              remoteUrl: "https://github.com/owner/repo.git",
            },
            rootPath: "/server/project",
            provider: "github",
            owner: "owner",
            name: "repo",
          }),
        );
        mockResolveRepository.mockReturnValue(Effect.succeed(repository));
        mockPollIssues.mockReturnValue(Effect.succeed({ repository, issues: [issue] }));
        yield* setConfig(intake, projectId);
        yield* poll(intake, projectId, "rebuild-poll", 1);
        yield* sql`
        INSERT INTO agent_control_project_policies (project_id, policy_json, revision, updated_at)
        VALUES (${projectId}, '{"fullAccess":false}', 1, '2026-07-22T10:00:00.000Z')
      `;
        yield* sql`
        INSERT INTO agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES (${projectId}, 'observe', NULL, 1, 999, '2026-07-22T10:00:00.000Z')
      `;
        yield* sql`DELETE FROM agent_control_github_issues WHERE project_id = ${projectId}`;

        yield* projection.rebuild;

        assert.equal((yield* intake.listObservedIssues({ projectId })).issues.length, 1);
        assert.deepStrictEqual(
          yield* sql`SELECT revision FROM agent_control_project_policies WHERE project_id = ${projectId}`,
          [{ revision: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT mode, revision FROM agent_control_project_states WHERE project_id = ${projectId}`,
          [{ mode: "observe", revision: 1 }],
        );
      }),
  );

  it.effect(
    "rolls back event, projections, cursor, and receipt when projection persistence fails",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const intake = yield* AgentControlGithubIntake;
        const projectId = ProjectId.make("project-github-atomic");
        yield* addProject(sql, projectId);
        mockResolveIdentity.mockReturnValue(
          Effect.succeed({
            canonicalKey: "github.com/owner/repo",
            locator: {
              source: "git-remote",
              remoteName: "origin",
              remoteUrl: "https://github.com/owner/repo.git",
            },
            rootPath: "/server/project",
            provider: "github",
            owner: "owner",
            name: "repo",
          }),
        );
        mockResolveRepository.mockReturnValue(Effect.succeed(repository));
        yield* setConfig(intake, projectId);
        mockPollIssues.mockReturnValue(
          Effect.succeed({
            repository,
            issues: [issue, { ...issue, issueNodeId: "issue-node-conflict" }],
          }),
        );

        const result = yield* Effect.result(poll(intake, projectId, "atomic-poll", 1));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure.code, "internal-persistence-error");
        }
        assert.deepStrictEqual(
          yield* sql`
          SELECT event_type AS type FROM agent_control_events
          WHERE aggregate_kind = 'github-intake' AND stream_id = ${projectId}
          ORDER BY stream_version
        `,
          [{ type: "agentControl.github.config.set" }],
        );
        assert.deepStrictEqual(
          yield* sql`
          SELECT revision FROM agent_control_github_intake_states
          WHERE project_id = ${projectId}
        `,
          [{ revision: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`
          SELECT command_id FROM agent_control_command_receipts
          WHERE command_id = 'atomic-poll'
        `,
          [],
        );
      }),
  );
});
