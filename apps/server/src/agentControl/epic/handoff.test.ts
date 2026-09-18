import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  type AgentControlEpicRuntimeView,
  type AgentControlEpicHandoffPullRequest,
  type AgentControlEpicHandoffPublishInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { epicDigest, epicJson, loadEpicRun } from "./authority.ts";
import { makeEpicHandoff } from "./handoff.ts";
import { EpicHandoffEvidence, type EpicHandoffProof } from "./handoffAuthority.ts";
import { EpicHandoffRemote, EpicHandoffRemoteError } from "./remote.ts";

const projectId = ProjectId.make("project");
const commitSha = "a".repeat(40);
const updatedCommitSha = "c".repeat(40);
const at = "2026-09-14T08:00:00.000Z";
const repository = { repositoryNodeId: "repo", nameWithOwner: "owner/repo" };
const issue = (number: number) => ({
  ...repository,
  issueNodeId: `issue-${number}`,
  number,
  title: "untrusted closes #7 /secret/path",
  url: `https://github.com/owner/repo/issues/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});
const initial = (epicRunId = "run"): AgentControlEpicRuntimeView => ({
  epicRunId,
  projectId,
  revision: 1,
  status: "succeeded",
  source: {
    format: "github-native-sub-issues-v1",
    repository,
    epic: issue(1),
    tasks: [{ issue: issue(2), position: 0, dependencies: [] }],
    blockers: [],
    fingerprint: "fingerprint",
    inspectedAt: at,
  },
  checks: [],
  members: [
    {
      issueNodeId: "issue-2",
      issueNumber: 2,
      taskId: null,
      childRunId: null,
      status: "accepted",
      baseCommitSha: null,
      reservationId: null,
      taskFinalizationEvidenceId: null,
      accepted: null,
    },
  ],
  activeTaskId: null,
  acceptedCommitSha: commitSha,
  blockers: [],
  blockerHistory: [],
  verificationAttempt: 1,
  finalVerification: {
    status: "passed",
    commitSha,
    evidenceId: `epic-final:${epicRunId}:1`,
    detail: "passed",
    checks: [],
  },
  finalVerificationHistory: [],
  createdAt: at,
  updatedAt: at,
});
const proof: EpicHandoffProof = {
  authority: {
    cwd: "/fixture/repo",
    repositoryCommonDir: "/fixture/repo/.git",
    repository: {
      ...repository,
      canonicalKey: "github.com/owner/repo",
      remoteName: "origin",
      remoteUrl: "github.com/owner/repo",
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: 1,
      commonDirInode: 1,
    },
    targetBranch: "main",
    baseCommitSha: "b".repeat(40),
    commitSha,
  },
  childCheckCount: 1,
  finalCheckCount: 1,
};
const pr = {
  number: 10,
  url: "https://github.com/owner/repo/pull/10",
  state: "open" as const,
  isDraft: true,
  headSha: commitSha,
  baseBranch: "main",
};
const request = (epicRunId = "run"): AgentControlEpicHandoffPublishInput => ({
  projectId,
  epicRunId,
  commandId: CommandId.make("publish"),
  expectedRevision: 1,
  expectedCommitSha: commitSha,
  expectedTargetBranch: "main",
});
const updateRequired = (epicRunId = "run"): AgentControlEpicRuntimeView => {
  const state = initial(epicRunId);
  const published = {
    intentId: "retained-review-handoff",
    status: "published" as const,
    repository,
    targetBranch: "main",
    baseCommitSha: proof.authority.baseCommitSha,
    commitSha,
    branchName: "t3auto/epic-1-retained",
    verificationEvidenceId: state.finalVerification!.evidenceId,
    branchCreationAttempted: true,
    requestedAt: at,
    updatedAt: at,
    pullRequest: pr,
    error: null,
  };
  const verification = {
    status: "passed" as const,
    commitSha: updatedCommitSha,
    evidenceId: `epic-review-final:${epicRunId}:2`,
    detail: "review repair passed",
    checks: [],
  };
  return {
    ...state,
    acceptedCommitSha: updatedCommitSha,
    verificationAttempt: 2,
    finalVerification: verification,
    finalVerificationHistory: [state.finalVerification!, verification],
    handoff: {
      ...published,
      status: "update-required",
      commitSha: updatedCommitSha,
      verificationEvidenceId: verification.evidenceId,
    },
    handoffHistory: [
      {
        handoff: published,
        supersededByReviewRequestId: "review-request-1",
        supersededAt: at,
      },
    ],
  };
};
const updateRequest = (state = updateRequired()): AgentControlEpicHandoffPublishInput => ({
  ...request(state.epicRunId),
  expectedRevision: state.revision,
  expectedCommitSha: updatedCommitSha,
});
const updateProof: EpicHandoffProof = {
  ...proof,
  authority: { ...proof.authority, commitSha: updatedCommitSha },
};
const seed = Effect.fn("seedHandoff")(function* (state: AgentControlEpicRuntimeView) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_projects(project_id TEXT PRIMARY KEY, workspace_root TEXT, deleted_at TEXT)`;
  yield* sql`INSERT OR IGNORE INTO projection_projects VALUES(${state.projectId}, '/fixture/repo', NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_control_epic_runs(epic_run_id TEXT PRIMARY KEY,project_id TEXT,revision INTEGER,state_json TEXT,state_digest TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_control_epic_history(epic_run_id TEXT,revision INTEGER,state_json TEXT,state_digest TEXT,PRIMARY KEY(epic_run_id,revision))`;
  yield* sql`INSERT INTO agent_control_epic_runs VALUES(${state.epicRunId},${state.projectId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
  yield* sql`INSERT INTO agent_control_epic_history VALUES(${state.epicRunId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
});
type TestRemote = Omit<EpicHandoffRemote["Service"], "readPullRequest"> &
  Partial<Pick<EpicHandoffRemote["Service"], "readPullRequest">>;
const build = (
  remote: TestRemote,
  verify: EpicHandoffEvidence["Service"]["verify"] = () => Effect.succeed(proof),
) =>
  Effect.gen(function* () {
    const semaphore = yield* Semaphore.make(1);
    return yield* makeEpicHandoff({
      onChange: () => Effect.void,
      withProjectLock: (_id, effect) => semaphore.withPermit(effect),
    }).pipe(
      Effect.provideService(EpicHandoffEvidence, { verify }),
      Effect.provideService(EpicHandoffRemote, {
        readPullRequest: () => Effect.succeed(pr),
        ...remote,
      }),
    );
  });

describe("Epic handoff persistence and recovery", () => {
  it.effect(
    "previews and serializes an update-required publication onto the retained pull request",
    () =>
      Effect.gen(function* () {
        const state = updateRequired();
        yield* seed(state);
        let publishes = 0;
        let reads = 0;
        const updatedPr = { ...pr, headSha: updatedCommitSha };
        const service = yield* build(
          {
            prepare: () => Effect.die("An existing pull request update must not prepare a new PR"),
            readPullRequest: () => {
              reads++;
              return Effect.succeed(pr);
            },
            publish: (input) =>
              Effect.sync(() => {
                publishes++;
                assert.equal(input.commitSha, updatedCommitSha);
                assert.equal(input.expectedPreviousCommitSha, commitSha);
                assert.equal(input.ownershipCommitSha, commitSha);
                assert.equal(input.branchName, state.handoff?.branchName);
                return updatedPr;
              }),
          },
          () => Effect.succeed(updateProof),
        );
        const preview = yield* service.previewHandoff(updateRequest(state));
        assert.isTrue(preview.canPublish);
        assert.equal(preview.handoff?.status, "update-required");
        assert.equal(preview.handoff?.pullRequest?.headSha, commitSha);
        const results = yield* Effect.all(
          [
            service.publishHandoff(updateRequest(state)),
            service.publishHandoff({
              ...updateRequest(state),
              commandId: CommandId.make("concurrent-update"),
            }),
          ],
          { concurrency: "unbounded" },
        );
        assert.deepEqual(results[0], results[1]);
        assert.equal(results[0]?.handoff?.status, "published");
        assert.equal(results[0]?.handoff?.pullRequest?.number, pr.number);
        assert.equal(results[0]?.handoff?.pullRequest?.headSha, updatedCommitSha);
        assert.equal(results[0]?.handoffHistory?.[0]?.handoff.commitSha, commitSha);
        assert.equal(publishes, 1);
        assert.equal(reads, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("recovers a persisted update publication after a process restart", () =>
    Effect.gen(function* () {
      const state = updateRequired();
      yield* seed(state);
      let crashed = false;
      let publishes = 0;
      const updatedPr = { ...pr, headSha: updatedCommitSha };
      const remote: TestRemote = {
        prepare: () => Effect.die("An existing pull request update must not prepare a new PR"),
        readPullRequest: () => Effect.succeed(pr),
        publish: (input) => {
          publishes++;
          assert.equal(input.expectedPreviousCommitSha, commitSha);
          if (!crashed) {
            crashed = true;
            return Effect.die("Simulated process crash after the update intent was persisted");
          }
          return Effect.succeed(updatedPr);
        },
      };
      const first = yield* build(remote, () => Effect.succeed(updateProof));
      assert.isTrue(Exit.isFailure(yield* Effect.exit(first.publishHandoff(updateRequest(state)))));
      assert.equal(
        (yield* loadEpicRun(yield* SqlClient.SqlClient, state.epicRunId))?.handoff?.status,
        "publishing",
      );
      const restarted = yield* build(remote, () => Effect.succeed(updateProof));
      yield* restarted.recoverPending();
      const recovered = yield* loadEpicRun(yield* SqlClient.SqlClient, state.epicRunId);
      assert.equal(recovered?.handoff?.status, "published");
      assert.equal(recovered?.handoff?.pullRequest?.headSha, updatedCommitSha);
      assert.equal(publishes, 2);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  for (const condition of ["closed", "merged", "not-draft"] as const)
    it.effect(`retains the update intent through ${condition} and a later draft reopen`, () =>
      Effect.gen(function* () {
        const state = updateRequired(`update-${condition}`);
        yield* seed(state);
        let observed: AgentControlEpicHandoffPullRequest = {
          ...pr,
          ...(condition === "closed" ? { state: "closed" as const } : {}),
          ...(condition === "merged"
            ? {
                state: "merged" as const,
                isDraft: false,
                mergeCommitSha: "d".repeat(40),
              }
            : {}),
          ...(condition === "not-draft" ? { isDraft: false } : {}),
        };
        const service = yield* build(
          {
            prepare: () => Effect.die("An update intent must not prepare a replacement PR"),
            publish: () => Effect.die("Preview must not publish the retained update"),
            readPullRequest: () => Effect.succeed(observed),
          },
          () => Effect.succeed(updateProof),
        );

        const unavailable = yield* service.previewHandoff(updateRequest(state));
        assert.equal(unavailable.handoff?.status, "update-required");
        assert.notEqual(unavailable.handoff?.status, "published");
        assert.isFalse(unavailable.canPublish);
        assert.equal(unavailable.handoff?.pullRequest?.state, observed.state);
        assert.equal(
          (yield* loadEpicRun(yield* SqlClient.SqlClient, state.epicRunId))?.handoffHistory?.[0]
            ?.handoff.commitSha,
          commitSha,
        );

        observed = { ...pr, state: "open", isDraft: true };
        const reopened = yield* service.previewHandoff(updateRequest(state));
        assert.equal(reopened.handoff?.status, "update-required");
        assert.notEqual(reopened.handoff?.status, "published");
        assert.isTrue(reopened.canPublish);
        assert.isNull(reopened.handoff?.error);
        assert.equal(reopened.handoff?.pullRequest?.headSha, commitSha);
        assert.equal(
          (yield* loadEpicRun(yield* SqlClient.SqlClient, state.epicRunId))?.handoffHistory?.[0]
            ?.handoff.commitSha,
          commitSha,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  for (const state of ["closed", "merged"] as const)
    it.effect(
      `refreshes a published PR to ${state} after restart without creating a replacement`,
      () =>
        Effect.gen(function* () {
          yield* seed(initial());
          let publishes = 0;
          let reads = 0;
          const remote: TestRemote = {
            prepare: () => Effect.die("Publication preparation must not run for a saved PR"),
            publish: () => {
              publishes++;
              return Effect.succeed(pr);
            },
            readPullRequest: (input) => {
              reads++;
              assert.deepEqual(input, { cwd: "/fixture/repo", repository, pullRequest: pr });
              return Effect.succeed({ ...pr, state });
            },
          };
          const first = yield* build(remote);
          const published = yield* first.publishHandoff(request());
          const restarted = yield* build(remote, () =>
            Effect.die("Publication verification must not gate reading a saved PR"),
          );
          yield* restarted.recoverPending();
          const preview = yield* restarted.previewHandoff(request());
          assert.isFalse(preview.canPublish);
          assert.equal(preview.handoff?.pullRequest?.state, state);
          assert.equal(preview.handoff?.status, "blocked");
          assert.equal(preview.handoff?.intentId, published.handoff?.intentId);
          const saved = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
          assert.deepEqual(saved?.handoff, preview.handoff);
          assert.equal(saved?.acceptedCommitSha, commitSha);
          assert.deepEqual(saved?.finalVerification, published.finalVerification);
          const replay = yield* restarted.publishHandoff(request());
          assert.deepEqual(replay, saved);
          assert.equal(publishes, 1);
          assert.equal(reads, 1);
        }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );

  it.effect(
    "persists reopened and ready PR states, retaining the verified commit when humans update the PR",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        let current: AgentControlEpicHandoffPullRequest = { ...pr, state: "closed" };
        let publishes = 0;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: () => {
            publishes++;
            return Effect.succeed(pr);
          },
          readPullRequest: () => Effect.succeed(current),
        });
        yield* service.publishHandoff(request());
        yield* service.previewHandoff(request());
        current = { ...pr, isDraft: false, headSha: "c".repeat(40), baseBranch: "review" };
        const ready = yield* service.previewHandoff(request());
        assert.equal(ready.handoff?.pullRequest?.isDraft, false);
        assert.equal(ready.handoff?.pullRequest?.headSha, current.headSha);
        assert.equal(ready.commitSha, commitSha);
        assert.equal(ready.handoff?.commitSha, commitSha);
        assert.equal(ready.handoff?.targetBranch, "main");
        current = { ...current, isDraft: true };
        const reopened = yield* service.previewHandoff(request());
        assert.equal(reopened.handoff?.status, "published");
        assert.isNull(reopened.handoff?.error);
        assert.isFalse(reopened.canPublish);
        assert.equal(publishes, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("retains the published result when refresh fails and permits a read retry", () =>
    Effect.gen(function* () {
      yield* seed(initial());
      let unavailable = true;
      const service = yield* build({
        prepare: () => Effect.void,
        publish: () => Effect.succeed(pr),
        readPullRequest: () =>
          unavailable
            ? Effect.fail(
                new EpicHandoffRemoteError({
                  code: "remote-unavailable",
                  message: "Retry the PR read.",
                }),
              )
            : Effect.succeed({ ...pr, state: "merged" }),
      });
      const published = yield* service.publishHandoff(request());
      const failed = yield* service.previewHandoff(request());
      assert.deepEqual(failed.handoff, published.handoff);
      assert.isFalse(failed.canPublish);
      assert.equal(failed.blockers[0]?.code, "remote-unavailable");
      assert.deepEqual(yield* loadEpicRun(yield* SqlClient.SqlClient, "run"), published);
      unavailable = false;
      assert.equal(
        (yield* service.previewHandoff(request())).handoff?.pullRequest?.state,
        "merged",
      );
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "serializes concurrent refreshes, writes changed state once, and isolates project/run identities",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        yield* seed(initial("other-run"));
        let reads = 0;
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const service = yield* build({
          prepare: () => Effect.void,
          publish: () => Effect.succeed(pr),
          readPullRequest: () =>
            Effect.gen(function* () {
              reads++;
              yield* Deferred.succeed(started, undefined);
              yield* Deferred.await(release);
              return { ...pr, state: "closed" as const };
            }),
        });
        const published = yield* service.publishHandoff(request());
        const first = yield* service.previewHandoff(request()).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        const second = yield* service.previewHandoff(request()).pipe(Effect.forkChild);
        yield* Deferred.succeed(release, undefined);
        const results = yield* Effect.all([Fiber.join(first), Fiber.join(second)]);
        assert.deepEqual(results[0], results[1]);
        assert.equal(reads, 2);
        const sql = yield* SqlClient.SqlClient;
        const saved = yield* loadEpicRun(sql, "run");
        assert.equal(saved?.revision, published.revision + 1);
        assert.isUndefined((yield* loadEpicRun(sql, "other-run"))?.handoff);
        const wrongProject = yield* Effect.result(
          service.previewHandoff({ ...request(), projectId: ProjectId.make("other-project") }),
        );
        const missingRun = yield* Effect.result(
          service.previewHandoff({ ...request(), epicRunId: "absent" }),
        );
        assert.equal(wrongProject._tag, "Failure");
        assert.equal(missingRun._tag, "Failure");
        assert.equal(reads, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "publishes the exact accepted commit once under concurrent requests and replays durable history",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        const sql = yield* SqlClient.SqlClient;
        let calls = 0;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (input, hooks) =>
            Effect.gen(function* () {
              calls++;
              assert.equal(input.commitSha, commitSha);
              const saved = yield* loadEpicRun(sql, "run").pipe(Effect.orDie);
              assert.equal(saved?.handoff?.intentId, input.ownershipToken);
              yield* hooks.beforeBranchCreate();
              return pr;
            }),
        });
        const results = yield* Effect.all(
          [
            service.publishHandoff(request()),
            service.publishHandoff({ ...request(), commandId: CommandId.make("other-client") }),
          ],
          { concurrency: "unbounded" },
        );
        assert.equal(calls, 1);
        assert.equal(results[0]!.handoff?.status, "published");
        assert.deepEqual(results[0], results[1]);
        const reloaded = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
        assert.deepEqual(reloaded, results[0]);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  for (const boundary of ["uploaded", "branch-created", "pr-created"] as const)
    it.effect(`resumes one durable intent after restart at ${boundary}`, () =>
      Effect.gen(function* () {
        yield* seed(initial());
        let interrupted = false;
        let uploads = 0;
        let branches = 0;
        let prs = 0;
        let token: string | undefined;
        const adapter: TestRemote = {
          prepare: () => Effect.void,
          publish: (input, hooks) =>
            Effect.gen(function* () {
              if (token) assert.equal(input.ownershipToken, token);
              token = input.ownershipToken;
              if (!uploads) uploads++;
              if (boundary === "uploaded" && !interrupted) {
                interrupted = true;
                return yield* Effect.die("Simulated process crash without shutdown cleanup");
              }
              if (!branches) {
                yield* hooks.beforeBranchCreate();
                branches++;
              }
              if (boundary === "branch-created" && !interrupted) {
                interrupted = true;
                return yield* Effect.die("Simulated process crash without shutdown cleanup");
              }
              if (!prs) prs++;
              if (boundary === "pr-created" && !interrupted) {
                interrupted = true;
                return yield* Effect.die("Simulated process crash without shutdown cleanup");
              }
              return pr;
            }),
        };
        const first = yield* build(adapter);
        assert.isTrue(Exit.isFailure(yield* Effect.exit(first.publishHandoff(request()))));
        const pending = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
        assert.equal(pending?.handoff?.status, "publishing");
        const restarted = yield* build(adapter);
        yield* restarted.recoverPending();
        const recovered = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
        assert.equal(recovered?.handoff?.status, "published");
        assert.equal(uploads, 1);
        assert.equal(branches, 1);
        assert.equal(prs, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );
  it.effect(
    "allows the same server to retry after a client disconnect without duplicating the intent",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        const started = yield* Deferred.make<void>();
        let attempts = 0;
        let intent: string | undefined;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (input, hooks) =>
            Effect.gen(function* () {
              if (intent) assert.equal(input.ownershipToken, intent);
              intent = input.ownershipToken;
              attempts++;
              if (attempts === 1) {
                yield* hooks.beforeBranchCreate();
                yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }
              assert.isTrue(input.branchCreationAttempted);
              return pr;
            }),
        });
        const requestFiber = yield* service.publishHandoff(request()).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(requestFiber);
        const failed = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
        assert.equal(failed?.handoff?.status, "failed");
        assert.equal(failed?.status, "succeeded");
        assert.isTrue((yield* service.previewHandoff(request())).canPublish);
        const retried = yield* service.publishHandoff(request());
        assert.equal(retried.handoff?.status, "published");
        assert.equal(attempts, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "retains a known collision when the client disconnects before its outcome is saved",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        const sql = yield* SqlClient.SqlClient;
        const savingCollision = yield* Deferred.make<void>();
        const allowSave = yield* Deferred.make<void>();
        let writes = 0;
        const withTransaction: SqlClient.SqlClient["withTransaction"] = (effect) =>
          Effect.gen(function* () {
            writes++;
            if (writes === 3) {
              yield* Deferred.succeed(savingCollision, undefined);
              yield* Deferred.await(allowSave);
            }
            return yield* sql.withTransaction(effect);
          });
        const controlledSql = new Proxy(sql, {
          get: (target, key, receiver) =>
            key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
        });
        let calls = 0;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (_input, hooks) =>
            Effect.gen(function* () {
              calls++;
              yield* hooks.beforeBranchCreate();
              return yield* new EpicHandoffRemoteError({
                code: "remote-branch-collision",
                message: "Existing foreign branch.",
              });
            }),
        }).pipe(Effect.provideService(SqlClient.SqlClient, controlledSql));
        const requestFiber = yield* service.publishHandoff(request()).pipe(Effect.forkChild);
        yield* Deferred.await(savingCollision);
        yield* Effect.sync(() => requestFiber.interruptUnsafe());
        yield* Deferred.succeed(allowSave, undefined);
        yield* Fiber.await(requestFiber);
        const saved = yield* loadEpicRun(sql, "run");
        assert.equal(saved?.handoff?.status, "blocked");
        assert.equal(saved?.handoff?.error?.code, "remote-branch-collision");
        yield* service.publishHandoff(request());
        assert.equal(calls, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  for (const state of ["closed", "merged"] as const)
    it.effect(`retains an existing ${state} PR and never replaces it`, () =>
      Effect.gen(function* () {
        yield* seed(initial());
        let calls = 0;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: () =>
            Effect.sync(() => {
              calls++;
              return { ...pr, state };
            }),
        });
        const result = yield* service.publishHandoff(request());
        assert.equal(result.handoff?.status, "blocked");
        assert.equal(result.handoff?.pullRequest?.state, state);
        yield* service.publishHandoff(request());
        assert.equal(calls, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );
  it.effect(
    "keeps local success and resumes the same branch and intent after an uncertain response",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        let first = true;
        let name: string | undefined;
        let token: string | undefined;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (input) =>
            Effect.gen(function* () {
              if (name) assert.equal(input.branchName, name);
              if (token) assert.equal(input.ownershipToken, token);
              name = input.branchName;
              token = input.ownershipToken;
              if (first) {
                first = false;
                return yield* new EpicHandoffRemoteError({
                  code: "remote-unavailable",
                  message: "Response lost.",
                });
              }
              return pr;
            }),
        });
        const blocked = yield* service.publishHandoff(request());
        assert.equal(blocked.status, "succeeded");
        assert.equal(blocked.acceptedCommitSha, commitSha);
        assert.equal(blocked.handoff?.status, "failed");
        const retried = yield* service.publishHandoff(request());
        assert.equal(retried.handoff?.status, "published");
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "rejects project and confirmation mismatches before remote work and isolates runs",
    () =>
      Effect.gen(function* () {
        yield* seed(initial());
        yield* seed(initial("run-2"));
        let calls = 0;
        const branches: string[] = [];
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (input) =>
            Effect.sync(() => {
              calls++;
              branches.push(input.branchName);
              return pr;
            }),
        });
        assert.equal(
          (yield* service
            .publishHandoff({ ...request(), projectId: ProjectId.make("other") })
            .pipe(Effect.flip)).code,
          "epic-unavailable",
        );
        assert.equal(
          (yield* service
            .publishHandoff({ ...request(), expectedCommitSha: "c".repeat(40) })
            .pipe(Effect.flip)).code,
          "handoff-confirmation-stale",
        );
        assert.equal(
          (yield* service
            .publishHandoff({ ...request(), expectedTargetBranch: "other" })
            .pipe(Effect.flip)).code,
          "handoff-confirmation-stale",
        );
        assert.equal(calls, 0);
        yield* service.publishHandoff(request());
        yield* service.publishHandoff(request("run-2"));
        assert.notEqual(branches[0], branches[1]);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  for (const code of ["remote-branch-collision", "pull-request-collision"])
    it.effect(`never retries a known ${code} as ambiguous success`, () =>
      Effect.gen(function* () {
        yield* seed(initial());
        let calls = 0;
        const service = yield* build({
          prepare: () => Effect.void,
          publish: (_input, hooks) =>
            Effect.gen(function* () {
              calls++;
              yield* hooks.beforeBranchCreate();
              return yield* new EpicHandoffRemoteError({
                code,
                message: "The remote name belongs to other work.",
              });
            }),
        });
        const first = yield* service.publishHandoff(request());
        assert.equal(first.handoff?.status, "blocked");
        const second = yield* service.publishHandoff(request());
        assert.deepEqual(second, first);
        assert.equal(calls, 1);
        assert.isFalse((yield* service.previewHandoff(request())).canPublish);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );
  it.effect("forgets a rejected branch attempt durably before retrying the same intent", () =>
    Effect.gen(function* () {
      yield* seed(initial());
      let calls = 0;
      let token: string | undefined;
      const service = yield* build({
        prepare: (input) => Effect.sync(() => assert.isFalse(input.branchCreationAttempted)),
        publish: (input, hooks) =>
          Effect.gen(function* () {
            assert.isFalse(input.branchCreationAttempted);
            if (token) assert.equal(input.ownershipToken, token);
            token = input.ownershipToken;
            calls++;
            yield* hooks.beforeBranchCreate();
            if (calls === 1)
              return yield* new EpicHandoffRemoteError({
                code: "remote-branch-rejected",
                message: "Request rejected.",
              });
            return pr;
          }),
      });
      const rejected = yield* service.publishHandoff(request());
      assert.equal(rejected.handoff?.status, "failed");
      assert.isFalse(rejected.handoff?.branchCreationAttempted);
      assert.isTrue((yield* service.previewHandoff(request())).canPublish);
      const saved = yield* loadEpicRun(yield* SqlClient.SqlClient, "run");
      assert.isFalse(saved?.handoff?.branchCreationAttempted);
      assert.equal((yield* service.publishHandoff(request())).handoff?.status, "published");
      assert.equal(calls, 2);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("does not retain branch creation authority when saving the attempt fails", () =>
    Effect.gen(function* () {
      yield* seed(initial());
      const sql = yield* SqlClient.SqlClient;
      let writes = 0;
      const withTransaction: SqlClient.SqlClient["withTransaction"] = (effect) =>
        Effect.gen(function* () {
          writes++;
          if (writes === 2) yield* sql`SELECT missing_column FROM agent_control_epic_runs`;
          return yield* sql.withTransaction(effect);
        });
      const controlledSql = new Proxy(sql, {
        get: (target, key, receiver) =>
          key === "withTransaction" ? withTransaction : Reflect.get(target, key, receiver),
      });
      let remoteCreates = 0;
      const service = yield* build({
        prepare: () => Effect.void,
        publish: (input, hooks) =>
          Effect.gen(function* () {
            assert.isFalse(input.branchCreationAttempted);
            yield* hooks.beforeBranchCreate();
            remoteCreates++;
            return pr;
          }),
      }).pipe(Effect.provideService(SqlClient.SqlClient, controlledSql));
      const failed = yield* service.publishHandoff(request());
      assert.equal(failed.handoff?.error?.code, "handoff-persistence-failed");
      assert.isFalse(failed.handoff?.branchCreationAttempted);
      assert.equal(remoteCreates, 0);
      assert.equal((yield* service.publishHandoff(request())).handoff?.status, "published");
      assert.equal(remoteCreates, 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});
