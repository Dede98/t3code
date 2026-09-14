import { assert, describe, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  type AgentControlEpicRuntimeView,
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
const seed = Effect.fn("seedHandoff")(function* (state: AgentControlEpicRuntimeView) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_control_epic_runs(epic_run_id TEXT PRIMARY KEY,project_id TEXT,revision INTEGER,state_json TEXT,state_digest TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_control_epic_history(epic_run_id TEXT,revision INTEGER,state_json TEXT,state_digest TEXT,PRIMARY KEY(epic_run_id,revision))`;
  yield* sql`INSERT INTO agent_control_epic_runs VALUES(${state.epicRunId},${state.projectId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
  yield* sql`INSERT INTO agent_control_epic_history VALUES(${state.epicRunId},${state.revision},${epicJson(state)},${epicDigest(state)})`;
});
const build = (remote: EpicHandoffRemote["Service"]) =>
  Effect.gen(function* () {
    const semaphore = yield* Semaphore.make(1);
    return yield* makeEpicHandoff({
      onChange: () => Effect.void,
      withProjectLock: (_id, effect) => semaphore.withPermit(effect),
    }).pipe(
      Effect.provideService(EpicHandoffEvidence, { verify: () => Effect.succeed(proof) }),
      Effect.provideService(EpicHandoffRemote, remote),
    );
  });

describe("Epic handoff persistence and recovery", () => {
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
        const adapter: EpicHandoffRemote["Service"] = {
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
