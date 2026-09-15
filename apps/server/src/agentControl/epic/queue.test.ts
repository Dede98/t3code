import {
  AgentControlEpicRpcError,
  AgentControlInternalPersistenceError,
  CommandId,
  ProjectId,
  type AgentControlEpicHandoff,
  type AgentControlEpicHandoffPullRequest,
  type AgentControlEpicPreview,
  type AgentControlEpicQueueChangeInput,
  type AgentControlEpicSource,
  type AgentControlProjectState,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { epicJson, loadEpicRun, loadSelectedEpic, saveEpicRun } from "./authority.ts";
import { makeEpicQueue } from "./queue.ts";
import { loadEpicQueue } from "./queueAuthority.ts";
import { EpicHandoffRemote, EpicHandoffRemoteError, type EpicQueueBaseInput } from "./remote.ts";
import { createEpicRun, insertEpicRun } from "./runState.ts";

const projectId = ProjectId.make("queue-project");
const at = "2026-09-15T08:00:00.000Z";
const repository = { repositoryNodeId: "queue-repository", nameWithOwner: "owner/repo" };
const initialSha = "a".repeat(40);
const resultSha = "b".repeat(40);
const mergedSha = "c".repeat(40);
const issue = (number: number) => ({
  ...repository,
  issueNodeId: `issue-${number}`,
  number,
  title: `Issue ${number}`,
  url: `https://github.com/owner/repo/issues/${number}`,
  state: "open" as const,
  subIssueCount: 0,
});
const source = (number: number): AgentControlEpicSource => ({
  format: "github-native-sub-issues-v1",
  repository,
  epic: { ...issue(number), subIssueCount: 1 },
  dependencies: [],
  tasks: [{ issue: issue(number + 100), position: 0, dependencies: [] }],
  blockers: [],
  fingerprint: `fingerprint-${number}`,
  inspectedAt: at,
});
const pullRequest: AgentControlEpicHandoffPullRequest = {
  number: 301,
  url: "https://github.com/owner/repo/pull/301",
  state: "open",
  isDraft: true,
  headSha: resultSha,
  baseBranch: "main",
  mergeCommitSha: null,
};
const handoff: AgentControlEpicHandoff = {
  intentId: "approved-publication",
  status: "published",
  repository,
  targetBranch: "main",
  baseCommitSha: initialSha,
  commitSha: resultSha,
  branchName: "t3auto/epic-a",
  verificationEvidenceId: "shared-check-a",
  requestedAt: at,
  updatedAt: at,
  pullRequest,
  error: null,
};
const unavailable = (code: string) => new EpicHandoffRemoteError({ code, message: code });
const isEpicError = Schema.is(AgentControlEpicRpcError);
const isPersistenceError = Schema.is(AgentControlInternalPersistenceError);
const assertEpicError = (error: unknown, code: string) => {
  assert(isEpicError(error));
  assert.equal(error.code, code);
};

const fixture = Effect.fn("epicQueueFixture")(function* (id = projectId) {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: 85 });
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,created_at,updated_at,scripts_json) VALUES (${id},'Queue test','/isolated/queue',${at},${at},'[]')`;
  yield* sql`INSERT INTO agent_control_project_policies(project_id,policy_json,revision,updated_at) VALUES (${id},${epicJson({ verificationChecks: [] })},1,${at})`;
  let project: AgentControlProjectState = {
    schemaVersion: 1,
    projectId: id,
    mode: "observe",
    pausedFromMode: null,
    revision: 1,
    sequence: 1,
    updatedAt: at,
  };
  let authorityLost = false;
  const engine = AgentControlEngine.of({
    getProjectState: () =>
      Effect.suspend(() =>
        authorityLost
          ? Effect.fail(
              new AgentControlInternalPersistenceError({ code: "internal-persistence-error" }),
            )
          : Effect.succeed(project),
      ),
    dispatchHuman: () => Effect.die("Queue must not change Armed authority"),
    dispatchController: () => Effect.die("Queue must not change Armed authority"),
    dispatchSystem: () => Effect.die("Queue must not change Armed authority"),
    streamDomainEvents: Stream.never,
  });
  let observation = pullRequest;
  let readError: EpicHandoffRemoteError | undefined;
  let fetchError: EpicHandoffRemoteError | undefined;
  let fetchGate = Effect.void;
  let reads = 0;
  const fetched: EpicQueueBaseInput[] = [];
  const previews = new Map<number, AgentControlEpicPreview>();
  const remote = EpicHandoffRemote.of({
    readPullRequest: (input) =>
      Effect.suspend(() => {
        reads += 1;
        assert.deepEqual(input.repository, repository);
        assert.equal(input.pullRequest.number, observation.number);
        return readError ? Effect.fail(readError) : Effect.succeed(observation);
      }),
    refreshQueueBase: (input) =>
      Effect.gen(function* () {
        fetched.push(input);
        yield* fetchGate;
        if (fetchError) return yield* fetchError;
        return { commitSha: input.previousHandoff ? mergedSha : initialSha, targetBranch: "main" };
      }),
    prepare: () => Effect.die("Queue must not prepare publication"),
    publish: () => Effect.die("Queue must not publish pull requests"),
  });
  const preview = (number: number) =>
    Effect.sync(
      () =>
        previews.get(number) ?? {
          projectId: id,
          source: source(number),
          canStart: true,
          blockers: [],
        },
    );
  const make = () =>
    makeEpicQueue.pipe(
      Effect.provideService(AgentControlEngine, engine),
      Effect.provideService(EpicHandoffRemote, remote),
    );
  let commandSequence = 0;
  const change = Effect.fn("queueFixtureChange")(function* (
    action: AgentControlEpicQueueChangeInput["action"],
    command = `${id}-command-${++commandSequence}`,
  ) {
    const service = yield* make();
    const current = yield* loadEpicQueue(sql, id);
    return yield* service.change(
      {
        projectId: id,
        commandId: CommandId.make(command),
        expectedRevision: current?.revision ?? 0,
        action,
      },
      preview,
    );
  });
  const approve = (number: number) =>
    change({
      kind: "approve",
      epicNumber: number,
      expectedFingerprint: source(number).fingerprint,
    });
  const succeed = Effect.fn("queueFixtureSucceed")(function* (
    publish = false,
    publishedHandoff = handoff,
  ) {
    const selected = (yield* loadSelectedEpic(sql, id))!;
    return yield* sql.withTransaction(
      saveEpicRun(sql, selected, {
        status: "succeeded",
        acceptedCommitSha: resultSha,
        finalVerification: {
          status: "passed",
          commitSha: resultSha,
          evidenceId: "shared-check-a",
          detail: "Passed",
          checks: [],
        },
        ...(publish ? { handoff: publishedHandoff } : {}),
      }),
    );
  });
  return {
    sql,
    id,
    make,
    preview,
    previews,
    approve,
    change,
    succeed,
    fetched,
    process: Effect.fn("queueFixtureProcess")(function* () {
      return yield* (yield* make()).process(id, preview);
    }),
    read: () => loadEpicQueue(sql, id),
    runs: () => sql`SELECT epic_run_id FROM agent_control_epic_runs WHERE project_id=${id}`,
    selected: () => loadSelectedEpic(sql, id),
    reads: () => reads,
    setMode: (mode: AgentControlProjectState["mode"]) => {
      project = { ...project, mode, revision: project.revision + 1 };
    },
    loseAuthority: () => {
      authorityLost = true;
    },
    setObservation: (state: AgentControlEpicHandoffPullRequest["state"], number = 301) => {
      observation = {
        ...pullRequest,
        number,
        url: `https://github.com/owner/repo/pull/${number}`,
        state,
        mergeCommitSha: state === "merged" ? mergedSha : null,
      };
    },
    setReadError: (error?: EpicHandoffRemoteError) => {
      readError = error;
    },
    setFetchError: (error?: EpicHandoffRemoteError) => {
      fetchError = error;
    },
    setFetchGate: (gate: Effect.Effect<void>) => {
      fetchGate = gate;
    },
  };
});

describe("durable Epic queue", () => {
  it.effect(
    "rejects adoption of a stopped manual Epic without changing its selection or creating a queue",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const manual = yield* createEpicRun({
          projectId,
          commandId: "stopped-manual",
          source: source(10),
          checks: [],
        });
        yield* f.sql.withTransaction(insertEpicRun(f.sql, manual));
        yield* f.sql.withTransaction(saveEpicRun(f.sql, manual, { status: "stopped" }));
        const error = yield* f.approve(20).pipe(Effect.flip);
        assertEpicError(error, "epic-stopped");
        assert.isNull(yield* f.read());
        assert.equal((yield* f.selected())?.epicRunId, manual.epicRunId);
        assert.equal((yield* f.selected())?.status, "stopped");
        assert.equal((yield* f.runs()).length, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("reschedules unchanged human review without growing queue or run history", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.approve(10);
      f.setMode("armed");
      yield* f.process();
      yield* f.succeed(true);
      yield* TestClock.adjust("60 seconds");
      yield* f.process();
      const waiting = (yield* f.read())!;
      const run = (yield* f.selected())!;
      const queueHistory =
        yield* f.sql`SELECT revision FROM agent_control_epic_queue_history WHERE project_id=${projectId}`;
      const runHistory =
        yield* f.sql`SELECT revision FROM agent_control_epic_history WHERE epic_run_id=${run.epicRunId}`;
      for (let observation = 0; observation < 3; observation += 1) {
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
      }
      const refreshed = (yield* f.read())!;
      assert.equal(refreshed.revision, waiting.revision);
      assert.notEqual(refreshed.nextCheckAt, waiting.nextCheckAt);
      assert.deepEqual({ ...refreshed, nextCheckAt: null }, { ...waiting, nextCheckAt: null });
      assert.deepEqual(
        yield* f.sql`SELECT revision FROM agent_control_epic_queue_history WHERE project_id=${projectId}`,
        queueHistory,
      );
      assert.deepEqual(
        yield* f.sql`SELECT revision FROM agent_control_epic_history WHERE epic_run_id=${run.epicRunId}`,
        runHistory,
      );
      assert.equal((yield* f.selected())?.revision, run.revision);
      assert.equal(f.reads(), 4);
      assert.equal(f.fetched.length, 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("leaves an empty queue unscheduled without starting ordinary work", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const approved = yield* f.approve(10);
      yield* f.change({ kind: "remove", entryId: approved.entries[0]!.entryId });
      f.setMode("armed");
      assert.isTrue(yield* f.process());
      const empty = (yield* f.read())!;
      assert.deepEqual(empty.entries, []);
      assert.isNull(empty.nextCheckAt);
      assert.isNull(empty.nextEntryId);
      yield* TestClock.adjust("10 minutes");
      assert.isTrue(yield* f.process());
      assert.deepEqual(yield* f.read(), empty);
      assert.equal((yield* f.runs()).length, 0);
      assert.equal(f.fetched.length, 0);
      assert.equal(f.reads(), 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "starts A once, retains explicit publication and human merge gates, and starts B on the refreshed target",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        f.setMode("armed");
        yield* f.process();
        const a = (yield* f.selected())!;
        assert.equal(a.source.epic.number, 10);
        assert.deepEqual(a.initialBase, { commitSha: initialSha, targetBranch: "main" });
        yield* f.process();
        assert.equal((yield* f.runs()).length, 1);
        yield* f.succeed();
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.include((yield* f.read())!.waitReason!, "Explicitly publish");
        assert.equal(f.reads(), 0);
        assert.equal((yield* f.runs()).length, 1);
        yield* f.succeed(true);
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.include((yield* f.read())!.waitReason!, "human review and merge");
        assert.equal((yield* f.runs()).length, 1);
        // A rebuilt service uses only persisted queue/run state while GitHub remains open.
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.runs()).length, 1);
        f.setObservation("merged");
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        const b = (yield* f.selected())!;
        assert.equal(b.source.epic.number, 20);
        assert.deepEqual(b.initialBase, { commitSha: mergedSha, targetBranch: "main" });
        assert.equal(f.fetched[1]?.previousHandoff?.pullRequest?.state, "merged");
        assert.equal(
          (yield* loadEpicRun(f.sql, a.epicRunId))?.handoff?.pullRequest?.state,
          "merged",
        );
        assert.deepEqual(
          (yield* f.read())!.entries.map((entry) => entry.status),
          ["merged", "active"],
        );
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.runs()).length, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "adopts an existing selected Epic without replacing it and survives close/reopen before merge",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const existing = yield* createEpicRun({
          projectId,
          commandId: "manual-start",
          source: source(10),
          checks: [],
        });
        yield* f.sql.withTransaction(insertEpicRun(f.sql, existing));
        f.setMode("armed");
        const approved = yield* f.approve(20);
        assert.equal(approved.entries[0]?.epicRunId, existing.epicRunId);
        assert.equal(approved.entries[0]?.status, "active");
        assert.equal((yield* f.selected())?.epicRunId, existing.epicRunId);
        yield* f.succeed(true);
        f.setObservation("closed");
        yield* f.process();
        assert.include((yield* f.read())!.waitReason!, "closed without merge");
        assert.equal((yield* f.selected())?.handoff?.status, "blocked");
        assert.equal(f.fetched.length, 0);
        f.setObservation("open");
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.include((yield* f.read())!.waitReason!, "human review and merge");
        assert.equal((yield* f.selected())?.handoff?.status, "published");
        f.setObservation("merged");
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.selected())?.source.epic.number, 20);
        assert.equal((yield* f.runs()).length, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "persists merge observation across a failed fetch and restart without starting on the old base",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        f.setMode("armed");
        yield* f.process();
        yield* f.succeed(true);
        f.setReadError(unavailable("github-temporarily-unavailable"));
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.read())!.entries[0]?.status, "active");
        assert.equal((yield* f.runs()).length, 1);
        const reads = f.reads();
        yield* f.process();
        assert.equal(f.reads(), reads);
        f.setReadError();
        f.setObservation("merged");
        f.setFetchError(unavailable("fetch-failed"));
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.read())!.entries[0]?.status, "merged");
        assert.equal((yield* f.runs()).length, 1);
        assert.include((yield* f.read())!.waitReason!, "fetch-failed");
        yield* f.process();
        assert.equal(f.fetched.length, 2);
        f.setFetchError();
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.runs()).length, 2);
        assert.equal((yield* f.selected())?.initialBase?.commitSha, mergedSha);
        assert.equal(f.reads(), reads + 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "keeps explicit approval order, skips blocked prerequisites, and only reorders or removes unstarted entries",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        let q = yield* f.approve(30);
        q = yield* f.change({
          kind: "reorder",
          entryIds: [q.entries[1]!.entryId, q.entries[0]!.entryId, q.entries[2]!.entryId],
        });
        assert.deepEqual(
          q.entries.map((entry) => entry.source.epic.number),
          [20, 10, 30],
        );
        q = yield* f.change({ kind: "remove", entryId: q.entries[2]!.entryId });
        const blocker = {
          code: "missing-prerequisite",
          issueNumber: 20,
          message: "Epic #20 waits for prerequisite #9.",
        };
        f.previews.set(20, { projectId, source: source(20), canStart: false, blockers: [blocker] });
        f.setMode("armed");
        yield* f.process();
        assert.equal((yield* f.selected())?.source.epic.number, 10);
        q = (yield* f.read())!;
        assert.deepEqual(q.entries[0]?.blockers, [blocker]);
        const active = q.entries.find((entry) => entry.status === "active")!;
        const rejected = yield* f
          .change({ kind: "remove", entryId: active.entryId })
          .pipe(Effect.flip);
        assertEpicError(rejected, "entry-started");
        assert.deepEqual(
          (yield* f.read())!.entries.map((entry) => entry.source.epic.number),
          [20, 10],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "replays commands and rejects stale competing client edits without duplicating approval",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const service = yield* f.make();
        const input = {
          projectId,
          commandId: CommandId.make("approve-idempotent"),
          expectedRevision: 0,
          action: {
            kind: "approve" as const,
            epicNumber: 10,
            expectedFingerprint: source(10).fingerprint,
          },
        };
        const first = yield* service.change(input, f.preview);
        assert.deepEqual(yield* service.change(input, f.preview), first);
        const conflict = yield* service
          .change({ ...input, action: { ...input.action, epicNumber: 20 } }, f.preview)
          .pipe(Effect.flip);
        assertEpicError(conflict, "command-conflict");
        const results = yield* Effect.all(
          [20, 30].map((number) =>
            service
              .change(
                {
                  projectId,
                  commandId: CommandId.make(`approve-${number}`),
                  expectedRevision: first.revision,
                  action: {
                    kind: "approve",
                    epicNumber: number,
                    expectedFingerprint: source(number).fingerprint,
                  },
                },
                f.preview,
              )
              .pipe(Effect.exit),
          ),
          { concurrency: 2 },
        );
        assert.equal(results.filter(Exit.isSuccess).length, 1);
        assert.equal((yield* f.read())!.entries.length, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "uses the last executed merge after an earlier blocked Epic becomes ready, including restart",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        yield* f.approve(30);
        f.previews.set(10, {
          projectId,
          source: source(10),
          canStart: false,
          blockers: [
            { code: "missing-prerequisite", issueNumber: 10, message: "Wait for prerequisite." },
          ],
        });
        f.setMode("armed");
        yield* f.process();
        assert.equal((yield* f.selected())?.source.epic.number, 20);
        yield* f.succeed(true);
        f.setObservation("merged");
        f.previews.delete(10);
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.selected())?.source.epic.number, 10);
        const aHandoff = {
          ...handoff,
          intentId: "a-was-executed-after-b",
          branchName: "t3auto/epic-after-b",
          pullRequest: {
            ...pullRequest,
            number: 302,
            url: "https://github.com/owner/repo/pull/302",
          },
        };
        yield* f.succeed(true, aHandoff);
        f.setObservation("merged", 302);
        f.setFetchError(unavailable("fetch-failed"));
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.runs()).length, 2);
        assert.deepEqual(
          (yield* f.read())!.entries.map((entry) => entry.status),
          ["merged", "merged", "pending"],
        );
        f.setFetchError();
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        assert.equal((yield* f.selected())?.source.epic.number, 30);
        assert.equal(f.fetched.at(-1)?.previousHandoff?.intentId, "a-was-executed-after-b");
        assert.equal(f.fetched.at(-1)?.previousHandoff?.pullRequest?.number, 302);
        assert.equal((yield* f.runs()).length, 3);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("disarm during fetch prevents activation and re-arm resumes from saved approval", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.approve(10);
      f.setMode("armed");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      f.setFetchGate(
        Effect.gen(function* () {
          yield* Deferred.succeed(entered, undefined);
          yield* Deferred.await(release);
        }),
      );
      const processing = yield* f.process().pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      f.setMode("observe");
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(processing);
      assert.equal((yield* f.runs()).length, 0);
      assert.equal((yield* f.read())!.entries[0]?.status, "pending");
      yield* f.process();
      assert.equal(f.fetched.length, 1);
      f.setMode("armed");
      yield* f.process();
      assert.equal((yield* f.runs()).length, 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("propagates authority loss instead of converting it into a project blocker", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.approve(10);
      f.setMode("armed");
      f.loseAuthority();
      const error = yield* f.process().pipe(Effect.flip);
      assert(isPersistenceError(error));
      assert.equal(error.code, "internal-persistence-error");
      assert.equal((yield* f.runs()).length, 0);
      assert.equal(f.fetched.length, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "propagates corrupt persisted authority and leaves unrelated Armed projects selectable",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const other = yield* fixture(ProjectId.make("other-queue-project"));
        yield* f.approve(10);
        yield* other.approve(20);
        f.setMode("armed");
        other.setMode("armed");
        f.setFetchError(unavailable("fetch-failed"));
        yield* f.process();
        yield* other.process();
        assert.equal((yield* f.runs()).length, 0);
        assert.equal((yield* other.runs()).length, 1);
        // Deliberately simulate on-disk damage past SQLite's mutation guard.
        yield* f.sql`DROP TRIGGER agent_control_epic_queue_revision_guard`;
        yield* f.sql`UPDATE agent_control_epic_queues SET state_digest='corrupt' WHERE project_id=${projectId}`;
        const error = yield* f.process().pipe(Effect.flip);
        assertEpicError(error, "authority-conflict");
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});
