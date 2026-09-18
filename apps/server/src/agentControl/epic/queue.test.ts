import {
  AgentControlEpicRpcError,
  AgentControlInternalPersistenceError,
  CommandId,
  ProjectId,
  type AgentControlEpicHandoff,
  type AgentControlEpicHandoffPullRequest,
  type AgentControlEpicPreview,
  type AgentControlEpicProjectDependencyPlan,
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
import { epicIssueContentFingerprint, epicSourceFingerprint } from "../github/githubEpicSource.ts";
import {
  epicJson,
  loadEpicRun,
  loadProjectEpics,
  loadSelectedEpic,
  saveEpicRun,
} from "./authority.ts";
import { makeEpicQueue } from "./queue.ts";
import { loadEpicQueue, loadEnabledEpicQueue } from "./queueAuthority.ts";
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

const fixture = Effect.fn("epicQueueFixture")(function* (id = projectId, migration = 86) {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations({ toMigrationInclusive: migration });
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
  const previewCalls: number[] = [];
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
    Effect.sync(() => {
      previewCalls.push(number);
      return (
        previews.get(number) ?? {
          projectId: id,
          source: source(number),
          canStart: true,
          blockers: [],
        }
      );
    });
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
    previewCalls,
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
  for (const [changedField, parallelism] of [
    ["title", 1],
    ["body", 2],
  ] as const) {
    it.effect(
      `keeps a changed ${changedField} local to its reviewed queue entry across recovery and requires fresh approval`,
      () =>
        Effect.gen(function* () {
          const f = yield* fixture();
          const inspectedSource = (changed: boolean): AgentControlEpicSource => {
            const original = source(10);
            const tasks = original.tasks.map((task) => {
              const content = {
                title: changed && changedField === "title" ? "Changed task" : task.issue.title,
                body: changed && changedField === "body" ? "Changed scope" : "Approved scope",
              };
              return {
                ...task,
                issue: {
                  ...task.issue,
                  title: content.title,
                  contentFingerprint: epicIssueContentFingerprint(content),
                },
              };
            });
            return { ...original, tasks, fingerprint: epicSourceFingerprint(original.epic, tasks) };
          };
          const approveSource = (current: AgentControlEpicSource) =>
            f.change({
              kind: "approve",
              epicNumber: current.epic.number,
              expectedFingerprint: current.fingerprint,
              parallelism,
              dependencyPlan: {
                version: 1,
                sourceFingerprint: current.fingerprint,
                rationale: "Reviewed isolated document task.",
                tasks: current.tasks.map((task) => ({
                  issueNodeId: task.issue.issueNodeId,
                  dependsOn: [],
                })),
              },
            });
          const approvedSource = inspectedSource(false);
          f.previews.set(10, { projectId, source: approvedSource, canStart: true, blockers: [] });
          const approved = (yield* approveSource(approvedSource)).entries[0]!;
          const currentSource = inspectedSource(true);
          f.previews.set(10, { projectId, source: currentSource, canStart: true, blockers: [] });
          f.setMode("armed");
          yield* f.process();
          const blocked = (yield* f.read())!.entries[0]!;
          assert.equal(blocked.status, "pending");
          assert.equal(blocked.blockers[0]?.code, "scope-changed");
          assert.include(blocked.blockers[0]!.message, "approve its current scope");
          assert.deepEqual(blocked.source, approved.source);
          assert.deepEqual(blocked.dependencyPlan, approved.dependencyPlan);
          assert.isNull(yield* f.selected());
          assert.lengthOf(yield* f.runs(), 0);
          assert.lengthOf(f.fetched, 0);

          // A fresh service reads the durable blocker without failing recovery.
          yield* TestClock.adjust("5 minutes");
          yield* f.process();
          assert.deepEqual((yield* f.read())!.entries[0], blocked);
          yield* f.approve(20);
          yield* f.process();
          assert.equal((yield* f.selected())?.source.epic.number, 20);
          assert.equal((yield* f.read())!.entries[0]?.blockers[0]?.code, "scope-changed");
          assert.lengthOf(yield* f.runs(), 1);

          yield* f.change({ kind: "remove", entryId: approved.entryId });
          yield* approveSource(currentSource);
          yield* f.succeed(true);
          f.setObservation("merged");
          yield* f.process();
          const selected = (yield* f.selected())!;
          assert.equal(selected.source.epic.number, 10);
          assert.equal(selected.dependencyPlan?.sourceFingerprint, currentSource.fingerprint);
          assert.equal(selected.parallelism, parallelism);
          assert.lengthOf(yield* f.runs(), 2);
        }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
    );
  }

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

  it.effect(
    "leaves a failed queue explicitly after disarm and pending removal, preserving history and command authority",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        f.setMode("armed");
        yield* f.process();
        const selected = (yield* f.selected())!;
        const failed = yield* f.sql.withTransaction(
          saveEpicRun(f.sql, selected, {
            status: "blocked",
            blockers: [{ code: "child-failed", issueNumber: 110, message: "Child failed." }],
          }),
        );
        assertEpicError(yield* f.change({ kind: "leave" }).pipe(Effect.flip), "queue-busy");
        f.setMode("observe");
        assertEpicError(yield* f.change({ kind: "leave" }).pipe(Effect.flip), "queue-has-pending");
        const pending = (yield* f.read())!.entries.find((entry) => entry.status === "pending")!;
        const before = yield* f.change({ kind: "remove", entryId: pending.entryId });
        const input = {
          projectId,
          commandId: CommandId.make("leave-failed"),
          expectedRevision: before.revision,
          action: { kind: "leave" as const },
        };
        const service = yield* f.make();
        const left = yield* service.change(input, f.preview);
        assert.isFalse(left.enabled);
        assert.isNull(yield* f.selected());
        assert.isNull(yield* loadEnabledEpicQueue(f.sql, projectId));
        const retained = (yield* loadEpicRun(f.sql, selected.epicRunId))!;
        assert.equal(retained.status, "stopped");
        assert.deepEqual(retained.blockers, failed.blockers);
        assert.deepEqual(yield* service.change(input, f.preview), left);
        assert.equal((yield* f.runs()).length, 1);
        assert.isFalse(yield* f.process());
        assertEpicError(
          yield* service
            .change({ ...input, commandId: CommandId.make("stale-leave") }, f.preview)
            .pipe(Effect.flip),
          "revision-conflict",
        );
        const fresh = yield* f.approve(20);
        assert.isTrue(fresh.enabled);
        assert.isAbove(fresh.revision, left.revision);
        assert.equal(fresh.entries.length, 1);
        assert.equal(fresh.entries[0]?.source.epic.number, 20);
        f.setMode("armed");
        yield* f.process();
        assert.equal((yield* f.runs()).length, 2);
        assert.equal((yield* f.selected())?.source.epic.number, 20);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("does not hide a lost active selection when leaving", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.approve(10);
      f.setMode("armed");
      yield* f.process();
      f.setMode("observe");
      const before = yield* f.read();
      yield* f.sql`DELETE FROM agent_control_epic_targets WHERE project_id=${projectId}`;
      assertEpicError(yield* f.change({ kind: "leave" }).pipe(Effect.flip), "authority-conflict");
      assert.deepEqual(yield* f.read(), before);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect("bounds dependency scans and rechecks immediately after an explicit queue edit", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      yield* f.approve(10);
      f.previews.set(10, {
        projectId,
        source: source(10),
        canStart: false,
        blockers: [
          { code: "missing-prerequisite", issueNumber: 10, message: "Wait for prerequisite." },
        ],
      });
      f.previewCalls.length = 0;
      f.setMode("armed");
      yield* f.process();
      assert.deepEqual(f.previewCalls, [10]);
      yield* TestClock.adjust("60 seconds");
      yield* f.process();
      assert.deepEqual(f.previewCalls, [10]);
      yield* TestClock.adjust("4 minutes");
      yield* f.process();
      assert.deepEqual(f.previewCalls, [10, 10]);
      yield* f.approve(20);
      yield* f.process();
      assert.equal((yield* f.selected())?.source.epic.number, 20);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "leaves after a confirmed merge while preserving the completed run and PR mapping",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        f.setMode("armed");
        yield* f.process();
        yield* f.succeed(true);
        f.setObservation("merged");
        yield* TestClock.adjust("60 seconds");
        yield* f.process();
        const completed = (yield* f.selected())!;
        assert.equal((yield* f.read())!.entries[0]?.status, "merged");
        f.setMode("observe");
        yield* f.change({ kind: "leave" });
        assert.isNull(yield* f.selected());
        assert.deepEqual(yield* loadEpicRun(f.sql, completed.epicRunId), completed);
        assert.isFalse(yield* f.process());
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );

  it.effect(
    "skips an approved Epic whose children all closed and stops inspection at the first eligible successor",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        yield* f.approve(30);
        f.previews.set(10, {
          projectId,
          source: {
            ...source(10),
            tasks: source(10).tasks.map((task) => ({
              ...task,
              issue: { ...task.issue, state: "closed" },
            })),
          },
          canStart: true,
          blockers: [],
        });
        f.previewCalls.length = 0;
        f.setMode("armed");
        yield* f.process();
        const queue = (yield* f.read())!;
        assert.equal(queue.entries[0]?.status, "pending");
        assert.equal(queue.entries[0]?.blockers[0]?.code, "no-open-tasks");
        assert.equal((yield* f.selected())?.source.epic.number, 20);
        assert.deepEqual(f.previewCalls, [10, 20]);
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
        const previewsAfterStart = f.previewCalls.length;
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
        assert.equal(f.previewCalls.length, previewsAfterStart);
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

const approvePlanned = Effect.fn("approvePlannedQueueEpic")(function* (
  f: Effect.Success<ReturnType<typeof fixture>>,
  current: AgentControlEpicSource,
) {
  f.previews.set(current.epic.number, {
    projectId: f.id,
    source: current,
    canStart: current.blockers.length === 0,
    blockers: current.blockers,
  });
  return yield* f.change({
    kind: "approve",
    epicNumber: current.epic.number,
    expectedFingerprint: current.fingerprint,
    parallelism: 2,
    dependencyPlan: {
      version: 1,
      sourceFingerprint: current.fingerprint,
      rationale: "Reviewed independent document changes.",
      tasks: current.tasks.map((task) => ({
        issueNodeId: task.issue.issueNodeId,
        dependsOn: task.dependencies.map((dependency) => dependency.issueNodeId),
      })),
    },
  });
});
const configureParallel = Effect.fn("configureParallelQueue")(function* (
  f: Effect.Success<ReturnType<typeof fixture>>,
  limit = 2,
) {
  const queue = (yield* f.read())!;
  const projectDependencyPlan: AgentControlEpicProjectDependencyPlan = {
    version: 1,
    rationale:
      "Reviewed all Epic scopes; independent tasks use separate files and dependent tasks require human merge.",
    epics: queue.entries.map((entry) => ({
      issueNodeId: entry.source.epic.issueNodeId,
      sourceFingerprint: entry.source.fingerprint,
    })),
    tasks: queue.entries.flatMap((entry) => entry.dependencyPlan!.tasks),
  };
  return yield* f.change({ kind: "configure", maxActiveEpics: limit, projectDependencyPlan });
});

describe("parallel Epic queue persistence and admission", () => {
  it.effect(
    "requires opt-in, admits two distinct durable targets and preserves identities over service recovery and unchanged polls",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(projectId, 93);
        yield* approvePlanned(f, source(10));
        yield* approvePlanned(f, source(20));
        assert.equal((yield* f.read())!.maxActiveEpics ?? 1, 1);
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(f.change({ kind: "configure", maxActiveEpics: 2 }))),
        );
        yield* configureParallel(f);
        f.setMode("armed");
        yield* f.process();
        const initial = yield* loadProjectEpics(f.sql, f.id);
        assert.lengthOf(initial, 2);
        assert.equal(new Set(initial.map((run) => run.epicRunId)).size, 2);
        assert.equal(new Set(initial.map((run) => run.members[0]!.issueNodeId)).size, 2);
        assert.include(
          initial.map((run) => run.epicRunId),
          (yield* f.selected())!.epicRunId,
        );
        assert.lengthOf(
          (yield* f.read())!.entries.filter((entry) => entry.status === "active"),
          2,
        );
        for (const run of initial) {
          assert.equal(run.initialBase?.commitSha, initialSha);
          assert.isDefined(run.projectDependencyPlanDigest);
        }
        const ids = initial.map((run) => run.epicRunId).sort();
        for (let iteration = 0; iteration < 3; iteration += 1) {
          // A fresh service instance is built on every fixture process invocation.
          yield* TestClock.adjust("1 minute");
          yield* f.process();
          assert.deepEqual(
            (yield* loadProjectEpics(f.sql, f.id)).map((run) => run.epicRunId).sort(),
            ids,
          );
        }
        assert.lengthOf(yield* f.runs(), 2);
        assert.lengthOf(f.fetched, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "releases review and stopped execution slots independently without discarding their authority",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(projectId, 93);
        for (const number of [10, 20, 30, 40]) yield* approvePlanned(f, source(number));
        yield* configureParallel(f);
        f.setMode("armed");
        yield* f.process();
        const initial = yield* loadProjectEpics(f.sql, f.id);
        const reviewing = initial.find((run) => run.source.epic.number === 10)!;
        const stopped = initial.find((run) => run.source.epic.number === 20)!;
        yield* f.sql.withTransaction(
          saveEpicRun(f.sql, reviewing, {
            status: "succeeded",
            acceptedCommitSha: resultSha,
            finalVerification: {
              status: "passed",
              commitSha: resultSha,
              evidenceId: "passed",
              detail: "Passed",
              checks: [],
            },
          }),
        );
        yield* f.sql.withTransaction(saveEpicRun(f.sql, stopped, { status: "stopped" }));
        yield* TestClock.adjust("1 minute");
        yield* f.process();
        const recovered = yield* loadProjectEpics(f.sql, f.id);
        assert.lengthOf(recovered, 4);
        assert.deepEqual(
          recovered
            .filter((run) => run.status === "running")
            .map((run) => run.source.epic.number)
            .sort(),
          [30, 40],
        );
        assert.equal(
          recovered.find((run) => run.epicRunId === stopped.epicRunId)?.status,
          "stopped",
        );
        const queue = (yield* f.read())!;
        assert.equal(
          queue.entries.find((entry) => entry.epicRunId === reviewing.epicRunId)?.blockers[0]?.code,
          "review-handoff",
        );
        assert.equal(
          queue.entries.find((entry) => entry.epicRunId === stopped.epicRunId)?.status,
          "stopped",
        );
        assert.lengthOf(f.fetched, 4);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect("disarm prevents all new autonomous Epic admissions", () =>
    Effect.gen(function* () {
      const f = yield* fixture(projectId, 93);
      yield* approvePlanned(f, source(10));
      yield* approvePlanned(f, source(20));
      yield* configureParallel(f);
      yield* f.process();
      assert.lengthOf(yield* loadProjectEpics(f.sql, f.id), 0);
      assert.lengthOf(f.fetched, 0);
      f.setMode("armed");
      yield* f.process();
      assert.lengthOf(yield* loadProjectEpics(f.sql, f.id), 2);
      f.setMode("observe");
      const before = yield* f.read();
      yield* TestClock.adjust("1 minute");
      yield* f.process();
      assert.deepEqual(yield* f.read(), before);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "admits independent work alongside covered native task prerequisites and isolates changed scopes",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture(projectId, 93);
        const dependent: AgentControlEpicSource = {
          ...source(20),
          tasks: source(20).tasks.map((task) => ({ ...task, dependencies: [issue(110)] })),
          blockers: [
            {
              code: "missing-prerequisite",
              issueNumber: 110,
              message: "Waiting for the prerequisite.",
            },
          ],
        };
        yield* approvePlanned(f, source(10));
        yield* approvePlanned(f, dependent);
        yield* approvePlanned(f, source(30));
        yield* configureParallel(f, 3);
        f.previews.set(30, {
          projectId: f.id,
          source: { ...source(30), epic: { ...source(30).epic, title: "Changed intent" } },
          canStart: true,
          blockers: [],
        });
        f.setMode("armed");
        yield* f.process();
        assert.deepEqual(
          (yield* loadProjectEpics(f.sql, f.id)).map((run) => run.source.epic.number).sort(),
          [10, 20],
        );
        assert.equal(
          (yield* f.read())!.entries.find((entry) => entry.source.epic.number === 30)?.blockers[0]
            ?.code,
          "scope-changed",
        );
        yield* TestClock.adjust("1 minute");
        yield* f.process();
        assert.lengthOf(yield* f.runs(), 2);
        assert.lengthOf(f.fetched, 2);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
  it.effect(
    "upgrades a serial queue and active single-Epic run without changing their authority or replaying starts",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        yield* f.approve(10);
        yield* f.approve(20);
        f.setMode("armed");
        yield* f.process();
        const before = (yield* f.selected())!;
        const queue = (yield* f.read())!;
        yield* runMigrations({ toMigrationInclusive: 93 });
        assert.deepEqual(yield* f.selected(), before);
        assert.deepEqual(yield* f.read(), queue);
        assert.deepEqual(
          (yield* loadProjectEpics(f.sql, f.id)).map((run) => run.epicRunId),
          [before.epicRunId],
        );
        yield* TestClock.adjust("1 minute");
        yield* f.process();
        assert.equal((yield* f.selected())!.epicRunId, before.epicRunId);
        assert.equal((yield* f.read())!.maxActiveEpics ?? 1, 1);
        assert.lengthOf(yield* f.runs(), 1);
        assert.lengthOf(f.fetched, 1);
      }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
  );
});
