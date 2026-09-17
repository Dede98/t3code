import { persistEpicTransitionDiagnostic } from "./diagnostics.ts";
import {
  AgentControlTaskId,
  ProjectId,
  type AgentControlEpicRuntimeView,
  type AgentControlStageRunCommandResult,
  type AgentControlControlledThreadReservationCommandResult,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { AgentControlControlledThreadActivation } from "../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import { epicDigest, epicError, loadSelectedEpic, saveEpicRun } from "../epic/authority.ts";
import { insertEpicRun } from "../epic/runState.ts";
import { AgentControlStageRun } from "../stageRun/Services/AgentControlStageRun.ts";
import { AgentControlStageRunLeaseEngine } from "../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlWorktreeController } from "../worktree/Services/AgentControlWorktreeController.ts";
import { makeEpicTaskExecution } from "./epicExecution.ts";

const projectId = ProjectId.make("parallel-execution");
const at = "2026-09-17T08:00:00.000Z";
const taskId = (n: number) => AgentControlTaskId.make(`task-${n}`);
const initial = (): AgentControlEpicRuntimeView => {
  const repository = { repositoryNodeId: "repo", nameWithOwner: "owner/repo" };
  const issue = (n: number) => ({
    ...repository,
    issueNodeId: `issue-${n}`,
    number: n,
    title: `Task ${n}`,
    url: `https://github.com/owner/repo/issues/${n}`,
    state: "open" as const,
    subIssueCount: 0,
  });
  const dependencyPlan = {
    version: 1 as const,
    sourceFingerprint: "source",
    rationale: "A and B own separate modules; C consumes both.",
    tasks: [
      { issueNodeId: "issue-1", dependsOn: [] },
      { issueNodeId: "issue-2", dependsOn: [] },
      { issueNodeId: "issue-3", dependsOn: ["issue-1", "issue-2"] },
    ],
  };
  return {
    epicRunId: "epic-execution-test",
    projectId,
    revision: 1,
    status: "running",
    parallelism: 2,
    dependencyPlan,
    dependencyPlanDigest: epicDigest(dependencyPlan),
    source: {
      format: "github-native-sub-issues-v1",
      repository,
      epic: issue(10),
      tasks: [1, 2, 3].map((n) => ({ issue: issue(n), position: n, dependencies: [] })),
      blockers: [],
      fingerprint: "source",
      inspectedAt: at,
    },
    checks: [],
    members: [1, 2, 3].map((n) => ({
      issueNodeId: `issue-${n}`,
      issueNumber: n,
      taskId: taskId(n),
      childRunId: null,
      status: n < 3 ? "running" : "pending",
      baseCommitSha: "a".repeat(40),
      reservationId: null,
      taskFinalizationEvidenceId: null,
      accepted: null,
    })),
    initialBase: { commitSha: "a".repeat(40), targetBranch: "main" },
    activeTaskId: taskId(1),
    acceptedCommitSha: null,
    blockers: [],
    blockerHistory: [],
    verificationAttempt: 1,
    finalVerification: null,
    finalVerificationHistory: [],
    createdAt: at,
    updatedAt: at,
  };
};

const fixture = Effect.fn("epicExecutionFixture")(function* () {
  yield* runMigrations({ toMigrationInclusive: 89 });
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO agent_control_project_states(project_id,mode,paused_from_mode,revision,last_event_sequence,updated_at)
    VALUES (${projectId},'armed',NULL,1,1,${at})`;
  yield* insertEpicRun(sql, initial());
  const active = new Set<string>();
  const calls = new Map<string, number>();
  const receipts = new Map<string, unknown>();
  let failWorktree: string | undefined;
  let failCode = "test-interruption";
  const count = (key: string) => calls.set(key, (calls.get(key) ?? 0) + 1);
  const stages = {
    prepareInitial: (input: { taskId: string; commandId: string }) =>
      Effect.sync(() => {
        let result = receipts.get(input.commandId) as AgentControlStageRunCommandResult | undefined;
        if (!result) {
          count(`stage:${input.taskId}`);
          result = {
            state: {
              stageRunId: `stage-${input.taskId}`,
              attemptId: `attempt-${input.taskId}`,
              taskRevision: 1,
              githubIntakeSequence: 1,
              sourceIdentityFingerprint: "source",
            },
          } as AgentControlStageRunCommandResult;
          receipts.set(input.commandId, result);
        }
        return result;
      }),
    getStageRun: () => Effect.die("unused"),
    listStageRuns: () => Effect.die("unused"),
  } satisfies AgentControlStageRun["Service"];
  const leases = {
    dispatchController: () => Effect.succeed({ _tag: "Accepted", result: {}, events: [] }),
  } as unknown as AgentControlStageRunLeaseEngine["Service"];
  const worktrees = {
    reserveAndMaterialize: (input: { taskId: string }) =>
      Effect.gen(function* () {
        if (failWorktree === input.taskId)
          return yield* Effect.fail(epicError(failCode, "materialization interrupted"));
        return {
          reservationId: `worktree-${input.taskId}`,
        } as AgentControlWorktreeReservationState;
      }),
  } as unknown as AgentControlWorktreeController["Service"];
  const threads = {
    activateInitial: (input: { taskId: string; commandId: string }) =>
      Effect.sync(() => {
        let result = receipts.get(input.commandId) as
          | AgentControlControlledThreadReservationCommandResult
          | undefined;
        if (!result) {
          count(`thread:${input.taskId}`);
          active.add(input.taskId);
          result = {
            reservation: {
              controlledThreadReservationId: `reservation-${input.taskId}`,
              threadId: `thread-${input.taskId}`,
            },
          } as AgentControlControlledThreadReservationCommandResult;
          receipts.set(input.commandId, result);
        }
        return result;
      }),
  } satisfies AgentControlControlledThreadActivation["Service"];
  const build = makeEpicTaskExecution().pipe(
    Effect.provideService(AgentControlStageRun, stages),
    Effect.provideService(AgentControlStageRunLeaseEngine, leases),
    Effect.provideService(AgentControlWorktreeController, worktrees),
    Effect.provideService(AgentControlControlledThreadActivation, threads),
  );
  return {
    sql,
    build,
    active,
    calls,
    fail: (id?: string, code = "test-interruption") => {
      failWorktree = id;
      failCode = code;
    },
  };
});

it.effect(
  "starts both authorized members, retains receipt identity on duplicate ticks and restart, and never starts dependencies",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const run = yield* f.build;
      yield* Effect.all([run(projectId), run(projectId)], { concurrency: 2 });
      assert.deepEqual([...f.active], [taskId(1), taskId(2)]);
      assert.equal(f.calls.get(`thread:${taskId(1)}`), 1);
      assert.equal(f.calls.get(`thread:${taskId(2)}`), 1);
      yield* (yield* f.build)(projectId);
      assert.equal(f.calls.get(`stage:${taskId(1)}`), 1);
      const bindings = yield* f.sql`SELECT * FROM agent_control_epic_task_executions`;
      assert.equal(bindings.length, 2);
      assert.isNull((yield* loadSelectedEpic(f.sql, projectId))!.members[2]!.childRunId);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "a failed start leaves another member runnable and restart resumes its original reservation",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.fail(taskId(1));
      yield* (yield* f.build)(projectId);
      assert.deepEqual([...f.active], [taskId(2)]);
      const before = (yield* loadSelectedEpic(f.sql, projectId))!.members[0]!.childRunId;
      f.fail();
      yield* (yield* f.build)(projectId);
      assert.equal((yield* loadSelectedEpic(f.sql, projectId))!.members[0]!.childRunId, before);
      assert.equal(f.calls.get(`stage:${taskId(1)}`), 1);
      assert.equal(f.calls.get(`thread:${taskId(1)}`), 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("pause and disarm forbid new starts and changed plan authority cannot be adopted", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* f.sql`UPDATE agent_control_project_states SET mode='paused',paused_from_mode='armed' WHERE project_id=${projectId}`;
    yield* (yield* f.build)(projectId);
    assert.equal(f.active.size, 0);
    yield* f.sql`UPDATE agent_control_project_states SET mode='armed',paused_from_mode=NULL WHERE project_id=${projectId}`;
    f.fail(taskId(1));
    yield* (yield* f.build)(projectId);
    const state = (yield* loadSelectedEpic(f.sql, projectId))!;
    assert.equal(
      (yield* Effect.result(
        f.sql.withTransaction(saveEpicRun(f.sql, state, { dependencyPlanDigest: "b".repeat(64) })),
      ))._tag,
      "Failure",
    );
    yield* f.sql`UPDATE agent_control_project_states SET mode='manual',paused_from_mode=NULL WHERE project_id=${projectId}`;
    f.fail();
    yield* (yield* f.build)(projectId);
    assert.isFalse(f.active.has(taskId(1)));
    const mutation = yield* Effect.result(
      f.sql`UPDATE agent_control_epic_task_executions SET plan_digest=${"b".repeat(64)}`,
    );
    assert.equal(mutation._tag, "Failure");
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect("persists separate transition blockers without replacing execution ownership", () =>
  Effect.gen(function* () {
    const f = yield* fixture();
    yield* (yield* f.build)(projectId);
    const before = (yield* loadSelectedEpic(f.sql, projectId))!;
    const input = (n: number) => ({
      projectId,
      taskId: taskId(n),
      worktreeReservationId: `worktree-${taskId(n)}`,
      transitionId: `implementation:handoff-${n}`,
    });
    const failure = { operation: "worktree-use", reason: "stage-run-missing" };
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), {
      operation: "admit",
      reason: "revision-conflict",
    });
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), before);
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), {
      operation: "worktree-use",
      reason: "worktree-evidence-stale",
      cause: { code: "task-not-consumable", cause: { reason: "watermark-not-completed" } },
    });
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), before);
    yield* persistEpicTransitionDiagnostic(
      f.sql,
      {
        ...input(1),
        worktreeReservationId: "foreign-worktree",
      },
      failure,
    );
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), before);
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), failure);
    yield* persistEpicTransitionDiagnostic(f.sql, input(2), failure);
    const blocked = (yield* loadSelectedEpic(f.sql, projectId))!;
    assert.equal(blocked.status, "blocked");
    assert.equal(blocked.blockers.length, 2);
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), {
      operation: "current-task",
      reason: "task-evidence-stale",
      cause: { reason: "task-status-inactive" },
    });
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), blocked);
    assert.include(blocked.members[0]!.blocker!, "stage-run-missing");
    assert.include(blocked.members[0]!.blocker!, "resume");
    assert.deepEqual(
      blocked.members.map((member) => member.childRunId),
      before.members.map((member) => member.childRunId),
    );
    assert.deepEqual(
      blocked.members.map((member) => member.reservationId),
      before.members.map((member) => member.reservationId),
    );
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), failure);
    yield* (yield* f.build)(projectId);
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), blocked);
    assert.equal(f.calls.get(`thread:${taskId(1)}`), 1);
    assert.equal(f.calls.get(`thread:${taskId(2)}`), 1);
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), null);
    const recovered = (yield* loadSelectedEpic(f.sql, projectId))!;
    assert.equal(recovered.status, "blocked");
    assert.isUndefined(recovered.members[0]!.blocker);
    assert.equal(recovered.blockers.length, 1);
    assert.equal(recovered.blockers[0]!.issueNumber, 2);
    assert.isNotNull(recovered.members[1]!.blocker);
    assert.equal(recovered.blockerHistory.length, 2);
    yield* f.sql.withTransaction(
      saveEpicRun(f.sql, recovered, { status: "running", blockers: [] }),
    );
    yield* persistEpicTransitionDiagnostic(f.sql, input(2), null);
    const resumed = (yield* loadSelectedEpic(f.sql, projectId))!;
    assert.equal(resumed.status, "running");
    assert.isUndefined(resumed.members[1]!.blocker);
    assert.isUndefined(resumed.members[1]!.waitReason);
    yield* f.sql`UPDATE agent_control_project_states SET mode='observe' WHERE project_id=${projectId}`;
    yield* persistEpicTransitionDiagnostic(f.sql, input(1), failure);
    assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), resumed);
  }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "retains a permanent preparation blocker across restart without reassigning its execution",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      f.fail(taskId(1), "reservation-conflict");
      yield* (yield* f.build)(projectId);
      const blocked = (yield* loadSelectedEpic(f.sql, projectId))!;
      assert.equal(blocked.status, "blocked");
      assert.equal(blocked.blockers[0]?.code, "preparation:reservation-conflict");
      assert.include(blocked.members[0]!.blocker!, "resume");
      const bindings = yield* f.sql`SELECT * FROM agent_control_epic_task_executions`;
      assert.equal(bindings.length, 1);
      assert.equal(f.active.size, 0);
      yield* (yield* f.build)(projectId);
      assert.deepEqual(yield* loadSelectedEpic(f.sql, projectId), blocked);
      assert.deepEqual(yield* f.sql`SELECT * FROM agent_control_epic_task_executions`, bindings);
      assert.equal(f.calls.get(`stage:${taskId(1)}`), 1);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
