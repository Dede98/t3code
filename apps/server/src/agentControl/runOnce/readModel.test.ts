import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlRunOnceId,
  ProjectId,
  type AgentControlRunOnceSnapshot,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type * as NodeSqlite from "node:sqlite";
import * as NodeSqliteClient from "../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../persistence/Migrations.ts";

let fixtureDatabase: NodeSqlite.DatabaseSync;
const persistence = Layer.effectDiscard(runMigrations()).pipe(
  Layer.provideMerge(
    NodeSqliteClient.layerMemory({
      _testHooks: {
        registerFunctions: (database) => {
          NodeSqliteClient.registerNodeSqliteFunctions(database);
          fixtureDatabase = database;
        },
      },
    }),
  ),
);
import { AgentControlRuntimeLayerLive } from "../runtimeLayer.ts";
import { AgentControlRunOnceController } from "./Services/AgentControlRunOnceController.ts";
import { persistRunOnceDiagnostic } from "./diagnostics.ts";
import { deriveRunOnceCommandId } from "./identity.ts";
import { AgentControlRunOnceError } from "./model.ts";
import { makeAgentControlRunOnceReadModel } from "./readModel.ts";
import {
  AgentControlRunOnceReadNotificationsLive,
  AgentControlRunOnceReadNotifications,
} from "./readNotifications.ts";

const encodeUnknownJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const unused = () => Effect.die("A read subscription must never start work");
const controller = Layer.succeed(AgentControlRunOnceController, {
  recover: unused(),
  processProject: unused,
  prepare: unused,
  subscribePublicationWakeups: Effect.succeed(Stream.never),
  recoverPublicationConsumer: unused,
  pullPublications: unused,
  acknowledgePublication: unused,
  subscribePublications: Effect.succeed(Stream.never),
});
const layer = it.layer(
  Layer.mergeAll(
    AgentControlRuntimeLayerLive,
    controller,
    AgentControlRunOnceReadNotificationsLive,
  ).pipe(Layer.provideMerge(persistence), Layer.provideMerge(NodeServices.layer)),
);
const at = "2026-09-01T00:00:00.000Z";
const projectId = ProjectId.make("read-run-project");
const taskId = "read-task";
const runId = AgentControlRunOnceId.make(`run-once-${"a".repeat(64)}`);

// These are persisted read-side fixtures, not execution-authority fixtures.
// Use the migrated schema's actual columns while bypassing write-authority
// triggers. The production reader must still validate every returned payload.
let fixtureOrdinal = 0;
const insertFixture = Effect.fn("insertFixture")(function* (
  table: string,
  input: Record<string, string | number | null>,
) {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql.unsafe<{ name: string; type: string; notnull: number }>(
    `PRAGMA table_info(${table})`,
  ).unprepared;
  fixtureOrdinal += 1;
  const row: Record<string, string | number | null> = {};
  for (const column of columns) {
    if (column.notnull)
      row[column.name] = /INT|REAL/.test(column.type)
        ? fixtureOrdinal
        : `${table}-${column.name}-${fixtureOrdinal}`;
  }
  Object.assign(row, input);
  const names = Object.keys(row);
  yield* Effect.sync(() =>
    fixtureDatabase
      .prepare(
        `INSERT INTO ${table} (${names.map((name) => `"${name}"`).join(",")}) VALUES (${names.map(() => "?").join(",")})`,
      )
      .run(...names.map((name) => row[name]!)),
  );
});

const allowReadFixtures = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const triggers = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_schema WHERE type = 'trigger'`;
  for (const trigger of triggers) yield* sql.unsafe(`DROP TRIGGER "${trigger.name}"`).unprepared;
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql`PRAGMA ignore_check_constraints = ON`;
});

const seed = Effect.gen(function* () {
  yield* allowReadFixtures;
  yield* insertFixture("projection_projects", {
    project_id: projectId,
    title: "Read project",
    workspace_root: "/isolated/project",
    default_model_selection_json: null,
    scripts_json: "[]",
    created_at: at,
    updated_at: at,
    deleted_at: null,
  });
  const task = {
    schemaVersion: 1,
    taskId,
    source: {
      projectId,
      repositoryNodeId: "repo",
      issueNodeId: "issue",
      issueNumber: 1,
      issueUrl: "https://example.invalid/1",
    },
    status: "succeeded",
    sourceGate: "eligible",
    stage: "verification",
    sourceUpdatedAt: at,
    githubIntakeSequence: 7,
    sourceSnapshot: {
      repositoryNodeId: "repo",
      issueNodeId: "issue",
      number: 1,
      url: "https://example.invalid/1",
      state: "open",
      title: "Small task",
      body: "Must never appear in the snapshot",
      contentTrust: "untrusted-external",
      updatedAt: at,
      timelineComplete: true,
      ready: true,
      paused: false,
      eligible: true,
      eligibilityReason: "eligible",
    },
    createdAt: at,
    updatedAt: at,
    revision: 1,
    sequence: 1,
  };
  yield* insertFixture("agent_control_task_states", {
    task_id: taskId,
    project_id: projectId,
    repository_node_id: "repo",
    issue_node_id: "issue",
    issue_number: 1,
    issue_url: task.source.issueUrl,
    status: task.status,
    source_gate: "eligible",
    stage: task.stage,
    source_updated_at: at,
    github_intake_sequence: 7,
    state_json: encodeUnknownJson(task),
    created_at: at,
    updated_at: at,
    revision: 1,
    last_event_sequence: 1,
  });
  yield* insertFixture("agent_control_run_once_states", {
    run_id: runId,
    project_id: projectId,
    status: "completed",
    next_ordinal: 10,
    last_step: "completed",
    task_id: taskId,
    stage_run_id: "stage-1",
    lease_id: null,
    worktree_reservation_id: null,
    controlled_thread_reservation_id: null,
    terminal_task_event_id: "terminal",
    activation_project_revision: 2,
    reset_project_revision: 3,
    updated_at: at,
  });
  for (const ordinal of [1, 2, 3, 4, 5]) {
    const stageKind =
      ordinal === 1 ? "planning" : ordinal % 2 === 0 ? "implementation" : "verification";
    const stage = {
      schemaVersion: 1,
      projectId,
      taskId,
      stageRunId: `stage-${ordinal}`,
      attemptId: `attempt-${ordinal}`,
      roleId: stageKind,
      stageKind,
      stageOrdinal: ordinal,
      attemptOrdinal: 1,
      status: ordinal === 3 ? "failed" : "succeeded",
      taskRevision: 1,
      githubIntakeSequence: 7,
      sourceIdentityFingerprint: "fingerprint",
      createdAt: at,
      updatedAt: at,
      revision: 1,
      sequence: ordinal,
    };
    yield* insertFixture("agent_control_stage_run_states", {
      stage_run_id: stage.stageRunId,
      project_id: projectId,
      task_id: taskId,
      stage_ordinal: ordinal,
      attempt_ordinal: 1,
      state_json: encodeUnknownJson(stage),
    });
    const prefix = ordinal === 1 ? "initial_planning" : stageKind;
    yield* insertFixture(`agent_control_${prefix}_handoff_intents`, {
      handoff_id: `handoff-${ordinal}`,
      stage_run_id: stage.stageRunId,
      task_id: taskId,
      project_id: projectId,
      thread_id: `thread-${ordinal}`,
      worktree_path: "/isolated/worktree",
      provider_delivery_id: `delivery-${ordinal}`,
      worktree_reservation_id: "worktree",
      provider_instance_id: "codex-production",
      model_selection_json: encodeUnknownJson({ instanceId: "codex-production", model: "gpt-5" }),
    });
    if (stageKind !== "verification") continue;
    yield* insertFixture("agent_control_verification_check_manifests", {
      provider_delivery_id: `delivery-${ordinal}`,
      handoff_id: `handoff-${ordinal}`,
      checks_json: encodeUnknownJson([
        {
          id: "test",
          command: "node",
          args: ["--test"],
          cwd: ".",
          required: true,
          timeoutMs: 1000,
          allowTemporaryFiles: false,
          resultFormat: "exit-code",
        },
      ]),
      manifest_digest: `manifest-${ordinal}`,
    });
    yield* insertFixture("agent_control_verification_check_starts", {
      provider_delivery_id: `delivery-${ordinal}`,
      check_id: "test",
      provider_turn_id: `turn-${ordinal}`,
      manifest_digest: `manifest-${ordinal}`,
    });
    yield* insertFixture("agent_control_verification_check_results", {
      provider_delivery_id: `delivery-${ordinal}`,
      check_id: "test",
      provider_turn_id: `turn-${ordinal}`,
      manifest_digest: `manifest-${ordinal}`,
      status: ordinal === 3 ? "failed" : "passed",
      result_json: encodeUnknownJson({
        exitCode: ordinal === 3 ? 1 : 0,
        stdout: ordinal === 3 ? "first failure" : "final success",
        stderr: "",
      }),
      completed_at: at,
    });
    yield* insertFixture("agent_control_verification_evaluation_evidence", {
      stage_run_id: stage.stageRunId,
      evidence_id: `evidence-${ordinal}`,
      marker_id: `marker-${ordinal}`,
      evaluation_fingerprint: `evaluation-${ordinal}`,
      project_id: projectId,
      task_id: taskId,
      provider_delivery_id: `delivery-${ordinal}`,
      verdict: ordinal === 3 ? "failed" : "passed",
      error_code: null,
      evaluated_at: at,
    });
    yield* insertFixture("agent_control_verification_evaluation_markers", {
      marker_id: `marker-${ordinal}`,
      evidence_id: `evidence-${ordinal}`,
      evaluation_fingerprint: `evaluation-${ordinal}`,
    });
  }
  yield* insertFixture("agent_control_run_once_repairs", {
    run_id: runId,
    repair_stage_run_id: "stage-4",
  });
});

layer("Run-Once client read model", (it) => {
  it.effect(
    "reconstructs repair and separate verification evidence; streams durable updates and reconnects without execution",
    () =>
      Effect.gen(function* () {
        yield* seed;
        const sql = yield* SqlClient.SqlClient;
        const read = yield* makeAgentControlRunOnceReadModel;
        const snapshot = yield* read.getSnapshot({ projectId });
        assert.deepStrictEqual(
          snapshot.runs[0]?.stages.map((stage) => stage.displayStage),
          ["planning", "implementation", "verification", "repair", "verification"],
        );
        assert.equal(
          snapshot.runs[0]?.stages[2]?.verification?.checks[0]?.output?.trim(),
          "first failure",
        );
        assert.equal(
          snapshot.runs[0]?.stages[4]?.verification?.checks[0]?.output?.trim(),
          "final success",
        );
        assert.equal(snapshot.runs[0]?.stages[4]?.verification?.verdict, "passed");
        assert.equal(snapshot.runs[0]?.stages[4]?.providerInstanceId, "codex-production");
        assert.equal(snapshot.runs[0]?.stages[4]?.model, "gpt-5");
        assert.deepStrictEqual(snapshot.armed, { enabled: false });
        assert.isFalse(encodeUnknownJson(snapshot).includes("Must never appear"));
        assert.deepStrictEqual(
          (yield* read.getSnapshot({ projectId, runId: AgentControlRunOnceId.make("another-run") }))
            .runs,
          [],
        );

        const initial = yield* Deferred.make<void>();
        const changed = yield* Deferred.make<AgentControlRunOnceSnapshot>();
        const stream = yield* read.subscribe({ projectId });
        const fiber = yield* stream.pipe(
          Stream.runForEach((value) =>
            value.blockers.length
              ? Deferred.succeed(changed, value)
              : Deferred.succeed(initial, undefined),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(initial);
        yield* persistRunOnceDiagnostic(
          sql,
          projectId,
          new AgentControlRunOnceError({
            projectId,
            runId,
            step: "stage-prepared",
            reason: "downstream-rejected",
            cause: { code: "provider-unavailable" },
          }),
        );
        const received = yield* Deferred.await(changed);
        assert.equal(received.runs[0]?.errorCode, "downstream-rejected: provider-unavailable");
        yield* Fiber.interrupt(fiber);
        const reconnect = yield* read.subscribe({ projectId });
        const reconnected = yield* Stream.runHead(reconnect);
        assert.deepStrictEqual(Option.getOrThrow(reconnected), received);

        const checkInitial = yield* Deferred.make<void>();
        const checkChanged = yield* Deferred.make<AgentControlRunOnceSnapshot>();
        const checkStream = yield* read.subscribe({ projectId });
        const checkFiber = yield* checkStream.pipe(
          Stream.runForEach((value) =>
            value.runs[0]?.stages[4]?.verification?.checks[0]?.status === "missing"
              ? Deferred.succeed(checkChanged, value)
              : Deferred.succeed(checkInitial, undefined),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(checkInitial);
        yield* sql`DELETE FROM agent_control_verification_check_results WHERE provider_delivery_id = 'delivery-5'`;
        const notifications = yield* AgentControlRunOnceReadNotifications;
        yield* notifications.publish("handoff-5");
        const missing = yield* Deferred.await(checkChanged);
        yield* Fiber.interrupt(checkFiber);
        assert.equal(missing.runs[0]?.stages[4]?.verification?.checks[0]?.status, "missing");
        yield* sql`DELETE FROM agent_control_verification_check_manifests WHERE provider_delivery_id = 'delivery-5'`;
        const unavailable = yield* read.getSnapshot({ projectId });
        yield* persistRunOnceDiagnostic(sql, projectId, null);
        assert.deepStrictEqual((yield* read.getSnapshot({ projectId })).blockers, []);
        assert.equal(
          unavailable.runs[0]?.stages[4]?.verification?.errorCode,
          "verification-checks-missing",
        );

        // A human-ended run keeps the immutable rejection visible after its
        // current diagnostic is cleared, without blocking the next activation.
        yield* sql`UPDATE agent_control_run_once_states SET terminal_task_event_id = NULL
          WHERE run_id = ${runId}`;
        yield* insertFixture("agent_control_run_once_step_evidence", {
          run_id: runId,
          project_id: projectId,
          ordinal: 4,
          step: "lease-reserved",
        });
        yield* insertFixture("agent_control_worktree_controller_operations", {
          command_id: deriveRunOnceCommandId(runId, 5, "worktree-ready"),
          command_type: "reserve-and-materialize",
          project_id: projectId,
          task_id: taskId,
          status: "rejected",
          rejection_code: "default-remote-ref-unavailable",
        });
        const ended = yield* read.getSnapshot({ projectId, runId });
        assert.equal(ended.runs[0]?.state.status, "completed");
        assert.equal(
          ended.runs[0]?.errorCode,
          "downstream-rejected: default-remote-ref-unavailable",
        );
        assert.deepStrictEqual(ended.blockers, []);
        const nextRunId = AgentControlRunOnceId.make(`run-once-${"b".repeat(64)}`);
        yield* sql`UPDATE agent_control_run_once_states SET run_id = ${nextRunId}
          WHERE run_id = ${runId}`;
        assert.isNull((yield* read.getSnapshot({ projectId })).runs[0]?.errorCode);
        // The latest run no longer identifies the old blocked task. Its durable
        // execution history must still prevent advertising a fresh activation.
        yield* sql`UPDATE agent_control_task_states
          SET status = 'candidate', stage = 'intake', source_gate = 'eligible',
            state_json = json_set(state_json, '$.status', 'candidate', '$.stage', 'intake')
          WHERE task_id = ${taskId}`;
        assert.isNull((yield* read.getSnapshot({ projectId })).nextTaskId);
      }).pipe(Effect.scoped),
  );
  it.effect("reads Armed authority before run publication, across resume and after takeover", () =>
    Effect.gen(function* () {
      yield* allowReadFixtures;
      const sql = yield* SqlClient.SqlClient;
      const read = yield* makeAgentControlRunOnceReadModel;
      const id = ProjectId.make("armed-authority-read");
      yield* insertFixture("projection_projects", {
        project_id: id,
        title: "Armed authority",
        workspace_root: "/isolated/armed-authority",
        default_model_selection_json: null,
        scripts_json: "[]",
        created_at: at,
        updated_at: at,
        deleted_at: null,
      });
      yield* insertFixture("agent_control_project_states", {
        project_id: id,
        mode: "armed",
        paused_from_mode: null,
        revision: 1,
        last_event_sequence: 100,
        updated_at: at,
      });
      assert.deepStrictEqual((yield* read.getSnapshot({ projectId: id })).armed, { enabled: true });
      yield* insertFixture("agent_control_events", {
        event_id: "armed-origin-event",
        command_id: "armed-origin-command",
        aggregate_kind: "project-controller",
        stream_id: id,
        stream_version: 2,
        sequence: 101,
        actor_authority: "system",
        event_type: "agentControl.project.mode.changed",
        payload_json: encodeUnknownJson({ previousMode: "armed", mode: "run-once" }),
      });
      yield* insertFixture("agent_control_command_receipts", {
        command_id: "armed-origin-command",
        status: "accepted",
        authority: "system",
        aggregate_kind: "project-controller",
        aggregate_id: id,
        event_created: 1,
        result_sequence: 101,
        result_stream_version: 2,
      });
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'run-once', revision = 2, last_event_sequence = 101 WHERE project_id = ${id}`;
      const activating = yield* read.getSnapshot({ projectId: id });
      assert.deepStrictEqual(activating.armed, { enabled: true });
      assert.deepStrictEqual(activating.runs, []);
      const activeRunId = AgentControlRunOnceId.make("armed-read-active-run");
      yield* insertFixture("agent_control_run_once_states", {
        run_id: activeRunId,
        project_id: id,
        status: "active",
        next_ordinal: 2,
        last_step: "activation-admitted",
        task_id: null,
        stage_run_id: null,
        lease_id: null,
        worktree_reservation_id: null,
        controlled_thread_reservation_id: null,
        terminal_task_event_id: null,
        activation_project_revision: 2,
        reset_project_revision: null,
        updated_at: at,
      });
      yield* insertFixture("agent_control_run_once_activations", {
        run_id: activeRunId,
        project_id: id,
        origin_mode: "armed",
      });
      const running = yield* read.getSnapshot({ projectId: id });
      assert.equal(running.projectState.mode, "run-once");
      assert.equal(running.runs[0]?.originMode, "armed");
      assert.equal(running.runs[0]?.state.status, "active");
      assert.deepStrictEqual(running.armed, { enabled: true });
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'paused', paused_from_mode = 'run-once', revision = 3 WHERE project_id = ${id}`;
      assert.deepStrictEqual((yield* read.getSnapshot({ projectId: id })).armed, {
        enabled: false,
      });
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'run-once', paused_from_mode = NULL, revision = 4 WHERE project_id = ${id}`;
      assert.deepStrictEqual((yield* read.getSnapshot({ projectId: id })).armed, { enabled: true });
      yield* persistRunOnceDiagnostic(
        sql,
        id,
        new AgentControlRunOnceError({
          projectId: id,
          runId: null,
          step: null,
          reason: "downstream-rejected",
          cause: { code: "default-remote-ref-unavailable" },
        }),
      );
      const blocked = yield* read.getSnapshot({ projectId: id });
      assert.deepStrictEqual(blocked.armed, { enabled: true });
      assert.equal(blocked.blockers.length, 1);
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'observe', revision = 5 WHERE project_id = ${id}`;
      const stopped = yield* read.getSnapshot({ projectId: id });
      assert.deepStrictEqual(stopped.armed, { enabled: false });
      assert.equal(stopped.runs[0]?.originMode, "armed");
      assert.equal(stopped.runs[0]?.state.status, "active");
      const reconnected = yield* Stream.runHead(yield* read.subscribe({ projectId: id }));
      assert.deepStrictEqual(Option.getOrThrow(reconnected).armed, { enabled: false });
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'armed', revision = 6 WHERE project_id = ${id}`;
      assert.deepStrictEqual((yield* read.getSnapshot({ projectId: id })).armed, { enabled: true });
      // A later manual Run Once must not inherit an earlier Armed activation.
      yield* insertFixture("agent_control_events", {
        event_id: "observe-origin-event",
        command_id: "observe-origin-command",
        aggregate_kind: "project-controller",
        stream_id: id,
        stream_version: 7,
        sequence: 102,
        actor_authority: "human",
        event_type: "agentControl.project.mode.changed",
        payload_json: encodeUnknownJson({ previousMode: "observe", mode: "run-once" }),
      });
      yield* insertFixture("agent_control_command_receipts", {
        command_id: "observe-origin-command",
        status: "accepted",
        authority: "human",
        aggregate_kind: "project-controller",
        aggregate_id: id,
        event_created: 1,
        result_sequence: 102,
        result_stream_version: 7,
      });
      yield* sql`UPDATE agent_control_project_states
        SET mode = 'run-once', revision = 7, last_event_sequence = 102 WHERE project_id = ${id}`;
      assert.deepStrictEqual((yield* read.getSnapshot({ projectId: id })).armed, {
        enabled: false,
      });
    }).pipe(Effect.scoped),
  );
});
