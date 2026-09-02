import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlRuntimeLayerLive } from "../runtimeLayer.ts";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { AgentControlProjection } from "../Services/AgentControlProjection.ts";
import { AgentControlCommandReceiptRepository } from "../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlEventStore } from "../../persistence/Services/AgentControlEventStore.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";

const layer = it.layer(
  AgentControlRuntimeLayerLive.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

const addProject = (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  deletedAt: string | null = null,
) =>
  sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, default_model_selection_json,
      scripts_json, created_at, updated_at, deleted_at
    ) VALUES (
      ${projectId}, 'Agent Control Test', '/tmp/agent-control-test', NULL, '[]',
      '2026-07-22T10:00:00.000Z', '2026-07-22T10:00:00.000Z', ${deletedAt}
    )
  `;

const setMode = (
  commandId: string,
  projectId: ProjectId,
  expectedRevision: number,
  mode: "manual" | "observe" | "run-once" | "armed" | "paused",
) => ({ commandId: CommandId.make(commandId), projectId, expectedRevision, mode });

layer("AgentControlEngine", (it) => {
  it.effect("reads Manual by default and persists Observe, Pause, Resume and no-op receipts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const events = yield* AgentControlEventStore;
      const receipts = yield* AgentControlCommandReceiptRepository;
      const projectId = ProjectId.make("project-engine-modes");
      yield* addProject(sql, projectId);

      assert.deepStrictEqual(yield* engine.getProjectState({ projectId }), {
        schemaVersion: 1,
        projectId,
        mode: "manual",
        pausedFromMode: null,
        revision: 0,
        sequence: 0,
        updatedAt: null,
      });

      const observe = yield* engine.dispatchHuman(setMode("mode-observe", projectId, 0, "observe"));
      assert.equal(observe.eventCreated, true);
      assert.equal(observe.state.revision, 1);
      const pause = yield* engine.dispatchHuman(setMode("mode-pause", projectId, 1, "paused"));
      assert.equal(pause.state.pausedFromMode, "observe");
      const resume = yield* engine.dispatchHuman(setMode("mode-resume", projectId, 2, "observe"));
      assert.equal(resume.state.pausedFromMode, null);

      const noop = yield* engine.dispatchHuman(setMode("mode-noop", projectId, 3, "observe"));
      assert.equal(noop.eventCreated, false);
      assert.equal(noop.state.revision, 3);
      assert.equal((yield* events.readStream(projectId, 0, 10)).length, 3);
      const noopReceipt = yield* receipts.getByCommandId(CommandId.make("mode-noop"));
      assert.equal(noopReceipt._tag, "Some");
      if (noopReceipt._tag === "Some") {
        assert.equal(noopReceipt.value.status, "accepted");
        assert.equal(noopReceipt.value.eventCreated, false);
      }
    }),
  );

  it.effect("replays accepted receipts and fails closed on payload or authority reuse", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const projectId = ProjectId.make("project-engine-idempotency");
      yield* addProject(sql, projectId);
      const original = setMode("idempotent-command", projectId, 0, "observe");
      const accepted = yield* engine.dispatchHuman(original);
      yield* engine.dispatchHuman(setMode("later-pause", projectId, 1, "paused"));

      const replayed = yield* engine.dispatchHuman(original);
      assert.deepStrictEqual(replayed, accepted);
      assert.equal(replayed.state.mode, "observe");

      const changedPayload = yield* Effect.result(
        engine.dispatchHuman({ ...original, mode: "manual" }),
      );
      assert.equal(changedPayload._tag, "Failure");
      if (changedPayload._tag === "Failure") {
        assert.equal(changedPayload.failure.code, "command-identity-mismatch");
      }

      const changedAuthority = yield* Effect.result(engine.dispatchSystem(original));
      assert.equal(changedAuthority._tag, "Failure");
      if (changedAuthority._tag === "Failure") {
        assert.equal(changedAuthority.failure.code, "command-identity-mismatch");
      }
    }),
  );

  it.effect(
    "commits human Armed and Run Once transitions while generic system reset fails closed",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;
        const projectId = ProjectId.make("project-engine-run-once");
        yield* addProject(sql, projectId);

        yield* engine.dispatchHuman(setMode("run-once-observe", projectId, 0, "observe"));
        const armed = yield* engine.dispatchHuman(setMode("run-once-armed", projectId, 1, "armed"));
        assert.equal(armed.state.mode, "armed");
        const armedPaused = yield* engine.dispatchHuman(
          setMode("run-once-armed-pause", projectId, 2, "paused"),
        );
        assert.equal(armedPaused.state.pausedFromMode, "armed");
        const armedResumed = yield* engine.dispatchHuman(
          setMode("run-once-armed-resume", projectId, 3, "armed"),
        );
        assert.equal(armedResumed.state.mode, "armed");
        yield* engine.dispatchHuman(setMode("run-once-back-observe", projectId, 4, "observe"));
        const activation = yield* engine.dispatchHuman(
          setMode("run-once-activate", projectId, 5, "run-once"),
        );
        assert.equal(activation.state.mode, "run-once");
        const paused = yield* engine.dispatchHuman(
          setMode("run-once-pause", projectId, 6, "paused"),
        );
        assert.equal(paused.state.pausedFromMode, "run-once");
        const resumed = yield* engine.dispatchHuman(
          setMode("run-once-resume", projectId, 7, "run-once"),
        );
        assert.equal(resumed.state.mode, "run-once");
        assert.equal(resumed.state.pausedFromMode, null);

        const reset = yield* Effect.result(
          engine.dispatchSystem(setMode("run-once-system-reset", projectId, 8, "observe")),
        );
        assert.equal(reset._tag, "Failure");
        assert.equal((yield* engine.getProjectState({ projectId })).mode, "run-once");

        const takeover = yield* engine.dispatchHuman(
          setMode("run-once-human-takeover", projectId, 8, "observe"),
        );
        assert.equal(takeover.state.mode, "observe");
      }),
  );

  it.effect("persists rejected commands and requires a new id for a corrected request", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const projectId = ProjectId.make("project-engine-rejection");
      yield* addProject(sql, projectId);
      const unavailable = setMode("unavailable-command", projectId, 0, "paused");

      const first = yield* Effect.result(engine.dispatchHuman(unavailable));
      assert.equal(first._tag, "Failure");
      if (first._tag === "Failure") assert.equal(first.failure.code, "transition-not-allowed");
      const retry = yield* Effect.result(engine.dispatchHuman(unavailable));
      assert.equal(retry._tag, "Failure");
      if (retry._tag === "Failure") {
        assert.equal(retry.failure.code, "command-previously-rejected");
        if (retry.failure.code === "command-previously-rejected") {
          assert.equal(retry.failure.originalErrorCode, "transition-not-allowed");
        }
      }
      const corrected = yield* engine.dispatchHuman(
        setMode("corrected-command", projectId, 0, "observe"),
      );
      assert.equal(corrected.state.mode, "observe");
    }),
  );

  it.effect(
    "serializes parallel commands and rolls back event, projection and receipt together",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;
        const projectId = ProjectId.make("project-engine-concurrency");
        yield* addProject(sql, projectId);
        const results = yield* Effect.all(
          [
            Effect.result(engine.dispatchHuman(setMode("parallel-a", projectId, 0, "observe"))),
            Effect.result(engine.dispatchHuman(setMode("parallel-b", projectId, 0, "observe"))),
          ],
          { concurrency: "unbounded" },
        );
        assert.equal(results.filter((result) => result._tag === "Success").length, 1);
        const failure = results.find((result) => result._tag === "Failure");
        assert.equal(failure?._tag, "Failure");
        if (failure?._tag === "Failure") assert.equal(failure.failure.code, "revision-conflict");

        const rollbackProject = ProjectId.make("project-engine-rollback");
        yield* addProject(sql, rollbackProject);
        const cursorBeforeCorruption = yield* sql<{ readonly sequence: number }>`
        SELECT last_applied_sequence AS sequence
        FROM agent_control_projection_state
        WHERE projector_name = 'agent-control-project-modes-v1'
      `;
        yield* sql`
        UPDATE agent_control_projection_state
        SET last_applied_sequence = 99
        WHERE projector_name = 'agent-control-project-modes-v1'
      `;
        const rollback = yield* Effect.result(
          engine.dispatchHuman(setMode("rollback-command", rollbackProject, 0, "observe")),
        );
        assert.equal(rollback._tag, "Failure");
        const eventRows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_events
        WHERE stream_id = ${rollbackProject}
      `;
        const receiptRows = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
        WHERE command_id = 'rollback-command'
      `;
        assert.equal(eventRows[0]?.count, 0);
        assert.equal(receiptRows[0]?.count, 0);
        yield* sql`
        UPDATE agent_control_projection_state
        SET last_applied_sequence = ${cursorBeforeCorruption[0]!.sequence}
        WHERE projector_name = 'agent-control-project-modes-v1'
      `;
      }),
  );

  it.effect("checks missing and deleted canonical projects", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const engine = yield* AgentControlEngine;
      const missing = ProjectId.make("project-engine-missing");
      const missingRead = yield* Effect.result(engine.getProjectState({ projectId: missing }));
      assert.equal(missingRead._tag, "Failure");
      if (missingRead._tag === "Failure") assert.equal(missingRead.failure.code, "project-missing");

      const deleted = ProjectId.make("project-engine-deleted");
      yield* addProject(sql, deleted, "2026-07-22T11:00:00.000Z");
      const deletedWrite = yield* Effect.result(
        engine.dispatchHuman(setMode("deleted-command", deleted, 0, "observe")),
      );
      assert.equal(deletedWrite._tag, "Failure");
      if (deletedWrite._tag === "Failure")
        assert.equal(deletedWrite.failure.code, "project-deleted");
    }),
  );

  it.effect(
    "publishes committed events and deterministically catches up or rebuilds projections",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const engine = yield* AgentControlEngine;
        const projection = yield* AgentControlProjection;
        const projectId = ProjectId.make("project-engine-recovery");
        yield* addProject(sql, projectId);
        yield* sql`
        INSERT INTO agent_control_project_policies (
          project_id, policy_json, revision, updated_at
        ) VALUES (${projectId}, '{"fullAccess":true}', 1, '2026-07-22T10:00:00.000Z')
      `;

        const eventFiber = yield* Stream.runHead(engine.streamDomainEvents).pipe(Effect.forkChild);
        yield* Effect.yieldNow;
        const accepted = yield* engine.dispatchHuman(
          setMode("recovery-observe", projectId, 0, "observe"),
        );
        const published = yield* Fiber.join(eventFiber);
        assert.equal(published._tag, "Some");
        if (published._tag === "Some")
          assert.equal(published.value.sequence, accepted.resultSequence);

        yield* sql`DELETE FROM agent_control_project_states`;
        yield* sql`DELETE FROM agent_control_projection_state`;
        yield* projection.bootstrap;
        assert.equal((yield* engine.getProjectState({ projectId })).mode, "observe");

        yield* projection.rebuild;
        yield* projection.rebuild;
        assert.equal((yield* engine.getProjectState({ projectId })).revision, 1);
        const receipts = yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM agent_control_command_receipts
        WHERE command_id = 'recovery-observe'
      `;
        const policies = yield* sql<{ readonly revision: number }>`
        SELECT revision FROM agent_control_project_policies WHERE project_id = ${projectId}
      `;
        assert.equal(receipts[0]?.count, 1);
        assert.deepStrictEqual(policies, [{ revision: 1 }]);

        yield* sql`
        UPDATE agent_control_events SET payload_json = '{broken'
        WHERE command_id = 'recovery-observe'
      `;
        const corruptRebuild = yield* Effect.result(projection.rebuild);
        assert.equal(corruptRebuild._tag, "Failure");
        if (corruptRebuild._tag === "Failure") {
          assert.equal(corruptRebuild.failure._tag, "AgentControlPersistenceDecodeError");
        }
      }),
  );
});
