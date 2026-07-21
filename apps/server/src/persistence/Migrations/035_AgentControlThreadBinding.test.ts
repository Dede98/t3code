import {
  AgentControlAttemptId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlTaskId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ProjectionThreadRepositoryLive } from "../Layers/ProjectionThreads.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { ProjectionThreadRepository } from "../Services/ProjectionThreads.ts";

const layer = it.layer(
  ProjectionThreadRepositoryLive.pipe(Layer.provideMerge(NodeSqliteClient.layerMemory())),
);

layer("035_AgentControlThreadBinding", (it) => {
  it.effect("upgrades existing thread projections and persists Agent Control bindings", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threads = yield* ProjectionThreadRepository;

      yield* runMigrations({ toMigrationInclusive: 34 });
      yield* sql`
        INSERT INTO projection_threads (
          thread_id,
          project_id,
          title,
          model_selection_json,
          runtime_mode,
          interaction_mode,
          branch,
          worktree_path,
          latest_turn_id,
          created_at,
          updated_at,
          archived_at,
          latest_user_message_at,
          pending_approval_count,
          pending_user_input_count,
          has_actionable_proposed_plan,
          deleted_at
        ) VALUES (
          'thread-before-agent-control',
          'project-before-agent-control',
          'Existing thread',
          '{"instanceId":"codex","model":"gpt-5.4"}',
          'full-access',
          'default',
          NULL,
          NULL,
          NULL,
          '2026-07-21T12:00:00.000Z',
          '2026-07-21T12:00:00.000Z',
          NULL,
          NULL,
          0,
          0,
          0,
          NULL
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 35 });

      const migratedRows = yield* sql<{ readonly agentControlJson: string | null }>`
        SELECT agent_control_json AS "agentControlJson"
        FROM projection_threads
        WHERE thread_id = 'thread-before-agent-control'
      `;
      assert.deepStrictEqual(migratedRows, [{ agentControlJson: null }]);

      const threadId = ThreadId.make("thread-before-agent-control");
      const migratedThread = Option.getOrThrow(
        yield* threads.getById({
          threadId,
        }),
      );
      assert.strictEqual(migratedThread.title, "Existing thread");
      assert.strictEqual(migratedThread.agentControl, null);

      const binding = {
        taskId: AgentControlTaskId.make("task-after-migration"),
        stageRunId: AgentControlStageRunId.make("stage-run-after-migration"),
        attemptId: AgentControlAttemptId.make("attempt-after-migration"),
        roleId: AgentControlRoleId.make("role-after-migration"),
        controlState: "controlled" as const,
      };
      yield* threads.upsert({
        ...migratedThread,
        agentControl: binding,
      });

      const updatedThread = Option.getOrThrow(yield* threads.getById({ threadId }));
      assert.deepStrictEqual(updatedThread.agentControl, binding);
    }),
  );
});
