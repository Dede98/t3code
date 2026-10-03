import * as Scheduler from "../scheduling/Scheduler.ts";
import * as Statement from "effect/unstable/sql/Statement";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations } from "../persistence/Migrations.ts";
import { ThreadLaunchService } from "../orchestration-v2/ThreadLaunchService.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { ScheduledTaskService, layer } from "./ScheduledTaskService.ts";

const decodeTaskInput = Schema.decodeEffect(ScheduledTaskUpsertInput);

it.effect(
  "preserves independent partial updates and never resurrects deleted tasks across WAL connections",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-schedule-races-"));
    const filename = NodePath.join(directory, "statev2.sqlite");
    const dependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Scheduler.layer,
      Layer.mock(ThreadLaunchService)({}),
      Layer.mock(ThreadManagementService)({}),
    );
    const makeLayer = () =>
      layer.pipe(Layer.provide(dependencies), Layer.provide(NodeSqliteClient.layer({ filename })));
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode=WAL`;
        yield* runMigrations();
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      // Build separate production services and native connections in fresh layers.
      // Services must retain their connection scope while exercised.
      yield* Effect.gen(function* () {
        const a = yield* ScheduledTaskService;
        yield* Effect.gen(function* () {
          const b = yield* ScheduledTaskService;
          const input = yield* decodeTaskInput({
            id: "task-race",
            title: "Before",
            prompt: "Before prompt",
            enabled: false,
            schedule: { type: "interval", everyMs: 60000 },
            projectId: "project-race",
            workspaceStrategy: { type: "root" },
            modelSelection: { instanceId: "codex", model: "gpt-5.4" },
            runtimeMode: "full-access",
            interactionMode: "default",
          });
          yield* a.upsert(input);
          const beforeWrite = yield* Deferred.make<void>();
          const releaseWrite = yield* Deferred.make<void>();
          let paused = false;
          const pauseFirstUpdate: Statement.Transformer = (statement) => {
            if (paused || !statement.compile()[0].includes("UPDATE scheduled_tasks SET"))
              return Effect.succeed(statement);
            paused = true;
            return Deferred.succeed(beforeWrite, undefined).pipe(
              Effect.andThen(Deferred.await(releaseWrite)),
              Effect.as(statement),
            );
          };
          // A has read the old schedule and is stopped exactly before its SQL
          // UPDATE. B commits on an independent connection, invalidating A's
          // compare-and-swap. A must reload instead of losing B's partial edit.
          const taskA = yield* a
            .patch({ id: input.id!, projectId: input.projectId, title: "From MCP" })
            .pipe(
              Effect.provideService(Statement.CurrentTransformer, pauseFirstUpdate),
              Effect.forkScoped,
            );
          yield* Deferred.await(beforeWrite);
          yield* b.setEnabled({ id: input.id!, enabled: true });
          yield* Deferred.succeed(releaseWrite, undefined);
          yield* Fiber.join(taskA);
          const saved = (yield* b.list()).tasks[0]!;
          assert.equal(saved.title, "From MCP");
          assert.equal(saved.enabled, true);
          assert.isNotNull(saved.nextRunAt);
          const beforeStaleWrite = yield* Deferred.make<void>();
          const releaseStaleWrite = yield* Deferred.make<void>();
          const staleEdit = yield* a
            .patch({ id: saved.id, projectId: saved.projectId, prompt: "Stale edit" })
            .pipe(
              Effect.provideService(Statement.CurrentTransformer, (statement) =>
                statement.compile()[0].includes("UPDATE scheduled_tasks SET")
                  ? Deferred.succeed(beforeStaleWrite, undefined).pipe(
                      Effect.andThen(Deferred.await(releaseStaleWrite)),
                      Effect.as(statement),
                    )
                  : Effect.succeed(statement),
              ),
              Effect.result,
              Effect.forkScoped,
            );
          yield* Deferred.await(beforeStaleWrite);
          yield* b.delete({ id: saved.id });
          yield* Deferred.succeed(releaseStaleWrite, undefined);
          const deleted = yield* Fiber.join(staleEdit);
          assert.equal(deleted._tag, "Failure");
          if (deleted._tag === "Failure")
            assert.equal(deleted.failure.message, "Schedule task not found.");
          assert.equal((yield* b.list()).tasks.length, 0);
        }).pipe(Effect.provide(makeLayer()));
      }).pipe(Effect.provide(makeLayer()));
      const reader = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
      try {
        assert.equal(reader.prepare("PRAGMA journal_mode").get()?.journal_mode, "wal");
        assert.equal(reader.prepare("SELECT count(*) AS n FROM scheduled_tasks").get()?.n, 0);
      } finally {
        reader.close();
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);

it.effect.each(
  [false, true].flatMap((recovery) =>
    (["schedule", "disable", "delete"] as const).map((change) => ({ recovery, change })),
  ),
)(
  "recovery=$recovery preserves a concurrent $change after its schedule read",
  ({ recovery, change }) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-schedule-completion-"));
    const filename = NodePath.join(directory, "statev2.sqlite");
    const dependencies = Layer.mergeAll(
      NodeCrypto.layer,
      Scheduler.layer,
      Layer.mock(ThreadLaunchService)({
        launch: () => Effect.die("controlled dispatch failure"),
      }),
      Layer.mock(ThreadManagementService)({}),
    );
    const service = () =>
      layer.pipe(Layer.provide(dependencies), Layer.provide(NodeSqliteClient.layer({ filename })));
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode=WAL`;
        yield* runMigrations();
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const runner = yield* ScheduledTaskService;
        yield* Effect.gen(function* () {
          const editor = yield* ScheduledTaskService;
          const input = yield* decodeTaskInput({
            id: "completion-race",
            title: "Before",
            prompt: "Run",
            enabled: true,
            schedule: { type: "interval", everyMs: 60000 },
            projectId: "race-project",
            workspaceStrategy: { type: "root" },
            modelSelection: { instanceId: "codex", model: "gpt-5.4" },
            runtimeMode: "full-access",
            interactionMode: "default",
          });
          const { task } = yield* runner.upsert(input);
          const beforeCompletion = yield* Deferred.make<void>();
          const releaseCompletion = yield* Deferred.make<void>();
          let completionWrites = 0;
          const running = yield* runner.runNow({ id: task.id }).pipe(
            Effect.provideService(Statement.CurrentTransformer, (statement) => {
              const query = statement.compile()[0];
              if (
                query.includes("run_count = run_count + 1") &&
                recovery &&
                completionWrites++ === 0
              ) {
                return Effect.die("controlled completion write failure");
              }
              return query.includes("UPDATE scheduled_tasks") &&
                query.includes("run_count = run_count + 1")
                ? Deferred.succeed(beforeCompletion, undefined).pipe(
                    Effect.andThen(Deferred.await(releaseCompletion)),
                    Effect.as(statement),
                  )
                : Effect.succeed(statement);
            }),
            Effect.exit,
            Effect.forkScoped,
          );
          yield* Deferred.await(beforeCompletion);
          if (change === "delete") yield* editor.delete({ id: task.id });
          else
            yield* editor.patch({
              id: task.id,
              projectId: task.projectId,
              ...(change === "disable"
                ? { enabled: false }
                : { schedule: { type: "interval" as const, everyMs: 3600000 } }),
            });
          const edited = (yield* editor.list()).tasks[0];
          yield* Deferred.succeed(releaseCompletion, undefined);
          const outcome = yield* Fiber.join(running);
          assert.equal(outcome._tag, recovery ? "Failure" : "Success");
          const saved = (yield* editor.list()).tasks[0];
          if (change === "delete") assert.isUndefined(saved);
          else {
            assert.deepEqual(saved?.schedule, edited?.schedule);
            assert.equal(saved?.enabled, edited?.enabled);
            assert.equal(saved?.nextRunAt, edited?.nextRunAt);
            assert.equal(saved?.lastRunStatus, "failed");
            assert.equal(saved?.runCount, 1);
          }
        }).pipe(Effect.provide(service()));
      }).pipe(Effect.provide(service()));
      const observer = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
      try {
        assert.equal(observer.prepare("PRAGMA journal_mode").get()?.journal_mode, "wal");
      } finally {
        observer.close();
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);
