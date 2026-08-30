import * as NodeServices from "@effect/platform-node/NodeServices";
import { AgentControlTaskId, type AgentControlTaskState, ProjectId } from "@t3tools/contracts";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { layer as AgentControlTaskStateRepositoryLive } from "./AgentControlTaskStateRepository.ts";

const createdAt = "2026-08-05T08:15:30.000Z";
const updatedAt = "2026-08-05T09:45:10.000Z";
const sourceUpdatedAt = "2026-08-05T09:30:00.000Z";

const makeState = (suffix: string): AgentControlTaskState => ({
  schemaVersion: 1,
  taskId: AgentControlTaskId.make(`task-state-repository-${suffix}`),
  source: {
    projectId: ProjectId.make(`task-state-project-${suffix}`),
    repositoryNodeId: `repository-node-${suffix}`,
    issueNodeId: `issue-node-${suffix}`,
    issueNumber: 41,
    issueUrl: `https://github.test/t3/task/issues/41?case=${suffix}`,
  },
  status: "candidate",
  sourceGate: "eligible",
  stage: "intake",
  sourceUpdatedAt,
  githubIntakeSequence: 7,
  sourceSnapshot: {
    repositoryNodeId: `repository-node-${suffix}`,
    issueNodeId: `issue-node-${suffix}`,
    number: 41,
    url: `https://github.test/t3/task/issues/41?case=${suffix}`,
    state: "open",
    title: `Task title ${suffix}`,
    body: `Task body ${suffix}`,
    contentTrust: "untrusted-external",
    updatedAt: sourceUpdatedAt,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  },
  createdAt,
  updatedAt,
  revision: 1,
  sequence: 1,
});

const makeHarness = Effect.fn("makeTaskStateRepositoryHarness")(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fileSystem.makeTempDirectoryScoped({
    prefix: "t3-task-state-repository-",
  });
  const filename = path.join(directory, "state.sqlite");
  const scope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const context = yield* Layer.buildWithScope(
    AgentControlTaskStateRepositoryLive.pipe(
      Layer.provideMerge(NodeSqliteClient.layer({ filename })),
    ),
    scope,
  );
  const sql = Context.get(context, SqlClient.SqlClient);
  const repository = Context.get(context, AgentControlTaskStateRepository);
  assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
  yield* sql`PRAGMA foreign_keys = ON`;
  assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
  yield* runMigrations({ toMigrationInclusive: 42 }).pipe(
    Effect.provideService(SqlClient.SqlClient, sql),
  );
  return { filename, repository, sql };
});

const withHarness = <A, E, R>(
  use: (harness: Effect.Success<ReturnType<typeof makeHarness>>) => Effect.Effect<A, E, R>,
) => Effect.scoped(Effect.flatMap(makeHarness(), use)).pipe(Effect.provide(NodeServices.layer));

it.effect("findByIdentity decodes a complete valid task projection", () =>
  withHarness(({ repository }) =>
    Effect.gen(function* () {
      const state = makeState("identity");
      yield* repository.save(state, 0);

      const found = yield* repository.findByIdentity(
        state.source.projectId,
        state.source.repositoryNodeId,
        state.source.issueNodeId,
      );

      assert.isTrue(Option.isSome(found));
      if (Option.isSome(found)) {
        assert.equal(found.value.createdAt, createdAt);
        assert.equal(found.value.updatedAt, updatedAt);
        assert.deepStrictEqual(found.value, state);
      }
    }),
  ),
);

it.effect("findBySourceNumber decodes a complete valid task projection", () =>
  withHarness(({ repository }) =>
    Effect.gen(function* () {
      const state = makeState("source-number");
      yield* repository.save(state, 0);

      const found = yield* repository.findBySourceNumber(
        state.source.projectId,
        state.source.repositoryNodeId,
        state.source.issueNumber,
      );

      assert.isTrue(Option.isSome(found));
      if (Option.isSome(found)) {
        assert.equal(found.value.createdAt, createdAt);
        assert.equal(found.value.updatedAt, updatedAt);
        assert.deepStrictEqual(found.value, state);
      }
    }),
  ),
);

it.effect("keeps absent identity and source-number lookups empty and project-scoped", () =>
  withHarness(({ repository }) =>
    Effect.gen(function* () {
      const state = makeState("absence");
      yield* repository.save(state, 0);

      assert.isTrue(
        Option.isNone(
          yield* repository.findByIdentity(
            state.source.projectId,
            state.source.repositoryNodeId,
            "missing-issue-node",
          ),
        ),
      );
      assert.isTrue(
        Option.isNone(
          yield* repository.findBySourceNumber(
            state.source.projectId,
            state.source.repositoryNodeId,
            42,
          ),
        ),
      );
      assert.isTrue(
        Option.isNone(
          yield* repository.findBySourceNumber(
            ProjectId.make("foreign-task-state-project"),
            state.source.repositoryNodeId,
            state.source.issueNumber,
          ),
        ),
      );
    }),
  ),
);

it.effect("fails closed when relational task evidence diverges from state JSON", () =>
  withHarness(({ repository, sql }) =>
    Effect.gen(function* () {
      const state = makeState("corrupt");
      yield* repository.save(state, 0);
      yield* sql`
        UPDATE agent_control_task_states
        SET updated_at = '2026-08-05T10:00:00.000Z'
        WHERE task_id = ${state.taskId}
      `;

      const identity = yield* Effect.result(
        repository.findByIdentity(
          state.source.projectId,
          state.source.repositoryNodeId,
          state.source.issueNodeId,
        ),
      );
      const sourceNumber = yield* Effect.result(
        repository.findBySourceNumber(
          state.source.projectId,
          state.source.repositoryNodeId,
          state.source.issueNumber,
        ),
      );

      assert.equal(identity._tag, "Failure");
      assert.equal(sourceNumber._tag, "Failure");
      if (identity._tag === "Failure") {
        assert.equal(identity.failure._tag, "AgentControlPersistenceDecodeError");
        assert.equal(identity.failure.operation, "AgentControlTaskStateRepository.findByIdentity");
      }
      if (sourceNumber._tag === "Failure") {
        assert.equal(sourceNumber.failure._tag, "AgentControlPersistenceDecodeError");
        assert.equal(
          sourceNumber.failure.operation,
          "AgentControlTaskStateRepository.findBySourceNumber",
        );
      }
    }),
  ),
);

it.effect("rejects duplicate, excess, BLOB, and wrong mirror storage authority", () =>
  withHarness(({ filename, repository }) =>
    Effect.gen(function* () {
      const suffixes = ["duplicate", "excess", "blob", "mirror-blob"] as const;
      for (const suffix of suffixes) yield* repository.save(makeState(suffix), 0);

      const database = new NodeSqlite.DatabaseSync(filename);
      try {
        database.exec("PRAGMA ignore_check_constraints = ON");
        const triggers = database
          .prepare(
            `SELECT name, sql FROM main.sqlite_schema
             WHERE type = 'trigger' AND tbl_name = 'agent_control_task_states'
               AND sql LIKE '%BEFORE UPDATE%'`,
          )
          .all() as unknown as ReadonlyArray<{ readonly name: string; readonly sql: string }>;
        for (const trigger of triggers) {
          database.exec(`DROP TRIGGER main."${trigger.name.replaceAll('"', '""')}"`);
        }
        try {
          const source = (suffix: string) =>
            (
              database
                .prepare(
                  "SELECT state_json AS source FROM main.agent_control_task_states WHERE task_id = ?",
                )
                .get(`task-state-repository-${suffix}`) as { readonly source: string }
            ).source;
          const duplicate = source("duplicate").replace(
            '"title":"Task title duplicate"',
            '"title":"Task title duplicate","title":"Task title duplicate"',
          );
          database
            .prepare("UPDATE main.agent_control_task_states SET state_json = ? WHERE task_id = ?")
            .run(duplicate, "task-state-repository-duplicate");
          const excess = source("excess");
          database
            .prepare("UPDATE main.agent_control_task_states SET state_json = ? WHERE task_id = ?")
            .run(`${excess.slice(0, -1)},"unexpected":true}`, "task-state-repository-excess");
          database
            .prepare("UPDATE main.agent_control_task_states SET state_json = ? WHERE task_id = ?")
            .run(Buffer.from(source("blob"), "utf8"), "task-state-repository-blob");
          database
            .prepare(
              `UPDATE main.agent_control_task_states SET issue_url = CAST(issue_url AS BLOB)
               WHERE task_id = ?`,
            )
            .run("task-state-repository-mirror-blob");
        } finally {
          for (const trigger of triggers) database.exec(trigger.sql);
        }
      } finally {
        database.close();
      }

      for (const suffix of suffixes) {
        const result = yield* Effect.result(
          repository.get(AgentControlTaskId.make(`task-state-repository-${suffix}`)),
        );
        assert.equal(result._tag, "Failure", suffix);
        if (result._tag === "Failure") {
          assert.equal(result.failure._tag, "AgentControlPersistenceDecodeError", suffix);
        }
      }
    }),
  ),
);
