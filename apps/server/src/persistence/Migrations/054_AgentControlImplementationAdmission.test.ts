import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { runMigrations } from "../Migrations.ts";

it.live(
  "installs the durable implementation admission boundary without rewriting planning rows",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-implementation-admission-migration-",
        });
        const filename = path.join(directory, "state.sqlite");
        const scopeA = yield* Scope.make("sequential");
        const scopeB = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
        const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
        const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
        const sqlA = Context.get(contextA, SqlClient.SqlClient);
        const sqlB = Context.get(contextB, SqlClient.SqlClient);

        for (const sql of [sqlA, sqlB]) {
          assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
          yield* sql`PRAGMA foreign_keys = ON`;
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        }
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sqlA),
        );
        const planningColumnsBefore = yield* sqlA`PRAGMA table_info(
        agent_control_controlled_thread_reservation_states
      )`;
        const planningTriggerBefore = yield* sqlA<{ readonly sql: string }>`
        SELECT sql FROM sqlite_schema
        WHERE type = 'trigger' AND name = 'agent_control_controlled_thread_event_validate'
      `;

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 54 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlA),
          ),
          [[54, "AgentControlImplementationAdmission"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 54 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          ),
          [],
        );
        assert.deepStrictEqual(
          yield* sqlB`PRAGMA table_info(agent_control_controlled_thread_reservation_states)`,
          planningColumnsBefore,
        );

        const objects = yield* sqlB<{ readonly name: string; readonly type: string }>`
        SELECT name, type FROM sqlite_schema
        WHERE name IN (
          'agent_control_implementation_thread_stream_catalog',
          'agent_control_implementation_thread_reservation_states',
          'agent_control_controlled_thread_stream_catalog_all',
          'agent_control_controlled_thread_reservation_states_all',
          'agent_control_implementation_admission_evidence',
          'agent_control_implementation_admission_receipts',
          'agent_control_implementation_admission_markers'
        ) ORDER BY name
      `;
        assert.equal(objects.length, 7);
        assert.equal(objects.filter((object) => object.type === "table").length, 5);
        assert.equal(objects.filter((object) => object.type === "view").length, 2);

        const planningTriggerAfter = yield* sqlB<{ readonly sql: string }>`
        SELECT sql FROM sqlite_schema
        WHERE type = 'trigger' AND name = 'agent_control_controlled_thread_event_validate'
      `;
        assert.equal(planningTriggerBefore.length, 1);
        assert.include(
          planningTriggerAfter[0]!.sql,
          "json_extract(NEW.payload_json, '$.stageKind') = 'planning'",
        );

        const storageTriggers = yield* sqlB<{ readonly count: number }>`
        SELECT count(*) AS count FROM sqlite_schema
        WHERE type = 'trigger' AND name LIKE 'agent_control_implementation_%_storage_validate'
      `;
        assert.deepStrictEqual(storageTriggers, [{ count: 5 }]);
        assert.deepStrictEqual(yield* sqlB`PRAGMA foreign_key_check`, []);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
