import { assert, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ProjectionThreadSessionRepositoryLive } from "../Layers/ProjectionThreadSessions.ts";
import { runMigrations } from "../Migrations.ts";
import { ProjectionThreadSessionRepository } from "../Services/ProjectionThreadSessions.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

layer("027_028_ProviderInstanceIdColumns", (it) => {
  it.effect("continues when provider_session_runtime was partially migrated", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;

      yield* runMigrations({ toMigrationInclusive: 26 });
      yield* sql`
        ALTER TABLE provider_session_runtime
        ADD COLUMN provider_instance_id TEXT
      `;

      yield* runMigrations({ toMigrationInclusive: 28 });

      const migrations = yield* sql<{
        readonly migration_id: number;
        readonly name: string;
      }>`
        SELECT migration_id, name
        FROM effect_sql_migrations
        WHERE migration_id IN (27, 28)
        ORDER BY migration_id
      `;
      assert.deepStrictEqual(migrations, [
        {
          migration_id: 27,
          name: "ProviderSessionRuntimeInstanceId",
        },
        {
          migration_id: 28,
          name: "ProjectionThreadSessionInstanceId",
        },
      ]);

      const providerSessionColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(provider_session_runtime)
      `;
      assert.ok(providerSessionColumns.some((column) => column.name === "provider_instance_id"));

      const projectionThreadSessionColumns = yield* sql<{ readonly name: string }>`
        PRAGMA table_info(projection_thread_sessions)
      `;
      assert.ok(
        projectionThreadSessionColumns.some((column) => column.name === "provider_instance_id"),
      );

      const providerSessionIndexes = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(provider_session_runtime)
      `;
      assert.ok(
        providerSessionIndexes.some(
          (index) => index.name === "idx_provider_session_runtime_instance",
        ),
      );

      const projectionThreadSessionIndexes = yield* sql<{ readonly name: string }>`
        PRAGMA index_list(projection_thread_sessions)
      `;
      assert.ok(
        projectionThreadSessionIndexes.some(
          (index) => index.name === "idx_projection_thread_sessions_instance",
        ),
      );
    }),
  );

  it.effect("preserves ambiguous legacy rows as readable null projections", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const threadId = ThreadId.make("thread-legacy-instance");

      yield* runMigrations({ toMigrationInclusive: 26 });
      yield* sql`
        INSERT INTO provider_session_runtime (
          thread_id,
          provider_name,
          adapter_key,
          runtime_mode,
          status,
          last_seen_at,
          resume_cursor_json,
          runtime_payload_json
        ) VALUES (
          ${threadId},
          'codex',
          'codex',
          'full-access',
          'running',
          '2026-01-01T00:00:00.000Z',
          NULL,
          NULL
        )
      `;
      yield* sql`
        INSERT INTO projection_thread_sessions (
          thread_id,
          status,
          provider_name,
          runtime_mode,
          active_turn_id,
          last_error,
          updated_at
        ) VALUES (
          ${threadId},
          'running',
          'codex',
          'full-access',
          NULL,
          NULL,
          '2026-01-01T00:00:00.000Z'
        )
      `;

      yield* runMigrations({ toMigrationInclusive: 28 });

      const runtimeRows = yield* sql<{ readonly providerInstanceId: string | null }>`
        SELECT provider_instance_id AS "providerInstanceId"
        FROM provider_session_runtime
        WHERE thread_id = ${threadId}
      `;
      const projectionRows = yield* sql<{ readonly providerInstanceId: string | null }>`
        SELECT provider_instance_id AS "providerInstanceId"
        FROM projection_thread_sessions
        WHERE thread_id = ${threadId}
      `;
      assert.deepStrictEqual(runtimeRows, [{ providerInstanceId: null }]);
      assert.deepStrictEqual(projectionRows, [{ providerInstanceId: null }]);

      yield* Effect.gen(function* () {
        const repository = yield* ProjectionThreadSessionRepository;
        const legacyRead = yield* repository.getByThreadId({ threadId });
        assert.equal(Option.isSome(legacyRead), true);
        if (Option.isSome(legacyRead)) {
          assert.equal(legacyRead.value.providerInstanceId, null);
        }

        yield* repository.upsert({
          threadId,
          status: "running",
          providerName: "codex",
          providerInstanceId: ProviderInstanceId.make("codex_work"),
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: "2026-01-01T00:00:01.000Z",
        });

        const migratedRead = yield* repository.getByThreadId({ threadId });
        assert.equal(Option.isSome(migratedRead), true);
        if (Option.isSome(migratedRead)) {
          assert.equal(migratedRead.value.providerInstanceId, "codex_work");
        }
      }).pipe(Effect.provide(ProjectionThreadSessionRepositoryLive));
    }),
  );
});
