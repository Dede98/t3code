// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/unstable/sql/Migrator";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { runMigrations, forkIntegrationMigrationEntries } from "./Migrations.ts";
import OrchestrationV2 from "./Migrations/055_OrchestrationV2.ts";
import ClaudeSessionStore from "./Migrations/033_ClaudeSessionStore.ts";
import RemoveIndexes from "./Migrations/056_RemoveRedundantProjectionIndexes.ts";

it.effect.each(
  (
    [
      "preview53",
      "preview54",
      "preview54-indexes",
      "upstream-integration",
      "fork-integration",
    ] as const
  ).flatMap((history) => [false, true].map((failFirst) => ({ history, failFirst }))),
)(
  "preserves $history ledger and V2 progress across fresh WAL connections (retry=$failFirst)",
  ({ history, failFirst }) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-v2-lineage-"));
    const filename = NodePath.join(directory, "statev2.sqlite");
    const connection = () => NodeSqliteClient.layer({ filename });
    return Effect.gen(function* () {
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA journal_mode=WAL`;
        if (history === "fork-integration") {
          yield* Migrator.make({})({
            loader: Migrator.fromRecord(
              Object.fromEntries(
                forkIntegrationMigrationEntries
                  .filter(([id]) => id <= 55)
                  .map(([id, name, migration]) => [`${id}_${name}`, migration]),
              ),
            ),
          });
        } else {
          yield* runMigrations({ toMigrationInclusive: history === "preview53" ? 52 : 53 });
          yield* Migrator.make({})({
            loader: Migrator.fromRecord({
              [history === "preview53" ? "53_OrchestrationV2" : "54_OrchestrationV2"]:
                OrchestrationV2,
              ...(history === "preview54-indexes"
                ? { "55_RemoveRedundantProjectionIndexes": RemoveIndexes }
                : {}),
              ...(history === "upstream-integration"
                ? { "55_ClaudeSessionStore": ClaudeSessionStore }
                : {}),
            }),
          });
        }
        yield* sql`INSERT INTO orchestration_v2_legacy_imports
            (thread_id, source_updated_at, shell_imported_at, transcript_imported_at, imported_message_count)
            VALUES ('used-thread', '2026-09-15', '2026-09-15', '2026-09-16', 42)`;
      }).pipe(Effect.provide(connection()));
      const observer = new NodeSqlite.DatabaseSync(filename);
      try {
        assert.equal(observer.prepare("PRAGMA journal_mode").get()?.journal_mode, "wal");
        const ledger = observer
          .prepare("SELECT * FROM effect_sql_migrations ORDER BY migration_id")
          .all();
        const last = Number(ledger.at(-1)?.migration_id);
        const progress = observer.prepare("SELECT * FROM orchestration_v2_legacy_imports").all();
        if (failFirst) {
          observer.exec(
            "CREATE TRIGGER fail_upgrade BEFORE INSERT ON effect_sql_migrations BEGIN SELECT RAISE(ABORT, 'injected upgrade failure'); END",
          );
          assert.equal(
            (yield* Effect.result(runMigrations().pipe(Effect.provide(connection()))))._tag,
            "Failure",
          );
          assert.deepEqual(
            observer.prepare("SELECT * FROM effect_sql_migrations ORDER BY migration_id").all(),
            ledger,
          );
          observer.exec("DROP TRIGGER fail_upgrade");
        }
        yield* runMigrations().pipe(Effect.provide(connection()));
        assert.deepEqual(yield* runMigrations().pipe(Effect.provide(connection())), []);
        assert.deepEqual(
          observer
            .prepare(
              "SELECT * FROM effect_sql_migrations WHERE migration_id <= ? ORDER BY migration_id",
            )
            .all(last),
          ledger,
        );
        assert.deepEqual(
          observer.prepare("SELECT * FROM orchestration_v2_legacy_imports").all(),
          progress,
        );
        assert.equal(
          observer.prepare("SELECT MAX(migration_id) AS id FROM effect_sql_migrations").get()?.id,
          57,
        );
        assert.ok(
          observer
            .prepare("PRAGMA table_info(projection_threads)")
            .all()
            .some((row) => row.name === "auto_settle_disabled_at"),
        );
        assert.ok(
          observer
            .prepare("SELECT name FROM sqlite_master WHERE name='claude_session_store_keys'")
            .get(),
        );
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
