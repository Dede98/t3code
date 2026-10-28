// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";
import { migrationEntries, runMigrations } from "./Migrations.ts";
import { seedForkMigrations } from "./Migrations.testkit.ts";
import PeerLinks from "./Migrations/070_PeerLinks.ts";
import PeerLinkFence from "./Migrations/071_PeerLinkFence.ts";
import WaitingLinkRequestIndex from "./Migrations/072_WaitingLinkRequestIndex.ts";
import { initializeV2Database } from "./initializeV2Database.ts";

const seedFork = (through = 55) => seedForkMigrations(through);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

it.effect(
  "upgrades the fork ledger on a snapshot and preserves the original, including WAL",
  () => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-fork-v2-"));
    const source = NodePath.join(directory, "state.sqlite");
    const destination = NodePath.join(directory, "statev2.sqlite");
    return Effect.gen(function* () {
      yield* seedFork().pipe(Effect.provide(NodeSqliteClient.layer({ filename: source })));
      const writer = new NodeSqlite.DatabaseSync(source);
      try {
        writer.exec(
          "PRAGMA journal_mode=WAL; CREATE TABLE fork_sentinel (value TEXT); INSERT INTO fork_sentinel VALUES ('committed');",
        );
        writer
          .prepare("INSERT INTO claude_session_store_keys VALUES (?, ?, ?, ?)")
          .run("-fork-project", "native-session", "session.jsonl", 1234);
        const nativeEntry = yield* encodeJson({
          type: "user",
          uuid: "native-message",
          message: { role: "user", content: "preserved native context ".repeat(100) },
        });
        writer
          .prepare(
            "INSERT INTO claude_session_store_entries (session_id, subpath, entry_uuid, entry_json) VALUES (?, ?, ?, ?)",
          )
          .run("native-session", "session.jsonl", "native-message", nativeEntry);
        const history = writer
          .prepare("SELECT * FROM effect_sql_migrations ORDER BY migration_id")
          .all();
        const original = NodeFS.readFileSync(source);
        yield* initializeV2Database(destination);
        const migrate = runMigrations().pipe(
          Effect.provide(NodeSqliteClient.layer({ filename: destination })),
        );
        assert.deepEqual(yield* migrate, [
          [56, "ForkOrchestrationV2"],
          [57, "RemoveRedundantProjectionIndexes"],
          [58, "ScheduledTaskWebhooks"],
          [59, "WebhookRelayDeliveries"],
          [60, "McpAppModelContext"],
          [61, "ThreadSnapshotWindowIndexes"],
          [62, "PullRequestReviewGuides"],
          [63, "ThreadFailureLookupIndex"],
          [64, "DropUnusedV1Projections"],
          [65, "CanonicalizeThreadPullRequestLinks"],
          [66, "CodexUsageNativeTurnIndex"],
          [67, "RepairClientSettlementTimestamps"],
          [68, "WebhookDedupe"],
          [69, "ShellStopRequestIndex"],
          [70, "ThreadMessageSearch"],
          [71, "PeerLinks"],
          [72, "PeerLinkFence"],
          [73, "WaitingLinkRequestIndex"],
        ]);
        // Each startup owns a fresh connection/layer.
        assert.deepEqual(yield* migrate, []);
        yield* initializeV2Database(destination);
        const reader = new NodeSqlite.DatabaseSync(destination, { readOnly: true });
        try {
          assert.deepEqual(
            reader
              .prepare(
                "SELECT * FROM effect_sql_migrations WHERE migration_id <= 55 ORDER BY migration_id",
              )
              .all(),
            history,
          );
          assert.equal(reader.prepare("SELECT value FROM fork_sentinel").get()?.value, "committed");
          assert.equal(
            reader.prepare("SELECT count(*) AS n FROM orchestration_v2_events").get()?.n,
            0,
          );
          assert.equal(
            reader.prepare("SELECT count(*) AS n FROM claude_session_store_keys").get()?.n,
            1,
          );
          assert.equal(
            reader.prepare("SELECT entry_json FROM claude_session_store_entries").get()?.entry_json,
            nativeEntry,
          );
        } finally {
          reader.close();
        }
        assert.deepEqual(
          writer.prepare("SELECT * FROM effect_sql_migrations ORDER BY migration_id").all(),
          history,
        );
        assert.deepEqual(NodeFS.readFileSync(source), original);
        assert.equal(
          writer
            .prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='orchestration_v2_events'")
            .get()?.n,
          0,
        );
      } finally {
        writer.close();
      }
    }).pipe(
      Effect.provide(NodeServices.layer),
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);

it.effect("rolls back a failed fork foundation and retries from a fresh connection", () => {
  const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-fork-retry-"));
  const filename = NodePath.join(directory, "statev2.sqlite");
  const db = NodeSqliteClient.layer({ filename });
  return Effect.gen(function* () {
    yield* Effect.gen(function* () {
      yield* seedFork();
      const sql = yield* SqlClient.SqlClient;
      yield* sql`CREATE TRIGGER fail_v2 BEFORE INSERT ON effect_sql_migrations WHEN NEW.migration_id = 56 BEGIN SELECT RAISE(ABORT, 'injected failure'); END`;
    }).pipe(Effect.provide(db));
    assert.equal(
      (yield* Effect.result(
        runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename }))),
      ))._tag,
      "Failure",
    );
    const reader = new NodeSqlite.DatabaseSync(filename);
    try {
      assert.equal(
        reader.prepare("SELECT MAX(migration_id) AS id FROM effect_sql_migrations").get()?.id,
        55,
      );
      assert.equal(
        reader
          .prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='orchestration_v2_events'")
          .get()?.n,
        0,
      );
      reader.exec("DROP TRIGGER fail_v2");
    } finally {
      reader.close();
    }
    assert.deepEqual(
      yield* runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename }))),
      [
        [56, "ForkOrchestrationV2"],
        [57, "RemoveRedundantProjectionIndexes"],
        [58, "ScheduledTaskWebhooks"],
        [59, "WebhookRelayDeliveries"],
        [60, "McpAppModelContext"],
        [61, "ThreadSnapshotWindowIndexes"],
        [62, "PullRequestReviewGuides"],
        [63, "ThreadFailureLookupIndex"],
        [64, "DropUnusedV1Projections"],
        [65, "CanonicalizeThreadPullRequestLinks"],
        [66, "CodexUsageNativeTurnIndex"],
        [67, "RepairClientSettlementTimestamps"],
        [68, "WebhookDedupe"],
        [69, "ShellStopRequestIndex"],
        [70, "ThreadMessageSearch"],
        [71, "PeerLinks"],
        [72, "PeerLinkFence"],
        [73, "WaitingLinkRequestIndex"],
      ],
    );
  }).pipe(
    Effect.ensuring(Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true }))),
  );
});

it.effect("refuses to start when another migration took a fork lineage V2 id", () =>
  Effect.gen(function* () {
    yield* seedFork();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO effect_sql_migrations (migration_id, name) VALUES (56, 'FutureMainMigration')
    `;
    const before = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;

    const exit = yield* Effect.exit(runMigrations());

    assert.equal(exit._tag, "Failure");
    assert.include(String(exit), "56_ForkOrchestrationV2 (recorded: FutureMainMigration)");
    assert.notInclude(String(exit), "55_");
    assert.deepEqual(yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`, before);
    assert.equal(
      (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_events'`).length,
      0,
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each([33, 52, 53].map((through) => ({ through })))(
  "finishes a fork database at migration $through without replaying shifted migrations",
  ({ through }) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-fork-partial-"));
    const filename = NodePath.join(directory, "statev2.sqlite");
    return Effect.gen(function* () {
      yield* seedFork(through);
      yield* runMigrations();
      assert.deepEqual(yield* runMigrations(), []);
      const sql = yield* SqlClient.SqlClient;
      assert.equal(
        (yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id=33`)[0]?.name,
        "ClaudeSessionStore",
      );
      assert.equal(
        (yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id=56`)[0]?.name,
        "ForkOrchestrationV2",
      );
    }).pipe(
      Effect.provide(NodeSqliteClient.layer({ filename })),
      Effect.ensuring(
        Effect.sync(() => NodeFS.rmSync(directory, { recursive: true, force: true })),
      ),
    );
  },
);

it.effect.each([0, 32, 52, 53, 54].map((through) => ({ through })))(
  "initializes upstream history $through and survives a fresh-connection restart",
  ({ through }) => {
    const directory = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-upstream-v2-"));
    const filename = NodePath.join(directory, "statev2.sqlite");
    return Effect.gen(function* () {
      if (through > 0)
        yield* runMigrations({ toMigrationInclusive: through }).pipe(
          Effect.provide(NodeSqliteClient.layer({ filename })),
        );
      yield* runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      assert.deepEqual(
        yield* runMigrations().pipe(Effect.provide(NodeSqliteClient.layer({ filename }))),
        [],
      );
      const reader = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
      try {
        assert.equal(
          reader.prepare("SELECT name FROM effect_sql_migrations WHERE migration_id=55").get()
            ?.name,
          "OrchestrationV2",
        );
        assert.equal(
          reader.prepare("SELECT name FROM effect_sql_migrations WHERE migration_id=57").get()
            ?.name,
          "ClaudeSessionStore",
        );
        assert.equal(
          reader.prepare("SELECT count(*) AS n FROM orchestration_v2_events").get()?.n,
          0,
        );
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

it.effect.each([57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72])(
  "preserves official upstream webhook ledger through %s",
  (through) =>
    Effect.gen(function* () {
      const official = [
        ...migrationEntries.filter(([id]) => id <= 56),
        ...migrationEntries
          .filter(([id]) => id >= 58 && id <= 70)
          .map(([id, name, migration]) => [id - 1, name, migration] as const),
        [70, "PeerLinks", PeerLinks] as const,
        [71, "PeerLinkFence", PeerLinkFence] as const,
        [72, "WaitingLinkRequestIndex", WaitingLinkRequestIndex] as const,
      ].filter(([id]) => id <= through);
      yield* Migrator.make({})({
        loader: Migrator.fromRecord(
          Object.fromEntries(official.map(([id, name, migration]) => [`${id}_${name}`, migration])),
        ),
      });
      const sql = yield* SqlClient.SqlClient;
      const history = yield* sql`SELECT * FROM effect_sql_migrations ORDER BY migration_id`;
      const appContext =
        through >= 59
          ? [
              {
                thread_id: "thread",
                item_id: "item",
                server: "server",
                tool: "tool",
                text: "preserved",
                updated_at: "2026-10-08",
              },
            ]
          : [];
      if (through >= 59) {
        yield* sql`INSERT INTO mcp_app_model_context ${sql.insert(appContext)}`;
      }
      if (through >= 70) {
        yield* sql`INSERT INTO peer_environment_links
          (environment_id, secret_name, label, urls_json, access, linked_at, expires_at)
          VALUES ('peer', 'kept-secret-ref', 'Peer', '[]', 'read', '2026-10-28', '2026-11-28')`;
      }
      yield* runMigrations();
      assert.ok(
        (yield* sql`PRAGMA table_info(peer_environment_links)`).some(
          (column) => column.name === "fenced",
        ),
      );
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_projection_turn_items_waiting_link_idx'`)
          .length,
        1,
      );
      if (through >= 70) {
        assert.equal(
          (yield* sql`SELECT secret_name FROM peer_environment_links WHERE environment_id = 'peer'`)[0]
            ?.secret_name,
          "kept-secret-ref",
        );
      }
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_projection_turn_items_failed_error_idx'`)
          .length,
        1,
      );
      assert.deepEqual(yield* sql`SELECT * FROM mcp_app_model_context`, appContext);
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_projection_nodes_live_idx'`)
          .length,
        1,
      );
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'orchestration_v2_projection_turn_items_stop_request_idx'`)
          .length,
        1,
      );
      assert.deepEqual(
        yield* sql`SELECT * FROM effect_sql_migrations WHERE migration_id <= ${through} ORDER BY migration_id`,
        history,
      );
      assert.deepEqual(yield* runMigrations(), []);
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'pull_request_review_guides'`)
          .length,
        1,
      );
      assert.equal(
        (yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = ${through >= 70 ? through + 1 : through >= 69 ? 70 : through >= 68 ? 69 : through >= 67 ? 68 : through >= 66 ? 67 : through >= 65 ? 66 : through >= 64 ? 65 : through >= 63 ? 64 : through >= 62 ? 63 : through >= 61 ? 62 : through >= 59 ? 61 : 59}`)[0]
          ?.name,
        "ClaudeSessionStore",
      );
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'scheduled_task_webhook_relay_deliveries'`)
          .length,
        1,
      );
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'scheduled_task_webhook_pending'`)
          .length,
        1,
      );
      assert.equal(
        (yield* sql`SELECT name FROM sqlite_master WHERE name = 'claude_session_store_keys'`)
          .length,
        1,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
