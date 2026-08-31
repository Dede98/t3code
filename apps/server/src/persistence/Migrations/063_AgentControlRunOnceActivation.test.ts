import * as NodeServices from "@effect/platform-node/NodeServices";
// @effect-diagnostics nodeBuiltinImport:off - captures a real production 062 fixture in a child.
import * as NodeChildProcess from "node:child_process";
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeNet from "node:net";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";
import { AgentControlRunOnceId, AgentControlTaskId, ProjectId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import { loadRunOnceTerminalAuthority } from "../../agentControl/runOnce/authority.ts";
import { AgentControlTaskEventStore } from "../../agentControl/task/Services/AgentControlTaskEventStore.ts";
import { layer as AgentControlTaskEventStoreLive } from "../../agentControl/task/Layers/AgentControlTaskEventStore.ts";
import {
  makeMigration063,
  type Migration063FaultPoint,
} from "./063_AgentControlRunOnceActivation.ts";

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const capturePopulatedProduction062 = (
  controlDirectory: string,
  snapshotFilename: string,
): Promise<void> =>
  new Promise((resolve, reject) => {
    const acknowledgementSocket = NodePath.join(
      "/tmp",
      `t3-run-once-062-${process.pid}-${NodeCrypto.randomUUID()}.sock`,
    );
    const preload = NodeURL.pathToFileURL(
      NodePath.join(
        process.cwd(),
        "apps/server/src/agentControl/task/testing/captureProduction062OnCleanup.mjs",
      ),
    ).href;
    const childEnvironment: NodeJS.ProcessEnv = {
      ...process.env,
      PATH: `/opt/homebrew/opt/node@24/bin:${process.env.PATH ?? ""}`,
      TMPDIR: controlDirectory,
      NODE_OPTIONS: [process.env.NODE_OPTIONS, `--import=${preload}`].filter(Boolean).join(" "),
      T3_RUN_ONCE_062_ACK_SOCKET: acknowledgementSocket,
      T3_RUN_ONCE_062_SNAPSHOT: snapshotFilename,
    };
    delete childEnvironment.ELECTRON_RUN_AS_NODE;
    const output: Array<string> = [];
    let captured = false;
    let settled = false;
    const server = NodeNet.createServer((socket) => {
      let request = "";
      socket.setEncoding("utf8");
      socket.on("data", (chunk) => {
        request += chunk;
        if (!request.includes("\n")) return;
        try {
          assert.equal(request.trim(), "snapshot-ready");
          const database = new NodeSqlite.DatabaseSync(snapshotFilename, { readOnly: true });
          try {
            const authority = database
              .prepare(`SELECT
                (SELECT count(*) FROM main.effect_sql_migrations WHERE migration_id = 62)
                  AS migration,
                (SELECT count(*)
                 FROM main.agent_control_task_verification_finalization_markers) AS markers`)
              .get() as { readonly markers: number; readonly migration: number };
            assert.deepStrictEqual(authority, { markers: 1, migration: 1 });
          } finally {
            database.close();
          }
          captured = true;
          socket.end("ack\n");
        } catch (cause) {
          socket.end("error\n");
          if (!settled) {
            settled = true;
            reject(cause);
          }
        }
      });
    });
    const closeServer = () => {
      server.close();
      if (NodeFS.existsSync(acknowledgementSocket)) NodeFS.unlinkSync(acknowledgementSocket);
    };
    server.once("error", (cause) => {
      if (settled) return;
      settled = true;
      reject(cause);
    });
    server.listen(acknowledgementSocket, () => {
      const child = NodeChildProcess.spawn(
        NodePath.join(process.cwd(), "node_modules/.bin/vp"),
        [
          "test",
          "run",
          "apps/server/src/agentControl/task/Layers/AgentControlTaskVerificationFinalizer.test.ts",
          "-t",
          "migrates a populated production 061 authority to 062 and replays after restart",
        ],
        {
          cwd: process.cwd(),
          env: childEnvironment,
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      child.stdout.on("data", (chunk) => output.push(String(chunk)));
      child.stderr.on("data", (chunk) => output.push(String(chunk)));
      child.once("error", (cause) => {
        closeServer();
        if (settled) return;
        settled = true;
        reject(cause);
      });
      child.once("close", (code) => {
        closeServer();
        if (settled) return;
        settled = true;
        if (code === 0 && captured) resolve();
        else {
          reject(
            new Error(
              `production 062 fixture ${captured ? "child failed" : "was not captured"}\n${output.join("")}`,
            ),
          );
        }
      });
    });
  });

it.live("installs run-once authority on a fresh database and is visible over WAL", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-run-once-063-" });
      const filename = path.join(directory, "fresh.sqlite");
      const writer = yield* openDatabase(filename);
      const observer = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(writer.scope, Exit.void));

      const applied = yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
        Effect.provideService(SqlClient.SqlClient, writer.sql),
      );
      assert.equal(applied.length, 63);
      assert.deepStrictEqual(applied.at(-1), [63, "AgentControlRunOnceActivation"]);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql<{ readonly name: string }>`
          SELECT name FROM main.sqlite_schema
          WHERE type = 'table' AND name LIKE 'agent_control_run_once_%'
          ORDER BY name
        `,
        [
          { name: "agent_control_run_once_activations" },
          { name: "agent_control_run_once_publications" },
          { name: "agent_control_run_once_states" },
          { name: "agent_control_run_once_step_claims" },
          { name: "agent_control_run_once_step_evidence" },
          { name: "agent_control_run_once_step_markers" },
          { name: "agent_control_run_once_step_receipts" },
        ],
      );
      assert.deepStrictEqual(yield* observer.sql`PRAGMA foreign_key_check`, []);
      assert.equal((yield* observer.sql`PRAGMA integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "upgrades 062 atomically, preserves bytes, rejects collisions, and requires strict UDFs",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-run-once-063-upgrade-" });
        const filename = path.join(directory, "upgrade.sqlite");
        yield* Effect.promise(() => capturePopulatedProduction062(directory, filename));
        const writer = yield* openDatabase(filename);
        const observer = yield* openDatabase(filename);
        yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
        yield* Effect.addFinalizer(() => Scope.close(writer.scope, Exit.void));
        const terminalAuthorityBefore = yield* writer.sql<Record<string, unknown>>`
          SELECT evidence.task_id AS "taskId", evidence.project_id AS "projectId",
            typeof(evidence.finalization_json) AS "evidenceStorage",
            hex(CAST(evidence.finalization_json AS BLOB)) AS "evidenceBytes",
            typeof(event.payload_json) AS "eventStorage",
            hex(CAST(event.payload_json AS BLOB)) AS "eventBytes",
            hex(CAST(task.state_json AS BLOB)) AS "projectionBytes",
            receipt.receipt_id AS "receiptId", receipt.status AS "receiptStatus",
            marker.marker_id AS "markerId", marker.marker_fingerprint AS "markerFingerprint"
          FROM main.agent_control_task_verification_finalization_evidence evidence
          JOIN main.agent_control_task_verification_finalization_receipts receipt
            ON receipt.receipt_id = evidence.receipt_id
          JOIN main.agent_control_task_verification_finalization_markers marker
            ON marker.marker_id = evidence.marker_id
          JOIN main.agent_control_events event ON event.event_id = evidence.task_event_id
          JOIN main.agent_control_task_states task ON task.task_id = evidence.task_id
          ORDER BY evidence.task_id
        `;
        assert.lengthOf(terminalAuthorityBefore, 1);
        assert.deepStrictEqual(yield* writer.sql`PRAGMA foreign_key_check`, []);

        const at = "2026-08-31T11:00:00.000Z";
        const payload = new TextEncoder().encode(
          '{"changedAt":"2026-08-31T11:00:00.000Z","mode":"observe","pausedFromMode":null,"previousMode":"manual","previousPausedFromMode":null,"projectId":"migration-063-project"}',
        );
        const sequence = (yield* writer.sql<{ readonly sequence: number }>`
        INSERT INTO main.agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'migration-063-event', 'project-controller', 'migration-063-project', 1,
          'agentControl.project.mode.changed', ${at}, 'migration-063-command', NULL,
          'migration-063-command', 'human', ${payload}, '{"schemaVersion":1}'
        ) RETURNING sequence
      `)[0]!.sequence;
        yield* writer.sql`
        INSERT INTO main.agent_control_project_states (
          project_id, mode, paused_from_mode, revision, last_event_sequence, updated_at
        ) VALUES ('migration-063-project', 'observe', NULL, 1, ${sequence}, ${at})
      `;
        const eventBefore = yield* writer.sql<Record<string, unknown>>`
        SELECT typeof(payload_json) AS storage, hex(payload_json) AS bytes
        FROM main.agent_control_events WHERE event_id = 'migration-063-event'
      `;
        const stateBefore = yield* writer.sql<Record<string, unknown>>`
        SELECT * FROM main.agent_control_project_states WHERE project_id = 'migration-063-project'
      `;
        const schemaBefore = yield* writer.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
        for (const faultPoint of [
          "before-project-state-rebuild",
          "after-project-state-rebuild",
          "after-run-once-tables",
          "after-run-once-triggers",
        ] satisfies ReadonlyArray<Migration063FaultPoint>) {
          const result = yield* Effect.exit(
            writer.sql.withTransaction(
              makeMigration063((point) =>
                point === faultPoint ? Effect.die(new Error(`injected ${point}`)) : Effect.void,
              ).pipe(Effect.provideService(SqlClient.SqlClient, writer.sql)),
            ),
          );
          assert.isTrue(Exit.isFailure(result), faultPoint);
          assert.deepStrictEqual(
            yield* writer.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
            schemaBefore,
            faultPoint,
          );
          assert.deepStrictEqual(
            yield* writer.sql<Record<string, unknown>>`
            SELECT * FROM main.agent_control_project_states
            WHERE project_id = 'migration-063-project'
          `,
            stateBefore,
            faultPoint,
          );
        }

        yield* writer.sql`CREATE TABLE main.agent_control_run_once_partial (value TEXT)`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              writer.sql.withTransaction(
                makeMigration063().pipe(Effect.provideService(SqlClient.SqlClient, writer.sql)),
              ),
            ),
          ),
        );
        yield* writer.sql`DROP TABLE main.agent_control_run_once_partial`;

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
            Effect.provideService(SqlClient.SqlClient, writer.sql),
          ),
          [[63, "AgentControlRunOnceActivation"] as const],
        );
        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 63 }).pipe(
            Effect.provideService(SqlClient.SqlClient, observer.sql),
          ),
          [],
        );
        assert.deepStrictEqual(
          yield* observer.sql<Record<string, unknown>>`
          SELECT typeof(payload_json) AS storage, hex(payload_json) AS bytes
          FROM main.agent_control_events WHERE event_id = 'migration-063-event'
        `,
          eventBefore,
        );
        assert.deepStrictEqual(
          yield* observer.sql<Record<string, unknown>>`
          SELECT * FROM main.agent_control_project_states
          WHERE project_id = 'migration-063-project'
        `,
          stateBefore,
        );
        assert.deepStrictEqual(
          yield* observer.sql<Record<string, unknown>>`
            SELECT evidence.task_id AS "taskId", evidence.project_id AS "projectId",
              typeof(evidence.finalization_json) AS "evidenceStorage",
              hex(CAST(evidence.finalization_json AS BLOB)) AS "evidenceBytes",
              typeof(event.payload_json) AS "eventStorage",
              hex(CAST(event.payload_json AS BLOB)) AS "eventBytes",
              hex(CAST(task.state_json AS BLOB)) AS "projectionBytes",
              receipt.receipt_id AS "receiptId", receipt.status AS "receiptStatus",
              marker.marker_id AS "markerId",
              marker.marker_fingerprint AS "markerFingerprint"
            FROM main.agent_control_task_verification_finalization_evidence evidence
            JOIN main.agent_control_task_verification_finalization_receipts receipt
              ON receipt.receipt_id = evidence.receipt_id
            JOIN main.agent_control_task_verification_finalization_markers marker
              ON marker.marker_id = evidence.marker_id
            JOIN main.agent_control_events event ON event.event_id = evidence.task_event_id
            JOIN main.agent_control_task_states task ON task.task_id = evidence.task_id
            ORDER BY evidence.task_id
          `,
          terminalAuthorityBefore,
        );

        const taskEventStore = Context.get(
          yield* Layer.buildWithScope(
            Layer.fresh(AgentControlTaskEventStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, observer.sql)),
            ),
            observer.scope,
          ),
          AgentControlTaskEventStore,
        );
        const terminalRow = terminalAuthorityBefore[0]!;
        const taskId = AgentControlTaskId.make(String(terminalRow.taskId));
        const events = yield* taskEventStore.readStream(taskId, 0, 500);
        const terminal = yield* loadRunOnceTerminalAuthority(
          observer.sql,
          ProjectId.make(String(terminalRow.projectId)),
          AgentControlRunOnceId.make("run-once-terminal-binding-production-062"),
          taskId,
          events,
        );
        assert.isNotNull(terminal);
        assert.equal(terminal?.status, "failed");
        assert.equal(terminal?.event.payload.status, "failed");

        const canonical = '{"schemaVersion":1}';
        const fingerprint = "0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72";
        const udf = yield* observer.sql<Record<string, unknown>>`
        SELECT
          t3_run_once_canonical_blob_match(CAST(${canonical} AS BLOB), ${fingerprint}) AS valid,
          t3_run_once_canonical_blob_match(${canonical}, ${fingerprint}) AS textValue,
          t3_run_once_canonical_blob_match(CAST('{ "schemaVersion":1}' AS BLOB), ${fingerprint})
            AS noncanonical,
          t3_run_once_canonical_blob_match(
            CAST('{"schemaVersion":1,"schemaVersion":1}' AS BLOB), ${fingerprint}
          ) AS duplicateKey,
          t3_run_once_canonical_blob_match(x'7b22736368656d6156657273696f6e223a317d00', ${fingerprint})
            AS nulByte,
          t3_run_once_canonical_blob_match(x'ff', ${fingerprint}) AS invalidUtf8
      `;
        assert.deepStrictEqual(udf, [
          {
            valid: 1,
            textValue: 0,
            noncanonical: 0,
            duplicateKey: 0,
            nulByte: 0,
            invalidUtf8: 0,
          },
        ]);

        for (const shadow of [
          "agent_control_run_once_activations",
          "agent_control_run_once_states",
          "agent_control_run_once_step_evidence",
        ]) {
          yield* observer.sql.unsafe(`CREATE TEMP TABLE ${shadow}(shadow TEXT)`).unprepared;
        }
        assert.equal(
          (yield* observer.sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_run_once_states
        `)[0]!.count,
          0,
        );
        assert.deepStrictEqual(yield* observer.sql`PRAGMA main.foreign_key_check`, []);
        assert.equal((yield* observer.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
