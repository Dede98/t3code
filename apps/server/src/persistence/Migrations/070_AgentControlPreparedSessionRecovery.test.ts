import type * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import Migration070 from "./070_AgentControlPreparedSessionRecovery.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const decodeArchivedSession = Schema.decodeUnknownSync(
  Schema.fromJsonString(
    Schema.Struct({
      provider_delivery_id: Schema.String,
      session_created_at: Schema.String,
      resume_cursor_json: Schema.String,
    }),
  ),
);
let native: NodeSqlite.DatabaseSync;
it.layer(
  NodeSqliteClient.layerMemory({
    _testHooks: {
      registerFunctions: (database) => {
        native = database;
        NodeSqliteClient.registerNodeSqliteFunctions(database);
      },
    },
  }),
)("prepared session recovery", (it) => {
  it.effect("archives only never-attempted sessions and restores every immutable guard", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 69 });
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`PRAGMA ignore_check_constraints = ON`;
      const before = yield* sql<{
        name: string;
        sql: string;
      }>`SELECT name, sql FROM sqlite_schema WHERE type='trigger' ORDER BY name`;
      const stages = ["initial_planning", "implementation", "verification"] as const;
      const cases = [
        "claimed",
        "retry-wait",
        "attested",
        "attempted",
        "accepted",
        "ambiguous",
        "correlated",
      ];
      for (const stage of stages) {
        const prefix = `agent_control_${stage}`;
        const tables = [
          `${prefix}_deliveries`,
          `${prefix}_session_evidence`,
          `${prefix}_delivery_attestations`,
        ];
        const triggers = before.filter((entry) =>
          tables.some((table) => entry.sql.includes(`ON ${table}`)),
        );
        for (const trigger of triggers)
          yield* sql.unsafe(`DROP TRIGGER ${trigger.name}`).unprepared;
        const columns = yield* sql.unsafe<{
          name: string;
          type: string;
          notnull: number;
          pk: number;
        }>(`PRAGMA table_info(${prefix}_deliveries)`);
        for (const item of cases) {
          const id = `${stage}-${item}`;
          const values = Object.fromEntries(
            columns.map((column) => [
              column.name,
              column.notnull || column.pk
                ? column.type === "INTEGER" ||
                  ["revision", "claim_generation", "attempt_count", "interrupt_requested"].includes(
                    column.name,
                  )
                  ? 1
                  : id
                : null,
            ]),
          );
          Object.assign(values, {
            provider_delivery_id: id,
            thread_id: id,
            handoff_fingerprint: Buffer.from(id).toString("hex").padEnd(64, "0").slice(0, 64),
            model_selection_fingerprint: "a".repeat(64),
            updated_at: "2026-09-06T10:00:00.000Z",
            provider_instance_id: "provider",
            runtime_mode: "approval-required",
            model_selection_json: '{"model":"native"}',

            state:
              item === "attested" || item === "correlated"
                ? "claimed"
                : item === "attempted"
                  ? "delivery-attempted"
                  : item === "accepted"
                    ? "provider-started"
                    : item,
            provider_turn_id: item === "accepted" ? "native-turn" : null,
            provider_session_created_at: item === "correlated" ? "2026-09-06T10:00:00.000Z" : null,
          });
          native
            .prepare(
              `INSERT INTO ${prefix}_deliveries (${columns.map((c) => c.name).join(",")}) VALUES (${columns.map(() => "?").join(",")})`,
            )
            .run(...columns.map((c) => values[c.name] ?? null));
          native
            .prepare(`INSERT INTO ${prefix}_session_evidence VALUES (?,?,?,?,?,?,?,?,?,?)`)
            .run(
              id,
              id,
              "provider",
              "approval-required",
              "/tmp/prepared-session",
              '{"model":"native"}',
              "a".repeat(64),
              "2026-09-06T10:00:00.000Z",
              '{"threadId":"old-native-thread"}',
              "2026-09-06T10:00:00.000Z",
            );
          if (item === "attested")
            native
              .prepare(`INSERT INTO ${prefix}_delivery_attestations VALUES (?,?,?,?,?)`)
              .run(
                id,
                "provider",
                '{"model":"native"}',
                "a".repeat(64),
                "2026-09-06T10:00:00.000Z",
              );
        }
        for (const trigger of triggers) yield* sql.unsafe(trigger.sql).unprepared;
      }
      const rollback = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* Migration070;
            return yield* Effect.fail("rollback migration fixture");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(rollback));
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_schema WHERE name='agent_control_prepared_session_archive'`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name, sql FROM sqlite_schema WHERE type='trigger' ORDER BY name`,
        before,
      );
      yield* runMigrations({ toMigrationInclusive: 70 });
      for (const stage of stages) {
        const rows = yield* sql.unsafe<{ provider_delivery_id: string }>(
          `SELECT provider_delivery_id FROM agent_control_${stage}_session_evidence ORDER BY provider_delivery_id`,
        );
        assert.deepStrictEqual(
          rows.map((row) => row.provider_delivery_id),
          cases
            .filter((item) => item !== "claimed" && item !== "retry-wait")
            .map((item) => `${stage}-${item}`)
            .sort(),
        );
      }
      const archived = yield* sql<{
        stage: string;
        provider_delivery_id: string;
        evidence_json: string;
      }>`SELECT stage, provider_delivery_id, evidence_json FROM agent_control_prepared_session_archive ORDER BY stage, provider_delivery_id`;
      assert.equal(archived.length, 6);
      for (const row of archived) {
        const evidence = decodeArchivedSession(row.evidence_json);
        assert.equal(evidence.provider_delivery_id, row.provider_delivery_id);
        assert.equal(evidence.resume_cursor_json, '{"threadId":"old-native-thread"}');
        assert.equal(evidence.session_created_at, "2026-09-06T10:00:00.000Z");
      }
      assert.deepStrictEqual(
        yield* sql`SELECT name, sql FROM sqlite_schema WHERE type='trigger' AND name NOT LIKE 'agent_control_prepared_session_archive_%' ORDER BY name`,
        before,
      );
      for (const stage of stages) {
        const claimed = yield* sql.unsafe(
          `SELECT state, revision, claim_owner_id, claim_expires_at FROM agent_control_${stage}_deliveries WHERE provider_delivery_id=?`,
          [`${stage}-claimed`],
        );
        assert.deepStrictEqual(claimed, [
          { state: "retry-wait", revision: 2, claim_owner_id: null, claim_expires_at: null },
        ]);
        assert.deepStrictEqual(
          yield* sql.withTransaction(
            sql.unsafe(
              `UPDATE agent_control_${stage}_deliveries SET state='delivery-attempted', revision=revision+1 WHERE provider_delivery_id=? AND state='claimed' AND revision=1 RETURNING provider_delivery_id`,
              [`${stage}-claimed`],
            ),
          ),
          [],
        );
      }
      const insertOldAttestation = sql`INSERT INTO agent_control_initial_planning_delivery_attestations
      VALUES ('initial_planning-claimed','provider','{"model":"native"}',${"a".repeat(64)},'2026-09-06T10:00:00.000Z')`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(sql.withTransaction(insertOldAttestation))));
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM agent_control_initial_planning_delivery_attestations WHERE provider_delivery_id='initial_planning-claimed'`,
        [],
      );
      // Seed the new runtime session; the handoff fixture itself is outside this migration test.
      const validate = before.find(
        (row) => row.name === "agent_control_initial_planning_session_evidence_validate",
      )!;
      yield* sql`DROP TRIGGER agent_control_initial_planning_session_evidence_validate`;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`INSERT INTO agent_control_initial_planning_session_evidence VALUES (
        'initial_planning-claimed','initial_planning-claimed','provider','approval-required',
        '/tmp/prepared-session','{"model":"native"}',${"a".repeat(64)},
        '2026-09-06T10:03:00.000Z','{"threadId":"new-native-thread"}','2026-09-06T10:03:00.000Z')`;
          yield* insertOldAttestation;
        }),
      );
      yield* sql.unsafe(validate.sql).unprepared;
      assert.equal(
        (yield* sql`SELECT * FROM agent_control_initial_planning_delivery_attestations WHERE provider_delivery_id='initial_planning-claimed'`)
          .length,
        1,
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(sql`DELETE FROM agent_control_prepared_session_archive`)),
      );
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            sql`UPDATE agent_control_prepared_session_archive SET stage='invalid'`,
          ),
        ),
      );
    }),
  );
});
