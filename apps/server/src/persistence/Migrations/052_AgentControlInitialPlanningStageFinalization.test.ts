import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration052 from "./052_AgentControlInitialPlanningStageFinalization.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-08-02T08:00:00.000Z";

layer("052_AgentControlInitialPlanningStageFinalization", (it) => {
  it.effect(
    "is transactional and idempotent while preserving event rows, triggers, and sequence",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 51 });
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'migration-052-prepared', 'stage-run', 'migration-052-stage', 1,
            'agentControl.stageRun.prepared', ${at}, 'migration-052-prepare', NULL,
            'migration-052-prepare', 'controller', '{}', '{}'
          )
        `;
        yield* sql`
          CREATE TABLE migration_052_trigger_log(value TEXT NOT NULL)
        `;
        yield* sql`
          CREATE TRIGGER migration_052_preserved_event_trigger
          AFTER INSERT ON agent_control_events
          WHEN NEW.command_id = 'migration-052-trigger'
          BEGIN
            INSERT INTO migration_052_trigger_log(value) VALUES (NEW.event_id);
          END
        `;
        const sequenceBefore = yield* sql`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `;

        const rollback = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* Migration052;
              yield* sql.unsafe("CREATE TABLE migration_052_broken(").unprepared;
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(rollback));
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count FROM sqlite_schema
            WHERE name LIKE 'agent_control_initial_planning_finalization_%'
               OR name = 'agent_control_initial_planning_stage_started'
               OR name = 'agent_control_initial_planning_result_evidence'
          `,
          [{ count: 0 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'`,
          sequenceBefore,
        );

        yield* sql.withTransaction(Migration052);
        const schemaCount = (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM sqlite_schema
        `)[0]!.count;
        yield* sql.withTransaction(Migration052);
        assert.equal(
          (yield* sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM sqlite_schema
          `)[0]!.count,
          schemaCount,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT event_id AS "eventId", sequence FROM agent_control_events
            WHERE event_id = 'migration-052-prepared'
          `,
          [{ eventId: "migration-052-prepared", sequence: 1 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'`,
          sequenceBefore,
        );
        assert.deepStrictEqual(
          yield* sql<{ readonly name: string }>`
            SELECT name FROM sqlite_schema
            WHERE type = 'table' AND name IN (
              'agent_control_initial_planning_stage_started',
              'agent_control_initial_planning_result_evidence',
              'agent_control_initial_planning_finalization_receipts',
              'agent_control_initial_planning_finalization_markers'
            ) ORDER BY name
          `,
          [
            { name: "agent_control_initial_planning_finalization_markers" },
            { name: "agent_control_initial_planning_finalization_receipts" },
            { name: "agent_control_initial_planning_result_evidence" },
            { name: "agent_control_initial_planning_stage_started" },
          ],
        );

        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'migration-052-started', 'stage-run', 'migration-052-stage', 2,
            'agentControl.stageRun.planningStarted', ${at}, 'migration-052-trigger', NULL,
            'migration-052-trigger', 'system', '{}', '{}'
          )
        `;
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'migration-052-release', 'stage-run-lease', 'migration-052-lease', 2,
            'agentControl.stageRunLease.releasedAfterPlanning', ${at},
            'migration-052-release-command', NULL, 'migration-052-release-command',
            'system', '{}', '{}'
          )
        `;
        assert.deepStrictEqual(yield* sql`SELECT value FROM migration_052_trigger_log`, [
          { value: "migration-052-started" },
        ]);
        assert.deepStrictEqual(
          yield* sql`
            SELECT name, type FROM pragma_table_info(
              'agent_control_initial_planning_result_evidence'
            ) WHERE name IN (
              'task_revision', 'delivery_revision', 'plan_event_sequence',
              'finalization_fingerprint', 'proposed_plan_json'
            ) ORDER BY name
          `,
          [
            { name: "delivery_revision", type: "INTEGER" },
            { name: "finalization_fingerprint", type: "TEXT" },
            { name: "plan_event_sequence", type: "INTEGER" },
            { name: "proposed_plan_json", type: "TEXT" },
            { name: "task_revision", type: "INTEGER" },
          ],
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      }),
  );
});
