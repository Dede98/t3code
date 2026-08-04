import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration051 from "./051_AgentControlInitialPlanningHandoff.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const legacyLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const deliveryLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-30T12:00:00.000Z";

layer("051_AgentControlInitialPlanningHandoff", (it) => {
  it.effect(
    "is transactional, idempotent, preserves prior schema/data/sequence, and has no projection FKs",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 50 });
        yield* sql`
          CREATE TABLE migration_051_preserved(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL
          )
        `;
        yield* sql`
          CREATE UNIQUE INDEX migration_051_preserved_value
          ON migration_051_preserved(value)
        `;
        yield* sql`
          CREATE TRIGGER migration_051_preserved_trigger
          AFTER INSERT ON migration_051_preserved
          BEGIN SELECT NEW.id; END
        `;
        yield* sql`INSERT INTO migration_051_preserved(value) VALUES ('before')`;
        const sequenceBefore = yield* sql`
          SELECT name, seq FROM sqlite_sequence ORDER BY name
        `;

        const rollback = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* Migration051;
              yield* sql.unsafe("CREATE TABLE migration_051_broken(").unprepared;
            }),
          ),
        );
        assert.equal(Exit.isFailure(rollback), true);
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count
            FROM sqlite_schema
            WHERE name LIKE 'agent_control_initial_planning_%'
          `,
          [{ count: 0 }],
        );
        assert.deepStrictEqual(yield* sql`SELECT id, value FROM migration_051_preserved`, [
          { id: 1, value: "before" },
        ]);
        assert.deepStrictEqual(
          yield* sql`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
          sequenceBefore,
        );

        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 52 }), [
          [51, "AgentControlInitialPlanningHandoff"],
          [52, "AgentControlInitialPlanningStageFinalization"],
        ]);
        const schemaCount = (yield* sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM sqlite_schema
          `)[0]!.count;
        yield* sql.withTransaction(Migration051);
        assert.equal(
          (yield* sql<{ readonly count: number }>`
              SELECT count(*) AS count FROM sqlite_schema
            `)[0]!.count,
          schemaCount,
        );
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 52 }), []);
        assert.deepStrictEqual(
          yield* sql<{ readonly name: string }>`
            SELECT name
            FROM sqlite_schema
            WHERE type = 'table'
              AND name IN (
                'agent_control_initial_planning_legacy_materializations',
                'agent_control_initial_planning_handoff_intents',
                'agent_control_initial_planning_handoff_receipts',
                'agent_control_initial_planning_handoff_accepted',
                'agent_control_initial_planning_turn_accepted',
                'agent_control_initial_planning_deliveries',
                'agent_control_initial_planning_session_evidence'
              )
            ORDER BY name
          `,
          [
            { name: "agent_control_initial_planning_deliveries" },
            { name: "agent_control_initial_planning_handoff_accepted" },
            { name: "agent_control_initial_planning_handoff_intents" },
            { name: "agent_control_initial_planning_handoff_receipts" },
            { name: "agent_control_initial_planning_legacy_materializations" },
            { name: "agent_control_initial_planning_session_evidence" },
            { name: "agent_control_initial_planning_turn_accepted" },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count
            FROM sqlite_schema schema,
              pragma_foreign_key_list(schema.name) foreignKey
            WHERE schema.name LIKE 'agent_control_initial_planning_%'
              AND foreignKey."table" LIKE 'projection_%'
          `,
          [{ count: 0 }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
          sequenceBefore,
        );
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM sqlite_schema
               WHERE type = 'index'
                 AND name = 'migration_051_preserved_value') AS indexes,
              (SELECT count(*) FROM sqlite_schema
               WHERE type = 'trigger'
                 AND name = 'migration_051_preserved_trigger') AS triggers
          `,
          [{ indexes: 1, triggers: 1 }],
        );
      }),
  );
});

legacyLayer("051_AgentControlInitialPlanningHandoff legacy cutover", (it) => {
  it.effect("classifies pre-cutover materializations once and rejects every retrofit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 50 });
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`
        DROP TRIGGER agent_control_controlled_thread_coordinator_accepted_validate
      `;
      yield* sql.withTransaction(
        sql`
          INSERT INTO agent_control_controlled_thread_materialization_accepted(
            coordinator_command_id, finalization_owner_id,
            coordinator_command_fingerprint,
            controlled_thread_reservation_id, thread_id,
            materialization_command_id, materialization_command_fingerprint,
            orchestration_result_sequence, accepted_at
          ) VALUES (
            'legacy-coordinator',
            '00000000-0000-0000-0000-000000000051',
            ${"a".repeat(64)}, 'legacy-reservation',
            't3-auto-reserved-thread-legacy', 'legacy-materialization',
            ${"b".repeat(64)}, 1, ${at}
          )
        `,
      );
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* sql.withTransaction(Migration051);
      assert.deepStrictEqual(
        yield* sql`
          SELECT coordinator_command_id AS "coordinatorCommandId",
            controlled_thread_reservation_id AS "reservationId",
            thread_id AS "threadId"
          FROM agent_control_initial_planning_legacy_materializations
        `,
        [
          {
            coordinatorCommandId: "legacy-coordinator",
            reservationId: "legacy-reservation",
            threadId: "t3-auto-reserved-thread-legacy",
          },
        ],
      );
      for (const statement of [
        sql`
          UPDATE agent_control_initial_planning_legacy_materializations
          SET materialization_accepted_at = materialization_accepted_at
        `,
        sql`DELETE FROM agent_control_initial_planning_legacy_materializations`,
        sql`
          INSERT INTO agent_control_initial_planning_legacy_materializations(
            coordinator_command_id, controlled_thread_reservation_id,
            thread_id, materialization_accepted_at
          ) VALUES (
            'legacy-coordinator', 'legacy-reservation',
            't3-auto-reserved-thread-legacy', ${at}
          )
        `,
      ]) {
        assert.equal((yield* Effect.exit(statement))._tag, "Failure");
      }
      const retrofit = yield* Effect.exit(
        sql.withTransaction(
          sql`
            INSERT INTO agent_control_initial_planning_handoff_intents(
              handoff_id, handoff_fingerprint, coordinator_command_id,
              coordinator_command_fingerprint, materialization_command_id,
              materialization_command_fingerprint, project_id,
              controlled_thread_reservation_id, thread_id, task_id,
              task_revision, github_intake_sequence,
              source_identity_fingerprint, stage_run_id, attempt_id, role_id,
              stage_kind, stage_ordinal, attempt_ordinal, lease_id,
              lease_holder_id, fence_token, worktree_reservation_id,
              worktree_path, provider_instance_id, runtime_mode,
              model_selection_json, planning_role, template_version,
              prompt_text, turn_request_command_id, message_id,
              provider_delivery_id, created_at, planning_deadline_at,
              accepted_marker_handoff_id
            ) VALUES (
              'retrofit-handoff', ${"c".repeat(64)}, 'legacy-coordinator',
              ${"a".repeat(64)}, 'legacy-materialization', ${"b".repeat(64)},
              'project', 'legacy-reservation',
              't3-auto-reserved-thread-legacy', 'task', 1, 1,
              ${"d".repeat(64)}, 'stage', 'attempt', 'planning', 'planning',
              1, 1, 'lease', 'holder', 1, 'worktree', '/worktree',
              'provider', 'approval-required',
              '{"instanceId":"provider","model":"model"}', 'planner',
              'agent-control-initial-planning-prompt-v1', 'planning only',
              'turn-command', 'message', 'delivery', ${at},
              '2026-07-30T12:30:00.000Z', 'retrofit-handoff'
            )
          `,
        ),
      );
      assert.equal(Exit.isFailure(retrofit), true);
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*) FROM agent_control_initial_planning_handoff_intents)
              AS intents,
            (SELECT count(*) FROM agent_control_initial_planning_deliveries)
              AS deliveries
        `,
        [{ intents: 0, deliveries: 0 }],
      );
    }),
  );
});

deliveryLayer("051_AgentControlInitialPlanningHandoff delivery state", (it) => {
  it.effect("enforces total numeric types and the delivery CAS state machine", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_delivery_insert_validate`;
      const insertDelivery = (input: {
        readonly suffix: string;
        readonly state: string;
        readonly revision: unknown;
        readonly owner: string | null;
        readonly generation: unknown;
        readonly expiresAt: string | null;
        readonly attempts: unknown;
        readonly castNumerics?: boolean;
      }) =>
        sql.unsafe(
          `
            INSERT INTO agent_control_initial_planning_deliveries(
              provider_delivery_id, handoff_id, handoff_fingerprint,
              controlled_thread_reservation_id, thread_id,
              turn_request_command_id, message_id, provider_instance_id,
              state, revision, claim_owner_id, claim_generation,
              claim_expires_at, attempt_count, next_attempt_at,
              planning_deadline_at, provider_turn_id, provider_accepted_at,
              provider_session_created_at, provider_resume_cursor_json,
              terminal_at, last_error_code, interrupt_requested, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
              ${input.castNumerics ? "CAST(? AS INTEGER)" : "?"},
              ?, ${input.castNumerics ? "CAST(? AS INTEGER)" : "?"}, ?,
              ${input.castNumerics ? "CAST(? AS INTEGER)" : "?"},
              NULL, ?,
              NULL, NULL, NULL, NULL, NULL, NULL, 0, ?)
          `,
          [
            `delivery-${input.suffix}`,
            `handoff-${input.suffix}`,
            input.suffix.length.toString(16).padStart(64, "0"),
            `reservation-${input.suffix}`,
            `thread-${input.suffix}`,
            `command-${input.suffix}`,
            `message-${input.suffix}`,
            "provider",
            input.state,
            input.revision,
            input.owner,
            input.generation,
            input.expiresAt,
            input.attempts,
            "2026-07-30T13:00:00.000Z",
            at,
          ],
        );

      for (const [suffix, value] of [
        ["text", "0"],
        ["real", 0.5],
        ["blob", new Uint8Array([0])],
        ["null", null],
      ] as const) {
        assert.equal(
          (yield* Effect.exit(
            sql.withTransaction(
              insertDelivery({
                suffix,
                state: "pending",
                revision: value,
                owner: null,
                generation: 0,
                expiresAt: null,
                attempts: 0,
              }),
            ),
          ))._tag,
          "Failure",
          suffix,
        );
      }

      yield* sql.withTransaction(
        insertDelivery({
          suffix: "cas",
          state: "turn-accepted",
          revision: 0,
          owner: null,
          generation: 0,
          expiresAt: null,
          attempts: 0,
          castNumerics: true,
        }),
      );
      yield* sql`DROP TRIGGER agent_control_initial_planning_turn_accepted_validate`;
      yield* sql`
        INSERT INTO agent_control_initial_planning_turn_accepted(
          handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
          thread_id, turn_request_command_id, message_id, message_event_id,
          message_event_sequence, turn_request_event_id,
          turn_request_event_sequence, message_event_envelope_json,
          turn_request_event_envelope_json, event_evidence_digest,
          receipt_authority, accepted_at
        ) VALUES (
          'handoff-cas', ${"3".padStart(64, "0")}, 'reservation-cas',
          'thread-cas', 'command-cas', 'message-cas', 'message-event-cas',
          CAST(1 AS INTEGER), 'turn-event-cas', CAST(2 AS INTEGER),
          '{"sequence":1}', '{"sequence":2}', ${"4".repeat(64)},
          'agent-control', ${at}
        )
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_deliveries
        SET state = 'claimed', revision = 1, claim_owner_id = 'owner',
          claim_generation = 1, claim_expires_at = '2026-07-30T12:02:00.000Z',
          attempt_count = 1
        WHERE handoff_id = 'handoff-cas'
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_deliveries
        SET state = 'delivery-attempted', revision = 2,
          provider_session_created_at = ${at},
          provider_resume_cursor_json = 'null'
        WHERE handoff_id = 'handoff-cas'
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_deliveries
        SET state = 'provider-started', revision = 3,
          claim_owner_id = NULL, claim_expires_at = NULL,
          provider_turn_id = 'turn', provider_accepted_at = ${at}
        WHERE handoff_id = 'handoff-cas'
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_deliveries
        SET state = 'interrupt-requested', revision = 4, interrupt_requested = 1
        WHERE handoff_id = 'handoff-cas'
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_deliveries
        SET state = 'completed', revision = 5, terminal_at = ${at}
        WHERE handoff_id = 'handoff-cas'
      `;
      assert.deepStrictEqual(
        yield* sql`
          SELECT state, revision, claim_generation AS "claimGeneration",
            attempt_count AS "attemptCount", provider_turn_id AS "providerTurnId"
          FROM agent_control_initial_planning_deliveries
          WHERE handoff_id = 'handoff-cas'
        `,
        [
          {
            state: "completed",
            revision: 5,
            claimGeneration: 1,
            attemptCount: 1,
            providerTurnId: "turn",
          },
        ],
      );
      assert.equal(
        (yield* Effect.exit(
          sql`
              UPDATE agent_control_initial_planning_deliveries
              SET revision = 6
              WHERE handoff_id = 'handoff-cas'
            `,
        ))._tag,
        "Failure",
      );
    }),
  );

  it.effect("retains every delivery row regardless of state or provider evidence", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql`PRAGMA ignore_check_constraints = ON`;
      yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_delivery_insert_validate`;
      yield* sql`
        DROP TRIGGER IF EXISTS agent_control_initial_planning_session_evidence_validate
      `;
      yield* sql`
        DROP TRIGGER IF EXISTS agent_control_initial_planning_delivery_attestation_validate
      `;

      const states = [
        "pending",
        "turn-accepted",
        "claimed",
        "delivery-attempted",
        "provider-started",
        "interrupt-requested",
        "retry-wait",
        "ambiguous",
        "completed",
        "failed",
        "interrupted",
      ] as const;

      for (const [stateIndex, state] of states.entries()) {
        for (const [evidenceIndex, evidence] of (
          ["without-evidence", "with-evidence"] as const
        ).entries()) {
          const suffix = `${state}-${evidence}`;
          yield* sql.withTransaction(
            sql`
              INSERT INTO agent_control_initial_planning_deliveries(
              provider_delivery_id, handoff_id, handoff_fingerprint,
              controlled_thread_reservation_id, thread_id,
              turn_request_command_id, message_id, provider_instance_id,
              state, revision, claim_owner_id, claim_generation,
              claim_expires_at, attempt_count, next_attempt_at,
              planning_deadline_at, provider_turn_id, provider_accepted_at,
              provider_session_created_at, provider_resume_cursor_json,
              terminal_at, last_error_code, interrupt_requested, updated_at
            ) VALUES (
              ${`delivery-delete-${suffix}`}, ${`handoff-delete-${suffix}`},
              ${(stateIndex * 2 + evidenceIndex + 100).toString(16).padStart(64, "0")},
              ${`reservation-delete-${suffix}`}, ${`thread-delete-${suffix}`},
              ${`command-delete-${suffix}`}, ${`message-delete-${suffix}`},
              'provider', ${state}, 0, NULL, 0, NULL, 0, NULL, ${at},
              ${evidence === "with-evidence" ? `turn-${suffix}` : null},
              ${evidence === "with-evidence" ? at : null},
              ${evidence === "with-evidence" ? at : null},
              ${evidence === "with-evidence" ? "null" : null},
              NULL, NULL, 0, ${at}
              )
            `,
          );
          if (evidence === "with-evidence") {
            yield* sql.withTransaction(
              Effect.all(
                [
                  sql`
                    INSERT INTO agent_control_initial_planning_session_evidence(
                      provider_delivery_id, thread_id, provider_instance_id,
                      runtime_mode, cwd, model_selection_json,
                      model_selection_fingerprint, session_created_at,
                      resume_cursor_json, recorded_at
                    ) VALUES (
                      ${`delivery-delete-${suffix}`}, ${`thread-delete-${suffix}`},
                      'provider', 'approval-required', ${`/tmp/${suffix}`},
                      '{"instanceId":"provider","model":"native"}',
                      ${(stateIndex * 2 + evidenceIndex + 200).toString(16).padStart(64, "0")},
                      ${at}, 'null', ${at}
                    )
                  `,
                  sql`
                    INSERT INTO agent_control_initial_planning_delivery_attestations(
                      provider_delivery_id, provider_instance_id,
                      model_selection_json, model_selection_fingerprint, recorded_at
                    ) VALUES (
                      ${`delivery-delete-${suffix}`}, 'provider',
                      '{"instanceId":"provider","model":"native"}',
                      ${(stateIndex * 2 + evidenceIndex + 300).toString(16).padStart(64, "0")},
                      ${at}
                    )
                  `,
                ],
                { concurrency: 1, discard: true },
              ),
            );
          }

          const deletion = yield* Effect.exit(
            sql.withTransaction(
              sql`
                DELETE FROM agent_control_initial_planning_deliveries
                WHERE provider_delivery_id = ${`delivery-delete-${suffix}`}
              `,
            ),
          );
          assert.equal(deletion._tag, "Failure", suffix);
        }
      }

      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*)
             FROM agent_control_initial_planning_deliveries
             WHERE provider_delivery_id LIKE 'delivery-delete-%') AS deliveries,
            (SELECT count(*)
             FROM agent_control_initial_planning_session_evidence
             WHERE provider_delivery_id LIKE 'delivery-delete-%') AS sessionEvidence,
            (SELECT count(*)
             FROM agent_control_initial_planning_delivery_attestations
             WHERE provider_delivery_id LIKE 'delivery-delete-%') AS deliveryAttestations
        `,
        [
          {
            deliveries: states.length * 2,
            sessionEvidence: states.length,
            deliveryAttestations: states.length,
          },
        ],
      );
    }),
  );
});
