import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration052 from "./052_AgentControlInitialPlanningStageFinalization.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const encodeJson = Schema.encodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const decodeJsonObject = (value: string) => decodeJson(value) as Record<string, unknown>;
const at = "2026-08-02T08:00:00.000Z";
const metadata = '{"schemaVersion":1}';
const preparedPayload = encodeJson({
  projectId: "migration-052-project",
  taskId: "migration-052-task",
  stageRunId: "migration-052-stage",
  attemptId: "migration-052-attempt",
  roleId: "planning",
  stageKind: "planning",
  stageOrdinal: 1,
  attemptOrdinal: 1,
  status: "prepared",
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "1".repeat(64),
  preparedAt: at,
});
const startedPayload = encodeJson({
  projectId: "migration-052-project",
  taskId: "migration-052-task",
  stageRunId: "migration-052-stage",
  attemptId: "migration-052-attempt",
  roleId: "planning",
  stageKind: "planning",
  stageOrdinal: 1,
  attemptOrdinal: 1,
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "1".repeat(64),
  handoffId: "migration-052-handoff",
  handoffFingerprint: "2".repeat(64),
  controlledThreadReservationId: "migration-052-reservation",
  threadId: "migration-052-thread",
  providerDeliveryId: "migration-052-delivery",
  providerInstanceId: "migration-052-provider",
  providerTurnId: "migration-052-turn",
  runtimeMode: "approval-required",
  modelSelectionFingerprint: "3".repeat(64),
  leaseId: "migration-052-lease",
  leaseHolderId: "migration-052-holder",
  fenceToken: 1,
  status: "running",
  startedAt: at,
});
const startedIdentity = decodeJsonObject(startedPayload);
delete startedIdentity.status;
delete startedIdentity.startedAt;
const terminalPayload = (status: "succeeded" | "failed" | "cancelled") =>
  encodeJson({
    ...startedIdentity,
    resultEvidenceId: "migration-052-result",
    finalizedAt: at,
    status,
  });
const leaseReleasedPayload = encodeJson({
  leaseId: "migration-052-lease",
  projectId: "migration-052-project",
  taskId: "migration-052-task",
  stageRunId: "migration-052-stage",
  attemptId: "migration-052-attempt",
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "1".repeat(64),
  holderId: "migration-052-holder",
  fenceToken: 1,
  handoffId: "migration-052-handoff",
  handoffFingerprint: "2".repeat(64),
  controlledThreadReservationId: "migration-052-reservation",
  threadId: "migration-052-thread",
  providerDeliveryId: "migration-052-delivery",
  providerInstanceId: "migration-052-provider",
  providerTurnId: "migration-052-turn",
  runtimeMode: "approval-required",
  modelSelectionFingerprint: "3".repeat(64),
  resultEvidenceId: "migration-052-result",
  stageStatus: "succeeded",
  releasedAt: at,
});
const leaseReservedPayload = encodeJson({
  leaseId: "migration-052-lease",
  projectId: "migration-052-project",
  taskId: "migration-052-task",
  stageRunId: "migration-052-stage",
  attemptId: "migration-052-attempt",
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "1".repeat(64),
  holderId: "migration-052-holder",
  fenceToken: 1,
  acquiredAt: at,
  renewedAt: at,
  expiresAt: "2026-08-02T09:00:00.000Z",
});

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
            'migration-052-prepare', 'controller', ${preparedPayload}, ${metadata}
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
        const orphanReceipt = yield* Effect.exit(
          sql.withTransaction(sql`
            INSERT INTO agent_control_initial_planning_finalization_receipts (
              finalization_command_id, finalization_fingerprint, result_evidence_id,
              handoff_id, outcome, stage_event_id, stage_event_sequence,
              lease_event_id, lease_event_sequence, accepted_at
            ) VALUES (
              'orphan-command', ${"a".repeat(64)}, 'orphan-evidence', 'orphan-handoff',
              'failed', 'orphan-stage-event', 1, 'orphan-lease-event', 2, ${at}
            )
          `),
        );
        assert.isTrue(Exit.isFailure(orphanReceipt));
        const orphanMarker = yield* Effect.exit(
          sql.withTransaction(sql`
            INSERT INTO agent_control_initial_planning_finalization_markers (
              marker_id, marker_fingerprint, finalization_command_id,
              result_evidence_id, handoff_id, committed_at
            ) VALUES (
              'orphan-marker', ${"b".repeat(64)}, 'orphan-command',
              'orphan-evidence', 'orphan-handoff', ${at}
            )
          `),
        );
        assert.isTrue(Exit.isFailure(orphanMarker));
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM agent_control_initial_planning_finalization_receipts)
                AS receipts,
              (SELECT count(*) FROM agent_control_initial_planning_finalization_markers)
                AS markers
          `,
          [{ receipts: 0, markers: 0 }],
        );

        const insertStarted = (input: {
          readonly eventId: string;
          readonly aggregateKind?: string;
          readonly eventType?: string;
          readonly authority?: string;
          readonly commandId?: string;
          readonly streamId?: string;
          readonly streamVersion?: number;
          readonly occurredAt?: string;
          readonly payload?: string | Uint8Array;
          readonly metadata?: string | Uint8Array;
        }) =>
          sql.unsafe(
            `INSERT INTO agent_control_events (
               event_id, aggregate_kind, stream_id, stream_version, event_type,
               occurred_at, command_id, causation_event_id, correlation_id,
               actor_authority, payload_json, metadata_json
             ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)`,
            [
              input.eventId,
              input.aggregateKind ?? "stage-run",
              input.streamId ?? "migration-052-stage",
              input.streamVersion ?? 2,
              input.eventType ?? "agentControl.stageRun.planningStarted",
              input.occurredAt ?? at,
              input.commandId ?? `command-${input.eventId}`,
              input.commandId ?? `command-${input.eventId}`,
              input.authority ?? "system",
              input.payload ?? startedPayload,
              input.metadata ?? metadata,
            ],
          );
        const invalidLifecycleCases = [
          { eventId: "migration-052-invalid-authority", authority: "human" },
          { eventId: "migration-052-invalid-aggregate", aggregateKind: "stage-run-lease" },
          {
            eventId: "migration-052-invalid-type",
            eventType: "agentControl.stageRun.planningSucceeded",
          },
          { eventId: "migration-052-invalid-stream", streamId: "foreign-stage" },
          { eventId: "migration-052-invalid-revision", streamVersion: 3 },
          { eventId: "migration-052-invalid-time", occurredAt: "2026-08-02T08:00:01.000Z" },
          { eventId: "migration-052-empty-payload", payload: "{}" },
          { eventId: "migration-052-empty-metadata", metadata: "{}" },
          {
            eventId: "migration-052-extra-metadata",
            metadata: '{"schemaVersion":1,"extra":true}',
          },
          {
            eventId: "migration-052-duplicate-metadata",
            metadata: '{"schemaVersion":1,"schemaVersion":1}',
          },
          {
            eventId: "migration-052-missing-field",
            payload: encodeJson({ ...decodeJsonObject(startedPayload), handoffId: undefined }),
          },
          {
            eventId: "migration-052-extra-field",
            payload: encodeJson({ ...decodeJsonObject(startedPayload), extra: true }),
          },
          {
            eventId: "migration-052-wrong-status",
            payload: encodeJson({ ...decodeJsonObject(startedPayload), status: "prepared" }),
          },
          {
            eventId: "migration-052-duplicate-key",
            payload: startedPayload.replace(
              '{"projectId":',
              '{"projectId":"duplicate","projectId":',
            ),
          },
          { eventId: "migration-052-noncanonical-json", payload: ` ${startedPayload}` },
          {
            eventId: "migration-052-payload-blob",
            payload: new TextEncoder().encode(startedPayload),
          },
          {
            eventId: "migration-052-metadata-blob",
            metadata: new TextEncoder().encode(metadata),
          },
        ] as const;
        for (const invalid of invalidLifecycleCases) {
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(insertStarted(invalid))),
            invalid.eventId,
          );
        }
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count FROM agent_control_events
            WHERE event_id LIKE 'migration-052-invalid-%'
               OR event_id LIKE 'migration-052-empty-%'
               OR event_id IN (
                 'migration-052-missing-field', 'migration-052-extra-field',
                 'migration-052-wrong-status', 'migration-052-duplicate-key',
                 'migration-052-noncanonical-json', 'migration-052-payload-blob',
                 'migration-052-metadata-blob', 'migration-052-extra-metadata',
                 'migration-052-duplicate-metadata'
               )
          `,
          [{ count: 0 }],
        );
        yield* insertStarted({
          eventId: "migration-052-trigger-event",
          commandId: "migration-052-trigger",
        });
        assert.deepStrictEqual(yield* sql`SELECT value FROM migration_052_trigger_log`, [
          { value: "migration-052-trigger-event" },
        ]);
        for (const [eventType, status] of [
          ["agentControl.stageRun.planningSucceeded", "succeeded"],
          ["agentControl.stageRun.planningFailed", "failed"],
          ["agentControl.stageRun.planningCancelled", "cancelled"],
        ] as const) {
          const exit = yield* Effect.exit(
            sql.unsafe(
              `INSERT INTO agent_control_events (
                 event_id, aggregate_kind, stream_id, stream_version, event_type,
                 occurred_at, command_id, causation_event_id, correlation_id,
                 actor_authority, payload_json, metadata_json
               ) VALUES (?, 'stage-run', 'migration-052-stage', 3, ?, ?, ?, NULL, ?,
                         'human', ?, ?)`,
              [
                `migration-052-terminal-${status}`,
                eventType,
                at,
                `migration-052-terminal-command-${status}`,
                `migration-052-terminal-command-${status}`,
                terminalPayload(status),
                metadata,
              ],
            ),
          );
          assert.isTrue(Exit.isFailure(exit), eventType);
        }
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'migration-052-lease-reserved', 'stage-run-lease', 'migration-052-lease', 1,
            'agentControl.stageRunLease.reserved', ${at}, 'migration-052-lease-reserve', NULL,
            'migration-052-lease-reserve', 'controller', ${leaseReservedPayload}, ${metadata}
          )
        `;
        for (const invalid of [
          { eventId: "migration-052-release-authority", authority: "controller" },
          { eventId: "migration-052-release-empty", authority: "system", payload: "{}" },
          {
            eventId: "migration-052-release-fence",
            authority: "system",
            payload: encodeJson({ ...decodeJsonObject(leaseReleasedPayload), fenceToken: 2 }),
          },
          {
            eventId: "migration-052-release-outcome",
            authority: "system",
            payload: encodeJson({
              ...decodeJsonObject(leaseReleasedPayload),
              stageStatus: "running",
            }),
          },
        ]) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sql.unsafe(
                  `INSERT INTO agent_control_events (
                     event_id, aggregate_kind, stream_id, stream_version, event_type,
                     occurred_at, command_id, causation_event_id, correlation_id,
                     actor_authority, payload_json, metadata_json
                   ) VALUES (?, 'stage-run-lease', 'migration-052-lease', 2,
                     'agentControl.stageRunLease.releasedAfterPlanning', ?, ?, NULL, ?, ?, ?, ?)`,
                  [
                    invalid.eventId,
                    at,
                    `command-${invalid.eventId}`,
                    `command-${invalid.eventId}`,
                    invalid.authority,
                    invalid.payload ?? leaseReleasedPayload,
                    metadata,
                  ],
                ),
              ),
            ),
            invalid.eventId,
          );
        }
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count FROM agent_control_events
            WHERE event_id LIKE 'migration-052-terminal-%'
               OR event_id LIKE 'migration-052-release-%'
          `,
          [{ count: 0 }],
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`
              UPDATE agent_control_events SET actor_authority = 'human'
              WHERE event_id = 'migration-052-trigger-event'
            `),
          ),
        );
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(sql`
              DELETE FROM agent_control_events
              WHERE event_id = 'migration-052-trigger-event'
            `),
          ),
        );
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

        yield* sql`DROP TRIGGER agent_control_initial_planning_stage_event_validate`;
        yield* sql`DROP TRIGGER agent_control_initial_planning_lifecycle_event_no_delete`;
        yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            'migration-053-existing-invalid', 'stage-run', 'migration-053-stage', 2,
            'agentControl.stageRun.planningStarted', ${at}, 'migration-053-command', NULL,
            'migration-053-command', 'human', '{}', '{}'
          )
        `;
        const rejectedUpgrade = yield* Effect.exit(runMigrations({ toMigrationInclusive: 53 }));
        assert.isTrue(Exit.isFailure(rejectedUpgrade));
        yield* sql`
          DELETE FROM agent_control_events WHERE event_id = 'migration-053-existing-invalid'
        `;
        yield* runMigrations({ toMigrationInclusive: 53 });
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count FROM sqlite_schema
            WHERE type = 'trigger' AND name IN (
              'agent_control_initial_planning_stage_event_validate',
              'agent_control_initial_planning_lifecycle_event_no_delete'
            )
          `,
          [{ count: 2 }],
        );
      }),
  );
});
