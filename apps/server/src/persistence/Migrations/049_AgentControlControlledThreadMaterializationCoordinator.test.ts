import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import Migration049 from "./049_AgentControlControlledThreadMaterializationCoordinator.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const legacyValidationLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));
const at = "2026-07-27T10:00:00.000Z";
const encodeJson = Schema.encodeUnknownEffect(Schema.UnknownFromJsonString);
const encodeJsonSync = Schema.encodeUnknownSync(Schema.UnknownFromJsonString);

const seedPreparedReservation = Effect.fn("seedPreparedReservationBeforeMigration049")(function* (
  suffix = "",
) {
  const sql = yield* SqlClient.SqlClient;
  const payload = {
    controlledThreadReservationId: `controlled-thread-reservation-before-049${suffix}`,
    threadId: `t3-auto-reserved-thread-before-049${suffix}`,
    projectId: `project-before-049${suffix}`,
    taskId: `task-before-049${suffix}`,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint: "a".repeat(64),
    stageRunId: `stage-before-049${suffix}`,
    attemptId: `attempt-before-049${suffix}`,
    roleId: "planning",
    stageKind: "planning",
    stageOrdinal: 1,
    attemptOrdinal: 1,
    leaseId: `lease-before-049${suffix}`,
    fenceToken: 1,
    worktreeReservationId: `worktree-before-049${suffix}`,
    status: "prepared",
    preparedAt: at,
  } as const;
  const payloadJson = yield* encodeJson(payload);
  const worktreeEventId = `worktree-event-before-049${suffix}`;
  const worktreePayloadJson = yield* encodeJson({
    reservationId: payload.worktreeReservationId,
    projectId: payload.projectId,
    taskId: payload.taskId,
    taskRevision: payload.taskRevision,
    githubIntakeSequence: payload.githubIntakeSequence,
    sourceIdentityFingerprint: payload.sourceIdentityFingerprint,
    stageRunId: payload.stageRunId,
    attemptId: payload.attemptId,
    leaseId: payload.leaseId,
    fenceToken: payload.fenceToken,
    repository: {
      repositoryNodeId: "repository-before-049",
      nameWithOwner: "owner/repository-before-049",
      canonicalKey: "github.com/owner/repository-before-049",
      remoteName: "origin",
      remoteUrl: "github.com/owner/repository-before-049",
      defaultRemoteRef: "refs/remotes/origin/main",
      commonDirDevice: 1,
      commonDirInode: 2,
    },
    repositoryWorkspace: "/tmp/repository-before-049",
    repositoryCommonDir: "/tmp/repository-before-049/.git",
    baseRef: "origin/main",
    baseCommitSha: "b".repeat(40),
    branchName: "t3auto/migration-before-049",
    internalWorktreePath: "/tmp/worktree-before-049",
    targetGenerationId: "c".repeat(64),
    worktreeRootDevice: 1,
    worktreeRootInode: 2,
    worktreeParentDevice: 1,
    worktreeParentInode: 3,
    reservedAt: at,
  });
  const rows = yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
          INSERT INTO agent_control_worktree_stream_catalog (
            reservation_id, project_id, task_id, stage_run_id, attempt_id,
            lease_id, fence_token, created_at, initial_event_id,
            initial_stream_version
          ) VALUES (
            ${payload.worktreeReservationId}, ${payload.projectId},
            ${payload.taskId}, ${payload.stageRunId}, ${payload.attemptId},
            ${payload.leaseId}, ${payload.fenceToken}, ${at},
            ${worktreeEventId}, 1
          )
        `;
      yield* sql`
          INSERT INTO agent_control_worktree_event_envelopes (
            event_id, reservation_id, stream_version, event_type, project_id,
            task_id, stage_run_id, attempt_id, lease_id, fence_token, created_at
          ) VALUES (
            ${worktreeEventId}, ${payload.worktreeReservationId}, 1,
            'agentControl.worktree.reserved', ${payload.projectId},
            ${payload.taskId}, ${payload.stageRunId}, ${payload.attemptId},
            ${payload.leaseId}, ${payload.fenceToken}, ${at}
          )
        `;
      yield* sql`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            ${worktreeEventId}, 'worktree-reservation',
            ${payload.worktreeReservationId}, 1,
            'agentControl.worktree.reserved', ${at},
            ${`worktree-command-before-049${suffix}`}, NULL,
            ${`worktree-command-before-049${suffix}`}, 'controller',
            ${worktreePayloadJson}, '{"schemaVersion":1}'
          )
        `;
      yield* sql`
          INSERT INTO agent_control_controlled_thread_stream_catalog (
            controlled_thread_reservation_id, event_id, stream_version,
            command_id, event_type, thread_id, project_id, task_id,
            task_revision, github_intake_sequence, source_identity_fingerprint,
            stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
            attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
            prepared_at
          ) VALUES (
            ${payload.controlledThreadReservationId}, ${`event-before-049${suffix}`}, 1,
            ${`command-before-049${suffix}`},
            'agentControl.controlledThreadReservation.prepared',
            ${payload.threadId}, ${payload.projectId}, ${payload.taskId},
            ${payload.taskRevision}, ${payload.githubIntakeSequence},
            ${payload.sourceIdentityFingerprint}, ${payload.stageRunId},
            ${payload.attemptId}, ${payload.roleId}, ${payload.stageKind},
            ${payload.stageOrdinal}, ${payload.attemptOrdinal}, ${payload.leaseId},
            ${payload.fenceToken}, ${payload.worktreeReservationId},
            ${payload.preparedAt}
          )
        `;
      const inserted = yield* sql<{ readonly sequence: number }>`
          INSERT INTO agent_control_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_authority, payload_json, metadata_json
          ) VALUES (
            ${`event-before-049${suffix}`}, 'controlled-thread-reservation',
            ${payload.controlledThreadReservationId}, 1,
            'agentControl.controlledThreadReservation.prepared', ${at},
            ${`command-before-049${suffix}`}, NULL,
            ${`command-before-049${suffix}`}, 'controller',
            ${payloadJson}, '{"schemaVersion":1}'
          )
          RETURNING sequence
        `;
      const sequence = inserted[0]!.sequence;
      const stateJson = yield* encodeJson({
        schemaVersion: 1,
        ...payload,
        revision: 1,
        sequence,
      });
      yield* sql`
          INSERT INTO agent_control_controlled_thread_reservation_states (
            controlled_thread_reservation_id, thread_id, project_id, task_id,
            task_revision, github_intake_sequence, source_identity_fingerprint,
            stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
            attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
            status, revision, last_event_sequence, prepared_at, state_json
          ) VALUES (
            ${payload.controlledThreadReservationId}, ${payload.threadId},
            ${payload.projectId}, ${payload.taskId}, ${payload.taskRevision},
            ${payload.githubIntakeSequence}, ${payload.sourceIdentityFingerprint},
            ${payload.stageRunId}, ${payload.attemptId}, ${payload.roleId},
            ${payload.stageKind}, ${payload.stageOrdinal}, ${payload.attemptOrdinal},
            ${payload.leaseId}, ${payload.fenceToken},
            ${payload.worktreeReservationId}, 'prepared', 1, ${sequence},
            ${payload.preparedAt},
            ${stateJson}
          )
        `;
      return inserted;
    }),
  );
  return {
    sequence: rows[0]!.sequence,
    controlledThreadReservationId: payload.controlledThreadReservationId,
    payload,
  };
});

layer("049_AgentControlControlledThreadMaterializationCoordinator", (it) => {
  it.effect("preserves prepared data, sequences, triggers, indices, and is idempotent", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 48 });
      const seeded = yield* seedPreparedReservation();
      yield* sql`
        CREATE TRIGGER test_049_dependent_trigger
        AFTER INSERT ON agent_control_events
        WHEN NEW.aggregate_kind = 'task'
        BEGIN
          SELECT 1;
        END
      `;
      const worktreeIndices = yield* sql<{ readonly name: string }>`
        SELECT name
        FROM sqlite_schema
        WHERE type = 'index'
          AND name LIKE 'idx_agent_control_worktree_%'
        ORDER BY name
      `;

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* Migration049;
          assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
          assert.deepStrictEqual(
            yield* sql`
              SELECT schema.name, foreignKey."table" AS parentTable
              FROM sqlite_schema AS schema,
                pragma_foreign_key_list(schema.name) AS foreignKey
              WHERE foreignKey."table" LIKE '%_old_049'
            `,
            [],
          );
        }),
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT status, revision, last_event_sequence AS lastEventSequence
          FROM agent_control_controlled_thread_reservation_states
          WHERE controlled_thread_reservation_id =
            ${seeded.controlledThreadReservationId}
        `,
        [{ status: "prepared", revision: 1, lastEventSequence: seeded.sequence }],
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        seeded.sequence,
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name FROM sqlite_schema
          WHERE type = 'trigger' AND name = 'test_049_dependent_trigger'
        `,
        [{ name: "test_049_dependent_trigger" }],
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly name: string }>`
          SELECT name
          FROM sqlite_schema
          WHERE type = 'index'
            AND name LIKE 'idx_agent_control_worktree_%'
          ORDER BY name
        `,
        worktreeIndices,
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count
          FROM agent_control_worktree_stream_catalog AS catalog
          JOIN agent_control_worktree_event_envelopes AS envelope
            ON envelope.event_id = catalog.initial_event_id
          JOIN agent_control_events AS event
            ON event.event_id = envelope.event_id
          WHERE catalog.reservation_id = 'worktree-before-049'
        `,
        [{ count: 1 }],
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*) FROM pragma_foreign_key_list(
              'agent_control_controlled_thread_reservation_states'
            )) AS reservationProjectionForeignKeys,
            (SELECT count(*) FROM pragma_foreign_key_list(
              'projection_threads'
            )) AS threadProjectionForeignKeys,
            (SELECT count(*) FROM sqlite_schema
             WHERE type = 'table'
               AND name LIKE
                 'agent_control_controlled_thread_materialization_%') AS evidenceTables
        `,
        [
          {
            reservationProjectionForeignKeys: 0,
            threadProjectionForeignKeys: 0,
            evidenceTables: 3,
          },
        ],
      );
      const schemaCount = (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM sqlite_schema
        `)[0]!.count;
      yield* sql.withTransaction(Migration049);
      assert.equal(
        (yield* sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM sqlite_schema
          `)[0]!.count,
        schemaCount,
      );
      const firstRunnerPass = yield* runMigrations({ toMigrationInclusive: 49 });
      assert.deepStrictEqual(
        firstRunnerPass.map(([id]) => id),
        [49],
      );
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 49 }), []);
    }),
  );

  it.effect("rejects non-total reservation event and coordinator intent JSON", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 48 });
      const seeded = yield* seedPreparedReservation("-json-negative");
      yield* sql.withTransaction(Migration049);
      const materializingPayload = {
        ...seeded.payload,
        status: "materializing",
        coordinatorCommandId: "coordinator-json-negative",
        coordinatorCommandFingerprint: "b".repeat(64),
        materializingTransitionCommandId: "materializing-json-negative",
        materializationCommandId: "materialization-json-negative",
        materializationCommandFingerprint: "c".repeat(64),
        leaseHolderId: "holder-json-negative",
        materializingAt: at,
      } as const;
      const eventPayloads = [
        {
          name: "extra-key",
          json: encodeJsonSync({ ...materializingPayload, extra: true }),
        },
        {
          name: "duplicate-root-key",
          json: encodeJsonSync(materializingPayload).replace(/}$/, ',"status":"materializing"}'),
        },
        {
          name: "missing-key",
          json: encodeJsonSync(
            Object.fromEntries(
              Object.entries(materializingPayload).filter(([key]) => key !== "leaseHolderId"),
            ),
          ),
        },
        {
          name: "null",
          json: encodeJsonSync({ ...materializingPayload, leaseHolderId: null }),
        },
        {
          name: "wrong-type",
          json: encodeJsonSync({ ...materializingPayload, fenceToken: "1" }),
        },
        {
          name: "unknown-status",
          json: encodeJsonSync({ ...materializingPayload, status: "unknown" }),
        },
      ] as const;

      for (const testCase of eventPayloads) {
        const failed = yield* Effect.exit(
          sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`
                INSERT INTO agent_control_controlled_thread_stream_catalog (
                  controlled_thread_reservation_id, event_id, stream_version,
                  command_id, event_type, thread_id, project_id, task_id,
                  task_revision, github_intake_sequence, source_identity_fingerprint,
                  stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
                  attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
                  prepared_at, coordinator_command_id,
                  coordinator_command_fingerprint,
                  materializing_transition_command_id,
                  materialization_command_id, materialization_command_fingerprint,
                  lease_holder_id, materializing_at
                ) VALUES (
                  ${seeded.payload.controlledThreadReservationId},
                  ${`event-${testCase.name}`}, 2,
                  ${materializingPayload.materializingTransitionCommandId},
                  'agentControl.controlledThreadReservation.materializing',
                  ${seeded.payload.threadId}, ${seeded.payload.projectId},
                  ${seeded.payload.taskId}, ${seeded.payload.taskRevision},
                  ${seeded.payload.githubIntakeSequence},
                  ${seeded.payload.sourceIdentityFingerprint},
                  ${seeded.payload.stageRunId}, ${seeded.payload.attemptId},
                  ${seeded.payload.roleId}, ${seeded.payload.stageKind},
                  ${seeded.payload.stageOrdinal}, ${seeded.payload.attemptOrdinal},
                  ${seeded.payload.leaseId}, ${seeded.payload.fenceToken},
                  ${seeded.payload.worktreeReservationId}, ${seeded.payload.preparedAt},
                  ${materializingPayload.coordinatorCommandId},
                  ${materializingPayload.coordinatorCommandFingerprint},
                  ${materializingPayload.materializingTransitionCommandId},
                  ${materializingPayload.materializationCommandId},
                  ${materializingPayload.materializationCommandFingerprint},
                  ${materializingPayload.leaseHolderId},
                  ${materializingPayload.materializingAt}
                )
              `;
              yield* sql`
                INSERT INTO agent_control_events (
                  event_id, aggregate_kind, stream_id, stream_version, event_type,
                  occurred_at, command_id, causation_event_id, correlation_id,
                  actor_authority, payload_json, metadata_json
                ) VALUES (
                  ${`event-${testCase.name}`}, 'controlled-thread-reservation',
                  ${seeded.payload.controlledThreadReservationId}, 2,
                  'agentControl.controlledThreadReservation.materializing', ${at},
                  ${materializingPayload.materializingTransitionCommandId}, NULL,
                  ${materializingPayload.coordinatorCommandId}, 'controller',
                  ${testCase.json}, '{"schemaVersion":1}'
                )
              `;
            }),
          ),
        );
        assert.equal(failed._tag, "Failure", testCase.name);
      }

      const preparedState = {
        schemaVersion: 1,
        ...seeded.payload,
        revision: 1,
        sequence: seeded.sequence,
      } as const;
      const projectionStates = [
        {
          name: "projection-extra-key",
          json: encodeJsonSync({ ...preparedState, extra: true }),
        },
        {
          name: "projection-duplicate-root-key",
          json: encodeJsonSync(preparedState).replace(/}$/, ',"status":"prepared"}'),
        },
        {
          name: "projection-missing-key",
          json: encodeJsonSync(
            Object.fromEntries(
              Object.entries(preparedState).filter(([key]) => key !== "attemptOrdinal"),
            ),
          ),
        },
        {
          name: "projection-null",
          json: encodeJsonSync({ ...preparedState, leaseId: null }),
        },
        {
          name: "projection-wrong-type",
          json: encodeJsonSync({ ...preparedState, revision: "1" }),
        },
        {
          name: "projection-unknown-status",
          json: encodeJsonSync({ ...preparedState, status: "unknown" }),
        },
        {
          name: "projection-relational-conflict",
          json: encodeJsonSync({ ...preparedState, taskId: "noncanonical-task" }),
        },
      ] as const;
      for (const testCase of projectionStates) {
        const failed = yield* Effect.exit(
          sql`
            UPDATE agent_control_controlled_thread_reservation_states
            SET state_json = ${testCase.json}
            WHERE controlled_thread_reservation_id =
              ${seeded.controlledThreadReservationId}
          `,
        );
        assert.equal(failed._tag, "Failure", testCase.name);
      }

      const validModel =
        '{"instanceId":"provider","model":"model","options":[{"id":"reasoningEffort","value":"high"}]}';
      const validBinding = encodeJsonSync({
        taskId: seeded.payload.taskId,
        stageRunId: seeded.payload.stageRunId,
        attemptId: seeded.payload.attemptId,
        roleId: seeded.payload.roleId,
        controlState: "controlled",
      });
      const intentJsonCases = [
        {
          name: "model-extra-key",
          model: '{"instanceId":"provider","model":"model","extra":true}',
          binding: validBinding,
        },
        {
          name: "model-duplicate-root",
          model: '{"instanceId":"provider","instanceId":"other","model":"model"}',
          binding: validBinding,
        },
        {
          name: "model-duplicate-nested",
          model:
            '{"instanceId":"provider","model":"model","options":[{"id":"x","id":"y","value":"high"}]}',
          binding: validBinding,
        },
        {
          name: "model-missing-key",
          model: '{"instanceId":"provider"}',
          binding: validBinding,
        },
        {
          name: "model-null",
          model: '{"instanceId":"provider","model":null}',
          binding: validBinding,
        },
        {
          name: "model-wrong-type",
          model: '{"instanceId":"provider","model":1}',
          binding: validBinding,
        },
        {
          name: "model-incomplete-options",
          model: '{"instanceId":"provider","model":"model","options":[{"id":"x"}]}',
          binding: validBinding,
        },
        {
          name: "binding-extra-key",
          model: validModel,
          binding: validBinding.replace(/}$/, ',"extra":true}'),
        },
        {
          name: "binding-relational-conflict",
          model: validModel,
          binding: validBinding.replace(seeded.payload.taskId, "different-task"),
        },
      ] as const;
      for (const testCase of intentJsonCases) {
        const failed = yield* Effect.exit(
          sql`
            INSERT INTO agent_control_controlled_thread_materialization_intents (
              coordinator_command_id, request_fingerprint,
              coordinator_command_fingerprint, policy_binding_fingerprint,
              runtime_observation_fingerprint, project_id,
              controlled_thread_reservation_id, thread_id, task_id, task_revision,
              github_intake_sequence, source_identity_fingerprint, stage_run_id,
              attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
              lease_id, lease_holder_id, fence_token, worktree_reservation_id,
              materializing_transition_command_id, bound_transition_command_id,
              materialization_command_id, materialization_command_fingerprint,
              title, model_selection_json, runtime_mode, interaction_mode, branch,
              worktree_path, binding_json, materializing_event_id,
              materializing_event_sequence, bound_event_id, bound_event_sequence,
              orchestration_result_sequence, materializing_at, materialized_at,
              bound_at, accepted_at, accepted_marker_command_id
            ) VALUES (
              ${`coordinator-${testCase.name}`}, ${"a".repeat(64)},
              ${"b".repeat(64)}, ${"c".repeat(64)}, ${"d".repeat(64)},
              ${seeded.payload.projectId},
              ${`${seeded.payload.controlledThreadReservationId}-${testCase.name}`},
              ${`${seeded.payload.threadId}-${testCase.name}`},
              ${seeded.payload.taskId}, 1, 1, ${seeded.payload.sourceIdentityFingerprint},
              ${seeded.payload.stageRunId}, ${seeded.payload.attemptId}, 'planning',
              'planning', 1, 1, ${seeded.payload.leaseId}, 'holder', 1,
              ${seeded.payload.worktreeReservationId}, ${`begin-${testCase.name}`},
              ${`bound-${testCase.name}`}, ${`materialize-${testCase.name}`},
              ${"e".repeat(64)}, 'Title', ${testCase.model}, 'approval-required',
              'plan', 'branch', '/tmp/worktree', ${testCase.binding},
              ${`materializing-event-${testCase.name}`}, 2,
              ${`bound-event-${testCase.name}`}, 3, 2, ${at}, ${at}, ${at}, ${at},
              ${`coordinator-${testCase.name}`}
            )
          `,
        );
        assert.equal(failed._tag, "Failure", testCase.name);
      }
    }),
  );
});

rollbackLayer("049_AgentControlControlledThreadMaterializationCoordinator rollback", (it) => {
  it.effect("rolls the complete rebuild back on a late schema failure and remains retryable", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 48 });
      const seeded = yield* seedPreparedReservation();
      yield* sql`
        CREATE TRIGGER
          agent_control_controlled_thread_materialization_accepted_no_delete
        BEFORE INSERT ON orchestration_events
        BEGIN
          SELECT 1;
        END
      `;
      const before = (yield* sql<{ readonly sql: string }>`
          SELECT sql FROM sqlite_schema
          WHERE type = 'table' AND name = 'agent_control_events'
        `)[0]!.sql;
      const failed = yield* Effect.exit(sql.withTransaction(Migration049));
      assert.equal(failed._tag, "Failure");
      assert.deepStrictEqual(
        yield* sql`
          SELECT
            (SELECT count(*) FROM sqlite_schema
             WHERE type = 'table'
               AND name =
                 'agent_control_controlled_thread_materialization_accepted')
              AS acceptedTable,
            (SELECT count(*) FROM agent_control_events
             WHERE event_id = 'event-before-049') AS preservedEvents,
            (SELECT count(*)
             FROM agent_control_controlled_thread_reservation_states
             WHERE controlled_thread_reservation_id =
               ${seeded.controlledThreadReservationId}) AS preservedProjection
        `,
        [{ acceptedTable: 0, preservedEvents: 1, preservedProjection: 1 }],
      );
      assert.equal(
        (yield* sql<{ readonly sql: string }>`
            SELECT sql FROM sqlite_schema
            WHERE type = 'table' AND name = 'agent_control_events'
          `)[0]!.sql,
        before,
      );
      assert.equal(
        (yield* sql<{ readonly seq: number }>`
          SELECT seq FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.seq,
        seeded.sequence,
      );

      yield* sql`
        DROP TRIGGER
          agent_control_controlled_thread_materialization_accepted_no_delete
      `;
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count FROM sqlite_schema
          WHERE name =
            'agent_control_controlled_thread_materialization_accepted_no_delete'
        `,
        [{ count: 0 }],
      );
      yield* sql.withTransaction(Migration049);
      yield* sql.withTransaction(Migration049);
      assert.deepStrictEqual(
        yield* sql`
          SELECT count(*) AS count FROM sqlite_schema
          WHERE type = 'table'
            AND name =
              'agent_control_controlled_thread_materialization_accepted'
        `,
        [{ count: 1 }],
      );
    }),
  );
});

legacyValidationLayer(
  "049_AgentControlControlledThreadMaterializationCoordinator legacy JSON validation",
  (it) => {
    it.effect("rejects non-total 047/048 JSON while rolling the rebuild back", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 48 });
        assert.deepStrictEqual(
          yield* sql`
            SELECT count(*) AS count FROM sqlite_schema
            WHERE name =
              'agent_control_controlled_thread_materialization_accepted'
          `,
          [{ count: 0 }],
        );
        yield* seedPreparedReservation("-legacy-json");
        yield* sql`DROP TRIGGER agent_control_controlled_thread_event_no_update`;
        yield* sql`
          UPDATE agent_control_events
          SET payload_json = json_set(payload_json, '$.extra', 1)
          WHERE event_id = 'event-before-049-legacy-json'
        `;
        assert.equal((yield* Effect.exit(sql.withTransaction(Migration049)))._tag, "Failure");
        assert.deepStrictEqual(
          yield* sql`
            SELECT
              (SELECT count(*) FROM sqlite_schema
               WHERE name =
                 'agent_control_controlled_thread_materialization_accepted')
                AS acceptedTable,
              json_type(payload_json, '$.extra') AS extraType
            FROM agent_control_events
            WHERE event_id = 'event-before-049-legacy-json'
          `,
          [{ acceptedTable: 0, extraType: "integer" }],
        );
      }),
    );
  },
);
