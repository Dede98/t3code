import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const at = "2026-07-24T10:00:00.000Z";
const encodeJson = Schema.encodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

const reservationPayload = (input: {
  readonly reservationId: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly targetGenerationId?: string;
}) => ({
  reservationId: input.reservationId,
  projectId: input.projectId,
  taskId: input.taskId,
  taskRevision: 1,
  githubIntakeSequence: 1,
  sourceIdentityFingerprint: "a".repeat(64),
  stageRunId: input.stageRunId,
  attemptId: input.attemptId,
  leaseId: input.leaseId,
  fenceToken: 1,
  repository: {
    repositoryNodeId: "repository-node",
    nameWithOwner: "owner/repository",
    canonicalKey: "github.com/owner/repository",
    remoteName: "origin",
    remoteUrl: "github.com/owner/repository",
    defaultRemoteRef: "refs/remotes/origin/main",
    commonDirDevice: 1,
    commonDirInode: 2,
  },
  repositoryWorkspace: "/tmp/repository",
  repositoryCommonDir: "/tmp/repository/.git",
  baseRef: "origin/main",
  baseCommitSha: "b".repeat(40),
  branchName: "t3auto/issue-1-migration",
  internalWorktreePath: "/tmp/worktrees/reservation-generation",
  targetGenerationId: input.targetGenerationId ?? "c".repeat(64),
  worktreeRootDevice: 1,
  worktreeRootInode: 2,
  worktreeParentDevice: 1,
  worktreeParentInode: 3,
  reservedAt: at,
});

layer("046_AgentControlWorktreeReservationFoundation", (it) => {
  it.effect("preserves data and commit-closes immutable worktree event metadata", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-before-046', 'stage-run-lease', 'lease-before-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-before-046',
          NULL, 'command-before-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        INSERT INTO agent_control_command_receipts (
          command_id, command_fingerprint, authority, aggregate_kind, aggregate_id,
          status, result_sequence, result_stream_version, event_created,
          accepted_at, error_code
        ) VALUES (
          'command-before-046', 'fingerprint-before-046', 'controller',
          'stage-run-lease', 'lease-before-046', 'accepted', 1, 1, 1, ${at}, NULL
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      yield* runMigrations({ toMigrationInclusive: 46 });

      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_events
          WHERE event_id = 'event-before-046'
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_command_receipts
          WHERE command_id = 'command-before-046'
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_reservation_states')
        `)[0]!.count,
        0,
      );
      assert.isAbove(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_stream_catalog')
        `)[0]!.count,
        0,
      );
      assert.isAbove(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM pragma_foreign_key_list('agent_control_worktree_event_envelopes')
        `)[0]!.count,
        0,
      );

      const identity = {
        reservationId: "reservation-valid-046",
        eventId: "event-valid-046",
        projectId: "project-valid-046",
        taskId: "task-valid-046",
        stageRunId: "stage-valid-046",
        attemptId: "attempt-valid-046",
        leaseId: "lease-valid-046",
      };
      const insertCatalog = (value: typeof identity) => sql`
        INSERT INTO agent_control_worktree_stream_catalog (
          reservation_id, project_id, task_id, stage_run_id, attempt_id,
          lease_id, fence_token, created_at, initial_event_id, initial_stream_version
        ) VALUES (
          ${value.reservationId}, ${value.projectId}, ${value.taskId},
          ${value.stageRunId}, ${value.attemptId}, ${value.leaseId}, 1, ${at},
          ${value.eventId}, 1
        )
      `;
      const insertEnvelope = (value: typeof identity) => sql`
        INSERT INTO agent_control_worktree_event_envelopes (
          event_id, reservation_id, stream_version, event_type,
          project_id, task_id, stage_run_id, attempt_id, lease_id,
          fence_token, created_at
        ) VALUES (
          ${value.eventId}, ${value.reservationId}, 1,
          'agentControl.worktree.reserved', ${value.projectId}, ${value.taskId},
          ${value.stageRunId}, ${value.attemptId}, ${value.leaseId}, 1, ${at}
        )
      `;
      const insertEvent = (value: typeof identity, payloadJson: string) => sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          ${value.eventId}, 'worktree-reservation', ${value.reservationId}, 1,
          'agentControl.worktree.reserved', ${at}, ${`command-${value.eventId}`},
          NULL, ${`command-${value.eventId}`}, 'controller', ${payloadJson},
          '{"schemaVersion":1}'
        )
      `;
      const validPayload = yield* encodeJson(reservationPayload(identity));

      const catalogOnly = yield* Effect.exit(
        sql.withTransaction(insertCatalog({ ...identity, reservationId: "catalog-only" })),
      );
      assert.equal(catalogOnly._tag, "Failure");

      const envelopeOnlyIdentity = {
        ...identity,
        reservationId: "envelope-only",
        eventId: "event-envelope-only",
      };
      const envelopeOnly = yield* Effect.exit(
        sql.withTransaction(insertEnvelope(envelopeOnlyIdentity)),
      );
      assert.equal(envelopeOnly._tag, "Failure");

      const withoutEventIdentity = {
        ...identity,
        reservationId: "without-event",
        eventId: "event-without-event",
      };
      const withoutEvent = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertCatalog(withoutEventIdentity);
            yield* insertEnvelope(withoutEventIdentity);
          }),
        ),
      );
      assert.equal(withoutEvent._tag, "Failure");

      assert.equal(
        (yield* Effect.exit(
          insertEvent(
            { ...identity, reservationId: "event-only", eventId: "event-only" },
            validPayload,
          ),
        ))._tag,
        "Failure",
      );

      const incompleteIdentity = {
        ...identity,
        reservationId: "incomplete-payload",
        eventId: "event-incomplete-payload",
      };
      const incomplete = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertCatalog(incompleteIdentity);
            yield* insertEnvelope(incompleteIdentity);
            yield* insertEvent(
              incompleteIdentity,
              yield* encodeJson({
                reservationId: incompleteIdentity.reservationId,
                projectId: incompleteIdentity.projectId,
              }),
            );
          }),
        ),
      );
      assert.equal(incomplete._tag, "Failure");
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM agent_control_worktree_stream_catalog
          WHERE reservation_id = ${incompleteIdentity.reservationId}
        `)[0]!.count,
        0,
      );

      const mismatchIdentity = {
        ...identity,
        reservationId: "identity-mismatch",
        eventId: "event-identity-mismatch",
      };
      const mismatch = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* insertCatalog(mismatchIdentity);
            yield* sql`
              INSERT INTO agent_control_worktree_event_envelopes (
                event_id, reservation_id, stream_version, event_type,
                project_id, task_id, stage_run_id, attempt_id, lease_id,
                fence_token, created_at
              ) VALUES (
                ${mismatchIdentity.eventId}, ${mismatchIdentity.reservationId}, 1,
                'agentControl.worktree.reserved', 'wrong-project',
                ${mismatchIdentity.taskId}, ${mismatchIdentity.stageRunId},
                ${mismatchIdentity.attemptId}, ${mismatchIdentity.leaseId}, 1, ${at}
              )
            `;
          }),
        ),
      );
      assert.equal(mismatch._tag, "Failure");

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertCatalog(identity);
          yield* insertEnvelope(identity);
          yield* insertEvent(identity, validPayload);
        }),
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count
          FROM agent_control_worktree_stream_catalog AS catalog
          JOIN agent_control_worktree_event_envelopes AS envelope
            ON envelope.event_id = catalog.initial_event_id
          JOIN agent_control_events AS event ON event.event_id = envelope.event_id
          WHERE catalog.reservation_id = ${identity.reservationId}
        `)[0]!.count,
        1,
      );
      assert.equal(
        (yield* Effect.exit(sql`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE event_id = ${identity.eventId}
        `))._tag,
        "Failure",
      );
      assert.equal(
        (yield* Effect.exit(sql`
          DELETE FROM agent_control_events WHERE event_id = ${identity.eventId}
        `))._tag,
        "Failure",
      );

      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'foreign-mutable-046', 'task', 'foreign-task-046', 1,
          'agentControl.task.created', ${at}, 'foreign-command-046',
          NULL, 'foreign-command-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      yield* sql`
        UPDATE agent_control_events SET payload_json = '{"preserved":true}'
        WHERE event_id = 'foreign-mutable-046'
      `;
      yield* sql`DELETE FROM agent_control_events WHERE event_id = 'foreign-mutable-046'`;

      const schemaObjectCount = (yield* sql<{ readonly count: number }>`
        SELECT COUNT(*) AS count FROM sqlite_master
        WHERE name LIKE 'agent_control_worktree_%'
      `)[0]!.count;
      yield* runMigrations({ toMigrationInclusive: 46 });
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE name LIKE 'agent_control_worktree_%'
        `)[0]!.count,
        schemaObjectCount,
      );
    }),
  );
});

const rollbackLayer = it.layer(Layer.mergeAll(NodeSqliteClient.layerMemory()));

rollbackLayer("046_AgentControlWorktreeReservationFoundation rollback", (it) => {
  it.effect("rolls back the complete migration when later DDL conflicts", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* runMigrations({ toMigrationInclusive: 45 });
      yield* sql`
        INSERT INTO agent_control_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type,
          occurred_at, command_id, causation_event_id, correlation_id,
          actor_authority, payload_json, metadata_json
        ) VALUES (
          'event-rollback-046', 'stage-run-lease', 'lease-rollback-046', 1,
          'agentControl.stageRunLease.reserved', ${at}, 'command-rollback-046',
          NULL, 'command-rollback-046', 'controller', '{}', '{"schemaVersion":1}'
        )
      `;
      const sequenceBefore = (yield* sql<{ readonly sequence: number }>`
        SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
      `)[0]!.sequence;
      yield* sql`
        CREATE TABLE agent_control_worktree_reservation_states (
          sentinel TEXT PRIMARY KEY
        )
      `;
      yield* sql`
        INSERT INTO agent_control_worktree_reservation_states (sentinel)
        VALUES ('must-survive')
      `;

      assert.equal(
        (yield* Effect.exit(runMigrations({ toMigrationInclusive: 46 })))._tag,
        "Failure",
      );
      assert.deepStrictEqual(
        yield* sql`
          SELECT event_id, aggregate_kind, stream_id
          FROM agent_control_events WHERE event_id = 'event-rollback-046'
        `,
        [
          {
            event_id: "event-rollback-046",
            aggregate_kind: "stage-run-lease",
            stream_id: "lease-rollback-046",
          },
        ],
      );
      assert.equal(
        (yield* sql<{ readonly sequence: number }>`
          SELECT seq AS sequence FROM sqlite_sequence WHERE name = 'agent_control_events'
        `)[0]!.sequence,
        sequenceBefore,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT sentinel FROM agent_control_worktree_reservation_states`,
        [{ sentinel: "must-survive" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM sqlite_master
          WHERE type = 'table'
            AND name IN (
              'agent_control_worktree_controller_operations',
              'agent_control_worktree_stream_catalog',
              'agent_control_worktree_event_envelopes',
              'agent_control_worktree_target_claims'
            )
        `)[0]!.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT COUNT(*) AS count FROM effect_sql_migrations WHERE migration_id = 46
        `)[0]!.count,
        0,
      );
    }),
  );
});
