import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalJson, type JsonValue } from "../../agentControl/initialPlanning/eventEvidence.ts";
import {
  TASK_VERIFICATION_FINALIZATION_CANDIDATES_SQL,
  TASK_VERIFICATION_FINALIZATION_PUBLICATION_RECOVERY_SQL,
} from "../../agentControl/task/Layers/AgentControlTaskVerificationFinalizer.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import {
  makeMigration062,
  type Migration062FaultPoint,
} from "./062_AgentControlTaskVerificationFinalization.ts";

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const seedLegacyTask = Effect.fn("seedLegacyTask")(function* (sql: SqlClient.SqlClient) {
  const at = "2026-08-30T08:00:00.000Z";
  const source = {
    projectId: "migration-062-project",
    repositoryNodeId: "repository-062",
    issueNodeId: "issue-062",
    issueNumber: 62,
    issueUrl: "https://example.invalid/issues/62",
  } as const;
  const snapshot = {
    repositoryNodeId: source.repositoryNodeId,
    issueNodeId: source.issueNodeId,
    number: source.issueNumber,
    url: source.issueUrl,
    state: "open",
    title: "Migration 062",
    body: null,
    contentTrust: "untrusted-external",
    updatedAt: at,
    timelineComplete: true,
    ready: true,
    paused: false,
    eligible: true,
    eligibilityReason: "eligible",
  } as const;
  const payload = canonicalJson({
    createdAt: at,
    githubIntakeSequence: 1,
    source,
    sourceGate: "eligible",
    sourceSnapshot: snapshot,
    sourceUpdatedAt: at,
    stage: "intake",
    status: "candidate",
    taskId: "task-062",
  } as unknown as JsonValue);
  const inserted = yield* sql<{ readonly sequence: number }>`
    INSERT INTO main.agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_authority, payload_json, metadata_json
    ) VALUES (
      'task-event-062', 'task', 'task-062', 1, 'agentControl.task.created',
      ${at}, 'task-command-062', NULL, 'task-command-062', 'controller',
      ${payload}, '{"schemaVersion":1}'
    ) RETURNING sequence
  `;
  const sequence = inserted[0]!.sequence;
  const state = canonicalJson({
    createdAt: at,
    githubIntakeSequence: 1,
    revision: 1,
    schemaVersion: 1,
    sequence,
    source,
    sourceGate: "eligible",
    sourceSnapshot: snapshot,
    sourceUpdatedAt: at,
    stage: "intake",
    status: "candidate",
    taskId: "task-062",
    updatedAt: at,
  } as unknown as JsonValue);
  yield* sql`
    INSERT INTO main.agent_control_task_states (
      task_id, project_id, repository_node_id, issue_node_id, issue_number,
      issue_url, status, source_gate, stage, source_updated_at,
      github_intake_sequence, state_json, created_at, updated_at,
      revision, last_event_sequence
    ) VALUES (
      'task-062', 'migration-062-project', 'repository-062', 'issue-062', 62,
      'https://example.invalid/issues/62', 'candidate', 'eligible', 'intake', ${at},
      1, ${state}, ${at}, ${at}, 1, ${sequence}
    )
  `;
  return { payload, state };
});

it.live("installs task Verification finalization atomically and preserves legacy bytes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-task-finalization-062-" });
      const database = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      const observer = yield* openDatabase(path.join(directory, "upgrade.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
      );
      const legacy = yield* seedLegacyTask(database.sql);
      const eventBefore = yield* database.sql<Record<string, unknown>>`
        SELECT typeof(payload_json) AS storage, hex(payload_json) AS bytes
        FROM main.agent_control_events WHERE event_id = 'task-event-062'
      `;
      const stateBefore = yield* database.sql<Record<string, unknown>>`
        SELECT typeof(state_json) AS storage, hex(state_json) AS bytes
        FROM main.agent_control_task_states WHERE task_id = 'task-062'
      `;
      const schemaBefore = yield* database.sql<Record<string, unknown>>`
        SELECT type, name, tbl_name AS "tableName", sql
        FROM main.sqlite_schema ORDER BY type, name
      `;
      for (const faultPoint of [
        "before-events-rebuild",
        "after-events-rebuild",
        "after-task-states-rebuild",
        "after-companions",
        "after-install",
      ] satisfies ReadonlyArray<Migration062FaultPoint>) {
        const result = yield* Effect.exit(
          database.sql.withTransaction(
            makeMigration062(faultPoint).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sql),
            ),
          ),
        );
        assert.isTrue(Exit.isFailure(result), faultPoint);
        assert.deepStrictEqual(
          yield* database.sql<Record<string, unknown>>`
            SELECT type, name, tbl_name AS "tableName", sql
            FROM main.sqlite_schema ORDER BY type, name
          `,
          schemaBefore,
          faultPoint,
        );
      }
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sql),
        ),
        [[62, "AgentControlTaskVerificationFinalization"] as const],
      );
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql<Record<string, unknown>>`
          SELECT typeof(payload_json) AS storage, hex(payload_json) AS bytes
          FROM main.agent_control_events WHERE event_id = 'task-event-062'
        `,
        eventBefore,
      );
      assert.deepStrictEqual(
        yield* observer.sql<Record<string, unknown>>`
          SELECT typeof(state_json) AS storage, hex(state_json) AS bytes
          FROM main.agent_control_task_states WHERE task_id = 'task-062'
        `,
        stateBefore,
      );
      assert.equal(
        Buffer.from(String(eventBefore[0]!.bytes), "hex").toString("utf8"),
        legacy.payload,
      );
      assert.equal(
        Buffer.from(String(stateBefore[0]!.bytes), "hex").toString("utf8"),
        legacy.state,
      );
      const eventSchema = yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table' AND name = 'agent_control_events'
      `;
      assert.include(eventSchema[0]!.sql, "agentControl.task.finalizedAfterVerification");
      const taskSchema = yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table' AND name = 'agent_control_task_states'
      `;
      assert.include(taskSchema[0]!.sql, "stage IN ('intake', 'verification')");
      const publicationSchema = yield* observer.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table'
          AND name = 'agent_control_task_verification_finalization_publications'
      `;
      assert.include(publicationSchema[0]!.sql, "claim_fence INTEGER NOT NULL");
      assert.include(publicationSchema[0]!.sql, "lease_expires_at TEXT");
      assert.include(
        publicationSchema[0]!.sql,
        "lease_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', lease_expires_at), 0",
      );
      assert.include(publicationSchema[0]!.sql, "lease_expires_at > claimed_at");
      assert.include(publicationSchema[0]!.sql, "completed_at < lease_expires_at");
      const validationTriggers = Object.fromEntries(
        (yield* observer.sql<{ readonly name: string; readonly sql: string }>`
            SELECT name, sql FROM main.sqlite_schema
            WHERE type = 'trigger' AND name IN (
              'agent_control_task_verification_finalization_event_validate',
              'agent_control_task_verification_finalization_evidence_validate',
              'agent_control_task_verification_finalization_marker_validate',
              'agent_control_task_verification_finalization_publication_insert_validate',
              'agent_control_task_verification_finalization_publication_update_validate'
            ) ORDER BY name
          `).map((row) => [row.name, row.sql] as const),
      );
      assert.lengthOf(Object.keys(validationTriggers), 5);
      const eventValidation =
        validationTriggers.agent_control_task_verification_finalization_event_validate!;
      const evidenceValidation =
        validationTriggers.agent_control_task_verification_finalization_evidence_validate!;
      const markerValidation =
        validationTriggers.agent_control_task_verification_finalization_marker_validate!;
      assert.isBelow(
        eventValidation.indexOf("t3_task_verification_finalization_payload_storage"),
        eventValidation.indexOf("json_extract"),
      );
      assert.include(eventValidation, "typeof(t3_task_verification_finalization_payload_storage");
      assert.isBelow(
        evidenceValidation.indexOf("t3_task_verification_finalization_document_storage"),
        evidenceValidation.indexOf("json_extract"),
      );
      assert.include(
        evidenceValidation,
        "typeof(t3_task_verification_finalization_document_storage",
      );
      assert.include(markerValidation, "t3_task_verification_finalization_marker_match");
      assert.include(
        validationTriggers.agent_control_task_verification_finalization_publication_insert_validate!,
        "publication is inconsistent",
      );
      assert.include(
        validationTriggers.agent_control_task_verification_finalization_publication_update_validate!,
        "OLD.status = 'pending' AND NEW.status = 'claimed'",
      );
      assert.include(
        validationTriggers.agent_control_task_verification_finalization_publication_update_validate!,
        "OLD.publication_owner_id IS NEW.publication_owner_id",
      );
      assert.include(
        validationTriggers.agent_control_task_verification_finalization_publication_update_validate!,
        "NEW.claim_fence = OLD.claim_fence + 1",
      );
      assert.include(
        validationTriggers.agent_control_task_verification_finalization_publication_update_validate!,
        "NEW.claimed_at >= OLD.lease_expires_at",
      );
      const unboundPublication = yield* Effect.exit(observer.sql`
        INSERT INTO main.agent_control_task_verification_finalization_publications (
          handoff_id, marker_id, task_finalization_evidence_id, task_id,
          task_event_id, task_event_stream_version, publication_owner_id,
          status, revision, claim_fence, created_at, claimed_at, lease_expires_at, completed_at
        ) VALUES (
          'unbound-handoff', 'unbound-marker', 'unbound-evidence', 'unbound-task',
          'unbound-event', 2, NULL, 'pending', 1, 0,
          '2026-08-30T10:00:00.000Z', NULL, NULL, NULL
        )
      `);
      assert.isTrue(Exit.isFailure(unboundPublication));
      assert.deepStrictEqual(
        yield* observer.sql`
          SELECT handoff_id FROM main.agent_control_task_verification_finalization_publications
        `,
        [],
      );
      assert.deepStrictEqual(yield* observer.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* observer.sql`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
      const plan = yield* observer.sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${TASK_VERIFICATION_FINALIZATION_CANDIDATES_SQL}`,
        ["", 64],
      );
      assert.isTrue(
        plan.some(({ detail }) => detail.includes("verification_finalization_markers")),
      );
      assert.isTrue(plan.some(({ detail }) => detail.includes("verification_marker_id")));
      assert.isFalse(plan.some(({ detail }) => detail.includes("USE TEMP B-TREE")));
      const publicationPlan = yield* observer.sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${TASK_VERIFICATION_FINALIZATION_PUBLICATION_RECOVERY_SQL}`,
        ["", 64],
      );
      assert.isTrue(
        publicationPlan.some(({ detail }) =>
          detail.includes("task_verification_finalization_markers"),
        ),
      );
      assert.isTrue(
        publicationPlan.some(({ detail }) =>
          detail.includes("task_verification_finalization_publications"),
        ),
      );
      assert.isFalse(publicationPlan.some(({ detail }) => detail.includes("USE TEMP B-TREE")));
      for (const shadow of [
        "agent_control_verification_finalization_markers",
        "agent_control_verification_finalization_evidence",
        "agent_control_verification_finalization_receipts",
        "agent_control_task_verification_finalization_markers",
        "agent_control_task_verification_finalization_publications",
      ]) {
        yield* observer.sql.unsafe(`CREATE TEMP TABLE ${shadow}(handoff_id TEXT)`);
      }
      assert.deepStrictEqual(
        yield* observer.sql.unsafe(TASK_VERIFICATION_FINALIZATION_CANDIDATES_SQL, ["", 64]),
        [],
      );
      assert.deepStrictEqual(
        yield* observer.sql.unsafe(TASK_VERIFICATION_FINALIZATION_PUBLICATION_RECOVERY_SQL, [
          "",
          64,
        ]),
        [],
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails closed on a partial schema and succeeds after explicit repair", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-task-finalization-062-partial-",
      });
      const database = yield* openDatabase(path.join(directory, "partial.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
      );
      yield* database.sql`
        CREATE TABLE main.agent_control_task_verification_finalization_evidence(id TEXT)
      `;
      const failed = yield* Effect.exit(
        database.sql.withTransaction(
          makeMigration062().pipe(Effect.provideService(SqlClient.SqlClient, database.sql)),
        ),
      );
      assert.isTrue(Exit.isFailure(failed));
      const partial = yield* database.sql<{ readonly sql: string }>`
        SELECT sql FROM main.sqlite_schema
        WHERE type = 'table'
          AND name = 'agent_control_task_verification_finalization_evidence'
      `;
      assert.include(partial[0]!.sql, "id TEXT");
      yield* database.sql`
        DROP TABLE main.agent_control_task_verification_finalization_evidence
      `;
      assert.deepStrictEqual(
        yield* database.sql`
          SELECT name FROM main.sqlite_schema
          WHERE name = 'agent_control_task_verification_finalization_evidence'
        `,
        [],
      );
      const retry = yield* openDatabase(path.join(directory, "partial.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(retry.scope, Exit.void));
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
          Effect.provideService(SqlClient.SqlClient, retry.sql),
        ),
        [[62, "AgentControlTaskVerificationFinalization"] as const],
      );
      assert.deepStrictEqual(yield* retry.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* retry.sql`PRAGMA integrity_check`, [{ integrity_check: "ok" }]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("installs on a fresh database through the production migration loader", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-task-finalization-062-fresh-",
      });
      const database = yield* openDatabase(path.join(directory, "fresh.sqlite"));
      yield* Effect.addFinalizer(() => Scope.close(database.scope, Exit.void));
      const executed = yield* runMigrations({ toMigrationInclusive: 62 }).pipe(
        Effect.provideService(SqlClient.SqlClient, database.sql),
      );
      assert.deepStrictEqual(executed.at(-1), [62, "AgentControlTaskVerificationFinalization"]);
      assert.deepStrictEqual(yield* database.sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* database.sql`PRAGMA integrity_check`, [
        { integrity_check: "ok" },
      ]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
