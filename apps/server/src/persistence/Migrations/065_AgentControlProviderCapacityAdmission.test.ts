import * as NodeServices from "@effect/platform-node/NodeServices";
import { ModelSelection, ProviderInstanceId, ProjectId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalJson, sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";
import {
  providerAdmissionId,
  providerAdmissionUsageEvidence,
  type ProviderAdmissionRequest,
} from "../../agentControl/providerAdmission/model.ts";
import {
  PROVIDER_ADMISSION_OLDEST_ELIGIBLE_SQL,
  ProviderAdmissionStoreLive,
} from "../../agentControl/providerAdmission/Layers/ProviderAdmissionStore.ts";
import { ProviderAdmissionStore } from "../../agentControl/providerAdmission/Services/ProviderAdmissionStore.ts";
import { canonicalProviderModelSelectionEvidence } from "../../provider/Services/ProviderAdapter.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";
import {
  EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS,
  makeMigration065,
  PROVIDER_ADMISSION_SCHEMA_OBJECTS,
} from "./065_AgentControlProviderCapacityAdmission.ts";

const at = "2026-09-06T08:00:00.000Z";
const later = "2026-09-06T08:02:00.000Z";
const afterExpiry = "2026-09-06T08:03:00.000Z";
const afterTakeover = "2026-09-06T08:05:00.000Z";

const openDatabase = (filename: string) =>
  Effect.gen(function* () {
    const scope = yield* Scope.make("sequential");
    const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(context, SqlClient.SqlClient);
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA journal_mode = WAL`, [{ journal_mode: "wal" }]);
    return { scope, sql } as const;
  });

const request = (suffix: string, providerInstanceId = "codex-a"): ProviderAdmissionRequest => {
  const modelSelection: ModelSelection = {
    instanceId: ProviderInstanceId.make(providerInstanceId),
    model: "gpt-5.6",
  };
  const model = canonicalProviderModelSelectionEvidence(modelSelection);
  return {
    stage: "implementation",
    projectId: String(ProjectId.make(`project-${suffix}`)),
    taskId: `task-${suffix}`,
    stageRunId: `stage-${suffix}`,
    attemptId: `attempt-${suffix}`,
    handoffId: `handoff-${suffix}`,
    providerDeliveryId: `delivery-${suffix}`,
    threadId: ThreadId.make(`thread-${suffix}`),
    providerInstanceId: ProviderInstanceId.make(providerInstanceId),
    stageLeaseId: `lease-${suffix}`,
    stageLeaseHolderId: `holder-${suffix}`,
    stageFenceToken: 1,
    modelSelection,
    modelSelectionJson: model.modelSelectionJson,
    modelSelectionFingerprint: model.modelSelectionFingerprint,
    requestedAt: at,
  };
};

const intentDocument = (admissionId: string, value: ProviderAdmissionRequest) => ({
  admissionId,
  attemptId: value.attemptId,
  handoffId: value.handoffId,
  modelSelectionFingerprint: value.modelSelectionFingerprint,
  projectId: value.projectId,
  providerDeliveryId: value.providerDeliveryId,
  providerInstanceId: String(value.providerInstanceId),
  requestedAt: value.requestedAt,
  schemaVersion: 1,
  stage: value.stage,
  stageFenceToken: value.stageFenceToken,
  stageLeaseHolderId: value.stageLeaseHolderId,
  stageLeaseId: value.stageLeaseId,
  stageRunId: value.stageRunId,
  taskId: value.taskId,
  threadId: String(value.threadId),
});

it.live("installs Migration 065 atomically and exposes the indexed authority in WAL", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-admission-065-" });
      const filename = path.join(directory, "authority.sqlite");
      const writer = yield* openDatabase(filename);
      const observer = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(observer.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(writer.scope, Exit.void));

      yield* writer.sql.unsafe(
        "CREATE TEMP TABLE agent_control_provider_admission_current(shadow TEXT)",
      ).unprepared;
      yield* writer.sql.unsafe("ATTACH DATABASE ':memory:' AS shadow").unprepared;
      yield* writer.sql.unsafe(
        "CREATE TABLE shadow.agent_control_provider_admission_current(shadow TEXT)",
      ).unprepared;

      const migrations = yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
        Effect.provideService(SqlClient.SqlClient, writer.sql),
      );
      assert.deepStrictEqual(migrations.at(-1), [65, "AgentControlProviderCapacityAdmission"]);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, observer.sql),
        ),
        [],
      );
      const objects = yield* observer.sql<{ readonly name: string }>`
        SELECT name FROM main.sqlite_schema
        WHERE name IN ${observer.sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)}
        ORDER BY name
      `;
      assert.equal(objects.length, PROVIDER_ADMISSION_SCHEMA_OBJECTS.length);
      assert.deepStrictEqual(
        yield* writer.sql`
          SELECT name FROM temp.sqlite_schema
          WHERE name='agent_control_provider_admission_current'
        `,
        [{ name: "agent_control_provider_admission_current" }],
      );
      assert.deepStrictEqual(
        yield* writer.sql`
          SELECT name FROM shadow.sqlite_schema
          WHERE name='agent_control_provider_admission_current'
        `,
        [{ name: "agent_control_provider_admission_current" }],
      );
      const ddl = yield* observer.sql<{ readonly name: string; readonly source: string }>`
        SELECT name,sql AS source FROM main.sqlite_schema
        WHERE name IN ${observer.sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)} AND sql IS NOT NULL
        ORDER BY name
      `;
      assert.deepStrictEqual(
        Object.fromEntries(ddl.map((row) => [row.name, sha256Utf8(row.source)])),
        EXPECTED_PROVIDER_ADMISSION_DDL_FINGERPRINTS,
      );
      const udf = yield* observer.sql<{
        readonly valid: number;
        readonly text: number;
        readonly invalid: number;
      }>`
        SELECT t3_run_once_canonical_blob_match(
          CAST('{"schemaVersion":1}' AS BLOB),
          '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72'
        ) AS valid,
        t3_run_once_canonical_blob_match(
          '{"schemaVersion":1}',
          '0e9561cfb83d50990a103b3896fe249a11fe27fa28985448187f93ec12116d72'
        ) AS text,
        t3_run_once_canonical_blob_match(CAST('{"schemaVersion":2}' AS BLOB), ${"0".repeat(64)}) AS invalid
      `;
      assert.deepStrictEqual(udf, [{ valid: 1, text: 0, invalid: 0 }]);
      assert.deepStrictEqual(yield* observer.sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* observer.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("commits one provider slot across two native WAL connections and preserves FIFO", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-capacity-wal-" });
      const filename = path.join(directory, "capacity.sqlite");
      const first = yield* openDatabase(filename);
      const second = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(second.scope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(first.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
        Effect.provideService(SqlClient.SqlClient, first.sql),
      );
      const firstContext = yield* Layer.buildWithScope(
        Layer.fresh(ProviderAdmissionStoreLive).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, first.sql)),
        ),
        first.scope,
      );
      const secondContext = yield* Layer.buildWithScope(
        Layer.fresh(ProviderAdmissionStoreLive).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, second.sql)),
        ),
        second.scope,
      );
      const firstStore = Context.get(firstContext, ProviderAdmissionStore);
      const secondStore = Context.get(secondContext, ProviderAdmissionStore);
      const usage = providerAdmissionUsageEvidence({
        providerInstanceId: ProviderInstanceId.make("codex-a"),
        status: "allowed",
        observedAt: at,
        source: "refresh",
        nextRelevantAt: null,
      });
      const admitted = yield* firstStore.request({
        request: request("first"),
        usage,
        ownerId: "owner-first",
        leaseExpiresAt: later,
        now: at,
      });
      assert.equal(admitted._tag, "Admitted");
      if (admitted._tag !== "Admitted") return;
      const foreignOwner = yield* secondStore.request({
        request: request("first"),
        usage,
        ownerId: "owner-restart",
        leaseExpiresAt: later,
        now: at,
      });
      assert.deepStrictEqual(foreignOwner, {
        _tag: "Waiting",
        admissionId: admitted.permit.admissionId,
        retryAt: later,
      });
      const replayed = yield* secondStore.request({
        request: request("first"),
        usage,
        ownerId: "owner-first",
        leaseExpiresAt: later,
        now: at,
      });
      assert.equal(replayed._tag, "Admitted");
      if (replayed._tag !== "Admitted") return;
      assert.equal(replayed.permit.providerFenceToken, admitted.permit.providerFenceToken);
      assert.equal(replayed.permit.admissionMarkerId, admitted.permit.admissionMarkerId);
      const waiting = yield* secondStore.request({
        request: request("second"),
        usage,
        ownerId: "owner-second",
        leaseExpiresAt: later,
        now: at,
      });
      assert.equal(waiting._tag, "Waiting");
      const parallel = yield* secondStore.request({
        request: request("parallel", "codex-b"),
        usage: providerAdmissionUsageEvidence({
          providerInstanceId: ProviderInstanceId.make("codex-b"),
          status: "warning",
          observedAt: at,
          source: "runtime-event",
          nextRelevantAt: null,
        }),
        ownerId: "owner-parallel",
        leaseExpiresAt: later,
        now: at,
      });
      assert.equal(parallel._tag, "Admitted");
      const capacity = yield* second.sql<{ readonly count: number }>`
        SELECT count(*) AS count FROM main.agent_control_provider_capacity_current
        WHERE active_state IN ('claimed','admitted','entered','quarantined')
      `;
      assert.deepStrictEqual(capacity, [{ count: 2 }]);
      const takeover = yield* secondStore.request({
        request: request("first"),
        usage,
        ownerId: "owner-takeover",
        leaseExpiresAt: afterTakeover,
        now: afterExpiry,
      });
      assert.equal(takeover._tag, "Admitted");
      if (takeover._tag !== "Admitted") return;
      assert.equal(takeover.permit.providerFenceToken, admitted.permit.providerFenceToken + 1);
      assert.notEqual(takeover.permit.admissionMarkerId, admitted.permit.admissionMarkerId);
      const staleEntry = yield* Effect.exit(
        firstStore.validateAndEnterInTransaction({
          permit: admitted.permit,
          boundary: "turn-start",
          enteredAt: afterExpiry,
        }),
      );
      assert.isTrue(Exit.isFailure(staleEntry));
      const indexInfo = yield* second.sql<{ readonly name: string }>`
        PRAGMA main.index_info('idx_agent_control_provider_admission_queue')
      `;
      assert.deepStrictEqual(
        indexInfo.map((row) => row.name),
        ["provider_instance_id", "status", "usage_eligible", "requested_at", "admission_id"],
      );
      const plan = yield* second.sql.unsafe<{ readonly detail: string }>(
        `EXPLAIN QUERY PLAN ${PROVIDER_ADMISSION_OLDEST_ELIGIBLE_SQL}`,
        ["codex-a"],
      );
      assert.isTrue(
        plan.some((row) => row.detail.includes("idx_agent_control_provider_admission_queue")),
      );
      assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
      assert.isFalse(plan.some((row) => row.detail.startsWith("SCAN ")));
      assert.deepStrictEqual(yield* second.sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* second.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "rejects projection revision, marker, and fence rewinds and audits persisted corruption",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-rewind-" });
        const filename = path.join(directory, "rewind.sqlite");
        const db = yield* openDatabase(filename);
        yield* Effect.addFinalizer(() => Scope.close(db.scope, Exit.void));
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, db.sql),
        );
        const context = yield* Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, db.sql)),
          ),
          db.scope,
        );
        const store = Context.get(context, ProviderAdmissionStore);
        const value = request("rewind");
        const usage = providerAdmissionUsageEvidence({
          providerInstanceId: value.providerInstanceId,
          status: "allowed",
          observedAt: at,
          source: "refresh",
          nextRelevantAt: null,
        });
        const first = yield* store.request({
          request: value,
          usage,
          ownerId: "rewind-owner-1",
          leaseExpiresAt: later,
          now: at,
        });
        if (first._tag !== "Admitted") return yield* Effect.die("missing first permit");
        const takeover = yield* store.request({
          request: value,
          usage,
          ownerId: "rewind-owner-2",
          leaseExpiresAt: afterTakeover,
          now: afterExpiry,
        });
        if (takeover._tag !== "Admitted") return yield* Effect.die("missing takeover permit");

        const oldMarker = yield* Effect.exit(db.sql`
        UPDATE main.agent_control_provider_admission_current SET
          owner_id=${first.permit.admissionOwnerId},
          lease_expires_at=${first.permit.admissionLeaseExpiresAt},
          provider_fence_token=${first.permit.providerFenceToken},
          admission_marker_id=${first.permit.admissionMarkerId},
          admission_marker_fingerprint=${first.permit.admissionMarkerFingerprint},
          revision=revision+1,updated_at=${afterTakeover}
        WHERE admission_id=${first.permit.admissionId}
      `);
        assert.isTrue(Exit.isFailure(oldMarker));
        const lowerCapacityFence = yield* Effect.exit(db.sql`
        UPDATE main.agent_control_provider_capacity_current SET
          last_fence_token=${first.permit.providerFenceToken},
          active_fence_token=${first.permit.providerFenceToken},
          revision=revision+1,updated_at=${afterTakeover}
        WHERE provider_instance_id=${String(value.providerInstanceId)}
      `);
        assert.isTrue(Exit.isFailure(lowerCapacityFence));

        const waitingValue = request("revision", "codex-revision");
        yield* store.request({
          request: waitingValue,
          usage: providerAdmissionUsageEvidence({
            providerInstanceId: waitingValue.providerInstanceId,
            status: "rejected",
            observedAt: at,
            source: "refresh",
            nextRelevantAt: null,
          }),
          ownerId: "revision-owner",
          leaseExpiresAt: later,
          now: at,
        });
        const skippedRevision = yield* Effect.exit(db.sql`
        UPDATE main.agent_control_provider_admission_current
        SET revision=revision+2,updated_at=${later}
        WHERE handoff_id=${waitingValue.handoffId}
      `);
        assert.isTrue(Exit.isFailure(skippedRevision));

        const [trigger] = yield* db.sql<{ readonly source: string }>`
        SELECT sql AS source FROM main.sqlite_schema
        WHERE name='agent_control_provider_capacity_current_validate_update'
      `;
        assert.isDefined(trigger);
        yield* db.sql.unsafe(
          "DROP TRIGGER main.agent_control_provider_capacity_current_validate_update",
        ).unprepared;
        yield* db.sql`
        UPDATE main.agent_control_provider_capacity_current SET
          last_fence_token=${first.permit.providerFenceToken},
          active_fence_token=${first.permit.providerFenceToken},
          revision=revision+1,updated_at=${afterTakeover}
        WHERE provider_instance_id=${String(value.providerInstanceId)}
      `;
        yield* db.sql.unsafe(trigger!.source).unprepared;

        const auditScope = yield* Scope.make("sequential");
        const audited = yield* Effect.exit(
          Layer.buildWithScope(
            Layer.fresh(ProviderAdmissionStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, db.sql)),
            ),
            auditScope,
          ),
        );
        assert.isTrue(Exit.isFailure(audited));
        yield* Scope.close(auditScope, Exit.void);
        assert.deepStrictEqual(yield* db.sql`PRAGMA main.foreign_key_check`, []);
        assert.equal((yield* db.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rolls back a faulted 065 and rejects a pre-existing partial schema", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-admission-repair-",
      });
      const filename = path.join(directory, "repair.sqlite");
      const db = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(db.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
        Effect.provideService(SqlClient.SqlClient, db.sql),
      );
      const fault = yield* Effect.exit(
        db.sql.withTransaction(
          makeMigration065((point) =>
            point === "after-indexes" ? Effect.die("injected-065-fault") : Effect.void,
          ).pipe(Effect.provideService(SqlClient.SqlClient, db.sql)),
        ),
      );
      assert.isTrue(Exit.isFailure(fault));
      assert.deepStrictEqual(
        yield* db.sql`
          SELECT name FROM main.sqlite_schema
          WHERE name IN ${db.sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)}
        `,
        [],
      );
      yield* db.sql.unsafe(
        "CREATE TABLE main.agent_control_provider_admission_intents(sentinel TEXT)",
      ).unprepared;
      const partial = yield* Effect.exit(
        db.sql.withTransaction(
          makeMigration065().pipe(Effect.provideService(SqlClient.SqlClient, db.sql)),
        ),
      );
      assert.isTrue(Exit.isFailure(partial));
      assert.deepStrictEqual(
        yield* db.sql`
          SELECT name FROM main.sqlite_schema
          WHERE name IN ${db.sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)}
        `,
        [{ name: "agent_control_provider_admission_intents" }],
      );
      yield* db.sql.unsafe("DROP TABLE main.agent_control_provider_admission_intents").unprepared;
      yield* db.sql.withTransaction(
        makeMigration065().pipe(Effect.provideService(SqlClient.SqlClient, db.sql)),
      );
      assert.equal(
        (yield* db.sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.sqlite_schema
          WHERE name IN ${db.sql.in(PROVIDER_ADMISSION_SCHEMA_OBJECTS)}
        `)[0]?.count,
        PROVIDER_ADMISSION_SCHEMA_OBJECTS.length,
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("preserves populated Migration-064 bytes and rejects correct bytes in TEXT storage", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-provider-admission-bytes-",
      });
      const filename = path.join(directory, "bytes.sqlite");
      const db = yield* openDatabase(filename);
      yield* Effect.addFinalizer(() => Scope.close(db.scope, Exit.void));
      yield* runMigrations({ toMigrationInclusive: 64 }).pipe(
        Effect.provideService(SqlClient.SqlClient, db.sql),
      );
      yield* db.sql`
        INSERT INTO main.checkpoint_diff_blobs(
          thread_id,from_turn_count,to_turn_count,diff,created_at
        ) VALUES ('historical-thread',1,2,${"ä\u0000historical-diff"},${at})
      `;
      const before = yield* db.sql`
        SELECT typeof(diff) AS storage,hex(CAST(diff AS BLOB)) AS bytes
        FROM main.checkpoint_diff_blobs WHERE thread_id='historical-thread'
      `;
      yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
        Effect.provideService(SqlClient.SqlClient, db.sql),
      );
      assert.deepStrictEqual(
        yield* db.sql`
          SELECT typeof(diff) AS storage,hex(CAST(diff AS BLOB)) AS bytes
          FROM main.checkpoint_diff_blobs WHERE thread_id='historical-thread'
        `,
        before,
      );
      const value = request("wrong-storage");
      const admissionId = providerAdmissionId(value);
      const document = canonicalJson(intentDocument(admissionId, value));
      const fingerprint = sha256Utf8(document);
      const wrongModelStorage = yield* Effect.exit(db.sql`
        INSERT INTO main.agent_control_provider_admission_intents (
          admission_id,stage,project_id,task_id,stage_run_id,attempt_id,handoff_id,
          provider_delivery_id,thread_id,provider_instance_id,stage_lease_id,
          stage_lease_holder_id,stage_fence_token,model_selection_json,
          model_selection_fingerprint,requested_at,intent_json,intent_fingerprint
        ) VALUES (
          ${admissionId},${value.stage},${value.projectId},${value.taskId},${value.stageRunId},
          ${value.attemptId},${value.handoffId},${value.providerDeliveryId},${value.threadId},
          ${value.providerInstanceId},${value.stageLeaseId},${value.stageLeaseHolderId},
          ${value.stageFenceToken},${value.modelSelectionJson},${value.modelSelectionFingerprint},
          ${value.requestedAt},${new TextEncoder().encode(document)},${fingerprint}
        )
      `);
      assert.isTrue(Exit.isFailure(wrongModelStorage));
      const wrongIntentStorage = yield* Effect.exit(db.sql`
        INSERT INTO main.agent_control_provider_admission_intents (
          admission_id,stage,project_id,task_id,stage_run_id,attempt_id,handoff_id,
          provider_delivery_id,thread_id,provider_instance_id,stage_lease_id,
          stage_lease_holder_id,stage_fence_token,model_selection_json,
          model_selection_fingerprint,requested_at,intent_json,intent_fingerprint
        ) VALUES (
          ${admissionId},${value.stage},${value.projectId},${value.taskId},${value.stageRunId},
          ${value.attemptId},${value.handoffId},${value.providerDeliveryId},${value.threadId},
          ${value.providerInstanceId},${value.stageLeaseId},${value.stageLeaseHolderId},
          ${value.stageFenceToken},${new TextEncoder().encode(value.modelSelectionJson)},
          ${value.modelSelectionFingerprint},${value.requestedAt},${document},${fingerprint}
        )
      `);
      assert.isTrue(Exit.isFailure(wrongIntentStorage));
      assert.deepStrictEqual(
        yield* db.sql`SELECT admission_id FROM main.agent_control_provider_admission_intents`,
        [],
      );
      assert.deepStrictEqual(yield* db.sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* db.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
