import { ModelSelection, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ProviderAdmissionStoreLive } from "../../agentControl/providerAdmission/Layers/ProviderAdmissionStore.ts";
import { ProviderAdmissionStore } from "../../agentControl/providerAdmission/Services/ProviderAdmissionStore.ts";
import {
  providerAdmissionUsageEvidence,
  type ProviderAdmissionRequest,
  type ProviderResourceAdmissionRequest,
} from "../../agentControl/providerAdmission/model.ts";
import { canonicalProviderModelSelectionEvidence } from "../../provider/Services/ProviderAdapter.ts";
import { runMigrations } from "../Migrations.ts";
import * as NodeSqliteClient from "../NodeSqliteClient.ts";

const at = "2026-09-17T10:00:00.000Z";
const expiry = "2026-09-17T10:02:00.000Z";
const later = "2026-09-17T10:03:00.000Z";
const nextExpiry = "2026-09-17T10:05:00.000Z";
const instance = ProviderInstanceId.make("codex-migration");
const modelSelection: ModelSelection = { instanceId: instance, model: "test-model" };
const model = canonicalProviderModelSelectionEvidence(modelSelection);
const usage = providerAdmissionUsageEvidence({
  providerInstanceId: instance,
  status: "allowed",
  observedAt: at,
  source: "refresh",
  nextRelevantAt: null,
});
const request = (id: string): ProviderAdmissionRequest => ({
  stage: "implementation",
  projectId: `project-${id}`,
  taskId: `task-${id}`,
  stageRunId: `stage-${id}`,
  attemptId: `attempt-${id}`,
  handoffId: `handoff-${id}`,
  providerDeliveryId: `delivery-${id}`,
  threadId: ThreadId.make(`thread-${id}`),
  providerInstanceId: instance,
  stageLeaseId: `lease-${id}`,
  stageLeaseHolderId: `stage-owner-${id}`,
  stageFenceToken: 1,
  modelSelection,
  modelSelectionJson: model.modelSelectionJson,
  modelSelectionFingerprint: model.modelSelectionFingerprint,
  requestedAt: at,
});
const store = () =>
  Effect.service(ProviderAdmissionStore).pipe(
    Effect.provide(Layer.fresh(ProviderAdmissionStoreLive)),
  );
const admit = (
  service: ProviderAdmissionStore["Service"],
  id: string,
  ownerId = `owner-${id}`,
  now = at,
  leaseExpiresAt = expiry,
) => service.request({ request: request(id), usage, ownerId, now, leaseExpiresAt });

it.effect(
  "upgrades a live legacy admission without replacing its permit and allocates isolated same-provider lanes",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 90 });
      const legacy = yield* store();
      const original = yield* admit(legacy, "a");
      assert.equal(original._tag, "Admitted");
      if (original._tag !== "Admitted") return;
      assert.equal((yield* admit(legacy, "b"))._tag, "Waiting");
      const before =
        yield* sql`SELECT * FROM agent_control_provider_admission_current ORDER BY admission_id`;
      const claims = yield* sql`SELECT * FROM agent_control_provider_claim_history`;
      const markers = yield* sql`SELECT * FROM agent_control_provider_authority_markers`;
      const capacityBefore =
        yield* sql`SELECT provider_instance_id,last_fence_token,active_admission_id,active_state,active_owner_id,active_lease_expires_at,active_fence_token,active_marker_fingerprint,revision,updated_at FROM agent_control_provider_capacity_current`;
      yield* runMigrations({ toMigrationInclusive: 91 });
      assert.deepEqual(
        yield* sql`SELECT * FROM agent_control_provider_admission_current ORDER BY admission_id`,
        before,
      );
      assert.deepEqual(yield* sql`SELECT * FROM agent_control_provider_claim_history`, claims);
      assert.deepEqual(yield* sql`SELECT * FROM agent_control_provider_authority_markers`, markers);
      assert.deepEqual(
        yield* sql`SELECT provider_instance_id,last_fence_token,active_admission_id,active_state,active_owner_id,active_lease_expires_at,active_fence_token,active_marker_fingerprint,revision,updated_at FROM agent_control_provider_capacity_current`,
        capacityBefore,
      );
      const reopened = yield* store();
      assert.deepEqual(yield* admit(reopened, "a"), original);
      const [b, c] = yield* Effect.all([admit(reopened, "b"), admit(reopened, "c")], {
        concurrency: 2,
      });
      assert.equal(b._tag, "Admitted");
      assert.equal(c._tag, "Admitted");
      if (b._tag !== "Admitted" || c._tag !== "Admitted") return;
      assert.equal(
        new Set([
          original.permit.providerFenceToken,
          b.permit.providerFenceToken,
          c.permit.providerFenceToken,
        ]).size,
        3,
      );
      assert.equal(
        (yield* sql`SELECT * FROM agent_control_provider_capacity_current WHERE active_state='admitted'`)
          .length,
        3,
      );
      assert.deepEqual(yield* admit(reopened, "a"), original);
      const taken = yield* admit(reopened, "a", "replacement-owner", later, nextExpiry);
      assert.equal(taken._tag, "Admitted");
      if (taken._tag !== "Admitted") return;
      assert.isAbove(
        taken.permit.providerFenceToken,
        Math.max(b.permit.providerFenceToken, c.permit.providerFenceToken),
      );
      assert.equal((yield* admit(reopened, "a", "owner-a", later, nextExpiry))._tag, "Waiting");
      assert.deepEqual(yield* admit(reopened, "b"), b);
      assert.deepEqual(yield* admit(reopened, "c"), c);
      const stale = yield* Effect.result(
        sql.withTransaction(
          reopened.validateAndEnterInTransaction({
            permit: original.permit,
            boundary: "turn-start",
            enteredAt: later,
          }),
        ),
      );
      assert.equal(stale._tag, "Failure");
      yield* store();
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);

it.effect(
  "migration and restart retain unknown entered shared capacity and its manual competitor waits",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 90 });
      const legacy = yield* store();
      const limits = {
        maxConcurrent: 1,
        interactiveReserve: 0,
        backgroundAgingMs: 30_000,
        maxInteractiveBurst: 3,
      };
      const resource = (id: string): ProviderResourceAdmissionRequest => ({
        idempotencyKey: `turn-${id}`,
        providerInstanceId: instance,
        threadId: `thread-${id}`,
        accountScope: "migration-account",
        workloadClass: "interactive",
        source: "manual",
        requestedAt: at,
      });
      const first = yield* legacy.requestResource!({
        request: resource("uncertain"),
        usage,
        limits,
        ownerId: "old-owner",
        leaseExpiresAt: expiry,
        now: at,
      });
      assert.equal(first.decision._tag, "Admitted");
      if (first.decision._tag !== "Admitted") return;
      const permit = first.decision.permit;
      yield* legacy.enterResource!({
        permit,
        enteredAt: at,
        providerTurnId: "possibly-running-turn",
      });
      const before = yield* sql`SELECT * FROM resource_admission_provider_requests`;
      yield* runMigrations({ toMigrationInclusive: 91 });
      assert.deepEqual(yield* sql`SELECT * FROM resource_admission_provider_requests`, before);
      const restarted = yield* store();
      yield* restarted.reconcileResource!({
        requestId: permit.requestId,
        observedActivity: "unknown",
        ownerId: "new-owner",
        leaseExpiresAt: nextExpiry,
        observedAt: later,
      });
      const manual = yield* restarted.requestResource!({
        request: resource("manual"),
        usage,
        limits,
        ownerId: "manual-owner",
        leaseExpiresAt: nextExpiry,
        now: later,
      });
      assert.equal(manual.decision._tag, "Waiting");
      const active = yield* restarted.listResourceActive!;
      const uncertain = active.find((row) => row.requestId === permit.requestId);
      assert.equal(uncertain?.status, "entered");
      assert.equal(uncertain?.lastObservedActivity, "unknown");
      assert.deepEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layerMemory())),
);
