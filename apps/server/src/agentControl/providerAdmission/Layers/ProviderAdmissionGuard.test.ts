import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqlite from "node:sqlite";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import { ModelSelection, ProviderInstanceId } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import Migration076 from "../../../persistence/Migrations/076_AgentControlVerificationChecks.ts";
import {
  executeVerificationCheck,
  prepareVerificationCheckManifest,
} from "../../verificationTurn/checkEvidence.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import { canonicalJson } from "../../initialPlanning/eventEvidence.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardShape,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import {
  providerAdmissionUsageEvidence,
  type ProviderAdmissionPermit,
  type ProviderAdmissionRequest,
  type ProviderAdmissionStage,
} from "../model.ts";
import { ProviderAdmissionGuard } from "../Services/ProviderAdmissionGuard.ts";
import { ProviderAdmissionReleaseAuthority } from "../Services/ProviderAdmissionReleaseAuthority.ts";
import { ProviderAdmissionRuntime } from "../Services/ProviderAdmissionRuntime.ts";
import { ProviderAdmissionStore } from "../Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionGuardLive } from "./ProviderAdmissionGuard.ts";
import { ProviderAdmissionReleaseAuthorityLive } from "./ProviderAdmissionReleaseAuthority.ts";
import { ProviderAdmissionStoreLive } from "./ProviderAdmissionStore.ts";

const at = "2026-09-06T08:00:00.000Z";
const leaseExpiry = "2099-09-06T08:00:00.000Z";
const sourceFingerprint = "a".repeat(64);

const request = (
  stage: ProviderAdmissionStage,
  providerInstanceId: ProviderInstanceId,
): ProviderAdmissionRequest => {
  const suffix = stage;
  const modelSelection: ModelSelection = { instanceId: providerInstanceId, model: "gpt-5.6" };
  const model = canonicalProviderModelSelectionEvidence(modelSelection);
  return {
    stage,
    projectId: `project-${suffix}`,
    taskId: `task-${suffix}`,
    stageRunId: `stage-${suffix}`,
    attemptId: `attempt-${suffix}`,
    handoffId: `handoff-${suffix}`,
    providerDeliveryId: `delivery-${suffix}`,
    threadId: `thread-${suffix}`,
    providerInstanceId,
    stageLeaseId: `lease-${suffix}`,
    stageLeaseHolderId: `holder-${suffix}`,
    stageFenceToken: stage === "initial-planning" ? 1 : stage === "implementation" ? 2 : 3,
    modelSelection,
    modelSelectionJson: model.modelSelectionJson,
    modelSelectionFingerprint: model.modelSelectionFingerprint,
    requestedAt: at,
  };
};

const taskGuardShape: AgentControlTaskConsumerGuardShape = {
  inspectProject: () => Effect.die("not used"),
  useTaskConsumable: (_projectId, _taskId, use) => use({} as never, {} as never),
  useTaskConsumableInTransaction: (_projectId, _taskId, use) => use({} as never, {} as never),
  useTaskForProviderEffectInTransaction: (_projectId, _taskId, use) =>
    use({} as never, {} as never),
};

const seedStageAndDelivery = (
  sql: SqlClient.SqlClient,
  value: ProviderAdmissionRequest,
  includeDelivery = true,
) =>
  Effect.gen(function* () {
    yield* sql`
      INSERT INTO main.agent_control_stage_run_states (
        stage_run_id,project_id,task_id,attempt_id,role_id,stage_kind,stage_ordinal,
        attempt_ordinal,status,task_revision,github_intake_sequence,
        source_identity_fingerprint,state_json,created_at,updated_at,revision,last_event_sequence
      ) VALUES (
        ${value.stageRunId},${value.projectId},${value.taskId},${value.attemptId},
        ${`role-${value.stage}`},
        ${value.stage === "initial-planning" ? "planning" : value.stage},1,1,'prepared',1,1,
        ${sourceFingerprint},'{}',${at},${at},1,1
      )
    `;
    yield* sql`
      INSERT INTO main.agent_control_stage_run_lease_states (
        lease_id,project_id,task_id,stage_run_id,attempt_id,task_revision,
        github_intake_sequence,source_identity_fingerprint,holder_id,fence_token,
        status,acquired_at,renewed_at,expires_at,released_at,state_json,revision,last_event_sequence
      ) VALUES (
        ${value.stageLeaseId},${value.projectId},${value.taskId},${value.stageRunId},
        ${value.attemptId},1,1,${sourceFingerprint},${value.stageLeaseHolderId},
        ${value.stageFenceToken},'reserved',${at},${at},${leaseExpiry},NULL,'{}',1,1
      )
    `;
    if (!includeDelivery) return;
    if (value.stage === "initial-planning") {
      yield* sql`
        INSERT INTO main.agent_control_initial_planning_deliveries (
          provider_delivery_id,handoff_id,handoff_fingerprint,
          controlled_thread_reservation_id,thread_id,turn_request_command_id,message_id,
          provider_instance_id,state,revision,claim_owner_id,claim_generation,
          claim_expires_at,attempt_count,next_attempt_at,planning_deadline_at,
          provider_turn_id,provider_accepted_at,provider_session_created_at,
          provider_resume_cursor_json,terminal_at,last_error_code,interrupt_requested,updated_at
        ) VALUES (
          ${value.providerDeliveryId},${value.handoffId},${"1".repeat(64)},
          'reservation-initial','thread-initial-planning','command-initial','message-initial',
          ${value.providerInstanceId},'claimed',1,'delivery-owner',1,${leaseExpiry},0,NULL,
          ${leaseExpiry},NULL,NULL,NULL,NULL,NULL,NULL,0,${at}
        )
      `;
      return;
    }
    const table =
      value.stage === "implementation"
        ? "agent_control_implementation_deliveries"
        : "agent_control_verification_deliveries";
    yield* sql.unsafe(
      `INSERT INTO main.${table} (
        provider_delivery_id,handoff_id,handoff_fingerprint,admission_marker_id,
        materialization_evidence_id,controlled_thread_reservation_id,thread_id,
        stage_run_id,attempt_id,lease_id,lease_holder_id,fence_token,
        provider_instance_id,runtime_mode,model_selection_fingerprint,
        turn_request_command_id,message_id,planning_thread_id,plan_id,state,revision,
        claim_owner_id,claim_generation,claim_expires_at,attempt_count,next_attempt_at,
        provider_turn_id,provider_accepted_at,provider_session_created_at,
        provider_resume_cursor_json,terminal_at,last_error_code,interrupt_requested,updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      [
        value.providerDeliveryId,
        value.handoffId,
        value.stage === "implementation" ? "2".repeat(64) : "3".repeat(64),
        `stage-admission-${value.stage}`,
        `materialization-${value.stage}`,
        `reservation-${value.stage}`,
        value.threadId,
        value.stageRunId,
        value.attemptId,
        value.stageLeaseId,
        value.stageLeaseHolderId,
        value.stageFenceToken,
        value.providerInstanceId,
        value.stage === "implementation" ? "full-access" : "approval-required",
        value.modelSelectionFingerprint,
        `command-${value.stage}`,
        `message-${value.stage}`,
        `planning-thread-${value.stage}`,
        `plan-${value.stage}`,
        "claimed",
        1,
        "delivery-owner",
        1,
        leaseExpiry,
        0,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        at,
      ],
    );
  });

const seedNonInitialDelivery = (
  database: NodeSqlite.DatabaseSync,
  value: ProviderAdmissionRequest,
) => {
  const table =
    value.stage === "implementation"
      ? "agent_control_implementation_deliveries"
      : "agent_control_verification_deliveries";
  database
    .prepare(
      `INSERT INTO main.${table} (
        provider_delivery_id,handoff_id,handoff_fingerprint,admission_marker_id,
        materialization_evidence_id,controlled_thread_reservation_id,thread_id,
        stage_run_id,attempt_id,lease_id,lease_holder_id,fence_token,
        provider_instance_id,runtime_mode,model_selection_fingerprint,
        turn_request_command_id,message_id,planning_thread_id,plan_id,state,revision,
        claim_owner_id,claim_generation,claim_expires_at,attempt_count,next_attempt_at,
        provider_turn_id,provider_accepted_at,provider_session_created_at,
        provider_resume_cursor_json,terminal_at,last_error_code,interrupt_requested,updated_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      value.providerDeliveryId,
      value.handoffId,
      value.stage === "implementation" ? "2".repeat(64) : "3".repeat(64),
      `stage-admission-${value.stage}`,
      `materialization-${value.stage}`,
      `reservation-${value.stage}`,
      value.threadId,
      value.stageRunId,
      value.attemptId,
      value.stageLeaseId,
      value.stageLeaseHolderId,
      value.stageFenceToken,
      value.providerInstanceId,
      value.stage === "implementation" ? "full-access" : "approval-required",
      value.modelSelectionFingerprint,
      `command-${value.stage}`,
      `message-${value.stage}`,
      `planning-thread-${value.stage}`,
      `plan-${value.stage}`,
      "claimed",
      1,
      "delivery-owner",
      1,
      leaseExpiry,
      0,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      0,
      at,
    );
};

const seedInitialPlanningFinalization = (
  database: NodeSqlite.DatabaseSync,
  permit: ProviderAdmissionPermit,
  finalizedAt: string,
  ordinal = 1,
) => {
  const terminalEventId = `terminal-event-${permit.admissionId}`;
  const resultEvidenceId = `result-${permit.admissionId}`;
  const commandId = `finalize-${permit.admissionId}`;
  const finalizationFingerprint = `${"b".repeat(63)}${ordinal}`;
  const markerId = `finalization-marker-${permit.admissionId}`;
  const markerFingerprint = `${"c".repeat(63)}${ordinal}`;
  const stageEventId = `stage-terminal-${permit.admissionId}`;
  const leaseEventId = `lease-terminal-${permit.admissionId}`;
  database
    .prepare(
      `INSERT INTO main.agent_control_initial_planning_result_evidence (
        result_evidence_id,finalization_command_id,finalization_fingerprint,outcome,
        handoff_id,handoff_fingerprint,project_id,task_id,task_revision,
        github_intake_sequence,source_identity_fingerprint,controlled_thread_reservation_id,
        thread_id,stage_run_id,attempt_id,lease_id,lease_holder_id,fence_token,
        provider_delivery_id,provider_instance_id,provider_turn_id,runtime_mode,
        model_selection_fingerprint,delivery_terminal_state,delivery_revision,terminal_at,
        orchestration_started_event_id,orchestration_started_sequence,
        orchestration_terminal_event_id,orchestration_terminal_sequence,
        plan_id,plan_event_id,plan_event_sequence,proposed_plan_json,proposed_plan_digest,
        stage_event_id,stage_event_sequence,stage_event_stream_version,
        lease_event_id,lease_event_sequence,lease_event_stream_version,finalized_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      resultEvidenceId,
      commandId,
      finalizationFingerprint,
      "failed",
      permit.handoffId,
      `${"d".repeat(63)}${ordinal}`,
      permit.projectId,
      permit.taskId,
      1,
      1,
      sourceFingerprint,
      `reservation-${permit.admissionId}`,
      permit.threadId,
      permit.stageRunId,
      permit.attemptId,
      permit.stageLeaseId,
      permit.stageLeaseHolderId,
      permit.stageFenceToken,
      permit.providerDeliveryId,
      permit.providerInstanceId,
      `provider-turn-${permit.admissionId}`,
      "approval-required",
      permit.modelSelectionFingerprint,
      "failed",
      1,
      finalizedAt,
      `started-event-${permit.admissionId}`,
      ordinal * 2 - 1,
      terminalEventId,
      ordinal * 2,
      null,
      null,
      null,
      null,
      null,
      stageEventId,
      ordinal,
      3,
      leaseEventId,
      ordinal,
      2,
      finalizedAt,
    );
  database
    .prepare(
      `INSERT INTO main.agent_control_initial_planning_finalization_receipts (
        finalization_command_id,finalization_fingerprint,result_evidence_id,handoff_id,
        outcome,stage_event_id,stage_event_sequence,lease_event_id,lease_event_sequence,accepted_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      commandId,
      finalizationFingerprint,
      resultEvidenceId,
      permit.handoffId,
      "failed",
      stageEventId,
      ordinal,
      leaseEventId,
      ordinal,
      finalizedAt,
    );
  database
    .prepare(
      `INSERT INTO main.agent_control_initial_planning_finalization_markers (
        marker_id,marker_fingerprint,finalization_command_id,result_evidence_id,
        handoff_id,committed_at
      ) VALUES (?,?,?,?,?,?)`,
    )
    .run(markerId, markerFingerprint, commandId, resultEvidenceId, permit.handoffId, finalizedAt);
};

const seedVerificationFinalization = (
  database: NodeSqlite.DatabaseSync,
  permit: ProviderAdmissionPermit,
  finalizedAt: string,
  terminalRuntimeEventId: string,
) => {
  const evidenceId = `verification-finalization-evidence-${permit.admissionId}`;
  const receiptId = `verification-finalization-receipt-${permit.admissionId}`;
  const markerId = `verification-finalization-marker-${permit.admissionId}`;
  const commandId = `verification-finalization-command-${permit.admissionId}`;
  const finalizationFingerprint = "e".repeat(64);
  database
    .prepare(
      `INSERT INTO main.agent_control_verification_finalization_evidence (
        finalization_evidence_id,receipt_id,marker_id,finalization_command_id,
        finalization_fingerprint,finalization_json,handoff_id,handoff_fingerprint,
        project_id,task_id,task_revision,github_intake_sequence,source_identity_fingerprint,
        stage_run_id,attempt_id,lease_id,lease_holder_id,fence_token,provider_delivery_id,
        provider_instance_id,provider_turn_id,delivery_revision,delivery_terminal_state,
        terminal_runtime_event_id,terminal_at,start_evidence_id,start_receipt_id,start_marker_id,
        evaluation_authority,evaluation_id,evaluation_evidence_id,evaluation_receipt_id,
        evaluation_marker_id,evaluation_disposition,verification_verdict,invalid_output_code,
        outcome,terminal_cause,stage_event_id,stage_event_sequence,stage_event_stream_version,
        lease_event_id,lease_event_sequence,lease_event_stream_version,finalized_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      evidenceId,
      receiptId,
      markerId,
      commandId,
      finalizationFingerprint,
      "{}",
      permit.handoffId,
      "f".repeat(64),
      permit.projectId,
      permit.taskId,
      1,
      1,
      sourceFingerprint,
      permit.stageRunId,
      permit.attemptId,
      permit.stageLeaseId,
      permit.stageLeaseHolderId,
      Math.max(3, permit.stageFenceToken),
      permit.providerDeliveryId,
      permit.providerInstanceId,
      `provider-turn-${permit.admissionId}`,
      1,
      "failed",
      terminalRuntimeEventId,
      finalizedAt,
      `verification-start-evidence-${permit.admissionId}`,
      `verification-start-receipt-${permit.admissionId}`,
      `verification-start-marker-${permit.admissionId}`,
      "not-applicable",
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      "failed",
      "provider-delivery-failed",
      `verification-stage-event-${permit.admissionId}`,
      1,
      3,
      `verification-lease-event-${permit.admissionId}`,
      1,
      2,
      finalizedAt,
    );
  database
    .prepare(
      `INSERT INTO main.agent_control_verification_finalization_receipts (
        receipt_id,marker_id,finalization_evidence_id,finalization_command_id,
        finalization_fingerprint,handoff_id,outcome,terminal_cause,stage_event_id,
        stage_event_sequence,lease_event_id,lease_event_sequence,status,accepted_at
      ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    )
    .run(
      receiptId,
      markerId,
      evidenceId,
      commandId,
      finalizationFingerprint,
      permit.handoffId,
      "failed",
      "provider-delivery-failed",
      `verification-stage-event-${permit.admissionId}`,
      1,
      `verification-lease-event-${permit.admissionId}`,
      1,
      "accepted",
      finalizedAt,
    );
  database
    .prepare(
      `INSERT INTO main.agent_control_verification_finalization_markers (
        marker_id,marker_fingerprint,receipt_id,finalization_evidence_id,
        finalization_command_id,finalization_fingerprint,handoff_id,committed_at
      ) VALUES (?,?,?,?,?,?,?,?)`,
    )
    .run(
      markerId,
      "a".repeat(64),
      receiptId,
      evidenceId,
      commandId,
      finalizationFingerprint,
      permit.handoffId,
      finalizedAt,
    );
};

const guardScenario =
  (
    matchingNativeTerminal: boolean,
    recoverPreInvoke = false,
    preparedSession: boolean | "retry-wait" = false,
    archiveObstacle?:
      | "unarchived"
      | "foreign-model"
      | "newer-delivery"
      | "accepted"
      | "attempted"
      | "attested"
      | "session-rebound",
  ) =>
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-admission-guard-" });
        const filename = path.join(directory, "guard.sqlite");
        const firstScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
        const firstContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          firstScope,
        );
        const sql = Context.get(firstContext, SqlClient.SqlClient);
        yield* sql`PRAGMA journal_mode=WAL`;
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const storeContext = yield* Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
          ),
          firstScope,
        );
        const store = Context.get(storeContext, ProviderAdmissionStore);
        const values = [
          request("initial-planning", ProviderInstanceId.make("guard-initial")),
          request("implementation", ProviderInstanceId.make("guard-implementation")),
          request("verification", ProviderInstanceId.make("guard-verification")),
        ];
        const permits: Array<ProviderAdmissionPermit> = [];
        for (const value of values) {
          const decision = yield* store.request({
            request: value,
            usage: providerAdmissionUsageEvidence({
              providerInstanceId: value.providerInstanceId,
              status: "allowed",
              observedAt: at,
              source: "refresh",
              nextRelevantAt: null,
            }),
            ownerId: `owner-${value.stage}`,
            leaseExpiresAt: recoverPreInvoke ? "2099-09-06T07:59:00.000Z" : leaseExpiry,
            now: at,
          });
          assert.equal(decision._tag, "Admitted");
          if (decision._tag === "Admitted") permits.push(decision.permit);
        }
        assert.equal(permits.length, 3);

        const deliveryTriggers = yield* sql<{ readonly name: string; readonly source: string }>`
        SELECT name,sql AS source FROM main.sqlite_schema
        WHERE type='trigger' AND tbl_name IN (
          'agent_control_stage_run_states',
          'agent_control_stage_run_lease_states',
          'agent_control_initial_planning_deliveries',
          'agent_control_implementation_deliveries',
          'agent_control_verification_deliveries',
          'agent_control_initial_planning_session_evidence',
          'agent_control_implementation_session_evidence',
          'agent_control_verification_session_evidence'
        ) AND sql IS NOT NULL
        ORDER BY name
      `;
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        // Existing tables, STRICT/check constraints, and the native DML boundary
        // remain active. Fixture-only parent FKs and transition triggers are
        // suspended, then every exact trigger source is restored before guard use.
        yield* sql`PRAGMA foreign_keys=OFF`;
        const fixtureDatabase = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const database = new NodeSqlite.DatabaseSync(filename);
            database.exec("PRAGMA foreign_keys=OFF");
            return database;
          }),
          (database) => Effect.sync(() => database.close()),
        );
        for (const value of values) {
          yield* sql.withTransaction(
            seedStageAndDelivery(sql, value, value.stage === "initial-planning"),
          );
          if (value.stage !== "initial-planning") {
            yield* Effect.sync(() => seedNonInitialDelivery(fixtureDatabase, value));
          }
        }
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(trigger.source).unprepared;
        }
        yield* sql`PRAGMA foreign_keys=ON`;

        const guardContext = yield* Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionGuardLive).pipe(
            Layer.provide(Layer.mock(AgentControlStageRunLeaseEngine)({})),
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
            Layer.provide(Layer.succeed(ProviderAdmissionStore, store)),
            Layer.provide(Layer.succeed(AgentControlTaskConsumerGuard, taskGuardShape)),
          ),
          firstScope,
        );
        const guard = Context.get(guardContext, ProviderAdmissionGuard);
        for (const permit of permits) {
          const table =
            permit.stage === "initial-planning"
              ? "agent_control_initial_planning_deliveries"
              : permit.stage === "implementation"
                ? "agent_control_implementation_deliveries"
                : "agent_control_verification_deliveries";
          const before = yield* sql.unsafe(
            `SELECT state,revision,claim_generation,attempt_count FROM main.${table}
           WHERE provider_delivery_id=?`,
            [permit.providerDeliveryId],
          );
          yield* guard.enter(permit, "session-start");
          const replay = yield* Effect.exit(guard.enter(permit, "session-start"));
          assert.isTrue(
            Exit.isSuccess(replay),
            Exit.isFailure(replay) ? Cause.pretty(replay.cause) : undefined,
          );
          yield* guard.enter(permit, "turn-start");
          assert.deepStrictEqual(
            yield* sql`
          SELECT status FROM agent_control_provider_admission_current
          WHERE admission_id=${permit.admissionId}
        `,
            [{ status: "admitted" }],
          );
          assert.deepStrictEqual(
            yield* sql.unsafe(
              `SELECT state,revision,claim_generation,attempt_count FROM main.${table}
             WHERE provider_delivery_id=?`,
              [permit.providerDeliveryId],
            ),
            before,
          );
          assert.equal(
            (yield* sql<{ readonly count: number }>`
            SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
            WHERE admission_id=${permit.admissionId}
              AND authority_kind IN ('session-entry','turn-entry')
          `)[0]?.count,
            1,
          );
        }

        // Exercise the second guard check after the consumer commits its delivery CAS.
        // Only fixture setup bypasses delivery transitions; all admission guards remain active.
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        for (const permit of permits) {
          const table =
            permit.stage === "initial-planning"
              ? "agent_control_initial_planning_deliveries"
              : permit.stage === "implementation"
                ? "agent_control_implementation_deliveries"
                : "agent_control_verification_deliveries";
          yield* Effect.sync(() =>
            fixtureDatabase
              .prepare(`
          UPDATE main.${table} SET state='delivery-attempted', revision=revision+1,
            attempt_count=1, provider_session_created_at=?, provider_resume_cursor_json='{}'
          WHERE provider_delivery_id=?
        `)
              .run(at, permit.providerDeliveryId),
          );
        }
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(trigger.source).unprepared;
        }
        for (const permit of permits) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(guard.enter(permit, "session-start"))));
          yield* guard.enter(permit, "turn-start");
        }
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        for (const permit of permits) {
          const table =
            permit.stage === "initial-planning"
              ? "agent_control_initial_planning_deliveries"
              : permit.stage === "implementation"
                ? "agent_control_implementation_deliveries"
                : "agent_control_verification_deliveries";
          yield* Effect.sync(() =>
            fixtureDatabase
              .prepare(`
          UPDATE main.${table} SET state='claimed' ${
            recoverPreInvoke
              ? ", provider_session_created_at=NULL, provider_resume_cursor_json=NULL, claim_expires_at='2099-09-06T07:59:00.000Z'"
              : ""
          } WHERE provider_delivery_id=?
        `)
              .run(permit.providerDeliveryId),
          );
        }
        for (const trigger of deliveryTriggers) {
          yield* sql.unsafe(trigger.source).unprepared;
        }

        if (preparedSession) {
          // Seed the pre-070 persisted preparation through the historical schema,
          // then restore its exact guards before executing the real upgrade.
          for (const trigger of deliveryTriggers) {
            if (trigger.name.includes("_session_evidence_"))
              yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
          }
          for (const value of values) {
            if (archiveObstacle === "unarchived") continue;
            const sessions = `agent_control_${value.stage.replaceAll("-", "_")}_session_evidence`;
            yield* Effect.sync(() =>
              fixtureDatabase
                .prepare(`INSERT INTO ${sessions}
              (provider_delivery_id,thread_id,provider_instance_id,runtime_mode,cwd,
               model_selection_json,model_selection_fingerprint,session_created_at,resume_cursor_json,recorded_at)
              VALUES (?,?,?,?,?,?,?,?,?,?)`)
                .run(
                  value.providerDeliveryId,
                  value.threadId,
                  value.providerInstanceId,
                  value.stage === "verification" ? "approval-required" : "full-access",
                  "/isolated/worktree",
                  value.modelSelectionJson,
                  archiveObstacle === "foreign-model"
                    ? "0".repeat(64)
                    : value.modelSelectionFingerprint,
                  at,
                  "{}",
                  at,
                ),
            );
          }
          for (const trigger of deliveryTriggers) {
            if (trigger.name.includes("_session_evidence_"))
              yield* sql.unsafe(trigger.source).unprepared;
          }
          if (preparedSession === "retry-wait" || archiveObstacle === "unarchived") {
            for (const value of values) {
              const table = `agent_control_${value.stage.replaceAll("-", "_")}_deliveries`;
              yield* sql.withTransaction(
                sql.unsafe(
                  `UPDATE ${table} SET state='retry-wait',revision=revision+1,
                claim_owner_id=NULL,claim_expires_at=NULL,next_attempt_at=?,last_error_code='transient-not-accepted'
                WHERE provider_delivery_id=?`,
                  [at, value.providerDeliveryId],
                ),
              );
            }
          }
        }

        const wrongProvider = yield* Effect.exit(
          guard.enter(
            { ...permits[0]!, providerInstanceId: ProviderInstanceId.make("foreign-provider") },
            "turn-start",
          ),
        );
        const wrongModel = yield* Effect.exit(
          guard.enter({ ...permits[0]!, modelSelectionFingerprint: "0".repeat(64) }, "turn-start"),
        );
        assert.isTrue(Exit.isFailure(wrongProvider));
        assert.isTrue(Exit.isFailure(wrongModel));

        const secondScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
        const secondSqlContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          secondScope,
        );
        const secondSql = Context.get(secondSqlContext, SqlClient.SqlClient);
        yield* secondSql`PRAGMA foreign_keys=ON`;
        if (recoverPreInvoke) {
          // The fixture above reproduces the old pre-CAS guard: entry evidence
          // exists, but no delivery attestation or session correlation was committed.
          if (preparedSession) {
            yield* runMigrations({ toMigrationInclusive: 70 }).pipe(
              Effect.provideService(SqlClient.SqlClient, secondSql),
            );
            assert.equal(
              (yield* secondSql`SELECT * FROM agent_control_prepared_session_archive`).length,
              archiveObstacle === "unarchived" ? 0 : 3,
            );
            for (const value of values) {
              const table = `agent_control_${value.stage.replaceAll("-", "_")}_deliveries`;
              assert.equal(
                (yield* secondSql.unsafe<{ state: string }>(`SELECT state FROM ${table}`))[0]
                  ?.state,
                "retry-wait",
              );
            }
          }
          if (
            archiveObstacle !== undefined &&
            !["unarchived", "foreign-model"].includes(archiveObstacle)
          ) {
            // Deliberately divergent post-upgrade snapshots exercise the runtime proof;
            // restore every exact DDL guard before opening the restarted store.
            const fixtureTriggers = yield* secondSql<{
              name: string;
              source: string;
            }>`SELECT name,sql AS source FROM sqlite_schema
              WHERE type='trigger' AND (tbl_name LIKE 'agent_control_%_deliveries'
                OR tbl_name LIKE 'agent_control_%_session_evidence' OR tbl_name LIKE 'agent_control_%_delivery_attestations')`;
            for (const trigger of fixtureTriggers)
              yield* secondSql.unsafe(`DROP TRIGGER "${trigger.name}"`).unprepared;
            yield* Effect.sync(() => {
              for (const value of values) {
                const prefix = `agent_control_${value.stage.replaceAll("-", "_")}`;
                if (archiveObstacle === "attested")
                  fixtureDatabase
                    .prepare(`INSERT INTO ${prefix}_delivery_attestations
                  (provider_delivery_id,provider_instance_id,model_selection_json,model_selection_fingerprint,recorded_at)
                  VALUES (?,?,?,?,?)`)
                    .run(
                      value.providerDeliveryId,
                      value.providerInstanceId,
                      value.modelSelectionJson,
                      value.modelSelectionFingerprint,
                      at,
                    );
                else if (archiveObstacle === "session-rebound")
                  fixtureDatabase
                    .prepare(`INSERT INTO ${prefix}_session_evidence
                  (provider_delivery_id,thread_id,provider_instance_id,runtime_mode,cwd,model_selection_json,model_selection_fingerprint,session_created_at,resume_cursor_json,recorded_at)
                  VALUES (?,?,?,?,?,?,?,?,?,?)`)
                    .run(
                      value.providerDeliveryId,
                      value.threadId,
                      value.providerInstanceId,
                      value.stage === "verification" ? "approval-required" : "full-access",
                      "/isolated/worktree",
                      value.modelSelectionJson,
                      value.modelSelectionFingerprint,
                      at,
                      "{}",
                      at,
                    );
                else
                  fixtureDatabase
                    .prepare(
                      `UPDATE ${prefix}_deliveries SET ${
                        archiveObstacle === "newer-delivery"
                          ? "revision=revision+2,claim_generation=claim_generation+1"
                          : archiveObstacle === "accepted"
                            ? "state='provider-started',next_attempt_at=NULL,last_error_code=NULL,provider_turn_id='accepted-native-turn',provider_accepted_at='2099-09-06T07:59:00.000Z',provider_session_created_at='2099-09-06T07:59:00.000Z',provider_resume_cursor_json='{}'"
                            : "state='delivery-attempted',next_attempt_at=NULL,claim_owner_id='new-attempted-owner',claim_expires_at='2099-09-06T07:59:00.000Z',provider_session_created_at='2099-09-06T07:59:00.000Z',provider_resume_cursor_json='{}'"
                      } WHERE provider_delivery_id=?`,
                    )
                    .run(value.providerDeliveryId);
              }
            });
            for (const trigger of fixtureTriggers)
              yield* secondSql.unsafe(trigger.source).unprepared;
          }
          if (preparedSession === true && archiveObstacle === undefined) {
            const name = "agent_control_provider_admission_current_validate_update";
            const source = (yield* secondSql<{
              source: string;
            }>`SELECT sql AS source FROM sqlite_schema WHERE name=${name}`)[0]!.source;
            const divergent = source.replace("BEFORE UPDATE", "BEFORE /* divergent */ UPDATE");
            yield* secondSql.unsafe(`DROP TRIGGER ${name}`).unprepared;
            yield* secondSql.unsafe(divergent).unprepared;
            const refused = yield* Effect.exit(
              runMigrations({ toMigrationInclusive: 72 }).pipe(
                Effect.provideService(SqlClient.SqlClient, secondSql),
              ),
            );
            assert.isTrue(Exit.isFailure(refused));
            assert.equal(
              (yield* secondSql`SELECT * FROM effect_sql_agent_control_migrations WHERE migration_id=72`)
                .length,
              0,
            );
            assert.equal(
              (yield* secondSql`SELECT * FROM sqlite_schema WHERE name='agent_control_prepared_session_archive_no_insert'`)
                .length,
              0,
            );
            assert.equal(
              (yield* secondSql<{
                source: string;
              }>`SELECT sql AS source FROM sqlite_schema WHERE name=${name}`)[0]!.source,
              divergent,
            );
            yield* secondSql.unsafe(`DROP TRIGGER ${name}`).unprepared;
            yield* secondSql.unsafe(source).unprepared;
          }
          // These admission/archive fixtures intentionally omit parent orchestration
          // rows. Stop at the archive-hardening migration under test, before 073
          // rebuilds unrelated Repair tables and validates their foreign keys.
          yield* runMigrations({ toMigrationInclusive: preparedSession ? 72 : 68 }).pipe(
            Effect.provideService(SqlClient.SqlClient, secondSql),
          );
        }
        const secondStoreLayer = Layer.fresh(ProviderAdmissionStoreLive).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, secondSql)),
        );
        const releaseContext = yield* Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionReleaseAuthorityLive).pipe(
            Layer.provideMerge(secondStoreLayer),
            Layer.provide(
              Layer.succeed(ProviderAdmissionRuntime, {
                awaitFailure: Effect.never,
                request: () => Effect.die("not used"),
                usageChanged: () => Effect.die("not used"),
                capacityReleased: () => Effect.void,
              }),
            ),
          ),
          secondScope,
        );
        const releaseAuthority = Context.get(releaseContext, ProviderAdmissionReleaseAuthority);
        const secondStore = Context.get(releaseContext, ProviderAdmissionStore);
        yield* releaseAuthority.recover;
        assert.deepStrictEqual(
          yield* secondSql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          ORDER BY provider_instance_id
        `,
          [{ status: "quarantined" }, { status: "quarantined" }, { status: "quarantined" }],
        );
        if (recoverPreInvoke) {
          const recoveryAt = "2099-09-06T07:59:01.000Z";
          if (archiveObstacle !== undefined) {
            assert.equal(yield* secondStore.minimumDeadline, null);
            assert.deepStrictEqual(yield* secondStore.listDueDeadlines(recoveryAt), []);
            for (const value of values) {
              const denied = yield* secondStore.request({
                request: value,
                usage: providerAdmissionUsageEvidence({
                  providerInstanceId: value.providerInstanceId,
                  status: "allowed",
                  observedAt: at,
                  source: "refresh",
                  nextRelevantAt: null,
                }),
                ownerId: "unsafe-recovery",
                now: recoveryAt,
                leaseExpiresAt: leaseExpiry,
              });
              assert.equal(denied?._tag, "Waiting");
            }
            assert.deepStrictEqual(
              yield* secondSql`SELECT provider_fence_token AS fence FROM agent_control_provider_admission_current ORDER BY provider_instance_id`,
              [{ fence: 1 }, { fence: 1 }, { fence: 1 }],
            );
            return;
          }
          assert.equal(yield* secondStore.minimumDeadline, "2099-09-06T07:59:00.000Z");
          assert.equal((yield* secondStore.listDueDeadlines(recoveryAt)).length, 3);
          for (const [index, value] of values.entries()) {
            const old = permits[index]!;
            const recovered = yield* secondStore.request({
              request: value,
              usage: providerAdmissionUsageEvidence({
                providerInstanceId: value.providerInstanceId,
                status: "allowed",
                observedAt: at,
                source: "refresh",
                nextRelevantAt: null,
              }),
              ownerId: `restarted-${value.stage}`,
              now: recoveryAt,
              leaseExpiresAt: leaseExpiry,
            });
            assert.equal(recovered._tag, "Admitted");
            if (recovered._tag !== "Admitted") return;
            assert.equal(recovered.permit.providerFenceToken, old.providerFenceToken + 1);
            assert.isTrue(
              Exit.isFailure(
                yield* Effect.exit(
                  secondSql.withTransaction(
                    secondStore.validateAndEnterInTransaction({
                      permit: old,
                      boundary: "turn-start",
                      enteredAt: recoveryAt,
                    }),
                  ),
                ),
              ),
            );
            const table =
              value.stage === "initial-planning"
                ? "agent_control_initial_planning_deliveries"
                : value.stage === "implementation"
                  ? "agent_control_implementation_deliveries"
                  : "agent_control_verification_deliveries";
            assert.deepStrictEqual(
              yield* secondSql.unsafe(
                `SELECT state,claim_owner_id FROM ${table} WHERE provider_delivery_id=?`,
                [value.providerDeliveryId],
              ),
              [{ state: "retry-wait", claim_owner_id: null }],
            );
            assert.deepStrictEqual(
              yield* secondSql.withTransaction(
                secondSql.unsafe(
                  `UPDATE ${table} SET state='delivery-attempted',revision=revision+1
            WHERE provider_delivery_id=? AND state='claimed' AND claim_owner_id='delivery-owner' RETURNING state`,
                  [value.providerDeliveryId],
                ),
              ),
              [],
            );
          }
          // Upgrade an already persisted 068 recovery as well as a 070 archive;
          // startup must continue validating both immutable authority shapes.
          yield* runMigrations({ toMigrationInclusive: 72 }).pipe(
            Effect.provideService(SqlClient.SqlClient, secondSql),
          );
          if (preparedSession) {
            for (const statement of [
              "DELETE FROM agent_control_prepared_session_archive",
              "UPDATE agent_control_prepared_session_archive SET reason=reason",
              "INSERT INTO agent_control_prepared_session_archive SELECT * FROM agent_control_prepared_session_archive",
            ])
              assert.isTrue(Exit.isFailure(yield* Effect.exit(secondSql.unsafe(statement))));
          }
          const revalidated = yield* Layer.buildWithScope(
            Layer.fresh(secondStoreLayer),
            secondScope,
          );
          assert.equal(
            (yield* Context.get(revalidated, ProviderAdmissionStore).listEnteredWithoutRelease)
              .length,
            0,
          );
          return;
        }
        // An expired permit alone cannot justify replay after a persisted invocation
        // boundary. The fixture retains its session correlation from attempted CAS.
        for (const value of values) {
          const attempted = yield* secondStore.request({
            request: value,
            usage: providerAdmissionUsageEvidence({
              providerInstanceId: value.providerInstanceId,
              status: "allowed",
              observedAt: at,
              source: "refresh",
              nextRelevantAt: null,
            }),
            ownerId: "expired-attempted-owner",
            now: "2100-01-01T00:00:00.000Z",
            leaseExpiresAt: "2100-01-01T00:01:00.000Z",
          });
          assert.equal(attempted._tag, "Waiting");
        }
        const blocked = yield* Context.get(releaseContext, ProviderAdmissionStore).request({
          request: {
            ...request("implementation", ProviderInstanceId.make("guard-initial")),
            projectId: "project-blocked",
            taskId: "task-blocked",
            stageRunId: "stage-blocked",
            attemptId: "attempt-blocked",
            handoffId: "handoff-blocked",
            providerDeliveryId: "delivery-blocked",
            threadId: "thread-blocked",
            stageLeaseId: "lease-blocked",
            stageLeaseHolderId: "holder-blocked",
          },
          usage: providerAdmissionUsageEvidence({
            providerInstanceId: ProviderInstanceId.make("guard-initial"),
            status: "allowed",
            observedAt: at,
            source: "refresh",
            nextRelevantAt: null,
          }),
          ownerId: "owner-blocked",
          leaseExpiresAt: leaseExpiry,
          now: at,
        });
        assert.equal(blocked._tag, "Waiting");

        const finalizationTriggers = yield* secondSql<{
          readonly name: string;
          readonly source: string;
        }>`
        SELECT name,sql AS source FROM main.sqlite_schema
        WHERE type='trigger' AND tbl_name IN (
          'agent_control_initial_planning_result_evidence',
          'agent_control_initial_planning_finalization_receipts',
          'agent_control_initial_planning_finalization_markers',
          'agent_control_verification_finalization_evidence',
          'agent_control_verification_finalization_receipts',
          'agent_control_verification_finalization_markers'
        ) AND sql IS NOT NULL
        ORDER BY name
      `;
        for (const trigger of finalizationTriggers) {
          yield* secondSql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        const finalizedAt = "2026-09-06T09:00:00.000Z";
        const releasePermit = permits[0]!;
        const terminalEventId = `terminal-event-${releasePermit.admissionId}`;
        const terminalCommandId = `terminal-command-${releasePermit.admissionId}`;
        const terminalMetadata = canonicalJson({
          providerRuntimeLifecycle: {
            runtimeEventType: "turn.completed",
            providerState: "failed",
            providerInstanceId: releasePermit.providerInstanceId,
            providerTurnId: `provider-turn-${releasePermit.admissionId}`,
            runtimeEventId: `runtime-terminal-${releasePermit.admissionId}`,
          },
        });
        const terminalPayload = canonicalJson({
          session: {
            activeTurnId: null,
            lastError: "provider failed",
            providerInstanceId: releasePermit.providerInstanceId,
            providerName: "codex",
            runtimeMode: "approval-required",
            status: "error",
            threadId: releasePermit.threadId,
            updatedAt: finalizedAt,
          },
          threadId: releasePermit.threadId,
        });
        yield* secondSql`
        INSERT INTO main.orchestration_events (
          event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,
          command_id,causation_event_id,correlation_id,actor_kind,payload_json,metadata_json
        ) VALUES (
          ${terminalEventId},'thread',${releasePermit.threadId},1,'thread.session-set',
          ${finalizedAt},${terminalCommandId},NULL,${terminalCommandId},'provider',${terminalPayload},
          ${terminalMetadata}
        )
      `;
        yield* Effect.sync(() =>
          seedInitialPlanningFinalization(fixtureDatabase, releasePermit, finalizedAt),
        );
        const verificationPermit = permits[2]!;
        const verificationTerminalEventId = `terminal-event-${verificationPermit.admissionId}`;
        const verificationTerminalMetadata = canonicalJson({
          providerRuntimeMessage: {
            eventType: "turn.completed",
            providerInstanceId: verificationPermit.providerInstanceId,
            providerItemId: null,
            providerTurnId: matchingNativeTerminal
              ? `provider-turn-${verificationPermit.admissionId}`
              : `foreign-provider-turn-${verificationPermit.admissionId}`,
            runtimeEventId: `native-runtime-event-${verificationPermit.admissionId}`,
          },
        });
        const verificationTerminalPayload = canonicalJson({
          session: {
            activeTurnId: null,
            lastError: "provider failed",
            providerInstanceId: verificationPermit.providerInstanceId,
            providerName: "codex",
            runtimeMode: "approval-required",
            status: "error",
            threadId: verificationPermit.threadId,
            updatedAt: finalizedAt,
          },
          threadId: verificationPermit.threadId,
        });
        yield* secondSql`
        INSERT INTO main.orchestration_events (
          event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,
          command_id,causation_event_id,correlation_id,actor_kind,payload_json,metadata_json
        ) VALUES (
          ${verificationTerminalEventId},'thread',${verificationPermit.threadId},1,
          'thread.session-set',${finalizedAt},
          ${`terminal-command-${verificationPermit.admissionId}`},NULL,
          ${`terminal-command-${verificationPermit.admissionId}`},'provider',
          ${verificationTerminalPayload},${verificationTerminalMetadata}
        )
      `;
        yield* Effect.sync(() =>
          seedVerificationFinalization(
            fixtureDatabase,
            verificationPermit,
            finalizedAt,
            matchingNativeTerminal
              ? `native-runtime-event-${verificationPermit.admissionId}`
              : verificationTerminalEventId,
          ),
        );
        for (const trigger of finalizationTriggers) {
          yield* secondSql.unsafe(trigger.source).unprepared;
        }

        const verificationRelease = yield* Effect.exit(
          secondSql.withTransaction(
            secondStore.releaseFromFinalizationInTransaction({
              stage: "verification",
              handoffId: verificationPermit.handoffId,
              finalizedAt,
            }),
          ),
        );
        assert.equal(Exit.isSuccess(verificationRelease), matchingNativeTerminal);
        assert.deepStrictEqual(
          yield* secondSql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE admission_id=${verificationPermit.admissionId}
        `,
          [{ status: matchingNativeTerminal ? "released" : "quarantined" }],
        );
        assert.equal(
          (yield* secondSql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${verificationPermit.admissionId} AND authority_kind='release'
        `)[0]?.count,
          matchingNativeTerminal ? 1 : 0,
        );

        const wrongTerminal = yield* Effect.exit(
          secondSql.withTransaction(
            secondStore.releaseFromFinalizationInTransaction({
              stage: "initial-planning",
              handoffId: permits[0]!.handoffId,
              finalizedAt: "2026-09-06T09:00:01.000Z",
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(wrongTerminal));
        const rolledBack = yield* Effect.exit(
          secondSql.withTransaction(
            secondStore
              .releaseFromFinalizationInTransaction({
                stage: "initial-planning",
                handoffId: permits[0]!.handoffId,
                finalizedAt,
              })
              .pipe(Effect.andThen(Effect.fail("force-outer-rollback"))),
          ),
        );
        assert.isTrue(Exit.isFailure(rolledBack));
        assert.deepStrictEqual(
          yield* secondSql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE admission_id=${permits[0]!.admissionId}
        `,
          [{ status: "quarantined" }],
        );
        assert.equal(
          (yield* secondSql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${permits[0]!.admissionId} AND authority_kind='release'
        `)[0]?.count,
          0,
        );
        assert.equal(
          yield* secondSql.withTransaction(
            secondStore.releaseFromFinalizationInTransaction({
              stage: "initial-planning",
              handoffId: permits[0]!.handoffId,
              finalizedAt,
            }),
          ),
          permits[0]!.providerInstanceId,
        );
        assert.equal(
          yield* secondSql.withTransaction(
            secondStore.releaseFromFinalizationInTransaction({
              stage: "initial-planning",
              handoffId: permits[0]!.handoffId,
              finalizedAt,
            }),
          ),
          permits[0]!.providerInstanceId,
        );
        assert.deepStrictEqual(
          yield* secondSql<{
            readonly status: string;
            readonly activeAdmissionId: string | null;
            readonly lastFenceToken: number;
          }>`
          SELECT admission.status,capacity.active_admission_id AS "activeAdmissionId",
            capacity.last_fence_token AS "lastFenceToken"
          FROM main.agent_control_provider_admission_current admission
          JOIN main.agent_control_provider_capacity_current capacity
            ON capacity.provider_instance_id=admission.provider_instance_id
          WHERE admission.admission_id=${permits[0]!.admissionId}
        `,
          [{ status: "released", activeAdmissionId: null, lastFenceToken: 1 }],
        );
        assert.equal(
          (yield* secondSql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${permits[0]!.admissionId} AND authority_kind='release'
        `)[0]?.count,
          1,
        );
        yield* secondStore.quarantineIfEntered({
          permit: permits[0]!,
          observedAt: "2026-09-06T09:00:01.000Z",
        });
        assert.deepStrictEqual(
          yield* secondSql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE admission_id=${permits[0]!.admissionId}
        `,
          [{ status: "released" }],
        );
        assert.equal(
          (yield* secondSql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${permits[0]!.admissionId} AND authority_kind='quarantine'
        `)[0]?.count,
          1,
        );
        const nextPermit = yield* secondStore.admitOldest({
          providerInstanceId: String(permits[0]!.providerInstanceId),
          ownerId: "release-successor-owner",
          now: "2098-09-06T09:00:01.000Z",
          leaseExpiresAt: leaseExpiry,
        });
        if (nextPermit === null) return yield* Effect.die("missing release successor");
        assert.equal(nextPermit.handoffId, "handoff-blocked");
        assert.equal(nextPermit.providerFenceToken, 2);
        assert.deepStrictEqual(
          yield* secondSql<{
            readonly activeAdmissionId: string | null;
            readonly activeFenceToken: number | null;
            readonly lastFenceToken: number;
          }>`
          SELECT active_admission_id AS "activeAdmissionId",
            active_fence_token AS "activeFenceToken",last_fence_token AS "lastFenceToken"
          FROM main.agent_control_provider_capacity_current
          WHERE provider_instance_id=${String(permits[0]!.providerInstanceId)}
        `,
          [
            {
              activeAdmissionId: nextPermit.admissionId,
              activeFenceToken: 2,
              lastFenceToken: 2,
            },
          ],
        );
        assert.equal(
          yield* secondSql.withTransaction(
            secondStore.releaseFromFinalizationInTransaction({
              stage: "initial-planning",
              handoffId: permits[0]!.handoffId,
              finalizedAt,
            }),
          ),
          permits[0]!.providerInstanceId,
        );
        assert.deepStrictEqual(
          yield* secondSql<{ readonly activeAdmissionId: string | null }>`
          SELECT active_admission_id AS "activeAdmissionId"
          FROM main.agent_control_provider_capacity_current
          WHERE provider_instance_id=${String(permits[0]!.providerInstanceId)}
        `,
          [{ activeAdmissionId: nextPermit.admissionId }],
        );

        const terminalRestartScope = yield* Scope.make("sequential");
        const terminalRestartSqlContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          terminalRestartScope,
        );
        const terminalRestartSql = Context.get(terminalRestartSqlContext, SqlClient.SqlClient);
        yield* terminalRestartSql`PRAGMA foreign_keys=ON`;
        const terminalRestart = yield* Effect.exit(
          Layer.buildWithScope(
            Layer.fresh(ProviderAdmissionStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, terminalRestartSql)),
            ),
            terminalRestartScope,
          ),
        );
        assert.isTrue(Exit.isSuccess(terminalRestart));
        yield* Scope.close(terminalRestartScope, Exit.void);

        // Remove the intentionally parent-less fixture deliveries before the FK
        // audit. Their production immutability triggers are restored immediately
        // afterwards, just as they were before guard execution.
        for (const trigger of deliveryTriggers) {
          yield* secondSql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        for (const trigger of finalizationTriggers) {
          yield* secondSql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        yield* Effect.sync(() => {
          fixtureDatabase.exec(`
          DELETE FROM main.agent_control_initial_planning_finalization_markers;
          DELETE FROM main.agent_control_initial_planning_finalization_receipts;
          DELETE FROM main.agent_control_initial_planning_result_evidence;
          DELETE FROM main.agent_control_verification_finalization_markers;
          DELETE FROM main.agent_control_verification_finalization_receipts;
          DELETE FROM main.agent_control_verification_finalization_evidence;
          DELETE FROM main.agent_control_initial_planning_deliveries;
          DELETE FROM main.agent_control_implementation_deliveries;
          DELETE FROM main.agent_control_verification_deliveries;
        `);
        });
        for (const trigger of deliveryTriggers) {
          yield* secondSql.unsafe(trigger.source).unprepared;
        }
        for (const trigger of finalizationTriggers) {
          yield* secondSql.unsafe(trigger.source).unprepared;
        }
        assert.deepStrictEqual(yield* secondSql`PRAGMA main.foreign_key_check`, []);
        assert.equal((yield* secondSql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
      }),
    ).pipe(Effect.provide(NodeServices.layer));

it.live(
  "guards all stage effects, revalidates replay, and quarantines restart ambiguity",
  guardScenario(false),
);
it.live(
  "releases verification capacity using distinct native and orchestration event IDs",
  guardScenario(true),
);

it.live(
  "fails restart closed for entered and terminal projections without their complete authority chain",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-admission-forged-current-",
        });

        for (const status of ["entered", "quarantined", "released", "superseded"] as const) {
          const filename = path.join(directory, `${status}.sqlite`);
          const firstScope = yield* Scope.make("sequential");
          const firstContext = yield* Layer.buildWithScope(
            NodeSqliteClient.layer({ filename }),
            firstScope,
          );
          const sql = Context.get(firstContext, SqlClient.SqlClient);
          yield* sql`PRAGMA journal_mode=WAL`;
          yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          );
          const storeContext = yield* Layer.buildWithScope(
            Layer.fresh(ProviderAdmissionStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
            ),
            firstScope,
          );
          const store = Context.get(storeContext, ProviderAdmissionStore);
          const providerInstanceId = ProviderInstanceId.make(`forged-${status}`);
          const value = request("initial-planning", providerInstanceId);
          const decision = yield* store.request({
            request: {
              ...value,
              projectId: `project-${status}`,
              taskId: `task-${status}`,
              stageRunId: `stage-${status}`,
              attemptId: `attempt-${status}`,
              handoffId: `handoff-${status}`,
              providerDeliveryId: `delivery-${status}`,
              threadId: `thread-${status}`,
              stageLeaseId: `lease-${status}`,
              stageLeaseHolderId: `holder-${status}`,
            },
            usage: providerAdmissionUsageEvidence({
              providerInstanceId,
              status: "allowed",
              observedAt: at,
              source: "refresh",
              nextRelevantAt: null,
            }),
            ownerId: `owner-${status}`,
            leaseExpiresAt: leaseExpiry,
            now: at,
          });
          assert.equal(decision._tag, "Admitted");
          if (decision._tag !== "Admitted") continue;

          const validationTriggers = yield* sql<{
            readonly name: string;
            readonly source: string;
          }>`
            SELECT name,sql AS source FROM main.sqlite_schema
            WHERE name IN (
              'agent_control_provider_admission_current_validate_update',
              'agent_control_provider_capacity_current_validate_update'
            ) AND sql IS NOT NULL
            ORDER BY name
          `;
          assert.equal(validationTriggers.length, 2);
          for (const trigger of validationTriggers) {
            yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
          }
          yield* sql`
            UPDATE main.agent_control_provider_admission_current
            SET status=${status},revision=revision+1,updated_at=${at}
            WHERE admission_id=${decision.permit.admissionId}
          `;
          if (status === "entered" || status === "quarantined") {
            yield* sql`
              UPDATE main.agent_control_provider_capacity_current
              SET active_state=${status},revision=revision+1,updated_at=${at}
              WHERE provider_instance_id=${providerInstanceId}
            `;
          } else {
            yield* sql`
              UPDATE main.agent_control_provider_capacity_current SET
                active_admission_id=NULL,active_state=NULL,active_owner_id=NULL,
                active_lease_expires_at=NULL,active_fence_token=NULL,
                active_marker_fingerprint=NULL,revision=revision+1,updated_at=${at}
              WHERE provider_instance_id=${providerInstanceId}
            `;
          }
          for (const trigger of validationTriggers) {
            yield* sql.unsafe(trigger.source).unprepared;
          }
          assert.deepStrictEqual(
            yield* sql<{ readonly name: string; readonly source: string }>`
              SELECT name,sql AS source FROM main.sqlite_schema
              WHERE name IN (
                'agent_control_provider_admission_current_validate_update',
                'agent_control_provider_capacity_current_validate_update'
              ) AND sql IS NOT NULL
              ORDER BY name
            `,
            validationTriggers,
          );
          assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
          assert.equal((yield* sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
          yield* Scope.close(firstScope, Exit.void);

          const restartScope = yield* Scope.make("sequential");
          const restartSqlContext = yield* Layer.buildWithScope(
            NodeSqliteClient.layer({ filename }),
            restartScope,
          );
          const restartSql = Context.get(restartSqlContext, SqlClient.SqlClient);
          yield* restartSql`PRAGMA foreign_keys=ON`;
          const restarted = yield* Effect.exit(
            Layer.buildWithScope(
              Layer.fresh(ProviderAdmissionStoreLive).pipe(
                Layer.provide(Layer.succeed(SqlClient.SqlClient, restartSql)),
              ),
              restartScope,
            ),
          );
          assert.isTrue(Exit.isFailure(restarted), status);
          yield* Scope.close(restartScope, Exit.void);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("supersedes deadline-finalized waiting and admitted pre-entry work transactionally", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-admission-cancel-" });
      const filename = path.join(directory, "cancel.sqlite");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const sqlContext = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
      const sql = Context.get(sqlContext, SqlClient.SqlClient);
      yield* sql`PRAGMA journal_mode=WAL`;
      yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
      const storeContext = yield* Layer.buildWithScope(
        Layer.fresh(ProviderAdmissionStoreLive).pipe(
          Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
        ),
        scope,
      );
      const store = Context.get(storeContext, ProviderAdmissionStore);
      const signals = yield* Ref.make<ReadonlyArray<string>>([]);
      const releaseContext = yield* Layer.buildWithScope(
        Layer.fresh(ProviderAdmissionReleaseAuthorityLive).pipe(
          Layer.provide(Layer.succeed(ProviderAdmissionStore, store)),
          Layer.provide(
            Layer.succeed(
              ProviderAdmissionRuntime,
              ProviderAdmissionRuntime.of({
                awaitFailure: Effect.never,
                request: () => Effect.die("unused"),
                usageChanged: () => Effect.die("unused"),
                capacityReleased: (providerInstanceId) =>
                  Ref.update(signals, (current) => [...current, providerInstanceId]),
              }),
            ),
          ),
        ),
        scope,
      );
      const release = Context.get(releaseContext, ProviderAdmissionReleaseAuthority);
      const makeRequest = (suffix: string, providerInstanceId: ProviderInstanceId) => {
        const base = request("initial-planning", providerInstanceId);
        return {
          ...base,
          projectId: `project-${suffix}`,
          taskId: `task-${suffix}`,
          stageRunId: `stage-${suffix}`,
          attemptId: `attempt-${suffix}`,
          handoffId: `handoff-${suffix}`,
          providerDeliveryId: `delivery-${suffix}`,
          threadId: `thread-${suffix}`,
          stageLeaseId: `lease-${suffix}`,
          stageLeaseHolderId: `holder-${suffix}`,
        } satisfies ProviderAdmissionRequest;
      };
      const waitingRequest = makeRequest(
        "deadline-waiting",
        ProviderInstanceId.make("deadline-waiting-provider"),
      );
      const waitingDecision = yield* store.request({
        request: waitingRequest,
        usage: providerAdmissionUsageEvidence({
          providerInstanceId: waitingRequest.providerInstanceId,
          status: "rejected",
          observedAt: at,
          source: "refresh",
          nextRelevantAt: leaseExpiry,
        }),
        ownerId: "waiting-owner",
        leaseExpiresAt: leaseExpiry,
        now: at,
      });
      assert.equal(waitingDecision._tag, "Waiting");
      if (waitingDecision._tag !== "Waiting") return;
      const waitingPermit: ProviderAdmissionPermit = {
        ...waitingRequest,
        admissionId: waitingDecision.admissionId,
        admissionMarkerId: "not-admitted",
        admissionMarkerFingerprint: "0".repeat(64),
        admissionOwnerId: "not-admitted",
        admissionLeaseExpiresAt: leaseExpiry,
        providerFenceToken: 0,
        usageEvidenceFingerprint: "0".repeat(64),
      };
      const admittedRequest = makeRequest(
        "deadline-admitted",
        ProviderInstanceId.make("deadline-admitted-provider"),
      );
      const admittedDecision = yield* store.request({
        request: admittedRequest,
        usage: providerAdmissionUsageEvidence({
          providerInstanceId: admittedRequest.providerInstanceId,
          status: "allowed",
          observedAt: at,
          source: "refresh",
          nextRelevantAt: null,
        }),
        ownerId: "admitted-owner",
        leaseExpiresAt: leaseExpiry,
        now: at,
      });
      assert.equal(admittedDecision._tag, "Admitted");
      if (admittedDecision._tag !== "Admitted") return;

      const finalizationTriggers = yield* sql<{ readonly name: string; readonly source: string }>`
        SELECT name,sql AS source FROM main.sqlite_schema
        WHERE type='trigger' AND tbl_name IN (
          'agent_control_initial_planning_result_evidence',
          'agent_control_initial_planning_finalization_receipts',
          'agent_control_initial_planning_finalization_markers'
        ) AND sql IS NOT NULL
        ORDER BY name
      `;
      for (const trigger of finalizationTriggers) {
        yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
      }
      const fixtureDatabase = yield* Effect.acquireRelease(
        Effect.sync(() => {
          const database = new NodeSqlite.DatabaseSync(filename);
          database.exec("PRAGMA foreign_keys=OFF");
          return database;
        }),
        (database) => Effect.sync(() => database.close()),
      );
      const finalizedAt = "2026-09-06T09:00:00.000Z";
      for (const [index, permit] of [waitingPermit, admittedDecision.permit].entries()) {
        yield* sql`
          INSERT INTO main.orchestration_events (
            event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,
            command_id,causation_event_id,correlation_id,actor_kind,payload_json,metadata_json
          ) VALUES (
            ${`terminal-event-${permit.admissionId}`},'thread',${permit.threadId},1,
            'thread.session-set',${finalizedAt},${`terminal-command-${permit.admissionId}`},
            NULL,${`terminal-command-${permit.admissionId}`},'server',
            ${canonicalJson({
              session: {
                activeTurnId: null,
                lastError: "planning deadline exceeded",
                providerInstanceId: permit.providerInstanceId,
                providerName: "codex",
                runtimeMode: "approval-required",
                status: "error",
                threadId: permit.threadId,
                updatedAt: finalizedAt,
              },
              threadId: permit.threadId,
            })},
            ${canonicalJson({})}
          )
        `;
        yield* Effect.sync(() =>
          seedInitialPlanningFinalization(fixtureDatabase, permit, finalizedAt, index + 1),
        );
      }

      const rolledBack = yield* Effect.exit(
        sql.withTransaction(
          release
            .releaseInTransaction({
              stage: "initial-planning",
              handoffId: waitingPermit.handoffId,
              finalizedAt,
            })
            .pipe(Effect.andThen(Effect.fail("force-cancel-rollback"))),
        ),
      );
      assert.isTrue(Exit.isFailure(rolledBack));
      assert.deepStrictEqual(
        yield* sql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE admission_id=${waitingPermit.admissionId}
        `,
        [{ status: "waiting" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${waitingPermit.admissionId} AND authority_kind='supersede'
        `)[0]?.count,
        0,
      );
      assert.deepStrictEqual(yield* Ref.get(signals), []);

      const waitingProvider = yield* sql.withTransaction(
        release.releaseInTransaction({
          stage: "initial-planning",
          handoffId: waitingPermit.handoffId,
          finalizedAt,
        }),
      );
      assert.deepStrictEqual(yield* Ref.get(signals), []);
      yield* release.signalCommitted(waitingProvider);
      assert.deepStrictEqual(yield* Ref.get(signals), [String(waitingPermit.providerInstanceId)]);
      assert.equal(
        yield* sql.withTransaction(
          release.releaseInTransaction({
            stage: "initial-planning",
            handoffId: waitingPermit.handoffId,
            finalizedAt,
          }),
        ),
        waitingPermit.providerInstanceId,
      );
      const divergent = yield* Effect.exit(
        sql.withTransaction(
          release.releaseInTransaction({
            stage: "initial-planning",
            handoffId: waitingPermit.handoffId,
            finalizedAt: "2026-09-06T09:00:01.000Z",
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(divergent));

      const admittedProvider = yield* sql.withTransaction(
        release.releaseInTransaction({
          stage: "initial-planning",
          handoffId: admittedDecision.permit.handoffId,
          finalizedAt,
        }),
      );
      yield* release.signalCommitted(admittedProvider);
      assert.deepStrictEqual(
        yield* sql<{
          readonly status: string;
          readonly activeAdmissionId: string | null;
        }>`
          SELECT admission.status,capacity.active_admission_id AS "activeAdmissionId"
          FROM main.agent_control_provider_admission_current admission
          JOIN main.agent_control_provider_capacity_current capacity
            ON capacity.provider_instance_id=admission.provider_instance_id
          WHERE admission.admission_id=${admittedDecision.permit.admissionId}
        `,
        [{ status: "superseded", activeAdmissionId: null }],
      );
      yield* store.quarantineIfEntered({
        permit: admittedDecision.permit,
        observedAt: "2026-09-06T09:00:01.000Z",
      });
      assert.deepStrictEqual(
        yield* sql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE admission_id=${admittedDecision.permit.admissionId}
        `,
        [{ status: "superseded" }],
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE admission_id=${admittedDecision.permit.admissionId}
            AND authority_kind='quarantine'
        `)[0]?.count,
        0,
      );
      assert.equal(
        (yield* sql<{ readonly count: number }>`
          SELECT count(*) AS count FROM main.agent_control_provider_authority_markers
          WHERE authority_kind='supersede'
        `)[0]?.count,
        2,
      );
      assert.equal(
        (yield* store.request({
          request: admittedRequest,
          usage: providerAdmissionUsageEvidence({
            providerInstanceId: admittedRequest.providerInstanceId,
            status: "allowed",
            observedAt: finalizedAt,
            source: "runtime-event",
            nextRelevantAt: null,
          }),
          ownerId: "new-owner",
          leaseExpiresAt: leaseExpiry,
          now: finalizedAt,
        }))._tag,
        "Waiting",
      );

      const supersedeRestartScope = yield* Scope.make("sequential");
      const supersedeRestartSqlContext = yield* Layer.buildWithScope(
        NodeSqliteClient.layer({ filename }),
        supersedeRestartScope,
      );
      const supersedeRestartSql = Context.get(supersedeRestartSqlContext, SqlClient.SqlClient);
      yield* supersedeRestartSql`PRAGMA foreign_keys=ON`;
      const supersedeRestart = yield* Effect.exit(
        Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, supersedeRestartSql)),
          ),
          supersedeRestartScope,
        ),
      );
      assert.isTrue(Exit.isSuccess(supersedeRestart));
      yield* Scope.close(supersedeRestartScope, Exit.void);

      yield* Effect.sync(() => {
        fixtureDatabase.exec(`
          DELETE FROM main.agent_control_initial_planning_finalization_markers;
          DELETE FROM main.agent_control_initial_planning_finalization_receipts;
          DELETE FROM main.agent_control_initial_planning_result_evidence;
        `);
      });
      for (const trigger of finalizationTriggers) {
        yield* sql.unsafe(trigger.source).unprepared;
      }
      assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "recovers legacy pre-invoke quarantine with a new fence and invalidates late old delivery CAS",
  guardScenario(true, true),
);

it.live(
  "recovers capacity after migration070 retires a persisted prepared session claim in all three stages",
  guardScenario(true, true, true),
);

it.live(
  "recovers capacity after migration070 archives an already retry-wait prepared session in all three stages",
  guardScenario(true, true, "retry-wait"),
);

for (const obstacle of [
  "unarchived",
  "foreign-model",
  "newer-delivery",
  "accepted",
  "attempted",
  "attested",
  "session-rebound",
] as const) {
  it.live(
    `refuses archived preparation recovery without exact uninvoked proof: ${obstacle}`,
    guardScenario(true, true, true, obstacle),
  );
}

it.effect(
  "retains live verification check authority across admission expiry and revokes it on quarantine",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* TestClock.setTime(DateTime.toEpochMillis(DateTime.makeUnsafe(at)));
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-verification-check-guard-",
        });
        const filename = path.join(directory, "guard.sqlite");
        const context = yield* Layer.build(NodeSqliteClient.layer({ filename }));
        const sql = Context.get(context, SqlClient.SqlClient);
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        const storeContext = yield* Layer.build(
          Layer.fresh(ProviderAdmissionStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
          ),
        );
        const store = Context.get(storeContext, ProviderAdmissionStore);
        const value = request("verification", ProviderInstanceId.make("verification-check-guard"));
        const admissionExpiry = "2026-09-06T08:02:00.000Z";
        const decision = yield* store.request({
          request: value,
          usage: providerAdmissionUsageEvidence({
            providerInstanceId: value.providerInstanceId,
            status: "allowed",
            observedAt: at,
            source: "refresh",
            nextRelevantAt: null,
          }),
          ownerId: "check-owner",
          leaseExpiresAt: admissionExpiry,
          now: at,
        });
        assert.equal(decision._tag, "Admitted");
        if (decision._tag !== "Admitted") return;
        const permit = decision.permit;
        const triggers = yield* sql<{ name: string; source: string }>`
      SELECT name,sql AS source FROM main.sqlite_schema WHERE type='trigger' AND tbl_name IN (
        'agent_control_stage_run_states', 'agent_control_stage_run_lease_states', 'agent_control_verification_deliveries'
      ) AND sql IS NOT NULL ORDER BY name`;
        const fixture = yield* Effect.acquireRelease(
          Effect.sync(() => {
            const db = new NodeSqlite.DatabaseSync(filename);
            db.exec("PRAGMA foreign_keys=OFF");
            return db;
          }),
          (db) => Effect.sync(() => db.close()),
        );
        const mutateFixture = (use: () => void) =>
          Effect.gen(function* () {
            for (const trigger of triggers)
              yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
            yield* Effect.sync(use);
            for (const trigger of triggers) yield* sql.unsafe(trigger.source).unprepared;
          });
        for (const trigger of triggers)
          yield* sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        yield* sql`PRAGMA foreign_keys=OFF`;
        yield* seedStageAndDelivery(sql, value, false);
        yield* Effect.sync(() => seedNonInitialDelivery(fixture, value));
        for (const trigger of triggers) yield* sql.unsafe(trigger.source).unprepared;
        yield* sql`PRAGMA foreign_keys=ON`;
        const guardContext = yield* Layer.build(
          Layer.fresh(ProviderAdmissionGuardLive).pipe(
            Layer.provide(Layer.mock(AgentControlStageRunLeaseEngine)({})),
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
            Layer.provide(Layer.succeed(ProviderAdmissionStore, store)),
            Layer.provide(Layer.succeed(AgentControlTaskConsumerGuard, taskGuardShape)),
          ),
        );
        const guard = Context.get(guardContext, ProviderAdmissionGuard);
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(guard.enter(permit, "verification-check"))),
        );
        yield* guard.enter(permit, "session-start");
        yield* mutateFixture(() => {
          fixture
            .prepare(`UPDATE agent_control_verification_deliveries SET state='delivery-attempted',
        revision=revision+1,attempt_count=1,provider_session_created_at=?,provider_resume_cursor_json='{}'
        WHERE provider_delivery_id=?`)
            .run(at, value.providerDeliveryId);
        });
        yield* guard.enter(permit, "turn-start");
        const earlyMarkers =
          yield* sql`SELECT * FROM agent_control_provider_authority_markers WHERE admission_id=${permit.admissionId} ORDER BY marker_id`;
        const earlyCapacity =
          yield* sql`SELECT * FROM agent_control_provider_capacity_current WHERE provider_instance_id=${String(permit.providerInstanceId)}`;
        yield* guard.enter(permit, "verification-check");
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_provider_authority_markers WHERE admission_id=${permit.admissionId} ORDER BY marker_id`,
          earlyMarkers,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_provider_capacity_current WHERE provider_instance_id=${String(permit.providerInstanceId)}`,
          earlyCapacity,
        );
        yield* mutateFixture(() => {
          fixture
            .prepare(`UPDATE agent_control_verification_deliveries SET state='provider-started',
        revision=revision+1,provider_turn_id='native-check-turn',provider_accepted_at=?,claim_owner_id=NULL,claim_expires_at=NULL
        WHERE provider_delivery_id=?`)
            .run(at, value.providerDeliveryId);
        });
        const markers =
          yield* sql`SELECT * FROM agent_control_provider_authority_markers WHERE admission_id=${permit.admissionId} ORDER BY marker_id`;
        const capacity =
          yield* sql`SELECT * FROM agent_control_provider_capacity_current WHERE provider_instance_id=${String(permit.providerInstanceId)}`;
        yield* Migration076.pipe(Effect.provideService(SqlClient.SqlClient, sql));
        const cwd = path.join(directory, "code");
        yield* fs.makeDirectory(cwd);
        yield* fs.writeFileString(
          path.join(cwd, "focused.test.cjs"),
          "require('node:test')('focused check', () => require('node:assert/strict').equal(1, 1));\n",
        );
        yield* Effect.sync(() => {
          NodeChildProcess.execFileSync("git", ["init", "--quiet"], { cwd });
          NodeChildProcess.execFileSync("git", ["add", "focused.test.cjs"], { cwd });
          NodeChildProcess.execFileSync(
            "git",
            [
              "-c",
              "user.name=Test",
              "-c",
              "user.email=test@example.invalid",
              "commit",
              "-qm",
              "fixture",
            ],
            { cwd },
          );
        });
        const manifest = yield* prepareVerificationCheckManifest(sql, {
          permit,
          cwd,
          checks: ["cross-expiry", "after-expiry", "quarantined", "after-quarantine"].map((id) => ({
            id,
            command: process.execPath,
            args: ["--test", "--test-reporter=tap", "focused.test.cjs"],
            cwd: ".",
            required: true,
            timeoutMs: 300_000,
            allowTemporaryFiles: false,
            resultFormat: "node-test" as const,
          })),
        });
        let executions = 0;
        const command = Effect.sync(() => {
          executions += 1;
          return {
            exitCode: 0,
            stdout: NodeChildProcess.execFileSync(
              process.execPath,
              ["--test", "--test-reporter=tap", "focused.test.cjs"],
              { cwd, encoding: "utf8" },
            ),
            stderr: "",
          };
        });
        const execute = <E = never>(
          checkId: string,
          during: Effect.Effect<void, E> = Effect.void,
        ) =>
          executeVerificationCheck(sql, {
            manifest,
            checkId,
            providerTurnId: "native-check-turn",
            authorize: guard.enter(permit, "verification-check"),
            execute: command.pipe(Effect.tap(() => during)),
          });
        yield* TestClock.adjust("119 seconds");
        assert.equal((yield* execute("cross-expiry", TestClock.adjust("121 seconds"))).exitCode, 0);
        assert.equal((yield* execute("after-expiry")).exitCode, 0);
        yield* TestClock.adjust("121 seconds");
        assert.equal((yield* execute("after-expiry")).exitCode, 0);
        assert.equal(executions, 2);
        for (const boundary of ["session-start", "turn-start"] as const) {
          assert.isTrue(Exit.isFailure(yield* Effect.exit(guard.enter(permit, boundary))));
        }
        assert.deepStrictEqual(
          yield* sql`SELECT check_id AS id,status FROM agent_control_verification_check_results ORDER BY check_id`,
          [
            { id: "after-expiry", status: "passed" },
            { id: "cross-expiry", status: "passed" },
          ],
        );
        yield* guard.enter(permit, "verification-check");
        yield* guard.enter(permit, "verification-check");
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_provider_authority_markers WHERE admission_id=${permit.admissionId} ORDER BY marker_id`,
          markers,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_provider_capacity_current WHERE provider_instance_id=${String(permit.providerInstanceId)}`,
          capacity,
        );
        for (const invalid of [
          { ...permit, providerFenceToken: permit.providerFenceToken + 1 },
          { ...permit, stageFenceToken: permit.stageFenceToken + 1 },
          { ...permit, admissionOwnerId: "other-owner" },
          { ...permit, stageLeaseHolderId: "other-holder" },
          { ...permit, providerDeliveryId: "old-verification-delivery" },
          { ...permit, stage: "implementation" as const },
        ])
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(guard.enter(invalid, "verification-check"))),
          );
        yield* mutateFixture(() => {
          fixture
            .prepare(
              "UPDATE agent_control_stage_run_lease_states SET expires_at=? WHERE lease_id=?",
            )
            .run("2000-01-01T00:00:00.000Z", permit.stageLeaseId);
        });
        assert.isTrue(
          Exit.isFailure(yield* Effect.exit(guard.enter(permit, "verification-check"))),
        );
        yield* mutateFixture(() => {
          fixture
            .prepare(
              "UPDATE agent_control_stage_run_lease_states SET expires_at=? WHERE lease_id=?",
            )
            .run(leaseExpiry, permit.stageLeaseId);
        });
        assert.equal(
          (yield* execute("quarantined", guard.quarantineIfEntered(permit))).exitCode,
          125,
        );
        assert.isTrue(Exit.isFailure(yield* Effect.exit(execute("after-quarantine"))));
        assert.isTrue(Exit.isFailure(yield* Effect.exit(execute("after-expiry"))));
        assert.equal(executions, 3);
        assert.deepStrictEqual(
          yield* sql`SELECT check_id AS id,status FROM agent_control_verification_check_results ORDER BY check_id`,
          [
            { id: "after-expiry", status: "passed" },
            { id: "cross-expiry", status: "passed" },
            { id: "quarantined", status: "unavailable" },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT status FROM agent_control_provider_admission_current WHERE admission_id=${permit.admissionId}`,
          [{ status: "quarantined" }],
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
