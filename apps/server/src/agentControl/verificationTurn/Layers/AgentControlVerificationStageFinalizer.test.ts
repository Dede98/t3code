import * as NodeServices from "@effect/platform-node/NodeServices";
import { AgentControlTaskId, AgentControlWorktreeReservationId } from "@t3tools/contracts";
import * as NodeSqlite from "node:sqlite";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Cause from "effect/Cause";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import {
  canonicalJson,
  parseCanonicalJson,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import { layer as StageEventStoreLive } from "../../stageRun/Layers/AgentControlStageRunEventStore.ts";
import { layer as StageProjectionLive } from "../../stageRun/Layers/AgentControlStageRunProjection.ts";
import { layer as StageStateRepositoryLive } from "../../stageRun/Layers/AgentControlStageRunStateRepository.ts";
import { AGENT_CONTROL_STAGE_RUN_PROJECTOR } from "../../stageRun/projector.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import {
  AgentControlStageRunEngine,
  type AgentControlStageRunEngineShape,
} from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { layer as LeaseEventStoreLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseEventStore.ts";
import { layer as LeaseProjectionLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseProjection.ts";
import { layer as LeaseStateRepositoryLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseStateRepository.ts";
import { AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR } from "../../stageRunLease/invariant.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import {
  AgentControlStageRunLeaseEngine,
  type AgentControlStageRunLeaseEngineShape,
} from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import {
  ProviderAdmissionReleaseAuthority,
  type ProviderAdmissionReleaseAuthorityShape,
} from "../../providerAdmission/Services/ProviderAdmissionReleaseAuthority.ts";
import { ProviderAdmissionError } from "../../providerAdmission/Services/ProviderAdmissionStore.ts";
import {
  deriveVerificationEvaluationEvidenceId,
  deriveVerificationEvaluationId,
  deriveVerificationEvaluationMarkerId,
  deriveVerificationEvaluationReceiptId,
  deriveVerificationStageStartCommandId,
  deriveVerificationStageStartEvidenceId,
  deriveVerificationStageStartEventId,
  deriveVerificationStageStartMarkerId,
  deriveVerificationStageStartReceiptId,
  fingerprintVerificationTurn,
} from "../identity.ts";
import type { AgentControlVerificationClaim } from "../model.ts";
import {
  loadAgentControlVerificationTaskAuthorityInTransaction,
  loadAgentControlVerificationWorktreeAuthorityInTransaction,
} from "../historicalAuthority.ts";
import {
  AgentControlVerificationHandoffStore,
  type AgentControlVerificationHandoffStoreShape,
} from "../Services/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationEvaluator } from "../Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationStageFinalizer } from "../Services/AgentControlVerificationStageFinalizer.ts";
import {
  AgentControlVerificationStageFinalizerHooks,
  type AgentControlVerificationStageFinalizerHooksShape,
} from "../Services/AgentControlVerificationStageFinalizerHooks.ts";
import { AgentControlVerificationStageFinalizerLive } from "./AgentControlVerificationStageFinalizer.ts";
import { AgentControlVerificationHandoffStoreLive } from "./AgentControlVerificationHandoffStore.ts";

type Outcome = "passed" | "failed-verdict" | "invalid-output" | "delivery-failed" | "interrupted";

const timestamp = "2026-08-29T10:00:00.000Z";
const startedAt = "2026-08-29T10:00:01.000Z";
const terminalAt = "2026-08-29T10:00:02.000Z";
const fingerprint = "a".repeat(64);

const makeClaim = Effect.fn("makeVerificationFinalizerClaim")(function* (
  suffix: string,
  outcome: Outcome,
) {
  const handoffId = `handoff-${suffix}`;
  const providerDeliveryId = `delivery-${suffix}`;
  const projectId = `project-${suffix}`;
  const taskId = `task-${suffix}`;
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId: projectId as never,
    taskId: taskId as never,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint: fingerprint,
    stageKind: "verification",
    stageOrdinal: 3,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({
    projectId: projectId as never,
    taskId: taskId as never,
  });
  const deliveryState =
    outcome === "delivery-failed"
      ? "failed"
      : outcome === "interrupted"
        ? "interrupted"
        : "completed";
  return {
    evidence: {
      handoffId,
      handoffFingerprint: fingerprintVerificationTurn("test-handoff", [handoffId]),
      materializationEvidenceId: `materialization-evidence-${suffix}`,
      materializationReceiptId: `materialization-receipt-${suffix}`,
      materializationMarkerId: `materialization-marker-${suffix}`,
      admissionEvidenceId: `admission-evidence-${suffix}`,
      admissionReceiptId: `admission-receipt-${suffix}`,
      admissionMarkerId: `admission-marker-${suffix}`,
      projectId,
      taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      taskSourceEventId: `task-source-event-${suffix}`,
      taskSourceEventSequence: 1,
      taskSourceEventStreamVersion: 1,
      stageRunId,
      attemptId,
      leaseId,
      leaseHolderId: `holder-${suffix}`,
      fenceToken: 3,
      worktreeReservationId: `worktree-${suffix}`,
      worktreeRevision: 2,
      worktreeEventId: `worktree-event-${suffix}`,
      worktreeEventSequence: 2,
      worktreeEventStreamVersion: 2,
      worktreeOwnershipFingerprint: fingerprint,
      worktreeVerifiedAt: timestamp,
      worktreePath: `/tmp/worktree-${suffix}`,
      branch: `branch-${suffix}`,
      controlledThreadReservationId: `reservation-${suffix}`,
      threadId: `thread-${suffix}`,
      planningThreadId: `planning-thread-${suffix}`,
      planId: `plan-${suffix}`,
      proposedPlanDigest: fingerprint,
      providerInstanceId: "codex",
      runtimeMode: "approval-required",
      modelSelection: { instanceId: "codex", model: "gpt-5-codex" },
      modelSelectionJson: '{"instanceId":"codex","model":"gpt-5-codex"}',
      modelSelectionFingerprint: fingerprint,
      templateVersion: "agent-control-verification-prompt-v2",
      promptContractFingerprint: fingerprint,
      promptText: "closed prompt fixture",
      promptDigest: fingerprint,
      resultSchemaVersion: "agent-control-verification-result-v1",
      resultSchemaFingerprint: fingerprint,
      turnRequestCommandId: `turn-command-${suffix}`,
      messageId: `message-${suffix}`,
      messageEventId: `message-event-${suffix}`,
      turnRequestEventId: `turn-request-event-${suffix}`,
      messageEventTemplateJson: "{}",
      turnRequestEventTemplateJson: "{}",
      eventTemplateDigest: fingerprint,
      providerDeliveryId,
      createdAt: timestamp,
    },
    delivery: {
      providerDeliveryId,
      handoffId,
      handoffFingerprint: fingerprintVerificationTurn("test-handoff", [handoffId]),
      admissionMarkerId: `admission-marker-${suffix}`,
      materializationEvidenceId: `materialization-evidence-${suffix}`,
      controlledThreadReservationId: `reservation-${suffix}`,
      threadId: `thread-${suffix}`,
      stageRunId,
      attemptId,
      leaseId,
      leaseHolderId: `holder-${suffix}`,
      fenceToken: 3,
      providerInstanceId: "codex",
      runtimeMode: "approval-required",
      modelSelectionFingerprint: fingerprint,
      turnRequestCommandId: `turn-command-${suffix}`,
      messageId: `message-${suffix}`,
      planningThreadId: `planning-thread-${suffix}`,
      planId: `plan-${suffix}`,
      state: deliveryState,
      revision: 6,
      claimOwnerId: null,
      claimGeneration: 1,
      claimExpiresAt: null,
      attemptCount: 1,
      nextAttemptAt: null,
      providerTurnId: `provider-turn-${suffix}`,
      providerAcceptedAt: startedAt,
      providerSessionCreatedAt: timestamp,
      providerResumeCursorJson: null,
      terminalAt,
      terminalEventId: `runtime-terminal-${suffix}`,
      terminalEventType: deliveryState === "completed" ? "turn.completed" : "turn.aborted",
      terminalProviderState: deliveryState,
      terminalObservationDigest: fingerprint,
      lastErrorCode:
        deliveryState === "completed"
          ? null
          : deliveryState === "failed"
            ? "provider-turn-failed"
            : "provider-turn-interrupted",
      interruptRequested: deliveryState === "interrupted",
      updatedAt: terminalAt,
    },
  } as unknown as AgentControlVerificationClaim;
});

const schemaSql = `
  PRAGMA journal_mode = WAL;
  PRAGMA foreign_keys = ON;
  CREATE TABLE agent_control_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE, aggregate_kind TEXT NOT NULL, stream_id TEXT NOT NULL,
    stream_version INTEGER NOT NULL, event_type TEXT NOT NULL, occurred_at TEXT NOT NULL,
    command_id TEXT NOT NULL, causation_event_id TEXT, correlation_id TEXT NOT NULL,
    actor_authority TEXT NOT NULL, payload_json TEXT NOT NULL, metadata_json TEXT NOT NULL,
    UNIQUE(aggregate_kind, stream_id, stream_version)
  );
  CREATE TABLE agent_control_stage_run_states (
    stage_run_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL UNIQUE, role_id TEXT NOT NULL, stage_kind TEXT NOT NULL,
    stage_ordinal INTEGER NOT NULL, attempt_ordinal INTEGER NOT NULL, status TEXT NOT NULL,
    task_revision INTEGER NOT NULL, github_intake_sequence INTEGER NOT NULL,
    source_identity_fingerprint TEXT NOT NULL, state_json TEXT NOT NULL,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revision INTEGER NOT NULL,
    last_event_sequence INTEGER NOT NULL
  );
  CREATE TABLE agent_control_stage_run_lease_states (
    lease_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, task_id TEXT NOT NULL,
    stage_run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, task_revision INTEGER NOT NULL,
    github_intake_sequence INTEGER NOT NULL, source_identity_fingerprint TEXT NOT NULL,
    holder_id TEXT NOT NULL, fence_token INTEGER NOT NULL, status TEXT NOT NULL,
    acquired_at TEXT NOT NULL, renewed_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    released_at TEXT, state_json TEXT NOT NULL, revision INTEGER NOT NULL,
    last_event_sequence INTEGER NOT NULL
  );
  CREATE TABLE agent_control_projection_state (
    projector_name TEXT PRIMARY KEY, last_applied_sequence INTEGER NOT NULL, updated_at TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_stage_started_evidence (
    start_evidence_id TEXT PRIMARY KEY, handoff_id TEXT NOT NULL, provider_delivery_id TEXT NOT NULL,
    start_command_id TEXT NOT NULL, start_fingerprint TEXT NOT NULL, stage_event_id TEXT NOT NULL,
    stage_event_sequence INTEGER NOT NULL, delivery_revision INTEGER NOT NULL,
    claim_generation INTEGER NOT NULL, attempt_count INTEGER NOT NULL, started_at TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_stage_started_receipts (
    start_receipt_id TEXT PRIMARY KEY, start_evidence_id TEXT NOT NULL, provider_delivery_id TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_stage_started_markers (
    start_marker_id TEXT PRIMARY KEY, start_evidence_id TEXT NOT NULL,
    start_receipt_id TEXT NOT NULL, provider_delivery_id TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_evaluation_evidence (
    evaluation_id TEXT PRIMARY KEY, evidence_id TEXT NOT NULL, receipt_id TEXT NOT NULL,
    marker_id TEXT NOT NULL, provider_delivery_id TEXT NOT NULL,
    evaluation_fingerprint TEXT NOT NULL, authority_digest TEXT NOT NULL,
    authority_json TEXT NOT NULL, disposition TEXT NOT NULL, verdict TEXT, error_code TEXT,
    semantic_result_digest TEXT, source_disposition TEXT NOT NULL, source_message_id TEXT,
    source_event_id TEXT, source_event_sequence INTEGER, source_event_stream_version INTEGER,
    raw_output_digest TEXT, output_byte_length INTEGER NOT NULL, terminal_event_id TEXT NOT NULL,
    terminal_sequence INTEGER NOT NULL, terminal_stream_version INTEGER NOT NULL,
    terminal_observation_digest TEXT NOT NULL, start_marker_id TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_evaluation_receipts (
    receipt_id TEXT PRIMARY KEY, evaluation_id TEXT NOT NULL, evidence_id TEXT NOT NULL,
    provider_delivery_id TEXT NOT NULL, status TEXT NOT NULL, evaluation_fingerprint TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_evaluation_markers (
    marker_id TEXT PRIMARY KEY, evaluation_id TEXT NOT NULL, evidence_id TEXT NOT NULL,
    receipt_id TEXT NOT NULL, provider_delivery_id TEXT NOT NULL, marker_version INTEGER NOT NULL,
    evaluation_fingerprint TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_finalization_evidence (
    finalization_evidence_id TEXT PRIMARY KEY, receipt_id TEXT NOT NULL, marker_id TEXT NOT NULL,
    finalization_command_id TEXT NOT NULL, finalization_fingerprint TEXT NOT NULL,
    finalization_json TEXT NOT NULL, handoff_id TEXT NOT NULL, handoff_fingerprint TEXT NOT NULL,
    project_id TEXT NOT NULL, task_id TEXT NOT NULL, task_revision INTEGER NOT NULL,
    github_intake_sequence INTEGER NOT NULL, source_identity_fingerprint TEXT NOT NULL,
    stage_run_id TEXT NOT NULL, attempt_id TEXT NOT NULL, lease_id TEXT NOT NULL,
    lease_holder_id TEXT NOT NULL, fence_token INTEGER NOT NULL, provider_delivery_id TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL, provider_turn_id TEXT NOT NULL,
    delivery_revision INTEGER NOT NULL, delivery_terminal_state TEXT NOT NULL,
    terminal_runtime_event_id TEXT NOT NULL, terminal_at TEXT NOT NULL,
    start_evidence_id TEXT NOT NULL, start_receipt_id TEXT NOT NULL, start_marker_id TEXT NOT NULL,
    evaluation_authority TEXT NOT NULL, evaluation_id TEXT, evaluation_evidence_id TEXT,
    evaluation_receipt_id TEXT, evaluation_marker_id TEXT, evaluation_disposition TEXT,
    verification_verdict TEXT, invalid_output_code TEXT, outcome TEXT NOT NULL,
    terminal_cause TEXT NOT NULL, stage_event_id TEXT NOT NULL,
    stage_event_sequence INTEGER NOT NULL, stage_event_stream_version INTEGER NOT NULL,
    lease_event_id TEXT NOT NULL, lease_event_sequence INTEGER NOT NULL,
    lease_event_stream_version INTEGER NOT NULL, finalized_at TEXT NOT NULL
  );
  CREATE UNIQUE INDEX idx_verification_finalization_evidence_handoff
    ON agent_control_verification_finalization_evidence(handoff_id);
  CREATE TABLE agent_control_verification_finalization_receipts (
    receipt_id TEXT PRIMARY KEY, marker_id TEXT NOT NULL, finalization_evidence_id TEXT NOT NULL,
    finalization_command_id TEXT NOT NULL, finalization_fingerprint TEXT NOT NULL,
    handoff_id TEXT NOT NULL, outcome TEXT NOT NULL, terminal_cause TEXT NOT NULL,
    stage_event_id TEXT NOT NULL, stage_event_sequence INTEGER NOT NULL,
    lease_event_id TEXT NOT NULL, lease_event_sequence INTEGER NOT NULL,
    status TEXT NOT NULL, accepted_at TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_finalization_markers (
    marker_id TEXT PRIMARY KEY, marker_fingerprint TEXT NOT NULL, receipt_id TEXT NOT NULL,
    finalization_evidence_id TEXT NOT NULL, finalization_command_id TEXT NOT NULL,
    finalization_fingerprint TEXT NOT NULL, handoff_id TEXT NOT NULL, committed_at TEXT NOT NULL
  );
  CREATE TABLE agent_control_verification_handoff_intents (handoff_id TEXT PRIMARY KEY);
`;

const insertEvent = (
  database: NodeSqlite.DatabaseSync,
  event: {
    readonly eventId: string;
    readonly aggregateKind: "stage-run" | "stage-run-lease";
    readonly streamId: string;
    readonly streamVersion: number;
    readonly type: string;
    readonly occurredAt: string;
    readonly commandId: string;
    readonly causationEventId: string | null;
    readonly authority: "controller" | "system";
    readonly payload: object;
  },
) =>
  database
    .prepare(`INSERT INTO agent_control_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
      command_id, causation_event_id, correlation_id, actor_authority, payload_json, metadata_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(
      event.eventId,
      event.aggregateKind,
      event.streamId,
      event.streamVersion,
      event.type,
      event.occurredAt,
      event.commandId,
      event.causationEventId,
      event.commandId,
      event.authority,
      JSON.stringify(event.payload),
      '{"schemaVersion":1}',
    );

const seedAuthority = Effect.fn("seedVerificationFinalizerAuthority")(function* (
  filename: string,
  claim: AgentControlVerificationClaim,
  outcome: Outcome,
) {
  const database = new NodeSqlite.DatabaseSync(filename);
  try {
    database.exec(schemaSql);
    const preparedEventId = `prepared-${claim.evidence.handoffId}`;
    const startCommandId = deriveVerificationStageStartCommandId(
      claim.evidence.providerDeliveryId,
      claim.delivery.providerTurnId!,
    );
    const startEventId = deriveVerificationStageStartEventId(startCommandId);
    const startEvidenceId = deriveVerificationStageStartEvidenceId(startCommandId);
    const startReceiptId = deriveVerificationStageStartReceiptId(startCommandId);
    const startMarkerId = deriveVerificationStageStartMarkerId(startCommandId);
    const startFingerprint = fingerprintVerificationTurn("stage-start", [
      claim.evidence.admissionEvidenceId,
      claim.evidence.admissionReceiptId,
      claim.evidence.admissionMarkerId,
      claim.evidence.materializationEvidenceId,
      claim.evidence.materializationReceiptId,
      claim.evidence.materializationMarkerId,
      claim.evidence.handoffId,
      claim.evidence.handoffFingerprint,
      claim.evidence.providerDeliveryId,
      "5",
      "1",
      "1",
      claim.evidence.threadId,
      claim.evidence.planningThreadId,
      claim.evidence.planId,
      claim.delivery.providerTurnId!,
      startEventId,
      startedAt,
    ]);
    const preparedPayload = {
      projectId: claim.evidence.projectId,
      taskId: claim.evidence.taskId,
      stageRunId: claim.evidence.stageRunId,
      attemptId: claim.evidence.attemptId,
      roleId: "verifier",
      stageKind: "verification",
      stageOrdinal: 3,
      attemptOrdinal: 1,
      status: "prepared",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      preparedAt: timestamp,
    };
    const startedPayload = {
      ...preparedPayload,
      status: "running",
      admissionEvidenceId: claim.evidence.admissionEvidenceId,
      admissionReceiptId: claim.evidence.admissionReceiptId,
      admissionMarkerId: claim.evidence.admissionMarkerId,
      materializationEvidenceId: claim.evidence.materializationEvidenceId,
      materializationReceiptId: claim.evidence.materializationReceiptId,
      materializationMarkerId: claim.evidence.materializationMarkerId,
      handoffId: claim.evidence.handoffId,
      handoffFingerprint: claim.evidence.handoffFingerprint,
      providerDeliveryId: claim.evidence.providerDeliveryId,
      deliveryRevision: 5,
      claimGeneration: 1,
      attemptCount: 1,
      controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
      threadId: claim.evidence.threadId,
      planningThreadId: claim.evidence.planningThreadId,
      planId: claim.evidence.planId,
      proposedPlanDigest: claim.evidence.proposedPlanDigest,
      providerInstanceId: claim.evidence.providerInstanceId,
      providerTurnId: claim.delivery.providerTurnId,
      runtimeMode: "approval-required",
      modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
      leaseId: claim.evidence.leaseId,
      leaseHolderId: claim.evidence.leaseHolderId,
      fenceToken: 3,
      startedAt,
    };
    insertEvent(database, {
      eventId: preparedEventId,
      aggregateKind: "stage-run",
      streamId: claim.evidence.stageRunId,
      streamVersion: 1,
      type: "agentControl.stageRun.prepared",
      occurredAt: timestamp,
      commandId: `prepare-${claim.evidence.handoffId}`,
      causationEventId: null,
      authority: "controller",
      payload: preparedPayload,
    });
    insertEvent(database, {
      eventId: startEventId,
      aggregateKind: "stage-run",
      streamId: claim.evidence.stageRunId,
      streamVersion: 2,
      type: "agentControl.stageRun.verificationStarted",
      occurredAt: startedAt,
      commandId: startCommandId,
      causationEventId: claim.evidence.turnRequestEventId,
      authority: "system",
      payload: startedPayload,
    });
    const planningStageRunId = yield* deriveAgentControlStageRunId({
      projectId: claim.evidence.projectId,
      taskId: claim.evidence.taskId as never,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      stageKind: "planning",
      stageOrdinal: 1,
    });
    const implementationStageRunId = yield* deriveAgentControlStageRunId({
      projectId: claim.evidence.projectId,
      taskId: claim.evidence.taskId as never,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      stageKind: "implementation",
      stageOrdinal: 2,
    });
    const planningAttemptId = yield* deriveAgentControlAttemptId(planningStageRunId, 1);
    const implementationAttemptId = yield* deriveAgentControlAttemptId(implementationStageRunId, 1);
    const leasePayload = {
      leaseId: claim.evidence.leaseId,
      projectId: claim.evidence.projectId,
      taskId: claim.evidence.taskId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      holderId: claim.evidence.leaseHolderId,
      acquiredAt: timestamp,
      renewedAt: timestamp,
      expiresAt: "2026-08-29T11:00:00.000Z",
    };
    for (const [index, stageRunId, attemptId] of [
      [1, planningStageRunId, planningAttemptId],
      [2, implementationStageRunId, implementationAttemptId],
      [3, claim.evidence.stageRunId, claim.evidence.attemptId],
    ] as const) {
      const reserveVersion = index * 2 - 1;
      insertEvent(database, {
        eventId: `lease-reserved-${index}-${claim.evidence.handoffId}`,
        aggregateKind: "stage-run-lease",
        streamId: claim.evidence.leaseId,
        streamVersion: reserveVersion,
        type: "agentControl.stageRunLease.reserved",
        occurredAt: timestamp,
        commandId: `reserve-${index}-${claim.evidence.handoffId}`,
        causationEventId: null,
        authority: "controller",
        payload: { ...leasePayload, stageRunId, attemptId, fenceToken: index },
      });
      if (index < 3) {
        insertEvent(database, {
          eventId: `lease-released-${index}-${claim.evidence.handoffId}`,
          aggregateKind: "stage-run-lease",
          streamId: claim.evidence.leaseId,
          streamVersion: reserveVersion + 1,
          type: "agentControl.stageRunLease.releasedBeforeExecution",
          occurredAt: timestamp,
          commandId: `release-${index}-${claim.evidence.handoffId}`,
          causationEventId: null,
          authority: "controller",
          payload: {
            leaseId: claim.evidence.leaseId,
            stageRunId,
            attemptId,
            holderId: claim.evidence.leaseHolderId,
            fenceToken: index,
            releasedAt: timestamp,
          },
        });
      }
    }
    const stageState = {
      schemaVersion: 1,
      projectId: claim.evidence.projectId,
      taskId: claim.evidence.taskId,
      stageRunId: claim.evidence.stageRunId,
      attemptId: claim.evidence.attemptId,
      roleId: "verifier",
      stageKind: "verification",
      stageOrdinal: 3,
      attemptOrdinal: 1,
      status: "running",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint: fingerprint,
      createdAt: timestamp,
      updatedAt: startedAt,
      revision: 2,
      sequence: 2,
    };
    database
      .prepare(`INSERT INTO agent_control_stage_run_states VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )`)
      .run(
        claim.evidence.stageRunId,
        claim.evidence.projectId,
        claim.evidence.taskId,
        claim.evidence.attemptId,
        "verifier",
        "verification",
        3,
        1,
        "running",
        1,
        1,
        fingerprint,
        canonicalJson(stageState as unknown as JsonValue),
        timestamp,
        startedAt,
        2,
        2,
      );
    const leaseState = {
      schemaVersion: 1,
      ...leasePayload,
      stageRunId: claim.evidence.stageRunId,
      attemptId: claim.evidence.attemptId,
      fenceToken: 3,
      status: "reserved",
      releasedAt: null,
      revision: 5,
      sequence: 7,
    };
    database
      .prepare(`INSERT INTO agent_control_stage_run_lease_states VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )`)
      .run(
        claim.evidence.leaseId,
        claim.evidence.projectId,
        claim.evidence.taskId,
        claim.evidence.stageRunId,
        claim.evidence.attemptId,
        1,
        1,
        fingerprint,
        claim.evidence.leaseHolderId,
        3,
        "reserved",
        timestamp,
        timestamp,
        "2026-08-29T11:00:00.000Z",
        null,
        canonicalJson(leaseState as unknown as JsonValue),
        5,
        7,
      );
    database
      .prepare("INSERT INTO agent_control_projection_state VALUES (?, ?, ?)")
      .run(AGENT_CONTROL_STAGE_RUN_PROJECTOR, 2, startedAt);
    database
      .prepare("INSERT INTO agent_control_projection_state VALUES (?, ?, ?)")
      .run(AGENT_CONTROL_STAGE_RUN_LEASE_PROJECTOR, 7, timestamp);
    database
      .prepare(`INSERT INTO agent_control_verification_stage_started_evidence
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(
        startEvidenceId,
        claim.evidence.handoffId,
        claim.evidence.providerDeliveryId,
        startCommandId,
        startFingerprint,
        startEventId,
        2,
        5,
        1,
        1,
        startedAt,
      );
    database
      .prepare("INSERT INTO agent_control_verification_stage_started_receipts VALUES (?, ?, ?)")
      .run(startReceiptId, startEvidenceId, claim.evidence.providerDeliveryId);
    database
      .prepare("INSERT INTO agent_control_verification_stage_started_markers VALUES (?, ?, ?, ?)")
      .run(startMarkerId, startEvidenceId, startReceiptId, claim.evidence.providerDeliveryId);

    if (outcome === "passed" || outcome === "failed-verdict" || outcome === "invalid-output") {
      const evaluationId = deriveVerificationEvaluationId({
        providerDeliveryId: claim.evidence.providerDeliveryId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId: claim.delivery.providerTurnId!,
        resultSchemaFingerprint: claim.evidence.resultSchemaFingerprint!,
      });
      const evidenceId = deriveVerificationEvaluationEvidenceId(evaluationId);
      const receiptId = deriveVerificationEvaluationReceiptId(evaluationId);
      const markerId = deriveVerificationEvaluationMarkerId(evaluationId);
      const disposition = outcome === "invalid-output" ? "invalid-output" : "evaluated";
      const verdict =
        outcome === "passed" ? "passed" : outcome === "failed-verdict" ? "failed" : null;
      const errorCode = outcome === "invalid-output" ? "schema-violation" : null;
      const semanticDigest = outcome === "invalid-output" ? null : fingerprint;
      const authorityJson = canonicalJson({
        admission: {
          evidenceId: claim.evidence.admissionEvidenceId,
          markerId: claim.evidence.admissionMarkerId,
          receiptId: claim.evidence.admissionReceiptId,
        },
        attemptId: claim.evidence.attemptId,
        controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
        disposition,
        errorCode,
        evaluationId,
        evidenceId,
        handoffFingerprint: claim.evidence.handoffFingerprint,
        handoffId: claim.evidence.handoffId,
        lease: {
          fenceToken: 3,
          holderId: claim.evidence.leaseHolderId,
          leaseId: claim.evidence.leaseId,
        },
        markerId,
        materialization: {
          evidenceId: claim.evidence.materializationEvidenceId,
          markerId: claim.evidence.materializationMarkerId,
          receiptId: claim.evidence.materializationReceiptId,
        },
        modelSelectionFingerprint: fingerprint,
        prompt: {
          contractFingerprint: fingerprint,
          digest: fingerprint,
          templateVersion: claim.evidence.templateVersion,
        },
        providerDeliveryId: claim.evidence.providerDeliveryId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId: claim.delivery.providerTurnId,
        receiptId,
        resultSchema: { fingerprint, version: claim.evidence.resultSchemaVersion },
        semanticResultDigest: semanticDigest,
        source: {
          byteLength: 20,
          disposition: "captured",
          eventId: `source-event-${claim.evidence.handoffId}`,
          eventSequence: 9,
          eventStreamVersion: 1,
          messageId: `source-message-${claim.evidence.handoffId}`,
          rawDigest: fingerprint,
        },
        stageStartMarkerId: startMarkerId,
        stageRunId: claim.evidence.stageRunId,
        task: {
          githubIntakeSequence: 1,
          projectId: claim.evidence.projectId,
          revision: 1,
          sourceIdentityFingerprint: fingerprint,
          taskId: claim.evidence.taskId,
        },
        terminal: {
          eventId: claim.delivery.terminalEventId,
          observationDigest: fingerprint,
          runtimeEventId: claim.delivery.terminalEventId,
          sequence: 10,
          state: "completed",
          streamVersion: 2,
        },
        threadId: claim.evidence.threadId,
        verdict,
        worktree: {
          branch: claim.evidence.branch,
          eventId: claim.evidence.worktreeEventId,
          eventSequence: 2,
          eventStreamVersion: 2,
          ownershipFingerprint: fingerprint,
          path: claim.evidence.worktreePath,
          reservationId: claim.evidence.worktreeReservationId,
          revision: 2,
          verifiedAt: timestamp,
        },
      } satisfies JsonValue);
      const evaluationFingerprint = fingerprintVerificationTurn("evaluation-fingerprint", [
        authorityJson,
      ]);
      database
        .prepare(`INSERT INTO agent_control_verification_evaluation_evidence VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      )`)
        .run(
          evaluationId,
          evidenceId,
          receiptId,
          markerId,
          claim.evidence.providerDeliveryId,
          evaluationFingerprint,
          sha256Utf8(authorityJson),
          authorityJson,
          disposition,
          verdict,
          errorCode,
          semanticDigest,
          "captured",
          `source-message-${claim.evidence.handoffId}`,
          `source-event-${claim.evidence.handoffId}`,
          9,
          1,
          fingerprint,
          20,
          claim.delivery.terminalEventId,
          10,
          2,
          fingerprint,
          startMarkerId,
        );
      database
        .prepare(
          "INSERT INTO agent_control_verification_evaluation_receipts VALUES (?, ?, ?, ?, ?, ?)",
        )
        .run(
          receiptId,
          evaluationId,
          evidenceId,
          claim.evidence.providerDeliveryId,
          "accepted",
          evaluationFingerprint,
        );
      database
        .prepare(
          "INSERT INTO agent_control_verification_evaluation_markers VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          markerId,
          evaluationId,
          evidenceId,
          receiptId,
          claim.evidence.providerDeliveryId,
          1,
          evaluationFingerprint,
        );
    }
  } finally {
    database.close();
  }
});

const unavailable = () => Effect.die(new Error("unexpected test dependency call"));
const defaultHooks: AgentControlVerificationStageFinalizerHooksShape = {
  beforeTransaction: () => Effect.void,
  afterAuthoritativeRead: () => Effect.void,
  afterStageProjection: () => Effect.void,
  afterLeaseProjection: () => Effect.void,
  afterEvidence: () => Effect.void,
  afterReceipt: () => Effect.void,
  beforeMarker: () => Effect.void,
  afterCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};
const noopProviderAdmissionRelease: ProviderAdmissionReleaseAuthorityShape = {
  releaseInTransaction: () => Effect.succeed(null),
  signalCommitted: () => Effect.void,
  recover: Effect.void,
};

const buildRuntime = (
  filename: string,
  claim: AgentControlVerificationClaim,
  scope: Scope.Scope,
  hooks: AgentControlVerificationStageFinalizerHooksShape = defaultHooks,
  providerAdmissionRelease: ProviderAdmissionReleaseAuthorityShape = noopProviderAdmissionRelease,
) =>
  Effect.gen(function* () {
    const sqlContext = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
    const sql = Context.get(sqlContext, SqlClient.SqlClient);
    const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
    const stageEvents = Context.get(
      yield* Layer.buildWithScope(StageEventStoreLive.pipe(Layer.provide(sqlLayer)), scope),
      AgentControlStageRunEventStore,
    );
    const stageStates = Context.get(
      yield* Layer.buildWithScope(StageStateRepositoryLive.pipe(Layer.provide(sqlLayer)), scope),
      AgentControlStageRunStateRepository,
    );
    const leaseEvents = Context.get(
      yield* Layer.buildWithScope(LeaseEventStoreLive.pipe(Layer.provide(sqlLayer)), scope),
      AgentControlStageRunLeaseEventStore,
    );
    const leaseStates = Context.get(
      yield* Layer.buildWithScope(LeaseStateRepositoryLive.pipe(Layer.provide(sqlLayer)), scope),
      AgentControlStageRunLeaseStateRepository,
    );
    const cursors = Context.get(
      yield* Layer.buildWithScope(
        AgentControlProjectionStateRepositoryLive.pipe(Layer.provide(sqlLayer)),
        scope,
      ),
      AgentControlProjectionStateRepository,
    );
    const projectionDeps = Layer.mergeAll(
      sqlLayer,
      Layer.succeed(AgentControlStageRunEventStore, stageEvents),
      Layer.succeed(AgentControlStageRunStateRepository, stageStates),
      Layer.succeed(AgentControlStageRunLeaseEventStore, leaseEvents),
      Layer.succeed(AgentControlStageRunLeaseStateRepository, leaseStates),
      Layer.succeed(AgentControlProjectionStateRepository, cursors),
    );
    const stageProjection = Context.get(
      yield* Layer.buildWithScope(StageProjectionLive.pipe(Layer.provide(projectionDeps)), scope),
      AgentControlStageRunProjection,
    );
    const leaseProjection = Context.get(
      yield* Layer.buildWithScope(LeaseProjectionLive.pipe(Layer.provide(projectionDeps)), scope),
      AgentControlStageRunLeaseProjection,
    );
    const stagePublications = yield* Ref.make(0);
    const leasePublications = yield* Ref.make(0);
    const stageEngine = AgentControlStageRunEngine.of({
      get: unavailable,
      dispatchPreparedController: unavailable,
      replayAccepted: unavailable,
      publishCommitted: (events) => Ref.update(stagePublications, (count) => count + events.length),
      rebuild: unavailable(),
      streamDomainEvents: Stream.never,
      subscribeDomainEvents: Effect.succeed(Stream.never),
    } satisfies AgentControlStageRunEngineShape);
    const leaseEngine = AgentControlStageRunLeaseEngine.of({
      dispatchController: unavailable,
      dispatchSystem: unavailable,
      toView: unavailable,
      runtimeHolderId: unavailable(),
      rebuild: unavailable(),
      publishCommitted: (events) => Ref.update(leasePublications, (count) => count + events.length),
      streamDomainEvents: Stream.never,
      subscribeDomainEvents: Effect.succeed(Stream.never),
    } satisfies AgentControlStageRunLeaseEngineShape);
    const store = AgentControlVerificationHandoffStore.of({
      loadAcceptedByHandoffId: (handoffId) =>
        Effect.succeed(handoffId === claim.evidence.handoffId ? Option.some(claim) : Option.none()),
      insertAcceptedInTransaction: unavailable,
      loadAcceptedByTurnRequestCommandId: unavailable,
      loadAcceptedByThreadId: unavailable,
      listRecoverable: unavailable,
      isHandoffOwnedTurnRequest: unavailable,
      loadTurnAcceptance: unavailable,
      markTurnAccepted: unavailable,
      claim: unavailable,
      markDeliveryAttempted: unavailable,
      markProviderStarted: unavailable,
      scheduleRetry: unavailable,
      markAmbiguous: unavailable,
      observeProviderStarted: unavailable,
      observeProviderTerminal: unavailable,
      listStageStartCandidates: unavailable,
      listStageFinalizationCandidates: (afterHandoffId = "", limit = 64) =>
        Effect.succeed(
          claim.evidence.handoffId > afterHandoffId && limit > 0 ? [claim.evidence.handoffId] : [],
        ),
    } satisfies AgentControlVerificationHandoffStoreShape);
    const evaluator = AgentControlVerificationEvaluator.of({
      processHandoff: () =>
        Effect.succeed({ _tag: "Replayed", evaluationId: "fixture-evaluation" }),
      recover: Effect.void,
      prepare: () => Effect.void,
      drain: Effect.void,
    });
    const finalizerDeps = Layer.mergeAll(
      projectionDeps,
      Layer.succeed(AgentControlStageRunProjection, stageProjection),
      Layer.succeed(AgentControlStageRunLeaseProjection, leaseProjection),
      Layer.succeed(AgentControlStageRunEngine, stageEngine),
      Layer.succeed(AgentControlStageRunLeaseEngine, leaseEngine),
      Layer.succeed(AgentControlVerificationHandoffStore, store),
      Layer.succeed(AgentControlVerificationEvaluator, evaluator),
      Layer.succeed(AgentControlVerificationStageFinalizerHooks, hooks),
      Layer.succeed(ProviderAdmissionReleaseAuthority, providerAdmissionRelease),
    );
    const finalizer = Context.get(
      yield* Layer.buildWithScope(
        AgentControlVerificationStageFinalizerLive.pipe(Layer.provide(finalizerDeps)),
        scope,
      ),
      AgentControlVerificationStageFinalizer,
    );
    return { finalizer, leasePublications, sql, stagePublications } as const;
  });

const counts = (sql: SqlClient.SqlClient) =>
  sql<{
    readonly events: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
  }>`
    SELECT
      (SELECT count(*) FROM agent_control_events WHERE event_type IN (
        'agentControl.stageRun.verificationSucceeded',
        'agentControl.stageRun.verificationFailed',
        'agentControl.stageRun.verificationCancelled',
        'agentControl.stageRunLease.releasedAfterVerification'
      )) AS events,
      (SELECT count(*) FROM agent_control_verification_finalization_evidence) AS evidence,
      (SELECT count(*) FROM agent_control_verification_finalization_receipts) AS receipts,
      (SELECT count(*) FROM agent_control_verification_finalization_markers) AS markers
  `;

it.live.each([
  ["passed", "succeeded", "verification-passed"],
  ["failed-verdict", "failed", "verification-failed"],
  ["invalid-output", "failed", "verification-invalid-output"],
  ["delivery-failed", "failed", "provider-delivery-failed"],
  ["interrupted", "cancelled", "provider-delivery-interrupted"],
] as const)(
  "finalizes %s and releases its lease in one durable boundary",
  ([outcome, status, cause]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: `verification-finalizer-${outcome}-`,
        });
        const filename = `${directory}/state.sqlite`;
        const claim = yield* makeClaim(outcome, outcome);
        yield* seedAuthority(filename, claim, outcome);
        const releaseCalls = yield* Ref.make(0);
        const releaseSignals = yield* Ref.make<ReadonlyArray<string | null>>([]);
        const markerCountFromFreshConnection = () => {
          const observer = new NodeSqlite.DatabaseSync(filename);
          try {
            return Number(
              (
                observer
                  .prepare(
                    `SELECT count(*) AS count
                     FROM main.agent_control_verification_finalization_markers
                     WHERE handoff_id=?`,
                  )
                  .get(claim.evidence.handoffId) as { readonly count: number }
              ).count,
            );
          } finally {
            observer.close();
          }
        };
        const runtime = yield* buildRuntime(filename, claim, scope, defaultHooks, {
          releaseInTransaction: ({ stage, handoffId }) =>
            Effect.gen(function* () {
              assert.equal(stage, "verification");
              assert.equal(handoffId, claim.evidence.handoffId);
              assert.equal(markerCountFromFreshConnection(), 0);
              yield* Ref.update(releaseCalls, (count) => count + 1);
              return "provider-release-verification";
            }),
          signalCommitted: (providerInstanceId) =>
            Effect.gen(function* () {
              assert.equal(markerCountFromFreshConnection(), 1);
              yield* Ref.update(releaseSignals, (current) => [...current, providerInstanceId]);
            }),
          recover: Effect.void,
        });
        const result = yield* runtime.finalizer.processHandoff(claim.evidence.handoffId);
        assert.equal(result._tag, "Finalized");
        assert.deepStrictEqual(yield* counts(runtime.sql), [
          { events: 2, evidence: 1, receipts: 1, markers: 1 },
        ]);
        assert.deepStrictEqual(
          yield* runtime.sql`SELECT status FROM agent_control_stage_run_states`,
          [{ status }],
        );
        assert.deepStrictEqual(
          yield* runtime.sql`SELECT status FROM agent_control_stage_run_lease_states`,
          [{ status: "released" }],
        );
        assert.deepStrictEqual(
          yield* runtime.sql`SELECT terminal_cause AS cause FROM agent_control_verification_finalization_evidence`,
          [{ cause }],
        );
        assert.equal(yield* Ref.get(runtime.stagePublications), 1);
        assert.equal(yield* Ref.get(runtime.leasePublications), 1);
        assert.equal(yield* Ref.get(releaseCalls), 1);
        assert.deepStrictEqual(yield* Ref.get(releaseSignals), ["provider-release-verification"]);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("rolls the finalization boundary back when provider release rejects", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "verification-finalizer-release-rollback-",
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim("release-rollback", "delivery-failed");
      yield* seedAuthority(filename, claim, "delivery-failed");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const releaseSignals = yield* Ref.make(0);
      const runtime = yield* buildRuntime(filename, claim, scope, defaultHooks, {
        releaseInTransaction: () =>
          Effect.fail(
            new ProviderAdmissionError({
              operation: "verification-finalizer-test",
              reason: "authority-divergent",
            }),
          ),
        signalCommitted: () => Ref.update(releaseSignals, (count) => count + 1),
        recover: Effect.void,
      });
      const stateBefore = yield* runtime.sql<{
        readonly lease: string;
        readonly stage: string;
      }>`
        SELECT stage.status AS stage,lease.status AS lease
        FROM main.agent_control_stage_run_states stage
        JOIN main.agent_control_stage_run_lease_states lease
          ON lease.stage_run_id=stage.stage_run_id
      `;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(runtime.finalizer.processHandoff(claim.evidence.handoffId)),
        ),
      );
      assert.deepStrictEqual(yield* counts(runtime.sql), [
        { events: 0, evidence: 0, receipts: 0, markers: 0 },
      ]);
      assert.deepStrictEqual(
        yield* runtime.sql<{
          readonly lease: string;
          readonly stage: string;
        }>`
          SELECT stage.status AS stage,lease.status AS lease
          FROM main.agent_control_stage_run_states stage
          JOIN main.agent_control_stage_run_lease_states lease
            ON lease.stage_run_id=stage.stage_run_id
        `,
        stateBefore,
      );
      assert.equal(yield* Ref.get(runtime.stagePublications), 0);
      assert.equal(yield* Ref.get(runtime.leasePublications), 0);
      assert.equal(yield* Ref.get(releaseSignals), 0);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("targets MAIN authority when TEMP shadows every Stage and Lease persistence seam", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "verification-finalizer-temp-shadow-",
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim("temp-shadow", "passed");
      yield* seedAuthority(filename, claim, "passed");
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const runtime = yield* buildRuntime(filename, claim, scope);
      for (const table of [
        "agent_control_events",
        "agent_control_stage_run_states",
        "agent_control_stage_run_lease_states",
        "agent_control_projection_state",
      ] as const) {
        yield* runtime.sql.unsafe(`CREATE TEMP TABLE ${table}(sentinel TEXT)`).unprepared;
      }

      assert.equal(
        (yield* runtime.finalizer.processHandoff(claim.evidence.handoffId))._tag,
        "Finalized",
      );
      assert.deepStrictEqual(
        yield* runtime.sql`
          SELECT
            (SELECT count(*) FROM main.agent_control_events WHERE event_type IN (
              'agentControl.stageRun.verificationSucceeded',
              'agentControl.stageRunLease.releasedAfterVerification'
            )) AS events,
            (SELECT status FROM main.agent_control_stage_run_states) AS stage,
            (SELECT status FROM main.agent_control_stage_run_lease_states) AS lease
        `,
        [{ events: 2, stage: "succeeded", lease: "released" }],
      );
      for (const table of [
        "agent_control_events",
        "agent_control_stage_run_states",
        "agent_control_stage_run_lease_states",
        "agent_control_projection_state",
      ] as const) {
        assert.deepStrictEqual(
          yield* runtime.sql.unsafe(`SELECT count(*) AS count FROM temp.${table}`).unprepared,
          [{ count: 0 }],
          table,
        );
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "loads every claim and historical source from MAIN despite connection-local TEMP shadows",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "verification-finalizer-source-temp-shadow-",
        });
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const context = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename: `${directory}/state.sqlite` }),
          scope,
        );
        const sql = Context.get(context, SqlClient.SqlClient);
        yield* runMigrations({ toMigrationInclusive: 61 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        for (const table of [
          "agent_control_verification_handoff_intents",
          "agent_control_verification_handoff_receipts",
          "agent_control_verification_handoff_accepted",
          "agent_control_verification_materialization_evidence",
          "agent_control_verification_materialization_receipts",
          "agent_control_verification_materialization_markers",
          "agent_control_verification_admission_evidence",
          "agent_control_verification_admission_receipts",
          "agent_control_verification_admission_markers",
          "agent_control_verification_deliveries",
          "agent_control_events",
          "agent_control_task_states",
          "agent_control_worktree_stream_catalog",
          "agent_control_worktree_event_envelopes",
          "agent_control_worktree_reservation_states",
        ] as const) {
          yield* sql.unsafe(`CREATE TEMP TABLE ${table}(sentinel TEXT)`).unprepared;
        }
        const storeContext = yield* Layer.buildWithScope(
          AgentControlVerificationHandoffStoreLive.pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
          ),
          scope,
        );
        const store = Context.get(storeContext, AgentControlVerificationHandoffStore);
        assert.isTrue(Option.isNone(yield* store.loadAcceptedByHandoffId("missing-handoff")));

        const taskExit = yield* Effect.exit(
          loadAgentControlVerificationTaskAuthorityInTransaction(
            sql,
            AgentControlTaskId.make("missing-task"),
            1,
          ),
        );
        assert.isTrue(Exit.isFailure(taskExit));
        if (Exit.isFailure(taskExit)) {
          assert.include(
            Cause.pretty(taskExit.cause),
            "AgentControlVerificationHistoricalAuthorityError",
          );
        }
        const worktreeExit = yield* Effect.exit(
          loadAgentControlVerificationWorktreeAuthorityInTransaction(
            sql,
            AgentControlWorktreeReservationId.make("missing-worktree"),
          ),
        );
        assert.isTrue(Exit.isFailure(worktreeExit));
        if (Exit.isFailure(worktreeExit)) {
          assert.include(
            Cause.pretty(worktreeExit.cause),
            "AgentControlVerificationHistoricalAuthorityError",
          );
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("replays identically after restart without DML, hooks, revision, or publication", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "verification-finalizer-replay-",
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim("replay", "passed");
      yield* seedAuthority(filename, claim, "passed");
      const firstScope = yield* Scope.make("sequential");
      const first = yield* buildRuntime(filename, claim, firstScope);
      assert.equal(
        (yield* first.finalizer.processHandoff(claim.evidence.handoffId))._tag,
        "Finalized",
      );
      yield* Scope.close(firstScope, Exit.void);

      const replayHooks = yield* Ref.make(0);
      const hook = () => Ref.update(replayHooks, (count) => count + 1);
      const secondScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
      const second = yield* buildRuntime(filename, claim, secondScope, {
        ...defaultHooks,
        beforeTransaction: hook,
        afterAuthoritativeRead: hook,
        afterStageProjection: hook,
        afterLeaseProjection: hook,
        afterEvidence: hook,
        afterReceipt: hook,
        beforeMarker: hook,
        afterCommit: hook,
        afterPublication: hook,
      });
      const before = yield* second.sql<{
        readonly changes: number;
      }>`SELECT total_changes() AS changes`;
      assert.equal(
        (yield* second.finalizer.processHandoff(claim.evidence.handoffId))._tag,
        "Replayed",
      );
      const after = yield* second.sql<{
        readonly changes: number;
      }>`SELECT total_changes() AS changes`;
      assert.deepStrictEqual(after, before);
      assert.equal(yield* Ref.get(replayHooks), 0);
      assert.equal(yield* Ref.get(second.stagePublications), 0);
      assert.equal(yield* Ref.get(second.leasePublications), 0);
      assert.deepStrictEqual(yield* counts(second.sql), [
        { events: 2, evidence: 1, receipts: 1, markers: 1 },
      ]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails closed on partial, divergent, corrupt, or identity-conflicting replay chains", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      for (const corruption of [
        "partial",
        "receipt-fingerprint",
        "receipt-identity",
        "marker-identity",
        "evidence-storage",
        "evidence-identity",
        "evaluation-identity",
      ] as const) {
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: `verification-finalizer-${corruption}-`,
        });
        const filename = `${directory}/state.sqlite`;
        const claim = yield* makeClaim(corruption, "passed");
        yield* seedAuthority(filename, claim, "passed");
        const firstScope = yield* Scope.make("sequential");
        const first = yield* buildRuntime(filename, claim, firstScope);
        assert.equal(
          (yield* first.finalizer.processHandoff(claim.evidence.handoffId))._tag,
          "Finalized",
        );
        yield* Scope.close(firstScope, Exit.void);

        const native = new NodeSqlite.DatabaseSync(filename);
        const corruptionSql =
          corruption === "partial"
            ? "DELETE FROM agent_control_verification_finalization_markers"
            : corruption === "receipt-fingerprint"
              ? "UPDATE agent_control_verification_finalization_receipts SET finalization_fingerprint = 'divergent'"
              : corruption === "receipt-identity"
                ? "UPDATE agent_control_verification_finalization_receipts SET finalization_command_id = 'divergent'"
                : corruption === "marker-identity"
                  ? "UPDATE agent_control_verification_finalization_markers SET finalization_command_id = 'divergent'"
                  : corruption === "evidence-storage"
                    ? "UPDATE agent_control_verification_finalization_evidence SET stage_run_id = CAST(stage_run_id AS BLOB)"
                    : corruption === "evidence-identity"
                      ? "UPDATE agent_control_verification_finalization_evidence SET project_id = 'divergent'"
                      : "UPDATE agent_control_verification_finalization_evidence SET evaluation_id = 'divergent'";
        native.exec(corruptionSql);
        native.close();

        const replayScope = yield* Scope.make("sequential");
        const replay = yield* buildRuntime(filename, claim, replayScope);
        const before = yield* replay.sql<{
          readonly changes: number;
        }>`SELECT total_changes() AS changes`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(replay.finalizer.processHandoff(claim.evidence.handoffId)),
          ),
          corruption,
        );
        assert.deepStrictEqual(
          yield* replay.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          before,
        );
        assert.equal(yield* Ref.get(replay.stagePublications), 0);
        assert.equal(yield* Ref.get(replay.leasePublications), 0);
        yield* Scope.close(replayScope, Exit.void);
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("fails closed when any sealed Stage or Lease payload field diverges", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const mutations = [
        ["stage", "$.rawOutput", "secret"],
        ["stage", "$.evaluation.report", "secret"],
        ["stage", "$.admissionEvidenceId", "divergent"],
        ["stage", "$.admissionReceiptId", "divergent"],
        ["stage", "$.admissionMarkerId", "divergent"],
        ["stage", "$.materializationEvidenceId", "divergent"],
        ["stage", "$.materializationReceiptId", "divergent"],
        ["stage", "$.materializationMarkerId", "divergent"],
        ["stage", "$.startEvidenceId", "divergent"],
        ["stage", "$.startReceiptId", "divergent"],
        ["stage", "$.startMarkerId", "divergent"],
        ["stage", "$.controlledThreadReservationId", "divergent"],
        ["stage", "$.threadId", "divergent"],
        ["stage", "$.planningThreadId", "divergent"],
        ["stage", "$.planId", "divergent"],
        ["stage", "$.proposedPlanDigest", "divergent"],
        ["stage", "$.runtimeMode", "full-access"],
        ["stage", "$.modelSelectionFingerprint", "divergent"],
        ["stage", "$.claimGeneration", 99],
        ["stage", "$.attemptCount", 99],
        ["lease", "$.rawOutput", "secret"],
        ["lease", "$.evaluation.report", "secret"],
        ["lease", "$.admissionEvidenceId", "divergent"],
        ["lease", "$.materializationMarkerId", "divergent"],
        ["lease", "$.controlledThreadReservationId", "divergent"],
        ["lease", "$.threadId", "divergent"],
        ["lease", "$.planningThreadId", "divergent"],
        ["lease", "$.planId", "divergent"],
        ["lease", "$.proposedPlanDigest", "divergent"],
        ["lease", "$.modelSelectionFingerprint", "divergent"],
      ] as const;
      for (const [target, path, value] of mutations) {
        const suffix = `${target}-${path.slice(2)}`;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: `verification-finalizer-seal-${suffix}-`,
        });
        const filename = `${directory}/state.sqlite`;
        const claim = yield* makeClaim(suffix, "passed");
        yield* seedAuthority(filename, claim, "passed");
        const firstScope = yield* Scope.make("sequential");
        const first = yield* buildRuntime(filename, claim, firstScope);
        assert.equal(
          (yield* first.finalizer.processHandoff(claim.evidence.handoffId))._tag,
          "Finalized",
        );
        yield* Scope.close(firstScope, Exit.void);

        const native = new NodeSqlite.DatabaseSync(filename);
        native
          .prepare(`UPDATE agent_control_events SET payload_json = json_set(payload_json, ?, ?)
            WHERE event_type = ?`)
          .run(
            path,
            value,
            target === "stage"
              ? "agentControl.stageRun.verificationSucceeded"
              : "agentControl.stageRunLease.releasedAfterVerification",
          );
        native.close();

        const replayScope = yield* Scope.make("sequential");
        const replay = yield* buildRuntime(filename, claim, replayScope);
        const before = yield* replay.sql<{
          readonly changes: number;
        }>`SELECT total_changes() AS changes`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(replay.finalizer.processHandoff(claim.evidence.handoffId)),
          ),
          suffix,
        );
        assert.deepStrictEqual(
          yield* replay.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          before,
          suffix,
        );
        assert.equal(yield* Ref.get(replay.stagePublications), 0, suffix);
        assert.equal(yield* Ref.get(replay.leasePublications), 0, suffix);
        yield* Scope.close(replayScope, Exit.void);
      }
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "rejects a coherently resealed terminal payload forgery against durable source authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "verification-finalizer-coherent-forgery-",
        });
        const filename = `${directory}/state.sqlite`;
        const claim = yield* makeClaim("coherent-forgery", "passed");
        yield* seedAuthority(filename, claim, "passed");
        const firstScope = yield* Scope.make("sequential");
        const first = yield* buildRuntime(filename, claim, firstScope);
        assert.equal(
          (yield* first.finalizer.processHandoff(claim.evidence.handoffId))._tag,
          "Finalized",
        );
        yield* Scope.close(firstScope, Exit.void);

        const native = new NodeSqlite.DatabaseSync(filename);
        const row = native
          .prepare(`SELECT finalization_json AS finalizationJson
          FROM agent_control_verification_finalization_evidence`)
          .get() as { readonly finalizationJson: string };
        const parsed = parseCanonicalJson(row.finalizationJson) as Record<string, unknown> & {
          readonly stagePayload: Record<string, unknown>;
          readonly leasePayload: Record<string, unknown>;
        };
        const stagePayload = { ...parsed.stagePayload, planningThreadId: "forged-planning-thread" };
        const leasePayload = { ...parsed.leasePayload, planningThreadId: "forged-planning-thread" };
        const forgedDocument = { ...parsed, stagePayload, leasePayload };
        const finalizationJson = canonicalJson(forgedDocument as JsonValue);
        const finalizationFingerprint = fingerprintVerificationTurn("finalization-evidence", [
          finalizationJson,
        ]);
        const markerFingerprint = fingerprintVerificationTurn("finalization-marker", [
          String(parsed.handoffId),
          String(parsed.handoffFingerprint),
          String(parsed.finalizationCommandId),
          String(parsed.finalizationEvidenceId),
          finalizationFingerprint,
          String(parsed.stageEventId),
          String(parsed.stageEventSequence),
          String(parsed.leaseEventId),
          String(parsed.leaseEventSequence),
          String(parsed.finalizedAt),
        ]);
        native.exec("BEGIN IMMEDIATE");
        native
          .prepare(`UPDATE agent_control_events SET payload_json = ?
          WHERE event_type = 'agentControl.stageRun.verificationSucceeded'`)
          .run(canonicalJson(stagePayload as JsonValue));
        native
          .prepare(`UPDATE agent_control_events SET payload_json = ?
          WHERE event_type = 'agentControl.stageRunLease.releasedAfterVerification'`)
          .run(canonicalJson(leasePayload as JsonValue));
        native
          .prepare(`UPDATE agent_control_verification_finalization_evidence
          SET finalization_json = ?, finalization_fingerprint = ?`)
          .run(finalizationJson, finalizationFingerprint);
        native
          .prepare(`UPDATE agent_control_verification_finalization_receipts
          SET finalization_fingerprint = ?`)
          .run(finalizationFingerprint);
        native
          .prepare(`UPDATE agent_control_verification_finalization_markers
          SET finalization_fingerprint = ?, marker_fingerprint = ?`)
          .run(finalizationFingerprint, markerFingerprint);
        native.exec("COMMIT");
        native.close();

        const replayScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(replayScope, Exit.void));
        const replay = yield* buildRuntime(filename, claim, replayScope);
        const before = yield* replay.sql<{
          readonly changes: number;
        }>`SELECT total_changes() AS changes`;
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(replay.finalizer.processHandoff(claim.evidence.handoffId)),
          ),
        );
        assert.deepStrictEqual(
          yield* replay.sql<{ readonly changes: number }>`SELECT total_changes() AS changes`,
          before,
        );
        assert.equal(yield* Ref.get(replay.stagePublications), 0);
        assert.equal(yield* Ref.get(replay.leasePublications), 0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live.each([
  "afterStageProjection",
  "afterLeaseProjection",
  "afterEvidence",
  "afterReceipt",
  "beforeMarker",
] as const)("rolls back after %s and succeeds on a fresh restart", (faultPoint) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: `verification-finalizer-rollback-${faultPoint}-`,
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim(faultPoint, "passed");
      yield* seedAuthority(filename, claim, "passed");
      const failedScope = yield* Scope.make("sequential");
      const failed = yield* buildRuntime(filename, claim, failedScope, {
        ...defaultHooks,
        [faultPoint]: () => Effect.die(new Error(`injected-${faultPoint}`)),
      });
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(failed.finalizer.processHandoff(claim.evidence.handoffId)),
        ),
      );
      assert.deepStrictEqual(yield* counts(failed.sql), [
        { events: 0, evidence: 0, receipts: 0, markers: 0 },
      ]);
      assert.deepStrictEqual(
        yield* failed.sql`SELECT status, revision FROM agent_control_stage_run_states`,
        [{ status: "running", revision: 2 }],
      );
      assert.deepStrictEqual(
        yield* failed.sql`SELECT status, revision FROM agent_control_stage_run_lease_states`,
        [{ status: "reserved", revision: 5 }],
      );
      yield* Scope.close(failedScope, Exit.void);

      const retryScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(retryScope, Exit.void));
      const retry = yield* buildRuntime(filename, claim, retryScope);
      assert.equal(
        (yield* retry.finalizer.processHandoff(claim.evidence.handoffId))._tag,
        "Finalized",
      );
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live(
  "fails closed without a complete Evaluation chain and on unexpected Evaluation authority",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        for (const [suffix, outcome, seedOutcome] of [
          ["missing-evaluation", "passed", "delivery-failed"],
          ["unexpected-evaluation", "delivery-failed", "passed"],
          ["start-identity", "passed", "passed"],
        ] as const) {
          const directory = yield* fs.makeTempDirectoryScoped({
            prefix: `verification-finalizer-${suffix}-`,
          });
          const filename = `${directory}/state.sqlite`;
          const claim = yield* makeClaim(suffix, outcome);
          yield* seedAuthority(filename, claim, seedOutcome);
          if (suffix === "start-identity") {
            const native = new NodeSqlite.DatabaseSync(filename);
            native
              .prepare(`UPDATE agent_control_events
                SET payload_json = json_set(payload_json, '$.handoffId', 'divergent')
                WHERE event_type = 'agentControl.stageRun.verificationStarted'`)
              .run();
            native.close();
          }
          const scope = yield* Scope.make("sequential");
          const runtime = yield* buildRuntime(filename, claim, scope);
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(runtime.finalizer.processHandoff(claim.evidence.handoffId)),
            ),
          );
          assert.deepStrictEqual(yield* counts(runtime.sql), [
            { events: 0, evidence: 0, receipts: 0, markers: 0 },
          ]);
          assert.equal(yield* Ref.get(runtime.stagePublications), 0);
          assert.equal(yield* Ref.get(runtime.leasePublications), 0);
          yield* Scope.close(scope, Exit.void);
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("recovers keyset candidates and recreates an attempt-local worker after close", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "verification-finalizer-recovery-",
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim("recovery", "passed");
      yield* seedAuthority(filename, claim, "passed");
      const runtimeScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
      const runtime = yield* buildRuntime(filename, claim, runtimeScope, {
        ...defaultHooks,
        recoveryPageSize: 1,
      });

      const firstOwner = yield* Scope.make("sequential");
      yield* runtime.finalizer.prepare(Effect.void).pipe(Scope.provide(firstOwner));
      yield* runtime.finalizer.drain;
      assert.deepStrictEqual(yield* counts(runtime.sql), [
        { events: 2, evidence: 1, receipts: 1, markers: 1 },
      ]);
      yield* Scope.close(firstOwner, Exit.void);

      const secondOwner = yield* Scope.make("sequential");
      yield* runtime.finalizer.prepare(Effect.void).pipe(Scope.provide(secondOwner));
      yield* runtime.finalizer.drain;
      assert.deepStrictEqual(yield* counts(runtime.sql), [
        { events: 2, evidence: 1, receipts: 1, markers: 1 },
      ]);
      yield* Scope.close(secondOwner, Exit.void);
      assert.equal(yield* Ref.get(runtime.stagePublications), 1);
      assert.equal(yield* Ref.get(runtime.leasePublications), 1);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.live("lets one of two WAL connections win and the loser replay without publication", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "verification-finalizer-race-",
      });
      const filename = `${directory}/state.sqlite`;
      const claim = yield* makeClaim("race", "passed");
      yield* seedAuthority(filename, claim, "passed");
      const arrived = yield* Ref.make(0);
      const release = yield* Deferred.make<void>();
      const barrier = () =>
        Ref.updateAndGet(arrived, (count) => count + 1).pipe(
          Effect.tap((count) => (count === 2 ? Deferred.succeed(release, undefined) : Effect.void)),
          Effect.andThen(Deferred.await(release)),
        );
      const hooks = { ...defaultHooks, beforeTransaction: barrier };
      const scopeA = yield* Scope.make("sequential");
      const scopeB = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
      const runtimeA = yield* buildRuntime(filename, claim, scopeA, hooks);
      const runtimeB = yield* buildRuntime(filename, claim, scopeB, hooks);
      const fiberA = yield* Effect.forkChild(
        runtimeA.finalizer.processHandoff(claim.evidence.handoffId),
      );
      const fiberB = yield* Effect.forkChild(
        runtimeB.finalizer.processHandoff(claim.evidence.handoffId),
      );
      const results = [yield* Fiber.join(fiberA), yield* Fiber.join(fiberB)];
      assert.deepStrictEqual(results.map((result) => result._tag).toSorted(), [
        "Finalized",
        "Replayed",
      ]);
      assert.equal(
        (yield* Ref.get(runtimeA.stagePublications)) + (yield* Ref.get(runtimeB.stagePublications)),
        1,
      );
      assert.equal(
        (yield* Ref.get(runtimeA.leasePublications)) + (yield* Ref.get(runtimeB.leasePublications)),
        1,
      );
      assert.deepStrictEqual(yield* counts(runtimeA.sql), [
        { events: 2, evidence: 1, receipts: 1, markers: 1 },
      ]);
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
