import {
  AgentControlControlledThreadReservationId,
  AgentControlTaskState,
  CommandId,
  MessageId,
  ModelSelection,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
} from "../../initialPlanning/eventEvidence.ts";
import { fingerprintAgentControlSourceIdentity } from "../../stageRun/identity.ts";
import {
  implementationHandoffAuthorityMismatch,
  type AgentControlImplementationHandoffAuthority,
} from "../handoffValidation.ts";
import type { AgentControlImplementationClaim } from "../model.ts";
import { canonicalAgentControlImplementationPromptSource } from "../prompt.ts";
import {
  AgentControlImplementationHandoffStore,
  AgentControlImplementationStoreError,
  type AgentControlImplementationHandoffStoreShape,
  type AgentControlImplementationTurnAcceptance,
} from "../Services/AgentControlImplementationHandoffStore.ts";

const EvidenceRow = Schema.Struct({
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  materializationEvidenceId: Schema.String,
  materializationReceiptId: Schema.String,
  materializationMarkerId: Schema.String,
  admissionEvidenceId: Schema.String,
  admissionReceiptId: Schema.String,
  admissionMarkerId: Schema.String,
  projectId: ProjectId,
  taskId: Schema.String,
  taskRevision: Schema.Int,
  githubIntakeSequence: Schema.Int,
  sourceIdentityFingerprint: Schema.String,
  taskSourceEventId: Schema.String,
  taskSourceEventSequence: Schema.Int,
  taskSourceEventStreamVersion: Schema.Int,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Int,
  worktreeReservationId: Schema.String,
  worktreeRevision: Schema.Int,
  worktreeEventSequence: Schema.Int,
  worktreeOwnershipFingerprint: Schema.String,
  worktreeVerifiedAt: Schema.String,
  worktreePath: Schema.String,
  branch: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: Schema.String,
  proposedPlanDigest: Schema.String,
  providerInstanceId: ProviderInstanceId,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionJson: Schema.String,
  modelSelectionFingerprint: Schema.String,
  templateVersion: Schema.Literal("agent-control-implementation-prompt-v1"),
  promptText: Schema.String,
  promptDigest: Schema.String,
  turnRequestCommandId: CommandId,
  messageId: MessageId,
  messageEventId: Schema.String,
  turnRequestEventId: Schema.String,
  messageEventTemplateJson: Schema.String,
  turnRequestEventTemplateJson: Schema.String,
  eventTemplateDigest: Schema.String,
  providerDeliveryId: Schema.String,
  createdAt: Schema.String,
});

const DeliveryRow = Schema.Struct({
  providerDeliveryId: Schema.String,
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  admissionMarkerId: Schema.String,
  materializationEvidenceId: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Int,
  providerInstanceId: ProviderInstanceId,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: Schema.String,
  turnRequestCommandId: CommandId,
  messageId: MessageId,
  planningThreadId: ThreadId,
  planId: Schema.String,
  state: Schema.Literals([
    "pending",
    "turn-accepted",
    "claimed",
    "delivery-attempted",
    "provider-started",
    "retry-wait",
    "interrupt-requested",
    "ambiguous",
    "completed",
    "failed",
    "interrupted",
  ]),
  revision: Schema.Int,
  claimOwnerId: Schema.NullOr(Schema.String),
  claimGeneration: Schema.Int,
  claimExpiresAt: Schema.NullOr(Schema.String),
  attemptCount: Schema.Int,
  nextAttemptAt: Schema.NullOr(Schema.String),
  providerTurnId: Schema.NullOr(Schema.String),
  providerAcceptedAt: Schema.NullOr(Schema.String),
  providerSessionCreatedAt: Schema.NullOr(Schema.String),
  providerResumeCursorJson: Schema.NullOr(Schema.String),
  terminalAt: Schema.NullOr(Schema.String),
  lastErrorCode: Schema.NullOr(Schema.String),
  interruptRequested: Schema.Int,
  updatedAt: Schema.String,
});

const AuthorityRow = Schema.Struct({
  materializationEvidenceId: Schema.String,
  materializationReceiptId: Schema.String,
  materializationMarkerId: Schema.String,
  admissionEvidenceId: Schema.String,
  admissionReceiptId: Schema.String,
  admissionMarkerId: Schema.String,
  projectId: ProjectId,
  taskId: Schema.String,
  taskRevision: Schema.Int,
  githubIntakeSequence: Schema.Int,
  sourceIdentityFingerprint: Schema.String,
  taskSourceEventId: Schema.String,
  taskSourceEventSequence: Schema.Int,
  taskSourceEventStreamVersion: Schema.Int,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Int,
  worktreeReservationId: Schema.String,
  worktreeRevision: Schema.Int,
  worktreeEventSequence: Schema.Int,
  worktreeOwnershipFingerprint: Schema.String,
  worktreeVerifiedAt: Schema.String,
  worktreePath: Schema.String,
  branch: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: Schema.String,
  proposedPlanJson: Schema.String,
  proposedPlanDigest: Schema.String,
  repositoryDisplay: Schema.String,
  sourceRevision: Schema.String,
  taskTitle: Schema.String,
  taskBody: Schema.NullOr(Schema.String),
  providerInstanceId: ProviderInstanceId,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionJson: Schema.String,
  modelSelectionFingerprint: Schema.String,
  createdAt: Schema.String,
});

const TurnAcceptanceRow = Schema.Struct({
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  controlledThreadReservationId: Schema.String,
  threadId: ThreadId,
  planningThreadId: ThreadId,
  planId: Schema.String,
  turnRequestCommandId: CommandId,
  messageId: Schema.String,
  messageEventId: Schema.String,
  messageEventSequence: Schema.Int,
  turnRequestEventId: Schema.String,
  turnRequestEventSequence: Schema.Int,
  messageEventEnvelopeJson: Schema.String,
  turnRequestEventEnvelopeJson: Schema.String,
  eventEvidenceDigest: Schema.String,
  acceptedAt: Schema.String,
});

const TaskSourcePayload = Schema.Struct({
  taskId: Schema.String,
  source: Schema.Struct({
    projectId: ProjectId,
    repositoryNodeId: Schema.String,
    issueNodeId: Schema.String,
    issueNumber: Schema.Int,
    issueUrl: Schema.String,
  }),
  sourceUpdatedAt: Schema.String,
  githubIntakeSequence: Schema.Int,
  sourceSnapshot: Schema.Struct({
    repositoryNodeId: Schema.String,
    issueNodeId: Schema.String,
    number: Schema.Int,
    url: Schema.String,
    title: Schema.String,
    body: Schema.NullOr(Schema.String),
    updatedAt: Schema.String,
  }),
});

const decodeEvidence = Schema.decodeUnknownEffect(EvidenceRow);
const decodeDelivery = Schema.decodeUnknownEffect(DeliveryRow);
const decodeAuthority = Schema.decodeUnknownEffect(AuthorityRow);
const decodeAcceptance = Schema.decodeUnknownEffect(TurnAcceptanceRow);
const decodeTaskSourcePayload = Schema.decodeUnknownEffect(
  Schema.fromJsonString(TaskSourcePayload),
);
const decodeTaskProjection = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlTaskState),
);
const decodeModelSelection = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeModelSelection = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

const storeError = (
  operation: string,
  reason: AgentControlImplementationStoreError["reason"],
  cause?: unknown,
) =>
  new AgentControlImplementationStoreError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const candidateEvidenceError = (operation: string, cause?: unknown) =>
  storeError(operation, "candidate-evidence", cause);
const persistenceError = (operation: string, cause?: unknown) =>
  storeError(operation, "persistence", cause);
const revisionConflictError = (operation: string, cause?: unknown) =>
  storeError(operation, "revision-conflict", cause);
const isStoreError = Schema.is(AgentControlImplementationStoreError);
const preserveStoreError = (operation: string, cause: unknown) =>
  isStoreError(cause) ? cause : persistenceError(operation, cause);

const authorityFromRaw = Effect.fn("AgentControlImplementationHandoffStore.authorityFromRaw")(
  function* (raw: Record<string, unknown>) {
    const decodeBytes = (value: unknown, operation: string) =>
      Effect.try({
        try: () => decodeCanonicalUtf8Bytes(value),
        catch: (cause) => candidateEvidenceError(operation, cause),
      });
    const proposedPlanJson = yield* decodeBytes(
      raw.authorityProposedPlanBytes,
      "authority-proposed-plan-bytes",
    );
    const repositoryDisplay = yield* decodeBytes(
      raw.authorityRepositoryBytes,
      "authority-repository-bytes",
    );
    const sourceRevision = yield* decodeBytes(
      raw.authoritySourceRevisionBytes,
      "authority-source-revision-bytes",
    );
    const materializedRepositoryDisplay = yield* decodeBytes(
      raw.materializedRepositoryBytes,
      "materialized-repository-bytes",
    );
    const materializedSourceRevision = yield* decodeBytes(
      raw.materializedSourceRevisionBytes,
      "materialized-source-revision-bytes",
    );
    const materializedTaskTitle = yield* decodeBytes(
      raw.materializedTaskTitleBytes,
      "materialized-task-title-bytes",
    );
    const materializedTaskBody = yield* decodeBytes(
      raw.materializedTaskBodyBytes,
      "materialized-task-body-bytes",
    );
    const taskSourcePayloadJson = yield* decodeBytes(
      raw.authorityTaskSourcePayloadBytes,
      "authority-task-source-payload-bytes",
    );
    const modelSelectionJson = yield* decodeBytes(
      raw.authorityModelSelectionBytes,
      "authority-model-selection-bytes",
    );
    yield* Effect.try({
      try: () => {
        parseCanonicalJson(proposedPlanJson);
        parseCanonicalJson(taskSourcePayloadJson);
      },
      catch: (cause) => candidateEvidenceError("authority-proposed-plan-json", cause),
    });
    const taskSource = yield* decodeTaskSourcePayload(taskSourcePayloadJson).pipe(
      Effect.mapError((cause) => candidateEvidenceError("decode-task-source-payload", cause)),
    );
    const taskProjection =
      raw.authorityTaskProjectionBytes !== null &&
      raw.authorityTaskProjectionRevision === raw.authorityTaskRevision
        ? yield* decodeBytes(
            raw.authorityTaskProjectionBytes,
            "authority-task-projection-bytes",
          ).pipe(
            Effect.flatMap(decodeTaskProjection),
            Effect.mapError((cause) => candidateEvidenceError("decode-task-projection", cause)),
          )
        : null;
    const promptSource = canonicalAgentControlImplementationPromptSource({
      repositoryDisplay,
      sourceRevision,
      taskTitle: taskSource.sourceSnapshot.title,
      taskBody: taskSource.sourceSnapshot.body,
    });
    if (
      raw.actualTaskSourceEventId !== raw.authorityTaskSourceEventId ||
      raw.actualTaskSourceEventSequence !== raw.authorityTaskSourceEventSequence ||
      raw.actualTaskSourceEventStreamVersion !== raw.authorityTaskSourceEventStreamVersion ||
      raw.taskSourceAggregateKind !== "task" ||
      raw.taskSourceStreamId !== raw.authorityTaskId ||
      raw.taskSourceEventAuthority !== "controller" ||
      ![
        "agentControl.task.created",
        "agentControl.task.sourceGate.changed",
        "agentControl.task.needsAttentionMarked",
        "agentControl.task.sourceMissingRecovered",
      ].includes(String(raw.taskSourceEventType)) ||
      taskSource.taskId !== raw.authorityTaskId ||
      taskSource.source.projectId !== raw.authorityProjectId ||
      taskSource.githubIntakeSequence !== raw.authorityGithubIntakeSequence ||
      raw.authorityTaskSourceEventStreamVersion !== raw.authorityTaskRevision ||
      taskSource.source.repositoryNodeId !== raw.authorityWorktreeRepositoryNodeId ||
      taskSource.sourceSnapshot.repositoryNodeId !== taskSource.source.repositoryNodeId ||
      taskSource.sourceSnapshot.issueNodeId !== taskSource.source.issueNodeId ||
      taskSource.sourceSnapshot.number !== taskSource.source.issueNumber ||
      taskSource.sourceSnapshot.url !== taskSource.source.issueUrl ||
      taskSource.sourceSnapshot.updatedAt !== taskSource.sourceUpdatedAt ||
      fingerprintAgentControlSourceIdentity(taskSource.source) !==
        raw.authoritySourceIdentityFingerprint ||
      materializedRepositoryDisplay !== promptSource.repositoryDisplay ||
      materializedSourceRevision !== promptSource.sourceRevision ||
      materializedTaskTitle !== promptSource.taskTitle ||
      materializedTaskBody !== promptSource.taskBody ||
      (taskProjection !== null &&
        (taskProjection.taskId !== raw.authorityTaskId ||
          taskProjection.revision !== raw.authorityTaskRevision ||
          taskProjection.sequence !== raw.authorityTaskSourceEventSequence ||
          taskProjection.githubIntakeSequence !== raw.authorityGithubIntakeSequence ||
          taskProjection.source.projectId !== raw.authorityProjectId ||
          taskProjection.source.repositoryNodeId !== taskSource.source.repositoryNodeId ||
          taskProjection.source.issueNodeId !== taskSource.source.issueNodeId ||
          taskProjection.source.issueNumber !== taskSource.source.issueNumber ||
          taskProjection.source.issueUrl !== taskSource.source.issueUrl ||
          taskProjection.sourceUpdatedAt !== taskSource.sourceUpdatedAt ||
          taskProjection.sourceSnapshot.repositoryNodeId !==
            taskSource.sourceSnapshot.repositoryNodeId ||
          taskProjection.sourceSnapshot.issueNodeId !== taskSource.sourceSnapshot.issueNodeId ||
          taskProjection.sourceSnapshot.number !== taskSource.sourceSnapshot.number ||
          taskProjection.sourceSnapshot.url !== taskSource.sourceSnapshot.url ||
          taskProjection.sourceSnapshot.title !== taskSource.sourceSnapshot.title ||
          taskProjection.sourceSnapshot.body !== taskSource.sourceSnapshot.body ||
          taskProjection.sourceSnapshot.updatedAt !== taskSource.sourceSnapshot.updatedAt))
    ) {
      return yield* candidateEvidenceError("task-source-authority-invariant");
    }
    const authority = yield* decodeAuthority({
      materializationEvidenceId: raw.authorityMaterializationEvidenceId,
      materializationReceiptId: raw.authorityMaterializationReceiptId,
      materializationMarkerId: raw.materializationMarkerId,
      admissionEvidenceId: raw.authorityAdmissionEvidenceId,
      admissionReceiptId: raw.authorityAdmissionReceiptId,
      admissionMarkerId: raw.authorityAdmissionMarkerId,
      projectId: raw.authorityProjectId,
      taskId: raw.authorityTaskId,
      taskRevision: raw.authorityTaskRevision,
      githubIntakeSequence: raw.authorityGithubIntakeSequence,
      sourceIdentityFingerprint: raw.authoritySourceIdentityFingerprint,
      taskSourceEventId: raw.authorityTaskSourceEventId,
      taskSourceEventSequence: raw.authorityTaskSourceEventSequence,
      taskSourceEventStreamVersion: raw.authorityTaskSourceEventStreamVersion,
      stageRunId: raw.authorityStageRunId,
      attemptId: raw.authorityAttemptId,
      leaseId: raw.authorityLeaseId,
      leaseHolderId: raw.authorityLeaseHolderId,
      fenceToken: raw.authorityFenceToken,
      worktreeReservationId: raw.authorityWorktreeReservationId,
      worktreeRevision: raw.authorityWorktreeRevision,
      worktreeEventSequence: raw.authorityWorktreeEventSequence,
      worktreeOwnershipFingerprint: raw.authorityWorktreeOwnershipFingerprint,
      worktreeVerifiedAt: raw.authorityWorktreeVerifiedAt,
      worktreePath: raw.authorityWorktreePath,
      branch: raw.authorityBranch,
      controlledThreadReservationId: raw.authorityControlledThreadReservationId,
      threadId: raw.authorityThreadId,
      planningThreadId: raw.authorityPlanningThreadId,
      planId: raw.authorityPlanId,
      proposedPlanJson,
      proposedPlanDigest: raw.authorityProposedPlanDigest,
      repositoryDisplay,
      sourceRevision,
      taskTitle: taskSource.sourceSnapshot.title,
      taskBody: taskSource.sourceSnapshot.body,
      providerInstanceId: raw.authorityProviderInstanceId,
      runtimeMode: raw.authorityRuntimeMode,
      modelSelectionJson,
      modelSelectionFingerprint: raw.authorityModelSelectionFingerprint,
      createdAt: raw.authorityCreatedAt,
    }).pipe(Effect.mapError((cause) => candidateEvidenceError("decode-authority", cause)));
    const modelSelection = yield* decodeModelSelection(authority.modelSelectionJson).pipe(
      Effect.mapError((cause) => candidateEvidenceError("decode-authority-model-selection", cause)),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelection(modelSelection).pipe(
      Effect.mapError((cause) => candidateEvidenceError("encode-authority-model-selection", cause)),
    );
    if (
      canonicalModelSelectionJson !== authority.modelSelectionJson ||
      modelSelection.instanceId !== authority.providerInstanceId
    ) {
      return yield* candidateEvidenceError("authority-model-selection-invariant");
    }
    return { ...authority, modelSelection } satisfies AgentControlImplementationHandoffAuthority;
  },
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const selectAccepted = (
    predicate: string,
    parameters: ReadonlyArray<string | number>,
    limit = 1000,
  ) =>
    sql.unsafe<Record<string, unknown>>(
      `
      SELECT intent.handoff_id AS "handoffId",
        intent.handoff_fingerprint AS "handoffFingerprint",
        intent.materialization_evidence_id AS "materializationEvidenceId",
        intent.materialization_receipt_id AS "materializationReceiptId",
        marker.materialization_marker_id AS "materializationMarkerId",
        materialization.admission_evidence_id AS "admissionEvidenceId",
        materialization.admission_receipt_id AS "admissionReceiptId",
        materialization.admission_marker_id AS "admissionMarkerId",
        intent.project_id AS "projectId", intent.task_id AS "taskId",
        intent.task_revision AS "taskRevision",
        intent.github_intake_sequence AS "githubIntakeSequence",
        intent.source_identity_fingerprint AS "sourceIdentityFingerprint",
        intent.task_source_event_id AS "taskSourceEventId",
        intent.task_source_event_sequence AS "taskSourceEventSequence",
        intent.task_source_event_stream_version AS "taskSourceEventStreamVersion",
        intent.stage_run_id AS "stageRunId", intent.attempt_id AS "attemptId",
        intent.lease_id AS "leaseId", intent.lease_holder_id AS "leaseHolderId",
        intent.fence_token AS "fenceToken",
        intent.worktree_reservation_id AS "worktreeReservationId",
        intent.worktree_revision AS "worktreeRevision",
        intent.worktree_event_sequence AS "worktreeEventSequence",
        intent.worktree_ownership_fingerprint AS "worktreeOwnershipFingerprint",
        intent.worktree_verified_at AS "worktreeVerifiedAt",
        intent.worktree_path AS "worktreePath", intent.branch,
        intent.controlled_thread_reservation_id AS "controlledThreadReservationId",
        intent.thread_id AS "threadId", intent.planning_thread_id AS "planningThreadId",
        intent.plan_id AS "planId", intent.proposed_plan_digest AS "proposedPlanDigest",
        intent.provider_instance_id AS "providerInstanceId",
        intent.runtime_mode AS "runtimeMode",
        CAST(intent.model_selection_json AS BLOB) AS "modelSelectionBytes",
        intent.model_selection_fingerprint AS "modelSelectionFingerprint",
        intent.template_version AS "templateVersion",
        CAST(intent.prompt_text AS BLOB) AS "promptBytes",
        intent.prompt_digest AS "promptDigest",
        intent.turn_request_command_id AS "turnRequestCommandId",
        intent.message_id AS "messageId", intent.message_event_id AS "messageEventId",
        intent.turn_request_event_id AS "turnRequestEventId",
        CAST(intent.message_event_template_json AS BLOB) AS "messageTemplateBytes",
        CAST(intent.turn_request_event_template_json AS BLOB) AS "turnTemplateBytes",
        intent.event_template_digest AS "eventTemplateDigest",
        intent.provider_delivery_id AS "providerDeliveryId", intent.created_at AS "createdAt",
        materialization.materialization_evidence_id AS "authorityMaterializationEvidenceId",
        materialization_receipt.materialization_receipt_id AS
          "authorityMaterializationReceiptId",
        materialization.admission_evidence_id AS "authorityAdmissionEvidenceId",
        materialization.admission_receipt_id AS "authorityAdmissionReceiptId",
        materialization.admission_marker_id AS "authorityAdmissionMarkerId",
        materialization.project_id AS "authorityProjectId",
        materialization.task_id AS "authorityTaskId",
        materialization.task_revision AS "authorityTaskRevision",
        materialization.github_intake_sequence AS "authorityGithubIntakeSequence",
        materialization.source_identity_fingerprint AS "authoritySourceIdentityFingerprint",
        materialization.task_source_event_id AS "authorityTaskSourceEventId",
        materialization.task_source_event_sequence AS "authorityTaskSourceEventSequence",
        materialization.task_source_event_stream_version AS
          "authorityTaskSourceEventStreamVersion",
        materialization.stage_run_id AS "authorityStageRunId",
        materialization.attempt_id AS "authorityAttemptId",
        materialization.lease_id AS "authorityLeaseId",
        materialization.lease_holder_id AS "authorityLeaseHolderId",
        materialization.fence_token AS "authorityFenceToken",
        materialization.worktree_reservation_id AS "authorityWorktreeReservationId",
        materialization.worktree_revision AS "authorityWorktreeRevision",
        materialization.worktree_event_sequence AS "authorityWorktreeEventSequence",
        materialization.worktree_ownership_fingerprint AS
          "authorityWorktreeOwnershipFingerprint",
        materialization.worktree_verified_at AS "authorityWorktreeVerifiedAt",
        materialization.worktree_path AS "authorityWorktreePath",
        materialization.branch AS "authorityBranch",
        materialization.controlled_thread_reservation_id AS
          "authorityControlledThreadReservationId",
        materialization.thread_id AS "authorityThreadId",
        materialization.planning_thread_id AS "authorityPlanningThreadId",
        materialization.plan_id AS "authorityPlanId",
        CAST(materialization.proposed_plan_json AS BLOB) AS "authorityProposedPlanBytes",
        materialization.proposed_plan_digest AS "authorityProposedPlanDigest",
        CAST(worktree.repository_name_with_owner AS BLOB) AS "authorityRepositoryBytes",
        CAST(worktree.base_commit_sha AS BLOB) AS "authoritySourceRevisionBytes",
        CAST(materialization.repository_display AS BLOB) AS "materializedRepositoryBytes",
        CAST(materialization.source_revision AS BLOB) AS "materializedSourceRevisionBytes",
        CAST(materialization.task_title AS BLOB) AS "materializedTaskTitleBytes",
        CAST(materialization.task_body AS BLOB) AS "materializedTaskBodyBytes",
        worktree.repository_node_id AS "authorityWorktreeRepositoryNodeId",
        task_event.event_id AS "actualTaskSourceEventId",
        task_event.aggregate_kind AS "taskSourceAggregateKind",
        task_event.stream_id AS "taskSourceStreamId",
        task_event.stream_version AS "actualTaskSourceEventStreamVersion",
        task_event.sequence AS "actualTaskSourceEventSequence",
        task_event.event_type AS "taskSourceEventType",
        task_event.actor_authority AS "taskSourceEventAuthority",
        CAST(task_event.payload_json AS BLOB) AS "authorityTaskSourcePayloadBytes",
        task_projection.revision AS "authorityTaskProjectionRevision",
        CAST(task_projection.state_json AS BLOB) AS "authorityTaskProjectionBytes",
        materialization.provider_instance_id AS "authorityProviderInstanceId",
        materialization.runtime_mode AS "authorityRuntimeMode",
        CAST(materialization.model_selection_json AS BLOB) AS "authorityModelSelectionBytes",
        materialization.model_selection_fingerprint AS "authorityModelSelectionFingerprint",
        materialization.materialized_at AS "authorityCreatedAt",
        delivery.admission_marker_id AS "deliveryAdmissionMarkerId",
        delivery.materialization_evidence_id AS "deliveryMaterializationEvidenceId",
        delivery.stage_run_id AS "deliveryStageRunId",
        delivery.attempt_id AS "deliveryAttemptId", delivery.lease_id AS "deliveryLeaseId",
        delivery.lease_holder_id AS "deliveryLeaseHolderId",
        delivery.fence_token AS "deliveryFenceToken",
        delivery.runtime_mode AS "deliveryRuntimeMode",
        delivery.model_selection_fingerprint AS "deliveryModelSelectionFingerprint",
        delivery.planning_thread_id AS "deliveryPlanningThreadId",
        delivery.plan_id AS "deliveryPlanId", delivery.state, delivery.revision,
        delivery.claim_owner_id AS "claimOwnerId",
        delivery.claim_generation AS "claimGeneration",
        delivery.claim_expires_at AS "claimExpiresAt",
        delivery.attempt_count AS "attemptCount", delivery.next_attempt_at AS "nextAttemptAt",
        delivery.provider_turn_id AS "providerTurnId",
        delivery.provider_accepted_at AS "providerAcceptedAt",
        delivery.provider_session_created_at AS "providerSessionCreatedAt",
        CAST(delivery.provider_resume_cursor_json AS BLOB) AS "resumeCursorBytes",
        delivery.terminal_at AS "terminalAt", delivery.last_error_code AS "lastErrorCode",
        delivery.interrupt_requested AS "interruptRequested", delivery.updated_at AS "updatedAt"
      FROM agent_control_implementation_handoff_intents intent
      JOIN agent_control_implementation_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
      JOIN agent_control_implementation_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_implementation_materialization_evidence materialization
        ON materialization.materialization_evidence_id = intent.materialization_evidence_id
      JOIN agent_control_implementation_materialization_receipts materialization_receipt
        ON materialization_receipt.materialization_evidence_id =
          materialization.materialization_evidence_id
       AND materialization_receipt.materialization_receipt_id =
          intent.materialization_receipt_id
       AND materialization_receipt.materialization_fingerprint =
          materialization.materialization_fingerprint
       AND materialization_receipt.status = 'accepted'
      JOIN agent_control_implementation_materialization_markers marker
        ON marker.materialization_evidence_id = intent.materialization_evidence_id
       AND marker.materialization_receipt_id =
          materialization_receipt.materialization_receipt_id
       AND marker.materialization_fingerprint = materialization.materialization_fingerprint
       AND marker.handoff_id = intent.handoff_id
       AND marker.provider_delivery_id = intent.provider_delivery_id
      JOIN agent_control_implementation_admission_evidence admission
        ON admission.admission_evidence_id = materialization.admission_evidence_id
       AND admission.admission_fingerprint = materialization.admission_fingerprint
       AND admission.handoff_id = materialization.admission_handoff_id
       AND admission.project_id = materialization.project_id
       AND admission.task_id = materialization.task_id
       AND admission.task_revision = materialization.task_revision
       AND admission.github_intake_sequence = materialization.github_intake_sequence
       AND admission.source_identity_fingerprint =
          materialization.source_identity_fingerprint
       AND admission.implementation_stage_run_id = materialization.stage_run_id
       AND admission.implementation_attempt_id = materialization.attempt_id
       AND admission.implementation_lease_id = materialization.lease_id
       AND admission.implementation_lease_holder_id = materialization.lease_holder_id
       AND admission.implementation_fence_token = materialization.fence_token
       AND admission.worktree_reservation_id = materialization.worktree_reservation_id
       AND admission.worktree_revision = materialization.worktree_revision
       AND admission.worktree_event_sequence = materialization.worktree_event_sequence
       AND admission.worktree_ownership_fingerprint =
          materialization.worktree_ownership_fingerprint
       AND admission.worktree_verified_at = materialization.worktree_verified_at
       AND admission.implementation_controlled_thread_reservation_id =
          materialization.controlled_thread_reservation_id
       AND admission.implementation_thread_id = materialization.thread_id
       AND admission.planning_thread_id = materialization.planning_thread_id
       AND admission.plan_id = materialization.plan_id
       AND admission.proposed_plan_json = materialization.proposed_plan_json
       AND admission.proposed_plan_digest = materialization.proposed_plan_digest
      JOIN agent_control_implementation_admission_receipts admission_receipt
        ON admission_receipt.admission_evidence_id = admission.admission_evidence_id
       AND admission_receipt.receipt_id = materialization.admission_receipt_id
       AND admission_receipt.admission_command_id = admission.admission_command_id
       AND admission_receipt.admission_fingerprint = admission.admission_fingerprint
       AND admission_receipt.handoff_id = admission.handoff_id
      JOIN agent_control_implementation_admission_markers admission_marker
        ON admission_marker.admission_evidence_id = admission.admission_evidence_id
       AND admission_marker.receipt_id = admission_receipt.receipt_id
       AND admission_marker.marker_id = materialization.admission_marker_id
       AND admission_marker.admission_command_id = admission.admission_command_id
       AND admission_marker.handoff_id = admission.handoff_id
       AND admission_marker.marker_fingerprint =
          materialization.admission_marker_fingerprint
      JOIN agent_control_worktree_reservation_states worktree
        ON worktree.reservation_id = materialization.worktree_reservation_id
       AND worktree.project_id = materialization.project_id
       AND worktree.task_id = materialization.task_id
       AND worktree.task_revision = materialization.task_revision
       AND worktree.github_intake_sequence = materialization.github_intake_sequence
       AND worktree.source_identity_fingerprint = materialization.source_identity_fingerprint
       AND worktree.revision = materialization.worktree_revision
       AND worktree.last_event_sequence = materialization.worktree_event_sequence
       AND worktree.ownership_fingerprint = materialization.worktree_ownership_fingerprint
       AND worktree.verified_at = materialization.worktree_verified_at
       AND worktree.internal_worktree_path = materialization.worktree_path
       AND worktree.branch_name = materialization.branch
      LEFT JOIN agent_control_events task_event
        ON task_event.event_id = materialization.task_source_event_id
       AND task_event.stream_id = materialization.task_id
       AND task_event.stream_version = materialization.task_source_event_stream_version
       AND task_event.sequence = materialization.task_source_event_sequence
      LEFT JOIN agent_control_task_states task_projection
        ON task_projection.task_id = materialization.task_id
      JOIN agent_control_implementation_deliveries delivery
        ON delivery.handoff_id = intent.handoff_id
      WHERE ${predicate}
      ORDER BY intent.handoff_id LIMIT ?
      `,
      [...parameters, Math.max(1, Math.min(1000, Math.floor(limit)))],
    );

  const claimFromRow = Effect.fn("AgentControlImplementationHandoffStore.claimFromRow")(function* (
    raw: Record<string, unknown>,
  ) {
    const modelSelectionJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.modelSelectionBytes),
      catch: (cause) => candidateEvidenceError("model-selection-bytes", cause),
    });
    const promptText = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.promptBytes),
      catch: (cause) => candidateEvidenceError("prompt-bytes", cause),
    });
    const messageEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.messageTemplateBytes),
      catch: (cause) => candidateEvidenceError("message-template-bytes", cause),
    });
    const turnRequestEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.turnTemplateBytes),
      catch: (cause) => candidateEvidenceError("turn-template-bytes", cause),
    });
    const evidence = yield* decodeEvidence({
      ...raw,
      modelSelectionJson,
      promptText,
      messageEventTemplateJson,
      turnRequestEventTemplateJson,
    }).pipe(Effect.mapError((cause) => candidateEvidenceError("decode-evidence", cause)));
    const modelSelection = yield* decodeModelSelection(evidence.modelSelectionJson).pipe(
      Effect.mapError((cause) => candidateEvidenceError("decode-model-selection", cause)),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelection(modelSelection).pipe(
      Effect.mapError((cause) => candidateEvidenceError("encode-model-selection", cause)),
    );
    yield* Effect.try({
      try: () => {
        parseCanonicalJson(evidence.messageEventTemplateJson);
        parseCanonicalJson(evidence.turnRequestEventTemplateJson);
      },
      catch: (cause) => candidateEvidenceError("event-template-json", cause),
    });
    const evidenceWithModel = { ...evidence, modelSelection };
    const authority = yield* authorityFromRaw(raw);
    const authorityMismatch = yield* Effect.try({
      try: () => implementationHandoffAuthorityMismatch(authority, evidenceWithModel),
      catch: (cause) => candidateEvidenceError("handoff-authority-reconstruction", cause),
    });
    if (
      evidence.modelSelectionJson !== canonicalModelSelectionJson ||
      evidence.providerInstanceId !== modelSelection.instanceId ||
      authorityMismatch !== null
    ) {
      return yield* candidateEvidenceError(
        authorityMismatch === null
          ? "evidence-invariant"
          : `handoff-authority-${authorityMismatch}`,
      );
    }

    const resumeCursor =
      raw.resumeCursorBytes === null
        ? null
        : yield* Effect.try({
            try: () => decodeCanonicalUtf8Bytes(raw.resumeCursorBytes),
            catch: (cause) => candidateEvidenceError("resume-cursor-bytes", cause),
          });
    const delivery = yield* decodeDelivery({
      ...raw,
      admissionMarkerId: raw.deliveryAdmissionMarkerId,
      materializationEvidenceId: raw.deliveryMaterializationEvidenceId,
      stageRunId: raw.deliveryStageRunId,
      attemptId: raw.deliveryAttemptId,
      leaseId: raw.deliveryLeaseId,
      leaseHolderId: raw.deliveryLeaseHolderId,
      fenceToken: raw.deliveryFenceToken,
      runtimeMode: raw.deliveryRuntimeMode,
      modelSelectionFingerprint: raw.deliveryModelSelectionFingerprint,
      planningThreadId: raw.deliveryPlanningThreadId,
      planId: raw.deliveryPlanId,
      providerResumeCursorJson: resumeCursor,
    }).pipe(Effect.mapError((cause) => candidateEvidenceError("decode-delivery", cause)));
    if (
      delivery.handoffId !== evidence.handoffId ||
      delivery.handoffFingerprint !== evidence.handoffFingerprint ||
      delivery.admissionMarkerId !== evidence.admissionMarkerId ||
      delivery.materializationEvidenceId !== evidence.materializationEvidenceId ||
      delivery.threadId !== evidence.threadId ||
      delivery.stageRunId !== evidence.stageRunId ||
      delivery.attemptId !== evidence.attemptId ||
      delivery.leaseId !== evidence.leaseId ||
      delivery.leaseHolderId !== evidence.leaseHolderId ||
      delivery.fenceToken !== evidence.fenceToken ||
      delivery.providerInstanceId !== evidence.providerInstanceId ||
      delivery.runtimeMode !== evidence.runtimeMode ||
      delivery.modelSelectionFingerprint !== evidence.modelSelectionFingerprint ||
      delivery.turnRequestCommandId !== evidence.turnRequestCommandId ||
      delivery.messageId !== evidence.messageId ||
      delivery.planningThreadId !== evidence.planningThreadId ||
      delivery.planId !== evidence.planId ||
      ![0, 1].includes(delivery.interruptRequested ? 1 : 0)
    )
      return yield* candidateEvidenceError("delivery-evidence-invariant");
    return {
      evidence: evidenceWithModel,
      delivery: { ...delivery, interruptRequested: raw.interruptRequested === 1 },
    } satisfies AgentControlImplementationClaim;
  });

  const single = Effect.fn("AgentControlImplementationHandoffStore.single")(function* (
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length === 0) return Option.none<AgentControlImplementationClaim>();
    if (rows.length !== 1) return yield* candidateEvidenceError("non-unique-evidence");
    return Option.some(yield* claimFromRow(rows[0]!));
  });

  const insertAcceptedInTransaction: AgentControlImplementationHandoffStoreShape["insertAcceptedInTransaction"] =
    (evidence, authority) =>
      Effect.gen(function* () {
        const mismatch = yield* Effect.try({
          try: () => implementationHandoffAuthorityMismatch(authority, evidence),
          catch: (cause) => candidateEvidenceError("insert-authority-reconstruction", cause),
        });
        if (mismatch !== null) {
          return yield* candidateEvidenceError(`insert-authority-${mismatch}`);
        }
        yield* sql`
        INSERT INTO agent_control_implementation_handoff_intents (
          handoff_id, handoff_fingerprint, materialization_evidence_id,
          materialization_receipt_id, admission_marker_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          task_source_event_id, task_source_event_sequence, task_source_event_stream_version,
          stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
          worktree_reservation_id, controlled_thread_reservation_id, thread_id,
          worktree_revision, worktree_event_sequence, worktree_ownership_fingerprint,
          worktree_verified_at, worktree_path, branch,
          planning_thread_id, plan_id, proposed_plan_digest, provider_instance_id,
          runtime_mode, model_selection_json, model_selection_fingerprint,
          template_version, prompt_text, prompt_digest, turn_request_command_id,
          message_id, message_event_id, turn_request_event_id,
          message_event_template_json, turn_request_event_template_json,
          event_template_digest, provider_delivery_id, created_at
        ) VALUES (
          ${evidence.handoffId}, ${evidence.handoffFingerprint},
          ${evidence.materializationEvidenceId}, ${evidence.materializationReceiptId},
          ${evidence.admissionMarkerId}, ${evidence.projectId}, ${evidence.taskId},
          ${evidence.taskRevision}, ${evidence.githubIntakeSequence},
          ${evidence.sourceIdentityFingerprint}, ${evidence.taskSourceEventId},
          ${evidence.taskSourceEventSequence}, ${evidence.taskSourceEventStreamVersion},
          ${evidence.stageRunId}, ${evidence.attemptId},
          ${evidence.leaseId}, ${evidence.leaseHolderId}, ${evidence.fenceToken},
          ${evidence.worktreeReservationId}, ${evidence.controlledThreadReservationId},
          ${evidence.threadId}, ${evidence.worktreeRevision},
          ${evidence.worktreeEventSequence}, ${evidence.worktreeOwnershipFingerprint},
          ${evidence.worktreeVerifiedAt}, ${evidence.worktreePath}, ${evidence.branch},
          ${evidence.planningThreadId}, ${evidence.planId},
          ${evidence.proposedPlanDigest}, ${evidence.providerInstanceId}, ${evidence.runtimeMode},
          ${evidence.modelSelectionJson}, ${evidence.modelSelectionFingerprint},
          ${evidence.templateVersion}, ${evidence.promptText}, ${evidence.promptDigest},
          ${evidence.turnRequestCommandId}, ${evidence.messageId}, ${evidence.messageEventId},
          ${evidence.turnRequestEventId}, ${evidence.messageEventTemplateJson},
          ${evidence.turnRequestEventTemplateJson}, ${evidence.eventTemplateDigest},
          ${evidence.providerDeliveryId}, ${evidence.createdAt}
        )
      `;
        yield* sql`
        INSERT INTO agent_control_implementation_handoff_receipts (
          handoff_id, handoff_fingerprint, materialization_evidence_id,
          controlled_thread_reservation_id, thread_id, turn_request_command_id,
          message_id, provider_delivery_id, status, accepted_at
        ) VALUES (
          ${evidence.handoffId}, ${evidence.handoffFingerprint},
          ${evidence.materializationEvidenceId}, ${evidence.controlledThreadReservationId},
          ${evidence.threadId}, ${evidence.turnRequestCommandId}, ${evidence.messageId},
          ${evidence.providerDeliveryId}, 'accepted', ${evidence.createdAt}
        )
      `;
        yield* sql`
        INSERT INTO agent_control_implementation_handoff_accepted (
          handoff_id, handoff_fingerprint, materialization_evidence_id,
          controlled_thread_reservation_id, thread_id, turn_request_command_id,
          message_id, provider_delivery_id, accepted_at
        ) VALUES (
          ${evidence.handoffId}, ${evidence.handoffFingerprint},
          ${evidence.materializationEvidenceId}, ${evidence.controlledThreadReservationId},
          ${evidence.threadId}, ${evidence.turnRequestCommandId}, ${evidence.messageId},
          ${evidence.providerDeliveryId}, ${evidence.createdAt}
        )
      `;
        yield* sql`
        INSERT INTO agent_control_implementation_deliveries (
          provider_delivery_id, handoff_id, handoff_fingerprint, admission_marker_id,
          materialization_evidence_id, controlled_thread_reservation_id, thread_id,
          stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
          provider_instance_id, runtime_mode, model_selection_fingerprint,
          turn_request_command_id, message_id, planning_thread_id, plan_id,
          state, revision, claim_owner_id, claim_generation, claim_expires_at,
          attempt_count, next_attempt_at, provider_turn_id, provider_accepted_at,
          provider_session_created_at, provider_resume_cursor_json, terminal_at,
          last_error_code, interrupt_requested, updated_at
        ) VALUES (
          ${evidence.providerDeliveryId}, ${evidence.handoffId}, ${evidence.handoffFingerprint},
          ${evidence.admissionMarkerId}, ${evidence.materializationEvidenceId},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.stageRunId}, ${evidence.attemptId}, ${evidence.leaseId},
          ${evidence.leaseHolderId}, ${evidence.fenceToken}, ${evidence.providerInstanceId},
          ${evidence.runtimeMode}, ${evidence.modelSelectionFingerprint},
          ${evidence.turnRequestCommandId}, ${evidence.messageId},
          ${evidence.planningThreadId}, ${evidence.planId}, 'pending', 0,
          NULL, 0, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, ${evidence.createdAt}
        )
      `;
      }).pipe(Effect.mapError((cause) => preserveStoreError("insert-accepted", cause)));

  const loadAcceptedByHandoffId: AgentControlImplementationHandoffStoreShape["loadAcceptedByHandoffId"] =
    (handoffId) =>
      selectAccepted("intent.handoff_id = ?", [handoffId], 2).pipe(
        Effect.mapError((cause) => persistenceError("load-by-handoff", cause)),
        Effect.flatMap(single),
      );
  const loadAcceptedByTurnRequestCommandId: AgentControlImplementationHandoffStoreShape["loadAcceptedByTurnRequestCommandId"] =
    (commandId) =>
      selectAccepted("intent.turn_request_command_id = ?", [commandId], 2).pipe(
        Effect.mapError((cause) => persistenceError("load-by-turn-command", cause)),
        Effect.flatMap(single),
      );
  const loadAcceptedByThreadId: AgentControlImplementationHandoffStoreShape["loadAcceptedByThreadId"] =
    (threadId) =>
      selectAccepted("intent.thread_id = ?", [threadId], 2).pipe(
        Effect.mapError((cause) => persistenceError("load-by-thread", cause)),
        Effect.flatMap(single),
      );
  const listRecoverable: AgentControlImplementationHandoffStoreShape["listRecoverable"] = (
    now,
    limit = 100,
  ) =>
    sql
      .unsafe<{ readonly handoffId: string }>(
        `SELECT handoff_id AS "handoffId"
      FROM agent_control_implementation_deliveries delivery
      WHERE delivery.state IN ('pending','turn-accepted','provider-started','interrupt-requested')
        OR (delivery.state = 'retry-wait' AND delivery.next_attempt_at <= ?)
        OR (delivery.state IN ('claimed','delivery-attempted') AND delivery.claim_expires_at <= ?)
      ORDER BY handoff_id LIMIT ?`,
        [now, now, Math.max(1, Math.min(1000, Math.floor(limit)))],
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("list-recoverable", cause)),
        Effect.map((rows) => rows.map((row) => row.handoffId)),
      );
  const isHandoffOwnedTurnRequest: AgentControlImplementationHandoffStoreShape["isHandoffOwnedTurnRequest"] =
    (commandId) =>
      sql<{ readonly count: number }>`SELECT count(*) AS count
      FROM agent_control_implementation_handoff_accepted
      WHERE turn_request_command_id = ${commandId}`.pipe(
        Effect.mapError((cause) => persistenceError("is-owned", cause)),
        Effect.flatMap((rows) =>
          rows[0]?.count === 0 || rows[0]?.count === 1
            ? Effect.succeed(rows[0]?.count === 1)
            : Effect.fail(persistenceError("non-unique-ownership")),
        ),
      );
  const loadTurnAcceptance: AgentControlImplementationHandoffStoreShape["loadTurnAcceptance"] = (
    handoffId,
  ) =>
    sql<Record<string, unknown>>`
      SELECT handoff_id AS "handoffId", handoff_fingerprint AS "handoffFingerprint",
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", planning_thread_id AS "planningThreadId", plan_id AS "planId",
        turn_request_command_id AS "turnRequestCommandId", message_id AS "messageId",
        message_event_id AS "messageEventId", message_event_sequence AS "messageEventSequence",
        turn_request_event_id AS "turnRequestEventId",
        turn_request_event_sequence AS "turnRequestEventSequence",
        message_event_envelope_json AS "messageEventEnvelopeJson",
        turn_request_event_envelope_json AS "turnRequestEventEnvelopeJson",
        event_evidence_digest AS "eventEvidenceDigest", accepted_at AS "acceptedAt"
      FROM agent_control_implementation_turn_accepted WHERE handoff_id = ${handoffId}
    `.pipe(
      Effect.mapError((cause) => persistenceError("load-turn-acceptance", cause)),
      Effect.flatMap((rows) =>
        rows.length === 0
          ? Effect.succeed(Option.none())
          : rows.length !== 1
            ? Effect.fail(candidateEvidenceError("non-unique-turn-acceptance"))
            : decodeAcceptance(rows[0]).pipe(
                Effect.map((row) =>
                  Option.some(row satisfies AgentControlImplementationTurnAcceptance),
                ),
                Effect.mapError((cause) => candidateEvidenceError("decode-turn-acceptance", cause)),
              ),
      ),
    );

  const returning = `provider_delivery_id AS "providerDeliveryId", handoff_id AS "handoffId",
    handoff_fingerprint AS "handoffFingerprint", admission_marker_id AS "admissionMarkerId",
    materialization_evidence_id AS "materializationEvidenceId",
    controlled_thread_reservation_id AS "controlledThreadReservationId", thread_id AS "threadId",
    stage_run_id AS "stageRunId", attempt_id AS "attemptId", lease_id AS "leaseId",
    lease_holder_id AS "leaseHolderId", fence_token AS "fenceToken",
    provider_instance_id AS "providerInstanceId", runtime_mode AS "runtimeMode",
    model_selection_fingerprint AS "modelSelectionFingerprint",
    turn_request_command_id AS "turnRequestCommandId", message_id AS "messageId",
    planning_thread_id AS "planningThreadId", plan_id AS "planId", state, revision,
    claim_owner_id AS "claimOwnerId", claim_generation AS "claimGeneration",
    claim_expires_at AS "claimExpiresAt", attempt_count AS "attemptCount",
    next_attempt_at AS "nextAttemptAt", provider_turn_id AS "providerTurnId",
    provider_accepted_at AS "providerAcceptedAt",
    provider_session_created_at AS "providerSessionCreatedAt",
    provider_resume_cursor_json AS "providerResumeCursorJson", terminal_at AS "terminalAt",
    last_error_code AS "lastErrorCode", interrupt_requested AS "interruptRequested",
    updated_at AS "updatedAt"`;
  const updateOne = Effect.fn("AgentControlImplementationHandoffStore.updateOne")(function* (
    operation: string,
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length !== 1) return yield* revisionConflictError(`${operation}-cas-conflict`);
    const decoded = yield* decodeDelivery(rows[0]).pipe(
      Effect.mapError((cause) => persistenceError(`${operation}-decode`, cause)),
    );
    return { ...decoded, interruptRequested: decoded.interruptRequested === 1 };
  });
  const markTurnAccepted: AgentControlImplementationHandoffStoreShape["markTurnAccepted"] = (
    handoffId,
    expectedRevision,
    at,
  ) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_implementation_deliveries
        SET state='turn-accepted', revision=revision+1, updated_at=?
        WHERE handoff_id=? AND revision=? AND state='pending'
          AND EXISTS (SELECT 1 FROM agent_control_implementation_turn_accepted accepted
            WHERE accepted.handoff_id=agent_control_implementation_deliveries.handoff_id)
        RETURNING ${returning}`,
          [at, handoffId, expectedRevision],
        ),
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("mark-turn-accepted", cause)),
        Effect.flatMap((rows) => updateOne("mark-turn-accepted", rows)),
      );
  const claim: AgentControlImplementationHandoffStoreShape["claim"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql.unsafe<Record<string, unknown>>(
            `UPDATE agent_control_implementation_deliveries
          SET state='claimed', revision=revision+1, claim_owner_id=?, claim_generation=claim_generation+1,
            claim_expires_at=?, attempt_count=attempt_count+1, next_attempt_at=NULL, updated_at=?
          WHERE handoff_id=? AND (state='turn-accepted' OR (state='retry-wait' AND next_attempt_at<=?)
            OR (state='claimed' AND claim_expires_at<=?)) RETURNING handoff_id`,
            [input.ownerId, input.expiresAt, input.now, input.handoffId, input.now, input.now],
          );
          return rows.length === 0
            ? Option.none<AgentControlImplementationClaim>()
            : yield* loadAcceptedByHandoffId(input.handoffId);
        }),
      )
      .pipe(Effect.mapError((cause) => preserveStoreError("claim", cause)));
  const markDeliveryAttempted: AgentControlImplementationHandoffStoreShape["markDeliveryAttempted"] =
    (input) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT OR IGNORE INTO agent_control_implementation_delivery_attestations (
        provider_delivery_id, provider_instance_id, model_selection_json,
        model_selection_fingerprint, recorded_at) VALUES (${input.providerDeliveryId},
        ${input.providerInstanceId}, ${input.turnModelSelectionJson},
        ${input.turnModelSelectionFingerprint}, ${input.attemptedAt})`;
            return yield* sql.unsafe<Record<string, unknown>>(
              `UPDATE agent_control_implementation_deliveries
        SET state='delivery-attempted', revision=revision+1, provider_session_created_at=?,
          provider_resume_cursor_json=?, updated_at=? WHERE handoff_id=? AND revision=?
          AND state='claimed' AND claim_owner_id=? AND claim_generation=? RETURNING ${returning}`,
              [
                input.providerSessionCreatedAt,
                input.providerResumeCursorJson,
                input.attemptedAt,
                input.handoffId,
                input.expectedRevision,
                input.ownerId,
                input.claimGeneration,
              ],
            );
          }),
        )
        .pipe(
          Effect.mapError((cause) => persistenceError("mark-delivery-attempted", cause)),
          Effect.flatMap((rows) => updateOne("mark-delivery-attempted", rows)),
        );
  const markProviderStarted: AgentControlImplementationHandoffStoreShape["markProviderStarted"] = (
    input,
  ) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_implementation_deliveries
        SET state='provider-started', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
          provider_turn_id=?, provider_accepted_at=?, last_error_code=NULL, updated_at=?
        WHERE handoff_id=? AND revision=? AND state='delivery-attempted'
          AND claim_owner_id=? AND claim_generation=? RETURNING ${returning}`,
          [
            input.providerTurnId,
            input.acceptedAt,
            input.acceptedAt,
            input.handoffId,
            input.expectedRevision,
            input.ownerId,
            input.claimGeneration,
          ],
        ),
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("mark-provider-started", cause)),
        Effect.flatMap((rows) => updateOne("mark-provider-started", rows)),
      );
  const scheduleRetry: AgentControlImplementationHandoffStoreShape["scheduleRetry"] = (input) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_implementation_deliveries
      SET state='retry-wait', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
        next_attempt_at=?, last_error_code=?, updated_at=? WHERE handoff_id=? AND revision=?
        AND state IN ('claimed','delivery-attempted') AND claim_owner_id=? AND claim_generation=?
      RETURNING ${returning}`,
          [
            input.nextAttemptAt,
            input.errorCode,
            input.updatedAt,
            input.handoffId,
            input.expectedRevision,
            input.ownerId,
            input.claimGeneration,
          ],
        ),
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("schedule-retry", cause)),
        Effect.flatMap((rows) => updateOne("schedule-retry", rows)),
      );
  const markAmbiguous: AgentControlImplementationHandoffStoreShape["markAmbiguous"] = (input) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_implementation_deliveries
      SET state='ambiguous', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
        next_attempt_at=NULL, terminal_at=?, last_error_code='provider-acceptance-ambiguous', updated_at=?
      WHERE handoff_id=? AND revision=? AND state='delivery-attempted' RETURNING ${returning}`,
          [input.terminalAt, input.terminalAt, input.handoffId, input.expectedRevision],
        ),
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("mark-ambiguous", cause)),
        Effect.flatMap((rows) => updateOne("mark-ambiguous", rows)),
      );
  const observeProviderStarted: AgentControlImplementationHandoffStoreShape["observeProviderStarted"] =
    (input) =>
      sql
        .withTransaction(
          sql.unsafe<Record<string, unknown>>(
            `UPDATE agent_control_implementation_deliveries
      SET state='provider-started', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
        provider_turn_id=?, provider_accepted_at=?, terminal_at=NULL,
        last_error_code=NULL, updated_at=?
      WHERE thread_id=? AND state IN ('delivery-attempted','ambiguous') RETURNING ${returning}`,
            [input.providerTurnId, input.acceptedAt, input.acceptedAt, input.threadId],
          ),
        )
        .pipe(
          Effect.mapError((cause) => persistenceError("observe-provider-started", cause)),
          Effect.flatMap((rows) =>
            rows.length === 0
              ? Effect.succeed(Option.none())
              : updateOne("observe-provider-started", rows).pipe(Effect.map(Option.some)),
          ),
        );
  const observeProviderTerminal: AgentControlImplementationHandoffStoreShape["observeProviderTerminal"] =
    (input) =>
      sql
        .withTransaction(
          sql.unsafe<Record<string, unknown>>(
            `UPDATE agent_control_implementation_deliveries
      SET state=?, revision=revision+1, terminal_at=?, last_error_code=?, updated_at=?
      WHERE thread_id=? AND provider_turn_id=? AND state IN ('provider-started','interrupt-requested','ambiguous')
      RETURNING ${returning}`,
            [
              input.state,
              input.terminalAt,
              input.errorCode ?? null,
              input.terminalAt,
              input.threadId,
              input.providerTurnId,
            ],
          ),
        )
        .pipe(
          Effect.mapError((cause) => persistenceError("observe-provider-terminal", cause)),
          Effect.flatMap((rows) =>
            rows.length === 0
              ? Effect.succeed(Option.none())
              : updateOne("observe-provider-terminal", rows).pipe(Effect.map(Option.some)),
          ),
        );
  const listStageStartCandidates: AgentControlImplementationHandoffStoreShape["listStageStartCandidates"] =
    (limit = 100) =>
      sql<{ readonly handoffId: string }>`
    SELECT handoff_id AS "handoffId" FROM agent_control_implementation_deliveries delivery
    WHERE state IN ('provider-started','interrupt-requested','ambiguous','completed','failed','interrupted')
      AND provider_turn_id IS NOT NULL AND provider_accepted_at IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM agent_control_implementation_stage_started_markers marker
        WHERE marker.provider_delivery_id=delivery.provider_delivery_id)
    ORDER BY handoff_id LIMIT ${Math.max(1, Math.min(1000, Math.floor(limit)))}`.pipe(
        Effect.mapError((cause) => persistenceError("list-stage-start-candidates", cause)),
        Effect.map((rows) => rows.map((row) => row.handoffId)),
      );

  return AgentControlImplementationHandoffStore.of({
    insertAcceptedInTransaction,
    loadAcceptedByHandoffId,
    loadAcceptedByTurnRequestCommandId,
    loadAcceptedByThreadId,
    listRecoverable,
    isHandoffOwnedTurnRequest,
    loadTurnAcceptance,
    markTurnAccepted,
    claim,
    markDeliveryAttempted,
    markProviderStarted,
    scheduleRetry,
    markAmbiguous,
    observeProviderStarted,
    observeProviderTerminal,
    listStageStartCandidates,
  });
});

export const AgentControlImplementationHandoffStoreLive = Layer.effect(
  AgentControlImplementationHandoffStore,
  make,
);
