import {
  AgentControlControlledThreadReservationId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
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
  sha256Utf8,
} from "../../initialPlanning/eventEvidence.ts";
import { fingerprintAgentControlSourceIdentity } from "../../stageRun/identity.ts";
import {
  AgentControlVerificationHistoricalAuthorityError,
  loadAgentControlVerificationTaskAuthorityInTransaction,
  loadAgentControlVerificationWorktreeAuthorityInTransaction,
} from "../historicalAuthority.ts";
import {
  verificationHandoffAuthorityMismatch,
  type AgentControlVerificationHandoffAuthority,
} from "../handoffValidation.ts";
import type { AgentControlVerificationClaim, AgentControlVerificationDelivery } from "../model.ts";
import { canonicalAgentControlVerificationPromptSource } from "../prompt.ts";
import {
  AgentControlVerificationHandoffStore,
  AgentControlVerificationStoreError,
  makeAgentControlVerificationCandidateEvidenceError,
  type AgentControlVerificationCandidateEvidenceReason,
  type AgentControlVerificationHandoffStoreShape,
  type AgentControlVerificationTurnAcceptance,
} from "../Services/AgentControlVerificationHandoffStore.ts";
import { normalizeVerificationTerminalSource } from "../terminalObservation.ts";

const isVerificationHistoricalAuthorityError = Schema.is(
  AgentControlVerificationHistoricalAuthorityError,
);

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
  worktreeEventId: Schema.String,
  worktreeEventSequence: Schema.Int,
  worktreeEventStreamVersion: Schema.Int,
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
  runtimeMode: Schema.Literal("approval-required"),
  modelSelectionJson: Schema.String,
  modelSelectionFingerprint: Schema.String,
  templateVersion: Schema.Literals([
    "agent-control-verification-prompt-v1",
    "agent-control-verification-prompt-v2",
  ]),
  promptContractFingerprint: Schema.NullOr(Schema.String),
  promptText: Schema.String,
  promptDigest: Schema.String,
  resultSchemaVersion: Schema.NullOr(Schema.String),
  resultSchemaFingerprint: Schema.NullOr(Schema.String),
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
  runtimeMode: Schema.Literal("approval-required"),
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
    "completed",
    "failed",
    "interrupted",
    "retry-wait",
    "ambiguous",
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
  terminalEventId: Schema.NullOr(Schema.String),
  terminalEventType: Schema.NullOr(Schema.Literals(["turn.completed", "turn.aborted"])),
  terminalProviderState: Schema.NullOr(
    Schema.Literals(["completed", "failed", "interrupted", "cancelled"]),
  ),
  terminalObservationDigest: Schema.NullOr(Schema.String),
  lastErrorCode: Schema.NullOr(
    Schema.Literals([
      "provider-quota",
      "provider-timeout",
      "session-incompatible",
      "transient-not-accepted",
      "provider-acceptance-ambiguous",
      "provider-turn-failed",
      "provider-turn-aborted",
      "provider-turn-interrupted",
      "provider-turn-cancelled",
    ]),
  ),
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
  worktreeEventId: Schema.String,
  worktreeEventSequence: Schema.Int,
  worktreeEventStreamVersion: Schema.Int,
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
  runtimeMode: Schema.Literal("approval-required"),
  modelSelectionJson: Schema.String,
  modelSelectionFingerprint: Schema.String,
  implementationHandoffJson: Schema.String,
  implementationHandoffDigest: Schema.String,
  implementationProviderDeliveryJson: Schema.String,
  implementationProviderDeliveryDigest: Schema.String,
  implementationResultJson: Schema.String,
  implementationResultDigest: Schema.String,
  verificationAdmissionJson: Schema.String,
  verificationAdmissionDigest: Schema.String,
  verificationIdentityJson: Schema.String,
  verificationIdentityDigest: Schema.String,
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

const decodeEvidence = Schema.decodeUnknownEffect(EvidenceRow);
const decodeDelivery = Schema.decodeUnknownEffect(DeliveryRow);
const decodeAuthority = Schema.decodeUnknownEffect(AuthorityRow);
const decodeAcceptance = Schema.decodeUnknownEffect(TurnAcceptanceRow);
const decodeModelSelection = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeModelSelection = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

const storeError = (
  operation: string,
  reason: AgentControlVerificationStoreError["reason"],
  cause?: unknown,
) =>
  new AgentControlVerificationStoreError({
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const candidateEvidenceError = (
  operation: string,
  cause?: unknown,
  handoffId?: string,
  candidateReason: AgentControlVerificationCandidateEvidenceReason = "evidence-divergent",
) =>
  handoffId === undefined
    ? storeError(operation, "candidate-evidence", cause)
    : makeAgentControlVerificationCandidateEvidenceError({
        handoffId,
        candidateReason,
        operation,
        ...(cause === undefined ? {} : { cause }),
      });
const persistenceError = (operation: string, cause?: unknown) =>
  storeError(operation, "persistence", cause);
const revisionConflictError = (operation: string, cause?: unknown) =>
  storeError(operation, "revision-conflict", cause);
const terminalConflictError = (operation: string, cause?: unknown) =>
  storeError(operation, "terminal-conflict", cause);
const isStoreError = Schema.is(AgentControlVerificationStoreError);
const preserveStoreError = (operation: string, cause: unknown) =>
  isStoreError(cause) ? cause : persistenceError(operation, cause);

const authorityFromRaw = Effect.fn("AgentControlVerificationHandoffStore.authorityFromRaw")(
  function* (
    raw: Record<string, unknown>,
    taskAuthority: Effect.Success<
      ReturnType<typeof loadAgentControlVerificationTaskAuthorityInTransaction>
    >,
    worktreeAuthority: Effect.Success<
      ReturnType<typeof loadAgentControlVerificationWorktreeAuthorityInTransaction>
    >,
    handoffId: string,
  ) {
    const decodeBytes = (value: unknown, operation: string) =>
      Effect.try({
        try: () => decodeCanonicalUtf8Bytes(value),
        catch: (cause) =>
          candidateEvidenceError(operation, cause, handoffId, "evidence-undecodable"),
      });
    const proposedPlanJson = yield* decodeBytes(
      raw.authorityProposedPlanBytes,
      "authority-proposed-plan-bytes",
    );
    const repositoryDisplay = worktreeAuthority.state.repository.nameWithOwner;
    const sourceRevision = worktreeAuthority.state.baseCommitSha;
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
    const modelSelectionJson = yield* decodeBytes(
      raw.authorityModelSelectionBytes,
      "authority-model-selection-bytes",
    );
    const implementationHandoffJson = yield* decodeBytes(
      raw.authorityImplementationHandoffBytes,
      "authority-implementation-handoff-bytes",
    );
    const implementationProviderDeliveryJson = yield* decodeBytes(
      raw.authorityImplementationProviderDeliveryBytes,
      "authority-implementation-provider-delivery-bytes",
    );
    const implementationResultJson = yield* decodeBytes(
      raw.authorityImplementationResultBytes,
      "authority-implementation-result-bytes",
    );
    const verificationAdmissionJson = yield* decodeBytes(
      raw.authorityVerificationAdmissionBytes,
      "authority-verification-admission-bytes",
    );
    const verificationIdentityJson = yield* decodeBytes(
      raw.authorityVerificationIdentityBytes,
      "authority-verification-identity-bytes",
    );
    for (const [
      label,
      authorityJsonField,
      authorityDigestField,
      admissionJsonField,
      admissionDigestField,
    ] of [
      [
        "task",
        "authorityTaskHistoryBytes",
        "authorityTaskHistoryDigestBytes",
        "admissionTaskHistoryBytes",
        "admissionTaskHistoryDigestBytes",
      ],
      [
        "worktree",
        "authorityWorktreeHistoryBytes",
        "authorityWorktreeHistoryDigestBytes",
        "admissionWorktreeHistoryBytes",
        "admissionWorktreeHistoryDigestBytes",
      ],
      [
        "stage",
        "authorityStageHistoryBytes",
        "authorityStageHistoryDigestBytes",
        "admissionStageHistoryBytes",
        "admissionStageHistoryDigestBytes",
      ],
      [
        "lease",
        "authorityLeaseHistoryBytes",
        "authorityLeaseHistoryDigestBytes",
        "admissionLeaseHistoryBytes",
        "admissionLeaseHistoryDigestBytes",
      ],
      [
        "reservation",
        "authorityReservationHistoryBytes",
        "authorityReservationHistoryDigestBytes",
        "admissionReservationHistoryBytes",
        "admissionReservationHistoryDigestBytes",
      ],
      [
        "orchestration",
        "authorityOrchestrationHistoryBytes",
        "authorityOrchestrationHistoryDigestBytes",
        "admissionOrchestrationHistoryBytes",
        "admissionOrchestrationHistoryDigestBytes",
      ],
    ] as const) {
      const authorityJson = yield* decodeBytes(
        raw[authorityJsonField],
        `authority-${label}-history-bytes`,
      );
      const authorityDigest = yield* decodeBytes(
        raw[authorityDigestField],
        `authority-${label}-history-digest-bytes`,
      );
      const admissionJson = yield* decodeBytes(
        raw[admissionJsonField],
        `admission-${label}-history-bytes`,
      );
      const admissionDigest = yield* decodeBytes(
        raw[admissionDigestField],
        `admission-${label}-history-digest-bytes`,
      );
      yield* Effect.try({
        try: () => {
          parseCanonicalJson(authorityJson);
          parseCanonicalJson(admissionJson);
          if (
            authorityJson !== admissionJson ||
            authorityDigest !== admissionDigest ||
            sha256Utf8(authorityJson) !== authorityDigest
          ) {
            throw new Error(`${label} history prefix diverged`);
          }
        },
        catch: (cause) =>
          candidateEvidenceError(
            `authority-${label}-history`,
            cause,
            handoffId,
            "history-divergent",
          ),
      });
    }
    yield* Effect.try({
      try: () => {
        parseCanonicalJson(proposedPlanJson);
      },
      catch: (cause) =>
        candidateEvidenceError(
          "authority-proposed-plan-json",
          cause,
          handoffId,
          "evidence-undecodable",
        ),
    });
    const task = taskAuthority.state;
    const promptSource = canonicalAgentControlVerificationPromptSource({
      repositoryDisplay,
      sourceRevision,
      taskTitle: task.sourceSnapshot.title,
      taskBody: task.sourceSnapshot.body,
    });
    if (
      taskAuthority.event.eventId !== raw.authorityTaskSourceEventId ||
      taskAuthority.event.sequence !== raw.authorityTaskSourceEventSequence ||
      taskAuthority.event.streamVersion !== raw.authorityTaskSourceEventStreamVersion ||
      worktreeAuthority.event.eventId !== raw.authorityWorktreeEventId ||
      worktreeAuthority.event.sequence !== raw.authorityWorktreeEventSequence ||
      worktreeAuthority.event.streamVersion !== raw.authorityWorktreeEventStreamVersion ||
      worktreeAuthority.event.type !== "agentControl.worktree.ready" ||
      task.taskId !== raw.authorityTaskId ||
      task.source.projectId !== raw.authorityProjectId ||
      task.githubIntakeSequence !== raw.authorityGithubIntakeSequence ||
      raw.authorityTaskSourceEventStreamVersion !== raw.authorityTaskRevision ||
      raw.authorityWorktreeEventStreamVersion !== raw.authorityWorktreeRevision ||
      task.source.repositoryNodeId !== worktreeAuthority.state.repository.repositoryNodeId ||
      fingerprintAgentControlSourceIdentity(task.source) !==
        raw.authoritySourceIdentityFingerprint ||
      worktreeAuthority.state.status !== "ready" ||
      worktreeAuthority.state.reservationId !== raw.authorityWorktreeReservationId ||
      worktreeAuthority.state.projectId !== raw.authorityProjectId ||
      worktreeAuthority.state.taskId !== raw.authorityTaskId ||
      worktreeAuthority.state.taskRevision !== raw.authorityTaskRevision ||
      worktreeAuthority.state.githubIntakeSequence !== raw.authorityGithubIntakeSequence ||
      worktreeAuthority.state.sourceIdentityFingerprint !==
        raw.authoritySourceIdentityFingerprint ||
      worktreeAuthority.state.revision !== raw.authorityWorktreeRevision ||
      worktreeAuthority.state.sequence !== raw.authorityWorktreeEventSequence ||
      worktreeAuthority.state.ownershipFingerprint !== raw.authorityWorktreeOwnershipFingerprint ||
      worktreeAuthority.state.verifiedAt !== raw.authorityWorktreeVerifiedAt ||
      worktreeAuthority.state.internalWorktreePath !== raw.authorityWorktreePath ||
      worktreeAuthority.state.branchName !== raw.authorityBranch ||
      materializedRepositoryDisplay !== promptSource.repositoryDisplay ||
      materializedSourceRevision !== promptSource.sourceRevision ||
      materializedTaskTitle !== promptSource.taskTitle ||
      materializedTaskBody !== promptSource.taskBody
    ) {
      return yield* candidateEvidenceError(
        "historical-authority-invariant",
        undefined,
        handoffId,
        "history-divergent",
      );
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
      worktreeEventId: raw.authorityWorktreeEventId,
      worktreeEventSequence: raw.authorityWorktreeEventSequence,
      worktreeEventStreamVersion: raw.authorityWorktreeEventStreamVersion,
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
      taskTitle: task.sourceSnapshot.title,
      taskBody: task.sourceSnapshot.body,
      providerInstanceId: raw.authorityProviderInstanceId,
      runtimeMode: raw.authorityRuntimeMode,
      modelSelectionJson,
      modelSelectionFingerprint: raw.authorityModelSelectionFingerprint,
      implementationHandoffJson,
      implementationHandoffDigest: raw.authorityImplementationHandoffDigest,
      implementationProviderDeliveryJson,
      implementationProviderDeliveryDigest: raw.authorityImplementationProviderDeliveryDigest,
      implementationResultJson,
      implementationResultDigest: raw.authorityImplementationResultDigest,
      verificationAdmissionJson,
      verificationAdmissionDigest: raw.authorityVerificationAdmissionDigest,
      verificationIdentityJson,
      verificationIdentityDigest: raw.authorityVerificationIdentityDigest,
      createdAt: raw.authorityCreatedAt,
    }).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError("decode-authority", cause, handoffId, "evidence-undecodable"),
      ),
    );
    const modelSelection = yield* decodeModelSelection(authority.modelSelectionJson).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError(
          "decode-authority-model-selection",
          cause,
          handoffId,
          "evidence-undecodable",
        ),
      ),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelection(modelSelection).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError(
          "encode-authority-model-selection",
          cause,
          handoffId,
          "evidence-undecodable",
        ),
      ),
    );
    if (
      canonicalModelSelectionJson !== authority.modelSelectionJson ||
      modelSelection.instanceId !== authority.providerInstanceId
    ) {
      return yield* candidateEvidenceError(
        "authority-model-selection-invariant",
        undefined,
        handoffId,
      );
    }
    return { ...authority, modelSelection } satisfies AgentControlVerificationHandoffAuthority;
  },
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const handoffIntentColumns = yield* sql<{ readonly name: string }>`
    PRAGMA table_info(agent_control_verification_handoff_intents)
  `;
  const supportsResultContract = handoffIntentColumns.some(
    (column) => column.name === "result_schema_fingerprint",
  );
  const promptAuthorityProjection = supportsResultContract
    ? `COALESCE(intent.prompt_template_version, intent.template_version) AS "templateVersion",
        intent.prompt_contract_fingerprint AS "promptContractFingerprint",
        CAST(intent.prompt_text AS BLOB) AS "promptBytes",
        intent.prompt_digest AS "promptDigest",
        intent.result_schema_version AS "resultSchemaVersion",
        intent.result_schema_fingerprint AS "resultSchemaFingerprint"`
    : `intent.template_version AS "templateVersion",
        NULL AS "promptContractFingerprint",
        CAST(intent.prompt_text AS BLOB) AS "promptBytes",
        intent.prompt_digest AS "promptDigest",
        NULL AS "resultSchemaVersion",
        NULL AS "resultSchemaFingerprint"`;

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
        intent.worktree_event_id AS "worktreeEventId",
        intent.worktree_event_sequence AS "worktreeEventSequence",
        intent.worktree_event_stream_version AS "worktreeEventStreamVersion",
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
        ${promptAuthorityProjection},
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
        materialization.worktree_event_id AS "authorityWorktreeEventId",
        materialization.worktree_event_sequence AS "authorityWorktreeEventSequence",
        materialization.worktree_event_stream_version AS
          "authorityWorktreeEventStreamVersion",
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
        CAST(materialization.repository_display AS BLOB) AS "materializedRepositoryBytes",
        CAST(materialization.source_revision AS BLOB) AS "materializedSourceRevisionBytes",
        CAST(materialization.task_title AS BLOB) AS "materializedTaskTitleBytes",
        CAST(materialization.task_body AS BLOB) AS "materializedTaskBodyBytes",
        materialization.provider_instance_id AS "authorityProviderInstanceId",
        materialization.runtime_mode AS "authorityRuntimeMode",
        CAST(materialization.model_selection_json AS BLOB) AS "authorityModelSelectionBytes",
        materialization.model_selection_fingerprint AS "authorityModelSelectionFingerprint",
        CAST(materialization.implementation_handoff_json AS BLOB)
          AS "authorityImplementationHandoffBytes",
        materialization.implementation_handoff_digest AS "authorityImplementationHandoffDigest",
        CAST(materialization.implementation_provider_delivery_json AS BLOB)
          AS "authorityImplementationProviderDeliveryBytes",
        materialization.implementation_provider_delivery_digest
          AS "authorityImplementationProviderDeliveryDigest",
        CAST(materialization.implementation_result_json AS BLOB)
          AS "authorityImplementationResultBytes",
        materialization.implementation_result_digest AS "authorityImplementationResultDigest",
        CAST(materialization.verification_admission_json AS BLOB)
          AS "authorityVerificationAdmissionBytes",
        materialization.verification_admission_digest AS "authorityVerificationAdmissionDigest",
        CAST(materialization.verification_identity_json AS BLOB)
          AS "authorityVerificationIdentityBytes",
        materialization.verification_identity_digest AS "authorityVerificationIdentityDigest",
        CAST(materialization.task_history_json AS BLOB) AS "authorityTaskHistoryBytes",
        CAST(materialization.task_history_digest AS BLOB)
          AS "authorityTaskHistoryDigestBytes",
        CAST(materialization.worktree_history_json AS BLOB)
          AS "authorityWorktreeHistoryBytes",
        CAST(materialization.worktree_history_digest AS BLOB)
          AS "authorityWorktreeHistoryDigestBytes",
        CAST(materialization.stage_history_json AS BLOB) AS "authorityStageHistoryBytes",
        CAST(materialization.stage_history_digest AS BLOB)
          AS "authorityStageHistoryDigestBytes",
        CAST(materialization.lease_history_json AS BLOB) AS "authorityLeaseHistoryBytes",
        CAST(materialization.lease_history_digest AS BLOB)
          AS "authorityLeaseHistoryDigestBytes",
        CAST(materialization.reservation_history_json AS BLOB)
          AS "authorityReservationHistoryBytes",
        CAST(materialization.reservation_history_digest AS BLOB)
          AS "authorityReservationHistoryDigestBytes",
        CAST(materialization.orchestration_history_json AS BLOB)
          AS "authorityOrchestrationHistoryBytes",
        CAST(materialization.orchestration_history_digest AS BLOB)
          AS "authorityOrchestrationHistoryDigestBytes",
        CAST(admission.task_history_json AS BLOB) AS "admissionTaskHistoryBytes",
        CAST(admission.task_history_digest AS BLOB) AS "admissionTaskHistoryDigestBytes",
        CAST(admission.worktree_history_json AS BLOB) AS "admissionWorktreeHistoryBytes",
        CAST(admission.worktree_history_digest AS BLOB)
          AS "admissionWorktreeHistoryDigestBytes",
        CAST(admission.stage_history_json AS BLOB) AS "admissionStageHistoryBytes",
        CAST(admission.stage_history_digest AS BLOB) AS "admissionStageHistoryDigestBytes",
        CAST(admission.lease_history_json AS BLOB) AS "admissionLeaseHistoryBytes",
        CAST(admission.lease_history_digest AS BLOB) AS "admissionLeaseHistoryDigestBytes",
        CAST(admission.reservation_history_json AS BLOB)
          AS "admissionReservationHistoryBytes",
        CAST(admission.reservation_history_digest AS BLOB)
          AS "admissionReservationHistoryDigestBytes",
        CAST(admission.orchestration_history_json AS BLOB)
          AS "admissionOrchestrationHistoryBytes",
        CAST(admission.orchestration_history_digest AS BLOB)
          AS "admissionOrchestrationHistoryDigestBytes",
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
        CASE WHEN delivery.terminal_at IS NULL THEN NULL
          ELSE CAST(delivery.terminal_at AS BLOB) END AS "terminalAtBytes",
        CASE WHEN delivery.terminal_event_id IS NULL THEN NULL
          ELSE CAST(delivery.terminal_event_id AS BLOB) END AS "terminalEventIdBytes",
        CASE WHEN delivery.terminal_event_type IS NULL THEN NULL
          ELSE CAST(delivery.terminal_event_type AS BLOB) END AS "terminalEventTypeBytes",
        CASE WHEN delivery.terminal_provider_state IS NULL THEN NULL
          ELSE CAST(delivery.terminal_provider_state AS BLOB) END AS "terminalProviderStateBytes",
        CASE WHEN delivery.terminal_observation_digest IS NULL THEN NULL
          ELSE CAST(delivery.terminal_observation_digest AS BLOB)
          END AS "terminalObservationDigestBytes",
        CASE WHEN delivery.last_error_code IS NULL THEN NULL
          ELSE CAST(delivery.last_error_code AS BLOB) END AS "lastErrorCodeBytes",
        delivery.interrupt_requested AS "interruptRequested", delivery.updated_at AS "updatedAt",
        receipt.handoff_id AS "receiptPresent", accepted.handoff_id AS "acceptancePresent",
        materialization.materialization_evidence_id AS "materializationPresent",
        materialization_receipt.materialization_receipt_id AS "materializationReceiptPresent",
        marker.materialization_marker_id AS "markerPresent",
        admission.admission_evidence_id AS "admissionPresent",
        admission_receipt.receipt_id AS "admissionReceiptPresent",
        admission_marker.marker_id AS "admissionMarkerPresent",
        delivery.handoff_id AS "deliveryPresent"
      FROM agent_control_verification_handoff_intents intent
      LEFT JOIN agent_control_verification_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
       AND receipt.handoff_fingerprint = intent.handoff_fingerprint
       AND receipt.materialization_evidence_id = intent.materialization_evidence_id
       AND receipt.controlled_thread_reservation_id = intent.controlled_thread_reservation_id
       AND receipt.thread_id = intent.thread_id
       AND receipt.turn_request_command_id = intent.turn_request_command_id
       AND receipt.message_id = intent.message_id
       AND receipt.provider_delivery_id = intent.provider_delivery_id
       AND receipt.status = 'accepted'
      LEFT JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = receipt.handoff_id
       AND accepted.handoff_fingerprint = receipt.handoff_fingerprint
       AND accepted.materialization_evidence_id = receipt.materialization_evidence_id
       AND accepted.controlled_thread_reservation_id = receipt.controlled_thread_reservation_id
       AND accepted.thread_id = receipt.thread_id
       AND accepted.turn_request_command_id = receipt.turn_request_command_id
       AND accepted.message_id = receipt.message_id
       AND accepted.provider_delivery_id = receipt.provider_delivery_id
      LEFT JOIN agent_control_verification_materialization_evidence materialization
        ON materialization.materialization_evidence_id = intent.materialization_evidence_id
      LEFT JOIN agent_control_verification_materialization_receipts materialization_receipt
        ON materialization_receipt.materialization_evidence_id =
          materialization.materialization_evidence_id
       AND materialization_receipt.materialization_receipt_id =
          intent.materialization_receipt_id
       AND materialization_receipt.materialization_fingerprint =
          materialization.materialization_fingerprint
       AND materialization_receipt.status = 'accepted'
      LEFT JOIN agent_control_verification_materialization_markers marker
        ON marker.materialization_evidence_id = intent.materialization_evidence_id
       AND marker.materialization_receipt_id =
          materialization_receipt.materialization_receipt_id
       AND marker.materialization_fingerprint = materialization.materialization_fingerprint
       AND marker.handoff_id = intent.handoff_id
       AND marker.provider_delivery_id = intent.provider_delivery_id
      LEFT JOIN agent_control_verification_admission_evidence admission
        ON admission.admission_evidence_id = materialization.admission_evidence_id
       AND admission.admission_fingerprint = materialization.admission_fingerprint
       AND admission.implementation_result_evidence_id =
          materialization.implementation_result_evidence_id
       AND admission.project_id = materialization.project_id
       AND admission.task_id = materialization.task_id
       AND admission.task_revision = materialization.task_revision
       AND admission.github_intake_sequence = materialization.github_intake_sequence
       AND admission.source_identity_fingerprint =
          materialization.source_identity_fingerprint
       AND admission.verification_stage_run_id = materialization.stage_run_id
       AND admission.verification_attempt_id = materialization.attempt_id
       AND admission.lease_id = materialization.lease_id
       AND admission.lease_holder_id = materialization.lease_holder_id
       AND admission.verification_fence_token = materialization.fence_token
       AND admission.worktree_reservation_id = materialization.worktree_reservation_id
       AND admission.worktree_event_id = materialization.worktree_event_id
       AND admission.worktree_event_sequence = materialization.worktree_event_sequence
       AND admission.worktree_ownership_fingerprint =
          materialization.worktree_ownership_fingerprint
       AND admission.verification_controlled_thread_reservation_id =
          materialization.controlled_thread_reservation_id
       AND admission.verification_thread_id = materialization.thread_id
      LEFT JOIN agent_control_verification_admission_receipts admission_receipt
        ON admission_receipt.admission_evidence_id = admission.admission_evidence_id
       AND admission_receipt.receipt_id = materialization.admission_receipt_id
       AND admission_receipt.admission_command_id = admission.admission_command_id
       AND admission_receipt.admission_fingerprint = admission.admission_fingerprint
      LEFT JOIN agent_control_verification_admission_markers admission_marker
        ON admission_marker.admission_evidence_id = admission.admission_evidence_id
       AND admission_marker.receipt_id = admission_receipt.receipt_id
       AND admission_marker.marker_id = materialization.admission_marker_id
       AND admission_marker.admission_command_id = admission.admission_command_id
       AND admission_marker.marker_fingerprint =
          materialization.admission_marker_fingerprint
      LEFT JOIN agent_control_verification_deliveries delivery
        ON delivery.handoff_id = intent.handoff_id
      WHERE ${predicate}
      ORDER BY intent.handoff_id LIMIT ?
      `,
      [...parameters, Math.max(1, Math.min(1000, Math.floor(limit)))],
    );

  const claimFromRow = Effect.fn("AgentControlVerificationHandoffStore.claimFromRow")(function* (
    raw: Record<string, unknown>,
  ) {
    const handoffId = typeof raw.handoffId === "string" ? raw.handoffId : "unknown-handoff";
    for (const [field, label] of [
      ["receiptPresent", "handoff-receipt"],
      ["acceptancePresent", "handoff-acceptance"],
      ["materializationPresent", "materialization-evidence"],
      ["materializationReceiptPresent", "materialization-receipt"],
      ["markerPresent", "materialization-marker"],
      ["admissionPresent", "admission-evidence"],
      ["admissionReceiptPresent", "admission-receipt"],
      ["admissionMarkerPresent", "admission-marker"],
      ["deliveryPresent", "delivery"],
    ] as const) {
      if (raw[field] === null || raw[field] === undefined) {
        return yield* candidateEvidenceError(
          `missing-${label}`,
          undefined,
          handoffId,
          "companion-missing",
        );
      }
    }
    if (
      typeof raw.authorityTaskId !== "string" ||
      typeof raw.authorityTaskRevision !== "number" ||
      typeof raw.authorityWorktreeReservationId !== "string" ||
      typeof raw.worktreeEventId !== "string" ||
      typeof raw.worktreeEventSequence !== "number" ||
      typeof raw.worktreeEventStreamVersion !== "number"
    ) {
      return yield* candidateEvidenceError(
        "missing-authority-root",
        undefined,
        handoffId,
        "companion-missing",
      );
    }
    const mapHistoricalError = (operation: string, cause: unknown) => {
      if (!isVerificationHistoricalAuthorityError(cause)) {
        return persistenceError(operation, cause);
      }
      const candidateReason: AgentControlVerificationCandidateEvidenceReason =
        cause.reason === "projection-missing"
          ? "projection-missing"
          : cause.reason === "projection-divergent"
            ? "projection-divergent"
            : cause.reason === "history-missing"
              ? "history-missing"
              : cause.reason === "history-divergent"
                ? "history-divergent"
                : "evidence-undecodable";
      return candidateEvidenceError(operation, cause, handoffId, candidateReason);
    };
    const taskAuthority = yield* loadAgentControlVerificationTaskAuthorityInTransaction(
      sql,
      AgentControlTaskId.make(raw.authorityTaskId),
      raw.authorityTaskRevision,
    ).pipe(Effect.mapError((cause) => mapHistoricalError("task-authority", cause)));
    const worktreeAuthority = yield* loadAgentControlVerificationWorktreeAuthorityInTransaction(
      sql,
      AgentControlWorktreeReservationId.make(raw.authorityWorktreeReservationId),
      {
        eventId: EventId.make(raw.worktreeEventId),
        sequence: raw.worktreeEventSequence,
        streamVersion: raw.worktreeEventStreamVersion,
      },
    ).pipe(Effect.mapError((cause) => mapHistoricalError("worktree-authority", cause)));
    const modelSelectionJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.modelSelectionBytes),
      catch: (cause) =>
        candidateEvidenceError("model-selection-bytes", cause, handoffId, "evidence-undecodable"),
    });
    const promptText = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.promptBytes),
      catch: (cause) =>
        candidateEvidenceError("prompt-bytes", cause, handoffId, "evidence-undecodable"),
    });
    const messageEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.messageTemplateBytes),
      catch: (cause) =>
        candidateEvidenceError("message-template-bytes", cause, handoffId, "evidence-undecodable"),
    });
    const turnRequestEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.turnTemplateBytes),
      catch: (cause) =>
        candidateEvidenceError("turn-template-bytes", cause, handoffId, "evidence-undecodable"),
    });
    const evidence = yield* decodeEvidence({
      ...raw,
      modelSelectionJson,
      promptText,
      messageEventTemplateJson,
      turnRequestEventTemplateJson,
    }).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError("decode-evidence", cause, handoffId, "evidence-undecodable"),
      ),
    );
    const modelSelection = yield* decodeModelSelection(evidence.modelSelectionJson).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError("decode-model-selection", cause, handoffId, "evidence-undecodable"),
      ),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelection(modelSelection).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError("encode-model-selection", cause, handoffId, "evidence-undecodable"),
      ),
    );
    yield* Effect.try({
      try: () => {
        parseCanonicalJson(evidence.messageEventTemplateJson);
        parseCanonicalJson(evidence.turnRequestEventTemplateJson);
      },
      catch: (cause) =>
        candidateEvidenceError("event-template-json", cause, handoffId, "evidence-undecodable"),
    });
    const evidenceWithModel = { ...evidence, modelSelection };
    const authority = yield* authorityFromRaw(raw, taskAuthority, worktreeAuthority, handoffId);
    const authorityMismatch = yield* Effect.try({
      try: () => verificationHandoffAuthorityMismatch(authority, evidenceWithModel),
      catch: (cause) =>
        candidateEvidenceError(
          "handoff-authority-reconstruction",
          cause,
          handoffId,
          "evidence-divergent",
        ),
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
        undefined,
        handoffId,
        "evidence-divergent",
      );
    }

    const resumeCursor =
      raw.resumeCursorBytes === null
        ? null
        : yield* Effect.try({
            try: () => decodeCanonicalUtf8Bytes(raw.resumeCursorBytes),
            catch: (cause) =>
              candidateEvidenceError(
                "resume-cursor-bytes",
                cause,
                handoffId,
                "evidence-undecodable",
              ),
          });
    const decodeTerminalText = (value: unknown, operation: string) =>
      value === null
        ? Effect.succeed(null)
        : Effect.try({
            try: () => decodeCanonicalUtf8Bytes(value),
            catch: (cause) =>
              candidateEvidenceError(operation, cause, handoffId, "evidence-undecodable"),
          });
    const [
      terminalAt,
      terminalEventId,
      terminalEventType,
      terminalProviderState,
      terminalObservationDigest,
      lastErrorCode,
    ] = yield* Effect.all([
      decodeTerminalText(raw.terminalAtBytes, "terminal-at-bytes"),
      decodeTerminalText(raw.terminalEventIdBytes, "terminal-event-id-bytes"),
      decodeTerminalText(raw.terminalEventTypeBytes, "terminal-event-type-bytes"),
      decodeTerminalText(raw.terminalProviderStateBytes, "terminal-provider-state-bytes"),
      decodeTerminalText(raw.terminalObservationDigestBytes, "terminal-digest-bytes"),
      decodeTerminalText(raw.lastErrorCodeBytes, "last-error-code-bytes"),
    ]);
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
      terminalAt,
      terminalEventId,
      terminalEventType,
      terminalProviderState,
      terminalObservationDigest,
      lastErrorCode,
    }).pipe(
      Effect.mapError((cause) =>
        candidateEvidenceError("decode-delivery", cause, handoffId, "evidence-undecodable"),
      ),
    );
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
      return yield* candidateEvidenceError(
        "delivery-evidence-invariant",
        undefined,
        handoffId,
        "evidence-divergent",
      );
    return {
      evidence: evidenceWithModel,
      delivery: { ...delivery, interruptRequested: raw.interruptRequested === 1 },
    } satisfies AgentControlVerificationClaim;
  });

  const single = Effect.fn("AgentControlVerificationHandoffStore.single")(function* (
    rows: ReadonlyArray<Record<string, unknown>>,
    requestedHandoffId?: string,
  ) {
    if (rows.length === 0) return Option.none<AgentControlVerificationClaim>();
    if (rows.length !== 1)
      return yield* candidateEvidenceError(
        "non-unique-evidence",
        undefined,
        requestedHandoffId ?? "ambiguous-handoff",
        "companion-ambiguous",
      );
    return Option.some(yield* claimFromRow(rows[0]!));
  });

  const insertAcceptedInTransaction: AgentControlVerificationHandoffStoreShape["insertAcceptedInTransaction"] =
    (evidence, authority) =>
      Effect.gen(function* () {
        const mismatch = yield* Effect.try({
          try: () => verificationHandoffAuthorityMismatch(authority, evidence),
          catch: (cause) => candidateEvidenceError("insert-authority-reconstruction", cause),
        });
        if (mismatch !== null) {
          return yield* candidateEvidenceError(`insert-authority-${mismatch}`);
        }
        if (!supportsResultContract) {
          if (
            evidence.templateVersion !== "agent-control-verification-prompt-v1" ||
            evidence.promptContractFingerprint !== null ||
            evidence.resultSchemaVersion !== null ||
            evidence.resultSchemaFingerprint !== null
          ) {
            return yield* candidateEvidenceError("insert-v2-before-migration-060");
          }
          yield* sql`
            INSERT INTO agent_control_verification_handoff_intents (
              handoff_id, handoff_fingerprint, materialization_evidence_id,
              materialization_receipt_id, admission_marker_id, project_id, task_id,
              task_revision, github_intake_sequence, source_identity_fingerprint,
              task_source_event_id, task_source_event_sequence, task_source_event_stream_version,
              stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
              worktree_reservation_id, controlled_thread_reservation_id, thread_id,
              worktree_revision, worktree_event_id, worktree_event_sequence,
              worktree_event_stream_version, worktree_ownership_fingerprint,
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
              ${evidence.worktreeEventId}, ${evidence.worktreeEventSequence},
              ${evidence.worktreeEventStreamVersion}, ${evidence.worktreeOwnershipFingerprint},
              ${evidence.worktreeVerifiedAt}, ${evidence.worktreePath}, ${evidence.branch},
              ${evidence.planningThreadId}, ${evidence.planId},
              ${evidence.proposedPlanDigest}, ${evidence.providerInstanceId},
              ${evidence.runtimeMode}, ${evidence.modelSelectionJson},
              ${evidence.modelSelectionFingerprint}, ${evidence.templateVersion},
              ${evidence.promptText}, ${evidence.promptDigest},
              ${evidence.turnRequestCommandId}, ${evidence.messageId},
              ${evidence.messageEventId}, ${evidence.turnRequestEventId},
              ${evidence.messageEventTemplateJson}, ${evidence.turnRequestEventTemplateJson},
              ${evidence.eventTemplateDigest}, ${evidence.providerDeliveryId},
              ${evidence.createdAt}
            )
          `;
        } else {
          yield* sql`
        INSERT INTO agent_control_verification_handoff_intents (
          handoff_id, handoff_fingerprint, materialization_evidence_id,
          materialization_receipt_id, admission_marker_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
          task_source_event_id, task_source_event_sequence, task_source_event_stream_version,
          stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
          worktree_reservation_id, controlled_thread_reservation_id, thread_id,
          worktree_revision, worktree_event_id, worktree_event_sequence,
          worktree_event_stream_version, worktree_ownership_fingerprint,
          worktree_verified_at, worktree_path, branch,
          planning_thread_id, plan_id, proposed_plan_digest, provider_instance_id,
          runtime_mode, model_selection_json, model_selection_fingerprint,
          template_version, prompt_template_version, prompt_contract_fingerprint,
          prompt_text, prompt_digest,
          result_schema_version, result_schema_fingerprint, turn_request_command_id,
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
          ${evidence.worktreeEventId}, ${evidence.worktreeEventSequence},
          ${evidence.worktreeEventStreamVersion}, ${evidence.worktreeOwnershipFingerprint},
          ${evidence.worktreeVerifiedAt}, ${evidence.worktreePath}, ${evidence.branch},
          ${evidence.planningThreadId}, ${evidence.planId},
          ${evidence.proposedPlanDigest}, ${evidence.providerInstanceId}, ${evidence.runtimeMode},
          ${evidence.modelSelectionJson}, ${evidence.modelSelectionFingerprint},
          'agent-control-verification-prompt-v1', ${evidence.templateVersion === "agent-control-verification-prompt-v2" ? evidence.templateVersion : null},
          ${evidence.promptContractFingerprint},
          ${evidence.promptText}, ${evidence.promptDigest}, ${evidence.resultSchemaVersion},
          ${evidence.resultSchemaFingerprint},
          ${evidence.turnRequestCommandId}, ${evidence.messageId}, ${evidence.messageEventId},
          ${evidence.turnRequestEventId}, ${evidence.messageEventTemplateJson},
          ${evidence.turnRequestEventTemplateJson}, ${evidence.eventTemplateDigest},
          ${evidence.providerDeliveryId}, ${evidence.createdAt}
        )
          `;
        }
        yield* sql`
        INSERT INTO agent_control_verification_handoff_receipts (
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
        INSERT INTO agent_control_verification_handoff_accepted (
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
        INSERT INTO agent_control_verification_deliveries (
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

  const loadAcceptedByHandoffId: AgentControlVerificationHandoffStoreShape["loadAcceptedByHandoffId"] =
    (handoffId) =>
      sql.withTransaction(selectAccepted("intent.handoff_id = ?", [handoffId], 2)).pipe(
        Effect.flatMap((rows) => single(rows, handoffId)),
        Effect.mapError((cause) => preserveStoreError("load-by-handoff", cause)),
      );
  const loadAcceptedByTurnRequestCommandId: AgentControlVerificationHandoffStoreShape["loadAcceptedByTurnRequestCommandId"] =
    (commandId) =>
      sql
        .withTransaction(selectAccepted("intent.turn_request_command_id = ?", [commandId], 2))
        .pipe(
          Effect.flatMap(single),
          Effect.mapError((cause) => preserveStoreError("load-by-turn-command", cause)),
        );
  const loadAcceptedByThreadId: AgentControlVerificationHandoffStoreShape["loadAcceptedByThreadId"] =
    (threadId) =>
      sql.withTransaction(selectAccepted("intent.thread_id = ?", [threadId], 2)).pipe(
        Effect.flatMap(single),
        Effect.mapError((cause) => preserveStoreError("load-by-thread", cause)),
      );
  const listRecoverable: AgentControlVerificationHandoffStoreShape["listRecoverable"] = (
    now,
    afterExclusive = "",
    limit = 100,
  ) =>
    sql
      .unsafe<{ readonly handoffId: string }>(
        `SELECT intent.handoff_id AS "handoffId"
      FROM agent_control_verification_handoff_intents intent
      LEFT JOIN agent_control_verification_deliveries delivery
        ON delivery.handoff_id = intent.handoff_id
      WHERE intent.handoff_id > ? AND (
        delivery.handoff_id IS NULL
        OR delivery.state IN ('pending','turn-accepted','provider-started')
        OR (delivery.state = 'retry-wait' AND delivery.next_attempt_at <= ?)
        OR (delivery.state IN ('claimed','delivery-attempted') AND delivery.claim_expires_at <= ?)
      )
      ORDER BY intent.handoff_id LIMIT ?`,
        [afterExclusive, now, now, Math.max(1, Math.min(1000, Math.floor(limit)))],
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("list-recoverable", cause)),
        Effect.map((rows) => rows.map((row) => row.handoffId)),
      );
  const isHandoffOwnedTurnRequest: AgentControlVerificationHandoffStoreShape["isHandoffOwnedTurnRequest"] =
    (commandId) =>
      sql<{ readonly count: number }>`SELECT count(*) AS count
      FROM agent_control_verification_handoff_accepted
      WHERE turn_request_command_id = ${commandId}`.pipe(
        Effect.mapError((cause) => persistenceError("is-owned", cause)),
        Effect.flatMap((rows) =>
          rows[0]?.count === 0 || rows[0]?.count === 1
            ? Effect.succeed(rows[0]?.count === 1)
            : Effect.fail(persistenceError("non-unique-ownership")),
        ),
      );
  const loadTurnAcceptance: AgentControlVerificationHandoffStoreShape["loadTurnAcceptance"] = (
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
        CAST(message_event_envelope_json AS BLOB) AS "messageEventEnvelopeBytes",
        CAST(turn_request_event_envelope_json AS BLOB) AS "turnRequestEventEnvelopeBytes",
        CAST(event_evidence_digest AS BLOB) AS "eventEvidenceDigestBytes",
        accepted_at AS "acceptedAt"
      FROM agent_control_verification_turn_accepted WHERE handoff_id = ${handoffId}
    `.pipe(
      Effect.mapError((cause) => persistenceError("load-turn-acceptance", cause)),
      Effect.flatMap((rows) =>
        rows.length === 0
          ? Effect.succeed(Option.none())
          : rows.length !== 1
            ? Effect.fail(candidateEvidenceError("non-unique-turn-acceptance"))
            : Effect.all([
                Effect.try({
                  try: () => decodeCanonicalUtf8Bytes(rows[0]!.messageEventEnvelopeBytes),
                  catch: (cause) =>
                    candidateEvidenceError("message-envelope-bytes", cause, handoffId),
                }),
                Effect.try({
                  try: () => decodeCanonicalUtf8Bytes(rows[0]!.turnRequestEventEnvelopeBytes),
                  catch: (cause) => candidateEvidenceError("turn-envelope-bytes", cause, handoffId),
                }),
                Effect.try({
                  try: () => decodeCanonicalUtf8Bytes(rows[0]!.eventEvidenceDigestBytes),
                  catch: (cause) =>
                    candidateEvidenceError("event-evidence-digest-bytes", cause, handoffId),
                }),
              ]).pipe(
                Effect.flatMap(
                  ([messageEventEnvelopeJson, turnRequestEventEnvelopeJson, eventEvidenceDigest]) =>
                    decodeAcceptance({
                      ...rows[0]!,
                      messageEventEnvelopeJson,
                      turnRequestEventEnvelopeJson,
                      eventEvidenceDigest,
                    }),
                ),
                Effect.map((row) =>
                  Option.some(row satisfies AgentControlVerificationTurnAcceptance),
                ),
                Effect.mapError((cause) =>
                  isStoreError(cause)
                    ? cause
                    : candidateEvidenceError("decode-turn-acceptance", cause, handoffId),
                ),
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
    terminal_event_id AS "terminalEventId", terminal_event_type AS "terminalEventType",
    terminal_provider_state AS "terminalProviderState",
    terminal_observation_digest AS "terminalObservationDigest",
    last_error_code AS "lastErrorCode", interrupt_requested AS "interruptRequested",
    updated_at AS "updatedAt"`;
  const updateOne = Effect.fn("AgentControlVerificationHandoffStore.updateOne")(function* (
    operation: string,
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length !== 1) return yield* revisionConflictError(`${operation}-cas-conflict`);
    const decoded = yield* decodeDelivery(rows[0]).pipe(
      Effect.mapError((cause) => persistenceError(`${operation}-decode`, cause)),
    );
    return { ...decoded, interruptRequested: decoded.interruptRequested === 1 };
  });
  const decodeTerminalReread = Effect.fn(
    "AgentControlVerificationHandoffStore.decodeTerminalReread",
  )(function* (operation: string, raw: Record<string, unknown>) {
    const decodeNullable = (value: unknown, field: string) =>
      value === null
        ? Effect.succeed(null)
        : Effect.try({
            try: () => decodeCanonicalUtf8Bytes(value),
            catch: (cause) => persistenceError(`${operation}-${field}-bytes`, cause),
          });
    const [
      terminalAt,
      terminalEventId,
      terminalEventType,
      terminalProviderState,
      terminalObservationDigest,
      lastErrorCode,
    ] = yield* Effect.all([
      decodeNullable(raw.terminalAtBytes, "terminal-at"),
      decodeNullable(raw.terminalEventIdBytes, "terminal-event-id"),
      decodeNullable(raw.terminalEventTypeBytes, "terminal-event-type"),
      decodeNullable(raw.terminalProviderStateBytes, "terminal-provider-state"),
      decodeNullable(raw.terminalObservationDigestBytes, "terminal-observation-digest"),
      decodeNullable(raw.lastErrorCodeBytes, "last-error-code"),
    ]);
    const decoded = yield* decodeDelivery({
      ...raw,
      terminalAt,
      terminalEventId,
      terminalEventType,
      terminalProviderState,
      terminalObservationDigest,
      lastErrorCode,
    }).pipe(Effect.mapError((cause) => persistenceError(`${operation}-decode`, cause)));
    return { ...decoded, interruptRequested: decoded.interruptRequested === 1 };
  });
  const markTurnAccepted: AgentControlVerificationHandoffStoreShape["markTurnAccepted"] = (
    handoffId,
    expectedRevision,
    at,
  ) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_verification_deliveries
        SET state='turn-accepted', revision=revision+1, updated_at=?
        WHERE handoff_id=? AND revision=? AND state='pending'
          AND EXISTS (SELECT 1 FROM agent_control_verification_turn_accepted accepted
            WHERE accepted.handoff_id=agent_control_verification_deliveries.handoff_id)
        RETURNING ${returning}`,
          [at, handoffId, expectedRevision],
        ),
      )
      .pipe(
        Effect.mapError((cause) => persistenceError("mark-turn-accepted", cause)),
        Effect.flatMap((rows) => updateOne("mark-turn-accepted", rows)),
      );
  const claim: AgentControlVerificationHandoffStoreShape["claim"] = (input) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql.unsafe<Record<string, unknown>>(
            `UPDATE agent_control_verification_deliveries
          SET state='claimed', revision=revision+1, claim_owner_id=?, claim_generation=claim_generation+1,
            claim_expires_at=?, attempt_count=attempt_count+1, next_attempt_at=NULL, updated_at=?
          WHERE handoff_id=? AND (state='turn-accepted' OR (state='retry-wait' AND next_attempt_at<=?)
            OR (state='claimed' AND claim_expires_at<=?)) RETURNING handoff_id`,
            [input.ownerId, input.expiresAt, input.now, input.handoffId, input.now, input.now],
          );
          return rows.length === 0
            ? Option.none<AgentControlVerificationClaim>()
            : yield* loadAcceptedByHandoffId(input.handoffId);
        }),
      )
      .pipe(Effect.mapError((cause) => preserveStoreError("claim", cause)));
  const markDeliveryAttempted: AgentControlVerificationHandoffStoreShape["markDeliveryAttempted"] =
    (input) =>
      sql
        .withTransaction(
          Effect.gen(function* () {
            yield* sql`INSERT OR IGNORE INTO agent_control_verification_delivery_attestations (
        provider_delivery_id, provider_instance_id, model_selection_json,
        model_selection_fingerprint, recorded_at) VALUES (${input.providerDeliveryId},
        ${input.providerInstanceId}, ${input.turnModelSelectionJson},
        ${input.turnModelSelectionFingerprint}, ${input.attemptedAt})`;
            const rows = yield* sql.unsafe<Record<string, unknown>>(
              `UPDATE agent_control_verification_deliveries
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
            return yield* updateOne("mark-delivery-attempted", rows);
          }),
        )
        .pipe(Effect.mapError((cause) => preserveStoreError("mark-delivery-attempted", cause)));
  const markProviderStarted: AgentControlVerificationHandoffStoreShape["markProviderStarted"] = (
    input,
  ) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_verification_deliveries
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
  const scheduleRetry: AgentControlVerificationHandoffStoreShape["scheduleRetry"] = (input) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_verification_deliveries
      SET state='retry-wait', revision=revision+1, claim_owner_id=NULL, claim_expires_at=NULL,
        next_attempt_at=?, last_error_code=?, updated_at=? WHERE handoff_id=? AND revision=?
        AND state='claimed' AND claim_owner_id=? AND claim_generation=?
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
  const markAmbiguous: AgentControlVerificationHandoffStoreShape["markAmbiguous"] = (input) =>
    sql
      .withTransaction(
        sql.unsafe<Record<string, unknown>>(
          `UPDATE agent_control_verification_deliveries
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
  const observeProviderStarted: AgentControlVerificationHandoffStoreShape["observeProviderStarted"] =
    (input) =>
      sql
        .withTransaction(
          sql.unsafe<Record<string, unknown>>(
            `UPDATE agent_control_verification_deliveries
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
  const observeProviderTerminal: AgentControlVerificationHandoffStoreShape["observeProviderTerminal"] =
    (input) =>
      Effect.gen(function* () {
        const terminalSource =
          input.observation.runtimeEventType === "turn.completed" &&
          input.observation.providerState !== null
            ? {
                runtimeEventId: input.observation.runtimeEventId,
                runtimeEventType: input.observation.runtimeEventType,
                threadId: input.threadId,
                providerInstanceId: input.providerInstanceId,
                providerTurnId: input.providerTurnId,
                providerState: input.observation.providerState,
                terminalAt: input.observation.terminalAt,
              }
            : input.observation.runtimeEventType === "turn.aborted" &&
                input.observation.providerState === null
              ? {
                  runtimeEventId: input.observation.runtimeEventId,
                  runtimeEventType: input.observation.runtimeEventType,
                  threadId: input.threadId,
                  providerInstanceId: input.providerInstanceId,
                  providerTurnId: input.providerTurnId,
                  terminalAt: input.observation.terminalAt,
                }
              : undefined;
        if (terminalSource === undefined) {
          return yield* terminalConflictError("observe-provider-terminal-shape");
        }
        const canonicalObservation = yield* normalizeVerificationTerminalSource(terminalSource, {
          providerDeliveryId: input.providerDeliveryId,
          threadId: input.threadId,
          providerInstanceId: input.providerInstanceId,
          providerTurnId: input.providerTurnId,
        }).pipe(
          Effect.mapError((cause) =>
            terminalConflictError("observe-provider-terminal-normalize", cause),
          ),
        );
        if (
          canonicalObservation.deliveryState !== input.observation.deliveryState ||
          canonicalObservation.lastErrorCode !== input.observation.lastErrorCode ||
          canonicalObservation.observationDigest !== input.observation.observationDigest
        ) {
          return yield* terminalConflictError("observe-provider-terminal-observation");
        }
        const readDelivery = Effect.fn("AgentControlVerificationHandoffStore.readTerminalDelivery")(
          function* (operation: string) {
            const rows = yield* sql
              .unsafe<Record<string, unknown>>(
                `SELECT ${returning},
                CASE WHEN terminal_at IS NULL THEN NULL
                  ELSE CAST(terminal_at AS BLOB) END AS "terminalAtBytes",
                CASE WHEN terminal_event_id IS NULL THEN NULL
                  ELSE CAST(terminal_event_id AS BLOB) END AS "terminalEventIdBytes",
                CASE WHEN terminal_event_type IS NULL THEN NULL
                  ELSE CAST(terminal_event_type AS BLOB) END AS "terminalEventTypeBytes",
                CASE WHEN terminal_provider_state IS NULL THEN NULL
                  ELSE CAST(terminal_provider_state AS BLOB) END AS "terminalProviderStateBytes",
                CASE WHEN terminal_observation_digest IS NULL THEN NULL
                  ELSE CAST(terminal_observation_digest AS BLOB)
                  END AS "terminalObservationDigestBytes",
                CASE WHEN last_error_code IS NULL THEN NULL
                  ELSE CAST(last_error_code AS BLOB) END AS "lastErrorCodeBytes"
              FROM agent_control_verification_deliveries
              WHERE provider_delivery_id=?`,
                [input.providerDeliveryId],
              )
              .pipe(Effect.mapError((cause) => persistenceError(`${operation}-query`, cause)));
            if (rows.length !== 1) {
              return yield* revisionConflictError(`${operation}-missing`);
            }
            return yield* decodeTerminalReread(operation, rows[0]!);
          },
        );
        const identityMatches = (delivery: AgentControlVerificationDelivery) =>
          delivery.handoffId === input.handoffId &&
          delivery.providerDeliveryId === input.providerDeliveryId &&
          delivery.threadId === input.threadId &&
          delivery.providerInstanceId === input.providerInstanceId &&
          delivery.providerTurnId === input.providerTurnId &&
          delivery.stageRunId === input.stageRunId &&
          delivery.attemptId === input.attemptId &&
          delivery.leaseId === input.leaseId &&
          delivery.leaseHolderId === input.leaseHolderId &&
          delivery.fenceToken === input.fenceToken &&
          delivery.modelSelectionFingerprint === input.modelSelectionFingerprint;
        const observationMatches = (delivery: AgentControlVerificationDelivery) =>
          delivery.state === input.observation.deliveryState &&
          delivery.terminalAt === input.observation.terminalAt &&
          delivery.terminalEventId === input.observation.runtimeEventId &&
          delivery.terminalEventType === input.observation.runtimeEventType &&
          delivery.terminalProviderState === input.observation.providerState &&
          delivery.terminalObservationDigest === input.observation.observationDigest &&
          delivery.lastErrorCode === input.observation.lastErrorCode;
        const isTerminal = (delivery: AgentControlVerificationDelivery) =>
          delivery.state === "completed" ||
          delivery.state === "failed" ||
          delivery.state === "interrupted";

        const preflight = yield* readDelivery("observe-provider-terminal-preflight");
        if (!identityMatches(preflight)) {
          return yield* terminalConflictError("observe-provider-terminal-identity");
        }
        if (isTerminal(preflight)) {
          if (observationMatches(preflight)) {
            return { _tag: "Replayed", delivery: preflight } as const;
          }
          return yield* terminalConflictError("observe-provider-terminal-conflict");
        }
        if (
          preflight.state !== "provider-started" ||
          preflight.providerAcceptedAt === null ||
          preflight.revision !== input.expectedRevision
        ) {
          return yield* revisionConflictError("observe-provider-terminal-preflight-conflict");
        }

        yield* input.beforeCas ?? Effect.void;
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            const updated = yield* sql.unsafe<Record<string, unknown>>(
              `UPDATE agent_control_verification_deliveries
                SET state=?, revision=revision+1, claim_owner_id=NULL,
                  claim_expires_at=NULL, next_attempt_at=NULL, terminal_at=?,
                  terminal_event_id=?, terminal_event_type=?, terminal_provider_state=?,
                  terminal_observation_digest=?, last_error_code=?, updated_at=?
                WHERE handoff_id=? AND provider_delivery_id=? AND thread_id=?
                  AND provider_instance_id=? AND provider_turn_id=?
                  AND stage_run_id=? AND attempt_id=? AND lease_id=?
                  AND lease_holder_id=? AND fence_token=?
                  AND model_selection_fingerprint=? AND revision=?
                  AND state='provider-started' AND provider_accepted_at IS NOT NULL
                RETURNING ${returning}`,
              [
                input.observation.deliveryState,
                input.observation.terminalAt,
                input.observation.runtimeEventId,
                input.observation.runtimeEventType,
                input.observation.providerState,
                input.observation.observationDigest,
                input.observation.lastErrorCode,
                input.observedAt,
                input.handoffId,
                input.providerDeliveryId,
                input.threadId,
                input.providerInstanceId,
                input.providerTurnId,
                input.stageRunId,
                input.attemptId,
                input.leaseId,
                input.leaseHolderId,
                input.fenceToken,
                input.modelSelectionFingerprint,
                input.expectedRevision,
              ],
            );
            if (updated.length === 1) {
              return {
                _tag: "Observed",
                delivery: yield* updateOne("observe-provider-terminal", updated),
              } as const;
            }
            if (updated.length > 1) {
              return yield* terminalConflictError("observe-provider-terminal-non-unique-update");
            }
            const delivery = yield* readDelivery("observe-provider-terminal-reread");
            if (!identityMatches(delivery)) {
              return yield* terminalConflictError("observe-provider-terminal-identity");
            }
            if (isTerminal(delivery)) {
              if (observationMatches(delivery)) {
                return { _tag: "Replayed", delivery } as const;
              }
              return yield* terminalConflictError("observe-provider-terminal-conflict");
            }
            return yield* revisionConflictError("observe-provider-terminal-cas-conflict");
          }),
        );
      }).pipe(
        Effect.catch((cause) => {
          if (isStoreError(cause)) return Effect.fail(cause);
          return sql<{
            readonly providerDeliveryId: string;
          }>`
              SELECT provider_delivery_id AS "providerDeliveryId"
              FROM agent_control_verification_deliveries
              WHERE provider_instance_id = ${input.providerInstanceId}
                AND terminal_event_id = ${input.observation.runtimeEventId}
          `.pipe(
            Effect.mapError((classificationCause) =>
              persistenceError(
                "observe-provider-terminal-classify-update-failure",
                classificationCause,
              ),
            ),
            Effect.flatMap((owners) =>
              owners.some((row) => row.providerDeliveryId !== input.providerDeliveryId)
                ? Effect.fail(terminalConflictError("observe-provider-terminal-event-owner"))
                : Effect.fail(persistenceError("observe-provider-terminal-update", cause)),
            ),
          );
        }),
      );
  const listStageStartCandidates: AgentControlVerificationHandoffStoreShape["listStageStartCandidates"] =
    (afterExclusive = "", limit = 100) =>
      sql<{ readonly handoffId: string }>`
    SELECT accepted.handoff_id AS "handoffId"
    FROM agent_control_verification_turn_accepted accepted
    JOIN agent_control_verification_deliveries delivery
      ON delivery.handoff_id = accepted.handoff_id
    WHERE accepted.handoff_id > ${afterExclusive}
      AND delivery.state IN ('provider-started','completed','failed','interrupted')
      AND delivery.provider_turn_id IS NOT NULL AND delivery.provider_accepted_at IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM agent_control_verification_stage_started_markers marker
        WHERE marker.provider_delivery_id=delivery.provider_delivery_id)
    ORDER BY accepted.handoff_id LIMIT ${Math.max(1, Math.min(1000, Math.floor(limit)))}`.pipe(
        Effect.mapError((cause) => persistenceError("list-stage-start-candidates", cause)),
        Effect.map((rows) => rows.map((row) => row.handoffId)),
      );
  return AgentControlVerificationHandoffStore.of({
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

export const AgentControlVerificationHandoffStoreLive = Layer.effect(
  AgentControlVerificationHandoffStore,
  make,
);
