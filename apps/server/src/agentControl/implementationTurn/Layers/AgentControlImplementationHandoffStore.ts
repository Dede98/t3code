import {
  AgentControlControlledThreadReservationId,
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
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  sha256Utf8,
} from "../../initialPlanning/eventEvidence.ts";
import {
  implementationMessagePayload,
  implementationTurnRequestPayload,
} from "../eventEvidence.ts";
import {
  deriveImplementationHandoffId,
  deriveImplementationMessageEventId,
  deriveImplementationMessageId,
  deriveImplementationProviderDeliveryId,
  deriveImplementationTurnRequestCommandId,
  deriveImplementationTurnRequestEventId,
  fingerprintImplementationHandoff,
} from "../identity.ts";
import type { AgentControlImplementationClaim } from "../model.ts";
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
const decodeAcceptance = Schema.decodeUnknownEffect(TurnAcceptanceRow);
const decodeModelSelection = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeModelSelection = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

const storeError = (operation: string, cause?: unknown) =>
  new AgentControlImplementationStoreError({
    operation,
    ...(cause === undefined ? {} : { cause }),
  });

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
      JOIN agent_control_implementation_materialization_markers marker
        ON marker.materialization_evidence_id = intent.materialization_evidence_id
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
      catch: (cause) => storeError("model-selection-bytes", cause),
    });
    const promptText = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.promptBytes),
      catch: (cause) => storeError("prompt-bytes", cause),
    });
    const messageEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.messageTemplateBytes),
      catch: (cause) => storeError("message-template-bytes", cause),
    });
    const turnRequestEventTemplateJson = yield* Effect.try({
      try: () => decodeCanonicalUtf8Bytes(raw.turnTemplateBytes),
      catch: (cause) => storeError("turn-template-bytes", cause),
    });
    const evidence = yield* decodeEvidence({
      ...raw,
      modelSelectionJson,
      promptText,
      messageEventTemplateJson,
      turnRequestEventTemplateJson,
    }).pipe(Effect.mapError((cause) => storeError("decode-evidence", cause)));
    const modelSelection = yield* decodeModelSelection(evidence.modelSelectionJson).pipe(
      Effect.mapError((cause) => storeError("decode-model-selection", cause)),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelection(modelSelection).pipe(
      Effect.mapError((cause) => storeError("encode-model-selection", cause)),
    );
    const handoffId = deriveImplementationHandoffId(evidence.materializationEvidenceId);
    const turnRequestCommandId = deriveImplementationTurnRequestCommandId(evidence.handoffId);
    const messageId = deriveImplementationMessageId(evidence.handoffId);
    const providerDeliveryId = deriveImplementationProviderDeliveryId(evidence.handoffId);
    const messageEventId = deriveImplementationMessageEventId(evidence.turnRequestCommandId);
    const turnRequestEventId = deriveImplementationTurnRequestEventId(
      evidence.turnRequestCommandId,
    );
    const sourceProposedPlan = {
      threadId: evidence.planningThreadId,
      planId: evidence.planId,
    } as const;
    const expectedMessageTemplate = canonicalInitialPlanningEventTemplate({
      streamVersion: 3,
      eventId: messageEventId,
      aggregateKind: "thread",
      aggregateId: evidence.threadId,
      type: "thread.message-sent",
      occurredAt: evidence.createdAt,
      commandId: evidence.turnRequestCommandId,
      causationEventId: null,
      correlationId: evidence.turnRequestCommandId,
      actorKind: "client",
      payload: implementationMessagePayload({
        threadId: evidence.threadId,
        messageId: evidence.messageId,
        promptText: evidence.promptText,
        createdAt: evidence.createdAt,
      }),
      metadata: {},
    });
    const expectedTurnTemplate = canonicalInitialPlanningEventTemplate({
      streamVersion: 4,
      eventId: turnRequestEventId,
      aggregateKind: "thread",
      aggregateId: evidence.threadId,
      type: "thread.turn-start-requested",
      occurredAt: evidence.createdAt,
      commandId: evidence.turnRequestCommandId,
      causationEventId: messageEventId,
      correlationId: evidence.turnRequestCommandId,
      actorKind: "client",
      payload: implementationTurnRequestPayload({
        threadId: evidence.threadId,
        messageId: evidence.messageId,
        modelSelection,
        runtimeMode: evidence.runtimeMode,
        sourceProposedPlan,
        createdAt: evidence.createdAt,
      }),
      metadata: {},
    });
    yield* Effect.try({
      try: () => {
        parseCanonicalJson(evidence.messageEventTemplateJson);
        parseCanonicalJson(evidence.turnRequestEventTemplateJson);
      },
      catch: (cause) => storeError("event-template-json", cause),
    });
    if (
      evidence.handoffId !== handoffId ||
      evidence.turnRequestCommandId !== turnRequestCommandId ||
      evidence.messageId !== messageId ||
      evidence.providerDeliveryId !== providerDeliveryId ||
      evidence.messageEventId !== messageEventId ||
      evidence.turnRequestEventId !== turnRequestEventId ||
      evidence.modelSelectionJson !== canonicalModelSelectionJson ||
      evidence.providerInstanceId !== modelSelection.instanceId ||
      evidence.promptDigest !== sha256Utf8(evidence.promptText) ||
      evidence.messageEventTemplateJson !== expectedMessageTemplate ||
      evidence.turnRequestEventTemplateJson !== expectedTurnTemplate ||
      evidence.eventTemplateDigest !==
        combinedInitialPlanningEventDigest(expectedMessageTemplate, expectedTurnTemplate) ||
      evidence.handoffFingerprint !==
        fingerprintImplementationHandoff({ ...evidence, modelSelection })
    )
      return yield* storeError("evidence-invariant");

    const resumeCursor =
      raw.resumeCursorBytes === null
        ? null
        : yield* Effect.try({
            try: () => decodeCanonicalUtf8Bytes(raw.resumeCursorBytes),
            catch: (cause) => storeError("resume-cursor-bytes", cause),
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
    }).pipe(Effect.mapError((cause) => storeError("decode-delivery", cause)));
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
      return yield* storeError("delivery-evidence-invariant");
    return {
      evidence: { ...evidence, modelSelection },
      delivery: { ...delivery, interruptRequested: raw.interruptRequested === 1 },
    } satisfies AgentControlImplementationClaim;
  });

  const single = Effect.fn("AgentControlImplementationHandoffStore.single")(function* (
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length === 0) return Option.none<AgentControlImplementationClaim>();
    if (rows.length !== 1) return yield* storeError("non-unique-evidence");
    return Option.some(yield* claimFromRow(rows[0]!));
  });

  const insertAcceptedInTransaction: AgentControlImplementationHandoffStoreShape["insertAcceptedInTransaction"] =
    (evidence) =>
      Effect.gen(function* () {
        yield* sql`
        INSERT INTO agent_control_implementation_handoff_intents (
          handoff_id, handoff_fingerprint, materialization_evidence_id,
          materialization_receipt_id, admission_marker_id, project_id, task_id,
          task_revision, github_intake_sequence, source_identity_fingerprint,
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
          ${evidence.sourceIdentityFingerprint}, ${evidence.stageRunId}, ${evidence.attemptId},
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
      }).pipe(Effect.mapError((cause) => storeError("insert-accepted", cause)));

  const loadAcceptedByHandoffId: AgentControlImplementationHandoffStoreShape["loadAcceptedByHandoffId"] =
    (handoffId) =>
      selectAccepted("intent.handoff_id = ?", [handoffId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-handoff", cause)),
        Effect.flatMap(single),
      );
  const loadAcceptedByTurnRequestCommandId: AgentControlImplementationHandoffStoreShape["loadAcceptedByTurnRequestCommandId"] =
    (commandId) =>
      selectAccepted("intent.turn_request_command_id = ?", [commandId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-turn-command", cause)),
        Effect.flatMap(single),
      );
  const loadAcceptedByThreadId: AgentControlImplementationHandoffStoreShape["loadAcceptedByThreadId"] =
    (threadId) =>
      selectAccepted("intent.thread_id = ?", [threadId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-thread", cause)),
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
        Effect.mapError((cause) => storeError("list-recoverable", cause)),
        Effect.map((rows) => rows.map((row) => row.handoffId)),
      );
  const isHandoffOwnedTurnRequest: AgentControlImplementationHandoffStoreShape["isHandoffOwnedTurnRequest"] =
    (commandId) =>
      sql<{ readonly count: number }>`SELECT count(*) AS count
      FROM agent_control_implementation_handoff_accepted
      WHERE turn_request_command_id = ${commandId}`.pipe(
        Effect.mapError((cause) => storeError("is-owned", cause)),
        Effect.flatMap((rows) =>
          rows[0]?.count === 0 || rows[0]?.count === 1
            ? Effect.succeed(rows[0]?.count === 1)
            : Effect.fail(storeError("non-unique-ownership")),
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
      Effect.mapError((cause) => storeError("load-turn-acceptance", cause)),
      Effect.flatMap((rows) =>
        rows.length === 0
          ? Effect.succeed(Option.none())
          : rows.length !== 1
            ? Effect.fail(storeError("non-unique-turn-acceptance"))
            : decodeAcceptance(rows[0]).pipe(
                Effect.map((row) =>
                  Option.some(row satisfies AgentControlImplementationTurnAcceptance),
                ),
                Effect.mapError((cause) => storeError("decode-turn-acceptance", cause)),
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
    if (rows.length !== 1) return yield* storeError(`${operation}-cas-conflict`);
    const decoded = yield* decodeDelivery(rows[0]).pipe(
      Effect.mapError((cause) => storeError(`${operation}-decode`, cause)),
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
        Effect.mapError((cause) => storeError("mark-turn-accepted", cause)),
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
      .pipe(Effect.mapError((cause) => storeError("claim", cause)));
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
          Effect.mapError((cause) => storeError("mark-delivery-attempted", cause)),
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
        Effect.mapError((cause) => storeError("mark-provider-started", cause)),
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
        Effect.mapError((cause) => storeError("schedule-retry", cause)),
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
        Effect.mapError((cause) => storeError("mark-ambiguous", cause)),
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
          Effect.mapError((cause) => storeError("observe-provider-started", cause)),
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
          Effect.mapError((cause) => storeError("observe-provider-terminal", cause)),
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
        Effect.mapError((cause) => storeError("list-stage-start-candidates", cause)),
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
