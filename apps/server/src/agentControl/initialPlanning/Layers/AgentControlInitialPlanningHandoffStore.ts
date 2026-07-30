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
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../identity.ts";
import type {
  AgentControlInitialPlanningClaim,
  AgentControlInitialPlanningDelivery,
} from "../model.ts";
import {
  AgentControlInitialPlanningHandoffStore,
  AgentControlInitialPlanningStoreError,
  type AgentControlInitialPlanningHandoffStoreShape,
  type AgentControlInitialPlanningTurnAcceptance,
} from "../Services/AgentControlInitialPlanningHandoffStore.ts";

const EvidenceRow = Schema.Struct({
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  coordinatorCommandId: CommandId,
  coordinatorCommandFingerprint: Schema.String,
  materializationCommandId: CommandId,
  materializationCommandFingerprint: Schema.String,
  projectId: ProjectId,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  taskId: Schema.String,
  taskRevision: Schema.Int,
  githubIntakeSequence: Schema.Int,
  sourceIdentityFingerprint: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  roleId: Schema.Literal("planning"),
  stageKind: Schema.Literal("planning"),
  stageOrdinal: Schema.Literal(1),
  attemptOrdinal: Schema.Literal(1),
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Int,
  worktreeReservationId: Schema.String,
  worktreePath: Schema.String,
  providerInstanceId: ProviderInstanceId,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionJson: Schema.String,
  planningRole: Schema.Literal("planner"),
  templateVersion: Schema.String,
  promptText: Schema.String,
  turnRequestCommandId: CommandId,
  messageId: MessageId,
  providerDeliveryId: Schema.String,
  createdAt: Schema.String,
  planningDeadlineAt: Schema.String,
});

const DeliveryRow = Schema.Struct({
  providerDeliveryId: Schema.String,
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  turnRequestCommandId: CommandId,
  messageId: MessageId,
  providerInstanceId: ProviderInstanceId,
  state: Schema.Literals([
    "pending",
    "turn-accepted",
    "claimed",
    "delivery-attempted",
    "provider-started",
    "interrupt-requested",
    "retry-wait",
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
  planningDeadlineAt: Schema.String,
  providerTurnId: Schema.NullOr(Schema.String),
  providerAcceptedAt: Schema.NullOr(Schema.String),
  terminalAt: Schema.NullOr(Schema.String),
  lastErrorCode: Schema.NullOr(Schema.String),
  interruptRequested: Schema.Int,
  updatedAt: Schema.String,
});

const TurnAcceptanceRow = Schema.Struct({
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  turnRequestCommandId: CommandId,
  messageId: MessageId,
  messageEventId: Schema.String,
  messageEventSequence: Schema.Int,
  turnRequestEventId: Schema.String,
  turnRequestEventSequence: Schema.Int,
  acceptedAt: Schema.String,
});

const decodeEvidenceRow = Schema.decodeUnknownEffect(EvidenceRow);
const decodeDeliveryRow = Schema.decodeUnknownEffect(DeliveryRow);
const decodeTurnAcceptanceRow = Schema.decodeUnknownEffect(TurnAcceptanceRow);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeModelSelectionJson = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

const storeError = (operation: string, cause?: unknown) =>
  new AgentControlInitialPlanningStoreError({
    operation,
    ...(cause === undefined ? {} : { cause }),
  });

const deliveryFromRow = Effect.fn("AgentControlInitialPlanningHandoffStore.deliveryFromRow")(
  function* (raw: unknown) {
    const row = yield* decodeDeliveryRow(raw).pipe(
      Effect.mapError((cause) => storeError("decode-delivery", cause)),
    );
    if (
      row.revision < 0 ||
      row.claimGeneration < 0 ||
      row.attemptCount < 0 ||
      ![0, 1].includes(row.interruptRequested)
    ) {
      return yield* storeError("decode-delivery-invariant");
    }
    return {
      ...row,
      interruptRequested: row.interruptRequested === 1,
    } satisfies AgentControlInitialPlanningDelivery;
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
      SELECT
        intent.handoff_id AS "handoffId",
        intent.handoff_fingerprint AS "handoffFingerprint",
        intent.coordinator_command_id AS "coordinatorCommandId",
        intent.coordinator_command_fingerprint AS "coordinatorCommandFingerprint",
        intent.materialization_command_id AS "materializationCommandId",
        intent.materialization_command_fingerprint AS
          "materializationCommandFingerprint",
        intent.project_id AS "projectId",
        intent.controlled_thread_reservation_id AS
          "controlledThreadReservationId",
        intent.thread_id AS "threadId",
        intent.task_id AS "taskId",
        intent.task_revision AS "taskRevision",
        intent.github_intake_sequence AS "githubIntakeSequence",
        intent.source_identity_fingerprint AS "sourceIdentityFingerprint",
        intent.stage_run_id AS "stageRunId",
        intent.attempt_id AS "attemptId",
        intent.role_id AS "roleId",
        intent.stage_kind AS "stageKind",
        intent.stage_ordinal AS "stageOrdinal",
        intent.attempt_ordinal AS "attemptOrdinal",
        intent.lease_id AS "leaseId",
        intent.lease_holder_id AS "leaseHolderId",
        intent.fence_token AS "fenceToken",
        intent.worktree_reservation_id AS "worktreeReservationId",
        intent.worktree_path AS "worktreePath",
        intent.provider_instance_id AS "providerInstanceId",
        intent.runtime_mode AS "runtimeMode",
        intent.model_selection_json AS "modelSelectionJson",
        intent.planning_role AS "planningRole",
        intent.template_version AS "templateVersion",
        intent.prompt_text AS "promptText",
        intent.turn_request_command_id AS "turnRequestCommandId",
        intent.message_id AS "messageId",
        intent.provider_delivery_id AS "providerDeliveryId",
        intent.created_at AS "createdAt",
        intent.planning_deadline_at AS "planningDeadlineAt",
        delivery.state,
        delivery.revision,
        delivery.claim_owner_id AS "claimOwnerId",
        delivery.claim_generation AS "claimGeneration",
        delivery.claim_expires_at AS "claimExpiresAt",
        delivery.attempt_count AS "attemptCount",
        delivery.next_attempt_at AS "nextAttemptAt",
        delivery.provider_turn_id AS "providerTurnId",
        delivery.provider_accepted_at AS "providerAcceptedAt",
        delivery.terminal_at AS "terminalAt",
        delivery.last_error_code AS "lastErrorCode",
        delivery.interrupt_requested AS "interruptRequested",
        delivery.updated_at AS "updatedAt"
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_initial_planning_deliveries delivery
        ON delivery.handoff_id = intent.handoff_id
      WHERE ${predicate}
      ORDER BY intent.handoff_id
      LIMIT ?
      `,
      [...parameters, Math.max(1, Math.min(1000, Math.floor(limit)))],
    );

  const claimFromRow = Effect.fn("AgentControlInitialPlanningHandoffStore.claimFromRow")(function* (
    raw: Record<string, unknown>,
  ) {
    const evidenceRow = yield* decodeEvidenceRow(raw).pipe(
      Effect.mapError((cause) => storeError("decode-evidence", cause)),
    );
    const modelSelection = yield* decodeModelSelectionJson(evidenceRow.modelSelectionJson).pipe(
      Effect.mapError((cause) => storeError("decode-model-selection", cause)),
    );
    const canonicalModelSelectionJson = yield* encodeModelSelectionJson(modelSelection).pipe(
      Effect.mapError((cause) => storeError("encode-model-selection", cause)),
    );
    const [handoffId, turnRequestCommandId, messageId, providerDeliveryId] = yield* Effect.all([
      deriveAgentControlInitialPlanningHandoffId(
        evidenceRow.controlledThreadReservationId,
        evidenceRow.threadId,
      ),
      deriveAgentControlInitialPlanningTurnRequestCommandId(evidenceRow.handoffId),
      deriveAgentControlInitialPlanningMessageId(evidenceRow.handoffId),
      deriveAgentControlInitialPlanningProviderDeliveryId(evidenceRow.handoffId),
    ]);
    const expectedFingerprint = fingerprintAgentControlInitialPlanningHandoff({
      ...evidenceRow,
      modelSelectionJson: canonicalModelSelectionJson,
    });
    if (
      canonicalModelSelectionJson !== evidenceRow.modelSelectionJson ||
      evidenceRow.providerInstanceId !== modelSelection.instanceId ||
      evidenceRow.handoffId !== handoffId ||
      evidenceRow.turnRequestCommandId !== turnRequestCommandId ||
      evidenceRow.messageId !== messageId ||
      evidenceRow.providerDeliveryId !== providerDeliveryId ||
      evidenceRow.handoffFingerprint !== expectedFingerprint ||
      !Number.isFinite(Date.parse(evidenceRow.createdAt)) ||
      !Number.isFinite(Date.parse(evidenceRow.planningDeadlineAt)) ||
      Date.parse(evidenceRow.planningDeadlineAt) <= Date.parse(evidenceRow.createdAt)
    ) {
      return yield* storeError("evidence-invariant");
    }
    const delivery = yield* deliveryFromRow(raw);
    if (
      delivery.handoffId !== evidenceRow.handoffId ||
      delivery.handoffFingerprint !== evidenceRow.handoffFingerprint ||
      delivery.controlledThreadReservationId !== evidenceRow.controlledThreadReservationId ||
      delivery.threadId !== evidenceRow.threadId ||
      delivery.turnRequestCommandId !== evidenceRow.turnRequestCommandId ||
      delivery.messageId !== evidenceRow.messageId ||
      delivery.providerDeliveryId !== evidenceRow.providerDeliveryId ||
      delivery.providerInstanceId !== evidenceRow.providerInstanceId ||
      delivery.planningDeadlineAt !== evidenceRow.planningDeadlineAt
    ) {
      return yield* storeError("delivery-evidence-invariant");
    }
    return {
      evidence: {
        ...evidenceRow,
        modelSelection,
      },
      delivery,
    } satisfies AgentControlInitialPlanningClaim;
  });

  const singleClaim = Effect.fn("AgentControlInitialPlanningHandoffStore.singleClaim")(function* (
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length === 0) return Option.none<AgentControlInitialPlanningClaim>();
    if (rows.length !== 1) return yield* storeError("non-unique-evidence");
    return Option.some(yield* claimFromRow(rows[0]!));
  });

  const insertAcceptedInTransaction: AgentControlInitialPlanningHandoffStoreShape["insertAcceptedInTransaction"] =
    (evidence, hooks) =>
      Effect.gen(function* () {
        yield* sql`
          INSERT INTO agent_control_initial_planning_handoff_intents (
            handoff_id, handoff_fingerprint, coordinator_command_id,
            coordinator_command_fingerprint, materialization_command_id,
            materialization_command_fingerprint, project_id,
            controlled_thread_reservation_id, thread_id, task_id, task_revision,
            github_intake_sequence, source_identity_fingerprint, stage_run_id,
            attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
            lease_id, lease_holder_id, fence_token, worktree_reservation_id,
            worktree_path, provider_instance_id, runtime_mode,
            model_selection_json, planning_role, template_version, prompt_text,
            turn_request_command_id, message_id, provider_delivery_id,
            created_at, planning_deadline_at, accepted_marker_handoff_id
          ) VALUES (
            ${evidence.handoffId}, ${evidence.handoffFingerprint},
            ${evidence.coordinatorCommandId}, ${evidence.coordinatorCommandFingerprint},
            ${evidence.materializationCommandId},
            ${evidence.materializationCommandFingerprint}, ${evidence.projectId},
            ${evidence.controlledThreadReservationId}, ${evidence.threadId},
            ${evidence.taskId}, CAST(${evidence.taskRevision} AS INTEGER),
            CAST(${evidence.githubIntakeSequence} AS INTEGER),
            ${evidence.sourceIdentityFingerprint},
            ${evidence.stageRunId}, ${evidence.attemptId}, ${evidence.roleId},
            ${evidence.stageKind}, CAST(${evidence.stageOrdinal} AS INTEGER),
            CAST(${evidence.attemptOrdinal} AS INTEGER),
            ${evidence.leaseId}, ${evidence.leaseHolderId},
            CAST(${evidence.fenceToken} AS INTEGER),
            ${evidence.worktreeReservationId}, ${evidence.worktreePath},
            ${evidence.providerInstanceId}, ${evidence.runtimeMode},
            ${evidence.modelSelectionJson}, ${evidence.planningRole},
            ${evidence.templateVersion}, ${evidence.promptText},
            ${evidence.turnRequestCommandId}, ${evidence.messageId},
            ${evidence.providerDeliveryId}, ${evidence.createdAt},
            ${evidence.planningDeadlineAt}, ${evidence.handoffId}
          )
        `;
        yield* hooks?.afterIntent?.() ?? Effect.void;
        yield* sql`
          INSERT INTO agent_control_initial_planning_handoff_receipts (
            handoff_id, handoff_fingerprint, coordinator_command_id,
            coordinator_command_fingerprint, controlled_thread_reservation_id,
            thread_id, turn_request_command_id, message_id, provider_delivery_id,
            status, accepted_at, accepted_marker_handoff_id
          ) VALUES (
            ${evidence.handoffId}, ${evidence.handoffFingerprint},
            ${evidence.coordinatorCommandId}, ${evidence.coordinatorCommandFingerprint},
            ${evidence.controlledThreadReservationId}, ${evidence.threadId},
            ${evidence.turnRequestCommandId}, ${evidence.messageId},
            ${evidence.providerDeliveryId}, 'accepted', ${evidence.createdAt},
            ${evidence.handoffId}
          )
        `;
        yield* hooks?.afterReceipt?.() ?? Effect.void;
        yield* sql`
          INSERT INTO agent_control_initial_planning_handoff_accepted (
            handoff_id, handoff_fingerprint, coordinator_command_id,
            coordinator_command_fingerprint, controlled_thread_reservation_id,
            thread_id, turn_request_command_id, message_id, provider_delivery_id,
            accepted_at
          ) VALUES (
            ${evidence.handoffId}, ${evidence.handoffFingerprint},
            ${evidence.coordinatorCommandId}, ${evidence.coordinatorCommandFingerprint},
            ${evidence.controlledThreadReservationId}, ${evidence.threadId},
            ${evidence.turnRequestCommandId}, ${evidence.messageId},
            ${evidence.providerDeliveryId}, ${evidence.createdAt}
          )
        `;
        yield* hooks?.afterAccepted?.() ?? Effect.void;
        yield* sql`
          INSERT INTO agent_control_initial_planning_deliveries (
            provider_delivery_id, handoff_id, handoff_fingerprint,
            controlled_thread_reservation_id, thread_id,
            turn_request_command_id, message_id, provider_instance_id,
            state, revision, claim_owner_id, claim_generation,
            claim_expires_at, attempt_count, next_attempt_at,
            planning_deadline_at, provider_turn_id, provider_accepted_at,
            terminal_at, last_error_code, interrupt_requested, updated_at
          ) VALUES (
            ${evidence.providerDeliveryId}, ${evidence.handoffId},
            ${evidence.handoffFingerprint}, ${evidence.controlledThreadReservationId},
            ${evidence.threadId}, ${evidence.turnRequestCommandId},
            ${evidence.messageId}, ${evidence.providerInstanceId},
            'pending', 0, NULL, 0, NULL, 0, NULL,
            ${evidence.planningDeadlineAt}, NULL, NULL, NULL, NULL, 0,
            ${evidence.createdAt}
          )
        `;
        yield* hooks?.afterDelivery?.() ?? Effect.void;
      }).pipe(Effect.mapError((cause) => storeError("insert-accepted", cause)));

  const loadAcceptedByHandoffId: AgentControlInitialPlanningHandoffStoreShape["loadAcceptedByHandoffId"] =
    (handoffId) =>
      selectAccepted("intent.handoff_id = ?", [handoffId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-handoff", cause)),
        Effect.flatMap(singleClaim),
      );

  const loadAcceptedByTurnRequestCommandId: AgentControlInitialPlanningHandoffStoreShape["loadAcceptedByTurnRequestCommandId"] =
    (commandId) =>
      selectAccepted("intent.turn_request_command_id = ?", [commandId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-turn-command", cause)),
        Effect.flatMap(singleClaim),
      );

  const loadAcceptedByThreadId: AgentControlInitialPlanningHandoffStoreShape["loadAcceptedByThreadId"] =
    (threadId) =>
      selectAccepted("intent.thread_id = ?", [threadId], 2).pipe(
        Effect.mapError((cause) => storeError("load-by-thread", cause)),
        Effect.flatMap(singleClaim),
      );

  const listRecoverable: AgentControlInitialPlanningHandoffStoreShape["listRecoverable"] = (
    now,
    limit = 100,
  ) =>
    selectAccepted(
      `(
        delivery.state = 'pending'
        OR delivery.state = 'turn-accepted'
        OR (delivery.state = 'retry-wait' AND delivery.next_attempt_at <= ?)
        OR (delivery.state = 'claimed' AND delivery.claim_expires_at <= ?)
        OR (delivery.state = 'delivery-attempted' AND delivery.claim_expires_at <= ?)
        OR delivery.state = 'provider-started'
        OR delivery.state = 'interrupt-requested'
      )`,
      [now, now, now],
      limit,
    ).pipe(
      Effect.mapError((cause) => storeError("list-recoverable", cause)),
      Effect.flatMap((rows) => Effect.forEach(rows, claimFromRow, { concurrency: 1 })),
    );

  const isHandoffOwnedTurnRequest: AgentControlInitialPlanningHandoffStoreShape["isHandoffOwnedTurnRequest"] =
    (commandId) =>
      sql<{ readonly count: number }>`
        SELECT count(*) AS count
        FROM agent_control_initial_planning_handoff_accepted
        WHERE turn_request_command_id = ${commandId}
      `.pipe(
        Effect.mapError((cause) => storeError("is-handoff-owned", cause)),
        Effect.map((rows) => rows[0]?.count === 1),
      );

  const loadTurnAcceptance: AgentControlInitialPlanningHandoffStoreShape["loadTurnAcceptance"] = (
    handoffId,
  ) =>
    sql<Record<string, unknown>>`
        SELECT
          handoff_id AS "handoffId",
          handoff_fingerprint AS "handoffFingerprint",
          controlled_thread_reservation_id AS "controlledThreadReservationId",
          thread_id AS "threadId",
          turn_request_command_id AS "turnRequestCommandId",
          message_id AS "messageId",
          message_event_id AS "messageEventId",
          message_event_sequence AS "messageEventSequence",
          turn_request_event_id AS "turnRequestEventId",
          turn_request_event_sequence AS "turnRequestEventSequence",
          accepted_at AS "acceptedAt"
        FROM agent_control_initial_planning_turn_accepted
        WHERE handoff_id = ${handoffId}
      `.pipe(
      Effect.mapError((cause) => storeError("load-turn-acceptance", cause)),
      Effect.flatMap((rows) => {
        if (rows.length === 0) return Effect.succeed(Option.none());
        if (rows.length !== 1) return Effect.fail(storeError("non-unique-turn-acceptance"));
        return decodeTurnAcceptanceRow(rows[0]).pipe(
          Effect.map((row) => Option.some(row satisfies AgentControlInitialPlanningTurnAcceptance)),
          Effect.mapError((cause) => storeError("decode-turn-acceptance", cause)),
        );
      }),
    );

  const updateOne = Effect.fn("AgentControlInitialPlanningHandoffStore.updateOne")(function* (
    operation: string,
    rows: ReadonlyArray<Record<string, unknown>>,
  ) {
    if (rows.length !== 1) return yield* storeError(`${operation}-cas-conflict`);
    return yield* deliveryFromRow(rows[0]);
  });

  const deliveryReturning = `
    provider_delivery_id AS "providerDeliveryId",
    handoff_id AS "handoffId",
    handoff_fingerprint AS "handoffFingerprint",
    controlled_thread_reservation_id AS "controlledThreadReservationId",
    thread_id AS "threadId",
    turn_request_command_id AS "turnRequestCommandId",
    message_id AS "messageId",
    provider_instance_id AS "providerInstanceId",
    state, revision, claim_owner_id AS "claimOwnerId",
    claim_generation AS "claimGeneration",
    claim_expires_at AS "claimExpiresAt",
    attempt_count AS "attemptCount",
    next_attempt_at AS "nextAttemptAt",
    planning_deadline_at AS "planningDeadlineAt",
    provider_turn_id AS "providerTurnId",
    provider_accepted_at AS "providerAcceptedAt",
    terminal_at AS "terminalAt",
    last_error_code AS "lastErrorCode",
    interrupt_requested AS "interruptRequested",
    updated_at AS "updatedAt"
  `;

  const markTurnAccepted: AgentControlInitialPlanningHandoffStoreShape["markTurnAccepted"] = (
    handoffId,
    expectedRevision,
    at,
  ) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'turn-accepted', revision = revision + 1, updated_at = ?
       WHERE handoff_id = ? AND revision = ? AND state = 'pending'
         AND EXISTS (
           SELECT 1 FROM agent_control_initial_planning_turn_accepted accepted
           WHERE accepted.handoff_id = agent_control_initial_planning_deliveries.handoff_id
         )
       RETURNING ${deliveryReturning}`,
        [at, handoffId, expectedRevision],
      )
      .pipe(
        Effect.mapError((cause) => storeError("mark-turn-accepted", cause)),
        Effect.flatMap((rows) => updateOne("mark-turn-accepted", rows)),
      );

  const claim: AgentControlInitialPlanningHandoffStoreShape["claim"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'claimed',
           revision = revision + 1,
           claim_owner_id = ?,
           claim_generation = claim_generation + 1,
           claim_expires_at = ?,
           attempt_count = attempt_count + 1,
           next_attempt_at = NULL,
           updated_at = ?
       WHERE handoff_id = ?
         AND (
           state = 'turn-accepted'
           OR (state = 'retry-wait' AND next_attempt_at <= ?)
           OR (state = 'claimed' AND claim_expires_at <= ?)
         )
         AND planning_deadline_at > ?
       RETURNING handoff_id`,
        [
          input.ownerId,
          input.expiresAt,
          input.now,
          input.handoffId,
          input.now,
          input.now,
          input.now,
        ],
      )
      .pipe(
        Effect.mapError((cause) => storeError("claim", cause)),
        Effect.flatMap((rows) =>
          rows.length === 0
            ? Effect.succeed(Option.none())
            : loadAcceptedByHandoffId(input.handoffId),
        ),
      );

  const markDeliveryAttempted: AgentControlInitialPlanningHandoffStoreShape["markDeliveryAttempted"] =
    (input) =>
      sql
        .unsafe<Record<string, unknown>>(
          `UPDATE agent_control_initial_planning_deliveries
         SET state = 'delivery-attempted', revision = revision + 1,
             updated_at = ?
         WHERE handoff_id = ? AND revision = ? AND state = 'claimed'
           AND claim_owner_id = ? AND claim_generation = ?
         RETURNING ${deliveryReturning}`,
          [
            input.attemptedAt,
            input.handoffId,
            input.expectedRevision,
            input.ownerId,
            input.claimGeneration,
          ],
        )
        .pipe(
          Effect.mapError((cause) => storeError("mark-delivery-attempted", cause)),
          Effect.flatMap((rows) => updateOne("mark-delivery-attempted", rows)),
        );

  const markProviderStarted: AgentControlInitialPlanningHandoffStoreShape["markProviderStarted"] = (
    input,
  ) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'provider-started',
           revision = revision + 1,
           claim_owner_id = NULL,
           claim_expires_at = NULL,
           provider_turn_id = ?,
           provider_accepted_at = ?,
           last_error_code = NULL,
           updated_at = ?
       WHERE handoff_id = ? AND revision = ? AND state = 'delivery-attempted'
         AND claim_owner_id = ? AND claim_generation = ?
       RETURNING ${deliveryReturning}`,
        [
          input.providerTurnId,
          input.acceptedAt,
          input.acceptedAt,
          input.handoffId,
          input.expectedRevision,
          input.ownerId,
          input.claimGeneration,
        ],
      )
      .pipe(
        Effect.mapError((cause) => storeError("mark-provider-started", cause)),
        Effect.flatMap((rows) => updateOne("mark-provider-started", rows)),
      );

  const scheduleRetry: AgentControlInitialPlanningHandoffStoreShape["scheduleRetry"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'retry-wait',
           revision = revision + 1,
           claim_owner_id = NULL,
           claim_expires_at = NULL,
           next_attempt_at = ?,
           last_error_code = ?,
           updated_at = ?
       WHERE handoff_id = ? AND revision = ?
         AND state IN ('claimed', 'delivery-attempted')
         AND claim_owner_id = ? AND claim_generation = ?
       RETURNING ${deliveryReturning}`,
        [
          input.nextAttemptAt,
          input.errorCode,
          input.updatedAt,
          input.handoffId,
          input.expectedRevision,
          input.ownerId,
          input.claimGeneration,
        ],
      )
      .pipe(
        Effect.mapError((cause) => storeError("schedule-retry", cause)),
        Effect.flatMap((rows) => updateOne("schedule-retry", rows)),
      );

  const markAmbiguous: AgentControlInitialPlanningHandoffStoreShape["markAmbiguous"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'ambiguous', revision = revision + 1,
           claim_owner_id = NULL, claim_expires_at = NULL,
           next_attempt_at = NULL, terminal_at = ?,
           last_error_code = 'provider-acceptance-ambiguous', updated_at = ?
       WHERE handoff_id = ? AND revision = ?
         AND state IN ('delivery-attempted', 'provider-started', 'interrupt-requested')
       RETURNING ${deliveryReturning}`,
        [input.terminalAt, input.terminalAt, input.handoffId, input.expectedRevision],
      )
      .pipe(
        Effect.mapError((cause) => storeError("mark-ambiguous", cause)),
        Effect.flatMap((rows) => updateOne("mark-ambiguous", rows)),
      );

  const markTerminal: AgentControlInitialPlanningHandoffStoreShape["markTerminal"] = (input) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = ?, revision = revision + 1,
           claim_owner_id = NULL, claim_expires_at = NULL,
           next_attempt_at = NULL, terminal_at = ?, last_error_code = ?,
           updated_at = ?
       WHERE handoff_id = ? AND revision = ?
         AND state IN (
           'pending', 'turn-accepted', 'retry-wait', 'claimed',
           'delivery-attempted', 'provider-started', 'interrupt-requested'
         )
       RETURNING ${deliveryReturning}`,
        [
          input.state,
          input.terminalAt,
          input.errorCode ?? null,
          input.terminalAt,
          input.handoffId,
          input.expectedRevision,
        ],
      )
      .pipe(
        Effect.mapError((cause) => storeError("mark-terminal", cause)),
        Effect.flatMap((rows) => updateOne("mark-terminal", rows)),
      );

  const observeProviderStarted: AgentControlInitialPlanningHandoffStoreShape["observeProviderStarted"] =
    (input) =>
      sql
        .unsafe<Record<string, unknown>>(
          `UPDATE agent_control_initial_planning_deliveries
         SET state = 'provider-started', revision = revision + 1,
             claim_owner_id = NULL, claim_expires_at = NULL,
             provider_turn_id = ?, provider_accepted_at = ?,
             last_error_code = NULL, updated_at = ?
         WHERE thread_id = ? AND state = 'delivery-attempted'
         RETURNING ${deliveryReturning}`,
          [input.providerTurnId, input.acceptedAt, input.acceptedAt, input.threadId],
        )
        .pipe(
          Effect.mapError((cause) => storeError("observe-provider-started", cause)),
          Effect.flatMap((rows) =>
            rows.length === 0
              ? Effect.succeed(Option.none())
              : updateOne("observe-provider-started", rows).pipe(Effect.map(Option.some)),
          ),
        );

  const requestInterrupt: AgentControlInitialPlanningHandoffStoreShape["requestInterrupt"] = (
    input,
  ) =>
    sql
      .unsafe<Record<string, unknown>>(
        `UPDATE agent_control_initial_planning_deliveries
       SET state = 'interrupt-requested', interrupt_requested = 1,
           revision = revision + 1, updated_at = ?
       WHERE handoff_id = ? AND revision = ? AND state = 'provider-started'
         AND interrupt_requested = 0
       RETURNING ${deliveryReturning}`,
        [input.requestedAt, input.handoffId, input.expectedRevision],
      )
      .pipe(
        Effect.mapError((cause) => storeError("request-interrupt", cause)),
        Effect.flatMap((rows) => updateOne("request-interrupt", rows)),
      );

  const observeProviderTerminal: AgentControlInitialPlanningHandoffStoreShape["observeProviderTerminal"] =
    (input) =>
      sql
        .unsafe<Record<string, unknown>>(
          `UPDATE agent_control_initial_planning_deliveries
         SET state = ?, revision = revision + 1, terminal_at = ?,
             last_error_code = ?, updated_at = ?
         WHERE thread_id = ? AND provider_turn_id = ?
           AND state IN ('provider-started', 'interrupt-requested', 'ambiguous')
         RETURNING ${deliveryReturning}`,
          [
            input.state,
            input.terminalAt,
            input.errorCode ?? null,
            input.terminalAt,
            input.threadId,
            input.providerTurnId,
          ],
        )
        .pipe(
          Effect.mapError((cause) => storeError("observe-provider-terminal", cause)),
          Effect.flatMap((rows) =>
            rows.length === 0
              ? Effect.succeed(Option.none())
              : updateOne("observe-provider-terminal", rows).pipe(Effect.map(Option.some)),
          ),
        );

  const listExpired: AgentControlInitialPlanningHandoffStoreShape["listExpired"] = (now) =>
    selectAccepted(
      `delivery.state IN (
        'pending', 'turn-accepted', 'claimed', 'delivery-attempted',
        'provider-started', 'interrupt-requested', 'retry-wait'
      ) AND delivery.planning_deadline_at <= ?`,
      [now],
    ).pipe(
      Effect.mapError((cause) => storeError("list-expired", cause)),
      Effect.flatMap((rows) => Effect.forEach(rows, claimFromRow, { concurrency: 1 })),
    );

  return AgentControlInitialPlanningHandoffStore.of({
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
    markTerminal,
    observeProviderStarted,
    observeProviderTerminal,
    requestInterrupt,
    listExpired,
  });
});

export const AgentControlInitialPlanningHandoffStoreLive = Layer.effect(
  AgentControlInitialPlanningHandoffStore,
  make,
);
