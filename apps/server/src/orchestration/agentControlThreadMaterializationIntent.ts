import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlStageKind,
  AgentControlTaskId,
  AgentControlThreadBinding,
  AgentControlThreadMaterializeCommand,
  AgentControlWorktreeReservationId,
  CommandId,
  EventId,
  IsoDateTime,
  ModelSelection,
  OrchestrationCommandReceiptStatus,
  ProviderInteractionMode,
  ProjectId,
  ThreadId,
  RuntimeMode,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Equal from "effect/Equal";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

const StoredIntent = Schema.Struct({
  commandId: CommandId,
  commandType: Schema.Literal("thread.agent-control.materialize"),
  authority: Schema.Literal("agent-control"),
  aggregateKind: Schema.Literal("thread"),
  commandFingerprint: Schema.String,
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: ThreadId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  roleId: AgentControlRoleId,
  stageKind: AgentControlStageKind,
  stageOrdinal: Schema.Number,
  attemptOrdinal: Schema.Number,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: Schema.Number,
  worktreeReservationId: AgentControlWorktreeReservationId,
  title: Schema.String,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  branch: Schema.String,
  worktreePath: Schema.String,
  binding: AgentControlThreadBinding,
  createdEventId: Schema.NullOr(EventId),
  createdEventType: Schema.NullOr(Schema.Literal("thread.created")),
  createdEventSequence: Schema.NullOr(Schema.Number),
  createdEventStreamVersion: Schema.NullOr(Schema.Number),
  bindingEventId: Schema.NullOr(EventId),
  bindingEventType: Schema.NullOr(Schema.Literal("thread.agent-control-bound")),
  bindingEventSequence: Schema.NullOr(Schema.Number),
  bindingEventStreamVersion: Schema.NullOr(Schema.Number),
  acceptedReceiptCommandId: Schema.NullOr(CommandId),
  receiptStatus: OrchestrationCommandReceiptStatus,
  receiptResultSequence: Schema.Number,
  receiptAcceptedAt: IsoDateTime,
  receiptError: Schema.NullOr(Schema.String),
  createdAt: IsoDateTime,
});
export type StoredAgentControlThreadMaterializationIntent = typeof StoredIntent.Type;

const AcceptedReceiptEvidence = Schema.Struct({
  commandId: CommandId,
  commandType: Schema.Literal("thread.agent-control.materialize"),
  authority: Schema.Literal("agent-control"),
  aggregateKind: Schema.Literal("thread"),
  threadId: ThreadId,
  commandFingerprint: Schema.String,
  resultSequence: Schema.Number,
  acceptedAt: IsoDateTime,
  status: Schema.Literal("accepted"),
});
export type StoredAgentControlThreadMaterializationAcceptedReceiptEvidence =
  typeof AcceptedReceiptEvidence.Type;

const StoredIntentRow = Schema.Struct({
  ...StoredIntent.fields,
  modelSelection: Schema.fromJsonString(ModelSelection),
  binding: Schema.fromJsonString(AgentControlThreadBinding),
  modelSelectionCanonical: Schema.Literal(1),
  bindingCanonical: Schema.Literal(1),
});
const decodeStoredIntent = Schema.decodeUnknownEffect(StoredIntentRow);
const decodeAcceptedReceiptEvidence = Schema.decodeUnknownEffect(AcceptedReceiptEvidence);
const encodeCommand = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlThreadMaterializeCommand),
);
const encodeModelSelection = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeBinding = Schema.encodeUnknownEffect(Schema.fromJsonString(AgentControlThreadBinding));

export const fingerprintAgentControlThreadMaterializationCommand = Effect.fn(
  "fingerprintAgentControlThreadMaterializationCommand",
)(function* (crypto: Crypto.Crypto, command: AgentControlThreadMaterializeCommand) {
  const canonical = yield* encodeCommand(command);
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
  return Encoding.encodeHex(digest);
});

interface IntentReceiptCoordinates {
  readonly status: OrchestrationCommandReceiptStatus;
  readonly resultSequence: number;
  readonly acceptedAt: string;
  readonly error: string | null;
}

interface IntentEventCoordinates {
  readonly createdEventId: EventId;
  readonly createdEventSequence: number;
  readonly bindingEventId: EventId;
  readonly bindingEventSequence: number;
}

const intentBase = (command: AgentControlThreadMaterializeCommand, commandFingerprint: string) => ({
  commandId: command.commandId,
  commandType: command.type,
  authority: "agent-control" as const,
  aggregateKind: "thread" as const,
  commandFingerprint,
  controlledThreadReservationId: command.controlledThreadReservationId,
  threadId: command.threadId,
  projectId: command.projectId,
  taskId: command.taskId,
  taskRevision: command.taskRevision,
  githubIntakeSequence: command.githubIntakeSequence,
  sourceIdentityFingerprint: command.sourceIdentityFingerprint,
  stageRunId: command.stageRunId,
  attemptId: command.attemptId,
  roleId: command.roleId,
  stageKind: command.stageKind,
  stageOrdinal: command.stageOrdinal,
  attemptOrdinal: command.attemptOrdinal,
  leaseId: command.leaseId,
  fenceToken: command.fenceToken,
  worktreeReservationId: command.worktreeReservationId,
  title: command.title,
  modelSelection: command.modelSelection,
  runtimeMode: command.runtimeMode,
  interactionMode: command.interactionMode,
  branch: command.branch,
  worktreePath: command.worktreePath,
  binding: command.binding,
  createdAt: command.createdAt,
});

export const acceptedAgentControlThreadMaterializationIntent = (
  command: AgentControlThreadMaterializeCommand,
  commandFingerprint: string,
  events: IntentEventCoordinates,
): StoredAgentControlThreadMaterializationIntent => ({
  ...intentBase(command, commandFingerprint),
  createdEventId: events.createdEventId,
  createdEventType: "thread.created",
  createdEventSequence: events.createdEventSequence,
  createdEventStreamVersion: 1,
  bindingEventId: events.bindingEventId,
  bindingEventType: "thread.agent-control-bound",
  bindingEventSequence: events.bindingEventSequence,
  bindingEventStreamVersion: 2,
  acceptedReceiptCommandId: command.commandId,
  receiptStatus: "accepted",
  receiptResultSequence: events.bindingEventSequence,
  receiptAcceptedAt: command.createdAt,
  receiptError: null,
});

export const rejectedAgentControlThreadMaterializationIntent = (
  command: AgentControlThreadMaterializeCommand,
  commandFingerprint: string,
  receipt: IntentReceiptCoordinates,
): StoredAgentControlThreadMaterializationIntent => ({
  ...intentBase(command, commandFingerprint),
  createdEventId: null,
  createdEventType: null,
  createdEventSequence: null,
  createdEventStreamVersion: null,
  bindingEventId: null,
  bindingEventType: null,
  bindingEventSequence: null,
  bindingEventStreamVersion: null,
  acceptedReceiptCommandId: null,
  receiptStatus: receipt.status,
  receiptResultSequence: receipt.resultSequence,
  receiptAcceptedAt: receipt.acceptedAt,
  receiptError: receipt.error,
});

export const sameAgentControlThreadMaterializationCommandIntent = (
  stored: StoredAgentControlThreadMaterializationIntent,
  command: AgentControlThreadMaterializeCommand,
  commandFingerprint: string,
): boolean => {
  const expected = intentBase(command, commandFingerprint);
  return (
    stored.commandId === expected.commandId &&
    stored.commandType === expected.commandType &&
    stored.authority === expected.authority &&
    stored.aggregateKind === expected.aggregateKind &&
    stored.commandFingerprint === expected.commandFingerprint &&
    stored.controlledThreadReservationId === expected.controlledThreadReservationId &&
    stored.threadId === expected.threadId &&
    stored.projectId === expected.projectId &&
    stored.taskId === expected.taskId &&
    stored.taskRevision === expected.taskRevision &&
    stored.githubIntakeSequence === expected.githubIntakeSequence &&
    stored.sourceIdentityFingerprint === expected.sourceIdentityFingerprint &&
    stored.stageRunId === expected.stageRunId &&
    stored.attemptId === expected.attemptId &&
    stored.roleId === expected.roleId &&
    stored.stageKind === expected.stageKind &&
    stored.stageOrdinal === expected.stageOrdinal &&
    stored.attemptOrdinal === expected.attemptOrdinal &&
    stored.leaseId === expected.leaseId &&
    stored.fenceToken === expected.fenceToken &&
    stored.worktreeReservationId === expected.worktreeReservationId &&
    stored.title === expected.title &&
    Equal.equals(stored.modelSelection, expected.modelSelection) &&
    stored.runtimeMode === expected.runtimeMode &&
    stored.interactionMode === expected.interactionMode &&
    stored.branch === expected.branch &&
    stored.worktreePath === expected.worktreePath &&
    Equal.equals(stored.binding, expected.binding) &&
    stored.createdAt === expected.createdAt
  );
};

export const commandFromAgentControlThreadMaterializationIntent = (
  intent: StoredAgentControlThreadMaterializationIntent,
): AgentControlThreadMaterializeCommand => ({
  type: intent.commandType,
  commandId: intent.commandId,
  controlledThreadReservationId: intent.controlledThreadReservationId,
  threadId: intent.threadId,
  projectId: intent.projectId,
  taskId: intent.taskId,
  taskRevision: intent.taskRevision,
  githubIntakeSequence: intent.githubIntakeSequence,
  sourceIdentityFingerprint: intent.sourceIdentityFingerprint,
  stageRunId: intent.stageRunId,
  attemptId: intent.attemptId,
  roleId: intent.roleId,
  stageKind: intent.stageKind,
  stageOrdinal: intent.stageOrdinal,
  attemptOrdinal: intent.attemptOrdinal,
  leaseId: intent.leaseId,
  fenceToken: intent.fenceToken,
  worktreeReservationId: intent.worktreeReservationId,
  title: intent.title,
  modelSelection: intent.modelSelection,
  runtimeMode: intent.runtimeMode,
  interactionMode: intent.interactionMode,
  branch: intent.branch,
  worktreePath: intent.worktreePath,
  binding: intent.binding,
  createdAt: intent.createdAt,
});

export const insertAgentControlThreadMaterializationIntent = Effect.fn(
  "insertAgentControlThreadMaterializationIntent",
)(function* (sql: SqlClient.SqlClient, intent: StoredAgentControlThreadMaterializationIntent) {
  const modelSelectionJson = yield* encodeModelSelection(intent.modelSelection);
  const bindingJson = yield* encodeBinding(intent.binding);
  yield* sql`
    INSERT INTO orchestration_agent_control_thread_materialization_intents (
      command_id, command_type, authority, aggregate_kind, command_fingerprint,
      controlled_thread_reservation_id, thread_id, project_id, task_id,
      task_revision, github_intake_sequence, source_identity_fingerprint,
      stage_run_id, attempt_id, role_id, stage_kind, stage_ordinal,
      attempt_ordinal, lease_id, fence_token, worktree_reservation_id,
      title, model_selection_json, runtime_mode, interaction_mode, branch,
      worktree_path, binding_json, created_event_id, created_event_type,
      created_event_sequence, created_event_stream_version, binding_event_id,
      binding_event_type, binding_event_sequence, binding_event_stream_version,
      accepted_receipt_command_id,
      receipt_status, receipt_result_sequence, receipt_accepted_at,
      receipt_error, created_at
    ) VALUES (
      ${intent.commandId}, ${intent.commandType}, ${intent.authority},
      ${intent.aggregateKind}, ${intent.commandFingerprint},
      ${intent.controlledThreadReservationId}, ${intent.threadId},
      ${intent.projectId}, ${intent.taskId}, ${intent.taskRevision},
      ${intent.githubIntakeSequence}, ${intent.sourceIdentityFingerprint},
      ${intent.stageRunId}, ${intent.attemptId}, ${intent.roleId},
      ${intent.stageKind}, ${intent.stageOrdinal}, ${intent.attemptOrdinal},
      ${intent.leaseId}, ${intent.fenceToken}, ${intent.worktreeReservationId},
      ${intent.title}, ${modelSelectionJson},
      ${intent.runtimeMode}, ${intent.interactionMode}, ${intent.branch},
      ${intent.worktreePath}, ${bindingJson},
      ${intent.createdEventId}, ${intent.createdEventType},
      ${intent.createdEventSequence}, ${intent.createdEventStreamVersion},
      ${intent.bindingEventId}, ${intent.bindingEventType},
      ${intent.bindingEventSequence}, ${intent.bindingEventStreamVersion},
      ${intent.acceptedReceiptCommandId},
      ${intent.receiptStatus}, ${intent.receiptResultSequence},
      ${intent.receiptAcceptedAt}, ${intent.receiptError}, ${intent.createdAt}
    )
  `;
});

export const loadAgentControlThreadMaterializationIntent = Effect.fn(
  "loadAgentControlThreadMaterializationIntent",
)(function* (sql: SqlClient.SqlClient, commandId: CommandId) {
  const rows = yield* sql<Record<string, unknown>>`
    SELECT
      command_id AS "commandId", command_type AS "commandType",
      authority, aggregate_kind AS "aggregateKind",
      command_fingerprint AS "commandFingerprint",
      controlled_thread_reservation_id AS "controlledThreadReservationId",
      thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
      task_revision AS "taskRevision",
      github_intake_sequence AS "githubIntakeSequence",
      source_identity_fingerprint AS "sourceIdentityFingerprint",
      stage_run_id AS "stageRunId", attempt_id AS "attemptId",
      role_id AS "roleId", stage_kind AS "stageKind",
      stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
      lease_id AS "leaseId", fence_token AS "fenceToken",
      worktree_reservation_id AS "worktreeReservationId",
      title, model_selection_json AS "modelSelection",
      runtime_mode AS "runtimeMode", interaction_mode AS "interactionMode",
      branch, worktree_path AS "worktreePath", binding_json AS "binding",
      created_event_id AS "createdEventId",
      created_event_type AS "createdEventType",
      created_event_sequence AS "createdEventSequence",
      created_event_stream_version AS "createdEventStreamVersion",
      binding_event_id AS "bindingEventId",
      binding_event_type AS "bindingEventType",
      binding_event_sequence AS "bindingEventSequence",
      binding_event_stream_version AS "bindingEventStreamVersion",
      accepted_receipt_command_id AS "acceptedReceiptCommandId",
      receipt_status AS "receiptStatus",
      receipt_result_sequence AS "receiptResultSequence",
      receipt_accepted_at AS "receiptAcceptedAt",
      receipt_error AS "receiptError", created_at AS "createdAt",
      CASE WHEN
        json_valid(model_selection_json) = 1
        AND json_type(model_selection_json) = 'object'
        AND (SELECT count(*) FROM json_each(model_selection_json)) IN (2, 3)
        AND (SELECT count(*) FROM json_each(model_selection_json)) = (
          SELECT count(DISTINCT key) FROM json_each(model_selection_json)
        )
        AND NOT EXISTS (
          SELECT 1 FROM json_each(model_selection_json)
          WHERE key NOT IN ('instanceId', 'model', 'options')
        )
        AND (
          SELECT count(*) FROM json_each(model_selection_json)
          WHERE key = 'instanceId'
        ) = 1
        AND (
          SELECT count(*) FROM json_each(model_selection_json)
          WHERE key = 'model'
        ) = 1
        AND (
          (
            SELECT count(*) FROM json_each(model_selection_json)
            WHERE key = 'options'
          ) = 0
          OR (
            (
              SELECT count(*) FROM json_each(model_selection_json)
              WHERE key = 'options'
            ) = 1
            AND json_type(model_selection_json, '$.options') = 'array'
            AND NOT EXISTS (
              SELECT 1 FROM json_each(model_selection_json, '$.options') option
              WHERE json_type(option.value) <> 'object'
                OR (SELECT count(*) FROM json_each(option.value)) <> 2
                OR (SELECT count(DISTINCT key) FROM json_each(option.value)) <> 2
                OR EXISTS (
                  SELECT 1 FROM json_each(option.value)
                  WHERE key NOT IN ('id', 'value')
                )
                OR json_type(option.value, '$.id') <> 'text'
                OR length(trim(json_extract(option.value, '$.id'))) = 0
                OR json_type(option.value, '$.value')
                  NOT IN ('text', 'true', 'false')
                OR (
                  json_type(option.value, '$.value') = 'text'
                  AND length(trim(json_extract(option.value, '$.value'))) = 0
                )
            )
            AND NOT EXISTS (
              SELECT 1 FROM json_each(model_selection_json, '$.options') option
              GROUP BY json_extract(option.value, '$.id')
              HAVING count(*) <> 1
            )
          )
        )
        THEN 1 ELSE 0 END AS "modelSelectionCanonical",
      CASE WHEN
        json_valid(binding_json) = 1
        AND json_type(binding_json) = 'object'
        AND (SELECT count(*) FROM json_each(binding_json)) = 5
        AND (SELECT count(DISTINCT key) FROM json_each(binding_json)) = 5
        AND NOT EXISTS (
          SELECT 1 FROM json_each(binding_json)
          WHERE key NOT IN ('taskId', 'stageRunId', 'attemptId', 'roleId', 'controlState')
        )
        THEN 1 ELSE 0 END AS "bindingCanonical"
    FROM orchestration_agent_control_thread_materialization_intents
    WHERE command_id = ${commandId}
  `;
  return rows[0] === undefined
    ? Option.none<StoredAgentControlThreadMaterializationIntent>()
    : Option.some(yield* decodeStoredIntent(rows[0]));
});

export const insertAgentControlThreadMaterializationAcceptedReceiptEvidence = Effect.fn(
  "insertAgentControlThreadMaterializationAcceptedReceiptEvidence",
)(function* (sql: SqlClient.SqlClient, intent: StoredAgentControlThreadMaterializationIntent) {
  if (intent.receiptStatus !== "accepted" || intent.acceptedReceiptCommandId === null) {
    return;
  }
  yield* sql`
    INSERT INTO orchestration_agent_control_thread_materialization_receipts (
      command_id, command_type, authority, aggregate_kind, thread_id,
      command_fingerprint, result_sequence, accepted_at, status
    ) VALUES (
      ${intent.acceptedReceiptCommandId}, ${intent.commandType}, ${intent.authority},
      ${intent.aggregateKind}, ${intent.threadId}, ${intent.commandFingerprint},
      ${intent.receiptResultSequence}, ${intent.receiptAcceptedAt}, ${intent.receiptStatus}
    )
  `;
});

export const loadAgentControlThreadMaterializationAcceptedReceiptEvidence = Effect.fn(
  "loadAgentControlThreadMaterializationAcceptedReceiptEvidence",
)(function* (sql: SqlClient.SqlClient, commandId: CommandId) {
  const rows = yield* sql<Record<string, unknown>>`
    SELECT
      command_id AS "commandId", command_type AS "commandType",
      authority, aggregate_kind AS "aggregateKind", thread_id AS "threadId",
      command_fingerprint AS "commandFingerprint",
      result_sequence AS "resultSequence", accepted_at AS "acceptedAt", status
    FROM orchestration_agent_control_thread_materialization_receipts
    WHERE command_id = ${commandId}
  `;
  return rows[0] === undefined
    ? Option.none<StoredAgentControlThreadMaterializationAcceptedReceiptEvidence>()
    : Option.some(yield* decodeAcceptedReceiptEvidence(rows[0]));
});
