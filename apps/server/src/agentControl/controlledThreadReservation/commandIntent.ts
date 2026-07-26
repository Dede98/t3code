import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationCommand,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeReservationId,
  CommandId,
  ProjectId,
  ThreadId,
  type AgentControlControlledThreadReservationPrepareInitialInput,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const StoredIntent = Schema.Struct({
  commandId: CommandId,
  requestFingerprint: Schema.String,
  intentFingerprint: Schema.String,
  commandType: Schema.Literals([
    "agentControl.controlledThreadReservation.prepareInitial",
    "agentControl.controlledThreadReservation.prepare",
    "agentControl.controlledThreadReservation.transition",
  ]),
  authority: Schema.Literal("controller"),
  aggregateKind: Schema.Literal("controlled-thread-reservation"),
  aggregateId: AgentControlControlledThreadReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  controlledThreadReservationId: Schema.NullOr(AgentControlControlledThreadReservationId),
  threadId: Schema.NullOr(ThreadId),
  taskRevision: Schema.NullOr(Schema.Number),
  githubIntakeSequence: Schema.NullOr(Schema.Number),
  sourceIdentityFingerprint: Schema.NullOr(Schema.String),
  stageRunId: Schema.NullOr(AgentControlStageRunId),
  attemptId: Schema.NullOr(AgentControlAttemptId),
  roleId: Schema.NullOr(AgentControlRoleId),
  stageKind: Schema.NullOr(Schema.String),
  stageOrdinal: Schema.NullOr(Schema.Number),
  attemptOrdinal: Schema.NullOr(Schema.Number),
  leaseId: Schema.NullOr(AgentControlStageRunLeaseId),
  fenceToken: Schema.NullOr(Schema.Number),
  worktreeReservationId: Schema.NullOr(AgentControlWorktreeReservationId),
  expectedRevision: Schema.NullOr(Schema.Number),
  targetStatus: Schema.NullOr(Schema.String),
});
export type StoredControlledThreadCommandIntent = typeof StoredIntent.Type;
const decodeStoredIntent = Schema.decodeUnknownEffect(StoredIntent);
const encodeCommand = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlControlledThreadReservationCommand),
);

export const fingerprintControlledThreadCommandIntent = Effect.fn(
  "fingerprintControlledThreadCommandIntent",
)(function* (crypto: Crypto.Crypto, command: AgentControlControlledThreadReservationCommand) {
  const canonical = yield* encodeCommand(command);
  const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
  return Encoding.encodeHex(digest);
});

export const initialControlledThreadCommandIntent = (
  input: AgentControlControlledThreadReservationPrepareInitialInput,
  requestFingerprint: string,
  aggregateId: AgentControlControlledThreadReservationId,
): StoredControlledThreadCommandIntent => ({
  commandId: input.commandId,
  requestFingerprint,
  intentFingerprint: requestFingerprint,
  commandType: "agentControl.controlledThreadReservation.prepareInitial",
  authority: "controller",
  aggregateKind: "controlled-thread-reservation",
  aggregateId,
  projectId: input.projectId,
  taskId: input.taskId,
  controlledThreadReservationId: null,
  threadId: null,
  taskRevision: null,
  githubIntakeSequence: null,
  sourceIdentityFingerprint: null,
  stageRunId: null,
  attemptId: null,
  roleId: null,
  stageKind: null,
  stageOrdinal: null,
  attemptOrdinal: null,
  leaseId: null,
  fenceToken: null,
  worktreeReservationId: null,
  expectedRevision: null,
  targetStatus: null,
});

export const internalControlledThreadCommandIntent = Effect.fn(
  "internalControlledThreadCommandIntent",
)(function* (
  crypto: Crypto.Crypto,
  command: AgentControlControlledThreadReservationCommand,
  requestFingerprint: string,
) {
  return {
    commandId: command.commandId,
    requestFingerprint,
    intentFingerprint: yield* fingerprintControlledThreadCommandIntent(crypto, command),
    commandType: command.type,
    authority: "controller",
    aggregateKind: "controlled-thread-reservation",
    aggregateId: command.controlledThreadReservationId,
    projectId: command.projectId,
    taskId: command.taskId,
    controlledThreadReservationId: command.controlledThreadReservationId,
    threadId: command.threadId,
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
    expectedRevision: command.expectedRevision,
    targetStatus:
      command.type === "agentControl.controlledThreadReservation.transition"
        ? command.targetStatus
        : null,
  } satisfies StoredControlledThreadCommandIntent;
});

export const sameControlledThreadCommandIntent = (
  stored: StoredControlledThreadCommandIntent,
  expected: StoredControlledThreadCommandIntent,
) =>
  stored.commandId === expected.commandId &&
  stored.requestFingerprint === expected.requestFingerprint &&
  stored.intentFingerprint === expected.intentFingerprint &&
  stored.commandType === expected.commandType &&
  stored.authority === expected.authority &&
  stored.aggregateKind === expected.aggregateKind &&
  stored.aggregateId === expected.aggregateId &&
  stored.projectId === expected.projectId &&
  stored.taskId === expected.taskId &&
  stored.controlledThreadReservationId === expected.controlledThreadReservationId &&
  stored.threadId === expected.threadId &&
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
  stored.expectedRevision === expected.expectedRevision &&
  stored.targetStatus === expected.targetStatus;

export const insertControlledThreadCommandIntent = Effect.fn("insertControlledThreadCommandIntent")(
  function* (sql: SqlClient.SqlClient, intent: StoredControlledThreadCommandIntent) {
    yield* sql`
    INSERT INTO agent_control_controlled_thread_command_intents (
      command_id, request_fingerprint, intent_fingerprint, command_type,
      authority, aggregate_kind, aggregate_id, project_id, task_id,
      controlled_thread_reservation_id, thread_id, task_revision,
      github_intake_sequence, source_identity_fingerprint, stage_run_id,
      attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
      lease_id, fence_token, worktree_reservation_id, expected_revision,
      target_status
    ) VALUES (
      ${intent.commandId}, ${intent.requestFingerprint}, ${intent.intentFingerprint},
      ${intent.commandType}, ${intent.authority}, ${intent.aggregateKind},
      ${intent.aggregateId}, ${intent.projectId}, ${intent.taskId},
      ${intent.controlledThreadReservationId}, ${intent.threadId},
      ${intent.taskRevision}, ${intent.githubIntakeSequence},
      ${intent.sourceIdentityFingerprint}, ${intent.stageRunId}, ${intent.attemptId},
      ${intent.roleId}, ${intent.stageKind}, ${intent.stageOrdinal},
      ${intent.attemptOrdinal}, ${intent.leaseId}, ${intent.fenceToken},
      ${intent.worktreeReservationId}, ${intent.expectedRevision}, ${intent.targetStatus}
    )
  `;
  },
);

export const loadControlledThreadCommandIntent = Effect.fn("loadControlledThreadCommandIntent")(
  function* (sql: SqlClient.SqlClient, commandId: CommandId) {
    const rows = yield* sql<Record<string, unknown>>`
    SELECT
      command_id AS "commandId", request_fingerprint AS "requestFingerprint",
      intent_fingerprint AS "intentFingerprint", command_type AS "commandType",
      authority, aggregate_kind AS "aggregateKind", aggregate_id AS "aggregateId",
      project_id AS "projectId", task_id AS "taskId",
      controlled_thread_reservation_id AS "controlledThreadReservationId",
      thread_id AS "threadId", task_revision AS "taskRevision",
      github_intake_sequence AS "githubIntakeSequence",
      source_identity_fingerprint AS "sourceIdentityFingerprint",
      stage_run_id AS "stageRunId", attempt_id AS "attemptId",
      role_id AS "roleId", stage_kind AS "stageKind",
      stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
      lease_id AS "leaseId", fence_token AS "fenceToken",
      worktree_reservation_id AS "worktreeReservationId",
      expected_revision AS "expectedRevision", target_status AS "targetStatus"
    FROM agent_control_controlled_thread_command_intents
    WHERE command_id = ${commandId}
  `;
    return rows[0] === undefined
      ? Option.none<StoredControlledThreadCommandIntent>()
      : Option.some(yield* decodeStoredIntent(rows[0]));
  },
);
