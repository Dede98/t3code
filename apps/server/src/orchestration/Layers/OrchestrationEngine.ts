import type {
  AgentControlThreadMaterializeCommand,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
} from "@t3tools/contracts";
import {
  CommandId,
  EventId,
  ModelSelection,
  OrchestrationCommand,
  OrchestrationEvent as OrchestrationEventSchema,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { isSqlError, type SqlError } from "effect/unstable/sql/SqlError";

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import {
  PersistenceDecodeError,
  PersistenceSqlError,
  toPersistenceSqlError,
} from "../../persistence/Errors.ts";
import {
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT,
  VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX,
} from "../../agentControl/verificationTurn/runtimeEventAuthority.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT } from "../../agentControl/verificationTurn/prompt.ts";
import { AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT } from "../../agentControl/verificationTurn/verificationResult.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import { NodeSqliteTransactionHooks } from "../../persistence/Services/NodeSqliteTransactionHooks.ts";
import {
  OrchestrationCommandAuthorityMismatchError,
  OrchestrationCommandIdentityConflictError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  type OrchestrationDispatchError,
  type OrchestrationProjectorDecodeError,
} from "../Errors.ts";
import type { OrchestrationCommandAuthority } from "../CommandAuthority.ts";
import { decideOrchestrationCommand, isAgentControlReservedThreadCreate } from "../decider.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import {
  acceptedAgentControlThreadMaterializationIntent,
  commandFromAgentControlThreadMaterializationIntent,
  fingerprintAgentControlThreadMaterializationCommand,
  insertAgentControlThreadMaterializationAcceptedReceiptEvidence,
  insertAgentControlThreadMaterializationIntent,
  loadAgentControlThreadMaterializationAcceptedReceiptEvidence,
  loadAgentControlThreadMaterializationIntent,
  rejectedAgentControlThreadMaterializationIntent,
  sameAgentControlThreadMaterializationCommandIntent,
  type StoredAgentControlThreadMaterializationIntent,
} from "../agentControlThreadMaterializationIntent.ts";
import { validateAgentControlThreadMaterializationCommandIdentity } from "../agentControlThreadMaterializationCommand.ts";
import {
  canonicalJson,
  canonicalInitialPlanningEventEnvelope,
  canonicalInitialPlanningEventEnvelopeFromStoredJson,
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJsonObjectBytes,
  parseJsonStrict,
  type JsonValue,
} from "../../agentControl/initialPlanning/eventEvidence.ts";
import { loadSealableVerificationResultSource } from "../../agentControl/verificationTurn/orchestrationResultSource.ts";
import {
  AgentControlThreadMaterializationConvergencePolicy,
  AgentControlThreadMaterializationTransactionHooks,
} from "../Services/AgentControlThreadMaterializationTransactionHooks.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEnginePublicationHooks } from "../Services/OrchestrationEnginePublicationHooks.ts";
import { VerificationResultRuntimeEventAuthorityHooks } from "../Services/VerificationResultRuntimeEventAuthorityHooks.ts";
import {
  OrchestrationEngineService,
  type AgentControlImplementationTurnDispatchEvidence,
  type AgentControlInitialPlanningTurnDispatchEvidence,
  type AgentControlVerificationTurnDispatchEvidence,
  type AgentControlThreadMaterializationTransactionResult,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isOrchestrationCommandInvariantError = Schema.is(OrchestrationCommandInvariantError);
const isOrchestrationCommandIdentityConflictError = Schema.is(
  OrchestrationCommandIdentityConflictError,
);
const isPersistenceSqlError = Schema.is(PersistenceSqlError);

class VerificationResultRuntimeEventAuthorityRaceError {
  readonly _tag = "VerificationResultRuntimeEventAuthorityRaceError";
  readonly originalError: PersistenceSqlError;

  constructor(originalError: PersistenceSqlError) {
    this.originalError = originalError;
  }
}

const isRetryableMaterializationSqliteConflict = (error: PersistenceSqlError): boolean => {
  const seen = new Set<unknown>();
  const visit = (cause: unknown): boolean => {
    if (cause === null || cause === undefined || seen.has(cause)) return false;
    seen.add(cause);
    if (isSqlError(cause) && cause.isRetryable) return true;
    if (typeof cause !== "object") return false;
    const record = cause as Record<string, unknown>;
    const numericCode = typeof record.errcode === "number" ? record.errcode : undefined;
    if (numericCode !== undefined && ((numericCode & 0xff) === 5 || (numericCode & 0xff) === 6)) {
      return true;
    }
    if (
      typeof record.code === "string" &&
      (record.code.startsWith("SQLITE_BUSY") || record.code.startsWith("SQLITE_LOCKED"))
    ) {
      return true;
    }
    return visit(record.cause) || visit(record.reason);
  };
  return visit(error.cause);
};

export type VerificationResultRuntimeEventAuthorityRaceSignal =
  | "authority-trigger"
  | "authority-index"
  | "busy"
  | "busy-snapshot";

export const classifyVerificationResultRuntimeEventAuthorityRace = (
  error: PersistenceSqlError,
): VerificationResultRuntimeEventAuthorityRaceSignal | null => {
  const seen = new Set<unknown>();
  const visit = (cause: unknown): VerificationResultRuntimeEventAuthorityRaceSignal | null => {
    if (cause === null || cause === undefined || seen.has(cause)) {
      return null;
    }
    seen.add(cause);
    if (typeof cause !== "object") {
      return null;
    }
    const record = cause as Record<string, unknown>;
    if (record._tag === "ConnectionError") return null;
    const message = typeof record.message === "string" ? record.message : undefined;
    const constraint = typeof record.constraint === "string" ? record.constraint : undefined;
    const namedUniqueConflict =
      message ===
        `UNIQUE constraint failed: index '${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}'` ||
      constraint === `index '${VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX}'` ||
      constraint === VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_INDEX;
    const explicitAuthorityConflict =
      message === VERIFICATION_RESULT_RUNTIME_EVENT_AUTHORITY_CONFLICT;
    if (record.errcode === 1811 && explicitAuthorityConflict) return "authority-trigger";
    if (record.errcode === 2067 && namedUniqueConflict) return "authority-index";
    if (record.errcode === 517) return "busy-snapshot";
    if (record.errcode === 5) return "busy";
    return visit(record.cause) ?? visit(record.reason);
  };
  return visit(error.cause);
};

const isVerificationResultRuntimeEventAuthorityRace = (error: PersistenceSqlError): boolean =>
  classifyVerificationResultRuntimeEventAuthorityRace(error) !== null;

const isAgentControlThreadMaterializeCommand = (
  command: OrchestrationCommand,
): command is AgentControlThreadMaterializeCommand =>
  command.type === "thread.agent-control.materialize";

const MaterializationEventRow = Schema.Struct({
  sequence: Schema.Number,
  streamVersion: Schema.Number,
  eventId: EventId,
  type: Schema.Literals(["thread.created", "thread.agent-control-bound"]),
  aggregateKind: Schema.Literal("thread"),
  aggregateId: Schema.String,
  occurredAt: Schema.String,
  commandId: Schema.String,
  causationEventId: Schema.NullOr(EventId),
  correlationId: Schema.String,
  payload: Schema.fromJsonString(Schema.Unknown),
  metadata: Schema.fromJsonString(Schema.Unknown),
  jsonCanonical: Schema.Literal(1),
});
type MaterializationEventRow = typeof MaterializationEventRow.Type;
const decodeMaterializationEventRow = Schema.decodeUnknownEffect(MaterializationEventRow);
const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEventSchema);
const decodeModelSelectionJson = Schema.decodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const encodeModelSelectionJson = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));
const InitialPlanningEventRow = Schema.Struct({
  sequence: Schema.Int,
  streamVersion: Schema.Int,
  eventId: EventId,
  aggregateKind: Schema.Literal("thread"),
  aggregateId: Schema.String,
  type: Schema.Literals(["thread.message-sent", "thread.turn-start-requested"]),
  occurredAt: Schema.String,
  commandId: Schema.String,
  causationEventId: Schema.NullOr(EventId),
  correlationId: Schema.String,
  actorKind: Schema.Literal("client"),
  payloadJson: Schema.String,
  metadataJson: Schema.String,
});
const decodeInitialPlanningEventRow = Schema.decodeUnknownEffect(InitialPlanningEventRow);

const evidenceError = (issue: string, threadId: ThreadId) =>
  new PersistenceDecodeError({
    operation: "OrchestrationEngine.materializationEvidence",
    issue,
    correlation: { threadId },
  });

interface CommandEnvelope {
  command: OrchestrationCommand;
  authority: OrchestrationCommandAuthority;
  initialPlanning?: AgentControlInitialPlanningTurnDispatchEvidence;
  implementation?: AgentControlImplementationTurnDispatchEvidence;
  verification?: AgentControlVerificationTurnDispatchEvidence;
  result: Deferred.Deferred<{ sequence: number }, OrchestrationDispatchError>;
  cancelled: Deferred.Deferred<void>;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const materializationTransactionHooks = yield* AgentControlThreadMaterializationTransactionHooks;
  const materializationConvergencePolicy =
    yield* AgentControlThreadMaterializationConvergencePolicy;
  const crypto = yield* Crypto.Crypto;
  const publicationHooks = yield* OrchestrationEnginePublicationHooks;
  const verificationResultRuntimeEventAuthorityHooks =
    yield* VerificationResultRuntimeEventAuthorityHooks;
  const nodeSqliteTransactionHooks = yield* NodeSqliteTransactionHooks;

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const validateVerificationResultCaptureReplay = Effect.fn(
    "OrchestrationEngine.validateVerificationResultCaptureReplay",
  )(function* (
    command: Extract<OrchestrationCommand, { readonly type: "thread.verification-result.capture" }>,
    resultSequence: number,
  ) {
    const rows = yield* sql<{
      readonly payloadJson: string;
      readonly metadataJson: string;
    }>`
      SELECT
        payload_json AS "payloadJson",
        metadata_json AS "metadataJson"
      FROM main.orchestration_events
      WHERE sequence = ${resultSequence}
        AND typeof(sequence) = 'integer'
        AND sequence >= 1
        AND typeof(event_id) = 'text'
        AND length(CAST(event_id AS BLOB)) > 0
        AND typeof(aggregate_kind) = 'text'
        AND CAST(aggregate_kind AS BLOB) = CAST('thread' AS BLOB)
        AND typeof(stream_id) = 'text'
        AND CAST(stream_id AS BLOB) = CAST(${command.threadId} AS BLOB)
        AND typeof(stream_version) = 'integer'
        AND stream_version >= 1
        AND typeof(event_type) = 'text'
        AND CAST(event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
        AND typeof(occurred_at) = 'text'
        AND CAST(occurred_at AS BLOB) = CAST(${command.createdAt} AS BLOB)
        AND typeof(command_id) = 'text'
        AND CAST(command_id AS BLOB) = CAST(${command.commandId} AS BLOB)
        AND causation_event_id IS NULL
        AND typeof(correlation_id) = 'text'
        AND CAST(correlation_id AS BLOB) = CAST(${command.commandId} AS BLOB)
        AND typeof(actor_kind) = 'text'
        AND CAST(actor_kind AS BLOB) = CAST('provider' AS BLOB)
        AND typeof(payload_json) = 'text'
        AND t3_fatal_utf8(CAST(payload_json AS BLOB)) = 1
        AND json_valid(payload_json, 1) = 1
        AND typeof(metadata_json) = 'text'
        AND t3_fatal_utf8(CAST(metadata_json AS BLOB)) = 1
        AND json_valid(metadata_json, 1) = 1
        AND (
          SELECT count(*)
          FROM main.orchestration_events command_event
          WHERE typeof(command_event.command_id) = 'text'
            AND CAST(command_event.command_id AS BLOB) = CAST(${command.commandId} AS BLOB)
        ) = 1
    `.pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationEngine.validateVerificationResultCaptureReplay"),
      ),
    );
    const expectedPayload = canonicalJson({
      threadId: command.threadId,
      messageId: command.messageId,
      turnId: command.turnId,
      fragment: command.fragment,
      createdAt: command.createdAt,
    });
    const expectedMetadata = canonicalJson({
      providerRuntimeMessage: command.providerRuntimeMessage,
      verificationResultCapture: command.verificationResultCapture,
    });
    const stored = rows[0];
    const storedJsonMatches =
      stored === undefined
        ? false
        : (() => {
            try {
              return (
                canonicalJson(parseJsonStrict(stored.payloadJson)) === expectedPayload &&
                canonicalJson(parseJsonStrict(stored.metadataJson)) === expectedMetadata
              );
            } catch {
              return false;
            }
          })();
    const matches = rows.length === 1 && stored !== undefined && storedJsonMatches;
    if (!matches) {
      return yield* new OrchestrationCommandIdentityConflictError({
        commandId: command.commandId,
        commandType: command.type,
      });
    }
    return resultSequence;
  });

  const loadVerificationResultCaptureRuntimeFragment = Effect.fn(
    "OrchestrationEngine.loadVerificationResultCaptureRuntimeFragment",
  )(function* (
    command: Extract<OrchestrationCommand, { readonly type: "thread.verification-result.capture" }>,
  ) {
    const rows = yield* sql<{ readonly sequence: number }>`
      SELECT sequence
      FROM main.orchestration_events
      WHERE typeof(sequence) = 'integer'
        AND sequence >= 1
        AND typeof(event_type) = 'text'
        AND CAST(event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
        AND json_extract(
          metadata_json, '$.providerRuntimeMessage.runtimeEventId'
        ) IS ${command.providerRuntimeMessage.runtimeEventId}
      ORDER BY sequence
      LIMIT 2
    `.pipe(
      Effect.mapError(
        toPersistenceSqlError("OrchestrationEngine.loadVerificationResultCaptureRuntimeFragment"),
      ),
    );
    if (rows.length > 1) {
      return yield* new OrchestrationCommandIdentityConflictError({
        commandId: command.commandId,
        commandType: command.type,
      });
    }
    return rows[0]?.sequence ?? null;
  });

  const loadCommittedVerificationResultCaptureRuntimeFragment = Effect.fn(
    "OrchestrationEngine.loadCommittedVerificationResultCaptureRuntimeFragment",
  )(function* (
    command: Extract<OrchestrationCommand, { readonly type: "thread.verification-result.capture" }>,
    authority: OrchestrationCommandAuthority,
  ) {
    const rows = yield* sql<{ readonly sequence: number }>`
      SELECT capture.sequence
      FROM main.orchestration_events capture
      JOIN main.orchestration_command_receipts receipt
        ON typeof(capture.command_id) = 'text'
       AND CAST(receipt.command_id AS BLOB) = CAST(capture.command_id AS BLOB)
      JOIN main.agent_control_verification_deliveries delivery
        ON typeof(delivery.provider_delivery_id) = 'text'
       AND CAST(delivery.provider_delivery_id AS BLOB) = CAST(json_extract(
         capture.metadata_json, '$.verificationResultCapture.providerDeliveryId'
       ) AS BLOB)
      JOIN main.agent_control_verification_handoff_intents intent
        ON typeof(intent.handoff_id) = 'text'
       AND CAST(intent.handoff_id AS BLOB) = CAST(delivery.handoff_id AS BLOB)
      WHERE typeof(capture.sequence) = 'integer'
        AND capture.sequence >= 1
        AND typeof(capture.event_type) = 'text'
        AND CAST(capture.event_type AS BLOB) =
          CAST('thread.verification-result-fragment-captured' AS BLOB)
        AND CAST(json_extract(
          capture.metadata_json, '$.providerRuntimeMessage.runtimeEventId'
        ) AS BLOB) = CAST(${command.providerRuntimeMessage.runtimeEventId} AS BLOB)
        AND typeof(receipt.authority) = 'text'
        AND CAST(receipt.authority AS BLOB) = CAST(${authority} AS BLOB)
        AND typeof(receipt.aggregate_kind) = 'text'
        AND CAST(receipt.aggregate_kind AS BLOB) = CAST('thread' AS BLOB)
        AND typeof(receipt.aggregate_id) = 'text'
        AND CAST(receipt.aggregate_id AS BLOB) = CAST(capture.stream_id AS BLOB)
        AND typeof(receipt.accepted_at) = 'text'
        AND CAST(receipt.accepted_at AS BLOB) = CAST(capture.occurred_at AS BLOB)
        AND typeof(receipt.result_sequence) = 'integer'
        AND receipt.result_sequence = capture.sequence
        AND typeof(receipt.status) = 'text'
        AND CAST(receipt.status AS BLOB) = CAST('accepted' AS BLOB)
        AND receipt.error IS NULL
        AND typeof(delivery.attempt_id) = 'text'
        AND length(CAST(delivery.attempt_id AS BLOB)) > 0
        AND CAST(delivery.handoff_id AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.verificationResultCapture.handoffId'
        ) AS BLOB)
        AND typeof(delivery.thread_id) = 'text'
        AND CAST(delivery.thread_id AS BLOB) = CAST(capture.stream_id AS BLOB)
        AND typeof(delivery.provider_instance_id) = 'text'
        AND CAST(delivery.provider_instance_id AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.providerRuntimeMessage.providerInstanceId'
        ) AS BLOB)
        AND CAST(delivery.provider_instance_id AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.verificationResultCapture.providerInstanceId'
        ) AS BLOB)
        AND typeof(delivery.provider_turn_id) = 'text'
        AND CAST(delivery.provider_turn_id AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.providerRuntimeMessage.providerTurnId'
        ) AS BLOB)
        AND CAST(delivery.provider_turn_id AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.verificationResultCapture.providerTurnId'
        ) AS BLOB)
        AND typeof(delivery.state) = 'text'
        AND delivery.state IN ('provider-started', 'completed')
        AND typeof(intent.attempt_id) = 'text'
        AND CAST(intent.attempt_id AS BLOB) = CAST(delivery.attempt_id AS BLOB)
        AND typeof(intent.prompt_template_version) = 'text'
        AND CAST(intent.prompt_template_version AS BLOB) =
          CAST('agent-control-verification-prompt-v2' AS BLOB)
        AND typeof(intent.prompt_contract_fingerprint) = 'text'
        AND CAST(intent.prompt_contract_fingerprint AS BLOB) =
          CAST(${AGENT_CONTROL_VERIFICATION_PROMPT_CONTRACT_FINGERPRINT} AS BLOB)
        AND typeof(intent.result_schema_version) = 'text'
        AND CAST(intent.result_schema_version AS BLOB) =
          CAST('agent-control-verification-result-v1' AS BLOB)
        AND typeof(intent.result_schema_fingerprint) = 'text'
        AND CAST(intent.result_schema_fingerprint AS BLOB) =
          CAST(${AGENT_CONTROL_VERIFICATION_RESULT_SCHEMA_FINGERPRINT} AS BLOB)
        AND CAST(intent.result_schema_fingerprint AS BLOB) = CAST(json_extract(
          capture.metadata_json, '$.verificationResultCapture.resultSchemaFingerprint'
        ) AS BLOB)
        AND (
          SELECT count(*)
          FROM main.orchestration_events command_event
          WHERE typeof(command_event.command_id) = 'text'
            AND CAST(command_event.command_id AS BLOB) = CAST(capture.command_id AS BLOB)
        ) = 1
      ORDER BY capture.sequence
      LIMIT 2
    `.pipe(
      Effect.mapError(
        toPersistenceSqlError(
          "OrchestrationEngine.loadCommittedVerificationResultCaptureRuntimeFragment",
        ),
      ),
    );
    if (rows.length !== 1) {
      return null;
    }
    return yield* validateVerificationResultCaptureReplay(command, rows[0]!.sequence);
  });

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();
  const publishDomainEvent = (event: OrchestrationEvent) =>
    publicationHooks
      .onPublish({ source: publicationHooks.source, event })
      .pipe(Effect.andThen(PubSub.publish(eventPubSub, event)));

  const initialPlanningError = (detail: string): OrchestrationCommandInvariantError =>
    new OrchestrationCommandInvariantError({
      commandType: "thread.turn.start",
      detail,
    });

  const validateInitialPlanningTurnCommand = Effect.fn(
    "OrchestrationEngine.validateInitialPlanningTurnCommand",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlInitialPlanningTurnDispatchEvidence,
  ) {
    if (
      command.type !== "thread.turn.start" ||
      command.commandId !== evidence.turnRequestCommandId ||
      command.threadId !== evidence.threadId ||
      command.message.messageId !== evidence.messageId ||
      command.message.role !== "user" ||
      command.message.attachments.length !== 0 ||
      command.bootstrap !== undefined ||
      command.sourceProposedPlan !== undefined ||
      command.titleSeed !== undefined
    ) {
      return yield* initialPlanningError(
        "Initial Planning dispatch command identity or shape is invalid.",
      );
    }
    const rows = yield* sql<{
      readonly promptText: string;
      readonly createdAt: string;
      readonly runtimeMode: string;
      readonly modelSelectionJson: string;
      readonly interactionMode: string;
      readonly controlState: string | null;
    }>`
      SELECT
        intent.prompt_text AS "promptText",
        intent.created_at AS "createdAt",
        intent.runtime_mode AS "runtimeMode",
        intent.model_selection_json AS "modelSelectionJson",
        thread.interaction_mode AS "interactionMode",
        json_extract(thread.agent_control_json, '$.controlState') AS "controlState"
      FROM agent_control_initial_planning_handoff_intents intent
      JOIN agent_control_initial_planning_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
      JOIN agent_control_initial_planning_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_controlled_thread_reservation_states reservation
        ON reservation.controlled_thread_reservation_id =
          intent.controlled_thread_reservation_id
       AND reservation.thread_id = intent.thread_id
       AND reservation.status = 'bound'
       AND reservation.revision = 3
      JOIN projection_threads thread
        ON thread.thread_id = intent.thread_id
       AND thread.project_id = intent.project_id
       AND thread.worktree_path = intent.worktree_path
       AND thread.runtime_mode = intent.runtime_mode
       AND json(thread.model_selection_json) = json(intent.model_selection_json)
      WHERE intent.handoff_id = ${evidence.handoffId}
        AND intent.handoff_fingerprint = ${evidence.handoffFingerprint}
        AND intent.controlled_thread_reservation_id =
          ${evidence.controlledThreadReservationId}
        AND intent.thread_id = ${evidence.threadId}
        AND intent.turn_request_command_id = ${evidence.turnRequestCommandId}
        AND intent.message_id = ${evidence.messageId}
    `;
    if (rows.length !== 1) {
      return yield* initialPlanningError(
        "Initial Planning dispatch requires one complete accepted handoff.",
      );
    }
    const row = rows[0]!;
    const modelSelection = yield* decodeModelSelectionJson(row.modelSelectionJson).pipe(
      Effect.mapError(() =>
        initialPlanningError("Initial Planning handoff model selection is noncanonical."),
      ),
    );
    if (
      row.promptText !== command.message.text ||
      row.createdAt !== command.createdAt ||
      row.runtimeMode !== command.runtimeMode ||
      row.interactionMode !== "plan" ||
      command.interactionMode !== "plan" ||
      row.controlState !== "controlled" ||
      !Equal.equals(modelSelection, command.modelSelection)
    ) {
      return yield* initialPlanningError(
        "Initial Planning dispatch conflicts with frozen handoff authority.",
      );
    }
  });

  const validateInitialPlanningTurnReplay = Effect.fn(
    "OrchestrationEngine.validateInitialPlanningTurnReplay",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlInitialPlanningTurnDispatchEvidence,
  ) {
    if (command.type !== "thread.turn.start") {
      return yield* initialPlanningError("Initial Planning replay command type is invalid.");
    }
    yield* validateInitialPlanningTurnCommand(command, evidence);
    const rawEvents = yield* sql<Record<string, unknown>>`
      SELECT
        sequence, stream_version AS "streamVersion", event_id AS "eventId",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        event_type AS type, occurred_at AS "occurredAt", command_id AS "commandId",
        causation_event_id AS "causationEventId", correlation_id AS "correlationId",
        actor_kind AS "actorKind", payload_json AS "payloadJson",
        metadata_json AS "metadataJson",
        CAST(payload_json AS BLOB) AS "payloadBytes",
        CAST(metadata_json AS BLOB) AS "metadataBytes"
      FROM main.orchestration_events
      WHERE command_id = ${command.commandId}
      ORDER BY sequence
    `;
    const eventRows = yield* Effect.forEach(rawEvents, (row) =>
      Effect.gen(function* () {
        const type = row.type;
        const payloadKeys =
          type === "thread.message-sent"
            ? [
                "attachments",
                "createdAt",
                "messageId",
                "role",
                "streaming",
                "text",
                "threadId",
                "turnId",
                "updatedAt",
              ]
            : type === "thread.turn-start-requested"
              ? [
                  "createdAt",
                  "interactionMode",
                  "messageId",
                  "modelSelection",
                  "runtimeMode",
                  "threadId",
                ]
              : [];
        if (
          payloadKeys.length === 0 ||
          typeof row.payloadJson !== "string" ||
          typeof row.metadataJson !== "string"
        ) {
          return yield* initialPlanningError(
            "Initial Planning replay event rows are not canonical.",
          );
        }
        const parsed = yield* Effect.try({
          try: () => {
            const payload = parseCanonicalJsonObjectBytes(row.payloadBytes, payloadKeys);
            const metadata = parseCanonicalJsonObjectBytes(row.metadataBytes, []);
            if (payload.source !== row.payloadJson || metadata.source !== row.metadataJson) {
              throw new Error("SQLite TEXT and BLOB views disagree");
            }
            return {
              payload: payload.value,
              metadata: metadata.value,
              payloadJson: payload.source,
              metadataJson: metadata.source,
            };
          },
          catch: () =>
            initialPlanningError(
              "Initial Planning replay raw JSON is noncanonical, invalid, or has unexpected keys.",
            ),
        });
        const decoded = yield* decodeInitialPlanningEventRow(row).pipe(
          Effect.mapError(() =>
            initialPlanningError("Initial Planning replay event rows are not canonical."),
          ),
        );
        return { ...decoded, ...parsed };
      }),
    );
    if (eventRows.length !== 2) {
      return yield* initialPlanningError(
        "Initial Planning turn replay requires exactly two canonical events.",
      );
    }
    const messageRow = eventRows[0]!;
    const turnRow = eventRows[1]!;
    const messageEnvelopeJson = canonicalInitialPlanningEventEnvelopeFromStoredJson({
      ...messageRow,
      eventId: messageRow.eventId,
      aggregateId: ThreadId.make(messageRow.aggregateId),
      commandId: CommandId.make(messageRow.commandId),
      correlationId: CommandId.make(messageRow.correlationId),
      causationEventId: messageRow.causationEventId,
      payloadJson: messageRow.payloadJson,
      metadataJson: messageRow.metadataJson,
    });
    const turnEnvelopeJson = canonicalInitialPlanningEventEnvelopeFromStoredJson({
      ...turnRow,
      eventId: turnRow.eventId,
      aggregateId: ThreadId.make(turnRow.aggregateId),
      commandId: CommandId.make(turnRow.commandId),
      correlationId: CommandId.make(turnRow.correlationId),
      causationEventId: turnRow.causationEventId,
      payloadJson: turnRow.payloadJson,
      metadataJson: turnRow.metadataJson,
    });
    const messageTemplateJson = canonicalInitialPlanningEventTemplate({
      ...messageRow,
      eventId: messageRow.eventId,
      aggregateId: ThreadId.make(messageRow.aggregateId),
      commandId: CommandId.make(messageRow.commandId),
      correlationId: CommandId.make(messageRow.correlationId),
      causationEventId: messageRow.causationEventId,
      payload: messageRow.payload,
      metadata: messageRow.metadata,
    });
    const turnTemplateJson = canonicalInitialPlanningEventTemplate({
      ...turnRow,
      eventId: turnRow.eventId,
      aggregateId: ThreadId.make(turnRow.aggregateId),
      commandId: CommandId.make(turnRow.commandId),
      correlationId: CommandId.make(turnRow.correlationId),
      causationEventId: turnRow.causationEventId,
      payload: turnRow.payload,
      metadata: turnRow.metadata,
    });
    const evidenceDigest = combinedInitialPlanningEventDigest(
      messageEnvelopeJson,
      turnEnvelopeJson,
    );
    if (
      messageRow.eventId !== evidence.messageEventId ||
      turnRow.eventId !== evidence.turnRequestEventId ||
      messageRow.streamVersion !== 3 ||
      turnRow.streamVersion !== 4 ||
      messageTemplateJson !== evidence.messageEventTemplateJson ||
      turnTemplateJson !== evidence.turnRequestEventTemplateJson ||
      combinedInitialPlanningEventDigest(messageTemplateJson, turnTemplateJson) !==
        evidence.eventTemplateDigest ||
      turnRow.sequence !== messageRow.sequence + 1 ||
      turnRow.causationEventId !== messageRow.eventId
    ) {
      return yield* initialPlanningError(
        "Initial Planning replay event evidence conflicts with the frozen command.",
      );
    }
    const commandModelSelectionJson = yield* encodeModelSelectionJson(command.modelSelection).pipe(
      Effect.mapError(() =>
        initialPlanningError("Initial Planning replay model selection is invalid."),
      ),
    );
    const rows = yield* sql<{
      readonly sequence: number;
      readonly valid: number;
      readonly messageEventEnvelopeJson: string;
      readonly turnRequestEventEnvelopeJson: string;
      readonly eventEvidenceDigest: string;
    }>`
      SELECT
        turn_accepted.turn_request_event_sequence AS sequence,
        turn_accepted.message_event_envelope_json AS "messageEventEnvelopeJson",
        turn_accepted.turn_request_event_envelope_json AS "turnRequestEventEnvelopeJson",
        turn_accepted.event_evidence_digest AS "eventEvidenceDigest",
        CASE WHEN
          turn_accepted.handoff_id = ${evidence.handoffId}
          AND turn_accepted.handoff_fingerprint = ${evidence.handoffFingerprint}
          AND turn_accepted.controlled_thread_reservation_id =
            ${evidence.controlledThreadReservationId}
          AND turn_accepted.thread_id = ${evidence.threadId}
          AND turn_accepted.turn_request_command_id =
            ${evidence.turnRequestCommandId}
          AND turn_accepted.message_id = ${evidence.messageId}
          AND intent.prompt_text = ${command.message.text}
          AND intent.created_at = ${command.createdAt}
          AND intent.runtime_mode = ${command.runtimeMode}
          AND json(intent.model_selection_json) =
            json(${commandModelSelectionJson})
          AND message_event.event_type = 'thread.message-sent'
          AND message_event.command_id = ${command.commandId}
          AND message_event.stream_id = ${command.threadId}
          AND json_extract(message_event.payload_json, '$.messageId') =
            ${command.message.messageId}
          AND json_extract(message_event.payload_json, '$.text') =
            ${command.message.text}
          AND turn_event.event_type = 'thread.turn-start-requested'
          AND turn_event.command_id = ${command.commandId}
          AND turn_event.stream_id = ${command.threadId}
          AND json_extract(turn_event.payload_json, '$.messageId') =
            ${command.message.messageId}
          AND command_receipt.authority = 'agent-control'
          AND command_receipt.aggregate_kind = 'thread'
          AND command_receipt.aggregate_id = ${command.threadId}
          AND command_receipt.accepted_at = ${command.createdAt}
          AND command_receipt.status = 'accepted'
          AND command_receipt.error IS NULL
          AND command_receipt.result_sequence =
            turn_accepted.turn_request_event_sequence
          AND message.message_id = ${command.message.messageId}
          AND message.thread_id = ${command.threadId}
          AND message.role = 'user'
          AND message.text = ${command.message.text}
          AND message.attachments_json = '[]'
          AND message.turn_id IS NULL
          AND message.is_streaming = 0
          AND message.created_at = ${command.createdAt}
          AND message.updated_at = ${command.createdAt}
          AND pending.thread_id = ${command.threadId}
          AND pending.pending_message_id = ${command.message.messageId}
          AND pending.requested_at = ${command.createdAt}
          AND (
            (pending.turn_id IS NULL AND pending.state = 'pending')
            OR
            (pending.turn_id IS NOT NULL
              AND pending.state IN ('running', 'completed', 'interrupted', 'error'))
          )
          AND (
            SELECT count(*) FROM main.orchestration_events candidate
            WHERE candidate.command_id = ${command.commandId}
          ) = 2
        THEN 1 ELSE 0 END AS valid
      FROM agent_control_initial_planning_turn_accepted turn_accepted
      JOIN agent_control_initial_planning_handoff_intents intent
        ON intent.handoff_id = turn_accepted.handoff_id
      JOIN main.orchestration_events message_event
        ON message_event.event_id = turn_accepted.message_event_id
       AND message_event.sequence = turn_accepted.message_event_sequence
      JOIN main.orchestration_events turn_event
        ON turn_event.event_id = turn_accepted.turn_request_event_id
       AND turn_event.sequence = turn_accepted.turn_request_event_sequence
      JOIN main.orchestration_command_receipts command_receipt
        ON command_receipt.command_id = turn_accepted.turn_request_command_id
      JOIN projection_thread_messages message
        ON message.message_id = turn_accepted.message_id
      JOIN projection_turns pending
        ON pending.thread_id = turn_accepted.thread_id
       AND pending.pending_message_id = turn_accepted.message_id
      WHERE turn_accepted.handoff_id = ${evidence.handoffId}
         OR turn_accepted.turn_request_command_id = ${command.commandId}
         OR turn_accepted.thread_id = ${command.threadId}
    `;
    if (rows.length !== 1 || rows[0]?.valid !== 1) {
      return yield* initialPlanningError(
        "Initial Planning turn replay evidence is incomplete or inconsistent.",
      );
    }
    if (
      rows[0]!.sequence !== turnRow.sequence ||
      rows[0]!.messageEventEnvelopeJson !== messageEnvelopeJson ||
      rows[0]!.turnRequestEventEnvelopeJson !== turnEnvelopeJson ||
      rows[0]!.eventEvidenceDigest !== evidenceDigest ||
      messageRow.eventId === turnRow.eventId ||
      messageRow.streamVersion < 1
    ) {
      return yield* initialPlanningError(
        "Initial Planning turn replay ordering or acceptance evidence is inconsistent.",
      );
    }
    return turnRow.sequence;
  });

  const implementationError = (detail: string): OrchestrationCommandInvariantError =>
    new OrchestrationCommandInvariantError({
      commandType: "thread.turn.start",
      detail,
    });

  const validateImplementationTurnCommand = Effect.fn(
    "OrchestrationEngine.validateImplementationTurnCommand",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlImplementationTurnDispatchEvidence,
  ) {
    if (
      command.type !== "thread.turn.start" ||
      command.commandId !== evidence.turnRequestCommandId ||
      command.threadId !== evidence.threadId ||
      command.message.messageId !== evidence.messageId ||
      command.message.role !== "user" ||
      command.message.attachments.length !== 0 ||
      command.bootstrap !== undefined ||
      command.titleSeed !== undefined ||
      command.interactionMode !== "default" ||
      command.sourceProposedPlan?.threadId !== evidence.planningThreadId ||
      command.sourceProposedPlan.planId !== evidence.planId
    ) {
      return yield* implementationError(
        "Implementation dispatch command identity, source plan, or shape is invalid.",
      );
    }
    const rows = yield* sql<{
      readonly promptText: string;
      readonly createdAt: string;
      readonly runtimeMode: string;
      readonly modelSelectionJson: string;
      readonly interactionMode: string;
      readonly planningThreadId: string;
      readonly planId: string;
      readonly controlState: string | null;
    }>`
      SELECT intent.prompt_text AS "promptText", intent.created_at AS "createdAt",
        intent.runtime_mode AS "runtimeMode", intent.model_selection_json AS "modelSelectionJson",
        thread.interaction_mode AS "interactionMode",
        intent.planning_thread_id AS "planningThreadId", intent.plan_id AS "planId",
        json_extract(thread.agent_control_json, '$.controlState') AS "controlState"
      FROM agent_control_implementation_handoff_intents intent
      JOIN agent_control_implementation_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
      JOIN agent_control_implementation_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_implementation_materialization_markers marker
        ON marker.handoff_id = intent.handoff_id
      JOIN agent_control_implementation_thread_reservation_states reservation
        ON reservation.controlled_thread_reservation_id = intent.controlled_thread_reservation_id
       AND reservation.thread_id = intent.thread_id
       AND reservation.status = 'bound' AND reservation.revision = 3
      JOIN projection_threads thread
        ON thread.thread_id = intent.thread_id AND thread.project_id = intent.project_id
       AND thread.worktree_path = (
         SELECT materialization.worktree_path
         FROM agent_control_implementation_materialization_evidence materialization
         WHERE materialization.materialization_evidence_id = intent.materialization_evidence_id
       )
       AND thread.runtime_mode = intent.runtime_mode
       AND json(thread.model_selection_json) = json(intent.model_selection_json)
      WHERE intent.handoff_id = ${evidence.handoffId}
        AND intent.handoff_fingerprint = ${evidence.handoffFingerprint}
        AND intent.controlled_thread_reservation_id = ${evidence.controlledThreadReservationId}
        AND intent.thread_id = ${evidence.threadId}
        AND intent.turn_request_command_id = ${evidence.turnRequestCommandId}
        AND intent.message_id = ${evidence.messageId}
        AND intent.planning_thread_id = ${evidence.planningThreadId}
        AND intent.plan_id = ${evidence.planId}
    `;
    if (rows.length !== 1) {
      return yield* implementationError(
        "Implementation dispatch requires one complete accepted handoff.",
      );
    }
    const row = rows[0]!;
    const modelSelection = yield* decodeModelSelectionJson(row.modelSelectionJson).pipe(
      Effect.mapError(() =>
        implementationError("Implementation handoff model selection is noncanonical."),
      ),
    );
    if (
      row.promptText !== command.message.text ||
      row.createdAt !== command.createdAt ||
      row.runtimeMode !== command.runtimeMode ||
      row.interactionMode !== "default" ||
      row.planningThreadId !== evidence.planningThreadId ||
      row.planId !== evidence.planId ||
      row.controlState !== "controlled" ||
      !Equal.equals(modelSelection, command.modelSelection)
    ) {
      return yield* implementationError(
        "Implementation dispatch conflicts with frozen handoff authority.",
      );
    }
  });

  const validateImplementationTurnReplay = Effect.fn(
    "OrchestrationEngine.validateImplementationTurnReplay",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlImplementationTurnDispatchEvidence,
  ) {
    if (command.type !== "thread.turn.start") {
      return yield* implementationError("Implementation replay command type is invalid.");
    }
    yield* validateImplementationTurnCommand(command, evidence);
    const rawEvents = yield* sql<Record<string, unknown>>`
      SELECT sequence, stream_version AS "streamVersion", event_id AS "eventId",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId", event_type AS type,
        occurred_at AS "occurredAt", command_id AS "commandId",
        causation_event_id AS "causationEventId", correlation_id AS "correlationId",
        actor_kind AS "actorKind", payload_json AS "payloadJson", metadata_json AS "metadataJson",
        CAST(payload_json AS BLOB) AS "payloadBytes",
        CAST(metadata_json AS BLOB) AS "metadataBytes"
      FROM main.orchestration_events WHERE command_id = ${command.commandId} ORDER BY sequence
    `;
    const eventRows = yield* Effect.forEach(rawEvents, (raw) =>
      Effect.gen(function* () {
        const payloadKeys =
          raw.type === "thread.message-sent"
            ? [
                "attachments",
                "createdAt",
                "messageId",
                "role",
                "streaming",
                "text",
                "threadId",
                "turnId",
                "updatedAt",
              ]
            : raw.type === "thread.turn-start-requested"
              ? [
                  "createdAt",
                  "interactionMode",
                  "messageId",
                  "modelSelection",
                  "runtimeMode",
                  "sourceProposedPlan",
                  "threadId",
                ]
              : [];
        if (
          payloadKeys.length === 0 ||
          typeof raw.payloadJson !== "string" ||
          typeof raw.metadataJson !== "string"
        ) {
          return yield* implementationError("Implementation replay rows are not canonical.");
        }
        const parsed = yield* Effect.try({
          try: () => {
            const payload = parseCanonicalJsonObjectBytes(raw.payloadBytes, payloadKeys);
            const metadata = parseCanonicalJsonObjectBytes(raw.metadataBytes, []);
            if (payload.source !== raw.payloadJson || metadata.source !== raw.metadataJson) {
              throw new Error("SQLite TEXT and BLOB views disagree");
            }
            return {
              payload: payload.value,
              metadata: metadata.value,
              payloadJson: payload.source,
              metadataJson: metadata.source,
            };
          },
          catch: () =>
            implementationError(
              "Implementation replay raw JSON is noncanonical, invalid, or has unexpected keys.",
            ),
        });
        const decoded = yield* decodeInitialPlanningEventRow(raw).pipe(
          Effect.mapError(() =>
            implementationError("Implementation replay rows are not canonical."),
          ),
        );
        return { ...decoded, ...parsed };
      }),
    );
    if (eventRows.length !== 2) {
      return yield* implementationError(
        "Implementation turn replay requires exactly two canonical events.",
      );
    }
    const messageRow = eventRows[0]!;
    const turnRow = eventRows[1]!;
    const envelopeFromRow = (row: typeof messageRow) =>
      canonicalInitialPlanningEventEnvelopeFromStoredJson({
        ...row,
        aggregateId: ThreadId.make(row.aggregateId),
        commandId: CommandId.make(row.commandId),
        correlationId: CommandId.make(row.correlationId),
        payloadJson: row.payloadJson,
        metadataJson: row.metadataJson,
      });
    const templateFromRow = (row: typeof messageRow) =>
      canonicalInitialPlanningEventTemplate({
        ...row,
        aggregateId: ThreadId.make(row.aggregateId),
        commandId: CommandId.make(row.commandId),
        correlationId: CommandId.make(row.correlationId),
        payload: row.payload,
        metadata: row.metadata,
      });
    const messageEnvelopeJson = envelopeFromRow(messageRow);
    const turnEnvelopeJson = envelopeFromRow(turnRow);
    const messageTemplateJson = templateFromRow(messageRow);
    const turnTemplateJson = templateFromRow(turnRow);
    const eventEvidenceDigest = combinedInitialPlanningEventDigest(
      messageEnvelopeJson,
      turnEnvelopeJson,
    );
    if (
      messageRow.eventId !== evidence.messageEventId ||
      turnRow.eventId !== evidence.turnRequestEventId ||
      messageRow.streamVersion !== 3 ||
      turnRow.streamVersion !== 4 ||
      messageTemplateJson !== evidence.messageEventTemplateJson ||
      turnTemplateJson !== evidence.turnRequestEventTemplateJson ||
      combinedInitialPlanningEventDigest(messageTemplateJson, turnTemplateJson) !==
        evidence.eventTemplateDigest ||
      turnRow.sequence !== messageRow.sequence + 1 ||
      turnRow.causationEventId !== messageRow.eventId
    ) {
      return yield* implementationError(
        "Implementation replay events conflict with frozen evidence.",
      );
    }
    const rows = yield* sql<{
      readonly sequence: number;
      readonly messageEventEnvelopeJson: string;
      readonly turnRequestEventEnvelopeJson: string;
      readonly eventEvidenceDigest: string;
      readonly valid: number;
    }>`
      SELECT accepted.turn_request_event_sequence AS sequence,
        accepted.message_event_envelope_json AS "messageEventEnvelopeJson",
        accepted.turn_request_event_envelope_json AS "turnRequestEventEnvelopeJson",
        accepted.event_evidence_digest AS "eventEvidenceDigest",
        CASE WHEN accepted.handoff_fingerprint = ${evidence.handoffFingerprint}
          AND accepted.controlled_thread_reservation_id = ${evidence.controlledThreadReservationId}
          AND accepted.thread_id = ${evidence.threadId}
          AND accepted.planning_thread_id = ${evidence.planningThreadId}
          AND accepted.plan_id = ${evidence.planId}
          AND accepted.turn_request_command_id = ${command.commandId}
          AND accepted.message_id = ${command.message.messageId}
          AND receipt.authority = 'agent-control' AND receipt.status = 'accepted'
          AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = ${command.threadId}
          AND receipt.result_sequence = accepted.turn_request_event_sequence
          AND message.event_type = 'thread.message-sent'
          AND turn_event.event_type = 'thread.turn-start-requested'
          AND projected.message_id = ${command.message.messageId}
          AND projected.thread_id = ${command.threadId}
          AND projected.text = ${command.message.text}
          AND projected.attachments_json = '[]'
          AND pending.thread_id = ${command.threadId}
          AND pending.pending_message_id = ${command.message.messageId}
          AND (SELECT count(*) FROM main.orchestration_events event
            WHERE event.command_id = ${command.commandId}) = 2
        THEN 1 ELSE 0 END AS valid
      FROM agent_control_implementation_turn_accepted accepted
      JOIN main.orchestration_command_receipts receipt
        ON receipt.command_id = accepted.turn_request_command_id
      JOIN main.orchestration_events message ON message.event_id = accepted.message_event_id
       AND message.sequence = accepted.message_event_sequence
      JOIN main.orchestration_events turn_event ON turn_event.event_id = accepted.turn_request_event_id
       AND turn_event.sequence = accepted.turn_request_event_sequence
      JOIN projection_thread_messages projected ON projected.message_id = accepted.message_id
      JOIN projection_turns pending ON pending.thread_id = accepted.thread_id
       AND pending.pending_message_id = accepted.message_id
      WHERE accepted.handoff_id = ${evidence.handoffId}
         OR accepted.turn_request_command_id = ${command.commandId}
         OR accepted.thread_id = ${command.threadId}
    `;
    if (
      rows.length !== 1 ||
      rows[0]?.valid !== 1 ||
      rows[0].sequence !== turnRow.sequence ||
      rows[0].messageEventEnvelopeJson !== messageEnvelopeJson ||
      rows[0].turnRequestEventEnvelopeJson !== turnEnvelopeJson ||
      rows[0].eventEvidenceDigest !== eventEvidenceDigest
    ) {
      return yield* implementationError(
        "Implementation replay acceptance evidence is incomplete or inconsistent.",
      );
    }
    return turnRow.sequence;
  });

  const verificationError = (detail: string): OrchestrationCommandInvariantError =>
    new OrchestrationCommandInvariantError({
      commandType: "thread.turn.start",
      detail,
    });

  const validateVerificationTurnCommandShape = Effect.fn(
    "OrchestrationEngine.validateVerificationTurnCommandShape",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlVerificationTurnDispatchEvidence,
  ) {
    if (
      command.type !== "thread.turn.start" ||
      command.commandId !== evidence.turnRequestCommandId ||
      command.threadId !== evidence.threadId ||
      command.message.messageId !== evidence.messageId ||
      command.message.role !== "user" ||
      command.message.attachments.length !== 0 ||
      command.bootstrap !== undefined ||
      command.titleSeed !== undefined ||
      command.interactionMode !== "default" ||
      command.sourceProposedPlan?.threadId !== evidence.planningThreadId ||
      command.sourceProposedPlan.planId !== evidence.planId
    ) {
      return yield* verificationError(
        "Verification dispatch command identity, source plan, or shape is invalid.",
      );
    }
  });

  const validateVerificationTurnCommand = Effect.fn(
    "OrchestrationEngine.validateVerificationTurnCommand",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlVerificationTurnDispatchEvidence,
  ) {
    yield* validateVerificationTurnCommandShape(command, evidence);
    if (command.type !== "thread.turn.start") {
      return yield* verificationError("Verification dispatch command type is invalid.");
    }
    const rows = yield* sql<{
      readonly promptBytes: unknown;
      readonly createdAtBytes: unknown;
      readonly runtimeModeBytes: unknown;
      readonly modelSelectionBytes: unknown;
      readonly interactionModeBytes: unknown;
      readonly planningThreadIdBytes: unknown;
      readonly planIdBytes: unknown;
      readonly controlStateBytes: unknown;
    }>`
      SELECT CAST(intent.prompt_text AS BLOB) AS "promptBytes",
        CAST(intent.created_at AS BLOB) AS "createdAtBytes",
        CAST(intent.runtime_mode AS BLOB) AS "runtimeModeBytes",
        CAST(intent.model_selection_json AS BLOB) AS "modelSelectionBytes",
        CAST(thread.interaction_mode AS BLOB) AS "interactionModeBytes",
        CAST(intent.planning_thread_id AS BLOB) AS "planningThreadIdBytes",
        CAST(intent.plan_id AS BLOB) AS "planIdBytes",
        CAST(json_extract(thread.agent_control_json, '$.controlState') AS BLOB)
          AS "controlStateBytes"
      FROM agent_control_verification_handoff_intents intent
      JOIN agent_control_verification_handoff_receipts receipt
        ON receipt.handoff_id = intent.handoff_id
      JOIN agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = intent.handoff_id
      JOIN agent_control_verification_materialization_markers marker
        ON marker.handoff_id = intent.handoff_id
      JOIN agent_control_verification_thread_reservation_states reservation
        ON reservation.controlled_thread_reservation_id = intent.controlled_thread_reservation_id
       AND reservation.thread_id = intent.thread_id
       AND reservation.status = 'bound' AND reservation.revision = 3
      JOIN projection_threads thread
        ON thread.thread_id = intent.thread_id AND thread.project_id = intent.project_id
       AND thread.worktree_path = (
         SELECT materialization.worktree_path
         FROM agent_control_verification_materialization_evidence materialization
         WHERE materialization.materialization_evidence_id = intent.materialization_evidence_id
       )
       AND thread.runtime_mode = intent.runtime_mode
       AND json(thread.model_selection_json) = json(intent.model_selection_json)
      WHERE intent.handoff_id = ${evidence.handoffId}
        AND intent.handoff_fingerprint = ${evidence.handoffFingerprint}
        AND intent.controlled_thread_reservation_id = ${evidence.controlledThreadReservationId}
        AND intent.thread_id = ${evidence.threadId}
        AND intent.turn_request_command_id = ${evidence.turnRequestCommandId}
        AND intent.message_id = ${evidence.messageId}
        AND intent.planning_thread_id = ${evidence.planningThreadId}
        AND intent.plan_id = ${evidence.planId}
    `;
    if (rows.length !== 1) {
      return yield* verificationError(
        "Verification dispatch requires one complete accepted handoff.",
      );
    }
    const rawRow = rows[0]!;
    const row = yield* Effect.try({
      try: () => ({
        promptText: decodeCanonicalUtf8Bytes(rawRow.promptBytes),
        createdAt: decodeCanonicalUtf8Bytes(rawRow.createdAtBytes),
        runtimeMode: decodeCanonicalUtf8Bytes(rawRow.runtimeModeBytes),
        modelSelectionJson: decodeCanonicalUtf8Bytes(rawRow.modelSelectionBytes),
        interactionMode: decodeCanonicalUtf8Bytes(rawRow.interactionModeBytes),
        planningThreadId: decodeCanonicalUtf8Bytes(rawRow.planningThreadIdBytes),
        planId: decodeCanonicalUtf8Bytes(rawRow.planIdBytes),
        controlState: decodeCanonicalUtf8Bytes(rawRow.controlStateBytes),
      }),
      catch: (cause) =>
        verificationError(`Verification handoff contains invalid UTF-8 evidence: ${String(cause)}`),
    });
    const modelSelection = yield* decodeModelSelectionJson(row.modelSelectionJson).pipe(
      Effect.mapError(() =>
        verificationError("Verification handoff model selection is noncanonical."),
      ),
    );
    if (
      row.promptText !== command.message.text ||
      row.createdAt !== command.createdAt ||
      row.runtimeMode !== command.runtimeMode ||
      row.interactionMode !== "default" ||
      row.planningThreadId !== evidence.planningThreadId ||
      row.planId !== evidence.planId ||
      row.controlState !== "controlled" ||
      !Equal.equals(modelSelection, command.modelSelection)
    ) {
      return yield* verificationError(
        "Verification dispatch conflicts with frozen handoff authority.",
      );
    }
  });

  const validateVerificationTurnReplay = Effect.fn(
    "OrchestrationEngine.validateVerificationTurnReplay",
  )(function* (
    command: OrchestrationCommand,
    evidence: AgentControlVerificationTurnDispatchEvidence,
  ) {
    yield* validateVerificationTurnCommandShape(command, evidence);
    if (command.type !== "thread.turn.start") {
      return yield* verificationError("Verification replay command type is invalid.");
    }
    const rawEvents = yield* sql<Record<string, unknown>>`
      SELECT sequence, stream_version AS "streamVersion", event_id AS "eventId",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId", event_type AS type,
        occurred_at AS "occurredAt", command_id AS "commandId",
        causation_event_id AS "causationEventId", correlation_id AS "correlationId",
        actor_kind AS "actorKind", payload_json AS "payloadJson", metadata_json AS "metadataJson",
        CAST(payload_json AS BLOB) AS "payloadBytes",
        CAST(metadata_json AS BLOB) AS "metadataBytes"
      FROM main.orchestration_events WHERE command_id = ${command.commandId} ORDER BY sequence
    `;
    const eventRows = yield* Effect.forEach(rawEvents, (raw) =>
      Effect.gen(function* () {
        const payloadKeys =
          raw.type === "thread.message-sent"
            ? [
                "attachments",
                "createdAt",
                "messageId",
                "role",
                "streaming",
                "text",
                "threadId",
                "turnId",
                "updatedAt",
              ]
            : raw.type === "thread.turn-start-requested"
              ? [
                  "createdAt",
                  "interactionMode",
                  "messageId",
                  "modelSelection",
                  "runtimeMode",
                  "sourceProposedPlan",
                  "threadId",
                ]
              : [];
        if (
          payloadKeys.length === 0 ||
          typeof raw.payloadJson !== "string" ||
          typeof raw.metadataJson !== "string"
        ) {
          return yield* verificationError("Verification replay rows are not canonical.");
        }
        const parsed = yield* Effect.try({
          try: () => {
            const payload = parseCanonicalJsonObjectBytes(raw.payloadBytes, payloadKeys);
            const metadata = parseCanonicalJsonObjectBytes(raw.metadataBytes, []);
            if (payload.source !== raw.payloadJson || metadata.source !== raw.metadataJson) {
              throw new Error("SQLite TEXT and BLOB views disagree");
            }
            return {
              payload: payload.value,
              metadata: metadata.value,
              payloadJson: payload.source,
              metadataJson: metadata.source,
            };
          },
          catch: () =>
            verificationError(
              "Verification replay raw JSON is noncanonical, invalid, or has unexpected keys.",
            ),
        });
        const decoded = yield* decodeInitialPlanningEventRow(raw).pipe(
          Effect.mapError(() => verificationError("Verification replay rows are not canonical.")),
        );
        return { ...decoded, ...parsed };
      }),
    );
    if (eventRows.length !== 2) {
      return yield* verificationError(
        "Verification turn replay requires exactly two canonical events.",
      );
    }
    const messageRow = eventRows[0]!;
    const turnRow = eventRows[1]!;
    const envelopeFromRow = (row: typeof messageRow) =>
      canonicalInitialPlanningEventEnvelopeFromStoredJson({
        ...row,
        aggregateId: ThreadId.make(row.aggregateId),
        commandId: CommandId.make(row.commandId),
        correlationId: CommandId.make(row.correlationId),
        payloadJson: row.payloadJson,
        metadataJson: row.metadataJson,
      });
    const templateFromRow = (row: typeof messageRow) =>
      canonicalInitialPlanningEventTemplate({
        ...row,
        aggregateId: ThreadId.make(row.aggregateId),
        commandId: CommandId.make(row.commandId),
        correlationId: CommandId.make(row.correlationId),
        payload: row.payload,
        metadata: row.metadata,
      });
    const messageEnvelopeJson = envelopeFromRow(messageRow);
    const turnEnvelopeJson = envelopeFromRow(turnRow);
    const messageTemplateJson = templateFromRow(messageRow);
    const turnTemplateJson = templateFromRow(turnRow);
    const eventEvidenceDigest = combinedInitialPlanningEventDigest(
      messageEnvelopeJson,
      turnEnvelopeJson,
    );
    if (
      messageRow.eventId !== evidence.messageEventId ||
      turnRow.eventId !== evidence.turnRequestEventId ||
      messageRow.streamVersion !== 3 ||
      turnRow.streamVersion !== 4 ||
      messageTemplateJson !== evidence.messageEventTemplateJson ||
      turnTemplateJson !== evidence.turnRequestEventTemplateJson ||
      combinedInitialPlanningEventDigest(messageTemplateJson, turnTemplateJson) !==
        evidence.eventTemplateDigest ||
      turnRow.sequence !== messageRow.sequence + 1 ||
      turnRow.causationEventId !== messageRow.eventId
    ) {
      return yield* verificationError("Verification replay events conflict with frozen evidence.");
    }
    const rows = yield* sql<{
      readonly sequence: number;
      readonly messageEventEnvelopeBytes: unknown;
      readonly turnRequestEventEnvelopeBytes: unknown;
      readonly eventEvidenceDigestBytes: unknown;
      readonly valid: number;
    }>`
      SELECT accepted.turn_request_event_sequence AS sequence,
        CAST(accepted.message_event_envelope_json AS BLOB) AS "messageEventEnvelopeBytes",
        CAST(accepted.turn_request_event_envelope_json AS BLOB)
          AS "turnRequestEventEnvelopeBytes",
        CAST(accepted.event_evidence_digest AS BLOB) AS "eventEvidenceDigestBytes",
        CASE WHEN accepted.handoff_fingerprint = ${evidence.handoffFingerprint}
          AND accepted.controlled_thread_reservation_id = ${evidence.controlledThreadReservationId}
          AND accepted.thread_id = ${evidence.threadId}
          AND accepted.planning_thread_id = ${evidence.planningThreadId}
          AND accepted.plan_id = ${evidence.planId}
          AND accepted.turn_request_command_id = ${command.commandId}
          AND accepted.message_id = ${command.message.messageId}
          AND receipt.authority = 'agent-control' AND receipt.status = 'accepted'
          AND receipt.aggregate_kind = 'thread' AND receipt.aggregate_id = ${command.threadId}
          AND receipt.result_sequence = accepted.turn_request_event_sequence
          AND message.event_type = 'thread.message-sent'
          AND turn_event.event_type = 'thread.turn-start-requested'
          AND projected.message_id = ${command.message.messageId}
          AND projected.thread_id = ${command.threadId}
          AND projected.text = ${command.message.text}
          AND projected.attachments_json = '[]'
          AND pending.thread_id = ${command.threadId}
          AND pending.pending_message_id = ${command.message.messageId}
          AND (SELECT count(*) FROM main.orchestration_events event
            WHERE event.command_id = ${command.commandId}) = 2
        THEN 1 ELSE 0 END AS valid
      FROM agent_control_verification_turn_accepted accepted
      JOIN main.orchestration_command_receipts receipt
        ON receipt.command_id = accepted.turn_request_command_id
      JOIN main.orchestration_events message ON message.event_id = accepted.message_event_id
       AND message.sequence = accepted.message_event_sequence
      JOIN main.orchestration_events turn_event ON turn_event.event_id = accepted.turn_request_event_id
       AND turn_event.sequence = accepted.turn_request_event_sequence
      JOIN projection_thread_messages projected ON projected.message_id = accepted.message_id
      JOIN projection_turns pending ON pending.thread_id = accepted.thread_id
       AND pending.pending_message_id = accepted.message_id
      WHERE accepted.handoff_id = ${evidence.handoffId}
         OR accepted.turn_request_command_id = ${command.commandId}
         OR accepted.thread_id = ${command.threadId}
    `;
    const acceptedRow =
      rows.length === 1
        ? yield* Effect.try({
            try: () => ({
              ...rows[0]!,
              messageEventEnvelopeJson: decodeCanonicalUtf8Bytes(
                rows[0]!.messageEventEnvelopeBytes,
              ),
              turnRequestEventEnvelopeJson: decodeCanonicalUtf8Bytes(
                rows[0]!.turnRequestEventEnvelopeBytes,
              ),
              eventEvidenceDigest: decodeCanonicalUtf8Bytes(rows[0]!.eventEvidenceDigestBytes),
            }),
            catch: (cause) =>
              verificationError(
                `Verification replay acceptance contains invalid UTF-8 evidence: ${String(cause)}`,
              ),
          })
        : undefined;
    if (
      rows.length !== 1 ||
      acceptedRow?.valid !== 1 ||
      acceptedRow.sequence !== turnRow.sequence ||
      acceptedRow.messageEventEnvelopeJson !== messageEnvelopeJson ||
      acceptedRow.turnRequestEventEnvelopeJson !== turnEnvelopeJson ||
      acceptedRow.eventEvidenceDigest !== eventEvidenceDigest
    ) {
      return yield* verificationError(
        "Verification replay acceptance evidence is incomplete or inconsistent.",
      );
    }
    return turnRow.sequence;
  });
  const projectEventsOntoReadModel = (
    baseReadModel: OrchestrationReadModel,
    events: ReadonlyArray<OrchestrationEvent>,
  ): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError, never> =>
    Effect.gen(function* () {
      let nextReadModel = baseReadModel;
      for (const event of events) {
        nextReadModel = yield* projectEvent(nextReadModel, event);
      }
      return nextReadModel;
    });

  const loadMaterializationEvents = Effect.fn("loadMaterializationEvents")(function* (
    command: AgentControlThreadMaterializeCommand,
  ) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        sequence, stream_version AS "streamVersion", event_id AS "eventId",
        event_type AS type, aggregate_kind AS "aggregateKind",
        stream_id AS "aggregateId", occurred_at AS "occurredAt",
        command_id AS "commandId", causation_event_id AS "causationEventId",
        correlation_id AS "correlationId", payload_json AS payload,
        metadata_json AS metadata,
        CASE
          WHEN event_type = 'thread.created'
            AND json_valid(payload_json) = 1
            AND json_type(payload_json) = 'object'
            AND (SELECT count(*) FROM json_each(payload_json)) = 10
            AND (SELECT count(DISTINCT key) FROM json_each(payload_json)) = 10
            AND NOT EXISTS (
              SELECT 1 FROM json_each(payload_json)
              WHERE key NOT IN (
                'threadId', 'projectId', 'title', 'modelSelection', 'runtimeMode',
                'interactionMode', 'branch', 'worktreePath', 'createdAt', 'updatedAt'
              )
            )
            AND json_type(payload_json, '$.modelSelection') = 'object'
            AND (
              SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
            ) IN (2, 3)
            AND (
              SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
            ) = (
              SELECT count(DISTINCT key)
              FROM json_each(payload_json, '$.modelSelection')
            )
            AND NOT EXISTS (
              SELECT 1 FROM json_each(payload_json, '$.modelSelection')
              WHERE key NOT IN ('instanceId', 'model', 'options')
            )
            AND (
              SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
              WHERE key = 'instanceId'
            ) = 1
            AND (
              SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
              WHERE key = 'model'
            ) = 1
            AND (
              (
                SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
                WHERE key = 'options'
              ) = 0
              OR (
                (
                  SELECT count(*) FROM json_each(payload_json, '$.modelSelection')
                  WHERE key = 'options'
                ) = 1
                AND json_type(payload_json, '$.modelSelection.options') = 'array'
                AND NOT EXISTS (
                  SELECT 1
                  FROM json_each(payload_json, '$.modelSelection.options') option
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
                  SELECT 1
                  FROM json_each(payload_json, '$.modelSelection.options') option
                  GROUP BY json_extract(option.value, '$.id')
                  HAVING count(*) <> 1
                )
              )
            )
            AND json_valid(metadata_json) = 1
            AND json_type(metadata_json) = 'object'
            AND (SELECT count(*) FROM json_each(metadata_json)) = 0
            THEN 1
          WHEN event_type = 'thread.agent-control-bound'
            AND json_valid(payload_json) = 1
            AND json_type(payload_json) = 'object'
            AND (SELECT count(*) FROM json_each(payload_json)) = 3
            AND (SELECT count(DISTINCT key) FROM json_each(payload_json)) = 3
            AND NOT EXISTS (
              SELECT 1 FROM json_each(payload_json)
              WHERE key NOT IN ('threadId', 'binding', 'updatedAt')
            )
            AND json_type(payload_json, '$.binding') = 'object'
            AND (SELECT count(*) FROM json_each(payload_json, '$.binding')) = 5
            AND (
              SELECT count(DISTINCT key) FROM json_each(payload_json, '$.binding')
            ) = 5
            AND NOT EXISTS (
              SELECT 1 FROM json_each(payload_json, '$.binding')
              WHERE key NOT IN (
                'taskId', 'stageRunId', 'attemptId', 'roleId', 'controlState'
              )
            )
            AND json_valid(metadata_json) = 1
            AND json_type(metadata_json) = 'object'
            AND (SELECT count(*) FROM json_each(metadata_json)) = 0
            THEN 1
          ELSE 0
        END AS "jsonCanonical"
      FROM main.orchestration_events
      WHERE command_id = ${command.commandId}
      ORDER BY sequence ASC
    `;
    return yield* Effect.forEach(rows, (row) =>
      decodeMaterializationEventRow(row).pipe(
        Effect.mapError(() => evidenceError("materialization-event-row-invalid", command.threadId)),
        Effect.flatMap((decoded) =>
          decodeOrchestrationEvent({
            sequence: decoded.sequence,
            eventId: decoded.eventId,
            type: decoded.type,
            aggregateKind: decoded.aggregateKind,
            aggregateId: decoded.aggregateId,
            occurredAt: decoded.occurredAt,
            commandId: decoded.commandId,
            causationEventId: decoded.causationEventId,
            correlationId: decoded.correlationId,
            payload: decoded.payload,
            metadata: decoded.metadata,
          }).pipe(
            Effect.mapError(() =>
              evidenceError("materialization-event-payload-invalid", command.threadId),
            ),
            Effect.map((event) => ({ row: decoded, event })),
          ),
        ),
      ),
    );
  });

  const loadAuthoritativeCurrentMaterializedThread = Effect.fn(
    "loadAuthoritativeCurrentMaterializedThread",
  )(function* (
    command: AgentControlThreadMaterializeCommand,
    intent: StoredAgentControlThreadMaterializationIntent,
  ) {
    const streamRows = yield* sql<{
      readonly sequence: number;
      readonly streamVersion: number;
      readonly eventId: string;
      readonly commandId: string | null;
    }>`
      SELECT
        sequence, stream_version AS "streamVersion", event_id AS "eventId",
        command_id AS "commandId"
      FROM main.orchestration_events
      WHERE aggregate_kind = 'thread'
        AND stream_id = ${command.threadId}
      ORDER BY stream_version ASC
    `;
    if (streamRows.length < 2) {
      return yield* evidenceError("materialization-current-stream-incomplete", command.threadId);
    }
    for (const [index, row] of streamRows.entries()) {
      const prior = streamRows[index - 1];
      if (
        row.streamVersion !== index + 1 ||
        (prior !== undefined && row.sequence <= prior.sequence)
      ) {
        return yield* evidenceError(
          "materialization-current-stream-coordinates-inconsistent",
          command.threadId,
        );
      }
    }
    if (
      streamRows[0]?.eventId !== intent.createdEventId ||
      streamRows[0]?.sequence !== intent.createdEventSequence ||
      streamRows[0]?.commandId !== command.commandId ||
      streamRows[1]?.eventId !== intent.bindingEventId ||
      streamRows[1]?.sequence !== intent.bindingEventSequence ||
      streamRows[1]?.commandId !== command.commandId
    ) {
      return yield* evidenceError(
        "materialization-current-stream-origin-inconsistent",
        command.threadId,
      );
    }

    const allEvents = Array.from(yield* Stream.runCollect(eventStore.readAll()));
    const authoritative = yield* projectEventsOntoReadModel(
      createEmptyReadModel("1970-01-01T00:00:00.000Z"),
      allEvents,
    ).pipe(
      Effect.mapError(() =>
        evidenceError("materialization-current-stream-projector-invalid", command.threadId),
      ),
    );
    const projectionJsonRows = yield* sql<{ readonly jsonCanonical: number }>`
      SELECT
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
              )
              AND NOT EXISTS (
                SELECT 1 FROM json_each(model_selection_json, '$.options') option
                GROUP BY json_extract(option.value, '$.id')
                HAVING count(*) <> 1
              )
            )
          )
          AND json_valid(agent_control_json) = 1
          AND json_type(agent_control_json) = 'object'
          AND (SELECT count(*) FROM json_each(agent_control_json)) = 5
          AND (SELECT count(DISTINCT key) FROM json_each(agent_control_json)) = 5
          AND NOT EXISTS (
            SELECT 1 FROM json_each(agent_control_json)
            WHERE key NOT IN (
              'taskId', 'stageRunId', 'attemptId', 'roleId', 'controlState'
            )
          )
          THEN 1 ELSE 0 END AS "jsonCanonical"
      FROM projection_threads
      WHERE thread_id = ${command.threadId}
    `;
    if (projectionJsonRows.length !== 1 || projectionJsonRows[0]?.jsonCanonical !== 1) {
      return yield* evidenceError(
        "materialization-current-thread-projection-json-noncanonical",
        command.threadId,
      );
    }
    const projected = yield* projectionSnapshotQuery.getSnapshot();
    const authoritativeThread = authoritative.threads.find(
      (candidate) => candidate.id === command.threadId,
    );
    const projectedThread = projected.threads.find(
      (candidate) => candidate.id === command.threadId,
    );
    if (
      authoritative.snapshotSequence !== projected.snapshotSequence ||
      authoritativeThread === undefined ||
      projectedThread === undefined ||
      !Equal.equals(authoritativeThread, projectedThread)
    ) {
      return yield* evidenceError(
        "materialization-current-thread-projection-inconsistent",
        command.threadId,
      );
    }
    return projected;
  });

  const validateMaterializationReceipt = Effect.fn("validateMaterializationReceipt")(function* (
    command: AgentControlThreadMaterializeCommand,
    commandFingerprint: string,
    intent: StoredAgentControlThreadMaterializationIntent,
    acceptedMarkerExpectation: "required" | "absent" = "required",
  ) {
    const persistedFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
      crypto,
      commandFromAgentControlThreadMaterializationIntent(intent),
    ).pipe(
      Effect.mapError(() =>
        evidenceError("materialization-intent-fingerprint-invalid", command.threadId),
      ),
    );
    if (intent.commandFingerprint !== persistedFingerprint) {
      return yield* evidenceError(
        "materialization-intent-fingerprint-inconsistent",
        command.threadId,
      );
    }
    if (intent.commandFingerprint !== commandFingerprint) {
      return yield* new OrchestrationCommandIdentityConflictError({
        commandId: command.commandId,
        commandType: command.type,
      });
    }
    if (!sameAgentControlThreadMaterializationCommandIntent(intent, command, commandFingerprint)) {
      return yield* evidenceError(
        "materialization-intent-command-coordinates-inconsistent",
        command.threadId,
      );
    }

    const receiptOption = yield* commandReceiptRepository.getByCommandId({
      commandId: command.commandId,
    });
    if (Option.isNone(receiptOption)) {
      return yield* evidenceError("materialization-receipt-missing", command.threadId);
    }
    const receipt = receiptOption.value;
    if (
      receipt.commandId !== intent.commandId ||
      receipt.authority !== intent.authority ||
      receipt.aggregateKind !== intent.aggregateKind ||
      receipt.aggregateId !== intent.threadId ||
      receipt.acceptedAt !== intent.receiptAcceptedAt ||
      receipt.resultSequence !== intent.receiptResultSequence ||
      receipt.status !== intent.receiptStatus ||
      receipt.error !== intent.receiptError
    ) {
      return yield* evidenceError("materialization-receipt-inconsistent", command.threadId);
    }

    const acceptedReceiptEvidence =
      yield* loadAgentControlThreadMaterializationAcceptedReceiptEvidence(
        sql,
        command.commandId,
      ).pipe(
        Effect.mapError(() =>
          evidenceError("materialization-receipt-evidence-invalid", command.threadId),
        ),
      );
    const persisted = yield* loadMaterializationEvents(command);
    const projected = yield* projectionSnapshotQuery.getCommandReadModel();
    const thread = projected.threads.find((candidate) => candidate.id === command.threadId);

    if (receipt.status === "rejected") {
      if (
        intent.acceptedReceiptCommandId !== null ||
        Option.isSome(acceptedReceiptEvidence) ||
        persisted.length !== 0 ||
        thread !== undefined ||
        intent.createdEventId !== null ||
        intent.createdEventType !== null ||
        intent.createdEventSequence !== null ||
        intent.createdEventStreamVersion !== null ||
        intent.bindingEventId !== null ||
        intent.bindingEventType !== null ||
        intent.bindingEventSequence !== null ||
        intent.bindingEventStreamVersion !== null ||
        receipt.error === null
      ) {
        return yield* evidenceError(
          "rejected-materialization-evidence-inconsistent",
          command.threadId,
        );
      }
      return yield* new OrchestrationCommandPreviouslyRejectedError({
        commandId: command.commandId,
        detail: receipt.error,
      });
    }

    if (
      intent.acceptedReceiptCommandId !== command.commandId ||
      (acceptedMarkerExpectation === "required" && Option.isNone(acceptedReceiptEvidence)) ||
      (acceptedMarkerExpectation === "absent" && Option.isSome(acceptedReceiptEvidence)) ||
      (Option.isSome(acceptedReceiptEvidence) &&
        (acceptedReceiptEvidence.value.commandId !== intent.commandId ||
          acceptedReceiptEvidence.value.commandType !== intent.commandType ||
          acceptedReceiptEvidence.value.authority !== intent.authority ||
          acceptedReceiptEvidence.value.aggregateKind !== intent.aggregateKind ||
          acceptedReceiptEvidence.value.threadId !== intent.threadId ||
          acceptedReceiptEvidence.value.commandFingerprint !== intent.commandFingerprint ||
          acceptedReceiptEvidence.value.resultSequence !== intent.receiptResultSequence ||
          acceptedReceiptEvidence.value.acceptedAt !== intent.receiptAcceptedAt ||
          acceptedReceiptEvidence.value.status !== intent.receiptStatus))
    ) {
      return yield* evidenceError(
        "accepted-materialization-receipt-evidence-inconsistent",
        command.threadId,
      );
    }
    if (persisted.length !== 2) {
      return yield* evidenceError("materialization-event-count-invalid", command.threadId);
    }
    const created = persisted[0];
    const bound = persisted[1];
    if (
      created === undefined ||
      bound === undefined ||
      created.event.type !== "thread.created" ||
      bound.event.type !== "thread.agent-control-bound" ||
      created.row.eventId !== intent.createdEventId ||
      created.row.sequence !== intent.createdEventSequence ||
      created.row.streamVersion !== intent.createdEventStreamVersion ||
      bound.row.eventId !== intent.bindingEventId ||
      bound.row.sequence !== intent.bindingEventSequence ||
      bound.row.streamVersion !== intent.bindingEventStreamVersion ||
      created.row.sequence + 1 !== bound.row.sequence ||
      created.row.streamVersion !== 1 ||
      bound.row.streamVersion !== 2 ||
      receipt.resultSequence !== bound.row.sequence
    ) {
      return yield* evidenceError(
        "materialization-event-coordinates-inconsistent",
        command.threadId,
      );
    }

    for (const { event } of persisted) {
      if (
        event.aggregateKind !== "thread" ||
        event.aggregateId !== command.threadId ||
        event.commandId !== command.commandId ||
        event.correlationId !== command.commandId ||
        event.causationEventId !== null ||
        event.occurredAt !== command.createdAt ||
        Object.keys(event.metadata).length !== 0
      ) {
        return yield* evidenceError(
          "materialization-event-envelope-inconsistent",
          command.threadId,
        );
      }
    }
    if (
      created.event.payload.threadId !== command.threadId ||
      created.event.payload.projectId !== command.projectId ||
      created.event.payload.title !== command.title ||
      !Equal.equals(created.event.payload.modelSelection, command.modelSelection) ||
      created.event.payload.runtimeMode !== command.runtimeMode ||
      created.event.payload.interactionMode !== command.interactionMode ||
      created.event.payload.branch !== command.branch ||
      created.event.payload.worktreePath !== command.worktreePath ||
      created.event.payload.createdAt !== command.createdAt ||
      created.event.payload.updatedAt !== command.createdAt ||
      bound.event.payload.threadId !== command.threadId ||
      !Equal.equals(bound.event.payload.binding, command.binding) ||
      bound.event.payload.updatedAt !== command.createdAt
    ) {
      return yield* evidenceError(
        "materialization-event-command-binding-inconsistent",
        command.threadId,
      );
    }

    if (thread === undefined) {
      return yield* evidenceError(
        "materialization-thread-projection-inconsistent",
        command.threadId,
      );
    }
    const currentProjection = yield* loadAuthoritativeCurrentMaterializedThread(command, intent);
    return {
      sequence: receipt.resultSequence,
      readModel: currentProjection,
      events: persisted.map(({ event }) => event),
    };
  });

  type MaterializationTransactionOutcome =
    | {
        readonly _tag: "Accepted";
        readonly result: AgentControlThreadMaterializationTransactionResult;
      }
    | {
        readonly _tag: "Rejected";
        readonly error: OrchestrationCommandInvariantError;
      };

  const runAgentControlThreadMaterializationInTransaction = Effect.fn(
    "runAgentControlThreadMaterializationInTransaction",
  )(function* (
    command: AgentControlThreadMaterializeCommand,
    commandFingerprint: string,
  ): Effect.fn.Return<MaterializationTransactionOutcome, OrchestrationDispatchError | SqlError> {
    const receipt = yield* commandReceiptRepository.getByCommandId({
      commandId: command.commandId,
    });
    const intent = yield* loadAgentControlThreadMaterializationIntent(sql, command.commandId).pipe(
      Effect.mapError(() => evidenceError("materialization-intent-invalid", command.threadId)),
    );
    if (Option.isSome(receipt) || Option.isSome(intent)) {
      if (Option.isNone(receipt) || Option.isNone(intent)) {
        return yield* evidenceError(
          "materialization-receipt-intent-bijection-missing",
          command.threadId,
        );
      }
      const replay = yield* validateMaterializationReceipt(
        command,
        commandFingerprint,
        intent.value,
      );
      yield* materializationTransactionHooks.afterAuthoritativeRead({
        commandId: command.commandId,
        threadId: command.threadId,
        projectExists: replay.readModel.projects.some(
          (project) => project.id === command.projectId,
        ),
        threadExists: true,
        receiptExists: true,
        intentExists: true,
        createdEventSequence: intent.value.createdEventSequence,
        bindingEventSequence: intent.value.bindingEventSequence,
      });
      return {
        _tag: "Accepted",
        result: {
          command,
          commandFingerprint,
          committedEvents: [],
          lastSequence: replay.sequence,
          nextCommandReadModel: replay.readModel,
        },
      };
    }

    const authoritativeReadModel = yield* projectionSnapshotQuery.getCommandReadModel();
    const observation = {
      commandId: command.commandId,
      threadId: command.threadId,
      projectExists: authoritativeReadModel.projects.some(
        (project) => project.id === command.projectId,
      ),
      threadExists: authoritativeReadModel.threads.some((thread) => thread.id === command.threadId),
      receiptExists: false,
      intentExists: false,
      createdEventSequence: null,
      bindingEventSequence: null,
    } as const;
    yield* materializationTransactionHooks.afterAuthoritativeRead(observation);

    const decision = yield* Effect.result(
      decideOrchestrationCommand({
        command,
        readModel: authoritativeReadModel,
        authority: "agent-control",
      }).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provideService(OrchestrationEnginePublicationHooks, publicationHooks),
        Effect.mapError((cause) =>
          isOrchestrationCommandInvariantError(cause) ||
          isOrchestrationCommandIdentityConflictError(cause)
            ? cause
            : new PersistenceSqlError({
                operation: "OrchestrationEngine.materializationDecider",
                cause,
              }),
        ),
      ),
    );
    if (decision._tag === "Failure") {
      if (isOrchestrationCommandIdentityConflictError(decision.failure)) {
        return yield* decision.failure;
      }
      if (!isOrchestrationCommandInvariantError(decision.failure)) {
        return yield* decision.failure;
      }
      const error = decision.failure;
      const project = authoritativeReadModel.projects.find(
        (candidate) => candidate.id === command.projectId,
      );
      const threadExists = authoritativeReadModel.threads.some(
        (candidate) => candidate.id === command.threadId,
      );
      if (project === undefined || project.deletedAt !== null || threadExists) {
        return yield* error;
      }
      const rejectedAt = command.createdAt;
      yield* commandReceiptRepository.insert({
        commandId: command.commandId,
        authority: "agent-control",
        aggregateKind: "thread",
        aggregateId: command.threadId,
        acceptedAt: rejectedAt,
        resultSequence: authoritativeReadModel.snapshotSequence,
        status: "rejected",
        error: error.message,
      });
      yield* insertAgentControlThreadMaterializationIntent(
        sql,
        rejectedAgentControlThreadMaterializationIntent(command, commandFingerprint, {
          status: "rejected",
          resultSequence: authoritativeReadModel.snapshotSequence,
          acceptedAt: rejectedAt,
          error: error.message,
        }),
      ).pipe(
        Effect.mapError(() =>
          evidenceError("rejected-materialization-intent-invalid", command.threadId),
        ),
      );
      return { _tag: "Rejected", error };
    }

    const drafts = Array.isArray(decision.success) ? decision.success : [decision.success];
    if (
      drafts.length !== 2 ||
      drafts[0]?.type !== "thread.created" ||
      drafts[1]?.type !== "thread.agent-control-bound"
    ) {
      return yield* evidenceError("materialization-decider-event-shape-invalid", command.threadId);
    }

    yield* materializationTransactionHooks.beforeFirstEventAppend(observation);
    const created = yield* eventStore.appendAgentControlThreadMaterialization(drafts[0], 1);
    const afterCreated = {
      ...observation,
      createdEventSequence: created.sequence,
    };
    yield* materializationTransactionHooks.afterFirstEventAppend(afterCreated);
    let nextCommandReadModel = yield* projectEvent(authoritativeReadModel, created);
    yield* projectionPipeline.projectEvent(created);

    const bound = yield* eventStore.appendAgentControlThreadMaterialization(drafts[1], 2);
    const afterBound = {
      ...afterCreated,
      bindingEventSequence: bound.sequence,
    };
    yield* materializationTransactionHooks.afterSecondEventAppend(afterBound);
    nextCommandReadModel = yield* projectEvent(nextCommandReadModel, bound);
    yield* projectionPipeline.projectEvent(bound);
    yield* materializationTransactionHooks.afterProjection(afterBound);

    yield* commandReceiptRepository.insert({
      commandId: command.commandId,
      authority: "agent-control",
      aggregateKind: "thread",
      aggregateId: command.threadId,
      acceptedAt: command.createdAt,
      resultSequence: bound.sequence,
      status: "accepted",
      error: null,
    });
    yield* materializationTransactionHooks.afterReceiptInsert(afterBound);
    const acceptedIntent = acceptedAgentControlThreadMaterializationIntent(
      command,
      commandFingerprint,
      {
        createdEventId: created.eventId,
        createdEventSequence: created.sequence,
        bindingEventId: bound.eventId,
        bindingEventSequence: bound.sequence,
      },
    );
    yield* insertAgentControlThreadMaterializationIntent(sql, acceptedIntent).pipe(
      Effect.mapError(() =>
        evidenceError("accepted-materialization-intent-invalid", command.threadId),
      ),
    );
    yield* materializationTransactionHooks.afterIntentInsert(afterBound);

    const insertedIntent = yield* loadAgentControlThreadMaterializationIntent(
      sql,
      command.commandId,
    ).pipe(
      Effect.mapError(() =>
        evidenceError("materialization-intent-invalid-after-insert", command.threadId),
      ),
    );
    if (Option.isNone(insertedIntent)) {
      return yield* evidenceError("materialization-intent-missing-after-insert", command.threadId);
    }
    const validated = yield* validateMaterializationReceipt(
      command,
      commandFingerprint,
      insertedIntent.value,
      "absent",
    );
    yield* materializationTransactionHooks.beforeTransactionComplete(afterBound);
    return {
      _tag: "Accepted",
      result: {
        command,
        commandFingerprint,
        committedEvents: [created, bound],
        lastSequence: validated.sequence,
        nextCommandReadModel: validated.readModel,
      },
    };
  });

  const completeAgentControlMaterializationInTransaction = Effect.fn(
    "completeAgentControlMaterializationInTransaction",
  )(function* (result: AgentControlThreadMaterializationTransactionResult) {
    if (result.committedEvents.length === 0) return;
    const intent = yield* loadAgentControlThreadMaterializationIntent(
      sql,
      result.command.commandId,
    ).pipe(
      Effect.mapError(() =>
        evidenceError("materialization-intent-invalid-before-marker", result.command.threadId),
      ),
    );
    if (
      Option.isNone(intent) ||
      !sameAgentControlThreadMaterializationCommandIntent(
        intent.value,
        result.command,
        result.commandFingerprint,
      )
    ) {
      return yield* evidenceError(
        "materialization-intent-missing-before-marker",
        result.command.threadId,
      );
    }
    yield* insertAgentControlThreadMaterializationAcceptedReceiptEvidence(sql, intent.value).pipe(
      Effect.mapError(() =>
        evidenceError("accepted-materialization-receipt-evidence-invalid", result.command.threadId),
      ),
    );
  });

  const materializeAgentControlInTransaction = Effect.fn("materializeAgentControlInTransaction")(
    function* (command: AgentControlThreadMaterializeCommand) {
      yield* validateAgentControlThreadMaterializationCommandIdentity(command);
      const commandFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
        crypto,
        command,
      ).pipe(
        Effect.mapError(
          (cause) =>
            new PersistenceSqlError({
              operation: "OrchestrationEngine.materializationFingerprint",
              cause,
            }),
        ),
      );
      const outcome = yield* runAgentControlThreadMaterializationInTransaction(
        command,
        commandFingerprint,
      ).pipe(
        Effect.catchTag("SqlError", (sqlError) =>
          Effect.fail(
            toPersistenceSqlError("OrchestrationEngine.materializeAgentControlInTransaction")(
              sqlError,
            ),
          ),
        ),
      );
      if (outcome._tag === "Rejected") {
        return yield* outcome.error;
      }
      return outcome.result;
    },
  );

  const replayAgentControlMaterialization = Effect.fn("replayAgentControlMaterialization")(
    function* (command: AgentControlThreadMaterializeCommand) {
      yield* validateAgentControlThreadMaterializationCommandIdentity(command);
      const commandFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
        crypto,
        command,
      ).pipe(
        Effect.mapError(
          (cause) =>
            new PersistenceSqlError({
              operation: "OrchestrationEngine.materializationReplayFingerprint",
              cause,
            }),
        ),
      );
      return yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const receipt = yield* commandReceiptRepository.getByCommandId({
              commandId: command.commandId,
            });
            const intent = yield* loadAgentControlThreadMaterializationIntent(
              sql,
              command.commandId,
            ).pipe(
              Effect.mapError(() =>
                evidenceError("materialization-intent-invalid", command.threadId),
              ),
            );
            if (Option.isNone(receipt) || Option.isNone(intent)) {
              return yield* evidenceError(
                "materialization-receipt-intent-bijection-missing",
                command.threadId,
              );
            }
            const replay = yield* validateMaterializationReceipt(
              command,
              commandFingerprint,
              intent.value,
            );
            return {
              command,
              commandFingerprint,
              committedEvents: replay.events,
              lastSequence: replay.sequence,
              nextCommandReadModel: replay.readModel,
            } satisfies AgentControlThreadMaterializationTransactionResult;
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", (sqlError) =>
            Effect.fail(
              toPersistenceSqlError("OrchestrationEngine.replayAgentControlMaterialization")(
                sqlError,
              ),
            ),
          ),
        );
    },
  );

  const refreshAgentControlMaterialization = Effect.fn("refreshAgentControlMaterialization")(
    function* (_result: AgentControlThreadMaterializationTransactionResult) {
      commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();
    },
  );

  const publishAgentControlMaterialization = Effect.fn("publishAgentControlMaterialization")(
    function* (result: AgentControlThreadMaterializationTransactionResult) {
      for (const event of result.committedEvents) {
        yield* publishDomainEvent(event);
      }
    },
  );

  const processAgentControlThreadMaterialization = Effect.fn(
    "processAgentControlThreadMaterialization",
  )(function* (
    command: AgentControlThreadMaterializeCommand,
    cancelled: Deferred.Deferred<void>,
  ): Effect.fn.Return<
    | {
        readonly _tag: "Accepted";
        readonly committedEvents: ReadonlyArray<OrchestrationEvent>;
        readonly lastSequence: number;
        readonly nextCommandReadModel: OrchestrationReadModel;
      }
    | {
        readonly _tag: "Rejected";
        readonly error: OrchestrationCommandInvariantError;
      },
    OrchestrationDispatchError,
    never
  > {
    yield* validateAgentControlThreadMaterializationCommandIdentity(command);
    const commandFingerprint = yield* fingerprintAgentControlThreadMaterializationCommand(
      crypto,
      command,
    ).pipe(
      Effect.mapError(
        (cause) =>
          new PersistenceSqlError({
            operation: "OrchestrationEngine.materializationFingerprint",
            cause,
          }),
      ),
    );

    const initialAttempt = sql
      .withTransaction(
        Effect.gen(function* () {
          const outcome = yield* runAgentControlThreadMaterializationInTransaction(
            command,
            commandFingerprint,
          );
          if (outcome._tag === "Rejected") {
            return outcome;
          }
          yield* completeAgentControlMaterializationInTransaction(outcome.result);
          return {
            _tag: "Accepted" as const,
            committedEvents: outcome.result.committedEvents,
            lastSequence: outcome.result.lastSequence,
            nextCommandReadModel: outcome.result.nextCommandReadModel,
          };
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (sqlError) =>
          Effect.fail(
            toPersistenceSqlError(
              "OrchestrationEngine.processAgentControlThreadMaterialization:transaction",
            )(sqlError),
          ),
        ),
      );

    const replayCommittedWinner = Effect.fn("replayCommittedMaterializationWinner")(function* (
      originalError: PersistenceSqlError,
    ) {
      const maximumReadAttempts = Math.max(
        1,
        Math.floor(materializationConvergencePolicy.maximumReadAttempts),
      );
      for (let attempt = 1; attempt <= maximumReadAttempts; attempt += 1) {
        const replayAttempt = yield* Effect.result(
          sql
            .withTransaction(
              Effect.gen(function* () {
                if (materializationTransactionHooks.beforeConvergenceReceiptRead !== undefined) {
                  yield* materializationTransactionHooks
                    .beforeConvergenceReceiptRead({
                      commandId: command.commandId,
                      threadId: command.threadId,
                      projectExists: false,
                      threadExists: false,
                      receiptExists: false,
                      intentExists: false,
                      createdEventSequence: null,
                      bindingEventSequence: null,
                    })
                    .pipe(Effect.catchCause(() => Effect.fail(originalError)));
                }
                const receipt = yield* commandReceiptRepository
                  .getByCommandId({
                    commandId: command.commandId,
                  })
                  .pipe(Effect.mapError(() => originalError));
                if (Option.isNone(receipt)) {
                  return Option.none<{
                    readonly sequence: number;
                    readonly readModel: OrchestrationReadModel;
                  }>();
                }
                const intent = yield* loadAgentControlThreadMaterializationIntent(
                  sql,
                  command.commandId,
                ).pipe(
                  Effect.mapError(() =>
                    evidenceError("materialization-intent-invalid", command.threadId),
                  ),
                );
                if (Option.isNone(intent)) {
                  return yield* evidenceError(
                    "materialization-receipt-intent-bijection-missing",
                    command.threadId,
                  );
                }
                return Option.some(
                  yield* validateMaterializationReceipt(command, commandFingerprint, intent.value),
                );
              }),
            )
            .pipe(Effect.catchTag("SqlError", () => originalError)),
        );
        if (replayAttempt._tag === "Failure") {
          return yield* replayAttempt.failure;
        }
        if (Option.isSome(replayAttempt.success)) {
          const replay = replayAttempt.success.value;
          return {
            _tag: "Accepted" as const,
            committedEvents: [],
            lastSequence: replay.sequence,
            nextCommandReadModel: replay.readModel,
          };
        }
        if (attempt < maximumReadAttempts) {
          yield* Effect.sleep(materializationConvergencePolicy.delayBetweenAttempts);
        }
      }
      return yield* originalError;
    });

    const convergedAttempt = initialAttempt.pipe(
      Effect.catchIf(
        (error): error is PersistenceSqlError =>
          isPersistenceSqlError(error) && isRetryableMaterializationSqliteConflict(error),
        replayCommittedWinner,
      ),
    );

    return yield* Effect.raceFirst(
      convergedAttempt,
      Deferred.await(cancelled).pipe(Effect.flatMap(() => Effect.interrupt)),
    );
  });

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const dispatchStartSequence = commandReadModel.snapshotSequence;
    let processingStartedAtMs = 0;
    let runtimeEventAuthorityRaceDetected = false;
    let runtimeEventAuthorityTransactionBodyCompleted = false;
    let verificationCaptureCommitObserved = false;
    let verificationCaptureTransactionResult: {
      readonly committedEvents: ReadonlyArray<OrchestrationEvent>;
      readonly lastSequence: number;
      readonly nextCommandReadModel: OrchestrationReadModel;
    } | null = null;
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
      authority: envelope.authority,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      if (isAgentControlThreadMaterializeCommand(envelope.command)) {
        // A competing engine may have committed the winning materialization.
        // Refresh local authority without republishing another engine's events.
        commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();
        return;
      }

      commandReadModel = yield* projectEventsOntoReadModel(commandReadModel, persistedEvents);

      for (const persistedEvent of persistedEvents) {
        yield* publishDomainEvent(persistedEvent);
      }
    });

    return Effect.exit(
      Effect.gen(function* () {
        processingStartedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.command_authority": envelope.authority,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const initialPlanningEvidence = envelope.initialPlanning;
        if (initialPlanningEvidence !== undefined) {
          const receiptFirst = yield* sql.withTransaction(
            Effect.gen(function* () {
              const rows = yield* sql<{
                readonly receiptCount: number;
                readonly acceptanceCount: number;
              }>`
                SELECT
                  (SELECT count(*) FROM main.orchestration_command_receipts
                   WHERE command_id = ${envelope.command.commandId}) AS "receiptCount",
                  (SELECT count(*) FROM agent_control_initial_planning_turn_accepted
                   WHERE handoff_id = ${initialPlanningEvidence.handoffId}
                      OR turn_request_command_id = ${envelope.command.commandId})
                    AS "acceptanceCount"
              `;
              const row = rows[0];
              if (row === undefined || row.receiptCount > 1 || row.acceptanceCount > 1) {
                return yield* initialPlanningError(
                  "Initial Planning receipt-first coordinates are non-unique.",
                );
              }
              if (row.receiptCount === 0 && row.acceptanceCount === 0) {
                return Option.none<number>();
              }
              if (row.receiptCount !== 1 || row.acceptanceCount !== 1) {
                return yield* initialPlanningError(
                  "Initial Planning receipt-first evidence is partial.",
                );
              }
              return Option.some(
                yield* validateInitialPlanningTurnReplay(envelope.command, initialPlanningEvidence),
              );
            }),
          );
          if (Option.isSome(receiptFirst)) {
            return { sequence: receiptFirst.value };
          }
        }
        const implementationEvidence = envelope.implementation;
        if (implementationEvidence !== undefined) {
          const receiptFirst = yield* sql.withTransaction(
            Effect.gen(function* () {
              const rows = yield* sql<{
                readonly receiptCount: number;
                readonly acceptanceCount: number;
              }>`
                SELECT
                  (SELECT count(*) FROM main.orchestration_command_receipts
                   WHERE command_id = ${envelope.command.commandId}) AS "receiptCount",
                  (SELECT count(*) FROM agent_control_implementation_turn_accepted
                   WHERE handoff_id = ${implementationEvidence.handoffId}
                      OR turn_request_command_id = ${envelope.command.commandId})
                    AS "acceptanceCount"
              `;
              const row = rows[0];
              if (row === undefined || row.receiptCount > 1 || row.acceptanceCount > 1) {
                return yield* implementationError(
                  "Implementation receipt-first coordinates are non-unique.",
                );
              }
              if (row.receiptCount === 0 && row.acceptanceCount === 0) {
                return Option.none<number>();
              }
              if (row.receiptCount !== 1 || row.acceptanceCount !== 1) {
                return yield* implementationError(
                  "Implementation receipt-first evidence is partial.",
                );
              }
              return Option.some(
                yield* validateImplementationTurnReplay(envelope.command, implementationEvidence),
              );
            }),
          );
          if (Option.isSome(receiptFirst)) return { sequence: receiptFirst.value };
        }
        const verificationEvidence = envelope.verification;
        if (verificationEvidence !== undefined) {
          const receiptFirst = yield* sql.withTransaction(
            Effect.gen(function* () {
              const rows = yield* sql<{
                readonly receiptCount: number;
                readonly acceptanceCount: number;
              }>`
                SELECT
                  (SELECT count(*) FROM main.orchestration_command_receipts
                   WHERE command_id = ${envelope.command.commandId}) AS "receiptCount",
                  (SELECT count(*) FROM agent_control_verification_turn_accepted
                   WHERE handoff_id = ${verificationEvidence.handoffId}
                      OR turn_request_command_id = ${envelope.command.commandId})
                    AS "acceptanceCount"
              `;
              const row = rows[0];
              if (row === undefined || row.receiptCount > 1 || row.acceptanceCount > 1) {
                return yield* verificationError(
                  "Verification receipt-first coordinates are non-unique.",
                );
              }
              if (row.receiptCount === 0 && row.acceptanceCount === 0) {
                return Option.none<number>();
              }
              if (row.receiptCount !== 1 || row.acceptanceCount !== 1) {
                return yield* verificationError("Verification receipt-first evidence is partial.");
              }
              return Option.some(
                yield* validateVerificationTurnReplay(envelope.command, verificationEvidence),
              );
            }),
          );
          if (Option.isSome(receiptFirst)) return { sequence: receiptFirst.value };
        }

        // This namespace guard intentionally precedes receipt lookup. A stale
        // or directly persisted generic thread.create receipt must never grant
        // authority to materialize a reserved Agent Control thread id.
        if (isAgentControlReservedThreadCreate(envelope.command)) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail:
              "Reserved Agent Control thread identifiers require the dedicated materialization command.",
          });
        }

        if (
          isAgentControlThreadMaterializeCommand(envelope.command) &&
          envelope.authority !== "agent-control"
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail:
              "Command 'thread.agent-control.materialize' requires 'agent-control' authority.",
          });
        }

        const handoffOwnership = yield* sql<{
          readonly planningCount: number;
          readonly implementationCount: number;
          readonly verificationCount: number;
        }>`
          SELECT
            (SELECT count(*) FROM agent_control_initial_planning_handoff_accepted
             WHERE turn_request_command_id = ${envelope.command.commandId}) AS "planningCount",
            (SELECT count(*) FROM agent_control_implementation_handoff_accepted
             WHERE turn_request_command_id = ${envelope.command.commandId}) AS "implementationCount",
            (SELECT count(*) FROM agent_control_verification_handoff_accepted
             WHERE turn_request_command_id = ${envelope.command.commandId}) AS "verificationCount"
        `.pipe(
          Effect.mapError(toPersistenceSqlError("OrchestrationEngine.initialPlanningOwnership")),
        );
        const isPlanningOwned = handoffOwnership[0]?.planningCount === 1;
        const isImplementationOwned = handoffOwnership[0]?.implementationCount === 1;
        const isVerificationOwned = handoffOwnership[0]?.verificationCount === 1;
        if (
          (envelope.initialPlanning !== undefined && envelope.authority !== "agent-control") ||
          (isPlanningOwned && envelope.initialPlanning === undefined)
        ) {
          return yield* initialPlanningError(
            "Handoff-owned turn requests require the server-only Initial Planning path.",
          );
        }
        if (envelope.initialPlanning !== undefined) {
          yield* validateInitialPlanningTurnCommand(envelope.command, envelope.initialPlanning);
        }
        if (
          (envelope.implementation !== undefined && envelope.authority !== "agent-control") ||
          (isImplementationOwned && envelope.implementation === undefined)
        ) {
          return yield* implementationError(
            "Handoff-owned turn requests require the server-only Implementation path.",
          );
        }
        if (envelope.implementation !== undefined) {
          yield* validateImplementationTurnCommand(envelope.command, envelope.implementation);
        }
        if (
          (envelope.verification !== undefined && envelope.authority !== "agent-control") ||
          (isVerificationOwned && envelope.verification === undefined)
        ) {
          return yield* verificationError(
            "Handoff-owned turn requests require the server-only Verification path.",
          );
        }
        if (envelope.verification !== undefined) {
          yield* validateVerificationTurnCommand(envelope.command, envelope.verification);
        }

        if (isAgentControlThreadMaterializeCommand(envelope.command)) {
          const materialization = yield* processAgentControlThreadMaterialization(
            envelope.command,
            envelope.cancelled,
          );
          if (materialization._tag === "Rejected") {
            return yield* materialization.error;
          }

          commandReadModel = materialization.nextCommandReadModel;
          for (const [index, event] of materialization.committedEvents.entries()) {
            yield* publishDomainEvent(event);
            if (index === 0) {
              yield* Metric.update(
                Metric.withAttributes(
                  orchestrationCommandAckDuration,
                  metricAttributes({
                    ...baseMetricAttributes,
                    ackEventType: event.type,
                  }),
                ),
                Duration.millis(
                  Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs),
                ),
              );
            }
          }
          return { sequence: materialization.lastSequence };
        }

        const verificationRuntimeFragmentSequence =
          envelope.command.type === "thread.verification-result.capture"
            ? yield* loadVerificationResultCaptureRuntimeFragment(envelope.command)
            : null;
        if (
          envelope.command.type === "thread.verification-result.capture" &&
          verificationRuntimeFragmentSequence !== null
        ) {
          yield* validateVerificationResultCaptureReplay(
            envelope.command,
            verificationRuntimeFragmentSequence,
          );
        }

        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        const materializationIntent = yield* loadAgentControlThreadMaterializationIntent(
          sql,
          envelope.command.commandId,
        ).pipe(
          Effect.mapError(
            () =>
              new PersistenceDecodeError({
                operation: "OrchestrationEngine.materializationIntent",
                issue: "materialization-intent-invalid-for-command-type",
              }),
          ),
        );
        if (Option.isSome(materializationIntent)) {
          return yield* new OrchestrationCommandIdentityConflictError({
            commandId: envelope.command.commandId,
            commandType: envelope.command.type,
          });
        }
        if (Option.isSome(existingReceipt)) {
          if (existingReceipt.value.authority !== envelope.authority) {
            return yield* new OrchestrationCommandAuthorityMismatchError({
              commandId: envelope.command.commandId,
              receiptAuthority: existingReceipt.value.authority,
              attemptedAuthority: envelope.authority,
            });
          }
          if (existingReceipt.value.status === "accepted") {
            if (envelope.command.type === "thread.verification-result.capture") {
              const sequence = yield* validateVerificationResultCaptureReplay(
                envelope.command,
                existingReceipt.value.resultSequence,
              );
              return { sequence };
            }
            if (envelope.initialPlanning !== undefined) {
              const sequence = yield* validateInitialPlanningTurnReplay(
                envelope.command,
                envelope.initialPlanning,
              );
              return { sequence };
            }
            if (envelope.implementation !== undefined) {
              const sequence = yield* validateImplementationTurnReplay(
                envelope.command,
                envelope.implementation,
              );
              return { sequence };
            }
            if (envelope.verification !== undefined) {
              const sequence = yield* validateVerificationTurnReplay(
                envelope.command,
                envelope.verification,
              );
              return { sequence };
            }
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }
        if (verificationRuntimeFragmentSequence !== null) {
          return yield* new OrchestrationCommandIdentityConflictError({
            commandId: envelope.command.commandId,
            commandType: envelope.command.type,
          });
        }

        const decisionReadModel =
          envelope.implementation === undefined && envelope.verification === undefined
            ? commandReadModel
            : yield* projectionSnapshotQuery.getCommandReadModel();
        const eventBase = yield* decideOrchestrationCommand({
          command: envelope.command,
          readModel: decisionReadModel,
          authority: envelope.authority,
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.provideService(OrchestrationEnginePublicationHooks, publicationHooks),
          Effect.mapError((cause) =>
            isOrchestrationCommandInvariantError(cause)
              ? cause
              : new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Failed to generate an event identifier.",
                  cause,
                }),
          ),
        );
        const decidedEventBases = Array.isArray(eventBase) ? eventBase : [eventBase];
        const initialPlanning = envelope.initialPlanning;
        const implementation = envelope.implementation;
        const verification = envelope.verification;
        const durableAgentControlTurn = initialPlanning ?? implementation ?? verification;
        const eventBases =
          durableAgentControlTurn === undefined
            ? decidedEventBases
            : decidedEventBases.map((event, index) =>
                index === 0
                  ? {
                      ...event,
                      eventId: EventId.make(durableAgentControlTurn.messageEventId),
                    }
                  : {
                      ...event,
                      eventId: EventId.make(durableAgentControlTurn.turnRequestEventId),
                      causationEventId: EventId.make(durableAgentControlTurn.messageEventId),
                    },
              );
        const runtimeEventAuthorityObservation =
          envelope.command.type === "thread.verification-result.capture"
            ? {
                runtimeEventId: envelope.command.providerRuntimeMessage.runtimeEventId,
                commandId: envelope.command.commandId,
                threadId: envelope.command.threadId,
                fragmentKind: envelope.command.fragment.kind,
              }
            : null;
        const captureTransactionHooks =
          runtimeEventAuthorityObservation === null
            ? nodeSqliteTransactionHooks
            : {
                ...nodeSqliteTransactionHooks,
                afterAnyCommitBeforeReturn: () =>
                  Effect.sync(() => {
                    verificationCaptureCommitObserved = true;
                  }).pipe(
                    Effect.andThen(
                      nodeSqliteTransactionHooks.afterAnyCommitBeforeReturn?.() ?? Effect.void,
                    ),
                  ),
              };
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const committedEvents: OrchestrationEvent[] = [];
              let nextCommandReadModel = decisionReadModel;

              if (envelope.command.type === "thread.verification-result.capture") {
                const capture = envelope.command.verificationResultCapture;
                const sealed = yield* sql<{ readonly count: number }>`
                  SELECT count(*) AS count
                  FROM main.orchestration_events
                  WHERE stream_id = ${envelope.command.threadId}
                    AND event_type = 'thread.session-set'
                    AND json_extract(metadata_json, '$.verificationResultSource.handoffId')
                      IS ${capture.handoffId}
                    AND json_extract(
                      metadata_json, '$.verificationResultSource.providerDeliveryId'
                    ) IS ${capture.providerDeliveryId}
                    AND json_extract(
                      metadata_json, '$.verificationResultSource.providerInstanceId'
                    ) IS ${capture.providerInstanceId}
                    AND json_extract(
                      metadata_json, '$.verificationResultSource.providerTurnId'
                    ) IS ${capture.providerTurnId}
                    AND json_extract(
                      metadata_json, '$.verificationResultSource.resultSchemaFingerprint'
                    ) IS ${capture.resultSchemaFingerprint}
                `;
                if (sealed[0]?.count !== 0) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: envelope.command.type,
                    detail: "Verification result capture is sealed.",
                  });
                }
                yield* verificationResultRuntimeEventAuthorityHooks
                  .beforeAuthorityWrite(runtimeEventAuthorityObservation!)
                  .pipe(
                    Effect.mapError((error) =>
                      isVerificationResultRuntimeEventAuthorityRace(error)
                        ? new VerificationResultRuntimeEventAuthorityRaceError(error)
                        : error,
                    ),
                  );
              }

              if (
                envelope.command.type === "thread.session.set" &&
                envelope.command.verificationResultSource !== undefined
              ) {
                const seal = envelope.command.verificationResultSource;
                const source = yield* loadSealableVerificationResultSource(sql, {
                  threadId: envelope.command.threadId,
                  providerInstanceId: seal.providerInstanceId,
                  providerTurnId: seal.providerTurnId,
                  afterStreamVersion: 4,
                  handoffId: seal.handoffId,
                  providerDeliveryId: seal.providerDeliveryId,
                  resultSchemaFingerprint: seal.resultSchemaFingerprint,
                }).pipe(
                  Effect.mapError(
                    (cause) =>
                      new OrchestrationCommandInvariantError({
                        commandType: envelope.command.type,
                        detail: "Verification result source could not be sealed.",
                        cause,
                      }),
                  ),
                );
                if (
                  seal.sourceDisposition !== source.sourceDisposition ||
                  seal.finalMessageId !== source.finalMessageId ||
                  seal.sourceEventId !== source.sourceEventId ||
                  seal.outputDigest !== source.outputDigest ||
                  seal.outputByteLength !== source.outputByteLength
                ) {
                  return yield* new OrchestrationCommandInvariantError({
                    commandType: envelope.command.type,
                    detail: "Verification result source changed before the seal write.",
                  });
                }
              }

              for (const nextEvent of eventBases) {
                const persistedEvent =
                  durableAgentControlTurn === undefined
                    ? nextEvent
                    : {
                        ...nextEvent,
                        payload: parseJsonStrict(canonicalJson(nextEvent.payload as JsonValue)),
                        metadata: parseJsonStrict(
                          canonicalJson(nextEvent.metadata as JsonValue),
                        ) as { readonly [key: string]: JsonValue },
                      };
                const savedEvent = yield* eventStore
                  .append(persistedEvent)
                  .pipe(
                    Effect.mapError((error) =>
                      runtimeEventAuthorityObservation !== null &&
                      isPersistenceSqlError(error) &&
                      isVerificationResultRuntimeEventAuthorityRace(error)
                        ? new VerificationResultRuntimeEventAuthorityRaceError(error)
                        : error,
                    ),
                  );
                nextCommandReadModel = yield* projectEvent(nextCommandReadModel, savedEvent);
                yield* projectionPipeline.projectEvent(savedEvent);
                committedEvents.push(savedEvent);
              }

              const lastSavedEvent = committedEvents.at(-1) ?? null;
              if (lastSavedEvent === null) {
                return yield* new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Command produced no events.",
                });
              }

              yield* commandReceiptRepository.insert({
                commandId: envelope.command.commandId,
                authority: envelope.authority,
                aggregateKind: lastSavedEvent.aggregateKind,
                aggregateId: lastSavedEvent.aggregateId,
                acceptedAt: lastSavedEvent.occurredAt,
                resultSequence: lastSavedEvent.sequence,
                status: "accepted",
                error: null,
              });

              if (envelope.initialPlanning !== undefined) {
                const messageEvent = committedEvents[0];
                const turnRequestEvent = committedEvents[1];
                if (
                  envelope.command.type !== "thread.turn.start" ||
                  messageEvent?.type !== "thread.message-sent" ||
                  turnRequestEvent?.type !== "thread.turn-start-requested" ||
                  committedEvents.length !== 2
                ) {
                  return yield* initialPlanningError(
                    "Initial Planning turn decision produced an invalid event family.",
                  );
                }
                const messageEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: messageEvent.sequence,
                  streamVersion: 3,
                  eventId: messageEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.message-sent",
                  occurredAt: messageEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: null,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: messageEvent.payload as JsonValue,
                  metadata: messageEvent.metadata as { readonly [key: string]: JsonValue },
                });
                const turnRequestEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: turnRequestEvent.sequence,
                  streamVersion: 4,
                  eventId: turnRequestEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.turn-start-requested",
                  occurredAt: turnRequestEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: messageEvent.eventId,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: turnRequestEvent.payload as JsonValue,
                  metadata: turnRequestEvent.metadata as {
                    readonly [key: string]: JsonValue;
                  },
                });
                const eventEvidenceDigest = combinedInitialPlanningEventDigest(
                  messageEventEnvelopeJson,
                  turnRequestEventEnvelopeJson,
                );
                yield* sql`
                  INSERT INTO agent_control_initial_planning_turn_accepted (
                    handoff_id, handoff_fingerprint,
                    controlled_thread_reservation_id, thread_id,
                    turn_request_command_id, message_id,
                    message_event_id, message_event_sequence,
                    turn_request_event_id, turn_request_event_sequence,
                    message_event_envelope_json, turn_request_event_envelope_json,
                    event_evidence_digest,
                    receipt_authority, accepted_at
                  ) VALUES (
                    ${envelope.initialPlanning.handoffId},
                    ${envelope.initialPlanning.handoffFingerprint},
                    ${envelope.initialPlanning.controlledThreadReservationId},
                    ${envelope.initialPlanning.threadId},
                    ${envelope.initialPlanning.turnRequestCommandId},
                    ${envelope.initialPlanning.messageId},
                    ${messageEvent.eventId}, CAST(${messageEvent.sequence} AS INTEGER),
                    ${turnRequestEvent.eventId},
                    CAST(${turnRequestEvent.sequence} AS INTEGER),
                    ${messageEventEnvelopeJson}, ${turnRequestEventEnvelopeJson},
                    ${eventEvidenceDigest},
                    'agent-control', ${turnRequestEvent.occurredAt}
                  )
                `;
              }
              if (envelope.implementation !== undefined) {
                const messageEvent = committedEvents[0];
                const turnRequestEvent = committedEvents[1];
                if (
                  envelope.command.type !== "thread.turn.start" ||
                  messageEvent?.type !== "thread.message-sent" ||
                  turnRequestEvent?.type !== "thread.turn-start-requested" ||
                  committedEvents.length !== 2
                ) {
                  return yield* implementationError(
                    "Implementation turn decision produced an invalid event family.",
                  );
                }
                const messageEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: messageEvent.sequence,
                  streamVersion: 3,
                  eventId: messageEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.message-sent",
                  occurredAt: messageEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: null,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: messageEvent.payload as JsonValue,
                  metadata: messageEvent.metadata as { readonly [key: string]: JsonValue },
                });
                const turnRequestEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: turnRequestEvent.sequence,
                  streamVersion: 4,
                  eventId: turnRequestEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.turn-start-requested",
                  occurredAt: turnRequestEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: messageEvent.eventId,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: turnRequestEvent.payload as JsonValue,
                  metadata: turnRequestEvent.metadata as {
                    readonly [key: string]: JsonValue;
                  },
                });
                const eventEvidenceDigest = combinedInitialPlanningEventDigest(
                  messageEventEnvelopeJson,
                  turnRequestEventEnvelopeJson,
                );
                yield* sql`
                  INSERT INTO agent_control_implementation_turn_accepted (
                    handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
                    thread_id, planning_thread_id, plan_id, turn_request_command_id,
                    message_id, message_event_id, message_event_sequence,
                    turn_request_event_id, turn_request_event_sequence,
                    message_event_envelope_json, turn_request_event_envelope_json,
                    event_evidence_digest, receipt_authority, accepted_at
                  ) VALUES (
                    ${envelope.implementation.handoffId},
                    ${envelope.implementation.handoffFingerprint},
                    ${envelope.implementation.controlledThreadReservationId},
                    ${envelope.implementation.threadId},
                    ${envelope.implementation.planningThreadId},
                    ${envelope.implementation.planId},
                    ${envelope.implementation.turnRequestCommandId},
                    ${envelope.implementation.messageId},
                    ${messageEvent.eventId}, CAST(${messageEvent.sequence} AS INTEGER),
                    ${turnRequestEvent.eventId}, CAST(${turnRequestEvent.sequence} AS INTEGER),
                    ${messageEventEnvelopeJson}, ${turnRequestEventEnvelopeJson},
                    ${eventEvidenceDigest}, 'agent-control', ${turnRequestEvent.occurredAt}
                  )
                `;
              }
              if (envelope.verification !== undefined) {
                const messageEvent = committedEvents[0];
                const turnRequestEvent = committedEvents[1];
                if (
                  envelope.command.type !== "thread.turn.start" ||
                  messageEvent?.type !== "thread.message-sent" ||
                  turnRequestEvent?.type !== "thread.turn-start-requested" ||
                  committedEvents.length !== 2
                ) {
                  return yield* verificationError(
                    "Verification turn decision produced an invalid event family.",
                  );
                }
                const messageEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: messageEvent.sequence,
                  streamVersion: 3,
                  eventId: messageEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.message-sent",
                  occurredAt: messageEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: null,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: messageEvent.payload as JsonValue,
                  metadata: messageEvent.metadata as { readonly [key: string]: JsonValue },
                });
                const turnRequestEventEnvelopeJson = canonicalInitialPlanningEventEnvelope({
                  sequence: turnRequestEvent.sequence,
                  streamVersion: 4,
                  eventId: turnRequestEvent.eventId,
                  aggregateKind: "thread",
                  aggregateId: envelope.command.threadId,
                  type: "thread.turn-start-requested",
                  occurredAt: turnRequestEvent.occurredAt,
                  commandId: envelope.command.commandId,
                  causationEventId: messageEvent.eventId,
                  correlationId: envelope.command.commandId,
                  actorKind: "client",
                  payload: turnRequestEvent.payload as JsonValue,
                  metadata: turnRequestEvent.metadata as {
                    readonly [key: string]: JsonValue;
                  },
                });
                const eventEvidenceDigest = combinedInitialPlanningEventDigest(
                  messageEventEnvelopeJson,
                  turnRequestEventEnvelopeJson,
                );
                yield* sql`
                  INSERT INTO agent_control_verification_turn_accepted (
                    handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
                    thread_id, planning_thread_id, plan_id, turn_request_command_id,
                    message_id, message_event_id, message_event_sequence,
                    turn_request_event_id, turn_request_event_sequence,
                    message_event_envelope_json, turn_request_event_envelope_json,
                    event_evidence_digest, receipt_authority, accepted_at
                  ) VALUES (
                    ${envelope.verification.handoffId},
                    ${envelope.verification.handoffFingerprint},
                    ${envelope.verification.controlledThreadReservationId},
                    ${envelope.verification.threadId},
                    ${envelope.verification.planningThreadId},
                    ${envelope.verification.planId},
                    ${envelope.verification.turnRequestCommandId},
                    ${envelope.verification.messageId},
                    ${messageEvent.eventId}, CAST(${messageEvent.sequence} AS INTEGER),
                    ${turnRequestEvent.eventId}, CAST(${turnRequestEvent.sequence} AS INTEGER),
                    ${messageEventEnvelopeJson}, ${turnRequestEventEnvelopeJson},
                    ${eventEvidenceDigest}, 'agent-control', ${turnRequestEvent.occurredAt}
                  )
                `;
              }

              if (runtimeEventAuthorityObservation !== null) {
                runtimeEventAuthorityTransactionBodyCompleted = true;
              }
              const transactionResult = {
                committedEvents,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
              } as const;
              if (runtimeEventAuthorityObservation !== null) {
                verificationCaptureTransactionResult = transactionResult;
              }
              return transactionResult;
            }),
          )
          .pipe(
            Effect.provideService(NodeSqliteTransactionHooks, captureTransactionHooks),
            Effect.catchTag("SqlError", (sqlError) => {
              const persistenceError = toPersistenceSqlError(
                "OrchestrationEngine.processEnvelope:transaction",
              )(sqlError);
              return Effect.fail(
                runtimeEventAuthorityObservation !== null &&
                  runtimeEventAuthorityTransactionBodyCompleted &&
                  !verificationCaptureCommitObserved &&
                  isVerificationResultRuntimeEventAuthorityRace(persistenceError)
                  ? new VerificationResultRuntimeEventAuthorityRaceError(persistenceError)
                  : persistenceError,
              );
            }),
            Effect.catchTag(
              "VerificationResultRuntimeEventAuthorityRaceError",
              ({ originalError }) =>
                Effect.gen(function* () {
                  runtimeEventAuthorityRaceDetected = true;
                  if (
                    envelope.command.type !== "thread.verification-result.capture" ||
                    runtimeEventAuthorityObservation === null
                  ) {
                    return yield* originalError;
                  }
                  yield* verificationResultRuntimeEventAuthorityHooks.beforeCommittedWinnerRead(
                    runtimeEventAuthorityObservation,
                    originalError,
                  );
                  const committedWinner = yield* sql
                    .withTransaction(
                      loadCommittedVerificationResultCaptureRuntimeFragment(
                        envelope.command,
                        envelope.authority,
                      ),
                    )
                    .pipe(
                      Effect.catchTag("SqlError", (sqlError) =>
                        Effect.fail(
                          toPersistenceSqlError(
                            "OrchestrationEngine.processEnvelope:runtimeEventAuthorityRead",
                          )(sqlError),
                        ),
                      ),
                    );
                  if (committedWinner === null) {
                    return yield* originalError;
                  }
                  return {
                    committedEvents: [],
                    lastSequence: committedWinner,
                    nextCommandReadModel: decisionReadModel,
                  } as const;
                }),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* publishDomainEvent(event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(Effect.withSpan(`orchestration.command.${envelope.command.type}`)),
    ).pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            yield* Deferred.succeed(envelope.result, exit.value);
            return;
          }

          if (
            envelope.command.type === "thread.verification-result.capture" &&
            verificationCaptureCommitObserved &&
            verificationCaptureTransactionResult !== null
          ) {
            commandReadModel = verificationCaptureTransactionResult.nextCommandReadModel;
            yield* Effect.forEach(
              verificationCaptureTransactionResult.committedEvents,
              publishDomainEvent,
              { concurrency: 1, discard: true },
            );
            yield* Deferred.failCause(
              envelope.result,
              exit.cause as Cause.Cause<OrchestrationDispatchError>,
            );
            return;
          }

          if (envelope.command.type === "thread.verification-result.capture") {
            yield* Deferred.failCause(
              envelope.result,
              exit.cause as Cause.Cause<OrchestrationDispatchError>,
            );
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (
            !runtimeEventAuthorityRaceDetected &&
            envelope.initialPlanning === undefined &&
            envelope.implementation === undefined &&
            envelope.verification === undefined &&
            !isOrchestrationCommandPreviouslyRejectedError(error)
          ) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: commandReadModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (
              isOrchestrationCommandInvariantError(error) &&
              !isAgentControlReservedThreadCreate(envelope.command) &&
              !isAgentControlThreadMaterializeCommand(envelope.command)
            ) {
              yield* commandReceiptRepository
                .insert({
                  commandId: envelope.command.commandId,
                  authority: envelope.authority,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: yield* nowIso,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.catch(() => Effect.void));
            }
          }

          yield* Deferred.fail(envelope.result, error);
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();

  const worker = Effect.forever(Queue.take(commandQueue).pipe(Effect.flatMap(processEnvelope)));
  yield* Effect.forkScoped(worker);
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: commandReadModel.snapshotSequence }),
  );

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive, limit) =>
    eventStore.readFromSequence(fromSequenceExclusive, limit);

  const dispatchWithAuthority = (
    authority: OrchestrationCommandAuthority,
    command: OrchestrationCommand,
    durableTurn?:
      | {
          readonly kind: "initial-planning";
          readonly evidence: AgentControlInitialPlanningTurnDispatchEvidence;
        }
      | {
          readonly kind: "implementation";
          readonly evidence: AgentControlImplementationTurnDispatchEvidence;
        }
      | {
          readonly kind: "verification";
          readonly evidence: AgentControlVerificationTurnDispatchEvidence;
        },
  ) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      const cancelled = yield* Deferred.make<void>();
      yield* Queue.offer(commandQueue, {
        command,
        authority,
        ...(durableTurn?.kind === "initial-planning"
          ? { initialPlanning: durableTurn.evidence }
          : {}),
        ...(durableTurn?.kind === "implementation" ? { implementation: durableTurn.evidence } : {}),
        ...(durableTurn?.kind === "verification" ? { verification: durableTurn.evidence } : {}),
        result,
        cancelled,
        startedAtMs: yield* Clock.currentTimeMillis,
      });
      return yield* Deferred.await(result).pipe(
        Effect.onInterrupt(() =>
          isAgentControlThreadMaterializeCommand(command)
            ? Deferred.succeed(cancelled, undefined).pipe(
                Effect.andThen(Deferred.await(result).pipe(Effect.exit)),
                Effect.asVoid,
              )
            : Effect.void,
        ),
      );
    });

  const dispatch: OrchestrationEngineShape["dispatch"] = (command) =>
    dispatchWithAuthority("system", command);
  const dispatchClient: OrchestrationEngineShape["dispatchClient"] = (command) =>
    dispatchWithAuthority("client", command);
  const dispatchAgentControl: OrchestrationEngineShape["dispatchAgentControl"] = (command) =>
    dispatchWithAuthority("agent-control", command);
  const dispatchAgentControlInitialPlanningTurn: NonNullable<
    OrchestrationEngineShape["dispatchAgentControlInitialPlanningTurn"]
  > = (command, evidence) =>
    dispatchWithAuthority("agent-control", command, {
      kind: "initial-planning",
      evidence,
    }).pipe(
      Effect.catch((originalError) =>
        validateInitialPlanningTurnReplay(command, evidence).pipe(
          Effect.map((sequence) => ({ sequence })),
          Effect.mapError(() => originalError),
        ),
      ),
    );
  const dispatchAgentControlImplementationTurn: NonNullable<
    OrchestrationEngineShape["dispatchAgentControlImplementationTurn"]
  > = (command, evidence) =>
    dispatchWithAuthority("agent-control", command, {
      kind: "implementation",
      evidence,
    }).pipe(
      Effect.catch((originalError) =>
        validateImplementationTurnReplay(command, evidence).pipe(
          Effect.map((sequence) => ({ sequence })),
          Effect.mapError(() => originalError),
        ),
      ),
    );
  const dispatchAgentControlVerificationTurn: NonNullable<
    OrchestrationEngineShape["dispatchAgentControlVerificationTurn"]
  > = (command, evidence) =>
    dispatchWithAuthority("agent-control", command, {
      kind: "verification",
      evidence,
    }).pipe(
      Effect.catch((originalError) =>
        validateVerificationTurnReplay(command, evidence).pipe(
          Effect.map((sequence) => ({ sequence })),
          Effect.mapError(() => originalError),
        ),
      ),
    );

  return {
    readEvents,
    dispatch,
    dispatchClient,
    dispatchAgentControl,
    dispatchAgentControlInitialPlanningTurn,
    dispatchAgentControlImplementationTurn,
    dispatchAgentControlVerificationTurn,
    materializeAgentControlInTransaction,
    completeAgentControlMaterializationInTransaction,
    replayAgentControlMaterialization,
    refreshAgentControlMaterialization,
    publishAgentControlMaterialization,
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
    // The command read model's snapshotSequence tracks the latest committed
    // event sequence (updated on the worker fiber). A plain property read is a
    // consistent, committed value — reassignment of `commandReadModel` is
    // atomic on the single-threaded event loop.
    latestSequence: Effect.sync(() => commandReadModel.snapshotSequence),
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
