import type {
  AgentControlThreadMaterializeCommand,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import {
  EventId,
  OrchestrationCommand,
  OrchestrationEvent as OrchestrationEventSchema,
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
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
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
  AgentControlThreadMaterializationConvergencePolicy,
  AgentControlThreadMaterializationTransactionHooks,
} from "../Services/AgentControlThreadMaterializationTransactionHooks.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  OrchestrationEngineService,
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

const isRetryableSqliteConflict = (error: PersistenceSqlError): boolean => {
  const seen = new Set<unknown>();
  const visit = (cause: unknown): boolean => {
    if (cause === null || cause === undefined || seen.has(cause)) {
      return false;
    }
    seen.add(cause);
    if (isSqlError(cause) && cause.isRetryable) {
      return true;
    }
    if (typeof cause !== "object") {
      return false;
    }
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

const evidenceError = (issue: string, threadId: ThreadId) =>
  new PersistenceDecodeError({
    operation: "OrchestrationEngine.materializationEvidence",
    issue,
    correlation: { threadId },
  });

interface CommandEnvelope {
  command: OrchestrationCommand;
  authority: OrchestrationCommandAuthority;
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

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

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
      FROM orchestration_events
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
      FROM orchestration_events
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
        yield* PubSub.publish(eventPubSub, event);
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
          isPersistenceSqlError(error) && isRetryableSqliteConflict(error),
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
        yield* PubSub.publish(eventPubSub, persistedEvent);
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
            yield* PubSub.publish(eventPubSub, event);
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
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        const eventBase = yield* decideOrchestrationCommand({
          command: envelope.command,
          readModel: commandReadModel,
          authority: envelope.authority,
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
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
        const eventBases = Array.isArray(eventBase) ? eventBase : [eventBase];
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const committedEvents: OrchestrationEvent[] = [];
              let nextCommandReadModel = commandReadModel;

              for (const nextEvent of eventBases) {
                const savedEvent = yield* eventStore.append(nextEvent);
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

              return {
                committedEvents,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
              } as const;
            }),
          )
          .pipe(
            Effect.catchTag("SqlError", (sqlError) =>
              Effect.fail(
                toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
              ),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
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

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (!isOrchestrationCommandPreviouslyRejectedError(error)) {
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
  ) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      const cancelled = yield* Deferred.make<void>();
      yield* Queue.offer(commandQueue, {
        command,
        authority,
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

  return {
    readEvents,
    dispatch,
    dispatchClient,
    dispatchAgentControl,
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
