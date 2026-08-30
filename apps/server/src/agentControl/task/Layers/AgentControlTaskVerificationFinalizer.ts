import * as NodeCrypto from "node:crypto";

import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import {
  AgentControlTaskFinalizedAfterVerificationPayload,
  AgentControlTaskEvent,
  AgentControlTaskId,
  AgentControlStageRunLeaseReleasedAfterVerificationPayloadStorage,
  AgentControlStageRunVerificationTerminalPayloadStorage,
  CommandId,
  EventId,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  AgentControlVerificationStageFinalizationDocumentStorage,
  type AgentControlTaskEventDraft,
  type AgentControlTaskFinalizedAfterVerificationPayload as TaskFinalizationPayload,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FiberMap from "effect/FiberMap";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  parseJsonStrict,
  sha256Utf8,
  type JsonValue,
} from "../../initialPlanning/eventEvidence.ts";
import { fingerprintAgentControlSourceIdentity } from "../../stageRun/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { NodeSqliteTransactionHooks } from "../../../persistence/Services/NodeSqliteTransactionHooks.ts";
import {
  deriveVerificationFinalizationCommandId,
  deriveVerificationFinalizationEvidenceId,
  deriveVerificationFinalizationMarkerId,
  deriveVerificationFinalizationReceiptId,
  deriveVerificationLeaseReleaseEventId,
  deriveVerificationTerminalStageEventId,
  fingerprintVerificationTurn,
} from "../../verificationTurn/identity.ts";
import { projectAgentControlTaskEvent } from "../projector.ts";
import { decodeAgentControlTaskProjectionRow } from "./AgentControlTaskStateRepository.ts";
import {
  AgentControlTaskVerificationFinalizer,
  AgentControlTaskVerificationFinalizerError,
  type AgentControlTaskVerificationFinalizationPublication,
  type AgentControlTaskVerificationFinalizerShape,
} from "../Services/AgentControlTaskVerificationFinalizer.ts";
import { AgentControlTaskVerificationFinalizerHooks } from "../Services/AgentControlTaskVerificationFinalizerHooks.ts";
import { AgentControlTaskEngine } from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskProjection } from "../Services/AgentControlTaskProjection.ts";

export const TASK_VERIFICATION_FINALIZATION_CANDIDATES_SQL = `
  SELECT marker.handoff_id AS "handoffId"
  FROM main.agent_control_verification_finalization_markers marker
  CROSS JOIN main.agent_control_verification_finalization_evidence evidence
    ON evidence.finalization_evidence_id = marker.finalization_evidence_id
   AND evidence.marker_id = marker.marker_id
   AND evidence.receipt_id = marker.receipt_id
  CROSS JOIN main.agent_control_verification_finalization_receipts receipt
    ON receipt.receipt_id = marker.receipt_id
   AND receipt.finalization_evidence_id = evidence.finalization_evidence_id
   AND receipt.status = 'accepted'
  WHERE marker.handoff_id > ?
    AND NOT EXISTS (
      SELECT 1
      FROM main.agent_control_task_verification_finalization_markers task_marker
      WHERE task_marker.verification_marker_id = marker.marker_id
    )
  ORDER BY marker.handoff_id
  LIMIT ?
`;

export const TASK_VERIFICATION_FINALIZATION_PUBLICATION_RECOVERY_SQL = `
  SELECT publication.handoff_id AS "handoffId"
  FROM main.agent_control_task_verification_finalization_publications publication
  CROSS JOIN main.agent_control_task_verification_finalization_markers marker
    ON marker.marker_id = publication.marker_id
   AND marker.task_finalization_evidence_id = publication.task_finalization_evidence_id
   AND marker.handoff_id = publication.handoff_id
   AND marker.task_id = publication.task_id
   AND marker.task_event_id = publication.task_event_id
   AND marker.task_event_stream_version = publication.task_event_stream_version
  WHERE publication.status IN ('pending', 'claimed')
    AND publication.handoff_id > ?
  ORDER BY publication.handoff_id
  LIMIT ?
`;

export const TASK_VERIFICATION_FINALIZATION_PUBLICATION_LEASE_MILLIS = 30_000;

const SourceRow = Schema.Struct({
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  projectId: Schema.String,
  taskId: Schema.String,
  verificationTaskRevision: Schema.Int,
  githubIntakeSequence: Schema.Int,
  sourceIdentityFingerprint: Schema.String,
  verificationEvidenceId: Schema.String,
  verificationReceiptId: Schema.String,
  verificationMarkerId: Schema.String,
  verificationCommandId: Schema.String,
  verificationFingerprint: Schema.String,
  verificationMarkerFingerprint: Schema.String,
  finalizationJsonBytes: Schema.Unknown,
  deliveryTerminalState: Schema.String,
  verificationOutcome: Schema.String,
  terminalCause: Schema.String,
  terminalRuntimeEventId: Schema.String,
  evaluationAuthority: Schema.String,
  evaluationId: Schema.NullOr(Schema.String),
  evaluationEvidenceId: Schema.NullOr(Schema.String),
  evaluationReceiptId: Schema.NullOr(Schema.String),
  evaluationMarkerId: Schema.NullOr(Schema.String),
  evaluationDisposition: Schema.NullOr(Schema.String),
  verificationVerdict: Schema.NullOr(Schema.String),
  invalidOutputCode: Schema.NullOr(Schema.String),
  stageRunId: Schema.String,
  stageEventId: Schema.String,
  stageEventSequence: Schema.Int,
  stageEventStreamVersion: Schema.Int,
  stageEventType: Schema.String,
  stageOccurredAt: Schema.String,
  stageCommandId: Schema.String,
  stageCausationEventId: Schema.NullOr(Schema.String),
  stageCorrelationId: Schema.String,
  stageAuthority: Schema.String,
  stagePayloadBytes: Schema.Unknown,
  stageMetadataBytes: Schema.Unknown,
  leaseId: Schema.String,
  leaseEventId: Schema.String,
  leaseEventSequence: Schema.Int,
  leaseEventStreamVersion: Schema.Int,
  leaseEventType: Schema.String,
  leaseOccurredAt: Schema.String,
  leaseCommandId: Schema.String,
  leaseCausationEventId: Schema.NullOr(Schema.String),
  leaseCorrelationId: Schema.String,
  leaseAuthority: Schema.String,
  leasePayloadBytes: Schema.Unknown,
  leaseMetadataBytes: Schema.Unknown,
  finalizedAt: Schema.String,
});
const decodeSourceRow = Schema.decodeUnknownEffect(SourceRow);
const decodeVerificationDocument = Schema.decodeUnknownEffect(
  AgentControlVerificationStageFinalizationDocumentStorage,
);
const decodeTaskPayload = Schema.decodeUnknownEffect(
  AgentControlTaskFinalizedAfterVerificationPayload,
);
const decodeTaskEvent = Schema.decodeUnknownEffect(
  AgentControlTaskEvent.annotate({ parseOptions: { onExcessProperty: "error" } }),
);
const decodeVerificationStagePayload = Schema.decodeUnknownEffect(
  AgentControlStageRunVerificationTerminalPayloadStorage,
);
const decodeVerificationLeasePayload = Schema.decodeUnknownEffect(
  AgentControlStageRunLeaseReleasedAfterVerificationPayloadStorage,
);
const isFinalizerError = Schema.is(AgentControlTaskVerificationFinalizerError);

const TaskMetadata = Schema.Struct({ schemaVersion: Schema.Literal(1) }).annotate({
  parseOptions: { onExcessProperty: "error" },
});
const decodeTaskMetadata = Schema.decodeUnknownEffect(TaskMetadata);

const TaskAuthorityEventRow = Schema.Struct({
  sequenceStorageClass: Schema.Literal("integer"),
  sequence: PositiveInt,
  eventIdStorageClass: Schema.Literal("text"),
  eventId: EventId,
  typeStorageClass: Schema.Literal("text"),
  type: Schema.String,
  aggregateKindStorageClass: Schema.Literal("text"),
  aggregateKind: Schema.String,
  aggregateIdStorageClass: Schema.Literal("text"),
  aggregateId: Schema.String,
  streamVersionStorageClass: Schema.Literal("integer"),
  streamVersion: PositiveInt,
  occurredAtStorageClass: Schema.Literal("text"),
  occurredAt: IsoDateTime,
  commandIdStorageClass: Schema.Literal("text"),
  commandId: Schema.String,
  causationEventIdStorageClass: Schema.Literals(["null", "text"]),
  causationEventId: Schema.NullOr(Schema.String),
  correlationIdStorageClass: Schema.Literal("text"),
  correlationId: Schema.String,
  authorityStorageClass: Schema.Literal("text"),
  authority: Schema.String,
  payloadStorageClass: Schema.Literal("text"),
  payloadBytes: Schema.Unknown,
  metadataStorageClass: Schema.Literal("text"),
  metadataBytes: Schema.Unknown,
});
const decodeTaskAuthorityEventRow = Schema.decodeUnknownEffect(TaskAuthorityEventRow);

const PublicationStateRow = Schema.Struct({
  handoffIdStorageClass: Schema.Literal("text"),
  handoffId: Schema.String,
  markerIdStorageClass: Schema.Literal("text"),
  markerId: Schema.String,
  evidenceIdStorageClass: Schema.Literal("text"),
  evidenceId: Schema.String,
  taskIdStorageClass: Schema.Literal("text"),
  taskId: AgentControlTaskId,
  taskEventIdStorageClass: Schema.Literal("text"),
  taskEventId: EventId,
  taskEventStreamVersionStorageClass: Schema.Literal("integer"),
  taskEventStreamVersion: PositiveInt,
  ownerIdStorageClass: Schema.Literals(["null", "text"]),
  ownerId: Schema.NullOr(Schema.String),
  statusStorageClass: Schema.Literal("text"),
  status: Schema.Literals(["pending", "claimed", "completed"]),
  revisionStorageClass: Schema.Literal("integer"),
  revision: PositiveInt,
  claimFenceStorageClass: Schema.Literal("integer"),
  claimFence: NonNegativeInt,
  createdAtStorageClass: Schema.Literal("text"),
  createdAt: IsoDateTime,
  claimedAtStorageClass: Schema.Literals(["null", "text"]),
  claimedAt: Schema.NullOr(IsoDateTime),
  leaseExpiresAtStorageClass: Schema.Literals(["null", "text"]),
  leaseExpiresAt: Schema.NullOr(IsoDateTime),
  completedAtStorageClass: Schema.Literals(["null", "text"]),
  completedAt: Schema.NullOr(IsoDateTime),
});
const decodePublicationStateRow = Schema.decodeUnknownEffect(PublicationStateRow);

interface VerificationSourceAuthority {
  readonly row: typeof SourceRow.Type;
  readonly document: AgentControlVerificationStageFinalizationDocumentStorage;
}

interface BuiltFinalization {
  readonly commandId: CommandId;
  readonly evidenceId: string;
  readonly receiptId: string;
  readonly markerId: string;
  readonly eventId: EventId;
  readonly finalizationJson: string;
  readonly finalizationFingerprint: string;
  readonly markerFingerprint: string;
  readonly payload: TaskFinalizationPayload;
}

const taskIdentity = (prefix: string, domain: string, parts: ReadonlyArray<string>) =>
  `${prefix}-${sha256Utf8(
    canonicalJson({ domain: `agent-control-task-${domain}-v1`, parts } as unknown as JsonValue),
  )}`;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const taskEvents = yield* AgentControlTaskEventStore;
  const taskProjection = yield* AgentControlTaskProjection;
  const taskEngine = yield* AgentControlTaskEngine;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const hooks = yield* AgentControlTaskVerificationFinalizerHooks;
  const publicationOwnerId = hooks.publicationOwnerId ?? NodeCrypto.randomUUID();
  if (publicationOwnerId.length === 0) {
    return yield* Effect.die(new Error("task Verification publication owner must not be empty"));
  }
  const publicationLeaseDurationMillis =
    hooks.publicationLeaseDurationMillis ?? TASK_VERIFICATION_FINALIZATION_PUBLICATION_LEASE_MILLIS;
  if (
    !Number.isSafeInteger(publicationLeaseDurationMillis) ||
    publicationLeaseDurationMillis <= 0
  ) {
    return yield* Effect.die(
      new Error("task Verification publication lease duration must be a positive integer"),
    );
  }

  const publicationTime = Effect.fn("AgentControlTaskVerificationFinalizer.publicationTime")(
    function* () {
      const epochMillis = yield* Clock.currentTimeMillis;
      if (!Number.isSafeInteger(epochMillis)) {
        return yield* Effect.die(
          new Error("task Verification publication clock must return integer milliseconds"),
        );
      }
      const claimedAt = DateTime.makeUnsafe(epochMillis);
      return {
        claimedAt: DateTime.formatIso(claimedAt),
        leaseExpiresAt: DateTime.formatIso(
          DateTime.add(claimedAt, { milliseconds: publicationLeaseDurationMillis }),
        ),
      } as const;
    },
  );

  const error = (
    handoffId: string,
    operation: string,
    reason: AgentControlTaskVerificationFinalizerError["reason"],
    cause?: unknown,
  ) =>
    new AgentControlTaskVerificationFinalizerError({
      handoffId,
      operation,
      reason,
      ...(cause === undefined ? {} : { cause }),
    });

  const decodeCanonical = Effect.fn("AgentControlTaskVerificationFinalizer.decodeCanonical")(
    function* (handoffId: string, operation: string, raw: unknown) {
      return yield* Effect.try({
        try: () => {
          const source = decodeCanonicalUtf8Bytes(raw);
          return { source, value: parseCanonicalJson(source) };
        },
        catch: (cause) => error(handoffId, operation, "authority-conflict", cause),
      });
    },
  );

  const decodeStoredJson = Effect.fn("AgentControlTaskVerificationFinalizer.decodeStoredJson")(
    function* (handoffId: string, operation: string, raw: unknown) {
      return yield* Effect.try({
        try: () => {
          const source = decodeCanonicalUtf8Bytes(raw);
          return { source, value: parseJsonStrict(source) } as const;
        },
        catch: (cause) => error(handoffId, operation, "authority-conflict", cause),
      });
    },
  );

  const decodePersistedPayload = <A, E>(
    handoffId: string,
    operation: string,
    raw: unknown,
    decode: (value: unknown) => Effect.Effect<A, E>,
  ) =>
    Effect.gen(function* () {
      const parsed = yield* decodeStoredJson(handoffId, operation, raw);
      const value = yield* decode(parsed.value).pipe(
        Effect.mapError((cause) => error(handoffId, operation, "authority-conflict", cause)),
      );
      if (
        canonicalJson(value as unknown as JsonValue) !==
        canonicalJson(parsed.value as unknown as JsonValue)
      ) {
        return yield* error(handoffId, operation, "authority-conflict");
      }
      return { source: parsed.source, value } as const;
    });

  const loadPublicationState = Effect.fn(
    "AgentControlTaskVerificationFinalizer.loadPublicationState",
  )(function* (handoffId: string, event: AgentControlTaskEvent) {
    if (event.type !== "agentControl.task.finalizedAfterVerification") {
      return yield* error(handoffId, "publication-event-type", "authority-conflict");
    }
    const rows = yield* sql<Record<string, unknown>>`
      SELECT typeof(handoff_id) AS "handoffIdStorageClass", handoff_id AS "handoffId",
        typeof(marker_id) AS "markerIdStorageClass", marker_id AS "markerId",
        typeof(task_finalization_evidence_id) AS "evidenceIdStorageClass",
        task_finalization_evidence_id AS "evidenceId",
        typeof(task_id) AS "taskIdStorageClass", task_id AS "taskId",
        typeof(task_event_id) AS "taskEventIdStorageClass", task_event_id AS "taskEventId",
        typeof(task_event_stream_version) AS "taskEventStreamVersionStorageClass",
        task_event_stream_version AS "taskEventStreamVersion",
        typeof(publication_owner_id) AS "ownerIdStorageClass",
        publication_owner_id AS "ownerId",
        typeof(status) AS "statusStorageClass", status,
        typeof(revision) AS "revisionStorageClass", revision,
        typeof(claim_fence) AS "claimFenceStorageClass", claim_fence AS "claimFence",
        typeof(created_at) AS "createdAtStorageClass", created_at AS "createdAt",
        typeof(claimed_at) AS "claimedAtStorageClass", claimed_at AS "claimedAt",
        typeof(lease_expires_at) AS "leaseExpiresAtStorageClass",
        lease_expires_at AS "leaseExpiresAt",
        typeof(completed_at) AS "completedAtStorageClass", completed_at AS "completedAt"
      FROM main.agent_control_task_verification_finalization_publications
      WHERE handoff_id = ${handoffId}
    `.pipe(
      Effect.mapError((cause) => error(handoffId, "publication-state-read", "persistence", cause)),
    );
    if (rows.length !== 1) {
      return yield* error(handoffId, "publication-state-cardinality", "authority-conflict");
    }
    const row = yield* decodePublicationStateRow(rows[0]).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "publication-state-decode", "authority-conflict", cause),
      ),
    );
    if (
      row.handoffId !== handoffId ||
      row.taskId !== event.aggregateId ||
      row.taskEventId !== event.eventId ||
      row.taskEventStreamVersion !== event.streamVersion ||
      row.evidenceId !== event.payload.taskFinalizationEvidenceId ||
      row.createdAt !== event.occurredAt ||
      (row.status === "pending" &&
        (row.revision !== 1 ||
          row.claimFence !== 0 ||
          row.ownerId !== null ||
          row.claimedAt !== null ||
          row.leaseExpiresAt !== null ||
          row.completedAt !== null)) ||
      (row.status === "claimed" &&
        (row.revision < 2 ||
          row.claimFence < 1 ||
          row.ownerId === null ||
          row.claimedAt === null ||
          row.leaseExpiresAt === null ||
          row.leaseExpiresAt <= row.claimedAt ||
          row.completedAt !== null)) ||
      (row.status === "completed" &&
        (row.revision < 3 ||
          row.claimFence < 1 ||
          row.ownerId === null ||
          row.claimedAt === null ||
          row.leaseExpiresAt === null ||
          row.leaseExpiresAt <= row.claimedAt ||
          row.completedAt === null ||
          row.completedAt < row.claimedAt ||
          row.completedAt >= row.leaseExpiresAt))
    ) {
      return yield* error(handoffId, "publication-state-authority", "authority-conflict");
    }
    return row;
  });

  let activePublicationDeadlineScheduler:
    | {
        readonly attemptId: number;
        readonly schedule: (handoffId: string, leaseExpiresAt: string) => Effect.Effect<void>;
      }
    | undefined;
  const schedulePublicationDeadline = (handoffId: string, leaseExpiresAt: string) =>
    Effect.suspend(
      () => activePublicationDeadlineScheduler?.schedule(handoffId, leaseExpiresAt) ?? Effect.void,
    );

  const claimPublication = Effect.fn("AgentControlTaskVerificationFinalizer.claimPublication")(
    function* (handoffId: string, event: AgentControlTaskEvent) {
      let current = yield* loadPublicationState(handoffId, event);
      for (let attempt = 0; attempt < 3; attempt += 1) {
        if (current.status === "completed") return Option.none<typeof current>();
        const time = yield* publicationTime();
        if (current.status === "claimed") {
          if (current.leaseExpiresAt === null) {
            return yield* error(handoffId, "publication-claim-authority", "authority-conflict");
          }
          if (current.leaseExpiresAt > time.claimedAt) {
            if (current.ownerId === publicationOwnerId) return Option.some(current);
            yield* schedulePublicationDeadline(handoffId, current.leaseExpiresAt);
            return Option.none<typeof current>();
          }
        }
        yield* hooks.beforePublicationClaim?.(handoffId) ?? Effect.void;
        const updated =
          current.status === "pending"
            ? yield* sql<{ readonly handoffId: string }>`
                UPDATE main.agent_control_task_verification_finalization_publications
                SET publication_owner_id = ${publicationOwnerId}, status = 'claimed',
                    revision = revision + 1, claim_fence = 1,
                    claimed_at = ${time.claimedAt}, lease_expires_at = ${time.leaseExpiresAt}
                WHERE handoff_id = ${handoffId} AND status = 'pending'
                  AND publication_owner_id IS NULL AND revision = ${current.revision}
                  AND claim_fence = 0 AND lease_expires_at IS NULL
                RETURNING handoff_id AS "handoffId"
              `.pipe(
                Effect.mapError((cause) =>
                  error(handoffId, "publication-claim", "persistence", cause),
                ),
              )
            : yield* sql<{ readonly handoffId: string }>`
                UPDATE main.agent_control_task_verification_finalization_publications
                SET publication_owner_id = ${publicationOwnerId}, revision = revision + 1,
                    claim_fence = claim_fence + 1,
                    claimed_at = ${time.claimedAt}, lease_expires_at = ${time.leaseExpiresAt}
                WHERE handoff_id = ${handoffId} AND status = 'claimed'
                  AND publication_owner_id = ${current.ownerId}
                  AND revision = ${current.revision}
                  AND claim_fence = ${current.claimFence}
                  AND lease_expires_at = ${current.leaseExpiresAt}
                  AND lease_expires_at <= ${time.claimedAt}
                RETURNING handoff_id AS "handoffId"
              `.pipe(
                Effect.mapError((cause) =>
                  error(handoffId, "publication-takeover", "persistence", cause),
                ),
              );
        current = yield* loadPublicationState(handoffId, event);
        if (updated.length === 1) {
          if (current.status !== "claimed" || current.ownerId !== publicationOwnerId) {
            return yield* error(handoffId, "publication-claim-authority", "authority-conflict");
          }
          return Option.some(current);
        }
      }
      return yield* error(handoffId, "publication-claim-race", "revision-conflict");
    },
  );

  const renewPublicationFence = Effect.fn(
    "AgentControlTaskVerificationFinalizer.renewPublicationFence",
  )(function* (
    handoffId: string,
    event: AgentControlTaskEvent,
    claimed: typeof PublicationStateRow.Type,
  ) {
    const time = yield* publicationTime();
    const renewed = yield* sql<{ readonly handoffId: string }>`
      UPDATE main.agent_control_task_verification_finalization_publications
      SET revision = revision + 1, claimed_at = ${time.claimedAt},
          lease_expires_at = ${time.leaseExpiresAt}
      WHERE handoff_id = ${handoffId} AND status = 'claimed'
        AND publication_owner_id = ${publicationOwnerId}
        AND revision = ${claimed.revision}
        AND claim_fence = ${claimed.claimFence}
        AND lease_expires_at = ${claimed.leaseExpiresAt}
        AND lease_expires_at > ${time.claimedAt}
      RETURNING handoff_id AS "handoffId"
    `.pipe(
      Effect.mapError((cause) => error(handoffId, "publication-fence-renew", "persistence", cause)),
    );
    const current = yield* loadPublicationState(handoffId, event);
    if (renewed.length === 0) {
      if (
        current.status === "completed" ||
        current.status !== "claimed" ||
        current.ownerId !== publicationOwnerId ||
        current.claimFence !== claimed.claimFence
      ) {
        return Option.none<typeof current>();
      }
      return yield* error(handoffId, "publication-fence-race", "revision-conflict");
    }
    if (
      current.status !== "claimed" ||
      current.ownerId !== publicationOwnerId ||
      current.claimFence !== claimed.claimFence ||
      current.revision !== claimed.revision + 1 ||
      current.claimedAt !== time.claimedAt ||
      current.leaseExpiresAt !== time.leaseExpiresAt
    ) {
      return yield* error(handoffId, "publication-fence-authority", "authority-conflict");
    }
    return Option.some(current);
  });

  const maintainPublicationFence = Effect.fn(
    "AgentControlTaskVerificationFinalizer.maintainPublicationFence",
  )(function* (handoffId: string, claimFence: number) {
    const heartbeatEveryMillis = Math.max(1, Math.floor(publicationLeaseDurationMillis / 3));
    while (true) {
      yield* Effect.sleep(Duration.millis(heartbeatEveryMillis));
      const time = yield* publicationTime();
      const renewed = yield* sql<{ readonly handoffId: string }>`
        UPDATE main.agent_control_task_verification_finalization_publications
        SET revision = revision + 1, claimed_at = ${time.claimedAt},
            lease_expires_at = ${time.leaseExpiresAt}
        WHERE handoff_id = ${handoffId} AND status = 'claimed'
          AND publication_owner_id = ${publicationOwnerId}
          AND claim_fence = ${claimFence}
          AND lease_expires_at > ${time.claimedAt}
        RETURNING handoff_id AS "handoffId"
      `.pipe(
        Effect.mapError((cause) =>
          error(handoffId, "publication-fence-heartbeat", "persistence", cause),
        ),
      );
      if (renewed.length === 0) return;
    }
  });

  const publishDurably = Effect.fn("AgentControlTaskVerificationFinalizer.publishDurably")(
    function* (handoffId: string, event: AgentControlTaskEvent) {
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const claimed = yield* claimPublication(handoffId, event);
          if (Option.isNone(claimed)) return false;
          yield* hooks.beforePublicationFenceValidation?.(handoffId) ?? Effect.void;
          const fenced = yield* renewPublicationFence(handoffId, event, claimed.value);
          if (Option.isNone(fenced)) return false;
          const heartbeat = yield* Effect.forkChild(
            maintainPublicationFence(handoffId, fenced.value.claimFence).pipe(Effect.interruptible),
            { startImmediately: true },
          );
          return yield* Effect.gen(function* () {
            yield* taskEngine.publishCommitted([event]);
            yield* hooks.afterPublicationBeforeCompletion?.(handoffId) ?? Effect.void;
            const completedAt = (yield* publicationTime()).claimedAt;
            const completed = yield* sql
              .withTransaction(
                sql<{ readonly handoffId: string }>`
                UPDATE main.agent_control_task_verification_finalization_publications
                SET status = 'completed', revision = revision + 1, completed_at = ${completedAt}
                WHERE handoff_id = ${handoffId} AND status = 'claimed'
                  AND publication_owner_id = ${publicationOwnerId}
                  AND claim_fence = ${fenced.value.claimFence}
                  AND lease_expires_at > ${completedAt}
                RETURNING handoff_id AS "handoffId"
              `,
              )
              .pipe(
                Effect.mapError((cause) =>
                  error(handoffId, "publication-complete", "persistence", cause),
                ),
              );
            if (completed.length !== 1) {
              const current = yield* loadPublicationState(handoffId, event);
              if (current.status !== "completed") {
                return yield* error(handoffId, "publication-complete-race", "authority-conflict");
              }
            }
            return true;
          }).pipe(Effect.ensuring(Fiber.interrupt(heartbeat)));
        }),
      );
    },
  );

  const drainPublication = Effect.fn("AgentControlTaskVerificationFinalizer.drainPublication")(
    function* (handoffId: string, event: AgentControlTaskEvent) {
      return yield* publishDurably(handoffId, event).pipe(
        Effect.catchIf(
          (cause) => cause.reason === "persistence" || cause.reason === "revision-conflict",
          (cause) =>
            Effect.logWarning(
              "retrying Task Verification publication after a transient persistence failure",
              {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
              },
            ).pipe(
              Effect.andThen(
                hooks.afterPublicationAttemptFailure?.(handoffId, cause.operation) ?? Effect.void,
              ),
              Effect.andThen(publishDurably(handoffId, event)),
            ),
        ),
      );
    },
  );

  const loadSource = Effect.fn("AgentControlTaskVerificationFinalizer.loadSource")(function* (
    handoffId: string,
  ): Effect.fn.Return<VerificationSourceAuthority, AgentControlTaskVerificationFinalizerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT evidence.handoff_id AS "handoffId",
        evidence.handoff_fingerprint AS "handoffFingerprint",
        evidence.project_id AS "projectId", evidence.task_id AS "taskId",
        evidence.task_revision AS "verificationTaskRevision",
        evidence.github_intake_sequence AS "githubIntakeSequence",
        evidence.source_identity_fingerprint AS "sourceIdentityFingerprint",
        evidence.finalization_evidence_id AS "verificationEvidenceId",
        receipt.receipt_id AS "verificationReceiptId",
        marker.marker_id AS "verificationMarkerId",
        evidence.finalization_command_id AS "verificationCommandId",
        evidence.finalization_fingerprint AS "verificationFingerprint",
        marker.marker_fingerprint AS "verificationMarkerFingerprint",
        CAST(evidence.finalization_json AS BLOB) AS "finalizationJsonBytes",
        evidence.delivery_terminal_state AS "deliveryTerminalState",
        evidence.outcome AS "verificationOutcome",
        evidence.terminal_cause AS "terminalCause",
        evidence.terminal_runtime_event_id AS "terminalRuntimeEventId",
        evidence.evaluation_authority AS "evaluationAuthority",
        evidence.evaluation_id AS "evaluationId",
        evidence.evaluation_evidence_id AS "evaluationEvidenceId",
        evidence.evaluation_receipt_id AS "evaluationReceiptId",
        evidence.evaluation_marker_id AS "evaluationMarkerId",
        evidence.evaluation_disposition AS "evaluationDisposition",
        evidence.verification_verdict AS "verificationVerdict",
        evidence.invalid_output_code AS "invalidOutputCode",
        evidence.stage_run_id AS "stageRunId",
        stage.event_id AS "stageEventId", stage.sequence AS "stageEventSequence",
        stage.stream_version AS "stageEventStreamVersion",
        stage.event_type AS "stageEventType", stage.occurred_at AS "stageOccurredAt",
        stage.command_id AS "stageCommandId",
        stage.causation_event_id AS "stageCausationEventId",
        stage.correlation_id AS "stageCorrelationId",
        stage.actor_authority AS "stageAuthority",
        CAST(stage.payload_json AS BLOB) AS "stagePayloadBytes",
        CAST(stage.metadata_json AS BLOB) AS "stageMetadataBytes",
        evidence.lease_id AS "leaseId",
        lease.event_id AS "leaseEventId", lease.sequence AS "leaseEventSequence",
        lease.stream_version AS "leaseEventStreamVersion",
        lease.event_type AS "leaseEventType", lease.occurred_at AS "leaseOccurredAt",
        lease.command_id AS "leaseCommandId",
        lease.causation_event_id AS "leaseCausationEventId",
        lease.correlation_id AS "leaseCorrelationId",
        lease.actor_authority AS "leaseAuthority",
        CAST(lease.payload_json AS BLOB) AS "leasePayloadBytes",
        CAST(lease.metadata_json AS BLOB) AS "leaseMetadataBytes",
        evidence.finalized_at AS "finalizedAt"
      FROM main.agent_control_verification_finalization_evidence evidence
      JOIN main.agent_control_verification_finalization_receipts receipt
        ON receipt.receipt_id = evidence.receipt_id
       AND receipt.finalization_evidence_id = evidence.finalization_evidence_id
       AND receipt.marker_id = evidence.marker_id
       AND receipt.finalization_command_id = evidence.finalization_command_id
       AND receipt.finalization_fingerprint = evidence.finalization_fingerprint
       AND receipt.handoff_id = evidence.handoff_id
       AND receipt.outcome = evidence.outcome
       AND receipt.terminal_cause = evidence.terminal_cause
       AND receipt.stage_event_id = evidence.stage_event_id
       AND receipt.stage_event_sequence = evidence.stage_event_sequence
       AND receipt.lease_event_id = evidence.lease_event_id
       AND receipt.lease_event_sequence = evidence.lease_event_sequence
       AND receipt.status = 'accepted'
       AND receipt.accepted_at = evidence.finalized_at
      JOIN main.agent_control_verification_finalization_markers marker
        ON marker.marker_id = evidence.marker_id
       AND marker.receipt_id = receipt.receipt_id
       AND marker.finalization_evidence_id = evidence.finalization_evidence_id
       AND marker.finalization_command_id = evidence.finalization_command_id
       AND marker.finalization_fingerprint = evidence.finalization_fingerprint
       AND marker.handoff_id = evidence.handoff_id
       AND marker.committed_at = evidence.finalized_at
      JOIN main.agent_control_verification_handoff_accepted accepted
        ON accepted.handoff_id = evidence.handoff_id
       AND accepted.handoff_fingerprint = evidence.handoff_fingerprint
      JOIN main.agent_control_events stage
        ON stage.event_id = evidence.stage_event_id
       AND stage.aggregate_kind = 'stage-run'
       AND stage.stream_id = evidence.stage_run_id
       AND stage.stream_version = evidence.stage_event_stream_version
       AND stage.sequence = evidence.stage_event_sequence
      JOIN main.agent_control_events lease
        ON lease.event_id = evidence.lease_event_id
       AND lease.aggregate_kind = 'stage-run-lease'
       AND lease.stream_id = evidence.lease_id
       AND lease.stream_version = evidence.lease_event_stream_version
       AND lease.sequence = evidence.lease_event_sequence
      WHERE evidence.handoff_id = ${handoffId}
        AND typeof(evidence.finalization_json) = 'text'
        AND typeof(stage.payload_json) = 'text'
        AND typeof(stage.metadata_json) = 'text'
        AND typeof(lease.payload_json) = 'text'
        AND typeof(lease.metadata_json) = 'text'
    `.pipe(Effect.mapError((cause) => error(handoffId, "load-source", "persistence", cause)));
    if (rows.length !== 1) {
      return yield* error(handoffId, "load-source-cardinality", "authority-conflict");
    }
    const row = yield* decodeSourceRow(rows[0]).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "decode-source-row", "authority-conflict", cause),
      ),
    );
    const finalization = yield* decodeCanonical(
      handoffId,
      "decode-verification-finalization",
      row.finalizationJsonBytes,
    );
    const document = yield* decodeVerificationDocument(finalization.value).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "decode-verification-document", "authority-conflict", cause),
      ),
    );
    const stagePayload = yield* decodePersistedPayload(
      handoffId,
      "decode-stage-payload",
      row.stagePayloadBytes,
      decodeVerificationStagePayload,
    );
    const stageMetadata = yield* decodeCanonical(
      handoffId,
      "decode-stage-metadata",
      row.stageMetadataBytes,
    );
    const leasePayload = yield* decodePersistedPayload(
      handoffId,
      "decode-lease-payload",
      row.leasePayloadBytes,
      decodeVerificationLeasePayload,
    );
    const leaseMetadata = yield* decodeCanonical(
      handoffId,
      "decode-lease-metadata",
      row.leaseMetadataBytes,
    );
    const expectedVerificationCommandId = deriveVerificationFinalizationCommandId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedVerificationEvidenceId = deriveVerificationFinalizationEvidenceId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedVerificationReceiptId = deriveVerificationFinalizationReceiptId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedVerificationMarkerId = deriveVerificationFinalizationMarkerId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedStageEventId = deriveVerificationTerminalStageEventId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedLeaseEventId = deriveVerificationLeaseReleaseEventId(
      handoffId,
      row.handoffFingerprint,
    );
    const expectedVerificationFingerprint = fingerprintVerificationTurn("finalization-evidence", [
      finalization.source,
    ]);
    const expectedVerificationMarkerFingerprint = fingerprintVerificationTurn(
      "finalization-marker",
      [
        handoffId,
        row.handoffFingerprint,
        String(expectedVerificationCommandId),
        expectedVerificationEvidenceId,
        expectedVerificationFingerprint,
        document.stageEventId,
        String(document.stageEventSequence),
        document.leaseEventId,
        String(document.leaseEventSequence),
        document.finalizedAt,
      ],
    );
    const expectedStageType =
      document.outcome === "succeeded"
        ? "agentControl.stageRun.verificationSucceeded"
        : document.outcome === "failed"
          ? "agentControl.stageRun.verificationFailed"
          : "agentControl.stageRun.verificationCancelled";
    if (
      row.handoffId !== handoffId ||
      row.verificationTaskRevision < 1 ||
      row.githubIntakeSequence < 1 ||
      row.verificationCommandId !== expectedVerificationCommandId ||
      row.verificationEvidenceId !== expectedVerificationEvidenceId ||
      row.verificationReceiptId !== expectedVerificationReceiptId ||
      row.verificationMarkerId !== expectedVerificationMarkerId ||
      row.verificationFingerprint !== expectedVerificationFingerprint ||
      row.verificationMarkerFingerprint !== expectedVerificationMarkerFingerprint ||
      row.stageEventId !== expectedStageEventId ||
      row.leaseEventId !== expectedLeaseEventId ||
      row.stageEventType !== expectedStageType ||
      row.leaseEventType !== "agentControl.stageRunLease.releasedAfterVerification" ||
      row.stageAuthority !== "system" ||
      row.leaseAuthority !== "system" ||
      row.stageCommandId !== row.verificationCommandId ||
      row.leaseCommandId !== row.verificationCommandId ||
      row.stageCorrelationId !== row.verificationCommandId ||
      row.leaseCorrelationId !== row.verificationCommandId ||
      row.stageCausationEventId !== row.terminalRuntimeEventId ||
      row.leaseCausationEventId !== row.stageEventId ||
      row.stageOccurredAt !== row.finalizedAt ||
      row.leaseOccurredAt !== row.finalizedAt ||
      stageMetadata.source !== '{"schemaVersion":1}' ||
      leaseMetadata.source !== '{"schemaVersion":1}' ||
      canonicalJson(stagePayload.value) !== canonicalJson(document.stagePayload as JsonValue) ||
      canonicalJson(leasePayload.value) !== canonicalJson(document.leasePayload as JsonValue) ||
      document.handoffId !== handoffId ||
      document.handoffFingerprint !== row.handoffFingerprint ||
      document.finalizationCommandId !== row.verificationCommandId ||
      document.finalizationEvidenceId !== row.verificationEvidenceId ||
      document.outcome !== row.verificationOutcome ||
      document.terminalCause !== row.terminalCause ||
      document.deliveryTerminalState !== row.deliveryTerminalState ||
      document.terminalRuntimeEventId !== row.terminalRuntimeEventId ||
      document.stageEventId !== row.stageEventId ||
      document.stageEventSequence !== row.stageEventSequence ||
      document.stageEventStreamVersion !== row.stageEventStreamVersion ||
      document.leaseEventId !== row.leaseEventId ||
      document.leaseEventSequence !== row.leaseEventSequence ||
      document.leaseEventStreamVersion !== row.leaseEventStreamVersion ||
      document.finalizedAt !== row.finalizedAt ||
      document.stagePayload.projectId !== row.projectId ||
      document.stagePayload.taskId !== row.taskId ||
      document.stagePayload.taskRevision !== row.verificationTaskRevision ||
      document.stagePayload.githubIntakeSequence !== row.githubIntakeSequence ||
      document.stagePayload.sourceIdentityFingerprint !== row.sourceIdentityFingerprint ||
      canonicalJson(document.evaluation as JsonValue) !==
        canonicalJson(document.stagePayload.evaluation as JsonValue) ||
      document.evaluation.evaluationAuthority !== row.evaluationAuthority ||
      document.evaluation.evaluationId !== row.evaluationId ||
      document.evaluation.evaluationEvidenceId !== row.evaluationEvidenceId ||
      document.evaluation.evaluationReceiptId !== row.evaluationReceiptId ||
      document.evaluation.evaluationMarkerId !== row.evaluationMarkerId ||
      document.evaluation.evaluationDisposition !== row.evaluationDisposition ||
      document.evaluation.verificationVerdict !== row.verificationVerdict ||
      document.evaluation.invalidOutputCode !== row.invalidOutputCode
    ) {
      return yield* error(handoffId, "compare-source-authority", "authority-conflict");
    }
    return { row, document };
  });

  /**
   * Task events predate the canonical evidence encoders. Their original TEXT
   * bytes are immutable authority, but their object-key order is deliberately
   * not rewritten. Decode the exact UTF-8 bytes, close them through the typed
   * event schema, and replay every stream version before trusting the current
   * projection.
   */
  const loadTaskAuthorityAtRevision = Effect.fn(
    "AgentControlTaskVerificationFinalizer.loadTaskAuthorityAtRevision",
  )(function* (handoffId: string, taskId: AgentControlTaskId, targetRevision: number) {
    const projectionRows = yield* sql<Record<string, unknown>>`
      SELECT typeof(state_json) AS "stateStorageClass",
        CAST(state_json AS BLOB) AS "stateBytes",
        typeof(task_id) AS "taskIdStorageClass",
        typeof(project_id) AS "projectIdStorageClass",
        typeof(revision) AS "revisionStorageClass",
        typeof(last_event_sequence) AS "sequenceStorageClass",
        typeof(repository_node_id) AS "repositoryNodeIdStorageClass",
        typeof(issue_node_id) AS "issueNodeIdStorageClass",
        typeof(issue_number) AS "issueNumberStorageClass",
        typeof(issue_url) AS "issueUrlStorageClass",
        typeof(status) AS "statusStorageClass",
        typeof(source_gate) AS "sourceGateStorageClass",
        typeof(stage) AS "stageStorageClass",
        typeof(source_updated_at) AS "sourceUpdatedAtStorageClass",
        typeof(github_intake_sequence) AS "githubIntakeSequenceStorageClass",
        typeof(created_at) AS "createdAtStorageClass",
        typeof(updated_at) AS "updatedAtStorageClass",
        task_id AS "taskId", project_id AS "projectId", revision,
        last_event_sequence AS sequence,
        repository_node_id AS "repositoryNodeId", issue_node_id AS "issueNodeId",
        issue_number AS "issueNumber", issue_url AS "issueUrl", status,
        source_gate AS "sourceGate", stage, source_updated_at AS "sourceUpdatedAt",
        github_intake_sequence AS "githubIntakeSequence",
        created_at AS "createdAt", updated_at AS "updatedAt"
      FROM main.agent_control_task_states WHERE task_id = ${taskId}
    `.pipe(
      Effect.mapError((cause) => error(handoffId, "task-projection-read", "persistence", cause)),
    );
    if (projectionRows.length !== 1) {
      return yield* error(handoffId, "task-projection-cardinality", "authority-conflict");
    }
    const projection = yield* decodeAgentControlTaskProjectionRow(
      projectionRows[0]!,
      "AgentControlTaskVerificationFinalizer.taskProjection",
    ).pipe(
      Effect.mapError((cause) =>
        error(handoffId, "task-projection-decode", "authority-conflict", cause),
      ),
    );
    if (targetRevision < 1 || targetRevision > projection.revision) {
      return yield* error(handoffId, "task-history-target", "authority-conflict");
    }
    const rows = yield* sql<Record<string, unknown>>`
      SELECT typeof(sequence) AS "sequenceStorageClass", sequence,
        typeof(event_id) AS "eventIdStorageClass", event_id AS "eventId",
        typeof(event_type) AS "typeStorageClass", event_type AS type,
        typeof(aggregate_kind) AS "aggregateKindStorageClass",
        aggregate_kind AS "aggregateKind",
        typeof(stream_id) AS "aggregateIdStorageClass", stream_id AS "aggregateId",
        typeof(stream_version) AS "streamVersionStorageClass",
        stream_version AS "streamVersion",
        typeof(occurred_at) AS "occurredAtStorageClass", occurred_at AS "occurredAt",
        typeof(command_id) AS "commandIdStorageClass", command_id AS "commandId",
        typeof(causation_event_id) AS "causationEventIdStorageClass",
        causation_event_id AS "causationEventId",
        typeof(correlation_id) AS "correlationIdStorageClass",
        correlation_id AS "correlationId",
        typeof(actor_authority) AS "authorityStorageClass", actor_authority AS authority,
        typeof(payload_json) AS "payloadStorageClass",
        CAST(payload_json AS BLOB) AS "payloadBytes",
        typeof(metadata_json) AS "metadataStorageClass",
        CAST(metadata_json AS BLOB) AS "metadataBytes"
      FROM main.agent_control_events
      WHERE aggregate_kind = 'task' AND stream_id = ${taskId}
      ORDER BY stream_version, sequence
    `.pipe(Effect.mapError((cause) => error(handoffId, "task-history-read", "persistence", cause)));
    if (rows.length !== projection.revision) {
      return yield* error(handoffId, "task-history-count", "authority-conflict");
    }
    const events = yield* Effect.forEach(rows, (raw) =>
      Effect.gen(function* () {
        const row = yield* decodeTaskAuthorityEventRow(raw).pipe(
          Effect.mapError((cause) =>
            error(handoffId, "task-history-coordinates", "authority-conflict", cause),
          ),
        );
        const payload = yield* decodeStoredJson(
          handoffId,
          "task-history-payload-json",
          row.payloadBytes,
        );
        const metadata = yield* decodeStoredJson(
          handoffId,
          "task-history-metadata-json",
          row.metadataBytes,
        );
        const typedMetadata = yield* decodeTaskMetadata(metadata.value).pipe(
          Effect.mapError((cause) =>
            error(handoffId, "task-history-metadata-decode", "authority-conflict", cause),
          ),
        );
        const event = yield* decodeTaskEvent({
          sequence: row.sequence,
          eventId: row.eventId,
          type: row.type,
          aggregateKind: row.aggregateKind,
          aggregateId: row.aggregateId,
          streamVersion: row.streamVersion,
          occurredAt: row.occurredAt,
          commandId: row.commandId,
          causationEventId: row.causationEventId,
          correlationId: row.correlationId,
          authority: row.authority,
          payload: payload.value,
          metadata: typedMetadata,
        }).pipe(
          Effect.mapError((cause) =>
            error(handoffId, "task-history-event-decode", "authority-conflict", cause),
          ),
        );
        if (
          canonicalJson(event.payload as unknown as JsonValue) !==
            canonicalJson(payload.value as JsonValue) ||
          canonicalJson(event.metadata as unknown as JsonValue) !==
            canonicalJson(metadata.value as JsonValue)
        ) {
          return yield* error(handoffId, "task-history-schema-closure", "authority-conflict");
        }
        return event;
      }),
    );
    let currentState: AgentControlTaskState | null = null;
    let stateAtTarget: AgentControlTaskState | null = null;
    for (const [index, event] of events.entries()) {
      if (
        event.aggregateId !== taskId ||
        event.streamVersion !== index + 1 ||
        (index > 0 && event.sequence <= events[index - 1]!.sequence)
      ) {
        return yield* error(handoffId, "task-history-order", "authority-conflict");
      }
      currentState = yield* projectAgentControlTaskEvent(currentState, event).pipe(
        Effect.mapError((cause) =>
          error(handoffId, "task-history-project", "authority-conflict", cause),
        ),
      );
      if (event.streamVersion === targetRevision) stateAtTarget = currentState;
    }
    if (
      currentState === null ||
      stateAtTarget === null ||
      currentState.revision !== projection.revision ||
      stateAtTarget.revision !== targetRevision
    ) {
      return yield* error(handoffId, "task-history-state", "authority-conflict");
    }
    if (
      canonicalJson(projection as unknown as JsonValue) !==
      canonicalJson(currentState as unknown as JsonValue)
    ) {
      return yield* error(handoffId, "task-projection-authority", "authority-conflict");
    }
    return {
      state: stateAtTarget,
      event: events[targetRevision - 1]!,
      events,
      projection,
    } as const;
  });

  const loadTaskAuthority = Effect.fn("AgentControlTaskVerificationFinalizer.loadTaskAuthority")(
    function* (source: VerificationSourceAuthority) {
      const taskId = AgentControlTaskId.make(source.row.taskId);
      const authority = yield* loadTaskAuthorityAtRevision(
        source.row.handoffId,
        taskId,
        source.row.verificationTaskRevision,
      );
      if (
        authority.state.taskId !== taskId ||
        authority.state.source.projectId !== source.row.projectId ||
        authority.state.githubIntakeSequence !== source.row.githubIntakeSequence ||
        fingerprintAgentControlSourceIdentity(authority.state.source) !==
          source.row.sourceIdentityFingerprint ||
        authority.event.streamVersion !== source.row.verificationTaskRevision ||
        authority.projection.taskId !== taskId ||
        authority.projection.source.projectId !== source.row.projectId ||
        authority.projection.revision < source.row.verificationTaskRevision ||
        fingerprintAgentControlSourceIdentity(authority.projection.source) !==
          source.row.sourceIdentityFingerprint
      ) {
        return yield* error(source.row.handoffId, "compare-task-authority", "authority-conflict");
      }
      return authority;
    },
  );

  const buildFinalization = Effect.fn("AgentControlTaskVerificationFinalizer.buildFinalization")(
    function* (
      source: VerificationSourceAuthority,
      previous: AgentControlTaskState,
      taskSourceEvent: AgentControlTaskEvent,
    ): Effect.fn.Return<BuiltFinalization, AgentControlTaskVerificationFinalizerError> {
      const row = source.row;
      const document = source.document;
      if (
        previous.stage !== "intake" ||
        previous.status === "succeeded" ||
        previous.status === "failed" ||
        previous.status === "cancelled" ||
        previous.revision < row.verificationTaskRevision ||
        previous.source.projectId !== row.projectId ||
        previous.taskId !== row.taskId ||
        fingerprintAgentControlSourceIdentity(previous.source) !== row.sourceIdentityFingerprint
      ) {
        return yield* error(row.handoffId, "terminalization-precondition", "authority-conflict");
      }
      const identityParts = [
        row.verificationMarkerId,
        row.taskId,
        String(row.verificationTaskRevision),
      ];
      const commandId = CommandId.make(
        taskIdentity(
          "task-verification-finalization",
          "verification-finalization-command",
          identityParts,
        ),
      );
      const evidenceId = taskIdentity(
        "task-verification-finalization-evidence",
        "verification-finalization-evidence",
        identityParts,
      );
      const receiptId = taskIdentity(
        "task-verification-finalization-receipt",
        "verification-finalization-receipt",
        identityParts,
      );
      const markerId = taskIdentity(
        "task-verification-finalization-marker",
        "verification-finalization-marker",
        identityParts,
      );
      const eventId = EventId.make(
        taskIdentity(
          "task-finalized-after-verification-event",
          "finalized-after-verification-event",
          identityParts,
        ),
      );
      const payload = yield* decodeTaskPayload({
        projectId: row.projectId,
        taskId: row.taskId,
        verificationTaskRevision: row.verificationTaskRevision,
        previousTaskRevision: previous.revision,
        githubIntakeSequence: row.githubIntakeSequence,
        sourceIdentityFingerprint: row.sourceIdentityFingerprint,
        taskSourceEventId: taskSourceEvent.eventId,
        taskSourceEventSequence: taskSourceEvent.sequence,
        taskSourceEventStreamVersion: taskSourceEvent.streamVersion,
        handoffId: row.handoffId,
        handoffFingerprint: row.handoffFingerprint,
        verificationFinalizationEvidenceId: row.verificationEvidenceId,
        verificationFinalizationReceiptId: row.verificationReceiptId,
        verificationFinalizationMarkerId: row.verificationMarkerId,
        verificationFinalizationCommandId: CommandId.make(row.verificationCommandId),
        verificationFinalizationFingerprint: row.verificationFingerprint,
        verificationFinalizationMarkerFingerprint: row.verificationMarkerFingerprint,
        terminalStageRunId: row.stageRunId,
        terminalStageEventId: row.stageEventId,
        terminalStageEventSequence: row.stageEventSequence,
        terminalStageEventStreamVersion: row.stageEventStreamVersion,
        releasedLeaseId: row.leaseId,
        releasedLeaseEventId: row.leaseEventId,
        releasedLeaseEventSequence: row.leaseEventSequence,
        releasedLeaseEventStreamVersion: row.leaseEventStreamVersion,
        terminalRuntimeEventId: row.terminalRuntimeEventId,
        taskFinalizationEvidenceId: evidenceId,
        deliveryTerminalState: row.deliveryTerminalState,
        verificationOutcome: row.verificationOutcome,
        terminalCause: row.terminalCause,
        previousStatus: previous.status,
        status: row.verificationOutcome,
        stage: "verification",
        evaluation: document.evaluation,
        finalizedAt: row.finalizedAt,
      }).pipe(
        Effect.mapError((cause) =>
          error(row.handoffId, "build-task-payload", "authority-conflict", cause),
        ),
      );
      const finalizationJson = canonicalJson({
        schemaVersion: 1,
        commandId,
        taskFinalizationEvidenceId: evidenceId,
        taskFinalizationReceiptId: receiptId,
        taskFinalizationMarkerId: markerId,
        verificationFinalizationEvidenceId: row.verificationEvidenceId,
        verificationFinalizationReceiptId: row.verificationReceiptId,
        verificationFinalizationMarkerId: row.verificationMarkerId,
        verificationFinalizationCommandId: row.verificationCommandId,
        verificationFinalizationFingerprint: row.verificationFingerprint,
        verificationFinalizationMarkerFingerprint: row.verificationMarkerFingerprint,
        taskEventId: eventId,
        taskEventStreamVersion: previous.revision + 1,
        payload,
        finalizedAt: row.finalizedAt,
      } as unknown as JsonValue);
      const finalizationFingerprint = sha256Utf8(finalizationJson);
      const markerFingerprint = sha256Utf8(
        canonicalJson({
          domain: "agent-control-task-verification-finalization-marker-v1",
          evidenceId,
          receiptId,
          markerId,
          commandId,
          finalizationFingerprint,
          verificationMarkerId: row.verificationMarkerId,
          eventId,
          finalizedAt: row.finalizedAt,
        } as unknown as JsonValue),
      );
      return {
        commandId,
        evidenceId,
        receiptId,
        markerId,
        eventId,
        finalizationJson,
        finalizationFingerprint,
        markerFingerprint,
        payload,
      };
    },
  );

  const validateReplay = Effect.fn("AgentControlTaskVerificationFinalizer.validateReplay")(
    function* (handoffId: string, source: VerificationSourceAuthority) {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT evidence.task_finalization_evidence_id AS "evidenceId",
          evidence.receipt_id AS "evidenceReceiptId",
          evidence.marker_id AS "evidenceMarkerId",
          evidence.finalization_command_id AS "evidenceCommandId",
          evidence.finalization_fingerprint AS "evidenceFingerprint",
          CAST(evidence.finalization_json AS BLOB) AS "finalizationJsonBytes",
          evidence.verification_marker_id AS "verificationMarkerId",
          evidence.verification_finalization_command_id AS "verificationCommandId",
          evidence.verification_finalization_marker_fingerprint AS
            "verificationMarkerFingerprint",
          evidence.task_event_id AS "taskEventId",
          evidence.task_event_sequence AS "taskEventSequence",
          evidence.task_event_stream_version AS "taskEventStreamVersion",
          receipt.receipt_id AS "receiptId", receipt.marker_id AS "receiptMarkerId",
          receipt.task_finalization_evidence_id AS "receiptEvidenceId",
          receipt.finalization_command_id AS "receiptCommandId",
          receipt.finalization_fingerprint AS "receiptFingerprint",
          receipt.status AS "receiptStatus", receipt.accepted_at AS "receiptAcceptedAt",
          marker.marker_id AS "markerId", marker.receipt_id AS "markerReceiptId",
          marker.task_finalization_evidence_id AS "markerEvidenceId",
          marker.finalization_command_id AS "markerCommandId",
          marker.finalization_fingerprint AS "markerFinalizationFingerprint",
          marker.marker_fingerprint AS "markerFingerprint",
          marker.verification_marker_id AS "markerVerificationMarkerId",
          marker.committed_at AS "markerCommittedAt"
        FROM main.agent_control_task_verification_finalization_evidence evidence
        JOIN main.agent_control_task_verification_finalization_receipts receipt
          ON receipt.receipt_id = evidence.receipt_id
         AND receipt.task_finalization_evidence_id = evidence.task_finalization_evidence_id
        JOIN main.agent_control_task_verification_finalization_markers marker
          ON marker.marker_id = evidence.marker_id
         AND marker.receipt_id = receipt.receipt_id
         AND marker.task_finalization_evidence_id = evidence.task_finalization_evidence_id
        WHERE evidence.handoff_id = ${handoffId}
          AND typeof(evidence.finalization_json) = 'text'
      `.pipe(Effect.mapError((cause) => error(handoffId, "read-replay", "persistence", cause)));
      if (rows.length !== 1) return yield* error(handoffId, "replay-chain", "partial-replay");
      const row = rows[0]!;
      const taskEventId = String(row.taskEventId);
      const taskEventStreamVersion = Number(row.taskEventStreamVersion);
      const history = yield* loadTaskAuthorityAtRevision(
        handoffId,
        AgentControlTaskId.make(source.row.taskId),
        taskEventStreamVersion,
      );
      const taskEvent = history.event;
      if (
        taskEvent.type !== "agentControl.task.finalizedAfterVerification" ||
        taskEvent.eventId !== taskEventId ||
        taskEvent.sequence !== Number(row.taskEventSequence) ||
        taskEvent.streamVersion !== taskEventStreamVersion
      ) {
        return yield* error(handoffId, "replay-task-event", "identity-mismatch");
      }
      const previousHistory = yield* loadTaskAuthorityAtRevision(
        handoffId,
        AgentControlTaskId.make(source.row.taskId),
        taskEvent.payload.previousTaskRevision,
      );
      const expected = yield* buildFinalization(
        source,
        previousHistory.state,
        previousHistory.events[source.row.verificationTaskRevision - 1]!,
      );
      const storedFinalization = yield* decodeCanonical(
        handoffId,
        "replay-finalization-json",
        row.finalizationJsonBytes,
      );
      const next = yield* projectAgentControlTaskEvent(previousHistory.state, taskEvent).pipe(
        Effect.mapError((cause) => error(handoffId, "replay-project", "identity-mismatch", cause)),
      );
      if (
        expected.eventId !== taskEvent.eventId ||
        canonicalJson(expected.payload as unknown as JsonValue) !==
          canonicalJson(taskEvent.payload as unknown as JsonValue) ||
        storedFinalization.source !== expected.finalizationJson ||
        row.evidenceId !== expected.evidenceId ||
        row.evidenceReceiptId !== expected.receiptId ||
        row.evidenceMarkerId !== expected.markerId ||
        row.evidenceCommandId !== expected.commandId ||
        row.evidenceFingerprint !== expected.finalizationFingerprint ||
        row.verificationMarkerId !== source.row.verificationMarkerId ||
        row.verificationCommandId !== source.row.verificationCommandId ||
        row.verificationMarkerFingerprint !== source.row.verificationMarkerFingerprint ||
        row.receiptId !== expected.receiptId ||
        row.receiptMarkerId !== expected.markerId ||
        row.receiptEvidenceId !== expected.evidenceId ||
        row.receiptCommandId !== expected.commandId ||
        row.receiptFingerprint !== expected.finalizationFingerprint ||
        row.receiptStatus !== "accepted" ||
        row.receiptAcceptedAt !== source.row.finalizedAt ||
        row.markerId !== expected.markerId ||
        row.markerReceiptId !== expected.receiptId ||
        row.markerEvidenceId !== expected.evidenceId ||
        row.markerCommandId !== expected.commandId ||
        row.markerFinalizationFingerprint !== expected.finalizationFingerprint ||
        row.markerFingerprint !== expected.markerFingerprint ||
        row.markerVerificationMarkerId !== source.row.verificationMarkerId ||
        row.markerCommittedAt !== source.row.finalizedAt ||
        history.projection.revision < next.revision ||
        history.projection.stage !== "verification" ||
        history.projection.status !== expected.payload.status
      ) {
        return yield* error(handoffId, "compare-replay", "identity-mismatch");
      }
      const publication = yield* loadPublicationState(handoffId, taskEvent);
      if (publication.markerId !== expected.markerId) {
        return yield* error(handoffId, "compare-replay-publication", "identity-mismatch");
      }
      return {
        evidenceId: expected.evidenceId,
        event: taskEvent,
      } satisfies {
        readonly evidenceId: string;
        readonly event: AgentControlTaskEvent;
      };
    },
  );

  const replayFirst = Effect.fn("AgentControlTaskVerificationFinalizer.replayFirst")(function* (
    handoffId: string,
  ) {
    const counts = yield* sql<{ readonly count: number }>`
      SELECT
        (SELECT count(*) FROM main.agent_control_task_verification_finalization_evidence
          WHERE handoff_id = ${handoffId}) +
        (SELECT count(*) FROM main.agent_control_task_verification_finalization_receipts
          WHERE handoff_id = ${handoffId}) +
        (SELECT count(*) FROM main.agent_control_task_verification_finalization_markers
          WHERE handoff_id = ${handoffId}) +
        (SELECT count(*) FROM main.agent_control_task_verification_finalization_publications
          WHERE handoff_id = ${handoffId}) AS count
    `.pipe(Effect.mapError((cause) => error(handoffId, "replay-count", "persistence", cause)));
    const count = counts[0]?.count ?? 0;
    if (count === 0)
      return Option.none<{
        readonly evidenceId: string;
        readonly event: AgentControlTaskEvent;
      }>();
    if (count !== 4) return yield* error(handoffId, "replay-partial", "partial-replay");
    const source = yield* loadSource(handoffId);
    return Option.some(yield* validateReplay(handoffId, source));
  });

  const finalizeInTransaction = Effect.fn(
    "AgentControlTaskVerificationFinalizer.finalizeInTransaction",
  )(function* (handoffId: string) {
    const source = yield* loadSource(handoffId);
    const authority = yield* loadTaskAuthority(source);
    const previous = authority.projection;
    const taskSourceEvent = authority.events[source.row.verificationTaskRevision - 1];
    if (taskSourceEvent === undefined) {
      return yield* error(handoffId, "task-source-event", "authority-conflict");
    }
    const built = yield* buildFinalization(source, previous, taskSourceEvent);
    yield* hooks.beforeTransaction(handoffId);
    yield* hooks.afterAuthoritativeRead(handoffId);
    const draft: AgentControlTaskEventDraft = {
      eventId: built.eventId,
      type: "agentControl.task.finalizedAfterVerification",
      aggregateKind: "task",
      aggregateId: previous.taskId,
      occurredAt: source.row.finalizedAt,
      commandId: built.commandId,
      causationEventId: EventId.make(source.row.terminalRuntimeEventId),
      correlationId: built.commandId,
      authority: "system",
      metadata: { schemaVersion: 1 },
      payload: built.payload,
    };
    const appended = yield* taskEvents
      .append({
        taskId: previous.taskId,
        expectedStreamVersion: previous.revision,
        events: [draft],
      })
      .pipe(
        Effect.mapError((cause) =>
          error(
            handoffId,
            "append-task-event",
            cause._tag === "AgentControlTaskStreamVersionConflictError"
              ? "revision-conflict"
              : "persistence",
            cause,
          ),
        ),
      );
    const event = appended[0];
    if (appended.length !== 1 || event === undefined) {
      return yield* error(handoffId, "append-task-event", "persistence");
    }
    yield* taskProjection
      .projectEvent(event)
      .pipe(
        Effect.mapError((cause) => error(handoffId, "project-task-event", "persistence", cause)),
      );
    yield* hooks.afterTaskProjection(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_task_verification_finalization_evidence (
        task_finalization_evidence_id, receipt_id, marker_id,
        finalization_command_id, finalization_fingerprint, finalization_json,
        verification_evidence_id, verification_receipt_id, verification_marker_id,
        verification_finalization_command_id, verification_finalization_fingerprint,
        verification_finalization_marker_fingerprint, handoff_id, handoff_fingerprint,
        project_id, task_id, verification_task_revision, previous_task_revision,
        github_intake_sequence, source_identity_fingerprint,
        task_source_event_id, task_source_event_sequence, task_source_event_stream_version,
        delivery_terminal_state, verification_outcome, terminal_cause,
        terminal_runtime_event_id, evaluation_authority, evaluation_id,
        evaluation_evidence_id, evaluation_receipt_id, evaluation_marker_id,
        evaluation_disposition, verification_verdict, invalid_output_code,
        terminal_stage_event_id, terminal_stage_event_sequence,
        terminal_stage_run_id, terminal_stage_event_stream_version, released_lease_event_id,
        released_lease_event_sequence, released_lease_event_stream_version,
        released_lease_id,
        task_event_id, task_event_sequence, task_event_stream_version, finalized_at
      ) VALUES (
        ${built.evidenceId}, ${built.receiptId}, ${built.markerId},
        ${built.commandId}, ${built.finalizationFingerprint}, ${built.finalizationJson},
        ${source.row.verificationEvidenceId}, ${source.row.verificationReceiptId},
        ${source.row.verificationMarkerId}, ${source.row.verificationCommandId},
        ${source.row.verificationFingerprint}, ${source.row.verificationMarkerFingerprint},
        ${source.row.handoffId}, ${source.row.handoffFingerprint}, ${source.row.projectId},
        ${source.row.taskId}, ${source.row.verificationTaskRevision}, ${previous.revision},
        ${source.row.githubIntakeSequence}, ${source.row.sourceIdentityFingerprint},
        ${taskSourceEvent.eventId}, ${taskSourceEvent.sequence}, ${taskSourceEvent.streamVersion},
        ${source.row.deliveryTerminalState}, ${source.row.verificationOutcome},
        ${source.row.terminalCause}, ${source.row.terminalRuntimeEventId},
        ${source.row.evaluationAuthority}, ${source.row.evaluationId},
        ${source.row.evaluationEvidenceId}, ${source.row.evaluationReceiptId},
        ${source.row.evaluationMarkerId}, ${source.row.evaluationDisposition},
        ${source.row.verificationVerdict}, ${source.row.invalidOutputCode},
        ${source.row.stageEventId}, ${source.row.stageEventSequence},
        ${source.row.stageRunId}, ${source.row.stageEventStreamVersion}, ${source.row.leaseEventId},
        ${source.row.leaseEventSequence}, ${source.row.leaseEventStreamVersion},
        ${source.row.leaseId},
        ${event.eventId}, ${event.sequence}, ${event.streamVersion}, ${source.row.finalizedAt}
      )
    `.pipe(Effect.mapError((cause) => error(handoffId, "insert-evidence", "persistence", cause)));
    yield* hooks.afterEvidence(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_task_verification_finalization_receipts (
        receipt_id, marker_id, task_finalization_evidence_id,
        finalization_command_id, finalization_fingerprint,
        verification_marker_id, handoff_id, task_id,
        task_event_id, task_event_sequence, task_event_stream_version,
        status, accepted_at
      ) VALUES (
        ${built.receiptId}, ${built.markerId}, ${built.evidenceId},
        ${built.commandId}, ${built.finalizationFingerprint},
        ${source.row.verificationMarkerId}, ${source.row.handoffId}, ${source.row.taskId},
        ${event.eventId}, ${event.sequence}, ${event.streamVersion},
        'accepted', ${source.row.finalizedAt}
      )
    `.pipe(Effect.mapError((cause) => error(handoffId, "insert-receipt", "persistence", cause)));
    yield* hooks.afterReceipt(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_task_verification_finalization_publications (
        handoff_id, marker_id, task_finalization_evidence_id, task_id,
        task_event_id, task_event_stream_version, publication_owner_id,
        status, revision, claim_fence, created_at, claimed_at, lease_expires_at, completed_at
      ) VALUES (
        ${source.row.handoffId}, ${built.markerId}, ${built.evidenceId}, ${source.row.taskId},
        ${event.eventId}, ${event.streamVersion}, NULL,
        'pending', 1, 0, ${source.row.finalizedAt}, NULL, NULL, NULL
      )
    `.pipe(
      Effect.mapError((cause) => error(handoffId, "insert-publication", "persistence", cause)),
    );
    yield* hooks.beforeMarker(handoffId);
    yield* sql`
      INSERT INTO main.agent_control_task_verification_finalization_markers (
        marker_id, marker_fingerprint, receipt_id, task_finalization_evidence_id,
        finalization_command_id, finalization_fingerprint,
        verification_marker_id, handoff_id, task_id, task_event_id,
        task_event_sequence, task_event_stream_version, committed_at
      ) VALUES (
        ${built.markerId}, ${built.markerFingerprint}, ${built.receiptId}, ${built.evidenceId},
        ${built.commandId}, ${built.finalizationFingerprint},
        ${source.row.verificationMarkerId}, ${source.row.handoffId}, ${source.row.taskId},
        ${event.eventId}, ${event.sequence}, ${event.streamVersion}, ${source.row.finalizedAt}
      )
    `.pipe(Effect.mapError((cause) => error(handoffId, "insert-marker", "persistence", cause)));
    return {
      handoffId,
      taskFinalizationEvidenceId: built.evidenceId,
      event,
    } satisfies AgentControlTaskVerificationFinalizationPublication;
  });

  const processFresh = Effect.fn("AgentControlTaskVerificationFinalizer.processFresh")(function* (
    handoffId: string,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const nativeCommitted = yield* Ref.make(false);
        const nativeHooks = yield* NodeSqliteTransactionHooks;
        const transactionExit = yield* Effect.exit(
          restore(
            sql.withTransaction(finalizeInTransaction(handoffId)).pipe(
              Effect.provideService(NodeSqliteTransactionHooks, {
                afterAnyCommitBeforeReturn: () =>
                  Ref.set(nativeCommitted, true).pipe(
                    Effect.andThen(nativeHooks.afterAnyCommitBeforeReturn?.() ?? Effect.void),
                  ),
                afterCommitBeforeReturn: (observation) =>
                  (String(observation.boundary) === "agent-control-task-verification-finalization"
                    ? Ref.set(nativeCommitted, true)
                    : Effect.void
                  ).pipe(Effect.andThen(nativeHooks.afterCommitBeforeReturn(observation))),
              }),
            ),
          ),
        );
        if (Exit.isFailure(transactionExit)) {
          const replay = yield* replayFirst(handoffId);
          if (Option.isSome(replay)) {
            if (yield* Ref.get(nativeCommitted)) {
              const publicationExit = yield* Effect.exit(
                drainPublication(handoffId, replay.value.event),
              );
              if (Exit.isFailure(publicationExit)) {
                return yield* Effect.failCause(
                  Cause.combine(transactionExit.cause, publicationExit.cause),
                );
              }
              return yield* Effect.failCause(transactionExit.cause);
            }
            const publicationExit = yield* Effect.exit(
              drainPublication(handoffId, replay.value.event),
            );
            if (Exit.isFailure(publicationExit)) {
              return yield* Effect.failCause(publicationExit.cause);
            }
            return {
              _tag: "Replayed",
              taskFinalizationEvidenceId: replay.value.evidenceId,
            } as const;
          }
          return yield* Effect.failCause(transactionExit.cause);
        }
        const publication = transactionExit.value;
        const afterCommitExit = yield* Effect.exit(restore(hooks.afterCommit(handoffId)));
        const publicationExit = yield* Effect.exit(drainPublication(handoffId, publication.event));
        if (Exit.isFailure(afterCommitExit)) {
          if (Exit.isFailure(publicationExit)) {
            return yield* Effect.failCause(
              Cause.combine(afterCommitExit.cause, publicationExit.cause),
            );
          }
          return yield* Effect.failCause(afterCommitExit.cause);
        }
        if (Exit.isFailure(publicationExit)) {
          return yield* Effect.failCause(publicationExit.cause);
        }
        if (publicationExit.value) yield* restore(hooks.afterPublication(handoffId));
        return {
          _tag: "Finalized",
          taskFinalizationEvidenceId: publication.taskFinalizationEvidenceId,
        } as const;
      }),
    );
  });

  const processHandoff: AgentControlTaskVerificationFinalizerShape["processHandoff"] = (
    handoffId,
  ) =>
    Effect.gen(function* () {
      const replay = yield* replayFirst(handoffId);
      if (Option.isSome(replay)) {
        yield* drainPublication(handoffId, replay.value.event);
        return {
          _tag: "Replayed",
          taskFinalizationEvidenceId: replay.value.evidenceId,
        } as const;
      }
      return yield* processFresh(handoffId);
    }).pipe(
      Effect.mapError((cause) =>
        isFinalizerError(cause) ? cause : error(handoffId, "process", "persistence", cause),
      ),
    );

  const processCandidateSafely = Effect.fn(
    "AgentControlTaskVerificationFinalizer.processCandidateSafely",
  )(function* (handoffId: string) {
    const retryOnce = processHandoff(handoffId).pipe(
      Effect.catchIf(
        (cause) => cause.reason === "persistence" || cause.reason === "revision-conflict",
        (cause) =>
          Effect.logWarning("retrying Task Verification finalization after a transient race", {
            handoffId,
            operation: cause.operation,
            reason: cause.reason,
          }).pipe(Effect.andThen(processHandoff(handoffId))),
      ),
    );
    yield* retryOnce.pipe(
      Effect.catchIf(isFinalizerError, (cause) =>
        Effect.logError("task Verification finalization candidate failed", {
          handoffId,
          operation: cause.operation,
          reason: cause.reason,
        }),
      ),
    );
  });

  const listCandidates = (afterExclusive = "", limit = 64) =>
    sql
      .unsafe<{ readonly handoffId: string }>(TASK_VERIFICATION_FINALIZATION_CANDIDATES_SQL, [
        afterExclusive,
        Math.max(1, Math.min(1000, Math.floor(limit))),
      ])
      .pipe(Effect.mapError((cause) => error("recovery", "list-candidates", "persistence", cause)));

  const listPublicationRecoveryCandidates = (afterExclusive = "", limit = 64) =>
    sql
      .unsafe<{ readonly handoffId: string }>(
        TASK_VERIFICATION_FINALIZATION_PUBLICATION_RECOVERY_SQL,
        [afterExclusive, Math.max(1, Math.min(1000, Math.floor(limit)))],
      )
      .pipe(
        Effect.mapError((cause) =>
          error("recovery", "list-publication-candidates", "persistence", cause),
        ),
      );

  const recoverCommittedPublicationSafely = Effect.fn(
    "AgentControlTaskVerificationFinalizer.recoverCommittedPublicationSafely",
  )(function* (handoffId: string) {
    yield* Effect.gen(function* () {
      const replay = yield* replayFirst(handoffId);
      if (Option.isNone(replay)) {
        return yield* error(handoffId, "recover-publication-marker", "partial-replay");
      }
      yield* drainPublication(handoffId, replay.value.event);
    }).pipe(
      Effect.catchIf(isFinalizerError, (cause) =>
        Effect.logError("task Verification finalization publication recovery failed", {
          handoffId,
          operation: cause.operation,
          reason: cause.reason,
        }),
      ),
    );
  });

  const recover = Effect.gen(function* () {
    const pageSize = hooks.recoveryPageSize ?? 64;
    let cursor = "";
    while (true) {
      const candidates = yield* listCandidates(cursor, pageSize);
      if (candidates.length === 0) break;
      yield* Effect.forEach(candidates, ({ handoffId }) => processCandidateSafely(handoffId), {
        concurrency: 1,
        discard: true,
      });
      cursor = candidates.at(-1)!.handoffId;
      if (candidates.length < pageSize) break;
    }
    cursor = "";
    while (true) {
      const candidates = yield* listPublicationRecoveryCandidates(cursor, pageSize);
      if (candidates.length === 0) break;
      yield* Effect.forEach(
        candidates,
        ({ handoffId }) => recoverCommittedPublicationSafely(handoffId),
        { concurrency: 1, discard: true },
      );
      cursor = candidates.at(-1)!.handoffId;
      if (candidates.length < pageSize) break;
    }
  });
  const processSafely = (handoffId: string | null) =>
    handoffId === null ? recover : processCandidateSafely(handoffId);
  let nextAttemptId = 0;
  let activeWorker:
    | {
        readonly attemptId: number;
        readonly drain: Effect.Effect<void, AgentControlTaskVerificationFinalizerError>;
      }
    | undefined;
  let terminalDrain: Effect.Effect<void, AgentControlTaskVerificationFinalizerError> = Effect.void;
  const prepare: AgentControlTaskVerificationFinalizerShape["prepare"] = Effect.fn(
    "AgentControlTaskVerificationFinalizer.prepare",
  )(function* (activation) {
    const ownerScope = yield* Scope.Scope;
    const worker = yield* makeDrainableWorker(processSafely, { failureMode: "observable" });
    const publicationDeadlineFibers = yield* FiberMap.make<string>();
    const scheduledPublicationDeadlines = new Map<string, string>();
    nextAttemptId += 1;
    const attemptId = nextAttemptId;
    const schedule = Effect.fn("AgentControlTaskVerificationFinalizer.schedulePublicationDeadline")(
      function* (handoffId: string, leaseExpiresAt: string) {
        if (scheduledPublicationDeadlines.get(handoffId) === leaseExpiresAt) return;
        const deadlineEpochMillis = Date.parse(leaseExpiresAt);
        if (!Number.isSafeInteger(deadlineEpochMillis)) {
          return yield* Effect.die(
            new Error("task Verification publication deadline must be integer milliseconds"),
          );
        }
        scheduledPublicationDeadlines.set(handoffId, leaseExpiresAt);
        const awaitDeadline = Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          yield* Effect.sleep(Duration.millis(Math.max(0, deadlineEpochMillis - now)));
        });
        const wakeUp = awaitDeadline.pipe(
          Effect.andThen(worker.enqueue(handoffId)),
          Effect.ensuring(
            Effect.sync(() => {
              if (scheduledPublicationDeadlines.get(handoffId) === leaseExpiresAt) {
                scheduledPublicationDeadlines.delete(handoffId);
              }
            }),
          ),
        );
        yield* FiberMap.run(publicationDeadlineFibers, handoffId, wakeUp, {
          startImmediately: true,
        });
      },
    );
    activeWorker = { attemptId, drain: worker.drain };
    activePublicationDeadlineScheduler = { attemptId, schedule };
    yield* Scope.addFinalizer(
      ownerScope,
      Effect.sync(() => {
        if (activeWorker?.attemptId !== attemptId) return;
        terminalDrain = activeWorker.drain;
        activeWorker = undefined;
        if (activePublicationDeadlineScheduler?.attemptId === attemptId) {
          activePublicationDeadlineScheduler = undefined;
        }
      }),
    );
    const leaseEvents = yield* leaseEngine.subscribeDomainEvents;
    yield* Effect.forkScoped(
      Stream.runForEach(leaseEvents, (event) =>
        event.type === "agentControl.stageRunLease.releasedAfterVerification"
          ? activation.pipe(Effect.andThen(worker.enqueue(event.payload.handoffId)))
          : Effect.void,
      ),
      { startImmediately: true },
    );
    yield* Effect.forkScoped(activation.pipe(Effect.andThen(worker.enqueue(null))), {
      startImmediately: true,
    });
  });

  return AgentControlTaskVerificationFinalizer.of({
    processHandoff,
    recover,
    prepare,
    drain: Effect.suspend(() => activeWorker?.drain ?? terminalDrain),
  });
});

export const AgentControlTaskVerificationFinalizerLive = Layer.effect(
  AgentControlTaskVerificationFinalizer,
  make,
);
