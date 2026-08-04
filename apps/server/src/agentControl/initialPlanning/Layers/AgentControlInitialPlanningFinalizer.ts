import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  OrchestrationEvent,
  type OrchestrationProposedPlan,
  type AgentControlStageRunEvent,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEventDraft,
} from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionThreadProposedPlan } from "../../../persistence/Services/ProjectionThreadProposedPlans.ts";
import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseCanonicalJson,
  sha256Utf8,
  type JsonValue,
} from "../eventEvidence.ts";
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
} from "../identity.ts";
import {
  deriveInitialPlanningFinalizationCommandId,
  deriveInitialPlanningFinalizationMarkerId,
  deriveInitialPlanningLeaseReleaseEventId,
  deriveInitialPlanningResultEvidenceId,
  deriveInitialPlanningStageStartCommandId,
  deriveInitialPlanningStageStartedEventId,
  deriveInitialPlanningTerminalStageEventId,
  fingerprintInitialPlanningFinalization,
} from "../finalizationIdentity.ts";
import type { AgentControlInitialPlanningClaim } from "../model.ts";
import {
  AgentControlInitialPlanningFinalizer,
  AgentControlInitialPlanningFinalizerError,
  type AgentControlInitialPlanningFinalizationPublication,
  type AgentControlInitialPlanningFinalizerShape,
  type AgentControlInitialPlanningFinalizerResult,
} from "../Services/AgentControlInitialPlanningFinalizer.ts";
import { AgentControlInitialPlanningFinalizerHooks } from "../Services/AgentControlInitialPlanningFinalizerHooks.ts";
import { AgentControlInitialPlanningHandoffStore } from "../Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeup } from "../Services/AgentControlInitialPlanningWakeup.ts";
import { loadAuthoritativeStageRunState } from "../../stageRunLease/authoritative.ts";
import { loadAuthoritativeLeaseState } from "../../stageRunLease/authoritative.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { projectAgentControlStageRunEvent } from "../../stageRun/projector.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { projectAgentControlStageRunLeaseEvent } from "../../stageRunLease/projector.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";

const StoredOrchestrationRow = Schema.Struct({
  sequence: Schema.Number,
  streamVersion: Schema.Number,
  eventId: Schema.String,
  aggregateKind: Schema.String,
  aggregateId: Schema.String,
  type: Schema.String,
  occurredAt: Schema.String,
  commandId: Schema.NullOr(Schema.String),
  causationEventId: Schema.NullOr(Schema.String),
  correlationId: Schema.NullOr(Schema.String),
  actorKind: Schema.String,
  payloadJson: Schema.String,
  metadataJson: Schema.String,
});
const decodeStoredOrchestrationRow = Schema.decodeUnknownEffect(StoredOrchestrationRow);
const decodeOrchestrationEvent = Schema.decodeUnknownEffect(OrchestrationEvent);
const decodeProjectionPlan = Schema.decodeUnknownEffect(ProjectionThreadProposedPlan);

const corruptHandoffStoreOperations = new Set([
  "decode-evidence",
  "decode-delivery",
  "decode-delivery-invariant",
  "decode-model-selection",
  "encode-model-selection",
  "event-template-json",
  "evidence-invariant",
  "delivery-evidence-invariant",
  "non-unique-evidence",
]);

interface StoredOrchestrationEvent {
  readonly event: OrchestrationEvent;
  readonly streamVersion: number;
  readonly actorKind: string;
  readonly payloadJson: string;
  readonly metadataJson: string;
}

interface OrchestrationPlanningEvidence {
  readonly started: StoredOrchestrationEvent;
  readonly terminal: StoredOrchestrationEvent | null;
  readonly proposedPlan: OrchestrationProposedPlan | null;
  readonly proposedPlanJson: string | null;
  readonly proposedPlanDigest: string | null;
  readonly proposedPlanEvent: StoredOrchestrationEvent | null;
}

interface PlanningBinding {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly threadId: string;
  readonly providerInstanceId: string;
  readonly providerTurnId: string;
  readonly runtimeMode: "approval-required" | "full-access";
  readonly providerAcceptedAt: string;
  readonly terminalAt: string | null;
  readonly deliveryTerminalState: "completed" | "failed" | "interrupted" | null;
}

const StartMarkerRow = Schema.Struct({
  startCommandId: Schema.String,
  startFingerprint: Schema.String,
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  projectId: Schema.String,
  taskId: Schema.String,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  controlledThreadReservationId: Schema.String,
  threadId: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Number,
  providerDeliveryId: Schema.String,
  providerInstanceId: Schema.String,
  providerTurnId: Schema.String,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: Schema.String,
  providerAcceptedAt: Schema.String,
  deliveryRevision: Schema.Number,
  orchestrationStartedEventId: Schema.String,
  orchestrationStartedSequence: Schema.Number,
  orchestrationStartedStreamVersion: Schema.Number,
  stageEventId: Schema.String,
  stageEventSequence: Schema.Number,
  stageEventStreamVersion: Schema.Number,
  recordedAt: Schema.String,
});
type StartMarker = typeof StartMarkerRow.Type;
const decodeStartMarker = Schema.decodeUnknownEffect(StartMarkerRow);

const FinalizationReplayRow = Schema.Struct({
  resultEvidenceId: Schema.String,
  finalizationCommandId: Schema.String,
  finalizationFingerprint: Schema.String,
  outcome: Schema.Literals(["succeeded", "failed", "cancelled"]),
  handoffId: Schema.String,
  handoffFingerprint: Schema.String,
  projectId: Schema.String,
  taskId: Schema.String,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  controlledThreadReservationId: Schema.String,
  threadId: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Number,
  providerDeliveryId: Schema.String,
  providerInstanceId: Schema.String,
  providerTurnId: Schema.String,
  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
  modelSelectionFingerprint: Schema.String,
  deliveryTerminalState: Schema.Literals(["completed", "failed", "interrupted"]),
  deliveryRevision: Schema.Number,
  terminalAt: Schema.String,
  orchestrationStartedEventId: Schema.String,
  orchestrationStartedSequence: Schema.Number,
  orchestrationTerminalEventId: Schema.String,
  orchestrationTerminalSequence: Schema.Number,
  planId: Schema.NullOr(Schema.String),
  planEventId: Schema.NullOr(Schema.String),
  planEventSequence: Schema.NullOr(Schema.Number),
  proposedPlanJson: Schema.NullOr(Schema.String),
  proposedPlanDigest: Schema.NullOr(Schema.String),
  stageEventId: Schema.String,
  stageEventSequence: Schema.Number,
  stageEventStreamVersion: Schema.Number,
  leaseEventId: Schema.String,
  leaseEventSequence: Schema.Number,
  leaseEventStreamVersion: Schema.Number,
  finalizedAt: Schema.String,
  receiptFingerprint: Schema.String,
  receiptEvidenceId: Schema.String,
  receiptHandoffId: Schema.String,
  receiptOutcome: Schema.String,
  receiptStageEventId: Schema.String,
  receiptStageEventSequence: Schema.Number,
  receiptLeaseEventId: Schema.String,
  receiptLeaseEventSequence: Schema.Number,
  receiptAcceptedAt: Schema.String,
  markerId: Schema.String,
  markerFingerprint: Schema.String,
  markerCommandId: Schema.String,
  markerEvidenceId: Schema.String,
  markerHandoffId: Schema.String,
  markerCommittedAt: Schema.String,
});
type FinalizationReplay = typeof FinalizationReplayRow.Type;
const decodeFinalizationReplay = Schema.decodeUnknownEffect(FinalizationReplayRow);
const isFinalizerError = Schema.is(AgentControlInitialPlanningFinalizerError);

const finalizerError = (
  handoffId: string,
  operation: string,
  reason: AgentControlInitialPlanningFinalizerError["reason"],
  cause?: unknown,
) =>
  new AgentControlInitialPlanningFinalizerError({
    handoffId,
    operation,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const bindingParts = (
  evidence: {
    readonly handoffId: string;
    readonly handoffFingerprint: string;
    readonly projectId: string;
    readonly taskId: string;
    readonly taskRevision: number;
    readonly githubIntakeSequence: number;
    readonly sourceIdentityFingerprint: string;
    readonly controlledThreadReservationId: string;
    readonly threadId: string;
    readonly stageRunId: string;
    readonly attemptId: string;
    readonly leaseId: string;
    readonly leaseHolderId: string;
    readonly fenceToken: number;
    readonly providerDeliveryId: string;
    readonly providerInstanceId: string;
    readonly runtimeMode: string;
  },
  providerTurnId: string,
  modelSelectionFingerprint: string,
) => [
  evidence.handoffId,
  evidence.handoffFingerprint,
  evidence.projectId,
  evidence.taskId,
  String(evidence.taskRevision),
  String(evidence.githubIntakeSequence),
  evidence.sourceIdentityFingerprint,
  evidence.controlledThreadReservationId,
  evidence.threadId,
  evidence.stageRunId,
  evidence.attemptId,
  evidence.leaseId,
  evidence.leaseHolderId,
  String(evidence.fenceToken),
  evidence.providerDeliveryId,
  evidence.providerInstanceId,
  providerTurnId,
  evidence.runtimeMode,
  modelSelectionFingerprint,
];

const startFingerprintParts = (
  parts: ReadonlyArray<string>,
  input: {
    readonly providerAcceptedAt: string;
    readonly deliveryRevision: number;
    readonly started: StoredOrchestrationEvent;
    readonly stageEvent: AgentControlStageRunEvent;
  },
) => [
  ...parts,
  input.providerAcceptedAt,
  String(input.deliveryRevision),
  input.started.event.eventId,
  String(input.started.event.sequence),
  String(input.started.streamVersion),
  input.stageEvent.eventId,
  String(input.stageEvent.sequence),
  String(input.stageEvent.streamVersion),
  input.stageEvent.occurredAt,
];

const resultFingerprintParts = (row: {
  readonly handoffId: string;
  readonly handoffFingerprint: string;
  readonly projectId: string;
  readonly taskId: string;
  readonly taskRevision: number;
  readonly githubIntakeSequence: number;
  readonly sourceIdentityFingerprint: string;
  readonly controlledThreadReservationId: string;
  readonly threadId: string;
  readonly stageRunId: string;
  readonly attemptId: string;
  readonly leaseId: string;
  readonly leaseHolderId: string;
  readonly fenceToken: number;
  readonly providerDeliveryId: string;
  readonly providerInstanceId: string;
  readonly providerTurnId: string;
  readonly runtimeMode: string;
  readonly modelSelectionFingerprint: string;
  readonly outcome: string;
  readonly deliveryTerminalState: string;
  readonly deliveryRevision: number;
  readonly terminalAt: string;
  readonly orchestrationStartedEventId: string;
  readonly orchestrationStartedSequence: number;
  readonly orchestrationTerminalEventId: string;
  readonly orchestrationTerminalSequence: number;
  readonly planId: string | null;
  readonly planEventId: string | null;
  readonly planEventSequence: number | null;
  readonly proposedPlanJson: string | null;
  readonly proposedPlanDigest: string | null;
  readonly stageEventId: string;
  readonly stageEventSequence: number;
  readonly stageEventStreamVersion: number;
  readonly leaseEventId: string;
  readonly leaseEventSequence: number;
  readonly leaseEventStreamVersion: number;
  readonly finalizedAt: string;
}) => [
  row.handoffId,
  row.handoffFingerprint,
  row.projectId,
  row.taskId,
  String(row.taskRevision),
  String(row.githubIntakeSequence),
  row.sourceIdentityFingerprint,
  row.controlledThreadReservationId,
  row.threadId,
  row.stageRunId,
  row.attemptId,
  row.leaseId,
  row.leaseHolderId,
  String(row.fenceToken),
  row.providerDeliveryId,
  row.providerInstanceId,
  row.providerTurnId,
  row.runtimeMode,
  row.modelSelectionFingerprint,
  row.outcome,
  row.deliveryTerminalState,
  String(row.deliveryRevision),
  row.terminalAt,
  row.orchestrationStartedEventId,
  String(row.orchestrationStartedSequence),
  row.orchestrationTerminalEventId,
  String(row.orchestrationTerminalSequence),
  row.planId ?? "",
  row.planEventId ?? "",
  String(row.planEventSequence ?? 0),
  row.proposedPlanJson ?? "",
  row.proposedPlanDigest ?? "",
  row.stageEventId,
  String(row.stageEventSequence),
  String(row.stageEventStreamVersion),
  row.leaseEventId,
  String(row.leaseEventSequence),
  String(row.leaseEventStreamVersion),
  row.finalizedAt,
];

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* AgentControlInitialPlanningHandoffStore;
  const wakeup = yield* AgentControlInitialPlanningWakeup;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const stageProjection = yield* AgentControlStageRunProjection;
  const stageEngine = yield* AgentControlStageRunEngine;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseProjection = yield* AgentControlStageRunLeaseProjection;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const hooks = yield* AgentControlInitialPlanningFinalizerHooks;
  const publications =
    yield* PubSub.unbounded<AgentControlInitialPlanningFinalizationPublication>();

  const readOrchestrationHistory = Effect.fn(
    "AgentControlInitialPlanningFinalizer.readOrchestrationHistory",
  )(function* (binding: PlanningBinding) {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT sequence, stream_version AS "streamVersion", event_id AS "eventId",
        aggregate_kind AS "aggregateKind", stream_id AS "aggregateId",
        event_type AS type, occurred_at AS "occurredAt", command_id AS "commandId",
        causation_event_id AS "causationEventId", correlation_id AS "correlationId",
        actor_kind AS "actorKind", payload_json AS "payloadJson",
        metadata_json AS "metadataJson", CAST(payload_json AS BLOB) AS payload_bytes,
        CAST(metadata_json AS BLOB) AS metadata_bytes
      FROM orchestration_events
      WHERE aggregate_kind = 'thread' AND stream_id = ${binding.threadId}
      ORDER BY stream_version ASC, sequence ASC
    `.pipe(
      Effect.mapError((cause) =>
        finalizerError(binding.handoffId, "read-orchestration-history", "persistence", cause),
      ),
    );
    const decoded: Array<StoredOrchestrationEvent> = [];
    let previousStreamVersion: number | null = null;
    let previousSequence = 0;
    for (const raw of rows) {
      const row = yield* decodeStoredOrchestrationRow(raw).pipe(
        Effect.mapError((cause) =>
          finalizerError(
            binding.handoffId,
            "decode-orchestration-row",
            "corrupt-orchestration-history",
            cause,
          ),
        ),
      );
      if (
        !Number.isInteger(row.sequence) ||
        row.sequence <= previousSequence ||
        !Number.isInteger(row.streamVersion) ||
        (previousStreamVersion === null && row.streamVersion !== 0) ||
        (previousStreamVersion !== null && row.streamVersion !== previousStreamVersion + 1)
      ) {
        return yield* finalizerError(
          binding.handoffId,
          "orchestration-order",
          "corrupt-orchestration-history",
        );
      }
      previousSequence = row.sequence;
      previousStreamVersion = row.streamVersion;
      const payloadJson = yield* Effect.try({
        try: () => {
          const source = decodeCanonicalUtf8Bytes(raw.payload_bytes);
          parseCanonicalJson(source);
          if (source !== row.payloadJson) throw new Error("payload TEXT/BLOB mismatch");
          return source;
        },
        catch: (cause) =>
          finalizerError(
            binding.handoffId,
            "canonical-orchestration-payload",
            "corrupt-orchestration-history",
            cause,
          ),
      });
      const metadataJson = yield* Effect.try({
        try: () => {
          const source = decodeCanonicalUtf8Bytes(raw.metadata_bytes);
          parseCanonicalJson(source);
          if (source !== row.metadataJson) throw new Error("metadata TEXT/BLOB mismatch");
          return source;
        },
        catch: (cause) =>
          finalizerError(
            binding.handoffId,
            "canonical-orchestration-metadata",
            "corrupt-orchestration-history",
            cause,
          ),
      });
      const event = yield* decodeOrchestrationEvent({
        sequence: row.sequence,
        eventId: row.eventId,
        aggregateKind: row.aggregateKind,
        aggregateId: row.aggregateId,
        type: row.type,
        occurredAt: row.occurredAt,
        commandId: row.commandId,
        causationEventId: row.causationEventId,
        correlationId: row.correlationId,
        payload: parseCanonicalJson(payloadJson),
        metadata: parseCanonicalJson(metadataJson),
      }).pipe(
        Effect.mapError((cause) =>
          finalizerError(
            binding.handoffId,
            "decode-orchestration-event",
            "corrupt-orchestration-history",
            cause,
          ),
        ),
      );
      decoded.push({
        event,
        streamVersion: row.streamVersion,
        actorKind: row.actorKind,
        payloadJson,
        metadataJson,
      });
    }
    return decoded;
  });

  const reconstructOrchestrationEvidence = Effect.fn(
    "AgentControlInitialPlanningFinalizer.reconstructOrchestrationEvidence",
  )(function* (binding: PlanningBinding) {
    const history = yield* readOrchestrationHistory(binding);
    const providerEvent = (entry: StoredOrchestrationEvent) =>
      entry.actorKind === "provider" &&
      entry.event.commandId !== null &&
      entry.event.commandId.startsWith("provider:");
    const startCandidates = history.filter((entry) => {
      if (
        entry.event.type !== "thread.session-set" ||
        !providerEvent(entry) ||
        entry.event.occurredAt !== binding.providerAcceptedAt
      ) {
        return false;
      }
      const session = entry.event.payload.session;
      return (
        entry.event.payload.threadId === binding.threadId &&
        session.threadId === binding.threadId &&
        session.status === "running" &&
        session.activeTurnId === binding.providerTurnId &&
        session.providerInstanceId === binding.providerInstanceId &&
        session.runtimeMode === binding.runtimeMode &&
        session.providerName !== null
      );
    });
    if (startCandidates.length === 0) return null;
    const started = startCandidates[0]!;
    if (startCandidates.some((candidate) => candidate.payloadJson !== started.payloadJson)) {
      return yield* finalizerError(
        binding.handoffId,
        "ambiguous-provider-start",
        "corrupt-orchestration-history",
      );
    }
    const startedSession = started.event as Extract<
      OrchestrationEvent,
      { type: "thread.session-set" }
    >;
    const providerName = startedSession.payload.session.providerName;

    let terminal: StoredOrchestrationEvent | null = null;
    if (binding.terminalAt !== null) {
      const candidates = history.filter((entry) => {
        if (
          entry.event.type !== "thread.session-set" ||
          !providerEvent(entry) ||
          entry.event.sequence <= started.event.sequence ||
          entry.event.occurredAt !== binding.terminalAt
        ) {
          return false;
        }
        const session = entry.event.payload.session;
        const statusMatches =
          binding.deliveryTerminalState === "completed"
            ? session.status === "ready"
            : binding.deliveryTerminalState === "failed"
              ? session.status === "error"
              : session.status === "ready" || session.status === "error";
        return (
          statusMatches &&
          entry.event.payload.threadId === binding.threadId &&
          session.threadId === binding.threadId &&
          session.activeTurnId === null &&
          session.providerInstanceId === binding.providerInstanceId &&
          session.providerName === providerName &&
          session.runtimeMode === binding.runtimeMode
        );
      });
      if (candidates.length === 0)
        return {
          started,
          terminal: null,
          proposedPlan: null,
          proposedPlanJson: null,
          proposedPlanDigest: null,
          proposedPlanEvent: null,
        } satisfies OrchestrationPlanningEvidence;
      terminal = candidates[0]!;
      if (candidates.some((candidate) => candidate.payloadJson !== terminal!.payloadJson)) {
        return yield* finalizerError(
          binding.handoffId,
          "ambiguous-provider-terminal",
          "corrupt-orchestration-history",
        );
      }
    }

    const relevantPlanEvents = history.filter(
      (
        entry,
      ): entry is StoredOrchestrationEvent & {
        readonly event: Extract<OrchestrationEvent, { type: "thread.proposed-plan-upserted" }>;
      } =>
        entry.event.type === "thread.proposed-plan-upserted" &&
        entry.event.sequence > started.event.sequence &&
        entry.event.payload.threadId === binding.threadId &&
        entry.event.payload.proposedPlan.turnId === binding.providerTurnId,
    );
    if (relevantPlanEvents.some((entry) => !providerEvent(entry))) {
      return yield* finalizerError(
        binding.handoffId,
        "non-provider-plan-event",
        "corrupt-orchestration-history",
      );
    }
    let proposedPlan: OrchestrationProposedPlan | null = null;
    let proposedPlanEvent: StoredOrchestrationEvent | null = null;
    let proposedPlanJson: string | null = null;
    for (const entry of relevantPlanEvents) {
      const candidate = entry.event.payload.proposedPlan;
      if (
        candidate.id !== `plan:${binding.threadId}:turn:${binding.providerTurnId}` ||
        candidate.planMarkdown.trim().length === 0 ||
        candidate.implementedAt !== null ||
        candidate.implementationThreadId !== null ||
        candidate.createdAt < binding.providerAcceptedAt ||
        candidate.updatedAt < candidate.createdAt ||
        entry.event.occurredAt !== candidate.updatedAt
      ) {
        return yield* finalizerError(
          binding.handoffId,
          "invalid-plan",
          "corrupt-orchestration-history",
        );
      }
      if (proposedPlan === null) {
        proposedPlan = candidate;
        proposedPlanEvent = entry;
        proposedPlanJson = canonicalJson(candidate as JsonValue);
      } else {
        return yield* finalizerError(binding.handoffId, "multiple-plan-events", "ambiguous-plan");
      }
    }
    const chainEndSequence = Math.max(
      terminal?.event.sequence ?? started.event.sequence,
      proposedPlanEvent?.event.sequence ?? started.event.sequence,
    );
    for (const entry of history) {
      if (
        entry.event.sequence <= started.event.sequence ||
        entry.event.sequence >= chainEndSequence ||
        entry.event.type !== "thread.session-set"
      ) {
        continue;
      }
      const session = entry.event.payload.session;
      if (
        session.providerInstanceId !== binding.providerInstanceId ||
        session.providerName !== providerName ||
        session.runtimeMode !== binding.runtimeMode ||
        (session.activeTurnId !== null && session.activeTurnId !== binding.providerTurnId)
      ) {
        return yield* finalizerError(
          binding.handoffId,
          "provider-session-chain",
          "corrupt-orchestration-history",
        );
      }
    }

    const projectionRows = yield* sql<Record<string, unknown>>`
      SELECT plan_id AS "planId", thread_id AS "threadId", turn_id AS "turnId",
        plan_markdown AS "planMarkdown", implemented_at AS "implementedAt",
        implementation_thread_id AS "implementationThreadId", created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM projection_thread_proposed_plans
      WHERE thread_id = ${binding.threadId} AND turn_id = ${binding.providerTurnId}
      ORDER BY plan_id ASC
    `.pipe(
      Effect.mapError((cause) =>
        finalizerError(binding.handoffId, "read-plan-projection", "persistence", cause),
      ),
    );
    const projected = yield* Effect.forEach(projectionRows, (row) =>
      decodeProjectionPlan(row).pipe(
        Effect.mapError((cause) =>
          finalizerError(
            binding.handoffId,
            "decode-plan-projection",
            "corrupt-orchestration-history",
            cause,
          ),
        ),
      ),
    );
    if (
      (proposedPlan === null && projected.length !== 0) ||
      (proposedPlan !== null &&
        (projected.length !== 1 ||
          projected[0]?.planId !== proposedPlan.id ||
          projected[0]?.threadId !== binding.threadId ||
          projected[0]?.turnId !== proposedPlan.turnId ||
          projected[0]?.planMarkdown !== proposedPlan.planMarkdown ||
          projected[0]?.implementedAt !== proposedPlan.implementedAt ||
          projected[0]?.implementationThreadId !== proposedPlan.implementationThreadId ||
          projected[0]?.createdAt !== proposedPlan.createdAt ||
          projected[0]?.updatedAt !== proposedPlan.updatedAt))
    ) {
      return yield* finalizerError(
        binding.handoffId,
        "plan-projection-mismatch",
        "corrupt-orchestration-history",
      );
    }

    return {
      started,
      terminal,
      proposedPlan,
      proposedPlanJson,
      proposedPlanDigest: proposedPlanJson === null ? null : sha256Utf8(proposedPlanJson),
      proposedPlanEvent,
    } satisfies OrchestrationPlanningEvidence;
  });

  const readStartMarker = (handoffId: string) =>
    sql<Record<string, unknown>>`
      SELECT start_command_id AS "startCommandId", start_fingerprint AS "startFingerprint",
        handoff_id AS "handoffId", handoff_fingerprint AS "handoffFingerprint",
        project_id AS "projectId", task_id AS "taskId", task_revision AS "taskRevision",
        github_intake_sequence AS "githubIntakeSequence",
        source_identity_fingerprint AS "sourceIdentityFingerprint",
        controlled_thread_reservation_id AS "controlledThreadReservationId",
        thread_id AS "threadId", stage_run_id AS "stageRunId", attempt_id AS "attemptId",
        lease_id AS "leaseId", lease_holder_id AS "leaseHolderId", fence_token AS "fenceToken",
        provider_delivery_id AS "providerDeliveryId",
        provider_instance_id AS "providerInstanceId", provider_turn_id AS "providerTurnId",
        runtime_mode AS "runtimeMode", model_selection_fingerprint AS "modelSelectionFingerprint",
        provider_accepted_at AS "providerAcceptedAt", delivery_revision AS "deliveryRevision",
        orchestration_started_event_id AS "orchestrationStartedEventId",
        orchestration_started_sequence AS "orchestrationStartedSequence",
        orchestration_started_stream_version AS "orchestrationStartedStreamVersion",
        stage_event_id AS "stageEventId", stage_event_sequence AS "stageEventSequence",
        stage_event_stream_version AS "stageEventStreamVersion", recorded_at AS "recordedAt"
      FROM agent_control_initial_planning_stage_started WHERE handoff_id = ${handoffId}
    `.pipe(
      Effect.mapError((cause) =>
        finalizerError(handoffId, "read-start-marker", "persistence", cause),
      ),
      Effect.flatMap((rows) => {
        if (rows.length === 0) return Effect.succeed(Option.none<StartMarker>());
        if (rows.length !== 1)
          return Effect.fail(
            finalizerError(handoffId, "read-start-marker", "corrupt-stage-history"),
          );
        return decodeStartMarker(rows[0]).pipe(
          Effect.map(Option.some),
          Effect.mapError((cause) =>
            finalizerError(handoffId, "decode-start-marker", "corrupt-stage-history", cause),
          ),
        );
      }),
    );

  const readFinalizationReplay = (handoffId: string) =>
    sql<Record<string, unknown>>`
      SELECT evidence.result_evidence_id AS "resultEvidenceId",
        evidence.finalization_command_id AS "finalizationCommandId",
        evidence.finalization_fingerprint AS "finalizationFingerprint",
        evidence.outcome, evidence.handoff_id AS "handoffId",
        evidence.handoff_fingerprint AS "handoffFingerprint",
        evidence.project_id AS "projectId", evidence.task_id AS "taskId",
        evidence.task_revision AS "taskRevision",
        evidence.github_intake_sequence AS "githubIntakeSequence",
        evidence.source_identity_fingerprint AS "sourceIdentityFingerprint",
        evidence.controlled_thread_reservation_id AS "controlledThreadReservationId",
        evidence.thread_id AS "threadId", evidence.stage_run_id AS "stageRunId",
        evidence.attempt_id AS "attemptId", evidence.lease_id AS "leaseId",
        evidence.lease_holder_id AS "leaseHolderId", evidence.fence_token AS "fenceToken",
        evidence.provider_delivery_id AS "providerDeliveryId",
        evidence.provider_instance_id AS "providerInstanceId",
        evidence.provider_turn_id AS "providerTurnId", evidence.runtime_mode AS "runtimeMode",
        evidence.model_selection_fingerprint AS "modelSelectionFingerprint",
        evidence.delivery_terminal_state AS "deliveryTerminalState",
        evidence.delivery_revision AS "deliveryRevision", evidence.terminal_at AS "terminalAt",
        evidence.orchestration_started_event_id AS "orchestrationStartedEventId",
        evidence.orchestration_started_sequence AS "orchestrationStartedSequence",
        evidence.orchestration_terminal_event_id AS "orchestrationTerminalEventId",
        evidence.orchestration_terminal_sequence AS "orchestrationTerminalSequence",
        evidence.plan_id AS "planId", evidence.plan_event_id AS "planEventId",
        evidence.plan_event_sequence AS "planEventSequence",
        evidence.proposed_plan_json AS "proposedPlanJson",
        evidence.proposed_plan_digest AS "proposedPlanDigest",
        evidence.stage_event_id AS "stageEventId",
        evidence.stage_event_sequence AS "stageEventSequence",
        evidence.stage_event_stream_version AS "stageEventStreamVersion",
        evidence.lease_event_id AS "leaseEventId",
        evidence.lease_event_sequence AS "leaseEventSequence",
        evidence.lease_event_stream_version AS "leaseEventStreamVersion",
        evidence.finalized_at AS "finalizedAt",
        receipt.finalization_fingerprint AS "receiptFingerprint",
        receipt.result_evidence_id AS "receiptEvidenceId",
        receipt.handoff_id AS "receiptHandoffId", receipt.outcome AS "receiptOutcome",
        receipt.stage_event_id AS "receiptStageEventId",
        receipt.stage_event_sequence AS "receiptStageEventSequence",
        receipt.lease_event_id AS "receiptLeaseEventId",
        receipt.lease_event_sequence AS "receiptLeaseEventSequence",
        receipt.accepted_at AS "receiptAcceptedAt", marker.marker_id AS "markerId",
        marker.marker_fingerprint AS "markerFingerprint",
        marker.finalization_command_id AS "markerCommandId",
        marker.result_evidence_id AS "markerEvidenceId",
        marker.handoff_id AS "markerHandoffId", marker.committed_at AS "markerCommittedAt"
      FROM agent_control_initial_planning_result_evidence evidence
      JOIN agent_control_initial_planning_finalization_receipts receipt
        ON receipt.finalization_command_id = evidence.finalization_command_id
      JOIN agent_control_initial_planning_finalization_markers marker
        ON marker.finalization_command_id = evidence.finalization_command_id
      WHERE evidence.handoff_id = ${handoffId}
    `.pipe(
      Effect.mapError((cause) =>
        finalizerError(handoffId, "read-finalization-replay", "persistence", cause),
      ),
      Effect.flatMap((rows) => {
        if (rows.length === 0) return Effect.succeed(Option.none<FinalizationReplay>());
        if (rows.length !== 1)
          return Effect.fail(
            finalizerError(handoffId, "read-finalization-replay", "receipt-mismatch"),
          );
        return decodeFinalizationReplay(rows[0]).pipe(
          Effect.map(Option.some),
          Effect.mapError((cause) =>
            finalizerError(handoffId, "decode-finalization-replay", "receipt-mismatch", cause),
          ),
        );
      }),
    );

  const validateReplay = Effect.fn("AgentControlInitialPlanningFinalizer.validateReplay")(
    function* (row: FinalizationReplay) {
      const expectedCommandId = deriveInitialPlanningFinalizationCommandId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedEvidenceId = deriveInitialPlanningResultEvidenceId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedMarkerId = deriveInitialPlanningFinalizationMarkerId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedStageEventId = deriveInitialPlanningTerminalStageEventId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedLeaseEventId = deriveInitialPlanningLeaseReleaseEventId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedFingerprint = fingerprintInitialPlanningFinalization(
        "result",
        resultFingerprintParts(row),
      );
      const expectedMarkerFingerprint = fingerprintInitialPlanningFinalization("marker", [
        row.handoffId,
        row.handoffFingerprint,
        row.finalizationCommandId,
        row.resultEvidenceId,
        row.finalizationFingerprint,
        row.stageEventId,
        String(row.stageEventSequence),
        row.leaseEventId,
        String(row.leaseEventSequence),
        row.finalizedAt,
      ]);
      if (
        row.finalizationCommandId !== expectedCommandId ||
        row.resultEvidenceId !== expectedEvidenceId ||
        row.markerId !== expectedMarkerId ||
        row.stageEventId !== expectedStageEventId ||
        row.leaseEventId !== expectedLeaseEventId ||
        row.finalizationFingerprint !== expectedFingerprint ||
        row.markerFingerprint !== expectedMarkerFingerprint ||
        row.receiptFingerprint !== row.finalizationFingerprint ||
        row.receiptEvidenceId !== row.resultEvidenceId ||
        row.receiptHandoffId !== row.handoffId ||
        row.receiptOutcome !== row.outcome ||
        row.receiptStageEventId !== row.stageEventId ||
        row.receiptStageEventSequence !== row.stageEventSequence ||
        row.receiptLeaseEventId !== row.leaseEventId ||
        row.receiptLeaseEventSequence !== row.leaseEventSequence ||
        row.receiptAcceptedAt !== row.finalizedAt ||
        row.markerCommandId !== row.finalizationCommandId ||
        row.markerEvidenceId !== row.resultEvidenceId ||
        row.markerHandoffId !== row.handoffId ||
        row.markerCommittedAt !== row.finalizedAt
      ) {
        return yield* finalizerError(row.handoffId, "validate-replay-identity", "receipt-mismatch");
      }
      if (row.proposedPlanJson !== null) {
        yield* Effect.try({
          try: () => {
            parseCanonicalJson(row.proposedPlanJson!);
            if (sha256Utf8(row.proposedPlanJson!) !== row.proposedPlanDigest)
              throw new Error("plan digest mismatch");
          },
          catch: (cause) =>
            finalizerError(row.handoffId, "validate-replay-plan", "receipt-mismatch", cause),
        });
      }
      const stage = yield* loadAuthoritativeStageRunState(
        AgentControlStageRunId.make(row.stageRunId),
        stageEvents,
        stageStates,
      ).pipe(
        Effect.mapError((cause) =>
          finalizerError(row.handoffId, "replay-stage-history", "corrupt-stage-history", cause),
        ),
      );
      const lease = yield* loadAuthoritativeLeaseState(
        AgentControlStageRunLeaseId.make(row.leaseId),
        leaseEvents,
        leaseStates,
      ).pipe(
        Effect.mapError((cause) =>
          finalizerError(row.handoffId, "replay-lease-history", "corrupt-lease-history", cause),
        ),
      );
      if (Option.isNone(stage) || Option.isNone(lease)) {
        return yield* finalizerError(row.handoffId, "replay-history-missing", "receipt-mismatch");
      }
      const terminalStageEvent = stage.value.events[row.stageEventStreamVersion - 1];
      const releaseEvent = lease.value.events[row.leaseEventStreamVersion - 1];
      const releasedLeaseState = lease.value.statesByVersion[row.leaseEventStreamVersion - 1];
      const expectedTerminalType =
        row.outcome === "succeeded"
          ? "agentControl.stageRun.planningSucceeded"
          : row.outcome === "failed"
            ? "agentControl.stageRun.planningFailed"
            : "agentControl.stageRun.planningCancelled";
      if (
        stage.value.state.status !== row.outcome ||
        stage.value.state.revision !== row.stageEventStreamVersion ||
        stage.value.state.projectId !== row.projectId ||
        stage.value.state.taskId !== row.taskId ||
        stage.value.state.stageRunId !== row.stageRunId ||
        stage.value.state.attemptId !== row.attemptId ||
        stage.value.state.taskRevision !== row.taskRevision ||
        stage.value.state.githubIntakeSequence !== row.githubIntakeSequence ||
        stage.value.state.sourceIdentityFingerprint !== row.sourceIdentityFingerprint ||
        terminalStageEvent?.type !== expectedTerminalType ||
        terminalStageEvent?.eventId !== row.stageEventId ||
        terminalStageEvent.sequence !== row.stageEventSequence ||
        terminalStageEvent.payload.handoffId !== row.handoffId ||
        terminalStageEvent.payload.handoffFingerprint !== row.handoffFingerprint ||
        terminalStageEvent.payload.controlledThreadReservationId !==
          row.controlledThreadReservationId ||
        terminalStageEvent.payload.threadId !== row.threadId ||
        terminalStageEvent.payload.providerDeliveryId !== row.providerDeliveryId ||
        terminalStageEvent.payload.providerInstanceId !== row.providerInstanceId ||
        terminalStageEvent.payload.providerTurnId !== row.providerTurnId ||
        terminalStageEvent.payload.runtimeMode !== row.runtimeMode ||
        terminalStageEvent.payload.modelSelectionFingerprint !== row.modelSelectionFingerprint ||
        terminalStageEvent.payload.leaseId !== row.leaseId ||
        terminalStageEvent.payload.leaseHolderId !== row.leaseHolderId ||
        terminalStageEvent.payload.fenceToken !== row.fenceToken ||
        terminalStageEvent.payload.resultEvidenceId !== row.resultEvidenceId ||
        terminalStageEvent.payload.finalizedAt !== row.finalizedAt ||
        releasedLeaseState === undefined ||
        releasedLeaseState.projectId !== row.projectId ||
        releasedLeaseState.taskId !== row.taskId ||
        releasedLeaseState.stageRunId !== row.stageRunId ||
        releasedLeaseState.attemptId !== row.attemptId ||
        releasedLeaseState.holderId !== row.leaseHolderId ||
        releasedLeaseState.fenceToken !== row.fenceToken ||
        releasedLeaseState.status !== "released" ||
        releasedLeaseState.revision !== row.leaseEventStreamVersion ||
        releaseEvent?.eventId !== row.leaseEventId ||
        releaseEvent.sequence !== row.leaseEventSequence ||
        releaseEvent.type !== "agentControl.stageRunLease.releasedAfterPlanning" ||
        releaseEvent.payload.handoffId !== row.handoffId ||
        releaseEvent.payload.handoffFingerprint !== row.handoffFingerprint ||
        releaseEvent.payload.controlledThreadReservationId !== row.controlledThreadReservationId ||
        releaseEvent.payload.threadId !== row.threadId ||
        releaseEvent.payload.providerDeliveryId !== row.providerDeliveryId ||
        releaseEvent.payload.providerInstanceId !== row.providerInstanceId ||
        releaseEvent.payload.providerTurnId !== row.providerTurnId ||
        releaseEvent.payload.runtimeMode !== row.runtimeMode ||
        releaseEvent.payload.modelSelectionFingerprint !== row.modelSelectionFingerprint ||
        releaseEvent.payload.resultEvidenceId !== row.resultEvidenceId ||
        releaseEvent.payload.stageStatus !== row.outcome ||
        releaseEvent.payload.releasedAt !== row.finalizedAt
      ) {
        return yield* finalizerError(
          row.handoffId,
          "replay-history-coordinates",
          "receipt-mismatch",
        );
      }
      const startMarker = yield* readStartMarker(row.handoffId);
      const startEvent = stage.value.events[1];
      if (
        Option.isNone(startMarker) ||
        startEvent?.type !== "agentControl.stageRun.planningStarted"
      ) {
        return yield* finalizerError(
          row.handoffId,
          "replay-start-evidence-missing",
          "receipt-mismatch",
        );
      }
      const marker = startMarker.value;
      const orchestration = yield* reconstructOrchestrationEvidence({
        handoffId: row.handoffId,
        handoffFingerprint: row.handoffFingerprint,
        threadId: row.threadId,
        providerInstanceId: row.providerInstanceId,
        providerTurnId: row.providerTurnId,
        runtimeMode: row.runtimeMode,
        providerAcceptedAt: marker.providerAcceptedAt,
        terminalAt: row.terminalAt,
        deliveryTerminalState: row.deliveryTerminalState,
      });
      if (orchestration === null) {
        return yield* finalizerError(
          row.handoffId,
          "replay-orchestration-missing",
          "receipt-mismatch",
        );
      }
      const expectedStartCommandId = deriveInitialPlanningStageStartCommandId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedStartEventId = deriveInitialPlanningStageStartedEventId(
        row.handoffId,
        row.handoffFingerprint,
      );
      const expectedStartFingerprint = fingerprintInitialPlanningFinalization(
        "start",
        startFingerprintParts(
          bindingParts(row, row.providerTurnId, row.modelSelectionFingerprint),
          {
            providerAcceptedAt: marker.providerAcceptedAt,
            deliveryRevision: marker.deliveryRevision,
            started: orchestration.started,
            stageEvent: startEvent,
          },
        ),
      );
      if (
        marker.startCommandId !== expectedStartCommandId ||
        marker.startFingerprint !== expectedStartFingerprint ||
        marker.handoffId !== row.handoffId ||
        marker.handoffFingerprint !== row.handoffFingerprint ||
        marker.projectId !== row.projectId ||
        marker.taskId !== row.taskId ||
        marker.taskRevision !== row.taskRevision ||
        marker.githubIntakeSequence !== row.githubIntakeSequence ||
        marker.sourceIdentityFingerprint !== row.sourceIdentityFingerprint ||
        marker.controlledThreadReservationId !== row.controlledThreadReservationId ||
        marker.threadId !== row.threadId ||
        marker.stageRunId !== row.stageRunId ||
        marker.attemptId !== row.attemptId ||
        marker.leaseId !== row.leaseId ||
        marker.leaseHolderId !== row.leaseHolderId ||
        marker.fenceToken !== row.fenceToken ||
        marker.providerDeliveryId !== row.providerDeliveryId ||
        marker.providerInstanceId !== row.providerInstanceId ||
        marker.providerTurnId !== row.providerTurnId ||
        marker.runtimeMode !== row.runtimeMode ||
        marker.modelSelectionFingerprint !== row.modelSelectionFingerprint ||
        marker.providerAcceptedAt !== startEvent.occurredAt ||
        marker.orchestrationStartedEventId !== row.orchestrationStartedEventId ||
        marker.orchestrationStartedSequence !== row.orchestrationStartedSequence ||
        marker.orchestrationStartedStreamVersion !== orchestration.started.streamVersion ||
        marker.stageEventId !== expectedStartEventId ||
        marker.stageEventId !== startEvent.eventId ||
        marker.stageEventSequence !== startEvent.sequence ||
        marker.stageEventStreamVersion !== startEvent.streamVersion ||
        startEvent.payload.handoffId !== row.handoffId ||
        startEvent.payload.handoffFingerprint !== row.handoffFingerprint ||
        startEvent.payload.controlledThreadReservationId !== row.controlledThreadReservationId ||
        startEvent.payload.threadId !== row.threadId ||
        startEvent.payload.providerDeliveryId !== row.providerDeliveryId ||
        startEvent.payload.providerInstanceId !== row.providerInstanceId ||
        startEvent.payload.providerTurnId !== row.providerTurnId ||
        startEvent.payload.runtimeMode !== row.runtimeMode ||
        startEvent.payload.modelSelectionFingerprint !== row.modelSelectionFingerprint ||
        startEvent.payload.leaseId !== row.leaseId ||
        startEvent.payload.leaseHolderId !== row.leaseHolderId ||
        startEvent.payload.fenceToken !== row.fenceToken ||
        orchestration.started.event.eventId !== row.orchestrationStartedEventId ||
        orchestration.started.event.sequence !== row.orchestrationStartedSequence ||
        orchestration.terminal?.event.eventId !== row.orchestrationTerminalEventId ||
        orchestration.terminal.event.sequence !== row.orchestrationTerminalSequence ||
        (row.outcome === "succeeded" &&
          (orchestration.proposedPlan?.id !== row.planId ||
            orchestration.proposedPlanEvent?.event.eventId !== row.planEventId ||
            orchestration.proposedPlanEvent?.event.sequence !== row.planEventSequence ||
            orchestration.proposedPlanJson !== row.proposedPlanJson ||
            orchestration.proposedPlanDigest !== row.proposedPlanDigest))
      ) {
        return yield* finalizerError(
          row.handoffId,
          "replay-orchestration-coordinates",
          "receipt-mismatch",
        );
      }
      return row;
    },
  );

  const replayFirst = Effect.fn("AgentControlInitialPlanningFinalizer.replayFirst")(function* (
    handoffId: string,
  ) {
    const replay = yield* readFinalizationReplay(handoffId);
    if (Option.isNone(replay)) return Option.none<FinalizationReplay>();
    return Option.some(yield* validateReplay(replay.value));
  });

  const processNew = Effect.fn("AgentControlInitialPlanningFinalizer.processNew")(function* (
    claim: AgentControlInitialPlanningClaim,
  ) {
    const evidence = claim.evidence;
    const delivery = claim.delivery;
    const providerTurnId = delivery.providerTurnId;
    const providerAcceptedAt = delivery.providerAcceptedAt;
    if (providerTurnId === null || providerAcceptedAt === null) {
      return { _tag: "Waiting" as const } satisfies AgentControlInitialPlanningFinalizerResult;
    }
    const modelEvidence = canonicalProviderModelSelectionEvidence(evidence.modelSelection);
    if (modelEvidence.modelSelectionJson !== evidence.modelSelectionJson) {
      return yield* finalizerError(evidence.handoffId, "model-selection", "corrupt-handoff");
    }
    const expectedHandoffId = yield* deriveAgentControlInitialPlanningHandoffId(
      evidence.controlledThreadReservationId,
      evidence.threadId,
    );
    const expectedStageRunId = yield* deriveAgentControlStageRunId({
      projectId: evidence.projectId,
      taskId: AgentControlTaskId.make(evidence.taskId),
      taskRevision: evidence.taskRevision,
      githubIntakeSequence: evidence.githubIntakeSequence,
      sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
      stageKind: "planning",
      stageOrdinal: 1,
    });
    const expectedAttemptId = yield* deriveAgentControlAttemptId(expectedStageRunId, 1);
    const expectedLeaseId = yield* deriveAgentControlStageRunLeaseId({
      projectId: evidence.projectId,
      taskId: AgentControlTaskId.make(evidence.taskId),
    });
    const expectedProviderDeliveryId = yield* deriveAgentControlInitialPlanningProviderDeliveryId(
      evidence.handoffId,
    );
    if (
      evidence.handoffId !== expectedHandoffId ||
      evidence.stageRunId !== expectedStageRunId ||
      evidence.attemptId !== expectedAttemptId ||
      evidence.leaseId !== expectedLeaseId ||
      evidence.providerDeliveryId !== expectedProviderDeliveryId
    ) {
      return yield* finalizerError(
        evidence.handoffId,
        "derived-planning-identity",
        "identity-mismatch",
      );
    }
    const outcome =
      delivery.state === "completed"
        ? "succeeded"
        : delivery.state === "failed"
          ? "failed"
          : delivery.state === "interrupted"
            ? "cancelled"
            : null;
    const transaction = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const replay = yield* replayFirst(evidence.handoffId);
          if (Option.isSome(replay)) {
            return { _tag: "Replayed" as const, resultEvidenceId: replay.value.resultEvidenceId };
          }
          const orchestration = yield* reconstructOrchestrationEvidence({
            handoffId: evidence.handoffId,
            handoffFingerprint: evidence.handoffFingerprint,
            threadId: evidence.threadId,
            providerInstanceId: evidence.providerInstanceId,
            providerTurnId,
            runtimeMode: evidence.runtimeMode,
            providerAcceptedAt,
            terminalAt: delivery.terminalAt,
            deliveryTerminalState:
              delivery.state === "completed" ||
              delivery.state === "failed" ||
              delivery.state === "interrupted"
                ? delivery.state
                : null,
          });
          if (orchestration === null) {
            return yield* finalizerError(
              evidence.handoffId,
              "transactional-orchestration-history",
              "corrupt-orchestration-history",
            );
          }
          const stage = yield* loadAuthoritativeStageRunState(
            AgentControlStageRunId.make(evidence.stageRunId),
            stageEvents,
            stageStates,
          ).pipe(
            Effect.mapError((cause) =>
              finalizerError(evidence.handoffId, "stage-history", "corrupt-stage-history", cause),
            ),
          );
          const lease = yield* loadAuthoritativeLeaseState(
            AgentControlStageRunLeaseId.make(evidence.leaseId),
            leaseEvents,
            leaseStates,
          ).pipe(
            Effect.mapError((cause) =>
              finalizerError(evidence.handoffId, "lease-history", "corrupt-lease-history", cause),
            ),
          );
          if (Option.isNone(stage) || Option.isNone(lease)) {
            return yield* finalizerError(
              evidence.handoffId,
              "missing-stage-or-lease",
              "identity-mismatch",
            );
          }
          const stageState = stage.value.state;
          const leaseState = lease.value.state;
          if (
            stageState.projectId !== evidence.projectId ||
            stageState.taskId !== evidence.taskId ||
            stageState.stageRunId !== evidence.stageRunId ||
            stageState.attemptId !== evidence.attemptId ||
            stageState.taskRevision !== evidence.taskRevision ||
            stageState.githubIntakeSequence !== evidence.githubIntakeSequence ||
            stageState.sourceIdentityFingerprint !== evidence.sourceIdentityFingerprint ||
            leaseState.projectId !== evidence.projectId ||
            leaseState.taskId !== evidence.taskId ||
            leaseState.stageRunId !== evidence.stageRunId ||
            leaseState.attemptId !== evidence.attemptId ||
            leaseState.leaseId !== evidence.leaseId ||
            leaseState.holderId !== evidence.leaseHolderId ||
            leaseState.fenceToken !== evidence.fenceToken ||
            leaseState.taskRevision !== evidence.taskRevision ||
            leaseState.githubIntakeSequence !== evidence.githubIntakeSequence ||
            leaseState.sourceIdentityFingerprint !== evidence.sourceIdentityFingerprint
          ) {
            return yield* finalizerError(
              evidence.handoffId,
              "stage-lease-binding",
              "identity-mismatch",
            );
          }
          const observation = {
            handoffId: evidence.handoffId,
            stageRunId: evidence.stageRunId,
            leaseId: evidence.leaseId,
            deliveryRevision: delivery.revision,
            stageRevision: stageState.revision,
            leaseRevision: leaseState.revision,
          };
          yield* hooks.afterAuthoritativeRead(observation);

          const identity = {
            projectId: evidence.projectId,
            taskId: AgentControlTaskId.make(evidence.taskId),
            stageRunId: AgentControlStageRunId.make(evidence.stageRunId),
            attemptId: AgentControlAttemptId.make(evidence.attemptId),
            roleId: "planning" as const,
            stageKind: "planning" as const,
            stageOrdinal: 1 as const,
            attemptOrdinal: 1 as const,
            taskRevision: evidence.taskRevision,
            githubIntakeSequence: evidence.githubIntakeSequence,
            sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
            handoffId: evidence.handoffId,
            handoffFingerprint: evidence.handoffFingerprint,
            controlledThreadReservationId: evidence.controlledThreadReservationId,
            threadId: evidence.threadId,
            providerDeliveryId: evidence.providerDeliveryId,
            providerInstanceId: evidence.providerInstanceId,
            providerTurnId,
            runtimeMode: evidence.runtimeMode,
            modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
            leaseId: AgentControlStageRunLeaseId.make(evidence.leaseId),
            leaseHolderId: AgentControlStageRunLeaseHolderId.make(evidence.leaseHolderId),
            fenceToken: evidence.fenceToken,
          };
          const frozenParts = bindingParts(
            evidence,
            providerTurnId,
            modelEvidence.modelSelectionFingerprint,
          );
          const startCommandId = deriveInitialPlanningStageStartCommandId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const startEventId = deriveInitialPlanningStageStartedEventId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          let currentStage = stageState;
          const committedStageEvents: Array<AgentControlStageRunEvent> = [];
          let startEvent = stage.value.events[1];
          const existingStartMarker = yield* readStartMarker(evidence.handoffId);
          if (currentStage.status === "prepared") {
            if (Option.isSome(existingStartMarker) || startEvent !== undefined) {
              return yield* finalizerError(
                evidence.handoffId,
                "partial-start-marker",
                "corrupt-stage-history",
              );
            }
            const draft: AgentControlStageRunEventDraft = {
              eventId: startEventId,
              type: "agentControl.stageRun.planningStarted",
              aggregateKind: "stage-run",
              aggregateId: identity.stageRunId,
              occurredAt: providerAcceptedAt,
              commandId: startCommandId,
              causationEventId: null,
              correlationId: startCommandId,
              authority: "system",
              payload: { ...identity, status: "running", startedAt: providerAcceptedAt },
              metadata: { schemaVersion: 1 },
            };
            const appended = yield* stageEvents.append({
              stageRunId: identity.stageRunId,
              expectedStreamVersion: currentStage.revision,
              events: [draft],
            });
            startEvent = appended[0];
            if (startEvent === undefined) {
              return yield* finalizerError(
                evidence.handoffId,
                "missing-start-event",
                "corrupt-stage-history",
              );
            }
            yield* stageProjection.projectEvent(startEvent);
            currentStage = yield* projectAgentControlStageRunEvent(currentStage, startEvent).pipe(
              Effect.mapError((cause) =>
                finalizerError(
                  evidence.handoffId,
                  "project-start-event",
                  "corrupt-stage-history",
                  cause,
                ),
              ),
            );
            committedStageEvents.push(startEvent);
            const startFingerprint = fingerprintInitialPlanningFinalization(
              "start",
              startFingerprintParts(frozenParts, {
                providerAcceptedAt,
                deliveryRevision: delivery.revision,
                started: orchestration.started,
                stageEvent: startEvent,
              }),
            );
            yield* sql`
            INSERT INTO agent_control_initial_planning_stage_started (
              start_command_id, start_fingerprint, handoff_id, handoff_fingerprint,
              project_id, task_id, task_revision, github_intake_sequence,
              source_identity_fingerprint, controlled_thread_reservation_id, thread_id,
              stage_run_id, attempt_id, lease_id, lease_holder_id, fence_token,
              provider_delivery_id, provider_instance_id, provider_turn_id, runtime_mode,
              model_selection_fingerprint, provider_accepted_at, delivery_revision,
              orchestration_started_event_id, orchestration_started_sequence,
              orchestration_started_stream_version, stage_event_id, stage_event_sequence,
              stage_event_stream_version, recorded_at
            ) VALUES (
              ${startCommandId}, ${startFingerprint}, ${evidence.handoffId},
              ${evidence.handoffFingerprint}, ${evidence.projectId}, ${evidence.taskId},
              ${evidence.taskRevision}, ${evidence.githubIntakeSequence},
              ${evidence.sourceIdentityFingerprint}, ${evidence.controlledThreadReservationId},
              ${evidence.threadId}, ${evidence.stageRunId}, ${evidence.attemptId},
              ${evidence.leaseId}, ${evidence.leaseHolderId}, ${evidence.fenceToken},
              ${evidence.providerDeliveryId}, ${evidence.providerInstanceId}, ${providerTurnId},
              ${evidence.runtimeMode}, ${modelEvidence.modelSelectionFingerprint},
              ${providerAcceptedAt}, ${delivery.revision}, ${orchestration.started.event.eventId},
              ${orchestration.started.event.sequence}, ${orchestration.started.streamVersion},
              ${startEvent.eventId}, ${startEvent.sequence}, ${startEvent.streamVersion},
              ${providerAcceptedAt}
            )
          `;
          } else if (currentStage.status === "running") {
            if (
              Option.isNone(existingStartMarker) ||
              startEvent?.type !== "agentControl.stageRun.planningStarted"
            ) {
              return yield* finalizerError(
                evidence.handoffId,
                "missing-start-marker",
                "corrupt-stage-history",
              );
            }
            const marker = existingStartMarker.value;
            const expectedStartFingerprint = fingerprintInitialPlanningFinalization(
              "start",
              startFingerprintParts(frozenParts, {
                providerAcceptedAt: marker.providerAcceptedAt,
                deliveryRevision: marker.deliveryRevision,
                started: orchestration.started,
                stageEvent: startEvent,
              }),
            );
            if (
              marker.startCommandId !== startCommandId ||
              marker.startFingerprint !== expectedStartFingerprint ||
              marker.handoffId !== evidence.handoffId ||
              marker.handoffFingerprint !== evidence.handoffFingerprint ||
              marker.projectId !== evidence.projectId ||
              marker.taskId !== evidence.taskId ||
              marker.taskRevision !== evidence.taskRevision ||
              marker.githubIntakeSequence !== evidence.githubIntakeSequence ||
              marker.sourceIdentityFingerprint !== evidence.sourceIdentityFingerprint ||
              marker.controlledThreadReservationId !== evidence.controlledThreadReservationId ||
              marker.threadId !== evidence.threadId ||
              marker.stageRunId !== evidence.stageRunId ||
              marker.attemptId !== evidence.attemptId ||
              marker.leaseId !== evidence.leaseId ||
              marker.leaseHolderId !== evidence.leaseHolderId ||
              marker.fenceToken !== evidence.fenceToken ||
              marker.providerDeliveryId !== evidence.providerDeliveryId ||
              marker.providerInstanceId !== evidence.providerInstanceId ||
              marker.providerTurnId !== providerTurnId ||
              marker.runtimeMode !== evidence.runtimeMode ||
              marker.modelSelectionFingerprint !== modelEvidence.modelSelectionFingerprint ||
              marker.providerAcceptedAt !== providerAcceptedAt ||
              marker.stageEventId !== startEvent.eventId ||
              marker.stageEventSequence !== startEvent.sequence ||
              marker.stageEventStreamVersion !== startEvent.streamVersion ||
              marker.orchestrationStartedEventId !== orchestration.started.event.eventId ||
              marker.orchestrationStartedSequence !== orchestration.started.event.sequence ||
              marker.orchestrationStartedStreamVersion !== orchestration.started.streamVersion ||
              marker.recordedAt !== providerAcceptedAt ||
              startEvent.eventId !== startEventId ||
              startEvent.payload.handoffId !== evidence.handoffId ||
              startEvent.payload.handoffFingerprint !== evidence.handoffFingerprint ||
              startEvent.payload.controlledThreadReservationId !==
                evidence.controlledThreadReservationId ||
              startEvent.payload.threadId !== evidence.threadId ||
              startEvent.payload.providerDeliveryId !== evidence.providerDeliveryId ||
              startEvent.payload.providerInstanceId !== evidence.providerInstanceId ||
              startEvent.payload.providerTurnId !== providerTurnId ||
              startEvent.payload.runtimeMode !== evidence.runtimeMode ||
              startEvent.payload.modelSelectionFingerprint !==
                modelEvidence.modelSelectionFingerprint ||
              startEvent.payload.leaseId !== evidence.leaseId ||
              startEvent.payload.leaseHolderId !== evidence.leaseHolderId ||
              startEvent.payload.fenceToken !== evidence.fenceToken
            ) {
              return yield* finalizerError(
                evidence.handoffId,
                "start-marker-mismatch",
                "corrupt-stage-history",
              );
            }
          } else {
            return yield* finalizerError(
              evidence.handoffId,
              "unexpected-stage-status",
              "corrupt-stage-history",
            );
          }

          if (
            outcome === null ||
            orchestration.terminal === null ||
            (outcome === "succeeded" && orchestration.proposedPlan === null)
          ) {
            yield* hooks.beforeTransactionComplete({
              ...observation,
              stageRevision: currentStage.revision,
            });
            return {
              _tag: "Started" as const,
              event: startEvent!,
              observation,
              publish: committedStageEvents.length === 1,
            };
          }
          if (leaseState.status !== "reserved") {
            return yield* finalizerError(
              evidence.handoffId,
              "lease-not-reserved",
              "corrupt-lease-history",
            );
          }
          const terminal = orchestration.terminal!;
          const finalizationCommandId = deriveInitialPlanningFinalizationCommandId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const resultEvidenceId = deriveInitialPlanningResultEvidenceId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const terminalStageEventId = deriveInitialPlanningTerminalStageEventId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const terminalType =
            outcome === "succeeded"
              ? "agentControl.stageRun.planningSucceeded"
              : outcome === "failed"
                ? "agentControl.stageRun.planningFailed"
                : "agentControl.stageRun.planningCancelled";
          const terminalDraft = {
            eventId: terminalStageEventId,
            type: terminalType,
            aggregateKind: "stage-run",
            aggregateId: identity.stageRunId,
            occurredAt: delivery.terminalAt!,
            commandId: finalizationCommandId,
            causationEventId: null,
            correlationId: finalizationCommandId,
            authority: "system",
            payload: {
              ...identity,
              status: outcome,
              resultEvidenceId,
              finalizedAt: delivery.terminalAt!,
            },
            metadata: { schemaVersion: 1 },
          } as AgentControlStageRunEventDraft;
          const appendedStage = yield* stageEvents.append({
            stageRunId: identity.stageRunId,
            expectedStreamVersion: currentStage.revision,
            events: [terminalDraft],
          });
          const terminalStageEvent = appendedStage[0];
          if (terminalStageEvent === undefined) {
            return yield* finalizerError(
              evidence.handoffId,
              "missing-terminal-stage-event",
              "corrupt-stage-history",
            );
          }
          yield* stageProjection.projectEvent(terminalStageEvent);
          currentStage = yield* projectAgentControlStageRunEvent(
            currentStage,
            terminalStageEvent,
          ).pipe(
            Effect.mapError((cause) =>
              finalizerError(
                evidence.handoffId,
                "project-terminal-stage-event",
                "corrupt-stage-history",
                cause,
              ),
            ),
          );
          committedStageEvents.push(terminalStageEvent);

          const leaseEventId = deriveInitialPlanningLeaseReleaseEventId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const leaseDraft: AgentControlStageRunLeaseEventDraft = {
            eventId: leaseEventId,
            type: "agentControl.stageRunLease.releasedAfterPlanning",
            aggregateKind: "stage-run-lease",
            aggregateId: identity.leaseId,
            occurredAt: delivery.terminalAt!,
            commandId: finalizationCommandId,
            causationEventId: null,
            correlationId: finalizationCommandId,
            authority: "system",
            payload: {
              leaseId: identity.leaseId,
              projectId: evidence.projectId,
              taskId: AgentControlTaskId.make(evidence.taskId),
              stageRunId: identity.stageRunId,
              attemptId: identity.attemptId,
              taskRevision: evidence.taskRevision,
              githubIntakeSequence: evidence.githubIntakeSequence,
              sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
              holderId: identity.leaseHolderId,
              fenceToken: evidence.fenceToken,
              handoffId: evidence.handoffId,
              handoffFingerprint: evidence.handoffFingerprint,
              controlledThreadReservationId: evidence.controlledThreadReservationId,
              threadId: evidence.threadId,
              providerDeliveryId: evidence.providerDeliveryId,
              providerInstanceId: evidence.providerInstanceId,
              providerTurnId,
              runtimeMode: evidence.runtimeMode,
              modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
              resultEvidenceId,
              stageStatus: outcome,
              releasedAt: delivery.terminalAt!,
            },
            metadata: { schemaVersion: 1 },
          };
          const appendedLease = yield* leaseEvents.append({
            leaseId: identity.leaseId,
            expectedStreamVersion: leaseState.revision,
            events: [leaseDraft],
          });
          const releaseEvent = appendedLease[0];
          if (releaseEvent === undefined) {
            return yield* finalizerError(
              evidence.handoffId,
              "missing-lease-event",
              "corrupt-lease-history",
            );
          }
          yield* leaseProjection.projectEvent(releaseEvent);
          yield* projectAgentControlStageRunLeaseEvent(leaseState, releaseEvent).pipe(
            Effect.mapError((cause) =>
              finalizerError(
                evidence.handoffId,
                "project-lease-event",
                "corrupt-lease-history",
                cause,
              ),
            ),
          );

          const resultRow = {
            handoffId: evidence.handoffId,
            handoffFingerprint: evidence.handoffFingerprint,
            projectId: evidence.projectId,
            taskId: evidence.taskId,
            taskRevision: evidence.taskRevision,
            githubIntakeSequence: evidence.githubIntakeSequence,
            sourceIdentityFingerprint: evidence.sourceIdentityFingerprint,
            controlledThreadReservationId: evidence.controlledThreadReservationId,
            threadId: evidence.threadId,
            stageRunId: evidence.stageRunId,
            attemptId: evidence.attemptId,
            leaseId: evidence.leaseId,
            leaseHolderId: evidence.leaseHolderId,
            fenceToken: evidence.fenceToken,
            providerDeliveryId: evidence.providerDeliveryId,
            providerInstanceId: evidence.providerInstanceId,
            providerTurnId,
            runtimeMode: evidence.runtimeMode,
            modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
            outcome,
            deliveryTerminalState: delivery.state as "completed" | "failed" | "interrupted",
            deliveryRevision: delivery.revision,
            terminalAt: delivery.terminalAt!,
            orchestrationStartedEventId: orchestration.started.event.eventId,
            orchestrationStartedSequence: orchestration.started.event.sequence,
            orchestrationTerminalEventId: terminal.event.eventId,
            orchestrationTerminalSequence: terminal.event.sequence,
            planId: outcome === "succeeded" ? orchestration.proposedPlan!.id : null,
            planEventId:
              outcome === "succeeded" ? orchestration.proposedPlanEvent!.event.eventId : null,
            planEventSequence:
              outcome === "succeeded" ? orchestration.proposedPlanEvent!.event.sequence : null,
            proposedPlanJson: outcome === "succeeded" ? orchestration.proposedPlanJson : null,
            proposedPlanDigest: outcome === "succeeded" ? orchestration.proposedPlanDigest : null,
            stageEventId: terminalStageEvent.eventId,
            stageEventSequence: terminalStageEvent.sequence,
            stageEventStreamVersion: terminalStageEvent.streamVersion,
            leaseEventId: releaseEvent.eventId,
            leaseEventSequence: releaseEvent.sequence,
            leaseEventStreamVersion: releaseEvent.streamVersion,
            finalizedAt: delivery.terminalAt!,
          };
          const resultFingerprint = fingerprintInitialPlanningFinalization(
            "result",
            resultFingerprintParts(resultRow),
          );
          yield* sql`
          INSERT INTO agent_control_initial_planning_result_evidence (
            result_evidence_id, finalization_command_id, finalization_fingerprint, outcome,
            handoff_id, handoff_fingerprint, project_id, task_id, task_revision,
            github_intake_sequence, source_identity_fingerprint,
            controlled_thread_reservation_id, thread_id, stage_run_id, attempt_id,
            lease_id, lease_holder_id, fence_token, provider_delivery_id,
            provider_instance_id, provider_turn_id, runtime_mode, model_selection_fingerprint,
            delivery_terminal_state, delivery_revision, terminal_at,
            orchestration_started_event_id, orchestration_started_sequence,
            orchestration_terminal_event_id, orchestration_terminal_sequence,
            plan_id, plan_event_id, plan_event_sequence, proposed_plan_json,
            proposed_plan_digest, stage_event_id, stage_event_sequence,
            stage_event_stream_version, lease_event_id, lease_event_sequence,
            lease_event_stream_version, finalized_at
          ) VALUES (
            ${resultEvidenceId}, ${finalizationCommandId}, ${resultFingerprint}, ${outcome},
            ${evidence.handoffId}, ${evidence.handoffFingerprint}, ${evidence.projectId},
            ${evidence.taskId}, ${evidence.taskRevision}, ${evidence.githubIntakeSequence},
            ${evidence.sourceIdentityFingerprint}, ${evidence.controlledThreadReservationId},
            ${evidence.threadId}, ${evidence.stageRunId}, ${evidence.attemptId},
            ${evidence.leaseId}, ${evidence.leaseHolderId}, ${evidence.fenceToken},
            ${evidence.providerDeliveryId}, ${evidence.providerInstanceId}, ${providerTurnId},
            ${evidence.runtimeMode}, ${modelEvidence.modelSelectionFingerprint}, ${delivery.state},
            ${delivery.revision}, ${delivery.terminalAt}, ${orchestration.started.event.eventId},
            ${orchestration.started.event.sequence}, ${terminal.event.eventId},
            ${terminal.event.sequence}, ${resultRow.planId}, ${resultRow.planEventId},
            ${resultRow.planEventSequence}, ${resultRow.proposedPlanJson},
            ${resultRow.proposedPlanDigest}, ${terminalStageEvent.eventId},
            ${terminalStageEvent.sequence}, ${terminalStageEvent.streamVersion},
            ${releaseEvent.eventId}, ${releaseEvent.sequence}, ${releaseEvent.streamVersion},
            ${delivery.terminalAt}
          )
        `;
          yield* sql`
          INSERT INTO agent_control_initial_planning_finalization_receipts (
            finalization_command_id, finalization_fingerprint, result_evidence_id,
            handoff_id, outcome, stage_event_id, stage_event_sequence,
            lease_event_id, lease_event_sequence, accepted_at
          ) VALUES (
            ${finalizationCommandId}, ${resultFingerprint}, ${resultEvidenceId},
            ${evidence.handoffId}, ${outcome}, ${terminalStageEvent.eventId},
            ${terminalStageEvent.sequence}, ${releaseEvent.eventId}, ${releaseEvent.sequence},
            ${delivery.terminalAt}
          )
        `;
          const markerId = deriveInitialPlanningFinalizationMarkerId(
            evidence.handoffId,
            evidence.handoffFingerprint,
          );
          const markerFingerprint = fingerprintInitialPlanningFinalization("marker", [
            evidence.handoffId,
            evidence.handoffFingerprint,
            finalizationCommandId,
            resultEvidenceId,
            resultFingerprint,
            terminalStageEvent.eventId,
            String(terminalStageEvent.sequence),
            releaseEvent.eventId,
            String(releaseEvent.sequence),
            delivery.terminalAt!,
          ]);
          yield* sql`
          INSERT INTO agent_control_initial_planning_finalization_markers (
            marker_id, marker_fingerprint, finalization_command_id,
            result_evidence_id, handoff_id, committed_at
          ) VALUES (
            ${markerId}, ${markerFingerprint}, ${finalizationCommandId},
            ${resultEvidenceId}, ${evidence.handoffId}, ${delivery.terminalAt}
          )
        `;
          yield* hooks.beforeTransactionComplete({
            ...observation,
            stageRevision: currentStage.revision,
            leaseRevision: releaseEvent.streamVersion,
          });
          return {
            _tag: "Finalized" as const,
            observation,
            publication: {
              handoffId: evidence.handoffId,
              resultEvidenceId,
              outcome,
              stageEvents: committedStageEvents,
              leaseEvents: [releaseEvent],
            } satisfies AgentControlInitialPlanningFinalizationPublication,
          };
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          Effect.fail(
            finalizerError(evidence.handoffId, "finalization-transaction", "persistence", cause),
          ),
        ),
        Effect.mapError((cause) =>
          isFinalizerError(cause)
            ? cause
            : finalizerError(
                evidence.handoffId,
                "finalization-transaction",
                typeof cause === "object" &&
                  cause !== null &&
                  "_tag" in cause &&
                  (cause._tag === "AgentControlStageRunStreamVersionConflictError" ||
                    cause._tag === "AgentControlStageRunLeaseStreamVersionConflictError")
                  ? "revision-conflict"
                  : "persistence",
                cause,
              ),
        ),
      );

    if (transaction._tag === "Replayed") return transaction;
    if (transaction._tag === "Started") {
      if (transaction.publish) {
        yield* hooks.afterNativeCommit(transaction.observation);
        yield* Effect.uninterruptible(stageEngine.publishCommitted([transaction.event]));
        yield* hooks.afterPublication(transaction.observation);
      }
      return { _tag: "Started" as const, event: transaction.event };
    }
    yield* hooks.afterNativeCommit(transaction.observation);
    yield* Effect.uninterruptible(
      Effect.gen(function* () {
        yield* stageEngine.publishCommitted(transaction.publication.stageEvents);
        yield* leaseEngine.publishCommitted(transaction.publication.leaseEvents);
        yield* PubSub.publish(publications, transaction.publication);
      }),
    );
    yield* hooks.afterPublication(transaction.observation);
    return { _tag: "Finalized" as const, publication: transaction.publication };
  });

  const processHandoff = Effect.fn("AgentControlInitialPlanningFinalizer.processHandoff")(
    function* (handoffId: string) {
      const accepted = yield* replayFirst(handoffId);
      if (Option.isSome(accepted)) {
        return {
          _tag: "Replayed" as const,
          resultEvidenceId: accepted.value.resultEvidenceId,
        } satisfies AgentControlInitialPlanningFinalizerResult;
      }
      const claimOption = yield* store
        .loadAcceptedByHandoffId(handoffId)
        .pipe(
          Effect.mapError((cause) =>
            finalizerError(
              handoffId,
              "load-handoff",
              corruptHandoffStoreOperations.has(cause.operation)
                ? "corrupt-handoff"
                : "persistence",
              cause,
            ),
          ),
        );
      if (Option.isNone(claimOption)) {
        return yield* finalizerError(handoffId, "load-handoff", "corrupt-handoff");
      }
      const claim = claimOption.value;
      const providerTurnId = claim.delivery.providerTurnId;
      const providerAcceptedAt = claim.delivery.providerAcceptedAt;
      if (providerTurnId === null || providerAcceptedAt === null) {
        return { _tag: "Waiting" as const };
      }
      const deliveryTerminalState =
        claim.delivery.state === "completed" ||
        claim.delivery.state === "failed" ||
        claim.delivery.state === "interrupted"
          ? claim.delivery.state
          : null;
      const orchestration = yield* reconstructOrchestrationEvidence({
        handoffId: claim.evidence.handoffId,
        handoffFingerprint: claim.evidence.handoffFingerprint,
        threadId: claim.evidence.threadId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId,
        runtimeMode: claim.evidence.runtimeMode,
        providerAcceptedAt,
        terminalAt: claim.delivery.terminalAt,
        deliveryTerminalState,
      });
      if (orchestration === null) return { _tag: "Waiting" as const };
      const result = yield* processNew(claim).pipe(
        Effect.catchIf(
          (cause) => cause.reason === "revision-conflict" || cause.reason === "persistence",
          (cause) =>
            replayFirst(handoffId).pipe(
              Effect.flatMap((replay) =>
                Option.isSome(replay)
                  ? Effect.succeed({
                      _tag: "Replayed" as const,
                      resultEvidenceId: replay.value.resultEvidenceId,
                    })
                  : Effect.fail(cause),
              ),
            ),
        ),
      );
      if (claim.delivery.state === "ambiguous" && result._tag === "Started") {
        return { _tag: "Ambiguous" as const };
      }
      if (claim.delivery.state === "ambiguous") return { _tag: "Ambiguous" as const };
      return result;
    },
  );

  const recover = Effect.gen(function* () {
    const candidates = yield* store
      .listStageFinalizationCandidates()
      .pipe(
        Effect.mapError((cause) =>
          finalizerError("recovery", "list-candidates", "persistence", cause),
        ),
      );
    yield* Effect.forEach(
      candidates,
      (handoffId) =>
        processHandoff(handoffId).pipe(
          Effect.catchIf(
            (cause) => cause.reason !== "persistence" && cause.reason !== "revision-conflict",
            (cause) =>
              Effect.logError("initial planning stage finalization candidate failed", {
                handoffId,
                operation: cause.operation,
                reason: cause.reason,
                ...(cause.cause === undefined ? {} : { cause: cause.cause }),
              }),
          ),
        ),
      { concurrency: 1, discard: true },
    );
  });

  const processSafely = (handoffId: string | null) =>
    (handoffId === null ? recover : processHandoff(handoffId)).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("initial planning stage finalizer input failed", {
          handoffId,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  const worker = yield* makeDrainableWorker(processSafely);
  const start = Effect.fn("AgentControlInitialPlanningFinalizer.start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(wakeup.stream, (handoffId) => worker.enqueue(handoffId)),
    );
    const orchestrationSubscription = orchestrationEngine.subscribeDomainEvents;
    if (orchestrationSubscription !== undefined) {
      const stream = yield* orchestrationSubscription;
      yield* Effect.forkScoped(
        Stream.runForEach(stream, (event) =>
          event.aggregateKind === "thread" ? worker.enqueue(null) : Effect.void,
        ),
      );
    }
    yield* worker.enqueue(null);
  });

  return {
    processHandoff,
    recover,
    start,
    drain: worker.drain,
    streamPublications: Stream.fromPubSub(publications),
  } satisfies AgentControlInitialPlanningFinalizerShape;
});

export const AgentControlInitialPlanningFinalizerLive = Layer.effect(
  AgentControlInitialPlanningFinalizer,
  make,
);
