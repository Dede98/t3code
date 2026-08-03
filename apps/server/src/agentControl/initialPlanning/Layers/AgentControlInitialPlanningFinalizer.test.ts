import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "node:crypto";
import {
  AgentControlAttemptId,
  AgentControlControlledThreadReservationId,
  AgentControlRoleId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseHolderId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  CommandId,
  EventId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type AgentControlStageRunEvent,
  type AgentControlStageRunEventDraft,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseEventDraft,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { AgentControlProjectionStateRepositoryLive } from "../../../persistence/Layers/AgentControlProjectStates.ts";
import { AgentControlProjectionStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import { layer as AgentControlStageRunEventStoreLive } from "../../stageRun/Layers/AgentControlStageRunEventStore.ts";
import { layer as AgentControlStageRunProjectionLive } from "../../stageRun/Layers/AgentControlStageRunProjection.ts";
import { layer as AgentControlStageRunStateRepositoryLive } from "../../stageRun/Layers/AgentControlStageRunStateRepository.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { AgentControlStageRunEngine } from "../../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../../stageRun/Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { layer as AgentControlStageRunLeaseEventStoreLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseEventStore.ts";
import { layer as AgentControlStageRunLeaseProjectionLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseProjection.ts";
import { layer as AgentControlStageRunLeaseStateRepositoryLive } from "../../stageRunLease/Layers/AgentControlStageRunLeaseStateRepository.ts";
import { deriveAgentControlStageRunLeaseId } from "../../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../../stageRunLease/Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import {
  canonicalInitialPlanningEventTemplate,
  canonicalJson,
  combinedInitialPlanningEventDigest,
  initialPlanningMessagePayload,
  initialPlanningTurnRequestPayload,
  type JsonValue,
} from "../eventEvidence.ts";
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageEventId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  deriveAgentControlInitialPlanningTurnRequestEventId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../identity.ts";
import { fingerprintInitialPlanningFinalization } from "../finalizationIdentity.ts";
import type { AgentControlInitialPlanningHandoffEvidence } from "../model.ts";
import {
  AgentControlInitialPlanningFinalizer,
  type AgentControlInitialPlanningFinalizerShape,
} from "../Services/AgentControlInitialPlanningFinalizer.ts";
import {
  AgentControlInitialPlanningFinalizerHooks,
  type AgentControlInitialPlanningFinalizerHooksShape,
} from "../Services/AgentControlInitialPlanningFinalizerHooks.ts";
import { AgentControlInitialPlanningHandoffStore } from "../Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningFinalizerLive } from "./AgentControlInitialPlanningFinalizer.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "./AgentControlInitialPlanningHandoffStore.ts";

const createdAt = "2026-08-02T08:00:00.000Z";
const providerAcceptedAt = "2026-08-02T08:01:00.000Z";
const terminalAt = "2026-08-02T08:02:00.000Z";
const expiresAt = "2026-08-02T09:00:00.000Z";
const deadlineAt = "2026-08-02T10:00:00.000Z";
const barrierTimeout = "5 seconds";
const fixtureFingerprint = (value: string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const noopHooks: AgentControlInitialPlanningFinalizerHooksShape = {
  afterAuthoritativeRead: () => Effect.void,
  beforeTransactionComplete: () => Effect.void,
  afterNativeCommit: () => Effect.void,
  afterPublication: () => Effect.void,
};

interface SharedDatabase {
  readonly filename: string;
  readonly sqlA: SqlClient.SqlClient;
  readonly sqlB: SqlClient.SqlClient;
  readonly scopeA: Scope.Closeable;
  readonly scopeB: Scope.Closeable;
}

const makeSharedDatabase = Effect.fn("makeInitialPlanningFinalizerDatabase")(function* (
  toMigrationInclusive?: number,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({
    prefix: "t3-initial-planning-finalizer-",
  });
  const filename = path.join(directory, "state.sqlite");
  const scopeA = yield* Scope.make("sequential");
  const scopeB = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
  yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
  const contextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
  const contextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
  const sqlA = Context.get(contextA, SqlClient.SqlClient);
  const sqlB = Context.get(contextB, SqlClient.SqlClient);
  for (const sql of [sqlA, sqlB]) {
    const journal = yield* sql<{ readonly journal_mode: string }>`PRAGMA journal_mode = WAL`;
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.equal(journal[0]?.journal_mode, "wal");
  }
  const canonical = yield* fs.realPath(filename);
  const [databaseA] = yield* sqlA<{ readonly file: string }>`PRAGMA database_list`;
  const [databaseB] = yield* sqlB<{ readonly file: string }>`PRAGMA database_list`;
  assert.equal(databaseA?.file, canonical);
  assert.equal(databaseB?.file, canonical);
  assert.notStrictEqual(sqlA, sqlB);
  yield* runMigrations({ toMigrationInclusive }).pipe(
    Effect.provideService(SqlClient.SqlClient, sqlA),
  );
  return { filename, sqlA, sqlB, scopeA, scopeB } satisfies SharedDatabase;
});

interface FinalizerHarness {
  readonly finalizer: AgentControlInitialPlanningFinalizerShape;
  readonly store: AgentControlInitialPlanningHandoffStore["Service"];
  readonly stageEvents: AgentControlStageRunEventStore["Service"];
  readonly stageProjection: AgentControlStageRunProjection["Service"];
  readonly leaseEvents: AgentControlStageRunLeaseEventStore["Service"];
  readonly leaseProjection: AgentControlStageRunLeaseProjection["Service"];
  readonly stagePublished: Ref.Ref<ReadonlyArray<AgentControlStageRunEvent>>;
  readonly leasePublished: Ref.Ref<ReadonlyArray<AgentControlStageRunLeaseEvent>>;
}

const buildFinalizer = Effect.fn("buildInitialPlanningFinalizerHarness")(function* (
  sql: SqlClient.SqlClient,
  scope: Scope.Closeable,
  hooks: AgentControlInitialPlanningFinalizerHooksShape = noopHooks,
) {
  const sqlLayer = Layer.succeed(SqlClient.SqlClient, sql);
  const build = <I, E>(layer: Layer.Layer<I, E, never>) => Layer.buildWithScope(layer, scope);
  const stageEventContext = yield* build(
    Layer.fresh(AgentControlStageRunEventStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const stageStateContext = yield* build(
    Layer.fresh(AgentControlStageRunStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const leaseEventContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseEventStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const leaseStateContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const cursorContext = yield* build(
    Layer.fresh(AgentControlProjectionStateRepositoryLive).pipe(Layer.provide(sqlLayer)),
  );
  const stageEvents = Context.get(stageEventContext, AgentControlStageRunEventStore);
  const stageStates = Context.get(stageStateContext, AgentControlStageRunStateRepository);
  const leaseEvents = Context.get(leaseEventContext, AgentControlStageRunLeaseEventStore);
  const leaseStates = Context.get(leaseStateContext, AgentControlStageRunLeaseStateRepository);
  const cursors = Context.get(cursorContext, AgentControlProjectionStateRepository);
  const stageProjectionContext = yield* build(
    Layer.fresh(AgentControlStageRunProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlStageRunEventStore, stageEvents),
          Layer.succeed(AgentControlStageRunStateRepository, stageStates),
          Layer.succeed(AgentControlProjectionStateRepository, cursors),
        ),
      ),
    ),
  );
  const leaseProjectionContext = yield* build(
    Layer.fresh(AgentControlStageRunLeaseProjectionLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          sqlLayer,
          Layer.succeed(AgentControlStageRunLeaseEventStore, leaseEvents),
          Layer.succeed(AgentControlStageRunLeaseStateRepository, leaseStates),
          Layer.succeed(AgentControlProjectionStateRepository, cursors),
        ),
      ),
    ),
  );
  const stageProjection = Context.get(stageProjectionContext, AgentControlStageRunProjection);
  const leaseProjection = Context.get(leaseProjectionContext, AgentControlStageRunLeaseProjection);
  const storeContext = yield* build(
    Layer.fresh(AgentControlInitialPlanningHandoffStoreLive).pipe(Layer.provide(sqlLayer)),
  );
  const store = Context.get(storeContext, AgentControlInitialPlanningHandoffStore);
  const stagePublished = yield* Ref.make<ReadonlyArray<AgentControlStageRunEvent>>([]);
  const leasePublished = yield* Ref.make<ReadonlyArray<AgentControlStageRunLeaseEvent>>([]);
  const stageEngine = {
    publishCommitted: (events: ReadonlyArray<AgentControlStageRunEvent>) =>
      Ref.update(stagePublished, (current) => [...current, ...events]),
  } as AgentControlStageRunEngine["Service"];
  const leaseEngine = {
    publishCommitted: (events: ReadonlyArray<AgentControlStageRunLeaseEvent>) =>
      Ref.update(leasePublished, (current) => [...current, ...events]),
  } as AgentControlStageRunLeaseEngine["Service"];
  const orchestrationEngine = {} as OrchestrationEngineService["Service"];
  const dependencies = Layer.mergeAll(
    sqlLayer,
    Layer.succeed(AgentControlInitialPlanningHandoffStore, store),
    Layer.succeed(AgentControlStageRunEventStore, stageEvents),
    Layer.succeed(AgentControlStageRunStateRepository, stageStates),
    Layer.succeed(AgentControlStageRunProjection, stageProjection),
    Layer.succeed(AgentControlStageRunEngine, stageEngine),
    Layer.succeed(AgentControlStageRunLeaseEventStore, leaseEvents),
    Layer.succeed(AgentControlStageRunLeaseStateRepository, leaseStates),
    Layer.succeed(AgentControlStageRunLeaseProjection, leaseProjection),
    Layer.succeed(AgentControlStageRunLeaseEngine, leaseEngine),
    Layer.succeed(OrchestrationEngineService, orchestrationEngine),
    Layer.succeed(AgentControlInitialPlanningFinalizerHooks, hooks),
  );
  const finalizerContext = yield* build(
    Layer.fresh(AgentControlInitialPlanningFinalizerLive).pipe(Layer.provide(dependencies)),
  );
  return {
    finalizer: Context.get(finalizerContext, AgentControlInitialPlanningFinalizer),
    store,
    stageEvents,
    stageProjection,
    leaseEvents,
    leaseProjection,
    stagePublished,
    leasePublished,
  } satisfies FinalizerHarness;
});

type DeliveryState = "provider-started" | "completed" | "failed" | "interrupted" | "ambiguous";
type IdentityMismatch =
  | "handoff"
  | "reservation"
  | "thread"
  | "task"
  | "stageRun"
  | "attempt"
  | "lease"
  | "holder"
  | "fence"
  | "delivery";

interface SeededPlanning {
  readonly evidence: AgentControlInitialPlanningHandoffEvidence;
  readonly providerTurnId: TurnId;
  readonly planId: string;
  readonly stageRunId: AgentControlStageRunId;
  readonly attemptId: AgentControlAttemptId;
  readonly leaseId: AgentControlStageRunLeaseId;
}

const appendOrchestration = Effect.fn("appendInitialPlanningOrchestrationEvidence")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly suffix: string;
    readonly threadId: ThreadId;
    readonly type: "thread.session-set" | "thread.proposed-plan-upserted";
    readonly occurredAt: string;
    readonly payload: JsonValue;
    readonly metadata?: JsonValue;
  },
) {
  const versionRows = yield* sql<{ readonly version: number | null }>`
    SELECT MAX(stream_version) AS version FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${input.threadId}
  `;
  const streamVersion =
    versionRows[0]?.version === null || versionRows[0]?.version === undefined
      ? 0
      : versionRows[0].version + 1;
  const eventId = EventId.make(`provider-event-${input.suffix}-${streamVersion}`);
  const commandId = CommandId.make(`provider:${eventId}:${input.type}`);
  const payloadJson = canonicalJson(input.payload);
  const metadataJson = canonicalJson(input.metadata ?? {});
  const rows = yield* sql<{ readonly sequence: number }>`
    INSERT INTO orchestration_events (
      event_id, aggregate_kind, stream_id, stream_version, event_type,
      occurred_at, command_id, causation_event_id, correlation_id,
      actor_kind, payload_json, metadata_json
    ) VALUES (
      ${eventId}, 'thread', ${input.threadId}, ${streamVersion}, ${input.type},
      ${input.occurredAt}, ${commandId}, NULL, ${commandId}, 'provider',
      ${payloadJson}, ${metadataJson}
    ) RETURNING sequence
  `;
  return { eventId, sequence: rows[0]!.sequence, streamVersion };
});

const appendProviderStart = (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence" | "providerTurnId">,
  suffix: string,
) =>
  appendOrchestration(sql, {
    suffix: `${suffix}-start`,
    threadId: seeded.evidence.threadId,
    type: "thread.session-set",
    occurredAt: providerAcceptedAt,
    payload: {
      session: {
        activeTurnId: seeded.providerTurnId,
        lastError: null,
        providerInstanceId: seeded.evidence.providerInstanceId,
        providerName: "codex",
        runtimeMode: seeded.evidence.runtimeMode,
        status: "running",
        threadId: seeded.evidence.threadId,
        updatedAt: providerAcceptedAt,
      },
      threadId: seeded.evidence.threadId,
    },
  });

const appendProviderTerminal = (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence">,
  suffix: string,
  state: "completed" | "failed" | "interrupted",
) =>
  appendOrchestration(sql, {
    suffix: `${suffix}-terminal`,
    threadId: seeded.evidence.threadId,
    type: "thread.session-set",
    occurredAt: terminalAt,
    payload: {
      session: {
        activeTurnId: null,
        lastError: state === "failed" ? "provider failed" : null,
        providerInstanceId: seeded.evidence.providerInstanceId,
        providerName: "codex",
        runtimeMode: seeded.evidence.runtimeMode,
        status: state === "failed" ? "error" : "ready",
        threadId: seeded.evidence.threadId,
        updatedAt: terminalAt,
      },
      threadId: seeded.evidence.threadId,
    },
  });

const appendPlan = Effect.fn("appendInitialPlanningPlanEvidence")(function* (
  sql: SqlClient.SqlClient,
  seeded: Pick<SeededPlanning, "evidence" | "providerTurnId" | "planId">,
  suffix: string,
  options?: {
    readonly planMarkdown?: string;
    readonly planId?: string;
    readonly turnId?: string;
    readonly project?: boolean;
  },
) {
  const proposedPlan = {
    id: options?.planId ?? seeded.planId,
    turnId: options?.turnId ?? seeded.providerTurnId,
    planMarkdown: options?.planMarkdown ?? "# Proposed Plan\n\n1. Finalize Planning.",
    implementedAt: null,
    implementationThreadId: null,
    createdAt: providerAcceptedAt,
    updatedAt: providerAcceptedAt,
  } as const;
  const event = yield* appendOrchestration(sql, {
    suffix: `${suffix}-plan`,
    threadId: seeded.evidence.threadId,
    type: "thread.proposed-plan-upserted",
    occurredAt: providerAcceptedAt,
    payload: { proposedPlan, threadId: seeded.evidence.threadId },
    metadata: { providerTurnId: seeded.providerTurnId },
  });
  if (options?.project !== false) {
    yield* sql`
      INSERT INTO projection_thread_proposed_plans (
        plan_id, thread_id, turn_id, plan_markdown, created_at, updated_at,
        implemented_at, implementation_thread_id
      ) VALUES (
        ${proposedPlan.id}, ${seeded.evidence.threadId}, ${proposedPlan.turnId},
        ${proposedPlan.planMarkdown}, ${proposedPlan.createdAt}, ${proposedPlan.updatedAt},
        NULL, NULL
      )
      ON CONFLICT(plan_id) DO UPDATE SET
        thread_id = excluded.thread_id, turn_id = excluded.turn_id,
        plan_markdown = excluded.plan_markdown, updated_at = excluded.updated_at
    `;
  }
  return event;
});

const seedPlanning = Effect.fn("seedInitialPlanningFinalization")(function* (
  sql: SqlClient.SqlClient,
  harness: FinalizerHarness,
  suffix: string,
  deliveryState: DeliveryState = "provider-started",
  identityMismatch?: IdentityMismatch,
) {
  const projectId = ProjectId.make(`project-${suffix}`);
  const taskId = AgentControlTaskId.make(`task-${suffix}`);
  const sourceIdentityFingerprint = "3".repeat(64);
  const stageRunId = yield* deriveAgentControlStageRunId({
    projectId,
    taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageKind: "planning",
    stageOrdinal: 1,
  });
  const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
  const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
  const leaseHolderId = AgentControlStageRunLeaseHolderId.make(`holder-${suffix}`);
  const reservationId = AgentControlControlledThreadReservationId.make(`reservation-${suffix}`);
  const threadId = ThreadId.make(`thread-${suffix}`);
  const providerTurnId = TurnId.make(`turn-${suffix}`);
  const canonicalHandoffId = yield* deriveAgentControlInitialPlanningHandoffId(
    reservationId,
    threadId,
  );
  const evidenceThreadId =
    identityMismatch === "thread" ? ThreadId.make(`foreign-thread-${suffix}`) : threadId;
  const handoffId =
    identityMismatch === "handoff" ? `foreign-handoff-${suffix}` : canonicalHandoffId;
  const turnRequestCommandId =
    yield* deriveAgentControlInitialPlanningTurnRequestCommandId(handoffId);
  const messageId = yield* deriveAgentControlInitialPlanningMessageId(handoffId);
  const canonicalProviderDeliveryId =
    yield* deriveAgentControlInitialPlanningProviderDeliveryId(handoffId);
  const providerDeliveryId =
    identityMismatch === "delivery"
      ? `foreign-provider-delivery-${suffix}`
      : canonicalProviderDeliveryId;
  const messageEventId =
    yield* deriveAgentControlInitialPlanningMessageEventId(turnRequestCommandId);
  const turnRequestEventId =
    yield* deriveAgentControlInitialPlanningTurnRequestEventId(turnRequestCommandId);
  const modelSelection = {
    instanceId: ProviderInstanceId.make(`provider-${suffix}`),
    model: "gpt-5.6",
    options: [{ id: "reasoningEffort", value: "high" }],
  } as const;
  const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);
  const promptText = "Produce exactly one proposed plan.";
  const messageTemplate = canonicalInitialPlanningEventTemplate({
    streamVersion: 3,
    eventId: messageEventId,
    aggregateKind: "thread",
    aggregateId: evidenceThreadId,
    type: "thread.message-sent",
    occurredAt: createdAt,
    commandId: turnRequestCommandId,
    causationEventId: null,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: initialPlanningMessagePayload({
      threadId: evidenceThreadId,
      messageId,
      promptText,
      createdAt,
    }),
    metadata: {},
  });
  const turnTemplate = canonicalInitialPlanningEventTemplate({
    streamVersion: 4,
    eventId: turnRequestEventId,
    aggregateKind: "thread",
    aggregateId: evidenceThreadId,
    type: "thread.turn-start-requested",
    occurredAt: createdAt,
    commandId: turnRequestCommandId,
    causationEventId: messageEventId,
    correlationId: turnRequestCommandId,
    actorKind: "client",
    payload: initialPlanningTurnRequestPayload({
      threadId: evidenceThreadId,
      messageId,
      modelSelection,
      runtimeMode: "approval-required",
      createdAt,
    }),
    metadata: {},
  });
  const base = {
    handoffId,
    coordinatorCommandId: CommandId.make(`coordinator-${suffix}`),
    coordinatorCommandFingerprint: fixtureFingerprint(`coordinator-${suffix}`),
    materializationCommandId: CommandId.make(`materialization-${suffix}`),
    materializationCommandFingerprint: fixtureFingerprint(`materialization-${suffix}`),
    projectId,
    controlledThreadReservationId:
      identityMismatch === "reservation"
        ? AgentControlControlledThreadReservationId.make(`foreign-reservation-${suffix}`)
        : reservationId,
    threadId: evidenceThreadId,
    taskId:
      identityMismatch === "task" ? AgentControlTaskId.make(`foreign-task-${suffix}`) : taskId,
    taskRevision: 1,
    githubIntakeSequence: 1,
    sourceIdentityFingerprint,
    stageRunId:
      identityMismatch === "stageRun"
        ? AgentControlStageRunId.make(`foreign-stage-${suffix}`)
        : stageRunId,
    attemptId:
      identityMismatch === "attempt"
        ? AgentControlAttemptId.make(`foreign-attempt-${suffix}`)
        : attemptId,
    roleId: "planning",
    stageKind: "planning",
    stageOrdinal: 1,
    attemptOrdinal: 1,
    leaseId:
      identityMismatch === "lease"
        ? AgentControlStageRunLeaseId.make(`foreign-lease-${suffix}`)
        : leaseId,
    leaseHolderId:
      identityMismatch === "holder"
        ? AgentControlStageRunLeaseHolderId.make(`foreign-holder-${suffix}`)
        : leaseHolderId,
    fenceToken: identityMismatch === "fence" ? 2 : 1,
    worktreeReservationId: `worktree-${suffix}`,
    worktreePath: `/tmp/t3-initial-planning-finalizer-${suffix}`,
    planningRole: "planner",
    providerInstanceId: modelSelection.instanceId,
    runtimeMode: "approval-required",
    modelSelectionJson: modelEvidence.modelSelectionJson,
    templateVersion: "agent-control-initial-planning-prompt-v1",
    promptText,
    turnRequestCommandId,
    messageId,
    messageEventId,
    turnRequestEventId,
    providerDeliveryId,
  } as const;
  const evidence = {
    ...base,
    handoffFingerprint: fingerprintAgentControlInitialPlanningHandoff(base),
    modelSelection,
    messageEventTemplateJson: messageTemplate,
    turnRequestEventTemplateJson: turnTemplate,
    eventTemplateDigest: combinedInitialPlanningEventDigest(messageTemplate, turnTemplate),
    createdAt,
    planningDeadlineAt: deadlineAt,
  } satisfies AgentControlInitialPlanningHandoffEvidence;

  const preparedDraft: AgentControlStageRunEventDraft = {
    eventId: EventId.make(`stage-prepared-${suffix}`),
    type: "agentControl.stageRun.prepared",
    aggregateKind: "stage-run",
    aggregateId: stageRunId,
    occurredAt: createdAt,
    commandId: CommandId.make(`stage-prepare-${suffix}`),
    causationEventId: null,
    correlationId: CommandId.make(`stage-prepare-${suffix}`),
    authority: "controller",
    payload: {
      projectId,
      taskId,
      stageRunId,
      attemptId,
      roleId: AgentControlRoleId.make("planning"),
      stageKind: "planning",
      stageOrdinal: 1,
      attemptOrdinal: 1,
      status: "prepared",
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      preparedAt: createdAt,
    },
    metadata: { schemaVersion: 1 },
  };
  const [prepared] = yield* harness.stageEvents.append({
    stageRunId,
    expectedStreamVersion: 0,
    events: [preparedDraft],
  });
  yield* harness.stageProjection.projectEvent(prepared!);
  const reservedDraft: AgentControlStageRunLeaseEventDraft = {
    eventId: EventId.make(`lease-reserved-${suffix}`),
    type: "agentControl.stageRunLease.reserved",
    aggregateKind: "stage-run-lease",
    aggregateId: leaseId,
    occurredAt: createdAt,
    commandId: CommandId.make(`lease-reserve-${suffix}`),
    causationEventId: null,
    correlationId: CommandId.make(`lease-reserve-${suffix}`),
    authority: "controller",
    payload: {
      leaseId,
      projectId,
      taskId,
      stageRunId,
      attemptId,
      taskRevision: 1,
      githubIntakeSequence: 1,
      sourceIdentityFingerprint,
      holderId: leaseHolderId,
      fenceToken: 1,
      acquiredAt: createdAt,
      renewedAt: createdAt,
      expiresAt,
    },
    metadata: { schemaVersion: 1 },
  };
  const [reserved] = yield* harness.leaseEvents.append({
    leaseId,
    expectedStreamVersion: 0,
    events: [reservedDraft],
  });
  yield* harness.leaseProjection.projectEvent(reserved!);

  // Only the already-covered upstream materialization family is bypassed.
  // The handoff store, delivery CAS chain, stage/lease stores, projections,
  // finalization transaction, and replay path remain their production layers.
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_handoff_intent_validate`;
  yield* sql.withTransaction(harness.store.insertAcceptedInTransaction(evidence));
  yield* sql`DROP TRIGGER IF EXISTS agent_control_initial_planning_turn_accepted_validate`;
  const acceptedRows = yield* sql<{ readonly handoffId: string }>`
    SELECT handoff_id AS "handoffId"
    FROM agent_control_initial_planning_turn_accepted
  `;
  const messageEventSequence = acceptedRows.length * 2 + 1;
  const turnRequestEventSequence = messageEventSequence + 1;
  yield* sql`
    INSERT INTO agent_control_initial_planning_turn_accepted (
      handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
      thread_id, turn_request_command_id, message_id, message_event_id,
      message_event_sequence, turn_request_event_id, turn_request_event_sequence,
      message_event_envelope_json, turn_request_event_envelope_json,
      event_evidence_digest, receipt_authority, accepted_at
    ) VALUES (
      ${handoffId}, ${evidence.handoffFingerprint}, ${evidence.controlledThreadReservationId},
      ${evidence.threadId},
      ${turnRequestCommandId}, ${messageId}, ${messageEventId},
      CAST(${messageEventSequence} AS INTEGER), ${turnRequestEventId},
      CAST(${turnRequestEventSequence} AS INTEGER), ${messageTemplate}, ${turnTemplate},
      ${fixtureFingerprint(`event-evidence-${suffix}`)}, 'agent-control', ${createdAt}
    )
  `;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* harness.store.markTurnAccepted(handoffId, 0, createdAt);
  const claim = Option.getOrThrow(
    yield* harness.store.claim({
      handoffId,
      ownerId: `owner-${suffix}`,
      now: createdAt,
      expiresAt,
    }),
  );
  yield* harness.store.markDeliveryAttempted({
    providerDeliveryId,
    handoffId,
    ownerId: `owner-${suffix}`,
    claimGeneration: claim.delivery.claimGeneration,
    expectedRevision: claim.delivery.revision,
    attemptedAt: createdAt,
    providerSessionCreatedAt: createdAt,
    providerResumeCursorJson: "null",
    providerInstanceId: modelSelection.instanceId,
    turnModelSelectionJson: modelEvidence.modelSelectionJson,
    turnModelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
  });
  const started = yield* harness.store.markProviderStarted({
    handoffId,
    ownerId: `owner-${suffix}`,
    claimGeneration: claim.delivery.claimGeneration,
    expectedRevision: claim.delivery.revision + 1,
    providerTurnId,
    acceptedAt: providerAcceptedAt,
  });
  if (deliveryState === "ambiguous") {
    yield* harness.store.markAmbiguous({
      handoffId,
      expectedRevision: started.revision,
      terminalAt,
    });
  } else if (deliveryState !== "provider-started") {
    yield* harness.store.markTerminal({
      handoffId,
      expectedRevision: started.revision,
      state: deliveryState,
      terminalAt,
      errorCode: deliveryState === "failed" ? "provider-defect" : null,
    });
  }
  return {
    evidence,
    providerTurnId,
    planId: `plan:${evidence.threadId}:turn:${providerTurnId}`,
    stageRunId,
    attemptId,
    leaseId,
  } satisfies SeededPlanning;
});

const markTerminal = (
  store: AgentControlInitialPlanningHandoffStore["Service"],
  seeded: SeededPlanning,
  state: "completed" | "failed" | "interrupted",
) =>
  store.markTerminal({
    handoffId: seeded.evidence.handoffId,
    expectedRevision: 4,
    state,
    terminalAt,
    errorCode: state === "failed" ? "provider-defect" : null,
  });

const finalizationCounts = (sql: SqlClient.SqlClient, seeded: SeededPlanning) =>
  sql<{
    readonly stageEvents: number;
    readonly leaseEvents: number;
    readonly started: number;
    readonly evidence: number;
    readonly receipts: number;
    readonly markers: number;
  }>`
    SELECT
      (SELECT COUNT(*) FROM agent_control_events
       WHERE aggregate_kind = 'stage-run' AND stream_id = ${seeded.stageRunId}) AS "stageEvents",
      (SELECT COUNT(*) FROM agent_control_events
       WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${seeded.leaseId}) AS "leaseEvents",
      (SELECT COUNT(*) FROM agent_control_initial_planning_stage_started
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS started,
      (SELECT COUNT(*) FROM agent_control_initial_planning_result_evidence
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS evidence,
      (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_receipts
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS receipts,
      (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_markers
       WHERE handoff_id = ${seeded.evidence.handoffId}) AS markers
  `.pipe(Effect.map((rows) => rows[0]!));

const migration053HardeningTriggers = [
  "agent_control_initial_planning_stage_event_validate",
  "agent_control_initial_planning_lease_event_validate",
  "agent_control_initial_planning_lifecycle_event_no_update",
  "agent_control_initial_planning_lifecycle_event_no_delete",
  "agent_control_initial_planning_stage_started_validate",
  "agent_control_initial_planning_result_evidence_validate",
  "agent_control_initial_planning_finalization_receipt_validate",
  "agent_control_initial_planning_finalization_marker_validate",
] as const;

const prepareHistoricalMigration052 = Effect.fn("prepareHistoricalMigration052")(function* (
  sql: SqlClient.SqlClient,
) {
  for (const trigger of migration053HardeningTriggers) {
    yield* sql.unsafe(`DROP TRIGGER ${trigger}`).unprepared;
  }
  yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 53`;
});

const captureMigration053State = Effect.fn("captureMigration053State")(function* (
  sql: SqlClient.SqlClient,
) {
  return {
    migrations: yield* sql`
      SELECT migration_id, name, created_at
      FROM effect_sql_migrations
      ORDER BY migration_id
    `,
    deliveries: yield* sql`
      SELECT * FROM agent_control_initial_planning_deliveries ORDER BY handoff_id
    `,
    triggers: yield* sql`
      SELECT name, sql
      FROM sqlite_schema
      WHERE type = 'trigger'
        AND name LIKE 'agent_control_initial_planning_%'
      ORDER BY name
    `,
    stageStarted: yield* sql`
      SELECT * FROM agent_control_initial_planning_stage_started ORDER BY handoff_id
    `,
    resultEvidence: yield* sql`
      SELECT * FROM agent_control_initial_planning_result_evidence ORDER BY handoff_id
    `,
    receipts: yield* sql`
      SELECT * FROM agent_control_initial_planning_finalization_receipts ORDER BY handoff_id
    `,
    markers: yield* sql`
      SELECT * FROM agent_control_initial_planning_finalization_markers ORDER BY handoff_id
    `,
    sequences: yield* sql`SELECT name, seq FROM sqlite_sequence ORDER BY name`,
  };
});

const seedLegacyPlanningParents = Effect.fn("seedLegacyPlanningParents")(function* (
  sql: SqlClient.SqlClient,
  seeded: SeededPlanning,
  ordinal: number,
) {
  const evidence = seeded.evidence;
  const createdEventId = `legacy-materialization-created-${ordinal}`;
  const boundEventId = `legacy-materialization-bound-${ordinal}`;
  const createdSequence = 1000 + ordinal * 2;
  const boundSequence = createdSequence + 1;
  const finalizationOwnerId = `00000000-0000-4000-8000-${String(ordinal).padStart(12, "0")}`;
  const bindingJson = canonicalJson({
    attemptId: evidence.attemptId,
    controlState: "controlled",
    roleId: "planning",
    stageRunId: evidence.stageRunId,
    taskId: evidence.taskId,
  });
  const parentTables = [
    "orchestration_agent_control_thread_materialization_intents",
    "orchestration_agent_control_thread_materialization_receipts",
    "agent_control_controlled_thread_materialization_intents",
    "agent_control_controlled_thread_materialization_receipts",
    "agent_control_controlled_thread_materialization_accepted",
  ] as const;
  const triggers = yield* sql<{ readonly name: string; readonly sql: string }>`
    SELECT name, sql FROM sqlite_schema
    WHERE type = 'trigger' AND sql IS NOT NULL
      AND tbl_name IN ${sql.in(parentTables)}
    ORDER BY name
  `;
  yield* sql`PRAGMA foreign_keys = OFF`;
  for (const trigger of triggers) {
    yield* sql.unsafe(`DROP TRIGGER ${trigger.name}`).unprepared;
  }
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO orchestration_events (
          sequence, event_id, aggregate_kind, stream_id, stream_version,
          event_type, occurred_at, command_id, causation_event_id,
          correlation_id, actor_kind, payload_json, metadata_json
        ) VALUES
          (${createdSequence}, ${createdEventId}, 'thread', ${evidence.threadId}, 1,
            'thread.created', ${evidence.createdAt}, ${evidence.materializationCommandId},
            NULL, ${evidence.materializationCommandId}, 'server',
            ${canonicalJson({
              branch: `legacy-branch-${ordinal}`,
              createdAt: evidence.createdAt,
              interactionMode: "plan",
              modelSelection: evidence.modelSelection,
              projectId: evidence.projectId,
              runtimeMode: evidence.runtimeMode,
              threadId: evidence.threadId,
              title: "Initial planning",
              updatedAt: evidence.createdAt,
              worktreePath: evidence.worktreePath,
            })}, '{}'),
          (${boundSequence}, ${boundEventId}, 'thread', ${evidence.threadId}, 2,
            'thread.agent-control-bound', ${evidence.createdAt},
            ${evidence.materializationCommandId}, ${createdEventId},
            ${evidence.materializationCommandId}, 'server',
            ${canonicalJson({
              binding: {
                attemptId: evidence.attemptId,
                controlState: "controlled",
                roleId: "planning",
                stageRunId: evidence.stageRunId,
                taskId: evidence.taskId,
              },
              threadId: evidence.threadId,
              updatedAt: evidence.createdAt,
            })}, '{}')
      `;
      yield* sql`
        INSERT INTO orchestration_command_receipts (
          command_id, authority, aggregate_kind, aggregate_id, accepted_at,
          result_sequence, status, error
        ) VALUES (
          ${evidence.materializationCommandId}, 'agent-control', 'thread',
          ${evidence.threadId}, ${evidence.createdAt}, ${boundSequence}, 'accepted', NULL
        )
      `;
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
          accepted_receipt_command_id, receipt_status, receipt_result_sequence,
          receipt_accepted_at, receipt_error, created_at
        ) VALUES (
          ${evidence.materializationCommandId}, 'thread.agent-control.materialize',
          'agent-control', 'thread', ${evidence.materializationCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.projectId}, ${evidence.taskId}, ${evidence.taskRevision},
          ${evidence.githubIntakeSequence}, ${evidence.sourceIdentityFingerprint},
          ${evidence.stageRunId}, ${evidence.attemptId}, 'planning', 'planning', 1, 1,
          ${evidence.leaseId}, ${evidence.fenceToken}, ${evidence.worktreeReservationId},
          'Initial planning', ${evidence.modelSelectionJson}, 'approval-required', 'plan',
          ${`legacy-branch-${ordinal}`}, ${evidence.worktreePath}, ${bindingJson},
          ${createdEventId}, 'thread.created', ${createdSequence}, 1,
          ${boundEventId}, 'thread.agent-control-bound', ${boundSequence}, 2,
          ${evidence.materializationCommandId}, 'accepted', ${boundSequence},
          ${evidence.createdAt}, NULL, ${evidence.createdAt}
        )
      `;
      yield* sql`
        INSERT INTO orchestration_agent_control_thread_materialization_receipts (
          command_id, command_type, authority, aggregate_kind, thread_id,
          command_fingerprint, result_sequence, accepted_at, status
        ) VALUES (
          ${evidence.materializationCommandId}, 'thread.agent-control.materialize',
          'agent-control', 'thread', ${evidence.threadId},
          ${evidence.materializationCommandFingerprint}, ${boundSequence},
          ${evidence.createdAt}, 'accepted'
        )
      `;
    }),
  );
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_intents (
          coordinator_command_id, finalization_owner_id, request_fingerprint,
          coordinator_command_fingerprint, policy_binding_fingerprint,
          runtime_observation_fingerprint, project_id,
          controlled_thread_reservation_id, thread_id, task_id, task_revision,
          github_intake_sequence, source_identity_fingerprint, stage_run_id,
          attempt_id, role_id, stage_kind, stage_ordinal, attempt_ordinal,
          lease_id, lease_holder_id, fence_token, worktree_reservation_id,
          materializing_transition_command_id, bound_transition_command_id,
          materialization_command_id, materialization_command_fingerprint,
          title, model_selection_json, runtime_mode, interaction_mode, branch,
          worktree_path, binding_json, materializing_event_id,
          materializing_event_sequence, bound_event_id, bound_event_sequence,
          orchestration_result_sequence, materializing_at, materialized_at,
          bound_at, accepted_at, accepted_marker_command_id
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${finalizationOwnerId},
          ${fixtureFingerprint(`legacy-request-${ordinal}`)},
          ${evidence.coordinatorCommandFingerprint},
          ${fixtureFingerprint(`legacy-policy-${ordinal}`)},
          ${fixtureFingerprint(`legacy-runtime-${ordinal}`)}, ${evidence.projectId},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.taskId}, ${evidence.taskRevision}, ${evidence.githubIntakeSequence},
          ${evidence.sourceIdentityFingerprint}, ${evidence.stageRunId},
          ${evidence.attemptId}, 'planning', 'planning', 1, 1, ${evidence.leaseId},
          ${evidence.leaseHolderId}, ${evidence.fenceToken},
          ${evidence.worktreeReservationId}, ${`legacy-materializing-${ordinal}`},
          ${`legacy-bound-${ordinal}`}, ${evidence.materializationCommandId},
          ${evidence.materializationCommandFingerprint}, 'Initial planning',
          ${evidence.modelSelectionJson}, 'approval-required', 'plan',
          ${`legacy-branch-${ordinal}`}, ${evidence.worktreePath}, ${bindingJson},
          ${`legacy-materializing-event-${ordinal}`}, 1,
          ${`legacy-bound-event-${ordinal}`}, 2, ${boundSequence},
          ${evidence.createdAt}, ${evidence.createdAt}, ${evidence.createdAt},
          ${evidence.createdAt}, ${evidence.coordinatorCommandId}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_receipts (
          coordinator_command_id, request_fingerprint,
          coordinator_command_fingerprint, controlled_thread_reservation_id,
          thread_id, materialization_command_id,
          materialization_command_fingerprint, orchestration_result_sequence,
          status, accepted_at, accepted_marker_command_id
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${fixtureFingerprint(`legacy-request-${ordinal}`)},
          ${evidence.coordinatorCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.materializationCommandId}, ${evidence.materializationCommandFingerprint},
          ${boundSequence}, 'accepted', ${evidence.createdAt},
          ${evidence.coordinatorCommandId}
        )
      `;
      yield* sql`
        INSERT INTO agent_control_controlled_thread_materialization_accepted (
          coordinator_command_id, finalization_owner_id,
          coordinator_command_fingerprint, controlled_thread_reservation_id,
          thread_id, materialization_command_id,
          materialization_command_fingerprint, orchestration_result_sequence,
          accepted_at
        ) VALUES (
          ${evidence.coordinatorCommandId}, ${finalizationOwnerId},
          ${evidence.coordinatorCommandFingerprint},
          ${evidence.controlledThreadReservationId}, ${evidence.threadId},
          ${evidence.materializationCommandId}, ${evidence.materializationCommandFingerprint},
          ${boundSequence}, ${evidence.createdAt}
        )
      `;
    }),
  );
  for (const trigger of triggers) {
    yield* sql.unsafe(trigger.sql).unprepared;
  }
  yield* sql`PRAGMA foreign_keys = ON`;
});

const makeLegacyFinalizations = Effect.fn("makeLegacyFinalizations")(function* (
  suffixes: ReadonlyArray<string>,
) {
  const database = yield* makeSharedDatabase();
  const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
  const seeded: SeededPlanning[] = [];
  for (const suffix of suffixes) {
    const candidate = yield* seedPlanning(database.sqlA, harness, suffix);
    seeded.push(candidate);
    yield* appendProviderStart(database.sqlA, candidate, suffix);
    assert.equal(
      (yield* harness.finalizer.processHandoff(candidate.evidence.handoffId))._tag,
      "Started",
    );
    yield* appendPlan(database.sqlA, candidate, suffix);
    yield* appendProviderTerminal(database.sqlA, candidate, suffix, "completed");
    yield* markTerminal(harness.store, candidate, "completed");
    assert.equal(
      (yield* harness.finalizer.processHandoff(candidate.evidence.handoffId))._tag,
      "Finalized",
    );
  }
  yield* database.sqlA.unsafe("DROP TRIGGER agent_control_initial_planning_stage_started_no_update")
    .unprepared;
  for (const candidate of seeded) {
    const [started] = yield* database.sqlA<{
      readonly startFingerprint: string;
      readonly deliveryRevision: number;
      readonly orchestrationStartedEventId: string;
      readonly orchestrationStartedSequence: number;
      readonly orchestrationStartedStreamVersion: number;
      readonly stageEventId: string;
      readonly stageEventSequence: number;
      readonly stageEventStreamVersion: number;
      readonly providerAcceptedAt: string;
    }>`
      SELECT start_fingerprint AS "startFingerprint",
        delivery_revision AS "deliveryRevision",
        orchestration_started_event_id AS "orchestrationStartedEventId",
        orchestration_started_sequence AS "orchestrationStartedSequence",
        orchestration_started_stream_version AS "orchestrationStartedStreamVersion",
        stage_event_id AS "stageEventId",
        stage_event_sequence AS "stageEventSequence",
        stage_event_stream_version AS "stageEventStreamVersion",
        provider_accepted_at AS "providerAcceptedAt"
      FROM agent_control_initial_planning_stage_started
      WHERE handoff_id = ${candidate.evidence.handoffId}
    `;
    const modelEvidence = canonicalProviderModelSelectionEvidence(
      candidate.evidence.modelSelection,
    );
    const nextOrchestrationVersion = started!.orchestrationStartedStreamVersion + 3;
    const startFingerprint = fingerprintInitialPlanningFinalization("start", [
      candidate.evidence.handoffId,
      candidate.evidence.handoffFingerprint,
      candidate.evidence.projectId,
      candidate.evidence.taskId,
      String(candidate.evidence.taskRevision),
      String(candidate.evidence.githubIntakeSequence),
      candidate.evidence.sourceIdentityFingerprint,
      candidate.evidence.controlledThreadReservationId,
      candidate.evidence.threadId,
      candidate.evidence.stageRunId,
      candidate.evidence.attemptId,
      candidate.evidence.leaseId,
      candidate.evidence.leaseHolderId,
      String(candidate.evidence.fenceToken),
      candidate.evidence.providerDeliveryId,
      candidate.evidence.providerInstanceId,
      candidate.providerTurnId,
      candidate.evidence.runtimeMode,
      modelEvidence.modelSelectionFingerprint,
      started!.providerAcceptedAt,
      String(started!.deliveryRevision),
      started!.orchestrationStartedEventId,
      String(started!.orchestrationStartedSequence),
      String(nextOrchestrationVersion),
      started!.stageEventId,
      String(started!.stageEventSequence),
      String(started!.stageEventStreamVersion),
      started!.providerAcceptedAt,
    ]);
    yield* database.sqlA`
      UPDATE orchestration_events
      SET stream_version = stream_version + 3
      WHERE aggregate_kind = 'thread' AND stream_id = ${candidate.evidence.threadId}
    `;
    yield* database.sqlA.withTransaction(database.sqlA`
      UPDATE agent_control_initial_planning_stage_started
      SET orchestration_started_stream_version = ${nextOrchestrationVersion},
        start_fingerprint = ${startFingerprint}
      WHERE handoff_id = ${candidate.evidence.handoffId}
    `);
  }
  yield* database.sqlA.unsafe(
    `CREATE TRIGGER agent_control_initial_planning_stage_started_no_update
     BEFORE UPDATE ON agent_control_initial_planning_stage_started
     BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
  ).unprepared;
  yield* database.sqlA.unsafe("DROP TRIGGER agent_control_initial_planning_turn_accepted_no_delete")
    .unprepared;
  yield* database.sqlA.withTransaction(
    database.sqlA`DELETE FROM agent_control_initial_planning_turn_accepted`,
  );
  yield* database.sqlA.unsafe(
    `CREATE TRIGGER agent_control_initial_planning_turn_accepted_no_delete
     BEFORE DELETE ON agent_control_initial_planning_turn_accepted
     BEGIN SELECT RAISE(ABORT, 'initial planning turn acceptance is immutable'); END`,
  ).unprepared;
  for (const [index, candidate] of seeded.entries()) {
    yield* seedLegacyPlanningParents(database.sqlA, candidate, index + 1);
  }
  assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
  yield* prepareHistoricalMigration052(database.sqlA);
  return { database, seeded };
});

const makeLegacyFinalization = Effect.fn("makeLegacyFinalization")(function* (suffix: string) {
  const fixture = yield* makeLegacyFinalizations([suffix]);
  return { database: fixture.database, seeded: fixture.seeded[0]! };
});

const makeRecoverableMigration052Planning = Effect.fn("makeRecoverableMigration052Planning")(
  function* (suffix: string) {
    const database = yield* makeSharedDatabase(52);
    const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
    const seeded = yield* seedPlanning(database.sqlA, harness, suffix);
    yield* appendProviderStart(database.sqlA, seeded, suffix);
    assert.equal(
      (yield* harness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
      "Started",
    );
    yield* seedLegacyPlanningParents(database.sqlA, seeded, 1);
    yield* database.sqlA.unsafe(
      "DROP TRIGGER agent_control_initial_planning_turn_accepted_no_delete",
    ).unprepared;
    yield* database.sqlA.withTransaction(database.sqlA`
    DELETE FROM agent_control_initial_planning_turn_accepted
    WHERE handoff_id = ${seeded.evidence.handoffId}
  `);
    yield* database.sqlA.unsafe(
      `CREATE TRIGGER agent_control_initial_planning_turn_accepted_no_delete
     BEFORE DELETE ON agent_control_initial_planning_turn_accepted
     BEGIN SELECT RAISE(ABORT, 'initial planning turn acceptance is immutable'); END`,
    ).unprepared;
    assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
      stageEvents: 2,
      leaseEvents: 1,
      started: 1,
      evidence: 0,
      receipts: 0,
      markers: 0,
    });
    assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
    return { database, harness, seeded };
  },
);

type LegacyMutation =
  | "stage-project"
  | "stage-task"
  | "stage-thread"
  | "stage-provider"
  | "stage-provider-turn"
  | "stage-run"
  | "stage-attempt"
  | "stage-lease"
  | "stage-holder"
  | "stage-fence"
  | "stage-sequence"
  | "stage-revision"
  | "result-outcome"
  | "result-plan-digest"
  | "result-stage-coordinate"
  | "result-lease-coordinate"
  | "result-storage-class"
  | "result-noncanonical-json"
  | "receipt-command"
  | "receipt-fingerprint"
  | "receipt-handoff"
  | "receipt-outcome"
  | "receipt-coordinate"
  | "marker-incomplete"
  | "foreign-key";

const mutateLegacyEvidence = Effect.fn("mutateLegacyInitialPlanningEvidence")(function* (
  sql: SqlClient.SqlClient,
  handoffId: string,
  mutation: LegacyMutation,
  original: {
    readonly stage: Record<string, unknown>;
    readonly result: Record<string, unknown>;
    readonly receipt: Record<string, unknown>;
    readonly marker: Record<string, unknown>;
  },
  restore = false,
) {
  const table =
    mutation.startsWith("stage-") || mutation === "foreign-key"
      ? "agent_control_initial_planning_stage_started"
      : mutation.startsWith("result-")
        ? "agent_control_initial_planning_result_evidence"
        : mutation.startsWith("receipt-")
          ? "agent_control_initial_planning_finalization_receipts"
          : "agent_control_initial_planning_finalization_markers";
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql`PRAGMA ignore_check_constraints = ON`;
  yield* sql.unsafe(`DROP TRIGGER ${table}_no_update`).unprepared;
  switch (mutation) {
    case "stage-project":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET project_id = ${restore ? original.stage.project_id : "legacy-foreign-project"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-task":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET task_id = ${restore ? original.stage.task_id : "legacy-foreign-task"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-thread":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET thread_id = ${restore ? original.stage.thread_id : "legacy-foreign-thread"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-provider":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET provider_instance_id = ${
          restore ? original.stage.provider_instance_id : "legacy-foreign-provider"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-provider-turn":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET provider_turn_id = ${restore ? original.stage.provider_turn_id : "legacy-foreign-turn"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-run":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_run_id = ${restore ? original.stage.stage_run_id : "legacy-foreign-stage"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-attempt":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET attempt_id = ${restore ? original.stage.attempt_id : "legacy-foreign-attempt"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-lease":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET lease_id = ${restore ? original.stage.lease_id : "legacy-foreign-lease"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-holder":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET lease_holder_id = ${restore ? original.stage.lease_holder_id : "legacy-foreign-holder"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-fence":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET fence_token = ${restore ? original.stage.fence_token : 999}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-sequence":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_event_sequence = ${restore ? original.stage.stage_event_sequence : 900001}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "stage-revision":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET stage_event_stream_version = ${restore ? original.stage.stage_event_stream_version : 9}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-outcome":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET outcome = ${restore ? original.result.outcome : "failed"}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-plan-digest":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET proposed_plan_digest = ${
          restore ? original.result.proposed_plan_digest : "f".repeat(64)
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-stage-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET stage_event_sequence = ${restore ? original.result.stage_event_sequence : 900002}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-lease-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET lease_event_sequence = ${restore ? original.result.lease_event_sequence : 900003}
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-storage-class":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET project_id = ${
          restore
            ? original.result.project_id
            : new TextEncoder().encode(String(original.result.project_id))
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "result-noncanonical-json":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_result_evidence
        SET proposed_plan_json = ${
          restore
            ? original.result.proposed_plan_json
            : ` ${String(original.result.proposed_plan_json)}`
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-command":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET finalization_command_id = ${
          restore ? original.receipt.finalization_command_id : "legacy-foreign-command"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-fingerprint":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET finalization_fingerprint = ${
          restore ? original.receipt.finalization_fingerprint : "e".repeat(64)
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
    case "receipt-handoff":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET handoff_id = ${restore ? original.receipt.handoff_id : "legacy-foreign-handoff"}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "receipt-outcome":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET outcome = ${restore ? original.receipt.outcome : "failed"}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "receipt-coordinate":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_receipts
        SET stage_event_sequence = ${restore ? original.receipt.stage_event_sequence : 900004}
        WHERE result_evidence_id = ${original.receipt.result_evidence_id}
      `);
      break;
    case "marker-incomplete":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET finalization_command_id = ${
          restore ? original.marker.finalization_command_id : "legacy-missing-command"
        }
        WHERE result_evidence_id = ${original.marker.result_evidence_id}
      `);
      break;
    case "foreign-key":
      yield* sql.withTransaction(sql`
        UPDATE agent_control_initial_planning_stage_started
        SET orchestration_started_event_id = ${
          restore ? original.stage.orchestration_started_event_id : "legacy-missing-event"
        }
        WHERE handoff_id = ${handoffId}
      `);
      break;
  }
  yield* sql.unsafe(
    `CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
     BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
  ).unprepared;
  yield* sql`PRAGMA ignore_check_constraints = OFF`;
  yield* sql`PRAGMA foreign_keys = ON`;
});

const swapLegacyMarkerEvidence = Effect.fn("swapLegacyMarkerEvidence")(function* (
  sql: SqlClient.SqlClient,
  left: Record<string, unknown>,
  right: Record<string, unknown>,
  restore = false,
) {
  yield* sql`PRAGMA foreign_keys = OFF`;
  yield* sql.unsafe("DROP TRIGGER agent_control_initial_planning_finalization_markers_no_update")
    .unprepared;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = 'legacy-marker-swap-temporary'
        WHERE marker_id = ${left.marker_id}
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = ${restore ? right.result_evidence_id : left.result_evidence_id}
        WHERE marker_id = ${right.marker_id}
      `;
      yield* sql`
        UPDATE agent_control_initial_planning_finalization_markers
        SET result_evidence_id = ${restore ? left.result_evidence_id : right.result_evidence_id}
        WHERE marker_id = ${left.marker_id}
      `;
    }),
  );
  yield* sql.unsafe(
    `CREATE TRIGGER agent_control_initial_planning_finalization_markers_no_update
     BEFORE UPDATE ON agent_control_initial_planning_finalization_markers
     BEGIN SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable'); END`,
  ).unprepared;
  yield* sql`PRAGMA foreign_keys = ON`;
});

const withNode = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(NodeServices.layer));

it.effect.each<{
  readonly delivery: "completed" | "failed" | "interrupted";
  readonly expectedStage: "succeeded" | "failed" | "cancelled";
}>([
  { delivery: "completed", expectedStage: "succeeded" },
  { delivery: "failed", expectedStage: "failed" },
  { delivery: "interrupted", expectedStage: "cancelled" },
])(
  "migration 053 preserves a recoverable $delivery delivery and recovery finalizes it once",
  ({ delivery, expectedStage }) =>
    withNode(
      Effect.gen(function* () {
        const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
          `legacy-recoverable-${delivery}`,
        );
        if (delivery === "completed") {
          yield* appendPlan(database.sqlA, seeded, `legacy-recoverable-${delivery}`);
        }
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `legacy-recoverable-${delivery}`,
          delivery,
        );
        if (delivery === "interrupted") {
          const interruptRequested = yield* harness.store.requestInterrupt({
            handoffId: seeded.evidence.handoffId,
            expectedRevision: 4,
            requestedAt: terminalAt,
          });
          yield* harness.store.markTerminal({
            handoffId: seeded.evidence.handoffId,
            expectedRevision: interruptRequested.revision,
            state: delivery,
            terminalAt,
            errorCode: "provider-aborted",
          });
        } else {
          yield* markTerminal(harness.store, seeded, delivery);
        }
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
          stageEvents: 2,
          leaseEvents: 1,
          started: 1,
          evidence: 0,
          receipts: 0,
          markers: 0,
        });

        assert.deepStrictEqual(
          yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
          [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
        );

        const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
        yield* recovered.finalizer.recover;
        const finalized = yield* finalizationCounts(database.sqlB, seeded);
        assert.deepStrictEqual(finalized, {
          stageEvents: 3,
          leaseEvents: 2,
          started: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.deepStrictEqual(
          (yield* Ref.get(recovered.stagePublished)).map((event) => event.payload.status),
          [expectedStage],
        );
        assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);

        yield* recovered.finalizer.recover;
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), finalized);
        assert.equal((yield* Ref.get(recovered.stagePublished)).length, 1);
        assert.equal((yield* Ref.get(recovered.leasePublished)).length, 1);
      }),
    ),
);

it.effect.each<{
  readonly successor:
    | "provider-started"
    | "interrupt-requested"
    | "ambiguous"
    | "interrupt-requested-ambiguous";
}>([
  { successor: "provider-started" },
  { successor: "interrupt-requested" },
  { successor: "ambiguous" },
  { successor: "interrupt-requested-ambiguous" },
])("migration 053 accepts the recoverable $successor successor", ({ successor }) =>
  withNode(
    Effect.gen(function* () {
      const { database, harness, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-successor-${successor}`,
      );
      if (successor === "interrupt-requested") {
        yield* harness.store.requestInterrupt({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          requestedAt: terminalAt,
        });
      } else if (successor === "ambiguous") {
        yield* harness.store.markAmbiguous({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          terminalAt,
        });
      } else if (successor === "interrupt-requested-ambiguous") {
        const interruptRequested = yield* harness.store.requestInterrupt({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: 4,
          requestedAt: terminalAt,
        });
        yield* harness.store.markAmbiguous({
          handoffId: seeded.evidence.handoffId,
          expectedRevision: interruptRequested.revision,
          terminalAt,
        });
      }
      const before = yield* finalizationCounts(database.sqlA, seeded);
      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, database.sqlA),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const recovered = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* recovered.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), before);
      assert.equal((yield* Ref.get(recovered.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(recovered.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{
  readonly corruption: "provider-turn" | "revision-backward" | "revision-gap" | "illegal-state";
}>([
  { corruption: "provider-turn" },
  { corruption: "revision-backward" },
  { corruption: "revision-gap" },
  { corruption: "illegal-state" },
])("migration 053 rejects a recoverable delivery with $corruption corruption", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeRecoverableMigration052Planning(
        `legacy-successor-${corruption}`,
      );
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_delivery_transition_validate",
      ).unprepared;
      if (corruption === "provider-turn") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET provider_turn_id = 'foreign-provider-turn'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "revision-backward") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET revision = 3
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else if (corruption === "revision-gap") {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET state = 'completed', revision = 6,
            terminal_at = ${terminalAt}, updated_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      } else {
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_deliveries
          SET state = 'retry-wait', revision = 5,
            provider_turn_id = NULL, provider_accepted_at = NULL,
            next_attempt_at = ${terminalAt}, last_error_code = 'transient-not-accepted',
            updated_at = ${terminalAt}
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      }
      const before = yield* captureMigration053State(database.sqlA);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sqlA),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect("migration 053 rejects a partially present legacy result chain", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeLegacyFinalization("legacy-partial-result-chain");
      yield* database.sqlA.unsafe(
        "DROP TRIGGER agent_control_initial_planning_finalization_markers_no_delete",
      ).unprepared;
      yield* database.sqlA.withTransaction(database.sqlA`
        DELETE FROM agent_control_initial_planning_finalization_markers
        WHERE handoff_id = ${seeded.evidence.handoffId}
      `);
      assert.deepStrictEqual(yield* database.sqlA`PRAGMA foreign_key_check`, []);
      const before = yield* captureMigration053State(database.sqlA);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, database.sqlA),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
    }),
  ),
);

it.effect(
  "rejects mismatched legacy planning companion evidence before migration 053 hardening",
  () =>
    withNode(
      Effect.gen(function* () {
        const { database, seeded } = yield* makeLegacyFinalization("legacy-project-mismatch");
        yield* database.sqlA.unsafe(
          "DROP TRIGGER agent_control_initial_planning_stage_started_no_update",
        ).unprepared;
        yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_stage_started
          SET project_id = 'legacy-foreign-project'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
        yield* database.sqlA.unsafe(
          `CREATE TRIGGER agent_control_initial_planning_stage_started_no_update
           BEFORE UPDATE ON agent_control_initial_planning_stage_started
           BEGIN
             SELECT RAISE(ABORT, 'initial planning finalization evidence is immutable');
           END`,
        ).unprepared;
        const before = yield* captureMigration053State(database.sqlA);

        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, database.sqlA),
          ),
        );

        assert.isTrue(Exit.isFailure(upgrade));
        assert.deepStrictEqual(yield* captureMigration053State(database.sqlA), before);
      }),
    ),
);

it.effect("validates all legacy companion rows and rolls migration 053 back atomically", () =>
  withNode(
    Effect.gen(function* () {
      const { database, seeded } = yield* makeLegacyFinalizations([
        "legacy-validation-a",
        "legacy-validation-b",
        "legacy-validation-c",
      ]);
      const sql = database.sqlA;
      const valid = yield* captureMigration053State(sql);
      const target = seeded.toSorted((left, right) =>
        left.evidence.handoffId.localeCompare(right.evidence.handoffId),
      )[1]!;
      const rowFor = (rows: ReadonlyArray<Record<string, unknown>>) =>
        rows.find((row) => row.handoff_id === target.evidence.handoffId)!;
      const original = {
        stage: rowFor(valid.stageStarted),
        result: rowFor(valid.resultEvidence),
        receipt: rowFor(valid.receipts),
        marker: rowFor(valid.markers),
      };
      const mutations: ReadonlyArray<LegacyMutation> = [
        "stage-project",
        "stage-task",
        "stage-thread",
        "stage-provider",
        "stage-provider-turn",
        "stage-run",
        "stage-attempt",
        "stage-lease",
        "stage-holder",
        "stage-fence",
        "stage-sequence",
        "stage-revision",
        "result-outcome",
        "result-plan-digest",
        "result-stage-coordinate",
        "result-lease-coordinate",
        "result-storage-class",
        "result-noncanonical-json",
        "receipt-command",
        "receipt-fingerprint",
        "receipt-handoff",
        "receipt-outcome",
        "receipt-coordinate",
        "marker-incomplete",
        "foreign-key",
      ];
      for (const mutation of mutations) {
        yield* mutateLegacyEvidence(sql, target.evidence.handoffId, mutation, original);
        const foreignKeys = yield* sql<Record<string, unknown>>`PRAGMA foreign_key_check`;
        if (mutation === "foreign-key") assert.isAbove(foreignKeys.length, 0);
        const before = yield* captureMigration053State(sql);
        const upgrade = yield* Effect.exit(
          runMigrations({ toMigrationInclusive: 53 }).pipe(
            Effect.provideService(SqlClient.SqlClient, sql),
          ),
        );
        assert.isTrue(Exit.isFailure(upgrade), mutation);
        if (mutation === "foreign-key" && Exit.isFailure(upgrade)) {
          const rendered = Cause.pretty(upgrade.cause);
          assert.include(rendered, "agent_control_initial_planning_stage_started");
          assert.include(rendered, "orchestration_events");
        }
        assert.deepStrictEqual(yield* captureMigration053State(sql), before, mutation);
        yield* mutateLegacyEvidence(sql, target.evidence.handoffId, mutation, original, true);
      }

      const [leftMarker, rightMarker] = valid.markers;
      yield* swapLegacyMarkerEvidence(sql, leftMarker!, rightMarker!);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      const mismatchedMarkers = yield* captureMigration053State(sql);
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            runMigrations({ toMigrationInclusive: 53 }).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            ),
          ),
        ),
      );
      assert.deepStrictEqual(yield* captureMigration053State(sql), mismatchedMarkers);
      yield* swapLegacyMarkerEvidence(sql, leftMarker!, rightMarker!, true);

      const restored = yield* captureMigration053State(sql);
      assert.deepStrictEqual(restored.stageStarted, valid.stageStarted);
      assert.deepStrictEqual(restored.resultEvidence, valid.resultEvidence);
      assert.deepStrictEqual(restored.receipts, valid.receipts);
      assert.deepStrictEqual(restored.markers, valid.markers);
      assert.deepStrictEqual(restored.sequences, valid.sequences);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);

      assert.deepStrictEqual(
        yield* runMigrations({ toMigrationInclusive: 53 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        ),
        [[53, "AgentControlInitialPlanningStageFinalizationHardening"]],
      );
      const hardened = yield* captureMigration053State(sql);
      assert.deepStrictEqual(hardened.stageStarted, valid.stageStarted);
      assert.deepStrictEqual(hardened.resultEvidence, valid.resultEvidence);
      assert.deepStrictEqual(hardened.receipts, valid.receipts);
      assert.deepStrictEqual(hardened.markers, valid.markers);
      assert.deepStrictEqual(hardened.sequences, valid.sequences);
      assert.equal(hardened.migrations.filter((row) => row.migration_id === 53).length, 1);
      const hardenedTriggerNames = new Set(hardened.triggers.map((row) => row.name));
      for (const trigger of migration053HardeningTriggers) {
        assert.isTrue(hardenedTriggerNames.has(trigger), trigger);
      }
    }),
  ),
);

it.effect(
  "finalizes plan-before-completion exactly once and replays duplicate notifications receipt-first",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const afterCommitCounts = yield* Ref.make<ReadonlyArray<number>>([]);
        const publicationCount = yield* Ref.make(0);
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
          ...noopHooks,
          afterNativeCommit: () =>
            database.sqlA<{ readonly count: number }>`
            SELECT COUNT(*) AS count
            FROM agent_control_initial_planning_finalization_receipts
          `.pipe(
              Effect.flatMap((rows) =>
                Ref.update(afterCommitCounts, (current) => [...current, rows[0]!.count]),
              ),
              Effect.orDie,
            ),
          afterPublication: () => Ref.update(publicationCount, (count) => count + 1),
        });
        const seeded = yield* seedPlanning(database.sqlA, harness, "plan-first");
        yield* appendProviderStart(database.sqlA, seeded, "plan-first");
        const started = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(started._tag, "Started");
        yield* appendPlan(database.sqlA, seeded, "plan-first");
        yield* appendProviderTerminal(database.sqlA, seeded, "plan-first", "completed");
        yield* markTerminal(harness.store, seeded, "completed");

        const finalized = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(finalized._tag, "Finalized");
        const replayed = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(replayed._tag, "Replayed");
        const counts = yield* finalizationCounts(database.sqlA, seeded);
        assert.deepStrictEqual(counts, {
          stageEvents: 3,
          leaseEvents: 2,
          started: 1,
          evidence: 1,
          receipts: 1,
          markers: 1,
        });
        assert.deepStrictEqual(yield* Ref.get(afterCommitCounts), [0, 1]);
        assert.equal(yield* Ref.get(publicationCount), 2);
        assert.deepStrictEqual(
          (yield* Ref.get(harness.stagePublished)).map((event) => event.type),
          ["agentControl.stageRun.planningStarted", "agentControl.stageRun.planningSucceeded"],
        );
        assert.deepStrictEqual(
          (yield* Ref.get(harness.leasePublished)).map((event) => event.type),
          ["agentControl.stageRunLease.releasedAfterPlanning"],
        );
      }),
    ),
);

it.effect("joins completion-before-plan after a fresh-layer restart", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "terminal-first");
      yield* appendProviderStart(database.sqlA, seeded, "terminal-first");
      yield* appendProviderTerminal(database.sqlA, seeded, "terminal-first", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const started = yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(started._tag, "Started");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 2,
        leaseEvents: 1,
        started: 1,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });

      yield* appendPlan(database.sqlA, seeded, "terminal-first");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const finalized = yield* harnessB.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(finalized._tag, "Finalized");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 1);
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 1);
    }),
  ),
);

it.effect("fails closed when a second plan commits after the authoritative snapshot", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const authoritativeRead = yield* Deferred.make<void>();
      const releaseFinalizer = yield* Deferred.make<void>();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(authoritativeRead, undefined);
            yield* Deferred.await(releaseFinalizer);
          }),
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "late-second-plan");
      yield* appendProviderStart(database.sqlA, seeded, "late-second-plan");
      yield* appendPlan(database.sqlA, seeded, "late-second-plan-a");
      yield* appendProviderTerminal(database.sqlA, seeded, "late-second-plan", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");

      const fiber = yield* harnessA.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(authoritativeRead).pipe(Effect.timeout(barrierTimeout));
      yield* appendPlan(database.sqlB, seeded, "late-second-plan-b");
      yield* Deferred.succeed(releaseFinalizer, undefined);
      const raced = yield* Fiber.join(fiber).pipe(Effect.timeout(barrierTimeout));
      assert.isTrue(Exit.isFailure(raced));

      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const retry = yield* Effect.exit(
        harnessB.finalizer.processHandoff(seeded.evidence.handoffId),
      );
      assert.isTrue(Exit.isFailure(retry));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessA.leasePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
    }),
  ),
);

it.effect("rejoins provider-start and plan across restart before the terminal observation", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "restart-between");
      yield* appendProviderStart(database.sqlA, seeded, "restart-between");
      yield* appendPlan(database.sqlA, seeded, "restart-between");
      const started = yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(started._tag, "Started");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 2,
        leaseEvents: 1,
        started: 1,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });

      yield* appendProviderTerminal(database.sqlA, seeded, "restart-between", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      yield* harnessB.finalizer.recover;
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.deepStrictEqual(
        (yield* Ref.get(harnessB.stagePublished)).map((event) => event.type),
        ["agentControl.stageRun.planningSucceeded"],
      );
    }),
  ),
);

it.live("commits once when two fresh finalizers race on independent WAL connections", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const arrivedA = yield* Deferred.make<void>();
      const arrivedB = yield* Deferred.make<void>();
      const releaseA = yield* Deferred.make<void>();
      const releaseB = yield* Deferred.make<void>();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(arrivedA, undefined);
            yield* Deferred.await(releaseA);
          }),
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "two-finalizers");
      yield* appendProviderStart(database.sqlA, seeded, "two-finalizers");
      yield* appendPlan(database.sqlA, seeded, "two-finalizers");
      yield* appendProviderTerminal(database.sqlA, seeded, "two-finalizers", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB, {
        ...noopHooks,
        beforeTransactionComplete: () =>
          Effect.gen(function* () {
            yield* Deferred.succeed(arrivedB, undefined);
            yield* Deferred.await(releaseB);
          }),
      });

      const fiberB = yield* harnessB.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      const statusB = yield* Effect.race(
        Deferred.await(arrivedB).pipe(Effect.as({ _tag: "Arrived" as const })),
        Fiber.join(fiberB).pipe(Effect.map((result) => ({ _tag: "Completed" as const, result }))),
      ).pipe(Effect.timeout(barrierTimeout));
      assert.equal(statusB._tag, "Arrived");
      const fiberA = yield* harnessA.finalizer
        .processHandoff(seeded.evidence.handoffId)
        .pipe(Effect.result, Effect.forkChild);
      const statusA = yield* Effect.race(
        Deferred.await(arrivedA).pipe(Effect.as({ _tag: "Arrived" as const })),
        Fiber.join(fiberA).pipe(Effect.map((result) => ({ _tag: "Completed" as const, result }))),
      ).pipe(Effect.timeout(barrierTimeout));
      assert.equal(statusA._tag, "Arrived");
      yield* Deferred.succeed(releaseB, undefined);
      const resultB = yield* Fiber.join(fiberB).pipe(Effect.timeout(barrierTimeout));
      yield* Deferred.succeed(releaseA, undefined);
      const resultA = yield* Fiber.join(fiberA).pipe(Effect.timeout(barrierTimeout));
      assert.equal(resultA._tag, "Success");
      assert.equal(resultB._tag, "Success");
      if (resultA._tag === "Success" && resultB._tag === "Success") {
        assert.deepStrictEqual([resultA.success._tag, resultB.success._tag].sort(), [
          "Finalized",
          "Replayed",
        ]);
      }
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal(
        (yield* Ref.get(harnessA.stagePublished)).length +
          (yield* Ref.get(harnessB.stagePublished)).length,
        2,
      );
      assert.equal(
        (yield* Ref.get(harnessA.leasePublished)).length +
          (yield* Ref.get(harnessB.leasePublished)).length,
        1,
      );
    }),
  ),
);

it.effect.each<{
  readonly delivery: "failed" | "interrupted" | "ambiguous";
  readonly expectedResult: "Finalized" | "Ambiguous";
  readonly expectedStage: "failed" | "cancelled" | "running";
  readonly expectedStageEvents: number;
  readonly expectedLeaseEvents: number;
}>([
  {
    delivery: "failed",
    expectedResult: "Finalized",
    expectedStage: "failed",
    expectedStageEvents: 3,
    expectedLeaseEvents: 2,
  },
  {
    delivery: "interrupted",
    expectedResult: "Finalized",
    expectedStage: "cancelled",
    expectedStageEvents: 3,
    expectedLeaseEvents: 2,
  },
  {
    delivery: "ambiguous",
    expectedResult: "Ambiguous",
    expectedStage: "running",
    expectedStageEvents: 2,
    expectedLeaseEvents: 1,
  },
])(
  "$delivery maps to the exact stage and lease transition",
  ({ delivery, expectedResult, expectedStage, expectedStageEvents, expectedLeaseEvents }) =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
        const seeded = yield* seedPlanning(database.sqlA, harness, `outcome-${delivery}`, delivery);
        yield* appendProviderStart(database.sqlA, seeded, `outcome-${delivery}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `outcome-${delivery}`,
          delivery === "ambiguous" ? "interrupted" : delivery,
        );
        const result = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
        assert.equal(result._tag, expectedResult);
        const [stage] = yield* database.sqlA<{ readonly status: string }>`
          SELECT status FROM agent_control_stage_run_states
          WHERE stage_run_id = ${seeded.stageRunId}
        `;
        const [lease] = yield* database.sqlA<{ readonly status: string }>`
          SELECT status FROM agent_control_stage_run_lease_states
          WHERE lease_id = ${seeded.leaseId}
        `;
        assert.equal(stage?.status, expectedStage);
        assert.equal(lease?.status, delivery === "ambiguous" ? "reserved" : "released");
        const counts = yield* finalizationCounts(database.sqlA, seeded);
        assert.equal(counts.stageEvents, expectedStageEvents);
        assert.equal(counts.leaseEvents, expectedLeaseEvents);
        assert.equal(counts.evidence, delivery === "ambiguous" ? 0 : 1);
        assert.equal(counts.receipts, delivery === "ambiguous" ? 0 : 1);
        assert.equal(counts.markers, delivery === "ambiguous" ? 0 : 1);
      }),
    ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase before commit rolls back the complete terminal boundary", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        beforeTransactionComplete: () =>
          phase === "defect" ? Effect.die(new Error("before-commit")) : Effect.interrupt,
      });
      const seeded = yield* seedPlanning(database.sqlA, harness, `before-${phase}`);
      yield* appendProviderStart(database.sqlA, seeded, `before-${phase}`);
      yield* appendPlan(database.sqlA, seeded, `before-${phase}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `before-${phase}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect("receipt-first replay rejects targeted accepted-evidence corruption", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harnessA, "receipt-corruption");
      yield* appendProviderStart(database.sqlA, seeded, "receipt-corruption");
      yield* appendPlan(database.sqlA, seeded, "receipt-corruption");
      yield* appendProviderTerminal(database.sqlA, seeded, "receipt-corruption", "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      assert.equal(
        (yield* harnessA.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
        "Finalized",
      );
      yield* database.sqlA`
        DROP TRIGGER agent_control_initial_planning_finalization_receipts_no_update
      `;
      yield* database.sqlA.withTransaction(database.sqlA`
          UPDATE agent_control_initial_planning_finalization_receipts SET outcome = 'failed'
          WHERE handoff_id = ${seeded.evidence.handoffId}
        `);
      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const exit = yield* Effect.exit(harnessB.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlB, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
    }),
  ),
);

it.effect("finalization evidence tables reject every update and delete", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, "immutable-evidence");
      yield* appendProviderStart(database.sqlA, seeded, "immutable-evidence");
      yield* appendPlan(database.sqlA, seeded, "immutable-evidence");
      yield* appendProviderTerminal(database.sqlA, seeded, "immutable-evidence", "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      assert.equal(
        (yield* harness.finalizer.processHandoff(seeded.evidence.handoffId))._tag,
        "Finalized",
      );
      for (const table of [
        "agent_control_initial_planning_stage_started",
        "agent_control_initial_planning_result_evidence",
        "agent_control_initial_planning_finalization_receipts",
        "agent_control_initial_planning_finalization_markers",
      ] as const) {
        for (const statement of [
          `UPDATE ${table} SET handoff_id = handoff_id`,
          `DELETE FROM ${table}`,
        ]) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(database.sqlA.withTransaction(database.sqlA.unsafe(statement))),
            ),
            statement,
          );
        }
      }
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
    }),
  ),
);

it.effect("recovery isolates an invalid first candidate and finalizes the healthy successor", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seededCandidates = [
        yield* seedPlanning(database.sqlA, harness, "recovery-isolation-a"),
        yield* seedPlanning(database.sqlA, harness, "recovery-isolation-b"),
      ];
      for (const seeded of seededCandidates) {
        yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `terminal-${seeded.evidence.handoffId}`,
          "completed",
        );
        yield* markTerminal(harness.store, seeded, "completed");
      }
      const [invalid, healthy] = seededCandidates.toSorted((left, right) =>
        left.evidence.handoffId.localeCompare(right.evidence.handoffId),
      );
      assert.isDefined(invalid);
      assert.isDefined(healthy);
      yield* database.sqlA`
        UPDATE agent_control_stage_run_states SET status = 'running'
        WHERE stage_run_id = ${invalid!.stageRunId}
      `;

      yield* harness.finalizer.recover;

      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, invalid!), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, healthy!), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 2);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 1);
    }),
  ),
);

it.effect(
  "recovery records every invalid candidate and replays healthy candidates exactly once",
  () =>
    withNode(
      Effect.gen(function* () {
        const database = yield* makeSharedDatabase();
        const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
        const candidates = [
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-a"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-b"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-c"),
          yield* seedPlanning(database.sqlA, harness, "recovery-matrix-d"),
        ].toSorted((left, right) =>
          left.evidence.handoffId.localeCompare(right.evidence.handoffId),
        );
        for (const seeded of candidates) {
          yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
          yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
          yield* appendProviderTerminal(
            database.sqlA,
            seeded,
            `terminal-${seeded.evidence.handoffId}`,
            "completed",
          );
          yield* markTerminal(harness.store, seeded, "completed");
        }
        const invalid = candidates.slice(0, 2);
        const healthy = candidates.slice(2);
        for (const seeded of invalid) {
          yield* database.sqlA`
          UPDATE agent_control_stage_run_states SET status = 'running'
          WHERE stage_run_id = ${seeded!.stageRunId}
        `;
        }

        yield* harness.finalizer.recover;
        const publicationsAfterFirstRun = {
          stage: (yield* Ref.get(harness.stagePublished)).length,
          lease: (yield* Ref.get(harness.leasePublished)).length,
        };
        assert.deepStrictEqual(publicationsAfterFirstRun, { stage: 4, lease: 2 });
        for (const seeded of invalid) {
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(harness.finalizer.processHandoff(seeded!.evidence.handoffId)),
            ),
          );
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded!), {
            stageEvents: 1,
            leaseEvents: 1,
            started: 0,
            evidence: 0,
            receipts: 0,
            markers: 0,
          });
        }
        for (const seeded of healthy) {
          assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded!), {
            stageEvents: 3,
            leaseEvents: 2,
            started: 1,
            evidence: 1,
            receipts: 1,
            markers: 1,
          });
        }

        yield* harness.finalizer.recover;
        assert.deepStrictEqual(
          {
            stage: (yield* Ref.get(harness.stagePublished)).length,
            lease: (yield* Ref.get(harness.leasePublished)).length,
          },
          publicationsAfterFirstRun,
        );
      }),
    ),
);

it.effect("recovery preserves candidate defects and stops before later candidates", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const seededById = new Map<string, SeededPlanning>();
      const defect = new Error("candidate-defect");
      let firstHandoffId = "";
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: (observation) =>
          observation.handoffId === firstHandoffId ? Effect.die(defect) : Effect.void,
      });
      for (const suffix of ["recovery-defect-a", "recovery-defect-b"] as const) {
        const seeded = yield* seedPlanning(database.sqlA, harness, suffix);
        seededById.set(seeded.evidence.handoffId, seeded);
        yield* appendProviderStart(database.sqlA, seeded, `start-${suffix}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${suffix}`);
        yield* appendProviderTerminal(database.sqlA, seeded, `terminal-${suffix}`, "completed");
        yield* markTerminal(harness.store, seeded, "completed");
      }
      const handoffIds = [...seededById.keys()].toSorted();
      firstHandoffId = handoffIds[0]!;
      const exit = yield* Effect.exit(harness.finalizer.recover);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) assert.include(Cause.pretty(exit.cause), "candidate-defect");
      for (const handoffId of handoffIds) {
        assert.deepStrictEqual(
          yield* finalizationCounts(database.sqlA, seededById.get(handoffId)!),
          {
            stageEvents: 1,
            leaseEvents: 1,
            started: 0,
            evidence: 0,
            receipts: 0,
            markers: 0,
          },
        );
      }
    }),
  ),
);

it.effect("a real recovery fiber interrupt is preserved without trailing candidate work", () =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const arrived = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let firstHandoffId = "";
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterAuthoritativeRead: (observation) =>
          observation.handoffId === firstHandoffId
            ? Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(release)))
            : Effect.void,
      });
      const candidates = [
        yield* seedPlanning(database.sqlA, harness, "recovery-interrupt-a"),
        yield* seedPlanning(database.sqlA, harness, "recovery-interrupt-b"),
      ].toSorted((left, right) => left.evidence.handoffId.localeCompare(right.evidence.handoffId));
      firstHandoffId = candidates[0]!.evidence.handoffId;
      for (const seeded of candidates) {
        yield* appendProviderStart(database.sqlA, seeded, `start-${seeded.evidence.handoffId}`);
        yield* appendPlan(database.sqlA, seeded, `plan-${seeded.evidence.handoffId}`);
        yield* appendProviderTerminal(
          database.sqlA,
          seeded,
          `terminal-${seeded.evidence.handoffId}`,
          "completed",
        );
        yield* markTerminal(harness.store, seeded, "completed");
      }

      const fiber = yield* Effect.forkChild(harness.finalizer.recover);
      yield* Deferred.await(arrived).pipe(Effect.timeout(barrierTimeout));
      yield* Fiber.interrupt(fiber);
      const interrupted = yield* Fiber.await(fiber);
      assert.isTrue(Exit.isFailure(interrupted));
      if (Exit.isFailure(interrupted)) assert.isTrue(Cause.hasInterruptsOnly(interrupted.cause));
      for (const seeded of candidates) {
        assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
          stageEvents: 1,
          leaseEvents: 1,
          started: 0,
          evidence: 0,
          receipts: 0,
          markers: 0,
        });
      }
    }),
  ),
);

it.effect.each<{
  readonly caseName:
    | "missing"
    | "empty"
    | "multiple"
    | "conflicting"
    | "projection-mismatch"
    | "damaged";
}>([
  { caseName: "missing" },
  { caseName: "empty" },
  { caseName: "multiple" },
  { caseName: "conflicting" },
  { caseName: "projection-mismatch" },
  { caseName: "damaged" },
])("$caseName plan evidence cannot terminalize success", ({ caseName }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `plan-${caseName}`);
      yield* appendProviderStart(database.sqlA, seeded, `plan-${caseName}`);
      if (caseName === "empty") {
        yield* appendOrchestration(database.sqlA, {
          suffix: "empty-plan",
          threadId: seeded.evidence.threadId,
          type: "thread.proposed-plan-upserted",
          occurredAt: providerAcceptedAt,
          payload: {
            proposedPlan: {
              id: seeded.planId,
              turnId: seeded.providerTurnId,
              planMarkdown: "",
              implementedAt: null,
              implementationThreadId: null,
              createdAt: providerAcceptedAt,
              updatedAt: providerAcceptedAt,
            },
            threadId: seeded.evidence.threadId,
          },
        });
      } else if (caseName === "multiple") {
        yield* appendPlan(database.sqlA, seeded, "multiple-a");
        yield* appendPlan(database.sqlA, seeded, "multiple-b");
      } else if (caseName === "conflicting") {
        yield* appendPlan(database.sqlA, seeded, "conflicting-a");
        yield* appendPlan(database.sqlA, seeded, "conflicting-b", {
          planId: `${seeded.planId}-other`,
          planMarkdown: "# A different plan",
        });
      } else if (caseName === "projection-mismatch") {
        yield* appendPlan(database.sqlA, seeded, "projection-mismatch", { project: false });
      } else if (caseName === "damaged") {
        const versionRows = yield* database.sqlA<{ readonly version: number }>`
          SELECT MAX(stream_version) AS version FROM orchestration_events
          WHERE aggregate_kind = 'thread' AND stream_id = ${seeded.evidence.threadId}
        `;
        const version = versionRows[0]!.version + 1;
        const eventId = EventId.make("provider-event-damaged-plan");
        const commandId = CommandId.make("provider:damaged-plan");
        yield* database.sqlA`
          INSERT INTO orchestration_events (
            event_id, aggregate_kind, stream_id, stream_version, event_type,
            occurred_at, command_id, causation_event_id, correlation_id,
            actor_kind, payload_json, metadata_json
          ) VALUES (
            ${eventId}, 'thread', ${seeded.evidence.threadId}, ${version},
            'thread.proposed-plan-upserted', ${providerAcceptedAt}, ${commandId}, NULL,
            ${commandId}, 'provider', ${"{} "}, '{}'
          )
        `;
      }
      yield* appendProviderTerminal(database.sqlA, seeded, `plan-${caseName}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      if (caseName === "missing") {
        assert.isTrue(Exit.isSuccess(exit));
        if (Exit.isSuccess(exit)) assert.equal(exit.value._tag, "Started");
      } else {
        assert.isTrue(Exit.isFailure(exit));
      }
      const counts = yield* finalizationCounts(database.sqlA, seeded);
      assert.equal(counts.stageEvents, caseName === "missing" ? 2 : 1);
      assert.equal(counts.leaseEvents, 1);
      assert.equal(counts.evidence, 0);
      assert.equal(counts.receipts, 0);
      assert.equal(counts.markers, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{
  readonly mismatch: "task" | "stageRun" | "attempt" | "lease" | "holder" | "fence";
}>([
  { mismatch: "task" },
  { mismatch: "stageRun" },
  { mismatch: "attempt" },
  { mismatch: "lease" },
  { mismatch: "holder" },
  { mismatch: "fence" },
])("foreign $mismatch identity cannot mutate either aggregate", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(
        database.sqlA,
        harness,
        `identity-${mismatch}`,
        "provider-started",
        mismatch,
      );
      yield* appendProviderStart(database.sqlA, seeded, `identity-${mismatch}`);
      yield* appendPlan(database.sqlA, seeded, `identity-${mismatch}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `identity-${mismatch}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
    }),
  ),
);

it.effect.each<{ readonly mismatch: "handoff" | "reservation" | "thread" | "delivery" }>([
  { mismatch: "handoff" },
  { mismatch: "reservation" },
  { mismatch: "thread" },
  { mismatch: "delivery" },
])("the accepted-handoff authority rejects foreign $mismatch identity", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const exit = yield* Effect.exit(
        seedPlanning(database.sqlA, harness, `handoff-${mismatch}`, "provider-started", mismatch),
      );
      assert.isTrue(Exit.isFailure(exit));
      const writes = yield* database.sqlA<{
        readonly started: number;
        readonly evidence: number;
        readonly receipts: number;
        readonly markers: number;
      }>`
        SELECT
          (SELECT COUNT(*) FROM agent_control_initial_planning_stage_started) AS started,
          (SELECT COUNT(*) FROM agent_control_initial_planning_result_evidence) AS evidence,
          (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_receipts) AS receipts,
          (SELECT COUNT(*) FROM agent_control_initial_planning_finalization_markers) AS markers
      `;
      assert.deepStrictEqual(writes[0], { started: 0, evidence: 0, receipts: 0, markers: 0 });
    }),
  ),
);

it.effect.each<{ readonly mismatch: "provider" | "providerTurn" }>([
  { mismatch: "provider" },
  { mismatch: "providerTurn" },
])("foreign $mismatch orchestration evidence is ignored without mutation", ({ mismatch }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `orchestration-${mismatch}`);
      const foreignTurn = TurnId.make(`foreign-turn-${mismatch}`);
      yield* appendOrchestration(database.sqlA, {
        suffix: `foreign-${mismatch}`,
        threadId: seeded.evidence.threadId,
        type: "thread.session-set",
        occurredAt: providerAcceptedAt,
        payload: {
          session: {
            activeTurnId: mismatch === "providerTurn" ? foreignTurn : seeded.providerTurnId,
            lastError: null,
            providerInstanceId:
              mismatch === "provider"
                ? ProviderInstanceId.make("foreign-provider")
                : seeded.evidence.providerInstanceId,
            providerName: "codex",
            runtimeMode: seeded.evidence.runtimeMode,
            status: "running",
            threadId: seeded.evidence.threadId,
            updatedAt: providerAcceptedAt,
          },
          threadId: seeded.evidence.threadId,
        },
      });
      const result = yield* harness.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(result._tag, "Waiting");
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 1,
        leaseEvents: 1,
        started: 0,
        evidence: 0,
        receipts: 0,
        markers: 0,
      });
    }),
  ),
);

it.effect.each<{
  readonly corruption: "stage-projection" | "stage-history" | "lease-projection" | "lease-history";
}>([
  { corruption: "stage-projection" },
  { corruption: "stage-history" },
  { corruption: "lease-projection" },
  { corruption: "lease-history" },
])("$corruption prevents every finalization write and publication", ({ corruption }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harness = yield* buildFinalizer(database.sqlA, database.scopeA);
      const seeded = yield* seedPlanning(database.sqlA, harness, `corrupt-${corruption}`);
      yield* appendProviderStart(database.sqlA, seeded, `corrupt-${corruption}`);
      yield* appendPlan(database.sqlA, seeded, `corrupt-${corruption}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `corrupt-${corruption}`, "completed");
      yield* markTerminal(harness.store, seeded, "completed");
      if (corruption === "stage-projection") {
        yield* database.sqlA`
          UPDATE agent_control_stage_run_states SET status = 'running'
          WHERE stage_run_id = ${seeded.stageRunId}
        `;
      } else if (corruption === "stage-history") {
        yield* database.sqlA`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE aggregate_kind = 'stage-run' AND stream_id = ${seeded.stageRunId}
        `;
      } else if (corruption === "lease-projection") {
        yield* database.sqlA`
          UPDATE agent_control_stage_run_lease_states SET fence_token = 2
          WHERE lease_id = ${seeded.leaseId}
        `;
      } else {
        yield* database.sqlA`
          UPDATE agent_control_events SET payload_json = '{}'
          WHERE aggregate_kind = 'stage-run-lease' AND stream_id = ${seeded.leaseId}
        `;
      }
      const exit = yield* Effect.exit(harness.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(exit));
      const counts = yield* finalizationCounts(database.sqlA, seeded);
      assert.equal(counts.stageEvents, 1);
      assert.equal(counts.leaseEvents, 1);
      assert.equal(counts.started, 0);
      assert.equal(counts.evidence, 0);
      assert.equal(counts.receipts, 0);
      assert.equal(counts.markers, 0);
      assert.equal((yield* Ref.get(harness.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harness.leasePublished)).length, 0);
    }),
  ),
);

it.effect.each<{ readonly phase: "defect" | "interrupt" }>([
  { phase: "defect" },
  { phase: "interrupt" },
])("$phase after native commit replays complete evidence on a fresh layer", ({ phase }) =>
  withNode(
    Effect.gen(function* () {
      const database = yield* makeSharedDatabase();
      const harnessA = yield* buildFinalizer(database.sqlA, database.scopeA, {
        ...noopHooks,
        afterNativeCommit: () =>
          phase === "defect" ? Effect.die(new Error("response-loss")) : Effect.interrupt,
      });
      const seeded = yield* seedPlanning(database.sqlA, harnessA, `after-${phase}`);
      yield* appendProviderStart(database.sqlA, seeded, `after-${phase}`);
      yield* appendPlan(database.sqlA, seeded, `after-${phase}`);
      yield* appendProviderTerminal(database.sqlA, seeded, `after-${phase}`, "completed");
      yield* markTerminal(harnessA.store, seeded, "completed");
      const lost = yield* Effect.exit(harnessA.finalizer.processHandoff(seeded.evidence.handoffId));
      assert.isTrue(Exit.isFailure(lost));
      assert.deepStrictEqual(yield* finalizationCounts(database.sqlA, seeded), {
        stageEvents: 3,
        leaseEvents: 2,
        started: 1,
        evidence: 1,
        receipts: 1,
        markers: 1,
      });
      assert.equal((yield* Ref.get(harnessA.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessA.leasePublished)).length, 0);

      const harnessB = yield* buildFinalizer(database.sqlB, database.scopeB);
      const replayed = yield* harnessB.finalizer.processHandoff(seeded.evidence.handoffId);
      assert.equal(replayed._tag, "Replayed");
      assert.equal((yield* Ref.get(harnessB.stagePublished)).length, 0);
      assert.equal((yield* Ref.get(harnessB.leasePublished)).length, 0);
    }),
  ),
);
