import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  EventId,
  type ProviderSession,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";

import {
  DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS,
  providerAdmissionUsageEvidence,
  type ProviderAdmissionPermit,
  type ProviderAdmissionStage,
} from "../../agentControl/providerAdmission/model.ts";
import { ProviderAdmissionStoreLive } from "../../agentControl/providerAdmission/Layers/ProviderAdmissionStore.ts";
import { ProviderAdmissionStore } from "../../agentControl/providerAdmission/Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionRuntime } from "../../agentControl/providerAdmission/Services/ProviderAdmissionRuntime.ts";
import {
  ProviderResourceCoordinator,
  layer as coordinatorLayer,
} from "../../resourceAdmission/ProviderResourceCoordinator.ts";
import { makeMemoryHostBudgetLedger } from "../../resourceAdmission/HostBudgetLedger.ts";
import {
  ResourceAdmission,
  make as makeHostAdmission,
} from "../../resourceAdmission/ResourceAdmission.ts";
import { ResourcePressure } from "../../resourceAdmission/ResourcePressure.ts";
import { layerTest as settingsTest } from "../../serverSettings.ts";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { ProviderAdapterRequestError } from "../../provider/Errors.ts";
import {
  attestProviderSessionNativeConfiguration,
  canonicalProviderModelSelectionEvidence,
} from "../../provider/Services/ProviderAdapter.ts";
import { ProviderRegistry } from "../../provider/Services/ProviderRegistry.ts";
import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import {
  ProviderTurnDeliveryError,
  ProviderTurnRequestExecutor,
  type PreparedProviderTurnRequest,
} from "../Services/ProviderTurnRequestExecutor.ts";
import {
  buildInitialPlanningSessionEvidence,
  isInitialPlanningSessionEvidenceRow,
  mapProviderTurnDeliveryCause,
  ProviderTurnRequestExecutorLive,
} from "./ProviderTurnRequestExecutor.ts";

const createdAt = "2026-09-06T10:00:00.000Z";
const provider = ProviderDriverKind.make("codex");
const providerInstanceId = ProviderInstanceId.make("codex-executor-quarantine");
const projectId = ProjectId.make("project-executor-quarantine");
const threadId = ThreadId.make("thread-executor-quarantine");
const modelSelection = createModelSelection(providerInstanceId, "gpt-5.4");
const modelEvidence = canonicalProviderModelSelectionEvidence(modelSelection);

const permit: ProviderAdmissionPermit = {
  admissionId: "provider-admission:executor-quarantine",
  admissionMarkerId: "provider-admission-marker:executor-quarantine",
  admissionMarkerFingerprint: "a".repeat(64),
  stage: "initial-planning",
  projectId: String(projectId),
  taskId: "task-executor-quarantine",
  stageRunId: "stage-executor-quarantine",
  attemptId: "attempt-executor-quarantine",
  handoffId: "handoff-executor-quarantine",
  providerDeliveryId: "delivery-executor-quarantine",
  threadId: String(threadId),
  providerInstanceId,
  stageLeaseId: "lease-executor-quarantine",
  stageLeaseHolderId: "holder-executor-quarantine",
  stageFenceToken: 1,
  admissionOwnerId: "owner-executor-quarantine",
  admissionLeaseExpiresAt: "2099-09-06T10:00:00.000Z",
  providerFenceToken: 1,
  modelSelectionJson: modelEvidence.modelSelectionJson,
  modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
  usageEvidenceFingerprint: "b".repeat(64),
};

const thread = {
  id: threadId,
  projectId,
  worktreePath: "/tmp/executor-quarantine",
  runtimeMode: "approval-required",
  modelSelection,
  session: null,
};

const session = attestProviderSessionNativeConfiguration(
  {
    provider,
    providerInstanceId,
    status: "ready",
    runtimeMode: "approval-required",
    threadId,
    resumeCursor: null,
    cwd: "/tmp/executor-quarantine",
    model: modelSelection.model,
    createdAt,
    updatedAt: createdAt,
  } satisfies ProviderSession,
  modelSelection,
);

const snapshots = ProjectionSnapshotQuery.of({
  getThreadDetailById: () => Effect.succeed(Option.some(thread as never)),
  getProjectShellById: () => Effect.succeed(Option.none()),
} as unknown as ProjectionSnapshotQuery["Service"]);

const providerRegistry = ProviderRegistry.of({
  refreshWorkspaceSnapshot: () => Effect.succeed([]),
  getProviders: Effect.succeed([]),
  refresh: () => Effect.succeed([]),
  refreshInstance: () => Effect.succeed([]),
  getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
  setProviderMaintenanceActionState: () => Effect.succeed([]),
  streamChanges: Stream.empty,
});

const buildExecutor = (
  scope: Scope.Closeable,
  sql: SqlClient.SqlClient,
  providerService: ProviderService["Service"],
  dispatch: OrchestrationEngineService["Service"]["dispatch"],
  resourceCoordinator?: ProviderResourceCoordinator["Service"],
) =>
  Layer.buildWithScope(
    Layer.fresh(ProviderTurnRequestExecutorLive).pipe(
      Layer.provide(
        Layer.mergeAll(
          resourceCoordinator === undefined
            ? Layer.empty
            : Layer.succeed(ProviderResourceCoordinator, resourceCoordinator),
          Layer.succeed(SqlClient.SqlClient, sql),
          Layer.succeed(ProviderService, providerService),
          Layer.succeed(ProviderRegistry, providerRegistry),
          Layer.succeed(ProjectionSnapshotQuery, snapshots),
          Layer.succeed(
            OrchestrationEngineService,
            OrchestrationEngineService.of({
              readThreadEvents: () => Stream.empty,
              getThreadReplayStats: () => Effect.die("Unexpected getThreadReplayStats"),
              dispatch,
              readEvents: () => Stream.empty,
              dispatchClient: () => Effect.die("unused"),
              dispatchAgentControl: () => Effect.die("unused"),
              streamDomainEvents: Stream.empty,
              subscribeDomainEvents: Effect.succeed(Stream.empty),
              latestSequence: Effect.succeed(0),
            }),
          ),
        ),
      ),
      Layer.provideMerge(NodeServices.layer),
    ),
    scope,
  );

const makeProvider = (input: {
  readonly startSession: ProviderService["Service"]["startSession"];
  readonly listSessions: ProviderService["Service"]["listSessions"];
  readonly quarantineAdmissionIfEntered: NonNullable<
    ProviderService["Service"]["quarantineAdmissionIfEntered"]
  >;
}): ProviderService["Service"] =>
  ProviderService.of({
    compactThread: () => Effect.die("Unexpected compactThread"),
    assertConversationRollbackSupported: () => Effect.void,
    uploadFeedback: () => Effect.die("Unexpected uploadFeedback"),
    startSession: input.startSession,
    sendTurn: () => Effect.die("unused"),
    interruptTurn: () => Effect.die("unused"),
    respondToRequest: () => Effect.die("unused"),
    respondToUserInput: () => Effect.die("unused"),
    stopSession: () => Effect.die("unused"),
    listSessions: input.listSessions,
    getCapabilities: () => Effect.succeed({ sessionModelSwitch: "in-session" }),
    getInstanceInfo: () =>
      Effect.succeed({
        instanceId: providerInstanceId,
        driverKind: provider,
        displayName: undefined,
        enabled: true,
        continuationIdentity: {
          driverKind: provider,
          continuationKey: `codex:${providerInstanceId}`,
        },
      }),
    getSessionAttestation: () => Effect.succeed(session.initialPlanningAttestation),
    quarantineAdmissionIfEntered: input.quarantineAdmissionIfEntered,
    rollbackConversation: () => Effect.die("unused"),
    streamEvents: Stream.empty,
  });

it.effect("binds a session using its observation time after a slow provider start", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const sqlContext = yield* Layer.buildWithScope(
        SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
        scope,
      );
      const observedAt = "2026-09-06T10:00:02.000Z";
      const delayedSession = { ...session, updatedAt: observedAt };
      const providerService = makeProvider({
        startSession: () => Effect.succeed(delayedSession),
        listSessions: () => Effect.succeed([]),
        quarantineAdmissionIfEntered: () => Effect.die("Unexpected quarantine"),
      });
      let bound = false;
      const context = yield* buildExecutor(
        scope,
        Context.get(sqlContext, SqlClient.SqlClient),
        providerService,
        (command) =>
          Effect.sync(() => {
            assert.equal(command.type, "thread.session.set");
            if (command.type === "thread.session.set") {
              assert.equal(command.createdAt, observedAt);
              assert.equal(command.session.updatedAt, command.createdAt);
              assert.notEqual(command.createdAt, createdAt);
              bound = true;
            }
            return { sequence: 1 };
          }),
      );
      yield* Context.get(context, ProviderTurnRequestExecutor).ensureSessionForThread(
        threadId,
        createdAt,
        { modelSelection },
      );
      assert.isTrue(bound);
    }),
  ),
);

it.effect(
  "quarantines a committed session entry when thread binding fails and keeps both causes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const sqlContext = yield* Layer.buildWithScope(
          SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
          scope,
        );
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        const bindingDefect = new Error("thread binding failed");
        const quarantineDefect = new Error("quarantine persistence failed");
        let quarantineCalls = 0;
        const providerService = makeProvider({
          startSession: () => Effect.succeed(session),
          listSessions: () => Effect.succeed([]),
          quarantineAdmissionIfEntered: () =>
            Effect.sync(() => {
              quarantineCalls += 1;
            }).pipe(Effect.andThen(Effect.die(quarantineDefect))),
        });
        const context = yield* buildExecutor(scope, sql, providerService, () =>
          Effect.die(bindingDefect),
        );
        const executor = Context.get(context, ProviderTurnRequestExecutor);

        const automated = yield* Effect.exit(
          executor.ensureSessionForThread(threadId, createdAt, {
            modelSelection,
            providerAdmissionPermit: permit,
          }),
        );
        assert.isTrue(Exit.isFailure(automated));
        if (Exit.isFailure(automated)) {
          assert.isTrue(
            automated.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === bindingDefect,
            ),
          );
          assert.isTrue(
            automated.cause.reasons.some(
              (reason) => Cause.isDieReason(reason) && reason.defect === quarantineDefect,
            ),
          );
        }
        assert.equal(quarantineCalls, 1);

        const human = yield* Effect.exit(
          executor.ensureSessionForThread(threadId, createdAt, { modelSelection }),
        );
        assert.isTrue(Exit.isFailure(human));
        assert.equal(quarantineCalls, 1);
      }),
    ),
);

it.effect("does not invoke a turn when atomic session evidence persistence fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const scope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const sqlContext = yield* Layer.buildWithScope(
        SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
        scope,
      );
      const sql = Context.get(sqlContext, SqlClient.SqlClient);
      let started = false;
      let quarantineCalls = 0;
      const providerService = makeProvider({
        startSession: () =>
          Effect.gen(function* () {
            started = true;
            yield* sql.unsafe("DROP TABLE main.agent_control_initial_planning_session_evidence")
              .unprepared;
            return session;
          }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider,
                  method: "thread.turn.start",
                  detail: "Test fixture could not invalidate session evidence storage.",
                  cause,
                }),
            ),
          ),
        listSessions: () => Effect.succeed(started ? [session] : []),
        quarantineAdmissionIfEntered: () =>
          Effect.sync(() => {
            quarantineCalls += 1;
          }),
      });
      const context = yield* buildExecutor(
        scope,
        sql,
        ProviderService.of({
          ...providerService,
          sendTurnAtPreInvokeBoundary: (_input, boundary) =>
            Effect.gen(function* () {
              yield* boundary.beforeDeliveryCas();
              yield* boundary.persistDeliveryAttempted({
                ...modelEvidence,
                providerInstanceId,
                effectiveModelSelection: modelSelection,
              });
              return yield* Effect.die("Failed evidence must not reach adapter");
            }),
        }),
        () => Effect.succeed({ sequence: 1 }),
      );
      const executor = Context.get(context, ProviderTurnRequestExecutor);

      const prepared = yield* executor.prepareTurnDelivery({
        threadId,
        messageText: "persist session evidence",
        attachments: [],
        modelSelection,
        interactionMode: "plan",
        createdAt,
        providerDeliveryId: permit.providerDeliveryId,
        durableDeliveryKind: "initial-planning",
        providerAdmissionPermit: permit,
      });
      const result = yield* Effect.exit(
        executor.sendPreparedTurnAtPreInvokeBoundary(prepared, {
          claimGeneration: 1,
          beforeDeliveryCas: () => Effect.void,
          persistDeliveryAttempted: () =>
            Effect.die("Missing evidence storage must prevent delivery CAS"),
          afterDeliveryCas: () => Effect.die("Missing evidence storage must prevent invoke"),
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.equal(quarantineCalls, 0);
    }),
  ),
);

it.effect(
  "keeps prepared sessions replaceable until the delivery CAS and rolls evidence back with a rejected CAS",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const scope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
        const sqlContext = yield* Layer.buildWithScope(
          SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
          scope,
        );
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        // Seed durable authority directly; this test exercises executor/session transaction ownership.
        yield* sql`PRAGMA foreign_keys = OFF`;
        yield* sql`DROP TRIGGER agent_control_initial_planning_delivery_insert_validate`;
        yield* sql`DROP TRIGGER agent_control_initial_planning_session_evidence_validate`;
        yield* sql.withTransaction(sql`
      INSERT INTO agent_control_initial_planning_deliveries (
        provider_delivery_id, handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
        thread_id, turn_request_command_id, message_id, provider_instance_id,
        state, revision, claim_owner_id, claim_generation, claim_expires_at,
        attempt_count, planning_deadline_at, interrupt_requested, updated_at
      ) VALUES (${permit.providerDeliveryId}, 'handoff', ${"a".repeat(64)}, 'reservation',
        ${threadId}, 'turn-command', 'message', ${providerInstanceId}, 'claimed', 1,
        'claim-owner', 1, '2099-09-06T10:00:00.000Z', 1, '2099-09-06T10:00:00.000Z', 0, ${createdAt})
    `);
        let current = session;
        let started = false;
        const service = makeProvider({
          startSession: () =>
            Effect.sync(() => {
              started = true;
              return current;
            }),
          listSessions: () => Effect.succeed(started ? [current] : []),
          quarantineAdmissionIfEntered: () => Effect.void,
        });
        const providerService = ProviderService.of({
          ...service,
          getSessionAttestation: () => Effect.succeed(current.initialPlanningAttestation),
          sendTurnAtPreInvokeBoundary: (_input, boundary) =>
            Effect.gen(function* () {
              yield* boundary.beforeDeliveryCas();
              yield* boundary.persistDeliveryAttempted({
                ...modelEvidence,
                providerInstanceId,
                effectiveModelSelection: modelSelection,
              });
              yield* boundary.afterDeliveryCas();
              return {} as never;
            }),
        });
        const context = yield* buildExecutor(scope, sql, providerService, () =>
          Effect.succeed({ sequence: 1 }),
        );
        const executor = Context.get(context, ProviderTurnRequestExecutor);
        const input = {
          threadId,
          messageText: "prepared restart",
          modelSelection,
          createdAt,
          providerDeliveryId: permit.providerDeliveryId,
          durableDeliveryKind: "initial-planning" as const,
          providerAdmissionPermit: permit,
        };
        yield* executor.prepareTurnDelivery(input);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_initial_planning_session_evidence`,
          [],
        );
        // A process restart may resume an empty native thread by legitimately starting fresh.
        started = false;
        current = attestProviderSessionNativeConfiguration(
          {
            ...session,
            createdAt: "2026-09-06T10:03:00.000Z",
            resumeCursor: { threadId: "fresh-native-thread" },
          },
          modelSelection,
        );
        const prepared = yield* executor.prepareTurnDelivery(input);
        const rejected = yield* Effect.exit(
          executor.sendPreparedTurnAtPreInvokeBoundary(prepared, {
            claimGeneration: 1,
            beforeDeliveryCas: () => Effect.void,
            persistDeliveryAttempted: () =>
              Effect.gen(function* () {
                const rows =
                  yield* sql`SELECT session_created_at FROM agent_control_initial_planning_session_evidence`;
                assert.deepStrictEqual(rows, [{ session_created_at: current.createdAt }]);
                return yield* new ProviderAdapterRequestError({
                  provider,
                  method: "thread.turn.start",
                  detail: "stale delivery claim",
                });
              }).pipe(
                Effect.mapError(
                  () =>
                    new ProviderAdapterRequestError({
                      provider,
                      method: "thread.turn.start",
                      detail: "stale delivery claim",
                    }),
                ),
              ),
            afterDeliveryCas: () => Effect.die("Rejected CAS must not reach adapter"),
          }),
        );
        assert.isTrue(Exit.isFailure(rejected));
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM agent_control_initial_planning_session_evidence`,
          [],
        );
        yield* executor.sendPreparedTurnAtPreInvokeBoundary(prepared, {
          claimGeneration: 1,
          beforeDeliveryCas: () => Effect.void,
          persistDeliveryAttempted: () => Effect.void,
          afterDeliveryCas: () => Effect.void,
        });
        assert.deepStrictEqual(
          yield* sql`SELECT session_created_at FROM agent_control_initial_planning_session_evidence`,
          [{ session_created_at: current.createdAt }],
        );
        // Once delivery evidence has committed, a changed runtime must still fail closed.
        current = session;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(executor.prepareTurnDelivery(input))));
      }),
    ),
);

class SemanticAnnotation extends Context.Service<SemanticAnnotation, { readonly value: string }>()(
  "t3/orchestration/Layers/ProviderTurnRequestExecutor.test/SemanticAnnotation",
) {}

function assertCauseAnnotationsEqual(
  actual: Cause.Reason<unknown>,
  expected: Cause.Reason<unknown>,
): void {
  const actualAnnotations = new Map(actual.annotations);
  const expectedAnnotations = new Map(expected.annotations);
  actualAnnotations.delete(Cause.StackTrace.key);
  expectedAnnotations.delete(Cause.StackTrace.key);
  assert.deepStrictEqual(actualAnnotations, expectedAnnotations);
}

const evidenceSelection = {
  instanceId: ProviderInstanceId.make("codex-primary"),
  model: "gpt-5.4",
  options: [
    { id: "reasoningEffort", value: "high" },
    { id: "fastMode", value: false },
    { id: "thinking", value: "adaptive" },
  ],
} as const;
const sessionModelEvidence = canonicalProviderModelSelectionEvidence(evidenceSelection);
const sessionEvidenceAttestation = {
  threadId: ThreadId.make("thread-session-evidence"),
  providerInstanceId: evidenceSelection.instanceId,
  runtimeMode: "approval-required" as const,
  cwd: "/tmp/attested-worktree",
  ...sessionModelEvidence,
  sessionCreatedAt: "2026-07-30T12:00:00.000Z",
  resumeCursor: { threadId: "provider-thread" },
};

describe("Initial Planning session evidence", () => {
  it("accepts only the complete attested row and recomputed ModelSelection fingerprint", () => {
    const expected = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation: sessionEvidenceAttestation,
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    assert.isTrue(isInitialPlanningSessionEvidenceRow(expected, expected));

    for (const [field, value] of [
      ["providerDeliveryId", "different-delivery"],
      ["threadId", "different-thread"],
      ["providerInstanceId", "different-provider"],
      ["runtimeMode", "full-access"],
      ["cwd", "/tmp/different-worktree"],
      ["modelSelectionJson", '{"instanceId":"codex-primary","model":"different"}'],
      ["modelSelectionFingerprint", "f".repeat(64)],
      ["sessionCreatedAt", "2026-07-30T12:00:00.001Z"],
      ["resumeCursorJson", '{"threadId":"different-provider-thread"}'],
    ] as const) {
      assert.isFalse(
        isInitialPlanningSessionEvidenceRow({ ...expected, [field]: value }, expected),
        field,
      );
    }
  });

  it("normalizes key and option order but changes evidence for every effective option", () => {
    const reordered = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation: {
        ...sessionEvidenceAttestation,
        ...canonicalProviderModelSelectionEvidence({
          model: evidenceSelection.model,
          options: [
            { value: "adaptive", id: "thinking" },
            { value: false, id: "fastMode" },
            { value: "high", id: "reasoningEffort" },
          ],
          instanceId: evidenceSelection.instanceId,
        }),
      },
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    const expected = buildInitialPlanningSessionEvidence({
      providerDeliveryId: "delivery-session-evidence",
      attestation: sessionEvidenceAttestation,
      resumeCursorJson: '{"threadId":"provider-thread"}',
    });
    assert.deepStrictEqual(reordered, expected);

    for (const effectiveModelSelection of [
      {
        ...evidenceSelection,
        options: [
          { id: "reasoningEffort", value: "xhigh" },
          { id: "fastMode", value: false },
          { id: "thinking", value: "adaptive" },
        ],
      },
      {
        ...evidenceSelection,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: true },
          { id: "thinking", value: "adaptive" },
        ],
      },
      {
        ...evidenceSelection,
        options: [
          { id: "reasoningEffort", value: "high" },
          { id: "fastMode", value: false },
          { id: "thinking", value: "disabled" },
        ],
      },
    ] as const) {
      const changed = buildInitialPlanningSessionEvidence({
        providerDeliveryId: "delivery-session-evidence",
        attestation: {
          ...sessionEvidenceAttestation,
          ...canonicalProviderModelSelectionEvidence(effectiveModelSelection),
        },
        resumeCursorJson: '{"threadId":"provider-thread"}',
      });
      assert.notEqual(changed.modelSelectionFingerprint, expected.modelSelectionFingerprint);
      assert.isFalse(isInitialPlanningSessionEvidenceRow(changed, expected));
    }
  });
});

describe("Initial Planning delivery Cause mapping", () => {
  it("maps every Failure while preserving a combined Defect and Interrupt", () => {
    const providerFailure = new ProviderAdapterRequestError({
      provider: "cursor",
      method: "session/prompt",
      detail: "combined executor failure",
    });
    const defect = new Error("combined executor defect");
    const annotations = Context.make(SemanticAnnotation, { value: "preserved" });
    const failureReason = Cause.makeFailReason(providerFailure).annotate(annotations);
    const defectReason = Cause.makeDieReason(defect).annotate(annotations);
    const interruptReason = Cause.makeInterruptReason(47_002).annotate(annotations);
    const cause = Cause.fromReasons([failureReason, defectReason, interruptReason]);

    const mapped = mapProviderTurnDeliveryCause(cause, "not-attempted");

    assert.equal(mapped.reasons.length, 3);
    const mappedFailure = mapped.reasons[0]!;
    assert.isTrue(Cause.isFailReason(mappedFailure));
    if (Cause.isFailReason(mappedFailure)) {
      assert.instanceOf(mappedFailure.error, ProviderTurnDeliveryError);
      assert.strictEqual(mappedFailure.error.cause, providerFailure);
      assert.equal(mappedFailure.error.certainty, "not-attempted");
      assertCauseAnnotationsEqual(mappedFailure, failureReason);
    }
    const mappedDefect = mapped.reasons[1]!;
    const mappedInterrupt = mapped.reasons[2]!;
    assert.strictEqual(mappedDefect, defectReason);
    assert.strictEqual(mappedInterrupt, interruptReason);
    assert.isTrue(Cause.isDieReason(mappedDefect));
    if (Cause.isDieReason(mappedDefect)) {
      assert.strictEqual(mappedDefect.defect, defect);
    }
    assert.isTrue(Cause.isInterruptReason(mappedInterrupt));
    if (Cause.isInterruptReason(mappedInterrupt)) {
      assert.equal(mappedInterrupt.fiberId, 47_002);
    }
  });

  it("preserves individual Failure, Defect, and Interrupt paths", () => {
    const providerFailure = new ProviderAdapterRequestError({
      provider: "cursor",
      method: "session/prompt",
      detail: "individual executor failure",
    });
    const failure = mapProviderTurnDeliveryCause(Cause.fail(providerFailure), "acceptance-unknown");
    const failureReason = failure.reasons[0]!;
    assert.isTrue(Cause.isFailReason(failureReason));
    if (Cause.isFailReason(failureReason)) {
      assert.instanceOf(failureReason.error, ProviderTurnDeliveryError);
      assert.strictEqual(failureReason.error.cause, providerFailure);
      assert.equal(failureReason.error.certainty, "acceptance-unknown");
    }

    const defectReason = Cause.makeDieReason(new Error("individual executor defect"));
    const defect = mapProviderTurnDeliveryCause(Cause.fromReasons([defectReason]), "not-attempted");
    assert.strictEqual(defect.reasons[0], defectReason);

    const interruptReason = Cause.makeInterruptReason(47_001);
    const interrupt = mapProviderTurnDeliveryCause(
      Cause.fromReasons([interruptReason]),
      "not-attempted",
    );
    const mappedInterrupt = interrupt.reasons[0]!;
    assert.strictEqual(mappedInterrupt, interruptReason);
    assert.isTrue(Cause.isInterruptReason(mappedInterrupt));
    if (Cause.isInterruptReason(mappedInterrupt)) {
      assert.equal(mappedInterrupt.fiberId, 47_001);
    }
  });
});

const makeAdmissionRetryHarness = Effect.fn("makeAdmissionRetryHarness")(function* (
  stage: ProviderAdmissionStage,
) {
  const scope = yield* Scope.make("sequential");
  yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
  const sqlContext = yield* Layer.buildWithScope(
    SqlitePersistenceMemory.pipe(Layer.provideMerge(NodeServices.layer)),
    scope,
  );
  const sql = Context.get(sqlContext, SqlClient.SqlClient);
  // The executor owns evidence atomicity. Delivery authority is represented by
  // the provider boundary below; resource reservations use the actual stores.
  yield* sql`PRAGMA foreign_keys = OFF`;
  const tableStage = stage.replaceAll("-", "_");
  yield* sql.unsafe(`DROP TRIGGER agent_control_${tableStage}_session_evidence_validate`);
  const storeContext = yield* Layer.buildWithScope(
    ProviderAdmissionStoreLive.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, sql))),
    scope,
  );
  const store = Context.get(storeContext, ProviderAdmissionStore);
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const resources = ProviderAdmissionRuntime.of({
    awaitFailure: Effect.never,
    request: () => Effect.die("Only resource admission is used by this fixture"),
    usageChanged: () => Effect.void,
    capacityReleased: () => Effect.void,
    requestResource: (request, limits = DEFAULT_PROVIDER_RESOURCE_ADMISSION_LIMITS) =>
      Effect.gen(function* () {
        const observed = yield* now;
        return (yield* store.requestResource!({
          request,
          limits,
          usage: providerAdmissionUsageEvidence({
            providerInstanceId: request.providerInstanceId,
            status: "allowed",
            observedAt: observed,
            source: "capability",
            nextRelevantAt: null,
          }),
          ownerId: "executor-resource-owner",
          now: observed,
          leaseExpiresAt: DateTime.formatIso(
            DateTime.addDuration(yield* DateTime.now, "2 minutes"),
          ),
        })).decision;
      }),
    acquireResource: () => Effect.die("Provider scope should have free capacity"),
    enterResource: (resourcePermit, providerTurnId) =>
      now.pipe(
        Effect.flatMap((enteredAt) =>
          store.enterResource!({
            permit: resourcePermit,
            enteredAt,
            ...(providerTurnId === undefined ? {} : { providerTurnId }),
          }),
        ),
      ),
    releaseResource: (resourcePermit) =>
      now.pipe(
        Effect.flatMap((releasedAt) =>
          store.releaseResource!({ permit: resourcePermit, releasedAt }),
        ),
        Effect.asVoid,
      ),
    deferResource: (resourcePermit) =>
      now.pipe(
        Effect.flatMap((deferredAt) =>
          store.deferResource!({ permit: resourcePermit, deferredAt }),
        ),
        Effect.asVoid,
      ),
    cancelResource: (request) =>
      now.pipe(
        Effect.flatMap((cancelledAt) => store.cancelResource!({ request, cancelledAt })),
        Effect.asVoid,
      ),
    listResourceActive: store.listResourceActive!,
    reconcileResource: (requestId, observedActivity) =>
      Effect.gen(function* () {
        return yield* store.reconcileResource!({
          requestId,
          observedActivity,
          ownerId: "replacement-resource-owner",
          observedAt: yield* now,
          leaseExpiresAt: DateTime.formatIso(
            DateTime.addDuration(yield* DateTime.now, "2 minutes"),
          ),
        });
      }),
  });
  const waitStarted = yield* Deferred.make<void>();
  const pressureReleased = yield* Deferred.make<void>();
  let cpuPressure = true;
  const host = yield* makeHostAdmission({ ledger: yield* makeMemoryHostBudgetLedger() }).pipe(
    Effect.provideService(ResourcePressure, {
      sample: DateTime.now.pipe(
        Effect.map((date) => ({
          sampledAtMs: DateTime.toEpochMillis(date),
          telemetry: "available" as const,
          cpuUtilization: cpuPressure ? 0.95 : 0.1,
          availableMemoryBytes: 8 * 1024 ** 3,
          gpu: { status: "unavailable" as const },
        })),
      ),
      awaitChange: () =>
        Deferred.succeed(waitStarted, undefined).pipe(
          Effect.andThen(Deferred.await(pressureReleased)),
        ),
    }),
  );
  const newCoordinator = () =>
    Layer.buildWithScope(
      Layer.fresh(coordinatorLayer).pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(ResourceAdmission, host),
            Layer.succeed(ProviderAdmissionRuntime, resources),
            settingsTest(),
          ),
        ),
      ),
      scope,
    ).pipe(Effect.map((context) => Context.get(context, ProviderResourceCoordinator)));
  const coordinator = yield* newCoordinator();
  let nativeInvocations = 0;
  let uncertain = false;
  const service = ProviderService.of({
    ...makeProvider({
      startSession: () => Effect.succeed(session),
      listSessions: () => Effect.succeed([session]),
      quarantineAdmissionIfEntered: () => Effect.void,
    }),
    sendTurnAtPreInvokeBoundary: (_input, boundary) =>
      Effect.gen(function* () {
        // Model the existing exact entry deadline guard; production guard tests
        // independently exercise its owner, fence, lease, and stage authority.
        if (boundary.providerAdmissionPermit.admissionLeaseExpiresAt <= (yield* now))
          return yield* new ProviderAdapterRequestError({
            provider,
            method: "thread.turn.start",
            detail: "Expired entry authority",
          });
        yield* boundary.beforeDeliveryCas();
        yield* boundary.persistDeliveryAttempted({
          ...modelEvidence,
          providerInstanceId,
          effectiveModelSelection: modelSelection,
        });
        yield* boundary.afterDeliveryCas();
        boundary.onNativeInvocationStarted?.();
        nativeInvocations += 1;
        if (uncertain)
          return yield* new ProviderAdapterRequestError({
            provider,
            method: "thread.turn.start",
            detail: "Native acceptance unknown",
          });
        return { threadId, turnId: TurnId.make("retry-native-turn") };
      }),
  });
  const newExecutor = (resourceCoordinator = coordinator) =>
    buildExecutor(
      scope,
      sql,
      service,
      () => Effect.succeed({ sequence: 1 }),
      resourceCoordinator,
    ).pipe(Effect.map((context) => Context.get(context, ProviderTurnRequestExecutor)));
  const executor = yield* newExecutor();
  const sessionAttestation = session.initialPlanningAttestation;
  if (sessionAttestation === undefined) return yield* Effect.die("Missing fixture attestation");
  const prepare = Effect.gen(function* (): Effect.gen.Return<PreparedProviderTurnRequest> {
    const observed = yield* now;
    return {
      input: { threadId, input: "verify admission retry", modelSelection },
      providerDeliveryId: permit.providerDeliveryId,
      durableDeliveryKind: stage,
      providerAdmissionPermit: {
        ...permit,
        stage,
        admissionLeaseExpiresAt: DateTime.formatIso(
          DateTime.addDuration(yield* DateTime.now, "2 minutes"),
        ),
      },
      sessionAttestation,
      sessionResumeCursorJson: "null",
      sessionEvidenceRecordedAt: observed,
    };
  });
  const boundary = (claimGeneration: number) => ({
    claimGeneration,
    beforeDeliveryCas: () => Effect.void,
    persistDeliveryAttempted: () => Effect.void,
    afterDeliveryCas: () => Effect.void,
  });
  const unblock = Effect.sync(() => {
    cpuPressure = false;
  }).pipe(Effect.andThen(Deferred.succeed(pressureReleased, undefined)), Effect.asVoid);
  return {
    sql,
    host,
    coordinator,
    executor,
    newExecutor,
    newCoordinator,
    prepare,
    boundary,
    waitStarted,
    unblock,
    nativeInvocations: () => nativeInvocations,
    makeUncertain: () => {
      uncertain = true;
    },
  };
});

for (const stage of ["initial-planning", "implementation", "verification"] as const) {
  it.effect(
    `retries ${stage} once under a new claim after resource waiting expires entry authority`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const h = yield* makeAdmissionRetryHarness(stage);
          const prepared = yield* h.prepare;
          const first = yield* h.executor
            .sendPreparedTurnAtPreInvokeBoundary(prepared, h.boundary(1))
            .pipe(Effect.exit, Effect.forkChild);
          yield* Deferred.await(h.waitStarted);
          yield* TestClock.adjust("121 seconds");
          yield* h.unblock;
          const failed = yield* Fiber.join(first);
          assert.isTrue(Exit.isFailure(failed));
          if (Exit.isFailure(failed))
            assert.equal(
              failed.cause.reasons.find(Cause.isFailReason)?.error.certainty,
              "not-attempted",
            );
          assert.equal(h.nativeInvocations(), 0);
          assert.deepStrictEqual(
            yield* h.sql`SELECT status FROM resource_admission_provider_requests`,
            [{ status: "released" }],
          );
          // A replay of the failed claim cannot revive its terminal reservations.
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                h.executor.sendPreparedTurnAtPreInvokeBoundary(yield* h.prepare, h.boundary(1)),
              ),
            ),
          );
          assert.equal(h.nativeInvocations(), 0);
          // Reconstruct runtime layers over the same stores, as after process recovery.
          const replacement = yield* h.newCoordinator();
          yield* replacement.reconcile([]);
          const executor = yield* h.newExecutor(replacement);
          const result = yield* executor.sendPreparedTurnAtPreInvokeBoundary(
            yield* h.prepare,
            h.boundary(2),
          );
          assert.equal(result.certainty, "accepted");
          assert.equal(h.nativeInvocations(), 1);
          assert.deepStrictEqual(
            yield* h.sql`SELECT status FROM resource_admission_provider_requests ORDER BY requested_at`,
            [{ status: "released" }, { status: "entered" }],
          );
          yield* replacement.observeRuntimeEvent({
            type: "turn.completed",
            eventId: EventId.make("retry-completed"),
            provider,
            providerInstanceId,
            threadId,
            turnId: result.result.turnId,
            createdAt,
            payload: { state: "completed" },
          });
          assert.equal(
            (yield* h.host.snapshot).entries.filter((entry) => entry.state === "admitted").length,
            0,
          );
        }),
      ),
  );
}

it.effect("cancels a resource wait without invocation and lets a new durable claim retry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeAdmissionRetryHarness("verification");
      const running = yield* h.executor
        .sendPreparedTurnAtPreInvokeBoundary(yield* h.prepare, h.boundary(1))
        .pipe(Effect.forkChild);
      yield* Deferred.await(h.waitStarted);
      yield* Fiber.interrupt(running);
      assert.equal(h.nativeInvocations(), 0);
      assert.equal((yield* h.host.snapshot).entries[0]?.state, "canceled");
      yield* h.unblock;
      yield* h.executor.sendPreparedTurnAtPreInvokeBoundary(yield* h.prepare, h.boundary(2));
      assert.equal(h.nativeInvocations(), 1);
    }),
  ),
);

it.effect("retains ambiguous native invocation capacity across coordinator recovery", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const h = yield* makeAdmissionRetryHarness("verification");
      yield* h.unblock;
      h.makeUncertain();
      const result = yield* Effect.exit(
        h.executor.sendPreparedTurnAtPreInvokeBoundary(yield* h.prepare, h.boundary(1)),
      );
      assert.isTrue(Exit.isFailure(result));
      if (Exit.isFailure(result))
        assert.equal(
          result.cause.reasons.find(Cause.isFailReason)?.error.certainty,
          "acceptance-unknown",
        );
      assert.equal(h.nativeInvocations(), 1);
      const replacement = yield* h.newCoordinator();
      yield* replacement.reconcile([]);
      assert.deepStrictEqual(
        yield* h.sql`SELECT status,last_observed_activity FROM resource_admission_provider_requests`,
        [{ status: "entered", last_observed_activity: "unknown" }],
      );
      assert.equal(
        (yield* h.host.snapshot).entries.filter((entry) => entry.state === "admitted").length,
        1,
      );
      assert.equal(h.nativeInvocations(), 1);
    }),
  ),
);
