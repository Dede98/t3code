import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderSession,
} from "@t3tools/contracts";
import { createModelSelection } from "@t3tools/shared/model";
import { assert, describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";

import type { ProviderAdmissionPermit } from "../../agentControl/providerAdmission/model.ts";
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
) =>
  Layer.buildWithScope(
    Layer.fresh(ProviderTurnRequestExecutorLive).pipe(
      Layer.provide(
        Layer.mergeAll(
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

it.effect("quarantines a committed session entry when session evidence persistence fails", () =>
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
      const context = yield* buildExecutor(scope, sql, providerService, () =>
        Effect.succeed({ sequence: 1 }),
      );
      const executor = Context.get(context, ProviderTurnRequestExecutor);

      const result = yield* Effect.exit(
        executor.prepareTurnDelivery({
          threadId,
          messageText: "persist session evidence",
          attachments: [],
          modelSelection,
          interactionMode: "plan",
          createdAt,
          providerDeliveryId: permit.providerDeliveryId,
          durableDeliveryKind: "initial-planning",
          providerAdmissionPermit: permit,
        }),
      );
      assert.isTrue(Exit.isFailure(result));
      assert.equal(quarantineCalls, 1);
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
