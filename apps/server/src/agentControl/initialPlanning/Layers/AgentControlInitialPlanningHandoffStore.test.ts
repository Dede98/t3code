import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlControlledThreadReservationId,
  CommandId,
  EventId,
  ModelSelection,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageEventId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestEventId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../identity.ts";
import {
  canonicalInitialPlanningEventTemplate,
  combinedInitialPlanningEventDigest,
  initialPlanningMessagePayload,
  initialPlanningTurnRequestPayload,
} from "../eventEvidence.ts";
import type { AgentControlInitialPlanningHandoffEvidence } from "../model.ts";
import { AgentControlInitialPlanningHandoffStore } from "../Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningHandoffStoreLive } from "./AgentControlInitialPlanningHandoffStore.ts";

const at = "2026-07-30T12:00:00.000Z";
const deadline = "2026-07-30T13:00:00.000Z";
const encodeModelSelectionJson = Schema.encodeUnknownEffect(Schema.fromJsonString(ModelSelection));

it.effect(
  "uses a real WAL CAS across independent clients and recovers an expired claim by generation",
  () =>
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "initial-planning-store-wal-",
      });
      const filename = path.join(directory, "state.sqlite");
      const scopeA = yield* Scope.make("sequential");
      const scopeB = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(scopeB, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(scopeA, Exit.void));
      const sqlContextA = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeA);
      const sqlContextB = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scopeB);
      const sqlA = Context.get(sqlContextA, SqlClient.SqlClient);
      const sqlB = Context.get(sqlContextB, SqlClient.SqlClient);
      for (const sql of [sqlA, sqlB]) {
        yield* sql`PRAGMA journal_mode = WAL`;
        yield* sql`PRAGMA foreign_keys = ON`;
      }
      yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, sqlA));
      const buildStore = (sql: SqlClient.SqlClient, scope: Scope.Closeable) =>
        Layer.buildWithScope(
          Layer.fresh(AgentControlInitialPlanningHandoffStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
          ),
          scope,
        ).pipe(
          Effect.map((context) => Context.get(context, AgentControlInitialPlanningHandoffStore)),
        );
      const storeA = yield* buildStore(sqlA, scopeA);
      const storeB = yield* buildStore(sqlB, scopeB);

      const reservationId = AgentControlControlledThreadReservationId.make("reservation-wal-cas");
      const threadId = ThreadId.make("thread-wal-cas");
      const handoffId = yield* deriveAgentControlInitialPlanningHandoffId(reservationId, threadId);
      const [turnRequestCommandId, messageId, providerDeliveryId] = yield* Effect.all([
        deriveAgentControlInitialPlanningTurnRequestCommandId(handoffId),
        deriveAgentControlInitialPlanningMessageId(handoffId),
        deriveAgentControlInitialPlanningProviderDeliveryId(handoffId),
      ]);
      const modelSelection = {
        instanceId: ProviderInstanceId.make("provider-wal-cas"),
        model: "model-wal-cas",
        options: [{ id: "reasoningEffort", value: "high" }],
      } satisfies ModelSelection;
      const modelSelectionJson = yield* encodeModelSelectionJson(modelSelection);
      const [messageEventId, turnRequestEventId] = yield* Effect.all([
        deriveAgentControlInitialPlanningMessageEventId(turnRequestCommandId),
        deriveAgentControlInitialPlanningTurnRequestEventId(turnRequestCommandId),
      ]);
      const messageEventTemplateJson = canonicalInitialPlanningEventTemplate({
        streamVersion: 3,
        eventId: messageEventId,
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.message-sent",
        occurredAt: at,
        commandId: turnRequestCommandId,
        causationEventId: null,
        correlationId: turnRequestCommandId,
        actorKind: "client",
        payload: initialPlanningMessagePayload({
          threadId,
          messageId,
          promptText: "Plan only. untrusted-external data follows.",
          createdAt: at,
        }),
        metadata: {},
      });
      const turnRequestEventTemplateJson = canonicalInitialPlanningEventTemplate({
        streamVersion: 4,
        eventId: turnRequestEventId,
        aggregateKind: "thread",
        aggregateId: threadId,
        type: "thread.turn-start-requested",
        occurredAt: at,
        commandId: turnRequestCommandId,
        causationEventId: messageEventId,
        correlationId: turnRequestCommandId,
        actorKind: "client",
        payload: initialPlanningTurnRequestPayload({
          threadId,
          messageId,
          modelSelection,
          runtimeMode: "approval-required",
          createdAt: at,
        }),
        metadata: {},
      });
      const base = {
        handoffId,
        coordinatorCommandId: CommandId.make("coordinator-wal-cas"),
        coordinatorCommandFingerprint: "1".repeat(64),
        materializationCommandId: CommandId.make("materialization-wal-cas"),
        materializationCommandFingerprint: "2".repeat(64),
        projectId: ProjectId.make("project-wal-cas"),
        controlledThreadReservationId: reservationId,
        threadId,
        taskId: "task-wal-cas",
        taskRevision: 1,
        githubIntakeSequence: 1,
        sourceIdentityFingerprint: "3".repeat(64),
        stageRunId: "stage-wal-cas",
        attemptId: "attempt-wal-cas",
        roleId: "planning",
        stageKind: "planning",
        stageOrdinal: 1,
        attemptOrdinal: 1,
        leaseId: "lease-wal-cas",
        leaseHolderId: "holder-wal-cas",
        fenceToken: 1,
        worktreeReservationId: "worktree-wal-cas",
        worktreePath: "/tmp/initial-planning-wal-cas",
        planningRole: "planner",
        providerInstanceId: modelSelection.instanceId,
        runtimeMode: "approval-required",
        modelSelectionJson,
        templateVersion: "agent-control-initial-planning-prompt-v1",
        promptText: "Plan only. untrusted-external data follows.",
        turnRequestCommandId,
        messageId,
        messageEventId,
        turnRequestEventId,
        messageEventTemplateJson,
        turnRequestEventTemplateJson,
        eventTemplateDigest: combinedInitialPlanningEventDigest(
          messageEventTemplateJson,
          turnRequestEventTemplateJson,
        ),
        providerDeliveryId,
      } as const;
      const evidence = {
        ...base,
        handoffFingerprint: fingerprintAgentControlInitialPlanningHandoff(base),
        modelSelection,
        createdAt: at,
        planningDeadlineAt: deadline,
      } satisfies AgentControlInitialPlanningHandoffEvidence;

      // This fixture bypasses only the upstream materialization/event families.
      // The Handoff chain, transition trigger, real WAL file, and both claim
      // stores remain production implementations.
      yield* sqlA`PRAGMA foreign_keys = OFF`;
      yield* sqlA`DROP TRIGGER agent_control_initial_planning_handoff_intent_validate`;
      yield* sqlA.withTransaction(storeA.insertAcceptedInTransaction(evidence));
      yield* sqlA`
        DROP TRIGGER agent_control_initial_planning_turn_accepted_validate
      `;
      yield* sqlA`
        INSERT INTO agent_control_initial_planning_turn_accepted(
          handoff_id, handoff_fingerprint, controlled_thread_reservation_id,
          thread_id, turn_request_command_id, message_id, message_event_id,
          message_event_sequence, turn_request_event_id,
          turn_request_event_sequence, message_event_envelope_json,
          turn_request_event_envelope_json, event_evidence_digest,
          receipt_authority, accepted_at
        ) VALUES (
          ${handoffId}, ${evidence.handoffFingerprint}, ${reservationId},
          ${threadId}, ${turnRequestCommandId}, ${messageId},
          ${messageEventId}, CAST(1 AS INTEGER),
          ${turnRequestEventId}, CAST(2 AS INTEGER),
          ${messageEventTemplateJson}, ${turnRequestEventTemplateJson},
          ${"4".repeat(64)}, 'agent-control', ${at}
        )
      `;
      yield* sqlA`PRAGMA foreign_keys = ON`;
      assert.isTrue(yield* storeA.isHandoffOwnedTurnRequest(turnRequestCommandId));
      assert.isTrue(yield* storeB.isHandoffOwnedTurnRequest(turnRequestCommandId));

      yield* sqlB`
        CREATE TEMP VIEW agent_control_initial_planning_handoff_accepted AS
        SELECT missing.handoff_id, missing.turn_request_command_id
        FROM missing_initial_planning_ownership AS missing
      `;
      assert.equal(
        (yield* Effect.exit(storeB.isHandoffOwnedTurnRequest(turnRequestCommandId)))._tag,
        "Failure",
      );
      yield* sqlB`DROP VIEW agent_control_initial_planning_handoff_accepted`;

      yield* sqlB`
        CREATE TEMP VIEW agent_control_initial_planning_handoff_accepted AS
        SELECT CAST(x'00' AS BLOB) AS handoff_id, turn_request_command_id
        FROM main.agent_control_initial_planning_handoff_accepted
      `;
      assert.equal(
        (yield* Effect.exit(storeB.isHandoffOwnedTurnRequest(turnRequestCommandId)))._tag,
        "Failure",
      );
      yield* sqlB`DROP VIEW agent_control_initial_planning_handoff_accepted`;
      assert.isTrue(yield* storeB.isHandoffOwnedTurnRequest(turnRequestCommandId));

      yield* storeA.markTurnAccepted(handoffId, 0, at);

      const release = yield* Deferred.make<void>();
      const claimA = yield* Deferred.await(release).pipe(
        Effect.andThen(
          storeA.claim({
            handoffId,
            ownerId: "consumer-a",
            now: "2026-07-30T12:00:01.000Z",
            expiresAt: "2026-07-30T12:02:01.000Z",
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );
      const claimB = yield* Deferred.await(release).pipe(
        Effect.andThen(
          storeB.claim({
            handoffId,
            ownerId: "consumer-b",
            now: "2026-07-30T12:00:01.000Z",
            expiresAt: "2026-07-30T12:02:01.000Z",
          }),
        ),
        Effect.forkChild({ startImmediately: true }),
      );
      yield* Deferred.succeed(release, undefined);
      const results = [yield* Fiber.join(claimA), yield* Fiber.join(claimB)];
      assert.equal(results.filter(Option.isSome).length, 1);
      assert.equal(results.filter(Option.isNone).length, 1);
      const winner = Option.getOrThrow(results.find(Option.isSome)!);
      assert.equal(winner.delivery.revision, 2);
      assert.equal(winner.delivery.claimGeneration, 1);
      assert.equal(winner.delivery.attemptCount, 1);

      const recovered = yield* storeB.claim({
        handoffId,
        ownerId: "consumer-restart",
        now: "2026-07-30T12:02:02.000Z",
        expiresAt: "2026-07-30T12:04:02.000Z",
      });
      assert.isTrue(Option.isSome(recovered));
      assert.equal(Option.getOrThrow(recovered).delivery.revision, 3);
      assert.equal(Option.getOrThrow(recovered).delivery.claimGeneration, 2);
      assert.equal(Option.getOrThrow(recovered).delivery.attemptCount, 2);
      const retry = yield* storeB.scheduleRetry({
        handoffId,
        ownerId: "consumer-restart",
        claimGeneration: 2,
        expectedRevision: 3,
        nextAttemptAt: "2026-07-30T12:02:30.000Z",
        errorCode: "provider-quota",
        updatedAt: "2026-07-30T12:02:02.000Z",
      });
      assert.equal(retry.state, "retry-wait");
      const retryClaim = Option.getOrThrow(
        yield* storeA.claim({
          handoffId,
          ownerId: "consumer-a-retry",
          now: "2026-07-30T12:02:31.000Z",
          expiresAt: "2026-07-30T12:04:31.000Z",
        }),
      );
      assert.equal(retryClaim.evidence.providerDeliveryId, providerDeliveryId);
      assert.equal(retryClaim.delivery.revision, 5);
      assert.equal(retryClaim.delivery.claimGeneration, 3);
      assert.equal(retryClaim.delivery.attemptCount, 3);
      const attempted = yield* storeA.markDeliveryAttempted({
        providerDeliveryId,
        handoffId,
        ownerId: "consumer-a-retry",
        claimGeneration: 3,
        expectedRevision: 5,
        attemptedAt: "2026-07-30T12:02:32.000Z",
        providerSessionCreatedAt: at,
        providerResumeCursorJson: "null",
        providerInstanceId: String(modelSelection.instanceId),
        turnModelSelectionJson:
          canonicalProviderModelSelectionEvidence(modelSelection).modelSelectionJson,
        turnModelSelectionFingerprint:
          canonicalProviderModelSelectionEvidence(modelSelection).modelSelectionFingerprint,
      });
      assert.equal(attempted.state, "delivery-attempted");
      assert.deepStrictEqual(
        yield* sqlA`
          SELECT provider_instance_id AS "providerInstanceId",
            model_selection_json AS "modelSelectionJson",
            model_selection_fingerprint AS "modelSelectionFingerprint"
          FROM agent_control_initial_planning_delivery_attestations
          WHERE provider_delivery_id = ${providerDeliveryId}
        `,
        [
          {
            providerInstanceId: String(modelSelection.instanceId),
            modelSelectionJson:
              canonicalProviderModelSelectionEvidence(modelSelection).modelSelectionJson,
            modelSelectionFingerprint:
              canonicalProviderModelSelectionEvidence(modelSelection).modelSelectionFingerprint,
          },
        ],
      );
      assert.isTrue(
        Option.isNone(
          yield* storeB.claim({
            handoffId,
            ownerId: "consumer-b-blind-redelivery",
            now: "2026-07-30T12:04:32.000Z",
            expiresAt: "2026-07-30T12:06:32.000Z",
          }),
        ),
      );
      assert.deepStrictEqual(
        yield* sqlA`
          SELECT state, revision, claim_generation AS "claimGeneration",
            attempt_count AS "attemptCount"
          FROM agent_control_initial_planning_deliveries
          WHERE handoff_id = ${handoffId}
        `,
        [
          {
            state: "delivery-attempted",
            revision: 6,
            claimGeneration: 3,
            attemptCount: 3,
          },
        ],
      );

      const invalidProviderStarts: ReadonlyArray<readonly [string, unknown, unknown]> = [
        ["both-null", null, null],
        ["turn-only", "provider-turn-wal", null],
        ["accepted-only", null, "2026-07-30T12:02:33.000Z"],
        ["turn-real", 1.5, "2026-07-30T12:02:33.000Z"],
        ["turn-blob", new Uint8Array([1]), "2026-07-30T12:02:33.000Z"],
        ["accepted-numeric-text", "provider-turn-wal", "123"],
        ["accepted-real", "provider-turn-wal", 1.5],
        ["accepted-blob", "provider-turn-wal", new Uint8Array([1])],
      ];
      for (const [label, providerTurnId, providerAcceptedAt] of invalidProviderStarts) {
        assert.equal(
          (yield* Effect.exit(
            sqlA.unsafe(
              `UPDATE agent_control_initial_planning_deliveries
                 SET state = 'provider-started', revision = 7,
                   claim_owner_id = NULL, claim_expires_at = NULL,
                   provider_turn_id = ?, provider_accepted_at = ?
                 WHERE handoff_id = ?`,
              [providerTurnId, providerAcceptedAt, handoffId],
            ),
          ))._tag,
          "Failure",
          label,
        );
      }

      const providerStarted = yield* storeA.markProviderStarted({
        handoffId,
        ownerId: "consumer-a-retry",
        claimGeneration: 3,
        expectedRevision: attempted.revision,
        providerTurnId: "provider-turn-wal",
        acceptedAt: "2026-07-30T12:02:33.000Z",
      });
      assert.equal(providerStarted.state, "provider-started");
      const restartedAtProviderStarted = Option.getOrThrow(
        yield* storeB.loadAcceptedByHandoffId(handoffId),
      );
      assert.equal(restartedAtProviderStarted.delivery.providerTurnId, "provider-turn-wal");
      assert.equal(
        restartedAtProviderStarted.delivery.providerAcceptedAt,
        "2026-07-30T12:02:33.000Z",
      );
      assert.equal(restartedAtProviderStarted.delivery.providerSessionCreatedAt, at);
      assert.equal(restartedAtProviderStarted.delivery.providerResumeCursorJson, "null");

      for (const [label, turnExpression, acceptedExpression] of [
        ["both-null", "NULL", "NULL"],
        ["turn-null", "NULL", "provider_accepted_at"],
        ["accepted-null", "provider_turn_id", "NULL"],
        ["other-turn", "'different-provider-turn'", "provider_accepted_at"],
        ["other-accepted", "provider_turn_id", "'2026-07-30T12:02:34.000Z'"],
      ] as const) {
        assert.equal(
          (yield* Effect.exit(
            sqlA.unsafe(
              `UPDATE agent_control_initial_planning_deliveries
                 SET state = 'interrupt-requested', revision = 8,
                   interrupt_requested = 1,
                   provider_turn_id = ${turnExpression},
                   provider_accepted_at = ${acceptedExpression}
                 WHERE handoff_id = ?`,
              [handoffId],
            ),
          ))._tag,
          "Failure",
          label,
        );
      }

      const interruptRequested = yield* storeB.requestInterrupt({
        handoffId,
        expectedRevision: providerStarted.revision,
        requestedAt: "2026-07-30T12:02:34.000Z",
      });
      assert.equal(interruptRequested.state, "interrupt-requested");
      const restartedAtInterrupt = Option.getOrThrow(
        yield* storeA.loadAcceptedByHandoffId(handoffId),
      );
      assert.equal(restartedAtInterrupt.delivery.providerTurnId, "provider-turn-wal");
      assert.equal(restartedAtInterrupt.delivery.providerAcceptedAt, "2026-07-30T12:02:33.000Z");
      assert.equal(restartedAtInterrupt.delivery.providerSessionCreatedAt, at);
      assert.equal(restartedAtInterrupt.delivery.providerResumeCursorJson, "null");

      assert.equal(
        (yield* Effect.exit(
          sqlB`
              UPDATE agent_control_initial_planning_deliveries
              SET state = 'ambiguous', revision = 9,
                provider_turn_id = 'different-provider-turn',
                terminal_at = '2026-07-30T12:02:35.000Z',
                last_error_code = 'provider-acceptance-ambiguous'
              WHERE handoff_id = ${handoffId}
            `,
        ))._tag,
        "Failure",
      );
      const ambiguous = yield* storeB.markAmbiguous({
        handoffId,
        expectedRevision: interruptRequested.revision,
        terminalAt: "2026-07-30T12:02:35.000Z",
      });
      assert.equal(ambiguous.state, "ambiguous");
      assert.equal(ambiguous.providerTurnId, "provider-turn-wal");
      assert.equal(ambiguous.providerAcceptedAt, "2026-07-30T12:02:33.000Z");

      assert.isTrue(
        Option.isNone(
          yield* storeA.observeProviderTerminal({
            threadId,
            providerTurnId: "provider-turn-wal",
            state: "completed",
            terminalAt: "2026-07-30T12:02:36.000Z",
          }),
        ),
      );
      assert.deepStrictEqual(
        Option.getOrThrow(yield* storeB.loadAcceptedByHandoffId(handoffId)).delivery,
        ambiguous,
      );
      const completed = Option.getOrThrow(
        yield* storeA.observeProviderTerminal({
          threadId,
          providerTurnId: "provider-turn-wal",
          state: "completed",
          terminalAt: "2026-07-30T12:02:36.000Z",
          nativeEvent: {
            type: "turn.completed",
            eventId: EventId.make("native-terminal-wal"),
            provider: ProviderDriverKind.make("codex"),
            providerInstanceId: modelSelection.instanceId,
            threadId,
            turnId: TurnId.make("provider-turn-wal"),
            createdAt: "2026-07-30T12:02:36.000Z",
            payload: { state: "completed" },
          },
        }),
      );
      assert.equal(completed.state, "completed");
      assert.equal(completed.providerTurnId, "provider-turn-wal");
      assert.equal(completed.providerAcceptedAt, "2026-07-30T12:02:33.000Z");
      assert.deepStrictEqual(
        yield* sqlB`SELECT native_event_id, delivery_revision
          FROM agent_control_native_terminal_receipts WHERE handoff_id=${handoffId}`,
        [{ native_event_id: "native-terminal-wal", delivery_revision: completed.revision }],
      );
      assert.isTrue(
        Option.isNone(
          yield* storeB.observeProviderTerminal({
            threadId,
            providerTurnId: "different-provider-turn",
            state: "failed",
            terminalAt: "2026-07-30T12:02:37.000Z",
            errorCode: "provider-defect",
          }),
        ),
      );
      for (const lateState of ["completed", "failed"] as const) {
        assert.isTrue(
          Option.isNone(
            yield* storeB.observeProviderTerminal({
              threadId,
              providerTurnId: "provider-turn-wal",
              state: lateState,
              terminalAt: "2026-07-30T12:02:38.000Z",
              ...(lateState === "failed" ? { errorCode: "provider-defect" } : {}),
            }),
          ),
        );
      }
    }).pipe(Effect.provide(NodeServices.layer)),
);
