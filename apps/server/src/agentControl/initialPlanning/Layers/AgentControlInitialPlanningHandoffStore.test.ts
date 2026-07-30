import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AgentControlControlledThreadReservationId,
  CommandId,
  ModelSelection,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
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
import {
  deriveAgentControlInitialPlanningHandoffId,
  deriveAgentControlInitialPlanningMessageId,
  deriveAgentControlInitialPlanningProviderDeliveryId,
  deriveAgentControlInitialPlanningTurnRequestCommandId,
  fingerprintAgentControlInitialPlanningHandoff,
} from "../identity.ts";
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
          turn_request_event_sequence, receipt_authority, accepted_at
        ) VALUES (
          ${handoffId}, ${evidence.handoffFingerprint}, ${reservationId},
          ${threadId}, ${turnRequestCommandId}, ${messageId},
          'message-event-wal-cas', CAST(1 AS INTEGER),
          'turn-event-wal-cas', CAST(2 AS INTEGER), 'agent-control', ${at}
        )
      `;
      yield* sqlA`PRAGMA foreign_keys = ON`;
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
        handoffId,
        ownerId: "consumer-a-retry",
        claimGeneration: 3,
        expectedRevision: 5,
        attemptedAt: "2026-07-30T12:02:32.000Z",
      });
      assert.equal(attempted.state, "delivery-attempted");
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
    }).pipe(Effect.provide(NodeServices.layer)),
);
