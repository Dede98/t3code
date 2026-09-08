import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ModelSelection,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderUsageSnapshot,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { AgentControlImplementationTurnWakeup } from "../../implementationTurn/Services/AgentControlImplementationTurnWakeup.ts";
import { AgentControlInitialPlanningWakeup } from "../../initialPlanning/Services/AgentControlInitialPlanningWakeup.ts";
import { AgentControlVerificationTurnWakeup } from "../../verificationTurn/Services/AgentControlVerificationTurnWakeup.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardShape,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import { ProviderUsage } from "../../../provider/Services/ProviderUsage.ts";
import type { ProviderAdmissionRequest } from "../model.ts";
import { ProviderAdmissionGuard } from "../Services/ProviderAdmissionGuard.ts";
import { ProviderAdmissionRuntime } from "../Services/ProviderAdmissionRuntime.ts";
import {
  ProviderAdmissionError,
  ProviderAdmissionStore,
} from "../Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionRuntimeLive } from "./ProviderAdmissionRuntime.ts";
import {
  PROVIDER_ADMISSION_DUE_ADMITTED_DEADLINES_SQL,
  PROVIDER_ADMISSION_DUE_WAITING_DEADLINES_SQL,
  PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_AFTER_SQL,
  PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_SQL,
  PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_AFTER_SQL,
  PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_SQL,
  ProviderAdmissionStoreLive,
} from "./ProviderAdmissionStore.ts";
import { ProviderAdmissionGuardLive } from "./ProviderAdmissionGuard.ts";
import { providerAdmissionUsageEvidence } from "../model.ts";

type Observation =
  | { readonly _tag: "Unsupported"; readonly observedAt: string }
  | { readonly _tag: "Observed"; readonly snapshot: ProviderUsageSnapshot }
  | { readonly _tag: "SupportedUnusable"; readonly observedAt: string };

const driver = ProviderDriverKind.make("codex");
const epoch = "1970-01-01T00:00:00.000Z";
const reset = "1970-01-01T00:00:01.000Z";

const taskGuardShape: AgentControlTaskConsumerGuardShape = {
  inspectProject: () => Effect.die("not used"),
  useTaskConsumable: (_projectId, _taskId, use) => use({} as never, {} as never),
  useTaskConsumableInTransaction: (_projectId, _taskId, use) => use({} as never, {} as never),
  useTaskForProviderEffectInTransaction: (_projectId, _taskId, use) =>
    use({} as never, {} as never),
};

const snapshot = (
  providerInstanceId: ProviderInstanceId,
  status: "allowed" | "warning" | "rejected",
  resetsAt: string | null = null,
): ProviderUsageSnapshot => ({
  providerInstanceId,
  driver,
  observedAt: epoch,
  source: "refresh",
  status,
  windows: [
    {
      id: "primary",
      label: "Primary",
      usedPercent: status === "rejected" ? 100 : status === "warning" ? 90 : 10,
      resetsAt,
    },
  ],
});

const request = (
  suffix: string,
  providerInstanceId: ProviderInstanceId,
  stage: ProviderAdmissionRequest["stage"],
): ProviderAdmissionRequest => {
  const modelSelection: ModelSelection = { instanceId: providerInstanceId, model: "gpt-5.6" };
  const model = canonicalProviderModelSelectionEvidence(modelSelection);
  return {
    stage,
    projectId: `project-${suffix}`,
    taskId: `task-${suffix}`,
    stageRunId: `stage-${suffix}`,
    attemptId: `attempt-${suffix}`,
    handoffId: `handoff-${suffix}`,
    providerDeliveryId: `delivery-${suffix}`,
    threadId: `thread-${suffix}`,
    providerInstanceId,
    stageLeaseId: `lease-${suffix}`,
    stageLeaseHolderId: `holder-${suffix}`,
    stageFenceToken: 3,
    modelSelection,
    modelSelectionJson: model.modelSelectionJson,
    modelSelectionFingerprint: model.modelSelectionFingerprint,
    requestedAt: epoch,
  };
};

it.effect("binds all usage outcomes and wakes rejected admission from one global deadline", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-admission-runtime-" });
      const filename = path.join(directory, "runtime.sqlite");
      const sqlScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(sqlScope, Exit.void));
      const sqlContext = yield* Layer.buildWithScope(
        NodeSqliteClient.layer({ filename }),
        sqlScope,
      );
      const sql = Context.get(sqlContext, SqlClient.SqlClient);
      yield* sql`PRAGMA journal_mode=WAL`;
      yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );

      const allowedId = ProviderInstanceId.make("usage-allowed");
      const warningId = ProviderInstanceId.make("usage-warning");
      const rejectedId = ProviderInstanceId.make("usage-rejected");
      const staleRejectedId = ProviderInstanceId.make("usage-rejected-stale");
      const unsupportedId = ProviderInstanceId.make("usage-unsupported");
      const unusableId = ProviderInstanceId.make("usage-unusable");
      const observations = yield* Ref.make(
        new Map<ProviderInstanceId, Observation>([
          [allowedId, { _tag: "Observed", snapshot: snapshot(allowedId, "allowed") }],
          [warningId, { _tag: "Observed", snapshot: snapshot(warningId, "warning") }],
          [rejectedId, { _tag: "Observed", snapshot: snapshot(rejectedId, "rejected", reset) }],
          [
            staleRejectedId,
            { _tag: "Observed", snapshot: snapshot(staleRejectedId, "rejected", reset) },
          ],
          [unsupportedId, { _tag: "Unsupported", observedAt: epoch }],
          [unusableId, { _tag: "SupportedUnusable", observedAt: epoch }],
        ]),
      );
      const usageEvents = yield* PubSub.unbounded<never>();
      const wakeups = yield* Ref.make<Array<string>>([]);
      const rejectedDeadlineWake = yield* Deferred.make<void>();
      const wake = (handoffId: string) =>
        Ref.update(wakeups, (values) => [...values, handoffId]).pipe(
          Effect.andThen(
            handoffId === "handoff-rejected"
              ? Deferred.succeed(rejectedDeadlineWake, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
        );
      const usageLayer = Layer.succeed(ProviderUsage, {
        inspectForAdmission: (providerInstanceId) =>
          Ref.get(observations).pipe(Effect.map((current) => current.get(providerInstanceId)!)),
        getSnapshot: Effect.succeed([]),
        refresh: () => Effect.succeed({ refreshedAt: epoch, usage: [], failures: [] }),
        subscribeEvents: PubSub.subscribe(usageEvents),
      });
      const wakeupLayers = Layer.mergeAll(
        Layer.succeed(AgentControlInitialPlanningWakeup, { wake, stream: Stream.never }),
        Layer.succeed(AgentControlImplementationTurnWakeup, { wake, stream: Stream.never }),
        Layer.succeed(AgentControlVerificationTurnWakeup, {
          wake,
          stream: Stream.never,
          subscribe: Effect.succeed(Stream.never),
        }),
      );
      const storeLayer = Layer.fresh(ProviderAdmissionStoreLive).pipe(
        Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
      );
      const runtimeLayer = Layer.fresh(ProviderAdmissionRuntimeLive).pipe(
        Layer.provideMerge(storeLayer),
        Layer.provideMerge(usageLayer),
        Layer.provide(wakeupLayers),
      );
      const runtimeScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
      const runtimeContext = yield* Layer.buildWithScope(runtimeLayer, runtimeScope);
      const runtime = Context.get(runtimeContext, ProviderAdmissionRuntime);
      const store = Context.get(runtimeContext, ProviderAdmissionStore);

      assert.equal(
        (yield* runtime.request(request("allowed", allowedId, "initial-planning")))._tag,
        "Admitted",
      );
      assert.equal(
        (yield* runtime.request(request("warning", warningId, "implementation")))._tag,
        "Admitted",
      );
      assert.equal(
        (yield* runtime.request(request("unsupported", unsupportedId, "verification")))._tag,
        "Admitted",
      );
      assert.equal(
        (yield* runtime.request(request("rejected", rejectedId, "implementation")))._tag,
        "Waiting",
      );
      assert.equal(
        (yield* runtime.request(request("rejected-stale", staleRejectedId, "implementation")))._tag,
        "Waiting",
      );
      assert.equal(
        (yield* runtime.request(request("unusable", unusableId, "verification")))._tag,
        "Waiting",
      );
      const persisted = yield* sql<{
        readonly providerInstanceId: string;
        readonly status: string;
      }>`
        SELECT provider_instance_id AS "providerInstanceId",status
        FROM main.agent_control_provider_usage_evidence
        ORDER BY provider_instance_id
      `;
      assert.deepStrictEqual(
        persisted.map((row) => [row.providerInstanceId, row.status]),
        [
          ["usage-allowed", "allowed"],
          ["usage-rejected", "rejected"],
          ["usage-rejected-stale", "rejected"],
          ["usage-unsupported", "unsupported"],
          ["usage-unusable", "supported-unusable"],
          ["usage-warning", "warning"],
        ],
      );
      yield* Ref.update(observations, (current) =>
        new Map(current).set(unusableId, {
          _tag: "Observed",
          snapshot: { ...snapshot(unusableId, "allowed"), observedAt: reset },
        }),
      );
      assert.equal(
        (yield* runtime.request(request("unusable", unusableId, "verification")))._tag,
        "Waiting",
      );
      yield* TestClock.adjust("500 millis");
      yield* runtime.usageChanged(
        String(unusableId),
        providerAdmissionUsageEvidence({
          providerInstanceId: unusableId,
          status: "allowed",
          observedAt: "1970-01-01T00:00:00.500Z",
          source: "runtime-event",
          nextRelevantAt: null,
        }),
      );
      assert.deepStrictEqual(
        yield* sql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE handoff_id='handoff-unusable'
        `,
        [{ status: "admitted" }],
      );
      yield* Ref.update(observations, (current) =>
        new Map(current).set(rejectedId, {
          _tag: "Observed",
          snapshot: { ...snapshot(rejectedId, "allowed"), observedAt: reset },
        }),
      );
      yield* TestClock.adjust("500 millis");
      yield* Deferred.await(rejectedDeadlineWake);
      assert.isTrue((yield* Ref.get(wakeups)).includes("handoff-rejected"));
      const rejectedAfterWake = yield* sql<{
        readonly status: string;
        readonly markerId: string | null;
      }>`
          SELECT status,admission_marker_id AS "markerId"
          FROM main.agent_control_provider_admission_current
          WHERE handoff_id='handoff-rejected'
        `;
      assert.equal(rejectedAfterWake[0]?.status, "admitted");
      assert.match(rejectedAfterWake[0]?.markerId ?? "", /^provider-admission:/u);
      assert.equal(yield* store.minimumDeadline, reset);
      assert.equal(yield* store.minimumDeadlineAfter(reset), "1970-01-01T00:02:00.000Z");
      assert.deepStrictEqual(
        yield* sql<{ readonly status: string; readonly nextDeadlineAt: string }>`
          SELECT status,next_deadline_at AS "nextDeadlineAt"
          FROM main.agent_control_provider_admission_current
          WHERE handoff_id='handoff-rejected-stale'
        `,
        [{ status: "waiting", nextDeadlineAt: reset }],
      );
      assert.deepStrictEqual(yield* sql`PRAGMA main.foreign_key_check`, []);
      assert.equal((yield* sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "resumes persisted waiting work without polling Usage and admits only after new evidence",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-admission-resume-" });
        const filename = path.join(directory, "resume.sqlite");
        const sqlScope = yield* Scope.make("sequential");
        yield* Effect.addFinalizer(() => Scope.close(sqlScope, Exit.void));
        const sqlContext = yield* Layer.buildWithScope(
          NodeSqliteClient.layer({ filename }),
          sqlScope,
        );
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        yield* sql`PRAGMA journal_mode=WAL`;
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, sql),
        );

        const providerInstanceId = ProviderInstanceId.make("usage-recovery-no-poll");
        const inspectCalls = yield* Ref.make(0);
        const usageEvents = yield* PubSub.unbounded<never>();
        const usageLayer = Layer.succeed(ProviderUsage, {
          inspectForAdmission: () =>
            Ref.updateAndGet(inspectCalls, (count) => count + 1).pipe(
              Effect.as({
                _tag: "Observed" as const,
                snapshot: snapshot(providerInstanceId, "rejected"),
              }),
            ),
          getSnapshot: Effect.succeed([]),
          refresh: () => Effect.succeed({ refreshedAt: epoch, usage: [], failures: [] }),
          subscribeEvents: PubSub.subscribe(usageEvents),
        });
        const wakeupLayers = Layer.mergeAll(
          Layer.succeed(AgentControlInitialPlanningWakeup, {
            wake: () => Effect.void,
            stream: Stream.never,
          }),
          Layer.succeed(AgentControlImplementationTurnWakeup, {
            wake: () => Effect.void,
            stream: Stream.never,
          }),
          Layer.succeed(AgentControlVerificationTurnWakeup, {
            wake: () => Effect.void,
            stream: Stream.never,
            subscribe: Effect.succeed(Stream.never),
          }),
        );
        const buildRuntime = Effect.gen(function* () {
          const scope = yield* Scope.make("sequential");
          const storeLayer = Layer.fresh(ProviderAdmissionStoreLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
          );
          const runtimeLayer = Layer.fresh(ProviderAdmissionRuntimeLive).pipe(
            Layer.provideMerge(storeLayer),
            Layer.provideMerge(usageLayer),
            Layer.provide(wakeupLayers),
          );
          const context = yield* Layer.buildWithScope(runtimeLayer, scope);
          return { runtime: Context.get(context, ProviderAdmissionRuntime), scope } as const;
        });

        const first = yield* buildRuntime;
        assert.equal(
          (yield* first.runtime.request(request("no-poll", providerInstanceId, "implementation")))
            ._tag,
          "Waiting",
        );
        assert.equal(yield* Ref.get(inspectCalls), 1);
        yield* Scope.close(first.scope, Exit.void);

        const restarted = yield* buildRuntime;
        const recovered = yield* restarted.runtime.request(
          request("no-poll", providerInstanceId, "implementation"),
        );
        assert.equal(recovered._tag, "Waiting");
        assert.equal(yield* Ref.get(inspectCalls), 1);

        yield* TestClock.adjust("1 second");
        yield* restarted.runtime.usageChanged(
          String(providerInstanceId),
          providerAdmissionUsageEvidence({
            providerInstanceId,
            status: "allowed",
            observedAt: reset,
            source: "runtime-event",
            nextRelevantAt: null,
          }),
        );
        assert.equal(yield* Ref.get(inspectCalls), 1);
        assert.deepStrictEqual(
          yield* sql<{ readonly status: string }>`
          SELECT status FROM main.agent_control_provider_admission_current
          WHERE handoff_id='handoff-no-poll'
        `,
          [{ status: "admitted" }],
        );
        yield* Scope.close(restarted.scope, Exit.void);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "wakes an expired admitted lease across fresh native WAL runtimes and takes over monotonically",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "t3-admission-lease-deadline-",
        });
        const filename = path.join(directory, "lease-deadline.sqlite");
        const openSql = Effect.gen(function* () {
          const scope = yield* Scope.make("sequential");
          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
          const context = yield* Layer.buildWithScope(NodeSqliteClient.layer({ filename }), scope);
          const sql = Context.get(context, SqlClient.SqlClient);
          yield* sql`PRAGMA journal_mode=WAL`;
          return { scope, sql } as const;
        });
        const firstSql = yield* openSql;
        const secondSql = yield* openSql;
        yield* runMigrations({ toMigrationInclusive: 65 }).pipe(
          Effect.provideService(SqlClient.SqlClient, firstSql.sql),
        );

        const providerInstanceId = ProviderInstanceId.make("lease-deadline-entered");
        const parallelProviderInstanceId = ProviderInstanceId.make("lease-deadline-takeover");
        const restartProviderInstanceId = ProviderInstanceId.make("lease-deadline-restart");
        const originalRequest = request(
          "lease-deadline-entered",
          providerInstanceId,
          "initial-planning",
        );
        const parallelRequest = request(
          "lease-deadline-takeover",
          parallelProviderInstanceId,
          "implementation",
        );
        const restartRequest = request(
          "lease-deadline-restart",
          restartProviderInstanceId,
          "verification",
        );
        const usageEvents = yield* PubSub.unbounded<never>();
        const usageLayer = Layer.succeed(ProviderUsage, {
          inspectForAdmission: (instanceId) =>
            Effect.succeed({
              _tag: "Observed" as const,
              snapshot: snapshot(instanceId, "allowed"),
            }),
          getSnapshot: Effect.succeed([]),
          refresh: () => Effect.succeed({ refreshedAt: epoch, usage: [], failures: [] }),
          subscribeEvents: PubSub.subscribe(usageEvents),
        });
        const captureParallelWakeup = yield* Ref.make(false);
        const captureRestartWakeup = yield* Ref.make(false);
        const parallelWakeup = yield* Deferred.make<void>();
        const restartWakeup = yield* Deferred.make<void>();
        const wakeups = yield* Ref.make<Array<string>>([]);
        const wake = (handoffId: string) =>
          Effect.gen(function* () {
            yield* Ref.update(wakeups, (current) => [...current, handoffId]);
            if (
              handoffId === parallelRequest.handoffId &&
              (yield* Ref.get(captureParallelWakeup))
            ) {
              yield* Deferred.succeed(parallelWakeup, undefined);
            }
            if (handoffId === restartRequest.handoffId && (yield* Ref.get(captureRestartWakeup))) {
              yield* Deferred.succeed(restartWakeup, undefined);
            }
          });
        const wakeupLayers = Layer.mergeAll(
          Layer.succeed(AgentControlInitialPlanningWakeup, { wake, stream: Stream.never }),
          Layer.succeed(AgentControlImplementationTurnWakeup, { wake, stream: Stream.never }),
          Layer.succeed(AgentControlVerificationTurnWakeup, {
            wake,
            stream: Stream.never,
            subscribe: Effect.succeed(Stream.never),
          }),
        );
        const buildRuntime = (sql: SqlClient.SqlClient) =>
          Effect.gen(function* () {
            const scope = yield* Scope.make("sequential");
            yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
            const storeLayer = Layer.fresh(ProviderAdmissionStoreLive).pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, sql)),
            );
            const runtimeLayer = Layer.fresh(ProviderAdmissionRuntimeLive).pipe(
              Layer.provideMerge(storeLayer),
              Layer.provideMerge(usageLayer),
              Layer.provide(wakeupLayers),
            );
            const context = yield* Layer.buildWithScope(runtimeLayer, scope);
            return {
              runtime: Context.get(context, ProviderAdmissionRuntime),
              store: Context.get(context, ProviderAdmissionStore),
              scope,
            } as const;
          });

        const first = yield* buildRuntime(firstSql.sql);
        const firstDecision = yield* first.runtime.request(originalRequest);
        assert.equal(firstDecision._tag, "Admitted");
        if (firstDecision._tag !== "Admitted") return;
        assert.equal(firstDecision.permit.providerFenceToken, 1);
        yield* Scope.close(first.scope, Exit.void);

        const guardTriggers = yield* firstSql.sql<{
          readonly name: string;
          readonly source: string;
        }>`
          SELECT name,sql AS source FROM main.sqlite_schema
          WHERE type='trigger' AND tbl_name IN (
            'agent_control_stage_run_states',
            'agent_control_stage_run_lease_states',
            'agent_control_initial_planning_deliveries'
          ) AND sql IS NOT NULL
          ORDER BY name
        `;
        for (const trigger of guardTriggers) {
          yield* firstSql.sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        yield* firstSql.sql`PRAGMA foreign_keys=OFF`;
        yield* firstSql.sql.withTransaction(
          Effect.gen(function* () {
            yield* firstSql.sql`
              INSERT INTO main.agent_control_stage_run_states (
                stage_run_id,project_id,task_id,attempt_id,role_id,stage_kind,stage_ordinal,
                attempt_ordinal,status,task_revision,github_intake_sequence,
                source_identity_fingerprint,state_json,created_at,updated_at,revision,
                last_event_sequence
              ) VALUES (
                ${originalRequest.stageRunId},${originalRequest.projectId},
                ${originalRequest.taskId},${originalRequest.attemptId},
                'role-lease-deadline','planning',1,1,'running',1,1,${"a".repeat(64)},'{}',
                ${epoch},${epoch},1,1
              )
            `;
            yield* firstSql.sql`
              INSERT INTO main.agent_control_stage_run_lease_states (
                lease_id,project_id,task_id,stage_run_id,attempt_id,task_revision,
                github_intake_sequence,source_identity_fingerprint,holder_id,fence_token,
                status,acquired_at,renewed_at,expires_at,released_at,state_json,revision,
                last_event_sequence
              ) VALUES (
                ${originalRequest.stageLeaseId},${originalRequest.projectId},
                ${originalRequest.taskId},${originalRequest.stageRunId},
                ${originalRequest.attemptId},1,1,${"a".repeat(64)},
                ${originalRequest.stageLeaseHolderId},${originalRequest.stageFenceToken},
                'reserved',${epoch},${epoch},'2099-01-01T00:00:00.000Z',NULL,'{}',1,1
              )
            `;
            yield* firstSql.sql`
              INSERT INTO main.agent_control_initial_planning_deliveries (
                provider_delivery_id,handoff_id,handoff_fingerprint,
                controlled_thread_reservation_id,thread_id,turn_request_command_id,message_id,
                provider_instance_id,state,revision,claim_owner_id,claim_generation,
                claim_expires_at,attempt_count,next_attempt_at,planning_deadline_at,
                provider_turn_id,provider_accepted_at,provider_session_created_at,
                provider_resume_cursor_json,terminal_at,last_error_code,interrupt_requested,
                updated_at
              ) VALUES (
                ${originalRequest.providerDeliveryId},${originalRequest.handoffId},
                ${"b".repeat(64)},'reservation-lease-deadline',${originalRequest.threadId},
                'command-lease-deadline','message-lease-deadline',
                ${originalRequest.providerInstanceId},'claimed',1,'delivery-owner',1,
                '2099-01-01T00:00:00.000Z',0,NULL,'2099-01-01T00:00:00.000Z',
                NULL,NULL,NULL,NULL,NULL,NULL,0,${epoch}
              )
            `;
          }),
        );
        for (const trigger of guardTriggers) {
          yield* firstSql.sql.unsafe(trigger.source).unprepared;
        }
        yield* firstSql.sql`PRAGMA foreign_keys=ON`;

        const second = yield* buildRuntime(secondSql.sql);
        const beforeExpiry = yield* second.runtime.request(originalRequest);
        assert.deepStrictEqual(beforeExpiry, {
          _tag: "Waiting",
          admissionId: firstDecision.permit.admissionId,
          retryAt: firstDecision.permit.admissionLeaseExpiresAt,
        });
        yield* TestClock.adjust("30 seconds");
        const parallel = yield* second.runtime.request(parallelRequest);
        assert.equal(parallel._tag, "Admitted");
        if (parallel._tag !== "Admitted") return;
        assert.equal(parallel.permit.providerFenceToken, 1);
        yield* Ref.set(captureParallelWakeup, true);

        const guardContext = yield* Layer.buildWithScope(
          Layer.fresh(ProviderAdmissionGuardLive).pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, secondSql.sql)),
            Layer.provide(Layer.succeed(ProviderAdmissionStore, second.store)),
            Layer.provide(Layer.succeed(AgentControlTaskConsumerGuard, taskGuardShape)),
          ),
          second.scope,
        );
        const guard = Context.get(guardContext, ProviderAdmissionGuard);
        yield* guard.enter(firstDecision.permit, "session-start");
        assert.deepStrictEqual(
          yield* secondSql.sql<{ readonly status: string }>`
            SELECT status FROM main.agent_control_provider_admission_current
            WHERE admission_id=${firstDecision.permit.admissionId}
          `,
          [{ status: "entered" }],
        );

        yield* TestClock.adjust("90 seconds");
        assert.isTrue(Option.isNone(yield* Deferred.poll(parallelWakeup)));
        assert.equal(
          (yield* Ref.get(wakeups)).filter((handoffId) => handoffId === parallelRequest.handoffId)
            .length,
          1,
        );
        yield* TestClock.adjust("30 seconds");
        yield* Deferred.await(parallelWakeup);
        assert.equal(
          (yield* Ref.get(wakeups)).filter((handoffId) => handoffId === parallelRequest.handoffId)
            .length,
          2,
        );

        const takeover = yield* second.runtime.request(parallelRequest);
        assert.equal(takeover._tag, "Admitted");
        if (takeover._tag !== "Admitted") return;
        assert.equal(takeover.permit.providerFenceToken, parallel.permit.providerFenceToken + 1);
        assert.notEqual(takeover.permit.admissionMarkerId, parallel.permit.admissionMarkerId);
        assert.deepStrictEqual(
          yield* secondSql.sql<{ readonly count: number }>`
            SELECT count(*) AS count
            FROM main.agent_control_provider_claim_history
            WHERE admission_id=${takeover.permit.admissionId}
          `,
          [{ count: 2 }],
        );
        assert.deepStrictEqual(yield* second.runtime.request(originalRequest), {
          _tag: "Waiting",
          admissionId: firstDecision.permit.admissionId,
          retryAt: null,
        });
        assert.deepStrictEqual(
          yield* secondSql.sql<{ readonly count: number }>`
            SELECT count(*) AS count
            FROM main.agent_control_provider_authority_markers
            WHERE admission_id=${takeover.permit.admissionId} AND authority_kind='admission'
          `,
          [{ count: 2 }],
        );

        const restartFirst = yield* second.runtime.request(restartRequest);
        assert.equal(restartFirst._tag, "Admitted");
        if (restartFirst._tag !== "Admitted") return;
        yield* Scope.close(second.scope, Exit.void);
        yield* TestClock.adjust("2 minutes");
        yield* Ref.set(captureRestartWakeup, true);
        const restarted = yield* buildRuntime(firstSql.sql);
        yield* Deferred.await(restartWakeup);
        const restartTakeover = yield* restarted.runtime.request(restartRequest);
        assert.equal(restartTakeover._tag, "Admitted");
        if (restartTakeover._tag !== "Admitted") return;
        assert.equal(
          restartTakeover.permit.providerFenceToken,
          restartFirst.permit.providerFenceToken + 1,
        );

        assert.deepStrictEqual(
          yield* secondSql.sql<{ readonly name: string }>`
            PRAGMA main.index_info('idx_agent_control_provider_admission_lease_deadline')
          `.pipe(Effect.map((rows) => rows.map((row) => row.name))),
          ["status", "lease_expires_at", "provider_instance_id", "admission_id"],
        );
        for (const [statement, parameters, index] of [
          [
            PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_SQL,
            [],
            "idx_agent_control_provider_admission_deadline",
          ],
          [
            PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_SQL,
            [],
            "idx_agent_control_provider_admission_lease_deadline",
          ],
          [
            PROVIDER_ADMISSION_MINIMUM_WAITING_DEADLINE_AFTER_SQL,
            ["2100-01-01T00:00:00.000Z"],
            "idx_agent_control_provider_admission_deadline",
          ],
          [
            PROVIDER_ADMISSION_MINIMUM_ADMITTED_DEADLINE_AFTER_SQL,
            ["2100-01-01T00:00:00.000Z"],
            "idx_agent_control_provider_admission_lease_deadline",
          ],
          [
            PROVIDER_ADMISSION_DUE_WAITING_DEADLINES_SQL,
            ["2100-01-01T00:00:00.000Z"],
            "idx_agent_control_provider_admission_deadline",
          ],
          [
            PROVIDER_ADMISSION_DUE_ADMITTED_DEADLINES_SQL,
            ["2100-01-01T00:00:00.000Z"],
            "idx_agent_control_provider_admission_lease_deadline",
          ],
        ] as const) {
          const plan = yield* secondSql.sql.unsafe<{ readonly detail: string }>(
            `EXPLAIN QUERY PLAN ${statement}`,
            [...parameters],
          );
          assert.isTrue(plan.some((row) => row.detail.includes(index)));
          assert.isFalse(plan.some((row) => row.detail.includes("TEMP B-TREE")));
          assert.isFalse(plan.some((row) => row.detail.startsWith("SCAN ")));
        }
        for (const trigger of guardTriggers) {
          yield* firstSql.sql.unsafe(`DROP TRIGGER main."${trigger.name}"`).unprepared;
        }
        yield* firstSql.sql.withTransaction(
          firstSql.sql`
            DELETE FROM main.agent_control_initial_planning_deliveries
            WHERE provider_delivery_id=${originalRequest.providerDeliveryId}
          `,
        );
        for (const trigger of guardTriggers) {
          yield* firstSql.sql.unsafe(trigger.source).unprepared;
        }
        assert.deepStrictEqual(yield* secondSql.sql`PRAGMA main.foreign_key_check`, []);
        assert.equal((yield* secondSql.sql`PRAGMA main.integrity_check`)[0]?.integrity_check, "ok");
        yield* Scope.close(restarted.scope, Exit.void);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("reports a capacity pump defect through the typed runtime failure channel", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const usageEvents = yield* PubSub.unbounded<never>();
      const storeLayer = Layer.succeed(ProviderAdmissionStore, {
        resume: () => Effect.die("unused"),
        request: () => Effect.die("unused"),
        validateAndEnterInTransaction: () => Effect.die("unused"),
        quarantineIfEntered: () => Effect.die("unused"),
        admitOldest: () => Effect.die("injected-capacity-pump-defect"),
        quarantine: () => Effect.die("unused"),
        recordUsage: () => Effect.die("unused"),
        listWaiting: Effect.succeed([]),
        listDueDeadlines: () => Effect.succeed([]),
        listEnteredWithoutRelease: Effect.succeed([]),
        minimumDeadline: Effect.succeed(null),
        minimumDeadlineAfter: () => Effect.succeed(null),
        releaseFromFinalizationInTransaction: () => Effect.die("unused"),
        catchUpFinalized: Effect.succeed([]),
      });
      const usageLayer = Layer.succeed(ProviderUsage, {
        inspectForAdmission: () => Effect.die("unused"),
        getSnapshot: Effect.succeed([]),
        refresh: () => Effect.succeed({ refreshedAt: epoch, usage: [], failures: [] }),
        subscribeEvents: PubSub.subscribe(usageEvents),
      });
      const wakeupLayers = Layer.mergeAll(
        Layer.succeed(AgentControlInitialPlanningWakeup, {
          wake: () => Effect.void,
          stream: Stream.never,
        }),
        Layer.succeed(AgentControlImplementationTurnWakeup, {
          wake: () => Effect.void,
          stream: Stream.never,
        }),
        Layer.succeed(AgentControlVerificationTurnWakeup, {
          wake: () => Effect.void,
          stream: Stream.never,
          subscribe: Effect.succeed(Stream.never),
        }),
      );
      const runtimeScope = yield* Scope.make("sequential");
      yield* Effect.addFinalizer(() => Scope.close(runtimeScope, Exit.void));
      const runtimeContext = yield* Layer.buildWithScope(
        ProviderAdmissionRuntimeLive.pipe(
          Layer.provideMerge(storeLayer),
          Layer.provideMerge(usageLayer),
          Layer.provide(wakeupLayers),
        ),
        runtimeScope,
      );
      const runtime = Context.get(runtimeContext, ProviderAdmissionRuntime);

      yield* runtime.capacityReleased("defective-provider");
      const failure = yield* Effect.flip(runtime.awaitFailure);
      assert.instanceOf(failure, ProviderAdmissionError);
      assert.equal(failure.operation, "capacity-pump");
      assert.equal(failure.reason, "persistence");
    }),
  ).pipe(Effect.provide(NodeServices.layer)),
);
