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
import { runMigrations } from "../../../persistence/Migrations.ts";
import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";
import { ProviderUsage } from "../../../provider/Services/ProviderUsage.ts";
import type { ProviderAdmissionRequest } from "../model.ts";
import { ProviderAdmissionRuntime } from "../Services/ProviderAdmissionRuntime.ts";
import {
  ProviderAdmissionError,
  ProviderAdmissionStore,
} from "../Services/ProviderAdmissionStore.ts";
import { ProviderAdmissionRuntimeLive } from "./ProviderAdmissionRuntime.ts";
import { ProviderAdmissionStoreLive } from "./ProviderAdmissionStore.ts";
import { providerAdmissionUsageEvidence } from "../model.ts";

type Observation =
  | { readonly _tag: "Unsupported"; readonly observedAt: string }
  | { readonly _tag: "Observed"; readonly snapshot: ProviderUsageSnapshot }
  | { readonly _tag: "SupportedUnusable"; readonly observedAt: string };

const driver = ProviderDriverKind.make("codex");
const epoch = "1970-01-01T00:00:00.000Z";
const reset = "1970-01-01T00:00:01.000Z";

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
      const unsupportedId = ProviderInstanceId.make("usage-unsupported");
      const unusableId = ProviderInstanceId.make("usage-unusable");
      const observations = yield* Ref.make(
        new Map<ProviderInstanceId, Observation>([
          [allowedId, { _tag: "Observed", snapshot: snapshot(allowedId, "allowed") }],
          [warningId, { _tag: "Observed", snapshot: snapshot(warningId, "warning") }],
          [rejectedId, { _tag: "Observed", snapshot: snapshot(rejectedId, "rejected", reset) }],
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
      assert.equal(yield* store.minimumDeadline, null);
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

it.effect("reports a capacity pump defect through the typed runtime failure channel", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const usageEvents = yield* PubSub.unbounded<never>();
      const storeLayer = Layer.succeed(ProviderAdmissionStore, {
        resume: () => Effect.die("unused"),
        request: () => Effect.die("unused"),
        validateAndEnterInTransaction: () => Effect.die("unused"),
        admitOldest: () => Effect.die("injected-capacity-pump-defect"),
        quarantine: () => Effect.die("unused"),
        recordUsage: () => Effect.die("unused"),
        listWaiting: Effect.succeed([]),
        listEnteredWithoutRelease: Effect.succeed([]),
        minimumDeadline: Effect.succeed(null),
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
