import { assert, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as NodeSqliteClient from "../../../persistence/NodeSqliteClient.ts";
import {
  deriveVerificationEvaluationEvidenceId,
  deriveVerificationEvaluationId,
  deriveVerificationEvaluationMarkerId,
  deriveVerificationEvaluationReceiptId,
} from "../identity.ts";
import {
  AgentControlVerificationHandoffStore,
  AgentControlVerificationStoreError,
  type AgentControlVerificationHandoffStoreShape,
} from "../Services/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationEvaluator } from "../Services/AgentControlVerificationEvaluator.ts";
import { AgentControlVerificationEvaluatorHooks } from "../Services/AgentControlVerificationEvaluatorHooks.ts";
import { AgentControlVerificationEvaluatorLive } from "./AgentControlVerificationEvaluator.ts";

const unavailable = () => Effect.die(new Error("unexpected verification store operation"));
const store = AgentControlVerificationHandoffStore.of({
  loadAcceptedByHandoffId: () => Effect.succeed(Option.none()),
  insertAcceptedInTransaction: unavailable,
  loadAcceptedByTurnRequestCommandId: unavailable,
  loadAcceptedByThreadId: unavailable,
  listRecoverable: unavailable,
  isHandoffOwnedTurnRequest: unavailable,
  loadTurnAcceptance: unavailable,
  markTurnAccepted: unavailable,
  claim: unavailable,
  markDeliveryAttempted: unavailable,
  markProviderStarted: unavailable,
  scheduleRetry: unavailable,
  markAmbiguous: unavailable,
  observeProviderStarted: unavailable,
  observeProviderTerminal: unavailable,
  listStageStartCandidates: unavailable,
} as AgentControlVerificationHandoffStoreShape);

const evaluatorLayer = AgentControlVerificationEvaluatorLive.pipe(
  Layer.provide(
    Layer.merge(
      NodeSqliteClient.layerMemory(),
      Layer.succeed(AgentControlVerificationHandoffStore, store),
    ),
  ),
);
const layer = it.layer(evaluatorLayer);

it("derives one verdict-independent Evaluation identity and deterministic companions", () => {
  const authority = {
    providerDeliveryId: "verification-delivery",
    providerInstanceId: "codex",
    providerTurnId: "provider-turn",
    resultSchemaFingerprint: "f".repeat(64),
  };
  const evaluationId = deriveVerificationEvaluationId(authority);
  assert.equal(deriveVerificationEvaluationId({ ...authority }), evaluationId);
  assert.match(evaluationId, /^verification-evaluation-[0-9a-f]{64}$/u);
  assert.match(
    deriveVerificationEvaluationEvidenceId(evaluationId),
    /^verification-evaluation-evidence-[0-9a-f]{64}$/u,
  );
  assert.match(
    deriveVerificationEvaluationReceiptId(evaluationId),
    /^verification-evaluation-receipt-[0-9a-f]{64}$/u,
  );
  assert.match(
    deriveVerificationEvaluationMarkerId(evaluationId),
    /^verification-evaluation-marker-[0-9a-f]{64}$/u,
  );
});

it("requires an explicit evaluator layer instead of installing a global no-op", () => {
  const missingEvaluatorContext =
    Context.empty() as Context.Context<AgentControlVerificationEvaluator>;
  assert.isTrue(
    Option.isNone(Context.getOption(missingEvaluatorContext, AgentControlVerificationEvaluator)),
  );
  assert.throws(() => Context.get(missingEvaluatorContext, AgentControlVerificationEvaluator));
});

layer("AgentControlVerificationEvaluator", (it) => {
  it.effect("waits without DML when the durable handoff authority is absent", () =>
    Effect.gen(function* () {
      const evaluator = yield* AgentControlVerificationEvaluator;
      assert.deepStrictEqual(yield* evaluator.processHandoff("missing-handoff"), {
        _tag: "Waiting",
      });
    }),
  );
});

it.effect(
  "isolates corrupt recovery routing and a typed candidate before a healthy successor",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sqlContext = yield* Layer.build(NodeSqliteClient.layerMemory());
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        yield* sql`
          CREATE TABLE main.agent_control_verification_handoff_intents AS
          SELECT CAST('corrupt-routing' AS BLOB) AS handoff_id,
            'agent-control-verification-prompt-v2' AS prompt_template_version
          UNION ALL SELECT 'bad-candidate', 'agent-control-verification-prompt-v2'
          UNION ALL SELECT 'healthy-candidate', 'agent-control-verification-prompt-v2'
        `;
        const calls = yield* Ref.make<Array<string>>([]);
        const sensitiveCause = "SECRET_REPORT_AND_PROVIDER_PAYLOAD";
        const recoveryStore = AgentControlVerificationHandoffStore.of({
          ...store,
          loadAcceptedByHandoffId: (handoffId) =>
            Ref.update(calls, (entries) => [...entries, handoffId]).pipe(
              Effect.andThen(
                handoffId === "bad-candidate"
                  ? Effect.fail(
                      new AgentControlVerificationStoreError({
                        operation: "load-corrupt-candidate",
                        reason: "candidate-evidence",
                        handoffId,
                        candidateReason: "evidence-divergent",
                        cause: new Error(sensitiveCause),
                      }),
                    )
                  : Effect.succeed(Option.none()),
              ),
            ),
        });
        const evaluatorContext = yield* Layer.build(
          AgentControlVerificationEvaluatorLive.pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(SqlClient.SqlClient, sql),
                Layer.succeed(AgentControlVerificationHandoffStore, recoveryStore),
                Layer.succeed(
                  AgentControlVerificationEvaluatorHooks,
                  AgentControlVerificationEvaluatorHooks.of({
                    afterSourceLoad: () => Effect.void,
                    afterEvidence: () => Effect.void,
                    afterReceipt: () => Effect.void,
                    afterCommit: () => Effect.void,
                    recoveryPageSize: 2,
                  }),
                ),
              ),
            ),
          ),
        );
        const evaluator = Context.get(evaluatorContext, AgentControlVerificationEvaluator);
        const messages: Array<unknown> = [];
        const logger = Logger.make<unknown, void>(({ message }) => {
          if (Array.isArray(message)) messages.push(...message);
          else messages.push(message);
        });
        yield* evaluator.recover.pipe(
          Effect.provide(Logger.layer([logger], { mergeWithExisting: false })),
        );
        assert.deepStrictEqual(yield* Ref.get(calls), ["bad-candidate", "healthy-candidate"]);
        const containsSensitive = (value: unknown): boolean =>
          typeof value === "string"
            ? value.includes(sensitiveCause)
            : Array.isArray(value)
              ? value.some(containsSensitive)
              : typeof value === "object" && value !== null
                ? Object.values(value).some(containsSensitive)
                : false;
        assert.isFalse(messages.some(containsSensitive));
        assert.isTrue(
          messages.some(
            (message) =>
              typeof message === "object" &&
              message !== null &&
              "operation" in message &&
              message.operation === "load-evaluation-claim" &&
              "reason" in message &&
              message.reason === "persistence",
          ),
        );
      }),
    ),
);

it.effect.each(["defect", "interrupt"] as const)(
  "propagates an evaluator recovery %s without candidate isolation",
  (failureKind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const sqlContext = yield* Layer.build(NodeSqliteClient.layerMemory());
        const sql = Context.get(sqlContext, SqlClient.SqlClient);
        yield* sql`
          CREATE TABLE main.agent_control_verification_handoff_intents AS
          SELECT 'fatal-candidate' AS handoff_id,
            'agent-control-verification-prompt-v2' AS prompt_template_version
        `;
        const fatalStore = AgentControlVerificationHandoffStore.of({
          ...store,
          loadAcceptedByHandoffId: () =>
            failureKind === "defect" ? Effect.die("fatal-evaluator-defect") : Effect.interrupt,
        });
        const evaluatorContext = yield* Layer.build(
          AgentControlVerificationEvaluatorLive.pipe(
            Layer.provide(
              Layer.merge(
                Layer.succeed(SqlClient.SqlClient, sql),
                Layer.succeed(AgentControlVerificationHandoffStore, fatalStore),
              ),
            ),
          ),
        );
        const evaluator = Context.get(evaluatorContext, AgentControlVerificationEvaluator);
        const exit = yield* Effect.exit(evaluator.recover);
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isSuccess(exit)) return;
        assert.equal(
          failureKind === "defect" ? Cause.hasDies(exit.cause) : Cause.hasInterrupts(exit.cause),
          true,
        );
      }),
    ),
);

it.effect("closes a blocked evaluation worker and creates a fresh worker on retry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const sqlContext = yield* Layer.build(NodeSqliteClient.layerMemory());
      const sql = Context.get(sqlContext, SqlClient.SqlClient);
      yield* sql`
        CREATE TABLE main.agent_control_verification_handoff_intents AS
        SELECT 'blocked-candidate' AS handoff_id,
          'agent-control-verification-prompt-v2' AS prompt_template_version
      `;
      const firstEntered = yield* Deferred.make<void>();
      const retryEntered = yield* Deferred.make<void>();
      const calls = yield* Ref.make(0);
      const blockedFinalizers = yield* Ref.make(0);
      const blockingStore = AgentControlVerificationHandoffStore.of({
        ...store,
        loadAcceptedByHandoffId: () =>
          Ref.getAndUpdate(calls, (count) => count + 1).pipe(
            Effect.flatMap((call) =>
              call === 0
                ? Deferred.succeed(firstEntered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(Ref.update(blockedFinalizers, (count) => count + 1)),
                  )
                : Deferred.succeed(retryEntered, undefined).pipe(
                    Effect.andThen(Effect.succeed(Option.none())),
                  ),
            ),
          ),
      });
      const evaluatorContext = yield* Layer.build(
        AgentControlVerificationEvaluatorLive.pipe(
          Layer.provide(
            Layer.merge(
              Layer.succeed(SqlClient.SqlClient, sql),
              Layer.succeed(AgentControlVerificationHandoffStore, blockingStore),
            ),
          ),
        ),
      );
      const evaluator = Context.get(evaluatorContext, AgentControlVerificationEvaluator);

      const firstOwner = yield* Scope.make("sequential");
      yield* evaluator.prepare(Effect.void).pipe(Scope.provide(firstOwner));
      yield* Deferred.await(firstEntered);
      const firstClose = yield* Scope.close(firstOwner, Exit.void).pipe(Effect.forkChild);
      assert.isTrue(Exit.isSuccess(yield* Effect.exit(Fiber.join(firstClose))));
      assert.equal(yield* Ref.get(blockedFinalizers), 1);

      const retryOwner = yield* Scope.make("sequential");
      yield* evaluator.prepare(Effect.void).pipe(Scope.provide(retryOwner));
      yield* Deferred.await(retryEntered);
      yield* evaluator.drain;
      assert.equal(yield* Ref.get(calls), 2);
      assert.isTrue(Exit.isSuccess(yield* Effect.exit(Scope.close(retryOwner, Exit.void))));
      assert.equal(yield* Ref.get(blockedFinalizers), 1);
    }),
  ),
);
