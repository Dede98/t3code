import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
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
        CREATE TEMP TABLE agent_control_verification_handoff_intents (
          handoff_id,
          prompt_template_version
        )
      `;
        yield* sql`
        INSERT INTO temp.agent_control_verification_handoff_intents (
          handoff_id, prompt_template_version
        ) VALUES
          (CAST('corrupt-routing' AS BLOB), 'agent-control-verification-prompt-v2'),
          ('bad-candidate', 'agent-control-verification-prompt-v2'),
          ('healthy-candidate', 'agent-control-verification-prompt-v2')
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
              Layer.merge(
                Layer.succeed(SqlClient.SqlClient, sql),
                Layer.succeed(AgentControlVerificationHandoffStore, recoveryStore),
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
