import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import type { ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import { ProviderAdapterRequestError } from "../../../provider/Errors.ts";
import { ProviderService } from "../../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderTurnRequestExecutor } from "../../../orchestration/Services/ProviderTurnRequestExecutor.ts";
import type { AgentControlImplementationClaim } from "../model.ts";
import {
  AgentControlImplementationTurnConsumer,
  type AgentControlImplementationTurnConsumerShape,
} from "../Services/AgentControlImplementationTurnConsumer.ts";
import { AgentControlImplementationTurnConsumerHooks } from "../Services/AgentControlImplementationTurnConsumerHooks.ts";
import {
  AgentControlImplementationHandoffStore,
  isAgentControlImplementationCandidateEvidenceError,
  makeAgentControlImplementationCandidateEvidenceError,
} from "../Services/AgentControlImplementationHandoffStore.ts";
import { AgentControlImplementationTurnWakeup } from "../Services/AgentControlImplementationTurnWakeup.ts";
import { ProviderAdmissionRuntime } from "../../providerAdmission/Services/ProviderAdmissionRuntime.ts";
import type { ProviderAdmissionPermit } from "../../providerAdmission/model.ts";

const CLAIM_DURATION = Duration.minutes(2);
const RETRY_DELAY = Duration.seconds(30);

type ConsumerInput =
  | { readonly _tag: "handoff"; readonly handoffId: string }
  | { readonly _tag: "runtime"; readonly event: ProviderRuntimeEvent }
  | { readonly _tag: "recover" };

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const plus = (instant: DateTime.Utc, duration: Duration.Duration) =>
  DateTime.formatIso(DateTime.addDuration(instant, duration));

const hasExceptionalReasons = (cause: Cause.Cause<unknown>): boolean =>
  Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason);

const safeErrorCode = (cause: Cause.Cause<unknown>): string => {
  const squashed = Cause.squash(cause) as { readonly _tag?: string; readonly detail?: string };
  const detail = squashed.detail?.toLowerCase() ?? "";
  if (detail.includes("quota") || detail.includes("rate limit")) return "provider-quota";
  if (detail.includes("timeout") || detail.includes("timed out")) return "provider-timeout";
  if (
    squashed._tag === "ProviderValidationError" ||
    squashed._tag === "ProviderUnsupportedError" ||
    squashed._tag === "ProviderInstanceNotFoundError" ||
    squashed._tag === "ProviderAdapterValidationError"
  ) {
    return "session-incompatible";
  }
  return "transient-not-accepted";
};

const terminalState = (
  event: Extract<ProviderRuntimeEvent, { readonly type: "turn.completed" | "turn.aborted" }>,
): "completed" | "failed" | "interrupted" =>
  event.type === "turn.aborted"
    ? "failed"
    : event.payload.state === "completed"
      ? "completed"
      : event.payload.state === "interrupted" || event.payload.state === "cancelled"
        ? "interrupted"
        : "failed";

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const hooks = yield* AgentControlImplementationTurnConsumerHooks;
  const store = yield* AgentControlImplementationHandoffStore;
  const wakeup = yield* AgentControlImplementationTurnWakeup;
  const orchestration = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  const provider = yield* ProviderService;
  const executor = yield* ProviderTurnRequestExecutor;
  const providerAdmission = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderAdmissionRuntime),
  );
  const ownerId = yield* crypto.randomUUIDv4.pipe(
    Effect.orDie,
    Effect.map((uuid) => `implementation-consumer:${uuid}`),
  );

  const load = (handoffId: string) =>
    store.loadAcceptedByHandoffId(handoffId).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              makeAgentControlImplementationCandidateEvidenceError({
                handoffId,
                candidateReason: "base-candidate-missing",
                operation: "load-handoff-base",
              }),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const acceptTurn = Effect.fn("AgentControlImplementationTurnConsumer.acceptTurn")(function* (
    claim: AgentControlImplementationClaim,
  ) {
    const dispatch = orchestration.dispatchAgentControlImplementationTurn;
    if (dispatch === undefined) {
      return yield* Effect.die(new Error("Implementation turn dispatch is not installed."));
    }
    yield* dispatch(
      {
        type: "thread.turn.start",
        commandId: claim.evidence.turnRequestCommandId,
        threadId: claim.evidence.threadId,
        message: {
          messageId: claim.evidence.messageId,
          role: "user",
          text: claim.evidence.promptText,
          attachments: [],
        },
        modelSelection: claim.evidence.modelSelection,
        runtimeMode: claim.evidence.runtimeMode,
        interactionMode: "default",
        sourceProposedPlan: {
          threadId: claim.evidence.planningThreadId,
          planId: claim.evidence.planId,
        },
        createdAt: claim.evidence.createdAt,
      },
      {
        handoffId: claim.evidence.handoffId,
        handoffFingerprint: claim.evidence.handoffFingerprint,
        controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
        threadId: claim.evidence.threadId,
        planningThreadId: claim.evidence.planningThreadId,
        planId: claim.evidence.planId,
        turnRequestCommandId: claim.evidence.turnRequestCommandId,
        messageId: claim.evidence.messageId,
        messageEventId: claim.evidence.messageEventId,
        turnRequestEventId: claim.evidence.turnRequestEventId,
        messageEventTemplateJson: claim.evidence.messageEventTemplateJson,
        turnRequestEventTemplateJson: claim.evidence.turnRequestEventTemplateJson,
        eventTemplateDigest: claim.evidence.eventTemplateDigest,
      },
    );
    yield* hooks.afterTurnDispatchBeforeAcceptanceRead?.(claim.evidence.handoffId) ?? Effect.void;
    const accepted = yield* store.loadTurnAcceptance(claim.evidence.handoffId);
    if (Option.isNone(accepted)) {
      return yield* Effect.die(
        new Error("Implementation turn accepted without dedicated evidence."),
      );
    }
    const delivery = yield* store.markTurnAccepted(
      claim.evidence.handoffId,
      claim.delivery.revision,
      accepted.value.acceptedAt,
    );
    return { ...claim, delivery };
  });

  const scheduleRetry = Effect.fn("AgentControlImplementationTurnConsumer.scheduleRetry")(
    function* (
      claim: AgentControlImplementationClaim,
      cause: Cause.Cause<unknown>,
      definitelyRejected = false,
    ) {
      const at = yield* DateTime.now;
      yield* Effect.logWarning("Implementation provider delivery failed", {
        handoffId: claim.evidence.handoffId,
        cause,
      });
      const code = safeErrorCode(cause);
      if (code === "session-incompatible" && !definitelyRejected) {
        yield* store.markAmbiguous({
          handoffId: claim.evidence.handoffId,
          expectedRevision: claim.delivery.revision,
          terminalAt: DateTime.formatIso(at),
        });
        return;
      }
      yield* store.scheduleRetry({
        handoffId: claim.evidence.handoffId,
        ownerId,
        claimGeneration: claim.delivery.claimGeneration,
        expectedRevision: claim.delivery.revision,
        nextAttemptAt: plus(at, RETRY_DELAY),
        errorCode: code,
        updatedAt: DateTime.formatIso(at),
      });
    },
  );

  const reconcileAttempted = Effect.fn("AgentControlImplementationTurnConsumer.reconcileAttempted")(
    function* (claim: AgentControlImplementationClaim) {
      const sessions = yield* provider.listSessions();
      const active = sessions.find(
        (session) =>
          session.threadId === claim.evidence.threadId &&
          session.activeTurnId !== undefined &&
          session.providerInstanceId === claim.evidence.providerInstanceId &&
          session.runtimeMode === claim.evidence.runtimeMode &&
          session.cwd === claim.evidence.worktreePath &&
          session.model === claim.evidence.modelSelection.model,
      );
      const thread = yield* snapshots.getThreadDetailById(claim.evidence.threadId);
      if (
        active?.activeTurnId !== undefined &&
        Option.isSome(thread) &&
        Equal.equals(thread.value.modelSelection, claim.evidence.modelSelection) &&
        thread.value.worktreePath === claim.evidence.worktreePath
      ) {
        // Keep waiting for the provider start event; session.updatedAt is not its timestamp.
        return;
      }
      if (
        claim.delivery.claimExpiresAt !== null &&
        claim.delivery.claimExpiresAt > (yield* nowIso)
      ) {
        return;
      }
      yield* store.markAmbiguous({
        handoffId: claim.evidence.handoffId,
        expectedRevision: claim.delivery.revision,
        terminalAt: yield* nowIso,
      });
    },
  );

  const requestProviderAdmission = Effect.fn(
    "AgentControlImplementationTurnConsumer.requestProviderAdmission",
  )(function* (claim: AgentControlImplementationClaim) {
    if (providerAdmission === undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: String(claim.evidence.providerInstanceId),
        method: "provider-admission",
        detail: "Durable provider admission authority is unavailable.",
      });
    }
    return yield* providerAdmission
      .request({
        stage: "implementation",
        projectId: String(claim.evidence.projectId),
        taskId: claim.evidence.taskId,
        stageRunId: claim.evidence.stageRunId,
        attemptId: claim.evidence.attemptId,
        handoffId: claim.evidence.handoffId,
        providerDeliveryId: claim.evidence.providerDeliveryId,
        threadId: String(claim.evidence.threadId),
        providerInstanceId: claim.evidence.providerInstanceId,
        stageLeaseId: claim.evidence.leaseId,
        stageLeaseHolderId: claim.evidence.leaseHolderId,
        stageFenceToken: claim.evidence.fenceToken,
        modelSelection: claim.evidence.modelSelection,
        modelSelectionJson: claim.evidence.modelSelectionJson,
        modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
        requestedAt: claim.evidence.createdAt,
      })
      .pipe(Effect.orDie);
  });

  const deliver = Effect.fn("AgentControlImplementationTurnConsumer.deliver")(function* (
    claim: AgentControlImplementationClaim,
    providerAdmissionPermit: ProviderAdmissionPermit,
  ) {
    const current = yield* DateTime.now;
    yield* hooks.beforeClaim(claim.evidence.handoffId);
    const claimed = yield* store.claim({
      handoffId: claim.evidence.handoffId,
      ownerId,
      now: DateTime.formatIso(current),
      expiresAt: plus(current, CLAIM_DURATION),
    });
    if (Option.isNone(claimed)) return;
    const owned = claimed.value;
    yield* hooks.afterClaim(owned.evidence.handoffId);
    const prepared = yield* executor
      .prepareTurnDelivery({
        threadId: owned.evidence.threadId,
        messageText: owned.evidence.promptText,
        attachments: [],
        modelSelection: owned.evidence.modelSelection,
        interactionMode: "default",
        createdAt: DateTime.formatIso(current),
        providerDeliveryId: owned.evidence.providerDeliveryId,
        durableDeliveryKind: "implementation",
        providerAdmissionPermit,
      })
      .pipe(Effect.exit);
    if (Exit.isFailure(prepared)) {
      if (hasExceptionalReasons(prepared.cause)) return yield* Effect.failCause(prepared.cause);
      yield* scheduleRetry(owned, prepared.cause);
      return;
    }
    let attemptedRevision: number | undefined;
    const deliveryExit = yield* Effect.uninterruptibleMask((restore) =>
      restore(
        executor.sendPreparedTurnAtPreInvokeBoundary(prepared.value, {
          beforeDeliveryCas: () =>
            hooks.beforeDeliveryCas?.(owned.evidence.handoffId) ?? Effect.void,
          persistDeliveryAttempted: (attestation) => {
            const resumeCursorJson = prepared.value.sessionResumeCursorJson;
            const sessionAttestation = prepared.value.sessionAttestation;
            if (resumeCursorJson === undefined || sessionAttestation === undefined) {
              return Effect.fail(
                new ProviderAdapterRequestError({
                  provider: String(attestation.providerInstanceId),
                  method: "thread.turn.start",
                  detail: "Implementation resume correlation is unavailable.",
                }),
              );
            }
            return store
              .markDeliveryAttempted({
                providerDeliveryId: owned.evidence.providerDeliveryId,
                handoffId: owned.evidence.handoffId,
                ownerId,
                claimGeneration: owned.delivery.claimGeneration,
                expectedRevision: owned.delivery.revision,
                attemptedAt: DateTime.formatIso(current),
                providerSessionCreatedAt: sessionAttestation.sessionCreatedAt,
                providerResumeCursorJson: resumeCursorJson,
                providerInstanceId: String(attestation.providerInstanceId),
                turnModelSelectionJson: attestation.modelSelectionJson,
                turnModelSelectionFingerprint: attestation.modelSelectionFingerprint,
              })
              .pipe(
                Effect.tap((delivery) =>
                  Effect.sync(() => {
                    attemptedRevision = delivery.revision;
                  }),
                ),
                Effect.asVoid,
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: String(attestation.providerInstanceId),
                      method: "thread.turn.start",
                      detail: "Implementation delivery marker CAS failed.",
                      cause,
                    }),
                ),
              );
          },
          afterDeliveryCas: () => hooks.afterDeliveryCas?.(owned.evidence.handoffId) ?? Effect.void,
        }),
      ).pipe(Effect.exit),
    );
    if (Exit.isFailure(deliveryExit)) {
      const persisted = yield* load(owned.evidence.handoffId);
      if (
        persisted.delivery.state === "delivery-attempted" &&
        prepared.value.entryState?.externalOperationStarted === true
      ) {
        yield* store.markAmbiguous({
          handoffId: persisted.evidence.handoffId,
          expectedRevision: persisted.delivery.revision,
          terminalAt: yield* nowIso,
        });
      } else if (hasExceptionalReasons(deliveryExit.cause)) {
        return yield* Effect.failCause(deliveryExit.cause);
      } else if (
        persisted.delivery.state === "claimed" ||
        persisted.delivery.state === "delivery-attempted"
      ) {
        yield* scheduleRetry(
          persisted,
          deliveryExit.cause,
          persisted.delivery.state === "delivery-attempted",
        );
      }
      return;
    }
    const persisted = yield* load(owned.evidence.handoffId);
    if (
      persisted.delivery.state === "provider-started" &&
      persisted.delivery.providerTurnId === String(deliveryExit.value.result.turnId)
    ) {
      return;
    }
    if (
      persisted.delivery.state !== "delivery-attempted" ||
      attemptedRevision === undefined ||
      persisted.delivery.revision !== attemptedRevision
    ) {
      return yield* Effect.die(new Error("Implementation delivery evidence diverged."));
    }
    // Wait for turn.started to record the provider timestamp used by orchestration.
  });

  const processHandoff = Effect.fn("AgentControlImplementationTurnConsumer.processHandoff")(
    function* (handoffId: string) {
      let claim = yield* load(handoffId);
      if (
        claim.delivery.state === "completed" ||
        claim.delivery.state === "failed" ||
        claim.delivery.state === "interrupted" ||
        claim.delivery.state === "ambiguous" ||
        claim.delivery.state === "provider-started" ||
        claim.delivery.state === "interrupt-requested"
      ) {
        return;
      }
      if (claim.delivery.state === "delivery-attempted") {
        yield* reconcileAttempted(claim);
        return;
      }
      if (
        claim.delivery.state === "turn-accepted" ||
        claim.delivery.state === "retry-wait" ||
        claim.delivery.state === "pending" ||
        claim.delivery.state === "claimed"
      ) {
        const admission = yield* requestProviderAdmission(claim);
        if (admission._tag === "Waiting") return;
        if (claim.delivery.state === "pending") claim = yield* acceptTurn(claim);
        yield* deliver(claim, admission.permit);
      }
    },
  );

  const processRuntimeEvent = Effect.fn(
    "AgentControlImplementationTurnConsumer.processRuntimeEvent",
  )(function* (event: ProviderRuntimeEvent) {
    if (event.turnId === undefined) return;
    const claim = yield* store.loadAcceptedByThreadId(event.threadId);
    if (
      Option.isNone(claim) ||
      event.providerInstanceId !== claim.value.evidence.providerInstanceId
    ) {
      return;
    }
    if (event.type === "turn.started") {
      const observed = yield* store.observeProviderStarted({
        threadId: event.threadId,
        providerTurnId: String(event.turnId),
        acceptedAt: event.createdAt,
      });
      if (Option.isSome(observed)) yield* wakeup.wake(claim.value.evidence.handoffId);
      return;
    }
    if (event.type !== "turn.completed" && event.type !== "turn.aborted") return;
    const state = terminalState(event);
    const observed = yield* store.observeProviderTerminal({
      threadId: event.threadId,
      providerTurnId: String(event.turnId),
      state,
      terminalAt: event.createdAt,
      ...(state === "completed" ? {} : { errorCode: "provider-terminal" }),
    });
    if (Option.isSome(observed)) yield* wakeup.wake(claim.value.evidence.handoffId);
  });

  const recover = Effect.gen(function* () {
    const handoffIds = yield* store.listRecoverable(yield* nowIso);
    yield* Effect.forEach(
      handoffIds,
      (handoffId) =>
        processHandoff(handoffId).pipe(
          Effect.catchIf(isAgentControlImplementationCandidateEvidenceError, (cause) =>
            Effect.logError("implementation delivery candidate failed validation", {
              handoffId,
              operation: cause.operation,
              candidateReason: cause.candidateReason,
            }),
          ),
        ),
      { concurrency: 1, discard: true },
    );
  });
  const processSafely = (input: ConsumerInput) =>
    (input._tag === "handoff"
      ? processHandoff(input.handoffId)
      : input._tag === "runtime"
        ? processRuntimeEvent(input.event)
        : recover
    ).pipe(
      Effect.catchIf(isAgentControlImplementationCandidateEvidenceError, (cause) =>
        Effect.logError("implementation delivery candidate failed validation", {
          inputTag: input._tag,
          handoffId: cause.handoffId,
          operation: cause.operation,
          candidateReason: cause.candidateReason,
        }),
      ),
    );
  const worker = yield* makeDrainableWorker(processSafely);
  const start: AgentControlImplementationTurnConsumerShape["start"] = Effect.fn("start")(
    function* () {
      yield* Effect.forkScoped(
        Stream.runForEach(wakeup.stream, (handoffId) =>
          worker.enqueue({ _tag: "handoff", handoffId }),
        ),
      );
      yield* Effect.forkScoped(
        Stream.runForEach(provider.streamEvents, (event) =>
          worker.enqueue({ _tag: "runtime", event }),
        ),
      );
      yield* worker.enqueue({ _tag: "recover" });
    },
  );
  return AgentControlImplementationTurnConsumer.of({
    processHandoff,
    processRuntimeEvent,
    recover,
    start,
    drain: worker.drain,
  });
});

export const AgentControlImplementationTurnConsumerLive = Layer.effect(
  AgentControlImplementationTurnConsumer,
  make,
);
