import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
import { TurnId, type ProviderRuntimeEvent } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ProviderAdapterRequestError } from "../../../provider/Errors.ts";
import {
  ProviderService,
  type ProviderRuntimeEventDrainToken,
  type ProviderRuntimeEventPublication,
} from "../../../provider/Services/ProviderService.ts";
import {
  makeDurablePrefixOutcomeTracker,
  type DurablePrefixOutcomeTracker,
} from "../../../provider/runtimeEventPrefixOutcome.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderTurnRequestExecutor } from "../../../orchestration/Services/ProviderTurnRequestExecutor.ts";
import type {
  AgentControlVerificationClaim,
  AgentControlVerificationDeliveryErrorCode,
} from "../model.ts";
import {
  AgentControlVerificationOrchestrationHistoryError,
  loadVerificationTerminalFromOrchestrationHistory,
} from "../orchestrationTerminalHistory.ts";
import {
  AgentControlVerificationTurnConsumer,
  type AgentControlVerificationTurnConsumerShape,
} from "../Services/AgentControlVerificationTurnConsumer.ts";
import { AgentControlVerificationTurnConsumerHooks } from "../Services/AgentControlVerificationTurnConsumerHooks.ts";
import {
  AgentControlVerificationHandoffStore,
  AgentControlVerificationStoreError,
  isAgentControlVerificationCandidateEvidenceError,
  makeAgentControlVerificationCandidateEvidenceError,
} from "../Services/AgentControlVerificationHandoffStore.ts";
import { AgentControlVerificationTurnWakeup } from "../Services/AgentControlVerificationTurnWakeup.ts";
import { ProviderAdmissionRuntime } from "../../providerAdmission/Services/ProviderAdmissionRuntime.ts";
import type { ProviderAdmissionPermit } from "../../providerAdmission/model.ts";
import {
  normalizeVerificationTerminal,
  type VerificationTerminalObservation,
} from "../terminalObservation.ts";
import { AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION } from "../prompt.ts";
import { loadSealableVerificationResultSource } from "../orchestrationResultSource.ts";

const CLAIM_DURATION = Duration.minutes(2);
const RETRY_DELAY = Duration.seconds(30);
const RECOVERY_INTERVAL = Duration.seconds(5);
const isVerificationStoreError = Schema.is(AgentControlVerificationStoreError);
const isVerificationOrchestrationHistoryError = Schema.is(
  AgentControlVerificationOrchestrationHistoryError,
);

type ConsumerInput =
  | { readonly _tag: "handoff"; readonly handoffId: string }
  | { readonly _tag: "runtime"; readonly event: ProviderRuntimeEvent }
  | { readonly _tag: "recover" }
  | { readonly _tag: "provider-drain"; readonly token: ProviderRuntimeEventDrainToken };

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const plus = (instant: DateTime.Utc, duration: Duration.Duration) =>
  DateTime.formatIso(DateTime.addDuration(instant, duration));

const hasExceptionalReasons = (cause: Cause.Cause<unknown>): boolean =>
  Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason);

const safeErrorCodeForError = (
  error: unknown,
  depth = 0,
): AgentControlVerificationDeliveryErrorCode => {
  if (depth > 4 || typeof error !== "object" || error === null) {
    return "transient-not-accepted";
  }
  const failure = error as {
    readonly _tag?: string;
    readonly detail?: string;
    readonly cause?: unknown;
  };
  const detail = failure.detail?.toLowerCase() ?? "";
  if (detail.includes("quota") || detail.includes("rate limit")) return "provider-quota";
  if (detail.includes("timeout") || detail.includes("timed out")) return "provider-timeout";
  if (
    failure._tag === "ProviderValidationError" ||
    failure._tag === "ProviderUnsupportedError" ||
    failure._tag === "ProviderInstanceNotFoundError" ||
    failure._tag === "ProviderAdapterValidationError"
  ) {
    return "session-incompatible";
  }
  if (failure.cause !== undefined) return safeErrorCodeForError(failure.cause, depth + 1);
  return "transient-not-accepted";
};

const safeErrorCode = (cause: Cause.Cause<unknown>): AgentControlVerificationDeliveryErrorCode =>
  safeErrorCodeForError(Cause.squash(cause));

const safeCauseTag = (cause: Cause.Cause<unknown>): string => {
  const squashed = Cause.squash(cause);
  return typeof squashed === "object" && squashed !== null && "_tag" in squashed
    ? String(squashed._tag)
    : "UnknownError";
};

const safeStoreErrorContext = (cause: Cause.Cause<unknown>) => {
  const squashed = Cause.squash(cause);
  return isVerificationStoreError(squashed)
    ? { operation: squashed.operation, storeReason: squashed.reason }
    : {};
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const hooks = yield* AgentControlVerificationTurnConsumerHooks;
  const store = yield* AgentControlVerificationHandoffStore;
  const wakeup = yield* AgentControlVerificationTurnWakeup;
  const orchestration = yield* OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery;
  const provider = yield* ProviderService;
  const executor = yield* ProviderTurnRequestExecutor;
  const providerAdmission = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderAdmissionRuntime),
  );
  const ownerId = yield* crypto.randomUUIDv4.pipe(
    Effect.orDie,
    Effect.map((uuid) => `verification-consumer:${uuid}`),
  );
  const logOperationalFailure = (
    handoffId: string,
    phase: "prepare" | "delivery",
    cause: Cause.Cause<unknown>,
  ) =>
    Effect.logWarning("verification delivery operation failed", {
      handoffId,
      phase,
      errorCode: safeErrorCode(cause),
      errorTag: safeCauseTag(cause),
      cause,
    });

  const load = (handoffId: string) =>
    store.loadAcceptedByHandoffId(handoffId).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              makeAgentControlVerificationCandidateEvidenceError({
                handoffId,
                candidateReason: "base-candidate-missing",
                operation: "load-handoff-base",
              }),
            ),
          onSome: Effect.succeed,
        }),
      ),
    );

  const observeTerminal = Effect.fn("AgentControlVerificationTurnConsumer.observeTerminal")(
    function* (claim: AgentControlVerificationClaim, observation: VerificationTerminalObservation) {
      if (claim.delivery.providerTurnId === null || claim.delivery.providerAcceptedAt === null) {
        return yield* makeAgentControlVerificationCandidateEvidenceError({
          handoffId: claim.evidence.handoffId,
          candidateReason: "terminal-identity-divergent",
          operation: "provider-terminal-binding-missing",
        });
      }
      const result = yield* store.observeProviderTerminal({
        handoffId: claim.evidence.handoffId,
        providerDeliveryId: claim.evidence.providerDeliveryId,
        threadId: claim.evidence.threadId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId: TurnId.make(claim.delivery.providerTurnId),
        stageRunId: claim.evidence.stageRunId,
        attemptId: claim.evidence.attemptId,
        leaseId: claim.evidence.leaseId,
        leaseHolderId: claim.evidence.leaseHolderId,
        fenceToken: claim.evidence.fenceToken,
        modelSelectionFingerprint: claim.evidence.modelSelectionFingerprint,
        expectedRevision: claim.delivery.revision,
        observation,
        observedAt: yield* nowIso,
        beforeCas: hooks.beforeProviderTerminalCas?.(claim.evidence.handoffId) ?? Effect.void,
      });
      if (result._tag === "Observed") {
        yield* hooks.afterProviderTerminalCas?.(claim.evidence.handoffId) ?? Effect.void;
      }
      yield* wakeup.wake(claim.evidence.handoffId);
      return result;
    },
  );

  const reconcileTerminalHistory = Effect.fn(
    "AgentControlVerificationTurnConsumer.reconcileTerminalHistory",
  )(function* (claim: AgentControlVerificationClaim) {
    const acceptance = yield* store.loadTurnAcceptance(claim.evidence.handoffId);
    if (Option.isNone(acceptance)) {
      return yield* makeAgentControlVerificationCandidateEvidenceError({
        handoffId: claim.evidence.handoffId,
        candidateReason: "companion-missing",
        operation: "load-terminal-turn-acceptance",
      });
    }
    const recovered = yield* loadVerificationTerminalFromOrchestrationHistory(
      sql,
      claim,
      acceptance.value,
    ).pipe(
      Effect.mapError((cause) => {
        const historyError = isVerificationOrchestrationHistoryError(cause) ? cause : undefined;
        if (historyError?.reason === "persistence") {
          return new AgentControlVerificationStoreError({
            handoffId: claim.evidence.handoffId,
            operation: historyError.operation,
            reason: "persistence",
            cause,
          });
        }
        const candidateReason =
          historyError?.reason === "terminal-conflict"
            ? ("provider-terminal-conflict" as const)
            : historyError?.operation.includes("terminal-identity")
              ? ("terminal-identity-divergent" as const)
              : historyError?.operation.includes("projection") ||
                  historyError?.operation.includes("runtime-session")
                ? ("runtime-session-divergent" as const)
                : historyError?.operation.includes("decode") ||
                    historyError?.operation.includes("bytes") ||
                    historyError?.operation.includes("json")
                  ? ("orchestration-history-undecodable" as const)
                  : ("orchestration-history-divergent" as const);
        return makeAgentControlVerificationCandidateEvidenceError({
          handoffId: claim.evidence.handoffId,
          candidateReason,
          operation: historyError?.operation ?? "load-terminal-history",
          cause,
        });
      }),
    );
    if (recovered._tag === "Waiting") return;
    if (
      claim.evidence.templateVersion === AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION &&
      recovered.observation.deliveryState === "completed"
    ) {
      const seal = recovered.resultSourceSeal;
      if (seal === undefined) return;
      const source = yield* loadSealableVerificationResultSource(sql, {
        threadId: claim.evidence.threadId,
        providerInstanceId: claim.evidence.providerInstanceId,
        providerTurnId: seal.providerTurnId,
        afterStreamVersion: 4,
        sealedAtStreamVersion: recovered.terminalStreamVersion,
      }).pipe(
        Effect.mapError((cause) =>
          makeAgentControlVerificationCandidateEvidenceError({
            handoffId: claim.evidence.handoffId,
            candidateReason:
              cause.reason === "persistence"
                ? "orchestration-history-undecodable"
                : "orchestration-history-divergent",
            operation: cause.operation,
            cause,
          }),
        ),
      );
      if (
        seal.sourceDisposition !== source.sourceDisposition ||
        seal.finalMessageId !== source.finalMessageId ||
        seal.sourceEventId !== source.sourceEventId ||
        seal.outputDigest !== source.outputDigest ||
        seal.outputByteLength !== source.outputByteLength
      ) {
        return yield* makeAgentControlVerificationCandidateEvidenceError({
          handoffId: claim.evidence.handoffId,
          candidateReason: "orchestration-history-divergent",
          operation: "verification-result-source-seal-divergent",
        });
      }
    }
    yield* observeTerminal(claim, recovered.observation);
  });

  const acceptTurn = Effect.fn("AgentControlVerificationTurnConsumer.acceptTurn")(function* (
    claim: AgentControlVerificationClaim,
  ) {
    const dispatch = orchestration.dispatchAgentControlVerificationTurn;
    if (dispatch === undefined) {
      return yield* Effect.die(new Error("Verification turn dispatch is not installed."));
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
      return yield* Effect.die(new Error("Verification turn accepted without dedicated evidence."));
    }
    const delivery = yield* store.markTurnAccepted(
      claim.evidence.handoffId,
      claim.delivery.revision,
      accepted.value.acceptedAt,
    );
    return { ...claim, delivery };
  });

  const scheduleRetry = Effect.fn("AgentControlVerificationTurnConsumer.scheduleRetry")(function* (
    claim: AgentControlVerificationClaim,
    cause: Cause.Cause<unknown>,
  ) {
    const at = yield* DateTime.now;
    const code = safeErrorCode(cause);
    yield* store.scheduleRetry({
      handoffId: claim.evidence.handoffId,
      ownerId,
      claimGeneration: claim.delivery.claimGeneration,
      expectedRevision: claim.delivery.revision,
      nextAttemptAt: plus(at, RETRY_DELAY),
      errorCode: code,
      updatedAt: DateTime.formatIso(at),
    });
  });

  const reconcileAttempted = Effect.fn("AgentControlVerificationTurnConsumer.reconcileAttempted")(
    function* (claim: AgentControlVerificationClaim) {
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
    "AgentControlVerificationTurnConsumer.requestProviderAdmission",
  )(function* (claim: AgentControlVerificationClaim) {
    if (providerAdmission === undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: String(claim.evidence.providerInstanceId),
        method: "provider-admission",
        detail: "Durable provider admission authority is unavailable.",
      });
    }
    return yield* providerAdmission
      .request({
        stage: "verification",
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

  const deliver = Effect.fn("AgentControlVerificationTurnConsumer.deliver")(function* (
    claim: AgentControlVerificationClaim,
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
        durableDeliveryKind: "verification",
        providerAdmissionPermit,
      })
      .pipe(Effect.exit);
    if (Exit.isFailure(prepared)) {
      if (hasExceptionalReasons(prepared.cause)) return yield* Effect.failCause(prepared.cause);
      yield* scheduleRetry(owned, prepared.cause);
      yield* logOperationalFailure(owned.evidence.handoffId, "prepare", prepared.cause);
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
                  detail: "Verification resume correlation is unavailable.",
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
                      detail: "Verification delivery marker CAS failed.",
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
      const hasDefect = deliveryExit.cause.reasons.some(Cause.isDieReason);
      if (!hasDefect && Cause.hasInterrupts(deliveryExit.cause)) {
        return yield* Effect.failCause(deliveryExit.cause);
      }
      const persistedExit = yield* Effect.exit(
        hasDefect
          ? Effect.uninterruptible(load(owned.evidence.handoffId))
          : load(owned.evidence.handoffId),
      );
      if (Exit.isFailure(persistedExit)) {
        return yield* Effect.failCause(Cause.combine(deliveryExit.cause, persistedExit.cause));
      }
      const persisted = persistedExit.value;
      if (persisted.delivery.state === "delivery-attempted") {
        const markAmbiguous = nowIso.pipe(
          Effect.flatMap((terminalAt) =>
            store.markAmbiguous({
              handoffId: persisted.evidence.handoffId,
              expectedRevision: persisted.delivery.revision,
              terminalAt,
            }),
          ),
        );
        if (hasDefect) {
          const ambiguousExit = yield* Effect.exit(Effect.uninterruptible(markAmbiguous));
          if (Exit.isFailure(ambiguousExit)) {
            return yield* Effect.failCause(Cause.combine(deliveryExit.cause, ambiguousExit.cause));
          }
          return yield* Effect.failCause(deliveryExit.cause);
        }
        yield* markAmbiguous;
      } else if (hasDefect) {
        return yield* Effect.failCause(deliveryExit.cause);
      } else if (persisted.delivery.state === "claimed") {
        yield* scheduleRetry(persisted, deliveryExit.cause);
      }
      yield* logOperationalFailure(persisted.evidence.handoffId, "delivery", deliveryExit.cause);
      return;
    }
    const persisted = yield* load(owned.evidence.handoffId);
    if (
      ["provider-started", "completed", "failed", "interrupted"].includes(
        persisted.delivery.state,
      ) &&
      persisted.delivery.providerTurnId === String(deliveryExit.value.result.turnId)
    ) {
      return;
    }
    if (
      persisted.delivery.state !== "delivery-attempted" ||
      attemptedRevision === undefined ||
      persisted.delivery.revision !== attemptedRevision
    ) {
      return yield* Effect.die(new Error("Verification delivery evidence diverged."));
    }
    // Wait for turn.started to record the provider timestamp used by orchestration.
  });

  const processHandoff = Effect.fn("AgentControlVerificationTurnConsumer.processHandoff")(
    function* (handoffId: string) {
      let claim = yield* load(handoffId);
      if (claim.delivery.state === "ambiguous") {
        return;
      }
      if (claim.delivery.state === "provider-started") {
        yield* reconcileTerminalHistory(claim);
        return;
      }
      if (["completed", "failed", "interrupted"].includes(claim.delivery.state)) return;
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

  const processRuntimeEvent = Effect.fn("AgentControlVerificationTurnConsumer.processRuntimeEvent")(
    function* (event: ProviderRuntimeEvent) {
      if (
        event.type !== "turn.started" &&
        event.type !== "turn.completed" &&
        event.type !== "turn.aborted"
      ) {
        return;
      }
      const claim = yield* store.loadAcceptedByThreadId(event.threadId);
      if (
        Option.isNone(claim) ||
        event.providerInstanceId !== claim.value.evidence.providerInstanceId
      ) {
        return;
      }
      if (event.turnId === undefined) {
        if (event.type === "turn.completed" || event.type === "turn.aborted") {
          return yield* makeAgentControlVerificationCandidateEvidenceError({
            handoffId: claim.value.evidence.handoffId,
            candidateReason: "terminal-identity-divergent",
            operation: "provider-terminal-turn-id-missing",
          });
        }
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
      if (claim.value.delivery.providerTurnId !== String(event.turnId)) return;
      if (
        claim.value.delivery.state !== "provider-started" &&
        claim.value.delivery.state !== "completed" &&
        claim.value.delivery.state !== "failed" &&
        claim.value.delivery.state !== "interrupted"
      ) {
        return;
      }
      if (
        claim.value.evidence.templateVersion ===
          AGENT_CONTROL_VERIFICATION_PROMPT_TEMPLATE_VERSION &&
        event.type === "turn.completed" &&
        event.payload.state === "completed"
      ) {
        yield* reconcileTerminalHistory(claim.value);
        return;
      }
      const observation = yield* normalizeVerificationTerminal(event, {
        providerDeliveryId: claim.value.evidence.providerDeliveryId,
        threadId: claim.value.evidence.threadId,
        providerInstanceId: claim.value.evidence.providerInstanceId,
        providerTurnId: event.turnId,
      }).pipe(
        Effect.mapError((cause) =>
          makeAgentControlVerificationCandidateEvidenceError({
            handoffId: claim.value.evidence.handoffId,
            candidateReason: "terminal-identity-divergent",
            operation: "normalize-provider-terminal",
            cause,
          }),
        ),
      );
      yield* observeTerminal(claim.value, observation);
    },
  );

  const recoverWithPrefix = (prefixOutcome?: DurablePrefixOutcomeTracker) =>
    Effect.gen(function* () {
      const pageSize = hooks.recoveryPageSize ?? 64;
      const at = yield* nowIso;
      let cursor = "";
      while (true) {
        const handoffIds = yield* store.listRecoverable(at, cursor, pageSize);
        if (handoffIds.length === 0) break;
        yield* Effect.forEach(
          handoffIds,
          (handoffId) =>
            processHandoff(handoffId).pipe(
              Effect.catchIf(isAgentControlVerificationCandidateEvidenceError, (cause) =>
                (prefixOutcome === undefined
                  ? Effect.void
                  : prefixOutcome.recordIsolatedFailure(Cause.fail(cause))
                ).pipe(
                  Effect.andThen(
                    Effect.logError("verification delivery candidate failed validation", {
                      handoffId,
                      operation: cause.operation,
                      candidateReason: cause.candidateReason,
                    }),
                  ),
                ),
              ),
            ),
          { concurrency: 1, discard: true },
        );
        cursor = handoffIds.at(-1)!;
        if (handoffIds.length < pageSize) break;
      }
    });
  const recover = recoverWithPrefix();
  const processSafely = (
    input: ConsumerInput,
    prefixOutcome: DurablePrefixOutcomeTracker,
  ): Effect.Effect<void> =>
    (input._tag === "handoff"
      ? processHandoff(input.handoffId)
      : input._tag === "runtime"
        ? processRuntimeEvent(input.event)
        : input._tag === "recover"
          ? recoverWithPrefix(prefixOutcome)
          : Effect.gen(function* () {
              const stageDrainExit = yield* Effect.exit(wakeup.drainStageStarter ?? Effect.void);
              yield* prefixOutcome.acknowledge(
                input.token.verificationAcknowledgement,
                stageDrainExit,
              );
              if (Exit.isFailure(stageDrainExit)) {
                if (hasExceptionalReasons(stageDrainExit.cause)) {
                  return yield* Effect.failCause(stageDrainExit.cause as Cause.Cause<never>);
                }
                yield* prefixOutcome.recordIsolatedFailure(stageDrainExit.cause);
              }
            })
    ).pipe(
      Effect.catchIf(isAgentControlVerificationCandidateEvidenceError, (cause) =>
        (input._tag === "provider-drain"
          ? Effect.void
          : prefixOutcome.recordIsolatedFailure(Cause.fail(cause))
        ).pipe(
          Effect.andThen(
            Effect.logError("verification delivery candidate failed validation", {
              inputTag: input._tag,
              handoffId: cause.handoffId,
              operation: cause.operation,
              candidateReason: cause.candidateReason,
            }),
          ),
        ),
      ),
      Effect.catchCause((cause) => {
        if (hasExceptionalReasons(cause)) {
          return Effect.failCause(cause as Cause.Cause<never>);
        }
        return (
          input._tag === "provider-drain" ? Effect.void : prefixOutcome.recordIsolatedFailure(cause)
        ).pipe(
          Effect.andThen(
            Effect.logError("verification consumer input failed", {
              inputTag: input._tag,
              ...(input._tag === "handoff" ? { handoffId: input.handoffId } : {}),
              ...(input._tag === "runtime"
                ? { eventId: input.event.eventId, eventType: input.event.type }
                : {}),
              ...(input._tag === "provider-drain" ? { drainToken: input.token.id } : {}),
              errorTag: safeCauseTag(cause),
              ...safeStoreErrorContext(cause),
            }),
          ),
        );
      }),
    );
  let nextAttemptId = 0;
  let activeWorker:
    | {
        readonly attemptId: number;
        readonly drain: Effect.Effect<void>;
      }
    | undefined;

  const prepare: AgentControlVerificationTurnConsumerShape["prepare"] = Effect.fn(
    "AgentControlVerificationTurnConsumer.prepare",
  )(function* (providerEvents, activation, abortSignal) {
    const ownerScope = yield* Scope.Scope;
    const prefixOutcome = yield* makeDurablePrefixOutcomeTracker;
    const worker = yield* makeDrainableWorker(
      (input: ConsumerInput) => processSafely(input, prefixOutcome),
      { failureMode: "observable" },
    );
    const localActivation = activation === undefined ? yield* Deferred.make<void>() : undefined;
    const awaitActivation =
      localActivation === undefined ? activation! : Deferred.await(localActivation);
    const runAfterActivation = (effect: Effect.Effect<void>) =>
      abortSignal === undefined
        ? awaitActivation.pipe(Effect.andThen(effect))
        : Effect.raceFirst(awaitActivation.pipe(Effect.andThen(effect)), abortSignal);
    nextAttemptId += 1;
    const attemptId = nextAttemptId;
    activeWorker = { attemptId, drain: worker.drain };
    yield* Scope.addFinalizer(
      ownerScope,
      Effect.sync(() => {
        if (activeWorker?.attemptId === attemptId) activeWorker = undefined;
      }),
    );
    const wakeupPublications = yield* wakeup.subscribe;
    const runWakeupPump = Stream.runForEach(wakeupPublications, (handoffId) =>
      runAfterActivation(worker.enqueue({ _tag: "handoff", handoffId })),
    );
    yield* Effect.forkScoped(
      abortSignal === undefined ? runWakeupPump : Effect.raceFirst(runWakeupPump, abortSignal),
      { startImmediately: true },
    );
    const isLifecyclePublication = (
      value: ProviderRuntimeEventPublication | ProviderRuntimeEvent,
    ): value is ProviderRuntimeEventPublication =>
      "_tag" in value && (value._tag === "Event" || value._tag === "Drain");
    const runProviderPump = Stream.runForEach(
      providerEvents === undefined
        ? provider.streamEvents
        : Stream.fromSubscription(providerEvents),
      (publication) =>
        runAfterActivation(
          isLifecyclePublication(publication)
            ? publication._tag === "Event"
              ? worker.enqueue({ _tag: "runtime", event: publication.event })
              : worker.enqueue({ _tag: "provider-drain", token: publication.token })
            : worker.enqueue({ _tag: "runtime", event: publication }),
        ),
    );
    const providerPump = yield* Effect.forkScoped(
      abortSignal === undefined ? runProviderPump : Effect.raceFirst(runProviderPump, abortSignal),
      { startImmediately: true },
    );
    const recoveryLoop = awaitActivation.pipe(
      Effect.andThen(worker.enqueue({ _tag: "recover" })),
      Effect.andThen(
        Effect.forever(
          Effect.sleep(RECOVERY_INTERVAL).pipe(Effect.andThen(worker.enqueue({ _tag: "recover" }))),
        ),
      ),
    );
    yield* Effect.forkScoped(
      abortSignal === undefined ? recoveryLoop : Effect.raceFirst(recoveryLoop, abortSignal),
      { startImmediately: true },
    );
    return {
      commit:
        localActivation === undefined
          ? Effect.void
          : Deferred.succeed(localActivation, undefined).pipe(Effect.asVoid),
      drain: worker.drain,
      drainProviderEvents: (token) =>
        Effect.raceFirst(
          Deferred.await(token.verificationAcknowledgement),
          Effect.raceFirst(
            worker.awaitTermination,
            Fiber.await(providerPump).pipe(
              Effect.flatMap((exit) =>
                Exit.isFailure(exit)
                  ? Effect.failCause(exit.cause)
                  : Effect.die(
                      "Verification provider subscription ended before acknowledging its drain marker.",
                    ),
              ),
            ),
          ),
        ),
    };
  });
  const start: AgentControlVerificationTurnConsumerShape["start"] = Effect.fn(
    "AgentControlVerificationTurnConsumer.start",
  )(function* (providerEvents) {
    const activation = yield* prepare(providerEvents);
    yield* activation.commit;
  });
  return AgentControlVerificationTurnConsumer.of({
    processHandoff,
    processRuntimeEvent,
    recover,
    subscribeProviderEvents:
      provider.subscribeRuntimeEventPublications ??
      provider.subscribeEvents ??
      Effect.die("Verification provider subscription acquisition is unavailable."),
    prepare,
    start,
    drain: Effect.suspend(() => activeWorker?.drain ?? Effect.void),
  });
});

export const AgentControlVerificationTurnConsumerLive = Layer.effect(
  AgentControlVerificationTurnConsumer,
  make,
);
