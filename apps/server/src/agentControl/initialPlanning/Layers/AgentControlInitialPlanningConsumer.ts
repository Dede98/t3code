import { CommandId, type ProviderRuntimeEvent, TurnId } from "@t3tools/contracts";
import { makeDrainableWorker } from "@t3tools/shared/DrainableWorker";
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

import { ProviderService } from "../../../provider/Services/ProviderService.ts";
import { ProviderAdapterRequestError } from "../../../provider/Errors.ts";
import { ProviderSessionRuntimeRepository } from "../../../persistence/ProviderSessionRuntime.ts";
import { ProjectionTurnRepository } from "../../../persistence/Services/ProjectionTurns.ts";
import { OrchestrationEngineService } from "../../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderTurnRequestExecutor } from "../../../orchestration/Services/ProviderTurnRequestExecutor.ts";
import type {
  AgentControlInitialPlanningClaim,
  AgentControlInitialPlanningDelivery,
} from "../model.ts";
import {
  AgentControlInitialPlanningConsumer,
  type AgentControlInitialPlanningConsumerShape,
} from "../Services/AgentControlInitialPlanningConsumer.ts";
import { AgentControlInitialPlanningConsumerHooks } from "../Services/AgentControlInitialPlanningConsumerHooks.ts";
import { AgentControlInitialPlanningHandoffStore } from "../Services/AgentControlInitialPlanningHandoffStore.ts";
import { AgentControlInitialPlanningWakeup } from "../Services/AgentControlInitialPlanningWakeup.ts";
import { ProviderAdmissionRuntime } from "../../providerAdmission/Services/ProviderAdmissionRuntime.ts";
import type { ProviderAdmissionPermit } from "../../providerAdmission/model.ts";
import { canonicalProviderModelSelectionEvidence } from "../../../provider/Services/ProviderAdapter.ts";

const CLAIM_DURATION = Duration.minutes(2);
const RETRY_DELAY = Duration.seconds(30);
const RECOVERY_INTERVAL = Duration.seconds(5);

type ConsumerInput =
  | { readonly _tag: "handoff"; readonly handoffId: string }
  | { readonly _tag: "runtime"; readonly event: ProviderRuntimeEvent }
  | { readonly _tag: "recover" };

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const plus = (instant: DateTime.Utc, duration: Duration.Duration) =>
  DateTime.formatIso(DateTime.addDuration(instant, duration));

const terminalDeliveryState = (
  state: string,
): "completed" | "failed" | "interrupted" | undefined => {
  switch (state) {
    case "completed":
      return "completed";
    case "interrupted":
    case "cancelled":
      return "interrupted";
    case "error":
    case "failed":
      return "failed";
    default:
      return undefined;
  }
};

const safeErrorCode = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause) as { readonly _tag?: string; readonly detail?: string };
  const tag = error?._tag;
  const detail = error?.detail?.toLowerCase() ?? "";
  if (detail.includes("quota") || detail.includes("rate limit")) return "provider-quota";
  if (detail.includes("timeout") || detail.includes("timed out")) return "provider-timeout";
  if (
    tag === "ProviderValidationError" ||
    tag === "ProviderUnsupportedError" ||
    tag === "ProviderInstanceNotFoundError" ||
    tag === "ProviderAdapterValidationError"
  ) {
    return "session-incompatible";
  }
  return "transient-not-accepted";
};

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const hooks = yield* AgentControlInitialPlanningConsumerHooks;
  const store = yield* AgentControlInitialPlanningHandoffStore;
  const wakeup = yield* AgentControlInitialPlanningWakeup;
  const orchestrationEngine = yield* OrchestrationEngineService;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const projectionTurnRepository = yield* ProjectionTurnRepository;
  const providerRuntimeRepository = yield* ProviderSessionRuntimeRepository;
  const providerService = yield* ProviderService;
  const turnRequestExecutor = yield* ProviderTurnRequestExecutor;
  const providerAdmission = Option.getOrUndefined(
    yield* Effect.serviceOption(ProviderAdmissionRuntime),
  );
  const ownerId = yield* crypto.randomUUIDv4.pipe(
    Effect.orDie,
    Effect.map((uuid) => `initial-planning-consumer:${uuid}`),
  );

  const load = (handoffId: string) =>
    store.loadAcceptedByHandoffId(handoffId).pipe(
      Effect.flatMap(
        Option.match({
          onNone: () => Effect.die(new Error(`Unknown Initial Planning handoff '${handoffId}'.`)),
          onSome: Effect.succeed,
        }),
      ),
    );

  const settleThreadProjection = Effect.fn(
    "AgentControlInitialPlanningConsumer.settleThreadProjection",
  )(function* (
    claim: AgentControlInitialPlanningClaim,
    state: "completed" | "failed" | "interrupted",
    at: string,
  ) {
    const threadOption = yield* projectionSnapshotQuery.getThreadDetailById(
      claim.evidence.threadId,
    );
    if (Option.isSome(threadOption)) {
      const thread = threadOption.value;
      const current = thread.session;
      yield* orchestrationEngine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make(
          `server:initial-planning-terminal:${claim.evidence.handoffId}:${state}`,
        ),
        threadId: thread.id,
        session: {
          threadId: thread.id,
          status:
            state === "completed" ? "ready" : state === "interrupted" ? "interrupted" : "error",
          providerName: current?.providerName ?? null,
          providerInstanceId: current?.providerInstanceId ?? claim.evidence.providerInstanceId,
          runtimeMode: claim.evidence.runtimeMode,
          activeTurnId: null,
          lastError:
            state === "completed"
              ? null
              : state === "interrupted"
                ? "Initial planning interrupted"
                : "Initial planning failed",
          updatedAt: at,
        },
        createdAt: at,
      });
    }
    yield* projectionTurnRepository.deletePendingTurnStartByThreadId({
      threadId: claim.evidence.threadId,
    });
  });

  const markTerminal = Effect.fn("AgentControlInitialPlanningConsumer.markTerminal")(function* (
    claim: AgentControlInitialPlanningClaim,
    state: "completed" | "failed" | "interrupted",
    at: string,
    errorCode?: string,
  ) {
    const delivery = yield* store.markTerminal({
      handoffId: claim.evidence.handoffId,
      expectedRevision: claim.delivery.revision,
      state,
      terminalAt: at,
      ...(errorCode === undefined ? {} : { errorCode }),
    });
    yield* settleThreadProjection({ ...claim, delivery }, state, at);
    yield* wakeup.wake(claim.evidence.handoffId);
    return delivery;
  });

  const markAmbiguousAndSettle = Effect.fn(
    "AgentControlInitialPlanningConsumer.markAmbiguousAndSettle",
  )(function* (claim: AgentControlInitialPlanningClaim, at: string) {
    const delivery = yield* store.markAmbiguous({
      handoffId: claim.evidence.handoffId,
      expectedRevision: claim.delivery.revision,
      terminalAt: at,
    });
    yield* settleThreadProjection({ ...claim, delivery }, "failed", at);
    yield* wakeup.wake(claim.evidence.handoffId);
    return delivery;
  });

  const reconcileProviderStarted = Effect.fn(
    "AgentControlInitialPlanningConsumer.reconcileProviderStarted",
  )(function* (claim: AgentControlInitialPlanningClaim) {
    const providerTurnId = claim.delivery.providerTurnId;
    if (providerTurnId === null) {
      return yield* markAmbiguousAndSettle(claim, yield* nowIso);
    }
    const turn = yield* projectionTurnRepository.getByTurnId({
      threadId: claim.evidence.threadId,
      turnId: TurnId.make(providerTurnId),
    });
    if (Option.isSome(turn)) {
      const terminal = terminalDeliveryState(turn.value.state);
      if (terminal !== undefined) {
        const at = turn.value.completedAt ?? (yield* nowIso);
        return yield* markTerminal(
          claim,
          terminal,
          at,
          terminal === "completed"
            ? undefined
            : terminal === "interrupted"
              ? "provider-aborted"
              : "provider-defect",
        );
      }
    }
    const sessions = yield* providerService.listSessions();
    const active = sessions.find((session) => session.threadId === claim.evidence.threadId);
    if (
      active?.activeTurnId === providerTurnId &&
      active.providerInstanceId === claim.evidence.providerInstanceId &&
      active.runtimeMode === claim.evidence.runtimeMode &&
      active.cwd === claim.evidence.worktreePath &&
      active.model === claim.evidence.modelSelection.model
    ) {
      return claim.delivery;
    }
    const runtime = yield* providerRuntimeRepository.getByThreadId({
      threadId: claim.evidence.threadId,
    });
    if (
      Option.isSome(runtime) &&
      runtime.value.providerInstanceId === claim.evidence.providerInstanceId &&
      runtime.value.runtimeMode === claim.evidence.runtimeMode &&
      runtime.value.status === "running"
    ) {
      return claim.delivery;
    }
    return yield* markAmbiguousAndSettle(claim, yield* nowIso);
  });

  const reconcileAttempted = Effect.fn("AgentControlInitialPlanningConsumer.reconcileAttempted")(
    function* (claim: AgentControlInitialPlanningClaim) {
      const sessions = yield* providerService.listSessions();
      const active = sessions.find(
        (session) =>
          session.threadId === claim.evidence.threadId &&
          session.activeTurnId !== undefined &&
          session.providerInstanceId === claim.evidence.providerInstanceId &&
          session.runtimeMode === claim.evidence.runtimeMode &&
          session.cwd === claim.evidence.worktreePath &&
          session.model === claim.evidence.modelSelection.model,
      );
      const thread = yield* projectionSnapshotQuery.getThreadDetailById(claim.evidence.threadId);
      if (
        active?.activeTurnId !== undefined &&
        Option.isSome(thread) &&
        Equal.equals(thread.value.modelSelection, claim.evidence.modelSelection) &&
        thread.value.worktreePath === claim.evidence.worktreePath
      ) {
        return yield* store
          .observeProviderStarted({
            threadId: claim.evidence.threadId,
            providerTurnId: String(active.activeTurnId),
            acceptedAt: active.updatedAt,
          })
          .pipe(
            Effect.tap((observed) =>
              Option.isSome(observed) ? wakeup.wake(claim.evidence.handoffId) : Effect.void,
            ),
          );
      }
      if (
        claim.delivery.claimExpiresAt !== null &&
        claim.delivery.claimExpiresAt > (yield* nowIso)
      ) {
        return Option.none<AgentControlInitialPlanningDelivery>();
      }
      return Option.some(yield* markAmbiguousAndSettle(claim, yield* nowIso));
    },
  );

  const acceptTurn = Effect.fn("AgentControlInitialPlanningConsumer.acceptTurn")(function* (
    claim: AgentControlInitialPlanningClaim,
  ) {
    const dispatch = orchestrationEngine.dispatchAgentControlInitialPlanningTurn;
    if (dispatch === undefined) {
      return yield* Effect.die(new Error("Initial Planning dispatch is not installed."));
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
        interactionMode: "plan",
        createdAt: claim.evidence.createdAt,
      },
      {
        handoffId: claim.evidence.handoffId,
        handoffFingerprint: claim.evidence.handoffFingerprint,
        controlledThreadReservationId: claim.evidence.controlledThreadReservationId,
        threadId: claim.evidence.threadId,
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
    const acceptance = yield* store.loadTurnAcceptance(claim.evidence.handoffId);
    if (Option.isNone(acceptance)) {
      return yield* Effect.die(
        new Error("Initial Planning turn accepted without dedicated evidence."),
      );
    }
    const delivery = yield* store.markTurnAccepted(
      claim.evidence.handoffId,
      claim.delivery.revision,
      acceptance.value.acceptedAt,
    );
    return { ...claim, delivery };
  });

  const observeDeliveryFailure = (
    claim: AgentControlInitialPlanningClaim,
    cause: Cause.Cause<unknown>,
  ) =>
    hooks.beforeRetryClassification?.({
      handoffId: claim.evidence.handoffId,
      cause,
    }) ?? Effect.void;

  const schedulePreDeliveryFailure = Effect.fn(
    "AgentControlInitialPlanningConsumer.schedulePreDeliveryFailure",
  )(function* (
    claim: AgentControlInitialPlanningClaim,
    cause: Cause.Cause<unknown>,
    definitelyRejected = false,
  ) {
    const at = yield* DateTime.now;
    const errorCode = safeErrorCode(cause);
    if (errorCode === "session-incompatible" && !definitelyRejected) {
      yield* markTerminal(claim, "failed", DateTime.formatIso(at), errorCode);
      return;
    }
    yield* store.scheduleRetry({
      handoffId: claim.evidence.handoffId,
      ownerId,
      claimGeneration: claim.delivery.claimGeneration,
      expectedRevision: claim.delivery.revision,
      nextAttemptAt: plus(at, RETRY_DELAY),
      errorCode,
      updatedAt: DateTime.formatIso(at),
    });
  });

  const hasExceptionalReasons = (cause: Cause.Cause<unknown>): boolean =>
    Cause.hasInterrupts(cause) || cause.reasons.some(Cause.isDieReason);

  const requestProviderAdmission = Effect.fn(
    "AgentControlInitialPlanningConsumer.requestProviderAdmission",
  )(function* (claim: AgentControlInitialPlanningClaim) {
    const modelEvidence = canonicalProviderModelSelectionEvidence(claim.evidence.modelSelection);
    if (providerAdmission === undefined) {
      return yield* new ProviderAdapterRequestError({
        provider: String(claim.evidence.providerInstanceId),
        method: "provider-admission",
        detail: "Durable provider admission authority is unavailable.",
      });
    }
    return yield* providerAdmission
      .request({
        stage: "initial-planning",
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
        modelSelectionJson: modelEvidence.modelSelectionJson,
        modelSelectionFingerprint: modelEvidence.modelSelectionFingerprint,
        requestedAt: claim.evidence.createdAt,
      })
      .pipe(Effect.orDie);
  });

  const deliver = Effect.fn("AgentControlInitialPlanningConsumer.deliver")(function* (
    claim: AgentControlInitialPlanningClaim,
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

    const deliveryInput = {
      threadId: owned.evidence.threadId,
      messageText: owned.evidence.promptText,
      attachments: [],
      modelSelection: owned.evidence.modelSelection,
      interactionMode: "plan" as const,
      createdAt: DateTime.formatIso(current),
      providerDeliveryId: owned.evidence.providerDeliveryId,
      durableDeliveryKind: "initial-planning" as const,
      providerAdmissionPermit,
    };
    const prepareExit = yield* turnRequestExecutor
      .prepareTurnDelivery(deliveryInput)
      .pipe(Effect.exit);
    if (Exit.isFailure(prepareExit)) {
      yield* observeDeliveryFailure(owned, prepareExit.cause);
      if (hasExceptionalReasons(prepareExit.cause)) {
        return yield* Effect.failCause(prepareExit.cause);
      }
      yield* schedulePreDeliveryFailure(owned, prepareExit.cause);
      return;
    }

    let attemptedDelivery: AgentControlInitialPlanningDelivery | undefined;
    const boundaryExit = yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const exit = yield* restore(
          turnRequestExecutor.sendPreparedTurnAtPreInvokeBoundary(prepareExit.value, {
            beforeDeliveryCas: () =>
              hooks.beforeDeliveryCas?.(owned.evidence.handoffId) ?? Effect.void,
            persistDeliveryAttempted: (attestation) => {
              const resumeCursorJson = prepareExit.value.sessionResumeCursorJson;
              const sessionAttestation = prepareExit.value.sessionAttestation;
              if (resumeCursorJson === undefined || sessionAttestation === undefined) {
                return Effect.fail(
                  new ProviderAdapterRequestError({
                    provider: String(attestation.providerInstanceId),
                    method: "thread.turn.start",
                    detail: "Initial Planning resume correlation is unavailable.",
                  }),
                );
              }
              return nowIso.pipe(
                Effect.flatMap((attemptedAt) =>
                  store.markDeliveryAttempted({
                    providerDeliveryId: owned.evidence.providerDeliveryId,
                    handoffId: owned.evidence.handoffId,
                    ownerId,
                    claimGeneration: owned.delivery.claimGeneration,
                    expectedRevision: owned.delivery.revision,
                    attemptedAt,
                    providerSessionCreatedAt: sessionAttestation.sessionCreatedAt,
                    providerResumeCursorJson: resumeCursorJson,
                    providerInstanceId: String(attestation.providerInstanceId),
                    turnModelSelectionJson: attestation.modelSelectionJson,
                    turnModelSelectionFingerprint: attestation.modelSelectionFingerprint,
                  }),
                ),
                Effect.tap((delivery) =>
                  Effect.sync(() => {
                    attemptedDelivery = delivery;
                  }),
                ),
                Effect.asVoid,
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: String(attestation.providerInstanceId),
                      method: "thread.turn.start",
                      detail: "Initial Planning delivery marker CAS failed.",
                      cause,
                    }),
                ),
              );
            },
            afterDeliveryCas: () =>
              hooks.afterDeliveryCas?.(owned.evidence.handoffId) ?? Effect.void,
          }),
        ).pipe(Effect.exit);
        if (Exit.isFailure(exit)) {
          yield* observeDeliveryFailure(owned, exit.cause);
          const persisted = yield* load(owned.evidence.handoffId);
          if (
            persisted.delivery.state === "delivery-attempted" &&
            prepareExit.value.entryState?.externalOperationStarted === true
          ) {
            yield* markAmbiguousAndSettle(persisted, yield* nowIso);
          } else if (hasExceptionalReasons(exit.cause)) {
            return exit;
          } else if (persisted.delivery.state === "claimed") {
            yield* schedulePreDeliveryFailure(persisted, exit.cause);
          } else if (persisted.delivery.state === "delivery-attempted") {
            yield* schedulePreDeliveryFailure(persisted, exit.cause, true);
          } else if (
            persisted.delivery.state !== "provider-started" &&
            persisted.delivery.state !== "interrupt-requested" &&
            persisted.delivery.state !== "ambiguous"
          ) {
            return yield* Effect.die(
              new Error("Initial Planning provider boundary evidence diverged."),
            );
          }
        }
        return exit;
      }),
    );
    if (Exit.isFailure(boundaryExit)) {
      if (hasExceptionalReasons(boundaryExit.cause)) {
        return yield* Effect.failCause(boundaryExit.cause);
      }
      return;
    }
    const attempted =
      attemptedDelivery === undefined
        ? yield* load(owned.evidence.handoffId)
        : { ...owned, delivery: attemptedDelivery };
    const reconcileDeliveryRace = Effect.fn(
      "AgentControlInitialPlanningConsumer.reconcileDeliveryRace",
    )(function* (providerTurnId?: string) {
      const persisted = yield* load(attempted.evidence.handoffId);
      if (
        persisted.delivery.state === "provider-started" &&
        (providerTurnId === undefined || persisted.delivery.providerTurnId === providerTurnId)
      ) {
        return persisted.delivery;
      }
      if (providerTurnId === undefined && persisted.delivery.state === "delivery-attempted") {
        return yield* markAmbiguousAndSettle(persisted, yield* nowIso);
      }
      return yield* Effect.die(new Error("Initial Planning provider delivery evidence diverged."));
    });
    const acceptedAt = yield* nowIso;
    yield* store
      .markProviderStarted({
        handoffId: attempted.evidence.handoffId,
        ownerId,
        claimGeneration: attempted.delivery.claimGeneration,
        expectedRevision: attempted.delivery.revision,
        providerTurnId: String(boundaryExit.value.result.turnId),
        acceptedAt,
      })
      .pipe(
        Effect.catch(() => reconcileDeliveryRace(String(boundaryExit.value.result.turnId))),
        Effect.tap(() => wakeup.wake(attempted.evidence.handoffId)),
        Effect.asVoid,
      );
  });

  const processHandoff = Effect.fn("AgentControlInitialPlanningConsumer.processHandoff")(function* (
    handoffId: string,
  ) {
    let claim = yield* load(handoffId);
    if (
      claim.delivery.state === "ambiguous" ||
      claim.delivery.state === "completed" ||
      claim.delivery.state === "failed" ||
      claim.delivery.state === "interrupted"
    ) {
      return;
    }
    if (claim.delivery.planningDeadlineAt <= (yield* nowIso)) {
      return yield* processDeadline(claim);
    }
    if (
      claim.delivery.state === "provider-started" ||
      claim.delivery.state === "interrupt-requested"
    ) {
      yield* reconcileProviderStarted(claim);
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
  });

  const processDeadline = Effect.fn("AgentControlInitialPlanningConsumer.processDeadline")(
    function* (claim: AgentControlInitialPlanningClaim) {
      const at = yield* nowIso;
      if (claim.delivery.state === "provider-started") {
        const interrupting = claim.delivery.interruptRequested
          ? claim
          : {
              ...claim,
              delivery: yield* store.requestInterrupt({
                handoffId: claim.evidence.handoffId,
                expectedRevision: claim.delivery.revision,
                requestedAt: at,
              }),
            };
        const interruptExit = yield* Effect.uninterruptibleMask((restore) =>
          restore(providerService.interruptTurn({ threadId: claim.evidence.threadId })).pipe(
            Effect.exit,
          ),
        );
        if (Exit.isFailure(interruptExit)) {
          yield* markAmbiguousAndSettle(interrupting, at);
          return yield* Effect.failCause(interruptExit.cause);
        }
        return;
      }
      if (claim.delivery.state === "interrupt-requested") {
        yield* reconcileProviderStarted(claim);
        return;
      }
      if (claim.delivery.state === "delivery-attempted") {
        yield* markAmbiguousAndSettle(claim, at);
        return;
      }
      yield* markTerminal(claim, "failed", at, "planning-deadline");
    },
  );

  const processRuntimeEvent = Effect.fn("AgentControlInitialPlanningConsumer.processRuntimeEvent")(
    function* (event: ProviderRuntimeEvent) {
      if (event.turnId === undefined) return;
      const claimOption = yield* store.loadAcceptedByThreadId(event.threadId);
      if (Option.isNone(claimOption)) return;
      const claim = claimOption.value;
      if (event.providerInstanceId !== claim.evidence.providerInstanceId) return;
      if (event.type === "turn.started") {
        const observed = yield* store.observeProviderStarted({
          threadId: event.threadId,
          providerTurnId: String(event.turnId),
          acceptedAt: event.createdAt,
        });
        if (Option.isSome(observed)) yield* wakeup.wake(claim.evidence.handoffId);
        return;
      }
      if (event.type !== "turn.completed" && event.type !== "turn.aborted") return;
      const terminal =
        event.type === "turn.aborted"
          ? claim.delivery.interruptRequested
            ? "interrupted"
            : "failed"
          : event.payload.state === "completed"
            ? "completed"
            : event.payload.state === "interrupted" || event.payload.state === "cancelled"
              ? "interrupted"
              : "failed";
      const observed = yield* store.observeProviderTerminal({
        threadId: event.threadId,
        providerTurnId: String(event.turnId),
        state: terminal,
        terminalAt: event.createdAt,
        ...(terminal === "completed"
          ? {}
          : {
              errorCode:
                event.type === "turn.aborted"
                  ? "provider-aborted"
                  : terminal === "interrupted"
                    ? "provider-aborted"
                    : "provider-defect",
            }),
      });
      if (Option.isSome(observed)) {
        yield* settleThreadProjection(
          { ...claim, delivery: observed.value },
          terminal,
          event.createdAt,
        );
        yield* wakeup.wake(claim.evidence.handoffId);
      }
    },
  );

  const recover = Effect.fn("AgentControlInitialPlanningConsumer.recover")(function* () {
    const current = yield* nowIso;
    const [recoverable, expired] = yield* Effect.all([
      store.listRecoverable(current),
      store.listExpired(current),
    ]);
    const handoffIds = new Set([
      ...recoverable.map((claim) => claim.evidence.handoffId),
      ...expired.map((claim) => claim.evidence.handoffId),
    ]);
    yield* Effect.forEach(handoffIds, (handoffId) => processHandoff(handoffId), { concurrency: 1 });
  });

  const processSafely = (input: ConsumerInput) =>
    (input._tag === "handoff"
      ? processHandoff(input.handoffId)
      : input._tag === "runtime"
        ? processRuntimeEvent(input.event)
        : recover()
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.logError("initial planning consumer input failed", {
          inputTag: input._tag,
          cause: Cause.pretty(cause),
        }),
      ),
    );
  const worker = yield* makeDrainableWorker(processSafely);

  const start: AgentControlInitialPlanningConsumerShape["start"] = Effect.fn("start")(function* () {
    yield* Effect.forkScoped(
      Stream.runForEach(wakeup.stream, (handoffId) =>
        worker.enqueue({ _tag: "handoff", handoffId }),
      ),
    );
    yield* Effect.forkScoped(
      Stream.runForEach(providerService.streamEvents, (event) =>
        worker.enqueue({ _tag: "runtime", event }),
      ),
    );
    yield* worker.enqueue({ _tag: "recover" });
    yield* Effect.forkScoped(
      Effect.forever(
        Effect.sleep(RECOVERY_INTERVAL).pipe(Effect.andThen(worker.enqueue({ _tag: "recover" }))),
      ),
    );
  });

  return {
    start,
    drain: worker.drain,
  } satisfies AgentControlInitialPlanningConsumerShape;
});

export const AgentControlInitialPlanningConsumerLive = Layer.effect(
  AgentControlInitialPlanningConsumer,
  make,
);
