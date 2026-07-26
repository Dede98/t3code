import {
  AgentControlControlledThreadReservationGetInput,
  AgentControlControlledThreadReservationListInput,
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
  AgentControlControlledThreadReservationId,
  type AgentControlTaskId,
  type AgentControlControlledThreadReservationState,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlWorktreeReservationState,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
  deriveRejectedAgentControlControlledThreadReservationId,
} from "../identity.ts";
import {
  AgentControlControlledThreadReservation,
  type AgentControlControlledThreadReservationShape,
} from "../Services/AgentControlControlledThreadReservation.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";
import { toAgentControlControlledThreadReservationView } from "./AgentControlControlledThreadReservationEngine.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseState,
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktree } from "../../worktree/Services/AgentControlWorktree.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationGetInput, {
  onExcessProperty: "error",
});
const decodeList = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationListInput, {
  onExcessProperty: "error",
});
const decodePrepare = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationPrepareInitialInput,
  { onExcessProperty: "error" },
);
const encodePrepare = Schema.encodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      type: Schema.Literal("agentControl.controlledThreadReservation.prepareInitial"),
      authority: Schema.Literal("controller"),
      commandId: Schema.String,
      projectId: Schema.String,
      taskId: Schema.String,
    }),
  ),
);
const isRpcError = Schema.is(AgentControlControlledThreadReservationRpcError);
const RECEIPTLESS = new Set<AgentControlControlledThreadReservationRpcError["code"]>([
  "internal-persistence-error",
  "source-snapshot-unavailable",
  "source-watermark-stale",
  "task-projection-corrupt",
  "stage-run-missing",
  "stage-run-projection-corrupt",
  "lease-missing",
  "lease-projection-corrupt",
  "worktree-missing",
  "worktree-not-ready",
  "worktree-projection-corrupt",
  "worktree-history-ambiguous",
  "controlled-thread-reservation-corrupt",
]);

const safeError = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  operation: AgentControlControlledThreadReservationRpcError["operation"],
  projectId: AgentControlControlledThreadReservationGetInput["projectId"],
  taskId: AgentControlControlledThreadReservationPrepareInitialInput["taskId"] | null = null,
  controlledThreadReservationId: AgentControlControlledThreadReservationId | null = null,
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation,
    projectId,
    taskId,
    controlledThreadReservationId,
  });

const guardCode = (
  reason: AgentControlTaskConsumerGuardReason,
): AgentControlControlledThreadReservationRpcError["code"] => {
  switch (reason) {
    case "project-unavailable":
      return "project-unavailable";
    case "mode-inactive":
      return "project-mode-inactive";
    case "source-snapshot-unavailable":
      return "source-snapshot-unavailable";
    case "watermark-missing":
    case "watermark-not-completed":
    case "watermark-sequence-mismatch":
      return "source-watermark-stale";
    case "task-missing":
      return "task-missing";
    case "task-status-inactive":
      return "task-not-candidate";
    case "task-source-ineligible":
      return "task-ineligible";
    case "task-stage-inactive":
      return "task-stage-inactive";
    case "task-projection-corrupt":
      return "task-projection-corrupt";
    case "task-project-mismatch":
    case "task-sequence-mismatch":
    case "task-source-mismatch":
      return "source-snapshot-stale";
    case "internal-persistence-error":
      return "internal-persistence-error";
  }
};

const sameWorktreeBinding = (
  selected: AgentControlWorktreeReservationState,
  guarded: AgentControlWorktreeReservationState,
) =>
  guarded.reservationId === selected.reservationId &&
  guarded.projectId === selected.projectId &&
  guarded.taskId === selected.taskId &&
  guarded.taskRevision === selected.taskRevision &&
  guarded.githubIntakeSequence === selected.githubIntakeSequence &&
  guarded.sourceIdentityFingerprint === selected.sourceIdentityFingerprint &&
  guarded.stageRunId === selected.stageRunId &&
  guarded.attemptId === selected.attemptId &&
  guarded.leaseId === selected.leaseId &&
  guarded.fenceToken === selected.fenceToken &&
  guarded.status === "ready";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const availability = yield* AgentControlProjectAvailability;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const engine = yield* AgentControlControlledThreadReservationEngine;
  const events = yield* AgentControlControlledThreadReservationEventStore;
  const states = yield* AgentControlControlledThreadReservationStateRepository;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktree;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const worktreeController = yield* AgentControlWorktreeController;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;

  const fingerprint = Effect.fn("AgentControlControlledThreadReservation.fingerprint")(function* (
    input: AgentControlControlledThreadReservationPrepareInitialInput,
  ) {
    const canonical = yield* encodePrepare({
      type: "agentControl.controlledThreadReservation.prepareInitial",
      authority: "controller",
      commandId: input.commandId,
      projectId: input.projectId,
      taskId: input.taskId,
    });
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const ensureProject = (
    projectId: AgentControlControlledThreadReservationGetInput["projectId"],
    operation: AgentControlControlledThreadReservationRpcError["operation"],
  ) =>
    availability
      .ensureAvailable(projectId)
      .pipe(
        Effect.mapError((failure) =>
          safeError(
            failure._tag === "AgentControlProjectUnavailableError"
              ? "project-unavailable"
              : "internal-persistence-error",
            operation,
            projectId,
          ),
        ),
      );

  const persistRejected = Effect.fn("AgentControlControlledThreadReservation.persistRejected")(
    function* (
      input: AgentControlControlledThreadReservationPrepareInitialInput,
      commandFingerprint: string,
      code: AgentControlControlledThreadReservationRpcError["code"],
    ) {
      if (RECEIPTLESS.has(code))
        return yield* safeError(code, "prepare-initial", input.projectId, input.taskId);
      const aggregateId = yield* deriveRejectedAgentControlControlledThreadReservationId(input);
      const rejectedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* engine.validateTaskHistory(input.projectId, input.taskId);
          yield* receipts.insert({
            commandId: input.commandId,
            commandFingerprint,
            authority: "controller",
            aggregateKind: "controlled-thread-reservation",
            aggregateId,
            status: "rejected",
            resultSequence: 0,
            resultStreamVersion: 0,
            eventCreated: false,
            acceptedAt: rejectedAt,
            errorCode: code as AgentControlRejectedCommandErrorCode,
          });
        }),
      );
      return safeError(code, "prepare-initial", input.projectId, input.taskId);
    },
  );

  const initialBinding = Effect.fn("AgentControlControlledThreadReservation.initialBinding")(
    function* (input: AgentControlControlledThreadReservationPrepareInitialInput) {
      return yield* taskGuard
        .useTaskConsumable(input.projectId, input.taskId, (task) =>
          Effect.gen(function* () {
            const sourceIdentityFingerprint =
              yield* deriveAgentControlSourceIdentityFingerprint(task);
            const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
              input.projectId,
              input.taskId,
              stageEvents,
              stageStates,
            ).pipe(
              Effect.mapError((failure) =>
                safeError(
                  failure._tag === "AgentControlPersistenceSqlError"
                    ? "internal-persistence-error"
                    : "stage-run-projection-corrupt",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
              ),
            );
            const stageMatches = stageHistory.filter(
              (stage) =>
                stage.taskRevision === task.revision &&
                stage.githubIntakeSequence === task.githubIntakeSequence &&
                stage.sourceIdentityFingerprint === sourceIdentityFingerprint &&
                stage.stageKind === "planning" &&
                stage.roleId === "planning" &&
                stage.stageOrdinal === 1 &&
                stage.attemptOrdinal === 1,
            );
            if (stageMatches.length === 0) {
              return yield* safeError(
                "stage-run-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (stageMatches.length !== 1) {
              return yield* safeError(
                "stage-run-history-ambiguous",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const stage = stageMatches[0]!;
            if (stage.status !== "prepared") {
              return yield* safeError(
                "stage-run-not-prepared",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }

            const leaseRows = yield* leaseStates
              .listProject(input.projectId)
              .pipe(
                Effect.mapError(() =>
                  safeError(
                    "internal-persistence-error",
                    "prepare-initial",
                    input.projectId,
                    input.taskId,
                  ),
                ),
              );
            if (leaseRows.some((entry) => entry._tag === "Corrupt")) {
              return yield* safeError(
                "lease-projection-corrupt",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const candidates = leaseRows.filter(
              (entry) =>
                entry._tag === "Valid" &&
                entry.state.taskId === input.taskId &&
                entry.state.stageRunId === stage.stageRunId &&
                entry.state.attemptId === stage.attemptId,
            );
            if (candidates.length === 0) {
              return yield* safeError(
                "lease-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (candidates.length !== 1 || candidates[0]!._tag !== "Valid") {
              return yield* safeError(
                "lease-projection-corrupt",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const lease = yield* loadAuthoritativeLeaseState(
              candidates[0]!.state.leaseId,
              leaseEvents,
              leaseStates,
            ).pipe(
              Effect.mapError((failure) =>
                safeError(
                  failure._tag === "AgentControlPersistenceSqlError"
                    ? "internal-persistence-error"
                    : "lease-projection-corrupt",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
              ),
            );
            if (Option.isNone(lease)) {
              return yield* safeError(
                "lease-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const leaseState = lease.value.state;
            if (leaseState.status !== "reserved") {
              return yield* safeError(
                "lease-not-reserved",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (leaseState.holderId !== runtimeHolderId) {
              return yield* safeError(
                "lease-foreign-runtime",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const expiresAt = canonicalTimestampMillis(leaseState.expiresAt);
            const now = yield* DateTime.now;
            if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
              return yield* safeError(
                "lease-expired",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (
              leaseState.taskRevision !== task.revision ||
              leaseState.githubIntakeSequence !== task.githubIntakeSequence ||
              leaseState.sourceIdentityFingerprint !== sourceIdentityFingerprint
            ) {
              return yield* safeError(
                "source-snapshot-stale",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }

            const listed = yield* worktrees
              .listReservations({ projectId: input.projectId })
              .pipe(
                Effect.mapError((failure) =>
                  safeError(
                    failure.code === "internal-persistence-error"
                      ? "internal-persistence-error"
                      : "worktree-projection-corrupt",
                    "prepare-initial",
                    input.projectId,
                    input.taskId,
                  ),
                ),
              );
            if (listed.quarantinedCount !== 0) {
              return yield* safeError(
                "worktree-projection-corrupt",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const worktreeViews = listed.reservations.filter(
              (worktree) =>
                worktree.taskId === input.taskId &&
                worktree.stageRunId === stage.stageRunId &&
                worktree.attemptId === stage.attemptId &&
                worktree.leaseId === leaseState.leaseId &&
                worktree.fenceToken === leaseState.fenceToken,
            );
            if (worktreeViews.length === 0) {
              return yield* safeError(
                "worktree-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (worktreeViews.length !== 1) {
              return yield* safeError(
                "worktree-history-ambiguous",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (worktreeViews[0]!.status !== "ready") {
              return yield* safeError(
                "worktree-not-ready",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const worktree = yield* worktreeEngine
              .loadAuthoritative(worktreeViews[0]!.reservationId)
              .pipe(
                Effect.mapError((failure) =>
                  safeError(
                    failure.code === "internal-persistence-error"
                      ? "internal-persistence-error"
                      : "worktree-projection-corrupt",
                    "prepare-initial",
                    input.projectId,
                    input.taskId,
                  ),
                ),
              );
            if (worktree === null) {
              return yield* safeError(
                "worktree-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (
              worktree.status !== "ready" ||
              worktree.taskRevision !== task.revision ||
              worktree.githubIntakeSequence !== task.githubIntakeSequence ||
              worktree.sourceIdentityFingerprint !== sourceIdentityFingerprint
            ) {
              return yield* safeError(
                "source-snapshot-stale",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            return { task, sourceIdentityFingerprint, stage, lease: leaseState, worktree };
          }),
        )
        .pipe(
          Effect.mapError((failure) =>
            failure._tag === "AgentControlTaskConsumerGuardError"
              ? safeError(
                  guardCode(failure.reason),
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                )
              : failure,
          ),
        );
    },
  );

  const get: AgentControlControlledThreadReservationShape["get"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError(
            "validation",
            "get",
            rawInput.projectId,
            null,
            rawInput.controlledThreadReservationId,
          ),
        ),
      );
      const state = yield* engine.getAuthoritative(input.controlledThreadReservationId);
      if (Option.isNone(state) || state.value.projectId !== input.projectId) {
        return yield* safeError(
          "controlled-thread-reservation-missing",
          "get",
          input.projectId,
          null,
          input.controlledThreadReservationId,
        );
      }
      yield* engine.validateTaskHistory(input.projectId, state.value.taskId);
      yield* ensureProject(input.projectId, "get");
      return toAgentControlControlledThreadReservationView(state.value);
    });

  const list: AgentControlControlledThreadReservationShape["list"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list", rawInput.projectId)),
      );
      yield* ensureProject(input.projectId, "list");
      const entries = yield* states
        .listProject(input.projectId)
        .pipe(
          Effect.mapError(() => safeError("internal-persistence-error", "list", input.projectId)),
        );
      const taskIds = new Set(
        entries.flatMap((entry) =>
          entry._tag === "Valid"
            ? [entry.state.taskId]
            : entry.taskId === null
              ? []
              : [entry.taskId],
        ),
      );
      const eventTaskIds = new Set<AgentControlTaskId>();
      let cursor = 0;
      while (true) {
        const page = yield* events
          .readGlobal(cursor, 500)
          .pipe(
            Effect.mapError((failure) =>
              safeError(
                failure._tag === "AgentControlPersistenceSqlError"
                  ? "internal-persistence-error"
                  : "controlled-thread-reservation-corrupt",
                "list",
                input.projectId,
              ),
            ),
          );
        if (page.length === 0) break;
        for (const event of page) {
          if (event.sequence <= cursor) {
            return yield* safeError(
              "controlled-thread-reservation-corrupt",
              "list",
              input.projectId,
            );
          }
          cursor = event.sequence;
          if (event.payload.projectId === input.projectId) {
            eventTaskIds.add(event.payload.taskId);
            taskIds.add(event.payload.taskId);
          }
        }
      }
      const valid: Array<AgentControlControlledThreadReservationState> = [];
      let quarantinedCount = entries.filter((entry) => entry._tag === "Corrupt").length;
      for (const taskId of taskIds) {
        const history = yield* Effect.result(engine.validateTaskHistory(input.projectId, taskId));
        if (history._tag === "Failure") {
          if (history.failure.code === "internal-persistence-error") return yield* history.failure;
          quarantinedCount += 1;
          continue;
        }
        valid.push(...history.success);
      }
      for (const taskId of eventTaskIds) {
        if (!taskIds.has(taskId)) quarantinedCount += 1;
      }
      return {
        projectId: input.projectId,
        reservations: valid
          .toSorted(
            (left, right) =>
              left.taskRevision - right.taskRevision ||
              left.githubIntakeSequence - right.githubIntakeSequence ||
              left.stageOrdinal - right.stageOrdinal ||
              left.attemptOrdinal - right.attemptOrdinal ||
              left.controlledThreadReservationId.localeCompare(right.controlledThreadReservationId),
          )
          .map(toAgentControlControlledThreadReservationView),
        quarantinedCount,
      };
    });

  const prepareInitial: AgentControlControlledThreadReservationShape["prepareInitial"] = (
    rawInput,
  ) =>
    Effect.gen(function* () {
      const input = yield* decodePrepare(rawInput).pipe(
        Effect.mapError(() =>
          safeError("validation", "prepare-initial", rawInput.projectId, rawInput.taskId),
        ),
      );
      const commandFingerprint = yield* fingerprint(input).pipe(
        Effect.mapError(() =>
          safeError("internal-persistence-error", "prepare-initial", input.projectId, input.taskId),
        ),
      );
      const replay = yield* engine.replayReceiptFirst({
        commandId: input.commandId,
        projectId: input.projectId,
        taskId: input.taskId,
        commandFingerprint,
      });
      if (Option.isSome(replay)) return replay.value;

      // Complete reservation history is checked before any current authority,
      // Git, marker, registration, or worktree observation.
      yield* engine.validateTaskHistory(input.projectId, input.taskId);

      const initial = yield* Effect.result(initialBinding(input));
      if (initial._tag === "Failure") {
        const persisted = yield* Effect.result(
          persistRejected(input, commandFingerprint, initial.failure.code),
        );
        if (persisted._tag === "Failure") {
          const raced = yield* engine.replayReceiptFirst({
            commandId: input.commandId,
            projectId: input.projectId,
            taskId: input.taskId,
            commandFingerprint,
          });
          if (Option.isSome(raced)) return raced.value;
          return yield* isRpcError(persisted.failure)
            ? persisted.failure
            : safeError(
                "internal-persistence-error",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
        }
        return yield* persisted.success;
      }

      const binding = initial.success;
      const guarded = yield* worktreeController
        .useReadyWorktree(
          {
            projectId: input.projectId,
            reservationId: binding.worktree.reservationId,
          },
          (guardedWorktree) =>
            Effect.gen(function* () {
              if (!sameWorktreeBinding(binding.worktree, guardedWorktree)) {
                return yield* safeError(
                  "source-snapshot-stale",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                );
              }
              const stableIdentity = {
                projectId: input.projectId,
                taskId: input.taskId,
                taskRevision: binding.task.revision,
                githubIntakeSequence: binding.task.githubIntakeSequence,
                sourceIdentityFingerprint: binding.sourceIdentityFingerprint,
                stageRunId: binding.stage.stageRunId,
                attemptId: binding.stage.attemptId,
                roleId: binding.stage.roleId,
                stageKind: "planning" as const,
                stageOrdinal: 1 as const,
                attemptOrdinal: 1 as const,
              };
              const controlledThreadReservationId =
                yield* deriveAgentControlControlledThreadReservationId(stableIdentity);
              const threadId = yield* deriveAgentControlReservedThreadId(stableIdentity);
              return yield* engine.dispatchPreparedController(
                {
                  type: "agentControl.controlledThreadReservation.prepare",
                  commandId: input.commandId,
                  authority: "controller",
                  controlledThreadReservationId,
                  threadId,
                  ...stableIdentity,
                  leaseId: binding.lease.leaseId,
                  fenceToken: binding.lease.fenceToken,
                  worktreeReservationId: binding.worktree.reservationId,
                  expectedRevision: 0,
                },
                commandFingerprint,
              );
            }),
        )
        .pipe(
          Effect.mapError((failure) =>
            failure._tag === "AgentControlControlledThreadReservationRpcError"
              ? failure
              : safeError(
                  failure.code === "internal-persistence-error"
                    ? "internal-persistence-error"
                    : failure.code === "reservation-projection-corrupt"
                      ? "worktree-projection-corrupt"
                      : "worktree-not-ready",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
          ),
        );
      if (guarded._tag === "Rejected") return yield* guarded.error;
      yield* engine.publishCommitted(guarded.events);
      return guarded.result;
    });

  return AgentControlControlledThreadReservation.of({ get, list, prepareInitial });
});

export const layer = Layer.effect(AgentControlControlledThreadReservation, make);
