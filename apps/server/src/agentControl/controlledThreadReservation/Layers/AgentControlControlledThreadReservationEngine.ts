import {
  AgentControlControlledThreadReservationCommand,
  AgentControlControlledThreadReservationId,
  AgentControlControlledThreadReservationRejectedCommandCode,
  AgentControlControlledThreadReservationRpcError,
  type AgentControlControlledThreadReservationCommandResult,
  type AgentControlControlledThreadReservationPrepareCommand,
  type AgentControlControlledThreadReservationState,
  type AgentControlControlledThreadReservationView,
  type AgentControlRejectedCommandErrorCode,
  EventId,
  ProjectId as ProjectIdSchema,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { deriveRejectedAgentControlControlledThreadReservationId } from "../identity.ts";
import {
  loadAuthoritativeControlledThreadReservation,
  loadAuthoritativeControlledThreadReservationTaskHistory,
} from "../authoritative.ts";
import { decideAgentControlControlledThreadReservationCommand } from "../decider.ts";
import { validateAgentControlControlledThreadReservationState } from "../invariant.ts";
import { projectAgentControlControlledThreadReservationEvent } from "../projector.ts";
import {
  AgentControlControlledThreadReservationEngine,
  type AgentControlControlledThreadReservationDispatchOutcome,
  type AgentControlControlledThreadReservationEngineShape,
} from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationProjection } from "../Services/AgentControlControlledThreadReservationProjection.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseState,
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktree } from "../../worktree/Services/AgentControlWorktree.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationCommand);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationId);
const isRpcError = Schema.is(AgentControlControlledThreadReservationRpcError);
const isReservationCode = Schema.is(AgentControlControlledThreadReservationRejectedCommandCode);
const internalProjectId = ProjectIdSchema.make(
  "agent-control-controlled-thread-reservation-internal",
);

export const toAgentControlControlledThreadReservationView = (
  state: AgentControlControlledThreadReservationState,
): AgentControlControlledThreadReservationView => ({
  controlledThreadReservationId: state.controlledThreadReservationId,
  threadId: state.threadId,
  projectId: state.projectId,
  taskId: state.taskId,
  stageRunId: state.stageRunId,
  attemptId: state.attemptId,
  roleId: state.roleId,
  status: state.status,
  revision: state.revision,
  preparedAt: state.preparedAt,
});

const rpcError = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  input: {
    readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
    readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
    readonly controlledThreadReservationId?: AgentControlControlledThreadReservationId | undefined;
  },
  operation: AgentControlControlledThreadReservationRpcError["operation"] = "dispatch",
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation,
    projectId: input.projectId,
    taskId: input.taskId,
    controlledThreadReservationId: input.controlledThreadReservationId ?? null,
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

const sameCommandBinding = (
  state: AgentControlControlledThreadReservationState,
  command: AgentControlControlledThreadReservationPrepareCommand,
) =>
  state.controlledThreadReservationId === command.controlledThreadReservationId &&
  state.threadId === command.threadId &&
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.roleId === command.roleId &&
  state.stageKind === command.stageKind &&
  state.stageOrdinal === command.stageOrdinal &&
  state.attemptOrdinal === command.attemptOrdinal &&
  state.leaseId === command.leaseId &&
  state.fenceToken === command.fenceToken &&
  state.worktreeReservationId === command.worktreeReservationId &&
  state.status === "prepared" &&
  state.revision === 1 &&
  command.expectedRevision === 0 &&
  command.authority === "controller";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlControlledThreadReservationEventStore;
  const projection = yield* AgentControlControlledThreadReservationProjection;
  const states = yield* AgentControlControlledThreadReservationStateRepository;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktree;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;

  yield* projection.bootstrap.pipe(
    Effect.mapError(() =>
      rpcError(
        "controlled-thread-reservation-corrupt",
        {
          projectId: internalProjectId,
          taskId: "controlled-thread-reservation-internal" as never,
        },
        "dispatch",
      ),
    ),
  );
  const eventPubSub =
    yield* PubSub.unbounded<
      import("@t3tools/contracts").AgentControlControlledThreadReservationEvent
    >();

  const mapHistoryError = (
    failure: { readonly _tag: string },
    input: {
      readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
      readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
      readonly controlledThreadReservationId?:
        | AgentControlControlledThreadReservationId
        | undefined;
    },
  ) =>
    rpcError(
      failure._tag === "AgentControlPersistenceSqlError"
        ? "internal-persistence-error"
        : "controlled-thread-reservation-corrupt",
      input,
    );

  const validateTaskHistory: AgentControlControlledThreadReservationEngineShape["validateTaskHistory"] =
    (projectId, taskId) =>
      loadAuthoritativeControlledThreadReservationTaskHistory(
        projectId,
        taskId,
        events,
        states,
      ).pipe(Effect.mapError((failure) => mapHistoryError(failure, { projectId, taskId })));

  const loadState = (
    controlledThreadReservationId: AgentControlControlledThreadReservationId,
    projectId: AgentControlControlledThreadReservationRpcError["projectId"],
    taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>,
  ) =>
    loadAuthoritativeControlledThreadReservation(
      controlledThreadReservationId,
      events,
      states,
    ).pipe(
      Effect.mapError((failure) =>
        mapHistoryError(failure, { projectId, taskId, controlledThreadReservationId }),
      ),
    );

  const replayState = Effect.fn("AgentControlControlledThreadReservationEngine.replayState")(
    function* (input: {
      readonly commandId: AgentControlControlledThreadReservationCommand["commandId"];
      readonly projectId: AgentControlControlledThreadReservationRpcError["projectId"];
      readonly taskId: NonNullable<AgentControlControlledThreadReservationRpcError["taskId"]>;
      readonly commandFingerprint: string;
    }) {
      const receipt = yield* receipts
        .getByCommandId(input.commandId)
        .pipe(Effect.mapError(() => rpcError("internal-persistence-error", input)));
      if (Option.isNone(receipt)) {
        return Option.none<{
          readonly state: AgentControlControlledThreadReservationState;
          readonly result: AgentControlControlledThreadReservationCommandResult;
        }>();
      }
      const value = receipt.value;
      if (
        value.commandFingerprint !== input.commandFingerprint ||
        value.authority !== "controller" ||
        value.aggregateKind !== "controlled-thread-reservation"
      ) {
        return yield* rpcError("command-identity-mismatch", input);
      }
      if (value.status === "rejected") {
        const rejectedAggregateId =
          yield* deriveRejectedAgentControlControlledThreadReservationId(input);
        if (
          !isReservationCode(value.errorCode) ||
          value.aggregateId !== rejectedAggregateId ||
          value.resultSequence !== 0 ||
          value.resultStreamVersion !== 0 ||
          value.eventCreated
        ) {
          return yield* rpcError("controlled-thread-reservation-corrupt", input);
        }
        return yield* rpcError(value.errorCode, input);
      }
      const controlledThreadReservationId = yield* decodeReservationId(value.aggregateId).pipe(
        Effect.mapError(() => rpcError("controlled-thread-reservation-corrupt", input)),
      );
      const state = yield* loadState(
        controlledThreadReservationId,
        input.projectId,
        input.taskId,
      ).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                rpcError("controlled-thread-reservation-missing", {
                  ...input,
                  controlledThreadReservationId,
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      );
      yield* validateTaskHistory(input.projectId, input.taskId);
      if (
        state.projectId !== input.projectId ||
        state.taskId !== input.taskId ||
        state.revision !== value.resultStreamVersion ||
        state.sequence !== value.resultSequence ||
        (value.eventCreated && value.acceptedAt !== state.preparedAt)
      ) {
        return yield* rpcError("controlled-thread-reservation-corrupt", {
          ...input,
          controlledThreadReservationId,
        });
      }
      yield* validateAgentControlControlledThreadReservationState(state).pipe(
        Effect.mapError(() =>
          rpcError("controlled-thread-reservation-corrupt", {
            ...input,
            controlledThreadReservationId,
          }),
        ),
      );
      return Option.some({
        state,
        result: {
          reservation: toAgentControlControlledThreadReservationView(state),
          resultSequence: value.resultSequence,
          eventCreated: value.eventCreated,
        },
      });
    },
  );

  const replayReceiptFirst: AgentControlControlledThreadReservationEngineShape["replayReceiptFirst"] =
    (input) =>
      sql.withTransaction(replayState(input)).pipe(
        Effect.map(Option.map((replayed) => replayed.result)),
        Effect.catchTag("SqlError", () =>
          Effect.fail(rpcError("internal-persistence-error", input)),
        ),
      );

  const ensureDbAdmission = Effect.fn(
    "AgentControlControlledThreadReservationEngine.ensureDbAdmission",
  )(function* (command: AgentControlControlledThreadReservationPrepareCommand) {
    return yield* taskGuard
      .useTaskConsumable(command.projectId, command.taskId, (task) =>
        Effect.gen(function* () {
          const sourceIdentityFingerprint =
            yield* deriveAgentControlSourceIdentityFingerprint(task);
          if (
            task.revision !== command.taskRevision ||
            task.githubIntakeSequence !== command.githubIntakeSequence ||
            sourceIdentityFingerprint !== command.sourceIdentityFingerprint
          ) {
            return yield* rpcError("source-snapshot-stale", command);
          }
          const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
            command.projectId,
            command.taskId,
            stageEvents,
            stageStates,
          ).pipe(
            Effect.mapError((failure) =>
              rpcError(
                failure._tag === "AgentControlPersistenceSqlError"
                  ? "internal-persistence-error"
                  : "stage-run-projection-corrupt",
                command,
              ),
            ),
          );
          const stageMatches = stageHistory.filter(
            (stage) =>
              stage.stageRunId === command.stageRunId &&
              stage.attemptId === command.attemptId &&
              stage.roleId === command.roleId &&
              stage.stageKind === command.stageKind &&
              stage.stageOrdinal === command.stageOrdinal &&
              stage.attemptOrdinal === command.attemptOrdinal &&
              stage.taskRevision === command.taskRevision &&
              stage.githubIntakeSequence === command.githubIntakeSequence &&
              stage.sourceIdentityFingerprint === command.sourceIdentityFingerprint,
          );
          if (stageMatches.length === 0) return yield* rpcError("stage-run-missing", command);
          if (stageMatches.length !== 1) {
            return yield* rpcError("stage-run-history-ambiguous", command);
          }
          if (stageMatches[0]!.status !== "prepared") {
            return yield* rpcError("stage-run-not-prepared", command);
          }

          const lease = yield* loadAuthoritativeLeaseState(
            command.leaseId,
            leaseEvents,
            leaseStates,
          ).pipe(
            Effect.mapError((failure) =>
              rpcError(
                failure._tag === "AgentControlPersistenceSqlError"
                  ? "internal-persistence-error"
                  : "lease-projection-corrupt",
                command,
              ),
            ),
          );
          if (Option.isNone(lease)) return yield* rpcError("lease-missing", command);
          const leaseState = lease.value.state;
          if (leaseState.status !== "reserved") {
            return yield* rpcError("lease-not-reserved", command);
          }
          if (leaseState.holderId !== runtimeHolderId) {
            return yield* rpcError("lease-foreign-runtime", command);
          }
          if (leaseState.fenceToken !== command.fenceToken) {
            return yield* rpcError("fence-token-mismatch", command);
          }
          if (
            leaseState.projectId !== command.projectId ||
            leaseState.taskId !== command.taskId ||
            leaseState.stageRunId !== command.stageRunId ||
            leaseState.attemptId !== command.attemptId ||
            leaseState.taskRevision !== command.taskRevision ||
            leaseState.githubIntakeSequence !== command.githubIntakeSequence ||
            leaseState.sourceIdentityFingerprint !== command.sourceIdentityFingerprint
          ) {
            return yield* rpcError("source-snapshot-stale", command);
          }
          const expiresAt = canonicalTimestampMillis(leaseState.expiresAt);
          const now = yield* DateTime.now;
          if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
            return yield* rpcError("lease-expired", command);
          }

          const listed = yield* worktrees
            .listReservations({ projectId: command.projectId })
            .pipe(
              Effect.mapError((failure) =>
                rpcError(
                  failure.code === "internal-persistence-error"
                    ? "internal-persistence-error"
                    : "worktree-projection-corrupt",
                  command,
                ),
              ),
            );
          if (listed.quarantinedCount !== 0) {
            return yield* rpcError("worktree-projection-corrupt", command);
          }
          const matchingViews = listed.reservations.filter(
            (worktree) =>
              worktree.reservationId === command.worktreeReservationId &&
              worktree.projectId === command.projectId &&
              worktree.taskId === command.taskId &&
              worktree.stageRunId === command.stageRunId &&
              worktree.attemptId === command.attemptId &&
              worktree.leaseId === command.leaseId &&
              worktree.fenceToken === command.fenceToken,
          );
          if (matchingViews.length === 0) return yield* rpcError("worktree-missing", command);
          if (matchingViews.length !== 1) {
            return yield* rpcError("worktree-history-ambiguous", command);
          }
          if (matchingViews[0]!.status !== "ready") {
            return yield* rpcError("worktree-not-ready", command);
          }
          const worktree = yield* worktreeEngine
            .loadAuthoritative(command.worktreeReservationId)
            .pipe(
              Effect.mapError((failure) =>
                rpcError(
                  failure.code === "internal-persistence-error"
                    ? "internal-persistence-error"
                    : "worktree-projection-corrupt",
                  command,
                ),
              ),
            );
          if (worktree === null) return yield* rpcError("worktree-missing", command);
          if (worktree.status !== "ready") return yield* rpcError("worktree-not-ready", command);
          if (
            worktree.projectId !== command.projectId ||
            worktree.taskId !== command.taskId ||
            worktree.taskRevision !== command.taskRevision ||
            worktree.githubIntakeSequence !== command.githubIntakeSequence ||
            worktree.sourceIdentityFingerprint !== command.sourceIdentityFingerprint ||
            worktree.stageRunId !== command.stageRunId ||
            worktree.attemptId !== command.attemptId ||
            worktree.leaseId !== command.leaseId ||
            worktree.fenceToken !== command.fenceToken
          ) {
            return yield* rpcError("source-snapshot-stale", command);
          }
        }),
      )
      .pipe(
        Effect.mapError((failure) =>
          failure._tag === "AgentControlTaskConsumerGuardError"
            ? rpcError(guardCode(failure.reason), command)
            : failure,
        ),
      );
  });

  const insertRejected = (
    command: AgentControlControlledThreadReservationCommand,
    commandFingerprint: string,
    code: AgentControlControlledThreadReservationRpcError["code"],
    state: AgentControlControlledThreadReservationState | null,
    rejectedAt: string,
  ) =>
    receipts
      .insert({
        commandId: command.commandId,
        commandFingerprint,
        authority: "controller",
        aggregateKind: "controlled-thread-reservation",
        aggregateId: command.controlledThreadReservationId,
        status: "rejected",
        resultSequence: state?.sequence ?? 0,
        resultStreamVersion: state?.revision ?? 0,
        eventCreated: false,
        acceptedAt: rejectedAt,
        errorCode: code as AgentControlRejectedCommandErrorCode,
      })
      .pipe(
        Effect.as({
          _tag: "Rejected" as const,
          error: rpcError(code, command),
        }),
      );

  const dispatchPreparedController: AgentControlControlledThreadReservationEngineShape["dispatchPreparedController"] =
    (rawCommand, commandFingerprint) =>
      Effect.gen(function* () {
        const command = yield* decodeCommand(rawCommand).pipe(
          Effect.mapError(() => rpcError("validation", rawCommand)),
        );
        if (commandFingerprint.trim().length === 0) {
          return yield* rpcError("validation", command);
        }
        return yield* sql.withTransaction(
          Effect.gen(function* () {
            const replay = yield* replayState({
              commandId: command.commandId,
              projectId: command.projectId,
              taskId: command.taskId,
              commandFingerprint,
            });
            if (Option.isSome(replay)) {
              if (
                command.type !== "agentControl.controlledThreadReservation.prepare" ||
                !sameCommandBinding(replay.value.state, command)
              ) {
                return yield* rpcError("command-identity-mismatch", command);
              }
              return {
                _tag: "Accepted" as const,
                result: replay.value.result,
                events: [],
              } satisfies AgentControlControlledThreadReservationDispatchOutcome;
            }

            const history = yield* validateTaskHistory(command.projectId, command.taskId);
            const current =
              history.find(
                (state) =>
                  state.controlledThreadReservationId === command.controlledThreadReservationId,
              ) ?? null;
            const occurredAt = DateTime.formatIso(yield* DateTime.now);
            if (command.type === "agentControl.controlledThreadReservation.transition") {
              return yield* insertRejected(
                command,
                commandFingerprint,
                "state-not-available",
                current,
                occurredAt,
              );
            }

            // This is the post-Git, pre-commit authority recheck. Every failure
            // here is receiptless so a repaired/current observation may retry.
            yield* ensureDbAdmission(command);

            const decision = yield* Effect.result(
              decideAgentControlControlledThreadReservationCommand({
                state: current,
                command,
                eventId: EventId.make(
                  yield* crypto.randomUUIDv4.pipe(
                    Effect.mapError(() => rpcError("internal-persistence-error", command)),
                  ),
                ),
                occurredAt,
              }),
            );
            if (decision._tag === "Failure") {
              return yield* insertRejected(
                command,
                commandFingerprint,
                decision.failure.code,
                current,
                occurredAt,
              );
            }
            const appended =
              decision.success.length === 0
                ? []
                : yield* events.append({
                    controlledThreadReservationId: command.controlledThreadReservationId,
                    expectedStreamVersion: 0,
                    events: decision.success,
                  });
            let next = current;
            for (const event of appended) {
              yield* projection.projectEvent(event);
              next = yield* projectAgentControlControlledThreadReservationEvent(next, event);
            }
            if (next === null) {
              return yield* rpcError("controlled-thread-reservation-missing", command);
            }
            yield* receipts.insert({
              commandId: command.commandId,
              commandFingerprint,
              authority: "controller",
              aggregateKind: "controlled-thread-reservation",
              aggregateId: command.controlledThreadReservationId,
              status: "accepted",
              resultSequence: next.sequence,
              resultStreamVersion: next.revision,
              eventCreated: appended.length > 0,
              acceptedAt: occurredAt,
              errorCode: null,
            });
            return {
              _tag: "Accepted" as const,
              events: appended,
              result: {
                reservation: toAgentControlControlledThreadReservationView(next),
                resultSequence: next.sequence,
                eventCreated: appended.length > 0,
              },
            } satisfies AgentControlControlledThreadReservationDispatchOutcome;
          }),
        );
      }).pipe(
        Effect.mapError((failure) => {
          if (isRpcError(failure)) return failure;
          if (
            typeof failure === "object" &&
            failure !== null &&
            "_tag" in failure &&
            (failure._tag === "AgentControlPersistenceDecodeError" ||
              failure._tag === "AgentControlControlledThreadReservationStreamVersionConflictError")
          ) {
            return rpcError("controlled-thread-reservation-corrupt", rawCommand);
          }
          return rpcError("internal-persistence-error", rawCommand);
        }),
      );

  const getAuthoritative: AgentControlControlledThreadReservationEngineShape["getAuthoritative"] = (
    controlledThreadReservationId,
  ) =>
    loadAuthoritativeControlledThreadReservation(
      controlledThreadReservationId,
      events,
      states,
    ).pipe(
      Effect.mapError((failure) =>
        mapHistoryError(failure, {
          projectId: internalProjectId,
          taskId: "controlled-thread-reservation-internal" as never,
          controlledThreadReservationId,
        }),
      ),
    );

  const publishCommitted: AgentControlControlledThreadReservationEngineShape["publishCommitted"] = (
    committed,
  ) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });

  return AgentControlControlledThreadReservationEngine.of({
    dispatchPreparedController,
    replayReceiptFirst,
    getAuthoritative,
    validateTaskHistory,
    publishCommitted,
    rebuild: projection.rebuild.pipe(
      Effect.mapError((failure) =>
        mapHistoryError(failure, {
          projectId: internalProjectId,
          taskId: "controlled-thread-reservation-internal" as never,
        }),
      ),
    ),
    streamDomainEvents: Stream.fromPubSub(eventPubSub),
  });
});

export const layer = Layer.effect(AgentControlControlledThreadReservationEngine, make);
