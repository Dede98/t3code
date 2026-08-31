import {
  type AgentControlRunOnceId,
  type AgentControlTaskState,
  AgentControlStageRunLeaseCommandIntent,
  AgentControlStageRunLeaseReceiptableRejectionCode,
  AgentControlStageRunLeaseRejectedCommandCode,
  AgentControlStageRunLeaseCommand,
  AgentControlStageRunLeaseRpcError,
  type AgentControlStageRunLeaseCommandAuthority,
  type AgentControlStageRunLeaseCommandResult,
  type AgentControlStageRunLeaseEvent,
  type AgentControlStageRunLeaseReceiptableRejectionCode as LeaseReceiptCode,
  type AgentControlStageRunLeaseState,
  type AgentControlStageRunLeaseView,
  EventId,
  ProjectId as ProjectIdSchema,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseState,
} from "../authoritative.ts";
import { decideAgentControlStageRunLeaseCommand } from "../decider.ts";
import { makeAgentControlStageRunLeaseHolderId } from "../identity.ts";
import { canonicalTimestampMillis } from "../invariant.ts";
import { projectAgentControlStageRunLeaseEvent } from "../projector.ts";
import {
  AgentControlStageRunLeaseEngine,
  type AgentControlStageRunLeaseDispatchInput,
  type AgentControlStageRunLeaseDispatchOutcome,
  type AgentControlStageRunLeaseEngineShape,
} from "../Services/AgentControlStageRunLeaseEngine.ts";
import { requireRunOnceMethod } from "../../runOnce/context.ts";
import { AgentControlStageRunLeaseEventStore } from "../Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseTransactionHooks } from "../Services/AgentControlStageRunLeaseTransactionHooks.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlSourceIdentityFingerprint,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlStageRunLeaseCommand);
const encodeCommandIntent = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLeaseCommandIntent),
);
const isRpcError = Schema.is(AgentControlStageRunLeaseRpcError);
const isLeaseRpcCode = Schema.is(AgentControlStageRunLeaseRejectedCommandCode);
const isReceiptableCode = Schema.is(AgentControlStageRunLeaseReceiptableRejectionCode);
const internalProjectId = ProjectIdSchema.make("agent-control-stage-run-lease-internal");
const EXPIRING_WINDOW_MS = 15_000;

const rpcError = (
  code: AgentControlStageRunLeaseRpcError["code"],
  command: {
    readonly projectId: AgentControlStageRunLeaseRpcError["projectId"];
    readonly taskId: NonNullable<AgentControlStageRunLeaseRpcError["taskId"]>;
  },
) =>
  new AgentControlStageRunLeaseRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
  });

const guardCode = (
  reason: AgentControlTaskConsumerGuardReason,
): AgentControlStageRunLeaseRpcError["code"] => {
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

const sameCommandResult = (
  state: AgentControlStageRunLeaseState,
  command: AgentControlStageRunLeaseCommand,
  historicalHolderId: AgentControlStageRunLeaseState["holderId"],
) => {
  const renewedAt = canonicalTimestampMillis(state.renewedAt);
  const expiresAt = canonicalTimestampMillis(state.expiresAt);
  const durationMatches =
    "leaseDurationMs" in command &&
    renewedAt !== null &&
    expiresAt !== null &&
    expiresAt - renewedAt === command.leaseDurationMs;
  if (
    state.leaseId !== command.leaseId ||
    state.projectId !== command.projectId ||
    state.taskId !== command.taskId ||
    state.stageRunId !== command.stageRunId ||
    state.attemptId !== command.attemptId ||
    state.taskRevision !== command.taskRevision ||
    state.githubIntakeSequence !== command.githubIntakeSequence ||
    state.sourceIdentityFingerprint !== command.sourceIdentityFingerprint ||
    state.holderId !== historicalHolderId ||
    state.fenceToken !== command.fenceToken ||
    state.revision !== command.expectedRevision + 1
  ) {
    return false;
  }
  if (command.type === "agentControl.stageRunLease.reserve") {
    return (
      state.status === "reserved" &&
      state.taskRevision === command.taskRevision &&
      state.githubIntakeSequence === command.githubIntakeSequence &&
      state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
      durationMatches
    );
  }
  if (command.type === "agentControl.stageRunLease.renew") {
    return state.status === "reserved" && durationMatches;
  }
  if (command.type === "agentControl.stageRunLease.releaseBeforeExecution") {
    return state.status === "released" && state.releasedAt !== null;
  }
  return false;
};

const leaseDurationMatches = (renewedAt: string, expiresAt: string, leaseDurationMs: number) => {
  const renewedAtMillis = canonicalTimestampMillis(renewedAt);
  const expiresAtMillis = canonicalTimestampMillis(expiresAt);
  return (
    renewedAtMillis !== null &&
    expiresAtMillis !== null &&
    expiresAtMillis - renewedAtMillis === leaseDurationMs
  );
};

const eventMatchesCommandIntent = (
  event: AgentControlStageRunLeaseEvent,
  command: AgentControlStageRunLeaseCommand,
) => {
  if (
    event.aggregateKind !== "stage-run-lease" ||
    event.aggregateId !== command.leaseId ||
    event.commandId !== command.commandId ||
    event.correlationId !== command.commandId ||
    event.causationEventId !== null ||
    event.authority !== command.authority ||
    event.payload.leaseId !== command.leaseId ||
    event.payload.stageRunId !== command.stageRunId ||
    event.payload.attemptId !== command.attemptId ||
    event.payload.fenceToken !== command.fenceToken
  ) {
    return false;
  }
  if (command.type === "agentControl.stageRunLease.reserve") {
    return (
      event.type === "agentControl.stageRunLease.reserved" &&
      event.payload.projectId === command.projectId &&
      event.payload.taskId === command.taskId &&
      event.payload.taskRevision === command.taskRevision &&
      event.payload.githubIntakeSequence === command.githubIntakeSequence &&
      event.payload.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
      leaseDurationMatches(
        event.payload.renewedAt,
        event.payload.expiresAt,
        command.leaseDurationMs,
      )
    );
  }
  if (command.type === "agentControl.stageRunLease.renew") {
    return (
      event.type === "agentControl.stageRunLease.renewed" &&
      leaseDurationMatches(
        event.payload.renewedAt,
        event.payload.expiresAt,
        command.leaseDurationMs,
      )
    );
  }
  return (
    command.type === "agentControl.stageRunLease.releaseBeforeExecution" &&
    event.type === "agentControl.stageRunLease.releasedBeforeExecution"
  );
};

const sameInitialPosition = (
  left: {
    readonly projectId: string;
    readonly taskId: string;
    readonly taskRevision: number;
    readonly githubIntakeSequence: number;
    readonly stageKind: string;
    readonly stageOrdinal: number;
  },
  right: {
    readonly projectId: string;
    readonly taskId: string;
    readonly taskRevision: number;
    readonly githubIntakeSequence: number;
    readonly stageKind: string;
    readonly stageOrdinal: number;
  },
) =>
  left.projectId === right.projectId &&
  left.taskId === right.taskId &&
  left.taskRevision === right.taskRevision &&
  left.githubIntakeSequence === right.githubIntakeSequence &&
  left.stageKind === right.stageKind &&
  left.stageOrdinal === right.stageOrdinal;

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const runtimeAttemptId = yield* crypto.randomUUIDv4.pipe(
    Effect.mapError(
      () =>
        new AgentControlStageRunLeaseRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId: internalProjectId,
          taskId: null,
        }),
    ),
  );
  const holderId = yield* makeAgentControlStageRunLeaseHolderId(runtimeAttemptId);
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlStageRunLeaseEventStore;
  const projection = yield* AgentControlStageRunLeaseProjection;
  const states = yield* AgentControlStageRunLeaseStateRepository;
  const stageRunEvents = yield* AgentControlStageRunEventStore;
  const stageRuns = yield* AgentControlStageRunStateRepository;
  const guard = yield* AgentControlTaskConsumerGuard;
  const transactionHooks = yield* AgentControlStageRunLeaseTransactionHooks;

  yield* projection.bootstrap.pipe(
    Effect.mapError(
      () =>
        new AgentControlStageRunLeaseRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId: internalProjectId,
          taskId: null,
        }),
    ),
  );

  const eventPubSub = yield* PubSub.unbounded<AgentControlStageRunLeaseEvent>();

  const fingerprint = Effect.fn("AgentControlStageRunLeaseEngine.fingerprint")(function* (
    command: AgentControlStageRunLeaseCommand,
  ) {
    const { holderId: _holderId, ...intent } = command;
    const canonical = yield* encodeCommandIntent(intent);
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const replayAccepted = Effect.fn("AgentControlStageRunLeaseEngine.replayAccepted")(function* (
    command: AgentControlStageRunLeaseCommand,
    resultStreamVersion: number,
    resultSequence: number,
    eventCreated: boolean,
    acceptedAt: string,
  ) {
    const authoritative = yield* loadAuthoritativeLeaseState(command.leaseId, events, states).pipe(
      Effect.mapError((error) =>
        rpcError(
          error._tag === "AgentControlPersistenceSqlError"
            ? "internal-persistence-error"
            : "lease-projection-corrupt",
          command,
        ),
      ),
    );
    if (Option.isNone(authoritative)) {
      return yield* rpcError("lease-projection-corrupt", command);
    }
    const state = authoritative.value.statesByVersion[resultStreamVersion - 1];
    const resultEvent = authoritative.value.events[resultStreamVersion - 1];
    if (state === undefined || resultEvent === undefined) {
      return yield* rpcError("lease-projection-corrupt", command);
    }
    if (
      !eventCreated ||
      resultEvent.streamVersion !== resultStreamVersion ||
      resultEvent.sequence !== resultSequence ||
      resultEvent.occurredAt !== acceptedAt ||
      state.revision !== resultStreamVersion ||
      state.sequence !== resultSequence ||
      !eventMatchesCommandIntent(resultEvent, command) ||
      !sameCommandResult(state, command, resultEvent.payload.holderId)
    ) {
      return yield* rpcError("command-identity-mismatch", command);
    }
    return {
      state,
      resultSequence,
      eventCreated,
    } satisfies AgentControlStageRunLeaseCommandResult;
  });

  const insertRejected = Effect.fn("AgentControlStageRunLeaseEngine.insertRejected")(function* (
    command: AgentControlStageRunLeaseCommand,
    commandFingerprint: string,
    code: LeaseReceiptCode,
    state: AgentControlStageRunLeaseState | null,
    rejectedAt: string,
  ) {
    yield* receipts.insert({
      commandId: command.commandId,
      commandFingerprint,
      authority: command.authority,
      aggregateKind: "stage-run-lease",
      aggregateId: command.leaseId,
      status: "rejected",
      resultSequence: state?.sequence ?? 0,
      resultStreamVersion: state?.revision ?? 0,
      eventCreated: false,
      acceptedAt: rejectedAt,
      errorCode: code,
    });
    return { _tag: "Rejected" as const, error: rpcError(code, command) };
  });

  const replayReceipt = Effect.fn("AgentControlStageRunLeaseEngine.replayReceipt")(function* (
    command: AgentControlStageRunLeaseCommand,
    commandFingerprint: string,
  ) {
    const receipt = yield* receipts.getByCommandId(command.commandId);
    if (Option.isNone(receipt)) {
      return Option.none<AgentControlStageRunLeaseDispatchOutcome>();
    }
    const value = receipt.value;
    if (
      value.commandFingerprint !== commandFingerprint ||
      value.authority !== command.authority ||
      value.aggregateKind !== "stage-run-lease" ||
      value.aggregateId !== command.leaseId
    ) {
      return yield* rpcError("command-identity-mismatch", command);
    }
    if (value.status === "rejected") {
      const code = value.errorCode;
      if (!isLeaseRpcCode(code) || !isReceiptableCode(code)) {
        return yield* rpcError("lease-projection-corrupt", command);
      }
      return Option.some({
        _tag: "Rejected" as const,
        error: rpcError(code, command),
      });
    }
    const result = yield* replayAccepted(
      command,
      value.resultStreamVersion,
      value.resultSequence,
      value.eventCreated,
      value.acceptedAt,
    );
    return Option.some({ _tag: "Accepted" as const, result, events: [] });
  });

  const dispatchFor = (
    authority: AgentControlStageRunLeaseCommandAuthority,
    rawInput: AgentControlStageRunLeaseDispatchInput,
    runId: AgentControlRunOnceId | null = null,
  ) =>
    Effect.gen(function* () {
      const rawCommand = { ...rawInput, authority, holderId };
      const command = yield* decodeCommand(rawCommand).pipe(
        Effect.mapError(() => rpcError("validation", rawInput)),
      );
      const commandFingerprint = yield* fingerprint(command).pipe(
        Effect.mapError(() => rpcError("internal-persistence-error", command)),
      );

      const outcome = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // Receipt-first: no project, mode, source, task, stage-run, or lease
            // precondition is evaluated before immutable command identity replay.
            const replay = yield* replayReceipt(command, commandFingerprint);
            if (Option.isSome(replay)) return replay.value;

            const now = yield* DateTime.now;
            const occurredAt = DateTime.formatIso(now);
            const expiresAt =
              command.type === "agentControl.stageRunLease.reserve" ||
              command.type === "agentControl.stageRunLease.renew"
                ? DateTime.formatIso(DateTime.add(now, { milliseconds: command.leaseDurationMs }))
                : null;
            const eventId = EventId.make(
              yield* crypto.randomUUIDv4.pipe(
                Effect.mapError(() => rpcError("internal-persistence-error", command)),
              ),
            );

            const execute = Effect.fn("AgentControlStageRunLeaseEngine.execute")(function* () {
              const authoritative = yield* loadAuthoritativeLeaseState(
                command.leaseId,
                events,
                states,
              ).pipe(
                Effect.mapError((error) =>
                  rpcError(
                    error._tag === "AgentControlPersistenceSqlError"
                      ? "internal-persistence-error"
                      : "lease-projection-corrupt",
                    command,
                  ),
                ),
              );
              const state = Option.match(authoritative, {
                onNone: () => null,
                onSome: (value) => value.state,
              });
              const observation = {
                commandId: command.commandId,
                leaseId: command.leaseId,
                streamVersion: Option.match(authoritative, {
                  onNone: () => 0,
                  onSome: (value) => value.events[value.events.length - 1]?.streamVersion ?? 0,
                }),
                projectionRevision: state?.revision ?? null,
                fenceToken: state?.fenceToken ?? null,
              };
              yield* transactionHooks.afterAuthoritativeRead(observation);
              const decision = yield* Effect.result(
                decideAgentControlStageRunLeaseCommand({
                  state,
                  command,
                  eventId,
                  occurredAt,
                  expiresAt,
                }),
              );
              if (decision._tag === "Failure") {
                if (!isReceiptableCode(decision.failure.code)) {
                  return yield* decision.failure;
                }
                return yield* insertRejected(
                  command,
                  commandFingerprint,
                  decision.failure.code,
                  state,
                  occurredAt,
                );
              }
              yield* transactionHooks.beforeAppend(observation);
              const appended = yield* events.append({
                leaseId: command.leaseId,
                expectedStreamVersion: state?.revision ?? 0,
                events: decision.success,
              });
              let next = state;
              for (const event of appended) {
                yield* projection.projectEvent(event);
                next = yield* projectAgentControlStageRunLeaseEvent(next, event).pipe(
                  Effect.mapError(() => rpcError("lease-projection-corrupt", command)),
                );
              }
              if (next === null) return yield* rpcError("lease-missing", command);
              const committed = yield* loadAuthoritativeLeaseState(
                command.leaseId,
                events,
                states,
              ).pipe(
                Effect.mapError((error) =>
                  rpcError(
                    error._tag === "AgentControlPersistenceSqlError"
                      ? "internal-persistence-error"
                      : "lease-projection-corrupt",
                    command,
                  ),
                ),
              );
              if (Option.isNone(committed)) {
                return yield* rpcError("lease-projection-corrupt", command);
              }
              next = committed.value.state;
              yield* receipts.insert({
                commandId: command.commandId,
                commandFingerprint,
                authority: command.authority,
                aggregateKind: "stage-run-lease",
                aggregateId: command.leaseId,
                status: "accepted",
                resultSequence: next.sequence,
                resultStreamVersion: next.revision,
                eventCreated: appended.length > 0,
                acceptedAt: occurredAt,
                errorCode: null,
              });
              yield* transactionHooks.beforeTransactionComplete({
                commandId: command.commandId,
                leaseId: command.leaseId,
                streamVersion: next.revision,
                projectionRevision: next.revision,
                fenceToken: next.fenceToken,
              });
              return {
                _tag: "Accepted" as const,
                events: appended,
                result: {
                  state: next,
                  resultSequence: next.sequence,
                  eventCreated: appended.length > 0,
                },
              };
            });

            if (command.type !== "agentControl.stageRunLease.reserve") {
              return yield* execute();
            }

            const useSelected = (task: AgentControlTaskState) =>
              Effect.gen(function* () {
                const sourceIdentityFingerprint =
                  yield* deriveAgentControlSourceIdentityFingerprint(task);
                const stageRunId = yield* deriveAgentControlStageRunId({
                  projectId: command.projectId,
                  taskId: command.taskId,
                  taskRevision: task.revision,
                  githubIntakeSequence: task.githubIntakeSequence,
                  sourceIdentityFingerprint,
                  stageKind: "planning",
                  stageOrdinal: 1,
                });
                const attemptId = yield* deriveAgentControlAttemptId(stageRunId, 1);
                if (
                  command.taskRevision !== task.revision ||
                  command.githubIntakeSequence !== task.githubIntakeSequence ||
                  command.sourceIdentityFingerprint !== sourceIdentityFingerprint ||
                  command.stageRunId !== stageRunId ||
                  command.attemptId !== attemptId
                ) {
                  return yield* rpcError("source-snapshot-stale", command);
                }

                const stageRunResult = yield* Effect.result(
                  loadAuthoritativeInitialStageRunHistory(
                    command.projectId,
                    command.taskId,
                    stageRunEvents,
                    stageRuns,
                  ),
                );
                if (stageRunResult._tag === "Failure") {
                  return yield* rpcError(
                    stageRunResult.failure._tag === "AgentControlPersistenceSqlError"
                      ? "internal-persistence-error"
                      : "stage-run-projection-corrupt",
                    command,
                  );
                }
                if (
                  stageRunResult.success.some((state, index, history) =>
                    history
                      .slice(0, index)
                      .some((candidate) => sameInitialPosition(candidate, state)),
                  )
                ) {
                  return yield* rpcError("stage-run-history-ambiguous", command);
                }
                const stageRun = stageRunResult.success[0];
                if (stageRun === undefined) {
                  return yield* rpcError("stage-run-missing", command);
                }
                if (stageRun.status !== "prepared") {
                  return yield* rpcError("stage-run-not-prepared", command);
                }
                if (
                  stageRun.stageRunId !== command.stageRunId ||
                  stageRun.attemptId !== command.attemptId ||
                  stageRun.taskRevision !== command.taskRevision ||
                  stageRun.githubIntakeSequence !== command.githubIntakeSequence ||
                  stageRun.sourceIdentityFingerprint !== command.sourceIdentityFingerprint
                ) {
                  return yield* rpcError("source-snapshot-stale", command);
                }
                return yield* execute();
              });
            const guarded = yield* Effect.result(
              runId === null
                ? guard.useTaskConsumable(command.projectId, command.taskId, useSelected)
                : requireRunOnceMethod(
                    guard.useTaskSelectedForRunOnce,
                    "AgentControlTaskConsumerGuard.useTaskSelectedForRunOnce",
                  )(runId, command.projectId, command.taskId, useSelected),
            );
            if (guarded._tag === "Success") return guarded.success;
            if (isRpcError(guarded.failure)) {
              if (
                guarded.failure.code === "internal-persistence-error" ||
                guarded.failure.code === "stage-run-projection-corrupt" ||
                guarded.failure.code === "stage-run-history-ambiguous"
              ) {
                return yield* guarded.failure;
              }
              if (!isReceiptableCode(guarded.failure.code)) {
                return yield* guarded.failure;
              }
              return yield* insertRejected(
                command,
                commandFingerprint,
                guarded.failure.code,
                null,
                occurredAt,
              );
            }
            if (guarded.failure._tag === "AgentControlTaskConsumerGuardError") {
              const code = guardCode(guarded.failure.reason);
              if (code === "internal-persistence-error") {
                return yield* rpcError(code, command);
              }
              if (!isReceiptableCode(code)) {
                return yield* rpcError(code, command);
              }
              return yield* insertRejected(command, commandFingerprint, code, null, occurredAt);
            }
            if (guarded.failure._tag === "AgentControlStageRunLeaseStreamVersionConflictError") {
              return yield* rpcError("revision-conflict", command);
            }
            if (
              guarded.failure._tag === "AgentControlPersistenceDecodeError" ||
              guarded.failure._tag === "AgentControlProjectionCorruptError"
            ) {
              return yield* rpcError("lease-projection-corrupt", command);
            }
            return yield* rpcError("internal-persistence-error", command);
          }),
        )
        .pipe(
          Effect.mapError((cause) => {
            if (isRpcError(cause)) return cause;
            if (
              typeof cause === "object" &&
              cause !== null &&
              "_tag" in cause &&
              cause._tag === "AgentControlStageRunLeaseStreamVersionConflictError"
            ) {
              return rpcError("revision-conflict", command);
            }
            if (
              typeof cause === "object" &&
              cause !== null &&
              "_tag" in cause &&
              cause._tag === "AgentControlPersistenceDecodeError"
            ) {
              return rpcError("lease-projection-corrupt", command);
            }
            if (
              typeof cause === "object" &&
              cause !== null &&
              "_tag" in cause &&
              cause._tag === "AgentControlProjectionCorruptError"
            ) {
              return rpcError("lease-projection-corrupt", command);
            }
            return rpcError("internal-persistence-error", command);
          }),
        );
      if (outcome._tag === "Accepted" && outcome.events.length > 0) {
        yield* Effect.forEach(outcome.events, (event) => PubSub.publish(eventPubSub, event), {
          discard: true,
        });
      }
      return outcome;
    });

  const dispatchController: AgentControlStageRunLeaseEngineShape["dispatchController"] = (input) =>
    dispatchFor("controller", input);
  const dispatchControllerForRunOnce: NonNullable<
    AgentControlStageRunLeaseEngineShape["dispatchControllerForRunOnce"]
  > = (runId, input) => dispatchFor("controller", input, runId);
  const dispatchSystem: AgentControlStageRunLeaseEngineShape["dispatchSystem"] = (input) =>
    dispatchFor("system", input);

  const toView: AgentControlStageRunLeaseEngineShape["toView"] = (state) =>
    DateTime.now.pipe(
      Effect.map((now) => {
        const released = state.status === "released";
        const current = state.holderId === holderId;
        const remaining = canonicalTimestampMillis(state.expiresAt)! - DateTime.toEpochMillis(now);
        return {
          leaseId: state.leaseId,
          projectId: state.projectId,
          taskId: state.taskId,
          stageRunId: state.stageRunId,
          attemptId: state.attemptId,
          fenceToken: state.fenceToken,
          status: state.status,
          ownership: released ? "none" : current ? "current-runtime" : "foreign-runtime",
          health: released
            ? "healthy"
            : !current
              ? "recovery-required"
              : remaining <= 0
                ? "expired"
                : remaining <= EXPIRING_WINDOW_MS
                  ? "expiring"
                  : "healthy",
          acquiredAt: state.acquiredAt,
          renewedAt: state.renewedAt,
          expiresAt: state.expiresAt,
          releasedAt: state.releasedAt,
          revision: state.revision,
        } satisfies AgentControlStageRunLeaseView;
      }),
    );

  const rebuild = projection.rebuild.pipe(
    Effect.mapError((error) =>
      error._tag === "AgentControlPersistenceSqlError"
        ? new AgentControlStageRunLeaseRpcError({
            code: "internal-persistence-error",
            operation: "dispatch",
            projectId: internalProjectId,
            taskId: null,
          })
        : new AgentControlStageRunLeaseRpcError({
            code: "lease-projection-corrupt",
            operation: "dispatch",
            projectId: internalProjectId,
            taskId: null,
          }),
    ),
  );
  const publishCommitted: AgentControlStageRunLeaseEngineShape["publishCommitted"] = (committed) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });
  const streamDomainEvents = Stream.fromPubSub(eventPubSub);
  const subscribeDomainEvents = PubSub.subscribe(eventPubSub).pipe(
    Effect.map(Stream.fromSubscription),
  );

  return AgentControlStageRunLeaseEngine.of({
    dispatchController,
    dispatchControllerForRunOnce,
    dispatchSystem,
    toView,
    runtimeHolderId: Effect.succeed(holderId),
    rebuild,
    publishCommitted,
    streamDomainEvents,
    subscribeDomainEvents,
  });
});

export const layer = Layer.effect(AgentControlStageRunLeaseEngine, make);
