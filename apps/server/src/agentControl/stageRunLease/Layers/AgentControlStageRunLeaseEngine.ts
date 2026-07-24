import {
  AgentControlStageRunLeaseCommand,
  AgentControlStageRunLeaseRpcError,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlStageRunLeaseCommandAuthority,
  type AgentControlStageRunLeaseCommandResult,
  type AgentControlStageRunLeaseEvent,
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

import { decideAgentControlStageRunLeaseCommand } from "../decider.ts";
import { makeAgentControlStageRunLeaseHolderId } from "../identity.ts";
import { canonicalTimestampMillis, validateAgentControlStageRunLeaseState } from "../invariant.ts";
import { projectAgentControlStageRunLeaseEvent } from "../projector.ts";
import {
  AgentControlStageRunLeaseEngine,
  type AgentControlStageRunLeaseDispatchInput,
  type AgentControlStageRunLeaseDispatchOutcome,
  type AgentControlStageRunLeaseEngineShape,
} from "../Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseEventStore } from "../Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseProjection } from "../Services/AgentControlStageRunLeaseProjection.ts";
import { AgentControlStageRunLeaseStateRepository } from "../Services/AgentControlStageRunLeaseStateRepository.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlSourceIdentityFingerprint,
  deriveAgentControlStageRunId,
} from "../../stageRun/identity.ts";
import { validateInitialAgentControlStageRunState } from "../../stageRun/initialInvariant.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlStageRunLeaseCommand);
const encodeCommand = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunLeaseCommand),
);
const isRpcError = Schema.is(AgentControlStageRunLeaseRpcError);
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
    state.holderId !== command.holderId ||
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
  const stageRuns = yield* AgentControlStageRunStateRepository;
  const guard = yield* AgentControlTaskConsumerGuard;

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
    const canonical = yield* encodeCommand(command);
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const replayAccepted = Effect.fn("AgentControlStageRunLeaseEngine.replayAccepted")(function* (
    command: AgentControlStageRunLeaseCommand,
    resultStreamVersion: number,
    resultSequence: number,
    eventCreated: boolean,
  ) {
    let state: AgentControlStageRunLeaseState | null = null;
    let streamVersion = 0;
    while (streamVersion < resultStreamVersion) {
      const page = yield* events.readStream(
        command.leaseId,
        streamVersion,
        Math.min(500, resultStreamVersion - streamVersion),
      );
      if (page.length === 0) {
        return yield* rpcError("lease-projection-corrupt", command);
      }
      for (const event of page) {
        if (
          event.streamVersion !== streamVersion + 1 ||
          event.streamVersion > resultStreamVersion
        ) {
          return yield* rpcError("lease-projection-corrupt", command);
        }
        state = yield* projectAgentControlStageRunLeaseEvent(state, event).pipe(
          Effect.mapError(() => rpcError("lease-projection-corrupt", command)),
        );
        streamVersion = event.streamVersion;
      }
    }
    if (
      state === null ||
      streamVersion !== resultStreamVersion ||
      state.revision !== resultStreamVersion ||
      state.sequence !== resultSequence ||
      !sameCommandResult(state, command)
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
    code: AgentControlStageRunLeaseRpcError["code"],
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
      errorCode: code as AgentControlRejectedCommandErrorCode,
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
      return Option.some({
        _tag: "Rejected" as const,
        error: rpcError(code as AgentControlStageRunLeaseRpcError["code"], command),
      });
    }
    const result = yield* replayAccepted(
      command,
      value.resultStreamVersion,
      value.resultSequence,
      value.eventCreated,
    );
    return Option.some({ _tag: "Accepted" as const, result, events: [] });
  });

  const dispatchFor = (
    authority: AgentControlStageRunLeaseCommandAuthority,
    rawInput: AgentControlStageRunLeaseDispatchInput,
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
              const state = Option.getOrNull(yield* states.get(command.leaseId));
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
                return yield* insertRejected(
                  command,
                  commandFingerprint,
                  decision.failure.code,
                  state,
                  occurredAt,
                );
              }
              const appended = yield* events.append({
                leaseId: command.leaseId,
                expectedStreamVersion: state?.revision ?? 0,
                events: decision.success,
              });
              let next = state;
              for (const event of appended) {
                yield* projection.projectEvent(event);
                next = yield* projectAgentControlStageRunLeaseEvent(next, event);
              }
              if (next === null) return yield* rpcError("lease-missing", command);
              yield* validateAgentControlStageRunLeaseState(next).pipe(
                Effect.mapError(() => rpcError("lease-projection-corrupt", command)),
              );
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

            const guarded = yield* Effect.result(
              guard.useTaskConsumable(command.projectId, command.taskId, (task) =>
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
                    stageRuns.findInitialForTask(command.projectId, command.taskId),
                  );
                  if (stageRunResult._tag === "Failure") {
                    return yield* rpcError(
                      stageRunResult.failure._tag === "AgentControlPersistenceSqlError"
                        ? "internal-persistence-error"
                        : stageRunResult.failure.operation.includes("ambiguous")
                          ? "stage-run-history-ambiguous"
                          : "stage-run-projection-corrupt",
                      command,
                    );
                  }
                  if (Option.isNone(stageRunResult.success)) {
                    return yield* rpcError("stage-run-missing", command);
                  }
                  const stageRun = yield* validateInitialAgentControlStageRunState(
                    stageRunResult.success.value,
                  ).pipe(Effect.mapError(() => rpcError("stage-run-projection-corrupt", command)));
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
                }),
              ),
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

  return AgentControlStageRunLeaseEngine.of({
    dispatchController,
    dispatchSystem,
    toView,
    rebuild,
    streamDomainEvents: Stream.fromPubSub(eventPubSub),
  });
});

export const layer = Layer.effect(AgentControlStageRunLeaseEngine, make);
