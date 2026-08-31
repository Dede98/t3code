import {
  type AgentControlRunOnceId,
  AgentControlWorktreeCommand,
  AgentControlWorktreeRpcError,
  AgentControlWorktreeRejectedCommandCode,
  EventId,
  type AgentControlTaskState,
  type AgentControlWorktreeCommandResult,
  type AgentControlWorktreeEvent,
  type AgentControlWorktreeRejectedCommandCode as RejectionCode,
  type AgentControlWorktreeReservationState,
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
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { decideAgentControlWorktreeCommand } from "../decider.ts";
import { loadAuthoritativeWorktreeReservation } from "../authoritative.ts";
import { projectAgentControlWorktreeEvent } from "../projector.ts";
import {
  AgentControlWorktreeEngine,
  type AgentControlWorktreeDispatchOutcome,
  type AgentControlWorktreeEngineShape,
} from "../Services/AgentControlWorktreeEngine.ts";
import { requireRunOnceMethod } from "../../runOnce/context.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";
import { AgentControlWorktreeProjection } from "../Services/AgentControlWorktreeProjection.ts";
import { AgentControlWorktreeStateRepository } from "../Services/AgentControlWorktreeStateRepository.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlWorktreeCommand);
const encodeCommand = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeCommand),
);
const isRpcError = Schema.is(AgentControlWorktreeRpcError);
const isWorktreeCode = Schema.is(AgentControlWorktreeRejectedCommandCode);
const RECEIPTABLE = new Set<RejectionCode>([
  "validation",
  "project-unavailable",
  "project-mode-inactive",
  "task-missing",
  "task-not-candidate",
  "task-ineligible",
  "task-stage-inactive",
  "source-snapshot-unavailable",
  "source-snapshot-stale",
  "source-watermark-stale",
  "stage-run-missing",
  "stage-run-not-prepared",
  "stage-run-history-ambiguous",
  "lease-missing",
  "lease-not-reserved",
  "lease-expired",
  "lease-foreign-runtime",
  "lease-recovery-required",
  "fence-token-mismatch",
  "reservation-missing",
  "reservation-conflict",
  "revision-conflict",
  "state-not-available",
  "command-identity-mismatch",
]);

const rpcError = (
  code: AgentControlWorktreeRpcError["code"],
  command: AgentControlWorktreeCommand,
) =>
  new AgentControlWorktreeRpcError({
    code,
    operation: command.type === "agentControl.worktree.reserve" ? "reserve" : "materialize",
    projectId: command.projectId,
    taskId: command.taskId,
    reservationId: command.reservationId,
  });

const guardCode = (reason: AgentControlTaskConsumerGuardReason): RejectionCode => {
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

const commandBindingMatchesTask = Effect.fn("AgentControlWorktreeEngine.commandBindingMatchesTask")(
  function* (command: AgentControlWorktreeCommand, task: AgentControlTaskState) {
    const fingerprint = yield* deriveAgentControlSourceIdentityFingerprint(task);
    return (
      command.taskRevision === task.revision &&
      command.githubIntakeSequence === task.githubIntakeSequence &&
      command.sourceIdentityFingerprint === fingerprint
    );
  },
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlWorktreeEventStore;
  const projection = yield* AgentControlWorktreeProjection;
  const states = yield* AgentControlWorktreeStateRepository;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const guard = yield* AgentControlTaskConsumerGuard;
  const github = yield* AgentControlGithubStateRepository;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;

  yield* projection.bootstrap.pipe(
    Effect.mapError(
      () =>
        new AgentControlWorktreeRpcError({
          code: "reservation-projection-corrupt",
          operation: "reconcile",
          projectId: "agent-control-worktree-internal" as never,
          taskId: null,
          reservationId: null,
        }),
    ),
  );
  const eventPubSub = yield* PubSub.unbounded<AgentControlWorktreeEvent>();

  const fingerprint = Effect.fn("AgentControlWorktreeEngine.fingerprint")(function* (
    command: AgentControlWorktreeCommand,
  ) {
    const canonical = yield* encodeCommand(command);
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const authoritativeRecord = Effect.fn("AgentControlWorktreeEngine.authoritativeRecord")(
    function* (command: AgentControlWorktreeCommand) {
      return yield* loadAuthoritativeWorktreeReservation(
        command.reservationId,
        events,
        states,
      ).pipe(
        Effect.mapError((error) =>
          rpcError(
            error._tag === "AgentControlPersistenceSqlError"
              ? "internal-persistence-error"
              : "reservation-projection-corrupt",
            command,
          ),
        ),
        Effect.map(Option.getOrNull),
      );
    },
  );

  const authoritative = Effect.fn("AgentControlWorktreeEngine.authoritative")(function* (
    command: AgentControlWorktreeCommand,
  ) {
    return (yield* authoritativeRecord(command))?.state ?? null;
  });

  const insertRejected = (
    command: AgentControlWorktreeCommand,
    commandFingerprint: string,
    code: RejectionCode,
    state: AgentControlWorktreeReservationState | null,
    at: string,
  ) =>
    receipts
      .insert({
        commandId: command.commandId,
        commandFingerprint,
        authority: "controller",
        aggregateKind: "worktree-reservation",
        aggregateId: command.reservationId,
        status: "rejected",
        resultSequence: state?.sequence ?? 0,
        resultStreamVersion: state?.revision ?? 0,
        eventCreated: false,
        acceptedAt: at,
        errorCode: code,
      })
      .pipe(
        Effect.as({
          _tag: "Rejected" as const,
          error: rpcError(code, command),
        }),
      );

  const replayReceipt = Effect.fn("AgentControlWorktreeEngine.replayReceipt")(function* (
    command: AgentControlWorktreeCommand,
    commandFingerprint: string,
  ): Effect.fn.Return<
    Option.Option<AgentControlWorktreeDispatchOutcome>,
    AgentControlWorktreeRpcError
  > {
    const receipt = yield* receipts
      .getByCommandId(command.commandId)
      .pipe(Effect.mapError(() => rpcError("internal-persistence-error", command)));
    if (Option.isNone(receipt)) return Option.none<AgentControlWorktreeDispatchOutcome>();
    const value = receipt.value;
    if (
      value.commandFingerprint !== commandFingerprint ||
      value.authority !== "controller" ||
      value.aggregateKind !== "worktree-reservation" ||
      value.aggregateId !== command.reservationId
    ) {
      return yield* rpcError("command-identity-mismatch", command);
    }
    if (value.status === "rejected") {
      if (!isWorktreeCode(value.errorCode) || !RECEIPTABLE.has(value.errorCode)) {
        return yield* rpcError("reservation-projection-corrupt", command);
      }
      return Option.some({
        _tag: "Rejected",
        error: rpcError(value.errorCode, command),
      } satisfies AgentControlWorktreeDispatchOutcome);
    }
    const authoritativeState = yield* authoritativeRecord(command);
    if (
      authoritativeState === null ||
      value.resultStreamVersion < 1 ||
      value.resultStreamVersion > authoritativeState.statesByVersion.length
    ) {
      return yield* rpcError("reservation-projection-corrupt", command);
    }
    const resultState = authoritativeState.statesByVersion[value.resultStreamVersion - 1];
    const resultEvent = authoritativeState.events[value.resultStreamVersion - 1];
    if (
      resultState === undefined ||
      resultEvent === undefined ||
      resultState.revision !== value.resultStreamVersion ||
      resultState.sequence !== value.resultSequence ||
      (value.eventCreated && resultEvent.commandId !== command.commandId)
    ) {
      return yield* rpcError("reservation-projection-corrupt", command);
    }
    return Option.some({
      _tag: "Accepted",
      events: [],
      result: {
        state: resultState,
        resultSequence: value.resultSequence,
        eventCreated: value.eventCreated,
      },
    } satisfies AgentControlWorktreeDispatchOutcome);
  });

  const ensureAdmission = Effect.fn("AgentControlWorktreeEngine.ensureAdmission")(function* (
    command: AgentControlWorktreeCommand,
    current: AgentControlWorktreeReservationState | null,
    runId: AgentControlRunOnceId | null,
  ) {
    const useSelected = (task: AgentControlTaskState) =>
      Effect.gen(function* () {
        if (!(yield* commandBindingMatchesTask(command, task))) {
          return yield* rpcError("source-snapshot-stale", command);
        }
        const repository =
          command.type === "agentControl.worktree.reserve"
            ? command.repository
            : current?.repository;
        if (repository === undefined) {
          return yield* rpcError("reservation-missing", command);
        }
        const repositoryWorkspace =
          command.type === "agentControl.worktree.reserve"
            ? command.repositoryWorkspace
            : current?.repositoryWorkspace;
        const projectRows = yield* sql<{
          readonly workspaceRoot: unknown;
          readonly deletedAt: unknown;
        }>`
          SELECT workspace_root AS "workspaceRoot", deleted_at AS "deletedAt"
          FROM projection_projects WHERE project_id = ${command.projectId}
        `.pipe(Effect.mapError(() => rpcError("internal-persistence-error", command)));
        if (
          repositoryWorkspace === undefined ||
          projectRows[0]?.deletedAt !== null ||
          projectRows[0]?.workspaceRoot !== repositoryWorkspace
        ) {
          return yield* rpcError("project-unavailable", command);
        }
        const githubState = yield* github
          .get(command.projectId)
          .pipe(Effect.mapError(() => rpcError("internal-persistence-error", command)));
        if (
          Option.isNone(githubState) ||
          githubState.value.config === null ||
          githubState.value.config.repository.repositoryNodeId !== repository.repositoryNodeId ||
          githubState.value.config.repository.nameWithOwner !== repository.nameWithOwner ||
          task.source.repositoryNodeId !== repository.repositoryNodeId
        ) {
          return yield* rpcError("source-snapshot-stale", command);
        }
        const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
          command.projectId,
          command.taskId,
          stageEvents,
          stageStates,
        ).pipe(
          Effect.mapError((error) =>
            rpcError(
              error._tag === "AgentControlPersistenceSqlError"
                ? "internal-persistence-error"
                : "stage-run-projection-corrupt",
              command,
            ),
          ),
        );
        const matches = stageHistory.filter(
          (stage) =>
            stage.stageRunId === command.stageRunId &&
            stage.attemptId === command.attemptId &&
            stage.taskRevision === command.taskRevision &&
            stage.githubIntakeSequence === command.githubIntakeSequence &&
            stage.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
            stage.stageKind === "planning" &&
            stage.roleId === "planning" &&
            stage.stageOrdinal === 1 &&
            stage.attemptOrdinal === 1,
        );
        if (matches.length === 0) return yield* rpcError("stage-run-missing", command);
        if (matches.length !== 1) return yield* rpcError("stage-run-history-ambiguous", command);
        if (matches[0]!.status !== "prepared") {
          return yield* rpcError("stage-run-not-prepared", command);
        }
        const lease = yield* loadAuthoritativeLeaseState(
          command.leaseId,
          leaseEvents,
          leaseStates,
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
        if (Option.isNone(lease)) return yield* rpcError("lease-missing", command);
        const value = lease.value.state;
        if (value.status !== "reserved") return yield* rpcError("lease-not-reserved", command);
        if (value.holderId !== runtimeHolderId) {
          return yield* rpcError("lease-foreign-runtime", command);
        }
        if (value.fenceToken !== command.fenceToken) {
          return yield* rpcError("fence-token-mismatch", command);
        }
        if (
          value.projectId !== command.projectId ||
          value.taskId !== command.taskId ||
          value.stageRunId !== command.stageRunId ||
          value.attemptId !== command.attemptId ||
          value.taskRevision !== command.taskRevision ||
          value.githubIntakeSequence !== command.githubIntakeSequence ||
          value.sourceIdentityFingerprint !== command.sourceIdentityFingerprint
        ) {
          return yield* rpcError("source-snapshot-stale", command);
        }
        const now = yield* DateTime.now;
        const expiresAt = canonicalTimestampMillis(value.expiresAt);
        if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
          return yield* rpcError("lease-expired", command);
        }
        return task;
      });
    return yield* runId === null
      ? guard.useTaskConsumable(command.projectId, command.taskId, useSelected)
      : requireRunOnceMethod(
          guard.useTaskSelectedForRunOnce,
          "AgentControlTaskConsumerGuard.useTaskSelectedForRunOnce",
        )(runId, command.projectId, command.taskId, useSelected);
  });

  const dispatchFor = (
    runId: AgentControlRunOnceId | null,
    rawCommand: AgentControlWorktreeCommand,
  ) =>
    Effect.gen(function* () {
      const command = yield* decodeCommand(rawCommand).pipe(
        Effect.mapError(() => rpcError("validation", rawCommand)),
      );
      const commandFingerprint = yield* fingerprint(command).pipe(
        Effect.mapError(() => rpcError("internal-persistence-error", command)),
      );
      const outcome = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const replay = yield* replayReceipt(command, commandFingerprint);
            if (Option.isSome(replay)) return replay.value;
            const occurredAt = DateTime.formatIso(yield* DateTime.now);
            const current = yield* authoritative(command);
            const admitted = yield* Effect.result(ensureAdmission(command, current, runId));
            if (admitted._tag === "Failure") {
              const failure = admitted.failure;
              const code =
                failure._tag === "AgentControlTaskConsumerGuardError"
                  ? guardCode(failure.reason)
                  : isRpcError(failure)
                    ? failure.code
                    : "internal-persistence-error";
              if (!RECEIPTABLE.has(code)) {
                return yield* isRpcError(failure) ? failure : rpcError(code, command);
              }
              return yield* insertRejected(command, commandFingerprint, code, null, occurredAt);
            }
            const decision = yield* Effect.result(
              decideAgentControlWorktreeCommand({
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
              if (!RECEIPTABLE.has(decision.failure.code)) return yield* decision.failure;
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
                    reservationId: command.reservationId,
                    expectedStreamVersion: current?.revision ?? 0,
                    events: decision.success,
                  });
            let next = current;
            for (const event of appended) {
              yield* projection.projectEvent(event);
              next = yield* projectAgentControlWorktreeEvent(next, event).pipe(
                Effect.mapError(() => rpcError("reservation-projection-corrupt", command)),
              );
            }
            if (next === null) return yield* rpcError("reservation-missing", command);
            yield* receipts.insert({
              commandId: command.commandId,
              commandFingerprint,
              authority: "controller",
              aggregateKind: "worktree-reservation",
              aggregateId: command.reservationId,
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
              } satisfies AgentControlWorktreeCommandResult,
            };
          }),
        )
        .pipe(
          Effect.mapError((cause) =>
            isRpcError(cause) ? cause : rpcError("internal-persistence-error", command),
          ),
        );
      if (outcome._tag === "Accepted" && outcome.events.length > 0) {
        yield* Effect.forEach(outcome.events, (event) => PubSub.publish(eventPubSub, event), {
          discard: true,
        });
      }
      return outcome;
    });

  const loadAuthoritative: AgentControlWorktreeEngine["Service"]["loadAuthoritative"] = (
    reservationId,
  ) =>
    loadAuthoritativeWorktreeReservation(reservationId, events, states).pipe(
      Effect.map(Option.getOrNull),
      Effect.map((value) => value?.state ?? null),
      Effect.mapError(
        (error) =>
          new AgentControlWorktreeRpcError({
            code:
              error._tag === "AgentControlPersistenceSqlError"
                ? "internal-persistence-error"
                : "reservation-projection-corrupt",
            operation: "reconcile",
            projectId: "agent-control-worktree-internal" as never,
            taskId: null,
            reservationId,
          }),
      ),
    );

  const dispatchController: AgentControlWorktreeEngineShape["dispatchController"] = (command) =>
    dispatchFor(null, command);
  const dispatchControllerForRunOnce: NonNullable<
    AgentControlWorktreeEngineShape["dispatchControllerForRunOnce"]
  > = (runId, command) => dispatchFor(runId, command);

  return AgentControlWorktreeEngine.of({
    dispatchController,
    dispatchControllerForRunOnce,
    loadAuthoritative,
    rebuild: projection.rebuild.pipe(
      Effect.mapError(
        () =>
          new AgentControlWorktreeRpcError({
            code: "reservation-projection-corrupt",
            operation: "reconcile",
            projectId: "agent-control-worktree-internal" as never,
            taskId: null,
            reservationId: null,
          }),
      ),
    ),
    streamDomainEvents: Stream.fromPubSub(eventPubSub),
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const layer = Layer.effect(AgentControlWorktreeEngine, make);
