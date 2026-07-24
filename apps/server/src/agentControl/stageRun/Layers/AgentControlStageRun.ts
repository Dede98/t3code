import {
  AgentControlStageRunGetInput,
  AgentControlStageRunId,
  AgentControlStageRunListInput,
  AgentControlStageRunPrepareInitialInput,
  AgentControlStageRunRpcError,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlStageRunCommandResult,
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
  AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
  AGENT_CONTROL_INITIAL_STAGE_KIND,
  AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
  AGENT_CONTROL_PLANNING_ROLE_ID,
  deriveAgentControlAttemptId,
  deriveAgentControlSourceIdentityFingerprint,
  deriveAgentControlStageRunId,
  deriveRejectedAgentControlStageRunId,
} from "../identity.ts";
import {
  AgentControlStageRun,
  type AgentControlStageRunShape,
} from "../Services/AgentControlStageRun.ts";
import { AgentControlStageRunEngine } from "../Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunStateRepository } from "../Services/AgentControlStageRunStateRepository.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlStageRunGetInput);
const decodeList = Schema.decodeUnknownEffect(AgentControlStageRunListInput);
const decodePrepare = Schema.decodeUnknownEffect(AgentControlStageRunPrepareInitialInput);
const encodePrepare = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunPrepareInitialInput),
);
const decodeStageRunId = Schema.decodeUnknownEffect(AgentControlStageRunId);

const safeError = (
  code: AgentControlStageRunRpcError["code"],
  operation: AgentControlStageRunRpcError["operation"],
  projectId: AgentControlStageRunGetInput["projectId"],
  taskId: AgentControlStageRunGetInput["taskId"] | null = null,
) => new AgentControlStageRunRpcError({ code, operation, projectId, taskId });

const stageRunReadCode = (error: { readonly _tag: string }): AgentControlStageRunRpcError["code"] =>
  error._tag === "AgentControlPersistenceSqlError"
    ? "internal-persistence-error"
    : "stage-run-projection-corrupt";

const guardCode = (
  reason: AgentControlTaskConsumerGuardReason,
): AgentControlStageRunRpcError["code"] => {
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

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const availability = yield* AgentControlProjectAvailability;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const guard = yield* AgentControlTaskConsumerGuard;
  const engine = yield* AgentControlStageRunEngine;
  const states = yield* AgentControlStageRunStateRepository;

  const fingerprint = Effect.fn("AgentControlStageRun.fingerprint")(function* (
    input: AgentControlStageRunPrepareInitialInput,
  ) {
    const canonical = yield* encodePrepare(input);
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const ensureProject = Effect.fn("AgentControlStageRun.ensureProject")(function* (
    projectId: AgentControlStageRunGetInput["projectId"],
    operation: AgentControlStageRunRpcError["operation"],
  ) {
    const available = yield* Effect.result(availability.ensureAvailable(projectId));
    if (available._tag === "Failure") {
      return yield* safeError(
        available.failure._tag === "AgentControlProjectUnavailableError"
          ? "project-unavailable"
          : "internal-persistence-error",
        operation,
        projectId,
      );
    }
  });

  const replayReceipt = Effect.fn("AgentControlStageRun.replayReceipt")(function* (
    input: AgentControlStageRunPrepareInitialInput,
    commandFingerprint: string,
  ) {
    const existing = yield* receipts
      .getByCommandId(input.commandId)
      .pipe(
        Effect.mapError(() =>
          safeError("internal-persistence-error", "prepare-initial", input.projectId, input.taskId),
        ),
      );
    if (Option.isNone(existing)) return Option.none<AgentControlStageRunCommandResult>();
    const receipt = existing.value;
    if (
      receipt.commandFingerprint !== commandFingerprint ||
      receipt.authority !== "controller" ||
      receipt.aggregateKind !== "stage-run"
    ) {
      return yield* safeError(
        "command-identity-mismatch",
        "prepare-initial",
        input.projectId,
        input.taskId,
      );
    }
    if (receipt.status === "rejected") {
      return yield* safeError(
        "command-previously-rejected",
        "prepare-initial",
        input.projectId,
        input.taskId,
      );
    }
    const stageRunId = yield* decodeStageRunId(receipt.aggregateId).pipe(
      Effect.mapError(() =>
        safeError("stage-run-projection-corrupt", "prepare-initial", input.projectId, input.taskId),
      ),
    );
    const result = yield* engine.replayAccepted({
      stageRunId,
      projectId: input.projectId,
      resultStreamVersion: receipt.resultStreamVersion,
      resultSequence: receipt.resultSequence,
      eventCreated: receipt.eventCreated,
    });
    if (result.state.taskId !== input.taskId) {
      return yield* safeError(
        "command-identity-mismatch",
        "prepare-initial",
        input.projectId,
        input.taskId,
      );
    }
    return Option.some(result);
  });

  const persistGuardRejection = Effect.fn("AgentControlStageRun.persistGuardRejection")(function* (
    input: AgentControlStageRunPrepareInitialInput,
    commandFingerprint: string,
    code: AgentControlStageRunRpcError["code"],
  ) {
    const stageRunId = yield* deriveRejectedAgentControlStageRunId(input);
    const rejectedAt = DateTime.formatIso(yield* DateTime.now);
    yield* sql.withTransaction(
      receipts.insert({
        commandId: input.commandId,
        commandFingerprint,
        authority: "controller",
        aggregateKind: "stage-run",
        aggregateId: stageRunId,
        status: "rejected",
        resultSequence: 0,
        resultStreamVersion: 0,
        eventCreated: false,
        acceptedAt: rejectedAt,
        errorCode: code as AgentControlRejectedCommandErrorCode,
      }),
    );
    return safeError(code, "prepare-initial", input.projectId, input.taskId);
  });

  const getStageRun: AgentControlStageRunShape["getStageRun"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError("validation", "get-stage-run", rawInput.projectId, rawInput.taskId),
        ),
      );
      yield* ensureProject(input.projectId, "get-stage-run");
      const state = yield* states
        .findInitialForTask(input.projectId, input.taskId)
        .pipe(
          Effect.mapError((error) =>
            safeError(stageRunReadCode(error), "get-stage-run", input.projectId, input.taskId),
          ),
        );
      if (Option.isNone(state)) {
        return yield* safeError(
          "stage-run-missing",
          "get-stage-run",
          input.projectId,
          input.taskId,
        );
      }
      return state.value;
    });

  const listStageRuns: AgentControlStageRunShape["listStageRuns"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list-stage-runs", rawInput.projectId)),
      );
      yield* ensureProject(input.projectId, "list-stage-runs");
      const entries = yield* states
        .listProject(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "list-stage-runs", input.projectId),
          ),
        );
      return {
        projectId: input.projectId,
        stageRuns: entries.flatMap((entry) => (entry._tag === "Valid" ? [entry.state] : [])),
        quarantinedCount: entries.filter((entry) => entry._tag === "Corrupt").length,
      };
    });

  const prepareInitial: AgentControlStageRunShape["prepareInitial"] = (rawInput) =>
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
      const replay = yield* replayReceipt(input, commandFingerprint);
      if (Option.isSome(replay)) return replay.value;

      const guarded = yield* Effect.result(
        guard.useTaskConsumable(input.projectId, input.taskId, (task) =>
          Effect.gen(function* () {
            const sourceIdentityFingerprint =
              yield* deriveAgentControlSourceIdentityFingerprint(task);
            const stageRunId = yield* deriveAgentControlStageRunId({
              projectId: input.projectId,
              taskId: input.taskId,
              taskRevision: task.revision,
              githubIntakeSequence: task.githubIntakeSequence,
              sourceIdentityFingerprint,
              stageKind: AGENT_CONTROL_INITIAL_STAGE_KIND,
              stageOrdinal: AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
            });
            const attemptId = yield* deriveAgentControlAttemptId(
              stageRunId,
              AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
            );
            return yield* engine.dispatchPreparedController(
              {
                type: "agentControl.stageRun.prepare",
                commandId: input.commandId,
                projectId: input.projectId,
                taskId: input.taskId,
                stageRunId,
                attemptId,
                roleId: AGENT_CONTROL_PLANNING_ROLE_ID,
                stageKind: AGENT_CONTROL_INITIAL_STAGE_KIND,
                stageOrdinal: AGENT_CONTROL_INITIAL_STAGE_ORDINAL,
                attemptOrdinal: AGENT_CONTROL_INITIAL_ATTEMPT_ORDINAL,
                taskRevision: task.revision,
                githubIntakeSequence: task.githubIntakeSequence,
                sourceIdentityFingerprint,
                expectedRevision: 0,
              },
              commandFingerprint,
            );
          }),
        ),
      );
      if (guarded._tag === "Failure") {
        if (guarded.failure._tag === "AgentControlTaskConsumerGuardError") {
          const code = guardCode(guarded.failure.reason);
          if (code === "internal-persistence-error") {
            return yield* safeError(code, "prepare-initial", input.projectId, input.taskId);
          }
          const persisted = yield* Effect.result(
            persistGuardRejection(input, commandFingerprint, code),
          );
          if (persisted._tag === "Success") return yield* persisted.success;
          const raced = yield* replayReceipt(input, commandFingerprint);
          if (Option.isSome(raced)) return raced.value;
          return yield* safeError(
            "internal-persistence-error",
            "prepare-initial",
            input.projectId,
            input.taskId,
          );
        }
        return yield* guarded.failure;
      }
      if (guarded.success._tag === "Rejected") {
        return yield* guarded.success.error;
      }
      yield* engine.publishCommitted(guarded.success.events);
      return guarded.success.result;
    });

  return AgentControlStageRun.of({ getStageRun, listStageRuns, prepareInitial });
});

export const layer = Layer.effect(AgentControlStageRun, make);
