import {
  AgentControlStageRunCommand,
  AgentControlStageRunRpcError,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlStageRunPrepareCommand,
  type AgentControlStageRunCommandResult,
  type AgentControlStageRunEvent,
  type AgentControlStageRunState,
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

import { decideAgentControlStageRunCommand } from "../decider.ts";
import { deriveAgentControlAttemptId, deriveAgentControlStageRunId } from "../identity.ts";
import { validateInitialAgentControlStageRunState } from "../initialInvariant.ts";
import { projectAgentControlStageRunEvent } from "../projector.ts";
import {
  AgentControlStageRunEngine,
  type AgentControlStageRunEngineShape,
} from "../Services/AgentControlStageRunEngine.ts";
import { AgentControlStageRunEventStore } from "../Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunProjection } from "../Services/AgentControlStageRunProjection.ts";
import { AgentControlStageRunStateRepository } from "../Services/AgentControlStageRunStateRepository.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlStageRunCommand);
const isRpcError = Schema.is(AgentControlStageRunRpcError);
const internalProjectId = ProjectIdSchema.make("agent-control-stage-run-internal");

const rpcError = (
  code: AgentControlStageRunRpcError["code"],
  command: AgentControlStageRunCommand,
) =>
  new AgentControlStageRunRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
  });

const repositoryReadError = (
  error: { readonly _tag: string },
  command: AgentControlStageRunCommand,
) =>
  rpcError(
    error._tag === "AgentControlPersistenceSqlError"
      ? "internal-persistence-error"
      : "stage-run-projection-corrupt",
    command,
  );

const sameAcceptedCommand = (
  state: AgentControlStageRunState,
  command: AgentControlStageRunPrepareCommand,
) =>
  state.projectId === command.projectId &&
  state.taskId === command.taskId &&
  state.stageRunId === command.stageRunId &&
  state.attemptId === command.attemptId &&
  state.roleId === command.roleId &&
  state.stageKind === command.stageKind &&
  state.stageOrdinal === command.stageOrdinal &&
  state.attemptOrdinal === command.attemptOrdinal &&
  state.status === "prepared" &&
  state.taskRevision === command.taskRevision &&
  state.githubIntakeSequence === command.githubIntakeSequence &&
  state.sourceIdentityFingerprint === command.sourceIdentityFingerprint &&
  state.revision === 1 &&
  command.expectedRevision === 0;

const make = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlStageRunEventStore;
  const projection = yield* AgentControlStageRunProjection;
  const states = yield* AgentControlStageRunStateRepository;

  yield* projection.bootstrap.pipe(
    Effect.mapError(
      () =>
        new AgentControlStageRunRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId: internalProjectId,
          taskId: null,
        }),
    ),
  );

  const eventPubSub = yield* PubSub.unbounded<AgentControlStageRunEvent>();

  const replayAccepted: AgentControlStageRunEngineShape["replayAccepted"] = (input) =>
    states.get(input.stageRunId).pipe(
      Effect.mapError((error) =>
        error._tag === "AgentControlPersistenceSqlError"
          ? new AgentControlStageRunRpcError({
              code: "internal-persistence-error",
              operation: "prepare-initial",
              projectId: input.projectId,
              taskId: null,
            })
          : new AgentControlStageRunRpcError({
              code: "stage-run-projection-corrupt",
              operation: "prepare-initial",
              projectId: input.projectId,
              taskId: null,
            }),
      ),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(
              new AgentControlStageRunRpcError({
                code: "stage-run-missing",
                operation: "prepare-initial",
                projectId: input.projectId,
                taskId: null,
              }),
            ),
          onSome: (state) =>
            validateInitialAgentControlStageRunState(state).pipe(
              Effect.mapError(
                () =>
                  new AgentControlStageRunRpcError({
                    code: "stage-run-projection-corrupt",
                    operation: "prepare-initial",
                    projectId: input.projectId,
                    taskId: state.taskId,
                  }),
              ),
              Effect.flatMap((state) =>
                state.projectId !== input.projectId ||
                state.revision !== input.resultStreamVersion ||
                state.sequence !== input.resultSequence
                  ? Effect.fail(
                      new AgentControlStageRunRpcError({
                        code: "stage-run-projection-corrupt",
                        operation: "prepare-initial",
                        projectId: input.projectId,
                        taskId: state.taskId,
                      }),
                    )
                  : Effect.succeed({
                      state,
                      resultSequence: input.resultSequence,
                      eventCreated: input.eventCreated,
                    } satisfies AgentControlStageRunCommandResult),
              ),
            ),
        }),
      ),
    );

  const dispatchPreparedController: AgentControlStageRunEngineShape["dispatchPreparedController"] =
    (rawCommand, commandFingerprint) =>
      Effect.gen(function* () {
        const command = yield* decodeCommand(rawCommand).pipe(
          Effect.mapError(
            () =>
              new AgentControlStageRunRpcError({
                code: "validation",
                operation: "dispatch",
                projectId: rawCommand.projectId,
                taskId: rawCommand.taskId,
              }),
          ),
        );
        if (commandFingerprint.trim().length === 0) {
          return yield* rpcError("validation", command);
        }
        yield* states
          .findInitialForTask(command.projectId, command.taskId)
          .pipe(Effect.mapError((error) => repositoryReadError(error, command)));
        const occurredAt = DateTime.formatIso(yield* DateTime.now);

        const insertRejected = Effect.fn("AgentControlStageRunEngine.insertRejected")(function* (
          code: AgentControlStageRunRpcError["code"],
          state: AgentControlStageRunState | null,
        ) {
          yield* receipts.insert({
            commandId: command.commandId,
            commandFingerprint,
            authority: "controller",
            aggregateKind: "stage-run",
            aggregateId: command.stageRunId,
            status: "rejected",
            resultSequence: state?.sequence ?? 0,
            resultStreamVersion: state?.revision ?? 0,
            eventCreated: false,
            acceptedAt: occurredAt,
            errorCode: code as AgentControlRejectedCommandErrorCode,
          });
          return {
            _tag: "Rejected" as const,
            error: rpcError(code, command),
          };
        });

        const existingReceipt = yield* receipts.getByCommandId(command.commandId);
        if (Option.isSome(existingReceipt)) {
          const receipt = existingReceipt.value;
          if (
            receipt.commandFingerprint !== commandFingerprint ||
            receipt.authority !== "controller" ||
            receipt.aggregateKind !== "stage-run" ||
            receipt.aggregateId !== command.stageRunId
          ) {
            return yield* rpcError("command-identity-mismatch", command);
          }
          if (receipt.status === "rejected") {
            return yield* rpcError("command-previously-rejected", command);
          }
          if (command.type !== "agentControl.stageRun.prepare") {
            return yield* rpcError("command-identity-mismatch", command);
          }
          const expectedStageRunId = yield* deriveAgentControlStageRunId(command);
          const expectedAttemptId = yield* deriveAgentControlAttemptId(
            expectedStageRunId,
            command.attemptOrdinal,
          );
          if (
            expectedStageRunId !== command.stageRunId ||
            expectedAttemptId !== command.attemptId
          ) {
            return yield* rpcError("command-identity-mismatch", command);
          }
          const result = yield* replayAccepted({
            stageRunId: command.stageRunId,
            projectId: command.projectId,
            resultStreamVersion: receipt.resultStreamVersion,
            resultSequence: receipt.resultSequence,
            eventCreated: receipt.eventCreated,
          });
          if (!sameAcceptedCommand(result.state, command)) {
            return yield* rpcError("command-identity-mismatch", command);
          }
          return { _tag: "Accepted" as const, result, events: [] };
        }

        if (command.type === "agentControl.stageRun.status.set") {
          return yield* insertRejected("state-not-available", null);
        }

        const expectedStageRunId = yield* deriveAgentControlStageRunId(command);
        const expectedAttemptId = yield* deriveAgentControlAttemptId(
          command.stageRunId,
          command.attemptOrdinal,
        );
        if (expectedStageRunId !== command.stageRunId || expectedAttemptId !== command.attemptId) {
          return yield* insertRejected("stage-run-identity-conflict", null);
        }

        const snapshotState = yield* states
          .findBySnapshot(command)
          .pipe(Effect.mapError((error) => repositoryReadError(error, command)));
        const streamState = yield* states
          .get(command.stageRunId)
          .pipe(Effect.mapError((error) => repositoryReadError(error, command)));
        if (
          Option.isSome(snapshotState) &&
          Option.isSome(streamState) &&
          snapshotState.value.stageRunId !== streamState.value.stageRunId
        ) {
          return yield* insertRejected("stage-run-identity-conflict", streamState.value);
        }
        const state = Option.getOrNull(Option.isSome(streamState) ? streamState : snapshotState);
        const eventId = EventId.make(
          yield* crypto.randomUUIDv4.pipe(
            Effect.mapError(() => rpcError("internal-persistence-error", command)),
          ),
        );
        const decision = yield* Effect.result(
          decideAgentControlStageRunCommand({ state, command, eventId, occurredAt }),
        );
        if (decision._tag === "Failure") {
          return yield* insertRejected(decision.failure.code, state);
        }

        const appended =
          decision.success.length === 0
            ? []
            : yield* events.append({
                stageRunId: command.stageRunId,
                expectedStreamVersion: state?.revision ?? 0,
                events: decision.success,
              });
        let next = state;
        for (const event of appended) {
          yield* projection.projectEvent(event);
          next = yield* projectAgentControlStageRunEvent(next, event);
        }
        if (next === null) return yield* rpcError("stage-run-missing", command);
        const eventCreated = appended.length > 0;
        yield* receipts.insert({
          commandId: command.commandId,
          commandFingerprint,
          authority: "controller",
          aggregateKind: "stage-run",
          aggregateId: command.stageRunId,
          status: "accepted",
          resultSequence: next.sequence,
          resultStreamVersion: next.revision,
          eventCreated,
          acceptedAt: occurredAt,
          errorCode: null,
        });
        return {
          _tag: "Accepted" as const,
          events: appended,
          result: {
            state: next,
            resultSequence: next.sequence,
            eventCreated,
          },
        };
      }).pipe(
        Effect.mapError((cause) => {
          if (isRpcError(cause)) return cause;
          if (
            typeof cause === "object" &&
            cause !== null &&
            "_tag" in cause &&
            cause._tag === "AgentControlPersistenceDecodeError"
          ) {
            return new AgentControlStageRunRpcError({
              code: "stage-run-projection-corrupt",
              operation: "dispatch",
              projectId: rawCommand.projectId,
              taskId: rawCommand.taskId,
            });
          }
          if (
            typeof cause === "object" &&
            cause !== null &&
            "_tag" in cause &&
            cause._tag === "AgentControlStageRunStreamVersionConflictError"
          ) {
            return new AgentControlStageRunRpcError({
              code: "revision-conflict",
              operation: "dispatch",
              projectId: rawCommand.projectId,
              taskId: rawCommand.taskId,
            });
          }
          return new AgentControlStageRunRpcError({
            code: "internal-persistence-error",
            operation: "dispatch",
            projectId: rawCommand.projectId,
            taskId: rawCommand.taskId,
          });
        }),
      );

  const get: AgentControlStageRunEngineShape["get"] = (stageRunId) =>
    states.get(stageRunId).pipe(
      Effect.mapError((error) =>
        error._tag === "AgentControlPersistenceSqlError"
          ? new AgentControlStageRunRpcError({
              code: "internal-persistence-error",
              operation: "get-stage-run",
              projectId: internalProjectId,
              taskId: null,
            })
          : new AgentControlStageRunRpcError({
              code: "stage-run-projection-corrupt",
              operation: "get-stage-run",
              projectId: internalProjectId,
              taskId: null,
            }),
      ),
    );
  const publishCommitted: AgentControlStageRunEngineShape["publishCommitted"] = (committed) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });
  const rebuild = projection.rebuild.pipe(
    Effect.mapError((error) =>
      error._tag === "AgentControlPersistenceSqlError"
        ? new AgentControlStageRunRpcError({
            code: "internal-persistence-error",
            operation: "dispatch",
            projectId: internalProjectId,
            taskId: null,
          })
        : new AgentControlStageRunRpcError({
            code: "stage-run-projection-corrupt",
            operation: "dispatch",
            projectId: internalProjectId,
            taskId: null,
          }),
    ),
  );

  return AgentControlStageRunEngine.of({
    get,
    dispatchPreparedController,
    replayAccepted,
    publishCommitted,
    rebuild,
    streamDomainEvents: Stream.fromPubSub(eventPubSub),
  });
});

export const layer = Layer.effect(AgentControlStageRunEngine, make);
