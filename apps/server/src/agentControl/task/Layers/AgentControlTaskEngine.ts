import {
  AgentControlTaskCommand,
  AgentControlTaskId,
  AgentControlTaskRpcError,
  type AgentControlTaskCommandResult,
  type AgentControlTaskEvent,
  type AgentControlTaskState,
  type AgentControlRejectedCommandErrorCode,
  type ProjectId,
  ProjectId as ProjectIdSchema,
  EventId,
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

import { decideAgentControlTaskCommand } from "../decider.ts";
import { deriveAgentControlTaskId } from "../identity.ts";
import { projectAgentControlTaskEvent } from "../projector.ts";
import {
  AgentControlTaskEngine,
  type AgentControlTaskEngineShape,
} from "../Services/AgentControlTaskEngine.ts";
import { AgentControlTaskEventStore } from "../Services/AgentControlTaskEventStore.ts";
import { AgentControlTaskProjection } from "../Services/AgentControlTaskProjection.ts";
import { AgentControlTaskStateRepository } from "../Services/AgentControlTaskStateRepository.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../../persistence/Services/AgentControlProjectStates.ts";
import { AgentControlGithubStateRepository } from "../../github/Services/AgentControlGithubStateRepository.ts";

const decodeCommand = Schema.decodeUnknownEffect(AgentControlTaskCommand);
const encodeCommand = Schema.encodeUnknownEffect(Schema.fromJsonString(AgentControlTaskCommand));
const isTaskRpcError = Schema.is(AgentControlTaskRpcError);
const internalProjectId = ProjectIdSchema.make("agent-control-task-internal");

const rpcError = (code: AgentControlTaskRpcError["code"], command: AgentControlTaskCommand) =>
  new AgentControlTaskRpcError({
    code,
    operation: "dispatch",
    projectId: command.projectId,
    taskId: command.taskId,
  });

const makeEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const availability = yield* AgentControlProjectAvailability;
  const projects = yield* AgentControlProjectStateRepository;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const events = yield* AgentControlTaskEventStore;
  const projection = yield* AgentControlTaskProjection;
  const states = yield* AgentControlTaskStateRepository;
  const github = yield* AgentControlGithubStateRepository;

  yield* projection.bootstrap.pipe(
    Effect.mapError(
      () =>
        new AgentControlTaskRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId: internalProjectId,
          taskId: null,
        }),
    ),
  );

  const eventPubSub = yield* PubSub.unbounded<AgentControlTaskEvent>();

  const commandFingerprint = Effect.fn("AgentControlTaskEngine.commandFingerprint")(function* (
    command: AgentControlTaskCommand,
  ) {
    const canonical = yield* encodeCommand(command);
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const loadStateAtRevision = Effect.fn("AgentControlTaskEngine.loadStateAtRevision")(function* (
    taskId: AgentControlTaskId,
    projectId: ProjectId,
    revision: number,
  ) {
    let state: AgentControlTaskState | null = null;
    while ((state?.revision ?? 0) < revision) {
      const page: ReadonlyArray<AgentControlTaskEvent> = yield* events.readStream(
        taskId,
        state?.revision ?? 0,
        Math.min(500, revision - (state?.revision ?? 0)),
      );
      if (page.length === 0) {
        return yield* new AgentControlTaskRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId,
          taskId,
        });
      }
      for (const event of page) {
        state = yield* projectAgentControlTaskEvent(state, event);
      }
    }
    if (state === null || state.revision !== revision) {
      return yield* new AgentControlTaskRpcError({
        code: "internal-persistence-error",
        operation: "dispatch",
        projectId,
        taskId,
      });
    }
    return state;
  });

  const dispatch = (
    rawCommand: AgentControlTaskCommand,
    requireObserveMode: boolean,
  ): ReturnType<AgentControlTaskEngineShape["dispatchController"]> =>
    Effect.gen(function* () {
      const command = yield* decodeCommand(rawCommand).pipe(
        Effect.mapError(
          () =>
            new AgentControlTaskRpcError({
              code: "validation",
              operation: "dispatch",
              projectId: rawCommand.projectId,
              taskId: rawCommand.taskId,
            }),
        ),
      );
      const fingerprint = yield* commandFingerprint(command).pipe(
        Effect.mapError(() => rpcError("internal-persistence-error", command)),
      );
      const occurredAt = DateTime.formatIso(yield* DateTime.now);
      const eventId = EventId.make(
        yield* crypto.randomUUIDv4.pipe(
          Effect.mapError(() => rpcError("internal-persistence-error", command)),
        ),
      );

      const committed = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const existing = yield* receipts.getByCommandId(command.commandId);
            if (Option.isSome(existing)) {
              const receipt = existing.value;
              if (
                receipt.commandFingerprint !== fingerprint ||
                receipt.authority !== "controller" ||
                receipt.aggregateKind !== "task" ||
                receipt.aggregateId !== command.taskId
              ) {
                return yield* rpcError("command-identity-mismatch", command);
              }
              if (receipt.status === "rejected") {
                return yield* rpcError("command-previously-rejected", command);
              }
              const state = yield* loadStateAtRevision(
                command.taskId,
                command.projectId,
                receipt.resultStreamVersion,
              );
              if (state.sequence !== receipt.resultSequence) {
                return yield* rpcError("internal-persistence-error", command);
              }
              return {
                _tag: "Accepted" as const,
                events: [] as ReadonlyArray<AgentControlTaskEvent>,
                result: {
                  state,
                  resultSequence: receipt.resultSequence,
                  eventCreated: receipt.eventCreated,
                } satisfies AgentControlTaskCommandResult,
              };
            }

            if (requireObserveMode) {
              const available = yield* Effect.result(
                availability.ensureAvailable(command.projectId),
              );
              if (available._tag === "Failure") {
                return yield* rpcError(
                  available.failure._tag === "AgentControlProjectUnavailableError"
                    ? available.failure.reason === "missing"
                      ? "project-missing"
                      : "project-deleted"
                    : "internal-persistence-error",
                  command,
                );
              }
              const project = yield* projects.get(command.projectId);
              if (Option.isNone(project) || project.value.mode !== "observe") {
                return yield* rpcError("project-mode-inactive", command);
              }
            }

            const available = yield* Effect.result(availability.ensureAvailable(command.projectId));
            if (available._tag === "Failure") {
              const code =
                available.failure._tag === "AgentControlProjectUnavailableError"
                  ? available.failure.reason === "missing"
                    ? "project-missing"
                    : "project-deleted"
                  : "internal-persistence-error";
              const rejected = rpcError(code, command);
              yield* receipts.insert({
                commandId: command.commandId,
                commandFingerprint: fingerprint,
                authority: "controller",
                aggregateKind: "task",
                aggregateId: command.taskId,
                status: "rejected",
                resultSequence: 0,
                resultStreamVersion: 0,
                eventCreated: false,
                acceptedAt: occurredAt,
                errorCode: code,
              });
              return { _tag: "Rejected" as const, error: rejected };
            }

            if (command.type !== "agentControl.task.status.set") {
              const sourceMatches =
                command.sourcePrecondition.projectId === command.projectId &&
                command.sourcePrecondition.githubIntakeSequence === command.githubIntakeSequence &&
                command.sourcePrecondition.repositoryNodeId === command.source.repositoryNodeId &&
                (yield* github.matchesCompletedSnapshot(command.sourcePrecondition));
              if (!sourceMatches) {
                const rejected = rpcError("source-snapshot-stale", command);
                yield* receipts.insert({
                  commandId: command.commandId,
                  commandFingerprint: fingerprint,
                  authority: "controller",
                  aggregateKind: "task",
                  aggregateId: command.taskId,
                  status: "rejected",
                  resultSequence: 0,
                  resultStreamVersion: 0,
                  eventCreated: false,
                  acceptedAt: occurredAt,
                  errorCode: "source-snapshot-stale",
                });
                return { _tag: "Rejected" as const, error: rejected };
              }
            }

            if (command.type === "agentControl.task.createFromGithubIssue") {
              const expectedTaskId = yield* deriveAgentControlTaskId(command.source);
              if (expectedTaskId !== command.taskId) {
                const rejected = rpcError("source-identity-conflict", command);
                yield* receipts.insert({
                  commandId: command.commandId,
                  commandFingerprint: fingerprint,
                  authority: "controller",
                  aggregateKind: "task",
                  aggregateId: command.taskId,
                  status: "rejected",
                  resultSequence: 0,
                  resultStreamVersion: 0,
                  eventCreated: false,
                  acceptedAt: occurredAt,
                  errorCode: rejected.code as AgentControlRejectedCommandErrorCode,
                });
                return { _tag: "Rejected" as const, error: rejected };
              }
            }

            const stateOption = yield* states.get(command.taskId);
            const state = Option.getOrNull(stateOption);
            if (
              command.type === "agentControl.task.createFromGithubIssue" &&
              state !== null &&
              (state.source.projectId !== command.source.projectId ||
                state.source.repositoryNodeId !== command.source.repositoryNodeId ||
                state.source.issueNodeId !== command.source.issueNodeId ||
                state.source.issueNumber !== command.source.issueNumber ||
                state.source.issueUrl !== command.source.issueUrl)
            ) {
              const rejected = rpcError("source-identity-conflict", command);
              yield* receipts.insert({
                commandId: command.commandId,
                commandFingerprint: fingerprint,
                authority: "controller",
                aggregateKind: "task",
                aggregateId: command.taskId,
                status: "rejected",
                resultSequence: state.sequence,
                resultStreamVersion: state.revision,
                eventCreated: false,
                acceptedAt: occurredAt,
                errorCode: "source-identity-conflict",
              });
              return { _tag: "Rejected" as const, error: rejected };
            }
            if (command.type === "agentControl.task.createFromGithubIssue" && state === null) {
              const identityTask = yield* states.findByIdentity(
                command.projectId,
                command.source.repositoryNodeId,
                command.source.issueNodeId,
              );
              if (Option.isSome(identityTask) && identityTask.value.taskId !== command.taskId) {
                const rejected = rpcError("source-identity-conflict", command);
                yield* receipts.insert({
                  commandId: command.commandId,
                  commandFingerprint: fingerprint,
                  authority: "controller",
                  aggregateKind: "task",
                  aggregateId: command.taskId,
                  status: "rejected",
                  resultSequence: identityTask.value.sequence,
                  resultStreamVersion: identityTask.value.revision,
                  eventCreated: false,
                  acceptedAt: occurredAt,
                  errorCode: rejected.code as AgentControlRejectedCommandErrorCode,
                });
                return { _tag: "Rejected" as const, error: rejected };
              }
              const numberTask = yield* states.findBySourceNumber(
                command.projectId,
                command.source.repositoryNodeId,
                command.source.issueNumber,
              );
              if (Option.isSome(numberTask) && numberTask.value.taskId !== command.taskId) {
                const rejected = rpcError("source-identity-conflict", command);
                yield* receipts.insert({
                  commandId: command.commandId,
                  commandFingerprint: fingerprint,
                  authority: "controller",
                  aggregateKind: "task",
                  aggregateId: command.taskId,
                  status: "rejected",
                  resultSequence: numberTask.value.sequence,
                  resultStreamVersion: numberTask.value.revision,
                  eventCreated: false,
                  acceptedAt: occurredAt,
                  errorCode: "source-identity-conflict",
                });
                return { _tag: "Rejected" as const, error: rejected };
              }
            }

            const decision = yield* Effect.result(
              decideAgentControlTaskCommand({ state, command, eventId, occurredAt }),
            );
            if (decision._tag === "Failure") {
              yield* receipts.insert({
                commandId: command.commandId,
                commandFingerprint: fingerprint,
                authority: "controller",
                aggregateKind: "task",
                aggregateId: command.taskId,
                status: "rejected",
                resultSequence: state?.sequence ?? 0,
                resultStreamVersion: state?.revision ?? 0,
                eventCreated: false,
                acceptedAt: occurredAt,
                errorCode: decision.failure.code as AgentControlRejectedCommandErrorCode,
              });
              return { _tag: "Rejected" as const, error: decision.failure };
            }

            const appended =
              decision.success.length === 0
                ? []
                : yield* events.append({
                    taskId: command.taskId,
                    expectedStreamVersion: state?.revision ?? 0,
                    events: decision.success,
                  });
            let next = state;
            for (const event of appended) {
              yield* projection.projectEvent(event);
              next = yield* projectAgentControlTaskEvent(next, event);
            }
            if (next === null) return yield* rpcError("task-missing", command);
            const eventCreated = appended.length > 0;
            yield* receipts.insert({
              commandId: command.commandId,
              commandFingerprint: fingerprint,
              authority: "controller",
              aggregateKind: "task",
              aggregateId: command.taskId,
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
              } satisfies AgentControlTaskCommandResult,
            };
          }),
        )
        .pipe(
          Effect.mapError((cause) => {
            if (isTaskRpcError(cause)) return cause;
            if (
              typeof cause === "object" &&
              cause !== null &&
              "_tag" in cause &&
              cause._tag === "AgentControlTaskStreamVersionConflictError"
            ) {
              return rpcError("revision-conflict", command);
            }
            return rpcError("internal-persistence-error", command);
          }),
        );

      if (committed._tag === "Rejected") return yield* committed.error;
      for (const event of committed.events) yield* PubSub.publish(eventPubSub, event);
      return committed.result;
    });

  const dispatchController: AgentControlTaskEngineShape["dispatchController"] = (command) =>
    dispatch(command, false);
  const dispatchObservedController: AgentControlTaskEngineShape["dispatchObservedController"] = (
    command,
  ) => dispatch(command, true);

  const verifySourceSnapshot: AgentControlTaskEngineShape["verifySourceSnapshot"] = (
    precondition,
  ) =>
    sql
      .withTransaction(
        Effect.gen(function* () {
          const available = yield* Effect.result(
            availability.ensureAvailable(precondition.projectId),
          );
          if (available._tag === "Failure") {
            if (available.failure._tag === "AgentControlProjectUnavailableError") {
              return yield* new AgentControlTaskRpcError({
                code:
                  available.failure.reason === "missing" ? "project-missing" : "project-deleted",
                operation: "reconcile-once",
                projectId: precondition.projectId,
                taskId: null,
              });
            }
            return yield* new AgentControlTaskRpcError({
              code: "internal-persistence-error",
              operation: "reconcile-once",
              projectId: precondition.projectId,
              taskId: null,
            });
          }
          if (!(yield* github.matchesCompletedSnapshot(precondition))) {
            return yield* new AgentControlTaskRpcError({
              code: "source-snapshot-stale",
              operation: "reconcile-once",
              projectId: precondition.projectId,
              taskId: null,
            });
          }
        }),
      )
      .pipe(
        Effect.mapError((cause) =>
          isTaskRpcError(cause)
            ? cause
            : new AgentControlTaskRpcError({
                code: "internal-persistence-error",
                operation: "reconcile-once",
                projectId: precondition.projectId,
                taskId: null,
              }),
        ),
      );

  const get: AgentControlTaskEngineShape["get"] = (taskId) =>
    states.get(taskId).pipe(
      Effect.mapError(
        () =>
          new AgentControlTaskRpcError({
            code: "internal-persistence-error",
            operation: "get-task",
            projectId: internalProjectId,
            taskId,
          }),
      ),
    );

  const rebuild = projection.rebuild.pipe(
    Effect.mapError(
      () =>
        new AgentControlTaskRpcError({
          code: "internal-persistence-error",
          operation: "dispatch",
          projectId: internalProjectId,
          taskId: null,
        }),
    ),
  );
  const publishCommitted: AgentControlTaskEngineShape["publishCommitted"] = (committed) =>
    Effect.forEach(committed, (event) => PubSub.publish(eventPubSub, event), {
      discard: true,
    });
  const streamDomainEvents = Stream.fromPubSub(eventPubSub);
  const subscribeDomainEvents = PubSub.subscribe(eventPubSub).pipe(
    Effect.map(Stream.fromSubscription),
  );

  return AgentControlTaskEngine.of({
    get,
    dispatchController,
    dispatchObservedController,
    verifySourceSnapshot,
    rebuild,
    publishCommitted,
    streamDomainEvents,
    subscribeDomainEvents,
  });
});

export const layer = Layer.effect(AgentControlTaskEngine, makeEngine);
