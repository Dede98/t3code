import {
  AgentControlCommandIdentityMismatchError,
  AgentControlCommandPreviouslyRejectedError,
  type AgentControlEvent,
  AgentControlEventDecodeFailedError,
  AgentControlGetProjectStateInput,
  AgentControlInternalPersistenceError,
  AgentControlProjectDeletedError,
  AgentControlProjectMissingError,
  AgentControlProjectRevisionConflictError,
  AgentControlProjectionCorruptError,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlRuntimeRpcError,
  type AgentControlSetProjectModeCommand as AgentControlSetProjectModeCommandType,
  AgentControlSetProjectModeInput,
  type AgentControlSetProjectModeResult,
  AgentControlRuntimeValidationError,
  EventId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import type { AgentControlCommandAuthority } from "../AgentControlCommandAuthority.ts";
import {
  AgentControlPersistenceDecodeError,
  AgentControlProjectUnavailableError,
  AgentControlStreamVersionConflictError,
} from "../Errors.ts";
import {
  createDefaultAgentControlProjectState,
  decideAgentControlProjectCommand,
} from "../decider.ts";
import { projectAgentControlEvent } from "../projector.ts";
import {
  AgentControlEngine,
  type AgentControlEngineShape,
} from "../Services/AgentControlEngine.ts";
import { AgentControlProjection } from "../Services/AgentControlProjection.ts";
import { AgentControlCommandReceiptRepository } from "../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlEventStore } from "../../persistence/Services/AgentControlEventStore.ts";
import { AgentControlProjectAvailability } from "../../persistence/Services/AgentControlProjectAvailability.ts";
import { AgentControlProjectStateRepository } from "../../persistence/Services/AgentControlProjectStates.ts";

interface CommandEnvelope {
  readonly command: AgentControlSetProjectModeCommandType;
  readonly authority: AgentControlCommandAuthority;
  readonly result: Deferred.Deferred<AgentControlSetProjectModeResult, AgentControlRuntimeRpcError>;
}

type InfrastructureContext = "event-append" | "event-replay" | "projection" | "persistence";
const isProjectionCorruptError = Schema.is(AgentControlProjectionCorruptError);
const isStreamVersionConflictError = Schema.is(AgentControlStreamVersionConflictError);
const isPersistenceDecodeError = Schema.is(AgentControlPersistenceDecodeError);
const isProjectUnavailableError = Schema.is(AgentControlProjectUnavailableError);
const decodeGetProjectStateInput = Schema.decodeUnknownEffect(AgentControlGetProjectStateInput);
const decodeSetProjectModeInput = Schema.decodeUnknownEffect(AgentControlSetProjectModeInput);

function mapInfrastructureError(
  error: unknown,
  context: InfrastructureContext,
): AgentControlRuntimeRpcError {
  if (isProjectionCorruptError(error)) return error;
  if (isStreamVersionConflictError(error)) {
    return new AgentControlProjectRevisionConflictError({
      code: "revision-conflict",
      projectId: error.projectId,
      expectedRevision: error.expectedVersion,
      actualRevision: error.actualVersion,
    });
  }
  if (isPersistenceDecodeError(error)) {
    if (context === "event-append" || context === "event-replay") {
      return new AgentControlEventDecodeFailedError({
        code: "event-decode-failed",
        operation:
          context === "event-append"
            ? "append"
            : error.operation.includes("readStream")
              ? "stream-replay"
              : "global-replay",
      });
    }
    if (context === "projection") {
      return new AgentControlProjectionCorruptError({
        code: "projection-corrupt",
        projector: "agent-control-project-modes-v1",
      });
    }
  }
  return new AgentControlInternalPersistenceError({
    code: "internal-persistence-error",
  });
}

function unavailableError(
  error: AgentControlProjectUnavailableError,
): AgentControlProjectMissingError | AgentControlProjectDeletedError {
  return error.reason === "missing"
    ? new AgentControlProjectMissingError({
        code: "project-missing",
        projectId: error.projectId,
      })
    : new AgentControlProjectDeletedError({
        code: "project-deleted",
        projectId: error.projectId,
      });
}

const makeAgentControlEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const availability = yield* AgentControlProjectAvailability;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const eventStore = yield* AgentControlEventStore;
  const projectStates = yield* AgentControlProjectStateRepository;
  const projection = yield* AgentControlProjection;

  // Layer construction blocks here, so RPC handlers cannot observe a stale
  // projection after restart.
  yield* projection.bootstrap;

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<AgentControlEvent>();

  const commandFingerprint = Effect.fn("AgentControlEngine.commandFingerprint")(function* (
    command: AgentControlSetProjectModeCommandType,
  ) {
    const canonical = [
      command.type,
      command.commandId,
      command.projectId,
      String(command.expectedRevision),
      command.mode,
    ]
      .map((part) => `${part.length}:${part}`)
      .join("");
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const loadStateAtRevision = Effect.fn("AgentControlEngine.loadStateAtRevision")(function* (
    projectId: AgentControlSetProjectModeCommandType["projectId"],
    revision: number,
  ) {
    let state = createDefaultAgentControlProjectState(projectId);
    while (state.revision < revision) {
      const events = yield* eventStore
        .readStream(projectId, state.revision, Math.min(500, revision - state.revision))
        .pipe(Effect.mapError((error) => mapInfrastructureError(error, "event-replay")));
      if (events.length === 0) {
        return yield* new AgentControlProjectionCorruptError({
          code: "projection-corrupt",
          projector: "agent-control-project-modes-v1",
        });
      }
      for (const event of events) state = yield* projectAgentControlEvent(state, event);
    }
    if (state.revision !== revision) {
      return yield* new AgentControlProjectionCorruptError({
        code: "projection-corrupt",
        projector: "agent-control-project-modes-v1",
      });
    }
    return state;
  });

  const processEnvelope = Effect.fn("AgentControlEngine.processEnvelope")(function* (
    envelope: CommandEnvelope,
  ) {
    const fingerprint = yield* commandFingerprint(envelope.command).pipe(
      Effect.mapError(
        () =>
          new AgentControlInternalPersistenceError({
            code: "internal-persistence-error",
          }),
      ),
    );
    const availabilityResult = yield* Effect.result(
      availability.ensureAvailable(envelope.command.projectId),
    );
    let unavailableProject: AgentControlProjectUnavailableError | null = null;
    if (availabilityResult._tag === "Failure") {
      switch (availabilityResult.failure._tag) {
        case "AgentControlPersistenceSqlError":
          return yield* mapInfrastructureError(availabilityResult.failure, "persistence");
        case "AgentControlProjectUnavailableError":
          unavailableProject = availabilityResult.failure;
          break;
      }
    }
    const occurredAt = DateTime.formatIso(yield* DateTime.now);
    const eventId = EventId.make(
      yield* crypto.randomUUIDv4.pipe(
        Effect.mapError(
          () =>
            new AgentControlInternalPersistenceError({
              code: "internal-persistence-error",
            }),
        ),
      ),
    );

    const committed = yield* sql.withTransaction(
      Effect.gen(function* () {
        const existingReceipt = yield* receipts
          .getByCommandId(envelope.command.commandId)
          .pipe(Effect.mapError((error) => mapInfrastructureError(error, "persistence")));
        if (Option.isSome(existingReceipt)) {
          const receipt = existingReceipt.value;
          if (
            receipt.commandFingerprint !== fingerprint ||
            receipt.authority !== envelope.authority ||
            receipt.aggregateKind !== "project-controller"
          ) {
            return yield* new AgentControlCommandIdentityMismatchError({
              code: "command-identity-mismatch",
              commandId: envelope.command.commandId,
            });
          }
          if (unavailableProject !== null) {
            if (receipt.status === "rejected") {
              return {
                _tag: "Rejected" as const,
                error: new AgentControlCommandPreviouslyRejectedError({
                  code: "command-previously-rejected",
                  commandId: receipt.commandId,
                  originalErrorCode: receipt.errorCode,
                }),
              };
            }
            return {
              _tag: "Rejected" as const,
              error: unavailableError(unavailableProject),
            };
          }
          if (receipt.status === "rejected") {
            return {
              _tag: "Rejected" as const,
              error: new AgentControlCommandPreviouslyRejectedError({
                code: "command-previously-rejected",
                commandId: receipt.commandId,
                originalErrorCode: receipt.errorCode,
              }),
            };
          }
          const state = yield* loadStateAtRevision(
            envelope.command.projectId,
            receipt.resultStreamVersion,
          );
          if (state.sequence !== receipt.resultSequence) {
            return yield* new AgentControlProjectionCorruptError({
              code: "projection-corrupt",
              projector: "agent-control-project-modes-v1",
            });
          }
          return {
            _tag: "Accepted" as const,
            events: [],
            result: {
              state,
              resultSequence: receipt.resultSequence,
              eventCreated: receipt.eventCreated,
            } satisfies AgentControlSetProjectModeResult,
          };
        }

        if (unavailableProject !== null) {
          const error = unavailableError(unavailableProject);
          yield* receipts
            .insert({
              commandId: envelope.command.commandId,
              commandFingerprint: fingerprint,
              authority: envelope.authority,
              aggregateKind: "project-controller",
              aggregateId: envelope.command.projectId,
              status: "rejected",
              resultSequence: 0,
              resultStreamVersion: 0,
              eventCreated: false,
              acceptedAt: occurredAt,
              errorCode: error.code,
            })
            .pipe(Effect.mapError((cause) => mapInfrastructureError(cause, "persistence")));
          return { _tag: "Rejected" as const, error };
        }

        const persistedState = yield* projectStates
          .get(envelope.command.projectId)
          .pipe(Effect.mapError((error) => mapInfrastructureError(error, "projection")));
        const currentState = Option.getOrElse(persistedState, () =>
          createDefaultAgentControlProjectState(envelope.command.projectId),
        );

        if (currentState.revision !== envelope.command.expectedRevision) {
          const error = new AgentControlProjectRevisionConflictError({
            code: "revision-conflict",
            projectId: envelope.command.projectId,
            expectedRevision: envelope.command.expectedRevision,
            actualRevision: currentState.revision,
          });
          yield* receipts
            .insert({
              commandId: envelope.command.commandId,
              commandFingerprint: fingerprint,
              authority: envelope.authority,
              aggregateKind: "project-controller",
              aggregateId: envelope.command.projectId,
              status: "rejected",
              resultSequence: currentState.sequence,
              resultStreamVersion: currentState.revision,
              eventCreated: false,
              acceptedAt: occurredAt,
              errorCode: error.code,
            })
            .pipe(Effect.mapError((cause) => mapInfrastructureError(cause, "persistence")));
          return { _tag: "Rejected" as const, error };
        }

        const decision = yield* Effect.result(
          decideAgentControlProjectCommand({
            state: currentState,
            command: envelope.command,
            eventId,
            occurredAt,
            authority: envelope.authority,
          }),
        );
        if (decision._tag === "Failure") {
          yield* receipts
            .insert({
              commandId: envelope.command.commandId,
              commandFingerprint: fingerprint,
              authority: envelope.authority,
              aggregateKind: "project-controller",
              aggregateId: envelope.command.projectId,
              status: "rejected",
              resultSequence: currentState.sequence,
              resultStreamVersion: currentState.revision,
              eventCreated: false,
              acceptedAt: occurredAt,
              errorCode: decision.failure.code as AgentControlRejectedCommandErrorCode,
            })
            .pipe(Effect.mapError((cause) => mapInfrastructureError(cause, "persistence")));
          return { _tag: "Rejected" as const, error: decision.failure };
        }

        const events =
          decision.success.length === 0
            ? []
            : yield* eventStore
                .append({
                  projectId: envelope.command.projectId,
                  expectedStreamVersion: currentState.revision,
                  events: decision.success,
                })
                .pipe(Effect.mapError((error) => mapInfrastructureError(error, "event-append")));

        let nextState = currentState;
        for (const event of events) {
          yield* projection
            .projectEvent(event)
            .pipe(Effect.mapError((error) => mapInfrastructureError(error, "projection")));
          nextState = yield* projectAgentControlEvent(nextState, event);
        }
        const eventCreated = events.length > 0;
        yield* receipts
          .insert({
            commandId: envelope.command.commandId,
            commandFingerprint: fingerprint,
            authority: envelope.authority,
            aggregateKind: "project-controller",
            aggregateId: envelope.command.projectId,
            status: "accepted",
            resultSequence: nextState.sequence,
            resultStreamVersion: nextState.revision,
            eventCreated,
            acceptedAt: occurredAt,
            errorCode: null,
          })
          .pipe(Effect.mapError((cause) => mapInfrastructureError(cause, "persistence")));

        return {
          _tag: "Accepted" as const,
          events,
          result: {
            state: nextState,
            resultSequence: nextState.sequence,
            eventCreated,
          } satisfies AgentControlSetProjectModeResult,
        };
      }),
    );

    if (committed._tag === "Rejected") return yield* committed.error;
    for (const event of committed.events) yield* PubSub.publish(eventPubSub, event);
    return committed.result;
  });

  const worker = Effect.forever(
    Queue.take(commandQueue).pipe(
      Effect.flatMap((envelope) =>
        Effect.exit(processEnvelope(envelope)).pipe(
          Effect.flatMap((exit) =>
            Exit.isSuccess(exit)
              ? Deferred.succeed(envelope.result, exit.value)
              : Deferred.fail(
                  envelope.result,
                  Cause.squash(exit.cause) as AgentControlRuntimeRpcError,
                ),
          ),
        ),
      ),
    ),
  );
  yield* Effect.forkScoped(worker);

  const getProjectState: AgentControlEngineShape["getProjectState"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGetProjectStateInput(rawInput).pipe(
        Effect.mapError(
          () =>
            new AgentControlRuntimeValidationError({
              code: "validation",
              operation: "get-project-state",
            }),
        ),
      );
      yield* availability
        .ensureAvailable(input.projectId)
        .pipe(
          Effect.mapError((error) =>
            isProjectUnavailableError(error)
              ? unavailableError(error)
              : mapInfrastructureError(error, "persistence"),
          ),
        );
      const state = yield* projectStates
        .get(input.projectId)
        .pipe(Effect.mapError((error) => mapInfrastructureError(error, "projection")));
      return Option.getOrElse(state, () => createDefaultAgentControlProjectState(input.projectId));
    });

  const dispatchWithAuthority = (
    authority: AgentControlCommandAuthority,
    rawInput: AgentControlSetProjectModeInput,
  ) =>
    Effect.gen(function* () {
      const input = yield* decodeSetProjectModeInput(rawInput).pipe(
        Effect.mapError(
          () =>
            new AgentControlRuntimeValidationError({
              code: "validation",
              operation: "set-project-mode",
            }),
        ),
      );
      const result = yield* Deferred.make<
        AgentControlSetProjectModeResult,
        AgentControlRuntimeRpcError
      >();
      yield* Queue.offer(commandQueue, {
        command: { type: "agentControl.project.mode.set", ...input },
        authority,
        result,
      });
      return yield* Deferred.await(result);
    });

  return AgentControlEngine.of({
    getProjectState,
    dispatchHuman: (input) => dispatchWithAuthority("human", input),
    dispatchController: (input) => dispatchWithAuthority("controller", input),
    dispatchSystem: (input) => dispatchWithAuthority("system", input),
    get streamDomainEvents() {
      return Stream.fromPubSub(eventPubSub);
    },
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
  });
});

export const AgentControlEngineLive = Layer.effect(AgentControlEngine, makeAgentControlEngine);
