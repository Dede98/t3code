import {
  AgentControlControlledThreadReservationCommandResult,
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import { deriveAgentControlControlledThreadActivationCommandId } from "../identity.ts";
import {
  AgentControlControlledThreadActivation,
  type AgentControlControlledThreadActivationShape,
} from "../Services/AgentControlControlledThreadActivation.ts";
import { AgentControlControlledThreadActivationHooks } from "../Services/AgentControlControlledThreadActivationHooks.ts";
import {
  AgentControlControlledThreadMaterializationCoordinator,
  type AgentControlControlledThreadMaterializationCoordinatorError,
} from "../Services/AgentControlControlledThreadMaterializationCoordinator.ts";
import { AgentControlControlledThreadReservation } from "../Services/AgentControlControlledThreadReservation.ts";
import {
  AgentControlRunOnceExecutionContext,
  requireRunOnceMethod,
} from "../../runOnce/context.ts";

const decodeInput = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationPrepareInitialInput,
  { onExcessProperty: "error" },
);
const decodeResult = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationCommandResult,
  { onExcessProperty: "error" },
);

const rpcError = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  input: AgentControlControlledThreadReservationPrepareInitialInput,
  controlledThreadReservationId: AgentControlControlledThreadReservationRpcError["controlledThreadReservationId"],
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation: "prepare-initial",
    projectId: input.projectId,
    taskId: input.taskId,
    controlledThreadReservationId,
  });

const coordinatorCode = (
  reason: AgentControlControlledThreadMaterializationCoordinatorError["reason"],
): AgentControlControlledThreadReservationRpcError["code"] => {
  switch (reason) {
    case "validation":
      return "validation";
    case "command-identity-conflict":
      return "command-identity-mismatch";
    case "project-unavailable":
      return "project-unavailable";
    case "project-mode-inactive":
      return "project-mode-inactive";
    case "source-snapshot-stale":
      return "source-snapshot-stale";
    case "lease-expired":
      return "lease-expired";
    case "lease-foreign-runtime":
      return "lease-foreign-runtime";
    case "reservation-missing":
      return "controlled-thread-reservation-missing";
    case "reservation-conflict":
    case "historical-evidence-corrupt":
      return "controlled-thread-reservation-corrupt";
    case "task-unavailable":
    case "stage-run-unavailable":
    case "lease-unavailable":
    case "worktree-unavailable":
    case "reservation-not-prepared":
    case "runtime-policy-unavailable":
      return "state-not-available";
    case "internal-persistence-error":
      return "internal-persistence-error";
  }
};

const make = Effect.gen(function* () {
  const reservation = yield* AgentControlControlledThreadReservation;
  const coordinator = yield* AgentControlControlledThreadMaterializationCoordinator;
  const hooks = yield* AgentControlControlledThreadActivationHooks;

  const activateInitial: AgentControlControlledThreadActivationShape["activateInitial"] = Effect.fn(
    "AgentControlControlledThreadActivation.activateInitial",
  )(function* (rawInput) {
    const runId = yield* AgentControlRunOnceExecutionContext;
    const originalPrepareResult = yield* runId === null
      ? reservation.prepareInitial(rawInput)
      : requireRunOnceMethod(
          reservation.prepareInitialForRunOnce,
          "AgentControlControlledThreadReservation.prepareInitialForRunOnce",
        )(runId, rawInput);
    const input = yield* decodeInput(rawInput).pipe(
      Effect.mapError(() => rpcError("validation", rawInput, null)),
    );
    const prepared = yield* decodeResult(originalPrepareResult).pipe(
      Effect.mapError(() => rpcError("controlled-thread-reservation-corrupt", input, null)),
    );
    const controlledThreadReservationId = prepared.reservation.controlledThreadReservationId;
    if (
      prepared.reservation.projectId !== input.projectId ||
      prepared.reservation.taskId !== input.taskId ||
      prepared.reservation.status !== "prepared" ||
      prepared.reservation.revision !== 1
    ) {
      return yield* rpcError(
        "controlled-thread-reservation-corrupt",
        input,
        controlledThreadReservationId,
      );
    }

    const activationCommandId = yield* deriveAgentControlControlledThreadActivationCommandId(
      input.commandId,
      controlledThreadReservationId,
    );
    const observation = {
      prepareCommandId: input.commandId,
      activationCommandId,
      projectId: input.projectId,
      taskId: input.taskId,
      controlledThreadReservationId,
    } as const;
    yield* hooks.afterPrepareAcceptedBeforeMaterialize(observation);
    const materializeInput = {
      commandId: activationCommandId,
      projectId: input.projectId,
      controlledThreadReservationId,
    } as const;
    const materialized = yield* (
      runId === null
        ? coordinator.materializeInitial(materializeInput)
        : requireRunOnceMethod(
            coordinator.materializeInitialForRunOnce,
            "AgentControlControlledThreadMaterializationCoordinator.materializeInitialForRunOnce",
          )(runId, materializeInput)
    ).pipe(
      Effect.mapError((failure) =>
        rpcError(coordinatorCode(failure.reason), input, controlledThreadReservationId),
      ),
    );
    if (
      materialized.commandId !== activationCommandId ||
      materialized.controlledThreadReservationId !== controlledThreadReservationId ||
      materialized.threadId !== prepared.reservation.threadId ||
      materialized.status !== "bound"
    ) {
      return yield* rpcError(
        "controlled-thread-reservation-corrupt",
        input,
        controlledThreadReservationId,
      );
    }
    yield* hooks.afterMaterializationAcceptedBeforeReturn(observation);
    return originalPrepareResult;
  });

  const activateInitialForRunOnce: NonNullable<
    AgentControlControlledThreadActivationShape["activateInitialForRunOnce"]
  > = (runId, input) =>
    activateInitial(input).pipe(Effect.provideService(AgentControlRunOnceExecutionContext, runId));

  return AgentControlControlledThreadActivation.of({
    activateInitial,
    activateInitialForRunOnce,
  });
});

export const AgentControlControlledThreadActivationLive = Layer.effect(
  AgentControlControlledThreadActivation,
  make,
);
