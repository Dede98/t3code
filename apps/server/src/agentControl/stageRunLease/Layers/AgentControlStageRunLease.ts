import {
  AgentControlStageRunLeaseGetInput,
  AgentControlStageRunLeaseListInput,
  AgentControlStageRunLeaseRpcError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { deriveAgentControlStageRunLeaseId } from "../identity.ts";
import {
  AgentControlStageRunLease,
  type AgentControlStageRunLeaseShape,
} from "../Services/AgentControlStageRunLease.ts";
import { AgentControlStageRunLeaseEngine } from "../Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlStageRunLeaseStateRepository } from "../Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlStageRunLeaseGetInput);
const decodeList = Schema.decodeUnknownEffect(AgentControlStageRunLeaseListInput);

const safeError = (
  code: AgentControlStageRunLeaseRpcError["code"],
  operation: AgentControlStageRunLeaseRpcError["operation"],
  projectId: AgentControlStageRunLeaseGetInput["projectId"],
  taskId: AgentControlStageRunLeaseGetInput["taskId"] | null = null,
) => new AgentControlStageRunLeaseRpcError({ code, operation, projectId, taskId });

const make = Effect.gen(function* () {
  const availability = yield* AgentControlProjectAvailability;
  const engine = yield* AgentControlStageRunLeaseEngine;
  const states = yield* AgentControlStageRunLeaseStateRepository;

  const ensureProject = Effect.fn("AgentControlStageRunLease.ensureProject")(function* (
    projectId: AgentControlStageRunLeaseGetInput["projectId"],
    operation: AgentControlStageRunLeaseRpcError["operation"],
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

  const getLease: AgentControlStageRunLeaseShape["getLease"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError("validation", "get-lease", rawInput.projectId, rawInput.taskId),
        ),
      );
      yield* ensureProject(input.projectId, "get-lease");
      const leaseId = yield* deriveAgentControlStageRunLeaseId(input);
      const state = yield* states
        .get(leaseId)
        .pipe(
          Effect.mapError((error) =>
            safeError(
              error._tag === "AgentControlPersistenceSqlError"
                ? "internal-persistence-error"
                : "lease-projection-corrupt",
              "get-lease",
              input.projectId,
              input.taskId,
            ),
          ),
        );
      if (Option.isNone(state)) {
        return yield* safeError("lease-missing", "get-lease", input.projectId, input.taskId);
      }
      return yield* engine.toView(state.value);
    });

  const listLeases: AgentControlStageRunLeaseShape["listLeases"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list-leases", rawInput.projectId)),
      );
      yield* ensureProject(input.projectId, "list-leases");
      const entries = yield* states
        .listProject(input.projectId)
        .pipe(
          Effect.mapError(() =>
            safeError("internal-persistence-error", "list-leases", input.projectId),
          ),
        );
      const leases = yield* Effect.forEach(
        entries,
        (entry) =>
          entry._tag === "Valid"
            ? engine.toView(entry.state).pipe(Effect.map((view) => [view]))
            : Effect.succeed([]),
        { concurrency: 1 },
      );
      return {
        projectId: input.projectId,
        leases: leases.flat(),
        quarantinedCount: entries.filter((entry) => entry._tag === "Corrupt").length,
      };
    });

  return AgentControlStageRunLease.of({ getLease, listLeases });
});

export const layer = Layer.effect(AgentControlStageRunLease, make);
