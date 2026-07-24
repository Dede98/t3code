import {
  AgentControlWorktreeGetInput,
  AgentControlWorktreeListInput,
  AgentControlWorktreeReservationId,
  AgentControlWorktreeRpcError,
  type AgentControlWorktreeReservationState,
  type AgentControlWorktreeReservationView,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlWorktree,
  type AgentControlWorktreeShape,
} from "../Services/AgentControlWorktree.ts";
import { AgentControlWorktreeStateRepository } from "../Services/AgentControlWorktreeStateRepository.ts";
import { AgentControlWorktreeEventStore } from "../Services/AgentControlWorktreeEventStore.ts";
import { loadAuthoritativeWorktreeReservation } from "../authoritative.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlWorktreeGetInput);
const decodeList = Schema.decodeUnknownEffect(AgentControlWorktreeListInput);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);
const safeError = (
  code: AgentControlWorktreeRpcError["code"],
  operation: AgentControlWorktreeRpcError["operation"],
  projectId: AgentControlWorktreeGetInput["projectId"],
  reservationId: AgentControlWorktreeGetInput["reservationId"] | null = null,
) =>
  new AgentControlWorktreeRpcError({
    code,
    operation,
    projectId,
    taskId: null,
    reservationId,
  });

export const toAgentControlWorktreeReservationView = (
  state: AgentControlWorktreeReservationState,
): AgentControlWorktreeReservationView => ({
  reservationId: state.reservationId,
  projectId: state.projectId,
  taskId: state.taskId,
  taskRevision: state.taskRevision,
  githubIntakeSequence: state.githubIntakeSequence,
  stageRunId: state.stageRunId,
  attemptId: state.attemptId,
  leaseId: state.leaseId,
  fenceToken: state.fenceToken,
  repositoryNodeId: state.repository.repositoryNodeId,
  branchName: state.branchName,
  baseRef: state.baseRef,
  baseCommitSha: state.baseCommitSha,
  headCommitSha: state.headCommitSha,
  status: state.status,
  attentionCode: state.attentionCode,
  verifiedAt: state.verifiedAt,
  revision: state.revision,
  createdAt: state.createdAt,
  updatedAt: state.updatedAt,
});

const make = Effect.gen(function* () {
  const availability = yield* AgentControlProjectAvailability;
  const sql = yield* SqlClient.SqlClient;
  const states = yield* AgentControlWorktreeStateRepository;
  const events = yield* AgentControlWorktreeEventStore;

  const ensureProject = (
    projectId: AgentControlWorktreeGetInput["projectId"],
    operation: AgentControlWorktreeRpcError["operation"],
  ) =>
    availability
      .ensureAvailable(projectId)
      .pipe(
        Effect.mapError((error) =>
          safeError(
            error._tag === "AgentControlProjectUnavailableError"
              ? "project-unavailable"
              : "internal-persistence-error",
            operation,
            projectId,
          ),
        ),
      );

  const getReservation: AgentControlWorktreeShape["getReservation"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError("validation", "get-reservation", rawInput.projectId, rawInput.reservationId),
        ),
      );
      yield* ensureProject(input.projectId, "get-reservation");
      const state = yield* loadAuthoritativeWorktreeReservation(
        input.reservationId,
        events,
        states,
      ).pipe(
        Effect.mapError((error) =>
          safeError(
            error._tag === "AgentControlPersistenceSqlError"
              ? "internal-persistence-error"
              : "reservation-projection-corrupt",
            "get-reservation",
            input.projectId,
            input.reservationId,
          ),
        ),
      );
      if (Option.isNone(state) || state.value.state.projectId !== input.projectId) {
        return yield* safeError(
          "reservation-missing",
          "get-reservation",
          input.projectId,
          input.reservationId,
        );
      }
      return toAgentControlWorktreeReservationView(state.value.state);
    });

  const listReservations: AgentControlWorktreeShape["listReservations"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list-reservations", rawInput.projectId)),
      );
      yield* ensureProject(input.projectId, "list-reservations");
      const candidates = yield* sql<{ readonly reservationId: unknown }>`
        SELECT reservation_id AS "reservationId"
        FROM agent_control_worktree_reservation_states
        WHERE project_id = ${input.projectId}
        UNION
        SELECT stream_id AS "reservationId"
        FROM agent_control_events
        WHERE aggregate_kind = 'worktree-reservation'
          AND stream_version = 1
          AND json_valid(payload_json)
          AND json_extract(payload_json, '$.projectId') = ${input.projectId}
        ORDER BY "reservationId" ASC
      `.pipe(
        Effect.mapError(() =>
          safeError("internal-persistence-error", "list-reservations", input.projectId),
        ),
      );
      const healthy: Array<AgentControlWorktreeReservationState> = [];
      let quarantinedCount = 0;
      for (const candidate of candidates) {
        const decodedId = yield* Effect.result(decodeReservationId(candidate.reservationId));
        if (decodedId._tag === "Failure") {
          quarantinedCount += 1;
          continue;
        }
        const authoritative = yield* Effect.result(
          loadAuthoritativeWorktreeReservation(decodedId.success, events, states),
        );
        if (authoritative._tag === "Failure") {
          if (authoritative.failure._tag === "AgentControlPersistenceSqlError") {
            return yield* safeError(
              "internal-persistence-error",
              "list-reservations",
              input.projectId,
            );
          }
          quarantinedCount += 1;
          continue;
        }
        if (Option.isNone(authoritative.success)) {
          quarantinedCount += 1;
          continue;
        }
        if (authoritative.success.value.state.projectId !== input.projectId) {
          quarantinedCount += 1;
          continue;
        }
        healthy.push(authoritative.success.value.state);
      }
      const competing = new Set<string>();
      const byStage = new Map<string, Array<AgentControlWorktreeReservationState>>();
      for (const state of healthy) {
        const key = [state.projectId, state.taskId, state.stageRunId, state.attemptId].join("\0");
        const bucket = byStage.get(key) ?? [];
        bucket.push(state);
        byStage.set(key, bucket);
      }
      for (const bucket of byStage.values()) {
        if (bucket.length <= 1) continue;
        for (const state of bucket) competing.add(state.reservationId);
      }
      quarantinedCount += competing.size;
      const reservations = healthy
        .filter((state) => !competing.has(state.reservationId))
        .toSorted(
          (left, right) =>
            left.createdAt.localeCompare(right.createdAt) ||
            left.reservationId.localeCompare(right.reservationId),
        )
        .map(toAgentControlWorktreeReservationView);
      return {
        projectId: input.projectId,
        reservations,
        quarantinedCount,
      };
    });

  return AgentControlWorktree.of({ getReservation, listReservations });
});

export const layer = Layer.effect(AgentControlWorktree, make);
