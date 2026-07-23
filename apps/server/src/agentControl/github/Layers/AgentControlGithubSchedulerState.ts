import {
  AgentControlGithubCircuitState,
  AgentControlGithubReactorReasonCode,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  AgentControlPersistenceDecodeError,
  AgentControlPersistenceSqlError,
} from "../../Errors.ts";
import {
  AgentControlGithubSchedulerState,
  AgentControlGithubSchedulerStateRepository,
  type AgentControlGithubSchedulerStateRepositoryShape,
} from "../Services/AgentControlGithubSchedulerState.ts";

const SchedulerRow = Schema.Struct({
  state: Schema.fromJsonString(AgentControlGithubSchedulerState),
  projectId: ProjectId,
  generation: PositiveInt,
  lastGithubEventSequence: NonNegativeInt,
  activity: Schema.Literals(["active", "suspended"]),
  circuitState: AgentControlGithubCircuitState,
  consecutiveFailures: NonNegativeInt,
  lastAttemptAt: Schema.NullOr(IsoDateTime),
  nextAttemptAt: Schema.NullOr(IsoDateTime),
  cooldownUntil: Schema.NullOr(IsoDateTime),
  reasonCode: Schema.NullOr(AgentControlGithubReactorReasonCode),
  updatedAt: IsoDateTime,
});
const decodeRow = Schema.decodeUnknownEffect(SchedulerRow);
const decodeState = Schema.decodeUnknownEffect(AgentControlGithubSchedulerState);
const encodeState = Schema.encodeUnknownEffect(
  Schema.fromJsonString(AgentControlGithubSchedulerState),
);
const sqlError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceSqlError({ operation, cause });
const decodeError = (operation: string, cause: unknown) =>
  new AgentControlPersistenceDecodeError({ operation, cause });

const validDate = (value: string | null) => value === null || Number.isFinite(Date.parse(value));
const validState = (state: AgentControlGithubSchedulerState) => {
  if (
    !validDate(state.lastAttemptAt) ||
    !validDate(state.nextAttemptAt) ||
    !validDate(state.cooldownUntil) ||
    !validDate(state.updatedAt)
  ) {
    return false;
  }
  if (state.activity === "suspended") {
    return (
      state.circuitState === "open" &&
      state.nextAttemptAt === null &&
      state.cooldownUntil === null &&
      state.reasonCode !== null
    );
  }
  if (state.circuitState === "half-open") {
    return state.lastAttemptAt !== null && state.nextAttemptAt === null;
  }
  if (state.circuitState === "open") {
    return state.cooldownUntil !== null && state.nextAttemptAt === state.cooldownUntil;
  }
  return state.cooldownUntil === null;
};

const makeRepository = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const get: AgentControlGithubSchedulerStateRepositoryShape["get"] = (projectId) =>
    sql<Record<string, unknown>>`
      SELECT
        state_json AS state,
        project_id AS "projectId",
        generation,
        last_github_event_sequence AS "lastGithubEventSequence",
        activity,
        circuit_state AS "circuitState",
        consecutive_failures AS "consecutiveFailures",
        last_attempt_at AS "lastAttemptAt",
        next_attempt_at AS "nextAttemptAt",
        cooldown_until AS "cooldownUntil",
        reason_code AS "reasonCode",
        updated_at AS "updatedAt"
      FROM agent_control_github_scheduler_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubSchedulerStateRepository.get:query", cause),
      ),
      Effect.flatMap((rows) => {
        const row = rows[0];
        if (row === undefined) return Effect.succeed(Option.none());
        return decodeRow(row).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlGithubSchedulerStateRepository.get:decode", cause),
          ),
          Effect.flatMap(
            ({
              state,
              projectId: persistedProjectId,
              generation,
              lastGithubEventSequence,
              activity,
              circuitState,
              consecutiveFailures,
              lastAttemptAt,
              nextAttemptAt,
              cooldownUntil,
              reasonCode,
              updatedAt,
            }) =>
              state.projectId === projectId &&
              persistedProjectId === projectId &&
              state.generation === generation &&
              state.lastGithubEventSequence === lastGithubEventSequence &&
              state.activity === activity &&
              state.circuitState === circuitState &&
              state.consecutiveFailures === consecutiveFailures &&
              state.lastAttemptAt === lastAttemptAt &&
              state.nextAttemptAt === nextAttemptAt &&
              state.cooldownUntil === cooldownUntil &&
              state.reasonCode === reasonCode &&
              state.updatedAt === updatedAt &&
              validState(state)
                ? Effect.succeed(Option.some(state))
                : Effect.fail(
                    decodeError(
                      "AgentControlGithubSchedulerStateRepository.get:invariant",
                      new Error("scheduler state identity mismatch"),
                    ),
                  ),
          ),
        );
      }),
    );

  const save: AgentControlGithubSchedulerStateRepositoryShape["save"] = (rawState) =>
    Effect.gen(function* () {
      const state = yield* decodeState(rawState).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubSchedulerStateRepository.save:input", cause),
        ),
      );
      if (!validState(state)) {
        return yield* decodeError(
          "AgentControlGithubSchedulerStateRepository.save:invariant",
          new Error("invalid scheduler state"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubSchedulerStateRepository.save:encode", cause),
        ),
      );
      yield* sql`
        INSERT INTO agent_control_github_scheduler_states (
          project_id, state_json, generation, activity, circuit_state,
          last_github_event_sequence, consecutive_failures, last_attempt_at, next_attempt_at,
          cooldown_until, reason_code, updated_at
        ) VALUES (
          ${state.projectId}, ${stateJson}, ${state.generation}, ${state.activity},
          ${state.circuitState}, ${state.lastGithubEventSequence},
          ${state.consecutiveFailures}, ${state.lastAttemptAt},
          ${state.nextAttemptAt}, ${state.cooldownUntil}, ${state.reasonCode}, ${state.updatedAt}
        )
        ON CONFLICT (project_id) DO UPDATE SET
          state_json = excluded.state_json,
          generation = excluded.generation,
          last_github_event_sequence = excluded.last_github_event_sequence,
          activity = excluded.activity,
          circuit_state = excluded.circuit_state,
          consecutive_failures = excluded.consecutive_failures,
          last_attempt_at = excluded.last_attempt_at,
          next_attempt_at = excluded.next_attempt_at,
          cooldown_until = excluded.cooldown_until,
          reason_code = excluded.reason_code,
          updated_at = excluded.updated_at
      `.pipe(
        Effect.mapError((cause) =>
          sqlError("AgentControlGithubSchedulerStateRepository.save:query", cause),
        ),
      );
    });

  const deleteState: AgentControlGithubSchedulerStateRepositoryShape["delete"] = (projectId) =>
    sql`
      DELETE FROM agent_control_github_scheduler_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubSchedulerStateRepository.delete:query", cause),
      ),
      Effect.asVoid,
    );

  return AgentControlGithubSchedulerStateRepository.of({
    get,
    save,
    delete: deleteState,
  });
});

export const layer = Layer.effect(AgentControlGithubSchedulerStateRepository, makeRepository);
