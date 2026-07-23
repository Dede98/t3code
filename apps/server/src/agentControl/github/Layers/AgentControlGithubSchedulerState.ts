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
  AgentControlGithubSchedulerConflictError,
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
  schedulerRevision: PositiveInt,
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
const decodeRevision = Schema.decodeUnknownEffect(NonNegativeInt);
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
        scheduler_revision AS "schedulerRevision",
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
              schedulerRevision,
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
              state.schedulerRevision === schedulerRevision &&
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

  const actualRevision = (projectId: AgentControlGithubSchedulerState["projectId"]) =>
    sql<{ readonly schedulerRevision: unknown }>`
      SELECT scheduler_revision AS "schedulerRevision"
      FROM agent_control_github_scheduler_states
      WHERE project_id = ${projectId}
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubSchedulerStateRepository.actualRevision:query", cause),
      ),
      Effect.flatMap((rows) =>
        decodeRevision(rows[0]?.schedulerRevision ?? 0).pipe(
          Effect.mapError((cause) =>
            decodeError("AgentControlGithubSchedulerStateRepository.actualRevision:decode", cause),
          ),
        ),
      ),
    );

  const conflict = (
    projectId: AgentControlGithubSchedulerState["projectId"],
    expectedRevision: number,
  ) =>
    actualRevision(projectId).pipe(
      Effect.flatMap(
        (actual) =>
          new AgentControlGithubSchedulerConflictError({
            projectId,
            expectedRevision,
            actualRevision: actual,
          }),
      ),
    );

  const save: AgentControlGithubSchedulerStateRepositoryShape["save"] = (
    rawState,
    expectedRevision,
  ) =>
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
      if (
        !Number.isSafeInteger(expectedRevision) ||
        expectedRevision < 0 ||
        state.schedulerRevision !== expectedRevision + 1
      ) {
        return yield* decodeError(
          "AgentControlGithubSchedulerStateRepository.save:revision",
          new Error("invalid scheduler revision transition"),
        );
      }
      const stateJson = yield* encodeState(state).pipe(
        Effect.mapError((cause) =>
          decodeError("AgentControlGithubSchedulerStateRepository.save:encode", cause),
        ),
      );
      const rows =
        expectedRevision === 0
          ? yield* sql<Record<string, unknown>>`
              INSERT INTO agent_control_github_scheduler_states (
                project_id, state_json, scheduler_revision, generation, activity, circuit_state,
                last_github_event_sequence, consecutive_failures, last_attempt_at, next_attempt_at,
                cooldown_until, reason_code, updated_at
              ) VALUES (
                ${state.projectId}, ${stateJson}, ${state.schedulerRevision}, ${state.generation},
                ${state.activity}, ${state.circuitState}, ${state.lastGithubEventSequence},
                ${state.consecutiveFailures}, ${state.lastAttemptAt}, ${state.nextAttemptAt},
                ${state.cooldownUntil}, ${state.reasonCode}, ${state.updatedAt}
              )
              ON CONFLICT (project_id) DO NOTHING
              RETURNING project_id
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlGithubSchedulerStateRepository.save:insert", cause),
              ),
            )
          : yield* sql<Record<string, unknown>>`
              UPDATE agent_control_github_scheduler_states
              SET state_json = ${stateJson},
                  scheduler_revision = ${state.schedulerRevision},
                  generation = ${state.generation},
                  last_github_event_sequence = ${state.lastGithubEventSequence},
                  activity = ${state.activity},
                  circuit_state = ${state.circuitState},
                  consecutive_failures = ${state.consecutiveFailures},
                  last_attempt_at = ${state.lastAttemptAt},
                  next_attempt_at = ${state.nextAttemptAt},
                  cooldown_until = ${state.cooldownUntil},
                  reason_code = ${state.reasonCode},
                  updated_at = ${state.updatedAt}
              WHERE project_id = ${state.projectId}
                AND scheduler_revision = ${expectedRevision}
                AND generation <= ${state.generation}
                AND last_github_event_sequence <= ${state.lastGithubEventSequence}
              RETURNING project_id
            `.pipe(
              Effect.mapError((cause) =>
                sqlError("AgentControlGithubSchedulerStateRepository.save:update", cause),
              ),
            );
      if (rows.length === 0) {
        return yield* conflict(state.projectId, expectedRevision);
      }
      return state;
    });

  const deleteState: AgentControlGithubSchedulerStateRepositoryShape["delete"] = (
    projectId,
    expectedRevision,
  ) =>
    sql<Record<string, unknown>>`
      DELETE FROM agent_control_github_scheduler_states
      WHERE project_id = ${projectId}
        AND scheduler_revision = ${expectedRevision}
      RETURNING project_id
    `.pipe(
      Effect.mapError((cause) =>
        sqlError("AgentControlGithubSchedulerStateRepository.delete:query", cause),
      ),
      Effect.flatMap((rows) =>
        rows.length > 0
          ? Effect.void
          : actualRevision(projectId).pipe(
              Effect.flatMap((actual) =>
                actual === 0
                  ? Effect.void
                  : new AgentControlGithubSchedulerConflictError({
                      projectId,
                      expectedRevision,
                      actualRevision: actual,
                    }),
              ),
            ),
      ),
    );

  return AgentControlGithubSchedulerStateRepository.of({
    get,
    save,
    delete: deleteState,
  });
});

export const layer = Layer.effect(AgentControlGithubSchedulerStateRepository, makeRepository);
