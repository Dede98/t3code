import {
  AgentControlAttemptId,
  AgentControlStageRunId,
  AgentControlStageRunLeaseId,
  AgentControlTaskId,
  AgentControlWorktreeGetInput,
  AgentControlWorktreeListInput,
  AgentControlWorktreeReservationId,
  AgentControlWorktreeReservationState as AgentControlWorktreeReservationStateSchema,
  AgentControlWorktreeRpcError,
  IsoDateTime,
  NonNegativeInt,
  PositiveInt,
  ProjectId,
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
import {
  foldAuthoritativeWorktreeReservationStream,
  loadAuthoritativeWorktreeReservation,
  sameAgentControlWorktreeReservationState,
} from "../authoritative.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlWorktreeGetInput);
const decodeList = Schema.decodeUnknownEffect(AgentControlWorktreeListInput);
const decodeReservationId = Schema.decodeUnknownEffect(AgentControlWorktreeReservationId);
const WorktreeStreamCatalogRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: AgentControlTaskId,
  stageRunId: AgentControlStageRunId,
  attemptId: AgentControlAttemptId,
  leaseId: AgentControlStageRunLeaseId,
  fenceToken: PositiveInt,
  createdAt: IsoDateTime,
});
type WorktreeStreamCatalogRow = typeof WorktreeStreamCatalogRow.Type;
const decodeCatalogRow = Schema.decodeUnknownEffect(WorktreeStreamCatalogRow);
const CompositeResultRow = Schema.Struct({
  reservationId: AgentControlWorktreeReservationId,
  projectId: ProjectId,
  taskId: Schema.NullOr(AgentControlTaskId),
  resultStatus: Schema.Literals(["ready", "needs-attention"]),
  resultJson: Schema.String,
  statusKeyCount: NonNegativeInt,
  gitDevice: Schema.NullOr(NonNegativeInt),
  gitInode: Schema.NullOr(NonNegativeInt),
  gitDir: Schema.NullOr(Schema.String),
  marker: Schema.NullOr(Schema.String),
});
const decodeCompositeResultRow = Schema.decodeUnknownEffect(CompositeResultRow);
const decodeCompositeState = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlWorktreeReservationStateSchema),
);
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

  const compositeIntegrity = Effect.fn("AgentControlWorktree.compositeIntegrity")(function* (
    projectId: ProjectId,
    reservationId: AgentControlWorktreeReservationId | null,
  ) {
    const query =
      reservationId === null
        ? sql<Record<string, unknown>>`
            SELECT worktree_reservation_id AS "reservationId", project_id AS "projectId",
              task_id AS "taskId", result_status AS "resultStatus",
              result_json AS "resultJson",
              CASE
                WHEN COALESCE(json_valid(result_json), 0) = 1
                THEN (
                  SELECT COUNT(*) FROM json_each(result_json)
                  WHERE key = 'status'
                )
                ELSE 0
              END AS "statusKeyCount",
              git_created_device AS "gitDevice", git_created_inode AS "gitInode",
              git_created_git_dir AS "gitDir",
              marked_ownership_fingerprint AS marker
            FROM agent_control_worktree_controller_operations
            WHERE project_id = ${projectId} AND status = 'accepted'
          `
        : sql<Record<string, unknown>>`
            SELECT worktree_reservation_id AS "reservationId", project_id AS "projectId",
              task_id AS "taskId", result_status AS "resultStatus",
              result_json AS "resultJson",
              CASE
                WHEN COALESCE(json_valid(result_json), 0) = 1
                THEN (
                  SELECT COUNT(*) FROM json_each(result_json)
                  WHERE key = 'status'
                )
                ELSE 0
              END AS "statusKeyCount",
              git_created_device AS "gitDevice", git_created_inode AS "gitInode",
              git_created_git_dir AS "gitDir",
              marked_ownership_fingerprint AS marker
            FROM agent_control_worktree_controller_operations
            WHERE project_id = ${projectId} AND status = 'accepted'
              AND worktree_reservation_id = ${reservationId}
          `;
    const rows = yield* query;
    const corruptIds = new Set<AgentControlWorktreeReservationId>();
    let opaqueCount = 0;
    for (const raw of rows) {
      const decoded = yield* Effect.result(decodeCompositeResultRow(raw));
      if (decoded._tag === "Failure") {
        const id = yield* Effect.result(decodeReservationId(raw.reservationId));
        if (id._tag === "Success") corruptIds.add(id.success);
        else opaqueCount += 1;
        continue;
      }
      const row = decoded.success;
      const state = yield* Effect.result(decodeCompositeState(row.resultJson));
      const gitEvidence = row.gitDevice !== null && row.gitInode !== null && row.gitDir !== null;
      const noGitEvidence = row.gitDevice === null && row.gitInode === null && row.gitDir === null;
      const evidenceValid =
        row.resultStatus === "ready"
          ? gitEvidence && row.marker !== null
          : (gitEvidence || noGitEvidence) && (row.marker === null || gitEvidence);
      if (
        row.statusKeyCount !== 1 ||
        state._tag === "Failure" ||
        state.success.status !== row.resultStatus ||
        state.success.reservationId !== row.reservationId ||
        state.success.projectId !== row.projectId ||
        (row.taskId !== null && state.success.taskId !== row.taskId) ||
        !evidenceValid
      ) {
        corruptIds.add(row.reservationId);
      }
    }
    return { corruptIds, opaqueCount };
  });

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
      const composite = yield* compositeIntegrity(input.projectId, input.reservationId).pipe(
        Effect.mapError(() =>
          safeError(
            "internal-persistence-error",
            "get-reservation",
            input.projectId,
            input.reservationId,
          ),
        ),
      );
      if (composite.opaqueCount !== 0 || composite.corruptIds.has(input.reservationId)) {
        return yield* safeError(
          "reservation-projection-corrupt",
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
      const composite = yield* compositeIntegrity(input.projectId, null).pipe(
        Effect.mapError(() =>
          safeError("internal-persistence-error", "list-reservations", input.projectId),
        ),
      );
      const [catalogRows, eventRows, projectionRows] = yield* Effect.all([
        sql<Record<string, unknown>>`
          SELECT reservation_id AS "reservationId", project_id AS "projectId",
            task_id AS "taskId", stage_run_id AS "stageRunId",
            attempt_id AS "attemptId", lease_id AS "leaseId",
            fence_token AS "fenceToken", created_at AS "createdAt"
          FROM agent_control_worktree_stream_catalog
          ORDER BY reservation_id ASC
        `,
        sql<{ readonly reservationId: unknown }>`
          SELECT DISTINCT stream_id AS "reservationId"
          FROM agent_control_events
          WHERE aggregate_kind = 'worktree-reservation'
          ORDER BY stream_id ASC
        `,
        sql<{ readonly reservationId: unknown }>`
          SELECT reservation_id AS "reservationId"
          FROM agent_control_worktree_reservation_states
          ORDER BY reservation_id ASC
        `,
      ]).pipe(
        Effect.mapError(() =>
          safeError("internal-persistence-error", "list-reservations", input.projectId),
        ),
      );

      const catalogById = new Map<string, WorktreeStreamCatalogRow>();
      const invalidCatalogIds = new Set<string>();
      const opaqueQuarantinedIds = new Set<string>();
      for (const raw of catalogRows) {
        const decoded = yield* Effect.result(decodeCatalogRow(raw));
        if (decoded._tag === "Failure") {
          const reservationId = yield* Effect.result(decodeReservationId(raw.reservationId));
          if (reservationId._tag === "Success") {
            invalidCatalogIds.add(reservationId.success);
          } else {
            opaqueQuarantinedIds.add(
              `catalog:${typeof raw.reservationId}:${String(raw.reservationId)}`,
            );
          }
        } else {
          catalogById.set(decoded.success.reservationId, decoded.success);
        }
      }
      const decodeIds = Effect.fn("AgentControlWorktree.listReservations.decodeIds")(function* (
        rows: ReadonlyArray<{ readonly reservationId: unknown }>,
      ) {
        const ids = new Set<string>();
        for (const row of rows) {
          const decoded = yield* Effect.result(decodeReservationId(row.reservationId));
          if (decoded._tag === "Failure") {
            opaqueQuarantinedIds.add(`${typeof row.reservationId}:${String(row.reservationId)}`);
          } else {
            ids.add(decoded.success);
          }
        }
        return ids;
      });
      const eventIds = yield* decodeIds(eventRows);
      const projectionIds = yield* decodeIds(projectionRows);
      const allIds = new Set([
        ...catalogById.keys(),
        ...invalidCatalogIds,
        ...eventIds,
        ...projectionIds,
        ...composite.corruptIds,
      ]);
      const globallyQuarantinedIds = new Set<string>();
      const quarantined = new Set<string>();
      for (const reservationId of composite.corruptIds) quarantined.add(reservationId);
      const healthyById = new Map<string, AgentControlWorktreeReservationState>();
      const projectCatalogs: Array<WorktreeStreamCatalogRow> = [];

      for (const reservationId of allIds) {
        const catalog = catalogById.get(reservationId);
        if (catalog === undefined) {
          // Without the immutable catalog, an event/projection identity cannot
          // be assigned to a project by trusting mutable or corrupt payloads.
          globallyQuarantinedIds.add(reservationId);
          continue;
        }
        if (catalog.projectId !== input.projectId) continue;
        projectCatalogs.push(catalog);
        if (!eventIds.has(reservationId)) {
          quarantined.add(reservationId);
          continue;
        }

        const folded = yield* Effect.result(
          foldAuthoritativeWorktreeReservationStream(catalog.reservationId, events),
        );
        if (folded._tag === "Failure") {
          if (folded.failure._tag === "AgentControlPersistenceSqlError") {
            return yield* safeError(
              "internal-persistence-error",
              "list-reservations",
              input.projectId,
            );
          }
          quarantined.add(reservationId);
          continue;
        }
        if (Option.isNone(folded.success)) {
          quarantined.add(reservationId);
          continue;
        }
        const authoritative = folded.success.value.state;
        if (
          authoritative.reservationId !== catalog.reservationId ||
          authoritative.projectId !== catalog.projectId ||
          authoritative.taskId !== catalog.taskId ||
          authoritative.stageRunId !== catalog.stageRunId ||
          authoritative.attemptId !== catalog.attemptId ||
          authoritative.leaseId !== catalog.leaseId ||
          authoritative.fenceToken !== catalog.fenceToken ||
          authoritative.createdAt !== catalog.createdAt
        ) {
          quarantined.add(reservationId);
          continue;
        }

        const projected = yield* Effect.result(states.get(catalog.reservationId));
        if (projected._tag === "Failure") {
          if (projected.failure._tag === "AgentControlPersistenceSqlError") {
            return yield* safeError(
              "internal-persistence-error",
              "list-reservations",
              input.projectId,
            );
          }
          quarantined.add(reservationId);
          continue;
        }
        if (
          Option.isNone(projected.success) ||
          !sameAgentControlWorktreeReservationState(authoritative, projected.success.value)
        ) {
          quarantined.add(reservationId);
          continue;
        }
        healthyById.set(reservationId, authoritative);
      }

      const competing = new Set<string>();
      const byStage = new Map<string, Array<WorktreeStreamCatalogRow>>();
      for (const catalog of projectCatalogs) {
        const key = [catalog.projectId, catalog.taskId, catalog.stageRunId, catalog.attemptId].join(
          "\0",
        );
        const bucket = byStage.get(key) ?? [];
        bucket.push(catalog);
        byStage.set(key, bucket);
      }
      for (const bucket of byStage.values()) {
        if (bucket.length <= 1) continue;
        for (const catalog of bucket) competing.add(catalog.reservationId);
      }
      for (const reservationId of competing) quarantined.add(reservationId);
      const reservations = [...healthyById.values()]
        .filter((state) => !quarantined.has(state.reservationId))
        .toSorted(
          (left, right) =>
            left.createdAt.localeCompare(right.createdAt) ||
            left.reservationId.localeCompare(right.reservationId),
        )
        .map(toAgentControlWorktreeReservationView);
      const quarantinedIds = new Set([...globallyQuarantinedIds, ...quarantined]);
      return {
        projectId: input.projectId,
        reservations,
        quarantinedCount: opaqueQuarantinedIds.size + composite.opaqueCount + quarantinedIds.size,
      };
    });

  return AgentControlWorktree.of({ getReservation, listReservations });
});

export const layer = Layer.effect(AgentControlWorktree, make);
