import {
  AgentControlControlledThreadReservationGetInput,
  AgentControlControlledThreadReservationListInput,
  AgentControlControlledThreadReservationPrepareInitialInput,
  AgentControlControlledThreadReservationRpcError,
  AgentControlControlledThreadReservationId,
  type AgentControlControlledThreadReservationState,
  type AgentControlRejectedCommandErrorCode,
  type AgentControlWorktreeReservationState,
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
  deriveAgentControlControlledThreadReservationId,
  deriveAgentControlReservedThreadId,
  deriveRejectedAgentControlControlledThreadReservationId,
} from "../identity.ts";
import {
  AgentControlControlledThreadReservation,
  type AgentControlControlledThreadReservationShape,
} from "../Services/AgentControlControlledThreadReservation.ts";
import { AgentControlControlledThreadReservationEngine } from "../Services/AgentControlControlledThreadReservationEngine.ts";
import { AgentControlControlledThreadReservationEventStore } from "../Services/AgentControlControlledThreadReservationEventStore.ts";
import { AgentControlControlledThreadReservationStateRepository } from "../Services/AgentControlControlledThreadReservationStateRepository.ts";
import { AgentControlControlledThreadReservationTransactionHooks } from "../Services/AgentControlControlledThreadReservationTransactionHooks.ts";
import { toAgentControlControlledThreadReservationView } from "./AgentControlControlledThreadReservationEngine.ts";
import {
  loadAuthoritativeInitialStageRunHistory,
  loadAuthoritativeLeaseHistoryForStagePosition,
} from "../../stageRunLease/authoritative.ts";
import { canonicalTimestampMillis } from "../../stageRunLease/invariant.ts";
import { deriveAgentControlSourceIdentityFingerprint } from "../../stageRun/identity.ts";
import { AgentControlStageRunEventStore } from "../../stageRun/Services/AgentControlStageRunEventStore.ts";
import { AgentControlStageRunStateRepository } from "../../stageRun/Services/AgentControlStageRunStateRepository.ts";
import { AgentControlStageRunLeaseEventStore } from "../../stageRunLease/Services/AgentControlStageRunLeaseEventStore.ts";
import { AgentControlStageRunLeaseStateRepository } from "../../stageRunLease/Services/AgentControlStageRunLeaseStateRepository.ts";
import { AgentControlStageRunLeaseEngine } from "../../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import {
  AgentControlTaskConsumerGuard,
  type AgentControlTaskConsumerGuardReason,
} from "../../task/Services/AgentControlTaskConsumerGuard.ts";
import { AgentControlWorktree } from "../../worktree/Services/AgentControlWorktree.ts";
import { AgentControlWorktreeController } from "../../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlWorktreeEngine } from "../../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlCommandReceiptRepository } from "../../../persistence/Services/AgentControlCommandReceipts.ts";
import { AgentControlProjectAvailability } from "../../../persistence/Services/AgentControlProjectAvailability.ts";
import {
  foldAuthoritativeControlledThreadReservationStream,
  sameAgentControlControlledThreadReservationState,
} from "../authoritative.ts";
import {
  initialControlledThreadCommandIntent,
  insertControlledThreadCommandIntent,
} from "../commandIntent.ts";

const decodeGet = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationGetInput, {
  onExcessProperty: "error",
});
const decodeList = Schema.decodeUnknownEffect(AgentControlControlledThreadReservationListInput, {
  onExcessProperty: "error",
});
const decodePrepare = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationPrepareInitialInput,
  { onExcessProperty: "error" },
);
const StreamCatalogRow = Schema.Struct({
  controlledThreadReservationId: AgentControlControlledThreadReservationId,
  threadId: Schema.String,
  projectId: Schema.String,
  taskId: Schema.String,
  taskRevision: Schema.Number,
  githubIntakeSequence: Schema.Number,
  sourceIdentityFingerprint: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  roleId: Schema.String,
  stageKind: Schema.String,
  stageOrdinal: Schema.Number,
  attemptOrdinal: Schema.Number,
  leaseId: Schema.String,
  fenceToken: Schema.Number,
  worktreeReservationId: Schema.String,
  preparedAt: Schema.String,
});
type StreamCatalogRow = typeof StreamCatalogRow.Type;
const decodeStreamCatalogRow = Schema.decodeUnknownEffect(StreamCatalogRow);
const decodeControlledThreadReservationId = Schema.decodeUnknownEffect(
  AgentControlControlledThreadReservationId,
);
const encodePrepare = Schema.encodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      type: Schema.Literal("agentControl.controlledThreadReservation.prepareInitial"),
      authority: Schema.Literal("controller"),
      commandId: Schema.String,
      projectId: Schema.String,
      taskId: Schema.String,
    }),
  ),
);
const isRpcError = Schema.is(AgentControlControlledThreadReservationRpcError);
const RECEIPTLESS = new Set<AgentControlControlledThreadReservationRpcError["code"]>([
  "internal-persistence-error",
  "source-snapshot-unavailable",
  "source-watermark-stale",
  "task-projection-corrupt",
  "stage-run-missing",
  "stage-run-projection-corrupt",
  "lease-missing",
  "lease-projection-corrupt",
  "worktree-missing",
  "worktree-not-ready",
  "worktree-projection-corrupt",
  "worktree-history-ambiguous",
  "controlled-thread-reservation-corrupt",
]);

const safeError = (
  code: AgentControlControlledThreadReservationRpcError["code"],
  operation: AgentControlControlledThreadReservationRpcError["operation"],
  projectId: AgentControlControlledThreadReservationGetInput["projectId"],
  taskId: AgentControlControlledThreadReservationPrepareInitialInput["taskId"] | null = null,
  controlledThreadReservationId: AgentControlControlledThreadReservationId | null = null,
) =>
  new AgentControlControlledThreadReservationRpcError({
    code,
    operation,
    projectId,
    taskId,
    controlledThreadReservationId,
  });

const guardCode = (
  reason: AgentControlTaskConsumerGuardReason,
): AgentControlControlledThreadReservationRpcError["code"] => {
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

const sameWorktreeBinding = (
  selected: AgentControlWorktreeReservationState,
  guarded: AgentControlWorktreeReservationState,
) =>
  guarded.reservationId === selected.reservationId &&
  guarded.projectId === selected.projectId &&
  guarded.taskId === selected.taskId &&
  guarded.taskRevision === selected.taskRevision &&
  guarded.githubIntakeSequence === selected.githubIntakeSequence &&
  guarded.sourceIdentityFingerprint === selected.sourceIdentityFingerprint &&
  guarded.stageRunId === selected.stageRunId &&
  guarded.attemptId === selected.attemptId &&
  guarded.leaseId === selected.leaseId &&
  guarded.fenceToken === selected.fenceToken &&
  guarded.status === "ready";

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const availability = yield* AgentControlProjectAvailability;
  const receipts = yield* AgentControlCommandReceiptRepository;
  const engine = yield* AgentControlControlledThreadReservationEngine;
  const events = yield* AgentControlControlledThreadReservationEventStore;
  const states = yield* AgentControlControlledThreadReservationStateRepository;
  const taskGuard = yield* AgentControlTaskConsumerGuard;
  const stageEvents = yield* AgentControlStageRunEventStore;
  const stageStates = yield* AgentControlStageRunStateRepository;
  const leaseEvents = yield* AgentControlStageRunLeaseEventStore;
  const leaseStates = yield* AgentControlStageRunLeaseStateRepository;
  const leaseEngine = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktree;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const worktreeController = yield* AgentControlWorktreeController;
  const runtimeHolderId = yield* leaseEngine.runtimeHolderId;
  const transactionHooks = yield* AgentControlControlledThreadReservationTransactionHooks;

  const fingerprint = Effect.fn("AgentControlControlledThreadReservation.fingerprint")(function* (
    input: AgentControlControlledThreadReservationPrepareInitialInput,
  ) {
    const canonical = yield* encodePrepare({
      type: "agentControl.controlledThreadReservation.prepareInitial",
      authority: "controller",
      commandId: input.commandId,
      projectId: input.projectId,
      taskId: input.taskId,
    });
    const digest = yield* crypto.digest("SHA-256", new TextEncoder().encode(canonical));
    return Encoding.encodeHex(digest);
  });

  const ensureProject = (
    projectId: AgentControlControlledThreadReservationGetInput["projectId"],
    operation: AgentControlControlledThreadReservationRpcError["operation"],
  ) =>
    availability
      .ensureAvailable(projectId)
      .pipe(
        Effect.mapError((failure) =>
          safeError(
            failure._tag === "AgentControlProjectUnavailableError"
              ? "project-unavailable"
              : "internal-persistence-error",
            operation,
            projectId,
          ),
        ),
      );

  const persistRejected = Effect.fn("AgentControlControlledThreadReservation.persistRejected")(
    function* (
      input: AgentControlControlledThreadReservationPrepareInitialInput,
      commandFingerprint: string,
      code: AgentControlControlledThreadReservationRpcError["code"],
    ) {
      if (RECEIPTLESS.has(code))
        return yield* safeError(code, "prepare-initial", input.projectId, input.taskId);
      const aggregateId = yield* deriveRejectedAgentControlControlledThreadReservationId(input);
      const rejectedAt = DateTime.formatIso(yield* DateTime.now);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* engine.validateTaskHistory(input.projectId, input.taskId);
          yield* insertControlledThreadCommandIntent(
            sql,
            initialControlledThreadCommandIntent(input, commandFingerprint, aggregateId),
          );
          yield* receipts.insert({
            commandId: input.commandId,
            commandFingerprint,
            authority: "controller",
            aggregateKind: "controlled-thread-reservation",
            aggregateId,
            status: "rejected",
            resultSequence: 0,
            resultStreamVersion: 0,
            eventCreated: false,
            acceptedAt: rejectedAt,
            errorCode: code as AgentControlRejectedCommandErrorCode,
          });
        }),
      );
      return safeError(code, "prepare-initial", input.projectId, input.taskId);
    },
  );

  const initialBinding = Effect.fn("AgentControlControlledThreadReservation.initialBinding")(
    function* (input: AgentControlControlledThreadReservationPrepareInitialInput) {
      return yield* taskGuard
        .useTaskConsumable(input.projectId, input.taskId, (task) =>
          Effect.gen(function* () {
            const sourceIdentityFingerprint =
              yield* deriveAgentControlSourceIdentityFingerprint(task);
            const stageHistory = yield* loadAuthoritativeInitialStageRunHistory(
              input.projectId,
              input.taskId,
              stageEvents,
              stageStates,
            ).pipe(
              Effect.mapError((failure) =>
                safeError(
                  failure._tag === "AgentControlPersistenceSqlError"
                    ? "internal-persistence-error"
                    : "stage-run-projection-corrupt",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
              ),
            );
            const stageMatches = stageHistory.filter(
              (stage) =>
                stage.taskRevision === task.revision &&
                stage.githubIntakeSequence === task.githubIntakeSequence &&
                stage.sourceIdentityFingerprint === sourceIdentityFingerprint &&
                stage.stageKind === "planning" &&
                stage.roleId === "planning" &&
                stage.stageOrdinal === 1 &&
                stage.attemptOrdinal === 1,
            );
            if (stageMatches.length === 0) {
              return yield* safeError(
                "stage-run-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (stageMatches.length !== 1) {
              return yield* safeError(
                "stage-run-history-ambiguous",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const stage = stageMatches[0]!;
            if (stage.status !== "prepared") {
              return yield* safeError(
                "stage-run-not-prepared",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }

            const candidates = yield* loadAuthoritativeLeaseHistoryForStagePosition(
              {
                projectId: input.projectId,
                taskId: input.taskId,
                stageRunId: stage.stageRunId,
                attemptId: stage.attemptId,
                taskRevision: task.revision,
                githubIntakeSequence: task.githubIntakeSequence,
                sourceIdentityFingerprint,
              },
              leaseEvents,
              leaseStates,
            ).pipe(
              Effect.mapError((failure) =>
                safeError(
                  failure._tag === "AgentControlPersistenceSqlError"
                    ? "internal-persistence-error"
                    : "lease-projection-corrupt",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
              ),
            );
            if (candidates.length === 0) {
              return yield* safeError(
                "lease-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (candidates.length !== 1) {
              return yield* safeError(
                "lease-projection-corrupt",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const leaseState = candidates[0]!;
            if (leaseState.status !== "reserved") {
              return yield* safeError(
                "lease-not-reserved",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (leaseState.holderId !== runtimeHolderId) {
              return yield* safeError(
                "lease-foreign-runtime",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const expiresAt = canonicalTimestampMillis(leaseState.expiresAt);
            const now = yield* DateTime.now;
            if (expiresAt === null || expiresAt <= DateTime.toEpochMillis(now)) {
              return yield* safeError(
                "lease-expired",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (
              leaseState.taskRevision !== task.revision ||
              leaseState.githubIntakeSequence !== task.githubIntakeSequence ||
              leaseState.sourceIdentityFingerprint !== sourceIdentityFingerprint
            ) {
              return yield* safeError(
                "source-snapshot-stale",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }

            const listed = yield* worktrees
              .listReservations({ projectId: input.projectId })
              .pipe(
                Effect.mapError((failure) =>
                  safeError(
                    failure.code === "internal-persistence-error"
                      ? "internal-persistence-error"
                      : "worktree-projection-corrupt",
                    "prepare-initial",
                    input.projectId,
                    input.taskId,
                  ),
                ),
              );
            if (listed.quarantinedCount !== 0) {
              return yield* safeError(
                "worktree-projection-corrupt",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const worktreeViews = listed.reservations.filter(
              (worktree) =>
                worktree.taskId === input.taskId &&
                worktree.stageRunId === stage.stageRunId &&
                worktree.attemptId === stage.attemptId &&
                worktree.leaseId === leaseState.leaseId &&
                worktree.fenceToken === leaseState.fenceToken,
            );
            if (worktreeViews.length === 0) {
              return yield* safeError(
                "worktree-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (worktreeViews.length !== 1) {
              return yield* safeError(
                "worktree-history-ambiguous",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (worktreeViews[0]!.status !== "ready") {
              return yield* safeError(
                "worktree-not-ready",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            const worktree = yield* worktreeEngine
              .loadAuthoritative(worktreeViews[0]!.reservationId)
              .pipe(
                Effect.mapError((failure) =>
                  safeError(
                    failure.code === "internal-persistence-error"
                      ? "internal-persistence-error"
                      : "worktree-projection-corrupt",
                    "prepare-initial",
                    input.projectId,
                    input.taskId,
                  ),
                ),
              );
            if (worktree === null) {
              return yield* safeError(
                "worktree-missing",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            if (
              worktree.status !== "ready" ||
              worktree.taskRevision !== task.revision ||
              worktree.githubIntakeSequence !== task.githubIntakeSequence ||
              worktree.sourceIdentityFingerprint !== sourceIdentityFingerprint
            ) {
              return yield* safeError(
                "source-snapshot-stale",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
            }
            return { task, sourceIdentityFingerprint, stage, lease: leaseState, worktree };
          }),
        )
        .pipe(
          Effect.mapError((failure) =>
            failure._tag === "AgentControlTaskConsumerGuardError"
              ? safeError(
                  guardCode(failure.reason),
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                )
              : failure,
          ),
        );
    },
  );

  const get: AgentControlControlledThreadReservationShape["get"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeGet(rawInput).pipe(
        Effect.mapError(() =>
          safeError(
            "validation",
            "get",
            rawInput.projectId,
            null,
            rawInput.controlledThreadReservationId,
          ),
        ),
      );
      const state = yield* engine.getAuthoritative(input.controlledThreadReservationId);
      if (Option.isNone(state) || state.value.projectId !== input.projectId) {
        return yield* safeError(
          "controlled-thread-reservation-missing",
          "get",
          input.projectId,
          null,
          input.controlledThreadReservationId,
        );
      }
      yield* engine.validateTaskHistory(input.projectId, state.value.taskId);
      yield* ensureProject(input.projectId, "get");
      return toAgentControlControlledThreadReservationView(state.value);
    });

  const list: AgentControlControlledThreadReservationShape["list"] = (rawInput) =>
    Effect.gen(function* () {
      const input = yield* decodeList(rawInput).pipe(
        Effect.mapError(() => safeError("validation", "list", rawInput.projectId)),
      );
      yield* ensureProject(input.projectId, "list");
      const [rawCatalogRows, rawEventIds, rawProjectionIds] = yield* Effect.all([
        sql<Record<string, unknown>>`
          SELECT
            controlled_thread_reservation_id AS "controlledThreadReservationId",
            thread_id AS "threadId", project_id AS "projectId", task_id AS "taskId",
            task_revision AS "taskRevision",
            github_intake_sequence AS "githubIntakeSequence",
            source_identity_fingerprint AS "sourceIdentityFingerprint",
            stage_run_id AS "stageRunId", attempt_id AS "attemptId",
            role_id AS "roleId", stage_kind AS "stageKind",
            stage_ordinal AS "stageOrdinal", attempt_ordinal AS "attemptOrdinal",
            lease_id AS "leaseId", fence_token AS "fenceToken",
            worktree_reservation_id AS "worktreeReservationId",
            prepared_at AS "preparedAt"
          FROM agent_control_controlled_thread_stream_catalog
          WHERE stream_version = 1
          ORDER BY controlled_thread_reservation_id ASC
        `,
        sql<{ readonly controlledThreadReservationId: unknown }>`
          SELECT DISTINCT stream_id AS "controlledThreadReservationId"
          FROM agent_control_events
          WHERE aggregate_kind = 'controlled-thread-reservation'
          ORDER BY stream_id ASC
        `,
        sql<{ readonly controlledThreadReservationId: unknown }>`
          SELECT controlled_thread_reservation_id AS "controlledThreadReservationId"
          FROM agent_control_controlled_thread_reservation_states
          ORDER BY controlled_thread_reservation_id ASC
        `,
      ]).pipe(
        Effect.mapError(() => safeError("internal-persistence-error", "list", input.projectId)),
      );

      const opaque = new Set<string>();
      const catalogById = new Map<string, StreamCatalogRow>();
      const invalidCatalogIds = new Set<string>();
      for (const raw of rawCatalogRows) {
        const decoded = yield* Effect.result(decodeStreamCatalogRow(raw));
        if (decoded._tag === "Success") {
          catalogById.set(decoded.success.controlledThreadReservationId, decoded.success);
        } else {
          const decodedId = yield* Effect.result(
            decodeControlledThreadReservationId(raw.controlledThreadReservationId),
          );
          if (decodedId._tag === "Success") invalidCatalogIds.add(decodedId.success);
          else
            opaque.add(
              `catalog:${typeof raw.controlledThreadReservationId}:${String(raw.controlledThreadReservationId)}`,
            );
        }
      }
      const decodeIds = Effect.fn("AgentControlControlledThreadReservation.list.decodeIds")(
        function* (rows: ReadonlyArray<{ readonly controlledThreadReservationId: unknown }>) {
          const ids = new Set<string>();
          for (const row of rows) {
            const decoded = yield* Effect.result(
              decodeControlledThreadReservationId(row.controlledThreadReservationId),
            );
            if (decoded._tag === "Success") ids.add(decoded.success);
            else
              opaque.add(
                `${typeof row.controlledThreadReservationId}:${String(row.controlledThreadReservationId)}`,
              );
          }
          return ids;
        },
      );
      const eventIds = yield* decodeIds(rawEventIds);
      const projectionIds = yield* decodeIds(rawProjectionIds);
      const allIds = new Set([
        ...catalogById.keys(),
        ...invalidCatalogIds,
        ...eventIds,
        ...projectionIds,
      ]);
      const globalQuarantine = new Set<string>();
      const quarantine = new Set<string>();
      const healthy = new Map<string, AgentControlControlledThreadReservationState>();
      const projectCatalogs: Array<StreamCatalogRow> = [];

      for (const rawId of allIds) {
        const controlledThreadReservationId = AgentControlControlledThreadReservationId.make(rawId);
        const catalog = catalogById.get(rawId);
        if (catalog?.projectId === input.projectId) projectCatalogs.push(catalog);
        if (catalog !== undefined && catalog.projectId !== input.projectId) continue;
        const folded = yield* Effect.result(
          foldAuthoritativeControlledThreadReservationStream(controlledThreadReservationId, events),
        );
        if (folded._tag === "Failure") {
          if (folded.failure._tag === "AgentControlPersistenceSqlError") {
            return yield* safeError("internal-persistence-error", "list", input.projectId);
          }
          if (catalog === undefined) globalQuarantine.add(rawId);
          else quarantine.add(rawId);
          continue;
        }
        if (catalog === undefined) {
          globalQuarantine.add(rawId);
          continue;
        }
        if (Option.isNone(folded.success)) {
          quarantine.add(rawId);
          continue;
        }
        const state = folded.success.value;
        if (
          state.controlledThreadReservationId !== catalog.controlledThreadReservationId ||
          state.threadId !== catalog.threadId ||
          state.projectId !== catalog.projectId ||
          state.taskId !== catalog.taskId ||
          state.taskRevision !== catalog.taskRevision ||
          state.githubIntakeSequence !== catalog.githubIntakeSequence ||
          state.sourceIdentityFingerprint !== catalog.sourceIdentityFingerprint ||
          state.stageRunId !== catalog.stageRunId ||
          state.attemptId !== catalog.attemptId ||
          state.roleId !== catalog.roleId ||
          state.stageKind !== catalog.stageKind ||
          state.stageOrdinal !== catalog.stageOrdinal ||
          state.attemptOrdinal !== catalog.attemptOrdinal ||
          state.leaseId !== catalog.leaseId ||
          state.fenceToken !== catalog.fenceToken ||
          state.worktreeReservationId !== catalog.worktreeReservationId ||
          state.preparedAt !== catalog.preparedAt
        ) {
          quarantine.add(rawId);
          continue;
        }
        const projected = yield* Effect.result(states.get(controlledThreadReservationId));
        if (projected._tag === "Failure") {
          if (projected.failure._tag === "AgentControlPersistenceSqlError") {
            return yield* safeError("internal-persistence-error", "list", input.projectId);
          }
          quarantine.add(rawId);
          continue;
        }
        if (
          Option.isNone(projected.success) ||
          !sameAgentControlControlledThreadReservationState(state, projected.success.value)
        ) {
          quarantine.add(rawId);
          continue;
        }
        healthy.set(rawId, state);
      }

      const byPosition = new Map<string, Array<StreamCatalogRow>>();
      for (const catalog of projectCatalogs) {
        const key = [
          catalog.projectId,
          catalog.taskId,
          catalog.stageRunId,
          catalog.attemptId,
          catalog.roleId,
          String(catalog.stageOrdinal),
          String(catalog.attemptOrdinal),
        ].join("\0");
        const bucket = byPosition.get(key) ?? [];
        bucket.push(catalog);
        byPosition.set(key, bucket);
      }
      for (const bucket of byPosition.values()) {
        if (bucket.length <= 1) continue;
        for (const catalog of bucket) {
          quarantine.add(catalog.controlledThreadReservationId);
        }
      }
      const quarantinedIds = new Set([...globalQuarantine, ...quarantine]);
      return {
        projectId: input.projectId,
        reservations: [...healthy.values()]
          .filter((state) => !quarantine.has(state.controlledThreadReservationId))
          .toSorted(
            (left, right) =>
              left.taskRevision - right.taskRevision ||
              left.githubIntakeSequence - right.githubIntakeSequence ||
              left.stageOrdinal - right.stageOrdinal ||
              left.attemptOrdinal - right.attemptOrdinal ||
              left.controlledThreadReservationId.localeCompare(right.controlledThreadReservationId),
          )
          .map(toAgentControlControlledThreadReservationView),
        quarantinedCount: opaque.size + quarantinedIds.size,
      };
    });

  const prepareInitial: AgentControlControlledThreadReservationShape["prepareInitial"] = (
    rawInput,
  ) =>
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
      const replay = yield* engine.replayReceiptFirst({
        commandId: input.commandId,
        projectId: input.projectId,
        taskId: input.taskId,
        commandFingerprint,
      });
      if (Option.isSome(replay)) return replay.value;

      // Complete reservation history is checked before any current authority,
      // Git, marker, registration, or worktree observation.
      yield* engine.validateTaskHistory(input.projectId, input.taskId);

      const initial = yield* Effect.result(initialBinding(input));
      if (initial._tag === "Failure") {
        const persisted = yield* Effect.result(
          persistRejected(input, commandFingerprint, initial.failure.code),
        );
        if (persisted._tag === "Failure") {
          const raced = yield* engine.replayReceiptFirst({
            commandId: input.commandId,
            projectId: input.projectId,
            taskId: input.taskId,
            commandFingerprint,
          });
          if (Option.isSome(raced)) return raced.value;
          return yield* isRpcError(persisted.failure)
            ? persisted.failure
            : safeError(
                "internal-persistence-error",
                "prepare-initial",
                input.projectId,
                input.taskId,
              );
        }
        return yield* persisted.success;
      }

      const binding = initial.success;
      const guarded = yield* worktreeController
        .useReadyWorktree(
          {
            projectId: input.projectId,
            reservationId: binding.worktree.reservationId,
          },
          (guardedWorktree) =>
            Effect.gen(function* () {
              yield* transactionHooks.afterReadyInspection;
              if (!sameWorktreeBinding(binding.worktree, guardedWorktree)) {
                return yield* safeError(
                  "source-snapshot-stale",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                );
              }
              const stableIdentity = {
                projectId: input.projectId,
                taskId: input.taskId,
                taskRevision: binding.task.revision,
                githubIntakeSequence: binding.task.githubIntakeSequence,
                sourceIdentityFingerprint: binding.sourceIdentityFingerprint,
                stageRunId: binding.stage.stageRunId,
                attemptId: binding.stage.attemptId,
                roleId: binding.stage.roleId,
                stageKind: "planning" as const,
                stageOrdinal: 1 as const,
                attemptOrdinal: 1 as const,
              };
              const controlledThreadReservationId =
                yield* deriveAgentControlControlledThreadReservationId(stableIdentity);
              const threadId = yield* deriveAgentControlReservedThreadId(stableIdentity);
              const dispatched = yield* Effect.result(
                engine.dispatchPreparedController(
                  {
                    type: "agentControl.controlledThreadReservation.prepare",
                    commandId: input.commandId,
                    authority: "controller",
                    controlledThreadReservationId,
                    threadId,
                    ...stableIdentity,
                    leaseId: binding.lease.leaseId,
                    fenceToken: binding.lease.fenceToken,
                    worktreeReservationId: binding.worktree.reservationId,
                    expectedRevision: 0,
                  },
                  commandFingerprint,
                ),
              );
              if (dispatched._tag === "Failure") {
                if (dispatched.failure.code !== "internal-persistence-error") {
                  return yield* dispatched.failure;
                }
                return {
                  _tag: "ReplayReceiptAfterPersistenceConflict" as const,
                  error: dispatched.failure,
                };
              }
              return dispatched.success;
            }),
        )
        .pipe(
          Effect.mapError((failure) =>
            failure._tag === "AgentControlControlledThreadReservationRpcError"
              ? failure
              : safeError(
                  failure.code === "internal-persistence-error"
                    ? "internal-persistence-error"
                    : failure.code === "reservation-projection-corrupt"
                      ? "worktree-projection-corrupt"
                      : "worktree-not-ready",
                  "prepare-initial",
                  input.projectId,
                  input.taskId,
                ),
          ),
        );
      if (guarded._tag === "ReplayReceiptAfterPersistenceConflict") {
        // The guarded callback and its nested dispatch transaction are finished
        // before this one fresh receipt-only transaction begins.
        const raced = yield* Effect.result(
          engine.replayReceiptFirst({
            commandId: input.commandId,
            projectId: input.projectId,
            taskId: input.taskId,
            commandFingerprint,
          }),
        );
        if (raced._tag === "Failure") {
          return yield* raced.failure.code === "internal-persistence-error"
            ? guarded.error
            : raced.failure;
        }
        if (Option.isSome(raced.success)) return raced.success.value;
        return yield* guarded.error;
      }
      if (guarded._tag === "Rejected") return yield* guarded.error;
      yield* engine.publishCommitted(guarded.events);
      return guarded.result;
    });

  return AgentControlControlledThreadReservation.of({ get, list, prepareInitial });
});

export const layer = Layer.effect(AgentControlControlledThreadReservation, make);
