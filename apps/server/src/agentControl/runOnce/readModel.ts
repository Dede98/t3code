import {
  AgentControlInternalPersistenceError,
  AgentControlRunOnceSnapshot,
  type AgentControlRunOnceSnapshotInput,
  AgentControlRunOnceState,
  type AgentControlRunOnceStageView,
  AgentControlStageRunState,
  AgentControlVerificationChecks,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentControlEngine } from "../Services/AgentControlEngine.ts";
import { AgentControlTaskIntake } from "../task/Services/AgentControlTaskIntake.ts";
import { AgentControlTaskEngine } from "../task/Services/AgentControlTaskEngine.ts";
import { AgentControlStageRunEngine } from "../stageRun/Services/AgentControlStageRunEngine.ts";
import { AgentControlWorktreeEngine } from "../worktree/Services/AgentControlWorktreeEngine.ts";
import { AgentControlRunOnceController } from "./Services/AgentControlRunOnceController.ts";
import { AgentControlRunOnceReadNotifications } from "./readNotifications.ts";
import { selectAgentControlRunOnceCandidate } from "./selection.ts";
import { deriveRunOnceCommandId } from "./identity.ts";

const decodeSnapshot = Schema.decodeUnknownEffect(AgentControlRunOnceSnapshot);
const encodeSnapshot = Schema.encodeSync(Schema.fromJsonString(AgentControlRunOnceSnapshot));
const decodeRun = Schema.decodeUnknownEffect(AgentControlRunOnceState);
const decodeStage = Schema.decodeUnknownEffect(Schema.fromJsonString(AgentControlStageRunState));
const decodeChecks = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlVerificationChecks),
);
const decodeResult = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({
      exitCode: Schema.Int,
      stdout: Schema.String,
      stderr: Schema.String,
    }),
  ),
);

/** Reads only durable projections/evidence, bounded to one selected run. */
export const makeAgentControlRunOnceReadModel = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* AgentControlEngine;
  const tasks = yield* AgentControlTaskIntake;
  const taskEngine = yield* AgentControlTaskEngine;
  const stageEngine = yield* AgentControlStageRunEngine;
  const worktreeEngine = yield* AgentControlWorktreeEngine;
  const controller = yield* AgentControlRunOnceController;
  const notifications = yield* AgentControlRunOnceReadNotifications;

  const getSnapshot = Effect.fn("AgentControlRunOnce.getSnapshot")(function* (
    input: AgentControlRunOnceSnapshotInput,
  ) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const projectState = yield* engine.getProjectState(input);
          const listed = yield* tasks.listTasks(input);
          const intakeSequence = Math.max(
            0,
            ...listed.tasks.map((task) => task.githubIntakeSequence),
          );
          const nextTaskId =
            intakeSequence === 0
              ? null
              : yield* selectAgentControlRunOnceCandidate(sql, input.projectId, intakeSequence);
          const rows = yield* sql`
        SELECT 1 AS "schemaVersion", run_id AS "runId", project_id AS "projectId", status,
          next_ordinal AS "nextOrdinal", last_step AS "lastStep", task_id AS "taskId",
          stage_run_id AS "stageRunId", lease_id AS "leaseId",
          worktree_reservation_id AS "worktreeReservationId",
          controlled_thread_reservation_id AS "controlledThreadReservationId",
          terminal_task_event_id AS "terminalTaskEventId", activation_project_revision AS "activationProjectRevision",
          reset_project_revision AS "resetProjectRevision", updated_at AS "updatedAt"
        FROM main.agent_control_run_once_states
        WHERE project_id = ${input.projectId} AND (${input.runId ?? null} IS NULL OR run_id = ${input.runId ?? null})
        ORDER BY activation_project_revision DESC LIMIT 1
      `;
          const diagnostics = yield* sql<{ runId: string | null; errorCode: string }>`
        SELECT run_id AS "runId", error_code AS "errorCode" FROM main.agent_control_run_once_diagnostics
        WHERE project_id = ${input.projectId}
      `;
          const runs = [];
          for (const row of rows) {
            const state = yield* decodeRun(row);
            const stageRows =
              state.taskId === null
                ? []
                : yield* sql<{ stateJson: string; repair: number }>`
          SELECT stage.state_json AS "stateJson", EXISTS(
            SELECT 1 FROM main.agent_control_run_once_repairs repair
            WHERE repair.run_id = ${state.runId} AND repair.repair_stage_run_id = stage.stage_run_id
          ) AS repair
          FROM main.agent_control_stage_run_states stage
          WHERE stage.project_id = ${input.projectId} AND stage.task_id = ${state.taskId}
          ORDER BY stage.stage_ordinal, stage.attempt_ordinal
        `;
            const stages: AgentControlRunOnceStageView[] = [];
            for (const stageRow of stageRows) {
              const stage = yield* decodeStage(stageRow.stateJson);
              if (
                stage.stageKind !== "planning" &&
                stage.stageKind !== "implementation" &&
                stage.stageKind !== "verification"
              )
                continue;
              const prefix = stage.stageKind === "planning" ? "initial_planning" : stage.stageKind;
              const handoffs = yield* sql.unsafe<{
                handoffId: string;
                threadId: string;
                worktreePath: string;
                providerDeliveryId: string;
                errorCode: string | null;
                branch: string | null;
              }>(
                `SELECT intent.handoff_id AS "handoffId", intent.thread_id AS "threadId",
            intent.worktree_path AS "worktreePath", intent.provider_delivery_id AS "providerDeliveryId",
            delivery.last_error_code AS "errorCode", json_extract(worktree.state_json, '$.branchName') AS branch
            FROM main.agent_control_${prefix}_handoff_intents intent
            LEFT JOIN main.agent_control_${prefix}_deliveries delivery ON delivery.handoff_id = intent.handoff_id
            LEFT JOIN main.agent_control_worktree_reservation_states worktree ON worktree.reservation_id = intent.worktree_reservation_id
            WHERE intent.project_id = ? AND intent.task_id = ? AND intent.stage_run_id = ?`,
                [input.projectId, state.taskId, stage.stageRunId],
              ).unprepared;
              const handoff = handoffs[0];
              let verification: AgentControlRunOnceStageView["verification"] = null;
              if (stage.stageKind === "verification" && handoff) {
                const evaluations = yield* sql<{
                  verdict: "passed" | "failed" | null;
                  errorCode: string | null;
                  evaluatedAt: string;
                }>`
              SELECT evaluation.verdict, evaluation.error_code AS "errorCode", evaluation.evaluated_at AS "evaluatedAt"
              FROM main.agent_control_verification_evaluation_evidence evaluation
              JOIN main.agent_control_verification_evaluation_markers marker ON marker.marker_id = evaluation.marker_id
                AND marker.evidence_id = evaluation.evidence_id AND marker.evaluation_fingerprint = evaluation.evaluation_fingerprint
              WHERE evaluation.project_id = ${input.projectId} AND evaluation.task_id = ${state.taskId}
                AND evaluation.stage_run_id = ${stage.stageRunId} AND evaluation.provider_delivery_id = ${handoff.providerDeliveryId}
            `;
                const manifests = yield* sql<{ checksJson: string; manifestDigest: string }>`
              SELECT checks_json AS "checksJson", manifest_digest AS "manifestDigest"
              FROM main.agent_control_verification_check_manifests WHERE provider_delivery_id = ${handoff.providerDeliveryId}
                AND handoff_id = ${handoff.handoffId}
            `;
                const checks = manifests[0] ? yield* decodeChecks(manifests[0].checksJson) : [];
                const checkViews = [];
                for (const check of checks) {
                  const evidence = yield* sql<{
                    status: "passed" | "failed" | "unavailable" | "stale" | null;
                    resultJson: string | null;
                    completedAt: string | null;
                  }>`SELECT result.status, result.result_json AS "resultJson", result.completed_at AS "completedAt"
                FROM main.agent_control_verification_check_starts start
                LEFT JOIN main.agent_control_verification_check_results result
                  ON result.provider_delivery_id = start.provider_delivery_id AND result.check_id = start.check_id
                  AND result.provider_turn_id = start.provider_turn_id AND result.manifest_digest = start.manifest_digest
                WHERE start.provider_delivery_id = ${handoff.providerDeliveryId} AND start.check_id = ${check.id}
                  AND start.manifest_digest = ${manifests[0]!.manifestDigest}`;
                  const result = evidence[0]?.resultJson
                    ? yield* decodeResult(evidence[0].resultJson)
                    : null;
                  checkViews.push({
                    id: check.id,
                    command: check.command,
                    args: check.args,
                    cwd: check.cwd,
                    required: check.required,
                    status:
                      evidence[0]?.status ??
                      (evidence.length &&
                      (stage.status === "running" ||
                        stage.status === "queued" ||
                        stage.status === "prepared")
                        ? ("running" as const)
                        : ("missing" as const)),
                    exitCode: result?.exitCode ?? null,
                    output: result ? `${result.stdout}\n${result.stderr}`.slice(0, 8192) : null,
                    completedAt: evidence[0]?.completedAt ?? null,
                  });
                }
                verification = {
                  providerDeliveryId: handoff.providerDeliveryId,
                  verdict: evaluations[0]?.verdict ?? null,
                  errorCode:
                    evaluations[0]?.errorCode ??
                    (manifests.length === 0 ? "verification-checks-missing" : null),
                  evaluatedAt: evaluations[0]?.evaluatedAt ?? null,
                  checks: checkViews,
                };
              }
              stages.push({
                ...stage,
                displayStage: stageRow.repair ? "repair" : stage.stageKind,
                threadId: handoff ? ThreadId.make(handoff.threadId) : null,
                worktreePath: handoff?.worktreePath ?? null,
                branch: handoff?.branch ?? null,
                errorCode: handoff?.errorCode ?? null,
                verification,
              });
            }
            let errorCode =
              diagnostics.find((item) => item.runId === state.runId)?.errorCode ?? null;
            if (
              errorCode === null &&
              state.status === "completed" &&
              state.terminalTaskEventId === null &&
              state.taskId !== null
            ) {
              // Human takeover clears the current blocker. Its immutable rejected
              // worktree command still explains the ended run without blocking a new one.
              const leaseSteps = yield* sql<{ ordinal: number }>`
                SELECT ordinal FROM main.agent_control_run_once_step_evidence
                WHERE run_id = ${state.runId} AND step = 'lease-reserved'
              `;
              if (leaseSteps[0]) {
                const commandId = deriveRunOnceCommandId(
                  state.runId,
                  leaseSteps[0].ordinal + 1,
                  "worktree-ready",
                );
                const rejected = yield* sql<{ code: string }>`
                  SELECT rejection_code AS code FROM main.agent_control_worktree_controller_operations
                  WHERE command_id = ${commandId} AND project_id = ${state.projectId}
                    AND task_id = ${state.taskId} AND command_type = 'reserve-and-materialize'
                    AND status = 'rejected'
                `;
                if (rejected[0]) errorCode = `downstream-rejected: ${rejected[0].code}`;
              }
            }
            runs.push({
              errorCode,
              state,
              task: listed.tasks.find((task) => task.taskId === state.taskId) ?? null,
              stages,
            });
          }
          return yield* decodeSnapshot({
            blockers: diagnostics.map((item) => item.errorCode),
            projectId: input.projectId,
            projectState,
            tasks: listed.tasks,
            nextTaskId,
            runs,
          });
        }),
      )
      .pipe(
        Effect.mapError(
          () => new AgentControlInternalPersistenceError({ code: "internal-persistence-error" }),
        ),
      );
  });

  const subscribe = Effect.fn("AgentControlRunOnce.subscribe")(function* (
    input: AgentControlRunOnceSnapshotInput,
  ) {
    // Acquire every hot subscription before reading the snapshot. A concurrent
    // commit remains buffered and is refetched after the initial snapshot.
    const projectEvents = yield* (
      engine.subscribeDomainEvents ?? Effect.succeed(engine.streamDomainEvents)
    );
    const taskEvents = yield* taskEngine.subscribeDomainEvents;
    const stageEvents = yield* stageEngine.subscribeDomainEvents;
    const worktreeEvents = yield* worktreeEngine.subscribeDomainEvents;
    const publications = yield* controller.subscribePublicationWakeups;
    const checkEvents = yield* notifications.subscribe;
    const diagnosticEvents = yield* notifications.subscribeProjects;
    const updates = Stream.mergeAll(
      [
        diagnosticEvents.pipe(
          Stream.filter((projectId) => projectId === input.projectId),
          Stream.map(() => undefined),
        ),
        projectEvents.pipe(
          Stream.filter((event) => event.payload.projectId === input.projectId),
          Stream.map(() => undefined),
        ),
        taskEvents.pipe(
          Stream.filter(
            (event) =>
              ("source" in event.payload
                ? event.payload.source.projectId
                : event.payload.projectId) === input.projectId,
          ),
          Stream.map(() => undefined),
        ),
        stageEvents.pipe(
          Stream.filter((event) => event.payload.projectId === input.projectId),
          Stream.map(() => undefined),
        ),
        worktreeEvents.pipe(
          Stream.filter((event) => event.payload.projectId === input.projectId),
          Stream.map(() => undefined),
        ),
        publications.pipe(
          Stream.filter((event) => event.projectId === input.projectId),
          Stream.map(() => undefined),
        ),
        checkEvents.pipe(
          Stream.filterEffect((handoffId) =>
            sql`SELECT 1 FROM main.agent_control_verification_handoff_intents
        WHERE project_id = ${input.projectId} AND handoff_id = ${handoffId}`.pipe(
              Effect.map((rows) => rows.length > 0),
              Effect.orDie,
            ),
          ),
          Stream.map(() => undefined),
        ),
      ],
      { concurrency: "unbounded" },
    );
    return Stream.concat(
      Stream.fromEffect(getSnapshot(input)),
      updates.pipe(Stream.mapEffect(() => getSnapshot(input))),
    ).pipe(Stream.changesWith((left, right) => encodeSnapshot(left) === encodeSnapshot(right)));
  });
  return { getSnapshot, subscribe };
});
