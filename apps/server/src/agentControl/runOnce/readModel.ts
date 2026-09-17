import { unsettledEpicExecutions } from "../epic/executionState.ts";
import { inspectionProgress } from "../verificationTurn/inspectionPages.ts";
import { VERIFICATION_INSPECTION_DISPLAY } from "../../provider/VerificationInspection.ts";
import { verificationInspectionBase } from "../verificationTurn/checkEvidence.ts";
import { loadEpicQueue } from "../epic/queueAuthority.ts";
import { loadEpicRun, loadSelectedEpic } from "../epic/authority.ts";
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
import {
  isAgentControlRunOnceCandidateVacant,
  selectAgentControlRunOnceCandidate,
} from "./selection.ts";
import { deriveRunOnceCommandId } from "./identity.ts";

const encodeEpicRunIds = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));
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

type VerificationCheckStatus =
  | "passed"
  | "failed"
  | "unavailable"
  | "stale"
  | "missing"
  | "running";

export function resolveVerificationCheckStatus(input: {
  readonly progressStatus?: VerificationCheckStatus;
  readonly evidenceStatus?: VerificationCheckStatus | null;
  readonly hasStartEvidence: boolean;
  readonly stageStatus: (typeof AgentControlStageRunState.Type)["status"];
  readonly admissionWaiting: boolean;
}): VerificationCheckStatus {
  return (
    input.progressStatus ??
    input.evidenceStatus ??
    (!input.admissionWaiting &&
    input.hasStartEvidence &&
    (input.stageStatus === "running" ||
      input.stageStatus === "queued" ||
      input.stageStatus === "prepared")
      ? "running"
      : "missing")
  );
}

/** Reads durable run evidence; the selected Epic includes each of its task executions. */
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
          // The committed system mode event carries Armed authority before the
          // scheduler publishes its activation or Run-Once creates a run row.
          // Pause/resume keeps the latest original activation's origin; takeover
          // to Observe/Manual wins immediately, even before run cleanup.
          const activationModes =
            projectState.mode === "run-once"
              ? yield* sql<{ originMode: string }>`
                  SELECT json_extract(event.payload_json, '$.previousMode') AS "originMode"
                  FROM main.agent_control_events event
                  JOIN main.agent_control_command_receipts receipt
                    ON receipt.command_id = event.command_id
                  WHERE event.aggregate_kind = 'project-controller'
                    AND event.stream_id = ${input.projectId}
                    AND event.stream_version <= ${projectState.revision}
                    AND event.event_type = 'agentControl.project.mode.changed'
                    AND json_extract(event.payload_json, '$.mode') = 'run-once'
                    AND ((event.actor_authority = 'system'
                      AND json_extract(event.payload_json, '$.previousMode') = 'armed')
                      OR (event.actor_authority = 'human'
                      AND json_extract(event.payload_json, '$.previousMode') = 'observe'))
                    AND receipt.status = 'accepted' AND receipt.event_created = 1
                    AND receipt.authority = event.actor_authority
                    AND receipt.aggregate_kind = event.aggregate_kind
                    AND receipt.aggregate_id = event.stream_id
                    AND receipt.result_sequence = event.sequence
                    AND receipt.result_stream_version = event.stream_version
                  ORDER BY event.stream_version DESC LIMIT 1
                `
              : [];
          const armed = {
            enabled:
              projectState.mode === "armed" ||
              (projectState.mode === "run-once" && activationModes[0]?.originMode === "armed"),
          };
          const listed = yield* tasks.listTasks(input);
          const intakeSequence = Math.max(
            0,
            ...listed.tasks.map((task) => task.githubIntakeSequence),
          );
          const candidateTaskId =
            intakeSequence === 0
              ? null
              : yield* selectAgentControlRunOnceCandidate(sql, input.projectId, intakeSequence);
          const nextTaskId =
            candidateTaskId !== null &&
            (yield* isAgentControlRunOnceCandidateVacant(sql, input.projectId, candidateTaskId))
              ? candidateTaskId
              : null;
          const epic = yield* loadSelectedEpic(sql, input.projectId);
          const epicTables =
            yield* sql`SELECT 1 FROM sqlite_schema WHERE name='agent_control_epic_runs' AND type='table'`;
          const historicalIds = epicTables.length
            ? yield* sql<{
                epicRunId: string;
              }>`SELECT epic_run_id AS "epicRunId" FROM agent_control_epic_runs WHERE project_id=${input.projectId} AND (${epic?.epicRunId ?? null} IS NULL OR epic_run_id != ${epic?.epicRunId ?? null}) ORDER BY rowid DESC LIMIT 5`
            : [];
          const epicHistory = (yield* Effect.forEach(historicalIds, (row) =>
            loadEpicRun(sql, row.epicRunId),
          )).filter((run) => run !== null);
          const epicRunIds =
            epic?.members.flatMap((member) => (member.childRunId ? [member.childRunId] : [])) ?? [];

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
        AND (${input.runId !== undefined || epicRunIds.length === 0 ? 1 : 0} OR status = 'active'
          OR run_id = (SELECT latest.run_id FROM main.agent_control_run_once_states latest WHERE latest.project_id=${input.projectId} ORDER BY latest.activation_project_revision DESC LIMIT 1)
          OR run_id IN (
          SELECT value FROM json_each(${encodeEpicRunIds(epicRunIds)})
        ))
        ORDER BY (status='active') DESC,
          (run_id IN (SELECT value FROM json_each(${encodeEpicRunIds(epic?.members.flatMap((member) => (member.childRunId ? [member.childRunId] : [])) ?? [])}))) DESC,
          activation_project_revision DESC LIMIT ${input.runId !== undefined || epicRunIds.length === 0 ? 1 : 100}
      `;
          const executionTables = yield* sql`SELECT 1 FROM sqlite_schema
            WHERE name='agent_control_epic_task_executions' AND type='table'`;
          // Reuse stage, check, and Admission projection below. Execution bindings are
          // durable authority; a synthetic Run Once activation must never be written.
          const epicExecutions =
            executionTables.length === 0
              ? []
              : yield* sql<{
                  runId: string;
                  status: "active" | "completed";
                  executionBlocker: string | null;
                }>`
            SELECT 1 AS "schemaVersion", execution.execution_id AS "runId",
              execution.project_id AS "projectId",
              CASE WHEN json_extract(member.value,'$.status') IN ('accepted','failed')
                OR json_extract(epic.state_json,'$.status')='stopped' THEN 'completed' ELSE 'active' END AS status,
              CASE execution.phase WHEN 'reserved' THEN 2 WHEN 'stage-prepared' THEN 3
                WHEN 'lease-reserved' THEN 4 WHEN 'worktree-ready' THEN 5 ELSE 6 END AS "nextOrdinal",
              CASE WHEN json_extract(member.value,'$.status') IN ('accepted','failed')
                OR json_extract(epic.state_json,'$.status')='stopped' THEN 'completed'
                WHEN execution.phase='reserved' THEN 'task-selected' ELSE execution.phase END AS "lastStep",
              execution.task_id AS "taskId", execution.stage_run_id AS "stageRunId",
              execution.lease_id AS "leaseId", execution.worktree_reservation_id AS "worktreeReservationId",
              execution.controlled_thread_reservation_id AS "controlledThreadReservationId",
              (SELECT evidence.task_event_id FROM agent_control_task_verification_finalization_evidence evidence
                WHERE evidence.task_finalization_evidence_id=json_extract(member.value,'$.taskFinalizationEvidenceId')
                  AND evidence.task_id=execution.task_id AND evidence.project_id=execution.project_id) AS "terminalTaskEventId",
              execution.project_revision AS "activationProjectRevision", NULL AS "resetProjectRevision",
              json_extract(epic.state_json,'$.updatedAt') AS "updatedAt",
              json_extract(member.value,'$.blocker') AS "executionBlocker"
            FROM agent_control_epic_task_executions execution
            JOIN agent_control_epic_runs epic ON epic.epic_run_id=execution.epic_run_id AND epic.project_id=execution.project_id
            JOIN json_each(epic.state_json,'$.members') member
              ON json_extract(member.value,'$.taskId')=execution.task_id
              AND json_extract(member.value,'$.childRunId')=execution.execution_id
            WHERE execution.project_id=${input.projectId}
              AND json_extract(epic.state_json,'$.dependencyPlanDigest')=execution.plan_digest
              AND ((${input.runId ?? null} IS NOT NULL AND execution.execution_id=${input.runId ?? null})
                OR (${input.runId ?? null} IS NULL AND execution.epic_run_id=${epic?.epicRunId ?? null}))
            ORDER BY (status='active') DESC, execution.created_at, execution.task_id
          `;
          const unsettledExecutions = yield* unsettledEpicExecutions(sql, input.projectId);
          const diagnostics = yield* sql<{ runId: string | null; errorCode: string }>`
        SELECT run_id AS "runId", error_code AS "errorCode" FROM main.agent_control_run_once_diagnostics
        WHERE project_id = ${input.projectId}
      `;
          const runs = [];
          for (const row of [...epicExecutions, ...rows]) {
            const execution = epicExecutions.find((item) => item.runId === row.runId);
            const state = yield* decodeRun(
              execution
                ? {
                    ...row,
                    status: unsettledExecutions.has(execution.runId)
                      ? "active"
                      : !armed.enabled
                        ? "completed"
                        : row.status,
                  }
                : row,
            );
            const activations = yield* sql<{ originMode: string }>`
              SELECT origin_mode AS "originMode" FROM main.agent_control_run_once_activations
              WHERE project_id = ${input.projectId} AND run_id = ${state.runId}
            `;
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
                providerInstanceId: string;
                model: string;
              }>(
                `SELECT intent.handoff_id AS "handoffId", intent.thread_id AS "threadId",
            intent.worktree_path AS "worktreePath", intent.provider_delivery_id AS "providerDeliveryId",
            delivery.last_error_code AS "errorCode", json_extract(worktree.state_json, '$.branchName') AS branch,
            intent.provider_instance_id AS "providerInstanceId",
            json_extract(intent.model_selection_json, '$.model') AS model
            FROM main.agent_control_${prefix}_handoff_intents intent
            LEFT JOIN main.agent_control_${prefix}_deliveries delivery ON delivery.handoff_id = intent.handoff_id
            LEFT JOIN main.agent_control_worktree_reservation_states worktree ON worktree.reservation_id = intent.worktree_reservation_id
            WHERE intent.project_id = ? AND intent.task_id = ? AND intent.stage_run_id = ?`,
                [input.projectId, state.taskId, stage.stageRunId],
              ).unprepared;
              const handoff = handoffs[0];
              const admissionWait = handoff
                ? (yield* sql<{
                    reason:
                      | "provider-limit"
                      | "local-capacity"
                      | "cpu-pressure"
                      | "ram-pressure"
                      | "gpu-pressure"
                      | "interactive-priority"
                      | "telemetry-unavailable"
                      | "unsupported-requirement";
                    detail: string | null;
                    observedAt: string;
                  }>`
                      SELECT reason,detail,updated_at AS "observedAt"
                      FROM main.resource_admission_wait_status
                      WHERE handoff_id=${handoff.handoffId}
                      UNION ALL
                      SELECT CASE WHEN wait_reason='interactive-priority'
                        THEN 'interactive-priority' ELSE 'provider-limit' END AS reason,
                        CASE WHEN wait_reason='provider-usage'
                          THEN 'Provider usage limits are not ready.' ELSE NULL END AS detail,
                        updated_at AS "observedAt"
                      FROM main.resource_admission_provider_requests
                      WHERE handoff_id=${handoff.handoffId} AND status='waiting'
                      ORDER BY "observedAt" DESC LIMIT 1
                    `)[0]
                : undefined;
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
                const manifests = yield* sql<{
                  checksJson: string;
                  manifestDigest: string;
                  codeDigest: string;
                }>`
              SELECT checks_json AS "checksJson", manifest_digest AS "manifestDigest", code_digest AS "codeDigest"
              FROM main.agent_control_verification_check_manifests WHERE provider_delivery_id = ${handoff.providerDeliveryId}
                AND handoff_id = ${handoff.handoffId}
            `;
                const checks = manifests[0] ? yield* decodeChecks(manifests[0].checksJson) : [];
                const checkViews = [];
                const displayedChecks = [
                  ...checks,
                  ...(manifests[0] && verificationInspectionBase(manifests[0].codeDigest)
                    ? [VERIFICATION_INSPECTION_DISPLAY]
                    : []),
                ];
                for (const check of displayedChecks) {
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
                  const progress =
                    check.id === "git-diff" &&
                    evidence[0]?.status === "passed" &&
                    evidence[0]?.resultJson
                      ? yield* inspectionProgress(
                          sql,
                          handoff.providerDeliveryId,
                          manifests[0]!.manifestDigest,
                          manifests[0]!.codeDigest,
                          evidence[0].resultJson,
                        )
                      : null;
                  checkViews.push({
                    id: check.id,
                    command: check.command,
                    args: check.args,
                    cwd: check.cwd,
                    required: check.required,
                    status: resolveVerificationCheckStatus({
                      ...(progress?.status === undefined
                        ? {}
                        : { progressStatus: progress.status }),
                      ...(evidence[0]?.status === undefined
                        ? {}
                        : { evidenceStatus: evidence[0].status }),
                      hasStartEvidence: evidence.length > 0,
                      stageStatus: stage.status,
                      admissionWaiting: admissionWait !== undefined,
                    }),
                    exitCode: result?.exitCode ?? null,
                    output: result
                      ? `${progress ? progress.detail + "\n" : ""}${result.stdout}\n${result.stderr}`.slice(
                          0,
                          8192,
                        )
                      : null,
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
                providerInstanceId: handoff?.providerInstanceId ?? null,
                model: handoff?.model ?? null,
                errorCode: handoff?.errorCode ?? null,
                ...(admissionWait === undefined
                  ? {}
                  : {
                      admissionWait: {
                        reason: admissionWait.reason,
                        observedAt: admissionWait.observedAt,
                        ...(admissionWait.detail === null ? {} : { detail: admissionWait.detail }),
                      },
                    }),
                verification,
              });
            }
            let errorCode =
              execution?.executionBlocker ??
              diagnostics.find((item) => item.runId === state.runId)?.errorCode ??
              null;
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
              originMode: execution ? "armed" : (activations[0]?.originMode ?? null),
              errorCode,
              state,
              task: listed.tasks.find((task) => task.taskId === state.taskId) ?? null,
              stages,
            });
          }
          return yield* decodeSnapshot({
            epic,
            epicHistory,
            epicQueue: yield* loadEpicQueue(sql, input.projectId),
            armed,
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
