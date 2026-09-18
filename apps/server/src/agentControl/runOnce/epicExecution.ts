import {
  AgentControlRunOnceId,
  CommandId,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentControlControlledThreadActivation } from "../controlledThreadReservation/Services/AgentControlControlledThreadActivation.ts";
import {
  epicDigest,
  epicError,
  loadProjectEpics,
  loadTaskEpic,
  saveEpicRun,
} from "../epic/authority.ts";
import { AgentControlStageRun } from "../stageRun/Services/AgentControlStageRun.ts";
import { deriveAgentControlStageRunLeaseId } from "../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlWorktreeController } from "../worktree/Services/AgentControlWorktreeController.ts";
import { AgentControlImplementationTurnCoordinator } from "../implementationTurn/Services/AgentControlImplementationTurnCoordinator.ts";
import { AgentControlVerificationTurnCoordinator } from "../verificationTurn/Services/AgentControlVerificationTurnCoordinator.ts";
import {
  epicPreparationBlockerCode,
  persistImplementationEpicDiagnostic,
  persistVerificationRunOnceDiagnostic,
} from "./diagnostics.ts";
import { AgentControlRunOnceReadNotifications } from "./readNotifications.ts";
import {
  makeAgentControlRunOnceKeyedFence,
  withAgentControlRunOnceProjectFence,
} from "./context.ts";

/** Epic starts reuse the stage controllers and their durable command receipts. The
 * project stays Armed; Run Once's single activation authority is left intact. */
export const makeEpicTaskExecution = Effect.fn("makeEpicTaskExecution")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const stages = yield* AgentControlStageRun;
  const leases = yield* AgentControlStageRunLeaseEngine;
  const worktrees = yield* AgentControlWorktreeController;
  const threads = yield* AgentControlControlledThreadActivation;
  const implementation = yield* AgentControlImplementationTurnCoordinator;
  const verification = yield* AgentControlVerificationTurnCoordinator;
  const locks = makeAgentControlRunOnceKeyedFence<string>();

  const current = Effect.fn("EpicTaskExecution.current")(function* (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) {
    const epic = yield* loadTaskEpic(sql, projectId, taskId);
    const member = epic?.members.find((item) => item.taskId === taskId);
    const rows = yield* sql<{ mode: string; revision: number; pausedFromMode: string | null }>`
      SELECT mode, revision, paused_from_mode AS "pausedFromMode"
      FROM agent_control_project_states WHERE project_id=${projectId}`;
    const project = rows[0];
    if (
      !epic?.dependencyPlan ||
      !epic.dependencyPlanDigest ||
      epic.status !== "running" ||
      !member ||
      member.status !== "running" ||
      !member.baseCommitSha ||
      project?.mode !== "armed" ||
      project.pausedFromMode !== null
    )
      return null;
    return { epic, member, project };
  });

  const publishActivation = Effect.fn("EpicTaskExecution.publishActivation")(function* (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
    reservationId: string,
  ) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const state = yield* current(projectId, taskId);
        if (
          !state ||
          (state.member.reservationId === reservationId &&
            !state.member.waitReason &&
            !state.member.blocker)
        )
          return;
        yield* saveEpicRun(sql, state.epic, {
          members: state.epic.members.map((item) => {
            if (item.taskId !== taskId) return item;
            const { waitReason: _wait, blocker: _blocker, ...active } = item;
            return { ...active, reservationId };
          }),
        });
      }),
    );
  });

  const processTask = Effect.fn("EpicTaskExecution.processTask")(function* (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) {
    const binding = yield* withAgentControlRunOnceProjectFence(
      projectId,
      sql.withTransaction(
        Effect.gen(function* () {
          const state = yield* current(projectId, taskId);
          if (!state) return null;
          const { epic, member, project } = state;
          const executionId = AgentControlRunOnceId.make(
            `epic-task-${epicDigest({ epicRunId: epic.epicRunId, taskId, plan: epic.dependencyPlanDigest })}`,
          );
          if (member.childRunId !== null && member.childRunId !== executionId)
            return yield* epicError(
              "authority-conflict",
              "Epic task execution has a different owner.",
            );
          const now = DateTime.formatIso(yield* DateTime.now);
          yield* sql`INSERT INTO agent_control_epic_task_executions (
        execution_id,epic_run_id,project_id,task_id,plan_digest,base_commit_sha,project_revision,phase,created_at,updated_at
      ) VALUES (${executionId},${epic.epicRunId},${projectId},${taskId},${epic.dependencyPlanDigest!},${member.baseCommitSha!},${project.revision},'reserved',${now},${now})
      ON CONFLICT(execution_id) DO NOTHING`;
          const rows = yield* sql<{
            phase: string;
            planDigest: string;
            baseCommitSha: string;
            worktreeReservationId: string | null;
          }>`
        SELECT phase,plan_digest AS "planDigest",base_commit_sha AS "baseCommitSha",worktree_reservation_id AS "worktreeReservationId"
        FROM agent_control_epic_task_executions WHERE execution_id=${executionId} AND project_id=${projectId} AND task_id=${taskId}`;
          const row = rows[0];
          if (
            rows.length !== 1 ||
            !row ||
            row.planDigest !== epic.dependencyPlanDigest ||
            row.baseCommitSha !== member.baseCommitSha
          )
            return yield* epicError(
              "authority-conflict",
              "Epic task execution plan or base changed.",
            );
          if (member.childRunId === null)
            yield* saveEpicRun(sql, epic, {
              members: epic.members.map((item) =>
                item === member ? { ...item, childRunId: executionId } : item,
              ),
            });
          return { executionId, ...row };
        }),
      ),
    );
    if (!binding) return;
    if (binding.phase === "thread-activated") {
      if (binding.worktreeReservationId)
        yield* publishActivation(projectId, taskId, binding.worktreeReservationId);
      // Intake completion and Epic resume already wake this scheduler. Retry only
      // this member's unfinished transitions through the receipt-first coordinators.
      const planningResults = yield* sql<{ handoffId: string }>`
        SELECT result.handoff_id AS "handoffId" FROM agent_control_initial_planning_result_evidence result
        JOIN agent_control_initial_planning_handoff_intents intent ON intent.handoff_id=result.handoff_id
        WHERE result.project_id=${projectId} AND result.task_id=${taskId}
          AND intent.worktree_reservation_id=${binding.worktreeReservationId}
          AND result.outcome='succeeded'
          AND NOT EXISTS (SELECT 1 FROM agent_control_implementation_materialization_evidence materialized
            JOIN agent_control_implementation_materialization_markers marker
              ON marker.materialization_evidence_id=materialized.materialization_evidence_id
            WHERE materialized.admission_handoff_id=result.handoff_id)`;
      for (const result of planningResults) {
        if (!(yield* current(projectId, taskId))) return;
        yield* implementation
          .processHandoff(result.handoffId)
          .pipe(
            Effect.catch((failure) =>
              persistImplementationEpicDiagnostic(sql, result.handoffId, failure),
            ),
          );
      }
      const implementationResults = yield* sql<{ resultEvidenceId: string }>`
        SELECT result.result_evidence_id AS "resultEvidenceId" FROM agent_control_implementation_result_evidence result
        WHERE result.project_id=${projectId} AND result.task_id=${taskId}
          AND result.worktree_reservation_id=${binding.worktreeReservationId}
          AND result.outcome='succeeded'
          AND NOT EXISTS (SELECT 1 FROM agent_control_verification_materialization_evidence materialized
            JOIN agent_control_verification_materialization_markers marker
              ON marker.materialization_evidence_id=materialized.materialization_evidence_id
            WHERE materialized.implementation_result_evidence_id=result.result_evidence_id)`;
      for (const result of implementationResults) {
        if (!(yield* current(projectId, taskId))) return;
        yield* verification
          .processHandoff(result.resultEvidenceId)
          .pipe(
            Effect.catch((failure) =>
              persistVerificationRunOnceDiagnostic(sql, result.resultEvidenceId, failure),
            ),
          );
      }
      return;
    }
    const command = (step: string) =>
      CommandId.make(`epic-task-${epicDigest({ executionId: binding.executionId, step })}`);
    // Each call is receipt-first. Restarting between an external side effect and
    // this progress marker therefore adopts the original result and never starts
    // another thread, worktree, lease or provider request.
    if (!(yield* current(projectId, taskId))) return;
    const stage = (yield* stages.prepareInitial({ commandId: command("stage"), projectId, taskId }))
      .state;
    yield* sql`UPDATE agent_control_epic_task_executions SET stage_run_id=${stage.stageRunId},phase='stage-prepared'
      WHERE execution_id=${binding.executionId} AND phase='reserved'`;
    if (!(yield* current(projectId, taskId))) return;
    const leaseId = yield* deriveAgentControlStageRunLeaseId({ projectId, taskId });
    const lease = yield* leases.dispatchController({
      type: "agentControl.stageRunLease.reserve",
      commandId: command("lease"),
      leaseId,
      projectId,
      taskId,
      stageRunId: stage.stageRunId,
      attemptId: stage.attemptId,
      taskRevision: stage.taskRevision,
      githubIntakeSequence: stage.githubIntakeSequence,
      sourceIdentityFingerprint: stage.sourceIdentityFingerprint,
      fenceToken: 1,
      expectedRevision: 0,
      leaseDurationMs: 60_000,
    });
    if (lease._tag === "Rejected") return yield* lease.error;
    yield* sql`UPDATE agent_control_epic_task_executions SET lease_id=${leaseId},phase='lease-reserved'
      WHERE execution_id=${binding.executionId} AND phase='stage-prepared'`;
    if (!(yield* current(projectId, taskId))) return;
    const worktree = yield* worktrees.reserveAndMaterialize({
      commandId: command("worktree"),
      projectId,
      taskId,
    });
    yield* sql`UPDATE agent_control_epic_task_executions SET worktree_reservation_id=${worktree.reservationId},phase='worktree-ready'
      WHERE execution_id=${binding.executionId} AND phase='lease-reserved'`;
    if (!(yield* current(projectId, taskId))) return;
    const thread = yield* threads.activateInitial({
      commandId: command("thread"),
      projectId,
      taskId,
    });
    yield* sql`UPDATE agent_control_epic_task_executions SET controlled_thread_reservation_id=${thread.reservation.controlledThreadReservationId},
      thread_id=${thread.reservation.threadId},phase='thread-activated',updated_at=${DateTime.formatIso(yield* DateTime.now)}
      WHERE execution_id=${binding.executionId} AND phase='worktree-ready'`;
    yield* publishActivation(projectId, taskId, worktree.reservationId);
  });

  return Effect.fn("EpicTaskExecution.processProject")(function* (projectId: ProjectId) {
    const epics = (yield* loadProjectEpics(sql, projectId)).filter(
      (epic) => epic.dependencyPlan && epic.status === "running",
    );
    const groups = epics.map((epic) =>
      epic.members
        .filter((member) => member.status === "running" && member.taskId !== null)
        .sort(
          (a, b) => a.issueNumber - b.issueNumber || a.issueNodeId.localeCompare(b.issueNodeId),
        ),
    );
    // Interleave starts across Epics before returning to another member of a large Epic.
    const members = Array.from({
      length: Math.max(0, ...groups.map((group) => group.length)),
    }).flatMap((_, index) => groups.flatMap((group) => (group[index] ? [group[index]!] : [])));
    // Starting one task only enqueues its first turn. It does not wait for a
    // provider terminal event, so later members execute concurrently under Admission.
    yield* Effect.forEach(
      members,
      (member) =>
        locks.withPermit(member.taskId!, processTask(projectId, member.taskId!)).pipe(
          Effect.catch((failure) =>
            Effect.gen(function* () {
              yield* Effect.logWarning("Epic task start blocked", {
                projectId,
                taskId: member.taskId,
                failure,
              });
              // Keep the durable binding: uncertainty is never permission to reassign
              // the task or free Admission's provider/local capacity.
              yield* sql.withTransaction(
                Effect.gen(function* () {
                  const state = yield* current(projectId, member.taskId!);
                  if (!state) return;
                  const code = epicPreparationBlockerCode(failure);
                  const message =
                    code === null
                      ? String(failure)
                      : `Task #${member.issueNumber} could not prepare its execution (${code}). Inspect its source approval, lease and worktree ownership; resolve the cause and resume, or stop this Epic. Existing execution evidence is retained.`;
                  const blockers = [
                    ...state.epic.blockers,
                    {
                      code: `preparation:${code}`,
                      issueNumber: member.issueNumber,
                      message,
                    },
                  ];
                  yield* saveEpicRun(sql, state.epic, {
                    ...(code === null
                      ? {}
                      : {
                          status: "blocked" as const,
                          blockers,
                          blockerHistory: [
                            ...state.epic.blockerHistory,
                            {
                              recordedAt: DateTime.formatIso(yield* DateTime.now),
                              blockers,
                            },
                          ],
                        }),
                    members: state.epic.members.map((item) =>
                      item.taskId === member.taskId
                        ? { ...item, waitReason: "blocker" as const, blocker: message }
                        : item,
                    ),
                  });
                }),
              );
              const notifications = yield* AgentControlRunOnceReadNotifications;
              yield* notifications.publishProject(projectId);
            }),
          ),
        ),
      { discard: true },
    );
  });
});
