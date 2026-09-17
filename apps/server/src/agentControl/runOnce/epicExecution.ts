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
import { epicDigest, epicError, loadSelectedEpic, saveEpicRun } from "../epic/authority.ts";
import { AgentControlStageRun } from "../stageRun/Services/AgentControlStageRun.ts";
import { deriveAgentControlStageRunLeaseId } from "../stageRunLease/identity.ts";
import { AgentControlStageRunLeaseEngine } from "../stageRunLease/Services/AgentControlStageRunLeaseEngine.ts";
import { AgentControlWorktreeController } from "../worktree/Services/AgentControlWorktreeController.ts";
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
  const locks = makeAgentControlRunOnceKeyedFence<string>();

  const current = Effect.fn("EpicTaskExecution.current")(function* (
    projectId: ProjectId,
    taskId: AgentControlTaskId,
  ) {
    const epic = yield* loadSelectedEpic(sql, projectId);
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
    const epic = yield* loadSelectedEpic(sql, projectId);
    if (!epic?.dependencyPlan) return;
    const members = epic.members
      .filter((member) => member.status === "running" && member.taskId !== null)
      .sort((a, b) => a.issueNumber - b.issueNumber || a.issueNodeId.localeCompare(b.issueNodeId));
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
                  yield* saveEpicRun(sql, state.epic, {
                    members: state.epic.members.map((item) =>
                      item.taskId === member.taskId
                        ? { ...item, waitReason: "blocker" as const, blocker: String(failure) }
                        : item,
                    ),
                  });
                }),
              );
            }),
          ),
        ),
      { discard: true },
    );
  });
});
