import {
  AgentControlStageRunPreparedPayload,
  type AgentControlTaskState,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { epicDigest, loadSelectedEpic } from "../epic/authority.ts";
import { epicIssueContentFingerprint } from "../github/githubEpicSource.ts";
import {
  deriveAgentControlAttemptId,
  deriveAgentControlStageRunId,
  fingerprintAgentControlSourceIdentity,
} from "../stageRun/identity.ts";
import { loadAgentControlVerificationTaskAuthorityInTransaction } from "../verificationTurn/historicalAuthority.ts";
import { AgentControlTaskConsumerGuardError } from "./Services/AgentControlTaskConsumerGuard.ts";

const decodePrepared = Schema.decodeUnknownEffect(
  Schema.fromJsonString(AgentControlStageRunPreparedPayload),
);

/** The caller validates the latest complete task history and source gate first.
 * An Epic execution then keeps the task revision accepted by its initial stage;
 * later intake observations authorize continuation, never replace that evidence. */
export const loadEpicTaskExecutionAuthority = Effect.fn("loadEpicTaskExecutionAuthority")(
  function* (sql: SqlClient.SqlClient, task: AgentControlTaskState) {
    const projectId = task.source.projectId;
    const reject = (reason: AgentControlTaskConsumerGuardError["reason"]) =>
      new AgentControlTaskConsumerGuardError({ projectId, reason });
    const epic = yield* loadSelectedEpic(sql, projectId).pipe(
      Effect.mapError(() => reject("task-projection-corrupt")),
    );
    const installed = yield* sql`SELECT 1 FROM sqlite_schema
      WHERE type='table' AND name='agent_control_epic_task_executions'`;
    const bindings =
      installed.length === 0
        ? []
        : yield* sql<{
            executionId: string;
            epicRunId: string;
            planDigest: string;
            stageRunId: string | null;
            baseCommitSha: string;
          }>`SELECT execution_id AS "executionId",epic_run_id AS "epicRunId",
      plan_digest AS "planDigest",stage_run_id AS "stageRunId",base_commit_sha AS "baseCommitSha"
      FROM agent_control_epic_task_executions
      WHERE project_id=${projectId} AND task_id=${task.taskId}`;
    if (!epic?.dependencyPlan && bindings.length === 0) return task;
    const member = epic?.members.find((entry) => entry.taskId === task.taskId);
    const binding = bindings[0];
    const projects = yield* sql<{ mode: string; pausedFromMode: string | null }>`
      SELECT mode,paused_from_mode AS "pausedFromMode" FROM agent_control_project_states
      WHERE project_id=${projectId}`;
    if (
      !epic?.dependencyPlan ||
      epic.status !== "running" ||
      projects.length !== 1 ||
      projects[0]?.mode !== "armed" ||
      projects[0].pausedFromMode !== null ||
      !member ||
      member.status !== "running" ||
      !member.childRunId ||
      bindings.length !== 1 ||
      !binding ||
      binding.executionId !== member.childRunId ||
      binding.epicRunId !== epic.epicRunId ||
      binding.planDigest !== epic.dependencyPlanDigest ||
      binding.planDigest !== epicDigest(epic.dependencyPlan) ||
      binding.baseCommitSha !== member.baseCommitSha
    )
      return yield* reject("task-status-inactive");
    const frozen = epic.source.tasks.find(
      (entry) => entry.issue.issueNodeId === task.source.issueNodeId,
    )?.issue;
    if (
      !frozen ||
      frozen.repositoryNodeId !== task.source.repositoryNodeId ||
      frozen.number !== task.source.issueNumber ||
      (frozen.contentFingerprint !== undefined &&
        frozen.contentFingerprint !== epicIssueContentFingerprint(task.sourceSnapshot))
    )
      return yield* reject("task-source-mismatch");
    if (
      task.sourceGate !== "eligible" ||
      task.sourceSnapshot.state !== "open" ||
      !task.sourceSnapshot.ready ||
      task.sourceSnapshot.paused ||
      !task.sourceSnapshot.eligible ||
      !task.sourceSnapshot.timelineComplete
    )
      return yield* reject("task-source-ineligible");
    if (binding.stageRunId === null) return task;

    const commandId = `epic-task-${epicDigest({ executionId: binding.executionId, step: "stage" })}`;
    const stages = yield* sql<{ payload: string }>`
      SELECT event.payload_json AS payload FROM agent_control_events event
      JOIN agent_control_command_receipts receipt ON receipt.command_id=event.command_id
      WHERE event.aggregate_kind='stage-run' AND event.stream_id=${binding.stageRunId}
        AND event.stream_version=1 AND event.event_type='agentControl.stageRun.prepared'
        AND event.actor_authority='controller' AND event.command_id=${commandId}
        AND event.correlation_id=event.command_id AND event.causation_event_id IS NULL
        AND receipt.status='accepted' AND receipt.authority='controller'
        AND receipt.aggregate_kind='stage-run' AND receipt.aggregate_id=event.stream_id
        AND receipt.result_sequence=event.sequence AND receipt.result_stream_version=1
        AND receipt.event_created=1`;
    if (stages.length !== 1) return yield* reject("task-projection-corrupt");
    const stage = yield* decodePrepared(stages[0]!.payload).pipe(
      Effect.mapError(() => reject("task-projection-corrupt")),
    );
    if (
      stage.projectId !== projectId ||
      stage.taskId !== task.taskId ||
      stage.stageRunId !== binding.stageRunId ||
      stage.stageKind !== "planning" ||
      stage.roleId !== "planning" ||
      stage.stageOrdinal !== 1 ||
      stage.attemptOrdinal !== 1 ||
      stage.taskRevision > task.revision ||
      stage.githubIntakeSequence > task.githubIntakeSequence ||
      stage.sourceIdentityFingerprint !== fingerprintAgentControlSourceIdentity(task.source) ||
      stage.stageRunId !== (yield* deriveAgentControlStageRunId(stage)) ||
      stage.attemptId !== (yield* deriveAgentControlAttemptId(stage.stageRunId, 1))
    )
      return yield* reject("task-projection-corrupt");
    const historical = yield* loadAgentControlVerificationTaskAuthorityInTransaction(
      sql,
      task.taskId,
      stage.taskRevision,
    ).pipe(Effect.mapError(() => reject("task-projection-corrupt")));
    if (
      historical.state.source.projectId !== projectId ||
      historical.state.status !== "candidate" ||
      historical.state.stage !== "intake" ||
      historical.state.sourceGate !== "eligible" ||
      historical.state.githubIntakeSequence !== stage.githubIntakeSequence ||
      fingerprintAgentControlSourceIdentity(historical.state.source) !==
        stage.sourceIdentityFingerprint ||
      epicIssueContentFingerprint(historical.state.sourceSnapshot) !==
        epicIssueContentFingerprint(task.sourceSnapshot)
    )
      return yield* reject("task-source-mismatch");
    return historical.state;
  },
  (effect, _sql, task) =>
    effect.pipe(
      Effect.mapError((cause) =>
        cause._tag === "AgentControlTaskConsumerGuardError"
          ? cause
          : new AgentControlTaskConsumerGuardError({
              projectId: task.source.projectId,
              reason: "internal-persistence-error",
            }),
      ),
    ),
);
