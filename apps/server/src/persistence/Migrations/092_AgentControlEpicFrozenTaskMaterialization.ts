import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";

const trigger = "agent_control_implementation_materialization_evidence_validate";
const previousFingerprint = "cde9153265dd9e3ef1c0df8957376487a7364bc5dcfa5ad9dc910cc0f1ecd612";

// Verification already binds its immutable admission/result evidence. The older
// thread and Implementation triggers also equated that evidence with the latest
// intake projection. Keep historical checks and validate eligibility separately.
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ source: string }>`SELECT sql AS source FROM sqlite_schema
    WHERE type='trigger' AND name=${trigger}`;
  const source = rows[0]?.source;
  if (source === undefined || sha256Utf8(source) !== previousFingerprint)
    return yield* Effect.die(new Error("Epic materialization found divergent authority trigger"));

  const projectionStart = source.indexOf("AND typeof(task_projection.state_json)");
  const projectionEnd = source.indexOf(
    "        AND (\n          SELECT count(*)\n          FROM agent_control_events task_history",
    projectionStart,
  );
  if (projectionStart < 0 || projectionEnd < 0)
    return yield* Effect.die(new Error("Epic materialization task projection guard missing"));
  const projection = source
    .slice(projectionStart, projectionEnd)
    .replaceAll("task_event.", "current_task_event.")
    .replaceAll("NEW.task_revision", "task_projection.revision")
    .replaceAll("NEW.task_source_event_sequence", "task_projection.last_event_sequence")
    .replaceAll("NEW.github_intake_sequence", "task_projection.github_intake_sequence");
  let updated = source.slice(0, projectionStart) + projection + source.slice(projectionEnd);
  updated = updated.replace(
    `AND task_projection.revision IS NEW.task_revision
       AND task_projection.last_event_sequence IS NEW.task_source_event_sequence
       AND task_projection.github_intake_sequence IS NEW.github_intake_sequence`,
    `AND task_projection.revision >= NEW.task_revision
       AND task_projection.last_event_sequence >= NEW.task_source_event_sequence
       AND task_projection.github_intake_sequence >= NEW.github_intake_sequence
      JOIN agent_control_events current_task_event
        ON current_task_event.aggregate_kind='task'
       AND current_task_event.stream_id=NEW.task_id
       AND current_task_event.stream_version=task_projection.revision
       AND current_task_event.sequence=task_projection.last_event_sequence
       AND current_task_event.actor_authority='controller'
       AND current_task_event.event_type IN ('agentControl.task.created',
         'agentControl.task.sourceGate.changed','agentControl.task.sourceMissingRecovered')
       AND current_task_event.metadata_json='{"schemaVersion":1}'
       AND json_extract(current_task_event.payload_json,'$.githubIntakeSequence')=
         task_projection.github_intake_sequence
       AND task_projection.status='candidate' AND task_projection.stage='intake'
       AND task_projection.source_gate='eligible'
       AND json_extract(current_task_event.payload_json,'$.sourceGate')='eligible'
       AND json_extract(current_task_event.payload_json,'$.source') IS
         json_extract(task_event.payload_json,'$.source')
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.title') IS NEW.task_title
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.body') IS
         json_extract(task_event.payload_json,'$.sourceSnapshot.body')
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.state')='open'
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.ready')=1
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.paused')=0
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.eligible')=1
       AND json_extract(current_task_event.payload_json,'$.sourceSnapshot.timelineComplete')=1
       AND (task_projection.revision IS NEW.task_revision OR EXISTS (
         SELECT 1 FROM agent_control_epic_task_executions execution
         JOIN agent_control_epic_targets target ON target.project_id=execution.project_id
           AND target.epic_run_id=execution.epic_run_id
         JOIN agent_control_epic_runs epic ON epic.epic_run_id=execution.epic_run_id
           AND epic.project_id=execution.project_id
         JOIN json_each(epic.state_json,'$.members') member
           ON json_extract(member.value,'$.taskId')=execution.task_id
           AND json_extract(member.value,'$.childRunId')=execution.execution_id
         JOIN agent_control_project_states project ON project.project_id=execution.project_id
         WHERE execution.project_id=NEW.project_id AND execution.task_id=NEW.task_id
           AND execution.stage_run_id=admission.planning_stage_run_id
           AND execution.worktree_reservation_id=NEW.worktree_reservation_id
           AND execution.plan_digest=json_extract(epic.state_json,'$.dependencyPlanDigest')
           AND json_extract(epic.state_json,'$.status')='running'
           AND json_extract(member.value,'$.status')='running'
           AND project.mode='armed' AND project.paused_from_mode IS NULL
       ))`,
  );
  yield* sql.unsafe(`DROP TRIGGER ${trigger}`).unprepared;
  yield* sql.unsafe(updated).unprepared;

  const threadTrigger = "agent_control_controlled_thread_coordinator_accepted_validate";
  const threadRows = yield* sql<{ source: string }>`SELECT sql AS source FROM sqlite_schema
    WHERE type='trigger' AND name=${threadTrigger}`;
  const threadSource = threadRows[0]?.source;
  if (
    threadSource === undefined ||
    sha256Utf8(threadSource) !== "f8b9ee86d1317ca478d04ce32a7739b89d5c6961caaa91f8c8171eed489ac4ad"
  )
    return yield* Effect.die(new Error("Epic materialization found divergent thread trigger"));
  const threadUpdated = threadSource.replace(
    `AND task.revision IS intent.task_revision
       AND task.github_intake_sequence IS intent.github_intake_sequence`,
    `AND task.revision >= intent.task_revision
       AND task.github_intake_sequence >= intent.github_intake_sequence
       AND ((task.revision IS intent.task_revision
         AND task.github_intake_sequence IS intent.github_intake_sequence) OR EXISTS (
         SELECT 1 FROM agent_control_epic_task_executions execution
         JOIN agent_control_epic_targets target ON target.project_id=execution.project_id
           AND target.epic_run_id=execution.epic_run_id
         JOIN agent_control_epic_runs epic ON epic.epic_run_id=execution.epic_run_id
           AND epic.project_id=execution.project_id
         JOIN json_each(epic.state_json,'$.members') member
           ON json_extract(member.value,'$.taskId')=execution.task_id
           AND json_extract(member.value,'$.childRunId')=execution.execution_id
         JOIN agent_control_project_states project ON project.project_id=execution.project_id
         JOIN agent_control_events admitted_task ON admitted_task.aggregate_kind='task'
           AND admitted_task.stream_id=intent.task_id
           AND admitted_task.stream_version=intent.task_revision
         JOIN agent_control_events current_task ON current_task.aggregate_kind='task'
           AND current_task.stream_id=intent.task_id
           AND current_task.stream_version=task.revision
           AND current_task.sequence=task.last_event_sequence
         WHERE execution.project_id=intent.project_id AND execution.task_id=intent.task_id
           AND execution.stage_run_id=intent.stage_run_id
           AND execution.worktree_reservation_id=intent.worktree_reservation_id
           AND execution.plan_digest=json_extract(epic.state_json,'$.dependencyPlanDigest')
           AND json_extract(epic.state_json,'$.status')='running'
           AND json_extract(member.value,'$.status')='running'
           AND project.mode='armed' AND project.paused_from_mode IS NULL
           AND admitted_task.actor_authority='controller'
           AND admitted_task.event_type IN ('agentControl.task.created',
             'agentControl.task.sourceGate.changed','agentControl.task.sourceMissingRecovered')
           AND admitted_task.metadata_json='{"schemaVersion":1}'
           AND json_extract(admitted_task.payload_json,'$.githubIntakeSequence')=
             intent.github_intake_sequence
           AND json_extract(admitted_task.payload_json,'$.sourceGate')='eligible'
           AND current_task.actor_authority='controller'
           AND current_task.event_type IN ('agentControl.task.created',
             'agentControl.task.sourceGate.changed','agentControl.task.sourceMissingRecovered')
           AND current_task.metadata_json='{"schemaVersion":1}'
           AND json_extract(current_task.payload_json,'$.githubIntakeSequence')=
             task.github_intake_sequence
           AND json_extract(current_task.payload_json,'$.sourceGate')='eligible'
           AND json_extract(current_task.payload_json,'$.source') IS
             json_extract(admitted_task.payload_json,'$.source')
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.title') IS
             json_extract(admitted_task.payload_json,'$.sourceSnapshot.title')
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.body') IS
             json_extract(admitted_task.payload_json,'$.sourceSnapshot.body')
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.state')='open'
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.ready')=1
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.paused')=0
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.eligible')=1
           AND json_extract(current_task.payload_json,'$.sourceSnapshot.timelineComplete')=1
           AND json_extract(task.state_json,'$.source') IS
             json_extract(current_task.payload_json,'$.source')
           AND json_extract(task.state_json,'$.sourceSnapshot') IS
             json_extract(current_task.payload_json,'$.sourceSnapshot')
       ))`,
  );
  yield* sql.unsafe(`DROP TRIGGER ${threadTrigger}`).unprepared;
  yield* sql.unsafe(threadUpdated).unprepared;
});
