import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { sha256Utf8 } from "../../agentControl/initialPlanning/eventEvidence.ts";

// Verification's admission guard originally allowed only its initial reserve.
// Renewal is a distinct, fenced transition, not another admission.
const eventRenewal = `
  NEW.event_type='agentControl.stageRunLease.renewed'
  AND typeof(NEW.event_id)='text' AND length(NEW.event_id)>0
  AND typeof(NEW.command_id)='text' AND length(NEW.command_id)>0
  AND NEW.actor_authority IN ('controller','system')
  AND NEW.causation_event_id IS NULL AND NEW.correlation_id IS NEW.command_id
  AND NEW.metadata_json='{"schemaVersion":1}'
  AND NEW.payload_json=json_object(
    'leaseId',json_extract(NEW.payload_json,'$.leaseId'),
    'stageRunId',json_extract(NEW.payload_json,'$.stageRunId'),
    'attemptId',json_extract(NEW.payload_json,'$.attemptId'),
    'holderId',json_extract(NEW.payload_json,'$.holderId'),
    'fenceToken',json_extract(NEW.payload_json,'$.fenceToken'),
    'renewedAt',NEW.occurred_at,
    'expiresAt',json_extract(NEW.payload_json,'$.expiresAt'))
  AND strftime('%Y-%m-%dT%H:%M:%fZ',NEW.occurred_at) IS NEW.occurred_at
  AND strftime('%Y-%m-%dT%H:%M:%fZ',json_extract(NEW.payload_json,'$.expiresAt'))
    IS json_extract(NEW.payload_json,'$.expiresAt')
  AND json_extract(NEW.payload_json,'$.expiresAt')>NEW.occurred_at
  AND (SELECT count(*) FROM main.agent_control_stage_run_lease_states lease
    JOIN main.agent_control_events previous ON previous.sequence=lease.last_event_sequence
      AND previous.aggregate_kind='stage-run-lease' AND previous.stream_id=lease.lease_id
      AND previous.stream_version=lease.revision
      AND previous.event_type IN ('agentControl.stageRunLease.reserved','agentControl.stageRunLease.renewed')
    JOIN main.agent_control_stage_run_states stage ON stage.stage_run_id=lease.stage_run_id
    WHERE lease.lease_id=NEW.stream_id AND lease.status='reserved'
      AND stage.stage_kind='verification' AND stage.status IN ('prepared','running','waiting')
      AND NEW.stream_version=lease.revision+1 AND NEW.occurred_at>=lease.renewed_at
      AND json_extract(NEW.payload_json,'$.leaseId')=lease.lease_id
      AND json_extract(NEW.payload_json,'$.stageRunId')=lease.stage_run_id
      AND json_extract(NEW.payload_json,'$.attemptId')=lease.attempt_id
      AND json_extract(NEW.payload_json,'$.holderId')=lease.holder_id
      AND json_extract(NEW.payload_json,'$.fenceToken')=lease.fence_token
      AND json_type(NEW.payload_json,'$.fenceToken')='integer')=1
`;
const projectionRenewal = `
  OLD.status='reserved' AND NEW.status='reserved' AND NEW.released_at IS NULL
  AND NEW.lease_id IS OLD.lease_id AND NEW.project_id IS OLD.project_id
  AND NEW.task_id IS OLD.task_id AND NEW.stage_run_id IS OLD.stage_run_id
  AND NEW.attempt_id IS OLD.attempt_id AND NEW.holder_id IS OLD.holder_id
  AND NEW.fence_token IS OLD.fence_token AND NEW.acquired_at IS OLD.acquired_at
  AND NEW.task_revision IS OLD.task_revision
  AND NEW.github_intake_sequence IS OLD.github_intake_sequence
  AND NEW.source_identity_fingerprint IS OLD.source_identity_fingerprint
  AND NEW.revision=OLD.revision+1 AND NEW.last_event_sequence>OLD.last_event_sequence
  AND (SELECT count(*) FROM main.agent_control_events event
    WHERE event.aggregate_kind='stage-run-lease' AND event.stream_id=NEW.lease_id
      AND event.sequence=NEW.last_event_sequence AND event.stream_version=NEW.revision
      AND event.event_type='agentControl.stageRunLease.renewed'
      AND json_extract(event.payload_json,'$.stageRunId')=NEW.stage_run_id
      AND json_extract(event.payload_json,'$.attemptId')=NEW.attempt_id
      AND json_extract(event.payload_json,'$.holderId')=NEW.holder_id
      AND json_extract(event.payload_json,'$.fenceToken')=NEW.fence_token
      AND json_extract(event.payload_json,'$.renewedAt')=NEW.renewed_at
      AND json_extract(event.payload_json,'$.expiresAt')=NEW.expires_at)=1
`;

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [name, fingerprint, renewal] of [
    [
      "agent_control_verification_lease_event_validate",
      "75424b82066204971f48af00b43bd8098ceced5c08d8c85b4899925026a52085",
      eventRenewal,
    ],
    [
      "agent_control_verification_lease_projection_update_validate",
      "945cae894db4f416ce00175da17c027092e2df209421d67ba31330cfb90adf08",
      projectionRenewal,
    ],
  ] as const) {
    const rows = yield* sql<{
      readonly source: string;
    }>`SELECT sql AS source FROM main.sqlite_schema WHERE type='trigger' AND name=${name}`;
    const source = rows[0]?.source;
    if (source === undefined || sha256Utf8(source) !== fingerprint) {
      return yield* Effect.die(new Error(`verification renewal found divergent guard ${name}`));
    }
    const updated = source
      .replace("AND NOT COALESCE((", `AND NOT COALESCE(((${renewal}) OR (`)
      .replace("), 0)", ")), 0)");
    yield* sql.unsafe(`DROP TRIGGER main.${name}`).unprepared;
    yield* sql.unsafe(updated).unprepared;
  }
});
