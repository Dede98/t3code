export const verificationHandoffIntentTriggerSql = (
  resultContractPredicate?: string,
  schemaQualifier: "" | "main." = "",
): string => `
    CREATE TRIGGER ${schemaQualifier}agent_control_verification_handoff_intent_validate
    BEFORE INSERT ON agent_control_verification_handoff_intents
    WHEN NOT EXISTS (
      SELECT 1
      FROM agent_control_verification_materialization_evidence materialization
      JOIN agent_control_verification_materialization_receipts materialization_receipt
        ON materialization_receipt.materialization_evidence_id =
          materialization.materialization_evidence_id
      JOIN agent_control_verification_admission_evidence admission
        ON admission.admission_evidence_id = materialization.admission_evidence_id
      JOIN agent_control_verification_admission_receipts admission_receipt
        ON admission_receipt.admission_evidence_id = admission.admission_evidence_id
       AND admission_receipt.receipt_id = materialization.admission_receipt_id
      JOIN agent_control_verification_admission_markers admission_marker
        ON admission_marker.admission_evidence_id = admission.admission_evidence_id
       AND admission_marker.receipt_id = admission_receipt.receipt_id
       AND admission_marker.marker_id = materialization.admission_marker_id
      WHERE materialization.materialization_evidence_id IS NEW.materialization_evidence_id
        AND materialization_receipt.materialization_receipt_id IS
          NEW.materialization_receipt_id
        AND materialization_receipt.materialization_fingerprint IS
          materialization.materialization_fingerprint
        AND materialization_receipt.status IS 'accepted'
        AND NEW.admission_marker_id IS materialization.admission_marker_id
        AND admission.admission_fingerprint IS materialization.admission_fingerprint
        AND admission_marker.marker_fingerprint IS
          materialization.admission_marker_fingerprint
        AND NEW.project_id IS materialization.project_id
        AND NEW.task_id IS materialization.task_id
        AND NEW.task_revision IS materialization.task_revision
        AND NEW.github_intake_sequence IS materialization.github_intake_sequence
        AND NEW.source_identity_fingerprint IS materialization.source_identity_fingerprint
        AND NEW.task_source_event_id IS materialization.task_source_event_id
        AND NEW.task_source_event_sequence IS materialization.task_source_event_sequence
        AND NEW.task_source_event_stream_version IS
          materialization.task_source_event_stream_version
        AND NEW.stage_run_id IS materialization.stage_run_id
        AND NEW.attempt_id IS materialization.attempt_id
        AND NEW.lease_id IS materialization.lease_id
        AND NEW.lease_holder_id IS materialization.lease_holder_id
        AND NEW.fence_token IS materialization.fence_token
        AND NEW.worktree_reservation_id IS materialization.worktree_reservation_id
        AND NEW.worktree_revision IS materialization.worktree_revision
        AND NEW.worktree_event_id IS materialization.worktree_event_id
        AND NEW.worktree_event_sequence IS materialization.worktree_event_sequence
        AND NEW.worktree_event_stream_version IS
          materialization.worktree_event_stream_version
        AND NEW.worktree_ownership_fingerprint IS
          materialization.worktree_ownership_fingerprint
        AND NEW.worktree_verified_at IS materialization.worktree_verified_at
        AND NEW.worktree_path IS materialization.worktree_path
        AND NEW.branch IS materialization.branch
        AND NEW.controlled_thread_reservation_id IS
          materialization.controlled_thread_reservation_id
        AND NEW.thread_id IS materialization.thread_id
        AND NEW.planning_thread_id IS materialization.planning_thread_id
        AND NEW.plan_id IS materialization.plan_id
        AND NEW.proposed_plan_digest IS materialization.proposed_plan_digest
        AND NEW.provider_instance_id IS materialization.provider_instance_id
        AND NEW.runtime_mode IS materialization.runtime_mode
        AND NEW.model_selection_json IS materialization.model_selection_json
        AND NEW.model_selection_fingerprint IS materialization.model_selection_fingerprint
        AND NEW.template_version IS 'agent-control-verification-prompt-v1'
${resultContractPredicate === undefined ? "" : `        AND (${resultContractPredicate})\n`}        AND NEW.created_at IS materialization.materialized_at
        AND length(CAST(NEW.prompt_text AS BLOB)) <= 1048576
        AND json_extract(NEW.message_event_template_json, '$.aggregateId') IS NEW.thread_id
        AND json_extract(NEW.message_event_template_json, '$.eventId') IS
          NEW.message_event_id
        AND json_extract(NEW.message_event_template_json, '$.commandId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.message_event_template_json, '$.correlationId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.message_event_template_json, '$.occurredAt') IS NEW.created_at
        AND json_extract(NEW.message_event_template_json, '$.streamVersion') IS 3
        AND json_extract(NEW.message_event_template_json, '$.type') IS 'thread.message-sent'
        AND json_extract(NEW.message_event_template_json, '$.actorKind') IS 'client'
        AND json_extract(NEW.message_event_template_json, '$.payload.threadId') IS NEW.thread_id
        AND json_extract(NEW.message_event_template_json, '$.payload.messageId') IS NEW.message_id
        AND json_extract(NEW.message_event_template_json, '$.payload.text') IS NEW.prompt_text
        AND json_extract(NEW.message_event_template_json, '$.payload.createdAt') IS NEW.created_at
        AND json_extract(NEW.turn_request_event_template_json, '$.aggregateId') IS NEW.thread_id
        AND json_extract(NEW.turn_request_event_template_json, '$.eventId') IS
          NEW.turn_request_event_id
        AND json_extract(NEW.turn_request_event_template_json, '$.commandId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.turn_request_event_template_json, '$.correlationId') IS
          NEW.turn_request_command_id
        AND json_extract(NEW.turn_request_event_template_json, '$.causationEventId') IS
          NEW.message_event_id
        AND json_extract(NEW.turn_request_event_template_json, '$.occurredAt') IS NEW.created_at
        AND json_extract(NEW.turn_request_event_template_json, '$.streamVersion') IS 4
        AND json_extract(NEW.turn_request_event_template_json, '$.type') IS
          'thread.turn-start-requested'
        AND json_extract(NEW.turn_request_event_template_json, '$.actorKind') IS 'client'
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.threadId') IS
          NEW.thread_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.messageId') IS
          NEW.message_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.runtimeMode') IS
          NEW.runtime_mode
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.interactionMode') IS
          'default'
        AND json_extract(
          NEW.turn_request_event_template_json,
          '$.payload.sourceProposedPlan.threadId'
        ) IS materialization.planning_thread_id
        AND json_extract(
          NEW.turn_request_event_template_json,
          '$.payload.sourceProposedPlan.planId'
        ) IS materialization.plan_id
        AND json_extract(NEW.turn_request_event_template_json, '$.payload.modelSelection') IS
          json(NEW.model_selection_json)
    )
    BEGIN SELECT RAISE(ABORT, 'verification handoff intent is inconsistent'); END
  `;

export const AGENT_CONTROL_VERIFICATION_HANDOFF_INTENT_TRIGGER_SCHEMA_059_SQL =
  verificationHandoffIntentTriggerSql();
