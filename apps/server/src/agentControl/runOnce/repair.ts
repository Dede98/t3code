import { AgentControlRunOnceId, AgentControlStageRunId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { decodeVerificationResult } from "../verificationTurn/verificationResult.ts";

class RunOnceRepairEvidenceError extends Schema.TaggedError<RunOnceRepairEvidenceError>()(
  "RunOnceRepairEvidenceError",
  {},
) {}

const Repair = Schema.Struct({
  runId: AgentControlRunOnceId,
  verificationHandoffId: Schema.String,
  verificationMarkerId: Schema.String,
  verificationFingerprint: Schema.String,
  planningHandoffId: Schema.String,
  repairStageRunId: AgentControlStageRunId,
  reportJson: Schema.String,
  reportDigest: Schema.String,
  stageRunId: Schema.String,
  attemptId: Schema.String,
  leaseHolderId: Schema.String,
  fenceToken: Schema.Int,
  leaseEventId: Schema.String,
  leaseEventStreamVersion: Schema.Int,
});
export type RunOnceRepair = typeof Repair.Type;
const decodeRepair = Schema.decodeUnknownEffect(Repair);

/** An immutable claim is both the consumed limit and the recoverable handoff. */
export const loadRunOnceRepair = Effect.fn("loadRunOnceRepair")(function* (
  sql: SqlClient.SqlClient,
  verificationHandoffId: string,
) {
  const available = yield* sql`SELECT 1 FROM main.sqlite_schema
    WHERE type = 'table' AND name = 'agent_control_run_once_repairs'`;
  if (available.length === 0) return Option.none<RunOnceRepair>();
  const rows = yield* sql`
    SELECT repair.run_id AS "runId", repair.verification_handoff_id AS "verificationHandoffId",
      repair.verification_marker_id AS "verificationMarkerId",
      repair.verification_fingerprint AS "verificationFingerprint",
      planning_handoff_id AS "planningHandoffId", repair_stage_run_id AS "repairStageRunId",
      report_json AS "reportJson", report_digest AS "reportDigest",
      verification.stage_run_id AS "stageRunId", verification.attempt_id AS "attemptId",
      verification.lease_holder_id AS "leaseHolderId", verification.fence_token AS "fenceToken",
      verification.lease_event_id AS "leaseEventId",
      verification.lease_event_stream_version AS "leaseEventStreamVersion"
    FROM agent_control_run_once_repairs repair
    JOIN agent_control_verification_finalization_evidence verification
      ON verification.handoff_id = repair.verification_handoff_id
      AND verification.marker_id = repair.verification_marker_id
      AND verification.finalization_fingerprint = repair.verification_fingerprint
    JOIN agent_control_verification_finalization_markers verification_marker
      ON verification_marker.marker_id = verification.marker_id
    JOIN agent_control_verification_evaluation_evidence evaluation
      ON evaluation.evidence_id = verification.evaluation_evidence_id
      AND evaluation.semantic_result_digest = repair.report_digest
      AND evaluation.verdict = 'failed' AND evaluation.disposition = 'evaluated'
    JOIN agent_control_verification_evaluation_markers evaluation_marker
      ON evaluation_marker.marker_id = evaluation.marker_id
      AND evaluation_marker.evidence_id = evaluation.evidence_id
      AND evaluation_marker.evaluation_fingerprint = evaluation.evaluation_fingerprint
    WHERE repair.verification_handoff_id = ${verificationHandoffId}
  `;
  if (rows.length === 0) {
    const claim =
      yield* sql`SELECT 1 FROM agent_control_run_once_repairs WHERE verification_handoff_id = ${verificationHandoffId}`;
    if (claim.length !== 0) return yield* new RunOnceRepairEvidenceError({});
    return Option.none<RunOnceRepair>();
  }
  const repair = yield* decodeRepair(rows[0]);
  const result = yield* decodeVerificationResult(new TextEncoder().encode(repair.reportJson));
  if (result.verdict !== "failed" || result.semanticDigest !== repair.reportDigest) {
    return yield* new RunOnceRepairEvidenceError({});
  }
  return Option.some(repair);
});

/** Re-verification uses the immutable failure that authorized its implementation stage. */
export const loadRunOnceRepairForImplementationStage = Effect.fn(
  "loadRunOnceRepairForImplementationStage",
)(function* (sql: SqlClient.SqlClient, implementationStageRunId: string) {
  const available = yield* sql`SELECT 1 FROM main.sqlite_schema
    WHERE type = 'table' AND name = 'agent_control_run_once_repairs'`;
  if (available.length === 0) return Option.none<RunOnceRepair>();
  const rows = yield* sql<{ verificationHandoffId: string }>`
    SELECT verification_handoff_id AS "verificationHandoffId"
    FROM agent_control_run_once_repairs WHERE repair_stage_run_id = ${implementationStageRunId}`;
  if (rows.length === 0) return Option.none<RunOnceRepair>();
  if (rows.length !== 1) return yield* new RunOnceRepairEvidenceError({});
  return yield* loadRunOnceRepair(sql, rows[0]!.verificationHandoffId);
});
