import {
  AgentControlArmedClaimId,
  AgentControlArmedDispatchId,
  AgentControlArmedEvidenceId,
  AgentControlArmedMarkerId,
  AgentControlArmedReceiptId,
  AgentControlTaskId,
  EventId,
  type AgentControlArmedDispatch,
  type AgentControlArmedEpoch,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  parseJsonStrict,
  sha256Utf8,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import {
  isAgentControlRunOnceCandidateVacant,
  selectAgentControlRunOnceCandidate,
} from "../runOnce/selection.ts";
import { fingerprintAgentControlRunOnceSource } from "../runOnce/source.ts";
import {
  deriveArmedClaimId,
  deriveArmedDispatchId,
  deriveArmedEvidenceId,
  deriveArmedMarkerId,
  deriveArmedModeCommandId,
  deriveArmedNoCandidateIdentity,
  deriveArmedReceiptId,
  type ArmedIdentityEpoch,
} from "./identity.ts";
import { AgentControlArmedError } from "./model.ts";

const isArmedError = Schema.is(AgentControlArmedError);

interface TaskFrontierRow {
  readonly taskId: string;
  readonly issueNumber: number;
  readonly status: string;
  readonly sourceGate: string;
  readonly stage: string;
  readonly githubIntakeSequence: number;
  readonly revision: number;
  readonly lastEventSequence: number;
}

interface AuthoritySnapshot {
  readonly projectRevision: number;
  readonly projectEventSequence: number;
  readonly epoch: AgentControlArmedEpoch;
  readonly frontier: ReadonlyArray<TaskFrontierRow>;
}

export type ArmedClaimOutcome =
  | { readonly _tag: "inactive" }
  | { readonly _tag: "busy"; readonly retryAt: string }
  | { readonly _tag: "no-candidate"; readonly replayed: boolean }
  | {
      readonly _tag: "dispatch";
      readonly dispatch: AgentControlArmedDispatch;
      readonly replayed: boolean;
    };

const fail = (projectId: ProjectId, reason: AgentControlArmedError["reason"], cause?: unknown) =>
  new AgentControlArmedError({
    projectId,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const isInt = (value: unknown, minimum: number): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= minimum;

const decodeCanonicalPayload = (bytes: unknown, fingerprint: unknown) => {
  try {
    if (typeof fingerprint !== "string") return null;
    const source = decodeCanonicalUtf8Bytes(bytes);
    const value = parseJsonStrict(source);
    if (
      value === null ||
      Array.isArray(value) ||
      typeof value !== "object" ||
      canonicalJson(value) !== source ||
      sha256Utf8(source) !== fingerprint
    ) {
      return null;
    }
    return value as Readonly<Record<string, JsonValue>>;
  } catch {
    return null;
  }
};

const readSnapshot = Effect.fn("AgentControlArmed.readSnapshot")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
) {
  const sources = yield* sql<Record<string, unknown>>`
    SELECT project.mode, project.paused_from_mode AS "pausedFromMode",
      project.revision AS "projectRevision",
      project.last_event_sequence AS "projectEventSequence",
      github.event_id AS "githubEventId", github.sequence AS "githubEventSequence",
      github.stream_version AS "githubEventStreamVersion",
      json_extract(github.payload_json, '$.repository.repositoryNodeId') AS "repositoryNodeId",
      json_array_length(json_extract(github.payload_json, '$.issues')) AS "expectedIssueCount",
      projected.revision AS "githubProjectionRevision",
      projected.last_event_sequence AS "githubIntakeSequence",
      json_extract(projected.state_json, '$.config.revision') AS "githubConfigRevision",
      json_extract(projected.state_json, '$.pollStatus.status') AS "pollStatus",
      reconcile.revision AS "reconcileRevision", reconcile.status AS "reconcileStatus",
      reconcile.target_sequence AS "targetSequence",
      reconcile.last_completed_sequence AS "lastCompletedSequence"
    FROM main.agent_control_project_states project
    JOIN main.agent_control_github_intake_states projected
      ON projected.project_id = project.project_id
    JOIN main.agent_control_events github
      ON github.aggregate_kind = 'github-intake'
     AND github.stream_id = project.project_id
     AND github.sequence = projected.last_event_sequence
     AND github.stream_version = projected.revision
     AND github.event_type = 'agentControl.github.poll.succeeded'
    JOIN main.agent_control_task_reconcile_states reconcile
      ON reconcile.project_id = project.project_id
    WHERE project.project_id = ${projectId}
  `.pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
  if (sources.length === 0) return null;
  const source = sources[0];
  if (sources.length === 1 && source !== undefined && source.mode !== "armed") return null;
  if (
    sources.length !== 1 ||
    source === undefined ||
    source.mode !== "armed" ||
    source.pausedFromMode !== null ||
    !isInt(source.projectRevision, 1) ||
    !isInt(source.projectEventSequence, 1) ||
    typeof source.githubEventId !== "string" ||
    !isInt(source.githubEventSequence, 1) ||
    !isInt(source.githubEventStreamVersion, 1) ||
    source.githubIntakeSequence !== source.githubEventSequence ||
    source.githubProjectionRevision !== source.githubEventStreamVersion ||
    source.githubConfigRevision !== source.githubEventStreamVersion ||
    typeof source.repositoryNodeId !== "string" ||
    !isInt(source.expectedIssueCount, 0) ||
    source.pollStatus !== "success" ||
    source.reconcileStatus !== "completed" ||
    source.targetSequence !== source.githubIntakeSequence ||
    source.lastCompletedSequence !== source.githubIntakeSequence ||
    !isInt(source.reconcileRevision, 1)
  ) {
    return yield* fail(projectId, "source-watermark-stale");
  }
  const rawFrontier = yield* sql<Record<string, unknown>>`
    SELECT task_id AS "taskId", issue_number AS "issueNumber", status,
      source_gate AS "sourceGate", stage,
      github_intake_sequence AS "githubIntakeSequence", revision,
      last_event_sequence AS "lastEventSequence"
    FROM main.agent_control_task_states INDEXED BY idx_agent_control_armed_task_frontier
    WHERE project_id = ${projectId}
    ORDER BY task_id
  `.pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
  const frontier: Array<TaskFrontierRow> = [];
  for (const row of rawFrontier) {
    if (
      typeof row.taskId !== "string" ||
      !isInt(row.issueNumber, 1) ||
      typeof row.status !== "string" ||
      typeof row.sourceGate !== "string" ||
      typeof row.stage !== "string" ||
      !isInt(row.githubIntakeSequence, 1) ||
      !isInt(row.revision, 1) ||
      !isInt(row.lastEventSequence, 1)
    ) {
      return yield* fail(projectId, "task-history-corrupt");
    }
    frontier.push(row as unknown as TaskFrontierRow);
  }
  const taskFrontierFingerprint = sha256Utf8(
    canonicalJson({ schemaVersion: 1, tasks: frontier } as unknown as JsonValue),
  );
  const epoch = {
    githubIntakeSequence: source.githubIntakeSequence,
    githubEventId: EventId.make(source.githubEventId),
    githubEventSequence: source.githubEventSequence,
    githubEventStreamVersion: source.githubEventStreamVersion,
    sourceFingerprint: fingerprintAgentControlRunOnceSource({
      schemaVersion: 1,
      projectId,
      githubIntakeSequence: source.githubIntakeSequence as number,
      githubProjectionRevision: source.githubProjectionRevision as number,
      githubConfigRevision: source.githubConfigRevision as number,
      repositoryNodeId: source.repositoryNodeId,
      pollStatus: "success",
      expectedIssueCount: source.expectedIssueCount as number,
    }),
    reconcileRevision: source.reconcileRevision,
    taskFrontierSequence: Math.max(0, ...frontier.map((task) => task.lastEventSequence)),
    taskFrontierRevision: Math.max(0, ...frontier.map((task) => task.revision)),
    taskFrontierCount: frontier.length,
    taskFrontierFingerprint,
  } as AgentControlArmedEpoch;
  return {
    projectRevision: source.projectRevision as number,
    projectEventSequence: source.projectEventSequence as number,
    epoch,
    frontier,
  } satisfies AuthoritySnapshot;
});

const identityEpoch = (projectId: ProjectId, snapshot: AuthoritySnapshot): ArmedIdentityEpoch => ({
  projectId,
  projectRevision: snapshot.projectRevision,
  ...snapshot.epoch,
});

const loadDispatch = Effect.fn("AgentControlArmed.loadDispatch")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  dispatchId: string,
) {
  const rows = yield* sql<Record<string, unknown>>`
    SELECT evidence.dispatch_id AS "dispatchId", evidence.claim_id AS "claimId",
      evidence.evidence_id AS "evidenceId", evidence.receipt_id AS "receiptId",
      evidence.marker_id AS "markerId", evidence.mode_command_id AS "commandId",
      evidence.project_id AS "projectId", evidence.selected_task_id AS "selectedTaskId",
      evidence.project_revision AS "projectRevision",
      evidence.project_event_sequence AS "projectEventSequence",
      evidence.github_intake_sequence AS "githubIntakeSequence",
      evidence.github_event_id AS "githubEventId",
      evidence.github_event_sequence AS "githubEventSequence",
      evidence.github_event_stream_version AS "githubEventStreamVersion",
      evidence.source_fingerprint AS "sourceFingerprint",
      evidence.reconcile_revision AS "reconcileRevision",
      evidence.task_frontier_sequence AS "taskFrontierSequence",
      evidence.task_frontier_revision AS "taskFrontierRevision",
      evidence.task_frontier_count AS "taskFrontierCount",
      evidence.task_frontier_fingerprint AS "taskFrontierFingerprint",
      evidence.owner_id AS "evidenceOwnerId", evidence.fence_token AS "evidenceFenceToken",
      evidence.expires_at AS "evidenceExpiresAt", state.owner_id AS "ownerId",
      state.fence_token AS "fenceToken", evidence.claimed_at AS "claimedAt",
      state.expires_at AS "expiresAt", state.updated_at AS "updatedAt",
      state.status, state.activation_event_id AS "activationEventId",
      state.activation_event_sequence AS "activationEventSequence",
      state.activation_event_stream_version AS "activationEventStreamVersion",
      typeof(evidence.payload_json) AS "payloadStorage",
      evidence.payload_json AS "payloadJson", evidence.payload_fingerprint AS "payloadFingerprint",
      marker.marker_fingerprint AS "markerFingerprint", receipt.status AS "receiptStatus"
    FROM main.agent_control_armed_dispatch_evidence evidence
    JOIN main.agent_control_armed_dispatch_receipts receipt
      ON receipt.evidence_id = evidence.evidence_id
     AND receipt.receipt_id = evidence.receipt_id
     AND receipt.marker_id = evidence.marker_id
     AND receipt.dispatch_id = evidence.dispatch_id
     AND receipt.claim_id = evidence.claim_id
    JOIN main.agent_control_armed_dispatch_markers marker
      ON marker.evidence_id = evidence.evidence_id
     AND marker.marker_id = evidence.marker_id
     AND marker.receipt_id = evidence.receipt_id
     AND marker.dispatch_id = evidence.dispatch_id
     AND marker.claim_id = evidence.claim_id
    JOIN main.agent_control_armed_dispatch_states state
      ON state.dispatch_id = evidence.dispatch_id
    WHERE evidence.dispatch_id = ${dispatchId}
  `.pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
  return rows;
});

const decodeDispatch = (
  projectId: ProjectId,
  rows: ReadonlyArray<Record<string, unknown>>,
): AgentControlArmedDispatch | null => {
  const row = rows[0];
  const payload = decodeCanonicalPayload(row?.payloadJson, row?.payloadFingerprint);
  const taskFrontier = payload?.taskFrontier;
  if (
    rows.length !== 1 ||
    row === undefined ||
    typeof row.projectId !== "string" ||
    row.projectId !== projectId ||
    typeof row.dispatchId !== "string" ||
    typeof row.claimId !== "string" ||
    typeof row.evidenceId !== "string" ||
    typeof row.receiptId !== "string" ||
    typeof row.markerId !== "string" ||
    typeof row.commandId !== "string" ||
    typeof row.selectedTaskId !== "string" ||
    typeof row.ownerId !== "string" ||
    typeof row.evidenceOwnerId !== "string" ||
    typeof row.claimedAt !== "string" ||
    typeof row.expiresAt !== "string" ||
    typeof row.evidenceExpiresAt !== "string" ||
    typeof row.updatedAt !== "string" ||
    typeof row.githubEventId !== "string" ||
    typeof row.sourceFingerprint !== "string" ||
    typeof row.taskFrontierFingerprint !== "string" ||
    !isInt(row.fenceToken, 1) ||
    !isInt(row.evidenceFenceToken, 1) ||
    !isInt(row.projectRevision, 1) ||
    !isInt(row.projectEventSequence, 1) ||
    !isInt(row.githubIntakeSequence, 1) ||
    !isInt(row.githubEventSequence, 1) ||
    !isInt(row.githubEventStreamVersion, 1) ||
    !isInt(row.reconcileRevision, 1) ||
    !isInt(row.taskFrontierSequence, 0) ||
    !isInt(row.taskFrontierRevision, 0) ||
    !isInt(row.taskFrontierCount, 0) ||
    row.receiptStatus !== "accepted" ||
    row.payloadStorage !== "blob" ||
    row.markerFingerprint !== row.payloadFingerprint ||
    payload === null ||
    !Array.isArray(taskFrontier) ||
    taskFrontier.length !== row.taskFrontierCount ||
    sha256Utf8(canonicalJson({ schemaVersion: 1, tasks: taskFrontier })) !==
      row.taskFrontierFingerprint ||
    canonicalJson(payload) !==
      canonicalJson({
        schemaVersion: 1,
        dispatchId: row.dispatchId,
        claimId: row.claimId,
        evidenceId: row.evidenceId,
        receiptId: row.receiptId,
        markerId: row.markerId,
        commandId: row.commandId,
        projectId: row.projectId,
        selectedTaskId: row.selectedTaskId,
        projectRevision: row.projectRevision,
        projectEventSequence: row.projectEventSequence,
        epoch: {
          githubIntakeSequence: row.githubIntakeSequence,
          githubEventId: row.githubEventId,
          githubEventSequence: row.githubEventSequence,
          githubEventStreamVersion: row.githubEventStreamVersion,
          sourceFingerprint: row.sourceFingerprint,
          reconcileRevision: row.reconcileRevision,
          taskFrontierSequence: row.taskFrontierSequence,
          taskFrontierRevision: row.taskFrontierRevision,
          taskFrontierCount: row.taskFrontierCount,
          taskFrontierFingerprint: row.taskFrontierFingerprint,
        },
        ownerId: row.evidenceOwnerId,
        fenceToken: row.evidenceFenceToken,
        claimedAt: row.claimedAt,
        expiresAt: row.evidenceExpiresAt,
        kind: "dispatch",
        taskFrontier,
      }) ||
    row.ownerId !== row.evidenceOwnerId ||
    row.fenceToken !== row.evidenceFenceToken ||
    row.expiresAt !== row.evidenceExpiresAt ||
    (row.status === "claimed" && row.activationEventId !== null) ||
    (row.status === "activated" &&
      (typeof row.activationEventId !== "string" ||
        !isInt(row.activationEventSequence, 1) ||
        !isInt(row.activationEventStreamVersion, 1)))
  ) {
    return null;
  }
  return {
    schemaVersion: 1,
    dispatchId: AgentControlArmedDispatchId.make(row.dispatchId),
    claimId: AgentControlArmedClaimId.make(row.claimId),
    evidenceId: AgentControlArmedEvidenceId.make(row.evidenceId),
    receiptId: AgentControlArmedReceiptId.make(row.receiptId),
    markerId: AgentControlArmedMarkerId.make(row.markerId),
    commandId: row.commandId as never,
    projectId,
    selectedTaskId: AgentControlTaskId.make(row.selectedTaskId),
    projectRevision: row.projectRevision as number,
    projectEventSequence: row.projectEventSequence as number,
    epoch: {
      githubIntakeSequence: row.githubIntakeSequence as number,
      githubEventId: EventId.make(row.githubEventId),
      githubEventSequence: row.githubEventSequence as number,
      githubEventStreamVersion: row.githubEventStreamVersion as number,
      sourceFingerprint: row.sourceFingerprint,
      reconcileRevision: row.reconcileRevision as number,
      taskFrontierSequence: row.taskFrontierSequence as number,
      taskFrontierRevision: row.taskFrontierRevision as number,
      taskFrontierCount: row.taskFrontierCount as number,
      taskFrontierFingerprint: row.taskFrontierFingerprint,
    },
    ownerId: row.ownerId,
    fenceToken: row.fenceToken as number,
    claimedAt: row.claimedAt,
    expiresAt: row.expiresAt,
  };
};

const writeNoCandidate = Effect.fn("AgentControlArmed.writeNoCandidate")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  snapshot: AuthoritySnapshot,
  decidedAt: string,
) {
  const ids = {
    evidenceId: deriveArmedNoCandidateIdentity("evidence", identityEpoch(projectId, snapshot)),
    receiptId: deriveArmedNoCandidateIdentity("receipt", identityEpoch(projectId, snapshot)),
    markerId: deriveArmedNoCandidateIdentity("marker", identityEpoch(projectId, snapshot)),
  };
  const payload = {
    schemaVersion: 1,
    kind: "no-candidate",
    ...ids,
    projectId,
    projectRevision: snapshot.projectRevision,
    projectEventSequence: snapshot.projectEventSequence,
    epoch: snapshot.epoch,
    taskFrontier: snapshot.frontier,
    decidedAt,
  } as const;
  const payloadJson = canonicalJson(payload as unknown as JsonValue);
  const payloadBytes = new TextEncoder().encode(payloadJson);
  const payloadFingerprint = sha256Utf8(payloadJson);
  const existing = yield* sql<Record<string, unknown>>`
    SELECT evidence.evidence_id AS "evidenceId", evidence.receipt_id AS "receiptId",
      evidence.marker_id AS "markerId", evidence.project_id AS "projectId",
      evidence.project_revision AS "projectRevision",
      evidence.project_event_sequence AS "projectEventSequence",
      evidence.github_intake_sequence AS "githubIntakeSequence",
      evidence.github_event_id AS "githubEventId",
      evidence.github_event_sequence AS "githubEventSequence",
      evidence.github_event_stream_version AS "githubEventStreamVersion",
      evidence.source_fingerprint AS "sourceFingerprint",
      evidence.reconcile_revision AS "reconcileRevision",
      evidence.task_frontier_sequence AS "taskFrontierSequence",
      evidence.task_frontier_revision AS "taskFrontierRevision",
      evidence.task_frontier_count AS "taskFrontierCount",
      evidence.task_frontier_fingerprint AS "taskFrontierFingerprint",
      evidence.decided_at AS "decidedAt", typeof(evidence.payload_json) AS "payloadStorage",
      evidence.payload_json AS "payloadJson",
      evidence.payload_fingerprint AS "payloadFingerprint",
      receipt.status, receipt.receipt_id AS "receiptRecordId",
      receipt.marker_id AS "receiptMarkerId", receipt.accepted_at AS "acceptedAt",
      marker.marker_id AS "markerRecordId", marker.receipt_id AS "markerReceiptId",
      marker.marker_fingerprint AS "markerFingerprint", marker.committed_at AS "committedAt"
    FROM main.agent_control_armed_no_candidate_evidence evidence
    LEFT JOIN main.agent_control_armed_no_candidate_receipts receipt
      ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
     AND receipt.marker_id = evidence.marker_id
    LEFT JOIN main.agent_control_armed_no_candidate_markers marker
      ON marker.evidence_id = evidence.evidence_id AND marker.marker_id = evidence.marker_id
     AND marker.receipt_id = evidence.receipt_id
    WHERE evidence.project_id = ${projectId}
      AND evidence.github_intake_sequence = ${snapshot.epoch.githubIntakeSequence}
      AND evidence.github_event_id = ${snapshot.epoch.githubEventId}
      AND evidence.github_event_sequence = ${snapshot.epoch.githubEventSequence}
      AND evidence.github_event_stream_version = ${snapshot.epoch.githubEventStreamVersion}
      AND evidence.source_fingerprint = ${snapshot.epoch.sourceFingerprint}
      AND evidence.reconcile_revision = ${snapshot.epoch.reconcileRevision}
      AND evidence.task_frontier_sequence = ${snapshot.epoch.taskFrontierSequence}
      AND evidence.task_frontier_revision = ${snapshot.epoch.taskFrontierRevision}
      AND evidence.task_frontier_count = ${snapshot.epoch.taskFrontierCount}
      AND evidence.task_frontier_fingerprint = ${snapshot.epoch.taskFrontierFingerprint}
  `;
  if (existing.length !== 0) {
    const row = existing[0];
    const existingPayload = decodeCanonicalPayload(row?.payloadJson, row?.payloadFingerprint);
    const taskFrontier = existingPayload?.taskFrontier;
    const existingIds =
      row !== undefined && isInt(row.projectRevision, 1)
        ? {
            evidenceId: deriveArmedNoCandidateIdentity("evidence", {
              projectId,
              projectRevision: row.projectRevision,
              ...snapshot.epoch,
            }),
            receiptId: deriveArmedNoCandidateIdentity("receipt", {
              projectId,
              projectRevision: row.projectRevision,
              ...snapshot.epoch,
            }),
            markerId: deriveArmedNoCandidateIdentity("marker", {
              projectId,
              projectRevision: row.projectRevision,
              ...snapshot.epoch,
            }),
          }
        : null;
    if (
      existing.length !== 1 ||
      row === undefined ||
      existingIds === null ||
      row.evidenceId !== existingIds.evidenceId ||
      row.receiptId !== existingIds.receiptId ||
      row.markerId !== existingIds.markerId ||
      row.projectId !== projectId ||
      row.receiptRecordId !== existingIds.receiptId ||
      row.receiptMarkerId !== existingIds.markerId ||
      row.markerRecordId !== existingIds.markerId ||
      row.markerReceiptId !== existingIds.receiptId ||
      typeof row.decidedAt !== "string" ||
      row.acceptedAt !== row.decidedAt ||
      row.committedAt !== row.decidedAt ||
      typeof row.payloadFingerprint !== "string" ||
      row.markerFingerprint !== row.payloadFingerprint ||
      row.status !== "accepted" ||
      row.payloadStorage !== "blob" ||
      existingPayload === null ||
      !Array.isArray(taskFrontier) ||
      !isInt(row.projectRevision, 1) ||
      !isInt(row.projectEventSequence, 1) ||
      row.githubIntakeSequence !== snapshot.epoch.githubIntakeSequence ||
      row.githubEventId !== snapshot.epoch.githubEventId ||
      row.githubEventSequence !== snapshot.epoch.githubEventSequence ||
      row.githubEventStreamVersion !== snapshot.epoch.githubEventStreamVersion ||
      row.sourceFingerprint !== snapshot.epoch.sourceFingerprint ||
      row.reconcileRevision !== snapshot.epoch.reconcileRevision ||
      row.taskFrontierSequence !== snapshot.epoch.taskFrontierSequence ||
      row.taskFrontierRevision !== snapshot.epoch.taskFrontierRevision ||
      row.taskFrontierCount !== snapshot.epoch.taskFrontierCount ||
      row.taskFrontierFingerprint !== snapshot.epoch.taskFrontierFingerprint ||
      canonicalJson(taskFrontier) !== canonicalJson(snapshot.frontier as unknown as JsonValue) ||
      canonicalJson(existingPayload) !==
        canonicalJson({
          schemaVersion: 1,
          kind: "no-candidate",
          evidenceId: existingIds.evidenceId,
          receiptId: existingIds.receiptId,
          markerId: existingIds.markerId,
          projectId,
          projectRevision: row.projectRevision,
          projectEventSequence: row.projectEventSequence,
          epoch: snapshot.epoch,
          taskFrontier,
          decidedAt: row.decidedAt,
        })
    ) {
      return yield* fail(projectId, "identity-mismatch");
    }
    return { _tag: "no-candidate" as const, replayed: true };
  }
  const partial = yield* sql<{ readonly count: number }>`
    SELECT (
      (SELECT count(*) FROM main.agent_control_armed_no_candidate_evidence
        WHERE evidence_id = ${ids.evidenceId} OR receipt_id = ${ids.receiptId} OR marker_id = ${ids.markerId})
      + (SELECT count(*) FROM main.agent_control_armed_no_candidate_receipts
        WHERE evidence_id = ${ids.evidenceId} OR receipt_id = ${ids.receiptId} OR marker_id = ${ids.markerId})
      + (SELECT count(*) FROM main.agent_control_armed_no_candidate_markers
        WHERE evidence_id = ${ids.evidenceId} OR receipt_id = ${ids.receiptId} OR marker_id = ${ids.markerId})
    ) AS count
  `;
  if (partial[0]?.count !== 0) return yield* fail(projectId, "partial-replay");
  yield* sql`
    INSERT INTO main.agent_control_armed_no_candidate_evidence (
      evidence_id, receipt_id, marker_id, project_id, project_revision,
      project_event_sequence, github_intake_sequence, github_event_id,
      github_event_sequence, github_event_stream_version, source_fingerprint,
      reconcile_revision, task_frontier_sequence, task_frontier_revision,
      task_frontier_count, task_frontier_fingerprint, decided_at, payload_json,
      payload_fingerprint
    ) VALUES (
      ${ids.evidenceId}, ${ids.receiptId}, ${ids.markerId}, ${projectId},
      ${snapshot.projectRevision}, ${snapshot.projectEventSequence},
      ${snapshot.epoch.githubIntakeSequence}, ${snapshot.epoch.githubEventId},
      ${snapshot.epoch.githubEventSequence}, ${snapshot.epoch.githubEventStreamVersion},
      ${snapshot.epoch.sourceFingerprint}, ${snapshot.epoch.reconcileRevision},
      ${snapshot.epoch.taskFrontierSequence}, ${snapshot.epoch.taskFrontierRevision},
      ${snapshot.epoch.taskFrontierCount}, ${snapshot.epoch.taskFrontierFingerprint},
      ${decidedAt}, ${payloadBytes}, ${payloadFingerprint}
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_armed_no_candidate_receipts (
      receipt_id, evidence_id, marker_id, status, accepted_at
    ) VALUES (${ids.receiptId}, ${ids.evidenceId}, ${ids.markerId}, 'accepted', ${decidedAt})
  `;
  yield* sql`
    INSERT INTO main.agent_control_armed_no_candidate_markers (
      marker_id, evidence_id, receipt_id, marker_fingerprint, committed_at
    ) VALUES (${ids.markerId}, ${ids.evidenceId}, ${ids.receiptId}, ${payloadFingerprint}, ${decidedAt})
  `;
  return { _tag: "no-candidate" as const, replayed: false };
});

const writeDispatch = Effect.fn("AgentControlArmed.writeDispatch")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  snapshot: AuthoritySnapshot,
  selectedTaskId: AgentControlTaskId,
  ownerId: string,
  claimedAt: string,
  expiresAt: string,
) {
  const priorAttempts = yield* sql<{ readonly fenceToken: unknown }>`
    SELECT MAX(fence_token) AS "fenceToken"
    FROM main.agent_control_armed_dispatch_evidence
    WHERE project_id = ${projectId} AND project_revision = ${snapshot.projectRevision}
      AND github_event_id = ${snapshot.epoch.githubEventId}
      AND reconcile_revision = ${snapshot.epoch.reconcileRevision}
      AND task_frontier_sequence = ${snapshot.epoch.taskFrontierSequence}
      AND task_frontier_revision = ${snapshot.epoch.taskFrontierRevision}
      AND task_frontier_count = ${snapshot.epoch.taskFrontierCount}
      AND task_frontier_fingerprint = ${snapshot.epoch.taskFrontierFingerprint}
      AND selected_task_id = ${selectedTaskId}
  `;
  const priorFenceToken = priorAttempts[0]?.fenceToken;
  if (priorAttempts.length !== 1 || (priorFenceToken !== null && !isInt(priorFenceToken, 1))) {
    return yield* fail(projectId, "projection-corrupt");
  }
  const fenceToken = priorFenceToken === null ? 1 : (priorFenceToken as number) + 1;
  const dispatchId = deriveArmedDispatchId(
    identityEpoch(projectId, snapshot),
    selectedTaskId,
    fenceToken,
  );
  const dispatch = {
    schemaVersion: 1,
    dispatchId,
    claimId: deriveArmedClaimId(dispatchId),
    evidenceId: deriveArmedEvidenceId(dispatchId),
    receiptId: deriveArmedReceiptId(dispatchId),
    markerId: deriveArmedMarkerId(dispatchId),
    commandId: deriveArmedModeCommandId(dispatchId),
    projectId,
    selectedTaskId,
    projectRevision: snapshot.projectRevision,
    projectEventSequence: snapshot.projectEventSequence,
    epoch: snapshot.epoch,
    ownerId,
    fenceToken,
    claimedAt,
    expiresAt,
  } satisfies AgentControlArmedDispatch;
  const payload = {
    ...dispatch,
    kind: "dispatch",
    taskFrontier: snapshot.frontier,
  } as const;
  const payloadJson = canonicalJson(payload as unknown as JsonValue);
  const payloadBytes = new TextEncoder().encode(payloadJson);
  const payloadFingerprint = sha256Utf8(payloadJson);
  const existingRows = yield* loadDispatch(sql, projectId, dispatchId);
  if (existingRows.length !== 0) {
    const existing = decodeDispatch(projectId, existingRows);
    if (existing === null) return yield* fail(projectId, "identity-mismatch");
    return { dispatch: existing, replayed: true } as const;
  }
  yield* sql`
    INSERT INTO main.agent_control_armed_dispatch_evidence (
      evidence_id, receipt_id, marker_id, dispatch_id, claim_id, mode_command_id,
      project_id, selected_task_id, project_revision, project_event_sequence,
      github_intake_sequence, github_event_id, github_event_sequence,
      github_event_stream_version, source_fingerprint, reconcile_revision,
      task_frontier_sequence, task_frontier_revision, task_frontier_count,
      task_frontier_fingerprint, owner_id, fence_token, claimed_at, expires_at,
      payload_json, payload_fingerprint
    ) VALUES (
      ${dispatch.evidenceId}, ${dispatch.receiptId}, ${dispatch.markerId},
      ${dispatch.dispatchId}, ${dispatch.claimId}, ${dispatch.commandId}, ${projectId},
      ${selectedTaskId}, ${snapshot.projectRevision}, ${snapshot.projectEventSequence},
      ${snapshot.epoch.githubIntakeSequence}, ${snapshot.epoch.githubEventId},
      ${snapshot.epoch.githubEventSequence}, ${snapshot.epoch.githubEventStreamVersion},
      ${snapshot.epoch.sourceFingerprint}, ${snapshot.epoch.reconcileRevision},
      ${snapshot.epoch.taskFrontierSequence}, ${snapshot.epoch.taskFrontierRevision},
      ${snapshot.epoch.taskFrontierCount}, ${snapshot.epoch.taskFrontierFingerprint},
      ${ownerId}, ${fenceToken}, ${claimedAt}, ${expiresAt}, ${payloadBytes}, ${payloadFingerprint}
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_armed_dispatch_receipts (
      receipt_id, evidence_id, marker_id, dispatch_id, claim_id, status, accepted_at
    ) VALUES (
      ${dispatch.receiptId}, ${dispatch.evidenceId}, ${dispatch.markerId},
      ${dispatch.dispatchId}, ${dispatch.claimId}, 'accepted', ${claimedAt}
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_armed_dispatch_states (
      dispatch_id, project_id, status, owner_id, fence_token, expires_at,
      activation_event_id, activation_event_sequence, activation_event_stream_version, updated_at
    ) VALUES (
      ${dispatch.dispatchId}, ${projectId}, 'claimed', ${ownerId}, ${fenceToken}, ${expiresAt},
      NULL, NULL, NULL, ${claimedAt}
    )
  `;
  yield* sql`
    INSERT INTO main.agent_control_armed_dispatch_markers (
      marker_id, evidence_id, receipt_id, dispatch_id, claim_id,
      marker_fingerprint, committed_at
    ) VALUES (
      ${dispatch.markerId}, ${dispatch.evidenceId}, ${dispatch.receiptId},
      ${dispatch.dispatchId}, ${dispatch.claimId}, ${payloadFingerprint}, ${claimedAt}
    )
  `;
  return { dispatch, replayed: false } as const;
});

export const claimArmedDispatch = Effect.fn("claimArmedDispatch")(function* (
  sql: SqlClient.SqlClient,
  input: {
    readonly projectId: ProjectId;
    readonly ownerId: string;
    readonly claimedAt: string;
    readonly expiresAt: string;
  },
) {
  return yield* sql
    .withTransaction(
      Effect.gen(function* () {
        const active = yield* sql<Record<string, unknown>>`
        SELECT dispatch_id AS "dispatchId", owner_id AS "ownerId", status, expires_at AS "expiresAt"
        FROM main.agent_control_armed_dispatch_states
        WHERE project_id = ${input.projectId} AND status IN ('claimed', 'activated')
      `;
        if (active.length > 1) return yield* fail(input.projectId, "projection-corrupt");
        if (active.length === 1) {
          const row = active[0]!;
          if (
            typeof row.dispatchId !== "string" ||
            typeof row.ownerId !== "string" ||
            typeof row.expiresAt !== "string" ||
            (row.status !== "claimed" && row.status !== "activated")
          )
            return yield* fail(input.projectId, "projection-corrupt");
          const dispatch = decodeDispatch(
            input.projectId,
            yield* loadDispatch(sql, input.projectId, row.dispatchId),
          );
          if (dispatch === null) return yield* fail(input.projectId, "authority-conflict");
          const activatedModeEvents =
            row.status === "claimed"
              ? yield* sql<{ readonly eventId: unknown }>`
              SELECT event.event_id AS "eventId"
              FROM main.agent_control_events event
              JOIN main.agent_control_command_receipts receipt
                ON receipt.command_id = event.command_id
              WHERE event.aggregate_kind = 'project-controller'
                AND event.stream_id = ${input.projectId}
                AND event.command_id = ${dispatch.commandId}
                AND event.event_type = 'agentControl.project.mode.changed'
                AND event.actor_authority = 'system'
                AND receipt.status = 'accepted' AND receipt.event_created = 1
                AND receipt.result_sequence = event.sequence
                AND receipt.result_stream_version = event.stream_version
            `
              : [];
          if (activatedModeEvents.length > 1) {
            return yield* fail(input.projectId, "projection-corrupt");
          }
          if (
            row.status === "activated" ||
            activatedModeEvents.length === 1 ||
            (row.ownerId === input.ownerId && row.expiresAt > input.claimedAt)
          ) {
            return { _tag: "dispatch" as const, dispatch, replayed: true };
          }
          if (row.expiresAt > input.claimedAt) {
            return { _tag: "busy" as const, retryAt: row.expiresAt };
          }
          // Rotate an expired pre-activation claim to a fresh immutable dispatch.
          // Reusing the old command identity would let a fenced-out controller
          // authorize the system mode event after another controller won.
          const superseded = yield* sql<{ readonly dispatchId: unknown }>`
          UPDATE main.agent_control_armed_dispatch_states
          SET status = 'superseded', updated_at = ${input.claimedAt}
          WHERE dispatch_id = ${row.dispatchId} AND status = 'claimed'
            AND owner_id = ${row.ownerId} AND expires_at = ${row.expiresAt}
            AND expires_at <= ${input.claimedAt}
          RETURNING dispatch_id AS "dispatchId"
        `;
          if (superseded.length !== 1 || superseded[0]?.dispatchId !== row.dispatchId) {
            return { _tag: "busy" as const, retryAt: row.expiresAt };
          }
        }
        const snapshot = yield* readSnapshot(sql, input.projectId);
        if (snapshot === null) return { _tag: "inactive" as const };
        const selectedTaskId = yield* selectAgentControlRunOnceCandidate(
          sql,
          input.projectId,
          snapshot.epoch.githubIntakeSequence,
        ).pipe(Effect.mapError((cause) => fail(input.projectId, "persistence", cause)));
        if (selectedTaskId === null) {
          return yield* writeNoCandidate(sql, input.projectId, snapshot, input.claimedAt);
        }
        if (!(yield* isAgentControlRunOnceCandidateVacant(sql, input.projectId, selectedTaskId))) {
          return yield* fail(input.projectId, "task-history-corrupt");
        }
        const result = yield* writeDispatch(
          sql,
          input.projectId,
          snapshot,
          selectedTaskId,
          input.ownerId,
          input.claimedAt,
          input.expiresAt,
        );
        return { _tag: "dispatch" as const, ...result };
      }),
    )
    .pipe(
      Effect.mapError((cause) =>
        isArmedError(cause) ? cause : fail(input.projectId, "persistence", cause),
      ),
    );
});

export const activateArmedDispatch = Effect.fn("activateArmedDispatch")(function* (
  sql: SqlClient.SqlClient,
  dispatch: AgentControlArmedDispatch,
  event: {
    readonly eventId: string;
    readonly sequence: number;
    readonly streamVersion: number;
    readonly occurredAt: string;
  },
) {
  const projectId = dispatch.projectId;
  const dispatchId = dispatch.dispatchId;
  const updated = yield* sql<{ readonly dispatchId: unknown }>`
    UPDATE main.agent_control_armed_dispatch_states
    SET status = 'activated', activation_event_id = ${event.eventId},
      activation_event_sequence = ${event.sequence},
      activation_event_stream_version = ${event.streamVersion}, updated_at = ${event.occurredAt}
    WHERE dispatch_id = ${dispatchId} AND project_id = ${projectId} AND status = 'claimed'
      AND owner_id = ${dispatch.ownerId} AND fence_token = ${dispatch.fenceToken}
      AND expires_at = ${dispatch.expiresAt} AND expires_at > ${event.occurredAt}
    RETURNING dispatch_id AS "dispatchId"
  `.pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
  if (updated.length === 1 && updated[0]?.dispatchId === dispatchId) return false;
  const existing = yield* sql<Record<string, unknown>>`
    SELECT status, owner_id AS "ownerId", fence_token AS "fenceToken",
      expires_at AS "expiresAt", activation_event_id AS "eventId",
      activation_event_sequence AS sequence, activation_event_stream_version AS "streamVersion"
    FROM main.agent_control_armed_dispatch_states WHERE dispatch_id = ${dispatchId}
  `;
  if (
    existing.length === 1 &&
    existing[0]?.status === "activated" &&
    existing[0].ownerId === dispatch.ownerId &&
    existing[0].fenceToken === dispatch.fenceToken &&
    existing[0].expiresAt === dispatch.expiresAt &&
    existing[0].eventId === event.eventId &&
    existing[0].sequence === event.sequence &&
    existing[0].streamVersion === event.streamVersion
  )
    return true;
  return yield* fail(projectId, "authority-conflict");
});

export const finishArmedDispatch = Effect.fn("finishArmedDispatch")(function* (
  sql: SqlClient.SqlClient,
  dispatch: AgentControlArmedDispatch,
  status: "completed" | "superseded",
  updatedAt: string,
) {
  const projectId = dispatch.projectId;
  const dispatchId = dispatch.dispatchId;
  const updated = yield* (
    status === "completed"
      ? sql<{ readonly dispatchId: unknown }>`
        UPDATE main.agent_control_armed_dispatch_states
        SET status = 'completed', updated_at = ${updatedAt}
        WHERE dispatch_id = ${dispatchId} AND project_id = ${projectId} AND status = 'activated'
          AND owner_id = ${dispatch.ownerId} AND fence_token = ${dispatch.fenceToken}
          AND expires_at = ${dispatch.expiresAt}
        RETURNING dispatch_id AS "dispatchId"
      `
      : sql<{ readonly dispatchId: unknown }>`
        UPDATE main.agent_control_armed_dispatch_states
        SET status = 'superseded', updated_at = ${updatedAt}
        WHERE dispatch_id = ${dispatchId} AND project_id = ${projectId}
          AND status IN ('claimed', 'activated')
          AND owner_id = ${dispatch.ownerId} AND fence_token = ${dispatch.fenceToken}
          AND expires_at = ${dispatch.expiresAt}
        RETURNING dispatch_id AS "dispatchId"
      `
  ).pipe(Effect.mapError((cause) => fail(projectId, "persistence", cause)));
  if (updated.length === 1 && updated[0]?.dispatchId === dispatchId) return false;
  const existing = yield* sql<{
    readonly status: unknown;
    readonly ownerId: unknown;
    readonly fenceToken: unknown;
    readonly expiresAt: unknown;
  }>`
    SELECT status, owner_id AS "ownerId", fence_token AS "fenceToken",
      expires_at AS "expiresAt" FROM main.agent_control_armed_dispatch_states
    WHERE dispatch_id = ${dispatchId} AND project_id = ${projectId}
  `;
  if (
    existing.length === 1 &&
    existing[0]?.status === status &&
    existing[0].ownerId === dispatch.ownerId &&
    existing[0].fenceToken === dispatch.fenceToken &&
    existing[0].expiresAt === dispatch.expiresAt
  )
    return true;
  return yield* fail(projectId, "authority-conflict");
});

export const loadArmedCatchUpProjectIds = Effect.fn("loadArmedCatchUpProjectIds")(function* (
  sql: SqlClient.SqlClient,
) {
  const [armed, dispatches] = yield* Effect.all([
    sql<{ readonly projectId: unknown }>`
      SELECT project_id AS "projectId"
      FROM main.agent_control_project_states INDEXED BY idx_agent_control_armed_project_catchup
      WHERE mode = 'armed' AND paused_from_mode IS NULL
    `,
    sql<{ readonly projectId: unknown }>`
      SELECT project_id AS "projectId"
      FROM main.agent_control_armed_dispatch_states INDEXED BY idx_agent_control_armed_claim_recovery
      WHERE status IN ('claimed', 'activated')
    `,
  ]);
  const projects: Array<ProjectId> = [];
  const unique = new Set<string>();
  for (const row of [...armed, ...dispatches]) {
    if (typeof row.projectId !== "string" || row.projectId.length === 0) {
      return yield* fail("armed-recovery" as ProjectId, "projection-corrupt");
    }
    unique.add(row.projectId);
  }
  for (const project of [...unique].sort()) projects.push(project as ProjectId);
  return projects;
});
