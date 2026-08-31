import {
  type AgentControlRunOnceActivation,
  AgentControlRunOnceId,
  type AgentControlRunOnceStep,
  type AgentControlTaskEvent,
  type AgentControlTaskId,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import {
  canonicalJson,
  decodeCanonicalUtf8Bytes,
  sha256Utf8,
  type JsonValue,
} from "../initialPlanning/eventEvidence.ts";
import {
  deriveRunOnceClaimId,
  deriveRunOnceCommandId,
  deriveRunOnceEvidenceId,
  deriveRunOnceMarkerId,
  deriveRunOncePublicationId,
  deriveRunOnceReceiptId,
} from "./identity.ts";
import { AgentControlRunOnceError, type RunOnceStepBindings } from "./model.ts";

export interface RunOnceStateBinding {
  readonly projectId: ProjectId;
  readonly status: "active" | "completed" | "no-eligible-task";
  readonly taskId: string | null;
  readonly stageRunId: string | null;
  readonly leaseId: string | null;
  readonly worktreeReservationId: string | null;
  readonly controlledThreadReservationId: string | null;
  readonly terminalTaskEventId: string | null;
  readonly activationProjectRevision: number;
  readonly resetProjectRevision: number | null;
}

interface StepRow {
  readonly evidenceId: unknown;
  readonly receiptId: unknown;
  readonly markerId: unknown;
  readonly commandId: unknown;
  readonly payloadJson: unknown;
  readonly payloadFingerprint: unknown;
  readonly markerFingerprint: unknown;
  readonly publicationId: unknown;
  readonly projectId: unknown;
  readonly step: unknown;
  readonly ordinal: unknown;
  readonly recordedAt: unknown;
  readonly claimId: unknown;
  readonly claimedAt: unknown;
  readonly receiptStatus: unknown;
  readonly acceptedAt: unknown;
  readonly committedAt: unknown;
  readonly publishedAt: unknown;
  readonly attemptCount: unknown;
}

const failure = (
  projectId: ProjectId,
  runId: AgentControlRunOnceId | null,
  step: AgentControlRunOnceStep | null,
  reason: AgentControlRunOnceError["reason"],
  cause?: unknown,
) =>
  new AgentControlRunOnceError({
    projectId,
    runId,
    step,
    reason,
    ...(cause === undefined ? {} : { cause }),
  });

const sameBytes = (left: Uint8Array, right: Uint8Array) =>
  left.byteLength === right.byteLength && left.every((value, index) => value === right[index]);

const loadStepRows = (sql: SqlClient.SqlClient, runId: AgentControlRunOnceId, ordinal: number) =>
  sql<StepRow>`
    SELECT evidence.evidence_id AS "evidenceId", evidence.receipt_id AS "receiptId",
           evidence.marker_id AS "markerId", evidence.command_id AS "commandId",
           evidence.payload_json AS "payloadJson",
           evidence.payload_fingerprint AS "payloadFingerprint",
           marker.marker_fingerprint AS "markerFingerprint",
           publication.publication_id AS "publicationId",
           evidence.project_id AS "projectId", evidence.step, evidence.ordinal,
           evidence.recorded_at AS "recordedAt",
           claim.claim_id AS "claimId", claim.claimed_at AS "claimedAt",
           receipt.status AS "receiptStatus", receipt.accepted_at AS "acceptedAt",
           marker.committed_at AS "committedAt",
           publication.published_at AS "publishedAt",
           publication.attempt_count AS "attemptCount"
    FROM main.agent_control_run_once_step_evidence evidence
    JOIN main.agent_control_run_once_step_claims claim
      ON claim.run_id = evidence.run_id AND claim.ordinal = evidence.ordinal
     AND claim.step = evidence.step AND claim.command_id = evidence.command_id
    JOIN main.agent_control_run_once_step_receipts receipt
      ON receipt.evidence_id = evidence.evidence_id AND receipt.receipt_id = evidence.receipt_id
    JOIN main.agent_control_run_once_step_markers marker
      ON marker.evidence_id = evidence.evidence_id AND marker.receipt_id = receipt.receipt_id
    JOIN main.agent_control_run_once_publications publication
      ON publication.evidence_id = evidence.evidence_id AND publication.marker_id = marker.marker_id
    WHERE evidence.run_id = ${runId} AND evidence.ordinal = ${ordinal}
  `;

export interface WriteRunOnceStepInput {
  readonly runId: AgentControlRunOnceId;
  readonly projectId: ProjectId;
  readonly ordinal: number;
  readonly step: AgentControlRunOnceStep;
  readonly payload: JsonValue;
  readonly bindings: RunOnceStepBindings;
  readonly state: RunOnceStateBinding;
  readonly recordedAt: string;
}

const writeRunOnceStepInOwnedTransaction = Effect.fn("writeRunOnceStepInOwnedTransaction")(
  function* (sql: SqlClient.SqlClient, input: WriteRunOnceStepInput) {
    const commandId = deriveRunOnceCommandId(input.runId, input.ordinal, input.step);
    const evidenceId = deriveRunOnceEvidenceId(input.runId, input.ordinal, input.step);
    const receiptId = deriveRunOnceReceiptId(input.runId, input.ordinal, input.step);
    const markerId = deriveRunOnceMarkerId(input.runId, input.ordinal, input.step);
    const claimId = deriveRunOnceClaimId(input.runId, input.ordinal, input.step);
    const publicationId = deriveRunOncePublicationId(input.runId, input.ordinal, input.step);
    const payloadJson = canonicalJson(input.payload);
    const payloadBytes = new TextEncoder().encode(payloadJson);
    const payloadFingerprint = sha256Utf8(payloadJson);
    const markerFingerprint = sha256Utf8(
      canonicalJson({
        domain: "agent-control-run-once-step-marker-v1",
        runId: input.runId,
        ordinal: input.ordinal,
        step: input.step,
        evidenceId,
        receiptId,
        markerId,
        commandId,
        payloadFingerprint,
        recordedAt: input.recordedAt,
      }),
    );

    const existing = yield* loadStepRows(sql, input.runId, input.ordinal).pipe(
      Effect.mapError((cause) =>
        failure(input.projectId, input.runId, input.step, "persistence", cause),
      ),
    );
    if (existing.length !== 0) {
      const row = existing[0];
      if (
        existing.length !== 1 ||
        row === undefined ||
        row.evidenceId !== evidenceId ||
        row.receiptId !== receiptId ||
        row.markerId !== markerId ||
        row.commandId !== commandId ||
        row.payloadFingerprint !== payloadFingerprint ||
        row.markerFingerprint !== markerFingerprint ||
        row.publicationId !== publicationId ||
        row.projectId !== input.projectId ||
        row.step !== input.step ||
        row.ordinal !== input.ordinal ||
        row.recordedAt !== input.recordedAt ||
        !(row.payloadJson instanceof Uint8Array) ||
        !sameBytes(row.payloadJson, payloadBytes) ||
        row.claimId !== claimId ||
        row.claimedAt !== input.recordedAt ||
        row.receiptStatus !== "accepted" ||
        row.acceptedAt !== input.recordedAt ||
        row.committedAt !== input.recordedAt ||
        (row.publishedAt !== null && typeof row.publishedAt !== "string") ||
        typeof row.attemptCount !== "number" ||
        !Number.isSafeInteger(row.attemptCount) ||
        row.attemptCount < 0
      ) {
        return yield* failure(input.projectId, input.runId, input.step, "identity-mismatch");
      }
      return { replayed: true, publicationId } as const;
    }
    const partial = yield* sql<{ readonly count: number }>`
    SELECT (
      (SELECT COUNT(*) FROM main.agent_control_run_once_step_evidence
       WHERE run_id = ${input.runId} AND ordinal = ${input.ordinal}) +
      (SELECT COUNT(*) FROM main.agent_control_run_once_step_receipts
       WHERE run_id = ${input.runId} AND ordinal = ${input.ordinal}) +
      (SELECT COUNT(*) FROM main.agent_control_run_once_step_markers
       WHERE run_id = ${input.runId} AND ordinal = ${input.ordinal}) +
      (SELECT COUNT(*) FROM main.agent_control_run_once_publications
       WHERE run_id = ${input.runId} AND ordinal = ${input.ordinal})
    ) AS count
  `.pipe(
      Effect.mapError((cause) =>
        failure(input.projectId, input.runId, input.step, "persistence", cause),
      ),
    );
    if (partial[0]?.count !== 0) {
      return yield* failure(input.projectId, input.runId, input.step, "partial-replay");
    }

    yield* sql`
      INSERT INTO main.agent_control_run_once_step_claims (
        claim_id, run_id, ordinal, step, command_id, claimed_at
      ) VALUES (
        ${claimId}, ${input.runId}, ${input.ordinal}, ${input.step},
        ${commandId}, ${input.recordedAt}
      )
      ON CONFLICT (run_id, ordinal) DO NOTHING
    `.pipe(
      Effect.mapError((cause) =>
        failure(input.projectId, input.runId, input.step, "persistence", cause),
      ),
    );
    const claims = yield* sql<{
      readonly claimId: unknown;
      readonly step: unknown;
      readonly commandId: unknown;
      readonly claimedAt: unknown;
    }>`
    SELECT claim_id AS "claimId", step, command_id AS "commandId", claimed_at AS "claimedAt"
    FROM main.agent_control_run_once_step_claims
    WHERE run_id = ${input.runId} AND ordinal = ${input.ordinal}
  `;
    if (
      claims.length !== 1 ||
      claims[0]?.claimId !== claimId ||
      claims[0]?.step !== input.step ||
      claims[0]?.commandId !== commandId ||
      claims[0]?.claimedAt !== input.recordedAt
    ) {
      return yield* failure(input.projectId, input.runId, input.step, "identity-mismatch");
    }

    yield* Effect.gen(function* () {
      yield* sql`
        INSERT INTO main.agent_control_run_once_step_evidence (
          evidence_id, receipt_id, marker_id, run_id, project_id, ordinal, step,
          command_id, payload_json, payload_fingerprint, task_id, stage_run_id, lease_id,
          worktree_reservation_id, controlled_thread_reservation_id,
          terminal_task_event_id, terminal_task_event_sequence,
          terminal_task_event_stream_version, mode_event_id, mode_event_sequence,
          mode_event_stream_version, recorded_at
        ) VALUES (
          ${evidenceId}, ${receiptId}, ${markerId}, ${input.runId}, ${input.projectId},
          ${input.ordinal}, ${input.step}, ${commandId}, ${payloadBytes}, ${payloadFingerprint},
          ${input.bindings.taskId ?? null}, ${input.bindings.stageRunId ?? null},
          ${input.bindings.leaseId ?? null}, ${input.bindings.worktreeReservationId ?? null},
          ${input.bindings.controlledThreadReservationId ?? null},
          ${input.bindings.terminalTaskEventId ?? null},
          ${input.bindings.terminalTaskEventSequence ?? null},
          ${input.bindings.terminalTaskEventStreamVersion ?? null},
          ${input.bindings.modeEventId ?? null}, ${input.bindings.modeEventSequence ?? null},
          ${input.bindings.modeEventStreamVersion ?? null}, ${input.recordedAt}
        )
      `;
      yield* sql`
        INSERT INTO main.agent_control_run_once_step_receipts (
          receipt_id, evidence_id, marker_id, run_id, ordinal, step, command_id,
          status, accepted_at
        ) VALUES (
          ${receiptId}, ${evidenceId}, ${markerId}, ${input.runId}, ${input.ordinal},
          ${input.step}, ${commandId}, 'accepted', ${input.recordedAt}
        )
      `;
      yield* sql`
        INSERT INTO main.agent_control_run_once_publications (
          publication_id, marker_id, evidence_id, run_id, ordinal, step
        ) VALUES (
          ${publicationId}, ${markerId}, ${evidenceId}, ${input.runId},
          ${input.ordinal}, ${input.step}
        )
      `;
      if (input.ordinal === 1) {
        yield* sql`
          INSERT INTO main.agent_control_run_once_states (
            run_id, project_id, status, next_ordinal, last_step, task_id, stage_run_id,
            lease_id, worktree_reservation_id, controlled_thread_reservation_id,
            terminal_task_event_id, activation_project_revision, reset_project_revision,
            updated_at
          ) VALUES (
            ${input.runId}, ${input.state.projectId}, ${input.state.status}, 2, ${input.step},
            ${input.state.taskId}, ${input.state.stageRunId}, ${input.state.leaseId},
            ${input.state.worktreeReservationId}, ${input.state.controlledThreadReservationId},
            ${input.state.terminalTaskEventId}, ${input.state.activationProjectRevision},
            ${input.state.resetProjectRevision}, ${input.recordedAt}
          )
        `;
      } else {
        const updated = yield* sql<{ readonly runId: unknown }>`
          UPDATE main.agent_control_run_once_states SET
            status = ${input.state.status}, next_ordinal = ${input.ordinal + 1},
            last_step = ${input.step}, task_id = ${input.state.taskId},
            stage_run_id = ${input.state.stageRunId}, lease_id = ${input.state.leaseId},
            worktree_reservation_id = ${input.state.worktreeReservationId},
            controlled_thread_reservation_id = ${input.state.controlledThreadReservationId},
            terminal_task_event_id = ${input.state.terminalTaskEventId},
            reset_project_revision = ${input.state.resetProjectRevision},
            updated_at = ${input.recordedAt}
          WHERE run_id = ${input.runId} AND project_id = ${input.state.projectId}
            AND next_ordinal = ${input.ordinal}
            AND activation_project_revision = ${input.state.activationProjectRevision}
          RETURNING run_id AS "runId"
        `;
        if (updated.length !== 1 || updated[0]?.runId !== input.runId) {
          return yield* failure(input.projectId, input.runId, input.step, "projection-corrupt");
        }
      }
      // Marker is deliberately the final business DML in this transaction.
      yield* sql`
        INSERT INTO main.agent_control_run_once_step_markers (
          marker_id, evidence_id, receipt_id, run_id, ordinal, step, command_id,
          marker_fingerprint, committed_at
        ) VALUES (
          ${markerId}, ${evidenceId}, ${receiptId}, ${input.runId}, ${input.ordinal},
          ${input.step}, ${commandId}, ${markerFingerprint}, ${input.recordedAt}
        )
      `;
    }).pipe(
      Effect.mapError((cause) =>
        cause instanceof AgentControlRunOnceError
          ? cause
          : failure(input.projectId, input.runId, input.step, "persistence", cause),
      ),
    );
    return { replayed: false, publicationId } as const;
  },
);

/** Caller-owned transaction variant for a wider authority snapshot. */
export const writeRunOnceStepInTransaction = Effect.fn("writeRunOnceStepInTransaction")(function* (
  sql: SqlClient.SqlClient,
  input: WriteRunOnceStepInput,
) {
  return yield* writeRunOnceStepInOwnedTransaction(sql, input);
});

/** Atomically writes Claim + Evidence + Receipt + Projection + Publication + Marker. */
export const writeRunOnceStep = Effect.fn("writeRunOnceStep")(function* (
  sql: SqlClient.SqlClient,
  input: WriteRunOnceStepInput,
) {
  return yield* sql
    .withTransaction(writeRunOnceStepInOwnedTransaction(sql, input))
    .pipe(
      Effect.mapError((cause) =>
        cause instanceof AgentControlRunOnceError
          ? cause
          : failure(input.projectId, input.runId, input.step, "persistence", cause),
      ),
    );
});

export const admitRunOnceActivation = Effect.fn("admitRunOnceActivation")(function* (
  sql: SqlClient.SqlClient,
  activation: AgentControlRunOnceActivation,
) {
  const inserted = yield* sql<{ readonly runId: unknown }>`
    INSERT INTO main.agent_control_run_once_activations (
      run_id, project_id, activation_event_id, activation_event_sequence,
      activation_event_stream_version, activation_command_id,
      github_intake_sequence, github_event_id, github_event_sequence,
      github_event_stream_version, reconcile_revision, source_fingerprint, activated_at
    ) VALUES (
      ${activation.runId}, ${activation.projectId}, ${activation.activationEventId},
      ${activation.activationEventSequence}, ${activation.activationEventStreamVersion},
      ${activation.activationCommandId}, ${activation.githubIntakeSequence},
      ${activation.githubEventId}, ${activation.githubEventSequence},
      ${activation.githubEventStreamVersion}, ${activation.reconcileRevision},
      ${activation.sourceFingerprint}, ${activation.activatedAt}
    )
    ON CONFLICT (run_id) DO NOTHING
    RETURNING run_id AS "runId"
  `.pipe(
    Effect.mapError((cause) =>
      failure(activation.projectId, activation.runId, "activation-admitted", "persistence", cause),
    ),
  );
  const rows = yield* sql<Record<string, unknown>>`
    SELECT run_id AS "runId", project_id AS "projectId",
      activation_event_id AS "activationEventId",
      activation_event_sequence AS "activationEventSequence",
      activation_event_stream_version AS "activationEventStreamVersion",
      activation_command_id AS "activationCommandId",
      github_intake_sequence AS "githubIntakeSequence",
      github_event_id AS "githubEventId", github_event_sequence AS "githubEventSequence",
      github_event_stream_version AS "githubEventStreamVersion",
      reconcile_revision AS "reconcileRevision", source_fingerprint AS "sourceFingerprint",
      activated_at AS "activatedAt"
    FROM main.agent_control_run_once_activations WHERE run_id = ${activation.runId}
  `.pipe(
    Effect.mapError((cause) =>
      failure(activation.projectId, activation.runId, "activation-admitted", "persistence", cause),
    ),
  );
  const expected = {
    runId: activation.runId,
    projectId: activation.projectId,
    activationEventId: activation.activationEventId,
    activationEventSequence: activation.activationEventSequence,
    activationEventStreamVersion: activation.activationEventStreamVersion,
    activationCommandId: activation.activationCommandId,
    githubIntakeSequence: activation.githubIntakeSequence,
    githubEventId: activation.githubEventId,
    githubEventSequence: activation.githubEventSequence,
    githubEventStreamVersion: activation.githubEventStreamVersion,
    reconcileRevision: activation.reconcileRevision,
    sourceFingerprint: activation.sourceFingerprint,
    activatedAt: activation.activatedAt,
  };
  if (
    rows.length !== 1 ||
    canonicalJson(rows[0] as JsonValue) !== canonicalJson(expected as unknown as JsonValue) ||
    (inserted.length !== 0 && (inserted.length !== 1 || inserted[0]?.runId !== activation.runId))
  ) {
    return yield* failure(
      activation.projectId,
      activation.runId,
      "activation-admitted",
      "identity-mismatch",
    );
  }
  return { replayed: inserted.length === 0 } as const;
});

export interface RunOnceTerminalAuthority {
  readonly event: AgentControlTaskEvent;
  readonly status: "succeeded" | "failed" | "cancelled";
  readonly taskFinalizationEvidenceId: string;
}

export const loadRunOnceTerminalAuthority = Effect.fn("loadRunOnceTerminalAuthority")(function* (
  sql: SqlClient.SqlClient,
  projectId: ProjectId,
  runId: AgentControlRunOnceId,
  taskId: AgentControlTaskId,
  taskEvents: ReadonlyArray<AgentControlTaskEvent>,
) {
  const rows = yield* sql<{
    readonly evidenceId: unknown;
    readonly taskEventId: unknown;
    readonly taskEventSequence: unknown;
    readonly taskEventStreamVersion: unknown;
    readonly status: unknown;
  }>`
    SELECT evidence.task_finalization_evidence_id AS "evidenceId",
           evidence.task_event_id AS "taskEventId",
           evidence.task_event_sequence AS "taskEventSequence",
           evidence.task_event_stream_version AS "taskEventStreamVersion",
           json_extract(event.payload_json, '$.status') AS status
    FROM main.agent_control_task_verification_finalization_evidence evidence
    JOIN main.agent_control_task_verification_finalization_receipts receipt
      ON receipt.receipt_id = evidence.receipt_id
     AND receipt.task_finalization_evidence_id = evidence.task_finalization_evidence_id
     AND receipt.status = 'accepted'
    JOIN main.agent_control_task_verification_finalization_markers marker
      ON marker.marker_id = evidence.marker_id
     AND marker.receipt_id = receipt.receipt_id
     AND marker.task_finalization_evidence_id = evidence.task_finalization_evidence_id
    JOIN main.agent_control_events event ON event.event_id = evidence.task_event_id
    WHERE evidence.project_id = ${projectId} AND evidence.task_id = ${taskId}
  `.pipe(
    Effect.mapError((cause) =>
      failure(projectId, runId, "task-terminal-observed", "persistence", cause),
    ),
  );
  if (rows.length === 0) return null;
  const row = rows[0]!;
  const event = taskEvents.find(
    (candidate) =>
      candidate.eventId === row.taskEventId &&
      candidate.sequence === row.taskEventSequence &&
      candidate.streamVersion === row.taskEventStreamVersion,
  );
  if (
    rows.length !== 1 ||
    typeof row.evidenceId !== "string" ||
    event === undefined ||
    event.type !== "agentControl.task.finalizedAfterVerification" ||
    (row.status !== "succeeded" && row.status !== "failed" && row.status !== "cancelled") ||
    event.payload.status !== row.status
  ) {
    return yield* failure(projectId, runId, "task-terminal-observed", "authority-conflict");
  }
  return {
    event,
    status: row.status,
    taskFinalizationEvidenceId: row.evidenceId,
  } satisfies RunOnceTerminalAuthority;
});

export const decodeRunOncePayloadBytes = (raw: unknown) => decodeCanonicalUtf8Bytes(raw);
